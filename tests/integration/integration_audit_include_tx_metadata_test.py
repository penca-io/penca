"""[CHA-507] ``audit_data(include_tx_metadata=...)`` gates per-tx author/comment.

Author/comment move off cold data segments into a slim cold ``tx_log``; the
audit path reattaches them on demand by joining the cold ``tx_log`` on
``commit_seq_num``. The gate is pay-for-what-you-use:

* ``include_tx_metadata=True``  -> ``author`` / ``comment`` columns present
  (from the cold tx_log join), with the committed values.
* ``include_tx_metadata=False`` -> those two columns absent from the schema.

Both cases read from **cold** (after persist_and_snapshot_branch + purge), so
the True case genuinely exercises the join, not the on-segment columns.

RED on ``main``: the server ignores ``include_tx_metadata`` and the cold
segments still carry author/comment, so the False case still returns them —
the "columns absent" assertion fails. GREEN after IMPL-1 (thread the flag
server-side), IMPL-4 (drop author/comment from segments + cold tx_log join),
IMPL-5 (hot-tier gate).

Run via ``just integration-test``.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation

from .integration_helpers import (
    USER_SCHEMA,
    make_client,
    setup_schema,
)


def _ctx(client):
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    return {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "main_branch_uuid": main_branch_uuid,
        "table_uuid": table_uuid,
    }


def _commit_upsert(client, ctx, name, value, *, author, comment):
    tx = client.begin_tx(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        author=author,
        comment=comment,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=ctx["table_uuid"],
            upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
        ),
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )
    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=ctx["catalog_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )


def _drain_user_table_to_cold(client, ctx):
    """persist_and_snapshot_branch (writes the cold tx_log on GREEN) then purge
    the user table so the audit genuinely reads cold-stamped rows."""
    client.persist_and_snapshot_branch(
        catalog_uuid=ctx["catalog_uuid"], branch_uuid=ctx["main_branch_uuid"]
    )
    purge_response = client.purge(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        table_uuid=ctx["table_uuid"],
    )
    assert purge_response.HasField("purged_at_micros"), (
        "purge was a no-op; user rows still served from hot, cold path unexercised"
    )


def _audit(client, ctx, *, include_tx_metadata):
    return client.audit_data(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        table_uuid=ctx["table_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        include_tx_metadata=include_tx_metadata,
    )


class TestAuditIncludeTxMetadata:
    def test_include_true_reattaches_author_comment_from_cold(self):
        client = make_client()
        ctx = _ctx(client)
        _commit_upsert(client, ctx, "alice", 1, author="ada", comment="first")
        _drain_user_table_to_cold(client, ctx)

        upserts, _deletes = _audit(client, ctx, include_tx_metadata=True)
        assert "author" in upserts.schema.names, (
            "include_tx_metadata=True must reattach author from the cold tx_log"
        )
        assert "comment" in upserts.schema.names
        by_name = {
            n: (a, c)
            for n, a, c in zip(
                upserts.column("name").to_pylist(),
                upserts.column("author").to_pylist(),
                upserts.column("comment").to_pylist(),
                strict=True,
            )
        }
        assert by_name["alice"] == ("ada", "first")

    def test_include_false_omits_author_comment_from_cold(self):
        client = make_client()
        ctx = _ctx(client)
        _commit_upsert(client, ctx, "alice", 1, author="ada", comment="first")
        _drain_user_table_to_cold(client, ctx)

        upserts, _deletes = _audit(client, ctx, include_tx_metadata=False)
        assert "author" not in upserts.schema.names, (
            "include_tx_metadata=False must omit author (pay-for-what-you-use)"
        )
        assert "comment" not in upserts.schema.names
