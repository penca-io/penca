"""Integration coverage for CHA-86: a default (no-``as_of``) read returns
one consistent snapshot spanning the cold and hot tiers.

Run via ``just integration-test read_mvcc`` (or ``just integration-test``).

This is *coverage*, not a torn-read catcher: every merge-on-read probe
shares the single ``pg_now`` fence the default read now pins, so a tear
cannot be provoked deterministically. The test documents that the fenced
default read correctly merges a cold snapshot baseline with a hot delta
written after the persist cut — exercising the full ``stream_merged`` path
(cold snapshot + hot resolve/exclusion), not the all-hot fast path.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    make_client,
    setup_schema,
)


def _write_commit(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, name, value
):
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


class TestDefaultReadColdHotConsistency:
    def test_default_read_merges_cold_baseline_and_hot_delta(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _main = setup_schema(client)
        branch = client.create_branch(
            "read_mvcc_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        kw = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": branch.branch_uuid,
            "table_uuid": table_uuid,
        }

        # Cold baseline: commit alice + bob, persist to cold, snapshot to the
        # baseline, purge hot. CHA-444 (ADR 0027): Purge advances the read
        # fence Pu only to W_snap, so Snapshot must precede Purge for alice/bob
        # to form the snapshot baseline and be cleared from hot.
        _write_commit(client, name="alice", value=1, **kw)
        _write_commit(client, name="bob", value=2, **kw)
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)

        # Hot delta written strictly after the cold cut.
        _write_commit(client, name="carol", value=3, **kw)

        # Default read (no as_of, no open_tx) pins one pg_now snapshot and
        # merges both tiers — cold baseline {alice, bob} + hot {carol}.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )

        assert result.num_rows == 3
        assert set(result.column("name").to_pylist()) == {"alice", "bob", "carol"}
