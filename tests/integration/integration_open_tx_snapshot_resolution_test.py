"""CHA-540 — open-tx snapshot resolution happens once, and a dead ``open_tx_uuid``
is an error rather than a silent downgrade.

An autocommit read over Flight SQL resolves its snapshot to an integer once
(``pin_as_of_seq`` stamps the seq frontier on the ticket) and every downstream
resolution is then a pure arm. With a tx open, ``open_tx_uuid`` rides instead, and
``read_data`` used to resolve the tx **twice** against Postgres: once in
``resolve_base`` -> ``resolve_read_snapshot`` (a bare unvalidated
``begin_tx_log`` SELECT) and again for the data read via a second, validating
resolver. Both the bare SELECT and that second resolver are now gone —
``resolve_read_snapshot`` validates once via ``resolve_tx`` and ``read_data``
reuses the result.

This module pins:

- ``test_open_tx_read_issues_one_begin_tx_log_lookup`` — an open-tx read issues
  exactly ONE statement referencing the branch's ``begin_tx_log`` partition. This
  was the red test: before the fix it issued 2.
- the ``TestDeleteSchemaWithDeadTx`` and ``TestDeadTxCharacterization`` cases — a
  dead ``open_tx_uuid`` returns the same status codes it always has. These were
  GREEN before and after; acceptance criterion 2 is "same status codes it does
  today", so they are regression guards, not red tests.

The fallthrough these guards protect against had no user-visible consequence
before the change, which is why none of them went red for it: reads were covered
by the second validating resolver and every append path by ``resolve_tx``. It
mattered because deleting that second resolver leaves ``resolve_read_snapshot`` as
the read path's only dead-tx validation — so these guards are what would go red if
the fallthrough had survived the deletion.

Counting is via ``pg_stat_statements``, the same seam the CHA-367 / CHA-441
resolution-count tests use: ``count_stmts_referencing`` sums ``calls`` over
normalized statements whose text contains the per-branch ``begin_tx_log``
partition identifier, so background activity on other branches cannot pollute
the count. The merge SQL's own open-tx clause splices a *literal* synthetic row
rather than selecting from ``begin_tx_log`` (deliberately, so hot and cold get
identical SQL), so it does not contribute to this count.

Run: ``just integration-test --test-arg integration_open_tx_snapshot_resolution_test``.
"""

from __future__ import annotations

import pytest
from penca_client.errors import FailedPreconditionError, NotFoundError
from penca_client.naming import begin_tx_log_partition

from .integration_helpers import (
    count_stmts_referencing,
    ensure_pg_stat_statements,
    get_pg_driver,
    make_client,
    reset_pg_stat,
    setup_with_data,
)

# A well-formed uuid that was never begun, so it is absent from begin_tx_log.
NEVER_BEGUN_TX = "11111111-1111-1111-1111-111111111111"


# Serial: `pg_stat_statements_reset()` is instance-global, so under `-n auto` a
# concurrent worker's reset landing mid-test would zero the counters and read
# `lookups` as 1 on unfixed code — a false green on the assertion that must stay
# red. `TestDeadTxCharacterization` below touches no side channel and stays
# parallel.
@pytest.mark.serial
class TestOpenTxResolutionCount:
    def test_open_tx_read_issues_one_begin_tx_log_lookup(self):
        """One open-tx read resolves the tx once, not twice.

        Before the fix ``resolve_base`` resolved it via a bare unvalidated
        ``begin_tx_log`` SELECT and ``read_data`` re-resolved it via the validating
        ``get_tx_status``, so the count was 2. It is 1 now that
        ``resolve_read_snapshot`` validates once and ``read_data`` reuses the
        resulting ``scope.snapshot``.
        """
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]

        tx = client.begin_tx(catalog_uuid=cat, schema_uuid=sch, branch_uuid=br)

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        # Per-branch needle: pg_stat_statements normalizes literals but preserves
        # identifiers, so this matches only this branch's begin_tx_log reads.
        begin_partition = begin_tx_log_partition(cat, br)

        # Reset AFTER begin_tx so the BEGIN's own insert into begin_tx_log is not
        # counted — we are measuring the READ's resolution round trips only.
        reset_pg_stat(pg)
        result = client.read_data(
            catalog_uuid=cat,
            schema_uuid=sch,
            table_uuid=tbl,
            branch_uuid=br,
            open_tx_uuid=tx.tx_uuid,
        )
        lookups = count_stmts_referencing(pg, begin_partition)

        client.abort_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)

        # Correctness rides alongside the count: the tx has written nothing, so an
        # RYOW read still sees the 2 committed rows.
        assert result.num_rows == 2, (
            f"open-tx read must still see the 2 committed rows (got "
            f"{result.num_rows}) — a count regression must not be bought with "
            f"wrong data"
        )
        assert lookups > 0, (
            "sanity: an open-tx read must consult begin_tx_log at least once; 0 "
            "means the needle is wrong, not that the fix landed"
        )
        assert lookups == 1, (
            f"open-tx read issued {lookups} begin_tx_log lookup(s); the tx should "
            f"be resolved once, by the validating get_tx_status, and threaded "
            f"through to the data read (expected 1)"
        )


class TestDeleteSchemaWithDeadTx:
    """Characterization guard — GREEN before and after CHA-540.

    `delete_schema_cascade` passes `request_tx_uuid` straight into
    `resolve_read_snapshot` and, unlike `read_data`, never had a second validating
    resolver behind it. That looks like it should expose the fallthrough, and it
    does not: every append path first calls `resolve_tx`
    (`crates/penca-api/src/write/mod.rs`), which runs the same
    `begin_tx_log ⟕ abort_tx_log ⟕ commit_tx_log` join and rejects a dead tx with
    exactly these codes before the fallthrough can matter.

    So this pins existing behavior, not a defect. It earns its place because CHA-540
    changes the error type of the resolver these write paths call and consolidates
    the tx-liveness mapping onto one helper — this is what catches a regression in
    the write path's dead-tx rejection while that happens.
    """

    def test_delete_schema_with_never_begun_tx_raises_not_found(self):
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        br = ctx["main_branch_uuid"]

        with pytest.raises(NotFoundError):
            client.delete_schema(
                catalog_uuid=cat,
                schema_uuid=sch,
                branch_uuid=br,
                tx_uuid=NEVER_BEGUN_TX,
            )

        # The delete must not have partially applied.
        assert (
            client.get_schema(
                catalog_uuid=cat, schema_uuid=sch, branch_uuid=br
            ).schema_uuid
            == sch
        ), "schema must survive a rejected delete_schema"

    def test_delete_schema_with_aborted_tx_raises_failed_precondition(self):
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        br = ctx["main_branch_uuid"]

        tx = client.begin_tx(catalog_uuid=cat, schema_uuid=sch, branch_uuid=br)
        client.abort_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)

        with pytest.raises(FailedPreconditionError):
            client.delete_schema(
                catalog_uuid=cat,
                schema_uuid=sch,
                branch_uuid=br,
                tx_uuid=tx.tx_uuid,
            )

        assert (
            client.get_schema(
                catalog_uuid=cat, schema_uuid=sch, branch_uuid=br
            ).schema_uuid
            == sch
        ), "schema must survive a rejected delete_schema"


class TestDeadTxCharacterization:
    """Regression guards — GREEN before and after CHA-540.

    Expiry is deliberately not covered: inducing it requires waiting out the tx
    TTL, which costs far more than it pins given aborted and committed already
    exercise both the ``get_tx_status`` non-Open arms and the same status code.
    """

    def test_read_with_never_begun_tx_raises_not_found(self):
        client = make_client()
        ctx = setup_with_data(client)

        with pytest.raises(NotFoundError):
            client.read_data(
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                table_uuid=ctx["table_uuid"],
                branch_uuid=ctx["main_branch_uuid"],
                open_tx_uuid=NEVER_BEGUN_TX,
            )

    def test_read_with_aborted_tx_raises_failed_precondition(self):
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        br = ctx["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=cat, schema_uuid=ctx["schema_uuid"], branch_uuid=br
        )
        client.abort_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)

        with pytest.raises(FailedPreconditionError):
            client.read_data(
                catalog_uuid=cat,
                schema_uuid=ctx["schema_uuid"],
                table_uuid=ctx["table_uuid"],
                branch_uuid=br,
                open_tx_uuid=tx.tx_uuid,
            )

    def test_read_with_committed_tx_raises_failed_precondition(self):
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        br = ctx["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=cat, schema_uuid=ctx["schema_uuid"], branch_uuid=br
        )
        client.commit_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)

        with pytest.raises(FailedPreconditionError):
            client.read_data(
                catalog_uuid=cat,
                schema_uuid=ctx["schema_uuid"],
                table_uuid=ctx["table_uuid"],
                branch_uuid=br,
                open_tx_uuid=tx.tx_uuid,
            )
