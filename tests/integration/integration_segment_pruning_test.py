"""CHA-82 segment pruning integration tests.

## What these tests do — and don't — validate

**These tests validate end-to-end CORRECTNESS in the presence of
pruning.** They do NOT validate that pruning *happened* on any given
read. Row-count assertions are reachable via the production
non-pruning code paths (hot resolve wins; NULL-stats degrade to
keep-all; row-level filter applied post-read). The discriminating
pruning assertion lives at unit level —
`crates/penca-merge/src/lib.rs::tests::test_snapshot_pruning_*`
constructs segments through `snapshot_read_schema(user_schema)`
(the production writer shape) and asserts via the
`MockDlDriver::recorded_snapshot_reads()` counter that only the
surviving segments were fetched. After the post-review fix
(commit a845cb6) keys the v0 stats payload by column name, the
unit test is genuinely discriminating: any writer/reader schema
disagreement would fail it.

Integration-level segment-pruning assertion would require capturing
the `tracing::info!(target: "penca_merge::snapshot_pruning", …)`
event from the Python harness — non-trivial test-infra work
deferred to a follow-up ticket. Until then, the unit-level R1 + R5
+ R6 are the contract for "pruning actually trims segments"; the
tests here are the contract for "queries still return the right
rows."

## Per-test scope

- R2 (`test_snapshot_pruning_preserves_correctness_after_hot_update`):
  CORRECTNESS regression guard — a row whose snapshot value matches
  the filter and whose hot-tier update overwrites it to a different
  value must still appear in the result via the hot resolve, even
  if the snapshot segment got pruned. The test cannot distinguish
  "snapshot segment was pruned" from "snapshot segment was read
  and then masked by the exclusion set" — both produce the same
  row count. The assertion is on row count + value, not on
  pruning activity.

- R3 (`test_persist_unfiltered_after_hot_update_to_non_matching`):
  the ADR-0022 regression guard. Snapshot a row with a matching
  value, COLD-persist an update to a non-matching value, query with
  the filter. Result must be empty. The fixture explicitly persists
  between snapshot and update so the second write lands in cold
  persist segments — this is the load-bearing setup detail. If a
  future change adds persist-segment pruning, the segment with
  stats [50,50] would be dropped from BOTH the resolve query AND
  the exclusion-set query inside the same SessionContext; the
  stale snapshot value=500 would leak through and the assertion
  would change from 0 rows to 1.

- R4 (`test_legacy_empty_snapshot_statistics_does_not_error`):
  NO-CRASH guard for NULL stats. A segment with NULL `statistics`
  (pre-CHA-82 row or hand-NULLed row) must serve reads — the
  reader degrades to "no stats for this segment" and pruning keeps
  the segment. The test does NOT assert that pruning would have
  applied if stats had been present; that's R1's job.

- R7 (`test_persisting_unprunable_column_types_does_not_error`):
  NO-CRASH guard for unprunable column types. A table with a
  Date32 column (unprunable in compute_segment_statistics v0)
  alongside a prunable Int64 column must persist + snapshot +
  query without erroring. The test does NOT assert that the
  prunable column's stats drove pruning — the `amount > 100`
  filter is also applied row-wise post-read, so the result is
  the same whether the segment was pruned or scanned. The
  unit-level coverage in stats.rs's #[cfg(test)] mod
  (`round_trip_int32_and_utf8`) exercises the prunable-type path
  end-to-end.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import TABLE_SNAPSHOT_SEGMENT_METADATA
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    setup_schema,
)


def _prefix_table_name(catalog_uuid: str, table_name: str) -> str:
    """Build the per-catalog metadata table name (hyphenated-UUID prefix).

    Penca names per-catalog metadata tables ``{catalog_uuid}_<base>`` with
    the hyphenated UUID (quoted), so the prefix must keep the hyphens to
    address the physical relation the Rust naming helpers create. Mirrors
    the convention the lifecycle integration tests already rely on.
    """
    return f"{catalog_uuid}_{table_name}"


class TestSnapshotPruningRegressionGuards:
    """CHA-82 R2/R3: filter-aware pruning preserves correctness."""

    def test_snapshot_pruning_preserves_correctness_after_hot_update(self):
        """R2: snapshot row with value=50; hot-update to value=500; query
        WHERE value > 400.

        The snapshot segment's stats [50,50] would NOT match value > 400, so
        snapshot pruning skips the segment. But the hot-tier resolve picks
        up the current value=500 (which DOES match) independently.

        Assert: result contains the row at value=500.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)

        # Phase 1: write value=50, commit, persist, snapshot.
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [50]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Phase 2: hot-tier update to value=500 (matches filter `> 400`).
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [500]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
        )

        # Phase 3: read with filter "value > 400" — must include alice.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 400",
        )

        assert result.num_rows == 1, (
            f"expected 1 row (alice via hot update at 500); got {result.num_rows}"
        )
        rows = result.to_pylist()
        assert rows[0]["name"] == "alice"
        assert rows[0]["value"] == 500

    def test_persist_unfiltered_after_hot_update_to_non_matching(self):
        """R3 (ADR-0022 regression guard): snapshot row with value=500;
        persist (cold) an update to value=50; query WHERE value > 400.

        Setup explicitly persists between the snapshot and the update so the
        second write lands in cold persist segments (not in hot logs or the
        snapshot baseline). A future fixture refactor that collapses this
        timing silently weakens the regression guard.

        With current (correct) impl: persist resolve gives value=50; outer
        WHERE drops it; exclusion set includes row_uuid; snapshot scan drops
        the stale value=500 via exclusion; result empty.

        If a future change adds persist-segment pruning, the cold persist
        segment with stats [50,50] would be pruned from BOTH the resolve
        AND the exclusion-set queries inside the same SessionContext. The
        snapshot's stale value=500 would leak through. This test would
        then return 1 row instead of 0.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)

        # Phase 1: write value=500, commit, persist, snapshot.
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [500]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Phase 2: update to value=50 (does NOT match filter `> 400`), commit,
        # then PERSIST (so the update lands in cold persist segments — that's
        # the setup detail the ADR guard depends on).
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [50]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Phase 3: read with filter "value > 400" — must be empty.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 400",
        )

        assert result.num_rows == 0, (
            "ADR-0022 regression: cold persist segment with stats [50,50] "
            "was pruned from the exclusion set, allowing the stale snapshot "
            f"value=500 to leak through. Got {result.num_rows} rows."
        )

    def test_legacy_empty_snapshot_statistics_does_not_error(self):
        """R4: a snapshot segment with NULL statistics (pre-CHA-82 row, or
        one whose stats were nulled out via direct UPDATE) must still serve
        reads. The reader degrades to "no stats for this segment" and
        pruning keeps the segment.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice", "bob"], "value": [10, 20]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Force the snapshot segments to NULL stats — simulate a pre-CHA-82
        # row or a row whose stats got corrupted.
        pg = get_pg_driver()
        table_name = _prefix_table_name(catalog_uuid, TABLE_SNAPSHOT_SEGMENT_METADATA)
        pg.execute_no_result(
            SQL("UPDATE {} SET statistics = NULL").format(Identifier(table_name))
        )

        # Read with a filter — must succeed without erroring (pruning skipped
        # for segments with no stats; segment is read and filter applied
        # row-wise).
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 15",
        )

        assert result.num_rows == 1
        assert result.to_pylist()[0]["name"] == "bob"


class TestUnprunableColumnTypes:
    """CHA-82 R7: unprunable column types degrade silently."""

    def test_persisting_unprunable_column_types_does_not_error(self):
        """R7: a table with a Date32 column (unprunable in
        compute_segment_statistics v0) alongside a prunable Int64 column
        must persist + snapshot + query without erroring. The unprunable
        column is silently omitted from stats; the prunable column's
        stats drive pruning.
        """
        custom_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("amount", pa.int64()),
                pa.field("when", pa.date32()),
            ]
        )

        client = make_client()
        schema_uuid, _, catalog_uuid, branch_uuid = setup_schema(client)
        table_uuid = create_table_on_branch(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_name=f"unprunable_{uuid4().hex[:8]}",
            arrow_schema=custom_schema,
            primary_keys=["name"],
        )

        import datetime

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {
                        "name": ["alice", "bob"],
                        "amount": [50, 200],
                        "when": [
                            datetime.date(2026, 1, 1),
                            datetime.date(2026, 2, 1),
                        ],
                    },
                    schema=custom_schema,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Query on the prunable column — `amount > 100` matches only bob.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="amount > 100",
        )

        assert result.num_rows == 1
        assert result.to_pylist()[0]["name"] == "bob"
        assert result.to_pylist()[0]["amount"] == 200
