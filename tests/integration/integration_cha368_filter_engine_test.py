"""CHA-368 — single filter-execution engine (drop Postgres filter pushdown).

RED-phase acceptance for ``/do-issue`` CHA-368. These pin the *mechanism* the
ticket changes: the user ``WHERE`` must not be evaluated by Postgres —
DataFusion becomes the sole user-filter engine across tiers, so the unprovable
Postgres≡DataFusion equivalence obligation is eliminated. The observable is
white-box (what SQL Postgres runs), since query *results* are unchanged by the
refactor.

White-box seam: ``pg_stat_statements`` (preloaded on the test postgres). A
filtered read is issued, then the hot data-log resolve statement is inspected:

- RT-1 (``test_all_hot_resolve_reads_unfiltered_delta``): an all-hot filtered
  read must have Postgres return the FULL visible hot delta — the user
  predicate is applied by DataFusion, not spliced into the PG SQL. RED today:
  ``build_merge_resolved`` splices ``AND ({filter})`` (crates/penca-merge/src/sql.rs:119),
  so the resolve returns only the matching rows. GREEN once IMPL-1 drops the
  splice and IMPL-4 applies the ``full_plan_predicate`` residual on the all-hot
  path.
- RT-3 (``test_mixed_read_retires_exclusion_probe``): a mixed hot+cold read
  must NOT fire the hot exclusion probe (Query B) — its ``SELECT DISTINCT
  x.row_uuid FROM ( ... UNION ALL ... )`` statement must be absent, because the
  exclusion set is now derived from the unfiltered resolve output. RED today:
  the 4-probe ``build_resolved_and_exclusion_set`` (resolve.rs:414) fires
  ``hot_exclusion_row_uuids`` alongside ``resolve_hot``. GREEN once IMPL-2
  retires the probe.
- RT-2 (``test_cross_tier_stale_version_and_tombstone_exclusion``): a newer
  hot version that FAILS the filter, and a hot delete tombstone, must both
  exclude the older snapshot version that PASSES the filter. Regression guard
  for the post-union residual + exclusion-derived-from-resolve. May already be
  GREEN on main (the unfiltered exclusion set protects this today), so it rides
  alongside RT-1/RT-3, which carry the RED signal.

Invocation: ``just integration-test cha368_filter_engine``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.naming import upsert_log_table

from .integration_helpers import (
    USER_SCHEMA,
    ensure_pg_stat_statements,
    get_pg_driver,
    make_client,
    reset_pg_stat,
    setup_schema,
)


def _write_commit(client, ctx, rows: dict[str, list]) -> None:
    """Write one batch of ``rows`` and commit it on main."""
    catalog_uuid, schema_uuid, table_uuid, branch_uuid = ctx
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table(rows, schema=USER_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def _delete_commit(client, ctx, names: list[str]) -> None:
    """Delete rows by primary key ``names`` and commit on main."""
    catalog_uuid, schema_uuid, table_uuid, branch_uuid = ctx
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            deletes=pa.table({"name": names}, schema=pa.schema([("name", pa.utf8())])),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def _sum_resolve_rows(pg, upsert_log: str) -> int:
    """SUM(rows) over pg_stat_statements rows for the hot data-log RESOLVE.

    The resolve SELECT references the upsert log AND projects ``commit_micros``;
    the exclusion probe references the log but not ``commit_micros`` — so the
    two-needle match isolates the resolve statement's returned-row count. This
    is what tells us whether Postgres filtered (fewer rows) or returned the full
    visible delta (the CHA-368 target: DataFusion filters, PG does not).
    """
    rows = pg.execute(
        "SELECT COALESCE(SUM(rows), 0) FROM pg_stat_statements "
        "WHERE strpos(query, %s) > 0 AND strpos(query, 'commit_micros') > 0",
        (upsert_log,),
    )
    return int(rows[0][0])


def _count_exclusion_probes(pg, upsert_log: str) -> int:
    """Number of pg_stat_statements entries that are the *exclusion probe*
    (Query B) over this table's upsert log.

    The probe has a signature unique in the merge SQL: ``SELECT DISTINCT
    x.row_uuid FROM ( ... UNION ALL ... )``. The resolve (Query A) uses
    ``DISTINCT ON ("row_uuid")`` instead, so this needle matches only the
    probe. Pre-CHA-368 a merge read fires exactly one; once the probe is
    retired (exclusion derived from the resolve output), zero.
    """
    rows = pg.execute(
        "SELECT COUNT(*) FROM pg_stat_statements "
        "WHERE strpos(query, %s) > 0 AND strpos(query, 'DISTINCT x.row_uuid') > 0",
        (upsert_log,),
    )
    return int(rows[0][0])


class TestSingleFilterEngine:
    """CHA-368: DataFusion is the sole user-filter engine; PG never filters."""

    # Serialized: asserts on process-global white-box state (container stdout log
    # windows / pg_stat_statements counters) that a concurrent worker would
    # pollute. Runs in the serial phase, not under -n auto.
    # TODO(CHA-519): drop this mark once the structured per-request seam lands.
    @pytest.mark.serial
    def test_all_hot_resolve_reads_unfiltered_delta(self):
        """RT-1: an all-hot filtered read must make Postgres return the full
        visible hot delta (4 rows), not just the 2 that match ``value > 25``.

        The user predicate is DataFusion's job; the client still sees exactly
        the 2 matching rows. RED today because ``build_merge_resolved`` splices
        the filter into the PG SQL, so the resolve returns 2.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)
        ctx = (catalog_uuid, schema_uuid, table_uuid, branch_uuid)

        # All-hot: 4 committed rows, no persist / no snapshot.
        _write_commit(
            client,
            ctx,
            {"name": ["a", "b", "c", "d"], "value": [10, 20, 30, 40]},
        )

        upsert_log = upsert_log_table(table_uuid, branch_uuid)
        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        reset_pg_stat(pg)

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 25",
        )

        # DataFusion applied the filter: client sees only the 2 matching rows.
        assert result.num_rows == 2, (
            f"expected 2 rows matching value>25; got {result.num_rows}"
        )

        # White-box: Postgres returned the FULL visible delta (4), proving the
        # user predicate was NOT pushed into the PG resolve.
        resolve_rows = _sum_resolve_rows(pg, upsert_log)
        assert resolve_rows >= 4, (
            "CHA-368: the hot resolve must read the full visible delta "
            f"(4 rows) unfiltered — Postgres returned {resolve_rows}, i.e. it "
            "still filtered the user predicate."
        )

    def test_all_hot_filtered_read_spans_multiple_batches(self):
        """RT-1 regression (IMPL-4): a filtered all-hot read whose delta spans
        MORE than one stream batch must filter every batch, not just the first.

        The residual predicate is planned once per read (registering a throwaway
        table ``l`` on the session) and evaluated per batch; a per-batch re-plan
        would re-register ``l`` and error ("table l already exists") on the
        second batch. The default stream batch size is 1000, so 1500 hot rows
        force at least two batches — RT-1's 4-row delta never exercised this.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)
        ctx = (catalog_uuid, schema_uuid, table_uuid, branch_uuid)

        # All-hot delta larger than one stream batch (default 1000).
        n = 1500
        _write_commit(
            client,
            ctx,
            {"name": [f"r{i}" for i in range(n)], "value": list(range(n))},
        )

        # Selective residual that keeps rows drawn from BOTH batches (values
        # 1000..1499 all live in the second+ batch; 0..999 in the first).
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value >= 998",
        )
        assert result.num_rows == n - 998, (
            f"expected {n - 998} rows matching value>=998 across all batches; "
            f"got {result.num_rows} — the residual likely skipped later batches"
        )

    # Serialized: asserts on process-global white-box state (container stdout log
    # windows / pg_stat_statements counters) that a concurrent worker would
    # pollute. Runs in the serial phase, not under -n auto.
    # TODO(CHA-519): drop this mark once the structured per-request seam lands.
    @pytest.mark.serial
    def test_mixed_read_retires_exclusion_probe(self):
        """RT-3: a mixed hot+cold read must not fire the hot exclusion probe
        (Query B). RED today: the 4-probe merge fires ``hot_exclusion_row_uuids``
        (a ``SELECT DISTINCT x.row_uuid`` over the log) alongside the resolve.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)
        ctx = (catalog_uuid, schema_uuid, table_uuid, branch_uuid)

        # Cold baseline: write, persist, snapshot.
        _write_commit(client, ctx, {"name": ["a", "b"], "value": [10, 20]})
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
        # Hot delta on top (forces the mixed merge path, not the all-hot fast path).
        _write_commit(client, ctx, {"name": ["c"], "value": [30]})

        upsert_log = upsert_log_table(table_uuid, branch_uuid)
        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        reset_pg_stat(pg)

        client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 5",
        )

        probes = _count_exclusion_probes(pg, upsert_log)
        assert probes == 0, (
            "CHA-368: the hot exclusion probe (Query B) must be retired — the "
            "exclusion set is derived from the unfiltered resolve output. Saw "
            f"{probes} `SELECT DISTINCT x.row_uuid` probe statement(s) against "
            "the upsert log."
        )

    def test_cross_tier_stale_version_and_tombstone_exclusion(self):
        """RT-2: the current version decides membership across tiers.

        (a) snapshot value=500 PASSES ``value > 400``; a newer HOT update to
            value=50 FAILS it → row absent.
        (b) snapshot value=500 PASSES; a newer HOT delete tombstone → row
            absent.

        Regression guard for the post-union residual + exclusion-from-resolve.
        """
        # (a) hot update to a non-matching value shadows the snapshot.
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, branch_uuid = setup_schema(client)
        ctx = (catalog_uuid, schema_uuid, table_uuid, branch_uuid)
        _write_commit(client, ctx, {"name": ["alice"], "value": [500]})
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
        _write_commit(client, ctx, {"name": ["alice"], "value": [50]})  # hot, fails
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 400",
        )
        assert result.num_rows == 0, (
            "stale-version leak: snapshot value=500 surfaced even though the "
            f"current hot version (50) fails value>400. Got {result.num_rows}."
        )

        # (b) hot delete tombstone shadows a matching snapshot row.
        client2 = make_client()
        schema_uuid2, table_uuid2, catalog_uuid2, branch_uuid2 = setup_schema(client2)
        ctx2 = (catalog_uuid2, schema_uuid2, table_uuid2, branch_uuid2)
        _write_commit(client2, ctx2, {"name": ["bob"], "value": [500]})
        client2.persist(
            catalog_uuid=catalog_uuid2,
            schema_uuid=schema_uuid2,
            branch_uuid=branch_uuid2,
            table_uuid=table_uuid2,
        )
        client2.snapshot(
            catalog_uuid=catalog_uuid2,
            schema_uuid=schema_uuid2,
            branch_uuid=branch_uuid2,
            table_uuid=table_uuid2,
        )
        _delete_commit(client2, ctx2, ["bob"])  # hot tombstone
        result2 = client2.read_data(
            catalog_uuid=catalog_uuid2,
            schema_uuid=schema_uuid2,
            table_uuid=table_uuid2,
            branch_uuid=branch_uuid2,
            filter="value > 400",
        )
        assert result2.num_rows == 0, (
            "tombstone leak: deleted row (snapshot value=500) surfaced under "
            f"value>400. Got {result2.num_rows}."
        )
