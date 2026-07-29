"""CHA-492 — the SQL point read (`WHERE pk = value`) on a snapshot-only table
takes the DataFusion-free exact-cover bypass.

This is the target-flip of the CHA-492 confirming trace (see the Linear
ticket). It exercises the *exact user shape* that was measured at ~2.9ms —
`SELECT ... WHERE <pk> = <lit>` over Flight SQL — against a snapshot-only cold
table, and scrapes the query container's `penca=debug` markers to pin which
read arm actually serves it.

Three hypotheses, one trace:

  H2  PK→`ids` extraction fires on the SQL path         → `ids_rows=1`
  H3  the plan is in the bypass-eligible envelope        → `tier_shape=snapshot_only`
  H1  the structured seek set fully COVERS the request, → `direct_point_read=true`,
      so the read is served straight from the snapshot     NO `scan_snapshot` span
      sidecar with no DataFusion merge plan

Before CHA-492 the residual filter diverted this shape into the DataFusion
merge pipeline (`scan_snapshot`, no `direct_point_read`); Stage 1 recognizes
the exact cover at the QueryService gate and routes it to the bypass. The
no-filter `ids` read (same table, gRPC) is the contrast control: identical
plan shape that has always taken the bypass.

The change lands in `PencaTableProvider::scan`, shared below the ADBC-prepared
vs JDBC-statement DoGet split, so both drivers must land on the bypass — the
positive test is parametrized over both.

Scoped run:  just integration-test cha492_exact_cover_trace
"""

from __future__ import annotations

import json
import re

import pyarrow as pa
import pytest

from .integration_helpers import (
    container_log,
    make_client,
    poll_log_for,
    setup_with_data_named,
)
from .integration_point_read_test import _sql_steps_via

_PK_ONLY_SCHEMA = pa.schema([pa.field("name", pa.utf8())])

# Lines worth surfacing in the trace dump — the discriminating markers plus the
# span-timing close line that carries server-side `time.busy`.
_MARKERS = (
    "ids_rows=",
    "tier_shape=",
    "direct_point_read=",
    "scan_snapshot",
    "seek_snapshot_point",
    "read_data",
)

# `read_data{...}: ... close time.busy=<dur>` — the penca-api span close under
# PENCA_SPAN_TIMING=1; the busy duration is the server-side execution time.
_BUSY_RE = re.compile(r"read_data\{[^}]*\}.*close.*time\.busy=([0-9.]+(?:ns|µs|ms|s))")


def _snapshot_only_named(client) -> tuple[dict, dict]:
    """`setup_with_data_named` (PK=`name`, rows alice=10/bob=20, connection
    pinned to the fresh catalog) then persist→snapshot→purge so the table is
    snapshot-only cold (hot drained, `is_all_cold` + no persist band)."""
    ctx = setup_with_data_named(client)
    kw = {
        "catalog_uuid": ctx["catalog_uuid"],
        "schema_uuid": ctx["schema_uuid"],
        "branch_uuid": ctx["main_branch_uuid"],
        "table_uuid": ctx["table_uuid"],
    }
    # CHA-444: snapshot precedes purge so the baseline forms before Pu advances.
    client.persist(**kw)
    client.snapshot(**kw)
    client.purge(**kw)
    return ctx, kw


def _dump(label: str, window: str) -> None:
    lines = [line for line in window.splitlines() if any(m in line for m in _MARKERS)]
    print(f"\n===== {label} — query-container trace window =====")
    for line in lines:
        print(line)

    busy = _BUSY_RE.findall(window)
    if busy:
        print(f"----- read_data span time.busy: {busy}")

    print("=" * 60)


class TestCha492ExactCoverTrace:
    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_sql_pk_point_read_snapshot_only_takes_bypass(self, driver):
        """A SQL `WHERE pk = lit` read on a snapshot-only table is served by
        the DataFusion-free exact-cover bypass (`direct_point_read=true`, no
        `scan_snapshot`). Parametrized over both drivers: the fix lands in
        `PencaTableProvider::scan`, below the ADBC-prepared / JDBC-statement
        DoGet split, so one impl must cover both."""
        client = make_client()
        ctx, kw = _snapshot_only_named(client)
        target = f"{ctx['schema_name']}.{ctx['table_name']}"

        # The user shape: SQL `WHERE pk = lit` over Flight SQL
        since = len(container_log("query"))
        results = _sql_steps_via(
            driver,
            [f"SELECT value FROM {target} WHERE name = 'alice'"],
            ctx["catalog_name"],
        )
        status, payload = results[0]
        assert status == "OK_ROWS", results
        assert json.loads(payload) == [{"value": 10}], results

        # Flush the read_data span close before scraping. `ids_rows=1` is
        # emitted on both the merge and the bypass arm (PK→ids extraction still
        # fires), so it is a reliable barrier; then poll the target marker,
        # which times out (returns False) in the red state and flushes in green.
        assert poll_log_for("query", since, "ids_rows=1"), (
            "SQL point read produced no read_data span with ids_rows=1 — "
            "PK→ids extraction did not fire (harness/coverage issue)"
        )
        poll_log_for("query", since, "direct_point_read=true")
        sql_window = container_log("query")[since:]
        _dump(f"SQL  WHERE name='alice'  (snapshot-only, {driver})", sql_window)

        # H2: PK→ids extraction fired on the SQL path (scan still packs ids).
        assert "ids_rows=1" in sql_window
        # H3: the plan is snapshot-only (bypass-eligible envelope). `tier_shape`
        # is a string field, so the fmt subscriber renders it QUOTED.
        assert 'tier_shape="snapshot_only"' in sql_window, (
            "expected a snapshot-only plan after persist→snapshot→purge"
        )
        # H1 / TARGET (the point of the ticket): the structured seek exactly
        # covers the request, so the bypass serves it — the direct arm fires...
        assert "direct_point_read=true" in sql_window, (
            "the SQL point read must take the DataFusion-free exact-cover "
            "bypass (direct_point_read=true); it still falls into the merge "
            "pipeline (the residual is not recognized as fully covered)"
        )
        # ...and the DataFusion merge cold-scan does NOT. Flush barrier + exact
        # count: the direct_point_read poll above is a
        # barrier on a DIFFERENT needle, so a scan_snapshot from the same read
        # could be un-flushed at scrape time. Issue a second qualifying read and
        # poll ITS bypass marker; once that flushes the log is append-only, so
        # any scan_snapshot the first read emitted is guaranteed present — the
        # COUNT over the window is race-immune.
        barrier_since = len(container_log("query"))
        _sql_steps_via(
            driver,
            [f"SELECT value FROM {target} WHERE name = 'alice'"],
            ctx["catalog_name"],
        )
        assert poll_log_for("query", barrier_since, "direct_point_read=true"), (
            "flush-barrier qualifying read must also take the exact-cover bypass"
        )
        assert container_log("query")[since:].count("scan_snapshot") == 0, (
            "the exact-cover bypass must serve the read without building a "
            "DataFusion cold scan (scan_snapshot span present)"
        )

    def test_no_filter_ids_read_contrast_takes_bypass(self):
        """Same snapshot-only table, gRPC `ids` read with NO residual filter:
        the identical plan shape has always taken the DataFusion-free bypass.
        The control that isolates the residual as the only variable the SQL
        path had to overcome."""
        client = make_client()
        _ctx, kw = _snapshot_only_named(client)
        ids = pa.table({"name": ["alice"]}, schema=_PK_ONLY_SCHEMA)

        since = len(container_log("query"))
        result = client.read_data(ids=ids, **kw)
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [10]

        assert poll_log_for("query", since, "direct_point_read=true"), (
            "no-filter snapshot-only ids read must take the direct bypass arm"
        )
        contrast_window = container_log("query")[since:]
        _dump("gRPC ids read, NO filter  (snapshot-only)", contrast_window)

        assert "direct_point_read=true" in contrast_window
        assert "scan_snapshot" not in contrast_window, (
            "the bypass must not build a DataFusion cold scan"
        )

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_sql_pk_point_read_inside_open_tx_takes_bypass(self, driver):
        """CHA-501: the exact-cover bypass must fire for a point read issued
        *inside* an open transaction (`BEGIN … COMMIT`), not only autocommit.

        The in-tx `SELECT WHERE pk = lit` targets a key the tx has NOT written,
        so the plan is genuinely snapshot-only (no hot overlay — the CHA-473
        `EXISTS(upsert) OR EXISTS(delete)` gate is empty for the table). The
        read resolves at `ReadSnapshot::OpenTx`; before CHA-501 the
        `LatestSeq | AsOfSeq`-only axis gate in `read_data_seek_eligible`
        excluded it, so it fell into the DataFusion merge (`scan_snapshot`, no
        `direct_point_read`). CHA-501 drops the axis restriction — the sole gate
        is `is_snapshot_only(plan) && exact_selection` — so the OpenTx read takes
        the same DataFusion-free arm the autocommit shape above does.

        Parametrized over both drivers: BEGIN/SELECT/COMMIT run on one session
        (so the JDBC arm doesn't auto-commit each step), and the eligibility gate
        is at the convergent QueryManager layer below the ADBC-prepared /
        JDBC-statement DoGet split, so both must land on the bypass.
        """
        client = make_client()
        ctx, _kw = _snapshot_only_named(client)
        target = f"{ctx['schema_name']}.{ctx['table_name']}"
        select = f"SELECT value FROM {target} WHERE name = 'alice'"

        # ---- The user shape: a point SELECT inside an open tx over Flight SQL --
        since = len(container_log("query"))
        results = _sql_steps_via(
            driver, ["BEGIN", select, "COMMIT"], ctx["catalog_name"]
        )
        assert results[0][0] == "OK", results  # BEGIN
        status, payload = results[1]
        assert status == "OK_ROWS", results
        assert json.loads(payload) == [{"value": 10}], results
        assert results[2][0] == "OK", results  # COMMIT

        # Barrier on `ids_rows=1` (emitted on both merge and bypass arms — PK→ids
        # extraction fires regardless of the tx), then poll the target marker.
        assert poll_log_for("query", since, "ids_rows=1"), (
            "in-tx SQL point read produced no read_data span with ids_rows=1 — "
            "PK→ids extraction did not fire (harness/coverage issue)"
        )
        poll_log_for("query", since, "direct_point_read=true")
        tx_window = container_log("query")[since:]
        _dump(
            f"open-tx SELECT WHERE name='alice'  (snapshot-only, {driver})", tx_window
        )

        # The plan is snapshot-only even under OpenTx (hot log empty for the
        # table, no persist band) — the bypass-eligible envelope.
        assert 'tier_shape="snapshot_only"' in tx_window, (
            "expected a snapshot-only plan for the in-tx read after "
            "persist→snapshot→purge"
        )
        # TARGET: the OpenTx read takes the DataFusion-free exact-cover bypass.
        assert "direct_point_read=true" in tx_window, (
            "the in-tx point read must take the DataFusion-free exact-cover "
            "bypass (direct_point_read=true); before CHA-501 the OpenTx axis was "
            "excluded so it fell into the merge pipeline"
        )
        # ...and no DataFusion cold scan. Flush barrier + exact count (same
        # race-immunity argument as the autocommit test): issue a second in-tx
        # qualifying read and poll ITS bypass marker before counting.
        barrier_since = len(container_log("query"))
        _sql_steps_via(driver, ["BEGIN", select, "COMMIT"], ctx["catalog_name"])
        assert poll_log_for("query", barrier_since, "direct_point_read=true"), (
            "flush-barrier in-tx qualifying read must also take the bypass"
        )
        assert container_log("query")[since:].count("scan_snapshot") == 0, (
            "the exact-cover bypass must serve the in-tx read without building a "
            "DataFusion cold scan (scan_snapshot span present)"
        )

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_sql_point_read_of_own_write_in_tx_stays_merged(self, driver):
        """CHA-501 RYOW negative control (data-path mirror of CHA-471's metadata
        guard): when an open tx has WRITTEN a key, reading it back in the same tx
        must NOT take the seek bypass over the cold baseline. The tx's own
        uncommitted upsert row makes the CHA-473 `EXISTS(upsert)` existence gate
        report `hot_present` → the plan is not snapshot-only →
        `is_direct_seek_eligible` is false → the read rides the merge, whose
        OpenTx RYOW clause surfaces the tx's own write.

        The discriminator is **behavioral, not log-scraped**: the in-tx read
        returns the tx's own write (`999`), which is *only* possible via the
        merge. Had the axis-independent widening over-fired the bypass onto this
        read, the DataFusion-free seek would serve the snapshot-only cold
        baseline — which the tx's write does not touch — and return the stale
        committed value (`10`). So `value == 999` is exactly the "merged, not
        bypassed" guard. (A log-marker check is deliberately avoided here: the
        `UPDATE` is itself a server-side read-modify-write whose *pre-write*
        internal read of `alice` — a key the tx has not yet written — correctly
        DOES take the bypass, so `direct_point_read=true` legitimately appears in
        the window from that read, not from the RYOW `SELECT`.) Parametrized over
        both drivers.
        """
        client = make_client()
        ctx, _kw = _snapshot_only_named(client)
        target = f"{ctx['schema_name']}.{ctx['table_name']}"

        # BEGIN; UPDATE alice; SELECT alice (RYOW); COMMIT — one session so the
        # JDBC arm doesn't auto-commit each step.
        results = _sql_steps_via(
            driver,
            [
                "BEGIN",
                f"UPDATE {target} SET value = 999 WHERE name = 'alice'",
                f"SELECT value FROM {target} WHERE name = 'alice'",
                "COMMIT",
            ],
            ctx["catalog_name"],
        )
        assert results[0][0] == "OK", results  # BEGIN
        assert results[1][0] == "OK", results  # UPDATE
        # RYOW: the in-tx SELECT returns the tx's own uncommitted write (999),
        # not the stale cold baseline (10) a wrongly-fired bypass would serve.
        assert results[2][0] == "OK_ROWS", results
        assert json.loads(results[2][1]) == [{"value": 999}], results
        assert results[3][0] == "OK", results  # COMMIT

        # Post-commit external read confirms the write landed (and that the base
        # is genuinely cold + snapshot-only pre-write — otherwise the RYOW proof
        # above would be vacuous).
        external = client.execute_query(
            f"SELECT value FROM {target} WHERE name = 'alice'"
        )
        assert external.column("value").to_pylist() == [999], external
