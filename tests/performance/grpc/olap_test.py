"""gRPC-API OLAP performance suite (scans + filters only — no aggregates).

First-pass operational yardstick over the native gRPC ``ReadData``
surface, kept separate from the OLTP suite because the two have
opposite cost structures (throughput-dominated scans vs
fixed-overhead-dominated point ops). Server-side aggregates
(count/sum/avg/GROUP BY) are an explicit later Flight SQL pass — the
native ``ReadData`` path does not aggregate — so this suite measures
scan + merge-on-read throughput, not aggregation. See ``olap.md`` in
this directory and CHA-416.

OLAP = throughput-oriented analytical scans on the steady-state
cold-snapshotted tier, run at two scales (100k and 1M rows):
- full-table scan via ``read_data`` (no filter),
- bulk filtered scan via ``read_data(filter="value < <threshold>")``
  (predicate pushdown returning roughly half the table).

The 100k cold-snapshotted full scan also appears — among all eight tiers
— in the Query suite's cross-*tier* read benchmark
(``performance_query_test.py::test_read_data``). Here it is the baseline
for the 100k -> 1M cross-*scale* throughput story, so the single-cell
overlap is intentional: the two suites measure different dimensions.

Run via ``just perf-test grpc/olap_test.py``.
"""

from __future__ import annotations

import time

import psycopg

from ..performance_helpers import (
    PerfResult,
    create_postgres_baseline_table,
    drop_postgres_baseline_table,
    insert_postgres_baseline,
    pg_conninfo,
)


def _half_match_threshold(scale: int) -> float:
    """Return a ``value`` threshold that matches the first ``scale // 2`` rows.

    Baseline values are ``float(i) * 1.1`` for ``i`` in ``0..scale``, so
    ``value < scale * 1.1 * 0.5`` matches exactly the rows with
    ``i < scale // 2``.
    """
    return scale * 1.1 * 0.5


class TestOlapPerformance:
    """Analytical scan throughput over the native gRPC ReadData API."""

    def test_full_scan(self, olap_setup, perf_recorder):
        """Full-table scan via gRPC ReadData on cold-snapshotted data."""
        client = olap_setup.client
        context = olap_setup.context
        scale = olap_setup.scale

        start = time.perf_counter()
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows == scale

        # Postgres baseline: full scan of an equivalent table.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, scale)

            pg_start = time.perf_counter()
            rows = conn.execute("SELECT id, name, value FROM perf_baseline").fetchall()
            pg_elapsed = time.perf_counter() - pg_start

            assert len(rows) == scale
            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "olap_full_scan",
                "all_cold_snapshotted",
                scale,
                elapsed,
                pg_elapsed,
            )
        )

    def test_filtered_scan(self, olap_setup, perf_recorder):
        """Bulk filtered scan via gRPC ReadData predicate pushdown."""
        client = olap_setup.client
        context = olap_setup.context
        scale = olap_setup.scale

        threshold = _half_match_threshold(scale)
        expected_match = scale // 2

        start = time.perf_counter()
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            filter=f"value < {threshold}",
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows == expected_match

        # Postgres baseline: the same predicate over an equivalent table.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, scale)

            pg_start = time.perf_counter()
            rows = conn.execute(
                "SELECT id, name, value FROM perf_baseline WHERE value < %s",
                (threshold,),
            ).fetchall()
            pg_elapsed = time.perf_counter() - pg_start

            assert len(rows) == expected_match
            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "olap_filtered_scan",
                "all_cold_snapshotted",
                scale,
                elapsed,
                pg_elapsed,
                result_rows=expected_match,
            )
        )
