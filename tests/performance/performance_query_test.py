"""Performance tests for query/read paths across all system states.

Parameterizes reads across every permutation of hot/cold/snapshotted
storage.  Includes Postgres baseline comparisons and prints markdown
summary tables for docs/performance.md.

Uses a class-scoped ``query_setup`` fixture (from conftest.py) so all
five read tests share the same pre-built system state — reducing 40
expensive setup calls to 8.

Run via ``just perf-test``.
"""

from __future__ import annotations

import os
import time

import psycopg
import pyarrow as pa
import pytest
from penca_client._time import micros_to_datetime

from .performance_helpers import (
    PERF_SCHEMA,
    ROW_COUNT,
    PerfResult,
    create_postgres_baseline_table,
    drop_postgres_baseline_table,
    insert_postgres_baseline,
    pg_conninfo,
)

_FILTER_TARGET_ID = ROW_COUNT // 2
_FILTER_TARGET_NAME = f"row_{_FILTER_TARGET_ID}"

# Baseline values are float(i) * 1.1 for i in 0..ROW_COUNT, so
# `value < 1100.0` matches exactly 1000 rows (i ∈ [0, 999]). Picking a
# 1000-row match lets us compare against Postgres's bulk-return path
# where psycopg's per-tuple construction dominates, as opposed to the
# 1-row case where PG's baseline is mostly scan cost. Together they
# show whether Penca's absolute overhead or the Postgres baseline is
# the thing changing across workloads.
_BULK_FILTER_THRESHOLD = 1100.0
_BULK_FILTER_MATCH_COUNT = 1000
_requires_rust = pytest.mark.skipif(
    os.environ.get("PENCA_BACKEND", "") != "rust",
    reason="Flight SQL requires --backend rust",
)


@pytest.mark.usefixtures("query_setup")
class TestQueryPerformance:
    """Read performance across every system state permutation."""

    def test_read_data(self, query_setup, perf_recorder):
        """Full table scan via read_data."""
        client = query_setup.client
        context = query_setup.context
        state_info = query_setup.state_info

        start = time.perf_counter()
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows == state_info.total_rows

        # Postgres baseline read.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, ROW_COUNT)

            pg_start = time.perf_counter()
            rows = conn.execute("SELECT id, name, value FROM perf_baseline").fetchall()
            pg_table = pa.table(
                {
                    "id": [r[0] for r in rows],
                    "name": [r[1] for r in rows],
                    "value": [r[2] for r in rows],
                },
                schema=PERF_SCHEMA,
            )
            pg_elapsed = time.perf_counter() - pg_start

            assert pg_table.num_rows == ROW_COUNT
            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "read_data",
                query_setup.system_state.value,
                ROW_COUNT,
                elapsed,
                pg_elapsed,
            )
        )

    def test_read_data_time_travel(self, query_setup, perf_recorder):
        """Time-travel read (as_of first commit) via read_data."""
        client = query_setup.client
        context = query_setup.context
        state_info = query_setup.state_info

        first_response = state_info.committed_txs[0]
        as_of = micros_to_datetime(first_response.commit_micros)

        start = time.perf_counter()
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            as_of=as_of,
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows > 0
        perf_recorder.record(
            PerfResult(
                "read_data_time_travel",
                query_setup.system_state.value,
                table.num_rows,
                elapsed,
            )
        )

    @_requires_rust
    @pytest.mark.parametrize("match_count", [1, _BULK_FILTER_MATCH_COUNT])
    def test_query_filter_non_pk(self, query_setup, match_count, perf_recorder):
        """Predicate pushdown on a non-PK column via Flight SQL.

        Exercises the full CHA-142 path: DataFusion translates the
        ``WHERE`` clause to a bare SQL fragment via the unparser,
        ``PencaTableProvider::scan`` forwards it as
        ``ReadDataRequest.filter``, and ``stream_merged`` appends it to
        the resolved-upsert SQL in both tiers. The all-hot state takes
        the ``read_data`` fast path that skips ``stream_merged`` entirely.

        Parametrized over ``match_count`` so the summary captures both
        the selective (1 row) and bulk (1000 rows) result-set shapes —
        Penca's per-query fixed overhead is roughly constant across
        both, while Postgres's wall time scales with rows returned
        (psycopg tuple construction), so the ratio is noticeably
        tighter on the bulk case.
        """
        client = query_setup.client
        context = query_setup.context
        state_info = query_setup.state_info

        fqn = (
            f"{context['catalog_name']}."
            f"{context['schema_name']}."
            f"{context['table_name']}"
        )

        if match_count == 1:
            penca_where = f"name = '{_FILTER_TARGET_NAME}'"
            pg_where = "name = %s"
            pg_params: tuple = (_FILTER_TARGET_NAME,)
        else:
            penca_where = f"value < {_BULK_FILTER_THRESHOLD}"
            pg_where = "value < %s"
            pg_params = (_BULK_FILTER_THRESHOLD,)

        start = time.perf_counter()
        table = client.execute_query(
            f"SELECT id, name, value FROM {fqn} WHERE {penca_where}"
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows == match_count

        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, ROW_COUNT)

            pg_start = time.perf_counter()
            rows = conn.execute(
                f"SELECT id, name, value FROM perf_baseline WHERE {pg_where}",
                pg_params,
            ).fetchall()
            pg_elapsed = time.perf_counter() - pg_start

            assert len(rows) == match_count
            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                f"query_filter_non_pk[match_{match_count}]",
                query_setup.system_state.value,
                state_info.total_rows,
                elapsed,
                pg_elapsed,
                result_rows=match_count,
            )
        )

    @_requires_rust
    def test_query_aggregate(self, query_setup, perf_recorder):
        """Server-side aggregate via Flight SQL."""
        client = query_setup.client
        context = query_setup.context
        state_info = query_setup.state_info

        fqn = (
            f"{context['catalog_name']}."
            f"{context['schema_name']}."
            f"{context['table_name']}"
        )

        start = time.perf_counter()
        table = client.execute_query(
            f"SELECT COUNT(*) AS n, SUM(value) AS s, AVG(value) AS a FROM {fqn}"
        )
        elapsed = time.perf_counter() - start

        assert table.num_rows == 1
        assert table.column("n").to_pylist() == [state_info.total_rows]

        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, ROW_COUNT)

            pg_start = time.perf_counter()
            row = conn.execute(
                "SELECT COUNT(*), SUM(value), AVG(value) FROM perf_baseline"
            ).fetchone()
            pg_elapsed = time.perf_counter() - pg_start

            assert row is not None and row[0] == ROW_COUNT
            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "query_aggregate",
                query_setup.system_state.value,
                state_info.total_rows,
                elapsed,
                pg_elapsed,
                result_rows=1,
            )
        )
