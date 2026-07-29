"""Performance tests for write paths.

Measures write throughput into empty and populated tables, single vs
multi-transaction overhead, and Penca vs Postgres baseline.  Prints
markdown summary tables for docs/performance.md.

Run via ``just perf-test``.
"""

from __future__ import annotations

import time

import psycopg
import pytest

from .performance_helpers import (
    ROW_COUNT,
    PerfResult,
    create_postgres_baseline_table,
    drop_postgres_baseline_table,
    insert_and_commit,
    insert_postgres_baseline,
    make_client,
    pg_conninfo,
    setup_performance_schema,
)


class TestWritePerformance:
    """Write throughput under different conditions."""

    def test_write_into_empty_table(self, perf_recorder):
        """Baseline: write 100k rows into a fresh table (single tx)."""
        client = make_client()
        context = setup_performance_schema(client)

        start = time.perf_counter()
        insert_and_commit(client, context, offset=0, count=ROW_COUNT)
        elapsed = time.perf_counter() - start

        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert table.num_rows == ROW_COUNT

        # Postgres baseline write.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)

            pg_start = time.perf_counter()
            insert_postgres_baseline(conn, ROW_COUNT)
            pg_elapsed = time.perf_counter() - pg_start

            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "write_empty_table",
                "n/a",
                ROW_COUNT,
                elapsed,
                pg_elapsed,
                unit="write",
            )
        )

    def test_write_into_populated_table(self, perf_recorder):
        """Write 100k rows into a table that already has data."""
        client = make_client()
        context = setup_performance_schema(client)
        # Pre-populate with 100k rows (writes always go to hot regardless
        # of existing cold data, so no need to parametrize on table state).
        insert_and_commit(client, context, offset=0, count=ROW_COUNT)

        start = time.perf_counter()
        insert_and_commit(client, context, offset=ROW_COUNT, count=ROW_COUNT)
        elapsed = time.perf_counter() - start

        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert table.num_rows == ROW_COUNT * 2

        # Postgres baseline: write into a table that already has data.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=True) as conn:
            create_postgres_baseline_table(conn)
            insert_postgres_baseline(conn, ROW_COUNT, offset=0)

            pg_start = time.perf_counter()
            insert_postgres_baseline(conn, ROW_COUNT, offset=ROW_COUNT)
            pg_elapsed = time.perf_counter() - pg_start

            drop_postgres_baseline_table(conn)

        perf_recorder.record(
            PerfResult(
                "write_populated_table",
                "n/a",
                ROW_COUNT,
                elapsed,
                pg_elapsed,
                unit="write",
            )
        )

    @pytest.mark.parametrize("batch_count", [1, 10, 100])
    def test_write_multi_transaction(self, batch_count: int, perf_recorder):
        """Overhead of many small transactions vs one large (100k rows total)."""
        rows_per_batch = ROW_COUNT // batch_count
        client = make_client()
        context = setup_performance_schema(client)

        start = time.perf_counter()
        for batch_index in range(batch_count):
            offset = batch_index * rows_per_batch
            insert_and_commit(client, context, offset=offset, count=rows_per_batch)

        elapsed = time.perf_counter() - start

        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert table.num_rows == ROW_COUNT
        perf_recorder.record(
            PerfResult(
                "write_multi_tx",
                f"{batch_count}_batches",
                ROW_COUNT,
                elapsed,
                operations=batch_count,
                unit="tx",
            )
        )
