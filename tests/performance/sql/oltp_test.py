"""SQL-API (Flight SQL) OLTP performance suite — single-statement autocommit ops.

The SQL-path sibling of the gRPC OLTP suite (``tests/performance/grpc/oltp_test.py``,
CHA-416). It drives the *same* four point ops, but each as a single **autocommit
Flight SQL statement** over the harness's ADBC client rather than a native
``ReadData`` / ``WriteData`` RPC — so the delta against the gRPC suite isolates
the SQL-layer overhead (parse → plan → ADBC prepared-statement wire actions →
metadata resolve). This is the SQL-API later pass CHA-416 explicitly parked, and
the dead-simple single-statement companion to CHA-501's 7-statement pgbench
transaction. See ``oltp.md`` in this directory.

OLTP = latency-sensitive, fixed-overhead-dominated point ops:
- ``sql_point_read`` — ``SELECT … WHERE id = <lit>`` across the hot /
  cold-snapshotted / mixed tiers (the ``sql_oltp_read_setup`` fixture
  parametrizes the tier). Takes the ADBC PREPARED query path
  (``ActionCreatePreparedStatement`` → ``DoGet(CommandPreparedStatementQuery)``).
- ``sql_insert`` / ``sql_update`` / ``sql_delete`` — single-row DML via
  ``DoPutStatementUpdate``; writes always land hot, so no tier parametrization.

Each op is repeated N× and averaged (fixed-overhead-dominated; ``rows/s`` reads
as ops/s), mirroring the gRPC suite. Autocommit only — NOT a ``BEGIN … COMMIT``
transaction (that is pgbench / CHA-501). Flight SQL requires the Rust backend, so
the suite is skipped unless ``PENCA_BACKEND=rust``. Run via
``PENCA_BACKEND=rust just perf-test sql/oltp_test.py``.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable

import psycopg
import pytest

from ..performance_helpers import (
    ROW_COUNT,
    PerfResult,
    create_postgres_baseline_table,
    drop_postgres_baseline_table,
    insert_postgres_baseline,
    pg_conninfo,
)

# Point-read target: a primary key in the middle of the table.
_POINT_TARGET_ID = ROW_COUNT // 2
# A point op is sub-millisecond of real work dominated by the Flight SQL round
# trip, so average the latency over many repetitions. Overridable for a
# constrained host (a full write pass is _WRITE_COUNT synchronous round trips).
_POINT_READ_REPS = int(os.environ.get("PERF_SQL_READ_REPS", "100"))
_WRITE_COUNT = int(os.environ.get("PERF_SQL_WRITE_COUNT", "1000"))

# Flight SQL requires the Rust backend (the Python backend has no SQL surface),
# mirroring performance_pgbench_test.py.
_requires_rust = pytest.mark.skipif(
    os.environ.get("PENCA_BACKEND", "") != "rust",
    reason="Flight SQL requires --backend rust",
)


def _fqn(context: dict[str, str]) -> str:
    """The 3-part identifier Flight SQL needs (catalog.schema.table)."""
    return f"{context['catalog_name']}.{context['schema_name']}.{context['table_name']}"


def _measure_pg_point_read_baseline() -> float:
    """Postgres floor for the point-read arm: the same number of single-row PK
    lookups against a freshly seeded baseline table."""
    conninfo = pg_conninfo()
    with psycopg.connect(conninfo, autocommit=True) as conn:
        create_postgres_baseline_table(conn)
        insert_postgres_baseline(conn, ROW_COUNT)

        pg_start = time.perf_counter()
        for _ in range(_POINT_READ_REPS):
            rows = conn.execute(
                "SELECT id, name, value FROM perf_baseline WHERE id = %s",
                (_POINT_TARGET_ID,),
            ).fetchall()

        pg_elapsed = time.perf_counter() - pg_start

        assert len(rows) == 1
        drop_postgres_baseline_table(conn)

    return pg_elapsed


def _pg_write_baseline(
    seed_rows: int,
    timed_writes: Callable[[psycopg.Connection], float],
) -> float:
    """Postgres floor for a single-statement write op.

    Seeds ``seed_rows`` rows (0 = start empty, for INSERT), then hands the
    connection to ``timed_writes`` — which issues ``_WRITE_COUNT`` single-row
    statements, each committed in its own tx (the Postgres analog of Penca
    autocommit: one Flight SQL statement per tx) — and returns its elapsed
    seconds. The connect / create / seed / drop scaffold is shared here; the
    per-op statement stays a literal in ``timed_writes`` (psycopg types the
    query as ``LiteralString``).
    """
    conninfo = pg_conninfo()
    with psycopg.connect(conninfo, autocommit=False) as conn:
        create_postgres_baseline_table(conn)
        conn.commit()
        if seed_rows:
            insert_postgres_baseline(conn, seed_rows)
            conn.commit()

        pg_elapsed = timed_writes(conn)

        drop_postgres_baseline_table(conn)
        conn.commit()

    return pg_elapsed


def _pg_insert_writes(conn: psycopg.Connection) -> float:
    """Time ``_WRITE_COUNT`` single-row INSERTs, each in its own tx."""
    start = time.perf_counter()
    for row_index in range(_WRITE_COUNT):
        conn.execute(
            "INSERT INTO perf_baseline (id, name, value) VALUES (%s, %s, %s)",
            (row_index, f"row_{row_index}", float(row_index) * 1.1),
        )
        conn.commit()

    return time.perf_counter() - start


def _pg_update_writes(conn: psycopg.Connection) -> float:
    """Time ``_WRITE_COUNT`` single-row point UPDATEs, each in its own tx."""
    start = time.perf_counter()
    for row_index in range(_WRITE_COUNT):
        conn.execute(
            "UPDATE perf_baseline SET value = %s WHERE id = %s",
            (float(row_index) * 2.0, row_index),
        )
        conn.commit()

    return time.perf_counter() - start


def _pg_delete_writes(conn: psycopg.Connection) -> float:
    """Time ``_WRITE_COUNT`` single-row point DELETEs, each in its own tx."""
    start = time.perf_counter()
    for row_index in range(_WRITE_COUNT):
        conn.execute("DELETE FROM perf_baseline WHERE id = %s", (row_index,))
        conn.commit()

    return time.perf_counter() - start


@_requires_rust
class TestSqlOltpPerformance:
    """Single-client OLTP latency over the Flight SQL API (ADBC driver)."""

    def test_point_read(self, sql_oltp_read_setup, perf_recorder):
        """Single-row point read via an autocommit Flight SQL SELECT, across tiers."""
        client = sql_oltp_read_setup.client
        fqn = _fqn(sql_oltp_read_setup.context)
        select = f"SELECT id, name, value FROM {fqn} WHERE id = {_POINT_TARGET_ID}"

        start = time.perf_counter()
        for _ in range(_POINT_READ_REPS):
            table = client.execute_query(select)

        elapsed = time.perf_counter() - start

        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [_POINT_TARGET_ID]

        pg_elapsed = _measure_pg_point_read_baseline()

        perf_recorder.record(
            PerfResult(
                "sql_point_read",
                sql_oltp_read_setup.system_state.value,
                _POINT_READ_REPS,
                elapsed,
                pg_elapsed,
                result_rows=1,
                operations=_POINT_READ_REPS,
            )
        )

    def test_single_row_insert(self, sql_insert_setup, perf_recorder):
        """Single-row autocommit inserts via Flight SQL DoPutStatementUpdate (hot)."""
        client = sql_insert_setup.client
        fqn = _fqn(sql_insert_setup.context)

        start = time.perf_counter()
        for row_index in range(_WRITE_COUNT):
            client.execute_update(
                f"INSERT INTO {fqn} (id, name, value) "
                f"VALUES ({row_index}, 'row_{row_index}', {float(row_index) * 1.1})"
            )

        elapsed = time.perf_counter() - start

        count = client.execute_query(f"SELECT COUNT(*) AS c FROM {fqn}")
        assert count.column("c").to_pylist() == [_WRITE_COUNT]

        pg_elapsed = _pg_write_baseline(0, _pg_insert_writes)

        perf_recorder.record(
            PerfResult(
                "sql_insert",
                "hot",
                _WRITE_COUNT,
                elapsed,
                pg_elapsed,
                operations=_WRITE_COUNT,
                unit="insert",
            )
        )

    def test_single_row_update(self, sql_update_setup, perf_recorder):
        """Single-row point updates via Flight SQL (the read-modify-write path, hot)."""
        client = sql_update_setup.client
        fqn = _fqn(sql_update_setup.context)

        start = time.perf_counter()
        for row_index in range(_WRITE_COUNT):
            client.execute_update(
                f"UPDATE {fqn} SET value = {float(row_index) * 2.0} WHERE id = {row_index}"
            )

        elapsed = time.perf_counter() - start

        # Check a row whose seeded value (1.1) and updated value (2.0) differ, so
        # the assertion fails if the UPDATE silently no-ops. id=0 would be 0.0
        # either way (seeded 0*1.1 == updated 0*2.0) — a tautology.
        check = client.execute_query(f"SELECT value FROM {fqn} WHERE id = 1")
        assert check.column("value").to_pylist() == [2.0]

        pg_elapsed = _pg_write_baseline(ROW_COUNT, _pg_update_writes)

        perf_recorder.record(
            PerfResult(
                "sql_update",
                "hot",
                _WRITE_COUNT,
                elapsed,
                pg_elapsed,
                operations=_WRITE_COUNT,
                unit="update",
            )
        )

    def test_single_row_delete(self, sql_delete_setup, perf_recorder):
        """Single-row point deletes via Flight SQL (hot)."""
        client = sql_delete_setup.client
        fqn = _fqn(sql_delete_setup.context)

        start = time.perf_counter()
        for row_index in range(_WRITE_COUNT):
            client.execute_update(f"DELETE FROM {fqn} WHERE id = {row_index}")

        elapsed = time.perf_counter() - start

        # The seeded table shrank by exactly the deleted rows.
        count = client.execute_query(f"SELECT COUNT(*) AS c FROM {fqn}")
        assert count.column("c").to_pylist() == [ROW_COUNT - _WRITE_COUNT]

        pg_elapsed = _pg_write_baseline(ROW_COUNT, _pg_delete_writes)

        perf_recorder.record(
            PerfResult(
                "sql_delete",
                "hot",
                _WRITE_COUNT,
                elapsed,
                pg_elapsed,
                operations=_WRITE_COUNT,
                unit="delete",
            )
        )
