"""gRPC-API OLTP performance suite (point read + insert / update / delete).

First-pass operational yardstick over the native gRPC surface
(``ReadData`` / ``WriteData``) — deliberately dead simple and kept
separate from the OLAP suite because the two have opposite cost
structures (fixed-overhead-dominated point ops vs throughput-dominated
scans). Flight SQL and aggregates are explicit later passes; see
``oltp.md`` in this directory and CHA-416.

OLTP = latency-sensitive, fixed-overhead-dominated point ops:
- single-row point read via ``read_data(filter="id = <pk>")`` across
  the hot / cold-snapshotted / mixed tiers (the ``oltp_read_setup``
  fixture parametrizes the tier),
- the same point read via the CHA-398 ``ids`` PK-batch pushdown — the
  filter arm stays as the unpushed baseline the pushdown is measured
  against,
- single-row auto-commit insert via ``WriteData`` (writes always land
  hot, so there is no tier parametrization),
- single-row point update via ``WriteData`` upsert-on-existing and
  single-row point delete via a ``Mutation`` delete-tombstone, each over
  a table pre-seeded all-hot (the native write-op parity with the
  Flight SQL suite — there is no SQL-style ``UPDATE``/``DELETE`` on the
  native surface, so these are the upsert / delete-mutation paths).

All paths are the native gRPC ``ReadData`` / ``WriteData`` RPCs — no
Flight SQL, no server-side aggregate. Run via ``just perf-test grpc/oltp_test.py``.
"""

from __future__ import annotations

import time

import psycopg
import pyarrow as pa
from penca_client.types import Mutation

from ..performance_helpers import (
    PERF_SCHEMA,
    ROW_COUNT,
    PerfResult,
    create_postgres_baseline_table,
    drop_postgres_baseline_table,
    insert_and_commit,
    insert_postgres_baseline,
    pg_conninfo,
)

# Point-read target: a primary key in the middle of the table.
_POINT_TARGET_ID = ROW_COUNT // 2
# A point read is sub-millisecond of real work dominated by RPC
# overhead, so average the latency over many repetitions.
_POINT_READ_REPS = 100
# OLTP insert workload: one auto-commit RPC per row.
_INSERT_COUNT = 1_000
# OLTP update / delete workload: one auto-commit RPC per row, over a seeded table.
_WRITE_COUNT = 1_000

# Delete mutations carry primary-key columns only (perf table's PK is ``id``).
_PK_SCHEMA = pa.schema([pa.field("id", pa.int64())])


def _upsert_updated_row(client, context: dict[str, str], row_index: int) -> None:
    """Native ``WriteData`` upsert on an EXISTING PK with a new value.

    This is the gRPC update path — a latest-wins upsert over a row that already
    exists, distinct from the insert workload's upsert of fresh ids. Value is
    ``id * 2.0`` so it differs from the seeded ``id * 1.1`` (a same-value upsert
    would make the post-write assertion tautological)."""
    table = pa.table(
        {
            "id": [row_index],
            "name": [f"row_{row_index}"],
            "value": [float(row_index) * 2.0],
        },
        schema=PERF_SCHEMA,
    )
    client.write_data(
        None,
        Mutation(table_uuid=context["table_uuid"], upserts=table),
        author="perf-test",
        comment="perf oltp update",
        schema_uuid=context["schema_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def _delete_row(client, context: dict[str, str], row_index: int) -> None:
    """Native ``WriteData`` delete-tombstone on one PK (the gRPC delete path)."""
    table = pa.table({"id": [row_index]}, schema=_PK_SCHEMA)
    client.write_data(
        None,
        Mutation(table_uuid=context["table_uuid"], deletes=table),
        author="perf-test",
        comment="perf oltp delete",
        schema_uuid=context["schema_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def _measure_pg_point_read_baseline() -> float:
    """Postgres floor for one point-read arm: the same number of
    single-row PK lookups against a freshly seeded baseline table.
    Shared by the filter and ids arms — the floor is identical by
    construction."""
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


class TestOltpPerformance:
    """Single-client OLTP latency over the native gRPC API."""

    def test_point_read(self, oltp_read_setup, perf_recorder):
        """Single-row point read via gRPC ReadData PK-filter, across tiers."""
        client = oltp_read_setup.client
        context = oltp_read_setup.context

        start = time.perf_counter()
        for _ in range(_POINT_READ_REPS):
            table = client.read_data(
                schema_uuid=context["schema_uuid"],
                table_uuid=context["table_uuid"],
                branch_uuid=context["main_branch_uuid"],
                filter=f"id = {_POINT_TARGET_ID}",
            )

        elapsed = time.perf_counter() - start

        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [_POINT_TARGET_ID]

        pg_elapsed = _measure_pg_point_read_baseline()

        perf_recorder.record(
            PerfResult(
                "oltp_point_read",
                oltp_read_setup.system_state.value,
                _POINT_READ_REPS,
                elapsed,
                pg_elapsed,
                result_rows=1,
                operations=_POINT_READ_REPS,
            )
        )

    def test_point_read_ids(self, oltp_read_setup, perf_recorder):
        """Single-row point read via the CHA-398 ids PK-batch pushdown.

        Same target row and rep count as ``test_point_read`` so the two
        arms are directly comparable: the filter arm pays the full
        latest-wins dedup; this arm restricts every tier probe to the
        named row_uuid (served by the ``(row_uuid, tx_uuid)`` index on
        the hot logs)."""
        client = oltp_read_setup.client
        context = oltp_read_setup.context
        ids = pa.table(
            {"id": [_POINT_TARGET_ID]},
            schema=pa.schema([pa.field("id", pa.int64())]),
        )

        start = time.perf_counter()
        for _ in range(_POINT_READ_REPS):
            table = client.read_data(
                schema_uuid=context["schema_uuid"],
                table_uuid=context["table_uuid"],
                branch_uuid=context["main_branch_uuid"],
                ids=ids,
            )

        elapsed = time.perf_counter() - start

        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [_POINT_TARGET_ID]

        pg_elapsed = _measure_pg_point_read_baseline()

        perf_recorder.record(
            PerfResult(
                "oltp_point_read_ids",
                oltp_read_setup.system_state.value,
                _POINT_READ_REPS,
                elapsed,
                pg_elapsed,
                result_rows=1,
                operations=_POINT_READ_REPS,
            )
        )

    def test_single_row_insert(self, oltp_insert_setup, perf_recorder):
        """Single-row auto-commit inserts via gRPC WriteData (hot tier)."""
        client = oltp_insert_setup.client
        context = oltp_insert_setup.context

        start = time.perf_counter()
        for row_index in range(_INSERT_COUNT):
            insert_and_commit(client, context, offset=row_index, count=1)

        elapsed = time.perf_counter() - start

        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert table.num_rows == _INSERT_COUNT

        # Postgres baseline: N single-row INSERTs, each in its own tx.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=False) as conn:
            create_postgres_baseline_table(conn)
            conn.commit()

            pg_start = time.perf_counter()
            for row_index in range(_INSERT_COUNT):
                conn.execute(
                    "INSERT INTO perf_baseline (id, name, value) VALUES (%s, %s, %s)",
                    (row_index, f"row_{row_index}", float(row_index) * 1.1),
                )
                conn.commit()

            pg_elapsed = time.perf_counter() - pg_start

            drop_postgres_baseline_table(conn)
            conn.commit()

        perf_recorder.record(
            PerfResult(
                "oltp_insert",
                "hot",
                _INSERT_COUNT,
                elapsed,
                pg_elapsed,
                operations=_INSERT_COUNT,
                unit="insert",
            )
        )

    def test_single_row_update(self, oltp_update_setup, perf_recorder):
        """Single-row point updates via gRPC WriteData upsert-on-existing (hot)."""
        client = oltp_update_setup.client
        context = oltp_update_setup.context

        start = time.perf_counter()
        for row_index in range(_WRITE_COUNT):
            _upsert_updated_row(client, context, row_index)

        elapsed = time.perf_counter() - start

        # A row where the seeded value (1.1) and updated value (2.0) differ, so
        # the assertion fails if the upsert silently no-ops. id=0 would be 0.0
        # either way (seeded 0*1.1 == updated 0*2.0) — a tautology.
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            filter="id = 1",
        )
        assert table.column("value").to_pylist() == [2.0]

        # Postgres baseline: N point UPDATEs on a seeded baseline table, each
        # committed in its own tx.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=False) as conn:
            create_postgres_baseline_table(conn)
            conn.commit()
            insert_postgres_baseline(conn, ROW_COUNT)
            conn.commit()

            pg_start = time.perf_counter()
            for row_index in range(_WRITE_COUNT):
                conn.execute(
                    "UPDATE perf_baseline SET value = %s WHERE id = %s",
                    (float(row_index) * 2.0, row_index),
                )
                conn.commit()

            pg_elapsed = time.perf_counter() - pg_start

            drop_postgres_baseline_table(conn)
            conn.commit()

        perf_recorder.record(
            PerfResult(
                "oltp_update",
                "hot",
                _WRITE_COUNT,
                elapsed,
                pg_elapsed,
                operations=_WRITE_COUNT,
                unit="update",
            )
        )

    def test_single_row_delete(self, oltp_delete_setup, perf_recorder):
        """Single-row point deletes via gRPC WriteData delete-tombstone (hot)."""
        client = oltp_delete_setup.client
        context = oltp_delete_setup.context

        start = time.perf_counter()
        for row_index in range(_WRITE_COUNT):
            _delete_row(client, context, row_index)

        elapsed = time.perf_counter() - start

        # The seeded table shrank by exactly the deleted rows.
        table = client.read_data(
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert table.num_rows == ROW_COUNT - _WRITE_COUNT

        # Postgres baseline: N point DELETEs on a seeded baseline table, each
        # committed in its own tx.
        conninfo = pg_conninfo()
        with psycopg.connect(conninfo, autocommit=False) as conn:
            create_postgres_baseline_table(conn)
            conn.commit()
            insert_postgres_baseline(conn, ROW_COUNT)
            conn.commit()

            pg_start = time.perf_counter()
            for row_index in range(_WRITE_COUNT):
                conn.execute("DELETE FROM perf_baseline WHERE id = %s", (row_index,))
                conn.commit()

            pg_elapsed = time.perf_counter() - pg_start

            drop_postgres_baseline_table(conn)
            conn.commit()

        perf_recorder.record(
            PerfResult(
                "oltp_delete",
                "hot",
                _WRITE_COUNT,
                elapsed,
                pg_elapsed,
                operations=_WRITE_COUNT,
                unit="delete",
            )
        )
