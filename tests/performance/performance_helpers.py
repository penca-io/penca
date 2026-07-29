"""Shared helpers for performance tests.

Prerequisites:
    - Docker daemon must be running (Docker Desktop or dockerd).
    - Run via 'just perf-test' which sets PENCA_DB_* and
      PENCA_OBJECT_STORAGE_* automatically.
"""

from __future__ import annotations

import dataclasses
import enum
from uuid import uuid4

import pyarrow as pa
from penca_client.arrow import batch_to_ipc_bytes
from penca_client.client import PencaClient
from penca_client.config import ClientSettings, DbSettings
from penca_client.naming import system_schemas_table_uuid, system_tables_table_uuid
from penca_client.types import Mutation

ROW_COUNT = 100_000


PERF_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.utf8()),
        pa.field("value", pa.float64()),
    ]
)


def make_batch(offset: int, count: int) -> pa.RecordBatch:
    """Create a RecordBatch with ``count`` rows starting at ``offset``."""
    return pa.record_batch(
        {
            "id": list(range(offset, offset + count)),
            "name": [f"row_{i}" for i in range(offset, offset + count)],
            "value": [float(i) * 1.1 for i in range(offset, offset + count)],
        },
        schema=PERF_SCHEMA,
    )


def make_client(
    storage_format: int | None = None,  # noqa: ARG001
):
    """Return a PencaClient talking to the configured backend.

    ``storage_format`` is accepted for parametrization compatibility but
    ignored — storage format is now a server-side config set at backend
    startup.
    """
    return PencaClient.from_settings(ClientSettings())  # ty: ignore[missing-argument]


def _drive_system_tables_cold(client, context: dict[str, str]) -> None:
    """Persist -> Snapshot -> Purge ``__penca_system__.{tables,schemas}`` so a
    table-identifier resolve serves from the cold snapshot segment list — the
    read CHA-472's shared ``list_cache_for`` gate caches on the query and write
    paths.

    The test/perf profile disables the lifecycle scheduler
    (``SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS=-1``), so without this
    the system tables
    stay HOT (``hot_min == 0`` => the resolve reads ``__penca_system__.tables``
    straight from Postgres and the snapshot-list cache is never consulted),
    making the OLTP perf numbers blind to the CHA-472 cache. Production's
    scheduler keeps the system tables cold the same way; this mirrors the
    integration suite's ``_persist_purge_system_tables_past_grace``. CHA-444
    dropped the hot-purge grace, so each persist->snapshot->purge is
    self-contained with no wait.
    """
    catalog_uuid = context["catalog_uuid"]
    for sys_uuid in (
        system_tables_table_uuid(catalog_uuid),
        system_schemas_table_uuid(catalog_uuid),
    ):
        kw = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": context["schema_uuid"],
            "branch_uuid": context["main_branch_uuid"],
            "table_uuid": sys_uuid,
        }
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)


def setup_performance_schema(client) -> dict[str, str]:
    """Create catalog/schema/table for perf tests.

    Returns dict with schema_uuid, table_uuid, catalog_uuid,
    main_branch_uuid plus the string names (needed for Flight SQL which
    uses 3-part identifiers).
    """
    catalog_name = f"perf_cat_{uuid4().hex[:8]}"
    schema_name = "perf_schema"
    table_name = "perf_table"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    # Rebind the client's default catalog so subsequent calls
    # (create_table, write_data, persist, snapshot, read_data, etc.)
    # don't fall back to the bootstrap "public" catalog and silently
    # target the wrong partition tree.
    client.catalog = catalog_name
    schema_uuid = client.create_schema(
        schema_name,
        catalog_uuid=catalog_uuid,
        author="perf-test",
        comment="perf setup schema",
    )
    table_uuid = client.create_table(
        table_name,
        PERF_SCHEMA,
        primary_keys=["id"],
        schema_uuid=schema_uuid,
        author="perf-test",
        comment="perf setup table",
    )
    context = {
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "catalog_uuid": catalog_uuid,
        "main_branch_uuid": main_branch_uuid,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
    }
    # CHA-472: drive the system metadata tables cold AFTER the DDL so every perf
    # op's table-identifier resolve consults the cold snapshot segment list (and
    # the shared snapshot-list cache). Without this the system tables stay hot
    # and the resolve bypasses the cache entirely — see _drive_system_tables_cold.
    _drive_system_tables_cold(client, context)
    return context


def insert_and_commit(client, context: dict[str, str], offset: int, count: int):
    """Upsert ``count`` rows starting at ``offset`` via auto-commit WriteData.

    Returns the auto-commit ``WriteDataResponse`` carrying
    ``commit_micros``.
    """
    batch = make_batch(offset, count)
    return client.write_data(
        None,
        Mutation(
            table_uuid=context["table_uuid"],
            upserts=pa.Table.from_batches([batch]),
        ),
        author="perf-test",
        comment="perf insert_and_commit",
        schema_uuid=context["schema_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


class SystemState(enum.Enum):
    """Represents where data lives in the tiered storage architecture."""

    ALL_HOT = "all_hot"
    ALL_COLD_UNSNAPSHOTTED = "all_cold_unsnapshotted"
    ALL_COLD_SNAPSHOTTED = "all_cold_snapshotted"
    COLD_MIXED = "cold_snapshotted_and_unsnapshotted"
    HOT_AND_COLD_UNSNAPSHOTTED = "hot_and_cold_unsnapshotted"
    HOT_AND_COLD_SNAPSHOTTED = "hot_and_cold_snapshotted"
    HOT_AND_COLD_MIXED = "hot_and_cold_mixed"
    REALISTIC_TIMESERIES = "realistic_timeseries"


@dataclasses.dataclass
class SystemStateInfo:
    """Row distribution after ``prepare_system_state``."""

    total_rows: int
    hot_rows: int
    cold_unsnapshotted_rows: int
    cold_snapshotted_rows: int
    committed_txs: list


def _persist(client, context: dict[str, str]):
    client.persist(
        schema_uuid=context["schema_uuid"],
        table_uuid=context["table_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def _snapshot(client, context: dict[str, str]):
    client.snapshot(
        schema_uuid=context["schema_uuid"],
        table_uuid=context["table_uuid"],
        branch_uuid=context["main_branch_uuid"],
    )


def prepare_system_state(
    client,
    context: dict[str, str],
    state: SystemState,
    total_row_count: int,
) -> SystemStateInfo:
    """Drive a table into the specified system state with ``total_row_count`` rows.

    Enforces constraint: hot rows <= cold rows (unless no cold data).
    """
    committed_txs: list = []

    if state == SystemState.ALL_HOT:
        committed_txs.append(insert_and_commit(client, context, 0, total_row_count))
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=total_row_count,
            cold_unsnapshotted_rows=0,
            cold_snapshotted_rows=0,
            committed_txs=committed_txs,
        )

    if state == SystemState.ALL_COLD_UNSNAPSHOTTED:
        committed_txs.append(insert_and_commit(client, context, 0, total_row_count))
        _persist(client, context)
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=0,
            cold_unsnapshotted_rows=total_row_count,
            cold_snapshotted_rows=0,
            committed_txs=committed_txs,
        )

    if state == SystemState.ALL_COLD_SNAPSHOTTED:
        committed_txs.append(insert_and_commit(client, context, 0, total_row_count))
        _persist(client, context)
        _snapshot(client, context)
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=0,
            cold_unsnapshotted_rows=0,
            cold_snapshotted_rows=total_row_count,
            committed_txs=committed_txs,
        )

    if state == SystemState.COLD_MIXED:
        # Half snapshotted, half unsnapshotted, all cold.
        half = total_row_count // 2
        remainder = total_row_count - half
        committed_txs.append(insert_and_commit(client, context, 0, half))
        _persist(client, context)
        _snapshot(client, context)
        committed_txs.append(insert_and_commit(client, context, half, remainder))
        _persist(client, context)
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=0,
            cold_unsnapshotted_rows=remainder,
            cold_snapshotted_rows=half,
            committed_txs=committed_txs,
        )

    if state == SystemState.HOT_AND_COLD_UNSNAPSHOTTED:
        # 3/4 cold unsnapshotted, 1/4 hot.
        cold_count = (total_row_count * 3) // 4
        hot_count = total_row_count - cold_count
        committed_txs.append(insert_and_commit(client, context, 0, cold_count))
        _persist(client, context)
        committed_txs.append(insert_and_commit(client, context, cold_count, hot_count))
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=hot_count,
            cold_unsnapshotted_rows=cold_count,
            cold_snapshotted_rows=0,
            committed_txs=committed_txs,
        )

    if state == SystemState.HOT_AND_COLD_SNAPSHOTTED:
        # 3/4 cold snapshotted, 1/4 hot.
        cold_count = (total_row_count * 3) // 4
        hot_count = total_row_count - cold_count
        committed_txs.append(insert_and_commit(client, context, 0, cold_count))
        _persist(client, context)
        _snapshot(client, context)
        committed_txs.append(insert_and_commit(client, context, cold_count, hot_count))
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=hot_count,
            cold_unsnapshotted_rows=0,
            cold_snapshotted_rows=cold_count,
            committed_txs=committed_txs,
        )

    if state == SystemState.HOT_AND_COLD_MIXED:
        # 1/3 snapshotted, 1/3 unsnapshotted (cold), 1/3 hot.
        third = total_row_count // 3
        snap_count = third
        unsnap_count = third
        hot_count = total_row_count - snap_count - unsnap_count
        # Phase 1: insert -> persist -> snapshot (snapshotted cold).
        committed_txs.append(insert_and_commit(client, context, 0, snap_count))
        _persist(client, context)
        _snapshot(client, context)
        # Phase 2: insert -> persist (unsnapshotted cold).
        committed_txs.append(
            insert_and_commit(client, context, snap_count, unsnap_count)
        )
        _persist(client, context)
        # Phase 3: insert (hot).
        committed_txs.append(
            insert_and_commit(client, context, snap_count + unsnap_count, hot_count)
        )
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=hot_count,
            cold_unsnapshotted_rows=unsnap_count,
            cold_snapshotted_rows=snap_count,
            committed_txs=committed_txs,
        )

    if state == SystemState.REALISTIC_TIMESERIES:
        # Models time-series financial data: bulk history is snapshotted,
        # today's writes still in hot. 95% cold snapshotted, 5% hot.
        snap_count = (total_row_count * 95) // 100
        hot_count = total_row_count - snap_count
        # Phase 1: bulk historical data -> persist -> snapshot.
        committed_txs.append(insert_and_commit(client, context, 0, snap_count))
        _persist(client, context)
        _snapshot(client, context)
        # Phase 2: today's writes still in hot.
        committed_txs.append(insert_and_commit(client, context, snap_count, hot_count))
        return SystemStateInfo(
            total_rows=total_row_count,
            hot_rows=hot_count,
            cold_unsnapshotted_rows=0,
            cold_snapshotted_rows=snap_count,
            committed_txs=committed_txs,
        )

    raise ValueError(f"Unknown system state: {state}")


def create_postgres_baseline_table(conn) -> None:
    """Create a baseline Postgres table matching PERF_SCHEMA."""
    conn.execute("DROP TABLE IF EXISTS perf_baseline")
    conn.execute(
        """
        CREATE TABLE perf_baseline (
            id   BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL
        )
        """
    )


def insert_postgres_baseline(conn, row_count: int, offset: int = 0) -> None:
    """Insert rows into the baseline Postgres table via pipelined executemany."""
    batch = make_batch(offset, row_count)
    payload = batch_to_ipc_bytes(batch)
    reader = pa.ipc.open_stream(payload)
    decoded = reader.read_all()
    ids = decoded.column("id").to_pylist()
    names = decoded.column("name").to_pylist()
    values = decoded.column("value").to_pylist()
    params_seq = [(ids[i], names[i], values[i]) for i in range(decoded.num_rows)]
    with conn.cursor() as cur:
        with conn.pipeline():
            cur.executemany(
                "INSERT INTO perf_baseline (id, name, value) VALUES (%s, %s, %s)",
                params_seq,
            )


def drop_postgres_baseline_table(conn) -> None:
    """Drop the baseline Postgres table."""
    conn.execute("DROP TABLE IF EXISTS perf_baseline")


@dataclasses.dataclass
class PerfResult:
    """A single performance measurement.

    ``row_count`` is the throughput-relevant count — the size the
    operation worked against, and the denominator for rows/s. For scan
    ops that's the table size; for writes it's the rows inserted.

    ``result_rows`` is set when it differs meaningfully from
    ``row_count`` — today that's filter + aggregate queries which scan
    a 100k-row table but return 1 or 1000 rows. Making the distinction
    explicit surfaces a quirk in ratio comparisons against Postgres:
    PG's wall time scales with rows *returned* (tuple construction in
    psycopg), while Penca's fixed per-query overhead is dominated by
    RPC setup. Showing both numbers in the summary table makes the
    ratio easier to reason about.

    ``operations`` and ``unit`` make the work-unit explicit (CHA-438):
    ``elapsed_seconds`` spans ``operations`` repetitions of one
    ``unit`` (a point-read test loops 100 queries; a bulk write is one
    write). ``postgres_baseline_seconds`` always spans the SAME
    operation count, so per-op normalization compares like-for-like.
    Report/dashboard consumers derive ms-per-op and ops/s from these;
    they are deliberately NOT derived properties here because the same
    formula must also apply to history rows read back from SQLite
    (it lives in scripts/perf/metrics.py). ``row_count`` keeps its
    meaning above regardless of ``operations``.
    """

    operation: str
    system_state: str
    row_count: int
    elapsed_seconds: float
    postgres_baseline_seconds: float | None = None
    result_rows: int | None = None
    operations: int = 1
    unit: str = "query"

    @property
    def rows_per_second(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0

        return self.row_count / self.elapsed_seconds


def pg_conninfo() -> str:
    """Return Postgres connection string (for baseline comparisons)."""
    return DbSettings().conninfo  # ty: ignore[missing-argument]
