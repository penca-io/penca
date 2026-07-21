"""CHA-421: concurrent cold reads must not cross-contaminate.

Two simultaneous reads over different tables each go through ``stream_merged`` →
a per-unit DataFusion ``SessionContext`` derived from the process-wide template
(``penca_dl::derive_cold_session``) with a FRESH catalog. These tests guard
that the catalog isolation holds end-to-end under concurrency: they pass on
main and after CHA-421 (regression guards), and would catch a naive impl that
shared one ``catalog_list`` across derives — concurrent reads would then
collide on the fixed cold table names (``l``, ``exclusion``, ``upsert_log``,
``delete_log``).

sql-server side: the per-connection ``ctx`` is itself derived from a process
template via ``ConnSessionFactory::build_ctx`` (already template-derived before
CHA-421, so this ticket makes no production change there); the Flight SQL test
guards that connection-level isolation under concurrent connections.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pyarrow as pa
from adbc_driver_flightsql.dbapi import connect as flight_sql_connect
from penca_client import Mutation
from penca_client.config import ClientSettings

from .integration_helpers import USER_SCHEMA, make_client


def _make_cold_table(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_name, rows
):
    """Create ``table_name``, insert ``rows`` [(name, value), …], commit, persist.

    Persisting is what makes a subsequent read traverse the cold path
    (``stream_merged`` → ``build_persist_session`` → ``derive_cold_session``):
    a hot-only read takes the all-hot fast path that never builds a cold
    ``SessionContext``.
    """
    table_uuid = client.create_table(
        table_name,
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha421 isolation",
    )
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    batch = pa.table(
        {"name": [r[0] for r in rows], "value": [r[1] for r in rows]},
        schema=USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
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

    return table_uuid


def _names(table: pa.Table) -> list[str]:
    return sorted(table.column("name").to_pylist())


def test_concurrent_read_data_does_not_cross_contaminate():
    """Two simultaneous cold ReadData over different tables return only their
    own rows — the query service derives an isolated catalog per read, so
    concurrent reads must not leak the fixed cold table names across."""
    setup = make_client()
    catalog_name = f"iso_cat_{uuid4().hex[:8]}"
    catalog_uuid, branch_uuid = setup.create_catalog(catalog_name, "owner")
    schema_uuid = setup.create_schema(
        "iso_schema", catalog_uuid=catalog_uuid, author="test", comment="iso"
    )
    table_a = _make_cold_table(
        setup,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="table_a",
        rows=[("a1", 1), ("a2", 2)],
    )
    table_b = _make_cold_table(
        setup,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="table_b",
        rows=[("b1", 3), ("b2", 4)],
    )
    setup.close()

    # Separate clients per read stream so the concurrency lives entirely in the
    # query service, not in any client-side state.
    client_a = make_client()
    client_b = make_client()

    def read(client, table_uuid):
        return client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            columns=["name", "value"],
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            for _ in range(8):
                fut_a = pool.submit(read, client_a, table_a)
                fut_b = pool.submit(read, client_b, table_b)
                assert _names(fut_a.result()) == ["a1", "a2"], (
                    "table_a read leaked rows"
                )
                assert _names(fut_b.result()) == ["b1", "b2"], (
                    "table_b read leaked rows"
                )

    finally:
        client_a.close()
        client_b.close()


def _flight_select_names(
    catalog_name: str, schema_name: str, table_name: str
) -> list[str]:
    """Open a fresh Flight SQL connection (its own ConnSession), run one SELECT,
    return the sorted ``name`` column. A fresh connection per call puts the
    concurrency on the server's per-connection session derivation."""
    settings = ClientSettings()  # ty: ignore[missing-argument]
    assert settings.flight_sql_url is not None
    conn = flight_sql_connect(
        f"grpc://{settings.flight_sql_url}",
        db_kwargs={
            "adbc.flight.sql.rpc.with_cookie_middleware": "true",
            "adbc.flight.sql.rpc.call_header.x-penca-catalog": catalog_name,
        },
        autocommit=True,
    )
    conn.adbc_current_db_schema = schema_name
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT name, value FROM {catalog_name}.{schema_name}.{table_name}"
        )
        return sorted(cursor.fetch_arrow_table().column("name").to_pylist())

    finally:
        conn.close()


def test_concurrent_flight_sql_does_not_cross_contaminate():
    """Two simultaneous Flight SQL SELECTs over different cold tables, on
    separate connections, return only their own rows — guards the
    per-connection template-derived ctx isolation (build_ctx, unchanged by
    CHA-421) and, via ReadData fan-out, the query-service cold isolation."""
    client = make_client()
    catalog_name = f"iso_sql_{uuid4().hex[:8]}"
    catalog_uuid, branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_name = "iso_schema"
    schema_uuid = client.create_schema(
        schema_name, catalog_uuid=catalog_uuid, author="test", comment="iso"
    )
    _make_cold_table(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="table_a",
        rows=[("a1", 1), ("a2", 2)],
    )
    _make_cold_table(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="table_b",
        rows=[("b1", 3), ("b2", 4)],
    )
    client.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        for _ in range(4):
            fut_a = pool.submit(
                _flight_select_names, catalog_name, schema_name, "table_a"
            )
            fut_b = pool.submit(
                _flight_select_names, catalog_name, schema_name, "table_b"
            )
            assert fut_a.result() == ["a1", "a2"], "Flight SQL table_a leaked rows"
            assert fut_b.result() == ["b1", "b2"], "Flight SQL table_b leaked rows"
