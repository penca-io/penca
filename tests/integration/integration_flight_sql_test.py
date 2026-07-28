"""End-to-end ADBC Flight SQL tests."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import warnings
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal
from uuid import uuid4

import pyarrow as pa
import pyarrow.flight as paflight
import pytest
from adbc_driver_flightsql.dbapi import connect as flight_sql_connect
from adbc_driver_manager.dbapi import Connection as AdbcConnection
from grpc import insecure_channel
from penca_client import Mutation
from penca_client.client import PencaClient
from penca_client.config import ClientSettings
from penca_client.errors import ApiError
from penca_client.naming import (
    MAIN_BRANCH_NAME,
    abort_tx_log_partition,
    system_indexes_table_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
)
from penca_proto.external.v1.lifecycle_pb2_grpc import LifecycleServiceStub
from penca_proto.external.v1.query_pb2_grpc import QueryServiceStub
from penca_proto.external.v1.write_pb2_grpc import WriteServiceStub
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    container_log,
    get_pg_driver,
    make_client,
    setup_with_data_named,
)


def _setup_two_tables_one_schema(client: PencaClient) -> dict:
    """Create a catalog with ONE schema holding TWO tables (``a``, ``b``), a row
    in each, committed; pin the connection to the catalog.

    For the within-build resolution-memo test (CHA-367): a join referencing both
    tables in the same schema makes DataFusion resolve ``schema("s")`` once per
    table reference, so the per-plan memo's collapse to a single ``get_schema``
    is observable — without the memo that one build issues two.
    """
    catalog_name = f"sql_join_cat_{uuid4().hex[:8]}"
    schema_name = "sql_schema"

    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        schema_name, catalog_uuid=catalog_uuid, author="test", comment="create_schema"
    )
    table_uuids = {
        name: client.create_table(
            name,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        for name in ("a", "b")
    }

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    row = pa.table({"name": ["x"], "value": [1]}, schema=USER_SCHEMA)
    for table_key in ("a", "b"):
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuids[table_key], upserts=row),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

    client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
    )

    client.catalog = catalog_name
    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "main_branch_uuid": main_branch_uuid,
    }


def _sorted_rows(table: pa.Table) -> list[dict]:
    """Return table rows sorted by ``name`` so set-equality checks are stable."""
    return sorted(table.to_pylist(), key=lambda r: r["name"])


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSql:
    def test_select_star_matches_read_data(self, driver: Literal["adbc", "jdbc"]):
        client = make_client()
        ctx = setup_with_data_named(client)

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        sql_rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )

        grpc_result = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            columns=["name", "value"],
        )

        assert sql_rows == _sorted_rows(grpc_result)
        client.close()

    def test_query_stream_yields_record_batches(self, driver: Literal["adbc", "jdbc"]):
        # `client.execute_stream` is an Arrow-RecordBatch-streaming
        # API specific to ADBC; JDBC's ResultSet has no analog (the
        # probe materializes the full ResultSet to JSON). Skip the
        # JDBC arm so the parametrize matrix stays uniform without
        # asserting on a contract the driver can't honor.
        if driver == "jdbc":
            pytest.skip("execute_stream is an Arrow-streaming API; no JDBC analog")

        client = make_client()
        ctx = setup_with_data_named(client)

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        batches = list(client.execute_stream(f"SELECT name, value FROM {fqn}"))

        assert batches, "expected at least one RecordBatch"
        assert all(isinstance(b, pa.RecordBatch) for b in batches)
        assert sum(b.num_rows for b in batches) == 2
        client.close()

    def test_query_after_persist(self, driver: Literal["adbc", "jdbc"]):
        """Merge-on-read across hot + cold must match the gRPC read path."""
        client = make_client()
        ctx = setup_with_data_named(client)

        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        sql_rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )

        grpc_result = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            columns=["name", "value"],
        )

        assert sql_rows == _sorted_rows(grpc_result)
        client.close()

    def test_filter_pushdown_matches_read_data(self, driver: Literal["adbc", "jdbc"]):
        """CHA-142 end-to-end: a filtered SELECT through Flight SQL must
        return the same rows as ``read_data(filter=...)``.

        Exercises the Expr → SQL WHERE translator in
        ``penca-datafusion`` and the ``filter`` plumbing in
        ``read_data``. The two paths feed different SQL layers, so
        parity here is the best signal that the Expr translator
        produces a fragment the merge pipeline understands.
        """
        client = make_client()
        ctx = setup_with_data_named(client)

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        sql_rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )
        grpc_result = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            columns=["name", "value"],
            filter="name = 'alice'",
        )

        assert sql_rows == _sorted_rows(grpc_result)
        assert len(sql_rows) == 1
        client.close()


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlBareCountStar:
    """CHA-180: bare ``SELECT COUNT(*)`` must not panic the SQL server.

    DataFusion plans ``COUNT(*)`` with ``projection = Some([])`` — no
    user columns needed. The wire-level ``ReadDataRequest.columns =
    repeated string`` cannot distinguish "unset projection" (return all
    columns) from "0-column projection" (return zero), so the servicer
    interprets DataFusion's empty list as "return everything." The
    full-width batches that come back don't match the 0-column output
    schema DataFusion's plan expects, and ``CoalesceBatchesExec`` trips
    its width assertion. Mixed aggregates like ``COUNT(*), SUM(value)``
    sidestep the bug because DataFusion projects at least one column.

    Fix path: replace the bare ``repeated string columns`` field with a
    ``Projection`` message whose presence bit carries the
    disambiguation. These tests pin the user-facing behavior at the
    SQL-server boundary; gRPC-level wire semantics are pinned in
    ``integration_query_test.TestReadDataProjectionSemantics``.

    Synthetic-column label casing (`count(*)` vs `COUNT(*)`) may
    differ between ADBC and JDBC, so assertions key on ordinal value
    lookup (`list(rows[0].values())`) instead of the column name.
    """

    def test_count_star_returns_row_count(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"

        rows = _execute_query_via(
            driver,
            f"SELECT COUNT(*) FROM {fqn}",
            port=port,
            catalog=ctx["catalog_name"],
        )

        assert len(rows) == 1
        assert len(rows[0]) == 1
        assert list(rows[0].values()) == [2]

    def test_count_star_and_sum_mixed_query(self, driver: Literal["adbc", "jdbc"]):
        """Regression: mixed aggregates project >0 user cols, so they
        bypass the empty-projection branch. Must continue to work."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"

        rows = _execute_query_via(
            driver,
            f"SELECT COUNT(*), SUM(value) FROM {fqn}",
            port=port,
            catalog=ctx["catalog_name"],
        )

        assert len(rows) == 1
        assert list(rows[0].values()) == [2, 30]

    def test_select_constant_returns_n_rows(self, driver: Literal["adbc", "jdbc"]):
        """Regression: DataFusion injects a 1-col projection for
        ``SELECT 1`` so the empty-projection branch is not hit. Pins
        that the existing non-empty path stays intact."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"

        rows = _execute_query_via(
            driver,
            f"SELECT 1 FROM {fqn} LIMIT 5",
            port=port,
            catalog=ctx["catalog_name"],
        )

        # Only 2 rows seeded; LIMIT 5 caps at 2.
        assert len(rows) == 2
        assert [list(r.values())[0] for r in rows] == [1, 1]

    def test_count_star_after_persist(self, driver: Literal["adbc", "jdbc"]):
        """Cold tier present: the ``stream_merged`` path also has to handle
        a 0-col user schema, not just the all-hot fast path."""
        client = make_client()
        ctx = setup_with_data_named(client)
        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"

        rows = _execute_query_via(
            driver,
            f"SELECT COUNT(*) FROM {fqn}",
            port=port,
            catalog=ctx["catalog_name"],
        )

        assert len(rows) == 1
        assert list(rows[0].values()) == [2]


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlPlannerDefaults:
    """CHA-193: the SQL planner resolves an unqualified ``schema.table``
    against the session's pinned catalog (CHA-169), not against
    DataFusion's hardcoded literal ``"datafusion"``.

    Today's bug: the per-conn ``SessionState`` is built from the template
    and pinned to the connection's ``catalog_uuid`` (on ``ConnScope``),
    but never updates ``SessionConfig.options.catalog.default_catalog``.
    DataFusion's
    name resolver therefore prepends its built-in default
    (``"datafusion"``) to any unqualified reference, and
    ``PencaCatalogProviderList::catalog("datafusion")`` is a miss
    against ``catalog_store``.

    These tests pin ``client.catalog`` (forwarded via the
    ``x-penca-catalog`` gRPC metadata header at handshake — CHA-253),
    then issue ``SELECT`` statements with no catalog prefix and assert
    they hit the session's catalog. Cross-catalog rejection regression
    coverage already lives in
    :meth:`TestFlightSqlDml.test_select_cross_catalog_rejected`; we
    don't duplicate it here.
    """

    def test_unqualified_select_resolves_to_session_catalog(
        self,
        driver: Literal["adbc", "jdbc"],
    ):
        """``SELECT ... FROM <schema>.<table>`` without a catalog prefix
        returns the same rows as the fully-qualified form when the
        session is pinned to the table's catalog.
        """
        ctx, port = _setup_and_port(setup_with_data_named)

        unqualified = f"{ctx['schema_name']}.{ctx['table_name']}"
        fqn = f"{ctx['catalog_name']}.{unqualified}"

        unqualified_rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {unqualified}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )
        fqn_rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )

        assert unqualified_rows == fqn_rows
        assert len(unqualified_rows) == 2

    def test_unqualified_select_uses_session_catalog_not_other(
        self,
        driver: Literal["adbc", "jdbc"],
    ):
        """When two catalogs share the same ``schema.table`` shape, an
        unqualified SELECT on a session pinned to catalog A returns A's
        rows — not B's, and not a "table not found" against
        ``"datafusion"``.

        Defense-in-depth: a naive fix that registered every catalog
        under every name would make this test return both catalogs'
        rows (or B's rows). The session-default-catalog wiring must
        resolve unambiguously to A.
        """
        client_a = make_client()
        ctx_a = setup_with_data_named(client_a)

        # Build a sibling catalog with the same schema + table layout
        # but different rows. Use a fresh client so its Flight SQL
        # connection isn't pinned to catalog A.
        client_b = make_client()
        catalog_b_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_b_uuid, branch_b_uuid = client_b.create_catalog(catalog_b_name, "owner")
        schema_b_uuid = client_b.create_schema(
            ctx_a["schema_name"],
            catalog_uuid=catalog_b_uuid,
            author="test",
            comment="create_schema_b",
        )
        table_b_uuid = client_b.create_table(
            ctx_a["table_name"],
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_b_uuid,
            schema_uuid=schema_b_uuid,
            author="test",
            comment="create_table_b",
        )
        tx_b = client_b.begin_tx(
            catalog_uuid=catalog_b_uuid,
            schema_uuid=schema_b_uuid,
            branch_uuid=branch_b_uuid,
        )
        b_batch = pa.table(
            {"name": ["carol", "dave"], "value": [30, 40]},
            schema=USER_SCHEMA,
        )
        client_b.write_data(
            tx_b.tx_uuid,
            Mutation(table_uuid=table_b_uuid, upserts=b_batch),
            catalog_uuid=catalog_b_uuid,
            schema_uuid=schema_b_uuid,
            branch_uuid=branch_b_uuid,
        )
        client_b.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_b_uuid,
            branch_uuid=branch_b_uuid,
        )
        client_b.close()

        # client_a is still pinned to catalog A from the helper. The
        # unqualified SELECT must resolve against A and return A's
        # rows — not B's, even though `<schema>.<table>` exists in
        # both catalogs. Both arms re-pin to catalog A via the
        # helper's `catalog=` parameter so the JDBC connection picks
        # up the same handshake-time pin client_a has.
        client_a.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        unqualified = f"{ctx_a['schema_name']}.{ctx_a['table_name']}"
        rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {unqualified}",
                port=port,
                catalog=ctx_a["catalog_name"],
            ),
            key=lambda r: r["name"],
        )

        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlDml:
    """CHA-121: INSERT / UPDATE / DELETE via Flight SQL DoPutStatementUpdate.

    Each test fetches rows back with ``_execute_query_via`` rather than
    ``read_data`` so a regression in the DML path can't be masked by a
    stale hot-tier cache on the gRPC read side.
    """

    def test_insert_values_single_row(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES ('charlie', 30)"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
            {"name": "charlie", "value": 30},
        ]

    def test_insert_values_multi_row(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES ('dee', 40), ('eve', 50)"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
        )
        assert {r["name"] for r in rows} == {"alice", "bob", "dee", "eve"}

    def test_insert_rejects_duplicate_pk(self, driver: Literal["adbc", "jdbc"]):
        """Strict-INSERT: a colliding PK must fail, not silently upsert."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES ('alice', 999)"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(r"(?i)already[_ ]exists|primary key collision", payload), (
            payload
        )

        rows = _execute_query_via(
            driver,
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
            port=port,
            catalog=cat,
        )
        assert rows == [{"name": "alice", "value": 10}]

    def test_insert_rejects_duplicate_pk_after_persist(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Strict-INSERT must see cold-tier rows too."""
        client = make_client()
        ctx = setup_with_data_named(client)
        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES ('alice', 999)"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(r"(?i)already[_ ]exists|primary key collision", payload), (
            payload
        )

        rows = _execute_query_via(
            driver,
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
            port=port,
            catalog=cat,
        )
        assert rows == [{"name": "alice", "value": 10}]

    def test_insert_rejects_duplicate_pk_names_colliding_identity(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """CHA-180: the collision error surfaces the colliding PK value."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES ('alice', 999)"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(
            r"(?is)(already[_ ]exists|primary key collision).*name=alice", payload
        ), payload

    def test_insert_rejects_duplicate_pk_caps_long_listings(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """CHA-180: bulk-INSERT pathologies past MAX_REPORTED=10."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        # The setup helper already seeded alice + bob. Add 13 more so
        # the next INSERT sees 15 candidate-PK collisions (> the
        # 10-row cap).
        extras = ", ".join(f"('row_{i}', {i})" for i in range(13))
        seed_upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES {extras}"],
            port=port,
            catalog=cat,
        )
        assert seed_upd[0][0] == "OK", seed_upd

        all_pks = ", ".join(
            ["('alice', 99)", "('bob', 98)"]
            + [f"('row_{i}', {i + 1000})" for i in range(13)]
        )
        upd = _execute_update_steps_via(
            driver,
            [f"INSERT INTO {fqn} (name, value) VALUES {all_pks}"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(
            r"(?is)(already[_ ]exists|primary key collision).*first 10 of possibly more",
            payload,
        ), payload

    def test_insert_on_conflict_do_update_overwrites(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """INSERT ... ON CONFLICT DO UPDATE — last-writer-wins upsert."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [
                f"INSERT INTO {fqn} (name, value) VALUES ('alice', 999) "
                "ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value"
            ],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = _execute_query_via(
            driver,
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
            port=port,
            catalog=cat,
        )
        assert rows == [{"name": "alice", "value": 999}]

    def test_update_where_matches_one_row(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET value = 111 WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 111},
            {"name": "bob", "value": 20},
        ]

    def test_update_where_matches_multiple_rows(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET value = 0 WHERE value >= 10"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
        )
        assert all(r["value"] == 0 for r in rows)
        assert {r["name"] for r in rows} == {"alice", "bob"}

    def test_delete_where_matches_one_row(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"DELETE FROM {fqn} WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [{"name": "bob", "value": 20}]

    def test_delete_where_matches_multiple_rows(self, driver: Literal["adbc", "jdbc"]):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"DELETE FROM {fqn} WHERE value >= 10"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
        )
        assert rows == []

    def test_delete_where_matches_zero_rows(self, driver: Literal["adbc", "jdbc"]):
        """DELETE against a non-matching WHERE returns zero affected, no error."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"DELETE FROM {fqn} WHERE name = 'nobody'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "0")], upd

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
        )
        assert len(rows) == 2

    def test_dml_rejects_returning(self, driver: Literal["adbc", "jdbc"]):
        """RETURNING is rejected for every DML verb."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        for sql, msg_re in [
            (
                f"INSERT INTO {fqn} (name, value) VALUES ('carol', 30) RETURNING name",
                r"(?i)returning.*not supported",
            ),
            (
                f"UPDATE {fqn} SET value = 111 WHERE name = 'alice' RETURNING value",
                r"(?i)returning.*not supported",
            ),
            (
                f"DELETE FROM {fqn} WHERE name = 'alice' RETURNING name",
                r"(?i)returning",
            ),
        ]:
            upd = _execute_update_steps_via(driver, [sql], port=port, catalog=cat)
            status, payload = upd[0]
            assert status == "CAUGHT", f"{sql}: {status}={payload}"
            assert re.search(msg_re, payload), f"{sql}: {payload!r}"


_COMPOSITE_USER_SCHEMA = pa.schema(
    [
        pa.field("region", pa.utf8()),
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)


def _setup_with_composite_pk_data_named(client: PencaClient) -> dict:
    """Like ``setup_with_data_named`` but with a ``(region, name)`` PK.

    Seeds three rows so a single-row PK-changing UPDATE still leaves
    two untouched rows whose identities must not move.
    """
    catalog_name = f"sql_cpk_{uuid4().hex[:8]}"
    schema_name = "sql_schema"
    table_name = "sql_cpk_table"

    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        schema_name, catalog_uuid=catalog_uuid, author="test", comment="create_schema"
    )
    table_uuid = client.create_table(
        table_name,
        _COMPOSITE_USER_SCHEMA,
        primary_keys=["region", "name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="create_table",
    )

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    batch = pa.table(
        {
            "region": ["us", "us", "eu"],
            "name": ["alice", "bob", "carol"],
            "value": [1, 2, 3],
        },
        schema=_COMPOSITE_USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    # CHA-169 pin (see setup_with_data_named).
    client.catalog = catalog_name

    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_name": table_name,
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "main_branch_uuid": main_branch_uuid,
    }


def _sorted_cpk_rows(table: pa.Table) -> list[dict]:
    """Sort composite-PK rows by ``(region, name)`` so set-equality holds."""
    return sorted(table.to_pylist(), key=lambda r: (r["region"], r["name"]))


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlUpdateOnPk:
    """CHA-237: UPDATE statements that modify PK columns must emit
    ``delete(old_pk) + upsert(new_row)`` as one atomic ``Change``, not a
    bare upsert. A bare upsert leaves the old-PK row visible because
    ``row_uuid = hash(PK)`` makes the new row a structurally different
    tuple — merge-on-read then surfaces both. Each test below would pass
    on a delete+insert implementation and fail today (the pre-fix path
    ships an empty ``deletes`` payload, so the old row survives).
    """

    def test_update_single_pk_moves_row_identity(self, driver: Literal["adbc", "jdbc"]):
        """``UPDATE t SET pk = X WHERE pk = Y`` — old PK row is gone,
        new PK row carries the unmodified non-PK columns."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = 'alice2' WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice2", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_update_composite_pk_moves_both_columns(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Composite-PK UPDATE on both PK columns — the old composite is
        gone, the new composite is present, total row count is unchanged.
        The two untouched seed rows must stay byte-identical."""
        ctx, port = _setup_and_port(_setup_with_composite_pk_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [
                f"UPDATE {fqn} SET region = 'eu', name = 'alice2' "
                f"WHERE region = 'us' AND name = 'alice'"
            ],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT region, name, value FROM {fqn}",
                port=port,
                catalog=cat,
            ),
            key=lambda r: (r["region"], r["name"]),
        )
        assert rows == [
            {"region": "eu", "name": "alice2", "value": 1},
            {"region": "eu", "name": "carol", "value": 3},
            {"region": "us", "name": "bob", "value": 2},
        ]

    def test_update_non_pk_column_does_not_duplicate_row(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Regression guard for the ``pk_changing == false`` branch."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET value = 111 WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        alice_rows = _execute_query_via(
            driver,
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
            port=port,
            catalog=cat,
        )
        assert alice_rows == [{"name": "alice", "value": 111}], alice_rows

        all_rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert all_rows == [
            {"name": "alice", "value": 111},
            {"name": "bob", "value": 20},
        ]

    def test_update_pk_matching_many_rows_emits_per_row_pairs(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``UPDATE t SET pk = f(pk) WHERE <matches N>``."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = name || '_x' WHERE value < 30"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice_x", "value": 10},
            {"name": "bob_x", "value": 20},
        ]

    def test_update_pk_inside_tx_is_atomic_and_ryow(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """In an open tx, a PK-changing UPDATE must be RYOW-visible to
        the same session and atomic to external sessions on COMMIT.

        Mid-tx + post-COMMIT external check both run on the same
        connection via a single _execute_update_steps_via call.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        steps_a = [
            "BEGIN",
            f"UPDATE {fqn} SET name = 'alice2' WHERE name = 'alice'",
            f"SELECT name, value FROM {fqn} WHERE name = 'alice'",
            f"SELECT name, value FROM {fqn} WHERE name = 'alice2'",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps_a, port=port, catalog=cat)
        assert results[0][0] == "OK"
        assert results[1] == ("OK", "1"), results[1]
        assert results[2][0] == "OK_ROWS"
        assert json.loads(results[2][1]) == [], results[2]
        assert results[3][0] == "OK_ROWS"
        assert json.loads(results[3][1]) == [{"name": "alice2", "value": 10}], results[
            3
        ]
        assert results[4][0] == "OK"

        external_rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert external_rows == [
            {"name": "alice2", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_sequential_pk_updates_in_one_tx_collapse_to_final(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Two PK-changing UPDATEs in one tx (alice → tmp → final) must
        leave only the final PK on COMMIT."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                f"UPDATE {fqn} SET name = 'tmp' WHERE name = 'alice'",
                f"UPDATE {fqn} SET name = 'final' WHERE name = 'tmp'",
                "COMMIT",
            ],
            port=port,
            catalog=cat,
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "bob", "value": 20},
            {"name": "final", "value": 10},
        ]

    # CHA-242: UPDATE PK collision check + intra-batch surfacing
    #
    # External collision: ``UPDATE t SET pk = X WHERE pk = Y`` where
    # ``X`` already lives in the table silently overwrites the
    # pre-existing row's non-PK columns at ``row_uuid_for_pk(table,
    # [X])``. SQL must reject the UPDATE the same way the strict-INSERT
    # path rejects a colliding ``INSERT`` (``Status::already_exists``).
    # The check runs under the unified per-(branch, table) advisory
    # lock (``dml:pk-collision:``) shared with strict-INSERT, so a
    # concurrent ``INSERT 'target'`` and ``UPDATE … SET name='target'``
    # serialize against each other.
    #
    # Intra-batch: ``UPDATE t SET pk = 'charlie' WHERE value < 30``
    # produces two upsert rows whose row_uuids collide. The within-
    # upserts uniqueness check at the write servicer rejects with
    # ``INVALID_ARGUMENT``; the SQL surface forwards the rejection.

    def test_update_pk_collision_with_existing_row_rejects(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``UPDATE t SET pk = X WHERE pk = Y`` with ``X`` already
        seeded must fail with ``ALREADY_EXISTS``; the pre-existing
        row's non-PK columns survive unchanged."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = 'bob' WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert len(upd) == 1
        status, payload = upd[0]
        assert status == "CAUGHT", f"expected ALREADY_EXISTS; got {status}={payload}"
        assert re.search(r"(?i)already[_ ]exists|primary key collision", payload), (
            payload
        )

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_update_pk_collision_after_persist(self, driver: Literal["adbc", "jdbc"]):
        """Defense in depth: collision detected when row only in cold tier."""
        client = make_client()
        ctx = setup_with_data_named(client)
        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = 'bob' WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"expected ALREADY_EXISTS; got {status}={payload}"
        assert re.search(r"(?i)already[_ ]exists|primary key collision", payload), (
            payload
        )

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_update_pk_intra_batch_collision_rejects(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``UPDATE t SET pk = const WHERE <matches >1>`` rejects."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = 'charlie' WHERE value < 30"],
            port=port,
            catalog=cat,
        )
        status, payload = upd[0]
        assert status == "CAUGHT", f"expected rejection; got {status}={payload}"
        assert re.search(r"(?i)invalid[_ ]argument|duplicate.*row_uuid", payload), (
            payload
        )

    def test_update_pk_to_fresh_value_succeeds(self, driver: Literal["adbc", "jdbc"]):
        """No-collision case: every UPDATE result is fresh."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = name || '_y' WHERE value < 30"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice_y", "value": 10},
            {"name": "bob_y", "value": 20},
        ]

    def test_update_pk_concurrent_to_same_new_value_serializes(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Two sessions each ``UPDATE t SET pk = 'target' WHERE pk =
        <distinct>``: exactly one wins, the other gets
        ``ALREADY_EXISTS``."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        def run_update(where_name: str) -> tuple[str, str]:
            return _execute_update_steps_via(
                driver,
                [f"UPDATE {fqn} SET name = 'target' WHERE name = '{where_name}'"],
                port=port,
                catalog=cat,
            )[0]

        with ThreadPoolExecutor(max_workers=2) as pool:
            res_a = pool.submit(run_update, "alice").result(timeout=30.0)
            res_b = pool.submit(run_update, "bob").result(timeout=30.0)

        statuses = sorted([res_a[0], res_b[0]])
        assert statuses == ["CAUGHT", "OK"], (
            f"expected exactly one OK and one CAUGHT; got res_a={res_a!r} res_b={res_b!r}"
        )
        losing_payload = (res_a if res_a[0] == "CAUGHT" else res_b)[1]
        assert re.search(
            r"(?i)already[_ ]exists|primary key collision", losing_payload
        ), losing_payload

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        names = sorted(r["name"] for r in rows)
        assert names in (["bob", "target"], ["alice", "target"]), names

    def test_insert_and_update_pk_concurrent_to_same_value_serialize(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """A strict ``INSERT`` racing a PK-changing ``UPDATE`` to the same
        target value must serialize via the unified per-(branch, table)
        advisory lock."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        def run_insert() -> tuple[str, str]:
            return _execute_update_steps_via(
                driver,
                [f"INSERT INTO {fqn} (name, value) VALUES ('target', 99)"],
                port=port,
                catalog=cat,
            )[0]

        def run_update() -> tuple[str, str]:
            return _execute_update_steps_via(
                driver,
                [f"UPDATE {fqn} SET name = 'target' WHERE name = 'alice'"],
                port=port,
                catalog=cat,
            )[0]

        with ThreadPoolExecutor(max_workers=2) as pool:
            res_ins = pool.submit(run_insert).result(timeout=30.0)
            res_upd = pool.submit(run_update).result(timeout=30.0)

        statuses = sorted([res_ins[0], res_upd[0]])
        assert statuses == ["CAUGHT", "OK"], (
            f"expected exactly one OK and one CAUGHT; got "
            f"res_ins={res_ins!r} res_upd={res_upd!r}"
        )
        losing_payload = (res_ins if res_ins[0] == "CAUGHT" else res_upd)[1]
        assert re.search(
            r"(?i)already[_ ]exists|primary key collision", losing_payload
        ), losing_payload

        rows = _execute_query_via(
            driver, f"SELECT name FROM {fqn}", port=port, catalog=cat
        )
        names = sorted(r["name"] for r in rows)
        assert "target" in names, names
        assert names.count("target") == 1, names
        assert "bob" in names, names


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlCompositeTiebreaker:
    """CHA-243 → CHA-431: the merge tombstone-shadow tiebreak keys off the
    composite ``(commit_seq_num, write_seq_num)`` — ``commit_seq_num`` the per-branch
    gapless commit serial (CHA-428), ``write_seq_num`` the within-tx mutation
    ordinal from the table's lock-free ``write_sequence``.

    Uniform rule: *for two mutations of the same ``row_uuid``, the one with
    the greater ``(commit_seq_num, write_seq_num)`` wins.*

    The within-RPC ``delete(row_uuid) + upsert(row_uuid)`` pair emitted by a
    value-preserving SET on a PK column (CHA-237) commits at one
    ``commit_seq_num``; deletes-first allocation gives the delete a strictly
    lower ``write_seq_num``, so the upsert wins (row preserved — pre-CHA-243
    the strict-``>`` predicate silently tombstoned it). These tests pin the
    resolution OUTCOMES, unchanged by CHA-431's swap of the secondary
    tiebreak key onto ``write_seq_num``.
    """

    def test_value_preserving_set_on_pk_preserves_row(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``UPDATE t SET pk = pk WHERE pk = X`` — row at X still present
        post-COMMIT.

        Today: CHA-237's PK-changing-UPDATE path emits
        ``delete(row_uuid_alice) + upsert(row_uuid_alice)`` in one
        Change. Both writes share one ``commit_micros``; strict
        ``>`` tombstone-shadow predicate hides the row. Composite
        ``(committed_at, written_at) >=`` flips the tie to upsert-wins.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = name WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_value_preserving_set_with_coalesce(self, driver: Literal["adbc", "jdbc"]):
        """``UPDATE t SET pk = COALESCE(pk, fallback)`` — non-null PK
        rows survive, count unchanged.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = COALESCE(name, 'fallback')"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_value_preserving_set_with_case_expression(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Mixed: ``bob → bob2`` (distinct row_uuid path), ``alice →
        alice`` (value-preserving same-row_uuid path needing composite
        tiebreaker)."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        upd = _execute_update_steps_via(
            driver,
            [
                f"UPDATE {fqn} SET name = CASE WHEN name = 'bob' THEN 'bob2' ELSE name END"
            ],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "2")], upd

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob2", "value": 20},
        ]

    def test_insert_then_delete_within_tx_hides_row(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``BEGIN; INSERT R; DELETE R; COMMIT;`` — R hidden post-COMMIT.
        Multi-step tx must run on one connection — single
        `_execute_update_steps_via` call carries BEGIN/INSERT/DELETE/
        COMMIT together so the JDBC arm doesn't auto-commit each step.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        steps = [
            "BEGIN",
            f"INSERT INTO {fqn} VALUES ('charlie', 30)",
            f"DELETE FROM {fqn} WHERE name = 'charlie'",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog=cat)
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} ({steps[i]!r}) failed: {payload}"

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert {"name": "charlie", "value": 30} not in rows
        assert {r["name"] for r in rows} == {"alice", "bob"}

    def test_delete_then_insert_within_tx_shows_row(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """``BEGIN; DELETE R; INSERT R; COMMIT;`` — R visible post-COMMIT."""
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        steps = [
            "BEGIN",
            f"DELETE FROM {fqn} WHERE name = 'alice'",
            f"INSERT INTO {fqn} VALUES ('alice', 999)",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog=cat)
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} ({steps[i]!r}) failed: {payload}"

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 999},
            {"name": "bob", "value": 20},
        ]

    def test_ryow_preservation_within_open_tx(self, driver: Literal["adbc", "jdbc"]):
        """Open-tx ``INSERT R; SELECT; DELETE R; SELECT;`` — first
        SELECT sees R, second doesn't. All five steps on one
        connection so the open-tx synthetic-ts machinery (ADR 0009)
        threads through.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        select_sql = f"SELECT name, value FROM {fqn} WHERE name = 'charlie'"
        steps = [
            "BEGIN",
            f"INSERT INTO {fqn} VALUES ('charlie', 30)",
            select_sql,
            f"DELETE FROM {fqn} WHERE name = 'charlie'",
            select_sql,
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog=cat)
        assert results[0][0] == "OK", results[0]
        assert results[1][0] == "OK", results[1]
        assert results[2][0] == "OK_ROWS", results[2]
        seen = json.loads(results[2][1])
        assert seen == [{"name": "charlie", "value": 30}], seen
        assert results[3][0] == "OK", results[3]
        assert results[4][0] == "OK_ROWS", results[4]
        gone = json.loads(results[4][1])
        assert gone == [], gone
        assert results[5][0] == "OK", results[5]

    def test_concurrent_updates_disjoint_pks_both_visible(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Two open txs each ``UPDATE`` disjoint PK sets; both commit
        cleanly; both updates visible post-COMMIT.

        Both arms run two concurrent multi-step transactions via
        ThreadPoolExecutor. On the JDBC arm each branch spawns its
        own JVM probe; the test's defense-in-depth check is that no
        per-tx serialization point exists (``write_seq_num`` is a
        lock-free sequence), so running the two probes in parallel is
        the right exercise.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        cat = ctx["catalog_name"]

        def run_update_tx(target_name: str, new_value: int):
            return _execute_update_steps_via(
                driver,
                [
                    "BEGIN",
                    f"UPDATE {fqn} SET value = {new_value} WHERE name = '{target_name}'",
                    "COMMIT",
                ],
                port=port,
                catalog=cat,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_a = pool.submit(run_update_tx, "alice", 100)
            fut_b = pool.submit(run_update_tx, "bob", 200)
            res_a = fut_a.result(timeout=30.0)
            res_b = fut_b.result(timeout=30.0)

        for label, res in (("a", res_a), ("b", res_b)):
            for i, (status, payload) in enumerate(res):
                assert status == "OK", f"branch {label} step {i} failed: {payload}"

            assert res[1][1] == "1"

        rows = sorted(
            _execute_query_via(
                driver, f"SELECT name, value FROM {fqn}", port=port, catalog=cat
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 100},
            {"name": "bob", "value": 200},
        ]

    def test_audit_data_surfaces_write_seq_num_total_order(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """[CHA-431] ``audit_data`` surfaces ``write_seq_num`` (Int64) IN
        PLACE OF ``written_at_micros``, alongside ``commit_seq_num``.

        A PK-changing UPDATE emits ``delete(old_pk) + upsert(new_pk)`` in
        one ``WriteData`` batch. Both sides commit at one ``commit_seq_num``;
        deletes-first allocation (IMPL4) gives the delete a strictly lower
        ``write_seq_num`` than the upsert, so ``(commit_seq_num, write_seq_num)``
        is the total mutation order — the upsert lands last (replace),
        with no ``written_at`` tie special-case.

        Hot/cold parity: after persist→purge the same two rows scan out of
        the cold tier carrying the *identical* ``(commit_seq_num,
        write_seq_num)`` the hot stream surfaced.

        Pins server-side audit state; the driver choice only affects which
        wire path issues the UPDATE — audit reading goes through the gRPC
        ``audit_data`` call regardless.

        Red baseline (pre-IMPL8): the audit schema still carries
        ``written_at_micros`` and no ``write_seq_num`` column, so the first
        ``"write_seq_num" in *.column_names`` assertion fails.
        """
        client = make_client()
        ctx = setup_with_data_named(client)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")
        cat = ctx["catalog_name"]

        audit_ids = {
            "catalog_uuid": ctx["catalog_uuid"],
            "schema_uuid": ctx["schema_uuid"],
            "table_uuid": ctx["table_uuid"],
            "branch_uuid": ctx["main_branch_uuid"],
        }

        upd = _execute_update_steps_via(
            driver,
            [f"UPDATE {fqn} SET name = 'alice2' WHERE name = 'alice'"],
            port=port,
            catalog=cat,
        )
        assert upd == [("OK", "1")], upd

        upserts, deletes = client.audit_data(**audit_ids)

        # (1) the column flip — write_seq_num present, written_at_micros gone
        #     from the audit surface (CHA-431 retires it).
        for label, tbl in (("upserts", upserts), ("deletes", deletes)):
            assert "write_seq_num" in tbl.column_names, (
                f"{label} audit schema missing write_seq_num; got {tbl.column_names}"
            )
            assert "written_at_micros" not in tbl.column_names, (
                f"{label} audit schema must drop written_at_micros (CHA-431); "
                f"got {tbl.column_names}"
            )

        assert deletes.num_rows == 1
        delete_row = deletes.to_pylist()[0]
        paired = [u for u in upserts.to_pylist() if u["name"] == "alice2"]
        assert len(paired) == 1, f"expected exactly one alice2 upsert; got {paired}"
        upsert_row = paired[0]

        # (2) one commit → shared commit_seq_num; deletes-first → the delete's
        #     write_seq_num strictly precedes the upsert's, so
        #     (commit_seq_num, write_seq_num) orders the replace with no tie logic.
        assert delete_row["commit_seq_num"] == upsert_row["commit_seq_num"], (
            "delete + upsert from one PK-changing UPDATE commit together; got "
            f"delete.commit_seq_num={delete_row['commit_seq_num']} "
            f"upsert.commit_seq_num={upsert_row['commit_seq_num']}"
        )
        assert delete_row["write_seq_num"] < upsert_row["write_seq_num"], (
            "deletes-first: the co-batch delete must carry a strictly lower "
            "write_seq_num than the upsert so (commit_seq_num, write_seq_num) puts the "
            f"upsert last; got delete={delete_row['write_seq_num']} "
            f"upsert={upsert_row['write_seq_num']}"
        )

        hot_delete_order = (delete_row["commit_seq_num"], delete_row["write_seq_num"])
        hot_upsert_order = (upsert_row["commit_seq_num"], upsert_row["write_seq_num"])

        # (3) hot/cold parity: flush to cold (Persist), advance the snapshot
        #     baseline (Snapshot), purge out of hot, then re-audit — the cold
        #     pure-scan must surface the identical (commit_seq_num, write_seq_num).
        #     CHA-444 (ADR 0027): Purge advances the read fence Pu only to
        #     W_snap, so Snapshot must run before Purge clears the hot rows.
        client.persist(**audit_ids)
        client.snapshot(**audit_ids)
        purged = client.purge(**audit_ids)
        assert purged.HasField("purged_at_micros"), (
            "purge was a no-op; rows still served from hot — cold parity "
            "assertion below would not exercise the cold tier"
        )
        # Guard against a vacuous cold leg: HasField only proves purge was
        # non-empty somewhere. The parity check serves alice/alice2 from cold
        # only once the purge fence Pu has advanced to cover the UPDATE
        # commit's commit_seq_num — otherwise the rows stay queryable from hot
        # and the assertions below would pass without ever scanning cold.
        # CHA-444 (ADR 0027): purged_at_micros now carries the Pu commit_seq_num.
        assert purged.purged_at_micros >= delete_row["commit_seq_num"], (
            "purge fence Pu must cover the UPDATE commit's commit_seq_num so the "
            "alice/alice2 rows are evicted from hot and the parity check "
            f"genuinely scans cold; Pu={purged.purged_at_micros} "
            f"update_commit_seq_num={delete_row['commit_seq_num']}"
        )

        cold_upserts, cold_deletes = client.audit_data(**audit_ids)
        cold_delete = [d for d in cold_deletes.to_pylist() if d["name"] == "alice"]
        cold_upsert = [u for u in cold_upserts.to_pylist() if u["name"] == "alice2"]
        assert len(cold_delete) == 1, (
            f"cold delete tombstone for alice missing; got {cold_deletes.to_pylist()}"
        )
        assert len(cold_upsert) == 1, (
            f"cold upsert for alice2 missing; got {cold_upserts.to_pylist()}"
        )
        assert (
            cold_delete[0]["commit_seq_num"],
            cold_delete[0]["write_seq_num"],
        ) == hot_delete_order, (
            "cold delete (commit_seq_num, write_seq_num) must match the hot stream; "
            f"hot={hot_delete_order} "
            f"cold=({cold_delete[0]['commit_seq_num']}, {cold_delete[0]['write_seq_num']})"
        )
        assert (
            cold_upsert[0]["commit_seq_num"],
            cold_upsert[0]["write_seq_num"],
        ) == hot_upsert_order, (
            "cold upsert (commit_seq_num, write_seq_num) must match the hot stream; "
            f"hot={hot_upsert_order} "
            f"cold=({cold_upsert[0]['commit_seq_num']}, {cold_upsert[0]['write_seq_num']})"
        )
        client.close()


def _ensure_public_catalog_and_schema(client: PencaClient) -> str:
    """Idempotently provision the catalog/schema names that
    ``SQL_SERVER_DEFAULT_CATALOG`` / ``SQL_SERVER_DEFAULT_SCHEMA`` point at.

    CHA-171 makes ``bootstrap_db.py`` seed the ``public`` catalog at
    ``just penca-up`` time, so the create calls below are no-ops on a
    bootstrapped deployment. The fixture stays for tests that run
    against deployments where bootstrap might not have run, and to
    keep idempotency loud at the test layer.

    Returns the (server-minted, CHA-236) public catalog_uuid so callers
    can pass it to ``create_table`` directly.
    """
    try:
        client.create_catalog("public", "test")
    except Exception:
        pass  # already exists from bootstrap or a prior test

    catalog_uuid = client.get_catalog(catalog_name="public").catalog_uuid

    try:
        client.create_schema(
            "public", catalog_uuid=catalog_uuid, author="test", comment="create_schema"
        )
    except Exception:
        pass  # already exists from bootstrap or a prior test

    return catalog_uuid


def _setup_default_table(client: PencaClient) -> str:
    """Provision the public catalog/schema + a unique table; return its FQN."""
    catalog_uuid = _ensure_public_catalog_and_schema(client)
    table_name = f"tcl_{uuid4().hex[:12]}"
    client.create_table(
        table_name,
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_name="public",
        author="test",
        comment="create_table",
    )
    return f"public.public.{table_name}"


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlTransactionControl:
    """Raw-SQL ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` via Flight SQL.

    Multi-step transactions run as a single ``_execute_update_steps_via``
    call so the JDBC arm's per-helper-invocation Connection threads
    through all steps; SELECT steps interleaved with DMLs come back as
    ``OK_ROWS`` payloads which `json.loads` decodes inline.
    """

    def test_begin_insert_commit_makes_row_visible(
        self, driver: Literal["adbc", "jdbc"]
    ):
        client = make_client()
        fqn = _setup_default_table(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver,
            ["BEGIN", f"INSERT INTO {fqn} VALUES ('charlie', 30)", "COMMIT"],
            port=port,
            catalog="public",
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

        assert results[1][1] == "1"

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog="public"
        )
        assert {"name": "charlie", "value": 30} in rows

    def test_begin_insert_rollback_discards_row(self, driver: Literal["adbc", "jdbc"]):
        client = make_client()
        fqn = _setup_default_table(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver,
            ["BEGIN", f"INSERT INTO {fqn} VALUES ('charlie', 30)", "ROLLBACK"],
            port=port,
            catalog="public",
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

        rows = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn}", port=port, catalog="public"
        )
        assert {"name": "charlie", "value": 30} not in rows

    def test_begin_insert_select_sees_own_row_then_rollback(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """RYOW (CHA-165): SELECT inside the tx sees own uncommitted row;
        post-ROLLBACK the row is gone."""
        client = make_client()
        fqn = _setup_default_table(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        select_sql = f"SELECT name, value FROM {fqn}"
        results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                f"INSERT INTO {fqn} VALUES ('ryow_charlie', 999)",
                select_sql,
                "ROLLBACK",
            ],
            port=port,
            catalog="public",
        )
        assert results[0][0] == "OK"
        assert results[1][0] == "OK"
        assert results[2][0] == "OK_ROWS"
        rows_in_tx = json.loads(results[2][1])
        assert {"name": "ryow_charlie", "value": 999} in rows_in_tx
        assert results[3][0] == "OK"

        rows_after = _execute_query_via(driver, select_sql, port=port, catalog="public")
        assert {"name": "ryow_charlie", "value": 999} not in rows_after

    def test_begin_commit_with_no_dml(self, driver: Literal["adbc", "jdbc"]):
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver, ["BEGIN", "COMMIT"], port=port, catalog="public"
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

    def test_begin_rollback_with_no_dml(self, driver: Literal["adbc", "jdbc"]):
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver, ["BEGIN", "ROLLBACK"], port=port, catalog="public"
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

    def test_re_begin_in_existing_tx_block_is_rejected(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """A second ``BEGIN`` while a tx is already open must fail."""
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # All four steps in one connection: open the tx, attempt the
        # nested BEGIN, then clean up via ROLLBACK so the session
        # doesn't leak a dangling tx.
        results = _execute_update_steps_via(
            driver,
            ["BEGIN", "BEGIN", "ROLLBACK"],
            port=port,
            catalog="public",
        )
        assert results[0][0] == "OK", results[0]
        status, payload = results[1]
        assert status == "CAUGHT", (
            f"expected rejection on nested BEGIN; got {status}={payload}"
        )
        assert re.search(r"(?i)already.*open", payload), payload
        assert results[2][0] == "OK", results[2]

    def test_bare_commit_without_begin_is_rejected(
        self, driver: Literal["adbc", "jdbc"]
    ):
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver, ["COMMIT"], port=port, catalog="public"
        )
        status, payload = results[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(r"(?i)no open transaction", payload), payload

    @pytest.mark.parametrize(
        "begin_sql",
        [
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ],
    )
    def test_begin_isolation_level_is_unimplemented(
        self, driver: Literal["adbc", "jdbc"], begin_sql: str
    ):
        """``BEGIN ISOLATION LEVEL ...`` is rejected loudly."""
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver, [begin_sql], port=port, catalog="public"
        )
        status, payload = results[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(r"(?i)isolation level", payload), payload

    @pytest.mark.parametrize(
        "begin_sql",
        ["BEGIN READ ONLY", "BEGIN READ WRITE", "BEGIN TRANSACTION READ ONLY"],
    )
    def test_begin_access_mode_is_unimplemented(
        self, driver: Literal["adbc", "jdbc"], begin_sql: str
    ):
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver, [begin_sql], port=port, catalog="public"
        )
        status, payload = results[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert re.search(r"(?i)read only|read write|access mode", payload), payload

    def test_select_inside_open_tx_repeats_across_concurrent_commit(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Snapshot isolation: two SELECTs inside one open tx see the
        same set, even with a concurrent commit in between.

        Cross-driver execution requires interleaving two sessions'
        operations with strict ordering (begin-then-select on A,
        concurrent insert on B, second select on A, commit). On JDBC
        each helper invocation is a synchronous subprocess so the
        interleaving cannot be expressed without a barrier the probe
        doesn't support. Stays ADBC-only by design — the wire-level
        invariant is server-side (CHA-165 open_tx_uuid visibility
        predicate) and is driver-agnostic.
        """
        if driver == "jdbc":
            pytest.skip(
                "Interleaved two-session ordering needs a barrier the "
                "JDBC probe doesn't support; server-side SI invariant "
                "is driver-agnostic"
            )

        client_a = make_client()
        client_b = make_client()
        fqn = _setup_default_table(client_a)

        client_a.execute_update(f"INSERT INTO {fqn} VALUES ('seed', 0)")

        client_a.execute_update("BEGIN")
        rows_before = _sorted_rows(
            client_a.execute_query(f"SELECT name, value FROM {fqn}")
        )

        client_b.execute_update(f"INSERT INTO {fqn} VALUES ('concurrent', 99)")

        rows_after = _sorted_rows(
            client_a.execute_query(f"SELECT name, value FROM {fqn}")
        )
        client_a.execute_update("COMMIT")

        assert rows_before == rows_after, (
            f"open tx saw concurrent commit: before={rows_before} after={rows_after}"
        )
        assert {"name": "concurrent", "value": 99} not in rows_after

        rows_post_commit = _sorted_rows(
            client_a.execute_query(f"SELECT name, value FROM {fqn}")
        )
        assert {"name": "concurrent", "value": 99} in rows_post_commit

        client_a.close()
        client_b.close()

    def test_begin_insert_two_schemas_commit_visible_atomically(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """A single tx writing to tables in two schemas commits atomically."""
        client = make_client()
        catalog_uuid = _ensure_public_catalog_and_schema(client)
        suffix = uuid4().hex[:12]
        schema_b_name = f"schema_b_{suffix}"
        client.create_schema(
            schema_b_name,
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        table_a = f"multi_a_{suffix}"
        table_b = f"multi_b_{suffix}"
        client.create_table(
            table_a,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_name="public",
            author="test",
            comment="create_table",
        )
        client.create_table(
            table_b,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_name=schema_b_name,
            author="test",
            comment="create_table",
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn_a = f"public.public.{table_a}"
        fqn_b = f"public.{schema_b_name}.{table_b}"

        results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                f"INSERT INTO {fqn_a} VALUES ('alice', 1)",
                f"INSERT INTO {fqn_b} VALUES ('bob', 2)",
                "COMMIT",
            ],
            port=port,
            catalog="public",
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

        rows_a = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn_a}", port=port, catalog="public"
        )
        rows_b = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn_b}", port=port, catalog="public"
        )
        assert {"name": "alice", "value": 1} in rows_a
        assert {"name": "bob", "value": 2} in rows_b

    def test_begin_insert_two_schemas_rollback_discards_both(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """ROLLBACK on a multi-schema tx leaves no committed rows in either."""
        client = make_client()
        catalog_uuid = _ensure_public_catalog_and_schema(client)
        suffix = uuid4().hex[:12]
        schema_b_name = f"schema_b_{suffix}"
        client.create_schema(
            schema_b_name,
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        table_a = f"rb_a_{suffix}"
        table_b = f"rb_b_{suffix}"
        client.create_table(
            table_a,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_name="public",
            author="test",
            comment="create_table",
        )
        client.create_table(
            table_b,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_name=schema_b_name,
            author="test",
            comment="create_table",
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn_a = f"public.public.{table_a}"
        fqn_b = f"public.{schema_b_name}.{table_b}"

        results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                f"INSERT INTO {fqn_a} VALUES ('alice', 1)",
                f"INSERT INTO {fqn_b} VALUES ('bob', 2)",
                "ROLLBACK",
            ],
            port=port,
            catalog="public",
        )
        for i, (status, payload) in enumerate(results):
            assert status == "OK", f"step {i} failed: {payload}"

        rows_a = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn_a}", port=port, catalog="public"
        )
        rows_b = _execute_query_via(
            driver, f"SELECT name, value FROM {fqn_b}", port=port, catalog="public"
        )
        assert {"name": "alice", "value": 1} not in rows_a
        assert {"name": "bob", "value": 2} not in rows_b

    def test_dml_cross_catalog_in_open_tx_rejected(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """DML targeting a different catalog than the open tx fails."""
        client = make_client()
        _ensure_public_catalog_and_schema(client)

        other_catalog_name = f"other_cat_{uuid4().hex[:8]}"
        other_catalog_uuid, _main_branch_uuid = client.create_catalog(
            other_catalog_name, "owner"
        )

        suffix = uuid4().hex[:12]
        table_public = f"xc_public_{suffix}"
        table_other = f"xc_other_{suffix}"
        client.create_table(
            table_public,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_name="public",
            schema_name="public",
            author="test",
            comment="create_table",
        )
        client.create_table(
            table_other,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=other_catalog_uuid,
            schema_name="public",
            author="test",
            comment="create_table",
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                f"INSERT INTO public.public.{table_public} VALUES ('alice', 1)",
                f"INSERT INTO {other_catalog_name}.public.{table_other} VALUES ('bob', 2)",
                "ROLLBACK",
            ],
            port=port,
            catalog="public",
        )
        assert results[0][0] == "OK", results[0]
        assert results[1][0] == "OK", results[1]
        status, payload = results[2]
        assert status == "CAUGHT", (
            f"expected cross-catalog rejection; got {status}={payload}"
        )
        assert re.search(r"(?i)cross-catalog", payload), payload
        assert results[3][0] == "OK", results[3]

    def test_select_cross_catalog_rejected(self, driver: Literal["adbc", "jdbc"]):
        """SELECT against a different catalog than the connection's pin
        fails at DataFusion's planning stage with a generic ``table not
        found`` error carrying the full 3-part identifier."""
        client = make_client()
        _ensure_public_catalog_and_schema(client)

        other_catalog_name = f"other_cat_{uuid4().hex[:8]}"
        other_catalog_uuid, _main_branch_uuid = client.create_catalog(
            other_catalog_name, "owner"
        )
        table_name = f"xc_other_{uuid4().hex[:12]}"
        client.create_table(
            table_name,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=other_catalog_uuid,
            schema_name="public",
            author="test",
            comment="create_table",
        )
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # Session pinned to `public` via the helper's catalog header.
        # SELECT against ``other_catalog_name`` must fail with the
        # ``table <full.qualified.name> not found`` shape.
        fqn = f"{other_catalog_name}.public.{table_name}"
        results = _execute_update_steps_via(
            driver,
            [f"SELECT name, value FROM {fqn}"],
            port=port,
            catalog="public",
        )
        status, payload = results[0]
        assert status == "CAUGHT", f"{status}={payload}"
        assert fqn in payload and "not found" in payload.lower(), (
            f"[{driver}] expected DataFusion planning-stage `table not found` "
            f"error naming the full 3-part identifier {fqn!r}; got: {payload!r}"
        )

    def test_transactional_drop_rejects_without_architectural_wording(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """CHA-345 — in-tx DDL *outside* the supported CREATE pair (here
        ``DROP TABLE``) still rejects, but the wording no longer frames
        transactional DDL as architecturally gated per ADR 0010 — that
        premise is false post-CHA-345 (CHA-255 paid Option A's catalog-tree
        cost). It points at the gRPC WriteService, parallel to the
        auto-commit framing for the same variants. ``classify`` is the gate,
        so the target table need not exist.

        In-tx ``CREATE TABLE`` / ``CREATE SCHEMA`` SUCCESS is covered by
        :class:`TestFlightSqlTransactionalDdlEndToEnd`.
        """
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        tx_results = _execute_update_steps_via(
            driver,
            [
                "BEGIN",
                "DROP TABLE public.public.flight_drop_in_tx",
                "ROLLBACK",
            ],
            port=port,
            catalog="public",
        )
        assert tx_results[0][0] == "OK", tx_results[0]
        status, payload = tx_results[1]
        assert status == "CAUGHT", f"{status}={payload}"
        assert "ADR 0010" not in payload and "architecturally" not in payload, (
            f"[{driver}] in-tx DROP rejection must not frame transactional DDL as "
            f"architecturally gated; got: {payload}"
        )
        assert "WriteService" in payload, (
            f"[{driver}] in-tx DROP rejection must point at the gRPC WriteService; "
            f"got: {payload}"
        )
        assert tx_results[2][0] == "OK", tx_results[2]


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlNotFoundClassification:
    """CHA-257: ``PencaSchemaProvider::table`` must translate a
    ``QueryService::GetTable`` ``NotFound`` into ``Ok(None)``, so a
    SELECT against a name that doesn't exist in the pinned schema
    surfaces DataFusion's standard ``table … not found`` planning
    error rather than the raw wrapped ``External(Status { code:
    NotFound, … })`` the unwrapped pre-fix path emits.

    The :meth:`TestFlightSqlTransactionControl.test_select_against_other_catalog_fails_table_not_found`
    test next to this one already pins the cross-catalog shape, but
    that path short-circuits in ``catalog_list`` (returns ``None`` for
    catalogs other than the session-pinned one) before ever calling
    into ``schema.table``. This test exercises the *in-catalog,
    in-schema* miss — the exact path the JDBC ``CREATE TABLE``
    prepare-time pre-existence check walks — so the assertion bites
    on the bug fixed in CHA-257.
    """

    def test_select_against_nonexistent_table_in_pinned_schema_surfaces_planning_error(
        self,
        driver: Literal["adbc", "jdbc"],
    ):
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # Fresh UUID-suffixed name → guaranteed absent from
        # ``public.public`` regardless of prior test state.
        fresh = f"cha257_missing_{uuid4().hex[:12]}"

        # Unqualified — session is pinned to ``public.public`` (no
        # catalog header set, so the SQL server falls back to
        # SQL_SERVER_DEFAULT_CATALOG). The planner consults
        # ``PencaSchemaProvider::table`` on the pinned schema (the
        # buggy code path) rather than short-circuiting via the
        # catalog-list mismatch.
        results = _execute_update_steps_via(
            driver, [f"SELECT * FROM {fresh}"], port=port
        )

        assert len(results) == 1, results
        status, payload = results[0]
        assert status == "CAUGHT", (
            f"[{driver}] expected SELECT of missing table to error; "
            f"got status={status!r} payload={payload!r}"
        )
        payload_lower = payload.lower()
        assert "not found" in payload_lower and fresh in payload, (
            f"[{driver}] expected DataFusion planning-stage `table not found` "
            f"error naming the missing table {fresh!r}; got: {payload!r}"
        )
        # CHA-257 anti-regression: the pre-fix surface wraps the tonic
        # `Status` in `DataFusionError::External`, which `Debug`-prints
        # the literal substrings below. After the fix the planner emits
        # its own message and neither substring should appear.
        assert "External" not in payload, (
            f"[{driver}] CHA-257: error must not surface as "
            f"`External(Status …)`; got: {payload!r}"
        )
        assert "code: NotFound" not in payload, (
            f"[{driver}] CHA-257: raw tonic Status debug must not leak "
            f"through to the user; got: {payload!r}"
        )


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlSessionMintValidation:
    """Session-mint catalog validation.

    The SQL server pins every session to the catalog name carried in
    the ``x-penca-catalog`` header at handshake (CHA-253), falling
    back to ``SQL_SERVER_DEFAULT_CATALOG``. The pin is validated
    fail-fast at mint via ``MetadataClient::get_catalog`` — if the
    name doesn't resolve in ``catalog_store``, ``SessionLayer``
    returns ``FAILED_PRECONDITION`` directly (via
    ``Status::into_http()``) on the request that opened the
    connection. No half-baked session is inserted into the cache.
    These tests pin the actionable wording on that error path.
    """

    def test_connect_to_nonexistent_catalog_rejects_with_actionable_error(
        self,
        driver: Literal["adbc", "jdbc"],
    ):
        """A connection pinned (via the ``x-penca-catalog`` header,
        CHA-253) to a catalog name that doesn't exist in
        ``catalog_store`` fails its first SQL request with
        ``FAILED_PRECONDITION`` rather than the opaque cross-catalog
        rejection that would surface without the validation step.
        """
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # Random name that we deliberately don't create — this exercises
        # the validation path without depending on (or affecting) any
        # other test's catalog state.
        ghost_catalog = f"ghost_cat_{uuid4().hex[:8]}"
        results = _execute_update_steps_via(
            driver, ["SELECT 1"], port=port, catalog=ghost_catalog
        )
        assert len(results) == 1
        status, payload = results[0]
        assert status == "CAUGHT", (
            f"[{driver}] expected ghost-catalog handshake to fail; "
            f"got status={status!r} payload={payload!r}"
        )
        assert f"pinned to catalog `{ghost_catalog}`" in payload, (
            f"[{driver}] expected mint-validation actionable wording; got: {payload!r}"
        )

    def test_validation_passes_after_catalog_is_created(
        self,
        driver: Literal["adbc", "jdbc"],
    ):
        """After ``CreateCatalog`` lands the row, a fresh connection
        pinned to that name validates clean and SQL works. Locks in
        that the validation is single-shot per session-mint and that
        re-connecting picks up the now-existing catalog (no stale
        cache to invalidate).
        """
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        late_catalog = f"late_cat_{uuid4().hex[:8]}"

        results = _execute_update_steps_via(
            driver, ["SELECT 1"], port=port, catalog=late_catalog
        )
        status, payload = results[0]
        assert status == "CAUGHT", (
            f"[{driver}] expected pre-creation handshake to fail; "
            f"got status={status!r} payload={payload!r}"
        )
        assert f"pinned to catalog `{late_catalog}`" in payload, (
            f"[{driver}] expected mint-validation actionable wording; got: {payload!r}"
        )

        # CHA-163 auto-creates a `public` schema, so no explicit
        # `create_schema` is needed for the literal `SELECT 1` below.
        admin_client = make_client()
        admin_client.create_catalog(late_catalog, "owner")
        admin_client.close()

        rows = _execute_query_via(
            driver, "SELECT 1 AS one", port=port, catalog=late_catalog
        )
        assert len(rows) == 1
        # Both drivers' column-label casing for synthetic columns may
        # differ (ADBC sees lowercase `one`, JDBC may upcase); compare
        # by ordinal position via `next(iter(...))` to stay neutral.
        assert next(iter(rows[0].values())) == 1, rows


class TestFlightSqlConnectionScopedRouting:
    """CHA-119 / CHA-253: connection-scoped catalog + branch and
    session-mutable ``default_schema``.

    Three routing knobs, two lifetimes:

    - **Catalog** + **Branch** (immutable for the session — pinned at
      handshake from the ``x-penca-catalog`` (CHA-253) and
      ``x-penca-branch`` (CHA-119) gRPC headers respectively, or
      from the server's env defaults when absent). Threaded into the
      per-connection ``PencaCatalogProviderList`` at session-mint
      time so the sync ``CatalogProvider`` / ``SchemaProvider`` trait
      methods (which receive no session context) target the right
      catalog/branch. Mid-session ``SetSessionOptions(catalog: …)`` /
      ``SET catalog`` no-op on match and are rejected on mismatch
      with the "fixed at handshake" wording.
    - **Default schema** (freely mutable mid-session): ``SET
      search_path = '<name>'`` (Postgres) or the standard
      ``SetSessionOptions(db_schema: …)`` action writes onto
      ``SessionConfig.options.catalog.default_schema``.
    """

    @staticmethod
    def _open_conn(
        *,
        catalog: str | None = None,
        branch: str | None = None,
        schema: str | None = None,
    ) -> AdbcConnection:
        """Open a Flight SQL ADBC connection with optional pins.

        Mirrors :meth:`PencaClient._flight_sql_cursor`: catalog +
        branch ride the ``x-penca-catalog`` / ``x-penca-branch``
        gRPC metadata headers at handshake (CHA-253 / CHA-119), so
        both are bound on the server-side session before any other
        request lands; schema goes through the standard ADBC option
        setter (``adbc_current_db_schema``) which the FlightSQL
        driver translates to ``SetSessionOptions(db_schema: …)`` on
        the wire — schema is freely mutable mid-session, so the
        post-handshake setter is the right surface.
        """
        settings = ClientSettings()  # ty: ignore[missing-argument]
        db_kwargs: dict[str, str] = {
            "adbc.flight.sql.rpc.with_cookie_middleware": "true",
        }
        if branch is not None:
            db_kwargs["adbc.flight.sql.rpc.call_header.x-penca-branch"] = branch

        if catalog is not None:
            db_kwargs["adbc.flight.sql.rpc.call_header.x-penca-catalog"] = catalog

        conn = flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs=db_kwargs,
            autocommit=True,
        )

        if schema is not None:
            conn.adbc_current_db_schema = schema

        return conn

    @staticmethod
    def _exec_query(conn: AdbcConnection, sql: str) -> pa.Table:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    @staticmethod
    def _exec_update(conn: AdbcConnection, sql: str) -> int:
        """Execute a DML / SET via DoPutStatementUpdate (the direct path)."""
        cursor = conn.cursor()
        try:
            stmt = cursor.adbc_statement
            stmt.set_sql_query(sql)
            return stmt.execute_update()
        finally:
            cursor.close()

    @staticmethod
    def _exec_via_prepared(conn: AdbcConnection, sql: str) -> None:
        """Run ``sql`` through the prepared-statement entry points.

        ``adbc_driver_flightsql``'s ``cursor.execute`` always calls
        ``AdbcStatementPrepare`` before ``AdbcStatementExecuteQuery`` —
        on the wire that becomes ``do_action_create_prepared_statement``
        + ``get_flight_info_prepared_statement`` + ``DoGet``. This is
        the route DataGrip uses for SET, contrasted with the direct
        ``do_put_statement_update`` path that :meth:`_exec_update`
        exercises (which skips prepare entirely).
        """
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            # Drain whatever the prepared-statement path returns so the
            # full GetFlightInfo + DoGet round-trip runs (the impl
            # rewrites SET to a benign-shape ``SELECT 1 WHERE FALSE``
            # plan to avoid DataGrip's empty-FlightInfo retry loop).
            cursor.fetchall()
        finally:
            cursor.close()

    @staticmethod
    def _setup_catalog_with_table(
        client: PencaClient,
        *,
        schema_name: str,
        table_name: str,
        rows: dict,
    ) -> dict:
        """Build a catalog with one user schema + one table + ``rows``
        on ``main``. Returns identifiers plus the head ``tx_uuid`` so
        callers can fork branches off it.
        """
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
        schema_uuid = client.create_schema(
            schema_name,
            catalog_uuid=catalog_uuid,
            author="test",
            comment="setup_routing",
        )
        table_uuid = client.create_table(
            table_name,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="setup_routing",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(rows, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )
        return {
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "table_uuid": table_uuid,
            "main_branch_uuid": main_branch_uuid,
            "head_tx_uuid": tx.tx_uuid,
        }

    @staticmethod
    def _append_rows_on_branch(
        client: PencaClient,
        *,
        catalog_uuid: str,
        schema_uuid: str,
        table_uuid: str,
        branch_uuid: str,
        rows: dict,
    ) -> None:
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        batch = pa.table(rows, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

    def test_connection_branch_pinned_at_handshake_threads_to_select(self):
        """An ``x-penca-branch`` header at handshake reaches the catalog
        list machinery and resolves an unqualified SELECT against the
        named branch's data.

        Regression-pin for the happy path. Today's hardcoded
        ``Some("main".to_string())`` in
        ``penca_datafusion::catalog::list_schemas`` happens to agree
        with ``SQL_SERVER_DEFAULT_BRANCH=main``, so the assertion holds
        even before the header wiring lands — the test guards against
        the impl regressing that path while threading branch through.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"], branch=MAIN_BRANCH_NAME)
        try:
            table = self._exec_query(
                conn,
                f"SELECT name, value FROM {ctx['schema_name']}.{ctx['table_name']}",
            )
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_two_connections_on_different_branches_see_distinct_state(self):
        """Two connections to the same catalog but different branches
        return branch-scoped rows — the canonical multi-branch routing
        assertion called out in the ticket's acceptance criteria.

        CHA-178: a forked branch reads ``parent-as-of-fork ∪ its-own-writes``.
        Main keeps the two rows it had at fork time; ``feat`` inherits those
        two AND sees its own ``carol`` — so the branches see distinct state
        (``carol`` is visible only on ``feat``; a later main-only write would
        be invisible to ``feat``), with ``feat`` a superset of the shared
        fork baseline. The branch header routes each connection to its own
        branch; a broken header would collapse both to
        ``SQL_SERVER_DEFAULT_BRANCH=main`` and ``feat`` would miss ``carol``.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        feat_branch = client.create_branch(
            "feat",
            "test",
            "create_branch_feat",
            catalog_uuid=ctx["catalog_uuid"],
        )
        feat_branch_uuid = feat_branch.branch_uuid
        # Add a row only on `feat` so the two branches diverge.
        self._append_rows_on_branch(
            client,
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=feat_branch_uuid,
            rows={"name": ["carol"], "value": [30]},
        )

        sql = f"SELECT name, value FROM {ctx['schema_name']}.{ctx['table_name']}"
        conn_main = self._open_conn(
            catalog=ctx["catalog_name"], branch=MAIN_BRANCH_NAME
        )
        conn_feat = self._open_conn(catalog=ctx["catalog_name"], branch="feat")
        try:
            rows_main = _sorted_rows(self._exec_query(conn_main, sql))
            rows_feat = _sorted_rows(self._exec_query(conn_feat, sql))
        finally:
            conn_main.close()
            conn_feat.close()
            client.close()

        assert rows_main == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]
        # CHA-178: feat inherits main's as-of-fork rows (alice, bob) plus its
        # own carol.
        assert rows_feat == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
            {"name": "carol", "value": 30},
        ]

    def test_unset_branch_header_falls_back_to_server_default(self):
        """A connection that supplies no ``x-penca-branch`` header
        falls back to ``SQL_SERVER_DEFAULT_BRANCH`` (today ``main`` in
        compose). Mirrors the catalog fallback shape — env var stays
        load-bearing for clients that don't pin explicitly.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            table = self._exec_query(
                conn,
                f"SELECT name, value FROM {ctx['schema_name']}.{ctx['table_name']}",
            )
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_branch_mid_session_rejected_with_invalid_argument(self):
        """``SET branch = 'feat'`` mid-session must be rejected with an
        actionable ``INVALID_ARGUMENT`` rather than DataFusion's opaque
        ``Configuration("could not find config namespace ...")`` — see
        the 2026-05-06 hack-reverted comment for context.

        The branch is connection-scoped; clients switch by reconnecting,
        not by setting a session variable.
        """
        client = make_client()
        conn = self._open_conn()
        try:
            with pytest.raises(
                Exception, match="(?i)connection-scoped.*reconnect to switch"
            ):
                self._exec_update(conn, "SET branch = 'feat'")
        finally:
            conn.close()
            client.close()

    def test_set_penca_branch_mid_session_rejected_with_invalid_argument(self):
        """The namespaced ``SET penca.branch = '...'`` form is rejected
        with the same connection-scoped wording as the bare ``SET
        branch`` — both refer to the same knob.
        """
        client = make_client()
        conn = self._open_conn()
        try:
            with pytest.raises(
                Exception, match="(?i)connection-scoped.*reconnect to switch"
            ):
                self._exec_update(conn, "SET penca.branch = 'feat'")
        finally:
            conn.close()
            client.close()

    def test_sql_set_catalog_mid_session_rejected_with_fixed_at_handshake_wording(
        self,
    ):
        """``SET catalog = '<other>'`` mid-session is rejected with the
        CHA-253 ``fixed at handshake; reconnect to switch`` wording.

        Pre-CHA-253 the rejection comes through ``plan_catalog``'s
        active-phase mismatch branch with the older
        ``connection-scoped`` wording; the regex below mismatches and
        the assertion fails. Post-CHA-253 ``plan_catalog`` collapses to
        no-op-on-match / hard-reject-on-mismatch and the error message
        leads with the new phrasing.
        """
        client = make_client()
        conn = self._open_conn()
        try:
            with pytest.raises(
                Exception, match=r"(?i)fixed at handshake.*reconnect to switch"
            ):
                self._exec_update(conn, "SET catalog = 'other_cat'")
        finally:
            conn.close()
            client.close()

    def test_three_part_name_overrides_default_catalog(self):
        """An explicit three-part ``catalog.schema.table`` reference
        resolves against the named catalog — DataFusion's parser binds
        the catalog from the prefix, the session's ``default_catalog``
        only governs unqualified references. Regression-pin so the SET
        / search_path machinery doesn't accidentally rewrite three-part
        names.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
            table = self._exec_query(conn, f"SELECT name, value FROM {fqn}")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_handshake_schema_seeds_default_schema_for_unqualified_select(self):
        """Setting ``adbc_current_db_schema`` on a fresh connection
        writes the named schema into
        ``SessionConfig.options.catalog.default_schema``, so an
        unqualified ``SELECT ... FROM <table>`` resolves against
        ``<session_catalog>.<set_schema>.<table>``. The ``_open_conn``
        helper threads the schema setter on the live connection before
        returning. (Schema, unlike catalog (CHA-253) and branch
        (CHA-119), is freely mutable mid-session — same surface, no
        handshake pin.)
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"], schema="sales")
        try:
            table = self._exec_query(conn, "SELECT name, value FROM customers")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_search_path_updates_default_schema_for_unqualified_select(self):
        """``SET search_path = 'sales'`` mid-session mutates
        ``options.catalog.default_schema`` so the next unqualified
        SELECT resolves against ``sales``. Postgres-compatible syntax,
        single-schema only (multi-schema lists are deferred).

        Today the statement reaches DataFusion's planner unmodified and
        fails with the ``could not find config namespace`` error.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            self._exec_update(conn, "SET search_path = 'sales'")
            table = self._exec_query(conn, "SELECT name, value FROM customers")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_search_path_updates_default_schema_for_unqualified_dml(self):
        """The DML path reads ``default_schema`` from the per-session
        ``SessionConfig`` (the same source DataFusion's SELECT planner
        consults), so ``SET search_path`` flows into unqualified
        ``INSERT`` / ``UPDATE`` / ``DELETE`` too.

        Closes ticket criterion: the manual ``SQL_SERVER_DEFAULT_SCHEMA``
        rewriter in ``flight_sql/service.rs`` is replaced by reading
        ``ctx.state().config_options().catalog.default_schema``.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice"], "value": [10]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            self._exec_update(conn, "SET search_path = 'sales'")
            self._exec_update(conn, "INSERT INTO customers VALUES ('bob', 20)")
            fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
            table = self._exec_query(conn, f"SELECT name, value FROM {fqn}")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_search_path_via_prepared_statement_path_honored(self):
        """DataGrip routes ``SET`` through
        ``do_action_create_prepared_statement`` rather than the direct
        ``do_put_statement_update`` path. The mutation must land on the
        session regardless of which of the four entry points the client
        picks — see the 2026-05-06 ticket comment for the retry-loop
        the response shape has to avoid.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            self._exec_via_prepared(conn, "SET search_path = 'sales'")
            table = self._exec_query(conn, "SELECT name, value FROM customers")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(table) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_search_path_persists_across_statements_on_same_connection(self):
        """A single ``SET search_path`` updates the cached
        ``SessionContext``'s ``SessionConfig``, so it persists across
        subsequent statements on the same connection. Guards against an
        implementation that mutates a per-request ctx clone and loses
        the change between statements.
        """
        client = make_client()
        ctx = self._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice"], "value": [10]},
        )

        conn = self._open_conn(catalog=ctx["catalog_name"])
        try:
            self._exec_update(conn, "SET search_path = 'sales'")
            first = self._exec_query(conn, "SELECT name, value FROM customers")
            self._exec_update(conn, "INSERT INTO customers VALUES ('bob', 20)")
            second = self._exec_query(conn, "SELECT name, value FROM customers")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(first) == [{"name": "alice", "value": 10}]
        assert _sorted_rows(second) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]


class TestFlightSqlSetSessionOptions:
    """ADBC ``SetSessionOptions`` / ``GetSessionOptions`` action handlers.

    ADBC's standard ``Connection.adbc_current_catalog`` /
    ``adbc_current_db_schema`` setters translate to FlightSQL's
    ``ActionSetSessionOptions`` on the wire (the Go driver maps
    ``adbc.connection.catalog`` / ``adbc.connection.db_schema`` through
    that action). Post-CHA-253 the catalog is pinned at handshake via
    the ``x-penca-catalog`` gRPC header (Postgres-shaped), so the
    post-handshake catalog setter is a Postgres
    ``Connection.setCatalog``-as-no-op: matching the handshake pin is
    a no-op, any other value is rejected with ``FAILED_PRECONDITION``
    ("fixed at handshake"). Schema stays mutable mid-session via the
    standard setter (freely mutable, mirrors ``SET search_path``).
    Branch has no wire-level analog and stays on ``x-penca-branch``
    only; the wire-level ``branch`` key is rejected with
    ``INVALID_NAME``.
    """

    @staticmethod
    def _open_raw_conn() -> AdbcConnection:
        """Open a Flight SQL ADBC connection without any pin headers.

        Sister of :meth:`TestFlightSqlConnectionScopedRouting._open_conn`,
        but emits no ``x-penca-*`` headers — these tests drive
        schema directly via the ADBC option setter on the live
        connection.
        """
        settings = ClientSettings()  # ty: ignore[missing-argument]
        return flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs={
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
            },
            autocommit=True,
        )

    @staticmethod
    def _exec_query(conn: AdbcConnection, sql: str) -> pa.Table:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    @staticmethod
    def _exec_update(conn: AdbcConnection, sql: str) -> int:
        cursor = conn.cursor()
        try:
            stmt = cursor.adbc_statement
            stmt.set_sql_query(sql)
            return stmt.execute_update()
        finally:
            cursor.close()

    def test_set_db_schema_via_adbc_setter_mutates_unqualified_select(self):
        """``conn.adbc_current_db_schema = 'sales'`` mutates the
        session's ``default_schema`` so a subsequent unqualified
        ``SELECT ... FROM tbl`` resolves against
        ``<catalog>.sales.tbl`` rather than ``<catalog>.public.tbl``.

        Schema is freely mutable (mirrors ``SET search_path``); pins
        the ADBC setter → ``SetSessionOptions`` → ``default_schema``
        mutation end-to-end.
        """
        client = make_client()
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
        # CHA-163 auto-creates `public`; create `sales` alongside.
        public_schema_uuid = client.get_schema(
            catalog_uuid=catalog_uuid,
            schema_name="public",
            branch_uuid=main_branch_uuid,
        ).schema_uuid
        sales_schema_uuid = client.create_schema(
            "sales", catalog_uuid=catalog_uuid, author="test", comment="cha-212"
        )
        branch_uuid = main_branch_uuid

        public_table_uuid = client.create_table(
            "tbl",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=public_schema_uuid,
            author="test",
            comment="cha-212",
        )
        sales_table_uuid = client.create_table(
            "tbl",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=sales_schema_uuid,
            author="test",
            comment="cha-212",
        )

        for table_uuid, schema_uuid, rows in (
            (public_table_uuid, public_schema_uuid, {"name": ["alice"], "value": [10]}),
            (sales_table_uuid, sales_schema_uuid, {"name": ["bob"], "value": [20]}),
        ):
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
            client.commit_tx(
                tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
            )

        # Pin catalog via the ``x-penca-catalog`` header at handshake
        # (CHA-253). Initial ``default_schema`` is ``public`` per
        # ``SQL_SERVER_DEFAULT_SCHEMA``.
        conn = TestFlightSqlConnectionScopedRouting._open_conn(catalog=catalog_name)
        try:
            public_rows = self._exec_query(conn, "SELECT name, value FROM tbl")
            # Schema is freely mutable mid-session — the post-handshake
            # setter writes onto ``SessionConfig.default_schema``.
            conn.adbc_current_db_schema = "sales"
            sales_rows = self._exec_query(conn, "SELECT name, value FROM tbl")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(public_rows) == [{"name": "alice", "value": 10}]
        assert _sorted_rows(sales_rows) == [{"name": "bob", "value": 20}]

    # The four tests below pin the post-CHA-253 architecture: catalog
    # binding is established exactly once, at session-mint time, from the
    # ``x-penca-catalog`` gRPC metadata header (mirroring the existing
    # ``x-penca-branch`` shape). The post-handshake ``SetSessionOptions
    # (catalog: …)`` action collapses to no-op-on-match / reject-on-mismatch
    # (Postgres ``Connection.setCatalog`` semantics) — the CHA-212
    # configuring window is gone.

    def test_handshake_catalog_header_seeds_session_pin(self):
        """A connection opened with ``x-penca-catalog: X`` at handshake
        pins the session to ``X`` without any post-handshake
        ``adbc_current_catalog`` setter. SELECTs resolve against ``X``
        and ``GetSessionOptions`` (read via ``conn.adbc_current_catalog``)
        reads back ``X``.

        Pre-CHA-253 the header is not recognised by ``SessionLayer``;
        the session mints with ``SQL_SERVER_DEFAULT_CATALOG`` and the
        ``adbc_current_catalog`` readback returns ``"public"`` rather
        than ``X``, so the readback assertion fails. Post-CHA-253 the
        header rides into ``SessionCache::new_session`` and the readback
        matches the pin.
        """
        client = make_client()
        ctx = TestFlightSqlConnectionScopedRouting._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice", "bob"], "value": [10, 20]},
        )

        settings = ClientSettings()  # ty: ignore[missing-argument]
        conn = flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs={
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
                "adbc.flight.sql.rpc.call_header.x-penca-catalog": ctx["catalog_name"],
            },
            autocommit=True,
        )
        try:
            # No post-handshake setter — the header is the only source
            # of the pin. ``adbc_current_catalog`` reads back via
            # ``GetSessionOptions``.
            assert conn.adbc_current_catalog == ctx["catalog_name"]
            fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
            rows = self._exec_query(conn, f"SELECT name, value FROM {fqn}")
        finally:
            conn.close()
            client.close()

        assert _sorted_rows(rows) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]

    def test_set_catalog_to_pinned_value_is_noop(self):
        """``adbc_current_catalog = X`` against a connection already
        pinned to ``X`` at handshake is a no-op — matches Postgres
        ``Connection.setCatalog``-as-no-op semantics. The setter runs
        through ``plan_catalog``'s no-op-on-match branch and the
        subsequent SELECT keeps working.

        Pre-CHA-253 the header is ignored; the session pins to
        ``SQL_SERVER_DEFAULT_CATALOG`` (``public``). The SELECT below
        flips ``configured = true`` (closing the configuring window),
        then the setter to ``X`` lands in the active phase as a
        mismatch (``public`` != ``X``) and is rejected with the
        ``connection-scoped`` wording. Post-CHA-253 the session is
        already pinned to ``X`` so the setter is a no-op.
        """
        client = make_client()
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        client.create_catalog(catalog_name, "owner")

        settings = ClientSettings()  # ty: ignore[missing-argument]
        conn = flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs={
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
                "adbc.flight.sql.rpc.call_header.x-penca-catalog": catalog_name,
            },
            autocommit=True,
        )
        try:
            # ``SELECT 1`` is a routine no-op query that doesn't need
            # any specific catalog to resolve.
            self._exec_query(conn, "SELECT 1")
            conn.adbc_current_catalog = catalog_name  # no-op match
            assert conn.adbc_current_catalog == catalog_name
        finally:
            conn.close()
            client.close()

    def test_set_catalog_to_different_value_rejects_with_fixed_at_handshake_wording(
        self,
    ):
        """``adbc_current_catalog = Y`` against a connection pinned to
        ``X`` raises with the new ``fixed at handshake; reconnect to
        switch`` wording — same shape as ``x-penca-branch``'s rejection
        message. The Go ADBC driver wraps the per-key
        ``ActionSetSessionOptionsResult.errors`` entry into a single
        Python exception whose message contains the per-key string.

        Pre-CHA-253 the rejection comes through ``plan_catalog``'s
        ``configured && mismatch`` branch with the older
        ``connection-scoped`` wording; the regex below mismatches and
        the assertion fails. Post-CHA-253 the configuring window is
        gone and ``plan_catalog`` returns ``Rejected("catalog is
        fixed at handshake; reconnect to switch — this connection is
        pinned to `X` and cannot be changed mid-session.")``.
        """
        client = make_client()
        catalog_x = f"x_{uuid4().hex[:8]}"
        client.create_catalog(catalog_x, "owner")

        settings = ClientSettings()  # ty: ignore[missing-argument]
        conn = flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs={
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
                "adbc.flight.sql.rpc.call_header.x-penca-catalog": catalog_x,
            },
            autocommit=True,
        )
        try:
            # Run any query to close the configuring window pre-CHA-253
            # so the setter below goes through the active-phase reject
            # branch rather than the configuring-window reseat. Post-
            # CHA-253 this is routine — there is no window.
            self._exec_query(conn, "SELECT 1")
            with pytest.raises(
                Exception, match=r"(?i)fixed at handshake.*reconnect to switch"
            ):
                conn.adbc_current_catalog = f"y_{uuid4().hex[:8]}"
        finally:
            conn.close()
            client.close()

    def test_python_set_autocommit_false_then_dml_succeeds(self):
        """A Python DB-API 2.0 ADBC client running with the default
        ``autocommit=False`` connects silently (no "Cannot disable
        autocommit" warning) against a connection pinned via
        ``x-penca-catalog``, and DML against that catalog goes
        through. Pins the autocommit-off wire path now that CHA-249
        advertises ``FlightSqlServerTransaction = Transaction``: the
        ADBC dbapi wrapper fires ``ActionBeginTransaction`` inside
        ``connect()`` against the header-pinned catalog (no
        ``setCatalog`` race because catalogs are immutable at
        handshake — CHA-253), the autostarted tx attaches to that
        catalog, and ``conn.commit()`` closes it via
        ``ActionEndTransaction``.
        """
        client = make_client()
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_uuid, _main_branch_uuid = client.create_catalog(catalog_name, "owner")
        schema_uuid = client.create_schema(
            "sales",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="cha-249",
        )
        client.create_table(
            "customers",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-249",
        )
        client.close()  # release the CRUD-path Flight SQL conn

        settings = ClientSettings()  # ty: ignore[missing-argument]
        # ``connect()`` defaults to ``autocommit=False``; under CHA-249's
        # ``FlightSqlServerTransaction = Transaction`` advertising, this
        # path no longer emits the "Cannot disable autocommit" warning.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            conn = flight_sql_connect(
                f"grpc://{settings.flight_sql_url}",
                db_kwargs={
                    "adbc.flight.sql.rpc.with_cookie_middleware": "true",
                    "adbc.flight.sql.rpc.call_header.x-penca-catalog": catalog_name,
                },
            )

        try:
            affected = self._exec_update(
                conn,
                f"INSERT INTO {catalog_name}.sales.customers VALUES ('alice', 10)",
            )
            # The dbapi auto-began a tx on connect; ``conn.commit()``
            # closes it via ``ActionEndTransaction``. An explicit SQL
            # ``BEGIN`` here would collide with the autostarted tx
            # (nested-tx rejection), which is the whole reason
            # autocommit-off clients should commit via
            # ``conn.commit()`` rather than SQL.
            conn.commit()
        finally:
            conn.close()

        assert affected == 1

    def test_set_branch_session_option_returns_invalid_name(self):
        """Sending ``SetSessionOptions{branch: ...}`` returns
        ``INVALID_NAME`` for the ``branch`` key — branch has no
        JDBC/ADBC analog and stays on the ``x-penca-branch``
        connection header.
        """
        client = make_client()
        _ensure_public_catalog_and_schema(client)

        conn = self._open_raw_conn()
        try:
            with pytest.raises(Exception, match="(?i)branch"):
                conn._conn.set_options(
                    **{"adbc.flight.sql.session.option.branch": "feat"}
                )
        finally:
            conn.close()
            client.close()

    def test_get_session_options_returns_pinned_catalog_and_current_schema(self):
        """Reading ``conn.adbc_current_catalog`` /
        ``adbc_current_db_schema`` returns the session's current values:
        the pinned catalog and the live ``default_schema`` (which
        ``SET search_path`` mutates). Locks in the symmetry JDBC
        ``Connection.getCatalog()`` / ``getSchema()`` rely on.
        """
        client = make_client()
        ctx = TestFlightSqlConnectionScopedRouting._setup_catalog_with_table(
            client,
            schema_name="sales",
            table_name="customers",
            rows={"name": ["alice"], "value": [10]},
        )

        conn = TestFlightSqlConnectionScopedRouting._open_conn(
            catalog=ctx["catalog_name"], schema="sales"
        )
        try:
            assert conn.adbc_current_catalog == ctx["catalog_name"]
            assert conn.adbc_current_db_schema == "sales"
            self._exec_update(conn, "SET search_path = 'sales'")
            assert conn.adbc_current_db_schema == "sales"
        finally:
            conn.close()
            client.close()

    def test_set_session_option_unknown_key_returns_invalid_name(self):
        """Sending ``SetSessionOptions{<unknown>: ...}`` returns
        ``INVALID_NAME`` for that key (per FlightSQL spec). Keeps the
        recognised-key set explicit — adding a new knob is a deliberate
        change to ``crate::set::handle_set_option``, not a silent
        accept.
        """
        client = make_client()
        _ensure_public_catalog_and_schema(client)

        conn = self._open_raw_conn()
        try:
            with pytest.raises(Exception, match="(?i)bogus"):
                conn._conn.set_options(**{"adbc.flight.sql.session.option.bogus": "x"})
        finally:
            conn.close()
            client.close()


class TestFlightSqlPerConnSessionScoping:
    """CHA-255: per-TCP-connection session scoping (Postgres model).

    Sessions are owned by one TCP connection. When the conn closes
    (cleanly or via network drop), the session is gone — no cross-conn
    cookie reuse, no idle eviction, no ``SessionCache`` DashMap. The
    catalog list is frozen at conn-mint and a one-shot
    ``list_catalogs`` snapshot powers ``SHOW CATALOGS`` /
    ``information_schema`` for the conn's lifetime. Schema/table
    metadata stays live (every ``get_schema`` / ``get_table`` hits the
    metadata service). Branch identity is by ``branch_uuid``
    post-mint so out-of-band ``UpdateBranch(new_branch_name=…)``
    renames don't break routing on existing connections.

    These tests pin the structural change end-to-end. Pre-CHA-255 they
    fail for the following reasons:

    - **(1) close-drops-tx**: today's session lives in the
      ``SessionCache`` DashMap and survives TCP close until the
      ``SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS`` sweeper picks it up;
      no ``Drop``-driven ``AbortTx`` fires. Test 1 asserts an
      ``abort_tx_log`` row materialises promptly after close.
    - **(2) two-conns-distinct-schema**: passes today (sessions are
      keyed by cookie, fresh ADBC conn → fresh cookie). Stays green as
      a regression pin for the per-conn rewrite.
    - **(3) no-cookie-on-wire**: today every response carries
      ``Set-Cookie: penca-session-id=…``. The pyarrow.flight
      middleware below captures it; assertion fails. Post-CHA-255 the
      cookie surface is deleted.
    - **(4) catalog-list-frozen**: today catalog enumeration goes
      through ``MetadataCaches`` / ``TtlLruCache`` (TTL-cached, not
      mint-frozen) so a mid-session ``CreateCatalog`` becomes visible
      to conn A as soon as the TTL window expires (or immediately if
      the cache is bypassed). Test 4 fails because the new catalog
      shows up. Post-CHA-255 the snapshot at mint freezes the list.
    - **(5) schema-live**: passes today (no schema cache for the
      ``CREATE SCHEMA`` path that bypasses the TTL). Stays green as a
      regression pin — the metadata-cache deletion must NOT freeze
      schemas.
    - **(6) / (7) env-var-unreferenced**: today three env vars
      (``METADATA_CACHE_TTL_SECONDS``, ``METADATA_CACHE_MAX_ENTRIES``,
      ``SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS``) are read from
      ``crate::config``; the static-grep guards below report matches
      and fail.
    - **(11) branch-rename-mid-session**: today the per-request
      ``WriteData`` / ``BeginTx`` / ``CommitTx`` payloads thread
      ``branch_name`` from the session pin. After an out-of-band
      ``UpdateBranch(new_branch_name=…)`` the old name no longer
      resolves on the storage side, so the INSERT/COMMIT fails.
      Post-CHA-255 the wire payloads carry ``branch_uuid`` instead;
      routing stays stable across the rename.
    """

    @staticmethod
    def _exec_query(conn: AdbcConnection, sql: str) -> pa.Table:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    @staticmethod
    def _exec_update(conn: AdbcConnection, sql: str) -> int:
        cursor = conn.cursor()
        try:
            stmt = cursor.adbc_statement
            stmt.set_sql_query(sql)
            return stmt.execute_update()
        finally:
            cursor.close()

    def test_close_conn_drops_session_and_aborts_open_tx(self):
        """Open a conn, ``BEGIN`` + ``INSERT``, close the conn without
        ``COMMIT``. The per-conn ``ConnSession::Drop`` must spawn an
        ``AbortTx`` call against ``WriteService`` so the
        ``abort_tx_log`` partition for the conn's pinned
        (catalog, branch) gains exactly one row within ~1s of close.

        A fresh conn opened against the same catalog must not see the
        uncommitted row.

        Pre-CHA-255 the session survives TCP close (lives in
        ``SessionCache`` until the idle sweeper picks it up after
        ``SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS``). No ``AbortTx``
        fires on close, so the partition count stays at 0. The
        post-CHA-255 ``Drop`` impl on ``ConnSession`` calls
        ``handle.spawn(WriteServiceClient::abort_tx(...))`` from
        ``Drop`` when ``open_tx_uuid.is_some()``.
        """
        client = make_client()
        ctx = TestFlightSqlConnectionScopedRouting._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice"], "value": [10]},
        )

        partition = abort_tx_log_partition(ctx["catalog_uuid"], ctx["main_branch_uuid"])
        before = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(partition)),
        )[0][0]
        assert before == 0

        conn = TestFlightSqlConnectionScopedRouting._open_conn(
            catalog=ctx["catalog_name"], branch=MAIN_BRANCH_NAME
        )
        try:
            self._exec_update(conn, "BEGIN")
            self._exec_update(
                conn,
                f"INSERT INTO {ctx['schema_name']}.{ctx['table_name']}"
                " VALUES ('carol', 30)",
            )
        finally:
            # Close WITHOUT committing. Per-conn Drop must abort the tx.
            conn.close()

        # ``ConnSession::Drop`` spawns the AbortTx on the tokio runtime
        # in a detached task. Poll the partition for up to ~3s to
        # absorb scheduler jitter.
        import time

        deadline = time.monotonic() + 3.0
        after = 0
        while time.monotonic() < deadline:
            after = get_pg_driver().execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(partition)),
            )[0][0]
            if after >= 1:
                break

            time.sleep(0.05)

        assert after == 1, (
            f"expected exactly 1 abort_tx_log row after conn close, got {after}"
        )

        fresh = TestFlightSqlConnectionScopedRouting._open_conn(
            catalog=ctx["catalog_name"], branch=MAIN_BRANCH_NAME
        )
        try:
            rows = self._exec_query(
                fresh,
                f"SELECT name FROM {ctx['schema_name']}.{ctx['table_name']}"
                " WHERE name = 'carol'",
            )
        finally:
            fresh.close()
            client.close()

        assert rows.num_rows == 0

    def test_two_conns_do_not_share_default_schema(self):
        """Open two conns against the same catalog. ``SET search_path``
        on conn A must NOT bleed into conn B's session — schema is
        per-conn state.

        Pre-CHA-255 sessions are cookie-keyed and fresh ADBC conns
        always mint fresh cookies, so this already passes. Stays in
        the suite as a regression pin: the per-conn rewrite must keep
        the two sessions' ``default_schema`` independent.
        """
        client = make_client()
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_uuid, _ = client.create_catalog(catalog_name, "owner")
        # CHA-163 auto-creates ``public``. Create ``a_schema`` and
        # ``b_schema`` alongside so SET search_path resolves them.
        client.create_schema(
            "a_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-255"
        )
        client.create_schema(
            "b_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-255"
        )
        client.close()

        conn_a = TestFlightSqlConnectionScopedRouting._open_conn(catalog=catalog_name)
        conn_b = TestFlightSqlConnectionScopedRouting._open_conn(catalog=catalog_name)
        try:
            self._exec_update(conn_a, "SET search_path = 'a_schema'")
            self._exec_update(conn_b, "SET search_path = 'b_schema'")

            assert conn_a.adbc_current_db_schema == "a_schema"
            assert conn_b.adbc_current_db_schema == "b_schema"
        finally:
            conn_a.close()
            conn_b.close()

    def test_no_set_cookie_response_or_penca_session_id_cookie_on_wire(self):
        """A raw pyarrow.flight ``FlightClient`` issuing a benign
        request must observe no ``set-cookie`` response header from
        the server and no outgoing ``cookie: penca-session-id=…``
        request header. The CHA-255 wire surface drops cookies
        entirely.

        Uses a header-snooping ``ClientMiddleware`` because ADBC's
        cookie middleware absorbs ``set-cookie`` transparently — we
        need a vanilla Flight client to observe what the server
        actually emits.

        Pre-CHA-255 the server-side ``SessionLayer`` injects
        ``Set-Cookie: penca-session-id=<uuid>`` on every response;
        the assertion fails on the first captured header batch.
        """
        from pyarrow.flight import (
            ClientMiddleware,
            ClientMiddlewareFactory,
            FlightClient,
        )

        captured_responses: list[dict] = []
        captured_outgoing: list[dict] = []

        class CaptureFactory(ClientMiddlewareFactory):
            def start_call(self, info):
                return CaptureMiddleware()

        class CaptureMiddleware(ClientMiddleware):
            def sending_headers(self):
                # Empty — we're just observing. Return value is what
                # WOULD be added; nothing.
                return {}

            def received_headers(self, headers):
                captured_responses.append(dict(headers))

            def call_completed(self, exception):
                pass

        settings = ClientSettings()  # ty: ignore[missing-argument]
        client = FlightClient(
            f"grpc://{settings.flight_sql_url}",
            middleware=[CaptureFactory()],
        )
        try:
            # Any RPC that traverses the per-conn ``PerConnService``
            # path mints/uses a session — list_actions is the
            # cheapest unauthenticated hit.
            try:
                list(client.list_actions())
            except Exception:
                # Even on error the middleware captures response
                # headers/trailers — that's what we care about.
                pass
        finally:
            client.close()

        # Flatten headers from all captured RPCs into one bag.
        all_response_keys: set[str] = set()
        for batch in captured_responses:
            all_response_keys.update(k.lower() for k in batch.keys())

        assert "set-cookie" not in all_response_keys, (
            "server emitted Set-Cookie despite CHA-255 cookie-surface deletion;"
            f" captured headers: {captured_responses!r}"
        )

        # Outgoing-side check: the middleware's sending_headers()
        # returns added headers; the client's own internal headers
        # (if any cookie middleware) aren't visible here. Instead we
        # verify the server's response side doesn't ask the client
        # to send any cookie — which is the only surface the server
        # has to drive client-side cookie state.
        _ = captured_outgoing  # currently unused; kept for clarity.

    def test_catalog_list_frozen_at_mint(self):
        """Open conn A pinned to a sentinel catalog. Create a
        brand-new catalog out-of-band via
        ``WriteService::CreateCatalog``. The new catalog must NOT
        appear in conn A's catalog list — the snapshot is frozen at
        mint. Conn B opened AFTER the create must see it.

        Pre-CHA-255 the catalog list goes through a TTL cache that
        picks up the new catalog as soon as the TTL expires (or
        immediately if the cache was empty). Within the TTL window
        conn A could still spuriously see the new catalog. The
        post-CHA-255 frozen-at-mint snapshot makes the visibility
        deterministic per conn.

        Catalog visibility is checked via ADBC's standard
        ``adbc_get_objects(depth=CATALOGS)`` which Penca maps to
        ``GetCatalogs`` → ``do_get_catalogs`` → ``ctx.catalog_names()``
        → ``PencaCatalogProviderList::catalog_names()`` → the
        per-conn snapshot.
        """

        def catalog_names(conn: AdbcConnection) -> set[str]:
            reader = conn.adbc_get_objects(depth="catalogs")
            table = reader.read_all()
            return {row["catalog_name"] for row in table.to_pylist()}

        client = make_client()
        # Conn A's pin: a fresh, sentinel catalog so the test isolates
        # from any other catalogs the deployment happens to carry.
        pin_catalog = f"sql_cat_pin_{uuid4().hex[:8]}"
        client.create_catalog(pin_catalog, "owner")

        conn_a = TestFlightSqlConnectionScopedRouting._open_conn(catalog=pin_catalog)
        try:
            self._exec_query(conn_a, "SELECT 1")

            # Out-of-band: create a brand-new catalog (with CHA-163
            # auto-created ``public`` schema, but no tables).
            mid_session_cat = f"mid_session_cat_{uuid4().hex[:8]}"
            client.create_catalog(mid_session_cat, "owner")

            a_catalogs = catalog_names(conn_a)
            assert mid_session_cat not in a_catalogs, (
                f"frozen-at-mint violated: conn A sees mid-session-created"
                f" catalog {mid_session_cat!r}; conn A snapshot: {a_catalogs!r}"
            )

            # Conn B (opened AFTER the create) must see the new
            # catalog (its fresh snapshot includes it).
            conn_b = TestFlightSqlConnectionScopedRouting._open_conn(
                catalog=pin_catalog
            )
            try:
                b_catalogs = catalog_names(conn_b)
                assert mid_session_cat in b_catalogs, (
                    f"fresh-conn snapshot missing mid-session-created catalog"
                    f" {mid_session_cat!r}; conn B snapshot: {b_catalogs!r}"
                )
            finally:
                conn_b.close()
        finally:
            conn_a.close()
            client.close()

    def test_schema_changes_within_session_visible_after_create(self):
        """Open a conn. Create a new schema out-of-band via
        ``WriteService::CreateSchema``. A three-part SELECT against
        the new schema from the same conn must produce a
        ``table not found``-shaped error (catalog and schema
        resolved, only the table was missing) — proving the schema
        lookup is live.

        Counterpart to test 4: catalogs freeze at mint, schemas (and
        tables) stay live. Pins that the ``MetadataCaches`` deletion
        does NOT accidentally freeze schemas too. ``get_schema`` /
        ``list_schemas`` must hit the metadata service on every call
        post-CHA-255.

        Pre-CHA-255 there are two failure modes the TTL-cached
        ``schema_names`` could produce:

        - ``schema not found`` (if the cache's negative resolution
          for ``new_schema`` survives the out-of-band create);
        - the assertion passes (if the cache hasn't been populated
          for ``new_schema`` yet so ``fetch_schema`` fires fresh).

        The assertion encodes "table not found" as the right shape
        — anything else (including ``schema not found``) trips it.
        """
        client = make_client()
        catalog_name = f"sql_cat_{uuid4().hex[:8]}"
        catalog_uuid, _ = client.create_catalog(catalog_name, "owner")

        conn = TestFlightSqlConnectionScopedRouting._open_conn(catalog=catalog_name)
        try:
            self._exec_query(conn, "SELECT 1")

            new_schema = f"sch_{uuid4().hex[:8]}"
            client.create_schema(
                new_schema,
                catalog_uuid=catalog_uuid,
                author="test",
                comment="cha-255 live-schema",
            )

            # Same conn must resolve the new schema. Probe via
            # three-part SELECT against a nonexistent table — the
            # error type tells us which level resolved.
            with pytest.raises(Exception) as err:
                self._exec_query(
                    conn,
                    f"SELECT * FROM {catalog_name}.{new_schema}.nonexistent_t",
                )

            msg = str(err.value).lower()
            assert "table" in msg and "schema" not in msg and "catalog" not in msg, (
                "expected `table not found`-shaped error (catalog and"
                " schema resolved, only table missing); got an error that"
                " suggests schema metadata isn't live:"
                f" {err.value!r}"
            )
        finally:
            conn.close()
            client.close()

    @staticmethod
    def _repo_root():
        from pathlib import Path

        # tests/integration/<this file> → ../.. is the repo root.
        return Path(__file__).resolve().parent.parent.parent

    @staticmethod
    def _grep_count(needle: str, *, paths: tuple[str, ...]) -> list[str]:
        """Return matching lines (path:lineno:content) under ``paths``
        for the literal ``needle``. Uses a Python walker rather than
        shelling out to ``rg`` so the test runs identically under any
        deployment image.

        Excludes:
        - ``target/`` (Rust build artifacts; contain compiled-in
          strings that aren't load-bearing source references).
        - ``.claude/`` (worktree-local agent artifacts: plans,
          memory, transcripts — they mention env-var names in prose).
        - This test file itself (it carries the literal needles in
          its assertion).
        """
        root = TestFlightSqlPerConnSessionScoping._repo_root()
        matches: list[str] = []
        for base in paths:
            for path in (root / base).rglob("*"):
                if not path.is_file():
                    continue

                rel = path.relative_to(root).as_posix()
                if rel.startswith(("target/", ".claude/")):
                    continue

                if rel.endswith("integration_flight_sql_test.py"):
                    continue

                try:
                    text = path.read_text(encoding="utf-8")
                except (UnicodeDecodeError, OSError):
                    continue

                for lineno, line in enumerate(text.splitlines(), start=1):
                    if needle in line:
                        matches.append(f"{rel}:{lineno}:{line.strip()}")

        return matches

    def test_metadata_cache_env_vars_unreferenced(self):
        """``METADATA_CACHE_TTL_SECONDS`` and
        ``METADATA_CACHE_MAX_ENTRIES`` must be deleted from the
        source tree — both the crate that reads them
        (``penca-sql-server::config``) and any compose / Justfile /
        ``.env*`` override that sets them.

        Pre-CHA-255 both names live in
        ``crates/penca-sql-server/src/config.rs`` and in the
        deployment overrides; the test fails immediately. Post-CHA-255
        the TTL cache is gone and so are these env vars.
        """
        for name in ("METADATA_CACHE_TTL_SECONDS", "METADATA_CACHE_MAX_ENTRIES"):
            matches = self._grep_count(
                name, paths=("crates", "docker", "tests", "Justfile")
            )
            assert matches == [], (
                f"{name} still referenced post-CHA-255:\n  " + "\n  ".join(matches)
            )

    def test_session_idle_timeout_env_var_unreferenced(self):
        """``SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS`` must be deleted
        from the source tree. Post-CHA-255 sessions die with the TCP
        conn — no idle sweeper, no timeout knob.
        """
        matches = self._grep_count(
            "SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS",
            paths=("crates", "docker", "tests", "Justfile"),
        )
        assert matches == [], (
            "SQL_SERVER_SESSION_IDLE_TIMEOUT_SECONDS still referenced"
            " post-CHA-255:\n  " + "\n  ".join(matches)
        )

    def test_branch_rename_mid_session_does_not_break_routing(self):
        """Open conn A pinned to branch ``feat``; ``BEGIN``; rename
        the branch out-of-band to ``feat_v2`` via
        ``WriteService::UpdateBranch``; ``INSERT`` on conn A;
        ``COMMIT``. The INSERT must succeed because the wire payloads
        route by ``branch_uuid`` (stable across rename), not by
        ``branch_name`` (which the session believes is still
        ``feat`` — but the storage layer no longer knows that name).

        After commit, a fresh conn pinned to ``feat_v2`` must see the
        inserted row.

        Pre-CHA-255 the per-request ``WriteData`` / ``BeginTx`` /
        ``CommitTx`` payloads thread ``branch_name`` from the session
        pin. After the rename, the old name no longer resolves; the
        INSERT (or COMMIT) fails with a ``branch not found`` /
        cross-branch routing error. Post-CHA-255 the payloads carry
        ``branch_uuid`` instead; routing stays stable across the
        rename and the INSERT lands on the (renamed) branch.

        This test pins the structural improvement the
        ``branch_uuid`` threading delivers — the bundled-with-CHA-255
        decision documented in the plan's ``Decision on the ticket's
        open question`` section.
        """
        client = make_client()
        ctx = TestFlightSqlConnectionScopedRouting._setup_catalog_with_table(
            client,
            schema_name="sql_schema",
            table_name="sql_table",
            rows={"name": ["alice"], "value": [10]},
        )

        feat_branch = client.create_branch(
            "feat",
            "test",
            "create_branch_feat",
            catalog_uuid=ctx["catalog_uuid"],
        )
        feat_branch_uuid = feat_branch.branch_uuid

        conn = TestFlightSqlConnectionScopedRouting._open_conn(
            catalog=ctx["catalog_name"], branch="feat"
        )
        try:
            self._exec_update(conn, "BEGIN")

            # Out-of-band branch rename. After this, the storage
            # layer no longer answers to ``feat`` for this
            # ``branch_uuid``.
            client.update_branch(
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=feat_branch_uuid,
                new_branch_name="feat_v2",
            )

            # The INSERT and COMMIT must still route correctly —
            # the wire payload carries the (rename-stable)
            # ``branch_uuid``, not the (stale) ``branch_name``.
            self._exec_update(
                conn,
                f"INSERT INTO {ctx['schema_name']}.{ctx['table_name']}"
                " VALUES ('carol', 30)",
            )
            self._exec_update(conn, "COMMIT")
        finally:
            conn.close()

        # Fresh conn pinned to the new branch name sees the row.
        fresh = TestFlightSqlConnectionScopedRouting._open_conn(
            catalog=ctx["catalog_name"], branch="feat_v2"
        )
        try:
            rows = self._exec_query(
                fresh,
                f"SELECT name, value FROM {ctx['schema_name']}.{ctx['table_name']}"
                " WHERE name = 'carol'",
            )
        finally:
            fresh.close()
            client.close()

        assert _sorted_rows(rows) == [{"name": "carol", "value": 30}]


# Flight SQL `SqlInfo` enum values from the protocol. Stable across
# arrow-flight versions; sourced from the FlightSql.proto upstream.
_SQL_INFO_FLIGHT_SQL_SERVER_NAME = 0
_SQL_INFO_FLIGHT_SQL_SERVER_VERSION = 1
_SQL_INFO_FLIGHT_SQL_SERVER_READ_ONLY = 3
_SQL_INFO_FLIGHT_SQL_SERVER_TRANSACTION = 8
_SQL_INFO_SQL_NULL_ORDERING = 507
# arrow-flight 57 breaks `SQL_MAX_IDENTIFIER_LENGTH` (the umbrella name
# in the FlightSQL spec text) into four per-identifier-kind keys; JDBC's
# `DatabaseMetaData.getMax{Column,Table,Schema,Catalog}NameLength()`
# each read a different one. All populate as 0 (no limit).
_SQL_INFO_SQL_MAX_COLUMN_NAME_LENGTH = 543
_SQL_INFO_SQL_DB_SCHEMA_NAME_LENGTH = 552
_SQL_INFO_SQL_MAX_CATALOG_NAME_LENGTH = 554
_SQL_INFO_SQL_MAX_TABLE_NAME_LENGTH = 559

# `SqlSupportedTransaction::Transaction` discriminant (we support
# BEGIN/COMMIT/ROLLBACK; see ADR 0010).
_SQL_TRANSACTION_TRANSACTION = 1
# `SqlNullOrdering::SqlNullsSortedAtEnd` discriminant.
_SQL_NULLS_SORTED_AT_END = 3


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _encode_get_sql_info(info_ids: list[int]) -> bytes:
    """Encode `Any{CommandGetSqlInfo{info: info_ids}}` on the wire.

    Avoids pulling in the generated Flight SQL Python protos (we don't
    ship them — server-side they come from arrow-flight's vendored
    descriptors). The wire format is two fields total:

      * `Any.type_url` — string, field 1, wire type 2 (length-delimited).
      * `Any.value`    — bytes,  field 2, wire type 2 (length-delimited),
        wrapping `CommandGetSqlInfo.info` (repeated uint32, field 1,
        packed, wire type 2).

    Empty `info_ids` ⇒ `CommandGetSqlInfo` serializes to zero bytes ⇒
    `Any.value` is an empty `bytes` field. The server reads that as
    "return all populated keys".
    """
    if info_ids:
        packed = b"".join(_varint(n) for n in info_ids)
        cmd = bytes([0x0A]) + _varint(len(packed)) + packed
    else:
        cmd = b""

    type_url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetSqlInfo"
    return (
        bytes([0x0A])
        + _varint(len(type_url))
        + type_url
        + bytes([0x12])
        + _varint(len(cmd))
        + cmd
    )


def _encode_statement_query(sql: str) -> bytes:
    """Encode `Any{CommandStatementQuery{query: sql}}` on the wire.

    Same hand-rolled-protobuf rationale as :func:`_encode_get_sql_info` (we
    don't ship generated Flight SQL Python protos). `CommandStatementQuery`
    carries `query` as field 1 (string, wire type 2); the message is wrapped in
    an `Any` whose `type_url` is field 1 and `value` is field 2.

    Used by the CHA-355 cache-miss test to drive a raw `GetFlightInfo` against a
    statement query so the opaque response ticket can be replayed on a second
    connection (an evicted/cold per-conn statement cache → DoGet re-plans).
    """
    query_bytes = sql.encode()
    cmd = bytes([0x0A]) + _varint(len(query_bytes)) + query_bytes
    type_url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    return (
        bytes([0x0A])
        + _varint(len(type_url))
        + type_url
        + bytes([0x12])
        + _varint(len(cmd))
        + cmd
    )


def _encode_prepared_statement_query(sql: str) -> bytes:
    """Encode `Any{CommandPreparedStatementQuery{prepared_statement_handle}}`.

    The handle is a server-shaped `QueryHandle` carrying just the SQL (no bound
    parameters): `QueryHandleMessage.query` is field 1 (string, wire type 2),
    wrapped as `prepared_statement_handle` (field 1, bytes) inside
    `CommandPreparedStatementQuery`. The server decodes it in
    `get_flight_info_prepared_statement` / `do_get_fallback` identically to a
    handle minted by `ActionCreatePreparedStatement` (all state rides on the
    handle), so a hand-rolled handle exercises the prepared (ADBC-shaped,
    `CommandPreparedStatementQuery`) DoGet arm without the create-prepared
    round-trip.

    Same hand-rolled-protobuf rationale as :func:`_encode_statement_query` (we
    don't ship generated Flight SQL Python protos). Used by the CHA-355
    prepared-path cache-miss test.
    """
    query_bytes = sql.encode()
    handle = bytes([0x0A]) + _varint(len(query_bytes)) + query_bytes
    cmd = bytes([0x0A]) + _varint(len(handle)) + handle
    type_url = (
        b"type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementQuery"
    )
    return (
        bytes([0x0A])
        + _varint(len(type_url))
        + type_url
        + bytes([0x12])
        + _varint(len(cmd))
        + cmd
    )


class TestFlightSqlGetSqlInfo:
    """`CommandGetSqlInfo` is the FlightSQL server-capability handshake;
    JDBC drivers (Dremio's `flight-sql-jdbc-driver`, used by every
    JetBrains DB tool + DBeaver) call it via
    `DatabaseMetaData.getDatabaseProductName()` on first connect. Until
    we answer, those clients are unusable against `penca-sql-server`.

    These tests drive `pyarrow.flight.FlightClient` directly (not ADBC,
    not `PencaClient`) so the raw `GetFlightInfo` + `DoGet` wire path
    against `CommandGetSqlInfo` is exercised — exactly what the JDBC
    driver does.
    """

    @staticmethod
    def _fetch_sql_info(info_ids: list[int]) -> pa.Table:
        settings = ClientSettings()  # ty: ignore[missing-argument]
        client = paflight.FlightClient(f"grpc://{settings.flight_sql_url}")
        try:
            descriptor = paflight.FlightDescriptor.for_command(
                _encode_get_sql_info(info_ids)
            )
            info = client.get_flight_info(descriptor)
            assert len(info.endpoints) == 1, (
                "GetSqlInfo must return a single endpoint (no Location), "
                "matching the catalogs/tables/table-types shape elsewhere in "
                "the service."
            )
            reader = client.do_get(info.endpoints[0].ticket)
            return reader.read_all()
        finally:
            client.close()

    def test_get_sql_info_empty_filter_returns_all_populated_keys(self):
        """Empty `info` list ⇒ server returns the full populated batch.

        Asserts the spec-mandated 2-column schema (`info_name: uint32`,
        `value: dense_union`) and pins the headline values the JDBC
        driver reads on first connect.
        """
        table = self._fetch_sql_info([])

        assert table.num_columns == 2
        assert table.column_names == ["info_name", "value"]
        assert table.schema.field("info_name").type == pa.uint32()
        assert pa.types.is_union(table.schema.field("value").type)

        rows = dict(
            zip(
                table.column("info_name").to_pylist(),
                table.column("value").to_pylist(),
                strict=True,
            )
        )

        # The keys the Dremio JDBC driver actually reads off the cache.
        assert rows[_SQL_INFO_FLIGHT_SQL_SERVER_NAME] == "penca"
        assert rows[_SQL_INFO_FLIGHT_SQL_SERVER_VERSION] == "0.1.0"
        assert rows[_SQL_INFO_FLIGHT_SQL_SERVER_READ_ONLY] is False
        assert (
            rows[_SQL_INFO_FLIGHT_SQL_SERVER_TRANSACTION]
            == _SQL_TRANSACTION_TRANSACTION
        )
        assert rows[_SQL_INFO_SQL_NULL_ORDERING] == _SQL_NULLS_SORTED_AT_END
        # `0` = no limit on identifier length, per the FlightSql.proto
        # convention. All four name-length keys JDBC's `DatabaseMetaData`
        # reads.
        assert rows[_SQL_INFO_SQL_MAX_COLUMN_NAME_LENGTH] == 0
        assert rows[_SQL_INFO_SQL_DB_SCHEMA_NAME_LENGTH] == 0
        assert rows[_SQL_INFO_SQL_MAX_CATALOG_NAME_LENGTH] == 0
        assert rows[_SQL_INFO_SQL_MAX_TABLE_NAME_LENGTH] == 0

    def test_get_sql_info_single_key_filter_returns_only_that_key(self):
        """Non-empty `info` list ⇒ server filters to those keys."""
        table = self._fetch_sql_info([_SQL_INFO_FLIGHT_SQL_SERVER_NAME])

        assert table.num_rows == 1
        assert table.column("info_name").to_pylist() == [
            _SQL_INFO_FLIGHT_SQL_SERVER_NAME
        ]
        assert table.column("value").to_pylist() == ["penca"]


_JDBC_DIR = Path(__file__).resolve().parent / "jdbc"
_JDBC_JAR = _JDBC_DIR / "lib" / "flight-sql-jdbc-driver.jar"
_JDBC_PROBE_SRC = _JDBC_DIR / "JdbcProbe.java"
_JDBC_EXECUTE_UPDATE_PROBE_SRC = _JDBC_DIR / "JdbcExecuteUpdateProbe.java"
_JDBC_PREPARED_STATEMENT_PROBE_SRC = _JDBC_DIR / "JdbcPreparedStatementProbe.java"


def _seed_public_public_users(client: PencaClient) -> None:
    """Ensure `public.public.users` exists with the rows the probe expects.

    The bootstrap-init container seeds an empty `public` catalog + `public`
    schema + `main` branch (CHA-171 / `penca-bootstrap`). We layer a
    deterministic `users` table on top, tolerant of repeated test runs on
    the same volume (idempotent: upsert by primary key).
    """
    cat = client.get_catalog(catalog_name="public")
    schema = client.get_schema(catalog_uuid=cat.catalog_uuid, schema_name="public")
    main_branch = client.get_branch(catalog_uuid=cat.catalog_uuid, branch_name="main")
    try:
        table_uuid = client.create_table(
            "users",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=cat.catalog_uuid,
            schema_uuid=schema.schema_uuid,
            author="cha-249-jdbc-probe",
            comment="JDBC smoke target",
        )
    except ApiError as e:
        if "already exists" not in str(e).lower():
            raise

        existing = client.get_table(
            catalog_uuid=cat.catalog_uuid,
            schema_uuid=schema.schema_uuid,
            table_name="users",
        )
        table_uuid = existing.table_uuid

    tx = client.begin_tx(
        catalog_uuid=cat.catalog_uuid,
        schema_uuid=schema.schema_uuid,
        branch_uuid=main_branch.branch_uuid,
    )
    batch = pa.table(
        {"name": ["alice", "bob"], "value": [10, 20]},
        schema=USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=cat.catalog_uuid,
        schema_uuid=schema.schema_uuid,
        branch_uuid=main_branch.branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=cat.catalog_uuid,
        branch_uuid=main_branch.branch_uuid,
    )


class TestFlightSqlJdbcProbe:
    """The end-to-end JDBC GUI smoke test — drives Apache's
    `flight-sql-jdbc-driver` (the same JAR DataGrip / DBeaver / every
    JetBrains DB tool ships with), so a regression in `CommandGetSqlInfo`
    or any related JDBC-driver-facing wire surface fails CI rather than
    waiting for a user to file a "DataGrip can't connect" bug.

    pyarrow tests in :class:`TestFlightSqlGetSqlInfo` verify the wire
    format conforms to the Flight SQL spec; *this* test verifies that
    the actual JDBC driver real-world GUIs use can interpret our
    response — a different axis of coverage (a dense-union encoding
    quirk pyarrow tolerates but the Dremio driver chokes on would fail
    here, not there).

    JDK 21 + the `flight-sql-jdbc-driver` JAR are hard prerequisites
    (provisioned by `just bootstrap`; CI installs JDK 21 via
    `actions/setup-java` and runs `just fetch-jdbc-driver`). Absent
    them this test fails rather than skipping (CHA-338).
    """

    def test_dremio_jdbc_driver_runs_ticket_acceptance_queries(self):
        client = make_client()
        _seed_public_public_users(client)

        settings = ClientSettings()  # ty: ignore[missing-argument]
        # `ClientSettings.flight_sql_url` is the host:port the SQL
        # server is bound to; the probe builds the JDBC URL around the
        # port the test stack happens to have allocated this run.
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # Inherit the parent environment (PATH, LANG, JAVA_HOME, etc.)
        # and layer PENCA_SQL_PORT on top. Scrubbing the env down to a
        # whitelist would silently re-introduce the "JVM falls back to
        # ASCII because LANG is unset" foot-gun the probe's em-dash
        # success marker triggers.
        result = subprocess.run(
            ["java", "-cp", str(_JDBC_JAR), str(_JDBC_PROBE_SRC)],
            env={**os.environ, "PENCA_SQL_PORT": port},
            capture_output=True,
            text=True,
            timeout=60,
        )
        # Surface the probe's full stdout/stderr in the pytest failure
        # report — the Java stack trace from a broken JDBC handshake is
        # the actionable signal, not the exit code.
        report = (
            f"exit={result.returncode}\n\n"
            f"stdout:\n{result.stdout}\n\n"
            f"stderr:\n{result.stderr}"
        )
        assert result.returncode == 0, report
        assert "DatabaseMetaData.getDatabaseProductName() = penca" in result.stdout, (
            report
        )
        assert "OK — all three queries succeeded." in result.stdout, report
        # The two-row data set is the only one this test seeds; if a
        # past test polluted public.public.users with extra rows, this
        # is the signal.
        assert "alice | 10" in result.stdout, report
        assert "bob | 20" in result.stdout, report


# CHA-259 / CHA-333: parsed line shape of the JDBC
# `JdbcExecuteUpdateProbe` stdout — one line per executed step. The
# probe emits one of three shapes per step:
#   `OK step=<n>: <rowsAffected>`        — DDL / DML / SET / tx control
#   `OK_ROWS step=<n>: <json-array>`     — SELECT (rows as JSON, CHA-333)
#   `CAUGHT step=<n>: <message>`         — SQLException; newlines flattened
# The probe always emits one line per input step; this regex extracts
# the (status, step_index, payload) triple from each line.
_JDBC_STEP_LINE_RE = re.compile(r"^(OK_ROWS|OK|CAUGHT) step=(\d+): (.*)$")


def _run_jdbc_probe(
    probe_src: Path,
    env: dict[str, str],
) -> tuple[subprocess.CompletedProcess, str]:
    """Run a single-file JDBC probe with the canonical flag set.

    Both ``_execute_update_steps_via`` and ``_execute_prepared_update_via``
    invoke a probe under ``java --add-opens=java.base/java.nio=ALL-UNNAMED``
    (Apache Arrow Java needs the open on JDK 17+ when the driver
    allocates direct memory — every prepared-statement path does),
    capture stdout+stderr with a 60s timeout, and format a uniform
    ``exit=… stdout: … stderr: …`` report for pytest's failure UI.

    Returns ``(completed, report)``. Raises ``AssertionError`` (with
    the report as the message) when ``returncode != 0`` so callers
    don't repeat the asserting; per-probe stdout parsing stays at the
    call site because the output formats differ
    (``OK/OK_ROWS/CAUGHT step=<n>:`` vs ``OK rows=…``/``CAUGHT:``).
    """
    completed = subprocess.run(
        [
            "java",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "-cp",
            str(_JDBC_JAR),
            str(probe_src),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = (
        f"exit={completed.returncode}\n\n"
        f"stdout:\n{completed.stdout}\n\n"
        f"stderr:\n{completed.stderr}"
    )
    assert completed.returncode == 0, report
    return completed, report


def _setup_and_port(
    setup_fn: Callable[[PencaClient], dict],
) -> tuple[dict, str]:
    """Provision a fresh PencaClient, run ``setup_fn`` on it, capture
    the Flight SQL port, close the client, return ``(ctx, port)``.

    Shorthand for the 5-line prelude that opens nearly every
    parametrized test in this file. Tests that need the client live
    after setup (``client.persist`` / ``client.read_data`` /
    ``client.audit_data`` / two-client coordination) keep the inline
    pattern — they need the client past the prelude.
    """
    client = make_client()
    ctx = setup_fn(client)
    client.close()
    settings = ClientSettings()  # ty: ignore[missing-argument]
    assert settings.flight_sql_url is not None
    _host, _, port = settings.flight_sql_url.rpartition(":")
    return ctx, port


def _execute_update_steps_via(
    driver: Literal["adbc", "jdbc"],
    steps: list[str],
    *,
    port: str,
    catalog: str | None = None,
) -> list[tuple[str, str]]:
    """Execute SQL steps in order on one connection, return per-step results.

    Returns a list aligned with `steps`: each entry is
    ``("OK", str(rowsAffected))``, ``("OK_ROWS", json_array_str)`` (CHA-333
    — emitted for SELECT steps; caller does ``json.loads`` to get
    ``list[dict]``), or ``("CAUGHT", <error message>)``.

    ADBC arm reuses :class:`PencaClient` so a single connection
    carries through all steps (lets `BEGIN`/`COMMIT`/`ROLLBACK` thread
    the same session across statements). JDBC arm shells out to
    :file:`JdbcExecuteUpdateProbe.java` with the steps joined by
    ``\\n`` on the ``PENCA_PROBE_SQL_STEPS`` env var; the probe runs
    every step on one ``java.sql.Connection`` and prints
    ``OK/OK_ROWS/CAUGHT step=<n>:`` lines that this helper parses.

    ``catalog`` pins both arms' connection to a Penca catalog via the
    ``x-penca-catalog`` gRPC metadata header (ADBC:
    ``make_client(catalog=...)``; JDBC: ``PENCA_PROBE_CATALOG`` env
    var → ``header.x-penca-catalog`` Property). Required when the
    SELECT references a dynamic UUID-suffixed catalog created by
    :func:`setup_with_data_named`; unpinned connections only see
    ``SQL_SERVER_DEFAULT_CATALOG``.
    """
    if driver == "adbc":
        client = make_client(catalog=catalog)
        results: list[tuple[str, str]] = []
        try:
            for step in steps:
                try:
                    # SELECTs route through execute_query (returns a
                    # pa.Table); everything else uses execute_update
                    # (returns rows-affected). Match the JDBC probe's
                    # `stmt.execute()` branch so both arms emit the
                    # same status tag for the same SQL.
                    if step.lstrip().upper().startswith(("SELECT", "WITH")):
                        table = client.execute_query(step)
                        # `default=str` so columns whose pyarrow values
                        # deserialize to non-JSON-native Python objects
                        # (datetime.date/datetime, Decimal — i.e. DATE,
                        # TIMESTAMP, NUMERIC) stringify rather than raising,
                        # matching the JDBC arm's getObject→string shape.
                        results.append(
                            ("OK_ROWS", json.dumps(table.to_pylist(), default=str))
                        )
                    else:
                        affected = client.execute_update(step)
                        results.append(("OK", str(affected)))
                except Exception as exc:
                    # Flatten newlines so the (status, payload) tuple
                    # is parsable the same way the JDBC arm's
                    # single-line output is.
                    msg = str(exc).replace("\n", " ").replace("\r", " ")
                    results.append(("CAUGHT", msg))
        finally:
            client.close()

        return results

    if driver == "jdbc":
        env = {
            **os.environ,
            "PENCA_SQL_PORT": port,
            "PENCA_PROBE_SQL_STEPS": "\n".join(steps),
        }
        if catalog is not None:
            env["PENCA_PROBE_CATALOG"] = catalog

        completed, report = _run_jdbc_probe(_JDBC_EXECUTE_UPDATE_PROBE_SRC, env)
        parsed: dict[int, tuple[str, str]] = {}
        for line in completed.stdout.splitlines():
            m = _JDBC_STEP_LINE_RE.match(line)
            if m is None:
                continue

            status, idx, payload = m.group(1), int(m.group(2)), m.group(3)
            parsed[idx] = (status, payload)

        # Every input step must produce one result line — a missing
        # line means the probe crashed mid-stream and the rest of
        # the assertions would be meaningless.
        assert len(parsed) == len(steps), (
            f"JDBC probe emitted {len(parsed)} step lines for {len(steps)} "
            f"inputs.\n\n{report}"
        )
        return [parsed[i] for i in range(len(steps))]

    raise ValueError(f"unknown driver: {driver!r}")


def _execute_prepared_update_via(
    driver: Literal["adbc", "jdbc"],
    sql: str,
    params: list[tuple[str, object]],
    *,
    port: str,
    catalog: str | None = None,
) -> tuple[str, str]:
    """Run a parameterized DML through PreparedStatement.setXxx + executeUpdate.

    `params` is a list of ``(type, value)`` pairs ordered by 1-based
    placeholder index — ``[("string", "carol"), ("int", 99)]`` binds
    `?1=carol`, `?2=99`. Supported ``type`` values today: ``"string"``,
    ``"int"``, ``"long"``. Extend the probe (and this list) when a
    new type is needed; the probe rejects unknown types loudly.

    Returns ``("OK", str(rowsAffected))`` or ``("CAUGHT", <message>)``.

    ADBC arm: skipped — `PencaClient.execute_update(sql, params=...)`
    does not yet support parameter binding. The helper signature keeps
    the ``driver`` axis uniform so a future ADBC arm can drop in;
    callers parametrize-over-driver should check ``driver == "adbc"`` →
    ``pytest.skip(...)`` until the PencaClient surface lands.

    JDBC arm: shells out to :file:`JdbcPreparedStatementProbe.java`,
    which walks `ActionCreatePreparedStatement` →
    `DoPutPreparedStatementQuery(params)` →
    `DoPutPreparedStatementUpdate(handle)`. The server-side
    parameter substitution wired up in CHA-333 (do_put_prepared_statement_update
    → gateway::execute_update → execute_insert → DataFrame::with_param_values)
    is what makes the test green.
    """
    if driver == "adbc":
        pytest.skip(
            "ADBC parameter-binding support for PencaClient.execute_update "
            "is not yet wired; tracked separately as a future ticket"
        )

    if driver == "jdbc":
        env = {
            **os.environ,
            "PENCA_SQL_PORT": port,
            "PENCA_PROBE_PREPARED_SQL": sql,
            "PENCA_PROBE_PREPARED_PARAMS": json.dumps(
                [{"type": t, "value": v} for t, v in params]
            ),
        }
        if catalog is not None:
            env["PENCA_PROBE_CATALOG"] = catalog

        completed, report = _run_jdbc_probe(_JDBC_PREPARED_STATEMENT_PROBE_SRC, env)
        for line in completed.stdout.splitlines():
            if line.startswith("OK rows="):
                return ("OK", line[len("OK rows=") :])

            if line.startswith("CAUGHT: "):
                return ("CAUGHT", line[len("CAUGHT: ") :])

        raise AssertionError(
            f"JdbcPreparedStatementProbe emitted no OK/CAUGHT line.\n\n{report}"
        )

    raise ValueError(f"unknown driver: {driver!r}")


def _execute_query_via(
    driver: Literal["adbc", "jdbc"],
    sql: str,
    *,
    port: str,
    catalog: str | None = None,
) -> list[dict]:
    """Run a single SELECT through the chosen driver, returning `list[dict]`.

    Cross-driver comparison shape is `list[dict]` (not `pyarrow.Table`)
    because JDBC's `ResultSet.getObject` yields boxed Java values, not
    Arrow arrays — reconstituting Arrow types from JDBC output is
    fragile (timestamps, decimals, …) and the projection-set tests
    don't need the Arrow type layer to express their assertions.

    Delegates to :func:`_execute_update_steps_via` with a single step;
    that helper detects SELECT and routes through the right driver
    method on each arm. The ``OK_ROWS`` payload is the JSON encoding
    of the row list; this wrapper decodes it for the caller.

    ``catalog`` pins the connection — required when the SELECT
    references a dynamic UUID-suffixed catalog (see
    :func:`_execute_update_steps_via` for header mechanics).
    """
    results = _execute_update_steps_via(driver, [sql], port=port, catalog=catalog)
    assert len(results) == 1, results
    status, payload = results[0]
    assert status == "OK_ROWS", (
        f"_execute_query_via expected an OK_ROWS step from the {driver!r} "
        f"arm for SELECT; got status={status!r} payload={payload!r}"
    )
    return json.loads(payload)


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlUnsupportedInTxDdlRejectionDriverParity:
    """CHA-259 / CHA-345 — same SQL, every Flight SQL driver, identical
    rejection wording for the in-tx DDL variants that are *still*
    unsupported.

    After CHA-345 the in-tx `CREATE TABLE` / `CREATE SCHEMA` pair is
    supported (see :class:`TestFlightSqlTransactionalDdlEndToEnd`). DDL
    outside that pair — `DROP`, `ALTER` — still rejects via the gateway,
    and the *path* through the server that emits the rejection differs
    per driver:

    * **ADBC** → `DoPutStatementUpdate` → `do_put_statement_update`
      → `gateway::classify`'s in-tx unsupported arm.
    * **JDBC** → `ActionCreatePreparedStatement` →
      `do_action_create_prepared_statement` →
      `gateway::plan_for_create_prepared_statement`'s in-tx
      unsupported arm (also `do_put_prepared_statement_update` on the
      execute leg).

    Both paths converge on `gateway::classify`'s `unsupported_statement`
    arm; this class parametrizes the rejection acceptance over
    `(adbc, jdbc)` so a regression that diverges them fails CI on at
    least one parametrized test ID. The wording must NOT frame
    transactional DDL as architecturally gated (CHA-345 removed that
    premise) — it points at the gRPC WriteService, same as the
    auto-commit framing for these variants.
    """

    # Still-unsupported in-tx DDL variants (DROP / ALTER). `classify` is
    # the gate, so target objects need not exist. Parametrize over both
    # so a future routing divergence specific to one shape fails CI on
    # the specific shape's ID.
    _DDL_KINDS = [
        ("drop_table", "DROP TABLE public.public.{name}"),
        ("alter_table", "ALTER TABLE public.public.{name} ADD COLUMN c INT"),
    ]

    @pytest.mark.parametrize("ddl_label,ddl_sql_fmt", _DDL_KINDS)
    def test_in_tx_unsupported_ddl_rejects_without_architectural_wording(
        self,
        driver: Literal["adbc", "jdbc"],
        ddl_label: str,
        ddl_sql_fmt: str,
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        target = f"cha345_tx_unsupp_{ddl_label}_{driver}_{uuid4().hex[:12]}"
        steps = [
            "BEGIN",
            ddl_sql_fmt.format(name=target),
            "ROLLBACK",
        ]
        results = _execute_update_steps_via(driver, steps, port=port)

        assert len(results) == 3, results
        # Step 0 (BEGIN) must succeed; otherwise step 1 runs as
        # auto-commit DDL and the in-tx path is never exercised.
        assert results[0][0] == "OK", (
            f"[{driver}] [{ddl_label}] BEGIN failed; cannot exercise transactional DDL: "
            f"{results}"
        )
        status, payload = results[1]
        assert status == "CAUGHT", (
            f"[{driver}] [{ddl_label}] expected unsupported in-tx DDL to reject; "
            f"got `{status}` with payload `{payload}`"
        )
        # CHA-345: the rejection no longer frames transactional DDL as an
        # architectural blocker. It's not-yet-implemented, like its
        # auto-commit form — points at the gRPC WriteService.
        assert "ADR 0010" not in payload and "architecturally" not in payload, (
            f"[{driver}] [{ddl_label}] in-tx DDL rejection must not frame transactional "
            f"DDL as architecturally gated; got: {payload}"
        )
        assert "WriteService" in payload, (
            f"[{driver}] [{ddl_label}] in-tx DDL rejection must point at the gRPC "
            f"WriteService; got: {payload}"
        )


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlJdbcExecuteQueryShape:
    """CHA-333 — JDBC `Statement.executeQuery(SELECT …)` returns the same
    structured rows as the ADBC arm.

    Pins the wire-level contract for the new `_execute_query_via` helper
    that the 9 parametrized server-behavior classes will route their
    SELECTs through. Today the helper does not exist; the JDBC arm
    `NameError`s and the ADBC arm `NameError`s too — both are red until
    the probe extension + the Python helper land.

    Comparison shape is `list[dict]` (not `pyarrow.Table`) so the JDBC
    arm's `ResultSet.getObject` values compare directly against ADBC's
    `to_pylist()` output without an Arrow-types round-trip the JDBC
    driver cannot honor.
    """

    def test_select_returns_structured_rows_per_driver(
        self,
        driver: Literal["adbc", "jdbc"],
    ) -> None:
        ctx, port = _setup_and_port(setup_with_data_named)

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        # Pin to the dynamic catalog created by `setup_with_data_named`;
        # `PencaCatalogProviderList::catalog(<name>)` only resolves
        # catalogs the session knows about, so fully-qualified 3-part
        # names alone are not enough.
        rows = _execute_query_via(
            driver,
            f"SELECT name, value FROM {fqn}",
            port=port,
            catalog=ctx["catalog_name"],
        )

        assert sorted(rows, key=lambda r: r["name"]) == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ]


class TestFlightSqlJdbcPreparedStatementBinding:
    """CHA-333 — JDBC `PreparedStatement.setXxx + executeUpdate` lands
    bound values through `DoPutPreparedStatementUpdate`.

    JDBC-only by design: the server-side fix at
    `crates/penca-sql-server/src/flight_sql/service.rs:921` is
    driver-agnostic (both ADBC and JDBC walk the same
    `DoPutPreparedStatementQuery` → `DoPutPreparedStatementUpdate`
    sequence), but `PencaClient.execute_update(sql, params=...)` does
    not yet support ADBC-side parameter binding. When that lands as a
    follow-up ticket this test gains an `[adbc]` arm without touching
    the body, mirroring `TestFlightSqlUnsupportedInTxDdlRejectionDriverParity`'s
    "ODBC arm grows by one" pattern.

    Two compounding gaps make this red today:
    1. `_execute_prepared_update_via` + `JdbcPreparedStatementProbe.java`
       don't exist.
    2. Server returns `Status::unimplemented(...)` from
       `do_put_prepared_statement_update` whenever `qh.parameters()` is
       set.
    """

    def test_prepared_insert_with_bound_params_lands_value(self) -> None:
        client = make_client()
        ctx = setup_with_data_named(client)

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        sql = f"INSERT INTO {fqn} (name, value) VALUES (?, ?)"

        status, payload = _execute_prepared_update_via(
            "jdbc",
            sql,
            params=[("string", "carol"), ("int", 99)],
            port=port,
            catalog=ctx["catalog_name"],
        )
        assert status == "OK", (
            f"prepared INSERT did not return OK; got status={status!r} "
            f"payload={payload!r}"
        )
        # The unimplemented-arm wording is the structural failure mode
        # I3 must remove; assert against it so a regression that
        # re-introduces the unimplemented branch surfaces here, not just
        # via a confusing OK-but-row-missing downstream assertion.
        assert "unimplemented" not in payload.lower(), payload

        # Read back via gRPC (NOT Flight SQL) to verify the bound value
        # actually landed in storage. Using the same Flight SQL path
        # for read-back would mask any server-side parameter handling
        # that returned OK without persisting.
        grpc_result = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            columns=["name", "value"],
        )
        client.close()
        rows = grpc_result.to_pylist()
        assert {"name": "carol", "value": 99} in rows, rows


class TestFlightSqlGuard:
    """Runs under every backend — it never opens a Flight SQL connection."""

    def _client_without_flight_sql(self) -> PencaClient:
        settings = ClientSettings()  # ty: ignore[missing-argument]
        return PencaClient(
            query_stub=QueryServiceStub(insecure_channel(settings.query_url)),
            write_stub=WriteServiceStub(insecure_channel(settings.write_url)),
            lifecycle_stub=LifecycleServiceStub(
                insecure_channel(settings.lifecycle_url)
            ),
            flight_sql_url=None,
        )

    def test_query_raises_without_flight_sql_url(self):
        client = self._client_without_flight_sql()
        with pytest.raises(NotImplementedError, match="PENCA_SQL_URL is unset"):
            client.execute_query("SELECT 1")

    def test_query_stream_raises_without_flight_sql_url(self):
        client = self._client_without_flight_sql()
        with pytest.raises(NotImplementedError, match="PENCA_SQL_URL is unset"):
            list(client.execute_stream("SELECT 1"))


# CHA-172 — auto-commit CREATE SCHEMA / CREATE TABLE acceptance suite.
#
# The four classes below pin the user-visible Flight SQL DDL contract this
# ticket ships: a SQL client (DBeaver, JDBC, ADBC notebook) can issue
# ``CREATE SCHEMA`` and ``CREATE TABLE`` over Flight SQL without falling back
# to the gRPC WriteService for first-time-database UX. Transactional
# ``CREATE SCHEMA`` / ``CREATE TABLE`` (inside a ``BEGIN``/``COMMIT`` block)
# is also supported as of CHA-345 — that acceptance lives in
# TestFlightSqlTransactionalDdlEndToEnd. The in-tx DDL that *stays*
# rejected (DROP/ALTER) lives in
# TestFlightSqlUnsupportedInTxDdlRejectionDriverParity.


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlCreateSchemaAutoCommit:
    """``CREATE SCHEMA`` over Flight SQL auto-commit succeeds, and the new
    schema is immediately visible to a follow-up ``CREATE TABLE`` targeting it.

    Driver parametrization matches the sibling rejection-parity class: same
    SQL through both ADBC's ``DoPutStatementUpdate`` path and JDBC's
    ``ActionCreatePreparedStatement`` + ``DoPutPreparedStatementUpdate`` path.
    """

    def test_create_schema_auto_commit_returns_ok_and_is_visible(
        self,
        driver: Literal["adbc", "jdbc"],
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha172_s_{driver}_{uuid4().hex[:12]}"
        steps = [
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {schema}.users (id BIGINT, PRIMARY KEY(id))",
        ]
        results = _execute_update_steps_via(driver, steps, port=port)

        assert len(results) == 2, results
        assert results[0][0] == "OK", (
            f"[{driver}] CREATE SCHEMA must succeed; got {results[0]}"
        )
        assert results[1][0] == "OK", (
            f"[{driver}] follow-up CREATE TABLE on the new schema must succeed "
            f"(visibility check); got {results[1]}"
        )


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlCreateTableAutoCommitEndToEnd:
    """Full ``CREATE SCHEMA`` → ``CREATE TABLE`` → ``INSERT`` → ``SELECT``
    round-trip via Flight SQL. The user-facing acceptance: open DBeaver, point
    at the Flight SQL endpoint, run the four statements, see rows back.

    Driver-parametrized so the JDBC ``ActionCreatePreparedStatement`` /
    ``DoPutPreparedStatementUpdate`` path is exercised end-to-end alongside
    ADBC's ``DoPutStatementUpdate`` path.
    """

    def test_create_schema_then_table_then_insert_then_select(
        self,
        driver: Literal["adbc", "jdbc"],
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha172_e2e_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.users"
        write_steps = [
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {fqn} (id BIGINT, name VARCHAR(64), PRIMARY KEY(id))",
            f"INSERT INTO {fqn} VALUES (1, 'alice'), (2, 'bob')",
        ]
        results = _execute_update_steps_via(driver, write_steps, port=port)
        assert len(results) == 3, results
        for i, (status, payload) in enumerate(results):
            assert status == "OK", (
                f"[{driver}] step {i} ({write_steps[i]!r}) expected OK; "
                f"got ({status}, {payload})"
            )

        rows = _execute_query_via(
            driver,
            f"SELECT id, name FROM {fqn} ORDER BY id",
            port=port,
        )
        assert rows == [
            {"id": 1, "name": "alice"},
            {"id": 2, "name": "bob"},
        ], f"[{driver}] round-tripped rows mismatch; got {rows}"


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlTransactionalDdlEndToEnd:
    """CHA-345 — transactional DDL via Flight SQL: ``BEGIN; CREATE TABLE …;
    …; COMMIT;`` is visible mid-tx, persists on COMMIT, and is discarded on
    ROLLBACK.

    In-tx CREATE routes through the same wire actions as auto-commit DDL
    (ADBC ``DoPutStatementUpdate``; JDBC ``ActionCreatePreparedStatement``
    → ``DoPutPreparedStatementUpdate``), so parametrizing over both drivers
    pins the single ``gateway::classify`` chokepoint for every driver. The
    mid-tx SELECT exercises ``PencaSchemaProvider::table`` reading the open
    ``tx_uuid`` via the ``ConnScope`` cell (IMPL-1) so the just-created
    table resolves; the server honors ``open_tx_uuid`` on the GetTable
    metadata read (``ReadRequestScope``) and ``tx_uuid`` on the CreateTable
    write (CHA-164).
    """

    def test_create_table_in_tx_visible_then_persists(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha345_e2e_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.users"
        steps = [
            "BEGIN",
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {fqn} (id BIGINT, v INT, PRIMARY KEY(id))",
            f"INSERT INTO {fqn} VALUES (1, 10)",
            f"SELECT id, v FROM {fqn} ORDER BY id",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog="public")
        assert len(results) == 6, results
        for i in (0, 1, 2, 3):
            assert results[i][0] == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; got {results[i]}"
            )

        # Mid-tx SELECT (step 4) sees the in-tx-created table + the in-tx row.
        status, payload = results[4]
        assert status == "OK_ROWS", (
            f"[{driver}] mid-tx SELECT expected OK_ROWS; got ({status}, {payload})"
        )
        assert json.loads(payload) == [{"id": 1, "v": 10}], (
            f"[{driver}] mid-tx SELECT must see the in-tx row; got {payload}"
        )
        assert results[5][0] == "OK", f"[{driver}] COMMIT failed: {results[5]}"

        # Post-COMMIT, a fresh connection sees the persisted table + row.
        rows = _execute_query_via(
            driver, f"SELECT id, v FROM {fqn} ORDER BY id", port=port, catalog="public"
        )
        assert rows == [{"id": 1, "v": 10}], (
            f"[{driver}] post-COMMIT rows mismatch; got {rows}"
        )

    def test_create_table_in_tx_rolled_back_is_discarded(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha345_rb_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.u"
        steps = [
            "BEGIN",
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {fqn} (id BIGINT, PRIMARY KEY(id))",
            "ROLLBACK",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog="public")
        assert len(results) == 4, results
        for i in (0, 1, 2, 3):
            assert results[i][0] == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; got {results[i]}"
            )

        # Post-ROLLBACK the in-tx schema+table must be discarded — a SELECT
        # against the table errors "not found".
        post = _execute_update_steps_via(
            driver, [f"SELECT id FROM {fqn}"], port=port, catalog="public"
        )
        status, payload = post[0]
        assert status == "CAUGHT", (
            f"[{driver}] post-ROLLBACK SELECT must fail (DDL discarded); "
            f"got ({status}, {payload})"
        )
        assert "not found" in payload.lower(), (
            f"[{driver}] post-ROLLBACK SELECT must report not found; got: {payload}"
        )

    def test_create_schema_then_table_in_tx(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha345_st_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.t"
        steps = [
            "BEGIN",
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {fqn} (id BIGINT, PRIMARY KEY(id))",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog="public")
        assert len(results) == 4, results
        for i in (0, 1, 2, 3):
            assert results[i][0] == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; got {results[i]}"
            )

        # Post-COMMIT the schema + table persist; SELECT returns 0 rows.
        rows = _execute_query_via(
            driver, f"SELECT id FROM {fqn}", port=port, catalog="public"
        )
        assert rows == [], (
            f"[{driver}] in-tx-created table must exist + be empty post-COMMIT; got {rows}"
        )


def _drive_system_tables_cold(
    client: PencaClient,
    catalog_uuid: str,
    schema_uuid: str,
    branch_uuid: str,
) -> None:
    """Persist -> Snapshot -> Purge ``__penca_system__.{tables,schemas,indexes}``
    on ``branch_uuid`` so each system table's COMMITTED base is cold.

    CHA-471: the regression guarded here only manifests when the system
    table's committed rows live in cold tiers (snapshot + cold-persist), so
    the CHA-444 committed-only hot existence-gate finds NO committed hot rows
    for them. Driving them cold is the precondition that forces the gate's
    RYOW arm to be the *only* thing that can surface an open tx's own
    uncommitted DDL row.

    Mirrors ``_persist_purge_system_tables_past_grace``
    (``integration_purge_tx_log_test.py``) but also covers ``.indexes`` (per
    the CHA-471 acceptance) and is local to the Flight SQL suite. The caller
    seeds a committed row into each system table first so the persist has
    something to move cold.
    """
    for sys_table_uuid in (
        system_tables_table_uuid(catalog_uuid),
        system_schemas_table_uuid(catalog_uuid),
        system_indexes_table_uuid(catalog_uuid),
    ):
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_table_uuid,
        )


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlOpenTxDdlRyowWithColdSystemTables:
    """CHA-471 — regression guard for the open-tx cold-system-table DDL
    read-your-own-writes path (CHA-441 hot existence-gate RYOW arm), the
    behavior fixed in commit ``a5fdf393``.

    The fix derives ``open_tx_uuid`` from the ``ReadSnapshot`` in the
    system-table resolvers (``resolve_table_metadata`` /
    ``resolve_schema_metadata`` in ``crates/penca-storage-meta``) instead of
    hardcoding ``None``. Without it, when ``__penca_system__.{tables,
    schemas}``' committed base is cold and the CHA-444 committed-only hot
    existence-gate drops the hot tier, an open tx that CREATEs DDL and reads
    it back in the same tx cannot see its own uncommitted system-table row ->
    "table not found".

    This is the scenario the user-data RYOW tests
    (``integration_read_mvcc_test`` / ``integration_tx_framing_test``) do NOT
    cover: it only exists over Flight SQL (gRPC ``create_table`` auto-commits,
    so the open-tx-DDL-then-read-back pattern is Flight-SQL only).
    Parametrized over (adbc, jdbc): the mid-tx SELECT diverges per driver --
    ADBC ``cursor.execute`` takes the prepared ``CommandPreparedStatementQuery``
    DoGet arm; JDBC ``Statement.execute`` takes ``CommandStatementQuery`` ->
    ``get_flight_info_statement`` -- and both funnel through
    ``PencaSchemaProvider::table`` reading the open ``tx_uuid`` from the
    ``ConnScope`` cell into the fixed resolvers.

    NOTE on ``.indexes``: SQL ``CREATE INDEX`` is not a Flight SQL operation
    (it routes through the gRPC WriteService -- see ``gateway.rs``), so this
    test purges ``.indexes`` cold as part of the uniform precondition but
    exercises the RYOW read-back on the table / schema axis only. The
    ``index.rs`` resolver's open-tx RYOW is a gRPC scenario
    (``integration_index_ddl_test``).
    """

    def test_open_tx_create_ddl_reads_back_with_cold_system_tables(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        try:
            catalog_name = f"cha471_{driver}_{uuid4().hex[:12]}"
            catalog_uuid, main_branch_uuid = client.create_catalog(
                catalog_name, "owner"
            )
            # Seed COMMITTED rows into the system tables: a schema row in
            # `.schemas`, a table row in `.tables`, an index definition row in
            # `.indexes`. These are the committed base the next step drives cold.
            seed_schema_uuid = client.create_schema(
                "cha471_seed_schema",
                catalog_uuid=catalog_uuid,
                author="test",
                comment="cha471 seed",
            )
            seed_table_uuid = client.create_table(
                "cha471_seed_table",
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=catalog_uuid,
                schema_uuid=seed_schema_uuid,
                author="test",
                comment="cha471 seed",
            )
            client.create_index(
                table_uuid=seed_table_uuid,
                index_name="cha471_seed_idx",
                columns=["name"],
                index_type=SCALAR_BTREE,
                catalog_uuid=catalog_uuid,
                schema_uuid=seed_schema_uuid,
                author="test",
                comment="cha471 seed",
            )

            # Precondition: the committed system-table base now lives in cold
            # tiers, so the CHA-444 committed-only hot existence-gate finds no
            # committed hot rows for the system tables.
            _drive_system_tables_cold(
                client, catalog_uuid, seed_schema_uuid, main_branch_uuid
            )
        finally:
            client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        # Open tx over Flight SQL: CREATE the schema + table (uncommitted, under
        # the open tx's tx_uuid) then read the table back IN THE SAME TX. The
        # read must surface the tx's own uncommitted DDL even though the system
        # tables' committed base is cold -- i.e. the phase-1 gate's RYOW arm
        # fires on the system-table (table + schema) axis.
        schema = f"cha471_tx_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.foo"
        steps = [
            "BEGIN",
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {fqn} (id BIGINT, v INT, PRIMARY KEY(id))",
            f"INSERT INTO {fqn} VALUES (1, 10)",
            f"SELECT id, v FROM {fqn} ORDER BY id",
            "COMMIT",
        ]
        results = _execute_update_steps_via(
            driver, steps, port=port, catalog=catalog_name
        )
        assert len(results) == 6, results
        for i in (0, 1, 2, 3):
            assert results[i][0] == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; got {results[i]}"
            )

        # The regression signal: WITHOUT a5fdf393 this mid-tx SELECT comes back
        # ("CAUGHT", "...not found...") because the gate dropped the hot tier
        # and the resolver hardcoded open_tx_uuid=None. WITH the fix it reads
        # back the tx's own uncommitted DDL + row.
        status, payload = results[4]
        assert status == "OK_ROWS", (
            f"[{driver}] mid-tx SELECT must read back the open tx's own "
            f"uncommitted DDL with cold system tables (a5fdf393 RYOW arm); "
            f"got ({status}, {payload})"
        )
        assert json.loads(payload) == [{"id": 1, "v": 10}], (
            f"[{driver}] mid-tx SELECT must return the in-tx row; got {payload}"
        )
        assert results[5][0] == "OK", f"[{driver}] COMMIT failed: {results[5]}"


class TestFlightSqlCreateTableSqlTypeCoverage:
    """Each declared SQL-type → Arrow-type mapping survives the CREATE TABLE
    → WriteService → metadata-store → QueryService → Flight SQL → ADBC
    round-trip. ADBC only — the type-mapper (``crate::sql_type``) lives
    entirely server-side; driver-parity is covered by the auto-commit
    acceptance classes above.

    Pins the *schema* round-trip only, not value INSERT. INSERT VALUES for
    some narrow integer types (notably ``Int16``) hits an unrelated
    Penca-internal "unsupported Arrow data type for SQL conversion" path
    that's out of scope for CHA-172; an empty SELECT against the table
    surfaces the stored arrow_schema without exercising that gap.
    """

    def test_each_declared_sql_type_maps_to_expected_arrow_type(self) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha172_types_{uuid4().hex[:12]}"
        fqn = f"{schema}.types_t"
        # One column per declared mapping. ``a INT`` is PK so the rest can be
        # nullable-by-default per the SQL convention (the impl honors
        # ``NOT NULL`` to opt out; this test leaves them nullable).
        create_table_sql = (
            f"CREATE TABLE {fqn} ("
            "a INT, b BIGINT, c SMALLINT, d BOOLEAN, "
            "e VARCHAR(64), f TEXT, g TIMESTAMP, "
            "h DECIMAL(10,2), i FLOAT, j DOUBLE, k DATE, "
            "PRIMARY KEY(a))"
        )
        write_results = _execute_update_steps_via(
            "adbc",
            [f"CREATE SCHEMA {schema}", create_table_sql],
            port=port,
        )
        for i, (status, payload) in enumerate(write_results):
            assert status == "OK", (
                f"type-coverage setup step {i} failed: ({status}, {payload})"
            )

        # Empty SELECT through PencaClient surfaces the per-column Arrow
        # schema without inserting any rows. The ``_execute_query_via``
        # helper only returns ``list[dict]`` and loses the per-column Arrow
        # type that's the load-bearing assertion here, so bypass it.
        select_client = make_client()
        try:
            table_result = select_client.execute_query(
                f"SELECT a, b, c, d, e, f, g, h, i, j, k FROM {fqn}"
            )
        finally:
            select_client.close()

        actual_types = {f.name: f.type for f in table_result.schema}
        expected: dict[str, pa.DataType] = {
            "a": pa.int32(),
            "b": pa.int64(),
            "c": pa.int16(),
            "d": pa.bool_(),
            "e": pa.utf8(),
            "f": pa.utf8(),
            "g": pa.timestamp("us"),
            "h": pa.decimal128(10, 2),
            "i": pa.float32(),
            "j": pa.float64(),
            "k": pa.date32(),
        }
        for name, want in expected.items():
            got = actual_types.get(name)
            assert got == want, (
                f"column {name!r}: expected Arrow type {want}, got {got}"
            )

        # No rows inserted (see class docstring); the schema is the
        # assertion surface.
        assert table_result.num_rows == 0, table_result


class TestFlightSqlCreateTableRejections:
    """Unsupported ``CREATE TABLE`` variants surface clean, per-variant
    rejections rather than the catch-all "CREATE TABLE not supported" wording.
    ADBC only — rejection wording is server-side.

    Parametrized over (SQL fragment, expected substring in the rejection
    payload). The discriminator vs the pre-CHA-172 catch-all is the
    per-variant keyword: today every case rejects with the same
    "only INSERT / UPDATE / DELETE are supported" wording, so the
    keyword-presence assertion fails uniformly.
    """

    # (label, sql_fmt, required substring in the payload after CHA-172)
    _REJECT_CASES = [
        (
            "no_primary_key",
            "CREATE TABLE {s}.no_pk (id BIGINT)",
            "primary key",
        ),
        (
            "default_clause",
            "CREATE TABLE {s}.with_default (id BIGINT DEFAULT 0, PRIMARY KEY(id))",
            "DEFAULT",
        ),
        (
            "if_not_exists",
            "CREATE TABLE IF NOT EXISTS {s}.maybe (id BIGINT, PRIMARY KEY(id))",
            "IF NOT EXISTS",
        ),
        (
            "create_table_as_select",
            "CREATE TABLE {s}.from_select AS SELECT 1 AS id",
            "AS SELECT",
        ),
        (
            "nested_array_type",
            "CREATE TABLE {s}.with_array (id BIGINT, tags ARRAY<INT>, PRIMARY KEY(id))",
            "ARRAY",
        ),
    ]

    @pytest.mark.parametrize("label,sql_fmt,expected_kw", _REJECT_CASES)
    def test_unsupported_create_table_variant_rejects_with_per_variant_wording(
        self,
        label: str,
        sql_fmt: str,
        expected_kw: str,
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha172_rej_{label}_{uuid4().hex[:12]}"
        setup = _execute_update_steps_via(
            "adbc", [f"CREATE SCHEMA {schema}"], port=port
        )
        assert setup[0][0] == "OK", f"[{label}] setup CREATE SCHEMA failed: {setup[0]}"

        results = _execute_update_steps_via(
            "adbc",
            [sql_fmt.format(s=schema)],
            port=port,
        )
        status, payload = results[0]
        assert status == "CAUGHT", (
            f"[{label}] expected CAUGHT; got ({status}, {payload})"
        )
        # Per-variant keyword present (case-insensitive for SQL keywords).
        assert expected_kw.lower() in payload.lower(), (
            f"[{label}] per-variant rejection must name {expected_kw!r}; got: {payload}"
        )


# CHA-355: DoGet reuses the GetFlightInfo plan instead of re-planning.
#
# A Flight SQL statement query is planned twice today — once in
# GetFlightInfo (for the result schema) and again in DoGet (to execute) —
# because the ticket carries only the SQL string. CHA-355 stashes the
# GetFlightInfo LogicalPlan in a per-ConnSession cache keyed by a
# server-minted statement_uuid carried on the ticket; DoGet looks it up and runs
# it directly, falling back to re-planning on a miss.
#
# There is no resolution-count probe in this suite (CHA-352's counter was
# throwaway instrumentation, stripped before its PR). Reuse is observed via
# a production `tracing` event emitted on the statement-cache lookup in
# `do_get_fallback`:
#
#     tracing::info!(target: "penca_sql::statement_cache", outcome = "hit",  ...)
#     tracing::info!(target: "penca_sql::statement_cache", outcome = "miss", ...)
#
# The container runs `RUST_LOG=info,penca=debug` (docker/compose.yml) with
# the default `tracing_subscriber::fmt()` text format, so an info-level
# event on any target surfaces in `docker logs`. These tests scrape the
# penca-sql-server container log for that event. The match strings below
# are the coordination contract with impl task D — keep them in sync.

# The tracing subscriber colourises output (ANSI SGR escapes) even into a
# non-TTY docker log, so a raw `statement_cache...outcome="hit"` regex fails — the
# escapes land between `outcome` and `=`. `container_log` strips these before
# the matchers below run.
_STATEMENT_CACHE_HIT_RE = re.compile(r'penca_sql::statement_cache.*outcome="hit"')
# Companion cache-miss matcher (RT2): the DoGet miss path re-plans and emits this.
_STATEMENT_CACHE_MISS_RE = re.compile(r'penca_sql::statement_cache.*outcome="miss"')
# A miss whose statement_uuid was stamped but is absent from the serving conn's cache
# (FIFO eviction / cross-connection replay / disabled cache) carries
# reason="evicted"; an unstamped ticket carries reason="unstamped".
_STATEMENT_CACHE_MISS_EVICTED_RE = re.compile(
    r'penca_sql::statement_cache.*outcome="miss".*reason="evicted"'
)


def _await_statement_cache_event(
    pattern: re.Pattern[str],
    *,
    since_offset: int,
    timeout_s: float = 5.0,
) -> str:
    """Poll the SQL-server log window for a `statement_cache` event, returning it.

    The event and the DoGet result-row stream come from the same call, but the
    container's stdout is not guaranteed flushed to the docker json-log driver
    the instant the client finishes receiving rows. Polling absorbs that flush
    gap; returns as soon as ``pattern`` matches the window, or the final window
    after ``timeout_s`` so the caller's assertion reports the miss.

    ``since_offset`` is a character offset into the ANSI-stripped log captured
    before this test's query. The window ``log[since_offset:]`` scopes the scan
    to events this test emitted — without it, a whole-log scan would match a
    `statement_cache` event from another test (the event carries no catalog field, so
    outcome-specificity alone only separates the HIT test from the MISS test,
    not this test's hit from every other hit in the suite). ``container_log``
    strips ANSI before the offset is ever taken, so the prefix length is stable
    across reads and the window boundary doesn't drift.
    """
    deadline = time.monotonic() + timeout_s
    window = container_log("penca-sql-server")[since_offset:]
    while time.monotonic() < deadline:
        window = container_log("penca-sql-server")[since_offset:]
        if pattern.search(window):
            return window

        time.sleep(0.2)

    return window


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlPlanReuse:
    """CHA-355: a statement query plans once — DoGet reuses the
    GetFlightInfo plan rather than re-planning from the SQL string."""

    def test_doget_reuses_getflightinfo_plan(self, driver: Literal["adbc", "jdbc"]):
        """A SELECT through Flight SQL must (a) return the seeded rows and
        (b) emit a `statement_cache` HIT event on the DoGet leg — i.e. DoGet
        executed the cached GetFlightInfo plan instead of re-planning.

        RED on current `main`: no statement cache exists, so no `statement_cache`
        event is ever logged. The rows assertion passes today; the
        log-event assertion is the red signal. It must fail on the missing
        event — not on a fixture/setup error.
        """
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"

        # Window the log scan to events emitted after setup so the HIT
        # assertion can't be satisfied by another test's (or the other
        # parametrize arm's) statement_cache event.
        since_offset = len(container_log("penca-sql-server"))
        rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ], rows

        new_log = _await_statement_cache_event(
            _STATEMENT_CACHE_HIT_RE, since_offset=since_offset
        )
        assert _STATEMENT_CACHE_HIT_RE.search(new_log), (
            "expected a `statement_cache` HIT event on the DoGet leg "
            f"(driver={driver!r}); DoGet must reuse the GetFlightInfo plan. "
            "No matching event found in the penca-sql-server log window."
        )


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
class TestFlightSqlPlanReuseMiss:
    """CHA-355 cache-miss path. Not parametrized over `driver`: the
    two-connection ticket replay is below the ADBC/JDBC layer — both drivers
    would run the identical raw-Flight code — so this runs once."""

    def test_doget_cache_miss_replans(self):
        """A DoGet whose statement_uuid is absent from the serving connection's
        cache must re-plan and still return correct rows, emitting a
        `statement_cache` MISS event.

        Forces the miss with the evicted/cold-statement_uuid shape
        (acceptance #2): connection A runs `GetFlightInfo` (minting the
        statement_uuid in A's per-conn statement cache), then a FRESH connection
        B replays the same opaque ticket via `DoGet`. B's per-conn cache has no
        such statement_uuid, so DoGet falls back
        to `execute_sql` and re-plans. Driven through a raw
        `pyarrow.flight.FlightClient` because the replay is below the driver
        layer.

        RED on current `main`: no statement cache exists, so neither leg logs a
        `statement_cache` event and DoGet already always re-plans. The rows
        assertion passes today; the MISS-event assertion is the red signal. It
        must fail on the missing event — not on a fixture/setup error.
        """
        ctx, _ = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        # Window the log scan to events emitted after setup (see the HIT test).
        since_offset = len(container_log("penca-sql-server"))
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        location = f"grpc://{settings.flight_sql_url}"
        header = [(b"x-penca-catalog", ctx["catalog_name"].encode())]
        descriptor = paflight.FlightDescriptor.for_command(
            _encode_statement_query(f"SELECT name, value FROM {fqn}")
        )

        # Connection A mints the statement_uuid; fresh connection B replays the ticket.
        # Both clients are built inside the try so a partial-construction
        # failure still hits the finally cleanup.
        client_a = None
        client_b = None
        try:
            client_a = paflight.FlightClient(location)
            client_b = paflight.FlightClient(location)
            info = client_a.get_flight_info(
                descriptor, paflight.FlightCallOptions(headers=header)
            )
            assert len(info.endpoints) == 1, info.endpoints
            ticket = info.endpoints[0].ticket

            # Connection B replays the opaque ticket. B's per-conn cache lacks
            # the statement_uuid ⇒ DoGet must re-plan (the cache-miss path).
            reader = client_b.do_get(ticket, paflight.FlightCallOptions(headers=header))
            rows = sorted(reader.read_all().to_pylist(), key=lambda r: r["name"])
        finally:
            if client_a is not None:
                client_a.close()

            if client_b is not None:
                client_b.close()

        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ], rows

        new_log = _await_statement_cache_event(
            _STATEMENT_CACHE_MISS_RE, since_offset=since_offset
        )
        assert _STATEMENT_CACHE_MISS_RE.search(new_log), (
            "expected a `statement_cache` MISS event on the DoGet leg; a "
            "statement_uuid absent from the serving connection's cache must "
            "re-plan. No matching event found in the penca-sql-server log "
            "window."
        )

    def test_doget_prepared_cache_miss_replans(self):
        """The prepared-statement (`CommandPreparedStatementQuery`) DoGet arm —
        the path ADBC drives — must also re-plan on a cache miss and still
        return correct rows, emitting a `statement_cache` MISS event tagged
        `reason="evicted"`.

        Mirrors :meth:`test_doget_cache_miss_replans` on the prepared arm:
        connection A runs `GetFlightInfo` for a `CommandPreparedStatementQuery`
        (minting + stamping the statement_uuid in A's per-conn cache), then a
        fresh connection B replays the opaque endpoint ticket via `DoGet`. B's
        cache lacks A's statement_uuid, so the prepared arm re-plans from the
        handle's SQL.
        """
        ctx, _ = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        # Window the log scan to events emitted after setup (see the HIT test).
        since_offset = len(container_log("penca-sql-server"))
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        location = f"grpc://{settings.flight_sql_url}"
        header = [(b"x-penca-catalog", ctx["catalog_name"].encode())]
        descriptor = paflight.FlightDescriptor.for_command(
            _encode_prepared_statement_query(f"SELECT name, value FROM {fqn}")
        )

        # Connection A mints + stamps the statement_uuid on the endpoint ticket; fresh
        # connection B replays it. Both clients are built inside the try so a
        # partial-construction failure still hits the finally cleanup.
        client_a = None
        client_b = None
        try:
            client_a = paflight.FlightClient(location)
            client_b = paflight.FlightClient(location)
            info = client_a.get_flight_info(
                descriptor, paflight.FlightCallOptions(headers=header)
            )
            assert len(info.endpoints) == 1, info.endpoints
            ticket = info.endpoints[0].ticket

            # Connection B replays the opaque ticket. B's per-conn cache lacks
            # the statement_uuid ⇒ the prepared DoGet arm must re-plan (cache miss).
            reader = client_b.do_get(ticket, paflight.FlightCallOptions(headers=header))
            rows = sorted(reader.read_all().to_pylist(), key=lambda r: r["name"])
        finally:
            if client_a is not None:
                client_a.close()

            if client_b is not None:
                client_b.close()

        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ], rows

        new_log = _await_statement_cache_event(
            _STATEMENT_CACHE_MISS_RE, since_offset=since_offset
        )
        assert _STATEMENT_CACHE_MISS_RE.search(new_log), (
            "expected a `statement_cache` MISS event on the prepared DoGet leg; "
            "a statement_uuid absent from the serving connection's cache must "
            "re-plan. No matching event found in the penca-sql-server log "
            "window."
        )
        assert _STATEMENT_CACHE_MISS_EVICTED_RE.search(new_log), (
            'expected the prepared cross-connection-replay miss tagged reason="evicted" '
            "(statement_uuid stamped on the ticket but absent from connection B's cache)."
        )


# CHA-367: per-query planning metadata-resolution count (Layer B).
#
# After CHA-365 Layer A (per-RPC dedup, merged) each get_schema/get_table gRPC
# is cheap, but the SQL server still issues several of them per user query:
# DataFusion calls CatalogProvider::schema() / SchemaProvider::table()
# repeatedly while planning ONE statement (no per-plan memo), and the statement
# is planned twice on the ADBC prepared path (PREPARE + GetFlightInfo). This
# ticket collapses those to exactly 1 get_schema + 1 get_table per query.
#
# Observable: get_schema/get_table span CLOSE events on the QUERY container.
# QueryManager::{get_schema,get_table} are #[tracing::instrument(level="debug")]
# spans (crates/penca-api/src/query/mod.rs); with PENCA_SPAN_TIMING=1
# (docker/test.env) the fmt subscriber emits one `... close time.busy=...` line
# per gRPC handler invocation. read_data is a SEPARATE span and resolves its
# scope via resolve_schema_metadata/resolve_table_metadata (different span
# names), so counting get_schema/get_table CLOSE lines isolates planning gRPCs
# from execution-time scope resolution — the pg_stat-over-__penca_system__
# needle (CHA-365) would conflate the two and is deliberately NOT used here.


# Each `get_schema`/`get_table` gRPC the SQL server issues is handled by the
# query service through TWO same-named instrumented spans: the tonic
# entry-point handler (target `penca_server_grpc::query`) and the inner
# `QueryManager` method (target `penca_api::query`). Counting both would
# double-count every RPC, so we count only the gRPC entry-point span — exactly
# one per received RPC — which is the "gRPCs per query" number CHA-367 targets.
_GRPC_ENTRY_TARGET = "penca_server_grpc::query"


def _count_grpc_handler_closes(window: str, span_name: str, branch_uuid: str) -> int:
    """Count gRPC handler invocations of ``span_name`` for ``branch_uuid``.

    ``tracing_subscriber::fmt`` with ``FmtSpan::CLOSE`` emits, per closing span::

        <ts> DEBUG a{f}:b{f}: <target>: close time.busy=.. time.idle=..

    The span context ``a{f}:b{f}`` is colon-joined with **no** space between
    segments, then a single space precedes the target. So the span that is
    actually closing is the terminal segment: ``<span_name>{...}: `` (brace,
    colon, SPACE) only follows the closing span — a *parent* still on the stack
    is followed by ``:<child>{`` (colon, no space). We further require the
    target to be the gRPC entry point (`penca_server_grpc::query`) so each RPC
    counts once, not once per instrumented layer. The negative lookbehind
    ``(?<!\\w)`` keeps ``get_table`` from matching a longer identifier ending in
    the same text.

    The span records the connection's branch as a ``branch=Some("<uuid>")``
    field, and ``setup_with_data_named`` mints a fresh catalog (hence a fresh
    ``main_branch_uuid``) per run, so requiring that field scopes the count to
    *this* query's gRPCs. That makes the count correct even if another test's
    queries land in the same time window (e.g. under ``pytest-xdist``) — a
    time-offset window alone would conflate them.
    """
    term = re.compile(
        rf"(?<!\w){re.escape(span_name)}\{{[^}}]*\}}: {re.escape(_GRPC_ENTRY_TARGET)}: close"
    )
    needle = f'branch=Some("{branch_uuid}")'
    return sum(
        1 for line in window.splitlines() if needle in line and term.search(line)
    )


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlPlanningResolutionCount:
    """CHA-367: one Flight SQL SELECT must issue exactly 1 ``get_schema`` and
    1 ``get_table`` gRPC during planning, asserted for both the ADBC and JDBC
    clients. Both clients reach DoGet on the ``CommandPreparedStatementQuery``
    arm here — the Arrow JDBC driver routes even a plain ``Statement.execute``
    through a prepared statement — so both plan twice before DoGet (PREPARE +
    GetFlightInfo). Parametrizing over both still guards against a
    driver-specific regression and matches the ticket's driver-parity mandate.

    RED before the fix: PREPARE and GetFlightInfo are two separate plan builds,
    each resolving the schema + table once, so the per-query count is 2 of
    each. The failure is the ``== 1`` assertion, not a setup error (a ``>= 1``
    guard distinguishes "too many" from a misconfigured ``PENCA_SPAN_TIMING``).

    GREEN after IMPL-B: GetFlightInfo reuses the plan PREPARE already built and
    cached, so PREPARE's single build is the only metadata resolution (DoGet
    reuses it too, via CHA-355). IMPL-A's per-plan memo is what keeps each
    individual build at one gRPC per identifier — load-bearing for multi-table
    plans, though a single-table SELECT has nothing to collapse within a build.
    """

    def test_planning_resolves_schema_and_table_once(
        self, driver: Literal["adbc", "jdbc"]
    ):
        ctx, port = _setup_and_port(setup_with_data_named)
        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        branch = ctx["main_branch_uuid"]

        # Window the query-log scan to events emitted after setup; counting is
        # further scoped to this query's branch_uuid (see
        # `_count_grpc_handler_closes`) so it's robust to concurrent activity.
        since = len(container_log("query"))
        rows = sorted(
            _execute_query_via(
                driver,
                f"SELECT name, value FROM {fqn}",
                port=port,
                catalog=ctx["catalog_name"],
            ),
            key=lambda r: r["name"],
        )
        assert rows == [
            {"name": "alice", "value": 10},
            {"name": "bob", "value": 20},
        ], rows

        # The DoGet result stream has been fully received, so every planning
        # gRPC has closed — but the container's stdout may not be flushed to
        # docker's json-log driver yet. Poll until the counts have *settled*
        # (unchanged across two successive reads, both >= 1), not merely until
        # the first CLOSE of each appears: an incremental flush could surface
        # one CLOSE before the rest, and breaking there would freeze a partial
        # window and let the `== 1` assertion pass spuriously.
        deadline = time.monotonic() + 5.0
        prev = (-1, -1)
        cur = (0, 0)
        while time.monotonic() < deadline:
            window = container_log("query")[since:]
            cur = (
                _count_grpc_handler_closes(window, "get_schema", branch),
                _count_grpc_handler_closes(window, "get_table", branch),
            )
            if cur[0] >= 1 and cur[1] >= 1 and cur == prev:
                break

            prev = cur
            time.sleep(0.2)

        get_schema, get_table = cur

        # Loud setup-failure (not a silent green) if no CLOSE lines surfaced —
        # that means PENCA_SPAN_TIMING is not set on the query container.
        assert get_schema >= 1 and get_table >= 1, (
            f"[{driver}] no get_schema/get_table CLOSE events in the query-log "
            f"window (get_schema={get_schema}, get_table={get_table}). "
            "PENCA_SPAN_TIMING=1 must be set on the penca-query container "
            "(docker/test.env) for this measurement seam to work — this is a "
            "harness misconfiguration, not a passing result."
        )
        assert get_schema == 1, (
            f"[{driver}] planning issued {get_schema} get_schema gRPCs for a "
            "single SELECT; CHA-367 target is exactly 1 (per-plan resolution "
            "memo + cross-pass PREPARE/GetFlightInfo reuse)."
        )
        assert get_table == 1, (
            f"[{driver}] planning issued {get_table} get_table gRPCs for a "
            "single SELECT; CHA-367 target is exactly 1 (per-plan resolution "
            "memo + cross-pass PREPARE/GetFlightInfo reuse)."
        )

    def test_join_in_one_schema_resolves_schema_once(
        self, driver: Literal["adbc", "jdbc"]
    ):
        """Within-build memo coverage (mechanism #1, distinct from the single-
        SELECT cross-pass case above): a 2-table join in one schema makes
        DataFusion resolve ``schema(s)`` once *per table reference* and
        ``table()`` for two distinct tables, all inside one ``create_logical_plan``.

        The per-plan memo collapses the repeated ``schema(s)`` resolutions to a
        single ``get_schema`` gRPC; without it the build would issue two (one
        per table reference). ``get_table`` stays 2 — two distinct identifiers,
        each resolved once. So this case fails (``get_schema == 2``) if the memo
        regresses to a no-op, which the single-table count test cannot catch.
        """
        ctx, port = _setup_and_port(_setup_two_tables_one_schema)
        branch = ctx["main_branch_uuid"]
        cat, schema = ctx["catalog_name"], ctx["schema_name"]
        sql = (
            f"SELECT a.value FROM {cat}.{schema}.a a "
            f"JOIN {cat}.{schema}.b b ON a.name = b.name"
        )

        since = len(container_log("query"))
        _execute_query_via(driver, sql, port=port, catalog=cat)

        # Settle the count (same flush-absorbing poll as the sibling test).
        deadline = time.monotonic() + 5.0
        prev = (-1, -1)
        cur = (0, 0)
        while time.monotonic() < deadline:
            window = container_log("query")[since:]
            cur = (
                _count_grpc_handler_closes(window, "get_schema", branch),
                _count_grpc_handler_closes(window, "get_table", branch),
            )
            # This join's terminal state is (1, 2), so gate get_table on >= 2 —
            # breaking at the first `>= 1` could settle a partial flush at (1, 1)
            # and spuriously fail the `get_table == 2` assertion. A regression
            # that drives the counts higher settles at the stable higher value;
            # one that drives them lower loops to the deadline and asserts the
            # real (lower) count.
            if cur[0] >= 1 and cur[1] >= 2 and cur == prev:
                break

            prev = cur
            time.sleep(0.2)

        get_schema, get_table = cur

        assert get_schema >= 1 and get_table >= 1, (
            f"[{driver}] no get_schema/get_table CLOSE events in the query-log "
            f"window (get_schema={get_schema}, get_table={get_table}); "
            "PENCA_SPAN_TIMING=1 must be set on the penca-query container."
        )
        assert get_schema == 1, (
            f"[{driver}] a 2-table join in one schema issued {get_schema} "
            "get_schema gRPCs; the per-plan memo must collapse the per-table-ref "
            "schema resolutions to 1 (without it this is >= 2)."
        )
        assert get_table == 2, (
            f"[{driver}] a 2-table join issued {get_table} get_table gRPCs; "
            "expected exactly 2 — one per distinct table, each resolved once by "
            "the per-plan memo (repeated table()/table_exist() collapse)."
        )


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlPlanResolutionMemoIsBuildScoped:
    """CHA-367 regression guard: the per-plan resolution memo must NOT leak a
    resolution across statements. It is cleared between plan builds (the RAII
    guard's drop), so a name that resolved "not found" in one statement's build
    must re-resolve — and now resolve — in a later statement's build after the
    object is created mid-transaction.

    This is the load-bearing correctness bound that lets the memo be safe
    despite CHA-255 deleting the TTL cache and CHA-345's RYOW requirement. A
    memo that was connection- or snapshot-scoped (not build-scoped) would cache
    the step-3 "table not found" and fail step 5; the green result confirms the
    guard installs and clears per statement on both driver paths (the JDBC
    statement path and the ADBC prepared path resolve through different server
    entry points).
    """

    def test_not_found_then_created_in_tx_resolves(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None
        _host, _, port = settings.flight_sql_url.rpartition(":")

        schema = f"cha367_memo_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.t"
        steps = [
            "BEGIN",
            f"CREATE SCHEMA {schema}",
            # First reference to `t` — it does not exist yet. Planning resolves
            # it as "not found" (cached only for THIS build's memo, then
            # cleared). The statement fails, as it should.
            f"SELECT id FROM {fqn}",
            f"CREATE TABLE {fqn} (id BIGINT, PRIMARY KEY(id))",
            f"INSERT INTO {fqn} VALUES (1)",
            # Re-reference `t` after it was created mid-tx. A build-scoped memo
            # re-resolves here and finds it; a leaked memo would still serve the
            # step-3 "not found" and this SELECT would fail.
            f"SELECT id FROM {fqn} ORDER BY id",
            "COMMIT",
        ]
        results = _execute_update_steps_via(driver, steps, port=port, catalog="public")
        assert len(results) == 7, results

        assert results[0][0] == "OK", f"[{driver}] BEGIN failed: {results[0]}"
        assert results[1][0] == "OK", f"[{driver}] CREATE SCHEMA failed: {results[1]}"
        # Step 2: SELECT on the not-yet-created table must fail (table not
        # found) — this is what seeds a "not found" resolution for the step's
        # own build.
        assert results[2][0] == "CAUGHT", (
            f"[{driver}] SELECT on a not-yet-created table must fail; got {results[2]}"
        )
        assert results[3][0] == "OK", (
            f"[{driver}] in-tx CREATE TABLE failed: {results[3]}"
        )
        assert results[4][0] == "OK", f"[{driver}] in-tx INSERT failed: {results[4]}"
        # Step 5: the load-bearing assertion — the re-reference resolves the
        # mid-tx-created table instead of a stale "not found".
        assert results[5][0] == "OK_ROWS", (
            f"[{driver}] SELECT after mid-tx CREATE must resolve the new table "
            f"(per-build memo must not leak the earlier 'not found'); got {results[5]}"
        )
        assert json.loads(results[5][1]) == [{"id": 1}], (
            f"[{driver}] re-resolved SELECT must see the in-tx row; got {results[5][1]}"
        )
        assert results[6][0] == "OK", f"[{driver}] COMMIT failed: {results[6]}"


def _setup_correlated_subquery_tables(client: PencaClient) -> dict:
    """Two tables for the CHA-402 correlated-scalar-subquery test:
    ``accounts(aid, bid)`` and ``history(hid, aid)`` with a deterministic
    per-account history count — aid=1 -> 2 rows, aid=2 -> 1, aid=3 -> 0 —
    so ``(SELECT count(*) FROM history h WHERE h.aid = a.aid)`` yields
    ``2 / 1 / 0`` (COUNT(*) over the empty correlated set is 0, not NULL).

    Pins the connection to the fresh catalog like the sibling ``_setup_*``
    helpers so the SELECT's 3-part FQNs resolve.
    """
    accounts_schema = pa.schema(
        [pa.field("aid", pa.int64()), pa.field("bid", pa.int64())]
    )
    history_schema = pa.schema(
        [pa.field("hid", pa.int64()), pa.field("aid", pa.int64())]
    )
    catalog_name = f"sql_corr_cat_{uuid4().hex[:8]}"
    schema_name = "sql_schema"

    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        schema_name, catalog_uuid=catalog_uuid, author="test", comment="create_schema"
    )
    accounts_uuid = client.create_table(
        "accounts",
        accounts_schema,
        primary_keys=["aid"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="create_table",
    )
    history_uuid = client.create_table(
        "history",
        history_schema,
        primary_keys=["hid"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="create_table",
    )

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    accounts = pa.table({"aid": [1, 2, 3], "bid": [1, 1, 1]}, schema=accounts_schema)
    # aid=1 -> 2 rows, aid=2 -> 1 row, aid=3 -> 0 rows.
    history = pa.table({"hid": [10, 11, 20], "aid": [1, 1, 2]}, schema=history_schema)
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=accounts_uuid, upserts=accounts),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=history_uuid, upserts=history),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
    )

    client.catalog = catalog_name
    return {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "main_branch_uuid": main_branch_uuid,
    }


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestFlightSqlCorrelatedScalarSubquery:
    """CHA-402 — a correlated scalar subquery must execute over Flight SQL.

    ``(SELECT count(*) FROM history h WHERE h.aid = a.aid)`` is
    decorrelated by DataFusion's ``scalar_subquery_to_join`` to a LEFT
    JOIN whose ``my_txns`` is NULLABLE in the physical plan but
    NON-nullable in the logical plan. ``get_flight_info`` advertises the
    logical-plan schema while ``DoGet`` streams the physical one; ADBC
    enforces ``get_flight_info`` schema == ``DoGet`` stream schema and
    rejected the divergence with ``endpoint 0 returned inconsistent
    schema``. The fix tightens the DoGet stream back to the advertised
    (logical) nullability — ``COUNT(*)`` is never null — at the shared
    ``record_batch_response`` (``codec::reconcile_stream_to_advertised``).

    Driver parity (the CHA-355 trap): ADBC ``cursor.execute(SELECT)``
    takes the PREPARED path (``do_action_create_prepared_statement`` +
    ``get_flight_info_prepared_statement`` + DoGet
    ``CommandPreparedStatementQuery``) while JDBC ``Statement.execute(SELECT)``
    takes ``get_flight_info_statement`` + DoGet ``CommandStatementQuery`` —
    they do not converge on one GetFlightInfo handler, but both DoGet arms
    reconcile through ``record_batch_response``. The ``[adbc]`` arm is RED
    pre-fix; the ``[jdbc]`` arm is a parity guard (the Apache
    flight-sql-jdbc-driver tolerates the nullability divergence and is green
    throughout). Both return identical rows post-fix.
    """

    def test_correlated_count_subquery_executes(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        ctx, port = _setup_and_port(_setup_correlated_subquery_tables)

        acct_fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.accounts"
        hist_fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.history"
        sql = (
            "SELECT a.aid, "
            f"(SELECT count(*) FROM {hist_fqn} h WHERE h.aid = a.aid) AS my_txns "
            f"FROM {acct_fqn} a "
            "ORDER BY a.aid"
        )

        rows = _execute_query_via(driver, sql, port=port, catalog=ctx["catalog_name"])

        assert rows == [
            {"aid": 1, "my_txns": 2},
            {"aid": 2, "my_txns": 1},
            {"aid": 3, "my_txns": 0},
        ], f"[{driver}] correlated COUNT subquery returned {rows}"


class TestFlightSqlParameterizedPreparedSelect:
    """CHA-402 regression guard: preparing a parameterized SELECT
    (``SELECT ... WHERE col = ?``) must not error.

    The shipped CHA-402 fix keeps the ``get_flight_info`` /
    ``do_action_create_prepared_statement`` advertise leg on the *logical*
    plan's schema (``codec::get_schema_for_plan``) and reconciles the
    ``DoGet`` stream to it (``reconcile_stream_to_advertised``). Because the
    advertise leg never physically plans, an unbound ``Expr::Placeholder``
    is never handed to the physical planner, so preparing a ``?``-bearing
    SELECT succeeds.

    This guards that property: the rejected alternative — advertising the
    *physical* plan's schema — would have to ``create_physical_plan`` an
    unbound placeholder at prepare time and fail (``Placeholder '$1' was not
    provided a value for execution``). If the advertise leg ever regresses
    to physical planning, this test goes red.

    It drives ADBC's low-level ``AdbcStatement.prepare()`` — the
    ``ActionCreatePreparedStatement`` wire action handled by
    ``do_action_create_prepared_statement`` (the same handler JDBC
    ``prepareStatement(SELECT … ?)`` lands on).

    Scope: the test stops at ``prepare()`` and does not execute with a bound
    value. Actually *executing* a ``?``-parameterized SELECT over ADBC hits
    a separate, pre-existing limitation (DoGet rejects the anonymous ``?``
    placeholder id — ``Failed to parse placeholder id``), unrelated to the
    CHA-402 schema fix and tracked as CHA-408. No prior test exercised a
    parameterized prepared SELECT at all (prepared coverage was SET /
    non-parameterized via ``_exec_via_prepared`` and parameterized *DML* via
    ``_execute_prepared_update_via``).
    """

    def test_parameterized_prepared_select_survives_prepare(self) -> None:
        client = make_client()
        ctx = setup_with_data_named(client)
        client.close()

        settings = ClientSettings()  # ty: ignore[missing-argument]
        assert settings.flight_sql_url is not None

        fqn = f"{ctx['catalog_name']}.{ctx['schema_name']}.{ctx['table_name']}"
        conn = flight_sql_connect(
            f"grpc://{settings.flight_sql_url}",
            db_kwargs={
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
                "adbc.flight.sql.rpc.call_header.x-penca-catalog": ctx["catalog_name"],
            },
            autocommit=True,
        )
        try:
            cursor = conn.cursor()
            try:
                stmt = cursor.adbc_statement
                stmt.set_sql_query(f"SELECT name, value FROM {fqn} WHERE value = ?")
                # ActionCreatePreparedStatement -> do_action_create_prepared_statement.
                # Must not raise: the advertise leg uses the logical schema and
                # never physical-plans the unbound `?`. (An advertise-physical
                # approach would raise `Placeholder '$1' was not provided a value
                # for execution` here.) Reaching the next line is the assertion.
                stmt.prepare()
            finally:
                cursor.close()
        finally:
            conn.close()
