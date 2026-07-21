"""Integration tests for QueryService (reads, branch reads, tx reads, SQL queries).

Run via ``just integration-test``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.config import ClientSettings
from penca_client.errors import (
    FailedPreconditionError,
    InvalidRequestError,
    NotFoundError,
)
from penca_client.naming import (
    system_schemas_table_uuid,
    system_tables_table_uuid,
    upsert_log_table,
)
from penca_proto.external.v1.query_pb2_grpc import QueryServiceStub
from grpc import insecure_channel
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    count_stmts_referencing,
    create_table_on_branch,
    ensure_pg_stat_statements,
    get_pg_driver,
    make_client,
    reset_pg_stat,
    setup_schema,
    setup_with_data,
)

_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])


class TestQueryService:
    # -- Table reads -------------------------------------------------------

    def test_get_table(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog("table_get_cat", "owner")
        schema_uuid = client.create_schema(
            "table_get_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "get_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        assert response.table_uuid == table_uuid
        assert response.schema_uuid == schema_uuid
        assert response.table_name == "get_table"
        assert response.arrow_schema.equals(USER_SCHEMA)

    def test_list_tables(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "table_list_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "table_list_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        uuid_a = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        uuid_b = client.create_table(
            "table_b",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        tables = list(
            client.list_tables(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)
        )
        table_uuids = [table.table_uuid for table in tables]
        assert uuid_a in table_uuids
        assert uuid_b in table_uuids

    def test_get_table_by_names(self):
        """get_table resolves from catalog_name + schema_name + table_name."""
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "name_resolve_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "name_resolve_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "name_resolve_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        response = client.get_table(
            catalog_name="name_resolve_cat",
            schema_name="name_resolve_schema",
            table_name="name_resolve_table",
        )
        assert response.table_uuid == table_uuid
        assert response.table_name == "name_resolve_table"

    def test_get_table_resolves_metadata_once(self):
        """CHA-365 Layer A: a get_table by-name must issue the same number of
        ``__penca_system__.tables`` merge SELECTs as a get_table by-uuid.

        By-uuid resolves the table row once (the handler fetch only). By-name
        today resolves it twice — ``ReadRequestScope::resolve_table`` reads the
        row to turn the name into a uuid, then ``QueryManager::get_table``
        re-fetches it by uuid for the response. Layer A carries the resolved
        row on the scope and reuses it, collapsing the name path to one merge.

        Fail-first: by-name issues 2x the merges of by-uuid. Green after the
        carry: 1x == 1x. Counts via pg_stat_statements (K-agnostic equality —
        no dependence on how many SQL statements one stream_merged fans into).
        """
        client = make_client()
        # Unique catalog name per run: catalog_store is not branch-scoped, so a
        # fixed name collides with any residual state (e.g. a stale pgdata
        # volume from an interrupted run). The count needle keys off the
        # returned catalog_uuid, not the name, so uniqueness is free.
        cat_name = f"resolve_once_cat_{uuid4().hex[:8]}"
        catalog_uuid, branch_uuid = client.create_catalog(cat_name, "owner")
        schema_uuid = client.create_schema(
            "resolve_once_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "resolve_once_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        # The stream_merged over __penca_system__.tables reads this per-branch
        # upsert log; it's unique to this catalog, so background activity on
        # other catalogs doesn't pollute the count.
        tables_log = upsert_log_table(
            system_tables_table_uuid(catalog_uuid), branch_uuid
        )

        reset_pg_stat(pg)
        client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        by_uuid = count_stmts_referencing(pg, tables_log)

        reset_pg_stat(pg)
        client.get_table(
            catalog_name=cat_name,
            schema_name="resolve_once_schema",
            table_name="resolve_once_table",
        )
        by_name = count_stmts_referencing(pg, tables_log)

        assert by_uuid > 0, "sanity: get_table must touch __penca_system__.tables"
        assert by_name == by_uuid, (
            f"get_table by-name issued {by_name} __penca_system__.tables merge "
            f"statements vs {by_uuid} by-uuid — the name path double-resolves the "
            f"table row (CHA-365 Layer A: carry it on ReadRequestScope and reuse)"
        )

    def test_get_schema_resolves_metadata_once(self):
        """CHA-365 Layer A: a get_schema by-name must issue the same number of
        ``__penca_system__.schemas`` merge SELECTs as a get_schema by-uuid.

        By-uuid resolves the schema row once (handler fetch only). By-name
        today resolves it twice — ``ReadRequestScope::resolve_schema`` reads it
        to turn the name into a uuid, then ``QueryManager::get_schema``
        re-fetches it by uuid for the response. Layer A carries the resolved
        row and reuses it.

        Fail-first: by-name issues 2x; green after the carry: 1x == 1x.
        """
        client = make_client()
        # Unique catalog name per run — see test_get_table_resolves_metadata_once.
        cat_name = f"schema_resolve_once_cat_{uuid4().hex[:8]}"
        catalog_uuid, branch_uuid = client.create_catalog(cat_name, "owner")
        schema_uuid = client.create_schema(
            "schema_resolve_once_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        schemas_log = upsert_log_table(
            system_schemas_table_uuid(catalog_uuid), branch_uuid
        )

        reset_pg_stat(pg)
        client.get_schema(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)
        by_uuid = count_stmts_referencing(pg, schemas_log)

        reset_pg_stat(pg)
        client.get_schema(
            catalog_name=cat_name,
            schema_name="schema_resolve_once_schema",
        )
        by_name = count_stmts_referencing(pg, schemas_log)

        assert by_uuid > 0, "sanity: get_schema must touch __penca_system__.schemas"
        assert by_name == by_uuid, (
            f"get_schema by-name issued {by_name} __penca_system__.schemas merge "
            f"statements vs {by_uuid} by-uuid — the name path double-resolves the "
            f"schema row (CHA-365 Layer A: carry it on ReadRequestScope and reuse)"
        )

    def test_create_table_by_names(self):
        """create_table accepts catalog_name + schema_name instead of schema_uuid."""
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            "create_by_name_cat", "owner"
        )
        schema_uuid = client.create_schema(
            "create_by_name_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "created_by_name",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_name="create_by_name_cat",
            schema_name="create_by_name_schema",
            author="test",
            comment="create_table",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )
        assert response.table_name == "created_by_name"

    # -- By-uuid resolution needs no schema (CHA-381, Design X) ------------

    def test_read_data_by_table_uuid_without_schema_resolves(self):
        """CHA-381: read_data by table_uuid needs NO schema identifier.

        Before the fold-in, ``ReadRequestScope::resolve_table`` resolved the
        schema unconditionally, so a request with table_uuid but no schema
        raised InvalidRequest "must provide schema_uuid or schema_name"
        (-> InvalidRequestError). Now the by-uuid path skips schema
        resolution entirely and the read resolves.
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"by_uuid_noschema_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "schema_a", catalog_uuid=catalog_uuid, author="test", comment="cha-381"
        )
        table_uuid = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-381",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )

        # No schema_uuid / schema_name — by-uuid resolution must not require it.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert result.num_rows == 1

    def test_get_table_by_table_uuid_without_schema_resolves(self):
        """CHA-381: get_table by table_uuid needs NO schema identifier."""
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"get_by_uuid_noschema_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "schema_a", catalog_uuid=catalog_uuid, author="test", comment="cha-381"
        )
        table_uuid = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-381",
        )
        response = client.get_table(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert response.table_uuid == table_uuid
        assert response.schema_uuid == schema_uuid

    def test_get_table_by_uuid_ignores_passed_schema_returns_true_schema(self):
        """CHA-381: by-uuid resolution is catalog-wide (schema-agnostic).

        Passing a *different* schema_uuid alongside table_uuid resolves the
        table (uuid wins) and the response carries the table's TRUE schema_uuid,
        read off the ``__penca_system__.tables`` row — not the passed one.

        Pre-fix (current main): the get_table refetch arm is schema-scoped, so
        ``get_table(schema_b, table_uuid in schema_a)`` misses -> NotFound;
        read_data's schema-scoped arrow-schema fallback misses too -> NotFound.
        (schema_b is created so schema resolution itself passes — the miss is
        at the table layer, the exact behavior under test.)
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"catwide_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_a = client.create_schema(
            "schema_a", catalog_uuid=catalog_uuid, author="test", comment="cha-381"
        )
        schema_b = client.create_schema(
            "schema_b", catalog_uuid=catalog_uuid, author="test", comment="cha-381"
        )
        table_a = client.create_table(
            "table_a",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a,
            author="test",
            comment="cha-381",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_a,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_a,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )

        # Pass the WRONG schema (schema_b) alongside the table_uuid: uuid wins,
        # the lookup is catalog-wide, and the response carries the TRUE schema.
        response = client.get_table(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            schema_uuid=schema_b,
            table_uuid=table_a,
        )
        assert response.table_uuid == table_a
        assert response.schema_uuid == schema_a

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            schema_uuid=schema_b,
            table_uuid=table_a,
        )
        assert result.num_rows == 1

    # -- Branch reads ------------------------------------------------------

    def test_get_branch(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        created = client.create_branch(
            "get_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        fetched = client.get_branch(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=created.branch_uuid,
        )
        assert fetched.branch_uuid == created.branch_uuid
        assert fetched.branch_name == "get_branch"
        assert fetched.catalog_uuid == catalog_uuid

    def test_list_branches(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_a = client.create_branch(
            "branch_a",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_b = client.create_branch(
            "branch_b",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branches = list(
            client.list_branches(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)
        )
        branch_uuids = [branch.branch_uuid for branch in branches]
        assert branch_a.branch_uuid in branch_uuids
        assert branch_b.branch_uuid in branch_uuids

    def test_read_data_by_table_uuid(self):
        client = make_client()
        context = setup_with_data(client)
        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert result.num_rows == 2
        names = result.column("name").to_pylist()
        assert "alice" in names
        assert "bob" in names

    def test_read_data_by_table_name(self):
        client = make_client()
        context = setup_with_data(client)
        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_name="write_table",
            branch_uuid=context["main_branch_uuid"],
        )
        assert result.num_rows == 2

    def test_read_data_on_branch(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # Branch A with data
        branch_a = client.create_branch(
            "data_a",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        # CHA-184: CreateBranch forks every source-branch table onto the
        # new branch; no per-branch CreateTable needed.
        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_a.branch_uuid,
        )
        batch_a = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_a.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch_a,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_a.branch_uuid,
        )
        client.commit_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_a.branch_uuid,
        )

        # Branch B with different data — table inherits via CreateBranch fork.
        branch_b = client.create_branch(
            "data_b",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        batch_b = pa.table(
            {"name": ["charlie"], "value": [3]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch_b,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_b.branch_uuid,
        )

        # Read branch A — should only see alice
        result_a = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_a.branch_uuid,
        )
        assert result_a.num_rows == 1
        assert result_a.column("name").to_pylist() == ["alice"]

        # Read branch B — should only see charlie
        result_b = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        assert result_b.num_rows == 1
        assert result_b.column("name").to_pylist() == ["charlie"]

    def test_read_data_columns_projection(self):
        """``columns`` pushdown returns only the requested columns in order."""
        client = make_client()
        context = setup_with_data(client)
        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            columns=["name"],
        )
        assert result.schema.names == ["name"]
        assert result.num_rows == 2
        assert set(result.column("name").to_pylist()) == {"alice", "bob"}

    def test_read_data_columns_reordered(self):
        """``columns`` controls output order, not just subset membership."""
        client = make_client()
        context = setup_with_data(client)
        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            columns=["value", "name"],
        )
        assert result.schema.names == ["value", "name"]
        assert result.num_rows == 2

    def test_read_data_columns_unknown_raises(self):
        """Unknown column names are rejected up front by the servicer."""
        client = make_client()
        context = setup_with_data(client)
        with pytest.raises(InvalidRequestError) as exc_info:
            client.read_data(
                catalog_uuid=context["catalog_uuid"],
                schema_uuid=context["schema_uuid"],
                table_uuid=context["table_uuid"],
                branch_uuid=context["main_branch_uuid"],
                columns=["does_not_exist"],
            )

        assert "does_not_exist" in str(exc_info.value)

    def test_read_data_as_of(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "as_of_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        # First tx
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch1 = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch1,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Second tx
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch2 = pa.table(
            {"name": ["bob"], "value": [2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch2,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Read as_of the first commit — should only see alice
        from penca_client._time import micros_to_datetime

        as_of = micros_to_datetime(committed1.commit_micros)
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
            as_of=as_of,
        )
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]

    # -- RYOW (read-your-own-writes via open_tx_uuid) -----------------

    def test_read_data_ryow_basic(self):
        """Open tx + insert: ReadData(open_tx_uuid=tx) sees the row;
        a concurrent reader without open_tx_uuid does not. After
        CommitTx both readers see the row."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(
            {"name": ["ryow_alice"], "value": [101]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Open tx sees its own write.
        ryow = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx.tx_uuid,
        )
        assert ryow.column("name").to_pylist() == ["ryow_alice"]

        # Concurrent reader without open_tx_uuid does not.
        committed_view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert committed_view.num_rows == 0

        # After commit both views see the row.
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )
        post_commit = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert post_commit.column("name").to_pylist() == ["ryow_alice"]

    def test_read_data_post_abort(self):
        """ReadData(open_tx_uuid=tx) after AbortTx → FailedPreconditionError.
        ``begin_tx_log`` survives the abort (lifecycle sweep purges it
        later); the resolver consults ``abort_tx_log`` to reject RYOW
        reads against an aborted tx loudly rather than returning
        silently-empty visibility."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(
            {"name": ["doomed"], "value": [0]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.abort_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        with pytest.raises(FailedPreconditionError):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                open_tx_uuid=tx.tx_uuid,
            )

    def test_read_data_post_commit(self):
        """ReadData(open_tx_uuid=tx) after CommitTx → FailedPreconditionError
        with a precise "already committed at X; pass as_of_micros to
        view post-commit state" hint. ``begin_tx_log`` survives the
        commit (lifecycle sweep purges it later); the resolver
        consults ``commit_tx_log`` to reject RYOW reads against an
        already-committed tx with an actionable error rather than
        treating the tx as still Open and serving stale visibility."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(
            {"name": ["committed_row"], "value": [7]},
            schema=USER_SCHEMA,
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

        with pytest.raises(FailedPreconditionError) as exc_info:
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                open_tx_uuid=tx.tx_uuid,
            )

        assert "already committed" in str(exc_info.value)

    def test_read_data_si_excludes_concurrent_commit(self):
        """Snapshot isolation: another tx that commits AFTER tx_a's
        BEGIN must not appear in tx_a's open-tx view."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        # tx_b begins, writes, and commits while tx_a is still open.
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(
            {"name": ["concurrent_b"], "value": [99]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        # tx_a's snapshot anchored at its begin: must NOT see tx_b's row.
        view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx_a.tx_uuid,
        )
        assert view.num_rows == 0
        client.abort_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_read_data_ryow_plus_si(self):
        """RYOW + SI: tx_a sees its own writes AND not tx_b's
        concurrent commit."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch_a = pa.table(
            {"name": ["from_a"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_a.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch_a),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        # Concurrent tx_b commits.
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch_b = pa.table(
            {"name": ["from_b"], "value": [2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch_b),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx_a.tx_uuid,
        )
        assert view.column("name").to_pylist() == ["from_a"]
        client.abort_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_read_data_ryow_repeatable_reads(self):
        """Repeatable reads inside an open tx: same set of rows is
        returned across multiple ReadData calls even when an unrelated
        tx commits in between."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        first = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx_a.tx_uuid,
        )

        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch_b = pa.table(
            {"name": ["interloper"], "value": [42]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch_b),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        second = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            open_tx_uuid=tx_a.tx_uuid,
        )
        assert first.num_rows == second.num_rows
        assert first.column("name").to_pylist() == second.column("name").to_pylist()
        client.abort_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_read_data_branch_mismatch(self):
        """open_tx_uuid pinned to branch B but request specifies branch A
        → NotFoundError. The resolver targets the request branch's
        ``begin_tx_log`` leaf partition; a tx that lives only on a
        different branch is correctly surfaced as "not found on this
        branch" rather than a separate branch-mismatch error."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        side_branch = client.create_branch(
            "ryow_other_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(
            client, catalog_uuid, schema_uuid, side_branch.branch_uuid
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=side_branch.branch_uuid,
        )
        with pytest.raises(NotFoundError):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                open_tx_uuid=tx.tx_uuid,
            )

        client.abort_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=side_branch.branch_uuid,
        )

    def test_read_data_mutual_exclusion(self):
        """as_of and open_tx_uuid both set → ValueError at client
        layer (the server enforces canonically; the wrapper trips
        first)."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        from penca_client._time import micros_to_datetime

        with pytest.raises(ValueError, match="at most one of"):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                as_of=micros_to_datetime(1),
                open_tx_uuid=tx.tx_uuid,
            )

        client.abort_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_audit_data(self):
        client = make_client()
        context = setup_with_data(client)
        upserts, deletes = client.audit_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
        )
        assert upserts.num_rows == 2
        assert deletes.num_rows == 0

    def test_audit_data_surfaces_tombstones(self):
        """Deletes appear in the audit trail as their own table."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        table_uuid = context["table_uuid"]
        schema_uuid = context["schema_uuid"]
        branch_uuid = context["main_branch_uuid"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        delete_tx = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        # Seeded 2 upserts survive + the tombstone shows up separately.
        # CHA-218: audit schema dropped tx_uuid; identify the tombstone
        # by its commit_micros + author/comment match instead.
        assert upserts.num_rows == 2
        assert deletes.num_rows == 1
        assert deletes.column("commit_micros").to_pylist() == [delete_tx.commit_micros]

    def test_audit_data_delete_only_window(self):
        """Audit window containing only a delete returns empty upserts."""
        client = make_client()
        context = setup_with_data(client)
        catalog_uuid = context["catalog_uuid"]
        table_uuid = context["table_uuid"]
        schema_uuid = context["schema_uuid"]
        branch_uuid = context["main_branch_uuid"]
        seed_tx = context["tx"]

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        from penca_client._time import micros_to_datetime

        after = micros_to_datetime(seed_tx.commit_micros + 1)
        upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            after=after,
        )
        assert upserts.num_rows == 0
        assert deletes.num_rows == 1

    def test_audit_data_with_time_filter(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "audit_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        # First tx
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch1 = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch1,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Second tx
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch2 = pa.table(
            {"name": ["bob"], "value": [2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch2,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Audit with after filter — only the second tx
        from penca_client._time import micros_to_datetime

        after = micros_to_datetime(committed1.commit_micros + 1)
        upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
            after=after,
        )
        assert upserts.num_rows == 1
        assert deletes.num_rows == 0


class TestReadDataProjectionSemantics:
    """CHA-180: ``ReadDataRequest`` must disambiguate "no projection"
    from "0-column projection" at the wire boundary.

    The pre-fix proto encoded projection as ``repeated string columns =
    11``, which collapses "unset" and "explicitly empty" into the same
    wire shape. The fix replaces that field with a message-typed
    ``Projection projection = 11`` whose proto3 presence bit carries
    the missing third state. These tests bypass the Python
    ``client.read_data`` wrapper and construct ``ReadDataRequest``
    directly, pinning the wire semantic independent of any planner
    (DataFusion / Flight SQL) shape.
    """

    def test_read_data_empty_projection_returns_zero_columns(self):
        """``Projection{columns=[]}`` returns 0-col batches whose
        ``num_rows`` equals the table's row count.

        This is the new third state the fix introduces — pre-fix the
        symbol ``Projection`` does not exist in the generated proto
        module, so the import below is the red signal.
        """
        from penca_client.arrow import ipc_bytes_to_batch
        from penca_proto.external.v1.query_pb2 import (
            Projection,
            ReadDataRequest,
        )

        client = make_client()
        context = setup_with_data(client)
        settings = ClientSettings()  # ty: ignore[missing-argument]
        stub = QueryServiceStub(insecure_channel(settings.query_url))

        request = ReadDataRequest(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            branch_uuid=context["main_branch_uuid"],
            table_uuid=context["table_uuid"],
            projection=Projection(columns=[]),
        )
        batches = [
            ipc_bytes_to_batch(response.data) for response in stub.ReadData(request)
        ]

        assert batches, "expected at least one response batch"
        assert all(b.num_columns == 0 for b in batches)
        assert sum(b.num_rows for b in batches) == 2

    def test_read_data_projection_unset_returns_all_columns(self):
        """Default state (``projection`` unset) returns every user
        column — pinning the legacy behavior against the new wire
        shape.

        Constructs ``ReadDataRequest`` without touching the
        ``projection`` field at all so the test is also valid against
        the pre-fix shape (where field 11 was ``repeated string
        columns``); the assertions only depend on the contract, not on
        the field name.
        """
        from penca_client.arrow import ipc_bytes_to_batch
        from penca_proto.external.v1.query_pb2 import ReadDataRequest

        client = make_client()
        context = setup_with_data(client)
        settings = ClientSettings()  # ty: ignore[missing-argument]
        stub = QueryServiceStub(insecure_channel(settings.query_url))

        request = ReadDataRequest(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            branch_uuid=context["main_branch_uuid"],
            table_uuid=context["table_uuid"],
        )
        batches = [
            ipc_bytes_to_batch(response.data) for response in stub.ReadData(request)
        ]

        assert batches, "expected at least one response batch"
        names = batches[0].schema.names
        assert set(names) == {"name", "value"}
        assert sum(b.num_rows for b in batches) == 2

    def test_read_data_projection_excludes_pk_hot_only(self):
        """Projecting to non-PK columns only must work even though the
        PK is absent from the requested schema.

        Pre-FOLLOWUP-A this SchemaMismatched at ``cold_persist_schemas``,
        which the merge path called unconditionally (hot-only reads
        included) and which derived the cold delete shape from the
        *projected* user_schema. After the split, the merge-path delete
        schema is PK-independent, so dropping the PK from the user
        projection is no longer a planning error.
        """
        client = make_client()
        context = setup_with_data(client)

        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            columns=["value"],
        )

        assert result.schema.names == ["value"]
        assert result.num_rows == 2
        assert set(result.column("value").to_pylist()) == {10, 20}

    def test_read_data_projection_excludes_pk_after_persist(self):
        """Same as the hot-only variant, but with a persist in between
        so the cold tier is active. Exercises the cold-side
        ``cold_persist_schemas`` path that was the original surface of the
        PK-in-user_schema dependency."""
        client = make_client()
        context = setup_with_data(client)
        client.persist(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            branch_uuid=context["main_branch_uuid"],
            table_uuid=context["table_uuid"],
        )

        result = client.read_data(
            catalog_uuid=context["catalog_uuid"],
            schema_uuid=context["schema_uuid"],
            table_uuid=context["table_uuid"],
            branch_uuid=context["main_branch_uuid"],
            columns=["value"],
        )

        assert result.schema.names == ["value"]
        assert result.num_rows == 2
        assert set(result.column("value").to_pylist()) == {10, 20}


class TestBranchMaterialization:
    """Verify that create_branch eagerly materializes tables from the source branch."""

    def test_branch_inherits_table_and_supports_write_read(self):
        """Create schema -> create table on main -> create branch -> table visible
        on new branch -> write/read on new branch -> main unaffected."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # Create a branch from main — table should be automatically visible.
        branch = client.create_branch(
            "materialized",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )

        # Table should be visible on the new branch without calling create_table.
        table_info = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_name="write_table",
            branch_uuid=branch.branch_uuid,
        )
        assert table_info.table_uuid == table_uuid

        # Write data on the new branch.
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice", "bob"], "value": [10, 20]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Read data back from the new branch.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 2
        assert set(result.column("name").to_pylist()) == {"alice", "bob"}

        # Main branch should have no data (write was only on the new branch).
        main_result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert main_result.num_rows == 0


# ---------------------------------------------------------------------------
# CHA-142 — filter pushdown on the read path
# ---------------------------------------------------------------------------


_FILTER_STATES = [
    "all_hot",
    "all_cold_unsnapshotted",
    "all_cold_snapshotted",
    "hot_and_cold_unsnapshotted",
    "hot_and_cold_snapshotted",
    "hot_and_cold_mixed",
]

# States whose read hits the snapshot tier, where the residual filter runs
# through ``apply_filter_to_batch`` (CHA-353).
_SNAPSHOT_FILTER_STATES = [
    "all_cold_snapshotted",
    "hot_and_cold_snapshotted",
    "hot_and_cold_mixed",
]

# (filter SQL, client-side predicate) — every shape ``apply_filter_to_batch``
# must preserve when CHA-353 swaps its per-batch ``SELECT … WHERE`` for a
# reused, physical-compiled ``Expr``. The ``lower(...)`` case is the
# fail-first guard: a physical predicate built without the function registry
# can't resolve the UDF.
_SNAPSHOT_FILTER_SHAPES = [
    ("name = 'carol'", lambda n, v: n == "carol"),
    ("value > 25 AND name = 'frank'", lambda n, v: v > 25 and n == "frank"),
    ("lower(name) = 'carol'", lambda n, v: n.lower() == "carol"),
]


def _seed_filter_state(
    client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, state
):
    """Seed six rows in the requested tier distribution.

    Rows: (alice,10), (bob,20), (carol,30) in the first half; (dan,40),
    (eve,50), (frank,60) in the second. Split between tiers per state
    so every filter parity case exercises both halves of the range.
    """
    first = pa.table(
        {"name": ["alice", "bob", "carol"], "value": [10, 20, 30]},
        schema=USER_SCHEMA,
    )
    second = pa.table(
        {"name": ["dan", "eve", "frank"], "value": [40, 50, 60]},
        schema=USER_SCHEMA,
    )

    def _commit(batch):
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

    def _persist():
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

    def _snapshot():
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

    if state == "all_hot":
        _commit(first)
        _commit(second)
    elif state == "all_cold_unsnapshotted":
        _commit(first)
        _commit(second)
        _persist()
    elif state == "all_cold_snapshotted":
        _commit(first)
        _commit(second)
        _persist()
        _snapshot()
    elif state == "hot_and_cold_unsnapshotted":
        _commit(first)
        _persist()
        _commit(second)
    elif state == "hot_and_cold_snapshotted":
        _commit(first)
        _persist()
        _snapshot()
        _commit(second)
    elif state == "hot_and_cold_mixed":
        # Snapshot carries alice/bob; cold-unsnapshotted carries carol/dan;
        # hot carries eve/frank. Covers all three paths in one run.
        _commit(
            pa.table(
                {"name": ["alice", "bob"], "value": [10, 20]},
                schema=USER_SCHEMA,
            )
        )
        _persist()
        _snapshot()
        _commit(
            pa.table(
                {"name": ["carol", "dan"], "value": [30, 40]},
                schema=USER_SCHEMA,
            )
        )
        _persist()
        _commit(
            pa.table(
                {"name": ["eve", "frank"], "value": [50, 60]},
                schema=USER_SCHEMA,
            )
        )
    else:
        msg = f"unknown state {state}"
        raise ValueError(msg)


def _rows_sorted(result):
    """Return (name, value) tuples sorted by name — tier-union order varies."""
    names = result.column("name").to_pylist()
    values = result.column("value").to_pylist()
    return sorted(zip(names, values, strict=True))


class TestReadDataFilterPushdown:
    """CHA-142: filter pushdown on the read_data path.

    Verifies two properties:

    * Parity: filtering via the ``filter`` argument returns the same
      rows as filtering client-side after an unfiltered read. Runs
      against every tier distribution so both the hot-tier SQL, the
      cold-tier DataFusion SQL, and the per-segment snapshot filter
      are all exercised.
    * Correctness: the exclusion set that shadows stale snapshot rows
      must be built from the UNFILTERED logs (CHA-142 invariant).
      Validated by ``test_stale_snapshot_is_excluded``.
    """

    @pytest.mark.parametrize("state", _FILTER_STATES)
    def test_filter_parity(self, state):
        client = make_client()

        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid
        _seed_filter_state(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, state
        )

        pushdown = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value > 25",
        )
        baseline = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        expected = sorted(
            (n, v)
            for n, v in zip(
                baseline.column("name").to_pylist(),
                baseline.column("value").to_pylist(),
                strict=True,
            )
            if v > 25
        )
        assert _rows_sorted(pushdown) == expected
        assert pushdown.schema.equals(baseline.schema)

    def test_filter_all_hot_fast_path(self):
        """Named test for the fast-path branch.

        Mechanically a parity case on the ``all_hot`` state, but kept
        as a standalone test so a future refactor that silently
        collapses the fast path into the generic path fails here.
        """
        client = make_client()

        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid
        _seed_filter_state(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, "all_hot"
        )

        pushdown = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="name = 'alice' OR name = 'frank'",
        )
        assert _rows_sorted(pushdown) == [("alice", 10), ("frank", 60)]

    @pytest.mark.parametrize("filter_sql,predicate", _SNAPSHOT_FILTER_SHAPES)
    @pytest.mark.parametrize("state", _SNAPSHOT_FILTER_STATES)
    def test_snapshot_filter_shape_parity(self, state, filter_sql, predicate):
        """CHA-353: the snapshot-tier residual filter must return the same
        rows as a client-side filter, across string equality, compound, and
        UDF-bearing predicates. Behavior-preservation oracle for the
        reuse-parsed-Expr rewrite of ``apply_filter_to_batch`` — the
        ``lower(...)`` case fails first if the physical predicate is built
        without the function registry.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid
        _seed_filter_state(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, state
        )

        pushdown = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter=filter_sql,
        )
        baseline = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        expected = sorted(
            (n, v)
            for n, v in zip(
                baseline.column("name").to_pylist(),
                baseline.column("value").to_pylist(),
                strict=True,
            )
            if predicate(n, v)
        )
        assert _rows_sorted(pushdown) == expected
        assert pushdown.schema.equals(baseline.schema)

    def test_snapshot_filter_null_semantics(self):
        """CHA-353: three-valued logic (IS NULL / IS NOT NULL) on the
        snapshot-tier residual filter. Seeds a NULL value, then persists +
        snapshots so the read lands in ``apply_filter_to_batch``.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        batch = pa.table(
            {"name": ["alice", "bob"], "value": [10, None]}, schema=USER_SCHEMA
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
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
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        def _read(filt):
            return client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
                filter=filt,
            )

        is_null = _read("value IS NULL")
        assert _rows_sorted(is_null) == [("bob", None)]
        is_not_null = _read("value IS NOT NULL")
        assert _rows_sorted(is_not_null) == [("alice", 10)]

    def test_snapshot_filter_cross_type_literal(self):
        """CHA-353: integer literals compared to non-Int64 columns (Int32,
        Float64) must coerce, not error. ``parse_sql_expr`` leaves the literal
        ``Int64``; without coercion in ``compile_residual_filter`` the compiled
        predicate fails arrow's compare kernel and aborts the read. The
        Int64-only parity suite above can't reach this path.
        """
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"cha353_xtype_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="test", comment="cha353"
        )
        arrow_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("count", pa.int32()),
                pa.field("score", pa.float64()),
            ]
        )
        table_uuid = client.create_table(
            "t",
            arrow_schema,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha353",
        )
        batch = pa.table(
            {"name": ["a", "b", "c"], "count": [1, 6, 10], "score": [0.5, 2.5, 9.0]},
            schema=arrow_schema,
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )

        def _names(filt):
            r = client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=main_branch_uuid,
                filter=filt,
            )
            return sorted(r.column("name").to_pylist())

        # Int32 column vs Int64 literal.
        assert _names("count > 5") == ["b", "c"]
        # Float64 column vs Int64 literal.
        assert _names("score > 1") == ["b", "c"]

    def test_stale_snapshot_is_excluded(self):
        """Ticket-mandated correctness case.

        Insert a row, persist, snapshot, then update its value so the
        snapshot carries a stale version that matches the filter while
        the current hot-tier value does not. The exclusion set — built
        from UNFILTERED logs — must shadow the snapshot row. If the
        exclusion set were (incorrectly) filtered, the snapshot row
        would leak through.
        """
        client = make_client()

        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        # Phase 1: insert with value=5 (matches filter "value < 50").
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [5]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        # Phase 2: persist + snapshot — stale value 5 now lives in cold
        # snapshot segments.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        # Phase 3: hot-tier update to value=100 (does NOT match filter).
        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [100]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        # The stale snapshot value (5) matches; the current hot value
        # (100) does not. Neither should appear — the snapshot is
        # shadowed by the exclusion set, and the hot row is dropped by
        # the filter.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
            filter="value < 50",
        )
        assert result.num_rows == 0

        # Sanity-check the unfiltered read: the hot update wins and
        # only the value=100 row is visible.
        unfiltered = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert unfiltered.num_rows == 1
        assert unfiltered.column("value").to_pylist() == [100]


# ---------------------------------------------------------------------------
# CHA-361: GetPlan without committed_at pins to pg_now (no unbounded plan)
# ---------------------------------------------------------------------------


class TestTimeTravelCatalogReadRespectsAsOfOnColdTier:
    """A time-travel catalog resolution (read ``AS OF`` a past point)
    must not see catalog rows committed after that point, even once the
    system catalog ``__penca_system__.tables`` has been persisted to
    cold.

    CHA-361 (commit B): ``resolve_table_metadata`` /
    ``resolve_schema`` previously planned the catalog read with
    ``as_of=None``, so ``cold_max = hot_min`` and the cold-tier catalog
    rows were bounded only by the hot/cold cutoff — the read's snapshot
    was never threaded into the plan, and cold log/segment rows carry
    no snapshot filter of their own (only the plan's ``committed_at``
    window does; the hot tier was already tightened via
    ``tighten_for_hot``). A table created *after* the as_of, whose
    catalog row had moved to cold (``committed_at < hot_min``), would
    leak into a time-travel resolution. Passing the in-scope snapshot
    bounds ``cold_max = min(as_of + 1, hot_min)``, excluding it.

    Pre-fix: reading ``table_two`` ``AS OF`` a point before it existed
    resolves the leaked cold catalog row and returns an empty table.
    Post-fix: it raises ``NotFound`` — ``table_two`` did not exist at
    that point.
    """

    def test_time_travel_table_read_excludes_later_cold_catalog_row(self):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"tt_catalog_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "schema_a",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="cha-361",
        )
        table_one = client.create_table(
            "table_one",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-361",
        )

        # Commit a row to table_one to capture a server-side timestamp
        # that is strictly after table_one's catalog row and strictly
        # before table_two's — the time-travel point ``mid``.
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_one,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        mid_micros = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        ).commit_micros

        # table_two's catalog row commits strictly after ``mid``.
        client.create_table(
            "table_two",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="cha-361",
        )

        # Persist the system catalog so both catalog rows move to cold
        # (hot_min(__penca_system__.tables) advances past table_two's
        # commit). The cold tier is now the only source of the catalog
        # rows for a read below hot_min — the exact path commit B
        # tightened.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=system_tables_table_uuid(catalog_uuid),
        )

        as_of_mid = micros_to_datetime(mid_micros)

        # Sanity: table_one existed at ``mid`` — a time-travel read
        # resolves it via the cold catalog and returns its row.
        one = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_name="table_one",
            as_of=as_of_mid,
        )
        assert one.num_rows == 1, (
            "table_one existed at the as_of point and must resolve via "
            "the cold-tier catalog"
        )

        # The invariant: table_two did NOT exist at ``mid``. Its catalog
        # row (committed after ``mid``, now in cold) must not leak into
        # the time-travel resolution, so the read raises NotFound.
        with pytest.raises(NotFoundError):
            client.read_data(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                table_name="table_two",
                as_of=as_of_mid,
            )


# ---------------------------------------------------------------------------
# The CHA-215 time-travel segment-selection tests (TestTimeTravelSegmentFilters)
# were removed with the StorageMetadataService Plan RPC they dialed (CHA-445).
# Their DB-bound coverage — persist-segment interval-overlap straddle behavior
# and snapshot-picker watermark (not commit-time) ordering — is tracked for
# restoration in CHA-456.
# ---------------------------------------------------------------------------


def _commit_one_row(
    client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, name, value
):
    """Begin / mutate / commit a single-row upsert. Returns the
    committed ``Tx`` so the caller can pin ``commit_micros``."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    batch = pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA)
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


def _micros_to_dt(micros: int):
    """Wrap the client-side micros→datetime helper so tests don't
    duplicate the import line."""
    from penca_client._time import micros_to_datetime

    return micros_to_datetime(micros)


# ---------------------------------------------------------------------------
# CHA-218: cold reads/audit after pre-joining commit_tx_log into persist segments
# ---------------------------------------------------------------------------
#
# These tests pin the read-path consequences of CHA-218: cold merge-read
# collapses to a pure scan (no JOIN against cold ``commit_tx_log``); ``audit_data``
# becomes a hot + cold scan with the four denormalized tx metadata
# columns surfaced from cold segments directly. The ``audit_data`` schema
# drops ``tx_uuid``.
#
# RED today:
#
# - ``audit_data`` reads hot only; after a persist + hot purge the
#   cold-only window returns empty even though the rows exist in
#   cold persist segments.
# - The audit row schema still includes ``tx_uuid`` (today's
#   ``audit_tx_metadata_fields`` carries it as the first field).
# - Post-snapshot reads currently round-trip through the cold
#   ``commit_tx_log`` JOIN; after the JOIN is dropped, the equivalent
#   answer must still hold but is computed differently.


class TestColdRowsPreserveTxMetadataAfterHotPurge:
    """After a persist purges hot ``commit_tx_log_partition``, ``audit_data``
    still surfaces every version's ``(began_at_micros, commit_micros,
    comment, author)`` for cold rows — read directly from the persist
    segments, not via a JOIN. Pins the join-before-purge ordering
    invariant: Phase 1's widened JOIN must copy tx metadata onto cold
    rows before Phase 2 deletes the hot rows.

    RED today: cold ``audit_data`` rounds-trips through the cold
    ``commit_tx_log`` JOIN, which is gone after CHA-218. Without the
    denormalization on cold persist segments, post-purge audit on
    cold-only data would lose all four metadata fields.
    """

    def test_cold_audit_returns_per_version_denormalized_metadata(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Two committed txs with distinct (author, comment) on the same
        # table. Each writes a different row.
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            author="ada",
            comment="seed alice",
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [1]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            author="grace",
            comment="seed bob",
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["bob"], "value": [2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        committed2 = client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        # Persist — both rows go to cold; hot commit_tx_log is purged.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            include_tx_metadata=True,
        )
        # Both versions surface from cold with their denormalized metadata.
        # Index by name so the assertions are independent of row order.
        names = upserts.column("name").to_pylist()
        committed_at = upserts.column("commit_micros").to_pylist()
        began_at = upserts.column("began_at_micros").to_pylist()
        comments = upserts.column("comment").to_pylist()
        authors = upserts.column("author").to_pylist()
        by_name = {
            n: (ca, ba, c, a)
            for n, ca, ba, c, a in zip(
                names, committed_at, began_at, comments, authors, strict=True
            )
        }

        assert "alice" in by_name and "bob" in by_name, (
            f"expected both upsert versions in cold audit, got names={names}"
        )
        ca1, ba1, c1, a1 = by_name["alice"]
        ca2, ba2, c2, a2 = by_name["bob"]
        # CommitTxResponse no longer carries began_at_micros (CHA-222);
        # use the BeginTx response for that side of the assertion.
        assert ca1 == committed1.commit_micros
        assert ba1 == tx1.began_at_micros
        assert c1 == "seed alice"
        assert a1 == "ada"
        assert ca2 == committed2.commit_micros
        assert ba2 == tx2.began_at_micros
        assert c2 == "seed bob"
        assert a2 == "grace"


class TestPostPersistReadParity:
    """Reading a table before and after a persist returns the same rows,
    and time-travel ``as_of`` resolves correctly against cold persist
    segments under the new schema (no ``tx_uuid``, no JOIN).

    RED today: pre-CHA-218 cold reads materialize via the
    ``upsert_log JOIN commit_tx_log`` SQL builder; once we drop the JOIN
    and the cold ``commit_tx_log`` artifact, the planner needs to project
    ``commit_micros`` directly off the upsert segments. Without
    the change the cold path either misses the column or returns
    nothing once hot is purged.
    """

    def test_post_persist_read_matches_pre_persist_read(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Two commits — second supersedes the first row's value.
        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice", "bob"], "value": [1, 2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [99]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        # Pre-persist latest view.
        pre = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        pre_rows = sorted(
            zip(
                pre.column("name").to_pylist(),
                pre.column("value").to_pylist(),
                strict=True,
            )
        )
        assert pre_rows == [("alice", 99), ("bob", 2)]

        # Persist + post-persist latest view — identical content.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        post = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        post_rows = sorted(
            zip(
                post.column("name").to_pylist(),
                post.column("value").to_pylist(),
                strict=True,
            )
        )
        assert post_rows == pre_rows, (
            f"cold read after persist diverged: pre={pre_rows}, post={post_rows}"
        )

        # Time-travel: as_of the first commit must still see alice=1,
        # bob=2 from the cold persist segments alone — the new cold
        # filter must apply ``commit_micros <= as_of`` against
        # the per-row column.
        from penca_client._time import micros_to_datetime

        as_of = micros_to_datetime(committed1.commit_micros)
        snapshot_view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            as_of=as_of,
        )
        as_of_rows = sorted(
            zip(
                snapshot_view.column("name").to_pylist(),
                snapshot_view.column("value").to_pylist(),
                strict=True,
            )
        )
        assert as_of_rows == [("alice", 1), ("bob", 2)], (
            f"as_of read against cold persist segments diverged: got {as_of_rows}"
        )


class TestPostSnapshotReadParity:
    """A write → persist → snapshot → write → persist → read round-trip
    returns the union of cold snapshot baseline + post-snapshot cold
    persist segments + hot, with deduplication on ``row_uuid``.

    RED today: the cold visibility predicate against the snapshot
    baseline still depends on the JOIN-driven ``commit_micros``
    surface area; after CHA-218 the predicate runs against the
    denormalized column on persist segments. Snapshot-baseline rows have
    their commit_micros stamped at snapshot time; both must
    resolve consistently after the JOIN is dropped.
    """

    def test_post_snapshot_read_merges_baseline_plus_post_snapshot_persist(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # tx_a: alice=1, bob=2. Then persist + snapshot — baseline.
        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_a.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice", "bob"], "value": [1, 2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        # tx_b: alice → 99 (post-snapshot cold persist segment).
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [99]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # tx_c: carol=3, still in hot at read time.
        tx_c = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_c.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["carol"], "value": [3]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx_c.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        rows = sorted(
            zip(
                view.column("name").to_pylist(),
                view.column("value").to_pylist(),
                strict=True,
            )
        )
        assert rows == [("alice", 99), ("bob", 2), ("carol", 3)], (
            f"snapshot+persist+hot merge diverged: got {rows}"
        )


class TestAuditCoversPureColdData:
    """[CHA-217] After a persist, ``audit_data`` surfaces every cold
    committed version and every cold tombstone — purely from cold
    persist segments — with the four tx metadata fields populated and
    no ``tx_uuid`` column.

    RED today: ``audit_data`` calls ``hot.audit_upserts_stream`` /
    ``hot.audit_deletes_stream`` only. Once hot is purged at persist
    time, the cold-only window returns zero rows. The CHA-217
    extension folded into this PR adds the cold scan; the schema
    drop of ``tx_uuid`` lands in the same commit.
    """

    def test_audit_data_after_persist_returns_all_cold_versions_and_tombstones(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Seed two rows, then delete one, then persist — gives one cold
        # upsert (bob) plus one cold tombstone (alice) plus one cold
        # superseded upsert (alice's initial version).
        tx_seed = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            author="seeder",
            comment="seed",
        )
        client.write_data(
            tx_seed.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice", "bob"], "value": [1, 2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx_seed.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx_del = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            author="deleter",
            comment="del alice",
        )
        client.write_data(
            tx_del.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx_del.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        # Persist — moves both upsert versions and the tombstone into
        # cold persist segments and purges hot commit_tx_log.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            include_tx_metadata=True,
        )

        # Two upsert versions (alice + bob) survive in audit.
        assert upserts.num_rows == 2, (
            f"expected 2 cold upsert versions in audit, got {upserts.num_rows}"
        )
        # One tombstone (alice's delete).
        assert deletes.num_rows == 1, (
            f"expected 1 cold tombstone in audit, got {deletes.num_rows}"
        )

        # The four tx metadata fields are populated for every row.
        for col in ("began_at_micros", "commit_micros", "comment", "author"):
            assert col in upserts.schema.names
            assert col in deletes.schema.names
            assert upserts.column(col).null_count == 0, (
                f"audit upserts column {col} has nulls — cold rows must carry"
                " the denormalized tx metadata"
            )
            assert deletes.column(col).null_count == 0, (
                f"audit deletes column {col} has nulls — cold tombstones must"
                " carry the denormalized tx metadata"
            )

        # Schema does not include tx_uuid.
        assert "tx_uuid" not in upserts.schema.names
        assert "tx_uuid" not in deletes.schema.names


class TestAuditCoversHotColdMix:
    """[CHA-217] ``audit_data`` returns the full history in
    ``commit_micros`` order when versions span both tiers (cold
    persisted + hot un-persisted).

    RED today on the same grounds as ``TestAuditCoversPureColdData``
    — the cold half of the audit stream isn't surfaced. Once the cold
    extension lands the merge is in commit-time order across tiers.
    """

    def test_audit_data_spans_hot_and_cold(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        tx_cold = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_cold.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [1]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        committed_cold = client.commit_tx(
            tx_cold.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        tx_hot = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_hot.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["bob"], "value": [2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        committed_hot = client.commit_tx(
            tx_hot.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        assert upserts.num_rows == 2, (
            f"expected hot+cold audit to surface both versions, got {upserts.num_rows}"
        )
        # `audit_data` does not promise a sorted stream — the boundary
        # invariant `max(cold.committed_at) < min(hot.committed_at)`
        # holds, but neither tier sorts internally. Identify each
        # version by `name` rather than by stream position.
        committed_at = upserts.column("commit_micros").to_pylist()
        names = upserts.column("name").to_pylist()
        assert dict(zip(names, committed_at, strict=True)) == {
            "alice": committed_cold.commit_micros,
            "bob": committed_hot.commit_micros,
        }


class TestAuditHorizonPastSnapshot:
    """[CHA-217] After a snapshot, older versions sitting under the
    snapshot baseline remain visible in ``audit_data`` — the audit
    horizon is the underlying persist segments, not the snapshot.
    ADR 0011 governs the actual horizon via ``RetentionConfig``; the
    default (unbounded) is what this test exercises.

    RED today: audit reads hot only, and the snapshot operation
    materializes ``latest per row_uuid`` into a baseline that drops
    per-version metadata. With the CHA-217 cold extension folded in,
    the audit stream reads the underlying persist segments — which
    retain every version — so the pre-snapshot superseded version
    surfaces.
    """

    def test_audit_data_surfaces_versions_under_snapshot_baseline(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [1]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice"], "value": [2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        # Persist + snapshot — baseline materializes alice=2 only.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        # Both versions survive in audit even though the snapshot
        # baseline only materializes the latest. The audit horizon is
        # the cold persist segments, not the snapshot.
        values = sorted(upserts.column("value").to_pylist())
        assert values == [1, 2], (
            f"expected both pre-snapshot versions in audit, got values={values}"
        )


class TestEmptyAuditSchemaHeader:
    """``audit_data`` on a table with neither hot nor cold rows yields
    a non-zero-batch Arrow Table whose schema matches the post-CHA-218
    audit shape (user cols + four metadata fields, no ``tx_uuid``).
    Verifies the schema-header batch contract on the new schema.

    RED today: the audit schema still carries ``tx_uuid``, so the
    field-list assertion fails. Once the schema lands the field-list
    matches.
    """

    def test_empty_audit_yields_post_cha218_schema(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        assert upserts.num_rows == 0
        assert deletes.num_rows == 0
        # The schema is still recoverable on the empty Table because
        # the server emits an empty-schema-header batch first.
        upsert_names = list(upserts.schema.names)
        assert "tx_uuid" not in upsert_names
        # CHA-507: comment/author are opt-in (include_tx_metadata), so the
        # default audit schema omits them.
        assert "comment" not in upsert_names
        assert "author" not in upsert_names
        for required in (
            "name",
            "value",
            "began_at_micros",
            "commit_micros",
        ):
            assert required in upsert_names, (
                f"empty audit upserts schema missing {required!r}; got {upsert_names}"
            )

        delete_names = list(deletes.schema.names)
        assert "tx_uuid" not in delete_names
        # CHA-185: deletes carry the table's PK columns natively
        # (here, ``name`` from ``USER_SCHEMA``); ``row_uuid`` is no
        # longer projected.
        assert "row_uuid" not in delete_names
        # CHA-507: comment/author are opt-in, absent from the default schema.
        assert "comment" not in delete_names
        assert "author" not in delete_names
        for required in (
            "name",
            "began_at_micros",
            "commit_micros",
        ):
            assert required in delete_names, (
                f"empty audit deletes schema missing {required!r}; got {delete_names}"
            )


class TestAuditAppliesPurgeCutoff:
    """CHA-444 (ADR 0027): ``audit_data`` partitions hot vs cold by the read
    fence ``Pu`` (the purge watermark) — cold capped ``<= Pu``, hot floored
    ``> Pu``. Without the partition, a row that exists in BOTH tiers (a cold
    persist segment plus its still-hot copy — the persisted-but-unpurged
    steady state) would surface twice.

    Each cycle below runs a full Persist → Snapshot → Purge so ``Pu`` advances
    past the cycle's rows; a second Persist (without Purge) then leaves a row
    straddling both tiers, and the fence must serve it from exactly one tier.
    """

    def test_audit_post_purge_serves_each_version_from_one_tier(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Cycle 1: alice=1 → Persist → Snapshot → Purge. CHA-444 (ADR 0027):
        # Snapshot moves alice into the snapshot baseline and Purge (Pu =
        # W_snap) clears it from hot, so alice lives in cold only.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # Cycle 2: bob=2 → Persist (no Purge). Bob now lives in BOTH
        # tiers — a fresh cold persist segment plus the still-hot
        # upsert log row. Audit must surface bob exactly once.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "bob", 2
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        # Without the fence: bob duplicates → 3 rows. With the CHA-444 read
        # fence Pu: alice from the snapshot baseline (cold), bob from hot
        # (persisted-but-unpurged steady state — bob's commit_seq_num > Pu, and
        # its cold persist segment is excluded from the cold audit window),
        # so each version surfaces exactly once.
        names = sorted(upserts.column("name").to_pylist())
        assert names == ["alice", "bob"], (
            f"expected one alice + one bob across hot+cold audit, got {names}"
        )

        # Boundary invariant per the audit_upserts comment:
        # max(cold.committed_at) < min(hot.committed_at). We can't
        # observe tier directly, so pin the strictly-increasing
        # ordering of the two timestamps — alice was committed
        # before bob.
        by_name = dict(
            zip(
                upserts.column("name").to_pylist(),
                upserts.column("commit_micros").to_pylist(),
                strict=True,
            )
        )
        assert by_name["alice"] < by_name["bob"]

    def test_audit_post_purge_serves_each_tombstone_from_one_tier(self):
        """Delete-side mirror of
        :meth:`test_audit_post_purge_serves_each_version_from_one_tier`.
        ``audit_deletes`` applies the same strict ``hot_min``
        partition as ``audit_upserts``; without it, a tombstone that
        exists in both tiers between Persist and Purge would
        surface twice in the audit stream."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Cycle 1: upsert+delete alice → Persist → Snapshot → Purge. CHA-444
        # (ADR 0027): Snapshot folds alice's tombstone into the snapshot
        # baseline and Purge (Pu = W_snap) clears it from hot, so the
        # tombstone lives in cold only.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )
        tx_alice_del = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_alice_del.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        alice_del_tx = client.commit_tx(
            tx_alice_del.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # Cycle 2: upsert+delete bob → Persist (no Purge). Bob's
        # tombstone now lives in BOTH tiers — fresh cold persist
        # segment plus the still-hot delete_log row.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "bob", 2
        )
        tx_bob_del = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_bob_del.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["bob"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        bob_del_tx = client.commit_tx(
            tx_bob_del.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        _upserts, deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        # Without the fence: bob's tombstone duplicates → 3 deletes. With the
        # CHA-444 read fence Pu: alice's tombstone from the snapshot baseline
        # (cold) + bob's tombstone from hot (persisted-but-unpurged — bob's
        # commit_seq_num > Pu, its cold persist segment excluded from the cold
        # audit window), one each.
        committed = sorted(deletes.column("commit_micros").to_pylist())
        assert committed == [
            alice_del_tx.commit_micros,
            bob_del_tx.commit_micros,
        ], (
            f"expected one alice tombstone + one bob tombstone across "
            f"hot+cold audit, got commit_micros={committed}"
        )


# ---------------------------------------------------------------------------
# CHA-227: strict hot/cold partition + plan-time threading in plan()
# ---------------------------------------------------------------------------
#
# Pins the planner reshape: pre-Purge ``plan()`` returns
# ``cold_storage = None`` (hot serves everything); post-Purge the cold
# upper bound rides in ``PersistPlan.committed_at: IntegerRange``,
# matching the existing ``HotStoragePlan.committed_at`` field. The
# combined `(cold_max, hot_min)` window is a strict partition — no
# overlap, no merge-layer dedup absorbing same-version double presence.
#
# Sibling of ``TestAuditAppliesPurgeCutoff`` above: that locks the
# cutoff for ``audit_data`` (which has no dedup); CHA-227 propagates
# the same structural rule to ``read_data`` / ``plan()``.


def _max_committed_persist_seg_max_tx_q(catalog_uuid, branch_uuid, table_uuid):
    """``max(max_tx_commit_micros)`` over committed persist segments
    for ``(branch, table)`` — local mirror of the lifecycle-test helper.
    """
    seg_parent = f"{catalog_uuid}_table_persist_segment_metadata"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(max_tx_commit_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg_parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


class TestCHA227PlanStrictPartition:
    """``plan()`` (a) omits ``cold_storage`` pre-Persist, and (b) carries
    a half-open ``PersistPlan.committed_at = [None, hot_min)`` window
    post-Persist that the merge layer applies as a per-row filter on
    cold. Together with the existing
    ``HotStoragePlan.committed_at.min = hot_min``, this is the
    strict tier partition the ticket is named after. CHA-233 (ADR 0019)
    moved the cutoff source from ``max(purged_at) + 1`` to
    ``max(persisted_at) + 1``; the threading shape is unchanged.
    """

    def test_read_data_post_persist_each_row_surfaces_from_one_tier(
        self,
    ):
        """End-to-end pin of the strict partition. Cycle 1 (alice →
        Persist → Purge) leaves alice in cold only; cycle 2 (bob →
        Persist, no Purge) leaves bob in both tiers physically, but
        CHA-233's per-row ``cold.committed_at.max = hot_min``
        filter (sourced from ``max(persisted_at) + 1``) makes bob's
        hot copy invisible and cold serves it instead. End-state:
        ``read_data`` returns one alice + one bob, each from cold.

        Sibling of ``TestAuditAppliesPurgeCutoff`` for the read path —
        ``read_data`` already deduplicates via merge-layer ``row_uuid``
        collapse, but the planner-level strict partition is the
        structural fix.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Cycle 1: alice → Persist → Snapshot → Purge. CHA-444 (ADR 0027):
        # Snapshot moves alice into the snapshot baseline so Purge (Pu =
        # W_snap) clears it from hot — alice then lives in cold only.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # Cycle 2: bob → Persist (no Purge). Bob's hot row stays
        # physically present until Purge runs again.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "bob", 2
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # End-to-end: read_data returns one alice + one bob. (The plan-level
        # strict-partition cross-check moved to Rust assemble_plan unit tests
        # / CHA-456; here we pin the observable read result.)
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        assert result.num_rows == 2
        assert sorted(result.column("name").to_pylist()) == ["alice", "bob"]

    def test_read_data_open_tx_cold_read_unaffected_by_began_at(self):
        """Two open txs, both begun AFTER alice committed, see identical
        cold-side rows (``{alice}``) — a row committed before an open tx's
        frontier is visible to it regardless of how much later the tx began.

        CHA-444 (ADR 0027): the OpenTx cold read now applies the seq bound
        ``commit_seq_num < began_at_seq_num`` (``plan_commit_seq_upper`` →
        ``began_at_seq_num - 1``), replacing Persist's removed
        ``oldest_open_began_at`` write-time clamp. Both txs began after alice,
        so alice (``< both frontiers``) is visible to both. The
        commit-AFTER-began *exclusion* is pinned separately by
        ``test_read_data_open_tx_excludes_post_began_commit_from_cold``.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # Persist → Snapshot → Purge so alice lives in cold only (CHA-444 /
        # ADR 0027: Purge advances Pu only to W_snap, so Snapshot must run
        # first to move alice to the baseline and clear it from hot).
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        # Two open txs with different began_at — both see alice from cold.
        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        assert tx_a.began_at_micros < tx_b.began_at_micros

        view_a = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            open_tx_uuid=tx_a.tx_uuid,
        )
        view_b = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            open_tx_uuid=tx_b.tx_uuid,
        )
        assert view_a.column("name").to_pylist() == ["alice"]
        assert view_b.column("name").to_pylist() == view_a.column("name").to_pylist(), (
            "alice committed before both open txs' frontiers, so both see it "
            "from cold (CHA-444: the cold read bounds at "
            "commit_seq_num < began_at_seq_num)"
        )

        for tx in (tx_a, tx_b):
            client.abort_tx(
                tx.tx_uuid,
                catalog_uuid=catalog_uuid,
                branch_uuid=main_uuid,
            )

    def test_read_data_open_tx_excludes_post_began_commit_from_cold(self):
        """CHA-444 regression: an OpenTx read must NOT see a row committed by
        another tx *after* it began, even once that row has reached cold.

        CHA-444 (ADR 0027) dropped Persist's ``oldest_open_began_at`` write-time
        clamp (which used to keep cold frozen below every open tx's frontier),
        so the OpenTx snapshot-isolation bound ``commit_seq_num < began_at_seq_num``
        now rides the cold read's seq upper (``plan_commit_seq_upper`` →
        ``began_at_seq_num - 1``), covering the snapshot picker AND the cold
        persist read. Without it, X would serve the post-began row from cold.

        Setup: commit alice; open tx X; commit bob (after X began); Persist →
        Snapshot → Purge so both land in cold and hot is cleared. X must read
        only alice (committed before it began), never bob.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        # alice committed BEFORE X begins → visible to X.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )

        # Open tx X — its began_at_seq_num frontier sits between alice and bob.
        tx_x = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )

        # bob committed AFTER X began → must NOT be visible to X.
        _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "bob", 2
        )

        # Drive both rows into cold and clear hot.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        view = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            open_tx_uuid=tx_x.tx_uuid,
        )
        assert view.column("name").to_pylist() == ["alice"], (
            "OpenTx X must see only alice (committed before X began); bob "
            "committed after X began and must be excluded from the cold read "
            f"too — got {view.column('name').to_pylist()}"
        )

        client.abort_tx(tx_x.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_uuid)

    def test_read_data_as_of_post_purge_against_cold(self):
        """Post-Purge time-travel reads still work. Cycle through
        Persist+Purge of alice, then ``read_data(as_of=alice.commit)``
        returns alice from cold via the new
        ``PersistPlan.committed_at`` per-row filter. Locks the CHA-227
        replacement of the merge-layer ``cold_visibility_clause``
        ``AsOfMicros`` arm with the planner-driven IntegerRange.

        Regression guard: today this works via the soon-to-be-deleted
        ``cold_visibility_clause``; after CHA-227 it must work via the
        new field. The proto-shape assertion at the bottom pins which
        path served it.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        committed = _commit_one_row(
            client, catalog_uuid, schema_uuid, table_uuid, main_uuid, "alice", 1
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        # CHA-444 (ADR 0027): Snapshot before Purge so alice moves to the
        # snapshot baseline (cold) and Purge clears it from hot.
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
            as_of=_micros_to_dt(committed.commit_micros),
        )
        assert result.column("name").to_pylist() == ["alice"]
