"""CHA-380 red-tests — distinct entity-uuid PK columns on ``__penca_system__`` rows.

Today the bootstrap/metadata rows in ``__penca_system__.{schemas,tables,indexes}``
overload ``row_uuid`` *as* the described entity's own namespace uuid
(``row_uuid == schema_uuid`` / ``table_uuid`` / ``index_uuid``) — a special case
versus the universal auditable-store invariant (ADR 0013), where the entity's own
uuid is a first-class PK column and ``row_uuid = row_uuid_for_pk(parent, [pk...])``.

This ticket regularizes all three system tables: give each row a **distinct**
entity-uuid column (``schema_uuid`` / ``table_uuid`` / ``index_uuid``) and derive
``row_uuid`` canonically like every other Penca table.

**No gRPC-observable change** — ``get_table`` / ``list_tables`` already return the
entity uuid (sourced today from ``row_uuid``), so the responses are byte-identical
before and after. The only observable is the **physical** per-branch data-log
schema and the ``row_uuid`` derivation, which the integration suite reads via the
sanctioned white-box Postgres seam (``get_pg_driver``; see ``integration_helpers``).

Fail-first (current ``main``):

* the entity-uuid columns do not exist → ``SELECT table_uuid``/``schema_uuid``/
  ``index_uuid`` raises ``psycopg.errors.UndefinedColumn``;
* ``row_uuid`` equals the raw entity uuid, so the ``row_uuid ==
  row_uuid_for_pk(...)`` (and ``row_uuid != <entity_uuid>``) assertions fail.

Scoped run::

    just integration-test --test-arg integration_cha380_system_pk_columns_test
"""

from __future__ import annotations

from uuid import uuid4

from penca_client.naming import (
    row_uuid_for_pk,
    system_indexes_table_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
    upsert_log_table,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)


def _fresh_catalog_schema_table(client) -> dict:
    """Create a fresh catalog + schema ``s`` + table ``t`` on main; return the
    ids (all random-minted server-side, CHA-236)."""
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"cha380_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="cha380"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha380",
    )
    return {
        "catalog_uuid": catalog_uuid,
        "main_branch_uuid": main_branch_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }


def _one_row(upsert_tbl: str, columns: list[str], name_column: str, name_value: str):
    """Read ``columns`` off the single ``upsert_tbl`` row whose ``name_column``
    equals ``name_value`` (the auditable-store upsert log holds one live version
    per entity within the genesis tx). UUID columns come back as strings via the
    helper pool's ``_UUIDStringLoader``. Column names are ``Identifier``-quoted
    (psycopg forbids interpolating a runtime ``str`` into ``SQL``)."""
    rows = get_pg_driver().execute(
        SQL("SELECT {cols} FROM {tbl} WHERE {name_col} = %s").format(
            cols=SQL(", ").join(Identifier(column) for column in columns),
            tbl=Identifier(upsert_tbl),
            name_col=Identifier(name_column),
        ),
        (name_value,),
    )
    assert len(rows) == 1, (
        f"expected exactly one {upsert_tbl} row with {name_column}={name_value!r}, "
        f"got {len(rows)}"
    )
    return rows[0]


class TestSystemTablesPkColumn:
    """``__penca_system__.tables`` rows carry a distinct ``table_uuid`` PK column
    and a canonically-derived ``row_uuid``."""

    def test_tables_row_uuid_is_canonical_hash_of_table_uuid(self):
        """``row_uuid`` on the ``.tables`` row equals
        ``row_uuid_for_pk(system_tables_table_uuid(catalog), [table_uuid])``, NOT
        the raw ``table_uuid``.

        Red: today ``row_uuid == table_uuid``, so both assertions fail.
        """
        client = make_client()
        try:
            ctx = _fresh_catalog_schema_table(client)
            tables_log = upsert_log_table(
                system_tables_table_uuid(ctx["catalog_uuid"]),
                ctx["main_branch_uuid"],
            )
            (row_uuid,) = _one_row(tables_log, ["row_uuid"], "table_name", "t")

            expected = row_uuid_for_pk(
                system_tables_table_uuid(ctx["catalog_uuid"]), [ctx["table_uuid"]]
            )
            assert row_uuid == expected, (
                "the .tables row_uuid must be the canonical "
                "row_uuid_for_pk(system_tables_table_uuid, [table_uuid]); "
                f"got {row_uuid}, expected {expected}"
            )
            assert row_uuid != ctx["table_uuid"], (
                "row_uuid must no longer overload the raw table_uuid (ADR 0013)"
            )
        finally:
            client.close()

    def test_tables_upsert_log_exposes_table_uuid_column(self):
        """The ``.tables`` upsert log carries a distinct ``table_uuid`` column
        equal to the table's own uuid.

        Red: no ``table_uuid`` column exists → ``UndefinedColumn``.
        """
        client = make_client()
        try:
            ctx = _fresh_catalog_schema_table(client)
            tables_log = upsert_log_table(
                system_tables_table_uuid(ctx["catalog_uuid"]),
                ctx["main_branch_uuid"],
            )
            (table_uuid_col,) = _one_row(tables_log, ["table_uuid"], "table_name", "t")
            assert table_uuid_col == ctx["table_uuid"], (
                "the .tables row's table_uuid column must equal the table's uuid"
            )
        finally:
            client.close()


class TestSystemSchemasPkColumn:
    """``__penca_system__.schemas`` rows carry a distinct ``schema_uuid`` PK
    column and a canonically-derived ``row_uuid``."""

    def test_schemas_row_uuid_and_schema_uuid_column(self):
        """The ``.schemas`` row for ``s`` exposes a ``schema_uuid`` column equal
        to the schema's uuid, and ``row_uuid ==
        row_uuid_for_pk(system_schemas_table_uuid(catalog), [schema_uuid])``.

        Red: no ``schema_uuid`` column → ``UndefinedColumn`` on the SELECT.
        """
        client = make_client()
        try:
            ctx = _fresh_catalog_schema_table(client)
            schemas_log = upsert_log_table(
                system_schemas_table_uuid(ctx["catalog_uuid"]),
                ctx["main_branch_uuid"],
            )
            row_uuid, schema_uuid_col = _one_row(
                schemas_log, ["row_uuid", "schema_uuid"], "schema_name", "s"
            )
            assert schema_uuid_col == ctx["schema_uuid"], (
                "the .schemas row's schema_uuid column must equal the schema's uuid"
            )
            expected = row_uuid_for_pk(
                system_schemas_table_uuid(ctx["catalog_uuid"]), [ctx["schema_uuid"]]
            )
            assert row_uuid == expected, (
                "the .schemas row_uuid must be the canonical "
                "row_uuid_for_pk(system_schemas_table_uuid, [schema_uuid]); "
                f"got {row_uuid}, expected {expected}"
            )
            assert row_uuid != ctx["schema_uuid"], (
                "row_uuid must no longer overload the raw schema_uuid (ADR 0013)"
            )
        finally:
            client.close()


class TestSystemIndexesPkColumn:
    """``__penca_system__.indexes`` rows carry a distinct ``index_uuid`` PK
    column (separate from the parent ``table_uuid`` it already has) and a
    canonically-derived ``row_uuid``."""

    def test_indexes_row_uuid_and_index_uuid_column(self):
        """After ``create_index``, the ``.indexes`` row exposes an ``index_uuid``
        column equal to the index's uuid, and ``row_uuid ==
        row_uuid_for_pk(system_indexes_table_uuid(catalog), [index_uuid])``.

        Red: no ``index_uuid`` column → ``UndefinedColumn`` on the SELECT.
        """
        client = make_client()
        try:
            ctx = _fresh_catalog_schema_table(client)
            index_uuid = client.create_index(
                index_name="idx_name",
                columns=["name"],
                index_type=SCALAR_BTREE,
                table_uuid=ctx["table_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                author="test",
                comment="cha380",
            )
            indexes_log = upsert_log_table(
                system_indexes_table_uuid(ctx["catalog_uuid"]),
                ctx["main_branch_uuid"],
            )
            row_uuid, index_uuid_col = _one_row(
                indexes_log, ["row_uuid", "index_uuid"], "index_name", "idx_name"
            )
            assert index_uuid_col == index_uuid, (
                "the .indexes row's index_uuid column must equal the index's uuid"
            )
            expected = row_uuid_for_pk(
                system_indexes_table_uuid(ctx["catalog_uuid"]), [index_uuid]
            )
            assert row_uuid == expected, (
                "the .indexes row_uuid must be the canonical "
                "row_uuid_for_pk(system_indexes_table_uuid, [index_uuid]); "
                f"got {row_uuid}, expected {expected}"
            )
            assert row_uuid != index_uuid, (
                "row_uuid must no longer overload the raw index_uuid (ADR 0013)"
            )
        finally:
            client.close()
