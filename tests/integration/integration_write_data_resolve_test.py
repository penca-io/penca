"""Integration tests for the unified single-table ``write_data`` resolution (CHA-475).

Exercises the post-Step-0 single-table ``write_data`` gRPC API + the unified
``ResolvedScope`` write resolution: a write by ``table_uuid`` no longer requires
a schema identifier in the request (the eager ``__penca_system__.schemas``
resolve is gone), while a write by ``table_name`` resolves the schema once. Both
snapshot arms are covered (auto-commit default-frontier, explicit-tx OpenTx),
plus the preserved cross-schema-by-uuid (CHA-381) and system-table-rejection
invariants.

Run via ``just integration-test``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import InvalidRequestError
from penca_client.naming import (
    system_indexes_table_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
)

from .integration_helpers import USER_SCHEMA, make_client, setup_schema


class TestWriteDataResolution:
    def test_write_data_by_uuid_no_schema_autocommit(self):
        """RT-1 (headline): an auto-commit write by ``table_uuid`` with the
        request carrying ONLY catalog + branch + table_uuid (NO schema
        identifier) succeeds and the row is queryable. Pre-CHA-475 the eager
        ``resolve_with_schema`` rejected a schema-less request with
        "must provide schema_uuid or schema_name"."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        batch = pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA)
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
            author="t",
            comment="schema-less by-uuid write",
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [1]

    def test_write_data_by_name_autocommit(self):
        """RT-2: an auto-commit write by ``table_name`` at the request level —
        the default-frontier snapshot arm, by-name path resolves the schema
        once before ``resolve_table_by_name``."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        batch = pa.table({"name": ["bob"], "value": [2]}, schema=USER_SCHEMA)
        client.write_data(
            None,
            Mutation(table_name="write_table", upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="t",
            comment="by-name write",
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert result.column("name").to_pylist() == ["bob"]

    def test_write_data_by_name_in_open_tx(self):
        """RT-5: a write by ``table_name`` inside an explicit open tx — the
        OpenTx snapshot arm of the boundary resolution (distinct from the
        auto-commit default-frontier arm in RT-2). Commit, then read."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table({"name": ["carol"], "value": [3]}, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_name="write_table", upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert result.column("name").to_pylist() == ["carol"]

    def test_write_data_cross_schema_by_uuid(self):
        """RT-3 (CHA-381 preserved): a by-uuid write resolves the table
        catalog-wide, so a request whose schema identifier names a DIFFERENT
        schema than the table's residence still writes — the schema is ignored
        on the by-uuid path."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        other_schema_uuid = client.create_schema(
            "other_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="second schema",
        )
        assert other_schema_uuid != schema_uuid
        batch = pa.table({"name": ["dave"], "value": [4]}, schema=USER_SCHEMA)
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=other_schema_uuid,  # deliberately the WRONG schema
            branch_uuid=main_branch_uuid,
            author="t",
            comment="cross-schema by-uuid",
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert result.column("name").to_pylist() == ["dave"]

    def test_write_data_rejects_system_table(self):
        """RT-4: a write targeting a registered system table by table_uuid is
        rejected on the schema-less by-uuid path — ``assert_not_system_table``
        covers ``__penca_system__.{schemas,tables,indexes}``."""
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        batch = pa.table({"name": ["x"], "value": [0]}, schema=USER_SCHEMA)
        for system_uuid in (
            system_schemas_table_uuid(catalog_uuid),
            system_tables_table_uuid(catalog_uuid),
            system_indexes_table_uuid(catalog_uuid),
        ):
            with pytest.raises(InvalidRequestError):
                client.write_data(
                    None,
                    Mutation(table_uuid=str(system_uuid), upserts=batch),
                    catalog_uuid=catalog_uuid,
                    branch_uuid=main_branch_uuid,
                    author="t",
                    comment="system reject",
                )


class TestDdlScopeResolution:
    """CHA-479 wrinkle #2: the DDL write handlers resolve a by-``table_uuid``
    target catalog-wide (true residency, CHA-381) — the same dispatch
    ``write_data`` already uses (see ``test_write_data_cross_schema_by_uuid``).
    ``update_table`` by uuid with a request ``schema_uuid`` naming a DIFFERENT
    (user, non-system) schema than the table's residence resolves the table by
    its real schema and succeeds; the rename lands under the true schema.

    Pre-CHA-479 the DDL path opened ``WriteRequestScope::resolve_with_target_table``,
    which set ``scope.schema_uuid`` to the *request's* schema; ``update_table``'s
    in-tx ``meta_get_table`` existence check is schema-scoped (an ``l.schema_uuid``
    filter, ``query/meta_resolve.rs``), so the mismatched-schema call raised
    NOT_FOUND.

    ``update_table`` is the red case: its in-tx existence check WAS schema-scoped,
    so a mismatched-schema by-uuid call returned NOT_FOUND pre-CHA-479 and now
    succeeds. ``delete_table`` and the index handlers go through the SAME shared
    ``validate_write_target_table`` + ``resolve_table`` path, but their existence
    checks key on ``table_uuid`` alone (``delete_table_metadata_if_visible`` /
    ``meta_get_index``), so the mismatched-USER-schema case is green-from-start for
    them — they succeed before and after CHA-479. The two green-from-start guards
    below pin that the shared path resolves by true residency across handlers, not
    only ``update_table`` (roborev finding on the IMPL-3 commit).
    """

    def test_update_table_by_uuid_resolves_true_residency_not_request_schema(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        other_schema_uuid = client.create_schema(
            "other_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="second user schema (CHA-479 wrinkle-2)",
        )
        assert other_schema_uuid != schema_uuid
        # Rename T (resident in schema A) via a by-uuid request carrying the
        # WRONG schema B. Post-CHA-479 the schema is derived from the resolved
        # table row, so the rename resolves + lands under A. Pre-CHA-479 the
        # schema-scoped existence check looked for T under B -> NOT_FOUND.
        updated = client.update_table(
            USER_SCHEMA,
            catalog_uuid=catalog_uuid,
            schema_uuid=other_schema_uuid,  # deliberately the WRONG schema
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            new_table_name="renamed_by_uuid",
            author="t",
            comment="cross-schema by-uuid update",
        )
        assert updated.table_uuid == table_uuid
        assert updated.table_name == "renamed_by_uuid"
        assert updated.schema_uuid == schema_uuid  # true residency (A), not B

    def test_delete_table_by_uuid_with_mismatched_schema_succeeds(self):
        # Green-from-start guard: delete_table's existence check keys on
        # table_uuid, so a by-uuid delete with a mismatched user schema resolves
        # the table by true residency and succeeds before and after CHA-479.
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        other_schema_uuid = client.create_schema(
            "other_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="second user schema (CHA-479)",
        )
        assert other_schema_uuid != schema_uuid
        deleted_uuid = client.delete_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=other_schema_uuid,  # deliberately the WRONG (user) schema
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            author="t",
            comment="cross-schema by-uuid delete",
        )
        assert deleted_uuid == table_uuid

    def test_create_index_by_uuid_with_mismatched_schema_succeeds(self):
        # Green-from-start guard: create_index targets the owning table by
        # table_uuid (its existence check keys on the table), so a by-uuid index
        # create with a mismatched user schema resolves the table by true
        # residency and succeeds before and after CHA-479.
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        other_schema_uuid = client.create_schema(
            "other_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="second user schema (CHA-479)",
        )
        assert other_schema_uuid != schema_uuid
        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_by_uuid",
            columns=["name"],
            index_type=1,  # IndexType.INDEX_TYPE_SCALAR_BTREE
            catalog_uuid=catalog_uuid,
            schema_uuid=other_schema_uuid,  # deliberately the WRONG (user) schema
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cross-schema by-uuid create_index",
        )
        assert index_uuid
