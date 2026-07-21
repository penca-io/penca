"""Integration tests for index DDL (CHA-455).

Index definitions live in the auditable ``__penca_system__.indexes``
store and are exercised through the WriteService (Create/Update/Delete)
and QueryService (Get/List) surfaces, mirroring table DDL. Run via
``just integration-test query lifecycle``.

These are red-phase acceptance tests: before the CHA-455 implementation
lands they fail at the first index call. ``TestIndexDdlRoundTrip`` fails
with ``AttributeError`` (no ``client.create_index``);
``TestInlineCreateTableIndexes`` fails with ``TypeError`` on the
not-yet-added ``indexes=`` kwarg of ``create_table``.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from penca_client.errors import (
    AlreadyExistsError,
    NotFoundError,
)

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    make_client,
    setup_schema,
)


class TestIndexDdlRoundTrip:
    """CreateIndex -> GetIndex/ListIndex -> UpdateIndex -> DeleteIndex,
    plus time-travel + open-tx read-your-own-writes."""

    def test_create_get_list_roundtrip(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        index_uuid = client.create_index(
            table_name="write_table",
            index_name="idx_name",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_index",
        )
        assert index_uuid

        # get by uuid and by name both resolve the definition; column
        # order + index_type are echoed verbatim.
        by_uuid = client.get_index(
            index_uuid=index_uuid,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        by_name = client.get_index(
            index_name="idx_name",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        for idx in (by_uuid, by_name):
            assert idx.index_uuid == index_uuid
            assert idx.index_name == "idx_name"
            assert list(idx.columns) == ["name"]
            assert idx.index_type == SCALAR_BTREE
            assert idx.table_uuid == table_uuid

        listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert [i.index_name for i in listed] == ["idx_name"]

    def test_index_name_unique_only_within_table(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        client.create_index(
            table_name="write_table",
            index_name="dup",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="first",
        )
        # Same name on the SAME table -> AlreadyExists.
        with pytest.raises(AlreadyExistsError):
            client.create_index(
                table_name="write_table",
                index_name="dup",
                columns=["value"],
                index_type=SCALAR_BTREE,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="test",
                comment="dup",
            )

        # Same name on a DIFFERENT table -> OK (unique only within table).
        other_table = client.create_table(
            "other_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="other",
        )
        ok = client.create_index(
            table_name="other_table",
            index_name="dup",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="dup-other",
        )
        assert ok
        # Confirm it actually resolves on the other table (per-table
        # scoping), not just that the create returned a truthy uuid.
        other_listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=other_table,
        )
        assert "dup" in [i.index_name for i in other_listed]

    def test_update_index_rename_only(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        client.create_index(
            table_name="write_table",
            index_name="old_name",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create",
        )
        client.update_index(
            table_name="write_table",
            index_name="old_name",
            new_index_name="new_name",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="rename",
        )
        renamed = client.get_index(
            index_name="new_name",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert renamed.index_name == "new_name"
        with pytest.raises(NotFoundError):
            client.get_index(
                index_name="old_name",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )

    def test_delete_index(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        client.create_index(
            table_name="write_table",
            index_name="to_drop",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create",
        )
        client.delete_index(
            table_name="write_table",
            index_name="to_drop",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="drop",
        )
        with pytest.raises(NotFoundError):
            client.get_index(
                index_name="to_drop",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )

        listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert [i.index_name for i in listed] == []

    def test_time_travel_resolves_dropped_index(self):
        """as_of_micros pinned before the drop still resolves the index
        (auditable history)."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        # Create inside an explicit tx so we can pin as_of to the create's
        # commit time (the canonical pattern — commit_tx returns
        # commit_micros; the read Index has no commit-time field).
        tx = client.begin_tx(catalog_uuid=catalog_uuid)
        client.create_index(
            table_name="write_table",
            index_name="ephemeral",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            tx_uuid=tx.tx_uuid,
        )
        committed = client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid)
        as_of = committed.commit_micros

        client.delete_index(
            table_name="write_table",
            index_name="ephemeral",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="drop",
        )
        # Live read: gone.
        with pytest.raises(NotFoundError):
            client.get_index(
                index_name="ephemeral",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )

        # Time-travel read pinned before the drop: still resolves.
        historical = client.get_index(
            index_name="ephemeral",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            as_of_micros=as_of,
        )
        assert historical.index_name == "ephemeral"

    def test_open_tx_read_your_own_writes(self):
        """An index created inside an open tx is visible to that tx's
        reads (RYOW) and invisible to others until commit."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)

        tx = client.begin_tx(catalog_uuid=catalog_uuid)
        client.create_index(
            table_name="write_table",
            index_name="in_tx",
            columns=["name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            tx_uuid=tx.tx_uuid,
        )
        # RYOW: visible to the open tx.
        ryow = client.get_index(
            index_name="in_tx",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            open_tx_uuid=tx.tx_uuid,
        )
        assert ryow.index_name == "in_tx"
        # Invisible to a default (committed) read before commit.
        with pytest.raises(NotFoundError):
            client.get_index(
                index_name="in_tx",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )

        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid)
        committed = client.get_index(
            index_name="in_tx",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert committed.index_name == "in_tx"

    def test_update_index_rename_onto_existing_rejected(self):
        """Renaming onto a name held by a different index on the same
        table is rejected (no two rows sharing (table, name)); a no-op
        rename to the current name is allowed."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, _branch = setup_schema(client)
        for name in ("idx_a", "idx_b"):
            client.create_index(
                table_name="write_table",
                index_name=name,
                columns=["name"],
                index_type=SCALAR_BTREE,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="test",
                comment="create",
            )

        with pytest.raises(AlreadyExistsError):
            client.update_index(
                table_name="write_table",
                index_name="idx_a",
                new_index_name="idx_b",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="test",
                comment="rename-clash",
            )

        # No-op rename onto the current name is allowed.
        client.update_index(
            table_name="write_table",
            index_name="idx_a",
            new_index_name="idx_a",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="rename-noop",
        )
        assert (
            client.get_index(
                index_name="idx_a",
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            ).index_name
            == "idx_a"
        )


class TestInlineCreateTableIndexes:
    """Inline ``CreateTable.indexes`` materializes index definitions in
    the same tx as the table create (CHA-455)."""

    def test_inline_indexes_listed(self):
        client = make_client()
        catalog_uuid, _main = client.create_catalog(
            f"idx_inline_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="inline",
        )
        table_uuid = client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="inline",
            indexes=[
                {
                    "index_name": "idx_name",
                    "columns": ["name"],
                    "index_type": SCALAR_BTREE,
                },
                {
                    "index_name": "idx_value",
                    "columns": ["value"],
                    "index_type": SCALAR_BTREE,
                },
            ],
        )
        listed = client.list_indexes(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
        )
        assert sorted(i.index_name for i in listed) == ["idx_name", "idx_value"]
        for name in ("idx_name", "idx_value"):
            got = client.get_index(
                index_name=name,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
            )
            assert got.index_name == name
