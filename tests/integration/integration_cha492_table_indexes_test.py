"""CHA-492 — `Table.indexes` is populated in GetTable / ListTables.

The structured-seek transport needs the reader to know a table's DEFINED
indexes without a second round-trip, so `GetTable` / `ListTables` carry them on
`Table.indexes` (reusing the `Index` message: index_uuid / index_name /
columns / index_type). A composite index pins that the key columns come back in
declared order.

Fail-first: the `Table` proto (and the `TableInfo` client model) has no
`indexes` field, so `table_info.indexes` raises `AttributeError`.

Scoped run:  just integration-test cha492_table_indexes
"""

from __future__ import annotations

from .integration_helpers import (
    SCALAR_BTREE,
    make_client,
    setup_schema,
)


def _index_by_name(indexes, name: str):
    matches = [ix for ix in indexes if ix.index_name == name]
    assert matches, f"index {name!r} not present in Table.indexes: {list(indexes)}"
    return matches[0]


class TestTableIndexesPopulated:
    def test_get_table_and_list_tables_carry_defined_indexes(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # Composite index so column ORDER is observable end-to-end.
        index_uuid = client.create_index(
            table_name="write_table",
            index_name="idx_value_name",
            columns=["value", "name"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cha-492",
        )
        assert index_uuid

        got = client.get_table(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        idx = _index_by_name(got.indexes, "idx_value_name")
        assert idx.index_uuid == index_uuid
        assert list(idx.columns) == ["value", "name"]
        assert idx.index_type == SCALAR_BTREE

        # Same via ListTables.
        listed = list(
            client.list_tables(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
            )
        )
        table = next(t for t in listed if t.table_uuid == table_uuid)
        listed_idx = _index_by_name(table.indexes, "idx_value_name")
        assert listed_idx.index_uuid == index_uuid
        assert list(listed_idx.columns) == ["value", "name"]
