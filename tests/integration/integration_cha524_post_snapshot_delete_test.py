"""Integration tests for CHA-524 — reads after a post-snapshot delete.

Snapshot advances the hot read fence ``max(Pu, W_snap)`` (ADR 0027), so a delete
committed afterwards leaves the merge's ``deletes`` arm populated while its
``latest`` arm no longer carries the row's original upsert. The resolve's
tombstone arm then emits user columns as NULL — which a table declaring any
column non-nullable used to reject outright, wedging every subsequent read.

``__penca_system__.*`` is where this bit first (``PgDialect::system_*_arrow_schema``
declares its columns non-nullable), but it is not system-specific: any
client-declared ``nullable=False`` column hits the same path, which is why
``STRICT_SCHEMA`` below is non-nullable and the user-table case is covered too.

Run via ``just integration-test cha524_post_snapshot_delete``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa

from .integration_helpers import make_client

# Every other integration suite declares an all-nullable user schema, which is
# exactly why this defect went unnoticed — the merge only rejects NULL tombstone
# columns when the table declares them non-nullable.
STRICT_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ]
)


def _seed_catalog(client, table_names):
    """Create a catalog + schema + ``table_names`` on main, all `STRICT_SCHEMA`."""
    catalog_uuid, main_branch = client.create_catalog(
        f"cha524_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s1", catalog_uuid=catalog_uuid, author="test", comment="CHA-524"
    )
    table_uuids = [
        client.create_table(
            name,
            STRICT_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="CHA-524",
        )
        for name in table_names
    ]

    return catalog_uuid, main_branch, schema_uuid, table_uuids


def _delete_t2_after_snapshot(client, catalog_uuid, main_branch, schema_uuid):
    """Snapshot main, then drop ``t2`` — the tombstone lands above the fence."""
    client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid, branch_uuid=main_branch
    )
    client.delete_table(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch,
        table_name="t2",
        author="test",
        comment="CHA-524",
    )


def test_list_tables_survives_post_snapshot_delete_table():
    """ListTables must still resolve after a table is dropped post-snapshot."""
    client = make_client()
    catalog_uuid, main_branch, schema_uuid, _ = _seed_catalog(client, ["t1", "t2"])
    _delete_t2_after_snapshot(client, catalog_uuid, main_branch, schema_uuid)

    names = [
        t.table_name
        for t in client.list_tables(catalog_uuid=catalog_uuid, schema_uuid=schema_uuid)
    ]

    assert names == ["t1"], f"expected only the surviving table, got {names}"


def test_delete_catalog_survives_post_snapshot_delete_table():
    """DeleteCatalog must reap the catalog, not wedge on its own tombstones.

    The ticket's headline symptom: every retry failed identically, so the
    catalog and its cold objects could never be reaped.
    """
    client = make_client()
    catalog_uuid, main_branch, schema_uuid, _ = _seed_catalog(client, ["t1", "t2"])
    _delete_t2_after_snapshot(client, catalog_uuid, main_branch, schema_uuid)

    client.delete_catalog(catalog_uuid=catalog_uuid)

    remaining = [c.catalog_uuid for c in client.list_catalogs()]
    assert catalog_uuid not in remaining, "catalog survived DeleteCatalog"
