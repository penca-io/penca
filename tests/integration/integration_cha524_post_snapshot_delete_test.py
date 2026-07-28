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
import pytest
from penca_client import Mutation

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


def _rows(table):
    """Sorted ``(name, value)`` pairs.

    Order-independent because the merge SQL carries no final ORDER BY — PK
    order is a snapshot-layout side effect, not a read contract. A *list*, not
    a dict: folding to `{pk: value}` would also discard cardinality, and a row
    emitted twice (once from the snapshot stream, once from the resolved delta,
    when the exclusion set misses its ``row_uuid``) is the characteristic
    failure of the very merge path under test.
    """
    return sorted(zip(*table.to_pydict().values(), strict=True))


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


# The UUID args are keyword-only: three same-typed strings in an order that
# differs from PencaClient's own (catalog, schema, branch) would transpose
# silently into an opaque server-side resolve error.
def _commit(client, *, catalog_uuid, branch_uuid, schema_uuid, mutation):
    """Open a tx, apply one mutation, commit."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    client.write_data(
        tx.tx_uuid,
        mutation,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def _delete_t2_after_snapshot(client, *, catalog_uuid, branch_uuid, schema_uuid):
    """Snapshot main, then drop ``t2`` — the tombstone lands above the fence."""
    client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )
    client.delete_table(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="t2",
        author="test",
        comment="CHA-524",
    )


def test_list_tables_survives_post_snapshot_delete_table():
    """ListTables must still resolve after a table is dropped post-snapshot."""
    client = make_client()
    catalog_uuid, main_branch, schema_uuid, _ = _seed_catalog(client, ["t1", "t2"])
    _delete_t2_after_snapshot(
        client,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch,
        schema_uuid=schema_uuid,
    )

    def surviving():
        return [
            t.table_name
            for t in client.list_tables(
                catalog_uuid=catalog_uuid, schema_uuid=schema_uuid
            )
        ]

    assert surviving() == ["t1"], "tombstone above the fence"

    # Snapshot is itself a cold merge-on-read, so this flush — not the re-read —
    # is what drives the resolve over the tombstone; it would fail loudly if the
    # carrier schema regressed. It also compacts the tombstone away (snapshot
    # segments carry no `is_delete`), so the re-read pins the end state a real
    # deployment reaches within one scheduler tick.
    client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid, branch_uuid=main_branch
    )

    assert surviving() == ["t1"], "compacted end state"


@pytest.mark.parametrize(
    "flush_tombstone", [False, True], ids=["above-fence", "flushed"]
)
def test_delete_catalog_survives_post_snapshot_delete_table(flush_tombstone):
    """DeleteCatalog must reap the catalog, not wedge on its own tombstones.

    The ticket's headline symptom: every retry failed identically, so the
    catalog and its cold objects could never be reaped.

    Both states matter and neither subsumes the other. ``above-fence`` is the
    state the ticket reports — the tombstone still in the hot log — and is the
    only one that puts the NULL-carrying row on DeleteCatalog's own read path.
    ``flushed`` is where an operator retrying a long-wedged catalog actually
    finds it, after the scheduler has compacted the tombstone away.
    """
    client = make_client()
    catalog_uuid, main_branch, schema_uuid, _ = _seed_catalog(client, ["t1", "t2"])
    _delete_t2_after_snapshot(
        client,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch,
        schema_uuid=schema_uuid,
    )
    if flush_tombstone:
        client.persist_and_snapshot_branch(
            catalog_uuid=catalog_uuid, branch_uuid=main_branch
        )

    client.delete_catalog(catalog_uuid=catalog_uuid)

    remaining = [c.catalog_uuid for c in client.list_catalogs()]
    assert catalog_uuid not in remaining, "catalog survived DeleteCatalog"


def test_read_data_survives_post_snapshot_row_delete_on_non_nullable_column():
    """The same defect on a USER table — nothing about it is system-specific.

    ``__penca_system__.*`` only bit first because Penca declares those columns
    non-nullable; a client that does the same on its own table is affected
    identically.
    """
    client = make_client()
    catalog_uuid, main_branch, schema_uuid, (table_uuid,) = _seed_catalog(
        client, ["t1"]
    )
    _commit(
        client,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch,
        schema_uuid=schema_uuid,
        mutation=Mutation(
            table_uuid=table_uuid,
            upserts=pa.table(
                {"name": ["a", "b", "c"], "value": [1, 2, 3]}, schema=STRICT_SCHEMA
            ),
        ),
    )

    def delete_row(name):
        _commit(
            client,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch,
            schema_uuid=schema_uuid,
            mutation=Mutation(
                table_uuid=table_uuid,
                # Derived, not re-declared: a drifting pk type would otherwise
                # fail the delete on a type mismatch instead of exercising this.
                deletes=pa.table(
                    {"name": [name]}, schema=pa.schema([STRICT_SCHEMA.field("name")])
                ),
            ),
        )

    def survivors():
        return client.read_data(
            table_uuid=table_uuid,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch,
        )

    client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid, branch_uuid=main_branch
    )
    delete_row("a")

    # Assert the whole row, not just the pk: the defect is non-key user columns
    # arriving NULL, so a fix that kept the survivors with a NULL `value` must
    # fail here. The schema assertion pins the other half — the strict output
    # contract must survive, since relaxing it too would have turned this bug
    # into silent corruption rather than a loud one.
    # Order-independent: the merge SQL carries no final ORDER BY, so row order
    # is a snapshot-layout side effect (PK-sorted segments), not a read contract.
    first = survivors()
    assert _rows(first) == [("b", 2), ("c", 3)]
    assert first.schema == STRICT_SCHEMA, (
        "read_data must keep the declared non-nullability"
    )

    # Round 2 adds a second snapshot generation: `c` survives having been
    # carried forward through the prior compaction, and the new tombstone is
    # resolved against that carried-forward segment rather than a first-
    # generation one. (The tier configuration is the same as round 1 — the
    # seed upsert was already cold-only once the first snapshot fenced it.)
    client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid, branch_uuid=main_branch
    )
    delete_row("b")

    second = survivors()
    assert _rows(second) == [("c", 3)]
    assert second.schema == STRICT_SCHEMA
