"""CHA-531 red test: the cold-segment refcount gate must be
catalog-scoped, not branch-scoped.

Once a fork carries a parent segment by reference, the child's
referencing row lives in the CHILD's partition. ``eligible_segment_
delete_set_rows`` probes both of its ``NOT EXISTS`` refcount arms with
``branch_uuid = $1``, so the parent's sweep cannot see the child's
reference and deletes a file the child still reads.

The cross-branch reference is synthesized with direct SQL rather than
produced by a real fork snapshot: this pins the GC gate on its own,
independent of the carry-forward writer landing. Retirement itself is
disabled by default (CHA-468 — nothing calls ``retire_snapshots``), so
the enqueue and the reference-drop are simulated the same way
``integration_snapshot_gc_test.py`` does.

Run via ``just integration-test branch_fork_gc_refcount``.
"""

from __future__ import annotations

import time
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    SEGMENT_DELETE_SET,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    table_snapshot_uuid,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)

# ── Helpers ───────────────────────────────────────────────────────────


def _now_micros():
    return int(time.time() * 1_000_000)


def _make_env():
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"fgc_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "fgc_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-531"
    )
    table_uuid = client.create_table(
        "fgc_table",
        USER_SCHEMA,
        primary_keys=["name"],
        partition_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha-531",
    )
    return client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid


def _cycle(client, *, catalog_uuid, schema_uuid, table_uuid, branch_uuid, upserts):
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=upserts),
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
    response = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    assert response.HasField("snapshotted_at_micros")
    return table_snapshot_uuid(
        catalog_uuid, branch_uuid, table_uuid, response.snapshotted_at_micros
    )


def _column_names(table_name):
    """Live column list for a catalog-suffixed table.

    Copying a segment row across branches by name-list (rather than
    spelling the storage columns out) keeps this test from drifting
    every time the segment schema gains a column.
    """
    rows = get_pg_driver().execute(
        SQL(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = %s ORDER BY ordinal_position"
        ),
        (table_name,),
    )
    return [r[0] for r in rows]


def _copy_row_to_branch(table_name, *, where, params, overrides):
    """``INSERT INTO t (cols) SELECT cols-with-overrides FROM t WHERE ...``

    ``overrides`` maps column name → literal value, letting the copy land
    under a different branch / snapshot / segment uuid while every other
    column is carried verbatim — exactly what a carried segment row is.
    """
    cols = _column_names(table_name)
    select_items = []
    values = []
    for col in cols:
        if col in overrides:
            select_items.append(SQL("%s"))
            values.append(overrides[col])
        else:
            select_items.append(Identifier(col))

    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl} ({cols}) SELECT {vals} FROM {tbl} WHERE " + where
        ).format(
            tbl=Identifier(table_name),
            cols=SQL(", ").join(Identifier(c) for c in cols),
            vals=SQL(", ").join(select_items),
        ),
        (*values, *params),
    )


def _segment_rows(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(table_snapshot_segment_uuid, object_uri), ...]``.

    Rows, not distinct uris: small partitions pack into ONE file, so a
    single ``object_uri`` backs several segment rows. The copy below
    must target one specific row.
    """
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT table_snapshot_segment_uuid::text, object_uri FROM {tbl}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ' ORDER BY chunk_idx, "offset"'
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )
    return [(r[0], r[1]) for r in rows]


def _segment_uris(catalog_uuid, branch_uuid, snapshot_uuid):
    return sorted(
        {uri for _uuid, uri in _segment_rows(catalog_uuid, branch_uuid, snapshot_uuid)}
    )


def _delete_set_rows_for_uris(catalog_uuid, branch_uuid, uris):
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    return get_pg_driver().execute(
        SQL(
            "SELECT object_uri, written_at_micros FROM {tbl}"
            " WHERE branch_uuid = %s AND object_uri = ANY(%s)"
        ).format(tbl=Identifier(tbl)),
        (branch_uuid, list(uris)),
    )


def _insert_delete_set_row(catalog_uuid, branch_uuid, table_uuid, uri, written_at):
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl}"
            " (segment_delete_uuid, branch_uuid, table_uuid, object_uri,"
            "  written_at_micros)"
            " VALUES (%s, %s, %s, %s, %s)"
        ).format(tbl=Identifier(tbl)),
        (str(uuid4()), branch_uuid, table_uuid, uri, written_at),
    )


def _drop_snapshot_rows(catalog_uuid, branch_uuid, snapshot_uuid):
    """Drop a snapshot's segment + parent rows, as retirement would."""
    for name in (TABLE_SNAPSHOT_SEGMENT_METADATA, TABLE_SNAPSHOT_METADATA):
        get_pg_driver().execute_no_result(
            SQL(
                "DELETE FROM {tbl} WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(f"{catalog_uuid}_{name}")),
            (branch_uuid, snapshot_uuid),
        )


# ── Tests ─────────────────────────────────────────────────────────────


def test_parent_retire_does_not_delete_child_referenced_segment():
    """A cold file still referenced by ANOTHER branch's snapshot
    metadata must survive the owning branch's sweep.

    Sequence: parent snapshots; a child branch takes a carried reference
    to one of the parent's files; the parent retires the snapshot that
    produced it and enqueues the file past its grace window. Sweeping
    the parent must leave the file alone — its reference count is one,
    not zero, and the count is a CATALOG-wide property.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = _make_env()

    snap_main = _cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
            schema=USER_SCHEMA,
        ),
    )
    parent_rows = _segment_rows(catalog_uuid, main_branch, snap_main)
    assert parent_rows, (
        "the parent snapshot must have written at least one segment file"
    )
    shared_seg_uuid, shared_uri = parent_rows[0]

    child_branch = client.create_branch(
        f"fgc_child_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    # Synthesize the child's carried reference: same object_uri, new
    # snapshot + segment identity, under the child's branch_uuid. This
    # is the row shape CHA-531's carry-forward writer produces.
    child_snap = str(uuid4())
    seg_table = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    snap_table = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    _copy_row_to_branch(
        snap_table,
        where="branch_uuid = %s AND table_snapshot_uuid = %s",
        params=(main_branch, snap_main),
        overrides={"branch_uuid": child_branch, "table_snapshot_uuid": child_snap},
    )
    _copy_row_to_branch(
        seg_table,
        where="branch_uuid = %s AND table_snapshot_segment_uuid = %s",
        params=(main_branch, shared_seg_uuid),
        overrides={
            "branch_uuid": child_branch,
            "table_snapshot_uuid": child_snap,
            "table_snapshot_segment_uuid": str(uuid4()),
        },
    )
    child_refs = _segment_uris(catalog_uuid, child_branch, child_snap)
    assert shared_uri in child_refs, (
        "setup failed: the child must hold a reference to the parent's file"
    )

    # The parent retires the snapshot that produced the file and
    # enqueues it for GC, already past the grace window.
    _insert_delete_set_row(
        catalog_uuid, main_branch, table_uuid, shared_uri, _now_micros() - 10_000_000
    )
    _drop_snapshot_rows(catalog_uuid, main_branch, snap_main)

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    remaining = _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri])
    assert len(remaining) == 1, (
        "sweep deleted a cold file that another branch's snapshot still"
        " references. The refcount gate is catalog-wide, not branch-scoped:"
        f" {shared_uri} is referenced by branch {child_branch}, but"
        " eligible_segment_delete_set_rows probes its NOT EXISTS arms with"
        " branch_uuid = $1 and cannot see it."
    )
