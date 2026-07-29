"""CHA-531 red test: the cold-segment refcount gate must be
catalog-scoped, not branch-scoped.

Once a fork carries a parent segment by reference, the child's
referencing row lives in the CHILD's partition. ``eligible_segment_
delete_set_rows`` probes both of its ``NOT EXISTS`` refcount arms with
``branch_uuid = $1``, so the parent's sweep cannot see the child's
reference and deletes a file the child still reads.

One test per arm the gate has to get right: the base-segment refcount,
the index-sidecar refcount (a carried sidecar shares its file the same
way), and the grace clock (branch-keyed delete-set rows mean the
retirement that drops the last reference refreshes only its own branch's
row). Each pairs its survival assertion with a positive control, because
"the row is still there" is also what a sweep that never considered the
URI eligible looks like.

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
from penca_client.naming import (
    SEGMENT_DELETE_SET,
    TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    setup_partitioned_table,
    write_cycle,
)

# ── Helpers ───────────────────────────────────────────────────────────


def _now_micros():
    return int(time.time() * 1_000_000)


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


def _sidecar_rows(catalog_uuid, branch_uuid, snapshot_uuid=None):
    """``[(segment_index_uuid, object_uri), ...]``.

    ``snapshot_uuid`` narrows to one snapshot's sidecars; omit it to list
    every sidecar on the branch (the child's copied row hangs off a
    synthesized index header, not off a snapshot the writer produced).
    A sidecar row carries no snapshot of its own — it hangs off an index
    header — so narrowing joins through ``table_snapshot_index_metadata``.
    """
    child = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}"
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    if snapshot_uuid is None:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT segment_index_uuid::text, object_uri FROM {child}"
                " WHERE branch_uuid = %s ORDER BY object_uri"
            ).format(child=Identifier(child)),
            (branch_uuid,),
        )
    else:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT c.segment_index_uuid::text, c.object_uri"
                " FROM {child} c JOIN {parent} p"
                " ON p.branch_uuid = c.branch_uuid"
                " AND p.table_snapshot_index_uuid = c.table_snapshot_index_uuid"
                " WHERE c.branch_uuid = %s AND p.table_snapshot_uuid = %s"
                " ORDER BY c.object_uri"
            ).format(child=Identifier(child), parent=Identifier(parent)),
            (branch_uuid, snapshot_uuid),
        )

    return [(r[0], r[1]) for r in rows]


def _age_delete_set_rows(catalog_uuid, branch_uuid, uri):
    """Push a branch's delete-set rows for ``uri`` past the grace window."""
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    get_pg_driver().execute_no_result(
        SQL(
            "UPDATE {tbl} SET written_at_micros = %s"
            " WHERE branch_uuid = %s AND object_uri = %s"
        ).format(tbl=Identifier(tbl)),
        (_now_micros() - 10_000_000, branch_uuid, uri),
    )


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
    """Drop every row a snapshot owns, as retirement would.

    Must include the CHA-412 index sidecars and their headers, not only
    the base segments: real retirement drops them too
    (``delete_segment_index_metadata_for_segments``). A simulation that
    left the sidecars behind would leave the retiring branch's own row
    pinning the sidecar's ``object_uri``, so a sidecar-refcount test
    built on it would pass no matter how the gate is scoped.
    """
    child = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}"
    header = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    # Sidecars hang off the header, which is what carries the snapshot,
    # so they go first and by join.
    get_pg_driver().execute_no_result(
        SQL(
            "DELETE FROM {child} c USING {header} p"
            " WHERE p.branch_uuid = c.branch_uuid"
            " AND p.table_snapshot_index_uuid = c.table_snapshot_index_uuid"
            " AND c.branch_uuid = %s AND p.table_snapshot_uuid = %s"
        ).format(child=Identifier(child), header=Identifier(header)),
        (branch_uuid, snapshot_uuid),
    )
    for name in (
        TABLE_SNAPSHOT_INDEX_METADATA,
        TABLE_SNAPSHOT_SEGMENT_METADATA,
        TABLE_SNAPSHOT_METADATA,
    ):
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
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fgc")
    )

    snap_main = write_cycle(
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

    # Positive control. Surviving phase 1 is also what a sweep that never
    # considered the URI eligible at all looks like — a grace-window or
    # clock mismatch, an enqueue on a branch the sweep does not scan, a
    # swallowed cold-delete error. Dropping the child's reference and
    # sweeping again isolates the child reference as the one thing
    # pinning the file: if the row drains now, the sweep was live and
    # willing to delete this URI all along.
    _drop_snapshot_rows(catalog_uuid, child_branch, child_snap)
    assert _segment_uris(catalog_uuid, child_branch, child_snap) == [], (
        "the control's setup did not take: the child's synthesized segment"
        " rows are still present, so a surviving delete-set row below would"
        " mean nothing."
    )

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    drained = _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri])
    assert drained == [], (
        "the delete-set row survived a sweep with zero remaining references,"
        " so phase 1's survival proves nothing about the refcount gate."
        f" Either the sweep never treated {shared_uri} as eligible, or the"
        " cold-file delete failed and the row was left for retry (sweep.rs"
        " only drains a row after a successful delete)."
    )


def test_parent_sweep_spares_a_sidecar_another_branch_references():
    """The same cross-branch refcount contract for the SIDECAR arm.

    ``eligible_segment_delete_set_rows`` has a second ``NOT EXISTS`` arm
    against ``table_snapshot_segment_index_metadata``: a carried cold
    index sidecar (CHA-412) copies the prior file's ``object_uri`` by
    reference exactly as its base segment does, so it has the identical
    cross-branch exposure. A fix that makes only the base-segment arm
    catalog-wide turns the test above green while carried sidecar files
    are still deleted out from under the child.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fgc")
    )

    snap_main = write_cycle(
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
    sidecar_rows = _sidecar_rows(catalog_uuid, main_branch, snap_main)
    assert sidecar_rows, (
        "the parent snapshot must have written at least one row_uuid index"
        " sidecar (CHA-412) for this test to mean anything"
    )
    shared_sidecar_uuid, shared_uri = sidecar_rows[0]

    child_branch = client.create_branch(
        f"fgc_sidecar_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    # A carried sidecar reference: same object_uri, fresh identity, under
    # the child's branch_uuid — with its parent header row, since the
    # sidecar row is keyed by table_snapshot_index_uuid.
    child_snap = str(uuid4())
    child_index = str(uuid4())
    _copy_row_to_branch(
        f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}",
        where="branch_uuid = %s AND table_snapshot_uuid = %s",
        params=(main_branch, snap_main),
        overrides={"branch_uuid": child_branch, "table_snapshot_uuid": child_snap},
    )
    _copy_row_to_branch(
        f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}",
        where="branch_uuid = %s AND table_snapshot_uuid = %s",
        params=(main_branch, snap_main),
        overrides={
            "branch_uuid": child_branch,
            "table_snapshot_uuid": child_snap,
            "table_snapshot_index_uuid": child_index,
        },
    )
    _copy_row_to_branch(
        f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}",
        where="branch_uuid = %s AND segment_index_uuid = %s",
        params=(main_branch, shared_sidecar_uuid),
        overrides={
            "branch_uuid": child_branch,
            "table_snapshot_index_uuid": child_index,
            "segment_index_uuid": str(uuid4()),
        },
    )
    assert shared_uri in {
        uri for _u, uri in _sidecar_rows(catalog_uuid, child_branch)
    }, "setup failed: the child must hold a reference to the parent's sidecar"

    _insert_delete_set_row(
        catalog_uuid, main_branch, table_uuid, shared_uri, _now_micros() - 10_000_000
    )
    _drop_snapshot_rows(catalog_uuid, main_branch, snap_main)

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    assert _sidecar_rows(catalog_uuid, main_branch) == [], (
        "the retirement simulation did not take: the parent still holds its"
        " own sidecar rows, which would pin the URI through the branch-scoped"
        " arm and make the assertion below pass no matter how the gate is"
        " scoped."
    )

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    remaining = _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri])
    assert len(remaining) == 1, (
        "sweep deleted a cold INDEX SIDECAR that another branch's snapshot"
        f" still references. {shared_uri} is referenced by branch"
        f" {child_branch} via table_snapshot_segment_index_metadata, so the"
        " sidecar refcount arm must probe catalog-wide, not branch-scoped."
    )

    # Positive control: drop the child's copied sidecar and the file has
    # zero references left, so the same sweep drains it.
    _drop_snapshot_rows(catalog_uuid, child_branch, child_snap)

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    assert _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri]) == [], (
        "the delete-set row survived a sweep with zero remaining sidecar"
        " references, so the survival above proves nothing about the sidecar"
        " arm. Either the sweep never treated the URI as eligible, or the"
        " cold-file delete failed and the row was left for retry."
    )


def test_parent_sweep_respects_another_branchs_grace_window():
    """The grace clock is cross-branch too.

    ``naming::segment_delete_uuid`` is branch-keyed, so when a retirement
    on branch B drops the last reference to a file the parent enqueued
    long ago, only B's delete-set row gets a fresh ``written_at_micros``.
    Without a self-join arm the parent's own already-expired row goes
    eligible the instant B drops that reference, deleting the file inside
    the grace window a concurrent reader on B relies on.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fgc")
    )

    snap_main = write_cycle(
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
    assert parent_rows
    _shared_seg_uuid, shared_uri = parent_rows[0]

    sibling_branch = client.create_branch(
        f"fgc_grace_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    # Zero live references, but two delete-set rows for the same file: the
    # parent's is long expired, the sibling's was just written.
    _drop_snapshot_rows(catalog_uuid, main_branch, snap_main)
    _insert_delete_set_row(
        catalog_uuid, main_branch, table_uuid, shared_uri, _now_micros() - 10_000_000
    )
    # Stamped a minute ahead, not at "now". QUERY_TIMEOUT_SECONDS is 2s in
    # docker/test.env, and this timestamp comes from the test host's clock
    # while the threshold comes from Postgres's — gRPC latency plus any
    # container/host skew would eat a 2s margin and flip the row out of
    # grace, failing the test for a reason it does not test. Only
    # `_age_delete_set_rows` below should move it.
    _insert_delete_set_row(
        catalog_uuid,
        sibling_branch,
        table_uuid,
        shared_uri,
        _now_micros() + 60_000_000,
    )

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    remaining = _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri])
    assert len(remaining) == 1, (
        f"sweep deleted {shared_uri} while branch {sibling_branch} still held"
        " a within-grace delete-set row for it. The grace gate must be"
        " cross-branch: the retirement that dropped the last reference"
        " refreshed only the retiring branch's row."
    )

    # Positive control: age the sibling's row out of grace and the same
    # sweep drains the file, proving the grace arm — not some unrelated
    # ineligibility — was what spared it above.
    _age_delete_set_rows(catalog_uuid, sibling_branch, shared_uri)

    client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=main_branch)

    drained = _delete_set_rows_for_uris(catalog_uuid, main_branch, [shared_uri])
    assert drained == [], (
        "with every delete-set row for this file past grace and zero live"
        f" references, the sweep must drain {shared_uri}; it did not, so the"
        " assertion above proves nothing about the grace arm."
    )
