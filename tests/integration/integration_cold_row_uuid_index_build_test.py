"""Integration tests for the cold-tier internal ``row_uuid`` index BUILD
(CHA-412).

The snapshot lifecycle op auto-builds a strictly-internal ``row_uuid``
identity index (``index_uuid IS NULL``, never user-facing) for every cold
snapshot regardless of storage format — the sidecar follows the table's
format (ADR 0026 §6) — across the two-table materialization schema this PR
introduces:

* ``{catalog}_table_snapshot_index_metadata`` (parent) — one committed row
  per ``(snapshot, index)``; the internal index is ``index_uuid IS NULL``.
* ``{catalog}_table_snapshot_segment_index_metadata`` (child) — one committed
  sidecar per ``(segment, index)``, linked to its parent via
  ``table_snapshot_index_uuid``, with one index entry per base row.

These assertions are **metadata-only** (white-box PG introspection), matching
how the rest of the cold-storage suite verifies segment materialization
(``integration_lifecycle_test.py`` storage tuples) — the sorted
``(key, row_offset)`` artifact *content* is pinned exhaustively at the Rust
unit-test level (the ``penca_format::index`` build kernel), not by reading
cold file bytes here.

Red-phase: before this PR the two tables don't exist (helpers below treat an
``UndefinedTable`` as "no rows"), and once the schema lands but the BUILD is
unimplemented they stay empty — either way the parent/child row counts are 0,
so the assertions fail. Post-impl they hold.

Run via ``just integration-test query lifecycle``
(filter: ``--test-arg integration_cold_row_uuid_index_build``).
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.naming import (
    SEGMENT_DELETE_SET,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    table_snapshot_uuid,
)
from psycopg.errors import UndefinedTable
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)

# These tables are introduced by this PR's schema redesign (S1); use literal
# base names so the module imports before Python naming constants exist.
TABLE_SNAPSHOT_INDEX_METADATA = "table_snapshot_index_metadata"
TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA = "table_snapshot_segment_index_metadata"


def _setup(client):
    catalog_uuid, main_branch = client.create_catalog(
        f"ruidx_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="ruidx"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        partition_keys=["name"],  # per-partition segments
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="ruidx",
    )
    return catalog_uuid, main_branch, schema_uuid, table_uuid


def _cycle(client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, upserts):
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
    resp = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    return table_snapshot_uuid(
        catalog_uuid, branch_uuid, table_uuid, resp.snapshotted_at_micros
    )


def _base_segments(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(segment_uuid, row_count), ...]`` for a snapshot's committed base
    segments."""
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT table_snapshot_segment_uuid::text, row_count FROM {tbl}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )
    return [(r[0], r[1]) for r in rows]


def _base_segment_tuples(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(segment_uuid, object_uri, offset, row_count, chunk_idx), ...]`` — the
    carry-forward sharing identity (object_uri + offset) plus chunk_idx."""
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT table_snapshot_segment_uuid::text, object_uri,"
            ' "offset", row_count, chunk_idx FROM {tbl}'
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


def _delete_set_has_uri(catalog_uuid, uri):
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    rows = get_pg_driver().execute(
        SQL("SELECT 1 FROM {tbl} WHERE object_uri = %s").format(tbl=Identifier(tbl)),
        (uri,),
    )
    return len(rows) > 0


def _parent_index_rows(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(table_snapshot_index_uuid, index_uuid_is_null, committed), ...]`` for a
    snapshot's parent index records. UndefinedTable (schema not yet built) ⇒ []."""
    tbl = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    try:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT table_snapshot_index_uuid::text, index_uuid IS NULL,"
                " commit_micros IS NOT NULL FROM {tbl}"
                " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(tbl)),
            (branch_uuid, snapshot_uuid),
        )
    except UndefinedTable:
        return []

    return [(r[0], r[1], r[2]) for r in rows]


def _child_index_rows(catalog_uuid, branch_uuid, segment_uuid):
    """``[(table_snapshot_index_uuid, index_uuid_is_null, committed, object_uri, length), ...]``
    for one base segment's sidecars. The child carries no ``index_uuid`` — the
    internal-ness comes from the PARENT it links to via
    ``table_snapshot_index_uuid``. UndefinedTable ⇒ []."""
    child = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}"
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    try:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT c.table_snapshot_index_uuid::text, p.index_uuid IS NULL,"
                " c.commit_micros IS NOT NULL, c.object_uri, c.length"
                " FROM {child} c JOIN {parent} p"
                " ON p.branch_uuid = c.branch_uuid"
                " AND p.table_snapshot_index_uuid = c.table_snapshot_index_uuid"
                " WHERE c.branch_uuid = %s AND c.segment_uuid = %s"
            ).format(child=Identifier(child), parent=Identifier(parent)),
            (branch_uuid, segment_uuid),
        )
    except UndefinedTable:
        return []

    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


class TestRowUuidIndexBuild:
    """A cold snapshot auto-builds the internal ``row_uuid`` index:
    one committed parent row + one committed child sidecar per base segment,
    linked, ``index_uuid IS NULL``, one index entry per base row."""

    def test_snapshot_builds_internal_row_uuid_index(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        snap = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )

        base = _base_segments(catalog_uuid, branch, snap)
        assert base, "snapshot must have committed base segments"

        # PARENT: exactly one committed internal-index header for the snapshot.
        internal_parents = _internal_parents(catalog_uuid, branch, snap)
        assert len(internal_parents) == 1, (
            "snapshot must declare exactly one committed internal row_uuid index"
            f" parent row (index_uuid IS NULL); got"
            f" {_parent_index_rows(catalog_uuid, branch, snap)}"
        )
        parent_uuid = internal_parents[0][0]

        # CHILD: one committed sidecar per base segment, linked to the parent,
        # index_uuid NULL, one index entry per base row (length == row_count).
        for seg_uuid, row_count in base:
            children = _child_index_rows(catalog_uuid, branch, seg_uuid)
            internal = [c for c in children if c[1] and c[2]]
            assert len(internal) == 1, (
                f"base segment {seg_uuid} must have one committed internal"
                f" row_uuid sidecar; got {children}"
            )
            link, _is_null, _committed, object_uri, length = internal[0]
            assert link == parent_uuid, (
                "child sidecar must link to the snapshot's parent index row"
            )
            assert object_uri, "sidecar must record a non-empty object_uri"
            assert length == row_count, (
                f"sidecar must hold one entry per base row: length {length}"
                f" != row_count {row_count}"
            )


def _internal_parents(catalog_uuid, branch_uuid, snapshot_uuid):
    """The committed internal (``index_uuid IS NULL``) parent index rows for a
    snapshot (mirrors ``_internal_child``)."""
    return [
        p
        for p in _parent_index_rows(catalog_uuid, branch_uuid, snapshot_uuid)
        if p[1] and p[2]
    ]


def _internal_child(catalog_uuid, branch_uuid, segment_uuid):
    """The single committed internal (``index_uuid IS NULL``) sidecar for a
    segment, or ``None``."""
    internal = [
        c
        for c in _child_index_rows(catalog_uuid, branch_uuid, segment_uuid)
        if c[1] and c[2]
    ]
    return internal[0] if internal else None


class TestRowUuidIndexCarryForwardAndGc:
    """A REAL (built, not raw-SQL-seeded) internal ``row_uuid`` sidecar rides
    the CHA-455 lifecycle plumbing end-to-end: the child carries forward by
    reference with its base segment and the parent is re-declared each
    snapshot. (CHA-468 decoupled retirement from Snapshot, so the
    retirement-enqueues-the-sidecar leg is now covered by CHA-55.)"""

    def test_built_sidecar_carries_forward_and_parent_redeclared(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        snap1 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = _base_segment_tuples(catalog_uuid, branch, snap1)
        # carol = label-largest partition (highest chunk_idx); untouched in
        # cycle 2 so it carries forward (mirrors integration_lifecycle_test).
        carol = max(tuples1, key=lambda r: r[4])
        carol_seg, carol_uri, carol_offset = carol[0], carol[1], carol[2]

        carol_child = _internal_child(catalog_uuid, branch, carol_seg)
        assert carol_child is not None, (
            "cycle-1 build must emit an internal row_uuid sidecar for carol's"
            " segment (nothing to carry forward otherwise)"
        )
        carol_sidecar_uri = carol_child[3]

        # snap1's internal parent header, captured while snap1 is still the
        # latest snapshot — CHA-468 stopped Snapshot from retiring, so snap2
        # no longer drops snap1's parent header (asserted after snap2).
        p1 = _internal_parents(catalog_uuid, branch, snap1)
        assert len(p1) == 1, "snap1 declares one committed internal parent row"

        # Cycle 2 touches only alice -> carol carried forward by reference.
        snap2 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )
        tuples2 = _base_segment_tuples(catalog_uuid, branch, snap2)
        carried = [t for t in tuples2 if t[1] == carol_uri and t[2] == carol_offset]
        assert carried, f"carol's segment must carry forward into snap2: {tuples2}"
        carried_seg = carried[0][0]
        assert carried_seg != carol_seg

        # The carried child sidecar: NEW segment_uuid, SAME object_uri (by ref).
        carried_child = _internal_child(catalog_uuid, branch, carried_seg)
        assert carried_child is not None, (
            "the carried segment must carry its internal row_uuid sidecar forward"
        )
        assert carried_child[3] == carol_sidecar_uri, (
            "carried sidecar must reference the same file by uri (no rebuild):"
            f" {carried_child[3]} != {carol_sidecar_uri}"
        )

        # Parent re-declared per snapshot: snap2 has its own committed internal
        # parent row, distinct from snap1's.
        p2 = _internal_parents(catalog_uuid, branch, snap2)
        assert len(p2) == 1, "snap2 declares its own committed internal parent row"
        assert p1[0][0] != p2[0][0], "parent index row is re-declared per snapshot"

        # CHA-468: Snapshot no longer retires, so snap1 is not retired when
        # snap2 becomes latest — snap1's internal parent header survives
        # alongside snap2's (the carried child sidecar still references it).
        assert len(_internal_parents(catalog_uuid, branch, snap1)) == 1, (
            "snap1's internal parent header must survive — Snapshot no longer "
            "retires prior snapshots (CHA-468)"
        )

    @pytest.mark.skip(
        reason="CHA-468 removed the snapshot->retire trigger; "
        "sidecar-enqueue-on-retirement coverage returns with the "
        "PruneSnapshotSegments RPC (CHA-55)."
    )
    def test_retiring_segment_enqueues_sidecar_for_gc(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        snap1 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
        )
        seg1 = _base_segment_tuples(catalog_uuid, branch, snap1)[0][0]
        child = _internal_child(catalog_uuid, branch, seg1)
        assert child is not None, "cycle-1 build must emit a sidecar to later GC"
        sidecar_uri = child[3]

        # Rewrite alice's partition -> retires snap1's base segment + sidecar.
        _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [2]}, schema=USER_SCHEMA),
        )
        assert _delete_set_has_uri(catalog_uuid, sidecar_uri), (
            "retiring the base segment must enqueue its built row_uuid sidecar"
            " uri into segment_delete_set"
        )
