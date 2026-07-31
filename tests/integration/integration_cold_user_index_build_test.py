"""Integration tests for the cold-tier USER secondary-index BUILD (CHA-483,
chunk B-user of the CHA-463 umbrella).

At snapshot build the lifecycle op reads the live ``index_metadata`` (the
table's ``CreateIndex`` definitions) and, alongside the strictly-internal
``row_uuid`` index (CHA-412), declares a USER index parent row per defined
index (``index_uuid`` non-NULL == the user index uuid) and materializes a
per-segment sidecar over the indexed column(s) via the CHA-480 composite
build kernel:

* ``{catalog}_table_snapshot_index_metadata`` (parent) — one committed row
  per ``(snapshot, index)``; a USER index is ``index_uuid IS NOT NULL``.
* ``{catalog}_table_snapshot_segment_index_metadata`` (child) — one committed
  sidecar per ``(segment, index)``, linked to its parent via
  ``table_snapshot_index_uuid``.

Materialize-on-next-snapshot (the load-bearing decision, ADR 0026): a new
index is built for EVERY base segment in its first snapshot — new/rewritten
segments from the in-memory pack batch, and carried-forward segments by
reading their base file once. So a declared index is FULLY materialized at
the next snapshot (no partial/"eventual" coverage, no coverage signal).
``DROP INDEX`` is lazy: the parent simply isn't re-declared next snapshot.

Assertions are metadata-only (white-box PG introspection), matching
``integration_cold_row_uuid_index_build_test.py``; the sorted
``(key, row_offset)`` artifact content is pinned at the ``penca_format::index``
Rust unit level, not by reading cold bytes here.

Red-phase: before this PR no USER parent/child rows are declared (only the
internal ``index_uuid IS NULL`` index builds), so the parent/child counts are
0 and the assertions fail. Post-impl they hold.

Run via ``just integration-test query lifecycle``
(filter: ``--test-arg integration_cold_user_index_build``).
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    table_snapshot_uuid,
)
from psycopg.errors import UndefinedTable
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)

# Introduced by CHA-412's schema redesign; literal base names so the module
# imports regardless of Python naming-constant availability.
TABLE_SNAPSHOT_INDEX_METADATA = "table_snapshot_index_metadata"
TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA = "table_snapshot_segment_index_metadata"


def _setup(client):
    catalog_uuid, main_branch = client.create_catalog(
        f"uidx_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="uidx"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        partition_keys=["name"],  # per-partition segments (carry-forward unit)
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="uidx",
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
    """``[(segment_uuid, object_uri, offset), ...]`` for a snapshot's committed
    base segments. A carried-forward segment shares its prior ``(object_uri,
    offset)`` (carry-by-reference) under a new ``segment_uuid`` — that match is
    how a test proves a segment was carried, not rewritten."""
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            'SELECT table_snapshot_segment_uuid::text, object_uri, "offset"'
            " FROM {tbl} WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )
    return [(r[0], r[1], r[2]) for r in rows]


def _parent_rows(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(table_snapshot_index_uuid, index_uuid_or_None, committed,
    key_columns_or_None), ...]`` for a snapshot's parent index records.
    UndefinedTable (schema absent) ⇒ []."""
    tbl = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    try:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT table_snapshot_index_uuid::text, index_uuid::text,"
                " commit_micros IS NOT NULL, key_columns FROM {tbl}"
                " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(tbl)),
            (branch_uuid, snapshot_uuid),
        )
    except UndefinedTable:
        return []

    return [(r[0], r[1], r[2], r[3]) for r in rows]


def _committed_key_columns(catalog_uuid, branch_uuid, snapshot_uuid):
    """``{index_uuid_or_None: key_columns_or_None}`` over the snapshot's
    COMMITTED parents — the CHA-485 planner-input stamp, pinned where it is
    produced."""
    return {
        idx_uuid: key_columns
        for _, idx_uuid, committed, key_columns in _parent_rows(
            catalog_uuid, branch_uuid, snapshot_uuid
        )
        if committed
    }


def _user_parent(catalog_uuid, branch_uuid, snapshot_uuid, index_uuid):
    """The committed parent row whose ``index_uuid`` == ``index_uuid`` (the user
    index), or ``None``. Returns its ``table_snapshot_index_uuid``."""
    for link, idx_uuid, committed, _key_columns in _parent_rows(
        catalog_uuid, branch_uuid, snapshot_uuid
    ):
        if committed and idx_uuid == index_uuid:
            return link

    return None


def _child_rows(catalog_uuid, branch_uuid, segment_uuid):
    """``[(link, parent_index_uuid_or_None, committed, object_uri, length), ...]``
    for one base segment's sidecars, joined to the PARENT (the user-ness comes
    from ``parent.index_uuid``, not the child). UndefinedTable ⇒ []."""
    child = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}"
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    try:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT c.table_snapshot_index_uuid::text, p.index_uuid::text,"
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


def _user_child(catalog_uuid, branch_uuid, segment_uuid, index_uuid):
    """The single committed sidecar on ``segment_uuid`` whose parent is the user
    index ``index_uuid``, or ``None``. Returns ``(link, object_uri, length)``."""
    for link, idx_uuid, committed, object_uri, length in _child_rows(
        catalog_uuid, branch_uuid, segment_uuid
    ):
        if committed and idx_uuid == index_uuid:
            return (link, object_uri, length)

    return None


def _segment_content_hash(catalog_uuid, branch_uuid, segment_uuid):
    """A base segment's ``content_hash``, as text."""
    rows = get_pg_driver().execute(
        SQL(
            "SELECT content_hash::text FROM {tbl}"
            " WHERE branch_uuid = %s AND table_snapshot_segment_uuid = %s"
        ).format(tbl=Identifier(f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}")),
        (branch_uuid, segment_uuid),
    )
    return rows[0][0]


def _sidecar_content_hash(catalog_uuid, branch_uuid, segment_uuid, link):
    """The ``content_hash`` of ``segment_uuid``'s sidecar under one parent
    index, as text. ``link`` is the parent ``table_snapshot_index_uuid``
    returned by :func:`_user_child`."""
    rows = get_pg_driver().execute(
        SQL(
            "SELECT content_hash::text FROM {tbl}"
            " WHERE branch_uuid = %s AND segment_uuid = %s"
            " AND table_snapshot_index_uuid = %s"
        ).format(
            tbl=Identifier(f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA}")
        ),
        (branch_uuid, segment_uuid, link),
    )
    return rows[0][0]


class TestUserIndexBuild:
    """A cold snapshot materializes user ``CREATE INDEX`` definitions: a
    committed parent (``index_uuid`` non-NULL) + one committed sidecar per base
    segment, linked, for both single-column and composite indexes."""

    def test_single_column_user_index_materializes(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="idx",
        )
        assert index_uuid

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

        # PARENT: one committed user-index header (index_uuid == the created uuid).
        parent_link = _user_parent(catalog_uuid, branch, snap, index_uuid)
        assert parent_link is not None, (
            "snapshot must declare a committed user-index parent row for"
            f" index_uuid {index_uuid}; got {_parent_rows(catalog_uuid, branch, snap)}"
        )

        # CHA-485: the parent stamps the declared key columns (the planner's
        # covering-index input); the internal identity parent stays NULL.
        key_columns = _committed_key_columns(catalog_uuid, branch, snap)
        assert key_columns[index_uuid] == ["value"], (
            "user parent must carry the declared key columns in order;"
            f" got {key_columns[index_uuid]!r}"
        )
        assert key_columns[None] is None, (
            "internal identity parent must not carry key_columns"
        )

        # CHILD: one committed user sidecar per base segment, linked to the parent.
        for seg_uuid, _row_count in base:
            child = _user_child(catalog_uuid, branch, seg_uuid, index_uuid)
            assert child is not None, (
                f"base segment {seg_uuid} must have a committed user-index sidecar"
                f" for {index_uuid}; got {_child_rows(catalog_uuid, branch, seg_uuid)}"
            )
            link, object_uri, _length = child
            assert link == parent_link, (
                "child sidecar must link to the snapshot's user parent index row"
            )
            assert object_uri, "sidecar must record a non-empty object_uri"

    def test_composite_user_index_materializes(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_name_value",
            columns=["name", "value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="idx",
        )
        assert index_uuid

        # (The definition round-trip — columns echoed in order — is owned by
        # integration_index_ddl_test; here we assert the composite BUILD. The
        # sorted composite (key0, key1, row_offset) artifact content is pinned at
        # the penca_format::index Rust unit level, not read back here.)
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

        parent_link = _user_parent(catalog_uuid, branch, snap, index_uuid)
        assert parent_link is not None, (
            "snapshot must declare a committed parent row for the composite"
            f" user index {index_uuid}"
        )

        # CHA-485: composite stamp preserves the declared column ORDER (the
        # sidecar's sort-priority order — the planner binds probes by it).
        key_columns = _committed_key_columns(catalog_uuid, branch, snap)
        assert key_columns[index_uuid] == ["name", "value"], (
            "composite parent must stamp the declared key columns in order;"
            f" got {key_columns[index_uuid]!r}"
        )
        for seg_uuid, _row_count in base:
            child = _user_child(catalog_uuid, branch, seg_uuid, index_uuid)
            assert child is not None, (
                f"base segment {seg_uuid} must have a composite user-index sidecar"
                f" for {index_uuid}"
            )
            link, object_uri, _length = child
            assert link == parent_link, (
                "composite child sidecar must link to the snapshot's parent"
                " index row (not some other parent)"
            )
            assert object_uri, "sidecar must record a non-empty object_uri"

    def test_drop_index_stops_redeclaring(self):
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="idx",
        )

        snap1 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
        )
        # Precondition: snap1 declares the user parent.
        assert _user_parent(catalog_uuid, branch, snap1, index_uuid) is not None, (
            "snap1 must declare the user-index parent before the drop"
        )

        # DROP, then force a new snapshot.
        client.delete_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="drop",
        )
        snap2 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [2]}, schema=USER_SCHEMA),
        )

        # Re-derive-each-snapshot: the dropped index is not re-declared at snap2.
        assert _user_parent(catalog_uuid, branch, snap2, index_uuid) is None, (
            "a dropped index must NOT be re-declared at the next snapshot"
            f" (snap2 parents: {_parent_rows(catalog_uuid, branch, snap2)})"
        )

    def test_index_materializes_all_segments_incl_carried(self):
        """Materialize-on-next-snapshot: an index created when base segments
        already exist is built for EVERY segment in the next snapshot — the
        rewritten one from the in-memory batch AND the carried-forward one by
        reading its base file. Full materialization, no partial coverage."""
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        # S1: two partitions, NO index yet -> base segments without any sidecar.
        snap1 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice", "carol"], "value": [1, 3]}, schema=USER_SCHEMA),
        )
        assert len(_base_segments(catalog_uuid, branch, snap1)) >= 2

        # Define the index AFTER S1, then snapshot touching only alice's
        # partition -> carol's segment carries forward (it had no sidecar at S1).
        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="idx",
        )
        snap2 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )

        tuples1 = _base_segment_tuples(catalog_uuid, branch, snap1)
        tuples2 = _base_segment_tuples(catalog_uuid, branch, snap2)
        assert len(tuples2) >= 2, "snap2 must include the rewritten + carried segments"

        # The test only exercises the read-carried-base-file path if a segment is
        # actually CARRIED (not rewritten): a snap2 segment sharing a snap1
        # segment's (object_uri, offset) under a new uuid.
        prior_files = {(uri, off) for _seg, uri, off in tuples1}
        carried = [seg for seg, uri, off in tuples2 if (uri, off) in prior_files]
        assert carried, (
            "snap2 must carry forward an unchanged segment by reference so the"
            " materialize-on-carried path is exercised (not just a full rewrite)"
        )

        # FULL materialization: EVERY base segment in snap2 — rewritten AND
        # carried — has a committed user sidecar for the index, including the
        # carried one built by reading its base file.
        base2 = [seg for seg, _uri, _off in tuples2]
        covered = [
            seg
            for seg in base2
            if _user_child(catalog_uuid, branch, seg, index_uuid) is not None
        ]
        assert len(covered) == len(base2), (
            "every base segment in snap2 must have a user-index sidecar"
            f" (full materialization incl. carried): covered {len(covered)} of"
            f" {len(base2)}"
        )
        for seg in carried:
            assert _user_child(catalog_uuid, branch, seg, index_uuid) is not None, (
                "the carried segment must have a freshly-built user sidecar"
            )

    def test_user_index_sidecar_carries_forward_by_reference(self):
        """Steady-state covered carry: once an index already covers a segment,
        the next snapshot carries that sidecar forward by REFERENCE (same
        object_uri, no rebuild) — the covered_by_index ->
        insert_carried_segment_indexes path the index_slug refactor touches."""
        client = make_client()
        catalog_uuid, branch, schema_uuid, table_uuid = _setup(client)

        # Index exists from the start, so snap1 builds a sidecar for every
        # segment (both partitions).
        index_uuid = client.create_index(
            table_uuid=table_uuid,
            index_name="idx_value",
            columns=["value"],
            index_type=SCALAR_BTREE,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch,
            author="test",
            comment="idx",
        )
        snap1 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice", "carol"], "value": [1, 3]}, schema=USER_SCHEMA),
        )
        # Map each snap1 base file -> its user sidecar uri, and both rows'
        # content hashes for the inheritance assertion below.
        snap1_sidecar_by_file = {}
        snap1_hashes_by_file = {}
        for seg, uri, off in _base_segment_tuples(catalog_uuid, branch, snap1):
            child = _user_child(catalog_uuid, branch, seg, index_uuid)
            assert child is not None, "snap1 must build a user sidecar per segment"
            snap1_sidecar_by_file[(uri, off)] = child[1]  # object_uri
            snap1_hashes_by_file[(uri, off)] = (
                _segment_content_hash(catalog_uuid, branch, seg),
                _sidecar_content_hash(catalog_uuid, branch, seg, child[0]),
            )

        # snap2 rewrites only alice -> carol's segment carries forward, and its
        # already-built sidecar must carry by reference (same object_uri).
        snap2 = _cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch,
            table_uuid,
            pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )
        carried = [
            (seg, uri, off)
            for seg, uri, off in _base_segment_tuples(catalog_uuid, branch, snap2)
            if (uri, off) in snap1_sidecar_by_file
        ]
        assert carried, "snap2 must carry forward carol's unchanged segment"
        for seg, uri, off in carried:
            child = _user_child(catalog_uuid, branch, seg, index_uuid)
            assert child is not None, "carried segment must keep its user sidecar"
            assert child[1] == snap1_sidecar_by_file[(uri, off)], (
                "carried user sidecar must reference the SAME file as snap1 (carry"
                f" by reference, no rebuild): {child[1]} !="
                f" {snap1_sidecar_by_file[(uri, off)]}"
            )
            # CHA-545: a carried row is a fresh uuid over bytes nobody rewrote,
            # so it must inherit the prior row's content_hash verbatim. That
            # inheritance is the whole dedup — recomputing, or defaulting, would
            # give the same bytes two cache entries.
            prior_seg_hash, prior_sidecar_hash = snap1_hashes_by_file[(uri, off)]
            assert _segment_content_hash(catalog_uuid, branch, seg) == prior_seg_hash, (
                "a carried base segment must inherit its prior row's"
                f" content_hash, expected {prior_seg_hash}"
            )
            assert (
                _sidecar_content_hash(catalog_uuid, branch, seg, child[0])
                == prior_sidecar_hash
            ), (
                "a carried sidecar must inherit its prior row's content_hash,"
                f" expected {prior_sidecar_hash}"
            )
