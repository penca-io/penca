"""Integration tests for the built-in system-table composite NAME-index BUILD
(CHA-481, chunk B-sys of the CHA-463 cold-index umbrella).

Each cold snapshot of a ``__penca_system__`` table now auto-builds — in ADDITION
to CHA-412's strictly-internal ``row_uuid`` identity index (``index_uuid IS
NULL``) — a built-in composite NAME index so by-name metadata resolves (chunk M
/ CHA-484) can seek instead of full-scan+filter:

* ``__penca_system__.schemas`` -> key ``[schema_name]``            (single col)
* ``__penca_system__.tables``  -> key ``[schema_uuid, table_name]`` (composite)
* ``__penca_system__.indexes`` -> key ``[table_uuid, index_name]``  (composite)

The built-in name index is a DECLARED index with a deterministic NON-NULL
``index_uuid`` (``system_name_index_uuid``) — non-NULL by design so the CHA-454 /
``meta_plan.rs`` row_uuid read plan (which selects the internal sidecar via
``index_uuid IS NULL``) excludes it and the CHA-473 by-uuid metadata path stays
unaffected. It is a *built-in* index: the build records it only in
``table_snapshot_index_metadata``, never as a row in the
``__penca_system__.indexes`` user-DDL registry — that absence is structurally
guaranteed by the build path (it issues no write to the indexes table), so it is
not separately asserted here.

Assertions are metadata-only (white-box PG introspection), mirroring
``integration_cold_row_uuid_index_build_test.py``; the sorted
``(key…, row_offset)`` artifact content + key arity are pinned at the Rust unit
level (the ``penca_format::index`` kernel + the ``system_name_index_spec``
classifier).

Red-phase: before B-sys lands, no parent/child rows carry the name ``index_uuid``
(only the row_uuid ``index_uuid IS NULL`` rows exist), so the name-index
assertions fail. Post-impl they hold; the row_uuid assertions are regression
guards that must keep holding (the name index must NOT leak into the NULL join).

Run via ``just integration-test cold_system_name_index_build``.
"""

from __future__ import annotations

from uuid import uuid4

from penca_client.naming import (
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    system_indexes_table_uuid,
    system_name_index_uuid,
    system_schema_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
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
# imports before any new Python naming constant exists.
TABLE_SNAPSHOT_INDEX_METADATA = "table_snapshot_index_metadata"
TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA = "table_snapshot_segment_index_metadata"


def _snapshot_system_table(client, catalog_uuid, branch_uuid, sys_table_uuid):
    """Persist + snapshot one ``__penca_system__`` table; return its
    ``table_snapshot_uuid``. Mirrors the production scheduler driving
    Persist → Snapshot on the system tables (CHA-154)."""
    kw = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": system_schema_uuid(catalog_uuid),
        "branch_uuid": branch_uuid,
        "table_uuid": sys_table_uuid,
    }
    client.persist(**kw)
    resp = client.snapshot(**kw)
    # Each caller commits fresh rows to the system table immediately before this
    # call, so persist always flushes new cold data and snapshot is never a
    # no-op — a None watermark here means the persist/snapshot path itself
    # regressed, which is exactly what we want surfaced.
    assert resp.snapshotted_at_micros is not None, (
        "snapshotting a populated system table must materialize a cold snapshot"
        " (got a no-op watermark)"
    )
    return table_snapshot_uuid(
        catalog_uuid, branch_uuid, sys_table_uuid, resp.snapshotted_at_micros
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


def _parent_index_rows(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(table_snapshot_index_uuid, index_uuid_text_or_None, committed), ...]``
    for a snapshot's parent index records. ``index_uuid`` is rendered as text so
    the built-in name index (non-NULL deterministic uuid) is distinguishable from
    the row_uuid internal index (NULL). UndefinedTable ⇒ []."""
    tbl = f"{catalog_uuid}_{TABLE_SNAPSHOT_INDEX_METADATA}"
    try:
        rows = get_pg_driver().execute(
            SQL(
                "SELECT table_snapshot_index_uuid::text, index_uuid::text,"
                " commit_micros IS NOT NULL FROM {tbl}"
                " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(tbl)),
            (branch_uuid, snapshot_uuid),
        )
    except UndefinedTable:
        return []

    return [(r[0], r[1], r[2]) for r in rows]


def _child_index_rows(catalog_uuid, branch_uuid, segment_uuid):
    """``[(link_parent_uuid, parent_index_uuid_text_or_None, committed,
    object_uri, length), ...]`` for one base segment's sidecars. The child
    carries no ``index_uuid`` — which index it belongs to comes from the PARENT
    it links via ``table_snapshot_index_uuid``. UndefinedTable ⇒ []."""
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


def _assert_name_index_fully_built(
    catalog_uuid, branch_uuid, snapshot_uuid, sys_table_uuid
):
    """One snapshot of a system table must carry: a committed name-index parent
    (non-NULL ``index_uuid``) PLUS one committed composite name sidecar per base
    segment — AND the row_uuid internal index (NULL parent) must remain exactly
    one sidecar per segment (the name index must not leak into the NULL join)."""
    base = _base_segments(catalog_uuid, branch_uuid, snapshot_uuid)
    assert base, "system-table snapshot must have committed base segments"

    name_index_uuid = system_name_index_uuid(sys_table_uuid)
    parents = _parent_index_rows(catalog_uuid, branch_uuid, snapshot_uuid)

    # row_uuid internal parent (NULL) still present (CHA-412) — regression guard.
    null_parents = [p for p in parents if p[1] is None and p[2]]
    assert len(null_parents) == 1, (
        f"snapshot must keep exactly one committed row_uuid parent (index_uuid"
        f" IS NULL); got {parents}"
    )
    # Built-in name parent: a committed non-NULL parent with the derived uuid.
    name_parents = [p for p in parents if p[1] == name_index_uuid and p[2]]
    assert len(name_parents) == 1, (
        f"snapshot must declare exactly one committed built-in name-index parent"
        f" (index_uuid == {name_index_uuid}); got {parents}"
    )
    name_parent_uuid = name_parents[0][0]

    for seg_uuid, row_count in base:
        children = _child_index_rows(catalog_uuid, branch_uuid, seg_uuid)

        name_children = [c for c in children if c[1] == name_index_uuid and c[2]]
        assert len(name_children) == 1, (
            f"base segment {seg_uuid} must carry exactly one committed name"
            f" sidecar (parent index_uuid == {name_index_uuid}); got {children}"
        )
        link, _idx, _committed, object_uri, length = name_children[0]
        assert link == name_parent_uuid, (
            "name sidecar must link the snapshot's name-index parent"
        )
        assert object_uri, "name sidecar must record a non-empty object_uri"
        assert length == row_count, (
            f"name sidecar must hold one entry per base row: length {length}"
            f" != row_count {row_count}"
        )

        # By-uuid unaffected: the row_uuid (NULL parent) sidecar count is still
        # exactly one per segment — the name index did not leak into the NULL
        # join meta_plan.rs depends on.
        null_children = [c for c in children if c[1] is None and c[2]]
        assert len(null_children) == 1, (
            f"base segment {seg_uuid} must still carry exactly one committed"
            f" row_uuid sidecar (NULL parent); got {children}"
        )


def _new_catalog(client):
    catalog_uuid, main_branch = client.create_catalog(
        f"sysidx_{uuid4().hex[:8]}", "owner"
    )
    return catalog_uuid, main_branch


class TestSystemNameIndexBuild:
    """A cold snapshot of each ``__penca_system__`` table auto-builds its
    built-in composite name index alongside the row_uuid index."""

    def test_schemas_name_index_built(self):
        client = make_client()
        catalog_uuid, branch = _new_catalog(client)
        # Populate __penca_system__.schemas with extra rows (beyond genesis).
        client.create_schema(
            "s_alpha", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        client.create_schema(
            "s_beta", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        sys_uuid = system_schemas_table_uuid(catalog_uuid)
        snap = _snapshot_system_table(client, catalog_uuid, branch, sys_uuid)
        _assert_name_index_fully_built(catalog_uuid, branch, snap, sys_uuid)

    def test_tables_name_index_built(self):
        client = make_client()
        catalog_uuid, branch = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        for name in ("t_alpha", "t_beta"):
            client.create_table(
                name,
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="t",
                comment="sysidx",
            )

        sys_uuid = system_tables_table_uuid(catalog_uuid)
        snap = _snapshot_system_table(client, catalog_uuid, branch, sys_uuid)
        _assert_name_index_fully_built(catalog_uuid, branch, snap, sys_uuid)

    def test_indexes_name_index_built(self):
        client = make_client()
        catalog_uuid, branch = _new_catalog(client)
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        table_uuid = client.create_table(
            "t",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="t",
            comment="sysidx",
        )
        for name, col in (("idx_name", "name"), ("idx_value", "value")):
            client.create_index(
                table_uuid=table_uuid,
                index_name=name,
                columns=[col],
                index_type=SCALAR_BTREE,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="t",
                comment="sysidx",
            )

        sys_uuid = system_indexes_table_uuid(catalog_uuid)
        snap = _snapshot_system_table(client, catalog_uuid, branch, sys_uuid)
        _assert_name_index_fully_built(catalog_uuid, branch, snap, sys_uuid)

    def test_name_index_fully_covered_across_snapshots(self):
        """The built-in name index materializes for EVERY segment on EVERY
        snapshot (like row_uuid) — a 2nd snapshot's segments are also fully
        name-covered. Pins "always fully covered"; if a system table ever
        carried segments forward (it shouldn't — unpartitioned), a gap here
        would surface."""
        client = make_client()
        catalog_uuid, branch = _new_catalog(client)
        sys_uuid = system_schemas_table_uuid(catalog_uuid)

        client.create_schema(
            "s_first", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        snap1 = _snapshot_system_table(client, catalog_uuid, branch, sys_uuid)
        _assert_name_index_fully_built(catalog_uuid, branch, snap1, sys_uuid)

        client.create_schema(
            "s_second", catalog_uuid=catalog_uuid, author="t", comment="sysidx"
        )
        snap2 = _snapshot_system_table(client, catalog_uuid, branch, sys_uuid)
        assert snap2 != snap1, "second snapshot must be a distinct snapshot"
        _assert_name_index_fully_built(catalog_uuid, branch, snap2, sys_uuid)
