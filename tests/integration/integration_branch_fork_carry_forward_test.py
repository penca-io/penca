"""CHA-531 red tests: a fork's FIRST snapshot must carry the parent's
untouched partitions by reference instead of re-materializing the whole
table.

Committed RED. Today ``compute_snapshot_window`` resolves the prior
snapshot branch-scoped, so a fork sees no prior baseline,
``carry_forward_keys`` bails on the empty segment set, and the CHA-404
full-rewrite path re-writes every partition under the child's own
prefix — O(table) bytes per fork.

**Row materialization is the contract, not an incidental query detail.**
The child's first snapshot must insert its OWN ``branch_uuid``-scoped
segment rows that point at the parent's ``object_uri``; it must not rely
on staying reachable through the single-base cold pointer alone. The
assertions below are written against that mechanism deliberately. A
snapshot is a self-contained materialization of the table as of its
watermark — that is what lets the parent retire or rewrite its own
snapshot independently, and what gives the refcounted GC (CHA-405) a row
to count. Leaving the child dependent on the parent's live snapshot
would be byte-equivalent on day one and would break both properties.

Run via ``just integration-test branch_fork_carry_forward``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
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


def _storage_tuples(catalog_uuid, branch_uuid, snapshot_uuid):
    """``{(object_uri, offset, length, row_count), ...}`` for one
    snapshot's committed segment rows, scoped to ``branch_uuid``.

    Mirrors ``_select_snapshot_segment_storage_tuples`` in
    ``integration_lifecycle_test.py``. The storage tuple is
    carry-forward's sharing identity: a partition carried by REFERENCE
    yields a new segment row under the new snapshot pointing at the
    prior file verbatim, so the tuple is shared. A rewritten partition
    can never share one, because snapshot file uris embed the writing
    snapshot's uuid.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            'SELECT seg.object_uri, seg."offset", seg.length, seg.row_count'
            " FROM {seg} seg"
            " WHERE seg.branch_uuid = %s"
            "   AND seg.table_snapshot_uuid = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent)),
        (branch_uuid, snapshot_uuid),
    )
    return {(r[0], r[1], r[2], r[3]) for r in rows}


def _make_env():
    """Catalog + schema + partitioned table on ``main``."""
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"fcf_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "fcf_schema",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    )
    table_uuid = client.create_table(
        "fcf_table",
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
    """One write cycle on a branch: mutate → commit → persist → snapshot.

    Returns the resulting snapshot uuid.
    """
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
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


def _read_pairs(client, *, catalog_uuid, schema_uuid, table_uuid, branch_uuid):
    """Sorted ``[(name, value), ...]`` visible on a branch.

    A list, not a dict: duplicated rows are the most likely failure mode
    of a first-cut carry-forward (a partition both carried by reference
    AND rewritten), and ``dict`` would silently collapse them into a
    passing result.
    """
    table = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    names = table.column("name").to_pylist()
    values = table.column("value").to_pylist()
    return sorted(zip(names, values, strict=True))


# ── Tests ─────────────────────────────────────────────────────────────


def test_fork_first_snapshot_carries_untouched_partitions_by_reference():
    """The child touches only partition ``alice``: ``bob``'s and
    ``carol``'s parent-snapshot tuples must reappear VERBATIM under the
    child's first snapshot (carried by reference), while ``alice`` lands
    in a fresh file under the child's own prefix.

    Asserts on ``object_uri`` values AND on cardinality: a full-rewrite
    child produces the right COUNT of rows while sharing none of the
    parent's files, and a carry-and-also-rewrite child shares the files
    while still paying O(table) bytes. Only both together exclude both.
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
    parent_tuples = _storage_tuples(catalog_uuid, main_branch, snap_main)

    # Small partitions pack into ONE file in label order, so within the
    # parent's single file the offset IS the label rank (alice=0, bob=1,
    # carol=2) — the attribution the carried/rewritten split below
    # relies on. Pinned here because if packing ever drifts to three
    # files (every offset 0), the offset-keyed sets silently invert.
    assert len({t[0] for t in parent_tuples}) == 1, (
        f"three tiny partitions must share one packed file: {sorted(parent_tuples)}"
    )
    assert {t[1] for t in parent_tuples} == {0, 1, 2}, (
        f"offsets must be the label ranks 0/1/2: {sorted(parent_tuples)}"
    )

    child_branch = client.create_branch(
        f"fcf_child_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    snap_child = _cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )
    child_tuples = _storage_tuples(catalog_uuid, child_branch, snap_child)

    # (i) Untouched partitions carried by reference: bob and carol are
    # every parent tuple except alice's (offset 0).
    expected_carried = {t for t in parent_tuples if t[1] != 0}
    carried = parent_tuples & child_tuples
    assert carried == expected_carried, (
        "expected the child's snapshot to reference the parent's segments for"
        f" untouched partitions (bob, carol), got {sorted(carried)};"
        f" parent {sorted(parent_tuples)} vs child {sorted(child_tuples)}"
    )

    # (ii) The touched partition is rewritten into a fresh file under
    # the child's own prefix.
    rewritten = child_tuples - parent_tuples
    for uri, _offset, _length, _rows in rewritten:
        assert child_branch in uri, (
            f"rewritten uri must sit under the child's prefix: {uri}"
        )

    # (iii) Cardinality. Without this the arms above are satisfied by an
    # implementation that carries bob/carol by reference AND ALSO
    # rewrites them under the child's prefix: the intersection in (i) is
    # unchanged, the extra tuples in (ii) all sit under the child prefix,
    # and (iv)'s read is order-insensitive. That is exactly the "shares
    # files but still pays O(table) bytes" outcome this test exists to
    # catch, so pin the counts, not just the sets.
    assert len(child_tuples) == 3, (
        "the child's snapshot must hold exactly three segment rows (bob and"
        " carol carried, alice rewritten); extra rows mean untouched"
        f" partitions were re-materialized as well: {sorted(child_tuples)}"
    )
    assert len(rewritten) == 1, (
        "exactly one partition (alice) was touched, so exactly one segment"
        f" row may be freshly written: {sorted(rewritten)}"
    )

    # (iv) Content: the child sees its own alice plus the inherited
    # bob/carol. This arm passes today (the read path already folds the
    # base cold source) — it guards against a carry-forward that shares
    # files but loses rows.
    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
    ) == [("alice", 99), ("bob", 2), ("carol", 3)]

    # (v) The parent is untouched by the child's snapshot.
    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
    ) == [("alice", 1), ("bob", 2), ("carol", 3)]
