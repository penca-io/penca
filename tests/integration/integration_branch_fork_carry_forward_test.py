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
from penca_client._time import micros_to_datetime
from penca_client.naming import (
    TABLE_PERSIST_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    table_snapshot_uuid,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    setup_partitioned_table,
    write_and_persist,
    write_cycle,
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


def _segment_row_count(catalog_uuid, branch_uuid, snapshot_uuid):
    """Number of committed segment ROWS under one snapshot.

    ``_storage_tuples`` returns a *set*, so two carried rows for the same
    slice — distinct ``table_snapshot_segment_uuid``, identical storage
    columns, which is exactly the shape ``insert_carried_snapshot_
    segments`` produces — collapse into one element and a set-cardinality
    assertion cannot see the duplicate. The read path dedupes on primary
    key, so the content arm cannot see it either. Counting rows is the
    only arm that excludes a double-carry.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {seg}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent)),
        (branch_uuid, snapshot_uuid),
    )
    return int(rows[0][0])


def _snapshot_seq_watermark(catalog_uuid, branch_uuid, snapshot_uuid):
    """``W_snap`` — the snapshot's ``commit_seq_num``, i.e. the seq up to
    which the snapshot claims to describe the table.

    The read planner's base-cold gate for a fork is
    ``child_snapshot_seq < fork_commit_seq_num``, so this column is what
    decides whether a fork ever stops folding its parent's cold tier.
    """
    snap_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT commit_seq_num FROM {snap}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(snap=Identifier(snap_parent)),
        (branch_uuid, snapshot_uuid),
    )
    assert len(rows) == 1, f"expected one committed snapshot row, got {rows}"
    return int(rows[0][0])


def _persist_watermark(catalog_uuid, branch_uuid, table_uuid):
    """Latest committed ``persisted_at_micros`` for one branch's table —
    the same value ``compute_snapshot_window`` bounds a snapshot by.

    Used as a SERVER-clock anchor for an ``as_of`` snapshot: comparing a
    client-side ``datetime.now()`` against server-stamped watermarks
    would make the window bound race the two clocks.
    """
    persist_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(persisted_at_micros) FROM {tpm}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tpm=Identifier(persist_parent)),
        (branch_uuid, table_uuid),
    )
    return int(rows[0][0])


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
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fcf")
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

    snap_child = write_cycle(
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
        "the child's snapshot must hold exactly three storage slices (bob and"
        " carol carried, alice rewritten); extra slices mean untouched"
        f" partitions were re-materialized as well: {sorted(child_tuples)}"
    )
    assert _segment_row_count(catalog_uuid, child_branch, snap_child) == 3, (
        "the child's snapshot must hold exactly three segment ROWS. The set"
        " assertion above cannot see a partition that is carried twice (two"
        " rows, one storage slice), which is the one row-cardinality defect a"
        " first-cut carry-forward is most likely to produce."
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


def test_fork_first_snapshot_folds_parents_unsnapshotted_persist_tail():
    """The parent persists ``dave`` WITHOUT snapshotting, then the fork is
    taken. ``dave`` sits in neither the carried baseline (the parent's
    older snapshot) nor the child's own persist log, so the child's first
    snapshot must fold the parent's post-snapshot tail into its delta or
    the row is silently lost.

    This is the correctness hole that carrying the parent's snapshot as a
    baseline opens: before the change the child took the full-rewrite
    path, whose ``base_cold_storage`` fold picked the tail up for free.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fcf")
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
    parent_tuples = _storage_tuples(catalog_uuid, main_branch, snap_main)
    # The tail: committed and persisted on the parent, never snapshotted.
    # `dave` is a new partition; `bob` is an update to one the parent's
    # snapshot already holds, so the two together cover both ways a tail
    # can interact with the carried baseline.
    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": ["dave", "bob"], "value": [4, 20]}, schema=USER_SCHEMA
        ),
    )

    child_branch = client.create_branch(
        f"fcf_tail_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    snap_child = write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )
    child_tuples = _storage_tuples(catalog_uuid, child_branch, snap_child)

    # The carry contract still holds with a tail present, and this is the
    # only arm that pins it: the read assertions below pass just as well
    # if the tail fold drags the WHOLE parent table into `delta_groups`
    # and every partition is rewritten (which is what dropping
    # `base.cold.snapshot = None` in `snapshot_op` would do). `carol` is
    # the one partition neither branch touched, so it alone stays
    # carried; `alice` (child write) and `bob` (tail update) are
    # rewritten even though `bob` is only touched via the parent.
    carried = parent_tuples & child_tuples
    assert carried == {t for t in parent_tuples if t[1] == 2}, (
        "only the partition untouched by both the child and the parent's"
        f" tail may stay carried; parent {sorted(parent_tuples)} vs child"
        f" {sorted(child_tuples)}"
    )
    assert _segment_row_count(catalog_uuid, child_branch, snap_child) == 4, (
        "the child's snapshot must hold one row per partition: carol"
        " carried, alice/bob/dave rewritten"
    )

    # Read AFTER the child's own snapshot: the child's baseline now covers
    # the fork, so the read path stops folding the parent's cold source
    # (the CHA-178 read gate) and the answer comes from the child's
    # snapshot alone. That is what makes this a snapshot-write assertion
    # rather than a read-path one.
    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
    ) == [("alice", 99), ("bob", 20), ("carol", 3), ("dave", 4)], (
        "the child's first snapshot dropped the parent's persisted-but-not-"
        "snapshotted rows: they are in neither the carried baseline nor the"
        " child's own persist log, so the fork's delta must fold the parent's"
        " post-snapshot tail (seq-capped at the fork edge)."
    )

    # The parent keeps its own view — the fork neither snapshotted on its
    # behalf nor consumed its tail.
    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
    ) == [("alice", 1), ("bob", 20), ("carol", 3), ("dave", 4)]


def test_fork_first_snapshot_applies_parents_tail_delete():
    """A row the parent DELETES in its post-snapshot tail must not
    resurrect through a partition the child carries by reference.

    The delete half of the tail fold, and it fails for a different reason
    than the upsert half. Folding the parent's tail in as a base cold
    source runs ``filter_live_rows`` over it, so the parent's tombstone
    never reaches the resolved delta and never marks ``bob``'s partition
    touched. The partition is then carried verbatim from the parent's
    snapshot — which still holds the live ``bob`` — and the delete is
    undone. The tombstone reaches ``exclusion_set``, but that set is only
    applied to segments the writer restreams, never to a carried one.

    So the writer has to read the base's delete segments directly for
    touched-set attribution. This test is what pins that.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fcf")
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
    parent_tuples = _storage_tuples(catalog_uuid, main_branch, snap_main)

    # The tail: a delete only, committed and persisted, never snapshotted.
    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        deletes=pa.table({"name": ["bob"]}, schema=pa.schema([("name", pa.utf8())])),
    )

    child_branch = client.create_branch(
        f"fcf_del_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    ).branch_uuid

    snap_child = write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )
    child_tuples = _storage_tuples(catalog_uuid, child_branch, snap_child)

    # bob's partition (offset 1) must NOT be carried: the parent's tail
    # deleted the only row in it, so it has to be rewritten (or dropped).
    # Only carol (offset 2) is untouched by both branches.
    carried = parent_tuples & child_tuples
    assert carried == {t for t in parent_tuples if t[1] == 2}, (
        "a partition whose only row the parent's tail deleted was carried by"
        " reference from a snapshot that still holds that row; parent"
        f" {sorted(parent_tuples)} vs child {sorted(child_tuples)}"
    )

    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
    ) == [("alice", 99), ("carol", 3)], "the parent's tail delete resurrected"

    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
    ) == [("alice", 1), ("carol", 3)]


def test_fork_first_snapshot_watermark_covers_the_fork_point():
    """A fork's first snapshot materializes the parent through the fork
    edge, so its ``W_snap`` must be at least ``fork_commit_seq_num`` —
    even when the child's own persist log contributes no seq to the fold.

    ``W_snap`` folds the previous baseline with the max seq of the persist
    segments this snapshot consumes, and that max is windowed over the
    CHILD's log. The parent's carried baseline and folded tail both sit
    at or below the fork edge on a seq axis the child's log never covers,
    so with nothing of the child's own in the window the fold bases at the
    parent's (older) watermark and the snapshot understates what it holds.

    The child's commit seqs are seeded one past the fork edge, so the
    usual write-then-snapshot ordering hides this: the child's own segment
    always drags the fold above the fork point. Snapshotting ``as_of`` a
    micros bound BEFORE the child's persist is what isolates it — the
    child's segment leaves the window while the parent's baseline and tail
    stay in it. That is a real point-in-time snapshot, not a contrivance:
    the resulting snapshot genuinely holds the parent's table as of the
    fork.

    Understating is not merely conservative. ``meta_plan``'s base-cold
    gate is ``child_snapshot_seq < fork_commit_seq_num``, so a watermark
    short of the fork edge leaves that gate open for the life of the
    branch and every read re-enumerates and re-folds the parent's cold
    tier to dedup it against rows the child's own snapshot already holds.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fcf")
    )

    write_cycle(
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
    # The tail: persisted on the parent, never snapshotted, so the child's
    # first snapshot has real parent rows to fold in above the carried
    # baseline. Without it the snapshot would be a pure carry and the
    # watermark claim would be vacuous.
    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table({"name": ["dave"], "value": [4]}, schema=USER_SCHEMA),
    )
    as_of = _persist_watermark(catalog_uuid, main_branch, table_uuid)

    branch = client.create_branch(
        f"fcf_wm_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-531",
    )
    child_branch = branch.branch_uuid

    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )
    assert _persist_watermark(catalog_uuid, child_branch, table_uuid) > as_of, (
        "the child's persist must land strictly after `as_of` or the snapshot"
        " window still covers the child's own segment and the fold is dragged"
        " above the fork edge for the wrong reason"
    )

    response = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
        snapshot_at=micros_to_datetime(as_of),
    )
    assert response.HasField("snapshotted_at_micros")
    assert response.snapshotted_at_micros == as_of, (
        "the snapshot window must clamp to `as_of`, not to the child's later"
        " persist watermark, or the isolating precondition is gone"
    )
    snap_child = table_snapshot_uuid(
        catalog_uuid, child_branch, table_uuid, response.snapshotted_at_micros
    )

    assert (
        _snapshot_seq_watermark(catalog_uuid, child_branch, snap_child)
        >= branch.fork_commit_seq_num
    ), (
        "the child's first snapshot holds the parent's table as of the fork"
        " edge — carried baseline plus folded tail — so its W_snap must cover"
        f" fork_commit_seq_num={branch.fork_commit_seq_num}. Stamping the"
        " parent's older watermark leaves meta_plan's base-cold gate open"
        " forever."
    )

    # The snapshot is as-of the fork, so the child's own later write is
    # still served from its persist log on top of it.
    assert _read_pairs(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child_branch,
    ) == [("alice", 99), ("bob", 2), ("carol", 3), ("dave", 4)]


# Removed with CHA-539: this pinned the dual-axis clamp in `snapshot_op`, which
# capped a fork's first-snapshot watermark at
# `min(fork_commit_seq_num, resolve_committed_tx(as_of))` so an `as_of` pin below
# a parent commit could not close the base-cold gate over rows the snapshot never
# materialized. A fork no longer adopts its parent's baseline at snapshot time —
# it inherits explicit cold reference rows at CreateBranch — so there is no
# cross-branch fold to clamp and no stranded-commit case to guard. The clamp, its
# `resolve_committed_tx` call and the fold it protected are all deleted.
