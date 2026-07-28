"""Acceptance tests for CHA-405 ref-counted cold-segment GC and the
CHA-468 snapshot/retirement decoupling (ADR 0024 §4).

Snapshots are a read-optimization cache, not the history mechanism.
CHA-468 split snapshot-metadata retirement out of the Snapshot op and
disabled it by default: a Snapshot commit now only materialises a
baseline and never retires prior snapshots, so an open (RYOW) tx keeps a
usable baseline instead of falling back to the slower cold persist-log
reconstruction. Retirement is a separate, disabled-by-default op; the
open-tx-safe two-baseline retention that re-enables it lands in CHA-55.
Bounded time-travel history still belongs to persist-log retention
(CHA-425); the persist log is never GC'd here, so ``as_of`` reads stay
correct.

What this file still pins:

1. A Snapshot commit does NOT retire prior snapshots — every committed
   ``table_snapshot_metadata`` row survives and no file is enqueued in
   ``segment_delete_set`` — pinned by
   ``test_snapshot_commit_does_not_retire_prior_snapshots``.
2. ``sweep_segments`` deletes a file only at reference count zero —
   pinned by ``test_sweep_refcount_gates_physical_delete``, which
   simulates the retirement enqueue + reference-drop via direct SQL, so
   it is independent of the removed snapshot->retire trigger.

The two end-to-end suites that drove the ref-counted GC through the REAL
retirement trigger (``TestSharedUriGraceClock``, ``TestCarryForwardGc``)
are skipped: that trigger is gone until CHA-55's PruneSnapshotSegments
RPC re-introduces it, at which point their enqueue / deterministic
re-enqueue coverage returns.

All tests share the integration suite's ``QUERY_TIMEOUT_SECONDS=2``
grace override from ``docker/test.env``. The lifecycle scheduler is
disabled in the test profile
(``SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS=-1``),
so snapshot counts are fully test-driven.

Run via ``just integration-test snapshot_gc``.
"""

from __future__ import annotations

import os
import time
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.naming import (
    SEGMENT_DELETE_SET,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    MAX_SEGMENT_BYTES,
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    setup_schema,
)

# ── Constants ─────────────────────────────────────────────────────────

QUERY_TIMEOUT_SECONDS = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "2"))
GRACE_EPSILON_SECONDS = 1.0
GRACE_WAIT_SECONDS = QUERY_TIMEOUT_SECONDS + GRACE_EPSILON_SECONDS


# ── Helpers ───────────────────────────────────────────────────────────


def _make_branch(client, catalog_uuid, name):
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-405",
    )
    return branch.branch_uuid


def _commit_tx_writing_rows(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows
):
    """Begin a tx, write ``rows`` to ``table_uuid``, commit. Return
    ``commit_micros``."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    committed = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    return committed.commit_micros


def _persist_then_snapshot(client, catalog_uuid, schema_uuid, branch_uuid, table_uuid):
    """Persist the hot tier then snapshot; assert + return the new
    watermark. The shared tail of every write/persist/snapshot cycle
    helper in this module (CHA-406 carry-forward suites included)."""
    client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    snap = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    assert snap.snapshotted_at_micros, (
        "snapshot must produce a fresh watermark — new persist data arrived"
    )
    return snap.snapshotted_at_micros


def _write_persist_snapshot_cycle(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, cycle_idx
):
    """One full write -> persist -> snapshot cycle with a distinct row.

    Returns the new snapshot watermark (``snapshotted_at_micros``)."""
    _commit_tx_writing_rows(
        client,
        catalog_uuid,
        schema_uuid,
        branch_uuid,
        table_uuid,
        {"name": [f"r_{cycle_idx}"], "value": [cycle_idx]},
    )
    return _persist_then_snapshot(
        client, catalog_uuid, schema_uuid, branch_uuid, table_uuid
    )


def _committed_snapshots(catalog_uuid, branch_uuid, table_uuid):
    """``[(table_snapshot_uuid, snapshotted_at_micros)]`` committed,
    newest first."""
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT table_snapshot_uuid::text, snapshotted_at_micros"
            " FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
            " ORDER BY snapshotted_at_micros DESC"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )


def _segment_uris_for_snapshot(catalog_uuid, branch_uuid, snapshot_uuid):
    """Distinct ``object_uri`` set of one snapshot's segment rows."""
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT DISTINCT object_uri FROM {tbl}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )
    return sorted(r[0] for r in rows)


def _delete_set_rows_for_uris(catalog_uuid, branch_uuid, uris):
    """``[(object_uri, written_at_micros)]`` delete-set rows whose
    ``object_uri`` is in ``uris``."""
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    return get_pg_driver().execute(
        SQL(
            "SELECT object_uri, written_at_micros FROM {tbl}"
            " WHERE branch_uuid = %s AND object_uri = ANY(%s)"
        ).format(tbl=Identifier(tbl)),
        (branch_uuid, list(uris)),
    )


def _insert_delete_set_row(catalog_uuid, branch_uuid, table_uuid, uri, written_at):
    """Manually enqueue ``uri`` for GC, simulating a retirement enqueue
    (uuid4 PK is fine — the PK is ``(branch_uuid, segment_delete_uuid)``)."""
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


def _rewind_delete_set_rows(catalog_uuid, branch_uuid, uris, written_at):
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    get_pg_driver().execute_no_result(
        SQL(
            "UPDATE {tbl} SET written_at_micros = %s"
            " WHERE branch_uuid = %s AND object_uri = ANY(%s)"
        ).format(tbl=Identifier(tbl)),
        (written_at, branch_uuid, list(uris)),
    )


def _now_micros():
    return int(time.time() * 1_000_000)


def _segment_chunk_uris(catalog_uuid, branch_uuid, snapshot_uuid):
    """``[(chunk_idx, object_uri)]`` of one snapshot's segment rows in
    chunk order. The snapshot writer packs partitions in label order,
    so the first chunk's uri belongs to the label-smallest partition."""
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT chunk_idx, object_uri FROM {tbl}"
            " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            " ORDER BY chunk_idx"
        ).format(tbl=Identifier(seg)),
        (branch_uuid, snapshot_uuid),
    )


# ── Tests ─────────────────────────────────────────────────────────────


class TestSnapshotNoRetirement:
    """CHA-468 — a Snapshot commit only materialises a baseline; it no
    longer retires prior snapshots. Retirement is a separate,
    disabled-by-default op (re-enabled with the open-tx-safe two-baseline
    policy in CHA-55), so snapshots accumulate and open-tx (RYOW) reads
    keep their baseline instead of falling back to the persist log."""

    def test_snapshot_commit_does_not_retire_prior_snapshots(self):
        client = make_client()
        schema_uuid, _t, catalog_uuid, _main = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "no_retire")
        table_uuid = create_table_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        watermarks = []
        snapshot_uris = {}
        for cycle_idx in range(3):
            watermark = _write_persist_snapshot_cycle(
                client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, cycle_idx
            )
            watermarks.append(watermark)
            committed = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)
            latest_snapshot_uuid = committed[0][0]
            snapshot_uris[watermark] = _segment_uris_for_snapshot(
                catalog_uuid, branch_uuid, latest_snapshot_uuid
            )

        committed = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)
        assert len(committed) == 3, (
            "a Snapshot commit must NOT retire prior snapshots (CHA-468) — "
            f"expected all 3 committed table_snapshot_metadata rows to survive, "
            f"got {len(committed)}. Retirement is a separate disabled op (CHA-55)."
        )
        assert committed[0][1] == watermarks[-1], (
            "the newest snapshot must be the latest committed — "
            f"head watermark {committed[0][1]}, latest cycle {watermarks[-1]}"
        )

        # No prior-snapshot file is enqueued for GC: Snapshot does not retire,
        # and only retirement enqueues prior-snapshot files in
        # segment_delete_set.
        prior_uris = sorted(
            set(snapshot_uris[watermarks[0]]) | set(snapshot_uris[watermarks[1]])
        )
        enqueued = {
            row[0]
            for row in _delete_set_rows_for_uris(catalog_uuid, branch_uuid, prior_uris)
        }
        assert not enqueued, (
            "no prior-snapshot file may be enqueued in segment_delete_set when "
            f"Snapshot no longer retires (CHA-468); got {sorted(enqueued)}"
        )

        # The latest snapshot still serves reads.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert result.num_rows == 3, (
            f"latest read must see all 3 rows; got {result.num_rows}"
        )

        # Time travel at the first cycle's watermark stays correct — now served
        # by the still-present first snapshot rather than a persist-log fallback.
        as_of_cycle_1 = micros_to_datetime(watermarks[0])
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
            as_of=as_of_cycle_1,
        )
        assert result.num_rows == 1, (
            "as_of at the first cycle's watermark must return exactly the first "
            f"row; got {result.num_rows}"
        )


class TestSweepRefcount:
    """ADR 0024 §4 — ``sweep_segments`` must not physically delete an
    ``object_uri`` while any live ``table_snapshot_segment_metadata``
    row references it; deletes only at reference count zero."""

    def test_sweep_refcount_gates_physical_delete(self):
        client = make_client()
        schema_uuid, _t, catalog_uuid, _main = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "refcount")
        table_uuid = create_table_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        _write_persist_snapshot_cycle(
            client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, 0
        )
        committed = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)
        snapshot_uuid = committed[0][0]
        uris = _segment_uris_for_snapshot(catalog_uuid, branch_uuid, snapshot_uuid)
        assert uris, "snapshot must have written at least one segment file"

        # Simulate a retirement enqueue, already past the grace window.
        past_grace = _now_micros() - 10_000_000
        for uri in uris:
            _insert_delete_set_row(
                catalog_uuid, branch_uuid, table_uuid, uri, past_grace
            )

        # Phase 1: the snapshot's live segment rows still reference the
        # uris — sweep must skip them despite the elapsed grace window.
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        remaining = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, uris)
        assert len(remaining) == len(uris), (
            "sweep must not delete an object_uri referenced by a live "
            f"table_snapshot_segment_metadata row — {len(uris)} rows enqueued, "
            f"{len(remaining)} remain. ADR 0024 §4: delete only at reference "
            "count zero."
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert result.num_rows == 1, (
            f"the referenced snapshot file must be intact; got {result.num_rows} rows"
        )

        # Phase 2: drop the references (as retirement would), refcount
        # hits zero — sweep deletes the file then the set row.
        seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
        parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
        get_pg_driver().execute_no_result(
            SQL(
                "DELETE FROM {tbl} WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(seg)),
            (branch_uuid, snapshot_uuid),
        )
        get_pg_driver().execute_no_result(
            SQL(
                "DELETE FROM {tbl} WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(parent)),
            (branch_uuid, snapshot_uuid),
        )
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        remaining = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, uris)
        assert not remaining, (
            "at reference count zero past grace, sweep must delete the file "
            f"and drain the set row; {len(remaining)} rows remain"
        )


@pytest.mark.skip(
    reason="CHA-468 removed the snapshot->retire trigger; real-retirement "
    "enqueue / deterministic re-enqueue GC coverage returns with the "
    "PruneSnapshotSegments RPC (CHA-55)."
)
class TestSharedUriGraceClock:
    """ADR 0024 §4 + ADR 0019 — shared-file retirement end to end. A
    retired-but-still-referenced file survives sweep, and the grace
    clock restarts at the LAST reference drop: retirement re-enqueues
    onto the same deterministic ``segment_delete_uuid`` row, refreshing
    ``written_at_micros``."""

    def test_shared_uri_grace_clock_restarts_at_last_reference_drop(self):
        client = make_client()
        schema_uuid, _t, catalog_uuid, _main = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "shared_uri")
        # CHA-406: a partitioned table (partition ⊆ PK) so a cycle that
        # touches a strict subset carries the untouched partition's file
        # by reference — the real path that makes one physical file
        # referenced by successive snapshots.
        table_uuid = client.create_table(
            "cf_grace_table",
            USER_SCHEMA,
            primary_keys=["name"],
            partition_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-406",
        )

        def cycle(rows):
            _commit_tx_writing_rows(
                client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows
            )
            return _persist_then_snapshot(
                client, catalog_uuid, schema_uuid, branch_uuid, table_uuid
            )

        # Cycle 1: partitions a and b -> snapshot A. Partition b stays
        # untouched through cycles 2-3, so the file holding it is shared
        # by reference across the carried-forward snapshots.
        cycle({"name": ["a", "b"], "value": [1, 2]})
        snapshot_a_uuid = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)[
            0
        ][0]
        shared_uris = _segment_uris_for_snapshot(
            catalog_uuid, branch_uuid, snapshot_a_uuid
        )
        # The grace-clock assertions below track "the file holding b",
        # but shared_uris spans all of A's files — the two coincide only
        # because partitions a and b (one row each) co-locate in ONE
        # packed file (small_partitions_share_one_file). Pin that
        # assumption explicitly: if packing ever splits them, cycle 2
        # would rewrite a's file to refcount 0 and the far-past sweep
        # would delete it, failing the "still referenced" assertions
        # with a misleading message. One file = one shared uri.
        assert len(shared_uris) == 1, (
            "partitions a and b must co-locate in one packed file for this"
            f" test's shared-uri tracking; got {sorted(shared_uris)}"
        )

        # Cycle 2: touch ONLY a -> snapshot B carries b by reference.
        # Retirement retires A and enqueues its files; the shared file
        # is still referenced by B's carried-b row.
        cycle({"name": ["a"], "value": [10]})
        enqueued = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, shared_uris)
        assert len(enqueued) == len(shared_uris), (
            "retiring snapshot A must enqueue its files in segment_delete_set "
            f"— expected {len(shared_uris)} rows, got {len(enqueued)}. "
            "ADR 0024 §4: retirement drops metadata rows and defers file "
            "deletion to the ref-counted sweep."
        )

        # Far-past grace clock: only the refcount gate protects the
        # shared file now.
        long_ago = _now_micros() - 3_600_000_000
        _rewind_delete_set_rows(catalog_uuid, branch_uuid, shared_uris, long_ago)
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        remaining = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, shared_uris)
        assert len(remaining) == len(shared_uris), (
            "sweep must skip uris still referenced by snapshot B's carried-b "
            f"row — {len(remaining)} of {len(shared_uris)} remain"
        )

        # Cycle 3: touch ONLY a again -> snapshot C carries b by
        # reference AGAIN. Retirement retires B and RE-enqueues the
        # shared file onto the SAME deterministic segment_delete_uuid
        # row, refreshing written_at_micros — the grace clock restarts
        # at the last reference drop.
        cycle({"name": ["a"], "value": [11]})
        refreshed = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, shared_uris)
        assert len(refreshed) == len(shared_uris), (
            "the shared uris must still be enqueued after snapshot B's "
            f"retirement; got {len(refreshed)} of {len(shared_uris)}"
        )
        stale = [row for row in refreshed if row[1] <= long_ago]
        assert not stale, (
            "retirement re-enqueue must REFRESH written_at_micros on the "
            "existing delete-set row (grace restarts at the last reference "
            f"drop); stale rows: {stale}. Without the refresh a shared file "
            "is swept inside the query-timeout window of plans pinned to the "
            "just-retired snapshot."
        )

        # Within the refreshed grace window the rows survive. The
        # refresh itself is already pinned by the `stale` assertion
        # above, so deterministically re-stamp the clock to "now"
        # rather than racing the 2s window against RPC/PG latency since
        # the cycle-3 snapshot stamped it server-side.
        _rewind_delete_set_rows(catalog_uuid, branch_uuid, shared_uris, _now_micros())
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        remaining = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, shared_uris)
        assert len(remaining) == len(shared_uris), (
            "sweep within the refreshed grace window must not delete — "
            f"{len(remaining)} of {len(shared_uris)} remain"
        )

        # Cycle 4: touch b -> snapshot D rewrites b and stops carrying
        # it, so C's retirement drops the LAST reference to the shared
        # file. Past grace at reference count zero, the sweep drains it.
        cycle({"name": ["b"], "value": [20]})
        time.sleep(GRACE_WAIT_SECONDS)
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        remaining = _delete_set_rows_for_uris(catalog_uuid, branch_uuid, shared_uris)
        assert not remaining, (
            "past grace at reference count zero, sweep must drain the shared "
            f"uris; {len(remaining)} rows remain"
        )


@pytest.mark.skip(
    reason="CHA-468 removed the snapshot->retire trigger; real carry-forward "
    "retirement GC coverage returns with the PruneSnapshotSegments RPC (CHA-55)."
)
class TestCarryForwardGc:
    """CHA-406 red test (ADR 0024 §4): a carried-forward shared file
    survives sweep until the LAST referencing snapshot retires.

    ``TestSharedUriGraceClock`` above pins the same GC mechanics through
    a hand-inserted carried-row fixture; this test drives them through
    REAL carry-forward snapshots, so it is committed RED — today every
    snapshot fully rewrites every partition (CHA-404) and the step-2
    positive control (B carries A's untouched-partition file by
    reference) fails.

    Layout note: the table uses ``primary_keys=["name", "value"]`` with
    ``partition_keys=["name"]`` — partition ⊆ PK keeps it
    carry-forward-eligible while letting one partition hold many rows.
    (With ``primary_keys=["name"]`` a partition is a single ~30-byte
    row, which could never fill a segment file of its own; this test
    needs partitions a and b in SEPARATE files so the sweep's refcount
    discrimination between a still-referenced file and a dead sibling
    file is observable.)
    """

    # Per-partition row count sized so each partition's post-merge
    # in-memory footprint lands in (cap/2, cap]: big enough that two
    # partitions never pack into one file, small enough that one
    # partition never splits. The chunker's cost model is deterministic
    # (chunker.rs: Utf8 = len + 5, fixed-width Int64 = 8 + 1), and the
    # merged snapshot batch is row_uuid (36-char Utf8 → 41) + name
    # (1-char Utf8 → 6) + value (Int64 → 9) = 56 bytes/row. Targeting
    # 0.7 × cap lands genuinely mid-window.
    _PER_ROW_BYTES = 56
    _ROWS_PER_PARTITION = MAX_SEGMENT_BYTES * 7 // (10 * _PER_ROW_BYTES)

    def _create_partitioned_table(self, client, catalog_uuid, schema_uuid, branch_uuid):
        return client.create_table(
            "cf_gc_table",
            USER_SCHEMA,
            primary_keys=["name", "value"],
            partition_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-406",
        )

    def _cycle(self, client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows):
        """write → persist → snapshot, with caller-controlled
        partitioned rows."""
        _commit_tx_writing_rows(
            client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows
        )
        return _persist_then_snapshot(
            client, catalog_uuid, schema_uuid, branch_uuid, table_uuid
        )

    def test_carry_forward_shared_file_survives_sweep_until_last_reference(self):
        client = make_client()
        schema_uuid, _t, catalog_uuid, _main = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "cf_gc")
        table_uuid = self._create_partitioned_table(
            client, catalog_uuid, schema_uuid, branch_uuid
        )
        n = self._ROWS_PER_PARTITION

        # 1. Cycle 1: partitions a and b, each sized into its own file
        #    → snapshot A.
        self._cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_uuid,
            {"name": ["a"] * n + ["b"] * n, "value": list(range(n)) * 2},
        )
        snapshot_a_uuid = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)[
            0
        ][0]
        chunks_a = _segment_chunk_uris(catalog_uuid, branch_uuid, snapshot_a_uuid)
        uris_a = {uri for _, uri in chunks_a}
        assert len(chunks_a) == 2 and len(uris_a) == 2, (
            f"partitions a and b must land in separate single files: {chunks_a}"
        )
        a_uri = chunks_a[0][1]  # label order: a is the first chunk
        b_uri = chunks_a[1][1]

        # 2. Cycle 2 touches ONLY a → snapshot B commits; retirement
        #    retires A and enqueues A's files.
        self._cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_uuid,
            {"name": ["a"], "value": [0]},
        )
        snapshot_b_uuid = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)[
            0
        ][0]
        assert snapshot_b_uuid != snapshot_a_uuid
        uris_b = set(
            _segment_uris_for_snapshot(catalog_uuid, branch_uuid, snapshot_b_uuid)
        )
        # Positive control (RED today): B carries b's file by reference.
        assert b_uri in uris_b, (
            "expected snapshot B to carry the untouched partition b's file"
            f" by reference; B references {sorted(uris_b)}"
        )
        assert a_uri not in uris_b, "the touched partition a must be rewritten"
        # B's freshly-written a file — carried by C in step 4, so it
        # exercises a SECOND-generation carried reference blocking sweep.
        a_uri_b = (uris_b - {b_uri}).pop()
        enqueued = {
            row[0]
            for row in _delete_set_rows_for_uris(
                catalog_uuid, branch_uuid, [a_uri, b_uri]
            )
        }
        assert enqueued == {a_uri, b_uri}, (
            f"retiring A must enqueue both of its files; got {sorted(enqueued)}"
        )

        # 3. Far-past grace: only the refcount gate protects b's file.
        #    Sweep deletes the dead a file and keeps the shared b file.
        long_ago = _now_micros() - 3_600_000_000
        _rewind_delete_set_rows(catalog_uuid, branch_uuid, [a_uri, b_uri], long_ago)
        client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        remaining = {
            row[0]
            for row in _delete_set_rows_for_uris(
                catalog_uuid, branch_uuid, [a_uri, b_uri]
            )
        }
        assert remaining == {b_uri}, (
            "sweep must drain the unreferenced a file and keep the b file"
            f" still referenced by B's carried row; remaining {sorted(remaining)}"
        )

        # 4. Cycle 3 touches b → snapshot C rewrites b and carries a
        #    (B's rewritten-a file) forward. B retires: the LAST
        #    reference to A's b file drops (C rewrote b), but B's a file
        #    is now carried by C — a second-generation carried reference
        #    that must keep it alive while b_uri drains.
        self._cycle(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_uuid,
            {"name": ["b"], "value": [0]},
        )
        snapshot_c_uuid = _committed_snapshots(catalog_uuid, branch_uuid, table_uuid)[
            0
        ][0]
        assert a_uri_b in set(
            _segment_uris_for_snapshot(catalog_uuid, branch_uuid, snapshot_c_uuid)
        ), "snapshot C must carry B's a file forward (second-generation reference)"
        _rewind_delete_set_rows(catalog_uuid, branch_uuid, [b_uri, a_uri_b], long_ago)
        client.sweep_segments(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        remaining = {
            row[0]
            for row in _delete_set_rows_for_uris(
                catalog_uuid, branch_uuid, [b_uri, a_uri_b]
            )
        }
        assert remaining == {a_uri_b}, (
            "b_uri's last reference dropped so it must sweep, while B's a"
            " file survives via C's second-generation carried reference;"
            f" remaining {sorted(remaining)}"
        )

        # Read sanity through C's mixed carried + rewritten layout.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert result.num_rows == 2 * n, (
            f"expected {2 * n} rows through the carried+rewritten layout;"
            f" got {result.num_rows}"
        )
