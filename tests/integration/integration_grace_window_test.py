"""Acceptance tests for CHA-233 — plan-time pinning + universal grace window.

ADR 0019 ties three orthogonal mechanisms into one system invariant:

> Any ``Plan + Execute`` that completes within ``query_timeout_seconds``
> observes a consistent view of the data.

The user-visible bug surface is silent data loss in ``read_data`` /
``audit_data`` and ``NotFound`` from cold under ``compact``. Those
shapes are *symptoms* of destructive lifecycle ops (Purge of hot
rows, compact's deletion of old cold files) removing state the plan
needs. Rather than racing the symptom — which requires the
plan-vs-execute gap inside a single RPC to be wide enough to land a
concurrent Persist+Purge mid-flight, which it usually isn't in
practice — each test below pins the underlying *mechanism* directly,
single-shot:

1. ``persisted_at_micros`` as the plan cutoff source — pinned by
   ``MetadataClient::plan``'s unit-test surface (commit 2's
   ``compute_snapshot_picker_as_of`` helper) and exercised end-to-end
   here only via the no-double-count properties already in
   ``per_table_lifecycle``.
2. Hot-side ``Purge`` is no longer grace-bounded (CHA-444 / ADR 0027 —
   supersedes the ADR 0019 hot grace): Purge advances the read fence
   ``Pu`` to ``W_snap`` and clears hot immediately, relying on
   early-materialized MVCC reads instead of a wall-clock grace. The
   removed-mechanism tests (``test_purge_no_op_within_grace_window`` /
   ``test_purge_within_grace_leaves_hot_rows_intact``) are gone;
   snapshot-before-purge is pinned in ``integration_per_table_lifecycle_test``.
3. Grace-bounded compaction GC via ``segment_delete_set`` + sweep —
   pinned by ``test_segment_delete_entry_swept_after_grace``.
4. Enforced query runtime cap — pinned by
   ``test_query_exceeds_cap_returns_resource_exhausted``.

The lock-scoping subsection of ADR 0019 narrows the shared
``lifecycle:{table_uuid}:{branch_uuid}`` advisory key into three
per-operation keys (``persist:``, ``snapshot:``, ``purge:``). The
last two tests pre-hold the ADR-0018 shared key from the test side
and assert the cross-operation pairs (``Persist↔Purge``,
``Snapshot↔Purge``) complete unblocked — a direct mechanism probe
of which key each op takes.

All tests share the integration suite's ``QUERY_TIMEOUT_SECONDS=2``
override from ``docker/test.env`` so the cap path is exercised without
making the suite slow. See ADR 0019 §"Defaults" for the prod default
(900) and rationale.

Run via ``just integration-test grace_window``.
"""

from __future__ import annotations

import os
import threading
import time
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import ApiError
from penca_client.naming import upsert_log_table
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    make_lock_driver,
    setup_schema,
)

# ── Constants ─────────────────────────────────────────────────────────

QUERY_TIMEOUT_SECONDS = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "2"))
# Grace pad: extra wall-clock past the cap before re-issuing a
# grace-gated operation. Keeps the tests deterministic against clock
# resolution + small server-side scheduling jitter (the persist commit
# timestamp can be a few ms in the past relative to the test's notion
# of "now"). Tuned to be large enough that the grace window is
# definitively elapsed but small enough that the suite stays fast.
GRACE_EPSILON_SECONDS = 1.0
GRACE_WAIT_SECONDS = QUERY_TIMEOUT_SECONDS + GRACE_EPSILON_SECONDS

TABLE_PERSIST_METADATA = "table_persist_metadata"
TABLE_PURGE_METADATA = "table_purge_metadata"
SEGMENT_DELETE_SET = "segment_delete_set"


# ── Helpers ───────────────────────────────────────────────────────────


def _make_branch(client, catalog_uuid, name):
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-233",
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


def _count_segment_delete_set_rows(catalog_uuid, branch_uuid):
    """Count rows in ``segment_delete_set`` for ``branch_uuid``.

    The table is introduced by ADR 0019 §"Four-part mechanism" item 3.
    Pre-mechanism the table does not exist; this helper raises
    ``psycopg.errors.UndefinedTable`` then — which is the red signal
    ``test_segment_delete_entry_swept_after_grace`` pins on.
    """
    parent = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    rows = get_pg_driver().execute(
        SQL("SELECT count(*) FROM {tbl} WHERE branch_uuid = %s").format(
            tbl=Identifier(parent)
        ),
        (branch_uuid,),
    )
    return rows[0][0]


# ── Tests ─────────────────────────────────────────────────────────────


class TestQueryTimeoutCap:
    """ADR 0019 §"Four-part mechanism" item 4 — ``read_data`` and
    ``audit_data`` cancel exactly at ``T_q + query_timeout`` with
    gRPC ``RESOURCE_EXHAUSTED``. The cap is what closes the grace
    correctness argument."""

    def test_query_exceeds_cap_returns_resource_exhausted(self):
        """A ``read_data`` call that runs past ``query_timeout_seconds``
        must terminate with ``RESOURCE_EXHAUSTED``.

        We block the hot upsert log table at the PG layer for longer
        than the cap so the server-side hot scan stalls. Under ADR
        0019 (green) the ``tokio::time::timeout`` wrapper on the
        ``BatchStream`` fires after ``query_timeout_seconds`` and the
        client gets ``ResourceExhausted`` mapped to a typed
        ``ApiError``. Currently (red): no cap is enforced — the server
        waits out the lock and the client gets a successful (slow)
        response.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "timeout_cap")
        table_uuid = create_table_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # Write rows but DO NOT persist + purge — keep data in hot so
        # the server-side hot SELECT (not cold) is what gets blocked.
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_uuid,
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
        )

        hot_log = upsert_log_table(table_uuid, branch_uuid)
        lock_hold_seconds = QUERY_TIMEOUT_SECONDS + 5.0
        lock_acquired = threading.Event()
        lock_release = threading.Event()

        def hold_lock():
            # ACCESS EXCLUSIVE blocks even SELECT — the server's hot
            # scan will sit waiting until the cap fires or the lock
            # is released. The transaction is held for
            # ``lock_hold_seconds`` so the cap (at ``QUERY_TIMEOUT_SECONDS``)
            # fires comfortably before we let go.
            with get_pg_driver().transaction() as txn:
                txn.execute_no_result(
                    SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                        Identifier(hot_log)
                    )
                )
                lock_acquired.set()
                # Wait until either the test signals release or the
                # hold-window elapses.
                lock_release.wait(timeout=lock_hold_seconds)

        lock_thread = threading.Thread(target=hold_lock, daemon=True)
        lock_thread.start()
        try:
            assert lock_acquired.wait(timeout=5.0), (
                "Test setup: ACCESS EXCLUSIVE lock on hot upsert_log never acquired."
            )

            t0 = time.monotonic()
            with pytest.raises(ApiError) as exc_info:
                client.read_data(
                    catalog_uuid=catalog_uuid,
                    schema_uuid=schema_uuid,
                    table_uuid=table_uuid,
                    branch_uuid=branch_uuid,
                )

            elapsed = time.monotonic() - t0

            # The cap must fire near the configured timeout — generous
            # upper bound to absorb startup + RPC overhead.
            assert elapsed < lock_hold_seconds - 1.0, (
                f"read_data took {elapsed:.2f}s — should have been "
                f"capped near {QUERY_TIMEOUT_SECONDS}s, not waited "
                "for the lock to release. ADR 0019: enforce "
                "query_timeout_seconds at the BatchStream layer."
            )

            msg = str(exc_info.value).lower()
            assert any(
                token in msg
                for token in ("resource_exhausted", "timeout", "exceeded", "deadline")
            ), (
                f"read_data cancellation surface should mention the cap; "
                f"got: {exc_info.value!r}. "
                "ADR 0019 §'Defaults': RESOURCE_EXHAUSTED + a retry-pattern "
                "detail naming query_timeout_seconds."
            )
        finally:
            lock_release.set()
            lock_thread.join(timeout=lock_hold_seconds + 2.0)


class TestSegmentDeleteSetSweep:
    """ADR 0019 §"Four-part mechanism" item 3 — compaction inserts
    ``segment_delete_set`` rows inside the merge tx and a separate
    sweep (``sweep_segments``) deletes the underlying cold files after
    grace.
    """

    def test_segment_delete_entry_swept_after_grace(self):
        """Compact wave inserts a ``segment_delete_set`` row for each
        old URI; before grace the row is present (no file deletion has
        fired yet); after grace + ``sweep_segments`` the row is gone.

        Currently (red): the ``segment_delete_set`` table does not
        exist — the white-box query raises ``UndefinedTable``. The
        ``sweep_segments`` client method does not exist either. After
        commits 5-6 of the PR 2 sequence land (green): the table is
        created with the merge tx, compaction inserts rows in
        lockstep, and the sweep helper removes them past grace.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "seg_delete")
        table_uuid = create_table_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # Two persist segments → compact merges into one and enqueues
        # the two original URIs in ``segment_delete_set``.
        for i in range(2):
            _commit_tx_writing_rows(
                client,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                table_uuid,
                {"name": [f"r_{i}"], "value": [i]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Pre-grace: segment_delete_set rows must exist. The exact
        # count depends on how many input segments compact folded;
        # ``>= 1`` is sufficient — the precise inventory is in the
        # lifecycle test suite's compact-segment metadata pins.
        pre_grace = _count_segment_delete_set_rows(catalog_uuid, branch_uuid)
        assert pre_grace >= 1, (
            "Compact wave must insert at least one segment_delete_set "
            f"row for the old URIs. Got {pre_grace}. ADR 0019: defer "
            "the inline delete via the segment delete set + sweep."
        )

        # Sweep before grace elapses must be a no-op — the rows are
        # still within their grace window.
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        mid_grace = _count_segment_delete_set_rows(catalog_uuid, branch_uuid)
        assert mid_grace == pre_grace, (
            "sweep_segments within grace must not delete set rows — "
            f"pre={pre_grace}, mid={mid_grace}. ADR 0019: sweep is "
            "gated on ``written_at_micros + query_timeout < now``."
        )

        # Past grace: sweep clears the rows and deletes the underlying
        # cold files.
        time.sleep(GRACE_WAIT_SECONDS)
        client.sweep_segments(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        post_grace = _count_segment_delete_set_rows(catalog_uuid, branch_uuid)
        assert post_grace == 0, (
            "sweep_segments past grace must remove all set rows; got "
            f"{post_grace}. ADR 0019: sweep deletes the cold file "
            "then deletes the set row."
        )


class TestPerOperationLockKeys:
    """ADR 0019 §"Lock scoping" — the shared
    ``lifecycle:{table_uuid}:{branch_uuid}`` advisory key is split
    into three per-operation keys (``persist:``, ``snapshot:``,
    ``purge:``). Cross-operation pairs on the same ``T`` no longer
    serialize. Pillar 1 (plan-time threading) and pillar 3 (grace
    window) make this safe.

    Both tests pre-hold the ADR-0018 shared key
    ``lifecycle:{table_uuid}:{branch_uuid}`` from an out-of-band
    driver and then issue the cross-operation pair concurrently. The
    assertion is "both completed while the test held the shared key."

    * Red (ADR 0018 behavior): Persist / Snapshot / Purge all take
      the shared key; both ops block waiting on the test's hold and
      neither completes. The test fails on the "did not complete in
      time" assertion.
    * Green (ADR 0019 behavior): each op takes its own
      ``persist:`` / ``snapshot:`` / ``purge:`` key, which the test
      isn't holding. Both ops complete unblocked.

    Same-operation parallelism (``Persist(T)`` against ``Persist(T)``,
    etc.) is intentionally NOT tested here — those pairs still
    serialize under ADR 0019 by design (see the
    ``persist:`` / ``snapshot:`` / ``purge:`` correctness reasoning
    in the ADR).
    """

    # Generous timeout for the unblocked-op completion check. Persist
    # against an empty table is ~tens of ms; snapshot/purge similar.
    # 2 s is the QUERY_TIMEOUT cap and well above the worst observed
    # in-suite latency, so this gives both ops headroom without
    # bleeding into the cap window.
    _COMPLETE_WAIT_SECONDS = 1.5

    @staticmethod
    def _seed_table(client, catalog_uuid, schema_uuid, branch_uuid):
        """Create a table on the branch and commit one row, so the
        per-operation paths under test have work to do (Persist writes
        a non-empty segment; Snapshot/Purge see the committed
        ``persisted_at``). Small data — the test asserts on lock
        contention, not throughput."""
        table_uuid = create_table_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_uuid,
            {"name": ["seed"], "value": [0]},
        )
        return table_uuid

    @staticmethod
    def _run_in_thread(target):
        done = threading.Event()
        error: list[BaseException] = []

        def run():
            try:
                target()
            except BaseException as exc:  # noqa: BLE001
                error.append(exc)
            finally:
                done.set()

        thread = threading.Thread(target=run, daemon=True)
        return thread, done, error

    def test_concurrent_persist_and_purge_on_T_run_in_parallel(self):
        """``Persist(T)`` and ``Purge(T)`` on the same table must NOT
        serialize on the ADR-0018 shared key. Pre-hold
        ``lifecycle:{table_uuid}:{branch_uuid}`` from the test; both
        ops complete unblocked under ADR 0019's per-operation keys.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "lock_persist_purge")
        table_uuid = self._seed_table(client, catalog_uuid, schema_uuid, branch_uuid)

        shared_key = f"lifecycle:{table_uuid}:{branch_uuid}"
        lock_driver = make_lock_driver()
        persist_thread, persist_done, persist_error = self._run_in_thread(
            lambda: client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )
        )
        purge_thread, purge_done, purge_error = self._run_in_thread(
            lambda: client.purge(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )
        )

        try:
            with lock_driver.advisory_lock(shared_key):
                # While the test holds the ADR-0018 shared key:
                # - Red (shared key in server): both Persist and
                #   Purge block trying to acquire it; neither
                #   completes while we hold it.
                # - Green (per-op keys in server): each op takes
                #   ``persist:`` / ``purge:`` (independent of the
                #   shared key); both complete despite our hold.
                persist_thread.start()
                purge_thread.start()
                persist_completed = persist_done.wait(self._COMPLETE_WAIT_SECONDS)
                purge_completed = purge_done.wait(self._COMPLETE_WAIT_SECONDS)
        finally:
            # Lock auto-released by the `with` exit; join the threads
            # so the suite cleans up regardless of pass/fail.
            persist_thread.join(timeout=10.0)
            purge_thread.join(timeout=10.0)
            lock_driver.close()

        assert not persist_error, f"Persist crashed: {persist_error[0]!r}"
        assert not purge_error, f"Purge crashed: {purge_error[0]!r}"
        assert persist_completed, (
            "Persist(T) blocked on the ADR-0018 shared "
            f"'lifecycle:{table_uuid}:{branch_uuid}' advisory key. "
            "ADR 0019 §'Lock scoping': Persist must take "
            "'persist:{table_uuid}:{branch_uuid}', orthogonal to "
            "Purge's 'purge:{table_uuid}:{branch_uuid}'."
        )
        assert purge_completed, (
            "Purge(T) blocked on the ADR-0018 shared "
            f"'lifecycle:{table_uuid}:{branch_uuid}' advisory key. "
            "ADR 0019 §'Lock scoping': Purge must take "
            "'purge:{table_uuid}:{branch_uuid}', orthogonal to "
            "Persist's 'persist:{table_uuid}:{branch_uuid}'."
        )

    def test_concurrent_snapshot_and_purge_on_T_run_in_parallel(self):
        """``Snapshot(T)`` and ``Purge(T)`` on the same table must NOT
        serialize on the ADR-0018 shared key. Same shape as
        Persist+Purge — pre-hold the shared key, both ops should
        complete unblocked under ADR 0019.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "lock_snap_purge")
        table_uuid = self._seed_table(client, catalog_uuid, schema_uuid, branch_uuid)

        # Snapshot is cold-only — needs a committed Persist first
        # (and Snapshot writes are no-ops without one). Run Persist
        # outside the lock-held window so it doesn't muddy the
        # contention probe; the per-operation key under test is
        # ``snapshot:`` vs ``purge:``.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        shared_key = f"lifecycle:{table_uuid}:{branch_uuid}"
        lock_driver = make_lock_driver()
        snap_thread, snap_done, snap_error = self._run_in_thread(
            lambda: client.snapshot(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )
        )
        purge_thread, purge_done, purge_error = self._run_in_thread(
            lambda: client.purge(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )
        )

        try:
            with lock_driver.advisory_lock(shared_key):
                snap_thread.start()
                purge_thread.start()
                snap_completed = snap_done.wait(self._COMPLETE_WAIT_SECONDS)
                purge_completed = purge_done.wait(self._COMPLETE_WAIT_SECONDS)
        finally:
            snap_thread.join(timeout=10.0)
            purge_thread.join(timeout=10.0)
            lock_driver.close()

        assert not snap_error, f"Snapshot crashed: {snap_error[0]!r}"
        assert not purge_error, f"Purge crashed: {purge_error[0]!r}"
        assert snap_completed, (
            "Snapshot(T) blocked on the ADR-0018 shared "
            f"'lifecycle:{table_uuid}:{branch_uuid}' advisory key. "
            "ADR 0019 §'Lock scoping': Snapshot must take "
            "'snapshot:{table_uuid}:{branch_uuid}', orthogonal to "
            "Purge's 'purge:{table_uuid}:{branch_uuid}'."
        )
        assert purge_completed, (
            "Purge(T) blocked on the ADR-0018 shared "
            f"'lifecycle:{table_uuid}:{branch_uuid}' advisory key. "
            "ADR 0019 §'Lock scoping': Purge must take "
            "'purge:{table_uuid}:{branch_uuid}', orthogonal to "
            "Snapshot's 'snapshot:{table_uuid}:{branch_uuid}'."
        )
