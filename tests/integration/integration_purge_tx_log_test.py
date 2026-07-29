"""Integration tests for ``PurgeTxLog`` (CHA-221) — branch-scoped GC
across the four hot tx-log family tables (``commit_tx_log`` /
``tx_table_log`` / ``abort_tx_log`` / ``begin_tx_log``).

Pins:

- **Active table set ``S``** — distinct tables in ``tx_table_log[B]``
  whose writer tx is *settled* (committed, or aborted as-of the pass
  start). Tables outside ``S`` impose no cutoff; empty / never-written
  tables drop out automatically
  (``test_empty_table_does_not_pin_max_micros``).
- **Seq cutoffs (CHA-444 / ADR 0027)** — committed GC bound
  ``Pu = MIN(last_purged_commit_seq_num over S)`` and aborted GC bound
  ``Pa = MIN(last_purged_aborted_seq_num over S)``. An absent purge
  row on an axis treats that table as unpurged and blocks GC on it
  (``pu_cutoff`` / ``pa_cutoff`` ⇒ ``None``;
  ``test_unpersisted_table_pins_max_micros_at_one``).
- **Composite DELETE** — single SQL statement keyed on an eligibility
  CTE over four disjoint branches: committed ``commit_seq_num <= Pu``,
  aborted-with-writes ``aborted_at_seq_num < Pa``, pure begin+abort,
  and expired-begin (wall-clock grace). In-flight open txs are in none
  of the branches and are preserved unconditionally
  (``test_purge_tx_log_trims_after_full_persist_purge``,
  ``test_clamp_preserves_open_tx_begin_row``,
  ``test_pure_begin_abort_tx_is_gcd``,
  ``test_concurrent_purge_tx_log_and_writer_do_not_race``).
- **Response shape** — CHA-444: ``PurgeTxLog`` is fire-and-forget
  across the two seq axes and reports **no** watermark
  (``PurgeTxLogResponse`` is empty — the field was removed); callers
  observe the GC through the tx-log family row counts.
- **Long-cleanup-race** — a Purge committing *after* the pass's
  ``cleanup_started_at`` is invisible to the seq cutoffs via the as-of
  ``commit_micros <= cleanup_started_at`` filter on
  ``table_purge_metadata``
  (``test_purge_tx_log_excludes_future_committed_purge``).
- **Lock scope** — branch-scoped advisory lock
  ``purge_tx_log:{branch_uuid}`` serializes concurrent passes per
  branch but is orthogonal to per-table Persist / Snapshot / Purge
  locks; the write path is unaffected
  (``test_concurrent_purge_tx_log_and_writer_do_not_race``).
- **Live-query safety** — ``PurgeTxLog`` churning in the background
  does not break in-flight reads within ``query_timeout_seconds``
  (``test_live_query_safety_under_purge_tx_log_churn``).

Test 9 from the plan (scheduler drives ``PurgeTxLog`` per
``tick_branch``) lands with the scheduler commit and lives in
``integration_lifecycle_scheduler_test.py``, not here.

Run via ``just integration-test``.
"""

from __future__ import annotations

import threading
import time
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    abort_tx_log_partition,
    begin_tx_log_partition,
    commit_tx_log_partition,
    system_schemas_table_uuid,
    system_tables_table_uuid,
    tx_table_log_partition,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    setup_schema,
)

TABLE_PURGE_METADATA = "table_purge_metadata"
TABLE_PERSIST_METADATA = "table_persist_metadata"
TABLE_PERSIST_SEGMENT_METADATA = "table_persist_segment_metadata"


def _count_table_persist_rows(catalog_uuid, branch_uuid, table_uuid):
    """Count `table_persist_metadata` rows for `(branch, table)`."""
    parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )[0][0]


def _count_committed_persist_segments(catalog_uuid, branch_uuid, table_uuid):
    """Count plan-visible persist segments for `(branch, table)`."""
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg_parent)),
        (branch_uuid, table_uuid),
    )[0][0]


def _make_branch(client, catalog_uuid, name):
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-221",
    )
    return branch.branch_uuid


def _create_tables_on_branch(client, catalog_uuid, schema_uuid, branch_uuid, names):
    return {
        name: create_table_on_branch(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_name=name,
        )
        for name in names
    }


def _commit_tx_writing_rows(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuids, rows
):
    """Begin → write → commit a tx; return
    ``(tx_uuid, commit_micros)``."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    for table_uuid in table_uuids:
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
    return tx.tx_uuid, committed.commit_micros


def _persist_and_purge_past_grace(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuid
):
    """Persist(T) → Snapshot(T) → Purge(T). Returns the purge response.

    CHA-444 (ADR 0027): Purge advances Pu only to W_snap, so Snapshot must run
    before Purge can clear committed hot rows / advance the fence. The hot purge
    grace is gone, so no wait is needed (kept the name for call-site stability).
    """
    client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    return client.purge(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )


def _persist_purge_system_tables_past_grace(
    client, catalog_uuid, schema_uuid, branch_uuid
):
    """Persist → Snapshot → Purge ``__penca_system__.tables`` AND
    ``__penca_system__.schemas`` on the branch.

    Why: ``fork_tx`` (CHA-181) writes one row per inherited user table
    to ``__penca_system__.tables`` AND one row per inherited user
    schema to ``__penca_system__.schemas`` on every newly-forked
    branch; subsequent ``CreateSchema`` / ``CreateTable`` writes
    there too. Those writes leave ``tx_table_log[B]`` entries for
    both system tables, so they both appear in the active set ``S``.
    Without a committed purge watermark on either, ``MIN(Pu over S)`` is
    ``None`` (CHA-444 / ADR 0027) ⇒ no committed user-tx ``commit_tx_log`` rows
    are GC-eligible.

    The production scheduler ([CHA-154](
    https://linear.app/chapala/issue/CHA-154)) drives Persist → Snapshot
    → Purge on every table including the system tables, so this is a no-op
    in practice. Tests reaching into the algorithm have to mirror that
    explicitly. CHA-444 dropped the hot-purge grace, so each system table
    is a self-contained Persist → Snapshot → Purge with no wait.
    """
    # CHA-444: Persist → Snapshot → Purge each system table so Purge can
    # advance Pu = W_snap and the commit_tx_log GC has a watermark to trim against.
    for sys_uuid in (
        system_tables_table_uuid(catalog_uuid),
        system_schemas_table_uuid(catalog_uuid),
    ):
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=sys_uuid,
        )


def _count_commit_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid=None):
    part = commit_tx_log_partition(catalog_uuid, branch_uuid)
    if tx_uuid is None:
        return get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(part)),
        )[0][0]

    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(Identifier(part)),
        (tx_uuid,),
    )[0][0]


def _count_begin_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid=None):
    part = begin_tx_log_partition(catalog_uuid, branch_uuid)
    if tx_uuid is None:
        return get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(part)),
        )[0][0]

    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(Identifier(part)),
        (tx_uuid,),
    )[0][0]


def _count_abort_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid=None):
    part = abort_tx_log_partition(catalog_uuid, branch_uuid)
    if tx_uuid is None:
        return get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(part)),
        )[0][0]

    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(Identifier(part)),
        (tx_uuid,),
    )[0][0]


def _count_tx_table_log_rows(catalog_uuid, branch_uuid, tx_uuid=None):
    part = tx_table_log_partition(catalog_uuid, branch_uuid)
    if tx_uuid is None:
        return get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(part)),
        )[0][0]

    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(Identifier(part)),
        (tx_uuid,),
    )[0][0]


def _latest_committed_purged_at(catalog_uuid, branch_uuid, table_uuid):
    """Latest committed purge fence ``Pu`` (``last_purged_commit_seq_num``) for T,
    or None. CHA-444 (ADR 0027): the committed watermark is seq-axis now."""
    parent = f"{catalog_uuid}_{TABLE_PURGE_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(last_purged_commit_seq_num) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _latest_committed_aborted_purged_at(catalog_uuid, branch_uuid, table_uuid):
    """Latest committed abort fence ``Pa`` (``last_purged_aborted_seq_num``)
    for T, or None. CHA-444 (ADR 0027): the abort axis Purge owns."""
    parent = f"{catalog_uuid}_{TABLE_PURGE_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(last_purged_aborted_seq_num) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _insert_synthetic_purge_metadata(
    catalog_uuid, branch_uuid, table_uuid, last_purged_commit_seq_num, commit_micros
):
    """Phase-2-committed synthetic ``table_purge_metadata`` row on the seq axis.

    CHA-444 (ADR 0027): the committed watermark column is
    ``last_purged_commit_seq_num`` (``Pu``); the old ``purged_at_micros`` column
    is dropped. Used by the long-cleanup-race
    test to plant a *future*-committed purge whose ``commit_micros``
    exceeds the pass's ``cleanup_started_at`` so the as-of filter on
    ``table_purge_metadata.commit_micros`` excludes it from ``Pu``.
    """
    parent = f"{catalog_uuid}_{TABLE_PURGE_METADATA}"
    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl}"
            " (table_purge_uuid, branch_uuid, table_uuid,"
            "  last_purged_commit_seq_num, commit_micros)"
            " VALUES (%s, %s, %s, %s, %s)"
        ).format(tbl=Identifier(parent)),
        (
            str(uuid4()),
            branch_uuid,
            table_uuid,
            last_purged_commit_seq_num,
            commit_micros,
        ),
    )


def _insert_synthetic_commit_tx_log_row(
    catalog_uuid, branch_uuid, tx_uuid, began_at_micros, commit_micros, commit_seq_num
):
    """Synthetic committed ``commit_tx_log`` row whose ``commit_micros`` and
    ``commit_seq_num`` the test chooses directly. Bypasses ``BeginTx`` /
    ``CommitTx`` so the seq-axis committed GC eligibility (``commit_seq_num <=
    Pu``) can be exercised deterministically without racing the real commit
    allocator. CHA-428: ``commit_seq_num`` is NOT NULL and UNIQUE per branch —
    callers pass a value above any real commit on the branch.
    """
    part = commit_tx_log_partition(catalog_uuid, branch_uuid)
    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl}"
            " (tx_uuid, branch_uuid, began_at_micros, commit_micros,"
            "  comment, author, commit_seq_num)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(tbl=Identifier(part)),
        (
            tx_uuid,
            branch_uuid,
            began_at_micros,
            commit_micros,
            "cha-444 synthetic",
            "test",
            commit_seq_num,
        ),
    )


def _insert_synthetic_begin_tx_log_row(
    catalog_uuid, branch_uuid, tx_uuid, began_at_micros
):
    """Synthetic open-tx ``begin_tx_log`` row. ``expires_at_micros`` is set
    one minute past ``began_at_micros`` so a future ``began_at`` keeps the tx
    out of the expired-begin GC branch (the surviving-open-tx case)."""
    part = begin_tx_log_partition(catalog_uuid, branch_uuid)
    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl}"
            " (tx_uuid, branch_uuid, began_at_micros, began_at_seq_num,"
            "  expires_at_micros, comment, author)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ).format(tbl=Identifier(part)),
        (
            tx_uuid,
            branch_uuid,
            began_at_micros,
            # CHA-429: began_at_seq_num is NOT NULL. This synthetic row bypasses
            # the real begin allocator; the GC keys an open tx on expires_at
            # (not this seq), so reuse the test-chosen began_at_micros as the
            # seq — non-null and self-consistent (the PK is (tx_uuid,
            # branch_uuid), so no per-seq uniqueness is required).
            began_at_micros,
            began_at_micros + 60_000_000,
            "cha-444 synthetic open tx",
            "test",
        ),
    )


class TestPurgeTxLogStableState:
    """Steady-state correctness: the composite eligibility-set DELETE
    GCs settled txs and leaves everything else alone."""

    def test_empty_table_does_not_pin_max_micros(self):
        """Algorithm step 2: ``S = distinct tables in tx_table_log[B]``.
        An empty / never-written user table E is absent from ``S`` and
        contributes no constraint.

        Branch B has A (written + Persist+Purge'd) and E (no writes).
        E never appears in ``tx_table_log[B]`` ⇒ it does not pin
        ``max_micros`` at 0.

        Multiple PurgeTxLog passes are required for X1 to be GC'd
        because the system tables' ``purged_at`` is frozen at the
        last CreateTable's commit time (no new rows after that),
        and the GC has to step through:

        1. fork_tx in ``commit_tx_log[B]`` (sys_schemas's only writer);
           after pass 1, sys_schemas drops out of S.
        2. The two CreateTable txs on the branch (sys_tables's
           other writers); after pass 2, sys_tables drops out of S.
        3. X1 — finally eligible on pass 3 with S = {A}.

        This mirrors the scheduler's tick cadence (
        [CHA-154](https://linear.app/chapala/issue/CHA-154) runs
        PurgeTxLog once per tick — the GC self-heals across ticks
        per ticket §Liveness).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "empty_no_pin")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["a", "e"]
        )

        x1_tx, _x1_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["a"]],
            {"name": ["alice"], "value": [1]},
        )
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["a"]
        )
        _persist_purge_system_tables_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # Three passes drain fork_tx → CreateTable txs → X1's
        # commit_tx_log row, in that order.
        for _ in range(3):
            response = client.purge_tx_log(
                catalog_uuid=catalog_uuid,
                branch_uuid=branch_uuid,
            )

        # CHA-444 (ADR 0027): PurgeTxLog is fire-and-forget across two seq axes
        # and no longer reports a single watermark, so the response is unused.
        # The behavioral contract — E (no writes) is absent from S and does not
        # block GC of A's settled txs — is verified by X1 being GC'd below.
        del response
        assert _count_commit_tx_log_rows(catalog_uuid, branch_uuid, x1_tx) == 0, (
            "X1.committed_at < max_micros AND <= cleanup_started_at on "
            "pass 2 — X1's commit_tx_log row must be GC'd."
        )
        assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, x1_tx) == 0, (
            "X1 is in the eligibility set on pass 2 ⇒ X1's tx_table_log "
            "row is GC'd in the same composite DELETE."
        )

    def test_unpersisted_table_pins_max_micros_at_one(self):
        """Written-but-not-yet-Persisted table T blocks committed GC.
        ``S = {T, __penca_system__.tables}``; both are unpurged on the
        committed axis (``Pu`` absent) ⇒ ``compute_purge_tx_log_cutoffs``
        drops ``pu_cutoff`` to ``None`` ⇒ no committed tx is GC-eligible,
        so X1's ``commit_tx_log`` row is preserved. ``PurgeTxLog`` reports no
        watermark (the response is empty); the contract is the row-count
        preservation asserted below.

        Seq-axis successor of the old "max_micros pinned at 1" liveness
        branch (CHA-444 / ADR 0027) — an unpurged table's hot rows still
        exist, so its commit_tx_log entries must not be GC'd; block-on-``None``,
        not the pre-CHA-444 ``None → 0`` treatment.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "pinned_at_one")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t"]
        )

        x1_tx, _x1_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t"]],
            {"name": ["alice"], "value": [1]},
        )

        response = client.purge_tx_log(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        # CHA-444 (ADR 0027): PurgeTxLog reports no watermark. The contract —
        # an unpurged table T (Pu absent) blocks committed GC, so its txs are
        # preserved — is the seq-axis analog of "max_micros pinned at 1".
        del response
        assert _count_commit_tx_log_rows(catalog_uuid, branch_uuid, x1_tx) == 1, (
            "X1's commit_tx_log row must be preserved: T is unpurged (Pu absent) ⇒ "
            "pu_cutoff is None ⇒ no committed tx is GC-eligible."
        )
        assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, x1_tx) >= 1, (
            "X1 is in commit_tx_log after step 5 (preserved) ⇒ step 6 leaves "
            "X1's tx_table_log row alone."
        )

    def test_purge_tx_log_trims_after_full_persist_purge(self):
        """Multi-table happy path: write to A, B, C; Persist → Snapshot →
        Purge each; ``PurgeTxLog`` GCs the committed tx's ``commit_tx_log`` /
        ``tx_table_log`` rows once ``Pu = MIN(last_purged_commit_seq_num over S)``
        covers it. CHA-444 (ADR 0027): the GC reports no watermark — the
        contract is the row counts.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "happy_path")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["a", "b", "c"]
        )

        x1_tx, _x1_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["a"], tables["b"], tables["c"]],
            {"name": ["alice"], "value": [1]},
        )

        for name in ("a", "b", "c"):
            _persist_and_purge_past_grace(
                client, catalog_uuid, schema_uuid, branch_uuid, tables[name]
            )

        # See `_persist_purge_system_tables_past_grace` doc-comment —
        # fork_tx + CreateTable writes to __penca_system__.tables put
        # it in S; if we skip the Persist+Snapshot+Purge here, its absent
        # Pu blocks committed GC (pu_cutoff is None) and X1 survives.
        _persist_purge_system_tables_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # Each of A/B/C carries a committed purge fence Pu after
        # Persist → Snapshot → Purge (the same X1 commit on all three).
        purged_pus = [
            _latest_committed_purged_at(catalog_uuid, branch_uuid, tables[n])
            for n in ("a", "b", "c")
        ]
        assert all(pu is not None and pu > 0 for pu in purged_pus), (
            f"each of A/B/C must carry a committed Pu after purge; got {purged_pus}"
        )

        # Multiple PurgeTxLog passes drain the historical fork_tx +
        # CreateTable rows from commit_tx_log[B] before X1 becomes eligible:
        # pu_cutoff = MIN(Pu over S) only reaches X1's seq once the system
        # tables age out of S. See test 1's docstring for the full
        # walkthrough.
        for _ in range(3):
            client.purge_tx_log(
                catalog_uuid=catalog_uuid,
                branch_uuid=branch_uuid,
            )

        # CHA-444: PurgeTxLog reports no watermark (empty response) — the
        # contract is the GC itself: X1's committed commit_tx_log + tx_table_log
        # rows are gone.
        assert _count_commit_tx_log_rows(catalog_uuid, branch_uuid, x1_tx) == 0
        assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, x1_tx) == 0


class TestPurgeTxLogClamps:
    """The long-cleanup-race guard protects post-cleanup-start writes from a
    Purge that commits mid-pass. CHA-444 (ADR 0027) re-axises CHA-221's
    ``cleanup_started_at`` clamp onto the seq cutoffs: the bound now lives in
    the as-of ``commit_micros <= cleanup_started_at`` filter of the
    watermark read (``tx_table_log_purge_watermarks_for_branch``), not a
    wire-side ``min``. Uses synthetic SQL injection to pin it deterministically
    — the wall-clock version (run a real PurgeTxLog for > query_timeout, commit
    a tx mid-pass, Purge a side table) needs a server-side hook to pause the
    pass, which we don't have today. The injection pins the same observable
    contract: a future-committed Purge does not advance the cutoffs.
    """

    def test_purge_tx_log_excludes_future_committed_purge(self):
        """A Purge committing *after* the pass's ``cleanup_started_at`` is
        invisible to the seq cutoffs. Synthetic setup: a *future*-committed
        ``table_purge_metadata`` row for A (and the two ``__penca_system__``
        tables) carrying a huge ``last_purged_commit_seq_num``, plus a committed
        ``commit_tx_log`` row Y whose ``commit_seq_num`` sits between A's real ``Pu`` and
        that huge value. Without the as-of filter Y would be GC'd
        (``Y.commit_seq_num <= huge Pu``); with the
        ``commit_micros <= cleanup_started_at`` filter the future purge
        is excluded, ``Pu`` stays at the real (small) value, and Y survives.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "future_purge")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["a"]
        )

        # Real tx X1 → A, then a real Persist → Snapshot → Purge so A carries
        # a real (small) committed Pu visible at cleanup_started_at. The
        # system tables get the same so all of S has a real Pu.
        _x1_tx, x1_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["a"]],
            {"name": ["alice"], "value": [1]},
        )
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["a"]
        )
        _persist_purge_system_tables_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid
        )
        real_pu = _latest_committed_purged_at(catalog_uuid, branch_uuid, tables["a"])
        assert real_pu is not None, "A must carry a real committed Pu after purge"

        # Synthetic FUTURE-committed purge for A AND the two system tables,
        # carrying a huge Pu. ``committed_at`` far in the future ⇒ the as-of
        # filter must exclude it. All three are in S, so synthesizing only A's
        # would leave the system tables' real (small) Pu binding regardless.
        far_future = x1_committed_at + 24 * 60 * 60 * 1_000_000  # +24 h
        huge_pu = real_pu + 1_000_000
        for table_uuid in (
            tables["a"],
            system_tables_table_uuid(catalog_uuid),
            system_schemas_table_uuid(catalog_uuid),
        ):
            _insert_synthetic_purge_metadata(
                catalog_uuid,
                branch_uuid,
                table_uuid,
                last_purged_commit_seq_num=huge_pu,
                commit_micros=far_future,
            )

        # Synthetic committed tx Y with commit_seq_num between A's real Pu and the
        # future huge Pu. If the future purge were visible Y would be GC'd
        # (Y.commit_seq_num <= huge_pu); with the as-of filter it must survive.
        y_tx = str(uuid4())
        y_seq = real_pu + 1000
        _insert_synthetic_commit_tx_log_row(
            catalog_uuid,
            branch_uuid,
            y_tx,
            began_at_micros=x1_committed_at + 1,
            commit_micros=x1_committed_at + 2,
            commit_seq_num=y_seq,
        )

        client.purge_tx_log(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        assert _count_commit_tx_log_rows(catalog_uuid, branch_uuid, y_tx) == 1, (
            "the future-committed purge must be excluded by the as-of filter, "
            f"so Pu stays at the real value ({real_pu}) and Y (commit_seq_num="
            f"{y_seq}) is not GC-eligible."
        )

    def test_clamp_preserves_open_tx_begin_row(self):
        """A live open-tx ``begin_tx_log[B]`` row is preserved. CHA-444
        (ADR 0027): a begin row is eligible only via the expired-begin branch
        (``expires_at_micros < cleanup_started_at - grace`` AND no
        ``commit_tx_log`` / ``abort_tx_log`` row). A synthetic begin row that is
        committed/aborted-free AND not yet expired (``expires_at`` in the
        future) matches none of the four eligibility branches, so it survives.

        Real-open-tx safety pinned end-to-end by
        ``test_concurrent_purge_tx_log_and_writer_do_not_race``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "clamp_open_tx")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t"]
        )

        # Populate tx_table_log[B] so S is non-empty and the empty-set
        # fast-path does not skip the DELETEs.
        _x1_tx, x1_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t"]],
            {"name": ["alice"], "value": [1]},
        )
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["t"]
        )

        # Synthetic open tx O: future began_at (⇒ future expires_at, see
        # `_insert_synthetic_begin_tx_log_row`), no entry in commit_tx_log or
        # abort_tx_log. Expired-begin branch: expires_at < cleanup_started -
        # grace is FALSE (expires in the future) ⇒ O matches no eligibility
        # branch ⇒ preserved.
        o_tx = str(uuid4())
        future_began_at = x1_committed_at + 24 * 60 * 60 * 1_000_000  # +24 h
        _insert_synthetic_begin_tx_log_row(
            catalog_uuid,
            branch_uuid,
            o_tx,
            began_at_micros=future_began_at,
        )

        client.purge_tx_log(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        assert _count_begin_tx_log_rows(catalog_uuid, branch_uuid, o_tx) == 1, (
            "O is not expired (expires_at in the future) and has no "
            "commit_tx_log/abort_tx_log row — its begin_tx_log row must survive."
        )

    def test_pure_begin_abort_tx_is_gcd(self):
        """Pure begin+abort tx X is in the eligibility set's
        aborted-tx half (``aborted_at <= cleanup_started_at`` AND
        ``tx_uuid NOT IN tx_table_log[B]``). The composite DELETE
        clears X from all four tables in one shot:
        ``abort_tx_log[B]``, ``begin_tx_log[B]`` (X had a begin row),
        and the no-op DELETEs against ``commit_tx_log[B]`` /
        ``tx_table_log[B]`` (X never had entries there).

        Aborted txs that DID write (X in tx_table_log[B]) fall outside
        the eligibility set — their ``abort_tx_log`` row is preserved
        indefinitely. That's a known liveness gap in the spec but
        consistent with the chain across CHA-220 / CHA-233 / this
        ticket.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "begin_abort_gc")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["seed"]
        )

        # Seed the branch so tx_table_log[B] is non-empty — the
        # empty-set fast-path would skip the DELETE otherwise.
        _x_seed, _ = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["seed"]],
            {"name": ["seed"], "value": [0]},
        )
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["seed"]
        )

        # Pure begin + abort. X writes nothing — no tx_table_log entries.
        x_open = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.abort_tx(
            x_open.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        # Pre-PurgeTxLog: X has rows in begin_tx_log + abort_tx_log,
        # nothing in tx_table_log.
        assert _count_begin_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 1
        assert _count_abort_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 1
        assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0

        client.purge_tx_log(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        assert (
            _count_abort_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), (
            "X is in the eligibility set's aborted half "
            "(aborted_at <= cleanup_started_at, NOT IN tx_table_log) "
            "⇒ X's abort_tx_log row must be GC'd."
        )
        assert (
            _count_begin_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), (
            "Same composite DELETE: X is in the eligibility set ⇒ X's "
            "begin_tx_log row is GC'd in the same statement."
        )


class TestPurgeTxLogConcurrency:
    """The branch-scoped advisory lock
    ``purge_tx_log:{branch_uuid}`` (ADR 0019 §"Lock scoping" — per-op
    namespacing) serializes concurrent ``PurgeTxLog`` passes on the
    same branch but is orthogonal to per-table Persist / Snapshot /
    Purge locks AND to the write path."""

    def test_concurrent_purge_tx_log_and_writer_do_not_race(self):
        """A writer (BEGIN → write → COMMIT) running concurrently with
        ``PurgeTxLog`` completes cleanly — no ``NotFoundError`` from
        CommitTx, no FK or row-count anomalies. Pins the eligibility-
        set DELETE's open-tx safety: even when the writer's
        ``began_at < cleanup_started_at`` and the writer has
        ``tx_table_log`` entries from its mutate calls, the writer's
        ``tx_uuid`` is not in ``commit_tx_log[B] ∪ abort_tx_log[B]`` at
        statement start ⇒ not in the eligibility set ⇒ neither
        ``begin_tx_log`` nor ``tx_table_log`` rows are touched ⇒
        CommitTx finds everything where it expects.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "concurrent")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t"]
        )

        # Seed + age data so PurgeTxLog has real work to do during the race.
        _x_seed, _ = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t"]],
            {"name": ["seed"], "value": [0]},
        )
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["t"]
        )

        writer_done = threading.Event()
        purge_done = threading.Event()
        writer_error: list[BaseException] = []
        purge_error: list[BaseException] = []
        writer_tx_uuid: list[str] = []

        def run_writer():
            try:
                tx_uuid, _ = _commit_tx_writing_rows(
                    client,
                    catalog_uuid,
                    schema_uuid,
                    branch_uuid,
                    [tables["t"]],
                    {"name": ["concurrent"], "value": [99]},
                )
                writer_tx_uuid.append(tx_uuid)
            except BaseException as exc:  # noqa: BLE001
                writer_error.append(exc)
            finally:
                writer_done.set()

        def run_purge():
            try:
                client.purge_tx_log(
                    catalog_uuid=catalog_uuid,
                    branch_uuid=branch_uuid,
                )
            except BaseException as exc:  # noqa: BLE001
                purge_error.append(exc)
            finally:
                purge_done.set()

        writer_thread = threading.Thread(target=run_writer, daemon=True)
        purge_thread = threading.Thread(target=run_purge, daemon=True)
        writer_thread.start()
        purge_thread.start()
        assert writer_done.wait(timeout=15.0)
        assert purge_done.wait(timeout=15.0)
        writer_thread.join(timeout=1.0)
        purge_thread.join(timeout=1.0)

        assert not writer_error, f"Writer crashed: {writer_error[0]!r}"
        assert not purge_error, f"PurgeTxLog crashed: {purge_error[0]!r}"
        assert writer_tx_uuid, "writer did not record its tx_uuid"
        assert (
            _count_commit_tx_log_rows(catalog_uuid, branch_uuid, writer_tx_uuid[0]) == 1
        ), "Writer's commit_tx_log row must survive the concurrent PurgeTxLog pass."


class TestPurgeTxLogLiveQuerySafety:
    """ADR 0019 pillar 4: queries within ``query_timeout_seconds`` see
    a consistent view even under churn. ``PurgeTxLog`` running in the
    background must not break in-flight ``read_data`` calls."""

    def test_live_query_safety_under_purge_tx_log_churn(self):
        """A ``read_data`` consumer running for ~3 seconds against a
        branch with concurrent Persist+Purge+PurgeTxLog churn returns
        the same row set every time, no exceptions.

        Mechanism: each ``read_data`` plan captures a cutoff. Under
        the universal grace window, Purge cannot remove hot rows the
        plan still needs. PurgeTxLog cannot remove ``commit_tx_log`` rows
        the plan still resolves because ``cleanup_started_at_micros
        <= now`` and any tx whose hot rows are still readable was
        committed at ``committed_at <= cleanup_started_at`` — step 5's
        clamp engages.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "live_query")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t"]
        )

        # Seed with rows that should always be readable.
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t"]],
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
        )

        stop = threading.Event()
        churn_errors: list[BaseException] = []
        reader_errors: list[BaseException] = []
        reader_results: list[set[str]] = []

        def churn():
            try:
                while not stop.is_set():
                    _persist_and_purge_past_grace(
                        client, catalog_uuid, schema_uuid, branch_uuid, tables["t"]
                    )
                    client.purge_tx_log(
                        catalog_uuid=catalog_uuid,
                        branch_uuid=branch_uuid,
                    )
            except BaseException as exc:  # noqa: BLE001
                churn_errors.append(exc)

        def reader():
            try:
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and not stop.is_set():
                    result = client.read_data(
                        catalog_uuid=catalog_uuid,
                        schema_uuid=schema_uuid,
                        branch_uuid=branch_uuid,
                        table_uuid=tables["t"],
                    )
                    reader_results.append(set(result.column("name").to_pylist()))
            except BaseException as exc:  # noqa: BLE001
                reader_errors.append(exc)

        churn_thread = threading.Thread(target=churn, daemon=True)
        reader_thread = threading.Thread(target=reader, daemon=True)
        churn_thread.start()
        reader_thread.start()
        reader_thread.join(timeout=10.0)
        stop.set()
        churn_thread.join(timeout=15.0)

        assert not reader_errors, f"Reader crashed: {reader_errors[0]!r}"
        assert not churn_errors, f"Churn crashed: {churn_errors[0]!r}"
        assert reader_results, "Reader produced no observations"
        expected = {"alice", "bob", "carol"}
        for i, names in enumerate(reader_results):
            assert names == expected, (
                f"read_data observation #{i} mismatch under PurgeTxLog "
                f"churn: expected {expected}, got {names}. ADR 0019 "
                f"pillar 4: queries within query_timeout_seconds see a "
                f"consistent view."
            )


class TestPurgeTxLogAbortedCleanup:
    """CHA-444 (ADR 0027) reverses ADR 0021: aborted hot-row cleanup moves
    from Persist back to ``Purge(T)`` (Persist is committed-only CDC). Purge
    deletes aborted hot rows ``aborted_at_seq_num < F`` and advances the abort
    watermark ``Pa(T) = F`` on its own seq axis; ``PurgeTxLog``'s composite
    SQL GCs an aborted tx via ``aborted_at_seq_num < Pa = MIN(Pa over S)``.
    These tests pin the end-to-end "no orphan, no leak" property for aborted
    txs — both the mixed committed-plus-aborted case (test 9) and the
    aborted-only-table corner case (test 10) that earlier designs leaked
    indefinitely.

    Both tests use the same 3-pass `PurgeTxLog` drain loop as
    ``test_empty_table_does_not_pin_max_micros`` and
    ``test_purge_tx_log_trims_after_full_persist_purge`` because the
    historical fork_tx + CreateTable txs on ``sys_tables`` /
    ``sys_schemas`` structurally pin the committed cutoff ``MIN(Pu over S)``
    until they age out across passes (the abort axis ``Pa`` is independent —
    see those tests' docstrings for the full walkthrough).
    """

    def test_aborted_with_writes_tx_is_fully_gcd(self):
        """An aborted tx X with writes to T leaves no orphans:

        * Purge(T) deletes X's aborted hot rows from
          ``upsert_log[T,B]`` / ``delete_log[T,B]`` (``aborted_at_seq_num <
          F``) and advances ``Pa(T) = F`` past X's ``aborted_at_seq_num``.
        * PurgeTxLog (multi-pass drain) cleans X's
          ``abort_tx_log[B]`` / ``tx_table_log[B]`` / ``begin_tx_log[B]``
          rows via the abort branch ``aborted_at_seq_num < MIN(Pa over S)``.

        Pins the CHA-444 fix for the chicken-and-egg leak earlier designs
        left (abort_tx_log[X] + tx_table_log[X] stranded indefinitely).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "abort_writes_gc")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t"]
        )

        # Mixed setup: one committed write + one aborted write to T.
        w_tx_uuid, _w_committed_at = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t"]],
            {"name": ["alice"], "value": [1]},
        )

        # Aborted tx X writing to T.
        x_open = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        batch = pa.table({"name": ["bob"], "value": [2]}, schema=USER_SCHEMA)
        client.write_data(
            x_open.tx_uuid,
            Mutation(table_uuid=tables["t"], upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.abort_tx(
            x_open.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        # Pre-Persist state: T has 2 hot upsert rows (alice committed,
        # bob aborted); X is in abort_tx_log + tx_table_log + begin_tx_log.
        assert _count_abort_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 1
        assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) >= 1
        assert _count_begin_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 1

        # Persist → Snapshot → Purge(T). CHA-444: Purge clears BOTH the
        # committed hot row (alice, commit_seq_num <= Pu = W_snap) and the aborted
        # hot row (bob, aborted_at_seq_num < Pa = F), and advances Pa(T).
        _persist_and_purge_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid, tables["t"]
        )
        # Drain system-table watermarks so PurgeTxLog can clean
        # historical fork_tx + CreateTable_t txs.
        _persist_purge_system_tables_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # 3-pass drain loop (same structure as tests 1 and 3).
        for _ in range(3):
            client.purge_tx_log(
                catalog_uuid=catalog_uuid,
                branch_uuid=branch_uuid,
            )

        from penca_client.naming import delete_log_table, upsert_log_table

        upsert_tbl = upsert_log_table(tables["t"], branch_uuid)
        delete_tbl = delete_log_table(tables["t"], branch_uuid)
        assert (
            get_pg_driver().execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(upsert_tbl))
            )[0][0]
            == 0
        ), (
            "Hot upsert log must be empty (CHA-444: Purge clears both the "
            "committed and aborted hot rows)."
        )
        assert (
            get_pg_driver().execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(delete_tbl))
            )[0][0]
            == 0
        ), "Hot delete log must be empty."

        # Metadata fully cleaned for both W (committed) and X (aborted).
        for tx_uuid, label in (
            (w_tx_uuid, "W (committed)"),
            (x_open.tx_uuid, "X (aborted)"),
        ):
            assert _count_commit_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid) == 0, (
                f"commit_tx_log row for {label} must be GC'd."
            )
            assert _count_tx_table_log_rows(catalog_uuid, branch_uuid, tx_uuid) == 0, (
                f"tx_table_log row for {label} must be GC'd."
            )
            assert _count_begin_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid) == 0, (
                f"begin_tx_log row for {label} must be GC'd."
            )

        assert (
            _count_abort_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), "abort_tx_log row for X must be GC'd — no chicken-and-egg leak."

    def test_aborted_only_table_flows_through_pipeline(self):
        """Branch B with table T_a; tx X writes to T_a then aborts; no commits
        on T_a ever. CHA-444 (ADR 0027): Persist is committed-only, so on an
        aborts-only table Persist and Snapshot **no-op** — ``Purge(T_a)`` alone
        reclaims X's invisible hot rows on the abort axis (``aborted_at_seq_num
        < F``) and advances ``Pa(T_a) = F``, leaving the committed fence ``Pu``
        unset. ``PurgeTxLog`` then GCs X's tx-log-family rows via the abort
        branch ``aborted_at_seq_num < MIN(Pa over S)``.

        The scheduler enumerates T_a via ``ListModifiedTables``'s abort union
        and runs the same Persist → Snapshot → Purge sweep; the manual RPC
        sequence below mirrors it. Pins the fix for the aborted-only-table
        leak earlier designs left (hot rows + tx-log-family metadata stranded
        because nothing reclaimed an aborts-only table).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "abort_only_pipeline")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_a"]
        )

        # Only X writes to T_a, then aborts. No committed writes to T_a.
        x_open = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        batch = pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA)
        client.write_data(
            x_open.tx_uuid,
            Mutation(table_uuid=tables["t_a"], upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.abort_tx(
            x_open.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        from penca_client.naming import upsert_log_table

        upsert_tbl = upsert_log_table(tables["t_a"], branch_uuid)

        def _hot_upsert_count():
            return get_pg_driver().execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(upsert_tbl))
            )[0][0]

        # Pre-cleanup: T_a has X's hot upsert row (committed_at = NULL).
        assert _hot_upsert_count() == 1

        # Step (a): Persist(T_a) is a NO-OP — Persist is committed-only and
        # T_a has no committed rows. It neither advances persisted_at nor
        # touches X's (aborted) hot rows.
        persist_resp = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_a"],
        )
        assert not persist_resp.HasField("persisted_at_micros"), (
            "Persist on an aborts-only table must no-op (no committed rows)."
        )
        assert _hot_upsert_count() == 1, (
            "Persist must NOT touch aborted hot rows (CHA-444: Purge owns aborts)."
        )
        assert (
            _count_table_persist_rows(catalog_uuid, branch_uuid, tables["t_a"]) == 0
        ), "A no-op Persist writes no table_persist_metadata row."

        # Step (b): Snapshot(T_a) is a NO-OP — nothing was persisted.
        snapshot_resp = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_a"],
        )
        assert not snapshot_resp.HasField("snapshotted_at_micros"), (
            "Snapshot on an aborts-only table must no-op (no persist to fold)."
        )

        # Step (c): drain the system tables so their abort watermark Pa
        # advances to the current frontier (no aborts on them, but Purge
        # stamps Pa = F regardless), keeping MIN(Pa over S) non-None.
        _persist_purge_system_tables_past_grace(
            client, catalog_uuid, schema_uuid, branch_uuid
        )

        # Step (d): Purge(T_a) clears X's aborted hot rows and advances
        # Pa(T_a) = F. The committed fence Pu does NOT advance (no snapshot),
        # so the response watermark (Pu) is unset.
        purge_resp = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_a"],
        )
        assert not purge_resp.HasField("purged_at_micros"), (
            "Aborts-only Purge advances only Pa, not the committed fence Pu, "
            "so the response watermark is unset."
        )
        assert _hot_upsert_count() == 0, "Purge must clear X's aborted hot rows."
        assert (
            _latest_committed_aborted_purged_at(
                catalog_uuid, branch_uuid, tables["t_a"]
            )
            is not None
        ), "Purge must stamp the abort watermark Pa(T_a)."

        # Step (e): PurgeTxLog drain. X is GC'd via the abort branch
        # (aborted_at_seq_num < MIN(Pa over S)); the abort axis is independent
        # of the committed sys-table drain, but run the 3-pass loop for parity
        # with the other drains.
        for _ in range(3):
            client.purge_tx_log(
                catalog_uuid=catalog_uuid,
                branch_uuid=branch_uuid,
            )

        # Step (f): X's metadata fully GC'd.
        assert (
            _count_abort_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), "X's abort_tx_log row must be GC'd."
        assert (
            _count_tx_table_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), "X's tx_table_log row must be GC'd."
        assert (
            _count_begin_tx_log_rows(catalog_uuid, branch_uuid, x_open.tx_uuid) == 0
        ), "X's begin_tx_log row must be GC'd."
        assert _hot_upsert_count() == 0
