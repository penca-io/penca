"""Integration tests for the per-table lifecycle RPCs
(``Persist`` / ``Purge`` / ``Snapshot``).

Pins:

- **CHA-220 — per-table reshape.** ``Persist`` / ``Purge`` /
  ``Snapshot`` each take ``(catalog, branch, schema, table)`` and act
  on one physical user table per call. Other tables on the same
  branch are untouched.
- **CHA-233 / ADR 0019 §"Lock scoping" — per-operation per-table
  keys.** Each RPC takes its own advisory key:
  ``persist:{table_uuid}:{branch_uuid}`` /
  ``snapshot:{table_uuid}:{branch_uuid}`` /
  ``purge:{table_uuid}:{branch_uuid}``. Same-operation pairs on T
  serialize; cross-operation pairs (``Persist↔Snapshot``,
  ``Persist↔Purge``, ``Snapshot↔Purge``) are lock-free under pillars
  1 (plan-time threading) and 3 (grace window). Cross-operation
  parallelism is pinned in
  ``integration_grace_window_test.py::TestPerOperationLockKeys``.
- **CHA-233 — Persist is the hot/cold visibility cutoff.** ``plan()``'s
  cutoff is ``persisted_at_micros``. Immediately after ``Persist(T)``
  queries read the persisted rows from cold; the same rows still
  live physically in hot (until ``Purge(T)`` runs past the universal
  grace window, ADR 0019 mechanism 2), but the plan's hot filter
  ``committed_at >= max_persisted + 1`` structurally excludes them.
- **CHA-220 — Persist stops touching the tx-log family** (``commit_tx_log`` /
  ``begin_tx_log`` / ``abort_tx_log`` / ``tx_table_log``). Those leak
  until [CHA-221](https://linear.app/chapala/issue/CHA-221) lands —
  accepted as pre-1.0 dev-only.
- **CHA-228 — Snapshot no-ops symmetrically with Purge.**
  ``Snapshot(T)`` with no committed persist newer than the last
  snapshot writes no new ``table_snapshot_metadata`` row; response
  ``snapshotted_at_micros`` is unset.
- **CHA-228 — all three response watermark fields are proto3
  ``optional``.** Unset = no-op, set = "watermark after the call."
  Callers test field presence rather than comparing against ``0``.

Run via ``just integration-test``.
"""

from __future__ import annotations

import os
import threading
import time
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.naming import (
    abort_tx_log_partition,
    begin_tx_log_partition,
    commit_tx_log_partition,
    delete_log_table,
    tx_table_log_partition,
    upsert_log_table,
)
from penca_proto.external.v1.lifecycle_pb2 import PersistRequest, PurgeRequest
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    make_lock_driver,
    setup_schema,
)

_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])

# Per-catalog parents (CHA-198). CHA-220 adds ``table_purge_metadata``
# mirroring the ``table_persist_metadata`` shape; hard-coded here until
# the DDL + Python naming exports land.
TABLE_PERSIST_METADATA = "table_persist_metadata"
TABLE_PERSIST_SEGMENT_METADATA = "table_persist_segment_metadata"
TABLE_PURGE_METADATA = "table_purge_metadata"
TABLE_SNAPSHOT_METADATA = "table_snapshot_metadata"

# CHA-233 (ADR 0019): Purge is grace-bounded — gated on
# ``now - max_committed_at(table_persist_metadata) > query_timeout``.
# Tests that need to observe Purge actually deleting hot rows must
# sleep past the grace window before the Purge call. Mirrors the
# pattern in ``integration_grace_window_test.py``.
_QUERY_TIMEOUT_SECONDS = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "2"))
_GRACE_WAIT_SECONDS = _QUERY_TIMEOUT_SECONDS + 1.0


def _make_branch(client, catalog_uuid, name):
    """Create a branch on the catalog, return its UUID."""
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-220",
    )
    return branch.branch_uuid


def _create_tables_on_branch(client, catalog_uuid, schema_uuid, branch_uuid, names):
    """Create N user tables on a branch. Returns dict[name -> table_uuid]."""
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
    """Begin a tx, write ``rows`` to each table_uuid, commit. Returns the
    committed ``tx_uuid``.

    Post-CHA-222 ``CommitTxResponse`` only carries ``commit_micros``;
    callers that need the tx_uuid use the one allocated by ``begin_tx``.
    """
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

    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    return tx.tx_uuid


def _count_hot_upsert_log_rows(table_uuid, branch_uuid):
    """Count rows in the per-(table, branch) hot upsert log."""
    tbl = upsert_log_table(table_uuid, branch_uuid)
    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {}").format(Identifier(tbl)),
    )[0][0]


def _count_hot_delete_log_rows(table_uuid, branch_uuid):
    """Count rows in the per-(table, branch) hot delete log."""
    tbl = delete_log_table(table_uuid, branch_uuid)
    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {}").format(Identifier(tbl)),
    )[0][0]


def _count_table_persist_rows(catalog_uuid, branch_uuid, table_uuid):
    """Count ``table_persist_metadata`` rows for one ``(branch, table)`` pair."""
    parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )[0][0]


def _count_committed_persist_segments(catalog_uuid, branch_uuid, table_uuid):
    """Count plan-visible persist segments for one ``(branch, table)`` pair."""
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg_parent)),
        (branch_uuid, table_uuid),
    )[0][0]


def _latest_committed_purged_at(catalog_uuid, branch_uuid, table_uuid):
    """Return the latest committed purge watermark Pu (``last_purged_commit_seq_num``)
    for T, or None. CHA-444 (ADR 0027): the watermark is seq-axis now."""
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


def _latest_committed_snapshot_at(catalog_uuid, branch_uuid, table_uuid):
    """Return the latest committed ``snapshotted_at_micros`` for T, or None."""
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(snapshotted_at_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _count_committed_snapshot_rows(catalog_uuid, branch_uuid, table_uuid):
    """Count committed ``table_snapshot_metadata`` rows for T."""
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )[0][0]


def _commit_tx_deleting_rows(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, names
):
    """Begin a tx, delete the rows whose ``name`` PK is in ``names``,
    commit. Single-PK ``USER_SCHEMA`` only — composite-PK tables ship
    the batch inline at the call site instead."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            deletes=pa.table({"name": list(names)}, schema=_PK_SCHEMA_NAME),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


class TestPerTableLifecycle:
    def test_per_table_persist_isolation(self):
        """``Persist(T1)`` writes only T1's persist metadata + cold segments.

        Branch with three user tables, all written to. Per-table persist
        of T1 must produce exactly one ``table_persist_metadata`` row + a
        cold segment for T1, and leave T2 / T3 entirely untouched —
        no metadata rows, no cold segments, and hot rows preserved.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "iso")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1", "t2", "t3"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"], tables["t2"], tables["t3"]],
            {"name": ["alice"], "value": [1]},
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        # T1: one table_persist row + at least one committed cold segment;
        # hot row gone (it'll go away on purge; persist leaves hot intact
        # — see test_per_table_purge_correctness).
        assert _count_table_persist_rows(catalog_uuid, branch_uuid, tables["t1"]) >= 1
        assert (
            _count_committed_persist_segments(catalog_uuid, branch_uuid, tables["t1"])
            >= 1
        )

        # T2 / T3: zero persist metadata, zero cold segments, hot intact.
        for name in ("t2", "t3"):
            tu = tables[name]
            assert _count_table_persist_rows(catalog_uuid, branch_uuid, tu) == 0, (
                f"{name} should not have any table_persist rows after Persist(T1)"
            )
            assert _count_committed_persist_segments(catalog_uuid, branch_uuid, tu) == 0
            assert _count_hot_upsert_log_rows(tu, branch_uuid) == 1, (
                f"{name}'s hot upsert log must be intact after Persist(T1)"
            )

    def test_per_table_purge_correctness(self):
        """``Purge(T1)`` deletes T1's hot rows up to the purge fence ``Pu``.

        CHA-444 (ADR 0027): ``Pu = W_snap``, so a ``Snapshot(T1)`` must run
        before ``Purge(T1)`` can advance the fence and clear the committed hot
        rows with ``commit_seq_num <= Pu``. ``Purge`` returns ``purged_at_micros``
        carrying the seq fence ``Pu`` (legacy field name).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "purge_correct")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice", "bob"], "value": [1, 2]},
        )
        assert _count_hot_upsert_log_rows(tables["t1"], branch_uuid) == 2

        persist_response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        persisted_at = persist_response.persisted_at_micros
        assert persisted_at > 0

        # Hot still has the rows — persist no longer touches hot data.
        assert _count_hot_upsert_log_rows(tables["t1"], branch_uuid) == 2

        # CHA-444: Purge advances Pu only to W_snap, so Snapshot must run first.
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        purge_response = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert purge_response.HasField("purged_at_micros")
        pu = purge_response.purged_at_micros

        assert _count_hot_upsert_log_rows(tables["t1"], branch_uuid) == 0
        # Delete log is empty too (no deletes were written in this tx).
        assert _count_hot_delete_log_rows(tables["t1"], branch_uuid) == 0

        # ``table_purge_metadata`` carries the committed watermark Pu.
        assert (
            _latest_committed_purged_at(catalog_uuid, branch_uuid, tables["t1"]) == pu
        )

    def test_plan_cutoff_post_persist_serves_from_cold(self):
        """Between ``Persist(T)`` and ``Purge(T)``, reads serve from cold.

        Locks the CHA-233 (ADR 0019) cutoff source:
        ``persisted_at_micros`` is the hot/cold cutoff. The same rows
        physically live in both tiers post-persist (hot until Purge's
        grace-bounded delete runs), but plan()'s hot filter
        ``committed_at >= max_persisted + 1`` excludes them, so cold
        owns the read. End-to-end signal: ``read_data`` returns exactly
        N rows — no double-counting — and the hot log still physically
        holds the rows pending grace-bounded Purge.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "cut_pre_purge")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        assert _count_hot_upsert_log_rows(tables["t1"], branch_uuid) == 3
        assert (
            _count_committed_persist_segments(catalog_uuid, branch_uuid, tables["t1"])
            >= 1
        )

        # Read returns exactly 3 rows — no double-count between tiers.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=tables["t1"],
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 3
        assert sorted(result.column("name").to_pylist()) == ["alice", "bob", "carol"]

    def test_plan_cutoff_post_purge(self):
        """After ``Snapshot(T)`` → ``Purge(T)``, reads still return the rows
        from cold.

        Continuation of the previous test: post-purge, hot is empty. CHA-444
        (ADR 0027): Purge advances the read fence ``Pu`` only to ``W_snap``,
        so the Snapshot folds the rows into the cold snapshot baseline and
        Purge then clears the physical hot copy. The same N rows must come
        back via the baseline.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "cut_post_purge")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        # CHA-444: Snapshot advances W_snap so Purge can clear the hot rows.
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        assert _count_hot_upsert_log_rows(tables["t1"], branch_uuid) == 0
        assert (
            _count_committed_persist_segments(catalog_uuid, branch_uuid, tables["t1"])
            >= 1
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=tables["t1"],
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 3
        assert sorted(result.column("name").to_pylist()) == ["alice", "bob", "carol"]

    def test_no_op_purge_does_not_advance_watermark(self):
        """``Purge(T)`` on a brand-new (never-persisted) table is a
        true no-op: no ``table_purge_metadata`` row, no watermark
        advance, response ``purged_at_micros`` is unset.

        Under ADR 0019 the hot/cold cutoff is ``max_persisted + 1``,
        so ``plan()``'s structural hiding hazard moved with it: the
        no-op surface still matters for the downstream
        ``table_purge_metadata`` consumers (``purge_locked``'s
        idempotence check, CHA-221 branch-min commit_tx_log GC) — both must
        special-case "no purge row" rather than treat absence as
        ``0``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "noop_purge")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        # No data, no persist. Purge must succeed without writing a row.
        response = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not response.HasField("purged_at_micros")
        assert (
            _latest_committed_purged_at(catalog_uuid, branch_uuid, tables["t1"]) is None
        )

        # Second call is still a no-op — same shape, no row.
        response2 = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not response2.HasField("purged_at_micros")
        assert (
            _latest_committed_purged_at(catalog_uuid, branch_uuid, tables["t1"]) is None
        )

    def test_purge_without_persist_does_not_hide_hot_rows(self):
        """Regression for the data-hiding hazard: calling ``Purge(T)``
        on a dirty-but-never-persisted table must not advance the
        watermark past committed hot rows.

        If Purge fast-pathed to ``max(now, last_purged + 1)`` (the
        pre-fix behaviour), the hot rows would be excluded from both
        ``read_data`` (`plan()`) and ``audit_data`` (`plan_audit()`)
        and there'd be no cold copy to recover from. The fix is the
        true-no-op fast-path; this test pins the user-visible
        contract.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "purge_before_persist")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        # Commit rows but skip Persist; go straight to Purge.
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice", "bob"], "value": [1, 2]},
        )
        response = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not response.HasField("purged_at_micros")

        # Both rows remain visible — neither hot nor cold-filtered out.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=tables["t1"],
            branch_uuid=branch_uuid,
        )
        assert sorted(result.column("name").to_pylist()) == ["alice", "bob"]

    def test_persist_stops_touching_tx_log_family(self):
        """Per-table persist no longer purges the hot tx-log family.

        Pre-CHA-220 branch persist purged ``commit_tx_log`` / ``begin_tx_log`` /
        ``abort_tx_log`` / ``tx_table_log`` partitions up to the
        watermark. Per-table persist has no business touching them
        (they're shared across the branch). Rows for a committed tx
        survive the persist, leaking until
        [CHA-221](https://linear.app/chapala/issue/CHA-221) lands.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "commit_tx_log_leak")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        tx_uuid = _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        tx_part = commit_tx_log_partition(catalog_uuid, branch_uuid)
        begin_part = begin_tx_log_partition(catalog_uuid, branch_uuid)
        tx_table_part = tx_table_log_partition(catalog_uuid, branch_uuid)

        # commit_tx_log: committed tx's row survives the per-table persist.
        commit_tx_log_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(
                Identifier(tx_part)
            ),
            (tx_uuid,),
        )[0][0]
        assert commit_tx_log_count == 1, (
            "commit_tx_log row for committed tx must survive per-table persist "
            "(branch-scoped purge is gone; CHA-221 owns cleanup)"
        )

        # begin_tx_log: same — survives.
        begin_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(
                Identifier(begin_part)
            ),
            (tx_uuid,),
        )[0][0]
        assert begin_count == 1, "begin_tx_log row must survive per-table persist"

        # tx_table_log: the per-(tx, table) summary row also survives.
        tx_table_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(
                Identifier(tx_table_part)
            ),
            (tx_uuid,),
        )[0][0]
        assert tx_table_count >= 1, "tx_table_log row must survive per-table persist"

        # Open + abort a tx — its abort_tx_log row must survive too.
        tx_to_abort = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.abort_tx(
            tx_to_abort.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        abort_part = abort_tx_log_partition(catalog_uuid, branch_uuid)
        abort_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(
                Identifier(abort_part)
            ),
            (tx_to_abort.tx_uuid,),
        )[0][0]
        assert abort_count == 1, "abort_tx_log row must survive per-table persist"

    # Serial for contention, not for a side channel: this parks a servicer PG
    # connection (of PG_POOL_MAX=4) while asserting a bounded time, so concurrent
    # workers would make it flaky in both directions. Outlives CHA-519.
    @pytest.mark.serial
    def test_lock_serialization_per_table(self):
        """Persist's lock key is ``persist:{table_uuid}:{branch_uuid}``
        (ADR 0019 §"Lock scoping").

        Whitebox: hold ``persist:{T1}:{branch}`` from a separate
        connection; ``Persist(T1)`` must block on it. Concurrently,
        ``Persist(T2)`` on the same branch (different table → different
        lock key) runs to completion. Releasing the held lock unblocks
        ``Persist(T1)``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "lock_ser")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1", "t2"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"], tables["t2"]],
            {"name": ["alice"], "value": [1]},
        )

        holder = make_lock_driver()
        persist_t1_done = threading.Event()
        persist_t2_done = threading.Event()
        persist_t1_response = {}
        persist_t2_response = {}

        lock_key_t1 = f"persist:{tables['t1']}:{branch_uuid}"

        def run_persist_t1():
            persist_t1_response["value"] = client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=tables["t1"],
            )
            persist_t1_done.set()

        def run_persist_t2():
            persist_t2_response["value"] = client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=tables["t2"],
            )
            persist_t2_done.set()

        with holder.advisory_lock(lock_key_t1):
            t1_thread = threading.Thread(target=run_persist_t1, daemon=True)
            t2_thread = threading.Thread(target=run_persist_t2, daemon=True)
            t1_thread.start()
            t2_thread.start()

            # T2 runs to completion: different lock key → no contention.
            assert persist_t2_done.wait(timeout=10.0), (
                "Persist(T2) blocked while Persist(T1)'s lock was held — "
                "lock key must be per-table, not branch-wide"
            )

            # T1 stays blocked while we hold its lock.
            assert not persist_t1_done.wait(timeout=1.0), (
                "Persist(T1) returned while its own lock was held — "
                "advisory lock not acquired by lifecycle"
            )

        # Lock released — T1 completes.
        assert persist_t1_done.wait(timeout=10.0), (
            "Persist(T1) did not complete after lock release"
        )
        t1_thread.join(timeout=1.0)
        t2_thread.join(timeout=1.0)

        assert persist_t1_response["value"].persisted_at_micros > 0
        assert persist_t2_response["value"].persisted_at_micros > 0

        holder.close()

    def test_no_op_snapshot_does_not_advance_watermark(self):
        """``Snapshot(T)`` with no committed persist newer than the
        last snapshot is a true no-op: no new ``table_snapshot_metadata``
        row, response ``snapshotted_at_micros`` unset.

        Symmetric to ``test_no_op_purge_does_not_advance_watermark``
        (CHA-220). Before CHA-228 the second snapshot fell through to
        a redundant cold merge-read and tried to re-insert the same
        deterministic snapshot row.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "noop_snap")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        first = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert first.HasField("snapshotted_at_micros")
        first_watermark = first.snapshotted_at_micros
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 1
        )

        # Second snapshot with no intervening Persist must be a no-op.
        second = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not second.HasField("snapshotted_at_micros")
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 1
        )
        assert (
            _latest_committed_snapshot_at(catalog_uuid, branch_uuid, tables["t1"])
            == first_watermark
        )

    def test_snapshot_on_never_persisted_table_is_no_op(self):
        """``Snapshot(T)`` on a brand-new (never-persisted) table is a
        true no-op: no ``table_snapshot_metadata`` row, response
        ``snapshotted_at_micros`` unset. Sibling of
        ``test_no_op_purge_does_not_advance_watermark`` for the
        empty-cold path.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "snap_never_persisted")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not response.HasField("snapshotted_at_micros")
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 0
        )

    def test_snapshot_resumes_after_fresh_persist_following_no_op(self):
        """Tightening the no-op early-exit must not leave Snapshot
        permanently stuck. ``Persist → Snapshot → Snapshot (no-op) →
        Persist → Snapshot`` advances the watermark with a real second
        snapshot. CHA-468 decoupled retirement from Snapshot, so the
        second commit no longer drops the first — both committed
        ``table_snapshot_metadata`` rows survive.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "snap_resumes")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        first = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert first.HasField("snapshotted_at_micros")
        first_watermark = first.snapshotted_at_micros

        # No-op snapshot in between — must not block the next real one.
        noop = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not noop.HasField("snapshotted_at_micros")

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["bob"], "value": [2]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        second = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert second.HasField("snapshotted_at_micros")
        assert second.snapshotted_at_micros > first_watermark
        # CHA-468: Snapshot no longer retires, so both the first and the
        # second committed snapshot rows survive.
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 2
        )

    def test_lifecycle_noops_use_field_presence(self):
        """All three lifecycle RPCs return their ``*_at_micros``
        field unset on a no-op and set when the call did work. Pins
        the proto convention end-to-end so callers can distinguish
        "RPC did nothing" from "watermark is at value X" without
        overloading ``0``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "noop_field_presence")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        # No-op leg: never-persisted T. All three RPCs no-op.
        persist_noop = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        snapshot_noop = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        purge_noop = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not persist_noop.HasField("persisted_at_micros")
        assert not snapshot_noop.HasField("snapshotted_at_micros")
        assert not purge_noop.HasField("purged_at_micros")

        # Real-work leg: same RPCs after committed data exist.
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        persist_real = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        snapshot_real = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        # CHA-233: Purge is grace-bounded; the "real-work" Purge needs
        # the Persist's commit to age out of the universal grace window.
        time.sleep(_GRACE_WAIT_SECONDS)
        purge_real = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert persist_real.HasField("persisted_at_micros")
        assert snapshot_real.HasField("snapshotted_at_micros")
        assert purge_real.HasField("purged_at_micros")
        assert persist_real.persisted_at_micros > 0
        assert snapshot_real.snapshotted_at_micros > 0
        assert purge_real.purged_at_micros > 0

    def test_snapshot_records_watermark_when_merge_nets_to_empty(self):
        """Empty-merge case: a Persist whose new rows are all tombstoned
        by a later Persist nets to zero rows after the merge. Snapshot
        must still commit a ``table_snapshot_metadata`` row at the new
        watermark so subsequent calls fast-path. Without the placeholder
        the next Snapshot would re-derive ``cold_data_max = Some(v)``
        from the same persist segments and redo the merge-read forever.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "empty_merge")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        # Insert one row, delete it, then persist + snapshot. The cold
        # merge collapses to zero rows but the persist watermark is
        # still real — Snapshot must record it.
        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        _commit_tx_deleting_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            tables["t1"],
            ["alice"],
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert response.HasField("snapshotted_at_micros")
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 1
        )
        assert (
            _latest_committed_snapshot_at(catalog_uuid, branch_uuid, tables["t1"])
            == response.snapshotted_at_micros
        )

    def test_snapshot_after_empty_merge_fast_paths(self):
        """The empty-merge placeholder must surface its watermark to
        the planner so the next Snapshot(T) hits the CHA-228 pre-merge
        fast-path. Pins the symmetric no-op tightening against the
        post-merge case: re-snapshot after the empty merge is a true
        no-op, not a re-run of the cold merge-read.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "empty_merge_resume")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        _commit_tx_deleting_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            tables["t1"],
            ["alice"],
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        first = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert first.HasField("snapshotted_at_micros")

        # No new persist between snapshots → second call must no-op
        # symmetrically with Purge.
        second = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert not second.HasField("snapshotted_at_micros")
        assert (
            _count_committed_snapshot_rows(catalog_uuid, branch_uuid, tables["t1"]) == 1
        )

    def test_time_travel_at_empty_snapshot_watermark_returns_no_rows(self):
        """Reading at the empty snapshot's watermark must return zero
        rows. Verifies the placeholder is semantically equivalent to a
        snapshot with all rows tombstoned — not just a metadata
        bookkeeping entry, but a real point-in-time view that
        ``read_data`` respects.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "empty_merge_read")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        _commit_tx_deleting_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            tables["t1"],
            ["alice"],
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert result.num_rows == 0


def _max_committed_persist_segment_max_tx(catalog_uuid, branch_uuid, table_uuid):
    """Return ``max(max_tx_commit_micros)`` over committed persist
    segments for ``(branch, table)`` — the *true* watermark over the
    rows already in cold. CHA-227 pins ``persisted_at_micros`` to this
    value; pre-CHA-227 the field carried ``effective_target`` (a
    target/cap, not derived from actual data).
    """
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(max_tx_commit_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg_parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


class TestPerTableLifecycleCHA227:
    """CHA-227 pins ``persisted_at_micros`` to ``max(committed_at)`` over
    the rows we just wrote, decouples ``snapshot_locked`` from
    ``MetadataClient::plan``, and reads snapshot/persist watermarks
    directly. See the CHA-227 implementation plan v10."""

    def test_persist_stamps_watermark_at_max_committed_at(self):
        """``persisted_at_micros`` is derived from the rows we just
        persisted (``max(seg.max_tx_commit_micros)``), NOT from
        ``effective_target`` (the watermark cap). Locks the CHA-227
        stamping rule: when ``target_micros`` overshoots, the stamp
        clamps down to the actual data's max, not the request's cap.
        Symmetric to Purge stamping (which already uses
        ``persist.persisted_at_micros``).

        CHA-221 v2.1 (ADR 0021) broadens the CHA-227 stamping rule to
        ``persisted_at = max(max(committed_at over persisted rows),
        max(aborted_at over aborted txs whose hot rows Persist just
        cleaned))``. This test's setup has no aborts in the eligibility
        window, so the broadened max collapses to the original
        ``max(committed_at)`` and the assertion is preserved as-is. The
        all-aborts case is covered by
        ``test_aborted_only_table_flows_through_pipeline`` in
        ``integration_purge_tx_log_test.py``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "stamp_at_max")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        batch = pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA)
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=tables["t1"], upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        committed = client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )
        actual_committed_at = committed.commit_micros

        # ``target_micros`` 10s past the actual commit. Pre-CHA-227 the
        # stamp would carry this future cap; post-CHA-227 it clamps to
        # the actual data's max.
        future_target = actual_committed_at + 10_000_000
        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
            target_micros=future_target,
        )
        assert response.HasField("persisted_at_micros")
        assert response.persisted_at_micros == actual_committed_at, (
            f"persisted_at_micros must equal max(committed_at) over the rows "
            f"persisted ({actual_committed_at}); got "
            f"{response.persisted_at_micros} (target_micros was {future_target})"
        )
        # Cross-check the segment table directly: the stamped watermark
        # equals the max seg.max_tx.
        assert (
            _max_committed_persist_segment_max_tx(
                catalog_uuid, branch_uuid, tables["t1"]
            )
            == response.persisted_at_micros
        )

    def test_persist_respects_open_tx_clamp_under_new_stamping(self):
        """The open-tx clamp invariant
        (``persisted_at < every open tx.began_at``) survives the
        CHA-227 stamping rule shift. Pre-CHA-227 the stamp was always
        ``effective_target = min(target, oldest_open.began_at - 1)``,
        which made the invariant a tautology. Post-CHA-227 the stamp is
        ``max(committed_at)`` over persisted rows — the open-tx
        invariant must hold transitively because Persist still gates
        the rows it pulls from hot by ``effective_target``.

        Sibling of ``test_persist_respects_open_tx_clamp`` but pins the
        tighter equality: the stamp equals tx_b's exact
        ``commit_micros`` (not an arbitrarily-padded
        ``effective_target`` below ``tx_a.began_at``).

        CHA-221 v2.1 (ADR 0021) broadens the stamping rule to also
        include aborted_at over aborted txs whose hot rows Persist just
        cleaned. This test's setup has no aborts, so the broadened max
        collapses to ``max(committed_at)`` and the assertion remains
        the same. The open-tx clamp now transitively bounds both
        committed_at and aborted_at via Persist's existing
        ``effective_target``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "open_clamp_stamping")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        # tx_b commits a row; its committed_at < tx_a.began_at.
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        batch = pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA)
        client.write_data(
            tx_b.tx_uuid,
            Mutation(table_uuid=tables["t1"], upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        committed_b = client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

        # tx_a opens — clamps the watermark. target_micros far past
        # tx_a.began_at: both clamps must apply (open-tx + data-max).
        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        future_target = tx_a.began_at_micros + 10_000_000
        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
            target_micros=future_target,
        )

        # Open-tx invariant holds: stamp < tx_a.began_at.
        assert response.persisted_at_micros < tx_a.began_at_micros
        # New stamping rule: stamp == tx_b's exact committed_at, not an
        # opaque ``effective_target`` between committed_b and tx_a.began_at.
        assert response.persisted_at_micros == committed_b.commit_micros, (
            f"new stamping rule: persisted_at_micros must equal "
            f"max(committed_at) over persisted rows "
            f"({committed_b.commit_micros}); got "
            f"{response.persisted_at_micros}"
        )

        client.abort_tx(
            tx_a.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
        )

    def test_snapshot_reads_watermarks_directly(self):
        """CHA-227 decouples ``snapshot_locked`` from
        ``MetadataClient::plan``: it reads
        ``latest_committed_table_persist_watermark`` and
        ``read_snapshot_segments_for_table`` directly on the pool —
        plan-time atomicity is via explicit threading
        (``persisted_at`` bounds ``upper`` bounds the segment fetch's
        ``to_micros``), not a surrounding REPEATABLE READ tx — and
        stamps the new snapshot's ``snapshotted_at_micros`` to the
        persist watermark (``max(seg.max_tx)`` over included
        segments), not the post-resolution merged-batch's
        ``max(committed_at)``.

        Locks two things at once:
        1. Stamping rule: ``snapshotted_at_micros`` matches
           ``max(seg.max_tx_commit_micros)`` over the persist
           segments included in the snapshot.
        2. Symmetry with Persist (CHA-227 #1): both watermarks are
           ``max(committed_at)`` over the raw input rows.

        CHA-221 v2.1 (ADR 0021): Persist's stamping broadens to
        ``max(committed_at, aborted_at)``, but persist segments still
        contain only committed rows (aborted hot rows are deleted by
        Persist, not moved to cold). So ``max_seg.max_tx_committed_at``
        is unchanged for this test (no aborts in setup), and
        Snapshot's stamping symmetry holds. The all-aborts case —
        where Snapshot stamps directly from ``persisted_at`` with no
        segments — is covered by
        ``test_aborted_only_table_flows_through_pipeline`` in
        ``integration_purge_tx_log_test.py``.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "snap_watermarks")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice", "bob"], "value": [1, 2]},
        )
        persist_response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        snapshot_response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        assert snapshot_response.HasField("snapshotted_at_micros")

        # Snapshot stamp == max(seg.max_tx) == Persist stamp (CHA-227
        # tight symmetry: both derive from the same raw persist-log rows).
        max_seg = _max_committed_persist_segment_max_tx(
            catalog_uuid, branch_uuid, tables["t1"]
        )
        assert snapshot_response.snapshotted_at_micros == max_seg, (
            f"snapshot watermark must equal max(seg.max_tx) over included "
            f"persist segments ({max_seg}); got "
            f"{snapshot_response.snapshotted_at_micros}"
        )
        assert (
            snapshot_response.snapshotted_at_micros
            == persist_response.persisted_at_micros
        ), (
            "Snapshot watermark must equal the Persist watermark when no "
            "snapshot precedes — both derive from the same raw rows"
        )

    def test_snapshot_with_as_of_caps_at_persisted_at(self):
        """Snapshot's ``snapshot_at`` is an upper bound on the window,
        not the stamped watermark. CHA-227's snapshot rewrite computes
        ``upper = min(persisted_at, request.snapshotted_at_micros)``.
        Passing ``snapshot_at`` past the persist watermark must clamp
        the stamp at ``persisted_at`` — anything else would write a
        watermark past data that doesn't exist in cold.
        """
        from penca_client._time import micros_to_datetime

        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "snap_as_of_cap")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t1"]
        )

        _commit_tx_writing_rows(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t1"]],
            {"name": ["alice"], "value": [1]},
        )
        persist_response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
        )
        persisted_at = persist_response.persisted_at_micros

        # snapshot_at far past persisted_at — clamp to persisted_at.
        far_future = persisted_at + 10_000_000
        snapshot_response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t1"],
            snapshot_at=micros_to_datetime(far_future),
        )
        assert snapshot_response.HasField("snapshotted_at_micros")
        assert snapshot_response.snapshotted_at_micros == persisted_at, (
            f"snapshot_at past the persist watermark must clamp to "
            f"persisted_at ({persisted_at}); got "
            f"{snapshot_response.snapshotted_at_micros}"
        )


class TestPerTableLifecycleProtoShape:
    """Lock the proto reshape in. These fail at import / HasField time
    until ``PersistRequest`` carries the per-table identifier block and
    ``PurgeRequest`` exists.
    """

    def test_persist_request_carries_table_identifier(self):
        req = PersistRequest()
        for field in ("schema_uuid", "schema_name", "table_uuid", "table_name"):
            # ``HasField`` raises ``ValueError`` for unknown fields —
            # success here means the field exists on the message.
            req.HasField(field)

    def test_purge_request_carries_table_identifier(self):
        req = PurgeRequest()
        for field in (
            "catalog_uuid",
            "catalog_name",
            "schema_uuid",
            "schema_name",
            "branch_uuid",
            "branch_name",
            "table_uuid",
            "table_name",
        ):
            req.HasField(field)

    def test_persist_response_carries_only_persisted_at_micros(self):
        """Slim response — no ``rows_persisted`` / ``segment_uuids``."""
        from penca_proto.external.v1.lifecycle_pb2 import PersistResponse

        field_names = {f.name for f in PersistResponse.DESCRIPTOR.fields}
        assert field_names == {"persisted_at_micros"}, (
            f"PersistResponse must carry only persisted_at_micros, got {field_names}"
        )

    def test_purge_response_carries_only_purged_at_micros(self):
        from penca_proto.external.v1.lifecycle_pb2 import PurgeResponse

        field_names = {f.name for f in PurgeResponse.DESCRIPTOR.fields}
        assert field_names == {"purged_at_micros"}, (
            f"PurgeResponse must carry only purged_at_micros, got {field_names}"
        )

    def test_snapshot_response_carries_only_snapshotted_at_micros(self):
        """CHA-228: slim response — harmonized with PersistResponse /
        PurgeResponse. No ``table_snapshot_uuid`` / ``rows_in_snapshot``
        / ``table_snapshot_segment_uuids``."""
        from penca_proto.external.v1.lifecycle_pb2 import SnapshotResponse

        field_names = {f.name for f in SnapshotResponse.DESCRIPTOR.fields}
        assert field_names == {"snapshotted_at_micros"}, (
            f"SnapshotResponse must carry only snapshotted_at_micros, got {field_names}"
        )

    def test_lifecycle_response_watermark_fields_are_optional(self):
        """CHA-228: ``persisted_at_micros`` / ``purged_at_micros`` /
        ``snapshotted_at_micros`` are all proto3 ``optional`` so a
        no-op leaves the field unset (distinguishable from a real
        ``0`` watermark via ``HasField``).
        """
        from penca_proto.external.v1.lifecycle_pb2 import (
            PersistResponse,
            PurgeResponse,
            SnapshotResponse,
        )

        for cls, field in (
            (PersistResponse, "persisted_at_micros"),
            (PurgeResponse, "purged_at_micros"),
            (SnapshotResponse, "snapshotted_at_micros"),
        ):
            cls().HasField(field)  # raises ValueError if not optional
