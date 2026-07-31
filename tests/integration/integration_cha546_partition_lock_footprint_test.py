"""CHA-546 red test: branch ops must take no lock on a metadata parent.

The 8 metadata tables are LIST-partitioned by ``branch_uuid``, but their
call sites named the catalog-wide parent and let Postgres route. Naming a
parent takes a lock on the parent; naming a partition takes none. That
difference costs twice:

* a writer holding ``RowExclusiveLock`` on the parent deadlocks branch
  teardown, which walks leaf-to-parent while the writer walks
  parent-to-leaf;
* compact's ``enumerate_unsealed_persist_segments_for_scope`` opens with
  ``SELECT ... FOR UPDATE OF seg`` on the ``table_persist_segment_metadata``
  parent and holds ``ROW SHARE`` there across its whole cold read and
  merged write. Teardown's ``DROP TABLE`` needs ``ACCESS EXCLUSIVE`` on
  that same parent under a transaction-scoped 5s ``lock_timeout``, so
  ``DeleteBranch`` fails ``Aborted`` whenever any compact in the catalog
  has been mid-merge for more than five seconds. That is ordinary
  operation, not a race — and it is a *read* that causes it.

Both costs are the same fact, so one fixture covers both: hold
``ACCESS EXCLUSIVE`` on ``ONLY`` the 8 parents from an out-of-band session
— what a mid-``DROP TABLE`` teardown holds against every branch other than
the one it is deleting — and require every branch-scoped operation to
finish anyway.

Rejected alternatives, so a later reader does not "fix" this back into one:

* **Race a real compact against DeleteBranch.** Needs a compact to stay
  mid-merge for over five seconds to trip the timeout. Inherently flaky.
* **Simulate compact's lock with a raw ``SELECT ... FOR UPDATE`` on the
  partition.** That passes before the fix, so it is not a red test.

Setup runs to completion *before* the locks are taken: catalog, schema,
table, and branch creation are DDL, and DDL names parents by design.

Run via ``just integration-test cha546_partition_lock_footprint``.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

import pyarrow as pa
import pytest
from penca_client.naming import (
    COMPACT_SEGMENT_METADATA,
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_PURGE_METADATA,
    TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    make_lock_driver,
    setup_partitioned_table,
    write_and_persist,
    write_cycle,
)

# The 8 partitioned metadata tables. Excludes
# ``tx_log_persist_segment_metadata`` (CHA-507) and ``segment_delete_set``
# (CHA-531), which are catalog-wide and unpartitioned by design.
METADATA_PARENT_TAGS = (
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_PURGE_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    COMPACT_SEGMENT_METADATA,
    TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
)

# Generous relative to any of these ops on an idle stack, so a failure means
# "blocked on a lock", not "slow".
OP_DEADLINE_S = 20.0

# The holder must win its own locks first. A branch-scoped statement that
# names a parent can make even this contended, so failing here is the same
# defect surfacing one step earlier — the message says so. `lock_timeout` is
# per *statement*, so this bounds the whole acquisition only because all 8
# parents are locked by a single `LOCK TABLE`.
HOLDER_ACQUIRE_TIMEOUT_S = 30.0

_SEED = pa.table({"name": ["alice", "bob"], "value": [1, 2]}, schema=USER_SCHEMA)
_MORE = pa.table({"name": ["carol", "dave"], "value": [3, 4]}, schema=USER_SCHEMA)


class _ParentLockHolder:
    """Holds ``ACCESS EXCLUSIVE`` on every metadata parent in one transaction.

    ``ONLY`` is what makes this a valid model of teardown and a test that can
    actually go green: without it Postgres locks the named table *and every
    descendant*, so the fixture would hold the branch's own leaves and block a
    partition-targeted statement too. Teardown locks the deleted branch's
    leaves plus the parent descriptor — never a sibling branch's leaves — so
    parents-only is the state under test.

    All 8 are locked by a single statement because ``lock_timeout`` is
    per-statement: eight statements would let acquisition run to 8× the bound.

    Owns its own single connection rather than borrowing ``get_pg_driver()``'s
    shared pool: the locks must live exactly as long as this transaction, and a
    pooled connection handed back mid-hold would carry them to another caller.
    """

    def __init__(self, catalog_uuid: str) -> None:
        self._parents = [f"{catalog_uuid}_{tag}" for tag in METADATA_PARENT_TAGS]
        self._driver = make_lock_driver()
        self._held = threading.Event()
        self._release = threading.Event()
        # `_held` is also set on the failure path so `__enter__` never hangs, so
        # it alone does not mean the locks are held — this does.
        self._acquired = False
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        try:
            with self._driver.transaction() as tx:
                tx.execute_no_result(
                    f"SET LOCAL lock_timeout = '{int(HOLDER_ACQUIRE_TIMEOUT_S)}s'"
                )
                tx.execute_no_result(
                    SQL("LOCK TABLE {tbls} IN ACCESS EXCLUSIVE MODE").format(
                        tbls=SQL(", ").join(
                            SQL("ONLY {tbl}").format(tbl=Identifier(parent))
                            for parent in self._parents
                        )
                    )
                )

                self._acquired = True
                self._held.set()
                self._release.wait()
                raise _Rollback()
        except _Rollback:
            pass
        except BaseException as exc:  # noqa: BLE001 - surfaced to the test thread
            self._error = exc
        finally:
            self._held.set()
            self._driver.close()

    def __enter__(self) -> _ParentLockHolder:
        self._thread.start()
        signalled = self._held.wait(timeout=HOLDER_ACQUIRE_TIMEOUT_S + 5.0)
        if not signalled or not self._acquired:
            # Silently proceeding here would run both tests with no locks held
            # and report green — the worst outcome for a red test.
            self._release.set()
            raise AssertionError(
                "could not take ACCESS EXCLUSIVE on the metadata parents "
                f"({self._parents}). Something else is holding a lock on them — "
                "which is itself the CHA-546 defect, one step earlier."
            ) from self._error

        return self

    def __exit__(self, *_exc) -> None:
        self._release.set()
        self._thread.join(timeout=30.0)


class _Rollback(Exception):
    """Unwinds the holder's transaction without committing."""


def _within_deadline(label: str, fn, *args, **kwargs):
    """Run ``fn`` on a worker thread and fail if it does not return in time.

    The client exposes no per-call deadline, so the timeout lives here. The
    worker stays blocked on the RPC after a timeout; the holder's ``__exit__``
    releases the locks and lets it drain, which is why the executor is not
    shut down with ``wait=True``.
    """
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=OP_DEADLINE_S)
        except FutureTimeout:
            pytest.fail(
                f"{label} did not complete within {OP_DEADLINE_S}s while another "
                "session held ACCESS EXCLUSIVE on the 8 metadata parents. A "
                "branch-scoped statement is still naming a parent instead of the "
                "branch's partition (CHA-546)."
            )
    finally:
        executor.shutdown(wait=False)


@pytest.fixture(scope="module")
def seeded_branch():
    """Catalog + partitioned table + a branch with persist and snapshot state.

    Module-scoped: the setup is DDL-heavy and identical for both tests, and
    neither test mutates state the other reads.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid = (
        setup_partitioned_table("cha546_lockfoot")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch_uuid,
        upserts=_SEED,
    )
    # Leaves an unsealed persist tail so compact has segments to merge.
    write_and_persist(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch_uuid,
        upserts=_MORE,
    )
    return client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid


class TestParentLockFootprint:
    def test_branch_writes_proceed_under_parent_access_exclusive(self, seeded_branch):
        """Cost 1: the write path must not contend with teardown."""
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = seeded_branch

        with _ParentLockHolder(catalog_uuid):
            _within_deadline(
                "write -> commit -> persist",
                write_and_persist,
                client,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
                upserts=pa.table({"name": ["erin"], "value": [5]}, schema=USER_SCHEMA),
            )
            _within_deadline(
                "snapshot",
                client.snapshot,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
            )

    def test_branch_reads_and_compaction_proceed_under_parent_access_exclusive(
        self, seeded_branch
    ):
        """Cost 2: the read path — the larger of the two, and the ticket's point."""
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = seeded_branch

        with _ParentLockHolder(catalog_uuid):
            # enumerate_unsealed_persist_segments_for_scope's
            # `SELECT ... FOR UPDATE OF seg` — the site that makes DeleteBranch
            # fail Aborted in steady state.
            _within_deadline(
                "compact_persist_segments",
                client.compact_persist_segments,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
            )
            _within_deadline(
                "purge",
                client.purge,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
            )
            # meta_plan.rs: phase_one_fence_and_existence,
            # read_and_classify_persist_segments, hot_min_and_snapshot_pick.
            result = _within_deadline(
                "read_data",
                client.read_data,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                table_uuid=table_uuid,
                branch_uuid=branch_uuid,
            )

        assert result.num_rows > 0, "read returned no rows — setup did not seed"
