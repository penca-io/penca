"""[CHA-428] commit_seq_num — a monotonic, gapless commit-order serial allocated
at commit from a per-branch ``commit_tx_log_seq_num`` counter row.

These are the red-phase acceptance tests for the core allocation primitive.
They fail against the pre-CHA-428 schema:

* ``commit_tx_log`` has no ``commit_seq_num`` column → ``SELECT commit_seq_num`` raises
  psycopg ``UndefinedColumn``.
* the per-branch ``commit_tx_log_seq_num`` counter table does not exist → querying its
  partition raises ``UndefinedTable``.

They pass once IMPL-B/C/D/E land: the ``commit_tx_log_seq_num`` table + seeded counter
rows, the ``commit_seq_num`` column, and gapless per-commit allocation (genesis
included).

The load-bearing invariant (RT2) is *atomic-with-visibility*: the serial is
allocated by a counter-row UPDATE inside the committing statement, holding the
row lock to transaction end, so allocation order == commit-visibility order.
``race_repro.py`` is a design counter-example demonstrating the
``nextval``/IDENTITY race on a naive allocator — it is NOT part of this suite.

Run via ``just integration-test integration_commit_seq_num``.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.naming import (
    abort_tx_log_partition,
    begin_tx_log_partition,
    commit_tx_log_partition,
    row_uuid_for_pk,
)
from psycopg.sql import Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
    setup_schema,
)


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _commit_tx_log_seq_num_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Per-branch ``commit_tx_log_seq_num`` counter partition name.

    Computed inline via the public ``row_uuid_for_pk`` convention so the RED
    failure is "relation does not exist", not an ImportError on a helper
    IMPL-A has not added yet. Mirrors the ``commit_tx_log_partition`` derivation
    (``naming.py``) with the ``"commit_tx_log_seq_num"`` partition tag — IMPL-A adds
    ``penca_client.naming.commit_tx_log_seq_num_partition`` with this exact shape.
    """
    partition_uuid = row_uuid_for_pk(
        catalog_uuid, [branch_uuid, "commit_tx_log_seq_num"]
    )
    return f"{partition_uuid}_commit_tx_log_seq_num_partition"


def _abort_seq_num_partition(catalog_uuid: str, branch_uuid: str) -> str:
    """Per-branch ``abort_seq_num`` counter partition name (CHA-444 / ADR 0027).

    The abort-order counter is the abort-axis sibling of ``commit_tx_log_seq_num`` —
    a dedicated, gapless monotone counter, NOT a sample of the commit counter.
    Same ``row_uuid_for_pk`` derivation with the ``"abort_seq_num"`` tag.
    """
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, "abort_seq_num"])
    return f"{partition_uuid}_abort_seq_num_partition"


def _data_log_prefix(table_uuid: str, branch_uuid: str) -> str:
    """Shared name prefix for a (table, branch)'s hot data objects — the
    ``upsert_log`` / ``delete_log`` tables and the CHA-431
    ``write_sequence``. Mirrors the Rust
    ``row_uuid_for_pk(table_uuid, [branch_uuid])`` derivation so the RED
    failure is "relation/column does not exist", not an ImportError on a
    naming helper IMPL1 has not added yet."""
    return row_uuid_for_pk(table_uuid, [branch_uuid])


def _write_sequence_name(table_uuid: str, branch_uuid: str) -> str:
    """Per-(table, branch) ``write_sequence`` (CHA-431) — the lock-free PG
    sequence ``write_seq_num`` is allocated from, created at table birth at
    ``START 0``. IMPL1 adds ``penca_client.naming.write_sequence``
    producing this exact name."""
    return f"{_data_log_prefix(table_uuid, branch_uuid)}_data_write_seq"


def _upsert_log_table(table_uuid: str, branch_uuid: str) -> str:
    return f"{_data_log_prefix(table_uuid, branch_uuid)}_data_upsert_log"


def _tx_seq_rows(catalog_uuid: str, branch_uuid: str):
    """All ``(commit_seq_num, commit_micros)`` on a branch, ordered by seq.

    Ordered by ``commit_seq_num`` (not ``commit_micros``): wall-clock micros
    is microsecond-resolution, so concurrent commits can tie on
    ``commit_micros`` and a ``commit_micros`` sort would return tied
    rows in arbitrary order — making any seq-ordering assertion flaky. Seq is
    the unique, total order; callers assert ``commit_micros`` is
    *non-decreasing* across it (tie-tolerant) to verify timestamp order tracks
    seq order.
    """
    return get_pg_driver().execute(
        f"SELECT commit_seq_num, commit_micros "
        f"FROM {_qi(commit_tx_log_partition(catalog_uuid, branch_uuid))} "
        f"ORDER BY commit_seq_num",
    )


def _max_commit_seq(catalog_uuid: str, branch_uuid: str) -> int:
    """The branch's highest committed ``commit_seq_num`` (its commit-log frontier).

    Precondition: the branch has at least one committed tx — ``max()`` over an
    empty commit log raises ``ValueError``. Every caller targets a
    genesis-backed branch (``main`` or a forked child), so this always holds.
    """
    return max(r[0] for r in _tx_seq_rows(catalog_uuid, branch_uuid))


def _commit_upsert(client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, name):
    """begin → upsert one row → commit; returns the committed tx response."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table({"name": [name], "value": [1]}, schema=USER_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    return client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )


def _commit_delete(client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, name):
    """begin → delete one row by PK → commit; returns the committed tx response."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            deletes=pa.table(
                {"name": [name]}, schema=pa.schema([USER_SCHEMA.field("name")])
            ),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    return client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )


class TestCommitSeqNumAllocation:
    """RT1 — first commit = 0; subsequent commits increment monotonically."""

    def test_genesis_is_zero_and_sequence_is_gapless_monotonic(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # A few more commits beyond genesis / schema / table.
        for i in range(3):
            client.create_schema(
                f"more_{i}", catalog_uuid=catalog_uuid, author="t", comment="c"
            )

        rows = _tx_seq_rows(catalog_uuid, main_branch_uuid)
        seqs = [r[0] for r in rows]

        # Rows are seq-ordered; values must be the gapless run 0..N-1.
        assert seqs == list(range(len(seqs))), f"expected gapless 0..N; got {seqs}"

        # The genesis tx (chronologically first commit) carries commit_seq_num 0.
        genesis = get_pg_driver().execute(
            f"SELECT commit_seq_num FROM {_qi(commit_tx_log_partition(catalog_uuid, main_branch_uuid))} "
            f"WHERE comment = %s",
            ("catalog genesis",),
        )
        assert genesis[0][0] == 0, f"genesis commit_seq_num should be 0; got {genesis}"


class TestCommitSeqNumConcurrency:
    """RT2 — atomic-with-visibility under concurrent commits on one branch."""

    def test_concurrent_commits_unique_gapless_and_timestamp_ordered(self):
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # K concurrent committers on the SAME branch. Each thread uses its own
        # client; create_schema is one committed tx (catalog-scoped) per call.
        k = 8

        def commit_one(i: int) -> None:
            make_client().create_schema(
                f"cc_{i}_{uuid4().hex[:6]}",
                catalog_uuid=catalog_uuid,
                author="t",
                comment="c",
            )

        with ThreadPoolExecutor(max_workers=k) as pool:
            list(pool.map(commit_one, range(k)))

        rows = _tx_seq_rows(catalog_uuid, main_branch_uuid)
        seqs = [r[0] for r in rows]
        ts = [r[1] for r in rows]

        # Rows are seq-ordered → unique + gapless 0..N-1.
        assert len(set(seqs)) == len(seqs), f"commit_seq_num not unique: {seqs}"
        assert seqs == list(range(len(seqs))), f"not gapless 0..N: {seqs}"
        # Timestamp order tracks seq order: commit_micros is non-decreasing
        # across ascending commit_seq_num. Tie-tolerant (equal micros are fine —
        # microsecond wall-clock can tie under concurrency), but a true ordering
        # violation (seq N committed strictly after seq N+1) still fails. This is
        # the invariant a nextval/IDENTITY allocator violates.
        assert ts == sorted(ts), (
            f"commit_micros not non-decreasing along commit_seq_num: {list(zip(seqs, ts, strict=True))}"
        )


class TestCommitSeqNumAbortGapless:
    """RT3 — an aborted tx consumes no seq; the next commit leaves no gap."""

    def test_aborted_tx_does_not_advance_the_counter(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        before = [r[0] for r in _tx_seq_rows(catalog_uuid, main_branch_uuid)]
        next_seq = max(before) + 1

        # Open two txs; abort one; commit the other.
        tx_a = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.abort_tx(
            tx_a.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["b"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )

        b_seq = get_pg_driver().execute(
            f"SELECT commit_seq_num FROM {_qi(commit_tx_log_partition(catalog_uuid, main_branch_uuid))} "
            f"WHERE tx_uuid = %s::uuid",
            (tx_b.tx_uuid,),
        )
        # B takes the next seq — the aborted A consumed nothing.
        assert b_seq[0][0] == next_seq, (
            f"committed tx should take {next_seq} (no gap from abort); got {b_seq[0][0]}"
        )

        # One more commit continues contiguously.
        client.create_schema(
            "after_abort", catalog_uuid=catalog_uuid, author="t", comment="c"
        )
        after = [r[0] for r in _tx_seq_rows(catalog_uuid, main_branch_uuid)]
        assert sorted(after) == list(range(len(after))), (
            f"not gapless after abort: {after}"
        )


class TestCommitSeqNumBranchIsolation:
    """RT4 — per-branch counters are independent; the child is SEEDED from the
    fork commit (CHA-487), not restarted at 0."""

    def test_child_branch_counter_is_isolated_from_main(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # CHA-487: the child's counter is seeded from the source's fork commit,
        # so the fork tx lands at source_max + 1 (disjoint from parent).
        source_max = _max_commit_seq(catalog_uuid, main_branch_uuid)

        child = client.create_branch(
            "child",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="fork",
        )
        child_branch_uuid = child.branch_uuid

        # The fork tx is the child's FIRST commit; seeded, it lands at
        # source_max + 1 (not 0).
        child_rows = _tx_seq_rows(catalog_uuid, child_branch_uuid)
        child_seqs = [r[0] for r in child_rows]
        assert child_seqs == [source_max + 1], (
            f"child's only (fork) tx should be seq source_max+1={source_max + 1}; got {child_seqs}"
        )

        # The single counter row lives in the child's own commit_tx_log_seq_num
        # partition (branch-global — one row per branch, no key column).
        child_counter = get_pg_driver().execute(
            f"SELECT seq_num FROM {_qi(_commit_tx_log_seq_num_partition(catalog_uuid, child_branch_uuid))}",
        )
        assert child_counter[0][0] == source_max + 2, (
            f"child counter should be next-to-assign=source_max+2={source_max + 2} after fork; got {child_counter}"
        )

        # A commit on the child advances ONLY the child; main is untouched.
        main_before = [r[0] for r in _tx_seq_rows(catalog_uuid, main_branch_uuid)]
        _commit_upsert(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=table_uuid,
            name="c1",
        )
        child_after = sorted(
            r[0] for r in _tx_seq_rows(catalog_uuid, child_branch_uuid)
        )
        main_after = [r[0] for r in _tx_seq_rows(catalog_uuid, main_branch_uuid)]

        assert child_after == [source_max + 1, source_max + 2], (
            f"child should advance to [source_max+1, source_max+2]; got {child_after}"
        )
        assert main_after == main_before, (
            "main counter must not move when child commits"
        )


class TestTxLogSeqNumSeeding:
    """RT5 — exactly one branch-global commit_tx_log_seq_num counter row per branch.

    The commit counter is branch-global (CHA-428): one row per branch, no
    key column. Per-table write_seq counters are a separate table in CHA-431,
    so CreateTable must NOT add rows here.
    """

    def test_one_counter_row_per_branch_and_none_per_table(self):
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_part = _commit_tx_log_seq_num_partition(catalog_uuid, main_branch_uuid)

        # Exactly one counter row on main, advanced past genesis.
        main_rows = get_pg_driver().execute(f"SELECT seq_num FROM {_qi(main_part)}")
        assert len(main_rows) == 1, f"expected one counter row on main; got {main_rows}"
        assert main_rows[0][0] >= 1, (
            f"counter should have advanced past genesis (seq 0); got {main_rows[0][0]}"
        )

        # CreateTable must NOT add a counter row (per-table write_seq is CHA-431).
        client.create_table(
            "t2",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=_schema_uuid,
            author="t",
            comment="c",
        )
        assert len(get_pg_driver().execute(f"SELECT 1 FROM {_qi(main_part)}")) == 1, (
            "CreateTable must not add a commit_tx_log_seq_num row (per-table counter is CHA-431)"
        )

        # CHA-487: CreateBranch seeds the child's single counter row from the
        # source's fork commit. The fork tx consumed source_max + 1, so
        # next-to-assign is source_max + 2. source_max is read after the t2
        # CreateTable above (which advanced main's counter).
        source_max = _max_commit_seq(catalog_uuid, main_branch_uuid)
        child = client.create_branch(
            "child", catalog_uuid=catalog_uuid, author="t", comment="fork"
        )
        child_part = _commit_tx_log_seq_num_partition(catalog_uuid, child.branch_uuid)
        child_rows = get_pg_driver().execute(f"SELECT seq_num FROM {_qi(child_part)}")
        assert len(child_rows) == 1, (
            f"expected one counter row on child; got {child_rows}"
        )
        assert child_rows[0][0] == source_max + 2, (
            f"child counter should be next-to-assign=source_max+2={source_max + 2} after fork; got {child_rows[0][0]}"
        )


class TestForkSeqSeeding:
    """CHA-487 — the child's ``commit_seq_num`` counter is SEEDED from the fork
    commit, not restarted at 0.

    Seeding the child's counter to ``commit_seq_num(T) + 1`` (where ``T`` is the
    fork commit) makes parent (``<= T``) and child (``> T``) seqs disjoint and
    totally ordered, so the existing latest-wins-on-``commit_seq_num`` resolution
    lets the child shadow the parent with no lineage tiebreak — the substrate the
    cross-branch read merge (CHA-178) consumes.

    Mechanism note: the counter row holds *next-to-assign* (allocation does
    ``SET seq_num = seq_num + 1 RETURNING seq_num - 1``, i.e. returns the
    pre-increment value — ``crates/penca-storage-hot/src/tx.rs``). So the counter
    is seeded to ``source_max + 1`` and the fork tx (child's first commit) lands at
    ``source_max + 1``. Seeding to ``source_max`` would collide the fork tx with
    the parent's fork-point seq.
    """

    def test_forked_child_counter_seeded_from_fork_commit(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        # A few more commits on main so the source's MAX(commit_seq_num) is
        # unambiguously > 0 and the seeded child value is easy to read.
        for i in range(3):
            client.create_schema(
                f"pre_fork_{i}", catalog_uuid=catalog_uuid, author="t", comment="c"
            )

        source_max = _max_commit_seq(catalog_uuid, main_branch_uuid)
        assert source_max > 0, f"fixture must leave source_max > 0; got {source_max}"

        child = client.create_branch(
            "child",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="fork",
        )
        child_branch_uuid = child.branch_uuid

        # The fork tx is the child's FIRST commit. Seeded from the fork point, it
        # must land at source_max + 1 — NOT 0. This is the assertion that fails
        # pre-CHA-487 (today the child counter restarts at 0, so the fork tx is
        # seq 0): AssertionError expected [source_max+1], got [0].
        child_seqs = [r[0] for r in _tx_seq_rows(catalog_uuid, child_branch_uuid)]
        assert child_seqs == [source_max + 1], (
            f"seeded fork tx should be [source_max+1]=[{source_max + 1}]; got {child_seqs}"
        )

        # The child's counter row continues from the seed: next-to-assign is
        # source_max + 2 after the fork tx consumed source_max + 1.
        child_counter = get_pg_driver().execute(
            f"SELECT seq_num FROM {_qi(_commit_tx_log_seq_num_partition(catalog_uuid, child_branch_uuid))}",
        )
        assert child_counter[0][0] == source_max + 2, (
            f"child counter should be next-to-assign={source_max + 2} after fork; got {child_counter}"
        )

        # A local commit on the child continues contiguously from the seed.
        _commit_upsert(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=child_branch_uuid,
            table_uuid=table_uuid,
            name="c1",
        )
        child_after = sorted(
            r[0] for r in _tx_seq_rows(catalog_uuid, child_branch_uuid)
        )
        assert child_after == [source_max + 1, source_max + 2], (
            f"child should continue from seed to [{source_max + 1},{source_max + 2}]; got {child_after}"
        )

        # Disjointness / no-restart: every child seq is strictly greater than the
        # parent's fork-point seq, so parent (<= source_max) and child ranges
        # never overlap.
        assert min(child_after) > source_max, (
            f"child seqs must be disjoint from parent (all > {source_max}); got {child_after}"
        )

    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_grandchild_seeds_from_child_source_not_main(self):
        """The seed reads the SOURCE branch's log, not always ``main``'s.

        Every other seeding test forks off ``main``, so a regression that
        ignored ``source_branch_uuid`` and hard-coded ``main``'s partition would
        still pass them. Fork a grandchild from a child that has its own commits
        (so ``child_max > main_max``) and assert the grandchild seeds from the
        CHILD's max, exercising the ``source`` parameter distinctly from ``main``.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        child = client.create_branch(
            "child",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="fork-child",
        )
        child_branch_uuid = child.branch_uuid

        # Commit several times on the CHILD so its MAX(commit_seq_num) clears
        # both main's max and the child's own fork seed — making "seed from
        # child" vs "seed from main" unambiguously distinguishable.
        for i in range(3):
            _commit_upsert(
                client,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=child_branch_uuid,
                table_uuid=table_uuid,
                name=f"cc{i}",
            )

        child_max = _max_commit_seq(catalog_uuid, child_branch_uuid)
        main_max = _max_commit_seq(catalog_uuid, main_branch_uuid)
        assert child_max > main_max, (
            f"fixture must leave child_max ({child_max}) > main_max ({main_max}) "
            "so the source branch is distinguishable from main"
        )

        # Fork a grandchild FROM THE CHILD (not main).
        grandchild = client.create_branch(
            "grandchild",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=child_branch_uuid,
            author="t",
            comment="fork-grandchild",
        )

        # The grandchild's fork tx must seed from the CHILD's max
        # (child_max + 1), NOT main's (main_max + 1). A regression that read
        # main's partition regardless of source would produce main_max + 1 here.
        gc_seqs = [r[0] for r in _tx_seq_rows(catalog_uuid, grandchild.branch_uuid)]
        assert gc_seqs == [child_max + 1], (
            f"grandchild must seed from its source (child_max+1={child_max + 1}), "
            f"not main (main_max+1={main_max + 1}); got {gc_seqs}"
        )


def _persist_and_purge(client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid):
    """Flush committed rows to cold (Persist), advance the snapshot baseline
    (Snapshot), then Purge them out of hot so a read genuinely exercises the
    cold tier — Persist alone leaves rows queryable from hot, so a cold
    assertion could pass reading hot↔hot. CHA-444 (ADR 0027): Purge advances
    the read fence ``Pu`` only to ``W_snap``, so Snapshot must run first. Both
    watermark transitions are asserted; a no-op would silently make this a
    hot read."""
    ids = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": branch_uuid,
        "table_uuid": table_uuid,
    }
    persist_response = client.persist(**ids)
    assert persist_response.HasField("persisted_at_micros"), (
        "persist was a no-op; fixture did not move rows cold"
    )
    client.snapshot(**ids)
    purge_response = client.purge(**ids)
    assert purge_response.HasField("purged_at_micros"), (
        "purge was a no-op; rows still served from hot"
    )


def _persist_and_snapshot(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid
):
    """Flush committed rows to cold (Persist), then materialize a read-optimized
    SNAPSHOT baseline over them — WITHOUT purging hot. The rows then live in the
    snapshot baseline AND the cold persist-log AND hot at once. CHA-457: the
    baseline carries one table-level watermark (W_snap) and NO per-row seq, so
    the only tier that can wrongly surface a ``seq > N`` row under an AsOfSeq(N)
    read is the baseline — the persist-log + hot tiers filter per-row by seq."""
    ids = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": branch_uuid,
        "table_uuid": table_uuid,
    }
    persist_response = client.persist(**ids)
    assert persist_response.HasField("persisted_at_micros"), (
        "persist was a no-op; fixture did not move rows cold"
    )
    snapshot_response = client.snapshot(**ids)
    assert snapshot_response.HasField("snapshotted_at_micros"), (
        "snapshot was a no-op; no baseline was materialized"
    )


def _seq_by_name_via_audit(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid
):
    """Map each upsert row's ``name`` -> its ``commit_seq_num`` via audit_data,
    which reads BOTH tiers (CHA-430), so the map spans the cold+hot horizon."""
    upserts, _deletes = client.audit_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    names = upserts.column("name").to_pylist()
    seqs = upserts.column("commit_seq_num").to_pylist()
    assert len(names) == len(set(names)), f"duplicate names in audit output: {names}"

    return dict(zip(names, seqs, strict=True))


def _read_names_as_of_commit_seq_num(
    client, n, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid
):
    """Read on the seq axis via the ``read_data`` facade's ``as_of_seq``
    argument (CHA-429 I7) — the facade builds the ``commit_seq_num`` as_of
    arm internally. Returns the set of ``name``s visible at
    ``commit_seq_num <= n``."""
    table = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
        as_of_seq=n,
    )
    return set(table.column("name").to_pylist())


class TestReadDataAsOfCommitSeqNum:
    """CHA-429 RT: ``read_data`` with ``as_of = commit_seq_num = N`` returns
    exactly the snapshot ``{rows from txs with commit_seq_num <= N}`` across a
    cold(persisted+purged)+hot horizon.

    RED until I3/I4/I5 wire the seq read path. I1 shipped the
    ``commit_seq_num`` proto field, but the penca-api boundary normalizes
    only the ``commit_micros`` arm (seq arm -> latest), so the server ignores
    the bound and returns every row: rows with seq > N leak in and the
    row-set assertion fails. A behavioral red (wrong rows), not an
    import/field error.
    """

    def test_read_data_as_of_commit_seq_num_hot_and_cold(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        ids = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }

        # Commit cold rows, flush+purge them to cold, then commit hot rows.
        cold_names = ["c0", "c1", "c2"]
        hot_names = ["h0", "h1"]
        for name in cold_names:
            _commit_upsert(client, name=name, **ids)

        _persist_and_purge(client, **ids)
        for name in hot_names:
            _commit_upsert(client, name=name, **ids)

        seq_by_name = _seq_by_name_via_audit(client, **ids)
        # Sanity: cold rows precede hot rows on the seq axis.
        assert max(seq_by_name[c] for c in cold_names) < min(
            seq_by_name[h] for h in hot_names
        ), f"cold/hot seq split not contiguous: {seq_by_name}"

        all_names = cold_names + hot_names
        ordered = sorted(all_names, key=lambda nm: seq_by_name[nm])
        # N straddles the boundary: all 3 cold + the first hot = 4 visible.
        n = seq_by_name[ordered[3]]
        expected = {nm for nm in all_names if seq_by_name[nm] <= n}

        got = _read_names_as_of_commit_seq_num(client, n, **ids)
        assert got == expected, (
            f"as_of commit_seq_num={n} must return exactly {expected}; "
            f"got {got} (seq map: {seq_by_name})"
        )

        # Regression: the default (no-as_of) read still merges the whole
        # cold+hot horizon — the micros/default path is unaffected by I1.
        latest = client.read_data(**ids)
        assert set(latest.column("name").to_pylist()) == set(all_names), (
            "default read must return every committed row across both tiers"
        )

    def test_as_of_seq_over_snapshot_baseline_no_leak(self):
        """CHA-443 (folds CHA-457 part 1): AsOfSeq(N) over a SNAPSHOT baseline
        returns exactly ``{rows with commit_seq_num <= N}`` — the snapshot picker
        must bound on the seq axis.

        RED today: the baseline picker is ``committed_at``-only — for AsOfSeq,
        ``ReadSnapshot::AsOfSeq.plan_as_of_micros()`` is ``i64::MAX`` so
        ``compute_snapshot_picker_as_of`` selects the LATEST snapshot regardless
        of N, and the baseline scan applies no per-row seq filter. So all
        baseline rows leak: a read at the lowest seq returns every row instead
        of just the first. (The CHA-429 RT only exercises persist+purge, never
        a snapshot baseline, so this path was never covered.)"""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        ids = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }

        names = ["a", "b", "c"]
        for name in names:
            _commit_upsert(client, name=name, **ids)

        # a,b,c land in the cold SNAPSHOT baseline (W_snap = seq of c). No
        # purge: they also remain in the cold persist-log + hot, which filter
        # by seq correctly — so any seq>N row in the result is a baseline leak.
        _persist_and_snapshot(client, **ids)

        seq_by_name = _seq_by_name_via_audit(client, **ids)
        # Pin N at the LOWEST seq: two rows are strictly past the cutoff and
        # must not appear. The seq-filtered persist-log/hot tiers still serve
        # the first row, so the fixed result is exactly that one row.
        n = min(seq_by_name.values())
        expected = {nm for nm in names if seq_by_name[nm] <= n}

        got = _read_names_as_of_commit_seq_num(client, n, **ids)
        assert got == expected, (
            f"as_of commit_seq_num={n} over a snapshot baseline must return "
            f"exactly {expected} (no seq>N leak); got {got} (seq map: {seq_by_name})"
        )


class TestOpenTxSeqAnchor:
    """CHA-429 / CHA-444 RT (white-box): BEGIN captures ``began_at_seq_num`` =
    the per-branch ``commit_tx_log_seq_num`` (commit) counter frontier — it is the
    OpenTx read-isolation bound compared against commit ``commit_seq_num``, and
    ADR 0027 keeps it a commit-counter sample. ABORT captures
    ``aborted_at_seq_num`` from the **independent** per-branch ``abort_seq_num``
    counter (CHA-444 / ADR 0027) — a dedicated gapless counter, NOT a sample of
    the commit frontier, so the abort watermark ``Pa`` stays monotone. The
    OpenTx read snapshot pins on the seq axis via ``began_at_seq_num``
    (consumed by the merge in I4).

    Why white-box and not a behavioral RYOW red: seq-order == committed_at-
    order (one commit lock) and both anchors are captured in the SAME begin
    snapshot, so a seq-anchored open-tx read selects the IDENTICAL visible
    set as today's micros-anchored read — there is no deterministic
    behavioral divergence to assert (only non-deterministic microsecond
    ties). The load-bearing NEW state is the anchor *capture*, asserted here
    directly: the column read raises ``UndefinedColumn`` until I2 (the
    CHA-428 white-box red style). RYOW snapshot isolation itself is unchanged
    and stays covered by the CHA-165 suite.
    """

    def test_begin_and_abort_capture_seq_anchor(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        counter_part = _commit_tx_log_seq_num_partition(catalog_uuid, main_branch_uuid)

        def _frontier():
            # The counter holds next-to-allocate == the seq frontier (last
            # committed seq + 1). The anchor captured at begin/abort must
            # equal this (no intervening commit on this branch).
            return get_pg_driver().execute(f"SELECT seq_num FROM {_qi(counter_part)}")[
                0
            ][0]

        frontier_at_begin = _frontier()
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        began = get_pg_driver().execute(
            f"SELECT began_at_seq_num "
            f"FROM {_qi(begin_tx_log_partition(catalog_uuid, main_branch_uuid))} "
            f"WHERE tx_uuid = %s::uuid",
            (tx.tx_uuid,),
        )
        assert began[0][0] == frontier_at_begin, (
            f"began_at_seq_num must equal the counter frontier at begin "
            f"({frontier_at_begin}); got {began}"
        )

        # CHA-444 (ADR 0027): aborted_at_seq_num is allocated from the
        # dedicated, independent abort-order counter (abort_seq_num), NOT a
        # sample of the commit counter. The first abort on the branch takes
        # the abort-counter frontier (0) and bumps it by one.
        abort_counter_part = _abort_seq_num_partition(catalog_uuid, main_branch_uuid)

        def _abort_frontier():
            return get_pg_driver().execute(
                f"SELECT seq_num FROM {_qi(abort_counter_part)}"
            )[0][0]

        abort_frontier_before = _abort_frontier()
        client.abort_tx(
            tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
        )
        aborted = get_pg_driver().execute(
            f"SELECT aborted_at_seq_num "
            f"FROM {_qi(abort_tx_log_partition(catalog_uuid, main_branch_uuid))} "
            f"WHERE tx_uuid = %s::uuid",
            (tx.tx_uuid,),
        )
        assert aborted[0][0] == abort_frontier_before, (
            f"aborted_at_seq_num must equal the abort-counter frontier at abort "
            f"({abort_frontier_before}); got {aborted}"
        )
        assert _abort_frontier() == abort_frontier_before + 1, (
            "the dedicated abort counter must advance by one per abort "
            f"({abort_frontier_before} → {_abort_frontier()})"
        )


class TestWriteSequence:
    """RT1 [CHA-431] — ``write_seq_num`` is allocated from a per-(table, branch)
    lock-free Postgres SEQUENCE (``write_sequence``), created at table birth
    at ``START 0``, shared across the table's upsert + delete logs.

    RED against the pre-CHA-431 schema:

    * the hot ``upsert_log`` has no ``write_seq_num`` column → ``SELECT
      write_seq_num`` raises psycopg ``UndefinedColumn``.
    * the per-(table, branch) ``write_sequence`` does not exist → the
      ``pg_sequences`` lookup returns zero rows.

    GREEN once IMPL1 (naming) + IMPL3 (``CREATE SEQUENCE`` in
    ``create_data_tables``) + IMPL4 (the ``write_seq_num`` column + per-row
    ``nextval`` stamping) land.

    Run via ``just integration-test commit_seq_num``.
    """

    def test_first_write_stamps_write_seq_num_zero(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        # First mutation to a fresh table → first ``nextval`` on its
        # write_sequence (START 0) → write_seq_num == 0 on the row.
        _commit_upsert(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
            name="alice",
        )
        upsert_log = _upsert_log_table(table_uuid, main_branch_uuid)
        rows = get_pg_driver().execute(f"SELECT write_seq_num FROM {_qi(upsert_log)}")
        assert [r[0] for r in rows] == [0], (
            f"first write to a fresh table must stamp write_seq_num=0; got {rows}"
        )

    def test_sequence_per_table_create_table_adds_one_branch_replicates(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        def _seq_exists(seq_name: str) -> bool:
            return bool(
                get_pg_driver().execute(
                    "SELECT 1 FROM pg_sequences WHERE sequencename = %s",
                    (seq_name,),
                )
            )

        # The user table created by setup_schema has a write_sequence on main.
        assert _seq_exists(_write_sequence_name(table_uuid, main_branch_uuid)), (
            "user table must have a write_sequence on main"
        )

        # CreateTable adds exactly one write_sequence for the new table
        # (incl. — not asserted by name here — the system table/schema tables).
        t2_uuid = client.create_table(
            "t2",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="t",
            comment="c",
        )
        assert _seq_exists(_write_sequence_name(t2_uuid, main_branch_uuid)), (
            "CreateTable must create a write_sequence for the new table"
        )

        # CreateBranch replicates the per-table sequence onto the child — the
        # inherited table keeps its table_uuid (CHA-177: "same row_uuids, new
        # tx_uuid"), so the child's sequence is named for (table_uuid,
        # child_branch). Direct name check (not a global count) so the
        # assertion is attributable to this fork, not a concurrent test.
        child = client.create_branch(
            "child", catalog_uuid=catalog_uuid, author="t", comment="fork"
        )
        assert _seq_exists(_write_sequence_name(table_uuid, child.branch_uuid)), (
            "CreateBranch must replicate the per-table write_sequence onto "
            "the child branch"
        )

    def test_upsert_and_delete_share_one_write_sequence(self):
        # Load-bearing CHA-431 invariant: a table's upsert_log and delete_log
        # draw write_seq_num from ONE shared write_sequence, so a delete and
        # an upsert to the same row are structurally distinct and comparable.
        # Two independent per-log sequences (each STARTing at 0) would hand the
        # first row of EACH log the value 0 — a collision. So the proof is
        # simply: the two logs' values are DISTINCT. (Not a gapless 0..N run —
        # CACHE on the sequence can leave gaps across pooled PG connections.)
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        commit_ids = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }
        _commit_upsert(client, name="a", **commit_ids)
        _commit_delete(client, name="a", **commit_ids)

        prefix = _data_log_prefix(table_uuid, main_branch_uuid)

        def _seqs(suffix: str) -> list[int]:
            return [
                r[0]
                for r in get_pg_driver().execute(
                    f"SELECT write_seq_num FROM {_qi(prefix + suffix)}"
                )
            ]

        upsert_seqs = _seqs("_data_upsert_log")
        delete_seqs = _seqs("_data_delete_log")
        assert len(upsert_seqs) == 1 and len(delete_seqs) == 1, (
            f"expected one row in each log; got upsert={upsert_seqs} "
            f"delete={delete_seqs}"
        )
        assert upsert_seqs[0] != delete_seqs[0], (
            f"upsert_log and delete_log must draw DISTINCT write_seq_num from one "
            f"shared write_sequence; equal values mean two independent "
            f"per-log sequences both started at 0. got upsert={upsert_seqs} "
            f"delete={delete_seqs}"
        )
