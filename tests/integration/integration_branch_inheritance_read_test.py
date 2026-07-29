"""Integration tests for CHA-178 — cross-branch read inheritance.

A forked branch reads ``parent-state-as-of-the-fork ∪ its-own-writes`` —
nothing more, nothing less — resolving the parent's *cold* tier only, on top of
CHA-273 (persist-at-fork) + CHA-487 (seed child ``commit_seq_num``). The fork
point is a commit-order position (``commit_seq_num``, CHA-505).

Run via ``just integration-test branch_inheritance_read``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation

from .integration_helpers import (
    USER_SCHEMA,
    container_log,
    make_client,
    poll_log_for,
)


def _commit_rows(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows
) -> int:
    """Open + commit one tx upserting ``rows``. Returns the commit's
    ``commit_seq_num`` — a commit-order fork position for ``create_branch``."""
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
    resp = client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    )

    return resp.commit_seq_num


def _read(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, as_of_seq=None
):
    """Read a (branch, table) at the optional seq ``as_of``; return the pyarrow Table."""
    return client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
        as_of_seq=as_of_seq,
    )


def _names(table) -> set[str]:
    return set(table.column("name").to_pylist())


def _value_for(table, key: str):
    names = table.column("name").to_pylist()
    values = table.column("value").to_pylist()

    return next((v for n, v in zip(names, values, strict=True) if n == key), None)


def test_child_reads_inherited_then_own():
    """Child reads inherited parent rows, then inherited ∪ own; parent's
    post-fork commits stay invisible."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_read", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    # N=3 committed rows on main.
    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a1", "a2", "a3"], "value": [1, 2, 3]},
    )

    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # 1. Fork inherits the parent's 3 rows.
    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert got.num_rows == 3, f"child should inherit 3 parent rows, saw {got.num_rows}"
    assert _names(got) == {"a1", "a2", "a3"}

    # 2. Own writes compose with the inherited rows.
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["c1", "c2"], "value": [10, 20]},
    )
    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert _names(got) == {"a1", "a2", "a3", "c1", "c2"}

    # 3. Parent's post-fork commits are point-in-time invisible to the child.
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a4", "a5"], "value": [4, 5]},
    )
    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert _names(got) == {"a1", "a2", "a3", "c1", "c2"}, (
        "child must not see main's post-fork rows"
    )


def test_child_filtered_read_of_inherited_rows():
    """CHA-368 x CHA-178: a FILTERED read on a just-forked branch (before the
    child writes) resolves the parent cold source with no hot tier, so the base
    fold applies the user predicate as a DataFusion residual to the inherited
    rows. Non-matching parent rows must drop; matching ones survive. Guards the
    base-cold residual path, which no unfiltered inheritance test exercises."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_read_filter", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a1", "a2", "a3"], "value": [1, 2, 3]},
    )
    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # Filtered inherited read, child has no own tier yet: value > 1 keeps a2/a3
    # and drops a1 — the residual must reach the folded-in base cold rows.
    got = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child.branch_uuid,
        filter="value > 1",
    )
    assert _names(got) == {"a2", "a3"}, (
        f"filtered inherit must drop a1 (value=1); saw {_names(got)}"
    )
    assert got.num_rows == 2


def test_child_filtered_read_mixed_hot_and_inherited():
    """CHA-368 x CHA-178, mixed path: with child hot writes on top of the
    inherited base cold source, the base is folded in UNFILTERED and the residual
    is applied by ``assemble_parts`` to the whole combined batch after the fold.
    A non-matching inherited (base) row must still be dropped, while matching
    inherited + child rows survive. Complements the all-cold filtered test
    (``test_child_filtered_read_of_inherited_rows``) so both fold paths are
    covered under a filter."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_read_filter_mixed", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a1", "a2", "a3"], "value": [1, 2, 3]},
    )
    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # Child's OWN hot writes (both match the filter) — forces the mixed fold path.
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["c1", "c2"], "value": [10, 20]},
    )

    # value > 1: drop inherited a1 (base cold), keep a2/a3 (base) + c1/c2 (hot).
    got = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child.branch_uuid,
        filter="value > 1",
    )
    assert _names(got) == {"a2", "a3", "c1", "c2"}, (
        f"mixed filtered read must drop inherited a1 (value=1); saw {_names(got)}"
    )
    assert got.num_rows == 4


def test_per_source_seq_ceiling_time_travel():
    """Parent cold source is capped at ``min(fork_seed, as_of)``: a child
    time-travel read never sees the parent's post-fork commits, and the child
    shadows the parent above the fork."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_seq", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    # Fork commit: key "k" = 100 on main.
    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["k"], "value": [100]},
    )

    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # Post-fork commits for the SAME key on both branches (independent seqs > F).
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["k"], "value": [200]},
    )
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["k"], "value": [300]},
    )
    client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )

    # Time-travel at the fork seq sees the inherited fork value, never main's post-fork.
    at_fork = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        as_of_seq=fork_seq,
    )
    assert _value_for(at_fork, "k") == 100, (
        "child@fork must see the inherited fork value (100)"
    )

    # Current-time read sees the child's own value, never main's post-fork value.
    head = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert _value_for(head, "k") == 300, (
        "child head must see the child's own value (300)"
    )


def test_audit_honors_inherited_history():
    """``audit_data`` on a forked branch surfaces the inherited parent history,
    not just branch-local rows."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_audit", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["p1", "p2"], "value": [1, 2]},
    )

    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["c1"], "value": [10]},
    )

    upserts, _deletes = client.audit_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        after_seq=0,
        before_seq=fork_seq + 100,
    )
    audited = set(upserts.column("name").to_pylist())
    assert {"p1", "p2"} <= audited, (
        f"child audit must honor inherited parent history, saw {audited}"
    )
    assert "c1" in audited


def test_post_snapshot_current_time_skips_base_source():
    """Once the child snapshots (covering the fork), a current-time read is
    correct (inherited ∪ own) — the parent data is baked into the child's own
    snapshot, so the planner needs no base cold source."""
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_snap", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a1", "a2", "a3"], "value": [1, 2, 3]},
    )
    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )
    _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["c1", "c2"], "value": [10, 20]},
    )

    # Snapshot the child: the snapshot writer folds in the parent's cold data,
    # so the child's own baseline now covers the fork.
    client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )

    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert got.num_rows == 5, (
        f"post-snapshot child must read inherited ∪ own (5), saw {got.num_rows}"
    )
    assert _names(got) == {"a1", "a2", "a3", "c1", "c2"}


_PK_SCHEMA = pa.schema([pa.field("name", pa.utf8())])


def _commit_delete(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, keys
) -> None:
    """Open + commit one tx deleting ``keys`` (PK values) from ``table_uuid``."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            deletes=pa.table({"name": keys}, schema=_PK_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def test_child_delete_of_inherited_row():
    """A child deleting an inherited (parent) row excludes it from the child
    read without affecting the parent — the child's delete-log tombstone folds
    into the exclusion set against the parent base source (delete inheritance).
    """
    client = make_client()
    catalog_uuid, main = client.create_catalog("bi_del", "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )

    fork_seq = _commit_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
        rows={"name": ["a", "b"], "value": [1, 2]},
    )
    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # Sanity: the child inherits both parent rows.
    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert _names(got) == {"a", "b"}

    _commit_delete(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
        keys=["a"],
    )

    # The child read excludes the deleted inherited row; the parent is unaffected.
    got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=child.branch_uuid,
        table_uuid=table_uuid,
    )
    assert _names(got) == {"b"}, (
        f"child delete of an inherited row must exclude it, saw {_names(got)}"
    )
    main_got = _read(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
    )
    assert _names(main_got) == {"a", "b"}, "parent unaffected by the child's delete"


# CHA-539 — copy-at-fork changes WHERE a fork's inherited cold data comes from:
# the child's own materialized reference rows instead of a read-time reach into
# the parent. `base_cold_source` is the permanent gate marker
# (`penca-api/src/query/meta_plan.rs`); a string field renders quoted, matching
# the `tier_shape="..."` scrapes elsewhere.
_BASE_SOURCE_NONE = 'base_cold_source="none"'
_BASE_SOURCE_PRESENT = 'base_cold_source="present"'


def _base_cold_source_values(since: int, table_uuid: str) -> list[str]:
    """The gate marker's value for the USER table's plans in the log window.

    Scoping to `table=<uuid>` is load-bearing, not tidiness: one `read_data` RPC
    plans the `__penca_system__` metadata tables as well as the user table, and
    those system plans legitimately report ``none`` (no cold metadata in range).
    A bare `poll_log_for(_BASE_SOURCE_NONE)` therefore passes on a read whose
    user-table plan said ``present``. The marker is emitted inside the `plan`
    span, whose fields carry the table uuid on the same line.
    """
    return [
        _BASE_SOURCE_NONE if _BASE_SOURCE_NONE in line else _BASE_SOURCE_PRESENT
        for line in container_log("query")[since:].splitlines()
        if f"table={table_uuid}" in line and "base_cold_source=" in line
    ]


def _seed_forked_history(client, catalog_label: str):
    """Seed a parent whose cold tier straddles a snapshot, then fork off its head.

    Commit order on ``main`` for key ``k``, one commit per value::

        seq1: k=1
        seq2: k=2   -> persist + snapshot  (inherited baseline watermark W)
        seq3: k=3   -> persist             (cold persist above W)
        seq4: k=4   -> persist             (fork point)
        seq5: k=99  -> post-fork, must never be visible on the child

    The three read positions the CHA-539 gate has to distinguish all exist here:
    ``seq1`` is below W (answerable only from the parent's older cold state),
    ``seq3`` sits strictly between W and the fork, and ``seq4``/current-time is
    at the fork.
    """
    # Unique per run: these two suites are the ones iterated on while the gate
    # moves, and a fixed catalog name makes a second run against a live stack
    # fail with ALREADY_EXISTS rather than re-testing anything.
    catalog_uuid, main = client.create_catalog(
        f"{catalog_label}_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="t", comment="c"
    )
    table_uuid = client.create_table(
        "t",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="t",
        comment="c",
    )
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }

    def commit(value):
        return _commit_rows(
            client, branch_uuid=main, rows={"name": ["k"], "value": [value]}, **scope
        )

    seqs = {1: commit(1), 2: commit(2)}
    client.persist(branch_uuid=main, **scope)
    client.snapshot(branch_uuid=main, **scope)
    seqs[3] = commit(3)
    client.persist(branch_uuid=main, **scope)
    seqs[4] = commit(4)
    client.persist(branch_uuid=main, **scope)

    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=seqs[4], catalog_uuid=catalog_uuid
    )
    seqs[5] = commit(99)

    return scope, main, child.branch_uuid, seqs


@pytest.mark.serial
def test_forked_current_time_read_has_no_base_cold_source():
    """CHA-539: a fork's current-time read is answered entirely from the child's
    OWN cold tier, so the planner enumerates no base cold source.

    Fail-first: today the gate is ``child_snapshot_seq < fork_commit_seq_num``,
    open on every fork until the child snapshots, so the marker reads
    ``present``.
    """
    client = make_client()
    scope, _main, child, _seqs = _seed_forked_history(client, "bi_nobase")

    since = len(container_log("query"))
    got = _read(client, branch_uuid=child, **scope)

    assert _value_for(got, "k") == 4, (
        f"child head must read the parent state as-of the fork (4), saw {got.to_pydict()}"
    )
    # Flush barrier: the container's json-log flush lags the RPC return, so wait
    # for the marker to appear at all before asserting on WHICH value it carries.
    assert poll_log_for("query", since, "base_cold_source="), (
        "no base_cold_source marker reached the query log — the scrape window is "
        "wrong, not the gate"
    )
    assert _base_cold_source_values(since, scope["table_uuid"]) == [
        _BASE_SOURCE_NONE
    ], (
        "a forked current-time read must consult no base cold source, saw "
        f"{_base_cold_source_values(since, scope['table_uuid'])}"
    )


@pytest.mark.serial
def test_forked_below_fork_as_of_read_still_reaches_the_parent():
    """CHA-539 must NOT regress as-of-before-fork, which still reaches back into
    the parent's metadata. Two positions, two different arms of the child's plan:

    1. below the inherited baseline watermark — the child can pick no snapshot of
       its own and its inherited persist rows all sit above the pin, so the answer
       comes entirely from the parent. Breaks loudly if the base arm is dropped.
    2. strictly between that watermark and the fork — the child picks its
       inherited baseline AND the base arm fires. Breaks quietly if the two
       disagree.

    Green before CHA-539 and green after; the pre-change run is the baseline.
    """
    client = make_client()
    scope, _main, child, seqs = _seed_forked_history(client, "bi_belowfork")

    since = len(container_log("query"))
    below_baseline = _read(client, branch_uuid=child, as_of_seq=seqs[1], **scope)
    assert _value_for(below_baseline, "k") == 1, (
        "child as-of below the inherited baseline must reach the parent's older "
        f"cold state (1), saw {below_baseline.to_pydict()}"
    )

    between = _read(client, branch_uuid=child, as_of_seq=seqs[3], **scope)
    assert _value_for(between, "k") == 3, (
        "child as-of between the inherited baseline and the fork must resolve to "
        f"the parent's state at that pin (3), saw {between.to_pydict()}"
    )

    # Both reads are below the fork, so BOTH must keep enumerating the parent.
    assert poll_log_for("query", since, "base_cold_source="), (
        "no base_cold_source marker reached the query log — the scrape window is "
        "wrong, not the gate"
    )
    values = _base_cold_source_values(since, scope["table_uuid"])
    assert values == [_BASE_SOURCE_PRESENT, _BASE_SOURCE_PRESENT], (
        f"both below-fork reads must enumerate the parent's cold tier, saw {values}"
    )

    # The parent's post-fork commit stays invisible at every position.
    for label, as_of_seq in ("below", seqs[1]), ("between", seqs[3]), ("head", None):
        got = _read(client, branch_uuid=child, as_of_seq=as_of_seq, **scope)
        assert _value_for(got, "k") != 99, (
            f"child {label} read must never see the parent's post-fork commit"
        )
