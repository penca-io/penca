"""Integration tests for CHA-178 — cross-branch read inheritance.

A forked branch reads ``parent-state-as-of-the-fork ∪ its-own-writes`` —
nothing more, nothing less — resolving the parent's *cold* tier only, on top of
CHA-273 (persist-at-fork) + CHA-487 (seed child ``commit_seq_num``). The fork
point is a commit-order position (``commit_seq_num``, CHA-505).

Run via ``just integration-test branch_inheritance_read``.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation

from .integration_helpers import USER_SCHEMA, make_client


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
