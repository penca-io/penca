"""Integration tests for CHA-273 — synchronous persist-at-fork.

CreateBranch must synchronously persist (flush) the source branch's hot tier
→ cold before returning, so everything committed on the source at/before the
fork point is durable in cold storage. These tests assert on the SOURCE
(``main``) branch's cold tier only — the child cross-branch read path (CHA-178)
is not built yet, so a child read cannot be exercised here.

Run via ``just integration-test branch_persist_at_fork``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import InvalidRequestError
from penca_client.naming import (
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
    setup_partitioned_table,
    setup_schema,
    write_cycle,
)


def _write_committed_rows(
    client,
    *,
    catalog_uuid,
    schema_uuid,
    branch_uuid,
    table_uuid,
    rows,
) -> int:
    """Open + commit one tx upserting ``rows`` into ``table_uuid``.

    Returns the commit's ``commit_seq_num`` — a real fork point suitable as
    ``commit_seq_num``.
    """
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
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )

    return resp.commit_seq_num


def _committed_seg_count(catalog_uuid, branch_uuid, table_uuid) -> int:
    """Count cold persist segments for ``(branch, table)`` via direct PG read.

    Mirrors the white-box assertion in ``integration_branch_test.py``: segment
    rows exist only after a persist has run for that ``(branch, table)``.
    """
    tfsm_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(tfsm_parent)),
        (branch_uuid, table_uuid),
    )

    return rows[0][0]


def test_create_branch_flushes_source_tables_to_cold():
    """CreateBranch persists every source user table (catalog-wide) to cold.

    Fails today: ``create_branch`` only materializes metadata, so after the fork
    the source tables have zero cold persist segments and the ``> 0`` assertions
    fire.
    """
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"pf_cat_{uuid4().hex[:8]}", "owner"
    )
    s1_uuid = client.create_schema(
        "s1", catalog_uuid=catalog_uuid, author="test", comment="setup"
    )
    s2_uuid = client.create_schema(
        "s2", catalog_uuid=catalog_uuid, author="test", comment="setup"
    )
    t1_uuid = client.create_table(
        "t1",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=s1_uuid,
        author="test",
        comment="setup",
    )
    t2_uuid = client.create_table(
        "t2",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=s2_uuid,
        author="test",
        comment="setup",
    )

    # Commit rows into both schemas on main (the source branch).
    _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=s1_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=t1_uuid,
        rows={"name": ["alice", "amy"], "value": [1, 2]},
    )
    fork_seq = _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=s2_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=t2_uuid,
        rows={"name": ["bob"], "value": [3]},
    )

    # Control: nothing on main is persisted to cold yet.
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, t1_uuid) == 0
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, t2_uuid) == 0

    client.create_branch(
        "child",
        "test",
        "create_branch",
        commit_seq_num=fork_seq,
        catalog_uuid=catalog_uuid,
    )

    # CHA-273: CreateBranch synchronously flushed main's user tables to cold,
    # catalog-wide (both schemas).
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, t1_uuid) > 0, (
        "source table t1 was not flushed to cold at CreateBranch"
    )
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, t2_uuid) > 0, (
        "source table t2 was not flushed to cold at CreateBranch"
    )


def test_persist_after_create_branch_is_noop():
    """An explicit persist on the source after CreateBranch finds nothing new.

    Behavioral (public-API) proof of the flush: CreateBranch already persisted
    the source up to the fork point, so ``persist`` trips its strict-advance
    watermark gate and returns ``persisted_at_micros = None``. Fails today: with
    no flush at CreateBranch, the explicit persist flushes the committed rows
    and returns a non-None watermark.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    fork_seq = _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["alice", "amy", "ann"], "value": [1, 2, 3]},
    )

    client.create_branch(
        "child",
        "test",
        "create_branch",
        commit_seq_num=fork_seq,
        catalog_uuid=catalog_uuid,
    )

    response = client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )

    assert not response.HasField("persisted_at_micros"), (
        "expected persist no-op after CreateBranch flushed the source, "
        f"got persisted_at_micros={response.persisted_at_micros!r}"
    )


def test_flush_excludes_post_fork_source_commits():
    """Rows committed on the source AFTER the fork tx are not flushed at fork.

    Proves the fork *bound* (not merely that a flush happened): the flush is
    capped at the fork position's ``commit_micros``, so a source commit landing
    after the fork stays unpersisted — an explicit persist on the source does
    real work (non-no-op). Had the flush over-run to ``now``, that persist would
    no-op instead. (Distinct microseconds: R1 and R2 are separate commit round
    trips, so the non-strict-monotonic micros tie-case does not apply here.)
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    # Fork point: commit R1 and capture its commit_seq_num.
    fork_seq = _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["alice", "amy"], "value": [1, 2]},
    )
    # R2 committed on the source AFTER the fork point.
    _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["bob", "bea"], "value": [3, 4]},
    )

    client.create_branch(
        "child",
        "test",
        "create_branch",
        commit_seq_num=fork_seq,
        catalog_uuid=catalog_uuid,
    )

    # The flush persisted only R1 (<= fork). R2 (> fork) is still unpersisted, so
    # an explicit persist on the source now does real work.
    response = client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )
    assert response.HasField("persisted_at_micros"), (
        "expected the post-fork rows to be UNflushed (persist does real work); "
        "a no-op would mean CreateBranch over-flushed past the fork point"
    )


def test_unresolved_fork_position_is_rejected():
    """A well-formed but nonexistent fork position is a hard INVALID_ARGUMENT.

    CHA-505 retired the old unvalidated-base_tx → head fallback (CHA-494): you
    can never fork from an uncommitted position, so the resolver rejects it and
    no branch is created / no flush runs.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["alice"], "value": [1]},
    )

    with pytest.raises(InvalidRequestError):
        client.create_branch(
            "child",
            "test",
            "create_branch",
            commit_seq_num=10_000_000,
            catalog_uuid=catalog_uuid,
        )


def test_persist_branch_returns_watermark():
    """The lifecycle `PersistBranch` RPC persists the branch's modified tables
    catalog-wide and, with no target, returns the branch-head `Watermark`.

    The `Watermark` is a commit-order position {commit_seq_num, commit_micros} —
    no tx_uuid (dropped at cold-persist, CHA-430). Targeting a specific fork
    position is exercised end-to-end by the create_branch tests above (the write
    pod resolves the fork_point oneof to this position before calling this RPC).
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["alice", "amy"], "value": [1, 2]},
    )

    # No target → bounds at the branch head (the commit above).
    response = client.persist_branch(
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    # Head follows the DDL + data commits, so its seq/micros are strictly
    # positive — guards against a zeroed/default watermark.
    assert response.HasField("watermark"), (
        "an unset watermark is the partial-flush signal, not a zero value"
    )
    assert response.watermark.commit_seq_num > 0
    assert response.watermark.commit_micros > 0
    # The source's modified user tables are now durable in cold.
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, table_uuid) > 0


def test_persist_and_snapshot_branch_returns_watermark():
    """The lifecycle `PersistAndSnapshotBranch` RPC persists AND snapshots the
    branch's modified tables catalog-wide and returns the head `Watermark`.

    Sibling coverage for `persist_branch` — exercises the combined branch op the
    scheduler drives per tick (continue-on-error per table, server-side). Here
    every table succeeds, so it returns the head position and the source's
    modified tables are flushed to cold.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    _write_committed_rows(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        rows={"name": ["alice", "amy"], "value": [1, 2]},
    )

    response = client.persist_and_snapshot_branch(
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    assert response.HasField("watermark"), (
        "an unset watermark is the partial-flush signal, not a zero value"
    )
    assert response.watermark.commit_seq_num > 0
    assert response.watermark.commit_micros > 0
    assert _committed_seg_count(catalog_uuid, main_branch_uuid, table_uuid) > 0


# CHA-539 — CreateBranch materializes the child's OWN cold reference rows,
# pointing at the parent's object_uris, so the GC refcount gate can see the
# fork's claim on those files. Metadata only: no data copy, no new object.
_COLD_REFERENCE_TABLES = (
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    TABLE_SNAPSHOT_INDEX_METADATA,
    TABLE_SNAPSHOT_SEGMENT_INDEX_METADATA,
)

# The two segment tables whose rows name a cold file. Measured as a set of
# `object_uri`s, the same metadata-side proxy for stored objects
# `integration_branch_fork_storage_growth_test.py` uses — a fork that copies
# only metadata leaves this set unchanged.
_SEGMENT_URI_TABLES = (
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)


def _branch_row_count(catalog_uuid, branch_uuid, table_name) -> int:
    """COMMITTED rows on ``table_name`` for one branch.

    ``commit_micros IS NOT NULL`` is the assertion's teeth, not decoration: it is
    what plan visibility gates on, and every table here has it nullable because
    the storage-meta writers all insert NULL then stamp it in a second phase.
    Without the predicate a copy that inserts the child's reference rows and
    never commits them — the most likely way a two-phase copy gets built wrong —
    counts as present while no reader can see it.

    ``table_snapshot_segment_index_metadata`` has no ``table_uuid`` column (the
    index identity lives on its parent header), so this counts per branch only.
    """
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} "
            "WHERE branch_uuid = %s AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(f"{catalog_uuid}_{table_name}")),
        (branch_uuid,),
    )

    return rows[0][0]


def _segment_uris(catalog_uuid, branch_uuid=None, table_uuid=None) -> set[str]:
    """Committed segment ``object_uri``s, optionally narrowed to a branch/table."""
    uris: set[str] = set()
    for table_name in _SEGMENT_URI_TABLES:
        clause, params = "commit_micros IS NOT NULL", []
        for column, value in ("branch_uuid", branch_uuid), ("table_uuid", table_uuid):
            if value is not None:
                clause += f" AND {column} = %s"
                params.append(value)

        rows = get_pg_driver().execute(
            SQL("SELECT DISTINCT object_uri FROM {tbl} WHERE " + clause).format(
                tbl=Identifier(f"{catalog_uuid}_{table_name}")
            ),
            tuple(params),
        )
        uris.update(row[0] for row in rows)

    return uris


def _max_copied_persist_seq(catalog_uuid, branch_uuid) -> int | None:
    """``MAX(max_commit_seq_num)`` over a branch's committed persist segments."""
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(max_commit_seq_num) FROM {tbl} "
            "WHERE branch_uuid = %s AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}")),
        (branch_uuid,),
    )

    return rows[0][0]


def _seed_straddling_parent(client):
    """Seed ``main`` so its persist segments span a wide seq range, and return a
    fork position that lands INSIDE one of them.

    This is the ordinary case, not an edge case: the parent persists on the
    scheduler's cadence, so a fork at any non-head position falls mid-segment.
    Two commits are flushed by ONE persist, so the resulting segment covers
    ``[seq(first), seq(second)]`` and forking at ``seq(first)`` straddles it.
    """
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }

    def commit(name, value):
        return _write_committed_rows(
            client,
            branch_uuid=main_branch_uuid,
            rows={"name": [name], "value": [value]},
            **scope,
        )

    # Persisted together -> one segment spanning both seqs.
    fork_seq = commit("inside_lo", 1)
    above_fork_seq = commit("inside_hi", 2)
    client.persist(branch_uuid=main_branch_uuid, **scope)
    client.snapshot(branch_uuid=main_branch_uuid, **scope)

    return scope, main_branch_uuid, fork_seq, above_fork_seq


def test_fork_materializes_child_cold_reference_rows():
    """CHA-539: CreateBranch writes the child's own cold reference rows, naming
    the parent's ``object_uri``s.

    Fails today: ``create_branch`` materializes branch_store, partitions, the
    seq seed and schema/table metadata — no cold metadata at all.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }
    _write_committed_rows(
        client,
        branch_uuid=main_branch_uuid,
        rows={"name": ["a1", "a2"], "value": [1, 2]},
        **scope,
    )
    client.persist(branch_uuid=main_branch_uuid, **scope)
    client.snapshot(branch_uuid=main_branch_uuid, **scope)

    parent_uris = _segment_uris(catalog_uuid, main_branch_uuid, table_uuid)
    assert parent_uris, "parent must hold cold segments before the fork is meaningful"

    child = client.create_branch("child", "t", "fork", catalog_uuid=catalog_uuid)

    missing = [
        table_name
        for table_name in _COLD_REFERENCE_TABLES
        if _branch_row_count(catalog_uuid, child.branch_uuid, table_name) == 0
    ]
    assert not missing, (
        f"the fork must materialize the child's cold reference rows; {missing} "
        "have no row on the child"
    )

    child_uris = _segment_uris(catalog_uuid, child.branch_uuid, table_uuid)
    assert child_uris <= parent_uris, (
        "every child reference must name a file the PARENT already wrote, not a "
        f"fresh one; {child_uris - parent_uris} are new"
    )


def test_fork_writes_no_new_cold_objects():
    """The fork's COPY step is metadata-only: reference rows, never a cold file.

    Scoped to the user table on purpose. CreateBranch's flush is catalog-wide, so
    it also persists the `__penca_system__` schemas/tables — whose DDL rows are
    still hot here — and those legitimately produce new cold objects. Comparing
    catalog-wide would fail on that pre-existing flush and say nothing about the
    copy.

    Green before CHA-539 (nothing is copied at all) and green after (only
    metadata is copied) — the guard that the copy stays metadata-only.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }
    _write_committed_rows(
        client,
        branch_uuid=main_branch_uuid,
        rows={"name": ["a1", "a2"], "value": [1, 2]},
        **scope,
    )
    client.persist(branch_uuid=main_branch_uuid, **scope)
    client.snapshot(branch_uuid=main_branch_uuid, **scope)

    before = _segment_uris(catalog_uuid, table_uuid=table_uuid)
    assert before, "no cold segments to compare against"

    client.create_branch("child", "t", "fork", catalog_uuid=catalog_uuid)

    after = _segment_uris(catalog_uuid, table_uuid=table_uuid)
    assert after == before, (
        "CreateBranch must reference the parent's existing cold files, not write "
        f"new ones; {after - before} appeared"
    )


def test_copied_persist_rows_are_seq_clamped():
    """CHA-539: a copied persist row must not claim rows past the fork.

    The fork position lands inside an existing parent segment, so a verbatim
    copy would carry that segment's full ``max_commit_seq_num`` and expose the
    parent's post-fork rows to the child. The copy is clamped to the fork seq.

    Fails today: the child has no persist rows to assert over.
    """
    client = make_client()
    scope, main_branch_uuid, fork_seq, above_fork_seq = _seed_straddling_parent(client)

    parent_max = _max_copied_persist_seq(scope["catalog_uuid"], main_branch_uuid)
    assert parent_max is not None and parent_max >= above_fork_seq, (
        "fixture must produce a parent segment reaching past the fork seq; "
        f"parent max is {parent_max}, fork at {fork_seq}"
    )

    child = client.create_branch(
        "child",
        "t",
        "fork",
        commit_seq_num=fork_seq,
        catalog_uuid=scope["catalog_uuid"],
    )

    child_max = _max_copied_persist_seq(scope["catalog_uuid"], child.branch_uuid)
    assert child_max is not None, (
        "the fork must copy the parent's persist segment rows onto the child"
    )
    assert child_max == fork_seq, (
        "the straddling segment's copy must be CLAMPED to the fork seq (not "
        f"dropped, not verbatim); child max is {child_max}, fork at {fork_seq}, "
        f"parent max {parent_max}"
    )


def test_fork_at_historical_seq_hides_parents_post_fork_rows():
    """A fork inside an already-written parent segment sees the parent's rows at
    or below the fork, and none above it.

    Green today via the base-cold arm's plan-wide ``PersistPlan.commit_seq
    .max_seq`` ceiling; after CHA-539 the same guarantee has to come from the
    per-segment ``max_commit_seq_num`` ceiling applied to the clamped copies.
    This is the test that catches an unclamped or unenforced copy.
    """
    client = make_client()
    scope, _main_branch_uuid, fork_seq, _above = _seed_straddling_parent(client)

    child = client.create_branch(
        "child",
        "t",
        "fork",
        commit_seq_num=fork_seq,
        catalog_uuid=scope["catalog_uuid"],
    )

    got = client.read_data(branch_uuid=child.branch_uuid, **scope)
    names = set(got.column("name").to_pylist())
    assert names == {"inside_lo"}, (
        "the child must inherit only the parent rows at or below the fork seq; "
        f"saw {names}"
    )


def _column_names(table_name):
    rows = get_pg_driver().execute(
        SQL(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = %s ORDER BY ordinal_position"
        ),
        (table_name,),
    )

    return [r[0] for r in rows]


def _copy_rows_to_branch(table_name, *, where, params, overrides):
    """``INSERT INTO t (cols) SELECT cols-with-overrides FROM t WHERE ...``

    Copying by live column list rather than spelling the storage columns out
    keeps this from drifting whenever a segment schema gains a column — the same
    idiom ``integration_branch_fork_gc_refcount_test.py`` uses to synthesize a
    cross-branch reference independent of the writer that will produce it.
    """
    cols = _column_names(table_name)
    select_items, values = [], []
    for col in cols:
        if col in overrides:
            select_items.append(SQL("%s"))
            values.append(overrides[col])
        else:
            select_items.append(Identifier(col))

    get_pg_driver().execute_no_result(
        SQL(
            "INSERT INTO {tbl} ({cols}) SELECT {vals} FROM {tbl} WHERE " + where
        ).format(
            tbl=Identifier(table_name),
            cols=SQL(", ").join(Identifier(c) for c in cols),
            vals=SQL(", ").join(select_items),
        ),
        (*values, *params),
    )


def test_fork_audit_spanning_the_fork_hides_parents_post_fork_rows():
    """``audit_data`` must honor the per-segment ceiling, not just ``read_data``.

    `read_data` enforces it in `PersistPartitionStream`; `audit_data` reads the
    same segment list through `ColdStorageClient`, which ignored
    `max_commit_seq_num` entirely. Today that is masked by the separate
    `base_cold_*` arm, which caps the parent at the fork seq — so once CHA-539's
    copy puts clamped rows in the child's OWN `cold_upsert_segments`, an audit
    window reaching past the fork would emit the parent's post-fork rows.

    This fixture is the GENESIS-INHERIT shape: `_seed_straddling_parent`
    snapshots the parent after both commits, so that snapshot commits above the
    fork, `copy_inherited_snapshot`'s pick finds nothing, and
    `inherited_baseline_watermark` returns `None`. The base arm is therefore
    SKIPPED, not capped, and `inside_lo` comes solely from the child's own copied
    arm — so this guards the skip branch and a wholly un-windowed base arm. The
    `Some(watermark)` branch is covered by
    `test_fork_audit_dedupes_against_a_real_adopted_baseline` below.

    Asserted on the COUNT, not set membership: `audit_data` concatenates the two
    arms without dedup, so an overlap shows up as exact duplicates a set()
    assertion cannot see.

    Uses the real fork copy rather than synthesizing the child's rows. An earlier
    draft copied header + segment rows in by hand, which was necessary before
    `create_branch` did it — and became a double-count once it did, since the
    child then held two row sets naming one file.
    """
    client = make_client()
    scope, main_branch_uuid, fork_seq, above_fork_seq = _seed_straddling_parent(client)
    catalog_uuid = scope["catalog_uuid"]

    child = client.create_branch(
        "child", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    )

    # The parent's straddling segment and its header, as CHA-539's copy will
    # write them: same object_uri, clamped to the fork seq, sealed.
    child_max = _max_copied_persist_seq(catalog_uuid, child.branch_uuid)
    assert child_max == fork_seq, (
        f"setup failed: the child's clamped rows should cap at {fork_seq}, saw {child_max}"
    )

    # Window reaches PAST the fork, so only the ceiling can exclude the
    # parent's post-fork row.
    upserts, _deletes = client.audit_data(
        branch_uuid=child.branch_uuid,
        after_seq=0,
        before_seq=above_fork_seq + 100,
        **scope,
    )
    all_names = upserts.column("name").to_pylist()
    names = set(all_names)
    assert "inside_lo" in names, (
        f"the child's audit must surface the inherited pre-fork history, saw {names}"
    )
    # COUNT, not membership. The child's own arm and the base-cold arm both cover
    # the inherited range unless the base arm is capped at the adopted baseline,
    # and `audit_data` concatenates them without dedup — so a missing cap shows up
    # as exact duplicates that a set() assertion cannot see.
    assert all_names.count("inside_lo") == 1, (
        "the inherited change row was emitted "
        f"{all_names.count('inside_lo')} times; the audit base arm is not capped at "
        f"the inherited baseline. Full list: {all_names}"
    )
    assert "inside_hi" not in names, (
        "audit_data emitted the parent's POST-fork row: the per-segment "
        f"max_commit_seq_num ceiling is not applied on the audit read path. Saw {names}"
    )


def test_fork_reads_when_the_parents_snapshot_covers_its_whole_persist():
    """A fork must read its inherited rows before it writes anything of its own.

    This is the DEFAULT parent shape, not a corner: `write -> persist ->
    snapshot` with no `snapshot_at` sets `snapshotted_at_micros` exactly equal to
    the persist watermark. A segments-only copy window then copies nothing to the
    persist tier, so the child has no persist header, `persist_wm` is NULL, and
    the read plan's snapshot pick — bounded by
    `snapshotted_at_micros <= LEAST(as_of, COALESCE(persist_wm, -1))` — collapses
    to -1 and never picks the inherited snapshot. The copy is present in metadata
    and the read returns zero rows, which is the same class of failure seeding
    `Pu` was meant to close; `Pu` is only one of the two gates.

    Every other fork-read test hides it by leaving an unpersisted tail on the
    parent or writing on the child first.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("pf_covered")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]}, schema=USER_SCHEMA
        ),
    )
    child = client.create_branch(
        f"pf_covered_kid_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-539",
    ).branch_uuid

    # No write on the child, no second persist. Straight read.
    got = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child,
    )
    assert set(got.column("name").to_pylist()) == {"alice", "bob", "carol"}, (
        "a fresh fork read nothing: the inherited snapshot was never picked, so "
        "the copy is invisible. Check that the child has a persist header at the "
        f"baseline watermark. Saw {got.column('name').to_pylist()}"
    )


def test_fork_audit_stays_deduped_after_the_child_snapshots():
    """The audit base-arm cap must survive the child taking its own snapshot.

    Companion to the adopted-baseline test below: this one keeps the child's own
    snapshot in the picture on an ordinary `write_cycle` parent, so the audit stays
    deduped once the child has a snapshot of its own at all.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("pf_dedup")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": ["alice", "bob"], "value": [1, 2]}, schema=USER_SCHEMA
        ),
    )
    child = client.create_branch(
        f"pf_dedup_kid_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-539",
    ).branch_uuid
    # The child's OWN snapshot, which is what drags an unbounded MIN() upward.
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child,
        upserts=pa.table({"name": ["dave"], "value": [4]}, schema=USER_SCHEMA),
    )

    upserts, _deletes = client.audit_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child,
        after_seq=0,
        before_seq=10_000,
    )
    names = upserts.column("name").to_pylist()
    for inherited in ("alice", "bob"):
        assert names.count(inherited) == 1, (
            f"inherited change row {inherited!r} emitted {names.count(inherited)} "
            "times after the child snapshotted: the baseline-watermark pick is not "
            f"bounded at the fork. Full list: {names}"
        )


def test_fork_audit_dedupes_against_a_real_adopted_baseline():
    """The audit cap's `Some(watermark)` branch — the one that actually caps.

    Both sibling audit tests build the genesis-inherit shape, where the parent's
    snapshot commits above the fork so no baseline is adopted and the base arm is
    skipped outright. They pass with or without the fork bound on
    `inherited_baseline_watermark`, which left the live branch uncovered.

    This fixture forces a real adoption: commit -> persist -> snapshot puts the
    parent's snapshot at or below the fork, then a further commit -> persist moves
    the head, and the fork takes the head. `copy_inherited_snapshot` picks that
    snapshot, so the child holds an inherited baseline and the watermark is
    `Some`. The child then snapshots, which is what drags an unbounded
    `MIN(snapshotted_at_micros)` above the fork and makes the base arm re-select
    every pre-fork parent segment the child already owns as a copy.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }

    _write_committed_rows(
        client, branch_uuid=main_branch, rows={"name": ["base"], "value": [1]}, **scope
    )
    client.persist(branch_uuid=main_branch, **scope)
    # Snapshot BEFORE the fork position, so its commit_seq_num lands at or below
    # the fork and `copy_inherited_snapshot` adopts it.
    client.snapshot(branch_uuid=main_branch, **scope)

    fork_seq = _write_committed_rows(
        client, branch_uuid=main_branch, rows={"name": ["tail"], "value": [2]}, **scope
    )
    client.persist(branch_uuid=main_branch, **scope)

    child = client.create_branch(
        "kid", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    ).branch_uuid

    # Setup check: a baseline really was adopted, or this degenerates into the
    # genesis-inherit case the siblings already cover.
    inherited = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
            " AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}")),
        (child, table_uuid),
    )
    # A non-AssertionError so a rotted fixture can never be mistaken for the
    # duplicate-row regression this test exists to pin.
    if inherited[0][0] == 0:
        raise RuntimeError(
            "setup failed: the child adopted no baseline, so this exercises the "
            "skip branch rather than the cap"
        )

    # The child's own snapshot: what drags an unbounded MIN() above the fork.
    _write_committed_rows(
        client, branch_uuid=child, rows={"name": ["own"], "value": [3]}, **scope
    )
    client.persist(branch_uuid=child, **scope)
    client.snapshot(branch_uuid=child, **scope)

    upserts, _deletes = client.audit_data(
        branch_uuid=child, after_seq=0, before_seq=10_000, **scope
    )
    names = upserts.column("name").to_pylist()
    for inherited_name in ("base", "tail"):
        assert names.count(inherited_name) == 1, (
            f"inherited change row {inherited_name!r} emitted "
            f"{names.count(inherited_name)} times: the audit base arm is not capped "
            f"at the adopted baseline. Full list: {names}"
        )


def _commit_with_author(
    client, *, catalog_uuid, schema_uuid, branch_uuid, table_uuid, rows, author, comment
) -> int:
    """`_write_committed_rows`, but recording author/comment on the tx.

    Separate from the shared helper so adding tx metadata to one test cannot
    change what the other thirteen in this file record.
    """
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        author=author,
        comment=comment,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=pa.table(rows, schema=USER_SCHEMA)),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )

    return client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid
    ).commit_seq_num


def test_fork_audit_reattaches_the_parents_author_for_inherited_rows():
    """A fork's inherited change rows keep the author/comment of the parent's tx.

    The assertion that distinguishes "the inherited range is served by the right
    arm" from "it is served by an arm that joins the wrong commit map". Since the
    fork copy, the child's OWN arm serves the inherited range, but a commit's
    author lives in the tx_log of the branch that made it — the parent's. The
    child's own tx_log holds no row for any inherited `commit_seq_num`, because
    `seed_commit_seq_num_from_fork` starts the child's commits above the fork.

    `build_cold_audit_sql` joins with a LEFT JOIN, so getting this wrong nulls
    author/comment silently instead of erroring — which is why it needs a
    positive assertion on the value and not merely on the column's presence.
    Every other fork-audit test here leaves `include_tx_metadata` at False.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }

    fork_seq = _commit_with_author(
        client,
        branch_uuid=main_branch,
        rows={"name": ["inherited"], "value": [1]},
        author="ada",
        comment="on the parent",
        **scope,
    )
    client.persist(branch_uuid=main_branch, **scope)
    client.snapshot(branch_uuid=main_branch, **scope)

    child = client.create_branch(
        "kid", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    ).branch_uuid

    upserts, _deletes = client.audit_data(
        branch_uuid=child,
        after_seq=0,
        before_seq=10_000,
        include_tx_metadata=True,
        **scope,
    )
    assert "author" in upserts.schema.names, (
        "include_tx_metadata=True must reattach author on a fork's audit"
    )
    by_name = {
        n: (a, c)
        for n, a, c in zip(
            upserts.column("name").to_pylist(),
            upserts.column("author").to_pylist(),
            upserts.column("comment").to_pylist(),
            strict=True,
        )
    }
    assert by_name["inherited"] == ("ada", "on the parent"), (
        "the inherited row must carry the parent tx's author/comment, not NULL: "
        f"the child's own arm serves it but joins its own tx_log. Saw {by_name}"
    )


def test_fork_audit_does_not_double_match_a_straddling_parent_tx_log():
    """A parent tx_log segment spanning the fork must not attribute twice.

    The parent's map is unioned into the child's own audit arm so inherited rows
    recover their author. `build_cold_audit_sql` puts no predicate on the tx_log
    side of the join, so bounding segment SELECTION and the data side is not
    enough: a parent segment straddling the fork is admitted whole, and its
    post-fork `commit_seq_num`s are exactly the values the child's own commits
    use — both counters continue from the fork. One own-arm row would then match
    two `t` rows and be emitted twice, one copy carrying the PARENT's author.

    Built in the shape that reaches it: an exact-seq ForkPoint below head, where
    the fork-time flush no-ops (`target <= w_txlog`) and so never re-cuts the
    segment at the fork. The sibling fork-audit tests all fork at head after a
    single commit, where the parent's segment happens to end exactly at the fork
    and the straddle cannot occur.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main_branch = setup_schema(client)
    scope = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
    }

    # Five commits on main under one author, then ONE persist — so the parent's
    # cold tx_log is a single segment spanning every seq, fork included.
    fork_seq = None
    for i in range(1, 6):
        seq = _commit_with_author(
            client,
            branch_uuid=main_branch,
            rows={"name": [f"p{i}"], "value": [i]},
            author="ada",
            comment="on the parent",
            **scope,
        )
        if i == 3:
            fork_seq = seq

    client.persist(branch_uuid=main_branch, **scope)
    client.snapshot(branch_uuid=main_branch, **scope)

    # The fixture is only meaningful if the parent's cold tx_log really does span
    # the fork. A RuntimeError, not an assert: a fixture that stopped reaching the
    # straddle would otherwise read as the behaviour under test passing, which is
    # the failure mode this whole test exists to rule out.
    straddling = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s"
            " AND min_commit_seq_num <= %s AND max_commit_seq_num > %s"
        ).format(tbl=Identifier(f"{catalog_uuid}_tx_log_persist_segment_metadata")),
        (main_branch, fork_seq, fork_seq),
    )
    if straddling[0][0] == 0:
        raise RuntimeError(
            "setup failed: no parent cold tx_log segment spans the fork seq "
            f"({fork_seq}), so the double-match this test pins is unreachable — "
            "the fork-time flush must have re-cut the segment"
        )

    child = client.create_branch(
        "kid", "t", "fork", commit_seq_num=fork_seq, catalog_uuid=catalog_uuid
    ).branch_uuid
    _commit_with_author(
        client,
        branch_uuid=child,
        rows={"name": ["own"], "value": [99]},
        author="kid",
        comment="on the child",
        **scope,
    )
    client.persist(branch_uuid=child, **scope)

    upserts, _deletes = client.audit_data(
        branch_uuid=child,
        after_seq=0,
        before_seq=10_000,
        include_tx_metadata=True,
        **scope,
    )
    names = upserts.column("name").to_pylist()
    authors = upserts.column("author").to_pylist()

    # The child's own row: exactly once, and attributed to the CHILD. Twice, or
    # once with author "ada", is the straddle leaking through.
    assert names.count("own") == 1, (
        f"the child's own change row was emitted {names.count('own')} times: a "
        f"straddling parent tx_log segment double-matched it. Full list: {names}"
    )
    own_authors = {a for n, a in zip(names, authors, strict=True) if n == "own"}
    assert own_authors == {"kid"}, (
        f"the child's own row was attributed to the parent's tx at the same "
        f"commit_seq_num, saw {own_authors}"
    )

    # And the parent's post-fork commits stay invisible, on both axes.
    for post_fork in ("p4", "p5"):
        assert post_fork not in names, (
            f"{post_fork!r} was committed on main above the fork and must not "
            f"appear in the child's audit. Full list: {names}"
        )
