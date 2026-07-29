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
from penca_client.naming import TABLE_PERSIST_SEGMENT_METADATA
from psycopg.sql import SQL, Identifier

from .integration_helpers import USER_SCHEMA, get_pg_driver, make_client, setup_schema


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
