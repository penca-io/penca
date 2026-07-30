"""CHA-539 red test: branch deletion must not unlink another branch's cold files.

``delete_branch`` Phase 2 unlinks every URI its enumeration reaches with no
enqueue, no grace window and no refcount probe. Post-CHA-531 those URIs cross
the fork edge in **both** directions — a carried row lives in the child's
partition while its ``object_uri`` names the file the parent wrote — so deleting
either side of a fork destroys the other's data.

The existing suite stays green on this only because
``integration_sandbox_demo_test.py`` discards its forks before they ever
snapshot, so no carried rows exist.

**Never read the branch under test before the destructive step.** The query
service holds a process-lifetime decoded-segment cache (keyed by segment uuid,
no TTL), so a "setup sanity" read warms it and the post-delete read is served
from memory without touching object storage — the data loss is then invisible
and the test passes. The first draft of this file did exactly that. Setup checks
here therefore assert on metadata, which is also a stronger claim: that
carry-forward really did share a file.

Run via ``just integration-test branch_delete_refcount``.
"""

from __future__ import annotations

import time
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client.errors import InvalidRequestError
from penca_client.naming import (
    SEGMENT_DELETE_SET,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    setup_partitioned_table,
    write_cycle,
)

_SEED = pa.table(
    {
        "name": ["alice", "bob", "carol", "dave"],
        "value": [1, 2, 3, 4],
    },
    schema=USER_SCHEMA,
)
_SEED_NAMES = {"alice", "bob", "carol", "dave"}


def _snapshot_uris(catalog_uuid, branch_uuid) -> set[str]:
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT DISTINCT object_uri FROM {tbl}"
            " WHERE branch_uuid = %s AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(seg)),
        (branch_uuid,),
    )

    return {r[0] for r in rows}


def _age_queued_rows(catalog_uuid, uris):
    """Push the queued rows past the grace window.

    Load-bearing for any post-sweep survival assertion: the gate is
    `written_at_micros < now - query_timeout`, and the teardown enqueue stamps
    `now`. With QUERY_TIMEOUT_SECONDS=2 in docker/test.env a sweep firing
    milliseconds later finds the rows ineligible whatever the refcount arms
    say, so an unaged assertion passes on a fix that enqueues but leaves the
    gate blind — and flips to testing the gate only if teardown happens to take
    over 2s. Aging first makes the sweep's decision the only variable.
    """
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    get_pg_driver().execute_no_result(
        SQL(
            "UPDATE {tbl} SET written_at_micros = %s WHERE object_uri = ANY(%s)"
        ).format(tbl=Identifier(tbl)),
        (int(time.time() * 1_000_000) - 10_000_000, list(uris)),
    )


def _drop_reference_rows(catalog_uuid, branch_uuid):
    """Drop a branch's committed cold reference rows in both tiers."""
    for table_name in (TABLE_PERSIST_SEGMENT_METADATA, TABLE_SNAPSHOT_SEGMENT_METADATA):
        get_pg_driver().execute_no_result(
            SQL("DELETE FROM {tbl} WHERE branch_uuid = %s").format(
                tbl=Identifier(f"{catalog_uuid}_{table_name}")
            ),
            (branch_uuid,),
        )


def _queued_uris(catalog_uuid, uris) -> set[str]:
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    rows = get_pg_driver().execute(
        SQL("SELECT object_uri FROM {tbl} WHERE object_uri = ANY(%s)").format(
            tbl=Identifier(tbl)
        ),
        (list(uris),),
    )

    return {r[0] for r in rows}


def test_delete_main_is_rejected():
    """`main` is the catalog's root, not a tear-downable branch.

    Deleting it leaves every subsequent read failing with "main branch missing
    for catalog" — nothing to do with cold data. While forks are main-only
    (CHA-515) "delete the branch a fork inherits from" always means deleting
    `main`, so rejecting it removes a case that was never coherent rather than
    substituting for the refcount gate.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("bdr_main")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=_SEED,
    )

    with pytest.raises(InvalidRequestError, match="main branch"):
        client.delete_branch(branch_uuid=main_branch, catalog_uuid=catalog_uuid)

    # And the catalog is still usable afterwards — the guard rejected the request
    # rather than half-applying it.
    got = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
    )
    assert set(got.column("name").to_pylist()) == _SEED_NAMES


def test_branch_teardown_queues_its_files_and_the_gate_spares_shared_ones():
    """Teardown hands its cold URIs to the refcount gate instead of unlinking.

    A snapshotted fork's URIs split two ways, which gives the survival assertion
    and its positive control in ONE sweep:

    * **carried** (shared with the parent) — must survive, the parent still
      names them;
    * **child-only** (the partition the child rewrote) — must be collected,
      nothing names them once the child's rows are gone.

    Both halves matter. Survival alone is also what a sweep that never considered
    the URIs eligible looks like, and collection alone would not show the gate
    protecting anything.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("bdr_tear")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=_SEED,
    )
    child = client.create_branch(
        f"bdr_tear_kid_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-539",
    ).branch_uuid
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )

    parent_uris = _snapshot_uris(catalog_uuid, main_branch)
    child_uris = _snapshot_uris(catalog_uuid, child)
    shared = parent_uris & child_uris
    child_only = child_uris - parent_uris
    assert shared, (
        "setup failed: carry-forward did not share a file across the fork edge"
    )
    assert child_only, (
        "setup failed: the child rewrote no partition of its own, so there is no "
        "positive control"
    )

    client.delete_branch(branch_uuid=child, catalog_uuid=catalog_uuid)

    queued = _queued_uris(catalog_uuid, child_uris)
    assert queued == child_uris, (
        "teardown must ENQUEUE its cold URIs onto segment_delete_set and let the "
        f"refcount gate decide. Missing from the queue: {sorted(child_uris - queued)}"
    )

    # Age past grace, or the sweep skips them on the clock alone and proves nothing.
    _age_queued_rows(catalog_uuid, child_uris)
    client.sweep_segments(catalog_uuid=catalog_uuid)

    survived = _queued_uris(catalog_uuid, shared)
    assert survived == shared, (
        "the sweep collected files the PARENT still references; the refcount gate "
        f"did not see its rows. Swept: {sorted(shared - survived)}"
    )
    assert _queued_uris(catalog_uuid, child_only) == set(), (
        "the child-only URIs survived a sweep with zero remaining references, so "
        "the survival above proves nothing about the gate — either the sweep never "
        "treated them as eligible, or the cold delete failed and left the row"
    )


def test_delete_child_after_snapshot_preserves_parent_reads():
    """Deleting a fork that has snapshotted must not cost the PARENT its data.

    Since CHA-531 a fork's first snapshot carries the parent's untouched
    partitions **by reference**: the row lives in the child's partition while its
    ``object_uri`` names the file the parent wrote. So the child's teardown
    enumeration reaches across the fork edge into the parent's files.

    Small partitions pack into one file at distinct offsets, so the single file
    holding the seeded rows is exactly the one the child carries — unlinking it
    costs the parent every row.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("bdr_child")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=_SEED,
    )

    child = client.create_branch(
        f"bdr_kid_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-539",
    ).branch_uuid

    # One row into ONE partition, then snapshot: the rest carry by reference.
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=child,
        upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
    )

    # Metadata-only setup check — a read here would warm the decoded-segment
    # cache and mask the very loss this test exists to catch.
    shared = _snapshot_uris(catalog_uuid, main_branch) & _snapshot_uris(
        catalog_uuid, child
    )
    assert shared, (
        "setup failed: carry-forward did not share a file across the fork edge,"
        " so deleting the child could not reach the parent's data and this test"
        " would prove nothing"
    )

    client.delete_branch(branch_uuid=child, catalog_uuid=catalog_uuid)

    got = client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
    )
    assert set(got.column("name").to_pylist()) == _SEED_NAMES, (
        "deleting the fork destroyed the PARENT's data: the child's carried"
        " snapshot rows name files the parent wrote, and Phase 2 unlinks every"
        " URI its enumeration reaches"
    )
