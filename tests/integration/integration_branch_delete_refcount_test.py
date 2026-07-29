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

from uuid import uuid4

import pyarrow as pa
from penca_client.naming import SEGMENT_DELETE_SET, TABLE_SNAPSHOT_SEGMENT_METADATA
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


def _queued_uris(catalog_uuid, uris) -> set[str]:
    tbl = f"{catalog_uuid}_{SEGMENT_DELETE_SET}"
    rows = get_pg_driver().execute(
        SQL("SELECT object_uri FROM {tbl} WHERE object_uri = ANY(%s)").format(
            tbl=Identifier(tbl)
        ),
        (list(uris),),
    )

    return {r[0] for r in rows}


def test_delete_parent_queues_its_files_instead_of_unlinking_them():
    """Tearing down the branch a fork inherits from must hand its files to the
    refcount gate, not unlink them.

    Asserted on the delete-set queue rather than on a child read. The child's
    read cannot be the observable here: forks are main-only, so the parent IS
    ``main``, and deleting ``main`` breaks catalog resolution outright
    (``main branch missing for catalog ...``) for reasons that have nothing to do
    with CHA-539. Queue membership isolates what this ticket actually changes —
    Phase 2 unlinks immediately and enqueues nothing, so the files are gone
    before any gate can weigh the fork's claim on them.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("bdr_parent")
    )
    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=_SEED,
    )

    parent_uris = _snapshot_uris(catalog_uuid, main_branch)
    assert parent_uris, "setup failed: the parent must hold committed cold segments"

    client.create_branch(
        f"bdr_child_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-539",
    )

    client.delete_branch(branch_uuid=main_branch, catalog_uuid=catalog_uuid)

    queued = _queued_uris(catalog_uuid, parent_uris)
    assert queued == parent_uris, (
        "branch teardown must ENQUEUE its cold URIs onto segment_delete_set and"
        " let the refcount gate decide, not unlink them in Phase 2. Missing from"
        f" the queue: {sorted(parent_uris - queued)}"
    )

    # The fork's claim is what must keep them alive through a sweep.
    client.sweep_segments(catalog_uuid=catalog_uuid)
    still_queued = _queued_uris(catalog_uuid, parent_uris)
    assert still_queued == parent_uris, (
        "the sweep collected files the fork still inherits; the refcount gate"
        f" did not see the fork's reference. Swept: {sorted(parent_uris - still_queued)}"
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
