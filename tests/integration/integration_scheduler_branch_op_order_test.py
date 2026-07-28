"""The RPC ordering contract the lifecycle scheduler's two loops depend on.

The scheduler no longer calls ``PersistAndSnapshotBranch``. Its persist loop
drives ``PersistBranch`` and its snapshot loop drives ``SnapshotBranch``, on
independent cadences — so the two run with an arbitrary gap between them, and
Snapshot must work off state a *different* loop's earlier tick produced.

Both loops are pinned to ``-1`` in the test profile
(``SCHEDULER_{PERSIST,SNAPSHOT}_TICK_INTERVAL_SECONDS``), so nothing here races
a sweep. These tests drive the same RPCs by hand, in the scheduler's order, to
pin the contract the split relies on:

1. ``PersistBranch`` alone makes a table durable in cold WITHOUT snapshotting it
   — the snapshot loop may not have ticked yet.
2. ``SnapshotBranch`` alone then snapshots it, enumerating the PERSISTED set
   rather than the hot-modified one.
3. Purge's committed axis only advances after that Snapshot, since it is gated
   on ``Pu = W_snap`` — which is why Purge rides the snapshot loop.

Run via ``just integration-test scheduler_branch_op_order``.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import TABLE_SNAPSHOT_METADATA
from psycopg.sql import SQL, Identifier

from .integration_helpers import USER_SCHEMA, get_pg_driver, make_client, setup_schema


def _snapshot_count(catalog_uuid, branch_uuid, table_uuid) -> int:
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )

    return rows[0][0]


def _write_one_row(client, catalog_uuid, schema_uuid, branch_uuid, table_uuid, name):
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
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
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def test_persist_branch_does_not_snapshot():
    """The persist loop must not produce snapshots — that is the other loop's job.

    If PersistBranch snapshotted as a side effect, the cadence split would be
    meaningless: the expensive compaction would run at the short interval.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main = setup_schema(client)
    _write_one_row(client, catalog_uuid, schema_uuid, main, table_uuid, "a")

    resp = client.persist_branch(catalog_uuid=catalog_uuid, branch_uuid=main)

    assert resp.HasField("watermark"), (
        "a fully-successful PersistBranch must return its fork watermark; an "
        "absent watermark is the partial-failure signal"
    )
    assert _snapshot_count(catalog_uuid, main, table_uuid) == 0, (
        "PersistBranch must not snapshot — the snapshot loop owns that, on its "
        "own cadence"
    )


def test_snapshot_branch_consumes_a_previous_persist():
    """SnapshotBranch works off the PERSISTED set an earlier persist tick left.

    This is the cross-loop hand-off: the two loops tick independently, so the
    snapshot loop only ever sees state the persist loop already committed.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main = setup_schema(client)
    _write_one_row(client, catalog_uuid, schema_uuid, main, table_uuid, "a")

    client.persist_branch(catalog_uuid=catalog_uuid, branch_uuid=main)
    assert _snapshot_count(catalog_uuid, main, table_uuid) == 0

    resp = client.snapshot_branch(catalog_uuid=catalog_uuid, branch_uuid=main)

    assert resp.HasField("watermark")
    assert _snapshot_count(catalog_uuid, main, table_uuid) > 0, (
        "SnapshotBranch must snapshot a table an earlier PersistBranch made "
        "durable, enumerating the persisted set rather than hot-modified state"
    )


def test_purge_committed_axis_requires_the_snapshot_tick():
    """Purge's committed axis is gated on Pu = W_snap.

    This is the mechanical reason Purge rides the snapshot loop rather than the
    faster persist loop: with no Snapshot yet, the committed axis cannot advance,
    so purging on a persist tick would be a wasted round-trip.
    """
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main = setup_schema(client)
    _write_one_row(client, catalog_uuid, schema_uuid, main, table_uuid, "a")

    client.persist_branch(catalog_uuid=catalog_uuid, branch_uuid=main)
    before = client.purge(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
    )
    assert not before.HasField("purged_at_micros"), (
        "with no Snapshot yet there is no W_snap for Pu to advance to, so the "
        "committed axis must not advance"
    )

    client.snapshot_branch(catalog_uuid=catalog_uuid, branch_uuid=main)
    after = client.purge(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
    )

    assert after.HasField("purged_at_micros"), (
        "once the snapshot loop has ticked, Purge's committed axis advances to W_snap"
    )
