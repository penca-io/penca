"""Forward regression guard for CHA-509 — snapshot recovers a persisted table.

Snapshot enumeration keys on **persisted** (not hot-modified) data, so a table
that was persisted then purged still gets re-snapshotted on a lifecycle sweep.

This is a deliberately **skipped** guard: the bug it targets is not reproducible
today. Per-table Purge is snapshot-gated — ``Pu = W_snap`` (``purge.rs:121``),
no load-shed valve is wired — so a persisted-but-unsnapshotted table always
still has hot ``commit_tx_log`` rows (is still "modified") and gets snapshotted
regardless. The snapshot-on-persisted enumeration (CHA-509) is written
forward-compatibly; this test activates when CHA-466's purge-past-snapshot
load-shed valve (``Pu > W_snap``) lands. CHA-444 shipped the machinery and
deliberately parked the operating point at the floor ``Pu = W_snap``; CHA-466 is
what makes ``Pu`` a dial within ``[W_snap, P - grace]``.

Run via ``just integration-test snapshot_persist_recovery`` → 1 skipped.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
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


@pytest.mark.skip(
    reason="degradation recovery requires the CHA-466 purge-past-snapshot "
    "load-shed valve (Pu>W_snap), not yet wired; snapshot-on-persisted "
    "(CHA-509) is forward-compatible. See CHA-509."
)
def test_snapshot_recovers_persisted_then_purged_table():
    client = make_client()
    schema_uuid, table_uuid, catalog_uuid, main = setup_schema(client)

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=pa.table({"name": ["a"], "value": [1]}, schema=USER_SCHEMA),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main)

    client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main,
        table_uuid=table_uuid,
    )
    # MISSING PIECE: a load-shed purge that advances Pu past W_snap (CHA-466
    # valve, not yet wired). Once it exists, drop the table's hot rows here
    # WITHOUT snapshotting, so it leaves `list_modified` but stays in
    # `list_persisted`. Until then the degradation state is unreachable.

    client.persist_and_snapshot_branch(catalog_uuid=catalog_uuid, branch_uuid=main)

    assert _snapshot_count(catalog_uuid, main, table_uuid) > 0, (
        "a persisted (then purged-past-snapshot) table must still be snapshotted"
    )
