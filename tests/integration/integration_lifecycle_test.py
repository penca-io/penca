"""Integration tests for LifecycleService (persist, compact, snapshot).

Run via ``just integration-test``.
"""

from __future__ import annotations

import os
import re
import time
from uuid import uuid4

import psycopg
import pyarrow as pa
import pytest
from grpc import RpcError, StatusCode, insecure_channel
from penca_client import Mutation
from penca_client.naming import (
    COMPACT_SEGMENT_METADATA,
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_PURGE_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
    row_uuid_for_pk,
    table_snapshot_uuid,
    upsert_log_table,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    MAX_SEGMENT_BYTES,
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    setup_schema,
    setup_with_data,
)

_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])

# CHA-233 (ADR 0019): Purge is grace-bounded — gated on
# ``now - max_committed_at > query_timeout``. Tests that need to
# observe Purge actually deleting hot rows must sleep past the grace
# window. Mirrors ``integration_grace_window_test.py``.
_QUERY_TIMEOUT_SECONDS = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "2"))
_GRACE_WAIT_SECONDS = _QUERY_TIMEOUT_SECONDS + 1.0


def _setup_branch_with_committed_data(
    client, catalog_uuid, schema_uuid, table_uuid, branch_name
):
    """Create a branch, create the table on it, insert data, commit.

    Returns (branch, committed_tx).
    """
    branch = client.create_branch(
        branch_name,
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_branch",
    )
    create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch.branch_uuid,
    )
    batch = pa.table(
        {"name": ["alice", "bob"], "value": [10, 20]},
        schema=USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=batch,
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch.branch_uuid,
    )
    committed_tx = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch.branch_uuid,
    )
    return branch, committed_tx


class TestPersistBasic:
    def test_persist(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_branch"
        )
        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert response.persisted_at_micros > 0

    def test_persist_then_read(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_read_branch"
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 2
        names = result.column("name").to_pylist()
        assert "alice" in names
        assert "bob" in names


class TestPersistIdentifierResolution:
    def test_persist_with_branch_uuid(self):
        """Persist using catalog_uuid + branch_uuid + table_uuid."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_uuid_branch"
        )
        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert response.persisted_at_micros > 0

    def test_persist_with_branch_name(self):
        """Persist using catalog_name + branch_name."""
        client = make_client()
        catalog_name = f"persist_name_cat_{uuid4().hex[:8]}"
        catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
        schema_uuid = client.create_schema(
            "persist_name_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        table_uuid = client.create_table(
            "persist_name_table",
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )
        branch = client.create_branch(
            "persist_name_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(
            client,
            catalog_uuid,
            schema_uuid,
            branch.branch_uuid,
            table_name="persist_name_table",
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        response = client.persist(
            catalog_name=catalog_name,
            schema_name="persist_name_schema",
            branch_name="persist_name_branch",
            table_name="persist_name_table",
        )
        assert response.persisted_at_micros > 0


class TestPersistCorrectness:
    def test_purge_clears_hot_data(self):
        """After Persist(T) → Snapshot(T) → Purge(T), hot upsert rows are gone.

        Post-CHA-220: persist leaves hot intact; purge is the operation
        that empties the hot upsert/delete log up to T's read fence.
        CHA-444 (ADR 0027): the fence ``Pu`` advances only to ``W_snap``,
        so a Snapshot must run before Purge clears the committed hot rows.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_clear_branch"
        )
        hot_upsert_table = upsert_log_table(table_uuid, branch.branch_uuid)
        rows_before = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(hot_upsert_table)),
        )
        assert rows_before[0][0] > 0

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        # Persist alone leaves hot intact (CHA-220).
        rows_post_persist = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(hot_upsert_table)),
        )
        assert rows_post_persist[0][0] == rows_before[0][0]

        # CHA-444 (ADR 0027): Purge advances Pu only to W_snap, so Snapshot
        # must run first for Purge to clear the committed hot rows.
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        rows_after = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(hot_upsert_table)),
        )
        assert rows_after[0][0] == 0

    def test_persist_multiple_transactions(self):
        """Persist captures data from multiple committed transactions."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "persist_multi_tx_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch1 = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch1,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch2 = pa.table(
            {"name": ["bob"], "value": [2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch2,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert response.persisted_at_micros > 0

        # Verify readable end-to-end. Per CHA-220 the rows live in hot
        # (pre-purge) and in cold (post-persist); the merge layer dedups.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 2

    def test_persist_ignores_uncommitted_data(self):
        """Data from uncommitted transactions stays in hot storage after persist."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "persist_uncommitted_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch1 = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch1,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch2 = pa.table(
            {"name": ["bob"], "value": [2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch2,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        # tx2 NOT committed

        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        # Per-table persist; correctness invariant is that bob (uncommitted)
        # stays in hot and is not visible — checked by reading back below.
        assert response.persisted_at_micros > 0
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        # Committed alice visible; uncommitted bob is not (no RYOW here).
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]

    def test_persist_after_purge_is_noop_until_new_commits(self):
        """``Persist(T) → Purge(T) → Persist(T)`` second persist is a no-op.

        Per the scheduler loop ([CHA-154](https://linear.app/chapala/issue/CHA-154)),
        Purge is what clears the hot rows up to the prior persist watermark;
        without new commits after Purge, the next Persist reads zero hot
        rows and returns ``persisted_at_micros = 0``.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_idempotent_branch"
        )

        first = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert first.persisted_at_micros > 0

        # CHA-233: Purge is grace-bounded; wait the cap so the hot
        # DELETE fires (and the next Persist sees an empty hot log).
        time.sleep(_GRACE_WAIT_SECONDS)
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        second = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert not second.HasField("persisted_at_micros")

    def test_persist_upserts_and_deletes(self):
        """Persist correctly handles both upsert and delete logs."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "persist_del_branch",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch.branch_uuid)

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice", "bob"], "value": [1, 2]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )

        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )
        assert response.persisted_at_micros > 0

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["bob"]

    def test_persist_preserves_data_across_branches(self):
        """Persisting branch A does not affect branch B."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        branch_a, _tx_a = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_iso_a"
        )

        branch_b = client.create_branch(
            "persist_iso_b",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_b.branch_uuid)
        tx_b = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        batch_b = pa.table(
            {"name": ["charlie"], "value": [3]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx_b.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch_b,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        client.commit_tx(
            tx_b.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_b.branch_uuid,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_a.branch_uuid,
            table_uuid=table_uuid,
        )

        result_b = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_b.branch_uuid,
        )
        assert result_b.num_rows == 1
        assert result_b.column("name").to_pylist() == ["charlie"]


class TestPersistTwoPhase:
    def test_persist_segment_has_committed_at(self):
        """After persist, table_persist_segment_metadata rows have non-NULL commit_micros."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_commit_at_branch"
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        # CHA-203: classification flows through the parent's `log_kind`
        # column; segments JOIN up to find their kind.
        seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
        tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
        rows = get_pg_driver().execute(
            SQL(
                "SELECT seg.commit_micros"
                " FROM {seg} seg"
                " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
                "                       AND seg.branch_uuid = tfm.branch_uuid"
                " WHERE tfm.branch_uuid = %s"
                "   AND tfm.table_uuid = %s"
                "   AND tfm.log_kind = 'upsert_log'"
            ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
            (branch.branch_uuid, table_uuid),
        )
        assert len(rows) >= 1
        assert all(row[0] is not None for row in rows)

    def test_uncommitted_segment_invisible_to_reads(self):
        """Segments with NULL commit_micros are excluded from read plan."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "persist_invis_branch"
        )
        # Persist normally first to get data into cold.
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        # Insert a fake table_persist parent (committed) + a fake segment
        # with NULL commit_micros. The per-segment commit gate is
        # what matters for visibility; CHA-220 dropped the outer
        # branch_persist level entirely, so the parent table_persist row is
        # the only thing the segment hangs off.
        fake_table_persist_uuid = str(uuid4())
        table_persist_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
        segment_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
        get_pg_driver().execute_no_result(
            SQL(
                "INSERT INTO {tbl}"
                " (table_persist_uuid, branch_uuid, table_uuid,"
                "  persisted_at_micros, log_kind, commit_micros)"
                " VALUES (%s, %s, %s, 0, 'upsert_log', 0)"
            ).format(tbl=Identifier(table_persist_parent)),
            (
                fake_table_persist_uuid,
                branch.branch_uuid,
                table_uuid,
            ),
        )
        get_pg_driver().execute_no_result(
            SQL(
                "INSERT INTO {tbl}"
                " (table_persist_segment_uuid, table_persist_uuid, branch_uuid,"
                "  table_uuid,"
                "  min_tx_commit_micros, max_tx_commit_micros,"
                "  min_commit_seq_num, max_commit_seq_num,"
                "  object_uri, row_count, format)"
                " VALUES (%s, %s, %s, %s, 0, 0,"
                "  0, 0,"
                "  'fake://upsert', 999, 'parquet')"
            ).format(tbl=Identifier(segment_parent)),
            (
                str(uuid4()),
                fake_table_persist_uuid,
                branch.branch_uuid,
                table_uuid,
            ),
        )

        # The fake segment has NULL commit_micros (column not in INSERT).
        # Read should still return only the real data (2 rows), not 999+2.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch.branch_uuid,
        )
        assert result.num_rows == 2


class TestBranchDeletionColdStorageCleanup:
    def test_branch_delete_cleans_segment_metadata(self):
        """After deleting a branch, table_persist_segment_metadata rows for
        its per-branch data tables are removed."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch, _tx = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "delete_cold_branch"
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch.branch_uuid,
            table_uuid=table_uuid,
        )

        # Verify segment metadata exists. CHA-220 persist no longer writes
        # commit_tx_log cold segments (the tx-log family is hot-only until
        # CHA-221), so the scope is just T's segments.
        segment_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
        rows_before = get_pg_driver().execute(
            SQL(
                "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
            ).format(tbl=Identifier(segment_parent)),
            (branch.branch_uuid, table_uuid),
        )
        assert rows_before[0][0] > 0

        client.delete_branch(
            catalog_uuid=catalog_uuid,
            branch_uuid=branch.branch_uuid,
        )

        # Segment metadata should be cleaned up. DeleteBranch cascades
        # via DROP TABLE on the branch's leaf partition, so the parent
        # scan above returns zero rows for the dropped branch.
        rows_after = get_pg_driver().execute(
            SQL(
                "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
            ).format(tbl=Identifier(segment_parent)),
            (branch.branch_uuid, table_uuid),
        )
        assert rows_after[0][0] == 0


def _insert_and_commit(
    client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, rows
):
    """Insert rows and commit a transaction. Returns the committed tx."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=table_uuid,
            upserts=batch,
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


def _count_table_persist_segments(
    client, catalog_uuid, branch_uuid, table_uuid, log_kind
):
    """Count committed segments for a ``(branch, table, log_kind)`` tuple
    in the per-catalog ``{catalog_uuid}_table_persist_segment_metadata``
    parent.

    CHA-203: segments classify via the parent ``table_persist_metadata``
    row's ``log_kind`` column; the read JOINs up.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(*)"
            " FROM {seg} seg"
            " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
            "                       AND seg.branch_uuid = tfm.branch_uuid"
            " WHERE tfm.branch_uuid = %s"
            "   AND tfm.table_uuid = %s"
            "   AND tfm.log_kind = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (branch_uuid, table_uuid, log_kind),
    )
    return rows[0][0]


def _count_distinct_segment_files(
    client, catalog_uuid, branch_uuid, table_uuid, log_kind
):
    """Count *distinct* underlying object_uri across committed segments
    for a ``(branch, table, log_kind)``. Compact (CHA-168) follows the
    snapshot-segment pattern: metadata rows STAY, get UPDATEd to point
    at slices of a shared merged file.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(DISTINCT seg.object_uri)"
            " FROM {seg} seg"
            " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
            "                       AND seg.branch_uuid = tfm.branch_uuid"
            " WHERE tfm.branch_uuid = %s"
            "   AND tfm.table_uuid = %s"
            "   AND tfm.log_kind = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (branch_uuid, table_uuid, log_kind),
    )
    return rows[0][0]


def _select_persist_segment_seal_states(
    catalog_uuid, branch_uuid, table_uuid, log_kind
):
    """Return ``[(object_uri, is_sealed), ...]`` for every committed
    persist-segment row in ``(branch, table, log_kind)``. Used by the
    active+sealed compact tests to verify which rows transitioned.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT seg.object_uri, seg.is_sealed"
            " FROM {seg} seg"
            " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
            "                       AND seg.branch_uuid = tfm.branch_uuid"
            " WHERE tfm.branch_uuid = %s"
            "   AND tfm.table_uuid = %s"
            "   AND tfm.log_kind = %s"
            "   AND seg.commit_micros IS NOT NULL"
            " ORDER BY seg.min_tx_commit_micros"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (branch_uuid, table_uuid, log_kind),
    )
    return [(r[0], r[1]) for r in rows]


def _spoof_unsealed_persist_segment_sizes(
    catalog_uuid, branch_uuid, table_uuid, log_kind, size_bytes
):
    """Force ``size_bytes`` on every unsealed persist-segment row in
    ``(branch, table, log_kind)``. The active+sealed compact algorithm
    reads ``size_bytes`` from the segment rows to decide whether a fold
    would breach ``max_segment_bytes``; spoofing here lets a test fire
    the threshold deterministically without writing 64 MB of real data.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    get_pg_driver().execute_no_result(
        SQL(
            "UPDATE {seg}"
            "   SET size_bytes = %s"
            " WHERE branch_uuid = %s"
            "   AND is_sealed = FALSE"
            "   AND table_persist_uuid IN ("
            "     SELECT table_persist_uuid FROM {tfm}"
            "      WHERE branch_uuid = %s AND table_uuid = %s AND log_kind = %s"
            "   )"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (size_bytes, branch_uuid, branch_uuid, table_uuid, log_kind),
    )


class TestCompactPersistSegments:
    def test_noop_no_segments(self):
        """Compact with no persisted data returns an empty
        merged_object_uris list."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert list(response.merged_object_uris) == []

    def test_noop_single_segment(self):
        """One persist per kind — every group is size 1, no merging."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = _setup_branch_with_committed_data(
            client, catalog_uuid, schema_uuid, table_uuid, "compact_single"
        )[0].branch_uuid

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert list(response.merged_object_uris) == []

    def test_extend_active_below_threshold(self):
        """CHA-202 active+sealed compact: while the cumulative unsealed
        size stays under ``max_segment_bytes``, each compact wave folds
        every uncompacted segment into a single active merged file (the
        prior active, if any, is rewritten under a fresh URI with the
        new fold included).

        Goes through two waves: a fresh extend (no prior active) and an
        extend of an existing active. Both must leave every row with
        ``is_sealed = false``.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "compact_extend",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Wave 1: two persists → 2 uncompacted segments → fresh extend.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice"], "value": [10]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["bob"], "value": [20]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # 2 metadata rows, 2 distinct files before compact.
        assert (
            _count_table_persist_segments(
                client, catalog_uuid, branch_uuid, table_uuid, "upsert_log"
            )
            == 2
        )
        assert (
            _count_distinct_segment_files(
                client, catalog_uuid, branch_uuid, table_uuid, "upsert_log"
            )
            == 2
        )

        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert len(response.merged_object_uris) >= 1

        # Snapshot-segment pattern: rows STAY at 2, distinct files drop
        # to 1. Both rows is_sealed=false (the new active).
        assert (
            _count_table_persist_segments(
                client, catalog_uuid, branch_uuid, table_uuid, "upsert_log"
            )
            == 2
        )
        assert (
            _count_distinct_segment_files(
                client, catalog_uuid, branch_uuid, table_uuid, "upsert_log"
            )
            == 1
        )
        post_wave1 = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(post_wave1) == 2
        assert all(not is_sealed for _uri, is_sealed in post_wave1)
        wave1_active_uri = post_wave1[0][0]

        # Wave 2: one more persist, still well under threshold → extend
        # the active. Distinct file count stays at 1 (the new active
        # under a fresh URI replaces the prior one); rows STAY all
        # unsealed.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["carol"], "value": [30]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        post_wave2 = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(post_wave2) == 3
        assert all(not is_sealed for _uri, is_sealed in post_wave2)
        assert len({uri for uri, _ in post_wave2}) == 1
        # The wave-2 active is a fresh URI (the prior active's file is
        # deleted post-commit; the metadata rows point at the new one).
        assert post_wave2[0][0] != wave1_active_uri

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 3
        names = result.column("name").to_pylist()
        assert {"alice", "bob", "carol"} <= set(names)

    def test_seal_and_start_new_when_active_full(self):
        """CHA-202 seal-and-start-new: when the active merged file is at
        ``max_segment_bytes`` and the next uncompacted segment would
        breach it, the active's rows transition to ``is_sealed=true``
        in the same wave and a fresh active is started from the next
        uncompacted segment.

        Drives the threshold deterministically by spoofing
        ``size_bytes`` on the unsealed rows before each compact —
        writing 64 MB per persist would be infeasible.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "compact_seal",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Two persists → 2 uncompacted segments. First compact folds them
        # into a fresh active.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice"], "value": [10]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["bob"], "value": [20]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Force the active's rows to look "full" — each ≥ max_segment_bytes/2
        # so any further fold breaches the live cap.
        _spoof_unsealed_persist_segment_sizes(
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "upsert_log",
            size_bytes=MAX_SEGMENT_BYTES,
        )

        # Two more persists → 2 fresh uncompacted segments after the
        # active. Compact must seal the prior active and fold both
        # uncompacted into a new active.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["carol"], "value": [30]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["dave"], "value": [40]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        post = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        # 4 rows total: 2 sealed pointing at the prior active, 2 unsealed
        # pointing at the new active. Two distinct merged files.
        assert len(post) == 4
        sealed = [(u, s) for (u, s) in post if s]
        unsealed = [(u, s) for (u, s) in post if not s]
        assert len(sealed) == 2
        assert len(unsealed) == 2
        assert len({u for (u, _) in sealed}) == 1
        assert len({u for (u, _) in unsealed}) == 1
        # The sealed and new-active files must be distinct cold files.
        assert sealed[0][0] != unsealed[0][0]

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 4
        names = result.column("name").to_pylist()
        assert {"alice", "bob", "carol", "dave"} <= set(names)

    def test_compact_idempotent_after_seal(self):
        """A second compact wave with no new persists is a no-op even
        when the scope has both sealed and unsealed rows: sealed rows
        are excluded from enumeration; the active alone has nothing to
        fold against.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "compact_seal_idempotent",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Reach a seal state: wave 1 folds two persists into an active,
        # wave 2 seals it and starts a new active from two more persists.
        for name, value in [("alice", 10), ("bob", 20)]:
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                {"name": [name], "value": [value]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _spoof_unsealed_persist_segment_sizes(
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "upsert_log",
            size_bytes=MAX_SEGMENT_BYTES,
        )
        for name, value in [("carol", 30), ("dave", 40)]:
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                {"name": [name], "value": [value]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Snapshot the world before the idempotent re-compact.
        seal_states_before = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        csm_total_before = _count_compact_segment_rows(
            catalog_uuid, branch_uuid=branch_uuid, table_uuid=table_uuid
        )

        # Re-compact with no new persists — must be a no-op (no new file,
        # no row UPDATEs, no new compact_segment_metadata rows).
        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert list(response.merged_object_uris) == []

        seal_states_after = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        csm_total_after = _count_compact_segment_rows(
            catalog_uuid, branch_uuid=branch_uuid, table_uuid=table_uuid
        )
        assert seal_states_after == seal_states_before
        assert csm_total_after == csm_total_before

    # Serial for reason (b) — see the `serial` marker in pyproject.toml.
    # Heavy compaction against the pinned 2s QUERY_TIMEOUT_SECONDS: this
    # timed out under more workers than cores. Cheap to serialize, and a
    # queue flake costs a failed merge that only the queue can surface.
    @pytest.mark.serial
    def test_cascade_seal_when_active_full_and_next_uncompacted_breaches(self):
        """Stall regression: with an active at ``max_segment_bytes`` and
        a near-max uncompacted segment leading input order, the wave
        must commit a state-change (cascade-seal active + the unwritable
        accumulator) instead of returning ``None`` and looping forever.

        Setup forces every unsealed row to ``max_segment_bytes``: the
        active alone breaches, and the first uncompacted as a new
        active-seed plus the second uncompacted breaches again. The
        original algorithm bailed (None) at the second breach; the
        cascade-seal algorithm seals the active + the first uncompacted
        and leaves the second for the next wave.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "compact_cascade_seal",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Wave 1: two persists → fresh extend into an active.
        for name, value in [("alice", 10), ("bob", 20)]:
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                {"name": [name], "value": [value]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Two more persists → 2 fresh uncompacted segments after the
        # active. Spoof every unsealed row (active + the 2 uncompacted)
        # to ``max_segment_bytes`` so the active alone is full and each
        # uncompacted alone is also full — every fold breaches.
        for name, value in [("carol", 30), ("dave", 40)]:
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                {"name": [name], "value": [value]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        _spoof_unsealed_persist_segment_sizes(
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "upsert_log",
            size_bytes=MAX_SEGMENT_BYTES,
        )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # 4 rows total. Cascade-seal sealed the prior active (alice, bob)
        # AND the first uncompacted-that-can't-extend (carol); the second
        # (dave) is left unsealed for a future wave.
        post = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(post) == 4
        sealed = [(u, s) for (u, s) in post if s]
        unsealed = [(u, s) for (u, s) in post if not s]
        assert len(sealed) == 3
        assert len(unsealed) == 1
        # The 3 sealed rows occupy 2 distinct files (the prior active
        # merged file + carol's standalone file).
        assert len({u for (u, _) in sealed}) == 2

        # A second compact wave with the same state is a true no-op:
        # only dave is unsealed and a 1-input "merge" is a no-op.
        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert list(response.merged_object_uris) == []

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 4
        names = result.column("name").to_pylist()
        assert {"alice", "bob", "carol", "dave"} <= set(names)

    def test_standalone_seal_when_oversized_segment_leads(self):
        """Stall regression: an uncompacted segment larger than
        ``max_segment_bytes`` leading input order must be sealed in
        place (it's already at one merged file's worth of bytes); the
        loop must continue past it so the next two foldable uncompacted
        segments still merge into a new active.

        The original algorithm broke at the first oversized segment,
        leaving downstream foldable segments unreachable. The fix
        standalone-seals the oversized lead and continues.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "compact_standalone_seal",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # First persist → 1 unsealed segment. Spoof IT to oversized
        # (2× max_segment_bytes) BEFORE adding the next persists; the
        # spoof helper hits every unsealed row, so post-spoof additions
        # keep their natural (small) sizes.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice"], "value": [10]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _spoof_unsealed_persist_segment_sizes(
            catalog_uuid,
            branch_uuid,
            table_uuid,
            "upsert_log",
            size_bytes=2 * MAX_SEGMENT_BYTES,
        )
        for name, value in [("bob", 20), ("carol", 30)]:
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                {"name": [name], "value": [value]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # 3 rows total. alice is sealed-in-place; bob + carol merged
        # into a single active.
        post = _select_persist_segment_seal_states(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(post) == 3
        sealed = [(u, s) for (u, s) in post if s]
        unsealed = [(u, s) for (u, s) in post if not s]
        assert len(sealed) == 1
        assert len(unsealed) == 2
        # bob + carol now share one merged URI; alice keeps its own.
        assert len({u for (u, _) in unsealed}) == 1
        assert sealed[0][0] != unsealed[0][0]

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == 3
        names = result.column("name").to_pylist()
        assert {"alice", "bob", "carol"} <= set(names)


# CHA-202: compact_segment_metadata in-flight tracking
#
# The `compact_segment_metadata` table tracks merged compact files: a
# row INSERTs (commit_micros NULL) before the merged file is
# written, then UPDATEs to a non-NULL `commit_micros` inside the
# same tx that repoints the input `table_persist_segment_metadata` rows.
# Concurrent-compact safety comes from `SELECT FOR UPDATE` on the
# segment rows themselves; this table exists so a future orphan-cleanup
# routine can find merged files left behind by crashed/rolled-back
# compacts (NULL rows after the dust settles).


def _compact_segment_table(catalog_uuid):
    return f"{catalog_uuid}_{COMPACT_SEGMENT_METADATA}"


def _count_compact_segment_rows(
    catalog_uuid,
    *,
    branch_uuid=None,
    table_uuid=None,
    uncommitted_only=False,
):
    """Count compact_segment_metadata rows for a catalog, optionally
    filtered by scope and commit state."""
    tbl = _compact_segment_table(catalog_uuid)
    conditions = []
    params: list = []
    if branch_uuid is not None:
        conditions.append("branch_uuid = %s")
        params.append(branch_uuid)

    if table_uuid is not None:
        conditions.append("table_uuid = %s")
        params.append(table_uuid)

    if uncommitted_only:
        conditions.append("commit_micros IS NULL")

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    rows = get_pg_driver().execute(
        SQL("SELECT count(*) FROM {tbl}" + where).format(tbl=Identifier(tbl)),
        tuple(params),
    )
    return rows[0][0]


def _select_segment_commit_micros(catalog_uuid, branch_uuid, table_uuid):
    """Return all `commit_micros` values on
    `table_persist_segment_metadata` for a `(branch, table)`. Used by the
    visibility-window regression test."""
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT commit_micros FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(seg_parent)),
        (branch_uuid, table_uuid),
    )
    return [r[0] for r in rows]


class TestCompactSegmentMetadata:
    """CHA-202: post-compact `compact_segment_metadata` shape + the
    visibility-window invariant on `table_persist_segment_metadata`."""

    def test_compact_records_each_merged_file_in_compact_segment_metadata(self):
        """A successful compact leaves exactly one committed
        `compact_segment_metadata` row per merged file, scoped to the
        current `(branch, table)`.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "csm_records",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Two persists → two upsert segments, ripe for compaction.
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice"], "value": [10]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["bob"], "value": [20]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        assert (
            _count_compact_segment_rows(
                catalog_uuid, branch_uuid=branch_uuid, table_uuid=table_uuid
            )
            == 0
        )

        response = client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        assert len(response.merged_object_uris) >= 1

        in_scope_total = _count_compact_segment_rows(
            catalog_uuid, branch_uuid=branch_uuid, table_uuid=table_uuid
        )
        in_scope_null = _count_compact_segment_rows(
            catalog_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
            uncommitted_only=True,
        )
        assert in_scope_total >= 1
        assert in_scope_null == 0

    def test_segment_committed_at_never_observed_null_around_compact(self):
        """Visibility-window regression: `table_persist_segment_metadata.
        commit_micros` is never NULL at any pre/inter/post
        observation around persist + compact. The CHA-202 design moves
        orphan tracking off `table_persist_segment_metadata`, so the
        per-segment commit gate never goes NULL during compact.
        """
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "csm_visibility",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        # Pre-persist: no segment rows yet — vacuously holds.
        assert all(
            v is not None
            for v in _select_segment_commit_micros(
                catalog_uuid, branch_uuid, table_uuid
            )
        )

        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice"], "value": [10]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["bob"], "value": [20]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        post_persist = _select_segment_commit_micros(
            catalog_uuid, branch_uuid, table_uuid
        )
        assert len(post_persist) >= 2
        assert all(v is not None for v in post_persist)

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        # Post-compact snapshot — same row count (snapshot-segment
        # pattern keeps every metadata row), every committed_at still
        # non-null.
        post_compact = _select_segment_commit_micros(
            catalog_uuid, branch_uuid, table_uuid
        )
        assert len(post_compact) == len(post_persist)
        assert all(v is not None for v in post_compact)


class TestSnapshot:
    def test_snapshot_basic(self):
        """Insert data, persist to cold, then snapshot."""
        client = make_client()
        ctx = setup_with_data(client)

        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )

        response = client.snapshot(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
        )

        assert response.HasField("snapshotted_at_micros")
        assert response.snapshotted_at_micros > 0

        result = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_name="write_table",
        )
        assert result.num_rows == 2

    def test_snapshot_then_read(self):
        """After snapshot, read_data returns the same rows."""
        client = make_client()
        ctx = setup_with_data(client)

        before_table = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_name="write_table",
        )

        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        client.snapshot(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
        )

        after_table = client.read_data(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_name="write_table",
        )

        assert before_table.sort_by("name").equals(after_table.sort_by("name"))

    def test_snapshot_resolves_deletes(self):
        """Deleted rows are excluded from the snapshot."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main_uuid
        )
        batch = pa.table(
            {"name": ["alice", "bob"], "value": [10, 20]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main_uuid
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        response = client.snapshot(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )

        assert response.HasField("snapshotted_at_micros")

        result = client.read_data(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_name="write_table"
        )
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["bob"]

    def test_snapshot_deduplicates(self):
        """Multiple versions of the same row collapse to the latest."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main_uuid
        )
        batch = pa.table(
            {"name": ["alice"], "value": [10]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx2 = client.begin_tx(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=main_uuid
        )
        update_batch = pa.table(
            {"name": ["alice"], "value": [99]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx2.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=update_batch,
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.commit_tx(
            tx2.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        response = client.snapshot(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_uuid=table_uuid
        )

        assert response.HasField("snapshotted_at_micros")

        result = client.read_data(
            catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, table_name="write_table"
        )
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [99]

    def test_snapshot_metadata_committed(self):
        """Verify both snapshot_metadata and segment rows are committed."""
        client = make_client()
        ctx = setup_with_data(client)

        client.persist(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        response = client.snapshot(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        assert response.HasField("snapshotted_at_micros")

        # CHA-228: snap_uuid is deterministic on
        # (catalog, branch, table, snapshotted_at_micros). The response
        # only carries the watermark; the test derives the snap_uuid
        # to find the row.
        catalog_uuid = ctx["catalog_uuid"]
        snap_uuid = table_snapshot_uuid(
            catalog_uuid,
            ctx["main_branch_uuid"],
            ctx["table_uuid"],
            response.snapshotted_at_micros,
        )

        # CHA-198: per-catalog parent.
        snap_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
        snap_seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
        snap_rows = get_pg_driver().execute(
            SQL(
                "SELECT snapshotted_at_micros, commit_micros,"
                " partition_keys, clustering_keys"
                " FROM {tbl} WHERE table_snapshot_uuid = %s"
            ).format(tbl=Identifier(snap_parent)),
            (snap_uuid,),
        )
        assert len(snap_rows) == 1
        assert snap_rows[0][0] == response.snapshotted_at_micros
        assert snap_rows[0][1] is not None  # commit_micros
        assert snap_rows[0][1] > 0

        # CHA-404: the parent row records the write-time layout keys.
        # This table declares no partition or clustering keys, so the
        # effective clustering keys are the primary keys (the common
        # SQL-DDL case) and partition_keys is the EMPTY array — `{}`
        # (known: no keys) must stay distinguishable from NULL
        # (pre-CHA-404 row, unknown); CHA-406's key-change detection
        # relies on the distinction.
        partition_keys, clustering_keys = snap_rows[0][2], snap_rows[0][3]
        assert partition_keys == [], partition_keys
        assert clustering_keys == ["name"], clustering_keys

        seg_rows = get_pg_driver().execute(
            SQL(
                "SELECT commit_micros, row_count"
                " FROM {tbl} WHERE table_snapshot_uuid = %s"
            ).format(tbl=Identifier(snap_seg_parent)),
            (snap_uuid,),
        )
        assert len(seg_rows) > 0
        for committed_at, row_count in seg_rows:
            assert committed_at is not None
            assert committed_at > 0
            assert row_count > 0


# CHA-406 — delta-partition carry-forward onto immutable segments


def _select_snapshot_segment_storage_tuples(catalog_uuid, branch_uuid, snapshot_uuid):
    """Return ``[(object_uri, offset, length, row_count), ...]`` for one
    snapshot's committed segment rows (pattern:
    :func:`_select_snapshot_segment_row_counts`).

    The storage tuple is carry-forward's sharing identity: an untouched
    partition is carried by REFERENCE — a new
    ``table_snapshot_segment_uuid`` under the new snapshot pointing at
    the prior file verbatim (same ``object_uri`` + ``offset`` +
    ``length``). A rewritten partition can never share a tuple, because
    snapshot file uris embed the writing snapshot's uuid.
    """
    seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            'SELECT seg.object_uri, seg."offset", seg.length, seg.row_count'
            " FROM {seg} seg"
            " WHERE seg.branch_uuid = %s"
            "   AND seg.table_snapshot_uuid = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent)),
        (branch_uuid, snapshot_uuid),
    )
    return [(r[0], r[1], r[2], r[3]) for r in rows]


class TestCarryForwardSnapshot:
    """CHA-406 red tests: incremental snapshot v1b — untouched
    partitions carried onto the new snapshot by reference instead of
    being rewritten (ADR 0024 §3, algorithm steps 4–7).

    The CHA-406 carry-forward tests are committed RED: today every
    snapshot fully rewrites every partition into fresh files (CHA-404),
    so consecutive snapshots never share a storage tuple; the
    ineligible/fallback tests pin the full-rewrite paths and are green
    before and after. The two ``test_non_subset_*`` tests (CHA-448 v2)
    are also committed RED — they assert the partition ⊄ PK case engages
    carry-forward, the inverse of
    ``test_partition_key_not_subset_of_pk_full_rewrite``, which the
    CHA-448 v2 implementation retires.

    Partition-label ↔ tuple attribution note: the segment table stores
    no partition label (CHA-406 derives labels from ``statistics``), so
    these tests identify partitions positionally — small partitions
    pack into ONE file in label order (``small_partitions_share_one_file``
    in ``snapshot_op.rs``), so within cycle 1's single file the offset
    IS the label rank (alice=0, bob=1, carol=2).

    Coverage scope: every partition here holds one row, so two
    behaviors are pinned at the Rust streaming-unit layer rather than
    here — (a) merge correctness for a *partially* touched multi-row
    partition (prior rows merged with the delta, not replaced), covered
    by ``content_equivalence_red_tests``; (b) carrying a multi-chunk
    oversized partition by reference, covered by the packer's
    carried-interleave tests. The single-row constraint is what makes
    the offset-as-rank attribution above sound.
    """

    def _setup(self, *, partition_keys, primary_keys):
        client = make_client()
        catalog_uuid, main_branch_uuid = client.create_catalog(
            f"cf_cat_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "cf_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="carry_forward",
        )
        table_uuid = client.create_table(
            "cf_table",
            USER_SCHEMA,
            primary_keys=primary_keys,
            partition_keys=partition_keys,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="carry_forward",
        )
        return client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid

    def _cycle(self, env, *, upserts=None, deletes=None):
        """One write cycle: mutate → commit → persist → snapshot.

        Returns ``(snap_uuid, snapshotted_at_micros)``.
        """
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=upserts, deletes=deletes),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert response.HasField("snapshotted_at_micros")
        snap_uuid = table_snapshot_uuid(
            catalog_uuid, branch_uuid, table_uuid, response.snapshotted_at_micros
        )
        return snap_uuid, response.snapshotted_at_micros

    def _tuples(self, env, snap_uuid):
        _, catalog_uuid, _, _, branch_uuid = env
        return set(
            _select_snapshot_segment_storage_tuples(
                catalog_uuid, branch_uuid, snap_uuid
            )
        )

    @staticmethod
    def _assert_single_file_label_ranks(tuples1):
        """Pin the offset-is-label-rank premise the carried/rewritten
        attribution relies on: cycle 1's three tiny partitions pack into
        ONE file (one object_uri) at offsets 0/1/2 (alice/bob/carol).
        Asserted where it's relied on, not just in the Rust unit test —
        if packing ever drifts to three separate files (every offset 0),
        the offset-keyed `expected_carried`/`expected` sets below would
        silently invert."""
        assert len({t[0] for t in tuples1}) == 1, (
            f"three tiny partitions must share one packed file: {sorted(tuples1)}"
        )
        assert {t[1] for t in tuples1} == {0, 1, 2}, (
            f"offsets must be the label ranks 0/1/2: {sorted(tuples1)}"
        )

    def test_carry_forward_untouched_partitions_share_prior_files(self):
        """Cycle 2 touches only partition alice: bob's and carol's
        snapshot-1 tuples must reappear VERBATIM under snapshot 2
        (carried by reference), alice must land in a fresh file, and
        the read path — which needs no change — returns the merged
        content."""
        env = self._setup(partition_keys=["name"], primary_keys=["name"])
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env

        snap1, _ = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        self._assert_single_file_label_ranks(tuples1)

        snap2, _ = self._cycle(
            env,
            upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )
        tuples2 = self._tuples(env, snap2)

        # (i) Untouched partitions carried by reference: bob and carol
        # are every snapshot-1 tuple except alice's (offset 0 — label
        # rank, see class docstring).
        expected_carried = {t for t in tuples1 if t[1] != 0}
        carried = tuples1 & tuples2
        assert carried == expected_carried, (
            "expected carried segment rows sharing prior uris (bob, carol),"
            f" got {sorted(carried)};"
            f" snapshot 1 {sorted(tuples1)} vs snapshot 2 {sorted(tuples2)}"
        )

        # (ii) The touched partition is rewritten into a fresh file.
        uris1 = {t[0] for t in tuples1}
        assert any(t[0] not in uris1 for t in tuples2), (
            f"touched partition alice must land in a new file: {sorted(tuples2)}"
        )

        # (iii) Content correctness through the unchanged read path.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        by_name = result.sort_by("name")
        assert by_name.column("name").to_pylist() == ["alice", "bob", "carol"]
        assert by_name.column("value").to_pylist() == [99, 2, 3]

    def test_delete_only_partition_is_rewritten_others_carried(self):
        """Cycle 2 issues ONLY deletes covering every row of partition
        bob: the touched set must include bob purely from the cold
        delete-log's PK columns (the delete-attribution frontier).
        Snapshot 2 is then exactly snapshot 1's alice and carol tuples
        — bob emitted nothing — and reads return no bob rows."""
        env = self._setup(partition_keys=["name"], primary_keys=["name"])
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env

        snap1, _ = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        self._assert_single_file_label_ranks(tuples1)

        snap2, _ = self._cycle(
            env,
            deletes=pa.table({"name": ["bob"]}, schema=_PK_SCHEMA_NAME),
        )
        tuples2 = self._tuples(env, snap2)

        # alice (offset 0) and carol (offset 2) carried verbatim; the
        # fully-deleted bob partition (offset 1) emits nothing.
        expected = {t for t in tuples1 if t[1] != 1}
        assert tuples2 == expected, (
            "expected carried segment rows sharing prior uris (alice, carol)"
            f" and nothing else, got {sorted(tuples2)};"
            f" snapshot 1 was {sorted(tuples1)}"
        )

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        by_name = result.sort_by("name")
        assert by_name.column("name").to_pylist() == ["alice", "carol"]
        assert by_name.column("value").to_pylist() == [1, 3]

    def test_key_change_forces_full_rewrite(self):
        """A recorded-layout-keys mismatch (ADR 0024 invariant) forces
        a full rewrite: after hand-flipping the prior snapshot's
        recorded ``clustering_keys``, the next snapshot shares ZERO
        tuples — every partition rewritten."""
        env = self._setup(partition_keys=["name"], primary_keys=["name"])
        _, catalog_uuid, _, _, branch_uuid = env

        snap1, _ = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        snap2, _ = self._cycle(
            env,
            upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )
        tuples2 = self._tuples(env, snap2)
        # Positive control (RED today): the eligible cycle carried rows.
        assert tuples1 & tuples2, (
            "positive control: the eligible cycle 2 must carry untouched"
            f" partitions; snapshot 1 {sorted(tuples1)} vs snapshot 2"
            f" {sorted(tuples2)}"
        )

        # Flip the recorded clustering keys on the latest snapshot's
        # parent row (direct-SQL pattern as in
        # integration_snapshot_gc_test.py) — the next cycle's
        # key-change detection must see recorded ≠ current.
        snap_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
        get_pg_driver().execute_no_result(
            SQL(
                "UPDATE {tbl} SET clustering_keys = %s"
                " WHERE branch_uuid = %s AND table_snapshot_uuid = %s"
            ).format(tbl=Identifier(snap_parent)),
            (["value"], branch_uuid, snap2),
        )

        snap3, _ = self._cycle(
            env,
            upserts=pa.table({"name": ["alice"], "value": [100]}, schema=USER_SCHEMA),
        )
        tuples3 = self._tuples(env, snap3)
        # Non-emptiness guard: `not (tuples3 & tuples2)` passes vacuously
        # if a buggy full-rewrite emitted only the zero-row watermark
        # placeholder. Pin that all three rows were actually rewritten.
        assert sum(t[3] for t in tuples3) == 3, (
            f"forced full rewrite must emit all three live rows; got {sorted(tuples3)}"
        )
        assert not (tuples3 & tuples2), (
            "layout-key mismatch must force a full rewrite (zero shared"
            f" tuples); shared: {sorted(tuples3 & tuples2)}"
        )
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env
        by_name = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        ).sort_by("name")
        assert by_name.column("name").to_pylist() == ["alice", "bob", "carol"]
        assert by_name.column("value").to_pylist() == [100, 2, 3]

    def test_non_subset_delete_only_carries_untouched(self):
        """CHA-448 v2: partition_keys=["value"] ⊄ primary_keys=["name"].
        A delete-only cycle removing every row of the value=2 partition
        must engage carry-forward — value=1 and value=3 carried by
        reference — even though the partition column `value` is absent
        from the cold delete-log (it carries only PK `name`). The touched
        partition is attributed from the row_uuid → prior-partition
        reverse lookup, not the delete-log's partition column.

        RED pre-CHA-448: the partition ⊄ PK gate forces the CHA-404 full
        rewrite, so snap2 shares zero storage tuples with snap1 and the
        carried-by-reference assertion fails."""
        env = self._setup(partition_keys=["value"], primary_keys=["name"])
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env

        snap1, _ = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        # Typed int64 partition order 1<2<3 packs into one file at offsets
        # 0/1/2 (value rank) — the single-row-per-partition premise the
        # subset tests above rely on holds for int partition keys too.
        self._assert_single_file_label_ranks(tuples1)

        snap2, _ = self._cycle(
            env,
            deletes=pa.table({"name": ["bob"]}, schema=_PK_SCHEMA_NAME),
        )
        tuples2 = self._tuples(env, snap2)

        # value=1 (alice@offset0) and value=3 (carol@offset2) carried
        # verbatim; the fully-deleted value=2 partition (offset 1) emits
        # nothing.
        expected = {t for t in tuples1 if t[1] != 1}
        assert tuples2 == expected, (
            "non-subset delete-only must carry untouched value partitions"
            " by reference (alice, carol) and nothing else;"
            f" got {sorted(tuples2)}, snapshot 1 was {sorted(tuples1)}"
        )

        by_name = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        ).sort_by("name")
        assert by_name.column("name").to_pylist() == ["alice", "carol"]
        assert by_name.column("value").to_pylist() == [1, 3]

    def test_non_subset_partition_move_carries_untouched_no_dup(self):
        """CHA-448 v2 critical subtlety: a partition-key-value update
        moves a row across partitions with the SAME row_uuid and emits
        ONLY an upsert (no delete, because `value` ⊄ PK). The touched set
        must reverse-look-up the prior partition of the UPSERT's row_uuid
        — not just deletes — or the stale copy in the old partition is
        carried by reference and the row duplicates.

        alice moves value=1 → value=2. Assert: (a) the untouched value=3
        partition is carried by reference; (b) alice appears exactly once
        (value=2); (c) the old value=1 partition is rewritten (its stale
        row excluded), not carried.

        RED pre-CHA-448: partition ⊄ PK forces a full rewrite, so the
        untouched value=3 partition is not carried (assertion a fails).
        Post-impl, (b)+(c) additionally guard the upsert-attribution: an
        impl that reverse-looks-up only deletes leaves alice's stale
        value=1 copy carried → a duplicate row."""
        env = self._setup(partition_keys=["value"], primary_keys=["name"])
        client, catalog_uuid, schema_uuid, table_uuid, branch_uuid = env

        snap1, _ = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob", "carol"], "value": [1, 2, 3]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        self._assert_single_file_label_ranks(tuples1)

        # Move alice from partition value=1 to value=2 (same PK `name`, so
        # same row_uuid; upsert only, no delete emitted).
        snap2, _ = self._cycle(
            env,
            upserts=pa.table({"name": ["alice"], "value": [2]}, schema=USER_SCHEMA),
        )
        tuples2 = self._tuples(env, snap2)

        # (a) value=3 (carol@offset2) is the only untouched partition —
        # carried by reference.
        carol_tuple = {t for t in tuples1 if t[1] == 2}
        assert carol_tuple <= tuples2, (
            "untouched value=3 partition (carol) must be carried by"
            f" reference; snap1 {sorted(tuples1)} vs snap2 {sorted(tuples2)}"
        )

        # (c) the OLD value=1 partition (alice@offset0) is rewritten, not
        # carried — its prior tuple must NOT reappear (the reverse lookup
        # put value=1 in the touched set).
        old_alice_tuple = {t for t in tuples1 if t[1] == 0}
        assert not (old_alice_tuple & tuples2), (
            "old value=1 partition must be rewritten via reverse-lookup"
            f" attribution, not carried; shared {sorted(old_alice_tuple & tuples2)}"
        )

        # (b) alice appears exactly once, now in value=2 — no stale copy
        # left behind in value=1.
        by_name = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        ).sort_by("name")
        assert by_name.column("name").to_pylist() == ["alice", "bob", "carol"]
        assert by_name.column("value").to_pylist() == [2, 2, 3], (
            "alice must appear once at value=2 (moved); a duplicate stale"
            " alice@value=1 means the upsert reverse-lookup is missing:"
            f" {by_name.column('value').to_pylist()}"
        )

    def test_unpartitioned_table_full_path(self):
        """No partition keys: carry-forward gives no benefit (one
        partition, always touched) — CHA-404 behavior unchanged, zero
        shared tuples, watermark advances. Green today — regression
        guard."""
        env = self._setup(partition_keys=[], primary_keys=["name"])

        snap1, micros1 = self._cycle(
            env,
            upserts=pa.table(
                {"name": ["alice", "bob"], "value": [1, 2]},
                schema=USER_SCHEMA,
            ),
        )
        tuples1 = self._tuples(env, snap1)
        assert tuples1

        snap2, micros2 = self._cycle(
            env,
            upserts=pa.table({"name": ["alice"], "value": [99]}, schema=USER_SCHEMA),
        )
        tuples2 = self._tuples(env, snap2)
        assert tuples2
        assert micros2 > micros1, "watermark must advance across cycles"
        assert not (tuples1 & tuples2), (
            f"unpartitioned table never carries; shared: {sorted(tuples1 & tuples2)}"
        )


# CHA-215 — persist + snapshot chunk writes to cap segment size
#
# Pre-CHA-215, persist and snapshot each emitted exactly one cold
# segment per ``(table_uuid, log_kind)`` (persist) / per cycle (snapshot),
# capped only by client input volume. A 10 GiB persist wrote a 10 GiB
# segment, which (a) breaks cold-reader memory math (a segment is
# materialized into a single Arrow batch on read) and (b) stalls
# ``plan_wave`` compact since an oversized lead can't extend or seat a
# new active.
#
# Post-CHA-215, the in-memory ``RecordBatch`` is chunked at write time
# so every emitted segment has a standalone size
# ``<= max_segment_bytes``; chunks land as sibling
# ``table_persist_segment_metadata`` / ``table_snapshot_segment_metadata``
# rows under the same parent ``table_persist_uuid`` /
# ``table_snapshot_uuid``.
#
# These tests run under the 1 MiB cap from ``docker/test.env``
# (override of the 64 MiB compose default), so ~2 MiB of synthetic
# data exercises the chunker without writing 128 MiB.


# Per-row in-memory Arrow size of a USER_SCHEMA row with a 16-char
# ``name``: 16-byte utf8 payload + 4-byte offset + 8-byte int64 +
# 2 × 0.125-byte validity slots ≈ 28.25 bytes. CHA-215's chunker walks
# rows with the same column-typed math, so deriving (a) the breach-cap
# row count and (b) the per-chunk in-memory-byte upper bound from this
# constant matches what the production code measures.
#
# Why row_count here: this CHA-215 suite pins chunk *boundaries* via
# the row-count invariant, independent of the recorded byte figure.
# ``max_segment_bytes`` is a memory-safety bound on the uncompressed
# Arrow batch a cold reader re-materializes from a segment; the chunker
# measures *that* quantity, and ``row_count × _PER_ROW_BYTES``
# reconstructs it as a loose upper bound. Post-CHA-347 ``size_bytes`` is
# itself the uncompressed in-memory footprint (no longer the on-disk
# file size), so it can be asserted directly — see
# ``TestSegmentSizeBytesIsInMemoryFootprint`` and
# ``_select_persist_segment_sizes``.
_PER_ROW_BYTES = 28


def _rows_to_exceed_bytes(target_bytes: int) -> dict:
    """Build a USER_SCHEMA rows dict whose in-memory Arrow size
    comfortably exceeds ``target_bytes``. Pads the row count by 50% so
    minor measurement differences can't push the result below the
    target."""
    n = (target_bytes // _PER_ROW_BYTES) * 3 // 2 + 1
    return {
        "name": [f"user_{i:010d}" for i in range(n)],
        "value": list(range(n)),
    }


def _select_persist_segment_row_counts(catalog_uuid, branch_uuid, table_uuid, log_kind):
    """Return ``[(segment_uuid, parent_table_persist_uuid, row_count), ...]``
    for every committed persist-segment row in ``(branch, table,
    log_kind)``. ``row_count`` is the chunk-boundary invariant the
    CHA-215 chunker enforces against ``max_segment_bytes``; post-CHA-347
    ``size_bytes`` carries the same uncompressed footprint — use
    ``_select_persist_segment_sizes`` to assert on it directly."""
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT seg.table_persist_segment_uuid,"
            "       seg.table_persist_uuid,"
            "       seg.row_count"
            " FROM {seg} seg"
            " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
            "                       AND seg.branch_uuid = tfm.branch_uuid"
            " WHERE tfm.branch_uuid = %s"
            "   AND tfm.table_uuid = %s"
            "   AND tfm.log_kind = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (branch_uuid, table_uuid, log_kind),
    )
    return [(r[0], r[1], r[2]) for r in rows]


def _select_snapshot_segment_row_counts(catalog_uuid, branch_uuid, table_uuid):
    """Snapshot-side mirror of
    :func:`_select_persist_segment_row_counts`."""
    seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    snap_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT seg.table_snapshot_segment_uuid,"
            "       seg.table_snapshot_uuid,"
            "       seg.row_count"
            " FROM {seg} seg"
            " INNER JOIN {snap} snap"
            "   ON seg.table_snapshot_uuid = snap.table_snapshot_uuid"
            "  AND seg.branch_uuid = snap.branch_uuid"
            " WHERE snap.branch_uuid = %s"
            "   AND snap.table_uuid = %s"
            "   AND seg.commit_micros IS NOT NULL"
            "   AND snap.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), snap=Identifier(snap_parent)),
        (branch_uuid, table_uuid),
    )
    return [(r[0], r[1], r[2]) for r in rows]


def _assert_chunk_in_memory_size_within_cap(seg_rows):
    """Assert every chunk's ``row_count × _PER_ROW_BYTES`` is at most
    ``MAX_SEGMENT_BYTES`` plus a single-row tolerance. The chunker
    emits a chunk only when the *next* row would breach, so the last
    accepted row can land at exactly the cap; allowing one row of
    slack covers that boundary case."""
    tolerance = _PER_ROW_BYTES
    for seg_uuid, _, row_count in seg_rows:
        estimated_bytes = row_count * _PER_ROW_BYTES
        assert estimated_bytes <= MAX_SEGMENT_BYTES + tolerance, (
            f"chunk {seg_uuid} has row_count {row_count} →"
            f" ~{estimated_bytes}B in memory, exceeds MAX_SEGMENT_BYTES"
            f" {MAX_SEGMENT_BYTES}"
        )


class TestChunkedPersistAndSnapshot:
    """CHA-215 acceptance: persist and snapshot chunk their RecordBatch
    so no emitted cold segment exceeds ``max_segment_bytes`` (1 MiB
    under integration tests via ``docker/test.env``); the chunks are
    sibling segment-metadata rows under one parent persist / snapshot
    uuid; and ``compact_persist_segments`` over the resulting set never
    stalls."""

    def test_persist_chunks_oversized_upsert_input(self):
        """One tx writes > 2 × ``MAX_SEGMENT_BYTES`` of in-memory data.
        Persist emits >= 2 sibling ``table_persist_segment_metadata`` rows
        under one ``table_persist_uuid``; each chunk's in-memory size
        (``row_count × _PER_ROW_BYTES``) respects the cap; chunk
        segment_uuids are distinct; sum of chunk row_counts equals the
        input row count."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        rows = _rows_to_exceed_bytes(2 * MAX_SEGMENT_BYTES)
        expected_row_count = len(rows["name"])
        _insert_and_commit(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, rows
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        seg_rows = _select_persist_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(seg_rows) >= 2, (
            f"oversized persist should emit >= 2 chunk segments; got {len(seg_rows)}"
        )
        assert len({tfu for (_, tfu, _) in seg_rows}) == 1, (
            "all chunks must hang off one parent table_persist_uuid"
        )
        _assert_chunk_in_memory_size_within_cap(seg_rows)
        seg_uuids = [seg_uuid for (seg_uuid, _, _) in seg_rows]
        assert len(seg_uuids) == len(set(seg_uuids))
        assert sum(rc for (_, _, rc) in seg_rows) == expected_row_count, (
            "chunk row_counts must sum to the input row count"
        )

    # Serial for reason (b) — see the `serial` marker in pyproject.toml.
    # Heavy compaction against the pinned 2s QUERY_TIMEOUT_SECONDS: this
    # timed out under more workers than cores. Cheap to serialize, and a
    # queue flake costs a failed merge that only the queue can surface.
    @pytest.mark.serial
    def test_persist_chunked_segments_compact_normally(self):
        """``compact_persist_segments`` over the chunk siblings produced
        by an oversized persist must NOT stall: the call returns, every
        chunk row's in-memory size still respects the cap (compact may
        merge below-cap chunks or standalone-seal at-cap chunks; either
        is legal — what's illegal is a breach or a stall), and the data
        remains queryable end-to-end."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "chunked_persist_compact",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        create_table_on_branch(client, catalog_uuid, schema_uuid, branch_uuid)

        rows = _rows_to_exceed_bytes(2 * MAX_SEGMENT_BYTES)
        expected_row_count = len(rows["name"])
        _insert_and_commit(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, rows
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        pre = _select_persist_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(pre) >= 2

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        post = _select_persist_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(post) == len(pre)
        _assert_chunk_in_memory_size_within_cap(post)

        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )
        assert result.num_rows == expected_row_count

    def test_snapshot_chunks_oversized_output(self):
        """Snapshot over > 2 × ``MAX_SEGMENT_BYTES`` of persisted data
        emits >= 2 sibling ``table_snapshot_segment_metadata`` rows
        under one ``table_snapshot_uuid``; each chunk's in-memory size
        respects the cap; chunk segment_uuids are distinct; sum of
        chunk row_counts equals the input row count."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        rows = _rows_to_exceed_bytes(2 * MAX_SEGMENT_BYTES)
        expected_row_count = len(rows["name"])
        _insert_and_commit(
            client, catalog_uuid, schema_uuid, table_uuid, branch_uuid, rows
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        seg_rows = _select_snapshot_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid
        )
        assert len(seg_rows) >= 2, (
            f"oversized snapshot should emit >= 2 chunk segments; got {len(seg_rows)}"
        )
        assert len({tsu for (_, tsu, _) in seg_rows}) == 1, (
            "all chunks must hang off one parent table_snapshot_uuid"
        )
        _assert_chunk_in_memory_size_within_cap(seg_rows)
        seg_uuids = [seg_uuid for (seg_uuid, _, _) in seg_rows]
        assert len(seg_uuids) == len(set(seg_uuids))
        assert sum(rc for (_, _, rc) in seg_rows) == expected_row_count

    def test_persist_below_cap_is_single_segment(self):
        """Control: a persist whose batch is well below
        ``MAX_SEGMENT_BYTES`` still emits exactly one segment per
        ``(table, log_kind)``. The chunker must be a no-op on small
        inputs — ``chunk_idx = 0`` only."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            {"name": ["alice", "bob"], "value": [10, 20]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        seg_rows = _select_persist_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(seg_rows) == 1, (
            f"small persist should emit exactly one segment; got {len(seg_rows)}"
        )
        _, _, row_count = seg_rows[0]
        assert row_count == 2

    def test_persist_oversized_single_tx_all_same_committed_at(self):
        """Edge case proving ``chunk_idx`` is load-bearing in the
        segment-UUID hash: one tx writes enough rows to require
        chunking; every row therefore shares the same
        ``commit_micros``. Per-chunk ``(min, max)`` collapses to
        one pair across every chunk — so distinct
        ``table_persist_segment_uuid`` values can only come from the
        ``chunk_idx`` term."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            _rows_to_exceed_bytes(2 * MAX_SEGMENT_BYTES),
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        seg_rows = _select_persist_segment_row_counts(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(seg_rows) >= 2
        seg_uuids = [seg_uuid for (seg_uuid, _, _) in seg_rows]
        assert len(seg_uuids) == len(set(seg_uuids)), (
            "chunk segment_uuids must be distinct even when every row"
            " shares one commit_micros"
        )

        # Cross-check: every chunk's (min, max) commit_micros is
        # identical (one tx → one timestamp), so ``(min, max)`` alone
        # would NOT have produced distinct segment uuids.
        seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
        tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
        bounds = get_pg_driver().execute(
            SQL(
                "SELECT DISTINCT seg.min_tx_commit_micros,"
                "       seg.max_tx_commit_micros"
                " FROM {seg} seg"
                " INNER JOIN {tfm} tfm"
                "   ON seg.table_persist_uuid = tfm.table_persist_uuid"
                "  AND seg.branch_uuid = tfm.branch_uuid"
                " WHERE tfm.branch_uuid = %s"
                "   AND tfm.table_uuid = %s"
                "   AND tfm.log_kind = %s"
                "   AND seg.commit_micros IS NOT NULL"
            ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
            (branch_uuid, table_uuid, "upsert_log"),
        )
        assert len(bounds) == 1, (
            f"single-tx persist should leave every chunk with the same"
            f" (min, max) commit_micros pair; got {bounds}"
        )


# CHA-347 — recorded ``size_bytes`` is the in-memory Arrow footprint
#
# ``size_bytes`` on persist/snapshot segment rows must be the standalone
# uncompressed in-memory Arrow footprint — the unit every consumer
# (write-time chunker, compaction fold, merge reader-RAM budget) compares
# against ``max_segment_bytes`` — NOT the on-disk serialized (Lance/parquet)
# file size. The segment a reader re-materializes is the *cold batch*
# (row_uuid + user cols + CHA-218 audit tx-metadata), not the bare user
# schema, so the recorded footprint is derived from that wider schema
# below. Pre-CHA-347 ``size_bytes`` is the on-disk size and fails these
# equalities; post-fix it is the chunker footprint of the cold batch.


# Mirror of the chunker's per-row byte arithmetic
# (crates/penca-api/src/lifecycle/chunker.rs): variable-width columns
# cost ``payload + 4 (offset) + 1 (validity share)``; fixed-width cost
# ``width + 1``. The fixed-width ``user_<10 digits>`` name makes every
# row's footprint deterministic, so recorded ``size_bytes`` is an exact
# multiple of the per-row figure.
def _utf8_row_bytes(char_len: int) -> int:
    return char_len + 4 + 1


_I64_ROW_BYTES = 8 + 1

# ``row_uuid`` is a 36-char UUID rendered as Utf8 (penca-merge
# schema.rs); USER_SCHEMA is ``name`` (15-char Utf8 here) + ``value``
# (Int64).
_ROW_UUID_ROW_BYTES = _utf8_row_bytes(36)
_USER_COLS_ROW_BYTES = _utf8_row_bytes(15) + _I64_ROW_BYTES

# Snapshot segments store ``snapshot_read_schema`` = row_uuid + user
# cols (= 41 + 29 = 70 B/row).
_SNAPSHOT_PER_ROW_BYTES = _ROW_UUID_ROW_BYTES + _USER_COLS_ROW_BYTES

# Persist + compacted segments store the audit ``cold_upsert_schema`` =
# row_uuid + user cols + written_at + committed_at + began_at +
# commit_seq_num. CHA-507 dropped comment/author from cold segments (they
# now live once per tx in the cold tx_log, joined on demand), so the two
# empty-Utf8 (5 B each) terms are gone. CHA-430 appended ``commit_seq_num``
# (i64). (= 41 + 29 + 27 + 9 = 106 B/row.)
_PERSIST_PER_ROW_BYTES = (
    _ROW_UUID_ROW_BYTES
    + _USER_COLS_ROW_BYTES
    + 3 * _I64_ROW_BYTES  # written_at, committed_at, began_at
    + _I64_ROW_BYTES  # commit_seq_num (CHA-430)
)


def _uniform_rows(n: int, start: int = 0) -> dict:
    """USER_SCHEMA rows of fixed per-row width: each ``name`` is the
    15-char ``user_<10 digits>`` literal, each ``value`` an int64. The
    fixed width makes recorded ``size_bytes`` an exact multiple of the
    per-row figures above. ``start`` offsets the key space so successive
    persists produce non-overlapping PKs (distinct rows that survive a
    compaction concat)."""
    return {
        "name": [f"user_{i:010d}" for i in range(start, start + n)],
        "value": list(range(start, start + n)),
    }


def _select_persist_segment_sizes(catalog_uuid, branch_uuid, table_uuid, log_kind):
    """``[(segment_uuid, row_count, size_bytes), ...]`` for every
    committed persist-segment row. CHA-347: ``size_bytes`` is the
    standalone in-memory Arrow footprint."""
    seg_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT seg.table_persist_segment_uuid,"
            "       seg.row_count,"
            "       seg.size_bytes"
            " FROM {seg} seg"
            " INNER JOIN {tfm} tfm ON seg.table_persist_uuid = tfm.table_persist_uuid"
            "                       AND seg.branch_uuid = tfm.branch_uuid"
            " WHERE tfm.branch_uuid = %s"
            "   AND tfm.table_uuid = %s"
            "   AND tfm.log_kind = %s"
            "   AND seg.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), tfm=Identifier(tfm_parent)),
        (branch_uuid, table_uuid, log_kind),
    )
    return [(r[0], r[1], r[2]) for r in rows]


def _select_snapshot_segment_sizes(catalog_uuid, branch_uuid, table_uuid):
    """Snapshot-side mirror of :func:`_select_persist_segment_sizes`."""
    seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    snap_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT seg.table_snapshot_segment_uuid,"
            "       seg.row_count,"
            "       seg.size_bytes"
            " FROM {seg} seg"
            " INNER JOIN {snap} snap"
            "   ON seg.table_snapshot_uuid = snap.table_snapshot_uuid"
            "  AND seg.branch_uuid = snap.branch_uuid"
            " WHERE snap.branch_uuid = %s"
            "   AND snap.table_uuid = %s"
            "   AND seg.commit_micros IS NOT NULL"
            "   AND snap.commit_micros IS NOT NULL"
        ).format(seg=Identifier(seg_parent), snap=Identifier(snap_parent)),
        (branch_uuid, table_uuid),
    )
    return [(r[0], r[1], r[2]) for r in rows]


class TestSegmentSizeBytesIsInMemoryFootprint:
    """CHA-347 acceptance criterion 1: a freshly-written persist /
    snapshot segment records its standalone in-memory Arrow footprint as
    ``size_bytes`` — not the on-disk serialized file size. Fresh writes
    go straight through the chunker, so each segment's ``size_bytes``
    equals ``row_count × <per-row footprint of the segment's cold
    schema>`` exactly (no proportional split). Persist segments store the
    wider audit ``cold_upsert_schema``; snapshot segments store
    ``snapshot_read_schema`` — hence the two distinct per-row constants."""

    def test_persist_segment_size_bytes_is_in_memory_footprint(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            _uniform_rows(200),
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        seg_rows = _select_persist_segment_sizes(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert seg_rows, "persist must emit at least one committed segment"
        for seg_uuid, row_count, size_bytes in seg_rows:
            assert size_bytes == row_count * _PERSIST_PER_ROW_BYTES, (
                f"persist segment {seg_uuid}: recorded size_bytes {size_bytes}"
                f" must equal in-memory footprint"
                f" {row_count * _PERSIST_PER_ROW_BYTES} (row_count {row_count} ×"
                f" {_PERSIST_PER_ROW_BYTES}), not the on-disk Lance file size"
            )

    def test_snapshot_segment_size_bytes_is_in_memory_footprint(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch_uuid = main_branch_uuid

        _insert_and_commit(
            client,
            catalog_uuid,
            schema_uuid,
            table_uuid,
            branch_uuid,
            _uniform_rows(200),
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=branch_uuid,
        )

        seg_rows = _select_snapshot_segment_sizes(catalog_uuid, branch_uuid, table_uuid)
        assert seg_rows, "snapshot must emit at least one committed segment"
        for seg_uuid, row_count, size_bytes in seg_rows:
            assert size_bytes == row_count * _SNAPSHOT_PER_ROW_BYTES, (
                f"snapshot segment {seg_uuid}: recorded size_bytes {size_bytes}"
                f" must equal in-memory footprint"
                f" {row_count * _SNAPSHOT_PER_ROW_BYTES} (row_count {row_count} ×"
                f" {_SNAPSHOT_PER_ROW_BYTES}), not the on-disk Lance file size"
            )


class TestCompactedSegmentSizeBytesIsInMemoryFootprint:
    """CHA-347 acceptance criterion 3 (+ criterion 1 at the
    compaction-re-point site): after compaction, the merged segment's
    re-pointed rows record the in-memory footprint of the merged batch
    (proportionally split by ``row_count``), not the on-disk merged-file
    size, and the merged segment's footprint stays within
    ``max_segment_bytes``.

    ``compact_plan`` is UNCHANGED — it already reads ``size_bytes`` and
    folds while ``current_size + size_bytes <= max_segment_bytes``. The
    corrected cap behaviour follows purely from the corrected stored
    value the fold reads."""

    def test_compacted_segment_size_bytes_is_in_memory_and_within_cap(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        branch = client.create_branch(
            "cha347_compact_size",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_branch",
        )
        branch_uuid = branch.branch_uuid
        # No create_table_on_branch: CreateBranch forks the parent's
        # tables (CHA-184), so the branch already has `table_uuid`.

        # Three small persists with non-overlapping keys → three
        # uncompacted segments, each far below the cap, so one compact
        # wave folds all three into a single merged active.
        per_persist = 100
        for k in range(3):
            _insert_and_commit(
                client,
                catalog_uuid,
                schema_uuid,
                table_uuid,
                branch_uuid,
                _uniform_rows(per_persist, start=k * per_persist),
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=table_uuid,
            )

        pre = _select_persist_segment_sizes(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert len(pre) >= 2, "need >= 2 uncompacted segments to fold"

        client.compact_persist_segments(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=table_uuid,
        )

        post = _select_persist_segment_sizes(
            catalog_uuid, branch_uuid, table_uuid, "upsert_log"
        )
        assert post, "compacted table must still have committed segments"
        total_rows = sum(rc for (_, rc, _) in post)
        total_size = sum(sz for (_, _, sz) in post)

        # Criterion 1 @ compaction (the load-bearing red→green signal):
        # the re-pointed sizes track the in-memory footprint of the
        # merged cold_upsert batch, not the on-disk merged-file size. The
        # proportional split (compact.rs:606) uses integer division, so
        # each of the ``len(post)`` rows can round down by up to 1 B —
        # assert the SUM within that slack.
        expected = total_rows * _PERSIST_PER_ROW_BYTES
        assert abs(total_size - expected) <= len(post), (
            f"summed re-pointed size_bytes {total_size} must track the"
            f" in-memory footprint {expected} (±{len(post)} for the"
            f" proportional-split rounding), not the on-disk merged-file size"
        )

        # Criterion 3 sanity check (not the discriminating assertion —
        # see roborev finding: at this data scale the merged footprint is
        # far below the 1 MiB cap, so this passes regardless of the
        # sizing unit; the equality above is what pins the fix). It
        # guards only that the corrected unit didn't somehow push a
        # small-input merge past the cap.
        assert total_size <= MAX_SEGMENT_BYTES, (
            f"merged segment in-memory footprint {total_size} exceeds"
            f" max_segment_bytes {MAX_SEGMENT_BYTES}"
        )


# CHA-198 — catalog-scoped persist + snapshot metadata tables
#
# Tests below pin the per-catalog physical isolation of the five
# persist/purge/snapshot metadata tables, their per-branch LIST
# partitioning, and the structured ``branch_uuid`` / ``table_uuid``
# columns the new writers populate.
#
# Post-CHA-198 + CHA-220 shape:
#
# - Five metadata parents are renamed to per-catalog tables, prefixed
#   with the owning catalog UUID (``{catalog_uuid}_table_persist_metadata``,
#   etc.). Their *base* string is named by the constants
#   ``TABLE_PERSIST_METADATA`` / ``TABLE_PURGE_METADATA`` / ... below.
#   CHA-220 dropped ``branch_persist_metadata`` and added
#   ``table_purge_metadata`` in its place.
# - Each parent is LIST-partitioned by ``branch_uuid``. Per-branch
#   leaf partitions: ``{partition_uuid}_<base>_partition`` where
#   ``partition_uuid = row_uuid_for_pk(get_system_<base>_table_uuid(
#   catalog_uuid), [branch_uuid_str])`` — same shape as
#   ``commit_tx_log_partition``.
# - Every row carries ``branch_uuid`` and ``table_uuid`` (all five
#   parents are table-scoped post-CHA-220).
# - ``DeleteCatalog`` drops parents + per-branch partitions via PG
#   cascade. ``DeleteBranch`` drops the per-branch partitions of those
#   parents (and only those) for that branch.

ALL_PER_CATALOG_METADATA_BASES = (
    TABLE_PERSIST_METADATA,
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_PURGE_METADATA,
    TABLE_SNAPSHOT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)

# CHA-444 (ADR 0027): the per-branch abort-order counter parent
# (``{catalog_uuid}_abort_seq_num``), seeded one row per branch at branch
# creation. It is a tx-log-family sibling (LIST-partitioned like ``commit_tx_log``,
# NOT like the metadata family), so it is deliberately NOT part of
# ``ALL_PER_CATALOG_METADATA_BASES`` — the per-branch-partition-naming tests
# below would compute the wrong leaf name for it. The per-catalog isolation
# test checks it separately via the parent's ``branch_uuid`` column.
ABORT_SEQ_NUM = "abort_seq_num"


def _per_catalog_metadata_table(catalog_uuid: str, base: str) -> str:
    """Per-catalog metadata parent table name, ``{uuid}_<base>``."""
    return f"{catalog_uuid}_{base}"


def _per_branch_metadata_partition(
    catalog_uuid: str, branch_uuid: str, base: str
) -> str:
    """``{partition_uuid}_<base>_partition`` for the named branch.

    Mirrors the Rust per-branch list partition for the persist/snapshot/
    purge metadata family: ``partition_uuid = row_uuid_for_pk(catalog_uuid,
    [branch_uuid, base])``. Inlined here rather than added to
    ``penca_client.naming`` because Python is being deprecated.
    """
    partition_uuid = row_uuid_for_pk(catalog_uuid, [branch_uuid, base])
    return f"{partition_uuid}_{base}_partition"


def _create_catalog_with_table(
    client,
    *,
    catalog_prefix: str,
) -> tuple[str, str, str, str]:
    """Create catalog + schema + table on main; return all four UUIDs."""
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"{catalog_prefix}_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "schema_a",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-198",
    )
    table_uuid = client.create_table(
        "user_table",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha-198",
    )
    main_uuid = main_branch_uuid
    return catalog_uuid, schema_uuid, table_uuid, main_uuid


def _commit_one_row(
    client, *, catalog_uuid: str, schema_uuid: str, branch_uuid: str, table_uuid: str
) -> None:
    """Begin → mutate → commit a single-row tx on the named branch."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(
        {"name": ["alice"], "value": [1]},
        schema=USER_SCHEMA,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


def _select_branch_uuids(catalog_uuid: str, base: str) -> list[str]:
    """Return all distinct ``branch_uuid`` values in the per-catalog parent."""
    parent = _per_catalog_metadata_table(catalog_uuid, base)
    rows = get_pg_driver().execute(
        SQL("SELECT DISTINCT branch_uuid FROM {}").format(Identifier(parent)),
        (),
    )
    return [r[0] for r in rows]


def _list_tables_with_prefix(prefix: str) -> list[str]:
    """All ``table_name`` in the public schema starting with ``prefix``."""
    rows = get_pg_driver().execute(
        SQL(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = 'public' AND table_name LIKE %s"
        ),
        (f"{prefix}%",),
    )
    return [r[0] for r in rows]


class TestPerCatalogMetadataIsolation:
    """Two catalogs A and B, each persisted + snapshotted + purged, see
    each other's metadata only in their own per-catalog parent.

    CHA-198 isolates the parents to ``{catalog_uuid}_<base>`` tables;
    CHA-220 replaces ``branch_persist_metadata`` with
    ``table_purge_metadata`` in the five-parent metadata set; CHA-444
    (ADR 0027) adds the per-branch ``abort_seq_num`` counter parent —
    six per-catalog tables, all branch-isolated.
    """

    def test_per_catalog_isolation_across_all_six_tables(self):
        client = make_client()
        cat_a, schema_a, table_a, main_a = _create_catalog_with_table(
            client, catalog_prefix="cat_a"
        )
        cat_b, schema_b, table_b, main_b = _create_catalog_with_table(
            client, catalog_prefix="cat_b"
        )

        # CHA-444 (ADR 0027): Purge advances Pu only to W_snap, so each
        # catalog's full lifecycle is Persist → Snapshot → Purge — the
        # Snapshot must precede the Purge for it to stamp a
        # ``table_purge_metadata`` row.
        for cat, schema, table, branch in (
            (cat_a, schema_a, table_a, main_a),
            (cat_b, schema_b, table_b, main_b),
        ):
            _commit_one_row(
                client,
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.persist(
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.snapshot(
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.purge(
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )

        # The five metadata parents (rows from persist/snapshot/purge) plus
        # the ``abort_seq_num`` counter (seeded one row per branch at
        # creation) must each hold only their own catalog's branch.
        for base in (*ALL_PER_CATALOG_METADATA_BASES, ABORT_SEQ_NUM):
            a_branches = set(_select_branch_uuids(cat_a, base))
            b_branches = set(_select_branch_uuids(cat_b, base))
            assert a_branches, (
                f"catalog A's {base} parent should hold >=1 row after persist+snapshot"
            )
            assert b_branches, (
                f"catalog B's {base} parent should hold >=1 row after persist+snapshot"
            )
            assert a_branches == {main_a}, (
                f"catalog A's {base} contains foreign branch_uuids"
                f" {a_branches - {main_a}}"
            )
            assert b_branches == {main_b}, (
                f"catalog B's {base} contains foreign branch_uuids"
                f" {b_branches - {main_b}}"
            )
            assert main_b not in a_branches, (
                f"catalog A's {base} leaked B's main branch_uuid"
            )
            assert main_a not in b_branches, (
                f"catalog B's {base} leaked A's main branch_uuid"
            )


class TestPerBranchMetadataPartitionAndDropBranchCleanup:
    """Each per-catalog parent is LIST-partitioned by ``branch_uuid``;
    each branch lives in its own leaf. ``DeleteBranch`` drops the
    dropped branch's leaves and only those.

    RED today: the LIST partitions don't exist; the
    ``information_schema.tables`` query for partition names returns 0.
    Post-CHA-198 (before DeleteBranch): each base has a per-branch
    leaf for ``main`` and ``feature``. After DeleteBranch on
    ``feature``: ``main``'s leaves survive, ``feature``'s are gone.
    """

    def test_per_branch_partitions_then_drop_branch_cleans_only_dropped_branch(
        self,
    ):
        client = make_client()
        cat_uuid, schema_uuid, table_uuid_main, main_uuid = _create_catalog_with_table(
            client, catalog_prefix="cat_drop_branch"
        )

        feature_branch = client.create_branch(
            f"feature_{uuid4().hex[:6]}",
            author="test",
            comment="cha-198",
            catalog_uuid=cat_uuid,
        )
        feature_uuid = feature_branch.branch_uuid
        feature_table_uuid = create_table_on_branch(
            client,
            cat_uuid,
            schema_uuid,
            feature_uuid,
            table_name="user_table_feature",
        )

        for branch, table in (
            (main_uuid, table_uuid_main),
            (feature_uuid, feature_table_uuid),
        ):
            _commit_one_row(
                client,
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.persist(
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.purge(
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            main_part = _per_branch_metadata_partition(cat_uuid, main_uuid, base)
            feature_part = _per_branch_metadata_partition(cat_uuid, feature_uuid, base)

            for partition, expected_branch in (
                (main_part, main_uuid),
                (feature_part, feature_uuid),
            ):
                got_partition = get_pg_driver().execute(
                    SQL(
                        "SELECT 1 FROM information_schema.tables"
                        " WHERE table_schema = 'public' AND table_name = %s"
                    ),
                    (partition,),
                )
                assert got_partition, (
                    f"expected per-branch partition {partition} of {base}"
                    f" for branch {expected_branch} to exist"
                )

                rows = get_pg_driver().execute(
                    SQL("SELECT DISTINCT branch_uuid FROM {}").format(
                        Identifier(partition)
                    ),
                    (),
                )
                seen = {r[0] for r in rows}
                assert seen.issubset({expected_branch}), (
                    f"partition {partition} contains foreign branches"
                    f" {seen - {expected_branch}}"
                )

        client.delete_branch(
            catalog_uuid=cat_uuid,
            branch_uuid=feature_uuid,
        )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            main_part = _per_branch_metadata_partition(cat_uuid, main_uuid, base)
            feature_part = _per_branch_metadata_partition(cat_uuid, feature_uuid, base)

            main_alive = get_pg_driver().execute(
                SQL(
                    "SELECT count(*) FROM information_schema.tables"
                    " WHERE table_schema = 'public' AND table_name = %s"
                ),
                (main_part,),
            )[0][0]
            assert main_alive == 1, (
                f"main's {base} partition {main_part} must survive"
                " DeleteBranch(feature)"
            )

            feature_alive = get_pg_driver().execute(
                SQL(
                    "SELECT count(*) FROM information_schema.tables"
                    " WHERE table_schema = 'public' AND table_name = %s"
                ),
                (feature_part,),
            )[0][0]
            assert feature_alive == 0, (
                f"feature's {base} partition {feature_part} must be dropped"
                " by DeleteBranch"
            )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            survivors = set(_select_branch_uuids(cat_uuid, base))
            assert feature_uuid not in survivors, (
                f"feature branch_uuid leaked into {base} after DeleteBranch"
            )


class TestDeleteCatalogCascadesMetadata:
    """``DeleteCatalog`` drops every per-catalog parent and every
    per-branch partition under it.

    RED today: per-catalog parents don't exist, so the pre-delete
    existence assertion fails. Post-CHA-198: pre-delete the parents +
    partitions exist; post-delete they're all gone.
    """

    def test_delete_catalog_drops_all_metadata_tables_and_partitions(self):
        client = make_client()
        cat_uuid, schema_uuid, table_uuid_main, main_uuid = _create_catalog_with_table(
            client, catalog_prefix="cat_cascade"
        )

        feature_branch = client.create_branch(
            f"feature_{uuid4().hex[:6]}",
            author="test",
            comment="cha-198",
            catalog_uuid=cat_uuid,
        )
        feature_uuid = feature_branch.branch_uuid
        feature_table_uuid = create_table_on_branch(
            client,
            cat_uuid,
            schema_uuid,
            feature_uuid,
            table_name="user_table_feature",
        )

        for branch, table in (
            (main_uuid, table_uuid_main),
            (feature_uuid, feature_table_uuid),
        ):
            _commit_one_row(
                client,
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.persist(
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.purge(
                catalog_uuid=cat_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch,
                table_uuid=table,
            )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            parent = _per_catalog_metadata_table(cat_uuid, base)
            parent_alive = get_pg_driver().execute(
                SQL(
                    "SELECT count(*) FROM information_schema.tables"
                    " WHERE table_schema = 'public' AND table_name = %s"
                ),
                (parent,),
            )[0][0]
            assert parent_alive == 1, (
                f"pre-delete: per-catalog parent {parent} must exist"
            )

        client.delete_catalog(catalog_uuid=cat_uuid)

        residue_parents = _list_tables_with_prefix(cat_uuid)
        assert residue_parents == [], (
            f"per-catalog tables prefixed with {cat_uuid} should all be gone"
            f" after DeleteCatalog, found: {residue_parents}"
        )

        for branch in (main_uuid, feature_uuid):
            for base in ALL_PER_CATALOG_METADATA_BASES:
                partition = _per_branch_metadata_partition(cat_uuid, branch, base)
                alive = get_pg_driver().execute(
                    SQL(
                        "SELECT count(*) FROM information_schema.tables"
                        " WHERE table_schema = 'public' AND table_name = %s"
                    ),
                    (partition,),
                )[0][0]
                assert alive == 0, (
                    f"per-branch partition {partition} must be dropped"
                    f" by DeleteCatalog (branch={branch}, base={base})"
                )


class TestSnapshotPerCatalogMetadataRoundTrip:
    """A snapshot lands rows in the per-catalog snapshot tables (in
    the main-branch partition), and Plan() then surfaces them.

    RED today: the per-catalog parents don't exist. Post-CHA-198:
    rows are present, and Plan surfaces them via
    ``cold_storage.snapshot``.
    """

    def test_snapshot_lands_in_per_catalog_per_branch_partition(self):
        client = make_client()
        cat_uuid, schema_uuid, table_uuid, main_uuid = _create_catalog_with_table(
            client, catalog_prefix="cat_snap"
        )

        _commit_one_row(
            client,
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.persist(
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.snapshot(
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        snap_parent = _per_catalog_metadata_table(cat_uuid, TABLE_SNAPSHOT_METADATA)
        snap_seg_parent = _per_catalog_metadata_table(
            cat_uuid, TABLE_SNAPSHOT_SEGMENT_METADATA
        )

        snap_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(snap_parent)),
            (),
        )[0][0]
        assert snap_count >= 1, f"{snap_parent} should hold >=1 row after Snapshot"

        seg_count = get_pg_driver().execute(
            SQL("SELECT count(*) FROM {}").format(Identifier(snap_seg_parent)),
            (),
        )[0][0]
        assert seg_count >= 1, f"{snap_seg_parent} should hold >=1 row after Snapshot"

        snap_main_part = _per_branch_metadata_partition(
            cat_uuid, main_uuid, TABLE_SNAPSHOT_METADATA
        )
        snap_seg_main_part = _per_branch_metadata_partition(
            cat_uuid, main_uuid, TABLE_SNAPSHOT_SEGMENT_METADATA
        )
        for partition in (snap_main_part, snap_seg_main_part):
            partition_rows = get_pg_driver().execute(
                SQL("SELECT count(*) FROM {}").format(Identifier(partition)),
                (),
            )[0][0]
            assert partition_rows >= 1, (
                f"main-branch leaf partition {partition} should hold >=1 row"
                " after Snapshot (parent had rows; partition routes by branch_uuid)"
            )

        # (The plan-level read-back of the snapshot segments moved to Rust
        # assemble_plan unit tests / CHA-456; the per-catalog/per-branch
        # partition routing is pinned by the direct partition-row counts
        # above.)


class TestStructuredColumnsBranchAndTableUuid:
    """After persist + snapshot, every row in all five per-catalog
    parents carries non-NULL ``branch_uuid``; rows in the four
    table-scoped parents also carry non-NULL ``table_uuid``.

    CHA-203 dropped the legacy ``data_log_prefix_uuid`` column from
    every parent — the test no longer cross-checks it; the structured
    ``(branch_uuid, table_uuid)`` pair is the canonical surface.
    """

    def test_branch_and_table_uuid_columns_populated_consistently(self):
        client = make_client()
        cat_uuid, schema_uuid, table_uuid, main_uuid = _create_catalog_with_table(
            client, catalog_prefix="cat_struct"
        )

        _commit_one_row(
            client,
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        client.persist(
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        # CHA-444 (ADR 0027): Snapshot before Purge so Purge advances Pu and
        # genuinely stamps a ``table_purge_metadata`` row (whose branch_uuid /
        # table_uuid columns this test verifies are non-NULL).
        client.snapshot(
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )
        client.purge(
            catalog_uuid=cat_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            parent = _per_catalog_metadata_table(cat_uuid, base)
            null_branches = get_pg_driver().execute(
                SQL("SELECT count(*) FROM {tbl} WHERE branch_uuid IS NULL").format(
                    tbl=Identifier(parent)
                ),
                (),
            )[0][0]
            assert null_branches == 0, (
                f"{parent} has {null_branches} rows with NULL branch_uuid"
            )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            parent = _per_catalog_metadata_table(cat_uuid, base)

            null_tables = get_pg_driver().execute(
                SQL("SELECT count(*) FROM {tbl} WHERE table_uuid IS NULL").format(
                    tbl=Identifier(parent)
                ),
                (),
            )[0][0]
            assert null_tables == 0, (
                f"{parent} has {null_tables} rows with NULL table_uuid"
            )


class TestBootstrapOnlyCatalogStoreGlobal:
    """After bootstrap, the public schema holds ``catalog_store`` and
    **none** of the bare-name persist / snapshot metadata parents.

    RED today: bare names exist as global parents. Post-CHA-198: those
    parents are per-catalog (``{catalog_uuid}_<base>``), so the bare
    names disappear from the public schema entirely.

    NOTE: integration-test fixtures keep state between tests in the
    same run; the assertion is invariant — bare names should never
    exist after the rename, regardless of how many catalogs have been
    created in the session.
    """

    def test_bare_name_metadata_tables_absent_catalog_store_present(self):
        catalog_store_count = get_pg_driver().execute(
            SQL(
                "SELECT count(*) FROM information_schema.tables"
                " WHERE table_schema = 'public' AND table_name = 'catalog_store'"
            ),
            (),
        )[0][0]
        assert catalog_store_count == 1, (
            "catalog_store must be present in the public schema after bootstrap"
        )

        for base in ALL_PER_CATALOG_METADATA_BASES:
            present = get_pg_driver().execute(
                SQL(
                    "SELECT count(*) FROM information_schema.tables"
                    " WHERE table_schema = 'public' AND table_name = %s"
                ),
                (base,),
            )[0][0]
            assert present == 0, (
                f"bare-name {base} table must NOT exist after CHA-198 —"
                f" all metadata parents are per-catalog ({{uuid}}_{base})"
            )


# CHA-203: log_kind CHECK constraint


class TestLogKindCheckConstraintRejectsInvalid:
    """``table_persist_metadata.log_kind`` carries a CHECK constraint
    restricting values to ``'upsert_log'`` / ``'delete_log'`` /
    ``'commit_tx_log'``. A direct INSERT with an out-of-domain value raises a
    Postgres constraint-violation error.

    RED today: ``log_kind`` doesn't exist on the column, so the INSERT
    instead raises ``UndefinedColumn`` (still an error — but for the
    wrong reason). Post-CHA-203: the column exists, the CHECK rejects
    the bad value, the error matches a psycopg integrity error.
    """

    def test_log_kind_check_rejects_invalid(self):
        # Need a real catalog so the per-catalog parent exists.
        client = make_client()
        cat_uuid, _schema_uuid, _table_uuid, _main_uuid = _create_catalog_with_table(
            client, catalog_prefix="log_kind_check"
        )

        tfm_parent = _per_catalog_metadata_table(cat_uuid, TABLE_PERSIST_METADATA)

        # Direct INSERT with log_kind='garbage'. The CHECK constraint
        # must reject the row; psycopg raises an integrity-error subclass.
        # (The exact subclass — CheckViolation vs IntegrityError —
        # depends on the psycopg version; matching the base class keeps
        # the assertion robust.)
        with pytest.raises(psycopg.errors.IntegrityError):
            get_pg_driver().execute_no_result(
                SQL(
                    "INSERT INTO {tbl}"
                    " (table_persist_uuid, branch_uuid, table_uuid,"
                    "  persisted_at_micros, log_kind)"
                    " VALUES (%s, %s, %s, 0, %s)"
                ).format(tbl=Identifier(tfm_parent)),
                (
                    str(uuid4()),
                    str(uuid4()),
                    str(uuid4()),
                    "garbage",
                ),
            )


# CHA-203: cold object_uri path layout


class TestColdSegmentPathsUnderCatalogBranchPrefix:
    """Every cold ``object_uri`` lives under
    ``{base_uri}/{catalog_uuid}/{branch_uuid}/{persist|snapshot}/...``.

    Two branches in the same catalog have disjoint
    ``{base_uri}/{catalog_uuid}/{branch_uuid}/`` subtrees. Two catalogs
    have disjoint ``{base_uri}/{catalog_uuid}/`` top-level subtrees.

    The prefix scheme makes catalog/branch deletion a one-directory
    sweep and makes branch tenancy visible from the filesystem layout
    (versus the pre-CHA-203 scheme that buried tenant identity in the
    ``hot_storage_table_name`` middle segment).

    RED today: writes use the old ``{base_uri}/{hot_storage_table_name}/...``
    layout, so the regex doesn't match. Post-CHA-203: the writers
    emit the new prefix and the regex matches every persisted URI.
    """

    def test_cold_segment_paths_under_catalog_branch_prefix(self):
        client = make_client()
        cat_a, schema_a, table_a, main_a = _create_catalog_with_table(
            client, catalog_prefix="cold_path_a"
        )
        feature_branch = client.create_branch(
            f"feat_{uuid4().hex[:6]}",
            catalog_uuid=cat_a,
            author="test",
            comment="cha-203",
        )
        feat_a = feature_branch.branch_uuid
        feat_table_a = create_table_on_branch(
            client, cat_a, schema_a, feat_a, table_name="user_table_feat"
        )
        cat_b, schema_b, table_b, main_b = _create_catalog_with_table(
            client, catalog_prefix="cold_path_b"
        )

        # Drive a persist + snapshot for each (catalog, branch) tuple so
        # every per-catalog parent has both persist and snapshot segments.
        for cat, schema, table, branch in (
            (cat_a, schema_a, table_a, main_a),
            (cat_a, schema_a, feat_table_a, feat_a),
            (cat_b, schema_b, table_b, main_b),
        ):
            _commit_one_row(
                client,
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.persist(
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )
            client.snapshot(
                catalog_uuid=cat,
                schema_uuid=schema,
                table_uuid=table,
                branch_uuid=branch,
            )
            client.purge(
                catalog_uuid=cat,
                schema_uuid=schema,
                branch_uuid=branch,
                table_uuid=table,
            )

        def _segment_uris(catalog_uuid: str) -> list[tuple[str, str, str]]:
            tfsm = _per_catalog_metadata_table(
                catalog_uuid, TABLE_PERSIST_SEGMENT_METADATA
            )
            snap_seg = _per_catalog_metadata_table(
                catalog_uuid, TABLE_SNAPSHOT_SEGMENT_METADATA
            )
            uris: list[tuple[str, str, str]] = []
            for parent in (tfsm, snap_seg):
                rows = get_pg_driver().execute(
                    SQL("SELECT branch_uuid, object_uri FROM {}").format(
                        Identifier(parent)
                    ),
                    (),
                )
                for branch_col, uri in rows:
                    uris.append((catalog_uuid, branch_col, uri))

            return uris

        def _parent_branch(catalog_uuid: str, branch_uuid: str) -> str | None:
            rows = get_pg_driver().execute(
                SQL("SELECT parent_branch_uuid FROM {} WHERE branch_uuid = %s").format(
                    Identifier(
                        _per_catalog_metadata_table(catalog_uuid, "branch_store")
                    )
                ),
                (branch_uuid,),
            )

            return str(rows[0][0]) if rows and rows[0][0] is not None else None

        def _branch_names_object(catalog_uuid: str, branch_uuid: str, uri: str) -> bool:
            """Whether ``branch_uuid`` holds a row for this exact object.

            What separates an inherited *reference* from a fresh write into
            someone else's subtree: only the former is still named by the branch
            whose prefix the URI sits under.
            """
            for base in (
                TABLE_PERSIST_SEGMENT_METADATA,
                TABLE_SNAPSHOT_SEGMENT_METADATA,
            ):
                rows = get_pg_driver().execute(
                    SQL(
                        "SELECT 1 FROM {} WHERE branch_uuid = %s AND object_uri = %s"
                        " LIMIT 1"
                    ).format(
                        Identifier(_per_catalog_metadata_table(catalog_uuid, base))
                    ),
                    (branch_uuid, uri),
                )
                if rows:
                    return True

            return False

        all_uris = _segment_uris(cat_a) + _segment_uris(cat_b)
        assert all_uris, "test setup: no cold segments were persisted"

        # (a) Every URI matches {base_uri}/{catalog_uuid}/{branch_uuid}/...
        #     The base_uri itself is configuration-dependent; we assert
        #     the path-component invariant: catalog_uuid appears as a
        #     segment, immediately followed by the WRITER's branch_uuid.
        #
        #     Writer, not the row's branch: since CHA-539 a fork's inherited
        #     persist/snapshot rows carry the parent's `object_uri` verbatim, so
        #     a child's row legitimately points into the parent's subtree. The
        #     layout rule binds whoever wrote the object; a referencing row does
        #     not move it.
        #
        #     Merely accepting "own branch OR parent" would be too weak — it also
        #     admits a fresh object WRITTEN under the parent's prefix, which is
        #     the layout violation this assertion exists to catch. So a URI under
        #     the parent's prefix has to prove it is a reference: the parent must
        #     still hold a committed row naming that exact object.
        for catalog_uuid, branch_uuid, uri in all_uris:
            own_prefix = f"/{catalog_uuid}/{branch_uuid}/"
            if own_prefix in uri:
                writer = branch_uuid
            else:
                parent = _parent_branch(catalog_uuid, branch_uuid)
                assert parent is not None and f"/{catalog_uuid}/{parent}/" in uri, (
                    f"object_uri {uri!r} must contain catalog+branch prefix"
                    f" {own_prefix} — or, for a fork's inherited row, its"
                    f" parent's (CHA-203 layout)"
                )
                assert _branch_names_object(catalog_uuid, parent, uri), (
                    f"object_uri {uri!r} sits under the parent's prefix but the"
                    f" parent holds no committed row for it — that is a fresh"
                    f" write into another branch's subtree, not an inherited"
                    f" reference (CHA-203 layout)"
                )
                writer = parent

            tail_re = re.compile(
                rf"/{re.escape(catalog_uuid)}/{re.escape(writer)}/(persist|snapshot)/"
            )
            assert tail_re.search(uri), (
                f"object_uri {uri!r} must place 'persist' or 'snapshot' as the"
                f" subdirectory under the {{catalog}}/{{branch}}/ prefix"
            )

        # (b) Two branches in catalog A have disjoint catalog/branch subtrees.
        main_a_prefix = f"/{cat_a}/{main_a}/"
        feat_a_prefix = f"/{cat_a}/{feat_a}/"
        main_a_uris = [u for c, b, u in all_uris if c == cat_a and b == main_a]
        feat_a_uris = [u for c, b, u in all_uris if c == cat_a and b == feat_a]
        assert main_a_uris and feat_a_uris, (
            "expected both branches of cat_a to have persisted segments"
        )
        # main is never a fork, so every object it references it also wrote.
        for u in main_a_uris:
            assert feat_a_prefix not in u, (
                f"main branch URI {u!r} leaked into feature branch subtree"
                f" {feat_a_prefix}"
            )

        # The fork's rows split in two, and both halves are load-bearing: the
        # table created ON the fork is written under the fork's own subtree, and
        # the rows it inherited at CreateBranch reference the parent's subtree
        # (CHA-539 — copied metadata, shared objects). Asserting both keeps the
        # subtree rule meaningful for a fork; asserting only "no main prefix"
        # would now be false, and dropping the check entirely would stop pinning
        # that a fork's own writes land under its own prefix.
        feat_own = [u for u in feat_a_uris if feat_a_prefix in u]
        feat_inherited = [u for u in feat_a_uris if main_a_prefix in u]
        assert feat_own, (
            f"the table created on the feature branch must be written under"
            f" {feat_a_prefix}, saw {feat_a_uris}"
        )
        assert feat_inherited, (
            f"the feature branch must reference the parent's objects for the"
            f" tables it inherited at fork time, saw {feat_a_uris}"
        )
        # No isdisjoint check here: both lists are filtered off the same source on
        # mutually exclusive substrings, so it could not fail.

        # (c) Two catalogs have disjoint top-level subtrees.
        cat_a_uris = [u for c, _b, u in all_uris if c == cat_a]
        cat_b_uris = [u for c, _b, u in all_uris if c == cat_b]
        assert cat_a_uris and cat_b_uris
        for u in cat_a_uris:
            assert f"/{cat_b}/" not in u, (
                f"catalog A URI {u!r} leaked into catalog B subtree /{cat_b}/"
            )

        for u in cat_b_uris:
            assert f"/{cat_a}/" not in u, (
                f"catalog B URI {u!r} leaked into catalog A subtree /{cat_a}/"
            )


# CHA-218: snapshot watermark sourced from cold persist segments, not commit_tx_log


class TestSnapshotWatermarkFromColdPersistSegments:
    """``snapshotted_at_micros`` equals
    ``MAX(commit_micros)`` across cold upsert + delete persist
    segments — not ``MAX(commit_micros)`` over cold ``commit_tx_log``.

    RED today: ``snapshot_locked`` reads cold ``commit_tx_log`` segments via
    ``read_commit_tx_log_window`` + ``filter_commit_tx_log_by_branch`` to compute
    the watermark. Under CHA-218 there are no cold ``commit_tx_log`` segments
    to read; the watermark must come from the data segments directly.

    Setup ensures cold upserts AND cold deletes both contribute so
    the ``MAX(...)`` clause must merge across both segment families.
    """

    def test_snapshot_watermark_matches_cold_segment_max_committed_at(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        main_uuid = main_branch_uuid

        tx_upsert = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_upsert.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["alice", "bob"], "value": [1, 2]},
                    schema=USER_SCHEMA,
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        upsert_committed = client.commit_tx(
            tx_upsert.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )

        tx_delete = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        client.write_data(
            tx_delete.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                deletes=pa.table({"name": ["alice"]}, schema=_PK_SCHEMA_NAME),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
        )
        delete_committed = client.commit_tx(
            tx_delete.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
        )
        expected_max = max(
            upsert_committed.commit_micros,
            delete_committed.commit_micros,
        )

        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_uuid,
            table_uuid=table_uuid,
        )
        snap = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_uuid,
        )

        # Cross-check the watermark via direct SQL on cold persist
        # segments. The segment metadata's max_tx_commit_micros
        # IS the per-row denormalized commit_micros under
        # CHA-218 (segments are written from the JOIN result), so
        # MAX(max_tx_commit_micros) across both upsert and
        # delete segment families is the new watermark source.
        tfm_parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
        tfsm_parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
        rows = get_pg_driver().execute(
            SQL(
                "SELECT MAX(seg.max_tx_commit_micros)"
                " FROM {seg} seg"
                " INNER JOIN {tfm} tfm"
                "   ON seg.table_persist_uuid = tfm.table_persist_uuid"
                "  AND seg.branch_uuid = tfm.branch_uuid"
                " WHERE tfm.branch_uuid = %s"
                "   AND tfm.table_uuid = %s"
                "   AND tfm.log_kind IN ('upsert_log','delete_log')"
                "   AND seg.commit_micros IS NOT NULL"
            ).format(seg=Identifier(tfsm_parent), tfm=Identifier(tfm_parent)),
            (main_uuid, table_uuid),
        )
        cold_segment_max = rows[0][0]
        assert cold_segment_max == expected_max, (
            "test cross-check: cold segment MAX(commit_micros) must"
            f" equal max(tx_upsert.committed_at, tx_delete.committed_at)"
            f" = {expected_max}, got {cold_segment_max}"
        )

        # The snapshot's watermark equals the cold-segment max — i.e.
        # the new snapshot read path computes it without touching
        # cold commit_tx_log.
        assert snap.HasField("snapshotted_at_micros")
        assert snap.snapshotted_at_micros == expected_max, (
            f"snapshotted_at_micros={snap.snapshotted_at_micros} must equal"
            f" MAX(cold segment commit_micros)={expected_max}"
            " (computed without reading cold commit_tx_log)"
        )


# CHA-407: snapshot-compaction machinery removed (ADR 0024)


class TestSnapshotCompactionRemoved:
    """Snapshot segments are immutable per ADR 0024 — the compaction
    machinery is gone from the wire and the dead columns are gone from
    ``table_snapshot_segment_metadata``."""

    def test_compact_snapshot_segments_rpc_unimplemented(self):
        """The ``CompactSnapshotSegments`` RPC no longer exists on the
        lifecycle service: a raw call to the method path returns
        ``UNIMPLEMENTED`` from the tonic router.

        Invokes the method with raw bytes (an empty
        ``CompactSnapshotSegmentsRequest`` is valid wire bytes) instead
        of generated stubs, so the test stays importable after the
        stubs are deleted.
        """
        channel = insecure_channel(os.environ["PENCA_LIFECYCLE_URL"])
        call = channel.unary_unary(
            "/penca_proto.external.v1.LifecycleService/CompactSnapshotSegments",
            request_serializer=lambda b: b,
            response_deserializer=lambda b: b,
        )
        with pytest.raises(RpcError) as excinfo:
            call(b"")

        # The concrete grpcio error inherits Call, which provides
        # code(); the stubs expose only the bare RpcError base (same
        # pattern as penca_client.status.rpc_error_to_api_error).
        code = excinfo.value.code()  # ty: ignore[unresolved-attribute]
        assert code == StatusCode.UNIMPLEMENTED, (
            "CompactSnapshotSegments must be absent from the lifecycle"
            f" service; the server answered with {code}"
        )

    def test_snapshot_segment_schema_immutable_shape(self):
        """``table_snapshot_segment_metadata`` carries no compaction
        columns: ``min_partition_value`` / ``max_partition_value`` /
        ``is_sealed`` are gone, ``offset`` / ``length`` are NOT NULL
        (CHA-404 packed row-range addressing has no NULL shape), and
        the snapshot-side partial seal index is not created.
        """
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, _main_branch_uuid = setup_schema(
            client
        )
        seg_parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        rows = get_pg_driver().execute(
            "SELECT column_name, is_nullable FROM information_schema.columns"
            " WHERE table_name = %s",
            (seg_parent,),
        )
        nullable_by_column = {r[0]: r[1] for r in rows}
        assert nullable_by_column, f"DDL must have created {seg_parent}"
        for dead_column in ("min_partition_value", "max_partition_value", "is_sealed"):
            assert dead_column not in nullable_by_column, (
                f"{dead_column} must be dropped from {seg_parent}"
            )

        assert nullable_by_column["offset"] == "NO", "offset must be NOT NULL"
        assert nullable_by_column["length"] == "NO", "length must be NOT NULL"

        index_rows = get_pg_driver().execute(
            "SELECT indexname FROM pg_indexes WHERE tablename = %s",
            (seg_parent,),
        )
        seal_indexes = [r[0] for r in index_rows if r[0].endswith("_tssm_seal")]
        assert not seal_indexes, (
            f"snapshot-side partial seal index must not exist: {seal_indexes}"
        )
