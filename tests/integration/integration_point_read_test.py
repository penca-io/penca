"""CHA-398 — ``ids`` PK-batch point-lookup pushdown acceptance tests.

``ReadDataRequest.ids`` / ``AuditDataRequest.ids`` carry an Arrow IPC
record batch of exactly the table's declared primary-key columns, in
declared order (wire-identical to ``Change.deletes``). The server
derives ``row_uuid`` per row and restricts the read/audit to those
rows; absent/empty bytes mean no restriction.

Four acceptance groups:

- ``TestReadDataIds`` — exact-row returns across tiers (all-hot,
  persist+purged cold log, snapshot) and AND-composition with
  ``filter`` / projection / ``as_of`` / ``open_tx_uuid``.
- ``TestReadDataIdsValidation`` — bad batch shapes are rejected with
  ``INVALID_ARGUMENT`` at the boundary (raw-stub requests where the
  client cannot express the bad shape).
- ``TestAuditDataIds`` — the audit stream restricts to the named rows'
  history and composes with the ``committed_at`` window.
- ``TestHotLogRowUuidIndex`` — white-box: the ``(row_uuid, tx_uuid)``
  point-read index exists on both hot logs (deferred to CHA-398 by the
  DDL comment in ``crates/penca-db/src/dialect/pg.rs``).

Scoped run: ``just integration-test point_read``
"""

from __future__ import annotations

import json
import re
import time
from typing import Literal
from uuid import uuid4

import pyarrow as pa
import pytest
from grpc import RpcError, StatusCode, insecure_channel
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.arrow import batch_to_ipc_bytes
from penca_client.config import ClientSettings
from penca_client.errors import InvalidRequestError
from penca_client.naming import delete_log_table, upsert_log_table
from penca_proto.external.v1.query_pb2 import ReadDataRequest
from penca_proto.external.v1.query_pb2_grpc import QueryServiceStub

from .integration_flight_sql_test import _execute_update_steps_via
from .integration_helpers import (
    USER_SCHEMA,
    container_log,
    get_pg_driver,
    make_client,
    setup_schema,
    setup_with_data_named,
)

_PK_ONLY_SCHEMA = pa.schema([pa.field("name", pa.utf8())])

# Composite-PK fixture: two PK columns; declared order is (region, name).
_COMPOSITE_SCHEMA = pa.schema(
    [
        pa.field("region", pa.utf8()),
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)


def _ids(*names: str) -> pa.Table:
    """A PK-batch ids Table for the standard single-PK fixture."""
    return pa.table({"name": list(names)}, schema=_PK_ONLY_SCHEMA)


def _seed(client, ctx, rows: dict[str, int]) -> None:
    """Auto-commit upsert of ``{name: value}`` rows into the fixture table."""
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
    batch = pa.table(
        {"name": list(rows.keys()), "value": list(rows.values())},
        schema=USER_SCHEMA,
    )
    client.write_data(
        None,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author="test",
        comment="cha-398 fixture",
    )


def _read_ids(client, ctx, ids: pa.Table, **kwargs) -> pa.Table:
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
    return client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch_uuid,
        ids=ids,
        **kwargs,
    )


def _persist_and_purge(client, ctx) -> None:
    """Move committed rows to the cold tier: Persist, Snapshot, Purge.
    After this the hot logs no longer serve the rows and the plan's
    hot/cold read fence (``Pu``) covers them.

    CHA-444 (ADR 0027): Purge advances ``Pu`` only to ``W_snap``, so a
    Snapshot must run before Purge can clear the committed hot rows. Both
    watermark ops are no-op-capable (unset response watermark), so assert
    the transition actually happened — otherwise the cross-tier tests
    would silently pass against the hot tier, the one path they exist
    not to test."""
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
    persist_response = client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )
    assert persist_response.HasField("persisted_at_micros"), (
        "persist was a no-op; the cross-tier fixture did not move rows cold"
    )
    client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )
    purge_response = client.purge(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )
    assert purge_response.HasField("purged_at_micros"), (
        "purge was a no-op; rows still served from hot"
    )


def _setup_composite_table(client) -> tuple[str, str, str, str]:
    """Create a catalog/schema/composite-PK table (declared order:
    region, name); returns the same ctx tuple shape as ``setup_schema``."""
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"point_read_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "point_read_schema",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="composite pk fixture",
    )
    table_uuid = client.create_table(
        "composite_table",
        _COMPOSITE_SCHEMA,
        primary_keys=["region", "name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="composite pk fixture",
    )
    return schema_uuid, table_uuid, catalog_uuid, main_branch_uuid


class TestReadDataIds:
    def test_ids_returns_exactly_named_row_and_latest_version(self):
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20, "carol": 30})

        result = _read_ids(client, ctx, _ids("alice"))
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]
        assert result.column("value").to_pylist() == [10]

        # Update alice; the ids read must return the latest version only.
        _seed(client, ctx, {"alice": 99})
        result = _read_ids(client, ctx, _ids("alice"))
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [99]

    def test_ids_multi_row_batch(self):
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20, "carol": 30})

        result = _read_ids(client, ctx, _ids("alice", "carol"))
        assert result.num_rows == 2
        assert sorted(result.column("name").to_pylist()) == ["alice", "carol"]

    def test_ids_deleted_and_missing_rows_absent(self):
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        _seed(client, ctx, {"alice": 10, "bob": 20})

        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, deletes=_ids("bob")),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cha-398 fixture",
        )

        assert _read_ids(client, ctx, _ids("bob")).num_rows == 0
        # Nonexistent PK: silently absent, no error (mirrors delete
        # idempotence).
        assert _read_ids(client, ctx, _ids("ghost")).num_rows == 0

    def test_ids_composes_with_filter(self):
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 2000})

        # AND semantics: the filter excludes the id'd row -> empty.
        assert (
            _read_ids(client, ctx, _ids("alice"), filter="value > 1000").num_rows == 0
        )
        # And keeps it when it matches.
        result = _read_ids(client, ctx, _ids("alice"), filter="value < 1000")
        assert result.column("name").to_pylist() == ["alice"]

    def test_ids_composes_with_projection(self):
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20})

        result = _read_ids(client, ctx, _ids("alice"), columns=["value"])
        assert result.num_rows == 1
        assert result.schema.names == ["value"]
        assert result.column("value").to_pylist() == [10]

    def test_ids_composes_with_as_of(self):
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )
        _seed(client, ctx, {"alice": 2})

        # As-of the first commit, the ids read sees the historical version.
        result = _read_ids(
            client,
            ctx,
            _ids("alice"),
            as_of=micros_to_datetime(committed1.commit_micros),
        )
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [1]

    def test_ids_ryow_open_tx(self):
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table(
                    {"name": ["ryow_alice"], "value": [101]}, schema=USER_SCHEMA
                ),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # The open tx sees its own uncommitted write through the ids read.
        ryow = _read_ids(client, ctx, _ids("ryow_alice"), open_tx_uuid=tx.tx_uuid)
        assert ryow.column("name").to_pylist() == ["ryow_alice"]

        # A reader without the open tx does not.
        assert _read_ids(client, ctx, _ids("ryow_alice")).num_rows == 0

        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

    def test_ids_after_persist_purge_cold_log(self):
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20})
        _persist_and_purge(client, ctx)

        # The row's versions now live only in the cold log; the ids
        # restriction must still find it (correctness across tiers).
        result = _read_ids(client, ctx, _ids("alice"))
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["alice"]

    def test_ids_mixed_tier_latest_wins(self):
        """Cold version + newer hot re-upsert: the ids read must return
        exactly one row carrying the hot value — cross-tier dedup under
        the pushdown."""
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20})
        _persist_and_purge(client, ctx)
        _seed(client, ctx, {"alice": 999})

        result = _read_ids(client, ctx, _ids("alice"))
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [999]

    def test_ids_after_snapshot(self):
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        _seed(client, ctx, {"alice": 10, "bob": 20})
        # Persist → Snapshot → Purge inline so the snapshot's real work is
        # asserted directly. CHA-444 (ADR 0027): Purge advances Pu only to
        # W_snap, so Snapshot must precede Purge; the rows then live in the
        # snapshot baseline with hot cleared.
        persist_response = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert persist_response.HasField("persisted_at_micros"), (
            "persist was a no-op; nothing moved to cold"
        )
        snapshot_response = client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
        )
        assert snapshot_response.HasField("snapshotted_at_micros"), (
            "snapshot was a no-op; nothing moved to the snapshot tier"
        )
        client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )

        result = _read_ids(client, ctx, _ids("bob"))
        assert result.num_rows == 1
        assert result.column("name").to_pylist() == ["bob"]
        assert result.column("value").to_pylist() == [20]

    def test_ids_cold_snapshot_seek_multi_and_overlay(self):
        """CHA-454: a cold snapshot point lookup. A multi-id read over the
        snapshot baseline returns the exact union; a post-snapshot update overlays
        via the change-log resolve (the index is a baseline accelerator, not a
        complete answer).

        Note: these are correctness assertions — the full-scan fallback returns
        the same rows, so they do not prove the index seek engaged. The
        cold_point_lookup_seek bench is the signal against silent fallback (a
        throughput collapse); a white-box seek-vs-scan counter is not exposed to
        the client."""
        client = make_client()
        ctx = setup_schema(client)
        _seed(client, ctx, {"alice": 10, "bob": 20, "carol": 30, "dave": 40})
        _persist_and_purge(client, ctx)

        # Multi-id seek over the snapshot baseline → exact union, no extras.
        result = _read_ids(client, ctx, _ids("alice", "carol"))
        got = dict(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        )
        assert got == {"alice": 10, "carol": 30}

        # Update carol AFTER the snapshot: the change-log resolve overlays the
        # baseline (new value), while unchanged alice still comes from the
        # snapshot seek.
        _seed(client, ctx, {"carol": 333})
        result = _read_ids(client, ctx, _ids("alice", "carol"))
        got = dict(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        )
        assert got == {"alice": 10, "carol": 333}

    def test_ids_composite_pk_declared_order(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = (
            _setup_composite_table(client)
        )
        batch = pa.table(
            {
                "region": ["us", "us", "eu"],
                "name": ["alice", "bob", "alice"],
                "value": [1, 2, 3],
            },
            schema=_COMPOSITE_SCHEMA,
        )
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cha-398 fixture",
        )

        ids = pa.table(
            {"region": ["eu"], "name": ["alice"]},
            schema=pa.schema(
                [pa.field("region", pa.utf8()), pa.field("name", pa.utf8())]
            ),
        )
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            ids=ids,
        )
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [3]


def _raw_read(ctx, ids_bytes: bytes) -> None:
    """Issue a raw-stub ReadData with hand-built ids bytes and drain the
    stream (errors surface on iteration)."""
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
    settings = ClientSettings()  # ty: ignore[missing-argument]
    stub = QueryServiceStub(insecure_channel(settings.query_url))
    request = ReadDataRequest(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
        ids=ids_bytes,
    )
    list(stub.ReadData(request))


def _ipc(table: pa.Table) -> bytes:
    return batch_to_ipc_bytes(table.combine_chunks().to_batches()[0])


def _assert_invalid_argument(excinfo) -> None:
    assert excinfo.value.code() == StatusCode.INVALID_ARGUMENT


class TestReadDataIdsValidation:
    def test_client_rejects_zero_row_ids(self):
        """0-row ids is ambiguous (restrict-to-nothing vs unrestricted);
        the client fails fast instead of inheriting Change.deletes'
        0-row-collapses-to-absent convention, which would silently
        invert a point read into a full-table read."""
        client = make_client()
        ctx = setup_schema(client)
        empty = pa.table({"name": pa.array([], pa.utf8())}, schema=_PK_ONLY_SCHEMA)
        with pytest.raises(ValueError):
            _read_ids(client, ctx, empty)

    def test_wrong_column_order_rejected(self):
        client = make_client()
        ctx = _setup_composite_table(client)

        # Declared order is (region, name); send (name, region).
        reversed_ids = pa.table(
            {"name": ["alice"], "region": ["eu"]},
            schema=pa.schema(
                [pa.field("name", pa.utf8()), pa.field("region", pa.utf8())]
            ),
        )
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, _ipc(reversed_ids))

        _assert_invalid_argument(excinfo)

    def test_wrong_column_type_rejected(self):
        client = make_client()
        ctx = setup_schema(client)
        wrong_type = pa.table(
            {"name": [1]}, schema=pa.schema([pa.field("name", pa.int64())])
        )
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, _ipc(wrong_type))

        _assert_invalid_argument(excinfo)

    def test_extra_column_rejected(self):
        client = make_client()
        ctx = setup_schema(client)
        extra = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, _ipc(extra))

        _assert_invalid_argument(excinfo)

    def test_malformed_ipc_rejected(self):
        client = make_client()
        ctx = setup_schema(client)
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, b"garbage, not arrow ipc")

        _assert_invalid_argument(excinfo)

    def test_null_pk_value_rejected(self):
        """A null in a PK column cannot derive a row identity; the
        server rejects it rather than silently matching the
        empty-string key (arrow display renders null as "")."""
        client = make_client()
        ctx = setup_schema(client)
        with_null = pa.table(
            {"name": pa.array([None], pa.utf8())},
            schema=pa.schema([pa.field("name", pa.utf8())]),
        )
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, _ipc(with_null))

        _assert_invalid_argument(excinfo)

    def test_upsert_null_pk_rejected(self):
        """The upsert path shares the null-PK guard: a user-shape batch
        with a NULL primary key is rejected instead of silently minting
        the empty-string row identity (CHA-398 kernel contract)."""
        client = make_client()
        ctx = setup_schema(client)
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        batch = pa.table(
            {"name": pa.array([None], pa.utf8()), "value": pa.array([1], pa.int64())},
            schema=USER_SCHEMA,
        )
        with pytest.raises(InvalidRequestError, match="null"):
            client.write_data(
                None,
                Mutation(table_uuid=table_uuid, upserts=batch),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_branch_uuid,
                author="test",
                comment="cha-398 fixture",
            )

    def test_present_zero_row_batch_rejected(self):
        """A non-empty IPC payload decoding to 0 total rows is rejected
        rather than silently treated as unrestricted."""
        client = make_client()
        ctx = setup_schema(client)
        ids_bytes = batch_to_ipc_bytes(
            pa.RecordBatch.from_arrays(
                [pa.array([], pa.utf8())], schema=_PK_ONLY_SCHEMA
            )
        )
        assert ids_bytes, "fixture must produce non-empty IPC bytes"
        with pytest.raises(RpcError) as excinfo:
            _raw_read(ctx, ids_bytes)

        _assert_invalid_argument(excinfo)


class TestAuditDataIds:
    def _seed_history(self, client, ctx):
        """alice: v1, v2, delete. bob: one version (noise)."""
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx

        tx1 = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        client.write_data(
            tx1.tx_uuid,
            Mutation(
                table_uuid=table_uuid,
                upserts=pa.table({"name": ["alice"], "value": [1]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        committed1 = client.commit_tx(
            tx1.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        _seed(client, ctx, {"alice": 2, "bob": 20})
        client.write_data(
            None,
            Mutation(table_uuid=table_uuid, deletes=_ids("alice")),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="test",
            comment="cha-398 fixture",
        )
        return committed1

    def _audit_ids(self, client, ctx, ids: pa.Table, **kwargs):
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = ctx
        return client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            ids=ids,
            **kwargs,
        )

    def test_ids_restricts_audit_to_named_row(self):
        client = make_client()
        ctx = setup_schema(client)
        self._seed_history(client, ctx)

        upserts, deletes = self._audit_ids(client, ctx, _ids("alice"))
        assert upserts.num_rows == 2
        assert set(upserts.column("name").to_pylist()) == {"alice"}
        assert sorted(upserts.column("value").to_pylist()) == [1, 2]
        assert deletes.num_rows == 1

    def test_ids_composes_with_committed_at_window(self):
        client = make_client()
        ctx = setup_schema(client)
        committed1 = self._seed_history(client, ctx)

        upserts, _deletes = self._audit_ids(
            client,
            ctx,
            _ids("alice"),
            after=micros_to_datetime(committed1.commit_micros + 1),
        )
        # The window excludes v1; only v2 remains.
        assert upserts.num_rows == 1
        assert upserts.column("value").to_pylist() == [2]

    def test_ids_audit_spans_tiers_after_persist(self):
        client = make_client()
        ctx = setup_schema(client)
        self._seed_history(client, ctx)
        _persist_and_purge(client, ctx)

        upserts, deletes = self._audit_ids(client, ctx, _ids("alice"))
        # Cold-resident versions still appear, still restricted to alice.
        assert upserts.num_rows == 2
        assert set(upserts.column("name").to_pylist()) == {"alice"}
        assert deletes.num_rows == 1


class TestHotLogRowUuidIndex:
    def test_row_uuid_leading_index_on_both_hot_logs(self):
        """The ids pushdown probes the hot logs by ``row_uuid`` below
        the latest-wins dedup; without a row_uuid-LEADING index those
        probes seq-scan (the existing ``idx_tx_<log>`` leads with
        ``tx_uuid`` — wrong column). The DDL comment in
        ``crates/penca-db/src/dialect/pg.rs`` deferred this index to
        CHA-398; this test pins its existence at table-creation time."""
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        driver = get_pg_driver()

        for log in (
            upsert_log_table(table_uuid, main_branch_uuid),
            delete_log_table(table_uuid, main_branch_uuid),
        ):
            rows = driver.execute(
                "SELECT indexname, indexdef FROM pg_indexes WHERE tablename = %s",
                (log,),
            )
            assert rows, f"no indexes found for hot log {log}"
            row_uuid_leading = [
                indexdef
                for _name, indexdef in rows
                if "(row_uuid, tx_uuid)" in indexdef
            ]
            assert row_uuid_leading, (
                f"hot log {log} lacks a (row_uuid, tx_uuid) index; "
                f"found: {[d for _n, d in rows]}"
            )


# CHA-426 — SQL point lookups must populate ``ReadDataRequest.ids``.
#
# CHA-398 added the ``ids`` PK-batch restriction but only for callers who
# populate it explicitly; the SQL path (PencaTableProvider::scan) never
# does. These tests pin the end-to-end behavior: a point
# SELECT / UPDATE / DELETE issued over Flight SQL must produce a
# query-container ``read_data`` span close with ``ids_rows=1`` — the
# penca-api servicer records the decoded id count on its span
# (crates/penca-api/src/query/mod.rs), which renders on the span's
# ``close time.busy=..`` log line under RUST_LOG=info,penca=debug +
# PENCA_SPAN_TIMING=1 (docker/test.env), the same scrape seam as the
# CHA-417 span-breakdown tests. This is deliberately the SERVER-side
# decode signal (wire carried ids + kernel accepted them), not a deeper
# tier span: hot-only ids reads do not traverse ``merge_read_parts``.
#
# RED before the CHA-426 wiring: the SQL path always sent empty ``ids``,
# so every ``read_data`` close line carried ``ids_rows=0``.

# Anchored INSIDE the read_data span's brace group so a future span
# that carries its own ids_rows under a read_data parent cannot satisfy
# the pin from the scope-chain prefix.
_READ_DATA_IDS_ONE_RE = re.compile(r"read_data\{[^}]*ids_rows=1(\D|$)")


def _poll_for_read_data_ids_close(
    since: int, deadline_seconds: float = 5.0
) -> tuple[int, int]:
    """Poll the query-container log window for ``read_data`` span CLOSE
    lines carrying ``ids_rows=1``.

    The ``ids_rows`` field lives on the penca-api ``read_data`` span
    (recorded after the server decodes the ids batch); the outer
    penca-server-grpc span of the same name carries no ``ids_rows``,
    so requiring the field on the line keeps the match precise.

    Returns ``(ids_close_count, any_close_count)`` once a match surfaces
    or the deadline lapses. ``any_close_count`` is the CHA-417-style
    sanity guard: if NO span CLOSE lines appear at all, the span-timing
    seam is misconfigured and the failure is a harness error, not a red
    assertion.
    """
    deadline = time.monotonic() + deadline_seconds
    ids_closes = 0
    any_closes = 0
    while time.monotonic() < deadline:
        lines = container_log("query")[since:].splitlines()
        ids_closes = sum(
            1
            for line in lines
            if "close time.busy" in line and _READ_DATA_IDS_ONE_RE.search(line)
        )
        any_closes = sum(1 for line in lines if "close time.busy" in line)
        if ids_closes >= 1:
            break

        time.sleep(0.2)

    return ids_closes, any_closes


def _sql_steps_via(
    driver: Literal["adbc", "jdbc"], steps: list[str], catalog: str
) -> list[tuple[str, str]]:
    """Run ``steps`` on one connection of ``driver``, pinned to ``catalog``.

    Thin wrapper over the flight-SQL suite's driver-parametrized step
    runner so both arms (ADBC prepared path, JDBC statement path) drive
    the same SQL through their real wire actions.
    """
    settings = ClientSettings()  # ty: ignore[missing-argument]
    assert settings.flight_sql_url is not None
    _host, _, port = settings.flight_sql_url.rpartition(":")
    return _execute_update_steps_via(driver, steps, port=port, catalog=catalog)


def _assert_ids_pushdown(since: int, context: str) -> None:
    ids_closes, any_closes = _poll_for_read_data_ids_close(since)
    assert any_closes >= 1, (
        "no span CLOSE lines at all in the query-log window — either "
        "PENCA_SPAN_TIMING is unset on the query container "
        "(docker/test.env) or no debug-level spans are enabled "
        "(RUST_LOG); harness/coverage issue, not a red result."
    )
    assert ids_closes >= 1, (
        f"expected >= 1 `read_data` span CLOSE with ids_rows=1 after {context}; "
        f"got 0 (window had {any_closes} CLOSE lines, so span timing works — "
        "the SQL path is not populating ReadDataRequest.ids)."
    )


class TestSqlPointLookupIdsPushdown:
    """CHA-426: point SQL statements restrict the read via the ids PK batch."""

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_point_select_pushes_ids(self, driver):
        setup_client = make_client()
        try:
            ctx = setup_with_data_named(setup_client)
        finally:
            setup_client.close()

        target = f"{ctx['schema_name']}.{ctx['table_name']}"
        since = len(container_log("query"))
        results = _sql_steps_via(
            driver,
            [f"SELECT value FROM {target} WHERE name = 'alice'"],
            ctx["catalog_name"],
        )
        status, payload = results[0]
        assert status == "OK_ROWS", results
        assert json.loads(payload) == [{"value": 10}], results

        _assert_ids_pushdown(since, f"a {driver} point SELECT")

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_point_update_pushes_ids(self, driver):
        setup_client = make_client()
        try:
            ctx = setup_with_data_named(setup_client)
        finally:
            setup_client.close()

        target = f"{ctx['schema_name']}.{ctx['table_name']}"
        # The UPDATE runs ALONE in its log window: its read-modify-write
        # SELECT is what must push ids — sharing a window with the
        # verification SELECT would let that query's own pushdown turn
        # this test green even if the DML read path regressed.
        since = len(container_log("query"))
        results = _sql_steps_via(
            driver,
            [f"UPDATE {target} SET value = 99 WHERE name = 'alice'"],
            ctx["catalog_name"],
        )
        assert results[0] == ("OK", "1"), results
        _assert_ids_pushdown(since, f"a {driver} point UPDATE")

        # Statements autocommit, so a second connection sees the change.
        results = _sql_steps_via(
            driver,
            [f"SELECT value FROM {target} WHERE name = 'alice'"],
            ctx["catalog_name"],
        )
        status, payload = results[0]
        assert status == "OK_ROWS", results
        assert json.loads(payload) == [{"value": 99}], results

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_point_delete_pushes_ids(self, driver):
        setup_client = make_client()
        try:
            ctx = setup_with_data_named(setup_client)
        finally:
            setup_client.close()

        target = f"{ctx['schema_name']}.{ctx['table_name']}"
        # DELETE alone in its window — same isolation rationale as the
        # UPDATE test: the pin is the DML pk_select read, not the
        # verification SELECT that follows.
        since = len(container_log("query"))
        results = _sql_steps_via(
            driver,
            [f"DELETE FROM {target} WHERE name = 'bob'"],
            ctx["catalog_name"],
        )
        assert results[0] == ("OK", "1"), results
        _assert_ids_pushdown(since, f"a {driver} point DELETE")

        results = _sql_steps_via(
            driver,
            [f"SELECT name FROM {target} WHERE name = 'bob'"],
            ctx["catalog_name"],
        )
        status, payload = results[0]
        assert status == "OK_ROWS", results
        assert json.loads(payload) == [], results
