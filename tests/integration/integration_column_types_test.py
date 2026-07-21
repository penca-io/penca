"""Integration tests for CHA-386 — canonical Arrow<->Postgres type registry.

Three acceptance classes, one per ticket criterion:

* :class:`TestColumnTypeGapReproduction` (R1) — the four already-declarable
  types (DATE/DECIMAL/SMALLINT/TIMESTAMP) round-trip CREATE -> INSERT ->
  SELECT through the hot store via Flight SQL. **Fails today**: the SQL
  ``INSERT`` reaches ``data.rs::insert_upserts`` -> ``row_codec::
  arrow_to_sql_literal``, which has no arm for ``Int16``/``Decimal128``/
  ``Date32``/``Timestamp`` (the *same* gap that breaks ``rows_to_batch`` on
  read). Both sides are closed by T_codec.

* :class:`TestWidenedTypeRoundTrip` (R2) — the widened Arrow set that only
  the gRPC ``create_table`` door can declare (unsigned ints, Int8, Float16,
  Large/View strings+binary, Decimal256, Date64, Time, Large/FixedSize
  lists) round-trips through the hot store; tz-aware ``Timestamp`` rides the
  Flight SQL ``TIMESTAMP WITH TIME ZONE`` path. **Fails today**: the wider
  types either reject at ``arrow_type_to_sql`` (FixedSizeList) on CREATE, or
  break at ``arrow_to_sql_literal`` on write / ``rows_to_batch`` on read.
  Each family is closed by its Phase-2 widen task.

* :class:`TestUnsupportedTypeRejection` (R3) — an unsupported column type
  (``Struct``) is rejected at the **single** gRPC ``WriteService::
  create_table`` gate with wording citing the canonical registry, and the
  SQL token-translation layer rejects with registry-citing wording too
  (a *separate* layer, not a shared gate). **Fails today**: gRPC
  ``create_table`` validates only primary keys, not column types.

Run via ``just integration-test column_types``.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Literal
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.config import ClientSettings
from penca_client.errors import InvalidRequestError

from .integration_flight_sql_test import (
    _ensure_public_catalog_and_schema,
    _execute_query_via,
    _execute_update_steps_via,
)
from .integration_helpers import make_client

# Wording fragment every CHA-386 rejection must carry — a substring of the
# canonical registry's module path. Deliberately the dash/underscore-
# agnostic core (`core::types`) so it matches both the human-authored prose
# form (`penca-core::types`) and a `module_path!()`-derived Rust form
# (`penca_core::types`). Asserted for the gRPC gate and the SQL
# translation layer alike (different layers, both citing the registry).
_REGISTRY_CITE = "core::types"


def _flight_port() -> str:
    settings = ClientSettings()  # ty: ignore[missing-argument]
    assert settings.flight_sql_url is not None
    _host, _, port = settings.flight_sql_url.rpartition(":")
    return port


def _grpc_round_trip(
    schema: pa.Schema, batch: pa.Table, *, persist_to_cold: bool = False
) -> pa.Table:
    """Create a table with ``schema`` via gRPC, upsert ``batch``, read it
    back through the canonical read path. Returns the materialized Table.

    Isolates the hot row codec: ``write_data`` writes via
    ``arrow_to_sql_literal`` and ``read_data`` rebuilds via
    ``rows_to_batch`` — both keyed on the stored Arrow schema, so the
    returned Table's types must equal ``schema`` exactly (identity
    round-trip — the registry must not collapse e.g. LargeUtf8 -> Utf8).

    With ``persist_to_cold=True`` the committed rows are flushed to cold
    (``persist``) and the hot copies dropped (``purge``) before the read,
    so the read is served entirely from the cold tier — exercising the
    Parquet/Lance writers + readers on the widened set (CHA-386 Phase 3).
    """
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"cha386_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "cha386_schema",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="create_schema",
    )
    pk = schema.field(0).name
    table_uuid = client.create_table(
        f"t_{uuid4().hex[:8]}",
        schema,
        primary_keys=[pk],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="create_table",
    )
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=batch),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
    )
    if persist_to_cold:
        # Flush to cold (Parquet/Lance write) then drop the hot copies so
        # the subsequent read is served only from the cold tier. Assert
        # both watermarks advanced — a silent persist/purge no-op would
        # leave the data in hot and the round-trip would never touch the
        # cold writers, defeating the test.
        persist_resp = client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert persist_resp.persisted_at_micros > 0, (
            "persist was a no-op; cold tier not exercised"
        )
        # CHA-444 (ADR 0027): Purge advances the read fence Pu only to W_snap,
        # so a Snapshot must run first for Purge to drop the hot rows
        # (otherwise it no-ops and the read falls back to the hot copy).
        client.snapshot(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        purge_resp = client.purge(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            table_uuid=table_uuid,
        )
        assert purge_resp.purged_at_micros > 0, (
            "purge was a no-op; read would fall back to the hot copy"
        )

    return client.read_data(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        table_uuid=table_uuid,
    )


def _assert_field_types_preserved(out: pa.Table, expected: pa.Schema) -> None:
    """Assert the read-back column *types* equal the declared types,
    field-for-field. Compares ``field.type`` (not ``Schema.equals``) so the
    identity check — the registry must not collapse e.g. LargeUtf8 -> Utf8 —
    is not coupled to nullability metadata (a PK column may be normalized to
    non-nullable by the write path without that being a type-registry bug).
    """
    got = [(f.name, f.type) for f in out.schema]
    want = [(f.name, f.type) for f in expected]
    assert got == want, f"type drift (collapse?): got {got}, want {want}"


# ── R1 — the declarable-but-un-round-trippable gap ─────────────────────


@pytest.mark.parametrize("driver", ["adbc", "jdbc"])
class TestColumnTypeGapReproduction:
    """CHA-386 baseline: the four types the DDL gate already admits must
    round-trip through the hot store. Same SQL through both drivers
    (ADBC ``DoPutStatementUpdate`` / JDBC ``ActionCreatePreparedStatement``
    + ``DoPutPreparedStatementUpdate`` on the INSERT; ADBC
    ``CommandPreparedStatementQuery`` / JDBC ``CommandStatementQuery`` on
    the SELECT), mirroring TestFlightSqlCreateTableAutoCommitEndToEnd.
    """

    def test_date_decimal_smallint_timestamp_round_trip(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        port = _flight_port()

        schema = f"cha386_gap_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.t"
        steps = [
            f"CREATE SCHEMA {schema}",
            (
                f"CREATE TABLE {fqn} "
                f"(id BIGINT, d DATE, dec DECIMAL(10,2), s SMALLINT, "
                f"ts TIMESTAMP, PRIMARY KEY(id))"
            ),
            (
                f"INSERT INTO {fqn} VALUES "
                f"(1, DATE '2024-01-15', 123.45, 42, "
                f"TIMESTAMP '2024-01-15 12:30:00')"
            ),
        ]
        results = _execute_update_steps_via(driver, steps, port=port)
        for i, (status, payload) in enumerate(results):
            assert status == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; "
                f"got ({status}, {payload}). Today the INSERT breaks at "
                f"row_codec::arrow_to_sql_literal (no Int16/Decimal128/"
                f"Date32/Timestamp arm)."
            )

        rows = _execute_query_via(
            driver, f"SELECT id, d, dec, s, ts FROM {fqn} ORDER BY id", port=port
        )
        assert len(rows) == 1, rows
        row = rows[0]
        assert row["id"] == 1
        assert int(row["s"]) == 42
        # d/dec/ts: JSON encodings differ across drivers; assert presence +
        # the salient value substring so the check holds for both arms.
        assert row["d"] is not None and "2024-01-15" in str(row["d"])
        assert row["dec"] is not None and "123.45" in str(row["dec"])
        assert row["ts"] is not None and "2024-01-15" in str(row["ts"])


# ── R2 — the widened core set ──────────────────────────────────────────


class TestWidenedTypeRoundTrip:
    """Each Phase-2 family round-trips through the hot store via the gRPC
    door (the only door that can declare unsigned/Large/View/FixedSize
    types). Identity round-trip: the read-back schema must equal the
    declared schema field-for-field.
    """

    def test_unsigned_ints_and_int8_round_trip(self) -> None:
        # UInt64 carries u64::MAX -> PG NUMERIC (the lossy-BIGINT correction).
        schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("u8", pa.uint8()),
                pa.field("u16", pa.uint16()),
                pa.field("u32", pa.uint32()),
                pa.field("u64", pa.uint64()),
                pa.field("i8", pa.int8()),
            ]
        )
        batch = pa.table(
            {
                "name": ["a"],
                "u8": pa.array([255], pa.uint8()),
                "u16": pa.array([65535], pa.uint16()),
                "u32": pa.array([4294967295], pa.uint32()),
                "u64": pa.array([18446744073709551615], pa.uint64()),
                "i8": pa.array([-128], pa.int8()),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch)
        _assert_field_types_preserved(out, schema)
        assert out.column("u64")[0].as_py() == 18446744073709551615

    def test_float16_and_decimal256_round_trip(self) -> None:
        schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("f16", pa.float16()),
                pa.field("d256", pa.decimal256(40, 10)),
            ]
        )
        batch = pa.table(
            {
                "name": ["a"],
                "f16": pa.array([1.5], pa.float16()),
                "d256": pa.array(
                    [decimal.Decimal("123.4567890123")], pa.decimal256(40, 10)
                ),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch)
        _assert_field_types_preserved(out, schema)

    def test_large_and_view_strings_and_binary_round_trip(self) -> None:
        schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("lutf8", pa.large_utf8()),
                pa.field("utf8v", pa.string_view()),
                pa.field("lbin", pa.large_binary()),
                pa.field("binv", pa.binary_view()),
            ]
        )
        batch = pa.table(
            {
                "name": ["a"],
                "lutf8": pa.array(["x"], pa.large_utf8()),
                "utf8v": pa.array(["y"], pa.string_view()),
                "lbin": pa.array([b"\x01\x02"], pa.large_binary()),
                "binv": pa.array([b"\x03"], pa.binary_view()),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch)
        # Identity: Large/View variants must NOT collapse to Utf8/Binary.
        _assert_field_types_preserved(out, schema)

    def test_date64_and_time_round_trip(self) -> None:
        schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("d64", pa.date64()),
                pa.field("t32", pa.time32("ms")),
                pa.field("t64", pa.time64("us")),
            ]
        )
        batch = pa.table(
            {
                "name": ["a"],
                "d64": pa.array([1705276800000], pa.date64()),  # 2024-01-15
                "t32": pa.array([45000000], pa.time32("ms")),  # 12:30:00
                "t64": pa.array([45000000000], pa.time64("us")),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch)
        _assert_field_types_preserved(out, schema)

    def test_large_and_fixed_size_list_round_trip(self) -> None:
        schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("llist", pa.large_list(pa.int32())),
                pa.field("flist", pa.list_(pa.int32(), 2)),
            ]
        )
        batch = pa.table(
            {
                "name": ["a"],
                "llist": pa.array([[1, 2, 3]], pa.large_list(pa.int32())),
                "flist": pa.array([[4, 5]], pa.list_(pa.int32(), 2)),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch)
        _assert_field_types_preserved(out, schema)

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_tz_aware_timestamp_round_trip(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        """tz-aware TIMESTAMP -> PG TIMESTAMPTZ, read back as
        Timestamp(_, Some(tz)). Today sql_type_to_arrow rejects
        ``TIMESTAMP WITH TIME ZONE`` at the DDL gate.
        """
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        port = _flight_port()

        schema = f"cha386_tz_{driver}_{uuid4().hex[:12]}"
        fqn = f"{schema}.t"
        steps = [
            f"CREATE SCHEMA {schema}",
            (
                f"CREATE TABLE {fqn} "
                f"(id BIGINT, ts TIMESTAMP WITH TIME ZONE, PRIMARY KEY(id))"
            ),
            (
                f"INSERT INTO {fqn} VALUES "
                f"(1, TIMESTAMP WITH TIME ZONE '2024-01-15 12:30:00+00')"
            ),
        ]
        results = _execute_update_steps_via(driver, steps, port=port)
        for i, (status, payload) in enumerate(results):
            assert status == "OK", (
                f"[{driver}] step {i} ({steps[i]!r}) expected OK; "
                f"got ({status}, {payload}). Today the CREATE rejects "
                f"TIMESTAMP WITH TIME ZONE at sql_type_to_arrow."
            )

        rows = _execute_query_via(
            driver, f"SELECT id, ts FROM {fqn} ORDER BY id", port=port
        )
        assert len(rows) == 1, rows
        assert rows[0]["ts"] is not None and "2024-01-15" in str(rows[0]["ts"])


# ── Phase 3 — cold-tier round-trip of the widened set ──────────────────


class TestWidenedColdRoundTrip:
    """The widened set survives a flush to cold (Parquet/Lance writers)
    and reads back identically. ``persist`` + ``purge`` force the read to
    be served only from the cold tier, so this exercises the cold
    write+read path (``penca-format`` readers/writers) on the widened
    types — including the Lance edge cases (Float16, Decimal256, Date64).
    If a cold writer cannot represent a type it fails loud at its
    boundary (a typed ``FormatError``) rather than corrupting silently.
    """

    def test_widened_set_round_trips_through_cold(self) -> None:
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("u32", pa.uint32()),
                pa.field("f16", pa.float16()),
                pa.field("d128", pa.decimal128(10, 2)),
                pa.field("d256", pa.decimal256(40, 10)),
                pa.field("d64", pa.date64()),
                pa.field("ts", pa.timestamp("us")),
                pa.field("lst", pa.list_(pa.int32())),
                pa.field("lutf8", pa.large_utf8()),
            ]
        )
        batch = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "u32": pa.array([4294967295], pa.uint32()),
                "f16": pa.array([1.5], pa.float16()),
                "d128": pa.array([decimal.Decimal("123.45")], pa.decimal128(10, 2)),
                "d256": pa.array(
                    [decimal.Decimal("123.4567890123")], pa.decimal256(40, 10)
                ),
                "d64": pa.array([1705276800000], pa.date64()),  # 2024-01-15
                "ts": pa.array([1705321800000000], pa.timestamp("us")),
                "lst": pa.array([[1, 2, 3]], pa.list_(pa.int32())),
                "lutf8": pa.array(["x"], pa.large_utf8()),
            },
            schema=schema,
        )
        out = _grpc_round_trip(schema, batch, persist_to_cold=True)
        # Types survive the cold write/read (no collapse) and the row is
        # recovered from cold after the hot copy was purged.
        _assert_field_types_preserved(out, schema)
        assert out.num_rows == 1
        # Value fidelity through the cold writers, not just type preservation
        # — catches scale drift / precision loss the Lance edge cases risk.
        assert out.column("u32")[0].as_py() == 4294967295
        assert out.column("f16")[0].as_py() == 1.5
        assert out.column("d128")[0].as_py() == decimal.Decimal("123.45")
        assert out.column("d256")[0].as_py() == decimal.Decimal("123.4567890123")
        assert out.column("d64")[0].as_py() == datetime.date(2024, 1, 15)
        assert out.column("ts")[0].as_py() == datetime.datetime(2024, 1, 15, 12, 30, 0)
        assert out.column("lst")[0].as_py() == [1, 2, 3]


# ── R3 — single-gate rejection of unsupported types ────────────────────


class TestUnsupportedTypeRejection:
    """The supported-set gate lives at exactly one place: the gRPC
    ``WriteService::create_table`` servicer. The SQL token-translation
    layer (``sql_type_to_arrow``) rejects unmappable tokens separately —
    a different layer, both citing the registry, NOT a shared gate.
    """

    def test_struct_column_rejected_at_grpc_gate(self) -> None:
        client = make_client()
        catalog_uuid, _ = client.create_catalog(
            f"cha386_rej_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "rej_schema",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="create_schema",
        )
        struct_schema = pa.schema(
            [
                pa.field("name", pa.utf8()),
                pa.field("s", pa.struct([pa.field("a", pa.int32())])),
            ]
        )
        # Single gate: an unsupported Arrow type is rejected with
        # registry-citing wording, regardless of which door it arrived
        # through. Today create_table validates only primary keys.
        with pytest.raises(InvalidRequestError) as ei:
            client.create_table(
                "struct_t",
                struct_schema,
                primary_keys=["name"],
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                author="test",
                comment="create_table",
            )

        assert _REGISTRY_CITE in str(ei.value), (
            f"gate rejection must cite the registry; got: {ei.value}"
        )

    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_sql_translation_layer_rejects_with_registry_cite(
        self, driver: Literal["adbc", "jdbc"]
    ) -> None:
        """A SQL type token with no Arrow mapping (e.g. UUID) is rejected
        at the translation layer (``sql_type_to_arrow``) with
        registry-citing wording — a separate layer from the gRPC gate.
        """
        client = make_client()
        _ensure_public_catalog_and_schema(client)
        client.close()
        port = _flight_port()

        schema = f"cha386_sqlrej_{driver}_{uuid4().hex[:12]}"
        steps = [
            f"CREATE SCHEMA {schema}",
            f"CREATE TABLE {schema}.t (id BIGINT, u UUID, PRIMARY KEY(id))",
        ]
        results = _execute_update_steps_via(driver, steps, port=port)
        # CREATE SCHEMA ok; the CREATE TABLE step must reject.
        create_status, create_payload = results[-1]
        assert create_status != "OK", (
            f"[{driver}] CREATE TABLE with UUID must reject; got OK"
        )
        assert _REGISTRY_CITE in str(create_payload), (
            f"[{driver}] SQL translation rejection must cite the registry; "
            f"got: {create_payload}"
        )
