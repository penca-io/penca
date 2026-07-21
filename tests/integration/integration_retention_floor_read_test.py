"""CHA-433: plan-time retention-floor enforcement on ``read_data`` (both axes).

The retention floor is the newest ``durable`` snapshot at/before the window
start (``now - retention_duration_seconds``). A read whose ``as_of`` falls below
the floor is rejected with gRPC ``FAILED_PRECONDITION`` — an error, not a clamp
(a clamped answer is data at a different instant than the caller asked for). The
boundary is strict ``<`` (the floor itself is served) and per-axis: a seq
``as_of`` compares against the floor's ``commit_seq_num``, a micros ``as_of``
against its ``snapshotted_at_micros``.

Making the floor real (not backdated): a snapshot backdated below the window
would also predate the table's own creation, so the read would fail NOT_FOUND
before the floor check runs. Instead we use a short retention window and let it
elapse (a brief sleep), so a genuinely recent durable snapshot — created *after*
the table exists — falls below ``now - retention_duration``. Retention is set at
the SCHEMA level (scope-B: schema is the broadest scope).

RED until CHA-433 lands: today no floor is enforced, so the below-floor reads
return data instead of raising — the ``pytest.raises`` assertions fire. The
null-floor guards (retention disabled / table younger than the window) pass now
and must keep passing. Run: ``just integration-test retention_floor_read``.
"""

from __future__ import annotations

import time
from uuid import uuid4

import grpc
import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_proto.external.v1.common_pb2 import RetentionConfig
from penca_proto.external.v1.query_pb2 import ReadDataRequest
from penca_proto.external.v1.write_pb2 import CreateCatalogRequest, CreateSchemaRequest
from psycopg.sql import SQL, Identifier

from .integration_helpers import USER_SCHEMA, get_pg_driver, make_client

# Short retention window (seconds) + a slightly longer wait, so the durable
# snapshot created below is comfortably older than ``now - _WINDOW_SECONDS`` by
# read time — it becomes the floor with margin to spare against clock skew.
_WINDOW_SECONDS = 1
_WAIT_SECONDS = 3


def _setup(client, *, schema_retention: RetentionConfig | None) -> dict:
    """Catalog (no retention) + schema (optional retention) + one table."""
    cat = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"floor_cat_{uuid4().hex[:8]}", owner="owner")
    )
    schema_req = CreateSchemaRequest(
        schema_name="floor_schema",
        catalog_uuid=cat.catalog_uuid,
        branch_uuid=cat.main_branch_uuid,
        author="test",
        comment="cha-433",
    )
    if schema_retention is not None:
        schema_req.default_retention_config.CopyFrom(schema_retention)

    schema = client._write.CreateSchema(schema_req)
    table_uuid = client.create_table(
        "floor_table",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=cat.catalog_uuid,
        schema_uuid=schema.schema_uuid,
        branch_uuid=cat.main_branch_uuid,
        author="test",
        comment="cha-433",
    )
    return {
        "catalog_uuid": cat.catalog_uuid,
        "schema_uuid": schema.schema_uuid,
        "branch_uuid": cat.main_branch_uuid,
        "table_uuid": table_uuid,
    }


def _cycle(client, ids: dict, n: int) -> None:
    """One write -> commit -> persist -> snapshot cycle with distinct data."""
    tx = client.begin_tx(
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    batch = pa.table({"name": [f"row{n}"], "value": [n]}, schema=USER_SCHEMA)
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=ids["table_uuid"], upserts=batch),
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    client.commit_tx(
        tx.tx_uuid, catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )
    client.persist(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    client.snapshot(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )


def _durable_floor(ids: dict) -> tuple[int, int]:
    """White-box read of the (single) durable snapshot's ``(seq, micros)``."""
    parent = f"{ids['catalog_uuid']}_table_snapshot_metadata"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT commit_seq_num, snapshotted_at_micros FROM {tbl} "
            "WHERE branch_uuid = %s AND table_uuid = %s "
            "AND durable AND commit_micros IS NOT NULL "
            "ORDER BY commit_seq_num LIMIT 1"
        ).format(tbl=Identifier(parent)),
        (ids["branch_uuid"], ids["table_uuid"]),
    )
    assert rows, "expected a durable snapshot to exist"
    return rows[0][0], rows[0][1]


def _read(client, ids: dict, *, commit_micros=None, commit_seq_num=None) -> list:
    """Raw ``ReadData`` over the query stub, materialized (raises on error).

    The floor error surfaces as the first stream item, so consuming the stream
    with ``list`` triggers it — unlike the facade, this gives exact ``as_of``
    control (no ``datetime`` rounding) and the raw gRPC status.
    """
    req = ReadDataRequest(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    if commit_micros is not None:
        req.commit_micros = commit_micros

    if commit_seq_num is not None:
        req.commit_seq_num = commit_seq_num

    return list(client._query.ReadData(req))


def _read_by_name(client, ids: dict, *, commit_seq_num=None) -> list:
    """Like `_read`, but resolves the table BY NAME so `ResolvedScope` populates
    `schema_row` — exercising the zero-roundtrip retention path (the SQL server's
    by-name resolve) rather than the by-uuid fetch fallback.
    """
    req = ReadDataRequest(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        schema_uuid=ids["schema_uuid"],
        table_name="floor_table",
    )
    if commit_seq_num is not None:
        req.commit_seq_num = commit_seq_num

    return list(client._query.ReadData(req))


def _assert_below_floor_rejected(client, ids: dict, **as_of) -> None:
    with pytest.raises(grpc.RpcError) as exc:
        _read(client, ids, **as_of)

    err = exc.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "retention" in (err.details() or "").lower()


def test_read_floor_seq_axis():
    client = make_client()
    ids = _setup(
        client,
        schema_retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    _cycle(client, ids, 1)
    floor_seq, _ = _durable_floor(ids)
    time.sleep(_WAIT_SECONDS)  # let the window elapse past the durable snapshot

    _assert_below_floor_rejected(client, ids, commit_seq_num=floor_seq - 1)
    _read(client, ids, commit_seq_num=floor_seq)  # exact floor accepted
    _read(client, ids)  # latest — above the floor, and a normal read is unaffected


def test_read_floor_by_name_uses_scope_schema_retention():
    # By-name resolve populates scope.schema_row, so retention is read from it
    # with no extra roundtrip (the SQL server's path). The floor must still fire.
    client = make_client()
    ids = _setup(
        client,
        schema_retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    _cycle(client, ids, 1)
    floor_seq, _ = _durable_floor(ids)
    time.sleep(_WAIT_SECONDS)

    with pytest.raises(grpc.RpcError) as exc:
        _read_by_name(client, ids, commit_seq_num=floor_seq - 1)

    err = exc.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    _read_by_name(client, ids, commit_seq_num=floor_seq)  # exact floor accepted


def test_read_floor_micros_axis():
    client = make_client()
    ids = _setup(
        client,
        schema_retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    _cycle(client, ids, 1)
    _, floor_micros = _durable_floor(ids)
    time.sleep(_WAIT_SECONDS)

    _assert_below_floor_rejected(client, ids, commit_micros=floor_micros - 1)
    _read(client, ids, commit_micros=floor_micros)  # exact floor accepted
    _read(client, ids)  # latest — above the floor, and a normal read is unaffected


def test_read_floor_null_when_retention_disabled():
    # No retention anywhere -> floor is None -> a below-any-floor read is a no-op.
    client = make_client()
    ids = _setup(client, schema_retention=None)
    _cycle(client, ids, 1)
    floor_seq, floor_micros = _durable_floor(ids)
    _read(client, ids, commit_seq_num=floor_seq - 1)
    _read(client, ids, commit_micros=floor_micros - 1)


def test_read_floor_null_when_table_younger_than_window():
    # Retention set with a LONG window, so the just-created durable is inside it
    # (no durable precedes the window) -> floor is None -> read unaffected.
    client = make_client()
    ids = _setup(
        client,
        schema_retention=RetentionConfig(
            retention_duration_seconds=3600, snapshot_density_seconds=1
        ),
    )
    _cycle(client, ids, 1)
    floor_seq, floor_micros = _durable_floor(ids)
    _read(client, ids, commit_seq_num=floor_seq - 1)
    _read(client, ids, commit_micros=floor_micros - 1)
