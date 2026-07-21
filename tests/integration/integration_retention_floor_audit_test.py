"""CHA-433: plan-time retention-floor enforcement on ``audit_data`` (seq axis).

``audit_data`` reads the raw persist history. A committed-window lower bound
(``after_seq``) below the retention floor is rejected with
``FAILED_PRECONDITION`` (an explicit request for pruned-out history); an *unset*
lower bound means "all retained history" and is silently clamped up to the floor
(inclusive), so no returned row predates it.

Deterministic floor: two writes then a single snapshot make the snapshot's
watermark (the floor's ``commit_seq_num``) sit *above* the first row — so that
first row is genuinely below the floor and the clamp is observable. A short
retention window is allowed to elapse (a brief sleep) so the snapshot falls
below ``now - retention_duration``. Retention is set at the SCHEMA level.

RED until CHA-433's audit path lands: today no floor is enforced, so the
below-floor audit doesn't raise and the unset-from audit returns the
below-floor row. The null-floor guard passes now and must keep passing.
Run: ``just integration-test retention_floor_audit``.
"""

from __future__ import annotations

import time
from uuid import uuid4

import grpc
import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_proto.external.v1.common_pb2 import RetentionConfig
from penca_proto.external.v1.query_pb2 import AuditDataRequest
from penca_proto.external.v1.write_pb2 import CreateCatalogRequest, CreateSchemaRequest
from psycopg.sql import SQL, Identifier

from .integration_helpers import USER_SCHEMA, get_pg_driver, make_client

_WINDOW_SECONDS = 1
_WAIT_SECONDS = 3


def _setup(client, *, schema_retention: RetentionConfig | None) -> dict:
    cat = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"aflr_cat_{uuid4().hex[:8]}", owner="owner")
    )
    schema_req = CreateSchemaRequest(
        schema_name="aflr_schema",
        catalog_uuid=cat.catalog_uuid,
        branch_uuid=cat.main_branch_uuid,
        author="test",
        comment="cha-433",
    )
    if schema_retention is not None:
        schema_req.default_retention_config.CopyFrom(schema_retention)

    schema = client._write.CreateSchema(schema_req)
    table_uuid = client.create_table(
        "aflr_table",
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


def _write(client, ids: dict, n: int) -> None:
    """One write in its own committed tx (a distinct commit_seq_num)."""
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


def _persist_snapshot(client, ids: dict) -> None:
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


def _durable_floor_seq(ids: dict) -> int:
    """White-box: the durable snapshot's ``commit_seq_num`` (its watermark)."""
    parent = f"{ids['catalog_uuid']}_table_snapshot_metadata"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT commit_seq_num FROM {tbl} "
            "WHERE branch_uuid = %s AND table_uuid = %s "
            "AND durable AND commit_micros IS NOT NULL "
            "ORDER BY snapshotted_at_micros DESC, commit_seq_num DESC LIMIT 1"
        ).format(tbl=Identifier(parent)),
        (ids["branch_uuid"], ids["table_uuid"]),
    )
    assert rows, "expected a durable snapshot to exist"
    return rows[0][0]


def _seed_floor(client, *, retention: RetentionConfig | None) -> tuple[dict, int]:
    """Two writes -> one snapshot -> wait. Returns (ids, floor_seq). The first
    row's commit_seq_num is strictly below floor_seq (the snapshot watermark)."""
    ids = _setup(client, schema_retention=retention)
    _write(client, ids, 1)
    _write(client, ids, 2)
    _persist_snapshot(client, ids)
    floor_seq = _durable_floor_seq(ids)
    time.sleep(_WAIT_SECONDS)  # let the window elapse past the snapshot
    return ids, floor_seq


def _durable_floor_micros(ids: dict) -> int:
    """White-box: the durable snapshot's ``snapshotted_at_micros`` (the floor's
    micros coordinate)."""
    parent = f"{ids['catalog_uuid']}_table_snapshot_metadata"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT snapshotted_at_micros FROM {tbl} "
            "WHERE branch_uuid = %s AND table_uuid = %s "
            "AND durable AND commit_micros IS NOT NULL "
            "ORDER BY snapshotted_at_micros DESC, commit_seq_num DESC LIMIT 1"
        ).format(tbl=Identifier(parent)),
        (ids["branch_uuid"], ids["table_uuid"]),
    )
    assert rows, "expected a durable snapshot to exist"
    return rows[0][0]


def _audit_raw(
    client, ids: dict, *, seq_from=None, seq_to=None, micros_from=None, micros_to=None
) -> None:
    """Consume the raw AuditData stream (raises grpc.RpcError on error). The
    committed window is single-axis: pass seq_* OR micros_*, never both."""
    req = AuditDataRequest(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    if seq_from is not None:
        req.commit_seq_num.min = seq_from

    if seq_to is not None:
        req.commit_seq_num.max = seq_to

    if micros_from is not None:
        req.commit_micros.min = micros_from

    if micros_to is not None:
        req.commit_micros.max = micros_to

    for _response in client._query.AuditData(req):
        pass


def test_audit_floor_explicit_from_below_rejected():
    client = make_client()
    ids, floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    with pytest.raises(grpc.RpcError) as exc:
        _audit_raw(client, ids, seq_from=floor_seq - 1)

    err = exc.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "retention" in (err.details() or "").lower()


def test_audit_floor_explicit_from_at_floor_completes():
    client = make_client()
    ids, floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    # Exactly at the floor is accepted (strict `<`).
    _audit_raw(client, ids, seq_from=floor_seq)


def test_audit_floor_unset_from_clamped_to_floor():
    # Unset lower bound = "all retained history" -> clamp to the floor. The
    # below-floor first row must NOT appear.
    client = make_client()
    ids, floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    # before_seq (max) set, after_seq (min) unset -> the lower bound is clamped.
    # Bound the (exclusive) upper at floor_seq + 1 so it covers the at-floor row
    # without an arbitrary far-future offset (the snapshot watermark = floor_seq
    # is the latest committed row here).
    upserts, _deletes = client.audit_data(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
        before_seq=floor_seq + 1,
    )
    seqs = upserts.column("commit_seq_num").to_pylist()
    assert seqs, "expected the at/above-floor row to be returned"
    assert min(seqs) >= floor_seq, (
        f"unset-from audit must clamp to the floor {floor_seq}; got {sorted(seqs)}"
    )


def test_audit_floor_micros_explicit_from_below_rejected():
    # The micros axis mirrors the seq axis: an explicit commit_micros lower
    # bound below the floor's snapshotted_at_micros is rejected.
    client = make_client()
    ids, _floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    floor_micros = _durable_floor_micros(ids)
    with pytest.raises(grpc.RpcError) as exc:
        _audit_raw(client, ids, micros_from=floor_micros - 1)

    err = exc.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "retention" in (err.details() or "").lower()


def test_audit_floor_micros_at_floor_completes():
    client = make_client()
    ids, _floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    floor_micros = _durable_floor_micros(ids)
    # Exactly at the floor is accepted (strict `<`).
    _audit_raw(client, ids, micros_from=floor_micros)


def test_audit_floor_micros_unset_from_clamped():
    # A fully-unbounded audit defaults to the micros axis, so its unset lower
    # bound clamps to floor_micros (the default/high-traffic clamp branch). The
    # floor's snapshotted_at_micros is the snapshot watermark = the at-floor
    # row's commit_micros, so the clamp is inclusive there: it drops the
    # below-floor row and keeps the at-floor row (strict `<` rejects below).
    client = make_client()
    ids, _floor_seq = _seed_floor(
        client,
        retention=RetentionConfig(
            retention_duration_seconds=_WINDOW_SECONDS, snapshot_density_seconds=1
        ),
    )
    floor_micros = _durable_floor_micros(ids)
    upserts, _deletes = client.audit_data(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    micros = upserts.column("commit_micros").to_pylist()
    assert micros, "expected the at/above-floor row to be returned"
    assert min(micros) >= floor_micros, (
        f"unset-from micros audit must clamp to the floor {floor_micros}; "
        f"got {sorted(micros)}"
    )


def test_audit_floor_null_when_retention_disabled():
    # No retention -> floor is None -> a below-any-floor audit is a no-op.
    client = make_client()
    ids, floor_seq = _seed_floor(client, retention=None)
    _audit_raw(client, ids, seq_from=max(floor_seq - 1, 0))
