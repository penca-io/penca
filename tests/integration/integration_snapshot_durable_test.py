"""CHA-432: ``durable`` sticky assignment on ``table_snapshot_metadata``.

A snapshot is stamped ``durable`` once, at creation, iff it is the first
snapshot (no prior durable rung), or density is unset (keep-all-in-window),
or its watermark is at least ``snapshot_density_seconds`` past the last
durable rung. The decision is sticky — never recomputed — so the retention
floor stays monotonic.

The density-boundary *arithmetic* is pinned exhaustively by the Rust
``decide_durable`` matrix (fast, deterministic). This end-to-end test pins the
*wiring*: the Snapshot op resolves the effective density, reads the last
durable watermark, applies the decision, and persists the flag. It stays
wall-clock-free by using a density (3600s) far larger than any real gap
between snapshots in the run, so every snapshot after the first is
density-gated to ``durable=false``; the unset-density case flips them all to
``true``.

RED before the column + assignment land: the ``durable`` projection raises
``UndefinedColumn``. Run: ``just integration-test snapshot_durable``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_proto.external.v1.common_pb2 import RetentionConfig
from penca_proto.external.v1.write_pb2 import CreateCatalogRequest, CreateSchemaRequest
from psycopg.sql import SQL, Identifier

from .integration_helpers import USER_SCHEMA, get_pg_driver, make_client


def _setup_table(client, density_seconds: int | None) -> dict:
    """Create catalog + schema (with optional density) + table; return ids.

    CHA-433: retention is schema-broadest, so the snapshot density is set on the
    schema, not the catalog (which no longer carries a retention policy).
    """
    resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"dur_cat_{uuid4().hex[:8]}", owner="owner")
    )
    catalog_uuid = resp.catalog_uuid
    branch_uuid = resp.main_branch_uuid

    schema_req = CreateSchemaRequest(
        schema_name="dur_schema",
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
        author="test",
        comment="cha-432",
    )
    if density_seconds is not None:
        schema_req.default_retention_config.CopyFrom(
            RetentionConfig(snapshot_density_seconds=density_seconds)
        )

    schema_uuid = client._write.CreateSchema(schema_req).schema_uuid
    table_uuid = client.create_table(
        "dur_table",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha-432",
    )
    return {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": branch_uuid,
        "table_uuid": table_uuid,
    }


def _write_persist_snapshot(client, ids: dict, n: int) -> int:
    """One cycle: write a distinct row, commit, persist to cold, snapshot.

    Distinct data per cycle so neither persist nor snapshot is a no-op — each
    snapshot lands at a strictly later watermark than the last.
    """
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
    persisted = client.persist(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    assert persisted.HasField("persisted_at_micros"), (
        "persist was a no-op; fixture did not move rows cold"
    )
    snap = client.snapshot(
        catalog_uuid=ids["catalog_uuid"],
        branch_uuid=ids["branch_uuid"],
        table_uuid=ids["table_uuid"],
    )
    assert snap.HasField("snapshotted_at_micros"), (
        "snapshot was a no-op; no new persist data since last snapshot"
    )
    return snap.snapshotted_at_micros


def _durable_rows(ids: dict) -> list[tuple[bool, int]]:
    """White-box read of (durable, snapshotted_at_micros), oldest first."""
    parent = f"{ids['catalog_uuid']}_table_snapshot_metadata"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT durable, snapshotted_at_micros FROM {tbl} "
            "WHERE branch_uuid = %s AND table_uuid = %s "
            "AND commit_micros IS NOT NULL ORDER BY snapshotted_at_micros"
        ).format(tbl=Identifier(parent)),
        (ids["branch_uuid"], ids["table_uuid"]),
    )
    return [(bool(r[0]), r[1]) for r in rows]


def test_durable_first_then_density_gated_and_sticky():
    client = make_client()
    # Density 3600s ≫ any real gap between snapshots in this run, so only the
    # first snapshot (no prior durable) is durable; the rest are density-gated.
    ids = _setup_table(client, density_seconds=3600)

    for n in (1, 2, 3):
        _write_persist_snapshot(client, ids, n)

    durables = [d for d, _ in _durable_rows(ids)]
    assert durables == [True, False, False], (
        f"first snapshot durable, later ones density-gated; got {durables}"
    )

    # Sticky: creating a later snapshot must not recompute earlier flags. Run
    # the assignment path once more (a 4th snapshot, itself density-gated to
    # False) and assert the first three rows are unchanged.
    _write_persist_snapshot(client, ids, 4)
    durables_after = [d for d, _ in _durable_rows(ids)]
    assert durables_after == [True, False, False, False], (
        f"earlier durable flags must stay sticky as new snapshots land; "
        f"got {durables_after}"
    )


def test_unset_density_makes_every_snapshot_durable():
    client = make_client()
    # No retention config at any level ⇒ density unset ⇒ every snapshot a rung.
    ids = _setup_table(client, density_seconds=None)

    for n in (1, 2):
        _write_persist_snapshot(client, ids, n)

    durables = [d for d, _ in _durable_rows(ids)]
    assert durables == [True, True], (
        f"unset density ⇒ every snapshot durable; got {durables}"
    )
