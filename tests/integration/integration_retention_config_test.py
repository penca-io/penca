"""CHA-495 — RetentionConfig reshape: seconds units + snapshot_density_seconds.

Two acceptance surfaces:

* RT1 (round-trip) — a RetentionConfig set with ``retention_duration_seconds`` +
  ``snapshot_density_seconds`` round-trips through create -> get on a catalog,
  coalesces table->schema->catalog per field, and the dropped
  ``retain_max_versions`` field is gone from the proto.
* RT2 (validation) — ``snapshot_density_seconds`` > ``retention_duration_seconds``
  on a submitted config is rejected with ``INVALID_ARGUMENT`` at the gRPC write
  boundary; equal / partial configs are accepted.

RED baseline (pre-reshape): the new proto fields don't exist, so each test builds
its ``RetentionConfig`` FIRST — ``RetentionConfig(retention_duration_seconds=...)``
raises ``ValueError`` in-process (field-absent), independent of the server. The
``retain_max_versions`` removal test is RED because that field still exists
(``pytest.raises(ValueError)`` does not fire) until the reshape lands.

Retention is set via the raw ``client._write`` / ``client._query`` stubs — the
PencaClient facade create_* methods intentionally do not expose retention.

Scoped run: ``just integration-test retention_config``

CHA-433 scope-B: retention becomes schema-broadest — the catalog no longer
carries a retention policy. ``test_catalog_has_no_retention_config`` is RED
until IMPL-0 reserves ``Catalog.default_retention_config``;
``test_retention_coalesce_table_schema`` pins the ``table -> schema`` rule. The
existing catalog-``default_retention_config`` tests above are reworked/removed by
IMPL-0 when the field is dropped (removing the field would otherwise break them).
"""

from __future__ import annotations

from uuid import uuid4

import grpc
import pytest
from penca_client.arrow import serialize_schema
from penca_proto.external.v1.common_pb2 import Catalog, RetentionConfig
from penca_proto.external.v1.write_pb2 import (
    CreateCatalogRequest,
    CreateSchemaRequest,
    CreateTableRequest,
    UpdateCatalogRequest,
    UpdateSchemaRequest,
    UpdateTableRequest,
)

from .integration_helpers import USER_SCHEMA, make_client

# RT1 — seconds round-trip + per-field coalesce + retain_max_versions removed


def test_retain_max_versions_field_removed():
    # RED pre-reshape: retain_max_versions still exists, so the constructor does
    # NOT raise and pytest.raises fails. GREEN once the field is dropped.
    with pytest.raises(ValueError):
        RetentionConfig(retain_max_versions=1)  # ty: ignore[unknown-argument]


# RT2 — validation: snapshot_density_seconds <= retention_duration_seconds
# (scope-B: on the schema — the broadest scope — and the table)


def test_validation_density_exceeds_duration_rejected_on_schema():
    # density (3600) > duration (600) -> INVALID_ARGUMENT on CreateSchema
    # (scope-B: the schema is the broadest retention scope).
    bad = RetentionConfig(retention_duration_seconds=600, snapshot_density_seconds=3600)

    client = make_client()
    catalog_name = f"ret_cat_{uuid4().hex[:8]}"
    cat_resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=catalog_name, owner="owner")
    )
    catalog_uuid = cat_resp.catalog_uuid
    branch_uuid = cat_resp.main_branch_uuid
    with pytest.raises(grpc.RpcError) as excinfo:
        client._write.CreateSchema(
            CreateSchemaRequest(
                schema_name="ret_schema",
                catalog_uuid=catalog_uuid,
                branch_uuid=branch_uuid,
                author="test",
                comment="cha-433",
                default_retention_config=bad,
            )
        )

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_validation_density_exceeds_duration_rejected_on_table():
    # Same rule on the retention_config carry-shape (CreateTable).
    bad = RetentionConfig(retention_duration_seconds=600, snapshot_density_seconds=3600)

    client = make_client()
    catalog_name = f"ret_cat_{uuid4().hex[:8]}"
    cat_resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=catalog_name, owner="owner")
    )
    catalog_uuid = cat_resp.catalog_uuid
    branch_uuid = cat_resp.main_branch_uuid
    schema_uuid = client.create_schema(
        "ret_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-495"
    )

    with pytest.raises(grpc.RpcError) as excinfo:
        client._write.CreateTable(
            CreateTableRequest(
                table_name="ret_table",
                arrow_schema=serialize_schema(USER_SCHEMA),
                primary_keys=["name"],
                retention_config=bad,
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                author="test",
                comment="cha-495",
            )
        )

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_validation_density_equals_duration_accepted():
    # Equal is allowed (>= one rung per window; the reject is a strict `>`).
    ok = RetentionConfig(retention_duration_seconds=600, snapshot_density_seconds=600)

    client = make_client()
    catalog_name = f"ret_cat_{uuid4().hex[:8]}"
    cat_resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=catalog_name, owner="owner")
    )
    resp = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=cat_resp.catalog_uuid,
            branch_uuid=cat_resp.main_branch_uuid,
            author="test",
            comment="cha-433",
            default_retention_config=ok,
        )
    )
    assert resp.schema_uuid


def test_validation_partial_config_accepted():
    # Only one field set -> no cross-field check possible -> accepted.
    partial = RetentionConfig(snapshot_density_seconds=3600)

    client = make_client()
    catalog_name = f"ret_cat_{uuid4().hex[:8]}"
    cat_resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=catalog_name, owner="owner")
    )
    resp = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=cat_resp.catalog_uuid,
            branch_uuid=cat_resp.main_branch_uuid,
            author="test",
            comment="cha-433",
            default_retention_config=partial,
        )
    )
    assert resp.schema_uuid


# CHA-433 scope-B — retention is schema-broadest; catalog-level retention gone


def test_catalog_has_no_retention_config():
    # scope-B: the catalog no longer carries a retention policy (schema is the
    # broadest scope). RED pre-reshape: `default_retention_config` (field 5) is
    # still declared on Catalog / Create/UpdateCatalogRequest, so the assertions
    # below fire. GREEN once IMPL-0 reserves the field.
    assert "default_retention_config" not in {
        field.name
        for field in Catalog.DESCRIPTOR.fields  # ty: ignore[unresolved-attribute]
    }, "Catalog must not expose default_retention_config (schema is broadest)"
    assert "default_retention_config" not in {
        field.name
        for field in CreateCatalogRequest.DESCRIPTOR.fields  # ty: ignore[unresolved-attribute]
    }
    assert "default_retention_config" not in {
        field.name
        for field in UpdateCatalogRequest.DESCRIPTOR.fields  # ty: ignore[unresolved-attribute]
    }


def test_retention_coalesce_table_schema():
    # scope-B: effective retention coalesces table -> schema (no catalog arm).
    # A schema sets the policy; a table with none of its own inherits it.
    schema_rc = RetentionConfig(
        retention_duration_seconds=7200, snapshot_density_seconds=300
    )

    client = make_client()
    catalog_name = f"ret_cat_{uuid4().hex[:8]}"
    cat_resp = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=catalog_name, owner="owner")
    )
    catalog_uuid = cat_resp.catalog_uuid
    branch_uuid = cat_resp.main_branch_uuid

    schema_resp = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=catalog_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-433 schema-broadest",
            default_retention_config=schema_rc,
        )
    )
    schema_uuid = schema_resp.schema_uuid

    client.create_table(
        "ret_table",
        USER_SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        author="test",
        comment="cha-433 schema-broadest",
    )

    info = client.get_table(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_name="ret_table",
    )
    assert info.retention_config.retention_duration_seconds == 7200
    assert info.retention_config.snapshot_density_seconds == 300


# CHA-433 do-no-harm guard — retention_duration_seconds immutable once set
# (loosening: CHA-511; shortening breaks descendant audit-below-fork: CHA-514)


def _make_schema_with_retention(client, duration_seconds: int):
    cat = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"ret_cat_{uuid4().hex[:8]}", owner="owner")
    )
    schema = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=cat.catalog_uuid,
            branch_uuid=cat.main_branch_uuid,
            author="test",
            comment="cha-433",
            default_retention_config=RetentionConfig(
                retention_duration_seconds=duration_seconds
            ),
        )
    )
    return cat, schema


def _update_schema_retention(client, cat, schema, duration_seconds: int):
    return client._write.UpdateSchema(
        UpdateSchemaRequest(
            schema_uuid=schema.schema_uuid,
            catalog_uuid=cat.catalog_uuid,
            branch_uuid=cat.main_branch_uuid,
            author="test",
            comment="update retention",
            default_retention_config=RetentionConfig(
                retention_duration_seconds=duration_seconds
            ),
        )
    )


def test_update_schema_rejects_loosening_retention():
    # Increasing the window (3600 -> 7200) is more liberal -> rejected.
    client = make_client()
    cat, schema = _make_schema_with_retention(client, 3600)
    with pytest.raises(grpc.RpcError) as excinfo:
        _update_schema_retention(client, cat, schema, 7200)

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_update_schema_rejects_shortening_retention():
    # CHA-433/CHA-514: shortening (3600 -> 1800) prunes pre-fork ancestor history
    # a descendant's audit-below-fork needs -> retention is immutable once set,
    # so shortening is rejected too (not only loosening).
    client = make_client()
    cat, schema = _make_schema_with_retention(client, 3600)
    with pytest.raises(grpc.RpcError) as excinfo:
        _update_schema_retention(client, cat, schema, 1800)

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_update_schema_allows_noop_retention():
    # Re-sending the same duration is a no-op -> allowed (immutable != frozen).
    client = make_client()
    cat, schema = _make_schema_with_retention(client, 3600)
    resp = _update_schema_retention(client, cat, schema, 3600)
    assert resp is not None


def test_update_schema_allows_establishing_retention():
    # Establishing a policy where none exists (unset -> set) is allowed; only
    # changing an already-set duration is rejected.
    client = make_client()
    cat = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"ret_cat_{uuid4().hex[:8]}", owner="owner")
    )
    schema = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=cat.catalog_uuid,
            branch_uuid=cat.main_branch_uuid,
            author="test",
            comment="cha-433",
        )
    )
    resp = _update_schema_retention(client, cat, schema, 3600)
    assert resp is not None


def test_update_schema_rejects_clearing_retention():
    # Omitting default_retention_config on update clears it (full replacement)
    # = set -> unset = retain forever = more liberal -> rejected.
    client = make_client()
    cat, schema = _make_schema_with_retention(client, 3600)
    with pytest.raises(grpc.RpcError) as excinfo:
        client._write.UpdateSchema(
            UpdateSchemaRequest(
                schema_uuid=schema.schema_uuid,
                catalog_uuid=cat.catalog_uuid,
                branch_uuid=cat.main_branch_uuid,
                author="test",
                comment="clear retention",
            )
        )

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_update_table_rejects_loosening_retention():
    # The guard applies to UpdateTable too (retention coalesces table -> schema).
    client = make_client()
    cat = client._write.CreateCatalog(
        CreateCatalogRequest(catalog_name=f"ret_cat_{uuid4().hex[:8]}", owner="owner")
    )
    schema = client._write.CreateSchema(
        CreateSchemaRequest(
            schema_name="ret_schema",
            catalog_uuid=cat.catalog_uuid,
            branch_uuid=cat.main_branch_uuid,
            author="test",
            comment="cha-433",
        )
    )
    table = client._write.CreateTable(
        CreateTableRequest(
            table_name="ret_table",
            schema_uuid=schema.schema_uuid,
            catalog_uuid=cat.catalog_uuid,
            branch_uuid=cat.main_branch_uuid,
            arrow_schema=serialize_schema(USER_SCHEMA),
            primary_keys=["name"],
            author="test",
            comment="cha-433",
            retention_config=RetentionConfig(retention_duration_seconds=3600),
        )
    )
    with pytest.raises(grpc.RpcError) as excinfo:
        client._write.UpdateTable(
            UpdateTableRequest(
                table_uuid=table.table_uuid,
                catalog_uuid=cat.catalog_uuid,
                branch_uuid=cat.main_branch_uuid,
                arrow_schema=serialize_schema(USER_SCHEMA),
                primary_keys=["name"],
                author="test",
                comment="loosen",
                retention_config=RetentionConfig(retention_duration_seconds=7200),
            )
        )

    err = excinfo.value
    assert isinstance(err, grpc.Call)
    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
