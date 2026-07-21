"""Python-native return types for the Penca client.

Wraps proto response objects with deserialized fields (e.g., Arrow
schemas as ``pa.Schema`` instead of raw bytes).
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
from penca_proto.external.v1.common_pb2 import (
    Catalog,
    Index,
    Table,
)
from penca_proto.external.v1.common_pb2 import (
    RetentionConfig as RetentionConfigProto,
)
from penca_proto.external.v1.common_pb2 import (
    Schema as SchemaProto,
)
from penca_proto.external.v1.write_pb2 import Change
from pyarrow import Schema

from penca_client.arrow import deserialize_schema, table_to_ipc_bytes


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    """Retention policy for versioned data."""

    retention_duration_seconds: int | None
    snapshot_density_seconds: int | None

    @staticmethod
    def from_proto(proto: RetentionConfigProto) -> RetentionConfig:
        """Construct a RetentionConfig from a proto RetentionConfig message."""
        return RetentionConfig(
            retention_duration_seconds=(
                proto.retention_duration_seconds
                if proto.HasField("retention_duration_seconds")
                else None
            ),
            snapshot_density_seconds=(
                proto.snapshot_density_seconds
                if proto.HasField("snapshot_density_seconds")
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class CatalogInfo:
    """Python-native representation of a catalog."""

    catalog_uuid: str
    catalog_name: str
    owner: str
    description: str
    # CHA-433: retention is schema-broadest; the catalog carries no policy.

    @staticmethod
    def from_proto(catalog: Catalog) -> CatalogInfo:
        """Convert a proto Catalog to a CatalogInfo."""
        return CatalogInfo(
            catalog_uuid=catalog.catalog_uuid,
            catalog_name=catalog.catalog_name,
            owner=catalog.owner,
            description=catalog.description,
        )


@dataclass(frozen=True, slots=True)
class SchemaInfo:
    """Python-native representation of a schema."""

    schema_uuid: str
    catalog_uuid: str
    schema_name: str
    description: str
    default_retention_config: RetentionConfig

    @staticmethod
    def from_proto(schema: SchemaProto) -> SchemaInfo:
        """Convert a proto Schema to a SchemaInfo."""
        return SchemaInfo(
            schema_uuid=schema.schema_uuid,
            catalog_uuid=schema.catalog_uuid,
            schema_name=schema.schema_name,
            description=schema.description,
            default_retention_config=RetentionConfig.from_proto(
                schema.default_retention_config,
            ),
        )


@dataclass(frozen=True, slots=True)
class Mutation:
    """Convenience input for :meth:`PencaClient.write_data`.

    Wraps one write against a single table — upserts and deletes each
    expressed as a ``pa.Table``. :meth:`to_proto` produces the payload
    ``Change`` (rows only); ``write_data`` lifts the table identity
    (``table_uuid`` / ``table_name``) onto the request. Identify the table
    via ``table_uuid``, ``table_name``, or both; the server resolves
    ``table_name`` to ``table_uuid`` via the same
    ``xxh3(schema_uuid:table_name)`` hash used elsewhere, so either is
    sufficient (at least one is required at the server).

    ``deletes`` is a ``pa.Table`` of *primary-key columns only, in the
    table's declared ``primary_keys`` order*. The server pulls
    ``primary_keys`` from ``__penca_system__.tables``, validates the
    batch's column order against it, and computes ``row_uuid``
    itself; callers do not call ``row_uuid_for_pk`` (CHA-185). A
    column-order mismatch is rejected with ``INVALID_ARGUMENT``
    rather than silently no-op'd.

    Zero-row Tables are treated as "no rows on this side" — they
    serialize to empty ``bytes`` so the server's row-count gating
    skips the ``tx_table_log`` emission. ``None`` is equivalent to
    omitting the side entirely.
    """

    table_uuid: str | None = None
    table_name: str | None = None
    upserts: pa.Table | None = None
    deletes: pa.Table | None = None

    def to_proto(self) -> Change:
        """Produce the wire ``Change`` payload (upserts + deletes only;
        the table identity is request-level). Coalesces multi-chunk Tables
        via ``combine_chunks()`` and emits a single Arrow IPC RecordBatch —
        the server's IPC reader expects one batch per stream (matches
        ``insert_upserts`` / ``insert_delete_pk_batches``). Zero-row Tables
        and ``None`` both serialize to empty ``bytes``."""
        return Change(
            upserts=_encode_table_or_empty(self.upserts),
            deletes=_encode_table_or_empty(self.deletes),
        )


def _encode_table_or_empty(table: pa.Table | None) -> bytes:
    if table is None or table.num_rows == 0:
        return b""

    return table_to_ipc_bytes(table)


@dataclass(frozen=True, slots=True)
class TableInfo:
    """Python-native representation of a table's metadata."""

    table_uuid: str
    schema_uuid: str
    table_name: str
    arrow_schema: Schema
    primary_keys: list[str]
    partition_keys: list[str]
    clustering_keys: list[str]
    description: str
    retention_config: RetentionConfig
    # Defined secondary indexes on the table (CHA-492), as the raw `Index`
    # proto messages — the same shape `get_index` returns.
    indexes: list[Index]

    @staticmethod
    def from_proto(table: Table) -> TableInfo:
        """Convert a proto Table to a TableInfo."""
        return TableInfo(
            table_uuid=table.table_uuid,
            schema_uuid=table.schema_uuid,
            table_name=table.table_name,
            arrow_schema=deserialize_schema(table.arrow_schema),
            primary_keys=list(table.primary_keys),
            partition_keys=list(table.partition_keys),
            clustering_keys=list(table.clustering_keys),
            description=table.description,
            retention_config=RetentionConfig.from_proto(table.retention_config),
            indexes=list(table.indexes),
        )
