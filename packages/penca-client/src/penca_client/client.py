"""Pure gRPC Penca client.

Opens one channel per microservice (query, write, lifecycle) and
exposes a native Python API that constructs proto requests and delegates
to the matching stub. Streaming RPCs (``ReadData``, ``AuditData``)
are materialized into a ``pa.Table`` inside the client so that the
user-facing method signatures stay identical to the pre-gRPC embedded
path. Benchmarks that care about the serialization overhead should
exercise the generated stubs directly.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from json import dumps

from adbc_driver_flightsql.dbapi import connect as flight_sql_connect
from adbc_driver_manager.dbapi import Connection as AdbcConnection
from adbc_driver_manager.dbapi import Cursor as AdbcCursor
from grpc import Channel, RpcError, insecure_channel
from penca_proto.external.v1.common_pb2 import (
    Branch,
    Index,
    IntegerRange,
    PaginationRequest,
)
from penca_proto.external.v1.lifecycle_pb2 import (
    BranchOpRequest,
    BranchOpResponse,
    CompactPersistSegmentsRequest,
    CompactPersistSegmentsResponse,
    PersistRequest,
    PersistResponse,
    PurgeRequest,
    PurgeResponse,
    PurgeTxLogRequest,
    PurgeTxLogResponse,
    SnapshotRequest,
    SnapshotResponse,
    SweepSegmentsRequest,
    SweepSegmentsResponse,
    Watermark,
)
from penca_proto.external.v1.lifecycle_pb2_grpc import LifecycleServiceStub
from penca_proto.external.v1.query_pb2 import (
    AuditDataRequest,
    GetBranchRequest,
    GetCatalogRequest,
    GetIndexRequest,
    GetSchemaRequest,
    GetTableRequest,
    ListBranchesRequest,
    ListCatalogsRequest,
    ListIndexesRequest,
    ListSchemasRequest,
    ListTablesRequest,
    Projection,
    ReadDataRequest,
)
from penca_proto.external.v1.query_pb2_grpc import QueryServiceStub
from penca_proto.external.v1.write_pb2 import (
    AbortTxRequest,
    AbortTxResponse,
    BeginTxRequest,
    BeginTxResponse,
    CommitTxRequest,
    CommitTxResponse,
    CreateBranchRequest,
    CreateCatalogRequest,
    CreateIndexRequest,
    CreateSchemaRequest,
    CreateTableIndexDefinition,
    CreateTableRequest,
    DeleteBranchRequest,
    DeleteCatalogRequest,
    DeleteIndexRequest,
    DeleteSchemaRequest,
    DeleteTableRequest,
    MergeBranchRequest,
    MergeBranchResponse,
    UpdateBranchRequest,
    UpdateCatalogRequest,
    UpdateIndexRequest,
    UpdateSchemaRequest,
    UpdateTableRequest,
    WriteDataRequest,
    WriteDataResponse,
)
from penca_proto.external.v1.write_pb2_grpc import WriteServiceStub
from pyarrow import RecordBatch, Schema, Table

from penca_client._time import datetime_to_micros
from penca_client.arrow import (
    ipc_bytes_to_batch,
    serialize_schema,
    table_to_ipc_bytes,
)
from penca_client.config import ClientSettings
from penca_client.status import rpc_error_to_api_error
from penca_client.types import CatalogInfo, Mutation, SchemaInfo, TableInfo


class PencaClient:
    """User-facing Penca client backed by pure-gRPC channels."""

    # Retry policy applied to every PencaClient channel. Treats transient
    # transport errors (socket drops, mid-handshake resets, temporarily
    # unreachable backends) as retryable so callers don't have to wrap each
    # RPC in their own retry loop. Non-idempotent writes are still safe
    # because every mutation carries an explicit tx_uuid / row_uuid —
    # replays collapse to the original commit. The one edge case is
    # BeginTx: the server allocates the tx_uuid, so an UNAVAILABLE retry
    # after the server received the original can leave an orphan tx. That
    # is bounded by the tx TTL reaper (~60s) and judged acceptable versus
    # making every caller handle transient failures.
    _RETRY_SERVICE_CONFIG = dumps(
        {
            "methodConfig": [
                {
                    "name": [{}],  # empty selector -> applies to every method
                    "retryPolicy": {
                        "maxAttempts": 5,
                        "initialBackoff": "0.1s",
                        "maxBackoff": "2s",
                        "backoffMultiplier": 2.0,
                        "retryableStatusCodes": ["UNAVAILABLE"],
                    },
                }
            ]
        }
    )

    def __init__(
        self,
        query_stub: QueryServiceStub,
        write_stub: WriteServiceStub,
        lifecycle_stub: LifecycleServiceStub,
        flight_sql_url: str | None = None,
        catalog: str | None = "public",
        branch: str | None = "main",
    ) -> None:
        self._query = query_stub
        self._write = write_stub
        self._lifecycle = lifecycle_stub
        self._flight_sql_url = flight_sql_url
        # See the ``catalog`` and ``branch`` properties below for the
        # semantics; the underscore-prefixed fields are the storage.
        self._catalog = catalog
        self._branch = branch
        self._flight_sql_conn: AdbcConnection | None = None

    @staticmethod
    def from_settings(
        settings: ClientSettings | None = None,
        *,
        catalog: str | None = "public",
        branch: str | None = "main",
    ) -> PencaClient:
        """Build a client from ``ClientSettings`` (defaults to env vars).

        ``catalog`` and ``branch`` seed the client's defaults — see the
        properties of the same name on :class:`PencaClient` for the
        full semantics.
        """
        if settings is None:
            settings = ClientSettings()  # ty: ignore[missing-argument]

        return PencaClient(
            query_stub=QueryServiceStub(PencaClient._channel(settings.query_url)),
            write_stub=WriteServiceStub(PencaClient._channel(settings.write_url)),
            lifecycle_stub=LifecycleServiceStub(
                PencaClient._channel(settings.lifecycle_url)
            ),
            flight_sql_url=settings.flight_sql_url,
            catalog=catalog,
            branch=branch,
        )

    @property
    def catalog(self) -> str | None:
        """The catalog this client operates against.

        Used as the fallback for within-catalog gRPC methods (when
        neither ``catalog_uuid`` nor ``catalog_name`` is passed) *and*
        as the Flight SQL session pin (CHA-253) — sent on the
        ``x-penca-catalog`` gRPC metadata header at handshake time
        (mirroring the existing ``x-penca-branch`` shape) so the
        server-side session is bound to this catalog before any other
        request lands. Catalog binding is established once, at
        session-mint, and immutable for the connection's lifetime —
        Postgres-shaped: a connection is to one catalog the way a
        PgJDBC connection is to one database. Mid-session
        ``Connection.setCatalog(X)`` is a no-op when ``X`` matches the
        pin and is rejected otherwise.

        ``None`` leaves both unset; the Flight SQL server then falls
        back to its ``SQL_SERVER_DEFAULT_CATALOG`` and within-catalog
        gRPC methods raise ``INVALID_ARGUMENT`` if the caller didn't
        supply a catalog explicitly. Catalog-CRUD methods
        (``create_catalog``, ``get_catalog``, ``delete_catalog``,
        ``update_catalog``, ``list_catalogs``) deliberately do **not**
        consult this — they operate on the catalog directly and a
        silent fallback would be confusing.
        """
        return self._catalog

    @catalog.setter
    def catalog(self, name: str | None) -> None:
        # Catalog is pinned at handshake on the SQL side (CHA-253), so
        # changing the value means reconnecting. Force the next SQL
        # call to open a *new* ADBC connection by closing the cached
        # one here; ``_flight_sql_cursor()`` will then pass the new
        # value via the ``x-penca-catalog`` gRPC metadata header at
        # the new handshake.
        if self._flight_sql_conn is not None:
            self._flight_sql_conn.close()
            self._flight_sql_conn = None

        self._catalog = name

    @property
    def branch(self) -> str | None:
        """The branch this client operates against.

        Used as the fallback for within-catalog gRPC methods (when
        neither ``branch_uuid`` nor ``branch_name`` is passed) *and*
        as the Flight SQL session pin (CHA-119) — sent as the
        ``x-penca-branch`` gRPC metadata header so the server pins
        the session at mint time. ``None`` leaves the SQL header
        unset; the Flight SQL server then falls back to its
        ``SQL_SERVER_DEFAULT_BRANCH``. Defaults to ``"main"``.
        """
        return self._branch

    @branch.setter
    def branch(self, name: str | None) -> None:
        # Branch is connection-scoped on the SQL side (CHA-119) — the
        # same shape as ``catalog`` above: the server pins the session
        # at mint time and rejects any mid-session ``x-penca-branch``
        # change. Drop the cached ADBC connection so the next
        # ``_flight_sql_cursor()`` reconnects with the new pin.
        if self._flight_sql_conn is not None:
            self._flight_sql_conn.close()
            self._flight_sql_conn = None

        self._branch = name

    @classmethod
    def _channel(cls, url: str) -> Channel:
        return insecure_channel(
            url,
            options=[
                ("grpc.enable_retries", 1),
                ("grpc.service_config", cls._RETRY_SERVICE_CONFIG),
                # TODO(CHA-136): Disabling encode/decode size limits as a
                # stop-gap so wide-schema responses don't trip gRPC's default
                # 4 MiB cap. Real fix is server-side chunking by
                # `default_stream_batch_size`; restore the default once
                # that lands.
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )

    @staticmethod
    def _set_optional(request: object, field_name: str, value: str | None) -> None:
        if value is not None:
            setattr(request, field_name, value)

    def _get_branch(
        self,
        branch_uuid: str | None,
        branch_name: str | None,
    ) -> tuple[str | None, str | None]:
        """Resolve the branch identifiers a request method should send.

        When the caller passes neither, fall back to
        ``self._branch`` (which itself defaults to ``"main"``).
        Only the client facade defaults — servers require explicit
        branches.
        """
        if branch_uuid is None and branch_name is None:
            return None, self._branch

        return branch_uuid, branch_name

    def _get_catalog(
        self,
        catalog_uuid: str | None,
        catalog_name: str | None,
    ) -> tuple[str | None, str | None]:
        """Resolve the catalog identifiers a within-catalog request
        method should send.

        When the caller passes neither, fall back to
        ``self._catalog`` (set at client construction). Returns
        ``(None, None)`` when no default was configured — the server
        will then reject the request with ``INVALID_ARGUMENT``, surfacing
        the "no catalog supplied and no default configured" case as a
        loud error rather than a silent miss.

        Deliberately not used by catalog-CRUD methods (``create_catalog``,
        ``get_catalog``, ``delete_catalog``, ``update_catalog``,
        ``list_catalogs``) — those operate on the catalog directly and a
        silent fallback would be confusing.
        """
        if catalog_uuid is None and catalog_name is None:
            return None, self._catalog

        return catalog_uuid, catalog_name

    def create_catalog(
        self,
        catalog_name: str,
        owner: str,
        description: str = "",
    ) -> tuple[str, str]:
        """Create a catalog. Returns ``(catalog_uuid, main_branch_uuid)``.

        CHA-236: namespace UUIDs are random server-side, so the
        ``CreateCatalogResponse`` is the only source of truth for both
        the catalog's UUID and the auto-bootstrapped ``main`` branch's
        UUID. Callers thread both forward instead of recomputing the
        main-branch UUID via a hash helper.
        """
        try:
            response = self._write.CreateCatalog(
                CreateCatalogRequest(
                    catalog_name=catalog_name,
                    owner=owner,
                    description=description,
                )
            )
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.catalog_uuid, response.main_branch_uuid

    def get_catalog(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
    ) -> CatalogInfo:
        """Look up a catalog by UUID or name. Exactly one must be set."""
        request = GetCatalogRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        try:
            response = self._query.GetCatalog(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return CatalogInfo.from_proto(response.catalog)

    def list_catalogs(self, owner: str | None = None) -> Iterator[CatalogInfo]:
        page_token = ""
        while True:
            request = ListCatalogsRequest(
                pagination=PaginationRequest(page_token=page_token),
            )
            self._set_optional(request, "owner", owner)
            try:
                response = self._query.ListCatalogs(request)
            except RpcError as e:
                raise rpc_error_to_api_error(e) from e

            for catalog in response.catalogs:
                yield CatalogInfo.from_proto(catalog)

            if not response.HasField("next_page_token"):
                break

            page_token = response.next_page_token

    def update_catalog(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        owner: str = "",
        description: str = "",
        new_catalog_name: str | None = None,
    ) -> CatalogInfo:
        """Update a catalog. Pass ``new_catalog_name`` to rename (CHA-236).

        The catalog UUID is stable across rename — clients keep
        addressing by ``catalog_uuid`` after this returns.
        """
        request = UpdateCatalogRequest(owner=owner, description=description)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "new_catalog_name", new_catalog_name)
        try:
            response = self._write.UpdateCatalog(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return CatalogInfo.from_proto(response.catalog)

    def delete_catalog(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
    ) -> str:
        """Delete a catalog. Returns the deleted catalog UUID."""
        request = DeleteCatalogRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        try:
            response = self._write.DeleteCatalog(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.catalog_uuid

    def create_schema(
        self,
        schema_name: str,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        description: str = "",
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Create a schema on a branch (defaults to ``self._branch``).

        Catalog identifiers follow the canonical client pattern: pass
        ``catalog_uuid`` or ``catalog_name`` explicitly, or set
        ``client.catalog`` once and rely on the per-call fallback.

        ``tx_uuid`` is mode-switching: ``None`` auto-commits (requires
        ``author`` and ``comment`` for tx attribution); any string
        appends to that open penca tx (``author`` / ``comment`` must
        be unset since the open tx already carries its own).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = CreateSchemaRequest(
            schema_name=schema_name,
            description=description,
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.CreateSchema(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.schema_uuid

    def get_schema(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        open_tx_uuid: str | None = None,
        as_of_micros: int | None = None,
        as_of_seq: int | None = None,
    ) -> SchemaInfo:
        """Get a schema's metadata.

        ``as_of_micros`` resolves ``schema_name`` against the historical
        snapshot rather than the latest committed view (CHA-236) — a
        renamed schema can still be found by its old name within the
        window where that name was current. ``as_of_seq`` is the same
        resolution on the ``commit_seq_num`` axis (CHA-460); pass at most one
        of ``as_of_micros`` / ``as_of_seq`` / ``open_tx_uuid``.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = GetSchemaRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "open_tx_uuid", open_tx_uuid)
        if as_of_micros is not None:
            request.as_of_micros = as_of_micros

        if as_of_seq is not None:
            request.as_of_seq = as_of_seq

        try:
            response = self._query.GetSchema(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return SchemaInfo.from_proto(response.schema)

    def list_schemas(
        self,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        as_of_micros: int | None = None,
        as_of_seq: int | None = None,
    ) -> Iterator[SchemaInfo]:
        """List schemas in a catalog. Pass ``catalog_uuid`` /
        ``catalog_name`` explicitly or rely on ``client.catalog``.

        ``as_of_micros`` snapshots the listing at a historical commit so
        renamed schemas surface under their old name within that
        window (CHA-236). ``as_of_seq`` is the same on the ``commit_seq_num``
        axis (CHA-460).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        page_token = ""
        while True:
            request = ListSchemasRequest(
                pagination=PaginationRequest(page_token=page_token),
            )
            self._set_optional(request, "catalog_uuid", catalog_uuid)
            self._set_optional(request, "catalog_name", catalog_name)
            self._set_optional(request, "branch_uuid", branch_uuid)
            self._set_optional(request, "branch_name", branch_name)
            if as_of_micros is not None:
                request.as_of_micros = as_of_micros

            if as_of_seq is not None:
                request.as_of_seq = as_of_seq

            try:
                response = self._query.ListSchemas(request)
            except RpcError as e:
                raise rpc_error_to_api_error(e) from e

            for schema in response.schemas:
                yield SchemaInfo.from_proto(schema)

            if not response.HasField("next_page_token"):
                break

            page_token = response.next_page_token

    def update_schema(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str = "",
        schema_name: str = "",
        description: str = "",
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
        new_schema_name: str | None = None,
    ) -> SchemaInfo:
        """Update a schema. ``author`` / ``comment`` are required for the
        auto-commit path (``tx_uuid`` unset) and must be unset when joining
        an open tx — see :meth:`create_schema` for the mode-switch.

        ``new_schema_name`` renames the schema on this branch (CHA-236) —
        the ``schema_uuid`` stays put, so existing references remain valid.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = UpdateSchemaRequest(
            schema_uuid=schema_uuid,
            schema_name=schema_name,
            description=description,
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        self._set_optional(request, "new_schema_name", new_schema_name)
        try:
            response = self._write.UpdateSchema(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return SchemaInfo.from_proto(response.schema)

    def delete_schema(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str = "",
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Delete a schema. Returns the deleted schema UUID. ``author`` /
        ``comment`` follow :meth:`create_schema`'s mode-switch."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = DeleteSchemaRequest(schema_uuid=schema_uuid)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.DeleteSchema(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.schema_uuid

    def create_table(
        self,
        table_name: str,
        arrow_schema: Schema,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        description: str = "",
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        clustering_keys: list[str] | None = None,
        indexes: list[dict] | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Create a table. Returns the table_uuid.

        ``tx_uuid`` is mode-switching (CHA-164): ``None`` auto-commits
        (requires ``author`` and ``comment`` for tx attribution); any
        string appends to that open penca tx (``author`` / ``comment``
        must be unset since the open tx already carries its own).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = CreateTableRequest(
            table_name=table_name,
            arrow_schema=serialize_schema(arrow_schema),
            description=description,
            primary_keys=primary_keys or [],
            partition_keys=partition_keys or [],
            clustering_keys=clustering_keys or [],
            # CHA-455: inline index definitions, accepted as dicts of
            # {index_name, columns, index_type}.
            indexes=[CreateTableIndexDefinition(**d) for d in (indexes or [])],
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)

        try:
            response = self._write.CreateTable(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.table_uuid

    def get_table(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        open_tx_uuid: str | None = None,
        as_of_micros: int | None = None,
        as_of_seq: int | None = None,
    ) -> TableInfo:
        """Get a table's metadata.

        ``as_of_micros`` resolves ``table_name`` / ``schema_name`` at
        the historical snapshot rather than the latest committed view
        (CHA-236) so a renamed table is still reachable by its old name
        within the window where it carried that name. ``as_of_seq`` is the
        same resolution on the ``commit_seq_num`` axis (CHA-460); pass at most
        one of ``as_of_micros`` / ``as_of_seq`` / ``open_tx_uuid``.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = GetTableRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "open_tx_uuid", open_tx_uuid)
        if as_of_micros is not None:
            request.as_of_micros = as_of_micros

        if as_of_seq is not None:
            request.as_of_seq = as_of_seq

        try:
            response = self._query.GetTable(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return TableInfo.from_proto(response.table)

    def list_tables(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        as_of_micros: int | None = None,
        as_of_seq: int | None = None,
    ) -> Iterator[TableInfo]:
        """List tables in a schema on a branch.

        ``as_of_micros`` snapshots the listing at a historical commit so
        renamed tables surface under their old name within that
        window (CHA-236). ``as_of_seq`` is the same on the ``commit_seq_num``
        axis (CHA-460).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        page_token = ""
        while True:
            request = ListTablesRequest(
                pagination=PaginationRequest(page_token=page_token),
            )
            self._set_optional(request, "catalog_uuid", catalog_uuid)
            self._set_optional(request, "catalog_name", catalog_name)
            self._set_optional(request, "schema_uuid", schema_uuid)
            self._set_optional(request, "schema_name", schema_name)
            self._set_optional(request, "branch_uuid", branch_uuid)
            self._set_optional(request, "branch_name", branch_name)
            if as_of_micros is not None:
                request.as_of_micros = as_of_micros

            if as_of_seq is not None:
                request.as_of_seq = as_of_seq

            try:
                response = self._query.ListTables(request)
            except RpcError as e:
                raise rpc_error_to_api_error(e) from e

            for table in response.tables:
                yield TableInfo.from_proto(table)

            if not response.HasField("next_page_token"):
                break

            page_token = response.next_page_token

    def update_table(
        self,
        arrow_schema: Schema,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        description: str = "",
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        clustering_keys: list[str] | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
        new_table_name: str | None = None,
    ) -> TableInfo:
        """Update a table's mutable fields. Returns the updated table.

        ``author`` / ``comment`` follow :meth:`create_table`'s mode-switch.
        ``new_table_name`` renames the table on this branch (CHA-236) —
        the ``table_uuid`` stays put so persisted segments + audit
        history keep resolving.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = UpdateTableRequest(
            arrow_schema=serialize_schema(arrow_schema),
            description=description,
            primary_keys=primary_keys or [],
            partition_keys=partition_keys or [],
            clustering_keys=clustering_keys or [],
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        self._set_optional(request, "new_table_name", new_table_name)
        try:
            response = self._write.UpdateTable(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return TableInfo.from_proto(response.table)

    def delete_table(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Delete a table on a branch. Returns the deleted table UUID.
        ``author`` / ``comment`` follow :meth:`create_table`'s mode-switch."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = DeleteTableRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.DeleteTable(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.table_uuid

    def create_index(
        self,
        *,
        index_name: str,
        columns: list[str],
        index_type: int,
        table_uuid: str | None = None,
        table_name: str | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Define a secondary index on a table. Returns the index_uuid.

        ``tx_uuid`` is mode-switching (see :meth:`create_schema`).
        ``index_name`` is unique only within the target table.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = CreateIndexRequest(
            index_name=index_name,
            columns=columns,
            # proto enum fields accept their int value at runtime; the
            # generated stub types the kwarg as the IndexType subtype.
            index_type=index_type,  # ty: ignore[invalid-argument-type]
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.CreateIndex(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.index_uuid

    def get_index(
        self,
        *,
        index_uuid: str | None = None,
        index_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        open_tx_uuid: str | None = None,
        as_of_micros: int | None = None,
    ) -> Index:
        """Get one index definition (the ``Index`` proto) by uuid or by
        ``(table, name)``. ``as_of_micros`` time-travels the auditable
        store; raises ``NotFoundError`` when no index resolves."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = GetIndexRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "index_uuid", index_uuid)
        self._set_optional(request, "index_name", index_name)
        self._set_optional(request, "open_tx_uuid", open_tx_uuid)
        if as_of_micros is not None:
            request.as_of_micros = as_of_micros

        try:
            response = self._query.GetIndex(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.index

    def list_indexes(
        self,
        *,
        table_uuid: str | None = None,
        table_name: str | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        open_tx_uuid: str | None = None,
        as_of_micros: int | None = None,
    ) -> list[Index]:
        """List every index defined on a table (the ``Index`` protos)."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = ListIndexesRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "open_tx_uuid", open_tx_uuid)
        if as_of_micros is not None:
            request.as_of_micros = as_of_micros

        try:
            response = self._query.ListIndexes(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return list(response.indexes)

    def update_index(
        self,
        *,
        new_index_name: str,
        index_uuid: str | None = None,
        index_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Rename an index (rename-only). Returns the index_uuid."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = UpdateIndexRequest(new_index_name=new_index_name)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "index_uuid", index_uuid)
        self._set_optional(request, "index_name", index_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.UpdateIndex(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.index_uuid

    def delete_index(
        self,
        *,
        index_uuid: str | None = None,
        index_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        tx_uuid: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> str:
        """Drop an index. Returns the deleted index_uuid; raises
        ``NotFoundError`` if it doesn't exist at the read snapshot."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = DeleteIndexRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        self._set_optional(request, "index_uuid", index_uuid)
        self._set_optional(request, "index_name", index_name)
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        try:
            response = self._write.DeleteIndex(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.index_uuid

    def create_branch(
        self,
        branch_name: str,
        author: str,
        comment: str,
        *,
        commit_seq_num: int | None = None,
        commit_micros: int | None = None,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        source_branch_uuid: str | None = None,
        source_branch_name: str | None = None,
    ) -> Branch:
        """Create a branch. Returns the Branch.

        The fork point is a commit-order position on the source branch. Pass
        ``commit_seq_num`` to fork from an exact commit (e.g. a
        ``CommitTxResponse.commit_seq_num``), or ``commit_micros`` to fork
        as of the latest commit at or before that wall-clock time. Omit both to
        fork from the source branch head. They are mutually exclusive fork
        coordinates — supplying both raises ``ValueError``.

        ``author`` and ``comment`` tag the per-branch table-materialization
        tx (CHA-174). Required — every CreateBranch is auto-commit (no
        ``tx_uuid`` mode-switch), so attribution flows from the caller.

        CHA-184: branches are catalog-scoped — the new branch
        materializes every schema visible on the source branch as a
        single fork tx, so no per-schema identifier is accepted.
        """
        if commit_seq_num is not None and commit_micros is not None:
            raise ValueError(
                "commit_seq_num and commit_micros are mutually "
                "exclusive fork coordinates; supply at most one"
            )

        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        source_branch_uuid, source_branch_name = self._get_branch(
            source_branch_uuid,
            source_branch_name,
        )
        request = CreateBranchRequest(
            branch_name=branch_name,
            author=author,
            comment=comment,
        )
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "source_branch_uuid", source_branch_uuid)
        self._set_optional(request, "source_branch_name", source_branch_name)
        if commit_seq_num is not None:
            request.commit_seq_num = commit_seq_num
        elif commit_micros is not None:
            request.commit_micros = commit_micros

        if branch_uuid is not None:
            request.branch_uuid = branch_uuid

        try:
            response = self._write.CreateBranch(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.branch

    def get_branch(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
    ) -> Branch:
        """Get a branch by UUID or name."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = GetBranchRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        try:
            response = self._query.GetBranch(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.branch

    def list_branches(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
    ) -> Iterator[Branch]:
        """List branches in a catalog."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        page_token = ""
        while True:
            request = ListBranchesRequest(
                pagination=PaginationRequest(page_token=page_token),
            )
            self._set_optional(request, "catalog_uuid", catalog_uuid)
            self._set_optional(request, "catalog_name", catalog_name)
            self._set_optional(request, "schema_uuid", schema_uuid)
            self._set_optional(request, "schema_name", schema_name)

            try:
                response = self._query.ListBranches(request)
            except RpcError as e:
                raise rpc_error_to_api_error(e) from e

            yield from response.branches
            if not response.HasField("next_page_token"):
                break

            page_token = response.next_page_token

    def update_branch(
        self,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        new_branch_name: str,
    ) -> Branch:
        """Rename a branch (CHA-236). Returns the updated branch.

        The ``branch_uuid`` is stable across rename — descendant branch
        references, persisted cold segments, and per-branch metadata
        keep resolving. Per-catalog ``UNIQUE(branch_name)`` rejects
        collisions with ``AlreadyExistsError``.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = UpdateBranchRequest(new_branch_name=new_branch_name)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        try:
            response = self._write.UpdateBranch(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return response.branch

    def delete_branch(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
    ) -> None:
        """Delete a branch.

        CHA-184: catalog-scoped — DeleteBranch cleans cold storage and
        metadata for every schema's tables on the branch, so no
        per-schema identifier is accepted.

        Raises ``InvalidRequestError`` when ``branch_uuid`` names the catalog's
        ``main`` branch: deleting it leaves the catalog unusable, since every read
        resolves main. Use ``delete_catalog`` to remove a catalog.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = DeleteBranchRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        try:
            self._write.DeleteBranch(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def merge_branch(
        self,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        source_branch_uuid: str | None = None,
        source_branch_name: str | None = None,
        target_branch_uuid: str | None = None,
        target_branch_name: str | None = None,
        comment: str = "",
        author: str = "",
    ) -> MergeBranchResponse:
        """Merge a branch. Returns the response with the merge's ``commit_micros``.

        CHA-184: catalog-scoped — MergeBranch fans out across every
        schema's tables on the source branch, so no per-schema
        identifier is accepted.

        The merge's ``comment`` and ``author`` are no longer surfaced on the
        response (tx framing is internal post-CHA-222); they are queryable
        per-row via :meth:`audit_data` on the target branch, windowed on the
        returned ``commit_micros``.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        request = MergeBranchRequest(comment=comment, author=author)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "source_branch_uuid", source_branch_uuid)
        self._set_optional(request, "source_branch_name", source_branch_name)
        self._set_optional(request, "target_branch_uuid", target_branch_uuid)
        self._set_optional(request, "target_branch_name", target_branch_name)
        try:
            return self._write.MergeBranch(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def begin_tx(
        self,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        comment: str = "",
        author: str = "",
        tx_uuid: str | None = None,
        timeout_seconds: int | None = None,
    ) -> BeginTxResponse:
        """Begin a transaction.

        Returns the response with the (server- or client-allocated)
        ``tx_uuid`` plus ``began_at_micros`` / ``expires_at_micros``.
        Supply ``tx_uuid`` for retry-idempotent BeginTx under transport
        failure; otherwise the server allocates one and echoes it back.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = BeginTxRequest(comment=comment, author=author)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        if tx_uuid is not None:
            request.tx_uuid = tx_uuid

        if timeout_seconds is not None:
            request.timeout_seconds = timeout_seconds

        try:
            return self._write.BeginTx(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def commit_tx(
        self,
        tx_uuid: str,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
    ) -> CommitTxResponse:
        """Commit a transaction. Returns the response with ``commit_micros``.

        Tx ops are catalog-scoped (CHA-163), so schema isn't part of
        the addressing. ``branch_uuid`` / ``branch_name`` are required
        by the server so the leaf ``commit_tx_log`` / ``begin_tx_log`` /
        ``abort_tx_log`` partitions can be addressed without a
        parent-table scan.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = CommitTxRequest(tx_uuid=tx_uuid)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        try:
            return self._write.CommitTx(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def abort_tx(
        self,
        tx_uuid: str,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
    ) -> AbortTxResponse:
        """Abort an open transaction. Returns the response with ``aborted_at_micros``.

        Tx ops are catalog-scoped (CHA-163), so schema isn't part of
        the addressing. ``branch_uuid`` / ``branch_name`` are required
        by the server so the leaf ``begin_tx_log`` / ``abort_tx_log``
        partitions can be addressed without a parent-table scan.

        Idempotent: re-aborting the same ``tx_uuid`` is a no-op. Raises
        ``FailedPreconditionError`` if the transaction has already
        committed.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = AbortTxRequest(tx_uuid=tx_uuid)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        try:
            return self._write.AbortTx(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def write_data(
        self,
        tx_uuid: str | None,
        mutation: Mutation,
        *,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        author: str | None = None,
        comment: str | None = None,
    ) -> WriteDataResponse:
        """Apply a single-table write against a branch.

        The :class:`Mutation`'s ``pa.Table`` upserts/deletes become the
        wire ``Change`` payload here (callers never touch Arrow IPC bytes),
        and its table identity (``table_uuid`` / ``table_name``) is lifted
        onto the request — read-symmetric with ``read_data`` (CHA-475).
        Multi-table atomic writes use an explicit begin/commit tx.

        ``tx_uuid`` is mode-switching, presence-based (not content-based):

        - ``None`` → auto-commit. The server opens + commits a penca tx,
          returned on the response. ``author`` / ``comment`` are tx
          metadata for that tx.
        - any string (including ``""``) → append to that already-open
          penca tx. ``author`` and ``comment`` must be ``None``. Format
          validation (rejecting empty / non-UUID values) is CHA-92's job
          at the servicer boundary.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = WriteDataRequest(change=mutation.to_proto())
        self._set_optional(request, "tx_uuid", tx_uuid)
        self._set_optional(request, "author", author)
        self._set_optional(request, "comment", comment)
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", mutation.table_uuid)
        self._set_optional(request, "table_name", mutation.table_name)
        try:
            return self._write.WriteData(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def persist(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        target_micros: int | None = None,
    ) -> PersistResponse:
        """Persist hot storage data for a single table to cold storage.

        ``target_micros`` is an optional upper bound; the server clamps
        to ``min(target_micros ?? now, oldest_open_began_at(branch) - 1)``
        so the open-tx invariant always holds. The actual watermark is
        returned in ``PersistResponse.persisted_at_micros`` (unset when
        the call was a no-op).

        Persist no longer touches hot log rows — those move to cold and
        stay queryable from hot until :meth:`purge` runs. The hot/cold
        visibility cutoff for ``plan()`` is ``purged_at_micros``, not
        ``persisted_at_micros``.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = PersistRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        if target_micros is not None:
            request.target_micros = target_micros

        try:
            return self._lifecycle.Persist(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def _branch_op(
        self,
        rpc,
        catalog_uuid: str | None,
        catalog_name: str | None,
        branch_uuid: str | None,
        branch_name: str | None,
        target: Watermark | None,
    ) -> BranchOpResponse:
        """Shared kernel for the catalog-wide `*Branch` lifecycle RPCs."""
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = BranchOpRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        # `target` is a resolved commit-order position (Watermark), not a
        # tx_uuid. Unset => the op bounds at the branch head.
        if target is not None:
            request.target.CopyFrom(target)

        try:
            return rpc(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def persist_branch(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        target: Watermark | None = None,
    ) -> BranchOpResponse:
        """Persist every modified table on a branch to cold (CHA-273).

        Catalog-wide sibling of :meth:`persist`. ``target`` is a resolved
        ``Watermark`` fork position (commit_seq_num + commit_micros); when unset,
        the op bounds at the branch head. Drives the lifecycle scheduler's
        persist loop, plus CreateBranch's persist-at-fork and branch-merge
        source flush.

        Continue-on-error per table: ``response.watermark`` is the position used
        and is set ONLY when every table succeeded. It is left unset when any
        table failed, so callers needing an all-or-nothing flush must check it.
        """
        return self._branch_op(
            self._lifecycle.PersistBranch,
            catalog_uuid,
            catalog_name,
            branch_uuid,
            branch_name,
            target,
        )

    def snapshot_branch(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        target: Watermark | None = None,
    ) -> BranchOpResponse:
        """Snapshot every PERSISTED table on a branch.

        Drives the lifecycle scheduler's snapshot loop. Enumerates the persisted
        set, not the hot-modified one, so a table persisted then dropped from hot
        is still re-snapshotted. Same withheld-watermark signal as
        :meth:`persist_branch`.
        """
        return self._branch_op(
            self._lifecycle.SnapshotBranch,
            catalog_uuid,
            catalog_name,
            branch_uuid,
            branch_name,
            target,
        )

    def persist_and_snapshot_branch(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        target: Watermark | None = None,
    ) -> BranchOpResponse:
        """Persist every modified table then snapshot every persisted table on a
        branch, per table (non-atomic).

        A client-side convenience for both phases in one round-trip; the
        scheduler drives :meth:`persist_branch` and :meth:`snapshot_branch`
        separately on independent cadences. Same withheld-watermark signal as
        :meth:`persist_branch`.
        """
        return self._branch_op(
            self._lifecycle.PersistAndSnapshotBranch,
            catalog_uuid,
            catalog_name,
            branch_uuid,
            branch_name,
            target,
        )

    def purge(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
    ) -> PurgeResponse:
        """Purge hot upsert/delete log rows for a single table up to
        that table's persist watermark, and advance the table's
        ``purged_at_micros`` — the hot/cold visibility cutoff used by
        ``plan()``.

        No-op fast-path: when there is no committed persist newer than
        the last purge, Purge does not write a ``table_purge_metadata``
        row — ``PurgeResponse.purged_at_micros`` is unset. On a real
        purge, the response carries the new watermark.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = PurgeRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)

        try:
            return self._lifecycle.Purge(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def purge_tx_log(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
    ) -> PurgeTxLogResponse:
        """GC the four hot tx-log family tables (commit_tx_log /
        tx_table_log / abort_tx_log / begin_tx_log) for a branch
        (CHA-221; CHA-444 / ADR 0027).

        Computes branch-min seq cutoffs from each table's stored purge
        watermarks — committed ``Pu`` (``last_purged_commit_seq_num``) and
        aborted ``Pa`` (``last_purged_aborted_seq_num``) — as of a
        captured ``cleanup_started_at_micros``, then runs one composite
        DELETE over four disjoint eligibility branches (committed
        ``commit_seq_num <= Pu``, aborted ``aborted_at_seq_num < Pa``,
        pure-begin+abort, and expired-begin wall-clock grace). The
        as-of clamp keeps a concurrent ``Purge`` committing mid-pass
        invisible to the cutoffs.

        Fire-and-forget: the GC spans two independent seq axes, so
        there is no single watermark to report — ``PurgeTxLogResponse``
        is empty. Callers observe the GC through the tx-log-family row
        counts.

        Branch-scoped lock ``purge_tx_log:{branch_uuid}`` — at most
        one pass per branch at a time, orthogonal to per-table
        Persist / Snapshot / Purge.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = PurgeTxLogRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)

        try:
            return self._lifecycle.PurgeTxLog(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def compact_persist_segments(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        min_persisted_at_micros: int | None = None,
        max_persisted_at_micros: int | None = None,
    ) -> CompactPersistSegmentsResponse:
        """Compact persist log segments for a single table.

        Per ``(table_uuid, log_kind)``: walks T's committed persist
        segments, applies size-aware merge selection, and merges each
        log_kind in its own short PG transaction. Idempotent.
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = CompactPersistSegmentsRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        if min_persisted_at_micros is not None or max_persisted_at_micros is not None:
            tf = IntegerRange()
            if min_persisted_at_micros is not None:
                tf.min = min_persisted_at_micros

            if max_persisted_at_micros is not None:
                tf.max = max_persisted_at_micros

            request.persisted_at.CopyFrom(tf)

        try:
            return self._lifecycle.CompactPersistSegments(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def snapshot(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        snapshot_at: datetime | None = None,
    ) -> SnapshotResponse:
        """Create a read-optimized, point-in-time snapshot.

        Returns the snapshot watermark in
        ``SnapshotResponse.snapshotted_at_micros`` (unset when the call
        was a no-op — no new persist data has arrived since the last
        snapshot).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = SnapshotRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        if snapshot_at is not None:
            request.snapshotted_at_micros = datetime_to_micros(snapshot_at)

        try:
            return self._lifecycle.Snapshot(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def sweep_segments(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
    ) -> SweepSegmentsResponse:
        """Physically delete cold segment files queued for removal by
        past compact waves in a catalog (CHA-233 / ADR 0019).

        Compact's merge tx enqueues each replaced URI in
        ``segment_delete_set`` atomically with the URI swap;
        ``sweep_segments`` reads rows whose
        ``written_at_micros + query_timeout < now``, deletes the
        cold file, then deletes the set row. Idempotent.

        Catalog-scoped, not branch-scoped: carry-forward makes one cold
        file reachable from any branch, so the sweep's reference-count
        gate spans the whole catalog (CHA-531).
        """
        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        request = SweepSegmentsRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)

        try:
            return self._lifecycle.SweepSegments(request)
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

    def read_data(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        as_of: datetime | None = None,
        as_of_seq: int | None = None,
        open_tx_uuid: str | None = None,
        columns: list[str] | None = None,
        filter: str | None = None,
        ids: Table | None = None,
        indexes: Table | None = None,
    ) -> Table:
        """Read a table using the canonical read path; return a materialized ``Table``.

        The underlying RPC streams batches over the wire; the client
        collects them so callers get the same ``Table`` shape they had
        before the gRPC rewrite.

        The as-of snapshot is single-axis (CHA-429): pass ``as_of`` (a
        ``datetime``) to snapshot at a commit *time* (``committed_at <=
        as_of``), or ``as_of_seq`` (an ``int``) to snapshot at a commit
        *sequence number* (``commit_seq_num <= as_of_seq``, the per-branch
        gapless commit-order serial surfaced by :meth:`audit_data`). The
        merge orders on the seq axis internally either way; the caller
        just chooses which axis to bound on.

        ``open_tx_uuid`` enables read-your-own-writes for a transaction
        that the caller has open: the read is anchored at the tx's
        BEGIN time and layered with the tx's own uncommitted writes.
        At most one of ``as_of`` / ``as_of_seq`` / ``open_tx_uuid`` may
        be set — RYOW only makes sense at the tx's own begin-time anchor.

        ``ids`` (CHA-398) is a point-lookup restriction: a ``pa.Table``
        of *primary-key columns only, in the table's declared
        ``primary_keys`` order* — the same shape as ``Mutation.deletes``.
        The server validates the batch and derives ``row_uuid`` itself;
        only rows whose primary key matches are returned, AND-composed
        with ``filter`` / ``columns`` / visibility. A 0-row ``ids``
        table raises ``ValueError`` — restrict-to-nothing vs
        unrestricted is ambiguous; pass ``None`` to read unrestricted.

        ``indexes`` (CHA-492) is the secondary-index sibling of ``ids``: a
        ``pa.Table`` of index-key columns carrying the equality values to
        seek. Each column must belong to some defined index; the batch may
        carry the union of several covering indexes' key columns (the server
        selects every index whose full key set is present), and column order
        is not significant (the server binds by name and probes each index in
        its own declared key order). An undefined index column is rejected
        before planning; a materialized index seeks its cold sidecar, a
        defined-but-unmaterialized one falls back to a merge scan. AND-composed
        with the other restrictions. A 0-row batch raises ``ValueError`` (same
        ambiguity as ``ids``).
        """
        if sum(x is not None for x in (as_of, as_of_seq, open_tx_uuid)) > 1:
            msg = "at most one of as_of / as_of_seq / open_tx_uuid may be set"
            raise ValueError(msg)

        if ids is not None and ids.num_rows == 0:
            msg = "ids with 0 rows is ambiguous; pass None to read unrestricted"
            raise ValueError(msg)

        if indexes is not None and indexes.num_rows == 0:
            msg = "indexes with 0 rows is ambiguous; pass None to read unrestricted"
            raise ValueError(msg)

        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = ReadDataRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        if as_of is not None:
            request.commit_micros = datetime_to_micros(as_of)

        if as_of_seq is not None:
            request.commit_seq_num = as_of_seq

        if open_tx_uuid is not None:
            request.open_tx_uuid = open_tx_uuid

        # CHA-180: ``columns`` maps to the three-state ``Projection``
        # wrapper. ``None`` leaves the field unset (servicer returns
        # every user column); a list (including an empty one) wraps in
        # ``Projection`` so an explicit ``[]`` reaches the wire as
        # "0-column projection" instead of collapsing to "no projection."
        if columns is not None:
            request.projection.CopyFrom(Projection(columns=columns))

        if filter is not None:
            request.filter = filter

        if ids is not None:
            request.ids = table_to_ipc_bytes(ids)

        if indexes is not None:
            request.indexes = table_to_ipc_bytes(indexes)

        try:
            batches = [
                ipc_bytes_to_batch(response.data)
                for response in self._query.ReadData(request)
            ]
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return Table.from_batches(batches)

    def audit_data(
        self,
        catalog_uuid: str | None = None,
        catalog_name: str | None = None,
        schema_uuid: str | None = None,
        schema_name: str | None = None,
        branch_uuid: str | None = None,
        branch_name: str | None = None,
        table_uuid: str | None = None,
        table_name: str | None = None,
        after: datetime | None = None,
        before: datetime | None = None,
        after_seq: int | None = None,
        before_seq: int | None = None,
        ids: Table | None = None,
        include_tx_metadata: bool = False,
    ) -> tuple[Table, Table]:
        """Return the audit trail for a table as ``(upserts, deletes)``.

        The underlying RPC streams batches over the wire; the client
        collects them into two materialized ``Table``s — one for upsert
        versions, one for tombstones. Each yielded ``AuditDataResponse``
        carries either ``upserts`` or ``deletes`` (never both); empty
        fields are skipped.

        The committed window is single-axis (CHA-429): pass the *time*
        bounds ``after`` / ``before`` (``datetime``, on ``committed_at``),
        or the *sequence* bounds ``after_seq`` / ``before_seq`` (``int``,
        on ``commit_seq_num``) — not both. Each pair is half-open
        ``[lower, upper)``: ``after`` / ``after_seq`` is the inclusive
        lower bound, ``before`` / ``before_seq`` the exclusive upper, so
        the "changes since N" cursor is ``after_seq = N + 1``.

        ``ids`` (CHA-398) restricts the audit stream to the named rows'
        history — same PK-batch contract as :meth:`read_data`'s ``ids``,
        composing with the committed window. A 0-row ``ids`` table raises
        ``ValueError``.

        Each returned upsert / delete row carries a ``commit_seq_num`` column
        (CHA-430) — the per-tx commit-order serial — alongside the tx
        metadata, usable as the cursor value the ``after_seq`` bound reads.

        ``include_tx_metadata`` (CHA-507) adds the per-tx ``author`` /
        ``comment`` columns, resolved by joining the commit tx log; left
        unset they are omitted (pay-for-what-you-use).
        """
        if ids is not None and ids.num_rows == 0:
            msg = "ids with 0 rows is ambiguous; pass None to audit unrestricted"
            raise ValueError(msg)

        micros_window = after is not None or before is not None
        seq_window = after_seq is not None or before_seq is not None
        if micros_window and seq_window:
            msg = (
                "audit_data committed window is single-axis: pass after/before "
                "(commit time) or after_seq/before_seq (commit seq), not both"
            )
            raise ValueError(msg)

        catalog_uuid, catalog_name = self._get_catalog(catalog_uuid, catalog_name)
        branch_uuid, branch_name = self._get_branch(branch_uuid, branch_name)
        request = AuditDataRequest()
        self._set_optional(request, "catalog_uuid", catalog_uuid)
        self._set_optional(request, "catalog_name", catalog_name)
        self._set_optional(request, "schema_uuid", schema_uuid)
        self._set_optional(request, "schema_name", schema_name)
        self._set_optional(request, "branch_uuid", branch_uuid)
        self._set_optional(request, "branch_name", branch_name)
        self._set_optional(request, "table_uuid", table_uuid)
        self._set_optional(request, "table_name", table_name)
        if after is not None:
            request.commit_micros.min = datetime_to_micros(after)

        if before is not None:
            request.commit_micros.max = datetime_to_micros(before)

        if after_seq is not None:
            request.commit_seq_num.min = after_seq

        if before_seq is not None:
            request.commit_seq_num.max = before_seq

        if ids is not None:
            request.ids = table_to_ipc_bytes(ids)

        request.include_tx_metadata = include_tx_metadata

        upsert_batches: list[RecordBatch] = []
        delete_batches: list[RecordBatch] = []
        try:
            for response in self._query.AuditData(request):
                if response.upserts:
                    upsert_batches.append(ipc_bytes_to_batch(response.upserts))

                if response.deletes:
                    delete_batches.append(ipc_bytes_to_batch(response.deletes))
        except RpcError as e:
            raise rpc_error_to_api_error(e) from e

        return Table.from_batches(upsert_batches), Table.from_batches(delete_batches)

    # Query vs. update are exposed as two methods because ADBC Python's
    # DB-API ``cursor.execute`` unconditionally routes through the query
    # RPC path (``GetFlightInfo`` + ``DoGet``), while Arrow Flight SQL
    # requires DML to go through ``DoPutStatementUpdate``. JDBC drivers
    # (e.g. Dremio's) paper over this by parsing SQL client-side and
    # dispatching per statement; ADBC Python intentionally does not.
    # Splitting at the client lets each method call the right underlying
    # ADBC statement entry point. See ADR 0004 for the full rationale.

    def execute_query(self, sql: str) -> Table:
        """Execute a SQL query over Flight SQL; return a materialized ``Table``.

        Requires the Rust backend — ``PENCA_SQL_URL`` must be set. The
        Python backend has no SQL surface and callers get
        ``NotImplementedError``. The Flight SQL server accepts anonymous
        handshakes; auth is not configured.
        """
        cursor = self._flight_sql_cursor()
        try:
            cursor.execute(sql)
            return cursor.fetch_arrow_table()
        finally:
            cursor.close()

    def execute_stream(self, sql: str) -> Iterator[RecordBatch]:
        """Execute a SQL query and yield ``RecordBatch`` chunks as they arrive.

        Same transport + auth requirements as :meth:`execute_query`.
        Callers that can't afford to materialize the whole result set use
        this path.
        """
        cursor = self._flight_sql_cursor()
        try:
            cursor.execute(sql)
            yield from cursor.fetch_record_batch()
        finally:
            cursor.close()

    def execute_update(self, sql: str) -> int:
        """Execute a SQL DML or transaction-control statement; return rows
        affected (``0`` for ``BEGIN`` / ``COMMIT`` / ``ROLLBACK``).

        Routes through Flight SQL's ``DoPutStatementUpdate`` via the
        low-level ADBC statement handle — ``cursor.execute`` would hit
        the query path instead. DMLs auto-commit when issued outside a
        ``BEGIN`` block; inside one, the SQL server's connection-local
        session cache binds them to a single Penca transaction (lazy
        ``BeginTx`` on the first DML, ``CommitTx`` on ``COMMIT``,
        ``AbortTx`` on ``ROLLBACK``). See ADR 0007.
        """
        cursor = self._flight_sql_cursor()
        try:
            stmt = cursor.adbc_statement
            stmt.set_sql_query(sql)
            return stmt.execute_update()
        finally:
            cursor.close()

    def close(self) -> None:
        """Release the Flight SQL connection, if one was opened."""
        if self._flight_sql_conn is not None:
            self._flight_sql_conn.close()
            self._flight_sql_conn = None

    def _flight_sql_cursor(self) -> AdbcCursor:
        if self._flight_sql_url is None:
            raise NotImplementedError(
                "Flight SQL is not available: PENCA_SQL_URL is unset."
            )

        if self._flight_sql_conn is None:
            # Enable the ADBC cookie middleware so the SQL server's
            # `penca-session-id` Set-Cookie / Cookie round-trip works
            # across multiple statements on the same connection. Without
            # this, penca-sql-server's connection-local session cache
            # can't bind successive statements (e.g. raw-SQL BEGIN ...
            # INSERT ... COMMIT). See ADR 0007 and
            # crates/penca-sql-server/src/session.rs.
            db_kwargs = {
                "adbc.flight.sql.rpc.with_cookie_middleware": "true",
            }
            # CHA-119 / CHA-253: pin the connection's branch + catalog
            # via the ``x-penca-branch`` / ``x-penca-catalog`` gRPC
            # metadata headers. The SQL server reads them at session-
            # mint time; both values are immutable for the session's
            # lifetime — mid-session attempts to change the catalog
            # via ``SetSessionOptions(catalog: …)`` /
            # ``Connection.setCatalog`` no-op when matching the pin
            # and raise ``FAILED_PRECONDITION`` otherwise.
            # ``branch.setter`` / ``catalog.setter`` drop the cached
            # ADBC connection so the next ``_flight_sql_cursor()``
            # reconnects with the new pin.
            if self._branch is not None:
                db_kwargs["adbc.flight.sql.rpc.call_header.x-penca-branch"] = (
                    self._branch
                )

            if self._catalog is not None:
                db_kwargs["adbc.flight.sql.rpc.call_header.x-penca-catalog"] = (
                    self._catalog
                )

            # Open in autocommit mode. With `FlightSqlServerTransaction =
            # Transaction` advertised (CHA-249), the dbapi default
            # `autocommit=False` would send a `BeginTransaction` action on
            # connect, leaving the conn with an open tx that any
            # subsequent explicit SQL `BEGIN` would collide with
            # (nested-tx rejection). PencaClient's transaction surface
            # is the explicit `BEGIN`/`COMMIT`/`ROLLBACK` SQL pattern
            # (Postgres-style), so autocommit=True is the correct
            # default — users open transactions when they want them.
            self._flight_sql_conn = flight_sql_connect(
                f"grpc://{self._flight_sql_url}",
                db_kwargs=db_kwargs,
                autocommit=True,
            )

        return self._flight_sql_conn.cursor()
