from penca_proto.external.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetCatalogRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ...) -> None: ...

class GetCatalogResponse(_message.Message):
    __slots__ = ("catalog",)
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    catalog: _common_pb2.Catalog
    def __init__(self, catalog: _Optional[_Union[_common_pb2.Catalog, _Mapping]] = ...) -> None: ...

class ListCatalogsRequest(_message.Message):
    __slots__ = ("owner", "pagination")
    OWNER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    owner: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, owner: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListCatalogsResponse(_message.Message):
    __slots__ = ("catalogs", "next_page_token")
    CATALOGS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    catalogs: _containers.RepeatedCompositeFieldContainer[_common_pb2.Catalog]
    next_page_token: str
    def __init__(self, catalogs: _Optional[_Iterable[_Union[_common_pb2.Catalog, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetBranchRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ...) -> None: ...

class GetBranchResponse(_message.Message):
    __slots__ = ("branch",)
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    branch: _common_pb2.Branch
    def __init__(self, branch: _Optional[_Union[_common_pb2.Branch, _Mapping]] = ...) -> None: ...

class ListBranchesRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "pagination")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListBranchesResponse(_message.Message):
    __slots__ = ("branches", "next_page_token")
    BRANCHES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    branches: _containers.RepeatedCompositeFieldContainer[_common_pb2.Branch]
    next_page_token: str
    def __init__(self, branches: _Optional[_Iterable[_Union[_common_pb2.Branch, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetSchemaRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "open_tx_uuid", "branch_uuid", "branch_name", "as_of_micros", "as_of_seq")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    AS_OF_SEQ_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    open_tx_uuid: str
    branch_uuid: str
    branch_name: str
    as_of_micros: int
    as_of_seq: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., open_tx_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., as_of_micros: _Optional[int] = ..., as_of_seq: _Optional[int] = ...) -> None: ...

class GetSchemaResponse(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: _common_pb2.Schema
    def __init__(self, schema: _Optional[_Union[_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class ListSchemasRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "pagination", "open_tx_uuid", "branch_uuid", "branch_name", "as_of_micros", "as_of_seq")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    AS_OF_SEQ_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    pagination: _common_pb2.PaginationRequest
    open_tx_uuid: str
    branch_uuid: str
    branch_name: str
    as_of_micros: int
    as_of_seq: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ..., open_tx_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., as_of_micros: _Optional[int] = ..., as_of_seq: _Optional[int] = ...) -> None: ...

class ListSchemasResponse(_message.Message):
    __slots__ = ("schemas", "next_page_token")
    SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    schemas: _containers.RepeatedCompositeFieldContainer[_common_pb2.Schema]
    next_page_token: str
    def __init__(self, schemas: _Optional[_Iterable[_Union[_common_pb2.Schema, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetTableRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "open_tx_uuid", "as_of_micros", "as_of_seq")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    AS_OF_SEQ_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    open_tx_uuid: str
    as_of_micros: int
    as_of_seq: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., open_tx_uuid: _Optional[str] = ..., as_of_micros: _Optional[int] = ..., as_of_seq: _Optional[int] = ...) -> None: ...

class GetTableResponse(_message.Message):
    __slots__ = ("table",)
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: _common_pb2.Table
    def __init__(self, table: _Optional[_Union[_common_pb2.Table, _Mapping]] = ...) -> None: ...

class ListTablesRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "pagination", "open_tx_uuid", "as_of_micros", "as_of_seq")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    AS_OF_SEQ_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    pagination: _common_pb2.PaginationRequest
    open_tx_uuid: str
    as_of_micros: int
    as_of_seq: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ..., open_tx_uuid: _Optional[str] = ..., as_of_micros: _Optional[int] = ..., as_of_seq: _Optional[int] = ...) -> None: ...

class ListTablesResponse(_message.Message):
    __slots__ = ("tables", "next_page_token")
    TABLES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tables: _containers.RepeatedCompositeFieldContainer[_common_pb2.Table]
    next_page_token: str
    def __init__(self, tables: _Optional[_Iterable[_Union[_common_pb2.Table, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetIndexRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "index_uuid", "index_name", "open_tx_uuid", "as_of_micros")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    index_uuid: str
    index_name: str
    open_tx_uuid: str
    as_of_micros: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., index_uuid: _Optional[str] = ..., index_name: _Optional[str] = ..., open_tx_uuid: _Optional[str] = ..., as_of_micros: _Optional[int] = ...) -> None: ...

class GetIndexResponse(_message.Message):
    __slots__ = ("index",)
    INDEX_FIELD_NUMBER: _ClassVar[int]
    index: _common_pb2.Index
    def __init__(self, index: _Optional[_Union[_common_pb2.Index, _Mapping]] = ...) -> None: ...

class ListIndexesRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "open_tx_uuid", "as_of_micros")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AS_OF_MICROS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    open_tx_uuid: str
    as_of_micros: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., open_tx_uuid: _Optional[str] = ..., as_of_micros: _Optional[int] = ...) -> None: ...

class ListIndexesResponse(_message.Message):
    __slots__ = ("indexes",)
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    indexes: _containers.RepeatedCompositeFieldContainer[_common_pb2.Index]
    def __init__(self, indexes: _Optional[_Iterable[_Union[_common_pb2.Index, _Mapping]]] = ...) -> None: ...

class Projection(_message.Message):
    __slots__ = ("columns",)
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, columns: _Optional[_Iterable[str]] = ...) -> None: ...

class ReadDataRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "commit_micros", "commit_seq_num", "open_tx_uuid", "projection", "filter", "ids", "indexes")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    OPEN_TX_UUID_FIELD_NUMBER: _ClassVar[int]
    PROJECTION_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    commit_micros: int
    commit_seq_num: int
    open_tx_uuid: str
    projection: Projection
    filter: str
    ids: bytes
    indexes: bytes
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., commit_micros: _Optional[int] = ..., commit_seq_num: _Optional[int] = ..., open_tx_uuid: _Optional[str] = ..., projection: _Optional[_Union[Projection, _Mapping]] = ..., filter: _Optional[str] = ..., ids: _Optional[bytes] = ..., indexes: _Optional[bytes] = ...) -> None: ...

class ReadDataResponse(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    def __init__(self, data: _Optional[bytes] = ...) -> None: ...

class AuditDataRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "commit_micros", "commit_seq_num", "ids", "include_tx_metadata")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_TX_METADATA_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    commit_micros: _common_pb2.IntegerRange
    commit_seq_num: _common_pb2.IntegerRange
    ids: bytes
    include_tx_metadata: bool
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., commit_micros: _Optional[_Union[_common_pb2.IntegerRange, _Mapping]] = ..., commit_seq_num: _Optional[_Union[_common_pb2.IntegerRange, _Mapping]] = ..., ids: _Optional[bytes] = ..., include_tx_metadata: bool = ...) -> None: ...

class AuditDataResponse(_message.Message):
    __slots__ = ("upserts", "deletes")
    UPSERTS_FIELD_NUMBER: _ClassVar[int]
    DELETES_FIELD_NUMBER: _ClassVar[int]
    upserts: bytes
    deletes: bytes
    def __init__(self, upserts: _Optional[bytes] = ..., deletes: _Optional[bytes] = ...) -> None: ...

class GetMaxCommitSeqNumRequest(_message.Message):
    __slots__ = ("catalog_uuid", "branch_uuid")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    branch_uuid: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ...) -> None: ...

class GetMaxCommitSeqNumResponse(_message.Message):
    __slots__ = ("max_commit_seq_num",)
    MAX_COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    max_commit_seq_num: int
    def __init__(self, max_commit_seq_num: _Optional[int] = ...) -> None: ...
