from penca_proto.external.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CreateCatalogRequest(_message.Message):
    __slots__ = ("catalog_name", "owner", "description")
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    catalog_name: str
    owner: str
    description: str
    def __init__(self, catalog_name: _Optional[str] = ..., owner: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class CreateCatalogResponse(_message.Message):
    __slots__ = ("catalog_uuid", "main_branch_uuid")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    MAIN_BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    main_branch_uuid: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., main_branch_uuid: _Optional[str] = ...) -> None: ...

class UpdateCatalogRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "new_catalog_name", "owner", "description")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    NEW_CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    new_catalog_name: str
    owner: str
    description: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., new_catalog_name: _Optional[str] = ..., owner: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class UpdateCatalogResponse(_message.Message):
    __slots__ = ("catalog",)
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    catalog: _common_pb2.Catalog
    def __init__(self, catalog: _Optional[_Union[_common_pb2.Catalog, _Mapping]] = ...) -> None: ...

class DeleteCatalogRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ...) -> None: ...

class DeleteCatalogResponse(_message.Message):
    __slots__ = ("catalog_uuid",)
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    def __init__(self, catalog_uuid: _Optional[str] = ...) -> None: ...

class CreateBranchRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_name", "branch_uuid", "source_branch_uuid", "source_branch_name", "comment", "author", "commit_seq_num", "commit_micros")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_name: str
    branch_uuid: str
    source_branch_uuid: str
    source_branch_name: str
    comment: str
    author: str
    commit_seq_num: int
    commit_micros: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., source_branch_uuid: _Optional[str] = ..., source_branch_name: _Optional[str] = ..., comment: _Optional[str] = ..., author: _Optional[str] = ..., commit_seq_num: _Optional[int] = ..., commit_micros: _Optional[int] = ...) -> None: ...

class CreateBranchResponse(_message.Message):
    __slots__ = ("branch",)
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    branch: _common_pb2.Branch
    def __init__(self, branch: _Optional[_Union[_common_pb2.Branch, _Mapping]] = ...) -> None: ...

class DeleteBranchRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_uuid", "branch_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_uuid: str
    branch_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ...) -> None: ...

class DeleteBranchResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class MergeBranchRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "source_branch_uuid", "source_branch_name", "target_branch_uuid", "target_branch_name", "comment", "author")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGET_BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    TARGET_BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    source_branch_uuid: str
    source_branch_name: str
    target_branch_uuid: str
    target_branch_name: str
    comment: str
    author: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., source_branch_uuid: _Optional[str] = ..., source_branch_name: _Optional[str] = ..., target_branch_uuid: _Optional[str] = ..., target_branch_name: _Optional[str] = ..., comment: _Optional[str] = ..., author: _Optional[str] = ...) -> None: ...

class MergeBranchResponse(_message.Message):
    __slots__ = ("commit_micros",)
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    commit_micros: int
    def __init__(self, commit_micros: _Optional[int] = ...) -> None: ...

class UpdateBranchRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_uuid", "branch_name", "new_branch_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    NEW_BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_uuid: str
    branch_name: str
    new_branch_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., new_branch_name: _Optional[str] = ...) -> None: ...

class UpdateBranchResponse(_message.Message):
    __slots__ = ("branch",)
    BRANCH_FIELD_NUMBER: _ClassVar[int]
    branch: _common_pb2.Branch
    def __init__(self, branch: _Optional[_Union[_common_pb2.Branch, _Mapping]] = ...) -> None: ...

class CreateSchemaRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_name", "description", "default_retention_config", "tx_uuid", "branch_uuid", "branch_name", "author", "comment")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_name: str
    description: str
    default_retention_config: _common_pb2.RetentionConfig
    tx_uuid: str
    branch_uuid: str
    branch_name: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_name: _Optional[str] = ..., description: _Optional[str] = ..., default_retention_config: _Optional[_Union[_common_pb2.RetentionConfig, _Mapping]] = ..., tx_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class CreateSchemaResponse(_message.Message):
    __slots__ = ("schema_uuid",)
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    schema_uuid: str
    def __init__(self, schema_uuid: _Optional[str] = ...) -> None: ...

class UpdateSchemaRequest(_message.Message):
    __slots__ = ("schema_uuid", "schema_name", "new_schema_name", "description", "default_retention_config", "catalog_uuid", "catalog_name", "tx_uuid", "branch_uuid", "branch_name", "author", "comment")
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    NEW_SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    schema_uuid: str
    schema_name: str
    new_schema_name: str
    description: str
    default_retention_config: _common_pb2.RetentionConfig
    catalog_uuid: str
    catalog_name: str
    tx_uuid: str
    branch_uuid: str
    branch_name: str
    author: str
    comment: str
    def __init__(self, schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., new_schema_name: _Optional[str] = ..., description: _Optional[str] = ..., default_retention_config: _Optional[_Union[_common_pb2.RetentionConfig, _Mapping]] = ..., catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class UpdateSchemaResponse(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: _common_pb2.Schema
    def __init__(self, schema: _Optional[_Union[_common_pb2.Schema, _Mapping]] = ...) -> None: ...

class DeleteSchemaRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "tx_uuid", "author", "comment")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class DeleteSchemaResponse(_message.Message):
    __slots__ = ("schema_uuid",)
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    schema_uuid: str
    def __init__(self, schema_uuid: _Optional[str] = ...) -> None: ...

class CreateTableRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_name", "arrow_schema", "primary_keys", "partition_keys", "clustering_keys", "description", "retention_config", "tx_uuid", "author", "comment", "indexes")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEYS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEYS_FIELD_NUMBER: _ClassVar[int]
    CLUSTERING_KEYS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_name: str
    arrow_schema: bytes
    primary_keys: _containers.RepeatedScalarFieldContainer[str]
    partition_keys: _containers.RepeatedScalarFieldContainer[str]
    clustering_keys: _containers.RepeatedScalarFieldContainer[str]
    description: str
    retention_config: _common_pb2.RetentionConfig
    tx_uuid: str
    author: str
    comment: str
    indexes: _containers.RepeatedCompositeFieldContainer[CreateTableIndexDefinition]
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_name: _Optional[str] = ..., arrow_schema: _Optional[bytes] = ..., primary_keys: _Optional[_Iterable[str]] = ..., partition_keys: _Optional[_Iterable[str]] = ..., clustering_keys: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., retention_config: _Optional[_Union[_common_pb2.RetentionConfig, _Mapping]] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ..., indexes: _Optional[_Iterable[_Union[CreateTableIndexDefinition, _Mapping]]] = ...) -> None: ...

class CreateTableResponse(_message.Message):
    __slots__ = ("table_uuid",)
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    table_uuid: str
    def __init__(self, table_uuid: _Optional[str] = ...) -> None: ...

class CreateTableIndexDefinition(_message.Message):
    __slots__ = ("index_name", "columns", "index_type")
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    index_type: _common_pb2.IndexType
    def __init__(self, index_name: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., index_type: _Optional[_Union[_common_pb2.IndexType, str]] = ...) -> None: ...

class CreateIndexRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "index_name", "columns", "index_type", "tx_uuid", "author", "comment")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    index_name: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    index_type: _common_pb2.IndexType
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., index_name: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., index_type: _Optional[_Union[_common_pb2.IndexType, str]] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ("index_uuid",)
    INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    index_uuid: str
    def __init__(self, index_uuid: _Optional[str] = ...) -> None: ...

class UpdateIndexRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "index_uuid", "index_name", "new_index_name", "tx_uuid", "author", "comment")
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
    NEW_INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
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
    new_index_name: str
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., index_uuid: _Optional[str] = ..., index_name: _Optional[str] = ..., new_index_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class UpdateIndexResponse(_message.Message):
    __slots__ = ("index_uuid",)
    INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    index_uuid: str
    def __init__(self, index_uuid: _Optional[str] = ...) -> None: ...

class DeleteIndexRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "index_uuid", "index_name", "tx_uuid", "author", "comment")
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
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
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
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., index_uuid: _Optional[str] = ..., index_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class DeleteIndexResponse(_message.Message):
    __slots__ = ("index_uuid",)
    INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    index_uuid: str
    def __init__(self, index_uuid: _Optional[str] = ...) -> None: ...

class UpdateTableRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "new_table_name", "arrow_schema", "primary_keys", "partition_keys", "clustering_keys", "description", "retention_config", "tx_uuid", "author", "comment")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NEW_TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEYS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEYS_FIELD_NUMBER: _ClassVar[int]
    CLUSTERING_KEYS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    new_table_name: str
    arrow_schema: bytes
    primary_keys: _containers.RepeatedScalarFieldContainer[str]
    partition_keys: _containers.RepeatedScalarFieldContainer[str]
    clustering_keys: _containers.RepeatedScalarFieldContainer[str]
    description: str
    retention_config: _common_pb2.RetentionConfig
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., new_table_name: _Optional[str] = ..., arrow_schema: _Optional[bytes] = ..., primary_keys: _Optional[_Iterable[str]] = ..., partition_keys: _Optional[_Iterable[str]] = ..., clustering_keys: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., retention_config: _Optional[_Union[_common_pb2.RetentionConfig, _Mapping]] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class UpdateTableResponse(_message.Message):
    __slots__ = ("table",)
    TABLE_FIELD_NUMBER: _ClassVar[int]
    table: _common_pb2.Table
    def __init__(self, table: _Optional[_Union[_common_pb2.Table, _Mapping]] = ...) -> None: ...

class DeleteTableRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "tx_uuid", "author", "comment")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    tx_uuid: str
    author: str
    comment: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ...) -> None: ...

class DeleteTableResponse(_message.Message):
    __slots__ = ("table_uuid",)
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    table_uuid: str
    def __init__(self, table_uuid: _Optional[str] = ...) -> None: ...

class BeginTxRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "comment", "author", "tx_uuid", "timeout_seconds")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    comment: str
    author: str
    tx_uuid: str
    timeout_seconds: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., comment: _Optional[str] = ..., author: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., timeout_seconds: _Optional[int] = ...) -> None: ...

class BeginTxResponse(_message.Message):
    __slots__ = ("tx_uuid", "began_at_micros", "expires_at_micros")
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    BEGAN_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    tx_uuid: str
    began_at_micros: int
    expires_at_micros: int
    def __init__(self, tx_uuid: _Optional[str] = ..., began_at_micros: _Optional[int] = ..., expires_at_micros: _Optional[int] = ...) -> None: ...

class CommitTxRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_uuid", "branch_name", "tx_uuid")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_uuid: str
    branch_name: str
    tx_uuid: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ...) -> None: ...

class CommitTxResponse(_message.Message):
    __slots__ = ("commit_micros", "commit_seq_num")
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    commit_micros: int
    commit_seq_num: int
    def __init__(self, commit_micros: _Optional[int] = ..., commit_seq_num: _Optional[int] = ...) -> None: ...

class AbortTxRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_uuid", "branch_name", "tx_uuid")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_uuid: str
    branch_name: str
    tx_uuid: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ...) -> None: ...

class AbortTxResponse(_message.Message):
    __slots__ = ("aborted_at_micros",)
    ABORTED_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    aborted_at_micros: int
    def __init__(self, aborted_at_micros: _Optional[int] = ...) -> None: ...

class Change(_message.Message):
    __slots__ = ("upserts", "deletes")
    UPSERTS_FIELD_NUMBER: _ClassVar[int]
    DELETES_FIELD_NUMBER: _ClassVar[int]
    upserts: bytes
    deletes: bytes
    def __init__(self, upserts: _Optional[bytes] = ..., deletes: _Optional[bytes] = ...) -> None: ...

class WriteDataRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "tx_uuid", "author", "comment", "change")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TX_UUID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    tx_uuid: str
    author: str
    comment: str
    change: Change
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., tx_uuid: _Optional[str] = ..., author: _Optional[str] = ..., comment: _Optional[str] = ..., change: _Optional[_Union[Change, _Mapping]] = ...) -> None: ...

class WriteDataResponse(_message.Message):
    __slots__ = ("commit_micros",)
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    commit_micros: int
    def __init__(self, commit_micros: _Optional[int] = ...) -> None: ...
