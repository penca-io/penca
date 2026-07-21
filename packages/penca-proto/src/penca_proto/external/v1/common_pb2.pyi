from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_TYPE_UNKNOWN: _ClassVar[IndexType]
    INDEX_TYPE_SCALAR_BTREE: _ClassVar[IndexType]
INDEX_TYPE_UNKNOWN: IndexType
INDEX_TYPE_SCALAR_BTREE: IndexType

class PaginationRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class IntegerRange(_message.Message):
    __slots__ = ("min", "max")
    MIN_FIELD_NUMBER: _ClassVar[int]
    MAX_FIELD_NUMBER: _ClassVar[int]
    min: int
    max: int
    def __init__(self, min: _Optional[int] = ..., max: _Optional[int] = ...) -> None: ...

class RetentionConfig(_message.Message):
    __slots__ = ("retention_duration_seconds", "snapshot_density_seconds")
    RETENTION_DURATION_SECONDS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_DENSITY_SECONDS_FIELD_NUMBER: _ClassVar[int]
    retention_duration_seconds: int
    snapshot_density_seconds: int
    def __init__(self, retention_duration_seconds: _Optional[int] = ..., snapshot_density_seconds: _Optional[int] = ...) -> None: ...

class Catalog(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "owner", "description")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    owner: str
    description: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., owner: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class Schema(_message.Message):
    __slots__ = ("schema_uuid", "catalog_uuid", "schema_name", "description", "default_retention_config")
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    schema_uuid: str
    catalog_uuid: str
    schema_name: str
    description: str
    default_retention_config: RetentionConfig
    def __init__(self, schema_uuid: _Optional[str] = ..., catalog_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., description: _Optional[str] = ..., default_retention_config: _Optional[_Union[RetentionConfig, _Mapping]] = ...) -> None: ...

class Table(_message.Message):
    __slots__ = ("table_uuid", "schema_uuid", "table_name", "arrow_schema", "primary_keys", "partition_keys", "clustering_keys", "description", "retention_config", "catalog_uuid", "indexes")
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ARROW_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEYS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEYS_FIELD_NUMBER: _ClassVar[int]
    CLUSTERING_KEYS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RETENTION_CONFIG_FIELD_NUMBER: _ClassVar[int]
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    table_uuid: str
    schema_uuid: str
    table_name: str
    arrow_schema: bytes
    primary_keys: _containers.RepeatedScalarFieldContainer[str]
    partition_keys: _containers.RepeatedScalarFieldContainer[str]
    clustering_keys: _containers.RepeatedScalarFieldContainer[str]
    description: str
    retention_config: RetentionConfig
    catalog_uuid: str
    indexes: _containers.RepeatedCompositeFieldContainer[Index]
    def __init__(self, table_uuid: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., arrow_schema: _Optional[bytes] = ..., primary_keys: _Optional[_Iterable[str]] = ..., partition_keys: _Optional[_Iterable[str]] = ..., clustering_keys: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., retention_config: _Optional[_Union[RetentionConfig, _Mapping]] = ..., catalog_uuid: _Optional[str] = ..., indexes: _Optional[_Iterable[_Union[Index, _Mapping]]] = ...) -> None: ...

class Index(_message.Message):
    __slots__ = ("index_uuid", "table_uuid", "index_name", "columns", "index_type")
    INDEX_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    index_uuid: str
    table_uuid: str
    index_name: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    index_type: IndexType
    def __init__(self, index_uuid: _Optional[str] = ..., table_uuid: _Optional[str] = ..., index_name: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., index_type: _Optional[_Union[IndexType, str]] = ...) -> None: ...

class Branch(_message.Message):
    __slots__ = ("branch_uuid", "catalog_uuid", "branch_name", "fork_commit_seq_num")
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    FORK_COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    branch_uuid: str
    catalog_uuid: str
    branch_name: str
    fork_commit_seq_num: int
    def __init__(self, branch_uuid: _Optional[str] = ..., catalog_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., fork_commit_seq_num: _Optional[int] = ...) -> None: ...
