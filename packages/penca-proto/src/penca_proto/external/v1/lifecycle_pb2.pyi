from penca_proto.external.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PersistRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "target_micros")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGET_MICROS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    target_micros: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., target_micros: _Optional[int] = ...) -> None: ...

class PersistResponse(_message.Message):
    __slots__ = ("persisted_at_micros",)
    PERSISTED_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    persisted_at_micros: int
    def __init__(self, persisted_at_micros: _Optional[int] = ...) -> None: ...

class Watermark(_message.Message):
    __slots__ = ("commit_seq_num", "commit_micros")
    COMMIT_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    COMMIT_MICROS_FIELD_NUMBER: _ClassVar[int]
    commit_seq_num: int
    commit_micros: int
    def __init__(self, commit_seq_num: _Optional[int] = ..., commit_micros: _Optional[int] = ...) -> None: ...

class BranchOpRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "branch_uuid", "branch_name", "target")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    branch_uuid: str
    branch_name: str
    target: Watermark
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., target: _Optional[_Union[Watermark, _Mapping]] = ...) -> None: ...

class BranchOpResponse(_message.Message):
    __slots__ = ("watermark",)
    WATERMARK_FIELD_NUMBER: _ClassVar[int]
    watermark: Watermark
    def __init__(self, watermark: _Optional[_Union[Watermark, _Mapping]] = ...) -> None: ...

class PurgeRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ...) -> None: ...

class PurgeResponse(_message.Message):
    __slots__ = ("purged_at_micros",)
    PURGED_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    purged_at_micros: int
    def __init__(self, purged_at_micros: _Optional[int] = ...) -> None: ...

class CompactPersistSegmentsRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "persisted_at")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    PERSISTED_AT_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    persisted_at: _common_pb2.IntegerRange
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., persisted_at: _Optional[_Union[_common_pb2.IntegerRange, _Mapping]] = ...) -> None: ...

class CompactPersistSegmentsResponse(_message.Message):
    __slots__ = ("merged_object_uris",)
    MERGED_OBJECT_URIS_FIELD_NUMBER: _ClassVar[int]
    merged_object_uris: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, merged_object_uris: _Optional[_Iterable[str]] = ...) -> None: ...

class SnapshotRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name", "schema_uuid", "schema_name", "branch_uuid", "branch_name", "table_uuid", "table_name", "snapshotted_at_micros")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_UUID_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_UUID_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOTTED_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    schema_uuid: str
    schema_name: str
    branch_uuid: str
    branch_name: str
    table_uuid: str
    table_name: str
    snapshotted_at_micros: int
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ..., schema_uuid: _Optional[str] = ..., schema_name: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., branch_name: _Optional[str] = ..., table_uuid: _Optional[str] = ..., table_name: _Optional[str] = ..., snapshotted_at_micros: _Optional[int] = ...) -> None: ...

class SnapshotResponse(_message.Message):
    __slots__ = ("snapshotted_at_micros",)
    SNAPSHOTTED_AT_MICROS_FIELD_NUMBER: _ClassVar[int]
    snapshotted_at_micros: int
    def __init__(self, snapshotted_at_micros: _Optional[int] = ...) -> None: ...

class PurgeTxLogRequest(_message.Message):
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

class PurgeTxLogResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SweepSegmentsRequest(_message.Message):
    __slots__ = ("catalog_uuid", "catalog_name")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    CATALOG_NAME_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    catalog_name: str
    def __init__(self, catalog_uuid: _Optional[str] = ..., catalog_name: _Optional[str] = ...) -> None: ...

class SweepSegmentsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListModifiedTablesRequest(_message.Message):
    __slots__ = ("catalog_uuid", "branch_uuid", "modified_at", "pagination")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    MODIFIED_AT_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    branch_uuid: str
    modified_at: _common_pb2.IntegerRange
    pagination: _common_pb2.PaginationRequest
    def __init__(self, catalog_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., modified_at: _Optional[_Union[_common_pb2.IntegerRange, _Mapping]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListModifiedTablesResponse(_message.Message):
    __slots__ = ("table_uuids", "next_page_token")
    TABLE_UUIDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    table_uuids: _containers.RepeatedScalarFieldContainer[str]
    next_page_token: str
    def __init__(self, table_uuids: _Optional[_Iterable[str]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ListPersistedTablesRequest(_message.Message):
    __slots__ = ("catalog_uuid", "branch_uuid", "persisted_at", "pagination")
    CATALOG_UUID_FIELD_NUMBER: _ClassVar[int]
    BRANCH_UUID_FIELD_NUMBER: _ClassVar[int]
    PERSISTED_AT_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    catalog_uuid: str
    branch_uuid: str
    persisted_at: _common_pb2.IntegerRange
    pagination: _common_pb2.PaginationRequest
    def __init__(self, catalog_uuid: _Optional[str] = ..., branch_uuid: _Optional[str] = ..., persisted_at: _Optional[_Union[_common_pb2.IntegerRange, _Mapping]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListPersistedTablesResponse(_message.Message):
    __slots__ = ("table_uuids", "next_page_token")
    TABLE_UUIDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    table_uuids: _containers.RepeatedScalarFieldContainer[str]
    next_page_token: str
    def __init__(self, table_uuids: _Optional[_Iterable[str]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
