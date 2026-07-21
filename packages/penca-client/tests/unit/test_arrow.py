"""Unit tests for Arrow IPC serialization round-trips."""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client.arrow import (
    batch_to_ipc_bytes,
    deserialize_schema,
    ipc_bytes_to_batch,
    serialize_schema,
    table_to_ipc_bytes,
)

_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)


def test_schema_round_trips_through_ipc_bytes() -> None:
    payload = serialize_schema(_SCHEMA)
    assert isinstance(payload, bytes)
    assert deserialize_schema(payload).equals(_SCHEMA)


def test_record_batch_round_trips_through_ipc_bytes() -> None:
    batch = pa.record_batch(
        {"name": ["alice", "bob"], "value": [10, 20]},
        schema=_SCHEMA,
    )
    decoded = ipc_bytes_to_batch(batch_to_ipc_bytes(batch))
    assert decoded.schema.equals(_SCHEMA)
    assert decoded.equals(batch)


def test_deserialize_schema_accepts_memoryview() -> None:
    payload = serialize_schema(_SCHEMA)
    # Server-side decoding sees `bytes`, but proto generated stubs hand
    # the client `memoryview` slices in some configurations — both must
    # round-trip.
    assert deserialize_schema(memoryview(payload)).equals(_SCHEMA)


def test_table_to_ipc_bytes_coalesces_multi_chunk_table():
    chunk_a = pa.record_batch({"id": pa.array([1, 2], pa.int64())})
    chunk_b = pa.record_batch({"id": pa.array([3], pa.int64())})
    table = pa.Table.from_batches([chunk_a, chunk_b])

    batch = ipc_bytes_to_batch(table_to_ipc_bytes(table))

    assert batch.num_rows == 3
    assert batch.column("id").to_pylist() == [1, 2, 3]


def test_table_to_ipc_bytes_rejects_zero_row_table():
    empty = pa.table({"id": pa.array([], pa.int64())})

    with pytest.raises(ValueError, match="non-empty"):
        table_to_ipc_bytes(empty)
