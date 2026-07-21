"""Arrow IPC serialization helpers.

Centralises Schema ↔ bytes and RecordBatch ↔ bytes conversion so
callers don't repeat BufferReader / BufferOutputStream boilerplate.
"""

from __future__ import annotations

from io import BytesIO

from pyarrow import BufferOutputStream, BufferReader, RecordBatch, Schema, Table
from pyarrow.ipc import new_stream, open_stream, read_schema


def deserialize_schema(data: bytes | memoryview) -> Schema:
    """Deserialize IPC wire bytes into a PyArrow Schema."""
    return read_schema(BufferReader(bytes(data)))


def serialize_schema(arrow_schema: Schema) -> bytes:
    """Serialize a PyArrow Schema to IPC wire bytes."""
    sink = BufferOutputStream()
    writer = new_stream(sink, arrow_schema)
    writer.close()
    return sink.getvalue().to_pybytes()


def batch_to_ipc_bytes(batch: RecordBatch) -> bytes:
    """Serialize a single RecordBatch to Arrow IPC stream bytes."""
    sink = BytesIO()
    with new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)

    return sink.getvalue()


def ipc_bytes_to_batch(data: bytes) -> RecordBatch:
    """Decode Arrow IPC stream bytes carrying exactly one RecordBatch."""
    with open_stream(BufferReader(data)) as reader:
        batches = list(reader)

    return batches[0]


def table_to_ipc_bytes(table: Table) -> bytes:
    """Coalesce a (possibly multi-chunk) ``Table`` into a single Arrow
    IPC ``RecordBatch`` payload — the wire contract shared by
    ``Mutation.upserts``/``deletes`` and the ``ids`` point-lookup
    fields (the server's IPC reader expects one batch per stream)."""
    if table.num_rows == 0:
        msg = 'table_to_ipc_bytes requires a non-empty Table; encode empty as b"" at the call site'
        raise ValueError(msg)

    return batch_to_ipc_bytes(table.combine_chunks().to_batches()[0])
