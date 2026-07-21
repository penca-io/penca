"""Unit tests for ``penca_client.types``.

Focused on :class:`Mutation` — the rest of the dataclasses are thin
proto-to-Python wrappers exercised end-to-end by the integration
suite.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation
from penca_client.arrow import ipc_bytes_to_batch

_USER_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)
_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])


def _upserts_table(rows: dict[str, list[object]]) -> pa.Table:
    return pa.table(rows, schema=_USER_SCHEMA)


def _deletes_table(names: list[str]) -> pa.Table:
    return pa.table({"name": names}, schema=_PK_SCHEMA_NAME)


def test_mutation_re_exported_from_package_root() -> None:
    """Mutation is part of the public surface alongside PencaClient."""
    import penca_client

    assert penca_client.Mutation is Mutation


def test_to_proto_upserts_only() -> None:
    """Upserts side encoded; deletes side empty."""
    upserts = _upserts_table({"name": ["alice", "bob"], "value": [1, 2]})
    proto = Mutation(table_uuid="tbl", upserts=upserts).to_proto()

    assert proto.deletes == b""
    decoded = ipc_bytes_to_batch(proto.upserts)
    assert decoded.schema.equals(_USER_SCHEMA)
    assert decoded.column("name").to_pylist() == ["alice", "bob"]
    assert decoded.column("value").to_pylist() == [1, 2]


def test_to_proto_deletes_only() -> None:
    """Deletes side encoded as a PK-only batch; upserts side empty."""
    deletes = _deletes_table(["alice"])
    proto = Mutation(table_uuid="tbl", deletes=deletes).to_proto()

    assert proto.upserts == b""
    decoded = ipc_bytes_to_batch(proto.deletes)
    assert decoded.schema.names == ["name"]
    assert decoded.column("name").to_pylist() == ["alice"]


def test_to_proto_both_sides() -> None:
    """Upserts + deletes in one Change."""
    upserts = _upserts_table({"name": ["alice"], "value": [1]})
    deletes = _deletes_table(["bob"])
    proto = Mutation(
        table_uuid="tbl",
        upserts=upserts,
        deletes=deletes,
    ).to_proto()

    assert ipc_bytes_to_batch(proto.upserts).column("name").to_pylist() == ["alice"]
    assert ipc_bytes_to_batch(proto.deletes).column("name").to_pylist() == ["bob"]


def test_to_proto_neither_side() -> None:
    """Empty Mutation produces a Change with both sides as empty bytes
    — the server's row-count gate treats it as a no-op for both the
    delete log and the (tx, table) index."""
    proto = Mutation(table_uuid="tbl").to_proto()

    assert proto.upserts == b""
    assert proto.deletes == b""


def test_to_proto_zero_row_table_serializes_to_empty_bytes() -> None:
    """A schema-only (zero-row) ``pa.Table`` is treated as "no rows on
    this side" — equivalent to ``None``. Skips the IPC encoding and
    surfaces as empty bytes so the server doesn't ship a schema
    header for a no-op write."""
    empty_upserts = _upserts_table({"name": [], "value": []})
    empty_deletes = _deletes_table([])
    proto = Mutation(
        table_uuid="tbl",
        upserts=empty_upserts,
        deletes=empty_deletes,
    ).to_proto()

    assert proto.upserts == b""
    assert proto.deletes == b""


def test_to_proto_coalesces_multi_chunk_table() -> None:
    """Tables built from multiple appends end up multi-chunk. The
    server's IPC reader expects a single batch per stream (mirroring
    ``insert_upserts`` and ``insert_delete_pk_batches``), so
    ``to_proto`` coalesces via ``combine_chunks()`` before encoding."""
    chunk_a = pa.record_batch({"name": ["alice"], "value": [1]}, schema=_USER_SCHEMA)
    chunk_b = pa.record_batch({"name": ["bob"], "value": [2]}, schema=_USER_SCHEMA)
    multi_chunk = pa.Table.from_batches([chunk_a, chunk_b])
    assert multi_chunk.column("name").num_chunks == 2  # sanity

    proto = Mutation(table_uuid="tbl", upserts=multi_chunk).to_proto()

    decoded = ipc_bytes_to_batch(proto.upserts)
    assert decoded.num_rows == 2
    assert decoded.column("name").to_pylist() == ["alice", "bob"]


def test_table_identity_lives_on_mutation_not_change_payload() -> None:
    """CHA-475: the table identity moved to the request level. ``to_proto``
    produces a payload-only ``Change``; the client lifts the Mutation's
    ``table_uuid`` / ``table_name`` onto the WriteDataRequest."""
    mutation = Mutation(
        table_name="users",
        upserts=_upserts_table({"name": ["alice"], "value": [1]}),
    )
    # Identity stays on the Mutation for write_data to lift onto the request.
    assert mutation.table_name == "users"
    assert mutation.table_uuid is None
    # The wire Change carries only the row payload.
    proto = mutation.to_proto()
    assert ipc_bytes_to_batch(proto.upserts).column("name").to_pylist() == ["alice"]
    assert proto.deletes == b""
