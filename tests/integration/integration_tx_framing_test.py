"""[CHA-222] Tx framing is internal; timestamps are flat on BeginTx / CommitTx /
AbortTx responses, and on auto-commit WriteData / MergeBranch.

The 12 named acceptance tests in this file fail against the pre-CHA-222 shapes:

* ``BeginTxResponse`` today wraps ``Tx tx = 1``; this file asserts it carries
  ``tx_uuid`` / ``began_at_micros`` / ``expires_at_micros`` directly.
* ``CommitTxResponse`` / ``AbortTxResponse`` today wrap ``Tx``; this file
  asserts they carry ``commit_micros`` / ``aborted_at_micros`` directly.
* ``WriteDataResponse`` today carries ``optional Tx tx = 1``; this file asserts
  it carries ``optional int64 commit_micros = 1`` directly, set on
  auto-commit and unset on append (FIXME(CHA-157) semantics preserved on the
  new field name).
* ``MergeBranchResponse`` today carries ``Tx merge_tx = 1``; this file asserts
  it carries ``int64 commit_micros = 1`` directly.
* ``QueryService.GetTx`` / ``ListTxs`` and the ``Tx`` message are removed
  entirely.
* The Python client drops ``get_tx`` / ``list_txs`` along with the RPCs.
* ``audit_data`` already surfaces ``comment`` / ``author`` per row (post-CHA-218
  denormalization), so it is the canonical replacement for callers that today
  read ``merge_tx.comment`` / ``merge_tx.author``.

Run via ``just integration-test integration_tx_framing``.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pyarrow as pa
import pytest
from google.protobuf import descriptor_pool
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.client import PencaClient

from .integration_helpers import (
    USER_SCHEMA,
    make_client,
    setup_schema,
)


class TestTxFramingFlattened:
    # 1
    def test_begin_tx_response_carries_flat_tx_uuid_began_expires(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        response = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Returned message is the flat BeginTxResponse, not the old Tx wrapper.
        assert response.DESCRIPTOR.name == "BeginTxResponse"
        UUID(response.tx_uuid)
        assert response.began_at_micros > 0
        assert response.expires_at_micros > response.began_at_micros

    # 2
    def test_begin_tx_echoes_client_supplied_tx_uuid(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        client_uuid = str(uuid4())

        response = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            tx_uuid=client_uuid,
        )

        assert response.DESCRIPTOR.name == "BeginTxResponse"
        assert response.tx_uuid == client_uuid

    # 3
    def test_commit_tx_response_carries_flat_commit_micros(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        begin = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        response = client.commit_tx(
            begin.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        assert response.DESCRIPTOR.name == "CommitTxResponse"
        assert response.commit_micros >= begin.began_at_micros

    # 4
    def test_abort_tx_response_carries_flat_aborted_at_micros(self):
        client = make_client()
        schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        begin = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        response = client.abort_tx(
            begin.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_branch_uuid,
        )

        # Today the wrapper returns None; after CHA-222 it returns the flat
        # AbortTxResponse with the aborted_at_micros watermark.
        assert response.DESCRIPTOR.name == "AbortTxResponse"
        assert response.aborted_at_micros > begin.began_at_micros

    # 5
    def test_write_data_auto_commit_returns_commit_micros_directly(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        response = client.write_data(
            None,  # auto-commit
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
            author="t",
            comment="auto-commit write",
        )

        assert response.HasField("commit_micros")
        # The flat watermark is usable as as_of on a subsequent read_data.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=main_branch_uuid,
            as_of=micros_to_datetime(response.commit_micros),
        )
        assert result.column("name").to_pylist() == ["alice"]

    # 6
    def test_write_data_append_returns_no_commit_micros(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        begin = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        response = client.write_data(
            begin.tx_uuid,  # append
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=main_branch_uuid,
        )

        # FIXME(CHA-157) semantics preserved on the new field: append path
        # leaves commit_micros unset.
        assert not response.HasField("commit_micros")

    # 7
    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_merge_branch_response_carries_flat_commit_micros(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "tgt_flat",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="create_branch",
        )
        source = client.create_branch(
            "src_flat",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="t",
            comment="create_branch",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        response = client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
        )

        assert response.DESCRIPTOR.name == "MergeBranchResponse"
        assert response.commit_micros > 0
        # Usable as a time-travel pin on target.
        result = client.read_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
            as_of=micros_to_datetime(response.commit_micros),
        )
        assert result.column("name").to_pylist() == ["alice"]

    # 8
    def test_get_tx_rpc_removed(self):
        # Generated grpc stubs bind methods in __init__, not on the class, so
        # checking the proto-level ServiceDescriptor is the load-bearing
        # assertion that the RPC itself is gone from the service definition.
        service = descriptor_pool.Default().FindServiceByName(
            "penca_proto.external.v1.QueryService"
        )
        method_names = {m.name for m in service.methods}
        assert "GetTx" not in method_names

    # 9
    def test_list_txs_rpc_removed(self):
        service = descriptor_pool.Default().FindServiceByName(
            "penca_proto.external.v1.QueryService"
        )
        method_names = {m.name for m in service.methods}
        assert "ListTxs" not in method_names

    # 10
    def test_tx_message_removed_from_descriptor_pool(self):
        pool = descriptor_pool.Default()
        with pytest.raises(KeyError):
            pool.FindMessageTypeByName("penca_proto.external.v1.Tx")

    # 11
    def test_python_client_has_no_get_tx_or_list_txs(self):
        assert not hasattr(PencaClient, "get_tx")
        assert not hasattr(PencaClient, "list_txs")

    # 12
    @pytest.mark.skip(
        reason="CHA-509: branch-off-branch (fork off a non-main branch) is disabled "
        "by the CHA-515 main-only guard; re-enable when multi-level inheritance lands."
    )
    def test_audit_data_surfaces_comment_and_author_for_merge_tx(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        target = client.create_branch(
            "tgt_audit",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="create_branch",
        )
        source = client.create_branch(
            "src_audit",
            catalog_uuid=catalog_uuid,
            source_branch_uuid=target.branch_uuid,
            author="t",
            comment="create_branch",
        )

        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        batch = pa.table(
            {"name": ["alice"], "value": [1]},
            schema=USER_SCHEMA,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=source.branch_uuid,
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=catalog_uuid,
            branch_uuid=source.branch_uuid,
        )

        merge = client.merge_branch(
            source_branch_uuid=source.branch_uuid,
            target_branch_uuid=target.branch_uuid,
            catalog_uuid=catalog_uuid,
            comment="merge-comment-X",
            author="merge-author-Y",
        )

        # New flat shape required (today merge_branch returns Tx).
        assert merge.DESCRIPTOR.name == "MergeBranchResponse"

        # The merge metadata must surface in audit_data so callers that today
        # read merge_tx.comment / merge_tx.author can migrate to audit_data
        # over the merge's commit_micros window.
        after = micros_to_datetime(merge.commit_micros)
        before = micros_to_datetime(merge.commit_micros + 1)
        upserts, _deletes = client.audit_data(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=target.branch_uuid,
            after=after,
            before=before,
            include_tx_metadata=True,
        )
        assert "merge-comment-X" in upserts.column("comment").to_pylist()
        assert "merge-author-Y" in upserts.column("author").to_pylist()
