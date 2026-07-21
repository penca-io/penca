"""[CHA-460] RT2 — ``QueryService.GetMaxCommitSeqNum`` returns the per-branch
**max committed** ``commit_seq_num`` (the inclusive seq frontier).

This is the capture primitive the SQL statement pin calls at ``GetFlightInfo``
to obtain the seq it pins on — the seq sibling of the (now-removed)
``NowMicros`` clock RPC. It wraps ``MetadataClient::branch_seq_frontier``
(CHA-443), which reads the per-branch ``commit_tx_log_seq_num`` counter and returns
``counter - 1`` (== ``MAX(commit_seq_num)`` over committed txs; genesis ``-1`` on an
empty branch). The value is the **inclusive** max committed seq — pin
``AsOfSeq(N)`` reads ``commit_seq_num <= N`` — NOT the next-to-allocate counter.

Run via ``just integration-test sql_seq_pin``.
"""

from __future__ import annotations

from penca_client.config import ClientSettings
from penca_client.naming import commit_tx_log_partition
from penca_proto.external.v1.query_pb2 import GetMaxCommitSeqNumRequest
from penca_proto.external.v1.query_pb2_grpc import QueryServiceStub
from grpc import insecure_channel
from psycopg.sql import Identifier

from .integration_helpers import get_pg_driver, make_client, setup_schema


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _request(catalog_uuid: str, branch_uuid: str) -> GetMaxCommitSeqNumRequest:
    return GetMaxCommitSeqNumRequest(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)


def _max_committed_seq(catalog_uuid: str, branch_uuid: str) -> int:
    """The branch's max committed ``commit_seq_num`` — what the RPC must return.

    Equals the ``commit_tx_log_seq_num`` counter minus one (gapless, no in-flight tx),
    sourced here straight off ``commit_tx_log`` so the assertion is independent of the
    counter-read path the RPC uses.
    """
    rows = get_pg_driver().execute(
        f"SELECT MAX(commit_seq_num) FROM {_qi(commit_tx_log_partition(catalog_uuid, branch_uuid))}"
    )
    return rows[0][0]


def _stub() -> QueryServiceStub:
    settings = ClientSettings()  # ty: ignore[missing-argument]
    return QueryServiceStub(insecure_channel(settings.query_url))


class TestGetMaxCommitSeqNum:
    def test_get_max_commit_seq_num_returns_committed_frontier(self):
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        # A few more commits beyond genesis/schema/table advance the frontier.
        for i in range(3):
            client.create_schema(
                f"more_{i}", catalog_uuid=catalog_uuid, author="t", comment="c"
            )

        expected = _max_committed_seq(catalog_uuid, main_branch_uuid)
        got = (
            _stub()
            .GetMaxCommitSeqNum(_request(catalog_uuid, main_branch_uuid))
            .max_commit_seq_num
        )
        assert got == expected, (got, expected)

    def test_get_max_commit_seq_num_isolated_per_branch(self):
        client = make_client()
        _schema_uuid, _table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)

        child = client.create_branch(
            "child",
            catalog_uuid=catalog_uuid,
            author="t",
            comment="fork",
        )
        # Commit only on main after the fork → main's frontier advances, the
        # child's stays at its fork commit.
        for i in range(2):
            client.create_schema(
                f"main_only_{i}", catalog_uuid=catalog_uuid, author="t", comment="c"
            )

        stub = _stub()
        main_got = stub.GetMaxCommitSeqNum(
            _request(catalog_uuid, main_branch_uuid)
        ).max_commit_seq_num
        child_got = stub.GetMaxCommitSeqNum(
            _request(catalog_uuid, child.branch_uuid)
        ).max_commit_seq_num

        assert main_got == _max_committed_seq(catalog_uuid, main_branch_uuid)
        assert child_got == _max_committed_seq(catalog_uuid, child.branch_uuid)
        # Per-branch isolation: main advanced past the fork, the child did not.
        assert main_got > child_got, (main_got, child_got)
