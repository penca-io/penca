"""[CHA-505] CreateBranch fork point is a commit-order position.

The fork point is ``commit_seq_num`` (exact, gapless) with ``commit_micros`` as
its wall-clock companion — not a ``tx_uuid``. CreateBranch accepts the position
as ``commit_seq_num`` (exact commit) OR ``commit_micros`` (as-of the
latest commit <= T), and records it on ``Branch.fork_commit_seq_num``. An
uncommitted position is a hard INVALID_ARGUMENT.

seq resolves by EXACT match (gapless → a passed seq either is a committed
position or it isn't). micros resolves AS-OF (wall-clock, non-gapless → pick the
latest committed tx at/before T, never an exact match).

RED on main: client.create_branch has no commit_seq_num / commit_micros
kwargs and still requires a positional base_tx_uuid; Branch has no
commit_seq_num — the breaking-API surface is the feature under test. Green
after IMPL-1 (proto), IMPL-3 (resolver + record), IMPL-4 (storage), IMPL-6
(facade/validation).
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client.errors import InvalidRequestError

from .integration_helpers import (
    USER_SCHEMA,
    make_client,
    setup_schema,
)


def _upsert(table_uuid: str, name: str, value: int) -> Mutation:
    return Mutation(
        table_uuid=table_uuid,
        upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
    )


def _commit_row(client, ids: dict, name: str, value: int):
    """begin → one upsert → commit on main; return the CommitTxResponse
    (carries commit_seq_num + commit_micros)."""
    tx = client.begin_tx(
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    client.write_data(
        tx.tx_uuid,
        _upsert(ids["table_uuid"], name, value),
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    return client.commit_tx(
        tx.tx_uuid, catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )


def _fixture(client):
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    ids = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": main_branch_uuid,
        "table_uuid": table_uuid,
    }
    # Three committed positions on main: c1 < c2 < c3 (head).
    c1 = _commit_row(client, ids, "a", 1)
    c2 = _commit_row(client, ids, "b", 2)
    c3 = _commit_row(client, ids, "c", 3)
    # Fixture precondition: strictly increasing commit_micros so the as-of (<=)
    # micros cases resolve unambiguously. commit_micros is microsecond wall-clock
    # and can tie; sequential round-trips make a tie improbable, and this guard
    # makes one fail loudly as a fixture problem rather than as a resolver bug.
    assert c1.commit_micros < c2.commit_micros < c3.commit_micros, (
        "fixture commits must have strictly increasing micros; got "
        f"{c1.commit_micros}, {c2.commit_micros}, {c3.commit_micros}"
    )
    return catalog_uuid, (c1, c2, c3)


class TestForkPointResolution:
    def test_fork_by_seq_records_exact_position(self):
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        branch = client.create_branch(
            "child_seq",
            "test",
            "fork by seq",
            commit_seq_num=c2.commit_seq_num,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c2.commit_seq_num

    def test_fork_by_micros_exact_resolves_that_commit(self):
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        branch = client.create_branch(
            "child_micros_exact",
            "test",
            "fork by micros exact",
            commit_micros=c2.commit_micros,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c2.commit_seq_num

    def test_fork_by_micros_between_commits_resolves_earlier(self):
        """A T strictly between c2 and c3 resolves to c2 — proves as-of (<=),
        not exact-match (=)."""
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        mid = (c2.commit_micros + c3.commit_micros) // 2
        assert c2.commit_micros < mid < c3.commit_micros, (
            "fixture commits too close in wall-clock to place a T strictly "
            f"between them: c2={c2.commit_micros} c3={c3.commit_micros}"
        )
        branch = client.create_branch(
            "child_micros_between",
            "test",
            "fork by micros between",
            commit_micros=mid,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c2.commit_seq_num

    def test_fork_from_head_when_no_position_given(self):
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        branch = client.create_branch(
            "child_head",
            "test",
            "fork from head",
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c3.commit_seq_num

    def test_fork_from_uncommitted_seq_rejected(self):
        client = make_client()
        catalog_uuid, _ = _fixture(client)

        with pytest.raises(InvalidRequestError):
            client.create_branch(
                "child_bad_seq",
                "test",
                "fork uncommitted seq",
                commit_seq_num=10_000_000,
                catalog_uuid=catalog_uuid,
            )

    def test_fork_from_pre_genesis_micros_rejected(self):
        """No committed tx at/before T (T predates genesis) → INVALID_ARGUMENT."""
        client = make_client()
        catalog_uuid, _ = _fixture(client)

        with pytest.raises(InvalidRequestError):
            client.create_branch(
                "child_pre_genesis",
                "test",
                "fork pre-genesis micros",
                commit_micros=1,
                catalog_uuid=catalog_uuid,
            )

    def test_fork_by_future_micros_resolves_to_head(self):
        """A T past the head commit resolves to head (the as-of `<=` upper end):
        forking as-of a future wall-clock = everything committed so far."""
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        branch = client.create_branch(
            "child_future_micros",
            "test",
            "fork by future micros",
            commit_micros=c3.commit_micros + 10_000_000,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c3.commit_seq_num

    def test_fork_with_both_positions_rejected(self):
        """seq and micros are mutually exclusive fork coordinates — the proto
        `oneof` makes both-on-the-wire impossible, so the client facade guards
        against supplying both up front with a ValueError."""
        client = make_client()
        catalog_uuid, (c1, c2, c3) = _fixture(client)

        with pytest.raises(ValueError):
            client.create_branch(
                "child_both",
                "test",
                "fork with both positions",
                commit_seq_num=c2.commit_seq_num,
                commit_micros=c2.commit_micros,
                catalog_uuid=catalog_uuid,
            )
