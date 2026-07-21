"""[CHA-507] A fork position survives ``PurgeTxLog``.

``resolve_fork_watermark`` (CHA-505) queries only the **hot** ``commit_tx_log``,
which ``PurgeTxLog`` GCs once the position is persisted. So a legitimately
committed fork position — ``(commit_seq_num, commit_micros)`` — resolves to
``INVALID_ARGUMENT`` after purge even though the data is durably in cold.

CHA-507 persists a slim cold ``tx_log`` (written first in
``persist_and_snapshot_branch``) and gives ``resolve_fork_watermark`` a cold
fallback, so a purged-but-committed position still resolves.

RED on ``main``: after a full persist → purge → PurgeTxLog drain the hot
``commit_tx_log`` row for the fork position is gone, and both
``create_branch(commit_seq_num=...)`` and ``create_branch(commit_micros=...)``
raise ``InvalidRequestError``. GREEN after IMPL-2 (``persist_tx_log`` writes the
cold ``tx_log``), IMPL-3 (it runs first in ``persist_and_snapshot_branch``),
IMPL-6 (``resolve_fork_watermark`` cold fallback), IMPL-7 (``PurgeTxLog`` clamp).

Drain shape mirrors ``integration_purge_tx_log_test.py`` — the branch-level
``persist_and_snapshot_branch`` is what makes the GREEN path durable; the
per-table ``purge`` + multi-pass ``PurgeTxLog`` is what makes the hot row vanish.

Run via ``just integration-test``.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    commit_tx_log_partition,
    system_schemas_table_uuid,
    system_tables_table_uuid,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
    setup_schema,
)


def _commit_row(client, ids, name: str, value: int):
    """begin → one upsert → commit on main; return ``(tx_uuid, CommitTxResponse)``
    (the response carries ``commit_seq_num`` + ``commit_micros``)."""
    tx = client.begin_tx(
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=ids["table_uuid"],
            upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
        ),
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    committed = client.commit_tx(
        tx.tx_uuid, catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )
    return tx.tx_uuid, committed


def _count_commit_tx_log_rows(catalog_uuid, branch_uuid, tx_uuid) -> int:
    part = commit_tx_log_partition(catalog_uuid, branch_uuid)
    return get_pg_driver().execute(
        SQL("SELECT count(*) FROM {} WHERE tx_uuid = %s").format(Identifier(part)),
        (tx_uuid,),
    )[0][0]


def _drain_to_cold(client, ids):
    """Make everything committed at branch head durable in cold and GC the hot
    ``commit_tx_log`` rows.

    1. ``persist_and_snapshot_branch`` — on GREEN this runs ``persist_tx_log``
       first (writing the cold ``tx_log``), then persists + snapshots every
       modified table. On ``main`` it is just the branch persist+snapshot.
    2. per-table ``purge`` on the user table + the two ``__penca_system__``
       tables — advances each table's committed fence ``Pu`` (needed so
       ``MIN(Pu over S)`` reaches the user commits; see the purge suite).
    3. multi-pass ``PurgeTxLog`` — drains the historical fork/CreateTable rows
       out of ``S`` so ``Pu`` reaches the user commits, GCing their hot
       ``commit_tx_log`` rows.
    """
    client.persist_and_snapshot_branch(
        catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )
    for table_uuid in (
        ids["table_uuid"],
        system_tables_table_uuid(ids["catalog_uuid"]),
        system_schemas_table_uuid(ids["catalog_uuid"]),
    ):
        client.purge(
            catalog_uuid=ids["catalog_uuid"],
            schema_uuid=ids["schema_uuid"],
            branch_uuid=ids["branch_uuid"],
            table_uuid=table_uuid,
        )

    for _ in range(3):
        client.purge_tx_log(
            catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
        )


def _fixture_purged_fork_point(client):
    """Commit c1 < c2 < c3 on main, drain to cold, and return
    ``(catalog_uuid, ids, c1_tx, c1)`` with c1's hot ``commit_tx_log`` row GC'd.
    """
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    ids = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": main_branch_uuid,
        "table_uuid": table_uuid,
    }
    c1_tx, c1 = _commit_row(client, ids, "a", 1)
    _c2_tx, c2 = _commit_row(client, ids, "b", 2)
    _c3_tx, c3 = _commit_row(client, ids, "c", 3)
    assert c1.commit_micros < c2.commit_micros < c3.commit_micros, (
        "fixture commits must have strictly increasing micros; got "
        f"{c1.commit_micros}, {c2.commit_micros}, {c3.commit_micros}"
    )

    _drain_to_cold(client, ids)

    assert _count_commit_tx_log_rows(catalog_uuid, main_branch_uuid, c1_tx) == 0, (
        "precondition: c1's hot commit_tx_log row must be GC'd by the drain, so "
        "the fork position is resolvable only from cold."
    )
    return catalog_uuid, ids, c1_tx, c1


class TestForkPointSurvivesPurge:
    def test_fork_by_seq_from_purged_position_resolves_from_cold(self):
        client = make_client()
        catalog_uuid, _ids, _c1_tx, c1 = _fixture_purged_fork_point(client)

        branch = client.create_branch(
            "child_seq_purged",
            "test",
            "fork by seq from purged position",
            commit_seq_num=c1.commit_seq_num,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c1.commit_seq_num

    def test_fork_by_micros_from_purged_position_resolves_from_cold(self):
        client = make_client()
        catalog_uuid, _ids, _c1_tx, c1 = _fixture_purged_fork_point(client)

        branch = client.create_branch(
            "child_micros_purged",
            "test",
            "fork by micros from purged position",
            commit_micros=c1.commit_micros,
            catalog_uuid=catalog_uuid,
        )
        assert branch.fork_commit_seq_num == c1.commit_seq_num
