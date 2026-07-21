"""[CHA-431] write_seq_num resolution semantics — latest non-deleted wins,
ordered by ``(commit_seq_num, write_seq_num)`` and nothing else.

Within one commit, multiple mutations to the same row are disambiguated by
``write_seq_num`` — an intra-tx ordinal allocated per ``write_data`` call from
the table's lock-free ``write_sequence`` (CHA-431). This retires the old
read-side "deletes processed first" tiebreak.

The resolution *outcomes* CHA-431 specifies hold under the landed mechanism:
CHA-429 made ``commit_seq_num`` the primary axis, and within one tx the separate
``write_data`` calls get distinct call-order ``write_seq_num`` as the secondary,
so latest-wins + the tombstone-shadow order update-then-delete /
insert-then-delete to DELETED and delete-then-upsert to the upsert. So the two
scenario tests here are behavior-PRESERVATION guards — they stay green across
the CHA-431 secondary-axis swap onto ``write_seq_num`` and the dropped read-side
tie special-case.

The genuinely RED check is the write-time invariant CHA-431 introduces: within
ONE batch (single ``write_data``) touching a row with both a delete and an
upsert, the delete must get a strictly lower ``write_seq_num`` than the upsert
(deletes-first), so ``(commit_seq_num, write_seq_num)`` puts the upsert last (replace)
with no tie logic. Fails today (no ``write_seq_num`` column); green after IMPL4.

Every scenario is asserted in BOTH tiers — hot, and after flush→cold — for
hot/cold parity.

Run via ``just integration-test write_seq_num``.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import (
    commit_tx_log_partition,
    delete_log_table,
    upsert_log_table,
)
from psycopg.sql import Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
    setup_schema,
)

_PK_SCHEMA = pa.schema([USER_SCHEMA.field("name")])


def _qi(name: str) -> str:
    return Identifier(name).as_string(None)


def _upsert(table_uuid: str, name: str, value: int = 1) -> Mutation:
    return Mutation(
        table_uuid=table_uuid,
        upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
    )


def _delete(table_uuid: str, name: str) -> Mutation:
    return Mutation(
        table_uuid=table_uuid,
        deletes=pa.table({"name": [name]}, schema=_PK_SCHEMA),
    )


def _replace(table_uuid: str, name: str, value: int) -> Mutation:
    # One BATCH (single write_data) carrying both a delete and an upsert of the
    # same row — deletes-first within the batch gives the upsert a strictly
    # later write_seq_num, so it wins (replace semantics).
    return Mutation(
        table_uuid=table_uuid,
        deletes=pa.table({"name": [name]}, schema=_PK_SCHEMA),
        upserts=pa.table({"name": [name], "value": [value]}, schema=USER_SCHEMA),
    )


def _commit_calls(client, ids: dict, *calls: Mutation) -> None:
    """begin → one ``write_data`` call per ``calls`` element → commit.

    Each element is a separate WriteData WITHIN THE SAME tx (one shared
    ``commit_seq_num``), so the only thing ordering them is ``write_seq_num`` in call
    order — which is exactly what the within-tx red scenarios need.
    """
    tx = client.begin_tx(
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    for mut in calls:
        client.write_data(
            tx.tx_uuid,
            mut,
            catalog_uuid=ids["catalog_uuid"],
            schema_uuid=ids["schema_uuid"],
            branch_uuid=ids["branch_uuid"],
        )

    client.commit_tx(
        tx.tx_uuid, catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )


def _persist_and_purge(client, ids: dict) -> None:
    """Flush committed rows to cold (Persist), advance the snapshot baseline
    (Snapshot), then Purge them out of hot so a read genuinely exercises the
    cold tier. CHA-444 (ADR 0027): Purge advances the read fence ``Pu`` only
    to ``W_snap``, so Snapshot must run first. Both watermark transitions are
    asserted; a no-op would silently leave this a hot read."""
    persisted = client.persist(**ids)
    assert persisted.HasField("persisted_at_micros"), (
        "persist was a no-op; fixture did not move rows cold"
    )
    client.snapshot(**ids)
    purged = client.purge(**ids)
    assert purged.HasField("purged_at_micros"), (
        "purge was a no-op; rows still served from hot"
    )


def _setup_scenarios(client) -> dict:
    """Build the four resolution scenarios on a fresh table; return the ids."""
    schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
    ids = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": main_branch_uuid,
        "table_uuid": table_uuid,
    }

    # (1) insert-then-delete (upsert then delete, separate calls in one penca
    #     tx) → DELETED. The separate write_data calls get distinct call-order
    #     write_seq_num under one shared commit_seq_num, so the delete (later ordinal)
    #     wins — a behavior-preservation guard across the CHA-431 swap.
    _commit_calls(client, ids, _upsert(table_uuid, "i"), _delete(table_uuid, "i"))

    # (2) update-then-delete (row pre-exists; update then delete, one tx) →
    #     DELETED. Same mechanism as (1) — a guard, green today and after.
    _commit_calls(client, ids, _upsert(table_uuid, "u", 1))
    _commit_calls(client, ids, _upsert(table_uuid, "u", 2), _delete(table_uuid, "u"))

    # (3) delete-then-upsert in ONE BATCH → replace (upsert wins). Already green
    #     pre-CHA-431, pinned so a regression in the batch path is caught.
    _commit_calls(client, ids, _upsert(table_uuid, "b", 1))
    _commit_calls(client, ids, _replace(table_uuid, "b", 2))

    # (4) separate calls serialize in call order → last write wins (value 2).
    _commit_calls(client, ids, _upsert(table_uuid, "s", 1), _upsert(table_uuid, "s", 2))

    return ids


def _assert_resolved(client, ids: dict) -> None:
    result = client.read_data(
        catalog_uuid=ids["catalog_uuid"],
        schema_uuid=ids["schema_uuid"],
        table_uuid=ids["table_uuid"],
        branch_uuid=ids["branch_uuid"],
    )
    rows = result.to_pydict() if result.num_rows else {"name": [], "value": []}
    name_to_value = dict(zip(rows["name"], rows["value"], strict=True))
    names = set(name_to_value)

    assert "i" not in names, (
        f"insert-then-delete within one tx must resolve DELETED; got {name_to_value}"
    )
    assert "u" not in names, (
        f"update-then-delete within one tx must resolve DELETED; got {name_to_value}"
    )
    assert name_to_value.get("b") == 2, (
        f"delete-then-upsert in one batch must resolve to the upsert (replace); "
        f"got {name_to_value}"
    )
    assert name_to_value.get("s") == 2, (
        f"separate calls must serialize in call order (last write wins); "
        f"got {name_to_value}"
    )


class TestWriteSeqNumResolution:
    """RT2 — (commit_seq_num, write_seq_num) resolution + hot/cold parity."""

    def test_resolution_hot(self):
        client = make_client()
        ids = _setup_scenarios(client)
        _assert_resolved(client, ids)

    def test_resolution_cold(self):
        client = make_client()
        ids = _setup_scenarios(client)
        _persist_and_purge(client, ids)
        _assert_resolved(client, ids)

    def test_deletes_first_within_batch_orders_write_seq_num(self):
        # The load-bearing write-time invariant (the one CHA-431 actually
        # changes): within ONE batch (single write_data) touching a row with
        # both a delete and an upsert, WriteData allocates the delete's
        # write_seq_num BEFORE the upsert's. So (commit_seq_num, write_seq_num) places
        # the upsert strictly after the delete — replace, with no read-side tie
        # special-case. This is the write-time half of CHA-431 (was the
        # load-bearing red before IMPL4 added the column + deletes-first order).
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        ids = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }
        # One batch carrying delete "x" + upsert "x" (fresh row): the delete
        # lands one row in delete_log, the upsert one row in upsert_log.
        _commit_calls(client, ids, _replace(table_uuid, "x", 2))

        delete_seq = get_pg_driver().execute(
            f"SELECT write_seq_num FROM {_qi(delete_log_table(table_uuid, main_branch_uuid))}"
        )[0][0]
        upsert_seq = get_pg_driver().execute(
            f"SELECT write_seq_num FROM {_qi(upsert_log_table(table_uuid, main_branch_uuid))}"
        )[0][0]
        assert delete_seq < upsert_seq, (
            f"deletes-first: the co-batch delete must get a strictly lower "
            f"write_seq_num than the upsert so the upsert wins (replace); got "
            f"delete={delete_seq} upsert={upsert_seq}"
        )


def _commit_one_row(client, ids: dict, name: str, value: int):
    """begin → one upsert → commit; return the CommitTxResponse."""
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
    resp = client.commit_tx(
        tx.tx_uuid, catalog_uuid=ids["catalog_uuid"], branch_uuid=ids["branch_uuid"]
    )
    return tx.tx_uuid, resp


class TestCommitTxCommitSeqNum:
    """[CHA-505] CommitTxResponse surfaces the committed tx's commit_seq_num, so
    a client actually has a commit-order position to fork a branch from.

    RED on main: CommitTxResponse has no ``commit_seq_num`` field — the field's
    absence IS the feature under test (additive-proto baseline). Green after
    IMPL-1 (proto) + IMPL-2 (populate from CommittedTx).
    """

    def test_commit_tx_returns_commit_seq_num(self):
        client = make_client()
        schema_uuid, table_uuid, catalog_uuid, main_branch_uuid = setup_schema(client)
        ids = {
            "catalog_uuid": catalog_uuid,
            "schema_uuid": schema_uuid,
            "branch_uuid": main_branch_uuid,
            "table_uuid": table_uuid,
        }

        tx1, resp1 = _commit_one_row(client, ids, "a", 1)
        tx2, resp2 = _commit_one_row(client, ids, "b", 2)

        # The response carries an int64 commit_seq_num on the gapless
        # commit-order axis.
        assert isinstance(resp1.commit_seq_num, int)
        assert isinstance(resp2.commit_seq_num, int)
        assert resp1.commit_seq_num >= 0

        # Strictly increasing in commit order (later commit → larger seq).
        assert resp2.commit_seq_num > resp1.commit_seq_num, (
            f"commit_seq_num must increase with commit order; got "
            f"{resp1.commit_seq_num} then {resp2.commit_seq_num}"
        )

        # White-box: the returned seq is the ACTUAL allocated commit_seq_num on
        # the commit_tx_log row for that tx (response/storage agreement).
        part = commit_tx_log_partition(catalog_uuid, main_branch_uuid)
        for tx_uuid, resp in ((tx1, resp1), (tx2, resp2)):
            stored = get_pg_driver().execute(
                f"SELECT commit_seq_num FROM {_qi(part)} WHERE tx_uuid = '{tx_uuid}'"
            )[0][0]
            assert stored == resp.commit_seq_num, (
                f"CommitTxResponse.commit_seq_num ({resp.commit_seq_num}) must match "
                f"the stored commit_tx_log.commit_seq_num ({stored}) for tx {tx_uuid}"
            )
