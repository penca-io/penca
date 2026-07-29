"""CHA-430: commit_seq_num on the cold path — stamped onto persist segments
and surfaced as a column from ``audit_data``.

``test_audit_data_carries_commit_seq_num_hot_and_cold`` (AC1 + AC3): the
audit upsert/delete schemas carry a ``commit_seq_num`` column whose value
matches the commit-time seq, and a row read from cold (after persist +
purge) carries the IDENTICAL seq it had in hot (hot/cold agreement).

The ``since`` cursor that *filters* audit on this column is the
read-surface work in CHA-429 (``committed`` oneOf over an IntegerRange,
shared with ReadData's ``as_of`` axis), not this ticket — CHA-430 only
stamps the value and surfaces it for reading.
"""

from __future__ import annotations

import pyarrow as pa
from penca_client import Mutation

from .integration_helpers import (
    USER_SCHEMA,
    make_client,
    setup_with_data,
)

_PK_SCHEMA_NAME = pa.schema([pa.field("name", pa.utf8())])


def _commit_upsert(client, ctx, names, values, *, author="a", comment="c"):
    """Commit one tx upserting ``names``/``values``; return the commit response."""
    tx = client.begin_tx(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        author=author,
        comment=comment,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=ctx["table_uuid"],
            upserts=pa.table({"name": names, "value": values}, schema=USER_SCHEMA),
        ),
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )

    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=ctx["catalog_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )


def _commit_delete(client, ctx, names, *, author="a", comment="c"):
    """Commit one tx deleting ``names`` by PK; return the commit response."""
    tx = client.begin_tx(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        author=author,
        comment=comment,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=ctx["table_uuid"],
            deletes=pa.table({"name": names}, schema=_PK_SCHEMA_NAME),
        ),
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )

    return client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=ctx["catalog_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
    )


def _audit(client, ctx, **kwargs):
    return client.audit_data(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        table_uuid=ctx["table_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        **kwargs,
    )


def _persist_and_purge(client, ctx):
    """Flush committed rows to cold (Persist), advance the snapshot baseline
    (Snapshot), then Purge them out of hot so the re-audit genuinely reads
    cold-stamped rows.

    Persist alone leaves the rows queryable from hot until Purge runs, so
    a cold assertion after only Persist can pass reading hot↔hot and never
    exercise the cold stamp. CHA-444 (ADR 0027): Purge advances the read
    fence ``Pu`` only to ``W_snap``, so a Snapshot must run before Purge can
    clear the committed hot rows. All three ops are no-op-capable (unset
    response watermark); assert each transition actually happened —
    otherwise the cross-tier assertions would silently pass against hot."""
    persist_response = client.persist(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        table_uuid=ctx["table_uuid"],
    )
    assert persist_response.HasField("persisted_at_micros"), (
        "persist was a no-op; fixture did not move rows cold"
    )
    client.snapshot(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        table_uuid=ctx["table_uuid"],
    )
    purge_response = client.purge(
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        table_uuid=ctx["table_uuid"],
    )
    assert purge_response.HasField("purged_at_micros"), (
        "purge was a no-op; rows still served from hot"
    )


def _seq_by_name(table):
    """Map each upsert/delete row's ``name`` -> its ``commit_seq_num``.

    Asserts names are unique first: if audit ever returned both a hot and
    a cold copy of the same row (e.g. a cold read that didn't actually
    displace hot), the duplicates would otherwise collapse silently to
    whichever batch the server emitted last — masking the very hot/cold
    divergence these tests exist to catch."""
    names = table.column("name").to_pylist()
    seqs = table.column("commit_seq_num").to_pylist()
    assert len(names) == len(set(names)), f"duplicate names in audit output: {names}"

    return dict(zip(names, seqs, strict=True))


class TestAuditCommitSeqNum:
    def test_audit_data_carries_commit_seq_num_hot_and_cold(self):
        """AC1 + AC3: audit_data surfaces ``commit_seq_num`` per row, the value
        is gapless-monotonic in commit order, and the SAME row read from
        cold (after persist) carries the IDENTICAL seq it had in hot."""
        client = make_client()
        ctx = setup_with_data(client)  # tx0 commits alice + bob (shared seq)

        _commit_upsert(client, ctx, ["carol"], [30])  # tx1
        _commit_upsert(client, ctx, ["dave"], [40])  # tx2
        _commit_delete(client, ctx, ["bob"])  # tx3 (delete-log row)

        # all-hot audit
        upserts_hot, deletes_hot = _audit(client, ctx)
        assert "commit_seq_num" in upserts_hot.schema.names, (
            "audit_data upserts must carry a commit_seq_num column (CHA-430)"
        )
        assert "commit_seq_num" in deletes_hot.schema.names, (
            "audit_data deletes must carry a commit_seq_num column (CHA-430)"
        )
        hot_map = _seq_by_name(upserts_hot)
        # alice + bob share tx0's seq; carol (tx1) and dave (tx2) strictly higher.
        assert hot_map["alice"] == hot_map["bob"]
        assert hot_map["bob"] < hot_map["carol"] < hot_map["dave"]
        # The delete (tx3) lands after every upsert tx on the seq axis.
        deletes_hot_map = _seq_by_name(deletes_hot)
        assert deletes_hot_map["bob"] > hot_map["dave"]

        # --- flush hot -> cold + purge hot, then re-audit (rows now
        # genuinely served from cold, not lingering hot copies) ---
        _persist_and_purge(client, ctx)
        upserts_cold, deletes_cold = _audit(client, ctx)
        assert "commit_seq_num" in upserts_cold.schema.names
        assert "commit_seq_num" in deletes_cold.schema.names
        cold_map = _seq_by_name(upserts_cold)
        # Hot/cold agreement: the stamped cold seq equals the hot seq per row,
        # for both the upsert segment and the (separate) cold delete segment.
        assert cold_map == hot_map
        assert _seq_by_name(deletes_cold) == deletes_hot_map


def _audit_upsert_names_committed_seq_min(client, ctx, min_seq):
    """Audit on the seq axis via the ``audit_data`` facade's ``after_seq``
    argument (CHA-429 I7) — the facade builds the
    ``committed{commit_seq_num}`` arm internally. Returns the set of upsert
    ``name``s in the ``[min_seq, inf)`` window."""
    upserts, _deletes = _audit(client, ctx, after_seq=min_seq)
    # An over-pruning regression that drops every upsert must surface as an
    # empty `name` set against `expected`, not blow up; the facade returns an
    # empty Table (no `name` column) when no rows match.
    if upserts.num_rows == 0:
        return set()

    return set(upserts.column("name").to_pylist())


class TestAuditCommittedSeqWindow:
    """CHA-429 RT: ``audit_data`` ``committed{commit_seq_num: IntegerRange{min: N+1}}``
    returns exactly the txs with ``commit_seq_num > N`` across a
    cold(persist+purge)+hot horizon — the "changes since N" cursor.

    RED until I6: I1 ships the ``commit_seq_num`` committed arm but the
    penca-api boundary ignores it (only the micros window is wired), so the
    full audit horizon comes back and rows with seq <= N leak in. A
    behavioral red (wrong row set), not a field/import error.
    """

    def test_audit_committed_seq_window_hot_and_cold(self):
        client = make_client()
        ctx = setup_with_data(client)  # tx0: alice + bob (shared seq S)
        _commit_upsert(client, ctx, ["carol"], [30])  # tx1: seq S+1
        _persist_and_purge(client, ctx)  # alice, bob, carol -> cold
        _commit_upsert(client, ctx, ["dave"], [40])  # hot: seq S+2
        _commit_upsert(client, ctx, ["erin"], [50])  # hot: seq S+3

        upserts_all, _deletes = _audit(client, ctx)
        seq = _seq_by_name(upserts_all)  # name -> seq, across cold+hot
        # N = tx0's seq (alice/bob). The window {seq > N} spans a COLD row
        # (carol) plus the HOT rows (dave, erin), exercising cold seq pruning
        # AND the hot predicate together.
        n = seq["alice"]
        assert seq["bob"] == n and seq["carol"] > n and seq["dave"] > n, (
            f"fixture seq layout unexpected: {seq}"
        )
        expected = {name for name, s in seq.items() if s > n}

        got = _audit_upsert_names_committed_seq_min(client, ctx, n + 1)
        assert got == expected, (
            f"committed seq window min={n + 1} must return exactly {expected}; "
            f"got {got} (seq map: {seq})"
        )
