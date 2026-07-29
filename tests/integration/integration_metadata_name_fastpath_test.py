"""CHA-484 red-tests — by-name metadata fast-path (direct seek, DataFusion bypass).

By-name metadata resolves over the snapshot-covered ``__penca_system__``
tables (``get_table(table_name=…)``, ``get_schema(schema_name=…)``,
``get_index(index_name=…)``) must seek the CHA-481 built-in composite name
index through the CHA-482 internal ``seeks`` path, taking the snapshot-only
DataFusion bypass — emitting the ``direct_point_read=true`` debug marker
(CHA-380 unified metadata reads onto ``read_data``'s shared cold-read kernel,
so both emit the same marker; the CHA-476 count-pinned tests stay stable
because those tests never snapshot-cover the system tables, so their metadata
resolves don't fast-path).

Fail-first: the marker does not exist today — by-name resolves ride
``stream_merged`` with a ``format!("l.table_name = '…'")`` filter string, so
the marker assertions in tests 1–3 fail on current ``main``. The DDL-churn and
never-covered tests are green regression guards: a system table with hot rows
is NOT snapshot-only, must fall back (correct results, no marker), and must
keep doing so after the rewire.

Scoped run::

    just integration-test metadata_name_fastpath
"""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from penca_client.naming import (
    system_indexes_table_uuid,
    system_schema_uuid,
    system_schemas_table_uuid,
    system_tables_table_uuid,
)

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    container_log,
    make_client,
    poll_log_for,
)

# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
pytestmark = pytest.mark.serial

# The DataFusion-free seek bypass marker — a `tracing` fmt-subscriber unquoted
# bool field. CHA-380 unified metadata reads onto read_data's kernel, so this
# is the same ``direct_point_read=true`` read_data emits.
_DIRECT_MARKER = "direct_point_read=true"


def _poll_resolve_closes(span: str, since: int, want: int) -> int:
    """Poll the query-log window until it holds >= ``want`` CLOSE lines for
    ``span`` (``PENCA_SPAN_TIMING`` renders one per resolve); returns the
    observed count.

    Flush barrier for NEGATIVE marker assertions: log lines are written in
    order, so once a resolve's span-CLOSE line is flushed, any marker event
    that resolve emitted is already present in the window — a plain count over
    the window is then immune to flush-lag races (the barrier pattern from
    ``integration_direct_point_read_test.py``, adapted to a line that exists
    on both the fallback and bypass paths).
    """
    deadline = time.monotonic() + 5.0
    closes = 0
    while time.monotonic() < deadline:
        closes = sum(
            1
            for line in container_log("query")[since:].splitlines()
            if "close time.busy" in line and span in line
        )
        if closes >= want:
            break

        time.sleep(0.2)

    return closes


def _snapshot_cover(
    client, catalog_uuid: str, branch_uuid: str, sys_table_uuid: str
) -> int:
    """Persist → snapshot → purge one ``__penca_system__`` table so its plan
    is snapshot-only (CHA-444: snapshot precedes purge so Pu <= W_snap).
    Returns the snapshot's server-side ``snapshotted_at_micros`` watermark —
    a valid ``as_of_micros`` pin at which pre-cover DDL rows are visible."""
    kw = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": system_schema_uuid(catalog_uuid),
        "branch_uuid": branch_uuid,
        "table_uuid": sys_table_uuid,
    }
    client.persist(**kw)
    resp = client.snapshot(**kw)
    client.purge(**kw)
    # Proto3-optional: unset (a no-op snapshot) reads as 0 — fail the fixture
    # loudly rather than letting a 0 leak into an as_of_micros pin.
    assert resp.snapshotted_at_micros, (
        "snapshotting a populated system table must materialize a cold "
        "snapshot (got a no-op watermark)"
    )
    return resp.snapshotted_at_micros


def _new_catalog_schema_tables(client) -> dict:
    """Fresh catalog + schema ``s`` + tables ``t_alpha``/``t_beta``; returns
    their uuids."""
    catalog_uuid, branch_uuid = client.create_catalog(
        f"cha484_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="create_schema"
    )
    table_uuids = {}
    for name in ("t_alpha", "t_beta"):
        table_uuids[name] = client.create_table(
            name,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            author="test",
            comment="create_table",
        )

    return {
        "catalog_uuid": catalog_uuid,
        "branch_uuid": branch_uuid,
        "schema_uuid": schema_uuid,
        "table_uuids": table_uuids,
    }


class TestByNameDirectSeek:
    """A by-name resolve over a snapshot-covered system table takes the
    direct seek bypass (``direct_point_read=true``) and stays correct."""

    def test_by_name_get_table_snapshot_covered_takes_direct_seek(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)
            _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_tables_table_uuid(ctx["catalog_uuid"]),
            )

            since = len(container_log("query"))
            info = client.get_table(
                table_name="t_alpha",
                schema_uuid=ctx["schema_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.table_uuid == ctx["table_uuids"]["t_alpha"]
            assert info.table_name == "t_alpha"
            assert info.schema_uuid == ctx["schema_uuid"]
            assert poll_log_for("query", since, _DIRECT_MARKER), (
                "a by-name get_table over a snapshot-covered "
                "__penca_system__.tables must be served by the direct "
                f"name-index seek ({_DIRECT_MARKER}); not emitted today "
                "(served via stream_merged + l.table_name filter string)"
            )
        finally:
            client.close()

    def test_by_name_get_schema_snapshot_covered_takes_direct_seek(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)
            _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_schemas_table_uuid(ctx["catalog_uuid"]),
            )

            since = len(container_log("query"))
            info = client.get_schema(
                schema_name="s",
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.schema_uuid == ctx["schema_uuid"]
            assert info.schema_name == "s"
            assert info.catalog_uuid == ctx["catalog_uuid"]
            assert poll_log_for("query", since, _DIRECT_MARKER), (
                "a by-name get_schema over a snapshot-covered "
                "__penca_system__.schemas must be served by the direct "
                f"name-index seek ({_DIRECT_MARKER}); not emitted today "
                "(served via stream_merged + l.schema_name filter string)"
            )
        finally:
            client.close()

    def test_by_name_get_index_snapshot_covered_takes_direct_seek(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)
            table_uuid = ctx["table_uuids"]["t_alpha"]
            index_uuid = client.create_index(
                index_name="idx_name",
                columns=["name"],
                index_type=SCALAR_BTREE,
                table_uuid=table_uuid,
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                author="test",
                comment="create_index",
            )
            _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_indexes_table_uuid(ctx["catalog_uuid"]),
            )

            since = len(container_log("query"))
            info = client.get_index(
                index_name="idx_name",
                table_uuid=table_uuid,
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.index_uuid == index_uuid
            assert info.index_name == "idx_name"
            assert poll_log_for("query", since, _DIRECT_MARKER), (
                "a (table_uuid, index_name) get_index over a snapshot-covered "
                "__penca_system__.indexes must be served by the direct "
                f"name-index seek ({_DIRECT_MARKER}); not emitted today "
                "(served via stream_merged + l.index_name filter string)"
            )
        finally:
            client.close()


class TestByNameSeekGate:
    """CHA-501: the direct-seek gate is **axis-independent** — a by-name resolve
    over a snapshot-only system table takes the seek regardless of the read axis
    (default current-time, explicit time-travel `as_of`, or open-tx snapshot).

    This SUPERSEDES the CHA-484 conservative gate, which restricted the seek to
    `LatestSeq` out of a worry that "the latest snapshot's name index could serve
    the wrong row" for a historical/since-renamed resolve. That worry doesn't
    hold: the planner resolves the **as_of-bounded** snapshot
    (`hot_min_and_snapshot_pick`'s seq/as_of clause), so the seek reads the
    as_of-appropriate snapshot's own name index — not "the latest." And the
    correctness backstop is the CHA-473 loose existence gate: a snapshot-only plan
    provably has no hot overlay, so the seek is exact on any axis.

    The must-stay-green counter-guards live elsewhere: a table the tx has WRITTEN
    keeps its uncommitted hot row → `EXISTS(upsert)` true → not snapshot-only →
    no seek (CHA-471 over Flight SQL for the open-tx-DDL RYOW case;
    `TestByNameFallback` for the committed-churn case)."""

    def test_time_travel_and_open_tx_resolves_take_direct_seek(self):
        """CHA-501 red: today the shared `is_direct_seek_eligible` is
        `LatestSeq`-only, so the as_of and open-tx resolves ride `stream_merged`
        and emit no `direct_point_read`. After the axis-independent widening, all
        three qualifying resolves — default, time-travel, and open-tx of a table
        the tx has NOT written — take the direct seek."""
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)
            snap_micros = _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_tables_table_uuid(ctx["catalog_uuid"]),
            )
            resolve_kw = {
                "schema_uuid": ctx["schema_uuid"],
                "catalog_uuid": ctx["catalog_uuid"],
                "branch_uuid": ctx["branch_uuid"],
            }

            # 1) default current-time (`LatestSeq`) — the always-eligible control.
            since = len(container_log("query"))
            info = client.get_table(table_name="t_alpha", **resolve_kw)
            assert info.table_uuid == ctx["table_uuids"]["t_alpha"]
            assert poll_log_for("query", since, _DIRECT_MARKER), (
                "qualifying default current-time by-name resolve must take "
                "the direct seek"
            )

            # 2) time-travel (`AsOfMicros`) at the snapshot's own watermark:
            # t_alpha predates it and is fully cold, so the as_of-bounded snapshot
            # the planner picks IS this snapshot — the name seek is exact.
            tt_since = len(container_log("query"))
            historical = client.get_table(
                table_name="t_alpha", as_of_micros=snap_micros, **resolve_kw
            )
            assert historical.table_uuid == ctx["table_uuids"]["t_alpha"]
            assert poll_log_for("query", tt_since, _DIRECT_MARKER), (
                "CHA-501: an as_of time-travel resolve of a fully-cold table must "
                f"now take the direct seek ({_DIRECT_MARKER}); the as_of-bounded "
                "snapshot pick makes it exact"
            )

            # 3) open-tx (`OpenTx`) resolve of a table the tx has NOT written —
            # snapshot-only (the tx added no hot row for __penca_system__.tables),
            # so it takes the seek. The tx-WROTE-the-table RYOW guard is CHA-471.
            tx = client.begin_tx(
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
                author="test",
                comment="cha501 open-tx seek",
            )
            try:
                ot_since = len(container_log("query"))
                ryow = client.get_table(
                    table_name="t_alpha", open_tx_uuid=tx.tx_uuid, **resolve_kw
                )
                assert ryow.table_uuid == ctx["table_uuids"]["t_alpha"]
                assert poll_log_for("query", ot_since, _DIRECT_MARKER), (
                    "CHA-501: an open-tx resolve of a table the tx has not "
                    f"written must now take the direct seek ({_DIRECT_MARKER})"
                )
            finally:
                client.abort_tx(tx.tx_uuid, catalog_uuid=ctx["catalog_uuid"])
        finally:
            client.close()


class TestByNameFallback:
    """Regression guards, green today and after the rewire: a system table
    that is not snapshot-only must fall back (correct results, no marker)."""

    def test_ddl_churn_falls_back_correctly(self):
        """After covering ``__penca_system__.tables``, a ``create_table``
        leaves hot rows (NOT snapshot-only): by-name resolves see the new
        table (hot visibility), stay correct for pre-churn tables, and emit
        NO ``direct_point_read`` marker for either."""
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)
            _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_tables_table_uuid(ctx["catalog_uuid"]),
            )
            t_new_uuid = client.create_table(
                "t_new",
                USER_SCHEMA,
                primary_keys=["name"],
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                author="test",
                comment="ddl churn",
            )

            since = len(container_log("query"))
            resolve_kw = {
                "schema_uuid": ctx["schema_uuid"],
                "catalog_uuid": ctx["catalog_uuid"],
                "branch_uuid": ctx["branch_uuid"],
            }
            churned = client.get_table(table_name="t_new", **resolve_kw)
            assert churned.table_uuid == t_new_uuid, (
                "churn-window by-name resolve must see the hot (unpersisted)"
                " create_table row"
            )
            pre_churn = client.get_table(table_name="t_alpha", **resolve_kw)
            assert pre_churn.table_uuid == ctx["table_uuids"]["t_alpha"]

            # Barrier before the negative count: both resolves' span-CLOSE
            # lines must be flushed, so any marker they emitted is in-window.
            closes = _poll_resolve_closes("resolve_table_metadata", since, 2)
            assert closes >= 2, (
                "expected >= 2 resolve_table_metadata span CLOSE lines after "
                f"two churn-window get_table calls, got {closes} — either "
                "PENCA_SPAN_TIMING is unset or the resolves did not fire; "
                "harness/coverage issue, not a fallback result"
            )
            assert container_log("query")[since:].count(_DIRECT_MARKER) == 0, (
                "a system table with hot DDL rows is not snapshot-only; "
                "churn-window by-name resolves must fall back to the merged "
                f"read and emit no {_DIRECT_MARKER}"
            )
        finally:
            client.close()

    def test_never_covered_falls_back_correctly(self):
        """Negative control: with ``__penca_system__.tables`` never
        snapshot-covered, a by-name resolve is correct and emits no marker."""
        client = make_client()
        try:
            ctx = _new_catalog_schema_tables(client)

            since = len(container_log("query"))
            info = client.get_table(
                table_name="t_beta",
                schema_uuid=ctx["schema_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.table_uuid == ctx["table_uuids"]["t_beta"]

            closes = _poll_resolve_closes("resolve_table_metadata", since, 1)
            assert closes >= 1, (
                "expected a resolve_table_metadata span CLOSE line after a "
                "by-name get_table; harness/coverage issue, not a fallback "
                "result"
            )
            assert container_log("query")[since:].count(_DIRECT_MARKER) == 0, (
                "a never-snapshot-covered system table must not take the "
                f"direct seek; no {_DIRECT_MARKER} may be emitted"
            )
        finally:
            client.close()
