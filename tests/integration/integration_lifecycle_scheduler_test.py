"""Integration tests for the dirty-set enumeration RPCs that drive the
lifecycle scheduler (CHA-154; rehomed onto `LifecycleService` by CHA-445).

The scheduler's autonomous tick loop is `loop { tick(); sleep }` —
testing it end-to-end would mostly be exercising `tokio::time::sleep`.
What's worth pinning is the data plane the scheduler reads from:

- **`ListModifiedTables` (RPC on `LifecycleService`)** —
  paginated, half-open `[min_micros, max_micros)` window on
  `commit_tx_log.commit_micros`. Joins `commit_tx_log` ▷ `tx_table_log` to
  enumerate distinct tables a committed tx touched. Aborted-tx writes
  are structurally excluded (no `commit_tx_log` row). Ordered by
  `MAX(commit_micros) ASC` so the scheduler catches up the stale
  tail before chasing recent writers.

- **`ListPersistedTables` (RPC on `LifecycleService`)** — symmetric to
  `ListModifiedTables` but keyed on
  `table_persist_metadata.commit_micros`. Excludes never-persisted
  tables.

The integration test profile disables the scheduler binary's tick
loop (`SCHEDULER_TICK_INTERVAL_SECONDS=-1`) so the suite asserts RPC
behavior without the loop racing manual lifecycle calls in sibling
test files. Run via ``just integration-test lifecycle_scheduler``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from grpc import insecure_channel
from penca_client import Mutation
from penca_client.config import ClientSettings
from penca_proto.external.v1.common_pb2 import IntegerRange, PaginationRequest
from penca_proto.external.v1.lifecycle_pb2 import (
    ListModifiedTablesRequest,
    ListPersistedTablesRequest,
)
from penca_proto.external.v1.lifecycle_pb2_grpc import (
    LifecycleServiceStub,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    create_table_on_branch,
    get_pg_driver,
    make_client,
    setup_schema,
)

# ── Per-catalog parent table names ────────────────────────────────────

TABLE_PERSIST_METADATA = "table_persist_metadata"
TABLE_PURGE_METADATA = "table_purge_metadata"
TABLE_SNAPSHOT_METADATA = "table_snapshot_metadata"


# ── Helpers ───────────────────────────────────────────────────────────


def _make_lifecycle_stub() -> LifecycleServiceStub:
    """Direct gRPC channel to `LifecycleService`.

    `ListModifiedTables` / `ListPersistedTables` are dirty-set
    enumeration RPCs the scheduler calls on `LifecycleService` (CHA-445
    rehomed them off the deleted `StorageMetadataService`). `PencaClient`
    doesn't expose them, so tests dial the stub directly.
    """
    settings = ClientSettings()  # ty: ignore[missing-argument]
    return LifecycleServiceStub(insecure_channel(settings.lifecycle_url))


def _make_branch(client, catalog_uuid, name):
    branch = client.create_branch(
        f"{name}_{uuid4().hex[:6]}",
        catalog_uuid=catalog_uuid,
        author="test",
        comment="cha-154",
    )
    return branch.branch_uuid


def _create_tables_on_branch(client, catalog_uuid, schema_uuid, branch_uuid, names):
    return {
        name: create_table_on_branch(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            table_name=name,
        )
        for name in names
    }


def _commit_writes(client, catalog_uuid, schema_uuid, branch_uuid, table_uuids, rows):
    """Begin a tx, write `rows` to each `table_uuid`, commit. Returns
    `(tx_uuid, commit_micros)` — tests need the timestamp to pin
    `ListModifiedTables` window bounds.
    """
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    for table_uuid in table_uuids:
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )

    committed = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    return tx.tx_uuid, committed.commit_micros


def _abort_tx_with_writes(
    client, catalog_uuid, schema_uuid, branch_uuid, table_uuids, rows
):
    """Begin a tx, write `rows` to each `table_uuid`, abort. Used to
    pin that `ListModifiedTables` excludes the writes (no `commit_tx_log`
    row → no join match).
    """
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    batch = pa.table(rows, schema=USER_SCHEMA)
    for table_uuid in table_uuids:
        client.write_data(
            tx.tx_uuid,
            Mutation(table_uuid=table_uuid, upserts=batch),
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
        )

    client.abort_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )


def _list_modified_tables(
    catalog_uuid,
    branch_uuid,
    *,
    min_micros=None,
    max_micros=None,
    page_size=None,
    page_token=None,
):
    """Single-page wrapper around the stub. Returns the response so
    callers can inspect both `table_uuids` and `next_page_token`.
    """
    request = ListModifiedTablesRequest(
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    if min_micros is not None or max_micros is not None:
        tf = IntegerRange()
        if min_micros is not None:
            tf.min = min_micros

        if max_micros is not None:
            tf.max = max_micros

        request.modified_at.CopyFrom(tf)

    if page_size is not None or page_token is not None:
        pr = PaginationRequest()
        if page_size is not None:
            pr.page_size = page_size

        if page_token is not None:
            pr.page_token = page_token

        request.pagination.CopyFrom(pr)

    stub = _make_lifecycle_stub()
    return stub.ListModifiedTables(request)


def _list_modified_tables_all(
    catalog_uuid, branch_uuid, *, min_micros=None, max_micros=None, page_size=None
):
    """Drain every page; return the assembled `table_uuids` list with
    cross-page ordering preserved.
    """
    out: list[str] = []
    page_token: str | None = None
    while True:
        resp = _list_modified_tables(
            catalog_uuid,
            branch_uuid,
            min_micros=min_micros,
            max_micros=max_micros,
            page_size=page_size,
            page_token=page_token,
        )
        out.extend(resp.table_uuids)
        if not resp.HasField("next_page_token"):
            return out

        page_token = resp.next_page_token


def _list_persisted_tables(
    catalog_uuid,
    branch_uuid,
    *,
    min_micros=None,
    max_micros=None,
    page_size=None,
    page_token=None,
):
    """Single-page wrapper around `ListPersistedTables`. The half-open
    window filters on `table_persist_metadata.commit_micros`.
    """
    request = ListPersistedTablesRequest(
        catalog_uuid=catalog_uuid,
        branch_uuid=branch_uuid,
    )
    if min_micros is not None or max_micros is not None:
        tf = IntegerRange()
        if min_micros is not None:
            tf.min = min_micros

        if max_micros is not None:
            tf.max = max_micros

        request.persisted_at.CopyFrom(tf)

    if page_size is not None or page_token is not None:
        pr = PaginationRequest()
        if page_size is not None:
            pr.page_size = page_size

        if page_token is not None:
            pr.page_token = page_token

        request.pagination.CopyFrom(pr)

    stub = _make_lifecycle_stub()
    return stub.ListPersistedTables(request)


def _list_persisted_tables_all(
    catalog_uuid, branch_uuid, *, min_micros=None, max_micros=None, page_size=None
):
    """Drain every page; preserve cross-page ordering."""
    out: list[str] = []
    page_token: str | None = None
    while True:
        resp = _list_persisted_tables(
            catalog_uuid,
            branch_uuid,
            min_micros=min_micros,
            max_micros=max_micros,
            page_size=page_size,
            page_token=page_token,
        )
        out.extend(resp.table_uuids)
        if not resp.HasField("next_page_token"):
            return out

        page_token = resp.next_page_token


# ── White-box state probes (per-table watermarks) ────────────────────


def _latest_committed_persisted_at(catalog_uuid, branch_uuid, table_uuid):
    parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(persisted_at_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _latest_committed_snapshot_at(catalog_uuid, branch_uuid, table_uuid):
    parent = f"{catalog_uuid}_{TABLE_SNAPSHOT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(snapshotted_at_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _latest_committed_purged_at(catalog_uuid, branch_uuid, table_uuid):
    """Latest committed purge fence Pu (``last_purged_commit_seq_num``) for T,
    or None. CHA-444 (ADR 0027): the watermark is seq-axis now."""
    parent = f"{catalog_uuid}_{TABLE_PURGE_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(last_purged_commit_seq_num) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _latest_persist_committed_at(catalog_uuid, branch_uuid, table_uuid):
    """Read `table_persist_metadata.commit_micros` for the latest
    committed persist on `(branch, table)`. `ListPersistedTables`
    filters on this column (phase-2 commit time), distinct from
    `persisted_at_micros` (the watermark)."""
    parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT max(commit_micros) FROM {tbl}"
            " WHERE branch_uuid = %s AND table_uuid = %s"
            "   AND commit_micros IS NOT NULL"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )
    return rows[0][0] if rows else None


def _count_persist_rows(catalog_uuid, branch_uuid, table_uuid):
    parent = f"{catalog_uuid}_{TABLE_PERSIST_METADATA}"
    return get_pg_driver().execute(
        SQL(
            "SELECT count(*) FROM {tbl} WHERE branch_uuid = %s AND table_uuid = %s"
        ).format(tbl=Identifier(parent)),
        (branch_uuid, table_uuid),
    )[0][0]


# ── ListModifiedTables ───────────────────────────────────────────────


class TestListModifiedTables:
    def test_includes_aborted_tx_writes(self):
        """Aborted-tx writes DO appear in the response (CHA-221 v2.1
        / ADR 0021).

        v1 (pre-ADR-0021) structurally excluded aborted-tx writes via
        the `commit_tx_log`-only join — only committed txs landed in `commit_tx_log`,
        so the join naturally dropped aborted-tx rows. v2.1 broadens
        the join to `commit_tx_log ∪ abort_tx_log` so the scheduler triggers
        Persist on tables touched by aborted writes too; without this,
        aborted-only tables would never have Persist called and their
        hot rows + tx-log family metadata would leak indefinitely
        (Persist owns aborted hot cleanup per ADR 0021).

        Pins the v2.1 semantic shift: an aborted-tx write to a table
        with no committed writes must appear in `ListModifiedTables`.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "include_aborted")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_aborted", "t_committed"]
        )
        floor = max(
            1,
            _commit_writes(
                client,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                # Touch `t_committed` first to anchor a `min_micros`
                # floor that excludes the system-table writes from
                # branch creation; we only want to assert about the
                # two user tables in this test.
                [tables["t_committed"]],
                {"name": ["seed"], "value": [0]},
            )[1],
        )

        # Aborted tx that writes to t_aborted only. Under v2.1 the
        # union join with abort_tx_log surfaces this in the response.
        _abort_tx_with_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_aborted"]],
            {"name": ["alice"], "value": [1]},
        )
        # Committed tx that writes to t_committed only. This *must*
        # also appear.
        _, c_committed = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_committed"]],
            {"name": ["bob"], "value": [2]},
        )

        result = _list_modified_tables_all(
            catalog_uuid,
            branch_uuid,
            min_micros=floor,
            max_micros=c_committed + 1,
        )
        assert tables["t_committed"] in result, (
            "Committed-tx writes must appear (unchanged from v1)."
        )
        assert tables["t_aborted"] in result, (
            "v2.1 (ADR 0021): ListModifiedTables now includes "
            "aborted-tx writes so the scheduler triggers Persist on "
            "aborted-only tables. The join must union commit_tx_log with "
            "abort_tx_log."
        )

    def test_window_bounds_are_half_open(self):
        """`modified_at = [A, C)` matches `committed_at ∈ {A, B}`,
        excludes `committed_at = C`. Pins the
        `IntegerRange`-documented half-open contract on this RPC's
        `commit_tx_log.commit_micros` join key.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "half_open")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_a", "t_b", "t_c"]
        )

        _, a = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_a"]],
            {"name": ["a"], "value": [1]},
        )
        _, b = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_b"]],
            {"name": ["b"], "value": [2]},
        )
        _, c = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_c"]],
            {"name": ["c"], "value": [3]},
        )
        # Three distinct micros — guaranteed by the database clock's
        # monotonic stamping per CHA-103. (If this ever flakes, the
        # invariant is broken upstream.)
        assert a < b < c

        result_ab = _list_modified_tables_all(
            catalog_uuid, branch_uuid, min_micros=a, max_micros=c
        )
        assert sorted(result_ab) == sorted([tables["t_a"], tables["t_b"]]), (
            f"[{a}, {c}) must include t_a, t_b and exclude t_c; got {result_ab}"
        )

        result_only_a = _list_modified_tables_all(
            catalog_uuid, branch_uuid, min_micros=a, max_micros=a + 1
        )
        assert result_only_a == [tables["t_a"]], (
            f"[{a}, {a + 1}) is the smallest non-empty window covering A only; "
            f"got {result_only_a}"
        )

        # Bounds omitted on the user-window side: anchor at `a` so the
        # branch-creation system-table writes are excluded.
        result_unbounded = _list_modified_tables_all(
            catalog_uuid, branch_uuid, min_micros=a
        )
        assert sorted(result_unbounded) == sorted(
            [tables["t_a"], tables["t_b"], tables["t_c"]]
        ), f"open-ended max_micros must include all three; got {result_unbounded}"

    def test_pagination_roundtrip(self):
        """Reassembled paged result equals the unpaginated call, and
        cross-page ordering is preserved. Implementation must use a
        deterministic ORDER BY (`MAX(committed_at) ASC`,
        `table_uuid ASC` tiebreak) so identical pagination is stable.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "pagination")

        names = [f"tp{i}" for i in range(5)]
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, names
        )
        first_micros = None
        for n in names:
            _, c = _commit_writes(
                client,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                [tables[n]],
                {"name": [n], "value": [1]},
            )
            if first_micros is None:
                first_micros = c

        full = _list_modified_tables_all(
            catalog_uuid, branch_uuid, min_micros=first_micros
        )
        # All 5 must show up.
        assert sorted(full) == sorted(tables.values()), (
            f"unpaginated response missing tables: got {full}, "
            f"expected {sorted(tables.values())}"
        )

        paged = _list_modified_tables_all(
            catalog_uuid, branch_uuid, min_micros=first_micros, page_size=2
        )
        assert paged == full, (
            f"paged ordering diverged from unpaginated: paged={paged}, full={full}"
        )

    def test_orders_by_max_committed_at_asc(self):
        """`tx-1` touches `[t_old, t_recent]` at `X`; `tx-2` re-touches
        `t_recent` at `Y > X`. Response ordering is
        `[t_old, t_recent]` because `MAX(committed_at)` per table is
        `(X, Y)` — least-recently-modified first. The scheduler relies
        on this to drain the stale tail before chasing recent writers.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "ordering")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_old", "t_recent"]
        )

        _, x = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_old"], tables["t_recent"]],
            {"name": ["seed"], "value": [0]},
        )
        _, y = _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_recent"]],
            {"name": ["fresh"], "value": [1]},
        )
        assert x < y

        result = _list_modified_tables_all(catalog_uuid, branch_uuid, min_micros=x)
        # Filter to the user tables under test — branch creation can
        # leave system-table rows in the same window.
        filtered = [t for t in result if t in tables.values()]
        assert filtered == [tables["t_old"], tables["t_recent"]], (
            f"expected [t_old, t_recent] (MAX(committed_at) ASC); got {filtered}"
        )


# ── ListPersistedTables ──────────────────────────────────────────────


class TestListPersistedTables:
    """Drives the scheduler's per-tick Purge enumeration.

    Each test writes + persists user tables explicitly (no scheduler
    involvement). Filtering by `min_micros` floored at the first
    user-table persist's `commit_micros` excludes the system-table
    persists that the running scheduler container may have written in
    parallel.
    """

    def test_excludes_never_persisted_tables(self):
        """A table written-to-but-never-persisted does NOT appear.

        `ListPersistedTables` filters
        `table_persist_metadata.commit_micros IS NOT NULL` —
        structurally there is no row for a table with no persist. The
        committed-phase filter symmetrically excludes phase-1 rows
        whose phase-2 flip never happened (those would have
        `commit_micros = NULL`).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "persist_only")
        tables = _create_tables_on_branch(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            ["t_persisted", "t_written_only"],
        )

        # Persist t_persisted explicitly (writes then Persist).
        _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_persisted"]],
            {"name": ["alice"], "value": [1]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_persisted"],
        )
        floor = _latest_persist_committed_at(
            catalog_uuid, branch_uuid, tables["t_persisted"]
        )
        assert floor is not None, (
            "test setup failed: Persist did not produce a "
            "table_persist_metadata row for t_persisted"
        )

        # Write to t_written_only but never call Persist. The row
        # lives in the hot upsert_log; no `table_persist_metadata`
        # entry exists, so ListPersistedTables must omit it.
        _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_written_only"]],
            {"name": ["bob"], "value": [2]},
        )

        result = _list_persisted_tables_all(catalog_uuid, branch_uuid, min_micros=floor)
        assert tables["t_persisted"] in result
        assert tables["t_written_only"] not in result, (
            "ListPersistedTables returned a never-persisted table — the "
            "commit_micros IS NOT NULL filter is missing or wrong"
        )

    def test_window_bounds_are_half_open(self):
        """`persisted_at = [A, C)` matches `committed_at ∈ {A, B}`,
        excludes `committed_at = C`. Pins the
        `IntegerRange`-documented half-open contract on
        `table_persist_metadata.commit_micros`.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "persist_half_open")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_a", "t_b", "t_c"]
        )

        for name in ["t_a", "t_b", "t_c"]:
            _commit_writes(
                client,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                [tables[name]],
                {"name": [name], "value": [1]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=tables[name],
            )

        a = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables["t_a"])
        b = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables["t_b"])
        c = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables["t_c"])
        assert None not in (a, b, c)
        assert a < b < c, f"non-monotonic persist commits: {a} {b} {c}"

        result_ab = _list_persisted_tables_all(
            catalog_uuid, branch_uuid, min_micros=a, max_micros=c
        )
        # Filter to user tables — running scheduler may have persisted
        # system tables in the same window.
        filtered = [t for t in result_ab if t in tables.values()]
        assert sorted(filtered) == sorted([tables["t_a"], tables["t_b"]]), (
            f"[{a}, {c}) must include t_a, t_b and exclude t_c; got {filtered}"
        )

        result_only_a = _list_persisted_tables_all(
            catalog_uuid, branch_uuid, min_micros=a, max_micros=a + 1
        )
        filtered = [t for t in result_only_a if t in tables.values()]
        assert filtered == [tables["t_a"]], (
            f"[{a}, {a + 1}) is the smallest non-empty window covering A; "
            f"got {filtered}"
        )

    def test_pagination_roundtrip(self):
        """Reassembled paged result equals the unpaginated call, and
        cross-page ordering is preserved. Implementation must use a
        deterministic ORDER BY (`MAX(committed_at) ASC, table_uuid
        ASC`) so paged ordering is stable.
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "persist_pagination")
        names = [f"tp{i}" for i in range(5)]
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, names
        )
        first_micros = None
        for n in names:
            _commit_writes(
                client,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                [tables[n]],
                {"name": [n], "value": [1]},
            )
            client.persist(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
                table_uuid=tables[n],
            )
            c = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables[n])
            if first_micros is None:
                first_micros = c

        full = _list_persisted_tables_all(
            catalog_uuid, branch_uuid, min_micros=first_micros
        )
        filtered_full = [t for t in full if t in tables.values()]
        assert sorted(filtered_full) == sorted(tables.values()), (
            f"unpaginated response missing tables: got {filtered_full}, "
            f"expected {sorted(tables.values())}"
        )

        paged = _list_persisted_tables_all(
            catalog_uuid, branch_uuid, min_micros=first_micros, page_size=2
        )
        filtered_paged = [t for t in paged if t in tables.values()]
        assert filtered_paged == filtered_full, (
            f"paged ordering diverged from unpaginated: "
            f"paged={filtered_paged}, full={filtered_full}"
        )

    def test_orders_by_max_committed_at_asc(self):
        """Persisting `t_old` first then `t_recent` orders the response
        as `[t_old, t_recent]` — least-recently-persisted first. The
        scheduler relies on this to drain the stale tail before
        chasing tables that were just persisted (and therefore still
        sitting inside the grace window upstream).
        """
        client = make_client()
        schema_uuid, _t_main, catalog_uuid, _main_branch_uuid = setup_schema(client)
        branch_uuid = _make_branch(client, catalog_uuid, "persist_ordering")
        tables = _create_tables_on_branch(
            client, catalog_uuid, schema_uuid, branch_uuid, ["t_old", "t_recent"]
        )

        _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_old"]],
            {"name": ["seed"], "value": [0]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_old"],
        )
        x = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables["t_old"])

        _commit_writes(
            client,
            catalog_uuid,
            schema_uuid,
            branch_uuid,
            [tables["t_recent"]],
            {"name": ["fresh"], "value": [1]},
        )
        client.persist(
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            table_uuid=tables["t_recent"],
        )
        y = _latest_persist_committed_at(catalog_uuid, branch_uuid, tables["t_recent"])
        assert x is not None and y is not None and x < y

        result = _list_persisted_tables_all(catalog_uuid, branch_uuid, min_micros=x)
        filtered = [t for t in result if t in tables.values()]
        assert filtered == [tables["t_old"], tables["t_recent"]], (
            f"expected [t_old, t_recent] (MAX(committed_at) ASC); got {filtered}"
        )
