"""CHA-441 red-tests — snapshot-list cache + hot existence-gate.

The snapshot segment *list* (``table_snapshot_segment_metadata``, not the
decoded bytes — that is CHA-252) is immutable between snapshot commits, so a
process cache keyed ``(catalog, branch, table)`` lets a warm current-time read
skip the per-read PG round-trip. This module pins:

- ``test_snapshot_list_cache_hit_cuts_pg_read`` — a 2nd current-time read issues
  ZERO snapshot-segment-metadata reads (served from cache). Fail-first: today
  ``plan()`` reads the list on every read, so the 2nd read issues 1+, not 0.

Counting is via ``pg_stat_statements`` (the CHA-367 resolution-count seam):
``count_stmts_referencing`` sums ``calls`` over normalized statements whose text
contains the per-catalog ``…_table_snapshot_segment_metadata`` identifier, so
background activity on other catalogs can't pollute the count.

Run: ``just integration-test --test-arg integration_snapshot_list_cache_test``.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from penca_client import Mutation
from penca_client._time import micros_to_datetime
from penca_client.naming import TABLE_SNAPSHOT_SEGMENT_METADATA

from .integration_helpers import (
    USER_SCHEMA,
    container_log,
    count_stmts_referencing,
    ensure_pg_stat_statements,
    get_pg_driver,
    make_client,
    poll_log_for,
    reset_pg_stat,
    setup_with_data,
)
from .integration_purge_tx_log_test import (
    _persist_purge_system_tables_past_grace,
)


def _force_cold_snapshot(client, ctx) -> None:
    """Persist the hot tier then snapshot, so a committed cold snapshot exists
    and a current-time read must resolve a snapshot segment list."""
    kw = {
        "catalog_uuid": ctx["catalog_uuid"],
        "schema_uuid": ctx["schema_uuid"],
        "branch_uuid": ctx["main_branch_uuid"],
        "table_uuid": ctx["table_uuid"],
    }
    client.persist(**kw)
    client.snapshot(**kw)


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
class TestSnapshotListCache:
    def test_snapshot_list_cache_hit_cuts_pg_read(self):
        """A warm current-time read serves the snapshot segment list from the
        process cache, issuing ZERO ``table_snapshot_segment_metadata`` reads.

        Fail-first: ``MetadataClient::plan`` reads the list on every read, so the
        2nd read still issues 1+ snapshot-metadata statements. Green after
        CHA-441 caches the list keyed ``(catalog, branch, table)``.
        """
        client = make_client()
        ctx = setup_with_data(client)
        _force_cold_snapshot(client, ctx)

        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        # Per-catalog needle: pg_stat_statements preserves identifiers, so this
        # matches only this catalog's snapshot-segment-metadata reads.
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        # First current-time read — cache miss, populates the entry.
        reset_pg_stat(pg)
        r1 = client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        first = count_stmts_referencing(pg, seg_table)

        # Second current-time read — expect a cache hit: no snapshot-list PG read.
        reset_pg_stat(pg)
        r2 = client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        second = count_stmts_referencing(pg, seg_table)

        assert first > 0, (
            "sanity: the first read must issue the snapshot-segment-metadata "
            "read (the thing the cache elides)"
        )
        # Correctness rides alongside the perf assertion: both reads see the
        # snapshot baseline rows regardless of cache state.
        assert r1.num_rows == 2 and r2.num_rows == 2, (
            f"both current-time reads return the 2 snapshot rows "
            f"(got {r1.num_rows}, {r2.num_rows})"
        )
        assert second == 0, (
            f"2nd current-time read issued {second} "
            f"table_snapshot_segment_metadata read(s); CHA-441 caches the "
            f"snapshot list so a warm read hits the cache (expected 0)"
        )

    def test_time_travel_read_shares_wsnap_cache_entry(self):
        """CHA-492: the snapshot-list cache is keyed on W_snap, so an
        explicit-as_of (time-travel) read that resolves the SAME snapshot as a
        current-time read HITS the shared cache entry (no fresh snapshot-metadata
        read) — and still returns the historical row set. Content-addressing on
        the immutable snapshot version replaces the old current-time-only bypass
        gate: it is always safe to serve, because a given W_snap is one snapshot;
        historical correctness comes from the as_of visibility filter on the hot
        delta, not from bypassing the cache. (Supersedes the old
        `test_time_travel_read_bypasses_cache`.)
        """
        client = make_client()
        ctx = setup_with_data(client)  # alice, bob committed in one tx
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]
        t_alice_bob = ctx["tx"].commit_micros
        _force_cold_snapshot(client, ctx)  # snapshot the {alice, bob} baseline

        # carol commits strictly after the snapshot — a current-time-only row.
        tx = client.begin_tx(catalog_uuid=cat, schema_uuid=sch, branch_uuid=br)
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=tbl,
                upserts=pa.table({"name": ["carol"], "value": [3]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=cat,
            schema_uuid=sch,
            branch_uuid=br,
        )
        client.commit_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        # Warm the cache, then confirm a current-time read is a hit AND sees carol.
        client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        reset_pg_stat(pg)
        current = client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        assert count_stmts_referencing(pg, seg_table) == 0, (
            "warm current-time read must hit the cache (0 snapshot-metadata reads)"
        )
        assert set(current.column("name").to_pylist()) == {"alice", "bob", "carol"}

        # Time-travel read at as_of before carol. CHA-492: keyed on W_snap, this
        # resolves the SAME baseline snapshot as the current-time read above
        # (carol is a hot row, never snapshotted — there is one baseline), so it
        # HITS the shared cache entry (0 fresh snapshot-metadata reads).
        reset_pg_stat(pg)
        historical = client.read_data(
            catalog_uuid=cat,
            schema_uuid=sch,
            table_uuid=tbl,
            branch_uuid=br,
            as_of=micros_to_datetime(t_alice_bob),
        )
        assert count_stmts_referencing(pg, seg_table) == 0, (
            "W_snap-keyed cache: a time-travel read resolving the same snapshot "
            "hits the shared entry (0 snapshot-metadata reads) — content-"
            "addressing makes serving it safe, no bypass needed"
        )
        assert set(historical.column("name").to_pylist()) == {"alice", "bob"}, (
            "time-travel read returns the historical set (pre-carol): the shared "
            "snapshot baseline minus the as_of-excluded hot row"
        )

    def test_distinct_snapshots_get_distinct_wsnap_entries(self):
        """CHA-492: TWO committed snapshots get DISTINCT
        W_snap cache entries, and a time-travel read resolving the OLDER snapshot
        fetches ITS OWN snapshot by identity (the fused pick's
        table_snapshot_uuid) — never the warmed latest. This pins the content-
        addressing that makes the cache correct by construction: KEY and VALUE
        come from one pick, so an older snapshot's entry can't be served the
        newer's segments (the qpna divergence the uuid threading eliminates).
        """
        client = make_client()
        ctx = setup_with_data(client)  # alice, bob committed in one tx
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]
        _force_cold_snapshot(client, ctx)  # S1 = {alice, bob}

        # carol commits, then a SECOND snapshot → S2 = {alice, bob, carol}, with
        # a strictly later snapshot watermark than S1.
        tx = client.begin_tx(catalog_uuid=cat, schema_uuid=sch, branch_uuid=br)
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=tbl,
                upserts=pa.table({"name": ["carol"], "value": [3]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=cat,
            schema_uuid=sch,
            branch_uuid=br,
        )
        committed = client.commit_tx(tx.tx_uuid, catalog_uuid=cat, branch_uuid=br)
        t_carol = committed.commit_micros
        _force_cold_snapshot(client, ctx)  # S2

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        # Warm the cache on the LATEST snapshot (S2 → its own W_snap key).
        current = client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        assert set(current.column("name").to_pylist()) == {"alice", "bob", "carol"}
        reset_pg_stat(pg)
        client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        assert count_stmts_referencing(pg, seg_table) == 0, (
            "warm current-time read hits S2's entry (0 snapshot-metadata reads)"
        )

        # A read at S1's era resolves the OLDER snapshot → a DISTINCT W_snap key
        # → a cache MISS (it is NOT served S2's warmed entry), fetched by S1's own
        # table_snapshot_uuid, returning S1's historical set.
        reset_pg_stat(pg)
        historical = client.read_data(
            catalog_uuid=cat,
            schema_uuid=sch,
            table_uuid=tbl,
            branch_uuid=br,
            as_of=micros_to_datetime(t_carol - 1),
        )
        assert count_stmts_referencing(pg, seg_table) > 0, (
            "the older-snapshot read must MISS (its W_snap key is distinct from "
            "the warmed latest) and fetch its own snapshot by uuid — not be served "
            "the cached newer entry"
        )
        assert set(historical.column("name").to_pylist()) == {"alice", "bob"}, (
            "the older-snapshot read returns its historical set, not the latest"
        )

        # Its own entry now caches — a repeat is a hit (0 reads), confirming the
        # two snapshots hold SEPARATE, content-addressed entries.
        reset_pg_stat(pg)
        client.read_data(
            catalog_uuid=cat,
            schema_uuid=sch,
            table_uuid=tbl,
            branch_uuid=br,
            as_of=micros_to_datetime(t_carol - 1),
        )
        assert count_stmts_referencing(pg, seg_table) == 0, (
            "the older snapshot's own entry is now cached (distinct from latest)"
        )


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
class TestHotExistenceGate:
    """The hot existence-gate (phase-1 predicate-parity probe) lets a read with
    no emittable hot rows take the snapshot-only fast path — observable as a
    ``tier_shape="snapshot_only"`` debug event on the query container (the
    CHA-441 / task-34mf observability seam). Today ``plan()`` always emits a hot
    plan and the staged all-cold arm never engages, so the event never appears.
    """

    def _cold_kw(self, client):
        ctx = setup_with_data(client)
        return {
            "catalog_uuid": ctx["catalog_uuid"],
            "schema_uuid": ctx["schema_uuid"],
            "branch_uuid": ctx["main_branch_uuid"],
            "table_uuid": ctx["table_uuid"],
        }

    def test_fully_cold_read_takes_snapshot_only_fast_path(self):
        """Persist→snapshot→purge (baseline cold, Pu == W_snap, hot drained); a
        current-time read engages the snapshot-only fast path.

        Fail-first: no ``tier_shape="snapshot_only"`` event exists today.
        """
        client = make_client()
        kw = self._cold_kw(client)
        # CHA-444: snapshot must precede purge so the baseline forms and Pu
        # advances only to W_snap (⇒ Pu ≤ W_snap ⇒ snapshot-only eligible).
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)

        since = len(container_log("query"))
        result = client.read_data(
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            table_uuid=kw["table_uuid"],
            branch_uuid=kw["branch_uuid"],
        )

        assert result.num_rows == 2, (
            f"fully-cold read returns the 2 snapshot baseline rows "
            f"(got {result.num_rows})"
        )
        assert poll_log_for("query", since, 'tier_shape="snapshot_only"'), (
            'expected a tier_shape="snapshot_only" dispatch event on the query '
            "container after a fully-cold read — the phase-1 hot existence-gate "
            "should engage the snapshot-only fast path (CHA-441)"
        )

    def test_hot_rows_read_takes_merged_path(self):
        """Negative control: a hot row past the fence ⇒ merged path, NOT
        snapshot-only."""
        client = make_client()
        kw = self._cold_kw(client)
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)
        # Hot delta strictly after the cold cut → hot_present=true → merged.
        tx = client.begin_tx(
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            branch_uuid=kw["branch_uuid"],
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=kw["table_uuid"],
                upserts=pa.table({"name": ["carol"], "value": [3]}, schema=USER_SCHEMA),
            ),
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            branch_uuid=kw["branch_uuid"],
        )
        client.commit_tx(
            tx.tx_uuid, catalog_uuid=kw["catalog_uuid"], branch_uuid=kw["branch_uuid"]
        )

        since = len(container_log("query"))
        result = client.read_data(
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            table_uuid=kw["table_uuid"],
            branch_uuid=kw["branch_uuid"],
        )

        assert result.num_rows == 3, (
            f"merged read returns 3 rows (got {result.num_rows})"
        )
        assert poll_log_for("query", since, 'tier_shape="merged"'), (
            'expected a tier_shape="merged" dispatch event with a live hot row '
            "(the negative control for the snapshot-only fast path)"
        )
        # Fence the failure mode this control exists for: a live hot row must
        # NOT be misclassified as cold-only. The merged event is already
        # present (above), so the window is settled — check absence directly,
        # no poll (and no wasted sleep).
        assert 'tier_shape="snapshot_only"' not in container_log("query")[since:], (
            "a read with a live hot row past the fence must NOT also emit "
            'tier_shape="snapshot_only" — the gate wrongly took the fast path'
        )


class TestDecompositionParity:
    """Behavior-preservation guard for the read-path decomposition (dissolve
    plan() → stream_change_log + stream_snapshot). A pure refactor cannot
    fail-first on parity, so this is the green safety net the refactor must keep
    green; the broad parity surface lives in the existing read_mvcc / point_read
    / audit suites, and the as_of-cache-bypass guard rides impl-task 41kc (it is
    meaningful and non-flaky only once the cache exists). This pins the single
    most load-bearing invariant: latest-wins across the snapshot baseline and
    the hot delta.
    """

    def test_latest_wins_across_snapshot_baseline_and_hot_delta(self):
        """A row present in the cold snapshot baseline AND updated in the hot
        delta resolves to the hot (latest) version; an unshadowed baseline row
        survives. This is the snapshot-minus-exclusion + hot merge the
        decomposition reorganizes — it must stay byte-correct."""
        client = make_client()
        ctx = setup_with_data(client)  # alice=10, bob=20 committed on main
        kw = {
            "catalog_uuid": ctx["catalog_uuid"],
            "schema_uuid": ctx["schema_uuid"],
            "branch_uuid": ctx["main_branch_uuid"],
            "table_uuid": ctx["table_uuid"],
        }
        # Cold baseline {alice:10, bob:20}; purge clears them from hot.
        client.persist(**kw)
        client.snapshot(**kw)
        client.purge(**kw)

        # Hot delta: update alice strictly after the cold cut (shadows the
        # baseline row); bob stays baseline-only.
        tx = client.begin_tx(
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            branch_uuid=kw["branch_uuid"],
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=kw["table_uuid"],
                upserts=pa.table(
                    {"name": ["alice"], "value": [99]}, schema=USER_SCHEMA
                ),
            ),
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            branch_uuid=kw["branch_uuid"],
        )
        client.commit_tx(
            tx.tx_uuid, catalog_uuid=kw["catalog_uuid"], branch_uuid=kw["branch_uuid"]
        )

        result = client.read_data(
            catalog_uuid=kw["catalog_uuid"],
            schema_uuid=kw["schema_uuid"],
            table_uuid=kw["table_uuid"],
            branch_uuid=kw["branch_uuid"],
        )
        by_name = dict(
            zip(
                result.column("name").to_pylist(),
                result.column("value").to_pylist(),
                strict=True,
            )
        )
        assert by_name == {"alice": 99, "bob": 20}, (
            f"latest-wins across tiers broke: expected alice=99 (hot shadows "
            f"baseline), bob=20 (baseline survives), got {by_name}"
        )


# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
@pytest.mark.serial
class TestSystemTableResolveCache:
    """CHA-472 red-tests — extend the CHA-441 snapshot-list cache to the
    ``__penca_system__.tables`` IDENTIFIER resolve (hit on every read/write)
    and to the write path.

    The CHA-441 test above only snapshots the *user* table, so the system-table
    identifier resolve reads ``__penca_system__.tables`` from HOT (``hot_min ==
    0`` ⇒ no snapshot-list read). These tests drive ``__penca_system__.tables``
    itself COLD via ``_persist_purge_system_tables_past_grace`` so the resolve
    must consult a snapshot segment list — the read the W_snap-keyed
    snapshot-list cache (CHA-472/492) serves from cache on both the query and
    write paths. Same per-catalog ``…_table_snapshot_segment_metadata`` needle.
    """

    def test_system_table_resolve_cache_hit_cuts_pg_read(self):
        """QUERY path: a 2nd current-time read issues ZERO
        ``table_snapshot_segment_metadata`` reads once the system-table resolve
        consults the cache.

        Fail-first: the system-table resolves pass ``cache = None`` today (ADR
        0028 "the three system-table Nones"), so the 2nd read re-reads the cold
        ``__penca_system__.tables`` snapshot list — 1+, not 0. The user table's
        own data stays HOT here, so the only snapshot-list read measured is the
        system-table identifier resolve.
        """
        client = make_client()
        ctx = setup_with_data(client)  # alice, bob in the user table (hot)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]
        # Drive __penca_system__.{tables,schemas} cold so resolving the user
        # table by uuid must consult the system table's snapshot segment list.
        _persist_purge_system_tables_past_grace(client, cat, sch, br)

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        # 1st read — warms the system-table snapshot-list cache entry.
        reset_pg_stat(pg)
        client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        first = count_stmts_referencing(pg, seg_table)

        # 2nd read — expect a system-table cache hit: no snapshot-list PG read.
        reset_pg_stat(pg)
        r2 = client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )
        second = count_stmts_referencing(pg, seg_table)

        assert first > 0, (
            "sanity: the first read must issue the system-table "
            "snapshot-segment-metadata read (the thing the cache will elide)"
        )
        assert r2.num_rows == 2, (
            f"the read still returns the 2 user rows (got {r2.num_rows})"
        )
        assert second == 0, (
            f"2nd current-time read issued {second} table_snapshot_segment_metadata "
            f"read(s); the system-table identifier resolve still bypasses the cache "
            f"(cache=None). CHA-472/492 routes it through the W_snap-keyed cache (expected 0)"
        )

    def test_write_path_resolve_cache_hit_cuts_pg_read(self):
        """WRITE path: a 2nd AUTOCOMMIT point write resolves its target table
        with ZERO ``table_snapshot_segment_metadata`` reads.

        Fail-first (two reasons today): ``penca_write.rs`` builds
        ``SegmentCache::disabled()`` with no list cache, AND the write
        resolves the target identifier under the autocommit tx's ``OpenTx``
        snapshot (which the gate bypasses). CHA-472 IMPL-3 enables the caches
        AND resolves the autocommit identifier under a cache-eligible
        current-time snapshot, so the 2nd write hits the cache.
        """
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]
        _persist_purge_system_tables_past_grace(client, cat, sch, br)

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        def _autocommit_write(name, value):
            client.write_data(
                None,  # tx_uuid=None ⇒ autocommit (NOT an open tx → not RYOW)
                Mutation(
                    table_uuid=tbl,
                    upserts=pa.table(
                        {"name": [name], "value": [value]}, schema=USER_SCHEMA
                    ),
                ),
                catalog_uuid=cat,
                schema_uuid=sch,
                branch_uuid=br,
                author="cha472",
                comment="rt2 write-path cache",
            )

        # 1st autocommit write — warms the system-table snapshot-list cache.
        reset_pg_stat(pg)
        _autocommit_write("dave", 4)
        first = count_stmts_referencing(pg, seg_table)

        # 2nd autocommit write — expect a cache hit on the target-table resolve.
        reset_pg_stat(pg)
        _autocommit_write("erin", 5)
        second = count_stmts_referencing(pg, seg_table)

        assert first > 0, (
            "sanity: the first autocommit write must issue the system-table "
            "snapshot-segment-metadata read its target-table resolve consults"
        )
        assert second == 0, (
            f"2nd autocommit write issued {second} table_snapshot_segment_metadata "
            f"read(s); the write path still pays a PG round-trip per write "
            f"(disabled cache + OpenTx resolution). CHA-472 enables the caches and "
            f"resolves the autocommit identifier under a current-time pin (expected 0)"
        )

    def test_system_table_resolve_time_travel_shares_wsnap_cache(self):
        """CHA-492: an ``as_of`` read's system-table identifier resolve is keyed
        on W_snap like every other read, so resolving the same snapshot as a
        current-time read HITS the shared cache entry (no fresh snapshot-metadata
        read) and still returns the historical set. Supersedes the old
        ``list_cache_for``-gated bypass (that gate is removed).

        Complements — does NOT duplicate — the parallel CHA-471 Flight-SQL
        open-tx DDL RYOW guard (its own ticket, disjoint files).
        """
        client = make_client()
        ctx = setup_with_data(client)
        cat = ctx["catalog_uuid"]
        sch = ctx["schema_uuid"]
        tbl = ctx["table_uuid"]
        br = ctx["main_branch_uuid"]
        t_alice_bob = ctx["tx"].commit_micros
        _persist_purge_system_tables_past_grace(client, cat, sch, br)

        pg = get_pg_driver()
        ensure_pg_stat_statements(pg)
        seg_table = f"{cat}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"

        # Warm the system-table cache via a current-time read.
        client.read_data(
            catalog_uuid=cat, schema_uuid=sch, table_uuid=tbl, branch_uuid=br
        )

        # An as_of read resolving the same snapshot HITS the shared W_snap entry.
        reset_pg_stat(pg)
        historical = client.read_data(
            catalog_uuid=cat,
            schema_uuid=sch,
            table_uuid=tbl,
            branch_uuid=br,
            as_of=micros_to_datetime(t_alice_bob),
        )
        assert count_stmts_referencing(pg, seg_table) == 0, (
            "W_snap-keyed cache: an as_of system-table resolve landing on the "
            "same snapshot hits the shared entry (0 snapshot-metadata reads); "
            "content-addressing replaces the old time-travel bypass"
        )
        assert historical.num_rows == 2, (
            f"the as_of read still returns the {{alice, bob}} baseline "
            f"(got {historical.num_rows})"
        )
