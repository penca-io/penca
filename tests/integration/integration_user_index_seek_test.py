"""CHA-485 red-tests: user-query index seek (planner covering-index selection
+ multi-entry seek intersection).

A SQL lookup whose predicate fully binds a user ``CREATE INDEX``'s key
columns must seek the per-segment cold index sidecars (CHA-483 build, CHA-480
kernel) instead of full-scanning the snapshot baseline, riding the internal
``seeks`` param (CHA-482) through the merge path. The seek is *selection, not
filtering* (ADR 0023): DataFusion's residual still applies the exact
predicate, and only the identity entry may restrict the exclusion set — so
hot-tier shadowing and merged correctness are untouched.

Marker contract (emitted by the ``index_select`` pass call-site in
``read_data`` when it selects at least one covering index):
``index_seek=true`` plus ``index_seek_entries=<n>`` as ``tracing`` fmt fields
on the query service — the same subscriber shape as ``direct_point_read=true``
(unquoted bool / bare int), scraped from container stdout by byte-offset
window.

Fail-first: no ``index_seek`` event exists today — an indexed-column SQL
lookup is served by the full-scan + residual-filter path, so every positive
marker poll below fails while every row-correctness assertion already passes.
If a *rows* assertion fails red, the test itself is wrong.

Scoped run::

    just integration-test user_index_seek
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation

from .integration_helpers import container_log, make_client, poll_log_for

# Proto ``IndexType`` value (common.proto): INDEX_TYPE_SCALAR_BTREE = 1.
INDEX_TYPE_SCALAR_BTREE = 1

# Emitted once per read that selected covering user indexes; the entry count
# rides the same event as a bare-int field.
_SEEK_MARKER = "index_seek=true"

# Wider than USER_SCHEMA: two Utf8 non-PK columns (single + composite + AND
# cases) and an Int64 column (the typed-seek case).
_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("city", pa.utf8()),
        pa.field("tier", pa.utf8()),
        pa.field("score", pa.int64()),
    ]
)

_ROWS = {
    "name": ["alice", "bob", "carol", "dave"],
    "city": ["paris", "paris", "london", "oslo"],
    "tier": ["gold", "silver", "gold", "bronze"],
    # 2/9/10/100 pin typed ordering: lexicographic strings sort
    # ["10", "100", "2", "9"], so a string-compare seek over the
    # natively-sorted Int64 sidecar under-selects. See CHA-480 kernel note.
    "score": [2, 9, 10, 100],
}


def _setup_named(client):
    """Create catalog/schema/table on _SCHEMA, write _ROWS, commit. Returns a
    ctx dict with names (for SQL) and uuids (for lifecycle calls); pins the
    client's Flight SQL connection to the fresh catalog (CHA-169)."""
    catalog_name = f"useek_{uuid4().hex[:8]}"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="useek"
    )
    table_uuid = client.create_table(
        "t",
        _SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="useek",
    )
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=pa.table(_ROWS, schema=_SCHEMA)),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_branch_uuid
    )
    client.catalog = catalog_name

    return {
        "fqn": f"{catalog_name}.s.t",
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "table_uuid": table_uuid,
        "main_branch_uuid": main_branch_uuid,
    }


def _create_index(client, ctx, index_name: str, columns: list[str]) -> str:
    return client.create_index(
        table_uuid=ctx["table_uuid"],
        index_name=index_name,
        columns=columns,
        index_type=INDEX_TYPE_SCALAR_BTREE,
        catalog_uuid=ctx["catalog_uuid"],
        schema_uuid=ctx["schema_uuid"],
        branch_uuid=ctx["main_branch_uuid"],
        author="test",
        comment="useek",
    )


def _snapshot_cycle(client, ctx) -> None:
    """persist -> snapshot -> purge, so the baseline (and the index sidecars
    declared for it, CHA-483 materialize-on-next-snapshot) covers the writes.
    CHA-444: snapshot precedes purge so Pu <= W_snap."""
    kw = {
        "catalog_uuid": ctx["catalog_uuid"],
        "schema_uuid": ctx["schema_uuid"],
        "branch_uuid": ctx["main_branch_uuid"],
        "table_uuid": ctx["table_uuid"],
    }
    client.persist(**kw)
    client.snapshot(**kw)
    client.purge(**kw)


def _names(result) -> list[str]:
    return sorted(result.column("name").to_pylist())


class TestCoveringSeek:
    """RT1 — a fully-bound covering index is seeked; partial binding, an
    unindexed column, and a not-yet-snapshotted index are not."""

    def test_single_column_index_point_lookup_seeks(self):
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_city", ["city"])

        # One window covers the whole test; negatives are asserted by an
        # exact marker COUNT after a polled flush barrier (the repo pattern
        # from integration_direct_point_read_test.py — unpolled absence
        # windows race the container's json-log flush lag).
        window = len(container_log("query"))

        # Visible-index lag (ADR 0026 §5): before any post-CREATE snapshot
        # there is no baseline and no sidecar — correct rows, no seek.
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"
        )
        assert _names(result) == ["alice", "bob"]

        _snapshot_cycle(client, ctx)

        # Post-snapshot the declared index is fully materialized (ADR 0026
        # §8 — no partial coverage): the lookup must seek it.
        since = len(container_log("query"))
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"
        )
        assert _names(result) == ["alice", "bob"]
        assert poll_log_for("query", since, _SEEK_MARKER), (
            "a point lookup on an indexed user column must select the "
            f"covering index ({_SEEK_MARKER}); not emitted today — the read "
            "is served by full-scan + residual filter"
        )
        assert poll_log_for("query", since, "index_seek_entries=1")

        # Control: unindexed column -> no covering index, no seek.
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE tier = 'gold'"
        )
        assert _names(result) == ["alice", "carol"]

        # Flush barrier + exact count: once the barrier's marker has flushed
        # the log is append-only and every read above is fully present.
        # Exactly the positive read and the barrier may emit the marker; the
        # lag read or the unindexed control seeking would push the count
        # past 2.
        barrier_since = len(container_log("query"))
        client.execute_query(f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'")
        assert poll_log_for("query", barrier_since, _SEEK_MARKER), (
            "flush-barrier qualifying read must seek the covering index"
        )
        assert container_log("query")[window:].count(_SEEK_MARKER) == 2, (
            "exactly the positive read and the flush barrier may emit "
            f"{_SEEK_MARKER}; a visible-index-lag or unindexed-column read "
            "seeking would push the count past 2"
        )

    def test_composite_index_full_binding_seeks(self):
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_city_tier", ["city", "tier"])
        _snapshot_cycle(client, ctx)

        window = len(container_log("query"))

        # Prefix-only binding does NOT cover the (city, tier) key -> no seek
        # (v1 has no prefix-scan), rows still correct via full-scan+filter.
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"
        )
        assert _names(result) == ["alice", "bob"]

        # Every key column equality-bound -> one composite tuple probe.
        since = len(container_log("query"))
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris' AND tier = 'silver'"
        )
        assert _names(result) == ["bob"]
        assert poll_log_for("query", since, _SEEK_MARKER), (
            "a fully-bound composite predicate must seek the composite "
            f"index ({_SEEK_MARKER}); not emitted today"
        )

        # Flush barrier + exact count (see test above): only the full-bind
        # read and the barrier may seek; a prefix-only read seeking would
        # push the count past 2.
        barrier_since = len(container_log("query"))
        client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris' AND tier = 'silver'"
        )
        assert poll_log_for("query", barrier_since, _SEEK_MARKER), (
            "flush-barrier qualifying read must seek the composite index"
        )
        assert container_log("query")[window:].count(_SEEK_MARKER) == 2, (
            f"exactly the full-bind read and the flush barrier may emit "
            f"{_SEEK_MARKER}; a prefix-only read seeking would push the "
            "count past 2"
        )


class TestIntersectionTypedAndMerge:
    """RT2 — AND across two single-column indexes intersects; Int64 keys seek
    with typed comparison; hot-tier shadowing stays merge-correct."""

    def test_multi_index_and_intersects(self):
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_city", ["city"])
        _create_index(client, ctx, "idx_tier", ["tier"])
        _snapshot_cycle(client, ctx)

        # city='paris' matches {alice,bob}; tier='gold' matches
        # {alice,carol}; the AND is their strict intersection.
        since = len(container_log("query"))
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris' AND tier = 'gold'"
        )
        assert _names(result) == ["alice"]
        assert poll_log_for("query", since, "index_seek_entries=2"), (
            "an AND across two single-column indexes must emit one seek "
            "entry per index (index_seek_entries=2) and intersect their "
            "offsets; no index_seek event is emitted today"
        )

    def test_typed_int_index_seeks_correctly(self):
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_score", ["score"])
        _snapshot_cycle(client, ctx)

        # Scores {2, 9, 10, 100}: string-ordered they sort
        # ["10","100","2","9"], so a lexicographic seek over the
        # natively-sorted Int64 sidecar misses 10 entirely (silent row
        # loss). The typed seek must return exactly carol.
        since = len(container_log("query"))
        result = client.execute_query(f"SELECT name FROM {ctx['fqn']} WHERE score = 10")
        assert _names(result) == ["carol"]
        assert poll_log_for("query", since, _SEEK_MARKER), (
            "an equality lookup on an indexed Int64 column must seek with "
            f"typed key comparison ({_SEEK_MARKER}); not emitted today"
        )

    def test_hot_shadow_row_merges_correctly_with_seek(self):
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_city", ["city"])
        _snapshot_cycle(client, ctx)

        # Post-snapshot hot upsert moves alice paris -> berlin: the baseline
        # row is shadowed. The index seek is selection on the snapshot
        # baseline ONLY (never the exclusion set), so merged results must be
        # identical to the unindexed path.
        tx = client.begin_tx(
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=ctx["table_uuid"],
                upserts=pa.table(
                    {
                        "name": ["alice"],
                        "city": ["berlin"],
                        "tier": ["gold"],
                        "score": [2],
                    },
                    schema=_SCHEMA,
                ),
            ),
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )
        client.commit_tx(
            tx.tx_uuid,
            catalog_uuid=ctx["catalog_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
        )

        # Old value: the sidecar still holds alice under 'paris', but the
        # merge shadows the baseline row -> bob only. Marker still present
        # (the seek fired; the residual + exclusion set did the shadowing).
        since = len(container_log("query"))
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"
        )
        assert _names(result) == ["bob"]
        assert poll_log_for("query", since, _SEEK_MARKER), (
            "the stale-key lookup must still seek the baseline index "
            f"({_SEEK_MARKER}) and rely on the merge to shadow the old row; "
            "no index_seek event is emitted today"
        )

        # New value lives only in the hot tier: the seek finds nothing in
        # the baseline and the hot arm serves the row.
        since = len(container_log("query"))
        result = client.execute_query(
            f"SELECT name FROM {ctx['fqn']} WHERE city = 'berlin'"
        )
        assert _names(result) == ["alice"]
        assert poll_log_for("query", since, _SEEK_MARKER), (
            "the fresh-key lookup carries a covering index on the snapshot "
            f"leg, so the pass must still select it ({_SEEK_MARKER}); no "
            "index_seek event is emitted today"
        )

    def test_ids_restricted_read_with_covering_filter_seeks(self):
        """gRPC ``read_data`` with BOTH an ids restriction and a covering
        filter — the combined identity+user seek shape the SQL path can't
        express (ids ride only the gRPC surface). The ids set straddles the
        filter (dave is oslo), so the result pins that BOTH the ids
        restriction and the residual filter applied on the seeked path."""
        client = make_client()
        ctx = _setup_named(client)
        _create_index(client, ctx, "idx_city", ["city"])
        _snapshot_cycle(client, ctx)

        ids = pa.table(
            {"name": ["alice", "dave"]},
            schema=pa.schema([pa.field("name", pa.utf8())]),
        )
        since = len(container_log("query"))
        result = client.read_data(
            ids=ids,
            filter="city = 'paris'",
            catalog_uuid=ctx["catalog_uuid"],
            schema_uuid=ctx["schema_uuid"],
            branch_uuid=ctx["main_branch_uuid"],
            table_uuid=ctx["table_uuid"],
        )
        assert _names(result) == ["alice"], (
            "ids {alice, dave} ∩ city='paris' is exactly alice — a dropped "
            "residual would leak dave"
        )
        assert poll_log_for("query", since, "index_seek_entries=1"), (
            "an ids-restricted filtered read must still select the covering "
            "user index alongside the identity entry"
        )
