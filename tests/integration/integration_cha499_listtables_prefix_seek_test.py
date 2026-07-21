"""CHA-499 — ListTables serves a MATERIALIZED indexes read via a prefix seek.

Follow-up to CHA-496 (PR #299), which collapsed ``meta_list_tables``'s
per-table index-resolution N+1 into ONE ``resolve_index_metadata`` read
filtered to ``l.table_uuid IN (<the N listed tables>)`` and demuxed by
``table_uuid``. That is one round-trip — but still a merge SCAN (base decode +
filter), not a point seek.

CHA-499 turns that bounded scan into an O(log n) leading-PREFIX seek on the
existing composite ``(table_uuid, index_name)`` name sidecar
(``SystemNameIndexSpec``) when ``__penca_system__.indexes`` is
snapshot-materialized: a ``table_uuid``-only (arity-1) probe seeks the leading
key column and returns every index row of each listed table. The
not-materialized case stays the CHA-496 filtered scan (the derived
``l.table_uuid IN (…)`` residual), so the seek is built ON TOP of the scan.

Observable via the ``direct_point_read=true`` debug marker the shared
cold-read bypass emits (``stream_cold_read``) — the same marker the CHA-484
by-name fast-path tests assert. Because only the indexes table is
snapshot-covered in these tests, a bare ``direct_point_read=true`` in the
window uniquely identifies
the indexes resolve (the tables/schemas resolves are not covered → they ride
``stream_merged`` and never emit the marker).

Fail-first: on the pre-CHA-499 query side ``meta_list_tables`` selects the
CHA-496 ``Scan`` (filtered merge read), so the indexes resolve emits NO
``direct_point_read=true`` even when the table is materialized → test 1 fails.
The demux-correctness half (each table carries its own index in declared column
order) and the not-materialized fallback control are green regression guards.

Scoped run::

    just integration-test cha499_listtables_prefix_seek
"""

from __future__ import annotations

import re
import time
from uuid import uuid4

from penca_client.naming import (
    system_indexes_table_uuid,
    system_schema_uuid,
)

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    container_log,
    make_client,
    poll_log_for,
)

# The DataFusion-free seek bypass marker (CHA-380 unified metadata reads onto
# read_data's ``direct_point_read``). Only ``__penca_system__.indexes`` is
# snapshot-covered and ``list_tables`` issues no user-data read, so a bare
# marker in the window uniquely identifies the indexes resolve.
_DIRECT_MARKER = "direct_point_read=true"

# Per table: (index_name, indexed columns). Distinct columns per table so the
# demux is observably keyed on table_uuid, and a composite pins column ORDER
# (mirrors CHA-496's _INDEX_SPECS).
_INDEX_SPECS = {
    "t_a": ("idx_a", ["value"]),
    "t_b": ("idx_b", ["name"]),
    "t_c": ("idx_c", ["value", "name"]),
}


def _own_close_re(span: str) -> re.Pattern[str]:
    """Regex matching a span's OWN span-CLOSE line, excluding CLOSE lines of
    CHILD spans that merely carry ``span`` as an ancestor scope prefix (the
    ``tracing`` fmt subscriber separates the innermost span from its event
    target with ``}: `` — colon-SPACE). Mirrors CHA-496."""
    return re.compile(re.escape(span) + r"\{[^}]*\}: \S")


def _count_closes(span: str, since: int) -> int:
    pattern = _own_close_re(span)
    return sum(
        1
        for line in container_log("query")[since:].splitlines()
        if "close time.busy" in line and pattern.search(line)
    )


def _poll_stable_closes(
    span: str,
    since: int,
    min_count: int,
    settle_s: float = 0.5,
    deadline_s: float = 8.0,
) -> int:
    """Poll ``span``'s own-close count until it reaches >= ``min_count`` AND
    stops growing for ``settle_s`` seconds (flush-lag barrier without a
    sentinel). Mirrors CHA-496 — the measured list_tables is the only emitter
    of ``resolve_index_metadata`` in the window."""
    deadline = time.monotonic() + deadline_s
    last = -1
    stable_since = deadline
    while time.monotonic() < deadline:
        count = _count_closes(span, since)
        if count != last:
            last = count
            stable_since = time.monotonic()
        elif count >= min_count and (time.monotonic() - stable_since) >= settle_s:
            return count

        time.sleep(0.1)

    return last


def _snapshot_cover(
    client, catalog_uuid: str, branch_uuid: str, sys_table_uuid: str
) -> int:
    """Persist → snapshot → purge one ``__penca_system__`` table so its plan is
    snapshot-only (CHA-444: snapshot precedes purge so Pu <= W_snap), building
    its composite name sidecar. Mirrors CHA-484's fixture. Returns the
    snapshot's ``snapshotted_at_micros`` watermark."""
    kw = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": system_schema_uuid(catalog_uuid),
        "branch_uuid": branch_uuid,
        "table_uuid": sys_table_uuid,
    }
    client.persist(**kw)
    resp = client.snapshot(**kw)
    client.purge(**kw)
    assert resp.snapshotted_at_micros, (
        "snapshotting a populated system table must materialize a cold "
        "snapshot (got a no-op watermark)"
    )
    return resp.snapshotted_at_micros


def _setup_indexed_tables(client) -> dict:
    """Fresh catalog + schema ``s`` + the _INDEX_SPECS tables, each with its own
    materialized-eligible secondary index. Returns uuids."""
    catalog_uuid, branch_uuid = client.create_catalog(
        f"cha499_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="cha-499"
    )
    table_uuids: dict[str, str] = {}
    index_uuids: dict[str, str] = {}
    for table_name, (index_name, columns) in _INDEX_SPECS.items():
        table_uuids[table_name] = client.create_table(
            table_name,
            USER_SCHEMA,
            primary_keys=["name"],
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-499",
        )
        index_uuids[table_name] = client.create_index(
            index_name=index_name,
            columns=columns,
            index_type=SCALAR_BTREE,
            table_name=table_name,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            branch_uuid=branch_uuid,
            author="test",
            comment="cha-499",
        )

    return {
        "catalog_uuid": catalog_uuid,
        "branch_uuid": branch_uuid,
        "schema_uuid": schema_uuid,
        "table_uuids": table_uuids,
        "index_uuids": index_uuids,
    }


def _assert_demux_correct(listed, ctx):
    """Each listed table carries ONLY its own index, in declared column order
    (green from CHA-492/496; guards that the demux attaches by table_uuid, not
    by scan position — must stay green under the seek path too)."""
    by_uuid = {table.table_uuid: table for table in listed}
    for table_name, (index_name, columns) in _INDEX_SPECS.items():
        table = by_uuid[ctx["table_uuids"][table_name]]
        got_names = [ix.index_name for ix in table.indexes]
        assert got_names == [index_name], (
            f"{table_name} must carry exactly its own index {index_name!r}, got {got_names}"
        )
        index = table.indexes[0]
        assert index.index_uuid == ctx["index_uuids"][table_name]
        assert list(index.columns) == columns  # declared order preserved


class TestListTablesPrefixSeek:
    def test_materialized_indexes_list_takes_prefix_seek(self):
        """RED driver: a ListTables over a snapshot-covered
        __penca_system__.indexes must serve the indexes resolve via the
        leading-prefix seek (direct_point_read=true), still as ONE read, with
        correct demux."""
        client = make_client()
        try:
            ctx = _setup_indexed_tables(client)
            _snapshot_cover(
                client,
                ctx["catalog_uuid"],
                ctx["branch_uuid"],
                system_indexes_table_uuid(ctx["catalog_uuid"]),
            )

            since = len(container_log("query"))
            listed = list(
                client.list_tables(
                    catalog_uuid=ctx["catalog_uuid"],
                    schema_uuid=ctx["schema_uuid"],
                    branch_uuid=ctx["branch_uuid"],
                )
            )

            _assert_demux_correct(listed, ctx)

            # The target: the indexes resolve took the table_uuid prefix seek.
            # Only the indexes table is snapshot-covered, so a bare marker in
            # the window uniquely identifies that resolve.
            assert poll_log_for("query", since, _DIRECT_MARKER), (
                "a ListTables over a snapshot-covered __penca_system__.indexes "
                f"must serve the indexes resolve via the table_uuid prefix seek "
                f"({_DIRECT_MARKER}); pre-CHA-499 it rides the CHA-496 filtered "
                "merge scan and emits no marker"
            )

            # CHA-496 pin still holds: exactly ONE indexes read for the list.
            closes = _poll_stable_closes("resolve_index_metadata", since, min_count=1)
            assert closes == 1, (
                f"prefix-seek ListTables must still issue exactly ONE "
                f"__penca_system__.indexes read, got {closes}"
            )
        finally:
            client.close()

    def test_not_materialized_indexes_list_falls_back_to_scan(self):
        """Green control: freshly-created (not snapshot-covered) index rows have
        no sidecar, so the indexes resolve must fall back to the CHA-496 filtered
        scan — correct rows, NO direct_point_read marker. Pins the ticket's
        not-materialized non-goal."""
        client = make_client()
        try:
            ctx = _setup_indexed_tables(client)  # NO _snapshot_cover.

            since = len(container_log("query"))
            listed = list(
                client.list_tables(
                    catalog_uuid=ctx["catalog_uuid"],
                    schema_uuid=ctx["schema_uuid"],
                    branch_uuid=ctx["branch_uuid"],
                )
            )

            _assert_demux_correct(listed, ctx)

            # Flush barrier: wait for the resolve's CLOSE line to flush (any
            # marker it emitted is then already in the window), then assert the
            # marker is ABSENT — the not-materialized resolve scanned.
            closes = _poll_stable_closes("resolve_index_metadata", since, min_count=1)
            assert closes == 1
            window = container_log("query")[since:]
            assert _DIRECT_MARKER not in window, (
                "a not-materialized indexes list must ride the CHA-496 filtered "
                f"scan (no {_DIRECT_MARKER}), got the seek marker"
            )
        finally:
            client.close()
