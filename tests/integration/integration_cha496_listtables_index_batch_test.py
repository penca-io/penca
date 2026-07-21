"""CHA-496 — ListTables batches index resolution into ONE indexes read.

Follow-up to CHA-492 (PR #298), which populated ``Table.indexes`` on
``GetTable`` / ``ListTables`` by resolving a table's defined indexes ONCE PER
TABLE inside ``QueryManager::meta_list_tables`` — an N+1 on a list-many RPC (N
identical fused watermark queries + N filtered reads of the single
``__penca_system__.indexes`` table). CHA-492 left the fix as ``TODO(CHA-496)``.

This pins the fix: a ``ListTables`` over a schema with several indexed tables
must issue exactly ONE ``__penca_system__.indexes`` read, demuxed by
``table_uuid`` — observed as exactly one ``resolve_index_metadata`` span-CLOSE
line in the query container's ``penca=debug`` log (``PENCA_SPAN_TIMING=1``,
docker/test.env renders one close per resolve).

Fail-first: on ``main`` the per-table loop emits N=3 ``resolve_index_metadata``
closes for a 3-indexed-table schema, so ``n == 1`` fails with observed 3. The
demux-correctness half (each table carries only its own index, in declared
column order) is already green from CHA-492 and stays green.

Scoped run:  just integration-test cha496_listtables_index_batch
"""

from __future__ import annotations

import re
import time
from uuid import uuid4

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    container_log,
    make_client,
)


def _own_close_re(span: str) -> re.Pattern[str]:
    """Regex matching a span's OWN span-CLOSE line, excluding CLOSE lines of
    CHILD spans that merely carry ``span`` as an ancestor scope prefix. The
    ``tracing`` fmt subscriber joins nested spans with ``}:<child>`` (no space)
    but separates the innermost span from its event target with ``}: <target>``
    (colon-SPACE), so ``span{...}: `` (trailing space before the target)
    uniquely identifies the innermost-span (own) close. A plain
    ``span in line`` substring test would over-count every child span's close
    (~14× per resolve) — the ancestor-prefix trap."""
    return re.compile(re.escape(span) + r"\{[^}]*\}: \S")


def _count_closes(span: str, since: int) -> int:
    """Count ``span``'s OWN span-CLOSE lines in the query-log window (one per
    resolve under ``PENCA_SPAN_TIMING``)."""
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
    stops growing for ``settle_s`` seconds, then return it.

    Flush-lag barrier WITHOUT a sentinel: when the measured operation is the
    only emitter of ``span`` in the ``since:`` window, its close count rises
    monotonically as the container flushes and then stops at its final value.
    Waiting for that value to settle fences the count against flush lag without
    coupling to any *other* marker's identity — unlike a "distinct sentinel"
    poll, which is only sound if the sentinel marker is one the measured op
    never emits itself (the trap that made an earlier `list_schemas` /
    `resolve_schema_metadata` sentinel here unsound: ListTables emits
    `resolve_schema_metadata` too, so the poll tripped on its own
    scope-resolution close instead of fencing the index read)."""
    deadline = time.monotonic() + deadline_s
    last = -1
    stable_since = deadline  # not-yet-stable sentinel
    while time.monotonic() < deadline:
        count = _count_closes(span, since)
        if count != last:
            last = count
            stable_since = time.monotonic()
        elif count >= min_count and (time.monotonic() - stable_since) >= settle_s:
            return count

        time.sleep(0.1)

    return last


# Per table: (index_name, indexed columns). Distinct columns per table so the
# demux is observably keyed on table_uuid, and a composite pins column ORDER.
_INDEX_SPECS = {
    "t_a": ("idx_a", ["value"]),
    "t_b": ("idx_b", ["name"]),
    "t_c": ("idx_c", ["value", "name"]),
}


class TestListTablesBatchesIndexResolution:
    def test_list_tables_issues_single_indexes_read(self):
        client = make_client()
        catalog_uuid, branch_uuid = client.create_catalog(
            f"cha496_{uuid4().hex[:8]}", "owner"
        )
        schema_uuid = client.create_schema(
            "s", catalog_uuid=catalog_uuid, author="test", comment="cha-496"
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
                comment="cha-496",
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
                comment="cha-496",
            )

        since = len(container_log("query"))
        listed = list(
            client.list_tables(
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=branch_uuid,
            )
        )

        # --- Demux correctness: each table carries ONLY its own index, in
        # declared column order (already green from CHA-492; guards that the
        # batch demux attaches by table_uuid, not by scan position). ---
        by_uuid = {table.table_uuid: table for table in listed}
        for table_name, (index_name, columns) in _INDEX_SPECS.items():
            table = by_uuid[table_uuids[table_name]]
            got_names = [ix.index_name for ix in table.indexes]
            assert got_names == [index_name], (
                f"{table_name} must carry exactly its own index "
                f"{index_name!r}, got {got_names}"
            )
            index = table.indexes[0]
            assert index.index_uuid == index_uuids[table_name]
            assert list(index.columns) == columns  # declared order preserved

        # --- N+1 pin (the target): exactly ONE __penca_system__.indexes read
        # for the whole list. The measured list_tables is the ONLY emitter of
        # `resolve_index_metadata` in [since:], so its own-close count rises
        # monotonically to its final value then stops — poll until it stabilizes
        # (race-immune to flush lag; no sentinel-marker coupling). A returned 0
        # means no close ever flushed → PENCA_SPAN_TIMING unset on the query
        # container (docker/test.env), surfaced by the `got 0` assertion. ---
        closes = _poll_stable_closes("resolve_index_metadata", since, min_count=1)
        assert closes == 1, (
            f"ListTables over {len(_INDEX_SPECS)} indexed tables must issue "
            f"exactly ONE __penca_system__.indexes read (one "
            f"resolve_index_metadata span close), got {closes}"
        )
