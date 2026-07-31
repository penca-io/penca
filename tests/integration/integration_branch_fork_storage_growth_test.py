"""CHA-531 red test: storage growth for N forks of one table is
O(delta) per fork, not O(N x table).

Each fork's first snapshot currently takes the CHA-404 full-rewrite
path, so N forks that each touch a single partition cost N full copies
of the table. With cross-branch carry-forward, untouched partitions are
shared by reference and only the touched partition is rewritten.

The ``(object_uri, offset)`` **slice** is the unit of measurement, not
the file: the packer emits one segment row per partition and packs
several small partitions into one file, so a uri is not a unit of
storage. Carry-forward copies both columns verbatim, so a carried slice
collapses onto the row it references and contributes nothing — which is
exactly what separates "referenced" from "copied".

Run via ``just integration-test branch_fork_storage_growth``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client.naming import (
    TABLE_PERSIST_SEGMENT_METADATA,
    TABLE_SNAPSHOT_SEGMENT_METADATA,
)
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    setup_partitioned_table,
    write_cycle,
)

_PARTITIONS = ["p0", "p1", "p2", "p3", "p4", "p5"]
_FORKS = 3

# ── Helpers ───────────────────────────────────────────────────────────


def _distinct_slice_bytes(
    catalog_uuid, table_name=TABLE_SNAPSHOT_SEGMENT_METADATA, table_uuid=None
):
    """``(distinct_file_count, total_bytes)`` over every committed segment row in
    ``table_name``, deduped by storage slice.

    Parameterized over the segment table because both cold tiers carry
    ``object_uri`` / ``"offset"`` / ``size_bytes`` and the same slice identity
    applies to each. Defaults to the snapshot tier, which is the only one
    carry-forward shares.

    ``size_bytes`` is the *partition slice's* in-memory footprint
    (CHA-347), not a file size: the packer emits one segment row per
    partition and packs several small partitions into one file, so a
    single ``object_uri`` carries many rows with distinct ``offset``s.
    Deduping by uri alone would therefore measure one partition, not the
    table. ``(object_uri, "offset")`` is the slice identity, and
    carry-forward copies both verbatim, so a carried row collapses onto
    the row it references and contributes nothing — which is exactly the
    "referenced, not copied" distinction being measured.
    """
    seg = f"{catalog_uuid}_{table_name}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(DISTINCT object_uri), coalesce(sum(bytes), 0) FROM ("
            '  SELECT object_uri, "offset", max(size_bytes) AS bytes FROM {tbl}'
            "  WHERE commit_micros IS NOT NULL"
            + ("  AND table_uuid = %s" if table_uuid is not None else "")
            + '  GROUP BY object_uri, "offset"'
            ") f"
        ).format(tbl=Identifier(seg)),
        () if table_uuid is None else (table_uuid,),
    )
    return int(rows[0][0]), int(rows[0][1])


# ── Tests ─────────────────────────────────────────────────────────────


def test_fork_storage_growth_is_o_delta():
    """Three forks each touching ONE of six partitions must add roughly
    one partition's bytes apiece, not one table's."""
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fsg")
    )

    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": _PARTITIONS, "value": list(range(len(_PARTITIONS)))},
            schema=USER_SCHEMA,
        ),
    )
    baseline_files, baseline_bytes = _distinct_slice_bytes(catalog_uuid)

    # Guard the metric before asserting on it: size_bytes defaults to 0,
    # so a table of zero-byte segments would make the bound below pass
    # (or fail) for reasons unrelated to carry-forward.
    assert baseline_files > 0, "the parent snapshot must have written segment files"
    assert baseline_bytes > 0, (
        "size_bytes is not populated, so the growth bound would be vacuous;"
        f" baseline over {baseline_files} files is {baseline_bytes} bytes"
    )

    for i in range(_FORKS):
        child = client.create_branch(
            f"fsg_child{i}_{uuid4().hex[:6]}",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="cha-531",
        ).branch_uuid
        write_cycle(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=child,
            upserts=pa.table(
                {"name": [_PARTITIONS[i]], "value": [100 + i]}, schema=USER_SCHEMA
            ),
        )

    final_files, final_bytes = _distinct_slice_bytes(catalog_uuid)
    growth = final_bytes - baseline_bytes

    # A full rewrite per fork adds ~one table's bytes per fork. Carrying
    # untouched partitions by reference adds only the touched partition.
    # The bound is a ratio, not a byte count, so it survives format and
    # compression drift: each fork may add at most half a table.
    budget = baseline_bytes * _FORKS // 2
    assert growth <= budget, (
        f"{_FORKS} forks each touching 1 of {len(_PARTITIONS)} partitions grew"
        f" cold storage by {growth} bytes of segment footprint (CHA-347"
        f" size_bytes, a format-independent proxy for stored bytes) over a"
        f" {baseline_bytes}-byte baseline (budget {budget}); distinct files"
        f" {baseline_files} -> {final_files}. Each fork's first snapshot is"
        " re-materializing the whole table instead of carrying untouched"
        " partitions by reference."
    )


def _reference_row_counts(catalog_uuid, branch_uuid):
    """``(persist_rows, snapshot_rows)`` — the branch's own committed cold
    reference rows.

    Rows, not distinct uris: the metadata cost CHA-539 accepts is one row per
    referenced cold segment, and several rows legitimately share one file.
    """
    counts = []
    for table_name in (TABLE_PERSIST_SEGMENT_METADATA, TABLE_SNAPSHOT_SEGMENT_METADATA):
        rows = get_pg_driver().execute(
            SQL(
                "SELECT count(*) FROM {tbl}"
                " WHERE branch_uuid = %s AND commit_micros IS NOT NULL"
            ).format(tbl=Identifier(f"{catalog_uuid}_{table_name}")),
            (branch_uuid,),
        )
        counts.append(int(rows[0][0]))

    return tuple(counts)


def test_fork_materializes_metadata_reference_rows_per_cold_segment():
    """CHA-539: a fork's cost becomes O(cold segments) in METADATA rows, while
    object count and bytes stay bounded.

    That trade is the ticket's accepted risk ("Fork cost goes O(1) ->
    O(cold segments) in metadata rows written. Still no data copy"). Pinning the
    row growth explicitly is what stops a later change from quietly turning it
    into byte growth: `test_fork_storage_growth_is_o_delta` above bounds the
    bytes, and this bounds what the fork is allowed to spend instead.

    Fail-first: a fork writes no cold reference rows at all today, so the child's
    counts are (0, 0) immediately after CreateBranch — before it has written or
    snapshotted anything of its own.
    """
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = (
        setup_partitioned_table("fsgref")
    )

    write_cycle(
        client,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=main_branch,
        upserts=pa.table(
            {"name": _PARTITIONS, "value": list(range(len(_PARTITIONS)))},
            schema=USER_SCHEMA,
        ),
    )
    parent_persist, parent_snapshot = _reference_row_counts(catalog_uuid, main_branch)
    assert parent_persist > 0 and parent_snapshot > 0, (
        "the parent must hold committed rows in both cold tiers before the fork"
    )
    # Both tiers, because the fork materializes reference rows in both. Measuring
    # only the snapshot tier would let an implementation satisfy the persist
    # assertion above by re-materializing the parent's persist segments under the
    # child's own per-branch prefix — O(N forks x persist bytes) of real storage —
    # and still report "unchanged".
    # Scoped to the user table. Catalog-wide would also count the
    # `__penca_system__` persist segments each fork's own CreateBranch flush
    # writes for the schema/table DDL rows it materializes — real objects, but
    # nothing to do with whether the fork copied this table's DATA.
    baseline = {
        table_name: _distinct_slice_bytes(catalog_uuid, table_name, table_uuid)
        for table_name in (
            TABLE_PERSIST_SEGMENT_METADATA,
            TABLE_SNAPSHOT_SEGMENT_METADATA,
        )
    }
    for table_name, (files, byte_total) in baseline.items():
        assert files > 0 and byte_total > 0, (
            f"{table_name} baseline is {files} files / {byte_total} bytes, so the"
            " neutrality assertion below would be vacuous"
        )

    # Fork only — no write, no snapshot on the child. Isolates what the FORK
    # materializes from what a later child snapshot would produce.
    children = [
        client.create_branch(
            f"fsgref_child{i}_{uuid4().hex[:6]}",
            catalog_uuid=catalog_uuid,
            author="test",
            comment="cha-539",
        ).branch_uuid
        for i in range(_FORKS)
    ]

    for i, child in enumerate(children):
        child_persist, child_snapshot = _reference_row_counts(catalog_uuid, child)
        assert child_persist >= parent_persist, (
            f"fork {i} must materialize a persist reference row per inherited cold"
            f" segment; parent has {parent_persist}, child has {child_persist}"
        )
        assert child_snapshot >= parent_snapshot, (
            f"fork {i} must materialize a snapshot reference row per inherited cold"
            f" segment; parent has {parent_snapshot}, child has {child_snapshot}"
        )

    # ...and the rows must be references, not copies: same slices, same bytes,
    # in EVERY tier the fork wrote a reference row for.
    for table_name, expected in baseline.items():
        actual = _distinct_slice_bytes(catalog_uuid, table_name, table_uuid)
        assert actual == expected, (
            f"{_FORKS} forks changed {table_name} from {expected[0]} files /"
            f" {expected[1]} bytes to {actual[0]} / {actual[1]}. The fork copies"
            " METADATA only; a change here means it started copying data."
        )
