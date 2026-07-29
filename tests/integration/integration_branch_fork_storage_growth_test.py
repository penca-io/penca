"""CHA-531 red test: storage growth for N forks of one table is
O(delta) per fork, not O(N x table).

Each fork's first snapshot currently takes the CHA-404 full-rewrite
path, so N forks that each touch a single partition cost N full copies
of the table. With cross-branch carry-forward, untouched partitions are
shared by reference and only the touched partition is rewritten.

Distinct ``object_uri`` is the unit of measurement: carried rows share
a uri with the parent, so deduping by uri is exactly what separates
"referenced" from "copied".

Run via ``just integration-test branch_fork_storage_growth``.
"""

from __future__ import annotations

from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation
from penca_client.naming import TABLE_SNAPSHOT_SEGMENT_METADATA
from psycopg.sql import SQL, Identifier

from .integration_helpers import (
    USER_SCHEMA,
    get_pg_driver,
    make_client,
)

_PARTITIONS = ["p0", "p1", "p2", "p3", "p4", "p5"]
_FORKS = 3

# ── Helpers ───────────────────────────────────────────────────────────


def _make_env():
    client = make_client()
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"fsg_cat_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "fsg_schema", catalog_uuid=catalog_uuid, author="test", comment="cha-531"
    )
    table_uuid = client.create_table(
        "fsg_table",
        USER_SCHEMA,
        primary_keys=["name"],
        partition_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha-531",
    )
    return client, catalog_uuid, schema_uuid, table_uuid, main_branch_uuid


def _cycle(client, *, catalog_uuid, schema_uuid, table_uuid, branch_uuid, upserts):
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid, schema_uuid=schema_uuid, branch_uuid=branch_uuid
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(table_uuid=table_uuid, upserts=upserts),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
    client.persist(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=branch_uuid,
        table_uuid=table_uuid,
    )
    response = client.snapshot(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        table_uuid=table_uuid,
        branch_uuid=branch_uuid,
    )
    assert response.HasField("snapshotted_at_micros")


def _distinct_uri_bytes(catalog_uuid):
    """``(distinct_file_count, total_bytes)`` over every committed
    snapshot segment in the catalog, deduped by storage slice.

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
    seg = f"{catalog_uuid}_{TABLE_SNAPSHOT_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT count(DISTINCT object_uri), coalesce(sum(bytes), 0) FROM ("
            '  SELECT object_uri, "offset", max(size_bytes) AS bytes FROM {tbl}'
            "  WHERE commit_micros IS NOT NULL"
            '  GROUP BY object_uri, "offset"'
            ") f"
        ).format(tbl=Identifier(seg)),
        (),
    )
    return int(rows[0][0]), int(rows[0][1])


# ── Tests ─────────────────────────────────────────────────────────────


def test_fork_storage_growth_is_o_delta():
    """Three forks each touching ONE of six partitions must add roughly
    one partition's bytes apiece, not one table's."""
    client, catalog_uuid, schema_uuid, table_uuid, main_branch = _make_env()

    _cycle(
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
    baseline_files, baseline_bytes = _distinct_uri_bytes(catalog_uuid)

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
        _cycle(
            client,
            catalog_uuid=catalog_uuid,
            schema_uuid=schema_uuid,
            table_uuid=table_uuid,
            branch_uuid=child,
            upserts=pa.table(
                {"name": [_PARTITIONS[i]], "value": [100 + i]}, schema=USER_SCHEMA
            ),
        )

    final_files, final_bytes = _distinct_uri_bytes(catalog_uuid)
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
