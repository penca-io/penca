"""CHA-492 — a materialized user-index equality lookup is served DataFusion-free.

CHA-485 selects a covering user index, but only as a *scan accelerator under
DataFusion*: the read still runs the `scan_snapshot` merge arm (with
`index_seek=true` / `index_seek_entries=n` inside it). CHA-492 recognizes that
when a single covering index fully covers the predicate on a snapshot-only
table, the seek IS the exact answer, so the read is routed to the same
DataFusion-free `seek_snapshot_point` bypass the identity `ids` point read
takes — no `SessionContext`, no `scan_snapshot`.

The predicate binds a NON-PK column (`city`), so a DataFusion-free seek serving
it can only be the user-index seek — the identity (PK) seek cannot answer a
non-PK equality. That makes `seek_snapshot_point` present + `scan_snapshot`
absent a sufficient witness of the user-index bypass without scraping the
sidecar's `index_uuid`.

Fail-first: today the read runs `scan_snapshot` and never emits
`seek_snapshot_point`, so both trace assertions fail while the row-correctness
assertion already passes. Parametrized over both drivers: the fix lands in
`PencaTableProvider::scan`, below the ADBC-prepared / JDBC-statement split.

Scoped run:  just integration-test cha492_user_index_seek
"""

from __future__ import annotations

import json
from uuid import uuid4

import pyarrow as pa
import pytest
from penca_client import Mutation

from .integration_helpers import (
    SCALAR_BTREE,
    container_log,
    make_client,
    poll_log_for,
)
from .integration_point_read_test import _sql_steps_via

# Serial: reads a process-global side channel; see the `serial` marker in
# pyproject.toml. TODO(CHA-519): drop with the scrape it protects.
pytestmark = pytest.mark.serial

# name (PK) + a Utf8 non-PK column to index + an int64 payload.
_SCHEMA = pa.schema(
    [
        pa.field("name", pa.utf8()),
        pa.field("city", pa.utf8()),
        pa.field("value", pa.int64()),
    ]
)
_ROWS = {
    "name": ["alice", "bob", "carol"],
    "city": ["paris", "paris", "london"],
    "value": [10, 20, 30],
}


def _setup_indexed_snapshot_only(client) -> dict:
    """Create catalog/schema/table on `_SCHEMA`, write `_ROWS`, commit, declare
    an index on `city`, then persist→snapshot→purge so the baseline (and the
    declared `city` index sidecar, CHA-483 materialize-on-next-snapshot) is
    snapshot-only cold. Pins the client's Flight SQL connection to the fresh
    catalog (CHA-169)."""
    catalog_name = f"cha492seek_{uuid4().hex[:8]}"
    catalog_uuid, main_branch_uuid = client.create_catalog(catalog_name, "owner")
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="cha492seek"
    )
    table_uuid = client.create_table(
        "t",
        _SCHEMA,
        primary_keys=["name"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author="test",
        comment="cha492seek",
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
    client.create_index(
        table_uuid=table_uuid,
        index_name="idx_city",
        columns=["city"],
        index_type=SCALAR_BTREE,
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author="test",
        comment="cha492seek",
    )
    kw = {
        "catalog_uuid": catalog_uuid,
        "schema_uuid": schema_uuid,
        "branch_uuid": main_branch_uuid,
        "table_uuid": table_uuid,
    }
    client.persist(**kw)
    client.snapshot(**kw)
    client.purge(**kw)

    client.catalog = catalog_name
    return {"catalog_name": catalog_name, "fqn": f"{catalog_name}.s.t"}


class TestCha492UserIndexSeek:
    @pytest.mark.parametrize("driver", ["adbc", "jdbc"])
    def test_materialized_user_index_equality_seeks_datafusion_free(self, driver):
        client = make_client()
        ctx = _setup_indexed_snapshot_only(client)

        since = len(container_log("query"))
        results = _sql_steps_via(
            driver,
            [f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"],
            ctx["catalog_name"],
        )
        status, payload = results[0]
        assert status == "OK_ROWS", results
        rows = json.loads(payload)
        assert sorted(r["name"] for r in rows) == ["alice", "bob"], results

        # The DataFusion-free arm: a materialized covering index equality on a
        # snapshot-only table serves via `seek_snapshot_point`, no merge scan.
        assert poll_log_for("query", since, "seek_snapshot_point"), (
            "a materialized user-index equality lookup on a snapshot-only "
            "table must be served DataFusion-free (seek_snapshot_point); today "
            "it runs scan_snapshot under DataFusion (index seek as a scan "
            "accelerator only)"
        )
        # Flush barrier + exact count: the seek poll above
        # is a barrier on a DIFFERENT needle, so a scan_snapshot from the same
        # read could still be un-flushed at scrape time. Issue a second
        # qualifying read and poll ITS bypass marker; once that flushes the log
        # is append-only, so any scan_snapshot the first read emitted is
        # guaranteed present — a COUNT over the window is race-immune.
        barrier_since = len(container_log("query"))
        _sql_steps_via(
            driver,
            [f"SELECT name FROM {ctx['fqn']} WHERE city = 'paris'"],
            ctx["catalog_name"],
        )
        assert poll_log_for("query", barrier_since, "seek_snapshot_point"), (
            "flush-barrier qualifying read must also take the DataFusion-free bypass"
        )
        assert container_log("query")[since:].count("scan_snapshot") == 0, (
            "the user-index exact-cover bypass must not build a DataFusion "
            "cold scan (scan_snapshot span present)"
        )
