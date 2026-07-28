"""CHA-473 — by-uuid metadata point reads restrict via the ``row_uuids`` field.

The by-uuid metadata resolves over the system tables
(``__penca_system__.{tables,schemas,indexes}``) must thread the structured
CHA-398 ``row_uuids`` restriction instead of the opaque
``l.row_uuid = '<uuid>'`` filter string. Once they do, the exclusion-set
probe restricts to ``WHERE row_uuid IN (…)`` (an index seek on the
``(row_uuid, tx_uuid)`` index ``create_data_tables`` builds for the system
tables) rather than scanning the whole hot log.

The server-side signal is the ``row_uuids`` count recorded on the
``resolve_{table,schema,index}_metadata`` spans (mirrors the CHA-426
``ids_rows`` seam): a by-uuid resolve must close with ``row_uuids=1``. This
renders on the span's ``close time.busy=..`` line under
``RUST_LOG=info,penca=debug`` + ``PENCA_SPAN_TIMING=1`` (docker/test.env),
the same scrape seam the CHA-426 / CHA-417 tests use.

RED before the CHA-473 wiring: the resolves passed ``row_uuids: None`` and
carried no such field, so no resolve span CLOSE line ever carried
``row_uuids=1``.

Scoped run: ``just integration-test metadata_point_read``
"""

from __future__ import annotations

import re
import time
from uuid import uuid4

import pytest

from .integration_helpers import (
    SCALAR_BTREE,
    USER_SCHEMA,
    container_log,
    make_client,
)

# Serialized: asserts on process-global white-box state (container stdout log
# windows / pg_stat_statements counters) that a concurrent worker would
# pollute. Runs in the serial phase, not under -n auto.
# TODO(CHA-519): drop this mark once the structured per-request seam lands.
pytestmark = pytest.mark.serial


def _meta_row_uuids_one_re(span: str) -> re.Pattern[str]:
    """Match a ``<span>{… row_uuids=1 …}`` field group.

    Anchored INSIDE the span's brace group so a child span that inherits
    ``row_uuids`` through the scope-chain prefix cannot satisfy the pin.
    """
    return re.compile(rf"{re.escape(span)}\{{[^}}]*row_uuids=1(\D|$)")


def _poll_for_meta_restricted(
    span: str, since: int, deadline_seconds: float = 5.0
) -> tuple[int, int]:
    """Poll the query-container log window for ``<span>`` CLOSE lines that
    carry ``row_uuids=1``.

    Returns ``(restricted_close_count, any_close_count)``. ``any_close_count``
    is the CHA-417-style sanity guard: if NO span CLOSE lines appear at all,
    the span-timing seam is misconfigured and the failure is a harness error,
    not a red assertion.
    """
    pat = _meta_row_uuids_one_re(span)
    deadline = time.monotonic() + deadline_seconds
    restricted = 0
    any_closes = 0
    while time.monotonic() < deadline:
        lines = container_log("query")[since:].splitlines()
        restricted = sum(
            1 for line in lines if "close time.busy" in line and pat.search(line)
        )
        any_closes = sum(
            1 for line in lines if "close time.busy" in line and span in line
        )
        if restricted >= 1:
            break

        time.sleep(0.2)

    return restricted, any_closes


def _assert_meta_restricted(span: str, since: int, context: str) -> None:
    restricted, any_closes = _poll_for_meta_restricted(span, since)
    assert any_closes >= 1, (
        f"no `{span}` CLOSE lines at all in the query-log window after "
        f"{context} — either PENCA_SPAN_TIMING is unset, penca=debug is "
        "off, or this RPC does not drive that resolve; harness/coverage "
        "issue, not a red result."
    )
    assert restricted >= 1, (
        f"expected >= 1 `{span}` span CLOSE with row_uuids=1 after {context}; "
        f"got 0 (window had {any_closes} `{span}` CLOSE lines, so the span "
        "fires — the resolve is not threading the row_uuids restriction)."
    )


def _new_catalog_schema_table(client) -> dict:
    """Create a fresh catalog + schema + table and return their uuids."""
    catalog_uuid, branch_uuid = client.create_catalog(
        f"cha473_{uuid4().hex[:8]}", "owner"
    )
    schema_uuid = client.create_schema(
        "s", catalog_uuid=catalog_uuid, author="test", comment="create_schema"
    )
    table_uuid = client.create_table(
        "t",
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
        "table_uuid": table_uuid,
    }


# RT-1 — table metadata point-read-by-uuid restricts the resolve.
class TestTableMetadataPointRead:
    """CHA-473: a by-uuid table resolve threads the row_uuids restriction."""

    def test_get_table_by_uuid_restricts_resolve(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_table(client)
            since = len(container_log("query"))
            info = client.get_table(
                table_uuid=ctx["table_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.table_uuid == ctx["table_uuid"]
            _assert_meta_restricted(
                "resolve_table_metadata", since, "a get_table by uuid"
            )
        finally:
            client.close()


# RT-2 — schema + index metadata point-read-by-uuid restrict the resolve.
class TestSchemaIndexMetadataPointRead:
    """CHA-473: by-uuid schema/index resolves thread the row_uuids restriction."""

    def test_get_schema_by_uuid_restricts_resolve(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_table(client)
            since = len(container_log("query"))
            info = client.get_schema(
                schema_uuid=ctx["schema_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                branch_uuid=ctx["branch_uuid"],
            )
            assert info.schema_uuid == ctx["schema_uuid"]
            _assert_meta_restricted(
                "resolve_schema_metadata", since, "a get_schema by uuid"
            )
        finally:
            client.close()

    def test_get_index_by_uuid_restricts_resolve(self):
        client = make_client()
        try:
            ctx = _new_catalog_schema_table(client)
            index_uuid = client.create_index(
                index_name="idx_value",
                columns=["value"],
                index_type=SCALAR_BTREE,
                table_uuid=ctx["table_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
                author="test",
                comment="create_index",
            )
            since = len(container_log("query"))
            info = client.get_index(
                index_uuid=index_uuid,
                table_uuid=ctx["table_uuid"],
                catalog_uuid=ctx["catalog_uuid"],
                schema_uuid=ctx["schema_uuid"],
            )
            assert info.index_uuid == index_uuid
            _assert_meta_restricted(
                "resolve_index_metadata", since, "a get_index by uuid"
            )
        finally:
            client.close()
