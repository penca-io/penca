"""gRPC-API by-name metadata-resolve performance suite (CHA-484).

Measures the CHA-484 win: a by-name metadata resolve
(``get_table(table_name=…)`` / ``get_schema(schema_name=…)``) over a
snapshot-covered ``__penca_system__`` table seeks the CHA-481 built-in
composite name index through the internal seek path, skipping DataFusion
entirely (the ~1.37 ms plan+exec the CHA-473 secondary finding measured).

Arms, each ``_RESOLVE_REPS``-averaged like the OLTP point read:
  1. by-name ``get_table`` over a snapshot-covered system table — the
     fast-path arm (``direct_point_read``);
  2. by-name ``get_schema`` over a snapshot-covered system table;
  3. by-uuid ``get_table`` — the CHA-473 identity arm, for reference;
  4. by-name ``get_table`` over an *uncovered* (hot) system table — a
     fallback-latency reference.

``setup_performance_schema`` already drives ``__penca_system__.{tables,
schemas}`` cold (``_drive_system_tables_cold``), so its context is the
covered fixture; the uncovered fixture is a sibling catalog left hot.

**Interpreting the numbers.** The ~1.37 ms this change removes is a
*server-internal* DataFusion plan+exec slice; at end-to-end RPC
granularity in the debug perf image, per-resolve latency is dominated by
the RPC round-trip, the identifier-snapshot resolution round-trips, and
(for ``get_table``) arrow_schema deserialization, so the covered fast
path and the fallback sit within measurement noise of each other. This
suite is therefore a **latency baseline + no-regression guard**, not a
visual A/B of the DataFusion delta. The elimination of the DataFusion
plan is proven *qualitatively* by the CHA-484 integration test
(``integration_metadata_name_fastpath_test``): the covered by-name
resolve emits ``direct_point_read=true`` and no snapshot-scan span — the
CHA-473 evidence shape. Arm 4 is a hot-tier resolve, a *different* code
path, not a clean isolation of the DataFusion delta (the pre-CHA-484
cold+DataFusion path no longer exists to measure against).

No Postgres baseline: a metadata identifier resolve has no meaningful
single-statement PG equivalent, so these rows report Penca wall time
only (``postgres_baseline_seconds=None``). Run via
``just perf-test grpc/metadata_test.py``.
"""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from ..performance_helpers import (
    PERF_SCHEMA,
    PerfResult,
    make_client,
    setup_performance_schema,
)

# A metadata resolve is sub-millisecond of real work dominated by RPC
# overhead, so average the latency over many repetitions (matches the OLTP
# point-read arms).
_RESOLVE_REPS = 100


class _MetadataResolveSetup:
    """A snapshot-covered context (fast path) plus a sibling uncovered
    catalog (DataFusion fallback), sharing one client."""

    def __init__(self, client, covered: dict[str, str], uncovered: dict[str, str]):
        self.client = client
        self.covered = covered
        self.uncovered = uncovered


@pytest.fixture(scope="class")
def metadata_resolve_setup() -> _MetadataResolveSetup:
    client = make_client()
    # setup_performance_schema drives __penca_system__.{tables,schemas} cold,
    # so its context resolves by name via the direct seek.
    covered = setup_performance_schema(client)

    # Sibling catalog left hot: its system tables are never persisted/
    # snapshotted, so a by-name resolve there takes the merge fallback.
    uncovered_catalog_name = f"perf_uncov_{uuid4().hex[:8]}"
    uncovered_catalog_uuid, uncovered_branch_uuid = client.create_catalog(
        uncovered_catalog_name, "owner"
    )
    uncovered_schema_uuid = client.create_schema(
        "perf_uncov_schema",
        catalog_uuid=uncovered_catalog_uuid,
        author="perf-test",
        comment="perf uncovered schema",
    )
    uncovered_table_uuid = client.create_table(
        "perf_uncov_table",
        PERF_SCHEMA,
        primary_keys=["id"],
        catalog_uuid=uncovered_catalog_uuid,
        schema_uuid=uncovered_schema_uuid,
        author="perf-test",
        comment="perf uncovered table",
    )
    uncovered = {
        "catalog_uuid": uncovered_catalog_uuid,
        "main_branch_uuid": uncovered_branch_uuid,
        "schema_uuid": uncovered_schema_uuid,
        "table_uuid": uncovered_table_uuid,
        "schema_name": "perf_uncov_schema",
        "table_name": "perf_uncov_table",
    }
    return _MetadataResolveSetup(client, covered, uncovered)


class TestMetadataResolvePerformance:
    """Single-client by-name metadata-resolve latency over the gRPC API."""

    def test_by_name_get_table_covered(self, metadata_resolve_setup, perf_recorder):
        """by-name get_table over a snapshot-covered system table (fast path)."""
        client = metadata_resolve_setup.client
        covered = metadata_resolve_setup.covered

        start = time.perf_counter()
        for _ in range(_RESOLVE_REPS):
            info = client.get_table(
                table_name=covered["table_name"],
                schema_uuid=covered["schema_uuid"],
                catalog_uuid=covered["catalog_uuid"],
                branch_uuid=covered["main_branch_uuid"],
            )

        elapsed = time.perf_counter() - start

        assert info.table_uuid == covered["table_uuid"]

        perf_recorder.record(
            PerfResult(
                "metadata_by_name_get_table_covered",
                "all_cold_snapshotted",
                _RESOLVE_REPS,
                elapsed,
                None,
                result_rows=1,
                operations=_RESOLVE_REPS,
                unit="resolve",
            )
        )

    def test_by_name_get_schema_covered(self, metadata_resolve_setup, perf_recorder):
        """by-name get_schema over a snapshot-covered system table (fast path)."""
        client = metadata_resolve_setup.client
        covered = metadata_resolve_setup.covered

        start = time.perf_counter()
        for _ in range(_RESOLVE_REPS):
            info = client.get_schema(
                schema_name=covered["schema_name"],
                catalog_uuid=covered["catalog_uuid"],
                branch_uuid=covered["main_branch_uuid"],
            )

        elapsed = time.perf_counter() - start

        assert info.schema_uuid == covered["schema_uuid"]

        perf_recorder.record(
            PerfResult(
                "metadata_by_name_get_schema_covered",
                "all_cold_snapshotted",
                _RESOLVE_REPS,
                elapsed,
                None,
                result_rows=1,
                operations=_RESOLVE_REPS,
                unit="resolve",
            )
        )

    def test_by_uuid_get_table_covered(self, metadata_resolve_setup, perf_recorder):
        """by-uuid get_table (CHA-473 identity seek) — reference arm."""
        client = metadata_resolve_setup.client
        covered = metadata_resolve_setup.covered

        start = time.perf_counter()
        for _ in range(_RESOLVE_REPS):
            info = client.get_table(
                table_uuid=covered["table_uuid"],
                schema_uuid=covered["schema_uuid"],
                catalog_uuid=covered["catalog_uuid"],
                branch_uuid=covered["main_branch_uuid"],
            )

        elapsed = time.perf_counter() - start

        assert info.table_uuid == covered["table_uuid"]

        perf_recorder.record(
            PerfResult(
                "metadata_by_uuid_get_table_covered",
                "all_cold_snapshotted",
                _RESOLVE_REPS,
                elapsed,
                None,
                result_rows=1,
                operations=_RESOLVE_REPS,
                unit="resolve",
            )
        )

    def test_by_name_get_table_uncovered(self, metadata_resolve_setup, perf_recorder):
        """by-name get_table over an uncovered (hot) system table — a
        fallback-latency reference (a different tier, not the pre-CHA-484
        cold+DataFusion path; see the module docstring)."""
        client = metadata_resolve_setup.client
        uncovered = metadata_resolve_setup.uncovered

        start = time.perf_counter()
        for _ in range(_RESOLVE_REPS):
            info = client.get_table(
                table_name=uncovered["table_name"],
                schema_uuid=uncovered["schema_uuid"],
                catalog_uuid=uncovered["catalog_uuid"],
                branch_uuid=uncovered["main_branch_uuid"],
            )

        elapsed = time.perf_counter() - start

        assert info.table_uuid == uncovered["table_uuid"]

        perf_recorder.record(
            PerfResult(
                "metadata_by_name_get_table_uncovered",
                "all_hot",
                _RESOLVE_REPS,
                elapsed,
                None,
                result_rows=1,
                operations=_RESOLVE_REPS,
                unit="resolve",
            )
        )
