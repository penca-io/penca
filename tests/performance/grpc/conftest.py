"""Fixtures for the gRPC-API operational perf suites (OLTP / OLAP).

Lives under ``tests/performance/grpc/`` and inherits the parent
``tests/performance/conftest.py`` (host-memory guard, etc.) — pytest
applies conftests hierarchically, so nothing here re-declares those.
"""

from __future__ import annotations

import dataclasses

import pytest

from ..performance_helpers import (
    ROW_COUNT,
    SystemState,
    make_client,
    prepare_system_state,
    setup_performance_schema,
)

# OLTP point reads are measured across the latency-relevant tiers: all hot
# (merge-on-read fast path), all cold-snapshotted (the steady-state production
# tier), a hot+cold mixture (includes the cold-*persist* tier), and the
# realistic 95% cold-snapshotted + 5% hot tail (no persist tier) — the closest
# shape to a real steady-state workload.
OLTP_READ_STATES = [
    pytest.param(state, id=state.value)
    for state in (
        SystemState.ALL_HOT,
        SystemState.ALL_COLD_SNAPSHOTTED,
        SystemState.HOT_AND_COLD_MIXED,
        SystemState.REALISTIC_TIMESERIES,
    )
]


@dataclasses.dataclass
class OltpReadSetup:
    """Pre-built tiered state for the OLTP point-read test."""

    client: object
    context: dict[str, str]
    system_state: SystemState


@dataclasses.dataclass
class OltpInsertSetup:
    """Fresh empty writable table for the OLTP single-row insert test."""

    client: object
    context: dict[str, str]


@dataclasses.dataclass
class OltpMutateSetup:
    """Table pre-seeded all-hot with ``ROW_COUNT`` rows for point update/delete.

    The point update / delete ops target ids ``0..N-1`` of the seeded rows, so
    the rows must exist. Update and delete get separate fixture instances so the
    delete workload's row removals never perturb the update timing.
    """

    client: object
    context: dict[str, str]


@pytest.fixture(scope="class", params=OLTP_READ_STATES)
def oltp_read_setup(request) -> OltpReadSetup:
    """Client + schema + table driven to the target tier for point reads."""
    system_state: SystemState = request.param
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, system_state, ROW_COUNT)
    return OltpReadSetup(
        client=client,
        context=context,
        system_state=system_state,
    )


@pytest.fixture(scope="class")
def oltp_insert_setup() -> OltpInsertSetup:
    """Client + schema + a fresh empty table for single-row inserts.

    Writes always land in hot storage regardless of any existing cold
    data, so the insert workload is not parametrized over tiers.
    """
    client = make_client()
    context = setup_performance_schema(client)
    return OltpInsertSetup(client=client, context=context)


@pytest.fixture(scope="class")
def oltp_update_setup() -> OltpMutateSetup:
    """Client + schema + a table seeded all-hot with ``ROW_COUNT`` rows.

    The point update upserts existing ids, so the rows must pre-exist. Its own
    fixture instance (separate from ``oltp_delete_setup``) keeps the two write
    workloads independent.
    """
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, SystemState.ALL_HOT, ROW_COUNT)
    return OltpMutateSetup(client=client, context=context)


@pytest.fixture(scope="class")
def oltp_delete_setup() -> OltpMutateSetup:
    """Client + schema + a table seeded all-hot with ``ROW_COUNT`` rows.

    Separate seeded table from ``oltp_update_setup`` so each write op measures
    against an untouched hot table of the same shape.
    """
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, SystemState.ALL_HOT, ROW_COUNT)
    return OltpMutateSetup(client=client, context=context)


# OLAP scans run on the steady-state cold-snapshotted tier at two scales
# so the 100k -> 1M throughput crossover is visible in one table.
OLAP_SCALES = [
    pytest.param(100_000, id="100k"),
    pytest.param(1_000_000, id="1m"),
]


@dataclasses.dataclass
class OlapSetup:
    """Pre-built cold-snapshotted state at a given scale for OLAP scans."""

    client: object
    context: dict[str, str]
    scale: int


@pytest.fixture(scope="class", params=OLAP_SCALES)
def olap_setup(request) -> OlapSetup:
    """Client + schema + table driven to cold-snapshotted at ``scale`` rows."""
    scale: int = request.param
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, SystemState.ALL_COLD_SNAPSHOTTED, scale)
    return OlapSetup(
        client=client,
        context=context,
        scale=scale,
    )
