"""Fixtures for the SQL-API (Flight SQL) operational perf suite.

Lives under ``tests/performance/sql/`` and inherits the parent
``tests/performance/conftest.py`` (host-memory guard, the ``perf_recorder``
sink, etc.) — pytest applies conftests hierarchically, so nothing here
re-declares those.

The gRPC OLTP suite (``tests/performance/grpc/``) drives the native
``ReadData`` / ``WriteData`` RPCs; this suite drives the *same* point ops as
single autocommit Flight SQL statements over the harness's ADBC client, so the
delta between the two isolates the SQL-layer overhead (parse → plan → ADBC
prepared-statement wire actions → metadata resolve). See ``oltp.md`` and
CHA-504.
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

# SQL OLTP point reads are measured across the same latency-relevant tiers the
# gRPC OLTP suite names (all_hot / all_cold_snapshotted / hot_and_cold_mixed).
# The gRPC suite additionally sweeps ``realistic_timeseries``; CHA-504 scopes the
# SQL pass to these three so the op-for-op comparison stays 1:1 on the tiers that
# matter for the SQL-overhead question.
SQL_OLTP_READ_STATES = [
    pytest.param(state, id=state.value)
    for state in (
        SystemState.ALL_HOT,
        SystemState.ALL_COLD_SNAPSHOTTED,
        SystemState.HOT_AND_COLD_MIXED,
    )
]


@dataclasses.dataclass
class SqlOltpReadSetup:
    """Pre-built tiered state for the SQL OLTP point-read test."""

    client: object
    context: dict[str, str]
    system_state: SystemState


@dataclasses.dataclass
class SqlWriteSetup:
    """Table state for a single-statement SQL write op (insert / update / delete).

    Writes always land in hot storage regardless of any existing cold data, so
    the write ops are not parametrized over tiers; ``system_state`` is fixed to
    ``hot`` in the recorded result.
    """

    client: object
    context: dict[str, str]


@pytest.fixture(scope="class", params=SQL_OLTP_READ_STATES)
def sql_oltp_read_setup(request) -> SqlOltpReadSetup:
    """Client + schema + table driven to the target tier for SQL point reads."""
    system_state: SystemState = request.param
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, system_state, ROW_COUNT)
    return SqlOltpReadSetup(
        client=client,
        context=context,
        system_state=system_state,
    )


@pytest.fixture(scope="class")
def sql_insert_setup() -> SqlWriteSetup:
    """Client + schema + a fresh EMPTY table for single-row SQL inserts.

    Mirrors the gRPC suite's ``oltp_insert_setup``: writes land hot, so the
    insert workload starts from an empty table and is not tier-parametrized.
    """
    client = make_client()
    context = setup_performance_schema(client)
    return SqlWriteSetup(client=client, context=context)


@pytest.fixture(scope="class")
def sql_update_setup() -> SqlWriteSetup:
    """Client + schema + a table seeded ``all_hot`` with ``ROW_COUNT`` rows.

    The point ``UPDATE`` targets ids ``0..N-1`` of the seeded rows, so the rows
    must exist. Its own fixture instance (separate from ``sql_delete_setup``) so
    the delete workload's row removals never perturb the update timing.
    """
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, SystemState.ALL_HOT, ROW_COUNT)
    return SqlWriteSetup(client=client, context=context)


@pytest.fixture(scope="class")
def sql_delete_setup() -> SqlWriteSetup:
    """Client + schema + a table seeded ``all_hot`` with ``ROW_COUNT`` rows.

    Separate seeded table from ``sql_update_setup`` so each write op measures
    against an untouched hot table of the same shape.
    """
    client = make_client()
    context = setup_performance_schema(client)
    prepare_system_state(client, context, SystemState.ALL_HOT, ROW_COUNT)
    return SqlWriteSetup(client=client, context=context)
