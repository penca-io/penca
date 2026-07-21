"""Shared fixtures for performance tests."""

from __future__ import annotations

import dataclasses
import os
import warnings
from pathlib import Path

import pytest

from .perf_record import (
    PerfRecorder,
    PerfSink,
    derive_file,
    derive_test,
    derive_type,
    extract_params,
    run_context,
)
from .performance_helpers import (
    ROW_COUNT,
    SystemState,
    SystemStateInfo,
    make_client,
    prepare_system_state,
    setup_performance_schema,
)

PERF_DIR = Path(__file__).parent

MIN_AVAILABLE_MEMORY_GB = 6
MAX_SWAP_USED_PCT = 80


def _check_host_memory():
    """Fail fast if the host is low on memory or swap is saturated.

    Docker Desktop's QEMU VM loses network connectivity when the host
    is memory-constrained, causing Lance reads to crash mid-test.
    """
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split()
                meminfo[parts[0].rstrip(":")] = int(parts[1])  # kB

        available_gb = meminfo.get("MemAvailable", 0) / (1024**2)
        swap_total = meminfo.get("SwapTotal", 0)
        swap_free = meminfo.get("SwapFree", 0)
        swap_pct = ((swap_total - swap_free) / swap_total * 100) if swap_total else 0
    except (OSError, KeyError, ZeroDivisionError):
        return

    if swap_pct > MAX_SWAP_USED_PCT:
        pytest.exit(
            f"Swap {swap_pct:.0f}% used (>{MAX_SWAP_USED_PCT}%)."
            f" Available memory: {available_gb:.1f} GB."
            " Free memory before running perf tests — Docker will"
            " lose connectivity under swap pressure.",
            returncode=1,
        )

    if available_gb < MIN_AVAILABLE_MEMORY_GB:
        warnings.warn(
            f"Low memory: {available_gb:.1f} GB available"
            f" (need ~{MIN_AVAILABLE_MEMORY_GB} GB),"
            f" swap {swap_pct:.0f}% used."
            " Docker may crash under load."
            " Close heavy apps or reduce Docker Desktop memory.",
            stacklevel=2,
        )


SYSTEM_STATES = [pytest.param(state, id=state.value) for state in SystemState]


@pytest.fixture(scope="session", autouse=True)
def _check_host_memory_fixture():
    """Warn once at session start if the host is low on memory."""
    _check_host_memory()


@dataclasses.dataclass
class QuerySetup:
    """Pre-built system state for read-only query tests."""

    client: object
    context: dict[str, str]
    state_info: SystemStateInfo
    system_state: SystemState


@pytest.fixture(scope="class", params=SYSTEM_STATES)
def query_setup(request) -> QuerySetup:
    """Create client + schema + table driven to the target system state.

    Class-scoped so all query tests within a class share the same
    pre-built state (all query tests are read-only).
    """
    system_state: SystemState = request.param
    client = make_client()
    context = setup_performance_schema(client)
    state_info = prepare_system_state(client, context, system_state, ROW_COUNT)
    return QuerySetup(
        client=client,
        context=context,
        state_info=state_info,
        system_state=system_state,
    )


@pytest.fixture(scope="session", autouse=True)
def _perf_sink():
    """Session-wide perf-result sink.

    Captures the run context once and records every measurement to the JSONL
    file (always-on capture). The per-run HTML report + SQLite ingest are
    driven by the ``perf-test`` recipe over that JSONL, so the session teardown
    only prints a one-line pointer.
    """
    sink = PerfSink(os.environ.get("PERF_RESULTS_JSON"), run_context())
    yield sink
    print(
        f"\n[perf] {sink.count} measurement(s) recorded to {sink.json_path};"
        " HTML report at .perf/report-<run_id>.html"
        " — explore history with 'just perf-trends' / 'just perf-dashboard'"
    )


@pytest.fixture
def perf_recorder(request, _perf_sink) -> PerfRecorder:
    """Per-test recorder tagged with this test's type/file/test + params.

    The tags are derived from the pytest node: the file's directory (under
    tests/performance/) is the type, the filename the file, the test function
    the test; the test's parametrization becomes the row's params.
    """
    node = request.node
    file_path = Path(node.path)
    rel_dir = (
        ""
        if file_path.parent == PERF_DIR
        else str(file_path.parent.relative_to(PERF_DIR))
    )
    type_name = derive_type(rel_dir)
    file_name = derive_file(file_path.name)
    test_name = derive_test(node.originalname or node.name)
    callspec = getattr(node, "callspec", None)
    params = extract_params(callspec.params) if callspec is not None else {}
    return PerfRecorder(type_name, file_name, test_name, params, _perf_sink)
