"""Recording of perf measurements, tagged by a type/file/test hierarchy.

Each measurement is tagged with where it came from so the SQLite history (and
the eventual Penca-backed store) can be sliced by it:

- ``type`` — the subdirectory under ``tests/performance/`` (the suite category;
  e.g. ``grpc``), or ``performance`` for files directly under it,
- ``file`` — the test file,
- ``test`` — the individual test function,

plus the test's parametrization (``num_rows``, ``mode``, ``batch_count``, ... as
``params_json``) and the run context (branch, commit, hostname, UTC timestamp).

This module is deliberately free of ``penca_client`` / ``performance_helpers``
imports so it stays importable standalone (the static tests load it by path).
The ``derive_*`` / ``extract_params`` helpers are pure; ``run_context`` /
``PerfSink`` / ``PerfRecorder`` are the per-session recording machinery wired
into pytest via ``conftest.py``.
"""

from __future__ import annotations

import datetime
import enum
import json
import socket
import subprocess
from uuid import uuid4

# ---------------------------------------------------------------------------
# Hierarchy mapping (pure)
# ---------------------------------------------------------------------------

# Files placed directly under tests/performance/ (no subdirectory) belong to
# this default type.
_DEFAULT_TYPE = "performance"


def derive_type(rel_dir: str) -> str:
    """Map a test file's directory (relative to tests/performance/) to a type.

    The first path component is the type; a file directly under
    tests/performance/ (empty ``rel_dir``) maps to the default type.
    """
    trimmed = rel_dir.strip("/")
    if not trimmed:
        return _DEFAULT_TYPE

    return trimmed.split("/")[0]


def derive_file(filename: str) -> str:
    """Map a test filename to a file label (``performance_write_test.py`` -> ``write``)."""
    stem = filename
    if stem.endswith("_test.py"):
        stem = stem[: -len("_test.py")]

    if stem.startswith("performance_"):
        stem = stem[len("performance_") :]

    return stem


def derive_test(func_name: str) -> str:
    """Map a test function name to a test label (``test_write_into_empty_table`` -> ``write_into_empty_table``)."""
    if func_name.startswith("test_"):
        return func_name[len("test_") :]

    return func_name


def _json_safe(value):
    """Coerce a pytest param value to a JSON-serializable form.

    Enum-valued params (the query suite parametrizes on a ``SystemState`` enum)
    become their ``.value``; primitives pass through. This is what lets the
    parametrization round-trip through ``params_json``.
    """
    if isinstance(value, enum.Enum):
        return value.value

    return value


def extract_params(params: dict) -> dict:
    """Return the test's parametrization with values coerced JSON-safe."""
    return {key: _json_safe(value) for key, value in params.items()}


# ---------------------------------------------------------------------------
# Run context
# ---------------------------------------------------------------------------


def _git(args: list[str]) -> str:
    """Best-effort ``git`` value for run provenance.

    Git is an external tool and the run context is non-critical metadata — a
    perf run must not abort because the tree is detached or git is absent, so a
    missing value degrades to ``"unknown"`` rather than raising.
    """
    try:
        completed = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"

    return completed.stdout.strip()


def run_context() -> dict:
    """Capture run_id / branch / commit / host / UTC timestamp once per session.

    ``run_id`` is a fresh UUID per perf session, stamped on every measurement
    so a single run can be sliced out of the accumulated SQLite history.
    """
    return {
        "run_id": str(uuid4()),
        "branch": _git(["rev-parse", "--abbrev-ref", "HEAD"]),
        "commit_sha": _git(["rev-parse", "HEAD"]),
        "hostname": socket.gethostname(),
        "ts_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Recording machinery
# ---------------------------------------------------------------------------


class PerfSink:
    """Session-wide JSONL sink for perf measurements.

    Each recorded measurement is appended to ``json_path`` as one JSON object
    per line (when ``$PERF_RESULTS_JSON`` is set) — the single capture format
    the SQLite ingest and the per-run HTML report both read. ``count`` tracks
    how many rows were written so the session teardown can report it.
    """

    def __init__(self, json_path: str | None, run_context: dict):
        self.json_path = json_path
        self.run_context = run_context
        self.count = 0
        # Start each session from a clean file so re-runs don't pile stale rows
        # onto a fixed PERF_RESULTS_JSON path. The perf-test recipe also
        # truncates, but doing it here keeps a direct `pytest` run correct too.
        if json_path is not None:
            open(json_path, "w").close()

    def add(self, row: dict) -> None:
        self.count += 1
        if self.json_path is not None:
            with open(self.json_path, "a") as handle:
                handle.write(json.dumps(row) + "\n")


class PerfRecorder:
    """Records one test's measurements, tagged with its type/file/test + params."""

    def __init__(
        self,
        type_name: str,
        file_name: str,
        test_name: str,
        params: dict,
        sink: PerfSink,
    ):
        self.type_name = type_name
        self.file_name = file_name
        self.test_name = test_name
        self.params = params
        self.sink = sink

    def record(self, measurement) -> None:
        """Emit one measurement row (type/file/test + params + run context).

        ``rows_per_second`` is READ from the measurement (PerfResult exposes it
        as a property) rather than recomputed here, so the throughput formula
        lives in exactly one place.
        """
        row = {
            "type": self.type_name,
            "file": self.file_name,
            "test": self.test_name,
            "operation": measurement.operation,
            "system_state": measurement.system_state,
            "row_count": measurement.row_count,
            "result_rows": measurement.result_rows,
            "elapsed_seconds": measurement.elapsed_seconds,
            "postgres_baseline_seconds": measurement.postgres_baseline_seconds,
            "rows_per_second": measurement.rows_per_second,
            "operations": measurement.operations,
            "unit": measurement.unit,
            # sort_keys for a canonical, stable series identity across runs
            # (matches the ingest dedupe_key serialization).
            "params_json": json.dumps(self.params, sort_keys=True),
            **self.sink.run_context,
        }
        self.sink.add(row)
