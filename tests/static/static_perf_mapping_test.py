"""Static checks for the perf-namespace mapping helpers (CHA-419).

The perf framework maps the test-tree onto a type/file/test
hierarchy so the eventual "store perf results in Penca itself" migration is
mechanical: a subdirectory under ``tests/performance/`` is the ``type``, a test
file is the ``file``, an individual test is the ``test``. These four pure string->value
helpers live in ``tests/performance/perf_record.py`` — a module loaded by path
here (it is not on ``sys.path`` as a package) so these assertions need no
Docker, no penca_client, and no running penca services. They run under
``just static-test perf_mapping`` and ``just check``.

Per feedback_dont_test_upstream_libs / feedback_exhaustive_helper_cross_product
this pins the Penca-owned derivation only, with the cross-product spelled out.
"""

from __future__ import annotations

import enum
import importlib.util
import json
from pathlib import Path

PERF_RECORD = Path(__file__).parents[2] / "tests/performance/perf_record.py"


def _load_perf_record():
    """Load perf_record.py by path.

    Raises FileNotFoundError until the impl task lands the module — that is the
    intended red state for this red-test.
    """
    spec = importlib.util.spec_from_file_location("perf_record", PERF_RECORD)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_derive_type_defaults_and_first_component():
    mod = _load_perf_record()
    assert mod.derive_type("") == "performance"
    assert mod.derive_type("branch") == "branch"
    assert mod.derive_type("branch/sub") == "branch"


def test_derive_file_strips_prefix_and_suffix():
    mod = _load_perf_record()
    assert mod.derive_file("performance_write_test.py") == "write"
    assert mod.derive_file("performance_query_test.py") == "query"
    assert mod.derive_file("performance_lifecycle_test.py") == "lifecycle"
    assert mod.derive_file("performance_pgbench_test.py") == "pgbench"


def test_derive_test_strips_test_prefix():
    mod = _load_perf_record()
    assert mod.derive_test("test_write_into_empty_table") == "write_into_empty_table"
    assert mod.derive_test("test_oltp_single_row_writes") == "oltp_single_row_writes"


class _Mode(enum.Enum):
    ALL_HOT = "all_hot"


def test_extract_params_coerces_to_json_safe_values():
    mod = _load_perf_record()
    # Primitives pass through unchanged.
    assert mod.extract_params({"batch_count": 4}) == {"batch_count": 4}
    assert mod.extract_params({}) == {}
    # Enum-valued params (the query suite parametrizes on a SystemState enum)
    # must be coerced to a JSON-serializable form so params_json round-trips.
    # This is the case that distinguishes real extraction from identity: a
    # passthrough impl would leave the enum object and break json.dumps.
    coerced = mod.extract_params({"mode": _Mode.ALL_HOT, "batch_count": 2})
    assert coerced == {"mode": "all_hot", "batch_count": 2}
    json.dumps(coerced)  # must not raise
