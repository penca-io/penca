"""Static wiring checks for the perf-results framework (CHA-419).

Structural source-file assertions (no Docker, no penca services) that the
build is wired for the SQLite-backed perf flow: the data dir is gitignored, the
``perf-test`` recipe emits JSONL + ingests it + defaults to penca=info with a
``--trace`` opt-in, a ``perf-trends`` recipe exists, matplotlib is a dev
dependency, and the perf test files (including the gRPC suites) route results
through ``perf_recorder`` rather than the old class-var list + markdown
``teardown_class``.

These are grep-style guarantees over committed files (per
feedback_dont_test_upstream_libs: structural one-time checks, not behavior).
The Justfile/pyproject assertions are scoped to the relevant recipe body /
dependency block (not whole-file substring) so an unrelated comment or a
runtime-deps placement can't satisfy them. Runs under ``just static-test
perf_framework_wiring`` and ``just check``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).parents[2]


def _read(rel: str) -> str:
    return (REPO / rel).read_text()


def _recipe_body(justfile_text: str, name: str) -> str:
    """Return the indented body of the ``name`` recipe (excluding the header).

    A recipe is ``name [params]:`` at column 0 followed by indented lines; the
    body ends at the next column-0 non-blank line. Scoping to the body keeps the
    guards from being satisfied by a match in a comment or a different recipe.

    The header match tolerates a trailing dependency list (``name: dep``) by not
    anchoring on the colon — so a recipe given a dependency is still recognized.
    """
    header = re.compile(r"^" + re.escape(name) + r"\b[^:]*:")
    lines = justfile_text.splitlines()
    body: list[str] = []
    in_recipe = False
    for line in lines:
        if not in_recipe:
            if header.match(line):
                in_recipe = True

            continue

        if line.strip() == "":
            body.append(line)
            continue

        if not line[0].isspace():
            break

        body.append(line)

    return "\n".join(body)


def _toml_section(toml_text: str, header: str) -> str:
    """Return the lines of the ``[header]`` table up to the next ``[`` section."""
    marker = f"[{header}]"
    start = toml_text.find(marker)
    assert start != -1, f"missing {marker} in pyproject.toml"
    rest = toml_text[start + len(marker) :]
    end = rest.find("\n[")
    return rest if end == -1 else rest[:end]


def test_perf_data_dir_is_gitignored():
    assert ".perf/" in _read(".gitignore")


def test_perf_test_recipe_jsonl_always_record_gates_sqlite_and_reports():
    body = _recipe_body(_read("Justfile"), "perf-test")
    # JSONL capture is unconditional — the single always-on capture format.
    assert "PERF_RESULTS_JSON" in body
    # SQLite persistence is opt-in behind --record: the ingest call exists but
    # is gated by the record flag (no longer unconditional).
    assert "--record" in body
    assert "record_run" in body  # the flag var the ingest is wrapped in
    assert "scripts/perf/results_to_sqlite.py" in body
    # A static HTML report is rendered at the end of every run (unconditional).
    assert "scripts/perf/render_report.py" in body


def test_perf_dashboard_recipe_forwards_run_id():
    body = _recipe_body(_read("Justfile"), "perf-dashboard")
    # The dashboard recipe forwards an optional run_id to the streamlit script
    # as the --run_id flag, so an incidental token can't satisfy the gate.
    assert "--run_id" in body


def test_perf_test_recipe_defaults_to_info_with_trace_opt_in():
    body = _recipe_body(_read("Justfile"), "perf-test")
    # Default verbosity is representative (penca=info) so trace overhead doesn't
    # perturb the recorded latency...
    assert "penca=info" in body
    # ...and trace-level spans + span timing are available behind --trace.
    assert "--trace" in body
    assert "penca=trace" in body
    assert "PENCA_SPAN_TIMING" in body


def test_perf_trends_recipe_invokes_trends_script():
    body = _recipe_body(_read("Justfile"), "perf-trends")
    assert "scripts/perf/trends.py" in body


def test_matplotlib_is_a_dev_dependency():
    dev = _toml_section(_read("pyproject.toml"), "dependency-groups")
    assert "matplotlib" in dev


def test_perf_tests_route_through_recorder_not_teardown_markdown():
    rel_paths = [
        f"tests/performance/performance_{service}_test.py"
        for service in ("write", "query", "lifecycle", "pgbench")
    ] + [
        "tests/performance/grpc/oltp_test.py",
        "tests/performance/grpc/olap_test.py",
    ]
    for rel in rel_paths:
        text = _read(rel)
        assert "perf_recorder" in text, f"{rel}: not migrated to perf_recorder"
        assert "teardown_class" not in text, (
            f"{rel}: still defines a markdown teardown_class"
        )
