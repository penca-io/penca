"""Pin the side-channel-helper -> ``@pytest.mark.serial`` correspondence (CHA-518).

``just integration-test`` runs the suite in two disjoint phases: everything
``-m "not serial"`` under ``-n auto``, then ``-m serial`` alone. A test that
reads a process-global side channel — a container's stdout log window, or the
instance-global ``pg_stat_statements`` counters — is only correct in the second
phase, because a concurrent worker driving the same stack pollutes both.

Nothing in pytest ties the two together: the marker is what puts a test in the
serial phase, and the helper call is what makes it need to be there. Miss the
mark and the test silently joins the parallel phase, where it fails
intermittently on a polluted window rather than with a clear error.

So enforce it statically. The check walks the call graph rather than grepping
test bodies, because two files reach a scraper only through a module-level
helper (``_await_statement_cache_event`` in the Flight SQL suite,
``_poll_for_read_data_ids_close`` in the point-read suite) — a body grep marks
neither.

Lives in ``tests/static`` because it reads source and needs no Docker, so it
runs in the Python-unit CI job, which — unlike the integration job — runs on
every PR. CHA-519 deletes the side channels; this file goes with them.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).parents[2]
INTEGRATION = REPO / "tests" / "integration"

# The process-global readers. Defined in integration_helpers.py; calling any of
# them from a test obligates the marker.
SIDE_CHANNEL_HELPERS = frozenset(
    {
        "container_log",
        "poll_log_for",
        "reset_pg_stat",
        "count_stmts_referencing",
    }
)


def _called_names(node: ast.AST) -> set[str]:
    """Every callee name reachable in ``node``, bare or attribute."""
    names = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue

        func = child.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)

    return names


def _has_serial_mark(node: ast.AST) -> bool:
    for decorator in getattr(node, "decorator_list", []):
        for attr in ast.walk(decorator):
            if isinstance(attr, ast.Attribute) and attr.attr == "serial":
                return True

    return False


def _module_is_serial(tree: ast.Module) -> bool:
    """True when the module sets ``pytestmark`` to (or including) ``serial``."""
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue

        if not any(
            isinstance(t, ast.Name) and t.id == "pytestmark" for t in node.targets
        ):
            continue

        for attr in ast.walk(node.value):
            if isinstance(attr, ast.Attribute) and attr.attr == "serial":
                return True

    return False


def _coupled_functions(tree: ast.Module) -> set[str]:
    """Names of functions that reach a side-channel helper, transitively.

    Fixed-point rather than one pass: a test may call a helper that calls a
    helper. Names are module-unique enough in this suite that a flat map is
    sufficient and keeps the check readable.
    """
    functions: dict[str, ast.AST] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions[node.name] = node

    coupled: set[str] = set()
    while True:
        newly_found = {
            name
            for name, node in functions.items()
            if name not in coupled
            and _called_names(node) & (SIDE_CHANNEL_HELPERS | coupled)
        }
        if not newly_found:
            return coupled

        coupled |= newly_found


def _enclosing_class(tree: ast.Module, target: ast.AST) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and any(m is target for m in node.body):
            return node

    return None


def test_every_side_channel_test_is_marked_serial():
    unmarked: list[str] = []

    for path in sorted(INTEGRATION.glob("integration_*_test.py")):
        tree = ast.parse(path.read_text())
        if _module_is_serial(tree):
            continue

        coupled = _coupled_functions(tree)
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            if not node.name.startswith("test") or node.name not in coupled:
                continue

            enclosing = _enclosing_class(tree, node)
            if _has_serial_mark(node) or (
                enclosing is not None and _has_serial_mark(enclosing)
            ):
                continue

            unmarked.append(f"{path.name}::{node.name}")

    assert not unmarked, (
        "these tests reach a process-global side channel but are not marked "
        "@pytest.mark.serial, so they would run in the -n auto phase and race "
        f"a concurrent worker: {unmarked}"
    )


def test_side_channel_helpers_still_exist():
    """Guard the guard: a rename would silently empty the check above.

    Without this, CHA-519 renaming ``container_log`` turns the correspondence
    test into a vacuous pass rather than a failure that says "come update me".
    """
    helpers_source = (INTEGRATION / "integration_helpers.py").read_text()
    tree = ast.parse(helpers_source)
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    missing = SIDE_CHANNEL_HELPERS - defined
    assert not missing, (
        f"{sorted(missing)} no longer exist in integration_helpers.py — update "
        "SIDE_CHANNEL_HELPERS, or drop this file if CHA-519 removed the scrapes"
    )
