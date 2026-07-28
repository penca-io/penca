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

# The primitives that actually touch process-global state. Everything else is
# derived: `_side_channel_helpers` also treats any function in
# integration_helpers.py that *reaches* one of these as a side-channel helper,
# so adding a wrapper there cannot quietly open a hole in the check below.
ROOT_SIDE_CHANNEL_HELPERS = frozenset(
    {
        "container_log",
        "poll_log_for",
        "reset_pg_stat",
        "count_stmts_referencing",
        # Called by every scraper and nothing else, so it is the clearest tell
        # even when a test reaches the counters by raw SQL.
        "ensure_pg_stat_statements",
    }
)

# Reaching the counters without going through a helper at all — e.g.
# ``SELECT ... FROM pg_stat_statements`` inline, as
# integration_cha368_filter_engine_test.py does. Matched against string
# literals so those functions are roots in their own right.
#
# SQL text only. There is no log-side equivalent: container_log shells
# ``["docker", "logs", container]`` as separate list elements, so no single
# string constant to match — reading that channel means calling the helper,
# which the call graph already covers.
SIDE_CHANNEL_SQL_LITERALS = ("pg_stat_statements",)


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


def _body_sets_serial_pytestmark(body: list[ast.stmt]) -> bool:
    """True when ``body`` assigns ``pytestmark`` to (or including) ``serial``.

    Applies to a module body and to a class body — a class-level
    ``pytestmark`` is the natural way to mark a class without decorating it,
    and missing it would report a spurious failure.
    """
    for node in body:
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


def _touches_side_channel_literal(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            if any(lit in child.value for lit in SIDE_CHANNEL_SQL_LITERALS):
                return True

    return False


def _coupled_functions(tree: ast.Module, roots: frozenset[str]) -> set[str]:
    """Names of functions in ``tree`` that reach ``roots``, transitively.

    Fixed-point rather than one pass: a test may call a helper that calls a
    helper.

    Names are NOT unique within a module — the Flight SQL suite defines
    ``_exec_query`` in three classes, and the write suite defines ``_setup``
    twice. So a name maps to every definition sharing it and their callees are
    unioned: an over-broad match costs a spurious mark, while keying on the
    last definition would silently drop a scraping sibling out of the result,
    which is the direction that fails quiet.
    """
    definitions: dict[str, list[ast.AST]] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            definitions.setdefault(node.name, []).append(node)

    coupled = {
        name
        for name, nodes in definitions.items()
        if any(_touches_side_channel_literal(n) for n in nodes)
    }
    while True:
        newly_found = {
            name
            for name, nodes in definitions.items()
            if name not in coupled
            and any(_called_names(n) & (roots | coupled) for n in nodes)
        }
        if not newly_found:
            return coupled

        coupled |= newly_found


def _side_channel_helpers() -> frozenset[str]:
    """The roots plus every ``integration_helpers`` wrapper that reaches one.

    Without this the check has a blind spot in the direction most likely to be
    exercised: a shared wrapper belongs in ``integration_helpers.py``, and one
    added there would be invisible to a roots-only scan, so its callers would
    look unmarked-but-innocent.

    Returns exactly the roots today, since ``poll_log_for`` — the one wrapper
    that reaches another — is itself a root.
    """
    tree = ast.parse((INTEGRATION / "integration_helpers.py").read_text())
    return ROOT_SIDE_CHANNEL_HELPERS | _coupled_functions(
        tree, ROOT_SIDE_CHANNEL_HELPERS
    )


def _enclosing_class(tree: ast.Module, target: ast.AST) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and any(m is target for m in node.body):
            return node

    return None


def test_every_side_channel_test_is_marked_serial():
    helpers = _side_channel_helpers()
    unmarked: list[str] = []
    scanned = 0

    # Glob what the recipe's phases actually select, so a module named outside
    # the `_test` convention is scanned too. integration_helpers.py is included
    # rather than skipped: the phases collect it, so a `test_*` added there
    # would run, and its non-test helpers simply never match below.
    for path in sorted(INTEGRATION.glob("integration_*.py")):
        tree = ast.parse(path.read_text())
        coupled = _coupled_functions(tree, helpers)
        scanned += sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name.startswith("test")
            and node.name in coupled
        )

        # Counted before this skip, not after: a module-level mark satisfies
        # the invariant but its tests are still evidence the scan is working.
        if _body_sets_serial_pytestmark(tree.body):
            continue

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            if not node.name.startswith("test") or node.name not in coupled:
                continue

            enclosing = _enclosing_class(tree, node)
            if _has_serial_mark(node) or (
                enclosing is not None
                and (
                    _has_serial_mark(enclosing)
                    or _body_sets_serial_pytestmark(enclosing.body)
                )
            ):
                continue

            unmarked.append(f"{path.name}::{node.name}")

    assert not unmarked, (
        "these tests reach a process-global side channel but are not marked "
        "@pytest.mark.serial, so they would run in the -n auto phase and race "
        f"a concurrent worker: {unmarked}"
    )

    # A pass means nothing if the scan found nothing to check. Renaming a
    # helper out of the root set, or any regression that empties the call-graph
    # walk, would otherwise leave this silently green — the one way a guard
    # like this fails without anyone noticing. The floor is well under the
    # current count so ordinary edits don't trip it.
    assert scanned >= 30, (
        f"only {scanned} side-channel tests found; the check is not looking at "
        "anything. Did a helper in ROOT_SIDE_CHANNEL_HELPERS get renamed?"
    )
