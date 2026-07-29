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

This checks reason (a) of the ``serial`` marker only — the side-channel one.
Reason (b) marks (contention: a test parking or saturating servicer PG
connections) are hand-placed and outlive CHA-519, so deleting this file must
not un-mark them. See the marker description in ``pyproject.toml``.

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


def _is_serial_mark(node: ast.AST) -> bool:
    """True only for the ``pytest.mark.serial`` dotted path.

    Matching any attribute named ``serial`` would accept an unrelated mention
    inside another decorator's arguments — ``@pytest.mark.parametrize("mode",
    [Mode.serial])`` on a scraping test would satisfy the check and mask a
    genuinely missing mark. This file's whole job is to be the thing that
    cannot be fooled, so it requires the ``.mark.`` parent too.
    """
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "serial"
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "mark"
    )


def _has_serial_mark(node: ast.AST) -> bool:
    for decorator in getattr(node, "decorator_list", []):
        if any(_is_serial_mark(n) for n in ast.walk(decorator)):
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

        if any(_is_serial_mark(n) for n in ast.walk(node.value)):
            return True

    return False


def _touches_side_channel_literal(node: ast.AST) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            if any(lit in child.value for lit in SIDE_CHANNEL_SQL_LITERALS):
                return True

    return False


def _coupled_functions(trees: list[ast.Module], roots: frozenset[str]) -> set[str]:
    """Names of functions across ``trees`` that reach ``roots``, transitively.

    Fixed-point rather than one pass: a test may call a helper that calls a
    helper.

    Takes every module at once rather than one at a time because imports are
    not modelled at all: a callee resolves purely by name, so a helper defined
    in a sibling test module is invisible to a per-module walk. These modules
    do import helpers from each other, so that gap is reachable — no current
    test depends on it (today's cross-module callers also scrape directly and
    are flagged on their own), but the resolution should not rest on that.
    Note the same-name unioning below therefore spans modules too.

    Names are NOT unique — the Flight SQL suite defines ``_exec_query`` in
    three classes, and the write suite defines ``_setup`` twice — so a name
    maps to every definition sharing it and their callees are unioned. An
    over-broad match costs a spurious mark; keying on the last definition
    would silently drop a scraping sibling, which is the direction that hides.
    """
    definitions: dict[str, list[ast.AST]] = {}
    for tree in trees:
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
        [tree], ROOT_SIDE_CHANNEL_HELPERS
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
    paths = sorted(INTEGRATION.glob("integration_*.py"))
    trees = {path: ast.parse(path.read_text()) for path in paths}
    # One closure over every module, so a helper imported from a sibling test
    # module resolves. Per-module closures left those callees unresolved.
    coupled = _coupled_functions(list(trees.values()), helpers)

    for path, tree in trees.items():
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

    # A pass means nothing if the scan found nothing to check, and that is the
    # one way a guard like this fails without anyone noticing. Two assertions,
    # because neither subsumes the other.
    #
    # Exact: every root must still exist. A rename is the likely cause of a
    # silently-narrowed scan, and it is cheap to detect precisely rather than
    # to infer from a count.
    defined = {
        node.name
        for node in ast.walk(
            ast.parse((INTEGRATION / "integration_helpers.py").read_text())
        )
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    missing = ROOT_SIDE_CHANNEL_HELPERS - defined
    assert not missing, (
        f"{sorted(missing)} no longer exist in integration_helpers.py, so the "
        "scan below silently stopped covering them — update "
        "ROOT_SIDE_CHANNEL_HELPERS, or delete this file if CHA-519 removed the "
        "scrapes"
    )

    # Coarse: catches a scan that has stopped seeing most of the suite,
    # whatever the cause. Both figures re-derived under the global closure —
    # 47 today, and dropping `container_log` alone yields exactly 30 — so the
    # floor sits close enough to bite while leaving room for ordinary edits.
    # It does NOT catch subtler walk regressions that leave the count intact;
    # the existence check above is the precise instrument.
    assert scanned >= 40, (
        f"only {scanned} side-channel tests found; the check has stopped "
        "looking at most of the suite"
    )
