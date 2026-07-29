"""Smoke test for ``examples/oltp_demo.py`` (CHA-527).

The demo's claim is that a primary-key seek is still a seek once the rows live in
open columnar files — it drives the table cold with persist + snapshot + purge,
then times the lookup over both the gRPC client and Flight SQL. That is two
numbers a reader is invited to trust, so what needs coverage is that both are
really measured, that the seek returned exactly the row it sought, and that the
tier transition the word "cold" depends on actually ran. Purge is the load-bearing
step there: persist alone leaves the rows queryable from hot, so without it a
"cold" read still carries a hot arm.

Deliberately a subprocess smoke test rather than an import-and-assert: the demo
is a flat ``main()`` that prints, with no seam to call into, and adding one
purely for a test would change an example whose job is to read simply. Same
shape as ``integration_audit_demo_test.py``.

Run via ``just integration-test oltp_demo``.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from .integration_helpers import (
    demo_catalog_names,
    make_client,
    reaped_demo_catalogs,
)

# The demo_ catalog prefix has more than one producer (audit_demo.py,
# oltp_demo.py) and reaped_demo_catalogs deletes every match, so two of these
# running concurrently reap each other's live catalog. They conflict only with
# EACH OTHER, not with the stack, so one xdist worker is enough — `serial`
# would hoist them onto the sequential phase that gates the whole recipe, and
# the oltp demo is expensive.
pytestmark = pytest.mark.xdist_group("demo_catalogs")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEMO_PATH = _REPO_ROOT / "examples" / "oltp_demo.py"

# Small enough to keep the suite quick, large enough that the seek is a seek
# rather than a scan of a handful of rows. The demo's own default is far larger;
# none of the assertions below depend on the scale.
_ROWS = 2_000
_REPS = 5

# The demo seeds owner_<id zero-padded to 6>, and looks up the row at the middle
# of the key range. Derived here rather than pinned so the two move together if
# _ROWS changes.
_TARGET_ID = _ROWS // 2
_TARGET_OWNER = f"owner_{_TARGET_ID:06d}"
_ROW_SECTION = "--- The row we looked up ---"
_LATENCY_SECTION = "--- Point lookup latency on cold columnar (mean per lookup) ---"

# A pandas/tabulate cell holding a millisecond figure: "| 1.23 |", "|  0.4 |".
# Lookahead on the closing pipe so adjacent cells share their delimiter —
# re.findall does not overlap, so consuming it would hide every second cell.
_MS_CELL = re.compile(r"\|\s*\d+\.?\d*\s*(?=\|)")
# The demo's lifecycle line. Kept as a cheap presence check, but it is only an
# announcement — _assert_tier_transition below is what actually pins the move.
_TIER_LINE = "Persisting, snapshotting and purging to cold columnar storage..."
# The watermarks the demo prints straight after it. A real one is a number; an
# unset one renders as a non-numeric placeholder, which on the scheduler-idle
# test profile means the lifecycle call behind it did not happen.
_WATERMARKS = ("persisted_at", "snapshotted_at", "purged_at")
# Captures label -> value from the demo's watermark line, so the assertion can
# require the label's PRESENCE before judging its value. A bare
# `"persisted_at=none" not in stdout` check would pass vacuously the moment the
# label was renamed or the line dropped. Derived from _WATERMARKS rather than
# respelling the names, so the two cannot drift apart.
_WATERMARK_VALUE = re.compile(rf"({'|'.join(_WATERMARKS)})=(\S+)")


def test_oltp_demo_seeks_one_row_on_both_paths_against_cold_columnar():
    client = make_client()

    with reaped_demo_catalogs(client) as before:
        result = subprocess.run(
            [
                sys.executable,
                str(_DEMO_PATH),
                "--rows",
                str(_ROWS),
                "--reps",
                str(_REPS),
            ],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
            # capture_output streams nothing while it runs, so an unbounded wait
            # on a wedged demo would hang the suite silently. TimeoutExpired is
            # the legible failure.
            timeout=300,
        )
        _assert_walkthrough(result)

        # Inside the context, so it pins the demo's own cleanup without gating
        # the reap: if the demo stranded a catalog, the exit still takes it out.
        assert demo_catalog_names(client) == before, (
            "oltp_demo.py must delete the catalog it created — it is pure "
            "scaffolding once the numbers are printed, and every leaked catalog "
            "sits on a stack the rest of the suite shares"
        )


def _assert_walkthrough(result) -> None:
    assert result.returncode == 0, result.stderr[-4000:]

    stdout = result.stdout
    for marker in (_TIER_LINE, _ROW_SECTION, _LATENCY_SECTION):
        assert marker in stdout, f"missing {marker!r} in:\n{stdout[-2000:]}"

    _assert_tier_transition(stdout)
    _assert_found_the_right_row(stdout)
    _assert_both_arms_measured(stdout)


def _table_body_rows(section: str) -> list[str]:
    """The data rows of a printed markdown table.

    Structural rather than heuristic: take the pipe-prefixed lines, drop the
    first (the header), then drop the alignment rule by its content — its cells
    hold nothing but dashes and colons. Matching body rows by "contains a digit"
    would count a header as a body row the moment a column name gained one.
    """
    piped = [line for line in section.splitlines() if line.startswith("|")]

    return [
        line
        for line in piped[1:]
        if not set(line.replace("|", "").strip()) <= set("-: ")
    ]


def _assert_tier_transition(stdout: str) -> None:
    """The rows really moved to cold, rather than the demo saying so.

    The announcement line is an unconditional print — deleting the persist,
    snapshot and purge calls while keeping it would leave every other assertion
    here green. The watermarks are the real signal, and the demo prints them for
    exactly this check: on the test profile the lifecycle scheduler is idle, so
    these calls are the only thing that can move them, and a watermark that is
    not a number means one did not run.

    Purge is the load-bearing one, and for the reason the demo gives: persist
    leaves the rows in the hot tables, so the plan keeps attaching a hot arm
    until purge deletes them. The watermark this asserts on is the observable
    proxy for that delete — it commits atomically with it — not the cause.
    Without it, every "cold" number in the run is a hot read wearing a cold
    label.
    """
    printed = dict(_WATERMARK_VALUE.findall(stdout))

    missing = [field for field in _WATERMARKS if field not in printed]
    assert not missing, (
        f"the demo printed no {missing} watermark, so nothing here pins the tier "
        f"transition. Absence must fail rather than exempt itself:\n"
        f"{stdout[-2000:]}"
    )

    # Judged on shape, not on the spelling of the demo's placeholder. Comparing
    # against the literal "none" would stop catching anything the moment that
    # string became "unset" or "-", which is the same drift the label check
    # above exists to prevent — a real watermark is a number.
    no_ops = [field for field in _WATERMARKS if not printed[field].isdigit()]
    # Report the offending values: a non-numeric watermark means either the
    # lifecycle call was a no-op or the demo changed how it prints them, and the
    # predicate cannot tell those apart. The value can.
    assert not no_ops, (
        f"{[(field, printed[field]) for field in no_ops]} carry no numeric "
        f"watermark — either the lifecycle call was a no-op, or the demo "
        f"stopped printing raw micros:\n{stdout[-2000:]}"
    )


def _assert_found_the_right_row(stdout: str) -> None:
    """The seek returned the target row, and only it.

    Sliced to the row section rather than searched over the whole run: the demo
    names the target id in its progress lines too, so a whole-stdout check would
    pass even if the lookup returned nothing.

    Sliced between the two section headers, NOT on a bare "---": the printed
    table's own alignment rule (``|---:|``) contains that string, so splitting on
    it cuts the section off above the data rows and the assertion then fails on
    a table that is in fact correct.
    """
    row_section = stdout.split(_ROW_SECTION)[1].split(_LATENCY_SECTION)[0]

    assert _TARGET_OWNER in row_section, (
        f"the lookup must return account {_TARGET_ID} ({_TARGET_OWNER}); "
        f"section was:\n{row_section}"
    )
    # Exactly one body row, rather than the absence of one sentinel id. A
    # sentinel only catches a scan that happens to include that id, and it
    # silently stops catching anything at all if the demo ever seeds from 1
    # instead of 0 — the string it looks for would simply never be printed.
    # Counting rows is independent of both the id scheme and _ROWS.
    body_rows = _table_body_rows(row_section)
    assert len(body_rows) == 1, (
        f"a point lookup returns exactly one row; saw {len(body_rows)} in:\n"
        f"{row_section}"
    )


def _assert_both_arms_measured(stdout: str) -> None:
    """Both surfaces, each with a real measurement.

    Three separate roles, because a run that quietly skipped an arm would print
    a smaller table and still exit 0: the labels say which arms are named, the
    structural row count pins that there are exactly two of them, and the
    numeric cells pin that each printed a number rather than a placeholder.
    """
    # Bounded at the trailing prose, not open to end-of-stdout: an unbounded
    # slice lets text printed after the table satisfy both checks below.
    latency_section = stdout.split(_LATENCY_SECTION)[1].split("\n\n")[0]

    lowered = latency_section.lower()
    for label in ("grpc", "sql"):
        assert label in lowered, (
            f"the latency table must name the {label!r} arm; table was:\n"
            f"{latency_section[:2000]}"
        )

    # Exactly two rows, counted structurally. `>=` on numeric cells would let a
    # third arm — or a `path` column that happened to render numerically — pass
    # as "both arms measured".
    rows = _table_body_rows(latency_section)
    assert len(rows) == 2, (
        f"expected exactly two arms (gRPC and SQL), saw {len(rows)} table rows "
        f"in:\n{latency_section[:2000]}"
    )

    # And each row carries a real number rather than a placeholder.
    measured = _MS_CELL.findall(latency_section)
    assert len(measured) >= 2, (
        f"expected a measurement for each arm, saw {len(measured)} numeric "
        f"cells in:\n{latency_section[:2000]}"
    )
