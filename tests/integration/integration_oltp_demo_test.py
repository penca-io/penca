"""Smoke test for ``examples/oltp_demo.py`` (CHA-527).

The demo's claim is that a primary-key seek is still a seek once the rows live in
open columnar files — it drives the table cold with persist + snapshot, then
times the lookup over both the gRPC client and Flight SQL. That is two numbers a
reader is invited to trust, so what needs coverage is that both are really
measured and that the seek actually found the row it claims to have found.

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

from .integration_helpers import make_client

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
# id 0 — seeded, but not the row the demo asks for. The negative control: without
# it, a lookup that returned the whole table would satisfy the positive check.
_NON_TARGET_OWNER = "owner_000000"

_ROW_SECTION = "--- The row we looked up ---"
_LATENCY_SECTION = "--- Point lookup latency on cold columnar (mean per seek) ---"

# A pandas/tabulate cell holding a millisecond figure: "| 1.23 |", "|  0.4 |".
_MS_CELL = re.compile(r"\|\s*\d+\.?\d*\s*\|")


def _demo_catalogs(client) -> set[str]:
    return {
        catalog.catalog_name
        for catalog in client.list_catalogs()
        if catalog.catalog_name.startswith("demo_")
    }


def test_oltp_demo_seeks_one_row_on_both_paths_against_cold_columnar():
    client = make_client()
    before = _demo_catalogs(client)

    try:
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

        # Inside the try, so it pins the demo's cleanup without gating the reap:
        # if the demo stranded a catalog, the finally still takes it out.
        assert _demo_catalogs(client) == before, (
            "oltp_demo.py must delete the catalog it created — it is pure "
            "scaffolding once the numbers are printed, and every leaked catalog "
            "sits on a stack the rest of the suite shares"
        )
    finally:
        # finally, and reaping whatever appeared rather than gating on a count:
        # the demo creates its catalog before printing anything, so every red run
        # strands one, which is precisely what this reap exists to prevent.
        try:
            leaked = _demo_catalogs(client) - before
        except Exception as exc:  # noqa: BLE001 - must not mask a real failure
            print(f"(could not list catalogs to reap: {exc})")
            leaked = set()

        for catalog_name in leaked:
            try:
                client.delete_catalog(catalog_name=catalog_name)
            except Exception as exc:  # noqa: BLE001 - must not mask a real failure
                print(f"(could not delete catalog {catalog_name}: {exc})")


def _assert_walkthrough(result) -> None:
    assert result.returncode == 0, result.stderr[-4000:]

    stdout = result.stdout
    for marker in (_ROW_SECTION, _LATENCY_SECTION):
        assert marker in stdout, f"missing {marker!r} in:\n{stdout[-2000:]}"

    _assert_found_the_right_row(stdout)
    _assert_both_arms_measured(stdout)


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
    assert _NON_TARGET_OWNER not in row_section, (
        f"the lookup returned {_NON_TARGET_OWNER} as well — this is a point "
        f"seek, not a scan; section was:\n{row_section}"
    )


def _assert_both_arms_measured(stdout: str) -> None:
    """Both surfaces, each with a real measurement.

    A run that quietly skipped an arm would print a smaller table and still exit
    0. Checking the labels AND the count of millisecond cells catches both a
    missing arm and an arm that printed a placeholder instead of a number.
    """
    latency_section = stdout.split(_LATENCY_SECTION)[1]

    lowered = latency_section.lower()
    for label in ("grpc", "sql"):
        assert label in lowered, (
            f"the latency table must name the {label!r} arm; table was:\n"
            f"{latency_section[:2000]}"
        )

    measured = _MS_CELL.findall(latency_section)
    assert len(measured) >= 2, (
        f"expected a measurement for each arm (gRPC and SQL), saw "
        f"{len(measured)} numeric cells in:\n{latency_section[:2000]}"
    )
