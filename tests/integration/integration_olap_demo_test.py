"""Smoke test for ``examples/olap_demo.py`` (CHA-527).

The demo answers one analytical question two ways against the same cold columnar
copy: gRPC ``ReadData`` has no aggregate surface, so it ships every row and
groups client-side, while Flight SQL pushes the ``GROUP BY`` into the engine and
returns only the grouped rows. Printing two timings side by side is only honest
if both arms answered the *same* question — so the load-bearing assertion here
is that the two aggregates agree, not that either one is fast.

Deliberately a subprocess smoke test rather than an import-and-assert: the demo
is a flat ``main()`` that prints, with no seam to call into, and adding one
purely for a test would change an example whose job is to read simply. Same
shape as ``integration_audit_demo_test.py``.

Run via ``just integration-test olap_demo``.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

from .integration_helpers import make_client

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEMO_PATH = _REPO_ROOT / "examples" / "olap_demo.py"

# Small enough to keep the suite quick. Every assertion below is
# scale-independent — the two arms are compared against each other and against
# this number — so the demo's much larger default is pinned by the same checks.
_ROWS = 3_000
_SEED = 20260728

_GRPC_SECTION = "--- Aggregate via gRPC ReadData (rows shipped, grouped here) ---"
_SQL_SECTION = "--- Aggregate via Flight SQL (GROUP BY pushed into the engine) ---"
_LATENCY_SECTION = "--- Analytical query latency ---"

# A tabulate body row: "| us-east |  1234 |  56789 |". Rejects the header rule
# ("|:---|---:|") because that row has no digits outside the dashes.
_TABLE_ROW = re.compile(r"^\|([^|]+)\|\s*(\d+)\s*\|\s*(-?\d+)\s*\|\s*$", re.MULTILINE)
_MS_CELL = re.compile(r"\|\s*\d+\.?\d*\s*\|")


def _demo_catalogs(client) -> set[str]:
    return {
        catalog.catalog_name
        for catalog in client.list_catalogs()
        if catalog.catalog_name.startswith("demo_")
    }


def test_olap_demo_answers_the_same_question_on_both_paths():
    client = make_client()
    before = _demo_catalogs(client)

    try:
        result = subprocess.run(
            [
                sys.executable,
                str(_DEMO_PATH),
                "--rows",
                str(_ROWS),
                "--seed",
                str(_SEED),
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

        # Inside the try, so it pins the demo's cleanup without gating the reap.
        assert _demo_catalogs(client) == before, (
            "olap_demo.py must delete the catalog it created — it is pure "
            "scaffolding once the numbers are printed, and every leaked catalog "
            "sits on a stack the rest of the suite shares"
        )
    finally:
        # finally, and reaping whatever appeared: the demo creates its catalog
        # before printing anything, so every red run strands one.
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
    for marker in (_GRPC_SECTION, _SQL_SECTION, _LATENCY_SECTION):
        assert marker in stdout, f"missing {marker!r} in:\n{stdout[-2000:]}"

    grpc_groups = _parse_groups(stdout, _GRPC_SECTION, _SQL_SECTION)
    sql_groups = _parse_groups(stdout, _SQL_SECTION, _LATENCY_SECTION)

    _assert_arms_agree(grpc_groups, sql_groups)
    _assert_covers_every_seeded_row(grpc_groups)
    _assert_both_arms_measured(stdout)


def _parse_groups(
    stdout: str, section: str, next_section: str
) -> dict[str, tuple[int, int]]:
    """``region -> (events, total_cents)`` from one printed aggregate table.

    ``total_cents`` is an integer column on purpose: the two arms sum in
    different engines and in different orders, and a float sum is not
    associative — comparing them exactly would be a flake waiting to happen,
    while rounding to compare would weaken the assertion. Integer cents make the
    equality exact and meaningful.

    Bounded by the *next* section header, NOT by a bare "---": the printed
    table's own alignment rule (``|---:|``) contains that string, so splitting on
    it would cut the body off above the data rows and parse zero groups.
    """
    body = stdout.split(section)[1].split(next_section)[0]
    groups = {
        region.strip(): (int(events), int(total))
        for region, events, total in _TABLE_ROW.findall(body)
    }
    assert groups, f"no aggregate rows parsed from {section!r}:\n{body}"

    return groups


def _assert_arms_agree(
    grpc_groups: dict[str, tuple[int, int]],
    sql_groups: dict[str, tuple[int, int]],
) -> None:
    """The load-bearing check: two paths, one answer.

    Compares whole dicts rather than iterating one side, so a SQL arm that
    dropped or invented a region fails on the key set instead of silently
    passing a per-key loop over the other arm's keys.
    """
    assert grpc_groups == sql_groups, (
        "the gRPC and Flight SQL arms must produce the same aggregate — the "
        "timing comparison is only honest if both answered the same question.\n"
        f"gRPC: {grpc_groups}\n"
        f"SQL:  {sql_groups}"
    )


def _assert_covers_every_seeded_row(groups: dict[str, tuple[int, int]]) -> None:
    """Every seeded row landed in exactly one group.

    A positive control: two arms that both read the wrong tier, or both read
    nothing, would agree with each other and satisfy the equality above.
    """
    counted = sum(events for events, _total in groups.values())
    assert counted == _ROWS, (
        f"the aggregate covers {counted} rows, expected {_ROWS} — both arms "
        f"agreeing on a short read is still a short read. Groups: {groups}"
    )
    assert len(groups) > 1, (
        f"a single group makes GROUP BY trivial and the columnar claim "
        f"untested; saw {groups}"
    )


def _assert_both_arms_measured(stdout: str) -> None:
    latency_section = stdout.split(_LATENCY_SECTION)[1]

    lowered = latency_section.lower()
    for label in ("grpc", "sql"):
        assert label in lowered, (
            f"the latency table must name the {label!r} arm; table was:\n"
            f"{latency_section[:2000]}"
        )

    measured = _MS_CELL.findall(latency_section)
    assert len(measured) >= 2, (
        f"expected a measurement for each arm, saw {len(measured)} numeric "
        f"cells in:\n{latency_section[:2000]}"
    )
