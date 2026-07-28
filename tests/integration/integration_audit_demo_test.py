"""Smoke test for ``examples/audit_demo.py`` (CHA-517).

The README's quick start tells a reader to run this, and it had no coverage
anywhere — so a break in the four-step ``read_data`` / ``audit_data`` / ``as_of``
walkthrough would surface first to a launch reader rather than to CI.

Deliberately a subprocess smoke test rather than an import-and-assert: the demo
is a flat ``main()`` that prints, with no seam to call into, and adding one purely
for a test would change an example whose job is to read simply. This asserts what
the README actually promises — the documented command runs clean and prints the
four sections.

Run via ``just integration-test audit_demo``.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from .integration_helpers import make_client

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEMO_PATH = _REPO_ROOT / "examples" / "audit_demo.py"


def _demo_catalogs(client) -> set[str]:
    return {
        catalog.catalog_name
        for catalog in client.list_catalogs()
        if catalog.catalog_name.startswith("demo_")
    }


def test_audit_demo_runs_the_documented_walkthrough():
    # audit_demo.py names its catalog demo_<hex> and never deletes it, and unlike
    # sandbox_demo it does not print the uuid — so diffing the catalog list is the
    # only way to reap it, and without that the suite leaks one per run onto a
    # stack every other test shares.
    client = make_client()
    before = _demo_catalogs(client)

    try:
        result = subprocess.run(
            [sys.executable, str(_DEMO_PATH)],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=300,
        )
        _assert_walkthrough(result)

        # Inside the try, so it pins the demo's behaviour without gating cleanup:
        # if more than one catalog appeared, the finally still reaps all of them.
        created = _demo_catalogs(client) - before
        assert len(created) == 1, f"expected one new demo_ catalog, saw {created}"
    finally:
        # finally, and reaping whatever appeared rather than gating on the count:
        # audit_demo.py creates its catalog before printing anything, so every red
        # run strands one — which is precisely what this reap exists to prevent.
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
    # Both audit sections must print the upsert/delete split the slices key on. A
    # bare `"Deletes:" in stdout` would pass on either one alone, and the missing
    # section would then surface as an unpack error rather than an assertion.
    assert stdout.count("Deletes:") == 2, stdout[-2000:]

    for marker in (
        "TX 1 committed",
        "TX 2 committed",
        "TX 3 committed",
        # Dashed forms: the same strings the slices below key on, so a change to
        # the demo's header decoration fails here with the loop's message rather
        # than as an IndexError in a slice.
        "--- Current state (read_data) ---",
        "--- Full audit trail (audit_data) ---",
        "--- Audit trail (after TX 1 only) ---",
        "--- Time-travel: state as of TX 1 ---",
    ):
        assert marker in stdout, f"missing {marker!r} in:\n{stdout[-2000:]}"

    # The tombstone is the point of the audit trail: bob is deleted in TX 3, so he
    # must be gone from the current state but present in the as-of-TX-1 read.
    # Sliced between the two section headers, not on a bare "---": the headers are
    # themselves wrapped in dashes, so splitting on "---" yielded a single space
    # and the assertion held no matter what the demo printed. alice is the positive
    # control that keeps an empty slice from passing.
    current = stdout.split("--- Current state (read_data) ---")[1].split(
        "--- Full audit trail"
    )[0]
    assert "alice" in current, current
    assert "bob" not in current, current

    # The time-filtered audit really filtered: bob's only upsert is TX 1, so after
    # TX 1 he appears among the deletes and not among the upserts — unlike the full
    # trail, where he is in both. charlie (inserted TX 2) is the positive control.
    after = stdout.split("--- Audit trail (after TX 1 only) ---")[1].split(
        "--- Time-travel"
    )[0]
    after_upserts, after_deletes = after.split("Deletes:")
    assert "charlie" in after_upserts, after
    assert "bob" not in after_upserts, after
    assert "bob" in after_deletes, after

    # And demonstrate the contrast rather than only asserting it in a comment: the
    # *unfiltered* trail carries bob on both sides. Without this the full-trail
    # section is pinned at "the header printed", so an audit_data that regressed to
    # returning nothing would print "(none)" twice and pass.
    full = stdout.split("--- Full audit trail (audit_data) ---")[1].split(
        "--- Audit trail (after"
    )[0]
    full_upserts, full_deletes = full.split("Deletes:")
    assert "bob" in full_upserts, full
    assert "bob" in full_deletes, full

    # bob present catches as_of being ignored; charlie absent catches it resolving
    # to the wrong commit, since he does not exist until TX 2.
    time_travel = stdout.split("--- Time-travel: state as of TX 1 ---")[1]
    assert "bob" in time_travel, time_travel
    assert "charlie" not in time_travel, time_travel
