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

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEMO_PATH = _REPO_ROOT / "examples" / "audit_demo.py"


def test_audit_demo_runs_the_documented_walkthrough():
    result = subprocess.run(
        [sys.executable, str(_DEMO_PATH)],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    assert result.returncode == 0, result.stderr[-4000:]

    stdout = result.stdout
    for marker in (
        "TX 1 committed",
        "TX 2 committed",
        "TX 3 committed",
        "Current state (read_data)",
        "Full audit trail (audit_data)",
        "Audit trail (after TX 1 only)",
        "Time-travel: state as of TX 1",
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

    time_travel = stdout.split("--- Time-travel: state as of TX 1 ---")[1]
    assert "bob" in time_travel, time_travel
