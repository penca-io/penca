"""CHA-86: Penca has exactly one MVCC consistent-read mechanism.

Every read pins a bounded snapshot (``AsOfMicros`` for the default /
time-travel paths, ``OpenTx`` inside an open transaction). The unbounded
``ReadSnapshot::Latest`` variant is removed so no code path can issue a
torn read across the merge-on-read probes.

These are pure source-input checks — no Docker. They live under
``tests/static/`` (run via ``just static-test``, also wired into
``just check``). Red baseline before the fix: ``ReadSnapshot::Latest``
appears in the enum definition plus the production + test construction
sites, and ADR 0007 still describes the pin in the future tense.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CRATES = REPO_ROOT / "crates"
ADR_0006 = REPO_ROOT / "docs/decisions/0006-sql-dml-out-of-write-microservice.md"
ADR_0007 = REPO_ROOT / "docs/decisions/0007-session-entity.md"


def _rust_sources() -> list[Path]:
    return sorted(CRATES.rglob("*.rs"))


class TestReadSnapshotOneMechanism:
    def test_no_read_snapshot_latest_in_crates(self):
        # The variant is gone, so it may not be constructed or matched in
        # ANY crate source — production or ``#[cfg(test)]`` modules alike.
        offenders: list[str] = []
        for path in _rust_sources():
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                # Word-boundary match so a future variant whose name
                # merely starts with `Latest` is not a false positive.
                if re.search(r"ReadSnapshot::Latest\b", line):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")

        assert not offenders, (
            "ReadSnapshot::Latest must not appear in crate sources "
            "(CHA-86 removed the variant):\n" + "\n".join(offenders)
        )

    def test_read_snapshot_enum_has_no_latest_variant(self):
        snapshot_rs = CRATES / "penca-merge/src/snapshot.rs"
        text = snapshot_rs.read_text(encoding="utf-8")
        assert not re.search(r"^\s*Latest\s*,", text, re.MULTILINE), (
            "ReadSnapshot must not declare a `Latest` variant"
        )

    def test_adrs_describe_pin_in_present_tense(self):
        # The pin is implemented; ADR 0006/0007 must not describe it as
        # something CHA-86 "will" do.
        # Forbid the common future-tense phrasings ("will pin", "will be
        # pinned", "shall pin", "shall be pinned").
        future_tense = re.compile(r"(will|shall)\s+(be\s+)?pin", re.IGNORECASE)
        for adr in (ADR_0006, ADR_0007):
            text = adr.read_text(encoding="utf-8")
            assert not future_tense.search(text), (
                f"{adr.relative_to(REPO_ROOT)} still describes the CHA-86 "
                "pin in the future tense"
            )
