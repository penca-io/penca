#!/usr/bin/env python3
"""Validate commit messages against the Conventional Commits format.

Reads allowed scopes from linear/labels.toml. Intended to run as a
pre-commit commit-msg hook.

Usage:
    python scripts/check_commit_msg.py <commit-msg-file>
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

TYPES = {"feat", "fix", "refactor", "perf", "test", "docs", "build", "chore"}

LABELS_PATH = Path(__file__).resolve().parent.parent / "linear" / "labels.toml"

# Matches: type[(scope)][!]: description
SUBJECT_RE = re.compile(
    r"^(?P<type>[a-z]+)"
    r"(?:\((?P<scope>[a-z][a-z0-9-]*)\))?"
    r"(?P<breaking>!)?"
    r": "
    r"(?P<desc>.+)$"
)


def load_scopes() -> set[str]:
    """Parse scope names from linear/labels.toml (lightweight, no toml dep)."""
    scopes: set[str] = set()
    for line in LABELS_PATH.read_text().splitlines():
        line = line.strip()
        if line.startswith("[") and line.endswith("]"):
            scopes.add(line[1:-1])

    return scopes


def validate(message: str) -> list[str]:
    errors: list[str] = []
    lines = message.strip().splitlines()
    if not lines:
        errors.append("Commit message is empty.")
        return errors

    subject = lines[0]

    # Skip merge commits
    if subject.startswith("Merge "):
        return errors

    match = SUBJECT_RE.match(subject)
    if not match:
        errors.append(
            f"Subject does not match conventional commits format.\n"
            f"  Expected: <type>(<scope>): <description>\n"
            f"  Got:      {subject}"
        )
        return errors

    commit_type = match.group("type")
    scope = match.group("scope")
    desc = match.group("desc")

    if commit_type not in TYPES:
        allowed = ", ".join(sorted(TYPES))
        errors.append(f"Unknown type '{commit_type}'. Allowed: {allowed}")

    if scope is not None:
        scopes = load_scopes()
        if scope not in scopes:
            allowed = ", ".join(sorted(scopes))
            errors.append(f"Unknown scope '{scope}'. Allowed: {allowed}")

    if desc and desc[0].isupper():
        errors.append(f"Description must be lowercase: '{desc}'")

    if desc and desc.endswith("."):
        errors.append("Description must not end with a period.")

    if len(subject) > 72:
        errors.append(f"Subject line is {len(subject)} chars (max 72).")

    return errors


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: check_commit_msg.py <commit-msg-file>", file=sys.stderr)
        sys.exit(1)

    message = Path(sys.argv[1]).read_text()
    errors = validate(message)

    if errors:
        print("Commit message validation failed:\n", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)

        print(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
