"""Enforce blank lines after compound statement blocks.

Style rule: add a blank line after every compound statement block
(if/elif/else, for, while, with, try/except/finally) before the
next sibling statement.

NOTE: This script uses only the stdlib so it can run without uv/venv
in pre-commit hooks.

Usage:
    python scripts/check_blank_lines.py [--fix] [paths...]

Without --fix, prints violations and exits non-zero if any found.
With --fix, inserts missing blank lines in-place.
If no paths are given, defaults to packages/penca-client/src/penca_client/.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

COMPOUND_TYPES = (
    ast.If,
    ast.For,
    ast.While,
    ast.With,
    ast.Try,
    ast.AsyncFor,
    ast.AsyncWith,
)


def find_violations(source: str, tree: ast.Module) -> list[int]:
    """Return line numbers where a blank line should be inserted.

    Each returned line number is the last line of a compound statement
    that is immediately followed by a sibling statement with no blank
    line in between.
    """
    lines = source.splitlines()
    violations: list[int] = []

    def check_body(body: list[ast.stmt]) -> None:
        for index in range(len(body) - 1):
            statement = body[index]
            next_statement = body[index + 1]

            if not isinstance(statement, COMPOUND_TYPES):
                continue

            end_line = statement.end_lineno
            next_start = next_statement.lineno

            if end_line is None or next_start is None:
                continue

            # Check whether there is at least one blank line between
            # end of compound statement and start of next statement.
            has_blank = False
            for line_number in range(end_line, next_start - 1):
                line_content = lines[line_number]  # 0-indexed: end_line is 1-indexed
                if line_content.strip() == "":
                    has_blank = True
                    break

            if not has_blank:
                violations.append(end_line)

        # Recurse into nested bodies.
        for statement in body:
            for attr in ("body", "orelse", "finalbody", "handlers"):
                nested_body = getattr(statement, attr, None)
                if isinstance(nested_body, list) and nested_body:
                    if attr == "handlers":
                        for handler in nested_body:
                            check_body(handler.body)
                    else:
                        check_body(nested_body)

    check_body(tree.body)
    return sorted(set(violations))


def fix_source(source: str, violations: list[int]) -> str:
    """Insert blank lines after the given line numbers."""
    lines = source.splitlines(keepends=True)
    # Process in reverse so insertions don't shift line numbers.
    for line_number in sorted(violations, reverse=True):
        index = (
            line_number  # insert after this 1-indexed line = at this 0-indexed position
        )
        lines.insert(index, "\n")

    return "".join(lines)


def process_file(file_path: Path, fix: bool) -> list[str]:
    """Check (and optionally fix) a single file. Returns violation messages."""
    source = file_path.read_text()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    violations = find_violations(source, tree)

    if not violations:
        return []

    messages = [
        f"{file_path}:{line_number}: missing blank line after compound statement"
        for line_number in violations
    ]

    if fix:
        fixed = fix_source(source, violations)
        file_path.write_text(fixed)

    return messages


def main() -> int:
    fix = "--fix" in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != "--fix"]

    if not args:
        args = ["packages/penca-client/src/penca_client/"]

    paths: list[Path] = []
    for arg in args:
        path = Path(arg)
        if path.is_file() and path.suffix == ".py":
            paths.append(path)
        elif path.is_dir():
            paths.extend(sorted(path.rglob("*.py")))

    all_messages: list[str] = []
    for file_path in paths:
        all_messages.extend(process_file(file_path, fix))

    for message in all_messages:
        print(message)

    if all_messages and fix:
        print(f"\nFixed {len(all_messages)} violation(s).")

    return 1 if all_messages and not fix else 0


if __name__ == "__main__":
    sys.exit(main())
