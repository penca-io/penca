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
If no paths are given, defaults to the whole repo, matching `just lint` /
`just format-check` and the repo-wide pre-commit hook.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

# Never checked. The generated proto stubs must stay byte-identical to
# `just compile-protos-py` output — the pre-commit hook excludes them, and this
# script has no exclusion config of its own. The rest are build/venv trees that a
# repo-root walk would otherwise pull in; ruff gets these from its own defaults.
EXCLUDED_DIR_NAMES = frozenset(
    {".git", ".venv", "__pycache__", "node_modules", "target", "build", "dist"}
)
EXCLUDED_SUBTREES = (Path("packages/penca-proto/src/penca_proto"),)

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


@lru_cache(maxsize=1)
def repo_root() -> Path:
    """The repo root, which is what ``EXCLUDED_SUBTREES`` is relative to.

    Anchoring on the process cwd instead would make the generated-stub guard lapse
    for any invocation from a subdirectory: from ``packages/penca-proto`` the
    stubs normalize to ``src/penca_proto``, which matches no subtree entry, and
    ``--fix`` rewrites them. ruff is cwd-independent for the same reason — it
    resolves excludes against the config file, not the cwd. Cached so the walk
    does not re-run git per directory.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return Path.cwd().resolve()

    if result.returncode != 0:
        return Path.cwd().resolve()

    return Path(result.stdout.strip()).resolve()


def normalized(path: Path) -> Path:
    """Repo-root-relative form, so every invocation compares the same way."""
    resolved = path.resolve()

    try:
        return resolved.relative_to(repo_root())
    except ValueError:
        return resolved


def is_excluded_dir(directory: Path) -> bool:
    relative = normalized(directory)
    if EXCLUDED_DIR_NAMES & set(relative.parts):
        return True

    return any(
        relative == subtree or subtree in relative.parents
        for subtree in EXCLUDED_SUBTREES
    )


def is_excluded(path: Path) -> bool:
    """True for a file inside an excluded tree.

    Applied to explicitly named files too, not just walked ones: otherwise
    ``--fix packages/penca-proto/.../foo_pb2.py`` rewrites a generated stub, which
    is exactly the byte-identity this guard exists to protect. ruff's
    ``force-exclude`` is unconditional the same way.
    """
    return is_excluded_dir(path.parent)


def walk_python_files(root: Path) -> list[Path]:
    """Every ``.py`` file under ``root``, pruning excluded trees as it descends.

    ``os.walk`` with in-place ``dirnames`` pruning rather than ``rglob``: rglob
    scandirs every directory before a filter can see the results, so a repo-root
    run would stat the whole of ``.venv`` and ``target/`` — gigabytes, hundreds of
    thousands of entries — only to discard them. This gate is on ``just check``'s
    hot path, and CI creates ``.venv`` at the repo root before running it.
    """
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in EXCLUDED_DIR_NAMES]
        current = Path(dirpath)
        if is_excluded_dir(current):
            dirnames[:] = []
            continue

        found.extend(current / name for name in filenames if name.endswith(".py"))

    return sorted(found)


def drop_gitignored(paths: list[Path]) -> list[Path]:
    """Filter out gitignored paths, the way ruff's respect-gitignore does.

    Without this, a repo-root walk picks up scratch trees like the gitignored
    ``tests/tdd/``, so anyone with local scratch files would see
    ``just format-check`` fail on code that is not in the repo. One batched
    ``git check-ignore`` call rather than one per file.

    NUL-delimited both ways: without ``-z`` git renders paths through
    ``core.quotePath``, so a non-ASCII filename comes back escaped, never matches,
    and survives the filter into a ``--fix`` rewrite.
    """
    if not paths:
        return paths

    try:
        result = subprocess.run(
            ["git", "check-ignore", "-z", "--stdin"],
            input="\0".join(str(path) for path in paths),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        msg = "git is required to skip gitignored paths, and is not on PATH"
        raise RuntimeError(msg) from exc

    # Exit 0 = some paths ignored, 1 = none ignored. Anything else is a real
    # failure (not a git repo) and should not silently widen the set.
    if result.returncode not in (0, 1):
        msg = f"git check-ignore failed: {result.stderr.strip()}"
        raise RuntimeError(msg)

    ignored = {entry for entry in result.stdout.split("\0") if entry}

    return [path for path in paths if str(path) not in ignored]


def main() -> int:
    fix = "--fix" in sys.argv
    args = [arg for arg in sys.argv[1:] if arg != "--fix"]

    if not args:
        args = ["."]

    paths: list[Path] = []
    for arg in args:
        path = Path(arg)
        if path.is_file() and path.suffix == ".py" and not is_excluded(path):
            paths.append(path)
        elif path.is_dir():
            paths.extend(walk_python_files(path))

    all_messages: list[str] = []
    for file_path in drop_gitignored(paths):
        all_messages.extend(process_file(file_path, fix))

    for message in all_messages:
        print(message)

    if all_messages and fix:
        print(f"\nFixed {len(all_messages)} violation(s).")

    return 1 if all_messages and not fix else 0


if __name__ == "__main__":
    sys.exit(main())
