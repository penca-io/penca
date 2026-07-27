"""Static checks for the blank-lines gate's file discovery (CHA-517).

``scripts/check_blank_lines.py`` runs with ``--fix`` from ``just format`` and from
the pre-commit hook, so its discovery logic decides which files a gate is allowed
to *rewrite*. Two invariants matter enough to pin: the generated proto stubs stay
byte-identical to ``just compile-protos-py`` output, and a gitignored scratch file
is never touched. Both were quietly broken at different points while the gate was
being widened from a narrow hardcoded path to the whole repo — the exclusion
anchored to the process cwd rather than the repo root, so it lapsed entirely for
any invocation from a subdirectory.

No Docker, no fixtures, no penca services — runs under ``just static-test
blank_lines_discovery`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]
SCRIPT = REPO_ROOT / "scripts/check_blank_lines.py"
STUB_SUBTREE = Path("packages/penca-proto/src/penca_proto")


def _load_script():
    spec = importlib.util.spec_from_file_location("check_blank_lines", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


checker = _load_script()


def test_generated_stubs_are_excluded_by_relative_and_absolute_path(monkeypatch):
    # Relative inputs resolve against the cwd, so pin it: this suite's whole
    # subject is path anchoring, and a false failure from pytest's cwd would be
    # indistinguishable from the bug.
    monkeypatch.chdir(REPO_ROOT)
    stub = STUB_SUBTREE / "external/v1/common_pb2.py"
    assert checker.is_excluded(stub)
    assert checker.is_excluded(REPO_ROOT / stub)


def test_generated_stubs_stay_excluded_from_a_subdirectory_cwd(monkeypatch):
    """The exclusion is anchored to the repo root, not the process cwd.

    Anchored to cwd, running from ``packages/penca-proto`` normalized the stubs to
    ``src/penca_proto``, which matched no subtree entry — so ``--fix`` rewrote
    generated code.
    """
    monkeypatch.chdir(REPO_ROOT / "packages/penca-proto")
    assert checker.is_excluded(Path("src/penca_proto/external/v1/common_pb2.py"))
    assert checker.is_excluded(REPO_ROOT / STUB_SUBTREE / "external/v1/common_pb2.py")


def test_build_and_venv_trees_are_excluded_wherever_they_appear(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)
    for candidate in (
        Path(".venv/lib/python3.10/site-packages/x.py"),
        Path("packages/penca-client/build/lib/x.py"),
        Path("target/debug/build/x.py"),
        Path("examples/__pycache__/x.py"),
    ):
        assert checker.is_excluded(candidate), candidate

    assert not checker.is_excluded(Path("examples/branch_demo.py"))
    assert not checker.is_excluded(Path("tests/static/x.py"))


def test_the_walk_never_descends_into_an_excluded_tree(tmp_path, monkeypatch):
    """Pruning happens *during* the walk, not by filtering its results.

    Spies on the directories actually visited, because the output alone cannot
    tell the two apart — the old rglob-plus-filter implementation returned the
    same file set. The contract here is a performance one: never scandir .venv or
    target on ``just check``'s hot path.
    """
    (tmp_path / "src").mkdir()
    (tmp_path / "src/keep.py").write_text("x = 1\n")
    for excluded in (".venv", "target", "__pycache__"):
        (tmp_path / excluded).mkdir()
        (tmp_path / excluded / "skip.py").write_text("x = 1\n")

    visited: list[str] = []
    real_walk = checker.os.walk

    def recording_walk(top, *args, **kwargs):
        for dirpath, dirnames, filenames in real_walk(top, *args, **kwargs):
            visited.append(dirpath)
            yield dirpath, dirnames, filenames

    monkeypatch.setattr(checker.os, "walk", recording_walk)

    found = {path.name for path in checker.walk_python_files(tmp_path)}
    assert found == {"keep.py"}

    # Liveness first: without it a glob-based implementation records no visits and
    # the "never descended" check below passes on an empty list.
    assert visited, "walk_python_files must walk with os.walk, not glob and filter"
    # Tree-relative parts, not a substring of the absolute path: pytest's basetemp
    # can itself sit under a directory named `target`, which would fail this on a
    # correct implementation — the wrong failure mode for a suite about path
    # anchoring.
    descended = {
        part
        for dirpath in visited
        for part in Path(dirpath).relative_to(tmp_path).parts
    }
    assert not descended & {".venv", "target", "__pycache__"}, visited


def test_gitignored_paths_are_dropped_including_non_ascii_names(monkeypatch):
    """``git check-ignore`` renders paths through ``core.quotePath``.

    Without NUL-delimited output a non-ASCII name comes back escaped, never
    matches, and survives the filter into a ``--fix`` rewrite — the exact case the
    filter exists to prevent. ``tests/tdd/`` is gitignored, so it is a real
    fixture rather than a synthetic one.
    """
    monkeypatch.chdir(REPO_ROOT)
    scratch = REPO_ROOT / "tests/tdd"
    created_scratch = not scratch.exists()
    scratch.mkdir(exist_ok=True)
    ascii_path = scratch / "static_probe.py"
    unicode_path = scratch / "static_probé.py"
    tracked = Path("examples/branch_demo.py")
    ascii_path.write_text("x = 1\n")
    unicode_path.write_text("x = 1\n")
    try:
        kept = checker.drop_gitignored(
            [
                ascii_path.relative_to(REPO_ROOT),
                unicode_path.relative_to(REPO_ROOT),
                tracked,
            ]
        )
        assert kept == [tracked], (
            f"gitignored scratch files must be dropped, kept {kept}"
        )
    finally:
        ascii_path.unlink(missing_ok=True)
        unicode_path.unlink(missing_ok=True)
        if created_scratch:
            scratch.rmdir()


def test_collect_paths_refuses_an_explicitly_named_generated_stub(monkeypatch):
    """The composition, not just its pieces.

    Dropping the ``is_excluded`` call from ``collect_paths`` leaves every
    single-function test green while ``just format <a stub>`` rewrites generated
    code, so the wiring needs its own check.
    """
    monkeypatch.chdir(REPO_ROOT)
    stub = STUB_SUBTREE / "external/v1/common_pb2.py"
    assert stub.is_file(), stub

    assert checker.collect_paths([str(stub)]) == []
    assert checker.collect_paths(["examples/branch_demo.py"]) == [
        Path("examples/branch_demo.py")
    ]


def test_collect_paths_drops_gitignored_files_from_a_walked_directory(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)
    scratch = REPO_ROOT / "tests/tdd"
    created_scratch = not scratch.exists()
    scratch.mkdir(exist_ok=True)
    probe = scratch / "static_collect_probe.py"
    probe.write_text("x = 1\n")
    try:
        collected = checker.collect_paths(["tests"])
        assert not any("tdd" in str(path) for path in collected), (
            f"gitignored scratch must not survive a walked directory: {collected}"
        )
        assert any(path.name.startswith("static_") for path in collected), (
            "the walk must still have found the real static tests"
        )
    finally:
        probe.unlink(missing_ok=True)
        if created_scratch:
            scratch.rmdir()


def test_collect_paths_rejects_a_path_that_does_not_exist(monkeypatch):
    """A mistyped scope must fail, not report success on nothing."""
    monkeypatch.chdir(REPO_ROOT)
    try:
        checker.collect_paths(["tests/statik"])
    except SystemExit as exc:
        assert "no such path" in str(exc) and "tests/statik" in str(exc)
    else:
        raise AssertionError("a nonexistent scope must be fatal")


def test_collect_paths_distinguishes_a_non_python_file_from_a_missing_one(monkeypatch):
    """An existing non-.py argument must not be reported as a missing path.

    `just format-check README.md` used to say "no such path: README.md", sending
    the reader hunting a typo that is not there.
    """
    monkeypatch.chdir(REPO_ROOT)
    assert (REPO_ROOT / "README.md").is_file()
    try:
        checker.collect_paths(["README.md"])
    except SystemExit as exc:
        assert "not a Python file or directory" in str(exc), str(exc)
        assert "no such path" not in str(exc), str(exc)
    else:
        raise AssertionError("an existing non-Python argument must be fatal too")


def test_stub_exclusion_agrees_with_ruff_config():
    """One exclusion policy, two files — pin that they still agree.

    ``pyproject.toml``'s ``[tool.ruff] extend-exclude`` and this script's
    ``EXCLUDED_SUBTREES`` both exist to keep the generated stubs byte-identical to
    ``just compile-protos-py`` output. They are maintained by hand in separate
    files and both sit on a ``--fix`` path, so a divergence silently un-protects
    the stubs from one of the two gates.
    """
    pyproject = (REPO_ROOT / "pyproject.toml").read_text()
    ruff_section = pyproject.split("[tool.ruff]", 1)[1].split("\n[", 1)[0]
    # Keyed on `extend-exclude` specifically. Scraping any quoted line in the
    # section passes for plain `exclude`, which is the regression the comment in
    # pyproject.toml exists to prevent: `exclude` REPLACES ruff's built-in
    # defaults, so node_modules stops being excluded there while this script still
    # excludes it — the one tree where the two gates genuinely diverge.
    assert "extend-exclude = [" in ruff_section, (
        "pyproject must use extend-exclude, not exclude — exclude replaces ruff's "
        f"built-in defaults. Section was:\n{ruff_section}"
    )
    entries = ruff_section.split("extend-exclude = [", 1)[1].split("]", 1)[0]
    excluded = {
        line.strip().strip(',"') for line in entries.splitlines() if line.strip()
    }

    assert excluded, f"no extend-exclude entries parsed from:\n{ruff_section}"
    assert excluded == {str(subtree) for subtree in checker.EXCLUDED_SUBTREES}, (
        "pyproject's ruff extend-exclude and check_blank_lines' EXCLUDED_SUBTREES "
        f"have diverged: ruff={excluded} "
        f"script={{{', '.join(str(s) for s in checker.EXCLUDED_SUBTREES)}}}"
    )
