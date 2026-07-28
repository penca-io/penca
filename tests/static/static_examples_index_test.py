"""The README's examples index names every script in ``examples/`` (CHA-527).

``examples/`` is a *family* of single-feature scripts plus one composite
flagship, and the index is how a reader asking "how do I do a point lookup?"
finds the right one file instead of reading a ~720-line story. An index that
silently misses a script is worse than no index — it tells the reader the family
is complete when it isn't.

Discovery-based on purpose: the expected set is globbed off the filesystem, never
hardcoded, because a hardcoded list is exactly the thing that rots. Both
directions are checked, so a *deleted* example cannot leave a dead entry behind
either.

Docker-free, so this runs on a branch PR. That matters: the integration job is
merge-queue only, so ``tests/static/`` is what gives ``examples/`` any pre-merge
coverage at all.

Run via ``just static-test examples_index``.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_EXAMPLES_DIR = _REPO_ROOT / "examples"
_README = _REPO_ROOT / "README.md"

# The index section's heading. The test and the README have to agree on this
# anchor, so it is pinned in one place and every assertion reads it.
_INDEX_HEADING = "## Examples"

# Anchored to a line start, because a plain substring search for "## Examples"
# also matches inside "### Examples" — and the README's per-script sections are
# h3s in exactly this space, so that collision is likely rather than theoretical.
_INDEX_START = re.compile(rf"^{re.escape(_INDEX_HEADING)}\s*$", re.MULTILINE)
_NEXT_H2 = re.compile(r"^## ", re.MULTILINE)

# A fenced code block, including the fence lines. Stripped before matching: the
# README pairs each example with a `uv run python examples/<name>.py` block, so
# without this a script that appears ONLY as a run command would count as
# indexed — precisely the "claims completeness it does not have" failure this
# file exists to prevent. The index entry has to be prose.
_FENCED_BLOCK = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)

# Matches an `examples/<name>.py` mention anywhere in the index section, however
# it is decorated — bare, in backticks, or as a markdown link target:
#   "`examples/oltp_demo.py`" -> "oltp_demo.py"
#   "[point lookup](examples/oltp_demo.py)" -> "oltp_demo.py"
# `[\w.-]` rather than `[A-Za-z0-9_]` so a hyphenated name is recognised as
# indexed instead of reported missing. Deliberately not anchored to a list-item
# or table-cell shape: the index's formatting is the author's call, and pinning
# it here would make every cosmetic README edit a test failure.
_EXAMPLE_MENTION = re.compile(r"examples/([\w.-]+\.py)")


def _index_section() -> str:
    """The README's examples-index section, prose only.

    Scoped to the section rather than searched over the whole README: a script
    named in some unrelated paragraph (the Quick start's run command, say) would
    otherwise satisfy the index assertion without the index listing it at all.
    """
    # Fences come out of the WHOLE file first, before anything else reads it.
    # Stripping them last leaves two ordering hazards: a `## Examples` line
    # inside an earlier fenced block would anchor the search in the wrong place,
    # and a `## ` inside a fence within the section would cut it mid-block,
    # stranding an unbalanced fence that can no longer be stripped.
    readme = _FENCED_BLOCK.sub("", _README.read_text(encoding="utf-8"))
    start = _INDEX_START.search(readme)
    assert start is not None, (
        f"README.md has no examples index section. Expected a line that is "
        f"exactly {_INDEX_HEADING!r}, with nothing after it. It is the entry "
        f"point for the examples family — see CHA-527."
    )

    # End at the next h2 so per-script `###` subsections stay inside the section.
    tail = readme[start.end() :]
    end = _NEXT_H2.search(tail)

    return tail[: end.start()] if end else tail


def _is_runnable_example(name: str) -> bool:
    """A file a reader would actually run, so a README entry is warranted.

    The `_` prefix is the escape hatch: a future `__init__.py` or shared private
    module is not something a reader runs. Applied to BOTH sides of the
    comparison — filtering only discovery would make an index entry for a
    private module fail as "names examples that do not exist", pointing at a
    file that is sitting right there.
    """
    return not name.startswith("_")


def _files_on_disk() -> set[str]:
    """Every `*.py` under examples/, private modules included."""
    return {path.name for path in _EXAMPLES_DIR.glob("*.py")}


def _discovered_examples() -> set[str]:
    """The runnable examples on disk — what the index is required to name."""
    return {name for name in _files_on_disk() if _is_runnable_example(name)}


def _indexed_examples() -> set[str]:
    """Every example the README's index section names, unfiltered.

    Deliberately NOT filtered by `_is_runnable_example`: filtering here would
    drop a private-module entry from the stale check entirely, so an index
    pointing at an `examples/_helpers.py` that does not exist could never be
    reported. The private-module skip belongs on the *required* side only.
    """
    return set(_EXAMPLE_MENTION.findall(_index_section()))


def test_examples_dir_is_not_empty():
    """Guard the two containment tests below against a vacuous pass.

    Both of them compare set against set, and two empty sets are equal — so
    without this a glob that silently stopped matching (a moved directory, a
    renamed suffix) would turn the index check green rather than red.
    """
    discovered = _discovered_examples()
    assert discovered, f"no *.py found under {_EXAMPLES_DIR} — the glob is broken"


def test_index_names_every_example():
    """Every script in examples/ appears in the README's index section."""
    discovered = _discovered_examples()
    indexed = _indexed_examples()

    missing = discovered - indexed
    assert not missing, (
        f"these examples are not named in the README's {_INDEX_HEADING!r} section: "
        f"{sorted(missing)}. Add each as `examples/<name>.py` with a one-line "
        f"description of the single feature it demonstrates."
    )


def test_index_names_no_missing_example():
    """The index names nothing that examples/ does not contain.

    The reverse containment, so deleting or renaming a script fails here instead
    of leaving a dead entry pointing a reader at a file that is gone.
    """
    indexed = _indexed_examples()

    # Compared against every file on disk, not just the runnable ones: an index
    # entry naming a private module that genuinely exists is odd but not stale,
    # while one naming a file that is gone is exactly what this catches.
    stale = indexed - _files_on_disk()
    assert not stale, (
        f"the README's {_INDEX_HEADING!r} section names examples that do not exist: "
        f"{sorted(stale)}. Remove the entry or restore the script."
    )
