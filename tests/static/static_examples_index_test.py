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
# Ends at the next h2 OR h3. Stopping only at the h2 swallowed every per-script
# `### examples/<name>.py` deep-dive that sits under this heading, so a script
# with a section but no index row still matched and the index table itself went
# unguarded — the exact silent-omission this file exists to catch.
_SECTION_END = re.compile(r"^#{2,3} ", re.MULTILINE)

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


def _read_readme() -> str:
    return _README.read_text(encoding="utf-8")


def _index_section(readme: str) -> str:
    """The README's index section — the entries themselves, prose only.

    Scoped to the section rather than searched over the whole README: a script
    named in some unrelated paragraph (the Quick start's run command, say) would
    otherwise satisfy the index assertion without the index listing it at all.
    And scoped to stop at the first per-script `###`, so that a script's own
    deep-dive section cannot stand in for an index entry.

    Takes the text rather than reading the file, so the scoping itself is
    testable — see ``test_index_section_stops_before_the_per_script_sections``.
    """
    # Fences come out of the WHOLE text first, before anything else reads it.
    # Stripping them last leaves two ordering hazards: a `## Examples` line
    # inside an earlier fenced block would anchor the search in the wrong place,
    # and a `## ` inside a fence within the section would cut it mid-block,
    # stranding an unbalanced fence that can no longer be stripped.
    stripped = _FENCED_BLOCK.sub("", readme)
    start = _INDEX_START.search(stripped)
    assert start is not None, (
        f"README.md has no examples index section. Expected a line that is "
        f"exactly {_INDEX_HEADING!r}, with nothing after it. It is the entry "
        f"point for the examples family — see CHA-527."
    )

    tail = stripped[start.end() :]
    end = _SECTION_END.search(tail)

    return tail[: end.start()] if end else tail


def _is_runnable_example(name: str) -> bool:
    """A file a reader would actually run, so a README entry is warranted.

    A future `__init__.py` or shared private module is not something a reader
    runs, so the index is not required to name it. See `_indexed_examples` for
    why the stale check deliberately does not apply this.
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
    return set(_EXAMPLE_MENTION.findall(_index_section(_read_readme())))


def test_index_section_stops_before_the_per_script_sections():
    """A script with a deep-dive section but no index row is NOT indexed.

    The index table is the thing under guard. Scoping the section to the next
    h2 swallowed every per-script `### examples/<name>.py` heading that lives
    under this one, so a script could satisfy the containment check on the
    strength of its own section alone — which is exactly the silent omission
    the module docstring says this file exists to prevent.

    Synthetic rather than the real README on purpose: the failure needs a
    script that is sectioned but unlisted, and the README must never actually
    be in that state.
    """
    readme = (
        "## Examples\n\n"
        "| Script | Shows |\n|---|---|\n"
        "| `examples/listed.py` | named in the index |\n\n"
        "### `examples/sectioned.py` — a section, but no index row\n\n"
        "Prose that mentions examples/sectioned.py the way a deep-dive would.\n\n"
        "## Architecture\n"
    )
    named = set(_EXAMPLE_MENTION.findall(_index_section(readme)))
    assert named == {"listed.py"}, (
        f"the index section must stop before the per-script sections, so a "
        f"sectioned-but-unlisted script does not count as indexed; saw {named}"
    )


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
