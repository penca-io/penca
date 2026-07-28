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
# anchor, so it is pinned in one place and both assertions read it.
_INDEX_HEADING = "## Examples"

# Matches an `examples/<stem>.py` mention anywhere in the index section, however
# it is decorated — bare, in backticks, or as a markdown link target:
#   "`examples/oltp_demo.py`" -> "oltp_demo.py"
#   "[point lookup](examples/oltp_demo.py)" -> "oltp_demo.py"
# Deliberately not anchored to a list-item or table-cell shape: the index's
# formatting is the author's call, and pinning it here would make every
# cosmetic README edit a test failure.
_EXAMPLE_MENTION = re.compile(r"examples/([A-Za-z0-9_]+\.py)")


def _index_section() -> str:
    """The README text from the examples-index heading to the next h2.

    Scoped to the section rather than searched over the whole README: a script
    named in some unrelated paragraph (the Quick start's run command, say) would
    otherwise satisfy the index assertion without the index listing it at all.
    """
    readme = _README.read_text(encoding="utf-8")
    assert _INDEX_HEADING in readme, (
        f"README.md has no examples index section (expected a {_INDEX_HEADING!r} "
        f"heading). It is the entry point for the examples family — see CHA-527."
    )

    after = readme.split(_INDEX_HEADING, 1)[1]
    # Split on the next h2 so per-script `###` subsections stay inside the
    # section; splitting on a bare "#" would cut at the first h3 instead.
    return after.split("\n## ", 1)[0]


def _discovered_examples() -> set[str]:
    return {path.name for path in _EXAMPLES_DIR.glob("*.py")}


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
    indexed = set(_EXAMPLE_MENTION.findall(_index_section()))

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
    discovered = _discovered_examples()
    indexed = set(_EXAMPLE_MENTION.findall(_index_section()))

    stale = indexed - discovered
    assert not stale, (
        f"the README's {_INDEX_HEADING!r} section names examples that do not exist: "
        f"{sorted(stale)}. Remove the entry or restore the script."
    )
