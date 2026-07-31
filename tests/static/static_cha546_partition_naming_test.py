"""CHA-546: every branch-scoped statement targets the branch's partition.

The 6 tx-log tables already resolve by partition name. The 8 metadata
tables did not — call sites named the catalog-wide parent and let
Postgres route on ``branch_uuid``. Naming the parent takes a lock on the
parent, so a writer deadlocks branch teardown (teardown walks
leaf-to-parent, the writer parent-to-leaf) and compact's ``SELECT ... FOR
UPDATE OF seg`` holds ``ROW SHARE`` on the parent across its whole cold
read and merged write — long enough that ``DeleteBranch`` trips
``lock_branch_teardown_partitions``' 5s ``lock_timeout`` in ordinary
operation. Naming a partition takes no parent lock at all, which is what
makes this a fix rather than a tidy-up.

Two exceptions are enumerated, not incidental:

* DDL in ``crates/penca-db/src/dialect/pg.rs`` must name parents to
  create, attach, and drop partitions.
* ``compact.rs``'s ``segment_delete_set`` refcount gate probes three
  parents catalog-wide on purpose (CHA-531) — carry-forward crosses fork
  edges, so a branch-scoped probe would delete a segment another branch's
  snapshot still references.

The ``compact.rs`` allowance is scoped to the two gate functions rather
than the whole file, so the three branch-scoped sites this ticket
converted cannot come back.

These are pure source-input checks — no Docker. They live under
``tests/static/`` (run via ``just static-test``, also wired into
``just check``).
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CRATES = REPO_ROOT / "crates"

# The 8 partitioned metadata tables' PARENT-name helpers. Their
# ``*_partition`` counterparts all exist in
# ``crates/penca-core/src/naming/tables.rs`` and are what call sites use.
# Deliberately excludes ``tx_log_persist_segment_metadata`` (CHA-507) and
# ``segment_delete_set`` (CHA-531) — both are catalog-wide and
# unpartitioned, so they have no partition to target.
PARENT_HELPERS = (
    "table_persist_metadata_table",
    "table_persist_segment_metadata_table",
    "table_purge_metadata_table",
    "table_snapshot_metadata_table",
    "table_snapshot_segment_metadata_table",
    "compact_segment_metadata_table",
    "table_snapshot_index_metadata_table",
    "table_snapshot_segment_index_metadata_table",
)

_PARENT_RE = re.compile(r"\b(" + "|".join(PARENT_HELPERS) + r")\b")
_FN_RE = re.compile(
    r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)"
)

# Files where naming a parent is the point.
UNRESTRICTED = frozenset(
    {
        "crates/penca-core/src/naming/tables.rs",  # the definitions
        "crates/penca-core/src/naming/mod.rs",  # the re-exports
        "crates/penca-db/src/dialect/pg.rs",  # partition DDL
    }
)

# CHA-531's catalog-wide refcount gate. Both build their ``NOT EXISTS``
# probes through ``segment_delete_set_referenced_predicate``, which takes
# the table names as arguments rather than deriving them.
GATE_FILE = "crates/penca-storage-meta/src/compact.rs"
GATE_FUNCTIONS = frozenset(
    {
        "eligible_segment_delete_set_rows",
        "reap_referenced_segment_delete_set_rows",
    }
)


def _rust_sources() -> list[Path]:
    return sorted(CRATES.rglob("*.rs"))


def _violations() -> list[str]:
    offenders: list[str] = []
    for path in _rust_sources():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel in UNRESTRICTED:
            continue

        enclosing = ""
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            fn_match = _FN_RE.search(line)
            if fn_match:
                enclosing = fn_match.group(1)

            hit = _PARENT_RE.search(line)
            if not hit:
                continue

            if rel == GATE_FILE and enclosing in GATE_FUNCTIONS:
                continue

            offenders.append(
                f"{rel}:{lineno}: {hit.group(1)} (in {enclosing or '<top level>'})"
            )

    return offenders


class TestPartitionDirectNaming:
    def test_no_branch_scoped_statement_names_a_metadata_parent(self):
        offenders = _violations()
        assert not offenders, (
            f"{len(offenders)} site(s) build a metadata table name from a "
            "parent-name helper. Branch-scoped statements must call the "
            "matching `*_partition(&catalog, &branch)` helper instead "
            "(CHA-546). Allowed: DDL in penca-db/src/dialect/pg.rs and the "
            "catalog-wide segment_delete_set refcount gate in "
            f"{GATE_FILE}::{{{', '.join(sorted(GATE_FUNCTIONS))}}}.\n"
            + "\n".join(offenders)
        )

    def test_refcount_gate_still_probes_parents_catalog_wide(self):
        # The converse guard: narrowing CHA-531's gate to a partition would
        # let the sweep delete a segment a forked branch's carried-forward
        # snapshot still references, and the check above would happily pass.
        text = (REPO_ROOT / GATE_FILE).read_text(encoding="utf-8")
        for fn in sorted(GATE_FUNCTIONS):
            body = _function_body(text, fn)
            assert body is not None, f"{GATE_FILE} no longer defines `{fn}`"
            assert _PARENT_RE.search(body), (
                f"{GATE_FILE}::{fn} must keep naming the catalog-wide parents "
                "— the segment_delete_set refcount gate spans fork edges "
                "(CHA-531). A branch-scoped probe reintroduces the bug where "
                "a parent's segment is deleted out from under a child's "
                "carried-forward snapshot."
            )

    def test_no_open_cha546_todos(self):
        # The comments in pg.rs::lock_branch_teardown_partitions and
        # write/mod.rs describe the parent-naming contention as the root
        # cause and defer it to this ticket. Once the conversion lands they
        # describe something that no longer happens.
        offenders: list[str] = []
        for path in _rust_sources():
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                if "TODO(CHA-546)" in line:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")

        assert not offenders, (
            "TODO(CHA-546) markers must be retired along with the fix:\n"
            + "\n".join(offenders)
        )


def _function_body(text: str, name: str) -> str | None:
    """Return the brace-balanced body of ``fn <name>``, or None if absent."""
    match = re.search(
        r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+" + re.escape(name) + r"\b",
        text,
    )
    if match is None:
        return None

    start = text.find("{", match.end())
    if start == -1:
        return None

    depth = 0
    for idx in range(start, len(text)):
        if text[idx] == "{":
            depth += 1
        elif text[idx] == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]

    return None
