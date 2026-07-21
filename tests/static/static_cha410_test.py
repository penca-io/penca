"""CHA-410 / CHA-431: persist-tier ``output_ordering`` honesty contract — structural guards.

The advertised ``[commit_seq_num ASC, write_seq_num ASC]`` ordering (CHA-431 extended
CHA-410's ``[commit_seq_num ASC]`` to the full total version order) on
``PersistTableProvider`` is only honest if the concatenated persist read stream
is genuinely non-decreasing in ``(commit_seq_num, write_seq_num)``. That requires two
cooperating halves:

1. **Within-segment sort** — ``chunk_persist_batch`` sorts each cold batch by the
   composite ``(commit_seq_num, write_seq_num)`` before chunking. Guarded by the Rust
   unit test ``persist_chunks_are_commit_seq_num_write_seq_num_ordered`` (penca-api).
2. **Cross-segment listing order** — the cold read-plan persist query lists
   segments by ``min_commit_seq_num`` (``chunk_idx`` tiebreak; within one persist op
   ``chunk_idx`` already follows the composite sort, across ops the ``commit_seq_num``
   ranges are disjoint). This is a SQL-level ordering built inside an async,
   PG-bound method, so it has no Rust unit test (roborev finding on commit
   3784138). These pure source greps pin it — run via ``just static-test
   cha410``, also wired into ``just check``.

If the advertisement is ever advertised while either half regresses, DataFusion
elides a real ``SortExec`` and silently returns mis-ordered / mis-merged rows —
so these guards protect a correctness invariant, not a cosmetic one.

Red baseline before CHA-410: the persist read-plan query ordered by
``max_tx_commit_micros`` (ties unordered) and the provider advertised no
ordering.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
# CHA-472 (ADR 0028) relocated the read-plan assembly from
# penca-storage-meta/src/plan.rs onto QueryManager.
PLAN_RS = REPO_ROOT / "crates/penca-api/src/query/meta_plan.rs"
PROVIDER_RS = REPO_ROOT / "crates/penca-dl/src/provider.rs"

TEST_SPLIT = "#[cfg(test)]"


def _production(path: Path) -> str:
    """Source text before the first ``#[cfg(test)]`` module (production code).

    Mirrors ``static_cha411_test.py`` so a reference inside a test module
    cannot satisfy a guard meant to pin a production code path.
    """
    text = path.read_text(encoding="utf-8")
    idx = text.find(TEST_SPLIT)
    return text if idx == -1 else text[:idx]


class TestPersistReadPlanOrdering:
    def test_persist_segment_list_ordered_by_commit_seq_num(self):
        prod = _production(PLAN_RS)
        assert "ORDER BY seg.min_commit_seq_num, seg.chunk_idx" in prod, (
            "the cold read-plan persist-segment query must ORDER BY "
            "seg.min_commit_seq_num so the concatenated PersistTableProvider stream "
            "is globally commit_seq_num-ordered (CHA-410 honesty contract)"
        )

    def test_persist_segment_list_not_ordered_by_committed_at(self):
        prod = _production(PLAN_RS)
        assert "ORDER BY seg.max_tx_commit_micros, seg.chunk_idx" not in prod, (
            "the persist read-plan ORDER BY must use the gapless commit_seq_num axis, "
            "not commit_micros (ties unordered) — see CHA-410"
        )


class TestPersistProviderAdvertisesOrdering:
    def test_provider_advertises_commit_seq_num_ordering(self):
        prod = _production(PROVIDER_RS)
        # The advertisement is built over the commit_seq_num column and passed as
        # the StreamingTableExec projected_output_ordering. Anchor to the call
        # shape so a comment mention alone does not satisfy it, and grep only
        # production text so a test-module reference cannot satisfy it either.
        assert 'col("commit_seq_num", &output_schema)' in prod, (
            "PersistTableProvider::scan must build its output_ordering over the "
            "commit_seq_num column (CHA-410)"
        )
