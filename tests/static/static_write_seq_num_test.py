"""[CHA-431] Structural guards on the merge-resolution surface.

Merge-on-read resolution must order by the seq axes only — ``(commit_seq_num,
write_seq_num)`` — with ``written_at_micros`` gone from the data-log resolution +
cold-schema surface. These greps are RED until the IMPL5/6/7 ordering flips and
IMPL9's ``written_at_micros`` removal land:

* the merge kernel (``merge_resolution.rs``) has no ``write_seq_num`` reference
  yet, and
* ``written_at_micros`` is still present across the resolution surface.

Run via ``just static-test write_seq_num``.
"""

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Files where ``written_at_micros`` exists ONLY as the (now-retired) merge
# tiebreaker / per-row cold column — distinct from the lifecycle/GC
# grace-window ``written_at_micros`` on metadata tables (a different column,
# which must remain), so these can legitimately assert a zero count.
RESOLUTION_SURFACE = [
    "crates/penca-sql/src/merge_resolution.rs",
    "crates/penca-dl/src/dialect.rs",
    "crates/penca-merge/src/sql.rs",
    "crates/penca-merge/src/schema.rs",
    "crates/penca-db/src/resolve.rs",
    "crates/penca-storage-hot/src/merge.rs",
]

MERGE_KERNEL = "crates/penca-sql/src/merge_resolution.rs"


def _count(needle: str, rel: str) -> int:
    return (REPO / rel).read_text().count(needle)


class TestMergeOrdersBySeqOnly:
    def test_kernel_uses_write_seq_num(self):
        assert _count("write_seq_num", MERGE_KERNEL) > 0, (
            "the merge kernel (merge_resolution.rs) must order on write_seq_num "
            "as the within-tx secondary axis"
        )

    def test_written_at_micros_gone_from_resolution_surface(self):
        offenders = {}
        for rel in RESOLUTION_SURFACE:
            count = _count("written_at_micros", rel)
            if count:
                offenders[rel] = count

        assert not offenders, (
            "written_at_micros must be fully removed from the data-log "
            f"resolution + cold-schema surface; still present in: {offenders}"
        )
