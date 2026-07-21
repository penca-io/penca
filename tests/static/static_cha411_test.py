"""CHA-411: snapshot reads route through a SnapshotTableProvider.

Behavior-preserving Phase-3 refactor — these are pure source-input checks
(no Docker) under ``tests/static/`` (run via ``just static-test cha411``,
also wired into ``just check``). They pin the *structural* half of CHA-411's
acceptance via three source greps (one per test method):

1. The ``stream_merged`` Phase-3 hot path no longer reads snapshot segments
   directly via ``dl.read_snapshot_segment``
   (``test_hot_path_does_not_read_snapshot_segment_directly``).
2. It drives the snapshot scan via ``DlDriver::scan_snapshot``
   (``test_hot_path_calls_scan_snapshot``).
3. The post-read Arrow exclusion/residual passes (``filter_snapshot_batch`` /
   ``apply_physical_filter``) are gone
   (``test_arrow_exclusion_and_residual_passes_removed``).

The remaining "no ``output_ordering`` advertised yet" criterion (the CHA-410
boundary) is a runtime guard, not a source grep — it lives in the ``penca-dl``
provider unit test ``snapshot_provider_scan_advertises_no_ordering``.

Red baseline before the fix: ``stream_pruned_snapshot_segments`` calls
``dl.read_snapshot_segment``; ``scan_snapshot`` is not called in production;
``filter_snapshot_batch`` + ``apply_physical_filter`` are the
exclusion/residual mechanism.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MERGE_LIB = REPO_ROOT / "crates/penca-merge/src/lib.rs"
MERGE_SRC = REPO_ROOT / "crates/penca-merge/src"

TEST_SPLIT = "#[cfg(test)]"


def _production(path: Path) -> str:
    """Source text before the first ``#[cfg(test)]`` module (production code)."""
    text = path.read_text(encoding="utf-8")
    idx = text.find(TEST_SPLIT)
    return text if idx == -1 else text[:idx]


class TestSnapshotProviderRouting:
    def test_hot_path_does_not_read_snapshot_segment_directly(self):
        prod = _production(MERGE_LIB)
        assert ".read_snapshot_segment(" not in prod, (
            "stream_merged Phase-3 must not call dl.read_snapshot_segment directly "
            "(CHA-411 routes through SnapshotTableProvider via scan_snapshot)"
        )

    def test_hot_path_calls_scan_snapshot(self):
        prod = _production(MERGE_LIB)
        # Anchor to the call shape (``scan_snapshot(``) like the negative guards,
        # so a doc-comment mention alone would not satisfy it.
        assert "scan_snapshot(" in prod, (
            "stream_merged Phase-3 must drive the snapshot scan via "
            "DlDriver::scan_snapshot"
        )

    def test_arrow_exclusion_and_residual_passes_removed(self):
        # Match call sites / definitions (``sym(``) rather than bare mentions,
        # so a doc comment that names the removed function (e.g. the
        # build_cold_snapshot_scan rustdoc) is not a false offender. Scope to the
        # production slice per file — a future regression unit test that merely
        # references the removed helper must not trip this guard either.
        offenders: list[str] = []
        for path in sorted(MERGE_SRC.rglob("*.rs")):
            text = _production(path)
            for sym in ("filter_snapshot_batch", "apply_physical_filter"):
                if f"{sym}(" in text:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}: {sym}")

        assert not offenders, (
            "the post-read Arrow exclusion/residual passes must be gone — the "
            "anti-join + residual now live in the snapshot scan plan:\n"
            + "\n".join(offenders)
        )
