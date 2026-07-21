//! Pure functions for plan-time / lifecycle watermark computations.
//!
//! Each helper takes plain primitives — no DB fixtures, no I/O — so the
//! input cross product is cheap to enumerate in unit tests. The SQL
//! wrappers that feed them live in `QueryManager::plan` (snapshot
//! picker) and `purge_locked` (purge watermark); see ADR 0019 for the
//! system invariant they preserve.

use uuid::Uuid;

/// Bound the snapshot picker's `as_of` by the plan-time cutoff so the
/// picked snapshot doesn't materialize rows that hot also serves.
///
/// Returns `min(as_of, cutoff - 1)`. Caller is responsible for
/// `cutoff > 0` — at `cutoff == 0` (pre-Persist) the snapshot picker is
/// skipped entirely (cold_storage = None). Since CHA-361 every plan
/// pins a bounded `as_of`, so there is no unset case to fall back from.
pub fn compute_snapshot_picker_as_of(as_of_micros: i64, cutoff_micros: i64) -> i64 {
    as_of_micros.min(cutoff_micros - 1)
}

/// CHA-443 (folds CHA-457): the snapshot seq watermark `W_snap` =
/// `max(prev_snapshot.commit_seq_num, MAX(included persist seg.max_commit_seq_num))`.
///
/// The genesis / first-empty snapshot (no prior, no segments) bases at
/// [`SNAPSHOT_SEQ_GENESIS`] (`-1`) so the empty baseline is selectable by the
/// seq-aware picker for any read (`W_snap <= N` holds for every `N >= 0`) while
/// contributing no rows. A carry-forward-only snapshot (no new persist
/// segments) keeps the prior watermark; otherwise the segment max wins when it
/// advances past the prior baseline.
///
/// The persist segments carry `max_commit_seq_num` inline (CHA-430); the snapshot
/// writer feeds `included_segment_max_seqs` from the
/// `max_persisted_segment_seq_for_window` aggregate over this snapshot's persist
/// window.
pub fn compute_snapshot_seq_watermark(
    prev_snapshot_seq: Option<i64>,
    included_segment_max_seqs: &[i64],
) -> i64 {
    let segments_max = included_segment_max_seqs.iter().copied().max();
    prev_snapshot_seq
        .unwrap_or(SNAPSHOT_SEQ_GENESIS)
        .max(segments_max.unwrap_or(SNAPSHOT_SEQ_GENESIS))
}

/// Seq-axis base for an empty baseline (no rows ≤ this watermark). Below the
/// genesis tx (`commit_seq_num = 0`), so an empty snapshot is picker-selectable for
/// every read and the change-log serves all rows.
pub const SNAPSHOT_SEQ_GENESIS: i64 = -1;

/// `purge_locked`'s strict-advance gate, axis-agnostic on the seq axis
/// (CHA-444 / ADR 0027): given a `candidate` purge target and the
/// `last_purged` watermark already committed, return `Some(candidate)`
/// only when it strictly advances past `last_purged`, else `None` (no
/// new `table_purge_metadata` row to write). Used on both purge axes —
/// committed `Pu` (candidate = the snapshot watermark `W_snap`) and
/// aborted `Pa` (candidate = the abort-counter frontier `F`).
///
/// This helper only encodes the monotone strict-advance rule that keeps
/// `latest_committed_table_purge_seq_watermark` from going backwards or
/// stamping a redundant row; the caller owns reading the candidate and
/// the last-committed watermark off their respective seq columns.
pub fn compute_purge_watermark(candidate: Option<i64>, last_purged: Option<i64>) -> Option<i64> {
    let candidate = candidate?;
    match last_purged {
        Some(lp) if candidate <= lp => None,
        _ => Some(candidate),
    }
}

// ─────────────────────────────────────────────────────────────────────
// CHA-221: branch-scoped tx-log family GC
// ─────────────────────────────────────────────────────────────────────

/// The two seq cutoffs CHA-221 / CHA-444's branch-scoped tx-log family GC
/// needs — one per axis (committed `Pu`, aborted `Pa`). See the field docs.
/// Expired-begin / pure-begin+abort eligibility is wall-clock and computed in
/// the DELETE itself, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeTxLogCutoffs {
    /// Committed GC bound (CHA-444 / ADR 0027): `MIN(Pu over S)` on the
    /// `commit_seq_num` axis — a committed tx with `commit_seq_num <= pu_cutoff` is
    /// purged from hot in every active table, so its commit_tx_log row is GC-safe.
    /// `None` when `S` is empty or any active table has no committed purge
    /// (an unpurged table blocks committed GC).
    pub pu_cutoff: Option<i64>,
    /// Aborted GC bound (CHA-444 / ADR 0027): `MIN(Pa over S)` on the
    /// `aborted_at_seq_num` axis — an aborted tx with `aborted_at_seq_num <
    /// pa_cutoff` has its aborted hot rows cleared in every active table.
    /// `None` when `S` is empty or any active table has no abort purge.
    pub pa_cutoff: Option<i64>,
}

/// Compute the seq cutoffs from the per-`(branch, table)` purge watermarks
/// read as-of `cleanup_started_at_micros`. Each entry is
/// `(table_uuid, Pu, Pa)`; a `None` watermark means that table has not been
/// purged on that axis and **blocks** GC on it (the strongest constraint),
/// so both cutoffs are the branch-min that drops to `None` on any unpurged
/// table or an empty `S`. Caller still runs the DELETE for the expired-begin
/// / pure-begin+abort branches even when both cutoffs are `None`.
pub fn compute_purge_tx_log_cutoffs(
    table_watermarks: &[(Uuid, Option<i64>, Option<i64>)],
) -> PurgeTxLogCutoffs {
    PurgeTxLogCutoffs {
        pu_cutoff: branch_min_watermark(table_watermarks.iter().map(|(_, pu, _)| *pu)),
        pa_cutoff: branch_min_watermark(table_watermarks.iter().map(|(_, _, pa)| *pa)),
    }
}

/// `MIN` over the set, or `None` if the set is empty or any element is `None`
/// (an unpurged table blocks GC on that axis).
fn branch_min_watermark(vals: impl Iterator<Item = Option<i64>>) -> Option<i64> {
    let mut acc: Option<i64> = None;
    for v in vals {
        let v = v?;
        acc = Some(acc.map_or(v, |a: i64| a.min(v)));
    }
    acc
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── compute_snapshot_picker_as_of ────────────────────────────────

    #[test]
    fn snapshot_picker_request_below_cutoff_returns_request() {
        assert_eq!(compute_snapshot_picker_as_of(50, 100), 50);
    }

    #[test]
    fn snapshot_picker_request_equal_cutoff_minus_one_returns_that() {
        assert_eq!(compute_snapshot_picker_as_of(99, 100), 99);
    }

    #[test]
    fn snapshot_picker_request_above_cutoff_clamps_to_cutoff_minus_one() {
        assert_eq!(compute_snapshot_picker_as_of(200, 100), 99);
    }

    #[test]
    fn snapshot_picker_cutoff_one_boundary() {
        // Cutoff = 1 means exactly one possible snapshot point (== 0).
        // The caller skips the snapshot picker entirely at cutoff == 0
        // (pre-Persist), so cutoff = 1 is the smallest "post-Persist"
        // boundary we ever reach.
        assert_eq!(compute_snapshot_picker_as_of(0, 1), 0);
        assert_eq!(compute_snapshot_picker_as_of(5, 1), 0);
    }

    // ── compute_snapshot_seq_watermark (CHA-443 / CHA-457 W_snap) ─────
    //
    // W_snap = max(prev.unwrap_or(GENESIS), MAX(segs).unwrap_or(GENESIS)),
    // GENESIS = -1. Cross product: prev {None, Some} × segs {empty,
    // below / equal / above prior}.

    #[test]
    fn snap_seq_watermark_genesis_empty_is_base() {
        // First-ever snapshot, empty merge: no prior, no segments → GENESIS,
        // so the empty baseline is picker-selectable for any read and adds
        // nothing (change-log serves everything).
        assert_eq!(
            compute_snapshot_seq_watermark(None, &[]),
            SNAPSHOT_SEQ_GENESIS
        );
    }

    #[test]
    fn snap_seq_watermark_carry_forward_keeps_prior() {
        // Carry-forward-only (no new persist segments) keeps the prior watermark.
        assert_eq!(compute_snapshot_seq_watermark(Some(7), &[]), 7);
    }

    #[test]
    fn snap_seq_watermark_first_snapshot_takes_segment_max() {
        // No prior; W_snap is the max seq across the included persist segments.
        assert_eq!(compute_snapshot_seq_watermark(None, &[3, 7, 2]), 7);
    }

    #[test]
    fn snap_seq_watermark_segments_above_prior_win() {
        assert_eq!(compute_snapshot_seq_watermark(Some(5), &[3, 7, 2]), 7);
    }

    #[test]
    fn snap_seq_watermark_prior_above_segments_wins() {
        // Prior baseline already covers a higher seq than this round's segments
        // (carried-forward older rows past the new persist window).
        assert_eq!(compute_snapshot_seq_watermark(Some(9), &[3, 7, 2]), 9);
    }

    #[test]
    fn snap_seq_watermark_equal_prior_and_segment() {
        assert_eq!(compute_snapshot_seq_watermark(Some(7), &[7]), 7);
    }

    #[test]
    fn snap_seq_watermark_genesis_tx_zero_single_segment() {
        // The genesis tx is seq 0; a first snapshot capturing only it has W_snap 0.
        assert_eq!(compute_snapshot_seq_watermark(None, &[0]), 0);
    }

    // ── compute_purge_watermark ──────────────────────────────────────
    //
    // Axis-agnostic strict-advance gate (CHA-444 / ADR 0027): return the
    // candidate only when it strictly advances past the last committed
    // watermark, else `None` (no row to stamp). Exercised across the four
    // (candidate ∈ {None, Some}) × (last_purged ∈ {None, Some})
    // permutations. The same gate runs on both purge axes — committed
    // `Pu` (candidate = `W_snap`) and aborted `Pa` (candidate = the abort
    // frontier `F`).

    #[test]
    fn purge_watermark_eligible_max_none_returns_none() {
        // No candidate watermark to advance to — Purge no-ops regardless
        // of last_purged.
        assert_eq!(compute_purge_watermark(None, None), None);
        assert_eq!(compute_purge_watermark(None, Some(100)), None);
    }

    #[test]
    fn purge_watermark_no_prior_purge_returns_eligible_max() {
        assert_eq!(compute_purge_watermark(Some(100), None), Some(100));
    }

    #[test]
    fn purge_watermark_eligible_max_above_last_purged_returns_eligible_max() {
        assert_eq!(compute_purge_watermark(Some(100), Some(50)), Some(100));
    }

    #[test]
    fn purge_watermark_eligible_max_equal_last_purged_returns_none() {
        // strict-advance: `<= last_purged` ⇒ no-op (avoids stamping a
        // redundant `table_purge_metadata` row).
        assert_eq!(compute_purge_watermark(Some(100), Some(100)), None);
    }

    #[test]
    fn purge_watermark_eligible_max_below_last_purged_returns_none() {
        // Defensive: SQL is monotone, so this shape shouldn't arise
        // in production, but the helper still no-ops rather than
        // walking the watermark backwards.
        assert_eq!(compute_purge_watermark(Some(50), Some(100)), None);
    }

    // ── compute_purge_tx_log_cutoffs ─────────────────────────────────
    //
    // The cross product (CHA-221 v2.1, post-clamp-collapse — 5 cases):
    //
    //   | dimension                       | values                  |
    //   |---------------------------------|-------------------------|
    //   | `table_purged_ats` size         | 0, 1, 2, 3              |
    //   | per-table `purged_at` shape     | None (no purge row),    |
    //   |                                 |   Some(>0)              |
    //   | 3-table ordering                | invariant under         |
    //   |                                 |   permutation           |
    //   | mixed None + Some                | None dominates min     |
    //
    // The `cleanup_started_at_micros` clamp from v1's cross product
    // is gone — that bound is now baked into the upstream as-of
    // filter on `table_purge_metadata.commit_micros`
    // (`tx_table_log_purge_watermarks_for_branch`), not into this
    // pure helper.

    fn uuid_n(n: u8) -> Uuid {
        let mut bytes = [0u8; 16];
        bytes[0] = n;
        Uuid::from_bytes(bytes)
    }

    fn rows_from(
        triples: &[(u8, Option<i64>, Option<i64>)],
    ) -> Vec<(Uuid, Option<i64>, Option<i64>)> {
        triples
            .iter()
            .map(|(n, pu, pa)| (uuid_n(*n), *pu, *pa))
            .collect()
    }

    #[test]
    fn cutoffs_empty_branch_is_none() {
        let c = compute_purge_tx_log_cutoffs(&[]);
        assert_eq!(c.pu_cutoff, None);
        assert_eq!(c.pa_cutoff, None);
    }

    #[test]
    fn cutoffs_unpurged_table_blocks_only_that_axis() {
        // A table never purged on an axis (`None`) blocks GC on that axis,
        // independently of the other.
        let c = compute_purge_tx_log_cutoffs(&rows_from(&[(1, None, Some(7))]));
        assert_eq!(c.pu_cutoff, None);
        assert_eq!(c.pa_cutoff, Some(7));
    }

    #[test]
    fn cutoffs_single_table_returns_its_watermarks() {
        let c = compute_purge_tx_log_cutoffs(&rows_from(&[(1, Some(50), Some(9))]));
        assert_eq!(c.pu_cutoff, Some(50));
        assert_eq!(c.pa_cutoff, Some(9));
    }

    #[test]
    fn cutoffs_multi_table_branch_min_per_axis() {
        let c = compute_purge_tx_log_cutoffs(&rows_from(&[
            (1, Some(30), Some(8)),
            (2, Some(50), Some(3)),
        ]));
        assert_eq!(c.pu_cutoff, Some(30));
        assert_eq!(c.pa_cutoff, Some(3));
    }

    #[test]
    fn cutoffs_permutation_invariant() {
        let a = compute_purge_tx_log_cutoffs(&rows_from(&[
            (1, Some(10), Some(1)),
            (2, Some(20), Some(2)),
            (3, Some(30), Some(3)),
        ]));
        let b = compute_purge_tx_log_cutoffs(&rows_from(&[
            (3, Some(30), Some(3)),
            (1, Some(10), Some(1)),
            (2, Some(20), Some(2)),
        ]));
        assert_eq!(a, b);
        assert_eq!(a.pu_cutoff, Some(10));
        assert_eq!(a.pa_cutoff, Some(1));
    }

    #[test]
    fn cutoffs_none_dominates_min_per_axis() {
        // Each axis is blocked independently by its own `None`.
        let c = compute_purge_tx_log_cutoffs(&rows_from(&[
            (1, None, Some(1_000)),
            (2, Some(1_000_000), None),
        ]));
        assert_eq!(c.pu_cutoff, None);
        assert_eq!(c.pa_cutoff, None);
    }
}
