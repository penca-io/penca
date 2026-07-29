//! Per-scope compact-wave planning for `compact_one_scope`. Walks the
//! unsealed scope rows, identifies the prior active merged file, and
//! decides — fold / cascade-seal / standalone-seal — how the wave
//! makes state-changing progress.

use std::collections::{HashMap, HashSet};

use sqlx::Row;
use sqlx::postgres::PgRow;

/// Per-input bookkeeping for one compact wave, consumed by
/// `compact_one_scope`.
pub(super) struct CompactInputMeta {
    pub(super) segment_uuid: String,
    pub(super) row_count: i64,
    pub(super) old_uri: String,
}

/// Result of [`plan_wave`].
///
/// - `input_indices`: indexes into the caller's row slice for the rows
///   that compose the new merged file, in canonical (post-merge slice)
///   order. The first index's parent (`table_persist_uuid` for persist,
///   `table_snapshot_uuid` for snapshot) is the parent under which the
///   merged URI is generated. Empty for a seal-only wave (no new file
///   produced, sealing alone is the progress).
/// - `seal_indices`: indexes of rows that must have `is_sealed` flipped
///   to `true` in the same merge tx. Includes the prior active's rows
///   (seal-and-start-new), cascade-sealed unwritable accumulators, and
///   oversized standalones sealed in place.
pub(super) struct WavePlan {
    pub(super) input_indices: Vec<usize>,
    pub(super) seal_indices: Vec<usize>,
}

/// Plan one compact wave on a single scope's unsealed segment rows.
///
/// The active merged file is the one `object_uri` appearing in >1 row: by the
/// active+sealed invariant, uncompacted rows have unique URIs.
///
/// Invariant: every `Some(plan)` commits state-changing progress — a new
/// merged file (`input_indices.len() ≥ 2`), ≥1 sealed row, or both. Without
/// it a scope stalls forever, `plan_wave` returning `None` on a non-empty
/// unsealed set.
///
/// See `docs/algorithms.md` § Compact (cold → cold) for the full
/// algorithm derivation, invariants, and wave-vs-cycle layering.
pub(super) fn plan_wave<F>(rows: &[PgRow], max_segment_bytes: i64, uri_of: F) -> Option<WavePlan>
where
    F: Fn(&PgRow) -> String,
{
    // Projecting out of the opaque `PgRow`s here keeps the fold algorithm
    // unit-testable without constructing `PgRow`s.
    let uris: Vec<String> = rows.iter().map(&uri_of).collect();
    let sizes: Vec<i64> = rows
        .iter()
        .map(|r| r.try_get("size_bytes").ok().flatten().unwrap_or(0i64))
        .collect();
    plan_wave_projected(&uris, &sizes, max_segment_bytes)
}

/// Pure fold core of [`plan_wave`], operating on per-row projections:
/// `uris[i]` is the row's `object_uri` (for active-file grouping) and
/// `sizes[i]` its recorded `size_bytes`. `uris` and `sizes` are parallel
/// and equal-length.
///
/// `sizes` MUST be the uncompressed in-memory Arrow footprint — the unit
/// `max_segment_bytes` denominates. Feeding the (smaller) on-disk size makes
/// the same byte budget over-fold past the cap.
fn plan_wave_projected(uris: &[String], sizes: &[i64], max_segment_bytes: i64) -> Option<WavePlan> {
    if uris.is_empty() {
        return None;
    }

    // Active = the URI appearing in >1 row; at most one such by the
    // active+sealed invariant.
    let mut uri_counts: HashMap<&str, usize> = HashMap::new();
    for u in uris {
        *uri_counts.entry(u.as_str()).or_insert(0) += 1;
    }
    debug_assert!(
        uri_counts.values().filter(|&&c| c > 1).count() <= 1,
        "active+sealed invariant violated: multiple shared URIs in unsealed set",
    );
    let active_uri: Option<&str> = uri_counts
        .iter()
        .find_map(|(u, c)| if *c > 1 { Some(*u) } else { None });

    let mut active_indices: Vec<usize> = Vec::new();
    let mut active_size: i64 = 0;
    if let Some(u) = active_uri {
        for (i, row_uri) in uris.iter().enumerate() {
            if row_uri == u {
                active_indices.push(i);
                active_size += sizes[i];
            }
        }
    }

    // Uncompacted = everything else, in input order.
    let active_set: HashSet<usize> = active_indices.iter().copied().collect();
    let uncompacted_indices: Vec<usize> = (0..uris.len())
        .filter(|i| !active_set.contains(i))
        .collect();

    let mut current: Vec<usize> = active_indices.clone();
    let mut current_size: i64 = active_size;
    let mut seal_indices: Vec<usize> = Vec::new();
    let mut folded: usize = 0;

    for idx in uncompacted_indices.iter().copied() {
        let s: i64 = sizes[idx];
        if current_size + s <= max_segment_bytes {
            current.push(idx);
            current_size += s;
            folded += 1;
        } else if folded >= 1 && current.len() >= 2 {
            // `current` is a writable new active. Stop here so the
            // wave produces exactly one new merged file; remaining
            // uncompacted stays for the next wave.
            break;
        } else if !current.is_empty() {
            // `current` is unwritable as-is — either the prior active
            // alone (folded = 0; rewriting it under a new URI without
            // folds is just an idempotent file copy) or a singleton
            // (1-input "merge" isn't worth a new file). Cascade-seal
            // it and restart with `idx` as the new seed.
            seal_indices.extend(current.iter().copied());
            current.clear();
            current.push(idx);
            current_size = s;
            folded = 1;
        } else {
            // `current` is empty AND `s > max_segment_bytes` —
            // oversized standalone. Seal `idx` in place (it's already
            // at one merged file's worth of bytes; folding anything
            // on top of it would breach) and continue with `current`
            // still empty.
            //
            // Do not delete this arm as unreachable: the per-row chunker caps
            // fresh persist+snapshot writes at `max_segment_bytes`, so no new
            // data reaches it, but legacy oversized rows written before that
            // cap can still be on disk in long-lived environments.
            seal_indices.push(idx);
        }
    }

    if folded == 0 && seal_indices.is_empty() {
        return None;
    }
    if current.len() < 2 {
        if seal_indices.is_empty() {
            // Pure singleton (no prior active, one uncompacted of
            // sub-threshold size) — wait for more uncompacted next
            // wave. A 1-input "merge" isn't worth a new file.
            return None;
        }
        // Seal-only wave: commit sealing as progress; the next wave
        // sees a smaller unsealed set and plans against it.
        return Some(WavePlan {
            input_indices: Vec::new(),
            seal_indices,
        });
    }

    Some(WavePlan {
        input_indices: current,
        seal_indices,
    })
}

#[cfg(test)]
mod tests {
    use super::plan_wave_projected;

    /// Distinct (no-active) URIs for `n` uncompacted rows.
    fn distinct_uris(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("s{i}")).collect()
    }

    fn merged_footprint(sizes: &[i64], input_indices: &[usize]) -> i64 {
        input_indices.iter().map(|&i| sizes[i]).sum()
    }

    #[test]
    fn fold_stops_before_breaching_cap() {
        // [40,40,40] @ cap 100: folds 0+1 (=80), defers 2 (80+40=120 > 100).
        let sizes = vec![40, 40, 40];
        let plan = plan_wave_projected(&distinct_uris(3), &sizes, 100).expect("wave");
        assert_eq!(plan.input_indices, vec![0, 1]);
        assert!(plan.seal_indices.is_empty());
        assert!(merged_footprint(&sizes, &plan.input_indices) <= 100);
    }

    #[test]
    fn fold_packs_all_when_under_cap() {
        // [30,30,30] @ cap 100: all three fold (90 <= 100).
        let sizes = vec![30, 30, 30];
        let plan = plan_wave_projected(&distinct_uris(3), &sizes, 100).expect("wave");
        assert_eq!(plan.input_indices, vec![0, 1, 2]);
        assert!(merged_footprint(&sizes, &plan.input_indices) <= 100);
    }

    #[test]
    fn two_segments_over_cap_together_do_not_merge() {
        // [60,60] @ cap 100: 60+60=120 > 100, so they must NOT merge —
        // the leading 60 cascade-seals, no new file is produced. Were
        // `sizes` the compressed on-disk unit (30 each) this would wrongly
        // fold to one 120-byte segment.
        let sizes = vec![60, 60];
        let plan = plan_wave_projected(&distinct_uris(2), &sizes, 100).expect("wave");
        assert!(plan.input_indices.is_empty());
        assert_eq!(plan.seal_indices, vec![0]);
    }

    #[test]
    fn empty_input_is_none() {
        assert!(plan_wave_projected(&[], &[], 100).is_none());
    }

    #[test]
    fn single_subthreshold_uncompacted_is_none() {
        // One uncompacted segment, no prior active: a 1-input "merge"
        // isn't worth a new file — wait for more next wave.
        let plan = plan_wave_projected(&distinct_uris(1), &[10], 100);
        assert!(plan.is_none());
    }
}
