//! Visibility predicate for merge-on-read.

/// Which snapshot a merge-on-read draws from. Distinct from the cold-tier
/// "snapshot segment" / Lance snapshot files — `ReadSnapshot` governs the
/// merge-on-read visibility predicate (the upper bound on the commit axis
/// and the optional OR'd own-writes clause), not on-disk materialization.
///
/// The read pins a point on EITHER commit axis — wall-clock
/// (`AsOfMicros`) or the gapless commit-order serial (`AsOfSeq`) — and the
/// merge always *orders* internally by `commit_seq_num` (the authoritative
/// total order; `commit_micros` can tie under concurrency). The
/// filter axis is whichever the caller passed; the order axis is always
/// seq.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSnapshot {
    /// Point-in-time on the commit-timestamp axis: `commit_micros
    /// <= ts`. The default read path pins this to a captured `pg_now`
    /// — there is no unbounded read variant.
    AsOfMicros(i64),
    /// Point-in-time on the commit-order axis: `commit_seq_num <=
    /// seq`. Exact — unlike `commit_micros`, the serial never ties.
    AsOfSeq(i64),
    /// Like [`ReadSnapshot::AsOfSeq`] for *planning* (an exact
    /// `commit_seq_num <= seq` pin on the per-branch commit frontier), but flagged
    /// as the DEFAULT "read latest" resolution rather than an explicit seq
    /// time-travel. The snapshot-list cache is keyed on the resolved snapshot's
    /// `W_snap`, so every read shape shares one content-addressed entry per
    /// snapshot version — this flag does not gate cache eligibility. The
    /// immutable cold baseline is all the cache holds; the hot change-log is
    /// always read fresh.
    LatestSeq(i64),
    /// Snapshot isolation for an open tx at `began_at_seq_num` plus
    /// read-your-own-writes for `tx_uuid`. Visibility predicate:
    ///
    /// ```text
    /// (commit_seq_num < began_at_seq_num) OR (tx_uuid = open_tx_uuid)
    /// ```
    ///
    /// The strict `<` excludes txs that committed at/after this tx's BEGIN
    /// frontier (snapshot isolation). `began_at_seq_num` is the
    /// `commit_tx_log_seq_num` counter value captured at BEGIN — the
    /// next-to-allocate frontier, so `< began_at_seq_num` is exactly
    /// "committed before this tx began". The OR clause picks up this tx's
    /// own uncommitted writes from the upsert/delete logs (where
    /// `commit_micros IS NULL`).
    OpenTx {
        began_at_seq_num: i64,
        tx_uuid: uuid::Uuid,
    },
}

impl ReadSnapshot {
    /// The open tx's uuid on an [`ReadSnapshot::OpenTx`] read, else
    /// `None`. Callers thread it into `QueryManager::plan` so the phase-1 hot
    /// existence gate includes the tx's own RYOW writes — derive it from the
    /// snapshot rather than assuming a read is non-open-tx.
    pub fn open_tx_uuid(&self) -> Option<String> {
        match self {
            ReadSnapshot::OpenTx { tx_uuid, .. } => Some(tx_uuid.to_string()),
            ReadSnapshot::AsOfMicros(_) | ReadSnapshot::AsOfSeq(_) | ReadSnapshot::LatestSeq(_) => {
                None
            }
        }
    }

    /// Inclusive `commit_micros` upper bound for cold-segment
    /// SELECTION that `QueryManager::plan` consumes.
    ///
    /// - [`ReadSnapshot::AsOfMicros`] → `ts` (the plan is inclusive on
    ///   `<= ts`, matching this variant's semantics).
    /// - [`ReadSnapshot::AsOfSeq`] / [`ReadSnapshot::OpenTx`] → `i64::MAX`.
    ///   These pin the *seq* axis, not `committed_at`. Cold segment
    ///   selection still tier-fences on the planner's `hot_min`
    ///   (`committed_at < hot_min`), so `i64::MAX` selects every cold
    ///   segment below the fence and the per-row `commit_seq_num` predicate in
    ///   the cold merge SQL does the visibility (`plan_commit_seq_upper` —
    ///   `began_at_seq_num - 1` for OpenTx). (Pruning these segments on
    ///   `min/max_commit_seq_num` is a later optimization, not correctness.)
    ///
    /// Always a concrete bound — there is no unbounded read variant.
    pub fn plan_as_of_micros(&self) -> i64 {
        match self {
            ReadSnapshot::AsOfMicros(ts) => *ts,
            ReadSnapshot::AsOfSeq(_) | ReadSnapshot::LatestSeq(_) | ReadSnapshot::OpenTx { .. } => {
                i64::MAX
            }
        }
    }

    /// Inclusive `commit_seq_num` upper bound for cold-segment SELECTION on
    /// the commit-order axis — the seq sibling
    /// of [`ReadSnapshot::plan_as_of_micros`].
    ///
    /// - [`ReadSnapshot::AsOfSeq`] → `Some(n)`: `plan` skips cold segments
    ///   whose `min_commit_seq_num > n` (every row past the bound), on top of
    ///   the `committed_at` tier fence.
    /// - [`ReadSnapshot::AsOfMicros`] → `None`: selection is on the
    ///   `committed_at` axis (`plan_as_of_micros`), no seq skip.
    /// - [`ReadSnapshot::OpenTx`] → `Some(began_at_seq_num - 1)`: snapshot
    ///   isolation on the seq axis — cold serves only `commit_seq_num <
    ///   began_at_seq_num`, matching the hot path's predicate
    ///   (`read_snapshot_clause`). This read-side bound is the ONLY thing
    ///   enforcing OpenTx isolation against cold: without it, cold would serve
    ///   rows committed *after* the tx began once they reach cold.
    pub fn plan_commit_seq_upper(&self) -> Option<i64> {
        match self {
            ReadSnapshot::AsOfSeq(n) | ReadSnapshot::LatestSeq(n) => Some(*n),
            ReadSnapshot::OpenTx {
                began_at_seq_num, ..
            } => Some(began_at_seq_num - 1),
            ReadSnapshot::AsOfMicros(_) => None,
        }
    }

    /// Tighten this snapshot's upper bound by the planner's `hot_max`
    /// (inclusive `commit_micros`). Only the `committed_at` axis is
    /// tightenable against a `committed_at` `hot_max`: the planner may
    /// return a tighter committed_at upper bound than the user requested
    /// (e.g. the latest committed row in hot is older than the user's
    /// as-of). For the seq axes (`AsOfSeq` / `OpenTx`) the bound is an
    /// exact `commit_seq_num` predicate, so there is nothing to intersect
    /// against a `committed_at` `hot_max` — return self.
    pub fn tighten_for_hot(&self, hot_max: Option<i64>) -> ReadSnapshot {
        match self {
            ReadSnapshot::AsOfMicros(ts) => match hot_max {
                Some(hot_max) => ReadSnapshot::AsOfMicros((*ts).min(hot_max)),
                None => self.clone(),
            },
            ReadSnapshot::AsOfSeq(_) | ReadSnapshot::LatestSeq(_) | ReadSnapshot::OpenTx { .. } => {
                self.clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tx(began_at_seq_num: i64) -> ReadSnapshot {
        ReadSnapshot::OpenTx {
            began_at_seq_num,
            tx_uuid: uuid::Uuid::nil(),
        }
    }

    // The hot existence gate threads `open_tx_uuid` from the snapshot, so the
    // accessor must return the tx uuid ONLY for OpenTx — a regression to `None`
    // here re-opens the cold-DDL RYOW gap on the system-table axis.
    #[test]
    fn open_tx_uuid_some_only_for_open_tx() {
        let tx = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        assert_eq!(
            ReadSnapshot::OpenTx {
                began_at_seq_num: 5,
                tx_uuid: tx,
            }
            .open_tx_uuid(),
            Some(tx.to_string())
        );
        assert!(ReadSnapshot::AsOfMicros(100).open_tx_uuid().is_none());
        assert!(ReadSnapshot::AsOfSeq(7).open_tx_uuid().is_none());
    }

    // OpenTx is the load-bearing case — it carries `began_at_seq_num - 1` so
    // cold serves only `commit_seq_num < began_at_seq_num` (snapshot isolation).
    // The hot path applies the same `< began_at_seq_num` predicate
    // (`read_snapshot_clause`).
    #[test]
    fn plan_commit_seq_upper_open_tx_is_began_minus_one() {
        assert_eq!(open_tx(100).plan_commit_seq_upper(), Some(99));
        // An open tx that began at frontier 0 sees nothing committed (cold
        // serves `<= -1`), which the genesis base (`SNAPSHOT_SEQ_GENESIS = -1`)
        // renders as empty.
        assert_eq!(open_tx(0).plan_commit_seq_upper(), Some(-1));
    }

    #[test]
    fn plan_commit_seq_upper_as_of_seq_passes_through() {
        assert_eq!(ReadSnapshot::AsOfSeq(42).plan_commit_seq_upper(), Some(42));
    }

    #[test]
    fn plan_commit_seq_upper_as_of_micros_is_none() {
        // AsOfMicros fences on the committed_at axis, not seq.
        assert_eq!(
            ReadSnapshot::AsOfMicros(1_000).plan_commit_seq_upper(),
            None
        );
    }
}
