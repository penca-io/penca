//! Read-plan assembly: the headline `plan()` orchestrator that fans
//! `(catalog, branch, table, as_of)` into a native hot/cold
//! [`penca_core::Plan`] (via the pure `assemble_plan`), plus the snapshot
//! + persist-segment readers it composes.

// The multi-arg SQL-read signatures here plus the `&self` receiver push
// several methods past the lint threshold; allowed at module scope rather
// than per-method.
#![allow(clippy::too_many_arguments)]

use penca_core::{
    BaseColdStorage, ColdStoragePlan, CommitSeqBounds, CommittedAtBounds, Format, HotStoragePlan,
    IndexSidecar, LogKind, PersistPlan, PersistSegment, Plan, SnapshotIndexDef, SnapshotPlan,
    SnapshotSegment, naming,
};
use std::collections::HashSet;
use std::sync::Arc;

use penca_db::driver::{DbDriver, SqlValue};
use penca_dl::list_cache::{CachedSnapshotList, SnapshotListCache};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use penca_storage_meta::helpers::{parse_uuid, qi};
use penca_storage_meta::watermarks;
use penca_storage_meta::{MetadataError, Result, RetentionFloor, SnapshotResult};

use super::QueryManager;
use super::meta_resolve::parse_meta_uuid;

/// A branch's fork lineage — `(parent_branch_uuid, fork_commit_seq_num,
/// fork_commit_micros)`. The position is carried on both axes because reads
/// arrive on both; seq stays the authority for any ceiling.
type BranchLineage = (String, i64, i64);

/// Result of [`Self::read_and_classify_persist_segments`].
struct ClassifiedPersistSegments {
    upsert_segments: Vec<PersistSegment>,
    delete_segments: Vec<PersistSegment>,
}

/// The snapshot a cache-eligible read resolves to, captured by the
/// fused [`QueryManager::hot_min_and_snapshot_pick`]. `w_snap` keys the
/// snapshot-list cache; `table_snapshot_uuid` is threaded into the miss-path
/// segment fetch so it reads THIS snapshot by identity (no re-pick), making the
/// cache KEY and VALUE come from one pick.
struct PickedSnapshot {
    table_snapshot_uuid: Uuid,
    w_snap: i64,
}

impl QueryManager {
    /// Generate a read plan spanning hot and cold storage — the shared 3-tier
    /// read-planner for all 6 callers (`read_data` + the lifecycle snapshot
    /// writer ×2 + the index / table / schema system-table reads).
    ///
    /// 1. Query `hot_min_commit_micros` (the Persist cutoff).
    /// 2. Pre-Persist (`hot_min == 0`): emit `cold_storage = None`; hot owns
    ///    every row (`hot_present = true`). Post-Persist: query the latest
    ///    committed snapshot bounded by `hot_min - 1` (yielding `W_snap`), then
    ///    the phase-1 fused capture — ONE round-trip returning the seq
    ///    `fence = max(Pu, W_snap)` and the hot existence gate `hot_present`
    ///    (see [`Self::phase_one_fence_and_existence`]) — then the committed
    ///    persist segments in `[snapshotted_at + 1, cold_max)`.
    /// 3. Build HotStoragePlan with table names + timestamp filter — but only
    ///    when `hot_present`; an empty hot tier emits `hot_storage = None` so
    ///    the staged all-cold dispatch engages.
    /// 4. Assemble and return the native `Plan`.
    ///
    /// 1 SQL query pre-Persist, 4 SQL queries post-Persist (hot_min, snapshot,
    /// phase-1 fence+existence, persist).
    ///
    /// `cache`, when `Some`, is the W_snap-keyed snapshot-list cache the
    /// snapshot-list read consumes; `None` opts out per service.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            table = %table_uuid,
            as_of_micros = %as_of_micros,
            commit_seq_upper = ?commit_seq_upper,
        ),
    )]
    pub async fn plan(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        table_uuid: &str,
        branch_uuid: &str,
        as_of_micros: i64,
        commit_seq_upper: Option<i64>,
        // The effective retention duration (`None` when retention is
        // disabled). Drives the folded floor read; the CALLER enforces the
        // `as_of < floor` check on the returned coordinates.
        retention_duration_seconds: Option<i64>,
        cache: Option<&SnapshotListCache>,
    ) -> Result<(Plan, Option<RetentionFloor>)> {
        let catalog = parse_uuid(catalog_uuid);
        let table = parse_uuid(table_uuid);
        let branch = parse_uuid(branch_uuid);

        let upsert_table_name = naming::upsert_log_table(&table, &branch);
        let delete_table_name = naming::delete_log_table(&table, &branch);
        let commit_tx_log_table_name = naming::commit_tx_log_partition(&catalog, &branch);

        // The hot/cold visibility cutoff is `persisted_at_micros` (ADR 0019).
        // Pre-Persist (`hot_min == 0`) hot owns every row; `assemble_plan`
        // returns `cold_storage = None` so the merge layer never sees double
        // presence. Post-Persist the cold tier serves `committed_at < hot_min`
        // and hot serves `>= hot_min` — a strict tier partition
        // `PersistPlan.committed_at` makes explicit. Between Persist's commit
        // and Purge's grace-bounded delete the same rows live physically in
        // both tiers, but the plan filter excludes the cold-side rows from hot
        // reads; the universal grace window keeps them present in hot long
        // enough for any concurrent pre-cutoff-pinned plan to finish (ADR 0019,
        // mechanism 2).
        //
        // Fused capture: `hot_min` AND `cache_pick` (the resolved snapshot's
        // IDENTITY + W_snap) in ONE round-trip, so the cache keys on the
        // snapshot version AND the miss-path fetches it by identity — the
        // snapshot is picked exactly once, here.
        let (hot_min, cache_pick, retention_floor, lineage) = self
            .hot_min_and_snapshot_pick(
                driver,
                catalog_uuid,
                branch_uuid,
                table_uuid,
                as_of_micros,
                commit_seq_upper,
                retention_duration_seconds,
            )
            .await?;

        // Pre-Persist (`hot_min == 0`): hot owns every row, so skip the
        // cold fetch AND the phase-1 capture — `assemble_plan` ignores the
        // cold inputs and omits cold_storage, `fence = None` (hot serves all),
        // and `hot_present = true`. Post-Persist: read the snapshot first (it
        // yields `W_snap`), THEN the phase-1 fused capture (which needs
        // `W_snap` to floor the fence), THEN the persist window — bounding BOTH
        // the snapshot picker (`compute_snapshot_picker_as_of`) and the persist
        // window (`cold_max`, recomputed identically inside `assemble_plan` for
        // the committed_at filter) by the single captured `hot_min`, so a
        // concurrent Persist+Snapshot+Purge between these reads can't shift
        // this plan's hot/cold cutoff.
        let (cold, fence, hot_present) = if hot_min == 0 {
            (
                ColdInputs {
                    snapshotted_at_micros: None,
                    snapshot_seq: 0,
                    snapshot_segments: Vec::new(),
                    indexes: Vec::new(),
                    upsert_segments: Vec::new(),
                    delete_segments: Vec::new(),
                },
                None,
                true,
            )
        } else {
            let snapshot_as_of = watermarks::compute_snapshot_picker_as_of(as_of_micros, hot_min);
            let SnapshotResult {
                snapshotted_at_micros,
                // W_snap of the picked snapshot — stamped onto
                // SnapshotPlan.commit_seq_num for the merge, and floors the
                // phase-1 fence below.
                commit_seq_num: snapshot_seq,
                snapshot_segments,
                // Declared user-index defs → SnapshotPlan.indexes.
                indexes,
                // The recorded layout keys drive carry-forward in the snapshot
                // writer, not query planning.
                ..
            } = self
                .list_snapshot_segments(
                    cache,
                    driver,
                    catalog_uuid,
                    branch_uuid,
                    table_uuid,
                    snapshot_as_of,
                    // On an AsOfSeq read this must also bound the snapshot PICK
                    // on `W_snap <= N`, not just the persist residual, or the
                    // baseline leaks rows past the seq cutoff.
                    commit_seq_upper,
                    // The fused pick (identity + W_snap) — the cache key AND
                    // the uuid the miss-path fetches by, so no re-pick.
                    cache_pick,
                )
                .await?;
            // W_snap from the picked snapshot. `None` ⇒ no committed snapshot
            // (SnapshotPlan is omitted anyway), so the genesis base is inert.
            let w_snap = snapshot_seq.unwrap_or(watermarks::SNAPSHOT_SEQ_GENESIS);

            // Phase-1 fused capture: ONE round-trip → `(fence, hot_present)`.
            // `fence = max(Pu, W_snap)` (ADR 0027) MUST be captured once and
            // used for BOTH the hot existence gate and (threaded into
            // `assemble_plan` below) the cold-persist fence — independent
            // per-side `Pu` reads would open the silent-gap window ADR-0019's
            // single-cutoff grace assumes away.
            let (fence, hot_present) = self
                .phase_one_fence_and_existence(
                    driver,
                    catalog_uuid,
                    branch_uuid,
                    table_uuid,
                    &upsert_table_name,
                    &delete_table_name,
                    w_snap,
                )
                .await?;

            // Half-open cold window `[snapshotted_at + 1, cold_max)`. Bounding
            // the fetch by the same `cold_max` `assemble_plan` stamps on the
            // wire-level committed_at keeps the hot/cold partition strict on
            // segments AND rows: a segment with `min_tx >= hot_min` (still
            // served by hot) is excluded before the cold tier ever sees it.
            let cold_max = cold_committed_at_max(as_of_micros, hot_min);
            let from_micros = snapshotted_at_micros.map(|s| s.saturating_add(1));
            // On a seq-axis read the `committed_at` window above is the tier
            // fence (`< hot_min`); `commit_seq_upper` additionally skips
            // segments whose every row is past the seq cutoff. `None` (micros
            // / OpenTx axes) leaves selection on `committed_at` alone.
            let (upsert_segments, delete_segments) = self
                .read_persist_segments_for_window(
                    driver,
                    catalog_uuid,
                    branch_uuid,
                    table_uuid,
                    from_micros,
                    Some(cold_max),
                    commit_seq_upper,
                )
                .await?;

            (
                ColdInputs {
                    snapshotted_at_micros,
                    snapshot_seq: w_snap,
                    snapshot_segments,
                    indexes,
                    upsert_segments,
                    delete_segments,
                },
                Some(fence),
                hot_present,
            )
        };

        // Enumerate the parent's cold tier as a second source for a
        // forked branch, gated on the picked child snapshot NOT already
        // covering the fork (`child_snapshot_seq < fork_commit_seq_num`). Once
        // a child snapshot covers the fork, that snapshot's own segment rows
        // already account for the parent's data — since CHA-531 by carrying the
        // parent's `object_uri`s forward by reference rather than by rewriting
        // them — so enumerating the base source again would double-count.
        // Steady-state forked reads return to the non-forked plan shape.
        // CHA-539: the child now HOLDS the parent's cold as-of the fork as its own
        // rows, so the base arm is exactly redundant for any read at or above the
        // fork — and still required below it, where the child inherited one
        // baseline rather than the parent's whole history.
        //
        // The old gate (`child_snapshot_seq < fork_commit_seq_num`) asked the
        // wrong question: the inherited snapshot carries the PARENT's `W_snap`,
        // which is at or below the fork, so it stayed open on every fork forever
        // and every forked read re-enumerated data the child already owned.
        //
        // Deliberately CONSERVATIVE. It closes the arm only for reads provably at
        // or above the fork; a read below it keeps today's path verbatim. So an
        // `as_of` between the inherited watermark and the fork still enumerates
        // the parent even though the copied rows could answer alone — the two
        // resolve to the same files and the fold dedups them, and preserving the
        // existing path for every below-fork read is worth more than saving an
        // enumeration on a rare time-travel query. Do not narrow this to the
        // inherited watermark.
        let read_is_below_fork = |fork_seq: i64, fork_micros: i64| {
            as_of_micros < fork_micros
                || commit_seq_upper.is_some_and(|as_of_seq| as_of_seq < fork_seq)
        };
        let base_cold_storage = match lineage {
            Some((parent_branch_uuid, fork_commit_seq_num, fork_commit_micros))
                if read_is_below_fork(fork_commit_seq_num, fork_commit_micros) =>
            {
                // Parent ceiling = min(fork_seed, as_of_seq): the fork is a hard
                // cap `as_of` can only push down.
                let commit_seq_ceiling = commit_seq_upper
                    .map_or(fork_commit_seq_num, |as_of_seq| {
                        fork_commit_seq_num.min(as_of_seq)
                    });
                self.enumerate_base_cold_source(
                    driver,
                    catalog_uuid,
                    &parent_branch_uuid,
                    table_uuid,
                    as_of_micros,
                    commit_seq_ceiling,
                )
                .await?
            }
            _ => None,
        };
        // Permanent gate-observability marker (dormant unless `penca=debug`),
        // mirroring `tier_shape` — the post-snapshot integration test scrapes it.
        tracing::debug!(
            base_cold_source = if base_cold_storage.is_some() {
                "present"
            } else {
                "none"
            },
            "read_data base cold source",
        );

        let mut plan = assemble_plan(
            hot_min,
            fence,
            hot_present,
            as_of_micros,
            cold,
            HotTableNames {
                upsert_table_name,
                delete_table_name,
                commit_tx_log_table_name,
            },
        );
        plan.base_cold_storage = base_cold_storage;
        // The floor rides back with the plan; read_data / plan_audit enforce
        // the below-floor check on it.
        Ok((plan, retention_floor))
    }

    /// Phase-1 fused capture: ONE Postgres round-trip returning the
    /// hot↔cold seq `fence` and the hot-existence gate `hot_present` for
    /// `(branch, table)`.
    ///
    /// `fence = GREATEST(Pu, W_snap)` where `Pu = MAX(last_purged_commit_seq_num)`
    /// over committed `table_purge_metadata` (the cold read fence, ADR 0027)
    /// and `W_snap` (the snapshot watermark, bound as `$3`) floors it
    /// so hot never overlaps the snapshot baseline while `Pu` lags `W_snap`.
    /// `Pu` NULL (no committed purge yet) ⇒ `GREATEST` returns `W_snap`. The
    /// fence is computed in the `fence` CTE so it is captured ONCE and returned
    /// for the cold-persist fence — a single cutoff per plan (ADR-0019's grace
    /// assumes one; independent per-side `Pu` reads would open a silent gap).
    ///
    /// `hot_present` is a deliberately **loose** existence gate:
    /// `true` iff either hot log holds ANY row (`EXISTS(upsert) OR
    /// EXISTS(delete)`), with NO fence/as_of predicate and NO `commit_tx_log` join.
    /// This is a safe over-approximation — `hot_present` only decides whether
    /// `assemble_plan` attaches `hot_storage`, and `plan_hot_storage` re-applies
    /// fence + as_of, so reporting `true` for below-fence / future / uncommitted
    /// rows merely runs the hot read, which re-filters (pre-Persist already
    /// passes `hot_present = true` unconditionally). It never false-negatives: a
    /// visible hot row IS a log row, so an empty log ⇒ no hot rows. The open
    /// tx's own writes are rows in the log, so the bare EXISTS covers RYOW
    /// without threading the tx uuid. Do not tighten it: a fence/as_of
    /// predicate forces a full `commit_tx_log` scan + per-row probe (~1,724
    /// shared-buffer hits) whose wall-clock balloons under contention, where
    /// the bare EXISTS short-circuits at the first row and is a ~0-page scan
    /// on the post-purge empty hot log.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn phase_one_fence_and_existence(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        upsert_table_name: &str,
        delete_table_name: &str,
        w_snap: i64,
    ) -> Result<(i64, bool)> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let purge_table = naming::table_purge_metadata_partition(&catalog, &branch);
        let sql = format!(
            "WITH fence AS (\
                 SELECT GREATEST(\
                     (SELECT MAX(last_purged_commit_seq_num) FROM {purge} \
                       WHERE branch_uuid = $1 AND table_uuid = $2 \
                         AND commit_micros IS NOT NULL), \
                     $3\
                 ) AS fence\
             ) \
             SELECT f.fence AS fence, (\
                 EXISTS (SELECT 1 FROM {upsert}) \
                 OR EXISTS (SELECT 1 FROM {delete})\
             ) AS hot_present \
             FROM fence f",
            purge = qi(&purge_table),
            upsert = qi(upsert_table_name),
            delete = qi(delete_table_name),
        );
        let params = vec![
            SqlValue::Uuid(branch),
            SqlValue::uuid_str(table_uuid)?,
            SqlValue::Int64(w_snap),
        ];
        let rows = driver.execute_params(&sql, &params).await?;
        let row = rows.first().ok_or_else(|| {
            MetadataError::Db(sqlx::Error::Protocol(
                "phase-1 fence/existence query returned no row".into(),
            ))
        })?;
        Ok((row.get("fence"), row.get("hot_present")))
    }

    /// Fused capture — ONE round-trip returning the persist watermark
    /// (→ `hot_min = persist_wm + 1`) AND the IDENTITY of the snapshot THIS read
    /// resolves to ([`PickedSnapshot`]: `table_snapshot_uuid` + its `W_snap`),
    /// the as_of-/seq-bounded pick. [`Self::list_snapshot_segments`] keys the
    /// cache on `W_snap` and — on a miss — threads `table_snapshot_uuid` into the
    /// segment fetch, so the snapshot is picked exactly ONCE (here) and the fetch
    /// reads it BY IDENTITY. That is what makes the cache correct by construction:
    /// KEY and VALUE come from the same pick, so they cannot name different
    /// snapshots (no re-pick, no divergence race to guard). `None` ⇒ no committed
    /// snapshot ⇒ the caller reads fresh and never caches (pre-Persist /
    /// cold-only-no-snapshot).
    ///
    /// The pick bound reproduces [`watermarks::compute_snapshot_picker_as_of`]
    /// (`min(as_of, hot_min - 1)`) as `LEAST($as_of, COALESCE(persist_wm, -1))`,
    /// so a cache read resolves the same snapshot a non-cache read of the same
    /// `as_of` would; the `commit_seq_num DESC` tiebreaker makes that single pick
    /// deterministic under a (degenerate) shared `snapshotted_at_micros`.
    async fn hot_min_and_snapshot_pick(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        as_of_micros: i64,
        commit_seq_upper: Option<i64>,
        retention_duration_seconds: Option<i64>,
    ) -> Result<(
        i64,
        Option<PickedSnapshot>,
        Option<RetentionFloor>,
        Option<BranchLineage>,
    )> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = parse_meta_uuid(table_uuid, "table_uuid")?;
        let persist_name = naming::table_persist_metadata_partition(&catalog, &branch);
        let snap_name = naming::table_snapshot_metadata_partition(&catalog, &branch);
        // Fold the child's fork lineage into this already-per-read query (a
        // branch_store PK join) so a non-forked read pays no extra round-trip
        // for the base-source gate.
        let branch_store = naming::branch_store_table(&catalog);
        // The seq bind trails `$3` (as_of) at `$4` when present.
        let seq_clause = match commit_seq_upper {
            Some(_) => "AND s.commit_seq_num <= $4 ",
            None => "",
        };
        let mut params: Vec<SqlValue> = vec![
            SqlValue::Uuid(table),
            SqlValue::Uuid(branch),
            SqlValue::Int64(as_of_micros),
        ];
        if let Some(seq) = commit_seq_upper {
            params.push(SqlValue::Int64(seq));
        }
        // Fold the retention floor onto the SAME round trip when retention is
        // enabled — an independent LATERAL over the CURRENT durable snapshots,
        // with the window start computed from the DB clock (no `now_micros`
        // threading). `None` ⇒ no floor read. Reuses `retention_floor_select`
        // so the durable/committed predicate lives in one place, shared with
        // `LifecycleManager::retention_floor`.
        let (floor_cols, floor_join) = match retention_duration_seconds {
            Some(duration_seconds) => {
                params.push(SqlValue::Int64(duration_seconds));
                let window_start =
                    penca_storage_meta::retention_window_start_expr(&format!("${}", params.len()));
                let inner = penca_storage_meta::retention_floor_select(
                    &qi(&snap_name),
                    "$2",
                    "$1",
                    &window_start,
                );
                (
                    ", f.commit_seq_num AS floor_seq, f.snapshotted_at_micros AS floor_micros",
                    format!(" LEFT JOIN LATERAL ({inner}) f ON TRUE"),
                )
            }
            None => ("", String::new()),
        };
        let sql = format!(
            "WITH hot AS ( \
                 SELECT MAX(persisted_at_micros) AS persist_wm \
                 FROM {persist} \
                 WHERE branch_uuid = $2 AND table_uuid = $1 AND commit_micros IS NOT NULL \
             ) \
             SELECT h.persist_wm, p.commit_seq_num AS w_snap, p.table_snapshot_uuid, \
                    bs.parent_branch_uuid, bs.fork_commit_seq_num, \
                    bs.fork_commit_micros{floor_cols} \
             FROM hot h \
             LEFT JOIN LATERAL ( \
                 SELECT s.commit_seq_num, s.table_snapshot_uuid \
                 FROM {snap} s \
                 WHERE s.table_uuid = $1 AND s.branch_uuid = $2 \
                   AND s.commit_micros IS NOT NULL \
                   AND s.snapshotted_at_micros <= LEAST($3, COALESCE(h.persist_wm, -1)) \
                   {seq_clause}\
                 ORDER BY s.snapshotted_at_micros DESC, s.commit_seq_num DESC LIMIT 1 \
             ) p ON TRUE \
             LEFT JOIN {store} bs ON bs.branch_uuid = $2{floor_join}",
            persist = qi(&persist_name),
            snap = qi(&snap_name),
            store = qi(&branch_store),
            seq_clause = seq_clause,
        );
        let rows = driver.execute_params(&sql, &params).await?;
        let row = rows.first();
        let persist_wm = row.and_then(|r| r.try_get::<Option<i64>, _>("persist_wm").ok().flatten());
        // Both columns present ⇒ a committed snapshot was picked. A `None`
        // `LEFT JOIN LATERAL` (no eligible snapshot) leaves them NULL.
        let pick = row.and_then(|r| {
            let w_snap = r.try_get::<Option<i64>, _>("w_snap").ok().flatten()?;
            let table_snapshot_uuid = r
                .try_get::<Option<Uuid>, _>("table_snapshot_uuid")
                .ok()
                .flatten()?;
            Some(PickedSnapshot {
                table_snapshot_uuid,
                w_snap,
            })
        });
        // The floor columns are absent from the projection when retention is
        // disabled (the fold is omitted) → `try_get` errors → None;
        // when present but no durable precedes the window, the LATERAL leaves
        // them NULL → None.
        let floor = row.and_then(|r| {
            let commit_seq_num = r.try_get::<Option<i64>, _>("floor_seq").ok().flatten()?;
            let snapshotted_at_micros =
                r.try_get::<Option<i64>, _>("floor_micros").ok().flatten()?;
            Some(RetentionFloor {
                commit_seq_num,
                snapshotted_at_micros,
            })
        });
        // `parent_branch_uuid` is nullable (NULL = non-forked, e.g. main) while
        // `fork_commit_seq_num` is NOT NULL (main = 0), so the fork signal is
        // parent presence alone.
        // Propagate a genuine decode error rather than masking it as "no
        // lineage" — on this live read path that would make a forked branch
        // silently drop its parent cold source and return incomplete data.
        let lineage = match row {
            Some(r) => {
                let parent: Option<Uuid> =
                    r.try_get("parent_branch_uuid").map_err(MetadataError::Db)?;
                let fork_commit_seq_num: i64 = r
                    .try_get("fork_commit_seq_num")
                    .map_err(MetadataError::Db)?;
                let fork_commit_micros: i64 =
                    r.try_get("fork_commit_micros").map_err(MetadataError::Db)?;
                parent.map(|parent| (parent.to_string(), fork_commit_seq_num, fork_commit_micros))
            }
            None => None,
        };
        Ok((persist_wm.map_or(0, |p| p + 1), pick, floor, lineage))
    }

    /// Cache-aware wrapper over [`Self::read_snapshot_segments_for_table`].
    ///
    /// The snapshot segment list is immutable between snapshot commits, so a
    /// `Some(cache)` serves the `(catalog, branch, table, W_snap)` entry from
    /// the process cache, skipping the segment-fetch round-trip. `cache_pick` is
    /// the snapshot the fused [`Self::hot_min_and_snapshot_pick`] resolved: its
    /// `w_snap` keys the entry (content-addressed by snapshot version — any read,
    /// current-time OR time-travel, hits its own immutable entry, no staleness to
    /// guard) and, on a MISS, its `table_snapshot_uuid` is threaded into the fetch
    /// so the VALUE reads exactly the keyed snapshot BY IDENTITY. The snapshot is
    /// picked once (in the fused query), so KEY and VALUE cannot name different
    /// snapshots — no re-pick, no divergence race. `cache_pick = None` (this read
    /// resolved no committed snapshot) ⇒ read fresh and never cache.
    ///
    /// Only a committed snapshot (`snapshotted_at_micros` + `commit_seq_num` both
    /// `Some`) is inserted; a table with no snapshot yet is never cached (it is
    /// the pre-Persist / not-yet-snapshotted edge, off the hot path).
    async fn list_snapshot_segments(
        &self,
        cache: Option<&SnapshotListCache>,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        snapshot_as_of: i64,
        commit_seq_upper: Option<i64>,
        cache_pick: Option<PickedSnapshot>,
    ) -> Result<SnapshotResult> {
        // Consult the cache only when a cache is present AND this read resolved a
        // committed snapshot to key on / fetch by. Otherwise fall through to the
        // as_of/seq pick (no-cache callers, and the not-yet-snapshotted edge).
        let (Some(cache), Some(pick)) = (cache, cache_pick) else {
            return self
                .read_snapshot_segments_for_table(
                    driver,
                    catalog_uuid,
                    branch_uuid,
                    table_uuid,
                    Some(snapshot_as_of),
                    commit_seq_upper,
                    None,
                )
                .await;
        };

        let key = (
            catalog_uuid.to_string(),
            branch_uuid.to_string(),
            table_uuid.to_string(),
            pick.w_snap,
        );
        // The key IS the validity — a given `W_snap` is one immutable snapshot,
        // so a hit needs no frontier re-check: a newer snapshot mints a
        // distinct key.
        if let Some(hit) = cache.get(&key) {
            tracing::debug!(snapshot_list_cache = "hit", table = table_uuid);
            return Ok(SnapshotResult {
                snapshotted_at_micros: Some(hit.snapshotted_at_micros),
                commit_seq_num: Some(hit.commit_seq_num),
                snapshot_segments: hit.segments.clone(),
                indexes: hit.indexes.clone(),
                partition_keys: hit.partition_keys.clone(),
                clustering_keys: hit.clustering_keys.clone(),
            });
        }
        tracing::debug!(snapshot_list_cache = "miss", table = table_uuid);

        // Fetch the VALUE by the fused pick's `table_snapshot_uuid`, never a
        // re-pick: reading by identity means the cached segments are exactly
        // the keyed snapshot's, so KEY (`w_snap`) and VALUE come from one pick
        // and cannot diverge. The uuid IS the pin — no as_of/seq re-bind.
        let result = self
            .read_snapshot_segments_for_table(
                driver,
                catalog_uuid,
                branch_uuid,
                table_uuid,
                None,
                None,
                Some(pick.table_snapshot_uuid),
            )
            .await?;
        // Cache only a real committed snapshot (both watermarks `Some`) — a
        // `None`-watermark row would later be served as a bogus committed
        // snapshot. `cached_snapshot_list` is the pure, unit-tested decision.
        if let Some(cached) = cached_snapshot_list(&result) {
            cache.insert(key, Arc::new(cached));
        }
        Ok(result)
    }

    /// Query the latest committed snapshot for `(branch, table)` whose
    /// `snapshotted_at_micros <= as_of_micros` and materialize its
    /// segments. Returns the snapshot's `snapshotted_at_micros` (used
    /// by the log query to filter older log segments) plus the segment
    /// list. Empty `Vec` + `None` timestamp if no eligible snapshot
    /// exists.
    ///
    /// **Picking by `snapshotted_at_micros` (not `commit_micros`).**
    /// `snapshotted_at_micros` is the watermark the snapshot is meant
    /// to represent — the data it materialized. `commit_micros`
    /// is when the snapshot's metadata row was committed. The two can
    /// be reordered (e.g., a long-running snapshot taken at S1 commits
    /// after a quick snapshot taken at S2 > S1); picking by
    /// commit-time would then return the wrong (older) snapshot
    /// content.
    ///
    /// **`as_of_micros` filter.** Time-travel reads at `as_of = T`
    /// need a snapshot with `snapshotted_at_micros <= T` — a later
    /// snapshot contains data committed after `T` that can't be
    /// un-applied from a materialized form. When `as_of_micros = None`
    /// (current-time read), pick the absolute latest snapshot.
    ///
    /// `branch_uuid = $2` sits on every WHERE for partition pruning.
    /// Segment rows come back `ORDER BY seg.chunk_idx` — write order, which is
    /// label-sorted partition-run order. The snapshot writer's ByPlan
    /// run-grouping depends on that; other callers get a deterministic order
    /// for free.
    // The span isolates this PG round-trip in a read-plan decomposition (busy
    // vs idle). Dormant under the default `penca=debug`; enable with
    // `…,penca_storage_meta=trace` + `PENCA_SPAN_TIMING=1` to time it.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(
            segments = tracing::field::Empty,
            user_indexes = tracing::field::Empty,
        )
    )]
    pub async fn read_snapshot_segments_for_table(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        as_of_micros: Option<i64>,
        // Seq-axis pick bound. `Some(N)` (an AsOfSeq read) picks the latest
        // snapshot whose W_snap (`commit_seq_num`) <= N, so the baseline can't
        // surface rows past the read's seq cutoff. `None` (micros / OpenTx /
        // lifecycle construction) leaves selection on `snapshotted_at_micros`
        // alone. Ignored when `pinned_snapshot_uuid` is `Some`.
        commit_seq_upper: Option<i64>,
        // `Some(uuid)` reads THAT snapshot by identity — the cache miss-path
        // threads the fused pick's `table_snapshot_uuid`, so there is no
        // re-pick and `as_of_micros`/`commit_seq_upper` are unused. `None`
        // picks the latest snapshot bounded by as_of / seq (the non-cache
        // callers: lifecycle snapshot writer, not-yet-cached reads).
        pinned_snapshot_uuid: Option<Uuid>,
    ) -> Result<SnapshotResult> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        // Parse fallibly (unlike the panicking `parse_uuid` above): a malformed
        // `table_uuid` surfaces as the same typed protocol error the
        // `meta_resolve` getters produce.
        let table = parse_meta_uuid(table_uuid, "table_uuid")?;
        let snap_name = naming::table_snapshot_metadata_partition(&catalog, &branch);
        let seg_name = naming::table_snapshot_segment_metadata_partition(&catalog, &branch);
        // The internal row_uuid index parent/child, joined in below so a
        // planned snapshot segment carries its sidecar inline.
        let idx_parent = naming::table_snapshot_index_metadata_partition(&catalog, &branch);
        let idx_child = naming::table_snapshot_segment_index_metadata_partition(&catalog, &branch);
        // Params `$1` = table, `$2` = branch (fixed in the JOINs / WHERE); the
        // rest are bound in push order below, each `$N` computed from
        // `params.len()`, so the pinned-uuid and as_of/seq picks share one
        // numbering scheme.
        let mut params: Vec<SqlValue> = vec![SqlValue::Uuid(table), SqlValue::Uuid(branch)];
        let snapshot_selection = if let Some(uuid) = pinned_snapshot_uuid {
            params.push(SqlValue::Uuid(uuid));
            format!("snap.table_snapshot_uuid = ${}", params.len())
        } else {
            let as_of_clause = match as_of_micros {
                Some(aom) => {
                    params.push(SqlValue::Int64(aom));
                    format!("AND snapshotted_at_micros <= ${} ", params.len())
                }
                None => String::new(),
            };
            let seq_clause = match commit_seq_upper {
                Some(seq) => {
                    params.push(SqlValue::Int64(seq));
                    format!("AND commit_seq_num <= ${} ", params.len())
                }
                None => String::new(),
            };
            format!(
                "snap.table_snapshot_uuid = (\
                     SELECT table_snapshot_uuid FROM {snap_table} \
                     WHERE table_uuid = $1 AND branch_uuid = $2 AND commit_micros IS NOT NULL \
                     {as_of_clause}{seq_clause}\
                     ORDER BY snapshotted_at_micros DESC, commit_seq_num DESC LIMIT 1\
                 )",
                snap_table = qi(&snap_name),
            )
        };
        // LEFT JOIN the index parents so each segment carries its sidecars'
        // read-coordinates inline — one query, no separate resolve round-trip.
        // The join admits EVERY committed parent, so rows become (segment ×
        // parent): the identity parent's child feeds the dedicated
        // `row_uuid_index_sidecar` slot, keyed parents feed `index_sidecars`.
        // Two kinds of keyed parent qualify — user secondary indexes (non-NULL
        // `key_columns`, which also feed the plan's `SnapshotIndexDef` planner
        // list) and the built-in system name index (non-NULL uuid + NULL
        // `key_columns`, matched by its deterministic `system_name_index_uuid`
        // via `name_parent_clause`; it lands in `index_sidecars` but is NOT a
        // planner candidate, so it never enters `SnapshotIndexDef`).
        //
        // Both joins MUST stay LEFT: a segment without a built sidecar yields
        // all-NULL `c.*` → no sidecar → full-scan fallback. Segment-row
        // contiguity comes from the primary ORDER BY alone (`chunk_idx` is
        // segment-unique within the picked snapshot); the secondary
        // `index_uuid` key only stabilizes within-segment row order.
        //
        // `system_name_index_spec` classifies the target: `Some` on the three
        // `__penca_system__` tables carrying a built-in name index (bind its
        // `index_uuid`, widen the parent filter to admit it), `None` on every
        // user table — so the user-table plan SQL gains no extra clause and no
        // extra bind. The name bind trails the optional as_of / seq binds.
        let name_spec = naming::system_name_index_spec(&catalog, &table);
        let name_parent_clause = match &name_spec {
            Some(spec) => {
                params.push(SqlValue::Uuid(spec.index_uuid));
                format!(" OR p.index_uuid = ${}", params.len())
            }
            None => String::new(),
        };
        let snapshot_sql = format!(
            "SELECT seg.table_snapshot_segment_uuid, seg.object_uri, \
                    seg.\"offset\", seg.length, seg.format, seg.content_hash, \
                    snap.snapshotted_at_micros, snap.commit_seq_num, \
                    seg.table_snapshot_uuid, seg.row_count, \
                    seg.size_bytes, seg.metadata, seg.statistics, \
                    snap.partition_keys, snap.clustering_keys, \
                    p.index_uuid AS parent_index_uuid, \
                    p.key_columns AS parent_key_columns, \
                    c.object_uri AS sidecar_object_uri, \
                    c.\"offset\" AS sidecar_offset, c.length AS sidecar_length, \
                    c.format AS sidecar_format, c.size_bytes AS sidecar_size_bytes, \
                    c.segment_index_uuid AS sidecar_segment_index_uuid, \
                    c.content_hash AS sidecar_content_hash \
             FROM {seg_table} seg \
             INNER JOIN {snap_table} snap \
               ON seg.table_snapshot_uuid = snap.table_snapshot_uuid \
             LEFT JOIN {idx_parent} p \
               ON p.table_snapshot_uuid = seg.table_snapshot_uuid \
              AND p.branch_uuid = $2 \
              AND p.commit_micros IS NOT NULL \
              AND (p.index_uuid IS NULL OR p.key_columns IS NOT NULL{name_parent_clause}) \
             LEFT JOIN {idx_child} c \
               ON c.table_snapshot_index_uuid = p.table_snapshot_index_uuid \
              AND c.segment_uuid = seg.table_snapshot_segment_uuid \
              AND c.branch_uuid = $2 \
              AND c.commit_micros IS NOT NULL \
             WHERE snap.table_uuid = $1 \
               AND seg.branch_uuid = $2 AND snap.branch_uuid = $2 \
               AND snap.commit_micros IS NOT NULL \
               AND seg.commit_micros IS NOT NULL \
               AND {snapshot_selection} \
             ORDER BY seg.chunk_idx, p.index_uuid NULLS FIRST",
            seg_table = qi(&seg_name),
            snap_table = qi(&snap_name),
            idx_parent = qi(&idx_parent),
            idx_child = qi(&idx_child),
            snapshot_selection = snapshot_selection,
            name_parent_clause = name_parent_clause,
        );
        let snapshot_rows = driver.execute_params(&snapshot_sql, &params).await?;

        let mut snapshotted_at_micros: Option<i64> = None;
        let mut partition_keys: Option<Vec<String>> = None;
        let mut clustering_keys: Option<Vec<String>> = None;
        let mut snapshot_commit_seq_num: Option<i64> = None;
        let mut snapshot_segments: Vec<SnapshotSegment> = Vec::new();
        // Distinct user-index defs across the parent rows — each def repeats
        // once per segment row, so dedupe by uuid and sort at the end.
        let mut indexes: Vec<SnapshotIndexDef> = Vec::new();
        let mut seen_index_uuids: HashSet<Uuid> = HashSet::new();

        for row in &snapshot_rows {
            // Capture the watermark from every row, INCLUDING zero-row
            // placeholder segments (emitted by snapshot_locked when the cold
            // merge nets to empty). Skipping placeholders before reading
            // `snapshotted_at_micros` loses the watermark and forces the next
            // Snapshot(T) to redo the cold merge-read forever.
            if snapshotted_at_micros.is_none() {
                snapshotted_at_micros = Some(row.get("snapshotted_at_micros"));
                // W_snap rides the same parent row (NOT NULL column).
                snapshot_commit_seq_num = Some(row.get("commit_seq_num"));
                // The layout keys are parent-level (identical across the
                // snapshot's rows). Decoding as `Option<Vec<String>>`
                // preserves SQL NULL as `None` vs `Some(vec![])` (`{}`,
                // known-no-keys) — the carry-forward eligibility gate depends
                // on that distinction — while still panicking on a genuine
                // type mismatch, like every other `get` here.
                partition_keys = row.get::<Option<Vec<String>>, _>("partition_keys");
                clustering_keys = row.get::<Option<Vec<String>>, _>("clustering_keys");
            }
            // Collect user-index defs BEFORE the placeholder skip so the
            // declared set doesn't depend on data presence. The `Some(cols)`
            // guard below keeps this planner list to user indexes only — the
            // built-in name parent is admitted by the join
            // (`name_parent_clause`) but has NULL `key_columns`, so it lands in
            // `index_sidecars` without ever becoming a planner candidate.
            let parent_index_uuid: Option<Uuid> = row.get("parent_index_uuid");
            let parent_key_columns: Option<Vec<String>> = row.get("parent_key_columns");
            if let (Some(idx), Some(cols)) = (parent_index_uuid, parent_key_columns.as_ref())
                && seen_index_uuids.insert(idx)
            {
                indexes.push(SnapshotIndexDef {
                    index_uuid: idx.to_string(),
                    key_columns: cols.clone(),
                });
            }

            let row_count: i64 = row.get("row_count");
            if row_count == 0 {
                continue;
            }

            let seg_uuid: Uuid = row.get("table_snapshot_segment_uuid");
            let snap_uuid: Uuid = row.get("table_snapshot_uuid");
            let format: Format = row.get::<String, _>("format").parse().map_err(|e| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "table_snapshot_segment_metadata.format decode failed: {e}"
                )))
            })?;
            let offset: i64 = row.get("offset");
            let length: i64 = row.get("length");
            let metadata: serde_json::Value = row.get("metadata");
            let statistics: Option<Vec<u8>> = row.get("statistics");
            let child_sidecar = decode_child_sidecar(row)?;

            // Rows are (segment × parent), contiguous per segment
            // because `chunk_idx` (the primary ORDER BY) is segment-unique
            // within the snapshot — the first row creates the segment, the
            // rest only attach their parent's sidecar.
            let seg_uuid_str = seg_uuid.to_string();
            if snapshot_segments
                .last()
                .map(|seg| seg.table_snapshot_segment_uuid.as_str())
                != Some(seg_uuid_str.as_str())
            {
                snapshot_segments.push(SnapshotSegment {
                    table_snapshot_segment_uuid: seg_uuid_str,
                    table_snapshot_uuid: snap_uuid.to_string(),
                    uri: row.get("object_uri"),
                    format,
                    offset,
                    length,
                    parquet_metadata: None,
                    row_count,
                    size_bytes: row.get("size_bytes"),
                    metadata_json: metadata.to_string(),
                    statistics: statistics.unwrap_or_default(),
                    content_hash: row.get("content_hash"),
                    row_uuid_index_sidecar: None,
                    index_sidecars: Vec::new(),
                });
            }
            let segment = snapshot_segments
                .last_mut()
                .expect("segment pushed or matched above");
            attach_parent_row(segment, parent_index_uuid, child_sidecar);
        }
        // Deterministic planner/seek iteration order.
        indexes.sort_by(|a, b| a.index_uuid.cmp(&b.index_uuid));
        for segment in &mut snapshot_segments {
            segment.index_sidecars.sort_by(|a, b| a.0.cmp(&b.0));
        }

        tracing::Span::current().record("segments", snapshot_segments.len());
        tracing::Span::current().record("user_indexes", indexes.len());

        Ok(SnapshotResult {
            snapshotted_at_micros,
            commit_seq_num: snapshot_commit_seq_num,
            snapshot_segments,
            indexes,
            partition_keys,
            clustering_keys,
        })
    }

    /// Return every committed `(upsert, delete)` persist segment
    /// for a table that overlaps the audit window
    /// `[from_micros, to_micros)` — the cold-side committed-at window.
    /// Shared by `audit_data` (audit horizon, may span the snapshot
    /// baseline) and `plan()` / `snapshot()` (segments past the
    /// snapshot watermark up to the user's `as_of`). Per ADR 0011 /
    /// `RetentionConfig`, the audit default is unbounded.
    ///
    /// The window maps onto the internal helper's exclusive
    /// `(snapshotted_at, as_of]` overlap filter via
    /// `snapshotted_at = from - 1` (since `from` is inclusive) and
    /// `as_of = to - 1` (since `to` is exclusive).
    // The span isolates this PG round-trip in a read-plan decomposition (busy
    // vs idle). Dormant under the default `penca=debug`; enable with
    // `…,penca_storage_meta=trace` + `PENCA_SPAN_TIMING=1` to time it.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn read_persist_segments_for_window(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        from_micros: Option<i64>,
        to_micros: Option<i64>,
        commit_seq_upper: Option<i64>,
    ) -> Result<(Vec<PersistSegment>, Vec<PersistSegment>)> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = parse_uuid(table_uuid);
        // Audit window `[from, to)` translates to the helper's overlap
        // bounds via `x ≥ y ≡ x > y − 1` and `x < y ≡ x ≤ y − 1`:
        //   `seg.max_tx ≥ from`  →  `min_segment_max_micros = from − 1`
        //   `seg.min_tx < to`    →  `max_segment_min_micros = to − 1`
        // `commit_seq_upper` passes through unchanged — it is an inclusive
        // `seg.min_commit_seq_num <= N` skip, not a `[from, to)` window, so it
        // needs no `− 1` translation.
        let min_segment_max_micros = from_micros.map(|f| f.saturating_sub(1));
        let max_segment_min_micros = to_micros.map(|t| t.saturating_sub(1));
        let ClassifiedPersistSegments {
            upsert_segments,
            delete_segments,
            ..
        } = self
            .read_and_classify_persist_segments(
                driver,
                &catalog,
                &branch,
                &table,
                min_segment_max_micros,
                max_segment_min_micros,
                commit_seq_upper,
            )
            .await?;
        Ok((upsert_segments, delete_segments))
    }

    /// The `commit_micros` at which a fork's OWN inherited coverage begins —
    /// `MIN(min_tx_commit_micros)` over the persist segments `CreateBranch`
    /// copied into the child. The audit base arm stops here (exclusively), so
    /// the two arms meet exactly: the parent covers `[genesis, floor)` and the
    /// child's copies cover `[floor, fork]`.
    ///
    /// Derived from the copied segments themselves rather than from the adopted
    /// snapshot's `snapshotted_at_micros`, because those two do not have to
    /// align. The copy is per-persist-*header*: a header whose
    /// `persisted_at_micros` sits above the baseline is copied with all of its
    /// segments, and such a segment carries every row of that persist run —
    /// including rows committed at or below the baseline. `snapshotted_at_micros`
    /// is caller-supplied on the public `Snapshot` RPC and
    /// `compute_snapshot_window` takes `min(as_of, persisted_at)`, so a baseline
    /// landing mid-run is directly reachable. Capping at `baseline + 1` then
    /// leaves the parent re-emitting every row the straddling copied segment
    /// already carries — observed as one inherited change row appearing three
    /// times, once per overlapping parent segment plus the child's own copy.
    ///
    /// Bounded at the fork on the seq axis: the child's own post-fork segments
    /// all sit above `fork_commit_seq_num`, so without the bound this `MIN`
    /// would keep dropping as the child persists its own writes and the base
    /// arm would widen back over ground the child already covers.
    ///
    /// `None` when the child copied no persist segments at all — there is then
    /// no own-arm coverage to be disjoint from, so the base arm keeps the
    /// caller's full window.
    pub async fn inherited_own_arm_floor(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        fork_commit_seq_num: i64,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let seg = naming::table_persist_segment_metadata_partition(&catalog, &branch);
        let rows = driver
            .execute_params(
                &format!(
                    "SELECT MIN(min_tx_commit_micros) AS floor FROM {seg} \
                     WHERE branch_uuid = $1 AND table_uuid = $2 \
                       AND commit_micros IS NOT NULL \
                       AND min_commit_seq_num <= $3",
                    seg = qi(&seg),
                ),
                &[
                    SqlValue::Uuid(branch),
                    SqlValue::uuid_str(table_uuid)?,
                    SqlValue::Int64(fork_commit_seq_num),
                ],
            )
            .await?;

        Ok(rows
            .first()
            .and_then(|row| row.try_get::<Option<i64>, _>("floor").ok().flatten()))
    }

    /// Read a branch's fork lineage from `branch_store` — the parent branch and
    /// the fork commit's position on both axes, `(parent, seq, micros)`. `None`
    /// for `main` and any branch with NULL lineage (non-forked). The read planner
    /// consults this to decide whether to enumerate a parent cold source for a
    /// forked-branch read.
    ///
    /// Both axes come back because reads arrive on both: a seq-axis read carries
    /// `commit_seq_upper`, a current-time or `AsOfMicros` read carries only
    /// `as_of_micros`. Seq remains the authority for any ceiling — see the
    /// non-strict-monotonic `commit_micros` note on the `branch_store` DDL.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn read_branch_lineage(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Option<(String, i64, i64)>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch_store = naming::branch_store_table(&catalog);
        let sql = format!(
            "SELECT parent_branch_uuid, fork_commit_seq_num, fork_commit_micros \
             FROM {store} WHERE branch_uuid = $1",
            store = qi(&branch_store),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        // Propagate a genuine decode/type error rather than swallowing it to
        // `None` — a masked failure would make a forked branch silently drop
        // its parent cold source and return incomplete data. `parent_branch_uuid`
        // is nullable (NULL = non-forked); `fork_commit_seq_num` is NOT NULL
        // (`main` = 0), so the fork signal is parent presence alone.
        let parent: Option<Uuid> = row
            .try_get("parent_branch_uuid")
            .map_err(MetadataError::Db)?;
        let fork_commit_seq_num: i64 = row
            .try_get("fork_commit_seq_num")
            .map_err(MetadataError::Db)?;
        let fork_commit_micros: i64 = row
            .try_get("fork_commit_micros")
            .map_err(MetadataError::Db)?;
        Ok(parent.map(|parent_branch_uuid| {
            (
                parent_branch_uuid.to_string(),
                fork_commit_seq_num,
                fork_commit_micros,
            )
        }))
    }

    /// Enumerate the parent branch's cold tier as a second cold
    /// source for a forked branch's read, capped at `commit_seq_ceiling`
    /// (= `min(fork_commit_seq_num, as_of_seq)`). Reuses the same two cold
    /// getters keyed on the parent `branch_uuid`; the ceiling both bounds the
    /// parent snapshot pick and, as `PersistPlan.commit_seq.max_seq`, the
    /// per-row persist visibility. Returns `None` when the parent has no cold
    /// data in range so the caller can skip the base arm entirely.
    ///
    /// **Single-level only — TODO(CHA-509).** This reads the *immediate*
    /// parent's own cold tier and does not recurse (`base_plan` carries no
    /// `base_cold_storage`). A fork chain `main → B → C` where an intermediate
    /// branch is unsnapshotted would drop the grandparent's rows: persist-at-
    /// fork flushes only a branch's *own* hot tier, and `B` inherits `main` by
    /// read-time merge rather than by copying `main`'s data into `B`'s cold, so
    /// `C`'s single-level read of `B`'s cold never sees `main`. Sound today
    /// only because forks are `main`-only.
    #[tracing::instrument(
        level = "trace",
        skip_all,
        fields(commit_seq_ceiling = commit_seq_ceiling)
    )]
    pub async fn enumerate_base_cold_source(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        parent_branch_uuid: &str,
        table_uuid: &str,
        as_of_micros: i64,
        commit_seq_ceiling: i64,
    ) -> Result<Option<BaseColdStorage>> {
        // Parent snapshot baseline, picked as-of the fork ceiling (seq) and the
        // read's `as_of_micros`. No cache: the base source is a cold-only,
        // forked-branch path, not the hot current-time read the cache serves.
        let snapshot = self
            .read_snapshot_segments_for_table(
                driver,
                catalog_uuid,
                parent_branch_uuid,
                table_uuid,
                Some(as_of_micros),
                Some(commit_seq_ceiling),
                None,
            )
            .await?;

        // Parent persist segments after that snapshot, seq-capped at the fork
        // ceiling (the committed_at upper is inert on the seq axis).
        let from_micros = snapshot
            .snapshotted_at_micros
            .map(|ts| ts.saturating_add(1));
        let (upsert_segments, delete_segments) = self
            .read_persist_segments_for_window(
                driver,
                catalog_uuid,
                parent_branch_uuid,
                table_uuid,
                from_micros,
                None,
                Some(commit_seq_ceiling),
            )
            .await?;

        let snapshot_plan = snapshot.snapshotted_at_micros.map(|ts| SnapshotPlan {
            segments: snapshot.snapshot_segments,
            indexes: snapshot.indexes,
            snapshotted_at_micros: ts,
            commit_seq_num: snapshot
                .commit_seq_num
                .unwrap_or(watermarks::SNAPSHOT_SEQ_GENESIS),
        });
        let persist_plan = if upsert_segments.is_empty() && delete_segments.is_empty() {
            None
        } else {
            Some(PersistPlan {
                upsert_segments,
                delete_segments,
                committed_at: Some(CommittedAtBounds {
                    min_micros: None,
                    max_micros: Some(as_of_micros.saturating_add(1)),
                }),
                commit_seq: Some(CommitSeqBounds {
                    min_seq: None,
                    max_seq: Some(commit_seq_ceiling),
                }),
            })
        };

        if snapshot_plan.is_none() && persist_plan.is_none() {
            return Ok(None);
        }
        Ok(Some(BaseColdStorage {
            cold: ColdStoragePlan {
                snapshot: snapshot_plan,
                persist: persist_plan,
            },
            commit_seq_ceiling,
        }))
    }

    /// Read committed log segments for upsert/delete tables and
    /// classify them per-bucket.
    ///
    /// Each upsert/delete cold row carries its own `commit_micros,
    /// began_at_micros, comment, author` denormalized columns, so the cold
    /// side has no JOIN partner to resolve.
    ///
    /// `min_segment_max_micros` / `max_segment_min_micros` are
    /// inequality-neutral overlap bounds against the per-segment
    /// watermark interval `[seg.min_tx, seg.max_tx]` — see the doc
    /// comment on the function body. Callers translate their own
    /// domain bounds onto these (e.g. `plan()` passes its snapshot
    /// watermark + as-of via `read_persist_segments_for_window`;
    /// that helper passes `from − 1` / `to − 1` to encode `[from, to)`).
    async fn read_and_classify_persist_segments(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        min_segment_max_micros: Option<i64>,
        max_segment_min_micros: Option<i64>,
        commit_seq_upper: Option<i64>,
    ) -> Result<ClassifiedPersistSegments> {
        // Two-hop JOIN — segment → table_persist_metadata surfaces `log_kind`
        // (upsert/delete classification).
        //
        // **Overlap filter on the per-segment watermark interval
        // `[seg.min_tx, seg.max_tx]`.** Two inequalities, both
        // inequality-neutral so callers can fit any domain on top:
        //
        // - `seg.max_tx > min_segment_max_micros` — the segment has
        //   at least one row past the caller's lower bound.
        //   `read_persist_segments_for_window` passes `from − 1` to
        //   encode "max_tx ≥ from".
        // - `seg.min_tx <= max_segment_min_micros` — the segment has
        //   at least one row at-or-before the caller's upper bound.
        //   `read_persist_segments_for_window` passes `to − 1` to
        //   encode "min_tx < to". Segments that
        //   straddle the bound from below are intentionally included;
        //   per-row filtering downstream prunes the over-bound rows.
        // - `seg.min_commit_seq_num <= commit_seq_upper` — a
        //   seq-axis read additionally drops segments whose smallest
        //   `commit_seq_num` already exceeds the cutoff. Composes with the
        //   committed_at tier fence; absent for the micros / OpenTx axes
        //   (`commit_seq_upper = None`).
        //
        // Both relations are named as the `branch_uuid` ARGUMENT's partitions,
        // never an ambient current-branch value: `enumerate_base_cold_source`
        // calls this with a fork's PARENT branch to resolve inherited cold,
        // while the ordinary read path calls it with the reading branch. Taking
        // the name from anywhere else compiles and silently reads the wrong
        // branch's segments.
        let seg_table = naming::table_persist_segment_metadata_partition(catalog_uuid, branch_uuid);
        let tfm_table = naming::table_persist_metadata_partition(catalog_uuid, branch_uuid);
        let mut log_sql = format!(
            "SELECT tfm.log_kind, \
                    seg.table_persist_segment_uuid AS segment_uuid, \
                    seg.object_uri, seg.\"offset\", seg.length, \
                    seg.row_count, seg.format, seg.content_hash, \
                    seg.min_tx_commit_micros, \
                    seg.max_tx_commit_micros, \
                    seg.max_commit_seq_num, \
                    seg.size_bytes, seg.metadata, seg.statistics \
             FROM {seg} seg \
             INNER JOIN {tfm} tfm \
               ON seg.table_persist_uuid = tfm.table_persist_uuid \
              AND seg.branch_uuid = tfm.branch_uuid \
             WHERE tfm.branch_uuid = $1 \
               AND tfm.log_kind IN ('upsert_log','delete_log') \
               AND tfm.table_uuid = $2 \
               AND seg.commit_micros IS NOT NULL",
            seg = qi(&seg_table),
            tfm = qi(&tfm_table),
        );
        log_sql.push_str(&persist_segment_overlap_clause(
            min_segment_max_micros,
            max_segment_min_micros,
            commit_seq_upper,
        ));
        // Order on the gapless commit_seq_num axis, NOT commit_micros (which
        // has unordered ties), so the single concatenated
        // PersistTableProvider stream is globally non-decreasing in
        // commit_seq_num — the segment-listing half of the output_ordering
        // honesty contract. Within-segment sort comes from
        // `chunk_persist_batch`; the chunk_idx tiebreak preserves it.
        log_sql.push_str(" ORDER BY seg.min_commit_seq_num, seg.chunk_idx");

        let log_rows = driver
            .execute_params(
                &log_sql,
                &[SqlValue::Uuid(*branch_uuid), SqlValue::Uuid(*table_uuid)],
            )
            .await?;

        let mut upsert_segments = Vec::new();
        let mut delete_segments = Vec::new();

        for row in &log_rows {
            let row_count: i64 = row.get("row_count");
            if row_count == 0 {
                continue;
            }

            let log_kind_text: String = row.get("log_kind");
            let log_kind: LogKind = log_kind_text.parse().map_err(|e| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "table_persist_metadata.log_kind decode failed: {e}"
                )))
            })?;
            let seg_uuid: Uuid = row.get("segment_uuid");
            let format: Format = row.get::<String, _>("format").parse().map_err(|e| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "table_persist_segment_metadata.format decode failed: {e}"
                )))
            })?;
            let offset: Option<i64> = row.get("offset");
            let length: Option<i64> = row.get("length");
            let metadata: serde_json::Value = row.get("metadata");
            let statistics: Option<Vec<u8>> = row.get("statistics");

            let segment = PersistSegment {
                segment_uuid: seg_uuid.to_string(),
                uri: row.get("object_uri"),
                format,
                row_count,
                size_bytes: row.get("size_bytes"),
                metadata_json: metadata.to_string(),
                statistics: statistics.unwrap_or_default(),
                offset,
                length,
                // Always set, even where it is inert: for an ordinarily-written
                // segment this equals the file's true maximum, so the ceiling
                // costs a no-op filter and needs no per-row special case.
                max_commit_seq_num: Some(row.get("max_commit_seq_num")),
                content_hash: row.get("content_hash"),
            };

            match log_kind {
                LogKind::UpsertLog => upsert_segments.push(segment),
                LogKind::DeleteLog => delete_segments.push(segment),
            }
        }

        Ok(ClassifiedPersistSegments {
            upsert_segments,
            delete_segments,
        })
    }

    /// `MAX(seg.max_commit_seq_num)` over the committed persist
    /// segments that overlap the snapshot's `[from_micros, to_micros)`
    /// committed-at window — the segment side of the snapshot seq watermark
    /// `W_snap` (fed into [`penca_storage_meta::watermarks::compute_snapshot_seq_watermark`]).
    /// `None` when the window covers no committed segment (carry-forward-only
    /// snapshot). Mirrors [`Self::read_and_classify_persist_segments`]' JOIN +
    /// commit gating + overlap clause, returning a scalar aggregate instead of
    /// the segment rows (`PersistSegment` carries no seq column).
    // The span isolates this PG round-trip in a read-plan decomposition (busy
    // vs idle), matching its spanned sibling
    // `read_persist_segments_for_window`. Dormant under the default
    // `penca=debug`; enable with `…,penca_storage_meta=trace` +
    // `PENCA_SPAN_TIMING=1` to time it.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn max_persisted_segment_seq_for_window(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        from_micros: Option<i64>,
        to_micros: Option<i64>,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = parse_uuid(table_uuid);
        let seg_table = naming::table_persist_segment_metadata_partition(&catalog, &branch);
        let tfm_table = naming::table_persist_metadata_partition(&catalog, &branch);
        let mut sql = format!(
            "SELECT MAX(seg.max_commit_seq_num) AS max_seq \
             FROM {seg} seg \
             INNER JOIN {tfm} tfm \
               ON seg.table_persist_uuid = tfm.table_persist_uuid \
              AND seg.branch_uuid = tfm.branch_uuid \
             WHERE tfm.branch_uuid = $1 \
               AND tfm.log_kind IN ('upsert_log','delete_log') \
               AND tfm.table_uuid = $2 \
               AND seg.commit_micros IS NOT NULL",
            seg = qi(&seg_table),
            tfm = qi(&tfm_table),
        );
        // Same `[from, to)` → overlap translation as
        // `read_persist_segments_for_window` (`from − 1` / `to − 1`); no seq
        // skip (snapshot construction selects on the committed_at window).
        sql.push_str(&persist_segment_overlap_clause(
            from_micros.map(|f| f.saturating_sub(1)),
            to_micros.map(|t| t.saturating_sub(1)),
            None,
        ));
        let rows = driver
            .execute_params(&sql, &[SqlValue::Uuid(branch), SqlValue::Uuid(table)])
            .await?;
        Ok(rows
            .first()
            .and_then(|row| row.get::<Option<i64>, _>("max_seq")))
    }
}

/// Build the persist-segment overlap `AND …` clause appended to the
/// cold-read SQL. Each bound is inequality-neutral against the per-segment
/// watermark interval `[seg.min_tx, seg.max_tx]` — callers translate their
/// own `[from, to)` domain onto `min_segment_max_micros` / `max_segment_min_micros`.
///
/// `commit_seq_upper` is the seq-axis skip — `min_commit_seq_num
/// <= N` drops cold segments whose every row is past the seq cutoff. It
/// composes with the committed_at tier fence rather than replacing it:
/// the lower bound stays the snapshot-baseline `committed_at` watermark.
/// `None` (micros / OpenTx axes) appends no seq predicate.
fn persist_segment_overlap_clause(
    min_segment_max_micros: Option<i64>,
    max_segment_min_micros: Option<i64>,
    commit_seq_upper: Option<i64>,
) -> String {
    let mut clause = String::new();
    if let Some(ts) = min_segment_max_micros {
        clause.push_str(&format!(" AND seg.max_tx_commit_micros > {ts}"));
    }
    if let Some(aom) = max_segment_min_micros {
        clause.push_str(&format!(" AND seg.min_tx_commit_micros <= {aom}"));
    }
    if let Some(seq) = commit_seq_upper {
        clause.push_str(&format!(" AND seg.min_commit_seq_num <= {seq}"));
    }
    clause
}

/// Hot-tier log-table names for the plan's [`penca_core::HotStoragePlan`].
pub(crate) struct HotTableNames {
    pub upsert_table_name: String,
    pub delete_table_name: String,
    pub commit_tx_log_table_name: String,
}

/// Decode one row's LEFT-JOINed child-sidecar columns into an
/// [`IndexSidecar`], or `None` when no child matched (NULL
/// `sidecar_object_uri`). The object_uri-presence gate stands in for "the
/// whole child row matched": offset/length/format/segment_index_uuid/
/// content_hash are all NOT NULL in the schema, so the non-Option `row.get`s
/// cannot hit a NULL while object_uri is present. Revisit if a nullable child
/// column is added.
fn decode_child_sidecar(row: &PgRow) -> Result<Option<IndexSidecar>> {
    row.get::<Option<String>, _>("sidecar_object_uri")
        .map(|object_uri| -> Result<IndexSidecar> {
            let sidecar_format: Format =
                row.get::<String, _>("sidecar_format")
                    .parse()
                    .map_err(|e| {
                        MetadataError::Db(sqlx::Error::Protocol(format!(
                            "table_snapshot_segment_index_metadata.format decode failed: {e}"
                        )))
                    })?;
            Ok(IndexSidecar {
                object_uri,
                offset: row.get("sidecar_offset"),
                length: row.get("sidecar_length"),
                format: sidecar_format,
                segment_index_uuid: row.get::<Uuid, _>("sidecar_segment_index_uuid").to_string(),
                size_bytes: row.get::<Option<i64>, _>("sidecar_size_bytes").unwrap_or(0),
                // Non-Option deliberately, unlike `size_bytes` directly above:
                // an `unwrap_or(Uuid::nil())` here would hand every sidecar the
                // same cache key and serve segment X's index for segment Y.
                content_hash: row.get::<Uuid, _>("sidecar_content_hash"),
            })
        })
        .transpose()
}

/// Route one (segment × parent) row's child sidecar onto the segment being
/// folded. Identity parent — or no parent at all (all-NULL LEFT JOIN) — owns
/// the dedicated `row_uuid_index_sidecar` slot; a keyed parent with a built
/// child lands in `index_sidecars` (user secondary index or built-in system
/// name index, both routed purely by their non-NULL `index_uuid`); a keyed
/// parent without a child for this segment attaches nothing — the seek treats
/// that entry as unresolved, a safe over-selection.
fn attach_parent_row(
    segment: &mut SnapshotSegment,
    parent_index_uuid: Option<Uuid>,
    child_sidecar: Option<IndexSidecar>,
) {
    match (parent_index_uuid, child_sidecar) {
        (None, sidecar) => segment.row_uuid_index_sidecar = sidecar,
        (Some(index_uuid), Some(sidecar)) => {
            segment
                .index_sidecars
                .push((index_uuid.to_string(), sidecar));
        }
        (Some(_), None) => {}
    }
}

/// Cold-tier inputs to [`assemble_plan`] — the snapshot watermark +
/// segments and the post-snapshot persist segments, already fetched from
/// Postgres by [`QueryManager::plan`].
pub(crate) struct ColdInputs {
    pub snapshotted_at_micros: Option<i64>,
    /// `W_snap` — the snapshot seq watermark stamped onto
    /// `SnapshotPlan.commit_seq_num`.
    pub snapshot_seq: i64,
    pub snapshot_segments: Vec<penca_core::SnapshotSegment>,
    /// User-index defs declared for the picked snapshot — stamped
    /// onto `SnapshotPlan.indexes` for planner covering-index selection.
    pub indexes: Vec<penca_core::SnapshotIndexDef>,
    pub upsert_segments: Vec<penca_core::PersistSegment>,
    pub delete_segments: Vec<penca_core::PersistSegment>,
}

/// The cache entry for a snapshot read result, or `None` when the table has no
/// committed snapshot yet (`snapshotted_at_micros` / `commit_seq_num` not both
/// `Some`). Split out as the pure decision behind
/// [`QueryManager::list_snapshot_segments`]'s insert: caching a
/// `None`-watermark row would later be served as a bogus committed snapshot.
fn cached_snapshot_list(result: &SnapshotResult) -> Option<CachedSnapshotList> {
    let (Some(snapshotted_at_micros), Some(commit_seq_num)) =
        (result.snapshotted_at_micros, result.commit_seq_num)
    else {
        return None;
    };
    Some(CachedSnapshotList {
        segments: result.snapshot_segments.clone(),
        // The declared user-index defs must ride the cache entry — a
        // cache-served default read still has to be able to seek.
        indexes: result.indexes.clone(),
        commit_seq_num,
        snapshotted_at_micros,
        partition_keys: result.partition_keys.clone(),
        clustering_keys: result.clustering_keys.clone(),
    })
}

/// Cold-tier committed-at upper cutoff = `min(as_of + 1, hot_min)`: the user
/// `as_of` wins when tighter than the persist watermark (time-travel must not
/// expand visibility past the request). Single source of truth for the
/// cutoff — `plan()` calls it to bound the persist-segment *fetch* and
/// `plan_cold_storage` calls it to stamp `PersistPlan.committed_at.max_micros`;
/// the two must be byte-identical or a concurrent Persist/Purge could shift the
/// fetched-vs-stamped cutoff and break the strict hot/cold partition.
fn cold_committed_at_max(as_of_micros: i64, hot_min: i64) -> i64 {
    as_of_micros.saturating_add(1).min(hot_min)
}

/// Assemble a [`penca_core::Plan`] from the fetched hot cutoff (`hot_min`)
/// and the cold inputs. Pure (no IO): the single captured `hot_min` bounds
/// the cold committed-at window (`cold_max = min(as_of + 1, hot_min)`) — the
/// same `hot_min` the caller threads into the snapshot-picker bound — so a
/// concurrent Persist/Purge between the fetches can't shift the cutoff.
pub(crate) fn assemble_plan(
    hot_min: i64,
    fence: Option<i64>,
    hot_present: bool,
    as_of_micros: i64,
    cold: ColdInputs,
    hot: HotTableNames,
) -> Plan {
    Plan {
        // Hot existence gate: emit the hot tier only when the phase-1 probe
        // found a row past the fence. `!hot_present` ⇒ `hot_storage = None`,
        // engaging the staged `is_all_cold` / `stream_all_cold` dispatch in
        // `read_data`. Pre-Persist callers pass `hot_present = true`.
        hot_storage: hot_present.then(|| plan_hot_storage(fence, as_of_micros, hot)),
        cold_storage: plan_cold_storage(hot_min, fence, as_of_micros, cold),
        // Populated by `plan()` for a forked branch; the pure assembler leaves
        // it `None`.
        base_cold_storage: None,
    }
}

/// Cold-tier plan: the snapshot baseline + post-snapshot persist segments.
/// `None` pre-Persist (`hot_min == 0`, hot owns every row) or when nothing
/// lives in cold for this read.
fn plan_cold_storage(
    hot_min: i64,
    fence: Option<i64>,
    as_of_micros: i64,
    cold: ColdInputs,
) -> Option<ColdStoragePlan> {
    if hot_min == 0 {
        return None;
    }
    let snapshot_plan = cold.snapshotted_at_micros.map(|ts| SnapshotPlan {
        segments: cold.snapshot_segments,
        indexes: cold.indexes,
        snapshotted_at_micros: ts,
        commit_seq_num: cold.snapshot_seq,
    });
    // The cold committed-at filter is deliberately NOT the tier cutoff:
    // `max_micros` is `as_of + 1` (exclusive), pure `AsOfMicros` visibility.
    // The tier fence lives on `commit_seq` below; the two are equivalent
    // (`committed_at < hot_min ⟺ commit_seq_num <= W_persist`), which is why
    // the persist-segment *fetch* in `plan()` can still bound by
    // `cold_committed_at_max` — it brings in exactly the rows the seq filter
    // then passes. On an AsOfSeq read `as_of_micros` is `i64::MAX`, so this
    // bound is inert and the seq fence does all the work.
    let cold_visibility_max = as_of_micros.saturating_add(1);
    let persist_plan = if cold.upsert_segments.is_empty() && cold.delete_segments.is_empty() {
        None
    } else {
        Some(PersistPlan {
            upsert_segments: cold.upsert_segments,
            delete_segments: cold.delete_segments,
            committed_at: Some(CommittedAtBounds {
                min_micros: None,
                max_micros: Some(cold_visibility_max),
            }),
            // The cold seq fence is upper-only (ADR 0027) —
            // `commit_seq_num <= fence` (inclusive), `fence = max(Pu, W_snap)`.
            // There is deliberately NO per-row lower bound — the segment fetch
            // (`from_micros = snapshotted_at + 1`) plus the snapshot exclusion
            // anti-join handle the baseline overlap. In the happy path
            // `fence = W_snap`, so the persist segments (which all sit above
            // `W_snap`) filter to empty and hot serves the `(W_snap, P]` band.
            // `fence = None` only pre-Persist (cold plan omitted anyway). The
            // merge layer folds `max_seq` into `commit_seq_upper`
            // (`min(fence, as_of_seq)`).
            commit_seq: Some(CommitSeqBounds {
                min_seq: None,
                max_seq: fence,
            }),
        })
    };

    if snapshot_plan.is_some() || persist_plan.is_some() {
        Some(ColdStoragePlan {
            snapshot: snapshot_plan,
            persist: persist_plan,
        })
    } else {
        None
    }
}

/// Hot-tier plan: the per-table log table names + the as-of window + the seq
/// tier fence. The hot lower bound is on `commit_seq_num` — hot serves
/// `commit_seq_num > fence` (`fence = max(Pu, W_snap)`, ADR 0027), carried in
/// `commit_seq.min_seq`. `committed_at` carries only the `max_micros = as_of`
/// visibility cap, with no `min_micros` tier lower bound. `fence` is `None`
/// pre-Persist ⇒ `commit_seq = None`, so hot owns every committed row.
fn plan_hot_storage(fence: Option<i64>, as_of_micros: i64, hot: HotTableNames) -> HotStoragePlan {
    HotStoragePlan {
        upsert_table_name: hot.upsert_table_name,
        delete_table_name: hot.delete_table_name,
        commit_tx_log_table_name: hot.commit_tx_log_table_name,
        committed_at: Some(CommittedAtBounds {
            min_micros: None,
            max_micros: Some(as_of_micros),
        }),
        commit_seq: fence.map(|f| CommitSeqBounds {
            min_seq: Some(f),
            max_seq: None,
        }),
    }
}

/// Does the read's `as_of` fall strictly below the retention floor?
///
/// Compares on the axis the read arrives on — the seq axis when a
/// `plan_commit_seq_upper` is present (an explicit seq read, or the
/// default/OpenTx read pinned on the seq frontier), else the micros axis. Strict
/// `<`: the floor snapshot itself serves an `as_of` equal to it.
/// `LatestSeq`/`OpenTx` carry a current-ish seq ≥ the floor, so a current read
/// never trips it — only a genuine below-floor time-travel read does.
pub(crate) fn retention_floor_below(
    floor: RetentionFloor,
    plan_as_of_micros: i64,
    plan_commit_seq_upper: Option<i64>,
) -> bool {
    match plan_commit_seq_upper {
        Some(seq) => seq < floor.commit_seq_num,
        None => plan_as_of_micros < floor.snapshotted_at_micros,
    }
}

#[cfg(test)]
mod retention_floor_below_tests {
    use super::{RetentionFloor, retention_floor_below};

    const FLOOR: RetentionFloor = RetentionFloor {
        commit_seq_num: 100,
        snapshotted_at_micros: 5_000,
    };

    #[test]
    fn seq_axis_below_at_above() {
        // seq axis is selected by a `Some` commit-seq-upper; micros is inert.
        assert!(retention_floor_below(FLOOR, i64::MAX, Some(99))); // below
        assert!(!retention_floor_below(FLOOR, i64::MAX, Some(100))); // exact floor accepted
        assert!(!retention_floor_below(FLOOR, i64::MAX, Some(101))); // above
    }

    #[test]
    fn micros_axis_below_at_above() {
        // micros axis is selected by `None` commit-seq-upper.
        assert!(retention_floor_below(FLOOR, 4_999, None)); // below
        assert!(!retention_floor_below(FLOOR, 5_000, None)); // exact floor accepted
        assert!(!retention_floor_below(FLOOR, 5_001, None)); // above
    }

    #[test]
    fn latest_seq_read_never_trips_the_floor() {
        // A "read latest" pins commit_seq_upper at the current frontier (>> an
        // old floor), so it is never rejected regardless of the micros value.
        assert!(!retention_floor_below(FLOOR, 0, Some(1_000_000)));
    }
}

#[cfg(test)]
mod cache_decision_tests {
    use super::cached_snapshot_list;
    use penca_storage_meta::SnapshotResult;

    fn result(snapshotted_at: Option<i64>, w_snap: Option<i64>) -> SnapshotResult {
        SnapshotResult {
            snapshotted_at_micros: snapshotted_at,
            commit_seq_num: w_snap,
            snapshot_segments: vec![],
            indexes: Vec::new(),
            partition_keys: None,
            clustering_keys: None,
        }
    }

    #[test]
    fn caches_committed_snapshot() {
        let cached = cached_snapshot_list(&result(Some(100), Some(7)))
            .expect("a committed snapshot (both watermarks Some) is cacheable");
        assert_eq!(cached.snapshotted_at_micros, 100);
        assert_eq!(cached.commit_seq_num, 7);
    }

    #[test]
    fn skips_uncommitted_or_partial() {
        // Not-yet-snapshotted: never cache (else a bogus committed snapshot is
        // served later).
        assert!(cached_snapshot_list(&result(None, None)).is_none());
        // Defensive: a half-populated row (only one watermark) is also skipped.
        assert!(cached_snapshot_list(&result(Some(100), None)).is_none());
        assert!(cached_snapshot_list(&result(None, Some(7))).is_none());
    }
}

#[cfg(test)]
mod assemble_tests {
    use super::{ColdInputs, HotTableNames, assemble_plan, attach_parent_row};
    use penca_core::{Format, IndexSidecar, SnapshotSegment};
    use uuid::Uuid;

    fn hot_names() -> HotTableNames {
        HotTableNames {
            upsert_table_name: "u".into(),
            delete_table_name: "d".into(),
            commit_tx_log_table_name: "t".into(),
        }
    }

    fn empty_cold() -> ColdInputs {
        ColdInputs {
            snapshotted_at_micros: None,
            snapshot_seq: 0,
            snapshot_segments: vec![],
            indexes: Vec::new(),
            upsert_segments: vec![],
            delete_segments: vec![],
        }
    }

    fn persist_seg(uuid: &str) -> penca_core::PersistSegment {
        penca_core::PersistSegment {
            segment_uuid: uuid.into(),
            uri: format!("s3://b/{uuid}.lance"),
            format: penca_core::Format::Lance,
            row_count: 1,
            size_bytes: 1,
            metadata_json: "{}".into(),
            statistics: vec![],
            offset: None,
            length: None,
            max_commit_seq_num: None,
            content_hash: penca_core::naming::deterministic_uuid_from(&[uuid]),
        }
    }

    // Pre-persist (`hot_min == 0`, no persist seq): no cold_storage; hot
    // window `[None, as_of]` and no seq fence — hot owns every committed row.
    #[test]
    fn pre_persist_omits_cold_and_bounds_hot_to_as_of() {
        let plan = assemble_plan(0, None, true, 1_000, empty_cold(), hot_names());
        assert!(
            plan.cold_storage.is_none(),
            "pre-persist (hot_min==0) must omit cold_storage"
        );
        let hot = plan.hot_storage.expect("hot_storage always present");
        let bounds = hot
            .committed_at
            .expect("hot committed_at always set (CHA-361)");
        assert_eq!(
            bounds.min_micros, None,
            "pre-persist hot has no lower bound"
        );
        assert_eq!(bounds.max_micros, Some(1_000), "hot upper bound is as_of");
        assert_eq!(
            hot.commit_seq, None,
            "pre-persist (W_persist None) leaves no seq fence — hot serves all"
        );
    }

    // The post-persist tier partition rides `commit_seq_num`: hot lower ==
    // W_persist (exclusive, on commit_seq.min_seq); cold upper == W_persist
    // (inclusive, on commit_seq.max_seq). The cold side carries NO per-row seq
    // lower — segment fetch + snapshot exclusion own the baseline overlap.
    // `committed_at` stays de-entangled: hot keeps only the as_of cap, cold's
    // upper is `as_of + 1`, not `min(as_of+1, hot_min)`.
    #[test]
    fn post_persist_partitions_on_seq() {
        let cold = ColdInputs {
            snapshotted_at_micros: Some(100),
            snapshot_seq: 7, // W_snap
            snapshot_segments: vec![],
            indexes: Vec::new(),
            upsert_segments: vec![persist_seg("u1")],
            delete_segments: vec![],
        };
        let plan = assemble_plan(500, Some(42), true, 1_000, cold, hot_names());
        let hot = plan.hot_storage.unwrap();
        let hot_at = hot.committed_at.unwrap();
        assert_eq!(
            hot_at.min_micros, None,
            "post-persist hot lower is now the seq fence, not committed_at"
        );
        assert_eq!(hot_at.max_micros, Some(1_000), "hot keeps the as_of cap");
        assert_eq!(
            hot.commit_seq.unwrap().min_seq,
            Some(42),
            "hot serves commit_seq_num > W_persist (exclusive lower on commit_seq)"
        );
        assert_eq!(
            hot.commit_seq.unwrap().max_seq,
            None,
            "hot has no seq upper (as_of visibility caps it instead)"
        );
        let persist = plan.cold_storage.unwrap().persist.unwrap();
        assert_eq!(
            persist.committed_at.unwrap().max_micros,
            Some(1_001),
            "cold committed_at upper de-entangled to as_of+1, not hot_min"
        );
        let cold_seq = persist.commit_seq.unwrap();
        assert_eq!(
            cold_seq.min_seq, None,
            "cold carries no per-row seq lower (exclusion owns the baseline overlap)"
        );
        assert_eq!(
            cold_seq.max_seq,
            Some(42),
            "cold serves commit_seq_num <= W_persist (inclusive upper)"
        );
    }

    // The cold committed_at upper is `as_of + 1` regardless of hot_min — the
    // seq fence owns the tier cutoff, so the micros bound is pure AsOfMicros
    // visibility.
    #[test]
    fn cold_committed_at_upper_is_as_of_plus_one() {
        for (hot_min, as_of) in [(500_i64, 1_000_i64), (5_000, 1_000), (300, 299)] {
            let cold = ColdInputs {
                snapshotted_at_micros: Some(10),
                snapshot_seq: 0,
                snapshot_segments: vec![],
                indexes: Vec::new(),
                upsert_segments: vec![persist_seg("u")],
                delete_segments: vec![],
            };
            let got = assemble_plan(hot_min, Some(9), true, as_of, cold, hot_names())
                .cold_storage
                .unwrap()
                .persist
                .unwrap()
                .committed_at
                .unwrap()
                .max_micros;
            assert_eq!(
                got,
                Some(as_of.saturating_add(1)),
                "cold committed_at upper = as_of+1 for hot_min={hot_min}, as_of={as_of}"
            );
        }
    }

    #[test]
    fn empty_cold_yields_no_cold_storage() {
        let plan = assemble_plan(500, Some(42), true, 1_000, empty_cold(), hot_names());
        assert!(
            plan.cold_storage.is_none(),
            "no snapshot + no persist ⇒ no cold_storage even post-persist"
        );
    }

    #[test]
    fn plan_carries_snapshot_seq_watermark() {
        let cold = ColdInputs {
            snapshotted_at_micros: Some(100),
            snapshot_seq: 5, // W_snap
            snapshot_segments: vec![],
            indexes: Vec::new(),
            upsert_segments: vec![persist_seg("u1")],
            delete_segments: vec![],
        };
        let snap = assemble_plan(500, Some(42), true, 1_000, cold, hot_names())
            .cold_storage
            .expect("post-persist cold_storage present")
            .snapshot
            .expect("snapshot plan present when snapshotted_at_micros is Some");
        assert_eq!(
            snap.commit_seq_num, 5,
            "assemble_plan must stamp W_snap (ColdInputs.snapshot_seq) onto \
             SnapshotPlan.commit_seq_num"
        );
    }

    // `hot_present = false` drops the hot tier (engaging the staged all-cold
    // dispatch), while the cold tier is assembled exactly as when the gate is
    // open.
    #[test]
    fn gate_false_drops_hot_storage_keeping_cold() {
        let cold = ColdInputs {
            snapshotted_at_micros: Some(100),
            snapshot_seq: 7,
            snapshot_segments: vec![],
            indexes: Vec::new(),
            upsert_segments: vec![persist_seg("u1")],
            delete_segments: vec![],
        };
        let plan = assemble_plan(500, Some(42), false, 1_000, cold, hot_names());
        assert!(
            plan.hot_storage.is_none(),
            "hot_present=false must drop the hot tier (all-cold dispatch engages)"
        );
        assert!(
            plan.cold_storage.is_some(),
            "the cold tier is independent of the hot gate"
        );
    }
    fn sidecar(uri: &str) -> IndexSidecar {
        IndexSidecar {
            object_uri: uri.to_string(),
            offset: 0,
            length: 1,
            format: Format::Parquet,
            segment_index_uuid: uri.to_string(),
            size_bytes: 1,
            content_hash: penca_core::naming::deterministic_uuid_from(&[uri]),
        }
    }

    /// The (segment × parent) row-routing helper — identity slot, keyed user
    /// slot, and the unresolved (no child) no-op.
    #[test]
    fn attach_parent_row_routes_slots() {
        let mut segment = SnapshotSegment::default();
        // No parent at all (all-NULL LEFT JOIN row): identity slot stays None.
        attach_parent_row(&mut segment, None, None);
        assert!(segment.row_uuid_index_sidecar.is_none());
        // Identity parent with a child: dedicated slot.
        attach_parent_row(&mut segment, None, Some(sidecar("mem://identity")));
        assert_eq!(
            segment.row_uuid_index_sidecar.as_ref().unwrap().object_uri,
            "mem://identity"
        );
        // User parent with a child: keyed slot, uuid stringified.
        let user = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        attach_parent_row(&mut segment, Some(user), Some(sidecar("mem://user")));
        assert_eq!(segment.index_sidecars.len(), 1);
        assert_eq!(segment.index_sidecars[0].0, user.to_string());
        // User parent WITHOUT a child for this segment: nothing attached —
        // the seek treats the entry as unresolved (safe over-selection).
        attach_parent_row(&mut segment, Some(user), None);
        assert_eq!(segment.index_sidecars.len(), 1);
        assert!(
            segment.row_uuid_index_sidecar.is_some(),
            "identity slot untouched"
        );
    }
}

#[cfg(test)]
mod overlap_clause_tests {
    use super::persist_segment_overlap_clause;

    #[test]
    fn overlap_clause_empty_when_all_unset() {
        assert_eq!(persist_segment_overlap_clause(None, None, None), "");
    }

    #[test]
    fn overlap_clause_emits_committed_at_window_without_seq() {
        // The micros / OpenTx axes pass `commit_seq_upper = None`: the
        // committed_at overlap stands alone, no seq predicate leaks in.
        let clause = persist_segment_overlap_clause(Some(99), Some(200), None);
        assert_eq!(
            clause,
            " AND seg.max_tx_commit_micros > 99 AND seg.min_tx_commit_micros <= 200"
        );
        assert!(!clause.contains("commit_seq_num"));
    }

    #[test]
    fn overlap_clause_appends_seq_skip_after_committed_at() {
        // A seq-axis read ANDs `min_commit_seq_num <= N` onto the committed_at
        // tier fence — it composes, it does not replace.
        assert_eq!(
            persist_segment_overlap_clause(Some(99), Some(200), Some(7)),
            " AND seg.max_tx_commit_micros > 99 \
             AND seg.min_tx_commit_micros <= 200 AND seg.min_commit_seq_num <= 7"
        );
    }

    #[test]
    fn overlap_clause_seq_skip_alone_when_no_committed_at_bounds() {
        assert_eq!(
            persist_segment_overlap_clause(None, None, Some(0)),
            " AND seg.min_commit_seq_num <= 0"
        );
    }
}
