//! Per-tier resolve + cross-tier union for the merge-on-read pipeline.

use std::collections::{HashMap, HashSet};

use arrow::array::{AsArray, BooleanArray, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use penca_core::{CommittedAtBounds, Plan};
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::DbDriver;
use penca_dl::dialect::DfDialect;
use penca_dl::driver::DlDriver;
use penca_dl::schema::{DELETE_LOG_TABLE, LogSchemas, UPSERT_LOG_TABLE};
use penca_storage_hot::execute_query_as_batch;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::schema::resolved_schema;
use crate::sql::{build_cold_merge_resolved, build_merge_resolved};
use crate::{MergeError, ReadSnapshot};

pub(crate) fn string_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a StringArray, MergeError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| MergeError::MissingColumn(name.to_string()))?;
    Ok(batch.column(idx).as_string())
}

pub(crate) fn int64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a Int64Array, MergeError> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| MergeError::MissingColumn(name.to_string()))?;
    Ok(batch.column(idx).as_primitive())
}

pub(crate) fn collect_row_uuids(batch: &RecordBatch) -> Result<Vec<String>, MergeError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let col = string_column(batch, "row_uuid")?;
    Ok((0..batch.num_rows())
        .map(|i| col.value(i).to_string())
        .collect())
}

pub(crate) fn extract_committed_at_bounds(
    filter: Option<&CommittedAtBounds>,
) -> (Option<i64>, Option<i64>) {
    match filter {
        Some(f) => (f.min_micros, f.max_micros),
        None => (None, None),
    }
}

/// The per-row `commit_seq_num <= N` upper bound the cold merge SQL must apply
/// for a seq-axis read. `AsOfSeq` carries one (`<= N`); `OpenTx` carries
/// `began_at_seq_num - 1` (snapshot isolation is enforced read-side — see
/// [`ReadSnapshot::plan_commit_seq_upper`]); `AsOfMicros` folds its visibility
/// into the committed_at window, so it has none.
///
/// Same bound the planner uses to skip whole cold segments
/// ([`ReadSnapshot::plan_commit_seq_upper`]) — the per-row filter here and
/// the segment skip there must agree, so this defers to that one source.
pub(crate) fn cold_commit_seq_upper(snapshot: &ReadSnapshot) -> Option<i64> {
    snapshot.plan_commit_seq_upper()
}

/// Fold the cold-tier seq fence `W_persist`
/// (`PersistPlan.commit_seq.max_seq`) together with the `AsOfSeq` visibility
/// cap into a single inclusive `commit_seq_num <= N` bound, `min` of the two.
/// The tier fence means cold serves `commit_seq_num <= W_persist`; the as-of
/// cap is the seq-axis time
/// travel. Either may be absent (e.g. the snapshot-write path carries no
/// `commit_seq`, an `AsOfMicros` read carries no as-of seq); `None` for both
/// leaves the cold read unbounded above on the seq axis (the committed_at
/// window then stands alone, as on the write path).
fn fold_cold_seq_upper(as_of_seq: Option<i64>, tier_upper: Option<i64>) -> Option<i64> {
    match (as_of_seq, tier_upper) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

#[tracing::instrument(
    skip_all,
    fields(
        snapshot = ?snapshot,
        has_hot_plan = plan.hot_storage.is_some(),
        hot_tier_seq_lower = tracing::field::Empty,
        resolved_rows = tracing::field::Empty,
    ),
)]
pub(crate) async fn resolve_hot<D: DbDriver<Row = PgRow>>(
    plan: &Plan,
    driver: &D,
    user_cols: &[&str],
    user_schema: &SchemaRef,
    snapshot: &ReadSnapshot,
    row_uuids: Option<&[Uuid]>,
) -> Result<RecordBatch, MergeError> {
    let out_schema = resolved_schema(user_schema);
    let Some(hot_plan) = &plan.hot_storage else {
        return Ok(RecordBatch::new_empty(out_schema));
    };

    let (_min, hot_max) = extract_committed_at_bounds(hot_plan.committed_at.as_ref());
    // The hot↔cold tier fence is the seq lower `commit_seq_num > W_persist`,
    // carried on `commit_seq.min_seq`. `None` pre-Persist.
    let tier_seq_lower = hot_plan.commit_seq.and_then(|c| c.min_seq);
    if let Some(w) = tier_seq_lower {
        tracing::Span::current().record("hot_tier_seq_lower", w);
    }
    let snapshot = snapshot.tighten_for_hot(hot_max);

    let sql = build_merge_resolved::<PgDialect>(
        &hot_plan.upsert_table_name,
        &hot_plan.delete_table_name,
        &hot_plan.commit_tx_log_table_name,
        user_cols,
        tier_seq_lower,
        &snapshot,
        row_uuids,
    );

    // Non-cursor one-shot: the next step (dedup by `row_uuid`) needs the
    // full resolved set in memory, so the cursor's per-batch round-trips
    // would be pure overhead.
    let batch = execute_query_as_batch(driver, &sql, &[], &out_schema).await?;
    tracing::Span::current().record("resolved_rows", batch.num_rows() as i64);
    Ok(batch)
}

#[tracing::instrument(
    skip_all,
    fields(
        has_cold_plan = plan.cold_storage.is_some(),
        persist_committed_at_present = tracing::field::Empty,
        resolved_rows = tracing::field::Empty,
    ),
)]
// The cold-read param set (plan + dl + schemas + committed_at/seq bounds) is
// irreducible here.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_cold<L: DlDriver + ?Sized>(
    plan: &Plan,
    dl: &L,
    user_cols: &[&str],
    user_schema: &SchemaRef,
    log_schemas: &LogSchemas,
    commit_seq_upper: Option<i64>,
    row_uuids: Option<&[Uuid]>,
) -> Result<RecordBatch, MergeError> {
    let out_schema = resolved_schema(user_schema);
    let Some(cold_plan) = &plan.cold_storage else {
        return Ok(RecordBatch::new_empty(out_schema));
    };

    let persist_committed_at = cold_plan
        .persist
        .as_ref()
        .and_then(|p| p.committed_at.as_ref());
    tracing::Span::current().record(
        "persist_committed_at_present",
        persist_committed_at.is_some(),
    );

    // Snapshot-only cold plan (no persist segments): the cold resolution query
    // would scan empty `upsert_log`/`delete_log` and return nothing, yet still
    // pay a fresh DataFusion SessionContext build + query plan (~6 ms). This
    // query only covers the persist tier; snapshot rows are read separately
    // via `scan_snapshot`.
    if cold_plan.persist.is_none() {
        tracing::Span::current().record("resolved_rows", 0i64);
        return Ok(RecordBatch::new_empty(out_schema));
    }

    let (committed_from, committed_to) = extract_committed_at_bounds(persist_committed_at);
    // Fold the cold tier upper `W_persist` into the seq bound.
    let tier_seq_upper = cold_plan
        .persist
        .as_ref()
        .and_then(|p| p.commit_seq)
        .and_then(|c| c.max_seq);
    let commit_seq_upper = fold_cold_seq_upper(commit_seq_upper, tier_seq_upper);
    let sql = build_cold_merge_resolved::<DfDialect>(
        UPSERT_LOG_TABLE,
        DELETE_LOG_TABLE,
        user_cols,
        committed_from,
        committed_to,
        commit_seq_upper,
        row_uuids,
    );
    let batch = dl.execute_sql(cold_plan, &sql, log_schemas).await?;
    tracing::Span::current().record("resolved_rows", batch.num_rows() as i64);
    Ok(batch)
}

/// Union two resolved batches (hot + cold) and keep the row with the
/// latest `commit_micros` per `row_uuid`.
///
/// Every *within-tier* ordering site keys on `commit_seq_num`, but this
/// *cross-tier* dedup intentionally keys on `commit_micros`. It is correct:
/// the hot/cold partition is strict on `commit_micros` (cold serves
/// `< hot_min`, hot serves `>= hot_min` — ADR 0019), so no
/// `committed_at`/`seq` inversion can straddle the tier boundary, and the
/// resolved batch carries no `commit_seq_num` column to key on anyway. The
/// ties that motivate keying on seq (same `commit_micros`, different
/// `commit_seq_num`) only occur *inside* a tier, where the per-tier merge SQL
/// already broke them on `commit_seq_num`.
pub(crate) fn union_latest(
    schema: &SchemaRef,
    a: &RecordBatch,
    b: &RecordBatch,
) -> Result<RecordBatch, MergeError> {
    if a.num_rows() == 0 && b.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let combined = if a.num_rows() == 0 {
        b.clone()
    } else if b.num_rows() == 0 {
        a.clone()
    } else {
        compute::concat_batches(schema, &[a.clone(), b.clone()])?
    };
    dedup_by_row_uuid(&combined)
}

/// Keep only the live (`is_delete = false`) rows of a resolved batch.
///
/// The dropped tombstones have already contributed their `row_uuid` to the
/// exclusion set upstream, so this only trims the emitted delta. A stale cold
/// version can never survive here: the resolves are unfiltered, so
/// `union_latest` always sees the newer hot version and keeps it (higher
/// `commit_micros` by the tier fence), and the residual then drops it if the
/// filter excludes it.
pub(crate) fn filter_live_rows(batch: &RecordBatch) -> Result<RecordBatch, MergeError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let idx = batch
        .schema()
        .index_of("is_delete")
        .map_err(|_| MergeError::MissingColumn("is_delete".to_string()))?;
    let is_delete = batch.column(idx).as_boolean();
    let keep: BooleanArray = (0..batch.num_rows()).map(|i| !is_delete.value(i)).collect();
    Ok(compute::filter_record_batch(batch, &keep)?)
}

/// Fan out the per-tier resolves, union the two resolved batches by latest
/// `commit_micros` per `row_uuid`, and fold every shadowing `row_uuid` into
/// the exclusion set. Returns the resolved batch + the exclusion set used to
/// filter the snapshot stream in Phase 3.
///
/// `resolve_hot` and `resolve_cold` each return the latest committed version
/// per `row_uuid` across BOTH logs — visible upserts (`is_delete = false`) and
/// winning tombstones (`is_delete = true`). Their cross-tier latest-wins union
/// yields one row per touched `row_uuid`; the full `row_uuid` set of that union
/// IS the exclusion set (it shadows same-uuid snapshot rows), and the
/// `is_delete = false` subset is the live delta.
#[tracing::instrument(
    skip_all,
    fields(
        snapshot = ?snapshot,
        has_hot_plan = plan.hot_storage.is_some(),
        has_cold_plan = plan.cold_storage.is_some(),
        resolved_rows = tracing::field::Empty,
        exclusion_set_size = tracing::field::Empty,
    ),
)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_resolved_and_exclusion_set<'a, D, L>(
    plan: &'a Plan,
    driver: &'a D,
    dl: &'a L,
    user_cols: &[&str],
    user_schema: &'a SchemaRef,
    log_schemas: &LogSchemas,
    snapshot: &'a ReadSnapshot,
    row_uuids: Option<&'a [Uuid]>,
) -> Result<(RecordBatch, HashSet<String>), MergeError>
where
    D: DbDriver<Row = PgRow>,
    L: DlDriver + ?Sized,
{
    let resolved_schema_ref = resolved_schema(user_schema);
    let commit_seq_upper = cold_commit_seq_upper(snapshot);

    // One scan per tier. Arm order is load-bearing for failure attribution
    // (errors surface in arm order): hot then cold.
    let (hot_a, cold_a) = tokio::try_join!(
        resolve_hot(plan, driver, user_cols, user_schema, snapshot, row_uuids),
        resolve_cold(
            plan,
            dl,
            user_cols,
            user_schema,
            log_schemas,
            commit_seq_upper,
            row_uuids,
        ),
    )?;

    // Cross-tier latest-wins dedup, then split into (live delta, exclusion set):
    // one row per touched row_uuid, carrying the winner's is_delete flag (hot's
    // commit_micros > cold's by the tier fence, so the hot version wins whenever
    // a row_uuid is in both).
    let (resolved, exclusion_set) =
        compose_resolved_and_exclusion(&resolved_schema_ref, &hot_a, &cold_a)?;

    tracing::Span::current().record("resolved_rows", resolved.num_rows() as i64);
    tracing::Span::current().record("exclusion_set_size", exclusion_set.len() as i64);

    Ok((resolved, exclusion_set))
}

/// Compose the hot + cold resolved batches into the `(live delta, exclusion
/// set)` pair. Shared by the mixed and all-cold builders, which differ
/// only in how they obtain `hot_a`/`cold_a` (the mixed path fans out both
/// probes; the all-cold path passes an empty `hot_a`).
///
/// Cross-tier latest-wins dedup ([`union_latest`]) yields one row per touched
/// `row_uuid` carrying the winner's `is_delete` flag. From that composed batch:
/// the **exclusion set** is EVERY touched `row_uuid` (upsert-winner or
/// tombstone-winner — each shadows a same-uuid snapshot row), derived BEFORE any
/// residual so it stays filter-independent; the **live delta** is the
/// surviving `is_delete = false` rows (tombstones only contributed their
/// `row_uuid` to the exclusion set).
fn compose_resolved_and_exclusion(
    schema: &SchemaRef,
    hot_a: &RecordBatch,
    cold_a: &RecordBatch,
) -> Result<(RecordBatch, HashSet<String>), MergeError> {
    let composed = union_latest(schema, hot_a, cold_a)?;
    let exclusion_set: HashSet<String> = collect_row_uuids(&composed)?.into_iter().collect();
    let resolved = filter_live_rows(&composed)?;
    Ok((resolved, exclusion_set))
}

/// Cold-only sibling of [`build_resolved_and_exclusion_set`] for plans
/// with no hot tier (`plan.hot_storage == None`): the resolved batch is
/// deduped by latest `commit_micros` per `row_uuid`, and every shadowing
/// `row_uuid` is folded into the exclusion set. No hot probe appears in
/// the flow.
///
/// `snapshot` supplies the read-time seq upper bound
/// ([`cold_commit_seq_upper`]) — for the lifecycle snapshot writer's
/// `AsOfMicros` read it folds to `None` (cold visibility is decided by the
/// plan's persist `committed_at` window). `read_data` routes fully-cold user
/// reads here too, so an `OpenTx` (`began_at_seq_num - 1`) or `AsOfSeq` read
/// must thread its per-row seq cutoff exactly as the mixed path does —
/// otherwise a row committed after an open tx began would leak from cold.
#[tracing::instrument(
    skip_all,
    fields(
        has_cold_plan = plan.cold_storage.is_some(),
        resolved_rows = tracing::field::Empty,
        exclusion_set_size = tracing::field::Empty,
    ),
)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_cold_resolved_and_exclusion_set<'a, L>(
    plan: &'a Plan,
    dl: &'a L,
    user_cols: &[&str],
    user_schema: &'a SchemaRef,
    log_schemas: &LogSchemas,
    snapshot: &'a ReadSnapshot,
    row_uuids: Option<&'a [Uuid]>,
) -> Result<(RecordBatch, HashSet<String>), MergeError>
where
    L: DlDriver + ?Sized,
{
    let resolved_schema_ref = resolved_schema(user_schema);

    // Thread the read-time seq upper into the cold resolve exactly as the mixed
    // path (`build_resolved_and_exclusion_set`) does. `AsOfMicros` (lifecycle
    // snapshot writer) folds to `None` — no seq-axis bound, cold visibility
    // rides the plan's `committed_at` window. `OpenTx` / `AsOfSeq` reads carry
    // their `commit_seq_num <=` cutoff so a post-`began_at` commit can't leak
    // from cold.
    let commit_seq_upper = cold_commit_seq_upper(snapshot);

    let cold_a = resolve_cold(
        plan,
        dl,
        user_cols,
        user_schema,
        log_schemas,
        commit_seq_upper,
        row_uuids,
    )
    .await?;

    // Mirrors the merged path with an empty hot batch (a row_uuid appears at
    // most once already, since the upsert/tombstone arms are mutually exclusive,
    // so the dedup is defensive).
    let (resolved, exclusion_set) = compose_resolved_and_exclusion(
        &resolved_schema_ref,
        &RecordBatch::new_empty(resolved_schema_ref.clone()),
        &cold_a,
    )?;

    tracing::Span::current().record("resolved_rows", resolved.num_rows() as i64);
    tracing::Span::current().record("exclusion_set_size", exclusion_set.len() as i64);

    Ok((resolved, exclusion_set))
}

fn dedup_by_row_uuid(batch: &RecordBatch) -> Result<RecordBatch, MergeError> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let uuids = string_column(batch, "row_uuid")?;
    let timestamps = int64_column(batch, "commit_micros")?;
    let mut best: HashMap<&str, (usize, i64)> = HashMap::new();
    for i in 0..batch.num_rows() {
        let uuid = uuids.value(i);
        let ts = timestamps.value(i);
        best.entry(uuid)
            .and_modify(|(idx, existing)| {
                if ts > *existing {
                    *idx = i;
                    *existing = ts;
                }
            })
            .or_insert((i, ts));
    }
    let mut keep = vec![false; batch.num_rows()];
    for (idx, _) in best.values() {
        keep[*idx] = true;
    }
    let mask = BooleanArray::from(keep);
    Ok(compute::filter_record_batch(batch, &mask)?)
}

#[cfg(test)]
mod tests {
    use super::{compose_resolved_and_exclusion, fold_cold_seq_upper};
    use crate::schema::resolved_schema;
    use crate::schema::test_fixtures::{resolved_batch_nullable, test_user_schema};

    /// CHA-524, the production shape: the tombstone is in HOT (committed after
    /// the snapshot) while its upsert is already COLD, so the hot arm's
    /// `deletes d LEFT JOIN latest l` finds no `latest` row and emits NULL user
    /// columns. Both batches are non-empty here, so — unlike the all-cold path,
    /// where `union_latest` short-circuits on the empty hot side — this drives
    /// the NULLs through `concat_batches` against the carrier schema and then
    /// through the cross-tier `dedup_by_row_uuid`.
    #[test]
    fn hot_null_tombstone_beats_cold_upsert_and_feeds_the_exclusion_set() {
        let schema = resolved_schema(&test_user_schema());
        let hot = resolved_batch_nullable(&["r1"], &[None], &[None], &[200], &[true]);
        let cold = resolved_batch_nullable(&["r1"], &[Some("a")], &[Some(1)], &[100], &[false]);

        let (resolved, exclusion) = compose_resolved_and_exclusion(&schema, &hot, &cold)
            .expect("a NULL-carrying hot tombstone must not abort the composition");

        assert_eq!(
            resolved.num_rows(),
            0,
            "the newer tombstone must shadow the cold upsert out of the live delta"
        );
        assert!(
            exclusion.contains("r1"),
            "the tombstone must still shadow its snapshot version"
        );
    }

    // The cold seq upper folds the tier fence `W_persist`
    // with the `AsOfSeq` visibility cap into one inclusive `<= min(..)` bound.
    #[test]
    fn fold_cold_seq_upper_takes_min_when_both_present() {
        assert_eq!(fold_cold_seq_upper(Some(7), Some(42)), Some(7));
        assert_eq!(fold_cold_seq_upper(Some(99), Some(42)), Some(42));
    }

    #[test]
    fn fold_cold_seq_upper_passes_through_lone_bound() {
        // AsOfMicros read post-Persist: no as-of seq, tier fence present.
        assert_eq!(fold_cold_seq_upper(None, Some(42)), Some(42));
        // Snapshot-write path: no tier fence, no as-of seq.
        assert_eq!(fold_cold_seq_upper(None, None), None);
        // AsOfSeq read with no tier fence stamped (defensive).
        assert_eq!(fold_cold_seq_upper(Some(7), None), Some(7));
    }
}
