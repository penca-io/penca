//! Persist: hot → cold movement for a single table (CHA-220). CHA-444
//! (ADR 0027): pure committed-only CDC — no open-tx clamp, no aborted-hot
//! cleanup (both removed; aborts move to Purge).
//!
//! [`LifecycleManager::persist`] is the public entry; `persist_locked`
//! runs the orchestration inside the per-table advisory lock. Step helpers:
//! `effective_target`, `read_hot_rows`, `compute_candidate_persisted_at_gated`,
//! `phase1_durable_writes`, `cleanup_persist_parents`, `phase2_parent_flip`.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::record_batch::RecordBatch;
use penca_core::LogKind;
use penca_core::naming::{
    self, commit_tx_log_partition, delete_log_table, persist_segment_uri,
    system_schemas_table_uuid, system_tables_table_uuid, table_persist_segment_uuid,
    upsert_log_table,
};
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{PersistRequest, PersistResponse};
use penca_storage_hot::HotStorageClient;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::lifecycle::batch_util::{
    batch_commit_seq_num_bounds, batch_committed_at_bounds, hot_upsert_read_schema,
    project_to_cold_layout, sort_record_batch_by_keys,
};
use crate::lifecycle::chunker::chunk_row_ranges;
use crate::lifecycle::durable_writer::{
    DurableSegmentWriter, PersistSegmentScope, PersistSegmentStep,
};

/// Chunk a per-`log_kind` cold batch into size-bounded segment batches
/// (`max_segment_bytes` cap, CHA-215), returning each chunk paired with its
/// in-memory byte size in chunk order.
///
/// CHA-410 / CHA-431: the returned chunks are ordered by the composite
/// `(commit_seq_num, write_seq_num)` — both within each chunk and across chunks
/// (chunk N's max < chunk N+1's min) — so the concatenated persist read stream
/// is honestly ordered on the total version-order axis that
/// `PersistTableProvider` advertises. The ordering guarantee is what lets
/// DataFusion elide a redundant `SortExec`.
fn chunk_persist_batch(
    batch: &RecordBatch,
    max_segment_bytes: i64,
) -> Result<Vec<(RecordBatch, i64)>, ApiError> {
    // CHA-431: `(commit_seq_num, write_seq_num)` is the complete total version order
    // — `commit_seq_num` is the gapless per-commit serial (CHA-430), `write_seq_num`
    // sub-orders mutations within one commit. Sorting each cold batch by the
    // composite is the within-segment half of the honesty contract; the
    // cross-segment half is the plan-side `ORDER BY min_commit_seq_num, chunk_idx`
    // (within one persist op `chunk_idx` already follows this sort; across
    // persist ops the `commit_seq_num` ranges are disjoint), so the concatenated
    // stream is globally non-decreasing in `(commit_seq_num, write_seq_num)`.
    //
    // sort_record_batch_by_keys uses Arrow's default SortOptions (ASC NULLS
    // FIRST) while PersistTableProvider advertises ASC NULLS LAST. The mismatch
    // stays moot because BOTH `commit_seq_num` and `write_seq_num` are non-nullable
    // (CHA-431 stamps `write_seq_num` via a NOT NULL DEFAULT nextval) — there are
    // no nulls to place, so the two nulls-placements never diverge.
    let sorted = sort_record_batch_by_keys(
        batch,
        &["commit_seq_num".to_string(), "write_seq_num".to_string()],
    )?;
    let ranges = chunk_row_ranges(&sorted, max_segment_bytes)?;
    Ok(ranges
        .into_iter()
        .map(|(offset, len, in_memory_bytes)| (sorted.slice(offset, len), in_memory_bytes))
        .collect())
}

impl LifecycleManager {
    /// Persist hot storage data to cold storage for a single table (CHA-220).
    ///
    /// Writes T's committed hot upsert + delete log contents up to
    /// `target_micros ?? now` as cold persist segments and commits the
    /// corresponding `table_persist_metadata` rows. **Does NOT remove hot
    /// rows** —
    /// they stay queryable until `purge` runs. Between Persist(T) and
    /// Purge(T) the same row exists in both tiers; the merge layer's
    /// per-`row_uuid` dedup collapses the temporary double presence.
    ///
    /// CHA-218: cold persist segments pre-join the four tx metadata
    /// columns (`commit_micros, began_at_micros, comment,
    /// author`) onto each upsert/delete row at write time, so the
    /// cold side reads as a pure scan. See ADR 0017.
    ///
    /// Metadata layout (CHA-220 removed the per-branch parent):
    /// - `TABLE_PERSIST_METADATA` — one row per `(table, persisted_at,
    ///   log_kind)` triple. `log_kind ∈ {upsert_log, delete_log}`.
    /// - `TABLE_PERSIST_SEGMENT_METADATA` — one row per cold file. Per-
    ///   segment `commit_micros` is the only plan-visibility
    ///   gate.
    ///
    /// Phase 1 — durable writes (per-segment incremental commits):
    /// for each non-empty `log_kind`, INSERT `table_persist_metadata`
    /// (NULL `committed_at`), then write the cold segment files +
    /// `table_persist_segment_metadata` incrementally — each row
    /// carries its tx metadata columns inline.
    ///
    /// Phase 2 — parent flips (one PG transaction): UPDATE every
    /// per-`log_kind` `table_persist_metadata` row to NOT-NULL
    /// `committed_at`. No hot deletes, no tx-log family deletes —
    /// those move to `purge` and CHA-221 respectively.
    ///
    /// Locked per-table via `persist:{table_uuid}:{branch_uuid}` —
    /// serializes `Persist(T)` against `Persist(T)` only (still
    /// load-bearing: two concurrent Persists would write overlapping
    /// cold segments and corrupt `audit_data`'s strict tier
    /// partition). Cross-operation pairs (`Persist↔Snapshot`,
    /// `Persist↔Purge`) are lock-free under ADR 0019 — pillars 1
    /// (plan-time threading) and 3 (grace window) make them safe
    /// without serialization. `Persist(T1)` and `Persist(T2)` run in
    /// parallel on different keys.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn persist<L, W>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        dl_driver: &L,
        writer: &W,
        request: &PersistRequest,
    ) -> Result<PersistResponse, ApiError>
    where
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        // `step1_now` pins every metadata read in this op (name
        // resolution + arrow-schema/PK read) to one PG-side timestamp.
        // Same value drives `effective_target` inside the lock. See
        // `resolve_catalog_branch_and_table`'s doc-comment for the
        // consistency rationale.
        let step1_now = penca_storage_meta::LifecycleManager::now_micros(pool).await?;
        let snapshot = penca_merge::ReadSnapshot::AsOfMicros(step1_now);
        let (catalog_uuid, branch_uuid, table_uuid) = self
            .resolve_catalog_branch_and_table(
                pool,
                dl_driver,
                request.catalog_uuid.as_deref(),
                request.catalog_name.as_deref(),
                request.schema_uuid.as_deref(),
                request.schema_name.as_deref(),
                request.branch_uuid.as_deref(),
                request.branch_name.as_deref(),
                request.table_uuid.as_deref(),
                request.table_name.as_deref(),
                &snapshot,
            )
            .await?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));

        let lock_key = format!("persist:{table_uuid}:{branch_uuid}");
        pool.advisory_lock(&lock_key, async || {
            self.persist_locked(
                pool,
                hot,
                dl_driver,
                writer,
                request,
                catalog_uuid,
                branch_uuid,
                table_uuid,
                step1_now,
                &snapshot,
            )
            .await
        })
        .await
    }

    /// Step 1 of `persist_locked`: the target micros — the request's
    /// `target_micros` or `step1_now`.
    ///
    /// CHA-444 (ADR 0027): Persist is pure committed-only CDC and **no
    /// longer clamps to the oldest open tx**. The old clamp
    /// (`effective_target < oldest_open_began_at`) only kept the shared
    /// *micros* watermark below open-tx begins to absorb out-of-order
    /// wall-clock commits; the `commit_seq_num` axis (CHA-428: a strictly-
    /// monotonic seq allocated *at commit*) obsoletes it — an open tx has
    /// no seq and is invisible, and on commit it gets a fresh max seq above
    /// every fence, so it cannot be stranded below `P`/`Pu`.
    fn effective_target(request_target_micros: Option<i64>, step1_now: i64) -> i64 {
        request_target_micros.unwrap_or(step1_now)
    }

    /// Step 2 of `persist_locked`: read T's committed hot upsert + delete
    /// rows committed at or before `effective_target`. CHA-444 (ADR 0027):
    /// committed-only — aborted hot rows are no longer Persist's concern
    /// (they move to Purge). The user schema + PKs are resolved internally
    /// (system tables use their hard-coded schemas; user tables read
    /// `__penca_system__.tables` under `snapshot`) and consumed by
    /// the two `read_committed_*` calls — neither value is needed
    /// after the helper returns.
    #[allow(clippy::too_many_arguments)]
    async fn read_hot_rows<L>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        dl_driver: &L,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        effective_target: i64,
        snapshot: &penca_merge::ReadSnapshot,
    ) -> Result<(RecordBatch, RecordBatch), ApiError>
    where
        L: DlDriver + ?Sized,
    {
        // `__penca_system__.{schemas,tables}` are real Penca Tables
        // (CHA-177); their data persists the same way as user tables.
        let sys_schemas_uuid = system_schemas_table_uuid(catalog_uuid);
        let sys_tables_uuid = system_tables_table_uuid(catalog_uuid);
        // CHA-185: persist also needs primary_keys for the widened
        // delete_log shape. CHA-380: system tables declare their entity-uuid
        // PK column (schema_uuid / table_uuid), so persist widens their
        // delete logs like every other table. `__penca_system__.indexes`
        // is not special-cased here — it reads its PK (`index_uuid`) off its
        // self-describing `__penca_system__.tables` row via the else arm.
        let (user_schema, primary_keys): (SchemaRef, Vec<String>) = if *table_uuid
            == sys_schemas_uuid
        {
            (
                Arc::new(PgDialect::system_schemas_arrow_schema()),
                PgDialect::system_schemas_primary_keys(),
            )
        } else if *table_uuid == sys_tables_uuid {
            (
                Arc::new(PgDialect::system_tables_arrow_schema()),
                PgDialect::system_tables_primary_keys(),
            )
        } else {
            let (arrow_schema_bytes, pks) = self
                .query_manager
                .get_table_metadata_by_branch(
                    pool,
                    dl_driver,
                    catalog_uuid,
                    branch_uuid,
                    table_uuid,
                    snapshot,
                )
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!("Table metadata not found: {table_uuid}"))
                })?;
            (
                Arc::new(try_schema_from_ipc_buffer(&arrow_schema_bytes).map_err(ApiError::Arrow)?),
                pks,
            )
        };
        let upsert_schema = hot_upsert_read_schema(&user_schema);

        let upsert_table = upsert_log_table(table_uuid, branch_uuid);
        let delete_table = delete_log_table(table_uuid, branch_uuid);
        let commit_tx_log_table = commit_tx_log_partition(catalog_uuid, branch_uuid);
        // Use exclusive upper bound: committed_at < effective_target + 1.
        let max_filter = Some(effective_target.saturating_add(1));

        let upsert_rows = hot
            .read_committed_upserts(
                pool,
                &upsert_table,
                &commit_tx_log_table,
                &upsert_schema,
                None,
                max_filter,
            )
            .await?;
        let delete_rows = hot
            .read_committed_deletes(
                pool,
                &delete_table,
                &commit_tx_log_table,
                &user_schema,
                &primary_keys,
                None,
                max_filter,
            )
            .await?;

        Ok((upsert_rows, delete_rows))
    }

    /// CHA-227: the candidate `persisted_at` is `max(committed_at)` over
    /// the committed rows being persisted. CHA-444 (ADR 0027): committed-
    /// only — the aborted-watermark broadening is gone (aborts move to
    /// Purge). Then the strict-advance gate: if the candidate wouldn't move
    /// past the last committed Persist watermark, return `None` so the
    /// caller can no-op without writing a redundant
    /// `table_persist_metadata` row. Assumes at least one batch is non-empty
    /// (caller's contract).
    async fn compute_candidate_persisted_at_gated(
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        upsert_batch: &RecordBatch,
        delete_batch: &RecordBatch,
    ) -> Result<Option<(i64, Option<i64>)>, ApiError> {
        let max_committed_at: Option<i64> = {
            let mut acc: Option<i64> = None;
            for batch in [upsert_batch, delete_batch] {
                if batch.num_rows() == 0 {
                    continue;
                }
                let (_, max) = batch_committed_at_bounds(batch)?;
                acc = Some(acc.map_or(max, |cur| cur.max(max)));
            }
            acc
        };
        // CHA-443 (IMPL-1): the persist SEQ watermark — MAX(commit_seq_num) over the
        // committed rows being persisted (the seq sibling of max_committed_at;
        // aborts carry no seq). `None` when there are no committed rows
        // (aborts-only) → the parent row's commit_seq_num is NULL.
        let max_committed_commit_seq_num: Option<i64> = {
            let mut acc: Option<i64> = None;
            for batch in [upsert_batch, delete_batch] {
                if batch.num_rows() == 0 {
                    continue;
                }
                let (_, max) = batch_commit_seq_num_bounds(batch)?;
                acc = Some(acc.map_or(max, |cur| cur.max(max)));
            }
            acc
        };
        let candidate_persisted_at: i64 =
            max_committed_at.expect("caller ensures at least one committed batch is non-empty");

        let last_persisted_at =
            penca_storage_meta::LifecycleManager::latest_committed_table_persist_watermark(
                pool,
                &catalog_uuid.to_string(),
                &branch_uuid.to_string(),
                &table_uuid.to_string(),
            )
            .await?;
        if let Some(last) = last_persisted_at
            && candidate_persisted_at <= last
        {
            return Ok(None);
        }
        Ok(Some((candidate_persisted_at, max_committed_commit_seq_num)))
    }

    /// Phase 1 of `persist_locked`: write the cold-tier segments + the
    /// per-`log_kind` `table_persist_metadata` rows (uncommitted). On
    /// any error the caller invokes `seg_writer.cleanup_on_err` (for
    /// segment files + uncommitted segment rows) and
    /// [`Self::cleanup_persist_parents`] (for parent rows).
    #[allow(clippy::too_many_arguments)]
    async fn phase1_durable_writes<W>(
        &self,
        pool: &PgDriver,
        writer: &W,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        effective_target: i64,
        upsert_batch: &RecordBatch,
        delete_batch: &RecordBatch,
        max_persisted_committed_at: i64,
        persist_commit_seq_num: Option<i64>,
        seg_writer: &mut DurableSegmentWriter<PersistSegmentScope<'_>>,
        table_persist_uuid_strs: &mut Vec<String>,
    ) -> Result<(), ApiError>
    where
        W: FormatWriter,
    {
        let storage_format_text = self.storage_format.extension();
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();

        // Per-`log_kind` write loop. Each kind gets its own
        // deterministic table_persist_uuid (rooted in
        // `(catalog, branch, table, persisted_at, log_kind)`) and one
        // or more sibling segments under it (CHA-215 chunker).
        for (log_kind, batch) in [
            (LogKind::UpsertLog, upsert_batch),
            (LogKind::DeleteLog, delete_batch),
        ] {
            if batch.num_rows() == 0 {
                continue;
            }
            let table_persist_uuid = naming::table_persist_uuid(
                catalog_uuid,
                branch_uuid,
                table_uuid,
                effective_target,
                log_kind,
            );
            let table_persist_uuid_str = table_persist_uuid.to_string();
            penca_storage_meta::LifecycleManager::insert_table_persist(
                pool,
                &catalog_str,
                &table_persist_uuid_str,
                &branch_str,
                &table_str,
                max_persisted_committed_at,
                log_kind,
                persist_commit_seq_num,
            )
            .await?;
            table_persist_uuid_strs.push(table_persist_uuid_str.clone());

            // CHA-215: chunk by `self.max_segment_bytes` so no emitted
            // segment exceeds the cap. Each chunk becomes a sibling
            // row under one `table_persist_uuid`. `committed_at` is an
            // inline column on the cold batch (CHA-218). CHA-410: the
            // chunks come back commit_seq_num-ordered (within and across
            // chunks) so the persist read stream is honestly ordered.
            let chunks = chunk_persist_batch(batch, self.max_segment_bytes)?;
            for (chunk_idx_usize, (chunk_batch, in_memory_bytes)) in chunks.into_iter().enumerate()
            {
                let chunk_idx = chunk_idx_usize as u32;
                let (min_ts, max_ts) = batch_committed_at_bounds(&chunk_batch)?;
                // CHA-430: stamp the chunk's commit-order (seq) range so
                // audit segment-selection can prune on the seq axis. The
                // commit_seq_num column rides on every cold row from IMPL-1.
                let (min_seq, max_seq) = batch_commit_seq_num_bounds(&chunk_batch)?;
                let seg_uuid = table_persist_segment_uuid(&table_persist_uuid, chunk_idx);
                let uri = persist_segment_uri(
                    &self.base_uri,
                    catalog_uuid,
                    branch_uuid,
                    &table_persist_uuid,
                    &seg_uuid,
                    storage_format_text,
                );
                let step = PersistSegmentStep {
                    seg_uuid_str: seg_uuid.to_string(),
                    table_persist_uuid_str: table_persist_uuid_str.clone(),
                    chunk_idx,
                    min_committed_at: min_ts,
                    max_committed_at: max_ts,
                    min_commit_seq_num: min_seq,
                    max_commit_seq_num: max_seq,
                    uri,
                    num_rows: chunk_batch.num_rows() as i64,
                    size_bytes: in_memory_bytes,
                    batch: chunk_batch,
                };
                seg_writer.write_segment(pool, writer, &step).await?;
            }
        }

        Ok(())
    }

    /// Phase 1 parent-row cleanup on error. Segment files + segment
    /// rows are cleaned up by `DurableSegmentWriter::cleanup_on_err`
    /// before this is called; this drops the per-`log_kind`
    /// `table_persist_metadata` parents whose children are now gone.
    /// Errors are swallowed.
    async fn cleanup_persist_parents(
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_persist_uuid_strs: &[String],
    ) {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        for tf_uuid in table_persist_uuid_strs {
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_table_persist(
                pool,
                &catalog_str,
                &branch_str,
                tf_uuid,
            )
            .await;
        }
    }

    /// Phase 2 of `persist_locked`: flip the per-`log_kind` parent rows to
    /// committed, in one PG tx. CHA-444 (ADR 0027): no hot deletes here —
    /// committed hot rows are cleared by Purge, and aborted hot rows are
    /// now Purge's concern too (ADR 0021 reversed).
    async fn phase2_parent_flip(
        pool: &PgDriver,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_persist_uuid_strs: &[String],
    ) -> Result<(), ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        let tx = pool
            .begin()
            .await
            .map_err(|e| ApiError::Metadata(e.into()))?;
        for tf_uuid in table_persist_uuid_strs {
            penca_storage_meta::LifecycleManager::commit_table_persist(
                &tx,
                &catalog_str,
                &branch_str,
                tf_uuid,
            )
            .await?;
        }
        tx.commit()
            .await
            .map_err(|e| ApiError::Metadata(e.into()))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_locked<L, W>(
        &self,
        pool: &PgDriver,
        hot: &HotStorageClient,
        dl_driver: &L,
        writer: &W,
        request: &PersistRequest,
        catalog_uuid: Uuid,
        branch_uuid: Uuid,
        table_uuid: Uuid,
        step1_now: i64,
        snapshot: &penca_merge::ReadSnapshot,
    ) -> Result<PersistResponse, ApiError>
    where
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        // -- Step 1: the target micros -------------------------------------
        // `step1_now` was sourced from PG before the lock so the watermark,
        // every `commit_micros` / `written_at_micros` default in this
        // op, and the name-resolution snapshot all share one monotone clock.
        let effective_target = Self::effective_target(request.target_micros, step1_now);

        if effective_target <= 0 {
            return Ok(PersistResponse {
                persisted_at_micros: None,
            });
        }

        // -- Step 2: read T's committed hot upsert + delete rows -----------
        let (upsert_rows, delete_rows) = self
            .read_hot_rows(
                pool,
                hot,
                dl_driver,
                &catalog_uuid,
                &branch_uuid,
                &table_uuid,
                effective_target,
                snapshot,
            )
            .await?;

        if upsert_rows.num_rows() + delete_rows.num_rows() == 0 {
            // Nothing committed to persist. No persist row written; a
            // subsequent Purge(T) will no-op (no persist newer than the
            // last purge).
            return Ok(PersistResponse {
                persisted_at_micros: None,
            });
        }

        // CHA-218: project the hot-shaped JOIN result to the cold
        // on-disk layout — drop the columns named `version_uuid` and
        // `tx_uuid`; keep `row_uuid + <user_cols> + (committed_at,
        // began_at, comment, author)`.
        let upsert_batch = project_to_cold_layout(&upsert_rows)?;
        let delete_batch = project_to_cold_layout(&delete_rows)?;

        // CHA-227: stamp `persisted_at_micros` to `max(committed_at)` over
        // the committed rows being persisted. Then the CHA-221 v2.1
        // strict-advance gate: if the candidate wouldn't strictly advance
        // past the last committed Persist, no-op.
        let (max_persisted_committed_at, persist_commit_seq_num) =
            match Self::compute_candidate_persisted_at_gated(
                pool,
                &catalog_uuid,
                &branch_uuid,
                &table_uuid,
                &upsert_batch,
                &delete_batch,
            )
            .await?
            {
                Some(v) => v,
                None => {
                    return Ok(PersistResponse {
                        persisted_at_micros: None,
                    });
                }
            };

        // CHA-507: the cold segments this Persist is about to make visible drop
        // author/comment; `audit_data` reattaches them by joining the cold
        // tx_log on `commit_seq_num`. Flush the tx_log covering these commits
        // FIRST — before Phase 2 flips the segments visible — the same
        // visibility invariant `persist_and_snapshot_branch` applies at branch
        // scope. Idempotent: no-ops (without even taking the lock) when the
        // tx_log already covers `persist_commit_seq_num`, so the scheduler's
        // `persist_and_snapshot_branch`, which flushes once upfront, makes every
        // nested per-table call here a cheap short-circuit. `None` means no
        // committed rows to persist (no seq to cover), so nothing to flush.
        if let Some(persist_seq) = persist_commit_seq_num {
            self.persist_tx_log(pool, writer, &catalog_uuid, &branch_uuid, persist_seq)
                .await?;
        }

        // -- Step 3: Phase 1 — durable writes ------------------------------
        // Segment-level cleanup-on-err lives on the writer; parent-row
        // cleanup uses the separately-tracked `table_persist_uuid_strs`.
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();
        let mut seg_writer = DurableSegmentWriter::new(PersistSegmentScope {
            catalog_str: &catalog_str,
            branch_str: &branch_str,
            table_str: &table_str,
            storage_format: self.storage_format,
        });
        let mut table_persist_uuid_strs: Vec<String> = Vec::new();
        let phase1_result = self
            .phase1_durable_writes(
                pool,
                writer,
                &catalog_uuid,
                &branch_uuid,
                &table_uuid,
                effective_target,
                &upsert_batch,
                &delete_batch,
                max_persisted_committed_at,
                persist_commit_seq_num,
                &mut seg_writer,
                &mut table_persist_uuid_strs,
            )
            .await;
        if let Err(err) = phase1_result {
            seg_writer.cleanup_on_err(pool, writer).await;
            Self::cleanup_persist_parents(
                pool,
                &catalog_uuid,
                &branch_uuid,
                &table_persist_uuid_strs,
            )
            .await;
            return Err(err);
        }

        // -- Step 4: Phase 2 — parent flips (one tx) -----------------------
        // CHA-220 / CHA-444: all hot deletes (committed and aborted) live in
        // Purge; tx-log family stays put until CHA-221's `PurgeTxLog`.
        Self::phase2_parent_flip(pool, &catalog_uuid, &branch_uuid, &table_persist_uuid_strs)
            .await?;

        Ok(PersistResponse {
            persisted_at_micros: Some(max_persisted_committed_at),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// CHA-410 / CHA-431: `chunk_persist_batch` must yield rows in the composite
    /// `(commit_seq_num, write_seq_num)` order — within each chunk and across chunks —
    /// so the persist read stream is honestly ordered on the total version
    /// order the provider advertises. Input is deliberately shuffled and the
    /// byte cap tiny to force multiple chunks (cross-chunk order matters too).
    /// The `commit_seq_num = 1` pair (rows "b","c") exercises the `write_seq_num`
    /// secondary key: same commit, distinct mutation ordinals.
    #[test]
    fn persist_chunks_are_commit_seq_num_write_seq_num_ordered() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("commit_seq_num", DataType::Int64, false),
            Field::new("write_seq_num", DataType::Int64, false),
        ]));
        let row_uuids = StringArray::from(vec!["d", "b", "c", "a", "e"]);
        let tx_seqs = Int64Array::from(vec![3i64, 1, 1, 0, 4]);
        let mut_seqs = Int64Array::from(vec![0i64, 5, 2, 0, 0]);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(row_uuids), Arc::new(tx_seqs), Arc::new(mut_seqs)],
        )
        .unwrap();

        // Tiny cap -> ~1 row per chunk, exercising cross-chunk ordering.
        let chunks = chunk_persist_batch(&batch, 1).unwrap();
        assert!(
            chunks.len() >= 2,
            "expected multiple chunks to exercise cross-chunk order; got {}",
            chunks.len()
        );

        let mut seen: Vec<(i64, i64)> = Vec::new();
        for (chunk, _bytes) in &chunks {
            let tx = chunk
                .column_by_name("commit_seq_num")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mt = chunk
                .column_by_name("write_seq_num")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..tx.len() {
                seen.push((tx.value(i), mt.value(i)));
            }
        }

        let mut expected = seen.clone();
        expected.sort_unstable();
        assert_eq!(
            seen, expected,
            "persist chunks must be (commit_seq_num, write_seq_num)-ordered across the \
             stream; got {seen:?}"
        );
        // The tie on commit_seq_num=1 must resolve by write_seq_num: c(1,2) before b(1,5).
        assert_eq!(
            seen,
            vec![(0, 0), (1, 2), (1, 5), (3, 0), (4, 0)],
            "composite order must place the lower write_seq_num first within a \
             tied commit_seq_num; got {seen:?}"
        );
    }
}
