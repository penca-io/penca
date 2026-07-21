//! Snapshot: cold-tier-only point-in-time materialization (CHA-228).
//!
//! [`LifecycleManager::snapshot`] is the public entry; `snapshot_locked`
//! runs the orchestration inside the per-table advisory lock. Two step
//! helpers split the planning: `compute_snapshot_window` and
//! `assemble_cold_only_plan`. The write itself is the CHA-404 packed
//! streaming pipeline — `penca_merge::stream_all_cold_parts` (delta once +
//! plan-ordered prior-snapshot stream) into
//! `packer::pack_merged_partition_stream`, each flushed file persisted
//! through [`DurableSegmentWriter::write_segment_group`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::record_batch::RecordBatch;
use penca_core::naming::{
    SystemNameIndexSpec, row_uuid_for_pk, segment_index_uri, snapshot_segment_uri,
    system_name_index_spec, table_snapshot_index_uuid, table_snapshot_segment_uuid,
    table_snapshot_uuid,
};
use penca_core::{
    ColdStoragePlan, CommittedAtBounds, Format, PersistPlan, PersistSegment, Plan, SnapshotPlan,
    SnapshotSegment,
};
use penca_db::driver::pg::PgDriver;
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_proto::external::v1::{Index, SnapshotRequest, SnapshotResponse};
use penca_storage_cold::ColdStorageClient;
use penca_storage_meta::watermarks::compute_snapshot_seq_watermark;
use penca_storage_meta::{CarriedSegmentSpec, SnapshotResult};
use uuid::Uuid;

use futures_util::{StreamExt, TryStreamExt};

use crate::error::ApiError;
use crate::lifecycle::LifecycleManager;
use crate::lifecycle::batch_util::{
    PartitionOrderKey, PartitionOrdering, partition_label, partition_order_key_from_statistics,
    partition_record_batch,
};
use crate::lifecycle::chunker::batch_in_memory_bytes;
use crate::lifecycle::durable_writer::{
    DurableSegmentWriter, SnapshotFileStep, SnapshotSegmentRowSpec, SnapshotSegmentScope,
};
use crate::lifecycle::packer::{PackStep, SegmentPacker, pack_merged_partition_stream};

/// The keys that govern a snapshot's intra-partition sort:
/// `clustering_keys` when the table declares them, else the primary
/// keys (CHA-404). One home for the rule — the recorded parent-row
/// keys and the actual segment sort must agree (CHA-406's key-change
/// detection reads the recorded value), so callers must never apply a
/// different default. Note this means tables without clustering keys
/// get PK-sorted snapshot segments (prunable min/max stats) where they
/// previously inherited merge-read order.
fn effective_clustering_keys(
    clustering_keys: Vec<String>,
    primary_keys: Vec<String>,
) -> Vec<String> {
    if clustering_keys.is_empty() {
        primary_keys
    } else {
        clustering_keys
    }
}

/// CHA-228: the empty-merge placeholder (all rows tombstoned by new
/// persist) — one zero-row segment at `chunk_idx = 0` so the watermark
/// gets committed to `table_snapshot_metadata`. Without it the next
/// Snapshot(T) would redo the same merge-read forever.
/// `read_snapshot_segments_for_table` filters zero-row rows out after
/// capturing the watermark.
fn empty_merge_placeholder_step(
    snap_uuid: &Uuid,
    snap_str: &str,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    base_uri: &str,
    storage_format_text: &str,
    user_schema: &SchemaRef,
) -> SnapshotFileStep {
    let placeholder_batch = RecordBatch::new_empty(penca_merge::snapshot_read_schema(user_schema));
    let seg_uuid = table_snapshot_segment_uuid(snap_uuid, 0);
    let uri = snapshot_segment_uri(
        base_uri,
        catalog_uuid,
        branch_uuid,
        snap_uuid,
        &seg_uuid,
        storage_format_text,
    );
    let statistics = penca_dl::stats::compute_segment_statistics(&placeholder_batch);
    SnapshotFileStep {
        snap_uuid_str: snap_str.to_string(),
        uri,
        file_batch: placeholder_batch,
        segment_rows: vec![SnapshotSegmentRowSpec {
            seg_uuid_str: seg_uuid.to_string(),
            chunk_idx: 0,
            partition_value: None,
            offset: 0,
            length: 0,
            size_bytes: 0,
            statistics,
        }],
    }
}

impl LifecycleManager {
    /// Create a read-optimized, point-in-time snapshot from cold storage.
    ///
    /// Cold-tier-only merge-on-read + partition split + segment write.
    /// Hot storage is excluded so live OLTP rows don't leak into the
    /// snapshot.
    ///
    /// Serialized against concurrent `Snapshot(T)` calls only, via a
    /// session-scoped advisory lock keyed by
    /// `snapshot:{table_uuid}:{branch_uuid}` — hygiene only:
    /// race-losers already no-op via the deterministic `snap_uuid`
    /// ON-CONFLICT exit, but the lock avoids the wasted merge-read
    /// and segment write the loser would otherwise do.
    /// Cross-operation pairs (`Persist↔Snapshot`, `Snapshot↔Purge`)
    /// are lock-free under ADR 0019 — pillar 1 (plan-time threading)
    /// makes them safe.
    ///
    /// With that mutex in place, segment writes happen outside any
    /// database transaction so a crash mid-write cannot leave an
    /// orphan file on disk: each step (insert row → write file →
    /// update size → commit row) auto-commits, and the cleanup path
    /// mirrors persist — delete the file first, then the row only if
    /// the file deletion succeeded.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn snapshot<R, L, W>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        dl_driver: &L,
        writer: &W,
        request: &SnapshotRequest,
    ) -> Result<SnapshotResponse, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        // See `persist`'s `step1_now` doc-comment.
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

        let lock_key = super::snapshot_lock_key(&table_uuid, &branch_uuid);
        pool.advisory_lock(&lock_key, async || {
            self.snapshot_locked(
                pool,
                readers,
                dl_driver,
                writer,
                request,
                catalog_uuid,
                branch_uuid,
                table_uuid,
                &snapshot,
            )
            .await
        })
        .await
    }

    /// Resolve the Snapshot(T) window. Returns `None` for the
    /// empty-resp fast path (never persisted, prior snapshot already
    /// covers everything, or `as_of` is already covered). On the
    /// non-empty path returns `(snapshotted_at_micros, SnapshotResult)`
    /// where `snapshotted_at_micros` is the inclusive upper bound for
    /// this snapshot and `SnapshotResult.snapshotted_at_micros` is the
    /// previous snapshot watermark (`None` if no prior snapshot).
    async fn compute_snapshot_window(
        &self,
        pool: &PgDriver,
        catalog_str: &str,
        branch_str: &str,
        table_str: &str,
        as_of_micros: Option<i64>,
    ) -> Result<Option<(i64, SnapshotResult)>, ApiError> {
        // CHA-227: plan-time atomicity is via explicit threading.
        // `persisted_at` (read first) bounds `upper`, and `upper + 1`
        // bounds the persist segment fetch's upper. A concurrent
        // Persist committing between the watermark read and the
        // segment-list read advances PG state but doesn't shift this
        // plan's bounds — segments with `min_tx > persisted_at` are
        // structurally excluded by `to_micros = upper + 1`. No
        // surrounding REPEATABLE READ tx required. Snapshot's
        // watermark is derived from raw persist-log inputs (mirrors
        // Persist's stamping rule) so it is well-defined even in
        // CHA-228's empty-merge case.
        let snapshot_result = self
            .query_manager
            .read_snapshot_segments_for_table(
                pool,
                catalog_str,
                branch_str,
                table_str,
                None,
                // CHA-443: snapshot construction picks the latest baseline to carry
                // forward; no seq-axis read cutoff applies here.
                None,
                // CHA-492: the snapshot writer picks (no pinned identity).
                None,
            )
            .await?;
        let persisted_at =
            penca_storage_meta::LifecycleManager::latest_committed_table_persist_watermark(
                pool,
                catalog_str,
                branch_str,
                table_str,
            )
            .await?
            .filter(|v| *v > 0);
        let upper: Option<i64> = match (as_of_micros, persisted_at) {
            (Some(t), Some(p)) => Some(t.min(p)),
            (None, Some(p)) => Some(p),
            _ => None,
        };
        // Early-exit conditions match Purge's `!real_purge` fast-path
        // symmetrically: never persisted, prior snapshot already
        // covers everything, or `as_of` already covered. Skip the
        // cold segment fetch in those cases — no merge work to do.
        let snapshotted_at_micros = match (upper, snapshot_result.snapshotted_at_micros) {
            (None, _) => return Ok(None),
            (Some(u), Some(s)) if u <= s => return Ok(None),
            (Some(u), _) => u,
        };
        Ok(Some((snapshotted_at_micros, snapshot_result)))
    }

    /// Assemble a cold-only `Plan` for the stream_all_cold_parts that
    /// produces the snapshot baseline: persist segments in
    /// `(prev_snap_watermark + 1, snapshotted_at_micros + 1)` plus the
    /// prior snapshot (if any) as the cold baseline.
    #[allow(clippy::too_many_arguments)]
    async fn assemble_cold_only_plan(
        &self,
        pool: &PgDriver,
        catalog_str: &str,
        branch_str: &str,
        table_str: &str,
        snapshotted_at_micros: i64,
        prev_snap_watermark: Option<i64>,
        snapshot_segments: Vec<SnapshotSegment>,
    ) -> Result<Plan, ApiError> {
        let from_micros = prev_snap_watermark.map(|s| s.saturating_add(1));
        let to_micros = Some(snapshotted_at_micros.saturating_add(1));
        let (cold_upsert_segments, cold_delete_segments) = self
            .query_manager
            .read_persist_segments_for_window(
                pool,
                catalog_str,
                branch_str,
                table_str,
                from_micros,
                to_micros,
                // Snapshot materializes the full committed_at window; no
                // seq-axis read cutoff applies at construction (CHA-429 #4).
                // The seq-aware baseline picker for AsOfSeq *reads* is
                // tracked separately in CHA-457.
                None,
            )
            .await?;

        // The PersistPlan carries the new `committed_at` filter from
        // CHA-227 commit 1 (consumed per-row by the merge SQL builders
        // in commit 7); the snapshot baseline (if any) sits underneath.
        let cold_persist_plan =
            if cold_upsert_segments.is_empty() && cold_delete_segments.is_empty() {
                None
            } else {
                Some(PersistPlan {
                    upsert_segments: cold_upsert_segments,
                    delete_segments: cold_delete_segments,
                    committed_at: Some(CommittedAtBounds {
                        min_micros: from_micros,
                        max_micros: to_micros,
                    }),
                    // CHA-443: snapshot-write read uses the committed-at window
                    // `[from, to)` (no hot tier to fence against); the seq fence
                    // is a read-plan concern, so `commit_seq` stays unset and the
                    // merge builder keeps the committed_at bounds.
                    commit_seq: None,
                })
            };
        let cold_snapshot_plan = prev_snap_watermark.map(|ts| SnapshotPlan {
            segments: snapshot_segments,
            snapshotted_at_micros: ts,
            // CHA-443: write-path baseline plan; W_snap is a read-plan concern,
            // not consulted on the snapshot-write merge.
            ..Default::default()
        });
        // CHA-178: on a forked branch's FIRST snapshot, fold the parent's cold
        // tier into the delta so the new baseline materializes
        // parent-as-of-fork ∪ the branch's own writes — the mechanism that lets
        // steady-state forked reads skip the base source (the read gate).
        // Gated on `prev_snap_watermark.is_none()`: every later snapshot carries
        // the prior snapshot as its cold baseline, which already folded the
        // parent (any child snapshot seq > fork_seed, so it stays
        // fork-covering), so re-folding would be pure redundant re-materialization
        // of the whole parent cold tier. Ceiling = fork_seed (the snapshot
        // captures the parent as-of the fork; there is no seq as_of to push it
        // lower).
        let base_cold_storage = if prev_snap_watermark.is_some() {
            None
        } else {
            match self
                .query_manager
                .read_branch_lineage(pool, catalog_str, branch_str)
                .await?
            {
                Some((parent_branch_uuid, fork_commit_seq_num)) => {
                    self.query_manager
                        .enumerate_base_cold_source(
                            pool,
                            catalog_str,
                            &parent_branch_uuid,
                            table_str,
                            snapshotted_at_micros,
                            fork_commit_seq_num,
                        )
                        .await?
                }
                None => None,
            }
        };
        Ok(Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: cold_snapshot_plan,
                persist: cold_persist_plan,
            }),
            base_cold_storage,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn snapshot_locked<R, L, W>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        dl_driver: &L,
        writer: &W,
        request: &SnapshotRequest,
        catalog_uuid: Uuid,
        branch_uuid: Uuid,
        table_uuid: Uuid,
        snapshot: &penca_merge::ReadSnapshot,
    ) -> Result<SnapshotResponse, ApiError>
    where
        R: FormatReader,
        L: DlDriver + ?Sized,
        W: FormatWriter,
    {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();

        // FOLLOWUP-A dropped `primary_keys` from `stream_merged`'s shape;
        // snapshot needs `partition_keys` (partition split) and the
        // effective clustering keys (in-segment sort) — `clustering_keys`
        // defaulting to `primary_keys` when unset (CHA-404).
        let (arrow_schema_bytes, partition_keys, clustering_keys, primary_keys) = self
            .query_manager
            .get_table_schema_and_layout_keys(
                pool,
                dl_driver,
                &catalog_str,
                &branch_str,
                &table_str,
                snapshot,
            )
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Table metadata not found: {table_str}")))?;
        // Keep `primary_keys` for the carry-forward attribution choice below
        // (`partition_subset_of_pk`: subset → delete-log columns, non-subset →
        // row_uuid reverse lookup); `effective_clustering_keys` would otherwise
        // consume it.
        let clustering_keys = effective_clustering_keys(clustering_keys, primary_keys.clone());
        let user_schema: SchemaRef =
            Arc::new(try_schema_from_ipc_buffer(&arrow_schema_bytes).map_err(ApiError::Arrow)?);

        // CHA-483: the live user secondary-index definitions for this table at
        // the snapshot's pin. Each is re-derived into a parent header + per-
        // segment sidecars below (materialize-on-next-snapshot); a dropped index
        // simply isn't listed here next cycle, so it stops being re-declared.
        let user_indexes = self
            .query_manager
            .meta_list_indexes(
                pool,
                dl_driver,
                &catalog_str,
                &table_str,
                Some(&branch_str),
                snapshot,
            )
            .await?;

        let (snapshotted_at_micros, snapshot_result) = match self
            .compute_snapshot_window(
                pool,
                &catalog_str,
                &branch_str,
                &table_str,
                request.snapshotted_at_micros,
            )
            .await?
        {
            Some(v) => v,
            None => {
                return Ok(SnapshotResponse {
                    snapshotted_at_micros: None,
                });
            }
        };
        let SnapshotResult {
            // CHA-485: planner-only field; the snapshot writer re-derives defs
            // from live index_metadata itself.
            indexes: _,
            snapshotted_at_micros: prev_snap_watermark,
            commit_seq_num: prev_snapshot_commit_seq_num,
            snapshot_segments,
            partition_keys: recorded_partition_keys,
            clustering_keys: recorded_clustering_keys,
        } = snapshot_result;

        // CHA-483: prior committed segments keyed by uuid, so the write tail can
        // read a CARRIED segment's base file to materialize a newly-active user
        // index's missing sidecar (materialize-on-next-snapshot). Built by
        // borrow before `snapshot_segments` is consumed by the paths below.
        // Only the carried-base read needs it, so skip the whole-map clone on the
        // common no-user-index table (the materialize pass early-returns anyway).
        let prior_segment_by_uuid: HashMap<String, SnapshotSegment> = if user_indexes.is_empty() {
            HashMap::new()
        } else {
            snapshot_segments
                .iter()
                .map(|seg| (seg.table_snapshot_segment_uuid.clone(), seg.clone()))
                .collect()
        };

        // CHA-443 (IMPL-2): the new snapshot's seq watermark W_snap =
        // max(prev snapshot's W_snap, MAX(persist seg max_commit_seq_num) over the
        // segments this snapshot folds in — the (prev_snap_watermark,
        // snapshotted_at] committed-at window). Carry-forward-only (no new
        // segments) keeps the prior watermark; the genesis case bases at
        // SNAPSHOT_SEQ_GENESIS inside the helper.
        let segment_seq_max = self
            .query_manager
            .max_persisted_segment_seq_for_window(
                pool,
                &catalog_str,
                &branch_str,
                &table_str,
                prev_snap_watermark.map(|w| w.saturating_add(1)),
                Some(snapshotted_at_micros.saturating_add(1)),
            )
            .await?;
        let snapshot_commit_seq_num = compute_snapshot_seq_watermark(
            prev_snapshot_commit_seq_num,
            &segment_seq_max.into_iter().collect::<Vec<i64>>(),
        );

        let snap_uuid = table_snapshot_uuid(
            &catalog_uuid,
            &branch_uuid,
            &table_uuid,
            snapshotted_at_micros,
        );
        let snap_str = snap_uuid.to_string();
        let storage_format_text = self.storage_format.extension();

        // CHA-432: decide the durable retention rung once, here at creation.
        // Resolve the effective snapshot density (table → schema → catalog),
        // read the last durable rung's watermark, and apply the pure kernel.
        // Snapshot is a cold background op under the snapshot advisory lock, so
        // these reads are not on any hot path. `durable` is sticky — the flag is
        // never recomputed on retry (`insert_snapshot_metadata` leaves it out of
        // its `DO UPDATE`), keeping the retention floor monotonic.
        let table_obj = self
            .query_manager
            .resolve_table_by_uuid(
                pool,
                dl_driver,
                &catalog_uuid,
                &table_str,
                Some(&branch_str),
                snapshot,
            )
            .await?;
        let schema_rc = crate::retention::fetch_parent_retention(
            &self.query_manager,
            pool,
            dl_driver,
            &catalog_str,
            &table_obj.schema_uuid,
        )
        .await?;
        let effective_retention =
            crate::retention::coalesce_retention(&table_obj.retention_config, &schema_rc);
        let last_durable_at = penca_storage_meta::LifecycleManager::last_durable_snapshot_at(
            pool,
            &catalog_str,
            &branch_str,
            &table_str,
        )
        .await?;
        let durable = decide_durable(
            last_durable_at,
            snapshotted_at_micros,
            effective_retention.snapshot_density_seconds,
        );

        let ctx = SnapshotWriteCtx {
            catalog_uuid: &catalog_uuid,
            branch_uuid: &branch_uuid,
            table_uuid: &table_uuid,
            catalog_str: &catalog_str,
            branch_str: &branch_str,
            table_str: &table_str,
            snap_uuid: &snap_uuid,
            snap_str: &snap_str,
            snapshotted_at_micros,
            commit_seq_num: snapshot_commit_seq_num,
            durable,
            user_schema: &user_schema,
            storage_format_text,
            partition_keys: &partition_keys,
            clustering_keys: &clustering_keys,
            user_indexes: &user_indexes,
        };

        // CHA-459: one typed PartitionOrdering for the whole snapshot
        // cycle — the ordering authority threaded into carry-forward key
        // derivation and both pack paths, so every leg merges partitions in
        // typed partition order rather than stringified-label order.
        let ordering = PartitionOrdering::new(&user_schema, &partition_keys)?;

        // CHA-406 carry-forward eligibility (ADR 0024 §3): engage iff a
        // prior committed snapshot exists with non-placeholder segments,
        // its recorded layout keys equal the current ones, the table is
        // partitioned with partition ⊆ PK, and every prior segment's
        // typed key is derivable from its statistics. Any failure → the
        // CHA-404 full-rewrite path, used verbatim.
        let prior_keys = carry_forward_keys(
            &ordering,
            &snapshot_segments,
            recorded_partition_keys.as_deref(),
            recorded_clustering_keys.as_deref(),
            &partition_keys,
            &clustering_keys,
            &user_schema,
        )?;

        let merge_snapshot = penca_merge::ReadSnapshot::AsOfMicros(snapshotted_at_micros);
        if let Some(prior_keys) = prior_keys {
            // ---- Carry-forward path ----
            // Resolve the windowed delta once (the prior snapshot is NOT
            // a merge baseline here — its untouched partitions carry by
            // reference, its touched ones stream and rewrite), derive the
            // touched set, split prior segments into the rewrite subset +
            // carried map, then stream-merge-pack the touched subset.
            let (delta_groups, delete_segments, exclusion_set) = self
                .resolve_windowed_delta(
                    pool,
                    dl_driver,
                    &catalog_str,
                    &branch_str,
                    &table_str,
                    &user_schema,
                    &partition_keys,
                    &merge_snapshot,
                    prev_snap_watermark,
                    snapshotted_at_micros,
                )
                .await?;

            // Touched set = partitions with delta upserts ∪ the prior
            // partitions a window's deletes/moves attribute to. Over-inclusion
            // is byte-correct — the delete-segment list is already
            // window-overlap-bounded, so no per-row committed_at filter is
            // needed.
            let mut touched: HashSet<Option<String>> = delta_groups
                .iter()
                .map(|(label, _)| label.clone())
                .collect();
            if partition_subset_of_pk(&partition_keys, &primary_keys) {
                // v1: the cold delete-log carries the partition columns
                // (partition ⊆ PK), and a partition-key change is a PK change
                // → delete+insert, so a delete's partition label is directly
                // derivable and moves surface as deletes.
                touched.extend(
                    Self::delete_attributed_labels(
                        readers,
                        &delete_segments,
                        &user_schema,
                        &partition_keys,
                        &primary_keys,
                    )
                    .await?,
                );
            } else {
                // CHA-448 v2: the partition column is outside the PK, so it is
                // absent from the cold delete-log AND a partition-key move
                // keeps the same row_uuid (upsert only, no delete). Reverse-
                // look up the PRIOR partition of every window-touched row_uuid
                // — upserts ∪ deletes — via the CHA-412 row_uuid sidecars; the
                // eligibility gate guaranteed every prior segment has one.
                let mut touched_row_uuids = touched_row_uuids_from_delta(&delta_groups)?;
                touched_row_uuids.extend(
                    Self::delete_row_uuids(readers, &delete_segments, &user_schema, &primary_keys)
                        .await?,
                );
                let sidecar_schema =
                    penca_format::index::segment_index_schema(&[arrow::datatypes::DataType::Utf8]);
                let prior_partition_labels = Self::reverse_lookup_attributed_labels(
                    readers,
                    &snapshot_segments,
                    &prior_keys,
                    &touched,
                    &touched_row_uuids,
                    &sidecar_schema,
                    self.segment_read_concurrency,
                )
                .await?;
                touched.extend(prior_partition_labels);
            }

            let (touched_segments, carried) =
                split_prior_segments_by_touch(prior_keys, snapshot_segments, &touched);

            // Carry-forward engaged: the headline observability for the
            // optimization — how many prior partitions were rewritten vs
            // carried by reference this cycle (counts only, no labels).
            // The decline path (full rewrite) logs a `warn!` in
            // `carry_forward_keys`; this is its success-path sibling.
            tracing::debug!(
                target: "penca_api::snapshot_carry_forward",
                touched_partitions = touched_segments.len(),
                carried_partitions = carried.len(),
                delta_rows = delta_groups.iter().map(|(_, b)| b.num_rows()).sum::<usize>(),
                "carry-forward engaged"
            );

            let touched_plan = SnapshotPlan {
                segments: touched_segments,
                snapshotted_at_micros: prev_snap_watermark
                    .expect("eligibility guarantees a prior snapshot watermark"),
                ..Default::default()
            };
            // Prior stream over the touched subset only, with the same
            // global exclusion set the delta resolve produced (CHA-142).
            let prior_stream = penca_merge::snapshot_segment_stream(
                &touched_plan,
                dl_driver,
                &user_schema,
                &user_schema,
                None,
                // CHA-485: the writer path seeks nothing.
                penca_merge::SnapshotSeeks::default(),
                exclusion_set,
                penca_merge::SnapshotStreamTuning {
                    segment_read_concurrency: self.segment_read_concurrency,
                    snapshot_prune_min_segments: 0,
                    segment_order: penca_merge::SegmentOrder::ByPlan,
                },
            );
            let packer =
                self.new_packer(&snap_uuid, &catalog_uuid, &branch_uuid, storage_format_text);
            let pack_steps = pack_merged_partition_stream(
                delta_groups,
                prior_stream,
                ordering,
                clustering_keys.clone(),
                carried,
                packer,
            );
            self.finish_snapshot_write(
                pool,
                readers,
                writer,
                pack_steps,
                &ctx,
                &prior_segment_by_uuid,
            )
            .await
        } else {
            // ---- CHA-404 full-rewrite path ----
            // The whole prior snapshot is the merge baseline; its
            // survivors stream in plan order (= label-sorted runs) with
            // the exclusion applied per batch inside the all-cold entry
            // (the plan is cold-only by construction). The packer
            // interleaves the delta and prior legs and flushes
            // whole-partition packed files; peak memory is the packing
            // buffer + one in-flight partition + the delta.
            let cold_only_plan = self
                .assemble_cold_only_plan(
                    pool,
                    &catalog_str,
                    &branch_str,
                    &table_str,
                    snapshotted_at_micros,
                    prev_snap_watermark,
                    snapshot_segments,
                )
                .await?;
            let parts = penca_merge::stream_all_cold_parts(penca_merge::MergeReadRequest {
                plan: &cold_only_plan,
                driver: pool,
                dl: dl_driver,
                user_schema: &user_schema,
                full_schema: &user_schema,
                snapshot: &merge_snapshot,
                filter: None,
                seeks: None,
                segment_order: penca_merge::SegmentOrder::ByPlan,
                segment_read_concurrency: self.segment_read_concurrency,
                snapshot_prune_min_segments: 0,
            })
            .await
            .map_err(ApiError::Merge)?;
            let delta_groups = partition_record_batch(&parts.resolved, &partition_keys)?;
            let packer =
                self.new_packer(&snap_uuid, &catalog_uuid, &branch_uuid, storage_format_text);
            let pack_steps = pack_merged_partition_stream(
                delta_groups,
                parts.snapshot_stream,
                ordering,
                clustering_keys.clone(),
                BTreeMap::new(),
                packer,
            );
            self.finish_snapshot_write(
                pool,
                readers,
                writer,
                pack_steps,
                &ctx,
                &prior_segment_by_uuid,
            )
            .await
        }
    }

    /// Build the packer for a snapshot cycle. The chunk_idx counter
    /// starts at 0 and stays dense across every flushed/carried row.
    fn new_packer(
        &self,
        snap_uuid: &Uuid,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        storage_format_text: &str,
    ) -> SegmentPacker {
        SegmentPacker::new(
            snap_uuid,
            catalog_uuid,
            branch_uuid,
            &self.base_uri,
            storage_format_text,
            self.max_segment_bytes,
        )
    }

    /// Resolve the carry-forward window's persist rows into the
    /// label-grouped delta plus the global exclusion set (CHA-406 phase
    /// A). Builds a persist-only `Plan` (no snapshot baseline —
    /// the prior snapshot is carried/rewritten, not merged) for the
    /// `(prev_watermark + 1, snap + 1]` window and runs
    /// `resolve_log_tiers`. Returns `(delta_groups, delete_segments,
    /// exclusion_set)`: `delete_segments` flow out for touched-set
    /// attribution; `exclusion_set` applies to the touched prior stream.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_windowed_delta<L>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        catalog_str: &str,
        branch_str: &str,
        table_str: &str,
        user_schema: &SchemaRef,
        partition_keys: &[String],
        merge_snapshot: &penca_merge::ReadSnapshot,
        prev_snap_watermark: Option<i64>,
        snapshotted_at_micros: i64,
    ) -> Result<
        (
            Vec<(Option<String>, RecordBatch)>,
            Vec<PersistSegment>,
            HashSet<String>,
        ),
        ApiError,
    >
    where
        L: DlDriver + ?Sized,
    {
        let from_micros = prev_snap_watermark.map(|s| s.saturating_add(1));
        let to_micros = Some(snapshotted_at_micros.saturating_add(1));
        let (upsert_segments, delete_segments) = self
            .query_manager
            .read_persist_segments_for_window(
                pool,
                catalog_str,
                branch_str,
                table_str,
                from_micros,
                to_micros,
                // Carry-forward materializes the full committed_at window; no
                // seq-axis read cutoff applies (CHA-429 #4; CHA-457 tracks the
                // seq-aware baseline picker for AsOfSeq reads).
                None,
            )
            .await?;
        let persist_plan = if upsert_segments.is_empty() && delete_segments.is_empty() {
            None
        } else {
            Some(PersistPlan {
                upsert_segments,
                delete_segments: delete_segments.clone(),
                committed_at: Some(CommittedAtBounds {
                    min_micros: from_micros,
                    max_micros: to_micros,
                }),
                // CHA-443: carry-forward delta read over the committed-at window;
                // no tier fence, so `commit_seq` stays unset (see the sibling
                // write-path plan above).
                commit_seq: None,
            })
        };
        let delta_plan = Plan {
            hot_storage: None,
            cold_storage: Some(ColdStoragePlan {
                snapshot: None,
                persist: persist_plan,
            }),
            base_cold_storage: None,
        };
        let tiers = penca_merge::resolve_log_tiers(&penca_merge::MergeReadRequest {
            plan: &delta_plan,
            driver: pool,
            dl: dl_driver,
            user_schema,
            full_schema: user_schema,
            snapshot: merge_snapshot,
            filter: None,
            seeks: None,
            segment_order: penca_merge::SegmentOrder::ByPlan,
            segment_read_concurrency: self.segment_read_concurrency,
            snapshot_prune_min_segments: 0,
        })
        .await
        .map_err(ApiError::Merge)?;
        let delta_groups = partition_record_batch(&tiers.resolved, partition_keys)?;
        Ok((delta_groups, delete_segments, tiers.exclusion_set))
    }

    /// Distinct partition labels a window's cold delete-log attributes
    /// to (CHA-406 delete attribution). The delete segments carry the
    /// table's PK columns (`cold_delete_schema`), and partition ⊆ PK by
    /// the eligibility gate, so the partition label is derivable from
    /// each delete row. Reads the segment FILES — `Plan` only lists
    /// them. Over-inclusion is safe (an extra rewrite is byte-correct).
    async fn delete_attributed_labels<R: FormatReader>(
        readers: &HashMap<i32, R>,
        delete_segments: &[PersistSegment],
        user_schema: &SchemaRef,
        partition_keys: &[String],
        primary_keys: &[String],
    ) -> Result<HashSet<Option<String>>, ApiError> {
        let mut labels: HashSet<Option<String>> = HashSet::new();
        if delete_segments.is_empty() {
            return Ok(labels);
        }
        let schema =
            penca_merge::cold_delete_schema(user_schema, primary_keys).map_err(ApiError::Merge)?;
        // Decode only the partition-key columns, and fold one batch at a
        // time rather than collecting the whole windowed delete log
        // resident — labels are all we need, and the delete window can be
        // large (CHA-406 keeps the streaming memory bound throughout).
        let projection: Vec<&str> = partition_keys.iter().map(String::as_str).collect();
        let mut stream = ColdStorageClient::read_persist_segments(
            readers,
            delete_segments,
            &schema,
            Some(&projection),
        );
        while let Some(batch) = stream.try_next().await.map_err(ApiError::ColdStorage)? {
            if batch.num_rows() == 0 {
                continue;
            }
            let cols: Vec<&ArrayRef> = partition_keys
                .iter()
                .map(|key| {
                    batch
                        .schema()
                        .index_of(key)
                        .map(|idx| batch.column(idx))
                        .map_err(|_| {
                            ApiError::Internal(format!(
                                "delete segment missing partition key '{key}'"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            for row in 0..batch.num_rows() {
                labels.insert(Some(partition_label(&cols, row)?));
            }
        }
        Ok(labels)
    }

    /// The `row_uuid` of every row in a window's cold delete-log — the delete
    /// identities the CHA-448 reverse-lookup attributes to their prior
    /// partition. Sibling to [`Self::delete_attributed_labels`], which projects
    /// the partition columns (present only when partition ⊆ PK); this projects
    /// `row_uuid`, which `cold_delete_schema` always carries, so it works when
    /// the partition column is outside the PK. Folds one batch at a time to
    /// keep the streaming memory bound (CHA-406).
    async fn delete_row_uuids<R: FormatReader>(
        readers: &HashMap<i32, R>,
        delete_segments: &[PersistSegment],
        user_schema: &SchemaRef,
        primary_keys: &[String],
    ) -> Result<HashSet<String>, ApiError> {
        let mut row_uuids: HashSet<String> = HashSet::new();
        if delete_segments.is_empty() {
            return Ok(row_uuids);
        }
        let schema =
            penca_merge::cold_delete_schema(user_schema, primary_keys).map_err(ApiError::Merge)?;
        let projection = ["row_uuid"];
        let mut stream = ColdStorageClient::read_persist_segments(
            readers,
            delete_segments,
            &schema,
            Some(&projection),
        );
        while let Some(batch) = stream.try_next().await.map_err(ApiError::ColdStorage)? {
            if batch.num_rows() == 0 {
                continue;
            }
            let column = batch.column_by_name("row_uuid").ok_or_else(|| {
                ApiError::Internal("delete segment missing row_uuid column".to_string())
            })?;
            let values = column
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    ApiError::Internal("delete row_uuid column is not Utf8".to_string())
                })?;
            for row in 0..values.len() {
                if values.is_valid(row) {
                    row_uuids.insert(values.value(row).to_string());
                }
            }
        }
        Ok(row_uuids)
    }

    /// Distinct PRIOR-snapshot partition labels the window's touched row_uuids
    /// (upserts ∪ deletes) lived in — the CHA-448 v2 attribution frontier for
    /// partition ⊄ PK. For each prior segment whose label is not already
    /// touched, read its internal row_uuid sidecar (CHA-412) and binary-search
    /// it for any touched row_uuid (`seek_row_offsets`); a hit adds that
    /// segment's label, so the partition is rewritten (not carried) and its
    /// stale copy is dropped by the global exclusion set during the rewrite —
    /// attribution, not dedup. `prior_keys` is aligned 1:1 with
    /// `snapshot_segments` (the `carry_forward_keys` output). Over-inclusion is
    /// byte-correct. CHA-412 builds a row_uuid sidecar for every snapshot
    /// segment, so a probed segment without one is an invariant violation this
    /// fn fails fast on (not a full-rewrite fallback — the PK-agnostic gate no
    /// longer guards it).
    ///
    /// Cost: O(not-yet-touched prior segments) sidecar reads per cycle — a
    /// random row_uuid is a uniform hash with no partition locality (CHA-412),
    /// so partition-stat pruning can't bound the candidate set and every such
    /// segment's sidecar must be probed. The probes run concurrently, bounded
    /// by `segment_read_concurrency` (the shared cold-read budget) like the
    /// full-rewrite path; each sidecar is read-and-dropped, so peak memory is
    /// one wave of batches regardless of the bound. Concurrency trades the
    /// sequential probe's intra-run dedup (which stopped probing a partition
    /// after its first hit), so a multi-segment partition now probes all its
    /// segments — still strictly cheaper than the full-rewrite fallback it
    /// replaces (which rereads + rewrites every segment's data), though not
    /// necessarily than the prior sequential probe for such partitions.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            touched_row_uuids = touched_row_uuids.len(),
            prior_segments = snapshot_segments.len(),
            sidecars_read = tracing::field::Empty,
            labels_attributed = tracing::field::Empty,
        ),
        err,
    )]
    async fn reverse_lookup_attributed_labels<R: FormatReader>(
        readers: &HashMap<i32, R>,
        snapshot_segments: &[SnapshotSegment],
        prior_keys: &[PartitionOrderKey],
        already_touched: &HashSet<Option<String>>,
        touched_row_uuids: &HashSet<String>,
        sidecar_schema: &SchemaRef,
        segment_read_concurrency: usize,
    ) -> Result<HashSet<Option<String>>, ApiError> {
        if touched_row_uuids.is_empty() {
            tracing::Span::current().record("sidecars_read", 0_u64);
            tracing::Span::current().record("labels_attributed", 0_usize);
            return Ok(HashSet::new());
        }
        let probe: Vec<&str> = touched_row_uuids.iter().map(String::as_str).collect();
        // Candidate prior segments: those whose partition isn't already being
        // rewritten (a delta upsert marked it). A row_uuid can sit in any
        // segment of a multi-segment partition, so every candidate is probed —
        // concurrently, since the probes are independent reads.
        let candidates: Vec<(&PartitionOrderKey, &SnapshotSegment)> = prior_keys
            .iter()
            .zip(snapshot_segments)
            .filter(|(key, _)| !already_touched.contains(key.label()))
            .collect();
        let sidecars_read = candidates.len() as u64;
        // Probe in bounded-concurrency waves of `segment_read_concurrency` (the
        // shared cold-read memory budget) — each sidecar is read-and-dropped, so
        // peak resident memory is one wave of batches. `try_join_all` per chunk
        // (rather than the `buffer_unordered` Stream combinator) keeps the
        // future's Send inference general enough to pass through the instrumented
        // Snapshot RPC handler.
        let mut labels: HashSet<Option<String>> = HashSet::new();
        for chunk in candidates.chunks(segment_read_concurrency.max(1)) {
            let wave = chunk.iter().map(|(key, segment)| {
                Self::probe_segment_for_label(readers, key, segment, sidecar_schema, &probe)
            });
            labels.extend(
                futures_util::future::try_join_all(wave)
                    .await?
                    .into_iter()
                    .flatten(),
            );
        }
        tracing::Span::current().record("sidecars_read", sidecars_read);
        tracing::Span::current().record("labels_attributed", labels.len());
        Ok(labels)
    }

    /// Probe one prior segment's row_uuid sidecar for any `probe_keys`,
    /// returning that segment's partition label if it contains one — a single
    /// concurrent unit of [`Self::reverse_lookup_attributed_labels`]. Fails
    /// fast if the segment has no sidecar (CHA-412 builds one for every
    /// snapshot segment, so absence is an invariant violation).
    async fn probe_segment_for_label<R: FormatReader>(
        readers: &HashMap<i32, R>,
        key: &PartitionOrderKey,
        segment: &SnapshotSegment,
        sidecar_schema: &SchemaRef,
        probe_keys: &[&str],
    ) -> Result<Option<Option<String>>, ApiError> {
        let sidecar_meta = segment.row_uuid_index_sidecar.as_ref().ok_or_else(|| {
            ApiError::Internal(format!(
                "carry-forward reverse lookup: prior segment {} has no row_uuid \
                 sidecar (CHA-412 builds one for every snapshot segment)",
                segment.table_snapshot_segment_uuid
            ))
        })?;
        let sidecar = ColdStorageClient::read_segment_index(readers, sidecar_meta, sidecar_schema)
            .await
            .map_err(ApiError::ColdStorage)?;
        // CHA-480: the seek kernel takes composite tuple probes; the row_uuid
        // index is single-column, so wrap each probe key as a 1-tuple.
        let probe_tuples: Vec<&[&str]> = probe_keys.iter().map(std::slice::from_ref).collect();
        let hit = !penca_format::index::seek_row_offsets(&sidecar, &probe_tuples)?.is_empty();
        Ok(hit.then(|| key.label().clone()))
    }

    /// Shared snapshot write tail: insert the uncommitted parent, drive
    /// the pack stream (files written + committed via the durable
    /// writer, carried specs accumulated), persist + commit the carried
    /// rows on the same two-phase gate, place the empty-merge
    /// placeholder when nothing landed, then commit the parent and
    /// retire older snapshots. On any error: best-effort cleanup — the
    /// durable writer's group rollback, a ROW-ONLY carried delete (the
    /// shared prior files must never be deleted here), and the
    /// uncommitted parent delete.
    /// CHA-483: materialize user secondary indexes for carried-forward segments
    /// (materialize-on-next-snapshot). A carried segment the prior snapshot
    /// already indexed carries its sidecar forward by reference; a newly-active
    /// index has the carried base file read back and the missing sidecar built.
    /// Base files stay carried-by-reference — only sidecars are added. The base
    /// reads run in bounded-concurrency waves of `segment_read_concurrency` (the
    /// shared cold-read memory budget; one read per prior segment serves all its
    /// uncovered indexes). Every row is keyed by a carried new-segment uuid, so
    /// the caller's phase-2 commit over those uuids covers them; built sidecar
    /// uris are pushed for the error-branch file cleanup.
    #[allow(clippy::too_many_arguments)]
    async fn materialize_carried_user_indexes<R: FormatReader, W: FormatWriter>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        writer: &W,
        ctx: &SnapshotWriteCtx<'_>,
        carried_specs: &[CarriedSegmentSpec],
        prior_segment_by_uuid: &HashMap<String, SnapshotSegment>,
        built_index_uris: &mut Vec<String>,
    ) -> Result<(), ApiError> {
        if ctx.user_indexes.is_empty() || carried_specs.is_empty() {
            return Ok(());
        }
        let prior_segs: Vec<String> = carried_specs
            .iter()
            .map(|spec| spec.prior_seg_uuid_str.clone())
            .collect();
        let existing = penca_storage_meta::LifecycleManager::list_segment_index_metadata(
            pool,
            ctx.catalog_str,
            ctx.branch_str,
            &prior_segs,
        )
        .await?;
        let existing_ids: HashSet<&str> = existing
            .iter()
            .map(|meta| meta.segment_index_uuid.as_str())
            .collect();

        // Classify each (carried segment, index): a prior sidecar with the
        // deterministic id ⇒ carry forward; otherwise ⇒ build. Collect the carry
        // pairs per index, and — per carried segment that needs any build — the
        // parsed new-segment uuid plus its uncovered indexes. `new_seg` is parsed
        // for every spec (covered or not) to keep the fail-fast surface up front.
        let mut covered_by_index: HashMap<String, Vec<(String, String)>> = HashMap::new();
        let mut uncovered: Vec<(&CarriedSegmentSpec, Uuid, Vec<&Index>)> = Vec::new();
        for spec in carried_specs {
            let prior_seg = Uuid::parse_str(&spec.prior_seg_uuid_str)
                .map_err(|e| ApiError::Internal(format!("invalid prior segment uuid: {e}")))?;
            let new_seg = Uuid::parse_str(&spec.new_seg_uuid_str)
                .map_err(|e| ApiError::Internal(format!("invalid carried segment uuid: {e}")))?;
            let mut to_build: Vec<&Index> = Vec::new();
            for index in ctx.user_indexes {
                let prior_sidecar_id =
                    row_uuid_for_pk(&prior_seg, &[index.index_uuid.as_str()]).to_string();
                if existing_ids.contains(prior_sidecar_id.as_str()) {
                    covered_by_index
                        .entry(index.index_uuid.clone())
                        .or_default()
                        .push((
                            spec.new_seg_uuid_str.clone(),
                            spec.prior_seg_uuid_str.clone(),
                        ));
                } else {
                    to_build.push(index);
                }
            }
            if !to_build.is_empty() {
                uncovered.push((spec, new_seg, to_build));
            }
        }

        // Read + build in bounded-concurrency waves over the carried segments
        // that need a build. Reads within a wave run concurrently (`try_join_all`,
        // not `buffer_unordered`, to keep the future's Send inference general
        // enough for the instrumented Snapshot handler); the wave's batches are
        // built and then DROPPED before the next wave reads — so peak resident
        // memory is one wave of base segments, not the whole carried set, even
        // on the post-CREATE-INDEX cycle where every segment is uncovered.
        let mut segments_read = 0usize;
        let mut sidecars_built = 0usize;
        for wave in uncovered.chunks(self.segment_read_concurrency.max(1)) {
            let reads = wave.iter().map(|(spec, _, indexes)| async move {
                let batch = read_carried_base_segment(
                    readers,
                    prior_segment_by_uuid,
                    &spec.prior_seg_uuid_str,
                    ctx.user_schema,
                    indexes,
                )
                .await?;
                Ok::<(&str, RecordBatch), ApiError>((spec.prior_seg_uuid_str.as_str(), batch))
            });
            let wave_batches: HashMap<&str, RecordBatch> =
                futures_util::future::try_join_all(reads)
                    .await?
                    .into_iter()
                    .collect();
            segments_read += wave_batches.len();
            for (spec, new_seg, indexes) in wave {
                let batch = wave_batches
                    .get(spec.prior_seg_uuid_str.as_str())
                    .expect("a base read was queued for every uncovered carried segment");
                for index in indexes {
                    let user_parent = user_parent_index_uuid(ctx.snap_uuid, index)?;
                    build_user_index_sidecar(
                        pool,
                        writer,
                        ctx,
                        &self.base_uri,
                        self.storage_format,
                        &spec.new_seg_uuid_str,
                        new_seg,
                        &user_parent,
                        index,
                        batch,
                        built_index_uris,
                    )
                    .await?;
                    sidecars_built += 1;
                }
            }
        }

        // Carry already-covered sidecars forward by reference, per index.
        for index in ctx.user_indexes {
            let Some(pairs) = covered_by_index.get(&index.index_uuid) else {
                continue;
            };
            let user_parent = user_parent_index_uuid(ctx.snap_uuid, index)?;
            penca_storage_meta::LifecycleManager::insert_carried_segment_indexes(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &user_parent,
                &index.index_uuid,
                pairs,
            )
            .await?;
        }
        // Carried base reads break the carry-forward no-read invariant, so the
        // snapshot span otherwise hides them — surface the rare cost: how many
        // carried segments were read back to build sidecars vs carried by ref.
        let sidecars_carried: usize = covered_by_index.values().map(Vec::len).sum();
        tracing::debug!(
            indexes = ctx.user_indexes.len(),
            segments_read,
            sidecars_built,
            sidecars_carried,
            "carried user-index materialization"
        );
        Ok(())
    }

    async fn finish_snapshot_write<'a, R, W>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        writer: &W,
        mut pack_steps: std::pin::Pin<
            Box<dyn futures_util::Stream<Item = Result<PackStep, ApiError>> + Send + 'a>,
        >,
        ctx: &SnapshotWriteCtx<'_>,
        prior_segment_by_uuid: &HashMap<String, SnapshotSegment>,
    ) -> Result<SnapshotResponse, ApiError>
    where
        R: FormatReader,
        W: FormatWriter,
    {
        let mut seg_writer = DurableSegmentWriter::new(SnapshotSegmentScope {
            catalog_str: ctx.catalog_str,
            branch_str: ctx.branch_str,
            table_str: ctx.table_str,
            storage_format: self.storage_format,
        });

        // Phase 1a: INSERT parent snapshot_metadata row (uncommitted).
        // Runs after the caller's fallible delta resolve, and the only
        // fallible work between here and the cleanup gate below is the
        // write loop itself — a stranded uncommitted parent is always
        // reachable by the error branch's delete.
        penca_storage_meta::LifecycleManager::insert_snapshot_metadata(
            pool,
            ctx.catalog_str,
            ctx.branch_str,
            ctx.table_str,
            ctx.snap_str,
            ctx.snapshotted_at_micros,
            ctx.partition_keys,
            ctx.clustering_keys,
            ctx.commit_seq_num,
            ctx.durable,
        )
        .await?;

        // Phase 1b: write each packed file as it flushes, accumulating
        // carried specs; then persist + commit the carried rows and the
        // empty-merge placeholder. All under one error→cleanup gate:
        // commit_snapshot_metadata (phase 2) is the atomic visibility
        // gate, so until it runs every row below the still-NULL parent
        // is unreachable regardless of its own commit state.
        let mut carried_specs: Vec<CarriedSegmentSpec> = Vec::new();
        // CHA-412: auto-build the internal `row_uuid` identity index for every
        // snapshot — a parent header + per-segment sidecars riding the same
        // two-phase gate (the phase-2 parent snapshot commit below). The index
        // is format-agnostic: the sidecar follows the table's storage format
        // (`self.storage_format`), written via the same `FormatWriter` as the
        // base segments. `parent_index_uuid` is the deterministic header id both
        // the per-segment children and the carry-forward JOIN reference.
        let parent_index_uuid = table_snapshot_index_uuid(ctx.snap_uuid, None).to_string();
        // CHA-481: the three __penca_system__ tables (schemas/tables/indexes)
        // ALSO carry a built-in composite name index, declared + materialized
        // alongside the row_uuid index on every snapshot. The classifier returns
        // None for every other table, so this is a no-op for user tables. Its
        // index_uuid is NON-NULL, so it never collides with the row_uuid parent
        // (index_uuid IS NULL) nor leaks into the row_uuid read plan's NULL join.
        let name_index_spec = system_name_index_spec(ctx.catalog_uuid, ctx.table_uuid);
        let name_index: Option<NameIndexBuild> =
            name_index_spec.as_ref().map(|spec| NameIndexBuild {
                spec,
                parent_index_uuid: table_snapshot_index_uuid(ctx.snap_uuid, Some(&spec.index_uuid))
                    .to_string(),
                slug: spec.index_uuid.to_string(),
            });
        let mut built_index_seg_uuids: Vec<String> = Vec::new();
        let mut built_index_uris: Vec<String> = Vec::new();
        let outcome: Result<(), ApiError> = async {
            // Declare + commit the snapshot's internal index parent header up
            // front. The header is fileless, so committing it before any child
            // sidecar means the error branch never has to undo a committed
            // parent that already owns committed children pointing at deleted
            // files: a child commit that fails later leaves only *uncommitted*
            // children, which the rollback deletes cleanly. The header stays
            // invisible until the phase-2 snapshot commit below (the snapshot
            // commit is the visibility gate); if this op fails, the header is
            // left an orphan that the next snapshot's retire sweep reclaims
            // (delete_orphaned_table_snapshot_index_rows). The early insert also
            // lets the carried-sidecar JOIN below resolve the parent.
            penca_storage_meta::LifecycleManager::insert_table_snapshot_index(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &parent_index_uuid,
                ctx.snap_str,
                None,
                None,
            )
            .await?;
            // CHA-483: re-declare a parent header per live user index
            // (`index_uuid` non-NULL). Inserted uncommitted alongside the
            // internal header; the single branch+snapshot-scoped commit below
            // commits them all. Re-deriving from `index_metadata` each snapshot
            // is what makes DROP lazy — a dropped index isn't listed, so no
            // parent is declared for it next cycle.
            for index in ctx.user_indexes {
                let user_parent = user_parent_index_uuid(ctx.snap_uuid, index)?;
                penca_storage_meta::LifecycleManager::insert_table_snapshot_index(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    &user_parent,
                    ctx.snap_str,
                    Some(&index.index_uuid),
                    // CHA-485: stamp the declared key columns onto the
                    // snapshot-scoped header — the planner's covering-index
                    // source (it never reads `index_metadata`, ADR 0026 §5).
                    Some(&index.columns),
                )
                .await?;
            }
            // CHA-481: declare the built-in name-index parent (non-NULL
            // index_uuid) for the system tables before the shared commit below,
            // so it commits alongside the row_uuid + user-index parents.
            if let Some(name) = &name_index {
                penca_storage_meta::LifecycleManager::insert_table_snapshot_index(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    &name.parent_index_uuid,
                    ctx.snap_str,
                    Some(&name.slug),
                    // Not planner-selectable: the metadata fast-path (CHA-484)
                    // hardcodes its index and never consults key_columns.
                    None,
                )
                .await?;
            }
            penca_storage_meta::LifecycleManager::commit_table_snapshot_index_for_snapshot(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                ctx.snap_str,
            )
            .await?;
            let mut wrote_any = false;
            while let Some(step) = pack_steps.next().await {
                match step? {
                    PackStep::File(file) => {
                        tracing::trace!(
                            target: "penca_api::snapshot_streaming",
                            uri = %file.uri,
                            partitions_in_file = file.segment_rows.len(),
                            file_rows = file.file_batch.num_rows(),
                            "packed snapshot file flush"
                        );
                        seg_writer.write_segment_group(pool, writer, &file).await?;
                        wrote_any = true;
                        build_file_segment_indexes(
                            pool,
                            writer,
                            ctx,
                            &self.base_uri,
                            self.storage_format,
                            &file,
                            &parent_index_uuid,
                            name_index.as_ref(),
                            &mut built_index_seg_uuids,
                            &mut built_index_uris,
                        )
                        .await?;
                    }
                    PackStep::Carried(specs) => carried_specs.extend(specs),
                }
            }
            // Carried rows ride the same two-phase gate as written rows:
            // insert NULL-committed, then bulk-commit.
            if !carried_specs.is_empty() {
                penca_storage_meta::LifecycleManager::insert_carried_snapshot_segments(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    ctx.snap_str,
                    &carried_specs,
                )
                .await?;
                let uuids: Vec<String> = carried_specs
                    .iter()
                    .map(|spec| spec.new_seg_uuid_str.clone())
                    .collect();
                penca_storage_meta::LifecycleManager::commit_snapshot_segments_by_uuids(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    &uuids,
                )
                .await?;
                // CHA-455: a carried base segment carries its cold-index
                // sidecars forward by reference. Insert NULL-committed,
                // then commit them in this SAME phase-2 (keyed by the new
                // segment uuids) so a sidecar never becomes visible out of
                // step with its base segment. No-op until CHA-412 emits
                // sidecars.
                let carry_pairs: Vec<(String, String)> = carried_specs
                    .iter()
                    .map(|spec| {
                        (
                            spec.new_seg_uuid_str.clone(),
                            spec.prior_seg_uuid_str.clone(),
                        )
                    })
                    .collect();
                penca_storage_meta::LifecycleManager::insert_carried_segment_indexes(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    &parent_index_uuid,
                    "row_uuid",
                    &carry_pairs,
                )
                .await?;
                // CHA-484: the built-in system-table NAME index (CHA-481) is
                // deliberately absent from this carry branch — it relies on
                // the three `__penca_system__` tables being carry-INELIGIBLE
                // (registered with empty partition/clustering keys, so the
                // CHA-406 gate never engages). If system tables ever become
                // carry-eligible, carry the name sidecar here too, or every
                // carried segment planless-degrades the CHA-484 by-name fast
                // path to the merge fallback (its entry absent from the
                // segment's keyed `index_sidecars`).
                // CHA-483: user secondary indexes on carried segments — carry
                // already-indexed sidecars forward by reference; build the
                // missing ones for newly-active indexes by reading the carried
                // base files (materialize-on-next-snapshot). Keyed by carried
                // new-segment uuids, so the phase-2 commit below covers them.
                self.materialize_carried_user_indexes(
                    pool,
                    readers,
                    writer,
                    ctx,
                    &carried_specs,
                    prior_segment_by_uuid,
                    &mut built_index_uris,
                )
                .await?;
                penca_storage_meta::LifecycleManager::commit_segment_index_metadata_for_segments(
                    pool,
                    ctx.catalog_str,
                    ctx.branch_str,
                    &uuids,
                )
                .await?;
            }
            // A carried-only snapshot commits its watermark through its
            // carried rows; the placeholder is only for a genuinely empty
            // merge (CHA-228).
            if !wrote_any && carried_specs.is_empty() {
                let placeholder = empty_merge_placeholder_step(
                    ctx.snap_uuid,
                    ctx.snap_str,
                    ctx.catalog_uuid,
                    ctx.branch_uuid,
                    &self.base_uri,
                    ctx.storage_format_text,
                    ctx.user_schema,
                );
                seg_writer
                    .write_segment_group(pool, writer, &placeholder)
                    .await?;
            }
            // CHA-412: commit the snapshot's built index sidecars (the parent
            // header was already committed up front). Still invisible until the
            // phase-2 snapshot commit below, so an index never becomes visible
            // out of step with its snapshot.
            penca_storage_meta::LifecycleManager::commit_segment_index_metadata_for_segments(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &built_index_seg_uuids,
            )
            .await?;
            Ok(())
        }
        .await;

        if let Err(err) = outcome {
            seg_writer.cleanup_on_err(pool, writer).await;
            let carried_uuids: Vec<String> = carried_specs
                .iter()
                .map(|spec| spec.new_seg_uuid_str.clone())
                .collect();
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_snapshot_segments_by_uuids(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &carried_uuids,
            )
            .await;
            // CHA-455: clean up any uncommitted carried sidecars too
            // (keyed by the new carried segment uuids).
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_segment_index_for_segments(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &carried_uuids,
            )
            .await;
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_snapshot_metadata(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                ctx.snap_str,
            )
            .await;
            // CHA-412: roll back the built index sidecars (files + uncommitted
            // child rows) and the uncommitted parent header.
            for uri in &built_index_uris {
                let _ = ColdStorageClient::delete_segment(writer, uri, true).await;
            }
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_segment_index_for_segments(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                &built_index_seg_uuids,
            )
            .await;
            let _ = penca_storage_meta::LifecycleManager::delete_uncommitted_table_snapshot_index_for_snapshot(
                pool,
                ctx.catalog_str,
                ctx.branch_str,
                ctx.snap_str,
            )
            .await;
            return Err(err);
        }

        // Phase 2: commit the parent snapshot record (auto-commit).
        penca_storage_meta::LifecycleManager::commit_snapshot_metadata(
            pool,
            ctx.catalog_str,
            ctx.branch_str,
            ctx.snap_str,
        )
        .await?;

        // CHA-468: Snapshot only materialises + commits the baseline.
        // Retirement of prior snapshots is a separate, disabled-by-default op
        // (`LifecycleManager::retire_snapshots`) — decoupled here so a newer
        // snapshot no longer strands an open (RYOW) tx's baseline and forces a
        // slow cold-persist-log reconstruction.
        Ok(SnapshotResponse {
            snapshotted_at_micros: Some(ctx.snapshotted_at_micros),
        })
    }
}

/// Borrowed context threaded into [`LifecycleManager::finish_snapshot_write`]
/// — the per-cycle identifiers, schema, and recorded layout keys it
/// needs to insert metadata and write the placeholder.
struct SnapshotWriteCtx<'a> {
    catalog_uuid: &'a Uuid,
    branch_uuid: &'a Uuid,
    // CHA-481: the snapshot target's table_uuid — classifies whether this is a
    // __penca_system__ table that also carries a built-in composite name index.
    table_uuid: &'a Uuid,
    catalog_str: &'a str,
    branch_str: &'a str,
    table_str: &'a str,
    snap_uuid: &'a Uuid,
    snap_str: &'a str,
    snapshotted_at_micros: i64,
    commit_seq_num: i64,
    /// CHA-432: whether this snapshot is a durable retention rung. Decided once,
    /// here at creation, from the last durable rung and the effective density.
    durable: bool,
    user_schema: &'a SchemaRef,
    storage_format_text: &'a str,
    partition_keys: &'a [String],
    clustering_keys: &'a [String],
    /// CHA-483: the table's live user secondary-index definitions, re-derived
    /// each snapshot into per-index parent headers + per-segment sidecars.
    user_indexes: &'a [Index],
}

/// CHA-481: the built-in composite name index to materialize on a
/// `__penca_system__` table's snapshot, alongside the row_uuid index. `spec`
/// names the key columns the per-segment sidecar sorts on; `parent_index_uuid`
/// is the committed (non-NULL `index_uuid`) name-index parent header; `slug`
/// (the `index_uuid` string) keys the sidecar uri + deterministic id, distinct
/// from the row_uuid index's `"row_uuid"` slug.
struct NameIndexBuild<'a> {
    spec: &'a SystemNameIndexSpec,
    parent_index_uuid: String,
    slug: String,
}

/// Build + write the per-segment cold-index sidecars for one packed snapshot
/// file. Every segment gets CHA-412's strictly-internal `row_uuid` identity
/// sidecar; each of the table's live user secondary indexes (CHA-483) and, for a
/// `__penca_system__` table, the built-in composite name index (CHA-481) add
/// their own composite sidecar over the same in-memory slice. Each sidecar is a
/// sorted `(key…, row_offset)` artifact (the CHA-480 kernel; the CHA-454 seek
/// reads it); every one goes through [`build_one_segment_sidecar`]. Child rows
/// reference their parent header and are NULL-committed (committed in the
/// snapshot's phase-2 gate, keyed by segment uuid). Accumulates one built
/// segment uuid per segment (for the segment-keyed phase-2 commit) + every
/// sidecar uri (for the error-branch file cleanup).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        file_uri = %file.uri,
        base_segments = file.segment_rows.len(),
        sidecars_built = tracing::field::Empty,
    ),
)]
async fn build_file_segment_indexes<W: FormatWriter>(
    pool: &PgDriver,
    writer: &W,
    ctx: &SnapshotWriteCtx<'_>,
    base_uri: &str,
    storage_format: Format,
    file: &SnapshotFileStep,
    row_uuid_parent_index_uuid: &str,
    name_index: Option<&NameIndexBuild<'_>>,
    built_seg_uuids: &mut Vec<String>,
    built_uris: &mut Vec<String>,
) -> Result<(), ApiError> {
    let n_uris_before = built_uris.len();
    for row in &file.segment_rows {
        // Zero-row segments (the empty-merge placeholder) get no sidecar.
        if row.length == 0 {
            continue;
        }
        let slice = file
            .file_batch
            .slice(row.offset as usize, row.length as usize);
        let seg_uuid = Uuid::parse_str(&row.seg_uuid_str)
            .map_err(|e| ApiError::Internal(format!("invalid segment uuid: {e}")))?;

        // CHA-412: the strictly-internal row_uuid identity index (every segment).
        let row_uuid_col = slice.column_by_name("row_uuid").ok_or_else(|| {
            ApiError::Internal("snapshot batch missing row_uuid column".to_string())
        })?;
        build_one_segment_sidecar(
            pool,
            writer,
            ctx,
            base_uri,
            storage_format,
            &seg_uuid,
            &row.seg_uuid_str,
            std::slice::from_ref(row_uuid_col),
            "row_uuid",
            row_uuid_parent_index_uuid,
            built_uris,
        )
        .await?;

        // CHA-483: one composite sidecar per live user secondary index, from the
        // same in-memory slice.
        for index in ctx.user_indexes {
            let user_parent = user_parent_index_uuid(ctx.snap_uuid, index)?;
            build_user_index_sidecar(
                pool,
                writer,
                ctx,
                base_uri,
                storage_format,
                &row.seg_uuid_str,
                &seg_uuid,
                &user_parent,
                index,
                &slice,
                built_uris,
            )
            .await?;
        }

        // CHA-481: the built-in composite name index (system tables only). Its
        // key columns are the system table's user columns (all Utf8); a missing
        // column is a fail-fast bug, not a degraded path.
        if let Some(name) = name_index {
            let key_cols = name
                .spec
                .key_columns
                .iter()
                .map(|col| {
                    slice.column_by_name(col).cloned().ok_or_else(|| {
                        ApiError::Internal(format!("snapshot batch missing {col} column"))
                    })
                })
                .collect::<Result<Vec<ArrayRef>, ApiError>>()?;
            build_one_segment_sidecar(
                pool,
                writer,
                ctx,
                base_uri,
                storage_format,
                &seg_uuid,
                &row.seg_uuid_str,
                &key_cols,
                &name.slug,
                &name.parent_index_uuid,
                built_uris,
            )
            .await?;
        }

        // One push per segment: the phase-2 commit
        // (`commit_segment_index_metadata_for_segments`) is keyed by
        // `segment_uuid` and commits all of a segment's sidecars at once.
        built_seg_uuids.push(row.seg_uuid_str.clone());
    }
    // `sidecars_built` counts written sidecars, not segments: beyond the row_uuid
    // sidecar, each user index (CHA-483) and the system name index (CHA-481) add
    // one per segment, so this exceeds the segment count when either is present.
    tracing::Span::current().record("sidecars_built", built_uris.len() - n_uris_before);
    Ok(())
}

/// CHA-483: the deterministic per-`(snapshot, index)` parent-header id for a
/// user secondary index — the value declared as the parent row and referenced
/// by every sidecar built or carried for that index this snapshot. Centralizes
/// the `index_uuid` parse + its error so the four call sites can't drift.
fn user_parent_index_uuid(snap_uuid: &Uuid, index: &Index) -> Result<String, ApiError> {
    let index_uuid = Uuid::parse_str(&index.index_uuid)
        .map_err(|e| ApiError::Internal(format!("invalid user index_uuid: {e}")))?;
    Ok(table_snapshot_index_uuid(snap_uuid, Some(&index_uuid)).to_string())
}

/// CHA-483: build + write one user secondary-index sidecar for a single base
/// segment from its in-memory rows. Derives the index's key columns from the
/// segment batch (a missing indexed column is a fail-fast bug) and delegates the
/// artifact build + child-row insert to [`build_one_segment_sidecar`], slugged by
/// the index's `index_uuid`.
#[allow(clippy::too_many_arguments)]
async fn build_user_index_sidecar<W: FormatWriter>(
    pool: &PgDriver,
    writer: &W,
    ctx: &SnapshotWriteCtx<'_>,
    base_uri: &str,
    storage_format: Format,
    seg_uuid_str: &str,
    seg_uuid: &Uuid,
    parent_index_uuid: &str,
    index: &Index,
    segment_batch: &RecordBatch,
    built_uris: &mut Vec<String>,
) -> Result<(), ApiError> {
    let key_cols: Vec<ArrayRef> = index
        .columns
        .iter()
        .map(|col| {
            segment_batch.column_by_name(col).cloned().ok_or_else(|| {
                ApiError::Internal(format!(
                    "snapshot batch missing indexed column {col:?} for index {}",
                    index.index_name
                ))
            })
        })
        .collect::<Result<_, ApiError>>()?;
    build_one_segment_sidecar(
        pool,
        writer,
        ctx,
        base_uri,
        storage_format,
        seg_uuid,
        seg_uuid_str,
        &key_cols,
        &index.index_uuid,
        parent_index_uuid,
        built_uris,
    )
    .await
}

/// Build + write a single cold-index sidecar over `key_cols` for one base
/// segment slice, then record its NULL-committed child row. The canonical core
/// shared by every per-segment index — the row_uuid identity index (CHA-412),
/// user secondary indexes (CHA-483), and the built-in system name index
/// (CHA-481). `slug` distinguishes sidecars on the same segment in both the uri
/// and the deterministic sidecar id (`"row_uuid"` for the internal identity
/// index; the `index_uuid` string for a declared index), so a fresh build and a
/// carry-forward agree on the id and a crash-retry collapses via ON CONFLICT.
/// Pushes the written uri for the error-branch file cleanup; the segment uuid is
/// tracked by the caller's per-segment accounting.
#[allow(clippy::too_many_arguments)]
async fn build_one_segment_sidecar<W: FormatWriter>(
    pool: &PgDriver,
    writer: &W,
    ctx: &SnapshotWriteCtx<'_>,
    base_uri: &str,
    storage_format: Format,
    seg_uuid: &Uuid,
    seg_uuid_str: &str,
    key_cols: &[ArrayRef],
    slug: &str,
    parent_index_uuid: &str,
    built_uris: &mut Vec<String>,
) -> Result<(), ApiError> {
    let sidecar = penca_format::index::build_segment_index(key_cols)?;
    let uri = segment_index_uri(
        base_uri,
        ctx.catalog_uuid,
        ctx.branch_uuid,
        ctx.snap_uuid,
        seg_uuid,
        slug,
        storage_format.extension(),
    );
    ColdStorageClient::write_segment_index(writer, &uri, &sidecar).await?;
    let seg_index_uuid = row_uuid_for_pk(seg_uuid, &[slug]).to_string();
    penca_storage_meta::LifecycleManager::insert_segment_index_metadata(
        pool,
        ctx.catalog_str,
        ctx.branch_str,
        &seg_index_uuid,
        seg_uuid_str,
        parent_index_uuid,
        &uri,
        0,
        sidecar.num_rows() as i64,
        storage_format.extension(),
        batch_in_memory_bytes(&sidecar)?,
        // TODO(CHA-490): populate statistics with the sorted composite-key
        // min/max bounds the cold seek consults in-planner (the kernel already
        // sorted the keys, so the bounds are the first/last entry). Deferred so
        // the seek owns the exact bound encoding rather than guessing it here.
        &[],
    )
    .await?;
    built_uris.push(uri);
    Ok(())
}

/// CHA-483: read one carried base segment's rows back into memory so a
/// newly-active user index's missing sidecar can be built (materialize-on-next-
/// snapshot). The base file stays carried-by-reference — only this read + the
/// new small sidecar are added, never a base rewrite. The returned batch is in
/// segment order, so a row's ordinal is its `row_offset`. The read is projected
/// to the union of the segment's uncovered indexes' key columns — never the full
/// schema — so a narrow index over a wide table only pulls the columns it needs,
/// keeping the per-wave cold IO + resident batch tight.
async fn read_carried_base_segment<R: FormatReader>(
    readers: &HashMap<i32, R>,
    prior_segment_by_uuid: &HashMap<String, SnapshotSegment>,
    prior_seg_uuid_str: &str,
    user_schema: &SchemaRef,
    indexes: &[&Index],
) -> Result<RecordBatch, ApiError> {
    let segment = prior_segment_by_uuid
        .get(prior_seg_uuid_str)
        .ok_or_else(|| {
            ApiError::Internal(format!(
                "carried segment {prior_seg_uuid_str} missing from prior-segment map"
            ))
        })?;
    let code = segment.format.as_wire_code();
    let reader = readers.get(&code).ok_or_else(|| {
        ApiError::Internal(format!("no reader registered for storage format {code}"))
    })?;
    // Union of indexed key columns across the segment's uncovered indexes, kept
    // in `user_schema` field order so the projected schema is a valid subset.
    let needed: HashSet<&str> = indexes
        .iter()
        .flat_map(|index| index.columns.iter().map(String::as_str))
        .collect();
    let projected_fields: Vec<_> = user_schema
        .fields()
        .iter()
        .filter(|field| needed.contains(field.name().as_str()))
        .map(|field| field.as_ref().clone())
        .collect();
    let projected_schema: SchemaRef = Arc::new(Schema::new(projected_fields));
    let cols: Vec<&str> = projected_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    reader
        .read_segment(
            &segment.uri,
            Some(segment.offset),
            Some(segment.length),
            &projected_schema,
            Some(&cols),
        )
        .await
        .map_err(|e| ApiError::Internal(format!("carried base read failed: {e}")))
}

/// True when every partition-key column is also a primary-key column (the v1
/// carry-forward shape). When false (CHA-448 v2), delete attribution can't read
/// the partition column from the cold delete-log and a partition-key move keeps
/// the same row_uuid, so the touched set is completed by the row_uuid
/// reverse-lookup instead of `delete_attributed_labels`.
fn partition_subset_of_pk(partition_keys: &[String], primary_keys: &[String]) -> bool {
    partition_keys.iter().all(|key| primary_keys.contains(key))
}

/// CHA-432: decide whether the snapshot being created is a `durable` retention
/// rung — the pure kernel of sticky durable assignment (ADR 0025 §2). Decided
/// once, at creation, from the last durable rung's watermark and the effective
/// density; the caller persists the result so the floor stays monotonic.
///
/// Durable iff any of:
/// - there is no prior durable rung (`last_durable_at_micros` is `None`) — the
///   first snapshot always anchors the ladder;
/// - density is unset (`None`) — keep-all-in-window, every snapshot durable
///   (CHA-55);
/// - the gap since the last durable is at least one density window
///   (`snapshotted_at_micros - last >= snapshot_density_seconds * 1_000_000`;
///   seconds→micros converted at the comparison).
fn decide_durable(
    last_durable_at_micros: Option<i64>,
    snapshotted_at_micros: i64,
    density_seconds: Option<i64>,
) -> bool {
    match (last_durable_at_micros, density_seconds) {
        (None, _) => true,
        (Some(_), None) => true,
        (Some(last), Some(density_seconds)) => {
            snapshotted_at_micros - last >= density_seconds * 1_000_000
        }
    }
}

/// Collect the `row_uuid` of every resolved delta row (the window's upsert
/// identities) across the partition groups. CHA-448 probes these against the
/// prior snapshot's row_uuid sidecars to attribute each row's PRIOR partition —
/// the upsert half of the touched set, which catches a partition-key move
/// (same row_uuid, no delete). Pure over its input.
fn touched_row_uuids_from_delta(
    delta_groups: &[(Option<String>, RecordBatch)],
) -> Result<HashSet<String>, ApiError> {
    let mut row_uuids: HashSet<String> = HashSet::new();
    for (_, batch) in delta_groups {
        let column = batch
            .column_by_name("row_uuid")
            .ok_or_else(|| ApiError::Internal("delta batch missing row_uuid column".to_string()))?;
        let values = column
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| ApiError::Internal("delta row_uuid column is not Utf8".to_string()))?;
        for row in 0..values.len() {
            if values.is_valid(row) {
                row_uuids.insert(values.value(row).to_string());
            }
        }
    }
    Ok(row_uuids)
}

/// CHA-406 carry-forward eligibility gate (ADR 0024 §3). Returns
/// `Some(per-segment keys)` when carry-forward applies — the typed
/// [`PartitionOrderKey`]s (one per prior segment, in segment/chunk_idx
/// order) double as the touched/carried split key (via `.label()`) and the
/// carried map's typed ordering key (CHA-459) — or `None` to fall back to
/// the CHA-404 full rewrite. Pure over its inputs.
#[allow(clippy::too_many_arguments)]
fn carry_forward_keys(
    ordering: &PartitionOrdering,
    snapshot_segments: &[SnapshotSegment],
    recorded_partition_keys: Option<&[String]>,
    recorded_clustering_keys: Option<&[String]>,
    partition_keys: &[String],
    clustering_keys: &[String],
    user_schema: &SchemaRef,
) -> Result<Option<Vec<PartitionOrderKey>>, ApiError> {
    // (a) a prior committed snapshot with non-placeholder segments
    // (zero-row placeholders are already filtered out upstream).
    if snapshot_segments.is_empty() {
        return Ok(None);
    }
    // (c) partitioned — an unpartitioned table is one partition, always
    // touched, so carry-forward buys nothing.
    if partition_keys.is_empty() {
        return Ok(None);
    }
    // (b) recorded layout keys present AND equal to the current ones.
    // `None` = a pre-CHA-404 parent row (unknown keys) → full rewrite;
    // any key change is the ADR 0024 layout-key invariant → full
    // rewrite.
    if recorded_partition_keys != Some(partition_keys)
        || recorded_clustering_keys != Some(clustering_keys)
    {
        return Ok(None);
    }
    // The gate is partition-key/PK-agnostic: whether the partition columns are
    // a PK subset only decides HOW the touched set is attributed (subset →
    // delete-log columns; non-subset → CHA-448 row_uuid reverse lookup), which
    // is `snapshot_locked`'s concern, not an eligibility question. CHA-412
    // builds a row_uuid sidecar for every snapshot segment, so the non-subset
    // reverse lookup always has its index (a missing one is an invariant
    // violation that `reverse_lookup_attributed_labels` fails fast on, not a
    // full-rewrite fallback).
    //
    // (d) every prior segment's typed key must be derivable from its
    // statistics; any underivable segment → warn + full rewrite.
    let mut keys = Vec::with_capacity(snapshot_segments.len());
    for seg in snapshot_segments {
        match partition_order_key_from_statistics(
            ordering,
            &seg.statistics,
            partition_keys,
            user_schema,
        )? {
            Some(key) => keys.push(key),
            None => {
                tracing::warn!(
                    target: "penca_api::snapshot_carry_forward",
                    segment = %seg.table_snapshot_segment_uuid,
                    "prior segment partition key underivable from statistics; \
                     falling back to full rewrite (CHA-404)"
                );
                return Ok(None);
            }
        }
    }
    Ok(Some(keys))
}

/// Split the prior snapshot's segments by their stats-derived partition
/// (CHA-406): segments whose label is in `touched` go to the rewrite
/// stream (original order preserved — that order is chunk_idx order =
/// typed-order runs, the ByPlan contract); the rest become the carried map
/// (typed [`PartitionOrderKey`] → that partition's prior segment uuids, in
/// prior chunk_idx order). `prior_keys` is aligned with `snapshot_segments`
/// 1:1 (the `carry_forward_keys` output), so the zip pairs each segment
/// with its key. Touched membership is by label identity (`.label()`); the
/// carried map orders by the typed key (CHA-459).
fn split_prior_segments_by_touch(
    prior_keys: Vec<PartitionOrderKey>,
    snapshot_segments: Vec<SnapshotSegment>,
    touched: &HashSet<Option<String>>,
) -> (
    Vec<SnapshotSegment>,
    BTreeMap<PartitionOrderKey, Vec<String>>,
) {
    let mut touched_segments: Vec<SnapshotSegment> = Vec::new();
    let mut carried: BTreeMap<PartitionOrderKey, Vec<String>> = BTreeMap::new();
    for (key, seg) in prior_keys.into_iter().zip(snapshot_segments) {
        if touched.contains(key.label()) {
            touched_segments.push(seg);
        } else {
            carried
                .entry(key)
                .or_default()
                .push(seg.table_snapshot_segment_uuid);
        }
    }
    (touched_segments, carried)
}

/// Shared fixtures for the CHA-404 red-test modules below.
#[cfg(test)]
mod rt_fixtures {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use uuid::Uuid;

    use crate::lifecycle::batch_util::{PartitionOrderKey, PartitionOrdering};
    use crate::lifecycle::packer::SegmentPacker;
    use arrow::array::ArrayRef;

    /// `row_uuid, pk, v` — the post-merge output shape (row_uuid + user
    /// cols) with `pk` as the single partition key.
    pub(super) fn rt_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("pk", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// One partition's rows: every row carries `pk = label`.
    pub(super) fn part_batch(label: &str, n_rows: usize) -> RecordBatch {
        let row_uuids: Vec<String> = (0..n_rows).map(|i| format!("{label}-r{i}")).collect();
        let pks = vec![label.to_string(); n_rows];
        let vs: Vec<i64> = (0..n_rows as i64).collect();
        RecordBatch::try_new(
            rt_schema(),
            vec![
                Arc::new(StringArray::from(row_uuids)),
                Arc::new(StringArray::from(pks)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap()
    }

    /// One partition's rows with explicit sort-key values; `row_uuid`
    /// encodes label+value so positional assertions stay readable.
    pub(super) fn part_rows(label: &str, vs: &[i64]) -> RecordBatch {
        let row_uuids: Vec<String> = vs.iter().map(|v| format!("{label}-v{v}")).collect();
        let pks = vec![label.to_string(); vs.len()];
        RecordBatch::try_new(
            rt_schema(),
            vec![
                Arc::new(StringArray::from(row_uuids)),
                Arc::new(StringArray::from(pks)),
                Arc::new(Int64Array::from(vs.to_vec())),
            ],
        )
        .unwrap()
    }

    pub(super) fn test_packer(max_segment_bytes: i64) -> SegmentPacker {
        SegmentPacker::new(
            &Uuid::from_u128(1),
            &Uuid::from_u128(2),
            &Uuid::from_u128(3),
            "memory://rt1",
            "parquet",
            max_segment_bytes,
        )
    }

    /// The typed `PartitionOrdering` for the `rt_schema` single Utf8 `pk`
    /// partition key (CHA-459) — what `pack_merged_partition_stream` now
    /// takes in place of a bare `partition_keys` vec.
    pub(super) fn rt_ordering() -> PartitionOrdering {
        PartitionOrdering::new(&rt_schema(), &["pk".to_string()]).unwrap()
    }

    /// A typed `PartitionOrderKey` for a single Utf8 `pk` label — used to
    /// build typed-keyed carried maps in the packer tests.
    pub(super) fn rt_key(label: &str) -> PartitionOrderKey {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![label]));
        rt_ordering()
            .order_key_from_key_arrays(&[arr], Some(label.to_string()))
            .unwrap()
    }
}

/// CHA-404 red tests — acceptance criterion 1: peak memory is bounded by
/// the packing buffer + one in-flight partition, never the whole table.
///
/// These tests are this ticket's outer TDD loop and are committed RED:
/// they reference the planned `lifecycle::packer` seam (`SegmentPacker`,
/// `pack_merged_partition_stream`) and the multi-row durable file step
/// (`SnapshotFileStep`) before those exist, fixing the API as its first
/// consumer. Expected failure mode pre-implementation: unresolved
/// `packer` / `SnapshotFileStep` imports (compile-red), then
/// assertion-red once stubs exist but the path still materializes the
/// whole table.
#[cfg(test)]
mod streaming_red_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::StreamExt;

    use crate::lifecycle::chunker::batch_in_memory_bytes;
    use crate::lifecycle::durable_writer::SnapshotFileStep;
    use crate::lifecycle::packer::{PackStep, pack_merged_partition_stream};

    use super::rt_fixtures::{part_batch, rt_ordering, test_packer};

    /// Per-partition segment rows must tile their file exactly:
    /// ascending offsets, lengths summing to the file's row count.
    fn assert_rows_tile_file(step: &SnapshotFileStep) {
        let mut expected_offset: i64 = 0;
        for row in &step.segment_rows {
            assert_eq!(
                row.offset, expected_offset,
                "offsets must be ascending+dense"
            );
            assert!(row.length > 0 || step.file_batch.num_rows() == 0);
            expected_offset += row.length;
        }
        assert_eq!(
            expected_offset,
            step.file_batch.num_rows() as i64,
            "segment row lengths must sum to the file row count"
        );
    }

    /// Packer accumulates whole partitions and flushes exactly when the
    /// next partition would exceed `max_segment_bytes`: a flushed file
    /// packs multiple partitions (one segment row each), and a partition
    /// is never split across files.
    #[test]
    fn packer_flushes_at_whole_partition_boundaries() {
        let a = part_batch("a", 4);
        let b = part_batch("b", 4);
        let c = part_batch("c", 4);
        let part_bytes = batch_in_memory_bytes(&a).unwrap();
        // Two partitions fit, three do not.
        let max = part_bytes * 2 + part_bytes / 2;

        let mut packer = test_packer(max);
        assert!(
            packer
                .push_partition(Some("a".into()), a)
                .unwrap()
                .is_empty()
        );
        assert!(
            packer
                .push_partition(Some("b".into()), b)
                .unwrap()
                .is_empty()
        );
        // Pushing c would breach the cap → file{a,b} flushes first.
        let flushed = packer.push_partition(Some("c".into()), c).unwrap();
        assert_eq!(flushed.len(), 1, "exactly one packed file flushes");
        let file_ab = &flushed[0];
        let labels: Vec<Option<&str>> = file_ab
            .segment_rows
            .iter()
            .map(|r| r.partition_value.as_deref())
            .collect();
        assert_eq!(
            labels,
            vec![Some("a"), Some("b")],
            "two whole partitions share one file"
        );
        assert_eq!(file_ab.file_batch.num_rows(), 8);
        assert_rows_tile_file(file_ab);
        assert!(
            batch_in_memory_bytes(&file_ab.file_batch).unwrap() <= max,
            "packed file must respect max_segment_bytes"
        );
        let chunk_idxs: Vec<u32> = file_ab.segment_rows.iter().map(|r| r.chunk_idx).collect();
        assert_eq!(
            chunk_idxs,
            vec![0, 1],
            "global chunk_idx is dense across the file"
        );

        let rest = packer.finish().unwrap();
        assert_eq!(rest.len(), 1, "final flush emits the buffered remainder");
        let file_c = &rest[0];
        assert_eq!(file_c.segment_rows.len(), 1);
        assert_eq!(file_c.segment_rows[0].partition_value.as_deref(), Some("c"));
        assert_eq!(
            file_c.segment_rows[0].chunk_idx, 2,
            "chunk_idx continues globally"
        );
        assert_rows_tile_file(file_c);
        assert_ne!(file_ab.uri, file_c.uri, "flushes go to distinct files");
    }

    /// A single partition larger than `max_segment_bytes` cannot pack:
    /// the pending buffer flushes first, then the oversized partition is
    /// split via `chunk_row_ranges` into its own single-segment files.
    ///
    /// Deliberate contract (roborev review on the red commit): the
    /// under-cap TAIL chunk also flushes immediately rather than being
    /// retained in the buffer to pack with later partitions. Oversized
    /// handling stays a self-contained `chunk_row_ranges` pass-through —
    /// the packing loss is at most one under-cap file per oversized
    /// partition, and keeping the oversized case isolated from the
    /// accumulation path is the simpler invariant to reason about
    /// (KISS; ADR 0024 mandates neither choice).
    #[test]
    fn packer_splits_oversized_partition_after_flushing_buffer() {
        let small = part_batch("a", 2);
        let huge = part_batch("b", 10);
        // Cap ≈ 4 rows of b: b (10 rows) must split into ≥3 chunks.
        let max = batch_in_memory_bytes(&huge.slice(0, 4)).unwrap();

        let mut packer = test_packer(max);
        assert!(
            packer
                .push_partition(Some("a".into()), small)
                .unwrap()
                .is_empty()
        );
        let flushed = packer.push_partition(Some("b".into()), huge).unwrap();
        assert!(
            flushed.len() >= 4,
            "expected file{{a}} + >=3 oversized-split files, got {}",
            flushed.len()
        );
        assert_eq!(flushed[0].segment_rows.len(), 1);
        assert_eq!(
            flushed[0].segment_rows[0].partition_value.as_deref(),
            Some("a")
        );
        let mut b_rows_total = 0i64;
        for step in &flushed[1..] {
            assert_eq!(
                step.segment_rows.len(),
                1,
                "oversized chunks are single-segment files"
            );
            let row = &step.segment_rows[0];
            assert_eq!(row.partition_value.as_deref(), Some("b"));
            assert_eq!(row.offset, 0, "each oversized chunk owns its file");
            b_rows_total += row.length;
            assert_rows_tile_file(step);
        }
        assert_eq!(
            b_rows_total, 10,
            "oversized split covers every row exactly once"
        );
        assert!(packer.finish().unwrap().is_empty(), "nothing left buffered");
    }

    /// The label-sorted-runs guard: an out-of-order prior-stream label
    /// (and equally a label resurfacing after a later one) must surface
    /// as `ApiError::Internal`, not silent mis-packing — it is the only
    /// thing standing between a violated `ORDER BY seg.chunk_idx`
    /// contract and corrupt partition placement.
    #[tokio::test]
    async fn out_of_order_prior_labels_fail_fast() {
        use futures_util::StreamExt;

        for batches in [
            // b then a: strictly decreasing.
            vec![part_batch("b", 2), part_batch("a", 2)],
            // a, b, then a again: resurfacing.
            vec![part_batch("a", 2), part_batch("b", 2), part_batch("a", 2)],
        ] {
            let input = futures_util::stream::iter(
                batches.into_iter().map(Ok::<_, penca_merge::MergeError>),
            )
            .boxed();
            let mut out = pack_merged_partition_stream(
                Vec::new(),
                input,
                rt_ordering(),
                Vec::new(),
                std::collections::BTreeMap::new(),
                test_packer(1 << 20),
            );
            let mut saw_internal = false;
            while let Some(step) = out.next().await {
                if let Err(crate::error::ApiError::Internal(msg)) = step {
                    assert!(
                        msg.contains("out of order"),
                        "unexpected Internal message: {msg}"
                    );
                    saw_internal = true;
                    break;
                }
            }
            assert!(saw_internal, "out-of-order prior label must fail fast");
        }
    }

    /// Finishing an empty packer emits nothing — the zero-row
    /// empty-merge placeholder (CHA-228) is the orchestration's job,
    /// not the packer's.
    #[test]
    fn packer_empty_finish_emits_nothing() {
        let packer = test_packer(1024);
        assert!(packer.finish().unwrap().is_empty());
    }

    /// The streaming proof: `pack_merged_partition_stream` must emit its
    /// first packed file BEFORE the prior-snapshot input stream is
    /// exhausted — reads and writes interleave; the path never collects
    /// the whole stream first (which is exactly what the legacy
    /// `collect_merge_read` + `build_segments_to_write` shape did).
    #[tokio::test]
    async fn pack_stream_emits_before_input_exhausted() {
        // Four label-sorted single-partition segments; cap fits only one
        // partition per file, so every completed partition flushes.
        let batches = vec![
            part_batch("a", 4),
            part_batch("b", 4),
            part_batch("c", 4),
            part_batch("d", 4),
        ];
        let n_inputs = batches.len();
        let part_bytes = batch_in_memory_bytes(&batches[0]).unwrap();
        let max = part_bytes + part_bytes / 2;

        let yielded = Arc::new(AtomicUsize::new(0));
        let yielded_in_stream = yielded.clone();
        let input = futures_util::stream::iter(batches.into_iter().enumerate())
            .map(move |(i, b)| {
                yielded_in_stream.store(i + 1, Ordering::SeqCst);
                Ok::<_, penca_merge::MergeError>(b)
            })
            .boxed();

        let mut out = pack_merged_partition_stream(
            Vec::new(), // no delta groups — prior stream only
            input,
            rt_ordering(),
            Vec::new(),                        // no sort keys
            std::collections::BTreeMap::new(), // no carried partitions
            test_packer(max),
        );

        let mut emitted: Vec<SnapshotFileStep> = Vec::new();
        while let Some(step) = out.next().await {
            let PackStep::File(step) = step.unwrap() else {
                panic!("prior-only pack stream must emit only file steps");
            };
            if emitted.is_empty() {
                let consumed = yielded.load(Ordering::SeqCst);
                assert!(
                    consumed < n_inputs,
                    "first file must flush before the input stream is exhausted \
                     (interleaving); input had already yielded {consumed}/{n_inputs}"
                );
            }
            emitted.push(step);
        }

        assert_eq!(
            emitted.len(),
            4,
            "one packed file per partition at this cap"
        );
        let labels: Vec<Option<String>> = emitted
            .iter()
            .map(|s| s.segment_rows[0].partition_value.clone())
            .collect();
        assert_eq!(
            labels,
            vec![
                Some("a".into()),
                Some("b".into()),
                Some("c".into()),
                Some("d".into())
            ],
            "files flush in label order"
        );
    }
}

/// CHA-406 red tests — the carry-forward streaming contracts.
///
/// Committed RED like the CHA-404 modules above (planned-seam
/// precedent): `oversized_partition_streams_before_prior_exhausted`
/// is assertion-red until the two-cursor streaming sorted-merge lands
/// (today `complete_partition` buffers the whole run before pushing);
/// `carried_labels_consume_chunk_idx_in_label_order` is compile-red —
/// it is the first consumer of the carried-interleave entry point
/// (`pack_merged_partition_stream`'s carried map + `PackStep` enum
/// output) and fixes that API's shape.
#[cfg(test)]
mod cf_streaming_red_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::StreamExt;

    use crate::lifecycle::batch_util::PartitionOrderKey;
    use crate::lifecycle::chunker::batch_in_memory_bytes;
    use crate::lifecycle::packer::{PackStep, pack_merged_partition_stream};

    use super::rt_fixtures::{part_rows, rt_key, rt_ordering, test_packer};

    /// Sub-partition memory bound (ADR 0024 "Streaming sorted-merge"):
    /// a single partition larger than the cap, its prior rows split
    /// across four clustering-sorted stream batches with delta rows
    /// interleaving across the whole key range, must flush its FIRST
    /// packed file before the prior input stream is exhausted. Today
    /// the run buffers whole (`run_batches`) and nothing emits until
    /// the partition completes at stream end — assertion-red.
    #[tokio::test]
    async fn oversized_partition_streams_before_prior_exhausted() {
        // Prior leg: one partition, even sort keys 0..30, four batches.
        let prior: Vec<_> = (0..4i64)
            .map(|i| {
                let vs: Vec<i64> = (0..4).map(|j| (i * 4 + j) * 2).collect();
                part_rows("a", &vs)
            })
            .collect();
        let n_inputs = prior.len();
        // Delta leg: odd keys interleaving the full prior range.
        let delta = part_rows("a", &[1, 7, 13, 19, 25, 31]);
        let total_rows = 16 + 6;
        // Cap ≈ 5 rows: far under the 22-row partition, comfortably
        // above one 4-row prior batch — a streaming merge can emit a
        // full file from roughly two consumed batches.
        let max = batch_in_memory_bytes(&part_rows("a", &[0, 2, 4, 6, 8])).unwrap();

        let yielded = Arc::new(AtomicUsize::new(0));
        let yielded_in_stream = yielded.clone();
        let input = futures_util::stream::iter(prior.into_iter().enumerate())
            .map(move |(i, b)| {
                yielded_in_stream.store(i + 1, Ordering::SeqCst);
                Ok::<_, penca_merge::MergeError>(b)
            })
            .boxed();

        let mut out = pack_merged_partition_stream(
            vec![(Some("a".to_string()), delta)],
            input,
            rt_ordering(),
            vec!["v".to_string()],
            std::collections::BTreeMap::new(),
            test_packer(max),
        );

        let mut emitted_rows = 0usize;
        let mut first_seen = false;
        while let Some(step) = out.next().await {
            let PackStep::File(step) = step.unwrap() else {
                panic!("no carried partitions in this fixture");
            };
            if !first_seen {
                first_seen = true;
                let consumed = yielded.load(Ordering::SeqCst);
                assert!(
                    consumed < n_inputs,
                    "first file of an oversized partition must flush before its \
                     prior input batches are fully consumed (sub-partition \
                     streaming); input had already yielded {consumed}/{n_inputs}"
                );
            }
            emitted_rows += step.file_batch.num_rows();
        }
        assert_eq!(emitted_rows, total_rows, "every merged row lands in a file");
    }

    /// Carried labels consume chunk_idx at their LABEL position, dense
    /// across rewritten and carried rows alike. Why it matters:
    /// `read_snapshot_segments_for_table`'s `ORDER BY seg.chunk_idx`
    /// is the next cycle's ByPlan label-sorted-run contract — a
    /// carried row whose chunk_idx is out of label order breaks
    /// `complete_partition`'s out-of-order fail-fast on the NEXT
    /// snapshot. Compile-red: the carried map parameter and the
    /// `PackStep` enum do not exist yet; this test fixes their shape.
    #[tokio::test]
    async fn carried_labels_consume_chunk_idx_in_label_order() {
        use std::collections::BTreeMap;

        use penca_core::naming::table_snapshot_segment_uuid;
        use uuid::Uuid;

        // Touched labels a and c arrive via the prior stream
        // (rewrites); untouched label b is carried by reference
        // between them. Cap under one partition's bytes so each
        // rewritten partition flushes (oversized-split) as it
        // completes, exposing the emission order around the carried
        // step.
        let a = part_rows("a", &[0, 2, 4, 6]);
        let c = part_rows("c", &[1, 3, 5, 7]);
        let max = batch_in_memory_bytes(&a).unwrap() / 2;

        let prior_b_uuid = Uuid::from_u128(0xb0b).to_string();
        let carried: BTreeMap<PartitionOrderKey, Vec<String>> =
            BTreeMap::from([(rt_key("b"), vec![prior_b_uuid.clone()])]);

        let input =
            futures_util::stream::iter([a, c].into_iter().map(Ok::<_, penca_merge::MergeError>))
                .boxed();
        let mut out = pack_merged_partition_stream(
            Vec::new(),
            input,
            rt_ordering(),
            Vec::new(),
            carried,
            test_packer(max),
        );

        // (label, chunk_idx) in emission order; carried steps recorded
        // under their spec'd new uuid for the determinism assertion.
        let mut sequence: Vec<(Option<String>, u32)> = Vec::new();
        let mut carried_specs = Vec::new();
        while let Some(step) = out.next().await {
            match step.unwrap() {
                PackStep::File(file) => {
                    for row in &file.segment_rows {
                        sequence.push((row.partition_value.clone(), row.chunk_idx));
                    }
                }
                PackStep::Carried(specs) => {
                    for spec in specs {
                        sequence.push((Some("b".to_string()), spec.chunk_idx));
                        carried_specs.push(spec);
                    }
                }
            }
        }

        // chunk_idx is dense in LABEL order across the file/carried mix.
        let labels: Vec<Option<String>> = sequence.iter().map(|(l, _)| l.clone()).collect();
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(
            labels, sorted,
            "emission (and chunk_idx) follows label order"
        );
        let idxs: Vec<u32> = sequence.iter().map(|(_, i)| *i).collect();
        assert_eq!(
            idxs,
            (0..sequence.len() as u32).collect::<Vec<_>>(),
            "chunk_idx dense across rewritten AND carried rows"
        );

        // The carried step interleaves BETWEEN the a and c file steps.
        let b_pos = sequence
            .iter()
            .position(|(l, _)| l.as_deref() == Some("b"))
            .expect("carried label b emitted");
        assert!(
            sequence[..b_pos]
                .iter()
                .all(|(l, _)| l.as_deref() == Some("a")),
            "everything before the carried step is label a"
        );
        assert!(
            sequence[b_pos + 1..]
                .iter()
                .all(|(l, _)| l.as_deref() == Some("c")),
            "everything after the carried step is label c"
        );

        // Spec content: prior uuid passes through; the new uuid is the
        // deterministic snapshot-uuid + chunk_idx derivation.
        assert_eq!(carried_specs.len(), 1);
        let spec = &carried_specs[0];
        assert_eq!(spec.prior_seg_uuid_str, prior_b_uuid);
        assert_eq!(
            spec.new_seg_uuid_str,
            table_snapshot_segment_uuid(&Uuid::from_u128(1), spec.chunk_idx).to_string(),
            "carried uuid is deterministic from snap uuid + assigned chunk_idx"
        );
    }

    /// The hard chunk_idx case: rewritten partitions a and c are BOTH
    /// under-cap, so without intervention they would pack into one
    /// shared file (a packs with c). A carried label b between them must
    /// force the buffer to flush so a's chunk_idx stays below b's below
    /// c's — otherwise a and c share a file spanning the carried b and
    /// the `ORDER BY chunk_idx` label-sorted contract breaks.
    #[tokio::test]
    async fn under_cap_carried_label_forces_buffer_flush() {
        use std::collections::BTreeMap;

        use uuid::Uuid;

        use crate::lifecycle::packer::PackStep;

        // Cap large enough that a and c each stay under it (they would
        // pack together absent the carried b).
        let a = part_rows("a", &[0, 1]);
        let c = part_rows("c", &[2, 3]);
        let prior_b_uuid = Uuid::from_u128(0xb0b).to_string();
        let carried: BTreeMap<PartitionOrderKey, Vec<String>> =
            BTreeMap::from([(rt_key("b"), vec![prior_b_uuid])]);

        let input =
            futures_util::stream::iter([a, c].into_iter().map(Ok::<_, penca_merge::MergeError>))
                .boxed();
        let mut out = pack_merged_partition_stream(
            Vec::new(),
            input,
            rt_ordering(),
            Vec::new(),
            carried,
            test_packer(1 << 20),
        );

        let mut sequence: Vec<(Option<String>, u32)> = Vec::new();
        let mut a_uri: Option<String> = None;
        let mut c_uri: Option<String> = None;
        while let Some(step) = out.next().await {
            match step.unwrap() {
                PackStep::File(file) => {
                    for row in &file.segment_rows {
                        sequence.push((row.partition_value.clone(), row.chunk_idx));
                        match row.partition_value.as_deref() {
                            Some("a") => a_uri = Some(file.uri.clone()),
                            Some("c") => c_uri = Some(file.uri.clone()),
                            _ => {}
                        }
                    }
                }
                PackStep::Carried(specs) => {
                    for spec in specs {
                        sequence.push((Some("b".to_string()), spec.chunk_idx));
                    }
                }
            }
        }

        assert_eq!(
            sequence,
            vec![
                (Some("a".to_string()), 0),
                (Some("b".to_string()), 1),
                (Some("c".to_string()), 2),
            ],
            "dense label-ordered chunk_idx across the under-cap a / carried b / under-cap c mix"
        );
        assert_ne!(
            a_uri, c_uri,
            "the carried b between a and c must split them into separate files"
        );
    }
}

/// CHA-404 red tests — acceptance criterion 2: the packed streaming
/// write produces the same row content per partition as today's
/// whole-table merge (`build_segments_to_write` as the reference
/// oracle), plus the packing invariants the new layout introduces.
///
/// Committed RED like `streaming_red_tests`: references the planned
/// `lifecycle::packer` seam. The legacy planner stays in production
/// until the restructure task lands; once it is removed from the
/// production path, its planning logic survives only as this module's
/// oracle.
///
/// Exclusion-set scope note: the partition-key-move case is pinned at
/// THIS seam by modeling the upstream anti-join (the moved row's stale
/// copy never reaches the stream — that behavior is penca-merge's,
/// covered by its CHA-411 tests and `just integration-test lifecycle`).
#[cfg(test)]
mod content_equivalence_red_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use arrow::util::display::array_value_to_string;
    use futures_util::StreamExt;
    use uuid::Uuid;

    use super::rt_fixtures::test_packer;
    use crate::error::ApiError;
    use crate::lifecycle::batch_util::{
        PartitionOrdering, partition_record_batch, sort_record_batch_by_keys,
    };
    use crate::lifecycle::chunker::{batch_in_memory_bytes, chunk_row_ranges};
    use crate::lifecycle::durable_writer::SnapshotFileStep;
    use crate::lifecycle::packer::{PackStep, pack_merged_partition_stream};
    use penca_core::naming::{snapshot_segment_uri, table_snapshot_segment_uuid};

    /// The LEGACY whole-table planner, preserved verbatim as this
    /// module's reference oracle after CHA-404 removed it from the
    /// production path (the packed streaming write replaced it).
    /// One per-segment write step planned by
    /// [`LifecycleManager::build_segments_to_write`]. `size_bytes` is the
    /// chunk's standalone in-memory footprint (CHA-347), carried from the
    /// chunker to be recorded. A named struct (not a positional tuple) to
    /// avoid field-transposition hazards and mirror [`SnapshotSegmentStep`].
    // Verbatim legacy copy — fields the oracle comparisons don't read
    // stay for fidelity with what production used to build.
    #[allow(dead_code)]
    struct SnapshotSegmentWriteStep {
        seg_uuid_str: String,
        uri: String,
        partition_value: Option<String>,
        chunk_idx: u32,
        batch: RecordBatch,
        size_bytes: i64,
    }

    /// Partition `merged` by `partition_keys`, sort each partition by
    /// `clustering_keys`, chunk each by `max_segment_bytes`, and assemble
    /// the per-segment write tuples. Clustering before chunking lays each
    /// segment out as a contiguous clustering-key range, so its per-column
    /// min/max stats are tight enough for the snapshot-tier segment pruner
    /// (ADR 0022) to skip — without it segments inherit the merge's
    /// `row_uuid` order and prune-by-stats never fires.
    /// `chunk_idx` increments globally across every chunk of every
    /// partition in one snapshot cycle so sibling segment_uuids stay
    /// distinct (it's the only uniquifier in
    /// `table_snapshot_segment_uuid`).
    ///
    /// CHA-228: empty-merge case (all rows tombstoned by new persist) —
    /// emit one zero-row placeholder segment at `chunk_idx = 0` so the
    /// watermark gets committed to `table_snapshot_metadata`. Without
    /// the placeholder, the next Snapshot(T) would re-derive
    /// `cold_data_max = Some(v)` from the same persist segments and
    /// redo the merge-read forever.
    /// `read_snapshot_segments_for_table` filters zero-row rows out of
    /// `SnapshotResult.snapshot_segments` after capturing the
    /// watermark, so cold merge-reads against this empty snapshot see
    /// `segments = []`.
    #[allow(clippy::too_many_arguments)]
    fn build_segments_to_write(
        merged: &RecordBatch,
        partition_keys: &[String],
        clustering_keys: &[String],
        snap_uuid: &Uuid,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        max_segment_bytes: i64,
        base_uri: &str,
        storage_format_text: &str,
    ) -> Result<Vec<SnapshotSegmentWriteStep>, ApiError> {
        let partitions = partition_record_batch(merged, partition_keys)?;

        // CHA-215: chunk each partition's batch so no emitted snapshot
        // segment exceeds `max_segment_bytes`.
        let mut segments_to_write: Vec<SnapshotSegmentWriteStep> = Vec::new();
        let mut chunk_idx: u32 = 0;
        for (pv, part_batch) in &partitions {
            if part_batch.num_rows() == 0 {
                continue;
            }
            // Cluster within the partition so segments come out as
            // contiguous clustering-key ranges (prunable min/max).
            let clustered = sort_record_batch_by_keys(part_batch, clustering_keys)?;
            for (offset, len, in_memory_bytes) in chunk_row_ranges(&clustered, max_segment_bytes)? {
                let chunk_batch = clustered.slice(offset, len);
                let seg_uuid = table_snapshot_segment_uuid(snap_uuid, chunk_idx);
                let uri = snapshot_segment_uri(
                    base_uri,
                    catalog_uuid,
                    branch_uuid,
                    snap_uuid,
                    &seg_uuid,
                    storage_format_text,
                );
                segments_to_write.push(SnapshotSegmentWriteStep {
                    seg_uuid_str: seg_uuid.to_string(),
                    uri,
                    partition_value: pv.clone(),
                    chunk_idx,
                    batch: chunk_batch,
                    size_bytes: in_memory_bytes,
                });
                chunk_idx += 1;
            }
        }

        if segments_to_write.is_empty() {
            let seg_uuid = table_snapshot_segment_uuid(snap_uuid, 0);
            let uri = snapshot_segment_uri(
                base_uri,
                catalog_uuid,
                branch_uuid,
                snap_uuid,
                &seg_uuid,
                storage_format_text,
            );
            segments_to_write.push(SnapshotSegmentWriteStep {
                seg_uuid_str: seg_uuid.to_string(),
                uri,
                partition_value: None,
                chunk_idx: 0,
                batch: RecordBatch::new_empty(merged.schema()),
                size_bytes: 0,
            });
        }

        Ok(segments_to_write)
    }

    /// `row_uuid, pk (nullable), v` — nullable partition column so the
    /// NULL-partition-value case is expressible.
    fn eq_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("pk", DataType::Utf8, true),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn rows_batch(rows: &[(&str, Option<&str>, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            eq_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0.to_string()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|r| r.1.map(str::to_string))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn empty_batch() -> RecordBatch {
        rows_batch(&[])
    }

    fn rows_of(batch: &RecordBatch) -> Vec<Vec<String>> {
        (0..batch.num_rows())
            .map(|r| {
                batch
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, r).unwrap())
                    .collect()
            })
            .collect()
    }

    /// Reference oracle invocation with the fixture identifiers in
    /// exactly one place. Returns `build_segments_to_write`'s output
    /// verbatim — placeholder included.
    fn oracle_steps(
        merged: &RecordBatch,
        partition_keys: &[String],
        sort_keys: &[String],
        max: i64,
    ) -> Vec<SnapshotSegmentWriteStep> {
        build_segments_to_write(
            merged,
            partition_keys,
            sort_keys,
            &Uuid::from_u128(11),
            &Uuid::from_u128(12),
            &Uuid::from_u128(13),
            max,
            "memory://rt2-oracle",
            "parquet",
        )
        .unwrap()
    }

    /// Per-partition oracle row content: today's whole-table planning.
    /// Skips the zero-row placeholder step (orchestration concern,
    /// asserted separately).
    fn oracle_partitions(
        merged: &RecordBatch,
        partition_keys: &[String],
        sort_keys: &[String],
        max: i64,
    ) -> BTreeMap<Option<String>, Vec<Vec<String>>> {
        let steps = oracle_steps(merged, partition_keys, sort_keys, max);
        let mut out: BTreeMap<Option<String>, Vec<Vec<String>>> = BTreeMap::new();
        for step in &steps {
            if step.batch.num_rows() == 0 {
                continue;
            }
            out.entry(step.partition_value.clone())
                .or_default()
                .extend(rows_of(&step.batch));
        }
        out
    }

    /// The oracle's cross-partition emission order — the run of distinct
    /// `partition_value`s (consecutive-deduped) the whole-table oracle
    /// emits, skipping the zero-row placeholder. CHA-459: this is typed
    /// partition order (both paths share `partition_record_batch`), so the
    /// streaming packer must match it exactly — the type-correct successor
    /// to the old stringified-label sort.
    fn oracle_label_order(
        merged: &RecordBatch,
        partition_keys: &[String],
        sort_keys: &[String],
        max: i64,
    ) -> Vec<Option<String>> {
        let mut order: Vec<Option<String>> = Vec::new();
        for step in oracle_steps(merged, partition_keys, sort_keys, max) {
            if step.batch.num_rows() == 0 {
                continue;
            }
            if order.last() != Some(&step.partition_value) {
                order.push(step.partition_value.clone());
            }
        }
        order
    }

    async fn run_pack(
        delta: &RecordBatch,
        prior: Vec<RecordBatch>,
        partition_keys: &[String],
        sort_keys: &[String],
        max: i64,
    ) -> Vec<SnapshotFileStep> {
        let delta_groups = partition_record_batch(delta, partition_keys).unwrap();
        let input =
            futures_util::stream::iter(prior.into_iter().map(Ok::<_, penca_merge::MergeError>))
                .boxed();
        let mut stream = pack_merged_partition_stream(
            delta_groups,
            input,
            PartitionOrdering::new(&delta.schema(), partition_keys).unwrap(),
            sort_keys.to_vec(),
            std::collections::BTreeMap::new(),
            test_packer(max),
        );
        let mut steps = Vec::new();
        while let Some(s) = stream.next().await {
            match s.unwrap() {
                PackStep::File(file) => steps.push(file),
                PackStep::Carried(_) => panic!("content-equivalence fixtures carry nothing"),
            }
        }
        steps
    }

    /// Walk the emitted file steps asserting every packing invariant,
    /// and return per-partition row content (in emission order).
    /// `expected_label_order` is the oracle's typed partition emission
    /// order (CHA-459): the streaming packer must flush partitions in that
    /// exact order.
    fn checked_new_path_partitions(
        steps: &[SnapshotFileStep],
        max: i64,
        expected_label_order: &[Option<String>],
    ) -> BTreeMap<Option<String>, Vec<Vec<String>>> {
        let mut out: BTreeMap<Option<String>, Vec<Vec<String>>> = BTreeMap::new();
        let mut expected_chunk_idx: u32 = 0;
        let mut label_order: Vec<Option<String>> = Vec::new();
        for step in steps {
            let mut offset: i64 = 0;
            for row in &step.segment_rows {
                assert_eq!(
                    row.offset, offset,
                    "offsets ascending+dense within the file"
                );
                assert_eq!(row.chunk_idx, expected_chunk_idx, "global chunk_idx dense");
                expected_chunk_idx += 1;
                offset += row.length;

                let slice = step
                    .file_batch
                    .slice(row.offset as usize, row.length as usize);
                assert_eq!(
                    row.size_bytes,
                    batch_in_memory_bytes(&slice).unwrap(),
                    "size_bytes is the partition slice's footprint"
                );
                assert_eq!(
                    row.statistics,
                    penca_dl::stats::compute_segment_statistics(&slice),
                    "statistics computed over the partition slice only (pruning stays partition-tight)"
                );

                out.entry(row.partition_value.clone())
                    .or_default()
                    .extend(rows_of(&slice));
                if label_order.last() != Some(&row.partition_value) {
                    label_order.push(row.partition_value.clone());
                }
            }
            assert_eq!(
                offset,
                step.file_batch.num_rows() as i64,
                "segment rows tile the file exactly"
            );
            if step.segment_rows.len() > 1 {
                assert!(
                    batch_in_memory_bytes(&step.file_batch).unwrap() <= max,
                    "multi-partition packed file must respect max_segment_bytes"
                );
            }
        }

        assert_eq!(
            label_order, expected_label_order,
            "partitions flush in the oracle's typed partition order"
        );
        out
    }

    /// merged = prior ++ delta (the logical whole-table view the oracle
    /// eats); both paths must agree per partition. Positional when sort
    /// keys are set; multiset otherwise.
    async fn assert_equivalent(
        delta: &RecordBatch,
        prior: Vec<RecordBatch>,
        partition_keys: &[String],
        sort_keys: &[String],
        max: i64,
    ) -> Vec<SnapshotFileStep> {
        let mut all = prior.clone();
        all.push(delta.clone());
        let merged = concat_batches(&delta.schema(), &all).unwrap();
        let mut oracle = oracle_partitions(&merged, partition_keys, sort_keys, max);
        let expected_order = oracle_label_order(&merged, partition_keys, sort_keys, max);
        let steps = run_pack(delta, prior, partition_keys, sort_keys, max).await;
        let mut new = checked_new_path_partitions(&steps, max, &expected_order);

        if sort_keys.is_empty() {
            for rows in oracle.values_mut() {
                rows.sort();
            }
            for rows in new.values_mut() {
                rows.sort();
            }
        }
        assert_eq!(
            oracle, new,
            "per-partition row content must match the oracle"
        );
        steps
    }

    const PK: &[&str] = &["pk"];
    fn keys(k: &[&str]) -> Vec<String> {
        k.iter().map(|s| s.to_string()).collect()
    }

    /// Unpartitioned table: one None-labeled group, content positional
    /// under the sort key.
    #[tokio::test]
    async fn unpartitioned_single_group() {
        let delta = rows_batch(&[
            ("r1", Some("x"), 3),
            ("r2", Some("y"), 1),
            ("r3", Some("z"), 2),
        ]);
        assert_equivalent(&delta, vec![], &[], &keys(&["v"]), 1 << 20).await;
    }

    /// String partition values incl. a quote-bearing label and a NULL
    /// partition value — label identity must match the oracle exactly.
    #[tokio::test]
    async fn string_keys_quote_and_null_partition_values() {
        let delta = rows_batch(&[
            ("r1", Some("O'Brien"), 1),
            ("r2", None, 2),
            ("r3", Some("a"), 3),
            ("r4", Some("O'Brien"), 4),
            ("r5", None, 5),
        ]);
        assert_equivalent(&delta, vec![], &keys(PK), &keys(&["v"]), 1 << 20).await;
    }

    /// Composite partition key → JSON-array labels.
    #[tokio::test]
    async fn composite_key_json_labels() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("pk", DataType::Utf8, true),
            Field::new("pk2", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2", "r3", "r4"])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("a"),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 1])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        assert_equivalent(
            &batch,
            vec![],
            &keys(&["pk", "pk2"]),
            &keys(&["v"]),
            1 << 20,
        )
        .await;
    }

    /// CHA-459: a non-string (`Int64`) partition key. The streaming packer
    /// and the whole-table oracle must agree per partition AND emit in
    /// typed order (`[2, 9, 10, 100]`, not the lexicographic
    /// `[10, 100, 2, 9]`) — the content-equivalence proof that the typed
    /// re-keying routes rows correctly, complementing RT2's emit-order pin.
    #[tokio::test]
    async fn int_partition_key_typed_order_equivalent() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("pk", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2", "r3", "r4", "r5"])),
                Arc::new(Int64Array::from(vec![10, 2, 100, 9, 2])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        assert_equivalent(&batch, vec![], &keys(&["pk"]), &keys(&["v"]), 1 << 20).await;
    }

    /// Delta rows merge into an existing prior partition: combined rows,
    /// clustering-sorted, positionally equal to the oracle over the
    /// same logical view.
    #[tokio::test]
    async fn delta_merges_into_prior_partition() {
        let prior = vec![
            rows_batch(&[("a-p0", Some("a"), 0), ("a-p1", Some("a"), 2)]),
            rows_batch(&[("b-p0", Some("b"), 5)]),
        ];
        let delta = rows_batch(&[("a-d0", Some("a"), 1), ("a-d1", Some("a"), 3)]);
        assert_equivalent(&delta, prior, &keys(PK), &keys(&["v"]), 1 << 20).await;
    }

    /// CHA-406 matrix extension: one partition's prior rows span THREE
    /// stream batches (even sort keys, clustering-sorted within and
    /// across batches — the ByPlan contract), with delta rows
    /// interleaving odds AND colliding with prior on keys 8 and 16 —
    /// key 16 being the last row of the final prior batch, so the tie
    /// sits at a batch boundary (the prior-before-delta merge must hold
    /// the delta's 16 back past the prior batch end). Positional
    /// equality vs the oracle pins the prior-before-delta tie order (the
    /// oracle's stable concat-then-sort). Green before and after the I2
    /// streaming merge — this is its regression pin.
    #[tokio::test]
    async fn multi_segment_prior_interleaved_delta_matches_oracle() {
        let prior = vec![
            rows_batch(&[
                ("a-p0", Some("a"), 0),
                ("a-p2", Some("a"), 2),
                ("a-p4", Some("a"), 4),
            ]),
            rows_batch(&[
                ("a-p6", Some("a"), 6),
                ("a-p8", Some("a"), 8),
                ("a-p10", Some("a"), 10),
            ]),
            rows_batch(&[
                ("a-p12", Some("a"), 12),
                ("a-p14", Some("a"), 14),
                ("a-p16", Some("a"), 16),
            ]),
        ];
        let delta = rows_batch(&[
            ("a-d1", Some("a"), 1),
            ("a-d5", Some("a"), 5),
            ("a-d8", Some("a"), 8),
            ("a-d11", Some("a"), 11),
            ("a-d16", Some("a"), 16),
        ]);
        let steps = assert_equivalent(&delta, prior, &keys(PK), &keys(&["v"]), 1 << 20).await;
        // The tie order, pinned explicitly on top of the positional
        // oracle match: at equal sort keys the prior row precedes the
        // delta row.
        let all: Vec<Vec<String>> = steps.iter().flat_map(|s| rows_of(&s.file_batch)).collect();
        let pos = |id: &str| {
            all.iter()
                .position(|r| r[0] == id)
                .unwrap_or_else(|| panic!("row {id} missing from output"))
        };
        assert!(pos("a-p8") < pos("a-d8"), "prior before delta at tie key 8");
        assert!(
            pos("a-p16") < pos("a-d16"),
            "prior before delta at boundary tie key 16"
        );
    }

    /// A delta-only partition interleaves between prior partitions in
    /// label order (a < b < c).
    #[tokio::test]
    async fn delta_only_partition_interleaves_in_label_order() {
        let prior = vec![
            rows_batch(&[("a-p0", Some("a"), 1)]),
            rows_batch(&[("c-p0", Some("c"), 2)]),
        ];
        let delta = rows_batch(&[("b-d0", Some("b"), 3)]);
        let steps = assert_equivalent(&delta, prior, &keys(PK), &keys(&["v"]), 1 << 20).await;
        let labels: Vec<Option<String>> = steps
            .iter()
            .flat_map(|s| s.segment_rows.iter().map(|r| r.partition_value.clone()))
            .collect();
        assert_eq!(
            labels,
            vec![Some("a".into()), Some("b".into()), Some("c".into())],
            "delta-only partition lands between prior labels"
        );
    }

    /// Partition-key move, modeled post-exclusion: the moved row's new
    /// version arrives in the delta under partition `a`; its stale copy
    /// in prior partition `b` was already dropped by the global
    /// exclusion anti-join upstream (penca-merge), so the stream never
    /// carries it. The row must appear exactly once, in `a`.
    #[tokio::test]
    async fn moved_row_appears_once_in_new_partition() {
        let prior = vec![rows_batch(&[("b-keep", Some("b"), 1)])];
        let delta = rows_batch(&[("moved", Some("a"), 2)]);
        let steps = assert_equivalent(&delta, prior, &keys(PK), &keys(&["v"]), 1 << 20).await;
        let all_rows: Vec<Vec<String>> =
            steps.iter().flat_map(|s| rows_of(&s.file_batch)).collect();
        let moved_count = all_rows.iter().filter(|r| r[0] == "moved").count();
        assert_eq!(
            moved_count, 1,
            "moved row appears exactly once (in its new partition)"
        );
    }

    /// One partition larger than the cap splits via chunk_row_ranges —
    /// chunk boundaries must match the oracle's exactly (same packer,
    /// same sorted input).
    #[tokio::test]
    async fn oversized_partition_chunks_match_oracle_boundaries() {
        let rows: Vec<(String, Option<&str>, i64)> =
            (0..12).map(|i| (format!("a-r{i}"), Some("a"), i)).collect();
        let rows_ref: Vec<(&str, Option<&str>, i64)> =
            rows.iter().map(|(u, p, v)| (u.as_str(), *p, *v)).collect();
        let big = rows_batch(&rows_ref);
        let max = batch_in_memory_bytes(&big.slice(0, 4)).unwrap();

        let oracle_lengths: Vec<i64> = oracle_steps(&big, &keys(PK), &keys(&["v"]), max)
            .iter()
            .map(|s| s.batch.num_rows() as i64)
            .collect();

        let steps = assert_equivalent(&big, vec![], &keys(PK), &keys(&["v"]), max).await;
        let new_lengths: Vec<i64> = steps
            .iter()
            .flat_map(|s| s.segment_rows.iter().map(|r| r.length))
            .collect();
        assert_eq!(
            new_lengths, oracle_lengths,
            "oversized chunk boundaries match the oracle"
        );
    }

    /// Many small partitions pack into ONE shared-uri file — the layout
    /// change the oracle cannot produce; content still matches it.
    #[tokio::test]
    async fn small_partitions_share_one_file() {
        let prior = vec![
            rows_batch(&[("a-r0", Some("a"), 1), ("a-r1", Some("a"), 2)]),
            rows_batch(&[("b-r0", Some("b"), 3)]),
            rows_batch(&[("c-r0", Some("c"), 4)]),
        ];
        let steps =
            assert_equivalent(&empty_batch(), prior, &keys(PK), &keys(&["v"]), 1 << 20).await;
        // One step-level uri over three segment rows IS the shared-file
        // layout - steps.len() + segment_rows.len() pin it fully.
        assert_eq!(steps.len(), 1, "everything fits one packed file");
        assert_eq!(
            steps[0].segment_rows.len(),
            3,
            "one segment row per partition"
        );
    }

    /// Empty inputs emit no file steps — the zero-row placeholder
    /// (CHA-228 watermark commit) is the orchestration's job, pinned by
    /// the integration suite, not the packer's.
    #[tokio::test]
    async fn empty_inputs_emit_no_steps() {
        let steps = run_pack(&empty_batch(), vec![], &keys(PK), &keys(&["v"]), 1 << 20).await;
        assert!(steps.is_empty());
    }
}

#[cfg(test)]
mod effective_clustering_keys_tests {
    use super::effective_clustering_keys;

    fn keys(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Empty clustering keys resolve to the primary keys — the common
    /// SQL-DDL case (CREATE TABLE never sets clustering keys; PKs are
    /// mandatory), so this is the default sort for most tables.
    #[test]
    fn empty_clustering_defaults_to_primary_keys() {
        assert_eq!(
            effective_clustering_keys(keys(&[]), keys(&["pk_a", "pk_b"])),
            keys(&["pk_a", "pk_b"])
        );
    }

    /// Declared clustering keys win; primary keys are not appended.
    #[test]
    fn declared_clustering_keys_win() {
        assert_eq!(
            effective_clustering_keys(keys(&["region"]), keys(&["pk_a"])),
            keys(&["region"])
        );
    }

    /// Both empty (non-SQL writers may omit PKs) → no sort, matching
    /// `sort_record_batch_by_keys`'s passthrough.
    #[test]
    fn both_empty_means_unsorted() {
        assert_eq!(
            effective_clustering_keys(keys(&[]), keys(&[])),
            Vec::<String>::new()
        );
    }
}

/// CHA-448: the pure upsert-identity collector behind the non-subset
/// reverse-lookup. The async sidecar-reading helpers (`delete_row_uuids`,
/// `reverse_lookup_attributed_labels`) are covered end-to-end by the
/// `test_non_subset_*` integration tests; this pins the one pure helper whose
/// miss path (a delta batch without a `row_uuid` column) is Penca-owned.
#[cfg(test)]
mod touched_row_uuids_tests {
    use super::rt_fixtures::part_batch;
    use super::touched_row_uuids_from_delta;

    #[test]
    fn collects_row_uuids_across_delta_groups() {
        let groups = vec![
            (Some("a".to_string()), part_batch("a", 2)),
            (Some("b".to_string()), part_batch("b", 1)),
        ];
        let mut got: Vec<String> = touched_row_uuids_from_delta(&groups)
            .unwrap()
            .into_iter()
            .collect();
        got.sort();
        assert_eq!(got, vec!["a-r0", "a-r1", "b-r0"]);
    }

    #[test]
    fn missing_row_uuid_column_errors() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        // A delta batch without a row_uuid column must fail fast, not silently
        // contribute no identities (which would under-attribute the touched set).
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        assert!(touched_row_uuids_from_delta(&[(None, batch)]).is_err());
    }
}

/// CHA-432 durable-rung decision matrix. `decide_durable` is the pure kernel
/// behind sticky durable assignment: a snapshot becomes a durable retention
/// rung iff there is no prior durable rung, or density is unset (every snapshot
/// durable), or it is at least `snapshot_density_seconds` past the last durable.
/// Decided once at creation, so it must be exhaustively pinned — a wrong accept
/// over-retains, a wrong reject leaves the floor too coarse for the window.
#[cfg(test)]
mod decide_durable_tests {
    use super::decide_durable;

    const SEC: i64 = 1_000_000; // micros per second

    #[test]
    fn no_prior_durable_is_always_durable() {
        // First rung: durable regardless of density or timestamp.
        assert!(decide_durable(None, 0, None));
        assert!(decide_durable(None, 5 * SEC, Some(600)));
    }

    #[test]
    fn unset_density_makes_every_snapshot_durable() {
        // Density unset ⇒ keep-all-in-window (CHA-55): every snapshot a rung,
        // even one micro after the last durable.
        assert!(decide_durable(Some(1_000 * SEC), 1_000 * SEC + 1, None));
    }

    #[test]
    fn gap_at_or_past_density_is_durable() {
        let t0 = 1_000 * SEC;
        // Boundary: gap exactly == density (>=) → durable.
        assert!(decide_durable(Some(t0), t0 + 600 * SEC, Some(600)));
        // Strictly past density → durable.
        assert!(decide_durable(Some(t0), t0 + 600 * SEC + 1, Some(600)));
    }

    #[test]
    fn gap_below_density_is_not_durable() {
        let t0 = 1_000 * SEC;
        // Just under one density window → not a new rung.
        assert!(!decide_durable(Some(t0), t0 + 600 * SEC - 1, Some(600)));
        // No advance at all → not a new rung.
        assert!(!decide_durable(Some(t0), t0, Some(600)));
    }
}

/// CHA-406 / CHA-448 eligibility gate matrix — each reject branch plus the
/// accept cases, including the NULL (`None`) vs empty (`Some(vec![])`)
/// recorded-key distinction the gate hinges on and the CHA-448 non-subset
/// with/without-sidecar split. The gate is correctness-critical (a wrong accept
/// risks data loss; a wrong reject is a silent perf regression), and it is a
/// cheap pure function, so it gets a spelled-out matrix.
#[cfg(test)]
mod carry_forward_keys_tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use penca_core::SnapshotSegment;

    use super::{
        PartitionOrderKey, PartitionOrdering, carry_forward_keys, split_prior_segments_by_touch,
    };

    fn keys(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn user_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    /// The typed ordering over the single `name` partition key — valid for
    /// every test here (reject paths return before it is consumed).
    fn ordering() -> PartitionOrdering {
        PartitionOrdering::new(&user_schema(), &keys(&["name"])).unwrap()
    }

    /// A typed [`PartitionOrderKey`] for a single Utf8 `name` label.
    fn key_for(name: &str) -> PartitionOrderKey {
        let arr: ArrayRef = Arc::new(StringArray::from(vec![name]));
        ordering()
            .order_key_from_key_arrays(&[arr], Some(name.to_string()))
            .unwrap()
    }

    /// Project a key vector (or option thereof) to its identity labels for
    /// assertions — `PartitionOrderKey` is intentionally not `Debug`.
    fn labels_of(keys: Option<Vec<PartitionOrderKey>>) -> Option<Vec<Option<String>>> {
        keys.map(|ks| ks.iter().map(|k| k.label().clone()).collect())
    }

    /// A prior segment whose constant-`name` stats yield the label
    /// `name` (the single-partition writer shape: row_uuid + user cols).
    fn segment_for(name: &str) -> SnapshotSegment {
        let schema = penca_merge::snapshot_read_schema(&user_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["r0"])),
                Arc::new(StringArray::from(vec![name])),
                Arc::new(Int64Array::from(vec![1i64])),
            ],
        )
        .unwrap();
        SnapshotSegment {
            table_snapshot_segment_uuid: format!("seg-{name}"),
            statistics: penca_dl::stats::compute_segment_statistics(&batch),
            ..Default::default()
        }
    }

    /// A segment with empty stats → label underivable.
    fn segment_no_stats() -> SnapshotSegment {
        SnapshotSegment {
            table_snapshot_segment_uuid: "seg-x".to_string(),
            statistics: Vec::new(),
            ..Default::default()
        }
    }

    /// The eligible case: recorded keys equal current, partition ⊆ PK,
    /// labels all derivable → Some(per-segment labels).
    #[test]
    fn accept_returns_labels() {
        let segs = vec![segment_for("a"), segment_for("b")];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            Some(&keys(&["name"])),
            Some(&keys(&["name"])),
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert_eq!(
            labels_of(out),
            Some(vec![Some("a".to_string()), Some("b".to_string())])
        );
    }

    #[test]
    fn reject_no_prior_segments() {
        let out = carry_forward_keys(
            &ordering(),
            &[],
            Some(&keys(&["name"])),
            Some(&keys(&["name"])),
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn reject_unpartitioned() {
        let segs = vec![segment_for("a")];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            Some(&keys(&[])),
            Some(&keys(&["name"])),
            &keys(&[]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    /// SQL NULL recorded keys (pre-CHA-404 parent) decode to `None` —
    /// distinct from `Some(vec![])` — and must reject.
    #[test]
    fn reject_null_recorded_keys() {
        let segs = vec![segment_for("a")];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            None,
            None,
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    /// `Some(vec![])` (known-no-keys `{}`) is NOT equal to a non-empty
    /// current partition_keys → reject (the empty-vs-NULL distinction
    /// resolves to the same reject here, but via the equality check, not
    /// the NULL check).
    #[test]
    fn reject_empty_recorded_keys_against_partitioned_current() {
        let segs = vec![segment_for("a")];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            Some(&keys(&[])),
            Some(&keys(&[])),
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn reject_changed_clustering_keys() {
        let segs = vec![segment_for("a")];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            Some(&keys(&["name"])),
            Some(&keys(&["value"])), // recorded clustering differs
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    /// CHA-448 v2: the gate is PK-agnostic, so partition ⊄ PK is eligible on
    /// the same terms as the subset case — the subset-vs-reverse-lookup choice
    /// is `snapshot_locked`'s, not the gate's. The label is derived from the
    /// `value` partition column, so the ordering must match it.
    #[test]
    fn accept_partition_not_subset_of_pk() {
        let value_ordering = PartitionOrdering::new(&user_schema(), &keys(&["value"])).unwrap();
        let segs = vec![segment_for("a")];
        let out = carry_forward_keys(
            &value_ordering,
            &segs,
            Some(&keys(&["value"])),
            Some(&keys(&["name"])),
            &keys(&["value"]), // partition key not in PK
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(
            out.is_some(),
            "non-subset partition is eligible; attribution strategy is chosen later"
        );
    }

    #[test]
    fn reject_underivable_label() {
        let segs = vec![segment_for("a"), segment_no_stats()];
        let out = carry_forward_keys(
            &ordering(),
            &segs,
            Some(&keys(&["name"])),
            Some(&keys(&["name"])),
            &keys(&["name"]),
            &keys(&["name"]),
            &user_schema(),
        )
        .unwrap();
        assert!(out.is_none());
    }

    /// The pure touched/carried split: touched labels route their
    /// segment to the rewrite list (order preserved); untouched labels
    /// accumulate their prior uuids into the carried map.
    #[test]
    fn split_routes_touched_to_rewrite_and_rest_to_carried() {
        use std::collections::{BTreeMap, HashSet};

        let seg = |uuid: &str| SnapshotSegment {
            table_snapshot_segment_uuid: uuid.to_string(),
            ..Default::default()
        };
        // Prior: a (touched), b (carried), c (touched). b spans two
        // prior segments to prove they accumulate in order.
        let prior_keys = vec![key_for("a"), key_for("b"), key_for("b"), key_for("c")];
        let segments = vec![seg("a0"), seg("b0"), seg("b1"), seg("c0")];
        let touched: HashSet<Option<String>> = [Some("a".to_string()), Some("c".to_string())]
            .into_iter()
            .collect();

        let (touched_segments, carried) =
            split_prior_segments_by_touch(prior_keys, segments, &touched);

        let touched_uuids: Vec<&str> = touched_segments
            .iter()
            .map(|s| s.table_snapshot_segment_uuid.as_str())
            .collect();
        assert_eq!(
            touched_uuids,
            vec!["a0", "c0"],
            "touched segments go to the rewrite list in order"
        );
        // PartitionOrderKey is not Debug; project the carried map to labels.
        let carried_by_label: BTreeMap<Option<String>, Vec<String>> = carried
            .into_iter()
            .map(|(key, uuids)| (key.label().clone(), uuids))
            .collect();
        assert_eq!(
            carried_by_label,
            BTreeMap::from([(
                Some("b".to_string()),
                vec!["b0".to_string(), "b1".to_string()]
            )]),
            "untouched label b accumulates both prior uuids in order"
        );
    }
}

/// CHA-459 Part A: the snapshot writer's cross-partition emit order — the
/// order baked into `chunk_idx`, which `read_snapshot_segments_for_table`
/// replays via `ORDER BY seg.chunk_idx` — must be *typed* partition-column
/// order for a non-string key. `pack_merged_partition_stream` over an
/// `Int64` key `{2,9,10,100}` must emit `[2,9,10,100]`, not the
/// lexicographic `[10,100,2,9]` the string-`BTreeMap` `drain_below`
/// produces today. Delta-only (empty prior, empty carried) isolates the
/// drain order as the deterministic red.
#[cfg(test)]
mod typed_partition_emit_order_red_test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use futures_util::StreamExt;

    use crate::lifecycle::batch_util::{PartitionOrdering, partition_record_batch};
    use crate::lifecycle::packer::{PackStep, pack_merged_partition_stream};

    use super::rt_fixtures::test_packer;

    fn int_pk_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("pk", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// Drive the pack stream with a delta-only `Int64`-partitioned batch
    /// (empty prior, empty carried) and return the emitted partition
    /// labels in `chunk_idx` order.
    async fn emitted_partition_order(pks: Vec<i64>) -> Vec<String> {
        let n = pks.len();
        let row_uuids: Vec<String> = (0..n).map(|i| format!("r{i}")).collect();
        let vs: Vec<i64> = (0..n as i64).collect();
        let delta = RecordBatch::try_new(
            int_pk_schema(),
            vec![
                Arc::new(StringArray::from(row_uuids)),
                Arc::new(Int64Array::from(pks)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap();
        let delta_groups = partition_record_batch(&delta, &["pk".to_string()]).unwrap();
        let prior =
            futures_util::stream::iter(Vec::<Result<RecordBatch, penca_merge::MergeError>>::new())
                .boxed();
        let mut stream = pack_merged_partition_stream(
            delta_groups,
            prior,
            PartitionOrdering::new(&int_pk_schema(), &["pk".to_string()]).unwrap(),
            Vec::new(),
            std::collections::BTreeMap::new(),
            test_packer(1 << 20),
        );
        let mut labels = Vec::new();
        while let Some(step) = stream.next().await {
            if let PackStep::File(file) = step.unwrap() {
                for row in &file.segment_rows {
                    labels.push(row.partition_value.clone().unwrap());
                }
            }
        }
        labels
    }

    #[tokio::test]
    async fn writer_emits_int_partitions_in_typed_order() {
        let order = emitted_partition_order(vec![10, 2, 100, 9, 2]).await;
        assert_eq!(
            order,
            vec!["2", "9", "10", "100"],
            "snapshot writer must emit Int partitions in typed-ascending \
             chunk_idx order, not lexicographic (\"10\" < \"2\" < \"9\")"
        );
    }
}
