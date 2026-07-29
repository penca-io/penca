//! Per-scope compaction algorithm for persist segments.
//!
//! [`compact_one_scope`] is the active+sealed merge loop used by
//! `LifecycleManager::compact_persist_segments`; [`PersistScope`]
//! (per-`LogKind`: delete-log vs upsert-log) carries the per-scope
//! state. Snapshot segments are immutable and never compact (ADR 0024).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::record_batch::RecordBatch;
use futures_util::TryStreamExt;
use penca_core::naming::persist_segment_uri;
use penca_core::{Format, LogKind, PersistSegment};
use penca_db::driver::pg::{PgDriver, PgTransactionDriver};
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_format::writer::FormatWriter;
use penca_merge::{ReadSnapshot, cold_delete_schema, cold_upsert_schema};
use penca_storage_cold::ColdStorageClient;
use penca_storage_meta::LifecycleManager;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::lifecycle::chunker::batch_in_memory_bytes;
use crate::lifecycle::compact_plan::{CompactInputMeta, plan_wave};

/// Persist-side scope: per-table per-`log_kind` unsealed-segment compaction.
pub(super) struct PersistScope<'a> {
    pub log_kind: LogKind,
    pub snapshot: &'a ReadSnapshot,
    pub query_manager: &'a crate::query::QueryManager,
}

impl<'a> PersistScope<'a> {
    const SEGMENT_UUID_COLUMN: &'static str = "table_persist_segment_uuid";
    const PARENT_UUID_COLUMN: &'static str = "table_persist_uuid";

    async fn enumerate_unsealed_segments(
        &self,
        tx: &PgTransactionDriver,
        catalog_str: &str,
        branch_str: &str,
        table_str: &str,
        min_at_micros: Option<i64>,
        max_at_micros: Option<i64>,
    ) -> Result<Vec<PgRow>, ApiError> {
        let rows = LifecycleManager::enumerate_unsealed_persist_segments_for_scope(
            tx,
            catalog_str,
            branch_str,
            table_str,
            self.log_kind,
            min_at_micros,
            max_at_micros,
            true,
        )
        .await?;
        Ok(rows)
    }

    async fn segment_schema<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
    ) -> Result<Option<SchemaRef>, ApiError> {
        match self.log_kind {
            LogKind::UpsertLog => {
                let arrow_schema_bytes = self
                    .query_manager
                    .get_table_arrow_schema_by_branch(
                        pool,
                        dl_driver,
                        catalog_uuid,
                        branch_uuid,
                        table_uuid,
                        self.snapshot,
                    )
                    .await?;
                arrow_schema_bytes
                    .map(|b| {
                        let user_schema: SchemaRef =
                            Arc::new(try_schema_from_ipc_buffer(&b).map_err(ApiError::Arrow)?);
                        Ok::<_, ApiError>(cold_upsert_schema(&user_schema))
                    })
                    .transpose()
            }
            // The cold delete schema carries PK columns — compact must read
            // the same shape persist wrote.
            LogKind::DeleteLog => {
                let meta = self
                    .query_manager
                    .get_table_metadata_by_branch(
                        pool,
                        dl_driver,
                        catalog_uuid,
                        branch_uuid,
                        table_uuid,
                        self.snapshot,
                    )
                    .await?;
                meta.map(|(bytes, primary_keys)| {
                    let user_schema: SchemaRef =
                        Arc::new(try_schema_from_ipc_buffer(&bytes).map_err(ApiError::Arrow)?);
                    cold_delete_schema(&user_schema, &primary_keys).map_err(ApiError::from)
                })
                .transpose()
            }
        }
    }

    fn segment_from_row(row: &PgRow) -> Result<PersistSegment, ApiError> {
        let segment_uuid: Uuid = row.get(Self::SEGMENT_UUID_COLUMN);
        let uri: String = row.get("object_uri");
        let format: Format = row.get::<String, _>("format").parse().map_err(|e| {
            ApiError::from(sqlx::Error::Protocol(format!(
                "table_persist_segment_metadata.format decode failed: {e}"
            )))
        })?;
        let row_count: i64 = row.try_get("row_count").unwrap_or(0);
        let offset: Option<i64> = row.get("offset");
        let length: Option<i64> = row.get("length");
        Ok(PersistSegment {
            segment_uuid: segment_uuid.to_string(),
            uri,
            format,
            row_count,
            size_bytes: 0,
            metadata_json: String::new(),
            statistics: Vec::new(),
            offset,
            length,
        })
    }
}

/// Per-scope active+sealed compaction algorithm. Returns the new merged URI,
/// or `None` when the wave produced no new file.
#[allow(clippy::too_many_arguments)]
pub(super) async fn compact_one_scope<L, R, W>(
    scope: &PersistScope<'_>,
    pool: &PgDriver,
    dl_driver: &L,
    readers: &HashMap<i32, R>,
    writer: &W,
    catalog_uuid: Uuid,
    branch_uuid: Uuid,
    table_uuid: Uuid,
    min_at_micros: Option<i64>,
    max_at_micros: Option<i64>,
    max_segment_bytes: i64,
    base_uri: &str,
    storage_format: Format,
) -> Result<Option<String>, ApiError>
where
    L: DlDriver + ?Sized,
    R: FormatReader,
    W: FormatWriter,
{
    let catalog_str = catalog_uuid.to_string();
    let branch_str = branch_uuid.to_string();
    let table_str = table_uuid.to_string();
    let storage_format_text = storage_format.extension();

    let tx = pool
        .begin()
        .await
        .map_err(|e| ApiError::Metadata(e.into()))?;

    let rows = scope
        .enumerate_unsealed_segments(
            &tx,
            &catalog_str,
            &branch_str,
            &table_str,
            min_at_micros,
            max_at_micros,
        )
        .await?;

    let plan = match plan_wave(&rows, max_segment_bytes, |r| {
        r.get::<String, _>("object_uri")
    }) {
        Some(p) => p,
        None => return Ok(None),
    };

    // Seal-only wave. The sealed rows keep pointing at their cold files, so
    // there is nothing to enqueue for deletion.
    if plan.input_indices.is_empty() {
        let seal_uuid_strs: Vec<String> = plan
            .seal_indices
            .iter()
            .map(|&i| {
                let u: Uuid = rows[i].get(PersistScope::SEGMENT_UUID_COLUMN);
                u.to_string()
            })
            .collect();
        let seal_uuid_refs: Vec<&str> = seal_uuid_strs.iter().map(String::as_str).collect();
        LifecycleManager::seal_table_persist_segments_by_uuids(
            &tx,
            &catalog_str,
            &branch_str,
            &seal_uuid_refs,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| ApiError::Metadata(e.into()))?;
        return Ok(None);
    }

    let segment_schema: SchemaRef = match scope
        .segment_schema(pool, dl_driver, &catalog_uuid, &branch_uuid, &table_uuid)
        .await?
    {
        Some(s) => s,
        None => return Ok(None),
    };

    let mut segments: Vec<PersistSegment> = Vec::with_capacity(plan.input_indices.len());
    let mut input_meta: Vec<CompactInputMeta> = Vec::with_capacity(plan.input_indices.len());
    for &i in &plan.input_indices {
        let row = &rows[i];
        let segment_uuid: Uuid = row.get(PersistScope::SEGMENT_UUID_COLUMN);
        let uri: String = row.get("object_uri");
        let row_count: i64 = row.try_get("row_count").unwrap_or(0);
        segments.push(PersistScope::segment_from_row(row)?);
        input_meta.push(CompactInputMeta {
            segment_uuid: segment_uuid.to_string(),
            row_count,
            old_uri: uri,
        });
    }

    let batches: Vec<RecordBatch> =
        ColdStorageClient::read_persist_segments(readers, &segments, &segment_schema, None)
            .try_collect()
            .await
            .map_err(ApiError::ColdStorage)?;
    let merged = concat_batches(&segment_schema, &batches).map_err(ApiError::Arrow)?;
    // Computed once and replayed onto every input row via the repoint below:
    // whole-merged-file stats apply uniformly, with no per-slice sharpening.
    let merged_stats = penca_dl::stats::compute_segment_statistics(&merged);

    let merged_file_uuid = Uuid::new_v4();
    let parent_uuid: Uuid = rows[plan.input_indices[0]].get(PersistScope::PARENT_UUID_COLUMN);
    let merged_uri = persist_segment_uri(
        base_uri,
        &catalog_uuid,
        &branch_uuid,
        &parent_uuid,
        &merged_file_uuid,
        storage_format_text,
    );

    // Phase 1: orphan-tracking INSERT (auto-commit) → write file. The orphan
    // row's `table_uuid` is always the user table_uuid.
    LifecycleManager::insert_compact_segment(
        pool,
        &catalog_str,
        &branch_str,
        &table_str,
        &merged_uri,
    )
    .await?;

    ColdStorageClient::write_table_persist_segment(writer, &merged_uri, &merged).await?;
    // The re-pointed `size_bytes` MUST be the in-memory Arrow footprint — the
    // unit `compact_plan` folds against — not the on-disk merged-file size.
    // The loop below splits it across inputs proportionally by row_count.
    let merged_size_bytes: i64 = batch_in_memory_bytes(&merged)?;

    // Phase 2, still inside `tx`. A seal-and-start-new wave's prior active is
    // NOT in `input_meta`: those rows stay at the prior URI and only flip
    // `is_sealed`.
    let total_rows: i64 = input_meta.iter().map(|m| m.row_count).sum();
    let mut cumulative: i64 = 0;
    let mut uris_to_defer_delete: HashSet<String> = HashSet::new();
    for meta in &input_meta {
        let proportional_size = if total_rows > 0 {
            merged_size_bytes * meta.row_count / total_rows
        } else {
            0
        };
        LifecycleManager::repoint_table_persist_segment(
            &tx,
            &catalog_str,
            &branch_str,
            &meta.segment_uuid,
            &merged_uri,
            cumulative,
            meta.row_count,
            proportional_size,
            storage_format_text,
            &merged_stats,
            false,
        )
        .await?;
        cumulative += meta.row_count;
        if meta.old_uri != merged_uri {
            // This row no longer references its old file. Reachable only for
            // rows in `input_meta`, so a seal-mode prior active — whose rows
            // stay sealed-and-pointing at their file — is never enqueued.
            uris_to_defer_delete.insert(meta.old_uri.clone());
        }
    }
    if !plan.seal_indices.is_empty() {
        let seal_uuid_strs: Vec<String> = plan
            .seal_indices
            .iter()
            .map(|&i| {
                let u: Uuid = rows[i].get(PersistScope::SEGMENT_UUID_COLUMN);
                u.to_string()
            })
            .collect();
        let seal_uuid_refs: Vec<&str> = seal_uuid_strs.iter().map(String::as_str).collect();
        LifecycleManager::seal_table_persist_segments_by_uuids(
            &tx,
            &catalog_str,
            &branch_str,
            &seal_uuid_refs,
        )
        .await?;
    }
    LifecycleManager::commit_compact_segment(&tx, &catalog_str, &branch_str, &merged_uri).await?;

    // Must happen inside the merge tx (ADR 0019 §"Four-part mechanism" item
    // 3) so the deferred-delete row commits atomically with the URI swap.
    // `sweep_segments` removes the file only once past the grace window, by
    // which time any concurrent plan holding the old URI has finished within
    // `query_timeout` and still found the file.
    if !uris_to_defer_delete.is_empty() {
        let defer_uris: Vec<String> = uris_to_defer_delete.into_iter().collect();
        LifecycleManager::insert_segment_delete_set_rows(
            &tx,
            &catalog_str,
            &branch_str,
            &table_str,
            &defer_uris,
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| ApiError::Metadata(e.into()))?;

    Ok(Some(merged_uri))
}
