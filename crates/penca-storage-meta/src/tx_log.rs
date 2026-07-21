//! CHA-507: cold `tx_log` persist-segment metadata + read helpers.
//!
//! `persist_tx_log` (penca-api) flushes a slim per-branch commit map —
//! `(commit_seq_num, commit_micros, author, comment)` — to cold files and
//! records one `tx_log_persist_segment_metadata` row per file via the same
//! two-phase (insert-uncommitted → write file → commit) durability the
//! persist path uses. Reads (`resolve_fork_watermark`'s cold fallback,
//! `audit_data`'s tx-metadata join) enumerate committed segments and seek the
//! sorted `commit_seq_num` column; [`LifecycleManager::tx_log_persist_watermark`]
//! (the derived `MAX(max_commit_seq_num)`) gates PurgeTxLog so a hot
//! `commit_tx_log` row is never GC'd before its cold copy is durable.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use penca_core::naming;
use penca_db::driver::{DbDriver, SqlValue};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{epoch, parse_uuid, qi};
use crate::{LifecycleManager, Result};

/// The Arrow schema of a cold `tx_log` segment (CHA-507): the slim commit map
/// `persist_tx_log` writes and `resolve_fork_watermark` / `audit_data` read.
/// Single source of truth so the write and read shapes never drift.
pub fn tx_log_arrow_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("commit_seq_num", DataType::Int64, false),
        Field::new("commit_micros", DataType::Int64, false),
        Field::new("author", DataType::Utf8, false),
        Field::new("comment", DataType::Utf8, false),
    ]))
}

/// One committed cold `tx_log` segment as stored: the file location + format
/// plus its commit-order and wall-clock bounds. The penca-api read planner
/// turns `(object_uri, format)` into a readable `PersistSegment` and prunes on
/// the bounds; keeping this a raw row-shape avoids coupling the metadata layer
/// to the read-plan types.
pub struct TxLogSegment {
    pub tx_log_segment_uuid: String,
    pub object_uri: String,
    pub format: String,
    pub row_count: i64,
    pub min_commit_seq_num: i64,
    pub max_commit_seq_num: i64,
    pub min_commit_micros: i64,
    pub max_commit_micros: i64,
}

impl LifecycleManager {
    /// Phase 1: insert an uncommitted (`committed_at_micros` NULL) tx_log
    /// segment row. Idempotent on the deterministic `tx_log_segment_uuid`
    /// (a re-run of the same persist range upserts the location).
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_tx_log_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        tx_log_segment_uuid: &str,
        min_commit_seq_num: i64,
        max_commit_seq_num: i64,
        min_commit_micros: i64,
        max_commit_micros: i64,
        object_uri: &str,
        row_count: i64,
        format_text: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "INSERT INTO {table} \
             (tx_log_segment_uuid, branch_uuid, min_commit_seq_num, max_commit_seq_num, \
              min_commit_micros, max_commit_micros, object_uri, row_count, format) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (branch_uuid, tx_log_segment_uuid) DO UPDATE \
                SET object_uri = EXCLUDED.object_uri, \
                    row_count = EXCLUDED.row_count, \
                    format = EXCLUDED.format, \
                    min_commit_seq_num = EXCLUDED.min_commit_seq_num, \
                    max_commit_seq_num = EXCLUDED.max_commit_seq_num, \
                    min_commit_micros = EXCLUDED.min_commit_micros, \
                    max_commit_micros = EXCLUDED.max_commit_micros",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(tx_log_segment_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::Int64(min_commit_seq_num),
                    SqlValue::Int64(max_commit_seq_num),
                    SqlValue::Int64(min_commit_micros),
                    SqlValue::Int64(max_commit_micros),
                    SqlValue::Text(object_uri.to_string()),
                    SqlValue::Int64(row_count),
                    SqlValue::Text(format_text.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Phase 2: stamp a tx_log segment committed — visible to reads and to the
    /// watermark. Only committed segments count.
    pub async fn commit_tx_log_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        tx_log_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "UPDATE {table} SET committed_at_micros = {epoch} \
             WHERE branch_uuid = $1 AND tx_log_segment_uuid = $2",
            table = qi(&table),
            epoch = epoch(),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(tx_log_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// List orphaned uncommitted tx_log segments for a branch as
    /// `(tx_log_segment_uuid, object_uri)`. Run at the start of persist_tx_log:
    /// the caller deletes each cold file, then drops the row via
    /// [`Self::delete_uncommitted_tx_log_persist_segment`] — file-first, so a
    /// failed file delete leaves the reclamation record for the next sweep
    /// rather than orphaning the file with no DB record. The `tx_log` URI kind
    /// is excluded from the persist/snapshot orphan sweeps, so this is its only
    /// reclamation path.
    pub async fn list_uncommitted_tx_log_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Vec<(String, String)>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "SELECT tx_log_segment_uuid, object_uri FROM {table} \
             WHERE branch_uuid = $1 AND committed_at_micros IS NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let uuid: Uuid = r.get("tx_log_segment_uuid");
                (uuid.to_string(), r.get("object_uri"))
            })
            .collect())
    }

    /// Crash cleanup: drop a tx_log segment row only while still uncommitted.
    pub async fn delete_uncommitted_tx_log_persist_segment(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        tx_log_segment_uuid: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "DELETE FROM {table} \
             WHERE branch_uuid = $1 AND tx_log_segment_uuid = $2 \
               AND committed_at_micros IS NULL",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::uuid_str(tx_log_segment_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// The tx_log persist watermark: highest `commit_seq_num` durable in cold
    /// for this branch (`MAX(max_commit_seq_num)` over committed segments), or
    /// `None` when nothing has been flushed. PurgeTxLog clamps its
    /// `commit_tx_log` cutoff to this so a hot row is never GC'd before its
    /// cold copy exists.
    pub async fn tx_log_persist_watermark(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Option<i64>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "SELECT MAX(max_commit_seq_num) AS wm FROM {table} \
             WHERE branch_uuid = $1 AND committed_at_micros IS NOT NULL",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows.first().and_then(|r| r.get::<Option<i64>, _>("wm")))
    }

    /// Every committed cold `tx_log` segment for a branch, ascending by
    /// `min_commit_seq_num`. Callers prune on the seq/micros bounds and read
    /// the file via `ColdStorageClient::read_persist_segments`.
    pub async fn read_committed_tx_log_segments(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Vec<TxLogSegment>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::tx_log_persist_segment_metadata_table(&catalog);
        let sql = format!(
            "SELECT tx_log_segment_uuid, min_commit_seq_num, max_commit_seq_num, \
                    min_commit_micros, max_commit_micros, object_uri, row_count, format \
             FROM {table} \
             WHERE branch_uuid = $1 AND committed_at_micros IS NOT NULL \
             ORDER BY min_commit_seq_num",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                let uuid: Uuid = r.get("tx_log_segment_uuid");
                TxLogSegment {
                    tx_log_segment_uuid: uuid.to_string(),
                    object_uri: r.get("object_uri"),
                    format: r.get("format"),
                    row_count: r.get("row_count"),
                    min_commit_seq_num: r.get("min_commit_seq_num"),
                    max_commit_seq_num: r.get("max_commit_seq_num"),
                    min_commit_micros: r.get("min_commit_micros"),
                    max_commit_micros: r.get("max_commit_micros"),
                }
            })
            .collect())
    }
}
