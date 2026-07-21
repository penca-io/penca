//! Read / write / lock methods against user-data tables and per-branch logs.

use std::pin::Pin;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use penca_core::naming;
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::{DbDriver, SqlValue, format_sql_uuid_array};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::query::stream_query_as_batches;
use crate::row_codec::{arrow_to_sql_literal, empty_batch, rows_to_batch};
use crate::{HotStorageClient, HotStorageError};

impl HotStorageClient {
    /// Read rows from a table with optional WHERE filtering.
    ///
    /// `where_clause` uses `$1`/`$2` placeholders; `params` provides the
    /// corresponding bind values.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            params_len = params.len(),
            rows = tracing::field::Empty,
        ),
    )]
    pub async fn read(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        schema: &SchemaRef,
        where_clause: Option<&str>,
        params: &[SqlValue],
    ) -> Result<RecordBatch, HotStorageError> {
        let cols = schema
            .fields()
            .iter()
            .map(|f| PgDialect::quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "SELECT {} FROM {}",
            cols,
            PgDialect::quote_identifier(table_name)
        );
        if let Some(wc) = where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(wc);
        }

        let rows = if params.is_empty() {
            driver.execute(&sql).await?
        } else {
            driver.execute_params(&sql, params).await?
        };

        tracing::Span::current().record("rows", rows.len());

        if rows.is_empty() {
            return Ok(empty_batch(schema));
        }
        rows_to_batch(&rows, schema)
    }

    /// Stream rows from a table as `RecordBatch` chunks via server-side cursor.
    ///
    /// Streaming counterpart to [`read`](Self::read). Where `read` materializes
    /// all rows before returning, `read_stream` pulls rows incrementally via
    /// the driver's [`fetch_stream`](DbDriver::fetch_stream), accumulating
    /// `batch_size` rows per `RecordBatch` before yielding. Used by the query
    /// manager's `read_data` read path (CHA-86) for upsert logs that may
    /// contain millions of rows.
    ///
    /// Works with both `PgDriver` (pool) and `PgTransactionDriver` — both
    /// implement true server-side cursor streaming. For transaction drivers,
    /// the mutex guard is held for the stream's lifetime, which is safe
    /// because transactions are used sequentially.
    pub fn read_stream<'a>(
        &'a self,
        driver: &'a (impl DbDriver<Row = PgRow> + 'a),
        table_name: &str,
        schema: &SchemaRef,
        where_clause: Option<&str>,
        params: &[SqlValue],
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, HotStorageError>> + Send + 'a>> {
        tracing::debug!(
            table_name = %table_name,
            params_len = params.len(),
            batch_size,
            "read_stream constructed",
        );

        let cols = schema
            .fields()
            .iter()
            .map(|f| PgDialect::quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let mut sql = format!(
            "SELECT {} FROM {}",
            cols,
            PgDialect::quote_identifier(table_name)
        );
        if let Some(wc) = where_clause {
            sql.push_str(" WHERE ");
            sql.push_str(wc);
        }

        stream_query_as_batches(driver, sql, params.to_vec(), schema.clone(), batch_size)
    }

    /// Return the row count for a table.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            count = tracing::field::Empty,
        ),
    )]
    pub async fn count_rows(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
    ) -> Result<i64, HotStorageError> {
        let sql = format!(
            "SELECT count(*) FROM {}",
            PgDialect::quote_identifier(table_name)
        );
        let rows = driver.execute(&sql).await?;
        let count: i64 = rows[0].get(0);
        tracing::Span::current().record("count", count);
        Ok(count)
    }

    /// Batch insert rows into an upsert log table.
    ///
    /// Builds a single multi-row INSERT with values formatted as SQL literals.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            tx_uuid = %tx_uuid,
            num_rows = user_batch.num_rows(),
        ),
    )]
    pub async fn insert_upserts(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        user_columns: &[&str],
        version_uuids: &[String],
        row_uuids: &[String],
        tx_uuid: &str,
        user_batch: &RecordBatch,
    ) -> Result<(), HotStorageError> {
        let num_rows = user_batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let user_col_ids = user_columns
            .iter()
            .map(|c| PgDialect::quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");

        let tx_literal = format!("'{tx_uuid}'");
        let mut values_parts = Vec::with_capacity(num_rows);

        for row in 0..num_rows {
            let mut vals = vec![
                format!("'{}'", version_uuids[row]),
                format!("'{}'", row_uuids[row]),
                tx_literal.clone(),
            ];
            for col_idx in 0..user_batch.num_columns() {
                vals.push(arrow_to_sql_literal(user_batch.column(col_idx), row)?);
            }
            values_parts.push(format!("({})", vals.join(", ")));
        }

        // ON CONFLICT (version_uuid) DO UPDATE: deterministic
        // version_uuid (ADR 0013) means same (row, tx) → same PK,
        // so multiple writes of the same row in one tx collapse to
        // last-write-wins. Auditable-store invariant enforced by the
        // PK alone (no separate UNIQUE(row_uuid, tx_uuid) needed).
        let user_set_assignments = user_columns
            .iter()
            .map(|c| {
                let q = PgDialect::quote_identifier(c);
                format!("{q} = EXCLUDED.{q}")
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} (version_uuid, row_uuid, tx_uuid, {}) VALUES {} \
             ON CONFLICT (version_uuid) DO UPDATE SET {}",
            PgDialect::quote_identifier(table_name),
            user_col_ids,
            values_parts.join(", "),
            user_set_assignments,
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }

    /// Emit per-(tx, table) summary rows into the per-branch
    /// `tx_table_log` partition table (CHA-181). One row per distinct
    /// `(tx_uuid, branch_uuid, table_uuid)` triple. `ON CONFLICT DO
    /// NOTHING` keeps emission idempotent across multiple `WriteData`
    /// calls within the same penca tx.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            tx_uuid = %tx_uuid,
            branch_uuid = %branch_uuid,
            num_tables = table_uuids.len(),
        ),
    )]
    pub async fn insert_tx_table_log(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        branch_uuid: &str,
        tx_uuid: &str,
        table_uuids: &[String],
    ) -> Result<(), HotStorageError> {
        if table_uuids.is_empty() {
            return Ok(());
        }
        let table_refs: Vec<&str> = table_uuids.iter().map(String::as_str).collect();
        let sql = format!(
            "INSERT INTO {} (tx_uuid, branch_uuid, table_uuid) \
             SELECT '{tx_uuid}'::uuid, '{branch_uuid}'::uuid, t \
             FROM unnest({}) AS t \
             ON CONFLICT (tx_uuid, branch_uuid, table_uuid) DO NOTHING",
            PgDialect::quote_identifier(table_name),
            format_sql_uuid_array(&table_refs),
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }

    /// Distinct `table_uuid`s a branch's committed txs have written to
    /// (CHA-181). Joins the branch's `tx_table_log` partition with its
    /// `commit_tx_log` partition on `tx_uuid` and filters to committed rows
    /// only — aborted/expired-tx rows in `tx_table_log` are dropped.
    ///
    /// Used by `merge_branch` to drive the per-table merge loop only
    /// over tables source actually wrote to since fork (skip the
    /// scan-empty-window cost on untouched tables); the same probe
    /// shape will serve CHA-5 conflict detection and CHA-168 persist.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            tx_table_log_partition = %tx_table_log_partition,
            commit_tx_log_partition = %commit_tx_log_partition,
            num_tables = tracing::field::Empty,
        ),
    )]
    pub async fn committed_table_uuids(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        tx_table_log_partition: &str,
        commit_tx_log_partition: &str,
    ) -> Result<Vec<uuid::Uuid>, HotStorageError> {
        let sql = format!(
            "SELECT DISTINCT tt.table_uuid \
             FROM {tt} tt \
             JOIN {tx} tl USING (tx_uuid) \
             WHERE tl.commit_micros IS NOT NULL",
            tt = PgDialect::quote_identifier(tx_table_log_partition),
            tx = PgDialect::quote_identifier(commit_tx_log_partition),
        );
        let rows = driver.execute(&sql).await?;
        let table_uuids: Vec<uuid::Uuid> = rows.iter().map(|r| r.get("table_uuid")).collect();
        tracing::Span::current().record("num_tables", table_uuids.len());
        Ok(table_uuids)
    }

    /// Batch insert rows into a delete log table.
    ///
    /// `version_uuid` is deterministic per ADR 0013 —
    /// `xxh3(row_uuid, tx_uuid)`, same derivation as the upsert path —
    /// and PK conflicts on a duplicate `(row_uuid, tx_uuid)` tombstone
    /// are absorbed via `ON CONFLICT (version_uuid) DO NOTHING`.
    ///
    /// `pk_batch` carries the table's declared primary keys in declared
    /// order; its schema names the columns and provides their values.
    /// The columns are written into `delete_log` between `row_uuid` and
    /// `tx_uuid`, mirroring the DDL emitted in
    /// `PgDialect::create_data_tables`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            tx_uuid = %tx_uuid,
            num_rows = pk_batch.num_rows(),
        ),
    )]
    pub async fn insert_deletes(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        pk_batch: &RecordBatch,
        row_uuids: &[String],
        tx_uuid: &str,
    ) -> Result<(), HotStorageError> {
        let num_rows = pk_batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        let pk_col_ids = pk_batch
            .schema()
            .fields()
            .iter()
            .map(|f| PgDialect::quote_identifier(f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let tx_uuid_parsed = tx_uuid
            .parse::<uuid::Uuid>()
            .map_err(|e| sqlx::Error::Protocol(format!("invalid tx_uuid: {e}")))?;
        let tx_literal = format!("'{tx_uuid}'");
        let mut values_parts = Vec::with_capacity(num_rows);

        for (row, row_uuid) in row_uuids.iter().enumerate() {
            let row_uuid_parsed = row_uuid
                .parse::<uuid::Uuid>()
                .map_err(|e| sqlx::Error::Protocol(format!("invalid row_uuid: {e}")))?;
            let version_uuid = naming::version_uuid(&row_uuid_parsed, &tx_uuid_parsed);
            let mut vals = vec![format!("'{version_uuid}'"), format!("'{row_uuid}'")];
            for col_idx in 0..pk_batch.num_columns() {
                vals.push(arrow_to_sql_literal(pk_batch.column(col_idx), row)?);
            }
            vals.push(tx_literal.clone());
            values_parts.push(format!("({})", vals.join(", ")));
        }

        let sql = format!(
            "INSERT INTO {} (version_uuid, row_uuid, {}, tx_uuid) VALUES {} \
             ON CONFLICT (version_uuid) DO NOTHING",
            PgDialect::quote_identifier(table_name),
            pk_col_ids,
            values_parts.join(", ")
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }

    /// Acquire a table-level lock (e.g. `"EXCLUSIVE"` mode).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            mode = %mode,
        ),
    )]
    pub async fn lock_table(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        mode: &str,
    ) -> Result<(), HotStorageError> {
        let sql = format!(
            "LOCK TABLE {} IN {} MODE",
            PgDialect::quote_identifier(table_name),
            mode
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }
}
