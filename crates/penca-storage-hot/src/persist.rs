//! Persist-side reads and deletes for moving hot rows to cold.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::{DbDriver, SqlValue, format_sql_uuid_array};
use sqlx::postgres::PgRow;

use crate::row_codec::{empty_batch, rows_to_batch};
use crate::sql_literal::build_committed_at_filter;
use crate::{HotStorageClient, HotStorageError};

/// Tx metadata fields the persist-side JOIN against `commit_tx_log`
/// carries onto each cold upsert/delete row. Column order matches the
/// `SELECT t.commit_micros, t.began_at_micros, t.commit_seq_num` projection in
/// [`HotStorageClient::read_committed_upserts`] and
/// [`HotStorageClient::read_committed_deletes`].
///
/// `author`/`comment` are no longer denormalized onto cold rows —
/// they live once per tx in the cold `tx_log` and are joined on demand by
/// `audit_data`. The trailing `commit_seq_num` position is
/// load-bearing: the cold on-disk schema tail must match this JOIN result tail
/// or DataFusion projection fails — keep it in sync with `penca_merge`'s
/// `cold_tx_metadata_fields`.
fn joined_tx_metadata_fields() -> Vec<Arc<Field>> {
    vec![
        Arc::new(Field::new("commit_micros", DataType::Int64, false)),
        Arc::new(Field::new("began_at_micros", DataType::Int64, false)),
        Arc::new(Field::new("commit_seq_num", DataType::Int64, false)),
    ]
}

impl HotStorageClient {
    /// Read committed upsert rows joined with `commit_tx_log`.
    ///
    /// Widened JOIN. Returns a `RecordBatch` with the upsert
    /// columns followed by the denormalized tx metadata fields —
    /// `commit_micros, began_at_micros` and `commit_seq_num`. The
    /// caller projects these onto each cold persist segment row.
    /// `author`/`comment` are deliberately NOT denormalized here — `audit_data`
    /// joins them from the cold tx_log on demand.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            upsert_table = %upsert_table,
            tx_table = %tx_table,
            min_commit_micros = ?min_commit_micros,
            max_commit_micros = ?max_commit_micros,
            rows = tracing::field::Empty,
        ),
    )]
    pub async fn read_committed_upserts(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        upsert_table: &str,
        tx_table: &str,
        upsert_schema: &SchemaRef,
        min_commit_micros: Option<i64>,
        max_commit_micros: Option<i64>,
    ) -> Result<RecordBatch, HotStorageError> {
        let upsert_columns = upsert_schema
            .fields()
            .iter()
            .map(|f| PgDialect::quote_column("u", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let filter = build_committed_at_filter(min_commit_micros, max_commit_micros);

        let sql = format!(
            "SELECT {upsert_columns}, \
                    t.commit_micros, t.began_at_micros, t.commit_seq_num \
             FROM {} u INNER JOIN {} t ON u.tx_uuid = t.tx_uuid \
             WHERE 1=1{filter}",
            PgDialect::quote_identifier(upsert_table),
            PgDialect::quote_identifier(tx_table),
        );

        let rows = driver.execute(&sql).await?;
        tracing::Span::current().record("rows", rows.len());

        // Build result schema: upsert fields + the denormalized tx
        // metadata fields. Order matches the SELECT projection above.
        let mut fields: Vec<Arc<Field>> = upsert_schema.fields().to_vec();
        fields.extend(joined_tx_metadata_fields());
        let result_schema = Arc::new(Schema::new(fields));

        if rows.is_empty() {
            return Ok(empty_batch(&result_schema));
        }
        rows_to_batch(&rows, &result_schema)
    }

    /// Read committed delete rows joined with `commit_tx_log`.
    ///
    /// Widened JOIN. The trailing tx metadata columns get
    /// pre-joined onto each cold delete segment row so the cold side reads
    /// as a pure scan.
    ///
    /// Returns a `RecordBatch` with columns
    /// `(version_uuid, row_uuid, <pk_cols...>, tx_uuid, write_seq_num,
    ///   commit_micros, began_at_micros, commit_seq_num)`. PK columns
    /// interleave between `row_uuid` and `tx_uuid` in table-declared order;
    /// their types are resolved from `user_schema`.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            delete_table = %delete_table,
            tx_table = %tx_table,
            min_commit_micros = ?min_commit_micros,
            max_commit_micros = ?max_commit_micros,
            num_primary_keys = primary_keys.len(),
            rows = tracing::field::Empty,
        ),
    )]
    pub async fn read_committed_deletes(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        delete_table: &str,
        tx_table: &str,
        user_schema: &SchemaRef,
        primary_keys: &[String],
        min_commit_micros: Option<i64>,
        max_commit_micros: Option<i64>,
    ) -> Result<RecordBatch, HotStorageError> {
        let filter = build_committed_at_filter(min_commit_micros, max_commit_micros);

        let pk_select = primary_keys
            .iter()
            .map(|pk| PgDialect::quote_column("d", pk))
            .collect::<Vec<_>>();
        let pk_select_sql = if pk_select.is_empty() {
            String::new()
        } else {
            format!(", {}", pk_select.join(", "))
        };

        // Project `d.write_seq_num` (the within-tx mutation ordinal)
        // alongside the tx metadata so persist + audit projections
        // downstream can carry it onto cold/audit rows.
        let sql = format!(
            "SELECT d.version_uuid, d.row_uuid{pk_select_sql}, d.tx_uuid, \
                    d.write_seq_num, \
                    t.commit_micros, t.began_at_micros, t.commit_seq_num \
             FROM {} d INNER JOIN {} t ON d.tx_uuid = t.tx_uuid \
             WHERE 1=1{filter}",
            PgDialect::quote_identifier(delete_table),
            PgDialect::quote_identifier(tx_table),
        );

        let rows = driver.execute(&sql).await?;
        tracing::Span::current().record("rows", rows.len());

        let mut fields: Vec<Arc<Field>> = vec![
            Arc::new(Field::new("version_uuid", DataType::Utf8, false)),
            Arc::new(Field::new("row_uuid", DataType::Utf8, false)),
        ];
        for pk in primary_keys {
            let field = user_schema
                .field_with_name(pk)
                .map_err(|_| HotStorageError::SchemaMismatch { pk: pk.clone() })?
                .clone();
            fields.push(Arc::new(field));
        }
        fields.push(Arc::new(Field::new("tx_uuid", DataType::Utf8, false)));
        // write_seq_num follows tx_uuid (matches the SELECT order +
        // the upsert read schema), projected through to cold.
        fields.push(Arc::new(Field::new(
            "write_seq_num",
            DataType::Int64,
            false,
        )));
        fields.extend(joined_tx_metadata_fields());
        let result_schema = Arc::new(Schema::new(fields));

        if rows.is_empty() {
            return Ok(empty_batch(&result_schema));
        }
        rows_to_batch(&rows, &result_schema)
    }

    /// Read `commit_tx_log` rows for a set of transaction UUIDs.
    ///
    /// Returns a `RecordBatch` with columns
    /// `(tx_uuid, branch_uuid, commit_micros)`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            tx_table = %tx_table,
            num_tx_uuids = tx_uuids.len(),
            rows = tracing::field::Empty,
        ),
    )]
    pub async fn read_commit_tx_log_by_uuids(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        tx_table: &str,
        tx_uuids: &[String],
    ) -> Result<RecordBatch, HotStorageError> {
        let result_schema = Arc::new(Schema::new(vec![
            Field::new("tx_uuid", DataType::Utf8, false),
            Field::new("branch_uuid", DataType::Utf8, false),
            Field::new("commit_micros", DataType::Int64, false),
        ]));

        if tx_uuids.is_empty() {
            return Ok(empty_batch(&result_schema));
        }

        let tx_uuid_refs: Vec<&str> = tx_uuids.iter().map(String::as_str).collect();
        let sql = format!(
            "SELECT tx_uuid, branch_uuid, commit_micros \
             FROM {} WHERE tx_uuid = ANY({})",
            PgDialect::quote_identifier(tx_table),
            format_sql_uuid_array(&tx_uuid_refs),
        );

        let rows = driver.execute(&sql).await?;
        tracing::Span::current().record("rows", rows.len());
        if rows.is_empty() {
            return Ok(empty_batch(&result_schema));
        }
        rows_to_batch(&rows, &result_schema)
    }

    /// Delete rows where a `uuid`-typed `column` matches any of `values`.
    ///
    /// Used by persist to remove committed upsert/delete rows from hot
    /// storage by their `version_uuid`. The sole caller today is
    /// lifecycle persist (phase 2), and both hot tables involved have a
    /// `uuid` `version_uuid` column — hence the uuid-specific signature.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            column = %column,
            num_values = values.len(),
        ),
    )]
    pub async fn delete_by_uuid_column(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        column: &str,
        values: &[String],
    ) -> Result<(), HotStorageError> {
        if values.is_empty() {
            return Ok(());
        }
        let value_refs: Vec<&str> = values.iter().map(String::as_str).collect();
        let sql = format!(
            "DELETE FROM {} WHERE {} = ANY({})",
            PgDialect::quote_identifier(table_name),
            PgDialect::quote_identifier(column),
            format_sql_uuid_array(&value_refs),
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }

    /// Delete rows matching composite `(uuid, uuid)` key pairs.
    ///
    /// Uses `(col_a, col_b) IN (SELECT unnest(...), unnest(...))`. Both
    /// columns must be `uuid`-typed (sole caller today is persist against
    /// the delete-log `(row_uuid, tx_uuid)` pair).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            col_a = %columns.0,
            col_b = %columns.1,
            num_pairs = value_pairs.len(),
        ),
    )]
    pub async fn delete_by_composite(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        columns: (&str, &str),
        value_pairs: &[(&str, &str)],
    ) -> Result<(), HotStorageError> {
        if value_pairs.is_empty() {
            return Ok(());
        }
        let col_a_values: Vec<&str> = value_pairs.iter().map(|(a, _)| *a).collect();
        let col_b_values: Vec<&str> = value_pairs.iter().map(|(_, b)| *b).collect();

        let sql = format!(
            "DELETE FROM {} WHERE ({}, {}) IN \
             (SELECT unnest({}), unnest({}))",
            PgDialect::quote_identifier(table_name),
            PgDialect::quote_identifier(columns.0),
            PgDialect::quote_identifier(columns.1),
            format_sql_uuid_array(&col_a_values),
            format_sql_uuid_array(&col_b_values),
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }

    /// Delete rows where `column` <= `threshold`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            column = %column,
            threshold,
        ),
    )]
    pub async fn delete_by_threshold(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        column: &str,
        threshold: i64,
    ) -> Result<(), HotStorageError> {
        let sql = format!(
            "DELETE FROM {} WHERE {} <= $1",
            PgDialect::quote_identifier(table_name),
            PgDialect::quote_identifier(column),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::Int64(threshold)])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The persist-side JOIN result tail must stay byte-identical (name,
    /// type, nullability, order) to the cold on-disk tail in
    /// `penca_merge`'s `cold_tx_metadata_fields` — the two are projected
    /// position-for-position, so drift only surfaces as a runtime
    /// DataFusion projection error. `penca-merge` (which can't be a dep
    /// here without a cycle) pins the cold side against the same literal
    /// in its own test (`CANONICAL_TX_METADATA_TAIL`); keep the two in
    /// sync.
    #[test]
    fn joined_tx_metadata_tail_is_canonical() {
        let expected: &[(&str, DataType, bool)] = &[
            ("commit_micros", DataType::Int64, false),
            ("began_at_micros", DataType::Int64, false),
            ("commit_seq_num", DataType::Int64, false),
        ];
        let fields = joined_tx_metadata_fields();
        let actual: Vec<(&str, DataType, bool)> = fields
            .iter()
            .map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let expected: Vec<(&str, DataType, bool)> = expected
            .iter()
            .map(|(n, t, nul)| (*n, t.clone(), *nul))
            .collect();
        assert_eq!(actual, expected);
    }
}
