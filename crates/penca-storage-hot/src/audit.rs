//! Audit-trail schema builders and streaming reads.

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use penca_db::dialect::pg::PgDialect;
use penca_db::dialect::{Dialect, row_uuid_in_clause};
use penca_db::driver::DbDriver;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::query::stream_query_as_batches;
use crate::sql_literal::{build_commit_seq_num_filter, build_committed_at_filter};
use crate::{HotStorageClient, HotStorageError};

/// Output schema for [`HotStorageClient::audit_upserts_stream`]: user
/// columns followed by tx/commit annotation columns. Exposed so the
/// API layer can emit a schema-header batch before the stream.
pub fn audit_upsert_schema(user_schema: &SchemaRef, include_tx_metadata: bool) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = user_schema.fields().to_vec();
    fields.extend(audit_tx_metadata_fields(include_tx_metadata));
    Arc::new(Schema::new(fields))
}

/// Output schema for [`HotStorageClient::audit_deletes_stream`]:
/// primary-key columns (filtered from `user_schema` in declared
/// `primary_keys` order) followed by tx/commit annotation columns.
/// `row_uuid` is dropped from the audit projection —
/// consumers read PKs directly instead of inverting the hash via a
/// join back to `upsert_log`. The column remains stored in
/// `delete_log` itself as the merge join key.
///
/// `primary_keys` is structurally guaranteed to be a subset of
/// `user_schema` (DDL-time invariant from
/// `PgDialect::create_data_tables`). The fallible signature mirrors
/// [`HotStorageClient::read_committed_deletes`] and surfaces a
/// programmer error if the invariant is ever violated.
pub fn audit_delete_schema(
    user_schema: &SchemaRef,
    primary_keys: &[String],
    include_tx_metadata: bool,
) -> Result<SchemaRef, HotStorageError> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(primary_keys.len() + 4);
    for pk in primary_keys {
        let field = user_schema
            .field_with_name(pk)
            .map_err(|_| HotStorageError::SchemaMismatch { pk: pk.clone() })?
            .clone();
        fields.push(Arc::new(field));
    }
    fields.extend(audit_tx_metadata_fields(include_tx_metadata));
    Ok(Arc::new(Schema::new(fields)))
}

/// Denormalized tx-metadata columns shared by hot and cold audit row schemas.
/// `tx_uuid` is dropped here because cold rows can't supply it — emitting a
/// NULL-when-cold column would be the worse asymmetry.
///
/// `write_seq_num` sits between `commit_micros` and `comment`: the within-tx
/// mutation ordinal that, paired with `commit_seq_num`, is the total mutation
/// order `(commit_seq_num, write_seq_num)`.
/// A UPDATE-on-PK pair (a delete and an upsert in one batch) commits at
/// one `commit_seq_num` with the delete's `write_seq_num` strictly below the
/// upsert's (deletes-first), so the pair is orderable from audit output
/// without joining back to `upsert_log`.
///
/// `commit_seq_num` (the per-branch gapless commit-order serial) is appended
/// at the END. The hot stream sources `write_seq_num`
/// from the upsert/delete log row and `commit_seq_num` from the `commit_tx_log`
/// JOIN; the cold pure-scan sources both from the persist-stamped columns
/// — same tx => same `(commit_seq_num, write_seq_num)`, so `audit_data` agrees
/// across tiers. Audit column order is independent of the cold on-disk
/// order: `project_to_audit_schema` maps by name, so only presence in
/// both schemas matters.
///
/// `comment`/`author` are present only when the caller requests
/// `include_tx_metadata` — on the hot tier they come from the `commit_tx_log`
/// JOIN this stream already does; on the cold tier they are joined from the
/// cold `tx_log`. When omitted, the schema carries only the always-inline
/// axes.
fn audit_tx_metadata_fields(include_tx_metadata: bool) -> Vec<Arc<Field>> {
    let mut fields = vec![
        Arc::new(Field::new("began_at_micros", DataType::Int64, false)),
        Arc::new(Field::new("commit_micros", DataType::Int64, false)),
        Arc::new(Field::new("write_seq_num", DataType::Int64, false)),
    ];
    if include_tx_metadata {
        // Nullable: the cold audit path reattaches these via a LEFT JOIN on the
        // tx_log, whose column type is nullable (the hot INNER JOIN never
        // actually produces nulls). Keeping the schema nullable lets both tiers
        // share one audit schema.
        fields.push(Arc::new(Field::new("comment", DataType::Utf8, true)));
        fields.push(Arc::new(Field::new("author", DataType::Utf8, true)));
    }
    fields.push(Arc::new(Field::new(
        "commit_seq_num",
        DataType::Int64,
        false,
    )));
    fields
}

/// Row restriction for the audit streams: the committed_at window plus the
/// ids point-lookup set. These travel together through both stream fns and
/// both penca-api callers.
pub struct AuditRowFilter<'a> {
    pub from_micros: Option<i64>,
    pub to_micros: Option<i64>,
    /// Half-open `commit_seq_num` window for the seq-axis `committed`
    /// cursor. ANDs with the committed_at window; both empty = full horizon.
    pub from_seq: Option<i64>,
    pub to_seq: Option<i64>,
    pub row_uuids: Option<&'a [Uuid]>,
    /// Project the per-tx `comment`/`author` (from the existing
    /// `commit_tx_log` JOIN) only when the caller asked for them.
    pub include_tx_metadata: bool,
}

impl HotStorageClient {
    /// Stream audit trail as `RecordBatch` chunks via server-side cursor.
    ///
    /// Returns user columns plus audit metadata (`began_at_micros`,
    /// `commit_micros`, `write_seq_num`, `comment`, `author`,
    /// `commit_seq_num`). Joins the upsert log against the committed tx log
    /// to enrich each row with transaction metadata.
    ///
    /// Delegates to [`stream_query_as_batches`] — the same streaming core
    /// used by [`HotStorageClient::read_stream`]. Rows are pulled
    /// incrementally via the driver's `fetch_stream`, accumulated into
    /// `batch_size` chunks, and yielded as `RecordBatch`es.
    ///
    /// # Streaming and transactions
    ///
    /// Audit reads scan potentially the entire history of a table —
    /// unbounded result sets that require true DB-level streaming to
    /// avoid OOM. Works with both `PgDriver` (pool) and
    /// `PgTransactionDriver` — both implement true server-side cursor
    /// streaming. For transaction drivers, the mutex guard is held for
    /// the stream's lifetime, which is safe because transactions are
    /// used sequentially.
    pub fn audit_upserts_stream<'a>(
        &'a self,
        driver: &'a (impl DbDriver<Row = PgRow> + 'a),
        upsert_table: &str,
        commit_part: &str,
        user_schema: &SchemaRef,
        batch_size: usize,
        filter: AuditRowFilter<'_>,
    ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, HotStorageError>> + Send + 'a>> {
        tracing::debug!(
            upsert_table = %upsert_table,
            commit_part = %commit_part,
            batch_size,
            from_micros = ?filter.from_micros,
            to_micros = ?filter.to_micros,
            // The seq-axis `committed` window alongside the micros one.
            from_seq = ?filter.from_seq,
            to_seq = ?filter.to_seq,
            // Count only (PII gate); 0 = unrestricted.
            ids_rows = filter.row_uuids.map_or(0, <[Uuid]>::len) as u64,
            "audit_upserts_stream constructed",
        );

        let user_columns_sql = user_schema
            .fields()
            .iter()
            .map(|f| PgDialect::quote_column("u", f.name()))
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause = build_committed_at_filter(filter.from_micros, filter.to_micros);
        let seq_clause = build_commit_seq_num_filter(filter.from_seq, filter.to_seq);
        // Shared dialect-generic clause builder (penca-sql) —
        // same shape the merge builders emit, unit-tested there.
        let ids_clause = row_uuid_in_clause::<PgDialect>(filter.row_uuids, " AND ", "u.");

        // `u.write_seq_num` lives on the upsert log row itself (the
        // within-tx mutation ordinal); the JOIN against commit_tx_log surfaces the
        // tx-scoped metadata columns, including `commit_seq_num`.
        // Comment/author come off the commit_tx_log JOIN this stream
        // already does, but are projected only on request.
        let author_comment = if filter.include_tx_metadata {
            "t.comment, t.author, "
        } else {
            ""
        };
        let sql = format!(
            "SELECT {user_columns_sql}, \
             t.began_at_micros, t.commit_micros, u.write_seq_num, \
             {author_comment}t.commit_seq_num \
             FROM {} u INNER JOIN {} t ON u.tx_uuid = t.tx_uuid \
             WHERE TRUE{where_clause}{seq_clause}{ids_clause} \
             ORDER BY t.commit_micros, u.row_uuid",
            PgDialect::quote_identifier(upsert_table),
            PgDialect::quote_identifier(commit_part),
        );

        let result_schema = audit_upsert_schema(user_schema, filter.include_tx_metadata);
        stream_query_as_batches(driver, sql, vec![], result_schema, batch_size)
    }

    /// Stream tombstones from `delete_table` joined with the branch's
    /// commit_part, ordered by commit time then PK columns.
    ///
    /// Output schema: `<pk_cols> + tx metadata`. PK columns
    /// in declared order, sourced from the widened `delete_log`. `row_uuid`
    /// is deliberately not projected — see [`audit_delete_schema`].
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    pub fn audit_deletes_stream<'a>(
        &'a self,
        driver: &'a (impl DbDriver<Row = PgRow> + 'a),
        delete_table: &str,
        commit_part: &str,
        user_schema: &SchemaRef,
        primary_keys: &[String],
        batch_size: usize,
        filter: AuditRowFilter<'_>,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<RecordBatch, HotStorageError>> + Send + 'a>>,
        HotStorageError,
    > {
        let pk_cols_sql = primary_keys
            .iter()
            .map(|pk| PgDialect::quote_column("d", pk))
            .collect::<Vec<_>>()
            .join(", ");

        let where_clause = build_committed_at_filter(filter.from_micros, filter.to_micros);
        let seq_clause = build_commit_seq_num_filter(filter.from_seq, filter.to_seq);
        let ids_clause = row_uuid_in_clause::<PgDialect>(filter.row_uuids, " AND ", "d.");

        // `d.write_seq_num` is per-row on the delete log (the
        // within-tx mutation ordinal); the commit_tx_log JOIN supplies the
        // tx-scoped metadata columns, including `commit_seq_num`.
        let author_comment = if filter.include_tx_metadata {
            "t.comment, t.author, "
        } else {
            ""
        };
        let sql = format!(
            "SELECT {pk_cols_sql}, \
             t.began_at_micros, t.commit_micros, d.write_seq_num, \
             {author_comment}t.commit_seq_num \
             FROM {} d INNER JOIN {} t ON d.tx_uuid = t.tx_uuid \
             WHERE TRUE{where_clause}{seq_clause}{ids_clause} \
             ORDER BY t.commit_micros, {pk_cols_sql}",
            PgDialect::quote_identifier(delete_table),
            PgDialect::quote_identifier(commit_part),
        );

        let result_schema =
            audit_delete_schema(user_schema, primary_keys, filter.include_tx_metadata)?;
        tracing::debug!(
            delete_table = %delete_table,
            commit_part = %commit_part,
            batch_size,
            from_micros = ?filter.from_micros,
            to_micros = ?filter.to_micros,
            // The seq-axis `committed` window alongside the micros one.
            from_seq = ?filter.from_seq,
            to_seq = ?filter.to_seq,
            num_primary_keys = primary_keys.len(),
            ids_rows = filter.row_uuids.map_or(0, <[Uuid]>::len) as u64,
            "audit_deletes_stream constructed",
        );
        Ok(stream_query_as_batches(
            driver,
            sql,
            vec![],
            result_schema,
            batch_size,
        ))
    }
}
