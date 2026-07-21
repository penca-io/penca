//! Streaming/exec primitives shared between hot-tier reads and out-of-crate callers.

use std::pin::Pin;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use futures_util::StreamExt as _;
use penca_db::driver::{DbDriver, SqlValue};
use sqlx::postgres::PgRow;
use tracing_futures::Instrument as _;

use crate::HotStorageError;
use crate::row_codec::rows_to_batch;

/// Stream rows from a SQL query as `RecordBatch` chunks.
///
/// Shared streaming core for [`crate::HotStorageClient::read_stream`] and
/// [`crate::HotStorageClient::audit_upserts_stream`]. Pulls rows incrementally via
/// the driver's [`fetch_stream`](DbDriver::fetch_stream), accumulates
/// `batch_size` rows per `RecordBatch`, and yields each batch as it fills.
///
/// Exposed to out-of-crate callers (penca-merge) so they can execute
/// merge-on-read SQL (built via the shared `penca_merge::sql` builder)
/// without going through the table-name-shaped helpers on
/// [`crate::HotStorageClient`].
pub fn stream_query_as_batches<'a, D: DbDriver<Row = PgRow> + 'a>(
    driver: &'a D,
    sql: String,
    params: Vec<SqlValue>,
    schema: SchemaRef,
    batch_size: usize,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, HotStorageError>> + Send + 'a>> {
    // CHA-417: one stream-level span per query (not per batch) so the hot
    // cursor read — the all_hot fast path included — shows up in the span
    // table with its own busy/idle timing; the counters the old
    // start/complete events carried are span fields recorded at
    // end-of-stream. An errored or client-cancelled stream still closes
    // the span (with timing) but leaves the count fields unrecorded —
    // a timed close with no counts reads as "aborted", not "zero rows".
    let span = tracing::debug_span!(
        "stream_query_as_batches",
        batch_size,
        batches_yielded = tracing::field::Empty,
        rows_yielded = tracing::field::Empty,
    );
    Box::pin(
        async_stream::try_stream! {
            let mut db_stream = driver.fetch_stream(&sql, &params);
            let mut rows: Vec<PgRow> = Vec::with_capacity(batch_size);
            let mut batches_yielded: u64 = 0;
            let mut rows_yielded: u64 = 0;
            while let Some(row_result) = db_stream.next().await {
                rows.push(row_result?);
                if rows.len() >= batch_size {
                    rows_yielded += rows.len() as u64;
                    yield rows_to_batch(&rows, &schema)?;
                    batches_yielded += 1;
                    rows.clear();
                }
            }
            if !rows.is_empty() {
                rows_yielded += rows.len() as u64;
                yield rows_to_batch(&rows, &schema)?;
                batches_yielded += 1;
            }
            tracing::Span::current().record("batches_yielded", batches_yielded);
            tracing::Span::current().record("rows_yielded", rows_yielded);
        }
        .instrument(span),
    )
}

/// Execute a SQL query and materialize all rows as a single `RecordBatch`.
///
/// Non-cursor one-shot sibling of [`stream_query_as_batches`], for callers
/// that need the full result in memory anyway (e.g. merge-on-read dedup,
/// which can't emit a row until every version of its `row_uuid` has been
/// seen). Skipping the cursor avoids the per-batch `FETCH FORWARD N`
/// round-trips; the PG `work_mem` safety net is given up in exchange.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        params_len = params.len(),
        rows = tracing::field::Empty,
    ),
)]
pub async fn execute_query_as_batch<D: DbDriver<Row = PgRow>>(
    driver: &D,
    sql: &str,
    params: &[SqlValue],
    schema: &SchemaRef,
) -> Result<RecordBatch, HotStorageError> {
    let rows = driver.execute_params(sql, params).await?;
    tracing::Span::current().record("rows", rows.len());
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    rows_to_batch(&rows, schema)
}
