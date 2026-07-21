//! Arrow IPC serialization for streaming gRPC responses.

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use futures_util::StreamExt;
use penca_api::ApiError;
use tonic::Status;
use tracing_futures::Instrument as _;

use crate::status::api_error_to_status;

/// Serialize a `RecordBatch` to Arrow IPC stream format bytes.
pub fn batch_to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Status> {
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
        .map_err(|e| Status::internal(format!("arrow ipc write error: {e}")))?;
    writer
        .write(batch)
        .map_err(|e| Status::internal(format!("arrow ipc write error: {e}")))?;
    writer
        .finish()
        .map_err(|e| Status::internal(format!("arrow ipc write error: {e}")))?;
    Ok(buf)
}

/// Drive an upstream batch stream, IPC-encode each batch, and yield
/// caller-shaped responses. Single canonical streaming-loop site for
/// `QueryService::read_data` and `QueryService::audit_data`.
///
/// Upstream `ApiError`s map through [`api_error_to_status`]; IPC encoding
/// errors propagate as `Status::internal` via [`batch_to_ipc_bytes`]. In
/// both cases the response stream terminates with the mapped status.
///
/// CHA-417: the whole response stream runs inside one `ipc_encode` debug
/// span (cumulative `batches`/`rows`/`bytes` recorded at end-of-stream,
/// not a span per batch). With `PENCA_SPAN_TIMING` set, the span-close
/// `time.busy` is the IPC-encode bucket's share of the response — note
/// `busy` also includes polling the upstream `stream_merged` stream, so read
/// it against the child spans' own timings. An errored or client-cancelled
/// stream still closes the span (with timing) but leaves the count fields
/// unrecorded — counts are stamped only on clean end-of-stream, so a
/// timed close with no counts reads as "aborted", not "zero rows".
pub(crate) fn ipc_response_stream<S, Resp, F>(
    batches: S,
    mut into_response: F,
) -> impl Stream<Item = Result<Resp, Status>>
where
    S: Stream<Item = Result<RecordBatch, ApiError>>,
    F: FnMut(Vec<u8>) -> Resp,
{
    let span = tracing::debug_span!(
        "ipc_encode",
        batches = tracing::field::Empty,
        rows = tracing::field::Empty,
        bytes = tracing::field::Empty,
    );
    async_stream::try_stream! {
        let mut batches_encoded: i64 = 0;
        let mut rows_encoded: i64 = 0;
        let mut bytes_encoded: i64 = 0;
        let mut batches = std::pin::pin!(batches);
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(api_error_to_status)?;
            let bytes = batch_to_ipc_bytes(&batch)?;
            batches_encoded += 1;
            rows_encoded += batch.num_rows() as i64;
            bytes_encoded += bytes.len() as i64;
            yield into_response(bytes);
        }
        tracing::Span::current().record("batches", batches_encoded);
        tracing::Span::current().record("rows", rows_encoded);
        tracing::Span::current().record("bytes", bytes_encoded);
    }
    .instrument(span)
}
