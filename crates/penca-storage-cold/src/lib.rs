//! Cold storage client for reading, writing, and deleting durable data in
//! object storage.
//!
//! Reads dispatch each segment to the appropriate [`FormatReader`] based on the
//! segment's [`penca_core::Format`] field, yielding one
//! [`RecordBatch`] per segment.
//! Writes and deletes go through the single supplied [`FormatWriter`] — a given
//! `LifecycleManager` is configured for one format at a time.

use std::collections::HashMap;
use std::pin::Pin;

use arrow::array::{BooleanArray, Int64Array};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures_core::Stream;
use penca_core::{IndexSidecar, PersistSegment};
use penca_format::reader::{FormatError, FormatReader, empty_batch};
use penca_format::writer::FormatWriter;

/// Errors from cold storage operations.
#[derive(Debug, thiserror::Error)]
pub enum ColdStorageError {
    #[error(transparent)]
    Format(#[from] FormatError),

    #[error("no reader registered for storage format {0}")]
    UnknownFormat(i32),
}

/// Reads persisted data from the cold storage tier.
///
/// Stateless unit struct — callers pass the readers map explicitly.
/// Each segment carries its own [`penca_core::Format`]; the client
/// dispatches on its wire code to the matching `FormatReader` and streams
/// results one batch per segment.
pub struct ColdStorageClient;

/// The per-row commit-order column a persist segment's ceiling is applied to.
/// Named here because the read that fetches a batch and the filter that bounds
/// it must agree on the column.
pub const COMMIT_SEQ_NUM_COLUMN: &str = "commit_seq_num";

/// Clamp a persist segment's batch to its own `max_commit_seq_num` ceiling.
///
/// A no-op for every ordinarily-written segment — the recorded maximum IS the
/// file's largest `commit_seq_num` — and for the cold tx_log carriers, which set
/// no ceiling. It bites only on a row that deliberately claims less than its file
/// holds: a fork's inherited persist references, clamped to the fork position
/// (CHA-539).
pub fn apply_segment_seq_ceiling(
    batch: &RecordBatch,
    segment: &PersistSegment,
) -> Result<RecordBatch, ArrowError> {
    let Some(ceiling) = segment.max_commit_seq_num else {
        // No ceiling: the tx_log carriers. The only legitimate pass-through.
        return Ok(batch.clone());
    };
    // A ceiling that cannot be applied is an error, never a pass-through. Read
    // paths widen their projection to keep this column precisely so the bound is
    // always enforceable; reaching here without it means that contract broke, and
    // silently returning the unfiltered batch would leak the rows the ceiling
    // exists to hide.
    let idx = batch
        .schema()
        .index_of(COMMIT_SEQ_NUM_COLUMN)
        .map_err(|_| {
            ArrowError::SchemaError(format!(
                "persist segment {} carries a commit_seq_num ceiling of {ceiling} but the \
             batch has no commit_seq_num column to apply it to",
                segment.segment_uuid
            ))
        })?;
    let seqs = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ArrowError::SchemaError("commit_seq_num is not Int64".into()))?;
    let keep: BooleanArray = seqs
        .iter()
        .map(|v| Some(v.is_some_and(|seq| seq <= ceiling)))
        .collect();

    arrow::compute::filter_record_batch(batch, &keep)
}

// Helper functions exist to work around async_stream's try_stream! macro
// not being able to call async trait methods directly when the returned
// stream must be Send. Extracting the await into a standalone async fn
// with concrete `impl FormatReader` bounds produces a Send future that
// the macro can drive.

/// Read a single log segment from the appropriate reader.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        uri = %segment.uri,
        offset = segment.offset,
        length = segment.length,
    ),
    err,
)]
async fn read_one_persist_segment(
    reader: &impl FormatReader,
    segment: &PersistSegment,
    schema: &SchemaRef,
    projection: Option<&[&str]>,
) -> Result<RecordBatch, ColdStorageError> {
    // The reader does no predicate filtering (ADR 0023); the persist tier in
    // particular must read every segment unfiltered to preserve the
    // merge-on-read exclusion set (ADR 0022).
    Ok(reader
        .read_segment(
            &segment.uri,
            segment.offset,
            segment.length,
            schema,
            projection,
        )
        .await?)
}

impl ColdStorageClient {
    /// Like [`Self::read_persist_segments`], but clamps each segment to its own
    /// `max_commit_seq_num` ceiling before yielding.
    ///
    /// A **sibling** rather than a flag on the unbounded reader, because the two
    /// callers need genuinely different behavior and a bool toggling distinct
    /// code paths is what `docs/style-guide.md` says to split:
    ///
    /// - `audit_data` must clamp. After CHA-539 a fork's own
    ///   `cold_upsert_segments` include inherited rows clamped to the fork
    ///   position, so an audit window reaching past the fork would otherwise
    ///   emit the parent's post-fork history.
    /// - **Compaction must NOT clamp.** Its slice arithmetic walks
    ///   `cumulative += row_count` to assign each input's `(offset, length)` in
    ///   the merged file, so a short read would misalign every downstream slice.
    ///   That is safe only because a fork's inherited rows are written
    ///   `is_sealed = TRUE` and the compact input query filters
    ///   `is_sealed = FALSE`, so a clamped row is never a compaction input.
    ///   Those two decisions hold each other up; changing either requires
    ///   revisiting this split.
    ///
    /// The clamp is applied INSIDE the loop, not by zipping the output against
    /// `segments`: this stream skips empty batches, so its output is not
    /// positionally 1:1 with the input and a zip would attribute one segment's
    /// ceiling to another's rows.
    pub fn read_persist_segments_bounded<'a, R: FormatReader + 'a>(
        readers: &'a HashMap<i32, R>,
        segments: &'a [PersistSegment],
        schema: &'a SchemaRef,
        projection: Option<&'a [&'a str]>,
    ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, ColdStorageError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let mut yielded = false;
            let mut total_rows: u64 = 0;

            for segment in segments {
                let code = segment.format.as_wire_code();
                let reader = readers
                    .get(&code)
                    .ok_or(ColdStorageError::UnknownFormat(code))?;

                let batch = read_one_persist_segment(reader, segment, schema, projection).await?;
                let batch = apply_segment_seq_ceiling(&batch, segment)
                    .map_err(|e| ColdStorageError::from(FormatError::Arrow(e)))?;

                if batch.num_rows() > 0 {
                    yielded = true;
                    total_rows += batch.num_rows() as u64;
                    yield batch;
                }
            }

            tracing::debug!(
                num_segments = segments.len(),
                total_rows,
                "cold.read_persist_segments_bounded complete",
            );

            if !yielded {
                yield empty_batch(schema);
            }
        })
    }

    /// Stream log segment data, yielding one `RecordBatch` per segment.
    ///
    /// Each individual segment file fits in memory (sizes are controlled at
    /// persist/compact time), but the total set of segments for a query may
    /// not. Streaming per-segment lets callers process incrementally.
    ///
    /// Empty segments (0 rows) are silently skipped. If no segments produce
    /// rows, yields a single empty `RecordBatch` with the expected schema
    /// so callers can infer the schema from the stream.
    ///
    /// Does NOT apply a segment's `max_commit_seq_num` ceiling — see
    /// [`Self::read_persist_segments_bounded`] for the arm that does, and why
    /// compaction must keep using this one.
    ///
    /// `projection`, when `Some`, narrows each segment read to the named
    /// columns; see [`FormatReader::read_segment`].
    pub fn read_persist_segments<'a, R: FormatReader + 'a>(
        readers: &'a HashMap<i32, R>,
        segments: &'a [PersistSegment],
        schema: &'a SchemaRef,
        projection: Option<&'a [&'a str]>,
    ) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, ColdStorageError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let mut yielded = false;
            let mut total_rows: u64 = 0;

            for segment in segments {
                let code = segment.format.as_wire_code();
                let reader = readers
                    .get(&code)
                    .ok_or(ColdStorageError::UnknownFormat(code))?;

                let batch = read_one_persist_segment(reader, segment, schema, projection).await?;

                if batch.num_rows() > 0 {
                    yielded = true;
                    total_rows += batch.num_rows() as u64;
                    yield batch;
                }
            }

            tracing::debug!(
                num_segments = segments.len(),
                total_rows,
                "cold.read_persist_segments complete",
            );

            if !yielded {
                yield empty_batch(schema);
            }
        })
    }

    /// Shared body for the cold-file writers below: write the batch via the
    /// `FormatWriter` and record `rows_written` on the caller's tracing span.
    /// The public wrappers keep distinct span names (segment vs sidecar) but
    /// share this one-line body so there is a single maintenance point.
    async fn write_cold_file<W: FormatWriter>(
        writer: &W,
        uri: &str,
        batch: &RecordBatch,
    ) -> Result<usize, ColdStorageError> {
        let rows_written = writer.write(uri, batch).await?;
        tracing::Span::current().record("rows_written", rows_written);
        Ok(rows_written)
    }

    /// Write a table persist segment to cold storage via the supplied writer.
    ///
    /// Returns the number of rows written from the underlying `FormatWriter`.
    #[tracing::instrument(
        level = "debug",
        skip(writer, batch),
        fields(
            uri = %uri,
            input_rows = batch.num_rows(),
            rows_written = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn write_table_persist_segment<W: FormatWriter>(
        writer: &W,
        uri: &str,
        batch: &RecordBatch,
    ) -> Result<usize, ColdStorageError> {
        Self::write_cold_file(writer, uri, batch).await
    }

    /// Write a cold `tx_log` persist segment (CHA-507) via the supplied
    /// writer. Same physical path as persist/snapshot segments; named
    /// distinctly so the call site reads as the tx_log artifact it is.
    pub async fn write_tx_log_persist_segment<W: FormatWriter>(
        writer: &W,
        uri: &str,
        batch: &RecordBatch,
    ) -> Result<usize, ColdStorageError> {
        Self::write_cold_file(writer, uri, batch).await
    }

    /// Write a snapshot segment to cold storage via the supplied writer.
    ///
    /// Returns the number of rows written from the underlying `FormatWriter`.
    #[tracing::instrument(
        level = "debug",
        skip(writer, batch),
        fields(
            uri = %uri,
            input_rows = batch.num_rows(),
            rows_written = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn write_snapshot_segment<W: FormatWriter>(
        writer: &W,
        uri: &str,
        batch: &RecordBatch,
    ) -> Result<usize, ColdStorageError> {
        Self::write_cold_file(writer, uri, batch).await
    }

    /// Write a per-segment cold-index sidecar (CHA-412) to cold storage via the
    /// supplied writer — a sidecar is itself a cold file, written the same way
    /// as a base segment. Returns the number of index entries written.
    #[tracing::instrument(
        level = "debug",
        skip(writer, batch),
        fields(
            uri = %uri,
            input_rows = batch.num_rows(),
            rows_written = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn write_segment_index<W: FormatWriter>(
        writer: &W,
        uri: &str,
        batch: &RecordBatch,
    ) -> Result<usize, ColdStorageError> {
        Self::write_cold_file(writer, uri, batch).await
    }

    /// Read a per-segment cold-index sidecar (CHA-412) in full from cold
    /// storage. Mirrors [`read_one_persist_segment`] for a single sidecar
    /// file: dispatch a [`FormatReader`] by the sidecar's wire format and read
    /// its `(offset, length)` slice, returning the whole sorted
    /// `(key, row_offset)` batch.
    ///
    /// This is the LIFECYCLE-side read: the snapshot op has no Query-pod
    /// segment cache (CHA-252), so the CHA-454 cache-backed seek path in
    /// `penca-dl` is not reachable here. The caller (CHA-448 reverse-lookup
    /// attribution) binary-searches the returned batch with
    /// `penca_format::index::seek_row_offsets`.
    #[tracing::instrument(
        level = "debug",
        skip(readers, schema),
        fields(
            uri = %sidecar.object_uri,
            rows = tracing::field::Empty,
        ),
        err,
    )]
    pub async fn read_segment_index<R: FormatReader>(
        readers: &HashMap<i32, R>,
        sidecar: &IndexSidecar,
        schema: &SchemaRef,
    ) -> Result<RecordBatch, ColdStorageError> {
        let code = sidecar.format.as_wire_code();
        let reader = readers
            .get(&code)
            .ok_or(ColdStorageError::UnknownFormat(code))?;
        let batch = reader
            .read_segment(
                &sidecar.object_uri,
                Some(sidecar.offset),
                Some(sidecar.length),
                schema,
                None,
            )
            .await?;
        tracing::Span::current().record("rows", batch.num_rows());
        Ok(batch)
    }

    /// Delete a segment file from cold storage via the supplied writer.
    ///
    /// When `missing_ok` is true, a missing file is not an error.
    #[tracing::instrument(
        level = "debug",
        skip(writer),
        fields(
            uri = %uri,
            missing_ok,
        ),
        err,
    )]
    pub async fn delete_segment<W: FormatWriter>(
        writer: &W,
        uri: &str,
        missing_ok: bool,
    ) -> Result<(), ColdStorageError> {
        Ok(writer.delete(uri, missing_ok).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;
    use penca_core::{Format, IndexSidecar};
    use penca_format::reader::AnyFormatReader;

    use super::{ColdStorageClient, ColdStorageError};

    /// Pin the Penca-owned format-dispatch miss path: a sidecar whose wire
    /// format has no registered reader must surface `UnknownFormat` (before any
    /// I/O), not panic or hang. The happy-path slice read is exercised
    /// end-to-end by the CHA-448 carry-forward reverse-lookup integration tests.
    #[tokio::test]
    async fn read_segment_index_unknown_format_errors() {
        let readers: HashMap<i32, AnyFormatReader> = HashMap::new();
        let sidecar = IndexSidecar {
            object_uri: "memory://sidecar".to_string(),
            offset: 0,
            length: 0,
            format: Format::Parquet,
            segment_index_uuid: "idx".to_string(),
            size_bytes: 0,
        };
        let schema = penca_format::index::segment_index_schema(&[DataType::Utf8]);
        let err = ColdStorageClient::read_segment_index(&readers, &sidecar, &schema)
            .await
            .expect_err("missing reader must error");
        assert!(
            matches!(err, ColdStorageError::UnknownFormat(code) if code == Format::Parquet.as_wire_code()),
            "expected UnknownFormat, got {err:?}",
        );
    }
}

#[cfg(test)]
mod ceiling_tests {
    use super::*;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// The ceiling is prescriptive: a row claiming less than its file holds must
    /// emit only the rows at or below it. Covers the three reachable shapes —
    /// clamped (below content), inert (at content), and unbounded (`None`, the
    /// tx_log carriers) — plus the projected-away case, which is why the filter
    /// runs on the full schema.
    #[test]
    fn segment_seq_ceiling_clamps_only_when_it_claims_less() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_uuid", DataType::Utf8, false),
            Field::new("commit_seq_num", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
            ],
        )
        .unwrap();
        let seg = |ceiling| PersistSegment {
            max_commit_seq_num: ceiling,
            ..PersistSegment::default()
        };

        // Clamped below the file's content: only rows at or below survive.
        let clamped = apply_segment_seq_ceiling(&batch, &seg(Some(20))).unwrap();
        assert_eq!(clamped.num_rows(), 2, "ceiling 20 must drop the seq-30 row");

        // Inert: the recorded max IS the file's max, the ordinary case.
        assert_eq!(
            apply_segment_seq_ceiling(&batch, &seg(Some(30)))
                .unwrap()
                .num_rows(),
            3,
        );

        // Unbounded — the tx_log carriers, built via `..default()`.
        assert_eq!(
            apply_segment_seq_ceiling(&batch, &seg(None))
                .unwrap()
                .num_rows(),
            3,
        );

        // Below every row: an empty batch, not an error.
        assert_eq!(
            apply_segment_seq_ceiling(&batch, &seg(Some(0)))
                .unwrap()
                .num_rows(),
            0,
        );

        // A ceiling that cannot be applied is an ERROR, not a pass-through: the
        // read path widens the projection to keep `commit_seq_num` precisely so
        // the bound is always enforceable, and passing the batch through here
        // would leak exactly the rows the ceiling exists to hide.
        let no_seq = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "row_uuid",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                "a", "b", "c",
            ]))],
        )
        .unwrap();
        assert!(
            apply_segment_seq_ceiling(&no_seq, &seg(Some(0))).is_err(),
            "an unenforceable ceiling must fail loudly, not silently pass rows through",
        );
        // ...but a carrier with no ceiling still passes through unfiltered.
        assert_eq!(
            apply_segment_seq_ceiling(&no_seq, &seg(None))
                .unwrap()
                .num_rows(),
            3,
        );
    }
}
