//! Parquet format reader using arrow-rs `parquet` crate + `object_store`.
//!
//! Reads go through the async range reader (`ParquetObjectReader` +
//! `ParquetRecordBatchStreamBuilder`), so column projection and the
//! `(offset, length)` slice drive selective byte-range fetches against object
//! storage rather than a whole-file slurp. The reader does no predicate
//! filtering — row filtering lives in DataFusion (ADR 0023).

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use super::{
    FormatError, FormatReader, empty_batch, present_columns, project_schema, requested_columns,
    shape_to_schema,
};
use crate::uri::uri_to_object_path;

/// A [`RowSelection`] that picks the physical row range `[offset, offset + length)`.
///
/// A sliced compacted segment is read by skipping the rows before
/// the slice and selecting exactly `length` rows.
fn slice_selection(offset: usize, length: usize) -> RowSelection {
    let mut selectors = Vec::with_capacity(2);
    if offset > 0 {
        selectors.push(RowSelector::skip(offset));
    }
    selectors.push(RowSelector::select(length));
    RowSelection::from(selectors)
}

/// Parquet reader backed by an [`ObjectStore`].
pub struct ParquetFormatReader {
    store: Arc<dyn ObjectStore>,
    base_uri: String,
}

impl ParquetFormatReader {
    pub fn new(store: Arc<dyn ObjectStore>, base_uri: String) -> Self {
        Self { store, base_uri }
    }

    /// The read itself, in the file's own schema. `projection`, when `Some`,
    /// is narrowed to the columns physically present and pushed down as a
    /// [`ProjectionMask`] so only those byte ranges are fetched; `None` reads
    /// every column the file has.
    ///
    /// Shaping to a caller's schema is the caller's job — `read_segment` runs
    /// it immediately, `read_segment_native`'s callers run it after their
    /// cache lookup.
    async fn read_in_file_schema(
        &self,
        uri: &str,
        offset: Option<i64>,
        length: Option<i64>,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch, FormatError> {
        let path = uri_to_object_path(&self.base_uri, uri);
        let reader = ParquetObjectReader::new(self.store.clone(), path);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let file_schema = builder.schema().clone();

        // A column added by a later `ALTER TABLE ADD COLUMN` is absent from an
        // older segment; it is null-filled by the shaping tail rather than
        // erroring the projection here. `present_names` is never empty: every
        // Penca segment carries `row_uuid` and every read requests it
        // (`snapshot_read_schema` prepends it), so the read below always
        // yields a row count to null-fill against.
        let read_schema = match projection {
            Some(names) => {
                let present_names = present_columns(&file_schema, names);
                let parquet_schema = builder.parquet_schema().clone();
                let mask = ProjectionMask::columns(&parquet_schema, present_names.iter().copied());
                builder = builder.with_projection(mask);
                project_schema(&file_schema, Some(&present_names))?
            }
            None => file_schema,
        };

        if let (Some(offset), Some(length)) = (offset, length) {
            builder = builder.with_row_selection(slice_selection(offset as usize, length as usize));
        }

        let mut stream = builder.build()?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        if batches.is_empty() {
            return Ok(empty_batch(&read_schema));
        }
        Ok(concat_batches(&batches[0].schema(), &batches)?)
    }
}

impl FormatReader for ParquetFormatReader {
    /// Read one parquet segment, optionally sliced to `(offset, length)`.
    ///
    /// `compact_persist_segments` merges N small files into one and
    /// re-points each input metadata row at a `(merged_uri, offset, length)`
    /// slice of the merged file, and packed snapshot files address
    /// one row range per partition, so the slice is expressed as a
    /// [`RowSelection`]. No predicate is pushed into the read — DataFusion
    /// filters the returned rows (ADR 0023).
    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            offset = ?offset,
            length = ?length,
            format = "parquet",
            projection_cols = ?projection.map(|p| p.len()),
            rows = tracing::field::Empty,
        ),
    )]
    async fn read_segment(
        &self,
        uri: &str,
        offset: Option<i64>,
        length: Option<i64>,
        schema: &SchemaRef,
        projection: Option<&[&str]>,
    ) -> Result<RecordBatch, FormatError> {
        let column_names = requested_columns(schema, projection);
        let present = self
            .read_in_file_schema(uri, offset, length, Some(&column_names))
            .await?;
        let out = shape_to_schema(&present, schema, projection)?;
        tracing::Span::current().record("rows", out.num_rows());
        Ok(out)
    }

    #[tracing::instrument(
        skip_all,
        fields(uri = %uri, offset = ?offset, length = ?length, format = "parquet"),
    )]
    async fn read_segment_native(
        &self,
        uri: &str,
        offset: Option<i64>,
        length: Option<i64>,
    ) -> Result<RecordBatch, FormatError> {
        self.read_in_file_schema(uri, offset, length, None).await
    }
}
