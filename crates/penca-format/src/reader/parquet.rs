//! Parquet format reader using arrow-rs `parquet` crate + `object_store`.
//!
//! This is the Rust port of `packages/penca/src/penca/lib/format/reader/parquet_reader.py`.
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
    FormatError, FormatReader, empty_batch, null_fill_to_schema, present_columns, project_schema,
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
        let output_schema = project_schema(schema, projection)?;
        let column_names: Vec<&str> = match projection {
            Some(cols) => cols.to_vec(),
            None => schema.fields().iter().map(|f| f.name().as_str()).collect(),
        };

        let path = uri_to_object_path(&self.base_uri, uri);
        let reader = ParquetObjectReader::new(self.store.clone(), path);
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

        let parquet_schema = builder.parquet_schema().clone();
        // Project only the requested columns that physically exist in
        // this segment file. A column added by a later `ALTER TABLE ADD COLUMN`
        // is absent from an older segment; it is null-filled to `output_schema`
        // after the read rather than erroring the projection. `present_names`
        // is never empty: every Penca segment carries `row_uuid` and every
        // read requests it (`snapshot_read_schema` prepends it), so the read
        // below always yields a row count to null-fill against.
        let present_names = present_columns(builder.schema(), &column_names);
        let output_mask = ProjectionMask::columns(&parquet_schema, present_names.iter().copied());
        builder = builder.with_projection(output_mask);

        if let (Some(offset), Some(length)) = (offset, length) {
            builder = builder.with_row_selection(slice_selection(offset as usize, length as usize));
        }

        let mut stream = builder.build()?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        if batches.is_empty() {
            tracing::Span::current().record("rows", 0);
            return Ok(empty_batch(&output_schema));
        }

        let present = concat_batches(&batches[0].schema(), &batches)?;
        let out = null_fill_to_schema(&present, &output_schema)?;
        tracing::Span::current().record("rows", out.num_rows());
        Ok(out)
    }
}
