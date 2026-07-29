//! Lance format reader using `lance-file` crate + `object_store`.
//!
//! Uses `lance_file::reader::FileReader` directly — the same API that
//! Python's `lance.file.LanceFileReader` wraps via PyO3.

use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::reader::{FileReader, FileReaderOptions, ReaderProjection};
use lance_io::ReadBatchParams;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};

use super::{
    FormatError, FormatReader, empty_batch, null_fill_to_schema, present_columns, project_schema,
};
use crate::uri::uri_to_object_path;

/// Lance reader backed by an [`object_store::ObjectStore`].
pub struct LanceFormatReader {
    object_store: Arc<lance_io::object_store::ObjectStore>,
    base_uri: String,
}

impl LanceFormatReader {
    pub fn new(object_store: Arc<lance_io::object_store::ObjectStore>, base_uri: String) -> Self {
        Self {
            object_store,
            base_uri,
        }
    }

    /// Open a FileReader for a given URI.
    async fn open_file(&self, uri: &str) -> Result<FileReader, FormatError> {
        let path = uri_to_object_path(&self.base_uri, uri);
        let config = SchedulerConfig::default_for_testing();
        let scan_scheduler = ScanScheduler::new(self.object_store.clone(), config);
        let file_scheduler = scan_scheduler.open_file(&path, &Default::default()).await?;

        let reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await?;

        Ok(reader)
    }

    /// Build a `ReaderProjection` from column names against the file's schema,
    /// or `None` to read every column.
    fn build_projection(
        reader: &FileReader,
        projection: Option<&[&str]>,
    ) -> Result<Option<ReaderProjection>, FormatError> {
        let Some(names) = projection else {
            return Ok(None);
        };
        let metadata = reader.metadata();
        let proj = ReaderProjection::from_column_names(
            metadata.version(),
            metadata.file_schema.as_ref(),
            names,
        )?;
        Ok(Some(proj))
    }

    /// Read a FileReader stream slice as a single RecordBatch.
    ///
    /// `params` selects the row range (`RangeFull` for whole-file,
    /// `Range(start..end)` for a slice). When `projection` is `Some`,
    /// dispatches through `read_stream_projected`; otherwise through
    /// `read_stream`.
    async fn read_with_params(
        reader: &FileReader,
        output_schema: &SchemaRef,
        projection: Option<ReaderProjection>,
        params: ReadBatchParams,
    ) -> Result<RecordBatch, FormatError> {
        let batch_size = 8192u32;
        let batch_readahead = 16u32;
        // Penca does no format-internal predicate filtering — row filtering
        // is owned by DataFusion (ADR 0023). `read_stream`/`read_stream_projected`
        // still require a `FilterExpression` argument, so we always pass
        // `no_filter()`. (lance-file 4.0.0's core decoders ignore it regardless;
        // Any future filter-aware decoding would still
        // plug in through DataFusion rather than this argument.)
        let mut stream = match projection {
            Some(p) => reader.read_stream_projected(
                params,
                batch_size,
                batch_readahead,
                p,
                FilterExpression::no_filter(),
            )?,
            None => reader.read_stream(
                params,
                batch_size,
                batch_readahead,
                FilterExpression::no_filter(),
            )?,
        };

        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        if batches.is_empty() {
            return Ok(empty_batch(output_schema));
        }
        Ok(concat_batches(output_schema, &batches)?)
    }
}

impl FormatReader for LanceFormatReader {
    /// Read one Lance segment, optionally slicing to `(offset, length)`.
    ///
    /// `compact_persist_segments` merges N small files into one and
    /// re-points each input metadata row at a `(merged_uri, offset,
    /// length)` slice of the merged file; packed snapshot
    /// files address one row range per partition the same way
    /// (CHA-404).
    ///
    /// No predicate is pushed into the read — row filtering is owned by
    /// DataFusion (ADR 0023); see `read_with_params`.
    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            offset = ?offset,
            length = ?length,
            format = "lance",
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
        let reader = self.open_file(uri).await?;
        // Project only the requested columns physically present in this
        // segment file, then null-fill the rest to `output_schema`. A column
        // added by a later `ALTER TABLE ADD COLUMN` is absent from an older
        // segment; `ReaderProjection::from_column_names` would otherwise reject
        // the missing name. `present_names` is never empty: every segment carries
        // `row_uuid` and every read requests it, so the read yields a row count.
        let file_arrow: SchemaRef = Arc::new(arrow::datatypes::Schema::from(
            reader.metadata().file_schema.as_ref(),
        ));
        let column_names: Vec<&str> = match projection {
            Some(cols) => cols.to_vec(),
            None => schema.fields().iter().map(|f| f.name().as_str()).collect(),
        };
        let present_names = present_columns(&file_arrow, &column_names);
        let present_schema = project_schema(schema, Some(&present_names))?;
        let proj = Self::build_projection(&reader, Some(&present_names))?;
        let params = match (offset, length) {
            (Some(offset), Some(length)) => {
                ReadBatchParams::Range(offset as usize..offset as usize + length as usize)
            }
            _ => ReadBatchParams::RangeFull,
        };
        let present = Self::read_with_params(&reader, &present_schema, proj, params).await?;
        let out = null_fill_to_schema(&present, &output_schema)?;
        tracing::Span::current().record("rows", out.num_rows());
        Ok(out)
    }
}
