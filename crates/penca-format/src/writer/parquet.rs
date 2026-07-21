//! Parquet format writer using arrow-rs `parquet` crate + `object_store`.
//!
//! This is the Rust port of `packages/penca/src/penca/lib/format/writer/parquet_writer.py`.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;

use super::{FormatWriter, delete_with_missing_ok};
use crate::reader::FormatError;
use crate::uri::uri_to_object_path;

/// Parquet writer backed by an [`ObjectStore`].
pub struct ParquetFormatWriter {
    store: Arc<dyn ObjectStore>,
    base_uri: String,
}

impl ParquetFormatWriter {
    pub fn new(store: Arc<dyn ObjectStore>, base_uri: String) -> Self {
        Self { store, base_uri }
    }
}

impl FormatWriter for ParquetFormatWriter {
    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            format = "parquet",
            rows = batch.num_rows(),
        ),
    )]
    async fn write(&self, uri: &str, batch: &RecordBatch) -> Result<usize, FormatError> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;

        let rows_written = batch.num_rows();
        let path = uri_to_object_path(&self.base_uri, uri);
        self.store.put(&path, buf.into()).await?;

        Ok(rows_written)
    }

    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            format = "parquet",
            missing_ok = missing_ok,
        ),
    )]
    async fn delete(&self, uri: &str, missing_ok: bool) -> Result<(), FormatError> {
        delete_with_missing_ok(
            self.store.as_ref(),
            &uri_to_object_path(&self.base_uri, uri),
            missing_ok,
        )
        .await
    }
}
