//! Lance format writer using `lance-file` crate + `object_store`.
//!
//! Uses `lance_file::writer::FileWriter::create_file_with_batches` — the same
//! API that Python's `lance.file.LanceFileWriter` wraps via PyO3.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use lance_core::datatypes::Schema as LanceSchema;
use lance_file::writer::{FileWriter, FileWriterOptions};

use super::{FormatWriter, delete_with_missing_ok};
use crate::reader::FormatError;
use crate::uri::uri_to_object_path;

/// Lance writer backed by a [`lance_io::object_store::ObjectStore`].
pub struct LanceFormatWriter {
    object_store: Arc<lance_io::object_store::ObjectStore>,
    /// Separate reference to the underlying object_store for delete operations.
    raw_store: Arc<dyn object_store::ObjectStore>,
    base_uri: String,
}

impl LanceFormatWriter {
    pub fn new(
        object_store: Arc<lance_io::object_store::ObjectStore>,
        raw_store: Arc<dyn object_store::ObjectStore>,
        base_uri: String,
    ) -> Self {
        Self {
            object_store,
            raw_store,
            base_uri,
        }
    }
}

impl FormatWriter for LanceFormatWriter {
    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            format = "lance",
            rows = batch.num_rows(),
        ),
    )]
    async fn write(&self, uri: &str, batch: &RecordBatch) -> Result<usize, FormatError> {
        let path = uri_to_object_path(&self.base_uri, uri);
        let lance_schema = LanceSchema::try_from(batch.schema().as_ref())?;
        let rows = batch.num_rows();

        FileWriter::create_file_with_batches(
            &self.object_store,
            &path,
            lance_schema,
            std::iter::once(batch.clone()),
            FileWriterOptions::default(),
        )
        .await?;

        Ok(rows)
    }

    #[tracing::instrument(
        skip_all,
        fields(
            uri = %uri,
            format = "lance",
            missing_ok = missing_ok,
        ),
    )]
    async fn delete(&self, uri: &str, missing_ok: bool) -> Result<(), FormatError> {
        delete_with_missing_ok(
            self.raw_store.as_ref(),
            &uri_to_object_path(&self.base_uri, uri),
            missing_ok,
        )
        .await
    }
}
