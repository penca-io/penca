//! Cold storage format writers.
//!
//! [`FormatWriter`] defines the interface for writing columnar segment files
//! to object storage. Implementations handle format-specific serialization.
//!
//! This is the Rust port of `packages/penca/src/penca/lib/format/writer/__init__.py`.

pub mod lance;
pub mod parquet;

use std::future::Future;

use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::reader::FormatError;

/// Delete a path, treating `NotFound` as success when `missing_ok` is true.
///
/// Shared by `ParquetFormatWriter::delete` and `LanceFormatWriter::delete`,
/// which delegate to different `ObjectStore` instances but want the same
/// missing-ok policy.
pub(crate) async fn delete_with_missing_ok(
    store: &dyn ObjectStore,
    path: &Path,
    missing_ok: bool,
) -> Result<(), FormatError> {
    match store.delete(path).await {
        Ok(()) => Ok(()),
        Err(object_store::Error::NotFound { .. }) if missing_ok => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Writes columnar segment files to cold storage.
///
/// Methods return `impl Future<...> + Send` instead of using `async fn`
/// so that the returned futures are guaranteed `Send`. This allows callers
/// (e.g. tonic gRPC servers) to use the trait with `Send`-bounded streams.
pub trait FormatWriter: Send + Sync {
    /// Write a `RecordBatch` to object storage.
    ///
    /// Returns the number of rows written. The on-disk serialized size
    /// is deliberately not returned: segment `size_bytes` is the
    /// uncompressed in-memory footprint, sourced from the
    /// chunker, not from the writer.
    fn write(
        &self,
        uri: &str,
        batch: &RecordBatch,
    ) -> impl Future<Output = Result<usize, FormatError>> + Send;

    /// Delete a file from object storage.
    ///
    /// If `missing_ok` is true, `FileNotFound` errors are silently ignored.
    fn delete(
        &self,
        uri: &str,
        missing_ok: bool,
    ) -> impl Future<Output = Result<(), FormatError>> + Send;
}

/// Enum dispatch writer supporting both Parquet and Lance formats.
///
/// Used when the write format is selected at runtime via config.
/// Implements `FormatWriter` by delegating to the inner concrete writer.
pub enum AnyFormatWriter {
    Parquet(parquet::ParquetFormatWriter),
    Lance(lance::LanceFormatWriter),
}

impl FormatWriter for AnyFormatWriter {
    async fn write(&self, uri: &str, batch: &RecordBatch) -> Result<usize, FormatError> {
        match self {
            Self::Parquet(w) => w.write(uri, batch).await,
            Self::Lance(w) => w.write(uri, batch).await,
        }
    }

    async fn delete(&self, uri: &str, missing_ok: bool) -> Result<(), FormatError> {
        match self {
            Self::Parquet(w) => w.delete(uri, missing_ok).await,
            Self::Lance(w) => w.delete(uri, missing_ok).await,
        }
    }
}
