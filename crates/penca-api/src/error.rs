//! Unified error type for the API orchestration layer.

use penca_dl::driver::DlError;
use penca_format::reader::FormatError;
use penca_merge::MergeError;
use penca_storage_cold::ColdStorageError;
use penca_storage_hot::HotStorageError;
use penca_storage_meta::MetadataError;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Metadata(#[from] MetadataError),

    #[error(transparent)]
    HotStorage(#[from] HotStorageError),

    #[error(transparent)]
    ColdStorage(#[from] ColdStorageError),

    /// Failure in the DataFusion-free direct point-read seek
    /// (`DatafusionDlDriver::seek_snapshot_point`). Maps to gRPC `INTERNAL`.
    #[error(transparent)]
    Dl(#[from] DlError),

    #[error(transparent)]
    Merge(#[from] MergeError),

    #[error(transparent)]
    Format(#[from] FormatError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    Uuid(#[from] uuid::Error),

    /// A concurrency conflict the caller can resolve by reissuing: the
    /// operation made no changes and a retry is safe. Distinct from
    /// `FailedPrecondition`, which means the request itself was wrong.
    #[error("aborted: {0}")]
    Aborted(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("failed precondition: {0}")]
    FailedPrecondition(String),

    #[error("not implemented: {0}")]
    Unimplemented(String),

    /// ADR 0019 — `read_data`/`audit_data` ran past the system-wide cap.
    /// Maps to gRPC `RESOURCE_EXHAUSTED`.
    #[error("query exceeded query_timeout_seconds: {0}")]
    QueryTimeout(String),

    /// Preferred over `.expect()` / `.unwrap()` in library code so an
    /// invariant bug surfaces as a typed error instead of a panic.
    #[error("internal invariant violated: {0}")]
    Internal(String),
}

/// Covers the driver-level paths (e.g. `PgDriver::advisory_lock`) that raise
/// raw sqlx errors; application queries already surface as `MetadataError`.
impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::Metadata(err.into())
    }
}
