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

    /// CHA-476: a failure in the DataFusion-free direct point-read seek
    /// (`DatafusionDlDriver::seek_snapshot_point`). Maps to a gRPC internal
    /// status (the `_` arm in `api_error_to_status`), like the cold-read
    /// errors it wraps.
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

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("not found: {0}")]
    NotFound(String),

    /// CHA-236 — a Create* / Update* hit a name-uniqueness collision
    /// (e.g. `CreateTable` against an existing `(branch, schema, name)`,
    /// or `UpdateCatalog` renaming to a name another catalog already
    /// holds). The gRPC layer maps this to `ALREADY_EXISTS`.
    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("failed precondition: {0}")]
    FailedPrecondition(String),

    #[error("not implemented: {0}")]
    Unimplemented(String),

    /// ADR 0019 / CHA-233 — `read_data` or `audit_data` ran past the
    /// system-wide cap. The message names the cap (`query_timeout_seconds`)
    /// and the retry pattern; the gRPC layer maps it to
    /// `RESOURCE_EXHAUSTED`.
    #[error("query exceeded query_timeout_seconds: {0}")]
    QueryTimeout(String),

    /// An internal invariant was violated — e.g. a scope constructor
    /// failed to populate a field its public name promises. The gRPC
    /// layer maps this to `INTERNAL`. Preferred over `.expect()` /
    /// `.unwrap()` in library code so an invariant bug surfaces as a
    /// typed error instead of a panic.
    #[error("internal invariant violated: {0}")]
    Internal(String),
}

/// Lift raw sqlx errors (e.g. from `PgDriver::advisory_lock` while
/// acquiring/releasing the lock) into the metadata variant. Application
/// queries already go through `LifecycleManager` and surface as
/// `MetadataError`; this just covers the driver-level paths that don't.
impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::Metadata(err.into())
    }
}
