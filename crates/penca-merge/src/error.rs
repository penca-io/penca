//! Error type for merge-on-read.

use penca_dl::driver::DlError;
use penca_storage_hot::HotStorageError;

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error(transparent)]
    HotStorage(#[from] HotStorageError),

    #[error(transparent)]
    Dl(#[from] DlError),

    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),

    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("invalid plan: {0}")]
    InvalidPlan(String),

    #[error("missing column: {0}")]
    MissingColumn(String),

    #[error("primary key '{pk}' not in user_schema")]
    SchemaMismatch { pk: String },
}
