//! Hot storage client for the Postgres-backed transactional tier.
//!
//! This is the Rust port of `packages/penca/src/penca/lib/storage/hot.py`.
//!
//! [`HotStorageClient`] provides typed methods for reading and writing recent,
//! unpersisted data in Postgres. All methods accept `&impl DbDriver<Row = PgRow>`,
//! supporting both pool and transaction drivers with true server-side cursor
//! streaming.
//!
//! **Note:** This client is currently coupled to Postgres — it generates
//! Postgres-specific SQL (`PgDialect`, `unnest`, `ARRAY[]::uuid[]`) and
//! constrains `Row = PgRow`. See CHA-117 for abstracting the dialect.

use arrow::datatypes::DataType;

mod audit;
mod data;
mod merge;
mod persist;
mod query;
mod row_codec;
mod sql_literal;
mod tx;

pub use audit::{AuditRowFilter, audit_delete_schema, audit_upsert_schema};
pub use query::{execute_query_as_batch, stream_query_as_batches};
pub use tx::{CommittedTx, TxStatus};

#[derive(Debug, thiserror::Error)]
pub enum HotStorageError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Uuid(#[from] uuid::Error),
    #[error("unsupported Arrow data type for SQL conversion: {0}")]
    UnsupportedType(DataType),
    #[error("primary key '{pk}' not in user_schema")]
    SchemaMismatch { pk: String },
}

/// Client for the Postgres-backed hot storage tier.
///
/// Stateless unit struct — all methods take an explicit driver.
/// The caller owns the driver and transaction lifecycle.
pub struct HotStorageClient;
