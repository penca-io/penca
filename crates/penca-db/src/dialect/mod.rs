//! SQL dialect extension for OLTP databases.
//!
//! The base [`Dialect`] trait (identifier quoting, `latest_per_partition`)
//! lives in `penca-sql`, shared with `penca-dl` for the cold tier.
//! [`DbDialect`] extends it with OLTP concerns — DDL type mapping and
//! engine-specific time expressions — implemented by [`pg::PgDialect`].

pub mod pg;

use arrow::datatypes::DataType;
pub use penca_sql::{
    CompositeMergeResolution, Dialect, build_composite_merge_resolution, leading_comma_if_nonempty,
    lex_compare_predicate, qualify_user_cols, row_uuid_in_clause, row_uuid_in_clause_after,
};

/// OLTP dialect — a full-featured SQL database that handles DDL and
/// Arrow-to-SQL type mapping on top of the base [`Dialect`] contract.
pub trait DbDialect: Dialect {
    /// Map an Arrow data type to a native SQL column type.
    fn arrow_type_to_sql(arrow_type: &DataType) -> Result<String, ArrowTypeError>;

    /// SQL expression for the current database time in epoch microseconds.
    fn microsecond_epoch() -> &'static str;
}

/// Error returned when an Arrow type has no SQL mapping.
#[derive(Debug, thiserror::Error)]
#[error("unsupported Arrow type: {0}")]
pub struct ArrowTypeError(pub DataType);
