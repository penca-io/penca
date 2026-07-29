//! Database driver abstraction.
//!
//! [`DbDriver`] defines the async query execution interface. Implementations
//! manage connection pooling, transactions, and result streaming.

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;

pub mod pg;

/// Value type for bind parameters in parameterized queries.
///
/// Only system metadata types — user data goes through Arrow RecordBatch,
/// never through bind parameters. This set is bounded by penca's own
/// schema design (UUIDs, timestamps, comments, flags), not by the full
/// database type catalog.
///
/// Each variant is *typed* so the driver encodes it with the correct
/// native database type from the start — a `Uuid` binds to a PG
/// `uuid` column, an `Int32` to `int4`, an `Int64` to `int8` — instead of
/// binding everything as `text` and relying on `$N::uuid` / `$N::integer`
/// casts scattered through every SQL string. A `Null` carries its target
/// [`SqlType`] for the same reason: a typed NULL still needs a type so PG
/// accepts it for a non-`text` column. This enum stays backend-agnostic —
/// the mapping to a concrete database type lives in each driver's bind
/// builder (e.g. `build_pg_args`), never here.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// Text string (names, comments, authors).
    Text(String),
    /// UUID — binds to a native `uuid` column.
    Uuid(uuid::Uuid),
    /// 32-bit integer (`int4` columns).
    Int32(i32),
    /// 64-bit integer (`int8` — timestamps in microseconds, counts, TTLs).
    Int64(i64),
    /// Boolean flag.
    Bool(bool),
    /// Raw bytes (Arrow schema, binary blobs).
    Bytes(Vec<u8>),
    /// PG `text[]` (partition_keys / clustering_keys / primary_keys on
    /// `__penca_system__.tables`). Bound natively via sqlx — keeps the
    /// SQL string identical across calls so PG's plan cache works.
    TextArray(Vec<String>),
    /// SQL NULL with a known target type, so the driver binds a typed NULL
    /// (`Option::<T>::None`) that the database accepts for that column.
    Null(SqlType),
}

/// The target type of a [`SqlValue::Null`] bind.
///
/// Backend-agnostic — each driver maps these to the right typed
/// `Option::<T>::None` so a NULL is accepted for a non-`text` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Text,
    Uuid,
    Int32,
    Int64,
    Bool,
    Bytes,
    /// PG `text[]` (nullable array columns, e.g.
    /// `table_snapshot_index_metadata.key_columns`).
    TextArray,
}

impl SqlValue {
    /// `Some(v)` → [`SqlValue::Int64`], `None` → typed NULL.
    pub fn from_opt_i64(value: Option<i64>) -> Self {
        match value {
            Some(v) => SqlValue::Int64(v),
            None => SqlValue::Null(SqlType::Int64),
        }
    }

    /// `Some(v)` → [`SqlValue::Uuid`], `None` → typed NULL.
    pub fn from_opt_uuid(value: Option<uuid::Uuid>) -> Self {
        match value {
            Some(v) => SqlValue::Uuid(v),
            None => SqlValue::Null(SqlType::Uuid),
        }
    }

    /// Parse a UUID string into a [`SqlValue::Uuid`], failing fast at the
    /// construction site. Callers hold system-generated UUID strings (from
    /// `penca_core::naming`); a malformed value surfaces here, not at bind
    /// time, via the propagated [`uuid::Error`].
    pub fn uuid_str(value: &str) -> Result<Self, uuid::Error> {
        Ok(SqlValue::Uuid(uuid::Uuid::parse_str(value)?))
    }
}

/// Format system-generated identifiers as a SQL text array literal
/// for use in `= ANY(...)` clauses.
///
/// # Safety boundary
///
/// Callers must only pass system-generated values (deterministic UUIDs,
/// UUID-prefixed table names from `penca_core::naming`). Never pass
/// user-supplied strings — they could contain SQL injection payloads.
/// For user-supplied arrays use [`SqlValue::TextArray`] with a
/// `$N::text[]` bind parameter instead.
/// Visibility is intentionally `pub` so sibling crates (`penca-storage-meta`)
/// can reuse it. Never expose to user-facing APIs.
pub fn format_sql_text_array(values: &[&str]) -> String {
    let inner: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("ARRAY[{}]::text[]", inner.join(","))
}

/// Format system-generated UUIDs as a SQL `uuid[]` array literal for
/// use in `= ANY(...)` clauses against `uuid`-typed columns.
///
/// Emitting `::uuid[]` directly (instead of `::text[]` + column-side
/// casts) keeps Postgres's type resolution on the column side and
/// avoids per-row `uuid → text` conversions when matching.
///
/// Safety boundary mirrors [`format_sql_text_array`]: callers must pass
/// system-generated UUID strings only.
pub fn format_sql_uuid_array(values: &[&str]) -> String {
    let inner: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
    format!("ARRAY[{}]::uuid[]", inner.join(","))
}

/// Executes queries against a transactional database.
///
/// All methods accept raw SQL strings. Table and column identifiers are
/// quoted by the [`Dialect`] layer before being interpolated into the SQL
/// string; values use the database's native bind parameter syntax (e.g.
/// `$1`/`$2` for Postgres).
///
/// The associated [`Row`](DbDriver::Row) type is set by each implementation
/// (e.g. `PgRow` for Postgres). This keeps the trait decoupled from any
/// specific database backend.
///
/// [`Dialect`]: crate::dialect::Dialect
pub trait DbDriver: Send + Sync {
    /// The row type returned by queries.
    type Row: Send;

    /// Execute a query and return all result rows.
    fn execute(
        &self,
        query: &str,
    ) -> impl Future<Output = Result<Vec<Self::Row>, sqlx::Error>> + Send;

    /// Execute a statement that returns no rows (INSERT/UPDATE/DELETE/DDL).
    fn execute_no_result(
        &self,
        query: &str,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Execute a statement for each query string in the slice.
    ///
    /// Runs all executions on a single connection for efficiency.
    fn execute_many(
        &self,
        queries: &[String],
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Execute a parameterized query and return all result rows.
    fn execute_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> impl Future<Output = Result<Vec<Self::Row>, sqlx::Error>> + Send;

    /// Execute a parameterized statement that returns no rows.
    fn execute_no_result_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Execute a parameterized query and return at most one row.
    fn fetch_optional(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> impl Future<Output = Result<Option<Self::Row>, sqlx::Error>> + Send;

    /// Release any resources held by this driver (e.g. connection pool).
    fn close(&self) -> impl Future<Output = ()> + Send;

    /// Stream rows from a query without loading all results into memory.
    ///
    /// Both pool-backed and transaction-scoped drivers implement true
    /// server-side cursor streaming via PostgreSQL's portal/cursor protocol.
    /// For transaction drivers, the connection is held for the stream's
    /// lifetime, which is safe because transactions are used sequentially.
    fn fetch_stream<'a>(
        &'a self,
        query: &'a str,
        params: &'a [SqlValue],
    ) -> Pin<Box<dyn Stream<Item = Result<Self::Row, sqlx::Error>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::{SqlType, SqlValue};

    // The Penca-owned mapping is the `from_opt_*` helpers and the
    // variant→SqlType pairing for typed NULLs; sqlx's own encode path is
    // deliberately not under test here.

    #[test]
    fn from_opt_i64_some_is_int64() {
        assert_eq!(SqlValue::from_opt_i64(Some(9)), SqlValue::Int64(9));
    }

    #[test]
    fn from_opt_i64_none_is_typed_null() {
        assert_eq!(SqlValue::from_opt_i64(None), SqlValue::Null(SqlType::Int64));
    }

    #[test]
    fn from_opt_uuid_some_is_uuid() {
        let u = uuid::Uuid::nil();
        assert_eq!(SqlValue::from_opt_uuid(Some(u)), SqlValue::Uuid(u));
    }

    #[test]
    fn from_opt_uuid_none_is_typed_null() {
        assert_eq!(SqlValue::from_opt_uuid(None), SqlValue::Null(SqlType::Uuid));
    }
}
