//! PostgreSQL driver using sqlx with connection pooling.
//!
//! [`PgDriver`] wraps a `sqlx::PgPool` and implements [`DbDriver`]. Each
//! operation checks out a connection from the pool, executes the query, and
//! returns the connection. Transactions are scoped via [`PgTransactionDriver`],
//! which holds a `sqlx::Transaction` and commits/rolls back on demand.

use std::pin::Pin;

use futures_core::Stream;
use futures_util::StreamExt;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgConnection, PgPool, PgPoolOptions, PgRow};
use sqlx::{Arguments, Connection, Executor, Postgres};
use tokio::runtime::Handle;
use tokio::sync::Mutex;

use super::{DbDriver, SqlType, SqlValue};

/// Build a `PgArguments` from a slice of [`SqlValue`].
///
/// This is the bridge between penca's database-agnostic [`SqlValue`] enum
/// and sqlx's typed bind parameters. Each variant maps to the appropriate
/// Postgres type via sqlx's `Encode` trait.
fn build_pg_args(params: &[SqlValue]) -> Result<PgArguments, sqlx::Error> {
    let mut args = PgArguments::default();
    for param in params {
        match param {
            SqlValue::Text(s) => args.add(s.clone()).map_err(sqlx::Error::Encode)?,
            SqlValue::Uuid(u) => args.add(*u).map_err(sqlx::Error::Encode)?,
            SqlValue::Int32(i) => args.add(*i).map_err(sqlx::Error::Encode)?,
            SqlValue::Int64(i) => args.add(*i).map_err(sqlx::Error::Encode)?,
            SqlValue::Bool(b) => args.add(*b).map_err(sqlx::Error::Encode)?,
            SqlValue::Bytes(b) => args.add(b.clone()).map_err(sqlx::Error::Encode)?,
            // Binding natively (rather than interpolating a literal) keeps
            // the SQL string identical across calls so PG's plan cache stays
            // warm on hot DDL paths.
            SqlValue::TextArray(arr) => args.add(arr.clone()).map_err(sqlx::Error::Encode)?,
            // A typed NULL: bind `Option::<T>::None` so the wire type matches
            // the column and PG accepts the NULL without a `$N::type` cast.
            SqlValue::Null(ty) => match ty {
                SqlType::Text => args
                    .add(Option::<String>::None)
                    .map_err(sqlx::Error::Encode)?,
                SqlType::Uuid => args
                    .add(Option::<uuid::Uuid>::None)
                    .map_err(sqlx::Error::Encode)?,
                SqlType::Int32 => args.add(Option::<i32>::None).map_err(sqlx::Error::Encode)?,
                SqlType::Int64 => args.add(Option::<i64>::None).map_err(sqlx::Error::Encode)?,
                SqlType::Bool => args
                    .add(Option::<bool>::None)
                    .map_err(sqlx::Error::Encode)?,
                SqlType::Bytes => args
                    .add(Option::<Vec<u8>>::None)
                    .map_err(sqlx::Error::Encode)?,
                SqlType::TextArray => args
                    .add(Option::<Vec<String>>::None)
                    .map_err(sqlx::Error::Encode)?,
            },
        }
    }
    Ok(args)
}

/// Owns a pooled connection holding a session-scoped advisory lock. On
/// drop (panic or early return), spawns a task to detach and close the
/// connection so the lock dies with the backend session instead of
/// riding back to the next pool consumer.
struct AdvisoryLockGuard {
    conn: Option<PoolConnection<Postgres>>,
    handle: Handle,
}

impl AdvisoryLockGuard {
    /// Must be called inside a Tokio runtime — captures the current [`Handle`]
    /// so [`Drop`] can schedule the detach+close on a still-live runtime.
    fn new(conn: PoolConnection<Postgres>) -> Self {
        Self {
            conn: Some(conn),
            handle: Handle::current(),
        }
    }

    fn as_conn_mut(&mut self) -> &mut PgConnection {
        self.conn.as_mut().expect("guard already disarmed")
    }

    /// Disarm the guard and return the connection for a normal unlock path.
    fn into_inner(mut self) -> PoolConnection<Postgres> {
        self.conn.take().expect("guard already disarmed")
    }
}

impl Drop for AdvisoryLockGuard {
    fn drop(&mut self) {
        // Still holding the conn => body() panicked before we could unlock.
        // Drop is sync, so schedule the detach+close on the runtime.
        if let Some(conn) = self.conn.take() {
            self.handle.spawn(async move {
                let raw = conn.detach();
                let _ = raw.close().await;
            });
        }
    }
}

/// PostgreSQL driver backed by a sqlx connection pool.
#[derive(Clone)]
pub struct PgDriver {
    pool: PgPool,
}

impl PgDriver {
    /// Create a new driver from a connection string.
    ///
    /// # Parameters
    /// - `conninfo`: A Postgres connection string
    ///   (e.g. `"postgres://penca:penca@localhost/penca"`).
    /// - `min_connections`: Minimum connections kept open in the pool.
    /// - `max_connections`: Maximum connections the pool will create.
    #[tracing::instrument(level = "info", skip_all, fields(min_connections, max_connections))]
    pub async fn connect(
        conninfo: &str,
        min_connections: u32,
        max_connections: u32,
    ) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .min_connections(min_connections)
            .max_connections(max_connections)
            .connect(conninfo)
            .await?;
        Ok(Self { pool })
    }

    /// Create a driver from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Begin a transaction, returning a [`PgTransactionDriver`] that commits
    /// on `.commit()` and rolls back on drop.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn begin(&self) -> Result<PgTransactionDriver, sqlx::Error> {
        let tx = self.pool.begin().await?;
        Ok(PgTransactionDriver {
            tx: Mutex::new(Some(tx)),
        })
    }

    /// Hold a Postgres session-scoped advisory lock while `body` runs.
    ///
    /// Acquires a dedicated pooled connection, runs
    /// `pg_advisory_lock(1, hashtext(key))` on it, awaits the caller's
    /// future, and releases the lock before returning the connection to
    /// the pool. The block is free to use this driver for unrelated
    /// queries — those check out separate connections from the pool.
    ///
    /// The connection is wrapped in an [`AdvisoryLockGuard`] so that a
    /// panic or early return from `body` still expels the connection
    /// (detach + close) rather than handing a live session-scoped lock
    /// back to the next pool consumer.
    ///
    /// The closure is spelled with [`AsyncFnOnce`] so the body can own
    /// captured state.
    #[tracing::instrument(level = "debug", skip_all, fields(key = %key))]
    pub async fn advisory_lock<F, R, E>(&self, key: &str, body: F) -> Result<R, E>
    where
        F: AsyncFnOnce() -> Result<R, E>,
        E: From<sqlx::Error>,
    {
        let conn = self.pool.acquire().await.map_err(E::from)?;
        // The guard fires on drop, so a panic in body() kills the session.
        let mut guard = AdvisoryLockGuard::new(conn);

        sqlx::query("SELECT pg_advisory_lock(1, hashtext($1))")
            .bind(key)
            .execute(guard.as_conn_mut())
            .await
            .map_err(E::from)?;

        let result = body().await;

        // Disarm: reached only if body() returned (panic path skips this).
        let mut conn = guard.into_inner();
        let unlock = sqlx::query("SELECT pg_advisory_unlock(1, hashtext($1))")
            .bind(key)
            .execute(&mut *conn)
            .await;
        if unlock.is_err() {
            // Unlock failed on a clean return — lock may still be held, so
            // don't hand this connection back to the pool.
            let raw = conn.detach();
            let _ = raw.close().await;
        }

        result
    }
}

impl DbDriver for PgDriver {
    type Row = PgRow;

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            db.statement = %query,
            rows_returned = tracing::field::Empty,
        ),
    )]
    async fn execute(&self, query: &str) -> Result<Vec<PgRow>, sqlx::Error> {
        let result = sqlx::query(query).fetch_all(&self.pool).await?;
        tracing::Span::current().record("rows_returned", result.len());
        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            db.statement = %query,
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_no_result(&self, query: &str) -> Result<(), sqlx::Error> {
        let result = self.pool.execute(query).await?;
        tracing::Span::current().record("rows_affected", result.rows_affected());
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            query_count = queries.len(),
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_many(&self, queries: &[String]) -> Result<(), sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        let mut total_rows: u64 = 0;
        for query in queries {
            let result = conn.execute(query.as_str()).await?;
            total_rows += result.rows_affected();
        }
        tracing::Span::current().record("rows_affected", total_rows);
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            db.statement = %query,
            params_count = params.len(),
            rows_returned = tracing::field::Empty,
        ),
    )]
    async fn execute_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        let args = build_pg_args(params)?;
        let result = sqlx::query_with(query, args).fetch_all(&self.pool).await?;
        tracing::Span::current().record("rows_returned", result.len());
        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            db.statement = %query,
            params_count = params.len(),
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_no_result_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<(), sqlx::Error> {
        let args = build_pg_args(params)?;
        let result = sqlx::query_with(query, args).execute(&self.pool).await?;
        tracing::Span::current().record("rows_affected", result.rows_affected());
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "pool",
            db.statement = %query,
            params_count = params.len(),
            row_found = tracing::field::Empty,
        ),
    )]
    async fn fetch_optional(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<Option<PgRow>, sqlx::Error> {
        let args = build_pg_args(params)?;
        let result = sqlx::query_with(query, args)
            .fetch_optional(&self.pool)
            .await?;
        tracing::Span::current().record("row_found", result.is_some());
        Ok(result)
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn close(&self) {
        self.pool.close().await;
    }

    /// True server-side cursor streaming via PostgreSQL's portal/cursor
    /// protocol, fetching rows incrementally.
    fn fetch_stream<'a>(
        &'a self,
        query: &'a str,
        params: &'a [SqlValue],
    ) -> Pin<Box<dyn Stream<Item = Result<PgRow, sqlx::Error>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let args = build_pg_args(params)?;
            let mut inner = sqlx::query_with(query, args).fetch(&self.pool);
            let mut count: u64 = 0;
            while let Some(row) = inner.next().await {
                count += 1;
                yield row?;
            }
            tracing::debug!(
                db.system = "postgres",
                driver_kind = "pool",
                db.statement = %query,
                params_count = params.len(),
                rows_streamed = count,
                "db.fetch_stream complete",
            );
        })
    }
}

/// Transaction-scoped driver bound to a single connection.
///
/// All operations reuse the same connection so row locks and writes
/// are visible across calls within the same transaction.
/// Rolls back automatically on drop if not explicitly committed.
///
/// Uses interior mutability (`Mutex`) so it can implement [`DbDriver`]
/// (which requires `&self`). No contention occurs because transactions
/// are always used sequentially.
pub struct PgTransactionDriver {
    tx: Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>,
}

impl PgTransactionDriver {
    /// Commit the transaction.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn commit(self) -> Result<(), sqlx::Error> {
        let tx = self.tx.into_inner().expect("transaction already consumed");
        tx.commit().await
    }

    /// Roll back the transaction.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn rollback(self) -> Result<(), sqlx::Error> {
        let tx = self.tx.into_inner().expect("transaction already consumed");
        tx.rollback().await
    }
}

impl DbDriver for PgTransactionDriver {
    type Row = PgRow;

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            db.statement = %query,
            rows_returned = tracing::field::Empty,
        ),
    )]
    async fn execute(&self, query: &str) -> Result<Vec<PgRow>, sqlx::Error> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let result = sqlx::query(query).fetch_all(&mut **tx).await?;
        tracing::Span::current().record("rows_returned", result.len());
        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            db.statement = %query,
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_no_result(&self, query: &str) -> Result<(), sqlx::Error> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let result = (&mut **tx).execute(query).await?;
        tracing::Span::current().record("rows_affected", result.rows_affected());
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            query_count = queries.len(),
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_many(&self, queries: &[String]) -> Result<(), sqlx::Error> {
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let mut total_rows: u64 = 0;
        for query in queries {
            let result = (&mut **tx).execute(query.as_str()).await?;
            total_rows += result.rows_affected();
        }
        tracing::Span::current().record("rows_affected", total_rows);
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            db.statement = %query,
            params_count = params.len(),
            rows_returned = tracing::field::Empty,
        ),
    )]
    async fn execute_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<Vec<PgRow>, sqlx::Error> {
        let args = build_pg_args(params)?;
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let result = sqlx::query_with(query, args).fetch_all(&mut **tx).await?;
        tracing::Span::current().record("rows_returned", result.len());
        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            db.statement = %query,
            params_count = params.len(),
            rows_affected = tracing::field::Empty,
        ),
    )]
    async fn execute_no_result_params(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<(), sqlx::Error> {
        let args = build_pg_args(params)?;
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let result = sqlx::query_with(query, args).execute(&mut **tx).await?;
        tracing::Span::current().record("rows_affected", result.rows_affected());
        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            db.system = "postgres",
            driver_kind = "tx",
            db.statement = %query,
            params_count = params.len(),
            row_found = tracing::field::Empty,
        ),
    )]
    async fn fetch_optional(
        &self,
        query: &str,
        params: &[SqlValue],
    ) -> Result<Option<PgRow>, sqlx::Error> {
        let args = build_pg_args(params)?;
        let mut guard = self.tx.lock().await;
        let tx = guard.as_mut().expect("transaction already consumed");
        let result = sqlx::query_with(query, args)
            .fetch_optional(&mut **tx)
            .await?;
        tracing::Span::current().record("row_found", result.is_some());
        Ok(result)
    }

    async fn close(&self) {
        // No-op: transaction is cleaned up via commit/rollback/drop.
    }

    /// Stream rows from the transaction connection via server-side cursor.
    ///
    /// Holds the `Mutex` guard for the stream's entire lifetime. This is
    /// safe because transactions are used sequentially — no other operation
    /// should be contending for the lock while the stream is being consumed.
    fn fetch_stream<'a>(
        &'a self,
        query: &'a str,
        params: &'a [SqlValue],
    ) -> Pin<Box<dyn Stream<Item = Result<PgRow, sqlx::Error>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let args = build_pg_args(params)?;
            let mut guard = self.tx.lock().await;
            let tx = guard.as_mut().expect("transaction already consumed");
            let mut inner = sqlx::query_with(query, args).fetch(&mut **tx);
            let mut count: u64 = 0;
            while let Some(row) = inner.next().await {
                count += 1;
                yield row?;
            }
            tracing::debug!(
                db.system = "postgres",
                driver_kind = "tx",
                db.statement = %query,
                params_count = params.len(),
                rows_streamed = count,
                "db.fetch_stream complete",
            );
        })
    }
}
