//! Postgres-transaction wrapper for penca-api.
//!
//! [`with_pg_tx`] runs a closure inside a fresh `pool.begin()` /
//! `tx.commit()` pair, lifting the sqlx error from `begin` and
//! `commit` into [`ApiError::Metadata`]. On a successful return the
//! transaction is committed and the body's value is returned; on
//! `Err` the transaction is dropped (sqlx rolls back).
//!
//! Mechanism non-goal: does NOT cover the auto-commit
//! phase-1 + cleanup-on-error shape used by
//! `lifecycle::LifecycleManager::{persist_locked, purge_locked,
//! snapshot_locked}`. Those operations are not wrapped in a single PG
//! transaction — each step inside auto-commits, and an error after
//! partial progress runs an explicit `cleanup_*` helper. Conflating
//! the two would lose the cleanup semantics.

use penca_db::driver::pg::{PgDriver, PgTransactionDriver};

use crate::error::ApiError;

pub(crate) async fn with_pg_tx<T, F>(pool: &PgDriver, body: F) -> Result<T, ApiError>
where
    F: AsyncFnOnce(&PgTransactionDriver) -> Result<T, ApiError>,
{
    let tx = pool
        .begin()
        .await
        .map_err(|e| ApiError::Metadata(e.into()))?;
    let value = body(&tx).await?;
    tx.commit()
        .await
        .map_err(|e| ApiError::Metadata(e.into()))?;
    Ok(value)
}
