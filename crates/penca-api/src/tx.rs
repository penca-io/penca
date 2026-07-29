//! Postgres-transaction wrapper for penca-api.
//!
//! Deliberately does NOT cover `lifecycle::LifecycleManager::{persist_locked,
//! purge_locked, snapshot_locked}`: each step there auto-commits and an error
//! after partial progress runs an explicit `cleanup_*` helper. Wrapping them
//! in a single transaction would lose the cleanup semantics.

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
