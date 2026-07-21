//! Shared helpers used across the `LifecycleManager` per-domain modules.
//!
//! - [`parse_uuid`] / [`qi`] / [`epoch`] are call-site aliases that
//!   collapse `Uuid::parse_str().expect(...)`, dialect identifier
//!   quoting, and the PG epoch SQL fragment into a single name each
//!   so the per-method bodies stay focused on the query shape.
//! - [`resolve_branch`] is the default-branch lookup that every
//!   read-side method threads when the caller didn't supply an
//!   explicit `branch_uuid`.

use penca_core::naming;
use penca_db::dialect::pg::PgDialect;
use penca_db::dialect::{DbDialect, Dialect};
use penca_db::driver::DbDriver;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::{LifecycleManager, MetadataError, Result};

pub fn parse_uuid(s: &str) -> Uuid {
    s.parse::<Uuid>().expect("invalid UUID")
}

pub fn qi(name: &str) -> String {
    PgDialect::quote_identifier(name)
}

pub fn epoch() -> &'static str {
    PgDialect::microsecond_epoch()
}

/// Resolve branch UUID, defaulting to the catalog's main branch via
/// a `branch_store` lookup on `branch_name = 'main'`. Post-CHA-236
/// `branch_uuid` is random-minted, so the default-branch path needs a
/// DB read instead of `naming::get_branch_uuid`. Most hot paths thread
/// an explicit `Some(branch_uuid)` and skip the lookup.
pub async fn resolve_branch(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &str,
    branch_uuid: Option<&str>,
) -> Result<String> {
    if let Some(b) = branch_uuid {
        return Ok(b.to_string());
    }
    let branch =
        LifecycleManager::get_branch_by_name(driver, catalog_uuid, naming::MAIN_BRANCH_NAME)
            .await?
            .ok_or_else(|| {
                MetadataError::Db(sqlx::Error::Protocol(format!(
                    "main branch missing for catalog {catalog_uuid}"
                )))
            })?;
    Ok(branch.branch_uuid)
}
