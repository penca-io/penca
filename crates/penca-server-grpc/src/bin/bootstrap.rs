//! Bootstrap global Penca tables in Postgres + seed the default catalog.
//!
//! Reads everything from env vars to match the rest of the Rust
//! servicer binaries (query / write / lifecycle all use
//! `*Config::from_env`). Run by the `bootstrap-init` compose service,
//! which inherits the same env block the runtime services use:
//!
//! - `DATABASE_URL` — Postgres connection string (required).
//! - `SQL_SERVER_DEFAULT_CATALOG` — name of the catalog to seed
//!   (required). Must match the SQL server's default-catalog config
//!   so unbound Flight SQL connections land on a real `catalog_store`
//!   row out of the box (CHA-171).
//!
//! Idempotent — re-running short-circuits at the catalog level.
//! `create_catalog_tables` issues a sequence of inserts that aren't
//! all idempotent today, so the cleanest guard is at the catalog
//! level.

use penca_core::config::required_env;
use penca_db::driver::pg::PgDriver;
use penca_storage_meta::LifecycleManager;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();

    let database_url = required_env("DATABASE_URL");
    let default_catalog = required_env("SQL_SERVER_DEFAULT_CATALOG");

    tracing::info!(default_catalog = %default_catalog, "penca-bootstrap starting");

    let driver = PgDriver::connect(&database_url, 1, 2).await?;

    LifecycleManager::bootstrap(&driver).await?;

    // CHA-236: probe by name (post-CHA-236 the catalog UUID is random
    // and not recomputable client-side). If missing, mint a fresh
    // catalog + main_branch + public_schema UUID server-side.
    let existing = LifecycleManager::get_catalog(&driver, None, Some(&default_catalog)).await?;
    let catalog_seeded = existing.is_none();
    if existing.is_none() {
        let catalog_uuid = Uuid::new_v4().to_string();
        let main_branch_uuid = Uuid::new_v4().to_string();
        let public_schema_uuid = Uuid::new_v4().to_string();
        LifecycleManager::create_catalog(
            &driver,
            &catalog_uuid,
            &default_catalog,
            "system",
            "auto-seeded by penca-bootstrap",
        )
        .await?;
        LifecycleManager::create_catalog_tables(
            &driver,
            &catalog_uuid,
            &main_branch_uuid,
            &public_schema_uuid,
        )
        .await?;
    }

    tracing::info!(catalog_seeded, "penca-bootstrap complete");

    Ok(())
}
