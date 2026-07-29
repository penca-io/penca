//! Penca data-lake (cold-tier) driver + dialect.
//!
//! Peer crate to `penca-db` (OLTP hot tier). Both depend on the shared
//! [`penca_sql::Dialect`] contract. `penca-dl` intentionally does **not**
//! depend on `penca-db` — a datalake crate shouldn't pull in the
//! transactional-DB crate just to satisfy a trait.
//!
//! [`schema`] carries the public contract types (`LogSchemas`, log-table name
//! constants) shared between [`driver::DlDriver::execute_sql`] and
//! `penca-merge`'s SQL builder; the `provider` module that wires cold segments
//! into a queryable `TableProvider` stays private, an implementation detail of
//! [`driver::DatafusionDlDriver`].

pub mod cache;
pub mod dialect;
pub mod driver;
pub mod list_cache;
mod provider;
pub mod schema;
mod session_template;
pub mod stats;

/// Re-exported so downstream callers (e.g. penca-api's `QueryManager`) can name
/// the cold-session template type for dependency injection without taking a
/// direct `datafusion` dependency — they only ever hold it opaquely and pass it
/// back into [`driver::DatafusionDlDriver::new`].
pub use datafusion::execution::context::SessionState;
pub use datafusion::prelude::SessionContext;
pub use session_template::{build_cold_session_template, derive_cold_session};
