//! Penca data-lake (cold-tier) driver + dialect.
//!
//! Peer crate to `penca-db` (OLTP hot tier). Both depend on the shared
//! [`penca_sql::Dialect`] contract. `penca-dl` intentionally does **not**
//! depend on `penca-db` — a datalake crate shouldn't pull in the
//! transactional-DB crate just to satisfy a trait.
//!
//! - [`dialect::DfDialect`] — DataFusion `Dialect` impl (`ROW_NUMBER()`
//!   variant of `latest_per_partition`).
//! - [`driver::DlDriver`] — abstraction for running SQL + reading
//!   snapshot segments against the cold tier.
//! - [`driver::DatafusionDlDriver`] — production impl backed by
//!   DataFusion + [`penca_format::reader::FormatReader`].
//! - [`schema`] — public contract types (`LogSchemas`, log-table name
//!   constants) consumed by [`driver::DlDriver::execute_sql`] and by
//!   `penca-merge`'s SQL builder.
//!
//! The DataFusion `TableProvider` + session builder that wires cold
//! persist segments into a queryable table live in a private
//! `provider` module — implementation detail of
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
