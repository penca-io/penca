//! Vendored Flight SQL service implementation.
//!
//! Adapted from `datafusion-flight-sql-server` v0.4.16
//! (https://github.com/datafusion-contrib/datafusion-flight-sql-server).
//!
//! We vendor this code rather than depending on the crate because:
//! 1. Flight SQL is a core protocol surface — we need full control over
//!    query execution, memory management, and streaming behavior.
//! 2. The upstream crate is pre-1.0 with known issues (OOM bug #49).
//! 3. It's a thin wrapper (~1,200 lines) around DataFusion's FlightSqlService trait.
//! 4. Vendored code lets us bump DataFusion/Arrow on our own schedule.
//!
//! When updating DataFusion/Arrow versions, review the upstream repo for
//! changes and port relevant fixes.

pub(crate) mod codec;
pub(crate) mod error;
pub(crate) mod headers;
pub(crate) mod pin;
pub(crate) mod server;
pub mod service;
pub(crate) mod session_options;
pub mod state;
pub(crate) mod statement_cache;

pub use service::FlightSqlService;
