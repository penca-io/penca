//! API orchestration layer composing storage clients and merge.
//!
//! Provides business-logic managers (Write, Query, Lifecycle) that
//! implement the Penca external API by composing storage clients
//! (`LifecycleManager`, `HotStorageClient`, `ColdStorageClient`) and the
//! merge algorithm.

pub mod error;
pub mod lifecycle;
mod pagination;
pub(crate) mod pk_batch;
pub mod query;
pub mod resolve;
pub(crate) mod retention;
pub(crate) mod scope;
mod tx;
pub mod write;

pub use error::ApiError;
pub use lifecycle::LifecycleManager;
pub use query::QueryManager;
pub use write::WriteManager;
