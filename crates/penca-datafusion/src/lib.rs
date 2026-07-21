//! DataFusion catalog, schema, and table providers for Penca.
//!
//! This crate bridges DataFusion's catalog abstraction with Penca's gRPC
//! microservices. Providers use `QueryServiceClient` for metadata discovery
//! and `QueryServiceClient` for data reads (merge-on-read via streaming).
//!
//! ## Design decision: gRPC clients vs in-process
//!
//! Providers communicate with Penca services via gRPC so that the Flight SQL
//! server exercises the same microservice boundaries as production. The
//! alternative — importing `penca-api` managers directly for in-process
//! composition — is trivially accessible by swapping how providers are
//! constructed. Only the wiring in `penca-sql-server` picks the transport.

pub(crate) mod catalog;
pub mod catalog_list;
pub(crate) mod conn_scope;
pub(crate) mod expr_to_sql;
pub(crate) mod pagination;
pub(crate) mod pk_ids;
pub(crate) mod plan_resolution_memo;
pub(crate) mod schema;
pub(crate) mod table;
#[cfg(test)]
pub(crate) mod test_support;

pub use crate::conn_scope::{ConnScope, PinnedAsOfSeqGuard};
pub use crate::pk_ids::encode_batch_ipc;
pub use crate::plan_resolution_memo::{
    PlanResolutionMemo, PlanResolutionMemoCell, PlanResolutionMemoGuard,
};
