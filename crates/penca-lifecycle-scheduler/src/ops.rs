//! Per-table lifecycle op: Purge.
//!
//! Persist and Snapshot are server-side per-branch RPCs; Purge stays a per-table
//! client call, driven from both of the snapshot loop's purge passes. It logs
//! and swallows `tonic::Status` errors — the caller's per-branch watermark
//! advances regardless; retry happens implicitly when the table re-enters a
//! future enumeration window. See the "Failure semantics" section on `crate`'s
//! module doc.

use penca_proto::external::v1::PurgeRequest;
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use tonic::transport::Channel;

/// Call Purge on a single table. Errors are logged and swallowed and the
/// purge watermark in the caller advances regardless. A table that
/// fails here is retried only when it re-enters a future
/// `ListPersistedTables` window — i.e., when its next committed
/// persist clears the universal grace gate.
///
/// Persist and Snapshot are not per-table client chains — each is one
/// server-side RPC per branch (the loop lives in `LifecycleManager`). Purge
/// stays per-table until CHA-502 moves it too.
#[tracing::instrument(
    skip_all,
    fields(
        catalog = %catalog_uuid,
        branch = %branch_uuid,
        table = %table_uuid,
    ),
)]
pub(crate) async fn purge_one(
    lifecycle: &mut LifecycleServiceClient<Channel>,
    catalog_uuid: &str,
    branch_uuid: &str,
    table_uuid: &str,
) {
    if let Err(e) = lifecycle
        .purge(PurgeRequest {
            catalog_uuid: Some(catalog_uuid.to_string()),
            branch_uuid: Some(branch_uuid.to_string()),
            table_uuid: Some(table_uuid.to_string()),
            ..Default::default()
        })
        .await
    {
        tracing::warn!(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            table = %table_uuid,
            error = %e,
            "Purge failed"
        );
    }
}
