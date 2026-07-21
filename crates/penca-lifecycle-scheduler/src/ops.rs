//! Per-table lifecycle op: Purge.
//!
//! Persist + Snapshot moved to the server-side `PersistAndSnapshotBranch` RPC
//! (CHA-273); Purge stays a per-table client call. It logs and swallows
//! `tonic::Status` errors — the scheduler's per-branch watermark advances
//! regardless; retry happens implicitly when the table re-enters a future
//! enumeration window. See the "Failure semantics" section on `crate`'s module
//! doc.

use penca_proto::external::v1::PurgeRequest;
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use tonic::transport::Channel;

/// Call Purge on a single table. Errors are logged and swallowed and the
/// purge watermark in the caller advances regardless. A table that
/// fails here is retried only when it re-enters a future
/// `ListPersistedTables` window — i.e., when its next committed
/// persist clears the universal grace gate.
///
/// CHA-273 rework: Persist + Snapshot are no longer a per-table client chain
/// here — the scheduler drives `PersistAndSnapshotBranch` once per branch (the
/// loop moved server-side into `LifecycleManager`). Purge stays per-table.
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
