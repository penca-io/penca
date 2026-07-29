//! Per-table lifecycle op: Purge.
//!
//! Persist and Snapshot are server-side per-branch RPCs; Purge stays a per-table
//! client call, driven from both of the snapshot loop's purge passes. It logs
//! and swallows `tonic::Status` errors — the caller's per-branch watermark
//! advances regardless. Retry timing depends on which sweep called it, so it is
//! documented once in `docs/services/lifecycle-scheduler.md` ("Failure
//! handling") rather than restated here.

use penca_proto::external::v1::PurgeRequest;
use penca_proto::external::v1::lifecycle_service_client::LifecycleServiceClient;
use tonic::transport::Channel;

/// Call Purge on a single table. Errors are logged and swallowed and the
/// caller's enumeration watermark advances regardless.
///
/// Called from BOTH of the snapshot loop's purge passes, which window on
/// different listings, so when a failed table comes back depends on the caller —
/// see `docs/services/lifecycle-scheduler.md` ("Failure handling").
///
/// Persist and Snapshot are not per-table client chains — each is one
/// server-side RPC per branch (the loop lives in `LifecycleManager`). Purge
/// stays per-table until CHA-502 moves it too.
#[tracing::instrument(
    skip_all,
    fields(table = %table_uuid),
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
