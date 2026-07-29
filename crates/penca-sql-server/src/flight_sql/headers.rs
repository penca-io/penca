//! Per-request validation of the `x-penca-branch` / `x-penca-catalog`
//! headers against the connection's handshake-pinned values.
//!
//! Branch and catalog are captured once at conn-mint
//! ([`crate::session::ConnSessionFactory::mint`]) and immutable for the
//! connection's lifetime. On every subsequent
//! request, [`validate_branch_header`] / [`validate_catalog_header`]
//! reject any header drift. The SQL-side mutation surfaces
//! (`SET branch = …`, `SET catalog = …`) and the Flight SQL
//! `SetSessionOptions` action are rejected separately by
//! [`crate::set`] — this module is the non-SQL path covering
//! header-drifting clients on existing connections.

use tonic::{Request, Status};

use crate::session::{
    BRANCH_HEADER_NAME, CATALOG_HEADER_NAME, SessionSnapshot, branch_header_from_request,
    catalog_header_from_request,
};

/// Reject requests whose `x-penca-branch` header disagrees with the
/// session's pinned branch. The branch is captured at session-mint
/// time and immutable for the connection's lifetime.
/// `SET branch = ...` / `SET penca.branch = ...` mid-session is
/// rejected separately by the `set` module dispatcher; this guard
/// catches the non-SQL path where a client mutates the header on a
/// live ADBC connection.
pub(crate) fn validate_branch_header<T>(
    request: &Request<T>,
    snapshot: &SessionSnapshot,
) -> Result<(), Status> {
    let Some(header) = branch_header_from_request(request) else {
        return Ok(());
    };
    let Some(supplied) = header.0 else {
        return Ok(());
    };
    if supplied != snapshot.branch_name {
        return Err(Status::failed_precondition(format!(
            "{BRANCH_HEADER_NAME} header `{supplied}` differs from this connection's \
             pinned branch `{}`; the branch is set at handshake time and immutable for \
             the connection's lifetime. Reconnect to switch branches.",
            snapshot.branch_name
        )));
    }
    Ok(())
}

/// Reject requests whose `x-penca-catalog` header disagrees with the
/// session's pinned catalog. Symmetric with [`validate_branch_header`]:
/// catalog is captured at session-mint time and immutable for the
/// connection's lifetime. The mid-session
/// `SetSessionOptions(catalog: …)` / `SET catalog = '…'` mutation
/// surfaces are rejected separately by [`crate::set::plan_catalog`];
/// this guard catches the case where a cookie-reusing client sends a
/// changed header value (the cache hot path silently ignores
/// header overrides on existing sessions — snapshot wins — so the
/// rejection has to fire here).
pub(crate) fn validate_catalog_header<T>(
    request: &Request<T>,
    snapshot: &SessionSnapshot,
) -> Result<(), Status> {
    let Some(header) = catalog_header_from_request(request) else {
        return Ok(());
    };
    let Some(supplied) = header.0 else {
        return Ok(());
    };
    if supplied != snapshot.catalog_name {
        return Err(Status::failed_precondition(format!(
            "{CATALOG_HEADER_NAME} header `{supplied}` differs from this connection's \
             pinned catalog `{}`; catalog is fixed at handshake; reconnect to switch.",
            snapshot.catalog_name
        )));
    }
    Ok(())
}
