//! Format / syntactic validation for gRPC request shapes (CHA-92).
//!
//! These run at the gRPC servicer boundary — the convergence point for
//! both direct-gRPC and Flight-SQL-gateway callers — before any request
//! reaches the manager / lib layer. They check only the wire shape:
//! UUID-parseability, required-field presence, name format, value bounds.
//! Existence / identifier resolution stays in `penca-api`'s `resolve_*`
//! (it returns `NOT_FOUND`); these validators only ever yield
//! `INVALID_ARGUMENT`.
//!
//! The per-RPC validators are grouped by service in the submodules below and
//! re-exported here, so callers reach them as `crate::validation::validate_*`.
//! This module holds only the shared primitives those groups build on.

mod lifecycle;
mod query;
mod write;

pub use lifecycle::{
    validate_branch_op, validate_compact_persist_segments, validate_list_modified_tables,
    validate_list_persisted_tables, validate_persist, validate_purge, validate_purge_tx_log,
    validate_snapshot, validate_sweep_segments,
};
pub use query::{
    validate_audit_data, validate_get_branch, validate_get_catalog, validate_get_index,
    validate_get_max_commit_seq_num, validate_get_schema, validate_get_table,
    validate_list_branches, validate_list_indexes, validate_list_schemas, validate_list_tables,
    validate_read_data,
};
pub use write::{
    validate_abort_tx, validate_begin_tx, validate_commit_tx, validate_create_branch,
    validate_create_catalog, validate_create_index, validate_create_schema, validate_create_table,
    validate_delete_branch, validate_delete_catalog, validate_delete_index, validate_delete_schema,
    validate_delete_table, validate_merge_branch, validate_update_branch, validate_update_catalog,
    validate_update_index, validate_update_schema, validate_update_table, validate_write_data,
};

use tonic::Status;
use uuid::Uuid;

/// Upper bound on a human-readable identifier name (bytes).
pub(crate) const MAX_NAME_LEN: usize = 255;

/// Parse a UUID-typed wire field, mapping a parse failure to
/// `INVALID_ARGUMENT`.
pub(crate) fn check_uuid(field: &str, value: &str) -> Result<(), Status> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|e| Status::invalid_argument(format!("{field}: invalid UUID '{value}': {e}")))
}

/// Parse an optional UUID-typed wire field when present.
pub(crate) fn check_opt_uuid(field: &str, value: Option<&str>) -> Result<(), Status> {
    match value {
        Some(v) => check_uuid(field, v),
        None => Ok(()),
    }
}

/// Validate a human-readable identifier name: non-empty (after trim),
/// within a length bound, no control characters. Deliberately permissive
/// on the character set — stricter rules risk rejecting names the rest of
/// the surface already accepts.
pub(crate) fn check_name(field: &str, name: &str) -> Result<(), Status> {
    if name.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }
    if name.len() > MAX_NAME_LEN {
        return Err(Status::invalid_argument(format!(
            "{field} exceeds the {MAX_NAME_LEN}-byte limit"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

/// A required UUID-typed wire field: present (non-empty) and parseable.
pub(crate) fn require_uuid(field: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    check_uuid(field, value)
}
