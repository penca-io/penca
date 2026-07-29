//! LifecycleService validators.
//!
//! Format-only: parse every present identifier UUID so a malformed value
//! fails as `INVALID_ARGUMENT` at the boundary rather than reaching the
//! partition SQL as `INTERNAL`. These intentionally do NOT require
//! `schema_uuid` when `table_uuid` is present — by-uuid resolution is
//! schema-agnostic (CHA-381 reworked the lifecycle resolver to resolve
//! tables catalog-wide via `resolve_table_by_uuid`). These stay format-only
//! so the servicer boundary stays decoupled from residency/existence, which
//! the resolver owns.

use penca_proto::external::v1::{
    BranchOpRequest, CompactPersistSegmentsRequest, ListModifiedTablesRequest,
    ListPersistedTablesRequest, PersistRequest, PurgeRequest, PurgeTxLogRequest, SnapshotRequest,
    SweepSegmentsRequest,
};
use tonic::Status;

use super::{check_opt_uuid, require_uuid};

/// Parse the (catalog, schema, branch, table) identifier quad that the
/// table-scoped lifecycle requests share.
fn check_lifecycle_table_idents(
    catalog_uuid: Option<&str>,
    schema_uuid: Option<&str>,
    branch_uuid: Option<&str>,
    table_uuid: Option<&str>,
) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", catalog_uuid)?;
    check_opt_uuid("schema_uuid", schema_uuid)?;
    check_opt_uuid("branch_uuid", branch_uuid)?;
    check_opt_uuid("table_uuid", table_uuid)
}

pub fn validate_persist(req: &PersistRequest) -> Result<(), Status> {
    check_lifecycle_table_idents(
        req.catalog_uuid.as_deref(),
        req.schema_uuid.as_deref(),
        req.branch_uuid.as_deref(),
        req.table_uuid.as_deref(),
    )
}

pub fn validate_purge(req: &PurgeRequest) -> Result<(), Status> {
    check_lifecycle_table_idents(
        req.catalog_uuid.as_deref(),
        req.schema_uuid.as_deref(),
        req.branch_uuid.as_deref(),
        req.table_uuid.as_deref(),
    )
}

pub fn validate_compact_persist_segments(
    req: &CompactPersistSegmentsRequest,
) -> Result<(), Status> {
    check_lifecycle_table_idents(
        req.catalog_uuid.as_deref(),
        req.schema_uuid.as_deref(),
        req.branch_uuid.as_deref(),
        req.table_uuid.as_deref(),
    )
}

pub fn validate_snapshot(req: &SnapshotRequest) -> Result<(), Status> {
    check_lifecycle_table_idents(
        req.catalog_uuid.as_deref(),
        req.schema_uuid.as_deref(),
        req.branch_uuid.as_deref(),
        req.table_uuid.as_deref(),
    )
}

pub fn validate_sweep_segments(req: &SweepSegmentsRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())
}

pub fn validate_purge_tx_log(req: &PurgeTxLogRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())
}

/// `catalog_uuid` / `branch_uuid` are required (non-optional) on the
/// dirty-set listing RPCs — they address per-catalog partitions directly,
/// so a missing/malformed value would otherwise reach the partition SQL as
/// `INTERNAL` (CHA-445 rehomed these off StorageMetadataService).
pub fn validate_list_modified_tables(req: &ListModifiedTablesRequest) -> Result<(), Status> {
    require_uuid("catalog_uuid", &req.catalog_uuid)?;
    require_uuid("branch_uuid", &req.branch_uuid)
}

pub fn validate_list_persisted_tables(req: &ListPersistedTablesRequest) -> Result<(), Status> {
    require_uuid("catalog_uuid", &req.catalog_uuid)?;
    require_uuid("branch_uuid", &req.branch_uuid)
}

/// `BranchOpRequest` (persist/snapshot branch ops, CHA-273) — format-only. The
/// catalog/branch may be addressed by name, so the UUID identifiers are
/// optional; parse each present one so a malformed value fails as
/// `INVALID_ARGUMENT` at the boundary rather than reaching `commit_tx_log` SQL as
/// `INTERNAL`. The optional `target` is a resolved `Watermark` position (no tx
/// identity), so there is nothing to UUID-validate. Names/residency are the
/// resolver's concern.
pub fn validate_branch_op(req: &BranchOpRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())
}

#[cfg(test)]
mod tests {
    use penca_proto::external::v1::Watermark;
    use tonic::Code;

    use super::*;

    fn code(res: Result<(), Status>) -> Code {
        res.expect_err("expected an error").code()
    }

    #[test]
    fn lifecycle_validators_parse_present_uuids() {
        // Table-scoped quad: a malformed table_uuid is rejected; all-absent
        // identifiers are a valid wire shape (resolution happens downstream).
        let persist_bad = PersistRequest {
            table_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_persist(&persist_bad)), Code::InvalidArgument);
        assert!(validate_persist(&PersistRequest::default()).is_ok());
        assert!(validate_snapshot(&SnapshotRequest::default()).is_ok());
        // Catalog-only request (the sweep is catalog-scoped since CHA-531).
        let sweep_bad = SweepSegmentsRequest {
            catalog_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_sweep_segments(&sweep_bad)),
            Code::InvalidArgument
        );
        assert!(validate_purge_tx_log(&PurgeTxLogRequest::default()).is_ok());
    }

    #[test]
    fn branch_op_validator_parses_optional_uuids() {
        // catalog/branch may be addressed by name, so an all-absent request is a
        // valid wire shape; a malformed catalog/branch uuid must be rejected as
        // INVALID_ARGUMENT at the boundary, not reach commit_tx_log SQL as
        // INTERNAL. The `target` is a resolved Watermark position — nothing to
        // UUID-validate.
        assert!(validate_branch_op(&BranchOpRequest::default()).is_ok());
        let bad_branch = BranchOpRequest {
            branch_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_branch_op(&bad_branch)), Code::InvalidArgument);
        let bad_catalog = BranchOpRequest {
            catalog_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_branch_op(&bad_catalog)),
            Code::InvalidArgument
        );
        let ok = BranchOpRequest {
            catalog_uuid: Some(VALID_UUID.into()),
            branch_uuid: Some(VALID_UUID.into()),
            target: Some(Watermark {
                commit_seq_num: 5,
                commit_micros: 100,
            }),
            ..Default::default()
        };
        assert!(validate_branch_op(&ok).is_ok());
    }

    const VALID_UUID: &str = "11111111-1111-1111-1111-111111111111";

    #[test]
    fn list_table_validators_require_catalog_and_branch_uuids() {
        // catalog_uuid / branch_uuid are non-optional on the dirty-set
        // listing RPCs (they address per-catalog partitions directly), so an
        // empty or malformed value must be rejected at the boundary — the
        // require_uuid coverage rehomed from the deleted storage-metadata
        // validators (CHA-445).
        assert_eq!(
            code(validate_list_modified_tables(
                &ListModifiedTablesRequest::default()
            )),
            Code::InvalidArgument,
            "empty catalog_uuid must be rejected"
        );
        let modified_malformed = ListModifiedTablesRequest {
            catalog_uuid: VALID_UUID.into(),
            branch_uuid: "nope".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_list_modified_tables(&modified_malformed)),
            Code::InvalidArgument,
            "malformed branch_uuid must be rejected"
        );
        let modified_ok = ListModifiedTablesRequest {
            catalog_uuid: VALID_UUID.into(),
            branch_uuid: VALID_UUID.into(),
            ..Default::default()
        };
        assert!(validate_list_modified_tables(&modified_ok).is_ok());

        assert_eq!(
            code(validate_list_persisted_tables(
                &ListPersistedTablesRequest::default()
            )),
            Code::InvalidArgument,
            "empty catalog_uuid must be rejected"
        );
        let persisted_malformed = ListPersistedTablesRequest {
            catalog_uuid: VALID_UUID.into(),
            branch_uuid: "nope".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_list_persisted_tables(&persisted_malformed)),
            Code::InvalidArgument,
            "malformed branch_uuid must be rejected"
        );
        let persisted_ok = ListPersistedTablesRequest {
            catalog_uuid: VALID_UUID.into(),
            branch_uuid: VALID_UUID.into(),
            ..Default::default()
        };
        assert!(validate_list_persisted_tables(&persisted_ok).is_ok());
    }
}
