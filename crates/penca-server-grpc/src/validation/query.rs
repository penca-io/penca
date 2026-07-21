//! QueryService read-side identifier validators.
//!
//! Parse present UUID fields so a malformed identifier fails as
//! `INVALID_ARGUMENT` at the boundary; existence stays in `resolve_*`.

use penca_proto::external::v1::{
    AuditDataRequest, GetBranchRequest, GetCatalogRequest, GetIndexRequest,
    GetMaxCommitSeqNumRequest, GetSchemaRequest, GetTableRequest, ListBranchesRequest,
    ListIndexesRequest, ListSchemasRequest, ListTablesRequest, ReadDataRequest,
};
use tonic::Status;

use super::check_opt_uuid;

pub fn validate_get_catalog(req: &GetCatalogRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())
}

pub fn validate_get_schema(req: &GetSchemaRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

pub fn validate_list_schemas(req: &ListSchemasRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

pub fn validate_get_table(req: &GetTableRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

pub fn validate_list_tables(req: &ListTablesRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

/// `GetIndex` (CHA-455): identifier format for the catalog/schema/branch/
/// table/index uuids + the open-tx pin.
pub fn validate_get_index(req: &GetIndexRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("index_uuid", req.index_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

/// `ListIndexes` (CHA-455): identifier format for the catalog/schema/
/// branch/table uuids + the open-tx pin.
pub fn validate_list_indexes(req: &ListIndexesRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

/// `GetMaxCommitSeqNum` (CHA-460): the seq-frontier capture the SQL pin calls.
/// Catalog + branch are both required (the sole caller always holds resolved
/// uuids); validate their format here, presence in the handler.
pub fn validate_get_max_commit_seq_num(req: &GetMaxCommitSeqNumRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())
}

pub fn validate_get_branch(req: &GetBranchRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())
}

pub fn validate_list_branches(req: &ListBranchesRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())
}

pub fn validate_read_data(req: &ReadDataRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("open_tx_uuid", req.open_tx_uuid.as_deref())
}

pub fn validate_audit_data(req: &AuditDataRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;

    const VALID_UUID: &str = "11111111-1111-1111-1111-111111111111";

    fn code(res: Result<(), Status>) -> Code {
        res.expect_err("expected an error").code()
    }

    #[test]
    fn read_validators_reject_malformed_open_tx_uuid() {
        // The read RPCs that carry `open_tx_uuid` feed it into the snapshot
        // resolver's `parse_uuid(...).expect(...)`, so a malformed value must
        // fail as INVALID_ARGUMENT at the boundary, not panic the handler.
        let get_schema = GetSchemaRequest {
            open_tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_get_schema(&get_schema)),
            Code::InvalidArgument
        );
        let list_schemas = ListSchemasRequest {
            open_tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_list_schemas(&list_schemas)),
            Code::InvalidArgument
        );
        let get_table = GetTableRequest {
            open_tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_get_table(&get_table)), Code::InvalidArgument);
        let list_tables = ListTablesRequest {
            open_tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_list_tables(&list_tables)),
            Code::InvalidArgument
        );
        let read_data = ReadDataRequest {
            open_tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_read_data(&read_data)), Code::InvalidArgument);
    }

    #[test]
    fn read_validators_accept_present_uuids() {
        let req = GetTableRequest {
            catalog_uuid: Some(VALID_UUID.into()),
            branch_uuid: Some(VALID_UUID.into()),
            schema_uuid: Some(VALID_UUID.into()),
            table_uuid: Some(VALID_UUID.into()),
            open_tx_uuid: Some(VALID_UUID.into()),
            ..Default::default()
        };
        assert!(validate_get_table(&req).is_ok());
        // All identifiers optional/absent is also a valid wire shape — name
        // resolution happens downstream in `resolve_*`.
        assert!(validate_get_table(&GetTableRequest::default()).is_ok());
    }
}
