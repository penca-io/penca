//! WriteService request-shape validators (transactions, catalogs, branches,
//! schemas, tables) plus the write-specific attribution helpers.

use arrow::ipc::convert::try_schema_from_ipc_buffer;
use penca_proto::external::v1::{
    AbortTxRequest, BeginTxRequest, CommitTxRequest, CreateBranchRequest, CreateCatalogRequest,
    CreateIndexRequest, CreateSchemaRequest, CreateTableRequest, DeleteBranchRequest,
    DeleteCatalogRequest, DeleteIndexRequest, DeleteSchemaRequest, DeleteTableRequest,
    MergeBranchRequest, RetentionConfig, UpdateBranchRequest, UpdateCatalogRequest,
    UpdateIndexRequest, UpdateSchemaRequest, UpdateTableRequest, WriteDataRequest,
};
use tonic::Status;

use super::{check_name, check_opt_uuid, check_uuid, require_uuid};

/// Author/comment are the auto-commit transaction's attribution metadata:
/// required when `tx_uuid` is unset (auto-commit mints a fresh tx that needs
/// them), and rejected when `tx_uuid` is set (the open tx already carries its
/// own attribution from `BeginTx`). Shared by `WriteData` and the six DDL
/// write RPCs (Create/Update/Delete × Schema/Table) — all of which resolve
/// their tx through the lib's `resolve_or_auto_commit_tx`, which no longer
/// re-defends the wire shape (CHA-92 moved that check up to the servicer).
fn check_author_comment(
    tx_uuid: Option<&str>,
    author: Option<&str>,
    comment: Option<&str>,
) -> Result<(), Status> {
    match tx_uuid {
        Some(_) => {
            if author.is_some() || comment.is_some() {
                return Err(Status::invalid_argument(
                    "author/comment are valid only when tx_uuid is unset (auto-commit)",
                ));
            }
        }
        None => {
            if author.is_none() {
                return Err(Status::invalid_argument(
                    "author is required for auto-commit (tx_uuid unset)",
                ));
            }
            if comment.is_none() {
                return Err(Status::invalid_argument(
                    "comment is required for auto-commit (tx_uuid unset)",
                ));
            }
        }
    }
    Ok(())
}

/// A required non-empty (after trim) string field. Unlike [`check_name`] this
/// imposes no length or control-character bound — it suits free-form
/// attribution like `comment` that may be long or multi-line.
fn require_present(field: &str, value: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!("{field} is required")));
    }
    Ok(())
}

/// A submitted `RetentionConfig`: when both knobs are set, the durable-snapshot
/// rung spacing must not exceed the retention window (at least one rung per
/// window). A partial config (only one field set) passes — the absent field
/// coalesces from a parent, so no cross-field check is possible here. `field`
/// names the request field for the error message. Cross-*level* coalesced
/// combinations are intentionally not validated (the read-side coalesce stays
/// infallible; a cross-level density > duration is degraded-but-safe).
fn check_retention_config(field: &str, rc: &Option<RetentionConfig>) -> Result<(), Status> {
    if let Some(rc) = rc
        && let (Some(duration), Some(density)) =
            (rc.retention_duration_seconds, rc.snapshot_density_seconds)
        && density > duration
    {
        return Err(Status::invalid_argument(format!(
            "{field}: snapshot_density_seconds ({density}) must be <= \
             retention_duration_seconds ({duration})"
        )));
    }
    Ok(())
}

/// `WriteData`: tx_uuid mode-switch + author/comment mutual exclusion +
/// author/comment required when tx_uuid is unset (auto-commit) +
/// identifier UUID format (incl. the CHA-475 request-level `table_uuid`).
pub fn validate_write_data(req: &WriteDataRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    if let Some(tx_uuid) = req.tx_uuid.as_deref() {
        if tx_uuid.is_empty() {
            return Err(Status::invalid_argument(
                "tx_uuid must not be empty when present (omit it for auto-commit)",
            ));
        }
        check_uuid("tx_uuid", tx_uuid)?;
    }
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `BeginTx`: ttl within `(0, max_tx_timeout_seconds]` + identifier format.
pub fn validate_begin_tx(req: &BeginTxRequest, max_tx_timeout_seconds: i64) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    if let Some(ttl) = req.timeout_seconds {
        if ttl <= 0 {
            return Err(Status::invalid_argument("timeout_seconds must be positive"));
        }
        if ttl > max_tx_timeout_seconds {
            return Err(Status::invalid_argument(format!(
                "timeout_seconds {ttl} exceeds the server maximum of {max_tx_timeout_seconds}"
            )));
        }
    }
    Ok(())
}

/// `CommitTx`: tx_uuid required + parseable.
pub fn validate_commit_tx(req: &CommitTxRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    require_uuid("tx_uuid", &req.tx_uuid)
}

/// `AbortTx`: tx_uuid required + parseable (mirrors CommitTx — the only other
/// tx-bearing write RPC with a required, non-optional `tx_uuid`).
pub fn validate_abort_tx(req: &AbortTxRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    require_uuid("tx_uuid", &req.tx_uuid)
}

/// `CreateCatalog`: name format.
pub fn validate_create_catalog(req: &CreateCatalogRequest) -> Result<(), Status> {
    // CHA-433: retention is schema-broadest; the catalog carries no policy.
    check_name("catalog_name", &req.catalog_name)
}

/// `UpdateCatalog`: identifier format + rename target format. `new_catalog_name`
/// flows into the catalog metadata UPDATE as text, so it gets the same
/// boundary name check as the other rename targets (new_{schema,table,branch}_name).
pub fn validate_update_catalog(req: &UpdateCatalogRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    if let Some(new_name) = req.new_catalog_name.as_deref() {
        check_name("new_catalog_name", new_name)?;
    }
    // CHA-433: retention is schema-broadest; the catalog carries no policy.
    Ok(())
}

/// `DeleteCatalog`: identifier format.
pub fn validate_delete_catalog(req: &DeleteCatalogRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())
}

/// `CreateBranch`: identifier format + branch name format + required
/// attribution. The fork point (`fork_point` oneof) needs no format check —
/// a seq/micros position that names no committed tx is rejected by the write-pod
/// resolver, not here. `author` / `comment` are non-optional because every
/// branch creation auto-commits a tx tagged with them — there is no mode-switch,
/// so they must be present (mirrors the auto-commit branch of
/// [`check_author_comment`] for the optional-`tx_uuid` RPCs).
pub fn validate_create_branch(req: &CreateBranchRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("source_branch_uuid", req.source_branch_uuid.as_deref())?;
    check_name("branch_name", &req.branch_name)?;
    require_present("author", &req.author)?;
    require_present("comment", &req.comment)
}

/// `DeleteBranch`: identifier format.
pub fn validate_delete_branch(req: &DeleteBranchRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())
}

/// `UpdateBranch`: identifier format + rename target format.
pub fn validate_update_branch(req: &UpdateBranchRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    if let Some(new_name) = req.new_branch_name.as_deref() {
        check_name("new_branch_name", new_name)?;
    }
    Ok(())
}

/// `MergeBranch`: identifier format for both endpoints. Unlike `CreateBranch`,
/// `author` / `comment` are not required — the merge tolerates empty
/// attribution (the proto does not mark them required and callers may omit
/// them), so only the identifiers are checked here.
pub fn validate_merge_branch(req: &MergeBranchRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("source_branch_uuid", req.source_branch_uuid.as_deref())?;
    check_opt_uuid("target_branch_uuid", req.target_branch_uuid.as_deref())
}

/// `CreateSchema`: identifier format + name format + author/comment shape.
/// `tx_uuid` is the CHA-164 join-tx mode-switch — validate it too, since the
/// manager forwards it unparsed into UUID-typed DDL SQL on the join path.
pub fn validate_create_schema(req: &CreateSchemaRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_name("schema_name", &req.schema_name)?;
    check_retention_config("default_retention_config", &req.default_retention_config)?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `UpdateSchema`: identifier format + rename target format + author/comment.
pub fn validate_update_schema(req: &UpdateSchemaRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    if let Some(new_name) = req.new_schema_name.as_deref() {
        check_name("new_schema_name", new_name)?;
    }
    check_retention_config("default_retention_config", &req.default_retention_config)?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `DeleteSchema`: identifier format + author/comment shape.
pub fn validate_delete_schema(req: &DeleteSchemaRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `CreateTable`: identifier format + name format + parseable arrow schema +
/// every column type in the canonical supported set + author/comment shape.
pub fn validate_create_table(req: &CreateTableRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_name("table_name", &req.table_name)?;
    let schema = try_schema_from_ipc_buffer(&req.arrow_schema).map_err(|e| {
        Status::invalid_argument(format!(
            "arrow_schema is not a parseable Arrow IPC schema: {e}"
        ))
    })?;
    validate_column_types(&schema)?;
    // CHA-455: inline index definitions bypass the standalone
    // CreateIndex validator, so apply the same per-index checks here
    // (name format + non-empty columns) plus an intra-list name dedup.
    // The table schema is already decoded, so also reject a column that
    // isn't in it — cheap here, and it turns a typo into a clean
    // InvalidArgument instead of a silent dead index (roborev 0yt6). The
    // standalone CreateIndex path has no schema in the request, so its
    // column-existence is deferred to artifact build (CHA-412).
    let schema_columns: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut seen_index_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for def in &req.indexes {
        check_name("index_name", &def.index_name)?;
        if def.columns.is_empty() {
            return Err(Status::invalid_argument(format!(
                "inline index `{}`: columns must be non-empty",
                def.index_name
            )));
        }
        for col in &def.columns {
            if !schema_columns.contains(col.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "inline index `{}`: column `{}` is not in the table schema",
                    def.index_name, col
                )));
            }
        }
        if !seen_index_names.insert(def.index_name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "duplicate inline index name `{}` in CreateTable",
                def.index_name
            )));
        }
    }
    check_retention_config("retention_config", &req.retention_config)?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// The single supported-type gate (CHA-386): every column's Arrow type must
/// be in the canonical registry. Lives here, at the gRPC validation
/// boundary, because both SQL DDL (translated by `penca-sql-server` and
/// dispatched to this servicer) and direct gRPC callers funnel through here
/// — so the supported set is decided in exactly one place, with identical
/// wording, before any I/O. Downstream consumers (row codec, segment stats,
/// chunker) are total over `CanonicalType` and never re-enumerate the set.
fn validate_column_types(schema: &arrow::datatypes::Schema) -> Result<(), Status> {
    for field in schema.fields() {
        if let Err(penca_core::types::UnsupportedType(dt)) =
            penca_core::types::CanonicalType::from_arrow(field.data_type())
        {
            return Err(Status::invalid_argument(format!(
                "column `{}` has unsupported type {dt:?} — see the penca-core::types \
                 registry for the canonical supported set (CHA-386)",
                field.name()
            )));
        }
    }
    Ok(())
}

/// `UpdateTable`: identifier format + rename target format + parseable arrow
/// schema (when a schema-evolution payload is present) + author/comment.
pub fn validate_update_table(req: &UpdateTableRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    if let Some(new_name) = req.new_table_name.as_deref() {
        check_name("new_table_name", new_name)?;
    }
    if !req.arrow_schema.is_empty() {
        let schema = try_schema_from_ipc_buffer(&req.arrow_schema).map_err(|e| {
            Status::invalid_argument(format!(
                "arrow_schema is not a parseable Arrow IPC schema: {e}"
            ))
        })?;
        // CHA-386: a schema-evolution payload can add columns, so the
        // supported-type gate applies here too — otherwise an UpdateTable
        // adding an unsupported-type column would fail deep in the lib as
        // `internal` instead of rejecting cleanly at this boundary, the
        // cross-path asymmetry the single gate exists to prevent.
        validate_column_types(&schema)?;
    }
    check_retention_config("retention_config", &req.retention_config)?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `DeleteTable`: identifier format + author/comment shape.
pub fn validate_delete_table(req: &DeleteTableRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `CreateIndex` (CHA-455): identifier format + index_name format +
/// non-empty `columns` + author/comment shape.
pub fn validate_create_index(req: &CreateIndexRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_name("index_name", &req.index_name)?;
    if req.columns.is_empty() {
        return Err(Status::invalid_argument(
            "columns must be non-empty — an index needs at least one column",
        ));
    }
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `UpdateIndex` (CHA-455): rename-only — identifier format + the new
/// index name format + author/comment shape.
pub fn validate_update_index(req: &UpdateIndexRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("index_uuid", req.index_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_name("new_index_name", &req.new_index_name)?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

/// `DeleteIndex` (CHA-455): identifier format + author/comment shape.
pub fn validate_delete_index(req: &DeleteIndexRequest) -> Result<(), Status> {
    check_opt_uuid("catalog_uuid", req.catalog_uuid.as_deref())?;
    check_opt_uuid("schema_uuid", req.schema_uuid.as_deref())?;
    check_opt_uuid("branch_uuid", req.branch_uuid.as_deref())?;
    check_opt_uuid("table_uuid", req.table_uuid.as_deref())?;
    check_opt_uuid("index_uuid", req.index_uuid.as_deref())?;
    check_opt_uuid("tx_uuid", req.tx_uuid.as_deref())?;
    check_author_comment(
        req.tx_uuid.as_deref(),
        req.author.as_deref(),
        req.comment.as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::*;
    use crate::validation::MAX_NAME_LEN;

    const VALID_UUID: &str = "11111111-1111-1111-1111-111111111111";

    fn code(res: Result<(), Status>) -> Code {
        res.expect_err("expected an error").code()
    }

    #[test]
    fn write_data_empty_tx_uuid_rejected() {
        let req = WriteDataRequest {
            tx_uuid: Some(String::new()),
            ..Default::default()
        };
        assert_eq!(code(validate_write_data(&req)), Code::InvalidArgument);
    }

    #[test]
    fn write_data_non_uuid_tx_uuid_rejected() {
        let req = WriteDataRequest {
            tx_uuid: Some("not-a-uuid".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_write_data(&req)), Code::InvalidArgument);
    }

    #[test]
    fn write_data_author_with_tx_uuid_rejected() {
        let req = WriteDataRequest {
            tx_uuid: Some(VALID_UUID.into()),
            author: Some("a".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_write_data(&req)), Code::InvalidArgument);
    }

    #[test]
    fn write_data_valid_append_and_auto_commit_ok() {
        let append = WriteDataRequest {
            tx_uuid: Some(VALID_UUID.into()),
            ..Default::default()
        };
        assert!(validate_write_data(&append).is_ok());
        let auto = WriteDataRequest {
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert!(validate_write_data(&auto).is_ok());
    }

    #[test]
    fn write_data_auto_commit_requires_author_and_comment() {
        let no_author = WriteDataRequest {
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_write_data(&no_author)), Code::InvalidArgument);
        let no_comment = WriteDataRequest {
            author: Some("a".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_write_data(&no_comment)),
            Code::InvalidArgument
        );
    }

    #[test]
    fn begin_tx_ttl_bounds() {
        let over = BeginTxRequest {
            timeout_seconds: Some(11),
            ..Default::default()
        };
        assert_eq!(code(validate_begin_tx(&over, 10)), Code::InvalidArgument);
        let zero = BeginTxRequest {
            timeout_seconds: Some(0),
            ..Default::default()
        };
        assert_eq!(code(validate_begin_tx(&zero, 10)), Code::InvalidArgument);
        let ok = BeginTxRequest {
            timeout_seconds: Some(10),
            ..Default::default()
        };
        assert!(validate_begin_tx(&ok, 10).is_ok());
    }

    #[test]
    fn commit_tx_required_and_parsed() {
        let empty = CommitTxRequest::default();
        assert_eq!(code(validate_commit_tx(&empty)), Code::InvalidArgument);
        let bad = CommitTxRequest {
            tx_uuid: "nope".into(),
            ..Default::default()
        };
        assert_eq!(code(validate_commit_tx(&bad)), Code::InvalidArgument);
        let ok = CommitTxRequest {
            tx_uuid: VALID_UUID.into(),
            ..Default::default()
        };
        assert!(validate_commit_tx(&ok).is_ok());
    }

    #[test]
    fn create_catalog_name_format() {
        let empty = CreateCatalogRequest::default();
        assert_eq!(code(validate_create_catalog(&empty)), Code::InvalidArgument);
        let control = CreateCatalogRequest {
            catalog_name: "ab\nc".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_catalog(&control)),
            Code::InvalidArgument
        );
        let too_long = CreateCatalogRequest {
            catalog_name: "x".repeat(MAX_NAME_LEN + 1),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_catalog(&too_long)),
            Code::InvalidArgument
        );
        let ok = CreateCatalogRequest {
            catalog_name: "good_name".into(),
            ..Default::default()
        };
        assert!(validate_create_catalog(&ok).is_ok());
    }

    #[test]
    fn create_schema_and_table_validate_join_tx_uuid() {
        let schema = CreateSchemaRequest {
            schema_name: "s".into(),
            tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_schema(&schema)), Code::InvalidArgument);
        let table = CreateTableRequest {
            table_name: "t".into(),
            tx_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_table(&table)), Code::InvalidArgument);
    }

    #[test]
    fn create_table_unparseable_arrow_schema_rejected() {
        let req = CreateTableRequest {
            table_name: "t".into(),
            arrow_schema: vec![1, 2, 3],
            ..Default::default()
        };
        assert_eq!(code(validate_create_table(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_table_unsupported_column_type_rejected_with_registry_cite() {
        // The single supported-type gate (CHA-386): a Struct column is
        // rejected here, at the gRPC validation boundary, with wording
        // citing the registry and naming the offending column.
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "bad",
                DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
                true,
            ),
        ]);
        let err = validate_column_types(&schema).expect_err("struct must reject");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("core::types"), "{}", err.message());
        assert!(err.message().contains("`bad`"), "{}", err.message());
    }

    #[test]
    fn update_table_unsupported_column_type_rejected_at_gate() {
        // CHA-386: UpdateTable's schema-evolution payload routes through the
        // same supported-type gate as CreateTable — an unsupported column
        // rejects cleanly at this boundary, not deep in the lib as
        // `internal`. (The type gate runs before the author/comment check,
        // so a default request still exercises it.)
        use std::sync::Arc;

        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "bad",
                DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
                true,
            ),
        ]);
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new_with_options(
                &mut buf,
                &Arc::new(schema),
                IpcWriteOptions::default(),
            )
            .unwrap();
            writer.finish().unwrap();
        }
        let req = UpdateTableRequest {
            arrow_schema: buf,
            ..Default::default()
        };
        let err = validate_update_table(&req).expect_err("unsupported column must reject");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(err.message().contains("core::types"), "{}", err.message());
        assert!(err.message().contains("`bad`"), "{}", err.message());
    }

    #[test]
    fn check_author_comment_enforces_the_auto_commit_contract() {
        // tx_uuid present (append): author/comment must be absent.
        assert_eq!(
            code(check_author_comment(Some(VALID_UUID), Some("a"), None)),
            Code::InvalidArgument
        );
        assert_eq!(
            code(check_author_comment(Some(VALID_UUID), None, Some("c"))),
            Code::InvalidArgument
        );
        assert!(check_author_comment(Some(VALID_UUID), None, None).is_ok());
        // tx_uuid absent (auto-commit): both required.
        assert_eq!(
            code(check_author_comment(None, None, Some("c"))),
            Code::InvalidArgument
        );
        assert_eq!(
            code(check_author_comment(None, Some("a"), None)),
            Code::InvalidArgument
        );
        assert!(check_author_comment(None, Some("a"), Some("c")).is_ok());
    }

    #[test]
    fn ddl_write_rpcs_enforce_author_comment() {
        // Each DDL write RPC resolves its tx through resolve_or_auto_commit_tx,
        // so all must re-impose the author/comment wire-shape at the boundary —
        // not just WriteData. Auto-commit (no tx_uuid) without author/comment
        // must be rejected; tx_uuid + author/comment must be rejected.
        let create_schema_bad = CreateSchemaRequest {
            schema_name: "s".into(),
            author: Some("a".into()),
            tx_uuid: Some(VALID_UUID.into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_schema(&create_schema_bad)),
            Code::InvalidArgument
        );
        let update_schema_bad = UpdateSchemaRequest::default();
        assert_eq!(
            code(validate_update_schema(&update_schema_bad)),
            Code::InvalidArgument
        );
        let delete_schema_bad = DeleteSchemaRequest::default();
        assert_eq!(
            code(validate_delete_schema(&delete_schema_bad)),
            Code::InvalidArgument
        );
        let update_table_bad = UpdateTableRequest::default();
        assert_eq!(
            code(validate_update_table(&update_table_bad)),
            Code::InvalidArgument
        );
        let delete_table_bad = DeleteTableRequest::default();
        assert_eq!(
            code(validate_delete_table(&delete_table_bad)),
            Code::InvalidArgument
        );
        // Happy paths: auto-commit with author + comment.
        let update_schema_ok = UpdateSchemaRequest {
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert!(validate_update_schema(&update_schema_ok).is_ok());
        let delete_table_ok = DeleteTableRequest {
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert!(validate_delete_table(&delete_table_ok).is_ok());
    }

    #[test]
    fn validate_abort_tx_requires_tx_uuid() {
        let empty = AbortTxRequest::default();
        assert_eq!(code(validate_abort_tx(&empty)), Code::InvalidArgument);
        let bad = AbortTxRequest {
            tx_uuid: "nope".into(),
            ..Default::default()
        };
        assert_eq!(code(validate_abort_tx(&bad)), Code::InvalidArgument);
        let ok = AbortTxRequest {
            tx_uuid: VALID_UUID.into(),
            ..Default::default()
        };
        assert!(validate_abort_tx(&ok).is_ok());
    }

    #[test]
    fn validate_create_branch_requires_name_and_attribution() {
        // No fork point is required — an unset fork_point forks from head, and
        // a bad position is rejected by the resolver, not this validator.
        let no_name = CreateBranchRequest {
            author: "a".into(),
            comment: "c".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_branch(&no_name)),
            Code::InvalidArgument
        );
        // author/comment are required — branch creation always auto-commits.
        let no_author = CreateBranchRequest {
            branch_name: "b".into(),
            comment: "c".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_branch(&no_author)),
            Code::InvalidArgument
        );
        let no_comment = CreateBranchRequest {
            branch_name: "b".into(),
            author: "a".into(),
            ..Default::default()
        };
        assert_eq!(
            code(validate_create_branch(&no_comment)),
            Code::InvalidArgument
        );
        // Minimal valid request: name + attribution, no fork point (forks head).
        let ok = CreateBranchRequest {
            branch_name: "b".into(),
            author: "a".into(),
            comment: "c".into(),
            ..Default::default()
        };
        assert!(validate_create_branch(&ok).is_ok());
    }

    #[test]
    fn catalog_update_delete_validate_idents_and_rename() {
        // new_catalog_name is a rename target — it must get the same boundary
        // name check as the other rename targets.
        let bad_name = UpdateCatalogRequest {
            new_catalog_name: Some("ab\nc".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_update_catalog(&bad_name)),
            Code::InvalidArgument
        );
        let bad_uuid = UpdateCatalogRequest {
            catalog_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_update_catalog(&bad_uuid)),
            Code::InvalidArgument
        );
        assert!(validate_update_catalog(&UpdateCatalogRequest::default()).is_ok());
        let delete_bad = DeleteCatalogRequest {
            catalog_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_delete_catalog(&delete_bad)),
            Code::InvalidArgument
        );
        assert!(validate_delete_catalog(&DeleteCatalogRequest::default()).is_ok());
    }

    #[test]
    fn branch_validators_reject_malformed_uuids() {
        let merge_bad = MergeBranchRequest {
            source_branch_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_merge_branch(&merge_bad)),
            Code::InvalidArgument
        );
        let update_bad = UpdateBranchRequest {
            branch_uuid: Some("nope".into()),
            ..Default::default()
        };
        assert_eq!(
            code(validate_update_branch(&update_bad)),
            Code::InvalidArgument
        );
        assert!(validate_delete_branch(&DeleteBranchRequest::default()).is_ok());
    }

    #[test]
    fn create_index_empty_columns_rejected() {
        let req = CreateIndexRequest {
            index_name: "idx".into(),
            columns: vec![],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_index(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_index_blank_name_rejected() {
        let req = CreateIndexRequest {
            index_name: String::new(),
            columns: vec!["c".into()],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_index(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_index_valid_passes() {
        let req = CreateIndexRequest {
            index_name: "idx".into(),
            columns: vec!["c".into()],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert!(validate_create_index(&req).is_ok());
    }

    fn ipc_schema_bytes() -> Vec<u8> {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::writer::StreamWriter;
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut w = StreamWriter::try_new(Vec::new(), &schema).expect("writer");
        w.finish().expect("finish");
        w.into_inner().expect("into_inner")
    }

    #[test]
    fn create_table_inline_index_empty_columns_rejected() {
        use penca_proto::external::v1::CreateTableIndexDefinition;
        let req = CreateTableRequest {
            table_name: "t".into(),
            arrow_schema: ipc_schema_bytes(),
            primary_keys: vec!["id".into()],
            indexes: vec![CreateTableIndexDefinition {
                index_name: "i".into(),
                columns: vec![],
                ..Default::default()
            }],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_table(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_table_duplicate_inline_index_names_rejected() {
        use penca_proto::external::v1::CreateTableIndexDefinition;
        let dup = CreateTableIndexDefinition {
            index_name: "i".into(),
            columns: vec!["id".into()],
            ..Default::default()
        };
        let req = CreateTableRequest {
            table_name: "t".into(),
            arrow_schema: ipc_schema_bytes(),
            primary_keys: vec!["id".into()],
            indexes: vec![dup.clone(), dup],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_table(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_table_inline_index_unknown_column_rejected() {
        use penca_proto::external::v1::CreateTableIndexDefinition;
        let req = CreateTableRequest {
            table_name: "t".into(),
            arrow_schema: ipc_schema_bytes(),
            primary_keys: vec!["id".into()],
            indexes: vec![CreateTableIndexDefinition {
                index_name: "i".into(),
                columns: vec!["nonexistent".into()],
                ..Default::default()
            }],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert_eq!(code(validate_create_table(&req)), Code::InvalidArgument);
    }

    #[test]
    fn create_table_valid_inline_index_passes() {
        use penca_proto::external::v1::CreateTableIndexDefinition;
        let req = CreateTableRequest {
            table_name: "t".into(),
            arrow_schema: ipc_schema_bytes(),
            primary_keys: vec!["id".into()],
            indexes: vec![CreateTableIndexDefinition {
                index_name: "i".into(),
                columns: vec!["id".into()],
                ..Default::default()
            }],
            author: Some("a".into()),
            comment: Some("c".into()),
            ..Default::default()
        };
        assert!(validate_create_table(&req).is_ok());
    }

    #[test]
    fn retention_density_exceeds_duration_rejected() {
        let rc = Some(RetentionConfig {
            retention_duration_seconds: Some(600),
            snapshot_density_seconds: Some(3600),
        });
        assert_eq!(
            code(check_retention_config("f", &rc)),
            Code::InvalidArgument
        );
    }

    #[test]
    fn retention_density_le_duration_ok() {
        for (duration, density) in [(600, 600), (3600, 600)] {
            let rc = Some(RetentionConfig {
                retention_duration_seconds: Some(duration),
                snapshot_density_seconds: Some(density),
            });
            assert!(check_retention_config("f", &rc).is_ok());
        }
    }

    #[test]
    fn retention_partial_or_absent_config_ok() {
        let only_duration = Some(RetentionConfig {
            retention_duration_seconds: Some(600),
            snapshot_density_seconds: None,
        });
        let only_density = Some(RetentionConfig {
            retention_duration_seconds: None,
            snapshot_density_seconds: Some(3600),
        });
        assert!(check_retention_config("f", &only_duration).is_ok());
        assert!(check_retention_config("f", &only_density).is_ok());
        assert!(check_retention_config("f", &None).is_ok());
    }
}
