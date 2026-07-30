//! Write operations for branches, transactions, and data mutations.
//!
//! [`WriteManager`] implements branch management, transaction lifecycle,
//! and data mutations (inserts, updates, deletes). Methods accept and
//! return proto messages directly.

use crate::query::QueryManager;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use penca_core::naming::{
    self, abort_tx_log_partition, begin_tx_log_partition, commit_tx_log_partition,
    delete_log_table, system_indexes_table_uuid, system_schema_uuid, system_schemas_table_uuid,
    system_tables_table_uuid, tx_table_log_partition, upsert_log_table,
};
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::{PgDriver, PgTransactionDriver};
use penca_db::driver::{DbDriver, SqlValue};
use penca_dl::driver::DlDriver;
use penca_format::reader::FormatReader;
use penca_proto::external::v1::create_branch_request::ForkPoint;
use penca_proto::external::v1::{
    AbortTxRequest, AbortTxResponse, BeginTxRequest, BeginTxResponse, Branch, Change,
    CommitTxRequest, CommitTxResponse, CreateBranchRequest, CreateBranchResponse,
    CreateCatalogRequest, CreateCatalogResponse, CreateIndexRequest, CreateIndexResponse,
    CreateSchemaRequest, CreateSchemaResponse, CreateTableRequest, CreateTableResponse,
    DeleteBranchRequest, DeleteBranchResponse, DeleteCatalogRequest, DeleteCatalogResponse,
    DeleteIndexRequest, DeleteIndexResponse, DeleteSchemaRequest, DeleteSchemaResponse,
    DeleteTableRequest, DeleteTableResponse, MergeBranchRequest, MergeBranchResponse,
    RetentionConfig, Table, UpdateBranchRequest, UpdateBranchResponse, UpdateCatalogRequest,
    UpdateCatalogResponse, UpdateIndexRequest, UpdateIndexResponse, UpdateSchemaRequest,
    UpdateSchemaResponse, UpdateTableRequest, UpdateTableResponse, Watermark, WriteDataRequest,
    WriteDataResponse,
};
use penca_storage_hot::{CommittedTx, HotStorageClient};
use penca_storage_meta::{
    LifecycleManager, rb_binary, rb_opt_i32, rb_opt_i64, rb_str, rb_string_list, rb_uuid_str,
};
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::resolve::{
    parse_resolved_uuid, resolve_branch, resolve_catalog, resolve_main_branch_uuid,
};
use crate::scope::ResolvedScope;
use crate::tx::with_pg_tx;

// Column names on `__penca_system__.tables` rows. The resolve CTE prepends
// `row_uuid` automatically, so it is not listed here.
const STC_TABLE_NAME: &str = "table_name";
const STC_ARROW_SCHEMA: &str = "arrow_schema";
const STC_PARTITION_KEYS: &str = "partition_keys";
const STC_CLUSTERING_KEYS: &str = "clustering_keys";
const STC_PRIMARY_KEYS: &str = "primary_keys";
const STC_DESCRIPTION: &str = "description";
const STC_RETENTION_DURATION_SECONDS: &str = "retention_duration_seconds";
const STC_SNAPSHOT_DENSITY_SECONDS: &str = "snapshot_density_seconds";

/// Penca write operations: catalog/schema/table DDL, branches,
/// transactions, and data mutations.
///
/// Holds service-level config (e.g. transaction TTL defaults).
/// Database state is accessed via the driver parameter on each method.
pub struct WriteManager {
    pub default_tx_timeout_seconds: i64,
    /// Handle for the metadata reads the write path needs (ADR 0028). Built
    /// with the snapshot-list + snapshot-segment caches ENABLED, so a hot
    /// point-write resolve shares the query path's caches; a disabled cache is
    /// the per-service opt-out.
    pub query_manager: crate::query::QueryManager,
}

/// Reject DDL targeting `__penca_system__`. The system
/// schema is structural (deterministic UUID anchor) and is managed
/// exclusively by `create_catalog_tables`; user mutation paths must
/// not Create/Update/Delete it.
fn assert_not_system_schema(catalog_uuid: &Uuid, schema_uuid: &Uuid) -> Result<(), ApiError> {
    if *schema_uuid == system_schema_uuid(catalog_uuid) {
        return Err(ApiError::InvalidRequest(
            "`__penca_system__` cannot be mutated via this RPC; namespace metadata is managed \
             exclusively through CRUD operations on schemas/tables."
                .to_string(),
        ));
    }
    Ok(())
}

/// Reject DDL / WriteData targeting the registered system tables
/// `__penca_system__.{schemas,tables,indexes}`. Those tables back the catalog's
/// namespace + index metadata; clients must mutate them via the
/// Create/Update/Delete{Schema,Table,Index} RPCs, not by writing rows directly.
fn assert_not_system_table(catalog_uuid: &Uuid, table_uuid: &Uuid) -> Result<(), ApiError> {
    if *table_uuid == system_schemas_table_uuid(catalog_uuid)
        || *table_uuid == system_tables_table_uuid(catalog_uuid)
        || *table_uuid == system_indexes_table_uuid(catalog_uuid)
    {
        return Err(ApiError::InvalidRequest(
            "`__penca_system__.{schemas,tables,indexes}` cannot be mutated via this RPC; \
             namespace + index metadata is managed exclusively through CRUD operations on \
             schemas/tables/indexes."
                .to_string(),
        ));
    }
    Ok(())
}

/// Validate a resolved write-target-table scope and return the parsed
/// `table_uuid`. Shared by `write_data` and the table/index DDL handlers so the
/// resolve+validate layering lives in one place. Rejects the registered system
/// tables `__penca_system__.{schemas,tables,indexes}` via the canonical
/// `table_uuid` guard ALWAYS; rejects `__penca_system__` as a *schema* only
/// when a schema row was resolved — the by-name path. The by-uuid path derives
/// the schema from the resolved table row (true residency) and relies on the
/// table guard, so it never re-asserts a caller-supplied schema there.
fn validate_write_target_table(scope: &ResolvedScope) -> Result<Uuid, ApiError> {
    let table = scope
        .table_row
        .as_ref()
        .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;
    let table_uuid = parse_resolved_uuid(&table.table_uuid, "table_uuid")?;
    assert_not_system_table(&scope.catalog_uuid, &table_uuid)?;
    if let Some(schema_row) = scope.schema_row.as_ref() {
        let schema_uuid = parse_resolved_uuid(&schema_row.schema_uuid, "schema_uuid")?;
        assert_not_system_schema(&scope.catalog_uuid, &schema_uuid)?;
    }
    Ok(table_uuid)
}

/// Validate a resolved write-target-schema scope and return the parsed
/// `schema_uuid`. Mirror of [`validate_write_target_table`] for the schema-target
/// DDL handlers (`update_schema`, `delete_schema`, `create_table`): require the
/// resolved `schema_uuid` (always `Some` for these requests, which carry a schema
/// ident) and reject `__penca_system__`. `create_schema` (mint path) is NOT a
/// caller — it has no schema ident and no system assert.
fn validate_write_target_schema(scope: &ResolvedScope) -> Result<Uuid, ApiError> {
    let schema_uuid = scope
        .schema_uuid
        .ok_or_else(|| ApiError::InvalidRequest("schema identifier required".into()))?;
    assert_not_system_schema(&scope.catalog_uuid, &schema_uuid)?;
    Ok(schema_uuid)
}

/// Validate `CreateTableRequest.primary_keys` against the
/// decoded `arrow_schema`. The three reachable failure modes:
///
/// * **Empty PK list** — Penca derives `row_uuid` from the PK so
///   PK-less tables are unsupported.
/// * **Duplicate PK names** — `["id", "id"]` propagates into the
///   dialect-layer `, col SQL_TYPE` builder and PG surfaces an
///   internal-looking `column "id" specified more than once`.
/// * **PK references undeclared column** — `["missing_col"]` parses
///   here but the dialect layer fires
///   `DataTableError::PrimaryKeyNotInSchema` with internal wording.
///
/// All three originate at this boundary regardless of whether the
/// caller is direct gRPC (`CreateTableRequest` over the wire) or
/// Flight SQL (`penca-sql-server::ddl::execute_create_table` builds
/// the request). Validating once here keeps the rejection wording
/// uniform across both wire paths.
fn validate_create_table_primary_keys(
    primary_keys: &[String],
    arrow_schema: &arrow::datatypes::Schema,
) -> Result<(), ApiError> {
    if primary_keys.is_empty() {
        return Err(ApiError::InvalidRequest(
            "CREATE TABLE requires at least one primary key column — Penca \
             derives row_uuid from the primary key so PK-less tables are not \
             supported"
                .to_string(),
        ));
    }
    let mut seen: HashSet<&str> = HashSet::with_capacity(primary_keys.len());
    for pk in primary_keys {
        if !seen.insert(pk.as_str()) {
            return Err(ApiError::InvalidRequest(format!(
                "primary key column `{pk}` listed more than once"
            )));
        }
    }
    let declared: HashSet<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    for pk in primary_keys {
        if !declared.contains(pk.as_str()) {
            return Err(ApiError::InvalidRequest(format!(
                "primary key references column `{pk}` that is not declared in arrow_schema"
            )));
        }
    }
    Ok(())
}

/// Map a sqlx error originating from a PG `UNIQUE` constraint
/// violation onto [`ApiError::AlreadyExists`] with `entity` as the
/// human-readable subject (e.g. "catalog", "branch"); pass everything
/// else through unchanged. Name-uniqueness on catalog + branch rename relies
/// on PG `UNIQUE` constraints.
fn map_unique_violation<T, E>(result: Result<T, E>, entity: &str) -> Result<T, ApiError>
where
    E: Into<penca_storage_meta::MetadataError>,
{
    match result {
        Ok(v) => Ok(v),
        Err(e) => {
            let meta: penca_storage_meta::MetadataError = e.into();
            if let penca_storage_meta::MetadataError::Db(ref sqlx_err) = meta
                && let Some(pg_err) = sqlx_err.as_database_error()
                && pg_err.code().as_deref() == Some("23505")
            {
                return Err(ApiError::AlreadyExists(format!(
                    "{entity} name already in use"
                )));
            }
            Err(ApiError::Metadata(meta))
        }
    }
}

/// Rename helper used by `update_schema` / `update_table`.
///
/// Returns the **finalized name** to write to the metadata row:
/// - When `request_new_name` is `Some(new)` and differs from
///   `existing_name`, runs `collision_check(new)` to ensure the
///   target name isn't already taken on this branch under the
///   request's snapshot. If a collision exists, returns
///   `ApiError::AlreadyExists`; otherwise returns the new name.
/// - When `request_new_name` is `None` or matches `existing_name`,
///   carries the existing name forward.
///
/// `__penca_system__.{schemas,tables}` has no PG `UNIQUE`
/// constraint (rows live in an auditable-store append log), so the
/// within-tx visibility check the closure performs is the
/// enforcement boundary.
async fn rename_or_carry_forward<F>(
    request_new_name: Option<&str>,
    existing_name: &str,
    entity: &str,
    branch_str: &str,
    collision_check: F,
) -> Result<String, ApiError>
where
    F: AsyncFnOnce(&str) -> Result<bool, ApiError>,
{
    let Some(new_name) = request_new_name else {
        return Ok(existing_name.to_string());
    };
    if new_name == existing_name {
        return Ok(existing_name.to_string());
    }
    if collision_check(new_name).await? {
        return Err(ApiError::AlreadyExists(format!(
            "{entity} {new_name} already exists on branch {branch_str}"
        )));
    }
    Ok(new_name.to_string())
}

fn retention_duration_seconds(rc: &Option<RetentionConfig>) -> Option<i64> {
    rc.as_ref().and_then(|r| r.retention_duration_seconds)
}

fn snapshot_density_seconds(rc: &Option<RetentionConfig>) -> Option<i64> {
    rc.as_ref().and_then(|r| r.snapshot_density_seconds)
}

/// Do-no-harm guard: on an update, a set `retention_duration_seconds` is
/// **immutable** — every change is rejected with `FAILED_PRECONDITION`. Both
/// directions are unsafe, for independent reasons:
///
/// - Loosening (a larger duration, or clearing it — an unset field replaces the
///   stored value on update, i.e. set -> unset = retain forever) would let a
///   time-travel read's historical retention fall *below* the current policy, so
///   the scope-based read floor could *wrongly reject* a valid read.
/// - Shortening prunes pre-fork ancestor history that a descendant branch's
///   `audit_data` below its fork point still needs, yielding silent wrong data
///   (missing rows read as absent). See TODO(CHA-514).
///
/// Establishing a policy where none exists (unset -> set) and no-op updates stay
/// allowed. This is only a partial tripwire on the *edit* trigger — it cannot
/// cover establishing-with-existing-forks, create-time retention + a later fork,
/// or the sliding-window aging that prunes pre-fork history with no write at all.
/// TODO(CHA-514) owns the read/prune-side fix.
fn reject_retention_duration_change(
    old: &Option<RetentionConfig>,
    new: &Option<RetentionConfig>,
) -> Result<(), ApiError> {
    let old_duration = retention_duration_seconds(old);
    let new_duration = retention_duration_seconds(new);
    // A set duration may not change; only unset -> set (establish) and no-op pass.
    if old_duration.is_some() && new_duration != old_duration {
        return Err(ApiError::FailedPrecondition(format!(
            "retention_duration_seconds is immutable once set (was {old_duration:?}, \
             requested {new_duration:?}); a set value may not be shortened, increased, \
             or cleared, only established where none exists (CHA-433; see CHA-514)"
        )));
    }

    Ok(())
}

/// Always-auto-commit variant. Used by callers that have no
/// `tx_uuid` mode-switch — `merge_branch`,
/// `materialize_metadata_from_source`, and any future caller for which
/// the open-tx path doesn't make sense. Returns `CommittedTx` directly
/// so callers don't have to `.expect("auto-commit always Some")` the
/// `Option` shape that [`resolve_or_auto_commit_tx`] needs for its
/// mode-switching contract.
async fn auto_commit_tx(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    author: &str,
    comment: &str,
) -> Result<(String, CommittedTx), ApiError> {
    let tx_uuid = Uuid::new_v4().to_string();
    let tx_part = commit_tx_log_partition(catalog_uuid, branch_uuid);
    let commit_tx_log_seq_num_part =
        naming::commit_tx_log_seq_num_partition(catalog_uuid, branch_uuid);
    let committed = HotStorageClient
        .auto_commit_tx(
            driver,
            &tx_part,
            &commit_tx_log_seq_num_part,
            &tx_uuid,
            &branch_uuid.to_string(),
            comment,
            author,
        )
        .await?;
    Ok((tx_uuid, committed))
}

/// Mode-switch shared between [`WriteManager::write_data`]
/// and the schema/table DDL mutations. When `request_tx_uuid` is set,
/// the caller is appending to an open tx (writes invisible until
/// CommitTx); when absent, auto-commit a fresh tx tagged with the
/// supplied `request_author` / `request_comment`.
///
/// Validates the same way the data path does: `author` / `comment` are
/// required for the auto-commit path; both must be unset when
/// `request_tx_uuid` is provided (the open tx already carries its own
/// attribution from BeginTx).
///
/// Returns `(tx_uuid, Option<CommittedTx>)`. The `CommittedTx` is
/// `Some` on the auto-commit path so callers can populate a `Tx`
/// response from the freshly minted row; `None` on the open-tx path
/// because the tx record was written by a prior `BeginTx`. Callers
/// that always auto-commit should reach for [`auto_commit_tx`] instead
/// to skip the `Option` and the mode-switch validation.
async fn resolve_or_auto_commit_tx(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    request_tx_uuid: Option<&str>,
    request_author: Option<&str>,
    request_comment: Option<&str>,
) -> Result<(String, Option<CommittedTx>), ApiError> {
    // Wire-shape checks (author/comment mutual-exclusion and
    // required-for-auto-commit) live in the servicer's `validate_write_data`;
    // the lib is "use at own risk" and deliberately does not re-defend them.
    match request_tx_uuid {
        Some(tx) => {
            // An append targets an existing tx — verify it is open
            // (begun, not aborted/expired/committed) before writing rows
            // that reference it, so an append against a non-open tx fails
            // fast instead of silently writing orphaned log rows.
            resolve_tx(driver, catalog_uuid, branch_uuid, tx).await?;
            Ok((tx.to_string(), None))
        }
        None => {
            // Servicer-validated to be present (see above); default to empty
            // for embedded callers rather than re-checking the wire contract.
            let author = request_author.unwrap_or_default();
            let comment = request_comment.unwrap_or_default();
            let (tx_uuid, committed) =
                auto_commit_tx(driver, catalog_uuid, branch_uuid, author, comment).await?;
            Ok((tx_uuid, Some(committed)))
        }
    }
}

/// Resolve an append-path `tx_uuid` to its open state.
///
/// Reads the single-shot `begin_tx_log ⟕ abort_tx_log ⟕ commit_tx_log` join via
/// `HotStorageClient::get_tx_status` against the request branch's leaf
/// partitions, and rejects:
/// - a tx with no `begin_tx_log` row (never begun, or begun on another
///   branch) → `NotFound`;
/// - a tx that is aborted / expired / already committed →
///   `FailedPrecondition`.
///
/// Snapshot read (`for_update=false`): a best-effort fast-fail, not a lock.
/// Under READ COMMITTED a concurrent `abort_tx` / expiry sweep can land an
/// `abort_tx_log` row after this SELECT but before the append commits, so a
/// racing append can still reference a tx that just went non-open. That's
/// acceptable here because final consistency is enforced at `CommitTx`
/// (`commit_open_tx` takes `FOR UPDATE OF begin_tx_log` and re-checks abort),
/// so no committed data ever references a non-open tx — the orphaned
/// upsert/delete rows are filtered by the `commit_tx_log` JOIN on read. The
/// fully-atomic tightening is to fold this predicate into the upsert/delete
/// INSERT as a CTE (see the note on `apply_change`). A `for_update=true`
/// lock here is not taken: every caller already runs inside a Pg transaction
/// (via `with_pg_tx`), but locking `begin_tx_log` on this advisory fast-fail
/// would only serialize concurrent appends to the same tx without buying any
/// correctness — `CommitTx` is the authoritative gate.
async fn resolve_tx(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    tx_uuid: &str,
) -> Result<(), ApiError> {
    let parsed = Uuid::parse_str(tx_uuid)
        .map_err(|e| ApiError::InvalidRequest(format!("invalid tx_uuid '{tx_uuid}': {e}")))?;
    let begin_partition = begin_tx_log_partition(catalog_uuid, branch_uuid);
    let abort_partition = abort_tx_log_partition(catalog_uuid, branch_uuid);
    let tx_partition = commit_tx_log_partition(catalog_uuid, branch_uuid);
    let hot = HotStorageClient;
    let status = hot
        .get_tx_status(
            driver,
            &begin_partition,
            &abort_partition,
            &tx_partition,
            &parsed,
            false,
        )
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "transaction not found on this branch \
                 (never begun, or begun on a different branch): {tx_uuid}"
            ))
        })?;
    match status {
        penca_storage_hot::TxStatus::Open { .. } => Ok(()),
        penca_storage_hot::TxStatus::Aborted { .. } => Err(ApiError::FailedPrecondition(format!(
            "transaction {tx_uuid} has been aborted"
        ))),
        penca_storage_hot::TxStatus::Expired { .. } => Err(ApiError::FailedPrecondition(format!(
            "transaction {tx_uuid} has expired"
        ))),
        penca_storage_hot::TxStatus::Committed { .. } => Err(ApiError::FailedPrecondition(
            format!("transaction {tx_uuid} has already been committed"),
        )),
    }
}

/// Which user tables on `source_branch_uuid` actually had a
/// committed write post-fork. Drives `merge_branch`'s per-table loop so
/// we only invoke `merge_table_data` on tables the source actually
/// wrote to — `committed_table_uuids` joins `tx_table_log` against
/// `commit_tx_log` so untouched tables are skipped without scanning their
/// empty post-fork window.
async fn enumerate_touched_table_uuids(
    tx: &PgTransactionDriver,
    hot: &HotStorageClient,
    catalog_uuid: &Uuid,
    source_branch_uuid: &Uuid,
) -> Result<HashSet<Uuid>, ApiError> {
    let source_tx_table_part = tx_table_log_partition(catalog_uuid, source_branch_uuid);
    let source_tx_part = commit_tx_log_partition(catalog_uuid, source_branch_uuid);
    Ok(hot
        .committed_table_uuids(tx, &source_tx_table_part, &source_tx_part)
        .await?
        .into_iter()
        .collect())
}

/// Merge one source-branch user table into the target branch under
/// `merge_tx_uuid`. Ensures target has the per-branch data tables, then
/// performs the bulk INSERT-FROM-SELECT from source's upsert/delete
/// logs into target's. Caller is responsible for emitting the
/// `tx_table_log` membership row after the loop.
#[allow(clippy::too_many_arguments)]
async fn merge_one_table_into_target(
    tx: &PgTransactionDriver,
    hot: &HotStorageClient,
    catalog_uuid: &Uuid,
    source_branch_uuid: &Uuid,
    target_branch_uuid: &Uuid,
    table_uuid: &Uuid,
    schema_ref: &SchemaRef,
    primary_keys: &[String],
    merge_tx_uuid: &str,
) -> Result<(), ApiError> {
    LifecycleManager::create_data_tables(
        tx,
        &table_uuid.to_string(),
        &target_branch_uuid.to_string(),
        schema_ref,
        primary_keys,
    )
    .await?;

    hot.merge_table_data(
        tx,
        &upsert_log_table(table_uuid, source_branch_uuid),
        &delete_log_table(table_uuid, source_branch_uuid),
        &upsert_log_table(table_uuid, target_branch_uuid),
        &delete_log_table(table_uuid, target_branch_uuid),
        &commit_tx_log_partition(catalog_uuid, source_branch_uuid),
        merge_tx_uuid,
        schema_ref,
    )
    .await?;
    Ok(())
}

/// Copy every `__penca_system__.schemas` row from a resolved source
/// batch set onto `new_branch_str` under `fork_tx_uuid`. Per-row write
/// shape mirrors the user-facing `create_schema` path
/// (`insert_schema_row`); fork_tx is the synthetic auto-commit tx that
/// owns every materialized row.
async fn materialize_schema_rows_from_batches(
    driver: &PgTransactionDriver,
    schema_batches: &[RecordBatch],
    catalog_str: &str,
    new_branch_str: &str,
    fork_tx_uuid: &str,
) -> Result<(), ApiError> {
    for batch in schema_batches {
        for i in 0..batch.num_rows() {
            // Re-inserting schema_uuid through insert_schema_row re-derives
            // the same row_uuid on the child branch.
            let schema_uuid_str = rb_uuid_str(batch, "schema_uuid", i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.schemas: missing schema_uuid".into())
            })?;
            let schema_name = rb_str(batch, "schema_name", i);
            let description = rb_str(batch, STC_DESCRIPTION, i);
            let retention_duration_seconds = rb_opt_i64(batch, STC_RETENTION_DURATION_SECONDS, i);
            let snapshot_density_seconds = rb_opt_i64(batch, STC_SNAPSHOT_DENSITY_SECONDS, i);

            LifecycleManager::insert_schema_row(
                driver,
                catalog_str,
                new_branch_str,
                &schema_uuid_str,
                fork_tx_uuid,
                &schema_name,
                &description,
                retention_duration_seconds,
                snapshot_density_seconds,
            )
            .await?;
        }
    }
    Ok(())
}

/// Copy every `__penca_system__.tables` row from a resolved source
/// batch set onto `new_branch_str` under `fork_tx_uuid`. Per row this
/// creates the deterministic per-branch data tables and then
/// inserts the metadata row carrying the source's `schema_uuid`,
/// arrow_schema, partition/clustering/PKs, description, and retention.
async fn materialize_table_rows_from_batches(
    driver: &PgTransactionDriver,
    table_batches: &[RecordBatch],
    catalog_str: &str,
    new_branch_str: &str,
    fork_tx_uuid: &str,
) -> Result<(), ApiError> {
    for batch in table_batches {
        for i in 0..batch.num_rows() {
            // Re-derives the same row_uuid via materialize_table_metadata on
            // the child.
            let table_uuid: Uuid = rb_uuid_str(batch, "table_uuid", i)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    ApiError::InvalidRequest("__penca_system__.tables: missing table_uuid".into())
                })?;
            // schema_uuid carried through stream_merged on the row
            // itself — drives the per-table `materialize_table_metadata`
            // call so `__penca_system__.tables` rows on the new branch
            // carry the source's schema_uuid.
            let row_schema_uuid = rb_uuid_str(batch, "schema_uuid", i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.tables: missing schema_uuid".into())
            })?;
            let arrow_schema_bytes = rb_binary(batch, STC_ARROW_SCHEMA, i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.tables: missing arrow_schema".into())
            })?;
            let arrow_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&arrow_schema_bytes)
                .map_err(ApiError::Arrow)?;

            // primary_keys MUST be read before create_data_tables so the
            // delete_log DDL can carry the PK columns.
            let partition_keys = rb_string_list(batch, STC_PARTITION_KEYS, i);
            let clustering_keys = rb_string_list(batch, STC_CLUSTERING_KEYS, i);
            let primary_keys = rb_string_list(batch, STC_PRIMARY_KEYS, i);
            let description = rb_str(batch, STC_DESCRIPTION, i);

            LifecycleManager::create_data_tables(
                driver,
                &table_uuid.to_string(),
                new_branch_str,
                &arrow_schema,
                &primary_keys,
            )
            .await?;
            let retention_duration_seconds = rb_opt_i64(batch, STC_RETENTION_DURATION_SECONDS, i);
            let snapshot_density_seconds = rb_opt_i64(batch, STC_SNAPSHOT_DENSITY_SECONDS, i);
            let table_name = rb_str(batch, STC_TABLE_NAME, i);

            LifecycleManager::materialize_table_metadata(
                driver,
                catalog_str,
                &row_schema_uuid,
                new_branch_str,
                &table_uuid.to_string(),
                fork_tx_uuid,
                &table_name,
                &arrow_schema_bytes,
                &partition_keys,
                &clustering_keys,
                &primary_keys,
                &description,
                retention_duration_seconds,
                snapshot_density_seconds,
            )
            .await?;
        }
    }
    Ok(())
}

/// Copy `__penca_system__.indexes` rows onto the forked branch, mirroring
/// [`materialize_table_rows_from_batches`].
///
/// Returns the number of index rows materialized — the caller MUST emit the
/// `__penca_system__.indexes` tx_table_log membership only when this is
/// non-zero. An empty fork writes no rows there, and a spurious membership on
/// an unpersisted table pins PurgeTxLog's `min(purged_at)` watermark at 0
/// forever.
async fn materialize_index_rows_from_batches(
    driver: &PgTransactionDriver,
    index_batches: &[RecordBatch],
    catalog_str: &str,
    new_branch_str: &str,
    fork_tx_uuid: &str,
) -> Result<usize, ApiError> {
    let mut count = 0usize;
    for batch in index_batches {
        for i in 0..batch.num_rows() {
            // Re-derives the same row_uuid via materialize_index_metadata on
            // the child.
            let index_uuid = rb_uuid_str(batch, "index_uuid", i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.indexes: missing index_uuid".into())
            })?;
            let table_uuid = rb_uuid_str(batch, "table_uuid", i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.indexes: missing table_uuid".into())
            })?;
            let index_name = rb_str(batch, "index_name", i);
            let columns = rb_string_list(batch, "columns", i);
            let index_type = rb_opt_i32(batch, "index_type", i).unwrap_or(0);
            LifecycleManager::materialize_index_metadata(
                driver,
                catalog_str,
                new_branch_str,
                &index_uuid,
                &table_uuid,
                fork_tx_uuid,
                &index_name,
                &columns,
                index_type,
            )
            .await?;
            count += 1;
        }
    }
    Ok(count)
}

impl WriteManager {
    /// Reject a `CreateBranch` whose source branch is not the catalog's `main`.
    ///
    /// Interim guard. MUST be called **before** `PersistBranch` flushes the
    /// source hot→cold, so a rejected non-main fork touches nothing. The read
    /// planner is single-level, so a fork off a non-main branch would silently
    /// drop grandparent rows on read.
    /// TODO(CHA-509): remove once the planner walks the full lineage chain.
    pub async fn ensure_fork_source_is_main(
        &self,
        pool: &PgDriver,
        request: &CreateBranchRequest,
    ) -> Result<(), ApiError> {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let source_branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.source_branch_uuid.as_deref(),
            request.source_branch_name.as_deref(),
        )
        .await?;
        let source_branch_uuid = parse_resolved_uuid(&source_branch.branch_uuid, "branch_uuid")?;
        let main_branch_uuid = resolve_main_branch_uuid(pool, &catalog_uuid).await?;
        if source_branch_uuid != main_branch_uuid {
            return Err(ApiError::Unimplemented(
                "multi-level branching not yet supported: source branch must be main (see CHA-509)"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Resolve a `CreateBranch` fork point — a source-branch commit-order
    /// position — to a full [`Watermark`] `{commit_seq_num, commit_micros}`. The
    /// write path calls this, then hands the position to `PersistBranch` (the
    /// flush target) and to [`Self::create_branch`] (the fork seed).
    ///
    /// After resolving the request's catalog + source branch, this delegates the
    /// position lookup itself to [`QueryManager::resolve_committed_tx`] (a read —
    /// hot `commit_tx_log`, then the durable cold `tx_log`), mapping a `None`
    /// (uncommitted in either tier) to a hard `INVALID_ARGUMENT`.
    pub async fn resolve_fork_watermark<R: FormatReader>(
        &self,
        pool: &PgDriver,
        readers: &HashMap<i32, R>,
        request: &CreateBranchRequest,
    ) -> Result<Watermark, ApiError> {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let source_branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.source_branch_uuid.as_deref(),
            request.source_branch_name.as_deref(),
        )
        .await?;
        let source_branch_uuid = parse_resolved_uuid(&source_branch.branch_uuid, "branch_uuid")?;

        // Resolving a commit-order position (hot commit_tx_log, then the durable
        // cold tx_log for positions PurgeTxLog has already GC'd) is a read, so it
        // lives on QueryManager. A position that resolves to no committed tx in
        // either tier is a hard INVALID_ARGUMENT — you can never fork from an
        // uncommitted position. For the head case (unset fork_point) that is
        // unreachable in practice: catalog genesis always commits seq 0, so a
        // real source branch is never empty.
        let describe = match &request.fork_point {
            Some(ForkPoint::CommitSeqNum(seq)) => format!("commit_seq_num {seq}"),
            Some(ForkPoint::CommitMicros(micros)) => format!("commit_micros {micros}"),
            None => "source head".to_string(),
        };
        self.query_manager
            .resolve_committed_tx(
                pool,
                readers,
                &catalog_uuid,
                &source_branch_uuid,
                request.fork_point.as_ref(),
            )
            .await?
            .ok_or_else(|| {
                ApiError::InvalidRequest(format!(
                    "CreateBranch fork point ({describe}) is not a committed tx on the source branch"
                ))
            })
    }

    /// Create a new branch with eager metadata materialization.
    ///
    /// Runs inside a transaction: creates the branch record, partition
    /// DDL, and copies every schema's metadata (schema rows + table
    /// rows) from the source branch onto the new branch as a single
    /// fork tx.
    ///
    /// Catalog-scoped: the request takes only the catalog identifier, and the
    /// materialization walks every schema visible on the source branch.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            source_branch_uuid = tracing::field::Empty,
            new_branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn create_branch<L>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &CreateBranchRequest,
        fork: &Watermark,
    ) -> Result<CreateBranchResponse, ApiError>
    where
        L: DlDriver + ?Sized,
    {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let source_branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.source_branch_uuid.as_deref(),
            request.source_branch_name.as_deref(),
        )
        .await?;
        let source_branch_uuid = parse_resolved_uuid(&source_branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record(
            "source_branch_uuid",
            tracing::field::display(&source_branch_uuid),
        );

        // Minted server-side; tests may pass an explicit `branch_uuid` for
        // setup determinism.
        let branch_uuid = if let Some(ref uuid_str) = request.branch_uuid {
            uuid_str
                .parse::<uuid::Uuid>()
                .map_err(|e| ApiError::InvalidRequest(format!("invalid branch_uuid: {e}")))?
        } else {
            Uuid::new_v4()
        };
        span.record("new_branch_uuid", tracing::field::display(&branch_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let source_branch_str = source_branch_uuid.to_string();

        // The SOURCE branch hot→cold flush already happened: the gRPC handler
        // resolved the fork position and called PersistBranch (in the lifecycle
        // pod) BEFORE this, so everything committed on the source at/before the
        // fork is already durable in cold — what the child's cross-branch cold
        // read consumes. This only records the fork and seeds the child; no
        // persist runs in the write pod.
        //
        // INVARIANT: PersistBranch bounds the source cold tier by
        // `commit_micros <= fork.commit_micros`, but `commit_micros` is only
        // *non-strictly* monotonic — a source commit in the SAME microsecond as
        // the fork (with a higher `commit_seq_num`) can leak into the source
        // cold tier. Harmless (persist is idempotent, the row is on the source),
        // but the child's "sees nothing committed after the fork" guarantee MUST
        // therefore be enforced by filtering the parent-cold read on
        // `commit_seq_num <= fork.commit_seq_num`, NOT on micros. Do not give
        // the parent-cold ceiling a micros bound.
        // TODO(CHA-500): once Persist accepts a seq cutoff, PersistBranch bounds
        // the flush at `fork.commit_seq_num` instead of `commit_micros`, making it
        // seq-exact and retiring this non-strict-micros leak entirely.
        with_pg_tx(pool, async |tx| {
            map_unique_violation(
                LifecycleManager::create_branch(
                    tx,
                    &catalog_str,
                    &branch_str,
                    &request.branch_name,
                    fork.commit_seq_num,
                    fork.commit_micros,
                    // The parent lineage the read planner's parent-cold source
                    // keys on.
                    Some(source_branch_str.as_str()),
                )
                .await,
                "branch",
            )?;

            LifecycleManager::ensure_branch_partitions(tx, &catalog_str, &branch_str).await?;

            // Seed the child's commit_seq_num counter from the fork commit T so
            // the child's seqs (> seq(T)) are disjoint from the parent's
            // (<= seq(T)); latest-wins-on-commit_seq_num resolution then shadows
            // the parent with no lineage tiebreak. MUST precede
            // materialize_metadata_from_source, whose fork/materialization tx is
            // the child's first commit and has to allocate the seeded value.
            //
            // Seeding from `fork.commit_seq_num` — T resolved ONCE under
            // PersistBranch — rather than a fresh MAX re-read closes the window
            // where a source commit between the flush and this read bumps MAX
            // past T.
            //
            // TODO(CHA-178): materialize_metadata_from_source reads source
            // metadata bounded on micros, so a source DDL in the same micros as
            // T (seq > fork_seq) could be inherited while sitting above this
            // seed; bound that read by seq (AsOfSeq(fork_seq)) to close it, and
            // give the parent-cold read ceiling the same fork_seq bound.
            LifecycleManager::seed_commit_seq_num_from_fork(
                tx,
                &catalog_str,
                &branch_str,
                fork.commit_seq_num,
            )
            .await?;

            self.materialize_metadata_from_source(
                tx,
                dl_driver,
                &catalog_uuid,
                &branch_uuid,
                &source_branch_uuid,
                &request.author,
                &request.comment,
            )
            .await?;

            // CHA-539: make the fork's claim on the parent's cold files an
            // EXPLICIT row in the child's own partition. Reaching across the fork
            // edge at plan time left the claim invisible to the sweep's refcount
            // gate, which is a `NOT EXISTS` probe over metadata tables — no row,
            // no probe can find it. Metadata only: every copied row carries the
            // parent's `object_uri`, so this is O(cold segments) in rows and O(1)
            // in bytes.
            //
            // Same table list `materialize_metadata_from_source` walked, and in
            // the same transaction, so `commit_micros` is stamped directly and a
            // rollback takes the whole copy with it.
            let inherited_tables = self
                .query_manager
                .list_table_uuids_for_branch(tx, dl_driver, &catalog_str, None, &source_branch_str)
                .await?;
            // Two distinct axes, so two distinct values. `fork.commit_micros` is
            // the fork POSITION; the copied rows' `commit_micros` is a phase-2
            // commit stamp answering "when did this row become visible", which is
            // now — stamping the fork position there would date the stamp before
            // the transaction that wrote the row. Inert while every consumer
            // treats the column as an IS NOT NULL visibility flag, but the column
            // has to keep meaning what it says for the next comparative reader.
            // Atomicity comes from this transaction, not from the value.
            let copy_commit_micros = LifecycleManager::now_micros(tx).await?;
            for inherited_table in &inherited_tables {
                LifecycleManager::materialize_fork_cold_references(
                    tx,
                    &catalog_str,
                    &branch_str,
                    &source_branch_str,
                    inherited_table,
                    fork.commit_seq_num,
                    fork.commit_micros,
                    copy_commit_micros,
                )
                .await?;
            }
            Ok(())
        })
        .await?;

        Ok(CreateBranchResponse {
            branch: Some(Branch {
                branch_uuid: branch_str,
                catalog_uuid: catalog_str,
                branch_name: request.branch_name.clone(),
                fork_commit_seq_num: fork.commit_seq_num,
            }),
        })
    }

    /// Delete a branch: enumerate its cold URIs, then drop its metadata and
    /// enqueue those URIs for the refcount gate, atomically.
    ///
    /// A pure metadata operation — it unlinks nothing itself. Dropping the
    /// branch's segment rows is what makes a file unreferenced; the next
    /// `sweep_segments` decides, past the universal grace window, whether any
    /// other branch still names it. That indirection is the whole point: since
    /// CHA-531 a carried row lives in one branch's partition while its
    /// `object_uri` names the file another branch wrote, so a teardown that
    /// unlinked what its own enumeration reached would destroy a sibling's data
    /// in either direction across a fork edge (CHA-539).
    ///
    /// Catalog-scoped: the walk covers every schema's tables on the branch, so
    /// cold segments + log/snapshot metadata for `s1.t1`, `s2.t2`, ... are all
    /// cleaned up.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn delete_branch<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &DeleteBranchRequest,
    ) -> Result<DeleteBranchResponse, ApiError> {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        // `main` is the catalog's root, not a branch you can tear down. Deleting
        // it leaves the catalog unusable for reasons unrelated to cold data —
        // every read resolves the main branch, so subsequent requests fail with
        // "main branch missing for catalog" — and `DeleteCatalog` is the
        // operation for removing a catalog.
        //
        // Not a substitute for the refcount gate, which is what actually makes
        // cross-fork teardown safe. This only removes a case that was never
        // coherent: while forks are main-only (CHA-515), "delete the branch a
        // fork inherits from" always means deleting `main`, so there is no
        // legitimate caller. TODO(CHA-509): once a fork can be a non-main
        // branch, deleting an intermediate parent becomes legitimate and the
        // gate is the only thing standing behind it.
        let main_branch_uuid = resolve_main_branch_uuid(pool, &catalog_uuid).await?;
        if branch_uuid == main_branch_uuid {
            return Err(ApiError::InvalidRequest(format!(
                "cannot delete the catalog's main branch ({branch_uuid}); \
                 use DeleteCatalog to remove the catalog"
            )));
        }

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        // `schema_uuid = None` makes this the catalog-wide table list. Read
        // outside the transaction because it is a schema-level read whose result
        // the enqueue does not depend on being point-in-time with: a table
        // created after this read has no cold segments on a branch being torn
        // down, and one dropped before it has none either.
        let table_uuid_strs = self
            .query_manager
            .list_table_uuids_for_branch(pool, dl_driver, &catalog_str, None, &branch_str)
            .await?;

        // Cold holds no commit_tx_log, so the touched set is just the data
        // tables; snapshot segments key directly on `(branch, table)`.
        let segment_table_uuids: Vec<&str> = table_uuid_strs.iter().map(String::as_str).collect();

        // Enumerate the files AND drop the metadata in one transaction. One
        // transaction because removing the references and queueing the files must
        // be a single fact: a crash between them leaves either an unreferenced
        // file nothing will ever collect, or a queued file still referenced with
        // no clock to reconcile. The enqueue's ON CONFLICT refresh gives a URI
        // already queued by another branch's retirement the later grace clock.
        //
        // The enumeration is INSIDE the transaction, not before it. Reading on
        // `pool` first left a window in which a concurrent lifecycle wave on this
        // branch could commit new segment rows and cold files after the read: the
        // CASCADE below then drops those rows, leaving a file that is both
        // unreferenced and absent from `segment_delete_set` — permanently
        // uncollectable, since enqueue-only teardown retired the orphan-scan
        // fallback that used to cover it. `drop_branch_partitions` takes
        // partition-level ACCESS EXCLUSIVE, which serialises such a wave out, so
        // enumerating in the same transaction closes the window instead of
        // merely narrowing it.
        with_pg_tx(pool, async |tx| {
            let persist_segments = LifecycleManager::get_table_persist_segments_for_tables(
                tx,
                &catalog_str,
                &branch_str,
                &segment_table_uuids,
            )
            .await?;

            let mut snap_segments: Vec<(String, String)> = Vec::new();
            for table_uuid_str in &table_uuid_strs {
                let segs = LifecycleManager::get_snapshot_segments_for_table(
                    tx,
                    &catalog_str,
                    &branch_str,
                    table_uuid_str,
                )
                .await?;
                snap_segments.extend(segs);
            }

            // Cold-index sidecars are their own files and their own delete-set
            // participants (ADR 0026 §5), but they are not reachable from the base
            // segment enumeration above — they hang off an index header. Without
            // this they would leak past the partition CASCADE below, and an
            // enqueue-only teardown would make that leak permanent rather than
            // merely untidy.
            //
            // `list_all_segment_index_uris`, not the committed-only planning read:
            // the CASCADE drops uncommitted sidecar rows too, so a sidecar whose
            // phase-2 stamp had not landed would lose its row without its URI ever
            // being queued. Matches the two sibling enumerations above, neither of
            // which filters on `commit_micros`.
            let snap_segment_uuids: Vec<String> =
                snap_segments.iter().map(|(uuid, _)| uuid.clone()).collect();
            let sidecar_uris: Vec<String> = LifecycleManager::list_all_segment_index_uris(
                tx,
                &catalog_str,
                &branch_str,
                &snap_segment_uuids,
            )
            .await?;

            // Also enumerate in-flight compact merged files tracked in
            // `compact_segment_metadata`. Two cases:
            //   - committed rows: the merged file is still referenced by
            //     `table_*_segment_metadata` rows on the branch and is
            //     covered by the persist/snap enumerations above. The
            //     overlap is harmless — the delete set holds one row per file.
            //   - NULL rows (crashed-mid-compact orphans): no segment
            //     metadata points at the merged file, so without this
            //     enumeration the file would leak past the partition
            //     CASCADE below.
            let compact_uris = LifecycleManager::get_compact_segment_uris_for_branch(
                tx,
                &catalog_str,
                &branch_str,
            )
            .await?;

            // Every URI the branch referenced, queued as one set. Deduped because
            // one physical file legitimately backs several rows (the packer packs
            // many partitions into one file) and `segment_delete_set` holds one row
            // per file.
            let queued_uris: Vec<String> = persist_segments
                .into_iter()
                .map(|(_, uri)| uri)
                .chain(snap_segments.into_iter().map(|(_, uri)| uri))
                .chain(compact_uris)
                .chain(sidecar_uris)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();

            let deleted = LifecycleManager::delete_branch(tx, &catalog_str, &branch_str).await?;
            if deleted {
                for table_uuid_str in &table_uuid_strs {
                    LifecycleManager::drop_data_tables(
                        tx,
                        table_uuid_str,
                        &branch_uuid.to_string(),
                    )
                    .await?;
                }

                // drop_branch_partitions removes the per-branch
                // leaves of branch_persist_metadata, table_persist_metadata,
                // table_persist_segment_metadata, table_snapshot_metadata,
                // and table_snapshot_segment_metadata via DROP TABLE
                // CASCADE — so explicit per-row DELETEs against those
                // parents would be redundant. Dropping those rows is exactly
                // what makes the enqueued files unreferenced, which is what
                // lets the sweep's refcount gate collect them. Only the
                // data-table drops above are tier-specific and stay here.
                LifecycleManager::drop_branch_partitions(tx, &catalog_str, &branch_str).await?;

                // Delete-set LAST, after the partition drops, per the ordering
                // invariant on `insert_segment_delete_set_rows`. Dropping a
                // partition takes ACCESS EXCLUSIVE on the catalog-wide parent; a
                // concurrent compact holds ROW SHARE on that same parent from its
                // opening `SELECT ... FOR UPDATE` and cannot release it, so
                // teardown must not be holding a delete-set row while it waits
                // for the parent. Still one transaction, so removing the
                // references and queueing the files remain one atomic fact.
                LifecycleManager::insert_segment_delete_set_rows(tx, &catalog_str, &queued_uris)
                    .await?;
            }
            Ok(())
        })
        .await?;

        Ok(DeleteBranchResponse {})
    }

    /// Rename a branch. Branches are catalog-scoped; the
    /// request carries the catalog identifier alongside the branch
    /// identifier and an `optional new_branch_name`. Not tx-tracked —
    /// branch renames update `branch_store` directly (same tier as
    /// `CreateBranch` / `DeleteBranch`). `branch_store.UNIQUE(branch_name)`
    /// enforces uniqueness; a collision surfaces as `ALREADY_EXISTS`.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn update_branch(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &UpdateBranchRequest,
    ) -> Result<UpdateBranchResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            driver,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        if let Some(new_name) = request.new_branch_name.as_deref() {
            map_unique_violation(
                LifecycleManager::update_branch_name(driver, &catalog_str, &branch_str, new_name)
                    .await,
                "branch",
            )?;
        }

        let branch = LifecycleManager::get_branch_row(driver, &catalog_str, &branch_str)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("branch not found: {branch_str}")))?;

        Ok(UpdateBranchResponse {
            branch: Some(branch),
        })
    }

    /// Merge a source branch into a target branch.
    ///
    /// Locks the source branch's commit_tx_log partition to serialize with
    /// concurrent commits, then copies data to the target branch.
    ///
    /// Catalog-scoped: the merge fans out across every schema's tables on the
    /// source branch, driven by source's `tx_table_log` since fork,
    /// intersected with source's catalog-wide `__penca_system__.tables`.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            source_branch_uuid = tracing::field::Empty,
            target_branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn merge_branch<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &MergeBranchRequest,
    ) -> Result<MergeBranchResponse, ApiError> {
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let source_branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.source_branch_uuid.as_deref(),
            request.source_branch_name.as_deref(),
        )
        .await?;
        let source_branch_uuid = parse_resolved_uuid(&source_branch.branch_uuid, "branch_uuid")?;
        let target_branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.target_branch_uuid.as_deref(),
            request.target_branch_name.as_deref(),
        )
        .await?;
        let target_branch_uuid = parse_resolved_uuid(&target_branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record(
            "source_branch_uuid",
            tracing::field::display(&source_branch_uuid),
        );
        span.record(
            "target_branch_uuid",
            tracing::field::display(&target_branch_uuid),
        );

        let catalog_str = catalog_uuid.to_string();
        let source_str = source_branch_uuid.to_string();
        let target_str = target_branch_uuid.to_string();

        let commit_micros = with_pg_tx(pool, async |tx| {
            let hot = HotStorageClient;

            // Lock the catalog's source-branch commit_tx_log partition to
            // serialize with concurrent commits. The partition is
            // catalog-scoped, so this blocks new commits on the source branch
            // from ANY schema in the catalog — the right scope for a
            // multi-schema-coherent merge.
            let source_tx_part = commit_tx_log_partition(&catalog_uuid, &source_branch_uuid);
            hot.lock_table(tx, &source_tx_part, "EXCLUSIVE").await?;

            // Fast-forward guard: reject if target has any tx committed past
            // source's fork point. With a unified upsert_log there is no
            // insert/update routing decision at merge time, but non-FF still
            // requires same-row conflict detection on both branches.
            //
            // TODO(CHA-5): extend merge conflict detection beyond fork-point
            // check. Non-FF needs same-row detection on both branches
            // post-fork (upsert/upsert and upsert/delete conflicts).
            let source_branch = LifecycleManager::get_branch_row(tx, &catalog_str, &source_str)
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!("source branch not found: {source_str}"))
                })?;
            ensure_fast_forward(
                tx,
                &catalog_uuid,
                &target_branch_uuid,
                source_branch.fork_commit_seq_num,
            )
            .await?;

            // Create merge transaction on target branch. A merge tx is just
            // an atomic auto-commit on commit_tx_log — same shape as WriteData's
            // auto-commit branch and the DDL auto-commits.
            let (merge_tx_uuid, committed) = auto_commit_tx(
                tx,
                &catalog_uuid,
                &target_branch_uuid,
                &request.author,
                &request.comment,
            )
            .await?;
            let commit_micros = committed.commit_micros;

            // Drive the merge loop from source's tx_table_log, not source's
            // full table-metadata, so merge_table_data is only called on tables
            // source actually wrote to since fork. On an untouched table it
            // would scan the empty post-fork window and write nothing — with
            // N=50 tables and writes to 3 that is 94 wasted SQL calls per merge.
            //
            // Source's `commit_tx_log_partition` contains only source-branch
            // txs, all post-fork by definition, so the JOIN's committed-only
            // filter is the precise predicate with no fork-point arithmetic.
            let touched_table_uuids =
                enumerate_touched_table_uuids(tx, &hot, &catalog_uuid, &source_branch_uuid).await?;

            // Fetch full table metadata for source. We still need user
            // tables' arrow_schema + naming for `merge_table_data`; only
            // the per-table merge_table_data calls themselves are pruned.
            // The read goes through stream_merged so it tolerates the
            // post-persist state where __penca_system__.tables rows live in
            // cold. `schema_uuid = None` makes it catalog-wide, so the merge
            // fans out across every schema's tables on the source branch.
            let table_batches = self
                .query_manager
                .resolve_table_metadata(
                    tx,
                    dl_driver,
                    &catalog_str,
                    None,
                    &source_str,
                    None,
                    // Catalog-wide read: no single row_uuid, no name key.
                    None,
                    None,
                    // As-of the merge commit point, never unbounded.
                    &penca_merge::ReadSnapshot::AsOfMicros(commit_micros),
                )
                .await?;

            // Collect distinct user table_uuids merge_tx writes to on the
            // target branch, so the tx_table_log membership rows can be emitted
            // after the loop: merge_table_data is bulk INSERT-FROM-SELECT, not
            // WriteData-shape, so the standard apply_change emit never fires.
            let mut merged_table_uuids: Vec<String> = Vec::with_capacity(touched_table_uuids.len());
            for batch in &table_batches {
                for i in 0..batch.num_rows() {
                    // Must match touched_table_uuids.
                    let table_uuid: Uuid = rb_uuid_str(batch, "table_uuid", i)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| {
                            ApiError::InvalidRequest(
                                "__penca_system__.tables: missing table_uuid".into(),
                            )
                        })?;
                    if !touched_table_uuids.contains(&table_uuid) {
                        continue;
                    }
                    let arrow_schema_bytes =
                        rb_binary(batch, STC_ARROW_SCHEMA, i).ok_or_else(|| {
                            ApiError::InvalidRequest(
                                "__penca_system__.tables: missing arrow_schema".into(),
                            )
                        })?;
                    let arrow_schema =
                        arrow::ipc::convert::try_schema_from_ipc_buffer(&arrow_schema_bytes)
                            .map_err(ApiError::Arrow)?;
                    let schema_ref: SchemaRef = Arc::new(arrow_schema);
                    let primary_keys = rb_string_list(batch, STC_PRIMARY_KEYS, i);

                    merge_one_table_into_target(
                        tx,
                        &hot,
                        &catalog_uuid,
                        &source_branch_uuid,
                        &target_branch_uuid,
                        &table_uuid,
                        &schema_ref,
                        &primary_keys,
                        &merge_tx_uuid,
                    )
                    .await?;

                    merged_table_uuids.push(table_uuid.to_string());
                }
            }

            if !merged_table_uuids.is_empty() {
                Self::emit_tx_table_log_for_ddl(
                    tx,
                    &catalog_uuid,
                    &target_branch_uuid,
                    &target_str,
                    &merge_tx_uuid,
                    &merged_table_uuids,
                )
                .await?;
            }

            Ok(commit_micros)
        })
        .await?;

        Ok(MergeBranchResponse { commit_micros })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            main_branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn create_catalog(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &CreateCatalogRequest,
    ) -> Result<CreateCatalogResponse, ApiError> {
        // Minted server-side. Clients cannot recompute these — they capture
        // them from the response (CreateCatalog returns the catalog + its
        // main_branch_uuid; ListSchemas / GetSchema returns the public schema).
        let catalog_uuid = Uuid::new_v4();
        let main_branch_uuid = Uuid::new_v4();
        let public_schema_uuid = Uuid::new_v4();

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record(
            "main_branch_uuid",
            tracing::field::display(&main_branch_uuid),
        );
        let catalog_uuid_str = catalog_uuid.to_string();
        let main_branch_uuid_str = main_branch_uuid.to_string();
        let public_schema_uuid_str = public_schema_uuid.to_string();
        map_unique_violation(
            LifecycleManager::create_catalog(
                driver,
                &catalog_uuid_str,
                &request.catalog_name,
                &request.owner,
                &request.description,
            )
            .await,
            "catalog",
        )?;

        // Bootstrap the catalog's per-catalog metadata tables
        // (branch_store, tx-log family) plus the
        // `__penca_system__.{schemas,tables}` data tables on main and
        // their four self-describing bootstrap rows.
        LifecycleManager::create_catalog_tables(
            driver,
            &catalog_uuid_str,
            &main_branch_uuid_str,
            &public_schema_uuid_str,
        )
        .await?;

        Ok(CreateCatalogResponse {
            catalog_uuid: catalog_uuid_str,
            main_branch_uuid: main_branch_uuid_str,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(catalog_uuid = tracing::field::Empty),
    )]
    pub async fn update_catalog(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &UpdateCatalogRequest,
    ) -> Result<UpdateCatalogResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;

        tracing::Span::current().record("catalog_uuid", tracing::field::display(&catalog_uuid));

        let catalog_str = catalog_uuid.to_string();
        // `catalog_store.UNIQUE(catalog_name)` enforces rename uniqueness; a
        // collision surfaces as `unique_violation` → `AlreadyExists`.
        map_unique_violation(
            LifecycleManager::update_catalog(
                driver,
                &catalog_str,
                &request.owner,
                &request.description,
                request.new_catalog_name.as_deref(),
            )
            .await,
            "catalog",
        )?;

        // Re-read for the response so the returned `Catalog` reflects
        // the new name (if renamed) and any other mutations.
        let catalog = LifecycleManager::get_catalog(driver, Some(&catalog_str), None)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("catalog not found: {catalog_str}")))?;

        Ok(UpdateCatalogResponse {
            catalog: Some(catalog),
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            main_branch_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn delete_catalog<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        request: &DeleteCatalogRequest,
    ) -> Result<DeleteCatalogResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));

        let catalog_str = catalog_uuid.to_string();
        // DeleteCatalog cascades against main only; per-branch schemas
        // are dropped en masse when `drop_catalog_tables` CASCADEs the
        // per-catalog physicals.
        let main_branch = resolve_main_branch_uuid(driver, &catalog_uuid).await?;
        span.record("main_branch_uuid", tracing::field::display(&main_branch));
        let main_branch_str = main_branch.to_string();

        let schema_uuids = self
            .query_manager
            .list_schema_uuids_for_catalog(driver, dl_driver, &catalog_str, None)
            .await?;

        // Skip the system schema in the cascade — it is a structural anchor
        // managed by `create_catalog_tables`, and the whole catalog physicals
        // get CASCADE-dropped below anyway.
        let system_schema_str = system_schema_uuid(&catalog_uuid).to_string();
        for schema_uuid in &schema_uuids {
            if schema_uuid == &system_schema_str {
                continue;
            }
            self.delete_schema_cascade(
                driver,
                dl_driver,
                &catalog_uuid,
                schema_uuid,
                &main_branch,
                None,
                Some("DeleteCatalog cascade"),
                Some("system"),
            )
            .await?;
        }

        // Drop all per-catalog tables (CASCADE cleans up partitions).
        LifecycleManager::drop_catalog_tables(driver, &catalog_str, &main_branch_str).await?;
        LifecycleManager::delete_catalog(driver, &catalog_str).await?;

        Ok(DeleteCatalogResponse {
            catalog_uuid: catalog_str,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn create_schema<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &CreateSchemaRequest,
    ) -> Result<CreateSchemaResponse, ApiError> {
        // Base-only resolve: CreateSchema carries no schema ident, so
        // `scope.schema_uuid` stays `None`. No `assert_not_system_*` needed —
        // a freshly minted v4 cannot collide with the deterministic
        // `system_schema_uuid(catalog)`.
        let scope =
            ResolvedScope::resolve_schema(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let schema_uuid = Uuid::new_v4();

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&scope.catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&scope.branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));

        let catalog_uuid_str = scope.catalog_uuid.to_string();
        let branch_uuid_str = scope.branch_uuid.to_string();
        let schema_uuid_str = schema_uuid.to_string();
        let rc = &request.default_retention_config;

        // Wrap the name-uniqueness check, auto-commit-into-commit_tx_log
        // INSERT, and the schema metadata INSERT in a single Pg tx so a
        // concurrent reader can't observe commit_tx_log committed before
        // `__penca_system__.schemas` lands.
        with_pg_tx(pool, async |tx| {
            // `__penca_system__.schemas` has no PG UNIQUE constraint (rows
            // live in an auditable-store append log), so this within-tx
            // visibility check IS the name-uniqueness enforcement.
            if self
                .query_manager
                .meta_get_schema(
                    tx,
                    dl_driver,
                    &catalog_uuid_str,
                    None,
                    Some(&request.schema_name),
                    Some(&branch_uuid_str),
                    &scope.snapshot,
                )
                .await?
                .is_some()
            {
                return Err(ApiError::AlreadyExists(format!(
                    "schema {} already exists on branch {}",
                    request.schema_name, branch_uuid_str
                )));
            }

            let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
                tx,
                &scope.catalog_uuid,
                &scope.branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;

            LifecycleManager::insert_schema_row(
                tx,
                &catalog_uuid_str,
                &branch_uuid_str,
                &schema_uuid_str,
                &tx_uuid,
                &request.schema_name,
                &request.description,
                retention_duration_seconds(rc),
                snapshot_density_seconds(rc),
            )
            .await?;

            Self::emit_tx_table_log_for_schemas_change(
                tx,
                &scope.catalog_uuid,
                &scope.branch_uuid,
                &branch_uuid_str,
                &tx_uuid,
            )
            .await?;
            Ok(())
        })
        .await?;

        Ok(CreateSchemaResponse {
            schema_uuid: schema_uuid_str,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn update_schema<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &UpdateSchemaRequest,
    ) -> Result<UpdateSchemaResponse, ApiError> {
        // Name → uuid uses a pg_now-pinned snapshot + RYOW under the request's
        // tx (writes never carry `as_of_micros`); branch resolves first via a
        // snapshot-blind `branch_store` SELECT. The write-only
        // `__penca_system__` guard layers on top, BEFORE any Pg tx opens.
        let scope =
            ResolvedScope::resolve_schema(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let schema_uuid = validate_write_target_schema(&scope)?;
        let write_snapshot = scope.snapshot;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));

        let branch_str = branch_uuid.to_string();
        let catalog_str = catalog_uuid.to_string();
        let schema_str = schema_uuid.to_string();

        // Wrap the existence check, commit_tx_log INSERT, schema-row INSERT, and
        // re-read in one Pg tx so concurrent readers can't observe a tx
        // committed before the new schema row lands.
        let schema = with_pg_tx(pool, async |tx| {
            // Carries the existing name forward when no rename was requested.
            // RYOW honoured.
            let existing = self
                .query_manager
                .meta_get_schema(
                    tx,
                    dl_driver,
                    &catalog_str,
                    Some(&schema_str),
                    None,
                    Some(&branch_str),
                    &write_snapshot,
                )
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("schema not found: {schema_uuid}")))?;

            // Rejects when another schema on this branch already uses the
            // target name, visible under this snapshot + open tx.
            let target_name = rename_or_carry_forward(
                request.new_schema_name.as_deref(),
                &existing.schema_name,
                "schema",
                &branch_str,
                async |new_name| {
                    Ok(self
                        .query_manager
                        .meta_get_schema(
                            tx,
                            dl_driver,
                            &catalog_str,
                            None,
                            Some(new_name),
                            Some(&branch_str),
                            &write_snapshot,
                        )
                        .await?
                        .is_some())
                },
            )
            .await?;

            let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;

            reject_retention_duration_change(
                &existing.default_retention_config,
                &request.default_retention_config,
            )?;
            let rc = &request.default_retention_config;
            LifecycleManager::insert_schema_row(
                tx,
                &catalog_str,
                &branch_str,
                &schema_str,
                &tx_uuid,
                &target_name,
                &request.description,
                retention_duration_seconds(rc),
                snapshot_density_seconds(rc),
            )
            .await?;

            Self::emit_tx_table_log_for_schemas_change(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_str,
                &tx_uuid,
            )
            .await?;

            let ryow_snapshot = QueryManager::resolve_read_snapshot(
                tx,
                &catalog_str,
                &branch_str,
                Some(&tx_uuid),
                None,
                None, // as_of_seq — inert on the OpenTx (RYOW) arm
                None,
            )
            .await?;
            let schema = self
                .query_manager
                .meta_get_schema(
                    tx,
                    dl_driver,
                    &catalog_str,
                    Some(&schema_str),
                    None,
                    Some(&branch_str),
                    &ryow_snapshot,
                )
                .await?;
            Ok(schema)
        })
        .await?;

        Ok(UpdateSchemaResponse { schema })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn delete_schema<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &DeleteSchemaRequest,
    ) -> Result<DeleteSchemaResponse, ApiError> {
        // DeleteSchema always carries a schema ident, so the resolved
        // `schema_uuid` is `Some` and the `__penca_system__` guard applies.
        let scope =
            ResolvedScope::resolve_schema(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let schema_uuid = validate_write_target_schema(&scope)?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));

        let schema_uuid_str = schema_uuid.to_string();

        // Wrap the cascade (table-tombstone fan-out + commit_tx_log INSERT +
        // schema-tombstone INSERT) in one Pg tx so partial failures roll
        // back atomically and concurrent readers see all-or-nothing.
        with_pg_tx(pool, async |tx| {
            self.delete_schema_cascade(
                tx,
                dl_driver,
                &catalog_uuid,
                &schema_uuid_str,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;
            Ok(())
        })
        .await?;

        Ok(DeleteSchemaResponse {
            schema_uuid: schema_uuid_str,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete_schema_cascade<L: DlDriver + ?Sized>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl_driver: &L,
        catalog_uuid: &Uuid,
        schema_uuid: &str,
        branch_uuid: &Uuid,
        request_tx_uuid: Option<&str>,
        request_author: Option<&str>,
        request_comment: Option<&str>,
    ) -> Result<(), ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let read_snapshot = QueryManager::resolve_read_snapshot(
            driver,
            &catalog_str,
            &branch_str,
            request_tx_uuid,
            None,
            None, // as_of_seq
            None,
        )
        .await?;
        let tables = self
            .query_manager
            .meta_list_tables(
                driver,
                dl_driver,
                &catalog_str,
                schema_uuid,
                Some(&branch_str),
                &read_snapshot,
            )
            .await?;

        // Soft-delete only: tombstones for each table and the schema. Physical
        // data tables stay addressable; the lifecycle sweep drops them after
        // commit.
        let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
            driver,
            catalog_uuid,
            branch_uuid,
            request_tx_uuid,
            request_author,
            request_comment,
        )
        .await?;
        for table in &tables {
            LifecycleManager::delete_table_metadata(
                driver,
                &catalog_str,
                &branch_str,
                &table.table_uuid,
                &tx_uuid,
            )
            .await?;
        }
        LifecycleManager::insert_schema_delete_row(
            driver,
            &catalog_str,
            &branch_str,
            schema_uuid,
            &tx_uuid,
        )
        .await?;

        // One row per system table this cascade actually wrote to: schemas
        // always (the schema tombstone), tables only if it hit any.
        let mut touched: Vec<String> = vec![system_schemas_table_uuid(catalog_uuid).to_string()];
        if !tables.is_empty() {
            touched.push(system_tables_table_uuid(catalog_uuid).to_string());
        }
        Self::emit_tx_table_log_for_ddl(
            driver,
            catalog_uuid,
            branch_uuid,
            &branch_str,
            &tx_uuid,
            &touched,
        )
        .await?;

        Ok(())
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn create_table<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &CreateTableRequest,
    ) -> Result<CreateTableResponse, ApiError> {
        // PK validation lives at this API boundary so SQL (via
        // penca-sql-server) and direct gRPC callers see identical wording for
        // the three reachable PK bugs. Runs before any I/O.
        //
        // The supported-column-type gate is deliberately NOT here — it is
        // enforced upstream in `penca-server-grpc`'s
        // `validation::write::validate_create_table`, the convergence point
        // both wire paths share. Any future *in-process* caller of
        // `create_table` that bypasses the gRPC servicer must replicate the
        // `CanonicalType::from_arrow` check itself; the asymmetry with the
        // in-crate PK check above is intentional, not an oversight.
        let user_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&request.arrow_schema)
            .map_err(ApiError::Arrow)?;
        validate_create_table_primary_keys(&request.primary_keys, &user_schema)?;

        // Refuse CreateTable in `__penca_system__` — its contents are managed
        // by the structural bootstrap, not user CRUD. Rejected before any tx
        // opens.
        let scope =
            ResolvedScope::resolve_schema(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let schema_uuid = validate_write_target_schema(&scope)?;
        let write_snapshot = scope.snapshot;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));

        let catalog_str = catalog_uuid.to_string();

        let table_uuid = Uuid::new_v4();
        span.record("table_uuid", tracing::field::display(&table_uuid));

        // Name-uniqueness pre-check + DDL + metadata INSERT run in ONE Pg tx,
        // so concurrent readers can't observe commit_tx_log committed before
        // the metadata row lands, and a duplicate `(branch, schema, name)` from
        // a concurrent CreateTable fails as `AlreadyExists` rather than
        // silently last-write-wins.
        let branch_uuid_str = branch_uuid.to_string();
        with_pg_tx(pool, async |tx| {
            if self
                .query_manager
                .meta_get_table(
                    tx,
                    dl_driver,
                    &catalog_str,
                    &schema_uuid.to_string(),
                    None,
                    Some(&request.table_name),
                    Some(&branch_uuid_str),
                    &write_snapshot,
                )
                .await?
                .is_some()
            {
                return Err(ApiError::AlreadyExists(format!(
                    "table {} already exists on branch {}",
                    request.table_name, branch_uuid_str
                )));
            }
            let (tx_uuid_str, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;
            // Per-branch data tables are deterministic in
            // `(table_uuid, branch_uuid)`, so a concurrent CreateTable on the
            // same `(table, branch)` shares the data table.
            LifecycleManager::create_data_tables(
                tx,
                &table_uuid.to_string(),
                &branch_uuid_str,
                &user_schema,
                &request.primary_keys,
            )
            .await?;

            let rc = &request.retention_config;
            LifecycleManager::insert_table_metadata(
                tx,
                &catalog_uuid.to_string(),
                &table_uuid.to_string(),
                &schema_uuid.to_string(),
                &branch_uuid_str,
                &tx_uuid_str,
                &request.table_name,
                &request.arrow_schema,
                &request.partition_keys,
                &request.clustering_keys,
                &request.primary_keys,
                &request.description,
                retention_duration_seconds(rc),
                snapshot_density_seconds(rc),
            )
            .await?;

            // Inline index definitions go into `__penca_system__.indexes` in
            // the SAME tx as the table, so they commit/abort atomically with it.
            for def in &request.indexes {
                LifecycleManager::insert_index_metadata(
                    tx,
                    &catalog_uuid.to_string(),
                    &Uuid::new_v4().to_string(),
                    &table_uuid.to_string(),
                    &branch_uuid_str,
                    &tx_uuid_str,
                    &def.index_name,
                    &def.columns,
                    def.index_type,
                )
                .await?;
            }

            // Membership: the tables system table always; the indexes
            // system table only when this CreateTable defined inline
            // indexes (so index reads resolve via (tx_uuid, table_uuid)).
            let mut touched = vec![system_tables_table_uuid(&catalog_uuid).to_string()];
            if !request.indexes.is_empty() {
                touched.push(system_indexes_table_uuid(&catalog_uuid).to_string());
            }
            Self::emit_tx_table_log_for_ddl(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_uuid_str,
                &tx_uuid_str,
                &touched,
            )
            .await?;
            Ok(())
        })
        .await?;

        Ok(CreateTableResponse {
            table_uuid: table_uuid.to_string(),
        })
    }

    /// Define a secondary index on a table. Writes a row into the
    /// auditable `__penca_system__.indexes` store (mirror of
    /// `create_table`). Name-uniqueness is enforced within the table.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
            index_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn create_index<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &CreateIndexRequest,
    ) -> Result<CreateIndexResponse, ApiError> {
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();
        let index_uuid = Uuid::new_v4();
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));
        span.record("index_uuid", tracing::field::display(&index_uuid));

        with_pg_tx(pool, async |tx| {
            if self
                .query_manager
                .meta_get_index(
                    tx,
                    dl_driver,
                    &catalog_str,
                    &table_str,
                    None,
                    Some(&request.index_name),
                    Some(&branch_str),
                    &scope.snapshot,
                )
                .await?
                .is_some()
            {
                return Err(ApiError::AlreadyExists(format!(
                    "index {} already exists on table {}",
                    request.index_name, table_str
                )));
            }
            let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;
            LifecycleManager::insert_index_metadata(
                tx,
                &catalog_str,
                &index_uuid.to_string(),
                &table_str,
                &branch_str,
                &tx_uuid,
                &request.index_name,
                &request.columns,
                request.index_type,
            )
            .await?;
            Self::emit_tx_table_log_for_ddl(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_str,
                &tx_uuid,
                &[system_indexes_table_uuid(&catalog_uuid).to_string()],
            )
            .await?;
            Ok(())
        })
        .await?;

        Ok(CreateIndexResponse {
            index_uuid: index_uuid.to_string(),
        })
    }

    /// Rename an index (rename-only — column/type changes are a
    /// drop+create rebuild). Appends a new auditable version carrying the
    /// new name with the existing columns/type.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn update_index<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &UpdateIndexRequest,
    ) -> Result<UpdateIndexResponse, ApiError> {
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));
        let table_str = table_uuid.to_string();

        let resolved_uuid = with_pg_tx(pool, async |tx| {
            let existing = self
                .query_manager
                .meta_get_index(
                    tx,
                    dl_driver,
                    &catalog_str,
                    &table_str,
                    request.index_uuid.as_deref(),
                    request.index_name.as_deref(),
                    Some(&branch_str),
                    &scope.snapshot,
                )
                .await?
                .ok_or_else(|| ApiError::NotFound("index not found".to_string()))?;

            // Reject a rename onto a name already taken by a DIFFERENT
            // index on the same table — otherwise two rows would share
            // `(table_uuid, index_name)` and name-based resolution would
            // be non-deterministic. A no-op rename (new == current) is
            // allowed.
            if request.new_index_name != existing.index_name
                && let Some(clash) = self
                    .query_manager
                    .meta_get_index(
                        tx,
                        dl_driver,
                        &catalog_str,
                        &table_str,
                        None,
                        Some(&request.new_index_name),
                        Some(&branch_str),
                        &scope.snapshot,
                    )
                    .await?
                && clash.index_uuid != existing.index_uuid
            {
                return Err(ApiError::AlreadyExists(format!(
                    "index {} already exists on table {}",
                    request.new_index_name, table_str
                )));
            }

            let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;
            LifecycleManager::insert_index_metadata(
                tx,
                &catalog_str,
                &existing.index_uuid,
                &table_str,
                &branch_str,
                &tx_uuid,
                &request.new_index_name,
                &existing.columns,
                existing.index_type,
            )
            .await?;
            Self::emit_tx_table_log_for_ddl(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_str,
                &tx_uuid,
                &[system_indexes_table_uuid(&catalog_uuid).to_string()],
            )
            .await?;
            Ok(existing.index_uuid)
        })
        .await?;

        Ok(UpdateIndexResponse {
            index_uuid: resolved_uuid,
        })
    }

    /// Drop an index. Soft-delete tombstone into
    /// `__penca_system__.indexes` (existence is a precondition — NotFound
    /// when absent at the read snapshot, the DDL-delete contract).
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn delete_index<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &DeleteIndexRequest,
    ) -> Result<DeleteIndexResponse, ApiError> {
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();
        let table_str = table_uuid.to_string();
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));

        let resolved_uuid = with_pg_tx(pool, async |tx| {
            // Resolve the index_uuid first (the request may address it by
            // name); the tombstone-if-visible call then enforces the
            // existence precondition under RYOW.
            let existing = self
                .query_manager
                .meta_get_index(
                    tx,
                    dl_driver,
                    &catalog_str,
                    &table_str,
                    request.index_uuid.as_deref(),
                    request.index_name.as_deref(),
                    Some(&branch_str),
                    &scope.snapshot,
                )
                .await?
                .ok_or_else(|| ApiError::NotFound("index not found".to_string()))?;

            let (tx_uuid, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;
            let visible = LifecycleManager::delete_index_metadata_if_visible(
                tx,
                &catalog_str,
                &branch_str,
                &existing.index_uuid,
                &tx_uuid,
                request.tx_uuid.as_deref(),
            )
            .await?;
            if !visible {
                return Err(ApiError::NotFound("index not found".to_string()));
            }
            Self::emit_tx_table_log_for_ddl(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_str,
                &tx_uuid,
                &[system_indexes_table_uuid(&catalog_uuid).to_string()],
            )
            .await?;
            Ok(existing.index_uuid)
        })
        .await?;

        Ok(DeleteIndexResponse {
            index_uuid: resolved_uuid,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn update_table<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &UpdateTableRequest,
    ) -> Result<UpdateTableResponse, ApiError> {
        // Rejects UpdateTable targeting `__penca_system__.*`.
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        // The by-uuid path derives schema_uuid from the resolved table row
        // (true residency) rather than the request's schema — the same
        // identifier dispatch read_data/write_data use, where table_uuid wins
        // over schema_uuid + table_name.
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::Internal("resolve_table did not populate schema_uuid".into())
        })?;
        let write_snapshot = scope.snapshot;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));
        let branch_uuid_str = branch_uuid.to_string();
        let catalog_uuid_str = catalog_uuid.to_string();
        let schema_uuid_str = schema_uuid.to_string();
        let table_uuid_str = table_uuid.to_string();

        // Wrap existence check, commit_tx_log INSERT, table-metadata INSERT,
        // schema-evolution DDL, and re-read in one Pg tx so concurrent
        // readers can't observe commit_tx_log committed before the new
        // metadata + evolved physical lands. The effective-retention
        // coalesce runs AFTER the tx commits (see post-commit block).
        let mut table = with_pg_tx(pool, async |tx| {
            let existing = self
                .query_manager
                .meta_get_table(
                    tx,
                    dl_driver,
                    &catalog_uuid_str,
                    &schema_uuid_str,
                    Some(&table_uuid_str),
                    None,
                    Some(&branch_uuid_str),
                    &write_snapshot,
                )
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("table not found: {table_uuid}")))?;

            // Rejects when another table on this branch already uses the
            // target name, visible under this snapshot + open tx.
            let target_table_name = rename_or_carry_forward(
                request.new_table_name.as_deref(),
                &existing.table_name,
                "table",
                &branch_uuid_str,
                async |new_name| {
                    Ok(self
                        .query_manager
                        .meta_get_table(
                            tx,
                            dl_driver,
                            &catalog_uuid_str,
                            &schema_uuid_str,
                            None,
                            Some(new_name),
                            Some(&branch_uuid_str),
                            &write_snapshot,
                        )
                        .await?
                        .is_some())
                },
            )
            .await?;

            let (tx_uuid_str, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;

            reject_retention_duration_change(
                &existing.retention_config,
                &request.retention_config,
            )?;
            let rc = &request.retention_config;
            // Per-branch data tables are deterministic in
            // `(table_uuid, branch_uuid)` — same names as Create, so no
            // carry-forward is needed.
            LifecycleManager::insert_table_metadata(
                tx,
                &catalog_uuid_str,
                &table_uuid_str,
                &schema_uuid_str,
                &branch_uuid_str,
                &tx_uuid_str,
                &target_table_name,
                &request.arrow_schema,
                &request.partition_keys,
                &request.clustering_keys,
                &request.primary_keys,
                &request.description,
                retention_duration_seconds(rc),
                snapshot_density_seconds(rc),
            )
            .await?;

            let old_schema =
                arrow::ipc::convert::try_schema_from_ipc_buffer(&existing.arrow_schema)
                    .map_err(ApiError::Arrow)?;
            let new_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&request.arrow_schema)
                .map_err(ApiError::Arrow)?;

            LifecycleManager::evolve_data_log_schema(
                tx,
                &upsert_log_table(&table_uuid, &branch_uuid),
                &old_schema,
                &new_schema,
            )
            .await?;

            Self::emit_tx_table_log_for_tables_change(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_uuid_str,
                &tx_uuid_str,
            )
            .await?;

            let ryow_snapshot = QueryManager::resolve_read_snapshot(
                tx,
                &catalog_uuid_str,
                &branch_uuid_str,
                Some(&tx_uuid_str),
                None,
                None, // as_of_seq — inert on the OpenTx (RYOW) arm
                None,
            )
            .await?;
            let table = self
                .query_manager
                .meta_get_table(
                    tx,
                    dl_driver,
                    &catalog_uuid_str,
                    &schema_uuid_str,
                    Some(&table_uuid_str),
                    None,
                    Some(&branch_uuid_str),
                    &ryow_snapshot,
                )
                .await?
                .ok_or_else(|| {
                    ApiError::NotFound(format!("table not found after update: {table_uuid}"))
                })?;
            Ok(table)
        })
        .await?;
        // Commit-before-coalesce: the parent reads inside
        // `apply_effective_retention` pass `None` for branch/open_tx
        // (canonical schema/catalog rows on main, independent of this
        // tx's state), so they don't need to see the tx's own writes.
        // Releasing the data-table-evolve lock first cuts the contention
        // window — `apply_effective_retention` is two metadata reads
        // that otherwise stretch the tx for nothing.

        // Coalesce retention so UpdateTableResponse.table.retention_config
        // matches GetTableResponse semantics — same proto type, same
        // meaning regardless of which RPC produced it.
        crate::retention::apply_effective_retention(
            &self.query_manager,
            pool,
            dl_driver,
            &catalog_uuid_str,
            &schema_uuid_str,
            &mut table,
        )
        .await?;

        Ok(UpdateTableResponse { table: Some(table) })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            schema_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn delete_table<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &DeleteTableRequest,
    ) -> Result<DeleteTableResponse, ApiError> {
        // Rejects DeleteTable targeting `__penca_system__.*`.
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        // Derived from the resolved table row on the by-uuid path (true
        // residency). The existence check is keyed on table_uuid, so the schema
        // is recorded for tracing only.
        let schema_uuid = scope.schema_uuid.ok_or_else(|| {
            ApiError::Internal("resolve_table did not populate schema_uuid".into())
        })?;
        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("schema_uuid", tracing::field::display(&schema_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));
        let branch_uuid_str = branch_uuid.to_string();
        let catalog_uuid_str = catalog_uuid.to_string();
        let table_uuid_str = table_uuid.to_string();

        // Wrap the commit_tx_log INSERT and the existence-checked tombstone
        // INSERT in one Pg tx so concurrent readers can't observe commit_tx_log
        // committed before the tombstone lands.
        with_pg_tx(pool, async |tx| {
            let (tx_uuid_str, _committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;

            // Soft-delete only: tombstone `__penca_system__.tables` iff the
            // table is visible at the request's open tx (RYOW honoured for
            // tables created in the same tx). The existence check + insert run
            // in ONE query; `false` means the table didn't exist → NotFound.
            // Data tables stay addressable; the lifecycle sweep drops them
            // after commit.
            let existed = LifecycleManager::delete_table_metadata_if_visible(
                tx,
                &catalog_uuid_str,
                &branch_uuid_str,
                &table_uuid_str,
                &tx_uuid_str,
                request.tx_uuid.as_deref(),
            )
            .await?;
            if !existed {
                return Err(ApiError::NotFound(format!("table not found: {table_uuid}")));
            }

            Self::emit_tx_table_log_for_tables_change(
                tx,
                &catalog_uuid,
                &branch_uuid,
                &branch_uuid_str,
                &tx_uuid_str,
            )
            .await?;
            Ok(())
        })
        .await?;

        Ok(DeleteTableResponse {
            table_uuid: table_uuid.to_string(),
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            tx_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn begin_tx(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        request: &BeginTxRequest,
    ) -> Result<BeginTxResponse, ApiError> {
        let catalog = resolve_catalog(
            driver,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            driver,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let tx_uuid = request
            .tx_uuid
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        span.record("tx_uuid", tx_uuid.as_str());
        let timeout = request
            .timeout_seconds
            // An over-max ttl is rejected at the servicer boundary
            // (validate_begin_tx), so there is deliberately no clamp here;
            // embedded lib callers pass the value through at their own risk.
            .unwrap_or(self.default_tx_timeout_seconds);

        let hot = HotStorageClient;
        let table_name = naming::begin_tx_log_table(&catalog_uuid);
        let commit_tx_log_seq_num_partition =
            naming::commit_tx_log_seq_num_partition(&catalog_uuid, &branch_uuid);

        let (began_at_micros, expires_at_micros) = hot
            .begin_tx(
                driver,
                &table_name,
                &commit_tx_log_seq_num_partition,
                &tx_uuid,
                &branch_uuid.to_string(),
                timeout,
                &request.comment,
                &request.author,
            )
            .await?;

        Ok(BeginTxResponse {
            tx_uuid,
            began_at_micros,
            expires_at_micros,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            tx_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn commit_tx(
        &self,
        pool: &PgDriver,
        request: &CommitTxRequest,
    ) -> Result<CommitTxResponse, ApiError> {
        // Tx ops are catalog-scoped; schema isn't needed.
        // commit_tx_log / begin_tx_log / abort_tx_log all live at the catalog
        // level, partitioned by branch. Accept catalog identifiers
        // (uuid or name); schema fields on the request are ignored
        // even if present, matching how the underlying tables are
        // actually keyed.
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;
        let tx_uuid = uuid::Uuid::parse_str(&request.tx_uuid).map_err(|_| {
            ApiError::InvalidRequest(format!("malformed tx_uuid: {}", request.tx_uuid))
        })?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("tx_uuid", tracing::field::display(&tx_uuid));

        let hot = HotStorageClient;
        let begin_partition = naming::begin_tx_log_partition(&catalog_uuid, &branch_uuid);
        let abort_partition = abort_tx_log_partition(&catalog_uuid, &branch_uuid);
        let tx_partition = commit_tx_log_partition(&catalog_uuid, &branch_uuid);

        // One Pg transaction so the FOR UPDATE lock acquired by
        // get_tx_status holds across the inner commit_tx INSERT.
        let committed = with_pg_tx(pool, async |tx| {
            // Status check + row lock on begin_tx_log. for_update=true
            // serializes us with concurrent commit_tx/abort_tx on the
            // same tx_uuid.
            let status = hot
                .get_tx_status(
                    tx,
                    &begin_partition,
                    &abort_partition,
                    &tx_partition,
                    &tx_uuid,
                    /*for_update=*/ true,
                )
                .await?;
            match status {
                None => {
                    return Err(ApiError::NotFound(format!(
                        "transaction not found on branch {branch_uuid}: {tx_uuid}"
                    )));
                }
                Some(penca_storage_hot::TxStatus::Aborted {
                    aborted_at_micros, ..
                }) => {
                    return Err(ApiError::FailedPrecondition(format!(
                        "transaction aborted at {aborted_at_micros}: {tx_uuid}"
                    )));
                }
                Some(penca_storage_hot::TxStatus::Committed { commit_micros }) => {
                    return Err(ApiError::FailedPrecondition(format!(
                        "transaction already committed at {commit_micros}: {tx_uuid}"
                    )));
                }
                Some(penca_storage_hot::TxStatus::Expired { expired_at_micros }) => {
                    return Err(ApiError::FailedPrecondition(format!(
                        "transaction expired at {expired_at_micros}: {tx_uuid}"
                    )));
                }
                Some(penca_storage_hot::TxStatus::Open { .. }) => {} // proceed
            }

            // Status check + FOR UPDATE above is the source of truth.
            // commit_open_tx is now an unconditional INSERT-from-begin_tx_log
            // — the lock guarantees the begin row is present and final,
            // and the status check guarantees we're in Open state.
            let commit_tx_log_seq_num_partition =
                naming::commit_tx_log_seq_num_partition(&catalog_uuid, &branch_uuid);
            let committed = hot
                .commit_open_tx(
                    tx,
                    &tx_partition,
                    &begin_partition,
                    &commit_tx_log_seq_num_partition,
                    &tx_uuid,
                )
                .await?;
            Ok(committed)
        })
        .await?;

        Ok(CommitTxResponse {
            commit_micros: committed.commit_micros,
            commit_seq_num: committed.commit_seq_num,
        })
    }

    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            tx_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn abort_tx(
        &self,
        pool: &PgDriver,
        request: &AbortTxRequest,
    ) -> Result<AbortTxResponse, ApiError> {
        // See `commit_tx` (RPC) — tx ops are catalog-scoped, schema is ignored.
        let catalog = resolve_catalog(
            pool,
            request.catalog_uuid.as_deref(),
            request.catalog_name.as_deref(),
        )
        .await?;
        let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
        let branch = resolve_branch(
            pool,
            &catalog_uuid,
            request.branch_uuid.as_deref(),
            request.branch_name.as_deref(),
        )
        .await?;
        let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;
        let tx_uuid = uuid::Uuid::parse_str(&request.tx_uuid).map_err(|_| {
            ApiError::InvalidRequest(format!("malformed tx_uuid: {}", request.tx_uuid))
        })?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("tx_uuid", tracing::field::display(&tx_uuid));

        let hot = HotStorageClient;
        let begin_partition = naming::begin_tx_log_partition(&catalog_uuid, &branch_uuid);
        let abort_partition = abort_tx_log_partition(&catalog_uuid, &branch_uuid);
        let tx_partition = commit_tx_log_partition(&catalog_uuid, &branch_uuid);
        // Aborts allocate from the dedicated abort-order counter.
        let seq_num_partition = naming::abort_seq_num_partition(&catalog_uuid, &branch_uuid);

        // See commit_tx (above) for the Pg-tx + FOR UPDATE locking story.
        let aborted_at_micros = with_pg_tx(pool, async |tx| {
            let status = hot
                .get_tx_status(
                    tx,
                    &begin_partition,
                    &abort_partition,
                    &tx_partition,
                    &tx_uuid,
                    /*for_update=*/ true,
                )
                .await?;
            match status {
                None => Err(ApiError::NotFound(format!(
                    "transaction not found on branch {branch_uuid}: {tx_uuid}"
                ))),
                // Aborting an already-aborted or expired tx is idempotent
                // — skip the INSERT and return success. The user gets the
                // same effective outcome (tx is no longer open); we surface
                // the existing/effective abort timestamp so the response
                // shape is uniform across paths.
                Some(penca_storage_hot::TxStatus::Aborted {
                    aborted_at_micros, ..
                })
                | Some(penca_storage_hot::TxStatus::Expired {
                    expired_at_micros: aborted_at_micros,
                }) => Ok(aborted_at_micros),
                // Already committed → can't abort.
                Some(penca_storage_hot::TxStatus::Committed { commit_micros }) => {
                    Err(ApiError::FailedPrecondition(format!(
                        "transaction already committed at {commit_micros}: {tx_uuid}"
                    )))
                }
                // Open: the FOR UPDATE lock above guarantees no existing
                // abort_tx_log row, so the INSERT below is unconditional
                // and the Pg-set `aborted_at_micros` flows back.
                Some(penca_storage_hot::TxStatus::Open { .. }) => {
                    let aborted_at_micros = hot
                        .abort_tx(
                            tx,
                            &abort_partition,
                            &begin_partition,
                            &seq_num_partition,
                            &tx_uuid,
                        )
                        .await?;
                    Ok(aborted_at_micros)
                }
            }
        })
        .await?;

        Ok(AbortTxResponse { aborted_at_micros })
    }

    /// Apply mutations against a branch. All changes are batched in a
    /// single Pg transaction for one round trip.
    ///
    /// `tx_uuid` is mode-switching:
    ///   - `None` → auto-commit. Server opens + commits a penca tx,
    ///     returns it on the response.
    ///   - `Some(id)` → append to the already-open penca tx with that
    ///     uuid. `author` / `comment` must be unset.
    #[tracing::instrument(
        skip_all,
        level = "debug",
        fields(
            catalog_uuid = tracing::field::Empty,
            branch_uuid = tracing::field::Empty,
            table_uuid = tracing::field::Empty,
        ),
    )]
    pub async fn write_data<L: DlDriver + ?Sized>(
        &self,
        pool: &PgDriver,
        dl_driver: &L,
        request: &WriteDataRequest,
    ) -> Result<WriteDataResponse, ApiError> {
        // Resolve identifiers once at the boundary, read-symmetric with
        // `read_data`, through the same cached `QueryManager` resolver. The
        // by-uuid target resolves catalog-wide with no schema touched; the
        // by-name target resolves the schema once. Cache eligibility falls out
        // of the snapshot: an autocommit write (no request tx_uuid) resolves at
        // LatestSeq → cache-eligible; an append (open tx_uuid) → OpenTx →
        // bypass. The write-side system guard then validates the canonical
        // uuids — `assert_not_system_table` always, `assert_not_system_schema`
        // only when a schema was actually resolved (the by-name path).
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        let table = scope
            .table_row
            .as_ref()
            .ok_or_else(|| ApiError::Internal("resolve_table did not populate table_row".into()))?;

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));
        span.record("table_uuid", tracing::field::display(&table_uuid));

        let commit_micros = with_pg_tx(pool, async |tx| {
            let (tx_uuid_str, committed) = resolve_or_auto_commit_tx(
                tx,
                &catalog_uuid,
                &branch_uuid,
                request.tx_uuid.as_deref(),
                request.author.as_deref(),
                request.comment.as_deref(),
            )
            .await?;

            if let Some(change) = request.change.as_ref() {
                Self::apply_change(
                    tx,
                    &catalog_uuid,
                    &branch_uuid,
                    &table_uuid,
                    table,
                    change,
                    &tx_uuid_str,
                )
                .await?;
            }

            Ok(committed.map(|c| c.commit_micros))
        })
        .await?;

        Ok(WriteDataResponse { commit_micros })
    }

    /// Explicit `tx_table_log` emit for the write paths that bypass
    /// `apply_change` — the DDL handlers (direct SQL into
    /// `__penca_system__.{schemas,tables}`), `merge_branch` (bulk
    /// INSERT-FROM-SELECT), and `materialize_metadata_from_source`'s fork emit.
    /// `apply_change` emits inline instead.
    async fn emit_tx_table_log_for_ddl(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        branch_uuid_str: &str,
        tx_uuid: &str,
        table_uuids: &[String],
    ) -> Result<(), ApiError> {
        if table_uuids.is_empty() {
            return Ok(());
        }
        let tx_table_part = tx_table_log_partition(catalog_uuid, branch_uuid);
        HotStorageClient
            .insert_tx_table_log(
                driver,
                &tx_table_part,
                branch_uuid_str,
                tx_uuid,
                table_uuids,
            )
            .await?;
        Ok(())
    }

    /// Emit one `tx_table_log` row for a DDL that wrote exactly
    /// `__penca_system__.schemas`. Thin delegate to the bulk variant, hiding
    /// the one-element slice literal at the call site.
    async fn emit_tx_table_log_for_schemas_change(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        branch_uuid_str: &str,
        tx_uuid: &str,
    ) -> Result<(), ApiError> {
        Self::emit_tx_table_log_for_ddl(
            driver,
            catalog_uuid,
            branch_uuid,
            branch_uuid_str,
            tx_uuid,
            &[system_schemas_table_uuid(catalog_uuid).to_string()],
        )
        .await
    }

    /// Emit one `tx_table_log` row for a DDL that wrote exactly
    /// `__penca_system__.tables`. Thin delegate to the bulk variant.
    async fn emit_tx_table_log_for_tables_change(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        branch_uuid_str: &str,
        tx_uuid: &str,
    ) -> Result<(), ApiError> {
        Self::emit_tx_table_log_for_ddl(
            driver,
            catalog_uuid,
            branch_uuid,
            branch_uuid_str,
            tx_uuid,
            &[system_tables_table_uuid(catalog_uuid).to_string()],
        )
        .await
    }

    /// Apply one `Change`'s row payload (upserts + deletes) to its
    /// pre-resolved target table within an existing transaction, then emit the
    /// `tx_table_log` row iff rows were written.
    ///
    /// The target table is resolved once at the handler boundary and threaded
    /// in, so this neither re-resolves the snapshot nor loops per-`Change` — a
    /// write targets exactly one table. The system-table guard lives at the
    /// boundary too.
    ///
    /// TODO(CHA-92): validate the tx is open before appending — the predicate
    /// is `tx_uuid IN begin_tx_log AND tx_uuid NOT IN abort_tx_log` (and not
    /// yet committed / not expired). Today an append against an aborted or
    /// expired tx writes orphaned rows to the upsert/delete logs; merge-on-read
    /// filters them out via the commit_tx_log JOIN, so they're invisible to readers
    /// but still write amplification. Fold the check into the upsert/delete
    /// INSERTs as a single-roundtrip CTE so we keep one round trip.
    async fn apply_change(
        driver: &PgTransactionDriver,
        catalog_uuid: &uuid::Uuid,
        branch_uuid: &uuid::Uuid,
        table_uuid: &uuid::Uuid,
        table: &Table,
        change: &Change,
        tx_uuid: &str,
    ) -> Result<(), ApiError> {
        if change.upserts.is_empty() && change.deletes.is_empty() {
            return Ok(());
        }

        let hot = HotStorageClient;

        // Reuse the boundary-resolved `Table` for the upsert/delete row shape
        // rather than refetching. The by-uuid path resolved catalog-wide, so a
        // write whose `table_uuid` lives in a schema other than the request
        // `schema_uuid` still writes — matching the read side.
        let user_schema: SchemaRef = Arc::new(
            arrow::ipc::convert::try_schema_from_ipc_buffer(&table.arrow_schema)
                .map_err(ApiError::Arrow)?,
        );

        let mut wrote_rows = false;

        // Deletes-first. The delete INSERT MUST run before the upsert
        // INSERT so that within one batch the co-occurring delete and upsert of
        // a row draw their `write_seq_num` (the upsert/delete logs' shared
        // `write_sequence`, via the column `DEFAULT nextval`) in that order
        // — the delete gets the strictly lower ordinal, so `(commit_seq_num,
        // write_seq_num)` resolution places the upsert last (replace) with no
        // read-side tie special-case.
        if !change.deletes.is_empty() {
            let inserted = Self::insert_delete_pk_batches(
                driver,
                &hot,
                &delete_log_table(table_uuid, branch_uuid),
                table_uuid,
                tx_uuid,
                &change.deletes,
                &user_schema,
                &table.primary_keys,
            )
            .await?;
            if inserted > 0 {
                wrote_rows = true;
            }
        }

        if !change.upserts.is_empty() {
            let inserted = Self::insert_rows(
                driver,
                &hot,
                &upsert_log_table(table_uuid, branch_uuid),
                table_uuid,
                tx_uuid,
                &change.upserts,
                &table.primary_keys,
            )
            .await?;
            if inserted > 0 {
                wrote_rows = true;
            }
        }

        // One tx_table_log row when rows were actually written (empty IPC
        // batches don't count). Idempotent across multiple WriteData calls in
        // one penca tx via the (tx_uuid, branch_uuid, table_uuid) PK conflict.
        if wrote_rows {
            let tx_table_part = tx_table_log_partition(catalog_uuid, branch_uuid);
            hot.insert_tx_table_log(
                driver,
                &tx_table_part,
                &branch_uuid.to_string(),
                tx_uuid,
                &[table_uuid.to_string()],
            )
            .await?;
        }

        Ok(())
    }

    /// Decode Arrow IPC bytes carrying user-shape rows, derive `row_uuid`
    /// per row from the table's primary keys, mint a fresh `version_uuid`
    /// per row, and append to the upsert log. Returns the number of rows
    /// inserted (sum across all batches), so the caller can gate
    /// `tx_table_log` emission on actual rows written.
    ///
    /// Rejects duplicate `row_uuid` within the upserts of one
    /// `Change`. Two upsert rows with the same `row_uuid` would share a
    /// `version_uuid = hash(row_uuid, tx_uuid)` and silently collapse
    /// through `insert_upserts`' `ON CONFLICT (version_uuid) DO UPDATE`
    /// — the caller loses a row without learning. PG enforces the same
    /// invariant for `INSERT ... ON CONFLICT DO UPDATE` ("ON CONFLICT
    /// DO UPDATE command cannot affect row a second time"). The ON
    /// CONFLICT branch stays load-bearing for cross-Change same-row
    /// writes within one Penca tx (case 2 in ADR 0009), so the check
    /// has to live here at intra-batch granularity, not on the SQL.
    async fn insert_rows(
        driver: &PgTransactionDriver,
        hot: &HotStorageClient,
        table_name: &str,
        table_uuid: &uuid::Uuid,
        tx_uuid: &str,
        ipc_bytes: &[u8],
        primary_keys: &[String],
    ) -> Result<usize, ApiError> {
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).map_err(ApiError::Arrow)?;

        // Spans every IPC batch in this call so duplicates split across
        // two batches in one Change still surface.
        let mut seen_row_uuids: HashSet<Uuid> = HashSet::new();

        let mut total_rows: usize = 0;
        for batch_result in reader {
            let batch = batch_result.map_err(ApiError::Arrow)?;
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let schema = batch.schema();
            let user_columns: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            let pk_cols: Vec<&dyn arrow::array::Array> = primary_keys
                .iter()
                .map(|pk| {
                    batch.column_by_name(pk).map(|c| c.as_ref()).ok_or_else(|| {
                        ApiError::InvalidRequest(format!(
                            "primary key column '{pk}' not found in batch"
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;

            // Same null-PK rejection the validated-batch entry points apply —
            // a NULL would mint the empty-string identity.
            crate::pk_batch::ensure_no_null_pks(&pk_cols, primary_keys, "upserts")?;

            let tx_uuid_parsed: uuid::Uuid = tx_uuid
                .parse()
                .map_err(|e| ApiError::InvalidRequest(format!("invalid tx_uuid: {e}")))?;

            let mut version_uuids = Vec::with_capacity(num_rows);
            let mut row_uuids = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // Row identity via the shared pk_batch kernel — the same
                // stringify-then-hash step as deletes and ids.
                let row_uuid = crate::pk_batch::row_uuid_for_row(&pk_cols, row_idx, table_uuid)?;
                if !seen_row_uuids.insert(row_uuid) {
                    return Err(ApiError::InvalidRequest(format!(
                        "WriteData upserts contain duplicate row_uuid {row_uuid} \
                         for table {table_uuid}"
                    )));
                }
                // Deterministic version_uuid (ADR 0013) — PK on
                // version_uuid enforces the auditable-store invariant
                // (one version per (entity, tx)) without a separate
                // UNIQUE constraint.
                let version_uuid = naming::version_uuid(&row_uuid, &tx_uuid_parsed);
                version_uuids.push(version_uuid.to_string());
                row_uuids.push(row_uuid.to_string());
            }

            hot.insert_upserts(
                driver,
                table_name,
                &user_columns,
                &version_uuids,
                &row_uuids,
                tx_uuid,
                &batch,
            )
            .await?;
            total_rows += num_rows;
        }

        Ok(total_rows)
    }

    /// Decode an Arrow IPC PK batch from `Change.deletes`, validate
    /// column order and types against the table's declared
    /// `primary_keys`, derive `row_uuid` server-side via
    /// `naming::row_uuid_for_pk`, and append
    /// `(version_uuid, row_uuid, <pk_cols...>, tx_uuid)` rows to the
    /// delete log.
    ///
    /// Validation is strict on both column **name/order** and Arrow
    /// **data type**: a batch whose schema doesn't match the declared
    /// PK columns is rejected with `InvalidRequest` rather than
    /// silently re-hashing mismatched values. A wrong type (e.g.
    /// `binary` where the table declared `utf8`) would either mint a
    /// `row_uuid` that disagrees with the upsert side — making the
    /// delete a silent no-op — or surface as a cryptic Pg cast error
    /// at INSERT time; catching it here gives a clear caller-visible
    /// error.
    ///
    /// Returns the number of rows inserted across all batches.
    #[allow(clippy::too_many_arguments)]
    async fn insert_delete_pk_batches(
        driver: &PgTransactionDriver,
        hot: &HotStorageClient,
        table_name: &str,
        table_uuid: &uuid::Uuid,
        tx_uuid: &str,
        ipc_bytes: &[u8],
        user_schema: &SchemaRef,
        primary_keys: &[String],
    ) -> Result<usize, ApiError> {
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).map_err(ApiError::Arrow)?;

        let mut total_rows: usize = 0;
        for batch_result in reader {
            let batch = batch_result.map_err(ApiError::Arrow)?;
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            // Validation + derivation share the read path's kernel, so
            // write-side and read-side row identity agree by construction.
            let row_uuids: Vec<String> = crate::pk_batch::validated_row_uuids_from_batch(
                &batch,
                table_uuid,
                user_schema,
                primary_keys,
                "deletes",
            )?
            .iter()
            .map(|row_uuid| row_uuid.to_string())
            .collect();

            hot.insert_deletes(driver, table_name, &batch, &row_uuids, tx_uuid)
                .await?;
            total_rows += num_rows;
        }
        Ok(total_rows)
    }

    /// Copy schema and table metadata from the source branch onto the
    /// new branch and create empty per-branch data tables for every
    /// table.
    ///
    /// Each materialized row needs a `tx_uuid` for the auditable-store row
    /// identity. A single auto-committed `fork_tx` on the new branch (parallel
    /// to `merge_tx` for `MergeBranch`) stamps every materialization, since
    /// they are conceptually one operation — the branch creation.
    ///
    /// The walk is catalog-wide:
    ///   1. Read source's `__penca_system__.schemas` via
    ///      `resolve_schema_metadata` and copy each schema row onto
    ///      the new branch via `insert_schema_row`.
    ///   2. Read source's `__penca_system__.tables` via
    ///      `resolve_table_metadata` (no schema filter) and for each
    ///      table call `create_data_tables` + `materialize_table_metadata`
    ///      using the row's own `schema_uuid`.
    ///   3. Emit `tx_table_log` rows for fork_tx writing into both
    ///      `__penca_system__.{schemas,tables}` so consumers can
    ///      resolve the system tables via the standard
    ///      `(tx_uuid, table_uuid)` index.
    #[allow(clippy::too_many_arguments)]
    async fn materialize_metadata_from_source<L: DlDriver + ?Sized>(
        &self,
        driver: &PgTransactionDriver,
        dl_driver: &L,
        catalog_uuid: &Uuid,
        new_branch_uuid: &Uuid,
        source_branch_uuid: &Uuid,
        author: &str,
        comment: &str,
    ) -> Result<(), ApiError> {
        let catalog_str = catalog_uuid.to_string();
        let new_branch_str = new_branch_uuid.to_string();
        let source_branch_str = source_branch_uuid.to_string();

        // The materialization tx is always auto-commit — no caller-supplied
        // tx_uuid mode-switch — so `auto_commit_tx` is used directly to skip
        // the `Option<CommittedTx>` unwrap `resolve_or_auto_commit_tx` forces.
        //
        // fork_tx and the `tx_table_log` rows below are emitted
        // unconditionally: every branch starts with a fork_tx by construction,
        // and an empty-source fast path never fires in practice since every
        // catalog has `public` + `__penca_system__`.
        let (fork_tx_uuid, fork_committed) =
            auto_commit_tx(driver, catalog_uuid, new_branch_uuid, author, comment).await?;
        // The fork captures source's schemas + tables as-of the fork commit
        // point — a single consistent snapshot, never unbounded.
        let fork_as_of = fork_committed.commit_micros;

        // Routed through stream_merged so a post-persist source (rows live in
        // cold) is tolerated.
        let schema_batches = self
            .query_manager
            .resolve_schema_metadata(
                driver,
                dl_driver,
                &catalog_str,
                &source_branch_str,
                None,
                // Catalog-wide fork copy: no single row_uuid, no name key.
                None,
                None,
                &penca_merge::ReadSnapshot::AsOfMicros(fork_as_of),
            )
            .await?;
        materialize_schema_rows_from_batches(
            driver,
            &schema_batches,
            &catalog_str,
            &new_branch_str,
            &fork_tx_uuid,
        )
        .await?;

        // `schema_uuid = None` gives the catalog-wide read across every schema.
        let table_batches = self
            .query_manager
            .resolve_table_metadata(
                driver,
                dl_driver,
                &catalog_str,
                None,
                &source_branch_str,
                None,
                // Catalog-wide read: no single row_uuid, no name key.
                None,
                None,
                &penca_merge::ReadSnapshot::AsOfMicros(fork_as_of),
            )
            .await?;
        materialize_table_rows_from_batches(
            driver,
            &table_batches,
            &catalog_str,
            &new_branch_str,
            &fork_tx_uuid,
        )
        .await?;

        // Index definitions are inherited by the child branch, mirroring the
        // schemas/tables fork copy.
        let index_batches = self
            .query_manager
            .resolve_index_metadata(
                driver,
                dl_driver,
                &catalog_str,
                &source_branch_str,
                None,
                // Catalog-wide fork copy: no row_uuid, no name key, no
                // table_uuid prefix.
                None,
                None,
                None,
                &penca_merge::ReadSnapshot::AsOfMicros(fork_as_of),
            )
            .await?;
        let materialized_index_count = materialize_index_rows_from_batches(
            driver,
            &index_batches,
            &catalog_str,
            &new_branch_str,
            &fork_tx_uuid,
        )
        .await?;

        // fork_tx always wrote rows to `__penca_system__` schemas + tables on
        // the child branch, and those writes bypass WriteData, so the
        // membership rows are emitted explicitly — otherwise consumers cannot
        // resolve the system tables via `(tx_uuid, table_uuid)`.
        //
        // `indexes` is included ONLY when the fork actually copied index rows.
        // An empty fork writes nothing there, and a spurious tx_table_log entry
        // on that unpersisted table would pin PurgeTxLog's `min(purged_at)`
        // watermark at 0 forever.
        let mut touched = vec![
            naming::system_schemas_table_uuid(catalog_uuid).to_string(),
            naming::system_tables_table_uuid(catalog_uuid).to_string(),
        ];
        if materialized_index_count > 0 {
            touched.push(naming::system_indexes_table_uuid(catalog_uuid).to_string());
        }
        Self::emit_tx_table_log_for_ddl(
            driver,
            catalog_uuid,
            new_branch_uuid,
            &new_branch_str,
            &fork_tx_uuid,
            &touched,
        )
        .await?;

        Ok(())
    }
}

/// Return `Ok(())` if the target has no commits past the fork point, else
/// [`ApiError::InvalidRequest`]. The fork point is the source branch's stored
/// `fork_commit_seq_num` (the commit-order position it forked from), so the
/// guard is a direct seq comparison against the target's `commit_tx_log` — no
/// base-tx lookup, and never vacuous: a branch always carries a real fork
/// position.
///
/// Precondition: this comparison is only meaningful when `target` and the
/// source share one `commit_seq_num` origin — which holds for the fork-from-main
/// / merge-to-main paths because the child's counter is seeded from the fork
/// point. Merging a branch into a target it did not fork from is out of scope
/// here; TODO(CHA-5) real conflict detection subsumes this shortcut.
async fn ensure_fast_forward(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &uuid::Uuid,
    target_branch_uuid: &uuid::Uuid,
    source_fork_commit_seq_num: i64,
) -> Result<(), ApiError> {
    let target_tx_part = commit_tx_log_partition(catalog_uuid, target_branch_uuid);
    let target_tx_q = PgDialect::quote_identifier(&target_tx_part);
    // commit_tx_log is committed-only by construction (commits are inserted at
    // `CommitTx`; aborts go to `abort_tx_log`), and the seeded fork seq
    // makes source and target seqs comparable, so a target commit past the fork
    // point is exactly `commit_seq_num > fork_commit_seq_num`.
    let sql = format!("SELECT 1 FROM {target_tx_q} WHERE commit_seq_num > $1 LIMIT 1");
    let row = driver
        .fetch_optional(&sql, &[SqlValue::Int64(source_fork_commit_seq_num)])
        .await
        .map_err(|e| ApiError::Metadata(e.into()))?;
    if row.is_some() {
        return Err(ApiError::InvalidRequest(
            "merge requires fast-forward: target has commits past source's fork point".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema(field_names: &[&str]) -> Schema {
        Schema::new(
            field_names
                .iter()
                .map(|n| Field::new(*n, DataType::Int64, false))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn validate_create_table_primary_keys_accepts_well_formed_request() {
        let schema = schema(&["id", "name"]);
        validate_create_table_primary_keys(&["id".into()], &schema).expect("happy path");
        validate_create_table_primary_keys(&["id".into(), "name".into()], &schema)
            .expect("composite PK");
    }

    #[test]
    fn validate_create_table_primary_keys_rejects_empty_list() {
        let schema = schema(&["id"]);
        let err = validate_create_table_primary_keys(&[], &schema).expect_err("empty rejects");
        let msg = err.to_string();
        assert!(msg.contains("at least one primary key"), "{msg}");
        assert!(msg.contains("invalid request"), "{msg}"); // ApiError::InvalidRequest prefix
    }

    #[test]
    fn validate_create_table_primary_keys_rejects_duplicates() {
        let schema = schema(&["id"]);
        let err = validate_create_table_primary_keys(&["id".into(), "id".into()], &schema)
            .expect_err("duplicate rejects");
        let msg = err.to_string();
        assert!(msg.contains("listed more than once"), "{msg}");
        assert!(msg.contains("`id`"), "{msg}");
    }

    #[test]
    fn validate_create_table_primary_keys_rejects_undeclared_column() {
        let schema = schema(&["id"]);
        let err = validate_create_table_primary_keys(&["missing".into()], &schema)
            .expect_err("undeclared rejects");
        let msg = err.to_string();
        assert!(msg.contains("not declared in arrow_schema"), "{msg}");
        assert!(msg.contains("`missing`"), "{msg}");
    }

    #[test]
    fn validate_create_table_primary_keys_dedup_check_fires_before_membership_check() {
        // Both checks would fire on this input; dedup runs first so
        // the rejection wording must name "listed more than once" not
        // "not declared in arrow_schema".
        let schema = schema(&["id"]);
        let err =
            validate_create_table_primary_keys(&["missing".into(), "missing".into()], &schema)
                .expect_err("dedup-then-membership ordering");
        let msg = err.to_string();
        assert!(msg.contains("listed more than once"), "{msg}");
    }
}
