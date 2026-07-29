//! Write operations for branches, transactions, and data mutations.
//!
//! [`WriteManager`] implements branch management, transaction lifecycle,
//! and data mutations (inserts, updates, deletes). Methods accept and
//! return proto messages directly.
//!
//! This is the Rust port of `packages/penca/src/penca/lib/api/write.py`.

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
use penca_format::writer::FormatWriter;
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

// Table metadata column names from resolved auditable store.
// CHA-177: column names on `__penca_system__.tables` rows. The
// resolve CTE prepends `row_uuid` automatically — and `row_uuid` IS
// the canonical table_uuid, so it's not a user column.
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
    /// CHA-472: the rehomed metadata read methods (ADR 0028) are now
    /// `QueryManager` methods, so the write path reaches them through this
    /// handle (`self.query_manager.resolve_table_metadata(..)` etc.). Built with
    /// the snapshot-list + snapshot-segment caches enabled (wired in
    /// `penca_write.rs`), so a hot point-write resolve shares the query path's
    /// caches; a disabled cache is the per-service opt-out.
    pub query_manager: crate::query::QueryManager,
}

/// CHA-236 — reject DDL targeting `__penca_system__`. The system
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

/// CHA-236 — reject DDL / WriteData targeting the registered system tables
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

/// CHA-479: validate a resolved write-target-table scope and return the parsed
/// `table_uuid`. Shared by `write_data` and the table/index DDL handlers
/// (`update_table`, `delete_table`, `{create,update,delete}_index`) so the
/// resolve+validate layering lives in one place instead of the same block
/// repeated six times. Rejects the registered system tables
/// `__penca_system__.{schemas,tables,indexes}` via the canonical `table_uuid`
/// guard ALWAYS; rejects `__penca_system__` as a *schema* only when a schema
/// row was resolved — the by-name path. The by-uuid path derives the schema from
/// the resolved table row (true residency, CHA-381) and relies on the table
/// guard, so it never re-asserts a caller-supplied schema there.
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

/// CHA-479: validate a resolved write-target-schema scope and return the parsed
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

/// CHA-172 — validate `CreateTableRequest.primary_keys` against the
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
/// else through unchanged. CHA-236: name-uniqueness on catalog +
/// branch rename relies on PG `UNIQUE` constraints.
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

/// CHA-236 rename helper used by `update_schema` / `update_table`.
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

/// CHA-433 do-no-harm guard: on an update, a set `retention_duration_seconds` is
/// **immutable** — every change is rejected with `FAILED_PRECONDITION`. Both
/// directions are unsafe, for independent reasons:
///
/// - Loosening (a larger duration, or clearing it — an unset field replaces the
///   stored value on update, i.e. set -> unset = retain forever) would let a
///   time-travel read's historical retention fall *below* the current policy, so
///   the scope-based read floor could *wrongly reject* a valid read (see
///   CHA-511).
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

/// CHA-164 mode-switch shared between [`WriteManager::write_data`]
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
    // CHA-92: wire-shape checks (author/comment mutual-exclusion and
    // required-for-auto-commit) moved up to the servicer's
    // `validate_write_data`; the lib is "use at own risk" and no longer
    // re-defends them.
    match request_tx_uuid {
        Some(tx) => {
            // CHA-92: an append targets an existing tx — verify it is open
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

/// CHA-92: resolve an append-path `tx_uuid` to its open state.
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

/// CHA-181: which user tables on `source_branch_uuid` actually had a
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
            // CHA-380: schema_uuid is a first-class column; re-inserting it
            // through insert_schema_row re-derives the same row_uuid on the
            // child branch.
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
/// creates the deterministic per-branch data tables (CHA-177) and then
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
            // CHA-380: table_uuid is a first-class column (re-derives the
            // same row_uuid via materialize_table_metadata on the child).
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
            // CHA-177: per-branch data tables are deterministic in
            // `(table_uuid, branch_uuid)`.
            let arrow_schema_bytes = rb_binary(batch, STC_ARROW_SCHEMA, i).ok_or_else(|| {
                ApiError::InvalidRequest("__penca_system__.tables: missing arrow_schema".into())
            })?;
            let arrow_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&arrow_schema_bytes)
                .map_err(ApiError::Arrow)?;

            // CHA-177: partition/clustering/primary_keys are PG `text[]`
            // (arrow `list<utf8>` → `text[]`).
            // CHA-185: primary_keys must be read before create_data_tables
            // so the delete_log DDL can carry the PK columns.
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

/// Copy `__penca_system__.indexes` rows onto the forked branch (CHA-455),
/// mirroring [`materialize_table_rows_from_batches`]. `index_uuid` is the
/// row's own PK column (CHA-380); `table_uuid` is the owning table; the
/// remaining columns are the index definition.
/// Returns the number of index rows materialized — the caller emits the
/// `__penca_system__.indexes` tx_table_log membership only when this is
/// non-zero. An empty fork (parent has no indexes) writes no rows there,
/// and a spurious membership on an unpersisted table pins PurgeTxLog's
/// `min(purged_at)` watermark at 0 forever (CHA-455).
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
            // CHA-380: index_uuid is a first-class column (re-derives the same
            // row_uuid via materialize_index_metadata on the child).
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
    // -- Branch -----------------------------------------------------------

    /// Reject a `CreateBranch` whose source branch is not the catalog's `main`.
    ///
    /// CHA-515 interim guard. The CreateBranch handler calls this **before**
    /// `PersistBranch` flushes the source hot→cold, so a rejected non-main fork
    /// touches nothing. The read planner is single-level (CHA-178), so a fork
    /// off a non-main branch would silently drop grandparent rows on read.
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
    /// CHA-184: catalog-scoped — the request takes only the catalog
    /// identifier, and the materialization walks every schema visible
    /// on the source branch.
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

        // CHA-236: mint random `branch_uuid` server-side. Tests can
        // still pass an explicit `branch_uuid` for setup determinism.
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
        // CHA-178: the resolved source branch is the child's parent lineage,
        // recorded on branch_store so the read planner can enumerate the
        // parent's cold tier as a second source.
        let source_branch_str = source_branch_uuid.to_string();

        // CHA-273 rework: the SOURCE branch hot→cold flush already happened —
        // the create_branch gRPC handler resolved the fork position (`fork`,
        // `resolve_fork_watermark`) and called PersistBranch (in the lifecycle
        // pod) BEFORE this. So everything committed on the source at/before the
        // fork is already durable in cold (what the child's cross-branch cold
        // read, CHA-178, consumes). We record the fork and seed the child from it
        // here; no persist runs in the write pod. `fork` is the resolved fork
        // position (`resolve_fork_watermark` errored if it named no committed
        // tx), and `fork.commit_seq_num` is what we record and seed the child from.
        //
        // INVARIANT (load-bearing for CHA-178): PersistBranch bounds the source
        // cold tier by `commit_micros <= fork.commit_micros`, but `commit_micros`
        // is only *non-strictly* monotonic — a source commit in the SAME
        // microsecond as the fork (with a higher `commit_seq_num`) can leak into
        // the source cold tier. Harmless (persist is idempotent; the row is on the
        // source), but the child's "sees nothing committed after the fork"
        // guarantee MUST be enforced by CHA-178 filtering the parent-cold read on
        // `commit_seq_num <= fork.commit_seq_num` (the seeded fork seq, CHA-487),
        // NOT on micros. Do not give CHA-178's parent-cold ceiling a micros bound.
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
                    // CHA-178: the resolved source branch is the parent lineage
                    // the read planner's parent-cold source keys on.
                    Some(source_branch_str.as_str()),
                )
                .await,
                "branch",
            )?;

            LifecycleManager::ensure_branch_partitions(tx, &catalog_str, &branch_str).await?;

            // CHA-487: seed the child's commit_seq_num counter from the fork
            // commit T so the child's seqs (> commit_seq_num(T)) are disjoint
            // from the parent's (<= commit_seq_num(T)); the existing
            // latest-wins-on-commit_seq_num resolution then shadows the parent
            // with no lineage tiebreak (the substrate CHA-178's cross-branch read
            // consumes). Must precede materialize_metadata_from_source, whose
            // fork/materialization tx is the child's first commit and must
            // allocate the seeded value.
            //
            // CHA-273 rework: seed from `fork.commit_seq_num` — T resolved ONCE
            // under PersistBranch — not a fresh MAX re-read here, closing the
            // window where a source commit between the flush and this read bumps
            // MAX past T.
            //
            // TODO(CHA-178): the remaining seam is the fork metadata-read axis.
            // materialize_metadata_from_source reads source metadata bounded on
            // micros, so a source DDL in the same micros as T (seq > fork_seq)
            // could be inherited while sitting above this seed; CHA-178 should
            // bound the source metadata read by seq (AsOfSeq(fork_seq)) to close
            // it, and give the parent-cold read ceiling the same fork_seq bound.
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

    /// Delete a branch with 3-phase cleanup.
    ///
    /// Phase 1: collect cold storage URIs.
    /// Phase 2: delete cold files via FormatWriter.
    /// Phase 3: transactional metadata cleanup.
    ///
    /// CHA-184: catalog-scoped — phases 1–3 walk every schema's tables
    /// on the branch, so cold segments + log/snapshot metadata for
    /// `s1.t1`, `s2.t2`, ... are all cleaned up.
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
        writer: &impl FormatWriter,
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

        let span = tracing::Span::current();
        span.record("catalog_uuid", tracing::field::display(&catalog_uuid));
        span.record("branch_uuid", tracing::field::display(&branch_uuid));

        let catalog_str = catalog_uuid.to_string();
        let branch_str = branch_uuid.to_string();

        // Phase 1: Collect all cold storage file URIs.
        // CHA-177: data tables = `data_log_prefix(table_uuid, branch)_data_*`,
        // deterministic per-branch.
        // CHA-184: schema_uuid=None → catalog-wide table list.
        let table_uuid_strs = self
            .query_manager
            .list_table_uuids_for_branch(pool, dl_driver, &catalog_str, None, &branch_str)
            .await?;

        // CHA-203/CHA-218: enumerate cold persist segments per
        // `(branch, table)`. Cold no longer holds commit_tx_log, so the
        // touched set is just the data tables. Snapshot segments key
        // directly on `(branch, table)`.
        let segment_table_uuids: Vec<&str> = table_uuid_strs.iter().map(String::as_str).collect();

        let persist_segments = LifecycleManager::get_table_persist_segments_for_tables(
            pool,
            &catalog_str,
            &branch_str,
            &segment_table_uuids,
        )
        .await?;

        let mut snap_segments: Vec<(String, String)> = Vec::new();
        for table_uuid_str in &table_uuid_strs {
            let segs = LifecycleManager::get_snapshot_segments_for_table(
                pool,
                &catalog_str,
                &branch_str,
                table_uuid_str,
            )
            .await?;
            snap_segments.extend(segs);
        }

        // CHA-202: also enumerate in-flight compact merged files
        // tracked in `compact_segment_metadata`. Two cases:
        //   - committed rows: the merged file is still referenced by
        //     `table_*_segment_metadata` rows on the branch and is
        //     covered by the persist/snap enumerations above. The
        //     overlap is harmless — `writer.delete` is idempotent.
        //   - NULL rows (crashed-mid-compact orphans): no segment
        //     metadata points at the merged file, so without this
        //     enumeration the file would leak past the partition
        //     CASCADE in Phase 3.
        let compact_uris =
            LifecycleManager::get_compact_segment_uris_for_branch(pool, &catalog_str, &branch_str)
                .await?;

        // Phase 2: Delete cold storage files. Best-effort: the metadata
        // rows pointing at them go away in Phase 3 via DROP PARTITION
        // CASCADE regardless of which file deletes succeed, so a
        // residual orphan-file scan would be the only follow-up needed.
        for (_, uri) in &persist_segments {
            let _ = writer.delete(uri, true).await;
        }
        for (_, uri) in &snap_segments {
            let _ = writer.delete(uri, true).await;
        }
        for uri in &compact_uris {
            let _ = writer.delete(uri, true).await;
        }

        // Phase 3: Transactional metadata cleanup.
        with_pg_tx(pool, async |tx| {
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

                // CHA-198: drop_branch_partitions removes the per-branch
                // leaves of branch_persist_metadata, table_persist_metadata,
                // table_persist_segment_metadata, table_snapshot_metadata,
                // and table_snapshot_segment_metadata via DROP TABLE
                // CASCADE — so explicit per-row DELETEs against those
                // parents would be redundant. Only the cold-storage file
                // deletes (Phase 2 above) and the data-table drops above
                // are tier-specific and stay here.
                LifecycleManager::drop_branch_partitions(tx, &catalog_str, &branch_str).await?;
            }
            Ok(())
        })
        .await?;

        Ok(DeleteBranchResponse {})
    }

    /// Rename a branch (CHA-236). Branches are catalog-scoped; the
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
    /// CHA-184: catalog-scoped — the merge fans out across every
    /// schema's tables on the source branch (driven by source's
    /// `tx_table_log` since fork, intersected with source's
    /// catalog-wide `__penca_system__.tables`).
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

            // Lock the catalog's source-branch commit_tx_log partition to serialize
            // with concurrent commits. After CHA-163 the partition is
            // catalog-scoped, so this lock blocks new commits on the source
            // branch from any schema in the catalog — broader than the
            // pre-CHA-163 per-schema lock, which is the right scope for
            // multi-schema-coherent merges.
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
            // auto-commit branch and CHA-164 DDL auto-commits.
            let (merge_tx_uuid, committed) = auto_commit_tx(
                tx,
                &catalog_uuid,
                &target_branch_uuid,
                &request.author,
                &request.comment,
            )
            .await?;
            let commit_micros = committed.commit_micros;

            // CHA-181: drive the merge loop from source's tx_table_log
            // (not source's full table-metadata) so we only call
            // merge_table_data on tables source actually wrote to since
            // fork. merge_table_data is INSERT-FROM-SELECT against
            // source's per-table upsert/delete log; on a table source
            // never wrote to, it scans the empty post-fork window and
            // writes nothing. With N=50 tables and writes to 3, today's
            // shape pays 94 wasted SQL calls per merge — the index exists
            // for exactly this kind of "tables this branch wrote to since
            // X" lookup, so use it on the merge call site too.
            //
            // (Source's `commit_tx_log_partition` only contains source-branch txs,
            // all of which are post-fork by definition, so the JOIN's
            // committed-only filter is the precise predicate without
            // needing fork-point arithmetic.)
            let touched_table_uuids =
                enumerate_touched_table_uuids(tx, &hot, &catalog_uuid, &source_branch_uuid).await?;

            // Fetch full table metadata for source. We still need user
            // tables' arrow_schema + naming for `merge_table_data`; only
            // the per-table merge_table_data calls themselves are pruned.
            // CHA-168: read goes through stream_merged so it tolerates
            // post-persist state where __penca_system__.tables rows live
            // in cold.
            // CHA-184: schema_uuid=None → catalog-wide read so the merge
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
                    // Catalog-wide read (every schema/table on the branch) —
                    // no single row_uuid (CHA-473) / no name key (CHA-484).
                    None,
                    None,
                    // CHA-86: read source's metadata as-of the merge commit
                    // point rather than unbounded.
                    &penca_merge::ReadSnapshot::AsOfMicros(commit_micros),
                )
                .await?;

            // CHA-181: collect distinct user table_uuids merge_tx writes
            // to on the target branch so we can emit the tx_table_log
            // membership rows after the loop. merge_table_data is bulk
            // INSERT-FROM-SELECT (not WriteData-shape) so the standard
            // apply_change emit doesn't fire.
            let mut merged_table_uuids: Vec<String> = Vec::with_capacity(touched_table_uuids.len());
            for batch in &table_batches {
                for i in 0..batch.num_rows() {
                    // CHA-380: table_uuid is a first-class column (was the
                    // overloaded row_uuid); it must match touched_table_uuids.
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

    // -- Catalog DDL ------------------------------------------------------

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
        // CHA-236: mint random `catalog_uuid` + `main_branch_uuid` +
        // `public_schema_uuid` server-side. Clients cannot recompute
        // these — they capture them from the response (CreateCatalog
        // returns the catalog + its main_branch_uuid; ListSchemas /
        // GetSchema returns the public schema).
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
        // their four self-describing bootstrap rows (CHA-177).
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
        // CHA-236: `new_catalog_name`, when set, renames the catalog
        // in place. `catalog_store.UNIQUE(catalog_name)` enforces
        // uniqueness; a collision surfaces as `unique_violation` →
        // `AlreadyExists`.
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
        // per-catalog physicals. CHA-236: main is random and threaded
        // into the cascade + drop.
        let main_branch = resolve_main_branch_uuid(driver, &catalog_uuid).await?;
        span.record("main_branch_uuid", tracing::field::display(&main_branch));
        let main_branch_str = main_branch.to_string();

        let schema_uuids = self
            .query_manager
            .list_schema_uuids_for_catalog(driver, dl_driver, &catalog_str, None)
            .await?;

        // CHA-236: skip the system schema in the cascade — it's a
        // structural anchor managed by `create_catalog_tables` and the
        // whole catalog physicals get CASCADE-dropped below anyway.
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

    // -- Schema DDL -------------------------------------------------------

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
        // CHA-236: mint random `schema_uuid` server-side. CHA-479: base-only
        // resolve through the shared `ResolvedScope` (CreateSchema carries no
        // schema ident, so `scope.schema_uuid` stays `None`), then mint. No
        // `assert_not_system_*`: a freshly minted v4 cannot collide with the
        // deterministic `system_schema_uuid(catalog)`.
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
            // CHA-236: explicit name-uniqueness pre-check (pg_now-pinned
            // snapshot + RYOW if joining an open tx). `__penca_system__.schemas`
            // has no PG UNIQUE constraint (rows live in an auditable-store
            // append log), so we enforce within-tx visibility here.
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
        // CHA-236: name → uuid uses a pg_now-pinned snapshot + RYOW under
        // the request's tx (no `as_of_micros` on writes). Branch resolves first via
        // `branch_store` SELECT (snapshot-blind). Reject
        // `__penca_system__` before opening a Pg tx — runs inside the
        // constructor.
        // CHA-479: resolve through the shared `ResolvedScope`, then layer the
        // write-only `__penca_system__` guard on top (the assert moved out of
        // the old `WriteRequestScope` constructor). UpdateSchema always carries
        // a schema ident, so `scope.schema_uuid` is `Some`.
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
            // CHA-164: look up the existing schema to carry forward its name
            // when no rename was requested. RYOW honoured.
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

            // CHA-236: when `new_schema_name` is set, rename. Reject if
            // another schema on this branch already uses the target name
            // (visible under our snapshot + open tx).
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
                None, // CHA-443: as_of_seq — inert on the OpenTx (RYOW) arm
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
        // CHA-479: resolve through the shared `ResolvedScope`, then reject
        // `__penca_system__` (the assert moved out of the old constructor).
        // DeleteSchema always carries a schema ident.
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
            None, // CHA-443: as_of_seq
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

        // CHA-177: soft-delete only — write tombstones for each table
        // and the schema. Physical data tables stay addressable;
        // lifecycle sweep drops them after commit.
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

        // CHA-181: emit one row per system table this cascade actually
        // wrote to. Schemas always (the schema tombstone). Tables only
        // if the cascade hit any user tables.
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

    // -- Table DDL --------------------------------------------------------

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
        // CHA-172: validate primary_keys + arrow_schema at the API
        // boundary so SQL (via penca-sql-server) and direct gRPC
        // callers see identical wording for the three reachable
        // PK bugs (empty / duplicate / undeclared). The PK list is
        // semantically meaningful regardless of how the request
        // reached us, so the validation belongs here, not at the
        // SQL parser. Runs before any I/O.
        //
        // CHA-386: the supported-column-type gate is NOT here — it is
        // enforced upstream in `penca-server-grpc`'s
        // `validation::write::validate_create_table` (the convergence
        // point both wire paths share: direct gRPC callers and the SQL
        // DDL path, which dispatches over `WriteServiceClient` and
        // re-enters that same servicer). Any future *in-process* caller
        // of `create_table` that bypasses the gRPC servicer must
        // replicate the `CanonicalType::from_arrow` check on every
        // column itself — the asymmetry with the in-crate PK check above
        // is intentional, not an oversight.
        let user_schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&request.arrow_schema)
            .map_err(ApiError::Arrow)?;
        validate_create_table_primary_keys(&request.primary_keys, &user_schema)?;

        // CHA-236: refuse CreateTable in `__penca_system__` — its
        // contents are managed by the structural bootstrap, not user
        // CRUD. Reject before opening a tx — runs inside the
        // constructor.
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

        // CHA-236: mint random `table_uuid` server-side.
        let table_uuid = Uuid::new_v4();
        span.record("table_uuid", tracing::field::display(&table_uuid));

        // CHA-236: name-uniqueness pre-check + the DDL + the metadata
        // INSERT run in one Pg tx so concurrent readers can't observe
        // commit_tx_log committed before the metadata row lands, and a
        // duplicate `(branch, schema, name)` from a concurrent
        // CreateTable fails as `AlreadyExists` instead of silently
        // last-write-wins.
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
            // CHA-177: per-branch data tables are deterministic in
            // `(table_uuid, branch_uuid)`. Concurrent CreateTable on the
            // same `(table, branch)` shares the data table.
            //
            // `user_schema` was decoded and the PK list validated
            // before the tx opened (CHA-172) — reused here.

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

            // CHA-455: inline index definitions are written into
            // `__penca_system__.indexes` in the SAME tx as the table, so
            // they commit/abort atomically with it. Each gets a random
            // server-minted index_uuid (mirroring the table_uuid mint).
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

    /// CHA-455: define a secondary index on a table. Writes a row into the
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
        // CHA-236-style server-minted random uuid.
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

    /// CHA-455: rename an index (rename-only — column/type changes are a
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

    /// CHA-455: drop an index. Soft-delete tombstone into
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
        // CHA-236: reject UpdateTable targeting `__penca_system__.*` —
        // runs inside the constructor.
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        // CHA-479 wrinkle #2: the by-uuid path derives schema_uuid from the
        // resolved table row (true residency, CHA-381) rather than the request's
        // schema — the same identifier dispatch read_data/write_data use
        // (table_uuid wins over schema_uuid + table_name). resolve_table always
        // populates schema_uuid.
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

            // CHA-236: when `new_table_name` is set, rename. Reject if
            // another table on this branch already uses the target name
            // (visible under our snapshot + open tx).
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
            // CHA-177: per-branch data tables are deterministic in
            // `(table_uuid, branch_uuid)` — same names as Create, no
            // carry-forward needed.
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
                None, // CHA-443: as_of_seq — inert on the OpenTx (RYOW) arm
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
        // CHA-236: reject DeleteTable targeting `__penca_system__.*` —
        // runs inside the constructor.
        let scope =
            ResolvedScope::resolve_table(&self.query_manager, pool, dl_driver, request, None)
                .await?;
        let catalog_uuid = scope.catalog_uuid;
        let branch_uuid = scope.branch_uuid;
        let table_uuid = validate_write_target_table(&scope)?;
        // CHA-479: schema_uuid is derived from the resolved table row on the
        // by-uuid path (true residency); delete_table's existence check is keyed
        // on table_uuid, so the schema is recorded for tracing only.
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

            // CHA-177: soft-delete only — write tombstone to
            // `__penca_system__.tables` iff the table is visible at the
            // request's open tx (RYOW honoured for tables created in the
            // same tx). The existence check + insert run in one query;
            // `false` means the table didn't exist → NotFound. Data
            // tables stay addressable; lifecycle sweep drops them after
            // commit.
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

    // -- Transactions -----------------------------------------------------

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
            // CHA-92: an over-max ttl is rejected at the servicer boundary
            // (validate_begin_tx), so there's no clamp here — embedded lib
            // callers pass the value through at their own risk.
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
        // Tx ops are catalog-scoped (CHA-163); schema isn't needed.
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
        // CHA-444: aborts allocate from the dedicated abort-order counter.
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

    // -- Data mutations ---------------------------------------------------

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
        // CHA-475: resolve the request's identifiers once at the boundary,
        // read-symmetric with `read_data` (`ResolvedScope::resolve_table`),
        // through the same cached `QueryManager` resolver (CHA-472). The by-uuid
        // target resolves catalog-wide with no schema touched (the eager
        // `__penca_system__.schemas` resolve is gone); the by-name target
        // resolves the schema once. Cache eligibility falls out of the snapshot:
        // an autocommit write (no request tx_uuid) resolves at LatestSeq →
        // cache-eligible; an append (open tx_uuid) → OpenTx → bypass. The
        // write-side system guard then validates the canonical uuids:
        // `assert_not_system_table` always, and `assert_not_system_schema` only
        // when a schema was actually resolved (the by-name path). The table guard
        // rejects the full registered system set
        // `__penca_system__.{schemas,tables,indexes}`.
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

    // -- Internal helpers -------------------------------------------------

    /// Process all changes (upserts, deletes) for a set of tables
    /// CHA-181: explicit `tx_table_log` emit for write paths that
    /// bypass `apply_change` — the 6 DDL handlers (which write rows
    /// to `__penca_system__.{schemas,tables}` via direct SQL),
    /// `merge_branch` (which uses bulk INSERT-FROM-SELECT via
    /// `merge_table_data`), and `materialize_metadata_from_source`'s
    /// fork emit. `apply_change` itself emits inline; these callers
    /// share this helper instead.
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

    /// CHA-181 wrapper: emit one `tx_table_log` row for a DDL that wrote
    /// exactly `__penca_system__.schemas` (Create/Update/DeleteSchema on
    /// a user schema). Thin delegate to `emit_tx_table_log_for_ddl`; the
    /// canonical entry-point is still the bulk variant — the wrapper
    /// just hides the one-element slice literal at the call site.
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

    /// CHA-181 wrapper: emit one `tx_table_log` row for a DDL that wrote
    /// exactly `__penca_system__.tables` (Create/Update/DeleteTable).
    /// Thin delegate to `emit_tx_table_log_for_ddl`; see
    /// [`Self::emit_tx_table_log_for_schemas_change`] for the rationale.
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
    /// CHA-475: the target table is resolved once at the handler boundary
    /// (`ResolvedScope::resolve_table`) and threaded in — this fn no longer
    /// re-resolves the snapshot or the table, and there is no per-`Change` loop
    /// or distinct-table dedup (a write targets exactly one table). The
    /// system-table guard also moved to the boundary.
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

        // CHA-387: reuse the `Table` resolved by the boundary
        // `ResolvedScope::resolve_table` for the upsert/delete row shape
        // (`arrow_schema`, `primary_keys`) — no second `get_table` refetch. The
        // by-uuid path resolved catalog-wide, so a write whose `table_uuid`
        // lives in a schema other than the request `schema_uuid` still writes,
        // matching the read side (CHA-381).
        let user_schema: SchemaRef = Arc::new(
            arrow::ipc::convert::try_schema_from_ipc_buffer(&table.arrow_schema)
                .map_err(ApiError::Arrow)?,
        );

        let mut wrote_rows = false;

        // CHA-431: deletes-first. The delete INSERT runs before the upsert
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

        // CHA-181: emit one tx_table_log row when rows were written (empty IPC
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
    /// CHA-242: rejects duplicate `row_uuid` within the upserts of one
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

            // CHA-398: same null-PK rejection the validated-batch entry
            // points apply — a NULL would mint the empty-string identity.
            crate::pk_batch::ensure_no_null_pks(&pk_cols, primary_keys, "upserts")?;

            let tx_uuid_parsed: uuid::Uuid = tx_uuid
                .parse()
                .map_err(|e| ApiError::InvalidRequest(format!("invalid tx_uuid: {e}")))?;

            let mut version_uuids = Vec::with_capacity(num_rows);
            let mut row_uuids = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // CHA-398: row identity via the shared pk_batch kernel —
                // same stringify-then-hash step as deletes and ids.
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
    /// delete log (CHA-185).
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

            // CHA-398: validation + derivation share the read path's
            // kernel — write-side and read-side row identity agree by
            // construction.
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
    /// CHA-164: each materialized row needs a `tx_uuid` for the
    /// auditable-store row identity. Auto-commit a single `fork_tx` on
    /// the new branch (parallel to `merge_tx` for `MergeBranch`),
    /// tagged with the request's `author` / `comment`, and stamp every
    /// materialization with that tx_uuid — they're conceptually one
    /// operation (the branch creation).
    ///
    /// CHA-184: the walk is catalog-wide.
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

        // CHA-174: the materialization tx is always auto-commit — no
        // caller-supplied tx_uuid mode-switch — so reach for
        // `auto_commit_tx` directly to skip the `Option<CommittedTx>`
        // unwrap that the mode-switching `resolve_or_auto_commit_tx`
        // would otherwise force. Local name `fork_tx_uuid` for symmetry
        // with `merge_tx` in `MergeBranch`.
        //
        // fork_tx and the two `tx_table_log` rows below are emitted
        // unconditionally — every branch starts with a fork_tx by
        // construction (catalog-wide consistency). The empty-source
        // fast path the pre-CHA-184 shape took never fires in practice
        // (every catalog has `public` + `__penca_system__`), so
        // dropping it removes a hidden branch without observable cost.
        let (fork_tx_uuid, fork_committed) =
            auto_commit_tx(driver, catalog_uuid, new_branch_uuid, author, comment).await?;
        // CHA-86: the fork captures source's schemas + tables as-of the
        // fork commit point — a single consistent snapshot, not unbounded.
        let fork_as_of = fork_committed.commit_micros;

        // -- Schemas ----------------------------------------------------
        //
        // Read source's `__penca_system__.schemas` rows. CHA-168:
        // routes through stream_merged so a post-persist source (rows live
        // in cold) is tolerated.
        let schema_batches = self
            .query_manager
            .resolve_schema_metadata(
                driver,
                dl_driver,
                &catalog_str,
                &source_branch_str,
                None,
                // Catalog-wide fork copy — no single row_uuid (CHA-473) / no
                // name key (CHA-484).
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

        // -- Tables -----------------------------------------------------
        //
        // Read source's `__penca_system__.tables` rows for every
        // schema (schema_uuid=None on resolve_table_metadata gives the
        // catalog-wide read).
        let table_batches = self
            .query_manager
            .resolve_table_metadata(
                driver,
                dl_driver,
                &catalog_str,
                None,
                &source_branch_str,
                None,
                // Catalog-wide read (every schema/table) — no single
                // row_uuid (CHA-473) / no name key (CHA-484).
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

        // -- Indexes (CHA-455) ------------------------------------------
        //
        // Copy source's `__penca_system__.indexes` rows so index
        // definitions are inherited by the child branch (mirroring the
        // schemas/tables fork copy).
        let index_batches = self
            .query_manager
            .resolve_index_metadata(
                driver,
                dl_driver,
                &catalog_str,
                &source_branch_str,
                None,
                // Catalog-wide fork copy — no single row_uuid (CHA-473) / no
                // name key (CHA-484) / no table_uuid prefix (CHA-499).
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

        // CHA-181 / CHA-184: fork_tx wrote rows to `__penca_system__`
        // schemas + tables on the child branch (always — every fork
        // inherits the source's schemas + tables). Those writes bypass
        // WriteData, so emit the membership rows explicitly so consumers
        // can resolve the system tables via `(tx_uuid, table_uuid)`.
        //
        // CHA-455: `indexes` is included ONLY when the fork actually
        // copied index rows (the source had indexes). An empty fork writes
        // nothing to `__penca_system__.indexes`, and a spurious
        // tx_table_log entry on that unpersisted table would pin
        // PurgeTxLog's `min(purged_at)` watermark at 0 forever.
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
/// position (subsumes the CHA-494 "absent base_tx ⇒ pass" concern).
///
/// Precondition: this comparison is only meaningful when `target` and the
/// source share one `commit_seq_num` origin — which holds for the fork-from-main
/// / merge-to-main paths because the child's counter is seeded from the fork
/// point (CHA-487). Merging a branch into a target it did not fork from is out
/// of scope here; real conflict detection (CHA-5) will subsume this shortcut.
async fn ensure_fast_forward(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &uuid::Uuid,
    target_branch_uuid: &uuid::Uuid,
    source_fork_commit_seq_num: i64,
) -> Result<(), ApiError> {
    let target_tx_part = commit_tx_log_partition(catalog_uuid, target_branch_uuid);
    let target_tx_q = PgDialect::quote_identifier(&target_tx_part);
    // commit_tx_log is committed-only by construction (commits are inserted at
    // `CommitTx`; aborts go to `abort_tx_log`), and the seeded fork seq (CHA-487)
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
