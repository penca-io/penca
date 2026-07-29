//! SQL DML → `WriteService::WriteData` translator.
//!
//! Maps each SQL DML verb onto the unified `WriteData` RPC
//! pair. `Change.upserts` ships user-shape Arrow IPC and the WriteService
//! derives `row_uuid` + mints `version_uuid` itself. penca-sql-server only
//! derives `row_uuid` here for two narrow purposes: building the
//! strict-INSERT collision-check IN-list, and populating `Change.deletes`
//! (which is wire-typed as `repeated string row_uuid`). See ADR 0006 for
//! the rationale.
//!
//! - `INSERT INTO t ...` (strict): SELECT collision check via
//!   `QueryServiceClient::read_data`, then `WriteData.change.upserts`.
//!   The check + write run inside a per-(branch, table) Pg advisory lock
//!   so two concurrent strict-INSERTs cannot both pass the check.
//! - `INSERT INTO t ... ON CONFLICT DO UPDATE`: `WriteData.change.upserts`
//!   with no collision check (last-writer-wins, no lock needed).
//! - `UPDATE t SET ... WHERE ...`: SELECT matching rows with SET expressions
//!   applied via DataFusion, then `WriteData.change.upserts`.
//! - `DELETE FROM t WHERE ...`: SELECT matching PK columns via DataFusion,
//!   derive `row_uuid`, then `WriteData.change.deletes`.
//!
//! `CommandStatementUpdate.transaction_id` is passed through verbatim
//! into `WriteDataRequest.tx_uuid` — presence-based, not content-based:
//! absent ⇒ auto-commit, present ⇒ append to that open tx. Empty-bytes
//! `transaction_id` from the Flight SQL wire is normalized to `None` at
//! the `flight_sql/service.rs` boundary; format validation of non-empty
//! values is the gRPC servicer's job, not the gateway's.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::compute::{cast, concat_batches};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ipc::convert::try_schema_from_ipc_buffer;
use arrow::ipc::reader::StreamReader;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::ParamValues;
use datafusion::execution::context::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Ident, Insert, ObjectName,
    ObjectNamePart, OnConflictAction, OnInsert, Statement as SqlStatement, TableFactor,
    TableObject,
};
use futures::StreamExt;
use penca_core::naming;
use penca_db::driver::pg::PgDriver;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::write_service_client::WriteServiceClient;
use penca_proto::external::v1::{
    Change, GetTableRequest, Projection, ReadDataRequest, Table, WriteDataRequest,
};
use tonic::Status;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::session::SessionSnapshot;
use crate::tx;

/// Recognition tag prepended to per-call UUID aliases for old-PK SELECT
/// columns in PK-changing UPDATEs. Collision-freedom comes
/// from the UUID portion — this prefix is purely cosmetic, so logs and
/// DataFusion error messages mentioning the alias are instantly
/// recognizable as Penca-internal rather than opaque hex.
const OLD_PK_ALIAS_TAG: &str = "__penca_old_pk_";

/// Internal error type for the advisory-locked critical sections in
/// `execute_insert` and `execute_update`. The Pg pool's
/// `advisory_lock` requires `E: From<sqlx::Error>`, but the rest of
/// the DML translator returns `tonic::Status`. Wrapping here keeps
/// the surface area at function boundaries — every public entry
/// point still surfaces a plain `Status`.
enum LockErr {
    Status(Status),
    Sqlx(sqlx::Error),
}

impl From<sqlx::Error> for LockErr {
    fn from(e: sqlx::Error) -> Self {
        LockErr::Sqlx(e)
    }
}

impl From<Status> for LockErr {
    fn from(s: Status) -> Self {
        LockErr::Status(s)
    }
}

impl From<LockErr> for Status {
    fn from(e: LockErr) -> Self {
        match e {
            LockErr::Status(s) => s,
            LockErr::Sqlx(e) => Status::internal(format!("advisory lock: {e}")),
        }
    }
}

/// Execute a parsed DML statement. Returns rows affected.
///
/// The caller is responsible for parsing (typically via
/// [`crate::parse::parse_one_statement`]) and for routing transaction-control
/// statements (`StartTransaction` / `Commit` / `Rollback`) to the
/// corresponding [`crate::tx`] handlers before reaching here.
///
/// `snapshot` is the per-request [`SessionSnapshot`] populated by the
/// session middleware; it carries the connection's pinned catalog (used
/// as the unqualified-DML default + for `tx::validate_session_catalog`),
/// the connection's pinned branch (addresses `WriteData` and the
/// strict-INSERT collision check) and any open `tx_uuid` (used
/// by `tx::resolve_tx_uuid_for_dml`). All session-state reads in the
/// DML hot path go through this snapshot — no direct cache lookups.
///
/// The unqualified-DML fallback schema is read from
/// `ctx.state().config_options().catalog.default_schema` — the same
/// source DataFusion's SELECT planner consults, so `SET search_path`
/// flows into both paths through one shared field.
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        kind = tracing::field::Empty,
        tx_uuid = tracing::field::Empty,
    ),
    err,
)]
// `params` stays a loose argument rather than a `DmlExecutor` field: bundling
// it would couple the executor's lifetime to a value only `execute_insert`
// consumes.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute(
    ctx: &SessionContext,
    write_channel: &Channel,
    query_channel: &Channel,
    pool: &PgDriver,
    snapshot: &SessionSnapshot,
    stmt: SqlStatement,
    transaction_id: Option<&str>,
    params: Option<ParamValues>,
) -> Result<i64, Status> {
    let default_schema = ctx.state().config_options().catalog.default_schema.clone();
    // Every wire payload routes by `branch_uuid` because it is rename-stable;
    // the connection's `branch_name` is diagnostic-only.
    let executor = DmlExecutor {
        ctx,
        write_channel,
        query_channel,
        pool,
        snapshot,
        default_schema: default_schema.as_str(),
        branch_uuid: snapshot.branch_uuid.as_str(),
    };
    match stmt {
        SqlStatement::Insert(insert) => {
            tracing::Span::current().record("kind", "insert");
            executor
                .execute_insert(insert, transaction_id, params)
                .await
        }
        SqlStatement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
            limit,
        } => {
            tracing::Span::current().record("kind", "update");
            if params.is_some() {
                return Err(Status::unimplemented(
                    "parameter binding for UPDATE is not yet supported; \
                     bind values via literal substitution or file a follow-up ticket",
                ));
            }
            if from.is_some() {
                return Err(Status::invalid_argument("UPDATE ... FROM is not supported"));
            }
            if returning.is_some() {
                return Err(Status::invalid_argument(
                    "UPDATE ... RETURNING is not supported",
                ));
            }
            if or.is_some() || limit.is_some() {
                return Err(Status::invalid_argument(
                    "UPDATE modifiers (OR/LIMIT) are not supported",
                ));
            }
            if !table.joins.is_empty() {
                return Err(Status::invalid_argument(
                    "UPDATE with JOIN is not supported",
                ));
            }
            let name = table_factor_name(&table.relation)?;
            executor
                .execute_update(name, assignments, selection, transaction_id)
                .await
        }
        SqlStatement::Delete(delete) => {
            tracing::Span::current().record("kind", "delete");
            if params.is_some() {
                return Err(Status::unimplemented(
                    "parameter binding for DELETE is not yet supported; \
                     bind values via literal substitution or file a follow-up ticket",
                ));
            }
            executor.execute_delete(delete, transaction_id).await
        }
        // `crate::gateway::classify` runs before this function and returns
        // `Err` for anything that isn't INSERT / UPDATE / DELETE. Reaching this
        // arm means the gateway invariant broke, so surface it as an internal
        // error rather than masking it as a user error.
        other => Err(Status::internal(format!(
            "dml::execute received non-DML statement `{other}`; \
             crate::gateway::classify is the invariant owner"
        ))),
    }
}

/// Per-call DML orchestrator, constructed by [`execute`].
///
/// All fields are borrows because the lifetime is the call —
/// `DmlExecutor` is constructed inside `execute()`, hands one method
/// off, and drops. No reuse across DMLs; no builder phase.
struct DmlExecutor<'a> {
    ctx: &'a SessionContext,
    write_channel: &'a Channel,
    query_channel: &'a Channel,
    pool: &'a PgDriver,
    snapshot: &'a SessionSnapshot,
    default_schema: &'a str,
    branch_uuid: &'a str,
}

/// Resolved DML target — the seven-step
/// `split_object_name` → catalog/schema defaulting → cross-catalog
/// name short-circuit → `fetch_table` → `parse_table_uuid` → cross-
/// catalog uuid check → `resolve_tx_uuid_for_dml` pipeline materialized
/// as one struct. Fields are owned rather than `&'a str` because
/// lifetimes here pay no perf back — the path is dominated by gRPC
/// round-trips — and `String` keeps the call sites lifetime-noise free.
struct ResolvedDmlTarget {
    catalog: String,
    schema: String,
    table_name: String,
    table_uuid: Uuid,
    table_meta: Table,
    effective_tx_uuid: Option<String>,
}

impl<'a> DmlExecutor<'a> {
    /// Resolve the DML target's `(catalog, schema, table)` triple, fetch
    /// table metadata, and compute the effective tx_uuid in one helper
    /// shared by [`Self::execute_insert`], [`Self::execute_update`], and
    /// [`Self::execute_delete`].
    ///
    /// The step order is load-bearing:
    /// 1. `split_object_name` parses the 1-/2-/3-part `ObjectName`.
    /// 2. Default missing catalog to `snapshot.catalog_name`.
    /// 3. **Name-level cross-catalog short-circuit**:
    ///    `validate_session_catalog_name` rejects before the wire
    ///    `get_table` would target a foreign catalog with the conn's
    ///    `branch_uuid`.
    /// 4. Default missing schema to `self.default_schema`.
    /// 5. `fetch_table` against the QueryService.
    /// 6. `parse_table_uuid` on the returned metadata.
    /// 7. **Uuid-level cross-catalog check**: `validate_session_catalog`
    ///    catches the rare case where a cached catalog name aliases a
    ///    different uuid than the conn's pin.
    /// 8. `resolve_tx_uuid_for_dml` picks the effective tx_uuid (explicit
    ///    `transaction_id` wins; otherwise the snapshot's open tx).
    async fn resolve_target(
        &self,
        transaction_id: Option<&str>,
        name: &ObjectName,
    ) -> Result<ResolvedDmlTarget, Status> {
        let (catalog_name, schema_name, table_name) = split_object_name(name)?;
        let resolved_catalog = catalog_name.unwrap_or_else(|| self.snapshot.catalog_name.clone());
        tx::validate_session_catalog_name(self.snapshot, &resolved_catalog)?;
        let resolved_schema = schema_name.unwrap_or_else(|| self.default_schema.to_string());
        // Resolve the effective tx *before* fetching the target so an in-tx DML
        // against a table created earlier in the same tx resolves it (the
        // get_table below threads open_tx_uuid).
        let effective_tx_uuid = tx::resolve_tx_uuid_for_dml(self.snapshot, transaction_id);
        if let Some(tx_uuid) = effective_tx_uuid.as_deref() {
            tracing::Span::current().record("tx_uuid", tx_uuid);
        }
        let table_meta = fetch_table(
            self.query_channel,
            Some(&resolved_catalog),
            Some(&resolved_schema),
            &table_name,
            self.branch_uuid,
            effective_tx_uuid.as_deref(),
        )
        .await?;
        let table_uuid = parse_table_uuid(&table_meta.table_uuid)?;
        tx::validate_session_catalog(self.snapshot, &table_meta.catalog_uuid)?;
        Ok(ResolvedDmlTarget {
            catalog: resolved_catalog,
            schema: resolved_schema,
            table_name,
            table_uuid,
            table_meta,
            effective_tx_uuid,
        })
    }

    async fn execute_insert(
        &self,
        insert: Insert,
        transaction_id: Option<&str>,
        params: Option<ParamValues>,
    ) -> Result<i64, Status> {
        if insert.returning.is_some() {
            return Err(Status::invalid_argument(
                "INSERT ... RETURNING is not supported",
            ));
        }
        let name = match &insert.table {
            TableObject::TableName(n) => n.clone(),
            TableObject::TableFunction(_) => {
                return Err(Status::invalid_argument(
                    "INSERT INTO TABLE FUNCTION(...) is not supported",
                ));
            }
        };

        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("INSERT without VALUES/SELECT source"))?;
        // The prepare-time path in `gateway::parameter_plan_for_dml` uses this
        // same rewrite, so positional indices line up: `?` at SQL position N
        // becomes `$N` in both plans, and `ParamValues::List` (1-indexed)
        // substitutes correctly via `with_param_values`.
        let source_sql = crate::gateway::rewrite_jdbc_placeholders(&source.to_string());
        let mut df =
            self.ctx.sql(&source_sql).await.map_err(|e| {
                Status::invalid_argument(format!("failed to plan INSERT source: {e}"))
            })?;
        // DataFusion's `with_param_values` resolves `$N` Placeholder
        // expressions through its type system, so a `setLong(1, X)` over JDBC
        // arrives as `ScalarValue::Int64(Some(X))` rather than a textual cast.
        if let Some(values) = params {
            df = df.with_param_values(values).map_err(|e| {
                Status::invalid_argument(format!("failed to bind INSERT parameters: {e}"))
            })?;
        }
        // TODO(CHA-155): stream batches and emit one Change per batch instead
        // of materialising the full source. The penca_write servicer is
        // uncapped (max_(de|en)coding_message_size = usize::MAX), but tonic
        // clients default to a 4MB encoding cap, so the WriteServiceClient we
        // construct from `write_channel` will reject a single oversized
        // WriteData request. Fine for OLTP row-at-a-time, the target today.
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("failed to execute INSERT source: {e}")))?;

        let renamed = combine_batches(&batches, &insert.columns)?;
        if renamed.num_rows() == 0 {
            return Ok(0);
        }
        let is_upsert = is_on_conflict_do_update(insert.on.as_ref())?;

        // TODO(CHA-120): cache table metadata so each DML doesn't make a fresh
        // QueryService::get_table call.
        let target = self.resolve_target(transaction_id, &name).await?;
        let arrow_schema_ref = decode_arrow_schema(&target.table_meta.arrow_schema)?;
        let catalog = Some(target.catalog.as_str());
        let schema = Some(target.schema.as_str());
        let effective_tx_uuid = target.effective_tx_uuid.as_deref();

        // Reject arity mismatches up front so the user gets a SQL-aware error
        // rather than the generic cast_to_schema fallback. Distinguishes the
        // partial-column INSERT case (`INSERT INTO t (a) VALUES (1)` against a
        // wider table) since it's standard SQL we don't yet support and the
        // generic "column count mismatch" wouldn't tell the user why.
        let target_cols = arrow_schema_ref.fields().len();
        let source_cols = renamed.num_columns();
        if source_cols != target_cols {
            let listed = !insert.columns.is_empty();
            let msg = if listed && source_cols < target_cols {
                format!(
                    "partial-column INSERT is not yet supported: column list has {source_cols} entries but table has {target_cols} (specify all columns)"
                )
            } else {
                format!("INSERT source has {source_cols} column(s) but table has {target_cols}")
            };
            return Err(Status::invalid_argument(msg));
        }

        // Cast the source columns to the user-schema types — DataFusion may
        // widen (e.g. Int32 → Int64) during VALUES literal inference, and the
        // upsert log columns are typed strictly.
        let casted = cast_to_schema(&renamed, &arrow_schema_ref)?;

        let upserts_ipc = encode_batch_ipc(&casted)?;
        let change = Change {
            upserts: upserts_ipc,
            deletes: Vec::new(),
        };
        let affected = casted.num_rows() as i64;

        if is_upsert {
            // INSERT ... ON CONFLICT DO UPDATE is intentionally last-writer-wins;
            // no collision check, no lock needed — the upsert_log append already
            // serialises through the per-tx write path.
            send_change(
                self.write_channel,
                catalog,
                schema,
                self.branch_uuid,
                &target.table_uuid.to_string(),
                effective_tx_uuid,
                change,
            )
            .await?;
            return Ok(affected);
        }

        // Strict INSERT: the collision check and the upsert_log append must
        // be one atomic critical section, otherwise two concurrent
        // strict-INSERTs on the same PK can both pass the check (each sees
        // an empty result) and both append, defeating the strict semantics.
        //
        // Per ADR 0006 the check (QueryService) and the write (WriteService)
        // are separate gRPC calls, so the SQL server has to serialise them
        // itself. Uses the same per-(branch, table) Postgres advisory lock
        // pattern as the lifecycle (persist/snapshot) path — see
        // penca-api/src/lifecycle.rs.
        //
        // This adds one Pg roundtrip per strict-INSERT on top of the
        // QueryService + WriteService calls. If profiling shows it dominates
        // small-INSERT latency, the alternatives are (a) push the check
        // back into WriteService with an optimistic-CC precondition on
        // WriteData, or (b) move to a per-row UNIQUE index on
        // `upsert_log.row_uuid` and let Pg enforce uniqueness on commit
        // (ADR 0001 trigger #4).
        let row_uuids =
            derive_row_uuids(&casted, &target.table_uuid, &target.table_meta.primary_keys)?;

        // Strict-INSERT and UPDATE-rewrites-PK must serialize against the SAME
        // per-(branch, table) key. With separate keys, a concurrent
        // strict-INSERT and an UPDATE-PK both targeting the same destination PK
        // can each pass their respective collision probes before the other
        // commits, defeating both checks.
        self.check_collisions_and_send(&target, &row_uuids, change)
            .await?;
        Ok(affected)
    }

    async fn execute_update(
        &self,
        name: ObjectName,
        assignments: Vec<Assignment>,
        selection: Option<Expr>,
        transaction_id: Option<&str>,
    ) -> Result<i64, Status> {
        let mut set: HashMap<String, String> = HashMap::with_capacity(assignments.len());
        for a in assignments {
            let col = assignment_column_name(&a.target)?;
            set.insert(col, a.value.to_string());
        }

        let target = self.resolve_target(transaction_id, &name).await?;
        let arrow_schema_ref = decode_arrow_schema(&target.table_meta.arrow_schema)?;
        let catalog = Some(target.catalog.as_str());
        let schema = Some(target.schema.as_str());
        let effective_tx_uuid = target.effective_tx_uuid.as_deref();

        // Reject any SET column that isn't in the user schema, mirroring the
        // old write-service-side check.
        for col_name in set.keys() {
            if arrow_schema_ref.field_with_name(col_name).is_err() {
                return Err(Status::invalid_argument(format!(
                    "SET references unknown column: {col_name}"
                )));
            }
        }

        // A SET that touches any PK column changes the row's
        // identity (``row_uuid = hash(PK)``), so the old-PK row must be
        // explicitly deleted — a bare upsert under the new PK leaves the
        // old row visible. When no PK is in the SET, ``row_uuid_old ==
        // row_uuid_new`` and the upserts-only fast path stays correct.
        //
        // When the SET moves a PK, mint a per-call alias for each PK column
        // so we can carry both the SET-applied (new) and un-modified (old)
        // PK values in the same SELECT projection without a name collision.
        // Alias = ``__penca_old_pk_<uuid_hex>`` — the UUID guarantees
        // collision-freedom against any user column, the tag prefix keeps
        // the alias recognizable in logs (see ``OLD_PK_ALIAS_TAG``).
        let old_pk_aliases: Option<HashMap<String, String>> = target
            .table_meta
            .primary_keys
            .iter()
            .any(|k| set.contains_key(k))
            .then(|| {
                target
                    .table_meta
                    .primary_keys
                    .iter()
                    .map(|pk| {
                        (
                            pk.clone(),
                            format!("{OLD_PK_ALIAS_TAG}{}", Uuid::new_v4().simple()),
                        )
                    })
                    .collect()
            });

        // Project all user columns, applying SET expressions in-place. We need
        // *all* user columns (not just the SET ones) because the upsert_log
        // append replaces the whole row — unmentioned columns must keep their
        // current values. When the SET moves a PK column, also project the
        // pre-SET PK values under their UUID aliases so we can later derive
        // the old-PK delete batch from the same one SELECT (two SELECTs would
        // observe different RYOW states between the old-PK and new-row reads).
        let mut select_parts: Vec<String> = Vec::with_capacity(
            arrow_schema_ref.fields().len() + target.table_meta.primary_keys.len(),
        );
        for field in arrow_schema_ref.fields() {
            let col = field.name();
            let quoted = quote_ident(col);
            let proj = match set.get(col) {
                Some(expr) => format!("({expr}) AS {quoted}"),
                None => format!("{quoted} AS {quoted}"),
            };
            select_parts.push(proj);
        }
        if let Some(aliases) = &old_pk_aliases {
            for pk in &target.table_meta.primary_keys {
                let alias = aliases
                    .get(pk)
                    .expect("alias map populated for every declared PK");
                select_parts.push(format!("{} AS {}", quote_ident(pk), quote_ident(alias)));
            }
        }
        let where_clause = match selection {
            Some(e) => format!(" WHERE {e}"),
            None => String::new(),
        };
        let sql = format!(
            "SELECT {} FROM {}{}",
            select_parts.join(", "),
            name,
            where_clause
        );
        let df =
            self.ctx.sql(&sql).await.map_err(|e| {
                Status::invalid_argument(format!("failed to plan UPDATE select: {e}"))
            })?;
        // TODO(CHA-155): stream the SELECT and emit per-batch Changes — same
        // 4MB tonic-client cap caveat as INSERT...SELECT. UPDATE...WHERE
        // matching many rows is capped today.
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("failed to execute UPDATE select: {e}")))?;
        let combined = match concat_user_batches(&batches, &arrow_schema_ref)? {
            Some(b) => b,
            None => return Ok(0),
        };
        let casted = cast_to_schema(&combined, &arrow_schema_ref)?;

        let upserts_ipc = encode_batch_ipc(&casted)?;
        // Carry the projected old-PK batch through to the collision-probe
        // step instead of rebuilding it from `batches`.
        let (deletes_ipc, old_pk_batch) = if let Some(aliases) = &old_pk_aliases {
            let pk_batch = build_old_pk_deletes_batch(
                &batches,
                &target.table_meta.primary_keys,
                aliases,
                &arrow_schema_ref,
            )?;
            (encode_batch_ipc(&pk_batch)?, Some(pk_batch))
        } else {
            (Vec::new(), None)
        };
        let change = Change {
            upserts: upserts_ipc,
            deletes: deletes_ipc,
        };
        let affected = casted.num_rows() as i64;

        // When the SET targets a PK column, probe the table for
        // new-PK row_uuids that aren't being vacated by this UPDATE — those
        // are the only ones at risk of clobbering a pre-existing row.
        // Subtracting all-old (not just moved) handles mixed batches where
        // some rows stay put: a value-preserving SET (`alice → alice`)
        // leaves the row in `casted` carrying its existing `row_uuid` —
        // that's the row itself, not a collision against another row, so
        // it must drop out of the probe. Run probe + send under the shared
        // `pk_collision_lock_key` so a concurrent strict-INSERT or another
        // UPDATE-PK targeting the same destination PK can't race past.
        let Some(old_pk_batch) = old_pk_batch else {
            // Non-PK-changing UPDATE: every `row_uuid` is unchanged, so no
            // external collision is possible — take the upserts-only fast path.
            send_change(
                self.write_channel,
                catalog,
                schema,
                self.branch_uuid,
                &target.table_uuid.to_string(),
                effective_tx_uuid,
                change,
            )
            .await?;
            return Ok(affected);
        };

        let new_uuids =
            derive_row_uuids(&casted, &target.table_uuid, &target.table_meta.primary_keys)?;
        let old_uuids = derive_row_uuids(
            &old_pk_batch,
            &target.table_uuid,
            &target.table_meta.primary_keys,
        )?;
        let old_set: HashSet<Uuid> = old_uuids.into_iter().collect();
        let collision_targets: Vec<Uuid> = new_uuids
            .into_iter()
            .filter(|u| !old_set.contains(u))
            .collect();

        self.check_collisions_and_send(&target, &collision_targets, change)
            .await?;
        Ok(affected)
    }

    /// Run the per-(branch, table) PK-collision critical section: take
    /// the advisory lock keyed by [`pk_collision_lock_key`], probe the
    /// table for any `collision_targets` row_uuid that already exist,
    /// then append the `change` via [`send_change`]. Lock + probe +
    /// append in one closure is load-bearing — two concurrent strict-
    /// INSERTs (or a strict-INSERT + UPDATE-PK) on the same destination
    /// PK could otherwise each pass their own probe before the other
    /// appends, defeating both checks.
    ///
    /// Shared by [`Self::execute_insert`]'s strict path and
    /// [`Self::execute_update`]'s PK-changing path.
    async fn check_collisions_and_send(
        &self,
        target: &ResolvedDmlTarget,
        collision_targets: &[Uuid],
        change: Change,
    ) -> Result<(), Status> {
        let catalog = Some(target.catalog.as_str());
        let schema = Some(target.schema.as_str());
        let effective_tx_uuid = target.effective_tx_uuid.as_deref();
        let lock_key = pk_collision_lock_key(self.branch_uuid, &target.table_uuid);
        self.pool
            .advisory_lock(&lock_key, async || -> Result<(), LockErr> {
                check_pk_collisions(
                    self.query_channel,
                    catalog,
                    schema,
                    &target.table_name,
                    self.branch_uuid,
                    collision_targets,
                    &target.table_meta.primary_keys,
                    effective_tx_uuid,
                )
                .await?;
                send_change(
                    self.write_channel,
                    catalog,
                    schema,
                    self.branch_uuid,
                    &target.table_uuid.to_string(),
                    effective_tx_uuid,
                    change,
                )
                .await?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn execute_delete(
        &self,
        delete: Delete,
        transaction_id: Option<&str>,
    ) -> Result<i64, Status> {
        if !delete.tables.is_empty()
            || delete.using.is_some()
            || delete.returning.is_some()
            || !delete.order_by.is_empty()
            || delete.limit.is_some()
        {
            return Err(Status::invalid_argument(
                "DELETE modifiers (multi-table / USING / RETURNING / ORDER BY / LIMIT) are not supported",
            ));
        }
        let tables = match &delete.from {
            FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
        };
        if tables.len() != 1 || !tables[0].joins.is_empty() {
            return Err(Status::invalid_argument(
                "DELETE supports exactly one table with no JOINs",
            ));
        }
        let name = table_factor_name(&tables[0].relation)?;
        let where_clause = match &delete.selection {
            Some(e) => format!(" WHERE {e}"),
            None => String::new(),
        };

        let target = self.resolve_target(transaction_id, &name).await?;
        let effective_tx_uuid = target.effective_tx_uuid.as_deref();

        if target.table_meta.primary_keys.is_empty() {
            return Err(Status::failed_precondition(
                "table has no primary keys; DELETE cannot derive row_uuid",
            ));
        }
        let pk_select = target
            .table_meta
            .primary_keys
            .iter()
            .map(|pk| quote_ident(pk))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {pk_select} FROM {name}{where_clause}");
        let df =
            self.ctx.sql(&sql).await.map_err(|e| {
                Status::invalid_argument(format!("failed to plan DELETE select: {e}"))
            })?;
        // TODO(CHA-155): stream the SELECT and emit per-batch Changes — same
        // 4MB tonic-client cap caveat as INSERT...SELECT.
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("failed to execute DELETE select: {e}")))?;

        // Ship the SELECT result directly as the Change.deletes PK batch: the
        // server pulls `primary_keys` from `__penca_system__.tables` and
        // derives `row_uuid_for_pk` itself, so no client-side hashing.
        // `pk_select` already projects columns in the table's declared PK
        // order, so the resulting batch satisfies the server-side
        // column-order invariant in `insert_delete_pk_batches`.
        let pk_batch = combine_batches(&batches, &[])?;
        let affected = pk_batch.num_rows() as i64;
        if affected == 0 {
            return Ok(0);
        }
        let deletes_ipc = encode_batch_ipc(&pk_batch)?;
        let change = Change {
            upserts: Vec::new(),
            deletes: deletes_ipc,
        };
        send_change(
            self.write_channel,
            Some(target.catalog.as_str()),
            Some(target.schema.as_str()),
            self.branch_uuid,
            &target.table_uuid.to_string(),
            effective_tx_uuid,
            change,
        )
        .await?;
        Ok(affected)
    }
}

/// Per-(branch, table) advisory lock key shared by strict-INSERT and
/// UPDATE-rewrites-PK collision-probe critical sections. The two MUST
/// share one key: otherwise a concurrent INSERT and UPDATE-PK targeting
/// the same destination PK could each pass its own probe before the
/// other commits, and both writes would land at the same `row_uuid`
/// (last-writer-wins data loss).
fn pk_collision_lock_key(branch_uuid: &str, table_uuid: &Uuid) -> String {
    format!("dml:pk-collision:{branch_uuid}:{table_uuid}")
}

/// Run the strict-INSERT collision check by asking the query service for
/// any extant rows matching `row_uuid IN (...)`. Returns
/// `Status::already_exists` listing the colliding PK identities if any
/// row comes back.
///
/// The filter is anchored at `latest l` (the merge-on-read CTE alias),
/// matching the SQL the query microservice builds from `ReadDataRequest.filter`.
#[allow(clippy::too_many_arguments)]
async fn check_pk_collisions(
    query_channel: &Channel,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
    table_name: &str,
    branch_uuid: &str,
    row_uuids: &[Uuid],
    primary_keys: &[String],
    open_tx_uuid: Option<&str>,
) -> Result<(), Status> {
    if row_uuids.is_empty() {
        return Ok(());
    }
    // TODO(CHA-155): chunk the IN list. Row UUIDs come from
    // `naming::row_uuid_for_pk()` so injection is not a concern, but the
    // unbounded list inflates the QueryService request (capped client-side
    // at tonic's 4MB default) and overwhelms Postgres's IN-list planner
    // past a few thousand entries. Fine for OLTP row-at-a-time today.
    let in_list = row_uuids
        .iter()
        .map(|u| format!("'{u}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let filter = format!("l.row_uuid IN ({in_list})");
    let mut client = QueryServiceClient::new(query_channel.clone());
    let mut stream = client
        .read_data(ReadDataRequest {
            catalog_name: catalog_name.map(|s| s.to_string()),
            schema_name: schema_name.map(|s| s.to_string()),
            table_name: Some(table_name.to_string()),
            branch_uuid: Some(branch_uuid.to_string()),
            branch_name: None,
            // Project to PK columns: narrow enough that the success
            // path still skips wide user-col chunks on cold ParquetExec
            // and the hot SQL doesn't materialize unused columns, but
            // wide enough that the error path can name the colliding
            // identities. A bare `num_rows` count would force the
            // operator to reverse `naming::row_uuid_for_pk` (a hash)
            // to figure out which row collided, which they can't.
            projection: Some(Projection {
                columns: primary_keys.to_vec(),
            }),
            catalog_uuid: None,
            schema_uuid: None,
            table_uuid: None,
            as_of: None,
            // RYOW: when the INSERT is part of an open tx, the collision
            // check must see this tx's own prior uncommitted writes too —
            // otherwise `BEGIN; INSERT(1); INSERT(1)` would miss the
            // second collision.
            open_tx_uuid: open_tx_uuid.map(|s| s.to_string()),
            filter: Some(filter),
            // The collision check filters by derived row_uuid, not PK values,
            // so neither the scan-side ids PK-batch pushdown nor a structured
            // secondary-index seek applies to this request.
            ids: Vec::new(),
            indexes: Vec::new(),
        })
        .await?
        .into_inner();

    // Cap on collision identities surfaced in the error. Bulk-INSERT
    // pathologies (thousands of duplicate-PK rows) shouldn't blow up
    // a single error string. First N is enough to debug; the count
    // suffix tells the operator there's more if so.
    const MAX_REPORTED: usize = 10;
    let mut samples: Vec<String> = Vec::with_capacity(MAX_REPORTED);
    let fmt_opts = FormatOptions::default();

    'outer: while let Some(resp) = stream.next().await {
        let resp = resp?;
        if resp.data.is_empty() {
            continue;
        }
        let cursor = std::io::Cursor::new(resp.data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| Status::internal(format!("collision-check IPC decode: {e}")))?;
        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Status::internal(format!("collision-check IPC batch: {e}")))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let formatters: Vec<ArrayFormatter> = batch
                .columns()
                .iter()
                .map(|c| ArrayFormatter::try_new(c.as_ref(), &fmt_opts))
                .collect::<Result<_, _>>()
                .map_err(|e| Status::internal(format!("collision-check value format: {e}")))?;
            // `batch.schema()` returns an owned `SchemaRef`; bind it
            // so the borrowed `&str` field names live through the row
            // loop below.
            let schema = batch.schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            for row in 0..batch.num_rows() {
                if samples.len() == MAX_REPORTED {
                    break 'outer;
                }
                let cells: Vec<String> = formatters
                    .iter()
                    .zip(names.iter())
                    .map(|(f, n)| format!("{n}={}", f.value(row)))
                    .collect();
                samples.push(format!("({})", cells.join(", ")));
            }
        }
    }

    if samples.is_empty() {
        return Ok(());
    }
    let header = if samples.len() == MAX_REPORTED {
        format!("primary key collision (first {MAX_REPORTED} of possibly more)")
    } else {
        format!("primary key collision on {} row(s)", samples.len())
    };
    Err(Status::already_exists(format!(
        "{header}: {}",
        samples.join(", ")
    )))
}

async fn fetch_table(
    query_channel: &Channel,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
    table_name: &str,
    branch_uuid: &str,
    open_tx_uuid: Option<&str>,
) -> Result<Table, Status> {
    let mut client = QueryServiceClient::new(query_channel.clone());
    let resp = client
        .get_table(GetTableRequest {
            catalog_name: catalog_name.map(|s| s.to_string()),
            schema_name: schema_name.map(|s| s.to_string()),
            table_name: Some(table_name.to_string()),
            branch_uuid: Some(branch_uuid.to_string()),
            branch_name: None,
            catalog_uuid: None,
            schema_uuid: None,
            table_uuid: None,
            // Tx-aware target resolution: a table (and its parent schema)
            // created earlier in the same tx must resolve so an in-tx DML
            // against it sees it. The server resolves schema_uuid/table_uuid
            // against the open tx's read snapshot.
            open_tx_uuid: open_tx_uuid.map(|s| s.to_string()),
            as_of_micros: None,
            // In-tx DML resolves via the open tx (RYOW), never the seq pin.
            as_of_seq: None,
        })
        .await?
        .into_inner();
    resp.table
        .ok_or_else(|| Status::not_found(format!("table not found: {table_name}")))
}

async fn send_change(
    write_channel: &Channel,
    catalog_name: Option<&str>,
    schema_name: Option<&str>,
    branch_uuid: &str,
    table_uuid: &str,
    transaction_id: Option<&str>,
    change: Change,
) -> Result<(), Status> {
    let tx_uuid = transaction_id.map(|tx| tx.to_string());
    // Auto-commit (`tx_uuid` unset) requires `author` and `comment` since
    // commit_tx_log.{author,comment} are NOT NULL. The append path must leave
    // them unset.
    let (author, comment) = if tx_uuid.is_some() {
        (None, None)
    } else {
        // Empty strings satisfy the NOT NULL constraint without claiming any
        // meaningful audit info — naming a specific identity (e.g.
        // "penca-sql-server") would just be wrong.
        // TODO(CHA-159): once auth lands as gRPC interceptors, the WriteService
        // derives `Tx.author` from the authenticated principal and this caller
        // stops setting `WriteDataRequest.author` entirely.
        // TODO(CHA-160): source the comment from Flight SQL session properties
        // (e.g. `application_name`).
        (Some(String::new()), Some(String::new()))
    };
    let mut client = WriteServiceClient::new(write_channel.clone());
    client
        .write_data(WriteDataRequest {
            catalog_name: catalog_name.map(|s| s.to_string()),
            schema_name: schema_name.map(|s| s.to_string()),
            branch_uuid: Some(branch_uuid.to_string()),
            branch_name: None,
            catalog_uuid: None,
            schema_uuid: None,
            table_uuid: Some(table_uuid.to_string()),
            table_name: None,
            tx_uuid,
            author,
            comment,
            change: Some(change),
        })
        .await?;
    Ok(())
}

fn decode_arrow_schema(bytes: &[u8]) -> Result<SchemaRef, Status> {
    try_schema_from_ipc_buffer(bytes)
        .map(Arc::new)
        .map_err(|e| Status::internal(format!("failed to decode arrow_schema: {e}")))
}

fn parse_table_uuid(s: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|e| Status::internal(format!("invalid table_uuid '{s}': {e}")))
}

/// Cast every column of `batch` to the corresponding type in `target`.
/// Field names are matched by index; `batch` must already share the same
/// schema layout as `target` (use `combine_batches` or `concat_user_batches`
/// first).
fn cast_to_schema(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, Status> {
    // Defense in depth — INSERT validates arity up front and UPDATE's
    // concat_user_batches projects to user_schema by construction, so
    // reaching this branch implies an upstream miss. Surface it as
    // invalid_argument anyway since a user-facing SQL error is the more
    // likely root cause than a server bug.
    if batch.num_columns() != target.fields().len() {
        return Err(Status::invalid_argument(format!(
            "column count mismatch: source has {}, target has {}",
            batch.num_columns(),
            target.fields().len()
        )));
    }
    let mut casted = Vec::with_capacity(target.fields().len());
    for (i, field) in target.fields().iter().enumerate() {
        let col = batch.column(i);
        let out = if col.data_type() == field.data_type() {
            col.clone()
        } else {
            cast(col, field.data_type()).map_err(|e| {
                Status::internal(format!(
                    "cast {} -> {}: {}",
                    col.data_type(),
                    field.data_type(),
                    e
                ))
            })?
        };
        casted.push(out);
    }
    RecordBatch::try_new(target.clone(), casted)
        .map_err(|e| Status::internal(format!("cast_to_schema rebuild: {e}")))
}

/// Concatenate UPDATE-select output batches into a single batch with
/// schema ordered to match the user_schema. Returns `None` if no rows
/// matched.
fn concat_user_batches(
    batches: &[RecordBatch],
    user_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, Status> {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total == 0 {
        return Ok(None);
    }
    // Project each batch to the user_schema column order — DataFusion is
    // free to return columns in any order that matches the SELECT list.
    let mut projected = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut indices = Vec::with_capacity(user_schema.fields().len());
        for field in user_schema.fields() {
            let idx = batch.schema().index_of(field.name()).map_err(|_| {
                Status::internal(format!(
                    "UPDATE select output missing column '{}'",
                    field.name()
                ))
            })?;
            indices.push(idx);
        }
        projected.push(
            batch
                .project(&indices)
                .map_err(|e| Status::internal(format!("projection rebuild: {e}")))?,
        );
    }
    let projected_schema = projected[0].schema();
    Ok(Some(
        concat_batches(&projected_schema, &projected)
            .map_err(|e| Status::internal(format!("concat UPDATE batches: {e}")))?,
    ))
}

/// Build the PK batch fed to `Change.deletes` for a PK-changing UPDATE.
/// Picks the pre-SET PK columns out of the SELECT output
/// (carried under their per-call UUID aliases), renames them back to
/// plain PK names in declared order, and casts each to its declared
/// user_schema type. The resulting batch matches the column-order +
/// dtype contract `penca_api::write::WriteService::insert_delete_pk_batches`
/// enforces; the server then derives `row_uuid_for_pk` per row.
fn build_old_pk_deletes_batch(
    batches: &[RecordBatch],
    primary_keys: &[String],
    old_pk_aliases: &HashMap<String, String>,
    user_schema: &SchemaRef,
) -> Result<RecordBatch, Status> {
    let pk_fields: Vec<Field> = primary_keys
        .iter()
        .map(|pk| {
            let f = user_schema.field_with_name(pk).map_err(|_| {
                Status::internal(format!(
                    "primary key '{pk}' missing from user_schema while building delete PK batch"
                ))
            })?;
            Ok(Field::new(
                pk.clone(),
                f.data_type().clone(),
                f.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let pk_schema: SchemaRef = Arc::new(Schema::new(pk_fields));

    let mut projected = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(primary_keys.len());
        for (i, pk) in primary_keys.iter().enumerate() {
            let alias = old_pk_aliases
                .get(pk)
                .expect("alias map populated for every declared PK");
            let idx = batch.schema().index_of(alias).map_err(|_| {
                Status::internal(format!(
                    "UPDATE select output missing old-PK alias for '{pk}'"
                ))
            })?;
            let src = batch.column(idx);
            let target_type = pk_schema.field(i).data_type();
            let casted = if src.data_type() == target_type {
                src.clone()
            } else {
                cast(src, target_type).map_err(|e| {
                    Status::internal(format!(
                        "cast old PK '{pk}': {} -> {target_type:?}: {e}",
                        src.data_type()
                    ))
                })?
            };
            cols.push(casted);
        }
        projected.push(
            RecordBatch::try_new(pk_schema.clone(), cols)
                .map_err(|e| Status::internal(format!("PK delete batch rebuild: {e}")))?,
        );
    }
    concat_batches(&pk_schema, &projected)
        .map_err(|e| Status::internal(format!("concat PK delete batches: {e}")))
}

/// Derive `row_uuid` per row from the table's primary-key columns via
/// `naming::row_uuid_for_pk` — used to build the strict-INSERT and
/// UPDATE-rewrites-PK collision-check IN-lists locally. The write
/// service derives row_uuid itself when it appends to `upsert_log`;
/// doing it here just lets us probe before the append without an
/// extra roundtrip.
///
/// Returns `Vec<Uuid>` rather than `Vec<String>` so the
/// `execute_update` set-difference path can use `HashSet<Uuid>`
/// directly (one 16-byte hash per probe vs per-element heap-traversed
/// `HashSet<String>`). Stringification for the IN-list happens once
/// inside `check_pk_collisions`.
fn derive_row_uuids(
    batch: &RecordBatch,
    table_uuid: &Uuid,
    primary_keys: &[String],
) -> Result<Vec<Uuid>, Status> {
    if primary_keys.is_empty() {
        return Err(Status::failed_precondition(
            "table has no primary keys; cannot derive row_uuid",
        ));
    }
    // Resolve PK columns once — column_by_name is a linear scan over the
    // schema fields, so doing it inside the row loop turns this into
    // O(rows × pks × fields).
    let pk_cols: Vec<&dyn Array> = primary_keys
        .iter()
        .map(|pk| {
            batch.column_by_name(pk).map(|c| c.as_ref()).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "primary key column '{pk}' missing from INSERT source"
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let num_rows = batch.num_rows();
    let mut row_uuids: Vec<Uuid> = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let pk_values: Vec<String> = pk_cols
            .iter()
            .map(|col| {
                arrow::util::display::array_value_to_string(*col, row_idx)
                    .map_err(|e| Status::internal(format!("array_value_to_string: {e}")))
            })
            .collect::<Result<_, _>>()?;
        let pk_refs: Vec<&str> = pk_values.iter().map(|s| s.as_str()).collect();
        row_uuids.push(naming::row_uuid_for_pk(table_uuid, &pk_refs));
    }
    Ok(row_uuids)
}

/// Concat executed source batches into one. If `columns` is non-empty,
/// rename the fields on the resulting batch to the user-specified names
/// (needed for `INSERT INTO t (a, b) VALUES (...)` where the VALUES
/// subquery yields `column1, column2`).
fn combine_batches(batches: &[RecordBatch], columns: &[Ident]) -> Result<RecordBatch, Status> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }
    let first_schema = batches[0].schema();
    let combined = concat_batches(&first_schema, batches)
        .map_err(|e| Status::internal(format!("failed to concat INSERT source batches: {e}")))?;
    if columns.is_empty() {
        return Ok(combined);
    }
    if columns.len() != first_schema.fields().len() {
        return Err(Status::invalid_argument(format!(
            "INSERT column list has {} entries but source produced {} columns",
            columns.len(),
            first_schema.fields().len()
        )));
    }
    let renamed_fields: Vec<Field> = first_schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(f, c)| Field::new(c.value.clone(), f.data_type().clone(), f.is_nullable()))
        .collect();
    let renamed_schema = Arc::new(Schema::new(renamed_fields));
    RecordBatch::try_new(renamed_schema, combined.columns().to_vec())
        .map_err(|e| Status::internal(format!("failed to rename INSERT source columns: {e}")))
}

/// Status-mapping adapter over the canonical encoder in `penca-datafusion`;
/// the encoding itself lives in one place for both the write path and the
/// scan-side `ids` batch.
fn encode_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, Status> {
    penca_datafusion::encode_batch_ipc(batch)
        .map_err(|e| Status::internal(format!("failed to encode Arrow IPC batch: {e}")))
}

fn is_on_conflict_do_update(on: Option<&OnInsert>) -> Result<bool, Status> {
    let Some(on) = on else { return Ok(false) };
    match on {
        OnInsert::OnConflict(c) => match &c.action {
            OnConflictAction::DoUpdate(_) => Ok(true),
            OnConflictAction::DoNothing => Err(Status::unimplemented(
                "INSERT ... ON CONFLICT DO NOTHING is not yet supported",
            )),
        },
        OnInsert::DuplicateKeyUpdate(_) => Err(Status::unimplemented(
            "MySQL-style ON DUPLICATE KEY UPDATE is not supported",
        )),
        _ => Err(Status::unimplemented(
            "unsupported ON-conflict clause on INSERT",
        )),
    }
}

/// Split a parsed `ObjectName` into `(catalog, schema, table)`. Supports
/// 1-, 2-, or 3-part names with the convention that shorter names are
/// left-padded with `None` (so a bare `t` resolves to `(None, None, "t")`
/// and the admin/write services apply their own defaulting rules).
type SplitName = (Option<String>, Option<String>, String);

fn split_object_name(name: &ObjectName) -> Result<SplitName, Status> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
            ObjectNamePart::Function(_) => Err(Status::invalid_argument(
                "function-style identifiers are not supported in DML target names",
            )),
        })
        .collect::<Result<_, _>>()?;
    match parts.as_slice() {
        [t] => Ok((None, None, t.clone())),
        [s, t] => Ok((None, Some(s.clone()), t.clone())),
        [c, s, t] => Ok((Some(c.clone()), Some(s.clone()), t.clone())),
        other => Err(Status::invalid_argument(format!(
            "expected table | schema.table | catalog.schema.table; got {}-part name",
            other.len()
        ))),
    }
}

fn table_factor_name(factor: &TableFactor) -> Result<ObjectName, Status> {
    match factor {
        TableFactor::Table { name, .. } => Ok(name.clone()),
        _ => Err(Status::invalid_argument(
            "DML target must be a table reference",
        )),
    }
}

fn assignment_column_name(target: &AssignmentTarget) -> Result<String, Status> {
    match target {
        AssignmentTarget::ColumnName(name) => {
            let parts = &name.0;
            if parts.len() != 1 {
                return Err(Status::invalid_argument(
                    "UPDATE SET target must be a bare column name",
                ));
            }
            match &parts[0] {
                ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
                ObjectNamePart::Function(_) => Err(Status::invalid_argument(
                    "UPDATE SET target must be an identifier",
                )),
            }
        }
        AssignmentTarget::Tuple(_) => Err(Status::invalid_argument(
            "tuple UPDATE SET targets are not supported",
        )),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::StreamReader;
    use datafusion::sql::parser::{DFParser, Statement as DFStatement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse_name(s: &str) -> ObjectName {
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).try_with_sql(s).unwrap();
        parser.parse_object_name(false).unwrap()
    }

    fn parse_stmt(sql: &str) -> SqlStatement {
        let mut stmts = DFParser::parse_sql(sql).unwrap();
        match stmts.pop_front().unwrap() {
            DFStatement::Statement(b) => *b,
            _ => panic!("expected plain SQL statement"),
        }
    }

    #[test]
    fn split_object_name_bare_table() {
        let got = split_object_name(&parse_name("t")).unwrap();
        assert_eq!(got, (None, None, "t".to_string()));
    }

    #[test]
    fn split_object_name_schema_qualified() {
        let got = split_object_name(&parse_name("s.t")).unwrap();
        assert_eq!(got, (None, Some("s".to_string()), "t".to_string()));
    }

    #[test]
    fn split_object_name_fully_qualified() {
        let got = split_object_name(&parse_name("c.s.t")).unwrap();
        assert_eq!(
            got,
            (
                Some("c".to_string()),
                Some("s".to_string()),
                "t".to_string()
            )
        );
    }

    #[test]
    fn split_object_name_rejects_four_parts() {
        let err = split_object_name(&parse_name("a.b.c.d")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn is_on_conflict_do_update_detects_upsert() {
        let SqlStatement::Insert(insert) =
            parse_stmt("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = EXCLUDED.a")
        else {
            panic!("expected INSERT");
        };
        assert!(is_on_conflict_do_update(insert.on.as_ref()).unwrap());
    }

    #[test]
    fn is_on_conflict_do_update_false_for_plain_insert() {
        let SqlStatement::Insert(insert) = parse_stmt("INSERT INTO t (a) VALUES (1)") else {
            panic!("expected INSERT");
        };
        assert!(!is_on_conflict_do_update(insert.on.as_ref()).unwrap());
    }

    #[test]
    fn is_on_conflict_do_nothing_is_unimplemented() {
        let SqlStatement::Insert(insert) =
            parse_stmt("INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING")
        else {
            panic!("expected INSERT");
        };
        let err = is_on_conflict_do_update(insert.on.as_ref()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("column1", DataType::Utf8, false),
            Field::new("column2", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn combine_batches_renames_columns_to_user_list() {
        let batch = sample_batch();
        let cols = vec![Ident::new("name"), Ident::new("value")];
        let out = combine_batches(&[batch], &cols).unwrap();
        let schema = out.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["name", "value"]);
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn combine_batches_keeps_schema_when_no_columns_given() {
        let batch = sample_batch();
        let out = combine_batches(&[batch], &[]).unwrap();
        let schema = out.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["column1", "column2"]);
    }

    #[test]
    fn combine_batches_rejects_column_count_mismatch() {
        let batch = sample_batch();
        let cols = vec![Ident::new("only_one")];
        let err = combine_batches(&[batch], &cols).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn insert_returning_is_surfaced_by_parser() {
        // Pins the contract execute_insert relies on: sqlparser-rs exposes
        // the RETURNING clause via insert.returning. If this ever flips to
        // None, execute_insert would silently drop RETURNING rather than
        // reject it.
        let SqlStatement::Insert(insert) = parse_stmt("INSERT INTO t (a) VALUES (1) RETURNING a")
        else {
            panic!("expected INSERT");
        };
        assert!(insert.returning.is_some());
    }

    #[test]
    fn encode_and_decode_round_trip() {
        let batch = sample_batch();
        let bytes = encode_batch_ipc(&batch).unwrap();
        let reader = StreamReader::try_new(bytes.as_slice(), None).unwrap();
        let decoded: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
    }

    #[test]
    fn derive_row_uuids_matches_naming_helper() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["alice", "bob"]))],
        )
        .unwrap();
        let table_uuid = Uuid::new_v4();
        let pks = vec!["name".to_string()];
        let out = derive_row_uuids(&batch, &table_uuid, &pks).unwrap();
        assert_eq!(
            out,
            vec![
                naming::row_uuid_for_pk(&table_uuid, &["alice"]),
                naming::row_uuid_for_pk(&table_uuid, &["bob"]),
            ]
        );
    }

    #[test]
    fn derive_row_uuids_rejects_missing_pk_column() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let table_uuid = Uuid::new_v4();
        let pks = vec!["name".to_string()];
        let err = derive_row_uuids(&batch, &table_uuid, &pks).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn derive_row_uuids_rejects_no_primary_keys() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let table_uuid = Uuid::new_v4();
        let err = derive_row_uuids(&batch, &table_uuid, &[]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }
}
