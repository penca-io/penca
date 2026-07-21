//! Cross-driver SQL-entry classifier (CHA-259).
//!
//! Every Flight SQL entry-point that takes raw SQL — `do_put_statement_update`,
//! `do_action_create_prepared_statement`, `do_put_prepared_statement_update`,
//! `get_flight_info_statement`, `get_flight_info_prepared_statement` — needs to
//! pattern-match the parsed [`SqlStatement`] against the same dispatch arms
//! (SET, transaction control, DML, SELECT) and reject unsupported variants
//! with the same wording. Each handler implementing that match independently
//! is how the routing drifted enough that JDBC `CREATE TABLE` produced
//! `Execution("schema provider does not support registering tables")` while
//! ADBC `CREATE TABLE` produced the actionable CHA-172 rejection — same SQL,
//! same intent, different per-driver path through the server (see [CHA-257]
//! for the surface). This module centralizes the classification step so the
//! per-handler differences collapse to one match, and a future routing-arm or
//! rejection-wording change touches one site.
//!
//! [CHA-257]: https://linear.app/chapala/issue/CHA-257
//!
//! ## Driver wire-action audit
//!
//! Why a single classifier is load-bearing. Each user-level driver call routes
//! through a different Flight SQL action sequence that lands on a different
//! server entry-point handler; the handler then needs to perform the same
//! classification, or the rejection wording the user sees diverges.
//!
//! | User-level call | ADBC action sequence | JDBC action sequence | Server handler(s) |
//! | --- | --- | --- | --- |
//! | DDL — `cursor.execute("CREATE TABLE …")` / `Statement.execute("CREATE TABLE …")` | `DoPutStatementUpdate` | `ActionCreatePreparedStatement` → `DoPutPreparedStatementUpdate` | `do_put_statement_update` (ADBC); `do_action_create_prepared_statement` + `do_put_prepared_statement_update` (JDBC) |
//! | DML — INSERT / UPDATE / DELETE | `DoPutStatementUpdate` | `ActionCreatePreparedStatement` → `DoPutPreparedStatementUpdate` | same as DDL row |
//! | SELECT | `GetFlightInfo` + `DoGet` | `ActionCreatePreparedStatement` → `GetFlightInfo` + `DoGet` | `get_flight_info_statement` + `do_get_fallback` (ADBC); `do_action_create_prepared_statement` + `get_flight_info_prepared_statement` + `do_get_fallback` (JDBC) |
//! | Tx control — explicit `BEGIN` / `COMMIT` / `ROLLBACK` SQL | `DoPutStatementUpdate` | `ActionCreatePreparedStatement` → `DoPutPreparedStatementUpdate` | same as DDL row |
//! | Structured tx control — `connection.set_autocommit(False)` / `do_action_begin_transaction` | `ActionBeginTransaction` / `ActionEndTransaction` | same | `do_action_begin_transaction` / `do_action_end_transaction` — **does not flow through the gateway** (structured Flight SQL actions, not SQL strings) |
//!
//! Five gateway-bound entry-points; one classifier. The structured tx-control
//! row is the explicit non-goal — those handlers receive typed Flight SQL
//! action payloads, not raw SQL, and route directly into [`crate::tx`].
//!
//! ## Invariant
//!
//! Every Flight SQL SQL-entry handler routes through one of this module's
//! three public helpers — [`execute_update`] (write paths),
//! [`plan_for_get_flight_info`] (read paths), or
//! [`plan_for_create_prepared_statement`] (JDBC prepare path, the
//! load-bearing asymmetric helper that accepts DML / BEGIN / COMMIT /
//! ROLLBACK at prepare time so the driver can route them to
//! `DoPutPreparedStatementUpdate`). All three call [`classify`] before
//! dispatch. `crate::dml::execute` matches only `Insert | Update | Delete`
//! after this commit; reaching its catch-all arm raises `Status::internal`
//! because that means the gateway invariant broke. The canonical place a
//! future "what does Penca do with `CREATE INDEX`?" change lives is here.

use std::sync::Arc;

use datafusion::common::ParamValues;
use datafusion::execution::context::{SQLOptions, SessionContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement as DFStatement;
use datafusion::sql::sqlparser::ast::{Set, Statement as SqlStatement};
use penca_db::driver::pg::PgDriver;
use tonic::Status;
use tonic::transport::Channel;

use crate::ddl::{CreateSchemaArgs, DdlKind};
use crate::session::{ConnSession, SessionSnapshot};

/// Result of [`plan_for_create_prepared_statement`]. Fields are
/// semantically distinct — `dataset_plan` drives the JDBC driver's
/// empty-schema → update-path heuristic, `parameter_plan` populates
/// the prepare-time `parameter_schema` — so naming them in the type
/// system prevents accidental positional swap at the destructure
/// site. The two `LogicalPlan` fields are equal for the SET / SELECT
/// arms and diverge for DML.
#[derive(Debug)]
pub(crate) struct PreparedPlan {
    pub(crate) rewritten_sql: Option<String>,
    pub(crate) dataset_plan: LogicalPlan,
    pub(crate) parameter_plan: LogicalPlan,
    /// CHA-367: `true` when `dataset_plan` is the same logical plan
    /// `get_flight_info_prepared_statement` would rebuild for this statement —
    /// so `do_action_create_prepared_statement` can cache it and stamp its
    /// `statement_uuid` on the handle, letting GetFlightInfo reuse it instead of
    /// re-planning (the cross-pass half of the resolution-dedup work). `true`
    /// for Select / Set, whose `dataset_plan` IS the query / placeholder plan
    /// that reaches GetFlightInfo + DoGet; `false` for Dml / Ddl / tx control,
    /// whose `dataset_plan` is an empty steering relation and which route to
    /// DoPut, never GetFlightInfo.
    pub(crate) dataset_plan_reusable_at_getflightinfo: bool,
}

/// Outcome of [`classify`] for a parsed [`SqlStatement`]. Each variant
/// names the dispatch arm a caller should route to; carrying the
/// original `SqlStatement` (rather than discarding the AST) lets the
/// downstream handler — `tx::validate_start_transaction`,
/// `dml::execute`, DataFusion's planner — keep its existing argument
/// shape without a second parse pass.
#[derive(Debug)]
pub(crate) enum Classified {
    /// `SET <name> = <value>` — route to [`crate::set::handle_set`].
    /// The parsed AST is carried so the caller can hand the same
    /// node to the SET handler without re-parsing.
    Set(Set),
    /// `BEGIN` / `START TRANSACTION` (any variant). The carried
    /// `SqlStatement` is the original `StartTransaction` AST, which
    /// the caller passes to [`crate::tx::validate_start_transaction`]
    /// before [`crate::tx::handle_begin`] — the validator inspects
    /// `modes` / `modifier` / `statements` / etc. on the parsed
    /// node, so dropping the AST and rebuilding it would lose
    /// information.
    StartTx(SqlStatement),
    /// `COMMIT` (any variant). Routes to
    /// [`crate::tx::handle_commit`]. The variant carries no payload
    /// because `handle_commit` reads everything it needs off the
    /// per-conn session.
    Commit,
    /// `ROLLBACK` (any variant). Routes to
    /// [`crate::tx::handle_rollback`]; payload-less for the same
    /// reason as [`Commit`].
    Rollback,
    /// `INSERT` / `UPDATE` / `DELETE`. Routes to
    /// [`crate::dml::execute`], which does its own outer match on
    /// the three DML variants — the carried `SqlStatement` is what
    /// it expects.
    Dml(SqlStatement),
    /// `SELECT` (any [`SqlStatement::Query`] shape). Routes to
    /// DataFusion's `statement_to_plan` via the caller's planning
    /// helper. The AST is carried so callers can plan directly from
    /// the parsed node instead of re-serializing + re-parsing the
    /// original SQL string.
    Select(SqlStatement),
    /// `CREATE SCHEMA` / `CREATE TABLE`, auto-commit (CHA-172) or
    /// transactional (CHA-345). Routes to [`crate::ddl::execute`] via the
    /// dispatcher in [`execute_update`], which threads
    /// `snapshot.open_tx_uuid` into the WriteService request — `None`
    /// auto-commits, `Some` writes under the open tx. The carried
    /// [`DdlKind`] is the typed two-variant subspace built at the
    /// classifier boundary — no `unreachable!()` branch at the
    /// dispatcher.
    Ddl(DdlKind),
}

/// Classify a parsed [`SqlStatement`] into the dispatch arm a Flight
/// SQL SQL-entry handler should route to.
///
/// Returns `Err(Status::failed_precondition(...))` for statements
/// outside the supported set, via [`unsupported_statement`]. The
/// supported set is the same in both auto-commit and transactional
/// context after CHA-345: `INSERT/UPDATE/DELETE` and `CREATE SCHEMA` /
/// `CREATE TABLE`. The unsupported variants (`DROP …`, `ALTER …`,
/// `CREATE INDEX`, `CREATE VIEW`, …) point at the gRPC WriteService;
/// the in-tx wording differs only in noting which variants are in
/// scope, not in any architectural claim (ADR 0010's "permanent" in-tx
/// gating was retired by CHA-345 — the open tx now threads through the
/// `ConnScope` cell to `PencaSchemaProvider::table`).
///
/// `CREATE SCHEMA` / `CREATE TABLE` route to [`Classified::Ddl`]
/// regardless of `snapshot.open_tx_uuid`; the dispatcher threads the
/// open tx into the WriteService request.
pub(crate) fn classify(
    stmt: SqlStatement,
    snapshot: &SessionSnapshot,
) -> Result<Classified, Status> {
    match stmt {
        SqlStatement::Set(set) => Ok(Classified::Set(set)),
        s @ SqlStatement::StartTransaction { .. } => Ok(Classified::StartTx(s)),
        SqlStatement::Commit { .. } => Ok(Classified::Commit),
        SqlStatement::Rollback { .. } => Ok(Classified::Rollback),
        s @ (SqlStatement::Insert(_) | SqlStatement::Update { .. } | SqlStatement::Delete(_)) => {
            Ok(Classified::Dml(s))
        }
        s @ SqlStatement::Query(_) => Ok(Classified::Select(s)),
        // CHA-345: `CREATE TABLE` / `CREATE SCHEMA` route to the DDL
        // translator whether or not a tx is open. In-tx, `ddl::execute`
        // threads `snapshot.open_tx_uuid` into the WriteService request
        // so the metadata row is written under the tx (CHA-164) and
        // resolves on the tx's own subsequent reads (the ConnScope cell,
        // IMPL-1). The former `open_tx_uuid.is_none()` guard — and the
        // ADR-0010 architectural blocker behind it — is gone.
        SqlStatement::CreateTable(ct) => Ok(Classified::Ddl(DdlKind::CreateTable(Box::new(ct)))),
        SqlStatement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => Ok(Classified::Ddl(DdlKind::CreateSchema(Box::new(
            CreateSchemaArgs {
                schema_name,
                if_not_exists,
                with,
                options,
                default_collate_spec,
                clone,
            },
        )))),
        other => Err(unsupported_statement(snapshot, &other)),
    }
}

/// Render the `failed_precondition` Status for an unsupported
/// statement variant. Sole source of the rejection wording for
/// everything `classify` doesn't route to a `Classified` arm: DROP /
/// ALTER / CREATE INDEX / CREATE VIEW / etc. — in both auto-commit and
/// transactional context (the `CREATE SCHEMA` / `CREATE TABLE` pair is
/// intercepted by `classify` before reaching here regardless of tx
/// state, post-CHA-345).
fn unsupported_statement(snapshot: &SessionSnapshot, other: &SqlStatement) -> Status {
    if snapshot.open_tx_uuid.is_some() {
        // CHA-345: `CREATE SCHEMA` / `CREATE TABLE` are now supported
        // in-tx (intercepted by `classify` before reaching here). The
        // variants that land here — DROP / ALTER / CREATE INDEX /
        // CREATE VIEW — are simply not yet implemented on the Flight SQL
        // surface, the same status as their auto-commit forms. No
        // architectural blocker (ADR 0010's "permanent" framing was
        // retired by CHA-345); use the gRPC WriteService meanwhile.
        Status::failed_precondition(format!(
            "this Flight SQL endpoint supports transactional `CREATE SCHEMA` / \
             `CREATE TABLE` (CHA-345) plus INSERT / UPDATE / DELETE inside a \
             BEGIN/COMMIT block; got `{other}`. Other DDL (`DROP …`, `ALTER …`, \
             `CREATE INDEX`, `CREATE VIEW`, …) is not yet supported on the Flight \
             SQL surface — use the gRPC WriteService API."
        ))
    } else {
        Status::failed_precondition(format!(
            "this Flight SQL endpoint supports INSERT / UPDATE / DELETE and \
             `CREATE SCHEMA` / `CREATE TABLE` (auto-commit or transactional); got \
             `{other}`. Other DDL (`DROP …`, `ALTER …`, `CREATE INDEX`, \
             `CREATE VIEW`, …) requires the gRPC WriteService API."
        ))
    }
}

/// Borrow-only context bundle for [`execute_update`]. Each Flight SQL
/// update handler constructs one on entry from its borrows of
/// `FlightSqlService` (`write_channel` / `query_channel` / `pool`) and
/// `FlightSqlSessionContext` (`session_ctx` / `snapshot` / `conn`),
/// then hands the bundle into the gateway. Lets [`execute_update`]
/// drop its 8-arg signature (`#[allow(clippy::too_many_arguments)]`)
/// to three meaningful args — the bundle plus what's unique per
/// call: `sql` and `transaction_id`. Mirrors the
/// [`crate::dml::DmlExecutor`] shape one layer up.
pub(crate) struct UpdateCtx<'a> {
    pub(crate) session_ctx: &'a SessionContext,
    pub(crate) write_channel: &'a Channel,
    pub(crate) query_channel: &'a Channel,
    pub(crate) pool: &'a PgDriver,
    pub(crate) snapshot: &'a SessionSnapshot,
    pub(crate) conn: &'a ConnSession,
}

/// Unified update entry-point for the three Flight SQL handlers that
/// receive raw SQL on a write path:
/// `do_put_statement_update`, `do_put_prepared_statement_update`, and
/// (transitively, via prepared-statement plumbing) the JDBC route
/// through `do_action_create_prepared_statement`.
///
/// Parses once, classifies via [`classify`], and dispatches:
/// * `Set` → [`crate::set::handle_set`], returning `0` rows affected.
/// * `StartTx` → [`crate::tx::validate_start_transaction`] then
///   [`crate::tx::handle_begin`], returning `0`.
/// * `Commit` → [`crate::tx::handle_commit`], returning `0`.
/// * `Rollback` → [`crate::tx::handle_rollback`], returning `0`.
/// * `Dml` → [`crate::dml::execute`] with the parsed AST.
/// * `Select` → `Err(invalid_argument)` — SELECT belongs on
///   `GetFlightInfo` + `DoGet`, not the update entry-points.
/// * DDL / other → propagates the `failed_precondition` Status
///   [`classify`] minted (CHA-172 / ADR 0010 wording).
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        sql_len = sql.len(),
        tx = transaction_id.unwrap_or("<none>"),
        has_params = params.is_some(),
        arm = tracing::field::Empty,
    ),
    err,
)]
pub(crate) async fn execute_update(
    gctx: &UpdateCtx<'_>,
    sql: &str,
    transaction_id: Option<&str>,
    params: Option<ParamValues>,
) -> Result<i64, Status> {
    let stmt = crate::parse::parse_one_statement(sql)?;
    let classified = classify(stmt, gctx.snapshot)?;
    // Parameter binding (CHA-333) is supported only by the DML arm
    // today — specifically `execute_insert`, which threads the values
    // through DataFusion's `with_param_values` on the planned VALUES /
    // SELECT source. SET / BEGIN / COMMIT / ROLLBACK / DDL have no
    // placeholder surface; UPDATE / DELETE could grow it in a
    // follow-up but the wire path doesn't yet. Reject non-DML calls
    // with parameters rather than silently dropping them.
    if params.is_some() && !matches!(classified, Classified::Dml(_)) {
        return Err(Status::unimplemented(
            "parameter binding is currently supported only for INSERT; \
             SET / BEGIN / COMMIT / ROLLBACK / UPDATE / DELETE / \
             CREATE prepared statements must not bind parameters",
        ));
    }
    match classified {
        Classified::Set(set) => {
            tracing::Span::current().record("arm", "set");
            crate::set::handle_set(gctx.snapshot, gctx.session_ctx, sql, set).await?;
            Ok(0)
        }
        Classified::StartTx(stmt) => {
            tracing::Span::current().record("arm", "start_tx");
            crate::tx::validate_start_transaction(&stmt)?;
            crate::tx::handle_begin(gctx.conn, gctx.snapshot, gctx.write_channel).await?;
            Ok(0)
        }
        Classified::Commit => {
            tracing::Span::current().record("arm", "commit");
            crate::tx::handle_commit(gctx.conn, gctx.snapshot, gctx.write_channel).await?;
            Ok(0)
        }
        Classified::Rollback => {
            tracing::Span::current().record("arm", "rollback");
            crate::tx::handle_rollback(gctx.conn, gctx.snapshot, gctx.write_channel).await?;
            Ok(0)
        }
        Classified::Dml(stmt) => {
            tracing::Span::current().record("arm", "dml");
            crate::dml::execute(
                gctx.session_ctx,
                gctx.write_channel,
                gctx.query_channel,
                gctx.pool,
                gctx.snapshot,
                stmt,
                transaction_id,
                params,
            )
            .await
        }
        Classified::Ddl(kind) => {
            tracing::Span::current().record("arm", "ddl");
            let default_schema = gctx
                .session_ctx
                .state()
                .config_options()
                .catalog
                .default_schema
                .clone();
            crate::ddl::execute(gctx.write_channel, gctx.snapshot, &default_schema, kind).await
        }
        Classified::Select(_) => {
            tracing::Span::current().record("arm", "select_rejected");
            Err(Status::invalid_argument(
                "SELECT routed to update entry-point — use GetFlightInfo + DoGet for queries",
            ))
        }
    }
}

/// Read-path planning entry-point for `get_flight_info_statement`
/// and `get_flight_info_prepared_statement`. These handlers serve
/// queries — by Flight SQL convention, only SELECT / SET land here.
///
/// Returns `(rewritten_sql, plan)`:
/// * `Set` → applies the SET eagerly via [`crate::set::handle_set`],
///   plans [`crate::set::SET_PLACEHOLDER_SQL`], and returns
///   `(Some(SET_PLACEHOLDER_SQL.to_string()), placeholder_plan)`.
///   The caller swaps the rewritten SQL into the response ticket so
///   the subsequent DoGet leg returns an empty result instead of
///   re-applying.
/// * `Select` → plans the parsed AST via DataFusion's
///   `statement_to_plan` (skipping a re-parse) and returns
///   `(None, plan)`.
/// * `Dml` / `Ddl` / `StartTx` / `Commit` / `Rollback` → `Err(invalid_argument)`.
///   These belong on the update entry-points; surfacing them here
///   means a misbehaving client.
/// * DDL / other unsupported → propagates the `failed_precondition`
///   Status [`classify`] minted (CHA-172 / ADR 0010 wording).
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(sql_len = sql.len(), arm = tracing::field::Empty),
    err,
)]
pub(crate) async fn plan_for_get_flight_info(
    ctx: &SessionContext,
    snapshot: &SessionSnapshot,
    sql_options: Option<SQLOptions>,
    sql: &str,
) -> Result<(Option<String>, LogicalPlan), Status> {
    let stmt = crate::parse::parse_one_statement(sql)?;
    let classified = classify(stmt, snapshot)?;
    match classified {
        Classified::Set(set) => {
            tracing::Span::current().record("arm", "set");
            crate::set::handle_set(snapshot, ctx, sql, set).await?;
            let plan = plan_sql(ctx, sql_options, crate::set::SET_PLACEHOLDER_SQL).await?;
            Ok((Some(crate::set::SET_PLACEHOLDER_SQL.to_string()), plan))
        }
        Classified::Select(stmt) => {
            tracing::Span::current().record("arm", "select");
            let plan = plan_statement(ctx, sql_options, stmt).await?;
            Ok((None, plan))
        }
        Classified::Dml(_)
        | Classified::Ddl(_)
        | Classified::StartTx(_)
        | Classified::Commit
        | Classified::Rollback => {
            tracing::Span::current().record("arm", "update_misroute");
            Err(Status::invalid_argument(
                "update statement routed to GetFlightInfo — use DoPutStatementUpdate",
            ))
        }
    }
}

/// Prepare-time planning entry-point for `do_action_create_prepared_statement`.
/// JDBC's `Statement.execute` walks `ActionCreatePreparedStatement`
/// for **every** SQL (DDL, DML, SELECT, SET, BEGIN/COMMIT/ROLLBACK)
/// — the driver decides later between `GetFlightInfo` + `DoGet` (for
/// queries) and `DoPutPreparedStatementUpdate` (for everything else)
/// based on the SQL kind. This helper has to accept that whole mix.
///
/// Returns a [`PreparedPlan`] — see the type for per-field semantics
/// (`rewritten_sql` for the SET-eager-applied placeholder swap;
/// `dataset_plan` for the JDBC empty-schema → update-path heuristic;
/// `parameter_plan` for the prepare-time `parameter_schema`). The
/// classification arms differ in what they put into each field:
///
/// * `Set` → `rewritten_sql = Some(SET_PLACEHOLDER_SQL)`;
///   `dataset_plan = parameter_plan` = non-empty placeholder plan
///   (`SELECT 1 WHERE FALSE`, schema `Int32(1)`). Driver routes to
///   `GetFlightInfo` + `DoGet`; the DoGet leg returns empty rows
///   instead of re-applying the SET.
/// * `Select` → `rewritten_sql = None`;
///   `dataset_plan = parameter_plan` = the parsed-AST plan.
/// * `Dml` → `rewritten_sql = None`; `dataset_plan` is an
///   **empty-schema** `EmptyRelation` (the flight-sql-jdbc-driver
///   inspects the prepare-time `dataset_schema` to decide between
///   `GetFlightInfo` + `DoGet` for queries and
///   `DoPutPreparedStatementUpdate` for updates — empty schema
///   steers the driver to the update path); `parameter_plan` is the
///   planned VALUES / SELECT source so the driver's prepare-time
///   `parameter_schema` reflects the actual `?` placeholders.
///   `gateway::execute_update` then decodes the handle's SQL and
///   runs the real dispatch; the caller stashes the original SQL
///   on the handle — no rewrite — so the execute leg sees the
///   user's actual statement.
/// * `StartTx` / `Commit` / `Rollback` → `rewritten_sql = None`;
///   both `dataset_plan` and `parameter_plan` are the empty
///   `EmptyRelation`. Tx control never carries parameters.
/// * DDL / other → propagates the `failed_precondition` Status
///   [`classify`] minted (CHA-172 / ADR 0010 wording). This is the
///   load-bearing fix for the JDBC residual gap from CHA-257 —
///   `CREATE TABLE` now rejects with the actionable wording *before*
///   DataFusion's planner sees the statement and bails with
///   `register_table`.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(sql_len = sql.len(), arm = tracing::field::Empty),
    err,
)]
pub(crate) async fn plan_for_create_prepared_statement(
    ctx: &SessionContext,
    snapshot: &SessionSnapshot,
    sql_options: Option<SQLOptions>,
    sql: &str,
) -> Result<PreparedPlan, Status> {
    let stmt = crate::parse::parse_one_statement(sql)?;
    let classified = classify(stmt, snapshot)?;
    match classified {
        Classified::Set(set) => {
            tracing::Span::current().record("arm", "set");
            crate::set::handle_set(snapshot, ctx, sql, set).await?;
            let plan = plan_sql(ctx, sql_options, crate::set::SET_PLACEHOLDER_SQL).await?;
            Ok(PreparedPlan {
                rewritten_sql: Some(crate::set::SET_PLACEHOLDER_SQL.to_string()),
                dataset_plan: plan.clone(),
                parameter_plan: plan,
                dataset_plan_reusable_at_getflightinfo: true,
            })
        }
        Classified::Select(stmt) => {
            tracing::Span::current().record("arm", "select");
            let plan = plan_statement(ctx, sql_options, stmt).await?;
            Ok(PreparedPlan {
                rewritten_sql: None,
                dataset_plan: plan.clone(),
                parameter_plan: plan,
                dataset_plan_reusable_at_getflightinfo: true,
            })
        }
        Classified::Dml(stmt) => {
            tracing::Span::current().record("arm", "dml");
            // CHA-333: dataset plan stays empty so the JDBC driver
            // picks the update path (empty dataset_schema heuristic),
            // but the parameter plan must reflect the actual `?`
            // placeholders in the SQL so the driver's prepare-time
            // parameter_schema is populated and `setXxx(N, value)`
            // doesn't fail with "ordinal N out of range". For INSERT
            // we plan the FULL statement (not just the VALUES source)
            // so DataFusion infers placeholder types from the target
            // table's column schema — `VALUES (?, ?)` standalone has
            // no typing context and would error with "unable to
            // determine type of query parameter ?". For UPDATE /
            // DELETE the binding path isn't wired through
            // `execute_update` yet (`dml::execute` rejects params for
            // non-INSERT), so we keep an empty parameter plan and the
            // driver will surface "ordinal out of range" if a user
            // tries to bind — which is the right failure mode until
            // UPDATE / DELETE binding lands.
            let parameter_plan = parameter_plan_for_dml(ctx, sql, &stmt).await?;
            Ok(PreparedPlan {
                rewritten_sql: None,
                dataset_plan: empty_update_plan(),
                parameter_plan,
                dataset_plan_reusable_at_getflightinfo: false,
            })
        }
        Classified::StartTx(_) | Classified::Commit | Classified::Rollback => {
            tracing::Span::current().record("arm", "tx_control");
            // Transaction control never carries parameters.
            Ok(PreparedPlan {
                rewritten_sql: None,
                dataset_plan: empty_update_plan(),
                parameter_plan: empty_update_plan(),
                dataset_plan_reusable_at_getflightinfo: false,
            })
        }
        Classified::Ddl(_) => {
            tracing::Span::current().record("arm", "ddl");
            // Auto-commit DDL (CHA-172). Empty dataset_schema steers
            // the JDBC driver to DoPutPreparedStatementUpdate via the
            // "empty schema = update path" heuristic (same shape as
            // Dml / StartTx / Commit / Rollback). DDL carries no `?`
            // placeholders so parameter_plan is empty too. The
            // execute leg re-classifies via `execute_update` and
            // routes the original SQL into `crate::ddl::execute_*`.
            Ok(PreparedPlan {
                rewritten_sql: None,
                dataset_plan: empty_update_plan(),
                parameter_plan: empty_update_plan(),
                dataset_plan_reusable_at_getflightinfo: false,
            })
        }
    }
}

/// Plan a DML statement so `parameter_schema_for_plan` can extract
/// the `?` types at prepare time. INSERT must be planned in full so
/// DataFusion infers placeholder types from the target table's
/// column schema; `VALUES (?, ?)` alone has no typing context and
/// would error with "unable to determine type of query parameter ?".
/// UPDATE and DELETE binding isn't wired through `execute_update`
/// yet — fall back to an empty plan so the driver reports "no
/// parameters" instead of attempting a bind we cannot honor.
///
/// If the INSERT carries syntax DataFusion's planner can't handle
/// (notably `ON CONFLICT DO UPDATE` — Penca's `execute_insert`
/// handles this directly at execute time but the planner errors with
/// "Insert-on clause not supported"), we fall back to the empty
/// parameter plan rather than rejecting the whole prepare. The driver
/// then surfaces "ordinal N out of range" if the caller tries to
/// `setXxx(N, ...)` on such a statement. That is correct because ON
/// CONFLICT plus bound parameters isn't supported today, and reaching
/// this fallback without parameter binding is the common case where
/// JDBC's `Statement.execute(sql)` walks the prepared-statement wire
/// even when the user never calls `setXxx`.
async fn parameter_plan_for_dml(
    ctx: &SessionContext,
    full_sql: &str,
    stmt: &SqlStatement,
) -> Result<LogicalPlan, Status> {
    if matches!(stmt, SqlStatement::Insert(_)) {
        let rewritten = rewrite_jdbc_placeholders(full_sql);
        match ctx.sql(&rewritten).await {
            Ok(df) => return Ok(df.into_unoptimized_plan()),
            Err(e) => {
                // Soft-fall-back. ON CONFLICT, RETURNING, and other
                // surfaces DataFusion's planner rejects still need
                // to walk DoPutPreparedStatementUpdate successfully
                // for the non-parameterized case; the actual SQL
                // executes via Penca's own dispatch in
                // `dml::execute`, not via DataFusion.
                //
                // Log the swallowed error at debug so a future
                // surface (typo'd table, syntax error, new
                // DataFusion rejection class) is observable without
                // changing the wire-level fall-back semantics.
                tracing::debug!(
                    error = %e,
                    "parameter_plan_for_dml: ctx.sql failed; falling back to empty parameter plan",
                );
                return Ok(empty_update_plan());
            }
        }
    }
    Ok(empty_update_plan())
}

/// Rewrite JDBC-style `?` placeholders to DataFusion-style `$N`
/// (1-indexed, in left-to-right order). DataFusion's SQL planner
/// recognizes only the `$N` form (`Can't parse placeholder: ?`);
/// JDBC drivers always send `?`. Single- and double-quoted strings
/// are skipped so `WHERE name = '?'` (literal question mark inside a
/// string) survives untouched. Doubled quote escapes inside strings
/// (`'it''s'`) are honored.
///
/// **Not handled**: SQL line comments (`-- …`) or block comments
/// (`/* … */`) — a `?` inside a comment will still be rewritten to
/// `$N`. Benign for the current callers (everything we pass to
/// DataFusion's planner strips comments before placeholder
/// resolution), but a future caller that uses the rewritten string
/// for anything else needs to add comment-skip handling.
pub(crate) fn rewrite_jdbc_placeholders(sql: &str) -> String {
    // Fast path: non-prepared ADBC writes never carry `?`. Skip the
    // full string scan + per-char branching for the common case.
    // (`memchr` under the hood; cheap.)
    if !sql.contains('?') {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len() + 16);
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut counter = 0_usize;
    while let Some(c) = chars.next() {
        match c {
            '\'' if !in_double => {
                out.push(c);
                if in_single {
                    // Doubled `''` is an escape inside a string — stay
                    // inside the string and emit the second quote.
                    if let Some(&'\'') = chars.peek() {
                        out.push(chars.next().unwrap());
                        continue;
                    }
                }
                in_single = !in_single;
            }
            '"' if !in_single => {
                out.push(c);
                in_double = !in_double;
            }
            '?' if !in_single && !in_double => {
                counter += 1;
                out.push('$');
                out.push_str(&counter.to_string());
            }
            _ => out.push(c),
        }
    }
    out
}

/// `LogicalPlan::EmptyRelation` with no fields. Produced for
/// non-query prepared statements (DML, BEGIN, COMMIT, ROLLBACK) so
/// the flight-sql-jdbc-driver's "empty dataset_schema = update path"
/// heuristic fires and routes execution to
/// `DoPutPreparedStatementUpdate`.
fn empty_update_plan() -> LogicalPlan {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::EmptyRelation;
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

/// Plan a SQL string via DataFusion's full parser+planner. Used for
/// the `SET` arm's placeholder. Mirrors the body of the
/// `FlightSqlSessionContext::sql_to_logical_plan` helper deleted in
/// this commit; lifted here so the gateway is the single source of
/// the planning shape Flight SQL uses.
async fn plan_sql(
    ctx: &SessionContext,
    sql_options: Option<SQLOptions>,
    sql: &str,
) -> Result<LogicalPlan, Status> {
    let plan = ctx
        .state()
        .create_logical_plan(sql)
        .await
        .map_err(crate::flight_sql::error::df_error_to_status)?;
    let verifier = sql_options.unwrap_or_default();
    verifier
        .verify_plan(&plan)
        .map_err(crate::flight_sql::error::df_error_to_status)?;
    Ok(plan)
}

/// Plan an already-parsed [`SqlStatement`] via DataFusion's
/// `statement_to_plan`, skipping the re-parse pass [`plan_sql`] would
/// otherwise do. Used for the `Select` arm — the AST already came
/// from `parse_one_statement`, so handing it to DataFusion directly
/// avoids the second parse over the same string.
async fn plan_statement(
    ctx: &SessionContext,
    sql_options: Option<SQLOptions>,
    stmt: SqlStatement,
) -> Result<LogicalPlan, Status> {
    let plan = ctx
        .state()
        .statement_to_plan(DFStatement::Statement(Box::new(stmt)))
        .await
        .map_err(crate::flight_sql::error::df_error_to_status)?;
    let verifier = sql_options.unwrap_or_default();
    verifier
        .verify_plan(&plan)
        .map_err(crate::flight_sql::error::df_error_to_status)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_one_statement;
    use datafusion::execution::session_state::SessionStateBuilder;

    fn snapshot(open_tx_uuid: Option<&str>) -> SessionSnapshot {
        SessionSnapshot::for_test(
            "public",
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-00000000beef",
            "main",
            open_tx_uuid.map(String::from),
        )
    }

    fn classify_sql(sql: &str, open_tx_uuid: Option<&str>) -> Result<Classified, Status> {
        let stmt = parse_one_statement(sql).expect("parse");
        classify(stmt, &snapshot(open_tx_uuid))
    }

    #[test]
    fn set_routes_to_set_arm() {
        let out = classify_sql("SET search_path = 'sales'", None).unwrap();
        assert!(matches!(out, Classified::Set(_)));
    }

    #[test]
    fn begin_routes_to_start_tx_arm() {
        let out = classify_sql("BEGIN", None).unwrap();
        match out {
            Classified::StartTx(SqlStatement::StartTransaction { .. }) => {}
            other => panic!("expected StartTx(StartTransaction), got {other:?}"),
        }
    }

    #[test]
    fn start_transaction_routes_to_start_tx_arm() {
        // `START TRANSACTION` is the SQL-standard synonym for `BEGIN`;
        // both must classify the same way — the validator that fires
        // downstream (`tx::validate_start_transaction`) rejects modes
        // and modifiers but accepts both bare forms.
        let out = classify_sql("START TRANSACTION", None).unwrap();
        assert!(matches!(out, Classified::StartTx(_)));
    }

    #[test]
    fn commit_routes_to_commit_arm() {
        let out = classify_sql("COMMIT", Some("tx-123")).unwrap();
        assert!(matches!(out, Classified::Commit));
    }

    #[test]
    fn rollback_routes_to_rollback_arm() {
        let out = classify_sql("ROLLBACK", Some("tx-123")).unwrap();
        assert!(matches!(out, Classified::Rollback));
    }

    #[test]
    fn insert_routes_to_dml_arm() {
        let out = classify_sql("INSERT INTO t VALUES (1)", None).unwrap();
        assert!(matches!(out, Classified::Dml(SqlStatement::Insert(_))));
    }

    #[test]
    fn update_routes_to_dml_arm() {
        let out = classify_sql("UPDATE t SET a = 1 WHERE id = 2", None).unwrap();
        assert!(matches!(out, Classified::Dml(SqlStatement::Update { .. })));
    }

    #[test]
    fn delete_routes_to_dml_arm() {
        let out = classify_sql("DELETE FROM t WHERE id = 1", None).unwrap();
        assert!(matches!(out, Classified::Dml(SqlStatement::Delete(_))));
    }

    #[test]
    fn select_routes_to_select_arm() {
        let out = classify_sql("SELECT 1", None).unwrap();
        assert!(matches!(out, Classified::Select(SqlStatement::Query(_))));
    }

    /// CHA-172 — auto-commit `CREATE TABLE` now routes to the
    /// `Classified::Ddl` arm; the dispatcher in `execute_update`
    /// destructures + calls `crate::ddl::execute_create_table`.
    #[test]
    fn create_table_auto_commit_routes_to_ddl_arm() {
        let out = classify_sql("CREATE TABLE t (id BIGINT, PRIMARY KEY(id))", None).unwrap();
        assert!(
            matches!(out, Classified::Ddl(DdlKind::CreateTable(_))),
            "expected Classified::Ddl(CreateTable), got {out:?}",
        );
    }

    /// CHA-172 — auto-commit `CREATE SCHEMA` mirrors the table side
    /// and routes to the same `Classified::Ddl` arm.
    #[test]
    fn create_schema_auto_commit_routes_to_ddl_arm() {
        let out = classify_sql("CREATE SCHEMA s", None).unwrap();
        assert!(
            matches!(out, Classified::Ddl(DdlKind::CreateSchema(_))),
            "expected Classified::Ddl(CreateSchema), got {out:?}",
        );
    }

    /// CHA-345 — transactional `CREATE TABLE` now routes to the DDL
    /// translator, same as the auto-commit case. CHA-255 paid Option
    /// A's per-conn catalog-tree cost, so the `open_tx_uuid` guard the
    /// classifier used to apply is gone: in-tx CREATE threads the open
    /// tx_uuid into `WriteService::CreateTable` (see IMPL-2 / ddl.rs).
    #[test]
    fn create_table_in_tx_routes_to_ddl_arm() {
        let out = classify_sql(
            "CREATE TABLE t (id BIGINT, PRIMARY KEY(id))",
            Some("tx-abc"),
        )
        .unwrap();
        assert!(
            matches!(out, Classified::Ddl(DdlKind::CreateTable(_))),
            "in-tx CREATE TABLE must route to Classified::Ddl, got {out:?}",
        );
    }

    /// CHA-345 — transactional `CREATE SCHEMA` mirrors the table side:
    /// routes to the DDL translator inside an open tx.
    #[test]
    fn create_schema_in_tx_routes_to_ddl_arm() {
        let out = classify_sql("CREATE SCHEMA s", Some("tx-abc")).unwrap();
        assert!(
            matches!(out, Classified::Ddl(DdlKind::CreateSchema(_))),
            "in-tx CREATE SCHEMA must route to Classified::Ddl, got {out:?}",
        );
    }

    /// CHA-345 — in-tx DDL *outside* the supported CREATE pair (`DROP`,
    /// `ALTER`, `CREATE INDEX`, `CREATE VIEW`) still rejects, but the
    /// wording no longer claims transactional DDL is architecturally
    /// unsupported per ADR 0010 (that premise is false post-CHA-345).
    /// It points at the gRPC WriteService, parallel to the auto-commit
    /// framing for the same variants.
    #[test]
    fn drop_table_in_tx_rejects_without_architectural_wording() {
        let err = classify_sql("DROP TABLE t", Some("tx-abc")).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let msg = err.message();
        // Post-CHA-345 the in-tx rejection for the still-unsupported
        // variants no longer frames transactional DDL as an architectural
        // blocker — it's just not-yet-implemented, like its auto-commit
        // form. So no "ADR 0010", no "architecturally", no "intentionally
        // unsupported", no "permanently".
        assert!(
            !msg.contains("ADR 0010")
                && !msg.contains("architecturally")
                && !msg.contains("intentionally unsupported")
                && !msg.to_lowercase().contains("permanently"),
            "in-tx DROP rejection must not frame transactional DDL as architecturally/\
             permanently gated; got: {msg}"
        );
        assert!(
            msg.contains("WriteService"),
            "in-tx DROP rejection must point at the gRPC WriteService; got: {msg}"
        );
    }

    /// `DROP TABLE` and the other auto-commit DDL variants outside the
    /// supported `CREATE SCHEMA` / `CREATE TABLE` pair still reject, with
    /// wording that names the supported set and points users at the gRPC
    /// WriteService for the unsupported half. CHA-345 dropped the
    /// ticket-number citation from the supported-set phrasing (it now
    /// reads "auto-commit or transactional"), so the assertion pins the
    /// supported variants + WriteService + the offending statement.
    #[test]
    fn drop_table_auto_commit_uses_the_generic_wording() {
        let err = classify_sql("DROP TABLE t", None).unwrap_err();
        let msg = err.message();
        assert!(
            msg.contains("CREATE SCHEMA") && msg.contains("CREATE TABLE"),
            "{msg}"
        );
        assert!(msg.contains("gRPC WriteService"), "{msg}");
        assert!(msg.contains("DROP TABLE"), "{msg}");
    }

    /// `BEGIN ISOLATION LEVEL …` parses as `StartTransaction` with a
    /// non-empty `modes` list; the classifier still routes to
    /// `StartTx` (rejection of the unsupported mode is
    /// `tx::validate_start_transaction`'s job downstream, not the
    /// classifier's). Pins the contract that the classifier doesn't
    /// double-validate.
    #[test]
    fn begin_with_modes_routes_to_start_tx_not_unsupported() {
        let out = classify_sql("BEGIN ISOLATION LEVEL SERIALIZABLE", None).unwrap();
        assert!(matches!(out, Classified::StartTx(_)));
    }

    /// `plan_for_create_prepared_statement` must accept BEGIN /
    /// COMMIT / ROLLBACK / DML at prepare time. JDBC's
    /// `Statement.execute("BEGIN")` walks
    /// `ActionCreatePreparedStatement` then
    /// `DoPutPreparedStatementUpdate`; if the prepare step errors,
    /// the JDBC client never gets to run the BEGIN. The execute leg
    /// re-routes the SQL through [`execute_update`] which actually
    /// dispatches to `tx::handle_begin`. Pins the regression that
    /// surfaced this — pre-fix, `[jdbc] test_create_table_inside_begin_rejects_with_adr_0010`
    /// failed at step 0 (BEGIN) because the prepare path rejected
    /// it as "update routed to GetFlightInfo".
    #[tokio::test]
    async fn plan_for_create_prepared_statement_accepts_tx_control() {
        let ctx = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let snap = snapshot(None);
        for sql in ["BEGIN", "COMMIT", "ROLLBACK", "INSERT INTO t VALUES (1)"] {
            let prepared = plan_for_create_prepared_statement(&ctx, &snap, None, sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` rejected at prepare time: {e}"));
            // Prep helper does NOT rewrite — the caller stashes the
            // original SQL on the handle so the execute leg sees
            // the user's actual statement.
            assert_eq!(
                prepared.rewritten_sql, None,
                "`{sql}` should not be rewritten at prep"
            );
            // **Load-bearing property** — the flight-sql-jdbc-driver
            // inspects the prepare-time `dataset_schema` and routes
            // empty-schema prepared statements to
            // `DoPutPreparedStatementUpdate` (vs `GetFlightInfo` +
            // `DoGet` for non-empty). A future refactor that swapped
            // `EmptyRelation { schema: empty }` for a non-empty
            // placeholder would silently break the JDBC routing; this
            // assertion catches that regression.
            assert!(
                prepared.dataset_plan.schema().fields().is_empty(),
                "`{sql}` must prepare with an empty dataset_schema so JDBC routes \
                 to DoPutPreparedStatementUpdate; got {} fields",
                prepared.dataset_plan.schema().fields().len()
            );
        }
    }

    /// CHA-172 — auto-commit `CREATE SCHEMA` / `CREATE TABLE` must
    /// prepare successfully now. JDBC's `Statement.execute("CREATE
    /// TABLE …")` walks ActionCreatePreparedStatement →
    /// DoPutPreparedStatementUpdate; if the prepare leg errored the
    /// driver wouldn't get to execute. Mirrors the same empty
    /// dataset-schema invariant as the tx-control test above so the
    /// JDBC routing heuristic stays satisfied.
    #[tokio::test]
    async fn plan_for_create_prepared_statement_accepts_ddl() {
        let ctx = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let snap = snapshot(None);
        for sql in [
            "CREATE TABLE t (id BIGINT, PRIMARY KEY(id))",
            "CREATE SCHEMA s",
        ] {
            let prepared = plan_for_create_prepared_statement(&ctx, &snap, None, sql)
                .await
                .unwrap_or_else(|e| panic!("`{sql}` rejected at prepare time: {e}"));
            assert_eq!(
                prepared.rewritten_sql, None,
                "`{sql}` must not be rewritten"
            );
            assert!(
                prepared.dataset_plan.schema().fields().is_empty(),
                "`{sql}` must prepare with an empty dataset_schema so JDBC routes \
                 to DoPutPreparedStatementUpdate; got {} fields",
                prepared.dataset_plan.schema().fields().len()
            );
            assert!(
                prepared.parameter_plan.schema().fields().is_empty(),
                "`{sql}` carries no `?` placeholders; parameter_plan must be empty",
            );
        }
    }

    /// CHA-345 — transactional DDL prepares successfully now (JDBC
    /// walks ActionCreatePreparedStatement → DoPutPreparedStatementUpdate
    /// for in-tx `CREATE TABLE`; if prepare errored the driver couldn't
    /// execute the CREATE). Mirrors the auto-commit DDL prepare test:
    /// empty dataset_schema so the driver routes to the update path.
    #[tokio::test]
    async fn plan_for_create_prepared_statement_accepts_in_tx_ddl() {
        let ctx = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let snap = snapshot(Some("tx-abc"));
        let prepared = plan_for_create_prepared_statement(
            &ctx,
            &snap,
            None,
            "CREATE TABLE t (id BIGINT, PRIMARY KEY(id))",
        )
        .await
        .unwrap_or_else(|e| panic!("in-tx CREATE TABLE rejected at prepare time: {e}"));
        assert_eq!(prepared.rewritten_sql, None);
        assert!(
            prepared.dataset_plan.schema().fields().is_empty(),
            "in-tx CREATE TABLE must prepare with empty dataset_schema so JDBC routes to \
             DoPutPreparedStatementUpdate; got {} fields",
            prepared.dataset_plan.schema().fields().len()
        );
    }

    /// `plan_for_get_flight_info` is stricter than the prep helper:
    /// DML / BEGIN / COMMIT / ROLLBACK reaching it means a client
    /// misrouted (these belong on update entry-points). Pins that
    /// asymmetry so a future "let's unify" refactor doesn't
    /// accidentally widen the read-path acceptance.
    #[tokio::test]
    async fn plan_for_get_flight_info_rejects_dml_as_misroute() {
        let ctx = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let snap = snapshot(None);
        let err = plan_for_get_flight_info(&ctx, &snap, None, "INSERT INTO t VALUES (1)")
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("DoPutStatementUpdate"),
            "{}",
            err.message()
        );
    }

    /// CHA-172 — auto-commit DDL on the read entry-point is a
    /// misbehaving client (DDL belongs on update entry-points,
    /// same as DML). Pins the asymmetry between the prep helper
    /// (accepts DDL — JDBC routes through prepare) and the read
    /// helper (rejects DDL — it's never a read).
    #[tokio::test]
    async fn plan_for_get_flight_info_rejects_ddl_as_misroute() {
        let ctx = Arc::new(SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        ));
        let snap = snapshot(None);
        for sql in [
            "CREATE TABLE t (id BIGINT, PRIMARY KEY(id))",
            "CREATE SCHEMA s",
        ] {
            let err = plan_for_get_flight_info(&ctx, &snap, None, sql)
                .await
                .unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument, "{sql}");
            assert!(
                err.message().contains("DoPutStatementUpdate"),
                "[{sql}] {}",
                err.message()
            );
        }
    }
}
