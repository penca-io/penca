//! `SET` statement dispatcher (CHA-119).
//!
//! Three Penca-specific knobs that DataFusion's planner doesn't know
//! about plus a fall-through for the planner's own
//! `SET datafusion.<x> = <y>` settings:
//!
//! - **`SET search_path = '<name>'`** — Postgres-compatible
//!   session-mutable default schema. Writes
//!   `SessionConfig.options.catalog.default_schema` directly so the
//!   change is reachable from both DataFusion's SELECT name resolver
//!   and the unqualified-DML path in [`crate::dml`] (one source of
//!   truth — no shadow field).
//! - **`SET (penca.)branch = '<name>'`** — rejected with
//!   `FAILED_PRECONDITION`. Branch is connection-scoped (CHA-119); a
//!   client that wants a different branch reconnects. The bare and
//!   namespaced forms are both intercepted because both refer to the
//!   same knob.
//! - **`SET (penca.)catalog = '<name>'`** — no-op when `<name>`
//!   matches the connection's pin, otherwise rejected with
//!   `FAILED_PRECONDITION` ("catalog is fixed at handshake; reconnect
//!   to switch"). Same Postgres `Connection.setCatalog`-as-no-op
//!   semantics as PgJDBC. The status code matches
//!   `flight_sql::headers::validate_branch_header` for the analogous
//!   "client tried to mutate a handshake-pinned knob" case — gRPC
//!   semantics put a state-of-system mismatch under
//!   `FAILED_PRECONDITION`, not `INVALID_ARGUMENT` (the SQL itself
//!   parses fine; it's the session state that makes it unacceptable).
//! - **Anything else** — delegated to DataFusion's planner via
//!   `ctx.sql(...)`, so `SET datafusion.execution.batch_size = …` and
//!   similar settings keep working unchanged.
//!
//! Invoked from all four entry points where a `SET` can land
//! (`do_put_statement_update`, `get_flight_info_statement`,
//! `do_action_create_prepared_statement`,
//! `get_flight_info_prepared_statement`). The prepared-statement
//! entry points rewrite the wire-level SQL to `SELECT 1 WHERE FALSE`
//! after applying the SET, so the DoGet leg returns an empty result
//! set instead of re-applying — matching the response shape ADBC /
//! DataGrip expect for SET (per the 2026-05-06 ticket comment on the
//! retry-loop the empty-FlightInfo response triggers).
//!
//! The per-key mutation logic lives in [`handle_set_option`], which
//! is the canonical dispatcher both this SQL path and the Flight SQL
//! `SetSessionOptions` wire action route through — single source of
//! truth for "what gets written, what gets rejected, in what wording".

use datafusion::execution::context::SessionContext;
use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Set, Value, ValueWithSpan};
use tonic::Status;

use crate::session::SessionSnapshot;

/// SQL the prepared-statement / get-flight-info paths swap in after
/// applying a `SET`, so the subsequent DoGet returns a benign empty
/// result rather than re-running the SET (which would be wasted work
/// for an idempotent op) or returning an empty FlightInfo (which
/// triggers DataGrip's 5-attempt retry loop — see the 2026-05-06
/// ticket comment).
pub const SET_PLACEHOLDER_SQL: &str = "SELECT 1 WHERE FALSE";

/// Which surface invoked the dispatcher. The dispatcher's result
/// genuinely depends on protocol for one case: SQL `SET branch = …`
/// returns [`SetOptionError::Rejected`] (CHA-119 connection-scoped
/// wording → `FAILED_PRECONDITION`); wire-level
/// `SetSessionOptions(branch: …)` returns [`SetOptionError::InvalidName`]
/// (→ Flight SQL `INVALID_NAME`) because branch has no wire analog —
/// it stays Penca-specific on the `x-penca-branch` header. Every
/// other key/state combination produces the same dispatcher result on
/// both surfaces; the surface only changes how callers *render* the
/// result.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SetOptionSurface {
    Sql,
    Wire,
}

/// Outcomes the dispatcher reports for a single key/value pair.
///
/// Each variant has a fixed rendering per surface:
/// - SQL caller: `tonic::Status` (`InvalidName` → fall through to
///   DataFusion's planner; `Rejected` → `FAILED_PRECONDITION`).
/// - Flight SQL `SetSessionOptions` wire caller: per-key entry on
///   `SetSessionOptionsResult.errors` for `InvalidName`; `Rejected`
///   short-circuits the per-key mechanism and returns gRPC
///   `Status::failed_precondition` so the handshake-pinned wording
///   survives the Go ADBC driver's per-key-message flattening (see
///   [`crate::flight_sql::session_options::handle_set_session_options`]).
#[derive(Debug)]
pub(crate) enum SetOptionError {
    /// The option key isn't one Penca recognizes — or, on the wire,
    /// the key has no wire-level analog (`branch` is the canonical
    /// example: SQL recognizes it, Wire does not). SQL callers fall
    /// through to DataFusion's planner so unknown keys reach the
    /// `datafusion.*` setting path; wire callers emit `INVALID_NAME`.
    InvalidName,
    /// The option name is recognized but cannot be set in the current
    /// session state — today: SQL's `SET catalog = <other>`, SQL's
    /// `SET branch = …`, and wire-side catalog mismatch mid-session
    /// (all carry the connection-scoped wording).
    Rejected(String),
}

/// Mutation produced by the dispatcher's plan phase. Applied verbatim
/// in [`apply_plan`]; safe to collect into a `Vec` across keys so the
/// wire path can validate all entries of a multi-key
/// `SetSessionOptions` request before mutating any session state.
///
/// Returning `NoOp` lets the wire path tell "key validated, nothing to
/// apply" (the `setCatalog`-to-pinned-value case) apart from "key
/// failed validation" (`Err`).
#[derive(Debug)]
pub(crate) enum SetOptionPlan {
    /// Nothing to apply — e.g. catalog setter value matches the
    /// handshake-pinned value. The wire path still records the key as
    /// a success.
    NoOp,
    /// Write the value onto
    /// `SessionConfig.options.catalog.default_schema`.
    WriteSchema(String),
}

/// Canonical session-mutation dispatcher, planning phase.
///
/// Validates a single `(key, value)` against the session's state and
/// returns the [`SetOptionPlan`] the caller should apply, or a
/// [`SetOptionError`] explaining why the key can't be set. The plan
/// phase never mutates session state — `apply_plan` does. The split
/// is what lets the wire path's multi-key `SetSessionOptions` request
/// validate-then-apply atomically: if any key returns
/// `SetOptionError::Rejected` the wire handler short-circuits before
/// any mutation lands.
///
/// The known keys (canonical, lowercase):
/// - `search_path` / `db_schema` / `schema` →
///   [`SetOptionPlan::WriteSchema`]. Aliases coexist because
///   `search_path` is the SQL knob name (Postgres) and
///   `db_schema` / `schema` are the ADBC/JDBC `Connection.setSchema`
///   knob names the Flight SQL `SetSessionOptions` action carries.
///   Surface-independent.
/// - `branch` — surface-dependent. On `Sql`,
///   [`SetOptionError::Rejected`] (CHA-119 connection-scoped wording).
///   On `Wire`, [`SetOptionError::InvalidName`] (no wire-level analog).
/// - `catalog` — Postgres `setCatalog`-as-no-op semantics.
///   [`SetOptionPlan::NoOp`] if the value matches the handshake pin;
///   otherwise [`SetOptionError::Rejected`] with the "fixed at
///   handshake" wording on either surface (the wire handler maps this
///   to gRPC `failed_precondition` rather than per-key
///   `INVALID_VALUE` — see the type-level note on
///   [`SetOptionError::Rejected`]).
/// - anything else → [`SetOptionError::InvalidName`]. SQL callers fall
///   through to DataFusion's planner so `datafusion.*` settings keep
///   working; the wire path emits an `INVALID_NAME` per-key error.
pub(crate) fn plan_set_option(
    snapshot: &SessionSnapshot,
    surface: SetOptionSurface,
    key: &str,
    value: &str,
) -> Result<SetOptionPlan, SetOptionError> {
    let key_lower = key.to_ascii_lowercase();
    match key_lower.as_str() {
        "search_path" | "db_schema" | "schema" => Ok(SetOptionPlan::WriteSchema(value.to_string())),
        "branch" => Err(handle_branch(surface)),
        "catalog" => plan_catalog(snapshot, value),
        _ => Err(SetOptionError::InvalidName),
    }
}

/// Apply a [`SetOptionPlan`] produced by [`plan_set_option`]. The plan
/// has already been validated against the session's state; apply is
/// infallible. Sync because the only remaining mutation is the
/// `default_schema` write on the cached `SessionContext` — no
/// `catalog_store` round-trip on this path now that catalog binding
/// is handshake-only (CHA-253).
pub(crate) fn apply_plan(ctx: &SessionContext, plan: SetOptionPlan) {
    match plan {
        SetOptionPlan::NoOp => {}
        SetOptionPlan::WriteSchema(schema) => write_default_schema(ctx, schema),
    }
}

/// Convenience for single-key callers (the SQL `SET` path): plan +
/// apply in one shot. Multi-key callers (the Flight SQL
/// `SetSessionOptions` wire handler) call [`plan_set_option`] and
/// [`apply_plan`] separately so a `Rejected` outcome on one key bails
/// before any other key's mutation lands.
pub(crate) fn handle_set_option(
    snapshot: &SessionSnapshot,
    ctx: &SessionContext,
    surface: SetOptionSurface,
    key: &str,
    value: &str,
) -> Result<(), SetOptionError> {
    let plan = plan_set_option(snapshot, surface, key, value)?;
    apply_plan(ctx, plan);
    Ok(())
}

/// Apply the surface-dependent branch outcome. SQL preserves CHA-119
/// (connection-scoped reject); Wire emits `INVALID_NAME` because
/// branch has no wire-level analog.
fn handle_branch(surface: SetOptionSurface) -> SetOptionError {
    match surface {
        SetOptionSurface::Sql => SetOptionError::Rejected(connection_scoped_message("branch")),
        SetOptionSurface::Wire => SetOptionError::InvalidName,
    }
}

/// Plan a catalog setter against the session's pinned catalog
/// (CHA-253). Postgres `Connection.setCatalog`-as-no-op semantics:
/// match → `NoOp`; mismatch → `Rejected` with the handshake-pinned
/// wording. Reads from the per-request snapshot — the cached catalog
/// is immutable after mint, so the snapshot is authoritative.
fn plan_catalog(
    snapshot: &SessionSnapshot,
    new_catalog: &str,
) -> Result<SetOptionPlan, SetOptionError> {
    if new_catalog == snapshot.catalog_name {
        return Ok(SetOptionPlan::NoOp);
    }
    Err(SetOptionError::Rejected(format!(
        "catalog is fixed at handshake; reconnect to switch — \
         this connection is pinned to `{}` and cannot be changed \
         mid-session.",
        snapshot.catalog_name,
    )))
}

/// Write the new schema name onto the per-session
/// `SessionConfig.options.catalog.default_schema`. Both DataFusion's
/// SELECT name resolver and the unqualified-DML path in
/// [`crate::dml::execute`] read from this field, so a single write
/// covers both surfaces.
///
/// The write borrows DataFusion's internal `Arc<RwLock<SessionState>>`
/// briefly. Concurrent `ctx.sql(...)` callers block on the read lock
/// during this window; in practice ADBC serialises statement execution
/// per connection, so the window is never contended. (`default_schema`
/// still lives on `SessionConfig`; the open tx moved to the `ConnScope`
/// cell in CHA-345, so this is now the only runtime mutation of the
/// cached `SessionState`.)
fn write_default_schema(ctx: &SessionContext, schema: String) {
    let state_lock = ctx.state_ref();
    let mut state = state_lock.write();
    state.config_mut().options_mut().catalog.default_schema = schema;
}

fn connection_scoped_message(knob: &str) -> String {
    format!(
        "{knob} is connection-scoped; reconnect to switch — \
         the value is pinned at handshake time (CHA-119 for branch, \
         CHA-169 for catalog) and cannot be changed mid-session."
    )
}

/// Apply a parsed `SET` statement to `ctx`, returning `Ok(())` on
/// success and `FAILED_PRECONDITION` on a connection-scoped knob. The
/// caller is responsible for matching `SqlStatement::Set(set)` first
/// and for any wire-level response shaping (e.g. the
/// `SET_PLACEHOLDER_SQL` rewrite the prepared-statement path uses).
///
/// `original_sql` is the verbatim wire SQL the caller already parsed;
/// it's threaded through to the fall-through path so DataFusion's own
/// `SET datafusion.<x> = <y>` is dispatched to `ctx.sql(original_sql)`
/// directly rather than re-serializing the parsed AST via `Display` and
/// re-parsing (would be a third parse pass and would drop whitespace /
/// comment / edge-case quoting that `Display::fmt` doesn't promise to
/// round-trip).
pub async fn handle_set(
    snapshot: &SessionSnapshot,
    ctx: &SessionContext,
    original_sql: &str,
    set: Set,
) -> Result<(), Status> {
    // Other shapes (`SET TIME ZONE`, `SET TRANSACTION`, etc.) — delegate.
    let Some(names) = extract_set_names(&set) else {
        return fall_through(ctx, original_sql).await;
    };
    dispatch_recognised(snapshot, ctx, &names, &set, original_sql).await
}

/// Pull the variable-name list from a parsed `SET` regardless of which
/// of the two recognized AST shapes the parser emitted. Returns `None`
/// for shapes we don't intercept (e.g. `SET TIME ZONE`), letting the
/// caller fall through to DataFusion's planner.
fn extract_set_names(set: &Set) -> Option<Vec<String>> {
    single_assignment_names(set).or_else(|| generic_session_param_names(set))
}

/// `SET <a>.<b>... = <value>` — the Postgres / Snowflake shape DFParser
/// emits for our targets (search_path, branch, catalog).
fn single_assignment_names(set: &Set) -> Option<Vec<String>> {
    match set {
        Set::SingleAssignment {
            variable: ObjectName(parts),
            ..
        } => Some(
            parts
                .iter()
                .filter_map(|p| p.as_ident().map(|i| i.value.clone()))
                .collect(),
        ),
        _ => None,
    }
}

/// MS-SQL-style `SET <name> <value>` — included for parity in case a
/// future dialect lands one of our targets here. DFParser doesn't
/// currently emit this for the Postgres `SET search_path = 'sales'`
/// form, but the plan calls out matching both shapes so a dialect
/// change doesn't silently slip a SET through unchecked.
fn generic_session_param_names(set: &Set) -> Option<Vec<String>> {
    use datafusion::sql::sqlparser::ast::SetSessionParamKind;
    match set {
        Set::SetSessionParam(SetSessionParamKind::Generic(g)) => Some(g.names.clone()),
        _ => None,
    }
}

/// Route a parsed `SET <names> = <value>` through [`handle_set_option`]
/// and render the result for the SQL surface.
///
/// Names that don't normalize to a Penca-recognized key (e.g.,
/// `datafusion.execution.batch_size`) skip the handler entirely and
/// fall through to DataFusion's planner — keeps options the planner
/// owns working unchanged.
async fn dispatch_recognised(
    snapshot: &SessionSnapshot,
    ctx: &SessionContext,
    names: &[String],
    set: &Set,
    original_sql: &str,
) -> Result<(), Status> {
    let Some(key) = normalize_set_key(names) else {
        return fall_through(ctx, original_sql).await;
    };
    let Some(string_value) = extract_single_string(set) else {
        return Err(Status::invalid_argument(format!(
            "SET {key} expects a single quoted-string value, e.g. \
             `SET {key} = 'sales'`; multi-schema lists are not yet supported"
        )));
    };
    match handle_set_option(snapshot, ctx, SetOptionSurface::Sql, &key, &string_value) {
        Ok(()) => Ok(()),
        Err(SetOptionError::InvalidName) => fall_through(ctx, original_sql).await,
        Err(SetOptionError::Rejected(msg)) => Err(Status::failed_precondition(msg)),
    }
}

/// Normalize the parsed name list to a single canonical option key
/// understood by [`handle_set_option`], or `None` if the SQL caller
/// should fall through to DataFusion's planner.
///
/// Maps `penca.branch` / `penca.catalog` to their bare forms so the
/// handler doesn't need to know about the optional `penca.`
/// namespace. The set of recognized SQL-side keys is deliberately
/// narrower than [`handle_set_option`]'s vocabulary — the SQL path
/// only accepts `search_path` (Postgres-style), not `db_schema` /
/// `schema` (ADBC/JDBC wire names), preserving exact CHA-119 surface
/// for SQL clients.
fn normalize_set_key(names: &[String]) -> Option<String> {
    let lower: Vec<String> = names.iter().map(|n| n.to_ascii_lowercase()).collect();
    match lower.as_slice() {
        [name] if name == "search_path" || name == "branch" || name == "catalog" => {
            Some(name.clone())
        }
        [ns, name] if ns == "penca" && (name == "branch" || name == "catalog") => {
            Some(name.clone())
        }
        _ => None,
    }
}

/// Pull a single string-shaped value out of a parsed `SET`. Accepts
/// quoted strings (`SET search_path = 'sales'`) and bare identifiers
/// (`SET search_path = sales`); rejects multi-value forms.
fn extract_single_string(set: &Set) -> Option<String> {
    let value = match set {
        Set::SingleAssignment { values, .. } if values.len() == 1 => &values[0],
        _ => return None,
    };
    match value {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s) | Value::DoubleQuotedString(s),
            ..
        }) => Some(s.clone()),
        Expr::Identifier(ident) => Some(ident.value.clone()),
        _ => None,
    }
}

/// Hand the SET to DataFusion's planner. Covers `SET datafusion.<x> = <y>`,
/// `SET TIME ZONE`, `SET TRANSACTION`, and any other shape we don't
/// special-case. Takes the original wire SQL (not `format!("{set}")`)
/// so the planner sees the exact string the client sent — `Display::fmt`
/// for `Set` isn't promised to round-trip whitespace / comments / edge-
/// case quoting, and re-parsing the formatted form would be a third
/// parse pass for what's already a fully-parsed AST.
///
/// Errors propagate as `INVALID_ARGUMENT` so the client sees
/// DataFusion's "could not find config namespace" / "unknown option"
/// wording when they hand us something nonsensical.
async fn fall_through(ctx: &SessionContext, original_sql: &str) -> Result<(), Status> {
    ctx.sql(original_sql)
        .await
        .map_err(|e| Status::invalid_argument(format!("SET failed: {e}")))?
        .collect()
        .await
        .map_err(|e| Status::invalid_argument(format!("SET failed: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_one_statement;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
    use tonic::Code;

    /// Bare DataFusion context for the dispatcher unit tests. The
    /// schema-write arm needs the default-feature planner so the
    /// fall-through `SET datafusion.…` test can actually mutate
    /// config; the catalog/branch arms don't touch the planner.
    fn bare_ctx() -> SessionContext {
        let state = SessionStateBuilder::new().with_default_features().build();
        SessionContext::new_with_state(state)
    }

    /// Per-request snapshot fixture pinned to `public`/`main` —
    /// matches the env defaults the rest of the suite is calibrated
    /// against. Tests that need a different pin construct a new one
    /// inline via `SessionSnapshot::for_test`.
    fn snapshot() -> SessionSnapshot {
        SessionSnapshot::for_test(
            "public",
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-00000000beef",
            "main",
            None,
        )
    }

    fn parse_set(sql: &str) -> Set {
        match parse_one_statement(sql).unwrap() {
            SqlStatement::Set(s) => s,
            other => panic!("expected SET, got {other:?}"),
        }
    }

    /// `handle_set` takes the original wire SQL alongside the parsed
    /// `Set` so the fall-through path can hand it straight to
    /// DataFusion. The recognised-knob paths (search_path, branch,
    /// catalog) ignore the string entirely; this helper reflects the
    /// caller contract where both come from one SQL fragment.
    async fn apply(sql: &str) -> Result<(), Status> {
        let ctx = bare_ctx();
        handle_set(&snapshot(), &ctx, sql, parse_set(sql)).await
    }

    #[tokio::test]
    async fn search_path_writes_default_schema_onto_session_config() {
        let ctx = bare_ctx();
        handle_set(
            &snapshot(),
            &ctx,
            "SET search_path = 'sales'",
            parse_set("SET search_path = 'sales'"),
        )
        .await
        .unwrap();
        assert_eq!(ctx.state().config_options().catalog.default_schema, "sales");
    }

    #[tokio::test]
    async fn search_path_accepts_bare_identifier() {
        let ctx = bare_ctx();
        handle_set(
            &snapshot(),
            &ctx,
            "SET search_path = sales",
            parse_set("SET search_path = sales"),
        )
        .await
        .unwrap();
        assert_eq!(ctx.state().config_options().catalog.default_schema, "sales");
    }

    #[tokio::test]
    async fn set_branch_rejected_with_connection_scoped_message() {
        let err = apply("SET branch = 'feat'").await.unwrap_err();
        // FailedPrecondition for parity with `validate_branch_header`
        // — both surfaces reject "tried to mutate the handshake-pinned
        // branch on a live connection" with the same gRPC code.
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(
            err.message().contains("connection-scoped"),
            "{}",
            err.message()
        );
        assert!(err.message().contains("branch"), "{}", err.message());
    }

    #[tokio::test]
    async fn set_penca_branch_rejected_with_same_message_as_bare_branch() {
        // Both forms refer to the same knob — the rejection wording
        // shouldn't diverge.
        let err = apply("SET penca.branch = 'feat'").await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("connection-scoped"));
        assert!(err.message().contains("branch"));
    }

    #[tokio::test]
    async fn set_catalog_rejected_with_fixed_at_handshake_message() {
        let err = apply("SET catalog = 'other'").await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(
            err.message().contains("fixed at handshake"),
            "{}",
            err.message()
        );
        assert!(err.message().contains("catalog"), "{}", err.message());
    }

    #[tokio::test]
    async fn set_penca_catalog_rejected_with_same_message_as_bare_catalog() {
        let err = apply("SET penca.catalog = 'other'").await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("fixed at handshake"));
        assert!(err.message().contains("catalog"));
    }

    #[tokio::test]
    async fn set_datafusion_option_falls_through_to_planner() {
        // Smoke test that the fall-through path hands the SET to
        // DataFusion's planner verbatim and the planner actually
        // applies the value (no rejection, no silent no-op).
        let ctx = bare_ctx();
        handle_set(
            &snapshot(),
            &ctx,
            "SET datafusion.execution.batch_size = 8192",
            parse_set("SET datafusion.execution.batch_size = 8192"),
        )
        .await
        .unwrap();
        assert_eq!(ctx.state().config_options().execution.batch_size, 8192);
    }

    #[tokio::test]
    async fn set_search_path_is_case_insensitive() {
        let ctx = bare_ctx();
        handle_set(
            &snapshot(),
            &ctx,
            "SET SEARCH_PATH = 'sales'",
            parse_set("SET SEARCH_PATH = 'sales'"),
        )
        .await
        .unwrap();
        assert_eq!(ctx.state().config_options().catalog.default_schema, "sales");
    }

    // -- Direct tests of handle_set_option ----------------------------

    /// Schema aliases (`db_schema` / `schema`) are wire-side knob names
    /// the ADBC/JDBC `Connection.setSchema` path emits; the SQL surface
    /// only exposes `search_path`. All three should land on the same
    /// `default_schema` write.
    #[tokio::test]
    async fn handle_set_option_db_schema_writes_default_schema() {
        let ctx = bare_ctx();
        handle_set_option(
            &snapshot(),
            &ctx,
            SetOptionSurface::Wire,
            "db_schema",
            "sales",
        )
        .unwrap();
        assert_eq!(ctx.state().config_options().catalog.default_schema, "sales");
    }

    #[tokio::test]
    async fn handle_set_option_schema_alias_writes_default_schema() {
        let ctx = bare_ctx();
        handle_set_option(&snapshot(), &ctx, SetOptionSurface::Wire, "schema", "sales").unwrap();
        assert_eq!(ctx.state().config_options().catalog.default_schema, "sales");
    }

    #[tokio::test]
    async fn handle_set_option_unknown_key_returns_invalid_name() {
        let ctx = bare_ctx();
        let err =
            handle_set_option(&snapshot(), &ctx, SetOptionSurface::Wire, "bogus", "x").unwrap_err();
        assert!(matches!(err, SetOptionError::InvalidName));
    }

    /// Branch surfaces differently per protocol: SQL gets the
    /// connection-scoped Rejected wording (FailedPrecondition); Wire
    /// gets InvalidName because there's no wire-level branch knob.
    #[tokio::test]
    async fn handle_set_option_branch_on_sql_surface_returns_rejected() {
        let ctx = bare_ctx();
        let err = handle_set_option(&snapshot(), &ctx, SetOptionSurface::Sql, "branch", "feat")
            .unwrap_err();
        let SetOptionError::Rejected(msg) = err else {
            panic!("expected Rejected, got {err:?}");
        };
        assert!(msg.contains("connection-scoped"));
        assert!(msg.contains("branch"));
    }

    #[tokio::test]
    async fn handle_set_option_branch_on_wire_surface_returns_invalid_name() {
        let ctx = bare_ctx();
        let err = handle_set_option(&snapshot(), &ctx, SetOptionSurface::Wire, "branch", "feat")
            .unwrap_err();
        assert!(matches!(err, SetOptionError::InvalidName));
    }

    /// Setting catalog to the pinned value is a no-op (Postgres
    /// `setCatalog` semantics).
    #[tokio::test]
    async fn handle_set_option_catalog_match_is_noop() {
        let ctx = bare_ctx();
        handle_set_option(
            &snapshot(),
            &ctx,
            SetOptionSurface::Wire,
            "catalog",
            "public",
        )
        .unwrap();
    }

    /// Setting catalog to a value other than the pin returns
    /// `Rejected` from the dispatcher on either surface — the
    /// surface-specific rendering (SQL: `FailedPrecondition`; Wire:
    /// gRPC `Status::failed_precondition` short-circuiting the per-key
    /// error mechanism) is the caller's responsibility (`handle_set`
    /// and `flight_sql::session_options::handle_set_session_options`).
    #[tokio::test]
    async fn handle_set_option_catalog_mismatch_wire_is_rejected() {
        let ctx = bare_ctx();
        let err = handle_set_option(
            &snapshot(),
            &ctx,
            SetOptionSurface::Wire,
            "catalog",
            "other",
        )
        .unwrap_err();
        let SetOptionError::Rejected(msg) = err else {
            panic!("expected Rejected, got {err:?}");
        };
        assert!(msg.contains("fixed at handshake"), "{msg}");
        assert!(msg.contains("public"), "{msg}");
    }

    #[tokio::test]
    async fn handle_set_option_catalog_mismatch_sql_is_rejected() {
        let ctx = bare_ctx();
        let err = handle_set_option(&snapshot(), &ctx, SetOptionSurface::Sql, "catalog", "other")
            .unwrap_err();
        let SetOptionError::Rejected(msg) = err else {
            panic!("expected Rejected, got {err:?}");
        };
        assert!(msg.contains("fixed at handshake"), "{msg}");
    }
}
