//! Transaction control (`BEGIN` / `COMMIT` / `ROLLBACK`) for penca-sql-server.
//!
//! `BEGIN` calls `WriteService::BeginTx` eagerly using the **connection's**
//! pinned catalog (set at conn-mint time from the SQL server's
//! `default_catalog` config or the `x-penca-catalog` header), records
//! the resulting `tx_uuid` on the per-TCP-connection
//! [`crate::session::ConnSession`], and returns. `COMMIT` and `ROLLBACK`
//! atomically take the cached `(catalog_uuid, tx_uuid)` pair via
//! [`ConnSession::take_open_tx`] and dispatch to `CommitTx` / `AbortTx`.
//!
//! Per [CHA-163](https://linear.app/chapala/issue/CHA-163), Penca
//! transactions are catalog-scoped — a single tx spans every schema in its
//! catalog. Cross-catalog atomicity is **not** supported (and would force
//! 2PC across per-catalog commit_tx_logs); a DML against a different catalog is
//! rejected by [`validate_session_catalog`] (used at the dml.rs entry
//! point) with `FAILED_PRECONDITION`. The check fires whether or not a
//! tx is open — catalog is a connection-level invariant (CHA-169), so a
//! mismatch is an error in either mode.
//!
//! Wire payloads route by `branch_uuid` (rename-stable per CHA-255);
//! `branch_name` stays only on the [`SessionSnapshot`] for the
//! `validate_branch_header` / `SET branch` rejection paths.
//!
//! See [ADR 0007](../../docs/decisions/0007-session-entity.md) for why
//! sessions are TCP-conn-local rather than cookie-identified.

use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use penca_proto::external::v1::write_service_client::WriteServiceClient;
use penca_proto::external::v1::{AbortTxRequest, BeginTxRequest, CommitTxRequest};
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::session::{ConnSession, SessionSnapshot};

/// Reject `BEGIN` / `START TRANSACTION` variants Penca doesn't honour, so
/// a client that asks for `BEGIN ISOLATION LEVEL SERIALIZABLE` doesn't
/// silently get the default snapshot tx instead. Returns
/// `Status::unimplemented` with a message naming the offending modifier.
///
/// Accepted shapes — every other field on `Statement::StartTransaction`
/// must be the default. Each is exercised by
/// [`tests::validate_start_transaction_accepts_plain_begin`]:
///
/// - `BEGIN`, `BEGIN TRANSACTION`, `BEGIN WORK`
/// - `START TRANSACTION`
///
/// Rejected shapes:
///
/// - `BEGIN ISOLATION LEVEL …` — no isolation-level knob; one mode (snapshot
///   in an open Penca tx, with RYOW per CHA-165).
/// - `BEGIN READ ONLY` / `BEGIN READ WRITE` — Penca has no read-only tx
///   mode; accepting `READ ONLY` would mean later DML inside the tx
///   *should* fail and doesn't.
/// - `BEGIN DEFERRED|IMMEDIATE|EXCLUSIVE|TRY|CATCH` — SQLite locking and
///   T-SQL exception modifiers; nothing to map them to.
/// - `BEGIN … END` blocks (`statements` / `exception` /
///   `has_end_keyword`) — BigQuery procedural form; Penca isn't a
///   procedural runtime.
///
/// Pure function on the AST — no I/O, no cache touch.
pub fn validate_start_transaction(stmt: &SqlStatement) -> Result<(), Status> {
    let SqlStatement::StartTransaction {
        modes,
        begin: _,
        transaction: _,
        modifier,
        statements,
        exception,
        has_end_keyword,
    } = stmt
    else {
        return Err(Status::internal(
            "validate_start_transaction called on non-StartTransaction statement",
        ));
    };

    if !modes.is_empty() {
        // Name every offending mode in one message so a client fixing
        // a multi-mode `BEGIN` (e.g. `ISOLATION LEVEL ... READ ONLY`)
        // gets one rejection round-trip, not one per mode.
        use datafusion::sql::sqlparser::ast::TransactionMode;
        let listed = modes
            .iter()
            .map(|mode| match mode {
                TransactionMode::IsolationLevel(level) => {
                    format!("ISOLATION LEVEL {level}")
                }
                TransactionMode::AccessMode(access) => format!("{access}"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Status::unimplemented(format!(
            "BEGIN {listed} is not supported; penca-sql-server runs every \
             BEGIN under snapshot isolation with read-your-own-writes \
             (CHA-165) and has no read-only/read-write transaction mode. \
             Reissue as a plain `BEGIN`."
        )));
    }
    if let Some(modifier) = modifier {
        return Err(Status::unimplemented(format!(
            "BEGIN {modifier} is not supported; Penca has no SQLite-style \
             locking or T-SQL exception modifiers. Reissue as a plain `BEGIN`.",
        )));
    }
    if !statements.is_empty() || exception.is_some() || *has_end_keyword {
        return Err(Status::unimplemented(
            "procedural `BEGIN ... END` blocks are not supported; \
             penca-sql-server treats `BEGIN` purely as a transaction-control \
             statement. Reissue without the inner statements / EXCEPTION / END.",
        ));
    }
    Ok(())
}

/// Begin a Penca transaction for the conn's pinned catalog. Returns
/// the resolved `(catalog_uuid, tx_uuid)` so the caller can return
/// `tx_uuid` to the client.
///
/// `snapshot` is the per-request [`SessionSnapshot`] taken at request
/// entry. Used to address the `BeginTx` payload by `branch_uuid` (CHA-255
/// — rename-stable) and to thread the `catalog_uuid` back to the caller.
/// `conn` is the per-TCP-connection [`ConnSession`]; `set_open_tx`
/// records the new `tx_uuid` on the authoritative `open_tx_uuid` mutex
/// and flips the `Arc`-shared `ConnScope.open_tx_cell` in one critical
/// section (CHA-345 — the provider tree reads the cell; see
/// [`ConnSession::set_open_tx`]).
///
/// Rejects `FAILED_PRECONDITION` if the conn already has an open
/// transaction — Penca doesn't support nested transactions, and a
/// re-`BEGIN` is almost certainly a client bug.
///
/// **Concurrency contract.** Multiple HTTP/2 streams on the same TCP
/// conn share the same `Arc<ConnSession>`; `set_open_tx` takes the
/// conn's `tokio::sync::Mutex` over `open_tx_uuid`, so two concurrent
/// `BEGIN`s on the same conn serialise — the first wins, the second
/// hits the "already has an open transaction" branch. ADBC drivers
/// serialise statement execution per connection anyway, so contention
/// is rare in practice.
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        tx_uuid = tracing::field::Empty,
    ),
    err,
)]
pub async fn handle_begin(
    conn: &ConnSession,
    snapshot: &SessionSnapshot,
    write_channel: &Channel,
) -> Result<(String, String), Status> {
    if snapshot.open_tx_uuid.is_some() {
        return Err(Status::failed_precondition(
            "session already has an open transaction; nested transactions are not supported",
        ));
    }

    let tx_uuid = call_begin_tx(
        write_channel,
        &snapshot.catalog_name,
        &snapshot.catalog_uuid,
        &snapshot.branch_uuid,
    )
    .await?;
    tracing::Span::current().record("tx_uuid", tx_uuid.as_str());
    conn.set_open_tx(tx_uuid.clone()).await?;
    Ok((snapshot.catalog_uuid.clone(), tx_uuid))
}

/// Commit the session's open Penca transaction.
///
/// Reads + clears the open tx in a single [`SessionCache::take_open_tx`]
/// borrow — the catalog_uuid and tx_uuid are returned together so the
/// CommitTx request payload can be assembled without a second cache hop
/// (and without the prior split-read race window between
/// `clear_open_tx` and `get_catalog`).
///
/// `COMMIT` with no open transaction is **not** a no-op — it returns
/// `FAILED_PRECONDITION`. A bare `COMMIT` outside a `BEGIN` block usually
/// means the client lost track of state (e.g. the session was idle-evicted
/// mid-transaction); silently succeeding would mask that. Matches Postgres
/// (which emits "WARNING: there is no transaction in progress" and ignores
/// the COMMIT), surfaced here as a clean error.
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        tx_uuid = tracing::field::Empty,
    ),
    err,
)]
pub async fn handle_commit(
    conn: &ConnSession,
    snapshot: &SessionSnapshot,
    write_channel: &Channel,
) -> Result<(), Status> {
    let Some((catalog_uuid, tx_uuid)) = conn.take_open_tx().await else {
        return Err(Status::failed_precondition("no open transaction to commit"));
    };
    tracing::Span::current().record("tx_uuid", tx_uuid.as_str());
    let mut client = WriteServiceClient::new(write_channel.clone());
    // CommitTx is catalog-scoped (CHA-163) — schema is not part of
    // the addressing. The branch is read from the session snapshot
    // by uuid (CHA-255 — rename-stable); the WriteService uses
    // it to target the leaf `commit_tx_log` / `begin_tx_log` /
    // `abort_tx_log` partitions directly.
    let req = CommitTxRequest {
        catalog_uuid: Some(catalog_uuid),
        branch_uuid: Some(snapshot.branch_uuid.clone()),
        branch_name: None,
        tx_uuid,
        ..Default::default()
    };
    client
        .commit_tx(req)
        .await
        .map_err(map_write_service_status)
        .map(|_| ())
}

/// Abort the session's open Penca transaction. Uses [`AbortTxRequest`] from
/// CHA-162 to insert an `abort_tx_log` row, blocking any later `CommitTx` on
/// the same `tx_uuid` and letting the lifecycle sweeper eagerly purge orphan
/// upsert/delete log rows.
///
/// `ROLLBACK` with no open transaction is `FAILED_PRECONDITION` (same
/// reasoning as `handle_commit`).
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        tx_uuid = tracing::field::Empty,
    ),
    err,
)]
pub async fn handle_rollback(
    conn: &ConnSession,
    snapshot: &SessionSnapshot,
    write_channel: &Channel,
) -> Result<(), Status> {
    let Some((catalog_uuid, tx_uuid)) = conn.take_open_tx().await else {
        return Err(Status::failed_precondition(
            "no open transaction to roll back",
        ));
    };
    tracing::Span::current().record("tx_uuid", tx_uuid.as_str());
    let mut client = WriteServiceClient::new(write_channel.clone());
    // See `handle_commit` for the catalog-scoped + branch addressing
    // rationale.
    let req = AbortTxRequest {
        catalog_uuid: Some(catalog_uuid),
        branch_uuid: Some(snapshot.branch_uuid.clone()),
        branch_name: None,
        tx_uuid,
        ..Default::default()
    };
    client
        .abort_tx(req)
        .await
        .map_err(map_write_service_status)
        .map(|_| ())
}

/// Reject a request whose target catalog differs from the session's
/// pinned catalog. This is the connection-level analog of Postgres
/// rejecting cross-database access on a connection — the session is
/// pinned to one catalog at mint time and every catalog-scoped action
/// (BEGIN, DML, SELECT) must agree.
///
/// Fires whether or not a tx is open: catalog mismatch is an error in
/// auto-commit mode too. The cross-catalog DML test in
/// `tests/integration/integration_flight_sql_test.py` exercises the
/// open-tx case; the auto-commit case is covered by the same rejection
/// path. Pure function on the snapshot — no cache hit.
pub fn validate_session_catalog(
    snapshot: &SessionSnapshot,
    target_catalog_uuid: &str,
) -> Result<(), Status> {
    if snapshot.catalog_uuid != target_catalog_uuid {
        let session_catalog_uuid = &snapshot.catalog_uuid;
        return Err(Status::failed_precondition(format!(
            "request targets catalog {target_catalog_uuid}, but this connection \
             is pinned to catalog {session_catalog_uuid}; cross-catalog access is \
             not supported on a single connection. Reconnect with the desired catalog."
        )));
    }
    Ok(())
}

/// Name-level cross-catalog check fired by [`crate::dml::execute`] *before*
/// the per-DML `get_table` round-trip. Necessary because the wire
/// `get_table` routes by `branch_uuid` (CHA-255 — rename-stable), which
/// is the conn's branch in the conn's catalog — addressing a different
/// catalog with that uuid hits a missing partition on the server side
/// (the partition table name hashes `(catalog_uuid, branch_uuid)`, so a
/// branch_uuid from a different catalog lookups against the target
/// catalog produce "relation does not exist" rather than the actionable
/// `cross-catalog` rejection).
///
/// The post-fetch [`validate_session_catalog`] check stays — it catches
/// the (rare) case where the resolved `(catalog_name -> catalog_uuid)`
/// pair on the server differs from the conn's pin (e.g. a rename + new
/// catalog with the same name combo). Both checks together close the
/// cross-catalog rejection on the name and the uuid axes.
pub fn validate_session_catalog_name(
    snapshot: &SessionSnapshot,
    target_catalog_name: &str,
) -> Result<(), Status> {
    if snapshot.catalog_name != target_catalog_name {
        let session_catalog_name = &snapshot.catalog_name;
        return Err(Status::failed_precondition(format!(
            "request targets catalog `{target_catalog_name}`, but this connection \
             is pinned to catalog `{session_catalog_name}`; cross-catalog access is \
             not supported on a single connection. Reconnect with the desired catalog."
        )));
    }
    Ok(())
}

/// Resolve the `tx_uuid` to thread through `WriteData` for a DML statement.
///
/// Resolution order:
/// 1. **Explicit `transaction_id` on the request payload** — used as-is,
///    the session cache is bypassed. Two callers reach this branch:
///    - **Structured Flight SQL transactions.** ADBC's
///      `connection.set_autocommit(False)` (and equivalents) calls
///      `do_action_begin_transaction`, which returns the real `tx_uuid`
///      bytes; the driver threads that `tx_uuid` through every subsequent
///      `CommandStatementUpdate.transaction_id` on the same connection
///      until `do_action_end_transaction`.
///    - **Programmatic Penca clients** that opened the tx via
///      `WriteService::BeginTx` directly over gRPC and are threading the
///      resulting `tx_uuid` through Flight SQL DML manually.
/// 2. **Snapshot says session has an open tx** — return the cached
///    `tx_uuid`. This is the raw-SQL `BEGIN ... INSERT ... COMMIT`
///    path: `BEGIN` populated the cache but the `INSERT` didn't carry a
///    `transaction_id` on the wire.
/// 3. **No open tx** — return `None`. `WriteData` auto-commits its own
///    one-shot tx.
///
/// The cross-catalog rejection that used to live here (CHA-163) moved up
/// to [`validate_session_catalog`] (CHA-169). The session's catalog is a
/// connection-level invariant, so the check fires before this function
/// runs and applies in auto-commit mode too.
///
/// Pure function — operates on the snapshot, no cache hit. Per-session
/// serialisation contract documented on [`handle_begin`] still holds:
/// ADBC drivers serialise statement execution per connection, so a
/// concurrent `COMMIT` between this read and the downstream `WriteData`
/// is impossible for the supported clients.
pub fn resolve_tx_uuid_for_dml(
    snapshot: &SessionSnapshot,
    explicit_tx_uuid: Option<&str>,
) -> Option<String> {
    if let Some(tx) = explicit_tx_uuid {
        return Some(tx.to_string());
    }
    snapshot.open_tx_uuid.clone()
}

async fn call_begin_tx(
    write_channel: &Channel,
    catalog_name: &str,
    catalog_uuid: &str,
    branch_uuid: &str,
) -> Result<String, Status> {
    let mut client = WriteServiceClient::new(write_channel.clone());
    let req = BeginTxRequest {
        catalog_uuid: Some(catalog_uuid.to_string()),
        catalog_name: Some(catalog_name.to_string()),
        // CHA-255: route by branch_uuid (rename-stable).
        branch_uuid: Some(branch_uuid.to_string()),
        branch_name: None,
        // Empty until CHA-159 wires the auth interceptor in.
        author: String::new(),
        // BeginTx invoked by SQL `BEGIN` — no user-supplied comment.
        comment: String::new(),
        ..Default::default()
    };
    let resp = client
        .begin_tx(req)
        .await
        .map_err(map_write_service_status)?;
    Ok(resp.into_inner().tx_uuid)
}

/// Pass through user-actionable codes (`FAILED_PRECONDITION`, `NOT_FOUND`,
/// `INVALID_ARGUMENT`) as-is so the SQL client sees the real error; surface
/// other codes as `INTERNAL` to distinguish "Penca said no" from
/// "WriteService unavailable / unexpected RPC error."
fn map_write_service_status(s: Status) -> Status {
    match s.code() {
        Code::FailedPrecondition | Code::NotFound | Code::InvalidArgument => s,
        _ => Status::internal(format!("WriteService error: {s}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // CHA-236: namespace UUIDs are server-minted random values; tests
    // use stable fixtures keyed by catalog name so the
    // session-catalog-validation tests remain deterministic.
    fn fixture_uuid_for(catalog_name: &str) -> String {
        penca_core::naming::deterministic_uuid_from(&[catalog_name, "test-fixture"]).to_string()
    }

    fn other_catalog_uuid() -> String {
        fixture_uuid_for("other")
    }

    fn default_catalog_uuid() -> String {
        fixture_uuid_for("public")
    }

    fn snapshot_for(catalog_name: &str, open_tx_uuid: Option<&str>) -> SessionSnapshot {
        SessionSnapshot::for_test(
            catalog_name,
            fixture_uuid_for(catalog_name),
            // Fixture branch_uuid: not the user-facing UUID semantics; the
            // tx tests don't exercise routing-by-branch_uuid, so a stable
            // sentinel is fine.
            "00000000-0000-0000-0000-00000000beef",
            "main",
            open_tx_uuid.map(String::from),
        )
    }

    #[test]
    fn validate_session_catalog_accepts_match() {
        let snap = snapshot_for("public", None);
        validate_session_catalog(&snap, &default_catalog_uuid())
            .expect("session catalog matches DML target");
    }

    #[test]
    fn validate_session_catalog_rejects_cross_catalog() {
        let snap = snapshot_for("public", None);
        let err = validate_session_catalog(&snap, &other_catalog_uuid())
            .expect_err("cross-catalog access must reject");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("cross-catalog"));
    }

    #[test]
    fn resolve_tx_uuid_explicit_wins() {
        let snap = snapshot_for("public", Some("cached"));
        // Explicit beats cached — the structured Flight SQL transaction
        // path threads its own tx_uuid through the request payload.
        assert_eq!(
            resolve_tx_uuid_for_dml(&snap, Some("explicit")),
            Some("explicit".to_string())
        );
    }

    #[test]
    fn resolve_tx_uuid_falls_back_to_snapshot() {
        let snap = snapshot_for("public", Some("cached"));
        assert_eq!(
            resolve_tx_uuid_for_dml(&snap, None),
            Some("cached".to_string())
        );
    }

    #[test]
    fn resolve_tx_uuid_returns_none_for_auto_commit() {
        let snap = snapshot_for("public", None);
        // No explicit, no cached → WriteData auto-commits.
        assert_eq!(resolve_tx_uuid_for_dml(&snap, None), None);
    }

    /// Helper — parse one SQL statement for the validator tests below.
    fn parse(sql: &str) -> SqlStatement {
        use datafusion::sql::parser::{DFParser, Statement as DFStatement};
        let mut stmts = DFParser::parse_sql(sql).unwrap();
        match stmts.pop_front().unwrap() {
            DFStatement::Statement(b) => *b,
            _ => panic!("expected plain SQL statement"),
        }
    }

    #[test]
    fn validate_start_transaction_accepts_plain_begin() {
        for sql in [
            "BEGIN",
            "BEGIN TRANSACTION",
            "BEGIN WORK",
            "START TRANSACTION",
        ] {
            validate_start_transaction(&parse(sql))
                .unwrap_or_else(|e| panic!("plain `{sql}` should pass: {e}"));
        }
    }

    #[test]
    fn validate_start_transaction_rejects_isolation_level() {
        for sql in [
            "BEGIN ISOLATION LEVEL SERIALIZABLE",
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
            "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ] {
            let err = validate_start_transaction(&parse(sql))
                .expect_err(&format!("`{sql}` should reject"));
            assert_eq!(err.code(), Code::Unimplemented, "{sql}");
            assert!(
                err.message().to_lowercase().contains("isolation level"),
                "`{sql}`: error must name the modifier; got: {}",
                err.message()
            );
        }
    }

    #[test]
    fn validate_start_transaction_rejects_access_mode() {
        for sql in [
            "BEGIN READ ONLY",
            "BEGIN READ WRITE",
            "BEGIN TRANSACTION READ ONLY",
        ] {
            let err = validate_start_transaction(&parse(sql))
                .expect_err(&format!("`{sql}` should reject"));
            assert_eq!(err.code(), Code::Unimplemented, "{sql}");
            let msg = err.message().to_lowercase();
            assert!(
                msg.contains("read only") || msg.contains("read write"),
                "`{sql}`: error must name the modifier; got: {}",
                err.message()
            );
        }
    }

    /// `BEGIN ISOLATION LEVEL ... READ ONLY` parses to two modes.
    /// The rejection message must name both so a client fixing one
    /// gets the second flagged in the same round-trip.
    #[test]
    fn validate_start_transaction_lists_every_mode_in_one_error() {
        let err =
            validate_start_transaction(&parse("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY"))
                .expect_err("multi-mode BEGIN should reject");
        assert_eq!(err.code(), Code::Unimplemented);
        let msg = err.message().to_lowercase();
        assert!(
            msg.contains("isolation level") && msg.contains("read only"),
            "expected both modes named in one message; got: {}",
            err.message()
        );
    }

    /// `BEGIN DEFERRED` / `IMMEDIATE` / `EXCLUSIVE` (SQLite) and
    /// `BEGIN TRY` / `CATCH` (T-SQL) hit the `modifier` branch of the
    /// validator. `DFParser`'s `GenericDialect` does flow these through
    /// `Statement::StartTransaction.modifier`, so the rejection branch
    /// is reachable in production and needs coverage to lock the
    /// "BEGIN {modifier} is not supported" contract.
    #[test]
    fn validate_start_transaction_rejects_modifier() {
        for sql in ["BEGIN DEFERRED", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE"] {
            let err = validate_start_transaction(&parse(sql))
                .expect_err(&format!("`{sql}` should reject"));
            assert_eq!(err.code(), Code::Unimplemented, "{sql}");
            assert!(
                err.message().to_lowercase().contains("locking")
                    || err.message().to_lowercase().contains("modifier"),
                "`{sql}`: error must name the modifier; got: {}",
                err.message()
            );
        }
    }
}
