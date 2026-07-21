//! Transaction-state types and `HotStorageClient` tx-log methods.

use penca_db::dialect::pg::PgDialect;
use penca_db::dialect::{DbDialect, Dialect};
use penca_db::driver::{DbDriver, SqlValue};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::{HotStorageClient, HotStorageError};

/// Status of a transaction looked up via
/// [`HotStorageClient::get_tx_status`]. The four variants are
/// mutually exclusive states a tx can be in — `Open` is the only one
/// that lets a CommitTx / AbortTx proceed; the other three are
/// terminal.
///
/// Pg's clock is canonical for the `Open` ↔ `Expired` boundary; an
/// `abort_tx_log` row short-circuits to `Aborted` regardless of TTL
/// (matches the lifecycle sweep's "abort-then-purge" ordering); a
/// `commit_tx_log` row short-circuits to `Committed` (data integrity
/// invariant: a tx can be `Aborted` xor `Committed` xor neither,
/// never both — guarded at INSERT time by `commit_open_tx` /
/// `auto_commit_tx` / `abort_tx`).
///
/// Naming note: the action that creates the row is called BEGIN
/// (matching SQL convention; see `begin_tx_log`), but the *state* a
/// tx is in after BeginTx and before any terminal action is `Open`.
/// English uses different roots for the action ("begin") and the
/// state ("open") here; we don't try to force them together.
///
/// Each variant carries the timestamp that defines it so error
/// messages can include "expired at X" / "aborted at Y" /
/// "committed at Z" without an extra round-trip. The branch is
/// **not** carried — every caller already knows it (RPCs require
/// branch in the request, and `get_tx_status` is called against the
/// leaf partitions for that branch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxStatus {
    /// Tx exists, not expired, not aborted, not committed — safe to
    /// commit / abort / read with RYOW visibility. `began_at_micros`
    /// is the Pg-clock timestamp recorded by `BeginTx` (tx-timeout axis);
    /// `began_at_seq_num` (CHA-429) is the `commit_tx_log_seq_num` counter
    /// frontier captured in the same BEGIN statement — the open tx's
    /// snapshot floor on the commit-order axis (visible iff
    /// `commit_seq_num < began_at_seq_num`).
    Open {
        began_at_micros: i64,
        began_at_seq_num: i64,
    },
    /// Tx exists but `expires_at_micros < pg_now()`. Semantically
    /// aborted; the lifecycle sweep hasn't moved it to
    /// `abort_tx_log` yet. `expired_at_micros` is the
    /// `expires_at_micros` from `begin_tx_log` (the boundary Pg
    /// crossed when classifying expired).
    Expired { expired_at_micros: i64 },
    /// Tx has an `abort_tx_log` row. `aborted_at_micros` is the
    /// timestamp from that row (Pg-set at abort_tx insert time);
    /// `aborted_at_seq_num` (CHA-429) is the `commit_tx_log_seq_num` counter
    /// frontier captured at abort — a ledger column with no read-path
    /// consumer yet (symmetric with the begin/commit seq columns).
    Aborted {
        aborted_at_micros: i64,
        aborted_at_seq_num: i64,
    },
    /// Tx has a `commit_tx_log` row. `commit_micros` is the timestamp
    /// from that row (Pg-set at commit_tx_log insert time).
    Committed { commit_micros: i64 },
}

/// Result of a successful commit_tx_log insert (either
/// [`HotStorageClient::commit_open_tx`] or
/// [`HotStorageClient::auto_commit_tx`]).
///
/// `branch_uuid` is omitted: the caller already knows the branch
/// (it's required on every CommitTx / AbortTx request and was used
/// to address the leaf partitions in the first place).
/// `commit_micros` is set by Pg at the `commit_tx_log` INSERT;
/// `began_at_micros`, `comment`, and `author` either flow from
/// `begin_tx_log` (commit_open_tx) or were supplied by the caller
/// (auto_commit_tx) and are returned uniformly so both helpers have
/// a symmetric shape.
#[derive(Debug, Clone)]
pub struct CommittedTx {
    pub began_at_micros: i64,
    pub commit_micros: i64,
    pub comment: String,
    pub author: String,
    /// CHA-428: the monotonic, gapless commit-order serial allocated for
    /// this tx from the branch's `commit_tx_log_seq_num` counter row at commit.
    pub commit_seq_num: i64,
}

/// Builds the shared commit-INSERT into `<commit_tx_log_partition>`, allocating
/// the CHA-428 `commit_seq_num` in the same statement.
///
/// `<commit_tx_log_partition>` is the per-branch `commit_tx_log` partition;
/// `<commit_tx_log_seq_num_partition>` is the per-branch counter partition. The `c`
/// CTE increments the branch's `commit_tx_log` counter row under a row lock held
/// to transaction end (so allocation order == commit-visibility order) and
/// returns the pre-increment value as `commit_seq_num` (first commit on a fresh
/// branch is `0`). Aborts roll the counter back, keeping the axis gapless.
///
/// Callers supply `source_select` — a SELECT projecting the five base
/// columns (`tx_uuid, branch_uuid, began_at_micros, comment, author`):
/// either a literal-row SELECT (auto_commit_tx) or `SELECT … FROM
/// <begin_tx_log> WHERE …` (commit_open_tx). It is CROSS JOINed with `c`
/// here, uniformly, so neither caller has to thread the seq itself.
fn commit_tx_log_insert_sql(
    commit_tx_log_partition: &str,
    commit_tx_log_seq_num_partition: &str,
    source_select: &str,
) -> String {
    // `commit_micros` is assigned explicitly with `clock_timestamp()`
    // — evaluated when the INSERT executes, AFTER the `c` CTE's UPDATE has
    // acquired the counter-row lock (data-modifying CTEs run first). This is
    // load-bearing: the column DEFAULT uses `now()` == transaction-start time,
    // which under concurrency inverts vs seq order (a tx that started earlier
    // but acquired the lock later gets a lower `now()` but a higher seq). With
    // `clock_timestamp()` the timestamp is taken under the lock, so it is
    // *non-decreasing* in seq order. It is not strictly increasing: at
    // microsecond resolution two commits <1µs apart can tie on
    // `commit_micros` while their `commit_seq_num` still differs — so
    // `commit_seq_num` is the authoritative total order; the timestamp only has to
    // never invert against it (the bug RT2 catches).
    let commit_micros_expr = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000000)::bigint";
    format!(
        "WITH c AS ( \
            UPDATE {lsn} SET seq_num = seq_num + 1 \
            RETURNING seq_num - 1 AS commit_seq_num \
         ), \
         src AS ({source_select}) \
         INSERT INTO {tx} \
           (tx_uuid, branch_uuid, began_at_micros, commit_micros, comment, author, commit_seq_num) \
         SELECT src.tx_uuid, src.branch_uuid, src.began_at_micros, \
                {commit_micros_expr}, src.comment, src.author, c.commit_seq_num \
         FROM src CROSS JOIN c \
         RETURNING began_at_micros, commit_micros, comment, author, commit_seq_num",
        lsn = PgDialect::quote_identifier(commit_tx_log_seq_num_partition),
        tx = PgDialect::quote_identifier(commit_tx_log_partition),
    )
}

impl HotStorageClient {
    /// Classify a transaction's state in one Pg round-trip.
    ///
    /// Reads `begin_tx_log` joined to `abort_tx_log` and `commit_tx_log`,
    /// evaluates expiry against Pg's clock, and discriminates into
    /// one of four [`TxStatus`] variants (or `None` if no
    /// `begin_tx_log` row exists for `tx_uuid` on the given branch).
    ///
    /// Pg's clock is canonical for the expiry comparison — never the
    /// server's — so the boundary is centralized and consistent with
    /// how `begin_tx_log` populated `expires_at_micros` at BEGIN.
    ///
    /// The three leaf-partition args (`begin_partition`,
    /// `abort_partition`, `tx_partition`) are the per-branch leaf
    /// partitions (`get_begin_tx_log_partition` /
    /// `get_abort_tx_log_partition` / `get_commit_tx_log_partition`). All callers
    /// know the branch up-front: read RPCs (ReadData) carry it on
    /// the request, write RPCs (CommitTx / AbortTx) require it on
    /// the request — see `feedback_target_partitions_directly.md`.
    /// A tx that exists only on a different branch is therefore
    /// correctly surfaced as `None` (not in this branch's
    /// partition).
    ///
    /// `for_update=true` adds `FOR UPDATE OF b` to lock the
    /// `begin_tx_log` row for the rest of the surrounding Pg
    /// transaction. CommitTx and AbortTx both pass it: the row lock
    /// serializes them on the same `tx_uuid` so the
    /// `commit_open_tx ↔ abort_tx ↔ recommit` mutual-exclusion
    /// precondition holds even under concurrent calls. Read paths
    /// pass `false` (no modification, no lock needed).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            tx_uuid = %tx_uuid,
            for_update,
            status = tracing::field::Empty,
        ),
    )]
    pub async fn get_tx_status(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        begin_partition: &str,
        abort_partition: &str,
        tx_partition: &str,
        tx_uuid: &uuid::Uuid,
        for_update: bool,
    ) -> Result<Option<TxStatus>, HotStorageError> {
        // FOR UPDATE OF b locks only the begin_tx_log row; the
        // abort_tx_log / commit_tx_log reads stay as snapshot reads (no lock
        // there because the writers — commit_open_tx / abort_tx — both
        // serialize via this same FOR UPDATE on begin_tx_log, so any
        // row in abort_tx_log / commit_tx_log that's visible to us under our
        // lock is final).
        // `begin_partition` is already the leaf partition for the
        // caller's branch, so the `a.branch_uuid = b.branch_uuid` /
        // `t.branch_uuid = b.branch_uuid` join conditions just
        // tighten the abort_tx_log / commit_tx_log lookups to the same
        // partition.
        let lock = if for_update { " FOR UPDATE OF b" } else { "" };
        let sql = format!(
            "SELECT b.began_at_micros, b.began_at_seq_num, b.expires_at_micros, \
                    a.aborted_at_micros, a.aborted_at_seq_num, \
                    t.commit_micros, \
                    ({epoch}) AS now_micros \
             FROM {begin} b \
             LEFT JOIN {abort} a \
                 ON a.tx_uuid = b.tx_uuid AND a.branch_uuid = b.branch_uuid \
             LEFT JOIN {tx} t \
                 ON t.tx_uuid = b.tx_uuid AND t.branch_uuid = b.branch_uuid \
             WHERE b.tx_uuid = $1{lock}",
            epoch = PgDialect::microsecond_epoch(),
            begin = PgDialect::quote_identifier(begin_partition),
            abort = PgDialect::quote_identifier(abort_partition),
            tx = PgDialect::quote_identifier(tx_partition),
        );

        let row = driver
            .fetch_optional(&sql, &[SqlValue::Uuid(*tx_uuid)])
            .await?;

        let status = row.map(|r| {
            // Terminal-state checks first — these are mutually
            // exclusive by the commit_open_tx / abort_tx INSERT guards, so
            // order between them is moot, but reporting the terminal
            // state is more actionable than reporting expiry on a tx
            // that's already terminal.
            let aborted_at: Option<i64> = r.get("aborted_at_micros");
            if let Some(aborted_at_micros) = aborted_at {
                return TxStatus::Aborted {
                    aborted_at_micros,
                    aborted_at_seq_num: r.get("aborted_at_seq_num"),
                };
            }
            let commit: Option<i64> = r.get("commit_micros");
            if let Some(commit_micros) = commit {
                return TxStatus::Committed { commit_micros };
            }
            let expires_at: i64 = r.get("expires_at_micros");
            let now_micros: i64 = r.get("now_micros");
            if expires_at < now_micros {
                return TxStatus::Expired {
                    expired_at_micros: expires_at,
                };
            }
            TxStatus::Open {
                began_at_micros: r.get("began_at_micros"),
                began_at_seq_num: r.get("began_at_seq_num"),
            }
        });

        let status_str = match &status {
            None => "none",
            Some(TxStatus::Aborted { .. }) => "aborted",
            Some(TxStatus::Committed { .. }) => "committed",
            Some(TxStatus::Expired { .. }) => "expired",
            Some(TxStatus::Open { .. }) => "open",
        };
        tracing::Span::current().record("status", status_str);

        Ok(status)
    }

    /// Insert a `begin_tx_log` record.
    ///
    /// Returns `(began_at_micros, expires_at_micros)`.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            table_name = %table_name,
            tx_uuid = %tx_uuid,
            branch_uuid = %branch_uuid,
            timeout_seconds,
        ),
    )]
    pub async fn begin_tx(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        table_name: &str,
        commit_tx_log_seq_num_partition: &str,
        tx_uuid: &str,
        branch_uuid: &str,
        timeout_seconds: i64,
        comment: &str,
        author: &str,
    ) -> Result<(i64, i64), HotStorageError> {
        let epoch = PgDialect::microsecond_epoch();
        // CHA-429: capture began_at_seq_num = the per-branch commit_tx_log_seq_num
        // counter frontier (next-to-allocate) in the SAME statement as
        // began_at_micros, so the snapshot anchor (seq) and timeout anchor
        // (micros) can't drift. The counter leaf holds exactly one row; an
        // in-flight commit's increment is invisible under MVCC until it
        // commits, so this reads the last-committed frontier. O(1) — never a
        // MAX scan over commit_tx_log.
        let sql = format!(
            "INSERT INTO {begin} \
             (tx_uuid, branch_uuid, began_at_micros, began_at_seq_num, \
              expires_at_micros, comment, author) \
             VALUES ($1, $2, {epoch}, (SELECT seq_num FROM {lsn}), \
                     {epoch} + ($3 * 1000000::bigint), $4, $5) \
             RETURNING began_at_micros, expires_at_micros",
            begin = PgDialect::quote_identifier(table_name),
            lsn = PgDialect::quote_identifier(commit_tx_log_seq_num_partition),
        );

        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(tx_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::Int64(timeout_seconds),
                    SqlValue::Text(comment.to_string()),
                    SqlValue::Text(author.to_string()),
                ],
            )
            .await?;

        let began_at: i64 = rows[0].get("began_at_micros");
        let expires_at: i64 = rows[0].get("expires_at_micros");
        Ok((began_at, expires_at))
    }

    /// Insert a `commit_tx_log` record by reading `begin_tx_log` inline.
    /// Atomic in a single SQL: no Rust round-trip of `comment` /
    /// `author` / `began_at` — those flow `begin_tx_log` → `commit_tx_log`
    /// server-side via the `INSERT ... SELECT FROM` shape.
    ///
    /// Caller is expected to have called
    /// [`Self::get_tx_status`] with `for_update=true` in the same Pg
    /// transaction immediately before, which:
    ///   1. Locks the `begin_tx_log` row (`FOR UPDATE`) — guarantees
    ///      the row stays present and final until our INSERT runs.
    ///   2. Confirms the tx is in `Open` state (i.e., not Aborted /
    ///      Committed / Expired).
    ///
    /// The status check + FOR UPDATE lock are the source of truth
    /// for tx classification. With `get_tx_status` seeing all three
    /// of `begin_tx_log` / `abort_tx_log` / `commit_tx_log`, and the FOR
    /// UPDATE serializing commit_open_tx ↔ abort_tx ↔ recommit on the
    /// same `tx_uuid`, **no in-statement guards are needed here**:
    ///
    /// - `NOT EXISTS abort_tx_log` / `NOT EXISTS commit_tx_log`: redundant
    ///   against the status check (the lock makes the status final
    ///   for the duration of our Pg-tx).
    /// - `expires_at_micros >= pg_now()`: was previously kept as a
    ///   "live clock at INSERT time" check, but TTL is a soft
    ///   contract — committing a tx whose status check passed at T
    ///   but whose INSERT runs at T+ε past `expires_at_micros` is
    ///   benign (no race with sweep, no invariant violated). The
    ///   `commit_micros` may end up a few microseconds past
    ///   the original expiry in the worst case; nothing depends on
    ///   that boundary.
    ///
    /// The INSERT is therefore unconditional — driven entirely by
    /// the begin_tx_log row (locked by the caller). The
    /// `RETURNING` clause is guaranteed to return exactly one row.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            commit_tx_log_partition = %commit_tx_log_partition,
            begin_tx_log_partition = %begin_tx_log_partition,
            tx_uuid = %tx_uuid,
            commit_micros = tracing::field::Empty,
            commit_seq_num = tracing::field::Empty,
        ),
    )]
    pub async fn commit_open_tx(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        commit_tx_log_partition: &str,
        begin_tx_log_partition: &str,
        commit_tx_log_seq_num_partition: &str,
        tx_uuid: &uuid::Uuid,
    ) -> Result<CommittedTx, HotStorageError> {
        let source = format!(
            "SELECT b.tx_uuid, b.branch_uuid, b.began_at_micros, b.comment, b.author \
             FROM {begin} b \
             WHERE b.tx_uuid = $1",
            begin = PgDialect::quote_identifier(begin_tx_log_partition),
        );
        let sql = commit_tx_log_insert_sql(
            commit_tx_log_partition,
            commit_tx_log_seq_num_partition,
            &source,
        );

        let rows = driver
            .execute_params(&sql, &[SqlValue::Uuid(*tx_uuid)])
            .await?;

        let commit_micros: i64 = rows[0].get("commit_micros");
        let commit_seq_num: i64 = rows[0].get("commit_seq_num");
        tracing::Span::current().record("commit_micros", commit_micros);
        tracing::Span::current().record("commit_seq_num", commit_seq_num);

        Ok(CommittedTx {
            began_at_micros: rows[0].get("began_at_micros"),
            commit_micros,
            comment: rows[0].get("comment"),
            author: rows[0].get("author"),
            commit_seq_num,
        })
    }

    /// Insert an `abort_tx_log` row by reading `begin_tx_log` inline.
    /// Atomic in a single SQL — no Rust round-trip of branch_uuid.
    ///
    /// Returns the Pg-set `aborted_at_micros` from the inserted row,
    /// which the API layer surfaces on `AbortTxResponse`.
    ///
    /// Caller is expected to have called [`Self::get_tx_status`]
    /// with `for_update=true` in the same Pg transaction beforehand
    /// (locks the `begin_tx_log` row → serializes with concurrent
    /// `commit_open_tx`; confirms the tx is in `Open` state — i.e., not
    /// Committed / Aborted / Expired). The status check + FOR UPDATE
    /// is the single source of truth for tx classification, so the
    /// INSERT is unconditional and `RETURNING` is guaranteed to yield
    /// exactly one row.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            abort_tx_log_partition = %abort_tx_log_partition,
            begin_tx_log_partition = %begin_tx_log_partition,
            tx_uuid = %tx_uuid,
            aborted_at_micros = tracing::field::Empty,
        ),
    )]
    pub async fn abort_tx(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        abort_tx_log_partition: &str,
        begin_tx_log_partition: &str,
        abort_seq_num_partition: &str,
        tx_uuid: &uuid::Uuid,
    ) -> Result<i64, HotStorageError> {
        // CHA-444 (ADR 0027): allocate aborted_at_seq_num from the dedicated
        // abort-order counter — the abort-axis sibling of the commit counter
        // (CHA-428). The `c` CTE increments the branch's abort counter row
        // under a row lock held to transaction end (allocation order = abort
        // visibility order) and returns the pre-increment value (first abort
        // on a branch is 0). This replaces CHA-429's *sample* of the commit
        // counter frontier, which stalled between commits and let two aborts
        // share a value — a hazard for the monotone purge abort watermark `Pa`.
        let sql = format!(
            "WITH c AS ( \
                 UPDATE {asn} SET seq_num = seq_num + 1 \
                 RETURNING seq_num - 1 AS aborted_at_seq_num \
             ) \
             INSERT INTO {abort} (tx_uuid, branch_uuid, aborted_at_seq_num) \
             SELECT b.tx_uuid, b.branch_uuid, c.aborted_at_seq_num \
             FROM {begin} b CROSS JOIN c WHERE b.tx_uuid = $1 \
             RETURNING aborted_at_micros",
            abort = PgDialect::quote_identifier(abort_tx_log_partition),
            begin = PgDialect::quote_identifier(begin_tx_log_partition),
            asn = PgDialect::quote_identifier(abort_seq_num_partition),
        );

        let rows = driver
            .execute_params(&sql, &[SqlValue::Uuid(*tx_uuid)])
            .await?;

        let aborted_at_micros: i64 = rows[0].get("aborted_at_micros");
        tracing::Span::current().record("aborted_at_micros", aborted_at_micros);
        Ok(aborted_at_micros)
    }

    /// Insert an atomically-committed transaction directly into
    /// `commit_tx_log`. Skips `begin_tx_log` entirely — the operation is its
    /// own commit. Used for CHA-164 auto-commit DDL, WriteData's
    /// auto-commit branch, and branch-merge transactions, which all
    /// have the same "no in-flight phase, just write the row" shape.
    /// Both timestamps are set by the database.
    // Threads tx identity (tx_uuid/branch), the two commit-target partitions
    // (commit_tx_log + commit_tx_log_seq_num), and authorship (comment/author); same shape as
    // `begin_tx` above, which carries the same allow.
    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            commit_tx_log_partition = %commit_tx_log_partition,
            tx_uuid = %tx_uuid,
            branch_uuid = %branch_uuid,
            commit_micros = tracing::field::Empty,
            commit_seq_num = tracing::field::Empty,
        ),
    )]
    pub async fn auto_commit_tx(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        commit_tx_log_partition: &str,
        commit_tx_log_seq_num_partition: &str,
        tx_uuid: &str,
        branch_uuid: &str,
        comment: &str,
        author: &str,
    ) -> Result<CommittedTx, HotStorageError> {
        let epoch = PgDialect::microsecond_epoch();
        // Literal-row SELECT (not VALUES) so `commit_tx_log_insert_sql` can wrap it
        // in the seq-allocating CTE: the columns must be named for the
        // `SELECT src.<col>` projection. Casts pin the param types since
        // these placeholders aren't in INSERT-target position any more.
        let source = format!(
            "SELECT $1::uuid AS tx_uuid, $2::uuid AS branch_uuid, \
             {epoch} AS began_at_micros, $3::text AS comment, $4::text AS author"
        );
        let sql = commit_tx_log_insert_sql(
            commit_tx_log_partition,
            commit_tx_log_seq_num_partition,
            &source,
        );

        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::uuid_str(tx_uuid)?,
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::Text(comment.to_string()),
                    SqlValue::Text(author.to_string()),
                ],
            )
            .await?;

        let commit_micros: i64 = rows[0].get("commit_micros");
        let commit_seq_num: i64 = rows[0].get("commit_seq_num");
        tracing::Span::current().record("commit_micros", commit_micros);
        tracing::Span::current().record("commit_seq_num", commit_seq_num);

        Ok(CommittedTx {
            began_at_micros: rows[0].get("began_at_micros"),
            commit_micros,
            comment: rows[0].get("comment"),
            author: rows[0].get("author"),
            commit_seq_num,
        })
    }
}
