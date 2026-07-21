//! Branch-coordinated lifecycle helpers (CHA-168 + CHA-221): wall-clock
//! reads, persist / abort listings, by-branch metadata reads, and the
//! branch-scoped `commit_tx_log` family GC (`delete_purge_tx_log_eligible`).

use penca_core::naming;
use penca_db::dialect::DbDialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::{DbDriver, SqlValue};
use penca_merge::ReadSnapshot;
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::qi;
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Return the database server's current epoch in microseconds.
    ///
    /// All lifecycle operations source wallclock from PG so the persist
    /// watermark and the segment `commit_micros` defaults all live on a
    /// single monotone clock. Process-time (`SystemTime::now()`) can drift from
    /// the database clock, so mixing the two would let a row's
    /// `commit_micros` and a watermark disagree on ordering. Reuses the
    /// dialect's `microsecond_epoch` SQL fragment so this is the only place
    /// that knows the engine expression.
    ///
    /// 1 SQL query.
    pub async fn now_micros(driver: &impl DbDriver<Row = PgRow>) -> Result<i64> {
        let sql = format!("SELECT {} AS now_micros", PgDialect::microsecond_epoch(),);
        let rows = driver.execute(&sql).await?;
        let row = rows.first().expect("SELECT epoch returns one row");
        Ok(row.get::<i64, _>("now_micros"))
    }

    /// The default bounded read snapshot (CHA-86): `AsOfMicros` pinned to
    /// `pg_now`. Used by metadata reads that have no explicit `as_of` and
    /// no open tx — there is no unbounded read variant.
    pub async fn now_snapshot(driver: &impl DbDriver<Row = PgRow>) -> Result<ReadSnapshot> {
        Ok(ReadSnapshot::AsOfMicros(Self::now_micros(driver).await?))
    }

    /// Return `began_at_seq_num` for an open tx (present in
    /// `begin_tx_log_partition`, not yet committed or aborted).
    /// Returns `None` if the tx is unknown or already settled.
    ///
    /// Used by RYOW reads to construct
    /// [`ReadSnapshot::OpenTx`]: the snapshot bound is the tx's
    /// `began_at_seq_num` (CHA-429 moved OpenTx visibility onto the
    /// commit-order axis, `commit_seq_num < began_at_seq_num`), and the
    /// OR-clause picks up the open tx's own uncommitted writes.
    pub async fn get_open_tx_began_at_seq_num(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        tx_uuid: &Uuid,
    ) -> Result<Option<i64>> {
        let begin_part = naming::begin_tx_log_partition(catalog_uuid, branch_uuid);
        // CHA-429: OpenTx visibility pins on the commit-order axis
        // (`commit_seq_num < began_at_seq_num`), so name resolution for an open
        // tx reads the seq frontier captured at BEGIN, not the micros.
        let sql = format!(
            "SELECT began_at_seq_num FROM {begin} \
             WHERE tx_uuid = $1 \
             LIMIT 1",
            begin = qi(&begin_part),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::Uuid(*tx_uuid)])
            .await?;
        Ok(rows.first().map(|r| r.get::<i64, _>("began_at_seq_num")))
    }

    /// Return distinct `table_uuid`s touched by committed txs on
    /// `(catalog, branch)` with `commit_micros <= effective_target`.
    ///
    /// Filter `commit_tx_log_partition` first, then probe
    /// `tx_table_log_partition` via `WHERE tx_uuid IN (...)` — matches the
    /// `feedback_tx_table_log_access_pattern` convention so the
    /// access pattern is self-documenting and consistent with every
    /// other call site that reaches into `tx_table_log` (CHA-181 has
    /// `tx_uuid` leading the PK).
    ///
    /// 1 SQL query.
    pub async fn touched_table_uuids_for_persist(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        effective_target_micros: i64,
    ) -> Result<Vec<Uuid>> {
        let tx_part = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
        let tx_table_part = naming::tx_table_log_partition(catalog_uuid, branch_uuid);
        let sql = format!(
            "SELECT DISTINCT table_uuid \
             FROM {tx_table} \
             WHERE tx_uuid IN ( \
                 SELECT tx_uuid FROM {tx} \
                 WHERE commit_micros <= $1 \
             )",
            tx_table = qi(&tx_table_part),
            tx = qi(&tx_part),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::Int64(effective_target_micros)])
            .await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<Uuid, _>("table_uuid"))
            .collect())
    }

    /// Enumerate distinct `table_uuid`s touched by **committed OR
    /// aborted** txs on `(catalog, branch)` whose
    /// `commit_tx_log.commit_micros` (committed half) OR
    /// `abort_tx_log.aborted_at_micros` (aborted half) falls in the
    /// half-open `[min_micros, max_micros)` window. Either bound may
    /// be `None`; both `None` matches all settled-tx history.
    ///
    /// CHA-221 (v2.1) broadened the semantic from "committed only" to
    /// "committed OR aborted" so the scheduler's per-tick Persist phase
    /// also runs on tables touched by aborted writes. Persist owns
    /// aborted hot-row cleanup ([ADR 0021](`docs/decisions/0021-persist-owns-aborted-hot-cleanup.md`));
    /// without this listing change, aborted-only tables would never
    /// have `Persist(T)` called and their hot rows + tx-log family
    /// metadata would leak indefinitely.
    ///
    /// Ordered by `MAX(modified_at_micros) ASC, table_uuid ASC` where
    /// `modified_at_micros = committed_at OR aborted_at` per-row —
    /// least-recently-modified first, with a UUID tiebreak so paging
    /// is stable across calls. Paginated via `LIMIT/OFFSET`.
    ///
    /// Filter `commit_tx_log_partition` / `abort_tx_log_partition`
    /// first (in the union subquery), then probe
    /// `tx_table_log_partition` by its `tx_uuid` PK leading column.
    /// Same `feedback_tx_table_log_access_pattern` convention as
    /// [`Self::touched_table_uuids_for_persist`].
    ///
    /// 1 SQL query.
    pub async fn list_modified_table_uuids_paginated(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        min_micros: Option<i64>,
        max_micros: Option<i64>,
        page_size: i64,
        offset: i64,
    ) -> Result<Vec<Uuid>> {
        let tx_part = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
        let abort_part = naming::abort_tx_log_partition(catalog_uuid, branch_uuid);
        let tx_table_part = naming::tx_table_log_partition(catalog_uuid, branch_uuid);

        let mut params: Vec<SqlValue> = Vec::new();
        let mut tx_filter_clauses: Vec<String> = Vec::new();
        let mut abort_filter_clauses: Vec<String> = Vec::new();
        if let Some(min) = min_micros {
            params.push(SqlValue::Int64(min));
            let n = params.len();
            tx_filter_clauses.push(format!("commit_micros >= ${n}"));
            abort_filter_clauses.push(format!("aborted_at_micros >= ${n}"));
        }
        if let Some(max) = max_micros {
            params.push(SqlValue::Int64(max));
            let n = params.len();
            tx_filter_clauses.push(format!("commit_micros < ${n}"));
            abort_filter_clauses.push(format!("aborted_at_micros < ${n}"));
        }
        let tx_where = if tx_filter_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", tx_filter_clauses.join(" AND "))
        };
        let abort_where = if abort_filter_clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", abort_filter_clauses.join(" AND "))
        };

        params.push(SqlValue::Int64(page_size));
        let limit_param = format!("${}", params.len());
        params.push(SqlValue::Int64(offset));
        let offset_param = format!("${}", params.len());

        let sql = format!(
            "SELECT ttl.table_uuid \
             FROM {tx_table} ttl \
             JOIN ( \
                 SELECT tx_uuid, commit_micros AS modified_at_micros FROM {tx}{tx_where} \
                 UNION ALL \
                 SELECT tx_uuid, aborted_at_micros AS modified_at_micros FROM {abort}{abort_where} \
             ) tx ON tx.tx_uuid = ttl.tx_uuid \
             GROUP BY ttl.table_uuid \
             ORDER BY MAX(tx.modified_at_micros) ASC, ttl.table_uuid ASC \
             LIMIT {limit_param} OFFSET {offset_param}",
            tx_table = qi(&tx_table_part),
            tx = qi(&tx_part),
            abort = qi(&abort_part),
        );
        let rows = driver.execute_params(&sql, &params).await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<Uuid, _>("table_uuid"))
            .collect())
    }

    /// Enumerate distinct `table_uuid`s on `(catalog_uuid, branch_uuid)`
    /// whose `table_persist_metadata.commit_micros` falls in the
    /// half-open `[min_micros, max_micros)` window. The
    /// `commit_micros IS NOT NULL` filter structurally excludes
    /// uncommitted persists (phase-1 rows that crashed before phase-2
    /// flip).
    ///
    /// Ordered by `MAX(commit_micros) ASC, table_uuid ASC`
    /// (least-recently-persisted first, UUID tiebreak for stable
    /// paging). Paginated via `LIMIT/OFFSET` — `page_size` and `offset`
    /// are caller-resolved (see `pagination::pagination_from_request`).
    ///
    /// Partition-pruned by `branch_uuid` (parent is LIST-partitioned by
    /// `branch_uuid` per CHA-220) — the scan is bounded to a single
    /// branch's partition slice. Within that slice the planner currently
    /// has no supporting index for the `commit_micros` window so
    /// it sequential-scans the partition. Acceptable today because
    /// `table_persist_metadata` is one row per `(table, persist)` and
    /// the scheduler tick window is small; if cardinality grows past
    /// that, add a `(branch_uuid, commit_micros)` index.
    ///
    /// 1 SQL query.
    pub async fn list_persisted_table_uuids_paginated(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        min_micros: Option<i64>,
        max_micros: Option<i64>,
        page_size: i64,
        offset: i64,
    ) -> Result<Vec<Uuid>> {
        let table = naming::table_persist_metadata_table(catalog_uuid);

        let mut params: Vec<SqlValue> = Vec::new();
        params.push(SqlValue::Uuid(*branch_uuid));
        let mut where_clauses: Vec<String> = vec![
            "branch_uuid = $1".to_string(),
            "commit_micros IS NOT NULL".to_string(),
        ];
        if let Some(min) = min_micros {
            params.push(SqlValue::Int64(min));
            where_clauses.push(format!("commit_micros >= ${}", params.len()));
        }
        if let Some(max) = max_micros {
            params.push(SqlValue::Int64(max));
            where_clauses.push(format!("commit_micros < ${}", params.len()));
        }

        params.push(SqlValue::Int64(page_size));
        let limit_param = format!("${}", params.len());
        params.push(SqlValue::Int64(offset));
        let offset_param = format!("${}", params.len());

        let sql = format!(
            "SELECT table_uuid FROM {table} \
             WHERE {where_clauses} \
             GROUP BY table_uuid \
             ORDER BY MAX(commit_micros) ASC, table_uuid ASC \
             LIMIT {limit_param} OFFSET {offset_param}",
            table = qi(&table),
            where_clauses = where_clauses.join(" AND "),
        );
        let rows = driver.execute_params(&sql, &params).await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<Uuid, _>("table_uuid"))
            .collect())
    }

    // ── CHA-221 branch-scoped tx-log family GC ───────────────────────

    /// Read S = distinct tables in `tx_table_log[B]` whose writer tx is
    /// **settled by the snapshot** (committed in `commit_tx_log[B]` with
    /// `commit_micros <= cleanup_started_at` OR aborted in
    /// `abort_tx_log[B]` with `aborted_at_micros <=
    /// cleanup_started_at`), left-joined with each table's stored seq
    /// watermarks `MAX(last_purged_commit_seq_num)` (`Pu`) and
    /// `MAX(last_purged_aborted_seq_num)` (`Pa`) from
    /// `table_purge_metadata`, **as of `cleanup_started_at_micros`**
    /// (CHA-444 / ADR 0027). One round-trip; no per-table queries.
    ///
    /// Why the settled-tx filter on `tx_table_log` entries: an
    /// in-flight open tx that has mutated table T inserts a
    /// `tx_table_log[B]` row at mutate time, before any commit/abort
    /// decision. Without this filter, T would be in S unpurged on the
    /// committed axis (an open tx has no `commit_seq_num`, so Purge can't have
    /// advanced `Pu(T)` over it), forcing `MIN(Pu over S)` to `None` and
    /// starving PurgeTxLog of committed progress until the open tx settles.
    /// Restricting S to tables touched by *settled* txs lets the cutoffs
    /// advance over in-flight writers on unrelated tables.
    ///
    /// Per-row `Pu` / `Pa` is `None` when the table has no committed
    /// `table_purge_metadata` row (phase-2 `commit_micros <=
    /// cleanup_started_at_micros`) that advanced that axis — either the
    /// table has never been Purged on it, or every such Purge committed
    /// *after* the cleanup pass started. A `None` on an axis **blocks** GC
    /// on it: `compute_purge_tx_log_cutoffs` takes `MIN over S` via
    /// `branch_min_watermark`, which drops the whole cutoff to `None` if
    /// any table in S is unpurged on that axis (an unpurged table may still
    /// hold commit_tx_log rows the GC must not drop). This is the
    /// strongest-constraint / block-on-`None` rule — *not* a `None → 0`
    /// substitution.
    ///
    /// CHA-221 v2.1 / CHA-444: the `commit_micros <= $cleanup_started_at`
    /// filter on `table_purge_metadata` is **load-bearing**. It pins
    /// the per-table `Pu` / `Pa` view to what was visible at the moment
    /// `PurgeTxLog` captured its `cleanup_started_at_micros`,
    /// independent of any concurrent `Purge(T)` whose phase-2 commits
    /// during `PurgeTxLog`'s SQL execution. Without this filter, a
    /// concurrent Purge advancing `Pu(T)` mid-pass could let
    /// `MIN(Pu over S)` jump past a tx that committed after
    /// `cleanup_started_at`, breaking the safety chain. See ADR 0021 /
    /// CHA-221 §"Long-cleanup-race". The same `cleanup_started_at`
    /// bound is reused on the `commit_tx_log` / `abort_tx_log` half of the
    /// S filter so both halves see a consistent snapshot.
    ///
    /// Reads the stored seq watermark columns (`last_purged_commit_seq_num` /
    /// `last_purged_aborted_seq_num`) directly. MUST NOT derive a
    /// substitute from `table_persist_metadata.persisted_at_micros`
    /// (ADR 0019 §"Reading the watermark", carried forward by ADR 0027) —
    /// the substitution looks equivalent but would let the GC advance past
    /// hot rows Purge has not yet cleared.
    ///
    /// 1 SQL query.
    pub async fn tx_table_log_purge_watermarks_for_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        cleanup_started_at_micros: i64,
    ) -> Result<Vec<(Uuid, Option<i64>, Option<i64>)>> {
        let tx_table_part = naming::tx_table_log_partition(catalog_uuid, branch_uuid);
        let commit_tx_log_part = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
        let abort_part = naming::abort_tx_log_partition(catalog_uuid, branch_uuid);
        let purge_table = naming::table_purge_metadata_table(catalog_uuid);
        // CHA-444 (ADR 0027): per-table seq watermarks `Pu`
        // (`last_purged_commit_seq_num`) and `Pa` (`last_purged_aborted_seq_num`),
        // as-of `cleanup_started_at` (the committed_at filter pins the view
        // against a concurrent Purge mid-pass — CHA-221 §Long-cleanup-race).
        let sql = format!(
            "SELECT t.table_uuid, p.pu AS pu, p.pa AS pa \
             FROM ( \
                 SELECT DISTINCT tt.table_uuid \
                 FROM {tx_table} tt \
                 INNER JOIN ( \
                     SELECT tx_uuid FROM {commit_tx_log} \
                     WHERE commit_micros <= $2 \
                     UNION ALL \
                     SELECT tx_uuid FROM {abort} \
                     WHERE aborted_at_micros <= $2 \
                 ) s ON s.tx_uuid = tt.tx_uuid \
             ) t \
             LEFT JOIN ( \
                 SELECT table_uuid, \
                        MAX(last_purged_commit_seq_num) AS pu, \
                        MAX(last_purged_aborted_seq_num) AS pa \
                 FROM {purge} \
                 WHERE branch_uuid = $1 \
                   AND commit_micros IS NOT NULL \
                   AND commit_micros <= $2 \
                 GROUP BY table_uuid \
             ) p ON p.table_uuid = t.table_uuid",
            tx_table = qi(&tx_table_part),
            commit_tx_log = qi(&commit_tx_log_part),
            abort = qi(&abort_part),
            purge = qi(&purge_table),
        );
        let rows = driver
            .execute_params(
                &sql,
                &[
                    SqlValue::Uuid(*branch_uuid),
                    SqlValue::Int64(cleanup_started_at_micros),
                ],
            )
            .await?;
        rows.iter()
            .map(|r| -> Result<(Uuid, Option<i64>, Option<i64>)> {
                Ok((
                    r.try_get::<Uuid, _>("table_uuid")?,
                    r.try_get::<Option<i64>, _>("pu")?,
                    r.try_get::<Option<i64>, _>("pa")?,
                ))
            })
            .collect()
    }

    /// CHA-221 / CHA-444's branch-scoped tx-log family DELETE — one SQL
    /// statement that GCs `commit_tx_log[B]`, `tx_table_log[B]`, `abort_tx_log[B]`,
    /// and `begin_tx_log[B]` for one fixed eligibility set. ADR 0027 re-axises
    /// the eligibility onto the purge seq watermarks; `eligible` is the union
    /// of four disjoint branches:
    ///
    /// 1. **Committed** — `commit_seq_num <= pu_cutoff` (`MIN(Pu over S)`). By the
    ///    branch-min, every active table has purged this tx's committed hot
    ///    rows, so its commit_tx_log row is GC-safe.
    /// 2. **Aborted-with-writes** — `aborted_at_seq_num < pa_cutoff`
    ///    (`MIN(Pa over S)`). Purge cleared the aborted hot rows in every
    ///    active table up to `Pa`.
    /// 3. **Pure begin+abort** (no `tx_table_log`) — no hot rows / no table
    ///    watermark dependency; GC-safe once the abort is in our snapshot
    ///    (`aborted_at_micros <= cleanup_started_at`).
    /// 4. **Expired-begin** — timed out, never committed/aborted;
    ///    `expires_at_micros < cleanup_started_at - expiry_grace`. Expiry is
    ///    intrinsically wall-clock (no monotone seq), and a grace `>=` one
    ///    sweep interval guarantees Purge has re-swept every table it wrote to,
    ///    so its hot rows are gone everywhere (ADR 0027 §5).
    ///
    /// The seq cutoffs are bounded to the statement view by the as-of filter on
    /// `table_purge_metadata.committed_at` in the watermarks read that feeds
    /// `compute_purge_tx_log_cutoffs` (CHA-221 §Long-cleanup-race). In-flight
    /// open txs are in none of the four branches, so their `begin_tx_log` /
    /// `tx_table_log` state is preserved.
    ///
    /// PG's `WITH ... DELETE` semantics: every sub-statement sees
    /// the same snapshot of the input tables (per the docs), so
    /// the four deletes all match against the same pre-statement
    /// `eligible` set. Atomic — succeeds together or not at all.
    ///
    /// 1 SQL statement.
    pub async fn delete_purge_tx_log_eligible(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        pu_cutoff: Option<i64>,
        pa_cutoff: Option<i64>,
        cleanup_started_at_micros: i64,
        expiry_grace_micros: i64,
    ) -> Result<()> {
        let commit_tx_log = qi(&naming::commit_tx_log_partition(catalog_uuid, branch_uuid));
        let abort = qi(&naming::abort_tx_log_partition(catalog_uuid, branch_uuid));
        let tx_table = qi(&naming::tx_table_log_partition(catalog_uuid, branch_uuid));
        let begin = qi(&naming::begin_tx_log_partition(catalog_uuid, branch_uuid));
        let expiry_bound = cleanup_started_at_micros.saturating_sub(expiry_grace_micros);

        // CHA-444 (ADR 0027): four disjoint eligibility branches. The cutoffs
        // are system-computed i64s, inlined directly (no injection surface).
        let mut branches: Vec<String> = Vec::new();
        // 1. Committed, purged from hot in every active table: commit_seq_num <= Pu.
        if let Some(pu) = pu_cutoff {
            branches.push(format!(
                "SELECT tx_uuid FROM {commit_tx_log} WHERE commit_seq_num <= {pu}"
            ));
        }
        // 2. Aborted-with-writes, aborted hot rows cleared everywhere:
        //    aborted_at_seq_num < Pa (Pa = the abort-frontier Purge cleaned to).
        if let Some(pa) = pa_cutoff {
            branches.push(format!(
                "SELECT tx_uuid FROM {abort} WHERE aborted_at_seq_num < {pa}"
            ));
        }
        // 3. Pure begin+abort (no writes): no hot rows / no table watermark
        //    dependency — GC-safe once the abort is in our snapshot.
        branches.push(format!(
            "SELECT a.tx_uuid FROM {abort} a \
             WHERE a.aborted_at_micros <= {cleanup} \
               AND NOT EXISTS (SELECT 1 FROM {tx_table} t WHERE t.tx_uuid = a.tx_uuid)",
            cleanup = cleanup_started_at_micros,
        ));
        // 4. Expired-begin (timed out, never committed/aborted): wall-clock
        //    grace (expiry is intrinsically wall-clock; there is no monotone
        //    seq for it). `expires_at < cleanup_started_at - grace` with grace
        //    >= one sweep interval ⇒ Purge has re-swept every table it wrote
        //    to, so its hot rows are gone everywhere.
        branches.push(format!(
            "SELECT b.tx_uuid FROM {begin} b \
             WHERE b.expires_at_micros < {expiry_bound} \
               AND NOT EXISTS (SELECT 1 FROM {commit_tx_log} t WHERE t.tx_uuid = b.tx_uuid) \
               AND NOT EXISTS (SELECT 1 FROM {abort} a WHERE a.tx_uuid = b.tx_uuid)"
        ));

        let eligible = branches.join(" UNION ALL ");
        let sql = format!(
            "WITH eligible AS ({eligible}), \
             d_commit_tx_log AS ( \
                 DELETE FROM {commit_tx_log} WHERE tx_uuid IN (SELECT tx_uuid FROM eligible) \
             ), \
             d_tx_table AS ( \
                 DELETE FROM {tx_table} WHERE tx_uuid IN (SELECT tx_uuid FROM eligible) \
             ), \
             d_abort AS ( \
                 DELETE FROM {abort} WHERE tx_uuid IN (SELECT tx_uuid FROM eligible) \
             ) \
             DELETE FROM {begin} WHERE tx_uuid IN (SELECT tx_uuid FROM eligible)"
        );
        driver.execute_no_result(&sql).await?;
        Ok(())
    }
}
