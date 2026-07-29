//! Shared SQL builders for merge-on-read queries.
//!
//! The same logical SQL runs against two engines — Postgres (the hot tier)
//! and DataFusion (the cold tier). Keeping the builder in one place is the
//! forcing function that keeps hot and cold tiers in sync: the SQL string
//! *is* the contract between them. The only dialect-specific construct is
//! "latest row per partition" (`DISTINCT ON` in Postgres, `ROW_NUMBER()`
//! window in DataFusion), which is delegated to
//! [`Dialect::latest_per_partition`].

use penca_sql::{
    CompositeMergeResolution, Dialect, build_composite_merge_resolution, leading_comma_if_nonempty,
    qualify_user_cols, row_uuid_in_clause, row_uuid_in_clause_after,
};
use uuid::Uuid;

use crate::ReadSnapshot;

/// Build the **Query A** SQL that resolves the committed upsert state for a
/// data table, joined with its transaction log.
///
/// Output columns: `row_uuid, <user_cols>, commit_micros`.
///
/// The query:
/// 1. Picks committed transactions, optionally clipped by
///    `tier_seq_lower` (the hot↔cold tier fence — hot serves
///    `commit_seq_num > W_persist`, skipping cold-already-persisted rows) and
///    `snapshot` (which governs the upper visibility bound and whether the
///    open tx's own uncommitted writes are included — see [`ReadSnapshot`]).
/// 2. Reads every upsert from `upsert_log`.
/// 3. Picks the latest committed upsert per `row_uuid`.
/// 4. Drops any row whose deletion tombstone is newer than its upsert.
///
/// `upsert_log`, `delete_log`, and `commit_tx_log` are bare (unquoted) table or
/// CTE names — the builder applies [`Dialect::quote_identifier`] to each.
/// User column names are also unquoted on input.
///
/// The resolve returns the latest committed version per `row_uuid`
/// across BOTH logs — a `UNION ALL` of visible upserts (`is_delete = false`,
/// user cols valued) and winning tombstones (`is_delete = true`, user cols
/// NULL). The full `row_uuid` set of the output IS the exclusion set.
/// No user filter is spliced here — DataFusion applies the residual once,
/// after cross-tier dedup (`apply_resolved_residual`).
///
/// `row_uuids` is the point-lookup restriction: when present, the per-log
/// source subqueries are restricted to those `row_uuid`s **below** the
/// latest-wins dedup, so only the named rows' versions are deduped —
/// O(table) → O(versions-of-named-rows).
///
/// [`Dialect::quote_identifier`]: penca_sql::Dialect::quote_identifier
pub fn build_merge_resolved<D: Dialect>(
    upsert_log: &str,
    delete_log: &str,
    commit_tx_log: &str,
    user_cols: &[&str],
    tier_seq_lower: Option<i64>,
    snapshot: &ReadSnapshot,
    row_uuids: Option<&[Uuid]>,
) -> String {
    let upsert_log_q = D::quote_identifier(upsert_log);
    let delete_log_q = D::quote_identifier(delete_log);
    let commit_tx_log_q = D::quote_identifier(commit_tx_log);

    let user_cols_u = qualify_user_cols::<D>("u", user_cols);
    let user_cols_l = qualify_user_cols::<D>("l", user_cols);
    let user_cols_leading = leading_comma_if_nonempty(user_cols);

    let as_of_filter = read_snapshot_clause(tier_seq_lower, snapshot);
    let open_tx_union = open_tx_union_clause::<D>(snapshot, true);

    // Step 1 (hot-tier-specific): build canonical-shape source SQLs.
    // Hot joins upsert/delete logs to `committed_tx` to recover the
    // commit timestamp; `write_seq_num` is per-row on the log.
    // The row_uuid restriction sits inside each source — below the
    // DISTINCT ON — so the dedup only touches the named rows' versions
    // (and the (row_uuid, tx_uuid) index can serve the probe).
    let ids_u = row_uuid_in_clause::<D>(row_uuids, " WHERE ", "u.");
    let ids_d = row_uuid_in_clause::<D>(row_uuids, " WHERE ", "d.");
    let upsert_source = format!(
        "(SELECT u.row_uuid{user_cols_leading}{user_cols_u}, \
                c.commit_micros, u.write_seq_num, c.commit_seq_num \
         FROM {upsert_log_q} u JOIN committed_tx c USING (tx_uuid){ids_u}) _u"
    );
    let delete_source = format!(
        "(SELECT d.row_uuid, c.commit_micros, d.write_seq_num, c.commit_seq_num \
         FROM {delete_log_q} d JOIN committed_tx c USING (tx_uuid){ids_d}) _d"
    );

    // Step 2 (shared): build the latest + deletes CTEs and the
    // composite-tiebreaker tombstone-shadow predicate, ordered on the
    // commit-order serial (`commit_seq_num`) — the authoritative total
    // order — with `write_seq_num` as the within-tx secondary.
    let composite = build_composite_merge_resolution::<D>(
        &upsert_source,
        &delete_source,
        user_cols,
        "commit_seq_num",
    );

    // Step 3 (hot-tier-specific): assemble the hot CTE list — `committed_tx`
    // (recovers each row's commit timestamp) plus the shared latest/deletes CTEs
    // — and splice it into the two-arm final SELECT.
    let cte_list = format!(
        "committed_tx AS (\
             SELECT tx_uuid, commit_micros, commit_seq_num FROM {commit_tx_log_q}{as_of_filter}\
             {open_tx_union}\
         ), {latest_cte}, {deletes_cte}",
        latest_cte = composite.latest_cte,
        deletes_cte = composite.deletes_cte,
    );
    two_arm_resolve_select(&cte_list, &composite, user_cols_leading, &user_cols_l)
}

/// Cold-tier variant of [`build_merge_resolved`].
///
/// Each cold upsert/delete row already carries `commit_micros`
/// inline (denormalized at persist time), so the cold side never JOINs
/// against a commit_tx_log table — cold has no commit_tx_log table at all.
///
/// The hot↔cold tier upper fence rides `commit_seq_num`, not `committed_at`.
/// Cold serves `commit_seq_num <= W_persist`; the caller folds `W_persist`
/// (from `PersistPlan.commit_seq.max_seq`) into `commit_seq_upper` as
/// `min(W_persist, as_of_seq)` before calling here, so this builder keeps a
/// single seq upper bound. `committed_at` carries only the `AsOfMicros`
/// visibility cap (`committed_to = as_of + 1`, inert `i64::MAX` on seq/OpenTx
/// reads) plus — on the snapshot-WRITE path, where `commit_seq` is absent —
/// the full `[from, to)` construction window. There is no per-row cold lower
/// bound: the segment fetch and the snapshot exclusion anti-join own the
/// baseline overlap. OpenTx `< began_at` rides the `commit_seq` upper bound
/// (`began_at_seq_num - 1`, threaded in by the caller).
///
/// Output columns: `row_uuid, <user_cols>, commit_micros, is_delete` — the
/// same two-arm (upsert / tombstone) shape as [`build_merge_resolved`]; see
/// that builder for the rationale. No user filter is spliced.
pub(crate) fn build_cold_merge_resolved<D: Dialect>(
    upsert_log: &str,
    delete_log: &str,
    user_cols: &[&str],
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    commit_seq_upper: Option<i64>,
    row_uuids: Option<&[Uuid]>,
) -> String {
    let upsert_log_q = D::quote_identifier(upsert_log);
    let delete_log_q = D::quote_identifier(delete_log);

    let user_cols_u = qualify_user_cols::<D>("u", user_cols);
    let user_cols_l = qualify_user_cols::<D>("l", user_cols);
    let user_cols_leading = leading_comma_if_nonempty(user_cols);
    let committed_at_filter_u =
        cold_visibility_clause(committed_from, committed_to, commit_seq_upper, None);
    let committed_at_filter_d =
        cold_visibility_clause(committed_from, committed_to, commit_seq_upper, Some("d"));

    // The row_uuid restriction AND-composes with the committed_at window when
    // one is present, else opens the WHERE.
    let ids_u = row_uuid_in_clause_after::<D>(row_uuids, &committed_at_filter_u, "u.");
    let ids_d = row_uuid_in_clause_after::<D>(row_uuids, &committed_at_filter_d, "d.");

    // Step 1 (cold-tier-specific): cold rows carry `commit_micros`
    // and `write_seq_num` inline (persist projects them through), so
    // the canonical source SQL reads them directly without a commit_tx_log JOIN.
    // The committed_at window filter applies before the per-row_uuid
    // pick in step 2; the row_uuid restriction sits in the same sources,
    // below the dedup.
    let upsert_source = format!(
        "(SELECT u.row_uuid{user_cols_leading}{user_cols_u}, \
                u.commit_micros, u.write_seq_num, u.commit_seq_num \
         FROM {upsert_log_q} u{committed_at_filter_u}{ids_u}) _u"
    );
    let delete_source = format!(
        "(SELECT d.row_uuid, d.commit_micros, d.write_seq_num, d.commit_seq_num \
         FROM {delete_log_q} d{committed_at_filter_d}{ids_d}) _d"
    );

    // Step 2 (shared): latest + deletes CTEs + tombstone-shadow, ordered on
    // the commit-order serial.
    let composite = build_composite_merge_resolution::<D>(
        &upsert_source,
        &delete_source,
        user_cols,
        "commit_seq_num",
    );

    // Step 3 (cold-tier-specific): cold has no commit_tx_log, so the CTE list is
    // just latest/deletes — splice it into the shared two-arm final SELECT;
    // see `build_merge_resolved` for the upsert/tombstone shape.
    let cte_list = format!(
        "{latest_cte}, {deletes_cte}",
        latest_cte = composite.latest_cte,
        deletes_cte = composite.deletes_cte,
    );
    two_arm_resolve_select(&cte_list, &composite, user_cols_leading, &user_cols_l)
}

/// Splice a tier's assembled CTE list into the shared two-arm final SELECT.
/// Arm 1 = the visible upsert per `row_uuid` (`is_delete = false`).
/// Arm 2 = the winning tombstone per `row_uuid` (`is_delete = true`); its user
/// cols come from the LEFT-JOINed `latest` (NULL for a delete-only `row_uuid`) so
/// the UNION type-matches arm 1 exactly — the values are never emitted
/// (`is_delete` rows are dropped downstream), only the `row_uuid` feeds the
/// exclusion set. The upsert-visible / delete-visible predicates are mutually
/// exclusive and exhaustive per `row_uuid`, so each touched `row_uuid` appears
/// exactly once.
///
/// `cte_list` is the full `WITH` body the tier builder assembled: hot prefixes
/// its `committed_tx` CTE, cold has only `latest`/`deletes`. Keeping the two-arm
/// shape here (not per builder) is the same forcing function the module doc
/// states — hot and cold stay in sync because the emitted SELECT is one string.
fn two_arm_resolve_select(
    cte_list: &str,
    composite: &CompositeMergeResolution,
    user_cols_leading: &str,
    user_cols_l: &str,
) -> String {
    format!(
        "WITH {cte_list} \
         SELECT l.row_uuid{user_cols_leading}{user_cols_l}, l.commit_micros, false AS is_delete \
         FROM latest l LEFT JOIN deletes d ON l.row_uuid = d.row_uuid \
         WHERE {upsert_visible} \
         UNION ALL \
         SELECT d.row_uuid{user_cols_leading}{user_cols_l}, d.commit_micros, true AS is_delete \
         FROM deletes d LEFT JOIN latest l ON d.row_uuid = l.row_uuid \
         WHERE {delete_visible}",
        upsert_visible = composite.upsert_visible_predicate,
        delete_visible = composite.delete_visible_predicate,
    )
}

/// Cold-tier snapshot scan: read snapshot segments through a registered
/// `SnapshotTableProvider` (aliased `l`) and express the exclusion-set
/// anti-join + residual filter in the plan.
///
/// `snapshot_table` / `exclusion_table` are the names the snapshot
/// `SessionContext` registers — the `SnapshotTableProvider` and a
/// single-column `row_uuid` exclusion `MemTable`. Output columns:
/// `row_uuid, <user_cols>`.
///
/// Invariant: the user `filter` is appended ONLY at the outer
/// `WHERE`; the `NOT IN (SELECT row_uuid FROM exclusion)` subquery stays
/// unfiltered — the exclusion set was built from the unfiltered composed
/// resolve upstream (the full row_uuid set of [`build_merge_resolved`] /
/// [`build_cold_merge_resolved`]).
pub(crate) fn build_cold_snapshot_scan<D: Dialect>(
    snapshot_table: &str,
    exclusion_table: &str,
    user_cols: &[&str],
    filter: Option<&str>,
    row_uuids: Option<&[Uuid]>,
) -> String {
    let snapshot_q = D::quote_identifier(snapshot_table);
    let exclusion_q = D::quote_identifier(exclusion_table);
    let user_cols_l = qualify_user_cols::<D>("l", user_cols);
    let user_cols_leading = leading_comma_if_nonempty(user_cols);

    // The user predicate is appended ONLY here, at the outer WHERE;
    // the exclusion anti-join subquery stays unfiltered.
    let user_filter = match filter {
        Some(f) if !f.is_empty() => format!(" AND ({f})"),
        _ => String::new(),
    };
    // Restricting the snapshot scan to the named rows is exact — row identity
    // IS row_uuid-from-PK, so no post-scan re-check.
    // PAIRING INVARIANT: must carry the SAME restriction as the
    // exclusion-set builders for this read — restricting exclusion but
    // not the scan would leak shadowed snapshot versions of
    // unrestricted rows. stream_merged threads one MergeReadRequest value
    // to both, which is what guarantees it.
    let ids = row_uuid_in_clause::<D>(row_uuids, " AND ", "l.");

    format!(
        "SELECT l.row_uuid{user_cols_leading}{user_cols_l} \
         FROM {snapshot_q} l \
         WHERE l.row_uuid NOT IN (SELECT row_uuid FROM {exclusion_q}){ids}{user_filter}"
    )
}

/// Cold-tier snapshot scan WITHOUT the exclusion anti-join — the
/// `ByPlan` (snapshot writer) variant of
/// [`build_cold_snapshot_scan`]. The in-plan `NOT IN` decorrelates to a
/// `CollectLeft` `LeftAnti` hash join that BUILDS on the snapshot side:
/// the entire prior snapshot materializes into the hash table and rows
/// come back in hash order — defeating both the streaming memory bound
/// and plan-order delivery. The ByPlan consumer applies the exclusion
/// set per batch instead (`stream_merged_parts`); the exclusion was built
/// from the unfiltered logs upstream either way.
pub(crate) fn build_cold_snapshot_scan_plain<D: Dialect>(
    snapshot_table: &str,
    user_cols: &[&str],
    filter: Option<&str>,
) -> String {
    let snapshot_q = D::quote_identifier(snapshot_table);
    let user_cols_l = qualify_user_cols::<D>("l", user_cols);
    let user_cols_leading = leading_comma_if_nonempty(user_cols);
    let user_filter = match filter {
        Some(f) if !f.is_empty() => format!(" WHERE ({f})"),
        _ => String::new(),
    };
    format!("SELECT l.row_uuid{user_cols_leading}{user_cols_l} FROM {snapshot_q} l{user_filter}")
}

/// Cold-tier visibility predicate. ANDs the half-open `commit_micros`
/// window with the optional `commit_seq_num <= seq` upper bound. Returns a
/// full `WHERE ...` prefix (with a leading space) when any bound is set, or
/// empty otherwise. Half-open on committed_at: `[min, max)`.
///
/// `alias = Some("d")` emits `d.commit_micros` / `d.commit_seq_num`;
/// `alias = None` emits bare columns. The aliased form is needed when
/// the predicate sits in a sub-clause where the bare column would be
/// ambiguous.
///
/// `commit_seq_upper` carries the **folded** seq upper `min(W_persist,
/// as_of_seq)` — both the hot↔cold tier fence (cold serves `commit_seq_num
/// <= W_persist`) and the `AsOfSeq` visibility cap. On the snapshot-WRITE
/// path (`commit_seq` absent on the plan) the tier fence isn't folded in and
/// `committed_at`'s `[from, to)` window stands alone. `committed_from` is
/// `None` on every read-path call (the cold read has no per-row lower bound);
/// it is `Some` only on the write-path construction window. OpenTx passes
/// `commit_seq_upper = began_at_seq_num - 1`, so cold serves only
/// `commit_seq_num < began_at_seq_num`.
fn cold_visibility_clause(
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    commit_seq_upper: Option<i64>,
    alias: Option<&str>,
) -> String {
    let col = |c: &str| match alias {
        Some(a) => format!("{a}.{c}"),
        None => c.to_string(),
    };
    let mut parts: Vec<String> = Vec::new();
    if let Some(min) = committed_from {
        parts.push(format!("{} >= {min}", col("commit_micros")));
    }
    if let Some(max) = committed_to {
        parts.push(format!("{} < {max}", col("commit_micros")));
    }
    if let Some(seq) = commit_seq_upper {
        parts.push(format!("{} <= {seq}", col("commit_seq_num")));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", parts.join(" AND "))
    }
}

/// Build the visibility WHERE clause for the `committed_tx` CTE.
///
/// Returns a full ` WHERE …` prefix (with leading space) when any
/// bound is set, or empty otherwise. The hot `commit_tx_log` table contains
/// only committed rows by construction (commits are inserted at
/// `CommitTx` time; aborts go to `abort_tx_log`), so no
/// "is-committed" sentinel predicate is needed against
/// `commit_micros`.
///
/// `tier_seq_lower` is the hot↔cold tier fence on the gapless commit-order
/// serial — hot serves `commit_seq_num > W_persist`. The seq partition is
/// exact at `W_persist`, with no same-microsecond ambiguity. `None`
/// pre-Persist (hot owns every row). Composed with the axis-aware as-of
/// bound below.
fn read_snapshot_clause(tier_seq_lower: Option<i64>, snapshot: &ReadSnapshot) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(w_persist) = tier_seq_lower {
        parts.push(format!("commit_seq_num > {w_persist}"));
    }
    match snapshot {
        ReadSnapshot::AsOfMicros(ts) => {
            parts.push(format!("commit_micros <= {ts}"));
        }
        ReadSnapshot::AsOfSeq(seq) | ReadSnapshot::LatestSeq(seq) => {
            // Visibility on the commit-order axis — exact, no committed_at tie
            // ambiguity. A default-latest read (`LatestSeq`) pins the same seq
            // predicate as an explicit `AsOfSeq`.
            parts.push(format!("commit_seq_num <= {seq}"));
        }
        ReadSnapshot::OpenTx {
            began_at_seq_num, ..
        } => {
            // RYOW snapshot isolation on the seq axis: include only txs
            // committed strictly before this tx's begin frontier. The tx's
            // own writes are unioned in via `open_tx_union_clause` so they
            // aren't gated on this committed-only filter.
            parts.push(format!("commit_seq_num < {began_at_seq_num}"));
        }
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", parts.join(" AND "))
    }
}

/// For `OpenTx`, emit a `UNION ALL` row that injects the open tx as a
/// synthetic `committed_tx` entry — `tx_uuid` matches the open tx, with
/// `commit_micros` and `commit_seq_num` both `i64::MAX` (when
/// `include_committed_at = true`; the exclusion-set CTE elides those
/// columns and passes `false`). The merge orders by
/// `commit_seq_num`, so the synthetic row carries `i64::MAX` on the seq axis
/// — higher than any real committed seq — which makes the tx's own
/// uncommitted writes win the latest-version-per-row_uuid race in
/// `latest_per_partition` and dominate any committed delete tombstone in
/// the deletes JOIN. (`commit_micros` is also `i64::MAX` so the
/// own-writes rows likewise win any cross-tier committed_at dedup; there
/// is no cold own-writes counterpart, so this is harmless.)
///
/// **Why a literal instead of `UNION ALL ... FROM begin_tx_log WHERE
/// tx_uuid = ...`?** The cold tier has no `begin_tx_log` table
/// registered in its DataFusion context — only `upsert_log`,
/// `delete_log`, and `commit_tx_log`. The shared SQL builder is generic over
/// `Dialect` and emits the *same* logical SQL to both tiers; the
/// moment we reference `begin_tx_log` we'd have to fork hot/cold
/// emission. The synthetic row's columns are constants, so the literal
/// expansion is free — no extra round-trip, same SQL on both tiers.
///
/// Cold tier never matches: only committed txs are persisted to cold, so
/// the synthetic tx_uuid joins to no upsert/delete rows there. Harmless
/// extra row in the CTE.
///
/// `AsOfMicros` / `AsOfSeq` emit nothing (no own-writes to inject).
fn open_tx_union_clause<D: Dialect>(snapshot: &ReadSnapshot, include_committed_at: bool) -> String {
    match snapshot {
        ReadSnapshot::AsOfMicros(_) | ReadSnapshot::AsOfSeq(_) | ReadSnapshot::LatestSeq(_) => {
            String::new()
        }
        ReadSnapshot::OpenTx { tx_uuid, .. } => {
            let uuid_lit = D::uuid_literal(tx_uuid);
            let cols_suffix = if include_committed_at {
                format!(
                    ", {max} AS commit_micros, {max} AS commit_seq_num",
                    max = i64::MAX
                )
            } else {
                String::new()
            };
            format!(" UNION ALL SELECT {uuid_lit} AS tx_uuid{cols_suffix}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use penca_db::dialect::pg::PgDialect;
    use penca_dl::dialect::DfDialect;

    const UPS: &str = "upsert_log";
    const DEL: &str = "delete_log";
    const TX: &str = "commit_tx_log";

    // An effectively-unbounded upper fence for the SQL-shape tests below —
    // there is no unbounded `ReadSnapshot` variant. `commit_micros <= i64::MAX`
    // matches every committed row, so these tests still exercise the
    // no-meaningful-bound shape.
    const LATEST: ReadSnapshot = ReadSnapshot::AsOfMicros(i64::MAX);

    #[test]
    fn merge_resolved_pg_uses_distinct_on() {
        let sql =
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name", "age"], None, &LATEST, None);
        assert!(
            sql.contains("DISTINCT ON (\"row_uuid\")"),
            "expected DISTINCT ON clause, got: {sql}",
        );
        assert!(!sql.contains("ROW_NUMBER"), "pg must not use ROW_NUMBER");
    }

    #[test]
    fn merge_resolved_df_uses_row_number() {
        let sql =
            build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name", "age"], None, &LATEST, None);
        // ORDER BY is the composite `(commit_seq_num, write_seq_num)`; see
        // [`merge_latest_orders_by_composite_desc`] for the dedicated lock-in.
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY \"row_uuid\" ORDER BY \"commit_seq_num\" DESC, \"write_seq_num\" DESC)"
            ),
            "expected ROW_NUMBER window with composite ORDER BY, got: {sql}",
        );
        assert!(!sql.contains("DISTINCT ON"), "df must not use DISTINCT ON");
    }

    // Both dialects share the two-arm UNION shape (the tier-specific sources
    // differ above the CTEs), so pin it once for each.
    #[test]
    fn merge_resolved_emits_two_arm_is_delete_union() {
        for sql in [
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None),
            build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None),
        ] {
            assert!(
                sql.contains("UNION ALL"),
                "two-arm resolve must UNION the arms: {sql}",
            );
            assert!(
                sql.contains("false AS is_delete"),
                "upsert arm must flag is_delete = false: {sql}",
            );
            assert!(
                sql.contains("true AS is_delete"),
                "tombstone arm must flag is_delete = true: {sql}",
            );
            assert!(
                sql.contains("FROM latest l LEFT JOIN deletes d ON l.row_uuid = d.row_uuid"),
                "upsert arm must LEFT JOIN deletes: {sql}",
            );
            assert!(
                sql.contains("FROM deletes d LEFT JOIN latest l ON d.row_uuid = l.row_uuid"),
                "tombstone arm must LEFT JOIN latest: {sql}",
            );
        }
    }

    #[test]
    fn merge_resolved_quotes_user_columns() {
        let sql =
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name", "age"], None, &LATEST, None);
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("\"age\""));
        assert!(sql.contains("u.\"name\""));
        assert!(sql.contains("l.\"age\""));
    }

    #[test]
    fn merge_resolved_omits_user_cols_cleanly_when_empty() {
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &[], None, &LATEST, None);
        // The joined CTE carries write_seq_num alongside commit_micros; the
        // final SELECT emits just row_uuid + commit_micros (the caller doesn't
        // consume write_seq_num).
        assert!(sql.contains("u.row_uuid, c.commit_micros, u.write_seq_num"));
        assert!(sql.contains("l.row_uuid, l.commit_micros"));
        assert!(!sql.contains(", ,"), "dangling comma: {sql}");
    }

    #[test]
    fn merge_resolved_reads_upsert_log_directly() {
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        assert!(
            sql.contains("FROM \"upsert_log\" u JOIN committed_tx"),
            "expected direct join on upsert_log, got: {sql}",
        );
    }

    #[test]
    fn merge_resolved_applies_as_of_micros() {
        let sql = build_merge_resolved::<PgDialect>(
            UPS,
            DEL,
            TX,
            &["name"],
            None,
            &ReadSnapshot::AsOfMicros(1_700_000_000_000_000),
            None,
        );
        assert!(sql.contains("WHERE commit_micros <= 1700000000000000"));
    }

    #[test]
    fn merge_resolved_applies_seq_tier_lower() {
        let sql =
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], Some(42), &LATEST, None);
        // The seq tier fence and the snapshot's upper bound coexist — every
        // read carries a bounded `<=` snapshot predicate.
        assert!(sql.contains("commit_seq_num > 42"), "seq tier fence: {sql}");
        assert!(sql.contains("commit_micros <= 9223372036854775807"));
    }

    #[test]
    fn merge_resolved_no_tier_lower_emits_only_upper_bound() {
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        // There is no unbounded read, so the committed_tx CTE always carries
        // the snapshot's `<= ts` predicate; with no seq tier fence
        // (pre-Persist) the CTE WHERE closes right after it.
        assert!(
            sql.contains("FROM \"commit_tx_log\" WHERE commit_micros <= 9223372036854775807)"),
            "committed_tx CTE should carry only the snapshot upper bound: {sql}",
        );
        // The tier fence appears as `... WHERE commit_seq_num > N` in the
        // committed_tx CTE; the tombstone-shadow predicate's `l.commit_seq_num >
        // d.commit_seq_num` is a different (aliased) shape, so match the fence form.
        assert!(
            !sql.contains("WHERE commit_seq_num >"),
            "pre-Persist read emits no seq tier fence: {sql}",
        );
    }

    #[test]
    fn merge_resolved_combines_seq_tier_and_as_of() {
        // Seq tier fence `commit_seq_num > W_persist` composed with the
        // AsOfMicros visibility cap on committed_at.
        let sql = build_merge_resolved::<PgDialect>(
            UPS,
            DEL,
            TX,
            &["name"],
            Some(100),
            &ReadSnapshot::AsOfMicros(200),
            None,
        );
        assert!(
            sql.contains("WHERE commit_seq_num > 100 AND commit_micros <= 200"),
            "combined seq tier + as_of: {sql}",
        );
    }

    #[test]
    fn merge_resolved_filters_tombstoned_upserts() {
        // The lexicographic spelling avoids DataFusion's incomplete support
        // for SQL row-value comparison; PG supports both forms — see
        // [`merge_predicate_uses_composite_geq`].
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        assert!(sql.contains(
            "d.row_uuid IS NULL OR \
             l.commit_seq_num > d.commit_seq_num OR \
             (l.commit_seq_num = d.commit_seq_num AND \
              l.write_seq_num >= d.write_seq_num)"
        ));
    }

    #[test]
    fn merge_resolved_open_tx_emits_strict_lt_and_unions_synthetic() {
        let snapshot = ReadSnapshot::OpenTx {
            began_at_seq_num: 1_500,
            tx_uuid: uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        };
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &snapshot, None);
        // Strict-< on the SEQ axis (began_at_seq_num), no `<=`.
        assert!(
            sql.contains("WHERE commit_seq_num < 1500"),
            "expected strict-< seq bound, got: {sql}",
        );
        assert!(!sql.contains("commit_seq_num <="));
        // Synthetic open-tx row UNIONed into committed_tx with i64::MAX on
        // both the committed_at and commit_seq_num axes — higher than any real
        // committed seq, so the tx's own uncommitted writes win the
        // latest-version-per-row_uuid race (ordered by commit_seq_num).
        assert!(
            sql.contains(
                "UNION ALL SELECT '11111111-1111-1111-1111-111111111111'::uuid AS tx_uuid, \
                 9223372036854775807 AS commit_micros, 9223372036854775807 AS commit_seq_num"
            ),
            "expected open-tx UNION row with i64::MAX on both axes, got: {sql}",
        );
    }

    #[test]
    fn merge_resolved_open_tx_df_dialect_omits_uuid_cast() {
        let tx = uuid::Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let snapshot = ReadSnapshot::OpenTx {
            began_at_seq_num: 99,
            tx_uuid: tx,
        };
        let sql = build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name"], None, &snapshot, None);
        // DataFusion uses Utf8 for tx_uuid — bare string literal, no `::uuid` cast.
        let lit = format!("'{tx}'");
        assert!(
            sql.contains(&format!("UNION ALL SELECT {lit} AS tx_uuid")),
            "expected open-tx UNION row without PG cast, got: {sql}",
        );
        assert!(!sql.contains(&format!("{lit}::uuid")));
    }

    #[test]
    fn merge_resolved_within_rpc_tie_upsert_wins() {
        // Lock-in for the seq tiebreaker specified in ADR 0009 §76. When an
        // open tx does INSERT R + DELETE R inside one WriteData RPC, both
        // writes share one `commit_seq_num`; the co-batch delete gets a
        // strictly lower `write_seq_num` (deletes-first), so the upsert wins
        // via `>=`. The tombstone-shadow predicate is semantically
        // `(commit_seq_num, write_seq_num) >= (d.commit_seq_num,
        // d.write_seq_num)`, written out lexicographically because
        // DataFusion's row-value comparison support is incomplete (PG supports
        // both forms). On tie the `=` branch takes the `>=` side → upsert wins
        // → row visible.
        //
        // Pins the composite shape AND the absence of the single-column
        // predicate, so anyone reverting the change without updating ADR 0009
        // fails loudly.
        let snapshot = ReadSnapshot::OpenTx {
            began_at_seq_num: 1_500,
            tx_uuid: uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
        };
        let sql = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &snapshot, None);
        assert!(
            sql.contains(
                "l.commit_seq_num > d.commit_seq_num OR \
                 (l.commit_seq_num = d.commit_seq_num AND \
                  l.write_seq_num >= d.write_seq_num)"
            ),
            "expected lexicographic composite tombstone-shadow predicate \
             (UPSERT wins on tie), got: {sql}",
        );
        // The single-column shape (no `write_seq_num` tiebreaker) must be
        // absent.
        assert!(
            !sql.contains("l.commit_micros > d.deleted_at"),
            "comparison must use the composite key; reverting to single-column \
             strict-> would flip back to DELETE-wins-on-tie (ADR 0009 §63 / §76 \
             regression): {sql}",
        );
    }

    #[test]
    fn merge_predicate_uses_composite_geq() {
        // Hot + cold builders both emit the composite lexicographic
        // predicate; pin the exact shape so anyone editing either path
        // fails loudly. Written out (rather than `(a, b) >= (c, d)`)
        // because DataFusion's row-value comparison support is
        // incomplete — PG handles both forms.
        let hot = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        let cold =
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None);
        let expected = "l.commit_seq_num > d.commit_seq_num OR \
                        (l.commit_seq_num = d.commit_seq_num AND \
                         l.write_seq_num >= d.write_seq_num)";
        assert!(
            hot.contains(expected),
            "hot builder missing composite tombstone-shadow predicate: {hot}",
        );
        assert!(
            cold.contains(expected),
            "cold builder missing composite tombstone-shadow predicate: {cold}",
        );
        // The single-column form (using the `deleted_at` alias) must be gone.
        assert!(
            !hot.contains("d.deleted_at"),
            "hot must not retain the deleted_at alias: {hot}",
        );
        assert!(
            !cold.contains("d.deleted_at"),
            "cold must not retain the deleted_at alias: {cold}",
        );
        // SQL-standard row-value form is fragile under DataFusion 52 —
        // pin its absence so a future cleanup doesn't silently break
        // the cold tier.
        assert!(
            !hot.contains("(l.commit_micros, l.write_seq_num) >="),
            "hot must not emit SQL row-value comparison: {hot}",
        );
        assert!(
            !cold.contains("(l.commit_micros, l.write_seq_num) >="),
            "cold must not emit SQL row-value comparison \
             (DataFusion executor schema mismatch): {cold}",
        );
    }

    #[test]
    fn merge_latest_orders_by_composite_desc() {
        // Composite ordering on the latest CTE — `commit_seq_num` first,
        // `write_seq_num` as the tiebreaker. Verify both dialects.
        let pg = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        assert!(
            pg.contains("ORDER BY \"row_uuid\", \"commit_seq_num\" DESC, \"write_seq_num\" DESC"),
            "pg latest CTE missing composite ORDER BY: {pg}",
        );
        let df = build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        assert!(
            df.contains(
                "ROW_NUMBER() OVER (PARTITION BY \"row_uuid\" ORDER BY \"commit_seq_num\" DESC, \"write_seq_num\" DESC)"
            ),
            "df latest CTE missing composite window ORDER BY: {df}",
        );
    }

    #[test]
    fn merge_deletes_cte_carries_written_at() {
        // The deletes CTE selects `row_uuid`, `commit_micros`,
        // and `write_seq_num` (preserved names, no alias) so the
        // outer composite predicate can bind both ordering keys via
        // `d.commit_micros` / `d.write_seq_num`.
        for sql in [
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None),
            build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None),
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None),
        ] {
            assert!(
                sql.contains("\"write_seq_num\""),
                "deletes CTE must carry write_seq_num: {sql}",
            );
            // No GROUP BY/MAX aggregation shape.
            assert!(
                !sql.contains("MAX(c.commit_micros) AS deleted_at"),
                "deletes CTE must not retain old MAX-aggregation: {sql}",
            );
            assert!(
                !sql.contains("MAX(d.commit_micros) AS deleted_at"),
                "cold deletes CTE must not retain old MAX-aggregation: {sql}",
            );
            assert!(
                !sql.contains("deleted_at"),
                "deletes CTE must not retain pre-CHA-243 deleted_at alias: {sql}",
            );
        }
    }

    #[test]
    fn merge_joined_cte_carries_written_at() {
        // Hot joined CTE pulls `u.write_seq_num` from the upsert
        // log; cold builder pulls it inline.
        let hot = build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None);
        assert!(
            hot.contains("c.commit_micros, u.write_seq_num"),
            "hot joined CTE must carry write_seq_num: {hot}",
        );
        let cold =
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None);
        assert!(
            cold.contains("u.commit_micros, u.write_seq_num"),
            "cold joined CTE must carry write_seq_num: {cold}",
        );
    }

    // The cold tier carries the committed-at window on the wire as
    // `PersistPlan.committed_at: IntegerRange`. The merge SQL builders
    // splice it inline; these tests pin the exact emitted shape so
    // anyone editing the clause notices the wire-shape change.

    #[test]
    fn cold_visibility_clause_none_returns_empty() {
        // Both committed_at bounds unset (the boundary unpacks an absent or
        // empty IntegerRange to `None`/`None`) and no seq bound → empty.
        assert_eq!(cold_visibility_clause(None, None, None, None), "");
        assert_eq!(cold_visibility_clause(None, None, None, Some("d")), "");
    }

    #[test]
    fn cold_visibility_clause_emits_half_open_when_both_set() {
        assert_eq!(
            cold_visibility_clause(Some(100), Some(200), None, None),
            " WHERE commit_micros >= 100 AND commit_micros < 200"
        );
    }

    #[test]
    fn cold_visibility_clause_max_only_omits_min_predicate() {
        let clause = cold_visibility_clause(None, Some(200), None, None);
        assert_eq!(clause, " WHERE commit_micros < 200");
    }

    #[test]
    fn cold_visibility_clause_min_only_omits_max_predicate() {
        let clause = cold_visibility_clause(Some(100), None, None, None);
        assert_eq!(clause, " WHERE commit_micros >= 100");
    }

    #[test]
    fn cold_visibility_clause_with_alias_prefixes_column() {
        assert_eq!(
            cold_visibility_clause(Some(100), Some(200), None, Some("d")),
            " WHERE d.commit_micros >= 100 AND d.commit_micros < 200"
        );
    }

    #[test]
    fn cold_visibility_clause_ands_seq_upper() {
        // An AsOfSeq read passes a `commit_seq_num <= N` upper bound, ANDed
        // after the committed_at window (or alone when no window).
        assert_eq!(
            cold_visibility_clause(Some(100), Some(200), Some(7), Some("d")),
            " WHERE d.commit_micros >= 100 AND d.commit_micros < 200 \
             AND d.commit_seq_num <= 7"
        );
        assert_eq!(
            cold_visibility_clause(None, None, Some(7), None),
            " WHERE commit_seq_num <= 7"
        );
    }

    // Cold resolve shares the hot two-arm (upsert / tombstone) shape.
    #[test]
    fn cold_merge_resolved_emits_two_arm_is_delete_union() {
        let sql =
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None);
        assert!(
            sql.contains("UNION ALL"),
            "cold resolve must UNION the arms: {sql}"
        );
        assert!(
            sql.contains("false AS is_delete"),
            "cold upsert arm flag: {sql}"
        );
        assert!(
            sql.contains("true AS is_delete"),
            "cold tombstone arm flag: {sql}"
        );
        assert!(
            sql.contains("FROM latest l LEFT JOIN deletes d ON l.row_uuid = d.row_uuid"),
            "cold upsert arm must LEFT JOIN deletes: {sql}",
        );
        assert!(
            sql.contains("FROM deletes d LEFT JOIN latest l ON d.row_uuid = l.row_uuid"),
            "cold tombstone arm must LEFT JOIN latest: {sql}",
        );
    }

    #[test]
    fn cold_merge_resolved_threads_committed_at_through_upsert_and_delete_logs() {
        // The cold builder filters the upsert log inline (bare column,
        // single-table FROM) and the delete log via the `d.` alias
        // (aggregation context). Both arms must apply the same
        // half-open window.
        let sql = build_cold_merge_resolved::<DfDialect>(
            UPS,
            DEL,
            &["name"],
            Some(100),
            Some(200),
            None,
            None,
        );
        assert!(
            sql.contains(
                "FROM \"upsert_log\" u WHERE commit_micros >= 100 AND commit_micros < 200"
            ),
            "upsert side missing committed_at filter: {sql}",
        );
        assert!(
            sql.contains(
                "FROM \"delete_log\" d WHERE d.commit_micros >= 100 AND d.commit_micros < 200"
            ),
            "delete side missing aliased committed_at filter: {sql}",
        );
    }

    #[test]
    fn cold_merge_resolved_threads_seq_upper_through_upsert_and_delete_logs() {
        // The `commit_seq_num <= N` bound must land on BOTH cold sources of the
        // full merge SQL (not just in the clause-builder unit test) — the
        // upsert log inline and the delete log via the `d.` alias — ANDed after
        // the committed_at window.
        let sql = build_cold_merge_resolved::<DfDialect>(
            UPS,
            DEL,
            &["name"],
            Some(100),
            Some(200),
            Some(7),
            None,
        );
        assert!(
            sql.contains(
                "FROM \"upsert_log\" u WHERE commit_micros >= 100 AND \
                 commit_micros < 200 AND commit_seq_num <= 7"
            ),
            "upsert side missing the seq upper bound: {sql}",
        );
        assert!(
            sql.contains(
                "FROM \"delete_log\" d WHERE d.commit_micros >= 100 AND \
                 d.commit_micros < 200 AND d.commit_seq_num <= 7"
            ),
            "delete side missing the aliased seq upper bound: {sql}",
        );
    }

    #[test]
    fn cold_merge_resolved_no_window_omits_where_on_both_logs() {
        let sql =
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None);
        assert!(
            !sql.contains("commit_micros >="),
            "no min bound expected with no window: {sql}",
        );
        assert!(
            !sql.contains("commit_micros <"),
            "no max bound expected with no window: {sql}",
        );
    }

    const SNAP: &str = "snapshot";
    const EXCL: &str = "exclusion";

    #[test]
    fn cold_snapshot_scan_selects_row_uuid_and_user_cols() {
        let sql = build_cold_snapshot_scan::<DfDialect>(SNAP, EXCL, &["name", "value"], None, None);
        // Anchor to the SELECT list so a builder that only emitted l.row_uuid in
        // the anti-join WHERE (and dropped it from the projection) still fails.
        assert!(
            sql.contains("SELECT l.row_uuid"),
            "must select l.row_uuid in the projection: {sql}",
        );
        assert!(
            sql.contains("l.\"name\"") && sql.contains("l.\"value\""),
            "must select l-qualified user cols: {sql}",
        );
        assert!(
            sql.contains("FROM \"snapshot\" l"),
            "must scan the snapshot table aliased l: {sql}",
        );
    }

    #[test]
    fn cold_snapshot_scan_anti_joins_exclusion() {
        let sql = build_cold_snapshot_scan::<DfDialect>(SNAP, EXCL, &["name"], None, None);
        assert!(
            sql.contains("l.row_uuid NOT IN (SELECT row_uuid FROM \"exclusion\")"),
            "must anti-join the exclusion table by row_uuid: {sql}",
        );
    }

    #[test]
    fn cold_snapshot_scan_appends_user_filter() {
        let sql = build_cold_snapshot_scan::<DfDialect>(
            SNAP,
            EXCL,
            &["value"],
            Some("l.value > 5"),
            None,
        );
        assert!(
            sql.ends_with(" AND (l.value > 5)"),
            "user filter appended at the outer WHERE: {sql}",
        );
        // The exclusion subquery stays unfiltered.
        assert!(
            sql.contains("l.row_uuid NOT IN (SELECT row_uuid FROM \"exclusion\")"),
            "exclusion subquery must remain filter-free: {sql}",
        );
    }

    #[test]
    fn cold_snapshot_scan_empty_filter_treated_as_none() {
        let none_sql = build_cold_snapshot_scan::<DfDialect>(SNAP, EXCL, &["value"], None, None);
        let empty_sql =
            build_cold_snapshot_scan::<DfDialect>(SNAP, EXCL, &["value"], Some(""), None);
        assert!(
            !none_sql.contains(" AND ("),
            "None filter must not append a trailing AND clause: {none_sql}",
        );
        assert_eq!(none_sql, empty_sql, "empty filter is treated as None");
        assert!(none_sql.contains("l.row_uuid NOT IN (SELECT row_uuid FROM \"exclusion\")"));
    }

    const U1: &str = "11111111-1111-1111-1111-111111111111";
    const U2: &str = "22222222-2222-2222-2222-222222222222";

    fn test_row_uuids() -> Vec<Uuid> {
        vec![Uuid::parse_str(U1).unwrap(), Uuid::parse_str(U2).unwrap()]
    }

    #[test]
    fn row_uuids_none_emits_no_in_clause() {
        // None (and empty-slice) restriction leaves every builder's SQL
        // free of a row_uuid IN clause.
        let empty: Vec<Uuid> = Vec::new();
        for sql in [
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, None),
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, Some(&empty)),
            build_cold_merge_resolved::<DfDialect>(UPS, DEL, &["name"], None, None, None, None),
            build_cold_snapshot_scan::<DfDialect>(SNAP, EXCL, &["name"], None, None),
        ] {
            assert!(
                !sql.contains("row_uuid IN ("),
                "no restriction must emit no IN clause: {sql}",
            );
        }
    }

    #[test]
    fn merge_resolved_restricts_sources_below_dedup_pg() {
        // The restriction sits inside the _u/_d sources — below the
        // DISTINCT ON — with PG-cast uuid literals.
        let uuids = test_row_uuids();
        let sql =
            build_merge_resolved::<PgDialect>(UPS, DEL, TX, &["name"], None, &LATEST, Some(&uuids));
        assert!(
            sql.contains(&format!(
                "FROM \"upsert_log\" u JOIN committed_tx c USING (tx_uuid) \
                 WHERE u.row_uuid IN ('{U1}'::uuid, '{U2}'::uuid)"
            )),
            "upsert source must carry the restriction below the dedup: {sql}",
        );
        assert!(
            sql.contains(&format!(
                "FROM \"delete_log\" d JOIN committed_tx c USING (tx_uuid) \
                 WHERE d.row_uuid IN ('{U1}'::uuid, '{U2}'::uuid)"
            )),
            "delete source must carry the restriction below the dedup: {sql}",
        );
    }

    #[test]
    fn merge_resolved_restriction_df_uses_bare_literals() {
        // DataFusion's row_uuid columns are Utf8 — bare string literals,
        // no ::uuid cast.
        let uuids = test_row_uuids();
        let sql =
            build_merge_resolved::<DfDialect>(UPS, DEL, TX, &["name"], None, &LATEST, Some(&uuids));
        assert!(
            sql.contains(&format!("WHERE u.row_uuid IN ('{U1}', '{U2}')")),
            "df restriction must use bare literals: {sql}",
        );
        assert!(!sql.contains("::uuid"), "df must not cast: {sql}");
    }

    #[test]
    fn cold_merge_resolved_restriction_composes_with_committed_at() {
        // With a committed_at window the restriction AND-composes; without
        // one it opens the WHERE.
        let uuids = test_row_uuids();
        let windowed = build_cold_merge_resolved::<DfDialect>(
            UPS,
            DEL,
            &["name"],
            Some(100),
            Some(200),
            None,
            Some(&uuids),
        );
        assert!(
            windowed.contains(&format!(
                "FROM \"upsert_log\" u WHERE commit_micros >= 100 AND \
                 commit_micros < 200 AND u.row_uuid IN ('{U1}', '{U2}')"
            )),
            "windowed upsert source must AND-compose: {windowed}",
        );
        assert!(
            windowed.contains(&format!(
                "FROM \"delete_log\" d WHERE d.commit_micros >= 100 AND \
                 d.commit_micros < 200 AND d.row_uuid IN ('{U1}', '{U2}')"
            )),
            "windowed delete source must AND-compose: {windowed}",
        );

        let bare = build_cold_merge_resolved::<DfDialect>(
            UPS,
            DEL,
            &["name"],
            None,
            None,
            None,
            Some(&uuids),
        );
        assert!(
            bare.contains(&format!(
                "FROM \"upsert_log\" u WHERE u.row_uuid IN ('{U1}', '{U2}')"
            )),
            "no window: restriction opens the WHERE: {bare}",
        );
    }

    #[test]
    fn cold_snapshot_scan_restriction_before_user_filter() {
        // The restriction lands after the anti-join, before the residual
        // user filter; the exclusion subquery stays untouched.
        let uuids = test_row_uuids();
        let sql = build_cold_snapshot_scan::<DfDialect>(
            SNAP,
            EXCL,
            &["value"],
            Some("l.value > 5"),
            Some(&uuids),
        );
        assert!(
            sql.contains(&format!(
                "NOT IN (SELECT row_uuid FROM \"exclusion\") AND l.row_uuid IN ('{U1}', '{U2}')"
            )),
            "restriction follows the anti-join: {sql}",
        );
        assert!(
            sql.ends_with(" AND (l.value > 5)"),
            "residual user filter stays last: {sql}",
        );
    }
}
