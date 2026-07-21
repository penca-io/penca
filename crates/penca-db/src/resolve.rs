//! Auditable store resolution via SQL.
//!
//! An auditable store is a composition of an upsert log, delete log, and
//! transaction log (README §"Auditable stores"). This module generates
//! SQL that resolves the latest committed state from these tables.
//!
//! The SQL uses CTEs to:
//! 1. Identify committed deletes with their latest delete event — the
//!    `(commit_seq_num, write_seq_num)` tuple of the row with the greatest
//!    commit-order position (CHA-431 seq tiebreaker).
//! 2. Rank upsert versions by `(commit_seq_num, write_seq_num)`
//!    per entity column, excluding upserts that lose the lexicographic
//!    comparison against the latest delete.
//! 3. Keep only the latest committed version of each entity.
//! 4. Identify effective deletes — entities where the latest action is a
//!    delete (no upsert wins the composite-`>=` comparison).
//!
//! The resolve is parameterized in two ways:
//!
//! - **entity_column** — the column the dedup partitions by. Always
//!   `row_uuid` post-CHA-177: data tables and the system tables
//!   (`__penca_system__.{schemas,tables}`) all share the
//!   `{prefix}_data_{upsert,delete}_log` shape and dedup on `row_uuid`
//!   (= `row_uuid_for_pk(parent_table_uuid, [pk_values])`).
//! - **open_tx_uuid** — when set, includes uncommitted writes from that
//!   open transaction in the result. Implemented by UNION-ALL'ing a
//!   synthetic row into `commit_tx_log_filtered` with a sentinel
//!   `commit_seq_num` (`i64::MAX`) so the open tx's writes rank
//!   latest in dedup. RYOW for sessions with an open tx; mirrors the
//!   data-path `as_of_tx_uuid` shape proposed in CHA-165.
//!
//! This is the Rust port of
//! `packages/penca/src/penca/lib/util/auditable_store.py`.

use crate::dialect::pg::PgDialect;
use crate::dialect::{
    Dialect, leading_comma_if_nonempty, lex_compare_predicate, qualify_user_cols,
};

/// Sentinel `commit_seq_num` for the synthetic open-tx row in
/// `commit_tx_log_filtered`. Sorts later than every real committed seq so the
/// open tx's writes rank latest in dedup (read-your-own-writes).
const OPEN_TX_SEQ_NUM_SENTINEL: i64 = i64::MAX;

/// Inputs to [`resolve_cte_sql`] / [`resolve_sql`]: which auditable
/// store to address (`upsert_table_name`, `delete_table_name`,
/// `commit_tx_log_table_name`, `entity_column`, `user_columns`) and which
/// slice of the tx-log to filter on (`row_filter`, `since_micros`,
/// `until_micros`, `branch_uuid`, `open_tx_uuid`).
///
/// Fields are `pub` so callers struct-literal at the call site —
/// there's one external consumer and the test consumers in this file;
/// a builder would be premature abstraction.
pub struct ResolveSpec<'a> {
    pub upsert_table_name: &'a str,
    pub delete_table_name: &'a str,
    pub commit_tx_log_table_name: &'a str,
    pub user_columns: &'a [&'a str],
    pub entity_column: &'a str,
    pub row_filter: Option<&'a str>,
    pub since_micros: Option<i64>,
    pub until_micros: Option<i64>,
    pub branch_uuid: Option<&'a str>,
    pub open_tx_uuid: Option<&'a str>,
}

/// Generate CTE definitions for transactional auditable store resolution.
///
/// Returns the CTE body (without `WITH` keyword or final `SELECT`) so
/// callers can embed it in larger queries.
///
/// The returned CTEs are: `commit_tx_log_filtered`, `committed_deletes`,
/// `upserts_ranked`, `latest_upserts`, `effective_deletes`. The entity
/// column is named whatever `spec.entity_column` selects.
pub fn resolve_cte_sql(spec: &ResolveSpec<'_>) -> String {
    let upsert_table = PgDialect::quote_identifier(spec.upsert_table_name);
    let delete_table = PgDialect::quote_identifier(spec.delete_table_name);
    let commit_tx_log_table = PgDialect::quote_identifier(spec.commit_tx_log_table_name);
    let entity_col = PgDialect::quote_identifier(spec.entity_column);
    // CHA-431: resolution orders on the seq axes `(commit_seq_num, write_seq_num)`;
    // commit_micros survives only as the audit since/until window filter.
    let commit_seq = PgDialect::quote_column("t", "commit_seq_num");

    let user_cols_qualified = qualify_user_cols::<PgDialect>("u", spec.user_columns);
    let user_cols_bare = spec
        .user_columns
        .iter()
        .map(|c| PgDialect::quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let lead = leading_comma_if_nonempty(spec.user_columns);

    let filter_clause = match spec.row_filter {
        Some(f) => format!(" AND {f}"),
        None => String::new(),
    };

    let mut tx_filters = Vec::new();
    if let Some(branch) = spec.branch_uuid {
        let col = PgDialect::quote_column("raw_t", "branch_uuid");
        tx_filters.push(format!("{col} = '{branch}'"));
    }
    if let Some(since) = spec.since_micros {
        let col = PgDialect::quote_column("raw_t", "commit_micros");
        tx_filters.push(format!("{col} > {since}"));
    }
    if let Some(until) = spec.until_micros {
        let col = PgDialect::quote_column("raw_t", "commit_micros");
        tx_filters.push(format!("{col} <= {until}"));
    }

    let raw_tx_seq = PgDialect::quote_column("raw_t", "commit_seq_num");
    let where_clause = if tx_filters.is_empty() {
        "TRUE".to_string()
    } else {
        tx_filters.join(" AND ")
    };

    // Synthetic open-tx row: the open tx's writes JOIN through
    // commit_tx_log_filtered and rank latest in the dedup ORDER BY DESC.
    // Read-your-own-writes for sessions with an open tx.
    let open_tx_union = match spec.open_tx_uuid {
        Some(tx) => format!(
            "\n    UNION ALL\n    SELECT '{tx}'::uuid AS tx_uuid,\
             \n           {OPEN_TX_SEQ_NUM_SENTINEL}::bigint AS commit_seq_num"
        ),
        None => String::new(),
    };

    // CHA-431: composite `(commit_seq_num, write_seq_num)` seq tiebreaker.
    // `committed_deletes` uses `DISTINCT ON` (PG-only resolver) to keep the
    // `write_seq_num` of the row with the greatest commit-order position, not
    // the per-column max — a per-column MAX would let the two extremes come
    // from different delete rows and break the lexicographic compare below.
    // `upserts_ranked` orders by the same composite key, and the
    // tombstone-shadow predicate goes through the shared
    // `lex_compare_predicate` so the composite semantic stays in
    // lockstep with the merge-on-read SQL (see
    // `penca_merge::sql::build_merge_resolved`). On tie the upsert
    // wins → row visible.
    //
    // Aliasing convention: per-row seq columns name the half they come
    // from — `upsert_commit_seq_num` / `upsert_write_seq_num`
    // on the upsert side, `delete_commit_seq_num` /
    // `delete_write_seq_num` on the delete side. The aliases on
    // `committed_deletes` are required (the outer JOIN binds them via
    // `cd.`); the matching aliases on `upserts_ranked` aren't
    // required but keep the predicate symmetric and let a reader
    // tell at a glance which side each comparand belongs to.
    //
    // Resolver shape diverges from the read-path's `latest LEFT JOIN
    // deletes` because `upserts_ranked` filters *then* ranks (so
    // `latest_upserts` can `WHERE row_rank = 1`) and
    // `effective_deletes` needs the pre-ranking committed_deletes set
    // for its anti-join. The lex-predicate kernel is the only piece
    // genuinely shared, so this resolver reaches for that primitive
    // directly rather than the `build_composite_merge_resolution`
    // CTE-pair helper.
    let tombstone_shadow_lex = lex_compare_predicate(
        &commit_seq,
        "u.write_seq_num",
        "cd.delete_commit_seq_num",
        "cd.delete_write_seq_num",
        false,
    );
    let tombstone_shadow_clause = format!("(cd.{entity_col} IS NULL OR {tombstone_shadow_lex})");

    format!(
        "commit_tx_log_filtered AS (
    SELECT raw_t.tx_uuid, {raw_tx_seq}
    FROM {commit_tx_log_table} raw_t
    WHERE {where_clause}{open_tx_union}
),
committed_deletes AS (
    SELECT DISTINCT ON (d.{entity_col}) d.{entity_col},
           {commit_seq} AS delete_commit_seq_num,
           d.write_seq_num AS delete_write_seq_num
    FROM {delete_table} d
    INNER JOIN commit_tx_log_filtered t ON d.tx_uuid = t.tx_uuid
    ORDER BY d.{entity_col}, {commit_seq} DESC, d.write_seq_num DESC
),
upserts_ranked AS (
    SELECT u.{entity_col}, u.tx_uuid{lead}{user_cols_qualified},
           {commit_seq} AS upsert_commit_seq_num,
           u.write_seq_num AS upsert_write_seq_num,
           ROW_NUMBER() OVER (
               PARTITION BY u.{entity_col}
               ORDER BY {commit_seq} DESC, u.write_seq_num DESC
           ) AS row_rank
    FROM {upsert_table} u
    INNER JOIN commit_tx_log_filtered t ON u.tx_uuid = t.tx_uuid
    LEFT JOIN committed_deletes cd ON u.{entity_col} = cd.{entity_col}
    WHERE {tombstone_shadow_clause}{filter_clause}
),
latest_upserts AS (
    SELECT {entity_col}{lead}{user_cols_bare}
    FROM upserts_ranked
    WHERE row_rank = 1
),
effective_deletes AS (
    SELECT cd.{entity_col}
    FROM committed_deletes cd
    WHERE cd.{entity_col} NOT IN (SELECT {entity_col} FROM latest_upserts)
)"
    )
}

/// Generate a complete SQL query that resolves a transactional store.
///
/// Returns a full query (`WITH ... SELECT`) that produces the latest
/// committed version of each entity with the given `spec.user_columns`
/// plus `spec.entity_column`.
pub fn resolve_sql(spec: &ResolveSpec<'_>) -> String {
    let user_cols_bare = spec
        .user_columns
        .iter()
        .map(|c| PgDialect::quote_identifier(c))
        .collect::<Vec<_>>()
        .join(", ");
    let lead = leading_comma_if_nonempty(spec.user_columns);
    let entity_col = PgDialect::quote_identifier(spec.entity_column);

    let cte_defs = resolve_cte_sql(spec);

    format!(
        "\
            WITH {cte_defs}
            SELECT {entity_col}{lead}{user_cols_bare} FROM latest_upserts
        "
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // CHA-243: emitted-SQL lock-ins for the composite tiebreaker.

    fn spec_with_user_cols<'a>(user_columns: &'a [&'a str]) -> ResolveSpec<'a> {
        ResolveSpec {
            upsert_table_name: "upsert_log",
            delete_table_name: "delete_log",
            commit_tx_log_table_name: "commit_tx_log",
            user_columns,
            entity_column: "row_uuid",
            row_filter: None,
            since_micros: None,
            until_micros: None,
            branch_uuid: None,
            open_tx_uuid: None,
        }
    }

    #[test]
    fn resolve_cte_uses_composite_ordering_and_predicate() {
        let sql = resolve_cte_sql(&spec_with_user_cols(&["name", "value"]));
        // CHA-431: `upserts_ranked` ranks by the seq composite
        // `(commit_seq_num, write_seq_num)` ordering key — no timestamp axis.
        assert!(
            sql.contains("ORDER BY t.\"commit_seq_num\" DESC, u.write_seq_num DESC"),
            "upserts_ranked must order by the seq composite key: {sql}",
        );
        // Tombstone-shadow predicate is the lex spelling of `(commit_seq_num,
        // write_seq_num) >= (delete_commit_seq_num, delete_write_seq_num)`, emitted by
        // the shared `lex_compare_predicate` helper. Same semantic as
        // the merge-on-read predicate in `penca_merge::sql`. We assert
        // the substring shape rather than the full multi-line format so
        // the test is robust against helper-internal formatting tweaks.
        assert!(
            sql.contains(
                "cd.\"row_uuid\" IS NULL OR \
                 t.\"commit_seq_num\" > cd.delete_commit_seq_num OR \
                 (t.\"commit_seq_num\" = cd.delete_commit_seq_num AND \
                 u.write_seq_num >= cd.delete_write_seq_num)"
            ),
            "tombstone-shadow predicate must be composite seq >=: {sql}",
        );
    }

    #[test]
    fn resolve_cte_committed_deletes_carries_write_seq() {
        let sql = resolve_cte_sql(&spec_with_user_cols(&[]));
        // CHA-431: `committed_deletes` carries `delete_write_seq_num`
        // alongside `delete_commit_seq_num` so the outer predicate can bind both
        // seq ordering keys. DISTINCT ON replaces the per-column MAX-GROUP-BY:
        // MAX across rows would mix the two extremes and break lexicographic
        // compare.
        assert!(
            sql.contains("DISTINCT ON (d.\"row_uuid\")"),
            "committed_deletes must use DISTINCT ON to keep one row \
             per entity: {sql}",
        );
        assert!(
            sql.contains("AS delete_commit_seq_num"),
            "committed_deletes must alias as delete_commit_seq_num: {sql}",
        );
        assert!(
            sql.contains("d.write_seq_num AS delete_write_seq_num"),
            "committed_deletes must carry delete_write_seq_num: {sql}",
        );
        assert!(
            !sql.contains("MAX("),
            "must not retain the pre-CHA-243 MAX aggregation: {sql}",
        );
    }

    #[test]
    fn resolve_cte_upserts_ranked_aliases_match_delete_side() {
        // CHA-431: symmetric `upsert_{commit_seq_num,write_seq_num}` aliases on
        // the `upserts_ranked` SELECT keep the predicate readable — every
        // comparand on the delete side has a sibling on the upsert side with
        // the same naming convention.
        let sql = resolve_cte_sql(&spec_with_user_cols(&[]));
        assert!(
            sql.contains("AS upsert_commit_seq_num"),
            "upserts_ranked must alias the JOIN-side commit seq as \
             upsert_commit_seq_num: {sql}",
        );
        assert!(
            sql.contains("u.write_seq_num AS upsert_write_seq_num"),
            "upserts_ranked must alias the per-row mutation ordinal as \
             upsert_write_seq_num: {sql}",
        );
    }
}
