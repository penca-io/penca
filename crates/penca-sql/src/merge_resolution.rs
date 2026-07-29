//! Composite-tiebreaker merge resolution kernel.
//!
//! The kernel is [`lex_compare_predicate`] — the lex-compare-as-OR/AND
//! form of `(a, b) >= (c, d)`. Written out rather than SQL row-value
//! comparison because DataFusion 52's executor schema for the row-value
//! form doesn't match its planner schema. Built on top of
//! that, [`build_composite_merge_resolution`] produces the latest +
//! deletes CTE pair and both tombstone-shadow predicates for the
//! read-path callers.

use crate::dialect::Dialect;

/// Build the lexicographic comparison predicate `(left_a, left_b) > (right_a, right_b)`
/// (when `strict_tie` is true) or `(left_a, left_b) >= (right_a, right_b)`
/// (when false), written out as the OR/AND form for dialect portability.
///
/// Result is a bare predicate string with no surrounding parens — callers
/// wrap with their own NULL guards / boolean composition.
///
/// Pure string templating; no `Dialect` parameter needed because the
/// `>` `>=` `=` `AND` `OR` constructs are dialect-agnostic on integers.
pub fn lex_compare_predicate(
    left_a: &str,
    left_b: &str,
    right_a: &str,
    right_b: &str,
    strict_tie: bool,
) -> String {
    let final_op = if strict_tie { ">" } else { ">=" };
    format!(
        "{left_a} > {right_a} OR \
         ({left_a} = {right_a} AND {left_b} {final_op} {right_b})"
    )
}

/// CTE bodies + tombstone-shadow predicates for the composite tiebreaker.
/// Returned by [`build_composite_merge_resolution`].
///
/// Aliases used in the predicates are fixed: `l.` for the latest CTE,
/// `d.` for the deletes CTE. Callers MUST alias their JOINed CTEs as
/// `l` and `d` for the predicates to compile.
pub struct CompositeMergeResolution {
    /// `latest AS (...)` — the latest committed upsert per `row_uuid`,
    /// picked by composite `(commit_micros, write_seq_num)`
    /// DESC ordering. Includes user columns.
    pub latest_cte: String,
    /// `deletes AS (...)` — the latest committed delete per `row_uuid`,
    /// picked by composite `(commit_micros, write_seq_num)`
    /// DESC ordering. No user columns (deletes only carry the row ID).
    pub deletes_cte: String,
    /// WHERE predicate filtering visible upserts. Parenthesized; ready
    /// to splice after `WHERE ` (no leading space). Includes the
    /// `d.row_uuid IS NULL` LEFT-JOIN guard. Upsert wins on tie
    /// (composite `>=`).
    pub upsert_visible_predicate: String,
    /// Mirror predicate filtering visible deletes. Parenthesized;
    /// ready to splice after `WHERE `. Includes the `l.row_uuid IS
    /// NULL` LEFT-JOIN guard. Delete wins ONLY on strict greater
    /// (composite `>`) — on tie, upsert wins and the delete drops.
    pub delete_visible_predicate: String,
}

/// Build the latest + deletes CTEs and both tombstone-shadow predicates
/// for the composite tiebreaker.
///
/// `upsert_source_sql` — a FROM-clause fragment (bare table/CTE name or
/// parenthesized aliased subquery) producing rows of shape
/// `(row_uuid, <user_cols>, commit_micros, write_seq_num)`.
///
/// `delete_source_sql` — same but `(row_uuid, commit_micros,
/// write_seq_num)` (no user cols).
///
/// Each caller is responsible for step 1 (building the tier-specific
/// source SQLs — hot tier JOINs to commit_tx_log, cold tier reads inline,
/// branch merge JOINs to source_committed_tx) and step 3 (splicing the
/// returned CTEs + predicates into the final statement). The shared
/// semantic — composite ordering key + lex tombstone-shadow — lives here,
/// in one place.
pub fn build_composite_merge_resolution<D: Dialect>(
    upsert_source_sql: &str,
    delete_source_sql: &str,
    user_cols: &[&str],
    order_primary: &str,
) -> CompositeMergeResolution {
    // `order_primary` is the primary latest-wins ordering /
    // tombstone-shadow key — `commit_seq_num` for read merge (the authoritative
    // commit order; `commit_micros` can tie under concurrency),
    // `commit_micros` for branch-merge (which discards source tx
    // identities, so no per-tx seq is available). `write_seq_num` (the
    // intra-tx mutation ordinal) is the within-tx secondary. The CTEs always
    // carry `commit_micros`
    // (the final SELECT / cross-tier dedup output) even when it is not the
    // order key.
    let order_cols: [&str; 2] = [order_primary, "write_seq_num"];
    let mut carry: Vec<&str> = vec!["commit_micros", "write_seq_num"];
    if order_primary != "commit_micros" && order_primary != "write_seq_num" {
        carry.push(order_primary);
    }

    let latest_select_cols: Vec<&str> = std::iter::once("row_uuid")
        .chain(user_cols.iter().copied())
        .chain(carry.iter().copied())
        .collect();
    let latest_body = D::latest_per_partition(
        &latest_select_cols,
        upsert_source_sql,
        "row_uuid",
        &order_cols,
    );
    let latest_cte = format!("latest AS ({latest_body})");

    let deletes_select_cols: Vec<&str> = std::iter::once("row_uuid")
        .chain(carry.iter().copied())
        .collect();
    let deletes_body = D::latest_per_partition(
        &deletes_select_cols,
        delete_source_sql,
        "row_uuid",
        &order_cols,
    );
    let deletes_cte = format!("deletes AS ({deletes_body})");

    let l_primary = format!("l.{order_primary}");
    let d_primary = format!("d.{order_primary}");
    let upsert_lex = lex_compare_predicate(
        &l_primary,
        "l.write_seq_num",
        &d_primary,
        "d.write_seq_num",
        false,
    );
    let upsert_visible_predicate = format!("(d.row_uuid IS NULL OR {upsert_lex})");

    let delete_lex = lex_compare_predicate(
        &d_primary,
        "d.write_seq_num",
        &l_primary,
        "l.write_seq_num",
        true,
    );
    let delete_visible_predicate = format!("(l.row_uuid IS NULL OR {delete_lex})");

    CompositeMergeResolution {
        latest_cte,
        deletes_cte,
        upsert_visible_predicate,
        delete_visible_predicate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_compare_predicate_non_strict_uses_geq_on_tiebreak() {
        // The non-strict form is the canonical upsert-wins-on-tie
        // shape: `a > c OR (a = c AND b >= d)`. Tied `(a, c)` flows into
        // the AND branch and `>=` lets the visible side win.
        let p = lex_compare_predicate(
            "l.commit_micros",
            "l.write_seq_num",
            "d.commit_micros",
            "d.write_seq_num",
            false,
        );
        assert_eq!(
            p,
            "l.commit_micros > d.commit_micros OR \
             (l.commit_micros = d.commit_micros AND \
              l.write_seq_num >= d.write_seq_num)"
        );
    }

    #[test]
    fn lex_compare_predicate_strict_uses_gt_on_tiebreak() {
        // The strict form is the mirror "delete wins ONLY on strict greater"
        // shape: `a > c OR (a = c AND b > d)`. Tied `(a, b) = (c, d)`
        // produces false (neither branch matches), which is what callers
        // want on the delete-visible side so upsert wins on tie.
        let p = lex_compare_predicate(
            "d.commit_micros",
            "d.write_seq_num",
            "l.commit_micros",
            "l.write_seq_num",
            true,
        );
        assert_eq!(
            p,
            "d.commit_micros > l.commit_micros OR \
             (d.commit_micros = l.commit_micros AND \
              d.write_seq_num > l.write_seq_num)"
        );
    }

    /// Minimal `Dialect` stub for testing — produces deterministic
    /// strings so the assertions don't have to mirror PG or DF's
    /// `latest_per_partition` quirks.
    struct StubDialect;
    impl Dialect for StubDialect {
        fn latest_per_partition(
            select_cols: &[&str],
            inner_from: &str,
            partition_col: &str,
            order_cols: &[&str],
        ) -> String {
            format!(
                "STUB[select={select:?}, from={inner_from}, partition={partition_col}, order={order_cols:?}]",
                select = select_cols,
            )
        }
    }

    #[test]
    fn composite_merge_resolution_latest_cte_carries_user_cols_and_composite_order() {
        // Read merge orders on the commit-order serial; committed_at
        // is still carried (final SELECT / cross-tier dedup output).
        let r = build_composite_merge_resolution::<StubDialect>(
            "upsert_src",
            "delete_src",
            &["name", "value"],
            "commit_seq_num",
        );
        assert!(
            r.latest_cte.starts_with("latest AS ("),
            "latest CTE must use the canonical `latest` name: {}",
            r.latest_cte,
        );
        assert!(
            r.latest_cte.contains(
                "select=[\"row_uuid\", \"name\", \"value\", \"commit_micros\", \"write_seq_num\", \"commit_seq_num\"]"
            ),
            "latest CTE must project user_cols + committed_at + seq: {}",
            r.latest_cte,
        );
        assert!(
            r.latest_cte
                .contains("order=[\"commit_seq_num\", \"write_seq_num\"]"),
            "latest CTE must order by composite (commit_seq_num, write_seq_num): {}",
            r.latest_cte,
        );
    }

    #[test]
    fn composite_merge_resolution_branch_merge_mode_orders_on_committed_at() {
        // Branch-merge discards source tx identities (no per-tx seq), so it
        // passes `commit_micros` as the order primary — committed_at is
        // NOT duplicated into the carry, and there is no commit_seq_num column.
        let r = build_composite_merge_resolution::<StubDialect>(
            "upsert_src",
            "delete_src",
            &["name"],
            "commit_micros",
        );
        assert!(
            r.latest_cte
                .contains("select=[\"row_uuid\", \"name\", \"commit_micros\", \"write_seq_num\"]"),
            "branch-merge latest CTE must not carry commit_seq_num: {}",
            r.latest_cte,
        );
        assert!(
            r.latest_cte
                .contains("order=[\"commit_micros\", \"write_seq_num\"]"),
            "branch-merge orders by (committed_at, write_seq_num): {}",
            r.latest_cte,
        );
        assert!(
            r.upsert_visible_predicate
                .contains("l.commit_micros > d.commit_micros"),
            "branch-merge tombstone-shadow stays on committed_at: {}",
            r.upsert_visible_predicate,
        );
    }

    #[test]
    fn composite_merge_resolution_deletes_cte_omits_user_cols() {
        let r = build_composite_merge_resolution::<StubDialect>(
            "upsert_src",
            "delete_src",
            &["name", "value"],
            "commit_seq_num",
        );
        assert!(
            r.deletes_cte.starts_with("deletes AS ("),
            "deletes CTE must use the canonical `deletes` name: {}",
            r.deletes_cte,
        );
        // Deletes don't carry user_cols — only row_uuid + timestamps + seq.
        assert!(
            r.deletes_cte.contains(
                "select=[\"row_uuid\", \"commit_micros\", \"write_seq_num\", \"commit_seq_num\"]"
            ),
            "deletes CTE must NOT carry user_cols: {}",
            r.deletes_cte,
        );
    }

    #[test]
    fn composite_merge_resolution_upsert_predicate_is_non_strict_with_null_guard() {
        // Upsert-visible predicate: composite `>=` (upsert wins on tie),
        // LEFT-JOIN guard `d.row_uuid IS NULL`, parens around the whole
        // thing so callers can splice after `WHERE ` directly.
        let r = build_composite_merge_resolution::<StubDialect>("_", "_", &[], "commit_seq_num");
        assert_eq!(
            r.upsert_visible_predicate,
            "(d.row_uuid IS NULL OR \
             l.commit_seq_num > d.commit_seq_num OR \
             (l.commit_seq_num = d.commit_seq_num AND \
              l.write_seq_num >= d.write_seq_num))"
        );
    }

    #[test]
    fn composite_merge_resolution_delete_predicate_is_strict_mirror_with_null_guard() {
        // Delete-visible predicate: mirror — `d.` and `l.` aliases swap,
        // strict `>` on the tiebreaker so upsert wins on tie (delete drops).
        // LEFT-JOIN guard `l.row_uuid IS NULL`.
        let r = build_composite_merge_resolution::<StubDialect>("_", "_", &[], "commit_seq_num");
        assert_eq!(
            r.delete_visible_predicate,
            "(l.row_uuid IS NULL OR \
             d.commit_seq_num > l.commit_seq_num OR \
             (d.commit_seq_num = l.commit_seq_num AND \
              d.write_seq_num > l.write_seq_num))"
        );
    }
}
