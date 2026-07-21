//! DataFusion dialect for SQL emitted against an in-process DataFusion engine.
//!
//! DataFusion supports standard double-quoted identifiers but has no
//! `DISTINCT ON`, so `latest_per_partition` overrides the base [`Dialect`]
//! to emit a `ROW_NUMBER()` subquery instead.
//!
//! Why no `DlDialect` extension trait (peer to [`DbDialect`])? The OLTP
//! side adds DDL emission and Arrow→SQL type mapping on top of [`Dialect`]
//! — concerns *every* OLTP engine shares, so an extension trait is
//! justified. Query engines (DataFusion, DuckDB, Velox, ...) have no
//! equivalent shared extension yet — they all need the base merge-SQL
//! contract and nothing more. An empty `DlDialect` marker would be trait
//! noise.
//!
//! If a future query-engine concern emerges (e.g. time-travel `AS OF`
//! syntax, statistics-driven hints, engine-specific UDF binding),
//! introduce `DlDialect: Dialect` here, declare the new method on it,
//! and have `DfDialect` impl it. Mirrors the Python side
//! (``packages/penca/src/penca/lib/dl/dialect.py``), which makes the
//! same call.
//!
//! [`Dialect`]: penca_sql::Dialect
//! [`DbDialect`]: penca_db::dialect::DbDialect

use penca_sql::Dialect;

/// DataFusion-specific SQL fragments.
pub struct DfDialect;

impl Dialect for DfDialect {
    fn latest_per_partition(
        select_cols: &[&str],
        inner_from: &str,
        partition_col: &str,
        order_cols: &[&str],
    ) -> String {
        let cols = select_cols
            .iter()
            .map(|c| Self::quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let partition = Self::quote_identifier(partition_col);
        let order = order_cols
            .iter()
            .map(|c| format!("{} DESC", Self::quote_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {cols} FROM (\
               SELECT {cols}, \
               ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {order}) AS rn \
               FROM {inner_from}\
             ) _t WHERE rn = 1"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_wraps_in_double_quotes() {
        assert_eq!(DfDialect::quote_identifier("row_uuid"), "\"row_uuid\"");
    }

    #[test]
    fn quote_identifier_escapes_embedded_quotes() {
        assert_eq!(DfDialect::quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn latest_per_partition_emits_row_number_window() {
        let sql = DfDialect::latest_per_partition(
            &["row_uuid", "commit_micros"],
            "joined",
            "row_uuid",
            &["commit_micros"],
        );
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY \"row_uuid\" ORDER BY \"commit_micros\" DESC)"
            ),
            "missing ROW_NUMBER window: {sql}",
        );
        assert!(sql.contains("WHERE rn = 1"));
        assert!(sql.contains("FROM joined"));
    }

    #[test]
    fn latest_per_partition_composite_order_emits_multiple_desc_keys() {
        // CHA-243: composite tiebreaker — order spans both committed
        // and written timestamps, with `committed` winning if distinct
        // and `written` resolving ties.
        let sql = DfDialect::latest_per_partition(
            &["row_uuid", "commit_micros", "write_seq_num"],
            "joined",
            "row_uuid",
            &["commit_micros", "write_seq_num"],
        );
        assert!(
            sql.contains(
                "ROW_NUMBER() OVER (PARTITION BY \"row_uuid\" ORDER BY \"commit_micros\" DESC, \"write_seq_num\" DESC)"
            ),
            "missing composite ORDER BY: {sql}",
        );
    }
}
