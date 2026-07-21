//! DataFusion `Expr` → SQL WHERE fragment translator.
//!
//! Used by `PencaTableProvider::scan` to push predicates down into the
//! query microservice via `ReadDataRequest.filter`. The resulting fragment
//! is consumed by both the hot path (Postgres, `PgDialect`) and the cold
//! path (DataFusion, `DfDialect`) inside `stream_merged`, so the translator
//! restricts itself to a portable operator set that parses identically on
//! both engines.
//!
//! Untranslatable predicates are dropped — the caller reports `Inexact`
//! pushdown so DataFusion keeps a post-filter for correctness.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;

/// AND-combine `filters` and produce a bare SQL WHERE fragment (no leading
/// `WHERE`).
///
/// Returns `None` when `filters` is empty or every filter is untranslatable.
/// Untranslatable individual filters are silently dropped — the caller is
/// expected to set `TableProviderFilterPushDown::Inexact`, so DataFusion
/// keeps a post-filter and correctness is preserved.
///
/// Identifier-case contract: under `PostgreSqlDialect`, the unparser
/// emits **quoted** identifiers (e.g. `"name" = 'alice'`), matching
/// the quoting style `build_merge_resolved` uses for user columns in
/// the outer SQL. DataFusion's SQL parser normalizes unquoted source
/// identifiers to lowercase before they reach this function, so case
/// resolution happens upstream. See `identifiers_are_quoted` for the
/// contract pin.
pub(crate) fn exprs_to_where_fragment(filters: &[Expr]) -> Option<String> {
    let dialect = PostgreSqlDialect {};
    let unparser = Unparser::new(&dialect);

    let parts: Vec<String> = filters
        .iter()
        .filter(|e| is_translatable(e))
        .filter_map(|e| unparser.expr_to_sql(e).ok().map(|s| s.to_string()))
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

/// Shallow predicate check. Mirrors the operator set we know parses safely
/// under both `PgDialect` and `DfDialect`. Used by both the unparse step
/// and `PencaTableProvider::supports_filters_pushdown` so the two stay
/// in sync.
pub(crate) fn is_translatable(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) | Expr::Literal(_, _) => true,
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            is_translatable_op(*op) && is_translatable(left) && is_translatable(right)
        }
        Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::Not(inner)
        | Expr::Negative(inner) => is_translatable(inner),
        Expr::InList(in_list) => {
            is_translatable(&in_list.expr) && in_list.list.iter().all(is_translatable)
        }
        Expr::Like(like) | Expr::SimilarTo(like) => {
            is_translatable(&like.expr) && is_translatable(&like.pattern)
        }
        Expr::Between(b) => {
            is_translatable(&b.expr) && is_translatable(&b.low) && is_translatable(&b.high)
        }
        _ => false,
    }
}

fn is_translatable_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;

    #[test]
    fn empty_filters_returns_none() {
        assert_eq!(exprs_to_where_fragment(&[]), None);
    }

    #[test]
    fn equality_translates() {
        let e = col("name").eq(lit("alice"));
        let sql = exprs_to_where_fragment(&[e]).unwrap();
        assert!(sql.contains("name") && sql.contains("'alice'"));
    }

    #[test]
    fn comparison_operators_translate() {
        for (e, frag) in [
            (col("x").gt(lit(1_i64)), ">"),
            (col("x").gt_eq(lit(1_i64)), ">="),
            (col("x").lt(lit(1_i64)), "<"),
            (col("x").lt_eq(lit(1_i64)), "<="),
            (col("x").not_eq(lit(1_i64)), "<>"),
        ] {
            let sql = exprs_to_where_fragment(&[e]).unwrap();
            assert!(sql.contains(frag), "fragment {sql} missing {frag}");
        }
    }

    #[test]
    fn and_or_combine() {
        let a = col("a").eq(lit(1_i64));
        let b = col("b").eq(lit(2_i64));
        let combined_and = a.clone().and(b.clone());
        let sql_and = exprs_to_where_fragment(&[combined_and]).unwrap();
        assert!(sql_and.contains("AND"));

        let combined_or = a.or(b);
        let sql_or = exprs_to_where_fragment(&[combined_or]).unwrap();
        assert!(sql_or.contains("OR"));
    }

    #[test]
    fn multiple_filters_anded() {
        let a = col("a").eq(lit(1_i64));
        let b = col("b").eq(lit(2_i64));
        let sql = exprs_to_where_fragment(&[a, b]).unwrap();
        assert!(sql.contains("AND"));
    }

    #[test]
    fn is_null_and_is_not_null() {
        let null = Expr::IsNull(Box::new(col("a")));
        assert!(
            exprs_to_where_fragment(&[null])
                .unwrap()
                .contains("IS NULL")
        );
        let not_null = Expr::IsNotNull(Box::new(col("a")));
        assert!(
            exprs_to_where_fragment(&[not_null])
                .unwrap()
                .contains("IS NOT NULL")
        );
    }

    #[test]
    fn in_list_translates() {
        let e = col("status").in_list(vec![lit("active"), lit("pending")], false);
        let sql = exprs_to_where_fragment(&[e]).unwrap();
        assert!(sql.contains("IN") && sql.contains("'active'") && sql.contains("'pending'"));
    }

    #[test]
    fn like_translates() {
        let e = Expr::Like(datafusion::logical_expr::Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(lit("a%")),
            escape_char: None,
            case_insensitive: false,
        });
        let sql = exprs_to_where_fragment(&[e]).unwrap();
        assert!(sql.contains("LIKE") && sql.contains("'a%'"));
    }

    #[test]
    fn untranslatable_filter_is_dropped() {
        // CASE WHEN is not in the translatable set.
        let translatable = col("a").eq(lit(1_i64));
        let untranslatable = Expr::Case(datafusion::logical_expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("b").eq(lit(1_i64))), Box::new(lit(true)))],
            else_expr: Some(Box::new(lit(false))),
        });
        let sql = exprs_to_where_fragment(&[untranslatable.clone(), translatable]).unwrap();
        assert!(sql.contains("a") && sql.contains('1'));

        // All-untranslatable returns None.
        assert_eq!(exprs_to_where_fragment(&[untranslatable]), None);
    }

    #[test]
    fn null_literal_does_not_panic() {
        // Smoke test: untyped NULL literals round-trip through the unparser
        // without panicking. Exact NULL spelling is dialect-specific and
        // isn't part of the contract this function maintains.
        let e = col("a").eq(Expr::Literal(ScalarValue::Utf8(None), None));
        let sql = exprs_to_where_fragment(&[e]).unwrap();
        assert!(sql.contains("a"));
    }

    #[test]
    fn identifiers_are_quoted() {
        // Contract pin: under `PostgreSqlDialect`, the DataFusion unparser
        // emits double-quoted identifiers (`"name" = 'alice'`). This
        // matches the quoting style `build_merge_resolved` uses for user
        // columns in the outer SQL, so the two compose without identifier
        // collisions. DataFusion's SQL parser normalizes unquoted
        // identifiers to lowercase upstream of this function, so whatever
        // reaches the unparser has already been case-resolved.
        let e = col("name").eq(lit("alice"));
        let sql = exprs_to_where_fragment(&[e]).unwrap();
        assert!(
            sql.contains("\"name\""),
            "expected quoted identifier, got {sql}"
        );
    }
}
