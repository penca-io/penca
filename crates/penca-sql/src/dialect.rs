//! SQL dialect contract — shared between hot and cold tiers.
//!
//! Every engine that runs penca's merge-on-read SQL implements
//! [`Dialect`]. Identifier quoting and `latest_per_partition` are both
//! minimum-viable requirements — the merge SQL builders in
//! `penca-merge` bound generically over this trait.
//!
//! Dialects with additional engine-specific concerns (DDL, type
//! mapping, engine functions) extend [`Dialect`] with their own
//! sub-trait in the crate that owns that engine — see
//! `penca_db::dialect::DbDialect`.

/// SQL dialect contract.
///
/// Default impls use SQL-standard double-quoting (escape embedded `"`
/// by doubling, then wrap). Dialects with non-standard quoting
/// override `quote_identifier`.
pub trait Dialect {
    /// Quote a table or column name for safe use in SQL.
    fn quote_identifier(name: &str) -> String {
        let escaped = name.replace('"', "\"\"");
        format!("\"{escaped}\"")
    }

    /// Return a qualified column reference (e.g. `alias."col"`).
    fn quote_column(table_alias: &str, column_name: &str) -> String {
        let col = Self::quote_identifier(column_name);
        format!("{table_alias}.{col}")
    }

    /// Emit a `SELECT` that returns the latest row per `partition_col`,
    /// ordered by `order_cols` DESC in left-to-right priority (composite
    /// ordering key).
    ///
    /// `inner_from` is a FROM-clause fragment (bare CTE name, table
    /// name, or parenthesized subquery) that already exposes
    /// `partition_col`, every column in `order_cols`, and every column
    /// in `select_cols`. Column and partition names are quoted by the
    /// implementation.
    ///
    /// CHA-243 → CHA-431: `order_cols` is a slice (not a single string) so
    /// the merge-on-read SQL can break ties on a primary key via a
    /// secondary key (e.g. `(commit_seq_num, write_seq_num)`) without introducing
    /// a parallel `latest_per_partition_composite` helper.
    ///
    /// - **Postgres:** `SELECT DISTINCT ON (partition_col) select_cols FROM inner_from ORDER BY partition_col, order_cols[0] DESC, order_cols[1] DESC, ...`
    /// - **DataFusion:** wraps `inner_from` in a `ROW_NUMBER()` window subquery (PARTITION BY partition_col ORDER BY order_cols[0] DESC, order_cols[1] DESC, ...) and filters `rn = 1`
    fn latest_per_partition(
        select_cols: &[&str],
        inner_from: &str,
        partition_col: &str,
        order_cols: &[&str],
    ) -> String;

    /// Emit a UUID literal of the dialect-native type. Postgres needs
    /// the explicit `::uuid` cast for UNION/JOIN type-checking against
    /// uuid columns; DataFusion stores tx_uuid as Utf8 and accepts a
    /// bare string literal. The default impl is the DataFusion shape;
    /// PgDialect overrides.
    ///
    /// Takes a typed `&uuid::Uuid` rather than a string so callers
    /// can't accidentally interpolate unvalidated user input — the
    /// type itself proves the value parsed correctly. `Display` of
    /// `uuid::Uuid` produces canonical lowercase hyphenated form;
    /// Pg's `::uuid` cast accepts either case but we get
    /// determinism.
    fn uuid_literal(uuid: &uuid::Uuid) -> String {
        format!("'{uuid}'")
    }
}

/// Quote each user column qualified by `alias` (e.g. `u."name"`) and join
/// with ", ". Returns an empty string for an empty slice.
pub fn qualify_user_cols<D: Dialect>(alias: &str, user_cols: &[&str]) -> String {
    user_cols
        .iter()
        .map(|c| D::quote_column(alias, c))
        .collect::<Vec<_>>()
        .join(", ")
}

/// CHA-398 point-lookup restriction clause —
/// `{prefix}{alias}row_uuid IN (<uuid literals>)`, or empty when no
/// restriction is present. `prefix` is the composition keyword
/// (`" WHERE "` / `" AND "`); `alias` includes the trailing dot
/// (`"u."`) or is `""` for a bare column. Literals go through
/// [`Dialect::uuid_literal`] (`'…'::uuid` on Postgres, bare `'…'` on
/// DataFusion, whose `row_uuid` columns are Utf8).
///
/// The list is inlined as literals with no cardinality cap — callers
/// own bounding the batch size (today the gRPC message cap bounds it;
/// large-set strategies are CHA-78's scope).
pub fn row_uuid_in_clause<D: Dialect>(
    row_uuids: Option<&[uuid::Uuid]>,
    prefix: &str,
    alias: &str,
) -> String {
    match row_uuids {
        Some(uuids) if !uuids.is_empty() => {
            let literals: Vec<String> = uuids.iter().map(|u| D::uuid_literal(u)).collect();
            format!("{prefix}{alias}row_uuid IN ({})", literals.join(", "))
        }
        _ => String::new(),
    }
}

/// [`row_uuid_in_clause`] composed after an existing (possibly empty)
/// `WHERE` clause: opens the `WHERE` when `existing_clause` is empty,
/// otherwise `AND`-composes.
pub fn row_uuid_in_clause_after<D: Dialect>(
    row_uuids: Option<&[uuid::Uuid]>,
    existing_clause: &str,
    alias: &str,
) -> String {
    let prefix = if existing_clause.is_empty() {
        " WHERE "
    } else {
        " AND "
    };
    row_uuid_in_clause::<D>(row_uuids, prefix, alias)
}

/// `", "` when `cols` is non-empty, `""` otherwise. Used for splicing a
/// column list into a SELECT projection that already starts with a
/// leading column (e.g. `row_uuid{lead}{user_cols_qualified}`).
pub fn leading_comma_if_nonempty<T>(cols: &[T]) -> &'static str {
    if cols.is_empty() { "" } else { ", " }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StubDialect;
    impl Dialect for StubDialect {
        fn latest_per_partition(
            _select_cols: &[&str],
            _inner_from: &str,
            _partition_col: &str,
            _order_cols: &[&str],
        ) -> String {
            unimplemented!()
        }
    }

    #[test]
    fn qualify_user_cols_empty_slice_returns_empty_string() {
        assert_eq!(qualify_user_cols::<StubDialect>("u", &[]), "");
    }

    #[test]
    fn leading_comma_if_nonempty_matches_emptiness() {
        assert_eq!(leading_comma_if_nonempty::<&str>(&[]), "");
        assert_eq!(leading_comma_if_nonempty(&["x"]), ", ");
    }
}
