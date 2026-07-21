//! CHA-507: cold-tier `audit_data` filter/project + optional tx_log join, on
//! DataFusion.
//!
//! Unifies the cold audit path onto DataFusion (replacing the hand-rolled Arrow
//! filter/project). Registers the cold data segments as `d` and, when the
//! caller asked for tx metadata, the cold `tx_log` segments as `t`, then LEFT
//! JOINs on `commit_seq_num` to reattach `author`/`comment` and projects to the
//! audit output schema. The committed_at / commit_seq_num windows and the
//! `row_uuid` restriction become SQL `WHERE` predicates.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use uuid::Uuid;

use crate::MergeError;

/// Filter + project cold audit rows via DataFusion, reattaching `author` /
/// `comment` from the cold `tx_log` (a `commit_seq_num` LEFT JOIN) when
/// `include_tx_metadata`.
///
/// `ctx` is the caller's session — the driver's template-derived cold session
/// (CHA-421), not a fresh `SessionContext::new()` — so the join shares the same
/// function registry + optimizer rules as the rest of the cold read. The `d` /
/// `t` MemTables are registered into it and it is used once.
///
/// `data_batches` are the cold data-segment rows read against `data_schema`
/// (no author/comment); `tx_log_batches` the cold tx_log rows against
/// `tx_log_schema` (empty when the flag is off). Output rows are canonicalized
/// to `audit_schema` (which is authoritative for column set + order).
///
/// Both inputs are `commit_seq_num`-sorted on disk; declaring that ordering to
/// let the planner pick a sort-merge join is a follow-up (TODO(CHA-509) window)
/// — the join is correct regardless of the physical strategy.
#[allow(clippy::too_many_arguments)]
pub async fn cold_audit_batches(
    ctx: &SessionContext,
    data_batches: Vec<RecordBatch>,
    data_schema: SchemaRef,
    tx_log_batches: Vec<RecordBatch>,
    tx_log_schema: SchemaRef,
    audit_schema: SchemaRef,
    include_tx_metadata: bool,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
    row_uuids: Option<&[Uuid]>,
) -> Result<Vec<RecordBatch>, MergeError> {
    let data = MemTable::try_new(data_schema, vec![data_batches])?;
    ctx.register_table("d", Arc::new(data))?;
    if include_tx_metadata {
        let txlog = MemTable::try_new(tx_log_schema, vec![tx_log_batches])?;
        ctx.register_table("t", Arc::new(txlog))?;
    }

    let sql = build_cold_audit_sql(
        &audit_schema,
        include_tx_metadata,
        committed_from,
        committed_to,
        seq_from,
        seq_to,
        row_uuids,
    );
    let raw = ctx.sql(&sql).await?.collect().await?;

    // Canonicalize to the audit schema: the LEFT JOIN marks author/comment
    // nullable, and DataFusion may qualify column names; re-attaching by
    // position (the SELECT is built in audit_schema order) fixes both.
    raw.into_iter()
        .map(|batch| {
            RecordBatch::try_new(audit_schema.clone(), batch.columns().to_vec())
                .map_err(MergeError::Arrow)
        })
        .collect()
}

/// Build the audit SELECT in `audit_schema` field order: every column comes
/// from the data table `d` except `author`/`comment`, which come from the
/// joined tx_log `t`. Filters are half-open `[from, to)` on `commit_micros` /
/// `commit_seq_num` (matching the hot SQL), plus a `row_uuid IN (...)`
/// restriction. UUIDs and integers are the only interpolated values, so no
/// escaping is needed.
fn build_cold_audit_sql(
    audit_schema: &SchemaRef,
    include_tx_metadata: bool,
    committed_from: Option<i64>,
    committed_to: Option<i64>,
    seq_from: Option<i64>,
    seq_to: Option<i64>,
    row_uuids: Option<&[Uuid]>,
) -> String {
    let select_list = audit_schema
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            let source = if include_tx_metadata && (name == "author" || name == "comment") {
                "t"
            } else {
                "d"
            };
            format!("{source}.\"{name}\" AS \"{name}\"")
        })
        .collect::<Vec<_>>()
        .join(", ");

    let from = if include_tx_metadata {
        "FROM d LEFT JOIN t ON d.\"commit_seq_num\" = t.\"commit_seq_num\""
    } else {
        "FROM d"
    };

    let mut conds: Vec<String> = Vec::new();
    if let Some(v) = committed_from {
        conds.push(format!("d.\"commit_micros\" >= {v}"));
    }
    if let Some(v) = committed_to {
        conds.push(format!("d.\"commit_micros\" < {v}"));
    }
    if let Some(v) = seq_from {
        conds.push(format!("d.\"commit_seq_num\" >= {v}"));
    }
    if let Some(v) = seq_to {
        conds.push(format!("d.\"commit_seq_num\" < {v}"));
    }
    if let Some(uuids) = row_uuids {
        if uuids.is_empty() {
            conds.push("FALSE".to_string());
        } else {
            let list = uuids
                .iter()
                .map(|u| format!("'{u}'"))
                .collect::<Vec<_>>()
                .join(", ");
            conds.push(format!("d.\"row_uuid\" IN ({list})"));
        }
    }

    let where_clause = if conds.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conds.join(" AND "))
    };

    // Deterministic commit-order output: the join does not preserve the inputs'
    // sort, and audit consumers read commit-ordered.
    format!("SELECT {select_list} {from}{where_clause} ORDER BY d.\"commit_seq_num\"")
}

#[cfg(test)]
mod tests {
    use super::build_cold_audit_sql;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use uuid::Uuid;

    fn audit_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("began_at_micros", DataType::Int64, false),
            Field::new("commit_micros", DataType::Int64, false),
            Field::new("write_seq_num", DataType::Int64, false),
            Field::new("comment", DataType::Utf8, true),
            Field::new("author", DataType::Utf8, true),
            Field::new("commit_seq_num", DataType::Int64, false),
        ]))
    }

    #[test]
    fn include_routes_author_comment_to_txlog_and_joins() {
        let sql = build_cold_audit_sql(
            &audit_schema(),
            true,
            Some(100),
            Some(200),
            None,
            None,
            None,
        );
        // author/comment come from the joined tx_log `t`, everything else from `d`.
        assert!(sql.contains("t.\"author\" AS \"author\""));
        assert!(sql.contains("t.\"comment\" AS \"comment\""));
        assert!(sql.contains("d.\"name\" AS \"name\""));
        assert!(sql.contains("d.\"commit_seq_num\" AS \"commit_seq_num\""));
        assert!(sql.contains("LEFT JOIN t ON d.\"commit_seq_num\" = t.\"commit_seq_num\""));
        assert!(sql.contains("d.\"commit_micros\" >= 100"));
        assert!(sql.contains("d.\"commit_micros\" < 200"));
        assert!(sql.trim_end().ends_with("ORDER BY d.\"commit_seq_num\""));
    }

    #[test]
    fn exclude_has_no_join_and_omits_author_comment() {
        let sql = build_cold_audit_sql(&audit_schema(), false, None, None, Some(5), Some(9), None);
        assert!(!sql.contains("LEFT JOIN"));
        assert!(!sql.contains("t.\""));
        // With the flag off the audit schema wouldn't carry author/comment, but
        // even if named they must resolve against `d`, never a missing `t`.
        assert!(sql.contains("d.\"commit_seq_num\" >= 5"));
        assert!(sql.contains("d.\"commit_seq_num\" < 9"));
    }

    #[test]
    fn empty_row_uuids_matches_nothing() {
        let sql = build_cold_audit_sql(&audit_schema(), false, None, None, None, None, Some(&[]));
        assert!(sql.contains("WHERE FALSE"));
    }

    #[test]
    fn row_uuids_render_as_in_list() {
        let u = Uuid::nil();
        let sql = build_cold_audit_sql(&audit_schema(), false, None, None, None, None, Some(&[u]));
        assert!(sql.contains(&format!("d.\"row_uuid\" IN ('{u}')")));
    }
}
