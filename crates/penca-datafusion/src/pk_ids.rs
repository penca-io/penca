//! Scan-time equality point-lookup extraction (CHA-426 / CHA-492).
//!
//! [`build_seek_batch`] inspects the filter conjunction DataFusion pushes into
//! `PencaTableProvider::scan` and, when every column of a key — the declared
//! primary key, or a defined secondary index's key columns — is pinned to a
//! literal, builds the Arrow IPC batch for `ReadDataRequest.ids` (CHA-398 PK
//! point-lookup) or `ReadDataRequest.indexes` (CHA-492 covering-index seek):
//! the point-lookup restriction the query service resolves below the
//! merge-on-read dedup (`penca-api`'s `pk_batch` kernel; wire-identical to
//! `Change.deletes`).
//!
//! ## Under-return guard — the only dangerous direction
//!
//! Pushdown stays `Inexact`: the full predicate still rides the WHERE
//! fragment and DataFusion's post-filter, so a too-LARGE `ids` set is
//! always trimmed downstream. A too-SMALL set would silently drop rows,
//! so every extraction step is skip-on-doubt (`None` → unrestricted
//! read):
//!
//! - Only bare `Column = Literal` equality (either orientation) and, on
//!   single-PK tables, the multi-row spellings — non-negated
//!   `Column IN (literal, …)` and all-same-column OR-of-equalities
//!   (DataFusion inlines small IN lists into OR chains) — count.
//!   Casts, functions, mixed OR trees, and negations never contribute.
//! - Every declared PK column must be pinned; extra predicates only
//!   narrow further and remain in the residual.
//! - Literals must cast LOSSLESSLY to the column's declared Arrow type
//!   (cast, then round-trip back and compare). The server derives
//!   `row_uuid` from the display string of the declared-type value, so
//!   a lossy literal would mint an identity matching nothing the write
//!   path produced.
//! - Float PK columns are excluded outright: IEEE `-0.0 = 0.0` is
//!   SQL-equal yet the two display differently, so one literal cannot
//!   name both stored identities — the one declared type where
//!   value-equality does not imply same-`row_uuid`.
//! - NULL literals never match under SQL semantics (and the server
//!   rejects null PK ids): skip.
//!
//! Conflicting equalities on one PK column (`pk = 1 AND pk = 2`) take
//! the first — over-return only; the untouched residual trims the
//! result to the correct empty set. Multi-row shapes beyond the
//! single-PK IN list / same-column OR (composite-PK IN,
//! OR-of-conjunctions) are deferred — they skip the pushdown today.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::logical_expr::utils::{split_binary, split_conjunction};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Encode `batch` as Arrow IPC stream bytes — the canonical encoder for
/// PK-batch wire fields (`Change.deletes`, `Change.upserts`,
/// `ReadDataRequest.ids`). `penca-sql-server`'s DML path delegates
/// here so the write side and the scan side cannot drift.
pub fn encode_batch_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Vec::new();
    let mut writer =
        StreamWriter::try_new_with_options(&mut buf, &batch.schema(), IpcWriteOptions::default())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(buf)
}

/// The extracted point-lookup restriction: the encoded
/// `ReadDataRequest.ids` payload plus its row count (for span fields —
/// counts only, PK values are PII-gated out of spans).
#[derive(Debug, PartialEq)]
pub(crate) struct PkIds {
    pub(crate) ipc_bytes: Vec<u8>,
    pub(crate) row_count: usize,
}

/// Build a structured point-lookup seek batch — the `ReadDataRequest.ids`
/// (primary key) or `ReadDataRequest.indexes` (a defined secondary index's key
/// columns) Arrow IPC payload — from the pushed filters, or `None` when the
/// conjunction does not pin every `key_column` to a losslessly-typed literal
/// (the unrestricted read).
///
/// The produced batch carries exactly `key_columns` in the given order with
/// each column's declared Arrow type and no nulls — the shape `penca-api`'s
/// batch validators demand. Shared by the PK-ids extraction and the CHA-492
/// covering-index seek so the two cannot drift.
pub(crate) fn build_seek_batch(
    filters: &[Expr],
    arrow_schema: &SchemaRef,
    key_columns: &[String],
) -> Option<PkIds> {
    if key_columns.is_empty() {
        return None;
    }

    let conjuncts: Vec<&Expr> = filters.iter().flat_map(split_conjunction).collect();
    // IN lists / OR-of-equalities produce multi-row batches; with more
    // than one key column the cross-product shape is deferred, so the
    // multi-row shapes only count for single-column keys (every composite
    // column would need row_count alignment).
    let allow_multi_row = key_columns.len() == 1;

    let mut fields = Vec::with_capacity(key_columns.len());
    let mut columns = Vec::with_capacity(key_columns.len());
    for key in key_columns {
        let declared = arrow_schema.field_with_name(key).ok()?.data_type().clone();
        if matches!(
            declared,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        ) {
            return None;
        }

        let values: Vec<ScalarValue> = pk_literals(&conjuncts, key, allow_multi_row)?
            .into_iter()
            .map(|value| lossless_cast(value, &declared))
            .collect::<Option<_>>()?;
        columns.push(ScalarValue::iter_to_array(values).ok()?);
        fields.push(Field::new(key, declared, false));
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).ok()?;
    let ipc_bytes = encode_batch_ipc(&batch).ok()?;
    Some(PkIds {
        ipc_bytes,
        row_count: batch.num_rows(),
    })
}

/// The literal(s) pinning `pk` in the conjunction: a one-element vec
/// for the first `pk = literal` equality, or the full value list for
/// the first usable multi-row shape — `pk IN (…)` or an all-same-column
/// OR-of-equalities (single-PK tables only; DataFusion's simplifier
/// inlines IN lists of ≤3 items into OR chains before they reach scan,
/// so the disjunctive spelling IS the common point-IN wire shape).
/// `None` when nothing pins the column.
fn pk_literals(conjuncts: &[&Expr], pk: &str, allow_multi_row: bool) -> Option<Vec<ScalarValue>> {
    for expr in conjuncts {
        match expr {
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Eq, ..
            }) => {
                let Some(literal) = eq_literal_for(expr, pk) else {
                    continue;
                };

                return Some(vec![literal.clone()]);
            }
            Expr::BinaryExpr(BinaryExpr {
                op: Operator::Or, ..
            }) if allow_multi_row => {
                // Usable only when EVERY disjunct is `pk = literal` on
                // this same column — one foreign leaf and the disjunction
                // no longer pins the column (skip-on-doubt; another
                // conjunct may still pin it).
                let disjuncts = split_binary(expr, Operator::Or);
                let literals: Option<Vec<ScalarValue>> = disjuncts
                    .iter()
                    .map(|disjunct| eq_literal_for(disjunct, pk).cloned())
                    .collect();
                match literals {
                    Some(values) if !values.is_empty() => return Some(values),
                    _ => continue,
                }
            }
            Expr::InList(in_list) if allow_multi_row && !in_list.negated => {
                let Expr::Column(column) = in_list.expr.as_ref() else {
                    continue;
                };
                if column.name != pk || in_list.list.is_empty() {
                    continue;
                }
                let mut values = Vec::with_capacity(in_list.list.len());
                for item in &in_list.list {
                    let Expr::Literal(literal, _) = item else {
                        return None;
                    };
                    values.push(literal.clone());
                }

                return Some(values);
            }
            _ => continue,
        }
    }

    None
}

/// The literal of a bare `pk = literal` equality (either orientation)
/// on exactly the named column, or `None` for any other shape.
fn eq_literal_for<'a>(expr: &'a Expr, pk: &str) -> Option<&'a ScalarValue> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = expr
    else {
        return None;
    };
    let (column, literal) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), Expr::Literal(literal, _)) => (column, literal),
        (Expr::Literal(literal, _), Expr::Column(column)) => (column, literal),
        _ => return None,
    };
    (column.name == pk).then_some(literal)
}

/// True when EVERY conjunct of `filters` is a [`build_seek_batch`]-consumable
/// equality shape (`col = lit`, non-negated `col IN (lit..)`, or an
/// all-same-column OR-of-equalities) on a column in `seeked_columns` — i.e.,
/// the structured ids/index seeks fully cover the predicate, so `scan` can drop
/// the pushed WHERE fragment (CHA-492: an empty pushed filter is the server's
/// exact-cover signal; the Inexact `FilterExec` re-applies exactness). Empty
/// `filters` returns false — the caller only strips a non-empty predicate.
pub(crate) fn all_conjuncts_seeked(filters: &[Expr], seeked_columns: &[&str]) -> bool {
    let conjuncts: Vec<&Expr> = filters.iter().flat_map(split_conjunction).collect();
    !conjuncts.is_empty()
        && conjuncts
            .iter()
            .all(|conjunct| is_seeked_equality(conjunct, seeked_columns))
}

/// One conjunct is "seeked" when it is a consumable equality shape (mirroring
/// [`pk_literals`]) on a `seeked_columns` member — so "fully covered" agrees
/// with what `build_seek_batch` actually extracted into the seek.
fn is_seeked_equality(expr: &Expr, seeked_columns: &[&str]) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Eq, ..
        }) => seeked_columns
            .iter()
            .any(|column| eq_literal_for(expr, column).is_some()),
        Expr::BinaryExpr(BinaryExpr {
            op: Operator::Or, ..
        }) => {
            let disjuncts = split_binary(expr, Operator::Or);
            !disjuncts.is_empty()
                && seeked_columns.iter().any(|column| {
                    disjuncts
                        .iter()
                        .all(|disjunct| eq_literal_for(disjunct, column).is_some())
                })
        }
        Expr::InList(in_list) if !in_list.negated => {
            !in_list.list.is_empty()
                && matches!(
                    in_list.expr.as_ref(),
                    Expr::Column(column) if seeked_columns.contains(&column.name.as_str())
                )
                && in_list
                    .list
                    .iter()
                    .all(|item| matches!(item, Expr::Literal(_, _)))
        }
        _ => false,
    }
}

/// Cast `value` to `declared` only when the round trip back is
/// identity — the lossless-or-skip under-return guard. `None` on a
/// null literal, a failed cast, a null cast result, or a round-trip
/// mismatch (`1.5 → 1 → 1.0`, `'007' → 7 → '7'`).
fn lossless_cast(value: ScalarValue, declared: &DataType) -> Option<ScalarValue> {
    if value.is_null() {
        return None;
    }
    // Float-typed LITERALS are excluded like float-typed PK columns:
    // SQL evaluates the comparison in the float domain, where distinct
    // integers above 2^53 collapse onto one f64 — such a literal can
    // round-trip through the declared integer type cleanly while the
    // predicate still matches neighbouring integers, an under-return
    // the residual cannot repair.
    if matches!(
        value.data_type(),
        DataType::Float16 | DataType::Float32 | DataType::Float64
    ) {
        return None;
    }
    if value.data_type() == *declared {
        return Some(value);
    }

    let casted = value.cast_to(declared).ok()?;
    if casted.is_null() {
        return None;
    }
    let round_tripped = casted.cast_to(&value.data_type()).ok()?;
    (round_tripped == value).then_some(casted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array as _, Int64Array, StringArray};
    use arrow::ipc::reader::StreamReader;
    use datafusion::logical_expr::{col, lit};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn composite_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn pks(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn decode(ipc: &[u8]) -> RecordBatch {
        let mut batches: Vec<RecordBatch> =
            StreamReader::try_new(std::io::Cursor::new(ipc.to_vec()), None)
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        assert_eq!(batches.len(), 1);
        batches.remove(0)
    }

    fn string_values(batch: &RecordBatch, idx: usize) -> Vec<String> {
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..array.len()).map(|i| array.value(i).into()).collect()
    }

    #[test]
    fn single_eq_exact_type() {
        let ids = build_seek_batch(&[col("name").eq(lit("alice"))], &schema(), &pks(&["name"]))
            .expect("full-PK equality must produce ids");
        assert_eq!(ids.row_count, 1);
        let batch = decode(&ids.ipc_bytes);
        assert_eq!(batch.schema().field(0).name(), "name");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(string_values(&batch, 0), ["alice"]);
    }

    #[test]
    fn literal_on_left_orientation() {
        let ids = build_seek_batch(&[lit("alice").eq(col("name"))], &schema(), &pks(&["name"]))
            .expect("literal = column must extract too");
        assert_eq!(string_values(&decode(&ids.ipc_bytes), 0), ["alice"]);
    }

    #[test]
    fn composite_eq_declared_order_from_one_and_expr() {
        // One combined AND expr, conjuncts in REVERSED declared order —
        // split_conjunction flattens it and the batch comes out in
        // declared (region, name) order.
        let combined = col("name")
            .eq(lit("alice"))
            .and(col("region").eq(lit("eu")));
        let ids = build_seek_batch(&[combined], &composite_schema(), &pks(&["region", "name"]))
            .expect("composite full-PK equality must produce ids");
        assert_eq!(ids.row_count, 1);
        let batch = decode(&ids.ipc_bytes);
        let names: Vec<&str> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["region", "name"]);
        assert_eq!(string_values(&batch, 0), ["eu"]);
        assert_eq!(string_values(&batch, 1), ["alice"]);
    }

    #[test]
    fn partial_composite_coverage_skips() {
        let filters = [col("region").eq(lit("eu"))];
        assert_eq!(
            build_seek_batch(&filters, &composite_schema(), &pks(&["region", "name"])),
            None
        );
    }

    #[test]
    fn non_pk_equality_skips() {
        let filters = [col("value").eq(lit(1_i64))];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn non_equality_op_skips() {
        let filters = [col("name").gt(lit("alice"))];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn null_literal_skips() {
        let filters = [col("name").eq(Expr::Literal(ScalarValue::Utf8(None), None))];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn lossless_widening_cast_allowed() {
        // Int32 literal against an Int64 PK column: widens losslessly,
        // batch carries the DECLARED Int64 type.
        let int_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let filters = [col("id").eq(Expr::Literal(ScalarValue::Int32(Some(7)), None))];
        let ids = build_seek_batch(&filters, &int_schema, &pks(&["id"])).expect("lossless cast");
        let batch = decode(&ids.ipc_bytes);
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(array.value(0), 7);
    }

    #[test]
    fn lossy_fraction_skips() {
        // Decimal 1.5 against an Int64 PK: slips past the float-literal
        // exclusion (not a float type) and must be caught by the cast
        // round trip (1.5 -> 1 -> 1.0) — the numeric pin for that branch
        // now that float literals are excluded earlier.
        let int_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let filters = [col("id").eq(Expr::Literal(ScalarValue::Decimal128(Some(15), 3, 1), None))];
        assert_eq!(build_seek_batch(&filters, &int_schema, &pks(&["id"])), None);
    }

    #[test]
    fn display_recanonicalizing_cast_skips() {
        // '007' parses to 7 but round-trips back as '7' — a row written
        // with PK 7 displays as "7", so '007' would have matched it in
        // SQL only after coercion; the raw-string identity mismatch
        // must skip, not mint a wrong row_uuid.
        let int_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let filters = [col("id").eq(lit("007"))];
        assert_eq!(build_seek_batch(&filters, &int_schema, &pks(&["id"])), None);
    }

    #[test]
    fn float_literal_above_2_53_against_int_pk_skips() {
        // 2^53 + 2 as f64 round-trips through Int64 cleanly, but float-
        // domain SQL equality also matches neighbouring integers — the
        // injectivity hole the float-literal exclusion closes.
        let int_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let filters = [col("id").eq(lit(9_007_199_254_740_994.0_f64))];
        assert_eq!(build_seek_batch(&filters, &int_schema, &pks(&["id"])), None);
    }

    #[test]
    fn pk_missing_from_schema_skips() {
        // Declared PK name absent from the arrow schema (stale or
        // projected metadata reaching the kernel) — skip, don't panic.
        let filters = [col("ghost").eq(lit("x"))];
        assert_eq!(
            build_seek_batch(&filters, &schema(), &pks(&["ghost"])),
            None
        );
    }

    #[test]
    fn in_list_with_non_literal_item_skips() {
        // One non-literal element aborts the whole extraction (None),
        // rather than continuing to a later conjunct.
        let filters = [col("name").in_list(vec![lit("alice"), col("value")], false)];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn float_pk_excluded() {
        let float_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "weight",
            DataType::Float64,
            false,
        )]));
        let filters = [col("weight").eq(lit(1.0_f64))];
        assert_eq!(
            build_seek_batch(&filters, &float_schema, &pks(&["weight"])),
            None
        );
    }

    #[test]
    fn in_list_single_pk_multi_row() {
        let filters = [col("name").in_list(vec![lit("alice"), lit("bob"), lit("carol")], false)];
        let ids = build_seek_batch(&filters, &schema(), &pks(&["name"])).expect("IN list ids");
        assert_eq!(ids.row_count, 3);
        let batch = decode(&ids.ipc_bytes);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(string_values(&batch, 0), ["alice", "bob", "carol"]);
    }

    #[test]
    fn or_of_same_pk_equalities_multi_row() {
        // DataFusion inlines IN lists of <= 3 items into OR chains, so
        // this is the wire shape a small `pk IN (…)` actually reaches
        // scan as.
        let filters = [col("name")
            .eq(lit("alice"))
            .or(col("name").eq(lit("bob")))
            .or(col("name").eq(lit("carol")))];
        let ids =
            build_seek_batch(&filters, &schema(), &pks(&["name"])).expect("same-column OR ids");
        assert_eq!(ids.row_count, 3);
        let batch = decode(&ids.ipc_bytes);
        assert_eq!(string_values(&batch, 0), ["alice", "bob", "carol"]);
    }

    #[test]
    fn cross_column_or_skips() {
        let filters = [col("name").eq(lit("alice")).or(col("value").gt(lit(1_i64)))];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn or_on_composite_pk_skips() {
        // Multi-row shapes are single-PK only; a same-column OR on one
        // column of a composite PK cannot align row counts.
        let filters = [
            col("region").eq(lit("eu")),
            col("name").eq(lit("alice")).or(col("name").eq(lit("bob"))),
        ];
        assert_eq!(
            build_seek_batch(&filters, &composite_schema(), &pks(&["region", "name"])),
            None
        );
    }

    #[test]
    fn negated_in_list_skips() {
        let filters = [col("name").in_list(vec![lit("alice")], true)];
        assert_eq!(build_seek_batch(&filters, &schema(), &pks(&["name"])), None);
    }

    #[test]
    fn in_list_on_composite_pk_skips() {
        let filters = [
            col("region").eq(lit("eu")),
            col("name").in_list(vec![lit("alice"), lit("bob")], false),
        ];
        assert_eq!(
            build_seek_batch(&filters, &composite_schema(), &pks(&["region", "name"])),
            None
        );
    }

    #[test]
    fn conflicting_equalities_first_wins() {
        // `pk = 'alice' AND pk = 'bob'` is unsatisfiable; taking the
        // first is over-return only — the residual trims to the correct
        // empty result.
        let filters = [col("name").eq(lit("alice")), col("name").eq(lit("bob"))];
        let ids = build_seek_batch(&filters, &schema(), &pks(&["name"])).expect("first eq wins");
        assert_eq!(string_values(&decode(&ids.ipc_bytes), 0), ["alice"]);
    }

    #[test]
    fn pk_equality_plus_residual_predicate_extracts() {
        let filters = [col("name").eq(lit("alice")), col("value").gt(lit(1_i64))];
        let ids =
            build_seek_batch(&filters, &schema(), &pks(&["name"])).expect("extra predicates ok");
        assert_eq!(decode(&ids.ipc_bytes).num_rows(), 1);
    }

    #[test]
    fn empty_primary_keys_skips() {
        let filters = [col("name").eq(lit("alice"))];
        assert_eq!(build_seek_batch(&filters, &schema(), &[]), None);
    }

    #[test]
    fn encode_batch_ipc_round_trips() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["alice"]))],
        )
        .unwrap();
        let bytes = encode_batch_ipc(&batch).unwrap();
        assert_eq!(decode(&bytes), batch);
    }

    // all_conjuncts_seeked: the exact-cover strip decision (CHA-492)

    #[test]
    fn all_conjuncts_seeked_true_for_single_equality() {
        assert!(all_conjuncts_seeked(
            &[col("name").eq(lit("alice"))],
            &["name"]
        ));
    }

    #[test]
    fn all_conjuncts_seeked_false_when_one_conjunct_unseeked() {
        // `name = 'alice' AND value > 0`: the value predicate is not a seek, so
        // the pushed filter must NOT be stripped (the seek doesn't cover it).
        let combined = col("name").eq(lit("alice")).and(col("value").gt(lit(0)));
        assert!(!all_conjuncts_seeked(&[combined], &["name"]));
    }

    #[test]
    fn all_conjuncts_seeked_false_when_equality_column_not_seeked() {
        assert!(!all_conjuncts_seeked(&[col("value").eq(lit(1))], &["name"]));
    }

    #[test]
    fn all_conjuncts_seeked_true_for_in_list() {
        assert!(all_conjuncts_seeked(
            &[col("name").in_list(vec![lit("a"), lit("b")], false)],
            &["name"]
        ));
    }

    #[test]
    fn all_conjuncts_seeked_false_for_negated_in_list() {
        assert!(!all_conjuncts_seeked(
            &[col("name").in_list(vec![lit("a")], true)],
            &["name"]
        ));
    }

    #[test]
    fn all_conjuncts_seeked_false_for_empty_filters() {
        // The caller only strips a NON-empty predicate.
        assert!(!all_conjuncts_seeked(&[], &["name"]));
    }
}
