//! Output-stream transforms applied before yielding each batch.

use std::sync::Arc;

use arrow::array::{BooleanArray, new_null_array};
use arrow::compute;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column as PhysColumn, InListExpr, Literal,
};
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use std::collections::HashMap;

use arrow::datatypes::DataType;
use penca_dl::driver::DlDriver;

use crate::MergeError;

/// Project `batch` to `out_schema`'s columns, in order.
///
/// A column in `out_schema` absent from `batch` is null-filled (if
/// nullable). This covers a snapshot-segment cache hit whose entry was decoded
/// against an older table schema (before an `ALTER TABLE ADD COLUMN`): the
/// cached batch lacks the newer column, whose value for those rows is NULL. A
/// non-nullable missing column is a real error.
pub(crate) fn project_to_output(
    batch: &RecordBatch,
    out_schema: &SchemaRef,
) -> Result<RecordBatch, MergeError> {
    let columns: Vec<_> = out_schema
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(idx) => Ok(batch.column(idx).clone()),
            Err(_) if field.is_nullable() => {
                Ok(new_null_array(field.data_type(), batch.num_rows()))
            }
            Err(_) => Err(MergeError::MissingColumn(field.name().to_string())),
        })
        .collect::<Result<Vec<_>, MergeError>>()?;
    Ok(RecordBatch::try_new(out_schema.clone(), columns)?)
}

/// Build the physical predicate for a snapshot-tier filter by running the
/// filter through DataFusion's **full planner once** and extracting the
/// `FilterExec` predicate to reuse per batch.
///
/// The plan runs the full analyzer (incl. TypeCoercion) + optimizer, matching
/// what a per-batch `SELECT * FROM l WHERE {filter}` would produce, without
/// re-planning per batch. The same predicate is also handed to segment
/// pruning, so the two filtering layers cannot diverge.
///
/// `schema` is what the predicate binds against *and* the schema of the
/// batches it later evaluates on (residual: `out_schema`; pruning:
/// `user_schema`). Filter columns are `l.`-qualified to match the merge SQL,
/// so the table is registered as `l`. Planning uses an all-nullable copy of
/// `schema` plus a one-row null batch: a non-empty table keeps the optimizer
/// from folding the scan to an `EmptyExec` (which would drop the `FilterExec`
/// we extract), and relaxing nullability lets the null row satisfy
/// `RecordBatch::try_new` for non-nullable columns. Neither affects the
/// predicate — coercion/lowering depend on column *types*, which are
/// unchanged — and the dummy data is never executed.
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn full_plan_predicate(
    session: &SessionContext,
    filter: &str,
    schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, MergeError> {
    let plan_schema = all_nullable(schema);
    let dummy = RecordBatch::try_new(
        plan_schema.clone(),
        plan_schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), 1))
            .collect(),
    )?;
    // Plan on the session the caller passes (the driver's template-derived
    // session), never a fresh `SessionContext::new()`, so the predicate shares
    // the same function registry + analyzer/optimizer rules as the rest of the
    // cold read.
    let mem = MemTable::try_new(plan_schema.clone(), vec![vec![dummy]])?;
    session.register_table("l", Arc::new(mem))?;
    let plan = session
        .sql(&format!("SELECT * FROM l WHERE {filter}"))
        .await?
        .create_physical_plan()
        .await?;
    if let Some(pred) = find_filter_predicate(&plan) {
        return Ok(pred);
    }
    // No `FilterExec` means the optimizer folded the predicate to a constant:
    // an always-true filter (`1=1`) leaves just the scan → keep every row; an
    // always-false filter (`1=2`) folds to an `EmptyExec` → drop every row.
    // Return the matching constant boolean predicate so per-batch evaluation
    // keeps all / drops all rather than erroring. The 1-row dummy table rules
    // out a scan that is empty for unrelated reasons.
    //
    // INVARIANT (load-bearing): the planning table is a `MemTable`, which
    // reports `TableProviderFilterPushDown::Unsupported`, so a *non-constant*
    // predicate is never absorbed into the scan — it always materializes as a
    // `FilterExec` that `find_filter_predicate` extracts. If the planning
    // provider is ever swapped for one that pushes filters into the scan, a
    // real predicate could leave no `FilterExec` here and be silently treated
    // as keep-all (returning unfiltered rows). Keep `MemTable`.
    let keep_all = !plan_contains_empty_exec(&plan);
    Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(keep_all)))))
}

/// Apply the user filter to a materialized batch as a residual, evaluated
/// through DataFusion's full-plan physical predicate.
///
/// DataFusion is the single user-filter engine. The hot/cold resolve SQL does
/// not splice the user `WHERE`, so the resolved log-tier batch is filtered
/// here instead — with the exact `full_plan_predicate` the snapshot tier
/// applies inside its scan, so the two tiers can never disagree. The caller
/// derives the exclusion set from the *unfiltered* resolved batch before this
/// runs, so the residual only trims the emitted rows.
///
/// Column references resolve against the batch's own schema (registered as
/// `l`), which must carry every column the filter names — the caller's read
/// projection guarantees this (`output ∪ filter` columns, ADR 0023). Reading the
/// schema off `batch` here keeps the "derive schema → apply residual" sequence in
/// one place, so the three residual sites (all-hot, mixed, all-cold) can't
/// drift. No-op when the filter is absent/empty or the batch is empty.
///
/// Fail-fast invariant (ADR 0023): if the projection ever
/// dropped a column the filter references, `full_plan_predicate` cannot bind it
/// and returns a hard planning error here — no sentinel, no keep-all/drop-all
/// fallback that would silently change results. A missing filter column is thus
/// surfaced as a `MergeError`, not swallowed; `residual_fails_fast_on_unprojected_column`
/// locks this. Reachable only if the read projection ever stopped including the
/// filter's columns (it can't today: filters push Inexact, so DataFusion keeps a
/// FilterExec and always projects them).
///
/// One-shot convenience over [`ResidualFilter`] for the merged/cold path, which
/// applies the residual to a single composed batch. The multi-batch all-hot
/// stream must instead [`ResidualFilter::compile`] ONCE and [`ResidualFilter::apply`]
/// per batch — re-planning per batch re-registers the planning table `l` on the
/// shared session and errors on the second batch.
pub async fn apply_resolved_residual(
    session: &SessionContext,
    filter: Option<&str>,
    batch: RecordBatch,
) -> Result<RecordBatch, MergeError> {
    // Preserve the empty-batch short-circuit: no need to plan a predicate for a
    // batch that filters to nothing regardless.
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    ResidualFilter::compile(session, filter, &batch.schema())
        .await?
        .apply(batch)
}

/// A user-filter residual compiled once and applied to many batches.
///
/// `full_plan_predicate` runs the filter through DataFusion's full planner and
/// registers a throwaway planning table `l` on the session. That is a per-read
/// cost, not a per-batch one: the all-hot read streams the resolved delta as
/// several batches and the predicate is identical across them, so planning per
/// batch both wastes work and — because it re-registers `l` on the caller's
/// single derived `SessionContext` — fails with "table l already exists" on the
/// second batch. Compile once, then [`apply`](Self::apply) per batch.
pub struct ResidualFilter {
    /// `None` when the filter is absent/empty — [`apply`](Self::apply) is then a
    /// pass-through.
    predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl ResidualFilter {
    /// Plan the filter into a reusable physical predicate against `schema` (the
    /// schema every batch [`apply`](Self::apply) later evaluates on, registered
    /// as `l`). `None`/empty filter compiles to a pass-through. Fails fast if the
    /// filter names a column absent from `schema` — see
    /// [`apply_resolved_residual`]'s fail-fast invariant.
    pub async fn compile(
        session: &SessionContext,
        filter: Option<&str>,
        schema: &SchemaRef,
    ) -> Result<Self, MergeError> {
        let predicate = match filter {
            Some(fragment) if !fragment.is_empty() => {
                Some(full_plan_predicate(session, fragment, schema).await?)
            }
            _ => None,
        };
        Ok(Self { predicate })
    }

    /// Apply the compiled residual to one batch. Pass-through when the filter was
    /// absent/empty or the batch is empty.
    pub fn apply(&self, batch: RecordBatch) -> Result<RecordBatch, MergeError> {
        let Some(predicate) = &self.predicate else {
            return Ok(batch);
        };
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        let evaluated = predicate.evaluate(&batch)?.into_array(batch.num_rows())?;
        let mask = evaluated
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                MergeError::from(datafusion::error::DataFusionError::Internal(
                    "user filter residual did not evaluate to a boolean mask".to_string(),
                ))
            })?;
        Ok(compute::filter_record_batch(&batch, mask)?)
    }
}

/// Whether the plan contains an `EmptyExec` — i.e. the optimizer proved the
/// filter always-false and replaced the (1-row) scan with no rows.
fn plan_contains_empty_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.as_any().downcast_ref::<EmptyExec>().is_some()
        || plan.children().iter().any(|c| plan_contains_empty_exec(c))
}

/// All-nullable copy of `schema` (same fields, order, and types) for the
/// planning-only `MemTable` in [`full_plan_predicate`].
fn all_nullable(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone().with_nullable(true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Walk a physical plan for the first `FilterExec` and clone its predicate.
fn find_filter_predicate(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn PhysicalExpr>> {
    if let Some(f) = plan.as_any().downcast_ref::<FilterExec>() {
        return Some(Arc::clone(f.predicate()));
    }
    plan.children().into_iter().find_map(find_filter_predicate)
}

/// Per-column equality binding sets extracted from a filter
/// fragment — `col = lit` (both orientations) and non-negated single-column
/// `col IN (lit, …)` conjuncts, unioned per column. Only columns whose Arrow
/// type is in the seek kernel's strict-cast allowlist bind (their literal
/// rendering round-trips `cast(Utf8 → T)` — see
/// `penca_format::index::seek_row_offsets`'s probe-string contract);
/// anything else — floats, timestamps, casts, non-literals — simply never
/// binds, which is always safe (the index is then not covering and the read
/// stays on full-scan + residual). Infallible: a fragment that fails to
/// parse (defensive — the same fragment plans inside `stream_merged`) binds
/// nothing.
#[tracing::instrument(
    level = "trace",
    skip_all,
    fields(filter_len = filter.len(), binding_columns = tracing::field::Empty)
)]
pub async fn equality_bindings<L: DlDriver + ?Sized>(
    dl: &L,
    filter: &str,
    user_schema: &SchemaRef,
) -> HashMap<String, Vec<String>> {
    let session = dl.derive_session();
    let predicate = match full_plan_predicate(&session, filter, user_schema).await {
        Ok(predicate) => predicate,
        Err(error) => {
            // `binding_columns` stays UNRECORDED on this arm — a timed span
            // with no count reads as "aborted", distinct from a successful
            // parse that bound zero columns (which records 0).
            tracing::debug!(%error, "equality_bindings: filter parse failed; no bindings");
            return HashMap::new();
        }
    };
    let bindings = bindings_from_predicate(&predicate, user_schema);
    tracing::Span::current().record("binding_columns", bindings.len());
    bindings
}

/// The pure conjunct walk behind [`equality_bindings`] — split out so unit
/// tests can drive it straight off [`full_plan_predicate`].
fn bindings_from_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    user_schema: &SchemaRef,
) -> HashMap<String, Vec<String>> {
    let mut bindings: HashMap<String, Vec<String>> = HashMap::new();
    for conjunct in split_conjunction(predicate) {
        if let Some(binary) = conjunct.as_any().downcast_ref::<BinaryExpr>() {
            // The optimizer rewrites small IN-lists into OR-of-equalities —
            // a same-column OR disjunct is the IN shape (a mixed-column OR
            // is not a binding and is skipped whole).
            if binary.op() == &Operator::Or {
                if let Some((column, values)) = same_column_or_equalities(conjunct)
                    && probe_type_allowlisted(&column, user_schema)
                {
                    bindings.entry(column).or_default().extend(values);
                }
                continue;
            }

            if binary.op() != &Operator::Eq {
                continue;
            }

            let column_literal = match (
                bare_column_name(binary.left()),
                literal_probe_string(binary.right()),
                bare_column_name(binary.right()),
                literal_probe_string(binary.left()),
            ) {
                (Some(column), Some(value), _, _) => Some((column, value)),
                (_, _, Some(column), Some(value)) => Some((column, value)),
                _ => None,
            };
            if let Some((column, value)) = column_literal
                && probe_type_allowlisted(&column, user_schema)
            {
                bindings.entry(column).or_default().push(value);
            }
        } else if let Some(in_list) = conjunct.as_any().downcast_ref::<InListExpr>() {
            if in_list.negated() {
                continue;
            }

            let Some(column) = bare_column_name(in_list.expr()) else {
                continue;
            };
            if !probe_type_allowlisted(&column, user_schema) {
                continue;
            }

            let values: Option<Vec<String>> =
                in_list.list().iter().map(literal_probe_string).collect();
            if let Some(values) = values {
                bindings.entry(column).or_default().extend(values);
            }
        }
    }

    bindings
}

/// Flatten an OR tree whose every leaf is `col = lit` on ONE common column
/// into that column's IN-style value list — the optimizer's rewrite of a
/// small IN-list. Any other shape (mixed columns, non-equality leaf) is
/// `None`: a disjunction only binds as a whole.
fn same_column_or_equalities(conjunct: &Arc<dyn PhysicalExpr>) -> Option<(String, Vec<String>)> {
    let mut column: Option<String> = None;
    let mut values: Vec<String> = Vec::new();
    let mut stack: Vec<&Arc<dyn PhysicalExpr>> = vec![conjunct];
    while let Some(expr) = stack.pop() {
        let binary = expr.as_any().downcast_ref::<BinaryExpr>()?;
        match binary.op() {
            Operator::Or => {
                stack.push(binary.left());
                stack.push(binary.right());
            }
            Operator::Eq => {
                let (leaf_column, value) = match (
                    bare_column_name(binary.left()),
                    literal_probe_string(binary.right()),
                    bare_column_name(binary.right()),
                    literal_probe_string(binary.left()),
                ) {
                    (Some(c), Some(v), _, _) => (c, v),
                    (_, _, Some(c), Some(v)) => (c, v),
                    _ => return None,
                };
                match &column {
                    None => column = Some(leaf_column),
                    Some(existing) if *existing == leaf_column => {}
                    Some(_) => return None,
                }
                values.push(value);
            }
            _ => return None,
        }
    }
    // Stack pops right-first; restore source order for determinism.
    values.reverse();
    column.map(|column| (column, values))
}

/// The bare column a conjunct side names, or `None` for anything else
/// (casts, functions, nested expressions — all safely non-binding).
fn bare_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    expr.as_any()
        .downcast_ref::<PhysColumn>()
        .map(|column| column.name().to_string())
}

/// A literal's seek-probe string form, or `None` when not a literal or not
/// renderable in the kernel's strict Utf8-cast grammar.
fn literal_probe_string(expr: &Arc<dyn PhysicalExpr>) -> Option<String> {
    let literal = expr.as_any().downcast_ref::<Literal>()?;
    match literal.value() {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        ScalarValue::Int8(Some(value)) => Some(value.to_string()),
        ScalarValue::Int16(Some(value)) => Some(value.to_string()),
        ScalarValue::Int32(Some(value)) => Some(value.to_string()),
        ScalarValue::Int64(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt8(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt16(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt32(Some(value)) => Some(value.to_string()),
        ScalarValue::UInt64(Some(value)) => Some(value.to_string()),
        ScalarValue::Boolean(Some(value)) => Some(value.to_string()),
        _ => None,
    }
}

/// v1 binding-type allowlist — kept beside the extraction so widening it is
/// one edit proving the new type's Utf8-cast round-trip.
fn probe_type_allowlisted(column: &str, user_schema: &SchemaRef) -> bool {
    let Ok(field) = user_schema.field_with_name(column) else {
        return false;
    };
    matches!(
        field.data_type(),
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
    )
}

#[cfg(test)]
mod binding_tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("city", arrow::datatypes::DataType::Utf8, true),
            Field::new("tier", arrow::datatypes::DataType::Utf8, true),
            Field::new("score", arrow::datatypes::DataType::Int64, true),
            Field::new("ratio", arrow::datatypes::DataType::Float64, true),
        ]))
    }

    async fn bindings(filter: &str) -> HashMap<String, Vec<String>> {
        let session = SessionContext::new();
        let schema = schema();
        let predicate = full_plan_predicate(&session, filter, &schema)
            .await
            .expect("fragment must plan");
        bindings_from_predicate(&predicate, &schema)
    }

    #[tokio::test]
    async fn equality_binds_both_orientations() {
        let got = bindings("l.city = 'paris' AND 'gold' = l.tier").await;
        assert_eq!(got["city"], vec!["paris".to_string()]);
        assert_eq!(got["tier"], vec!["gold".to_string()]);
    }

    #[tokio::test]
    async fn in_list_unions_per_column() {
        let got = bindings("l.city IN ('paris', 'oslo')").await;
        assert_eq!(got["city"], vec!["paris".to_string(), "oslo".to_string()]);
    }

    #[tokio::test]
    async fn typed_int_literal_renders_probe_string() {
        let got = bindings("l.score = 10").await;
        assert_eq!(got["score"], vec!["10".to_string()]);
    }

    #[tokio::test]
    async fn non_allowlisted_type_never_binds() {
        // Floats are outside the strict-cast round-trip allowlist.
        assert!(bindings("l.ratio = 1.5").await.is_empty());
    }

    #[tokio::test]
    async fn range_negated_and_non_literal_do_not_bind() {
        assert!(bindings("l.score > 5").await.is_empty());
        assert!(bindings("l.city NOT IN ('paris')").await.is_empty());
        assert!(bindings("l.city = l.tier").await.is_empty());
    }

    /// THE under-selection guard: if a mixed-column disjunction ever bound one
    /// side, the seek would drop the other side's rows and the residual could
    /// never restore them — every other skip merely over-selects.
    #[tokio::test]
    async fn mixed_column_or_binds_nothing() {
        assert!(
            bindings("l.city = 'paris' OR l.tier = 'gold'")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn same_column_explicit_or_binds_as_in() {
        let got = bindings("l.city = 'paris' OR l.city = 'oslo'").await;
        assert_eq!(got["city"], vec!["paris".to_string(), "oslo".to_string()]);
    }

    /// A compiled `ResidualFilter` applies to MANY batches on ONE
    /// session. `full_plan_predicate` registers a planning table
    /// `l`; the all-hot read streams the delta as several batches, so re-planning
    /// per batch would re-register `l` and error ("table l already exists") on the
    /// second batch. `compile` once + `apply` per batch must filter every batch.
    #[tokio::test]
    async fn residual_filter_applies_across_multiple_batches() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "value",
            arrow::datatypes::DataType::Int64,
            true,
        )]));
        let session = SessionContext::new();
        let residual = ResidualFilter::compile(&session, Some("value > 5"), &schema)
            .await
            .expect("filter compiles");
        let batch = |vals: Vec<i64>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::Int64Array::from(vals))],
            )
            .unwrap()
        };
        // Two separate batches through the SAME compiled residual — the second
        // must not error and must filter identically.
        let a = residual.apply(batch(vec![1, 7, 9])).unwrap();
        let b = residual.apply(batch(vec![3, 6, 4])).unwrap();
        assert_eq!(a.num_rows(), 2, "batch A keeps 7 and 9");
        assert_eq!(b.num_rows(), 1, "batch B keeps 6");
    }

    /// Per ADR 0023 the residual must FAIL FAST when the filter
    /// references a column absent from the read projection — never silently keep
    /// or drop rows (which would change results). The read projection includes
    /// the filter's columns by construction (filters push Inexact, so DataFusion
    /// keeps a FilterExec and always projects them); this locks the failure mode
    /// against a future regression that narrowed the projection past the filter.
    #[tokio::test]
    async fn residual_fails_fast_on_unprojected_column() {
        // Batch schema is [city] only; the filter references `score`, which the
        // projection dropped. Non-empty batch + non-empty filter → the residual
        // must plan and thus reject the unbindable column.
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "city",
            arrow::datatypes::DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::StringArray::from(vec!["paris"]))],
        )
        .unwrap();
        let result =
            apply_resolved_residual(&SessionContext::new(), Some("score > 5"), batch).await;
        assert!(
            result.is_err(),
            "residual must fail fast on a filter column absent from the projection, \
             not silently keep/drop rows",
        );
    }
}
