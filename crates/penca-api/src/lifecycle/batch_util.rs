//! Arrow batch utilities shared by persist / snapshot / compact paths:
//! committed-at bounds, hot↔cold schema projection, partition
//! labeling/split, and clustering sort. (Merge-read collection moved
//! out in CHA-404 — the snapshot path consumes
//! `penca_merge::stream_all_cold_parts` directly.)

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{SortColumn, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use penca_dl::stats::ScalarValue;

use crate::error::ApiError;

/// Compute (min, max) of an Int64 column across a batch, by name. Shared
/// by the per-segment bounds stamped at persist time — `commit_micros`
/// (the visibility/time axis) and `commit_seq_num` (CHA-430's commit-order
/// axis). Nulls are skipped; an empty/all-null column yields
/// `(i64::MAX, i64::MIN)` (the caller stamps these onto an empty chunk,
/// which has no rows to select).
fn batch_int64_col_bounds(batch: &RecordBatch, col_name: &str) -> Result<(i64, i64), ApiError> {
    let idx = batch
        .schema()
        .index_of(col_name)
        .map_err(|_| ApiError::InvalidRequest(format!("missing {col_name}")))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ApiError::InvalidRequest(format!("{col_name} not int64")))?;
    let mut min: i64 = i64::MAX;
    let mut max: i64 = i64::MIN;
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            let v = arr.value(i);
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }
    }
    Ok((min, max))
}

/// Compute (min, max) of `commit_micros` across a batch.
pub(super) fn batch_committed_at_bounds(batch: &RecordBatch) -> Result<(i64, i64), ApiError> {
    batch_int64_col_bounds(batch, "commit_micros")
}

/// Compute (min, max) of `commit_seq_num` across a batch (CHA-430). Stamped
/// onto each cold persist segment's metadata so segment selection can
/// later prune on the commit-order (seq) axis — a segment whose
/// `[min, max]` seq range falls outside a requested bound can be skipped
/// whole. The seq-axis read predicate itself lands with the read-surface
/// work in CHA-429; this only records the bound.
pub(super) fn batch_commit_seq_num_bounds(batch: &RecordBatch) -> Result<(i64, i64), ApiError> {
    batch_int64_col_bounds(batch, "commit_seq_num")
}

/// Hot-side upsert read schema: the columns
/// `HotStorageClient::read_committed_upserts` expects to bind to the
/// hot upsert table (`u.<col>`). Order: `version_uuid, row_uuid,
/// tx_uuid, <user_cols>, write_seq_num`. The denormalized tx
/// metadata columns the JOIN adds after this prefix come from `commit_tx_log`
/// (`t.<col>`) and are appended by the hot client itself — not
/// declared here.
///
/// CHA-431: `write_seq_num` is the within-tx mutation ordinal
/// (`write_sequence`-sourced default on `upsert_log`); persist projects
/// it through to cold so the merge-on-read `(commit_seq_num, write_seq_num)`
/// order stays consistent across tiers.
pub(super) fn hot_upsert_read_schema(user_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = vec![
        Arc::new(Field::new("version_uuid", DataType::Utf8, false)),
        Arc::new(Field::new("row_uuid", DataType::Utf8, false)),
        Arc::new(Field::new("tx_uuid", DataType::Utf8, false)),
    ];
    fields.extend(user_schema.fields().iter().cloned());
    // CHA-431: write_seq_num trails the user cols — persist projects it
    // through to cold (via project_to_cold_layout) so the merge's
    // (commit_seq_num, write_seq_num) order works on both tiers.
    fields.push(Arc::new(Field::new(
        "write_seq_num",
        DataType::Int64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

/// CHA-218: project the widened hot-shaped batch (returned by
/// `read_committed_upserts` / `read_committed_deletes`) to the cold
/// on-disk layout. Drops `version_uuid` and `tx_uuid`; keeps every
/// remaining column in its source order. The trailing tx metadata
/// columns (`committed_at, began_at, comment, author` and, CHA-430,
/// `commit_seq_num`) thereby become inline columns of the cold segment row.
pub(super) fn project_to_cold_layout(batch: &RecordBatch) -> Result<RecordBatch, ApiError> {
    let schema = batch.schema();
    let indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != "version_uuid" && f.name() != "tx_uuid")
        .map(|(i, _)| i)
        .collect();
    batch.project(&indices).map_err(ApiError::Arrow)
}

/// Stringified partition label for one row over the partition-key
/// columns: the bare value for a single key, a JSON-encoded list of the
/// stringified parts for composite keys. The single shared definition
/// of partition identity (CHA-404): `partition_record_batch` (delta
/// grouping) and the prior-stream run-grouping must agree on label
/// identity by construction, or a partition's delta and prior rows
/// land in different groups. Label shape is fixed by the column set
/// alone (single column → bare value, several → JSON list) — no
/// separate length parameter a second caller could get wrong.
pub(super) fn partition_label(columns: &[&ArrayRef], row: usize) -> Result<String, ApiError> {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| arrow::util::display::array_value_to_string(*c, row).map_err(ApiError::Arrow))
        .collect::<Result<_, _>>()?;
    if columns.len() == 1 {
        Ok(parts.into_iter().next().unwrap_or_default())
    } else {
        serde_json::to_string(&parts)
            .map_err(|e| ApiError::InvalidRequest(format!("partition label encode: {e}")))
    }
}

/// Derive a prior snapshot segment row's [`PartitionOrderKey`] (typed
/// order + label identity) from its `statistics` blob (CHA-406, ADR 0024
/// §3) — the prior-row identity mechanism: post-CHA-407 the segment table
/// records no partition label, but a label-exact writer leaves every
/// partition-key column constant within a segment row, so `min == max` in
/// its stats IS the partition value. CHA-459 widens the old
/// label-from-statistics to also encode the typed sort row off the SAME
/// 1-row key arrays (via `ordering`), so the carried map can merge in
/// typed order; the `.label()` projection is byte-identical to the old
/// result (same `partition_label` formatter, same scalars).
///
/// Returns:
/// - `Ok(Some(key))` — derivable; each key part is the stats scalar
///   materialized into a 1-row array, formatted by [`partition_label`]
///   (the SAME formatter the writer used) for the label and encoded by
///   `ordering` for the typed order. `partition_keys` empty mirrors
///   [`partition_record_batch`]'s `None` labeling (unused in practice —
///   the carry-forward gate requires non-empty keys — but keeps the type
///   honest).
/// - `Ok(None)` — underivable: `min != max`, missing/malformed/zero-row
///   stats, or a key column absent from them. Never an error: carry-
///   forward is an optimization and the caller falls back to a full
///   rewrite (with a `tracing::warn`).
///
/// All-null key columns (`null_count == row_count`, min/max absent)
/// materialize a 1-slot null array the same way, keeping parity with
/// [`partition_label`] / the converter on null rows.
pub(super) fn partition_order_key_from_statistics(
    ordering: &PartitionOrdering,
    statistics: &[u8],
    partition_keys: &[String],
    user_schema: &SchemaRef,
) -> Result<Option<PartitionOrderKey>, ApiError> {
    if partition_keys.is_empty() {
        return Ok(Some(ordering.order_key_from_key_arrays(&[], None)?));
    }
    // The writer-side batch shape (`row_uuid` + user cols); stats are
    // keyed by column NAME so the prefix is harmless.
    let stats_schema = penca_merge::snapshot_read_schema(user_schema);
    let parsed = penca_dl::stats::parse_segment_statistics(statistics, &stats_schema);
    if parsed.row_count == 0 {
        // Missing, malformed, or zero-row stats all parse to this.
        return Ok(None);
    }

    let mut key_arrays: Vec<ArrayRef> = Vec::with_capacity(partition_keys.len());
    for key in partition_keys {
        let Ok(idx) = stats_schema.index_of(key) else {
            return Ok(None);
        };
        let scalar = match (&parsed.per_column[idx].min, &parsed.per_column[idx].max) {
            (Some(min), Some(max)) if min == max => min.clone(),
            (None, None) if parsed.per_column[idx].null_count == Some(parsed.row_count) => {
                match ScalarValue::try_new_null(stats_schema.field(idx).data_type()) {
                    Ok(null_scalar) => null_scalar,
                    Err(_) => return Ok(None),
                }
            }
            // min != max (multi-valued column) or partial stats.
            _ => return Ok(None),
        };
        match scalar.to_array_of_size(1) {
            Ok(array) => key_arrays.push(array),
            Err(_) => return Ok(None),
        }
    }
    let key_refs: Vec<&ArrayRef> = key_arrays.iter().collect();
    // Degrade a formatting failure to `None` (full rewrite) too, so the
    // "never an error" contract holds end to end — symmetric with the
    // `to_array_of_size` guard above. On a valid 1-row array this is
    // effectively unreachable.
    let Ok(label) = partition_label(&key_refs, 0) else {
        return Ok(None);
    };
    Ok(Some(
        ordering.order_key_from_key_arrays(&key_arrays, Some(label))?,
    ))
}

/// Build the Arrow [`SortField`]s for `keys` from `schema` — default
/// options (ascending, nulls-first), matching `sort_record_batch_by_keys`
/// and the writer's clustering-sort convention so the typed partition
/// order agrees with the on-segment row order. Shared by
/// [`PartitionOrdering`] (and, CHA-459 IMPL2, the packer's clustering-key
/// merge in place of its private `sort_key_fields`).
pub(super) fn partition_sort_fields(
    schema: &SchemaRef,
    keys: &[String],
) -> Result<Vec<SortField>, ApiError> {
    keys.iter()
        .map(|key| {
            let idx = schema.index_of(key).map_err(|_| {
                ApiError::Internal(format!("partition/sort key '{key}' not in schema"))
            })?;
            Ok(SortField::new(schema.field(idx).data_type().clone()))
        })
        .collect()
}

/// The typed ordering key for one snapshot partition (CHA-459). Snapshot
/// partitions are *identified* by their stringified `partition_label`
/// (carry-forward equality + the stored `partition_value`) but must be
/// *ordered* by the typed partition-column value: a stringified-label
/// order is wrong for non-string keys (an `Int` key sorts `"10" < "2"`).
/// `sort_row` is the Arrow row encoding of the partition-key columns
/// (memcmp == typed lexicographic order, ASC nulls-first); `label` trails
/// as a strict-total-order tiebreak and identity carrier. `sort_row` is
/// `None` only for the unpartitioned single-partition table.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct PartitionOrderKey {
    sort_row: Option<OwnedRow>,
    label: Option<String>,
}

impl PartitionOrderKey {
    /// The stringified partition label — partition *identity* (carry-forward
    /// equality, the stored `partition_value`). Ordering is the key's `Ord`;
    /// this is the identity projection.
    pub(super) fn label(&self) -> &Option<String> {
        &self.label
    }
}

/// Typed partition ordering authority: owns one [`RowConverter`] over the
/// partition-key columns and mints [`PartitionOrderKey`]s whose `Ord` is
/// the typed partition order. One converter per snapshot cycle keeps every
/// partition (delta groups, prior-stream runs, carried segments) encoded
/// consistently and therefore mutually comparable.
pub(super) struct PartitionOrdering {
    keys: Vec<String>,
    converter: Option<RowConverter>,
}

impl PartitionOrdering {
    /// Build from the partition-key column types in `schema`. Empty `keys`
    /// (unpartitioned table) → no converter; the single partition orders
    /// trivially.
    pub(super) fn new(schema: &SchemaRef, keys: &[String]) -> Result<Self, ApiError> {
        let converter = if keys.is_empty() {
            None
        } else {
            Some(RowConverter::new(partition_sort_fields(schema, keys)?).map_err(ApiError::Arrow)?)
        };
        Ok(Self {
            keys: keys.to_vec(),
            converter,
        })
    }

    /// The partition-key column names this ordering encodes.
    pub(super) fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Order key from one row's partition-key column arrays (each a
    /// single-row array, in partition-key order), paired with its `label`
    /// identity. With no converter (empty keys) the typed component is
    /// `None`.
    pub(super) fn order_key_from_key_arrays(
        &self,
        key_arrays: &[ArrayRef],
        label: Option<String>,
    ) -> Result<PartitionOrderKey, ApiError> {
        let sort_row = match &self.converter {
            None => None,
            Some(converter) => {
                let rows = converter
                    .convert_columns(key_arrays)
                    .map_err(ApiError::Arrow)?;
                Some(rows.row(0).owned())
            }
        };
        Ok(PartitionOrderKey { sort_row, label })
    }

    /// Order key for `row` of a batch, given its partition-key column
    /// arrays (in partition-key order). Slices each column to the single
    /// row before encoding; `label` is the row's identity label.
    pub(super) fn order_key_at(
        &self,
        key_columns: &[&ArrayRef],
        row: usize,
        label: Option<String>,
    ) -> Result<PartitionOrderKey, ApiError> {
        let one_row: Vec<ArrayRef> = key_columns.iter().map(|c| c.slice(row, 1)).collect();
        self.order_key_from_key_arrays(&one_row, label)
    }
}

/// Split a RecordBatch into partitions by the given key columns.
///
/// Mirrors `lifecycle.py::_partition_table`: one entry per unique key
/// value, labeled by the stringified value (JSON-encoded list for
/// composite keys). Empty `partition_keys` yields a single unpartitioned
/// entry labeled with `None`. CHA-459: groups are emitted in *typed*
/// partition-column order (via [`PartitionOrdering`]), not stringified-label
/// order — so an `Int` key emits `2` before `10`.
pub(super) fn partition_record_batch(
    batch: &RecordBatch,
    partition_keys: &[String],
) -> Result<Vec<(Option<String>, RecordBatch)>, ApiError> {
    if partition_keys.is_empty() {
        return Ok(vec![(None, batch.clone())]);
    }

    let n = batch.num_rows();
    let mut labels: Vec<String> = Vec::with_capacity(n);
    let mut columns: Vec<&ArrayRef> = Vec::with_capacity(partition_keys.len());
    let mut key_indices: Vec<usize> = Vec::with_capacity(partition_keys.len());
    for key in partition_keys {
        let idx = batch.schema().index_of(key).map_err(|_| {
            ApiError::InvalidRequest(format!("partition key '{key}' not in schema"))
        })?;
        key_indices.push(idx);
        columns.push(batch.column(idx));
    }

    for row in 0..n {
        labels.push(partition_label(&columns, row)?);
    }

    // Group row indices by label (partition identity).
    let mut groups: std::collections::BTreeMap<String, Vec<u32>> =
        std::collections::BTreeMap::new();
    for (i, label) in labels.into_iter().enumerate() {
        groups.entry(label).or_default().push(i as u32);
    }

    // Emit groups in typed partition-column order (CHA-459), not the
    // BTreeMap's stringified-label order.
    let ordering = PartitionOrdering::new(&batch.schema(), partition_keys)?;
    let mut out: Vec<(PartitionOrderKey, Option<String>, RecordBatch)> =
        Vec::with_capacity(groups.len());
    for (label, indices) in groups {
        let idx_array = UInt32Array::from(indices);
        let taken_cols: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|c| take(c.as_ref(), &idx_array, None).map_err(ApiError::Arrow))
            .collect::<Result<_, _>>()?;
        let part_batch =
            RecordBatch::try_new(batch.schema(), taken_cols).map_err(ApiError::Arrow)?;
        // The group's partition-key columns are constant, at the same
        // positions as the source batch (`take` preserves column order) —
        // reuse the already-resolved indices, encode row 0.
        let key_arrays: Vec<ArrayRef> = key_indices
            .iter()
            .map(|&idx| part_batch.column(idx).slice(0, 1))
            .collect();
        let order_key = ordering.order_key_from_key_arrays(&key_arrays, Some(label.clone()))?;
        out.push((order_key, Some(label), part_batch));
    }

    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out
        .into_iter()
        .map(|(_, label, part_batch)| (label, part_batch))
        .collect())
}

/// Reorder `batch` so its rows are sorted by `keys` (a table's
/// `clustering_keys`). Returns the batch unchanged when `keys` is empty
/// or the batch has no rows.
///
/// Snapshot generation calls this on each partition before chunking so
/// cold segments come out as contiguous clustering-key ranges. That
/// gives each segment a tight per-column min/max, which is what the
/// snapshot-tier segment pruner (ADR 0022, `prune_segments_by_stats`)
/// needs to skip non-matching segments. Without it segments inherit the
/// merge's `row_uuid` order, so every segment's stats span the full
/// value range and pruning never fires.
pub(super) fn sort_record_batch_by_keys(
    batch: &RecordBatch,
    keys: &[String],
) -> Result<RecordBatch, ApiError> {
    if keys.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let sort_columns =
        keys.iter()
            .map(|key| {
                // `clustering_keys` come from validated table metadata (set at
                // `create_table`), so a key missing from the schema is an internal
                // invariant violation at snapshot time, not a caller error — the
                // caller cannot influence the persisted clustering keys here.
                let idx = batch.schema().index_of(key).map_err(|_| {
                    ApiError::Internal(format!("clustering key '{key}' not in schema"))
                })?;
                Ok(SortColumn {
                    values: batch.column(idx).clone(),
                    options: None,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
    let indices = lexsort_to_indices(&sort_columns, None).map_err(ApiError::Arrow)?;
    let sorted_cols: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None).map_err(ApiError::Arrow))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), sorted_cols).map_err(ApiError::Arrow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    #[test]
    fn partition_single_key_splits_by_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        let parts = partition_record_batch(&batch, &["id".to_string()]).unwrap();
        assert_eq!(parts.len(), 3);
        let labels: Vec<&str> = parts.iter().map(|(l, _)| l.as_deref().unwrap()).collect();
        assert_eq!(labels, vec!["a", "b", "c"]);
        assert_eq!(parts[0].1.num_rows(), 2); // two "a" rows
        assert_eq!(parts[1].1.num_rows(), 1);
        assert_eq!(parts[2].1.num_rows(), 1);
    }

    #[test]
    fn partition_no_keys_returns_whole_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let parts = partition_record_batch(&batch, &[]).unwrap();
        assert_eq!(parts.len(), 1);
        assert!(parts[0].0.is_none());
        assert_eq!(parts[0].1.num_rows(), 3);
    }

    /// CHA-459 Part A: a non-string partition key must group in *typed*
    /// partition-column order, not stringified-label order. For an `Int64`
    /// key with values `{2, 10}`, typed-ascending is `[2, 10]`; the old
    /// `BTreeMap<String, _>` grouping yields the lexicographic `["10", "2"]`
    /// (`'1' < '2'`), which is semantically wrong as an ordering and is the
    /// blocking correctness bug for any future `output_ordering`
    /// advertisement over the partition columns.
    #[test]
    fn partition_int_key_groups_in_typed_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("v", DataType::Int32, false),
        ]));
        // Rows interleaved so insertion order can't accidentally pass.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 2, 100, 9, 2])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let parts = partition_record_batch(&batch, &["pk".to_string()]).unwrap();
        let labels: Vec<&str> = parts.iter().map(|(l, _)| l.as_deref().unwrap()).collect();
        assert_eq!(
            labels,
            vec!["2", "9", "10", "100"],
            "Int partition groups must emit in typed-ascending order, not \
             lexicographic (\"10\" < \"2\" < \"9\")"
        );
    }

    #[test]
    fn sort_by_key_reorders_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("v", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
                Arc::new(Int32Array::from(vec![3, 1, 2])),
            ],
        )
        .unwrap();
        let sorted = sort_record_batch_by_keys(&batch, &["id".to_string()]).unwrap();
        let ids = sorted
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let vs = sorted
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), Some("c")]
        );
        assert_eq!(
            vs.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn sort_no_keys_is_passthrough() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![3, 1, 2]))]).unwrap();
        let out = sort_record_batch_by_keys(&batch, &[]).unwrap();
        let vs = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            vs.iter().collect::<Vec<_>>(),
            vec![Some(3), Some(1), Some(2)]
        );
    }

    #[test]
    fn sort_missing_key_is_internal_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let err = sort_record_batch_by_keys(&batch, &["nope".to_string()]).unwrap_err();
        assert!(matches!(err, ApiError::Internal(_)), "got {err:?}");
    }

    /// CHA-430: both per-segment bounds helpers route through the shared
    /// `batch_int64_col_bounds`, picking their own column by name. Pin
    /// that they read the right column and span its full min/max.
    #[test]
    fn seq_and_committed_bounds_read_their_own_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("commit_micros", DataType::Int64, false),
            Field::new("commit_seq_num", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![300, 100, 200])),
                Arc::new(Int64Array::from(vec![7, 4, 9])),
            ],
        )
        .unwrap();
        assert_eq!(batch_committed_at_bounds(&batch).unwrap(), (100, 300));
        assert_eq!(batch_commit_seq_num_bounds(&batch).unwrap(), (4, 9));
    }

    #[test]
    fn seq_bounds_skip_nulls() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "commit_seq_num",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(5), None, Some(2)]))],
        )
        .unwrap();
        assert_eq!(batch_commit_seq_num_bounds(&batch).unwrap(), (2, 5));
    }

    #[test]
    fn seq_bounds_missing_column_is_invalid_request() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let err = batch_commit_seq_num_bounds(&batch).unwrap_err();
        assert!(matches!(err, ApiError::InvalidRequest(_)), "got {err:?}");
    }
}

/// CHA-406/CHA-459 prior-row order-key-from-statistics derivation. The
/// matrix is spelled out per the helper-testing convention (pure primitive
/// signature, exhaustive cross-product): key column types × {constant
/// value, all-null} × {single key, composite key}, each asserting the
/// derived key's `.label()` equals `partition_label` over the same row —
/// the parity oracle. Plus the underivable degrades (min ≠ max, malformed
/// bytes, empty stats) that drive the full-rewrite fallback.
#[cfg(test)]
mod partition_order_key_from_statistics_tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Float64Array,
        Int16Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt32Array,
    };
    use arrow::datatypes::{TimeUnit, i256};

    /// Wrap one row of user columns into the writer-side batch shape
    /// (`row_uuid` + user cols) — the schema `compute_segment_statistics`
    /// keys its name-addressed stats by, and the schema the helper
    /// parses against.
    fn writer_batch(user_fields: Vec<Field>, user_cols: Vec<ArrayRef>) -> (SchemaRef, RecordBatch) {
        let user_schema: SchemaRef = Arc::new(Schema::new(user_fields));
        let stats_schema = penca_merge::snapshot_read_schema(&user_schema);
        let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["r0"]))];
        cols.extend(user_cols);
        let batch = RecordBatch::try_new(stats_schema, cols).unwrap();
        (user_schema, batch)
    }

    /// Compute stats over a 1-row writer batch, derive the partition
    /// label from them, and assert it matches `partition_label` taken
    /// directly over the same key columns (parity by construction).
    fn assert_parity(user_fields: Vec<Field>, user_cols: Vec<ArrayRef>, partition_keys: &[&str]) {
        let (user_schema, batch) = writer_batch(user_fields, user_cols);
        let stats = penca_dl::stats::compute_segment_statistics(&batch);
        let keys: Vec<String> = partition_keys.iter().map(|s| s.to_string()).collect();

        let ordering = PartitionOrdering::new(&user_schema, &keys).unwrap();
        let derived = partition_order_key_from_statistics(&ordering, &stats, &keys, &user_schema)
            .unwrap()
            .expect("key must be derivable from constant/all-null stats")
            .label()
            .clone()
            .expect("non-empty partition keys → Some label");

        let key_cols: Vec<&ArrayRef> = partition_keys
            .iter()
            .map(|k| batch.column(batch.schema().index_of(k).unwrap()))
            .collect();
        let expected = partition_label(&key_cols, 0).unwrap();
        assert_eq!(
            derived, expected,
            "derived label must equal partition_label over the same row"
        );
    }

    /// One 1-row user column of each matrix type, in both a constant
    /// and an all-null flavor. Returned as `(field, constant_array,
    /// null_array)` so the cross-product below can pair each with the
    /// single/composite key shapes.
    #[allow(clippy::type_complexity)]
    fn type_cases() -> Vec<(Field, ArrayRef, ArrayRef)> {
        vec![
            (
                Field::new("k", DataType::Utf8, true),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ),
            // Quote-bearing string — partition_label JSON-encodes
            // composite parts, so the escaping must round-trip.
            (
                Field::new("k", DataType::Utf8, true),
                Arc::new(StringArray::from(vec!["O'Brien \"Jr\""])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ),
            // Empty string — distinct from NULL.
            (
                Field::new("k", DataType::Utf8, true),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec![Option::<&str>::None])),
            ),
            (
                Field::new("k", DataType::Int64, true),
                Arc::new(Int64Array::from(vec![-42i64])),
                Arc::new(Int64Array::from(vec![Option::<i64>::None])),
            ),
            (
                Field::new("k", DataType::Date32, true),
                Arc::new(Date32Array::from(vec![19_000i32])),
                Arc::new(Date32Array::from(vec![Option::<i32>::None])),
            ),
            (
                Field::new("k", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_000i64,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![Option::<i64>::None])),
            ),
            (
                Field::new("k", DataType::Decimal128(20, 4), true),
                Arc::new(
                    Decimal128Array::from(vec![123_456i128])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Option::<i128>::None])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
            ),
            (
                Field::new("k", DataType::Boolean, true),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![Option::<bool>::None])),
            ),
            // The stats codec round-trips a wider set than the headline
            // types; cover a narrow int, an unsigned int, a float,
            // Date64, and Decimal256 so the parity-by-construction claim
            // is pinned for the formatting-surprise-prone types too.
            (
                Field::new("k", DataType::Int16, true),
                Arc::new(Int16Array::from(vec![-7i16])),
                Arc::new(Int16Array::from(vec![Option::<i16>::None])),
            ),
            (
                Field::new("k", DataType::UInt32, true),
                Arc::new(UInt32Array::from(vec![4_000_000_000u32])),
                Arc::new(UInt32Array::from(vec![Option::<u32>::None])),
            ),
            (
                Field::new("k", DataType::Float64, true),
                Arc::new(Float64Array::from(vec![3.5f64])),
                Arc::new(Float64Array::from(vec![Option::<f64>::None])),
            ),
            (
                Field::new("k", DataType::Date64, true),
                Arc::new(Date64Array::from(vec![1_700_000_000_000i64])),
                Arc::new(Date64Array::from(vec![Option::<i64>::None])),
            ),
            (
                Field::new("k", DataType::Decimal256(40, 6), true),
                Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(123_456_789i128)])
                        .with_precision_and_scale(40, 6)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal256Array::from(vec![Option::<i256>::None])
                        .with_precision_and_scale(40, 6)
                        .unwrap(),
                ),
            ),
        ]
    }

    /// Single partition key, every type × {constant, all-null}.
    #[test]
    fn single_key_parity_across_types() {
        for (field, constant, null) in type_cases() {
            assert_parity(vec![field.clone()], vec![constant], &["k"]);
            assert_parity(vec![field], vec![null], &["k"]);
        }
    }

    /// Composite key `(k, k2)` with `k2` a constant Int64, every first-
    /// key type × {constant, all-null} → JSON-list labels.
    #[test]
    fn composite_key_parity_across_types() {
        for (field, constant, null) in type_cases() {
            let k2_field = Field::new("k2", DataType::Int64, false);
            for first in [constant, null] {
                assert_parity(
                    vec![field.clone(), k2_field.clone()],
                    vec![first, Arc::new(Int64Array::from(vec![7i64]))],
                    &["k", "k2"],
                );
            }
        }
    }

    /// Empty partition keys → a derivable key whose label is `None` (the
    /// unpartitioned position), mirroring `partition_record_batch`.
    #[test]
    fn empty_keys_is_some_none() {
        let (user_schema, batch) = writer_batch(
            vec![Field::new("v", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        );
        let stats = penca_dl::stats::compute_segment_statistics(&batch);
        let ordering = PartitionOrdering::new(&user_schema, &[]).unwrap();
        let key = partition_order_key_from_statistics(&ordering, &stats, &[], &user_schema)
            .unwrap()
            .expect("empty keys are derivable");
        assert_eq!(key.label(), &None);
    }

    /// min ≠ max (a multi-valued column) is underivable → None →
    /// caller full-rewrites.
    #[test]
    fn distinct_min_max_is_underivable() {
        let user_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let stats_schema = penca_merge::snapshot_read_schema(&user_schema);
        let batch = RecordBatch::try_new(
            stats_schema,
            vec![
                Arc::new(StringArray::from(vec!["r0", "r1"])),
                Arc::new(Int64Array::from(vec![1i64, 2i64])),
            ],
        )
        .unwrap();
        let stats = penca_dl::stats::compute_segment_statistics(&batch);
        let ordering = PartitionOrdering::new(&user_schema, &["k".to_string()]).unwrap();
        let out = partition_order_key_from_statistics(
            &ordering,
            &stats,
            &["k".to_string()],
            &user_schema,
        )
        .unwrap();
        assert!(out.is_none(), "min != max must be underivable");
    }

    /// A key column absent from the parsed stats is underivable. (The
    /// ordering is built over a real key — in production the partition
    /// keys are always schema-present; the underivable signal is the stats
    /// lookup, exercised here via the bogus `absent` key.)
    #[test]
    fn missing_key_column_is_underivable() {
        let user_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let (_, batch) = writer_batch(
            vec![Field::new("k", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        );
        let stats = penca_dl::stats::compute_segment_statistics(&batch);
        let ordering = PartitionOrdering::new(&user_schema, &["k".to_string()]).unwrap();
        let out = partition_order_key_from_statistics(
            &ordering,
            &stats,
            &["absent".to_string()],
            &user_schema,
        )
        .unwrap();
        assert!(out.is_none());
    }

    /// Malformed and empty stats both degrade to None (never an error).
    #[test]
    fn malformed_and_empty_stats_are_underivable() {
        let user_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let ordering = PartitionOrdering::new(&user_schema, &["k".to_string()]).unwrap();
        for bytes in [b"not json at all".to_vec(), Vec::new()] {
            let out = partition_order_key_from_statistics(
                &ordering,
                &bytes,
                &["k".to_string()],
                &user_schema,
            )
            .unwrap();
            assert!(out.is_none(), "malformed/empty stats must be underivable");
        }
    }
}
