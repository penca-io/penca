//! Per-row byte-width chunker for segment writes.
//!
//! Splits a `RecordBatch` into chunks whose standalone in-memory
//! footprint is at most `max_bytes`, by walking each row's contribution
//! column-by-column. Used by `persist_locked` and `snapshot_locked` to
//! cap fresh cold-segment writes at `LifecycleManager::max_segment_bytes`.

use arrow::array::{
    ArrayRef, BinaryArray, BinaryViewArray, FixedSizeListArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, StringArray, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use penca_core::types::CanonicalType;

use crate::error::ApiError;

/// Per-row standalone in-memory byte footprint for a fixed-width
/// `DataType`. Returns `None` for variable-width or unsupported types.
///
/// The `+1` trailing byte is the per-column-per-row share of the
/// validity bitmap rounded up from 0.125 — overcounts slightly; the
/// cap is a ceiling so a small overshoot is fine.
fn fixed_width_byte_width(dtype: &DataType) -> Option<i64> {
    // Width classification is owned by the canonical type registry, so the
    // chunker and the row codec agree on the supported set by construction.
    // `None` covers both variable-width types (walked per row by
    // `variable_width_row_bytes`) and types outside the canonical set.
    CanonicalType::from_arrow(dtype)
        .ok()
        .and_then(|ct| ct.fixed_width_bytes())
}

/// Standalone in-memory byte footprint of one row in one
/// variable-width column. Surfaces unsupported `DataType`s and
/// downcast failures as `ApiError::InvalidRequest` rather than
/// panicking the lifecycle service.
///
/// The number is the per-row cost a fresh cold-tier reader would pay
/// after re-materializing the array from a single segment file — NOT
/// the shared-buffer cost `Array::get_array_memory_size` returns for a
/// `RecordBatch::slice`, which underreports the post-serialize footprint.
fn variable_width_row_bytes(col: &ArrayRef, idx: usize) -> Result<i64, ApiError> {
    match col.data_type() {
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ApiError::InvalidRequest("Utf8 column is not a StringArray".into())
            })?;
            let offs = arr.value_offsets();
            Ok((offs[idx + 1] - offs[idx]) as i64 + 4 + 1)
        }
        DataType::Binary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                ApiError::InvalidRequest("Binary column is not a BinaryArray".into())
            })?;
            let offs = arr.value_offsets();
            Ok((offs[idx + 1] - offs[idx]) as i64 + 4 + 1)
        }
        DataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest("LargeUtf8 column is not a LargeStringArray".into())
                })?;
            // i64 offsets → 8-byte offset share (+1 validity).
            let offs = arr.value_offsets();
            Ok((offs[idx + 1] - offs[idx]) + 8 + 1)
        }
        DataType::LargeBinary => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest("LargeBinary column is not a LargeBinaryArray".into())
                })?;
            let offs = arr.value_offsets();
            Ok((offs[idx + 1] - offs[idx]) + 8 + 1)
        }
        DataType::Utf8View => {
            let arr = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest("Utf8View column is not a StringViewArray".into())
                })?;
            // No offset buffer; cost the payload plus the 16-byte view
            // struct (+1 validity).
            Ok(arr.value(idx).len() as i64 + 16 + 1)
        }
        DataType::BinaryView => {
            let arr = col
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest("BinaryView column is not a BinaryViewArray".into())
                })?;
            Ok(arr.value(idx).len() as i64 + 16 + 1)
        }
        DataType::List(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| ApiError::InvalidRequest("List column is not a ListArray".into()))?;
            let offs = arr.value_offsets();
            let start = offs[idx] as usize;
            let end = offs[idx + 1] as usize;
            let child = arr.values();
            let child_fixed = fixed_width_byte_width(child.data_type());
            let mut child_sum: i64 = 0;
            for j in start..end {
                child_sum += match child_fixed {
                    Some(w) => w,
                    None => variable_width_row_bytes(child, j)?,
                };
            }
            Ok(child_sum + 4 + 1)
        }
        DataType::LargeList(_) => {
            let arr = col
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest("LargeList column is not a LargeListArray".into())
                })?;
            let offs = arr.value_offsets();
            let start = offs[idx] as usize;
            let end = offs[idx + 1] as usize;
            let child = arr.values();
            let child_fixed = fixed_width_byte_width(child.data_type());
            let mut child_sum: i64 = 0;
            for j in start..end {
                child_sum += match child_fixed {
                    Some(w) => w,
                    None => variable_width_row_bytes(child, j)?,
                };
            }
            // i64 offsets → 8-byte offset share (+1 validity).
            Ok(child_sum + 8 + 1)
        }
        DataType::FixedSizeList(_, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    ApiError::InvalidRequest(
                        "FixedSizeList column is not a FixedSizeListArray".into(),
                    )
                })?;
            // `value(idx)` returns the length-`len` child slice for this
            // logical row, already adjusted for the array's offset — unlike
            // `values()[idx*len..]`, which would index a sliced batch's
            // child buffer from the wrong position.
            let child = arr.value(idx);
            let child_fixed = fixed_width_byte_width(child.data_type());
            let mut child_sum: i64 = 0;
            for j in 0..child.len() {
                child_sum += match child_fixed {
                    Some(w) => w,
                    None => variable_width_row_bytes(&child, j)?,
                };
            }
            // Fixed size → no per-row offset, just the validity byte.
            Ok(child_sum + 1)
        }
        other => Err(ApiError::InvalidRequest(format!(
            "chunker: unsupported column DataType: {other:?}"
        ))),
    }
}

/// Per-row in-memory cost model for a single `RecordBatch`. The
/// fixed-width columns contribute a constant summed once per batch
/// ([`fixed_width_byte_width`]); only the variable-width columns are
/// walked per row ([`variable_width_row_bytes`]).
///
/// Pre-computing the fixed sum once avoids re-costing the same
/// constant on every (row, col) pair. Shared by [`chunk_row_ranges`]
/// (which accumulates per chunk) and [`batch_in_memory_bytes`] (which
/// sums over the whole batch), so the two agree on every row's cost by
/// construction.
struct RowCostModel<'a> {
    fixed_per_row: i64,
    variable_cols: Vec<&'a ArrayRef>,
}

impl<'a> RowCostModel<'a> {
    fn build(batch: &'a RecordBatch) -> Self {
        let mut fixed_per_row: i64 = 0;
        let mut variable_cols: Vec<&ArrayRef> = Vec::new();
        for col in batch.columns() {
            match fixed_width_byte_width(col.data_type()) {
                Some(w) => fixed_per_row += w,
                None => variable_cols.push(col),
            }
        }
        Self {
            fixed_per_row,
            variable_cols,
        }
    }

    /// Standalone in-memory footprint of row `idx` across all columns.
    fn row_bytes(&self, idx: usize) -> Result<i64, ApiError> {
        let mut row_bytes = self.fixed_per_row;
        for col in &self.variable_cols {
            row_bytes += variable_width_row_bytes(col, idx)?;
        }
        Ok(row_bytes)
    }
}

/// Walk the batch row-by-row and pick chunk boundaries so each chunk's
/// standalone in-memory size is at most `max_bytes`. Returns
/// `Vec<(offset, len, in_memory_bytes)>` covering `[0,
/// batch.num_rows())`, where `in_memory_bytes` is the chunk's footprint
/// (equal to [`batch_in_memory_bytes`] of `batch.slice(offset, len)`).
/// A single row whose own size exceeds `max_bytes` becomes its own
/// (oversized) chunk — rows are atomic; the chunker can't split them.
///
/// All call sites pre-guard `num_rows > 0`, so an empty batch
/// short-circuits to an empty `Vec`.
pub(super) fn chunk_row_ranges(
    batch: &RecordBatch,
    max_bytes: i64,
) -> Result<Vec<(usize, usize, i64)>, ApiError> {
    let n_rows = batch.num_rows();
    if n_rows == 0 {
        return Ok(Vec::new());
    }
    let model = RowCostModel::build(batch);
    let mut ranges: Vec<(usize, usize, i64)> = Vec::new();
    let mut chunk_start: usize = 0;
    let mut running: i64 = 0;
    for i in 0..n_rows {
        let row_bytes = model.row_bytes(i)?;
        if running + row_bytes > max_bytes && i > chunk_start {
            ranges.push((chunk_start, i - chunk_start, running));
            chunk_start = i;
            running = 0;
        }
        running += row_bytes;
    }
    ranges.push((chunk_start, n_rows - chunk_start, running));
    Ok(ranges)
}

/// Standalone in-memory Arrow footprint of an entire `RecordBatch`:
/// the sum of every row's per-column cost, over the same byte
/// arithmetic [`chunk_row_ranges`] bounds against. Used by callers
/// that record `size_bytes` without walking row-ranges (the
/// compaction re-point path, where the merged batch never goes
/// through the chunker).
///
/// Fallible so an unsupported `DataType` fails fast rather than being
/// silently undercounted.
pub(super) fn batch_in_memory_bytes(batch: &RecordBatch) -> Result<i64, ApiError> {
    let model = RowCostModel::build(batch);
    let mut total: i64 = 0;
    for i in 0..batch.num_rows() {
        total += model.row_bytes(i)?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, DurationSecondArray, FixedSizeListBuilder,
        Float64Array, Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array,
        LargeListBuilder, LargeStringArray, ListBuilder, StringArray, StringBuilder,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::batch_in_memory_bytes;
    use crate::error::ApiError;

    /// Assemble a `RecordBatch` from `(name, array)` columns, all
    /// nullable. Schema is derived from each array's own `DataType`.
    fn batch_of(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = cols
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = cols.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn int8_bool_uint8_are_two_bytes_per_row() {
        // Int8/UInt8/Boolean → 1 + 1 = 2 B/row.
        let b = batch_of(vec![(
            "c",
            Arc::new(Int8Array::from(vec![1i8, 2, 3])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 3 * 2);

        let b = batch_of(vec![(
            "c",
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 2 * 2);
    }

    #[test]
    fn int16_is_three_bytes_per_row() {
        // Int16/UInt16 → 2 + 1 = 3 B/row.
        let b = batch_of(vec![(
            "c",
            Arc::new(Int16Array::from(vec![1i16, 2, 3, 4])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 4 * 3);
    }

    #[test]
    fn int32_is_five_bytes_per_row() {
        // Int32/UInt32/Float32/Date32/Time32 → 4 + 1 = 5 B/row.
        let b = batch_of(vec![(
            "c",
            Arc::new(Int32Array::from(vec![1i32, 2, 3, 4])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 4 * 5);
    }

    #[test]
    fn int64_and_float64_are_nine_bytes_per_row() {
        // Int64/UInt64/Float64/Date64/Time64/Timestamp → 8 + 1 = 9 B/row.
        let b = batch_of(vec![(
            "c",
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 3 * 9);

        let b = batch_of(vec![(
            "c",
            Arc::new(Float64Array::from(vec![1.0f64, 2.0])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 2 * 9);
    }

    #[test]
    fn utf8_is_payload_plus_five_per_row() {
        // "a"=1+5, "bb"=2+5, "ccc"=3+5 → 6+7+8 = 21.
        let b = batch_of(vec![(
            "c",
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 21);
    }

    #[test]
    fn binary_is_payload_plus_five_per_row() {
        // [1]=1+5, [2,3]=2+5 → 6+7 = 13.
        let vals: Vec<&[u8]> = vec![&[1u8], &[2u8, 3u8]];
        let b = batch_of(vec![("c", Arc::new(BinaryArray::from(vals)) as ArrayRef)]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 13);
    }

    #[test]
    fn list_of_fixed_child_sums_child_plus_five_per_row() {
        // List<Int32>: [[1,2],[3]] → row0 2*5+5=15, row1 1*5+5=10 → 25.
        let mut lb = ListBuilder::new(Int32Builder::new());
        lb.values().append_value(1);
        lb.values().append_value(2);
        lb.append(true);
        lb.values().append_value(3);
        lb.append(true);
        let b = batch_of(vec![("c", Arc::new(lb.finish()) as ArrayRef)]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 25);
    }

    #[test]
    fn list_of_variable_child_recurses_into_child_cost() {
        // List<Utf8>: [["a","bb"]] → child ("a"=6)+("bb"=7)=13, +5 = 18.
        // Exercises the recursive `variable_width_row_bytes` branch.
        let mut lb = ListBuilder::new(StringBuilder::new());
        lb.values().append_value("a");
        lb.values().append_value("bb");
        lb.append(true);
        let b = batch_of(vec![("c", Arc::new(lb.finish()) as ArrayRef)]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 18);
    }

    #[test]
    fn multi_column_sums_fixed_and_variable_per_row() {
        // Utf8 "alice"=10,"bob"=8 + Int64 9,9 → row0 19, row1 17 → 36.
        // Pins that the column sum equals the chunk-walk's
        // `fixed_per_row + sum(variable)`.
        let b = batch_of(vec![
            (
                "name",
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int64Array::from(vec![1i64, 2])) as ArrayRef,
            ),
        ]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 36);
    }

    #[test]
    fn null_entries_still_cost_offset_and_validity() {
        // The footprint does not branch on validity: a null Utf8 row
        // still costs 0 (payload) + 4 (offset) + 1 (validity) = 5, and
        // a null Int64 still costs its full 8 + 1 = 9. Mirrors the
        // chunker's `offs[idx+1] - offs[idx]` / fixed-width arithmetic.
        let names = StringArray::from(vec![Some("ab"), None]); // (2+5)+(0+5)=12
        let vals = Int64Array::from(vec![Some(1i64), None]); // 9 + 9 = 18
        let b = batch_of(vec![
            ("name", Arc::new(names) as ArrayRef),
            ("value", Arc::new(vals) as ArrayRef),
        ]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 12 + 18);
    }

    #[test]
    fn empty_batch_is_zero() {
        let b = batch_of(vec![(
            "c",
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 0);
    }

    #[test]
    fn large_list_sizes_child_plus_eight_offset_per_row() {
        // LargeList<Int32> [1,2,3]: 3 * (4+1) child + 8 (i64 offset) + 1
        // validity = 24.
        let mut b = LargeListBuilder::new(Int32Builder::new());
        b.values().append_value(1);
        b.values().append_value(2);
        b.values().append_value(3);
        b.append(true);
        let batch = batch_of(vec![("c", Arc::new(b.finish()) as ArrayRef)]);
        assert_eq!(batch_in_memory_bytes(&batch).unwrap(), 3 * 5 + 8 + 1);
    }

    #[test]
    fn fixed_size_list_sizing_is_slice_offset_safe() {
        // FixedSizeList<Utf8, 2>, two rows with different string payloads:
        //   row 0 = ["a","b"]        → (1+4+1)*2 + 1 = 13
        //   row 1 = ["xxxx","yyyy"]  → (4+4+1)*2 + 1 = 19
        // Slicing to row 1 must cost row 1's strings (19), not row 0's —
        // guards the offset-aware `value(idx)` against the `idx*len` bug.
        let mut b = FixedSizeListBuilder::new(StringBuilder::new(), 2);
        b.values().append_value("a");
        b.values().append_value("b");
        b.append(true);
        b.values().append_value("xxxx");
        b.values().append_value("yyyy");
        b.append(true);
        let arr: ArrayRef = Arc::new(b.finish());
        let batch = batch_of(vec![("c", arr)]);
        assert_eq!(batch_in_memory_bytes(&batch.slice(0, 1)).unwrap(), 13);
        assert_eq!(batch_in_memory_bytes(&batch.slice(1, 1)).unwrap(), 19);
    }

    #[test]
    fn large_utf8_is_payload_plus_nine_per_row() {
        // LargeUtf8 uses i64 offsets → payload + 8 (offset) + 1 (validity).
        let b = batch_of(vec![(
            "c",
            Arc::new(LargeStringArray::from(vec!["xyz"])) as ArrayRef,
        )]);
        assert_eq!(batch_in_memory_bytes(&b).unwrap(), 3 + 8 + 1);
    }

    #[test]
    fn unsupported_dtype_fails_fast() {
        // Duration is outside the canonical set, so neither
        // `fixed_width_byte_width` nor `variable_width_row_bytes` matches it
        // → InvalidRequest, not a silent 0.
        let b = batch_of(vec![(
            "c",
            Arc::new(DurationSecondArray::from(vec![1i64])) as ArrayRef,
        )]);
        assert!(matches!(
            batch_in_memory_bytes(&b),
            Err(ApiError::InvalidRequest(_))
        ));
    }
}
