//! Content digest of a decoded cold segment (CHA-545).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryViewArray, FixedSizeListArray, GenericListArray, LargeListArray,
    ListArray, OffsetSizeTrait, RecordBatch, StringViewArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef};
use arrow::error::ArrowError;
use arrow::ipc::MetadataVersion;
use arrow::ipc::writer::{DictionaryHandling, IpcWriteOptions, StreamWriter};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

/// Alignment for the canonical IPC encoding. Pinned rather than defaulted so
/// the digest does not silently change under an arrow-rs upgrade.
const IPC_ALIGNMENT: usize = 8;

/// `xxh3_128` of a segment's **typed in-memory Arrow batch**, as a [`Uuid`].
///
/// The input is the decoded batch, never the encoded file bytes, so everything
/// that distinguishes two decoded batches is in the digest: values, types, column
/// names, nullability, column order, row count. That is the soundness property a
/// cache keyed by this value needs — equal hash must mean equal decode, or two
/// unrelated segments would share one entry.
///
/// It is **not** a defence against a reader whose schema differs from the
/// writer's. A reference copy inherits this value verbatim (`fork_copy`, snapshot
/// carry-forward) and it is never recomputed, so a fork that `ALTER`s a column
/// still reads the parent's bytes under the parent's hash — no digest could
/// separate the two, because neither row's was ever taken under its own read
/// schema. Serving both from one entry safely is the cached *value's* job: it
/// holds the file-native decode and callers shape after the lookup. See
/// `penca_dl::cache::SegmentCache`.
///
/// **Write-time only.** A digest taken before the format writer encodes a batch
/// is *not* guaranteed to equal the digest of a later decode of that file:
/// Parquet may widen a type, re-dictionary-encode, or normalize on round-trip.
/// Dedup never compares the two — the value is computed once at write and
/// inherited by reference copies, never recomputed. CHA-545 names checksum
/// reuse as a possible future use of this digest; that use requires
/// establishing round-trip stability first, and this function does not.
///
/// **Slice-invariant.** A batch from `slice()` stays a view onto its parent's
/// buffers, which physically hold bytes outside the slice, but the IPC writer
/// encodes only the logical range. Measured on arrow-rs 57.3 across the
/// supported set (see [`crate::types`]): primitives, decimals,
/// dates/times/timestamps, `Boolean` at a non-byte-aligned offset,
/// `Utf8`/`LargeUtf8`/`Binary`, and `List`/`LargeList`/`FixedSizeList` all hold
/// unaided. `Utf8View`/`BinaryView` do not, and [`compact_view_buffers`] below
/// is what makes them. `Dictionary` fails the same way under
/// `DictionaryHandling::Resend` but is rejected at the type boundary and cannot
/// reach here.
///
/// Changing the IPC options or the hash changes every future digest. That is
/// safe — stored digests are opaque identity, never recomputed and compared
/// against a stored value — but it must stay deliberate, which is why nothing
/// here relies on a library default.
pub fn segment_content_hash(batch: &RecordBatch) -> Result<Uuid, ArrowError> {
    let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5)?
        // Pinned for the same reason as the alignment: the default is `Resend`
        // today, and a future flip to `Delta` would make a batch's digest
        // depend on what the writer had already emitted.
        .with_dictionary_handling(DictionaryHandling::Resend);
    let compacted = compact_view_buffers(batch)?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(&mut bytes, &compacted.schema(), options)?;
    writer.write(&compacted)?;
    writer.finish()?;
    drop(writer);

    Ok(Uuid::from_u128(xxh3_128(&bytes)))
}

/// Rebuild every byte-view column so its data buffers hold only the rows the
/// batch covers.
///
/// The IPC writer truncates each buffer it can, with one exception: for
/// `Utf8View`/`BinaryView` it emits every variadic data buffer **whole**, since
/// proving no surviving view still points into a pruned buffer is expensive
/// (arrow-ipc 57.3 `writer::write_array_data` says so and points at `gc`). A
/// digest of a slice would otherwise encode the whole parent's string payload,
/// and both `compact` and the snapshot packer take one digest per slice of a
/// single `concat_batches` result — so a wave of `N` inputs would re-serialize
/// every input's bytes `N` times, on exactly the many-small-segments workload
/// compaction exists for.
///
/// Compacting first bounds each digest to its own rows. It also makes the
/// digest *canonical* for these types rather than merely cheaper: `gc` lays the
/// referenced bytes out in row order in buffers sized by the values alone, so
/// two arrays holding the same values agree even when their parents laid those
/// values out differently — which is the dedup this digest exists for.
///
/// [`CanonicalType`](crate::types::CanonicalType) admits a view at the top level
/// or as the child of a single-level list, and rejects nesting below that, so
/// one level of descent is total over the supported set.
fn compact_view_buffers(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|f| holds_byte_view(f.data_type()))
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .map(compact_column)
        .collect::<Result<Vec<_>, _>>()?;
    // `try_new` re-validates against the schema, which is what catches a
    // compaction that changed a length or a type rather than just a layout.
    RecordBatch::try_new(batch.schema(), columns)
}

/// Whether a column of this type carries variadic data buffers the IPC writer
/// would emit whole.
fn holds_byte_view(dt: &DataType) -> bool {
    match dt {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            matches!(child.data_type(), DataType::Utf8View | DataType::BinaryView)
        }
        _ => false,
    }
}

fn compact_column(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Utf8View => Ok(Arc::new(downcast::<StringViewArray>(array)?.gc())),
        DataType::BinaryView => Ok(Arc::new(downcast::<BinaryViewArray>(array)?.gc())),
        DataType::List(child) if holds_byte_view(array.data_type()) => {
            compact_list(downcast::<ListArray>(array)?, child)
        }
        DataType::LargeList(child) if holds_byte_view(array.data_type()) => {
            compact_list(downcast::<LargeListArray>(array)?, child)
        }
        DataType::FixedSizeList(child, size) if holds_byte_view(array.data_type()) => {
            // Unlike the offset flavours, `FixedSizeListArray::slice` slices its
            // child too, so `values()` is already this array's own range.
            let list = downcast::<FixedSizeListArray>(array)?;
            Ok(Arc::new(FixedSizeListArray::try_new(
                child.clone(),
                *size,
                compact_column(list.values())?,
                list.nulls().cloned(),
            )?))
        }
        _ => Ok(Arc::clone(array)),
    }
}

/// `GenericListArray::slice` keeps the whole child and slices only the offsets,
/// so compacting the child as-is would compact rows this list does not cover.
/// Narrow it to the range the offsets span, then rebase them onto it.
fn compact_list<O: OffsetSizeTrait>(
    list: &GenericListArray<O>,
    child: &FieldRef,
) -> Result<ArrayRef, ArrowError> {
    let offsets = list.offsets();
    let start = offsets[0].as_usize();
    let end = offsets[list.len()].as_usize();
    let values = compact_column(&list.values().slice(start, end - start))?;
    let rebased: Vec<O> = offsets.iter().map(|o| *o - offsets[0]).collect();
    Ok(Arc::new(GenericListArray::<O>::try_new(
        child.clone(),
        OffsetBuffer::new(rebased.into()),
        values,
        list.nulls().cloned(),
    )?))
}

fn downcast<A: Array + 'static>(array: &ArrayRef) -> Result<&A, ArrowError> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "{} column is not the array kind its type declares",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, ListArray, StringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};

    use super::*;

    fn batch(fields: Vec<Field>, columns: Vec<arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid fixture")
    }

    fn i64_col(name: &str, values: Vec<Option<i64>>) -> (Field, arrow::array::ArrayRef) {
        (
            Field::new(name, DataType::Int64, true),
            Arc::new(Int64Array::from(values)),
        )
    }

    fn one_i64(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
        let (f, c) = i64_col(name, values);
        batch(vec![f], vec![c])
    }

    fn hash(b: &RecordBatch) -> Uuid {
        segment_content_hash(b).expect("digest")
    }

    #[test]
    fn identical_batches_hash_equal() {
        let a = one_i64("a", vec![Some(1), Some(2), Some(3)]);
        let b = one_i64("a", vec![Some(1), Some(2), Some(3)]);
        assert_eq!(hash(&a), hash(&a), "same batch hashed twice is stable");
        assert_eq!(
            hash(&a),
            hash(&b),
            "independently built identical batches agree"
        );
    }

    #[test]
    fn differing_values_hash_differently() {
        let a = one_i64("a", vec![Some(1), Some(2), Some(3)]);
        let b = one_i64("a", vec![Some(1), Some(9), Some(3)]);
        assert_ne!(
            hash(&a),
            hash(&b),
            "one differing cell must change the hash"
        );
    }

    #[test]
    fn null_differs_from_value_in_the_same_cell() {
        let a = one_i64("a", vec![Some(1), Some(2)]);
        let b = one_i64("a", vec![Some(1), None]);
        assert_ne!(
            hash(&a),
            hash(&b),
            "null is not the same content as a value"
        );
    }

    /// The schema-divergence case the whole ticket exists for: same column name,
    /// same values as rendered, different type. A byte-level hash of the file
    /// would collide these; hashing the typed batch must not.
    #[test]
    fn same_values_under_different_column_type_hash_differently() {
        let as_int = one_i64("a", vec![Some(1), Some(2)]);
        let as_utf8 = batch(
            vec![Field::new("a", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec![Some("1"), Some("2")]))],
        );
        assert_ne!(
            hash(&as_int),
            hash(&as_utf8),
            "Int64 and Utf8 columns are different decoded objects"
        );
    }

    #[test]
    fn column_name_is_part_of_the_hash() {
        let a = one_i64("a", vec![Some(1), Some(2)]);
        let b = one_i64("b", vec![Some(1), Some(2)]);
        assert_ne!(hash(&a), hash(&b), "column name must change the hash");
    }

    #[test]
    fn nullability_flag_is_part_of_the_hash() {
        let nullable = batch(
            vec![Field::new("a", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        );
        let non_nullable = batch(
            vec![Field::new("a", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
        );
        assert_ne!(
            hash(&nullable),
            hash(&non_nullable),
            "nullability is schema, and schema is part of the identity"
        );
    }

    #[test]
    fn column_order_is_part_of_the_hash() {
        let (fa, ca) = i64_col("a", vec![Some(1), Some(2)]);
        let (fb, cb) = i64_col("b", vec![Some(3), Some(4)]);
        let ab = batch(vec![fa.clone(), fb.clone()], vec![ca.clone(), cb.clone()]);
        let ba = batch(vec![fb, fa], vec![cb, ca]);
        assert_ne!(hash(&ab), hash(&ba), "column order must change the hash");
    }

    #[test]
    fn row_count_is_part_of_the_hash() {
        let empty = one_i64("a", vec![]);
        let one = one_i64("a", vec![Some(1)]);
        let many = one_i64("a", vec![Some(1), Some(2), Some(3)]);
        assert_ne!(hash(&empty), hash(&one));
        assert_ne!(hash(&one), hash(&many));
        assert_eq!(
            hash(&empty),
            hash(&one_i64("a", vec![])),
            "zero rows still hashes deterministically"
        );
    }

    /// A sliced batch and an independently built batch of the same logical rows
    /// must hash equal — the digest is over content, not representation.
    ///
    /// Uses Utf8 because that is where a slice demonstrably stays a *view* — the
    /// guard below asserts it still shares its parent's values buffer, so that
    /// buffer physically holds bytes outside the slice. (`RecordBatch::slice`
    /// reports `offset() == 0` even here, so asserting on the offset would
    /// silently pass on a fixture that proves nothing.)
    #[test]
    fn slice_hashes_equal_to_an_independently_built_batch() {
        let parent = batch(
            vec![Field::new("s", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["aa", "bb", "cc", "dd"]))],
        );
        let sliced = parent.slice(1, 2);
        let standalone = batch(
            vec![Field::new("s", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["bb", "cc"]))],
        );

        assert_eq!(
            parent.column(0).to_data().buffers()[1].as_ptr(),
            sliced.column(0).to_data().buffers()[1].as_ptr(),
            "fixture must still be a view onto the parent's buffer, or this proves nothing"
        );
        assert_eq!(
            hash(&sliced),
            hash(&standalone),
            "the digest is over logical content, not buffer position"
        );
    }

    /// The primitive twin. `slice()` rebases the data pointer here rather than
    /// keeping a logical offset, so this is the cheaper case — asserted anyway
    /// because both paths reach `segment_content_hash` in production.
    #[test]
    fn primitive_slice_hashes_equal_to_an_independently_built_batch() {
        let parent = one_i64("a", vec![Some(1), None, Some(3), Some(4)]);
        assert_eq!(
            hash(&parent.slice(1, 2)),
            hash(&one_i64("a", vec![None, Some(3)])),
            "sliced primitive rows hash as their own content"
        );
    }

    /// The nested twin, and the reason a `List` of a non-view child needs no
    /// compaction step. Slicing a `List` rebases the offsets buffer but leaves
    /// the *child* array whole — the guard below pins that it still holds all
    /// four rows' values, shared with the parent — and the IPC writer still
    /// encodes only the logical range.
    ///
    /// This is the case an earlier revision assumed a `concat_batches` call was
    /// protecting. It was not: `concat` short-circuits single-array input to
    /// `slice(0, len)`, which rebuilds nothing.
    #[test]
    fn list_slice_hashes_equal_to_an_independently_built_batch() {
        fn lists(rows: Vec<Vec<i64>>) -> RecordBatch {
            let arr = ListArray::from_iter_primitive::<Int64Type, _, _>(
                rows.into_iter()
                    .map(|r| Some(r.into_iter().map(Some).collect::<Vec<_>>())),
            );
            let field = Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            );
            batch(vec![field], vec![Arc::new(arr)])
        }

        let parent = lists(vec![vec![1], vec![2, 2], vec![3, 3, 3], vec![4]]);
        let sliced = parent.slice(1, 2);

        let sliced_data = sliced.column(0).to_data();
        let parent_data = parent.column(0).to_data();
        assert_eq!(
            sliced_data.child_data()[0].len(),
            parent_data.child_data()[0].len(),
            "fixture must keep the parent's whole child array, or this proves nothing"
        );
        assert_eq!(
            hash(&sliced),
            hash(&lists(vec![vec![2, 2], vec![3, 3, 3]])),
            "a sliced list column hashes as its own logical rows"
        );
    }

    /// Above the 12-byte inline threshold, so the values live in a data buffer
    /// rather than in the view word itself — which is the only case where a
    /// view array has variadic buffers to carry.
    const LONG: [&str; 3] = [
        "aaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbb",
        "cccccccccccccccccccc",
    ];

    fn views(v: Vec<&str>) -> RecordBatch {
        batch(
            vec![Field::new("s", DataType::Utf8View, true)],
            vec![Arc::new(StringViewArray::from(v))],
        )
    }

    /// The type the IPC writer will *not* truncate: it emits each variadic data
    /// buffer whole rather than prove no surviving view still points into a
    /// pruned one, so a slice's encoding would otherwise carry bytes belonging
    /// to rows it does not cover. [`compact_view_buffers`] is what closes that,
    /// so the fixture guard asserting the slice really does still share the
    /// parent's data buffer is what makes the equality below non-trivial.
    #[test]
    fn sliced_view_column_hashes_equal_to_an_independently_built_batch() {
        let parent = views(LONG.to_vec());
        let sliced = parent.slice(1, 2);

        assert_eq!(
            parent.column(0).to_data().buffers()[1].as_ptr(),
            sliced.column(0).to_data().buffers()[1].as_ptr(),
            "fixture must still share the parent's data buffer, or this proves nothing"
        );
        assert_eq!(
            hash(&sliced),
            hash(&views(vec![LONG[1], LONG[2]])),
            "a sliced view column hashes as its own logical rows"
        );
        // One long value anywhere in the parent gives the array a data buffer,
        // so a slice of only *short* rows carries it too — compaction has to be
        // driven by the parent's layout, not by the sliced rows' lengths.
        assert_eq!(
            hash(&views(vec!["aa", "bb", LONG[0]]).slice(0, 2)),
            hash(&views(vec!["aa", "bb"])),
            "an all-inline slice of a buffer-carrying parent is invariant too"
        );
    }

    /// Independently written segments holding the same rows must agree even when
    /// their parents laid the bytes out differently — that agreement *is* the
    /// dedup. Views make this a real risk rather than a tautology: the same
    /// values reached through different builders can sit at different buffer
    /// offsets, which `gc` normalizes away.
    #[test]
    fn view_column_hashes_equal_across_differently_laid_out_parents() {
        assert_eq!(
            hash(&views(vec![LONG[0], LONG[1]])),
            hash(&views(vec![LONG[2], LONG[0], LONG[1]]).slice(1, 2)),
            "same values, different parent layout, one hash"
        );
    }

    /// The nested case. [`crate::types::CanonicalType`] admits a view as the
    /// child of a single-level list, where slicing leaves *both* the child array
    /// and its data buffers whole — so the compaction has to narrow the child to
    /// the offsets' range before it can help.
    #[test]
    fn sliced_list_of_view_column_hashes_equal_to_an_independently_built_batch() {
        fn list_of_views(rows: Vec<Vec<&str>>) -> RecordBatch {
            let child = Arc::new(Field::new("item", DataType::Utf8View, true));
            let mut values: Vec<&str> = Vec::new();
            let mut offsets: Vec<i32> = vec![0];
            for row in &rows {
                values.extend(row.iter().copied());
                offsets.push(values.len() as i32);
            }
            let arr = ListArray::try_new(
                Arc::clone(&child),
                OffsetBuffer::new(offsets.into()),
                Arc::new(StringViewArray::from(values)),
                None,
            )
            .expect("valid fixture");
            batch(
                vec![Field::new("l", DataType::List(child), true)],
                vec![Arc::new(arr)],
            )
        }

        let parent = list_of_views(vec![
            vec![LONG[0]],
            vec![LONG[1], LONG[2]],
            vec![LONG[0], LONG[2]],
        ]);
        let sliced = parent.slice(1, 1);

        assert_eq!(
            sliced.column(0).to_data().child_data()[0].len(),
            parent.column(0).to_data().child_data()[0].len(),
            "fixture must keep the parent's whole child array, or this proves nothing"
        );
        assert_eq!(
            hash(&sliced),
            hash(&list_of_views(vec![vec![LONG[1], LONG[2]]])),
            "a sliced list-of-view column hashes as its own logical rows"
        );
    }
}
