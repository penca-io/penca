//! Content digest of a decoded cold segment (CHA-545).

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::MetadataVersion;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

/// Alignment for the canonical IPC encoding. Pinned rather than defaulted so
/// the digest does not silently change under an arrow-rs upgrade.
const IPC_ALIGNMENT: usize = 8;

/// `xxh3_128` of a segment's **typed in-memory Arrow batch**, as a [`Uuid`].
///
/// The input is the decoded batch, never the encoded file bytes. That is what
/// makes the digest schema-sensitive: two segments referencing the same
/// `(object_uri, offset, length)` slice under different schemas must hash
/// differently, because they are two different decoded objects and a cache
/// keyed by this value would otherwise hand one caller the other's types.
///
/// **Write-time only.** A digest taken before the format writer encodes a batch
/// is *not* guaranteed to equal the digest of a later decode of that file:
/// Parquet may widen a type, re-dictionary-encode, or normalize on round-trip.
/// Dedup never compares the two — the value is computed once at write and
/// inherited by reference copies, never recomputed. CHA-545 names checksum
/// reuse as a possible future use of this digest; that use requires
/// establishing round-trip stability first, and this function does not.
///
/// Changing the normalization, the IPC options, or the hash changes every
/// future digest. That is safe — stored digests are opaque identity, never
/// recomputed and compared against a stored value — but it must stay
/// deliberate, which is why nothing here relies on a library default.
pub fn segment_content_hash(batch: &RecordBatch) -> Result<Uuid, ArrowError> {
    // A batch from `slice()` stays a view onto its parent's buffers, so those
    // buffers physically hold bytes outside the slice. Measured on arrow-rs
    // 57.3: the IPC writer already encodes only the logical range, so for
    // primitive and Utf8 columns this concat is redundant (removing it leaves
    // `slice_hashes_equal_to_an_independently_built_batch` green). It stays
    // because that redundancy is an arrow-rs implementation detail and this
    // digest must be representation-independent for every column type,
    // including the nested ones no test here covers.
    let compacted = arrow::compute::concat_batches(&batch.schema(), std::slice::from_ref(batch))?;

    let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new_with_options(&mut bytes, &compacted.schema(), options)?;
    writer.write(&compacted)?;
    writer.finish()?;
    drop(writer);

    Ok(Uuid::from_u128(xxh3_128(&bytes)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

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
    ///
    /// This currently passes with or without the `concat_batches` normalization
    /// — arrow-rs's IPC writer compacts on its own. It is a guard on the
    /// property, not proof that the normalization is reachable.
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
}
