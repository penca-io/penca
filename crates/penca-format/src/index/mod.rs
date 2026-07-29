//! Cold-tier index artifact build kernel (CHA-412 / ADR 0026 §2).
//!
//! The per-segment index artifact is the indexed column(s) **sorted**, paired
//! with each base row's **segment-relative** physical position — a flat sorted
//! `(key_0, …, key_{n-1}, row_offset)`, one entry per base row. Duplicate
//! composite keys form a contiguous run; a lookup binary-searches to the first
//! match and scans the equal-tuple run (the seek is CHA-454). `row_offset` is
//! the row's ordinal *within its segment* (`0..len`); the segment's own file
//! `offset` maps it to a file-physical row at read time.
//!
//! The kernel is **index-agnostic** — it sorts ANY key columns. CHA-412 feeds it
//! the single `row_uuid` column; CHA-480 generalizes it to **N-column composite
//! keys** (the sorted `(key…, row_offset)` layout is the contract the build
//! chunks CHA-481/483 and the seek chunk CHA-482 meet at). It is the canonical
//! *binary-searchable* form: CHA-454 loads the artifact through the shared
//! snapshot-segment cache (CHA-252) and binary-searches the sorted composite
//! key — there is no HashMap.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DynComparator, Int64Array, RecordBatch, StringArray, make_comparator,
};
use arrow::compute::{
    CastOptions, SortColumn, SortOptions, cast, cast_with_options, lexsort_to_indices, take,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;

use crate::reader::FormatError;

/// The segment-relative row-ordinal column — always the final column of the
/// artifact, following the key columns.
pub const INDEX_OFFSET_COL: &str = "row_offset";

/// Column name of the `idx`-th key column in an artifact: `key_0`, `key_1`, …,
/// uniformly regardless of arity (a single-column artifact's key is `key_0`).
/// The seek is positional, so these names are documentary; `segment_index_schema`
/// writes them and the cold reader rebuilds the same schema, so build and read
/// always agree.
fn index_key_col(idx: usize) -> String {
    format!("key_{idx}")
}

/// Schema of a per-segment index artifact: `(key_0, …, key_{n-1}, row_offset:
/// Int64)`. `key_types` are the indexed columns' datatypes (one per key column,
/// in sort-priority order); each `key_i` field is **nullable** so the kernel's
/// nulls-last sort stays honest for the generic (CHA-463 user-column) reuse —
/// for CHA-412's non-null `row_uuid` no nulls are present, but the schema must
/// not claim non-null for an arbitrary column. `row_offset` is non-nullable.
pub fn segment_index_schema(key_types: &[DataType]) -> SchemaRef {
    let mut fields: Vec<Field> = key_types
        .iter()
        .enumerate()
        .map(|(idx, key_type)| Field::new(index_key_col(idx), key_type.clone(), true))
        .collect();
    fields.push(Field::new(INDEX_OFFSET_COL, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

/// Build the sorted `(key_0, …, key_{n-1}, row_offset)` artifact for one base
/// segment from its `key_cols` (one array per key column, arity ≥ 1). Rows are
/// sorted ascending lexicographically by the composite key (nulls last),
/// preserving each row's segment-relative ordinal `row_offset` (`0..len`).
/// Duplicate composite keys form a contiguous run. Empty rows yield an empty
/// batch; **zero key columns is a fail-fast error** — no index has zero keys.
pub fn build_segment_index(key_cols: &[ArrayRef]) -> Result<RecordBatch, FormatError> {
    if key_cols.is_empty() {
        return Err(FormatError::Arrow(ArrowError::InvalidArgumentError(
            "build_segment_index requires at least one key column".to_string(),
        )));
    }
    // Every key column indexes the same rows, so they must be the same length.
    // Guard at the boundary with our own message rather than leaning on the
    // internal `lexsort_to_indices` length check.
    let row_count = key_cols[0].len();
    if let Some(col) = key_cols.iter().find(|col| col.len() != row_count) {
        return Err(FormatError::Arrow(ArrowError::InvalidArgumentError(
            format!(
                "build_segment_index key columns must all have the same length \
             (expected {row_count}, found {})",
                col.len()
            ),
        )));
    }
    let sort_columns: Vec<SortColumn> = key_cols
        .iter()
        .map(|col| SortColumn {
            values: Arc::clone(col),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect();
    let indices = lexsort_to_indices(&sort_columns, None)?;
    // Apply the one sort permutation to every key column, in order.
    let mut columns: Vec<ArrayRef> = key_cols
        .iter()
        .map(|col| take(col.as_ref(), &indices, None))
        .collect::<Result<_, ArrowError>>()?;
    // The offsets are the identity `0..len`, so the sort permutation IS the
    // sorted offset column: `sorted_offset[i] = indices[i]` (the original
    // position of the row now at sorted slot `i`). Cast the `UInt32` permutation
    // straight to `Int64` rather than materializing an identity array.
    columns.push(cast(&indices, &DataType::Int64)?);
    let key_types: Vec<DataType> = key_cols.iter().map(|col| col.data_type().clone()).collect();
    Ok(RecordBatch::try_new(
        segment_index_schema(&key_types),
        columns,
    )?)
}

/// Seek the sorted composite artifact (a [`build_segment_index`] output) for
/// each probe **tuple** in `probe_tuples`, returning the matching
/// segment-relative `row_offset`s. Each tuple's arity must be in
/// `1..=num_keys` (= `num_columns - 1`): a full-arity tuple is an exact
/// composite-key seek, and a SHORTER tuple is a leading-PREFIX seek that
/// matches every row whose leading key columns equal the probe (CHA-499). An
/// over-arity tuple, or tuples of differing arity within one call, is a
/// fail-fast error. Binary-searches the lexicographically-sorted composite key
/// to the first match of each tuple, then scans the contiguous
/// equal-(prefix-)run, so a duplicate composite key — or every row sharing a
/// probed prefix — returns every offset in its run. The union across
/// `probe_tuples` is returned **sorted and de-duplicated** (repeated tuples
/// collapse to one offset set).
///
/// Prefix-seek correctness rests entirely on `build_segment_index`'s
/// leading-column-first sort, which makes every row sharing a leading-key
/// prefix contiguous (pinned by `build_composite_leading_column_contiguity`).
///
/// Contract note (CHA-499): "a short probe is a prefix seek" is deliberate and
/// general — this shared primitive does NOT re-guard exact-arity intent. That
/// is safe because every internal seek caller fixes its probe arity at its own
/// resolve boundary from a declared key schema (`resolve_name_seek` enforces
/// the full composite arity; `resolve_prefix_seek` intentionally probes arity-1
/// on a `table_uuid`-leading spec; identity sidecars are single-column, so
/// their arity-1 probes stay exact), so an *accidental* short exact-probe
/// cannot originate internally. The one boundary we cannot constrain — a client
/// `ReadDataRequest.indexes` probe — is where "short = prefix" is simply the
/// reasonable contract (we never guess intent), with the read path's `Inexact`
/// residual as the correctness net. Keeping the contract general is also what
/// lets a future user-query partial-key seek (a `WHERE a = v` over a composite
/// `(a, b)` user index) reuse this exact primitive without a second entry point.
///
/// Keys are compared in the key columns' NATIVE types (CHA-485): each probe
/// element is cast from its string form to the corresponding key column's
/// `DataType` once per call (strict cast — an unrepresentable probe is a
/// fail-fast error, never a silently-never-matching NULL), then the sorted
/// artifact is binary-searched with a typed comparator matching the build's
/// ascending nulls-last order. Utf8 keys behave exactly as before (identity
/// cast); a natively-sorted non-string column (e.g. Int64) is compared
/// numerically — a lexicographic string compare over it would under-select.
///
/// Probe-string contract: each element must be in Arrow's Utf8→`DataType`
/// cast grammar for its key column (plain decimal integers, `true`/`false`
/// booleans, RFC3339 timestamps, …). Callers own that formatting — the
/// CHA-485 planner pass restricts covering-index selection to a type
/// allowlist whose literal renderings round-trip this grammar.
pub fn seek_row_offsets(
    sidecar: &RecordBatch,
    probe_tuples: &[&[&str]],
) -> Result<Vec<i64>, FormatError> {
    // The leading `num_columns - 1` columns are the composite key; the last is
    // `row_offset`. A column-less batch is a malformed sidecar.
    let num_keys = sidecar.num_columns().checked_sub(1).ok_or_else(|| {
        FormatError::Arrow(ArrowError::SchemaError(
            "index sidecar has no columns".to_string(),
        ))
    })?;
    // Validate probe arity at the boundary, before any data access, so a
    // mis-shaped caller is rejected even against a zero-row sidecar. CHA-499:
    // a probe SHORTER than `num_keys` is a leading-PREFIX seek (it matches
    // every row whose leading key columns equal the probe); only an OVER-arity
    // probe or a ragged batch (tuples of differing arity — one comparator set
    // is built for the whole batch, so `probe_arity` must be uniform) is a
    // fail-fast error.
    let Some(probe_arity) = probe_tuples.first().map(|first| first.len()) else {
        return Ok(Vec::new());
    };
    for probe in probe_tuples {
        if probe.len() != probe_arity {
            return Err(FormatError::Arrow(ArrowError::InvalidArgumentError(
                format!(
                    "seek probe tuples have mixed arity ({probe_arity} vs {}); all \
                     tuples in one seek must share arity",
                    probe.len()
                ),
            )));
        }
    }
    if probe_arity == 0 || probe_arity > num_keys {
        return Err(FormatError::Arrow(ArrowError::InvalidArgumentError(
            format!(
                "seek probe arity {probe_arity} must be in 1..={num_keys} \
             (the index key-column count)"
            ),
        )));
    }
    let n = sidecar.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    // Per key column: gather the probes' elements as Utf8 and cast ONCE to the
    // key column's native type, then build a typed comparator between the key
    // column and the cast probe column. `safe: false` makes an unrepresentable
    // probe (e.g. "abc" against an Int64 key) a fail-fast error — the lenient
    // default would yield a NULL probe that silently never matches.
    let strict_cast = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    // Only the leading `probe_arity` key columns are compared: for a full-arity
    // probe this is every key column (exact seek); for a shorter probe it is
    // the leading-prefix. The offset column stays at index `num_keys` (last).
    let mut comparators: Vec<DynComparator> = Vec::with_capacity(probe_arity);
    for key_idx in 0..probe_arity {
        let key_col = sidecar.column(key_idx);
        let probe_vals: StringArray = probe_tuples
            .iter()
            .map(|tuple| Some(tuple[key_idx]))
            .collect();
        let probe_col = cast_with_options(&probe_vals, key_col.data_type(), &strict_cast)?;
        // Ascending nulls-last matches `build_segment_index`'s sort order, so
        // a NULL in any key column compares Greater than every (non-null)
        // probe element and is never matched.
        comparators.push(make_comparator(
            key_col.as_ref(),
            probe_col.as_ref(),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )?);
    }
    let offsets = downcast_col_at::<Int64Array>(sidecar, num_keys)?;

    let mut out = Vec::new();
    for probe_idx in 0..probe_tuples.len() {
        // Lower-bound binary search for the first slot whose composite key is
        // >= the probe tuple.
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp_row_to_probe(&comparators, mid, probe_idx) == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // Scan the contiguous equal-tuple run, collecting every matching offset.
        let mut i = lo;
        while i < n && cmp_row_to_probe(&comparators, i, probe_idx) == Ordering::Equal {
            out.push(offsets.value(i));
            i += 1;
        }
    }
    // True union: distinct composite keys map to distinct runs, so the only
    // sources of a duplicate offset are a repeated probe tuple or distinct
    // probe strings casting to the same native value ("10"/"010" → Int64 10)
    // — collapse them.
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

/// Lexicographically compare the composite key at `row` against the probe
/// tuple at `probe_idx`, one typed comparator per key column (each compares
/// its key column against the cast probe column). Nulls-last ordering rides
/// the comparators' `SortOptions`, matching `build_segment_index`'s sort.
fn cmp_row_to_probe(comparators: &[DynComparator], row: usize, probe_idx: usize) -> Ordering {
    for comparator in comparators {
        match comparator(row, probe_idx) {
            Ordering::Equal => continue,
            non_equal => return non_equal,
        }
    }
    Ordering::Equal
}

/// Downcast the column at `idx` of a sidecar batch to a concrete Arrow array
/// type, erroring if mistyped (always well-typed in practice — the sidecar is a
/// [`segment_index_schema`] artifact — so this is a fail-fast guard, not a
/// recoverable path; today it only guards the `row_offset` column).
fn downcast_col_at<A: Array + 'static>(batch: &RecordBatch, idx: usize) -> Result<&A, FormatError> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| {
            FormatError::Arrow(ArrowError::SchemaError(format!(
                "index sidecar column {idx} is absent or mistyped"
            )))
        })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};

    use super::*;

    fn utf8(vals: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    fn utf8_opt(vals: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(vals.to_vec()))
    }

    fn offsets(batch: &RecordBatch) -> Vec<i64> {
        // `row_offset` is always the final column — index `num_columns - 1` —
        // so this reads it for single-column and composite sidecars alike.
        let col = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..batch.num_rows()).map(|i| col.value(i)).collect()
    }

    fn str_keys(batch: &RecordBatch) -> Vec<String> {
        str_col(batch, 0)
    }

    /// Read string key column `idx` of a sidecar batch positionally (the seek
    /// is positional — the leading `num_columns - 1` columns are the key tuple).
    fn str_col(batch: &RecordBatch, idx: usize) -> Vec<String> {
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| col.value(i).to_string())
            .collect()
    }

    #[test]
    fn empty_input_yields_empty_batch() {
        let batch = build_segment_index(&[utf8(&[])]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), segment_index_schema(&[DataType::Utf8]));
    }

    #[test]
    fn single_row() {
        let batch = build_segment_index(&[utf8(&["a"])]).unwrap();
        assert_eq!(str_keys(&batch), vec!["a"]);
        assert_eq!(offsets(&batch), vec![0]);
    }

    #[test]
    fn already_sorted() {
        let batch = build_segment_index(&[utf8(&["a", "b", "c"])]).unwrap();
        assert_eq!(str_keys(&batch), vec!["a", "b", "c"]);
        assert_eq!(offsets(&batch), vec![0, 1, 2]);
    }

    #[test]
    fn reverse_sorted() {
        let batch = build_segment_index(&[utf8(&["c", "b", "a"])]).unwrap();
        assert_eq!(str_keys(&batch), vec!["a", "b", "c"]);
        // offsets follow their keys: a was at 2, b at 1, c at 0.
        assert_eq!(offsets(&batch), vec![2, 1, 0]);
    }

    #[test]
    fn duplicate_keys_contiguous_runs_with_offsets() {
        // rows: b@0, a@1, b@2, a@3 -> sorted into contiguous a-run then b-run.
        let batch = build_segment_index(&[utf8(&["b", "a", "b", "a"])]).unwrap();
        assert_eq!(
            str_keys(&batch),
            vec!["a", "a", "b", "b"],
            "duplicate keys must form contiguous runs"
        );
        // Each key's offset set matches the original mapping (intra-run order
        // is unspecified, so compare as sets).
        let keys = str_keys(&batch);
        let offs = offsets(&batch);
        let a: BTreeSet<i64> = keys
            .iter()
            .zip(&offs)
            .filter(|(k, _)| k.as_str() == "a")
            .map(|(_, o)| *o)
            .collect();
        let b: BTreeSet<i64> = keys
            .iter()
            .zip(&offs)
            .filter(|(k, _)| k.as_str() == "b")
            .map(|(_, o)| *o)
            .collect();
        assert_eq!(a, BTreeSet::from([1, 3]));
        assert_eq!(b, BTreeSet::from([0, 2]));
    }

    #[test]
    fn non_utf8_int_key_is_supported() {
        // Shared-kernel genericity (CHA-463): an Int key column sorts the same.
        let key: ArrayRef = Arc::new(Int32Array::from(vec![30, 10, 20]));
        let batch = build_segment_index(&[key]).unwrap();
        assert_eq!(batch.schema(), segment_index_schema(&[DataType::Int32]));
        let sorted = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..batch.num_rows())
                .map(|i| sorted.value(i))
                .collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(offsets(&batch), vec![1, 2, 0]);
    }

    // CHA-454 R1: seek_row_offsets (the read/seek side)

    fn sorted(mut v: Vec<i64>) -> Vec<i64> {
        v.sort_unstable();
        v
    }

    #[test]
    fn seek_hit_returns_offset() {
        // a@0, b@1, c@2 — seek "b" lands on row_offset 1.
        let batch = build_segment_index(&[utf8(&["a", "b", "c"])]).unwrap();
        assert_eq!(seek_row_offsets(&batch, &[&["b"]]).unwrap(), vec![1]);
    }

    #[test]
    fn seek_miss_returns_empty() {
        let batch = build_segment_index(&[utf8(&["a", "b", "c"])]).unwrap();
        assert!(
            seek_row_offsets(&batch, &[&["z"]]).unwrap().is_empty(),
            "an absent key contributes no offset",
        );
    }

    #[test]
    fn seek_multi_key_unions_offsets() {
        // a@0, b@1, c@2, d@3 — seek {c, a} unions to {0, 2}.
        let batch = build_segment_index(&[utf8(&["a", "b", "c", "d"])]).unwrap();
        assert_eq!(
            sorted(seek_row_offsets(&batch, &[&["c"], &["a"]]).unwrap()),
            vec![0, 2]
        );
    }

    #[test]
    fn seek_duplicate_key_returns_full_run() {
        // rows b@0, a@1, b@2, a@3 -> sorted a,a,b,b; seek "a" must return BOTH
        // offsets of the equal-key run (scan the run, not just the first hit).
        let batch = build_segment_index(&[utf8(&["b", "a", "b", "a"])]).unwrap();
        assert_eq!(
            sorted(seek_row_offsets(&batch, &[&["a"]]).unwrap()),
            vec![1, 3],
            "every offset in the equal-key run must be returned",
        );
    }

    #[test]
    fn seek_empty_inputs_return_empty() {
        let empty = build_segment_index(&[utf8(&[])]).unwrap();
        assert!(seek_row_offsets(&empty, &[&["a"]]).unwrap().is_empty());
        let one = build_segment_index(&[utf8(&["a"])]).unwrap();
        assert!(seek_row_offsets(&one, &[]).unwrap().is_empty());
    }

    #[test]
    fn seek_repeated_probe_key_dedups() {
        // A repeated probe key must not double-count its offset (union, not
        // multiset) — else a duplicate `ids` entry would emit a duplicate row.
        let batch = build_segment_index(&[utf8(&["a", "b", "c"])]).unwrap();
        assert_eq!(
            seek_row_offsets(&batch, &[&["b"], &["b"]]).unwrap(),
            vec![1]
        );
    }

    // CHA-480: composite multi-column keys (build + seek)
    // The sorted `(key_0 … key_{n-1}, row_offset)` layout is the interface
    // contract the build chunks (CHA-481/483) and the seek chunk (CHA-482)
    // meet at. These cases lock it for N = 1, 2, 3 columns.

    #[test]
    fn build_composite_2col_lexicographic_sort() {
        // rows (schema_uuid, table_name): (s2,t1)@0, (s1,t2)@1, (s1,t1)@2.
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // lexicographic ascending: (s1,t1)@2, (s1,t2)@1, (s2,t1)@0.
        assert_eq!(str_col(&batch, 0), vec!["s1", "s1", "s2"]);
        assert_eq!(str_col(&batch, 1), vec!["t1", "t2", "t1"]);
        assert_eq!(offsets(&batch), vec![2, 1, 0]);
    }

    #[test]
    fn build_composite_3col_lexicographic_sort() {
        // rows (a,b,c): (a1,b1,c2)@0, (a1,b1,c1)@1, (a1,b2,c1)@2, (a0,b9,c9)@3.
        let batch = build_segment_index(&[
            utf8(&["a1", "a1", "a1", "a0"]),
            utf8(&["b1", "b1", "b2", "b9"]),
            utf8(&["c2", "c1", "c1", "c9"]),
        ])
        .unwrap();
        // ascending: (a0,b9,c9)@3, (a1,b1,c1)@1, (a1,b1,c2)@0, (a1,b2,c1)@2.
        assert_eq!(str_col(&batch, 0), vec!["a0", "a1", "a1", "a1"]);
        assert_eq!(str_col(&batch, 1), vec!["b9", "b1", "b1", "b2"]);
        assert_eq!(str_col(&batch, 2), vec!["c9", "c1", "c2", "c1"]);
        assert_eq!(offsets(&batch), vec![3, 1, 0, 2]);
    }

    #[test]
    fn build_composite_duplicate_tuple_contiguous_run() {
        // Two rows share the full tuple (s1,t1): @0 and @1; (s1,t2)@2.
        let batch =
            build_segment_index(&[utf8(&["s1", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // The (s1,t1) run is contiguous; intra-run order unspecified, compare set.
        let k0 = str_col(&batch, 0);
        let k1 = str_col(&batch, 1);
        let offs = offsets(&batch);
        let s1t1: BTreeSet<i64> = (0..batch.num_rows())
            .filter(|&i| k0[i] == "s1" && k1[i] == "t1")
            .map(|i| offs[i])
            .collect();
        assert_eq!(s1t1, BTreeSet::from([0, 2]));
    }

    #[test]
    fn seek_composite_2col_hit() {
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        let probes: &[&[&str]] = &[&["s1", "t2"]];
        assert_eq!(seek_row_offsets(&batch, probes).unwrap(), vec![1]);
    }

    #[test]
    fn seek_composite_3col_hit_and_miss() {
        let batch = build_segment_index(&[
            utf8(&["a1", "a1", "a1", "a0"]),
            utf8(&["b1", "b1", "b2", "b9"]),
            utf8(&["c2", "c1", "c1", "c9"]),
        ])
        .unwrap();
        let hit: &[&[&str]] = &[&["a1", "b1", "c1"]];
        assert_eq!(seek_row_offsets(&batch, hit).unwrap(), vec![1]);
        // a1 exists and b9/c9 exist in OTHER rows, but not paired under a1.
        let miss: &[&[&str]] = &[&["a1", "b9", "c9"]];
        assert!(seek_row_offsets(&batch, miss).unwrap().is_empty());
    }

    #[test]
    fn seek_composite_tuple_in_list_unions() {
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // IN-list of tuples: (s1,t2)@1 ∪ (s2,t1)@0 → {0,1} sorted+deduped.
        let probes: &[&[&str]] = &[&["s1", "t2"], &["s2", "t1"]];
        assert_eq!(
            sorted(seek_row_offsets(&batch, probes).unwrap()),
            vec![0, 1]
        );
    }

    #[test]
    fn seek_composite_no_match_partial_prefix() {
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // key_0 present but key_1 absent → a prefix match is NOT a hit.
        let absent_tail: &[&[&str]] = &[&["s1", "t9"]];
        assert!(seek_row_offsets(&batch, absent_tail).unwrap().is_empty());
        // key_0 absent entirely.
        let absent_head: &[&[&str]] = &[&["s9", "t1"]];
        assert!(seek_row_offsets(&batch, absent_head).unwrap().is_empty());
    }

    #[test]
    fn seek_composite_duplicate_tuple_full_run() {
        // (s1,t1) appears at @0 and @2 → seek must return BOTH offsets.
        let batch =
            build_segment_index(&[utf8(&["s1", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        let probes: &[&[&str]] = &[&["s1", "t1"]];
        assert_eq!(
            sorted(seek_row_offsets(&batch, probes).unwrap()),
            vec![0, 2]
        );
    }

    #[test]
    fn seek_composite_repeated_probe_dedups() {
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // Same tuple twice must not double-count (union, not multiset).
        let probes: &[&[&str]] = &[&["s1", "t2"], &["s1", "t2"]];
        assert_eq!(seek_row_offsets(&batch, probes).unwrap(), vec![1]);
    }

    #[test]
    fn seek_arity_mismatch_is_err() {
        let batch =
            build_segment_index(&[utf8(&["s2", "s1", "s1"]), utf8(&["t1", "t2", "t1"])]).unwrap();
        // 2-column sidecar: an OVER-arity (3) probe tuple is fail-fast Err.
        // (A 1-arity probe is now a valid leading-prefix seek — CHA-499, covered
        // by the seek_prefix_* tests.)
        let too_long: &[&[&str]] = &[&["s1", "t1", "x"]];
        assert!(seek_row_offsets(&batch, too_long).is_err());
    }

    #[test]
    fn build_empty_key_cols_is_err() {
        // No index has zero key columns — fail fast rather than emit a
        // degenerate offset-only sidecar.
        let no_cols: &[ArrayRef] = &[];
        assert!(build_segment_index(no_cols).is_err());
    }

    #[test]
    fn build_mismatched_key_col_lengths_is_err() {
        // Key columns index the same rows, so unequal lengths are a fail-fast
        // error rather than a silently-truncated or panicking sort.
        let ragged: &[ArrayRef] = &[utf8(&["a", "b"]), utf8(&["x"])];
        assert!(build_segment_index(ragged).is_err());
    }

    #[test]
    fn schema_column_names_are_positional() {
        // Key columns are named `key_0`, `key_1`, … uniformly regardless of arity
        // (a single-column artifact's key is `key_0`); `row_offset` is last. The
        // seek is positional, so these names are documentary.
        let single = segment_index_schema(&[DataType::Utf8]);
        assert_eq!(single.field(0).name(), "key_0");
        assert_eq!(single.field(1).name(), "row_offset");
        let composite = segment_index_schema(&[DataType::Utf8, DataType::Utf8]);
        assert_eq!(composite.field(0).name(), "key_0");
        assert_eq!(composite.field(1).name(), "key_1");
        assert_eq!(composite.field(2).name(), "row_offset");
        // The built artifact carries the same names end-to-end.
        let built = build_segment_index(&[utf8(&["a"])]).unwrap();
        assert_eq!(built.schema().field(0).name(), "key_0");
    }

    #[test]
    fn seek_composite_nulls_last_and_never_matched() {
        // Exercises cmp_row_to_probe's null branch — net-new vs the non-null
        // row_uuid index. A null in any key column sorts last (matching the
        // build's nulls_first:false) and is never matched by a non-null probe.
        let batch = build_segment_index(&[
            utf8_opt(&[Some("s1"), None, Some("s1")]),
            utf8_opt(&[Some("t1"), Some("t2"), None]),
        ])
        .unwrap();
        // Sorted nulls-last: (s1,t1)@0, (s1,null)@2, (null,t2)@1 — the null key_0
        // row sorts last, the null key_1 row sorts after its non-null sibling.
        assert_eq!(offsets(&batch), vec![0, 2, 1]);
        // The fully-non-null tuple matches only the row with no nulls.
        let hit: &[&[&str]] = &[&["s1", "t1"]];
        assert_eq!(seek_row_offsets(&batch, hit).unwrap(), vec![0]);
        // (s1,t2) does NOT match row 2 (key_1 null) nor row 1 (key_0 null).
        let miss: &[&[&str]] = &[&["s1", "t2"]];
        assert!(seek_row_offsets(&batch, miss).unwrap().is_empty());
    }

    #[test]
    fn seek_arity_mismatch_on_empty_sidecar_is_err() {
        // Arity is validated at the boundary, before the zero-row early return,
        // so an OVER-arity probe is rejected even against an empty composite
        // sidecar. (A valid-arity probe — full or prefix — returns Ok(empty).)
        let empty = build_segment_index(&[utf8(&[]), utf8(&[])]).unwrap();
        assert_eq!(empty.num_rows(), 0);
        let over_arity: &[&[&str]] = &[&["a", "b", "c"]];
        assert!(seek_row_offsets(&empty, over_arity).is_err());
        // A leading-prefix probe against the empty sidecar is valid → empty.
        let prefix: &[&[&str]] = &[&["only-one"]];
        assert!(seek_row_offsets(&empty, prefix).unwrap().is_empty());
    }

    // CHA-499: leading-prefix seek (short probe = prefix)
    // A probe SHORTER than the sidecar's key arity is a leading-prefix seek:
    // it matches every row whose leading key columns equal the probe and
    // returns the full contiguous run. Correctness rests entirely on
    // build_segment_index's leading-column-first sort (pinned by
    // build_composite_leading_column_contiguity below). The relaxation is
    // one-sided — an over-arity or ragged probe stays a fail-fast Err.

    #[test]
    fn seek_prefix_returns_full_run() {
        // Composite (table_uuid, index_name): table "t1" owns two indexes.
        // rows: (t1,ix_b)@0, (t2,ix_z)@1, (t1,ix_a)@2, (t3,ix_q)@3.
        let batch = build_segment_index(&[
            utf8(&["t1", "t2", "t1", "t3"]),
            utf8(&["ix_b", "ix_z", "ix_a", "ix_q"]),
        ])
        .unwrap();
        // An arity-1 probe on the leading table_uuid returns BOTH of t1's
        // index rows (offsets 0 and 2), regardless of index_name.
        let prefix: &[&[&str]] = &[&["t1"]];
        assert_eq!(
            sorted(seek_row_offsets(&batch, prefix).unwrap()),
            vec![0, 2],
            "a short probe seeks the full leading-key run",
        );
    }

    #[test]
    fn seek_prefix_multi_probe_unions() {
        let batch = build_segment_index(&[
            utf8(&["t1", "t2", "t1", "t3"]),
            utf8(&["ix_b", "ix_z", "ix_a", "ix_q"]),
        ])
        .unwrap();
        // Two arity-1 prefixes union their runs: t1 -> {0,2}, t3 -> {3}.
        let prefixes: &[&[&str]] = &[&["t1"], &["t3"]];
        assert_eq!(
            sorted(seek_row_offsets(&batch, prefixes).unwrap()),
            vec![0, 2, 3],
        );
    }

    #[test]
    fn seek_prefix_absent_leading_key_empty() {
        let batch =
            build_segment_index(&[utf8(&["t1", "t2", "t1"]), utf8(&["ix_b", "ix_z", "ix_a"])])
                .unwrap();
        // A prefix whose leading key is absent contributes no offsets.
        let absent: &[&[&str]] = &[&["t9"]];
        assert!(seek_row_offsets(&batch, absent).unwrap().is_empty());
    }

    #[test]
    fn seek_prefix_over_arity_still_errs() {
        // The relaxation is one-sided: a probe LONGER than the key arity stays
        // a fail-fast Err (cannot probe more columns than the sidecar has).
        let batch = build_segment_index(&[utf8(&["t1", "t2"]), utf8(&["ix_a", "ix_b"])]).unwrap();
        let too_long: &[&[&str]] = &[&["t1", "ix_a", "extra"]];
        assert!(seek_row_offsets(&batch, too_long).is_err());
    }

    #[test]
    fn seek_ragged_probe_arity_is_err() {
        // All probe tuples in one call must share arity (one comparator set is
        // built for the batch); mixed arity is fail-fast.
        let batch = build_segment_index(&[utf8(&["t1", "t2"]), utf8(&["ix_a", "ix_b"])]).unwrap();
        let ragged: &[&[&str]] = &[&["t1"], &["t2", "ix_b"]];
        assert!(seek_row_offsets(&batch, ragged).is_err());
    }

    #[test]
    fn build_composite_leading_column_contiguity() {
        // Mitigation #2: prefix-seek correctness rests on build_segment_index
        // sorting leading-column-first, so all rows sharing a leading key form
        // one contiguous run. A future re-key that broke leading-first order
        // would fail this AND the prefix seeks above.
        let batch = build_segment_index(&[
            utf8(&["t2", "t1", "t2", "t1", "t3"]),
            utf8(&["ix_a", "ix_c", "ix_b", "ix_a", "ix_z"]),
        ])
        .unwrap();
        // Walk the sorted leading key column: once a leading key ends, it must
        // never reappear (contiguity), and keys must be non-descending.
        let leading = str_col(&batch, 0);
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for window in leading.windows(2) {
            assert!(window[0] <= window[1], "leading key must be non-descending");
            if window[0] != window[1] {
                assert!(
                    !seen.contains(&window[1]),
                    "leading key {:?} reappeared after its run ended — not \
                     leading-column-first contiguous",
                    window[1],
                );
                seen.insert(window[0].clone());
            }
        }
    }

    // CHA-485: typed (non-Utf8) key seek
    // The build already sorts any key type; these pin that the SEEK compares
    // in the column's native type via the cast-probe + typed-comparator path.

    fn int64(vals: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(vals.to_vec()))
    }

    #[test]
    fn seek_int64_typed_ordering() {
        // Keys {2, 9, 10, 100} sort natively [2, 9, 10, 100]; as strings they
        // would sort ["10", "100", "2", "9"], so a lexicographic compare over
        // the natively-sorted sidecar under-selects (misses 10 entirely).
        let batch = build_segment_index(&[int64(&[2, 9, 10, 100])]).unwrap();
        assert_eq!(offsets(&batch), vec![0, 1, 2, 3]);
        assert_eq!(seek_row_offsets(&batch, &[&["10"]]).unwrap(), vec![2]);
        // Multi-probe union across the numeric order.
        assert_eq!(
            sorted(seek_row_offsets(&batch, &[&["100"], &["2"]]).unwrap()),
            vec![0, 3]
        );
        assert!(seek_row_offsets(&batch, &[&["11"]]).unwrap().is_empty());
    }

    #[test]
    fn seek_boolean_key() {
        let key: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let batch = build_segment_index(&[key]).unwrap();
        // false@1 sorts before true@0.
        assert_eq!(offsets(&batch), vec![1, 0]);
        assert_eq!(seek_row_offsets(&batch, &[&["true"]]).unwrap(), vec![0]);
        assert_eq!(seek_row_offsets(&batch, &[&["false"]]).unwrap(), vec![1]);
    }

    #[test]
    fn seek_mixed_type_composite() {
        // (Utf8, Int32) composite: each probe element casts to its own key
        // column's type independently.
        let city = utf8(&["b", "a", "a"]);
        let count: ArrayRef = Arc::new(Int32Array::from(vec![7, 7, 9]));
        let batch = build_segment_index(&[city, count]).unwrap();
        // Sorted: (a,7)@1, (a,9)@2, (b,7)@0.
        assert_eq!(offsets(&batch), vec![1, 2, 0]);
        assert_eq!(seek_row_offsets(&batch, &[&["a", "9"]]).unwrap(), vec![2]);
        assert_eq!(seek_row_offsets(&batch, &[&["b", "7"]]).unwrap(), vec![0]);
        assert!(seek_row_offsets(&batch, &[&["b", "9"]]).unwrap().is_empty());
    }

    #[test]
    fn seek_cast_aliased_probes_dedup() {
        // Distinct probe STRINGS may cast to the same native key ("10" and
        // "010" both parse to Int64 10) — the second source of duplicate
        // offsets besides a repeated tuple; the union must still collapse.
        let batch = build_segment_index(&[int64(&[2, 9, 10, 100])]).unwrap();
        assert_eq!(
            seek_row_offsets(&batch, &[&["10"], &["010"]]).unwrap(),
            vec![2]
        );
    }

    #[test]
    fn seek_unrepresentable_probe_is_err() {
        // Strict cast (`safe: false`): a probe the key type cannot represent
        // is a fail-fast error, never a silently-never-matching NULL.
        let batch = build_segment_index(&[int64(&[1, 2])]).unwrap();
        assert!(seek_row_offsets(&batch, &[&["abc"]]).is_err());
    }

    #[test]
    fn seek_typed_nulls_last_never_matched() {
        let key: ArrayRef = Arc::new(Int64Array::from(vec![Some(5), None, Some(3)]));
        let batch = build_segment_index(&[key]).unwrap();
        // Ascending nulls-last: 3@2, 5@0, null@1 — the null row sorts last
        // and no (non-null) probe ever matches it.
        assert_eq!(offsets(&batch), vec![2, 0, 1]);
        assert_eq!(seek_row_offsets(&batch, &[&["5"]]).unwrap(), vec![0]);
        assert_eq!(seek_row_offsets(&batch, &[&["3"]]).unwrap(), vec![2]);
    }
}
