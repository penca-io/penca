//! Whole-partition packing of snapshot segment files.
//!
//! [`SegmentPacker`] accumulates clustered partition batches and flushes
//! one packed segment FILE whenever the next partition would exceed
//! `max_segment_bytes` — one metadata row per partition, sharing the
//! file's uri via `(offset, length)` row ranges. A single partition
//! larger than the cap cannot pack: it splits via `chunk_row_ranges`
//! into its own single-segment files (including its under-cap tail —
//! deliberate, see the red-test contract note in `snapshot_op.rs`).
//!
//! [`pack_merged_partition_stream`] drives the packer from the two
//! merge-read legs: the delta groups (resolved once, grouped by
//! partition label) and the prior-snapshot survivor stream (exclusion
//! already applied in-scan, arriving as label-sorted runs).
//!
//! A touched prior partition is merged with its delta by a
//! two-cursor streaming sorted-merge ([`PartitionMerger`]) instead of a
//! whole-partition `concat + sort`, and the packer accumulates a
//! partition's merged output sub-partition at a time — flushing
//! cap-sized files greedily as rows arrive. Peak memory is therefore
//! `max_segment_bytes` (the output buffer) + one prior segment batch +
//! the resident delta: the whole-partition residency term is gone.

use std::collections::BTreeMap;
use std::pin::Pin;

use arrow::array::ArrayRef;
use arrow::compute::{concat_batches, interleave};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, Rows};
use futures_util::{Stream, StreamExt};
use penca_core::naming::{snapshot_segment_uri, table_snapshot_segment_uuid};
use uuid::Uuid;

use penca_storage_meta::CarriedSegmentSpec;

use crate::error::ApiError;
use crate::lifecycle::batch_util::{
    PartitionOrderKey, PartitionOrdering, partition_label, partition_sort_fields,
    sort_record_batch_by_keys,
};
use crate::lifecycle::chunker::{batch_in_memory_bytes, chunk_row_ranges};
use crate::lifecycle::durable_writer::{SnapshotFileStep, SnapshotSegmentRowSpec};

/// One step out of [`pack_merged_partition_stream`]: either a
/// freshly packed file for a rewritten/delta partition, or a batch of
/// carried-forward segment specs for an untouched partition referenced
/// by its prior file. Carried specs carry no `file_batch` — the
/// orchestration copies the prior row's storage columns server-side via
/// `insert_carried_snapshot_segments`; they must never reach the durable
/// file writer.
pub(super) enum PackStep {
    File(SnapshotFileStep),
    Carried(Vec<CarriedSegmentSpec>),
}

/// One partition open in the packer: its merged output accumulates here
/// until the partition completes. `emitted_any` records whether this
/// partition has already flushed a cap-sized file — once it has, the
/// partition is "oversized" and its tail flushes as its own file rather
/// than folding into the multi-partition buffer.
struct OpenPartition {
    label: Option<String>,
    /// Accumulated merged output, in final sorted order. Collapsed to a
    /// single concatenated batch on each push so re-concat cost stays
    /// bounded by the resident tail, not the whole partition.
    acc: Vec<RecordBatch>,
    acc_rows: i64,
    emitted_any: bool,
}

/// Accumulate whole partitions; flush packed files at the byte cap.
///
/// `chunk_idx` is the snapshot-global segment counter (the only
/// uniquifier in `table_snapshot_segment_uuid`), assigned in flush
/// order so it stays dense across files.
pub(super) struct SegmentPacker {
    snap_uuid: Uuid,
    catalog_uuid: Uuid,
    branch_uuid: Uuid,
    base_uri: String,
    storage_format_text: String,
    max_segment_bytes: i64,
    buffered: Vec<(Option<String>, RecordBatch)>,
    buffered_bytes: i64,
    chunk_idx: u32,
    /// The partition currently being streamed in sub-partition
    /// chunks. `None` between partitions and for the whole-partition
    /// `push_partition` path (delta-only labels).
    open: Option<OpenPartition>,
}

impl SegmentPacker {
    pub(super) fn new(
        snap_uuid: &Uuid,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        base_uri: &str,
        storage_format_text: &str,
        max_segment_bytes: i64,
    ) -> Self {
        Self {
            snap_uuid: *snap_uuid,
            catalog_uuid: *catalog_uuid,
            branch_uuid: *branch_uuid,
            base_uri: base_uri.to_string(),
            storage_format_text: storage_format_text.to_string(),
            max_segment_bytes,
            buffered: Vec::new(),
            buffered_bytes: 0,
            chunk_idx: 0,
            open: None,
        }
    }

    /// Append one whole clustered partition. Returns the file steps
    /// this push forced out: the pending buffer when the partition no
    /// longer fits, plus the partition's own chunk files when it is
    /// oversized. Zero-row partitions are skipped.
    ///
    /// Used for delta-only partitions, which are resident anyway (the
    /// delta is O(delta)); prior-bearing partitions stream through
    /// [`Self::push_partition_chunk`] / [`Self::finish_partition`].
    pub(super) fn push_partition(
        &mut self,
        label: Option<String>,
        clustered: RecordBatch,
    ) -> Result<Vec<SnapshotFileStep>, ApiError> {
        if clustered.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let part_bytes = batch_in_memory_bytes(&clustered)?;
        let mut out = Vec::new();

        if part_bytes > self.max_segment_bytes {
            // Oversized: cannot pack with anything. Flush the buffer,
            // then split into single-segment files — the under-cap tail
            // chunk included (self-contained pass-through; see the
            // red-test contract note).
            if let Some(step) = self.flush()? {
                out.push(step);
            }
            for (offset, len, chunk_bytes) in chunk_row_ranges(&clustered, self.max_segment_bytes)?
            {
                let chunk = clustered.slice(offset, len);
                out.push(self.single_partition_file(label.clone(), chunk, chunk_bytes)?);
            }
            return Ok(out);
        }

        if !self.buffered.is_empty()
            && self.buffered_bytes + part_bytes > self.max_segment_bytes
            && let Some(step) = self.flush()?
        {
            out.push(step);
        }
        self.buffered.push((label, clustered));
        self.buffered_bytes += part_bytes;
        Ok(out)
    }

    /// Stream one sub-partition of merged output into the open
    /// partition `label`. Accumulates rows, and once the accumulation
    /// crosses `max_segment_bytes` flushes the pending multi-partition
    /// buffer (so its chunk_idx stays below this oversized partition's)
    /// and then emits cap-sized single-partition files greedily — the
    /// boundaries `chunk_row_ranges` would produce over the whole
    /// partition, since that walk is prefix-stable. The under-cap tail
    /// is retained for the next chunk / `finish_partition`.
    pub(super) fn push_partition_chunk(
        &mut self,
        label: &Option<String>,
        batch: RecordBatch,
    ) -> Result<Vec<SnapshotFileStep>, ApiError> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let open = match &mut self.open {
            Some(open) => {
                debug_assert_eq!(&open.label, label, "push_partition_chunk label mismatch");
                open
            }
            None => {
                self.open = Some(OpenPartition {
                    label: label.clone(),
                    acc: Vec::new(),
                    acc_rows: 0,
                    emitted_any: false,
                });
                self.open.as_mut().expect("just set")
            }
        };
        open.acc.push(batch);
        open.acc_rows += open.acc.last().expect("just pushed").num_rows() as i64;

        let schema = open.acc[0].schema();
        let combined = concat_batches(&schema, open.acc.iter()).map_err(ApiError::Arrow)?;
        let ranges = chunk_row_ranges(&combined, self.max_segment_bytes)?;

        // Under cap so far: keep accumulating (collapse to one batch).
        if ranges.len() <= 1 {
            let open = self.open.as_mut().expect("open set above");
            open.acc = vec![combined];
            return Ok(Vec::new());
        }

        // Crossed the cap: flush the buffered multi-partition pending
        // first (label-order chunk_idx), then emit every sealed chunk
        // (all but the last) as its own single-partition file. The last
        // range may still grow, so it becomes the new accumulator.
        let mut out = Vec::new();
        if !self.open.as_ref().expect("open set").emitted_any
            && let Some(step) = self.flush()?
        {
            out.push(step);
        }
        let label = label.clone();
        let (last_offset, last_len, _) = *ranges.last().expect("len > 1");
        for (offset, len, chunk_bytes) in &ranges[..ranges.len() - 1] {
            let chunk = combined.slice(*offset, *len);
            out.push(self.single_partition_file(label.clone(), chunk, *chunk_bytes)?);
        }
        let tail = combined.slice(last_offset, last_len);
        let open = self.open.as_mut().expect("open set");
        open.acc_rows = tail.num_rows() as i64;
        open.acc = vec![tail];
        open.emitted_any = true;
        Ok(out)
    }

    /// Complete the open partition. An under-cap partition that
    /// never flushed folds into the multi-partition buffer (so small
    /// partitions still share one file); an oversized partition flushes
    /// its retained tail as its own file (the deliberate under-cap-tail
    /// contract, matching `push_partition`).
    pub(super) fn finish_partition(&mut self) -> Result<Vec<SnapshotFileStep>, ApiError> {
        let Some(open) = self.open.take() else {
            return Ok(Vec::new());
        };
        if open.acc_rows == 0 {
            return Ok(Vec::new());
        }
        let schema = open.acc[0].schema();
        let combined = concat_batches(&schema, open.acc.iter()).map_err(ApiError::Arrow)?;

        if !open.emitted_any {
            // Stayed under the cap the whole time: pack with siblings.
            return self.push_partition(open.label, combined);
        }
        // Oversized: the tail is its own file (it is already <= cap, but
        // route through chunk_row_ranges for a single uniform path).
        let mut out = Vec::new();
        for (offset, len, chunk_bytes) in chunk_row_ranges(&combined, self.max_segment_bytes)? {
            let chunk = combined.slice(offset, len);
            out.push(self.single_partition_file(open.label.clone(), chunk, chunk_bytes)?);
        }
        Ok(out)
    }

    /// Flush whatever is buffered. The packer is consumed — the
    /// zero-row empty-merge placeholder is the caller's job.
    pub(super) fn finish(mut self) -> Result<Vec<SnapshotFileStep>, ApiError> {
        debug_assert!(
            self.open.is_none(),
            "finish called with an open partition — finish_partition first"
        );
        Ok(self.flush()?.into_iter().collect())
    }

    /// Concatenate the buffered partitions into one file step with one
    /// segment row per partition. `size_bytes` and `statistics` are
    /// computed per partition slice, not per file, so pruning stats
    /// stay partition-tight.
    fn flush(&mut self) -> Result<Option<SnapshotFileStep>, ApiError> {
        if self.buffered.is_empty() {
            return Ok(None);
        }
        let parts = std::mem::take(&mut self.buffered);
        self.buffered_bytes = 0;

        let file_seg_uuid = table_snapshot_segment_uuid(&self.snap_uuid, self.chunk_idx);
        let uri = self.uri_for(&file_seg_uuid);
        let schema = parts[0].1.schema();
        let file_batch =
            concat_batches(&schema, parts.iter().map(|(_, b)| b)).map_err(ApiError::Arrow)?;

        let mut segment_rows = Vec::with_capacity(parts.len());
        let mut offset: i64 = 0;
        for (label, batch) in &parts {
            let seg_uuid = table_snapshot_segment_uuid(&self.snap_uuid, self.chunk_idx);
            let num_rows = batch.num_rows() as i64;
            segment_rows.push(SnapshotSegmentRowSpec {
                seg_uuid_str: seg_uuid.to_string(),
                chunk_idx: self.chunk_idx,
                partition_value: label.clone(),
                offset,
                length: num_rows,
                size_bytes: batch_in_memory_bytes(batch)?,
                statistics: penca_dl::stats::compute_segment_statistics(batch),
            });
            self.chunk_idx += 1;
            offset += num_rows;
        }

        Ok(Some(SnapshotFileStep {
            snap_uuid_str: self.snap_uuid.to_string(),
            uri,
            file_batch,
            segment_rows,
        }))
    }

    /// One chunk of an oversized partition: its own file, one row.
    fn single_partition_file(
        &mut self,
        label: Option<String>,
        chunk: RecordBatch,
        chunk_bytes: i64,
    ) -> Result<SnapshotFileStep, ApiError> {
        let seg_uuid = table_snapshot_segment_uuid(&self.snap_uuid, self.chunk_idx);
        let num_rows = chunk.num_rows() as i64;
        let row = SnapshotSegmentRowSpec {
            seg_uuid_str: seg_uuid.to_string(),
            chunk_idx: self.chunk_idx,
            partition_value: label,
            offset: 0,
            length: num_rows,
            size_bytes: chunk_bytes,
            statistics: penca_dl::stats::compute_segment_statistics(&chunk),
        };
        self.chunk_idx += 1;
        Ok(SnapshotFileStep {
            snap_uuid_str: self.snap_uuid.to_string(),
            uri: self.uri_for(&seg_uuid),
            file_batch: chunk,
            segment_rows: vec![row],
        })
    }

    /// Claim one dense chunk_idx per carried prior segment row, in the
    /// given prior-chunk_idx order, deriving each new row's deterministic
    /// uuid from `table_snapshot_segment_uuid(snap, idx)`. Storage
    /// columns are copied later, server-side, by
    /// `insert_carried_snapshot_segments`. Must follow
    /// [`Self::flush_for_carried`].
    fn claim_carried(&mut self, prior_seg_uuids: &[String]) -> Vec<CarriedSegmentSpec> {
        prior_seg_uuids
            .iter()
            .map(|prior| {
                let idx = self.chunk_idx;
                self.chunk_idx += 1;
                CarriedSegmentSpec {
                    new_seg_uuid_str: table_snapshot_segment_uuid(&self.snap_uuid, idx).to_string(),
                    chunk_idx: idx,
                    prior_seg_uuid_str: prior.clone(),
                }
            })
            .collect()
    }

    fn uri_for(&self, file_seg_uuid: &Uuid) -> String {
        snapshot_segment_uri(
            &self.base_uri,
            &self.catalog_uuid,
            &self.branch_uuid,
            &self.snap_uuid,
            file_seg_uuid,
            &self.storage_format_text,
        )
    }
}

/// Two-cursor streaming sorted-merge of one partition's prior
/// snapshot rows (arriving as a sorted stream of slices) with its delta
/// (resolved once, sorted once). Replaces the whole-partition
/// `concat + sort`: peak residency is the delta plus one prior slice,
/// not the whole partition.
///
/// Tie rule: prior before delta. Equal-key delta rows are held back
/// until the prior stream advances strictly past that key — equal keys
/// can span prior batch boundaries, and every prior row at a key must
/// precede the delta rows at that key. With empty sort keys there is no
/// comparator: prior slices pass through as they arrive and the delta
/// drains at `finish` (prior-then-delta, matching the legacy concat
/// order).
///
/// The emitted order equals a stable lexsort of `concat(prior, delta)`
/// (a stable merge of two sorted runs), so it matches the content
/// oracle in `snapshot_op.rs`.
struct PartitionMerger {
    /// Typed ordering key carrying the partition's identity label
    /// (`key.label()`, used for `partition_value`) and its typed sort row.
    key: PartitionOrderKey,
    /// Delta sorted once by the effective sort keys; `None` for a
    /// prior-only partition.
    delta: Option<RecordBatch>,
    /// Row-encoded delta sort keys for comparison; `None` when there is
    /// no delta or no sort keys.
    delta_rows: Option<Rows>,
    /// Cursor into `delta` / `delta_rows`.
    delta_pos: usize,
    /// Shared converter so prior and delta rows are mutually comparable;
    /// `None` with empty sort keys.
    converter: Option<RowConverter>,
    sort_keys: Vec<String>,
}

impl PartitionMerger {
    /// Build a merger for `label`. `delta` is the partition's delta
    /// group (already filtered non-empty by the caller); `None` for a
    /// prior-only partition.
    fn new(
        key: PartitionOrderKey,
        delta: Option<RecordBatch>,
        sort_keys: &[String],
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<Self, ApiError> {
        // Empty sort keys, or no delta to interleave: no comparator.
        if sort_keys.is_empty() || delta.is_none() {
            return Ok(Self {
                key,
                delta,
                delta_rows: None,
                delta_pos: 0,
                converter: None,
                sort_keys: sort_keys.to_vec(),
            });
        }
        let delta = delta.expect("checked some");
        let fields = partition_sort_fields(schema, sort_keys)?;
        let converter = RowConverter::new(fields).map_err(ApiError::Arrow)?;
        let delta = sort_record_batch_by_keys(&delta, sort_keys)?;
        let delta_rows = converter
            .convert_columns(&sort_key_columns(&delta, sort_keys)?)
            .map_err(ApiError::Arrow)?;
        Ok(Self {
            key,
            delta: Some(delta),
            delta_rows: Some(delta_rows),
            delta_pos: 0,
            converter: Some(converter),
            sort_keys: sort_keys.to_vec(),
        })
    }

    /// Merge an arriving prior slice against the delta cursor, returning
    /// the safely-orderable output (or `None` if it produced no rows).
    fn push_prior(&mut self, slice: &RecordBatch) -> Result<Option<RecordBatch>, ApiError> {
        if slice.num_rows() == 0 {
            return Ok(None);
        }
        // No comparator (empty sort keys, or no delta): prior passes
        // straight through; delta drains at finish.
        let (Some(converter), Some(delta), Some(delta_rows)) =
            (&self.converter, &self.delta, &self.delta_rows)
        else {
            return Ok(Some(slice.clone()));
        };

        let prior_rows = converter
            .convert_columns(&sort_key_columns(slice, &self.sort_keys)?)
            .map_err(ApiError::Arrow)?;

        // Two-cursor merge over (slice prior rows, delta cursor),
        // emitting delta rows strictly smaller than the current prior
        // key before the prior row (prior-before-delta at ties). After
        // the slice, delta rows equal to its boundary key remain held —
        // a later slice may carry more prior rows at that key.
        let n = slice.num_rows();
        let delta_len = delta.num_rows();
        // (source, row) indices: 0 = prior slice, 1 = delta batch.
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(n);
        let mut j = self.delta_pos;
        for i in 0..n {
            let prior_key = prior_rows.row(i);
            while j < delta_len && delta_rows.row(j) < prior_key {
                indices.push((1, j));
                j += 1;
            }
            indices.push((0, i));
        }
        self.delta_pos = j;

        Ok(Some(interleave_two(slice, delta, &indices)?))
    }

    /// Drain the delta tail once the prior stream is exhausted (all
    /// remaining delta rows sort at or after the last prior key).
    fn finish(mut self) -> Result<Option<RecordBatch>, ApiError> {
        let Some(delta) = self.delta.take() else {
            return Ok(None);
        };
        // Empty sort keys: delta was never consumed — emit it whole
        // (prior-then-delta). With a comparator: emit the cursor tail.
        let start = if self.converter.is_some() {
            self.delta_pos
        } else {
            0
        };
        if start >= delta.num_rows() {
            return Ok(None);
        }
        Ok(Some(delta.slice(start, delta.num_rows() - start)))
    }
}

/// Gather the sort-key columns of `batch` in `keys` order.
fn sort_key_columns(batch: &RecordBatch, keys: &[String]) -> Result<Vec<ArrayRef>, ApiError> {
    keys.iter()
        .map(|key| {
            let idx = batch.schema().index_of(key).map_err(|_| {
                ApiError::Internal(format!("sort key '{key}' not in snapshot stream schema"))
            })?;
            Ok(batch.column(idx).clone())
        })
        .collect()
}

/// Interleave two same-schema batches by `(source, row)` indices
/// (`0 = a`, `1 = b`), column by column.
fn interleave_two(
    a: &RecordBatch,
    b: &RecordBatch,
    indices: &[(usize, usize)],
) -> Result<RecordBatch, ApiError> {
    // Positional interleave assumes both legs share a schema (prior and
    // delta both project the snapshot read schema). Keep the fail-fast
    // posture the legacy `concat_batches` had.
    debug_assert_eq!(
        a.schema(),
        b.schema(),
        "interleave_two requires identically-schema'd batches"
    );
    let schema = a.schema();
    let columns: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|c| {
            let a_col = a.column(c).as_ref();
            let b_col = b.column(c).as_ref();
            interleave(&[a_col, b_col], indices).map_err(ApiError::Arrow)
        })
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(schema, columns).map_err(ApiError::Arrow)
}

/// Merge the two snapshot input legs into the packer, streaming packed
/// file steps out as partitions complete.
///
/// `delta_groups` is the windowed cold resolve grouped by partition label
/// (`partition_record_batch` output). The `snapshot_stream` must yield
/// prior-snapshot survivor batches in plan order, which is
/// typed-partition-order run order. That contract is provided
/// end-to-end by the snapshot wiring: `ORDER BY seg.chunk_idx`
/// in `read_snapshot_segments_for_table` plus the ordered (`ByPlan`)
/// segment scan — the writer emits partitions in typed partition order, so
/// chunk_idx order IS typed partition order.
///
/// `ordering` is the typed [`PartitionOrdering`]: it carries the
/// partition-key names and the `RowConverter` that mints the
/// [`PartitionOrderKey`]s the `delta` / `carried` maps and the order check
/// are keyed by, so every leg merges in typed partition order rather than
/// stringified-label order (an `Int` key would otherwise sort `"10" < "2"`).
///
/// Each touched prior partition is merged with its delta by a
/// [`PartitionMerger`] and streamed sub-partition at a time into the
/// packer; delta-only partitions interleave at their order position via
/// the whole-partition `push_partition` path. Untouched partitions are
/// carried by reference: `carried` maps a [`PartitionOrderKey`] to its
/// prior segment rows' uuids (in prior chunk_idx order), and each is
/// emitted as a [`PackStep::Carried`] at its order position, consuming
/// dense chunk_idx from the shared counter — so the new snapshot's
/// `ORDER BY seg.chunk_idx` still yields typed-order runs for the next
/// cycle. `carried` is disjoint from the stream and delta partitions by
/// construction (carried = untouched, stream/delta = touched); empty for
/// the full-rewrite path. Out-of-(typed-)order prior partitions
/// are an invariant violation (fail fast).
pub(super) fn pack_merged_partition_stream<'a>(
    delta_groups: Vec<(Option<String>, RecordBatch)>,
    snapshot_stream: Pin<
        Box<dyn Stream<Item = Result<RecordBatch, penca_merge::MergeError>> + Send + 'a>,
    >,
    ordering: PartitionOrdering,
    sort_keys: Vec<String>,
    carried: BTreeMap<PartitionOrderKey, Vec<String>>,
    mut packer: SegmentPacker,
) -> Pin<Box<dyn Stream<Item = Result<PackStep, ApiError>> + Send + 'a>> {
    Box::pin(async_stream::try_stream! {
        // Re-key delta groups by their typed PartitionOrderKey:
        // the BTreeMap then drains in typed partition order, not the
        // stringified-label order an Option<String> key would impose.
        let mut delta: BTreeMap<PartitionOrderKey, RecordBatch> = BTreeMap::new();
        for (label, batch) in delta_groups {
            if batch.num_rows() == 0 {
                continue;
            }
            let key = batch_order_key(&ordering, &batch, label)?;
            delta.insert(key, batch);
        }
        let mut carried = carried;

        let mut stream = snapshot_stream;
        // The currently open prior-stream partition merger.
        let mut active: Option<PartitionMerger> = None;
        let mut last_completed: Option<PartitionOrderKey> = None;

        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(ApiError::Merge)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let label_cols: Vec<&ArrayRef> = ordering
                .keys()
                .iter()
                .map(|key| {
                    batch
                        .schema()
                        .index_of(key)
                        .map(|idx| batch.column(idx))
                        .map_err(|_| {
                            ApiError::InvalidRequest(format!(
                                "partition key '{key}' not in snapshot stream schema"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;

            // Split the batch into contiguous same-partition row runs and
            // feed each slice to the active merger (opening a new partition
            // on a partition-key change).
            let n = batch.num_rows();
            let mut start = 0usize;
            let mut current = row_order_key(&ordering, &label_cols, 0)?;
            for row in 1..=n {
                let next = if row < n {
                    Some(row_order_key(&ordering, &label_cols, row)?)
                } else {
                    None
                };
                if next.as_ref() != Some(&current) {
                    let slice = batch.slice(start, row - start);
                    let same_run = active.as_ref().map(|m| &m.key) == Some(&current);
                    if !same_run {
                        if let Some(prev) = active.take() {
                            for step in close_partition(&mut packer, prev)? {
                                yield step;
                            }
                        }
                        check_partition_order(&mut last_completed, &current)?;
                        // A touched partition can't also be carried.
                        if carried.contains_key(&current) {
                            Err(ApiError::Internal(format!(
                                "partition {:?} is both rewritten and carried",
                                current.label()
                            )))?;
                        }
                        for step in
                            drain_below(&mut packer, &mut carried, &mut delta, Some(&current), &sort_keys)?
                        {
                            yield step;
                        }
                        active = Some(PartitionMerger::new(
                            current.clone(),
                            delta.remove(&current),
                            &sort_keys,
                            &batch.schema(),
                        )?);
                    }
                    let merger = active.as_mut().expect("active set above");
                    if let Some(out) = merger.push_prior(&slice)? {
                        let label = merger.key.label().clone();
                        for step in packer.push_partition_chunk(&label, out)? {
                            yield PackStep::File(step);
                        }
                    }
                    if let Some(next_key) = next {
                        start = row;
                        current = next_key;
                    }
                }
            }
        }

        if let Some(prev) = active.take() {
            for step in close_partition(&mut packer, prev)? {
                yield step;
            }
        }

        // Remaining carried + delta-only partitions, in merged typed order.
        for step in drain_below(&mut packer, &mut carried, &mut delta, None, &sort_keys)? {
            yield step;
        }

        for step in packer.finish()? {
            yield PackStep::File(step);
        }
    })
}

/// The typed [`PartitionOrderKey`] for `row` of a prior-stream batch:
/// `sort_row = None` + `label = None` for unpartitioned tables (no keys —
/// mirrors `partition_record_batch`'s single `None`-labeled group), else
/// the shared `partition_label` string plus the typed sort row.
fn row_order_key(
    ordering: &PartitionOrdering,
    label_cols: &[&ArrayRef],
    row: usize,
) -> Result<PartitionOrderKey, ApiError> {
    let label = if ordering.keys().is_empty() {
        None
    } else {
        Some(partition_label(label_cols, row)?)
    };
    ordering.order_key_at(label_cols, row, label)
}

/// The typed [`PartitionOrderKey`] for a delta group's `batch` (its
/// partition-key columns are constant; row 0 is representative), paired
/// with the group's identity `label`.
fn batch_order_key(
    ordering: &PartitionOrdering,
    batch: &RecordBatch,
    label: Option<String>,
) -> Result<PartitionOrderKey, ApiError> {
    if ordering.keys().is_empty() {
        return ordering.order_key_from_key_arrays(&[], label);
    }
    let cols: Vec<&ArrayRef> = ordering
        .keys()
        .iter()
        .map(|key| {
            batch
                .schema()
                .index_of(key)
                .map(|idx| batch.column(idx))
                .map_err(|_| {
                    ApiError::Internal(format!("partition key '{key}' not in delta schema"))
                })
        })
        .collect::<Result<_, _>>()?;
    ordering.order_key_at(&cols, 0, label)
}

/// Enforce typed-order arrival of prior-stream partitions: a
/// partition out of typed order (or resurfacing after a later one) breaks
/// the `ORDER BY seg.chunk_idx` contract — a mis-packing hazard, not a
/// degraded mode.
///
/// Cross-version note: this typed-arrival invariant assumes the prior
/// snapshot was written by code that already orders typed. A prior snapshot
/// written before that over a *non-string* partition key laid its segments in
/// stringified-label order (e.g. an Int key `[10, 100, 2, 9]`), which is
/// not typed-ascending, so its stream trips this check's loud fail-fast
/// (not silent corruption). Pre-release there is no such live data; the fix
/// is a one-time re-snapshot, so no migration path is built here.
fn check_partition_order(
    last_completed: &mut Option<PartitionOrderKey>,
    key: &PartitionOrderKey,
) -> Result<(), ApiError> {
    if let Some(prev) = last_completed.as_ref()
        && key <= prev
    {
        return Err(ApiError::Internal(format!(
            "snapshot stream partitions out of order: {:?} after {:?}",
            key.label(),
            prev.label()
        )));
    }
    *last_completed = Some(key.clone());
    Ok(())
}

/// Drain the merger's delta tail into the packer, then finalize the
/// open partition (fold under-cap, or flush the oversized tail).
fn close_partition(
    packer: &mut SegmentPacker,
    merger: PartitionMerger,
) -> Result<Vec<PackStep>, ApiError> {
    let label = merger.key.label().clone();
    let mut out = Vec::new();
    if let Some(tail) = merger.finish()? {
        out.extend(
            packer
                .push_partition_chunk(&label, tail)?
                .into_iter()
                .map(PackStep::File),
        );
    }
    out.extend(packer.finish_partition()?.into_iter().map(PackStep::File));
    Ok(out)
}

/// Drain pending carried + delta-only partitions in merged typed order
/// up to `bound` (`Some(key)` → strictly below it; `None` → all
/// remaining). Carried and delta partitions are disjoint by construction,
/// so the two `BTreeMap` heads never tie. A carried partition flushes the
/// pending multi-partition buffer first (chunk_idx is flush-assigned, so
/// the smaller-order buffered partitions must claim their indices before
/// the carried row) then emits its [`PackStep::Carried`]; a delta-only
/// partition clustering-sorts and packs whole.
fn drain_below(
    packer: &mut SegmentPacker,
    carried: &mut BTreeMap<PartitionOrderKey, Vec<String>>,
    delta: &mut BTreeMap<PartitionOrderKey, RecordBatch>,
    bound: Option<&PartitionOrderKey>,
    sort_keys: &[String],
) -> Result<Vec<PackStep>, ApiError> {
    let in_bounds = |key: &PartitionOrderKey| match bound {
        Some(limit) => key < limit,
        None => true,
    };
    let mut out = Vec::new();
    loop {
        let carried_head = carried.first_key_value().map(|(k, _)| k.clone());
        let delta_head = delta.first_key_value().map(|(k, _)| k.clone());
        // carried (untouched) and delta (touched) partition-sets are
        // disjoint by construction; an equal head means a partition is both
        // rewritten and carried — a snapshot-corrupting mis-split. Fail
        // fast in release builds too (a hard error, matching the
        // prior/carried open-path check in pack_merged_partition_stream),
        // not just a debug_assert.
        if carried_head.is_some() && carried_head == delta_head {
            return Err(ApiError::Internal(format!(
                "partition {:?} is both rewritten and carried",
                carried_head.as_ref().map(PartitionOrderKey::label)
            )));
        }
        // Pick the smaller head (disjoint → never equal); stop once it
        // is at/above the bound (the other head is then no smaller).
        let take_carried = match (&carried_head, &delta_head) {
            (Some(c), Some(d)) => c < d,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_carried {
            let key = carried_head.expect("carried head present");
            if !in_bounds(&key) {
                break;
            }
            let prior_uuids = carried.remove(&key).expect("carried head present");
            // A carried entry always has >=1 prior segment; guard anyway
            // so an empty vec can't prematurely flush the buffer (which
            // would split otherwise-packable small partitions).
            if prior_uuids.is_empty() {
                continue;
            }
            // Flush the pending multi-partition buffer before this
            // carried partition interleaves: chunk_idx is assigned at
            // flush, so the buffered partitions (all of smaller order) must
            // claim the lower indices first, else the carried row would
            // sort ahead of them in `ORDER BY chunk_idx`.
            if let Some(file) = packer.flush()? {
                out.push(PackStep::File(file));
            }
            let specs = packer.claim_carried(&prior_uuids);
            if !specs.is_empty() {
                out.push(PackStep::Carried(specs));
            }
        } else {
            let key = delta_head.expect("delta head present");
            if !in_bounds(&key) {
                break;
            }
            let batch = delta.remove(&key).expect("delta head present");
            out.extend(push_delta_only(
                packer,
                key.label().clone(),
                batch,
                sort_keys,
            )?);
        }
    }
    Ok(out)
}

/// A delta-only partition (no prior rows): clustering-sort and push it
/// whole. Resident by construction (the delta is O(delta)), so the
/// streaming sub-partition path buys nothing here.
fn push_delta_only(
    packer: &mut SegmentPacker,
    label: Option<String>,
    batch: RecordBatch,
    sort_keys: &[String],
) -> Result<Vec<PackStep>, ApiError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let clustered = sort_record_batch_by_keys(&batch, sort_keys)?;
    // Permanent off-by-default residency observability:
    // per-partition row counts under `penca_api=trace`.
    tracing::trace!(
        target: "penca_api::snapshot_streaming",
        partition_value = ?label,
        partition_rows = clustered.num_rows(),
        "snapshot delta-only partition completed"
    );
    Ok(packer
        .push_partition(label, clustered)?
        .into_iter()
        .map(PackStep::File)
        .collect())
}
