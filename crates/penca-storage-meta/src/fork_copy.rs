//! Materializing a fork's inherited cold references at `CreateBranch` (CHA-539).
//!
//! A fork's claim on its parent's cold files used to be **implicit** — the read
//! planner reached across the fork edge at plan time — so the GC refcount gate,
//! which is a `NOT EXISTS` probe over metadata tables, could not see it. This
//! module makes the claim an explicit row in the child's own partition.
//!
//! Metadata only: every copied row carries the PARENT's `object_uri`. Fork cost
//! goes from O(1) to O(cold segments) in rows written, and stays O(1) in bytes.

use penca_core::{LogKind, naming};
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::helpers::{parse_uuid, qi};
use crate::{LifecycleManager, Result};
use penca_db::driver::{DbDriver, SqlValue, format_sql_uuid_array};

/// The parent's cold state at a fork edge, as the child will reference it.
struct InheritedBaseline {
    /// The parent snapshot the child adopts, and its watermark. `None` when the
    /// parent has no committed snapshot at or below the fork.
    snapshot: Option<(Uuid, i64)>,
}

impl LifecycleManager {
    /// Copy the parent's cold reference rows for one table onto the child.
    ///
    /// Runs inside `CreateBranch`'s transaction, so `commit_micros` is stamped
    /// directly rather than through a second two-phase pass — a rollback takes
    /// the whole copy with it.
    ///
    /// The window is exactly what `QueryManager::enumerate_base_cold_source`
    /// returns at this fork: the parent's latest committed snapshot at or below
    /// the fork on BOTH axes, plus the persist segments above that snapshot's
    /// watermark. Deriving it the same way is what keeps the copied state and the
    /// read path's notion of "the parent as-of this fork" from drifting apart.
    ///
    /// Persist rows below the snapshot watermark are deliberately not copied: the
    /// child's own plan windows persist from `snapshotted_at + 1`, so they would
    /// be dead weight that nonetheless pins parent files for the branch's life.
    /// Reads below that watermark remain the parent-reaching path's job
    /// (TODO(CHA-509) for the recursive chain, CHA-514 for its retention).
    pub async fn materialize_fork_cold_references(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        child_branch_uuid: &str,
        parent_branch_uuid: &str,
        table_uuid: &str,
        fork_commit_seq_num: i64,
        fork_commit_micros: i64,
        commit_micros: i64,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let child = parse_uuid(child_branch_uuid);
        let table = parse_uuid(table_uuid);

        let baseline = Self::copy_inherited_snapshot(
            driver,
            &catalog,
            &child,
            parent_branch_uuid,
            &table,
            fork_commit_seq_num,
            fork_commit_micros,
            commit_micros,
        )
        .await?;

        Self::copy_inherited_persist(
            driver,
            &catalog,
            &child,
            parent_branch_uuid,
            &table,
            fork_commit_seq_num,
            baseline.snapshot.map(|(_, watermark)| watermark),
            commit_micros,
        )
        .await?;

        // Seed the child's purge watermark at the fork position.
        //
        // Load-bearing, not bookkeeping: the read plan's hot/cold fence is
        // `max(Pu, W_snap)`, and cold serves `commit_seq_num <= fence`. A fresh
        // fork has no purge and (when the parent's latest snapshot sits above the
        // fork) no snapshot either, so the fence would be 0 and every inherited
        // row would be filtered out of the cold tier — the copy would be present
        // in metadata and invisible to reads.
        //
        // `Pu = fork_commit_seq_num` is also just true: `Pu` means "everything at
        // or below this seq is out of hot and lives in cold", and for a fork
        // everything at or below the fork edge is exactly that — inherited cold,
        // never in the child's hot tier. The child's own writes start at
        // `fork_seq + 1` (`seed_commit_seq_num_from_fork`), so the fence lands
        // precisely on the tier boundary.
        let purge_uuid = naming::table_purge_uuid(&catalog, &child, &table, fork_commit_seq_num, 0);
        Self::insert_table_purge(
            driver,
            catalog_uuid,
            &purge_uuid.to_string(),
            child_branch_uuid,
            table_uuid,
            Some(fork_commit_seq_num),
            None,
        )
        .await?;
        Self::commit_table_purge(
            driver,
            catalog_uuid,
            child_branch_uuid,
            &purge_uuid.to_string(),
        )
        .await?;

        Ok(())
    }

    /// Steps 1–4: the snapshot header, its segments, its cold-index parents and
    /// their sidecars.
    #[allow(clippy::too_many_arguments)]
    async fn copy_inherited_snapshot(
        driver: &impl DbDriver<Row = PgRow>,
        catalog: &Uuid,
        child: &Uuid,
        parent_branch_uuid: &str,
        table: &Uuid,
        fork_commit_seq_num: i64,
        fork_commit_micros: i64,
        commit_micros: i64,
    ) -> Result<InheritedBaseline> {
        let snap_meta = naming::table_snapshot_metadata_table(catalog);
        let snap_seg = naming::table_snapshot_segment_metadata_table(catalog);
        let idx_meta = naming::table_snapshot_index_metadata_table(catalog);
        let idx_seg = naming::table_snapshot_segment_index_metadata_table(catalog);

        // Bounded on BOTH axes. Seq is the authority for the ceiling — a
        // same-micros higher-seq parent commit must not be inherited — and the
        // micros bound matches how the read path picks a baseline.
        let pick = driver
            .execute_params(
                &format!(
                    "SELECT table_snapshot_uuid, snapshotted_at_micros \
                     FROM {snap} \
                     WHERE branch_uuid = $1 AND table_uuid = $2 \
                       AND commit_micros IS NOT NULL \
                       AND commit_seq_num <= $3 \
                       AND snapshotted_at_micros <= $4 \
                     ORDER BY snapshotted_at_micros DESC, commit_seq_num DESC \
                     LIMIT 1",
                    snap = qi(&snap_meta),
                ),
                &[
                    SqlValue::uuid_str(parent_branch_uuid)?,
                    SqlValue::Uuid(*table),
                    SqlValue::Int64(fork_commit_seq_num),
                    SqlValue::Int64(fork_commit_micros),
                ],
            )
            .await?;
        let Some(row) = pick.first() else {
            return Ok(InheritedBaseline { snapshot: None });
        };
        let parent_snap: Uuid = row.get("table_snapshot_uuid");
        let snapshotted_at: i64 = row.get("snapshotted_at_micros");
        let new_snap = naming::table_snapshot_uuid(catalog, child, table, snapshotted_at);

        // 1. The header. `partition_keys` / `clustering_keys` carry verbatim
        //    because `carry_forward_keys` compares them against the live layout
        //    and declines carry-forward on a mismatch; `durable` carries verbatim
        //    so the child's retention floor starts where the parent's is.
        driver
            .execute_no_result_params(
                &format!(
                    "INSERT INTO {snap} \
                     (table_snapshot_uuid, branch_uuid, table_uuid, snapshotted_at_micros, \
                      commit_seq_num, durable, partition_keys, clustering_keys, commit_micros) \
                     SELECT $1, $2, old.table_uuid, old.snapshotted_at_micros, \
                            old.commit_seq_num, old.durable, old.partition_keys, \
                            old.clustering_keys, $5 \
                     FROM {snap} old \
                     WHERE old.branch_uuid = $3 AND old.table_snapshot_uuid = $4 \
                     ON CONFLICT (branch_uuid, table_snapshot_uuid) DO NOTHING",
                    snap = qi(&snap_meta),
                ),
                &[
                    SqlValue::Uuid(new_snap),
                    SqlValue::Uuid(*child),
                    SqlValue::uuid_str(parent_branch_uuid)?,
                    SqlValue::Uuid(parent_snap),
                    SqlValue::Int64(commit_micros),
                ],
            )
            .await?;

        // 2. Its segments. `chunk_idx` is re-densified by enumeration order —
        //    there is no packer here to inherit a counter from — and the new uuid
        //    chains off it so a retried copy collapses on the same ids.
        let segs = driver
            .execute_params(
                &format!(
                    "SELECT table_snapshot_segment_uuid FROM {seg} \
                     WHERE branch_uuid = $1 AND table_snapshot_uuid = $2 \
                       AND commit_micros IS NOT NULL \
                     ORDER BY chunk_idx, \"offset\"",
                    seg = qi(&snap_seg),
                ),
                &[
                    SqlValue::uuid_str(parent_branch_uuid)?,
                    SqlValue::Uuid(parent_snap),
                ],
            )
            .await?;
        let mut seg_map: Vec<(Uuid, Uuid, u32)> = Vec::with_capacity(segs.len());
        for (chunk_idx, seg_row) in segs.iter().enumerate() {
            let old: Uuid = seg_row.get("table_snapshot_segment_uuid");
            let chunk_idx = chunk_idx as u32;
            seg_map.push((
                naming::table_snapshot_segment_uuid(&new_snap, chunk_idx),
                old,
                chunk_idx,
            ));
        }
        // One statement for the whole snapshot's segments. The three arrays carry
        // what SQL cannot derive: the new uuid (a `naming` derivation), the old
        // uuid it copies from, and the re-densified `chunk_idx` — which is the
        // enumeration position, NOT `old.chunk_idx`, so it has to travel with the
        // pair rather than be read off the source row.
        if !seg_map.is_empty() {
            let new_uuids: Vec<String> = seg_map.iter().map(|(n, _, _)| n.to_string()).collect();
            let old_uuids: Vec<String> = seg_map.iter().map(|(_, o, _)| o.to_string()).collect();
            let new_refs: Vec<&str> = new_uuids.iter().map(String::as_str).collect();
            let old_refs: Vec<&str> = old_uuids.iter().map(String::as_str).collect();
            let chunk_arr = format!(
                "ARRAY[{}]::int[]",
                seg_map
                    .iter()
                    .map(|(_, _, c)| c.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {seg} \
                         (table_snapshot_segment_uuid, table_snapshot_uuid, branch_uuid, \
                          table_uuid, chunk_idx, object_uri, \"offset\", length, size_bytes, \
                          format, metadata, statistics, row_count, commit_micros) \
                         SELECT m.new_uuid, $1, $2, old.table_uuid, m.chunk_idx, old.object_uri, \
                                old.\"offset\", old.length, old.size_bytes, old.format, \
                                old.metadata, old.statistics, old.row_count, $4 \
                         FROM {seg} old \
                         JOIN unnest({new_arr}, {old_arr}, {chunk_arr}) \
                           AS m(new_uuid, old_uuid, chunk_idx) \
                           ON old.table_snapshot_segment_uuid = m.old_uuid \
                         WHERE old.branch_uuid = $3 \
                         ON CONFLICT (branch_uuid, table_snapshot_segment_uuid) DO NOTHING",
                        seg = qi(&snap_seg),
                        new_arr = format_sql_uuid_array(&new_refs),
                        old_arr = format_sql_uuid_array(&old_refs),
                        chunk_arr = chunk_arr,
                    ),
                    &[
                        SqlValue::Uuid(new_snap),
                        SqlValue::Uuid(*child),
                        SqlValue::uuid_str(parent_branch_uuid)?,
                        SqlValue::Int64(commit_micros),
                    ],
                )
                .await?;
        }

        // 3. Cold-index parents. `key_columns` carries so the planner's
        //    covering-index selection reads snapshot-index metadata only.
        let idx_parents = driver
            .execute_params(
                &format!(
                    "SELECT table_snapshot_index_uuid, index_uuid FROM {idx} \
                     WHERE branch_uuid = $1 AND table_snapshot_uuid = $2 \
                       AND commit_micros IS NOT NULL",
                    idx = qi(&idx_meta),
                ),
                &[
                    SqlValue::uuid_str(parent_branch_uuid)?,
                    SqlValue::Uuid(parent_snap),
                ],
            )
            .await?;
        let mut parent_map: Vec<(Uuid, Uuid, String)> = Vec::with_capacity(idx_parents.len());
        for idx_row in &idx_parents {
            let old_parent: Uuid = idx_row.get("table_snapshot_index_uuid");
            let index_uuid: Option<Uuid> = idx_row.get("index_uuid");
            let new_parent = naming::table_snapshot_index_uuid(&new_snap, index_uuid.as_ref());
            // Carry the slug forward rather than re-SELECTing it per
            // (parent x segment) pair below — it is already in hand here.
            let slug = index_uuid.map_or_else(|| "row_uuid".to_string(), |u| u.to_string());
            parent_map.push((new_parent, old_parent, slug));
        }
        if !parent_map.is_empty() {
            let new_p: Vec<String> = parent_map.iter().map(|(n, _, _)| n.to_string()).collect();
            let old_p: Vec<String> = parent_map.iter().map(|(_, o, _)| o.to_string()).collect();
            let new_refs: Vec<&str> = new_p.iter().map(String::as_str).collect();
            let old_refs: Vec<&str> = old_p.iter().map(String::as_str).collect();
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {idx} \
                         (table_snapshot_index_uuid, branch_uuid, table_snapshot_uuid, \
                          index_uuid, key_columns, commit_micros) \
                         SELECT m.new_parent, $1, $2, old.index_uuid, old.key_columns, $4 \
                         FROM {idx} old \
                         JOIN unnest({new_arr}, {old_arr}) AS m(new_parent, old_parent) \
                           ON old.table_snapshot_index_uuid = m.old_parent \
                         WHERE old.branch_uuid = $3 \
                         ON CONFLICT (branch_uuid, table_snapshot_index_uuid) DO NOTHING",
                        idx = qi(&idx_meta),
                        new_arr = format_sql_uuid_array(&new_refs),
                        old_arr = format_sql_uuid_array(&old_refs),
                    ),
                    &[
                        SqlValue::Uuid(*child),
                        SqlValue::Uuid(new_snap),
                        SqlValue::uuid_str(parent_branch_uuid)?,
                        SqlValue::Int64(commit_micros),
                    ],
                )
                .await?;
        }

        // 4. Sidecars. Each must hang off the NEW base-segment uuid and the NEW
        //    index parent, which is why this cannot be one flat INSERT..SELECT:
        //    the mapping only exists in Rust. The sidecar id is
        //    `row_uuid_for_pk(new_seg, [index_slug])` — identical to what a fresh
        //    build of that segment produces for the same index, so build and copy
        //    agree.
        // One statement for every (index parent x base segment) pair, which was
        // the largest loop here. Five arrays because the join needs BOTH old keys
        // and the row needs all three new ones; none of them are derivable in SQL
        // (the sidecar id is `row_uuid_for_pk(new_seg, [slug])`, identical to what
        // a fresh build of that segment produces, so build and copy agree).
        let mut sc_new: Vec<String> = Vec::new();
        let mut sc_new_seg: Vec<String> = Vec::new();
        let mut sc_new_parent: Vec<String> = Vec::new();
        let mut sc_old_seg: Vec<String> = Vec::new();
        let mut sc_old_parent: Vec<String> = Vec::new();
        for (new_parent, old_parent, slug) in &parent_map {
            for (new_seg, old_seg, _) in &seg_map {
                sc_new.push(naming::row_uuid_for_pk(new_seg, &[slug.as_str()]).to_string());
                sc_new_seg.push(new_seg.to_string());
                sc_new_parent.push(new_parent.to_string());
                sc_old_seg.push(old_seg.to_string());
                sc_old_parent.push(old_parent.to_string());
            }
        }
        if !sc_new.is_empty() {
            let a = |v: &Vec<String>| {
                format_sql_uuid_array(&v.iter().map(String::as_str).collect::<Vec<_>>())
            };
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {sidecar} \
                         (segment_index_uuid, branch_uuid, segment_uuid, \
                          table_snapshot_index_uuid, object_uri, \"offset\", length, \
                          format, size_bytes, statistics, commit_micros) \
                         SELECT m.new_sidecar, $1, m.new_seg, m.new_parent, old.object_uri, \
                                old.\"offset\", old.length, old.format, old.size_bytes, \
                                old.statistics, $3 \
                         FROM {sidecar} old \
                         JOIN unnest({a1}, {a2}, {a3}, {a4}, {a5}) \
                           AS m(new_sidecar, new_seg, new_parent, old_seg, old_parent) \
                           ON old.segment_uuid = m.old_seg \
                          AND old.table_snapshot_index_uuid = m.old_parent \
                         WHERE old.branch_uuid = $2 AND old.commit_micros IS NOT NULL \
                         ON CONFLICT (branch_uuid, segment_index_uuid) DO NOTHING",
                        sidecar = qi(&idx_seg),
                        a1 = a(&sc_new),
                        a2 = a(&sc_new_seg),
                        a3 = a(&sc_new_parent),
                        a4 = a(&sc_old_seg),
                        a5 = a(&sc_old_parent),
                    ),
                    &[
                        SqlValue::Uuid(*child),
                        SqlValue::uuid_str(parent_branch_uuid)?,
                        SqlValue::Int64(commit_micros),
                    ],
                )
                .await?;
        }

        Ok(InheritedBaseline {
            snapshot: Some((new_snap, snapshotted_at)),
        })
    }

    /// Steps 5–6: the persist headers and their segments, clamped and sealed.
    #[allow(clippy::too_many_arguments)]
    async fn copy_inherited_persist(
        driver: &impl DbDriver<Row = PgRow>,
        catalog: &Uuid,
        child: &Uuid,
        parent_branch_uuid: &str,
        table: &Uuid,
        fork_commit_seq_num: i64,
        baseline_watermark: Option<i64>,
        commit_micros: i64,
    ) -> Result<()> {
        let persist_meta = naming::table_persist_metadata_table(catalog);
        let persist_seg = naming::table_persist_segment_metadata_table(catalog);
        // Headers from the baseline INCLUSIVE; segments only strictly above it.
        //
        // The inclusive header is load-bearing and easy to lose. The read plan's
        // snapshot pick is bounded by
        // `snapshotted_at_micros <= LEAST(as_of, COALESCE(persist_wm, -1))`, where
        // `persist_wm = MAX(persisted_at_micros)` over the CHILD's persist
        // headers. When the parent's newest snapshot sits exactly at its persist
        // watermark — which is what `write -> persist -> snapshot` produces, the
        // default shape — a segments-only window copies nothing at all, leaving
        // the child with no persist header, `persist_wm` NULL, the bound collapsed
        // to -1, and the inherited snapshot never picked. Reads then return zero
        // rows even though the copy is present, which is the exact failure the
        // `Pu` seed above was meant to close; `Pu` is only one of two gates.
        //
        // Copying the baseline's header (header only, no segments) makes
        // `persist_wm >= snapshotted_at_micros` so the pick succeeds. No segments
        // means nothing is double-counted against the snapshot that already
        // materializes those rows, and no extra parent files are pinned.
        //
        // The window's UPPER bound is the segment query, not a second micros
        // predicate. Neither naive ceiling works: the fork-time `PersistBranch`
        // flush writes a header stamped after `fork_commit_micros`, and a
        // straddling run's header legitimately carries `commit_seq_num >
        // fork_seq`, so bounding on either axis drops rows the child needs. A
        // header earns its place by having at least one segment at or below the
        // fork — which is exactly what makes it part of "the parent as-of this
        // fork" — so the header is written only once its segments qualify.
        // Skipping that check copied segment-less headers for parent runs
        // entirely above the fork, which inflates the child's
        // `latest_committed_table_persist_watermark` and, through
        // `compute_snapshot_window`, stamps a freshly-forked child's first
        // snapshot at the parent's latest persist time.
        let header_from = baseline_watermark.unwrap_or(i64::MIN);
        let segment_from = baseline_watermark.map_or(i64::MIN, |w| w.saturating_add(1));

        let headers = driver
            .execute_params(
                &format!(
                    "SELECT table_persist_uuid, persisted_at_micros, log_kind FROM {meta} \
                     WHERE branch_uuid = $1 AND table_uuid = $2 \
                       AND commit_micros IS NOT NULL \
                       AND persisted_at_micros >= $3",
                    meta = qi(&persist_meta),
                ),
                &[
                    SqlValue::uuid_str(parent_branch_uuid)?,
                    SqlValue::Uuid(*table),
                    SqlValue::Int64(header_from),
                ],
            )
            .await?;

        for header in &headers {
            let old_header: Uuid = header.get("table_persist_uuid");
            let persisted_at: i64 = header.get("persisted_at_micros");
            let log_kind_text: String = header.get("log_kind");
            let log_kind: LogKind = log_kind_text.parse().map_err(|_| {
                crate::MetadataError::Db(sqlx::Error::Protocol(format!(
                    "table_persist_metadata.log_kind decode failed: {log_kind_text}"
                )))
            })?;
            let new_header =
                naming::table_persist_uuid(catalog, child, table, persisted_at, log_kind);

            // The baseline's own header is the one deliberate exception to
            // "a header needs qualifying segments": it is copied bare, because
            // the inherited snapshot already materializes its rows.
            let is_baseline_header = persisted_at <= segment_from.saturating_sub(1);

            // Derive every (new, old) uuid pair BEFORE writing the header — the
            // pairs are `naming` derivations off `new_header`, which is itself a
            // pure function of values already in hand, so nothing here needs the
            // header row to exist. Deriving first is what lets an empty result
            // skip the header instead of stranding a segment-less one.
            let mut new_uuids: Vec<String> = Vec::new();
            let mut old_uuids: Vec<String> = Vec::new();
            if !is_baseline_header {
                let segs = driver
                    .execute_params(
                        &format!(
                            "SELECT table_persist_segment_uuid, chunk_idx FROM {seg} \
                             WHERE branch_uuid = $1 AND table_persist_uuid = $2 \
                               AND commit_micros IS NOT NULL \
                               AND min_commit_seq_num <= $3 \
                             ORDER BY chunk_idx",
                            seg = qi(&persist_seg),
                        ),
                        &[
                            SqlValue::uuid_str(parent_branch_uuid)?,
                            SqlValue::Uuid(old_header),
                            SqlValue::Int64(fork_commit_seq_num),
                        ],
                    )
                    .await?;
                new_uuids.reserve(segs.len());
                old_uuids.reserve(segs.len());
                for seg_row in &segs {
                    let old_seg: Uuid = seg_row.get("table_persist_segment_uuid");
                    let chunk_idx: i32 = seg_row.get("chunk_idx");
                    // Err, not a sentinel: falling back to 0 would derive the uuid
                    // the header's real chunk 0 already owns, and the INSERT's
                    // ON CONFLICT DO NOTHING would then swallow the row — leaving
                    // the child one inherited segment short with no error and no
                    // signal, the worst failure mode for a copy whose job is
                    // completeness.
                    let chunk_idx = u32::try_from(chunk_idx).map_err(|_| {
                        crate::MetadataError::Db(sqlx::Error::Protocol(format!(
                            "table_persist_segment_metadata.chunk_idx out of range: {chunk_idx}"
                        )))
                    })?;
                    new_uuids.push(
                        naming::table_persist_segment_uuid(&new_header, chunk_idx).to_string(),
                    );
                    old_uuids.push(old_seg.to_string());
                }
                if new_uuids.is_empty() {
                    continue;
                }
            }

            // 5. The header. Required, not decorative: the read path INNER JOINs
            //    segments up to it for `log_kind`, so a segment row without its
            //    header on the SAME branch is invisible rather than merely
            //    unclassified.
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {meta} \
                         (table_persist_uuid, branch_uuid, table_uuid, persisted_at_micros, \
                          commit_seq_num, log_kind, commit_micros) \
                         SELECT $1, $2, old.table_uuid, old.persisted_at_micros, \
                                LEAST(old.commit_seq_num, $6), old.log_kind, $7 \
                         FROM {meta} old \
                         WHERE old.branch_uuid = $3 AND old.table_persist_uuid = $4 \
                         ON CONFLICT (branch_uuid, table_persist_uuid) DO NOTHING",
                        meta = qi(&persist_meta),
                    ),
                    &[
                        SqlValue::Uuid(new_header),
                        SqlValue::Uuid(*child),
                        SqlValue::uuid_str(parent_branch_uuid)?,
                        SqlValue::Uuid(old_header),
                        SqlValue::Int64(fork_commit_seq_num),
                        SqlValue::Int64(fork_commit_seq_num),
                        SqlValue::Int64(commit_micros),
                    ],
                )
                .await?;

            // 6. Its segments — copied in ONE statement per header. The old→new
            //    mapping lives in Rust (the uuids are `naming` derivations, not
            //    anything SQL can compute), which is the only reason this was ever
            //    a per-row loop — and it is expressible as a join against `unnest`
            //    of the two arrays.
            //
            //    Empty only for the baseline header, which is deliberately bare;
            //    every other header without qualifying segments was skipped above
            //    rather than written.
            if new_uuids.is_empty() {
                continue;
            }
            let new_refs: Vec<&str> = new_uuids.iter().map(String::as_str).collect();
            let old_refs: Vec<&str> = old_uuids.iter().map(String::as_str).collect();

            // `max_commit_seq_num = LEAST(old, fork_seq)` is the clamp the whole
            // design rests on. A fork point is an arbitrary commit-order
            // position, so a parent segment routinely straddles it, and a
            // verbatim copy would expose the parent's post-fork rows to the
            // child. The read paths honor this per-segment ceiling
            // (`apply_segment_seq_ceiling`).
            //
            // `is_sealed = TRUE` keeps the row out of the child's compaction
            // waves: the compact input query filters `is_sealed = FALSE`, and
            // repacking an inherited reference would rewrite the parent's bytes
            // under the child's prefix — turning a reference back into a copy and
            // losing the O(1)-bytes fork guarantee.
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {seg} \
                         (table_persist_segment_uuid, table_persist_uuid, branch_uuid, \
                          table_uuid, chunk_idx, min_tx_commit_micros, max_tx_commit_micros, \
                          min_commit_seq_num, max_commit_seq_num, object_uri, \"offset\", \
                          length, row_count, format, size_bytes, metadata, statistics, \
                          is_sealed, commit_micros) \
                         SELECT m.new_uuid, $1, $2, old.table_uuid, old.chunk_idx, \
                                old.min_tx_commit_micros, old.max_tx_commit_micros, \
                                old.min_commit_seq_num, LEAST(old.max_commit_seq_num, $4), \
                                old.object_uri, old.\"offset\", old.length, old.row_count, \
                                old.format, old.size_bytes, old.metadata, old.statistics, \
                                TRUE, $5 \
                         FROM {seg} old \
                         JOIN unnest({new_arr}, {old_arr}) AS m(new_uuid, old_uuid) \
                           ON old.table_persist_segment_uuid = m.old_uuid \
                         WHERE old.branch_uuid = $3 \
                         ON CONFLICT (branch_uuid, table_persist_segment_uuid) DO NOTHING",
                        seg = qi(&persist_seg),
                        new_arr = format_sql_uuid_array(&new_refs),
                        old_arr = format_sql_uuid_array(&old_refs),
                    ),
                    &[
                        SqlValue::Uuid(new_header),
                        SqlValue::Uuid(*child),
                        SqlValue::uuid_str(parent_branch_uuid)?,
                        SqlValue::Int64(fork_commit_seq_num),
                        SqlValue::Int64(commit_micros),
                    ],
                )
                .await?;
        }

        Ok(())
    }
}
