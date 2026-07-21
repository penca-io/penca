---
name: project_metadata_reads_to_querymanager
description: DONE (CHA-472, merged 2026-06-24) — MetadataClient READ methods rehomed onto QueryManager; write path now serves cached metadata reads. Transactional (OpenTx) regime still misses the fast-paths → CHA-501.
metadata: 
  node_type: memory
  type: project
  originSessionId: 8aba36cf-7e21-4519-bf1a-4c233ce7a6af
---

SHIPPED (ticket [CHA-472](https://linear.app/chapala/issue/CHA-472), Done 2026-06-24, PR #274, ADR 0028): the READ half of `MetadataClient` was rehomed onto `QueryManager` as `&self` methods (`get_table`/`resolve_{table,schema,index}_metadata`/`plan`+helpers/`read_system_table`), `WriteManager` now HOLDS a `QueryManager` (`QueryManager::for_metadata_reads`), and the write/lifecycle remainder was renamed `MetadataClient`→`LifecycleManager`. So query AND write paths resolve identifiers through the same cache-consulting methods.

CORRECTED CURRENT STATE (verified 2026-07-09, main @ a1d8a13e) — supersedes the old "write path runs cache-less" claim:
- `penca_write.rs` builds an **enabled** `snapshot_list_cache` + `snapshot_cache` and hands them to the write `QueryManager` (NOT `disabled()` — that stale text still lingers in the `WriteManager` struct doc comment `write/mod.rs:73-77`; CHA-501 fixes it).
- The old CHA-472 axis eligibility gate (default-current-time → `Some(cache)`, `open_tx → None`) is **GONE**: [CHA-492](https://linear.app/chapala/issue/CHA-492) re-keyed the cache on the resolved snapshot's `W_snap` (content-addressed, immutable per version), so `read_system_table` now passes `Some(cache)` **unconditionally for any snapshot axis** ("safe for any resolve", `meta_resolve.rs:357`). Hot log is always read fresh, so RYOW stays correct without bypassing the cold-baseline list cache.

STILL-OPEN GAP → the epic payoff was for the **autocommit** regime (sub-10ms all-cold-snapshotted point reads/writes). The **transactional (OpenTx) regime** still misses the fast-paths: the direct-seek bypass (`is_direct_seek_eligible`/`read_data_seek_eligible`, query/mod.rs:1697/1709) admits only `LatestSeq`/`AsOfSeq`, and RYOW statements keep a real hot overlay so they stay `merged`. Tracked in [CHA-501](https://linear.app/chapala/issue/CHA-501) (pgbench TPC-B ~250ms/txn): widen the seek gate to `is_snapshot_only` alone, enable the lifecycle scheduler for the pgbench test, and make `setup_pgbench_schema` drive its system tables cold (it currently doesn't, so the benchmark never exercises this cache).

**How to apply:** the rehome + shared caches are DONE — don't re-plan them. When touching write-path resolution perf or the seek/cache gates, the live work is extending the fast-paths into the OpenTx regime (CHA-501), and the correctness backstop is the CHA-473 loose existence gate (`is_snapshot_only` subsumes the tx's own writes). Related: [[project_persist_cdc_purge_governs_reads]], [[project_branch_create_flush_to_cold]].
