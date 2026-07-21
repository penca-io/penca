---
name: project_cha432_durable_substrate
description: CHA-432 snapshot durability substrate — LANDED (PR #308); the durable-flag + retention_floor contract the downstream retention ops build on
metadata:
  type: project
---

CHA-432 "Snapshot durability substrate" (audit-retention epic, ADR 0025 snapshot-as-floor) — **MERGED 2026-07-11, PR #308, Linear Done**. Blocker CHA-495 (RetentionConfig reshape) landed first (PR #301).

**What landed (the contract downstream ops consume):**
- `durable BOOLEAN NOT NULL DEFAULT false` on `table_snapshot_metadata` (pg.rs DDL; pre-release no-migration).
- `decide_durable(last_durable_at_micros, snapshotted_at_micros, density_seconds) -> bool` in `penca-api/src/lifecycle/snapshot_op.rs`: durable iff no prior durable rung, OR density unset (all durable, CHA-55), OR gap ≥ `density_seconds × 1_000_000`. Sticky — assigned once at snapshot creation, kept out of `insert_snapshot_metadata`'s `DO UPDATE` so the floor stays monotonic.
- `LifecycleManager::retention_floor(driver, catalog, branch, table, retention_duration_seconds: Option<i64>, now_micros) -> Result<Option<(commit_seq_num, snapshotted_at_micros)>>` in `penca-storage-meta/src/snapshot.rs`: newest durable committed snapshot with `snapshotted_at_micros ≤ now − duration×1e6`; `None` when duration unset (no query) or none precedes window. Plus `last_durable_snapshot_at(...)` (the assignment-time read). Both filter `durable AND commit_micros IS NOT NULL`.

**Scope boundary (load-bearing for the next tickets):** CHA-432 ships the standalone `retention_floor` helper ONLY — it does NOT touch `QueryManager::plan`/`hot_min`. The plan-path fold + `as_of < floor` `FAILED_PRECONDITION` enforcement is **CHA-433's** job (its Linear comment records this). See [[feedback_tickets_are_spirit_not_spec]], [[feedback_evaluate_ticket_necessity_first_principles]].

**Downstream (Backlog, unblocked by this):** CHA-433 (plan-time floor enforcement + Flight SQL driver-parity audit; folds `retention_floor` onto `hot_min`), CHA-434 (PrunePersistSegments — calls `retention_floor` directly), CHA-55 (snapshot retention v1 — durable-flag keep predicate). Related CHA-425 = superseded baseline-fold design.
