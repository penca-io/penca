---
name: project_branching_main_only_mvp
description: Branching is MAIN-ONLY for the MVP (CHA-515 guard); multi-level fork-off-a-fork deferred to CHA-509; single-base read invariant holds
metadata: 
  node_type: memory
  type: project
  originSessionId: 9145745c-419b-40a4-ace0-816f4f7c39fe
---

Decision 2026-07-18: forking is permitted **only from `main`** for the MVP. Multi-level fork inheritance (fork-off-a-fork, `main → B → C`) is **DEFERRED**.

**Why:** fork inheritance is single-level only (CHA-178). The read planner enumerates ONE immediate parent and does NOT recurse, so forking off a non-main branch silently drops grandparent rows on read (with B unsnapshotted, C never sees main). Ancestry only flattens when an intermediate branch SNAPSHOTS (the snapshot writer bakes own ∪ base into cold); before that the single-base read misses the grandparent.

**Tickets:** **CHA-515** (Urgent, interim) — fail-fast guard in `create_branch` rejecting a non-main `source_branch` (the MVP-safety gate). **CHA-509** (parent) — real multi-level inheritance (chain of base cold sources across planning + read-fold + the audit mirror); removes CHA-515's guard.

**Single-base invariant (today):** `Plan.base_cold_storage: Option<BaseColdStorage>` = one immediate parent, capped at the fork seq; `AuditPlan` mirrors it (`base_cold_*_segments` / `base_audit_seq_cap`). Do NOT build or assume multi-level inheritance in any branching/lineage/audit work; treat a non-main fork source as unsupported.

**Key code:** `create_branch` — `crates/penca-api/src/write/mod.rs:777` (CHA-515 guard). `enumerate_base_cold_source` — `crates/penca-api/src/query/meta_plan.rs` (`TODO(CHA-509)`). `fold_in_base_cold_source` — `crates/penca-merge/src/lib.rs`.

CHA-433's PR #317 merge carries this single-level base-cold source **unchanged** (unioned it with the retention floor, no recursion added) — consistent with this decision. Relates to [[project_cha433_plan_time_retention_floor]] and [[project_branch_create_flush_to_cold]]. Note: CHA-514's descendant-audit-below-fork hazard applies even under single-level main-only forking (a direct child of main auditing below its fork still needs main's pre-fork history), so it is NOT blocked on CHA-509.
