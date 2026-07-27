---
name: project_branching_main_only_mvp_single_base
description: "MVP branching is MAIN-ONLY (fork only from main); multi-level fork-off-a-fork inheritance is DEFERRED (CHA-509), guarded fail-fast by CHA-515"
metadata: 
  node_type: memory
  type: project
  originSessionId: a5550c71-2323-4b0a-829d-65d378434a04
---

**Decision (2026-07-18): branching is MAIN-ONLY for the MVP** — you may fork only from `main`. Multi-level fork inheritance (fork-off-a-fork) is DEFERRED.

**Why:** fork inheritance is single-level only (CHA-178). The read planner enumerates ONE immediate parent and does NOT recurse, so forking off a non-main branch silently drops grandparent rows on read (`main → B → C` with B unsnapshotted: C never sees main). Ancestry only flattens when an intermediate branch SNAPSHOTS (the snapshot writer bakes own ∪ base into cold); before that, the single-base read misses the grandparent.

**Tickets:**
- **CHA-515** (Urgent, interim): fail-fast guard in `create_branch` rejecting a non-main `source_branch` — the MVP-safety gate. Removes once CHA-509 lands.
- **CHA-509** (parent/umbrella): real multi-level inheritance (chain of base cold sources across planning + read-fold + the audit mirror). Removes CHA-515's guard. (NB: CHA-509 also shipped the persist/snapshot two-phase split — [[project_cha432_durable_substrate]] neighborhood.)

**Single-base invariant holds today** (do NOT build or assume multi-level):
- `Plan.base_cold_storage: Option<BaseColdStorage>` = one immediate parent, capped at the fork seq.
- `AuditPlan` has parallel single-base fields (`base_cold_*_segments` / `base_audit_seq_cap`). CHA-507 added `base_tx_log_segments` alongside them — also single-level (the one immediate parent's cold tx_log, to reattach inherited rows' author/comment). Consistent with the invariant; do not generalize it to a chain.
- Treat a non-main fork source as unsupported (CHA-515 enforces at `create_branch`).

**Key code:**
- `create_branch` — crates/penca-api/src/write/mod.rs:777 (CHA-515 guard goes here).
- `enumerate_base_cold_source` — crates/penca-api/src/query/meta_plan.rs:1005 (TODO(CHA-509) recursion point).
- `fold_in_base_cold_source` — crates/penca-merge/src/lib.rs (read-side fold).

**Two facts carried over from the folded-in duplicate (restored 2026-07-27 — deleting that file dropped them):**
- **[[CHA-514]]'s descendant-audit-below-fork hazard applies even under single-level main-only forking** — a direct child of `main` auditing below its own fork point still needs `main`'s pre-fork history — so CHA-514 is **NOT** blocked on [[CHA-509]]. This is a scheduling claim about a live ticket and is not derivable from the code.
- **[[CHA-433]]'s PR #317 merge carries this single-level base-cold source unchanged** — it unioned it with the retention floor and added no recursion.
