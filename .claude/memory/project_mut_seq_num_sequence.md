---
name: project_mut_seq_num_sequence
description: "mut_seq_num is a lock-free per-(table,branch) PG sequence, NOT a locked counter row — within-tx data ordering only"
metadata: 
  node_type: memory
  type: project
  originSessionId: 023ecd53-07c0-4306-8b1d-b7a53bc9f7a2
---

CHA-431 (tx_seq_num epic deliverable #4) allocates `mut_seq_num` from a **per-`(table, branch)` Postgres `SEQUENCE` (`nextval`)**, NOT a `mut_log_seq_num` counter ROW. The counter-row design in the parent epic CHA-400 was an over-generalization of `tx_seq_num`'s locked-row mechanism.

**Why a sequence is correct + better:** `mut_seq_num` is consulted in merge resolution (`ORDER BY tx_seq_num DESC, mut_seq_num DESC` and the tombstone-shadow lex predicate) **only when `tx_seq_num` ties** — and since `tx_seq_num` is the gapless per-branch commit serial (unique per committed tx), it only ties for rows from the **same tx**. So `mut_seq_num` is purely *within-tx* ordering and is never compared cross-tx. It therefore needs **neither atomic-with-visibility nor gaplessness** (the two properties that force `tx_seq_num` onto a lock-held counter row, see CHA-428). A locked counter row would hold a row lock from a tx's first data write to commit, serializing every concurrent writer to that table for the whole tx — a real throughput cliff. `nextval` is lock-free.

**How to apply:** sequence named from `(table_uuid, branch_uuid)` like `upsert_log_table`/`delete_log_table` (NO `catalog_uuid` → no threading into `create_data_tables`). Stamp via column **`DEFAULT nextval('<seq>')`** on BOTH upsert_log + delete_log so every writer (MutateData, CreateBranch-materialize, genesis, branch-merge) auto-allocates — no per-writer plumbing. `CREATE SEQUENCE … START 0 MINVALUE 0 **CACHE 1**` in `create_data_tables`, `DROP SEQUENCE` AFTER the log-table drops in `drop_data_tables` (the `DEFAULT` expr depends on the sequence). Deletes-first = run the delete INSERT before the upsert INSERT in `mutate_data`.

**CACHE 1 is LOAD-BEARING — verified the hard way (commit 72996bbb).** Do NOT "set a generous CACHE": `CACHE > 1` reserves a per-backend block of sequence values, so successive `mutate_data` calls in ONE penca tx that land on different pooled PG backends draw from different blocks — a *later* call on a lower-block backend gets a *smaller* `mut_seq_num` than an *earlier* call, inverting update-then-delete / insert-then-delete within one tx (RT2 `test_resolution_hot` went `u`=VISIBLE when it must be DELETED under CACHE 64). `CACHE 1` makes every `nextval` hit the real counter → globally monotonic in allocation order. Gaps are still fine; per-backend *blocks* are not.

Per-table scope chosen over per-branch/global for hard cross-table isolation; many sequence objects have **no runtime hot-path cost** (a backend only touches sequences it uses) and the aggregate footprint is in line with the existing per-`(table,branch)` upsert/delete log tables.

Contrast: `tx_seq_num` (CHA-428) MUST stay a locked counter row (atomic-with-visibility). Both are branch-local and restart per branch ([[project_branch_create_flush_to_cold]]).
