---
name: feedback-consistent-window-arg-shape
description: "Don't mix IntegerRange-struct and split from/to scalar args for the same conceptual window; unpack to scalars internally, IntegerRange only at the proto boundary"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 3fd45a36-f796-458c-a0f9-0ce3fd79b480
---

The user dislikes passing the same conceptual `[from, to)` window two different ways across the code. In CHA-429 the audit/merge path had `seq_from`/`seq_to` as split `i64` scalars but `cold_committed_at`/`hot_committed_at` as `IntegerRange` structs — they wanted it uniform.

**Why:** mixed shapes for one concept are noise — a reader can't tell why one window is a struct and another is two scalars, and it litters every signature that threads both axes.

**How to apply:** in Penca merge/audit/plan code, pass committed_at and tx_seq_num windows as split `Option<i64>` `(from, to)` scalars in *internal* signatures; keep the proto `IntegerRange` type only at the boundary (PlanResponse / ReadDataRequest / AuditDataRequest), unpacking `r.min`/`r.max` at the penca-api edge. This matches the consumers, which are already scalar — `build_committed_at_filter(from, to)`, `build_commit_seq_num_filter(from, to)`, the hot read's `min_committed_at_micros`, and `AuditRowFilter{from_micros,to_micros,from_seq,to_seq}`. Verify the direction against actual consumers before committing — the user defers to analysis, but the default is "scalars in, IntegerRange only at the wire." Decide the arg shape up front so new params (e.g. a seq bound added later) land in the agreed shape, not a fresh struct. Related: [[feedback_clippy_not_in_cargo_check]] (splitting a struct into scalars can push a fn past the 8-arg clippy limit — add `#[allow(clippy::too_many_arguments)]` with a one-line "irreducible" note).
