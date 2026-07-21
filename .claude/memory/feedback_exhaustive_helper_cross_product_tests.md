---
name: feedback-exhaustive-helper-cross-product-tests
description: Penca timestamp helpers (cleanup/purge/snapshot/clamp) need unit tests covering every reachable cross product of input states; helpers must take primitives so tests skip DB fixtures.
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 6911b277-2ba9-4e80-a973-395251d301fb
---

For Penca helpers that compute timestamps from system state — cleanup cutoff, Purge eligibility watermark, snapshot upper bound, Persist open-tx clamp, etc. — unit tests must cover **every reachable cross product** of input states, not just happy-path examples. The user has emphasized this multiple times when scoping CHA-221 and CHA-233.

**Why:** these helpers are the load-bearing correctness primitives. A single missed combination (e.g., `purged_at == cleanup_started_at` vs `purged_at > cleanup_started_at` deciding which clamp dominates) can silently break the live-query safety chain. Integration tests catch the obvious cases; only an exhaustive matrix catches the edge interactions.

**How to apply:**

1. **Helper signatures must be pure** — take inputs as plain primitives or small structs, return a typed output struct. No DB connection, no SQL execution inside the helper itself. The SQL composition wrapping the helper can have its own (smaller, integration-style) tests, but the timestamp logic belongs in a unit-testable function.
2. **Enumerate the cross product as a matrix in the ticket's scope of work**, not as prose. The user wants to see the dimensions and values to cover spelled out before implementation starts. Format: a markdown table with `dimension | values to cover`.
3. **The cross product is small once structural constraints are applied** — `query_timeout_seconds` can be fixed across tests; many dimensions collapse to a handful of values (`<`, `==`, `>` relative to another input; `0` vs positive; empty vs single vs multiple). The user has explicitly said the matrix shouldn't be too large.
4. **Include permutation invariance tests** for inputs that are sets/maps (e.g., per-table `purged_at` values) — verify the helper's output doesn't depend on iteration order.
5. **Cover the "empty input" branch explicitly** — empty Persist set, empty open-tx set, empty table set. These often have sentinel behavior (e.g., "treat as 0" or "skip the DELETE").

Related: the Purge-watermark invariant in `.claude/skills/review-pr/SKILL.md` (Step 5: read `table_purge_metadata.purged_at_micros` directly) — same flavor of "name the load-bearing distinction so future contributors don't substitute a near-equivalent".
