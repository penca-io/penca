---
name: feedback_integration_suite_full_fresh_before_pr
description: "Pre-PR gate is the FULL integration suite on a FRESH stack — never a hand-picked subset. Branch PR CI skips the Rust integration job entirely (merge-queue only), so local is the only signal."
metadata:
  node_type: memory
  type: feedback
---

Before declaring a PR ready, run the **full** integration suite —
`tests/integration/integration_*.py`, all of it — on a **fresh** stack. Not a
hand-picked subset of "the tests that touch the changed paths."

**Why the subset never suffices:** a contract or schema change ripples to every
test that exercises that surface, not just the new red-tests. CHA-507 changed
`audit_data`'s default contract (author/comment became opt-in via
`include_tx_metadata`); the subsets I picked (RT1/RT2/lifecycle/inheritance)
were green, and the merge-queue run then caught 6 pre-existing audit tests
still asserting the old always-present schema, in three files I had no reason
to suspect. You cannot hand-pick tests you don't know are affected.

**Why local is the only signal:** branch/PR CI **skips** the "Integration tests
(Rust backend)" job via a changed-paths gate. Only the **merge queue** runs the
full suite, on the merged result — so `just check` plus targeted modules can all
be green while the queue fails and ejects the PR with "failed status checks."
CHA-433 scope-B dropped catalog `default_retention_config`; I updated the
retention test file but missed `integration_snapshot_durable_test.py` (a
*different* feature's file that happened to set retention at the catalog level)
→ merge-queue CI failed 3+ times while I kept declaring it ready.

**How to apply — after any cross-cutting change** (removing/renaming a proto
field, changing a shared helper, a broad reshape):

1. `rg` EVERY usage of the removed/renamed symbol across `tests/`, `packages/`
   and `crates/` — not just the file you're editing.
2. Run the full suite locally on a fresh stack before pushing.

**Fresh stack, not the kept-up one.** The full suite must run against clean
volumes like CI does: `just penca-down` (it's `docker compose down -v` — drops
the postgres + seaweedfs volumes) then `just penca-up`, then pytest. Running the
*full* suite against a long-lived stack that partial runs have already poked
produces dozens of **state-pollution** false failures — whole test classes
assume near-clean catalog/branch/DB state. Signature to tell them apart:
pollution failures spread across many unrelated files and classes, while a real
regression from a scoped change clusters in the related tests.

Subsets are for fast inner-loop iteration only. When iterating, `just
integration-test` takes **variadic prefixes** — `just integration-test lifecycle
query branch_persist` runs all three in a single compose up/down cycle. Docker
startup (postgres + seaweedfs + 5 penca services, ~60–90s) dominates the cost,
so pass every prefix you need in one call rather than looping; adjacent files
cost ~10s more and often catch regressions. The kept-up-stack workaround in
[[reference_vm_resource_limits_docker_disk_memory]] is safe for **subsets only**
— for the full suite it is misleading.

Related: [[feedback_just_check_gate_trust]],
[[feedback_slow_commands_capture_and_wait]].
