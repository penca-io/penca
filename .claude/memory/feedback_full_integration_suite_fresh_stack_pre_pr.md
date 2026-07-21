---
name: feedback_full_integration_suite_fresh_stack_pre_pr
description: "Pre-PR gate is the FULL integration suite on a FRESH stack, never a hand-picked subset — subsets miss contract-change fallout"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: a5550c71-2323-4b0a-829d-65d378434a04
---

Before declaring a PR ready (and before enabling auto-merge), run the **full** integration suite — `tests/integration/integration_*.py`, all of it — on a **fresh** stack. Do NOT substitute a hand-picked subset of the "tests that touch the changed paths."

**Why (the CHA-507 miss):** CHA-507 changed `audit_data`'s default contract (author/comment became opt-in via `include_tx_metadata`). A contract/schema change ripples to *every* test that exercises that surface, not just the new red-tests. I ran subsets (RT1/RT2/lifecycle/inheritance) that looked green, but the merge-queue CI ran the full suite and caught 6 pre-existing audit tests still asserting the old always-present schema (`integration_query_test.py`, `integration_delete_pk_widening_test.py`, `integration_tx_framing_test.py`). A subset you pick by hand cannot cover tests you don't know are affected. **How to apply:** subsets are for fast inner-loop iteration only; the pre-PR gate is the whole suite.

**Fresh stack, not the kept-up one.** The full suite must run against clean volumes like CI does: `just penca-down` (it's `docker compose down -v` — removes postgres + seaweedfs volumes) then `just penca-up`, then pytest. Running the *full* suite against a long-lived stack that prior partial runs have poked produces dozens of **state-pollution** false failures (whole test classes that assume near-clean catalog/branch/DB state). The kept-up-stack workaround in [[reference_vm_task_limit_docker_test_workarounds]] is safe only for **subsets**; for the full suite it is misleading. Signature of pollution vs. a real regression: pollution failures are spread across many unrelated files/classes; a real regression from a scoped change clusters in the related tests.
