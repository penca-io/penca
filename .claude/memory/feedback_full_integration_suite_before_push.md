---
name: feedback_full_integration_suite_before_push
description: Branch PR CI SKIPS integration tests — run the FULL integration suite locally (fresh stack) after cross-cutting changes before pushing
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 9145745c-419b-40a4-ace0-816f4f7c39fe
---

The branch/PR CI **skips** the "Integration tests (Rust backend)" job (changed-paths gate); only the **merge-queue** CI runs the full integration suite on the merged result. So `just check` + a few targeted integration modules can all be green while the merge queue fails — the PR gets removed from the queue with "failed status checks."

**Why:** CHA-433 scope-B dropped catalog `default_retention_config`; I updated the retention test file but missed `integration_snapshot_durable_test.py` (a *different* feature's file that set retention at the catalog level) → `AttributeError: default_retention_config` → merge-queue CI failed on it 3+ times while I kept declaring it ready.

**How to apply — after any cross-cutting change** (removing/renaming a proto field, changing a shared helper, a broad reshape):
1. `rg` EVERY usage of the removed/renamed symbol across `tests/` + `packages/` + `crates/`, not just the file you're editing.
2. Run the **full** integration suite locally before pushing, not only the modules you touched.

**Running the full suite on this VM (the ~10min task kill vs ~17min suite):** bring the stack up once (`just penca-up`), source `docker/.client.env` + `docker/.baseline.env`, `export COMPOSE_PROJECT_NAME="penca-$(basename "$PWD")"` (`penca-penca`; it is exported by the Justfile, **not** in `docker/*.env` — see [[reference_vm_task_limit_docker_test_workarounds]]), then run pytest against the kept-up stack in **DISJOINT** file chunks (each file exactly once). NEVER re-run the same test file against a persistent stack: fixed-name tests (e.g. `branch_inheritance_read`) fail on the 2nd run with `AlreadyExistsError: catalog name already in use` — a **false** failure from state pollution, not a bug. `just penca-down` + `penca-up` resets to a clean stack. See [[reference_vm_task_limit_docker_test_workarounds]] and [[feedback_just_check_gate_trust]].
