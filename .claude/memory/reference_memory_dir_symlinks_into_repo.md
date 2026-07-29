---
name: reference_memory_dir_symlinks_into_repo
description: "The auto-memory dir symlinks into the penca repo's tracked .claude/memory/ — saving a memory dirties the working tree and can void a SHA-bracketed gate or leak into an unrelated PR"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05a4b981-1ec9-4115-91f7-2cdcd554c7ee
  modified: 2026-07-29T03:34:03.750Z
---

`/home/exedev/.claude/projects/-home-exedev-code-penca/memory/` is a **symlink to `/home/exedev/code/penca/.claude/memory/`**, and those `.md` files are **tracked in git**. So every memory save shows up in `git status --porcelain` for the repo.

Two consequences:

1. **It can void a gate.** The pre-PR gate records `GATE_START_DIRTY` and requires it empty (see [[feedback_full_integration_suite_fresh_stack_pre_pr]]). Saving a memory mid-gate dirties the tree. A markdown-only diff has no bearing on Rust/Python behavior, so the *result* still holds — but say so explicitly rather than letting a non-empty `DIRTY` line silently discredit a green run.
2. **It can leak into the wrong PR.** `git add -A` / `git commit -a` on a feature branch will sweep memory edits into that PR's diff. Stage memory files deliberately, in their own commit, or leave them uncommitted until the feature branch is out of the way.

Verify with `readlink -f <memory-dir>` — do not assume the memory dir is outside the project.
