---
name: feedback_review_subagent_head_hazard
description: /review-pr subagents leave HEAD on main even with isolation:worktree — always re-checkout the branch after, never rely on the worktree flag alone
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 9972813c-cbb5-4ea5-bc7f-ab53959f8e3c
---

The `/do-issue` `orch:spawn-review` dispatch spawns a `/review-pr <PR#>` subagent. That subagent runs `git fetch origin pull/N/head` and `git checkout` in the **same (non-isolated) working tree** the main session is on, and tends to leave HEAD on `main` when done ("working tree restored to main"). After it returns, the main session can find itself detached from the feature branch.

**Why:** multiple times now, after a review round, the shared tree was left off the feature branch (commits safe — pushed to origin/PR — but the session was off-branch); had to re-checkout to continue the drain. This happens **even with `isolation: "worktree"`**: on 2026-06-01 (CHA-369) HEAD still landed on `main`, and on 2026-06-02 (CHA-92) the shared tree ended up **detached** at the branch tip so the next commit landed as `[detached HEAD <sha>]` with the branch ref left behind. The subagent's own "restore to main" cleanup resets the shared tree regardless of the worktree, so the worktree flag is **not** a reliable guard on its own.

**How to apply:**
- **Always** re-checkout after every `orch:spawn-review` subagent returns — this is the load-bearing step, not optional. Run `git rev-parse --abbrev-ref HEAD`; if it moved to `main` or shows `HEAD` (detached), restore with `git checkout -B <branch> <tip-sha>` (the `-B` form fast-forwards the branch ref to the tip and reattaches in one step; plain `git checkout <branch>` suffices when the branch ref didn't move). Also `git worktree prune` if a stray worktree lingers. Commits are safe (pushed); only the working-tree HEAD drifts.
- Watch commit output for `[detached HEAD <sha>]` — that's the tell the branch ref was left behind; fix immediately with `git checkout -B <branch> <sha>` (no history lost; the new commit's parent is the branch's old tip).
- `isolation: "worktree"` is still worth setting (it reduces, not eliminates, the blast radius) but treat it as defense-in-depth, never as the fix — the re-checkout above is mandatory either way.

Relates to [[feedback_no_subagents]] (the spawn-review fresh-eyes pass is the sanctioned in-session subagent exception).
