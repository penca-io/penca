---
name: feedback_no_background_git_commits
description: Never background a chain that ends in git add/commit while continuing foreground work — pre-commit stash + staging race bundles foreign diffs into the wrong commit
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5e2dc0f8-5196-4877-8a1a-eb23cd5b2a6c
---

A backgrounded `cargo check && git add X && git commit` chain raced foreground commits in the same tree (CHA-417, 2026-06-10): the pre-commit hook stash/restore collided with the foreground's unstaged edits, the background `git add` staged its file into the *foreground's* next commit, and one commit silently bundled two kata candidates — caught only by `git show --stat`, fixed by a pre-push history split.

**Why:** git index + pre-commit stash are process-global per worktree; two writers interleave unpredictably.

**How to apply:** backgrounding the *build/test* part is fine, but the `git add`/`git commit` tail runs in the foreground, serialized with all other git activity. After any commit, verify `git show --stat HEAD` matches the intended file set before closing the kata task. Related: [[feedback_commit_before_kata_close_sha]].
