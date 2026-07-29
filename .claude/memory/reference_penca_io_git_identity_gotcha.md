---
name: reference_penca_io_git_identity_gotcha
description: penca-io/penca checkout has NO git user configured → commits auto-author as exedev@<dev-vm> (the identity CHA-150 scrubbed); set repo-local user to Nico Bautista Hobin <nico@penca.io> before committing
metadata: 
  node_type: memory
  type: reference
  originSessionId: 5eb2b7dd-8f01-4e0f-8623-c88a865d0251
  modified: 2026-07-21T22:06:09.704Z
---

A fresh `penca-io/penca` checkout ships with **no git `user.name`/`user.email`** configured (local or global), and **nothing in `just bootstrap` / `init-agent-tools` sets it** (verified 2026-07-29 — no `user.email` anywhere in the Justfile or `scripts/`). git then auto-guesses `exe.dev user <exedev@fabricdb-dev-1.exe.xyz>` — the exact dev-VM author identity CHA-150's squash scrubbed (~1,700 commits) to keep off the going-public repo. The squashed "Initial commit" is correctly authored `Nico Bautista Hobin <nico@penca.io>`.

Before committing here, set the repo-LOCAL identity to match:

    git config user.name "Nico Bautista Hobin"
    git config user.email "nico@penca.io"

It lives in `.git/config`, so a fresh clone / new VM must re-set it. Verify with `git log -1 --format='%ae'` after the first commit; re-author a stray `exedev@` commit with `git commit --amend --reset-author --no-edit` BEFORE closing the kata task (avoids the stale SHA in [[feedback_commit_before_kata_close_sha]]). See [[project_repo_moved_merge_queue_gate]].
