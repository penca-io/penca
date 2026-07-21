---
name: Git worktree usage — laptop-only fallback; VM-per-ticket is the default
description: VM-per-ticket (default) uses plain `git checkout -b` from repo root, no worktree. Laptop fallback still goes through `just worktree-new` / `worktree-remove`.
type: feedback
originSessionId: e352c7e5-699c-4e48-a71b-ea7b5149a7df
---

## VM-per-ticket (default): plain git branch

Each ticket runs end-to-end on its own VM, cloned from a pre-set-up [exe.dev](https://exe.dev/) base image. The VM provides the process and memory isolation worktrees used to give us — no worktree spawn, no re-rooting, no per-worktree symlink setup. Branch creation:

```bash
git checkout -b nhobin219/cha-XX-description
# (work happens at the repo root)
```

Cleanup post-merge:

```bash
git checkout main && git pull && git branch -d nhobin219/cha-XX-description
```

**Why:** laptop Rust compilation + concurrent `just integration-test` was crashing with more than one ticket at a time. Switching to one disposable VM per ticket removes the contention without bending skill ceremony around resource limits.

**How to apply:** `/do-issue`, `/clean-code-refactor`, and `/dream`'s promotion PR step all assume this default. Do not invoke `just worktree-new` / `worktree-remove` unless explicitly working from the laptop.

## Laptop fallback: worktrees via `just worktree-*`

When working from the laptop (no VM), worktree machinery still applies. **Always** through `just worktree-new <branch> <dir>` and `just worktree-remove <branch> <dir>` — bare `git worktree add` skips two things you need:

1. **Per-worktree memory symlink** (ADR 0016). Each worktree gets its own `~/.claude/projects/<slug>/memory` symlinked to its own `.claude/memory/`, so parallel worktrees don't stomp each other. Sessions started in a worktree without the symlink would land memory writes in an empty harness-created directory and lose them on worktree removal.
2. **Worktree-local `.venv` with the `unset VIRTUAL_ENV` guard.** Skipping the unset rewrites the parent worktree's `.pth` files to point at this worktree, breaking the parent on removal with `ModuleNotFoundError` for `penca` / `penca_proto`.

**How to apply (laptop only):**

- Always under `.claude/worktrees/<dir>` — never sibling directories like `~/Repos/penca-cha-xyz/`. Sibling worktrees are invisible to the VSCode workspace; under-`.claude/` worktrees show up in the same workspace.
- Before `just worktree-remove`, confirm which worktrees to remove. During CHA-52 cleanup, an active sibling worktree was nearly force-removed alongside the merged branch — one merged branch does not imply all related worktrees are done.

## Detection

`pwd | grep -q '/.claude/worktrees/'` matches → laptop session under a worktree. Otherwise assume VM-per-ticket and act accordingly (plain `git checkout -b` from repo root, no `just worktree-*`).
