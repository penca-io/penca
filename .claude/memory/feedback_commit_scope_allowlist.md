---
name: feedback_commit_scope_allowlist
description: "commit-msg hook enforces a fixed scope allowlist (bounded-context names, not crate names) and a fixed type set excluding `style`"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 9972813c-cbb5-4ea5-bc7f-ab53959f8e3c
---

`scripts/check_commit_msg.py` validates conventional-commit `<type>(<scope>)` against fixed sets. A commit is **rejected** (and the `kata close --commit` then records the wrong/stale SHA) if either is off:

- **Types** = `{feat, fix, refactor, perf, test, docs, build, chore}`. `style` is NOT allowed — for `cargo fmt` fixups use `chore: cargo fmt`, not `style:` (the `/do-issue` orch:open-pr text says `style:` but predates this hook).
- **Scopes** = bounded-context names: `admin, agent, api, branch, ci, cold, db, deps, docker, format, grpc, hosted, hot, infra, lifecycle, merge, meta, perf, proto, query, schema, sql, storage, storage-cold, storage-hot, storage-meta, write`. **Crate names are NOT scopes** — code in `crates/penca-datafusion` commits under `query` (or `schema`/`sql`), not `datafusion`. Omit the scope entirely when a change spans areas.

**Why:** I lost a commit + recorded a stale SHA on a kata close when `chore(datafusion)` was rejected mid-`/do-issue` drain.

**How to apply:** map crate → bounded-context scope before committing; for fmt fixups use `chore`. See [[feedback_use_just_commands]].
