---
name: save-memory
description: Save or update a memory entry. Always targets the **current repo root's** `.claude/memory/`, resolved via `git rev-parse --show-toplevel`; never the runtime `~/.claude/projects/<slug>/memory/` path.
allowed-tools: Bash Read Write Edit
---

# Save or update a memory entry

Invoke this whenever you decide to persist a fact, preference, or rule that should survive past the current conversation. The skill enforces the in-repo path so memory writes can never silently land outside git.

## The rule

**Memory writes target the current repo root's `.claude/memory/` — resolved as `$(git rev-parse --show-toplevel)/.claude/memory/<entry>.md`.** Never use the runtime symlink path (`~/.claude/projects/<slug>/memory/...`).

Why `git rev-parse --show-toplevel`: it's the canonical resolver for "the working tree git sees right now," invariant under whatever shell the session was started in. Anchoring writes to that path keeps memory edits inside git no matter what.

Why never the runtime path directly: it's usually a symlink into the repo (per ADR 0016, set up by `just memory-symlink-bootstrap` on a fresh checkout). On a fresh machine before that bootstrap runs, the runtime path is a non-symlinked harness-created directory — a write succeeds *silently* but the entry lands outside git: invisible to other machines, never reviewed in a PR. Writing to the repo-root path directly is invariant under that failure mode and the write surfaces in `git status` immediately.

## Procedure

1. **Resolve the current repo root** via `git rev-parse --show-toplevel`. The memory directory is `<that path>/.claude/memory/`.

2. **Verify the memory directory exists.** If `<toplevel>/.claude/memory/` does not exist, stop and tell the user — this likely means a pre-migration checkout. Don't create it ad-hoc.

3. **Write the entry file** with this frontmatter contract:
   ```markdown
   ---
   name: {{short title}}
   description: {{one-line summary; used by future sessions to decide relevance}}
   type: {{user | feedback | project | reference}}
   ---

   {{body — for feedback/project entries, structure as: rule/fact, then **Why:** and **How to apply:** lines}}
   ```

   Use the `Write` tool with the absolute path from step 1.

4. **Update the index.** Add (or update) the entry's one-line summary in `<toplevel>/.claude/memory/MEMORY.md` under the appropriate section. Keep the line under ~150 chars — `MEMORY.md` is loaded at session start as part of the prompt-cache prefix; brevity matters.

5. **Do not auto-commit.** Memory edits ride along in the current branch's next commit (or `/dream`'s curation PR). Leaving the file uncommitted is correct — `git status` carries the change for the next PR.

## Editing an existing entry

Use `Edit` against `<toplevel>/.claude/memory/<entry>.md` (`<toplevel>` from `git rev-parse --show-toplevel`). Don't append dated "UPDATE …" notes — rewrite the rule cleanly. If the underlying fact changes substantively, also revise the entry's `description:` frontmatter and its line in `MEMORY.md`.

## What NOT to save

Skip entries that are:
- Already covered in `CLAUDE.md`, `docs/` (especially `docs/style-guide.md`, `docs/decisions/`), or a skill body.
- About code patterns, file paths, or architecture derivable from the current tree.
- Ephemeral task details that won't recur.

When in doubt, ask whether the rule would be load-bearing for a future session three months from now. If not, skip — or better, route to the right docs/skill home rather than memory.

## Related

- ADR 0016 (`docs/decisions/0016-memory-store-in-repo-via-symlink.md`) — the architecture this skill enforces.
- `/dream` — bulk curation: merges duplicates, drops stale entries, promotes load-bearing rules into committed code (`docs/`, skills).
