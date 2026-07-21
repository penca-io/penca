---
name: dream
description: Curate the in-repo project memory store (.claude/memory/) — merge duplicates, drop stale entries, surface new patterns from recent sessions, and promote durable project-wide themes into docs/ and skill text. Applies all changes on a branch and opens a PR. Memory is version-controlled, so git is the review/backup/rollback mechanism — there is no staging dir or file backup.
allowed-tools: Bash Read Write Edit Glob Grep
---

# Dream: curate the project memory store

Read the project's memory store and recent session transcripts, then **apply** a curated memory store plus any theme promotions directly to the repo on a branch and open a PR. Memory lives in the git-tracked `.claude/memory/` directory, so every change is version-controlled — the PR is the review gate, and `git` is the backup and rollback path. There is no staging directory, no file backup, and no swap/prune dance.

Modeled on Anthropic's Managed Agents Dreams API (`platform.claude.com/docs/en/managed-agents/dreams`). The API version operates on managed `memory_store` and `session` resources; this skill operates on the in-repo memory store and local session transcripts.

## Inputs

- **Memory store (in repo, version-controlled):** `$(git rev-parse --show-toplevel)/.claude/memory/*.md`, index in `MEMORY.md`. This is the source of truth and the thing you edit — **not** the runtime `~/.claude/projects/<slug>/memory/` path (that's a symlink into here; editing it directly is the bug this skill was rewritten to avoid).
- **Sessions (optional, up to ~100):** recent JSONL transcripts under `~/.claude/projects/$(pwd | tr '/' '-')/*.jsonl`. These are *not* in the repo. Mine them for patterns to fold into the store.
- **Instructions (optional):** the user can scope the curation, e.g. "focus on coding-style preferences; ignore one-off debugging notes."

Invoking `/dream` is the authorization to curate, commit, push, and open a PR. The PR — not a mid-run prompt — is where the user reviews and approves. Never merge the PR (that is the user's, always).

## Procedure

### 1. Branch

VM-per-ticket default — plain branch from the repo root:

```bash
git checkout -b <user>/dream-curate-memory-<YYYY-MM-DD>
```

(Laptop-fallback worktree mechanics, if applicable, follow the repo's normal branching convention.)

### 2. Inventory the current store

```bash
MEM="$(git rev-parse --show-toplevel)/.claude/memory"
ls "$MEM"/*.md | grep -v MEMORY.md | wc -l
wc -c "$MEM/MEMORY.md"
SESS=~/.claude/projects/$(pwd | tr '/' '-')
ls "$SESS"/*.jsonl 2>/dev/null | wc -l
```

Read `MEMORY.md` and each entry body. Understand the store before changing it.

### 3. Identify candidates

Four dispositions:

- **Merge** — entries that say the same thing in different words, or cover the same rule from different angles (e.g. several proto-feedback entries → one; several naming-preference entries → one per coherent rule).
- **Drop** — entries that are superseded by newer state, contradicted by current code, about files/symbols/flags that no longer exist, already captured in committed code (an ADR, `docs/style-guide.md`, a service doc, a skill `SKILL.md`), or narrow / non-recurring (a one-off gotcha that won't recur often enough to earn its context cost).
- **Promote** — durable, project-wide rules that *aren't* already in committed code and should be. **Memory is not the home for durable conventions.** If a rule is load-bearing, promote it to docs/ or a skill **and drop it from memory in the same PR** (see step 5). Typical homes:
  - Coding conventions → `docs/style-guide.md`
  - Algorithm / design rationale → `docs/algorithms.md`, `docs/design-decisions.md`, or a new ADR under `docs/decisions/`
  - Service-specific design → `docs/services/<service>.md`
  - Recurring review pushbacks → `.claude/skills/review-pr/SKILL.md` ("What NOT to flag" or the relevant step)
  - Recurring planning rules → `.claude/skills/do-issue/SKILL.md`
  - **Corrections to a skill's own cheat-sheet/instructions** → fix that skill directly. An entry that only exists because a skill's text is wrong retires the moment the skill is fixed — this is the highest-value promotion.
- **Add** — patterns visible in recent sessions not yet in memory: repeated corrections, enthusiastically-accepted preferences, project-state facts (incidents, deadlines) future sessions need.

Memory should be tight. Default each candidate to the strictest disposition that fits — **promote** beats **keep** for any project-wide convention; **drop** beats **promote** for narrow gotchas.

### 4. Verify before changing

Code state is authoritative; do not drop or rewrite based on assumption.
- A memory cites a file path → confirm it exists (or doesn't).
- A memory cites a symbol → grep for it.
- A memory cites a deadline / "verified <date>" → check it against today.
- A memory claims a skill says X → read that skill and confirm before "fixing" it (the skill may already have changed).

If a rule is still valid but a cited symbol drifted, refresh the reference rather than dropping the rule.

### 5. Apply the changes in `.claude/memory/` (and promotion targets)

Edit the in-repo store directly:
- **Merge:** write the new combined entry, delete the merged-away files (`git rm` or `rm`), update any `[[links]]` in surviving entries that pointed at removed slugs, and update `MEMORY.md`.
- **Drop:** delete the files, update `MEMORY.md`.
- **Add:** write new entries (same frontmatter contract — `name`, `description`, `metadata.type`), add their index lines to `MEMORY.md`.
- **Promote:** apply the rule to its named target file (match the surrounding section style; never paste memory frontmatter into committed docs), **and delete the now-redundant memory entry in the same PR**, removing its `MEMORY.md` line. Because the docs change and the memory drop land atomically in one PR, there is no "retire later" deferral — that two-phase dance only existed when the store wasn't version-controlled. (Until the PR merges, `main` still carries the entry, so nothing is lost if the PR is revised.)

Keep `MEMORY.md` in sync with the actual file set — every index link must resolve, and every entry file must be indexed. Verify before committing:

```bash
MEM="$(git rev-parse --show-toplevel)/.claude/memory"
# index links that don't resolve to a file:
grep -oE '\(([a-z0-9_]+\.md)\)' "$MEM/MEMORY.md" | tr -d '()' | sort -u | \
  while read f; do [ -f "$MEM/$f" ] || echo "MISSING: $f"; done
# files vs index (empty = perfect match):
comm -3 <(grep -oE '\(([a-z0-9_]+\.md)\)' "$MEM/MEMORY.md" | tr -d '()' | sort -u) \
        <(ls "$MEM"/*.md | xargs -n1 basename | grep -v MEMORY.md | sort)
# no dangling [[links]] to removed slugs:
grep -rn '\[\[' "$MEM"/*.md
```

Do **not** write a `CHANGELOG.md` into `.claude/memory/` — the change log is the PR body (step 7), not a committed memory file.

### 6. Commit

Follow the repo's commit conventions (conventional commits; scope from `linear/labels.toml` — memory/skill changes use `docs(agent)`; omit scope when a change spans areas). Separate commits read best:
- one `docs(agent)` commit for the memory curation (merges / drops / adds / link fixes),
- one commit per promotion target group (e.g. `docs:` for `docs/style-guide.md`, `docs(agent):` for skill-text fixes), each dropping the corresponding memory entries.

Run any commit-msg/pre-commit hooks; fix subject-length / type / scope rejections rather than bypassing them.

### 7. Open the PR

Push the branch and open a PR (`gh pr create`; if `gh pr edit` is broken in this env, use `gh api -X PATCH` for later body edits). Stop after opening it — **never merge** (the user merges). The PR body is the change log:

```markdown
## Summary
<one-line: dream curation + promotions for <YYYY-MM-DD>>

## Merged (N → M)
- `old_a` + `old_b` → `new`: <rationale>

## Dropped — already captured / stale / narrow
- `old_c`: <which doc/ADR/skill covers it, or why it's stale (with the verification)>

## Promoted (rule moved to committed code; memory entry removed here)
- `old_d` → `docs/style-guide.md` (<section>): <rationale>
- `old_e` → `.claude/skills/review-pr/SKILL.md` ("What NOT to flag"): <rationale>

## Added
- `new_f`: <pattern + which sessions surfaced it>

## Net
Before: <N> entries, index <K> KB. After: <M> entries, index <K'> KB.
```

### 8. Report

Give the user the PR URL, the before/after numbers, and the top 3–5 changes worth their attention (highest-impact merges, riskiest drops, notable promotions). Note that review/rollback is via the PR and git — there is no separate file backup to manage.

## Notes

- **Git is the safety net.** Rollback is `git checkout main` / closing the PR; history is the backup. Don't reintroduce file backups, staging dirs, or `mv`/`rm -rf` swaps — they were a workaround for a non-versioned store and broke under the `.claude/memory/` symlink.
- A curated store changes prefix bytes → the next session pays one cold `cache_create` on the new index; subsequent sessions warm-hit the smaller prefix.
- Frequency: monthly is plenty for routine curation; run on demand when memory volume crosses a discomfort threshold, or right after a stretch of sessions that generated many new entries.

## Future: out-of-session API equivalent

A v2 could call the Anthropic API from a separate process (using `ANTHROPIC_API_KEY`) so the curation doesn't burn the current session's plan tokens. The contract — memory + sessions in, curated memory + promotions out as a PR — stays the same.
