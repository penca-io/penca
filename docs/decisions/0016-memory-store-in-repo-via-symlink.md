# 0016 — Memory store lives in the repo via symlink

Status: Accepted

## Context

Claude Code keeps a per-project, file-based "memory" store under
`~/.claude/projects/<project-slug>/memory/`. The store accumulates:

* User behavioral preferences (e.g. "don't delegate via subagents",
  "discuss before implementing").
* Workflow / tool habits ("use `just`, never bare `uv run`").
* Strategic context the model should carry across sessions.
* Strong/repeated style corrections that haven't earned a docs home.

Today the store is local-only:

* It lives outside any git tree.
* It's invisible to collaborators and to other machines.
* Curation runs through `/dream`, which stages a curated copy under
  `memory-staging/<date>/` and asks the user to swap it in. PR #69
  added an explicit user-approved swap with a dated backup
  (`memory-old-<date>/`). PR #70 added a follow-up step to prune
  backups older than 30 days, because they otherwise accumulate
  indefinitely.

The swap + backup + prune machinery is non-trivial, and the whole
mechanism is reinventing version control on top of `mv` and `find
-mtime +30`. Three related observations forced the rethink:

1. **Memory should be reviewable.** A memory entry shapes how Claude
   behaves on every future session — it deserves the same review
   surface as code changes. Today there's no commit, no diff, no
   collaborator visibility, no `git blame`.
2. **Memory should be durable.** Today's "durability" is a local
   directory plus a dated backup directory. On a fresh machine,
   `rm -rf ~/.claude/...`, or an honest typo, the rules are gone.
   Git already solves this.
3. **Memory should be cache-stable.** Every session pays
   `cache_creation_input_tokens` for any change to the prompt-cache
   prefix (system prompt → CLAUDE.md → memory index) since the last
   cache hit, plus `cache_read_input_tokens` for the prefix bytes on
   every read. Today's mid-session memory writes silently mutate
   `MEMORY.md` and entry files in the runtime path, so every
   subsequent session on the machine pays a fresh `cache_create`
   for the new prefix. Bundling memory writes into PR-reviewable
   commits naturally smooths this: between merges the prefix is
   stable (warm cache hits), and the per-merge cold-start cost
   amortises across however many sessions run in the window
   between merges. The same principle already motivates
   `feedback_claude_md_stability` for `CLAUDE.md`; this ADR
   extends it to memory.

## Decision

**The memory store moves into the repo at `.claude/memory/`. Each
working directory (the main checkout + every worktree) has its
own per-cwd project slug under `~/.claude/projects/`, and each
slug's `memory/` is a symlink to that checkout's own
`<checkout>/.claude/memory/`. Memory is isolated by working
directory; the global state changes only when a PR merges.**

Concretely:

1. **Repo path.** Memory entries live at `<repo>/.claude/memory/`
   alongside the existing `.claude/skills/` directory. The
   `MEMORY.md` index and each entry's `.md` file are committed.

2. **Per-worktree symlinks, no central global path.** Claude
   Code already derives a per-cwd project slug
   (`/home/nico/Repos/penca` → `-home-nico-Repos-penca`;
   `/home/nico/Repos/penca/.claude/worktrees/foo` →
   `-home-nico-Repos-penca-.claude-worktrees-foo`) and creates a
   directory per slug under `~/.claude/projects/`. We piggyback
   on that:

   - Main checkout:
     `~/.claude/projects/-home-nico-Repos-penca/memory`
     → `/home/nico/Repos/penca/.claude/memory/`
   - Worktree `foo`:
     `~/.claude/projects/-home-nico-Repos-penca-.claude-worktrees-foo/memory`
     → `/home/nico/Repos/penca/.claude/worktrees/foo/.claude/memory/`

   Each session reads memory from its own slug — i.e. its own
   checkout's `.claude/memory/`. Sessions in different worktrees
   are isolated from each other and from main.

3. **Mid-session memory writes always land on the checkout's
   branch.** When Claude saves a new memory entry in a worktree
   session, the write goes through that worktree's symlink to
   that worktree's `.claude/memory/` — i.e. to that worktree's
   branch, as an uncommitted change ready to be committed and
   PR'd. Writes in the main checkout go to `main`'s working tree,
   also as uncommitted change. There is no shared mutable global
   state; "global" memory is whatever main's `.claude/memory/`
   contains *after merge*.

   No "no edits on main" rule is needed — memory writes are
   always local to the checkout they happen in. To propagate a
   change globally, commit it and PR it. To start a fresh
   workspace where memory mutations won't conflict with another
   PR's, open a new worktree.

4. **Worktree setup / teardown glue.** When a new worktree is
   created, its `~/.claude/projects/<derived-slug>/memory/`
   symlink doesn't exist yet — Claude Code would initialise it
   as an empty directory on first session. A `just worktree-new
   <branch>` recipe (or a thin `git worktree add` wrapper)
   handles both the `git worktree add` and the symlink:

   ```bash
   git worktree add -b "$branch" ".claude/worktrees/$branch"
   slug="-$(echo "$PWD/.claude/worktrees/$branch" | tr / -)"
   target_project="$HOME/.claude/projects/$slug"
   mkdir -p "$target_project"
   ln -s "$PWD/.claude/worktrees/$branch/.claude/memory" \
         "$target_project/memory"
   ```

   On `just worktree-remove <branch>`, the recipe removes the
   worktree and the symlink directory together so stale
   `~/.claude/projects/...` entries don't accumulate.

5. **`/dream` becomes worktree-and-PR shaped.** No more staging
   directory, no more swap, no more backup, no more prune:

   - Step 1–3 (inventory, identify candidates, verify) unchanged.
   - Step 4 (write the staging output) → write directly into the
     worktree's `.claude/memory/`. The worktree branch IS the
     staging area; the diff is the changelog.
   - Step 5 (report findings) summarises the diff for the user.
   - Step 6 (swap with backup) → **deleted**. Merging the PR is
     the swap; `git pull` on the main checkout deploys it.
   - Step 7 (prune backups) → **deleted**. Git history is the
     backup.
   - Step 8 (promotion PR) → unchanged in shape; for memory
     entries that should become committed conventions
     (`docs/style-guide.md`, ADRs, skill bodies), the same PR can
     remove the corresponding memory entry from `.claude/memory/`,
     making the promotion + retirement atomic.

## Why symlink over copy

A copy-based deploy (a `just deploy-memory` recipe that copies
`<repo>/.claude/memory/*` to `~/.claude/projects/<slug>/memory/`)
would also work, but creates two failure modes the symlink avoids:

* **Drift on missed deploy.** Forgetting to run the deploy after
  `git pull` leaves the runtime stale. The symlink resolves this
  with zero ceremony.
* **Dual-write rule needed.** Mid-session writes hit the runtime
  copy but not the repo source. A memory rule "always also write
  to the repo" could enforce the dual-write, but it's discipline
  rather than mechanism; the symlink makes it one write by
  construction.

## Why per-cwd isolation over a redirected single symlink

An earlier draft of this ADR had one symlink at the main
checkout's project slug and proposed *redirecting* its target to
the active worktree at session start. That required:

* A "no edits to `.claude/memory/` on main" rule, enforced
  manually until a pre-commit hook existed, so sessions in the
  main checkout couldn't mutate global state.
* A `just memory-attach` / `just memory-detach` recipe that
  swung the symlink target dynamically per session.

Both are unnecessary. Claude Code already gives every working
directory its own project slug under `~/.claude/projects/`; we
just pre-create the symlink at worktree-add time so the harness
finds it on first session instead of initialising an empty
directory. Each session is naturally isolated to its checkout —
main sessions touch main's `.claude/memory/`, worktree sessions
touch the worktree's, and the only path to global state is `git
push` + PR merge + `git pull`. No discipline rule, no dynamic
state, no risk of running with a stale or wrongly-pointed
symlink.

The latency between "Claude wants to remember X" and "Claude
remembers X across all sessions" is bounded by PR review + merge
— minutes to hours, not days. The user accepted this up-front:
mid-session memory additions don't need to take effect
immediately.

If a fast path is ever needed, two options stay open:

* A "fast-track memory PR" recipe (`just memory-quick "<entry>"`)
  that opens a one-file PR for trivial-to-review entries.
* An explicit override for emergency additions, with a follow-up
  PR required within N days.

Neither is needed for the initial implementation.

## What this replaces

* PR #69's swap + backup machinery in `/dream` step 6.
* PR #70's prune step in `/dream` step 7.
* The `memory-staging/` and `memory-old-*` directory conventions
  entirely.

PR #70 stays in flight as a transitional fallback: while the live
store is still local, prune is the right hygiene step. Once this
ADR is implemented, both the swap+backup and the prune logic are
removed from `/dream`.

## Implementation

This PR lands the migration. Steps 1, 2, 4, 5, 6 are in the diff;
step 3 is a one-shot user action with a `just` recipe.

1. **Migrated 15 of 16 runtime entries** into `<repo>/.claude/memory/`.
   `project_product_strategy.md` is held back pending
   [CHA-209](https://linear.app/chapala/issue/CHA-209) — it carries
   competitive positioning notes that belong in the sibling
   non-backend repo, not in this repo.
2. **Committed in this PR.**
3. **Symlink the main-checkout's runtime path to the in-repo store
   (manual, once per machine after pulling this PR):**
   ```bash
   just memory-symlink-bootstrap
   ```
   The recipe is idempotent. It refuses to overwrite an existing
   different symlink target, and it moves any pre-existing memory
   directory to `~/.claude/projects/<slug>/memory-pre-symlink/` as
   a safety net (delete after a week of confirmed-working sessions).
4. **`just worktree-new <branch>` / `just worktree-remove <branch>`
   recipes** added; they wrap `git worktree add` / `git worktree
   remove` and create/remove the per-worktree symlink under
   `~/.claude/projects/<derived-slug>/memory`.
5. **`/dream` skill changes are deferred** to a follow-up. The
   step-6 swap and step-7 prune still exist in the skill body
   because the live runtime is still backup-managed until
   `memory-symlink-bootstrap` runs; once that happens (and the
   migration is confirmed working), those steps come out of the
   skill.
6. **`feedback_worktrees.md` updated** in this PR to require
   worktrees for any change that affects Claude behavior
   (`.claude/memory/`, `.claude/skills/`, `CLAUDE.md`), not just
   code. The pre-commit / CI mechanical enforcement is still a
   follow-up.

Close PR #70 prune step as superseded once the `/dream`
simplification follow-up lands.

## Consequences

**Positive.**

* Memory is git-managed: durable, reviewable, multi-machine
  consistent, blameable.
* `/dream` simplifies — staging, swap, backup, and prune all
  retire in favour of one PR per curation pass.
* Memory promotions to docs/skills become atomic with the
  corresponding memory-entry removal in the same PR.
* No "no edits on main" rule needed; no symlink-redirection state
  to manage. Working-directory-scoped isolation is provided by
  Claude Code's per-cwd project slug, which we just piggyback on.
* No new tooling beyond a `just worktree-new`/`worktree-remove`
  wrapper around `git worktree`.
* Prompt-cache prefix is stable between merges. Mid-session memory
  writes no longer silently invalidate the cache for every future
  session on the machine; cold-start `cache_create` cost gets paid
  once per merge and amortises across all sessions in the window.
  Pairs with `feedback_claude_md_stability` for the same reason.

**Negative.**

* Memory writes are no longer instant — they require a PR. The
  user has accepted this trade.
* Worktree creation/removal must go through the `just` wrapper
  (not bare `git worktree add`/`remove`), otherwise the symlink
  isn't set up / torn down. Documented in
  `feedback_worktrees`; a `git worktree add` hook is a possible
  follow-up if the wrapper gets skipped in practice.
* A `.claude/memory/` directory in the public repo exposes some
  preference content. Tracked in
  [CHA-209](https://linear.app/chapala/issue/CHA-209) — entries
  with competitive positioning or other sensitive material
  (`project_product_strategy.md` is the obvious one) move to the
  sibling non-backend repo as part of that ticket; they don't
  ride along into `<repo>/.claude/memory/`.
