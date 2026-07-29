---
name: do-issue
description: Drive a Linear ticket end-to-end through Penca's kata-task-graph + /loop-drain workflow. One human gate (plan approval at Step 3); everything else — red-tests, implementation, pre-PR cleanup passes, PR open, post-open subagent review — runs autonomously via the drain loop, driven by an explicit blocked-by graph emitted at Step 2.
argument-hint: "<Linear issue URL or CHA-NN>"
allowed-tools: Agent Bash Edit Glob Grep Read Skill Write
---

# Do a Linear issue

Drive a Linear ticket end-to-end. The workflow is encoded as a **kata task graph** emitted at Step 2 and consumed by a **/loop drain** that runs from Step 4 to PR merge. Graph nodes: red-tests, implementation work, three **orchestration tasks** (`orch:run-cleanup`, `orch:open-pr`, `orch:spawn-review`). Edges: kata `--blocked-by` links. The drain is a uniform consumer — it claims whatever `kata ready` returns and dispatches by task kind.

**One human gate: Step 3 plan approval** (user flips `plan-draft` → `approved`). Everything else is mechanism-bound — the dependency graph encodes the workflow, not the prose. Roborev findings, `/review-pr` findings, and cleanup-pass candidates all enqueue with `--label approved` and join the drain automatically, dynamically extending downstream `orch:*` tasks' `--blocked-by` so PR open waits on them.

**Ticket:** "$ARGUMENTS"

## When to use this skill

Use for any Linear ticket where:
- A reader has to make a non-obvious design decision (multiple files, new RPC, schema change, behavior change), OR
- The acceptance criteria list more than one behavioral test.

**Skip this skill** for typo fixes, one-line config tweaks, dependency bumps, or any change where writing a plan would be longer than the diff. Those go straight to a branch + commit.

**Refactor tickets hand off to `/clean-code-refactor` after Step 1.** If the digested ticket is a refactor (label = `refactor`, or the description's core ask is "rework X to shape Y" with no behavior change), invoke `/clean-code-refactor` once Step 1 is complete. That skill's Assess → Plan → Execute loop replaces Steps 2–5 here; this skill retains Step 1 (status + digest) and Step 6 (post-merge cleanup).

If the user invokes this on something trivial, say so and ask before continuing.

## Why kata, not Linear comments

Linear is the golden source of truth for *issues*. Intra-task chatter — plan items, red-phase tasks, implementation steps, roborev findings, PR-review follow-ups — does not belong there. Linear MCP round-trips dominated turn latency on the old flow.

kata (local-first SQLite issue tracker) is the per-ticket task queue:
- Tasks scoped by a `cha-NNN` label sourced from the Linear ticket identifier.
- Task kind expressed via additional labels: `red-test`, `impl`, `orch:run-cleanup`, `orch:open-pr`, `orch:spawn-review`. Source markers on late-arriving findings: `roborev`, `review-pr`, `cleanup-pass`, `agent-discovered`.
- Workflow encoded as a kata `--blocked-by` graph at Step 2; the drain consumer at Step 5 just respects it.
- Roborev / `/review-pr` / cleanup-pass enqueuers all auto-approve their findings (`--label approved`) and dynamically extend in-flight `orch:*` tasks' `--blocked-by` so PR open waits on inbound work.

Linear writes stay minimal: `state="In Progress"` plus a "Picked up by <hostname>" comment at the start (Step 1), a plan-visualization comment at the Step-3 approval gate, and `state="In Review"` + PR URL when the `orch:open-pr` task runs. Everything else is local.

## Methodology rules (apply throughout)

### Reasoning effort
Run **Step 1 (investigate / digest the ticket)** and **Step 2 (plan — emit the kata task graph)** at **xhigh** reasoning effort, every time — regardless of the session's default. These two phases fix the mechanism-bound contract the rest of the workflow drains *blind*: once tasks are approved, red-tests, implementation, cleanup, and PR open execute against the graph without re-deriving intent, so the upfront reasoning budget is where it pays off. The `/plan-reviewer` iteration (still Step 2) runs at xhigh too. The autonomous drain (Steps 4–6) and routine kata bookkeeping run at the session default.

### Mechanism-bound plans
Every implementation kata task must name **symbols/files to change** AND **symbols the new code path must invoke**. Goal-only tasks get rejected by `/plan-reviewer`. The rule prevents silent substitution of one mechanism for another at implementation time.

### Three valid responses
When you find a problem (plan gap, test infeasibility, drain blocker, perf regression), the only valid responses are:
1. **Execute** — propose / apply the concrete fix.
2. **Clarify** — ask for missing context before proceeding.
3. **Challenge** — push back on the premise if it's structurally wrong.

There is no fourth option of silently substituting an alternative and writing a commit message that paints the substitution as the spec'd work.

### Conventional commits
Format: `<type>(<scope>): <description>`. Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `build`, `chore`. Scopes are defined in `linear/labels.toml` (`agent`, `lifecycle`, `query`, `proto`, …) — omit when a change spans multiple areas. Description: lowercase, imperative mood, no period, under 72 chars. Footer: `CHA-XX` reference required.

**Commit scope: one commit = one logical change. A single kata task may close with many commits (`kata close --commit` is repeatable) when one logical change naturally splits — e.g. substantive change + a follow-up `chore: cargo fmt` fixup.** (`style` is not an allowed commit type — the commit-msg hook rejects it; use `chore` for fmt fixups. `scripts/check_commit_msg.py` validates the scope against the section names in `linear/labels.toml`, which are **bounded contexts, not crate names** — code in `crates/penca-datafusion` commits under `query`/`schema`/`sql`, never `datafusion`. Omit the scope entirely when a change spans areas. A rejected commit message aborts the commit, so a `kata close --commit $(git rev-parse HEAD)` chained after it records the *previous* SHA.)

### Read the conventions up front
Read `docs/style-guide.md`, `docs/development-methodology-guide.md`, and `.claude/skills/code-comments/SKILL.md` in full at the start of this skill — they are required inputs for every planning, implementation, and commit step, loaded **preemptively** (not on demand). `docs/algorithms.md` and `README.md` stay on demand within the relevant step.

`code-comments/SKILL.md` is the commenting standard every comment you write is measured against — comment WHY not WHAT, and default to *not* writing a comment. It is loaded here as **proactive implementer guidance** so comments are good the first time; it is deliberately **not** an `orch:run-cleanup` pass and is never auto-invoked by the drain (unlike `/clean-code-refactor` and `/tracing-instrument`). The point is that knowing the standard up front means no comment-cleanup pass is ever needed.

### kata invocation shape

Cheat sheet for the kata calls this skill makes:

- `kata create --label cha-NNN --label plan-draft --label <kind> [--label <source>] --priority N [--blocked-by <ref>] -- "<title>"` — create a task. `<kind>` is one of `red-test`, `impl`, `orch:run-cleanup`, `orch:open-pr`, `orch:spawn-review`. `<source>` (optional) marks late-arriving findings: `roborev`, `review-pr`, `cleanup-pass`, `agent-discovered`. Priority is 0..4 with 0 = highest (direct numeric pass-through from Linear: Urgent=1 → kata 1, None=0 → kata 0).
- **`--json` shapes differ per subcommand — this is the single biggest footgun in this skill.** Every row below was verified by running the installed CLI on throwaway tasks (2026-07-29). **Verify this way, not by reading kata's source:** `kata version` reports `dev` with an unknown commit, so the installed binary is not necessarily the tagged release in the module cache — a source-read of `ListIssues`/`ReadyIssues` suggests repeated `--label` AND-intersects, and the running binary demonstrably does not.

  | subcommand | wrapper | `qualified_id` | `labels` |
  |---|---|---|---|
  | `kata list --json` | `{kata_api_version, issues: [...]}` | ✅ `penca#9y1b` | ✅ `["approved", …]` (plain strings) |
  | `kata ready --json` | `{kata_api_version, issues: [...]}` | ❌ **null** | ❌ **null** |
  | `kata create --json` | `{kata_api_version, issue: {...}, event, changed}` | ❌ **absent** (use `.issue.short_id`) | — |
  | `kata show --json` | `{kata_api_version, issue, labels, links, comments}` | under `.issue` | ✅ but as objects — `.labels[].label` |

  Consequences you must code around: a `jq 'select(.labels | index("approved"))'` post-filter on **`kata ready` output evaluates `null | index(...)` → null and silently drops every row**, so the drain looks empty when work is ready. And `.issues[0].qualified_id` off `ready` (or `.qualified_id` off `create`) yields empty, silently breaking any downstream `kata claim` / `--blocked-by` wiring. Build refs as `penca#<short_id>` from `.issue.short_id` (create) or `.issues[].short_id` (ready).

- `kata list --label cha-NNN --label approved --json` — read the approved set. Use `.issues[]` in jq. **Repeated `--label` flags do NOT AND-intersect** (the second `--label` is silently dropped, despite `--help` claiming "repeatable, AND logic"), so always confirm the second label client-side: `jq '.issues[] | select(.labels | index("approved"))'`. This works here because `list` is the one query that returns `labels`.
- `kata ready --unowned --label cha-NNN --json` — surfaces tasks that are not blocked and not claimed. It cannot filter on `approved` (no `labels` in its output, and the second `--label` is dropped anyway), so **intersect the two queries** rather than post-filtering `ready`:
  ```bash
  # approved set (labels only exist on `list`) ∩ ready set (blockers/ownership only known to `ready`)
  comm -12 \
    <(kata list  --label cha-NNN --json | jq -r '.issues[] | select(.labels | index("approved")) | .short_id' | sort) \
    <(kata ready --unowned --label cha-NNN --json | jq -r '.issues[]?.short_id' | sort)
  ```
  Skipping the intersection breaks the Step-3 gate in one of two ways: post-filtering `ready` on `.labels` surfaces *nothing*, and not filtering at all surfaces unapproved `plan-draft` tasks. The human-readable `kata ready --label cha-NNN` (no `--json`) is reliable when you just need to eyeball what's ready — the `--json` shape is the unreliable part, not the readiness computation.
- `kata claim <ref>` — atomic claim. If a previous loop tick abandoned a claim (session restart, crash), `kata claim --force <ref>` is the recovery path (kata has no TTL-based auto-release).
- `kata close <ref> --done --commit <sha> --message "<text>"` — close the task. `--done` requires **both** a `--message` (≥40 chars after normalization, describing scope + how it was verified) **and** typed evidence. `--commit <sha>` is the evidence for code tasks (repeatable). Orchestration tasks produce no commit but **still need evidence** — use `--reviewed <path>` (cleanup walk, repeatable), `--pr <url>` and/or `--test "just check"` (PR open). `--done` with neither message nor evidence is rejected mid-drain. (`--wontfix` is the opposite — it *rejects* evidence; close challenged/duplicate findings with `--wontfix --message "<justification>"`.)
- `kata edit <ref> --blocked-by <other-ref>` — additive, repeatable. The dynamic-blocker mechanism — roborev / `/review-pr` / cleanup-pass enqueuers all extend in-flight `orch:*` tasks via this call.
- `kata create --idempotency-key <key> -- ...` — idempotent create. Re-running with the same key + identical content is a no-op (`changed:false`). Re-running with the same key + different content errors with `idempotency_mismatch`; the caller must use stable content per key (the roborev hook does — `<short-sha>:<finding-index>` is content-stable per review).
- Issue refs are `<project>#<short_id>` (e.g. `penca#p6yp`) — the canonical form, and what TUI shows. `kata` also accepts ULIDs and numeric ids, but a **bare numeric id is rejected** by `--blocked-by` and friends (`"…looks like a legacy issue number; use a short_id"`). Only `kata list --json` emits `qualified_id` directly; everywhere else assemble it from `short_id` per the shape table above.
- `kata edit <ref> --body "$(cat /tmp/body.md)"` — `kata edit` has **no** `--body-file` / `--body-stdin`; those are `kata create`-only flags.
- `kata delete --force --confirm "DELETE <qualified_id>"` — the help text says `"DELETE <short_id>"` but the daemon checks against `<qualified_id>` (`DELETE penca#p6yp`). Same shape for `kata purge --confirm "PURGE <qualified_id>"`.

## Step 1: Mark In Progress, pull main, and digest the ticket

**First action, before anything else:** move the ticket to "In Progress" via the Linear MCP tool with `id="$ARGUMENTS"` and `state="In Progress"`. This signals to the rest of the team that work has started and prevents another contributor from picking up the same ticket. Do this even before reading the description — if the ticket turns out to be trivial / not skill-appropriate, you can move it back, but the cost of an accidental status flip is far lower than the cost of duplicated work.

Immediately after the status flip, run `hostname -f` and post a comment on the ticket via the Linear MCP `save_comment` tool with `issueId="$ARGUMENTS"` and `body="Picked up by <hostname>"` (substitute the actual `hostname -f` output). This records which VM/agent claimed the ticket, so a human can find the running session.

Then, pull main to ensure that the local code checkout is up to date.

Aside from the plan-visualization comment at the Step-3 gate, these are **the only Linear writes** until the `orch:open-pr` task runs in Step 5.

Then fetch the full ticket:

```
Use the Linear MCP `get_issue` tool with id="$ARGUMENTS" and includeRelations=true.
Use the Linear MCP `list_comments` tool with issueId="$ARGUMENTS".
```

Read:
- Title, description, acceptance criteria
- `blockedBy` — confirm every blocker is closed; if not, stop and tell the user
- `relatedTo` — read related ticket descriptions if they're load-bearing for the design
- Comments — prior design discussion lives there

If the description points at PRs, ADRs, or code paths, read those too.

Then **grep the codebase for the ticket id** (e.g. `rg -n 'CHA-XX'` from the repo root). Past contributors leave durable hints in source as `FIXME(CHA-XX)`, `TODO(CHA-XX)`, ADR back-references, test xfails, and commit-message footers. Read every hit before drafting the plan — these often name the exact symbols/files the new code path must touch.

### Graph context — bounded neighborhood traversal + cross-reference verification

A single-ticket digest misses where the ticket sits in the broader Linear graph — and that context is what lets you **scope and plan the ticket well**, not merely catch errors. Knowing it's the v1 of a planned v2, that a sibling is already building a piece of it, or that it's gated on an in-flight enabler should shape where you draw the ticket's boundary, what you build now versus defer, and how you sequence the work. The same traversal also surfaces the highest-value plan-time objections (a sibling already built this; a cited reference is stale) that otherwise only land at the human review gate. The agent already has MCP read access to the whole graph; the gap is process, not access. So before drafting the plan, traverse a **bounded neighborhood** — **1–2 hops only** — gather that context, and verify the ticket's own cross-references against the live graph.

**Traverse** from the target ticket over:
- `blockedBy` / `blocks` / `relatedTo` / `parent`+`children` / epic — already present in the `get_issue ... includeRelations=true` payload fetched above.
- **Every `CHA-NNN` mentioned in the ticket body** — descriptions cite siblings in prose, not only via typed relations. Extract them with `rg -o 'CHA-[0-9]+'` over the description and dedupe against the relation set.

For each neighbor, fetch its **current** title / status / scope via the Linear MCP `get_issue` tool — the live record, **not** the target ticket's paraphrase of it. These are reads; they do **not** add to the skill's Linear-write budget.

Then:
1. **Verify the ticket's own cross-references against live state.** For each `CHA-NNN` the ticket asserts something about, confirm the claim matches that neighbor's actual current scope/status. A **contradicted or stale** reference — e.g. a ticket citing "CHA-385's row_uuid index" when the index work is actually CHA-412 and CHA-385 is unrelated identity rework (the motivating CHA-406 case) — is **flagged**, never silently propagated.
2. **Record the result** as the `## Graph context` section emitted in Step 2 (see below).

**Scope guardrail (load-bearing).** This context shapes **how you scope the ticket's own intent** (boundary, build-vs-defer, sequencing) and lets you **verify** its cross-references — but it is **never** license to **expand** scope onto neighbor work. More graph context is the primary scope-creep vector, so counter it explicitly: neighbor work becomes a plan note or a follow-up ticket, and is **never** folded into this PR. Keep the traversal to the ~1–2 hop neighborhood; do **not** load the whole graph (noise, attention dilution, and the same scope-creep risk), and do **not** stand up a searchable-Linear mirror — that is a separate, deferred idea.

**No Linear comments are read or written during the rest of the flow.** Teammate edits made after Step 1 are surfaced when the In-Review write fires inside the `orch:open-pr` task.

## Step 2: Emit the workflow as a kata task graph

### Resume guard — skip emit if the graph already exists

Re-invoking `/do-issue $ARGUMENTS` is the resume path; there is no separate `--resume` flag. First check whether a prior invocation already emitted this ticket's graph:

```bash
existing=$(kata list --label cha-NNN --json | jq '.issues | length')
```

If `existing > 0`, the graph is already in kata. **Skip the rest of Step 2 and proceed to Step 3.** Print the existing task set (`kata list --label cha-NNN --json | jq -r '.issues[] | "\(.qualified_id) [\(.labels | join(","))] \(.title)"'`) so the user can confirm what's there before the gate check. Re-emitting on top would either produce idempotency-mismatch errors (different body) or silent no-ops (identical body) — neither is the right resume behavior.

### Domain-specific pre-plan audits — fires when relevant

Some Penca domains have spec-undocumented behaviors that bite plans repeatedly if not audited up front.

#### Flight SQL — driver wire-action audit

**Fires when** the ticket changes any of:
- `crates/penca-sql-server/src/flight_sql/` or anything its `do_*` / `do_action_*` handlers call into (`dml.rs`, `set.rs`, `tx.rs`, `session.rs`).
- A user-visible JDBC / ODBC / ADBC behavior — error wording, returned schema, transaction semantics, prepared-statement metadata.
- A `SchemaProvider` / `CatalogProvider` / `TableProvider` method that DataFusion's planner consults during SQL planning.

**Required before drafting kata tasks — and the mapping must be _cited_, not asserted.** For each user-level driver call the ticket affects (`Statement.execute("CREATE TABLE …")`, `cursor.execute(SELECT …)`, `cursor.execute_update(…)`, `PreparedStatement.executeQuery(…)`, etc.), open the driver source and write down the exact Flight SQL action sequence it invokes, **with a `file:symbol`/`file:line` citation of the source you actually read** for each driver. A mapping with no citation is a guess; guesses are what this audit exists to stop.

- **ADBC** — the **Python `adbc_driver_manager`** DB-API layer decides prepare-vs-not before the Go/C `adbc-driver-flightsql` ever runs: `dbapi.py::Cursor._prepare_execute` calls `self._stmt.prepare()` **unconditionally** (it only skips on `NotSupportedError`, which the Flight SQL driver does not raise). So `cursor.execute(SELECT)` — which `PencaClient.execute_query` uses — takes the **prepared** path (`ActionCreatePreparedStatement` → `get_flight_info_prepared_statement` → `DoGet(CommandPreparedStatementQuery)`), even for a bare no-param SELECT. Only the low-level `stmt.execute_update` path (`PencaClient.execute_update`) skips prepare → `DoPutStatementUpdate`. Source: installed `adbc_driver_manager/dbapi.py`; repo `packages/penca-client/src/penca_client/client.py`. Upstream: https://github.com/apache/arrow-adbc.
- **JDBC** — Apache `flight-sql-jdbc-driver` (Java). A plain `Statement.execute(SELECT)` → `CommandStatementQuery` → `get_flight_info_statement`; the `PreparedStatement` / DML path → `ActionCreatePreparedStatement`. Under `java/flight/flight-sql-jdbc-driver/` in https://github.com/apache/arrow.
- **ODBC** — whichever bridge applies.

**The trap to internalize (CHA-355):** for the *same* `SELECT`, ADBC lands on the `CommandPreparedStatementQuery` DoGet arm while JDBC lands on the `CommandStatementQuery` arm — the two query paths do **not** converge on one server handler. A plan that wires behavior onto only one DoGet/GetFlightInfo arm silently excludes one driver.

Map each action to the corresponding server entry-point handler in `crates/penca-sql-server/src/flight_sql/service.rs`. Record the cited per-driver mapping (query **and** update paths) in the body of one planning kata task (the one that owns the cross-driver helper). If the entry-points diverge, the plan must include a task that introduces a shared helper called from every entry-point (or wires every divergent arm), plus at least one driver-parametrized acceptance test. See `.claude/memory/feedback_flight_sql_driver_parity.md`.

### Emit the task graph

The workflow is **five layers** of kata tasks, with the dependency chain expressed via `--blocked-by`. All emitted as `--label plan-draft` for Step 3 review.

**Layer 1 — red-test tasks** (`--label red-test`). One per acceptance criterion / test that must fail before implementation begins. No `--blocked-by`; these are the entry point of the graph. Body must name:
- The test file path(s) (`tests/integration/...`).
- The test invocation filter used to validate auto-close (`just integration-test --test-arg <filter>`) — the loop runs only the scoped tests to validate, not the full suite.
- The expected failure mode (which assertion fires; what error class). This is what auto-close validation checks for.

**Layer 2 — implementation tasks** (`--label impl`). One per commit-sized change. Each `--blocked-by` the red-test task it satisfies. Body names:
- Symbols/files to change.
- Symbols the new code path must invoke (the mechanism-bound rule).
- Mechanism non-goals when relevant.
- Which red-test task the change satisfies.

**Layer 3 — `orch:run-cleanup`** (single task, `--label orch:run-cleanup`, priority 4). `--blocked-by` every Layer 2 task. Body:

> Invoke `/clean-code-refactor --in-cleanup-context cha-NNN` and `/tracing-instrument --in-cleanup-context cha-NNN`. Scope = touched files (`git diff --name-only $(git merge-base origin/main HEAD)..HEAD`), read in full. Each cleanup-pass skill runs its Phase-1 walk and `/plan-reviewer` gate, then auto-flips `plan-draft` → `approved` on the candidate tasks it emitted (no human TUI step). Phase-2 drain is the outer `/do-issue` loop's job — this orchestration task closes once both walks have emitted their (possibly empty) candidate sets and extended this task's downstream `orch:open-pr` blocker chain.

**Layer 4 — `orch:open-pr`** (single task, `--label orch:open-pr`, priority 4). `--blocked-by` Layer 3. Body:

> Run `just check`; must exit 0. `gh pr create --base main --title "CHA-XX | <short>"` using the PR body template (see Step 5). Linear MCP `save_issue` with `state="In Review"` and `links=[{url, title}]` — the last Linear write of the flow.

**Layer 5 — `orch:spawn-review`** (single task, `--label orch:spawn-review`, priority 4). `--blocked-by` Layer 4. Body:

> Spawn an Opus subagent with fresh context to run `/review-pr <PR#>` at xhigh thinking budget. The subagent enqueues its findings with `--label approved --label review-pr` and does NOT extend any orch task. The Step-5 dispatch closes this task, drains the findings, then — if any findings were enqueued — creates a new `orch:spawn-review` task `--blocked-by` every finding. The loop iteratively re-reviews after each fix wave until a subagent run returns zero new findings; the PR is then ready for human merge.

Each task body includes the standard mechanism-bound fields where relevant.

For tickets that cross bounded contexts or introduce a new RPC/microservice, invoke `/software-architect` to produce a design framing before emitting tasks.

Map priority through from Linear: Linear-Urgent (1) → kata 1, Linear-High (2) → kata 2, etc. Linear-None (0) is rare; map to kata 4 (lowest). **Orchestration tasks always priority 4** — the drain consumer breaks priority ties in favor of red-tests, impl, and findings before orchestration.

### The `## Graph context` plan section

Alongside the task graph, the plan carries a **`## Graph context`** section built from the Step-1 neighborhood traversal. It names:
- **Roadmap position** — is this the v1 of a planned v2? What does it enable, and what does it block?
- **Already built vs in-flight** — what in the neighborhood is done or under review, so the plan does not rebuild a sibling's work.
- **Stale / contradicted cross-references** — any reference the live graph contradicts, optionally corrected in place or spun into a follow-up ticket (never silently propagated).

When a neighbor already covers a piece of the work, the relevant kata task body carries a one-line **neighbor note** — e.g. `> Neighbor note: CHA-412 (In Review) already builds the cold-tier index — position only, do not rebuild`. That note is the per-task expression of the Step-1 scope guardrail: the neighborhood positions the work, it does not enlarge it.

### `/plan-reviewer` gate — mandatory for correctness-invariant plans

After all tasks are emitted but **before** flipping any task out of `plan-draft`, invoke `/plan-reviewer`. It reads the task set with `kata list --label cha-NNN --label plan-draft --json` + `kata show <ref> --json` per task and audits:
- Mechanism-bound rule per implementation task.
- Dependency-graph sanity: no cycles, every Layer-2 task `--blocked-by` at least one Layer-1 task, Layers 3/4/5 chained in order, `orch:run-cleanup` blocked-by every Layer-2 task.
- Domain-specific invariants (Flight SQL audit, multi-step SQL ordering, watermark/clamp/grace invariants, advisory locks).

Address every `REVISE` item via `kata edit <ref> --body "$(cat <body-file>)"` (`kata edit` has no `--body-file`/`--body-stdin` — those are `kata create`-only flags), then re-invoke; iterate until `APPROVED`. If you reach a fourth iteration without convergence, halt and surface the unresolved item to the user.

For **purely mechanical** tickets (single function rename, one-line config tweak, doc-only edit with no behavioral effect), `/plan-reviewer` is optional.

## Step 3: STOP — plan approval

### Resume guard — skip the gate if the plan is already approved

```bash
plan_draft=$(kata list --label cha-NNN --label plan-draft --json | jq '.issues | length')
approved=$(kata list --label cha-NNN --label approved --json | jq '.issues | length')
```

If `plan_draft == 0` and `approved > 0`, the user already approved on a prior invocation. **Skip the gate and proceed to Step 4.** Print the approved count so the user has visibility, then move on without asking.

### The gate (first-time invocation)

**This is the only human gate in the workflow.** Do not proceed past this gate without the user flipping `plan-draft` → `approved` on every task in the graph (all five layers).

### Surface the plan as an HTML visualization

Before prompting for approval, generate a visual of the emitted graph and post it to the Linear ticket, so the approver can review the DAG + task summaries at a glance instead of reading `kata tui` / raw task bodies:

1. **Generate** — run the committed generator over the ticket's slug:
   ```bash
   python3 .claude/skills/do-issue/kata_plan_html.py cha-NNN -o /tmp/cha-NNN_plan.html
   ```
   It is pure-stdlib and deterministic (same kata state → byte-identical HTML): a Mermaid blocked-by DAG color-coded by layer (red-test → impl → orch) plus per-task cards.
2. **Upload** — Linear MCP `prepare_attachment_upload` → signed `PUT` to GCS (send the returned signed headers **verbatim**, 60s expiry) → keep the returned `assetUrl`. Do **not** use `attachmentCreate`: API-created attachment *entities* do not render in the Linear issue UI — the reliable surface is a markdown link in a **comment** (Linear re-signs the asset URL on view).
3. **Surface** — Linear MCP `save_comment` posting a markdown link to the asset, e.g. `📊 [plan-visualization](<assetUrl>)`.
4. **Resume** — on re-invocation, find the prior plan-visualization comment and update it **in place** (one comment, edited via `save_comment` with the existing comment id) rather than posting a new one — per the self-sufficient-resume-comment convention.

Plan visualization is a standard part of the gate, not an optional nicety. The generator is pure-stdlib and deterministic and the upload path is well-trodden, so it **should never fail** in normal operation. If generation or upload *does* fail, do **not** silently fall back and proceed — **flag it loudly**: surface the exact error to the user as a prominent, blocking failure and fix the visualizer, because a broken plan visualization is a defect, not a step to skip. (`kata tui` is the human's raw view of the plan only while that failure is being investigated.)

**Surface the `## Graph context` section** (from Step 1's neighborhood traversal) in the plan-visualization comment body and in the plan summary you present at the gate, so the approver sees the roadmap position and any flagged stale cross-references at a glance without opening each task. This rides the existing Step-3 plan-visualization comment — it is **not** a new Linear-write category.

Tell the user the task set is ready and point them at `kata tui` (and the plan-visualization comment) to review. Approval is *mechanism-bound*: the user removes the `plan-draft` label and adds `approved` to each task they're keeping. Edits and deletions happen in the TUI before that. The drain consumer in Step 5 gates on `--label approved`; nothing flagged `plan-draft` will surface.

**Or offer to flip them all in one shot.** After printing the TUI approval flow, also ask: *"or I can approve all tasks for you now — say the word."* If the user takes that path, iterate the `plan-draft` set and swap the label on each:

```bash
kata list --label cha-NNN --label plan-draft --json | jq -r '.issues[].qualified_id' | while read ref; do
  kata label rm "$ref" plan-draft
  kata label add "$ref" approved
done
```

The gate is unchanged — implementation only proceeds once tasks carry `approved`; whether the user did that themselves or asked the agent to do it doesn't matter.

Confirm the approval count:

```
kata list --label cha-NNN --label approved --json | jq '.issues | length'
```

The count should equal the layer-1-through-5 task count emitted at Step 2.

If the user wants to discuss before approving, do that — but no implementation step starts until at least one task carries the `approved` label.

## Step 4: Create the branch

Use the Linear-generated branch name from the ticket payload (the `gitBranchName` field returned by `get_issue`). The checkout is resume-safe — branch reuse is the right call when the loop dropped out and came back:

```bash
BRANCH="nhobin219/cha-NN-description"
if git rev-parse --verify "$BRANCH" >/dev/null 2>&1; then
  git checkout "$BRANCH"
else
  git checkout -b "$BRANCH"
fi
```

If `git status` is dirty before either checkout, stop and surface it — unexpected uncommitted state on a fresh VM means something is off, and on a resume run it means a prior tick left work mid-flight that the agent shouldn't blindly clobber.

## Step 5: Drain — the /loop consumer (runs until PR merge)

From here to PR merge, the workflow runs as a **/loop drain** with no human gates. The loop processes whatever `kata ready --unowned --label cha-NNN --label approved` surfaces and dispatches by task kind. Roborev / cleanup-pass / `/review-pr` findings auto-approve as they arrive and join the queue. Orchestration tasks unblock in sequence — `orch:run-cleanup` after all Layer-2 tasks close, `orch:open-pr` after `orch:run-cleanup`, `orch:spawn-review` after `orch:open-pr`. The loop exits when the queue is empty, roborev is quiet, **and** `gh pr view --json state` reports `MERGED`.

### Start the loop

Invoke `/loop` with no interval (self-paced via `ScheduleWakeup`) and a prompt that runs the drain step below. Document for the user: the loop is session-bound — if Claude Code closes mid-ticket, the loop dies and they re-invoke `/do-issue $ARGUMENTS` to resume. The drain consumer is idempotent — it picks up wherever the kata graph left off.

### Drain step — what each loop tick does

A single tick processes as many ready tasks as it can in one model invocation, then schedules the next wakeup based on what we're waiting on.

```bash
# Inner loop within one tick — process while there's ready work.
while true; do
  # `ready` knows blockers/ownership but carries no labels; `list` carries labels
  # but not readiness. Intersect them — see the --json shape table in the cheat sheet.
  short=$(comm -12 \
    <(kata list  --label cha-NNN --json | jq -r '.issues[] | select(.labels | index("approved")) | .short_id' | sort) \
    <(kata ready --unowned --label cha-NNN --json | jq -r '.issues[]?.short_id' | sort) \
    | head -1)
  [ -z "$short" ] && break
  ref="penca#$short"
  kata claim "$ref"
  # ... dispatch on task kind (see below) ...
done

# Nothing ready right now. Pick next wakeup cadence.
#
# Tight (~10s): anything could still enqueue work — roborev review in
#   flight, any orch:* task still open (cleanup-pass walks or the review
#   subagent may still enqueue findings), or no PR open yet.
# Long  (~45s): PR is open, no roborev in flight, no open orch:* tasks —
#   the only state change that matters is the human merging the PR.
# Exit: PR merged — proceed to Step 6.

pr_state=$(gh pr view --json state -q .state 2>/dev/null || echo "NOT_OPEN")
[ "$pr_state" = "MERGED" ] && exit  # Workflow complete.

# Parse the COUNTS off the Jobs line. A bare `grep -qE 'queued|running'` is always
# true: the header reads "Daemon: running" and the Jobs line spells out both words
# even at "0 queued, 0 running" — a wait-loop built on it never terminates.
# No `Jobs:` line at all means the daemon is down or unreachable ("Daemon not
# running…"), which is NOT idle — report `unknown` so the sweep blocks instead of
# declaring itself clean without ever having queried a queue. `unknown` groups
# with busy below: it keeps the loop alive on the tight cadence (a daemon restart
# resolves itself) and never lets the drain reach "ready for merge" on an
# uncertified sweep. Do NOT turn it into a bare `exit` — a tick that ends with no
# ScheduleWakeup and no stop just dies, stalling the ticket while looking idle.
# If it stays `unknown` across several ticks the daemon is genuinely down: say so
# via SendUserMessage (plain text between tool calls does not render mid-loop —
# see .claude/memory/feedback_send_user_message_mid_loop.md) and stop the loop
# deliberately with ScheduleWakeup(stop: true).
roborev_busy=$(roborev status 2>&1 \
  | awk '/^Jobs:/ { print ($2 + $4 > 0) ? 1 : 0; found=1 } END { if (!found) print "unknown" }')

open_orch=$(kata list --label cha-NNN --status open --json 2>/dev/null \
  | jq '[.issues[]? | .labels[] | select(startswith("orch:"))] | length')

if [ "$roborev_busy" = "unknown" ] || [ "$roborev_busy" = "1" ] \
   || [ "$open_orch" -gt 0 ] || [ "$pr_state" = "NOT_OPEN" ]; then
  exit  # ScheduleWakeup ~10s — work could appear soon.
else
  exit  # ScheduleWakeup ~45s — waiting on human merge.
fi
```

Cache-TTL note: both cadences stay well under the 5-minute prompt-cache window, so every tick rides a warm cache regardless of which branch fires. Don't widen the long-wait cadence past ~270s without a reason — you'd pay a cache miss for no benefit (the only state change you're polling for is the human merge, and 45s vs. 270s isn't materially different for a hour-scale event but is materially worse for cache).

### Dispatch by task kind

For the claimed `<ref>`, read its labels via `kata show <ref> --json | jq -r '.labels[]'` and dispatch:

**`red-test`** — auto-close after self-verification:
1. Write the failing tests per the task body (typically `tests/integration/...`).
2. Run the scoped test command from the body (`just integration-test --test-arg <filter>`).
3. **Self-verify**: each new test must FAIL (exit non-zero) AND the failure message must match the task's expected failure mode — not a compile error, not a runtime error in unrelated code. If the body lists multiple tests, every one must fail correctly.
4. Commit: `test(scope): add acceptance tests for <ticket title>`, footer `CHA-XX`.
5. `kata close <ref> --done --commit <sha> --message "<≥40 chars: scope + verification>"`. Implementation tasks blocked-by this red-test unblock.

If self-verification fails (test errored instead of failing for the right reason, or didn't fail at all), `kata claim --force <ref>` release is **not** the answer — leave the task claimed, post a brief diagnostic to the task body via `kata edit <ref> --comment "<what failed>"`, and exit the tick. The user surfaces it via `kata tui` or in the next session.

**`impl`** — implement, commit, close:
1. Read the task body's symbols/files + invocation list.
2. Implement. Inner-loop TDD in `tests/tdd/` (gitignored, run via `just tdd`) until green. `tests/tdd/` is wiped before `orch:run-cleanup` runs — it's a dev tool, not a deliverable. Comment to the `code-comments` standard as you write: WHY not WHAT, default to no comment, `TODO(CHA-NNN)` never bare — good comments now mean no comment-cleanup later.
3. Re-run the scoped integration test from the satisfied red-test's body and confirm it flips green.
4. Commit: `<type>(<scope>): <description>`, footer `CHA-XX`.
5. `kata close <ref> --done --commit <sha> --message "<≥40 chars: scope + verification>"`.

Each commit triggers the post-commit hook → `roborev post-commit` → async review → on completion, `scripts/roborev-kata-hook.sh` enqueues findings with `--label approved` and extends every in-flight `orch:*` task's `--blocked-by` to include the new findings. Findings drain via priority (severity-mapped: critical/high → 1, medium → 2, low → 3) before orchestration tasks (priority 4). `review_min_severity = 'medium'` (set by `just init-agent-tools`) means Lows never reach the queue.

**Consolidate before draining a backlog of findings.** Reviews are per-commit and isolated: roborev sees one diff, with no memory of earlier reviews and no view of the current tree. So a nit raised against commit 1 is re-raised against commits 2 and 3 for code that never changed, and a finding a later commit already fixed stays open. Both are queue volume that no diff will ever retire.

When more than ~3 roborev findings are open at once, run the consolidation pass before working any of them:

```bash
roborev compact --wait     # verifies open findings against the CURRENT tree,
                           # merges duplicates, closes the superseded originals
```

Then re-read the queue — findings whose roborev jobs `compact` closed should be closed in kata too (`kata close <ref> --wontfix --message "superseded by roborev compact: <reason>"`). Working the list before consolidating means implementing the same finding two or three times and fixing things already fixed.

**When the finding rate is the problem, fix the calibration, not the findings.** If a whole class of finding keeps arriving and keeps getting closed `--wontfix`, that is a guidelines gap, not a work queue. Run `roborev insights` (it mines review history for "noise candidates" — findings consistently dismissed without a code change), fold the result into `scripts/roborev-review-guidelines.md`, and re-apply with `just init-agent-tools`. Do this at `orch:run-cleanup` time, not mid-drain.

**`orch:run-cleanup`** — invoke cleanup-pass skills, close:
1. Compute the touched-files scope: `git diff --name-only $(git merge-base origin/main HEAD)..HEAD`.
2. Invoke `/clean-code-refactor` with `--in-cleanup-context cha-NNN` and the touched-files scope. The skill walks, emits candidate tasks (`--label approved --label cleanup-pass`), runs its `/plan-reviewer` gate, auto-flips labels, and returns. Each new task extends `orch:open-pr`'s `--blocked-by`.
3. Invoke `/tracing-instrument` with the same flag and scope. Same flow.
4. `kata close <ref> --done --reviewed <touched paths> --message "<≥40 chars: cleanup scope + outcome>"` (orch task, no commit — `--reviewed` is the evidence).

Why touched files (not just diff) for both cleanup passes: a diff can change the **boundary map** without editing the boundary callsite. A new wrapper fn can demote a previously-public fn to an internal helper (its existing `#[instrument]` is now double-span noise); the symmetric case promotes a previously-internal helper to the boundary via a new caller path. Diff-only walks miss both flips. For refactor candidates, the touched-files window is what lets the walk spot extract-helper opportunities where a new function shares logic with an existing one in the same file. `/plan-reviewer` drops candidates that aren't related to the boundary / extract-helper shift the way it drops any out-of-scope candidate.

**`orch:open-pr`** — gate, open PR, Linear In Review, close:
1. Run `just check`. Must exit 0. If it fails:
   - **fmt diff** → `cargo fmt --all`, commit as `chore: cargo fmt` (`style` is not an allowed commit type).
   - **clippy / cargo check error** → fix the underlying issue (no `#[allow]` without justification); commit as `fix:` or amend.
   - **ruff / ty / blank-lines** → analogous on the Python side.
   Re-run `just check` until green.
2. `gh pr create --base main --title "CHA-XX | <short description>"` with body:

```
## Summary

<one or two bullets on what changed and why>

## Acceptance

<list of integration tests added during the red-test drain; reviewer checks they are now green — or, for doc-only tickets, the baseline/post-edit grep counts>

Closes CHA-XX
```

Include `Closes CHA-XX` so merge auto-transitions the Linear issue.

3. Linear MCP `save_issue` with `state="In Review"` and `links=[{url: "<PR URL>", title: "PR #<n>"}]`. **The last Linear write of the entire flow.**
4. `kata close <ref> --done --pr <url> --test "just check" --message "<≥40 chars: PR opened + gate result>"` (orch task, no commit — `--pr`/`--test` are the evidence).

**`orch:spawn-review`** — spawn the review subagent, close current task, possibly re-arm the loop:

1. **Snapshot** the open review-pr finding set *before* spawning, so step 4 can tell which findings are new:
   ```bash
   pre_findings=$(kata list --label cha-NNN --status open --json \
     | jq -r '.issues[] | select(.labels | index("review-pr")) | .qualified_id' | sort)
   ```
2. **Spawn** via the `Agent` tool with:
   - `subagent_type="general-purpose"`
   - `model="opus"` (current Opus 4.7)
   - prompt instructing the subagent to run `/review-pr <PR#>` at **xhigh** thinking budget and to read the PR diff + Linear ticket itself rather than inherit the main session's framing. **Tell the subagent NOT to extend any `orch:*` task's `--blocked-by`** — this dispatch step owns the loop-arming decision; the subagent only enqueues findings under `cha-NNN` with `--label approved --label review-pr`.
3. **Close the current `orch:spawn-review`** with `kata close <ref> --done --pr <url> --message "<≥40 chars: review round + new-finding count>"` — regardless of whether new findings landed (orch task, no commit — `--pr` is the evidence). Each round's task is single-shot.
4. **Diff the finding set** to identify what the just-completed subagent added:
   ```bash
   post_findings=$(kata list --label cha-NNN --status open --json \
     | jq -r '.issues[] | select(.labels | index("review-pr")) | .qualified_id' | sort)
   new_findings=$(comm -13 <(echo "$pre_findings") <(echo "$post_findings"))
   n=$(echo "$new_findings" | grep -c .)
   ```
5. **Arm the next round** if and only if `n > 0`:
   ```bash
   if [ "$n" -gt 0 ]; then
     round=$(kata list --label cha-NNN --label orch:spawn-review --json \
       | jq '.issues | length')   # how many spawn-review tasks ever existed under this slug
     next="penca#$(kata create --label cha-NNN --label approved --label orch:spawn-review \
       --priority 4 --json -- "orch:spawn-review (round $((round + 1))) — re-review after findings close" \
       | jq -r '.issue.short_id')"   # `create --json` has NO .qualified_id — see the shape table
     echo "$new_findings" | while read f; do
       [ -n "$f" ] && kata edit "$next" --blocked-by "$f" >/dev/null
     done
   fi
   ```
   - Round-numbered title (`round 2`, `round 3`, …) keeps the iteration visible in `kata tui`.
   - The new task is blocked-by every finding from this round; the drain claims+processes findings first (as impl-like tasks), then unblocks+claims the new `orch:spawn-review`, spawning another subagent for a fresh-eyes pass on the fixes.
   - **Termination**: when a subagent run returns 0 new findings, no new `orch:spawn-review` is armed. The drain proceeds to "wait for human merge" (long-cadence wakeup).
   - **Final roborev sweep before reporting ready-for-merge.** The loop's exit is gated on `MERGED`, which the agent never reaches (the human merges — never `gh pr merge`), so in practice the loop is stopped by hand at "ready for merge." Roborev reviews lag each commit by seconds-to-minutes, so the *last* commits (review fixes, the PR-open commit) may still be under review when the queue looks empty. Before declaring ready-for-merge, poll `roborev status` until the **Jobs line reports 0 queued and 0 running** (parse the counts — see the `roborev_busy` awk above; a `grep` for the words matches even when idle), then drain the ready∩approved intersection one more time — and check `kata list --label cha-NNN --status open` for findings that landed after a cleanup pass. Use the intersection form from the tick loop above, never a `.labels` post-filter on `kata ready` output: `ready --json` carries no `labels`, so the filter silently matches nothing and the sweep looks clean when it isn't. Do **not** assume quiet from an empty `kata ready`; verify roborev directly. This same discipline applies to any **post-PR** commits the human later asks for (merge-conflict resolution, follow-up edits): every commit fires roborev, so re-sweep after them too. See `.claude/memory/feedback_poll_roborev_after_any_commits.md`.

The pre/post diff (not the absolute count) is what determines whether to re-arm — a subagent run that surfaces no NEW findings (even if old ones are still open and being drained) doesn't trigger another round. This is the right shape because the open findings are already going to drive a re-review via the existing `orch:spawn-review` chain: when they close, the LAST-armed `orch:spawn-review` unblocks and fires.

The project default is no subagents; this invocation is the explicit in-session exception. Fresh-eyes review is the one place fan-out earns its keep — the main session's context is anchored to the design decisions it just made, which is exactly the framing a reviewer needs to be free of. Do **not** also run `/review-pr` yourself in the main session.

**Source-labeled findings (`roborev`, `review-pr`, `cleanup-pass`, `agent-discovered`)** — same as `impl`: implement, commit, close. Priority drives them ahead of orchestration tasks.

### Doc-only tickets

When the ticket touches only Markdown / docs / skill files with no executable surface, substitute a deterministic grep-based acceptance set:
- At Step 2, each `red-test` task body lists N specific `rg` commands the rewritten file(s) must satisfy after the edit. Each must currently fail on the unedited file (the red baseline) and pass after.
- The loop's `red-test` processing runs the grep set instead of `just integration-test`. Baseline counts are captured by the body's commands; the matching `impl` task confirms post-edit counts.
- Auto-close validation: every grep returns the expected exit / count from the body.

The PR body's `## Acceptance` section pastes baseline-vs-post counts.

### Optional pre-cleanup advisory passes

Before `orch:run-cleanup` would unblock (i.e., while Layer-2 tasks are still draining), a focused advisory pass can be useful:
- `/code-quality-reviewer` — structural + type-idiom audit. Run before perf work.
- `/perf-engineer` — measurement-driven perf analysis.
- `/software-architect` — design revalidation for tickets that crossed bounded contexts in the plan.

These produce structured findings; address Critical/Important items as new `impl` tasks (`--label cha-NNN --label approved --label impl --label agent-discovered`) blocked-by `orch:run-cleanup` (so cleanup waits on them, not the reverse). For most tickets these are skippable — `orch:run-cleanup`'s `/clean-code-refactor` walk already covers structural concerns on the diff.

### Mid-drain kata tasks (agent-discovered)

If the agent finds work that wasn't in the plan, emit it as `kata create --label cha-NNN --label approved --label impl --label agent-discovered --priority N -- "<title>"`, with `--blocked-by` set on `orch:run-cleanup` so the new work blocks PR open. Auto-approve here is the same contract roborev/cleanup-pass/review-pr findings use; `agent-discovered` is just the source marker.

### Late-arriving findings — dynamic blocked-by extension

When `scripts/roborev-kata-hook.sh`, `/review-pr`, or a cleanup-pass invocation enqueues a new finding under `cha-NNN`, it also extends every still-open `orch:*` task's `--blocked-by` to include the new finding's qualified-id. The pattern (used by all three enqueuers):

```bash
new_finding_ref=$(kata create ... --json | jq -r '.qualified_id')
for orch_label in orch:run-cleanup orch:open-pr orch:spawn-review; do
  orch_ref=$(kata list --label cha-NNN --label "$orch_label" --status open --json | jq -r '.issues[0].qualified_id // empty')
  [ -n "$orch_ref" ] && kata edit "$orch_ref" --blocked-by "$new_finding_ref"
done
```

This guarantees PR open waits on all inbound findings — without it, a high-severity roborev finding could land seconds after the PR opens. The roborev hook implements this; the `/review-pr` skill implements this; the cleanup-pass skills implement this when invoked in in-cleanup-context mode.

### Robustness — claim recovery on session restart

If the user closes Claude Code mid-tick and re-invokes `/do-issue $ARGUMENTS`, the first drain check may find tasks claimed by an absent owner. Recovery: at loop start, run

```bash
kata list --label cha-NNN --status open --owned --json | jq -r '.issues[].qualified_id' | while read ref; do
  kata claim --force "$ref" 2>/dev/null
  # Then decide: did the previous claim leave work in flight? Inspect via git status / git log.
  # If a commit exists for this task already, close it. Otherwise re-do the dispatch.
done
```

In practice, the safer move is to leave any claimed task alone and surface them to the user — the agent can't safely tell whether a prior commit is "for this task" without strong heuristics. Better to leave the human in the loop on resume.

## Step 6: Cleanup after merge

After the PR merges (`gh pr view --json state -q .state` returns `MERGED`), sync main, drain the local agent-state for this ticket via the `just clean-agent-tools` recipe, then delete the local branch:

```bash
git checkout main
git pull
just clean-agent-tools cha-XX "nhobin219/cha-XX-description"
git branch -d "nhobin219/cha-XX-description"
```

The recipe closes any roborev jobs left open for the branch and soft-deletes the closed kata tasks under the `cha-XX` label. Soft-delete preserves the kata event log so `kata restore` can resurrect any task if a follow-up ticket needs to reference it.

If any `cha-NNN` tasks remain `open` after the drain finished (orphaned `plan-draft` from rejected candidates, abandoned claims), they're skipped by the closed-status filter and surface in `kata list --label cha-NNN` for ad-hoc triage. Don't bulk-delete the open set — those represent unresolved work and need eyes on them.

The VM itself is disposable; no per-branch teardown beyond the above is needed.

If the ticket revealed a recurring pitfall, the right concept to add to a docs page, or a tool gap, mention it to the user — those are candidates for follow-up Linear tickets, not for sneaking into this PR's scope.
