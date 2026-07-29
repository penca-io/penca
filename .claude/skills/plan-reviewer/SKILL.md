---
name: plan-reviewer
description: Audit a Penca `/do-issue` plan — a set of kata tasks under `cha-NNN plan-draft` — for the mechanism-bound rule + dependency-graph sanity. Use after `/do-issue` Step 2 emits the task set and before the user flips `plan-draft` → `approved`. Checks that every task names symbols/files to change AND symbols the new code path must invoke, has mechanism non-goals where relevant, avoids parallel-path workarounds, and that every implementation task is `--blocked-by` at least one red-test task with no cycles in the graph.
allowed-tools: Bash Read Grep Glob
---

# Plan reviewer

Audit Penca `/do-issue` plans against the **mechanism-bound rule** (per task) AND **dependency-graph sanity** (across tasks). Goal-only tasks get sent back; cycles in the `--blocked-by` graph get rejected outright.

This skill is advisory for `cha-NNN plan-draft` plans gated by a human at `/do-issue` Step 3. **Exception:** when every plan-draft task in the set also carries the `cleanup-pass` label (the in-cleanup-context output of `/clean-code-refactor` or `/tracing-instrument`), the parent `/do-issue` plan is already human-approved, so the skill itself auto-flips `plan-draft` → `approved` and extends `orch:open-pr`'s `--blocked-by` after returning `APPROVED`. See "Post-APPROVED — cleanup-pass auto-flip" below.

## Input — the kata task set

`/do-issue` Step 2 emits each plan item as a kata task labelled `cha-NNN plan-draft`. Read the whole set, then each task's body, before drafting findings:

```bash
kata list --label cha-NNN --label plan-draft --json
# → {kata_api_version, issues: [{qualified_id, title, priority, ...}, ...]}

# Then per task:
kata show <qualified_id> --json
# → {issue: {title, body, labels, ...}, relationships: {blocked_by: [...], blocks: [...]}, ...}
```

If the user supplies the `cha-NNN` slug directly, list with that label. Otherwise, derive it from the current branch name (`git rev-parse --abbrev-ref HEAD` → grep `cha-[0-9]+`).

## Per-task checks — apply to every task in the set

For each task, verify:

1. **Symbols/files to change are named.** Not "edit `lifecycle.rs`" but "add `sys_schemas_uuid` to the touched-tables set in `lifecycle.rs::persist_branch`". The task body should make a reader name the exact diff site.
2. **Symbols the new code path must invoke are named.** This is the half tasks most often miss. "The read path must call `penca_merge::stream_merged()` directly" is what you're looking for. If absent, the task is goal-only and should be sent back.
3. **Mechanism non-goals are stated** when relevant. "No new resolver path in `penca-storage-meta`" is the canonical example. Distinguish from ticket-level Out-of-scope — mechanism non-goals constrain *how this task is built*, not *what scope it covers*.
4. **No parallel-path workarounds** are implied. If the task introduces a new resolver, helper, or shim alongside the canonical mechanism (instead of using the canonical path directly), flag it. The point is to converge on canonical paths, not multiply them.
5. **Acceptance test reference present.** Every implementation task names the red-test kata task whose closure it satisfies. That reference is what the `--blocked-by` graph audit cross-checks below.

## Per-set checks — apply across the full task graph

After per-task checks pass, audit the graph:

6. **Every implementation task is `--blocked-by` at least one red-test task.** Walk `kata show <ref> --json` for each task and collect its blockers. **The edges are in `.links[]`, not `.relationships.blocked_by`** — that path is null (verified 2026-07-29). `kata show --json` returns top-level `{kata_api_version, issue, labels, links, comments}`: issue scalars (title, body, short_id, status) are under `.issue`, `.labels` is an array of *objects* (the string is `.labels[].label`, unlike `kata list --json` where labels are plain strings), and each link is `{from:{short_id}, to:{short_id}, type:"blocks"}` where **`from` blocks `to`** — so a task's blockers are the `from`s of links whose `to.short_id` is that task. Reading `.relationships.blocked_by` yields null for every task, which reads as "no task has blockers" and silently passes this check.

   ```bash
   kata show <ref> --json | jq -r '.links[]? | select(.type=="blocks") | "\(.from.short_id) -> \(.to.short_id)"'
   ```

   Dedup the edges across all tasks to build the graph. An implementation task with no `--blocked-by` edge breaks the mechanism-bound red gate — flag it.
7. **The `--blocked-by` graph is acyclic.** Build the edge list across the set; run a topological-sort check. A cycle is structural, not editorial — return `REJECT` and quote the cycle. Cycles indicate the planner didn't think through ordering; the right output is a rewrite, not a label-by-label fix in the TUI.
8. **No dangling `--blocked-by` references.** Every `blocked_by` ref must point at a task in the set (or a task already `approved`/closed). Stale references usually mean a task got deleted but its blockers weren't pruned — flag the dangler.
9. **Graph context section present + cross-references verified.** Non-trivial plans on a **cross-reference-bearing** ticket — one whose description cites other `CHA-NNN`s, or that carries `blockedBy` / `relatedTo` / `parent` relations — must carry a `## Graph context` section naming the neighborhood's current state (roadmap position, built-vs-in-flight, flagged stale refs). Absent → `REVISE`. And every cross-reference the ticket asserts must be checked against the **live graph**: if a ticket asserts a relation the live graph contradicts (e.g. the CHA-385 / CHA-412 stale-ref case from CHA-406), `REVISE` and quote the contradicted reference. `N-A` only when the ticket makes no cross-ticket references at all. This is the plan-time gate the `/do-issue` Step-1 graph-context traversal feeds; do not silently approve a cross-reference-bearing plan that skipped it.

## Domain-specific audits

### Flight SQL — driver wire-action audit

**Fires when** any task touches:
- `crates/penca-sql-server/src/flight_sql/` or anything its `do_*` / `do_action_*` handlers call into (`dml.rs`, `set.rs`, `tx.rs`, `session.rs`).
- A user-visible JDBC / ODBC / ADBC behavior — error wording, returned schema, transaction semantics, prepared-statement metadata.
- A `SchemaProvider` / `CatalogProvider` / `TableProvider` method that DataFusion's planner consults during SQL planning.

The plan must list, per user-level driver call the ticket affects, the Flight SQL action sequence each driver (ADBC + JDBC; ODBC when wired) invokes and the server entry-point handler it lands in — and **each driver's mapping must carry a `file:symbol`/`file:line` citation of the source the planner read** (e.g. ADBC `adbc_driver_manager/dbapi.py::_prepare_execute`, JDBC the `flight-sql-jdbc-driver` class). This is the un-fudgeable part: an **uncited or guessed mapping is an automatic `REVISE`**, exactly as if it were absent — do not accept a plausible-sounding sentence with no source. Verify the citation actually supports the claim where you can.

Two failure modes to reject specifically:
- **Wrong wire path.** The canonical trap: a bare `SELECT` via **ADBC** `cursor.execute()` takes the **prepared** path (`CommandPreparedStatementQuery` DoGet arm) because `adbc_driver_manager`'s `_prepare_execute` calls `prepare()` unconditionally — while the **same** `SELECT` via **JDBC** `Statement.execute` takes the `CommandStatementQuery` arm. They do **not** converge. A plan asserting "ADBC `cursor.execute` → `CommandStatementQuery`" is wrong-and-uncited → `REVISE`. (This is the CHA-355 miss.)
- **One-arm wiring.** If the query paths diverge across drivers, the plan must wire **every** divergent arm (or route them through one shared helper) AND include at least one driver-parametrized acceptance test. A fix that wires only one arm is a parallel-path workaround (check 4) → `REVISE`.

If the audit is absent, uncited, or guessed, return `REVISE` and quote the missing/uncited per-driver mapping in *Specific fixes*. See `.claude/memory/feedback_flight_sql_driver_parity.md`.

### Watermark / clamp / grace invariants

Fires when the plan mentions `persisted_at`, `purged_at`, `cleanup_started_at`, grace bounds, open-tx clamps, or snapshot-picker bounds. These categories have spec-undocumented correctness invariants that plan-review is the last reliable place to catch.

### Advisory locks / cross-RPC coordination

Fires when any task takes or relies on an advisory lock, or coordinates between independent RPCs.

### Algorithm specs the ticket itself authored

Fires when the Linear ticket description includes a numbered "Algorithm:" or "Step 1/2/3..." sequence the plan is mechanism-binding to. The audit checks that each step in the ticket spec maps to a kata task that binds the right canonical mechanism.

If you need conventions you don't already have in context, read the mechanism-bound plan rule in `.claude/skills/do-issue/SKILL.md` (Step 2), `docs/development-methodology-guide.md` (`#three-valid-responses`), and `docs/style-guide.md` on demand.

## Output shape

Three sections:

- **Per-task pass/fail** — one line per task: `<qualified_id>: PASS` or `<qualified_id>: REVISE — <which checks failed>`. Cite the task body excerpt that satisfies (or fails) each check.
- **Graph audit** — explicit lines, always emitted: "every implementation task `--blocked-by` red-test: PASS/REVISE", "acyclic: PASS/REJECT (cycle: A → B → C → A)", "no dangling refs: PASS/REVISE", "Graph context section + cross-references verified: PASS/REVISE/N-A" (`N-A` only when the ticket makes no cross-ticket references), and "driver wire-action audit: PASS/REVISE/N-A" (`N-A` only when no task touches the Flight SQL surface above; otherwise it must be `PASS` with the cited per-driver mapping confirmed, or `REVISE`). Stating these lines every time is what stops the graph-context and driver audits from being silently skipped.
- **Specific fixes** — for each failure, the exact addition or change the task body needs. Quote the task body verbatim and propose the replacement.
- **Verdict** — `APPROVED`, `REVISE` (with numbered items), or `REJECT` (cycle in `--blocked-by`, or the plan is goal-only and needs a rewrite, not edits).

## What "APPROVED" means now

The verdict is advisory **for the human-gated case**: the actual approval mechanism is the user flipping each task's label from `plan-draft` → `approved` in `kata tui` after reading this skill's output. The drain loop in `/do-issue` Step 5 gates on `--label approved`; nothing flagged `plan-draft` will surface. Do not write to the kata labels yourself — that is the user's gate, not the skill's.

### Exception — cleanup-pass auto-flip

When every plan-draft task in the set also carries the `cleanup-pass` label, the parent `/do-issue` plan was already human-approved at Step 3 and these tasks are sub-steps within that approved umbrella (emitted by `/clean-code-refactor` or `/tracing-instrument` from `orch:run-cleanup`). In that case the skill itself swaps `plan-draft` → `approved` and extends `orch:open-pr`'s `--blocked-by` after returning `APPROVED`, so the calling skill does not carry its own post-APPROVED handling.

## Post-APPROVED — cleanup-pass auto-flip

Run this once, after the verdict is `APPROVED` and before returning, **only if every plan-draft task in the audited set carries `cleanup-pass`**:

```bash
# Detect: are all plan-draft tasks under cha-NNN cleanup-pass?
total=$(kata list --label cha-NNN --json \
  | jq '[.issues[] | select(.labels | index("plan-draft"))] | length')
cleanup=$(kata list --label cha-NNN --json \
  | jq '[.issues[] | select(.labels | index("plan-draft")) | select(.labels | index("cleanup-pass"))] | length')

if [ "$total" -gt 0 ] && [ "$total" = "$cleanup" ]; then
  orch_open_pr=$(kata list --label cha-NNN --status open --json \
    | jq -r '.issues[] | select(.labels | index("orch:open-pr")) | .qualified_id' | head -1)

  kata list --label cha-NNN --json \
    | jq -r '.issues[] | select(.labels | index("plan-draft")) | select(.labels | index("cleanup-pass")) | .qualified_id' \
    | while read ref; do
        kata label rm "$ref" plan-draft >/dev/null
        kata label add "$ref" approved >/dev/null
        [ -n "$orch_open_pr" ] && kata edit "$orch_open_pr" --blocked-by "$ref" >/dev/null
        echo "auto-approved $ref; extended $orch_open_pr --blocked-by $ref"
      done
fi
```

Notes:
- The detection uses jq's `select(.labels | index("X"))` (not repeated `--label X` flags) because the `kata list --label A --label B` AND-intersect is buggy (it ignores the second `--label`).
- If `orch_open_pr` is empty (no `orch:open-pr` task under this slug), the script just swaps labels and skips blocker extension — the user is running this skill standalone or before `/do-issue` emitted orch tasks.
- This is the **only** label-write the skill performs. Non-cleanup-pass plan-draft tasks remain advisory; the user is the gate.

## Three valid responses

When you find an issue with a task or graph, the three valid responses are:
1. **Execute** — propose a concrete fix the author can paste into the task body or graph.
2. **Clarify** — ask the plan author for clarification on intent.
3. **Challenge** — push back on the plan's premise if it's directionally wrong.

There is no fourth option of silently approving a deficient plan and hoping the implementation catches the gap. Mid-review discovery of a structural concern is a halt-and-surface event.
