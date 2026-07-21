---
name: clean-code-refactor
description: Behavior-preserving Rust/Python refactors against Penca — one combined mechanism-bound plan covering every candidate the walk surfaces, emitted as kata tasks under `cha-NNN plan-draft` and gated once by `/plan-reviewer`, then one branch carrying every candidate as its own commit drained from the approved kata queue, gated once by `just check` at PR open. Defaults to a single PR for the entire batch. Use for refactor tickets (label = `refactor`), ad-hoc crate assessment when no ticket exists yet, or as the `/do-issue` `orch:run-cleanup` cleanup-context pass. Defaults to single-agent execution; subagent fan-out requires explicit user authorization in-session.
argument-hint: "[--in-cleanup-context cha-NNN] <scope>"
allowed-tools: Read Grep Glob Bash WebFetch Write Edit Skill mcp__language-server__definition mcp__language-server__references mcp__language-server__hover mcp__language-server__diagnostics
---

# Clean-code refactor

Drive behavior-preserving refactors of Penca code (Rust workspace + Python packages) through a two-phase gated loop: **Plan** (one combined mechanism-bound plan covering every candidate the walk surfaces, emitted as kata tasks under `cha-NNN plan-draft`, gated once by `/plan-reviewer` to `APPROVED`) → **Execute** (one branch per batch, each candidate drained from `kata ready --label cha-NNN --label approved` and landed as its own commit, gated once by `just check` at PR open).

Every candidate is named against one of three reference exemplars (bat `Controller`, ripgrep `Searcher`, tokio `FramedRead`) so the *shape* the refactor moves toward is explicit, not vibes.

**Single-PR default.** Every candidate the walk surfaces lands in **one batch, one branch, one PR**. Do not pre-split the batch into multiple PRs as a stylistic choice — the plan+review cycle is the expensive step, and one combined plan is cheaper for the reviewer than N serial plans with hidden cross-candidate dependencies. Split into multiple batches only when the scope is truly enormous — rule of thumb, ~20+ candidates spanning unrelated module trees that no reviewer would sensibly evaluate as a single change. When in doubt, keep it one PR. Scope pruning (dropping a candidate the user doesn't want) happens during `/plan-reviewer` iteration by deleting the task from the kata queue, not pre-emptively at walk time.

## When to use this skill

Three invocation modes; see "Invocation modes" below for how the contract differs across them:

- **Ad-hoc assessment** of a crate or package — no ticket required. The plan goes through `cha-NNN plan-draft` → human-approval → drain on a refactor-themed branch.
- **In-ticket refactor** during a refactor ticket's implementation phase, when `/do-issue` (or you) hands off to this skill after Step 1. Same contract as ad-hoc, but scoped to a Linear `cha-NNN`.
- **In-cleanup-context pass** invoked by `/do-issue`'s `orch:run-cleanup` orchestration task on a non-refactor ticket. The cleanup-context contract is auto-approve after `/plan-reviewer` `APPROVED` (no human TUI step) and skip Phase 2 (the outer `/do-issue` `/loop` drains).

If you're driving a Linear ticket through its full lifecycle (status → In Progress, plan-on-ticket, PR with `Closes CHA-XX`, post-merge cleanup), **start with `/do-issue`** — it handles ticket mechanics and hands off to this skill at Step 2 when the ticket's label is `refactor` (or the description's core ask is "rework X to shape Y" with no behavior change). `/do-issue` retains Step 1 (status + digest) and Step 6 (post-merge cleanup); this skill replaces Steps 2–5 for refactor work.

Do **not** use this skill for:
- Behavior changes, bug fixes, or new features — those are `feat:` / `fix:` work; route through `/do-issue` normally.
- Formatter churn or `clippy --fix` runs — those are `style:` commits handled outside this skill.
- Cross-bounded-context restructuring (new RPCs, microservice extraction, storage-tier splits) — invoke `/software-architect` first; this skill assumes the bounded contexts are fixed.

## Invocation modes

The Phase-1 walk + `/plan-reviewer` gate is identical across all three modes. The differences are at the gate and in Phase 2.

| | Ad-hoc / in-ticket refactor (default) | **In-cleanup-context** (`--in-cleanup-context cha-NNN`) |
|---|---|---|
| Phase-1 emit label | `cha-NNN plan-draft` | `cha-NNN plan-draft cleanup-pass` |
| `/plan-reviewer` gate | runs to APPROVED | runs to APPROVED |
| Post-APPROVED label swap | **human** flips `plan-draft` → `approved` in `kata tui` (or via `/do-issue` Step 3 bulk-approve) | **skill** auto-flips `plan-draft` → `approved` — no human gate |
| `orch:open-pr` blocker extension | n/a (no orch task exists) | every new task `kata edit`'s `orch:open-pr` to add the new task as a `--blocked-by` |
| Phase 2 (drain) | this skill drives the drain on its own branch | **skipped** — control returns to the outer `/do-issue` `/loop`, which drains via its uniform consumer |

In-cleanup-context invocation: `/clean-code-refactor --in-cleanup-context cha-NNN <touched-files-or-scope>`. Detect the flag before walking; the rest of the skill behaves the same up to the gate.

## Grounding principles

The principles below are inlined here intentionally — this skill is a durable artifact and must remain self-sufficient. Memory entries (`[[feedback_*]]` slugs) are `/dream`-curated and ephemeral; the skill does not cross-link them.

1. **Single responsibility per symbol.** Name a function/struct/module in one sentence without using "and". If you need "and", split it.
2. **Compose small functions; one transformation per function.** Either the function does exactly one thing or it composes other functions that each do one thing. No mixed concerns inside a single body.
3. **A type has one reason to change.** If two unrelated business needs would both motivate edits, it has two responsibilities. Split by concern.
4. **Config vs. execution split.** The type that *runs* an operation does not also own the knobs that *configure* it. Builder / config struct holds the knobs; the worker holds only what it needs to execute. (ripgrep `SearcherBuilder` → `Searcher`.)
5. **Delegation over mode flags.** When you catch yourself adding `mode: OutputMode`, `verbose: bool`, or a similar policy switch to a worker type, that is a trait (Sink, Strategy) waiting to be extracted.
6. **Composers stay thin.** A type that exists to compose others should add zero state and zero branching. If `poll_next` is doing more than `self.project().inner.poll_next(cx)`, the composition is leaking — usually one composed piece needs to grow, not the composer.
7. **Fail-fast at the boundary.** Validate inputs at the servicer / RPC / public-API boundary and trust internally. Library code does not re-validate paths only reachable from within the module.
8. **No premature abstraction.** Three similar lines is better than a bad helper. Wait until you have three or four uses before extracting; speculative generality and "what if we need to swap X later" are anti-patterns.
9. **No method or variable aliasing.** `qi = self._dialect.quote_identifier`, `s = settings.cold_storage`. Inline the full call/path at the use site; aliases are names without responsibility.
10. **Borrow over own.** Default `&str` / `&[T]` / `&Path` / `&impl Trait` in signatures unless the function needs ownership. No unnecessary `.clone()`.
11. **Route through `just` recipes, never bare `cargo`.** `just cargo-check`, `just cargo-test`, `just cargo-fmt`. Bare `cargo` invocations skip the workspace's pinned flags.
12. **Every refactor batch goes on its own branch — one batch per invocation by default.** One *batch* per branch (the full set of candidates in the approved Phase-1 plan, or the single candidate from an ad-hoc invocation), each candidate landed as its own commit on the batch branch. Default to **one batch = one PR for the entire approved set**; only split into multiple batches when the scope is truly enormous (rule of thumb: ~20+ candidates spanning unrelated module trees). The branch lifecycle is part of the contract, not a convenience.
13. **No semantic change inside a `refactor:` commit.** Behavior-preservation is the contract. If the candidate would alter observable behavior, record it as a "spin into own ticket" finding and stop — do not bundle.
14. **No `clippy --fix` / `cargo fmt` drive-bys inside refactor commits.** Formatter churn lives in `style:` commits; lint auto-fixes are evaluated by hand, never bulk-applied.
15. **Iterate kata task bodies in place via `kata edit <ref> --body "$(cat <body-file>)"`** (`kata edit` has no `--body-file`/`--body-stdin` — those are `kata create`-only flags)**.** Phase-1 iteration with `/plan-reviewer` updates the relevant task bodies — no new tasks per REVISE pass. (The exception is when `/plan-reviewer` returns `REVISE` because a candidate is missing entirely — emit a new task with `--label cha-NNN --label plan-draft`.)
16. **Cross-crate moves are in-scope when structurally justified.** Moving a type/helper/module across crate boundaries is fair game when it reduces dependencies, aligns a type with its sole consumer, removes a wrong-direction re-export, or eliminates a transitive heavyweight dep. The move must serve one of the three exemplar lenses (orchestrator-thinning, trait-delegation, pure-composition), not "shuffle code between files." Per-ticket constraints that narrow scope (e.g. "single-crate only") are guidance, not law — surface them in the Phase-1 plan so the user can confirm or lift them in-session rather than silently following text that may have been written speculatively.
17. **Explicit re-exports when splitting a module into submodules.** When a `foo.rs` becomes `foo/{mod,a,b,c}.rs`, `mod.rs` re-exports each submodule's public items as an explicit list (`pub use a::{ITEM_1, fn_2, …};`), not `pub use a::*;`. The explicit list keeps the module's public surface visible at a single read of `mod.rs` — which matters most for widely-consumed modules where the surface *is* the contract. Wildcard re-exports hide what consumers can reach for and let new pub items leak out of submodules silently.

Read `docs/style-guide.md` and `docs/development-methodology-guide.md` in full up front — they are the conventions baseline every refactor candidate is measured against, loaded **preemptively**, not on demand.

## Operational shape

Two sequential phases. Each phase produces an artifact the next phase reads — no implicit hand-offs, no shortcuts between phases.

### Phase 1 — Plan (one combined batch plan, gated by `/plan-reviewer`)

Walk the chosen scope (a crate, a module, a directory; or in cleanup-context mode, the touched-files list `/do-issue` passes). Identify candidates against the principles above. Emit **one kata task per candidate**, all under `cha-NNN plan-draft` (and `cleanup-pass` as an additional source label when invoked in `--in-cleanup-context` mode). The set of tasks *is* the plan; the `/plan-reviewer` pass *decides* — if a candidate should not land, the user (or `/plan-reviewer`) calls it out during review and the task is dropped from the queue via `kata delete`. Do not self-censor candidates pre-emptively at walk time.

When invoked **standalone with no ticket**, use a per-invocation scope label of the user's choosing (e.g. `refactor-penca-merge-2026-05-28`) in place of `cha-NNN`; the kata queue is still the source of truth. The plan does not also go to stdout.

Typical candidates a Phase-1 walk surfaces:

- **God-objects / orchestrators that also do the work** — a type that holds dependencies *and* runs the per-input branching inline. Lens: bat `Controller`.
- **Config-and-execution on the same type** — setters and runtime methods on one struct. Lens: ripgrep `Searcher` (`SearcherBuilder` → `Searcher`).
- **Mode flags / policy enums on worker types** — `mode: OutputMode`, `verbose: bool`, a `match` on input kind inside a worker. Lens: ripgrep `Sink` extraction.
- **Composers that grew state** — a wrapper/adapter/facade whose `poll_next` (or equivalent delegated method) is doing more than `self.project().inner.poll_next(cx)`. Lens: tokio `FramedRead` (move the responsibility into the composed piece, leave the composer as a single delegated call).
- **Long methods / deep nesting / mixed concerns** — single bodies that fail the one-sentence-without-"and" test. Pattern: Extract Method / Replace Temp with Query / Guard Clauses.
- **Primitive obsession, data clumps** — sets of primitives that travel together and motivate the same edits. Pattern: Introduce Parameter Object.
- **Aliases without responsibility** — `qi = self._dialect.quote_identifier`, `s = settings.cold_storage`. Pattern: inline at use site.
- **Borrow-over-own violations** — signatures taking ownership when borrowing would suffice; unnecessary `.clone()`.
- **Cross-crate moves with structural justification** — a type/helper aligned with its sole consumer, or a wrong-direction re-export. Lens: whichever of the three exemplars motivates the destination shape (principle #16).
- **Wildcard re-exports at a module boundary** — `pub use a::*;` where an explicit list belongs (principle #17).

For each candidate, the kata task names (in the task body — title is one line summarizing the transform):

- `crate/path/file.rs:LINE` — the symbol or block.
- **Smell category** — one of the bullets above (or a justified new one).
- **Recommended pattern** — Extract Method, Replace Temp with Query, Introduce Parameter Object, Replace Conditional with Polymorphism, Guard Clauses, Strategy / Facade / Composition / Builder.
- **Lens applied** — bat-orchestrator / ripgrep-trait-delegation / tokio-pure-composition (see "Reference exemplars" below), with the one-sentence operational insight that justifies it.
- **Expected effort** — S (single-file mechanical), M (multi-file, ~1 PR), L (cross-crate, needs decomposition into multiple candidates).
- **Approach** — one or two paragraphs naming the transform.
- **Symbols/files to change** AND **symbols the new code path must invoke** — the canonical-path call sites, the half most plans miss.
- **Mechanism non-goals** where relevant.
- **Behavior-preservation note** — tests that must stay green; semantic-change red flags. Confirm the change is behavior-preserving. If a candidate cannot be done without altering observable behavior, mark it "spin into own ticket" and exclude from the batch.
- **Inter-candidate dependencies** — anything earlier candidates in the batch must do first. Encode with `kata create --blocked-by <earlier-task-ref>` so the drain loop physically cannot start candidate #N until its prerequisites are closed (e.g. "depends on #2 already tightening visibility so this candidate's new module home doesn't accidentally re-widen" becomes `--blocked-by <#2-qualified-id>`).

Batch-level meta (kept in one **batch-summary task** with `--label cha-NNN --label plan-draft --label batch-summary`):

- One paragraph naming the scope, the candidates included (qualified-ids), the execution order with one-line justifications. Land smaller / lower-risk candidates first so later ones inherit any boundary-tightening done by earlier ones (visibility shifts and helper-module extractions usually go before parameter-object work and orchestrator splits). The `--blocked-by` graph captures the ordering at the task level; the summary is for the human reviewer.
- **External public-surface map** (when refactoring a crate) — the consumer table here so per-candidate task bodies can cite it without re-deriving.
- **Batch-level gate** — `just check` exits 0 *after the last candidate is committed*; no semantic change in any commit; per-candidate atomicity (one commit per candidate, no bundling).

**Invoke `/plan-reviewer` once** on the kata task set (`kata list --label cha-NNN --label plan-draft --json`). Iterate on `REVISE` items by editing the relevant task bodies via `kata edit <ref> --body "$(cat <body-file>)"` — no new tasks per pass. Re-invoke until the verdict is `APPROVED`.

**Hard stop:** no code edit happens before `/plan-reviewer` returns `APPROVED`. If three rounds fail to converge, halt and surface to the user (the same escape valve `/do-issue` uses).

#### In-cleanup-context post-APPROVED handling

When invoked with `--in-cleanup-context cha-NNN`, `/plan-reviewer` itself auto-flips `plan-draft` → `approved` on every cleanup-pass task and extends `orch:open-pr`'s `--blocked-by` after returning `APPROVED` (see `/plan-reviewer`'s "Post-APPROVED — cleanup-pass auto-flip" section). This skill does **no** post-APPROVED handling — return control to the caller (the outer `/do-issue` `/loop`); **skip Phase 2 entirely** — the outer drain claims and processes the now-approved cleanup-pass tasks via its uniform consumer.

### Phase 2 — Execute (one branch per batch, drain from kata)

Phase 2 runs only for the ad-hoc and in-ticket refactor modes. **In-cleanup-context mode skips Phase 2** — the outer `/do-issue` `/loop` is the drain.

Only after Phase 1 returns `APPROVED` *and* the user has flipped each task's label `plan-draft` → `approved` (in `kata tui`, or via the bulk-approve helper from `/do-issue` Step 3).

1. **Create the branch** via `git checkout -b <branch>` from the repo root — one branch for the entire batch; name it after the batch theme, not any single candidate.

2. **Drain the approved kata queue.** The `--blocked-by` edges from Phase 1 give the execution order for free:
   ```bash
   while true; do
     ref=$(kata ready --unowned --label cha-NNN --label approved --json | jq -r '.issues[0].qualified_id // empty')
     [ -z "$ref" ] && break
     kata claim "$ref"
     # ...apply the single behavior-preserving transform from the task body...
     git commit -m "refactor(<scope>): <description>

CHA-XX
"
     kata close "$ref" --done --commit "$(git rev-parse HEAD)"
   done
   ```
   The transform must match the task body's named symbols. If implementation reveals a structural blocker not in the task, halt and return to Phase 1 for that candidate — no silent substitution (the three-valid-responses rule). Iterating in place on a task body via `kata edit` is fine; substituting a *different* mechanism for the same candidate requires a Phase-1 revisit + re-approval.

   **One commit per candidate; no bundling, no semantic change.** Intermediate breakage between commits inside the batch is acceptable — the PR gate runs once at the end — but each individual commit should still represent a coherent transform. (A candidate may carry a follow-up `style:` commit when a rename or visibility tightening leaves a fmt diff; record both SHAs via `kata close --commit <sha1> --commit <sha2>`.)

3. **PR gate** — after the last candidate is committed, `just check` must exit 0 once before opening the PR. This is the project-wide gate that mirrors CI; it is a strict superset of `just cargo-check` (`lint` + `format-check` + `test` + `static-test` + `cargo-check`). The batch's atomicity is "all commits green together"; per-candidate gates are out.

4. **Open the PR** with a title naming the batch theme and a body listing each commit's candidate with a one-line summary. After merge, sync main and delete the branch: `git checkout main && git pull && git branch -d <branch>`.

For Python-touching refactors, `just check` already runs `lint` + `format-check` + `test` — no extra Python invocation needed.

### Rename candidates — local-binding shadow

A rename that changes a public function name (e.g. `get_X` → `X`) can land on an existing local variable in a caller's scope: `let X = get_X(...)` becomes `let X = X(...)`. Rust accepts this — the RHS resolves to the outer function before the LHS shadows it — but the shadow is a trap for code added later in the same scope, and Python is stricter: `X = X(...)` is `UnboundLocalError` because the LHS makes `X` a function-local across the entire function and the RHS resolves to the same unbound local. Two behavior-preserving fixes; pick the simpler one for the site: (a) rename the caller's local to a non-colliding name, or (b) module-qualify the call (`naming::X(...)`).

### Subagent default — single-agent, main session

This skill executes in the **main conversation** with no `Agent`-tool fan-out. Spawning a subagent (parallel Phase-1 walks across crates, parallel Phase-2 branches, etc.) requires **explicit in-session user authorization** — not just plan-time justification. The single-agent default exists because the project's review-loop discipline (`/plan-reviewer`, `/code-quality-reviewer`, `/perf-engineer`) is built around one author per change; parallel subagent runs split that discipline across contexts that cannot see each other's findings.

## Reference exemplars

Three real-world Rust types worth treating as the canonical shape. Every Phase-1 candidate names which of these lenses applies; every Phase-1 plan justifies the chosen lens.

### bat `Controller` — orchestrator composing single-purpose components

[`src/controller.rs`](https://github.com/sharkdp/bat/blob/master/src/controller.rs) (fetch with `WebFetch` for the latest shape).

`Controller` doesn't syntax-highlight, doesn't read files, doesn't paginate. It holds references to `Config`, `HighlightingAssets`, and an optional `LessOpenPreprocessor`, then picks an `OutputType` and a `Printer` per input. `run` is a single delegated call.

**Operational insight:** each module the orchestrator composes (`Input`, `Printer`, `Preprocessor`, `Assets`, `OutputType`) lives in its own file with one job. The file system itself enforces the boundary. If you cannot write a one-sentence description of what a file is responsible for, the file is doing too much. The orchestrator is *thin*: interesting work happens in the modules it composes, not in the orchestrator itself.

**When to apply this lens:** a candidate type has accumulated many call sites that branch on input type or mode. Split the dispatch into a thin orchestrator that holds dependencies, and move each branch into a single-purpose component.

### ripgrep `Searcher` — separating finding from reporting via a trait

[`crates/searcher/src/searcher/mod.rs`](https://github.com/BurntSushi/ripgrep/blob/master/crates/searcher/src/searcher/mod.rs) (fetch with `WebFetch` for the latest shape).

Two SRP wins in one type:

- **Config vs. execution split.** `SearcherBuilder` owns configuration; `Searcher` owns only what it needs to execute. The type that *runs* the operation does not also own the knobs that *configure* it.
- **Delegation through a trait.** `Searcher` finds matches and hands each one to a caller-supplied `Sink`. It never decides "should I print this in color, write JSON, count lines" — that's the `Sink`'s job.

**Operational insight:** when you catch yourself adding a `mode: OutputMode` enum or a `verbose: bool` flag to a worker type, that is a `Sink` waiting to be extracted. Likewise, when configuration setters and execution methods live on the same struct, the builder hasn't been extracted yet.

**When to apply this lens:** a candidate type mixes configuration and execution, or carries a policy enum/flag that gates its output behavior. Extract a builder for config; extract a trait for the policy switch.

### tokio-util `FramedRead` — pure composition with zero added logic

[`tokio-util/src/codec/framed_read.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio-util/src/codec/framed_read.rs) (fetch with `WebFetch` for the latest shape).

`FramedRead<T, D>` wraps an `AsyncRead` source `T` and a `Decoder` `D`. `poll_next` is **literally one delegated call**: `self.project().inner.poll_next(cx)`. No state, no branching, no policy: just composition.

**Operational insight:** if a composer type starts accruing its own state or conditionals, that is a signal the composition is not clean — usually one of the composed pieces needs to grow, not the composer.

**When to apply this lens:** a candidate is a wrapper / adapter / facade that has grown its own state or branching. The fix is rarely to split the composer; it is to move the responsibility into the composed piece that should own it, leaving the composer as a single delegated call.

### Lens routing — the three questions

For every candidate, answer:

| | bat `Controller` | ripgrep `Searcher` | tokio `FramedRead` |
| --- | --- | --- | --- |
| What's the data? | `Config`, `Input` | `Config`, file bytes | `AsyncRead` source |
| What's the *one* thing this type does? | orchestrate | find matches | adapt bytes → frames |
| What does it delegate? | print, preprocess, paginate | report matches (`Sink`) | read (`AsyncRead`), decode (`Decoder`) |

If a candidate fails all three (every responsibility lives inside it, delegates nothing, hard to name in one sentence), it is the "bad UserService" shape from the ticket and the refactor target is to land it in one of the rows above.

## References

`docs/style-guide.md` (repo-local Rust + Python conventions) and `docs/development-methodology-guide.md` (three valid responses, two-loop TDD, KISS) are read **preemptively** at the start of this skill — see the intro. Fetch on demand:

- [refactoring.guru / Refactoring Techniques](https://refactoring.guru/refactoring) — Composing Methods, Moving Features Between Objects, Organizing Data, Simplifying Conditional Expressions. Use to disambiguate which refactoring pattern fits a candidate.
- [refactoring.guru / Design Patterns](https://refactoring.guru/design-patterns) — Creational (Factory, Builder), Structural (Adapter, Facade, Composite), Behavioral (Strategy, Template Method, Chain of Responsibility).
- [SOLID](https://en.wikipedia.org/wiki/SOLID) — Single Responsibility, Open/Closed, Liskov, Interface Segregation, Dependency Inversion.
- [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) and [KISS](https://en.wikipedia.org/wiki/KISS_principle) — including the anti-patterns (premature abstraction, speculative generality).

## Three valid responses

When a refactor candidate hits a wall — implementation reveals a structural blocker, behavior-preservation can't be guaranteed, the named lens doesn't actually fit — the three valid responses are:

1. **Execute** — apply the refactor as planned, gates green.
2. **Clarify** — return to Phase 1 for that candidate, revise the plan, re-invoke `/plan-reviewer`.
3. **Challenge** — push back on the candidate's premise; record it as out-of-scope or as a "spin into own ticket" finding.

There is no fourth option of silently bundling a semantic change into a `refactor:` commit, skipping `/plan-reviewer` for an "obvious" refactor, or substituting a different mechanism than the one the approved plan named. Behavior-preservation is the contract; the gates exist to enforce it.
