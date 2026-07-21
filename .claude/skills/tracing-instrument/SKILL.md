---
name: tracing-instrument
description: Add structured `tracing`-crate instrumentation (spans + events + fields) to Penca Rust code — one combined plan emitted as kata tasks under `cha-NNN plan-draft` and gated once by `/plan-reviewer`, then one branch carrying every site as its own commit drained from the approved kata queue, gated once by `just check` at PR open. Scope is the `tracing` crate only (logs-as-events + spans); metrics and OpenTelemetry are out of scope. Use for observability tickets, ad-hoc instrumentation passes on a crate, or as the `/do-issue` `orch:run-cleanup` cleanup-context pass.
argument-hint: "[--in-cleanup-context cha-NNN] <scope>"
allowed-tools: Read Grep Glob Bash WebFetch Write Edit Skill mcp__language-server__definition mcp__language-server__references mcp__language-server__hover mcp__language-server__diagnostics
---

# Tracing instrument

Add structured `tracing`-crate instrumentation to Penca Rust code through a two-phase gated loop: **Plan** (one combined mechanism-bound plan covering every site the walk surfaces, emitted as kata tasks under `cha-NNN plan-draft`, gated once by `/plan-reviewer` to `APPROVED`) → **Execute** (one branch per batch, each site drained from `kata ready --label cha-NNN --label approved` and landed as its own commit, gated once by `just check` at PR open).

Every site is named against one of three reference exemplars (tokio `#[instrument]`, tower-http `TraceLayer`, sqlx query spans) so the *shape* the instrumentation moves toward is explicit, not vibes.

**Single-PR default.** Every site the walk surfaces lands in **one batch, one branch, one PR**. Do not pre-split the batch into multiple PRs as a stylistic choice — the plan+review cycle is the expensive step, and one combined plan is cheaper for the reviewer than N serial plans with hidden cross-site dependencies. Split into multiple batches only when the scope is truly enormous — rule of thumb, ~20+ sites spanning unrelated module trees that no reviewer would sensibly evaluate as a single change. When in doubt, keep it one PR. Scope pruning (dropping a site the user doesn't want) happens during `/plan-reviewer` iteration by deleting the task from the kata queue, not pre-emptively at walk time.

## When to use this skill

Three invocation modes; see "Invocation modes" below for how the contract differs across them:

- **Ad-hoc instrumentation pass** on a crate or binary — no ticket required. The plan goes through `cha-NNN plan-draft` → human-approval → drain on an instrument-themed branch.
- **In-ticket instrumentation** during an observability ticket's implementation phase, when `/do-issue` (or you) hands off to this skill. Same contract as ad-hoc, but scoped to a Linear `cha-NNN`.
- **In-cleanup-context pass** invoked by `/do-issue`'s `orch:run-cleanup` orchestration task on a non-instrumentation ticket. The cleanup-context contract is auto-approve after `/plan-reviewer` `APPROVED` (no human TUI step) and skip Phase 2 (the outer `/do-issue` `/loop` drains).

If you're driving a Linear ticket through its full lifecycle (status → In Progress, plan-on-ticket, PR with `Closes CHA-XX`, post-merge cleanup), **start with `/do-issue`** — it handles ticket mechanics and hands off to this skill when the ticket's core ask is "instrument X" with no functional behavior change. `/do-issue` retains the ticket-mechanics steps; this skill replaces the plan + implement steps for instrumentation work.

Do **not** use this skill for:
- Adding metrics (`metrics` crate, Prometheus exporters) — out of scope until Penca adopts a metrics stack.
- Adding OpenTelemetry / distributed-tracing exporters — out of scope until Penca adopts an exporter.
- Refactoring code that happens to touch existing log sites — that's `/clean-code-refactor` work. Instrumentation passes are behavior-additive: existing logs are not rewritten unless they violate a grounding principle below.
- Functional behavior changes, bug fixes, or new features — those are `feat:` / `fix:` work; route through `/do-issue` normally.
- Subscriber / pipeline configuration changes (env-filter defaults, formatter swaps) — those are infra concerns, handled outside this skill.

## Invocation modes

The Phase-1 walk + `/plan-reviewer` gate is identical across all three modes. The differences are at the gate and in Phase 2.

| | Ad-hoc / in-ticket instrumentation (default) | **In-cleanup-context** (`--in-cleanup-context cha-NNN`) |
|---|---|---|
| Phase-1 emit label | `cha-NNN plan-draft` | `cha-NNN plan-draft cleanup-pass` |
| `/plan-reviewer` gate | runs to APPROVED | runs to APPROVED |
| Post-APPROVED label swap | **human** flips `plan-draft` → `approved` in `kata tui` (or via `/do-issue` Step 3 bulk-approve) | **skill** auto-flips `plan-draft` → `approved` — no human gate |
| `orch:open-pr` blocker extension | n/a (no orch task exists) | every new task `kata edit`'s `orch:open-pr` to add the new task as a `--blocked-by` |
| Phase 2 (drain) | this skill drives the drain on its own branch | **skipped** — control returns to the outer `/do-issue` `/loop`, which drains via its uniform consumer |

In-cleanup-context invocation: `/tracing-instrument --in-cleanup-context cha-NNN <touched-files-or-scope>`. Detect the flag before walking; the rest of the skill behaves the same up to the gate.

## Grounding principles

The principles below are inlined here intentionally — this skill is a durable artifact and must remain self-sufficient.

1. **Spans bracket units of work; events record facts.** A span has a start and an end and captures everything inside as context (timing, nested spans, fields-in-scope). An event is a point-in-time fact emitted inside (or outside) a span. Do not event-log what a span should bracket. Do not span-wrap a single emit.
2. **Structured fields, never formatted strings.** `tracing::info!(query_id = %id, rows = n, "query complete")`, not `info!("query {} returned {} rows", id, n)`. Fields are filterable, indexable, and machine-readable; format strings are none of those. The trailing static message is the *event name*, not a place to interpolate.
3. **Span and event names are static literals.** `tracing` macro names must be string literals — dynamic strings defeat the macro-time optimization, break most exporters' aggregation, and silently downgrade your spans. Put dynamic values in fields.
4. **Levels carry semantics; default to `debug`.** `error` = the operation cannot continue / a contract was violated. `warn` = degraded but recoverable / unexpected but handled. `info` = lifecycle events at a boundary (RPC handler entry, startup/shutdown, scheduler tick). `debug` = per-operation detail useful when investigating. `trace` = inside-the-loop noise that is normally filtered. A new site defaults to `debug` unless it crosses a service boundary or represents a failure.
5. **Instrument at boundaries, not at every callsite.** Public async fns on RPC servicers, IO operations, transaction commits, scheduler ticks, lifecycle phase transitions. Pure internal helpers inherit the caller's span — adding `#[instrument]` there is noise and inflates exporter cardinality.
6. **One `#[instrument]` per boundary fn, with explicit `skip` and `fields`.** `#[instrument(skip_all, fields(req_id = %req.id, kind = ?req.kind), err)]`. The default macro behavior logs every argument by `Debug`, which is noisy and can leak data. **Use** `skip_all` **(not** `skip(specific_args)`**)** so every surfaced field comes from the explicit `fields(...)` list; this is the only form that catches the case where an arg is reused as a field source (e.g. `fields(schema = %name)` would otherwise auto-record `name` as a duplicate Debug field on top of the explicit `schema` field).
7. **Errors are logged once, at the boundary they cross out.** Library code returns `Result`; the binary / RPC handler logs at the point the error becomes user-visible. `#[instrument(err)]` does this for you at the function boundary; do not also `tracing::error!` inside the function for the same path. Double-logging an error is worse than not logging it — it inflates alert volume and breaks dedup.
8. **No PII, secrets, or unbounded blobs in fields.** Names, emails, tokens, raw query bodies that may contain user data, multi-MB payloads. Use `skip(...)` and emit a derived non-identifying summary (length, hash prefix, kind) instead. When in doubt, skip.
9. **No `println!` / `eprintln!` in library code.** Always `tracing` events. `println!` is fine in CLI binaries that produce user-facing stdout output, and `eprintln!` is fine for top-level binary panics before the subscriber is initialized — but inside `crates/*/src/lib.rs` and its descendants, the answer is always a `tracing` event.
10. **Subscriber init lives in `main`, never in a library.** Libraries emit; binaries decide where the emissions go. Each binary entry point owns its `tracing_subscriber` setup. The skill does not touch subscriber initialization unless explicitly invoked for an init-site fix.
11. **One commit per coherent site.** A "site" is one boundary fn + its enclosed events, or one previously-unstructured log site converted to structured form. Do not bundle "instrument scheduler tick" and "instrument query planner" into one commit — they read separately and may need to be reverted separately.
12. **No semantic change inside an instrumentation commit.** Adding a span, adding `#[instrument]`, restructuring a `tracing::warn!` from format-string to fields — all behavior-preserving (in the functional sense). If the change would alter control flow or returned values, record it as a "spin into own ticket" finding and stop — do not bundle.
13. **Route through `just` recipes, never bare `cargo`.** `just cargo-check`, `just cargo-test`, `just cargo-fmt`. Bare `cargo` skips the workspace's pinned flags.
14. **Every instrumentation batch goes on its own branch — one batch per invocation by default.** One *batch* per branch (the full set of sites in the approved Phase-1 plan, or the single site from an ad-hoc invocation), each site landed as its own commit on the batch branch. Default to **one batch = one PR for the entire approved set**; only split into multiple batches when the scope is truly enormous (rule of thumb: ~20+ sites spanning unrelated module trees). The branch lifecycle is part of the contract, not a convenience.
15. **Iterate kata task bodies in place via `kata edit <ref> --body "$(cat <body-file>)"`** (`kata edit` has no `--body-file`/`--body-stdin` — those are `kata create`-only flags)**.** Phase-1 iteration with `/plan-reviewer` updates the relevant task bodies — no new tasks per REVISE pass. (The exception is when `/plan-reviewer` returns `REVISE` because a site is missing entirely — emit a new task with `--label cha-NNN --label plan-draft`.)

If you need conventions you don't already have in context, read `docs/style-guide.md` and `docs/development-methodology-guide.md` on demand.

## Operational shape

Two sequential phases. Each phase produces an artifact the next phase reads — no implicit hand-offs, no shortcuts between phases.

### Phase 1 — Plan (one combined batch plan, gated by `/plan-reviewer`)

Walk the chosen scope (a crate, a module, a binary; or in cleanup-context mode, the touched-files list `/do-issue` passes). Identify instrumentation sites against the principles above. Emit **one kata task per site**, all under `cha-NNN plan-draft` (and `cleanup-pass` as an additional source label when invoked in `--in-cleanup-context` mode). The set of tasks *is* the plan; the `/plan-reviewer` pass *decides* — if a site should not land, the user (or `/plan-reviewer`) calls it out during review and the task is dropped from the queue via `kata delete`. Do not self-censor sites pre-emptively at walk time.

When invoked **standalone with no ticket**, use a per-invocation scope label of the user's choosing (e.g. `tracing-penca-lifecycle-2026-05-28`) in place of `cha-NNN`; the kata queue is still the source of truth.

Typical sites a Phase-1 walk surfaces:

- **Missing boundary spans** — public async fns on RPC servicers, scheduler entry points, IO operations that lack `#[instrument]`. Lens: tower-http `TraceLayer` for the outermost handler; tokio `#[instrument]` for inner async fns.
- **Silent error sites** — `Result` returned from a boundary fn with no `#[instrument(err)]` and no caller-side log. Lens: tokio `#[instrument(err)]`.
- **Format-string logs** — `info!("query {} returned {}", id, n)` that should be `info!(query_id = %id, rows = n, "query complete")`. Lens: sqlx structured-field events.
- **`println!` / `eprintln!` in library code** — direct violations of principle #9.
- **Over-instrumented hot loops** — `info!` inside a tight per-row loop that should be `trace!` or moved to a span field aggregated at loop exit.
- **Unstructured spans** — spans created with `tracing::info_span!("name")` with no fields, where the natural fields (request id, key, count) are right there at the callsite.

For each site, the kata task names (in the task body — title is one line summarizing the change):

- `crate/path/file.rs:LINE` — the symbol or block.
- **Site category** — one of the bullets above (or a justified new one).
- **Lens applied** — tokio-instrument / tower-http-trace-layer / sqlx-query-span (see "Reference exemplars" below).
- **Span name + field list** — the static literal span/event name and the explicit `(field = %value, …)` list. PII gates from principle #8 must be called out per-site if the natural fields would carry user data.
- **Level + justification** — which of `error`/`warn`/`info`/`debug`/`trace`, with the one-sentence reason tied to principle #4.
- **Behavior-preservation note** — confirm the change is additive only (no control-flow or return-value changes). If a site cannot be done additively, mark it "spin into own ticket" and exclude from the batch.
- **Inter-site dependencies** — anything earlier sites must do first (e.g. "depends on #2's `#[instrument]` on the outer fn being in place so this site's nested span has a parent"). Encode with `kata create --blocked-by <earlier-task-ref>` so the drain loop respects ordering.

Batch-level meta (kept in one **batch-summary task** with `--label cha-NNN --label plan-draft --label batch-summary`):

- One paragraph naming the scope, the sites included (qualified-ids), the execution order with one-line justifications. The `--blocked-by` graph captures the ordering at the task level; the summary is for the human reviewer.
- **Batch-level gate** — `just check` exits 0 *after the last site is committed*; no semantic change in any commit; per-site atomicity (one commit per site, no bundling).

**Invoke `/plan-reviewer` once** on the kata task set (`kata list --label cha-NNN --label plan-draft --json`). Iterate on `REVISE` items by editing the relevant task bodies via `kata edit <ref> --body "$(cat <body-file>)"` — no new tasks per pass. Re-invoke until the verdict is `APPROVED`.

#### Self-audit before posting

Before invoking `/plan-reviewer`, audit the combined plan for:

- **Nested-span field consistency.** When the plan instruments two or more spans on the same operation path (e.g. a planning span on the public fn + an IO span on a Stream / Future the fn returns), every field name that appears in both spans must carry the same identifier type and value-format. Field name across nested spans = same kind of value, full stop. If two spans both need a `catalog` field but at one site only the UUID is available and at the other only the name, *use different field names* (`catalog_uuid` vs `catalog_name`) — never let the same field name carry different types across spans on the same op.

**Hard stop:** no code edit happens before `/plan-reviewer` returns `APPROVED`. If three rounds fail to converge, halt and surface to the user.

#### In-cleanup-context post-APPROVED handling

When invoked with `--in-cleanup-context cha-NNN`, `/plan-reviewer` itself auto-flips `plan-draft` → `approved` on every cleanup-pass task and extends `orch:open-pr`'s `--blocked-by` after returning `APPROVED` (see `/plan-reviewer`'s "Post-APPROVED — cleanup-pass auto-flip" section). This skill does **no** post-APPROVED handling — return control to the caller (the outer `/do-issue` `/loop`); **skip Phase 2 entirely** — the outer drain claims and processes the now-approved cleanup-pass tasks via its uniform consumer.

### Phase 2 — Execute (one branch per batch, drain from kata)

Phase 2 runs only for the ad-hoc and in-ticket instrumentation modes. **In-cleanup-context mode skips Phase 2** — the outer `/do-issue` `/loop` is the drain.

Only after Phase 1 returns `APPROVED` *and* the user has flipped each task's label `plan-draft` → `approved` (in `kata tui`, or via the bulk-approve helper from `/do-issue` Step 3).

1. **Create the branch** via `git checkout -b <branch>` from the repo root — one branch for the entire batch; name it after the batch theme (e.g. `nhobin219/cha-XYZ-instrument-lifecycle-scheduler`), not any single site.

2. **Drain the approved kata queue.** The `--blocked-by` edges from Phase 1 give the execution order for free:
   ```bash
   while true; do
     ref=$(kata ready --unowned --label cha-NNN --label approved --json | jq -r '.issues[0].qualified_id // empty')
     [ -z "$ref" ] && break
     kata claim "$ref"
     # ...apply the additive instrumentation change from the task body...
     git commit -m "chore(<scope>): instrument <site>

CHA-XX
"
     kata close "$ref" --done --commit "$(git rev-parse HEAD)"
   done
   ```
   The change must match the task body's named span/event name, fields, and level. If implementation reveals a structural blocker not in the task (e.g. the boundary fn is generic over a non-`Debug` type, blocking the planned field), halt and return to Phase 1 for that site — no silent substitution.

   Commit as `chore(<scope>): instrument <site>` (default) or `feat(<scope>): add <thing> instrumentation` (only when the new visibility is the user-facing point of the change — e.g. enabling a new dashboard). **One commit per site; no bundling, no semantic change.** Intermediate gaps in instrumentation between commits are fine — the PR gate runs once at the end.

3. **PR gate** — after the last site is committed, `just check` must exit 0 once before opening the PR. This is the project-wide gate that mirrors CI; it is a strict superset of `just cargo-check` (`lint` + `format-check` + `test` + `static-test` + `cargo-check`). The batch's atomicity is "all commits green together"; per-site gates are out.

4. **Open the PR** with a title naming the batch theme and a body listing each commit's site with a one-line summary. After merge, sync main and delete the branch: `git checkout main && git pull && git branch -d <branch>`.

### Subagent default — single-agent, main session

This skill executes in the **main conversation** with no `Agent`-tool fan-out. Spawning a subagent (parallel Phase-1 walks across crates, parallel Phase-2 branches, etc.) requires **explicit in-session user authorization** — not just plan-time justification. The single-agent default exists because the project's review-loop discipline (`/plan-reviewer`, `/code-quality-reviewer`) is built around one author per change; parallel subagent runs split that discipline across contexts that cannot see each other's findings.

## Reference exemplars

Three canonical shapes from the `tracing` ecosystem. Every Phase-1 site names which lens applies; every Phase-1 plan justifies the chosen lens.

### tokio `#[instrument]` — span-per-async-fn at internal boundaries

[`tokio/src/runtime/scheduler/multi_thread/worker.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/scheduler/multi_thread/worker.rs) (fetch with `WebFetch` for the latest shape — search for `#[instrument]` and `tracing::`).

`#[instrument]` brackets an async fn in a span whose lifetime matches the future's. Default behavior logs all args by `Debug`; tokio's usage overrides this aggressively: `skip(self, ...)`, explicit `fields(...)`, and `err` / `ret` only where the return matters.

**Operational insight:** the macro is the right tool for inner boundary fns — the span is created lazily, the field set is fixed at compile time, and the lifetime exactly matches the future. The wrong tool for hot inner-loop fns (every invocation allocates a span) and for fns that take large/sensitive args without `skip`.

**When to apply this lens:** a candidate is an async fn at an internal boundary (one layer below an RPC handler, an IO operation, a scheduler tick subroutine) and currently has no span. Add `#[instrument(skip(...), fields(...), err)]` matching the plan's named fields and level.

### tower-http `TraceLayer` — span-per-request at the service boundary

[`tower-http/src/trace/mod.rs`](https://github.com/tower-rs/tower-http/blob/main/tower-http/src/trace/mod.rs) (fetch with `WebFetch` for the latest shape).

`TraceLayer` is a `tower::Layer` that wraps a service and creates one span per request, with hooks for `make_span_with`, `on_request`, `on_response`, `on_failure`. Works with tonic (which is `tower`-based) the same way it works with axum.

**Operational insight:** the outermost boundary of an RPC server is a `tower::Service`, not a Rust async fn — adding `#[instrument]` to a handler is one layer too deep. The request-lifecycle span (start, end, status, latency) belongs at the layer where the request first appears. Inner handlers get their own narrower `#[instrument]` spans nested inside.

**When to apply this lens:** the scope is a tonic gRPC server (or any tower-based service) with no per-request span. Install `TraceLayer` at the server builder, then narrow with `#[instrument]` on the inner handlers.

### sqlx query spans — structured-field events for fine-grained operations

[`sqlx-core/src/logger.rs`](https://github.com/launchbadge/sqlx/blob/main/sqlx-core/src/logger.rs) (fetch with `WebFetch` for the latest shape — search for `tracing::event!` and `QueryLogger`).

Each query gets a span with `db.statement`, `db.system`, `rows_affected`, `rows_returned` as structured fields, emitted at a level chosen by latency (slow queries log warn; normal queries log debug). Field names follow OpenTelemetry semantic conventions even though sqlx doesn't ship OTel exporters.

**Operational insight:** when a single operation has natural metadata (query id, key, count, duration, kind), the right shape is one span per operation with those metadata as fields — not one event per phase of the operation. Aggregate inside the span; emit one summary event at completion. The field names matter: `rows`, `query_id`, `req_id`, `kind` are reusable across collectors; `n`, `x`, `the_thing` are not.

**When to apply this lens:** a candidate is a per-operation site (a query, a write, a lifecycle phase) that currently logs in pieces with format strings. Replace with one span per operation, structured fields for the metadata, and one summary event at the end.

**Note on tracing 0.1 + Streams.** `tracing` 0.1 provides `Instrumented<F>: Future` but not `Instrumented<S>: Stream` — `.instrument(span)` on a `Stream` requires the (maintenance-mode) `tracing-futures` 0.2 crate with the `futures-03` feature. The workspace already pulls in `tracing-futures = "0.2"` via [CHA-310](https://linear.app/chapala/issue/CHA-310), so reach for `.instrument(span)` on streams freely; see [CHA-322](https://linear.app/chapala/issue/CHA-322) for the eventual removal path once `tracing` 0.2 absorbs Stream instrumentation. Considered-and-rejected alternative: `.instrument(span)` on the inner Future that produces the stream — closes the span before any streaming, captures only the handshake, misses streaming duration + mid-stream errors.

### Lens routing — the three questions

For every site, answer:

| | tokio `#[instrument]` | tower-http `TraceLayer` | sqlx query span |
| --- | --- | --- | --- |
| Where does the span begin? | at the macro-annotated fn entry | at the service layer, before the handler | at the operation start (manual `info_span!`) |
| What's the natural lifetime? | the future | the request | the operation |
| What populates the fields? | macro `fields(...)` + `skip(...)` | layer hooks (`make_span_with`, `on_response`) | callsite (`span.record(...)` as values become known) |

If a site fails all three (it's not a fn boundary, not a service boundary, and not a discrete operation), it is probably an *event*, not a span — emit a structured event at the appropriate level and move on.

### Stream instrumentation convention

`#[instrument]` doesn't attach cleanly to non-async fns that return a `Stream` — the work happens lazily inside the `async_stream::try_stream!` body, not in the fn call, and the `tracing` crate's `Instrument` trait only covers `Future`, not `Stream`. Two shapes, by what the site needs:

- **Stream-level span (preferred when timing is wanted)** — create a `debug_span!` with counter fields declared `tracing::field::Empty`, attach it to the stream via `tracing_futures::Instrument`, accumulate counters in the body, and `Span::current().record(...)` them at end-of-stream (CHA-417). With `PENCA_SPAN_TIMING` the span-close line carries busy/idle for the whole stream lifetime. An errored or cancelled stream still closes the span with timing but leaves the counters unrecorded — a timed close with no counts reads as "aborted", not "zero rows". When the span wraps a *new* pass-through stream layer (rather than a pre-existing block), gate the wrapper on `span.is_disabled()` so the disabled path stays zero-cost.
- **Bracket / constructed events (fallback when a span is overkill)** — manual `tracing::debug!` events using one of two name patterns: **`"<fn> start"` / `"<fn> complete"`** when the fn owns the stream body (partial-drop traces show start without complete — the operator-visible early-cancellation signal, not a bug), or **`"<fn> constructed"`** fire-and-yield when the fn builds a SQL string + delegates to a deeper streaming primitive without owning the cursor (cursor-lifecycle visibility comes from the downstream site).

Use the full fn name in the span/event literal, not an abbreviation — keeps names self-locating from a log grep. Field-set PII gates are identical to `#[instrument]` boundaries (principle #8).

In-repo precedent: `crates/penca-storage-hot/src/query.rs::stream_query_as_batches` carries the stream-level span (as do the response-stream siblings `ipc_encode` in `penca-server-grpc/src/ipc.rs` and `flight_encode` in `penca-sql-server/src/flight_sql/service.rs` — the latter with the `is_disabled` gate; all CHA-417); its three callers `read_stream` / `audit_upserts_stream` / `audit_deletes_stream` delegate (single `constructed` event).

## References

Fetch on demand:

- `docs/style-guide.md` — repo-local Rust + Python conventions.
- `docs/development-methodology-guide.md` — three valid responses, two-loop TDD, KISS.
- [`tracing` crate docs](https://docs.rs/tracing/latest/tracing/) — `Span`, `Event`, level semantics, `#[instrument]` attribute options.
- [`tracing-subscriber` docs](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/) — `EnvFilter`, `fmt::Subscriber`, layer composition. Read only for init-site fixes; this skill does not modify subscribers by default.
- [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/) — field naming reference (`db.system`, `rpc.service`, `http.request.method`). Useful for picking field names that will compose with OTel later, even though Penca does not export OTel today.

## Three valid responses

When an instrumentation site hits a wall — a planned field can't be derived without taking ownership of a moved value, the boundary fn is generic over a non-`Debug` type, `#[instrument]` on the candidate would change a public API (e.g. forcing a lifetime annotation) — the three valid responses are:

1. **Execute** — apply the instrumentation as planned, gates green.
2. **Clarify** — return to Phase 1 for that site, revise the plan, re-invoke `/plan-reviewer`.
3. **Challenge** — push back on the site's premise; record it as out-of-scope or as a "spin into own ticket" finding.

There is no fourth option of silently bundling a functional change into an instrumentation commit, skipping `/plan-reviewer` for an "obvious" instrumentation, or substituting a different mechanism than the one the approved plan named. Behavior-preservation (in the functional sense) is the contract; the gates exist to enforce it.
