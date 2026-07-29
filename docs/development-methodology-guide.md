# Development Methodology

Cross-cutting methodology principles for Penca development, referenced by
the agent skills. The end-to-end ticket *workflow* itself lives in the
[`/do-issue`](../.claude/skills/do-issue/SKILL.md) skill — the executable
form of the workflow, including its design gate and red-test gate. This
document holds the principles those skills rely on but that aren't specific
to one workflow step: the three valid responses to an approved plan, the
two-loop TDD model, and KISS.

## Three valid responses

Once a plan is signed off, the only valid responses to it during
implementation are:

1. **Execute** the plan as written.
2. **Clarify** if part of it is ambiguous — ask, don't guess.
3. **Challenge** if you believe it's wrong — surface the challenge to
   the user before changing course.

There is no fourth option of *silently substituting a different
approach*. A challenge that becomes a different commit is option 4
wearing a costume — the diff diverges from the approved plan, and the
commit message either lies about the divergence or papers over it.
Mid-task discovery of a "structural blocker" is a **halt-and-surface
event**, not a license to pivot. If you find yourself building
something not in the approved plan, stop and ask.

This applies equally to humans and agents, but it is the failure mode
that subagents fall into most often: rationalizing a deviation
post-hoc and writing a commit message that paints the deviation as
the spec'd work. Reviewing diffs against the plan's named mechanisms
(the mechanism-bound plan rule enforced by `/do-issue` and
`/plan-reviewer`) catches it after the fact; refusing option 4 prevents
it.

## Test-driven development

Penca uses a two-loop TDD model: an **outer loop** of integration tests
that pin externally observable behavior, and an **inner loop** of focused
red/green/refactor cycles that drive production code into existence.

The outer loop is the red-test phase of `/do-issue` — write the integration
tests in `tests/integration/` from the acceptance test list, commit them
red, get sign-off. Those tests are committed and stay in the repo as
regression guards.

The inner loop, defined by Kent Beck and Martin Fowler, is the
implementation phase. The cycle is:

1. **Pick the next behavior.** Usually one item from the acceptance test
   list, narrowed to a slice of code small enough to test directly. The
   sequence matters — start with slices that force key interface decisions.

2. **Red.** Write a single, concrete, runnable test for that slice in
   `tests/tdd/` (gitignored — see "Development tests are not committed"
   below). Run via `just tdd` and confirm it fails. If it passes without
   changes, either the test is wrong or the behavior already exists.

3. **Green.** Write the minimum code to make that test (and all prior
   tests) pass. Do not refactor during this step — the only goal is a
   passing test suite. "Make it run, then make it right."

4. **Refactor.** Improve the structure of both new and existing code.
   Address genuine duplication discovered through testing, not speculative
   abstractions. All tests must still pass after refactoring. Skipping
   this step is the most common way to screw up TDD — it produces a messy
   aggregation of code fragments.

5. **Repeat** until every outer-loop integration test is green.

### What to test

Tests should verify high-level behavior: "does this code successfully do the
thing it's supposed to do?" Do not write tests that add no value (e.g.,
testing that constants equal themselves, testing trivial getters, or testing
implementation details that aren't meaningful behavior).

**Don't test upstream libraries.** Before adding a test, decompose what it
actually asserts and label each link in the chain as Penca-owned or upstream.
If most of the chain is "library X does what its docs say," it isn't a test —
it's expensive boilerplate. A proposed integration test that read
`docker logs` for every servicer and grepped for a startup line turned out to
assert five things, of which four were someone else's job (compose env
presence — a `rg` away; every binary calling `init_tracing()` — also `rg`;
`tracing-subscriber` writing to stdout; docker capturing stdout) and only one
was ours. Pin the Penca-owned subset with the cheapest tool that covers it:
`rg` for one-time structural guarantees ("every binary calls X"), unit tests
for pure-function logic, and integration tests only for behavior that
genuinely needs the live stack — cross-service RPC ordering, real Postgres or
S3 interaction.

**Don't build test harnesses for local dev tooling.** Production and engine
paths earn tests; a convenience `just` recipe, a one-off script, or anything
whose failure is immediately visible to the person running it generally does
not. Restructuring a Justfile recipe into a script so a stub can drive it is
the specific thing to avoid — fix the bug, verify by hand, record the
verification in the commit message, and move on. Weigh the guard against the
failure it prevents, not against how embarrassing the bug was: "I got this
wrong twice" argues for care, not infrastructure. The questions that matter
are blast radius (a recipe that runs once per ticket on a disposable VM and
leaves a few stale rows behind is cheap to get wrong) and whether the failure
is *silent* — a hang announces itself the moment you Ctrl-C it, while exiting
0 having skipped every item past a list cap does not. Only silent *and* costly
earns machinery.

Cheap **structural** checks are a different thing and are already the repo
norm: `static_perf_framework_wiring_test.py` greps recipe bodies for the
symbols they must invoke, at near-zero cost and no restructuring. Add those
freely — just don't mistake them for behavioral coverage. A grep confirms a
guard is *present*, not that it is *correct*.

**Timestamp and watermark helpers are the exception — they earn exhaustive
coverage.** Helpers that compute a cutoff, eligibility watermark, snapshot
upper bound, or clamp from system state are load-bearing correctness
primitives, and a single missed input combination can silently break the
live-query safety chain. Give them unit tests over every *reachable* cross
product of input states, not happy-path examples. This constrains the
signature: the helper must take plain primitives or small structs and return
a typed output — no DB connection, no SQL inside it — so the tests need no
fixtures. Enumerate the cross product as a table (`dimension | values to
cover`) while scoping the work, not as prose. It stays small once structural
constraints apply: most dimensions collapse to `<` / `==` / `>` relative to
another input, or `0` vs positive, or empty vs single vs multiple. Cover the
empty-input branch explicitly (it usually has sentinel behavior), and add
permutation-invariance tests wherever an input is a set or map, so the output
can't depend on iteration order.

### Development tests are not committed

TDD tests are a development tool — they help you build changes verifiably and
catch regressions as you go. Write them in `tests/tdd/`, which
is gitignored and **not committed** to the repository. Run them with
`just tdd` (or `just tdd -k test_name` to filter). The repo's committed test
suite (`tests/integration/`, `tests/performance/`) is reserved for tests
that verify end-to-end functionality and meaningful system behavior. If a
TDD test happens to be valuable as a permanent regression test, promote
it to `tests/` deliberately.

Once a change is successfully implemented and all checks pass, wipe the
`tests/tdd/` directory so stale tests don't accumulate or
interfere with future work.

## KISS methodology

Follow the [KISS principle](https://en.wikipedia.org/wiki/KISS_principle): keep
it simple. Complexity should be introduced only when it earns its place — never
speculatively.

- **User interfaces must be dead simple.** The client API, proto definitions,
  and configuration surfaces are the most important places to keep simple.
  Internal complexity is tolerable when necessary; leaking complexity to the
  user interface is not.
- **Try the simplest approach first.** If a straightforward solution works, ship
  it. Add abstraction, indirection, or optimization only when a concrete need
  demands it.
- **Document why complexity exists.** When a design is necessarily complex (e.g.,
  partition-direct reads to avoid lock contention), record the rationale in
  `docs/design-decisions.md` so future readers understand it is intentional, not
  accidental.

## Layered session scope and caching

Building a DataFusion `SessionState` with the full default function registry +
analyzer/optimizer rule sets has a one-time *cold* cost (~1.4 ms — initialising
the default-function `OnceLock` singletons; this is the figure CHA-353's debug
bench reported). A *warm* `SessionContext::new()` is much cheaper (~128 µs in
release — it just collects the already-built singleton `Arc`s into a HashMap and
assembles the rule lists), but a request path that builds one per call still
pays that warm cost each time. The pattern Penca uses where a `SessionContext`
is needed on a hot path:

1. **Build the expensive `SessionState` once, process-wide** — a *template*
   (`SessionStateBuilder::new().with_default_features().build()`), constructed
   at service startup and held behind an `Arc`.
2. **Derive each per-unit context by a microsecond clone** —
   `SessionStateBuilder::new_from_existing(template.clone())` reuses the
   template's `scalar_functions` + analyzer/optimizer rules (HashMap + `Arc`
   clones) instead of rebuilding them.
3. **Give every derived context a FRESH `catalog_list`** —
   `.with_catalog_list(Arc::new(MemoryCatalogProviderList::new()))`. This is the
   load-bearing step: a cloned `SessionState` keeps
   `catalog_list: Arc<dyn CatalogProviderList>` `Arc`-shared, so without the
   swap two derived contexts would register their tables into the *same*
   catalog and concurrent units would collide on shared table names.
   (`new_from_existing` disables default-catalog creation because the template
   already had one — re-enable it on the config so the fresh list gets its
   `datafusion`/`public` default back, or unqualified `register_table` / SQL
   fails to resolve the catalog.)

**Why sharing the registry is correctness-safe.** Nothing query-specific lives
on the `SessionState`. Point-in-time (`as_of`) lives in the SQL string + the
per-query segment list on the `TableProvider`s; schema lives on the per-query
providers. The template caches only query-*independent* function/rule
machinery, so it is safe to share as long as the **catalog** (and thus the
registered tables + their as_of/schema state) stays per-unit.

Two instances of this pattern exist today:

- **penca-sql-server, per connection** — a startup template → one
  `Arc<SessionContext>` per TCP connection (`ConnSessionFactory::build_ctx`) →
  a per-connection `statement_cache` of already-planned `LogicalPlan`s. Three
  nested scopes: process (template) → connection (ctx + catalog snapshot) →
  statement (cached plan).
- **the cold-read path (query service)** — a startup template → one derived
  `SessionContext` per cold `stream_merged` unit (`penca_dl::derive_cold_session`,
  used by `build_persist_session` / `build_snapshot_session` and the
  snapshot-pruning predicate plan). CHA-421.

When you reach for `SessionContext::new()` on a request path, reach for a
context derived off a process template instead.
