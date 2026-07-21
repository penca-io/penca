---
name: code-comments
description: The Penca commenting standard — comment WHY, never WHAT. Self-documenting code and precise names carry the "what"; comments are reserved for non-obvious rationale, correctness invariants, third-party/spec quirks, regex intent, and TODO(CHA-NNN) pointers. The default action on a comment is DELETE, not add. Read preemptively while implementing so comments are good the first time (the primary use — NOT a cleanup pass, NOT wired into /do-issue orchestration); invoke ad-hoc with a scope to prune an over-commented file by hand.
argument-hint: "[<scope> — file/crate/dir to audit; omit for guidance-only]"
allowed-tools: Read Grep Glob Bash Edit WebFetch
---

# Code comments

The Penca commenting standard. **Comments are an apology, not a requirement — good code mostly documents itself.** A comment earns its place only when it tells the reader something the code and its names cannot: *why* this line exists, *why* it's written this odd way, what invariant it protects, or what third-party quirk forced it. Everything else is noise, and noise is worse than nothing — it rots, it lies after the code changes, and it buries the few comments that matter.

**The default action on any comment is DELETE.** This skill's bias is subtraction. The repo's problem is over-commenting, not under-commenting; adding a comment is the rare, deliberate exception, not the reflex.

## The one test

Before writing or keeping a comment, ask:

> **Would deleting this comment lose information the code + names + types + tests don't already carry?**

If **no** → delete it (or never write it).
If **yes** → first try to make the answer *no* by improving the code: rename a variable, extract a named constant, split a function, add a type. Only when the "why" genuinely cannot live in the code does it become a comment.

A comment is the fallback for what you couldn't express in the code itself — reach for a rename or an extraction first, a comment last.

## When to use this skill

**Primary use — read for guidance while implementing.** `/do-issue`'s "read the conventions up front" step loads this SKILL.md preemptively, alongside `docs/style-guide.md`. The implementer internalizes the standard and writes good comments *the first time*, so no cleanup pass is ever needed. This is the whole point: proactive, not reactive.

**Secondary use — ad-hoc audit.** Invoke `/code-comments <scope>` to prune an existing over-commented file, crate, or directory by hand. See "Ad-hoc audit mode" below. This is a manual, on-demand action a human chooses.

**This skill is deliberately NOT:**
- **A `/do-issue` cleanup pass.** It is not an `orch:run-cleanup` step and is never auto-invoked by the drain. Good comments come from the implementer knowing the standard, not from a bot re-commenting after the fact. (Contrast `/clean-code-refactor` and `/tracing-instrument`, which *are* wired as cleanup passes.)
- **The heavy two-phase kata-gated machinery** of `/clean-code-refactor`. There is no plan-draft/plan-reviewer cycle here — comment edits are behavior-preserving and low-risk. The ad-hoc mode is a straight walk-and-prune.
- **A license to touch logic.** Comment edits change comments only. If you spot a real bug or a refactor while auditing, that's a separate `/do-issue` ticket, not a smuggled diff.

## Grounding principles

Inlined here intentionally — this skill is a durable artifact and must remain self-sufficient.

### 1. Comment the WHY; let the code be the WHAT

The code already says what it does. A comment that restates the next line is pure redundancy — the #1 form of comment bloat in this repo.

```rust
// BAD — restates the code
// increment the counter
counter += 1;

// loop over every segment and read its bytes
for segment in segments {
    let bytes = segment.read()?;
}
```

```rust
// GOOD — no comment; the code is self-evident
counter += 1;

for segment in segments {
    let bytes = segment.read()?;
}
```

Keep the comment only where the *reason* is invisible in the code:

```rust
// GOOD — the WHY is not in the code (pydantic-core recursion_guard.rs shape)
// saturating_add is faster than checked_add (no error path) and the recursion
// limit is hit long before u8 overflows, so saturation is never actually reached.
self.depth = self.depth.saturating_add(1);
```

### 2. Only comment genuine complexity or non-obvious rationale

If a function is clear, it needs no comment. Reserve prose for business logic, a subtle algorithm step, a correctness invariant, or a decision a future reader would otherwise "fix" and break.

```python
# BAD — every line narrated
def hash_key(data):
    h = 0                        # the hash
    n = len(data)                # length of the string
    for i in range(n):           # loop over each character
        c = ord(data[i])         # get the character code
        h = (h << 5) - h + c     # update the hash
    return h
```

```python
# GOOD — one comment, only where the operation is non-obvious
def hash_key(data):
    h = 0
    for ch in data:
        # Polynomial rolling hash, 31*h + c: (h << 5) - h stands in for the
        # *31 (32 - 1), same multiplier as Java's String.hashCode.
        h = (h << 5) - h + ord(ch)
    return h & 0xFFFFFFFF  # clamp to 32-bit; callers index a fixed-size table
```

### 3. No commented-out code

Version control is the history. Dead code behind `//` is a distraction that readers must mentally evaluate and that greps pollute.

```rust
// BAD
persist_committed(tx)?;
// persist_all(tx)?;
// legacy_flush(tx)?;
```

```rust
// GOOD
persist_committed(tx)?;
```

If you might need it back, it's in `git log`. Delete it.

### 4. No journal / changelog comments

Who-changed-what-when belongs in `git log`, the commit body, or an ADR — never in a comment block that grows forever and drifts from reality.

```python
# BAD
# 2026-05-12 nhobin: switched locked counter -> sequence
# 2026-06-01 nhobin: added abort axis
# 2026-06-20 nhobin: purge now owns aborts
def next_seq(...): ...
```

```python
# GOOD — the function stands alone; history is in git / the ADR
def next_seq(...): ...
```

This includes citing tickets as history. `// CHA-444 replaced the old clamp` is archaeology; a reader doesn't need it. (A *forward* pointer — `TODO(CHA-155)` for unfinished work — is different and encouraged; see principle 8.)

### 5. No positional markers or section-divider banners

`// ======== Handlers ========`, `// ---- config ----`, `//////// setup` add visual noise. Structure comes from function boundaries, module layout, and whitespace — not ASCII banners.

### 6. Third-party / spec quirks — DO comment (this is what comments are FOR)

Undocumented library behavior, a deliberate deviation from a spec to match reality, a workaround for an upstream bug — a future reader *will* try to "clean this up" and break it unless you tell them why. These are the highest-value comments in the codebase.

```python
# GOOD — requests/sessions.py shape: deliberate spec deviation, flagged
# Do what browsers do, despite the standard: turn 302s into GETs.
if response.status_code == codes.found and method != "HEAD":
    method = "GET"

# GOOD — a real Penca driver quirk
# adbc_driver_manager calls prepare() unconditionally in _prepare_execute,
# so even a bare no-param SELECT takes the prepared path. Don't "optimize"
# it away — the driver, not us, decides. See CHA-355.
```

Anchor the quirk to a source of truth where one exists (an upstream issue number, an RFC section, a Linear ticket), the way pydantic-core writes `// see #143 this is used as a backup in case the identity check fails`.

### 7. Correctness invariants, safety, and empirical constants — DO comment

Why a value is *this* value, why an ordering is load-bearing, what breaks if the invariant is violated.

```rust
// GOOD — load-bearing invariant that the code alone doesn't reveal
// Swap in a FRESH catalog_list per derived context: a cloned SessionState
// keeps catalog_list Arc-shared, so two units would otherwise register their
// tables into the SAME catalog and collide on names.
.with_catalog_list(Arc::new(MemoryCatalogProviderList::new()))
```

```rust
// GOOD — empirically-tuned constant (pydantic-core shape)
// Trial and error: 16 is the sweet spot; larger makes the array lookups in
// the hot path measurably slower.
const ARRAY_SIZE: usize = 16;
```

```python
# GOOD — ordering rationale that prevents a real bug (requests shape)
# Extract keys first to avoid mutating the dict while iterating it.
none_keys = [k for k, v in merged.items() if v is None]
for key in none_keys:
    del merged[key]
```

### 8. Regex — always comment expected input and matching behavior

Regexes are write-once, read-never. State what they match, with a concrete example.

```python
# GOOD
# Matches "cha-<digits>" case-insensitively, capturing the number:
#   "CHA-432" -> "432", "cha-1" -> "1". Leading zeros preserved.
TICKET_RE = re.compile(r"cha-(\d+)", re.IGNORECASE)
```

Rust's `x` (verbose) flag or an inline example comment serves the same purpose for `regex` crate patterns.

### 9. TODOs always reference a Linear ticket: `TODO(CHA-NNN)`

A bare `TODO` / `FIXME` is untracked debt that bloats the codebase and never gets done. Every TODO names an actionable Linear issue, so the future work is greppable (`grep -r CHA-155`) and lives in the tracker, not just the source.

```rust
// BAD
// TODO: stream this instead of buffering the whole SELECT

// GOOD
// TODO(CHA-155): stream batches and emit one Change per batch instead of
// buffering the whole SELECT result set.
```

If you have a TODO with no ticket, mint the ticket first (or fold the fix into the current change). This mirrors `docs/style-guide.md` → "Future improvements and TODOs".

### 10. Doc comments (`///`, docstrings) state the current contract — not history or internals

Public doc comments describe what the API *is* and its contract for a caller: inputs, outputs, invariants, panics/errors. They do **not** narrate internal mechanics, derivations, or what a prior version did — those leak implementation detail and rot. This is the same rule `docs/style-guide.md` applies to proto comments ("describe current wire semantics — not history or internal derivations"), extended to all doc comments.

```rust
/// BAD — internal mechanics + history in a public doc comment
/// Returns the tie-breaker. Previously we derived this from data_log_prefix_uuid
/// but CHA-380 changed it; internally this reads the snapshot fast-path then
/// falls back to the persist substrate.
pub fn tie_breaker(&self) -> Uuid { ... }
```

```rust
/// GOOD — the caller's contract, nothing else
/// The deterministic tie-breaker for rows sharing a commit sequence.
/// Stable across reads; two rows never share one within a table.
pub fn tie_breaker(&self) -> Uuid { ... }
```

## Reference exemplars

Three real repositories whose comment discipline is the standard to match — one per relevant language, plus the canonical anti-pattern catalog. Fetch with `WebFetch` when you want fresh examples; do not treat Penca's *current* comments as exemplary (over-commenting is exactly the problem this skill exists to fix).

- **[psf/requests](https://github.com/psf/requests) (Python)** — `src/requests/sessions.py`, `adapters.py`, `utils.py`. Nearly every inline comment justifies a decision: a browser-compat deviation, an RFC exception kept for backwards compatibility, a security consideration (withholding `Proxy-Authorization` over TLS to avoid leaking it), an edge case that prevents a hang. The gold standard for "comment the WHY" in Python.
- **[pydantic/pydantic-core](https://github.com/pydantic/pydantic-core) (Rust)** — `src/recursion_guard.rs`, `src/lookup_key.rs`, `src/serializers/`. Comments document segfault-prevention rationale, performance trade-offs (`saturating_add` vs `checked_add`), platform stack-size constraints (wasm/PyPy), empirically-tuned constants, and defensive-`Drop` safety. The gold standard for Rust.
- **[ryanmcdermott/clean-code-javascript § Comments](https://github.com/ryanmcdermott/clean-code-javascript#comments)** — the canonical catalog of what NOT to do: comment only business-logic complexity, no commented-out code, no journal comments, no positional markers. Language-agnostic; the source of principles 2–5 above.

Supplementary, on demand: the **Linux kernel** networking/mm subsystems are the C/system gold standard for explaining *why a hardware workaround or memory-safety hack is necessary* — the same instinct as principle 6, at the systems level.

## Ad-hoc audit mode

When invoked as `/code-comments <scope>` (a file, crate, or directory), walk the scope and prune. This is a light, single-pass, behavior-preserving edit — no kata graph, no plan gate.

1. **Read the scope in full.** You must understand the code to judge whether a comment carries real "why".

2. **Grep for the high-frequency smells**, then judge each hit against the principles — grep *locates* candidates, it does not decide:
   ```bash
   # commented-out code (statements behind //  or  #, not prose)
   rg -n '^\s*(//|#)\s*[a-z_]+\s*[({=.].*[;):]?\s*$' <scope>
   # journal / dated comments
   rg -n '(//|#).*(20[0-9]{2}[-/][0-9]{2}|changelog|updated by|history:)' <scope>
   # positional-marker banners
   rg -n '(//|#)\s*[=\-*]{4,}' <scope>
   # bare TODO/FIXME without a ticket (rg's default engine has no look-around,
   # so filter with a second pass rather than a negative look-ahead)
   rg -n '(TODO|FIXME|XXX)' <scope> | rg -v 'CHA-'
   ```

3. **For each comment, apply the one test:**
   - **Redundant** (restates the code) → delete.
   - **Commented-out code / journal / positional marker** → delete.
   - **Bare TODO/FIXME** → mint or find the Linear ticket and rewrite as `TODO(CHA-NNN)`; if the fix is trivial and in-scope, do it and delete the TODO. Never leave it bare.
   - **Weak why-comment** (gestures at a reason but vaguely) → tighten it, or replace the comment with a rename/extraction that makes it unnecessary.
   - **Genuine why / invariant / quirk / regex / contract** → keep. Add a *missing* why-comment only where a real, non-obvious rationale is currently undocumented — sparingly.

4. **Commit** comment-only changes. Conventional type `docs` (comments are documentation-in-code): `docs(<scope>): prune redundant comments`. If the audit is part of a larger change, fold it into that change's commits instead of a standalone one.

5. **Gate.** Run `just check` once before opening a PR — comment edits rarely break it, but a mangled doc-comment or an unclosed block comment can. Route through `just`, never bare `cargo`/`ruff`.

Scope discipline: comments only. A tempting refactor or a real bug found mid-audit is a separate `/do-issue` ticket, surfaced to the user — not bundled into a `docs:` commit.

## Relationship to other skills and docs

- **`/clean-code-refactor`** is the natural partner: its self-documenting-code principles (meaningful names, small single-responsibility functions, no aliasing) are what make comments unnecessary in the first place. When the one test says "improve the code instead of commenting," that improvement is often a `/clean-code-refactor` candidate.
- **`docs/style-guide.md`** owns the `TODO(CHA-NNN)` convention (principle 9) and the proto-comment-hygiene rule that principle 10 generalizes. This skill is the fuller treatment; the style guide is the quick reference.
- **`docs/development-methodology-guide.md`** → KISS: "document *why* complexity exists" is the same instinct as principle 7, at the design level (record necessary complexity's rationale in `docs/design-decisions.md`, not scattered comments).
