---
name: feedback-clippy-not-in-cargo-check
description: cargo check does NOT run clippy — too_many_arguments etc. surface only at just check; run just cargo-clippy before pushing after signature changes
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 3fd45a36-f796-458c-a0f9-0ce3fd79b480
---

`cargo check` (and `cargo check --all-targets`) does NOT run clippy lints. Penca's `just check` runs `just cargo-clippy` with `-D warnings`, so clippy lints are hard errors at the PR gate but invisible to a plain `cargo check` loop. The pre-commit hook does not run clippy either, so a commit can land green and still fail `just check`.

**Why:** during CHA-429 I added params to several read/audit helpers (`resolve_cold`, `cold_*_audit_batches`, `resolve_query_snapshot`), pushing them to 8 args. `cargo check`/`cargo test` passed and the commits landed, but `just check` at `orch:open-pr` failed on `clippy::too_many_arguments` (8/7), forcing a separate fixup commit (`#[allow(clippy::too_many_arguments)]` — the codebase's established pattern, cf. `audit_deletes_stream`).

**How to apply:** after any change that adds function parameters, widens signatures, or could trip a lint (new clones, needless borrows, etc.), run `just cargo-clippy` (not just `cargo check`) BEFORE committing/pushing — ideally as part of the same iteration that made the change, so the `#[allow]` or refactor lands in the originating commit rather than a trailing fixup. For too_many_arguments specifically, `#[allow(clippy::too_many_arguments)]` with a one-line "why irreducible" comment is the accepted in-repo fix for helpers whose param set is genuinely cohesive. Related: [[feedback_just_check_gate_trust]], [[feedback_use_just_commands]].
