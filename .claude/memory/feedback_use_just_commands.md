---
name: Use just commands, not bare tool invocations
description: Run tests/lint/format via `just` recipes (test, lint, check, format), never bare `uv run pytest` / `uv run ruff`
type: feedback
originSessionId: 38ccb005-5c60-4664-ae22-c2067a924463
---
Always run dev tooling (tests, lint, format, type check) via the repo's `just` recipes rather than calling `uv run pytest` / `uv run ruff` / `uv run ty` directly.

**Why:** The justfile encodes the canonical invocation (correct paths, flags, env) and picks up changes in one place. Bare calls work by accident but drift from what CI actually runs and bypass recipe-level composition like `just check`.

**How to apply:**

- Tests: `just test` (unit) — pass pytest args via `just tdd -k ...`
- Integration: `just integration-test ...`
- Lint: `just lint`
- Format: `just format` (fix) / `just format-check` (check-only)
- Everything: `just check` (lint + format check + tests)
- Rust: `just cargo-check`, `just cargo-test`, `just cargo-clippy`, `just cargo-fmt`, `just cargo-fix`

Run `just --list` first if unsure which recipe applies — do not reach for `uv run` / `cargo` directly.
