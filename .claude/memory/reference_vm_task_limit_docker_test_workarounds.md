---
name: reference_vm_task_limit_docker_test_workarounds
description: This VM kills background/compile tasks at ~10 min; workarounds for Docker rebuilds and cargo test during a /do-issue drain
metadata: 
  node_type: memory
  type: reference
  originSessionId: 4e9f6b9a-52f3-49a3-8e29-85bd2dde29e0
---

Background Bash tasks on this VM are SIGTERM-killed at ~10 min wall-clock (the `run_in_background` / Bash `timeout` ceiling). This bites every long build. Workarounds proven on CHA-380 (PR #309):

- **Docker image rebuilds** (`just penca-up` recompiles when Rust source changes; a `penca-core` touch forces a near-full workspace rebuild, ~8–17 min). On kill, just **re-run** — Docker caches each COMPLETED layer (`cargo chef cook` deps + the `cargo build` RUN), so a retry resumes and usually finishes. Isolate the build: run `just penca-up` alone (build+start), NOT `just integration-test` (which does up+test+down in one killable task).
- **Run tests against the kept-up stack** instead of `just integration-test` per file (avoids rebuild-per-run): after `penca-up`, `export COMPOSE_PROJECT_NAME="penca-$(basename "$PWD")"` — **`penca-penca`** in the standard `~/code/penca` checkout, container `penca-penca-query-1` confirms — then `set -a && source docker/test.env docker/.client.env docker/.baseline.env && set +a`, then `uv run pytest tests/integration/integration_<name>_test.py -q`. Fast (~1 min/file), fits the limit.
- **`COMPOSE_PROJECT_NAME` is NOT in `docker/*.env`** — the Justfile *exports* it (`export COMPOSE_PROJECT_NAME := "penca-" + \`basename "$PWD"\``), so sourcing the env files alone leaves it unset and every test that reads it to fetch container logs dies with `KeyError: 'COMPOSE_PROJECT_NAME'`. Cost a full 17-minute suite run to 45 such failures on 2026-07-27 (CHA-517), clustered in the index-seek / point-read / metadata-fastpath / flight-sql files, which reads exactly like a real regression until you count them: 45 failures, 45 KeyErrors. **Attribute before debugging** — a 1:1 match between failure count and one error signature means the harness, not the code.
- **`cargo test --workspace` cannot finish locally** — penca-api's test binary links datafusion/lance; the test-profile compile (feature-unified with dev-deps) is >20 min and never fits, even with retries. Instead: run `cargo test -p <changed-crate> --lib` for the datafusion-FREE crates (penca-core/db/storage-meta compile fast, no datafusion), and lean on CI's "Rust clippy + fmt + test" job (no time limit, ~5 min) for the full-workspace test. `cargo check`/`clippy --workspace` DO fit (dev profile, ~5 min warm) and cover all non-test code.
- **Disk**: repeated rebuilds + killed `cargo test` runs fill `/` (50 G). `docker builder prune -f` is safe (see [[reference_docker_builder_prune_hangs]]); dropping host `target/debug` frees ~25 G when disk-blocked.
