---
name: reference_vm_resource_limits_docker_disk_memory
description: "This VM's hard limits and their workarounds — ~10min background-task kill, ~7GB RAM (OOM), 50GB disk — plus which docker prune is safe when a build is disk-blocked."
metadata:
  node_type: memory
  type: reference
---

Every long build on this VM runs into one of three ceilings. The workarounds
interact, so they live together.

## Task limit: background tasks are SIGTERM-killed at ~10 min

- **Docker image rebuilds** (`just penca-up` recompiles when Rust source
  changes; touching `penca-core` forces a near-full workspace rebuild, ~8–17
  min). On kill, just **re-run** — Docker caches each COMPLETED layer (the
  `cargo chef cook` deps and the `cargo build` RUN), so a retry resumes and
  usually finishes. Isolate the build: run `just penca-up` **alone**, never
  `just integration-test` (which does up + test + down in one killable task).
- **Run tests against the kept-up stack** rather than `just integration-test`
  per file. After `penca-up`:
  `export COMPOSE_PROJECT_NAME="penca-$(basename "$PWD")"`, then
  `set -a && source docker/test.env docker/.client.env docker/.baseline.env && set +a`,
  then `uv run pytest tests/integration/integration_<name>_test.py -q`
  (~1 min/file). Safe for **subsets only** — see
  [[feedback_integration_suite_full_fresh_before_pr]] for why the full suite
  needs a fresh stack.
- **Chunk the files DISJOINTLY — each file exactly once per stack.** Never
  re-run the same test file against a persistent stack: fixed-name tests (e.g.
  `branch_inheritance_read`) fail on the 2nd run with
  `AlreadyExistsError: catalog name already in use`. That is a **false** failure
  from state pollution, not a bug you introduced. `just penca-down` +
  `penca-up` resets to a clean stack when you need to re-run one.
- **`COMPOSE_PROJECT_NAME` is NOT in `docker/*.env`.** The Justfile *exports*
  it, so sourcing the env files alone leaves it unset and every test that reads
  it to fetch container logs dies with `KeyError: 'COMPOSE_PROJECT_NAME'`. Cost
  a full 17-minute run to 45 such failures, clustered in the index-seek /
  point-read / metadata-fastpath / flight-sql files — which reads exactly like
  a real regression until you count them: 45 failures, 45 identical KeyErrors.
  **Attribute before debugging** — a 1:1 match between failure count and one
  error signature means the harness, not the code.
- **`cargo test --workspace` cannot finish locally.** penca-api's test binary
  links datafusion/lance; the test-profile compile is >20 min and never fits,
  even with retries. Instead run `cargo test -p <crate> --lib` for the
  datafusion-free crates (penca-core / db / storage-meta compile fast) and lean
  on CI's Rust job (no time limit, ~5 min) for the full workspace.
  `cargo check` / `clippy --workspace` DO fit (~5 min warm) and cover all
  non-test code.
- **Killing a gate/wrapper script does NOT kill `just integration-test`** — it
  detaches and keeps running. The survivor then races the next run over the
  same `COMPOSE_PROJECT_NAME`, tearing down each other's containers, and
  **every test fails with no Python-level error text** (bare `FFFF...`, no
  `FAILED`/traceback lines, docker build output interleaved with pytest
  output). Cost a 33%-deep run on 2026-07-29 (CHA-531) that read like a real
  regression from doc-only commits. Before blaming code:
  `pgrep -af '[j]ust integration-test'` — **two** PIDs means a race, not a bug.
  Same attribute-before-debugging rule as the `COMPOSE_PROJECT_NAME` case
  above. Fixes: launch the wrapper under `setsid` so the whole process group
  dies together (`kill -- -<pgid>`), and have it refuse to start while another
  suite is alive. Bracket the first char in `pgrep -f` patterns
  (`'[j]ust ...'`) or the check matches its own cmdline — the recurring
  self-match hazard, cf. [[feedback_slow_commands_capture_and_wait]].

## Memory: ~7 GB, and an OOM kill silently moves the port

A large `just perf-test`-style load can **OOM-kill a penca container** mid-run —
e.g. `penca-lifecycle-1` exiting `137` while snapshotting 10M rows to Lance,
surfacing client-side as `failed to connect to all addresses … Connection
refused`. Check `docker ps -a | grep penca` for `Exited (137)`.

**Do not recover with `docker start <container>`** — Docker assigns it a **new
random host port** (the original ephemeral binding was released on stop) while
`docker/.client.env` still points at the old one, so it stays "connection
refused." Correct recovery is `just penca-down` then `just penca-up`: recreates
every container, regenerates `.client.env` with the actual ports, and frees the
partial-load data. The cached backend image makes this ~2 min, not a rebuild.
Empirically the wall sits between pgbench `scale 10` (1M/500k, fine) and
`scale 100` (10M/5M, OOMs the lifecycle snapshot).

## Disk: 50 GB root, and the prune you reach for matters

Order of safe reclaim: drop your own artifacts first, then build cache, then ask.

1. **`rm -rf target/debug/incremental` / `target/release`** — your own
   rebuildable artifacts, 25–48 GB, touches nothing shared. Always first.
   `target/debug` alone is ~30 GB of regenerable check output.
2. **`docker builder prune -f`** (no `-a`) — safe and fast; reclaimed 8.4 GB of
   *unused* cache in seconds when the root disk hit 100% mid-`cargo test`. It
   prunes only unused cache, leaves tagged images alone, and does **not** force
   a cargo rebuild (build cache is orthogonal to `target/`). But it is the
   **wrong lever before a Docker build** — it deletes the cargo-chef dependency
   layer `docker/Dockerfile.rust-server` relies on, so the next
   `just integration-test` pays a ~90-minute from-scratch dep rebuild. Reserve
   it for when a *host-side* `cargo` / `just check` build is disk-blocked.
3. **`docker image prune -a -f`** (unused tagged images, ~8.7 GB) — classified
   as a shared-VM destructive action; get explicit user authorization.

**Never `docker builder prune -af`.** The `-a` form invokes
`docker-buildx buildx prune -af` and can hang indefinitely — observed stuck 45+
min without exiting, after it had already cleared the cache to 0B. Chained
before a build (`prune -af && … > LOG`) the whole command stalls on the prune:
the build never starts and the redirect LOG is never even created, which looks
exactly like a silently-failed run. If one is already hung: `TaskStop` the task,
`pkill -f 'buildx prune'`, confirm `docker system df` shows the cache gone, then
relaunch the build alone.

**`just perf-test --profile`** builds the DWARF `profiling` image (~12 GB
unpacked vs ~1 GB release). A plain `penca-up --build` lets compose build the
same image once per service → five parallel layer unpacks race into separate
containerd snapshots and transiently multiply disk several-fold (`no space left
on device` twice on a 74 GB VM). Pre-build once, then skip:

```bash
CARGO_PROFILE=profiling docker compose -f docker/compose.yml --env-file docker/test.env \
  --profile infra --profile penca-backend build query
PENCA_SKIP_BUILD=1 just perf-test --profile <paths>
```

Note the big profiling build can GC-evict the *release* chef layer, so the next
integration-test pays a full dep rebuild.

Related: [[feedback_slow_commands_capture_and_wait]],
[[feedback_integration_suite_full_fresh_before_pr]].
