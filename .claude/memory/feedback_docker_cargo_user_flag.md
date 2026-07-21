---
name: feedback-docker-cargo-user-flag
description: When running `cargo` via `docker run` with a bind-mounted worktree, pass `--user $(id -u):$(id -g)` so build artifacts aren't root-owned and can be cleaned up by `just worktree-remove`
metadata:
  type: feedback
---

When this machine lacks a local Rust toolchain and I reach for
`docker run --rm -v "$PWD":/work -w /work rust:<ver>-slim ...` as a
stand-in for `cargo`, pass `--user $(id -u):$(id -g)` (and
`-e CARGO_HOME=/tmp/.cargo` so the non-root user has a writable cargo
metadata dir).

**Why:** Caught on CHA-243. Without `--user`, the container runs as
root by default. The bind-mount of `$PWD` → `/work` makes cargo write
`target/` straight back onto the host filesystem as root-owned. At
worktree-remove time, `just worktree-remove` (and `git worktree
remove` under it) try to `rm -rf` those files as the regular user and
fail with "Permission denied". Recovery required `sudo rm -rf` of the
worktree's target dir, leaving an awkward orphan-shell state until
cleanup finished.

The compose stack used by `just integration-test` doesn't have this
problem — `docker compose --build` runs under BuildKit isolation, so
image-layer writes don't touch the host filesystem. The issue is
specific to my ad-hoc bind-mount pattern.

**How to apply:** Any time I'm reaching for `docker run ... rust:<ver>
... cargo ...` in a worktree, the invocation needs `--user
$(id -u):$(id -g) -e CARGO_HOME=/tmp/.cargo`. Example:

```bash
docker run --rm \
  --user $(id -u):$(id -g) \
  -e CARGO_HOME=/tmp/.cargo \
  -v "$PWD":/work -w /work \
  rust:1.94-slim-bookworm \
  sh -c "apt-get update -qq && apt-get install -y -qq protobuf-compiler libprotobuf-dev pkg-config libssl-dev >/dev/null 2>&1 && cargo test ..."
```

Don't push this into `Justfile` — most Penca devs run `cargo`
natively via `rust-toolchain.toml`, and adding a docker-cargo recipe
would suggest the project endorses it. Workflow-specific to this
machine.
