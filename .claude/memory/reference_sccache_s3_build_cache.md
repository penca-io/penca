---
name: reference_sccache_s3_build_cache
description: "sccache→S3 build cache setup, its stale startup-error replay gotcha, and what invalidates cache keys"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 00166a15-baf2-4849-88f2-84b2c08812a4
---

Rust builds go through sccache (v0.15, `rustc-wrapper` in `~/.cargo/config.toml`) backed by S3 bucket **`fabric-sccache`** (us-west-1, 180-day object expiry). **Still `fabric-sccache` after the CHA-520 `fabric`→`penca` rename** — verified 2026-07-27 in `~/.config/sccache/config`. CHA-520 listed the bucket as in scope but it was not renamed, and it should not be "fixed" casually: renaming it forks every cache key and forces a cold rebuild across all VMs. Installed/configured by `just init-build-tools` (dependency of `init-agent-tools`, reached by `just bootstrap`). Verified 2026-06-11: clean full-workspace build = ~120 s at 100% hit rate vs 1 h+ cold.

Gotchas:

- **Stale error replay**: sccache caches its first server-startup error and replays it whenever a later server auto-start fails — a persistent `Failed to load config file ... missing field no_credentials` can be a ghost of a long-fixed config. Fix: `sccache --stop-server`, then retry. Servers auto-started inside sandboxed Bash commands die when the sandbox tears down, making this more frequent in agent sessions.
- **Cache keys** include rustflags and compiler version: never add per-VM rustflags (forks the keyspace); a `rust-toolchain.toml` bump invalidates everything — first build after is a full re-seed (slow once, then fast for all VMs).
- **Enabling/disabling the wrapper does not recompile**: cargo excludes RUSTC_WRAPPER from fingerprints, so seeding/refreshing the cache requires `cargo clean` first.
- No custom linker on purpose: rust 1.94 already defaults to rust-lld on x86_64-linux (mold was evaluated and dropped — its `-B` shim is silently ignored because rustc's own `-fuse-ld=lld` wins).
