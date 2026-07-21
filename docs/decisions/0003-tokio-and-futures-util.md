# 0003 — Keep both `tokio` and `futures-util`; prefer `tokio::try_join!` inside tokio-hosted code

- **Status:** Accepted
- **Date:** 2026-04-21
- **Ticket:** [CHA-144](https://linear.app/chapala/issue/CHA-144)

## Context

Rust async code has two overlapping ecosystems: the runtime-agnostic
`futures` crate family (typically pulled in as `futures-util` for the
combinators) and tokio's runtime-specific primitives in the `tokio`
crate. Both ship `join!`, `try_join!`, `select!`, and stream helpers.
The workspace today depends on both:

- `tokio` — every crate that hosts async work (`penca-api`,
  `penca-db`, `penca-merge`, `penca-storage-*`, `penca-dl`,
  `penca-server-grpc`, `penca-sql-server`, `penca-format`,
  `penca-datafusion`, `penca-storage-meta`) depends on it, typically
  with `features = ["macros", "rt"]`. Tokio owns: the runtime
  (`#[tokio::main]`, `spawn`, `spawn_blocking`), cooperative
  scheduling, timers, and the runtime-aware concurrency macros
  (`tokio::join!`, `tokio::try_join!`, `tokio::select!`).
- `futures-util` — seven import sites across
  `penca-storage-hot`, `penca-db`, `penca-api`, `penca-merge`,
  and tests. Used for the `Stream` / `TryStream` combinator traits
  (`StreamExt::next`, `TryStreamExt::try_next`, `TryStreamExt::try_collect`)
  and `futures_util::stream::empty()`.

When CHA-144 added a `try_join!` call to `penca-merge`, the question
came up: which crate's macro? And if we pick tokio's, could we drop
`futures-util` entirely?

## Decision

**Keep both crates. They cover distinct concerns.** Prefer the
tokio-native primitive whenever both exist, but reach for
`futures-util` for `Stream` / `TryStream` combinators.

Concretely:

| Need | Use |
| -- | -- |
| `join!` / `try_join!` / `select!` | `tokio::join!` / `tokio::try_join!` / `tokio::select!` |
| Spawning tasks, timers, channels | `tokio::{spawn, time, sync}` |
| `.next()`, `.try_next()` on a `Stream` | `futures_util::{StreamExt, TryStreamExt}` |
| `.try_collect()`, `.try_fold()` | `futures_util::TryStreamExt` |
| `stream::empty()`, `stream::iter(...)` | `futures_util::stream` |

## Rationale

### Why `tokio::try_join!` over `futures_util::try_join!`

Both macros expand to the same shape — poll every branch on the
current task in a fixed order per wake-up, short-circuit on the first
`Err`. They do not spawn. The difference is integration with the
runtime the code is running on.

`tokio::try_join!` participates in tokio's **cooperative scheduling
budget**: every branch poll counts against the current task's budget
(default 128 units), and when exhausted the macro yields `Pending` to
the runtime to let other tasks on the same worker make progress. In a
gRPC server where many concurrent `read_data` calls share the same
tokio worker pool, this keeps one hot `merge_read` from starving its
peers.

`futures_util::try_join!` has no knowledge of the runtime. Its branches
do not consume tokio's budget and do not participate in cooperative
yielding. That's exactly what you want in a runtime-agnostic library
and exactly what you don't want in a tokio-hosted hot path.

None of the penca crates are runtime-agnostic: sqlx, tonic,
`async-stream`, and the Arrow IPC path all transitively pin tokio. So
there is no portability cost to reaching for the tokio-native macro.

### Why we can't drop `futures-util`

`tokio_stream::StreamExt` replaces `futures_util::StreamExt` for
`.next()` and `.try_next()` — a clean substitution. But tokio ships
**no `TryStreamExt` analogue**. `try_collect` / `try_fold` /
`try_for_each` only exist in `futures-util`. Migrating off them would
mean hand-rolling `while let Some(x) = s.try_next().await? { …
}` loops at every call site (`penca-api/src/lifecycle.rs` alone has
three of these). That's a verbosity regression for no gain.

`futures-util` is also a no-runtime, lean dependency — it compiles to
trait and combinator machinery, not background threads or schedulers.
Keeping it alongside `tokio` is the common pattern in tokio-hosted
crates and is not a symptom of dependency sprawl.

### When the two overlap

For anything that exists in both (e.g., `stream::empty()`, `.next()`
on a `Stream`), prefer the tokio-native version when you're *already*
in a tokio runtime context, and prefer `futures-util` when you're
writing pure-type-level utility code that doesn't otherwise need
tokio. In practice, almost every penca call site sits on a tokio
runtime, so `tokio_stream` usage is reasonable; today's code happens
to use `futures_util::stream::empty()` because the surrounding
combinators already pulled `StreamExt` in — it would be consistent
either way. Don't churn existing usages just to line them up with
this rule.

## Trigger conditions to revisit

1. **tokio adds a `TryStreamExt` analogue** (or absorbs `tokio_stream`
   into the main crate with one). At that point the reason to keep
   `futures-util` disappears and a follow-up migration ticket is
   worthwhile.
2. **A crate that must be runtime-agnostic** (e.g., a shared utility
   exposed to non-tokio consumers) appears in the workspace. Such a
   crate should *not* depend on `tokio` for concurrency macros —
   `futures_util::try_join!` is correct there.
3. **Cooperative scheduling stops mattering** for a given hot path
   (e.g., the code is moved to its own runtime, or wrapped in
   `spawn_blocking`). The choice of macro becomes a stylistic one at
   that point; stay consistent with the surrounding file.

## Related

- [CHA-144](https://linear.app/chapala/issue/CHA-144) — the ticket
  whose implementation triggered writing this down.
- `tokio` cooperative scheduling:
  <https://tokio.rs/blog/2020-04-preemption> (background; not
  authoritative API reference).
