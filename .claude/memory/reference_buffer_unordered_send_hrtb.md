---
name: reference_buffer_unordered_send_hrtb
description: "Bounded-concurrency cold reads in lifecycle/RPC paths — use chunked try_join_all, not buffer_unordered (Send-not-general-enough HRTB)"
metadata: 
  node_type: memory
  type: reference
  originSessionId: c6325651-8258-4ecf-9297-7a78cc42e7e6
---

For bounded-concurrency cold reads inside a future that must be `Send` for all
lifetimes — anything reaching a `#[tracing::instrument]`-wrapped gRPC handler
(e.g. the Snapshot op in `penca-api/src/lifecycle/snapshot_op.rs`) — **use
chunked `futures_util::future::try_join_all` over `candidates.chunks(n)`, NOT
`StreamExt::buffer_unordered(n)`**.

`buffer_unordered` (and `buffered`) over `stream::iter(...).map(async move {…})`
that borrows generic params (`&HashMap<i32, R: FormatReader>`, `&self`'s
`L`/`W`/`PgDriver`) trips `error: implementation of std::marker::Send is not
general enough` — reported at the outermost instrumented handler ("Send would
have to be implemented for `&L`/`&PgDriver`/… but is implemented for `&'0 L`
for some specific lifetime"). The futures ARE Send (all captures are Sync); it's
a known rustc HRTB limitation of the buffer_unordered Stream combinator.
**Neither a named async fn nor `Box::pin(... as Pin<Box<dyn Future + Send>>)`
fixes it.** `try_join_all` per chunk is a Join future (no Stream combinator) and
infers Send fine, while still bounding concurrency for the memory cap.

Iterate the fix with `cargo check -p penca-server-grpc` (the binary lib where
the error surfaces) — `cargo check -p penca-api` (the lib alone) does NOT catch
it, only the docker build or a binary-crate check does.

CHA-448 hit this folding a perf review finding (parallelize reverse-lookup
sidecar probes). See [[feedback_clippy_not_in_cargo_check.md]] for the related
"lib check ≠ full check" theme.
