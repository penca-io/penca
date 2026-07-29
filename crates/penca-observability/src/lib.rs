//! Shared `tracing` subscriber init for Penca binaries.
//!
//! The filter directive is owned by the deployment environment, not by
//! this crate — `docker/compose.yml` sets `RUST_LOG` on every servicer
//! and operators / `docker/test.env` override per environment.
//! [`init_tracing`] reads that env var via
//! `EnvFilter::from_default_env`; when `RUST_LOG` is unset the filter
//! falls back to ERROR-only, surfacing the misconfiguration loudly
//! rather than papering over it with an in-code default that has to
//! stay in sync with compose.
//!
//! The crate exists to be the single chokepoint for tracing init across
//! every Penca binary — future format / layer / OTLP work changes
//! [`init_tracing`] once instead of touching seven `main.rs` files.

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

/// Install the global `tracing_subscriber::fmt` subscriber with the
/// `RUST_LOG`-derived `EnvFilter`. Call exactly once per binary at the
/// top of `main`.
pub fn init_tracing() {
    // When `PENCA_SPAN_TIMING` is set non-empty, emit a
    // span-close event carrying `time.busy` / `time.idle` for every
    // enabled span. Off by default (no per-span output, zero overhead);
    // opt-in for latency debugging — combine with a `…=trace` filter to
    // decompose a read into per-round-trip phases. Owned here so it stays
    // the one tracing chokepoint rather than a per-binary flag.
    //
    // Empty counts as off: `docker/compose.yml` injects
    // `PENCA_SPAN_TIMING: "${PENCA_SPAN_TIMING:-}"`, so the var is
    // always *present* (defaulting to "") in the container — a bare
    // `is_some()` would leave timing always-on and flood every
    // debug-level span on the hot path.
    let span_events = if std::env::var_os("PENCA_SPAN_TIMING").is_some_and(|v| !v.is_empty()) {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(span_events)
        .init();
}
