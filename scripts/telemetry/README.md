# scripts/telemetry

Offline summarizers for Penca performance telemetry. All are zero-dependency
Python 3 (stdlib only) and read from a file or stdin. They were written during
the CHA-365 Flight SQL metadata-amplification investigation to turn raw span
logs and CPU profiles into something you can read without a UI.

| script | input | answers |
|---|---|---|
| `span_trace_table.py` | `tracing` span-close logs (`PENCA_SPAN_TIMING=1`) | where wall time goes per span, and **how many times** each span fires (fan-out / amplification) |
| `span_window_table.py` | the same span-close logs, for suite runs over several tables | per-window (e.g. per-OLTP-tier) mean busy/idle per span + per-request span counts, windows derived from the `table=` span field |
| `samply_top.py` | a samply / Firefox-profiler JSON | top functions by self time and inclusive time |

`spanlog.py` is the shared span-close parser both span tables import (not a
runnable script) — wire-format changes go there, never per-table.

## span_trace_table.py — span-timing decomposition

Pairs with the `PENCA_SPAN_TIMING` knob in `penca-observability::init_tracing`.
When that env var is set (non-empty), tracing is configured with `FmtSpan::CLOSE`,
so every *enabled* span emits a `close time.busy=.. time.idle=..` event when it
closes. Enable it on the servicer you want to inspect, give `RUST_LOG` a verbose
enough filter that the spans you care about are on, run the workload, then feed
the logs in.

```bash
# 1. turn span timing on for a servicer (debug-only; e.g. a compose override):
#      PENCA_SPAN_TIMING=1
#      RUST_LOG=info,penca=debug,penca_merge=trace,penca_storage_meta=trace

# 2. run the workload (one query, a load loop, etc.), then summarize:
docker logs "${COMPOSE_PROJECT_NAME}-query-1" 2>&1 \
  | python3 scripts/telemetry/span_trace_table.py --prefix 2026-05-31T18:33:0 --totals

# or from a captured logfile:
python3 scripts/telemetry/span_trace_table.py query.log --prefix 2026-05-31T18:33: --totals
```

- `--prefix` keeps only lines whose timestamp `startswith` the given string —
  use it to isolate a single request/second instead of the whole container log.
- `--totals` adds a per-span aggregate sorted by **count**.

Reading the output: `busy`/`idle` are tracing's own accounting. `busy` is time
the span's future was being polled — note that for a **synchronous** span that
does `block_on`, a blocking RPC is charged to `busy`, not `idle`, so a span can
look CPU-bound while it is actually waiting on I/O. Crucially, `busy` is **not**
exclusive of children, so don't sum it across nesting depths. The reliable
amplification signal is the **`n=` count** column: it's how CHA-365 found a
single one-row `SELECT` firing `resolve_schema_metadata` 44× and
`resolve_table_metadata` 30× per query.

## samply_top.py — CPU profile top functions

[`samply`](https://github.com/mstange/samply) writes the Firefox Profiler
"processed profile" JSON (optionally gzipped). This reads it without the web UI.

```bash
# capture: samply needs kernel.perf_event_paranoid <= 1
sudo sysctl kernel.perf_event_paranoid=1
samply record --save-only -o /tmp/query-profile.json -p <PID>     # attach to a running process
# ...or wrap a command:  samply record -o /tmp/query-profile.json -- <cmd> [args]

# summarize:
python3 scripts/telemetry/samply_top.py /tmp/query-profile.json --top 30
python3 scripts/telemetry/samply_top.py /tmp/query-profile.json --grep merge
```

- `--top N` caps the number of rows (default 30).
- `--grep STR` shows only functions whose name contains `STR` (case-insensitive).

Output is two columns: `self%` (samples landing in that function as the stack
leaf) and `incl%` (samples anywhere in the function's subtree). The loader is
gzip-aware and tolerates the format drift between samply versions
(`stringArray` / `stringTable` / `shared.stringArray`).
