# OLAP (gRPC) Performance

A dead-simple, **stable** operational yardstick for the throughput-oriented
analytical path, measured over the **native gRPC API** (`ReadData`) —
deliberately **not** Flight SQL. Kept separate from the [OLTP suite](oltp.md)
because the two have opposite cost structures. Part of the gRPC-first
operational suite (CHA-416).

**Scans and filters only — no aggregates.** The native `ReadData` path does
not aggregate server-side, so `count`/`sum`/`avg`/`GROUP BY` are an explicit
later **Flight SQL** pass; this suite measures scan + merge-on-read
throughput, not aggregation.

Two operations on the steady-state `all_cold_snapshotted` tier, run at two
scales (100k and 1M rows) so the throughput crossover is visible in one table:

- **`olap_full_scan`** — a full-table scan via `read_data` (no filter).
- **`olap_filtered_scan`** — a bulk filtered scan via
  `read_data(filter="value < <threshold>")` (gRPC predicate pushdown), with
  the threshold chosen to return half the table.

The Postgres baseline runs the same scans over a direct `psycopg` connection.

> **Intentional overlap.** The 100k full scan also appears — among all eight
> tiers — in the Query suite's cross-*tier* read benchmark
> (`performance_query_test.py::test_read_data`). Here it is the baseline for
> the 100k → 1M cross-*scale* story, so the single-cell overlap is by design:
> the two suites measure different dimensions.

Run via `just perf-test grpc/olap_test.py`. Absolute numbers are not committed here
(host-dependent) — each run captures to `.perf/results.jsonl`, emits an HTML
report (`.perf/report-<run_id>.html`) comparing the run against history, and
with `--record` persists to the SQLite store; view trends with `just
perf-trends` / `just perf-dashboard`. See the main
[performance notes](../../../docs/performance.md#recording--viewing-results).

Penca's columnar/vectorized scan over pre-deduplicated cold-snapshotted Lance
segments keeps full-scan throughput close to the Postgres baseline and makes
the pushed-down filtered scan markedly faster, since the predicate trims rows
before they cross the wire. The crossover sharpens with scale: fixed per-query
overhead amortizes as the row count grows from 100k to 1M.
