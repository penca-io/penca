#!/usr/bin/env python3
"""Per-window span attribution from a PENCA_SPAN_TIMING log.

Companion to ``span_trace_table.py`` for suite runs that exercise several
tables sequentially (e.g. the gRPC OLTP tiers, CHA-417): where the totals
table aggregates the whole log, this groups span-close lines into time
windows derived from the ``table=<uuid>`` span field — pass 1 finds each
read-heavy table's [first_ts, last_ts] window and merges overlapping
windows (a tier references multiple uuids); pass 2 assigns every close
line (including table-less root spans like ``ipc_encode``) to the
enclosing window and prints per-window mean busy/idle per span name plus
per-request span counts, normalized by ``ipc_encode`` root closes.

Heuristics (documented, tune per suite): a window's anchor tables need
>= 200 close lines; the last three merged windows are labeled with the
OLTP tier run order. Zero-dependency Python 3; reads a file path, or
stdin when no path is given.
"""

import io
import re
import sys
from collections import defaultdict

from spanlog import parse_close

TABLE = re.compile(r"table=([0-9a-f-]{36})")


def main(path: str | None) -> None:
    lines = []
    table_window = {}
    table_count = defaultdict(int)
    # Both input modes decode with errors="replace" — span logs can carry
    # invalid UTF-8 and the stdin path must not be stricter than the file
    # path.
    src = (
        open(path, errors="replace")
        if path
        else io.TextIOWrapper(sys.stdin.buffer, errors="replace")
    )
    for raw in src:
        parsed = parse_close(raw)
        if parsed is None:
            continue

        ts = parsed["ts"]
        t = TABLE.search(parsed["chain"])
        table = t.group(1) if t else None
        if table:
            table_count[table] += 1
            lo, hi = table_window.get(table, (ts, ts))
            table_window[table] = (min(lo, ts), max(hi, ts))

        lines.append((ts, parsed["names"], parsed["busy_ms"], parsed["idle_ms"]))

    # OLTP tier tables: the three read-heavy tables, in run order
    # (all_hot -> all_cold_snapshotted -> hot_and_cold_mixed). OLAP tables
    # see only a handful of scans; tier tables see ~100 reads x ~3 plans.
    candidates = sorted(
        (t for t, n in table_count.items() if n >= 200),
        key=lambda t: table_window[t][0],
    )
    # Tables sharing a time window belong to the same tier (table uuid +
    # name uuid both appear in span fields); merge overlapping windows.
    windows = []
    for t in candidates:
        lo, hi = table_window[t]
        if windows and lo <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], hi))
        else:
            windows.append((lo, hi))

    print("read-heavy tables (run order):")
    for t in candidates:
        print(
            f"  {t[:8]}…  closes={table_count[t]}  window={table_window[t][0]}..{table_window[t][1]}"
        )

    print(f"merged windows: {len(windows)}")

    # The OLTP tier names are sound only when the run produced the expected
    # three read-heavy windows (run order all_hot -> all_cold_snapshotted ->
    # hot_and_cold_mixed, after any setup windows). Anything else gets
    # neutral labels rather than confidently wrong tier names.
    if len(windows) >= 3:
        if len(windows) > 3:
            print(
                f"note: found {len(windows)} read-heavy windows; labeling the "
                "last 3 as OLTP tiers, assuming earlier windows are setup"
            )

        labels = ["all_hot", "all_cold_snapshotted", "hot_and_cold_mixed"]
        selected = windows[-3:]
    else:
        print(
            f"warning: expected 3 read-heavy windows, found {len(windows)} — "
            "using neutral window labels"
        )
        labels = [f"window_{i + 1}" for i in range(len(windows))]
        selected = windows

    for tier, (lo, hi) in zip(labels, selected, strict=True):
        window_lines = [x for x in lines if lo <= x[0] <= hi]
        per_span = defaultdict(lambda: [0, 0.0, 0.0])
        n_req = 0
        for _, names, busy, idle in window_lines:
            if not names:
                continue

            inner = names[-1]
            agg = per_span[inner]
            agg[0] += 1
            agg[1] += busy
            agg[2] += idle
            if inner == "ipc_encode" and names[0] == "ipc_encode" and len(names) == 1:
                n_req += 1

        if not n_req:
            print(
                f"note: {tier} window {lo}..{hi}: no ipc_encode root closes — "
                "skipping (not a gRPC-suite query log?)"
            )
            continue

        print(f"\n=== {tier}  window={lo}..{hi}  requests={n_req} ===")
        print(f"{'span':<34}{'n/req':>7}{'busy ms/req':>13}{'idle ms/req':>13}")
        rows = []
        for name, (n, busy, idle) in per_span.items():
            rows.append(
                (
                    (busy + idle) / n_req,
                    name,
                    n / n_req,
                    busy / n_req,
                    idle / n_req,
                )
            )

        for _, name, npr, busy, idle in sorted(rows, reverse=True)[:18]:
            print(f"{name:<34}{npr:>7.1f}{busy:>13.2f}{idle:>13.2f}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
