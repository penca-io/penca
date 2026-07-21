#!/usr/bin/env python3
"""Decompose Penca `tracing` span-close timings into a per-span table.

Pairs with the `PENCA_SPAN_TIMING=1` knob (penca-observability `init_tracing`,
CHA-353): when set, every enabled span emits a `close time.busy=.. time.idle=..`
event. Run a servicer with `PENCA_SPAN_TIMING=1` and a `…=trace` RUST_LOG, then
feed its logs here to see where wall time goes per span.

Usage:
    # straight from a container, just the 18:33:0x window, with totals:
    docker logs penca-fabric-query-1 2>&1 \
        | python3 scripts/telemetry/span_trace_table.py --prefix 2026-05-31T18:33:0 --totals

    # from a captured logfile:
    python3 scripts/telemetry/span_trace_table.py query.log --prefix 2026-05-31T18:33: --totals

`busy`/`idle` are tracing's own accounting: `busy` = time the span's future was
being polled (for a *synchronous* span that does `block_on`, a blocking RPC is
charged here, NOT to idle); `idle` = time open but awaiting. Counts (the `n=`
column under --totals) are the reliable signal for fan-out/amplification — span
`busy` is NOT exclusive of children, so don't sum it across nesting depths.
"""

import argparse
import io
import sys

from spanlog import parse_close


def parse(lines, prefix):
    for line in lines:
        parsed = parse_close(line)
        if parsed is None:
            continue

        if prefix and not parsed["ts"].startswith(prefix):
            continue

        names = parsed["names"]
        chain = parsed["chain"]
        yield {
            "time": parsed["ts"].split("T")[-1][:12],
            "depth": len(names),
            "outer": names[0] if names else chain,
            "inner": names[-1] if names else chain,
            "target": parsed["target"].split("::")[-1],
            "busy": parsed["busy_ms"],
            "idle": parsed["idle_ms"],
        }


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("logfile", nargs="?", help="log file; omit to read stdin")
    ap.add_argument(
        "--prefix", default="", help="keep only lines whose timestamp startswith this"
    )
    ap.add_argument(
        "--totals",
        action="store_true",
        help="also print per-span aggregates (count + busy/idle)",
    )
    args = ap.parse_args()

    # errors="replace" on both modes, matching span_window_table — the two
    # tables must tolerate the same bytes or cross-checking them skews.
    src = (
        open(args.logfile, errors="replace")
        if args.logfile
        else io.TextIOWrapper(sys.stdin.buffer, errors="replace")
    )
    rows = list(parse(src, args.prefix))
    if not rows:
        print(
            "(no span-close events matched — is PENCA_SPAN_TIMING=1 set on that servicer?)",
            file=sys.stderr,
        )
        return

    print(
        f"{'time':<13}{'d':<2}{'outer->inner':<46}{'tgt':<14}{'busy_ms':>9}{'idle_ms':>9}"
    )
    for r in rows:
        label = (
            r["outer"] if r["outer"] == r["inner"] else f"{r['outer']}->{r['inner']}"
        )
        print(
            f"{r['time']:<13}{r['depth']:<2}{label:<46}{r['target']:<14}{r['busy']:>9.2f}{r['idle']:>9.2f}"
        )

    if args.totals:
        agg = {}
        for r in rows:
            a = agg.setdefault(r["inner"], [0.0, 0.0, 0])
            a[0] += r["busy"]
            a[1] += r["idle"]
            a[2] += 1

        print("\n=== totals by innermost span (sorted by count) ===")
        for name, (busy, idle, n) in sorted(agg.items(), key=lambda x: -x[1][2]):
            print(f"{name:<32} n={n:<4} busy={busy:8.2f}ms idle={idle:8.2f}ms")


if __name__ == "__main__":
    main()
