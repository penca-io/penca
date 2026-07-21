#!/usr/bin/env python3
"""Summarize a samply / Firefox-profiler JSON into top functions by self time.

samply writes the Firefox Profiler "processed profile" format (optionally
gzipped). This extracts per-function self-sample counts (leaf of stack) and
inclusive counts, so a CPU profile can be read without the web UI.

Usage:
    python3 scripts/telemetry/samply_top.py /tmp/query-profile.json [--top 30] [--grep merge]
"""

import argparse
import gzip
import json
import sys
from collections import defaultdict


def load(path):
    with open(path, "rb") as f:
        head = f.read(2)

    opener = gzip.open if head == b"\x1f\x8b" else open
    with opener(path, "rb") as f:
        return json.loads(f.read())


def strings_for(profile, thread):
    # format drift: per-thread stringArray/stringTable, or shared.stringArray
    for src in (
        thread.get("stringArray"),
        thread.get("stringTable"),
        profile.get("shared", {}).get("stringArray"),
    ):
        if src is not None:
            return src

    return []


def frame_names_for(thread, strings):
    """Resolve every frameTable entry to its function-name string, once.

    A stackTable node maps to a frame (`stackTable.frame`); a frame maps to a
    func (`frameTable.func`); a func maps to a name (`funcTable.name`, itself an
    index into the string table). Precomputing the frame->name list once per
    thread avoids re-walking that chain on every sample.
    """
    frame_func = thread.get("frameTable", {}).get("func", [])
    func_name = thread.get("funcTable", {}).get("name", [])
    out = []
    for ff in frame_func:
        s = func_name[ff]
        out.append(strings[s] if isinstance(s, int) and s < len(strings) else str(s))

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("profile")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument(
        "--grep", default="", help="only show functions whose name contains this"
    )
    args = ap.parse_args()

    profile = load(args.profile)
    threads = profile.get("threads", [])
    self_w = defaultdict(float)
    incl_w = defaultdict(float)
    total = 0.0

    for th in threads:
        strings = strings_for(profile, th)
        frame_names = frame_names_for(th, strings)
        st, sm = th.get("stackTable", {}), th.get("samples", {})
        stack_prefix = st.get("prefix", [])
        stack_frame = st.get("frame", [])
        stacks = sm.get("stack", [])
        weights = sm.get("weight") or [1] * len(stacks)

        for i, leaf in enumerate(stacks):
            if leaf is None:
                continue

            w = weights[i] if i < len(weights) else 1
            total += w
            self_w[frame_names[stack_frame[leaf]]] += w
            seen, node = set(), leaf
            while node is not None:
                nm = frame_names[stack_frame[node]]
                if nm not in seen:
                    seen.add(nm)
                    incl_w[nm] += w

                node = stack_prefix[node]

    if total == 0:
        print("no samples found (empty profile?)", file=sys.stderr)
        return

    rows = sorted(self_w.items(), key=lambda x: -x[1])
    if args.grep:
        rows = [r for r in rows if args.grep.lower() in r[0].lower()]

    print(f"total samples: {total:.0f}   threads: {len(threads)}\n")
    print(f"{'self%':>6} {'incl%':>6}  function")
    for name, w in rows[: args.top]:
        print(f"{100 * w / total:6.1f} {100 * incl_w[name] / total:6.1f}  {name[:96]}")


if __name__ == "__main__":
    main()
