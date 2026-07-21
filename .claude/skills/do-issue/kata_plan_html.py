#!/usr/bin/env python3
"""Render a kata task graph (one `cha-NNN` slug) to a single-file HTML plan.

The output is one self-contained `.html` file, but the dependency graph is
drawn by Mermaid loaded from a CDN at view time — it needs network when opened
(an inline-rendered variant is a tracked follow-up). The rendered graph is
pan/zoom-navigable (scroll to zoom, drag to pan, Fit button) via a small inline
vanilla-JS handler — no extra CDN dependency. Pure-stdlib, deterministic:
same kata state in -> byte-identical HTML out
(no timestamps, no randomness, every collection sorted). Reads kata via its
JSON CLI, classifies each task into the /do-issue layer model
(red-test -> impl -> orch:*), reconstructs the blocked-by DAG from the link
edges, and emits a Mermaid graph + per-task cards.

Usage:
    kata_plan_html.py <cha-slug> [-o OUT.html]
    kata_plan_html.py cha-92 -o plan.html

Intended caller: /do-issue Step 3, which uploads the result to Linear and
links it from a comment on the ticket.

I/O is isolated to `run_kata` / `fetch_payloads`; `build_graph` and `render`
are pure functions over the raw kata JSON so the static tests can drive them
with canned payloads (see tests/static/static_kata_plan_html_test.py).
"""

from __future__ import annotations

import argparse
import html
import json
import subprocess
import sys
from typing import Any

# --- layer model ------------------------------------------------------------

ORCH_ORDER = ["orch:run-cleanup", "orch:open-pr", "orch:spawn-review"]
ORCH_SHORT = {
    "orch:run-cleanup": "run-cleanup",
    "orch:open-pr": "open-PR",
    "orch:spawn-review": "spawn-review",
}
KIND_RED, KIND_IMPL, KIND_ORCH, KIND_OTHER = "red", "impl", "orch", "other"
LAYER_RANK = {KIND_RED: 0, KIND_IMPL: 1, KIND_ORCH: 2, KIND_OTHER: 3}


# --- kata access ------------------------------------------------------------


def run_kata(args: list[str]) -> Any:
    proc = subprocess.run(
        ["kata", *args, "--json"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"kata {' '.join(args)} failed: {proc.stderr.strip()}")

    return json.loads(proc.stdout)


def label_names(labels: list[Any]) -> list[str]:
    """kata list yields labels as strings; kata show as {label: ...} dicts.

    `build_graph` only ever feeds it the dict form (from `kata show`); the
    bare-string branch is defensive so a future caller can pass `kata list`
    labels through the same normalization. Both shapes are pinned in
    tests/static/static_kata_plan_html_test.py::TestLabelNames.
    """
    out = []
    for ln in labels or []:
        out.append(ln if isinstance(ln, str) else ln.get("label", ""))

    return out


def classify(labels: list[str]) -> str:
    if "red-test" in labels:
        return KIND_RED

    if "impl" in labels:
        return KIND_IMPL

    if any(ln.startswith("orch:") for ln in labels):
        return KIND_ORCH

    return KIND_OTHER


def orch_label(labels: list[str]) -> str:
    for ln in labels:
        if ln.startswith("orch:"):
            return ln

    return ""


# --- model ------------------------------------------------------------------


class Task:
    def __init__(self, qid: str, short: str, title: str, body: str, labels: list[str]):
        self.qid = qid
        self.short = short
        self.title = title
        self.body = body
        self.labels = labels
        self.kind = classify(labels)
        self.orch = orch_label(labels)
        self.code = ""  # assigned after sorting (R1, I2, ...)
        self.blocked_by: list[str] = []
        self.blocks: list[str] = []


def sort_key(t: Task) -> tuple:
    if t.kind == KIND_ORCH:
        sub = ORCH_ORDER.index(t.orch) if t.orch in ORCH_ORDER else 99
    else:
        sub = 0

    return (LAYER_RANK[t.kind], sub, t.qid)


def build_graph(list_payload: dict, show_payloads: list[dict]) -> list[Task]:
    """Reconstruct the layered, code-assigned task list from raw kata JSON.

    Pure: no subprocess. `list_payload` is a `kata list --json` result (the
    only place `qualified_id` lives); `show_payloads` is one `kata show --json`
    result per task (title/body/labels/links). Edges come from links whose
    `type == "blocks"`; only intra-set edges are recorded.
    """
    qid_by_short = {
        i["short_id"]: i["qualified_id"] for i in list_payload.get("issues", [])
    }

    tasks: dict[str, Task] = {}
    edges: set[tuple[str, str]] = set()
    for data in show_payloads:
        issue = data["issue"]
        short = issue.get("short_id") or ""
        qid = qid_by_short.get(short, f"penca#{short}")
        labels = label_names(data.get("labels", []))
        tasks[short] = Task(
            qid, short, issue.get("title", ""), issue.get("body", "") or "", labels
        )
        for ln in data.get("links", []):
            if ln.get("type") == "blocks":
                edges.add((ln["from"]["short_id"], ln["to"]["short_id"]))

    # keep only intra-set edges; record both directions on the nodes
    for src, dst in sorted(edges):
        if src in tasks and dst in tasks:
            tasks[dst].blocked_by.append(src)
            tasks[src].blocks.append(dst)

    ordered = sorted(tasks.values(), key=sort_key)
    rc = ic = fc = 0
    for t in ordered:
        if t.kind == KIND_RED:
            rc += 1
            t.code = f"R{rc}"
        elif t.kind == KIND_IMPL:
            ic += 1
            t.code = f"I{ic}"
        elif t.kind == KIND_ORCH:
            t.code = ORCH_SHORT.get(t.orch, t.orch or "orch")
        else:  # KIND_OTHER — late-arriving findings (roborev / review-pr / ...)
            fc += 1
            t.code = f"F{fc}"

    return ordered


def fetch_payloads(slug: str) -> tuple[dict, list[dict]]:
    """Shell to kata: the listing for `slug` plus one show payload per task."""
    listing = run_kata(["list", "--label", slug])
    qids = sorted(i["qualified_id"] for i in listing.get("issues", []))
    if not qids:
        sys.exit(f"no kata tasks under label {slug}")

    return listing, [run_kata(["show", qid]) for qid in qids]


# --- text helpers -----------------------------------------------------------


def strip_prefix(title: str) -> str:
    t = title
    for marker in (": ", " — ", " - "):
        # drop a leading "CHA-92 red-test:" / "CHA-92 impl:" style prefix once
        if t.lower().startswith("cha-") and marker in t:
            head, _, tail = t.partition(marker)
            if len(head) <= 40:
                t = tail

            break

    return t.strip()


def summarize(body: str, limit: int = 170) -> str:
    for raw in body.splitlines():
        line = raw.strip().lstrip("#-*> ").strip()
        if line:
            if len(line) > limit:
                cut = line[:limit].rsplit(" ", 1)[0]
                return cut + "…"

            return line

    return ""


def mermaid_text(s: str, limit: int = 46) -> str:
    """Plain, mermaid-safe label fragment (no quotes/brackets/pipes)."""
    s = strip_prefix(s)
    for bad in '"[]{}()|<>`':
        s = s.replace(bad, "")

    s = " ".join(s.split())
    if len(s) > limit:
        s = s[:limit].rsplit(" ", 1)[0] + "…"

    # ampersand-escape for parity with the e()-routed card path; the <b>/<br/>
    # label markup is added by the caller (render_mermaid), not here
    return s.replace("&", "&amp;")


def e(s: str) -> str:
    return html.escape(s, quote=True)


# --- rendering --------------------------------------------------------------

MERMAID_SUBGRAPHS = [
    (KIND_RED, "RED", "Red-tests"),
    (KIND_IMPL, "IMPL", "Implementation"),
    (KIND_ORCH, "ORCH", "Orchestration"),
    # no "&" in the subgraph title: it is injected raw into the Mermaid source
    # (unlike the e()-routed card heading and the escaped legend)
    (KIND_OTHER, "OTHER", "Findings / other"),
]


def render_mermaid(tasks: list[Task]) -> str:
    lines = [
        "flowchart LR",
        "  classDef red   fill:#2a1820,stroke:#f0617a,stroke-width:1.4px,color:#ffd9e0;",
        "  classDef impl  fill:#15212f,stroke:#5aa9ff,stroke-width:1.4px,color:#d6ecff;",
        "  classDef orch  fill:#1c1f28,stroke:#8b93a3,stroke-width:1.4px,color:#e6e9ef;",
        "  classDef other fill:#221d16,stroke:#d9b06a,stroke-width:1.4px,color:#f2e4c9;",
    ]
    by_kind = {k: [t for t in tasks if t.kind == k] for k, _, _ in MERMAID_SUBGRAPHS}
    for kind, gid, gtitle in MERMAID_SUBGRAPHS:
        group = by_kind[kind]
        if not group:
            continue

        lines.append(f"  subgraph {gid} [{gtitle}]")
        lines.append("    direction TB")
        for t in group:
            label = f"{t.code} · {t.short}<br/><b>{mermaid_text(t.title)}</b>"
            lines.append(f'    n{t.short}["{label}"]:::{t.kind}')

        lines.append("  end")

    for t in tasks:
        for dst in t.blocks:
            lines.append(f"  n{t.short} --> n{dst}")

    return "\n".join(lines)


CARD_SECTIONS = [
    (KIND_RED, "red", "Red-tests — acceptance, fail-first"),
    (KIND_IMPL, "impl", "Implementation — each blocked-by its red-test"),
    (KIND_ORCH, "orch", "Orchestration — autonomous drain"),
    (KIND_OTHER, "other", "Findings & other — late-arriving drain work"),
]


def render_cards(tasks: list[Task], by_short: dict[str, Task]) -> str:
    out = []
    for kind, cls, heading in CARD_SECTIONS:
        group = [t for t in tasks if t.kind == kind]
        if not group:
            continue

        out.append(f"  <h2>{e(heading)}</h2>")
        out.append('  <div class="grid">')
        for t in group:
            badge = {
                "red": "RED-TEST",
                "impl": "IMPL",
                "orch": "ORCH",
                "other": "FINDING",
            }[cls]
            bb = ", ".join(by_short[s].code for s in t.blocked_by if s in by_short)
            bl = ", ".join(by_short[s].code for s in t.blocks if s in by_short)
            dep = []
            if bb:
                dep.append(f"blocked-by {e(bb)}")

            if bl:
                dep.append(f"blocks {e(bl)}")

            dep_line = f'<span class="dep">{" · ".join(dep)}</span>' if dep else ""
            out.append(
                f"""    <div class="task {cls}">
      <div class="top"><span class="badge {cls}">{badge}</span>\
<span class="ref">{e(t.qid)} · {e(t.code)}</span></div>
      <h3>{e(strip_prefix(t.title))}</h3>
      <p>{e(summarize(t.body))}</p>
      <div class="meta">{dep_line}</div>
    </div>"""
            )

        out.append("  </div>")

    return "\n".join(out)


CSS = """
  :root{--bg:#0f1117;--panel:#171a23;--panel2:#1d212c;--ink:#e6e9ef;--muted:#9aa3b2;
    --line:#2a2f3a;--red:#f0617a;--blue:#5aa9ff;--gray:#8b93a3;--amber:#d9b06a;--accent:#7ee0c0}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);
    font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
  .wrap{max-width:1100px;margin:0 auto;padding:32px 24px 80px}
  header h1{margin:0 0 6px;font-size:23px}
  header .sub{color:var(--muted);font-size:14px}
  .pills{margin:14px 0 4px;display:flex;flex-wrap:wrap;gap:8px}
  .pill{font-size:12px;padding:4px 10px;border:1px solid var(--line);border-radius:999px;color:var(--muted)}
  .pill b{color:var(--ink);font-weight:600}
  h2{font-size:13px;text-transform:uppercase;letter-spacing:.12em;color:var(--muted);
    margin:38px 0 14px;border-bottom:1px solid var(--line);padding-bottom:8px}
  .card-graph{position:relative;background:var(--panel);border:1px solid var(--line);border-radius:14px;
    height:min(72vh,660px);overflow:hidden;cursor:grab;touch-action:none}
  .card-graph.grabbing{cursor:grabbing}
  .card-graph .mermaid{position:absolute;top:0;left:0;margin:0;transform-origin:0 0;display:block;will-change:transform}
  .card-graph .mermaid svg{max-width:none!important;display:block}
  .zoom-controls{position:absolute;top:10px;right:10px;z-index:5;display:flex;gap:6px}
  .zoom-controls button{width:30px;height:30px;font-size:16px;line-height:1;cursor:pointer;color:var(--ink);
    background:var(--panel2);border:1px solid var(--line);border-radius:8px;padding:0;font-family:inherit;
    display:inline-flex;align-items:center;justify-content:center}
  .zoom-controls button:hover{border-color:var(--accent);color:var(--accent)}
  .zoom-controls .fit{width:auto;padding:0 10px;font-size:12px}
  .zoom-hint{position:absolute;left:12px;bottom:9px;z-index:5;font-size:11px;color:var(--muted);
    background:rgba(15,17,23,.66);padding:2px 8px;border-radius:6px;pointer-events:none}
  .legend{display:flex;gap:18px;flex-wrap:wrap;margin:14px 2px 0;font-size:13px;color:var(--muted)}
  .legend span{display:inline-flex;align-items:center;gap:7px}
  .dot{width:11px;height:11px;border-radius:3px;display:inline-block}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(330px,1fr));gap:14px}
  .task{background:var(--panel);border:1px solid var(--line);border-left-width:3px;border-radius:12px;padding:14px 16px}
  .task.red{border-left-color:var(--red)} .task.impl{border-left-color:var(--blue)} .task.orch{border-left-color:var(--gray)}
  .task.other{border-left-color:var(--amber)}
  .task .top{display:flex;align-items:center;gap:9px;margin-bottom:7px;flex-wrap:wrap}
  .badge{font-size:11px;font-weight:700;padding:2px 8px;border-radius:6px}
  .badge.red{color:var(--red);background:#2a1820} .badge.impl{color:var(--blue);background:#15212f}
  .badge.orch{color:var(--gray);background:#1c1f28} .badge.other{color:var(--amber);background:#221d16}
  .ref{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:var(--muted)}
  .task h3{margin:0 0 7px;font-size:15px;line-height:1.35}
  .task p{margin:0 0 9px;color:#c6ccd8;font-size:13.5px}
  .meta{font-size:12px;color:var(--muted)}
  .dep{font-family:ui-monospace,Menlo,monospace;font-size:11.5px;color:var(--muted)}
  footer{margin-top:40px;color:var(--muted);font-size:12px;text-align:center}
"""

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{slug} · Plan</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>{css}</style></head>
<body><div class="wrap">
  <header>
    <h1>{slug} · implementation plan</h1>
    <div class="sub">kata task graph driving the <code>/do-issue</code> drain loop.</div>
    <div class="pills">
      <span class="pill"><b>{n_total}</b> tasks</span>
      <span class="pill"><b>{n_red}</b> red-tests</span>
      <span class="pill"><b>{n_impl}</b> impl</span>
      <span class="pill"><b>{n_orch}</b> orchestration</span>
      <span class="pill"><b>{n_other}</b> findings</span>
    </div>
  </header>
  <h2>Dependency graph (blocked-by DAG)</h2>
  <div class="card-graph" id="graph">
    <div class="zoom-controls">
      <button type="button" id="zoom-out" title="Zoom out" aria-label="Zoom out">&minus;</button>
      <button type="button" id="zoom-in" title="Zoom in" aria-label="Zoom in">+</button>
      <button type="button" class="fit" id="zoom-fit" title="Fit graph to view">Fit</button>
    </div>
    <pre class="mermaid">
{mermaid}
    </pre>
    <div class="zoom-hint">scroll to zoom · drag to pan</div>
  </div>
  <div class="legend">
    <span><i class="dot" style="background:#f0617a"></i> red-test</span>
    <span><i class="dot" style="background:#5aa9ff"></i> implementation</span>
    <span><i class="dot" style="background:#8b93a3"></i> orchestration</span>
    <span><i class="dot" style="background:#d9b06a"></i> findings &amp; other</span>
    <span>arrow = <b style="color:var(--ink)">blocks</b></span>
  </div>
{cards}
  <footer>Generated deterministically from kata · {slug}</footer>
</div>
<script>
mermaid.initialize({{startOnLoad:false,theme:'dark',securityLevel:'loose',
  flowchart:{{curve:'basis',nodeSpacing:38,rankSpacing:70}}}});
mermaid.run({{querySelector:'.mermaid'}}).then(function(){{
  var vp=document.getElementById('graph');
  var layer=vp.querySelector('.mermaid');
  var svg=layer&&layer.querySelector('svg');
  if(!svg){{return;}}
  // Pin the svg to its intrinsic (viewBox) px size; the layer transform owns
  // all scaling so zoom/pan math stays exact regardless of the mermaid defaults.
  var vb=svg.viewBox&&svg.viewBox.baseVal;
  var box=svg.getBoundingClientRect();
  var natW=(vb&&vb.width)?vb.width:box.width||1;
  var natH=(vb&&vb.height)?vb.height:box.height||1;
  svg.style.maxWidth='none';svg.style.width=natW+'px';svg.style.height=natH+'px';
  var scale=1,tx=0,ty=0,panning=false,startX=0,startY=0;
  var MIN=0.15,MAX=8;
  function clamp(v){{return v<MIN?MIN:(v>MAX?MAX:v);}}
  function apply(){{layer.style.transform='translate('+tx+'px,'+ty+'px) scale('+scale+')';}}
  function zoomAt(px,py,factor){{
    var ns=clamp(scale*factor);if(ns===scale){{return;}}
    var k=ns/scale;tx=px-(px-tx)*k;ty=py-(py-ty)*k;scale=ns;apply();
  }}
  function fit(){{
    var vw=vp.clientWidth,vh=vp.clientHeight;
    scale=clamp(Math.min(vw/natW,vh/natH,1));
    tx=(vw-natW*scale)/2;ty=(vh-natH*scale)/2;apply();
  }}
  vp.addEventListener('wheel',function(e){{
    e.preventDefault();
    var r=vp.getBoundingClientRect();
    zoomAt(e.clientX-r.left,e.clientY-r.top,e.deltaY<0?1.12:1/1.12);
  }},{{passive:false}});
  vp.addEventListener('mousedown',function(e){{
    panning=true;startX=e.clientX-tx;startY=e.clientY-ty;vp.classList.add('grabbing');e.preventDefault();
  }});
  window.addEventListener('mousemove',function(e){{
    if(!panning){{return;}}tx=e.clientX-startX;ty=e.clientY-startY;apply();
  }});
  window.addEventListener('mouseup',function(){{panning=false;vp.classList.remove('grabbing');}});
  function btnZoom(factor){{zoomAt(vp.clientWidth/2,vp.clientHeight/2,factor);}}
  document.getElementById('zoom-in').addEventListener('click',function(){{btnZoom(1.25);}});
  document.getElementById('zoom-out').addEventListener('click',function(){{btnZoom(1/1.25);}});
  document.getElementById('zoom-fit').addEventListener('click',fit);
  fit();
}});
</script>
</body></html>
"""


def render(slug: str, tasks: list[Task]) -> str:
    by_short = {t.short: t for t in tasks}
    counts = {k: sum(1 for t in tasks if t.kind == k) for k in LAYER_RANK}
    return PAGE.format(
        slug=e(slug.upper()),
        css=CSS,
        n_total=len(tasks),
        n_red=counts[KIND_RED],
        n_impl=counts[KIND_IMPL],
        n_orch=counts[KIND_ORCH],
        n_other=counts[KIND_OTHER],
        mermaid=render_mermaid(tasks),
        cards=render_cards(tasks, by_short),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Render a kata cha-NNN graph to HTML.")
    ap.add_argument("slug", help="kata label, e.g. cha-92")
    ap.add_argument("-o", "--out", help="output path (default: <slug>_plan.html)")
    args = ap.parse_args()
    listing, show_payloads = fetch_payloads(args.slug)
    tasks = build_graph(listing, show_payloads)
    out_path = args.out or f"{args.slug}_plan.html"
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(render(args.slug, tasks))

    print(f"wrote {out_path} ({len(tasks)} tasks)")


if __name__ == "__main__":
    main()
