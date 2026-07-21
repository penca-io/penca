#!/usr/bin/env python3
"""Render every Linear issue carrying one label into a single-file epic tracker.

Ad-hoc, label-driven sibling of `roadmap.py` (the Linear GraphQL client) and
`.claude/skills/do-issue/kata_plan_html.py` (the pan/zoom Mermaid + status-card
HTML). Tag every issue in an epic with one label, then:

    just epic-tracker "epic:cold-oltp"
    scripts/epic_tracker.py "epic:cold-oltp" -o epics-tracker.html

and you get an always-current structural view — nodes = the labelled issues,
edges = the real Linear `blocks` relations between them, colour = workflow
state, grouped into subgraphs by project. `--label` is the only required
argument, so the same script serves any future epic.

What it deliberately does NOT reproduce from a hand-curated tracker doc: the
editorial cross-epic narrative, the "①②③④" conceptual grouping (Linear has no
such field — group by project, or introduce `epic-axis:*` sub-labels and pass
`--group-by-label epic-axis`), and prose on the dependency edges. This is the
structural snapshot; the narrative stays in the Linear document.

Requires LINEAR_API_KEY (personal API key: https://linear.app/settings/api).

Pure-ish: `fetch_issues` is the only I/O; `build_graph` / `render` are pure
functions over the raw issue dicts so they can be unit-tested with canned data.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import html
import json
import os
import sys
import urllib.error
import urllib.request

LINEAR_API_URL = "https://api.linear.app/graphql"

# One paginated query: the labelled issues plus the `blocks` relations (both
# directions) and parent links needed to reconstruct the intra-epic DAG.
ISSUES_QUERY = """
query($label: String!, $after: String) {
  issues(filter: { labels: { name: { eq: $label } } }, first: 100, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      identifier
      title
      priority
      url
      state { name type }
      project { name }
      parent { identifier }
      assignee { name }
      labels { nodes { name } }
      attachments { nodes { url } }
      relations { nodes { type relatedIssue { identifier } } }
      inverseRelations { nodes { type issue { identifier } } }
    }
  }
}
"""

PRIORITY_LABELS = {0: "", 1: "Urgent", 2: "High", 3: "Medium", 4: "Low"}

# state.type -> (css class, status emoji). `started` is refined by state.name
# below so "In Review" and "In Progress" read differently. `duplicate` is a
# distinct Linear state type (separate from `canceled`); both are terminal/dead,
# so a duplicate shares the red `cancel` styling — otherwise it falls through to
# the todo default and reads as an active backlog node (e.g. CHA-508).
STATE_CLASS = {
    "completed": ("done", "✅"),
    "started": ("wip", "🔵"),
    "unstarted": ("todo", "◻️"),
    "backlog": ("todo", "◻️"),
    "canceled": ("cancel", "✖️"),
    "duplicate": ("cancel", "✖️"),
    "triage": ("todo", "◻️"),
}
# Ordering of the card sections, best-first — keyed by css class so "In Review"
# (wip) and "In Progress" (prog) land in their own sections (matching the graph),
# rather than collapsing into one `started` bucket under a single heading.
CSS_ORDER = ["done", "wip", "prog", "todo", "cancel"]


# --- Linear access ----------------------------------------------------------


def fetch_issues(api_key: str, label: str) -> list[dict]:
    """All issues carrying `label`, following pagination."""
    nodes: list[dict] = []
    after: str | None = None
    while True:
        payload = json.dumps(
            {"query": ISSUES_QUERY, "variables": {"label": label, "after": after}}
        ).encode()
        request = urllib.request.Request(
            LINEAR_API_URL,
            data=payload,
            headers={"Authorization": api_key, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request) as response:
                data = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            sys.exit(f"Linear API HTTP {exc.code}: {exc.read().decode()}")

        if "errors" in data:
            sys.exit(f"Linear API error: {data['errors']}")

        conn = data["data"]["issues"]
        nodes.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            return nodes

        after = conn["pageInfo"]["endCursor"]


# --- model ------------------------------------------------------------------


class Issue:
    def __init__(self, raw: dict):
        self.id: str = raw["identifier"]
        self.title: str = raw["title"]
        self.url: str = raw["url"]
        self.priority: int = raw.get("priority") or 0
        state = raw.get("state") or {}
        self.state_name: str = state.get("name", "")
        self.state_type: str = state.get("type", "backlog")
        project = raw.get("project")
        self.project: str = project["name"] if project else "(no project)"
        assignee = raw.get("assignee")
        self.assignee: str = assignee["name"] if assignee else ""
        parent = raw.get("parent")
        self.parent: str = parent["identifier"] if parent else ""
        self.labels: list[str] = [
            la["name"] for la in raw.get("labels", {}).get("nodes", [])
        ]
        self.pr_url: str = _first_pr(raw.get("attachments", {}).get("nodes", []))
        self.blocks: list[str] = []
        self.blocked_by: list[str] = []
        self._raw = raw

    def css(self) -> str:
        cls, _ = STATE_CLASS.get(self.state_type, ("todo", "◻️"))
        # split `started` into review (blue) vs in-progress (amber) by name
        if self.state_type == "started" and "review" not in self.state_name.lower():
            return "prog"

        return cls

    def emoji(self) -> str:
        _, emoji = STATE_CLASS.get(self.state_type, ("todo", "◻️"))
        return emoji

    def num(self) -> int:
        tail = self.id.rsplit("-", 1)[-1]
        return int(tail) if tail.isdigit() else 0


def _first_pr(attachments: list[dict]) -> str:
    for att in attachments:
        url = att.get("url", "")
        if "/pull/" in url or "/review/" in url:
            return url

    return ""


def build_graph(raw_issues: list[dict]) -> list[Issue]:
    """Issues + their intra-set `blocks` edges, reconstructed from relations.

    Pure. Edge `(a, b)` means *a blocks b*. Both `relations` (outgoing) and
    `inverseRelations` (incoming) are consulted so a `blocks` link is captured
    even when Linear only returns it from one side; only edges whose endpoints
    are both in the labelled set are kept.
    """
    issues = {raw["identifier"]: Issue(raw) for raw in raw_issues}
    edges: set[tuple[str, str]] = set()
    for iss in issues.values():
        for rel in iss._raw.get("relations", {}).get("nodes", []):
            if rel.get("type") == "blocks" and rel.get("relatedIssue"):
                edges.add((iss.id, rel["relatedIssue"]["identifier"]))

        for rel in iss._raw.get("inverseRelations", {}).get("nodes", []):
            if rel.get("type") == "blocks" and rel.get("issue"):
                edges.add((rel["issue"]["identifier"], iss.id))

    for blocker, blocked in sorted(edges):
        if blocker in issues and blocked in issues:
            issues[blocker].blocks.append(blocked)
            issues[blocked].blocked_by.append(blocker)

    return sorted(issues.values(), key=lambda i: i.num())


# --- text helpers -----------------------------------------------------------


def e(s: str) -> str:
    return html.escape(s, quote=True)


def safe_href(url: str) -> str:
    """Only http(s) URLs are emitted into href= (drops javascript:/data: etc.).

    Linear data is trusted-ish, but hrefs deserve the same hardening the title
    path already gets — a non-http scheme renders as a no-link instead.
    """
    return url if url.startswith(("http://", "https://")) else ""


def mermaid_text(s: str, limit: int = 52) -> str:
    """Mermaid-safe node-label fragment (no quotes/brackets/pipes)."""
    for bad in '"[]{}()|<>`':
        s = s.replace(bad, "")

    s = " ".join(s.split())
    if len(s) > limit:
        s = s[:limit].rsplit(" ", 1)[0] + "…"

    return s.replace("&", "&amp;")


def node_id(identifier: str) -> str:
    return identifier.replace("-", "")


# --- rendering --------------------------------------------------------------

# css class -> (mermaid fill, stroke, text)
MERMAID_STYLE = {
    "done": ("#d3f9d8", "#2f9e44", "#000"),
    "wip": ("#e7f5ff", "#1c7ed6", "#000"),  # in review
    "prog": ("#fff3bf", "#f08c00", "#000"),  # in progress
    "todo": ("#e9ecef", "#868e96", "#000"),
    "cancel": ("#ffe3e3", "#e03131", "#000"),
}


def group_key(iss: Issue, group_by: str, group_label: str | None) -> str:
    """The subgraph this issue belongs to (empty string = no subgraph).

    A `--group-by-label PREFIX` recovers the conceptual "①②③④" epic grouping the
    hand-doc had: tag issues `PREFIX:tx-seq-num` etc. and group on the suffix.
    An issue without a matching sub-label falls back to the `--group-by`
    behaviour (so `--group-by none` is still honoured alongside a label prefix).
    """
    if group_label:
        for la in iss.labels:
            if la.startswith(f"{group_label}:"):
                return la.split(":", 1)[1]

    return iss.project if group_by == "project" else ""


def render_mermaid(issues: list[Issue], group_by: str, group_label: str | None) -> str:
    lines = ["graph TD"]
    for cls, (fill, stroke, color) in MERMAID_STYLE.items():
        lines.append(f"  classDef {cls} fill:{fill},stroke:{stroke},color:{color};")

    groups: dict[str, list[Issue]] = {}
    for iss in issues:
        groups.setdefault(group_key(iss, group_by, group_label), []).append(iss)

    for gi, (gname, members) in enumerate(sorted(groups.items())):
        if gname:
            lines.append(f'  subgraph G{gi} ["{mermaid_text(gname, 40)}"]')

        for iss in members:
            label = f"{iss.id} {iss.emoji()}<br/><b>{mermaid_text(iss.title)}</b>"
            lines.append(f'    {node_id(iss.id)}["{label}"]:::{iss.css()}')

        if gname:
            lines.append("  end")

    ids = {iss.id for iss in issues}
    for iss in issues:
        for dst in iss.blocks:
            lines.append(f"  {node_id(iss.id)} --> {node_id(dst)}")

        # umbrella (parent) as a dashed sub-issue edge — intra-set, and only when
        # no `blocks` edge already connects parent→child (else it doubles up: the
        # parent-blocks-child pattern is common for umbrella issues)
        if iss.parent in ids and iss.parent not in iss.blocked_by:
            lines.append(f"  {node_id(iss.parent)} -.-> {node_id(iss.id)}")

    return "\n".join(lines)


def render_cards(issues: list[Issue]) -> str:
    # One section per distinct workflow state, so In Review vs In Progress and
    # Backlog vs Todo vs Triage never collapse under one mislabeled heading (the
    # `todo` css class spans several states). Sections are ordered best-first by
    # the state's css rank, then by name.
    by_state: dict[str, list[Issue]] = {}
    for iss in issues:
        by_state.setdefault(iss.state_name, []).append(iss)

    def rank(name: str) -> tuple[int, str]:
        css = by_state[name][0].css()
        return (CSS_ORDER.index(css) if css in CSS_ORDER else len(CSS_ORDER), name)

    out: list[str] = []
    for name in sorted(by_state, key=rank):
        group = by_state[name]
        heading = name or group[0].css()
        out.append(f'  <h2>{e(heading)} <span class="count">{len(group)}</span></h2>')
        out.append('  <div class="grid">')
        for iss in group:
            deps = []
            if iss.blocked_by:
                deps.append("blocked-by " + e(", ".join(sorted(iss.blocked_by))))

            if iss.blocks:
                deps.append("blocks " + e(", ".join(sorted(iss.blocks))))

            dep_line = f'<div class="dep">{" · ".join(deps)}</div>' if deps else ""
            ref_href = safe_href(iss.url)
            ref = f'<a href="{e(ref_href)}">{e(iss.id)}</a>' if ref_href else e(iss.id)
            pr_href = safe_href(iss.pr_url)
            pr = f' · <a href="{e(pr_href)}">PR</a>' if pr_href else ""
            prio = PRIORITY_LABELS.get(iss.priority, "")
            tags = [t for t in (iss.project, prio, iss.assignee) if t]
            pr_label_tags = [la for la in iss.labels if la]
            out.append(
                f"""    <div class="task {iss.css()}">
      <div class="top"><span class="badge {iss.css()}">{e(iss.state_name)}</span>\
<span class="ref">{ref}{pr}</span></div>
      <h3>{e(iss.title)}</h3>
      <div class="tags">{e(" · ".join(tags))}</div>
      <div class="labels">{e(" ".join(pr_label_tags))}</div>
      {dep_line}
    </div>"""
            )

        out.append("  </div>")

    return "\n".join(out)


CSS = """
  :root{--bg:#0f1117;--panel:#171a23;--panel2:#1d212c;--ink:#e6e9ef;--muted:#9aa3b2;
    --line:#2a2f3a;--done:#51cf66;--rev:#5aa9ff;--prog:#f0b429;--todo:#8b93a3;--cancel:#ff8787;--accent:#7ee0c0}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);
    font:15px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
  .wrap{max-width:1180px;margin:0 auto;padding:32px 24px 80px}
  header h1{margin:0 0 6px;font-size:23px}
  header .sub{color:var(--muted);font-size:14px}
  header .sub code{color:var(--accent)}
  .pills{margin:14px 0 4px;display:flex;flex-wrap:wrap;gap:8px}
  .pill{font-size:12px;padding:4px 10px;border:1px solid var(--line);border-radius:999px;color:var(--muted)}
  .pill b{color:var(--ink);font-weight:600}
  h2{font-size:13px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);
    margin:38px 0 14px;border-bottom:1px solid var(--line);padding-bottom:8px}
  h2 .count{color:var(--ink);font-weight:600}
  .card-graph{position:relative;background:var(--panel);border:1px solid var(--line);border-radius:14px;
    height:min(76vh,720px);overflow:hidden;cursor:grab;touch-action:none}
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
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px}
  .task{background:var(--panel);border:1px solid var(--line);border-left-width:3px;border-radius:12px;padding:13px 15px}
  .task.done{border-left-color:var(--done)} .task.wip{border-left-color:var(--rev)}
  .task.prog{border-left-color:var(--prog)} .task.todo{border-left-color:var(--todo)}
  .task.cancel{border-left-color:var(--cancel)}
  .task .top{display:flex;align-items:center;gap:9px;margin-bottom:7px;flex-wrap:wrap;justify-content:space-between}
  .badge{font-size:11px;font-weight:700;padding:2px 8px;border-radius:6px}
  .badge.done{color:#0f1117;background:var(--done)} .badge.wip{color:#0f1117;background:var(--rev)}
  .badge.prog{color:#0f1117;background:var(--prog)} .badge.todo{color:#0f1117;background:var(--todo)}
  .badge.cancel{color:#0f1117;background:var(--cancel)}
  .ref{font-family:ui-monospace,Menlo,monospace;font-size:12px;color:var(--muted)}
  .ref a{color:var(--muted);text-decoration:none} .ref a:hover{color:var(--accent)}
  .task h3{margin:0 0 7px;font-size:14.5px;line-height:1.35}
  .tags{font-size:12px;color:#c6ccd8;margin-bottom:3px}
  .labels{font-size:11px;color:var(--muted);font-family:ui-monospace,Menlo,monospace}
  .dep{font-family:ui-monospace,Menlo,monospace;font-size:11.5px;color:var(--muted);margin-top:6px}
  footer{margin-top:40px;color:var(--muted);font-size:12px;text-align:center}
  footer code{color:var(--accent)}
"""

PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>{css}</style></head>
<body><div class="wrap">
  <header>
    <h1>{title}</h1>
    <div class="sub">Every Linear issue labelled <code>{label}</code> · edges = <b>blocks</b> relations · \
colour = workflow state · generated {generated}</div>
    <div class="pills">{pills}</div>
  </header>
  <h2>Dependency graph (blocks DAG)</h2>
  <div class="card-graph" id="graph">
    <div class="zoom-controls">
      <button type="button" id="zoom-out" title="Zoom out" aria-label="Zoom out">&minus;</button>
      <button type="button" id="zoom-in" title="Zoom in" aria-label="Zoom in">+</button>
      <button type="button" class="fit" id="zoom-fit" title="Fit graph to view">Fit</button>
    </div>
    <pre class="mermaid">
{mermaid}
    </pre>
    <div class="zoom-hint">scroll to zoom · drag to pan · solid = blocks · dashed = sub-issue</div>
  </div>
  <div class="legend">
    <span><i class="dot" style="background:#51cf66"></i> done</span>
    <span><i class="dot" style="background:#5aa9ff"></i> in review</span>
    <span><i class="dot" style="background:#f0b429"></i> in progress</span>
    <span><i class="dot" style="background:#8b93a3"></i> backlog / todo</span>
    <span><i class="dot" style="background:#ff8787"></i> canceled / duplicate</span>
  </div>
{cards}
  <footer>Generated by <code>scripts/epic_tracker.py</code> from live Linear data · label <code>{label}</code> · {generated}</footer>
</div>
<script>
mermaid.initialize({{startOnLoad:false,theme:'dark',securityLevel:'loose',
  flowchart:{{curve:'basis',nodeSpacing:34,rankSpacing:64}}}});
mermaid.run({{querySelector:'.mermaid'}}).then(function(){{
  var vp=document.getElementById('graph');
  var layer=vp.querySelector('.mermaid');
  var svg=layer&&layer.querySelector('svg');
  if(!svg){{return;}}
  var vb=svg.viewBox&&svg.viewBox.baseVal;
  var box=svg.getBoundingClientRect();
  var natW=(vb&&vb.width)?vb.width:box.width||1;
  var natH=(vb&&vb.height)?vb.height:box.height||1;
  svg.style.maxWidth='none';svg.style.width=natW+'px';svg.style.height=natH+'px';
  var scale=1,tx=0,ty=0,panning=false,startX=0,startY=0;
  var MIN=0.12,MAX=8;
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
  vp.addEventListener('wheel',function(ev){{
    ev.preventDefault();
    var r=vp.getBoundingClientRect();
    zoomAt(ev.clientX-r.left,ev.clientY-r.top,ev.deltaY<0?1.12:1/1.12);
  }},{{passive:false}});
  vp.addEventListener('mousedown',function(ev){{
    panning=true;startX=ev.clientX-tx;startY=ev.clientY-ty;vp.classList.add('grabbing');ev.preventDefault();
  }});
  window.addEventListener('mousemove',function(ev){{
    if(!panning){{return;}}tx=ev.clientX-startX;ty=ev.clientY-startY;apply();
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


def render(
    issues: list[Issue],
    label: str,
    title: str,
    group_by: str,
    group_label: str | None,
) -> str:
    counts: dict[str, int] = {}
    for iss in issues:
        counts[iss.state_name] = counts.get(iss.state_name, 0) + 1

    pills = [f'<span class="pill"><b>{len(issues)}</b> issues</span>']
    for name, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        pills.append(f'<span class="pill"><b>{n}</b> {e(name)}</span>')

    generated = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return PAGE.format(
        title=e(title),
        css=CSS,
        label=e(label),
        generated=generated,
        pills="".join(pills),
        mermaid=render_mermaid(issues, group_by, group_label),
        cards=render_cards(issues),
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Render a Linear epic (one label) to HTML."
    )
    ap.add_argument("label", help='the epic label, e.g. "epic:cold-oltp"')
    ap.add_argument("-o", "--out", default="epics-tracker.html", help="output path")
    ap.add_argument(
        "--title", default=None, help="page title (default: derived from label)"
    )
    ap.add_argument(
        "--group-by",
        choices=["project", "none"],
        default="project",
        help="subgraph grouping (default: project)",
    )
    ap.add_argument(
        "--group-by-label",
        default=None,
        metavar="PREFIX",
        help='group by a label namespace, e.g. "epic-axis" groups on '
        "epic-axis:* sub-labels (recovers the ①②③④ epic split); "
        "falls back to project where absent",
    )
    args = ap.parse_args()

    api_key = os.environ.get("LINEAR_API_KEY", "")
    if not api_key:
        sys.exit(
            "Error: LINEAR_API_KEY not set. Create one at https://linear.app/settings/api"
        )

    raw = fetch_issues(api_key, args.label)
    if not raw:
        sys.exit(f"No issues carry the label {args.label!r}.")

    issues = build_graph(raw)
    title = args.title or f"Epic Tracker — {args.label}"
    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(render(issues, args.label, title, args.group_by, args.group_by_label))

    print(f"wrote {args.out} ({len(issues)} issues)")


if __name__ == "__main__":
    main()
