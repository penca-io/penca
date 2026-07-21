"""Static checks for the label-driven epic-tracker generator.

The generator lives at ``scripts/epic_tracker.py`` — a committed script, not a
package on ``sys.path`` — so the tests load it by path. Following
feedback_dont_test_upstream_libs, they pin only Penca-owned logic over canned
Linear-issue dicts (never the network): the intra-set ``blocks`` DAG
reconstruction (incl. relation/inverse-relation de-dup and dropping edges that
leave the labelled set), the ``--group-by-label`` / ``--group-by`` grouping
fallback, and that a hostile issue title cannot break out of the HTML. No
fixtures, no Linear API — runs under ``just static-test epic_tracker``.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

GENERATOR = Path(__file__).parents[2] / "scripts/epic_tracker.py"


def _load_generator():
    spec = importlib.util.spec_from_file_location("epic_tracker", GENERATOR)
    assert spec is not None
    assert spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


et = _load_generator()


def _issue(
    ident,
    *,
    state=("Backlog", "backlog"),
    project="Query Engine",
    parent=None,
    labels=(),
    pr=None,
    blocks=(),
    blocked_by=(),
):
    """Build one raw Linear-issue dict in the generator's GraphQL shape."""
    return {
        "identifier": ident,
        "title": f"title {ident}",
        "priority": 2,
        "url": f"https://linear.app/x/issue/{ident}",
        "state": {"name": state[0], "type": state[1]},
        "project": {"name": project} if project else None,
        "parent": {"identifier": parent} if parent else None,
        "assignee": None,
        "labels": {"nodes": [{"name": n} for n in labels]},
        "attachments": {"nodes": [{"url": pr}] if pr else []},
        "relations": {
            "nodes": [
                {"type": "blocks", "relatedIssue": {"identifier": b}} for b in blocks
            ]
        },
        "inverseRelations": {
            "nodes": [
                {"type": "blocks", "issue": {"identifier": b}} for b in blocked_by
            ]
        },
    }


def test_blocks_dag_intra_set_and_deduped():
    # 455 blocks 480 (stated from BOTH sides → must de-dup to one edge);
    # 480 blocks 481 and 999 (999 is outside the labelled set → dropped).
    raw = [
        _issue(
            "CHA-480",
            labels=["epic-axis:cold"],
            blocks=["CHA-481", "CHA-999"],
            blocked_by=["CHA-455"],
        ),
        _issue("CHA-455", state=("Done", "completed"), blocks=["CHA-480"]),
        _issue(
            "CHA-481",
            state=("In Review", "started"),
            project="Lifecycle Engine",
            parent="CHA-463",
        ),
    ]
    issues = et.build_graph(raw)

    assert [i.id for i in issues] == [
        "CHA-455",
        "CHA-480",
        "CHA-481",
    ]  # sorted by number
    by = {i.id: i for i in issues}
    assert by["CHA-455"].blocks == ["CHA-480"]
    assert by["CHA-480"].blocked_by == [
        "CHA-455"
    ]  # de-duped, not ["CHA-455","CHA-455"]
    assert by["CHA-480"].blocks == ["CHA-481"]  # CHA-999 dropped (outside set)
    assert by["CHA-481"].blocked_by == ["CHA-480"]  # captured via inverseRelations


def test_state_class_and_pr_extraction():
    done = et.Issue(
        _issue(
            "CHA-1", state=("Done", "completed"), pr="https://github.com/o/r/pull/270"
        )
    )
    review = et.Issue(_issue("CHA-2", state=("In Review", "started")))
    progress = et.Issue(_issue("CHA-3", state=("In Progress", "started")))
    backlog = et.Issue(_issue("CHA-4", state=("Backlog", "backlog")))
    canceled = et.Issue(_issue("CHA-5", state=("Canceled", "canceled")))
    # `duplicate` is a distinct Linear state type; it must land in the red
    # `cancel` bucket, not fall through to the todo default (regression: CHA-508
    # rendered as an active backlog node).
    duplicate = et.Issue(_issue("CHA-6", state=("Duplicate", "duplicate")))

    assert (
        done.css(),
        review.css(),
        progress.css(),
        backlog.css(),
        canceled.css(),
        duplicate.css(),
    ) == (
        "done",
        "wip",
        "prog",
        "todo",
        "cancel",
        "cancel",
    )
    assert done.pr_url.endswith("/pull/270")
    assert backlog.pr_url == ""


def test_group_key_label_prefix_with_groupby_fallback():
    tagged = et.Issue(_issue("CHA-1", labels=["cold", "epic-axis:tx-seq-num"]))
    untagged = et.Issue(_issue("CHA-2", project="Lifecycle Engine"))

    # prefix match wins
    assert et.group_key(tagged, "project", "epic-axis") == "tx-seq-num"
    # no matching sub-label → honour --group-by project ...
    assert et.group_key(untagged, "project", "epic-axis") == "Lifecycle Engine"
    # ... and --group-by none (the bug the review caught: must not hard-pin project)
    assert et.group_key(untagged, "none", "epic-axis") == ""
    # no prefix at all → plain --group-by behaviour
    assert et.group_key(untagged, "project", None) == "Lifecycle Engine"
    assert et.group_key(untagged, "none", None) == ""


def test_hostile_title_cannot_break_out_of_html():
    raw = [_issue("CHA-1")]
    raw[0]["title"] = "<script>EVILTITLE</script>"
    out = et.render(et.build_graph(raw), "epic:x", "T", "project", None)

    assert "EVILTITLE" in out  # not silently dropped
    assert (
        "<script>EVILTITLE" not in out
    )  # never emitted raw (cards escape, mermaid strips <>)
    assert "&lt;script&gt;EVILTITLE" in out  # card path escaped it


def test_render_mermaid_edges_subgraphs_and_parent():
    raw = [
        _issue("CHA-463", project="Query Engine"),
        _issue("CHA-480", project="Query Engine", parent="CHA-463", blocks=["CHA-481"]),
        _issue("CHA-481", project="Lifecycle Engine", state=("In Review", "started")),
    ]
    g = et.render_mermaid(et.build_graph(raw), "project", None)

    assert "graph TD" in g
    assert "classDef done" in g
    assert '["Lifecycle Engine"]' in g and '["Query Engine"]' in g  # project subgraphs
    assert "CHA480 --> CHA481" in g  # solid blocks edge
    assert "CHA463 -.-> CHA480" in g  # dashed parent (sub-issue) edge, intra-set


def test_href_scheme_gating():
    hostile = [_issue("CHA-1", pr="javascript:alert(1)")]
    hostile[0]["url"] = "javascript:alert(2)"
    out = et.render(et.build_graph(hostile), "epic:x", "T", "project", None)
    assert 'href="javascript:' not in out  # neither issue url nor pr emitted as a link
    assert "CHA-1" in out  # id still rendered (as plain text)

    safe = [_issue("CHA-2", pr="https://github.com/o/r/pull/9")]
    out2 = et.render(et.build_graph(safe), "epic:x", "T", "project", None)
    assert 'href="https://github.com/o/r/pull/9"' in out2  # http(s) PR link kept


def test_render_cards_one_section_per_state_no_mislabel():
    # Backlog + Todo share the `todo` css class — must not collapse into one
    # section headed by an arbitrary member's state_name.
    raw = [
        _issue("CHA-1", state=("Backlog", "backlog")),
        _issue("CHA-2", state=("Todo", "unstarted")),
        _issue("CHA-3", state=("Done", "completed")),
        _issue("CHA-4", state=("In Review", "started")),
        _issue("CHA-5", state=("In Progress", "started")),
    ]
    cards = et.render_cards(et.build_graph(raw))

    for name in ("Done", "In Review", "In Progress", "Backlog", "Todo"):
        assert f'<h2>{name} <span class="count">1</span>' in cards

    # best-first ordering by css rank: Done < In Review < In Progress < {Backlog,Todo}
    order = [cards.index(f"<h2>{n} ") for n in ("Done", "In Review", "In Progress")]
    assert order == sorted(order)


def test_render_mermaid_suppresses_doubled_parent_block_edge():
    # Umbrella parent that ALSO blocks its child: emit the solid blocks edge only,
    # never a second dashed parent edge over the same pair.
    raw = [
        _issue("CHA-463"),
        _issue("CHA-480", parent="CHA-463", blocked_by=["CHA-463"]),
    ]
    g = et.render_mermaid(et.build_graph(raw), "project", None)

    assert "CHA463 --> CHA480" in g  # solid blocks edge kept
    assert "CHA463 -.-> CHA480" not in g  # dashed parent edge suppressed (no double)


def test_fetch_issues_paginates(monkeypatch):
    pages = [
        {
            "pageInfo": {"hasNextPage": True, "endCursor": "C1"},
            "nodes": [{"identifier": "CHA-1"}],
        },
        {
            "pageInfo": {"hasNextPage": False, "endCursor": None},
            "nodes": [{"identifier": "CHA-2"}],
        },
    ]
    seen_after: list = []
    feed = iter(pages)

    class _Resp:
        def __init__(self, payload: bytes):
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self) -> bytes:
            return self._payload

    def fake_urlopen(request):
        seen_after.append(json.loads(request.data)["variables"]["after"])
        return _Resp(json.dumps({"data": {"issues": next(feed)}}).encode())

    monkeypatch.setattr(et.urllib.request, "urlopen", fake_urlopen)
    nodes = et.fetch_issues("KEY", "epic:x")

    assert [n["identifier"] for n in nodes] == [
        "CHA-1",
        "CHA-2",
    ]  # both pages accumulated
    assert seen_after == [None, "C1"]  # page 1 no cursor, page 2 threads endCursor
