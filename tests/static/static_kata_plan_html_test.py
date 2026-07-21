"""Static checks for the /do-issue plan-HTML generator (CHA-371).

The generator lives at ``.claude/skills/do-issue/kata_plan_html.py`` — a
committed script, not a package on ``sys.path``. These tests load it by path
and pin the Penca-owned logic only (per feedback_dont_test_upstream_libs): the
layer classifier, the blocked-by DAG reconstruction from kata link JSON, the
byte-identical-output determinism invariant the ticket calls out, and the
HTML/mermaid escaping that keeps a hostile task title from breaking the page.

The pure builder ``build_graph(list_payload, show_payloads)`` takes raw kata
JSON (the ``kata list --json`` listing + a ``kata show --json`` payload per
task) so these tests never shell out to kata. No Docker, no fixtures, no
penca services — runs under ``just static-test kata_plan_html`` and ``just
check``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

GENERATOR = Path(__file__).parents[2] / ".claude/skills/do-issue/kata_plan_html.py"


def _load_generator():
    spec = importlib.util.spec_from_file_location("kata_plan_html", GENERATOR)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # FileNotFoundError here until I1 lands the script
    return mod


kph = _load_generator()


# --- fixtures: canned kata JSON (the shapes verified against live kata) ------


def _list_payload(*shorts: str) -> dict:
    return {
        "kata_api_version": 1,
        "issues": [{"qualified_id": f"fabric#{s}", "short_id": s} for s in shorts],
    }


def _show_payload(
    short: str, title: str, body: str, labels: list[str], links: list[tuple[str, str]]
) -> dict:
    return {
        "kata_api_version": 1,
        "issue": {"short_id": short, "title": title, "body": body},
        "labels": [{"label": ln} for ln in labels],
        "links": [
            {"type": "blocks", "from": {"short_id": a}, "to": {"short_id": b}}
            for a, b in links
        ],
    }


class TestClassify:
    def test_layer_mapping(self):
        assert kph.classify(["red-test"]) == "red"
        assert kph.classify(["impl"]) == "impl"
        assert kph.classify(["orch:open-pr"]) == "orch"
        assert kph.classify(["cha-371"]) == "other"

    def test_precedence_red_over_impl_over_orch(self):
        # A task tagged with several kinds resolves to the highest layer.
        assert kph.classify(["impl", "red-test"]) == "red"
        assert kph.classify(["orch:run-cleanup", "impl"]) == "impl"


class TestLabelNames:
    """Pin the dual-shape normalization the ticket flags as a footgun:
    `kata show --json` yields `{label: ...}` dicts, `kata list --json` yields
    bare strings. Both must normalize to the same names (and thus same layer)."""

    def test_dict_and_bare_string_shapes_normalize_identically(self):
        dict_shape = [{"label": "impl"}, {"label": "cha-371"}]
        bare_shape = ["impl", "cha-371"]
        assert kph.label_names(dict_shape) == ["impl", "cha-371"]
        assert kph.label_names(bare_shape) == ["impl", "cha-371"]
        assert kph.label_names(dict_shape) == kph.label_names(bare_shape)

    def test_classify_agrees_across_shapes(self):
        # the kata-list (bare-string) form classifies to the same layer as the
        # kata-show (dict) form once run through label_names
        for raw, expected in (
            (["red-test"], "red"),
            (["impl"], "impl"),
            (["orch:open-pr"], "orch"),
        ):
            dicts = [{"label": ln} for ln in raw]
            assert kph.classify(kph.label_names(dicts)) == expected
            assert kph.classify(kph.label_names(raw)) == expected

    def test_empty_and_none_labels_are_safe(self):
        assert kph.label_names([]) == []
        assert kph.label_names(None) == []


class TestBuildGraph:
    def _graph(self):
        # one red-test (aaaa) blocking one impl (bbbb)
        link = ("aaaa", "bbbb")
        return kph.build_graph(
            _list_payload("aaaa", "bbbb"),
            [
                _show_payload(
                    "aaaa",
                    "CHA-371 red-test: foo",
                    "first body line\nsecond",
                    ["red-test"],
                    [link],
                ),
                _show_payload(
                    "bbbb", "CHA-371 impl: bar", "impl body", ["impl"], [link]
                ),
            ],
        )

    def test_blocked_by_dag_reconstruction(self):
        by_short = {t.short: t for t in self._graph()}
        # link {from:aaaa, to:bbbb} => bbbb blocked-by aaaa, aaaa blocks bbbb
        assert by_short["bbbb"].blocked_by == ["aaaa"]
        assert by_short["aaaa"].blocks == ["bbbb"]
        assert by_short["aaaa"].blocked_by == []
        assert by_short["bbbb"].blocks == []

    def test_codes_assigned_in_layer_order(self):
        ordered = self._graph()
        # red sorts before impl; codes are R1.. / I1..
        assert [t.code for t in ordered] == ["R1", "I1"]
        assert [t.kind for t in ordered] == ["red", "impl"]

    def test_qid_carried_from_listing(self):
        by_short = {t.short: t for t in self._graph()}
        # qualified_id lives in the listing, not the show payload's issue object
        assert by_short["aaaa"].qid == "fabric#aaaa"

    def test_orch_suborder(self):
        ordered = kph.build_graph(
            _list_payload("op", "rc", "sr"),
            [
                _show_payload("op", "open", "b", ["orch:open-pr"], []),
                _show_payload("rc", "clean", "b", ["orch:run-cleanup"], []),
                _show_payload("sr", "review", "b", ["orch:spawn-review"], []),
            ],
        )
        # run-cleanup < open-pr < spawn-review regardless of input order
        assert [t.short for t in ordered] == ["rc", "op", "sr"]

    def test_only_intra_set_edges_kept(self):
        # a link to a short_id not in the task set is dropped, not crashed on
        ordered = kph.build_graph(
            _list_payload("aaaa"),
            [
                _show_payload(
                    "aaaa", "CHA-371 impl: x", "b", ["impl"], [("aaaa", "ghost")]
                )
            ],
        )
        assert ordered[0].blocks == []

    def test_finding_other_kind_coded_and_after_orch(self):
        # a late-arriving finding (no kind label) classifies as "other", sorts
        # last, and gets an F# code rather than being mislabeled "orch"
        link = ("find", "op")
        ordered = kph.build_graph(
            _list_payload("op", "find"),
            [
                _show_payload("op", "CHA-1 orch: open", "b", ["orch:open-pr"], [link]),
                _show_payload("find", "roborev: something", "b", ["roborev"], [link]),
            ],
        )
        by_short = {t.short: t for t in ordered}
        assert by_short["find"].kind == "other"
        assert by_short["find"].code == "F1"
        assert ordered[-1].short == "find"  # other sorts after orch


class TestRenderSafety:
    def _tasks(self):
        return kph.build_graph(
            _list_payload("aaaa"),
            [_show_payload("aaaa", "CHA-371 red-test: foo", "body", ["red-test"], [])],
        )

    def test_determinism_byte_identical(self):
        tasks = self._tasks()
        first = kph.render("cha-371", tasks)
        second = kph.render("cha-371", tasks)
        assert first == second

    def test_title_is_html_escaped_in_cards(self):
        hostile = kph.build_graph(
            _list_payload("aaaa"),
            [
                _show_payload(
                    "aaaa",
                    'CHA-371 impl: <script>alert("x")&</script>',
                    "body",
                    ["impl"],
                    [],
                )
            ],
        )
        out = kph.render("cha-371", hostile)
        assert "<script>alert" not in out
        assert "&lt;script&gt;" in out

    def test_mermaid_text_strips_unsafe_chars(self):
        cleaned = kph.mermaid_text('foo [bar] "baz" {qux} <x> | y')
        for bad in '"[]{}()|<>`':
            assert bad not in cleaned

    def test_mermaid_text_escapes_ampersand(self):
        # & must be escaped for parity with the card path's e() routing
        assert kph.mermaid_text("tidy & rename") == "tidy &amp; rename"

    def test_other_kind_renders_node_and_card(self):
        # a finding (KIND_OTHER) must get a defined mermaid node and a card —
        # not a dangling edge to an undefined node, and not an invisible task
        tasks = kph.build_graph(
            _list_payload("op", "find"),
            [
                _show_payload(
                    "op", "CHA-1 orch: open", "b", ["orch:open-pr"], [("find", "op")]
                ),
                _show_payload(
                    "find", "roborev: x", "body", ["roborev"], [("find", "op")]
                ),
            ],
        )
        out = kph.render("cha-371", tasks)
        assert 'nfind["' in out  # node defined, not just referenced by an edge
        assert "nfind --> nop" in out  # the blocks edge is present
        assert "FINDING" in out  # the card badge for the other layer
        assert "<b>1</b> findings" in out  # pill count reconciles
        # subgraph title carries no raw '&' into the Mermaid source
        assert "[Findings / other]" in out
        assert "[Findings & other]" not in out
