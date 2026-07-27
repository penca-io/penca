"""Static checks for the launch demo's policies, printers, and CLI (CHA-517).

``examples/branch_demo.py``'s policy layer is infra-free, but the only other
tests that touch the module are Docker-gated integration tests — and branch-PR
CI skips the integration job (it is merge-queue only). So without these, nothing
that runs before a merge covers the policies at all (roborev finding on
224bf9f). These load the demo by path, the way
``static_kata_plan_html_test.py`` loads its generator, and pin only the
Penca-owned decision logic: the tie-break that keeps a run reproducible, the
prior that drives exploration off evidence rather than off the id ordering, the
fixed-split foil's wraparound, the unknown-policy failure, and — against a
hand-built ``DemoOutcome``, no engine needed — that the printers and the CLI's
input validation hold up. No Docker, no fixtures, no penca services — runs under
``just static-test branch_demo_policy`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
import random
import sys
from pathlib import Path

DEMO = Path(__file__).parents[2] / "examples/branch_demo.py"


def _load_demo():
    spec = importlib.util.spec_from_file_location("branch_demo", DEMO)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register before executing: the demo's dataclasses are defined under
    # `from __future__ import annotations`, and dataclasses resolves those string
    # annotations through sys.modules[cls.__module__], which is None for a module
    # built from a spec but never registered.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


demo = _load_demo()


def _tallies(**overrides: tuple[int, int]) -> dict[str, tuple[int, int]]:
    """All creatives untried, with the named ones overridden."""
    return {
        creative_id: overrides.get(creative_id, (0, 0))
        for creative_id in demo.CREATIVE_IDS
    }


def test_untried_outranks_a_creative_with_real_exposure():
    """Once a creative has meaningful exposure it cannot outrank an untried one, so
    exploration follows evidence. Deliberately not the strong form: a creative
    whose single first impression converts scores 0.667, above the untried 0.5."""
    untried = demo.smoothed_rate((0, 0))
    assert untried == 0.5

    # A creative shown enough times to have a real estimate cannot outrank an
    # untried one, even at this demo's best true rate (0.22).
    assert demo.smoothed_rate((50, 11)) < untried
    assert demo.smoothed_rate((100, 22)) < untried


def _all_measured() -> dict[str, tuple[int, int]]:
    """Every creative has real exposure, ``carousel`` clearly best.

    Comparing *measured* rates needs all four exposed: an untried creative scores
    0.5 and would rightly win, which is what
    ``test_untried_outranks_any_measured_rate`` covers.
    """
    return {"banner": (50, 2), "carousel": (50, 12), "story": (50, 7), "video": (50, 1)}


def test_greedy_prefers_the_better_measured_creative():
    assert demo.pick_greedy(_all_measured()) == "carousel"


def test_greedy_tie_break_picks_the_lowest_id_not_the_first_declared():
    """With every creative tied, the id key must decide — not declaration order.

    CREATIVE_IDS is patched so the two differ. Ranking over the fixed tuple is
    what makes the pick reproducible, so with the real fixture (where "banner" is
    both lowest-id and first-declared) dropping the secondary key would still
    return "banner" and this test would prove nothing.
    """
    original = demo.CREATIVE_IDS
    demo.CREATIVE_IDS = ("video", "banner")
    try:
        assert demo.pick_greedy({"video": (0, 0), "banner": (0, 0)}) == "banner"
    finally:
        demo.CREATIVE_IDS = original


def test_greedy_considers_creatives_missing_from_the_tallies():
    """Ranking is anchored to CREATIVE_IDS, not to whatever the read returned.

    A read that omitted a creative must not make it permanently unreachable —
    an absent creative is untried, and untried outranks measured.
    """
    partial = {"banner": (50, 2)}
    assert demo.pick_greedy(partial) != "banner"
    assert demo.pick_greedy(partial) in demo.CREATIVE_IDS


def test_even_splits_round_robin_and_wraps():
    picked = [demo.pick_even(index) for index in range(len(demo.CREATIVE_IDS) * 2)]
    assert picked[: len(demo.CREATIVE_IDS)] == list(demo.CREATIVE_IDS)
    assert picked[len(demo.CREATIVE_IDS) :] == list(demo.CREATIVE_IDS)


def test_epsilon_explores_and_exploits():
    """epsilon=0 collapses to greedy; epsilon=1 always takes the RNG's pick."""
    tallies = _all_measured()
    assert demo.pick_epsilon(tallies, random.Random(0), 0.0) == "carousel"

    explored = {
        demo.pick_epsilon(tallies, random.Random(seed), 1.0) for seed in range(40)
    }
    assert len(explored) > 1, "always-explore must not collapse to one creative"
    assert explored <= set(demo.CREATIVE_IDS)


def test_unknown_policy_fails_fast():
    try:
        demo.choose_creative("bandit", 0, _tallies(), random.Random(0), 0.1)
    except ValueError as exc:
        assert "bandit" in str(exc)
    else:
        raise AssertionError("an unknown policy name must raise ValueError")


def _synthetic_outcome(impressions: int = 100):
    """A DemoOutcome with no engine behind it, for the printers."""
    per_creative = {
        creative_id: (impressions // len(demo.CREATIVE_IDS), index)
        for index, creative_id in enumerate(demo.CREATIVE_IDS)
    }
    conversions = sum(converted for _shown, converted in per_creative.values())
    branches = tuple(
        demo.BranchOutcome(
            branch_name=policy_name,
            branch_uuid=f"uuid-{policy_name}",
            impressions=impressions,
            conversions=conversions,
            per_creative=per_creative,
            log_conversions=conversions,
            log_impressions=impressions,
        )
        for policy_name in demo.POLICY_NAMES
    )

    return demo.DemoOutcome(
        catalog_uuid="uuid-catalog",
        scoreboard=branches,
        main_tallies=dict.fromkeys(demo.CREATIVE_IDS, (0, 0)),
        main_impression_rows=0,
        remaining_branches=("main",),
    )


def test_printers_emit_a_scoreboard_and_the_isolation_proof(capsys):
    outcome = _synthetic_outcome()
    demo.print_round(0, "epsilon", ((0, demo.CREATIVE_IDS[0], 1),))
    demo.print_scoreboard(outcome)
    demo.print_isolation(outcome)

    printed = capsys.readouterr().out
    assert "scoreboard" in printed.lower()
    for policy_name in demo.POLICY_NAMES:
        assert policy_name in printed

    assert "prod is intact" in printed
    assert f"{len(demo.POLICY_NAMES)} parallel universes" in printed, (
        "the punchline must derive the branch count, not hardcode it"
    )


def test_scoreboard_survives_a_branch_that_was_never_shown_anything(capsys):
    """A zero-impression branch must not divide by zero in the rate column.

    Goes through print_scoreboard, not conversion_rate: the ZeroDivisionError this
    guards lived in the printer's comprehension, so re-inlining the division there
    has to fail this test.
    """
    demo.print_scoreboard(_synthetic_outcome(impressions=0))
    assert "n/a" in capsys.readouterr().out


def _parse_args_with(argv: list[str]):
    original = sys.argv
    sys.argv = ["branch_demo.py", *argv]
    try:
        return demo.parse_args()
    finally:
        sys.argv = original


def test_parse_args_accepts_the_defaults():
    config = _parse_args_with([])
    assert config.impressions == demo.DEFAULT_IMPRESSIONS
    assert config.round_size == demo.DEFAULT_ROUND_SIZE
    assert config.seed == demo.DEFAULT_SEED


def test_parse_args_rejects_input_that_would_leave_debris():
    """These all used to fail only after a catalog and three branches existed."""
    for argv in (
        ["--impressions", "0"],
        ["--round-size", "0"],
        ["--round-size", "-5"],
        ["--epsilon", "1.5"],
        ["--epsilon", "-0.1"],
    ):
        try:
            _parse_args_with(argv)
        except SystemExit as exit_code:
            assert exit_code.code == 2, f"{argv} should exit 2 with a usage message"
        else:
            raise AssertionError(f"{argv} must be rejected before any RPC")


def test_policy_rng_streams_are_independent_of_the_feed_and_of_each_other():
    """The seeding claim, asserted through the demo's own policy_rngs.

    A bare `seed + index` offset gave index 0 the same stream
    build_visitor_feed's int seed produces, so a reading policy at that position
    would explore against the very outcomes it is measuring.
    """
    config = demo.DemoConfig(
        impressions=8,
        round_size=4,
        epsilon=demo.DEFAULT_EPSILON,
        seed=demo.DEFAULT_SEED,
    )
    streams = {
        policy_name: [rng.random() for _ in range(4)]
        for policy_name, rng in demo.policy_rngs(config).items()
    }
    assert set(streams) == set(demo.POLICY_NAMES)

    # Derive the feed's stream from build_visitor_feed itself rather than
    # rebuilding random.Random(seed) by hand: a change to the feed's seeding must
    # fail here, not silently invalidate the comparison.
    feed_rng = random.Random(config.seed)
    expected_first_row = tuple(
        int(feed_rng.random() < rate) for *_head, rate in demo.CREATIVES
    )
    assert demo.build_visitor_feed(config)[0] == expected_first_row, (
        "the feed is no longer drawn from random.Random(seed) in CREATIVES order; "
        "update this test's model of it before trusting the comparison below"
    )

    feed_stream = [random.Random(config.seed).random() for _ in range(4)]
    for policy_name, stream in streams.items():
        assert stream != feed_stream, (
            f"{policy_name}'s stream must not match the one that built the feed"
        )

    distinct = {tuple(stream) for stream in streams.values()}
    assert len(distinct) == len(demo.POLICY_NAMES), (
        "each policy needs its own stream, or they explore in lockstep"
    )


class _FailingClient:
    """Minimal stand-in: records calls, raises what the test asks it to."""

    def __init__(self, raises=None, fail_on=None):
        self.raises = raises
        self.fail_on = fail_on
        self.deleted: list[str] = []
        self.aborted: list[str] = []

    def delete_branch(self, catalog_uuid: str, branch_uuid: str) -> None:
        self.deleted.append(branch_uuid)
        if self.raises is not None and branch_uuid == self.fail_on:
            raise self.raises

    def abort_tx(self, tx_uuid: str, catalog_uuid: str, branch_uuid: str) -> None:
        self.aborted.append(tx_uuid)
        if self.raises is not None:
            raise self.raises


def test_discard_branches_attempts_every_branch_and_reports_the_failures():
    """One failure must not strand the branches after it."""
    client = _FailingClient(raises=RuntimeError("boom"), fail_on="b")
    failed = demo.discard_branches(client, "cat", ["a", "b", "c"])

    assert client.deleted == ["a", "b", "c"], "every branch must be attempted"
    assert failed == ["b"]


def test_discard_branches_reports_nothing_when_every_delete_lands():
    client = _FailingClient()
    assert demo.discard_branches(client, "cat", ["a", "b"]) == []


def test_abort_quietly_does_not_replace_the_exception_being_unwound():
    """A failing abort must not become the error the reader sees."""
    client = _FailingClient(raises=RuntimeError("abort failed"))
    demo.abort_quietly(client, "cat", "tx", "branch")
    assert client.aborted == ["tx"]


def test_cleanup_helpers_let_a_second_ctrl_c_through():
    """They catch Exception, not BaseException, so an interrupt still propagates."""
    client = _FailingClient(raises=KeyboardInterrupt(), fail_on="a")
    try:
        demo.discard_branches(client, "cat", ["a"])
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt must not be swallowed")

    try:
        demo.abort_quietly(_FailingClient(raises=KeyboardInterrupt()), "c", "tx", "b")
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt must not be swallowed")
