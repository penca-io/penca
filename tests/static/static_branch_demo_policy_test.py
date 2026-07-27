"""Static checks for the launch demo's allocation policies (CHA-517).

``examples/branch_demo.py``'s policy layer is infra-free, but the only other
tests that touch the module are Docker-gated integration tests — and branch-PR
CI skips the integration job (it is merge-queue only). So without these, nothing
that runs before a merge covers the policies at all (roborev finding on
224bf9f). These load the demo by path, the way
``static_kata_plan_html_test.py`` loads its generator, and pin only the
Penca-owned decision logic: the tie-break that keeps a run reproducible, the
prior that drives exploration off evidence rather than off the id ordering, the
fixed-split foil's wraparound, and the unknown-policy failure. No Docker, no
fixtures, no penca services — runs under ``just static-test branch_demo_policy``
and ``just check``.
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


def test_untried_outranks_any_measured_rate():
    """The Laplace prior scores an untried creative above any rate a shown
    creative can actually measure here, so exploration is driven by evidence."""
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


def test_greedy_tie_break_is_deterministic_and_id_ordered():
    """Every creative equal — the pick must be stable across calls and be the
    lowest id, or a run stops reproducing."""
    picks = {demo.pick_greedy(_tallies()) for _ in range(20)}
    assert picks == {min(demo.CREATIVE_IDS)}


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
