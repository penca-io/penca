#!/usr/bin/env python3
"""Shared visitor feed and allocation policies for Penca's branch demo.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import random
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa

# Ad creatives with well-separated true conversion rates. The best performer is
# deliberately not the first by id, so "pick the winner" is never the same
# decision as "pick the first thing you see".
CREATIVES = (
    ("banner", "Save 20% on your first year", 0.06),
    ("carousel", "One copy of your data. Both workloads.", 0.22),
    ("story", "How we deleted our ETL pipeline", 0.14),
    ("video", "Watch a 90-second tour", 0.02),
)
CREATIVE_IDS = tuple(creative_id for creative_id, _headline, _rate in CREATIVES)

POLICY_NAMES = ("even", "greedy", "epsilon")

CREATIVES_SCHEMA = pa.schema(
    [
        pa.field("creative_id", pa.utf8()),
        pa.field("headline", pa.utf8()),
        pa.field("impressions", pa.int64()),
        pa.field("conversions", pa.int64()),
    ]
)
CREATIVES_PK_SCHEMA = pa.schema([pa.field("creative_id", pa.utf8())])
IMPRESSIONS_SCHEMA = pa.schema(
    [
        pa.field("visitor_id", pa.utf8()),
        pa.field("creative_id", pa.utf8()),
        pa.field("converted", pa.int64()),
    ]
)

DEFAULT_IMPRESSIONS = 3000
DEFAULT_EPSILON = 0.15
DEFAULT_SEED = 20260727


@dataclass(frozen=True)
class DemoConfig:
    impressions: int
    round_size: int
    epsilon: float
    seed: int


def build_visitor_feed(config: DemoConfig) -> tuple[tuple[int, ...], ...]:
    """One shared, reproducible stream of visitors.

    Returns one row per visitor holding that visitor's latent 0/1 outcome for
    *every* creative, in ``CREATIVES`` order. Fixing all four outcomes up front is
    what makes the branches share a single feed: two branches showing the same
    creative to the same visitor get the same answer, so their scoreboards differ
    only because their allocation policies differ.
    """
    rng = random.Random(config.seed)
    rates = tuple(rate for _creative_id, _headline, rate in CREATIVES)

    return tuple(
        tuple(int(rng.random() < rate) for rate in rates)
        for _visitor_index in range(config.impressions)
    )


def smoothed_rate(tally: tuple[int, int]) -> float:
    """Laplace-smoothed conversion rate for one creative's ``(shown, converted)``.

    The +1/+2 prior scores an untried creative at 0.5 — above any rate this demo
    can measure — so a greedy policy sweeps every creative once before it locks
    on. Without the prior all four start at 0/0 and the tie-break alone decides,
    which pins greedy to the first creative forever.
    """
    shown, converted = tally

    return (converted + 1) / (shown + 2)


def pick_even(visitor_index: int) -> str:
    """Fixed round-robin split. Reads nothing — this is the foil."""
    return CREATIVE_IDS[visitor_index % len(CREATIVE_IDS)]


def pick_greedy(tallies: Mapping[str, tuple[int, int]]) -> str:
    """Best smoothed rate so far, ties broken by ``creative_id`` for determinism."""
    ranked = sorted(
        tallies,
        key=lambda creative_id: (-smoothed_rate(tallies[creative_id]), creative_id),
    )

    return ranked[0]


def pick_epsilon(
    tallies: Mapping[str, tuple[int, int]],
    rng: random.Random,
    epsilon: float,
) -> str:
    """Greedy, with an ``epsilon`` chance of exploring uniformly instead."""
    if rng.random() < epsilon:
        return rng.choice(CREATIVE_IDS)

    return pick_greedy(tallies)


def choose_creative(
    policy_name: str,
    visitor_index: int,
    tallies: Mapping[str, tuple[int, int]],
    rng: random.Random,
    epsilon: float,
) -> str:
    """Dispatch to the named policy.

    ``even`` is handed ``tallies`` and ignores them, which is the whole reason it
    is here: it needs no read-your-writes, so it makes the other two legible.
    """
    if policy_name == "even":
        return pick_even(visitor_index)

    if policy_name == "greedy":
        return pick_greedy(tallies)

    if policy_name == "epsilon":
        return pick_epsilon(tallies, rng, epsilon)

    msg = f"unknown policy {policy_name!r}, expected one of {POLICY_NAMES}"
    raise ValueError(msg)
