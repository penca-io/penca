#!/usr/bin/env python3
"""Branchable OLTP + OLAP on one open columnar copy of your data.

Open-source and self-hostable on object storage — no second system, no ETL.

Forks three branches off `main`, drives one shared deterministic visitor feed
through all three, and lets each branch's ad-allocation policy read back its
*own* committed tallies to steer the next round — transacting and reading
analytically against the same copy of the data, in one loop. Then it scores the
branches against each other and throws all three away; prod is never touched.

`even` splits traffic evenly and reads nothing: it is the foil. `greedy` and
`epsilon` reallocate from what they just wrote, which is the whole point. The
policies are deliberately toy — the database mechanic is the product here, not
the bandit.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import argparse
import random
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from uuid import uuid4

import pyarrow as pa
from penca_client import Mutation, PencaClient

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
CREATIVE_POSITION = {
    creative_id: position for position, creative_id in enumerate(CREATIVE_IDS)
}
HEADLINES = {creative_id: headline for creative_id, headline, _rate in CREATIVES}

POLICY_NAMES = ("even", "greedy", "epsilon")

AUTHOR = "penca-demo"

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

# Impressions allocated per transaction. A round costs ~145ms measured
# 2026-07-27 — and costs the same whether it carries 1 impression or 25, because
# that time is fixed per-RPC overhead rather than per-row work (TODO(CHA-523):
# a 4-row cold read alone is ~70ms of it). So this knob buys wall-clock, not
# throughput: 25 puts the default run near 50s where `--round-size 1` would take
# 24 minutes. `--round-size 1` is the literal per-impression loop and still works.
DEFAULT_ROUND_SIZE = 25


@dataclass(frozen=True)
class DemoConfig:
    impressions: int
    round_size: int
    epsilon: float
    seed: int


@dataclass(frozen=True)
class ProdContext:
    """Identifiers for the seeded catalog every later call threads through."""

    catalog_uuid: str
    main_branch_uuid: str
    schema_uuid: str
    creatives_table_uuid: str
    impressions_table_uuid: str
    seed_commit_seq_num: int


@dataclass(frozen=True)
class BranchOutcome:
    branch_name: str
    branch_uuid: str
    impressions: int
    conversions: int
    per_creative: Mapping[str, tuple[int, int]]
    # Conversions re-derived from the append-only impressions log. Must equal
    # `conversions`: the tally UPDATE and the log append commit in one tx.
    log_conversions: int


@dataclass(frozen=True)
class DemoOutcome:
    catalog_uuid: str
    scoreboard: tuple[BranchOutcome, ...]
    main_tallies: Mapping[str, tuple[int, int]]
    main_impression_rows: int
    remaining_branches: tuple[str, ...]


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


def seed_prod(client: PencaClient) -> ProdContext:
    """Create the prod catalog, the two tables, and commit the zeroed tallies."""
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"prod_{uuid4().hex[:8]}", AUTHOR
    )
    schema_uuid = client.create_schema(
        "ads",
        catalog_uuid=catalog_uuid,
        author=AUTHOR,
        comment="create ads schema",
    )
    creatives_table_uuid = client.create_table(
        "creatives",
        CREATIVES_SCHEMA,
        primary_keys=["creative_id"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author=AUTHOR,
        comment="create creatives table",
    )
    impressions_table_uuid = client.create_table(
        "impressions",
        IMPRESSIONS_SCHEMA,
        primary_keys=["visitor_id"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        author=AUTHOR,
        comment="create impressions table",
    )

    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author=AUTHOR,
        comment="seed creatives with zeroed tallies",
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=creatives_table_uuid,
            upserts=pa.table(
                {
                    "creative_id": list(CREATIVE_IDS),
                    "headline": [HEADLINES[c] for c in CREATIVE_IDS],
                    "impressions": [0] * len(CREATIVE_IDS),
                    "conversions": [0] * len(CREATIVE_IDS),
                },
                schema=CREATIVES_SCHEMA,
            ),
        ),
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
    )
    committed = client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
    )

    return ProdContext(
        catalog_uuid=catalog_uuid,
        main_branch_uuid=main_branch_uuid,
        schema_uuid=schema_uuid,
        creatives_table_uuid=creatives_table_uuid,
        impressions_table_uuid=impressions_table_uuid,
        seed_commit_seq_num=committed.commit_seq_num,
    )


def fork_branches(client: PencaClient, prod: ProdContext) -> dict[str, str]:
    """Fork one branch per policy off ``main``, all at the seed commit.

    Every fork names the same explicit ``commit_seq_num`` so the three branches
    provably start from one identical view of prod, rather than from whatever the
    head happened to be when each call landed.
    """
    return {
        policy_name: client.create_branch(
            policy_name,
            AUTHOR,
            f"fork {policy_name} off main",
            commit_seq_num=prod.seed_commit_seq_num,
            catalog_uuid=prod.catalog_uuid,
        ).branch_uuid
        for policy_name in POLICY_NAMES
    }


def read_tallies(
    client: PencaClient,
    prod: ProdContext,
    branch_uuid: str,
) -> dict[str, tuple[int, int]]:
    """Read one branch's running ``creative_id -> (shown, converted)`` tallies."""
    got = client.read_data(
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        table_uuid=prod.creatives_table_uuid,
        branch_uuid=branch_uuid,
        columns=["creative_id", "impressions", "conversions"],
    )

    return {
        creative_id: (shown, converted)
        for creative_id, shown, converted in zip(
            got.column("creative_id").to_pylist(),
            got.column("impressions").to_pylist(),
            got.column("conversions").to_pylist(),
            strict=True,
        )
    }


def read_impression_log(
    client: PencaClient,
    prod: ProdContext,
    branch_uuid: str,
) -> pa.Table:
    """Read one branch's append-only impression log."""
    return client.read_data(
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        table_uuid=prod.impressions_table_uuid,
        branch_uuid=branch_uuid,
        columns=["creative_id", "converted"],
    )


def drive_round(
    client: PencaClient,
    prod: ProdContext,
    branch: tuple[str, str],
    visitor_indexes: Sequence[int],
    feed: Sequence[Sequence[int]],
    rng: random.Random,
    epsilon: float,
) -> tuple[tuple[int, str, int], ...]:
    """Run one round on one branch; return its ``(visitor, creative, converted)``.

    This is the load-bearing loop. The read is of the branch's *own* committed
    state, so the allocation is steered by what this branch's previous rounds
    wrote — on the same copy of the data it is transacting against. The tally
    UPDATE and the log append share one transaction, so a scoreboard read can
    never catch them disagreeing.
    """
    branch_name, branch_uuid = branch
    tallies = read_tallies(client, prod, branch_uuid)

    picks = tuple(
        (
            visitor_index,
            choose_creative(branch_name, visitor_index, tallies, rng, epsilon),
        )
        for visitor_index in visitor_indexes
    )
    outcomes = tuple(
        (
            visitor_index,
            creative_id,
            feed[visitor_index][CREATIVE_POSITION[creative_id]],
        )
        for visitor_index, creative_id in picks
    )

    updated = dict(tallies)
    for _visitor_index, creative_id, converted in outcomes:
        shown, conversions = updated[creative_id]
        updated[creative_id] = (shown + 1, conversions + converted)

    touched = sorted({creative_id for _v, creative_id, _c in outcomes})
    tx = client.begin_tx(
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        branch_uuid=branch_uuid,
        author=AUTHOR,
        comment=f"{branch_name}: {len(outcomes)} impressions",
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=prod.creatives_table_uuid,
            upserts=pa.table(
                {
                    "creative_id": touched,
                    "headline": [HEADLINES[c] for c in touched],
                    "impressions": [updated[c][0] for c in touched],
                    "conversions": [updated[c][1] for c in touched],
                },
                schema=CREATIVES_SCHEMA,
            ),
        ),
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.write_data(
        tx.tx_uuid,
        Mutation(
            table_uuid=prod.impressions_table_uuid,
            upserts=pa.table(
                {
                    "visitor_id": [f"v{v:06d}" for v, _c, _o in outcomes],
                    "creative_id": [creative_id for _v, creative_id, _o in outcomes],
                    "converted": [converted for _v, _c, converted in outcomes],
                },
                schema=IMPRESSIONS_SCHEMA,
            ),
        ),
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        branch_uuid=branch_uuid,
    )
    client.commit_tx(
        tx.tx_uuid,
        catalog_uuid=prod.catalog_uuid,
        branch_uuid=branch_uuid,
    )

    return outcomes


def collect_branch_outcome(
    client: PencaClient,
    prod: ProdContext,
    branch: tuple[str, str],
) -> BranchOutcome:
    """Read a branch's final tallies and reconcile them against its own log."""
    branch_name, branch_uuid = branch
    per_creative = read_tallies(client, prod, branch_uuid)
    log = read_impression_log(client, prod, branch_uuid)

    return BranchOutcome(
        branch_name=branch_name,
        branch_uuid=branch_uuid,
        impressions=sum(shown for shown, _ in per_creative.values()),
        conversions=sum(converted for _, converted in per_creative.values()),
        per_creative=per_creative,
        log_conversions=sum(log.column("converted").to_pylist()),
    )


def run_demo(
    client: PencaClient,
    config: DemoConfig,
    on_round: Callable[[int, str, tuple[tuple[int, str, int], ...]], None]
    | None = None,
) -> DemoOutcome:
    """Fork, drive the shared feed through every branch, score, then discard.

    Rounds are the outer loop and branches the inner one, so all three branches
    advance through the shared feed in lockstep — the point being that they see
    identical traffic and diverge only on policy.
    """
    feed = build_visitor_feed(config)
    prod = seed_prod(client)
    branches = fork_branches(client, prod)
    # One RNG per policy, seeded off the run seed, so epsilon's exploration is
    # reproducible and independent of how many rounds the other branches ran.
    rngs = {
        policy_name: random.Random(config.seed + position)
        for position, policy_name in enumerate(POLICY_NAMES)
    }

    for round_index, start in enumerate(
        range(0, config.impressions, config.round_size)
    ):
        visitor_indexes = range(
            start, min(start + config.round_size, config.impressions)
        )
        for policy_name in POLICY_NAMES:
            outcomes = drive_round(
                client,
                prod,
                (policy_name, branches[policy_name]),
                visitor_indexes,
                feed,
                rngs[policy_name],
                config.epsilon,
            )
            if on_round is not None:
                on_round(round_index, policy_name, outcomes)

    scoreboard = sorted(
        (
            collect_branch_outcome(client, prod, (policy_name, branch_uuid))
            for policy_name, branch_uuid in branches.items()
        ),
        key=lambda outcome: (-outcome.conversions, outcome.branch_name),
    )

    main_tallies = read_tallies(client, prod, prod.main_branch_uuid)
    main_impression_rows = read_impression_log(
        client, prod, prod.main_branch_uuid
    ).num_rows

    for branch_uuid in branches.values():
        client.delete_branch(catalog_uuid=prod.catalog_uuid, branch_uuid=branch_uuid)

    remaining = tuple(
        sorted(
            branch.branch_name
            for branch in client.list_branches(catalog_uuid=prod.catalog_uuid)
        )
    )

    return DemoOutcome(
        catalog_uuid=prod.catalog_uuid,
        scoreboard=tuple(scoreboard),
        main_tallies=main_tallies,
        main_impression_rows=main_impression_rows,
        remaining_branches=remaining,
    )


def print_round(
    round_index: int, policy_name: str, outcomes: Sequence[tuple[int, str, int]]
) -> None:
    """One line per branch-round, so the policies visibly diverge as it runs."""
    shown = sorted({creative_id for _v, creative_id, _o in outcomes})
    converted = sum(converted for _v, _c, converted in outcomes)
    print(
        f"  round {round_index:>4}  {policy_name:<8} -> {', '.join(shown):<34}"
        f" {converted:>3} conversions"
    )


def print_scoreboard(outcome: DemoOutcome) -> None:
    """Cross-branch scoreboard, then each branch's allocation."""
    print("\n--- Cross-branch scoreboard (ranked) ---")
    print(
        pa.table(
            {
                "branch": [branch.branch_name for branch in outcome.scoreboard],
                "impressions": [branch.impressions for branch in outcome.scoreboard],
                "conversions": [branch.conversions for branch in outcome.scoreboard],
                "rate": [
                    f"{branch.conversions / branch.impressions:.2%}"
                    for branch in outcome.scoreboard
                ],
            }
        )
        .to_pandas()
        .to_markdown(index=False)
    )

    print("\n--- Where each branch spent its traffic ---")
    rows = [
        (branch.branch_name, creative_id, shown, converted)
        for branch in outcome.scoreboard
        for creative_id, (shown, converted) in sorted(
            branch.per_creative.items(), key=lambda item: -item[1][0]
        )
    ]
    print(
        pa.table(
            {
                "branch": [row[0] for row in rows],
                "creative": [row[1] for row in rows],
                "impressions": [row[2] for row in rows],
                "conversions": [row[3] for row in rows],
            }
        )
        .to_pandas()
        .to_markdown(index=False)
    )


def print_isolation(outcome: DemoOutcome) -> None:
    """Prod's own state after the run, and the post-discard branch list."""
    print("\n--- prod (main) after the run ---")
    creative_ids = sorted(outcome.main_tallies)
    print(
        pa.table(
            {
                "creative": creative_ids,
                "impressions": [outcome.main_tallies[c][0] for c in creative_ids],
                "conversions": [outcome.main_tallies[c][1] for c in creative_ids],
            }
        )
        .to_pandas()
        .to_markdown(index=False)
    )
    print(f"\nprod impression log: {outcome.main_impression_rows} rows")
    print(f"branches remaining after discard: {', '.join(outcome.remaining_branches)}")
    print(
        "\nprod is intact. Three parallel universes ran against it and were thrown away."
    )


def parse_args() -> DemoConfig:
    parser = argparse.ArgumentParser(
        description="Branchable OLTP + OLAP on one open columnar copy of your data."
    )
    parser.add_argument("--impressions", type=int, default=DEFAULT_IMPRESSIONS)
    parser.add_argument("--round-size", type=int, default=DEFAULT_ROUND_SIZE)
    parser.add_argument("--epsilon", type=float, default=DEFAULT_EPSILON)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = parser.parse_args()

    return DemoConfig(
        impressions=args.impressions,
        round_size=args.round_size,
        epsilon=args.epsilon,
        seed=args.seed,
    )


def main() -> None:
    config = parse_args()
    print(
        f"Forking {len(POLICY_NAMES)} branches off main, driving {config.impressions} "
        f"shared impressions at {config.round_size} per transaction.\n"
    )

    outcome = run_demo(PencaClient.from_settings(), config, on_round=print_round)

    print_scoreboard(outcome)
    print_isolation(outcome)


if __name__ == "__main__":
    main()
