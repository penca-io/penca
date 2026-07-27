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
from collections.abc import Callable, Iterable, Mapping, Sequence
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
    # Re-derived from the append-only impressions log. Must equal `conversions`
    # and `impressions`: the tally UPDATE and the log append commit in one tx.
    # The row count is the one that catches an overlapping visitor range
    # replacing log rows instead of appending, since visitor_id is the PK.
    log_conversions: int
    log_impressions: int


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

    The +1/+2 prior scores an untried creative at 0.5, above any rate a creative
    with real exposure can measure at this demo's true rates. So greedy is pulled
    toward creatives it has not tried yet, and its exploration is driven by
    evidence rather than by the id tie-break — without the prior all four start at
    0/0 and the tie-break alone pins greedy to the first creative forever. This is
    a bias, not a guaranteed one-pass sweep: a creative whose first exposure
    converts scores above 0.5 and can be re-picked immediately.
    """
    shown, converted = tally

    return (converted + 1) / (shown + 2)


def pick_even(visitor_index: int) -> str:
    """Fixed round-robin split. Reads nothing — this is the foil."""
    return CREATIVE_IDS[visitor_index % len(CREATIVE_IDS)]


def pick_greedy(tallies: Mapping[str, tuple[int, int]]) -> str:
    """Best smoothed rate so far; among ties, the lowest ``creative_id``.

    Ranks over ``CREATIVE_IDS`` rather than over ``tallies``' keys, so the
    candidate set matches the other two policies no matter what the read returned:
    a creative missing from ``tallies`` counts as untried instead of becoming
    unreachable for the rest of the run. That fixed tuple is what makes the pick
    reproducible; the ``creative_id`` key only decides *which* tied creative wins.
    """
    ranked = sorted(
        CREATIVE_IDS,
        key=lambda creative_id: (
            -smoothed_rate(tallies.get(creative_id, (0, 0))),
            creative_id,
        ),
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


def abort_quietly(
    client: PencaClient, catalog_uuid: str, tx_uuid: str, branch_uuid: str
) -> None:
    """Best-effort abort of an open tx, never masking why we are unwinding.

    An open penca tx is not inert: until it times out server-side it clamps cold
    isolation and fences purge/GC on its branch. But the abort must not replace
    the exception that triggered it — ``abort_tx`` raises FailedPrecondition if the
    tx actually committed (a dropped commit response lands exactly there), and if
    the write failed because the stack is unreachable the abort fails too.
    ``Exception``, not ``BaseException``, so a second Ctrl-C still gets through.
    """
    try:
        client.abort_tx(tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
    except Exception as exc:
        print(f"  (could not abort tx {tx_uuid}: {exc})")


def discard_branches(
    client: PencaClient, catalog_uuid: str, branch_uuids: Iterable[str]
) -> list[str]:
    """Delete every branch, attempting all of them; return the ones that failed.

    Guarded per branch for two reasons: one failure must not strand the rest, and
    when this runs from a ``finally`` an unguarded raise would replace the
    exception being propagated — and the usual reason a delete fails is that the
    stack is unreachable, i.e. exactly the error worth keeping. Returning the
    failures rather than swallowing them lets the caller decide, which matters on
    the success path where there is no exception worth protecting.
    """
    failed: list[str] = []
    for branch_uuid in branch_uuids:
        try:
            client.delete_branch(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        except Exception as exc:
            print(f"  (could not delete branch {branch_uuid}: {exc})")
            failed.append(branch_uuid)

    return failed


def raise_for_undeleted(undeleted: Sequence[str], *, completed: bool) -> None:
    """Fail a run that finished normally but could not discard its branches."""
    if undeleted and completed:
        msg = f"could not discard branches: {', '.join(undeleted)}"
        raise RuntimeError(msg)


def discard_catalog(client: PencaClient, catalog_uuid: str) -> None:
    """Best-effort delete of a catalog, for unwinding a half-built seed."""
    try:
        client.delete_catalog(catalog_uuid=catalog_uuid)
    except Exception as exc:
        print(f"  (could not delete catalog {catalog_uuid}: {exc})")


def create_ads_tables(
    client: PencaClient, catalog_uuid: str, main_branch_uuid: str
) -> tuple[str, str, str]:
    """Create the ``ads`` schema and both tables.

    Returns ``(schema_uuid, creatives_table_uuid, impressions_table_uuid)``.
    ``branch_uuid`` explicitly on every call: omitting it falls back to the
    client's constructor-configured default branch, which need not be the "main"
    of this brand-new catalog.
    """
    schema_uuid = client.create_schema(
        "ads",
        catalog_uuid=catalog_uuid,
        branch_uuid=main_branch_uuid,
        author=AUTHOR,
        comment="create ads schema",
    )
    creatives_table_uuid = client.create_table(
        "creatives",
        CREATIVES_SCHEMA,
        primary_keys=["creative_id"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author=AUTHOR,
        comment="create creatives table",
    )
    impressions_table_uuid = client.create_table(
        "impressions",
        IMPRESSIONS_SCHEMA,
        primary_keys=["visitor_id"],
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author=AUTHOR,
        comment="create impressions table",
    )

    return schema_uuid, creatives_table_uuid, impressions_table_uuid


def commit_seed_tallies(
    client: PencaClient,
    catalog_uuid: str,
    main_branch_uuid: str,
    schema_uuid: str,
    creatives_table_uuid: str,
) -> int:
    """Commit the four creatives at zeroed tallies; return the commit's seq num."""
    tx = client.begin_tx(
        catalog_uuid=catalog_uuid,
        schema_uuid=schema_uuid,
        branch_uuid=main_branch_uuid,
        author=AUTHOR,
        comment="seed creatives with zeroed tallies",
    )
    try:
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
    except BaseException:
        abort_quietly(client, catalog_uuid, tx.tx_uuid, main_branch_uuid)
        raise

    return committed.commit_seq_num


def seed_prod(client: PencaClient) -> ProdContext:
    """Create the prod catalog, the two tables, and commit the zeroed tallies."""
    catalog_uuid, main_branch_uuid = client.create_catalog(
        f"prod_{uuid4().hex[:8]}", AUTHOR
    )
    try:
        schema_uuid, creatives_table_uuid, impressions_table_uuid = create_ads_tables(
            client, catalog_uuid, main_branch_uuid
        )
        seed_commit_seq_num = commit_seed_tallies(
            client, catalog_uuid, main_branch_uuid, schema_uuid, creatives_table_uuid
        )
    except BaseException:
        # Everything below the create_catalog lives inside the catalog, so
        # dropping it unwinds the whole partial seed. seed_prod created the
        # catalog, so seed_prod owns removing it — splitting this across the two
        # helpers would make the ownership ambiguous. Without it each failed
        # attempt strands another prod_<hex> catalog on a shared stack.
        discard_catalog(client, catalog_uuid)
        raise

    return ProdContext(
        catalog_uuid=catalog_uuid,
        main_branch_uuid=main_branch_uuid,
        schema_uuid=schema_uuid,
        creatives_table_uuid=creatives_table_uuid,
        impressions_table_uuid=impressions_table_uuid,
        seed_commit_seq_num=seed_commit_seq_num,
    )


def fork_branches(client: PencaClient, prod: ProdContext) -> dict[str, str]:
    """Fork one branch per policy off ``main``, all at the seed commit.

    Every fork names the same explicit ``commit_seq_num`` so the three branches
    provably start from one identical view of prod, rather than from whatever the
    head happened to be when each call landed.
    """
    created: dict[str, str] = {}
    try:
        for policy_name in POLICY_NAMES:
            created[policy_name] = client.create_branch(
                policy_name,
                AUTHOR,
                f"fork {policy_name} off main",
                commit_seq_num=prod.seed_commit_seq_num,
                catalog_uuid=prod.catalog_uuid,
            ).branch_uuid
    except BaseException:
        # A failure on the second or third fork would otherwise leave the earlier
        # ones live: run_demo's finally only covers branches it was handed.
        discard_branches(client, prod.catalog_uuid, created.values())
        raise

    return created


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


def allocate_round(
    policy_name: str,
    visitor_indexes: Sequence[int],
    tallies: Mapping[str, tuple[int, int]],
    feed: Sequence[Sequence[int]],
    rng: random.Random,
    epsilon: float,
) -> tuple[tuple[int, str, int], ...]:
    """Decide one round: per visitor, a creative and that visitor's outcome.

    Draws from ``rng`` once per visitor, in visitor order — an allocation that
    consumed it differently would shift epsilon's whole stream and change the
    scoreboard at a fixed seed.
    """
    outcomes: list[tuple[int, str, int]] = []
    for visitor_index in visitor_indexes:
        creative_id = choose_creative(policy_name, visitor_index, tallies, rng, epsilon)
        converted = feed[visitor_index][CREATIVE_POSITION[creative_id]]
        outcomes.append((visitor_index, creative_id, converted))

    return tuple(outcomes)


def apply_outcomes(
    tallies: Mapping[str, tuple[int, int]],
    outcomes: Sequence[tuple[int, str, int]],
) -> dict[str, tuple[int, int]]:
    """Fold a round's outcomes onto a copy of the tallies."""
    updated = dict(tallies)
    for _visitor_index, creative_id, converted in outcomes:
        shown, conversions = updated.get(creative_id, (0, 0))
        updated[creative_id] = (shown + 1, conversions + converted)

    return updated


def tally_batch(
    touched: Sequence[str], updated: Mapping[str, tuple[int, int]]
) -> pa.Table:
    """The ``creatives`` upsert payload — same primary keys, new running totals."""
    return pa.table(
        {
            "creative_id": list(touched),
            "headline": [HEADLINES[creative_id] for creative_id in touched],
            "impressions": [updated[creative_id][0] for creative_id in touched],
            "conversions": [updated[creative_id][1] for creative_id in touched],
        },
        schema=CREATIVES_SCHEMA,
    )


def impression_batch(outcomes: Sequence[tuple[int, str, int]]) -> pa.Table:
    """The ``impressions`` append payload, one row per visitor served."""
    return pa.table(
        {
            "visitor_id": [f"v{visitor:06d}" for visitor, _c, _o in outcomes],
            "creative_id": [creative_id for _v, creative_id, _o in outcomes],
            "converted": [converted for _v, _c, converted in outcomes],
        },
        schema=IMPRESSIONS_SCHEMA,
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

    This is the load-bearing loop: read → allocate → write. The read is of the
    branch's *own* committed state, so the allocation is steered by what this
    branch's previous rounds wrote — on the same copy of the data it is transacting
    against. The tally UPDATE and the log append share one transaction, so a
    scoreboard read can never catch them disagreeing.
    """
    branch_name, branch_uuid = branch
    tallies = read_tallies(client, prod, branch_uuid)
    outcomes = allocate_round(branch_name, visitor_indexes, tallies, feed, rng, epsilon)
    updated = apply_outcomes(tallies, outcomes)
    touched = sorted({creative_id for _v, creative_id, _c in outcomes})

    tx = client.begin_tx(
        catalog_uuid=prod.catalog_uuid,
        schema_uuid=prod.schema_uuid,
        branch_uuid=branch_uuid,
        author=AUTHOR,
        comment=f"{branch_name}: {len(outcomes)} impressions",
    )
    try:
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=prod.creatives_table_uuid,
                upserts=tally_batch(touched, updated),
            ),
            catalog_uuid=prod.catalog_uuid,
            schema_uuid=prod.schema_uuid,
            branch_uuid=branch_uuid,
        )
        client.write_data(
            tx.tx_uuid,
            Mutation(
                table_uuid=prod.impressions_table_uuid,
                upserts=impression_batch(outcomes),
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
    except BaseException:
        abort_quietly(client, prod.catalog_uuid, tx.tx_uuid, branch_uuid)
        raise

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
        log_impressions=log.num_rows,
    )


def policy_rngs(config: DemoConfig) -> dict[str, random.Random]:
    """One RNG per policy, so exploration is reproducible per branch.

    Seeded on a ``"seed:name"`` string rather than ``seed + index``: a bare offset
    gives index 0 the same stream ``build_visitor_feed``'s int seed produces, so a
    reading policy at that position would explore against the very outcomes that
    stream generated. A str seed cannot collide with the feed's int seed.
    """
    return {
        policy_name: random.Random(f"{config.seed}:{policy_name}")
        for policy_name in POLICY_NAMES
    }


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
    rngs = policy_rngs(config)

    completed = False
    try:
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
        completed = True
    finally:
        # finally, not straight-line: the docstring promises the forks are thrown
        # away, and a failed run is exactly when leaving three live branches behind
        # would hurt most. The prod_* catalog deliberately survives — prod
        # outliving its forks is the thing being demonstrated.
        undeleted = discard_branches(client, prod.catalog_uuid, branches.values())
        # Only swallow while unwinding. On a green run a failed delete must not
        # degrade to a printed line: the demo would exit 0 with live forks, and
        # print_isolation would list them directly above "N parallel universes …
        # were thrown away". A frame-local flag rather than sys.exc_info(), which
        # reports an exception being handled anywhere up the stack — a caller that
        # invoked run_demo from inside an except block would suppress this raise.
        raise_for_undeleted(undeleted, completed=completed)

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


def conversion_rate(branch: BranchOutcome) -> str:
    """Formatted rate, tolerant of a branch that was never shown anything."""
    if branch.impressions == 0:
        return "n/a"

    return f"{branch.conversions / branch.impressions:.2%}"


def print_scoreboard(outcome: DemoOutcome) -> None:
    """Cross-branch scoreboard, then each branch's allocation."""
    print("\n--- Cross-branch scoreboard (ranked) ---")
    print(
        pa.table(
            {
                "branch": [branch.branch_name for branch in outcome.scoreboard],
                "impressions": [branch.impressions for branch in outcome.scoreboard],
                "conversions": [branch.conversions for branch in outcome.scoreboard],
                "rate": [conversion_rate(branch) for branch in outcome.scoreboard],
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
            # creative_id breaks ties: `even` shows all four an equal number of
            # times, and without it their order comes from whatever row order
            # read_data returned, so identical input could print differently.
            branch.per_creative.items(),
            key=lambda item: (-item[1][0], item[0]),
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
        f"\nprod is intact. {len(POLICY_NAMES)} parallel universes ran against it "
        f"and were thrown away."
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
    # Validate before main() opens a client: every one of these otherwise fails
    # *after* seed_prod and fork_branches have created a catalog and three
    # branches, so a typo'd flag would leave debris behind a stack trace.
    if args.impressions < 1:
        parser.error("--impressions must be at least 1")

    if args.round_size < 1:
        parser.error("--round-size must be at least 1")

    if not 0.0 <= args.epsilon <= 1.0:
        parser.error("--epsilon must be between 0.0 and 1.0")

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
