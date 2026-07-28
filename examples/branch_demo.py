#!/usr/bin/env python3
"""Branchable OLTP + OLAP on one open columnar copy of your data.

Open-source and self-hostable on object storage — no second system, no ETL.

Forks three branches off `main`, drives one shared deterministic visitor feed
through all three, and lets each branch's ad-allocation policy read back its
*own* committed tallies to steer the next round. Then it scores the branches
against each other and throws all three away; prod is never touched.

The round loop is ordinary SQL. Each branch is one connection — branch selection
binds at handshake and is immutable for the connection's lifetime, the way a
Postgres connection is to one database — so a branch is a plain SQL endpoint and
the statements below are what any Flight SQL driver would send. Read
`run_rounds`; it is the point of this file.

Setup (catalog, tables, forks) uses the gRPC client, because forking pins to the
seed's commit_seq_num and SQL does not hand that back.

Requires Docker services running: just penca-up
"""

from __future__ import annotations

import argparse
import random
import time
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
CREATIVE_POSITION = {cid: pos for pos, cid in enumerate(CREATIVE_IDS)}
HEADLINES = {cid: headline for cid, headline, _rate in CREATIVES}

POLICY_NAMES = ("even", "greedy", "epsilon")
SCHEMA_NAME = "ads"
AUTHOR = "penca-demo"

DEFAULT_IMPRESSIONS = 3000
DEFAULT_EPSILON = 0.15
DEFAULT_SEED = 20260727
# Impressions per transaction. Most of a statement's cost is fixed rather than
# per-row, so this knob mostly buys wall-clock rather than throughput:
# `--round-size 1` is the literal per-impression loop and works, but takes many
# times longer for the same number of impressions. It also sets
# *decision* granularity — a round's picks are all evaluated against the one
# SELECT taken at its start, so greedy serves the same creative for a whole
# round. TODO(CHA-525): batching a round's statements would cut this further.
DEFAULT_ROUND_SIZE = 25

CREATIVES_SCHEMA = pa.schema(
    [
        pa.field("creative_id", pa.utf8()),
        pa.field("headline", pa.utf8()),
        pa.field("impressions", pa.int64()),
        pa.field("conversions", pa.int64()),
    ]
)
IMPRESSIONS_SCHEMA = pa.schema(
    [
        pa.field("visitor_id", pa.utf8()),
        pa.field("creative_id", pa.utf8()),
        pa.field("converted", pa.int64()),
    ]
)


@dataclass(frozen=True)
class DemoConfig:
    impressions: int
    round_size: int
    epsilon: float
    seed: int


@dataclass(frozen=True)
class ProdContext:
    """What setup produces: the seeded catalog, addressed both ways.

    uuids for the gRPC calls, names for the SQL.
    """

    catalog_uuid: str
    catalog_name: str
    main_branch_uuid: str
    schema_uuid: str
    creatives_table_uuid: str
    impressions_table_uuid: str
    seed_commit_seq_num: int

    @property
    def creatives(self) -> str:
        return f"{self.catalog_name}.{SCHEMA_NAME}.creatives"

    @property
    def impressions(self) -> str:
        return f"{self.catalog_name}.{SCHEMA_NAME}.impressions"


@dataclass(frozen=True)
class BranchOutcome:
    branch_name: str
    branch_uuid: str
    impressions: int
    conversions: int
    per_creative: Mapping[str, tuple[int, int]]
    # Re-derived from the append-only impressions log. Must equal `conversions`
    # and `impressions`: the tally upsert and the log append commit together.
    log_conversions: int
    log_impressions: int


@dataclass(frozen=True)
class DemoOutcome:
    catalog_uuid: str
    scoreboard: tuple[BranchOutcome, ...]
    main_tallies: Mapping[str, tuple[int, int]]
    main_impression_rows: int
    remaining_branches: tuple[str, ...]


# --- the policies ------------------------------------------------------------
# Deliberately toy. The database mechanic is the product here, not the bandit.


def build_visitor_feed(config: DemoConfig) -> tuple[tuple[int, ...], ...]:
    """One shared, reproducible stream of visitors.

    One row per visitor holding that visitor's latent 0/1 outcome for *every*
    creative. Fixing all four up front is what makes the branches share a feed:
    two branches showing the same creative to the same visitor get the same
    answer, so their scoreboards differ only because their policies differ.
    """
    rng = random.Random(config.seed)
    rates = tuple(rate for _cid, _headline, rate in CREATIVES)

    return tuple(
        tuple(int(rng.random() < rate) for rate in rates)
        for _visitor in range(config.impressions)
    )


def smoothed_rate(tally: tuple[int, int]) -> float:
    """Laplace-smoothed conversion rate, so an untried creative scores 0.5."""
    shown, converted = tally

    return (converted + 1) / (shown + 2)


def pick_even(visitor_index: int) -> str:
    """Fixed round-robin split on the visitor index.

    The foil — not because it skips the read (every branch reads) but because it
    ignores what the read said.
    """
    return CREATIVE_IDS[visitor_index % len(CREATIVE_IDS)]


def pick_greedy(tallies: Mapping[str, tuple[int, int]]) -> str:
    """Best smoothed rate so far; among ties, the lowest ``creative_id``.

    Ranks over ``CREATIVE_IDS`` rather than over ``tallies``' keys, so a creative
    missing from the read counts as untried instead of becoming unreachable.
    """
    return sorted(
        CREATIVE_IDS,
        key=lambda cid: (-smoothed_rate(tallies.get(cid, (0, 0))), cid),
    )[0]


def pick_epsilon(
    tallies: Mapping[str, tuple[int, int]], rng: random.Random, epsilon: float
) -> str:
    """Explore with probability ``epsilon``, else exploit."""
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
    if policy_name == "even":
        return pick_even(visitor_index)

    if policy_name == "greedy":
        return pick_greedy(tallies)

    if policy_name == "epsilon":
        return pick_epsilon(tallies, rng, epsilon)

    msg = f"unknown policy {policy_name!r}, expected one of {POLICY_NAMES}"
    raise ValueError(msg)


# --- setup, over gRPC --------------------------------------------------------


def seed_prod(client: PencaClient) -> ProdContext:
    """Create the prod catalog, the two tables, and commit the zeroed tallies."""
    catalog_name = f"prod_{uuid4().hex[:8]}"
    catalog_uuid, main_uuid = client.create_catalog(catalog_name, AUTHOR)

    try:
        schema_uuid = client.create_schema(
            SCHEMA_NAME,
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
            author=AUTHOR,
            comment="create ads schema",
        )
        table_uuids = {
            name: client.create_table(
                name,
                schema,
                primary_keys=[primary_key],
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_uuid,
                author=AUTHOR,
                comment=f"create {name} table",
            )
            for name, schema, primary_key in (
                ("creatives", CREATIVES_SCHEMA, "creative_id"),
                ("impressions", IMPRESSIONS_SCHEMA, "visitor_id"),
            )
        }

        # One row per creative at zero, so every later write is an in-place
        # update of an existing row rather than a first insert.
        zeroed = pa.table(
            {
                "creative_id": list(CREATIVE_IDS),
                "headline": [HEADLINES[cid] for cid in CREATIVE_IDS],
                "impressions": [0] * len(CREATIVE_IDS),
                "conversions": [0] * len(CREATIVE_IDS),
            },
            schema=CREATIVES_SCHEMA,
        )
        tx = client.begin_tx(
            catalog_uuid=catalog_uuid,
            branch_uuid=main_uuid,
            author=AUTHOR,
            comment="seed creatives with zeroed tallies",
        )
        try:
            client.write_data(
                tx.tx_uuid,
                Mutation(table_uuid=table_uuids["creatives"], upserts=zeroed),
                catalog_uuid=catalog_uuid,
                schema_uuid=schema_uuid,
                branch_uuid=main_uuid,
            )
            seed_seq = client.commit_tx(
                tx.tx_uuid,
                catalog_uuid=catalog_uuid,
                branch_uuid=main_uuid,
            ).commit_seq_num
        except BaseException:
            # An open tx is not inert: until it times out it clamps cold
            # isolation and fences purge/GC on its branch. Guarded so it never
            # replaces the exception that triggered it.
            try:
                client.abort_tx(
                    tx.tx_uuid, catalog_uuid=catalog_uuid, branch_uuid=main_uuid
                )
            except Exception as exc:
                print(f"  (could not abort tx: {exc})")

            raise
    except BaseException:
        # Everything above lives inside the catalog, so dropping it unwinds the
        # whole partial seed. Without this each failed attempt strands a
        # prod_<hex> catalog on a shared stack.
        discard_catalog(client, catalog_uuid)
        raise

    return ProdContext(
        catalog_uuid=catalog_uuid,
        catalog_name=catalog_name,
        main_branch_uuid=main_uuid,
        schema_uuid=schema_uuid,
        creatives_table_uuid=table_uuids["creatives"],
        impressions_table_uuid=table_uuids["impressions"],
        seed_commit_seq_num=seed_seq,
    )


def fork_branches(client: PencaClient, prod: ProdContext) -> dict[str, str]:
    """Fork one branch per policy off ``main``, all at the seed commit.

    Every fork is pinned to the same explicit commit_seq_num, so the three
    branches provably start from identical state.
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
        # A failure on the second or third fork would otherwise leave the
        # earlier ones live.
        discard_branches(client, prod.catalog_uuid, list(created.values()))
        raise

    return created


# --- the round loop, over SQL ------------------------------------------------


def sql_str(value: str) -> str:
    """Quote a SQL string literal.

    Inlined rather than bound because parameterized INSERT ... VALUES does not
    bind on this server yet — TODO(CHA-526). Once it does these become real
    placeholders and this helper goes away.
    """
    escaped = value.replace("'", "''")

    return f"'{escaped}'"


def read_tallies(conn: PencaClient, prod: ProdContext) -> dict[str, tuple[int, int]]:
    """``creative_id -> (shown, converted)`` for whichever branch ``conn`` is on."""
    got = conn.execute_query(
        f"SELECT creative_id, impressions, conversions FROM {prod.creatives}"
    )

    return {
        cid: (shown, converted)
        for cid, shown, converted in zip(
            got.column("creative_id").to_pylist(),
            got.column("impressions").to_pylist(),
            got.column("conversions").to_pylist(),
            strict=True,
        )
    }


def run_rounds(
    prod: ProdContext,
    branches: Mapping[str, PencaClient],
    config: DemoConfig,
    feed: Sequence[Sequence[int]],
    on_round: Callable[[int, str, tuple[tuple[int, str, int], ...]], None] | None,
) -> None:
    """Read → decide → write, one round at a time, on every branch.

    Rounds are the outer loop and branches the inner one, so all three advance
    through the shared feed in lockstep: they see identical traffic and diverge
    only on policy. Everything each branch does here is SQL on its own
    connection.
    """
    # Keyed off the branches actually passed in, not POLICY_NAMES: `branches` is
    # a parameter, so a caller driving a *subset* would otherwise KeyError here
    # rather than running the subset. A name that is not a policy at all still
    # fails, but in choose_creative, which says so.
    rngs = {name: random.Random(f"{config.seed}:{name}") for name in branches}

    for round_index, start in enumerate(
        range(0, config.impressions, config.round_size)
    ):
        visitors = range(start, min(start + config.round_size, config.impressions))

        for policy_name, conn in branches.items():
            # 1. Read this branch's own committed tallies. Before BEGIN on
            #    purpose: reading inside the open transaction would take the
            #    slower read-your-own-uncommitted-writes path and buy us nothing.
            tallies = read_tallies(conn, prod)

            # 2. Decide, from what we just read. `even` ignores the content and
            #    splits on the visitor index — that is what makes it the foil.
            outcomes = []
            for visitor in visitors:
                creative_id = choose_creative(
                    policy_name, visitor, tallies, rngs[policy_name], config.epsilon
                )
                outcomes.append(
                    (
                        visitor,
                        creative_id,
                        feed[visitor][CREATIVE_POSITION[creative_id]],
                    )
                )

            # 3. Fold the round into the running totals. The tally is cumulative,
            #    so writing it is a read-modify-write — read-your-writes is not
            #    optional here, it is the only way the number can be right.
            updated = dict(tallies)
            for _visitor, creative_id, converted in outcomes:
                shown, hits = updated.get(creative_id, (0, 0))
                updated[creative_id] = (shown + 1, hits + converted)

            served = sorted({creative_id for _v, creative_id, _c in outcomes})
            tally_rows = ", ".join(
                f"({sql_str(cid)}, {sql_str(HEADLINES[cid])}, "
                f"{updated[cid][0]}, {updated[cid][1]})"
                for cid in served
            )
            log_rows = ", ".join(
                f"({sql_str(f'v{visitor:06d}')}, {sql_str(creative_id)}, {converted})"
                for visitor, creative_id, converted in outcomes
            )

            # 4. Both tables, one transaction — so a scoreboard read can never
            #    catch the tally and the log disagreeing.
            conn.execute_update("BEGIN")
            try:
                conn.execute_update(
                    f"INSERT INTO {prod.creatives} "
                    "(creative_id, headline, impressions, conversions) "
                    f"VALUES {tally_rows} "
                    "ON CONFLICT (creative_id) DO UPDATE SET "
                    "impressions = EXCLUDED.impressions, "
                    "conversions = EXCLUDED.conversions"
                )
                conn.execute_update(
                    f"INSERT INTO {prod.impressions} "
                    f"(visitor_id, creative_id, converted) VALUES {log_rows}"
                )
                conn.execute_update("COMMIT")
            except BaseException:
                # Roll back rather than leave the transaction open to time out,
                # but never let the rollback replace the error that caused it.
                try:
                    conn.execute_update("ROLLBACK")
                except Exception as exc:
                    print(f"  (could not roll back on {policy_name}: {exc})")

                raise

            if on_round is not None:
                on_round(round_index, policy_name, tuple(outcomes))


def score_branch(
    conn: PencaClient, prod: ProdContext, name: str, branch_uuid: str
) -> BranchOutcome:
    """Final tallies, reconciled against the branch's own append-only log.

    The reconciliation is aggregated server-side rather than by pulling 3000
    rows back — the engine answering it analytically, on the same copy it just
    transacted against, is the thing worth showing.
    """
    per_creative = read_tallies(conn, prod)
    log = conn.execute_query(
        "SELECT count(*) AS rows, coalesce(sum(converted), 0) AS conversions "
        f"FROM {prod.impressions}"
    )

    return BranchOutcome(
        branch_name=name,
        branch_uuid=branch_uuid,
        impressions=sum(shown for shown, _ in per_creative.values()),
        conversions=sum(converted for _, converted in per_creative.values()),
        per_creative=per_creative,
        log_conversions=log.column("conversions")[0].as_py(),
        log_impressions=log.column("rows")[0].as_py(),
    )


# --- cleanup -----------------------------------------------------------------


def discard_branches(
    client: PencaClient, catalog_uuid: str, branch_uuids: Sequence[str]
) -> list[str]:
    """Delete every branch, attempting all of them; return the ones that failed.

    Guarded per branch: one failure must not strand the rest, and when this runs
    from a ``finally`` an unguarded raise would replace the exception being
    propagated.
    """
    failed: list[str] = []
    for branch_uuid in branch_uuids:
        try:
            client.delete_branch(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
        except Exception as exc:
            print(f"  (could not delete branch {branch_uuid}: {exc})")
            failed.append(branch_uuid)

    return failed


def discard_catalog(client: PencaClient, catalog_uuid: str) -> None:
    """Best-effort delete, for unwinding a half-built seed."""
    try:
        client.delete_catalog(catalog_uuid=catalog_uuid)
    except Exception as exc:
        print(f"  (could not delete catalog {catalog_uuid}: {exc})")


# --- the whole run -----------------------------------------------------------


def close_connections(branches: Mapping[str, PencaClient]) -> None:
    """Release every branch connection, reporting rather than raising.

    Each connection holds a Flight SQL session; closing is best-effort because
    this runs from unwind paths where the exception in flight is the one worth
    propagating.
    """
    for branch_name, conn in branches.items():
        try:
            conn.close()
        except Exception as exc:
            print(f"  (could not close the {branch_name} connection: {exc})")


def connect_to_branch(catalog_name: str, branch_name: str) -> PencaClient:
    """A Flight SQL connection pinned to one branch of one catalog."""
    return PencaClient.from_settings(catalog=catalog_name, branch=branch_name)


def run_demo(
    client: PencaClient,
    config: DemoConfig,
    on_round: Callable[[int, str, tuple[tuple[int, str, int], ...]], None]
    | None = None,
    connect: Callable[[str, str], PencaClient] = connect_to_branch,
) -> DemoOutcome:
    """Fork, drive the shared feed through every branch, score, then discard."""
    feed = build_visitor_feed(config)
    prod = seed_prod(client)
    branch_uuids: dict[str, str] = {}
    branches: dict[str, PencaClient] = {}
    try:
        forked_at = time.perf_counter()
        branch_uuids = fork_branches(client, prod)
        forked_ms = (time.perf_counter() - forked_at) * 1000
        # The differentiated claim, measured on the reader's own machine rather
        # than asserted: forking is a metadata operation, so it is fast and it
        # copies nothing — the forks read main's storage until they write their
        # own. No byte figure, because the client has no segment API and this
        # example will not reach into Postgres to get one;
        # tests/integration/integration_branch_demo_test.py proves the no-copy
        # half directly (main holds exactly one object, each fork holds zero).
        print(
            f"Forked {len(branch_uuids)} branches off main in {forked_ms:.0f}ms — "
            f"no data copied.\n"
            f"Each one starts from main's committed rows and shares its storage "
            f"until it writes.\n"
        )
        # One pinned connection per branch: three branches is literally three
        # endpoints, which is the shape a reader would use. Built by assignment
        # rather than a comprehension so that a failure on the second or third
        # connect leaves the earlier ones bound in `branches`, where the unwind
        # below can close them — a comprehension discards the whole dict.
        for policy_name in POLICY_NAMES:
            branches[policy_name] = connect(prod.catalog_name, policy_name)
    except BaseException:
        close_connections(branches)
        # Setup, so leave no debris: nothing has been demonstrated yet, and a
        # connection failure is exactly what a misconfigured reader hits — one
        # stranded catalog per attempt would pile up fast. Contrast the rounds
        # below, where a failure deliberately KEEPS the catalog, because by then
        # prod holds committed state that is the subject of the demo.
        discard_branches(client, prod.catalog_uuid, list(branch_uuids.values()))
        discard_catalog(client, prod.catalog_uuid)
        raise

    completed = False
    try:
        run_rounds(prod, branches, config, feed, on_round)
        scoreboard = sorted(
            (
                score_branch(branches[name], prod, name, branch_uuids[name])
                for name in POLICY_NAMES
            ),
            key=lambda outcome: (-outcome.conversions, outcome.branch_name),
        )
        completed = True
    finally:
        close_connections(branches)
        # finally, not straight-line: a failed run is exactly when leaving three
        # live branches behind would hurt most. The prod_* catalog deliberately
        # survives — prod outliving its forks is the thing being demonstrated.
        undeleted = discard_branches(
            client, prod.catalog_uuid, list(branch_uuids.values())
        )
        # Only swallow while unwinding. On a green run a failed delete must not
        # degrade to a printed line: the demo would exit 0 with live forks while
        # claiming below that all three were thrown away.
        if undeleted and completed:
            msg = (
                f"the run succeeded but these branches could not be discarded: "
                f"{', '.join(undeleted)} — still live in catalog "
                f"{prod.catalog_uuid}, delete them by hand"
            )
            raise RuntimeError(msg)

    # Main is read AFTER the discard on purpose: read before it, "prod
    # untouched" only covers the run; read after, it also covers main's one
    # shared cold object surviving the deletion of three forks that read it.
    main_conn = connect(prod.catalog_name, "main")
    try:
        main_log = main_conn.execute_query(
            f"SELECT count(*) AS rows FROM {prod.impressions}"
        )
        main_tallies = read_tallies(main_conn, prod)
        remaining = tuple(
            sorted(
                branch.branch_name
                for branch in client.list_branches(catalog_uuid=prod.catalog_uuid)
            )
        )
    finally:
        close_connections({"main": main_conn})

    return DemoOutcome(
        catalog_uuid=prod.catalog_uuid,
        scoreboard=tuple(scoreboard),
        main_tallies=main_tallies,
        main_impression_rows=main_log.column("rows")[0].as_py(),
        remaining_branches=remaining,
    )


# --- output ------------------------------------------------------------------


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
    if branch.impressions == 0:
        return "n/a"

    return f"{branch.conversions / branch.impressions:.2%}"


def _markdown(columns: dict[str, list]) -> str:
    return pa.table(columns).to_pandas().to_markdown(index=False)


def print_scoreboard(outcome: DemoOutcome) -> None:
    print("\n--- Cross-branch scoreboard (ranked) ---")
    print(
        _markdown(
            {
                "branch": [b.branch_name for b in outcome.scoreboard],
                "impressions": [b.impressions for b in outcome.scoreboard],
                "conversions": [b.conversions for b in outcome.scoreboard],
                "rate": [conversion_rate(b) for b in outcome.scoreboard],
            }
        )
    )

    print("\n--- Where each branch spent its traffic ---")
    rows = [
        (branch.branch_name, cid, shown, converted)
        for branch in outcome.scoreboard
        # creative_id breaks ties: `even` shows all four equally often, and
        # without it their order comes from whatever row order the read
        # returned, so identical input could print differently.
        for cid, (shown, converted) in sorted(
            branch.per_creative.items(), key=lambda item: (-item[1][0], item[0])
        )
    ]
    print(
        _markdown(
            {
                "branch": [row[0] for row in rows],
                "creative": [row[1] for row in rows],
                "impressions": [row[2] for row in rows],
                "conversions": [row[3] for row in rows],
            }
        )
    )


def print_isolation(outcome: DemoOutcome) -> None:
    print("\n--- prod (main) after the run ---")
    creative_ids = sorted(outcome.main_tallies)
    print(
        _markdown(
            {
                "creative": creative_ids,
                "impressions": [outcome.main_tallies[c][0] for c in creative_ids],
                "conversions": [outcome.main_tallies[c][1] for c in creative_ids],
            }
        )
    )
    print(f"\nprod impression log: {outcome.main_impression_rows} rows")
    # Name the catalog: it deliberately outlives the run, so a reader who wants
    # to poke at it — or clean it up — needs to know what it is called.
    print(f"prod catalog (kept): {outcome.catalog_uuid}")
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
    # *after* the catalog and three branches exist, so a typo'd flag would leave
    # debris behind a stack trace.
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
