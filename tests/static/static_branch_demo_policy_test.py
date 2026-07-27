"""Static checks for the launch demo's policy layer and SQL wiring (CHA-517).

``examples/branch_demo.py``'s decision layer is infra-free, but the only other
tests that touch it are Docker-gated integration tests — and branch-PR CI skips
the integration job (it is merge-queue only). So without these, nothing that
runs before a merge covers the policies at all.

Scope is the Penca-owned decision logic and the shape of the SQL the round loop
sends: the tie-break that keeps a run reproducible, the prior that drives
exploration off evidence rather than off id ordering, the fixed-split foil, the
round pipeline, the transaction envelope, the cleanup helpers' promise not to
mask the exception they are unwinding, and the CLI's input validation.

Deliberately NOT covered: the printers' formatting beyond one smoke check. They
are a launch cosmetic — a broken one is visible in the first second of running
the demo, which is not a failure mode worth a mutation-tested assertion each.

No Docker, no fixtures, no penca services — runs under ``just static-test
branch_demo_policy`` and ``just check``.
"""

from __future__ import annotations

import importlib.util
import random
import sys
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa

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


def _all_measured() -> dict[str, tuple[int, int]]:
    """Every creative has real exposure, ``carousel`` clearly best.

    Comparing *measured* rates needs all four exposed: an untried creative scores
    0.5 and would rightly win.
    """
    return {"banner": (50, 2), "carousel": (50, 12), "story": (50, 7), "video": (50, 1)}


# --- the decision layer ------------------------------------------------------


def test_untried_outranks_a_creative_with_real_exposure():
    """Once a creative has meaningful exposure it cannot outrank an untried one,
    so exploration follows evidence. Deliberately not the strong form: a creative
    whose single first impression converts scores 0.667, above the untried 0.5."""
    untried = demo.smoothed_rate((0, 0))
    assert untried == 0.5
    assert demo.smoothed_rate((50, 11)) < untried
    assert demo.smoothed_rate((100, 22)) < untried


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


def test_choose_creative_routes_epsilon_to_the_explorer():
    """epsilon must not collapse to greedy in the dispatcher.

    carousel is clearly best here, so a dispatch that fell through to pick_greedy
    would return it every time regardless of the rng.
    """
    explored = {
        demo.choose_creative("epsilon", 0, _all_measured(), random.Random(seed), 1.0)
        for seed in range(40)
    }
    assert len(explored) > 1, f"always-explore must reach more than one, saw {explored}"


def test_unknown_policy_fails_fast():
    try:
        demo.choose_creative("bandit", 0, _tallies(), random.Random(0), 0.1)
    except ValueError as exc:
        assert "bandit" in str(exc)
    else:
        raise AssertionError("an unknown policy name must raise ValueError")


def test_the_best_creative_is_not_the_lowest_id():
    """CREATIVES' own stated invariant, which every other test derives from.

    min(rates), not CREATIVE_IDS[0]: pick_greedy ties on the creative_id key, so
    from all-zero tallies it takes the lexicographically lowest id, which is what
    must not also be the winner. Position and minimum coincide only because
    CREATIVES happens to be listed alphabetically.
    """
    rates = {creative_id: rate for creative_id, _headline, rate in demo.CREATIVES}
    best = max(rates, key=lambda creative_id: rates[creative_id])

    assert best != min(rates), (
        f"the best creative must not also be the lowest id, saw {best} with {rates}"
    )
    assert len(set(rates.values())) == len(rates), (
        f"the rates must stay distinct or 'the best' is ambiguous, saw {rates}"
    )


# --- the round pipeline ------------------------------------------------------


def test_allocate_round_calls_the_policy_once_per_visitor_in_order():
    """The rng-consumption contract epsilon's reproducibility rests on."""
    seen: list[int] = []
    original = demo.choose_creative
    demo.choose_creative = lambda _p, visitor, _t, _r, _e: (
        seen.append(visitor) or demo.CREATIVE_IDS[0]
    )
    try:
        demo.allocate_round(
            "greedy", [3, 4, 5], _tallies(), [[0] * 4] * 6, random.Random(0), 0.0
        )
    finally:
        demo.choose_creative = original

    assert seen == [3, 4, 5], f"one call per visitor, in visitor order; saw {seen}"


def test_allocate_round_reads_each_visitors_own_latent_outcome():
    """The feed is per-visitor-per-creative, so the outcome must be indexed by
    both — not by the visitor alone, which would make every creative convert
    identically and erase the policies' whole reason to differ."""
    feed = [[0, 1, 0, 0], [0, 1, 0, 0]]
    outcomes = demo.allocate_round(
        "even", [0, 1], _tallies(), feed, random.Random(0), 0.0
    )

    converted = {creative: outcome for _v, creative, outcome in outcomes}
    assert converted[demo.CREATIVE_IDS[0]] == 0
    assert converted[demo.CREATIVE_IDS[1]] == 1


def test_apply_outcomes_folds_onto_a_copy():
    """The fold must not mutate the tallies the round was decided against."""
    before = _tallies(banner=(10, 2))
    updated = demo.apply_outcomes(before, ((0, "banner", 1),))

    assert updated["banner"] == (11, 3)
    assert before["banner"] == (10, 2), "the input tallies must not be mutated"


def test_policy_rngs_are_independent_of_the_feeds_stream():
    """Seeded on a "seed:name" string, not seed + index.

    A bare offset gives index 0 the same stream build_visitor_feed's int seed
    produces, so a reading policy at that position would explore against the very
    outcomes that stream generated.
    """
    config = demo.DemoConfig(impressions=8, round_size=2, epsilon=0.5, seed=7)
    rngs = demo.policy_rngs(config)

    assert set(rngs) == set(demo.POLICY_NAMES)
    streams = [[rngs[name].random() for _ in range(5)] for name in demo.POLICY_NAMES]
    assert len({tuple(stream) for stream in streams}) == len(streams), (
        "each policy must draw its own stream"
    )
    feed_first = random.Random(config.seed).random()
    assert all(stream[0] != feed_first for stream in streams), (
        "no policy may replay the stream that built the feed"
    )


def test_the_feed_is_shared_and_reproducible():
    config = demo.DemoConfig(impressions=16, round_size=4, epsilon=0.1, seed=99)

    assert demo.build_visitor_feed(config) == demo.build_visitor_feed(config)
    assert len(demo.build_visitor_feed(config)) == config.impressions


# --- the SQL the round loop sends -------------------------------------------


class _FakeSql:
    """A Flight SQL connection that records statements instead of sending them.

    Records into one shared ordered list so the *interleaving* is assertable —
    that the SELECT precedes BEGIN, that COMMIT closes the block — which is the
    property no outcome assertion can reach.
    """

    def __init__(self, calls, branch, tallies=None, fail_on=None):
        self.calls = calls
        self.branch = branch
        self.tallies = tallies or dict.fromkeys(demo.CREATIVE_IDS, (0, 0))
        self.fail_on = fail_on

    def execute_query(self, sql):
        self.calls.append((self.branch, "query", sql))
        if "count(*)" in sql:
            return pa.table({"rows": [0], "conversions": [0]})

        ids = sorted(self.tallies)

        return pa.table(
            {
                "creative_id": ids,
                "impressions": [self.tallies[c][0] for c in ids],
                "conversions": [self.tallies[c][1] for c in ids],
            }
        )

    def execute_update(self, sql):
        self.calls.append((self.branch, "update", sql))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"refused: {self.fail_on}")

        return 1


class _FakeAdmin:
    """The gRPC half — setup, forking and cleanup only."""

    def __init__(self, *, fail_create_branch_on_call=None, fail_delete_branch=None):
        self.calls: list[str] = []
        self.deleted: list[str | None] = []
        self.deleted_catalogs: list[str | None] = []
        self.branches = 0
        self.fail_create_branch_on_call = fail_create_branch_on_call
        self.fail_delete_branch = fail_delete_branch

    def create_catalog(self, _name, _author):
        self.calls.append("create_catalog")

        return ("cat", "uuid-main")

    def create_schema(self, *_a, **_k):
        self.calls.append("create_schema")

        return "schema"

    def create_table(self, name, *_a, **_k):
        self.calls.append("create_table")

        return f"tbl-{name}"

    def begin_tx(self, **_k):
        self.calls.append("begin_tx")

        return SimpleNamespace(tx_uuid="tx")

    def write_data(self, *_a, **_k):
        self.calls.append("write_data")

    def commit_tx(self, *_a, **_k):
        self.calls.append("commit_tx")

        return SimpleNamespace(commit_seq_num=1)

    def abort_tx(self, *_a, **_k):
        self.calls.append("abort_tx")

    def create_branch(self, name, *_a, **_k):
        self.branches += 1
        self.calls.append(f"create_branch:{name}")
        if self.fail_create_branch_on_call == self.branches:
            raise RuntimeError("fork refused")

        return SimpleNamespace(branch_uuid=f"uuid-{name}")

    def delete_branch(self, *, branch_uuid=None, **_k):
        self.calls.append(f"delete_branch:{branch_uuid}")
        if self.fail_delete_branch == branch_uuid:
            raise RuntimeError("delete refused")

        self.deleted.append(branch_uuid)

    def delete_catalog(self, *, catalog_uuid=None, **_k):
        self.calls.append("delete_catalog")
        self.deleted_catalogs.append(catalog_uuid)

    def list_branches(self, **_k):
        self.calls.append("list_branches")

        return [SimpleNamespace(branch_name="main")]


def _prod():
    return demo.ProdContext(
        catalog_uuid="cat",
        catalog_name="prod_test",
        main_branch_uuid="uuid-main",
        schema_uuid="schema",
        creatives_table_uuid="creatives",
        impressions_table_uuid="impressions",
        seed_commit_seq_num=1,
    )


def test_a_round_selects_before_it_opens_the_transaction():
    """CHA-517's hard design rule, as an interaction assertion.

    No outcome assertion can pin this. apply_outcomes computes exactly what the
    read returns, so replacing the per-round SELECT with an in-process dict
    carried across rounds yields a byte-identical scoreboard — read-your-writes
    deleted outright, every other test still green. What separates the two is
    observable only in the interaction: that a SELECT happens, that it precedes
    BEGIN, and that the allocation used its *content*.

    So the fake serves a tally the policy could not reach from zeros — carousel
    measured clearly best, everything else worse. From zeros, greedy takes banner
    on the untried tie-break.
    """
    calls: list[tuple[str, str, str]] = []
    session = demo.BranchSession(
        name="greedy",
        uuid="uuid-greedy",
        sql=_FakeSql(calls, "greedy", tallies=_all_measured()),
    )

    demo.drive_round(session, _prod(), [0], [[0, 0, 0, 0]], random.Random(0), 0.0)

    verbs = [
        "SELECT" if kind == "query" else sql.split()[0].upper()
        for _b, kind, sql in calls
    ]
    assert verbs == ["SELECT", "BEGIN", "INSERT", "INSERT", "COMMIT"], (
        f"the round must SELECT committed state before BEGIN; saw {calls}"
    )

    upsert = next(sql for _b, _k, sql in calls if "ON CONFLICT" in sql)
    assert "'carousel'" in upsert, (
        "the allocation must come from what the SELECT returned — carousel is "
        f"best only in the served tallies, and from zeros greedy takes banner. "
        f"Saw {upsert}"
    )


def test_a_failed_statement_rolls_the_transaction_back():
    """An open transaction is not inert: until it times out it clamps cold
    isolation and fences purge/GC on its branch. So a mid-block failure must
    ROLLBACK — and must still propagate its own error, not the rollback's."""
    calls: list[tuple[str, str, str]] = []
    session = demo.BranchSession(
        name="greedy",
        uuid="uuid-greedy",
        sql=_FakeSql(calls, "greedy", fail_on="ON CONFLICT"),
    )

    try:
        demo.commit_statements(
            session, ("INSERT INTO t ON CONFLICT x", "INSERT INTO u")
        )
    except RuntimeError as exc:
        assert "refused" in str(exc), f"the statement's own error must survive: {exc}"
    else:
        raise AssertionError("the failure must propagate")

    verbs = [sql.split()[0].upper() for _b, _k, sql in calls]
    assert verbs == ["BEGIN", "INSERT", "ROLLBACK"], (
        f"a failed statement must roll back and skip the rest; saw {verbs}"
    )


def test_the_upsert_carries_only_the_touched_creatives():
    """Deriving the keys from `outcomes` is what keeps the payload honest — a
    round that served one creative must not rewrite the other three's totals."""
    sql = demo.tally_upsert_sql(
        _prod(), ((0, demo.CREATIVE_IDS[1], 1),), {demo.CREATIVE_IDS[1]: (5, 2)}
    )

    assert f"'{demo.CREATIVE_IDS[1]}'" in sql
    for untouched in (demo.CREATIVE_IDS[0], demo.CREATIVE_IDS[2]):
        assert f"'{untouched}'" not in sql, f"{untouched} was not served; saw {sql}"

    assert "ON CONFLICT (creative_id) DO UPDATE" in sql, sql


def test_sql_str_escapes_a_quote():
    assert demo.sql_str("it's") == "'it''s'"


# --- run_demo's wiring -------------------------------------------------------


def _small_config():
    return demo.DemoConfig(impressions=4, round_size=2, epsilon=0.0, seed=1)


def _connect_factory(calls, tallies=None, fail_on=None):
    return lambda _catalog, branch: _FakeSql(calls, branch, tallies, fail_on)


def test_run_demo_forks_drives_scores_then_discards_in_that_order():
    """The wiring, which no outcome assertion reaches.

    Pins that every fork exists before any is discarded, and that list_branches
    is consulted after the deletes — otherwise "branches remaining" reports the
    pre-discard state.
    """
    admin = _FakeAdmin()
    outcome = demo.run_demo(admin, _small_config(), connect=_connect_factory([]))

    forks = [i for i, c in enumerate(admin.calls) if c.startswith("create_branch")]
    deletes = [i for i, c in enumerate(admin.calls) if c.startswith("delete_branch")]
    assert len(forks) == len(demo.POLICY_NAMES)
    assert len(deletes) == len(demo.POLICY_NAMES)
    assert max(forks) < min(deletes), "every fork exists before any is discarded"
    assert min(deletes) < admin.calls.index("list_branches"), (
        "list_branches must observe the post-discard state"
    )
    assert outcome.remaining_branches == ("main",)
    assert {branch.branch_name for branch in outcome.scoreboard} == set(
        demo.POLICY_NAMES
    )


def test_run_demo_reports_every_branch_round_to_the_callback():
    """on_round must fire per branch per round, with that round's own outcomes.

    Nothing else reaches it: run_demo defaults it to None. Silencing print_round,
    dropping the call, mispairing the arguments, or hoisting the callback out of
    the per-branch loop all survived otherwise.
    """
    seen: list[tuple[int, str, tuple[str, ...]]] = []
    # Non-dividing on purpose: 5 impressions at 2 per round makes the final round
    # partial, so run_demo's clamp is on the executed path. At 4/2 it is
    # unreachable and a run_demo with it deleted stays green.
    config = demo.DemoConfig(impressions=5, round_size=2, epsilon=0.0, seed=1)

    demo.run_demo(
        _FakeAdmin(),
        config,
        on_round=lambda index, policy, outcomes: seen.append(
            (index, policy, tuple(creative for _v, creative, _o in outcomes))
        ),
        connect=_connect_factory([]),
    )

    # `seen` is fully determined by the fixture, so pin it whole. greedy and
    # epsilon both take the lexicographically lowest id from all-zero tallies;
    # `even` round-robins on the visitor index, so its payload differs per round.
    rounds = -(-config.impressions // config.round_size)
    expected = []
    for index in range(rounds):
        start = index * config.round_size
        visitors = range(start, min(start + config.round_size, config.impressions))
        tied = (min(demo.CREATIVE_IDS),) * len(visitors)
        even = tuple(
            demo.CREATIVE_IDS[visitor % len(demo.CREATIVE_IDS)] for visitor in visitors
        )
        for policy in demo.POLICY_NAMES:
            expected.append((index, policy, even if policy == "even" else tied))

    assert seen == expected, f"callbacks must arrive exactly so;\n saw {seen}"


def test_run_demo_drops_the_catalog_when_a_fork_fails():
    """fork_branches discards the forks it made; only run_demo's wrapper drops
    the catalog, so deleting that leaves a prod_<hex> behind on every failed
    fork."""
    admin = _FakeAdmin(fail_create_branch_on_call=2)

    try:
        demo.run_demo(admin, _small_config(), connect=_connect_factory([]))
    except RuntimeError:
        pass
    else:
        raise AssertionError("the fork failure must propagate")

    assert admin.deleted_catalogs == ["cat"], (
        f"a failed fork must not strand the catalog, saw {admin.deleted_catalogs}"
    )
    assert admin.deleted == [f"uuid-{demo.POLICY_NAMES[0]}"], (
        f"the fork created before the failure must be discarded, saw {admin.deleted}"
    )


def test_a_green_run_that_cannot_discard_fails_loudly():
    """raise_for_undeleted must actually be called.

    Deleting the call site leaves the helper's own test green while the demo
    exits 0 with live forks — and print_isolation would list them right above
    "N parallel universes ran against it and were thrown away".
    """
    admin = _FakeAdmin(fail_delete_branch=f"uuid-{demo.POLICY_NAMES[0]}")

    try:
        demo.run_demo(admin, _small_config(), connect=_connect_factory([]))
    except RuntimeError as exc:
        assert "could not be discarded" in str(exc), exc
        assert demo.POLICY_NAMES[0] in str(exc), exc
    else:
        raise AssertionError("a green run with an undeleted branch must raise")


def test_a_failed_round_propagates_its_own_error_not_the_discard_error():
    """The `completed` flag must start False.

    Initialised True, a run that dies mid-round and then also fails a delete
    reports "could not discard branches" instead of the failure that actually
    ended it — cleanup masking the cause, which is the whole reason for the flag.
    """
    admin = _FakeAdmin(fail_delete_branch=f"uuid-{demo.POLICY_NAMES[0]}")

    try:
        demo.run_demo(
            admin, _small_config(), connect=_connect_factory([], fail_on="ON CONFLICT")
        )
    except RuntimeError as exc:
        assert "refused" in str(exc), f"the round failure must survive cleanup: {exc}"
    else:
        raise AssertionError("the round failure must propagate")


# --- cleanup helpers ---------------------------------------------------------


def test_discard_branches_attempts_every_branch_and_reports_the_failures():
    """One failure must not strand the rest, and the failures must come back
    rather than being swallowed — the caller decides what they mean."""
    admin = _FakeAdmin(fail_delete_branch="uuid-b")
    failed = demo.discard_branches(admin, "cat", ["uuid-a", "uuid-b", "uuid-c"])

    assert failed == ["uuid-b"]
    assert admin.deleted == ["uuid-a", "uuid-c"], (
        f"a failed delete must not stop the others, saw {admin.deleted}"
    )


def test_cleanup_helpers_let_a_second_ctrl_c_through():
    """`except Exception`, not `except BaseException`: a user hitting Ctrl-C
    during cleanup must not have it swallowed by the cleanup."""

    class _Interrupts:
        def delete_branch(self, **_k):
            raise KeyboardInterrupt

        def delete_catalog(self, **_k):
            raise KeyboardInterrupt

    for call in (
        lambda: demo.discard_branches(_Interrupts(), "cat", ["uuid-a"]),
        lambda: demo.discard_catalog(_Interrupts(), "cat"),
    ):
        try:
            call()
        except KeyboardInterrupt:
            continue

        raise AssertionError("KeyboardInterrupt must propagate through cleanup")


def test_seed_prod_drops_the_catalog_it_created_when_setup_fails():
    """Everything below create_catalog lives inside the catalog, so dropping it
    unwinds the whole partial seed. Without this each failed attempt strands
    another prod_<hex> on a shared stack."""

    class _FailsAtSchema(_FakeAdmin):
        def create_schema(self, *_a, **_k):
            raise RuntimeError("schema refused")

    admin = _FailsAtSchema()
    try:
        demo.seed_prod(admin)
    except RuntimeError:
        pass
    else:
        raise AssertionError("the setup failure must propagate")

    assert admin.deleted_catalogs == ["cat"], (
        f"a failed seed must drop its catalog, saw {admin.deleted_catalogs}"
    )


# --- the CLI -----------------------------------------------------------------


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
    assert config.epsilon == demo.DEFAULT_EPSILON
    assert config.seed == demo.DEFAULT_SEED

    # Two degenerate shapes a plausible edit reaches: a round size at or above
    # the impression count collapses the run to one decision (no read-your-writes
    # loop at all), and epsilon at 0 collapses that branch into greedy, erasing
    # the third policy from the scoreboard.
    assert demo.DEFAULT_ROUND_SIZE < demo.DEFAULT_IMPRESSIONS
    assert 0.0 < demo.DEFAULT_EPSILON < 1.0


def test_parse_args_rejects_input_that_would_leave_debris():
    """Validation must precede any RPC: each of these otherwise fails *after*
    seed_prod and fork_branches created a catalog and three branches, so a
    typo'd flag would leave debris behind a stack trace."""
    for argv in (
        ["--impressions", "0"],
        ["--round-size", "0"],
        ["--epsilon", "1.5"],
        ["--epsilon", "-0.1"],
    ):
        try:
            _parse_args_with(argv)
        except SystemExit as exc:
            assert exc.code == 2, f"{argv} must be an argparse usage error"
        else:
            raise AssertionError(f"{argv} must be rejected")


# --- printers (one smoke check; see the module docstring) --------------------


def test_printers_emit_a_scoreboard_and_the_isolation_proof(capsys):
    per_creative = {
        creative_id: (25, index) for index, creative_id in enumerate(demo.CREATIVE_IDS)
    }
    conversions = sum(converted for _shown, converted in per_creative.values())
    outcome = demo.DemoOutcome(
        catalog_uuid="uuid-catalog",
        scoreboard=tuple(
            demo.BranchOutcome(
                branch_name=policy_name,
                branch_uuid=f"uuid-{policy_name}",
                impressions=100,
                conversions=conversions,
                per_creative=per_creative,
                log_conversions=conversions,
                log_impressions=100,
            )
            for policy_name in demo.POLICY_NAMES
        ),
        main_tallies=dict.fromkeys(demo.CREATIVE_IDS, (0, 0)),
        main_impression_rows=0,
        remaining_branches=("main",),
    )

    demo.print_round(0, "epsilon", ((0, demo.CREATIVE_IDS[0], 1),))
    demo.print_scoreboard(outcome)
    demo.print_isolation(outcome)

    printed = capsys.readouterr().out
    assert "scoreboard" in printed.lower()
    for policy_name in demo.POLICY_NAMES:
        assert policy_name in printed

    # The evidence, not only the conclusion: deleting the remaining-branches line
    # left "prod is intact" printing right above nothing.
    assert "branches remaining after discard: main" in printed, printed
    assert "prod is intact" in printed
