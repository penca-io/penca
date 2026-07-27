"""Static checks for the launch demo's policies, printers, and CLI (CHA-517).

``examples/branch_demo.py``'s policy layer is infra-free, but the only other
tests that touch the module are Docker-gated integration tests — and branch-PR
CI skips the integration job (it is merge-queue only). So without these, nothing
that runs before a merge covers the policies at all (roborev finding on
224bf9f). These load the demo by path, the way
``static_kata_plan_html_test.py`` loads its generator, and pin only the
Penca-owned decision logic: the tie-break that keeps a run reproducible, the
prior that drives exploration off evidence rather than off the id ordering, the
fixed-split foil's wraparound, the unknown-policy failure, the round pipeline
(``allocate_round``'s one-call-per-visitor contract and its rng state,
``apply_outcomes``' copy-fold, both upsert payloads), the cleanup helpers' promise
not to mask the exception they are unwinding, and — against a hand-built
``DemoOutcome``, no engine needed — the printers and the CLI's input validation,
the per-policy rng seeding that keeps exploration independent of the stream that
built the feed, and the single-transaction guarantee behind ``commit_mutations``.
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

    # The evidence, not only the conclusion: deleting the remaining-branches line
    # outright left "prod is intact" printing right above nothing.
    assert "branches remaining after discard: main" in printed, printed
    assert "prod is intact" in printed

    # Patch POLICY_NAMES so the derived count differs from the real 3 — comparing
    # len(POLICY_NAMES) on both sides passes for a hardcoded "3". The outcome is
    # rebuilt inside the patched window so it carries two branches too: pinning the
    # printed count against a three-branch outcome would pin the count's *source*
    # (the module global) rather than the property, and would fail an equally
    # derived rewrite to len(outcome.scoreboard).
    original = demo.POLICY_NAMES
    demo.POLICY_NAMES = ("even", "greedy")
    try:
        capsys.readouterr()
        demo.print_isolation(_synthetic_outcome())
        assert "2 parallel universes" in capsys.readouterr().out, (
            "the punchline must derive the branch count, not hardcode it"
        )
    finally:
        demo.POLICY_NAMES = original


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
    # epsilon was the one omitted: default=0.0 passes every other test and
    # collapses the shipped epsilon branch into greedy on the exact command the
    # README tells launch readers to run.
    assert config.epsilon == demo.DEFAULT_EPSILON


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

    # One generator, its draws reused for both halves. Re-constructing
    # random.Random(seed) per iteration would yield [r0, r0, r0, r0] rather than
    # the feed's [r0, r1, r2, r3], and the comparison below could never fail.
    feed_rng = random.Random(config.seed)
    feed_stream = [feed_rng.random() for _ in range(4)]
    expected_first_row = tuple(
        int(draw < rate)
        for draw, (*_head, rate) in zip(feed_stream, demo.CREATIVES, strict=False)
    )
    assert demo.build_visitor_feed(config)[0] == expected_first_row, (
        "the feed is no longer drawn from random.Random(seed) in CREATIVES order; "
        "update this test's model of it before trusting the comparison below"
    )
    for policy_name, stream in streams.items():
        assert stream != feed_stream, (
            f"{policy_name}'s stream must not match the one that built the feed"
        )

    distinct = {tuple(stream) for stream in streams.values()}
    assert len(distinct) == len(demo.POLICY_NAMES), (
        "each policy needs its own stream, or they explore in lockstep"
    )


class _FailingClient:
    """Minimal stand-in: records calls, raises where the test asks it to.

    One predicate per method rather than a single shared ``fail_on`` selector. The
    shared version meant opposite things on different methods — ``None`` was "never
    fail" for one and "always fail" for another — and overloaded the branch-uuid
    namespace with a sentinel string.
    """

    def __init__(
        self,
        raises: BaseException | None = None,
        fail_delete_branch: str | None = None,
        fail_create_schema: bool = False,
        fail_delete_catalog: bool = False,
        fail_abort_tx: bool = False,
        fail_write_data: bool = False,
        fail_create_branch_on_call: int | None = None,
        fail_read_data: bool = False,
        fail_commit_tx: bool = False,
        tallies: dict[str, tuple[int, int]] | None = None,
        branch_tallies: dict[str, dict[str, tuple[int, int]]] | None = None,
    ):
        # A fail_* flag with no exception to raise is a test that pins nothing —
        # the vacuity class this fake exists to avoid, one forgotten kwarg away.
        assert raises is not None or not any(
            (
                fail_delete_branch,
                fail_create_schema,
                fail_delete_catalog,
                fail_abort_tx,
                fail_write_data,
                fail_create_branch_on_call,
                fail_read_data,
                fail_commit_tx,
            )
        ), "a fail_* flag without raises never fails"
        assert tallies is None or branch_tallies is None, (
            "branch_tallies supersedes tallies; pass one"
        )
        # fail_delete_branch is a uuid, not a bool: True would compare equal to no
        # branch and pass green, and the sibling flags being bools makes that slip
        # plausible.
        assert not isinstance(fail_delete_branch, bool), (
            "fail_delete_branch takes a branch uuid, not a bool"
        )
        self.raises = raises
        self.fail_delete_branch = fail_delete_branch
        self.fail_create_schema = fail_create_schema
        self.fail_delete_catalog = fail_delete_catalog
        self.fail_abort_tx = fail_abort_tx
        self.fail_write_data = fail_write_data
        self.fail_create_branch_on_call = fail_create_branch_on_call
        self.fail_read_data = fail_read_data
        self.fail_commit_tx = fail_commit_tx
        # What read_data serves back as the branch's committed tallies.
        # Derived, not hardcoded: a change to CREATIVES would otherwise leave the
        # fake serving run_demo a stale candidate set. Values may be a flat map
        # served to every branch, or keyed on branch_uuid to give each its own —
        # which is what lets a static test give the branches distinct totals.
        self.tallies = tallies or dict.fromkeys(demo.CREATIVE_IDS, (0, 0))
        self.branch_tallies = branch_tallies
        self.created_branches: list[str] = []
        self.fork_seq_nums: list[int | None] = []
        self.last_upserted_creatives: list[str] = []
        self.deleted: list[str] = []
        self.aborted: list[str] = []
        self.deleted_catalogs: list[str] = []
        # One ordered log across all methods. Separate per-method lists cannot pin
        # an ordering claim: two independent lists are satisfied in either order.
        self.calls: list[str] = []

    def _maybe_raise(self, should_fail: bool) -> None:
        if should_fail and self.raises is not None:
            raise self.raises

    def delete_branch(self, catalog_uuid: str, branch_uuid: str) -> None:
        self.deleted.append(branch_uuid)
        self.calls.append(f"delete_branch:{branch_uuid}")
        self._maybe_raise(branch_uuid == self.fail_delete_branch)

    def abort_tx(self, tx_uuid: str, catalog_uuid: str, branch_uuid: str) -> None:
        self.aborted.append(tx_uuid)
        self.calls.append(f"abort_tx:{tx_uuid}")
        self._maybe_raise(self.fail_abort_tx)

    def create_catalog(self, catalog_name: str, owner: str) -> tuple[str, str]:
        return "cat", "main-uuid"

    def create_schema(self, *args, **kwargs) -> str:
        self._maybe_raise(self.fail_create_schema)

        return "schema-uuid"

    def create_table(self, *args, **kwargs) -> str:
        return "table-uuid"

    def begin_tx(self, *args, **kwargs):
        self.calls.append("begin_tx")

        return SimpleNamespace(tx_uuid="tx")

    # mutation is required, mirroring PencaClient.write_data's positional, so a
    # call omitting the payload is a TypeError rather than a green pass.
    def write_data(self, tx_uuid, mutation, *args, **kwargs) -> None:
        # The mutation is in the key: without it, a loop writing mutations[0]
        # twice logs the same line twice and passes, while in the demo that would
        # drop either the tally UPDATE or the log append.
        self.calls.append(f"write_data:{tx_uuid}:{mutation}")
        if getattr(mutation, "table_uuid", None) == "creatives":
            self.last_upserted_creatives = mutation.upserts.column(
                "creative_id"
            ).to_pylist()

        self._maybe_raise(self.fail_write_data)

    def commit_tx(self, tx_uuid, *args, **kwargs):
        self.calls.append(f"commit_tx:{tx_uuid}")
        self._maybe_raise(self.fail_commit_tx)

        return SimpleNamespace(commit_seq_num=1)

    def read_data(self, *args, **kwargs):
        self.calls.append("read_data")
        self._maybe_raise(self.fail_read_data)
        # branch_tallies gives each branch its own map, which is what lets a static
        # test hand the branches distinct totals. A branch it does not name — main,
        # for the isolation read — gets zeros.
        if self.branch_tallies is None:
            served = self.tallies
        else:
            served = self.branch_tallies.get(
                str(kwargs.get("branch_uuid")), dict.fromkeys(demo.CREATIVE_IDS, (0, 0))
            )

        creative_ids = list(served)
        if kwargs.get("columns") == ["creative_id", "converted"]:
            return pa.table(
                {"creative_id": creative_ids, "converted": [0] * len(creative_ids)}
            )

        return pa.table(
            {
                "creative_id": creative_ids,
                "impressions": [served[c][0] for c in creative_ids],
                "conversions": [served[c][1] for c in creative_ids],
            }
        )

    def create_branch(self, branch_name, *args, **kwargs):
        self.calls.append(f"create_branch:{branch_name}")
        self.fork_seq_nums.append(kwargs.get("commit_seq_num"))
        self.created_branches.append(branch_name)
        self._maybe_raise(len(self.created_branches) == self.fail_create_branch_on_call)

        return SimpleNamespace(branch_uuid=f"uuid-{branch_name}")

    def list_branches(self, *args, **kwargs):
        self.calls.append("list_branches")

        return [SimpleNamespace(branch_name="main")]

    def delete_catalog(self, catalog_uuid: str) -> None:
        self.deleted_catalogs.append(catalog_uuid)
        self.calls.append(f"delete_catalog:{catalog_uuid}")
        self._maybe_raise(self.fail_delete_catalog)


def test_discard_branches_attempts_every_branch_and_reports_the_failures():
    """One failure must not strand the branches after it."""
    client = _FailingClient(raises=RuntimeError("boom"), fail_delete_branch="b")
    failed = demo.discard_branches(client, "cat", ["a", "b", "c"])

    assert client.deleted == ["a", "b", "c"], "every branch must be attempted"
    assert failed == ["b"]


def test_discard_branches_reports_nothing_when_every_delete_lands():
    client = _FailingClient()
    assert demo.discard_branches(client, "cat", ["a", "b"]) == []


def test_abort_quietly_does_not_replace_the_exception_being_unwound():
    """A failing abort must not become the error the reader sees."""
    client = _FailingClient(raises=RuntimeError("abort failed"), fail_abort_tx=True)
    demo.abort_quietly(client, "cat", "tx", "branch")
    assert client.aborted == ["tx"]


def test_cleanup_helpers_let_a_second_ctrl_c_through():
    """They catch Exception, not BaseException, so an interrupt still propagates."""
    client = _FailingClient(raises=KeyboardInterrupt(), fail_delete_branch="a")
    try:
        demo.discard_branches(client, "cat", ["a"])
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt must not be swallowed")

    try:
        demo.abort_quietly(
            _FailingClient(raises=KeyboardInterrupt(), fail_abort_tx=True),
            "c",
            "tx",
            "b",
        )
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("KeyboardInterrupt must not be swallowed")


def test_seed_prod_drops_the_catalog_it_created_when_setup_fails():
    """A failed seed must not strand a prod_<hex> catalog on a shared stack."""
    client = _FailingClient(raises=RuntimeError("no schema"), fail_create_schema=True)
    try:
        demo.seed_prod(client)
    except RuntimeError:
        pass
    else:
        raise AssertionError("the setup failure must propagate")

    assert client.deleted_catalogs == ["cat"], (
        "the catalog created before the failure must be dropped"
    )


def test_discard_catalog_does_not_propagate_a_delete_failure():
    """The delete really fails here — otherwise this pins only uuid forwarding."""
    client = _FailingClient(raises=RuntimeError("boom"), fail_delete_catalog=True)
    demo.discard_catalog(client, "cat")
    assert client.deleted_catalogs == ["cat"]


def test_a_green_run_fails_when_a_branch_could_not_be_discarded():
    """The whole point of the flag: exiting 0 with live forks is not acceptable."""
    try:
        demo.raise_for_undeleted(["epsilon"], catalog_uuid="cat", completed=True)
    except RuntimeError as exc:
        assert "epsilon" in str(exc)
        assert "cat" in str(exc), "the catalog must be named so cleanup is doable"
    else:
        raise AssertionError("a completed run with undeleted branches must raise")


def test_an_unwinding_run_keeps_the_original_exception():
    """While unwinding there is a real error to preserve, so stay quiet."""
    demo.raise_for_undeleted(["epsilon"], catalog_uuid="cat", completed=False)
    demo.raise_for_undeleted([], catalog_uuid="cat", completed=True)


def test_allocate_round_calls_the_policy_once_per_visitor_in_order():
    """The rng contract the refactor's docstring leans on, pinned.

    Compares against a reference that drives choose_creative by hand, and then
    compares rng *state* — which is what catches an extra or a missing draw, the
    failure that would silently shift epsilon's stream and rewrite the scoreboard.
    """
    indexes = [2, 0, 1]
    tallies = _all_measured()
    feed = [[0, 1, 0, 1]] * 3

    reference_rng = random.Random("pin")
    expected = tuple(
        (index, demo.choose_creative("epsilon", index, tallies, reference_rng, 0.5))
        for index in indexes
    )

    rng = random.Random("pin")
    got = demo.allocate_round("epsilon", indexes, tallies, feed, rng, 0.5)

    assert tuple((index, creative) for index, creative, _o in got) == expected
    assert rng.getstate() == reference_rng.getstate(), "extra or missing rng draws"


def test_allocate_round_reads_each_visitors_own_latent_outcome():
    """The visitor → row, creative → column coupling.

    Each visitor's own row has exactly one non-zero cell, at a column that is not
    that visitor's loop position: ``feed[5][1]`` and ``feed[0][0]``. So reading the
    row by loop position yields ``feed[0][1] == 0`` for the first and
    ``feed[1][0] == 0`` for the second, and reading the column by visitor index
    yields ``feed[5][5]`` and raises — both mis-indexings a weaker fixture would
    let through, and both elements of the pair can now fail.
    """
    # Row 0 written separately from the `* 4` replication, which aliases one list.
    feed = [[1, 0, 0, 0]] + [[0, 0, 0, 0]] * 4 + [[0, 1, 0, 0]]
    got = demo.allocate_round("even", [5, 0], {}, feed, random.Random(0), 0.0)

    assert got == (
        (5, demo.CREATIVE_IDS[1], 1),
        (0, demo.CREATIVE_IDS[0], 1),
    )


def test_apply_outcomes_folds_onto_a_copy():
    tallies = {"banner": (10, 2)}
    outcomes = [(0, "banner", 1), (1, "banner", 0), (2, "carousel", 1)]

    updated = demo.apply_outcomes(tallies, outcomes)

    assert updated["banner"] == (12, 3)
    assert updated["carousel"] == (1, 1), "absent creative starts from (0, 0)"
    assert tallies == {"banner": (10, 2)}, "the input must not be mutated"


def test_upsert_payloads_carry_only_the_touched_creatives():
    # Visitor indexes deliberately disjoint from their positions. Equal-to-position
    # indexes let a positional `enumerate` read pass — and in the real demo
    # visitor_indexes is a global range (round 2 starts at 25), so a positional read
    # would re-emit v000000.. every round and, since visitor_id is the impressions
    # primary key, silently REPLACE the previous round's log instead of appending.
    outcomes = [(7, "story", 1), (1, "banner", 0), (4, "story", 0)]
    # Fold onto ALL creatives, not {}: with an empty base, updated's keys equal the
    # touched set, so an implementation deriving touched from `updated` instead of
    # from `outcomes` passes — while in the real drive_round updated always holds
    # all four, and that regression would rewrite every creative's row each round.
    updated = demo.apply_outcomes(_tallies(), outcomes)

    tallies = demo.tally_upserts(outcomes, updated)
    assert tallies.column("creative_id").to_pylist() == ["banner", "story"]
    assert tallies.schema == demo.CREATIVES_SCHEMA
    assert dict(
        zip(
            tallies.column("creative_id").to_pylist(),
            tallies.column("impressions").to_pylist(),
            strict=True,
        )
    ) == {"banner": 1, "story": 2}

    assert tallies.column("headline").to_pylist() == [
        demo.HEADLINES["banner"],
        demo.HEADLINES["story"],
    ], "each row must carry its own creative's headline"

    log = demo.impression_upserts(outcomes)
    assert log.schema == demo.IMPRESSIONS_SCHEMA
    assert log.column("visitor_id").to_pylist() == ["v000007", "v000001", "v000004"]
    assert log.column("creative_id").to_pylist() == ["story", "banner", "story"]
    assert log.column("converted").to_pylist() == [1, 0, 0]


def test_a_failed_seed_transaction_aborts_then_drops_the_catalog():
    """Both halves of the unwind, in order, from one seed_prod call.

    The create_schema failure path never reaches the transaction, so without this
    the abort-then-discard ordering is unpinned: deleting commit_mutations'
    except clause leaves the rest of the suite green.
    """
    client = _FailingClient(raises=RuntimeError("write failed"), fail_write_data=True)
    try:
        demo.seed_prod(client)
    except RuntimeError:
        pass
    else:
        raise AssertionError("the write failure must propagate")

    # Whole sequence, not a tail slice: the slice bought ordering but dropped the
    # uuid identity and the exactly-once the two old per-method lists guaranteed.
    # Exactly four calls in this order, with the uuids pinned. The write_data
    # element is matched by prefix only: its mutation is a real Mutation whose repr
    # embeds an entire Arrow table.
    assert len(client.calls) == 4, f"each step exactly once; saw {client.calls}"
    assert client.calls[0] == "begin_tx"
    # "Mutation(" rather than the bare "write_data:tx:" prefix: pins that the seed
    # path passes a real payload, not merely some second argument.
    assert client.calls[1].startswith("write_data:tx:Mutation("), client.calls[1]
    assert client.calls[2:] == ["abort_tx:tx", "delete_catalog:cat"], (
        f"abort the tx, then drop the catalog; saw {client.calls}"
    )


def test_commit_mutations_puts_every_mutation_in_one_transaction():
    """The contract the helper exists for.

    An implementation opening a fresh transaction per mutation (begin, write,
    commit, per element) would pass every other test in this file, and only a
    torn scoreboard read in the integration demo would notice.
    """
    client = _FailingClient()

    seq = demo.commit_mutations(
        client,
        catalog_uuid="cat",
        schema_uuid="schema",
        branch_uuid="branch",
        comment="two tables, one tx",
        mutations=["m1", "m2"],
    )

    assert client.calls == [
        "begin_tx",
        "write_data:tx:m1",
        "write_data:tx:m2",
        "commit_tx:tx",
    ], f"one begin, both mutations on that tx, one commit; saw {client.calls}"
    assert seq == 1, "returns the commit's seq num"


def _small_config():
    return demo.DemoConfig(impressions=2, round_size=2, epsilon=0.0, seed=1)


def _prod():
    return demo.ProdContext(
        catalog_uuid="cat",
        main_branch_uuid="uuid-main",
        schema_uuid="schema",
        creatives_table_uuid="creatives",
        impressions_table_uuid="impressions",
        seed_commit_seq_num=1,
    )


def test_drive_round_reads_before_it_writes_and_allocates_from_the_read():
    """CHA-517's hard design rule, as an interaction assertion.

    No outcome assertion can pin this. apply_outcomes computes exactly what the
    read returns, so replacing the per-round read_data with an in-process dict
    carried across rounds yields a byte-identical scoreboard — read-your-writes
    deleted outright, every other test still green.

    What separates the two is observable only in the interaction: that a read
    happens, that it precedes the transaction, and that the allocation used its
    *content*. So the fake serves a tally the policy could not reach from zeros —
    carousel measured clearly best, everything else measured worse. From zeros,
    greedy takes banner on the untried tie-break.
    """
    client = _FailingClient(
        tallies={
            "banner": (50, 2),
            "carousel": (50, 40),
            "story": (50, 3),
            "video": (50, 1),
        }
    )

    demo.drive_round(
        client,
        _prod(),
        ("greedy", "uuid-greedy"),
        [0],
        [[0, 0, 0, 0]],
        random.Random(0),
        0.0,
    )

    assert [call.split(":")[0] for call in client.calls] == [
        "read_data",
        "begin_tx",
        "write_data",
        "write_data",
        "commit_tx",
    ], f"the round must read committed state before opening the tx; saw {client.calls}"

    assert client.last_upserted_creatives == ["carousel"], (
        "the allocation must come from what the read returned — carousel is best "
        "only in the served tallies, and from zeros greedy would take banner. "
        f"Saw {client.last_upserted_creatives}"
    )


def test_run_demo_forks_drives_scores_then_discards_in_that_order():
    """The wiring, which no outcome assertion reaches.

    Pins that the forks are created before any round runs, that the discard
    happens after the scoreboard reads, and that list_branches is consulted after
    the deletes — the ordering round 2 flagged and this PR fixed, with nothing
    else stopping its return.
    """
    client = _FailingClient()

    outcome = demo.run_demo(client, _small_config())

    kinds = [call.split(":")[0] for call in client.calls]
    assert kinds.count("create_branch") == len(demo.POLICY_NAMES)
    assert kinds.count("delete_branch") == len(demo.POLICY_NAMES)

    last_fork = max(
        index for index, kind in enumerate(kinds) if kind == "create_branch"
    )
    # First read, not first write: seed_prod's seeding write legitimately precedes
    # the forks, while a round always opens with the read.
    first_round_read = min(
        index for index, kind in enumerate(kinds) if kind == "read_data"
    )
    first_delete = min(
        index for index, kind in enumerate(kinds) if kind == "delete_branch"
    )
    last_read = max(index for index, kind in enumerate(kinds) if kind == "read_data")
    list_index = kinds.index("list_branches")

    assert last_fork < first_round_read, "every fork exists before any round reads"

    # Every read must land before the first delete: last_read is main's POST-discard
    # isolation read, so `last_read > first_delete` alone is satisfied wherever the
    # scoreboard collection sits — moving discard_branches ahead of it stayed green.
    # At impressions=2, round_size=2 that is 3 reads per branch: one round read plus
    # the tally and log reads collect_branch_outcome makes.
    assert kinds[:first_delete].count("read_data") == 3 * len(demo.POLICY_NAMES), (
        f"the scoreboard collection must precede the discard; saw {kinds}"
    )
    # After, not before: main's isolation read deliberately follows the discard, so
    # it pins that main survives deleting three forks that were reading its shared
    # cold object — the reason those reads were moved past the finally.
    assert last_read > first_delete, "main's isolation read must follow the discard"
    assert first_delete < list_index, (
        "list_branches must observe the post-discard state"
    )
    assert outcome.remaining_branches == ("main",)


def test_fork_branches_discards_what_it_created_before_it_failed():
    """The fork half of the unwind envelope, which had no test.

    seed_prod's two halves each have one; this is the structurally identical third.
    The second create_branch fails, so the first fork must already be discarded —
    that is what makes "earlier ones live" testable rather than asserted.
    """
    client = _FailingClient(
        raises=RuntimeError("second fork failed"), fail_create_branch_on_call=2
    )

    try:
        demo.fork_branches(client, _prod())
    except RuntimeError:
        pass
    else:
        raise AssertionError("the fork failure must propagate")

    assert client.deleted == [f"uuid-{demo.POLICY_NAMES[0]}"], (
        f"the fork created before the failure must be discarded, saw {client.deleted}"
    )


def test_fork_branches_forks_every_branch_at_the_same_commit():
    """ "Provably start from one identical view of prod" — otherwise unpinned.

    Dropping the commit_seq_num kwarg leaves behaviour identical in run_demo (the
    forks follow the seed commit, so head equals it) and the integration test
    passes the kwarg itself rather than going through fork_branches.
    """
    client = _FailingClient()
    prod = _prod()

    demo.fork_branches(client, prod)

    assert client.fork_seq_nums == [prod.seed_commit_seq_num] * len(
        demo.POLICY_NAMES
    ), f"every fork must name the seed commit; saw {client.fork_seq_nums}"


def test_choose_creative_routes_epsilon_to_the_explorer():
    """epsilon must not collapse to greedy in the dispatcher.

    carousel is clearly best here, so a dispatch that fell through to pick_greedy
    would return it every time regardless of the rng.
    """
    tallies = _all_measured()
    explored = {
        demo.choose_creative("epsilon", 0, tallies, random.Random(seed), 1.0)
        for seed in range(40)
    }

    assert len(explored) > 1, (
        f"always-explore must reach more than one creative, saw {explored}"
    )


def test_a_failed_round_propagates_its_own_error_not_the_discard_error():
    """The `completed` flag must start False.

    Initialised True, a run that dies mid-round and then also fails a delete
    reports "could not discard branches" instead of the read failure that actually
    ended it — cleanup masking the cause, which is the whole reason for the flag.
    """
    client = _FailingClient(
        raises=RuntimeError("round read failed"),
        fail_read_data=True,
        fail_delete_branch=f"uuid-{demo.POLICY_NAMES[0]}",
    )

    try:
        demo.run_demo(client, _small_config())
    except RuntimeError as exc:
        assert "round read failed" in str(exc), (
            f"the round failure must survive the cleanup, saw: {exc}"
        )
    else:
        raise AssertionError("the round failure must propagate")


def test_run_demo_raises_when_a_green_run_cannot_discard():
    """raise_for_undeleted must actually be called.

    Deleting the call site leaves the helper's own test green while the demo exits
    0 with live forks — and print_isolation would list them right above "N
    parallel universes ran against it and were thrown away".
    """
    client = _FailingClient(
        raises=RuntimeError("delete refused"),
        fail_delete_branch=f"uuid-{demo.POLICY_NAMES[0]}",
    )

    try:
        demo.run_demo(client, _small_config())
    except RuntimeError as exc:
        assert "could not be discarded" in str(exc), exc
        # The uuid, not the policy name: raise_for_undeleted reports branch uuids,
        # and this fake's uuids merely happen to embed the name.
        assert f"uuid-{demo.POLICY_NAMES[0]}" in str(exc), exc
    else:
        raise AssertionError("a green run with an undeleted branch must raise")


def test_run_demo_drops_the_catalog_when_a_fork_fails():
    """run_demo's own post-fork unwind, distinct from fork_branches' internal one.

    fork_branches discards the forks it made; only run_demo's wrapper drops the
    catalog, so deleting that leaves a prod_<hex> behind on every failed fork.
    """
    client = _FailingClient(
        raises=RuntimeError("second fork failed"), fail_create_branch_on_call=2
    )

    try:
        demo.run_demo(client, _small_config())
    except RuntimeError:
        pass
    else:
        raise AssertionError("the fork failure must propagate")

    assert client.deleted_catalogs == ["cat"], (
        f"a failed fork must not strand the catalog, saw {client.deleted_catalogs}"
    )


def test_a_failed_commit_still_aborts_the_transaction():
    """commit_tx belongs inside commit_mutations' try.

    Moved out, a commit that fails leaves the tx open — and an open penca tx
    clamps cold isolation and fences purge/GC on its branch until it times out.
    """
    client = _FailingClient(raises=RuntimeError("commit refused"), fail_commit_tx=True)

    try:
        demo.commit_mutations(
            client,
            catalog_uuid="cat",
            schema_uuid="schema",
            branch_uuid="branch",
            comment="one mutation",
            mutations=["m1"],
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("the commit failure must propagate")

    assert client.aborted == ["tx"], (
        f"a failed commit must still abort the tx, saw {client.aborted}"
    )


def test_scoreboard_prints_best_first_with_a_stable_creative_order(capsys):
    """The printed artifact is the launch deliverable, so pin its order.

    Substring presence alone survives flipping the sort — which prints the
    scoreboard worst-first, against the acceptance criterion — and survives
    dropping the per-creative tie-break, which is what makes the block
    reproducible for a screenshot.
    """
    branches = tuple(
        demo.BranchOutcome(
            branch_name=name,
            branch_uuid=f"uuid-{name}",
            impressions=100,
            conversions=conversions,
            # Equal impressions, inserted in REVERSE id order: only the id
            # tie-break can reorder them, because a stable sort on impressions
            # alone preserves insertion order — and CREATIVE_IDS is itself
            # alphabetical, so inserting in its own order would prove nothing.
            per_creative=dict.fromkeys(reversed(demo.CREATIVE_IDS), (25, 1)),
            log_conversions=conversions,
            log_impressions=100,
        )
        for name, conversions in (("epsilon", 30), ("greedy", 20), ("even", 10))
    )
    outcome = demo.DemoOutcome(
        catalog_uuid="cat",
        scoreboard=branches,
        main_tallies=dict.fromkeys(demo.CREATIVE_IDS, (0, 0)),
        main_impression_rows=0,
        remaining_branches=("main",),
    )

    demo.print_scoreboard(outcome)
    printed = capsys.readouterr().out

    ranks = [printed.index(name) for name in ("epsilon", "greedy", "even")]
    assert ranks == sorted(ranks), (
        f"the scoreboard must print best-first, saw {printed}"
    )

    allocation = printed.split("Where each branch spent")[1]
    # By line prefix, not split arithmetic: "epsilon" appears once per creative
    # row, so splitting on it and taking a slice silently ranged over greedy's and
    # even's rows too.
    seen = [line for line in allocation.splitlines() if line.startswith("| epsilon")]
    assert seen, allocation
    creative_order = [
        creative
        for creative in demo.CREATIVE_IDS
        if any(creative in line for line in seen)
    ]
    positions = [
        min(index for index, line in enumerate(seen) if creative in line)
        for creative in creative_order
    ]
    assert positions == sorted(positions), (
        f"tied creatives must print in creative_id order, saw {creative_order}"
    )


def test_main_wires_the_per_round_printer():
    """main must pass on_round, or an asciinema shows a silent pause.

    Nothing else reaches print_round: run_demo defaults it to None.
    """
    captured = {}

    def fake_run_demo(client, config, on_round=None):
        captured["on_round"] = on_round

        return _synthetic_outcome()

    original_run, original_client, original_argv = (
        demo.run_demo,
        demo.PencaClient,
        sys.argv,
    )
    demo.run_demo = fake_run_demo
    demo.PencaClient = SimpleNamespace(from_settings=lambda: _FailingClient())
    sys.argv = ["branch_demo.py", "--impressions", "2", "--round-size", "2"]
    try:
        demo.main()
    finally:
        demo.run_demo, demo.PencaClient, sys.argv = (
            original_run,
            original_client,
            original_argv,
        )

    assert captured["on_round"] is demo.print_round, (
        f"main must wire print_round, saw {captured['on_round']}"
    )


def _one_creative(conversions: int) -> dict[str, tuple[int, int]]:
    """Ten impressions on the first creative, `conversions` of them converting.

    Derived from CREATIVE_IDS rather than hardcoded, so a change to CREATIVES
    cannot leave these tests serving run_demo a candidate set the policies no
    longer rank over.
    """
    return {
        **dict.fromkeys(demo.CREATIVE_IDS, (0, 0)),
        demo.CREATIVE_IDS[0]: (10, conversions),
    }


def test_run_demo_ranks_the_scoreboard_best_first():
    """Ranked best-first, asserted in the suite branch-PR CI actually runs.

    The integration test asserts this end to end, but branch PRs skip that job. A
    static run needs branch-distinct totals or the comparison is vacuous, so the
    fake is keyed on branch_uuid here.
    """
    client = _FailingClient(
        branch_tallies={
            "uuid-even": _one_creative(1),
            "uuid-greedy": _one_creative(5),
            "uuid-epsilon": _one_creative(9),
        }
    )

    outcome = demo.run_demo(client, _small_config())

    ranked = [branch.conversions for branch in outcome.scoreboard]
    assert ranked == sorted(ranked, reverse=True), (
        f"the scoreboard must be ranked best-first, saw "
        f"{[(b.branch_name, b.conversions) for b in outcome.scoreboard]}"
    )
    assert len(set(ranked)) == len(ranked), "the fixture must give distinct totals"


def test_a_tied_scoreboard_breaks_on_branch_name():
    """The tie-break half of the sort key, which distinct totals never consult.

    greedy and epsilon are tied, and their insertion order (POLICY_NAMES) is
    reverse-alphabetical — so a stable sort without the branch_name key yields
    greedy before epsilon. A tie between even and greedy would prove nothing,
    since insertion order there is already alphabetical.
    """
    client = _FailingClient(
        branch_tallies={
            "uuid-even": _one_creative(1),
            "uuid-greedy": _one_creative(9),
            "uuid-epsilon": _one_creative(9),
        }
    )

    outcome = demo.run_demo(client, _small_config())

    # The precondition, mirroring the distinct-totals guard on the sibling test: if
    # the fixture drifts apart, the expected order still holds from the -conversions
    # half alone and this stops exercising the tie-break at all.
    tied = {branch.branch_name: branch.conversions for branch in outcome.scoreboard}
    assert tied["greedy"] == tied["epsilon"], (
        f"the fixture must tie greedy with epsilon, saw {tied}"
    )
    # The other half of the precondition: this only discriminates while greedy is
    # inserted BEFORE epsilon. Reorder POLICY_NAMES and a stable sort without the
    # tie-break yields the expected order anyway.
    names = list(demo.POLICY_NAMES)
    assert names.index("greedy") < names.index("epsilon"), (
        f"POLICY_NAMES must keep greedy before epsilon for this to bite, saw {names}"
    )

    assert [branch.branch_name for branch in outcome.scoreboard] == [
        "epsilon",
        "greedy",
        "even",
    ], (
        "tied branches must order by name, keeping the screenshot reproducible; saw "
        f"{[(b.branch_name, b.conversions) for b in outcome.scoreboard]}"
    )


def test_the_shipped_defaults_are_the_ones_the_readme_documents():
    """The shipped configuration is what a launch reader actually runs.

    Every other test overrides impressions / round_size / seed, and the defaults
    test pins forwarding rather than values — so a one-token edit to any DEFAULT_*
    falsified the README with the full gate green. That is not hypothetical: it is
    how the README's transcript went stale for 18 commits.
    """
    assert demo.DEFAULT_IMPRESSIONS == 3000
    assert demo.DEFAULT_ROUND_SIZE == 25
    assert demo.DEFAULT_EPSILON == 0.15
    assert demo.DEFAULT_SEED == 20260727

    # Guards the two degenerate shapes a plausible edit reaches: a round size at or
    # above the impression count collapses the run to one decision (no
    # read-your-writes loop at all), and epsilon at 0 collapses that branch into
    # greedy, erasing the third policy from the launch scoreboard.
    assert demo.DEFAULT_ROUND_SIZE < demo.DEFAULT_IMPRESSIONS
    assert 0.0 < demo.DEFAULT_EPSILON < 1.0


def test_run_demo_reports_every_branch_round_to_the_callback():
    """on_round must be called per branch per round, with the round's own outcomes.

    Nothing else reaches it: run_demo defaults it to None, and the printers are
    tested against a hand-built outcome. Silencing print_round, dropping the call,
    or swapping the callback's arguments all survived otherwise.
    """
    seen: list[tuple[int, str, tuple[str, ...]]] = []
    client = _FailingClient()
    config = demo.DemoConfig(impressions=4, round_size=2, epsilon=0.0, seed=1)

    demo.run_demo(
        client,
        config,
        on_round=lambda index, policy, outcomes: seen.append(
            (index, policy, tuple(creative for _v, creative, _o in outcomes))
        ),
    )

    # `seen` is fully determined by the fixture, so pin it whole rather than in
    # projections. Exact indexes, not merely non-decreasing: passing `start` instead
    # of round_index yields 0, 0, 0, 2, 2, 2 — still sorted, still six entries. And
    # the payloads by value, not by a difference assertion: greedy and epsilon are
    # byte-identical here, so handing greedy epsilon's outcomes — or swapping even's
    # and greedy's outright — kept every set, count and inequality true.
    #
    # With epsilon=0 and the fake's all-zero tallies, greedy and epsilon both take
    # the lexicographically lowest id (pick_greedy ties on the creative_id key),
    # while `even` round-robins on the visitor index, so its payload differs per
    # round and per position within the round.
    # Ceiling division and the same clamp run_demo applies, so the oracle models a
    # partial final round rather than assuming impressions divides evenly. Today's
    # fixture divides evenly; without this, editing it to one that does not would
    # break the oracle instead of exercising the demo.
    rounds = -(-config.impressions // config.round_size)
    expected = []
    for index in range(rounds):
        start = index * config.round_size
        visitors = range(start, min(start + config.round_size, config.impressions))
        tied = (min(demo.CREATIVE_IDS),) * len(visitors)
        even_payload = tuple(
            demo.CREATIVE_IDS[visitor % len(demo.CREATIVE_IDS)] for visitor in visitors
        )
        for policy in demo.POLICY_NAMES:
            expected.append((index, policy, even_payload if policy == "even" else tied))

    assert seen == expected, (
        f"every round must reach every branch once, with its own outcomes;\n"
        f"  saw  {seen}\n  want {expected}"
    )


def test_print_round_emits_the_branch_and_its_creatives(capsys):
    """print_round's own output, which the callback-wiring test cannot reach.

    That test passes a lambda, so silencing print_round itself survived it — and a
    silent print_round means an asciinema of the ~50s default run shows nothing
    between the banner and the scoreboard.
    """
    # Two outcomes, one converting, so the count and the sum differ: with a single
    # converting outcome both are 1, and `converted = len(outcomes)` — counting
    # impressions shown rather than conversions, the likeliest confusion in that
    # line — renders identically.
    demo.print_round(
        7,
        "epsilon",
        (
            (3, demo.CREATIVE_IDS[1], 1),
            (4, demo.CREATIVE_IDS[1], 0),
            (5, demo.CREATIVE_IDS[2], 0),
        ),
    )

    printed = capsys.readouterr().out
    assert printed.strip(), "print_round must emit a line"
    assert "7" in printed, printed
    assert "epsilon" in printed, printed
    assert demo.CREATIVE_IDS[1] in printed, printed
    # Both creatives, joined as the line renders them: the round's line reports
    # what it actually served, so `shown = [outcomes[0][1]]` — dropping everything
    # after the first — must not pass.
    assert f"{demo.CREATIVE_IDS[1]}, {demo.CREATIVE_IDS[2]}" in printed, printed
    # And deduped: the fixture serves CREATIVE_IDS[1] twice. Without the set,
    # a real --round-size 25 round renders "story, story, ..." twenty-five times
    # and blows the column on all 360 lines of the asciinema.
    assert printed.count(demo.CREATIVE_IDS[1]) == 1, printed
    # The conversion count too: `converted = 0` survived otherwise, and the count
    # is the only number in the line that changes as the run progresses. Anchored
    # on the leading space that `{converted:>3}` guarantees, since "1 conversions"
    # is itself a substring of "11 conversions" — what a count mutant prints once
    # the round size passes ten.
    assert " 1 conversions" in printed, printed


def test_scoreboard_prints_the_numbers_it_was_handed(capsys):
    """The printed values, not just their order — the launch artifact itself.

    The order-and-tie-break test deliberately equalises `shown` across all four
    creatives to isolate the id tie-break, which leaves the primary sort key
    `-item[1][0]` exercised by nothing, and asserts no printed number at all. Six
    mutants lived there: ranking the allocation ascending, ranking it by
    conversions, swapping the impressions/conversions columns in either table, and
    inverting or reformatting `conversion_rate` (which prints 666.67% instead of
    15.00%, right at the top of the screenshot).

    So: impressions and conversions that rank the creatives *differently*, and
    every cell asserted by value.
    """
    # Ranked by impressions: [1], [2], [3], [0]. By conversions: [0], [2], [3], [1]
    # — the exact reverse at both ends, so either mis-key reorders visibly.
    per_creative = dict(
        zip(demo.CREATIVE_IDS, ((10, 5), (40, 1), (30, 4), (20, 2)), strict=True)
    )
    branches = (
        demo.BranchOutcome(
            branch_name="epsilon",
            branch_uuid="uuid-epsilon",
            impressions=200,
            conversions=30,
            per_creative=per_creative,
            log_conversions=30,
            log_impressions=200,
        ),
    )
    outcome = demo.DemoOutcome(
        catalog_uuid="cat",
        scoreboard=branches,
        main_tallies=dict.fromkeys(demo.CREATIVE_IDS, (0, 0)),
        main_impression_rows=0,
        remaining_branches=("main",),
    )

    demo.print_scoreboard(outcome)
    printed = capsys.readouterr().out
    ranked, allocation = printed.split("Where each branch spent")

    def cells(block):
        return [
            [cell.strip() for cell in line.strip().strip("|").split("|")]
            for line in block.splitlines()
            if line.startswith("| epsilon")
        ]

    # 200 impressions / 30 conversions, in that column order, and the rate as a
    # percentage: inverted it reads 666.67%, and `:.2f` reads 0.15.
    assert cells(ranked) == [["epsilon", "200", "30", "15.00%"]], ranked

    assert cells(allocation) == [
        ["epsilon", demo.CREATIVE_IDS[1], "40", "1"],
        ["epsilon", demo.CREATIVE_IDS[2], "30", "4"],
        ["epsilon", demo.CREATIVE_IDS[3], "20", "2"],
        ["epsilon", demo.CREATIVE_IDS[0], "10", "5"],
    ], allocation


def test_the_best_creative_is_not_the_first_by_id():
    """CREATIVES' own stated invariant, which every test merely derives from.

    "The best performer is deliberately not the first by id, so 'pick the winner'
    is never the same decision as 'pick the first thing you see'." Swapping
    banner's and carousel's rates satisfies every other test — the policies still
    rank correctly over whatever rates they are given — while quietly making the
    greedy tie-break and the from-zeros fixtures prove nothing.
    """
    rates = {creative_id: rate for creative_id, _headline, rate in demo.CREATIVES}
    best = max(rates, key=lambda creative_id: rates[creative_id])

    # min(rates), not CREATIVE_IDS[0]: pick_greedy ties on the creative_id key, so
    # from all-zero tallies it takes the lexicographically lowest id, which is what
    # must not be the winner. Position and minimum coincide only because CREATIVES
    # happens to be listed alphabetically — pinning the position both passes a
    # reorder that puts the best creative on greedy's tie-break (the mutant this
    # test exists to kill) and fails a reorder that changes no rate at all.
    assert best != min(rates), (
        f"the best creative must not also be the lowest id, saw {best} with {rates}"
    )
    assert len(set(rates.values())) == len(rates), (
        f"the rates must stay distinct or 'the best' is ambiguous, saw {rates}"
    )
