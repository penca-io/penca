"""Integration tests for CHA-517 — the Show HN launch demo.

``examples/branch_demo.py`` forks three branches off ``main``, drives one shared
deterministic visitor feed through all three, and lets each branch's allocation
policy read back its *own* committed tallies to steer the next round. These tests
pin the four claims the launch post makes: the feed is shared and reproducible,
the read-your-writes policies pull clear of the fixed-split foil, forking does not
multiply stored bytes, and ``main`` comes out untouched with the forks discarded.

Run via ``just integration-test branch_demo``.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType

from penca_client.naming import TABLE_PERSIST_SEGMENT_METADATA
from psycopg.sql import SQL, Identifier

from .integration_helpers import get_pg_driver, make_client

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEMO_PATH = _REPO_ROOT / "examples" / "branch_demo.py"

# Small enough to keep the suite quick, large enough that the read-your-writes
# policies pull clear of the fixed split. Pinned rather than derived so the
# divergence assertion is reproducible run to run.
_IMPRESSIONS = 240
_ROUND_SIZE = 12
_SEED = 20260727


def _load_demo() -> ModuleType:
    """Import ``examples/branch_demo.py`` by path.

    ``examples/`` is not a package, and pytest puts ``tests/`` — not the repo
    root — on ``sys.path``, so neither a plain nor a relative import resolves.
    """
    spec = importlib.util.spec_from_file_location("branch_demo", _DEMO_PATH)
    if spec is None or spec.loader is None:
        msg = f"cannot build an import spec for {_DEMO_PATH}"
        raise RuntimeError(msg)

    module = importlib.util.module_from_spec(spec)
    # Register before executing: the demo's dataclasses are defined under
    # `from __future__ import annotations`, and dataclasses resolves those string
    # annotations through sys.modules[cls.__module__], which is None for a module
    # that was built from a spec but never registered.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    return module


def _config(demo: ModuleType):
    return demo.DemoConfig(
        impressions=_IMPRESSIONS,
        round_size=_ROUND_SIZE,
        epsilon=demo.DEFAULT_EPSILON,
        seed=_SEED,
    )


def _segment_stats(catalog_uuid: str, table_uuid: str) -> dict[str, tuple[int, int]]:
    """Per-branch ``(distinct objects, total bytes)`` of cold persist segments.

    White-box PG read: the gRPC API exposes no storage-footprint surface, and the
    segment index is where ``object_uri`` / ``size_bytes`` are recorded.

    ``size_bytes`` is the column to sum, not ``length`` — ``length`` is the slice
    length within a merged file and is written only by
    ``compact_persist_segments``, so a freshly persisted segment (which owns its
    whole object) reports 0 there.

    Committed rows only: persist is two-phase, and the read planner gates
    visibility on a segment's own ``commit_micros``. A crashed phase-2 leaves rows
    no read can ever see, which a footprint claim must not count.
    """
    parent = f"{catalog_uuid}_{TABLE_PERSIST_SEGMENT_METADATA}"
    rows = get_pg_driver().execute(
        SQL(
            "SELECT branch_uuid, count(DISTINCT object_uri), coalesce(sum(size_bytes), 0) "
            "FROM {tbl} WHERE table_uuid = %s AND commit_micros IS NOT NULL "
            "GROUP BY branch_uuid"
        ).format(tbl=Identifier(parent)),
        (table_uuid,),
    )

    return {row[0]: (int(row[1]), int(row[2])) for row in rows}


def _cleanup_branch(client, catalog_uuid: str, branch_uuid: str) -> None:
    """Best-effort, mirroring _cleanup_catalog: a cleanup failure must never
    become the test's failure, nor gate the cleanup that follows it."""
    try:
        client.delete_branch(catalog_uuid=catalog_uuid, branch_uuid=branch_uuid)
    except Exception as exc:  # noqa: BLE001 - cleanup must not mask a real failure
        print(f"(could not delete branch {branch_uuid}: {exc})")


def _cleanup_catalog(client, catalog_uuid: str) -> None:
    """Best-effort: never turn a cleanup failure into the test's failure."""
    try:
        client.delete_catalog(catalog_uuid=catalog_uuid)
    except Exception as exc:  # noqa: BLE001 - cleanup must not mask a real failure
        print(f"(could not delete catalog {catalog_uuid}: {exc})")


def test_shared_feed_is_deterministic():
    """One seed reproduces one feed, and every visitor carries a fixed latent
    outcome per creative — which is what makes the three branches consume a single
    shared stream rather than three independent simulations."""
    demo = _load_demo()

    first = demo.build_visitor_feed(_config(demo))
    second = demo.build_visitor_feed(_config(demo))
    assert first == second, "the same seed must reproduce the feed exactly"
    assert len(first) == _IMPRESSIONS

    assert all(len(outcomes) == len(demo.CREATIVES) for outcomes in first), (
        "every visitor needs one latent outcome per creative, so two branches "
        "showing the same creative to the same visitor agree"
    )
    assert {value for outcomes in first for value in outcomes} == {0, 1}, (
        "the feed must contain both outcomes; an all-zero feed would satisfy a "
        "subset check while making the divergence test meaningless"
    )


def test_demo_forks_diverges_and_isolates_main():
    """The read-your-writes branches beat the fixed-split foil, each branch's
    tallies reconcile with its own append-only log, and prod survives untouched
    with all three forks discarded."""
    demo = _load_demo()

    client = make_client()
    outcome = demo.run_demo(client, _config(demo))

    by_name = {branch.branch_name: branch for branch in outcome.scoreboard}
    assert set(by_name) == {"even", "greedy", "epsilon"}

    even = by_name["even"].conversions
    assert by_name["greedy"].conversions > even, (
        f"greedy reads its own tallies and must beat the fixed split; "
        f"greedy={by_name['greedy'].conversions} even={even}"
    )
    assert by_name["epsilon"].conversions > even, (
        f"epsilon reads its own tallies and must beat the fixed split; "
        f"epsilon={by_name['epsilon'].conversions} even={even}"
    )

    for branch in outcome.scoreboard:
        assert branch.impressions == _IMPRESSIONS
        assert sum(shown for shown, _ in branch.per_creative.values()) == _IMPRESSIONS
        assert (
            sum(converted for _, converted in branch.per_creative.values())
            == branch.conversions
        )
        assert branch.log_conversions == branch.conversions, (
            f"{branch.branch_name}: the in-place tally and the append-only "
            f"impressions log disagree ({branch.conversions} vs "
            f"{branch.log_conversions}) — they commit in one tx and cannot diverge"
        )
        assert branch.log_impressions == _IMPRESSIONS, (
            f"{branch.branch_name}: the impressions log holds "
            f"{branch.log_impressions} rows, expected {_IMPRESSIONS}. visitor_id is "
            f"the log's primary key, so append-only is a property of the visitor "
            f"ranges being disjoint — an overlapping range replaces rows instead, "
            f"and only the row count catches it"
        )

    # These reads happen AFTER the three delete_branch calls, so they pin more than
    # "the run left prod alone": main's rows live in one cold object that all three
    # forks were reading, and it has to survive their deletion. delete_branch is
    # safe today (every enumeration pins branch_uuid), but a regression there would
    # otherwise still print a green demo.
    assert all(tally == (0, 0) for tally in outcome.main_tallies.values()), (
        f"prod tallies must survive the discard untouched, saw {outcome.main_tallies}"
    )
    assert outcome.main_impression_rows == 0, "prod logged no impressions"
    assert outcome.remaining_branches == ("main",), (
        f"all three forks must be discarded, saw {outcome.remaining_branches}"
    )

    # After the assertions, and deliberately NOT in a finally: run_demo keeps the
    # prod catalog by design (prod outliving its forks is the demo), so the test
    # that created it cleans it up on success — while a red assertion leaves the
    # catalog behind on purpose, for inspection.
    _cleanup_catalog(client, outcome.catalog_uuid)


def test_forks_share_one_copy_of_the_seeded_data():
    """Forking three branches off the seeded catalog adds no stored row data.

    CreateBranch flushes the *source* hot tier to cold once (CHA-273) and copies
    metadata only — never row data (CHA-178). So main's cold footprint is flat
    across the second and third fork, each fork owns zero objects and zero bytes,
    and all three still read the full seeded set.
    """
    demo = _load_demo()
    client = make_client()
    prod = demo.seed_prod(client)

    # try opens as soon as the catalog exists, so every assertion below is covered
    # by the cleanup. This test reaps on red and green alike; the divergence test
    # above deliberately does the opposite and keeps its catalog on red, for
    # inspection.
    fork_uuids: list[str] = []
    try:
        baseline = _segment_stats(prod.catalog_uuid, prod.creatives_table_uuid)
        assert baseline == {}, (
            f"nothing is persisted before the first fork, saw {baseline}"
        )

        seeded = {creative_id for creative_id, _headline, _rate in demo.CREATIVES}
        main_footprint: tuple[int, int] | None = None
        for branch_name in ("even", "greedy", "epsilon"):
            branch = client.create_branch(
                branch_name,
                "cha-517",
                "fork",
                commit_seq_num=prod.seed_commit_seq_num,
                catalog_uuid=prod.catalog_uuid,
            )
            fork_uuids.append(branch.branch_uuid)

            stats = _segment_stats(prod.catalog_uuid, prod.creatives_table_uuid)
            observed = stats.get(prod.main_branch_uuid)
            assert observed is not None, "the fork must flush main's hot tier to cold"
            # Exactly one object: this is the "one copy of your data" claim the
            # README makes, so pin the count. Safe at any segment cap — the seeded
            # table is four rows in one commit, orders of magnitude under both the
            # 1 MiB test cap and the 64 MiB default. The byte total is deliberately
            # only checked as non-zero and flat; it moves with writer version and
            # compression, so pinning it would be brittle without adding meaning.
            assert observed[0] == 1, (
                f"main's seeded rows must live in exactly one object, saw {observed}"
            )
            assert observed[1] > 0, (
                f"main's cold footprint must carry bytes after a fork, saw {observed}"
            )
            if main_footprint is None:
                main_footprint = observed
            else:
                assert observed == main_footprint, (
                    "forking again must not duplicate or re-flush main's cold bytes; "
                    f"{main_footprint} -> {observed}"
                )

            for fork_uuid in fork_uuids:
                assert fork_uuid not in stats, (
                    f"fork {fork_uuid} must store zero segments of its own, "
                    f"saw {stats[fork_uuid]}"
                )

        for fork_uuid in fork_uuids:
            got = client.read_data(
                catalog_uuid=prod.catalog_uuid,
                schema_uuid=prod.schema_uuid,
                table_uuid=prod.creatives_table_uuid,
                branch_uuid=fork_uuid,
            )
            assert set(got.column("creative_id").to_pylist()) == seeded, (
                "every fork reads the full seeded set it stores none of"
            )
    finally:
        # finally, not straight-line: a red run is exactly when leaving live state
        # behind hurts most, and later tests share this stack.
        #
        # delete_branch enumerates a branch's segments and issues the object
        # deletes; delete_catalog resolves main and soft-deletes PG metadata.
        # Neither subsumes the other.
        #
        # Forks only, NOT main, so main's one seeded object is left to the object
        # store's own lifecycle. A knowing trade: delete_catalog resolves main to
        # run its cascade, so deleting main's branch first makes the catalog delete
        # fail with "branch not found: main" and leaks the catalog row instead —
        # verified by doing exactly that. Reaping main's objects would need the
        # branch delete to run *after* the catalog delete, which the API does not
        # offer.
        #
        # The forks own zero segments here (this test asserts it), so their deletes
        # are metadata hygiene rather than an object reap. Each step is best-effort
        # so one failure cannot skip what follows or replace a real assertion
        # failure with a cleanup error.
        for fork_uuid in fork_uuids:
            _cleanup_branch(client, prod.catalog_uuid, fork_uuid)

        _cleanup_catalog(client, prod.catalog_uuid)


def test_demo_script_runs_as_cli():
    """The script runs end-to-end with no manual setup beyond the sourced env."""
    result = subprocess.run(
        [
            sys.executable,
            str(_DEMO_PATH),
            "--impressions",
            str(_IMPRESSIONS),
            "--round-size",
            str(_ROUND_SIZE),
            "--seed",
            str(_SEED),
        ],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        # capture_output streams nothing while it runs, so an unbounded wait on a
        # wedged demo would hang the suite silently until the outer harness kills
        # it. TimeoutExpired is the legible failure.
        timeout=300,
    )
    assert result.returncode == 0, result.stderr[-4000:]

    stdout = result.stdout.lower()
    assert "scoreboard" in stdout, result.stdout[-2000:]
    assert "prod" in stdout, result.stdout[-2000:]

    # The demo keeps its catalog by design, so the run leaks one unless the test
    # cleans it up — and the uuid is only reachable because print_isolation names
    # it, which makes this that line's only coverage.
    kept = [
        line.split(":", 1)[1].strip()
        for line in result.stdout.splitlines()
        if line.startswith("prod catalog (kept):")
    ]
    assert len(kept) == 1, f"expected one kept-catalog line, saw {kept}"
    _cleanup_catalog(make_client(), kept[0])
