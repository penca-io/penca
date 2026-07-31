"""Static guards for the GHCR publish pipeline (CHA-187).

Structural source-file assertions (no Docker, no network, no penca
services) that the prebuilt-image path stays wired end to end: CI publishes
a native two-arch manifest list to GHCR on ``main`` merges, ``compose.yml``
runs that published image with an explicit pull policy while keeping the
from-source fallback, ``just penca-up`` pulls by default, and the docs no
longer promise a build.

The workflow and compose assertions parse YAML and assert on the resulting
dict rather than on substrings, so what gets pinned is key *placement* — a
``packages: write`` sitting under the wrong job cannot satisfy them.
Justfile assertions are scoped to the relevant recipe body (per
``_recipe_body``) so a match inside a comment or a neighbouring recipe
cannot either.

Per ``docs/development-methodology-guide.md`` these are one-time structural
checks, not behaviour: they confirm the config shape is *present*, not that
Docker honours it. Whether Compose pulls or builds is upstream behaviour and
was measured by hand instead (recorded on the PR).

Runs under ``just static-test ghcr_publish`` and ``just check``.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

REPO = Path(__file__).parents[2]

CI_WORKFLOW = ".github/workflows/ci.yml"
COMPOSE = "docker/compose.yml"

IMAGE = "ghcr.io/penca-io/penca-rust-server"
# What compose and the docs point at: the newest `v*` release, so a quickstart
# a reader follows today behaves the same next week. `:main` still exists and
# still gets published; it is just not what a reader is aimed at.
QUICKSTART_TAG = f"{IMAGE}:latest"
MAIN_TAG = f"{IMAGE}:main"

# Every service that runs a penca-rust-server binary. bootstrap-init is in the
# list deliberately: it is a one-shot job, but it runs the same image, so
# leaving it on a local tag would rebuild the whole thing on every `up`.
RUST_SERVICES = (
    "bootstrap-init",
    "query",
    "write",
    "lifecycle",
    "lifecycle-scheduler",
    "penca-sql-server",
)


def _read(rel: str) -> str:
    return (REPO / rel).read_text()


def _yaml(rel: str) -> dict:
    return yaml.safe_load(_read(rel))


def _recipe_body(justfile_text: str, name: str) -> str:
    """Return the indented body of the ``name`` recipe (excluding its header).

    A recipe is ``name [params]:`` at column 0 followed by indented lines; the
    body ends at the next column-0 non-blank line. Scoping to the body keeps a
    guard from being satisfied by a match in a comment or a different recipe.
    The header match tolerates a trailing dependency list (``name: dep``) by
    not anchoring on the colon, so a recipe with a dependency is still found.
    """
    header = re.compile(r"^" + re.escape(name) + r"\b[^:]*:")
    body: list[str] = []
    in_recipe = False

    for line in justfile_text.splitlines():
        if not in_recipe:
            if header.match(line):
                in_recipe = True

            continue

        if line.strip() == "":
            body.append(line)
            continue

        if not line[0].isspace():
            break

        body.append(line)

    return "\n".join(body)


def _job(workflow: dict, name: str) -> dict:
    jobs = workflow["jobs"]
    assert name in jobs, f"no job named {name!r} in ci.yml (jobs: {sorted(jobs)})"
    return jobs[name]


def _step_using(job: dict, action: str) -> dict:
    for step in job.get("steps", []):
        if step.get("uses", "").startswith(action):
            return step

    raise AssertionError(
        f"no step using {action!r} in job (steps: {len(job.get('steps', []))})"
    )


def _publish_job() -> dict:
    return _job(_yaml(CI_WORKFLOW), "publish-image")


# --- .github/workflows/ci.yml ------------------------------------------------


def test_publish_image_matrix_builds_each_arch_natively():
    """Assertion 1: one job per arch, arm64 on a native arm64 runner."""
    job = _publish_job()
    legs = {
        (leg["platform"], leg["runner"]) for leg in job["strategy"]["matrix"]["include"]
    }

    assert legs == {
        ("linux/amd64", "ubuntu-latest"),
        ("linux/arm64", "ubuntu-24.04-arm"),
    }, f"publish-image matrix must fan out one native runner per arch, got {legs}"


def test_publish_image_declares_packages_write_and_contents_read():
    """Assertion 2: job-level permissions REPLACE the workflow-level block.

    ``packages: write`` is what lets GITHUB_TOKEN push to GHCR; ``contents:
    read`` has to be re-declared alongside it or actions/checkout loses the
    read it inherits today.
    """
    perms = _publish_job()["permissions"]

    assert perms.get("packages") == "write", (
        f"publish-image needs packages: write, got {perms}"
    )
    assert perms.get("contents") == "read", (
        f"publish-image needs contents: read, got {perms}"
    )


def test_publish_image_authenticates_against_ghcr():
    """Assertion 3."""
    job = _publish_job()
    login = _step_using(job, "docker/login-action@v3")

    assert login["with"]["registry"] == "ghcr.io"


def test_publish_image_pushes_by_digest_without_tags():
    """Assertion 4: per-arch legs push digests only; the merge job owns tags.

    A ``tags:`` here would publish a single-arch image under a human-readable
    tag, which is exactly the manifest-list-less state this ticket removes.
    """
    job = _publish_job()
    with_ = _step_using(job, "docker/build-push-action@v6")["with"]
    outputs = with_["outputs"]

    for fragment in ("push-by-digest=true", "name-canonical=true", "push=true"):
        assert fragment in outputs, (
            f"{fragment!r} missing from build-push-action outputs: {outputs}"
        )

    assert "tags" not in with_, (
        "per-arch legs must not tag; publish-image-merge applies the tags"
    )


def test_publish_image_cache_scopes_are_per_arch():
    """Assertion 5: the two arches share no layers, so a shared scope thrashes.

    Asserted on the resolved per-leg values rather than on the templated
    string, since the legs no longer configure their sources identically.
    """
    job = _publish_job()
    with_ = _step_using(job, "docker/build-push-action@v6")["with"]

    assert "matrix.arch" in with_["cache-to"], (
        f"cache-to must be scoped per arch, got {with_['cache-to']!r}"
    )
    # Tie the step back to the matrix first, or the per-leg checks below govern
    # dead config: hardcoding both scopes into cache-from would reintroduce the
    # cross-arch thrash while every matrix.cache_from entry still looked fine.
    assert "matrix.cache_from" in with_["cache-from"], (
        f"cache-from must consume the per-leg sources, got {with_['cache-from']!r}"
    )

    for leg in job["strategy"]["matrix"]["include"]:
        arch, sources = leg["arch"], leg["cache_from"]
        other = "arm64" if arch == "amd64" else "amd64"

        assert f"scope=publish-{arch}" in sources, f"{arch} must read its own scope"
        assert f"scope=publish-{other}" not in sources, (
            f"{arch} reads the {other} scope; the two share no layers"
        )


def test_pushes_are_never_cancelled_mid_publish():
    """A cancelled main push loses its publish and still reports green.

    Every main push shares one concurrency group, so a docs-only merge landing
    behind a Rust merge cancels that run mid-build; the replacement's own
    `changes` diff covers only its own paths, skips publish-image, and
    ci-success reads success|skipped and passes. :main silently never receives
    the merged code. Only pull_request may supersede itself.
    """
    workflow = _yaml(CI_WORKFLOW)
    cancel = str(workflow["concurrency"]["cancel-in-progress"])

    # Pin the whole expression, not the absence of one spelling: an
    # `== 'pull_request' || == 'push'` allow-list defeats a `!=` blacklist.
    assert cancel == "${{ github.event_name == 'pull_request' }}", (
        f"cancel-in-progress must admit pull_request and nothing else, got {cancel!r}"
    )

    # And per-commit grouping is what actually stops a main push displacing
    # another — cancel-in-progress: false alone only serializes, and a third
    # push still cancels the queued one.
    group = str(workflow["concurrency"]["group"])
    assert "github.sha" in group and "push" in group, (
        f"push runs must not share a concurrency group, got group: {group!r}"
    )


def test_publish_is_scoped_to_main_merges():
    """Without this, `merge_group` builds would publish straight to :main.

    Not feature branches — `push:` is scoped to main and `v*` tags, so a
    branch push never triggers this workflow, and `pull_request` skips anyway
    because `changes` is skipped there. The real regression is the queue:
    `changes` DOES run on `merge_group` and can report rust=true, so dropping
    the event check would publish queue builds for PRs that may never merge.
    Every other publish-image assertion still passes without it.
    """
    condition = _publish_job().get("if", "")

    assert "github.event_name == 'push'" in condition, (
        f"publish-image must be push-scoped, got if: {condition!r}"
    )


def test_merge_job_cannot_publish_a_single_arch_manifest():
    """`always()` here would ship a one-arch :main whenever a leg failed.

    With fail-fast: false the surviving leg still uploads its digest, so an
    always() merge job would run `imagetools create` over that one digest and
    publish a manifest list covering a single architecture — silently breaking
    everyone on the other one. A plain `needs:` skips instead, because a matrix
    job's aggregate result is `failure` if any leg failed.
    """
    job = _job(_yaml(CI_WORKFLOW), "publish-image-merge")
    condition = str(job.get("if", ""))

    # Any status-check function re-enables the job on a failed leg, not just
    # always(): `if: ${{ !cancelled() }}` is the idiomatic alternative and was
    # red-verified during review to pass a bare "always" check.
    for fn in ("always", "cancelled", "failure", "success"):
        assert fn not in condition, (
            f"publish-image-merge must skip when a leg fails; {fn}() re-enables "
            f"it, got if: {condition!r}"
        )

    # And the convention is backed by an actual check, because a next-day
    # re-run can expire one leg's artifact and leave the aggregate green with
    # a single digest present.
    runs = "\n".join(step.get("run", "") for step in job["steps"])
    digest_check = re.search(
        r"if \[ \"\$\(ls -1 \| wc -l\)\" -ne 2 \]; then(.*?)fi", runs, re.DOTALL
    )
    assert digest_check, (
        "the merge job must assert it received both arch digests before tagging"
    )
    # Presence is not enough — the branch has to abort, or a one-arch manifest
    # still ships with a log line next to it.
    assert "exit 1" in digest_check.group(1), (
        f"the digest-count check must abort, got:{digest_check.group(1)}"
    )
    # Anchor on the command, not the bare phrase — the rationale comment above
    # it mentions `imagetools create` too.
    assert runs.index(digest_check.group(0)) < runs.index(
        "docker buildx imagetools create"
    ), "the digest check must run before the tags are applied"


def test_workflow_never_reaches_for_qemu():
    """Assertion 6: emulated rustc is 5-20x slower and can blow the job timeout.

    Every job, not just publish-image — QEMU anywhere in CI defeats the native
    two-runner matrix. Checks what each step *uses* rather than whether the
    file mentions the action, so the comment explaining the prohibition does
    not trip the guard meant to enforce it.
    """
    workflow = _yaml(CI_WORKFLOW)

    for name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            assert "setup-qemu-action" not in step.get("uses", ""), (
                f"job {name!r} sets up QEMU; build each arch on a native runner instead"
            )


def test_merge_job_assembles_one_manifest_list_under_both_tags():
    """Assertion 7."""
    job = _job(_yaml(CI_WORKFLOW), "publish-image-merge")
    needs = job["needs"]
    needs = [needs] if isinstance(needs, str) else needs

    assert "publish-image" in needs, (
        f"publish-image-merge must need publish-image, got {needs}"
    )

    runs = "\n".join(step.get("run", "") for step in job["steps"])

    assert "docker buildx imagetools create" in runs
    assert IMAGE in runs, f"merge job must name {IMAGE}"
    # The main-merge arm, as distinct from the release arm below: a moving
    # :main plus an immutable short-sha to pin against.
    assert "$image:main" in runs, "merge job must tag :main on a main push"
    assert "GITHUB_SHA::7" in runs, "merge job must also apply the short-sha tag"


def test_release_tags_publish_a_pinned_version_and_latest():
    """A `v*` tag must publish :<version> + :latest, not :main.

    :main moves under a reader, so a quickstart cannot be pinned to it. The
    version comes from the tag name — there is no hand-typed version field,
    which is what keeps a release anchored to a real commit.
    """
    workflow = _yaml(CI_WORKFLOW)

    # PyYAML parses the `on:` key as the boolean True (YAML 1.1), so reach for
    # whichever key survived rather than assuming the string.
    triggers = workflow.get("on", workflow.get(True))
    assert "v*" in triggers["push"]["tags"], (
        f"push trigger must fire on v* tags, got {triggers['push']}"
    )

    # The release must not be gated on the paths-filter: a docs-only tag is
    # still a release, and a `changes` hiccup must not swallow it.
    condition = _publish_job()["if"]
    assert "refs/tags/v" in condition, (
        f"publish-image must fire for v* tags, got if: {condition!r}"
    )
    assert "always()" in condition, (
        "publish-image needs always() or a skipped `changes` swallows the release"
    )

    job = _job(workflow, "publish-image-merge")
    runs = "\n".join(step.get("run", "") for step in job["steps"])
    assert "refs/tags/v" in runs, "merge job must branch on the tag ref"

    # :latest must never be attached on the main arm. Verified reachable by
    # review: moving `-t "$image:latest"` into the else-branch left every
    # other guard here green.
    main_arm = re.search(r"\n(\s*)else\n(.*?)\n\1fi\n", runs, re.DOTALL)
    assert main_arm, "merge job no longer has a main-push arm"
    assert "latest" not in main_arm.group(2), (
        "the main-push arm must not tag :latest — it would mean 'whatever "
        f"landed on main today', got {main_arm.group(2)!r}"
    )

    # And it must be gated on being the highest STABLE release, or a backport
    # (v1.0.1 after v2.0.0) drags :latest backwards onto older code, and a
    # pre-release claims it outright (`sort -V` ranks v1.0.0-rc1 above
    # v1.0.0 — the opposite of semver).
    # The tag must be applied INSIDE the highest-release comparison, not merely
    # somewhere after a `sort -V` that nothing consumes.
    gate = re.search(
        r"if \[ \"\$highest\" = \"\$release_tag\" \]; then(.*?)(?:else|fi)",
        runs,
        re.DOTALL,
    )
    assert gate, ":latest must be gated on this tag being the highest release"
    assert "latest" in gate.group(1), (
        f"the :latest tag must be applied inside the gate, got:{gate.group(1)}"
    )
    assert "sort -V" in runs and "tail -1" in runs, "the gate must rank the tags"

    # That gate is only meaningful because the checkout fetches every tag.
    # With the default shallow fetch the workspace holds exactly one tag — the
    # pushed one — so `highest` always equals it and :latest moves
    # unconditionally, with the shell script untouched and every other
    # assertion here still green.
    checkout = _step_using(job, "actions/checkout")
    assert checkout.get("with", {}).get("fetch-depth") == 0, (
        "the merge job's checkout must fetch all tags (fetch-depth: 0), or the "
        f"highest-release gate is vacuous; got {checkout.get('with')!r}"
    )
    assert runs.count(r"^v[0-9]+\.[0-9]+\.[0-9]+$") >= 2, (
        "the highest-release comparison must restrict to stable X.Y.Z on both "
        "the pushed tag and the candidate list"
    )
    assert f"{IMAGE}:latest" in runs or "$image:latest" in runs, (
        "a release must move :latest onto the pinned version"
    )


def test_ci_success_gates_on_the_publish_jobs():
    """Assertion 8: a failed publish has to turn main red.

    ``needs`` alone does not achieve that. ci-success runs ``if: always()``, so
    a failed dependency does not fail it — the verdict comes entirely from the
    ``success|skipped`` loop in its run body. Listing a job in ``needs`` and
    forgetting the loop loses the signal silently, so both are asserted.

    (Both are ``skipped`` on pull_request and merge_group, which that same loop
    tolerates, so the merge gate is unaffected.)
    """
    job = _job(_yaml(CI_WORKFLOW), "ci-success")
    needs = job["needs"]
    runs = "\n".join(step.get("run", "") for step in job["steps"])

    # Scope to the `for result in ... ; do` list, not the whole run body: the
    # neighbouring `echo` lines name every result too, so a body-wide check
    # stays green when a job is dropped from the list that actually decides.
    loop = re.search(r"for result in(.*?); do", runs, re.DOTALL)
    assert loop, "ci-success no longer has a for-result loop to check"
    checked = loop.group(1)

    for name in ("publish-image", "publish-image-merge"):
        assert name in needs, f"ci-success must need {name!r}, got {needs}"
        assert f"needs.{name}.result" in checked, (
            f"ci-success needs {name!r} but never checks its result — "
            "if: always() means this loop, not needs, is what decides the gate"
        )


def test_integration_job_runs_the_image_it_just_built():
    """Assertion 9: PENCA_IMAGE is pinned to the pre-build step's own tag.

    Once compose defaults to the published image, the suite would otherwise
    test :main rather than the code under review. Deriving both sides from the
    file means renaming one without the other fails here.
    """
    job = _job(_yaml(CI_WORKFLOW), "integration")
    prebuilt = _step_using(job, "docker/build-push-action@v6")["with"]["tags"]

    # Resolve the effective env for the step that actually runs the suite, not
    # a union over every step: a union would both accept PENCA_IMAGE set on an
    # unrelated step (which the suite never sees) and reject the equally valid
    # job-level placement.
    suite = [
        step for step in job["steps"] if "just integration-test" in step.get("run", "")
    ]
    assert len(suite) == 1, (
        f"expected exactly one integration-test step, found {len(suite)}"
    )

    env = {**job.get("env", {}), **suite[0].get("env", {})}

    assert env.get("PENCA_IMAGE") == prebuilt, (
        f"the integration-test step must run the pre-built tag {prebuilt!r}, "
        f"got PENCA_IMAGE={env.get('PENCA_IMAGE')!r}"
    )


# --- docker/compose.yml ------------------------------------------------------


def test_every_rust_service_runs_the_published_image():
    """Assertion 10.

    ``${PENCA_IMAGE:-...}`` keeps one hand-maintained default (style guide:
    configuration defaults live in one place) while letting CI point the same
    file at a locally built tag.
    """
    services = _yaml(COMPOSE)["services"]

    for name in RUST_SERVICES:
        image = services[name]["image"]
        assert image == f"${{PENCA_IMAGE:-{QUICKSTART_TAG}}}", f"{name} runs {image!r}"


def test_every_rust_service_pulls_rather_than_builds():
    """Assertion 11.

    Explicit even though Compose 2.40.3 already pulls by default: the Compose
    *spec* documents ``build`` as the default when a build section is present,
    so the key is what makes the behaviour version-independent.
    """
    services = _yaml(COMPOSE)["services"]

    for name in RUST_SERVICES:
        policy = services[name].get("pull_policy")
        assert policy == "missing", f"{name} has pull_policy={policy!r}, want 'missing'"


def test_every_rust_service_keeps_the_from_source_path():
    """Assertion 12.

    ``build:`` is both the contributor's ``--build=1`` path and the measured
    graceful degradation when the pull fails (offline, or before the first
    publish exists). Losing it turns those into hard failures.
    """
    services = _yaml(COMPOSE)["services"]

    for name in RUST_SERVICES:
        assert "build" in services[name], f"{name} lost its build: section"


# --- Justfile ----------------------------------------------------------------


def test_penca_up_does_not_build_by_default():
    """Assertion 13: the quickstart path pulls; building is opt-in."""
    body = _recipe_body(_read("Justfile"), "penca-up")

    assert "{{build}}" in body, "penca-up must take a build argument"

    assignments = re.findall(r'build_flag=("[^"]*")', body)
    assert assignments, "penca-up no longer assigns build_flag"
    assert assignments[0] == '""', (
        f"penca-up must default build_flag to empty, got {assignments[0]}"
    )

    # Compose tags a build's output under whatever `image:` resolves to, so a
    # build has to be redirected off the published ref or it shadows the pulled
    # image for every later run — pull_policy: missing never re-pulls it.
    #
    # Both the target and the guard are pinned, not just the shape: defaulting
    # to the published ref reinstates exactly that shadowing, and hoisting the
    # export out of the build-only branch points a plain `just penca-up` at a
    # local tag no registry serves, turning the quickstart's pull back into a
    # from-source build.
    redirect = re.search(
        r'if \[ -n "\$build_flag" \]; then\n(.*?)\n\s*fi', body, re.DOTALL
    )
    assert redirect, (
        "penca-up must redirect a --build=1 run inside the build-only branch"
    )

    assignment = re.search(r"export PENCA_IMAGE=(.*)", redirect.group(1))
    assert assignment, (
        "penca-up must point a --build=1 run at its own tag, or the build "
        "overwrites the pulled ghcr.io ref"
    )

    target = assignment.group(1)
    assert IMAGE not in target, (
        f"a local build must not be tagged under the published ref ({IMAGE})"
    )
    # CI passes --build=1 through integration-test but supplies its own tag.
    assert "${PENCA_IMAGE:-" in target, "an explicit PENCA_IMAGE must still win"
    # perf-test --profile exports CARGO_PROFILE=profiling before building, so
    # an unkeyed tag lets that DWARF image become whatever the next
    # integration-test run picks up.
    assert "CARGO_PROFILE" in target, (
        "the local tag must be profile-keyed, or release and profiling builds "
        "overwrite each other"
    )


def test_penca_up_never_leaves_the_image_to_up():
    """`up` must never be the thing that produces a missing image.

    The six servicers name one image ref but compose does not dedupe their
    identical build configs, so any path that lets `up` resolve a missing
    image fans out six concurrent targets racing to export one tag — the
    losers die with ``image "...": already exists``. No config-shape assertion
    reaches that race, but the *shape of the fix* is an ordinary Justfile line,
    and re-appending ``$build_flag`` to the ``up`` call would reinstate a
    first-build failure for every contributor while the rest of this suite
    stayed green.
    """
    body = _recipe_body(_read("Justfile"), "penca-up")
    # Match real invocations only: the prose explaining this invariant, and
    # the --db refusal message ("build context (compose.yml ...)"), both
    # mention compose and build without invoking either.
    code = [line for line in body.splitlines() if not line.strip().startswith("#")]

    up_calls = [line for line in code if "docker compose" in line and "up -d" in line]
    assert up_calls, "penca-up no longer brings the stack up"
    for line in up_calls:
        assert "--build" not in line and "build_flag" not in line, (
            f"`up` must not build; materialize the image beforehand: {line.strip()!r}"
        )

    # Both producing paths — explicit --build=1, and the pull-failure
    # degradation — must go through a single-service build.
    builds = [line for line in code if "docker compose" in line and " build " in line]
    assert len(builds) >= 2, (
        "expected a single-service build for both the --build=1 path and the "
        f"pull-failure fallback, found {len(builds)}"
    )
    # Every build site must redirect off the published ref first, not just the
    # --build=1 one. The pull-failure fallback is the default path whenever the
    # registry is unreachable, and a build stamped onto the published tag is
    # served forever by pull_policy: missing.
    # Split the materialize chain into its top-level arms and require, per arm
    # that builds, that a redirect precedes every build in it. Scope-aware on
    # purpose: the export can sit at the arm's own level while the build is
    # nested a block deeper, and it still dominates.
    #
    # Two earlier spellings of this guard were defeated: "an export appears
    # earlier in the file" let the --build=1 arm vouch for the fallback's
    # build site, and matching build lines by text collapsed the two, which
    # are byte-identical, so index() always returned the first.
    arms: list[list[str]] = []
    for line in code:
        if re.match(r"\s{4}(if|elif|else|fi)\b", line):
            arms.append([])

        if arms:
            arms[-1].append(line)

    checked = 0
    for arm in arms:
        build_at = [
            i for i, ln in enumerate(arm) if "docker compose" in ln and " build " in ln
        ]
        if not build_at:
            continue

        checked += 1
        export_at = [i for i, ln in enumerate(arm) if "export PENCA_IMAGE=" in ln]
        assert export_at, (
            f"arm builds without redirecting off the published ref: {arm[0].strip()!r}"
        )
        # Dominance, not just order. An `else`/`elif` only separates the two
        # when it closes a branch that was already open at the export — one
        # belonging to a block opened *after* it does not, since the export
        # still runs on every path into that block. Track depth rather than
        # indent: order alone let an export in one nested branch vouch for a
        # build in its sibling.
        first_export = min(export_at)
        for build in build_at:
            assert first_export < build, (
                "the redirect must precede the build in this arm, or compose "
                f"stamps the build onto the published ref: {arm[0].strip()!r}"
            )

            depth = 0
            for ln in arm[first_export + 1 : build]:
                if re.match(r"\s*if\b", ln):
                    depth += 1
                elif re.match(r"\s*fi\b", ln):
                    depth -= 1
                elif re.match(r"\s*(else|elif)\b", ln) and depth == 0:
                    raise AssertionError(
                        "the redirect is in a sibling branch of the build, so "
                        f"it never runs on that path: {arm[0].strip()!r}"
                    )

    assert checked >= 2, (
        "expected a build in both the --build=1 arm and the pull-failure "
        f"fallback, found {checked}"
    )

    redirects = [line for line in code if "export PENCA_IMAGE=" in line]
    assert len(redirects) >= 2, (
        "expected a redirect for both the --build=1 path and the pull-failure "
        f"fallback, found {len(redirects)}"
    )
    for line in redirects:
        assert IMAGE not in line, (
            f"a redirect must not point back at the published ref: {line.strip()!r}"
        )

    for line in builds:
        assert re.search(r"build \S+\s*(\\|\||$)", line.strip()), (
            f"build must name exactly one service, not fan out: {line.strip()!r}"
        )


def test_penca_up_needs_no_undocumented_tools():
    """The quickstart's first command must run with only the documented prereqs.

    Those are Docker, `uv` and `just`. `jq` is in neither prereq list, is not
    installed by `just bootstrap`, and macOS ships without it — so reaching for
    it here means a reader's very first Penca command dies at `command not
    found` before anything starts.
    """
    body = _recipe_body(_read("Justfile"), "penca-up")
    code = [line for line in body.splitlines() if not line.strip().startswith("#")]

    for line in code:
        assert not re.search(r"\bjq\b", line), (
            f"penca-up must not require jq — not a documented prereq: {line.strip()!r}"
        )


def test_local_code_recipes_force_a_build():
    """Assertion 14: these loops test the working tree, not the published tag.

    ``--build=1`` and not a bare ``--build``: just 1.51.0's ``arg`` attribute
    has no valueless-flag form, so ``--build`` alone fails with "option
    --build missing value".
    """
    justfile = _read("Justfile")

    for recipe in ("integration-test", "tdd", "perf-test"):
        body = _recipe_body(justfile, recipe)
        invocations = [line for line in body.splitlines() if "just penca-up" in line]

        assert invocations, f"{recipe} no longer invokes penca-up"
        for line in invocations:
            assert "--build=1" in line, f"{recipe} must force a build: {line.strip()!r}"


# --- docs --------------------------------------------------------------------


def test_docs_no_longer_promise_a_from_source_first_run():
    """Assertion 15.

    Matched against whitespace-collapsed text because this prose is
    hard-wrapped: the original README split the claim as "compiles the
    server\\nimage from source", so a raw substring check passed vacuously and
    would have kept passing with the stale promise fully intact.
    """
    readme = " ".join(_read("README.md").split())

    for stale in (
        "compiles the server image from source",
        "a prebuilt image is on the way",
    ):
        assert stale not in readme, f"README still says {stale!r}"

    development = " ".join(_read("docs/development.md").split())

    assert not re.search(r"arrives with[^.]*CHA-187", development), (
        "docs/development.md still forward-references CHA-187 for the published image"
    )
    # The prereq table used to claim every first run compiles regardless. Left
    # standing, a contributor runs the published :main believing their own
    # edits are under test.
    assert "builds the servicer image from source" not in development, (
        "docs/development.md still claims penca-up builds from source"
    )


def test_standalone_snippet_uses_the_tag_we_actually_publish():
    """Assertion 16: docs aim readers at releases, not at moving `main`.

    Someone who follows the quickstart today should get the same thing next
    week. `:main` moves on every qualifying merge, so it is the wrong thing to
    put in front of a reader — it stays published for tracking unreleased
    work, and the surrounding prose may explain it, but the runnable snippet
    must not use it.

    This depends on a `v*` release existing: until one is pushed, `:latest`
    does not resolve and Compose falls back to building from source.
    """
    development = _read("docs/development.md")

    # Scoped to the fenced block, both directions. The surrounding prose
    # mentions :latest independently, so a file-wide positive check would pass
    # with the snippet pointing at any other tag — a stale :v0.1.0, a
    # short-sha, a fork's registry — while claiming to pin this one.
    snippet = re.search(r"```bash\n(docker run.*?)```", development, re.DOTALL)
    assert snippet, "standalone docker run snippet not found"

    assert QUICKSTART_TAG in snippet.group(1), (
        f"standalone docker run snippet must use {QUICKSTART_TAG}, got:\n{snippet.group(1)}"
    )
    assert MAIN_TAG not in snippet.group(1), (
        f"the runnable snippet must not aim readers at the moving {MAIN_TAG}"
    )
