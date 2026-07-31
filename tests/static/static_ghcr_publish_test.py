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

IMAGE = "ghcr.io/penca-io/penca-rust-server"
PUBLISHED_TAG = f"{IMAGE}:main"

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


# --- .github/workflows/ci.yml ------------------------------------------------


def test_publish_image_matrix_builds_each_arch_natively():
    """Assertion 1: one job per arch, arm64 on a native arm64 runner."""
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image")
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
    perms = _job(_yaml(".github/workflows/ci.yml"), "publish-image")["permissions"]

    assert perms.get("packages") == "write", (
        f"publish-image needs packages: write, got {perms}"
    )
    assert perms.get("contents") == "read", (
        f"publish-image needs contents: read, got {perms}"
    )


def test_publish_image_authenticates_against_ghcr():
    """Assertion 3."""
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image")
    login = _step_using(job, "docker/login-action@v3")

    assert login["with"]["registry"] == "ghcr.io"


def test_publish_image_pushes_by_digest_without_tags():
    """Assertion 4: per-arch legs push digests only; the merge job owns tags.

    A ``tags:`` here would publish a single-arch image under a human-readable
    tag, which is exactly the manifest-list-less state this ticket removes.
    """
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image")
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
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image")
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


def test_publish_is_scoped_to_main_merges():
    """Dropping the `if:` would publish unreviewed branches straight to :main.

    Every other publish-image assertion still passes without it, so this is
    the one thing standing between a same-repo PR branch and the tag the
    quickstart pulls.
    """
    condition = _job(_yaml(".github/workflows/ci.yml"), "publish-image").get("if", "")

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
    condition = str(
        _job(_yaml(".github/workflows/ci.yml"), "publish-image-merge").get("if", "")
    )

    assert "always" not in condition, (
        f"publish-image-merge must skip when a leg fails, got if: {condition!r}"
    )


def test_workflow_never_reaches_for_qemu():
    """Assertion 6: emulated rustc is 5-20x slower and can blow the job timeout.

    Every job, not just publish-image — QEMU anywhere in CI defeats the native
    two-runner matrix. Checks what each step *uses* rather than whether the
    file mentions the action, so the comment explaining the prohibition does
    not trip the guard meant to enforce it.
    """
    workflow = _yaml(".github/workflows/ci.yml")

    for name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            assert "setup-qemu-action" not in step.get("uses", ""), (
                f"job {name!r} sets up QEMU; build each arch on a native runner instead"
            )


def test_merge_job_assembles_one_manifest_list_under_both_tags():
    """Assertion 7."""
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image-merge")
    needs = job["needs"]
    needs = [needs] if isinstance(needs, str) else needs

    assert "publish-image" in needs, (
        f"publish-image-merge must need publish-image, got {needs}"
    )

    runs = "\n".join(step.get("run", "") for step in job["steps"])

    assert "docker buildx imagetools create" in runs
    assert PUBLISHED_TAG in runs, f"merge job must tag {PUBLISHED_TAG}"
    assert "GITHUB_SHA::7" in runs, "merge job must also apply the short-sha tag"


def test_ci_success_gates_on_the_publish_jobs():
    """Assertion 8: a failed publish has to turn main red.

    ``needs`` alone does not achieve that. ci-success runs ``if: always()``, so
    a failed dependency does not fail it — the verdict comes entirely from the
    ``success|skipped`` loop in its run body. Listing a job in ``needs`` and
    forgetting the loop loses the signal silently, so both are asserted.

    (Both are ``skipped`` on pull_request and merge_group, which that same loop
    tolerates, so the merge gate is unaffected.)
    """
    job = _job(_yaml(".github/workflows/ci.yml"), "ci-success")
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
    job = _job(_yaml(".github/workflows/ci.yml"), "integration")
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
    services = _yaml("docker/compose.yml")["services"]

    for name in RUST_SERVICES:
        image = services[name]["image"]
        assert image == f"${{PENCA_IMAGE:-{PUBLISHED_TAG}}}", f"{name} runs {image!r}"


def test_every_rust_service_pulls_rather_than_builds():
    """Assertion 11.

    Explicit even though Compose 2.40.3 already pulls by default: the Compose
    *spec* documents ``build`` as the default when a build section is present,
    so the key is what makes the behaviour version-independent.
    """
    services = _yaml("docker/compose.yml")["services"]

    for name in RUST_SERVICES:
        policy = services[name].get("pull_policy")
        assert policy == "missing", f"{name} has pull_policy={policy!r}, want 'missing'"


def test_every_rust_service_keeps_the_from_source_path():
    """Assertion 12.

    ``build:`` is both the contributor's ``--build=1`` path and the measured
    graceful degradation when the pull fails (offline, or before the first
    publish exists). Losing it turns those into hard failures.
    """
    services = _yaml("docker/compose.yml")["services"]

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
    """Assertion 16: :latest is never published, so pointing at it 404s."""
    development = _read("docs/development.md")

    assert PUBLISHED_TAG in development, (
        f"standalone docker run snippet must use {PUBLISHED_TAG}"
    )
    assert f"{IMAGE}:latest" not in development, "nothing publishes :latest"
