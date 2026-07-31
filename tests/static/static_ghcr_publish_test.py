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
    """Assertion 5: the two arches share no layers, so a shared scope thrashes."""
    job = _job(_yaml(".github/workflows/ci.yml"), "publish-image")
    with_ = _step_using(job, "docker/build-push-action@v6")["with"]

    for key in ("cache-from", "cache-to"):
        assert "matrix.arch" in with_[key], (
            f"{key} must be scoped per arch, got {with_[key]!r}"
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
    """Assertion 8.

    Both are ``skipped`` on pull_request and merge_group, which ci-success
    already tolerates, so the merge gate is unaffected. What this buys is the
    post-merge signal: a failed publish turns main red instead of leaving every
    new user pulling a stale :main.
    """
    needs = _job(_yaml(".github/workflows/ci.yml"), "ci-success")["needs"]

    for job in ("publish-image", "publish-image-merge"):
        assert job in needs, f"ci-success must need {job!r}, got {needs}"


def test_integration_job_runs_the_image_it_just_built():
    """Assertion 9: PENCA_IMAGE is pinned to the pre-build step's own tag.

    Once compose defaults to the published image, the suite would otherwise
    test :main rather than the code under review. Deriving both sides from the
    file means renaming one without the other fails here.
    """
    job = _job(_yaml(".github/workflows/ci.yml"), "integration")
    prebuilt = _step_using(job, "docker/build-push-action@v6")["with"]["tags"]

    env = {}
    for step in job["steps"]:
        env.update(step.get("env", {}))

    assert env.get("PENCA_IMAGE") == prebuilt, (
        f"integration must run its pre-built tag {prebuilt!r}, got PENCA_IMAGE={env.get('PENCA_IMAGE')!r}"
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
    """Assertion 15."""
    readme = _read("README.md")

    for stale in (
        "compiles the server image from source",
        "a prebuilt image is on the way",
    ):
        assert stale not in readme, f"README still says {stale!r}"

    development = _read("docs/development.md")
    assert not re.search(r"arrives with[^\n]*CHA-187", development), (
        "docs/development.md still forward-references CHA-187 for the published image"
    )


def test_standalone_snippet_uses_the_tag_we_actually_publish():
    """Assertion 16: :latest is never published, so pointing at it 404s."""
    development = _read("docs/development.md")

    assert PUBLISHED_TAG in development, (
        f"standalone docker run snippet must use {PUBLISHED_TAG}"
    )
    assert f"{IMAGE}:latest" not in development, "nothing publishes :latest"
