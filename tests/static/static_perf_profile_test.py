"""Static checks for the samply perf-profile wiring (CHA-420).

These are pure source-file assertions — no Docker, no penca services. They pin
the Penca-owned wiring this ticket introduces (per feedback_dont_test_upstream_libs):
the gitignored ``.perf/`` dir, the ``just perf-test --profile`` flag shape, the
dedicated ``[profile.profiling]`` Cargo profile + its Dockerfile/compose plumbing,
and the docs for the operator prerequisite. They are the committed regression
guard that the recipe wiring doesn't rot.

The actual profile *capture* is validated by an out-of-band e2e smoke
(``just perf-test --profile query``) during implementation — it needs
passwordless sudo (servicers run as root in-container) and a profiling-image
build, so it can't live in the committed suite.

Runs under ``just static-test perf_profile`` and ``just check``.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).parents[2]


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text()


def _profiling_block(cargo_toml: str) -> str:
    """Return the body of the ``[profile.profiling]`` table (up to the next
    ``[`` header), or "" if the section is absent."""
    marker = "[profile.profiling]"
    start = cargo_toml.find(marker)
    if start == -1:
        return ""

    rest = cargo_toml[start + len(marker) :]
    end = rest.find("\n[")

    return rest if end == -1 else rest[:end]


def _just_recipe_body(justfile: str, name: str) -> str:
    """Return a just recipe's text — from its ``<name>``-prefixed definition
    line down to (but excluding) the next column-0 token (the next recipe, its
    doc-comment, or its ``[attr]``) — or "" if the recipe is absent. Recipe
    bodies are indented, so a non-blank column-0 line ends the recipe. Scopes
    the substring guards to one recipe so a stray marker in an unrelated recipe
    can't satisfy them."""
    lines = justfile.splitlines()
    # A just recipe header is `name:` or `name <args>:` — match that precisely so
    # a prefix-sharing recipe (`perf-profile-all:`) or a variable assignment
    # (`perf-profile := …`, excluded via the `:` negative-lookahead on `=`) can't
    # false-match.
    header = re.compile(rf"^{re.escape(name)}(\s[^:]*)?:(?!=)")
    start = next((i for i, ln in enumerate(lines) if header.match(ln)), None)
    if start is None:
        return ""

    body = [lines[start]]
    for line in lines[start + 1 :]:
        if line and not line[0].isspace():
            break

        body.append(line)

    return "\n".join(body)


def _md_section(markdown: str, heading_contains: str) -> str:
    """Return a markdown section — from the first (non-fenced) heading whose text
    contains ``heading_contains`` down to the next heading of the same or higher
    level — or "" if absent. Tracks ``````` fences so a ``#`` comment inside a
    code block isn't mistaken for a heading."""
    lines = markdown.splitlines()
    in_fence = False
    start = None
    for index, line in enumerate(lines):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence

        elif not in_fence and line.startswith("#") and heading_contains in line:
            start = index
            break

    if start is None:
        return ""

    start_level = len(lines[start]) - len(lines[start].lstrip("#"))
    section = [lines[start]]
    in_fence = False
    for line in lines[start + 1 :]:
        if line.lstrip().startswith("```"):
            in_fence = not in_fence

        elif not in_fence and line.startswith("#"):
            if len(line) - len(line.lstrip("#")) <= start_level:
                break

        section.append(line)

    return "\n".join(section)


def test_perf_profile_recipe_and_gitignore() -> None:
    gitignore = _read(".gitignore")
    assert any(
        line.strip().rstrip("/") == ".perf" for line in gitignore.splitlines()
    ), ".gitignore must ignore the .perf/ samply-profile output dir"

    # Profiling is an opt-in `--profile` flag on the perf-test recipe (not a
    # separate recipe), mirroring --trace.
    recipe = _just_recipe_body(_read("Justfile"), "perf-test")
    assert recipe, "Justfile must define a `perf-test` recipe"
    assert '"--profile"' in recipe, (
        "perf-test must recognize a --profile flag in its arg-splitting loop"
    )
    # Servicers run as root in-container, so samply attaches via sudo
    # (CAP_PERFMON); unprivileged perf_event_open across users is denied at any
    # perf_event_paranoid level, and root bypasses paranoid entirely.
    assert 'sudo "$samply_bin" record -p' in recipe, (
        "perf-test --profile must attach samply by PID under sudo "
        '(`sudo "$samply_bin" record -p`)'
    )
    assert "--save-only" in recipe, (
        "perf-test --profile must run samply with --save-only"
    )
    assert ".perf/profile-" in recipe, (
        "perf-test --profile must write profiles to .perf/profile-<svc>.json"
    )
    assert "sudo -n" in recipe, (
        "perf-test --profile must preflight passwordless sudo (sudo -n true)"
    )


def test_profiling_build_profile_wired() -> None:
    block = _profiling_block(_read("Cargo.toml"))
    assert block, "Cargo.toml must define a [profile.profiling] section"
    assert "debug =" in block, (
        "[profile.profiling] must set a debug level for DWARF symbolication"
    )

    dockerfile = _read("docker/Dockerfile.rust-server")
    assert "ARG CARGO_PROFILE" in dockerfile, (
        "Dockerfile must declare ARG CARGO_PROFILE to select the build profile"
    )
    assert "${CARGO_PROFILE}" in dockerfile, (
        "Dockerfile must build/COPY via ${CARGO_PROFILE}"
    )
    # Frame pointers are a rustc -C flag, not a Cargo profile key, so they're
    # forced via RUSTFLAGS in the profiling Docker build, not in Cargo.toml.
    # Pin the executable export (not a bare mention) so the guard can't pass on
    # the explanatory comment alone if the RUN wiring is deleted.
    assert 'export RUSTFLAGS="-Cforce-frame-pointers=yes"' in dockerfile, (
        "Dockerfile must force frame pointers via an executable "
        'export RUSTFLAGS="-Cforce-frame-pointers=yes" in the profiling build '
        "(not just a comment) — samply unwinding"
    )

    compose = _read("docker/compose.yml")
    assert "CARGO_PROFILE" in compose, (
        "compose.yml must wire a CARGO_PROFILE build arg through to the image build"
    )


def test_profiling_docs_present() -> None:
    # Profiling is contributor-facing, so CHA-522 moved it out of the README and
    # into the development guide along with the rest of the operational content.
    profiling = _md_section(_read("docs/development.md"), "Profiling")
    assert profiling, "docs/development.md must have a Profiling section"
    assert "perf-test --profile" in profiling, (
        "Profiling section must document the `just perf-test --profile` flag"
    )
    assert "sudo" in profiling, (
        "Profiling section must document the passwordless-sudo prerequisite "
        "(the actual prereq — servicers run as root, so samply attaches via sudo)"
    )
    assert "perf_event_paranoid" in profiling, (
        "Profiling section must explain kernel.perf_event_paranoid "
        "(why root/sudo bypasses it, so no sysctl tuning is needed)"
    )
    assert ".perf/profile-" in profiling, (
        "Profiling section must document the .perf/profile-<svc>.json output location"
    )
