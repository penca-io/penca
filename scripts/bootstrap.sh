#!/usr/bin/env bash
# CHA-334: single-command dev bootstrap.
#
# Run from the repo root via `just bootstrap`. Installs every dev
# prerequisite that isn't `just` itself, then wires the agent tooling
# (kata + roborev), memory symlink (ADR 0016), and pre-commit hooks
# (`pre-commit` + `commit-msg` stages).
#
# Idempotent end-to-end: every step gates on a `command -v` (or
# pkg-specific) check and is safe to re-run after a binary upgrade or
# pulling a config change. Partial failures (e.g. one installer hit a
# network blip) recover on the next invocation — no cleanup step
# required.
set -euo pipefail

log() { printf '─── %s\n' "$*"; }
skip() { printf '    (skip) %s\n' "$*"; }

# 1. clear stray core.hooksPath ---------------------------------------
# Both the roborev post-commit hook (step 10) and pre-commit (step 11)
# install into and resolve from `.git/hooks`; a leftover `core.hooksPath`
# redirects hook resolution elsewhere and makes pre-commit refuse to
# install ("Cowardly refusing to install hooks with core.hooksPath set").
# Gate on the local value being present so we only mutate config when
# there's actually a stray to clear. Scoped to local config (the
# likely-unintentional case) — a deliberate --global value is left alone.
if git config --local --get core.hooksPath >/dev/null 2>&1; then
    log "Clearing stray local core.hooksPath ($(git config --local --get core.hooksPath))"
    git config --local --unset-all core.hooksPath
else
    skip "no local core.hooksPath set"
fi

# 2. uv ----------------------------------------------------------------
if command -v uv >/dev/null 2>&1; then
    skip "uv already on PATH ($(command -v uv))"
else
    log "Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # uv's installer puts it at ~/.local/bin or ~/.cargo/bin; ensure
    # the rest of this script can find it on PATH for the pre-commit
    # step below.
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"
fi

# 3. rustup → rust toolchain → rust-analyzer --------------------------
if command -v rustup >/dev/null 2>&1; then
    skip "rustup already on PATH ($(command -v rustup))"
else
    log "Installing rustup"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain none
    export PATH="$HOME/.cargo/bin:$PATH"
fi
log "Installing Rust toolchain (reads rust-toolchain.toml)"
rustup toolchain install
log "Adding rust-analyzer component"
rustup component add rust-analyzer

# 3b. dev-only cargo tools (samply, cargo-sweep) ----------------------
# Installed via `just install-tools`. `cargo install --locked` is
# idempotent (no-ops when the binary is already present), consistent
# with this script's re-runnable design. cargo-sweep is required by
# `just vm-gc` to reclaim the host target/ tree (CHA-363); placed after
# step 3 so cargo is on PATH.
if command -v samply >/dev/null 2>&1 && command -v cargo-sweep >/dev/null 2>&1; then
    skip "dev cargo tools already present (samply, cargo-sweep)"
else
    log "Installing dev-only cargo tools (samply, cargo-sweep)"
    just install-tools
fi

# 3c. BuildKit builder GC policy: single LRU cap, no idle tier --------
# CHA-363: the build VM's daily ~1h cold Rust rebuild was caused by the
# builder GC evicting the ~2.5GB cargo-chef cook layer on an idle timer.
# A tiered policy with a short idle window (e.g. a 512MB cap on layers
# idle past 2h) culls the cook layer after any overnight gap, forcing a
# re-cook of ~590 release crates the next morning. A single LRU tier
# keyed only on total storage never evicts on idle: the cook layer is
# touched every build, so under an LRU cap the stale per-branch target/
# deltas are dropped first and the always-reused cook layer survives.
# Keep the 8GB ceiling — the VM disk is small; do NOT raise it or add an
# idle-window tier back.
#
# Merge into any existing /etc/docker/daemon.json via python3: top-level
# keys and other `builder` siblings are preserved, but the whole
# `builder.gc` subtree is replaced (that's the point — collapse any old
# tiers/defaultKeepStorage to a single LRU cap; it is not a deep merge).
# Write + restart the daemon only when the policy actually differs, so
# re-running bootstrap never bounces docker needlessly. This is the first
# sudo in bootstrap itself; bootstrap is interactive (docker-ensure
# already uses sudo elsewhere).
if command -v docker >/dev/null 2>&1; then
    log "Configuring BuildKit builder GC policy (single LRU cap)"
    if merged_daemon_json=$(python3 - <<'PY'
import json, os, sys

path = "/etc/docker/daemon.json"
desired_gc = {"enabled": True, "policy": [{"keepStorage": "8GB", "all": True}]}

data = {}
if os.path.exists(path):
    with open(path) as f:
        text = f.read().strip()
    if text:
        data = json.loads(text)

if data.get("builder", {}).get("gc") == desired_gc:
    sys.exit(2)  # already correct — signal "no write needed"

data.setdefault("builder", {})["gc"] = desired_gc
json.dump(data, sys.stdout, indent=2)
PY
    ); then
        # printf adds the trailing newline that $(...) strips from the
        # captured JSON, so the written file ends in a newline.
        printf '%s\n' "$merged_daemon_json" | sudo tee /etc/docker/daemon.json >/dev/null
        if command -v systemctl >/dev/null 2>&1; then
            sudo systemctl restart docker
            log "Installed single-tier BuildKit GC policy; restarted docker"
        else
            log "Wrote single-tier BuildKit GC policy; restart docker manually to apply (no systemctl)"
        fi
    else
        rc=$?
        if [ "$rc" -eq 2 ]; then
            skip "BuildKit GC policy already a single LRU cap"
        else
            echo "ERROR: could not compute desired daemon.json (python exit $rc)" >&2
            exit 1
        fi
    fi
else
    skip "docker not installed — leaving BuildKit GC policy unconfigured"
fi

# 3d. cargo on PATH for Claude's Bash tool (CHA-364) ------------------
# rustup's `. "$HOME/.cargo/env"` only runs in login shells (~/.profile)
# and interactive shells (~/.bashrc, after its non-interactive early
# return). Claude Code's Bash tool — and CI/automation — run a
# non-interactive non-login shell, which reads neither: a plain
# `bash -c` only sources `$BASH_ENV`. With it unset, `cargo` is off PATH
# and `just check` dies with `cargo: not found` (exit 127) after the
# uv-based lint/static-test steps pass.
#
# Claude injects `settings.json`'s `env` into every Bash tool shell, so
# point BASH_ENV at ~/.cargo/env there. Write the project-local,
# git-ignored .claude/settings.local.json (so bootstrap never dirties
# the tracked tree), merging the key in via python3 to preserve siblings
# like enabledMcpjsonServers, and only when the value actually differs.
# bash ignores a missing BASH_ENV file, so this is harmless without rust.
claude_settings=".claude/settings.local.json"
if [ -d .claude ]; then
    log "Setting BASH_ENV in $claude_settings so tool shells get cargo on PATH"
    if merged_settings=$(BASH_ENV_PATH="$HOME/.cargo/env" python3 - "$claude_settings" <<'PY'
import json, os, sys

path = sys.argv[1]
desired = os.environ["BASH_ENV_PATH"]

data = {}
if os.path.exists(path):
    with open(path) as f:
        text = f.read().strip()
    if text:
        data = json.loads(text)

if data.get("env", {}).get("BASH_ENV") == desired:
    sys.exit(2)  # already correct — signal "no write needed"

data.setdefault("env", {})["BASH_ENV"] = desired
json.dump(data, sys.stdout, indent=2)
PY
    ); then
        printf '%s\n' "$merged_settings" > "$claude_settings"
        log "Set BASH_ENV=$HOME/.cargo/env in $claude_settings"
    else
        rc=$?
        if [ "$rc" -eq 2 ]; then
            skip "BASH_ENV already set in $claude_settings"
        else
            echo "ERROR: could not update $claude_settings (python exit $rc)" >&2
            exit 1
        fi
    fi
else
    skip "no .claude/ dir — skipping BASH_ENV settings step"
fi

# 4. go (required — not auto-installed) -------------------------------
if ! command -v go >/dev/null 2>&1; then
    cat >&2 <<'EOF'
ERROR: go is required but not installed.
Install Go 1.26.3+ from https://go.dev/dl/ and re-run `just bootstrap`.
(Auto-installing Go is out of scope — platform variance across apt /
brew / tarball / version pinning makes a single one-liner unreliable.)
EOF
    exit 1
fi
skip "go already on PATH ($(command -v go))"

# 5. mcp-language-server ----------------------------------------------
if command -v mcp-language-server >/dev/null 2>&1; then
    skip "mcp-language-server already on PATH"
else
    log "Installing mcp-language-server via go install"
    go install github.com/isaacphi/mcp-language-server@latest
fi

# 6. kata --------------------------------------------------------------
if command -v kata >/dev/null 2>&1; then
    skip "kata already on PATH ($(command -v kata))"
else
    log "Installing kata via go install"
    go install go.kenn.io/kata/cmd/kata@latest
fi

# 7. roborev -----------------------------------------------------------
if command -v roborev >/dev/null 2>&1; then
    skip "roborev already on PATH ($(command -v roborev))"
else
    log "Installing roborev"
    curl -fsSL https://roborev.io/install.sh | bash
fi

# 7b. headroom context-compression proxy (CHA-465) -------------------
# Opt-in dev tool: a local proxy that compresses what an agent reads
# (tool outputs, file reads, query results) before it reaches the LLM.
# Installed here so a fresh VM has it available, but bootstrap does NOT
# point Claude Code at it — enabling is a deliberate `just headroom-proxy`
# + ANTHROPIC_BASE_URL launch (see README). uv is on PATH from step 2;
# `uv tool install` matches the repo's uv-managed tooling and isolates
# the package on PATH.
if command -v headroom >/dev/null 2>&1; then
    skip "headroom already on PATH ($(command -v headroom))"
else
    log "Installing headroom compression proxy via uv tool install"
    # The `headroom-ai[proxy]` package installs a console script named
    # `headroom` (per the docs quickstart: `headroom proxy --port 8787`)
    # — that's the binary the command -v gate above and the
    # `just headroom-proxy` recipe rely on.
    #
    # Non-fatal by design: headroom is optional / opt-in, so an install
    # failure (network blip, PyPI hiccup, a package yank) must NOT abort
    # bootstrap and skip the required steps that follow (java, Flight SQL
    # JDBC driver, agent tooling, memory symlink, pre-commit). The
    # command -v gate makes a later `just bootstrap` retry cleanly once
    # the install succeeds.
    uv tool install "headroom-ai[proxy]" \
        || log "headroom install failed — optional, skipping (re-run \`just bootstrap\` to retry)"
fi

# 8. java (required — not auto-installed) + Flight SQL JDBC driver ----
# JDK 21 + the flight-sql-jdbc-driver JAR back the `[jdbc]` half of the
# Flight SQL integration matrix (the DataGrip/DBeaver surface). They are
# hard prerequisites — the tests now fail rather than silently skip when
# they're absent, so bootstrap must provision both.
if ! command -v java >/dev/null 2>&1; then
    cat >&2 <<'EOF'
ERROR: java is required but not installed.
Install JDK 21 (`apt install openjdk-21-jdk-headless` or equivalent) and
re-run `just bootstrap`. (Auto-installing the JDK is out of scope —
platform variance across apt / brew / tarball / version pinning makes a
single one-liner unreliable.)
EOF
    exit 1
fi
skip "java already on PATH ($(command -v java))"
log "Fetching Flight SQL JDBC driver JAR"
just fetch-jdbc-driver

# 9. memory symlink (ADR 0016) ----------------------------------------
log "Wiring memory symlink"
just memory-symlink-bootstrap

# 10. kata daemon + project binding + roborev + optional issue-graph client --
# `just init-agent-tools` also applies the roborev review calibration
# (`review_min_severity`, `review_guidelines`) AFTER `roborev init`, which would
# otherwise regenerate `.roborev.toml` from defaults and drop it — see the
# recipe for why each is set.
#
# It also wires the OPTIONAL scoped shared issue-graph
# client (CHA-447): when PENCA_KATA_GRAPH_URL is set it probes the shared kata
# instance that scripts/kata-issue-graph.sh reads; unset leaves the VM
# local-only (the default). No global KATA_SERVER is exported — that stays
# scoped to the wrapper so the local task-queue daemon remains authoritative.
log "Wiring agent tools (kata daemon, project binding, roborev hook + review calibration, optional issue-graph client)"
just init-agent-tools

# 11. pre-commit hooks -------------------------------------------------
log "Installing pre-commit hooks (pre-commit + commit-msg stages)"
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg

log "Bootstrap complete. Try \`just check\`."
