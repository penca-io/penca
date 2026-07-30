# Penca development commands
# Install just: uv tool install rust-just

# Per-worktree compose project name. Without this, every worktree's
# `docker compose -f docker/compose.yml` resolves to project `docker`
# (basename of compose.yml's parent), so containers/networks/volumes
# collide across parallel worktrees even with random host ports. Deriving
# the project name from the worktree directory isolates each stack
# end-to-end (containers like `penca-cha-174-postgres-1`, volumes like
# `penca-cha-174_pgdata`).
export COMPOSE_PROJECT_NAME := "penca-" + `basename "$PWD"`

# Default recipe: list available commands
default:
    @just --list

# Create a worktree at .claude/worktrees/<dir> on a new branch. Sets up the
# per-worktree memory symlink (ADR 0016) and a worktree-local virtualenv.
#
# Usage:
#   just worktree-new nhobin219/cha-201-foo          → dir = cha-201-foo (basename of branch)
#   just worktree-new nhobin219/cha-201-foo cha-201  → dir = cha-201 (explicit override)
[no-cd]
worktree-new branch dir="":
    #!/usr/bin/env bash
    set -euo pipefail
    dir="{{dir}}"
    [ -z "$dir" ] && dir=$(basename "{{branch}}")
    git worktree add ".claude/worktrees/$dir" -b "{{branch}}"
    cd ".claude/worktrees/$dir"
    # Per-worktree memory symlink so each session reads/writes its own
    # .claude/memory/ instead of stomping a shared global path (ADR 0016).
    # Skipped pre-migration when .claude/memory/ doesn't exist on the branch yet.
    # Slug uses tr "/." "-" to match Claude Code's project-slug derivation —
    # both / and . in the absolute path become - (so /home/.../.claude/... →
    # -home-...--claude-..., note the double dash where the dot used to be).
    if [ -d .claude/memory ]; then
        slug=$(realpath . | tr "/." "-")
        target_project="$HOME/.claude/projects/$slug"
        mkdir -p "$target_project"
        [ -e "$target_project/memory" ] || \
            ln -s "$(realpath .)/.claude/memory" "$target_project/memory"
    fi
    # Worktree-local virtualenv. unset VIRTUAL_ENV avoids inheriting the parent's,
    # which would rewrite the parent worktree's editable-install .pth files.
    unset VIRTUAL_ENV
    uv sync --all-packages

# One-time bootstrap (ADR 0016): replace ~/.claude/projects/<slug>/memory/
# with a symlink to <repo>/.claude/memory/. Idempotent — skips if the symlink
# is already in place. The previous memory directory (if any) is preserved
# at ~/.claude/projects/<slug>/memory-pre-symlink/ as a safety net.
#
# Run this once on each machine after pulling the ADR 0016 migration.
memory-symlink-bootstrap:
    #!/usr/bin/env bash
    set -euo pipefail
    # Slug uses tr "/." "-" to match Claude Code's project-slug derivation.
    slug=$(realpath . | tr "/." "-")
    target="$HOME/.claude/projects/$slug/memory"
    repo_memory="$(realpath .)/.claude/memory"
    if [ ! -d "$repo_memory" ]; then
        echo "ERROR: $repo_memory does not exist. Pull the ADR 0016 migration first."
        exit 1
    fi
    if [ -L "$target" ]; then
        existing=$(readlink "$target")
        if [ "$existing" = "$repo_memory" ]; then
            echo "Symlink already in place: $target → $repo_memory"
            exit 0
        else
            echo "ERROR: $target is a symlink to $existing (expected $repo_memory). Remove manually if intended."
            exit 1
        fi
    fi
    mkdir -p "$HOME/.claude/projects/$slug"
    if [ -d "$target" ]; then
        echo "Backing up existing memory directory to ${target}-pre-symlink/"
        mv "$target" "${target}-pre-symlink"
    fi
    ln -s "$repo_memory" "$target"
    echo "Symlinked $target → $repo_memory"
    echo "Original (if any) preserved at ${target}-pre-symlink/. Safe to delete after a week of confirmed-working sessions."

# Remove a worktree, its branch (if merged), and its per-worktree project slug
# under ~/.claude/projects/.
#
# Usage:
#   just worktree-remove nhobin219/cha-201-foo          → dir = cha-201-foo
#   just worktree-remove nhobin219/cha-201-foo cha-201  → dir = cha-201
worktree-remove branch dir="":
    #!/usr/bin/env bash
    set -euo pipefail
    dir="{{dir}}"
    [ -z "$dir" ] && dir=$(basename "{{branch}}")
    worktree_path="$(realpath .)/.claude/worktrees/$dir"
    if [ -d "$worktree_path" ]; then
        # Slug uses tr "/." "-" to match Claude Code's project-slug derivation.
        slug=$(echo "$worktree_path" | tr "/." "-")
        rm -rf "$HOME/.claude/projects/$slug"
    fi
    git worktree remove ".claude/worktrees/$dir"
    git branch -d "{{branch}}"

# Single-command dev bootstrap (CHA-334). Run from the repo root after
# cloning. `just` is the only prerequisite — this recipe installs
# everything else (uv, rustup + Rust toolchain + rust-analyzer,
# mcp-language-server, kata, roborev) and wires the agent tooling
# (memory symlink, kata daemon + project binding, roborev post-commit
# hook) and pre-commit hooks (pre-commit + commit-msg stages).
#
# Idempotent: safe to re-run after a binary upgrade, after pulling a
# config change, or to recover from a partial failure.
bootstrap:
    bash scripts/bootstrap.sh

# Install dev-only tools not pinned in Cargo.toml or pyproject.toml.
# Run once after cloning. Currently installs:
#   - samply: CPU profiler used by the perf-engineer agent and during
#     manual perf investigation. See README "Profiling".
#   - cargo-sweep: reclaims stale target/ artifacts; used by `just vm-gc`
#     to bound the host build tree the BuildKit GC can't see (CHA-363).
install-tools:
    cargo install --locked samply
    cargo install --locked cargo-sweep

# Bound the disk pools the BuildKit builder GC policy can't (CHA-363):
# dangling docker images (no per-GB daemon budget) and stale host
# target/ artifacts. The build cache itself self-bounds via the daemon
# GC policy installed by scripts/bootstrap.sh. No sudo — safe for CI /
# agent callers, so it's wired as a penca-up dependency below.
vm-gc:
    #!/usr/bin/env bash
    set -euo pipefail
    # Dangling-only: drops orphaned prior penca-rust-server layers,
    # never tagged base images (postgres, seaweedfs) or the in-use image.
    # CI (PENCA_SKIP_BUILD=1) keeps its prebuilt tagged image. Best-effort:
    # vm-gc is a penca-up prerequisite, so a transient daemon hiccup (busy
    # daemon, race with a parallel-worktree prune) must not abort bring-up.
    docker image prune -f || echo "(skip) docker image prune failed — continuing"
    # mtime-stat only (cheap even on a multi-GB tree); preserves the warm
    # incremental state `just check` needs while evicting >3d-stale deltas.
    # 3 days (was 7): the sccache S3 cache makes evicted dep artifacts
    # cheap to re-materialize, so the window only needs to protect the
    # workspace incremental state (non-cacheable in sccache) across a
    # standard weekend (Fri → Mon ≈ 2.9d — just inside). Longer gaps
    # (holiday weekends) pay one ~minutes workspace recompile on the
    # next build; accepted trade for the extra reclaimed disk.
    if command -v cargo-sweep >/dev/null 2>&1; then
        cargo sweep -t 3 .
    else
        echo "(skip) cargo-sweep not installed — run 'just install-tools'"
    fi

# Initialize build-speed tooling: sccache, an S3-backed rustc cache
# (bucket fabric-sccache in us-west-1) shared across ticket VMs so cold
# builds pull compiled deps instead of recompiling them. Idempotent: safe
# to re-run on a fresh VM; runs automatically as a dependency of
# init-agent-tools. The sccache version is pinned; the toolchain pin in
# rust-toolchain.toml is what keeps cache keys identical across VMs.
# No custom linker: rust 1.94 already defaults to the fast rust-lld on
# x86_64-linux, and extra rustflags would fork the cache keyspace.
init-build-tools:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v sccache >/dev/null 2>&1; then
        mkdir -p "$HOME/.cargo/bin"
        tmp=$(mktemp -d)
        trap 'rm -rf "$tmp"' EXIT
        curl -fsSL -o "$tmp/sccache.tar.gz" \
            https://github.com/mozilla/sccache/releases/download/v0.15.0/sccache-v0.15.0-x86_64-unknown-linux-musl.tar.gz
        echo "782d2b5dd7ae0a55ebe368ab258114d0928d019ac2d949ab85d5d02f3926709e  $tmp/sccache.tar.gz" \
            | sha256sum -c --quiet
        tar xzf "$tmp/sccache.tar.gz" --strip-components=1 -C "$HOME/.cargo/bin" --wildcards '*/sccache'
        echo "Installed sccache v0.15.0"
    fi
    mkdir -p "$HOME/.config/sccache"
    if ! grep -q "fabric-sccache" "$HOME/.config/sccache/config" 2>/dev/null; then
        cat > "$HOME/.config/sccache/config" <<'SCCACHE_EOF'
    [cache.s3]
    bucket = "fabric-sccache"
    region = "us-west-1"
    # false = use the standard AWS credential chain (required field).
    no_credentials = false
    SCCACHE_EOF
        echo "Wrote ~/.config/sccache/config"
    fi
    if [ -e "$HOME/.cargo/config" ] && ! grep -q "rustc-wrapper" "$HOME/.cargo/config"; then
        echo "ERROR: legacy ~/.cargo/config exists; cargo prefers it and would" >&2
        echo "silently ignore the config.toml this recipe writes. Rename it to" >&2
        echo "config.toml (or merge 'rustc-wrapper = \"sccache\"' and 'jobs = 6'" >&2
        echo "into its [build] table) and re-run." >&2
        exit 1
    elif [ -e "$HOME/.cargo/config" ]; then
        # Legacy file already carries the wrapper — configured; nothing to
        # write (a config.toml would be shadowed anyway).
        :
    elif [ ! -e "$HOME/.cargo/config.toml" ]; then
        cat > "$HOME/.cargo/config.toml" <<'CARGO_EOF'
    [build]
    # 16-core / 15 GiB box: each rustc peaks ~1-2 GiB, so 16 parallel jobs
    # OOMs the machine. 6 leaves headroom for docker + the test stack.
    jobs = 6
    # S3-backed compile cache (bucket fabric-sccache, us-west-1); backend
    # config lives in ~/.config/sccache/config.
    rustc-wrapper = "sccache"
    CARGO_EOF
        echo "Wrote ~/.cargo/config.toml (sccache wrapper)"
    elif ! grep -q "rustc-wrapper" "$HOME/.cargo/config.toml"; then
        echo "ERROR: ~/.cargo/config.toml exists but has no rustc-wrapper entry." >&2
        echo "Add 'rustc-wrapper = \"sccache\"' and 'jobs = 6' to its [build]" >&2
        echo "table manually (not overwriting a config this recipe does not own)." >&2
        exit 1
    fi
    sccache --stop-server >/dev/null 2>&1 || true
    sccache --show-stats

# Initialize this workspace's agent-side workflow tooling (kata + roborev).
# Assumes the binaries are already installed (see README "To develop" for
# install commands). Idempotent: safe to re-run after a fresh checkout or
# VM rebuild.
#   - Symlinks kata onto PATH (~/go/bin/kata → ~/.local/bin/kata) when needed.
#   - Starts the kata daemon if not already running.
#   - Binds this workspace to the kata `penca` project.
#   - Registers the repo with the roborev daemon + installs the post-commit hook.
init-agent-tools: init-build-tools
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v kata >/dev/null 2>&1; then
        if [ -x "$HOME/go/bin/kata" ]; then
            mkdir -p "$HOME/.local/bin"
            ln -sf "$HOME/go/bin/kata" "$HOME/.local/bin/kata"
            echo "Symlinked $HOME/go/bin/kata → $HOME/.local/bin/kata"
        else
            echo "kata binary not found (expected at ~/go/bin/kata)" >&2
            exit 1
        fi
    fi
    if ! kata daemon status >/dev/null 2>&1; then
        nohup kata daemon start >/tmp/kata-daemon.log 2>&1 &
        disown
        sleep 1
    fi
    # `kata init` is idempotent and gates on the daemon's project DB,
    # not the local `.kata.toml` (which is checked into the repo). A
    # fresh VM's daemon DB doesn't know about penca even when
    # .kata.toml exists, so this must run unconditionally — CHA-334.
    kata init --project penca
    roborev init --agent claude-code

    # Review calibration (CHA-531 retro). `roborev init` regenerates
    # `.roborev.toml` from defaults, so these must be (re)applied after it —
    # they are `config set` calls rather than a hand-edited file so the tool
    # owns the serialization. Both are idempotent.
    #
    # `review_min_severity`: the kata bridge below turns every emitted finding
    # into a task that blocks PR open, so a Low costs the same to clear as a
    # High. Lows dominated volume (25 of 39 findings on 2026-07-28) and were
    # closed by judgement rather than fixed.
    #
    # `review_guidelines`: injected into every review prompt. Roborev reviews
    # ONE commit's diff with no view of the branch plan and no memory of prior
    # reviews, so without this it re-raises concerns a later commit already
    # addresses. Refresh the text from `roborev insights`, which mines the
    # review history for findings consistently dismissed without a code change.
    roborev config set review_min_severity medium
    roborev config set review_guidelines "$(cat scripts/roborev-review-guidelines.md)"

    # Optional shared issue-graph client (CHA-447). When a shared kata instance
    # is provisioned via PENCA_KATA_GRAPH_URL, probe it so the operator knows
    # the scoped read-only client (scripts/kata-issue-graph.sh) can reach it.
    # Opt-in: unset => local-only, today's default. We deliberately do NOT set a
    # global KATA_SERVER here — it is process-global and would route the local
    # cha-NNN task-queue drain to the shared daemon; reads go via the wrapper.
    if [ -n "${PENCA_KATA_GRAPH_URL:-}" ]; then
        if curl -fsS -m 5 "${PENCA_KATA_GRAPH_URL%/}/api/v1/ping" >/dev/null 2>&1; then
            echo "Shared issue-graph client configured → ${PENCA_KATA_GRAPH_URL} (reachable)"
        else
            echo "Shared issue-graph client configured → ${PENCA_KATA_GRAPH_URL} (unreachable — check the daemon/token)"
        fi
    else
        echo "(skip) shared issue-graph client not configured (PENCA_KATA_GRAPH_URL unset; local-only)"
    fi

# Close open roborev jobs for <branch> and soft-delete closed kata tasks
# under <cha>. Used by /do-issue Step 6 after PR merge.
#   `just clean-agent-tools cha-331 nhobin219/cha-331-foo`
# Kata delete is soft (event log retains the audit trail; `kata restore`
# can resurrect any task). `--confirm` wants the qualified-id
# (project#short_id), not the bare short_id — kata's help text is wrong.
# Both list commands serialize an empty result as JSON null (`null` /
# `{"issues":null}`), so the iterators need `?` or pipefail aborts the
# recipe when there's nothing left to clean.
clean-agent-tools cha branch:
    #!/usr/bin/env bash
    set -euo pipefail
    # Both list commands cap their output — `roborev list` at 50, `kata list` at
    # 200 — so listing once and iterating leaves everything past the cap behind
    # while still exiting 0. CHA-517 hit this: 69 open roborev jobs, 50 closed,
    # 19 silently stranded. So page until a pass has nothing new to try, taking
    # each tool's own default page size rather than naming a constant here that
    # would drift from it.
    #
    # Three properties this has to hold, each learned by getting it wrong:
    #
    #   Liveness — two independent ways a pass can make no progress, and both
    #   must stop it. An id that keeps failing is remembered and skipped, so a
    #   pass eventually attempts nothing and returns. An id the tool reports it
    #   *succeeded* on but that is still listed next pass (stale daemon, an id
    #   resolving to another scope, a soft-delete the listing does not reflect)
    #   is caught by the listing, since exit status says it worked. Guarding on
    #   only one of these hangs on the other; comparing listing text instead
    #   fails on a mere reorder.
    #
    #   A listing failure is a failure — never an empty worklist. Swallowing it
    #   would exit 0 having cleaned nothing, which is the exact silent
    #   under-clean this recipe exists to prevent.
    #
    #   Honest counts — a failing id is re-listed every pass, so counting
    #   attempts reports one stranded item several times. Failed ids are
    #   recorded per drain and skipped, so the count is of distinct items.
    failures=0

    list_roborev_jobs() {
        roborev list --branch "{{branch}}" --open --json | jq -r '.[]?.id'
    }

    close_roborev_job() {
        roborev close "$1"
    }

    list_kata_tasks() {
        kata list --label "{{cha}}" --status closed --json | jq -r '.issues[]?.qualified_id'
    }

    delete_kata_task() {
        kata delete "$1" --force --confirm "DELETE $1"
    }

    # Function names rather than eval'd command strings, so tool output is never
    # re-parsed as shell source. The `for id in $ids` split below is deliberate
    # and is the one place ids are word-split: both tools emit one bare id per
    # line with no whitespace or globbing characters, which is what makes the
    # split safe and the quoting everywhere else load-bearing.
    drain() {
        local kind="$1" list_fn="$2" act_fn="$3"
        local failed="" acted="" ids id err attempted

        while :; do
            if ! ids="$("$list_fn")"; then
                # Not "nothing cleaned": the listing runs every pass, so this
                # can land after several successful ones. What is certain is
                # that the remainder is unknown and untouched.
                echo "  (could not list remaining items to $kind — some may be left)" >&2
                failures=$((failures + 1))
                return
            fi

            [ -z "$ids" ] && return

            attempted=0
            for id in $ids; do
                case " $failed " in *" $id "*) continue ;; esac

                # Acted on successfully last pass and still listed: the action
                # is a no-op for this item, so retrying it forever is the one
                # way this loop can fail to terminate. Exit status cannot detect
                # it — the tool said it worked — so the listing is the evidence.
                case " $acted " in
                    *" $id "*)
                        echo "  (could not $kind $id: reported success but it is still listed)" >&2
                        failed="$failed $id"
                        failures=$((failures + 1))
                        continue
                        ;;
                esac

                attempted=1
                if err="$("$act_fn" "$id" 2>&1 >/dev/null)"; then
                    acted="$acted $id"
                else
                    # Keep the tool's own explanation — "daemon unreachable",
                    # "unknown id" — which is the whole diagnostic value here.
                    echo "  (could not $kind $id: $err)" >&2
                    failed="$failed $id"
                    failures=$((failures + 1))
                fi
            done

            # Every remaining id has already failed, so another pass is futile.
            [ "$attempted" -eq 0 ] && return
        done
    }

    drain "close roborev job" list_roborev_jobs close_roborev_job
    drain "delete kata task" list_kata_tasks delete_kata_task

    # Exit non-zero on any failure: the point of this recipe is that state does
    # not accumulate, and reporting success while leaving some behind is the bug
    # it exists to prevent.
    if [ "$failures" -gt 0 ]; then
        echo "clean-agent-tools: $failures item(s) could not be cleaned" >&2
        exit 1
    fi


# Launch the Headroom context-compression proxy (CHA-465). Opt-in: run
# this in a separate shell, then start Claude Code with
# ANTHROPIC_BASE_URL=http://localhost:8787 to route its API calls through
# the proxy. The default dev loop does not use it. See README.
headroom-proxy:
    headroom proxy --port 8787

# Regenerate Python protobuf bindings from all .proto files
compile-protos-py:
    uv run python -m grpc_tools.protoc \
        --proto_path=protos \
        --python_out=packages/penca-proto/src \
        --pyi_out=packages/penca-proto/src \
        --grpc_python_out=packages/penca-proto/src \
        protos/penca_proto/external/v1/common.proto \
        protos/penca_proto/external/v1/lifecycle.proto \
        protos/penca_proto/external/v1/write.proto \
        protos/penca_proto/external/v1/query.proto

# Rebuild Rust protobuf bindings (runs automatically via build.rs, but
# this recipe forces a clean rebuild)
compile-protos-rs:
    cargo build -p penca-proto

# Regenerate protobuf bindings for both Python and Rust
compile-protos: compile-protos-py compile-protos-rs

# Run ruff linter over the whole repo. The scope matches the repo-wide
# pre-commit hook (`pass_filenames: false`) on purpose: when this recipe was
# scoped to packages/penca-client/, examples/ and tests/ drifted out of
# compliance and only the hook noticed, silently rewriting files mid-commit.
lint:
    uv run ruff check .

# Run ruff formatter + blank line fixer
format path=".":
    uv run ruff format {{path}}
    python scripts/check_blank_lines.py --fix {{path}}

# Check formatting without modifying files
format-check path=".":
    uv run ruff format --check {{path}}
    python scripts/check_blank_lines.py {{path}}

# Run Python unit tests (no infra required).
test *args:
    uv run --package penca-client pytest packages/penca-client/tests/unit/ {{args}}

# Run static checks under tests/static/. These are source-file assertions
# (cross-language naming parity, skill→agent orchestration invariants,
# etc.) — no Docker, no fixtures, no penca services. Pass file-name
# prefixes to scope:
#   just static-test naming_parity skill_orchestration
static-test *prefixes:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ -z "{{prefixes}}" ]; then
        uv run pytest tests/static/ -s
    else
        files=""
        for prefix in {{prefixes}}; do
            files="$files tests/static/static_${prefix}_test.py"
        done
        uv run pytest $files -s
    fi

# Run all pre-push gates: Python lint + format-check + unit tests + static
# checks, plus Rust clippy + fmt-check + test. Mirrors what CI runs so a
# green `just check` should mean a green PR.
check: lint format-check test static-test cargo-check

# Build all Rust crates
cargo-build:
    cargo build --workspace

# Run Rust tests
cargo-test:
    cargo test --workspace

# Run clippy lints on all Rust crates. `--all-targets` is load-bearing: without
# it clippy skips `#[cfg(test)]` code, so an orphaned test helper or an unused
# test-only import passes this gate and only surfaces on a `cargo test` build.
cargo-clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Check Rust formatting
cargo-fmt-check:
    cargo fmt --all -- --check

# Format Rust code
cargo-fmt:
    cargo fmt --all

# Full Rust check: clippy + fmt + test
cargo-check: cargo-clippy cargo-fmt-check cargo-test

# Auto-fix Rust lints and formatting. `--all-targets` matches `cargo-clippy`: a
# fixer that cannot reach what the gate flags is a worse trap than one that
# rewrites test code, which `--allow-dirty` already warns it will do.
cargo-fix:
    cargo clippy --workspace --all-targets --fix --allow-dirty
    cargo fmt --all

# Ensure Docker Engine is running and accessible.
#
# Docker has two backends on Linux:
#   - Docker Engine: native daemon, talks via /var/run/docker.sock.
#     No VM overhead. Requires your user in the `docker` group:
#       sudo usermod -aG docker $USER && newgrp docker
#   - Docker Desktop: runs a QEMU VM (~2GB RAM) that hosts its own
#     daemon at ~/.docker/desktop/docker.sock. No group needed.
#
# Docker contexts control which backend the CLI talks to:
#   docker context ls              # list available contexts
#   docker context use default     # switch to Engine (native)
#   docker context use desktop-linux  # switch back to Desktop
#
# This recipe auto-selects Engine when available. To force Desktop,
# skip this recipe and run: docker context use desktop-linux
docker-ensure:
    #!/usr/bin/env bash
    set -euo pipefail

    # Prefer Docker Engine (native) over Desktop (QEMU VM).
    current=$(docker context show 2>/dev/null || echo "unknown")
    if [ "$current" = "desktop-linux" ]; then
        echo "Switching from Docker Desktop to Docker Engine (saves ~2GB RAM)..."
        docker context use default > /dev/null
    fi

    # Start dockerd if not running.
    if ! docker info > /dev/null 2>&1; then
        if command -v systemctl > /dev/null 2>&1; then
            echo "Starting Docker Engine..."
            sudo systemctl start docker
        else
            echo "Error: Docker daemon not running. Start it manually." >&2
            exit 1
        fi
    fi

    # Check group membership (only needed for Engine, not Desktop).
    if ! docker info > /dev/null 2>&1; then
        echo "Error: Cannot connect to Docker. Add yourself to the docker group:" >&2
        echo "  sudo usermod -aG docker \$USER && newgrp docker" >&2
        echo "Or switch back to Docker Desktop:" >&2
        echo "  docker context use desktop-linux" >&2
        exit 1
    fi

# Start infrastructure + the Rust servicers + Flight SQL gateway.
# Requires Docker daemon.
#
# Profile selects port bindings:
#   dev     -> fixed host ports + lifecycle scheduler running (DEFAULT)
#   test    -> random host ports (parallel-worktree safe) + scheduler idle,
#              so its tick loop cannot race a suite's manual lifecycle calls
#
# After containers are healthy, generates two host-env files with the
# actual Docker-assigned ports:
#   docker/.client.env    — the 6 PENCA_*_URL values PencaClient needs.
#                            Sourced by integration-test.
#   docker/.baseline.env  — PENCA_DB_* for direct Postgres access used
#                            by integration tests' white-box assertions.
#
# Note: no docker-ensure dependency — that recipe uses sudo which
# blocks non-interactive callers (CI, AI agents). Ensure Docker is
# running before invoking this recipe.
[arg("profile", long)]
[arg("db", long)]
penca-up profile="dev" db="": vm-gc
    #!/usr/bin/env bash
    set -euo pipefail

    compose_files="-f docker/compose.yml"
    env_file="--env-file docker/{{profile}}.env"

    # --db <dir>: persist Postgres and the object store under a host directory
    # instead of Docker-managed volumes, so the stack survives `penca-down` and
    # can be used as a real database. Compose substitutes these into the volume
    # short form, where a leading `/` makes it a bind mount; unset, the defaults
    # in compose.yml keep the named volumes and nothing changes.
    if [ -n "{{db}}" ]; then
        # Resolve WITHOUT creating anything (`realpath -m` tolerates a missing
        # path), because the repo check below refuses — and a refusal must not
        # leave the very directories it is refusing to use.
        db_dir="$(realpath -m "{{db}}")"

        # The Docker build context is the repo root (`context: ..` in
        # compose.yml), so a data directory inside the repo would be shipped to
        # the daemon on every build. Refuse rather than warn: penca-up builds by
        # default and Postgres creates its datadir mode 0700 owned by a container
        # uid, so the next build cannot read the context and FAILS outright.
        repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
        if [ -n "$repo_root" ]; then
            case "$db_dir/" in
                "$repo_root"/*)
                    echo "error: $db_dir is inside the repo, which is the Docker" >&2
                    echo "       build context (compose.yml uses \`context: ..\`)." >&2
                    echo "       penca-up builds by default, and Postgres creates" >&2
                    echo "       its datadir mode 0700 owned by a container uid —" >&2
                    echo "       so the next build cannot read the context and will" >&2
                    echo "       FAIL, not merely run slowly. It would also show up" >&2
                    echo "       in git status." >&2
                    echo "       Use a path outside the repo: --db ~/.penca/data" >&2
                    exit 1
                    ;;
            esac
        fi

        mkdir -p "$db_dir/pg" "$db_dir/s3"
        export PENCA_PG_VOLUME="$db_dir/pg"
        export PENCA_S3_VOLUME="$db_dir/s3"
        echo "Persistent storage: $db_dir (pg/ and s3/)"
    fi
    # Every service in both compose files is tagged with a profile (`infra`
    # or `penca-backend`), so a plain `docker compose up` would start
    # nothing. Activate both profiles for every invocation that acts on
    # containers (up / wait / down / port).
    profiles="--profile infra --profile penca-backend"

    # `up -d` respects `depends_on: service_completed_successfully` on
    # seaweedfs-init + bootstrap-init and `service_healthy` on
    # postgres/seaweedfs, so by the time it returns the full stack is
    # bootstrapped and ready for connections.
    #
    # `PENCA_SKIP_BUILD=1` tells compose to use the pre-existing
    # `penca-rust-server` image as-is — set by CI after a cache-aware
    # pre-build step. Local runs default to `--build` for fast
    # edit-run loops (BuildKit layer cache still applies).
    build_flag="--build"
    if [[ "${PENCA_SKIP_BUILD:-0}" == "1" ]]; then
        build_flag=""
    fi

    # CHA-439: opt the image build into the sccache→S3 compile cache when
    # host AWS creds exist; compose's secret source defaults to /dev/null
    # (which the Dockerfile degrades to a plain compile).
    if [ -s "$HOME/.aws/credentials" ]; then export PENCA_AWS_CREDENTIALS="$HOME/.aws/credentials"; fi
    if [ -s "$HOME/.aws/config" ]; then export PENCA_AWS_CONFIG="$HOME/.aws/config"; fi

    docker compose $compose_files $env_file $profiles up -d $build_flag

    # Query the ports Docker actually bound.
    pg_port=$(docker compose $compose_files $env_file $profiles port postgres 5432 | cut -d: -f2)
    s3_port=$(docker compose $compose_files $env_file $profiles port seaweedfs 8333 | cut -d: -f2)
    query_port=$(docker compose $compose_files $env_file $profiles port query 50052 | cut -d: -f2)
    write_port=$(docker compose $compose_files $env_file $profiles port write 50053 | cut -d: -f2)
    lifecycle_port=$(docker compose $compose_files $env_file $profiles port lifecycle 50054 | cut -d: -f2)
    penca_sql_port=$(docker compose $compose_files $env_file $profiles port penca-sql-server 50060 | cut -d: -f2)

    # Client env: gRPC channel URLs + Flight SQL URL.
    sed "s/__QUERY_PORT__/$query_port/; \
         s/__WRITE_PORT__/$write_port/; \
         s/__LIFECYCLE_PORT__/$lifecycle_port/; \
         s/__PENCA_SQL_PORT__/$penca_sql_port/" \
        docker/template.client.env > docker/.client.env

    # Baseline env: direct Postgres connection for white-box tests.
    sed "s/__DB_PORT__/$pg_port/" \
        docker/template.baseline.env > docker/.baseline.env

    echo "Profile: {{profile}}"
    if [ -n "{{db}}" ]; then
        echo "Storage: ${PENCA_PG_VOLUME%/pg} (persistent — survives penca-down)"
    else
        echo "Storage: docker volumes (wiped by penca-down)"
    fi
    echo "Postgres on :$pg_port, SeaweedFS on :$s3_port"
    echo "Servicers — query:$query_port write:$write_port lifecycle:$lifecycle_port"
    echo "Flight SQL — penca-sql-server:$penca_sql_port"
    echo "Generated docker/.client.env and docker/.baseline.env"

# Stop infrastructure + servicers and remove volumes. Requires Docker daemon.
#
# Before teardown, dumps per-service logs to
# /tmp/penca-logs-$(basename $PWD)/ so failures stay debuggable after
# the stack is removed. The basename suffix keeps parallel worktrees
# from clobbering each other.
[arg("profile", long)]
penca-down profile="dev":
    #!/usr/bin/env bash
    set -euo pipefail
    log_dir="/tmp/penca-logs-$(basename "$PWD")"
    rm -rf "$log_dir" && mkdir -p "$log_dir"
    printf '\n┌─ Service logs (saved before teardown) ──────────────────────────────\n'
    # Container names follow `${COMPOSE_PROJECT_NAME}-${svc}-1`. The
    # Justfile exports COMPOSE_PROJECT_NAME from the worktree basename
    # (`penca-<dir>`), so reuse that env var here rather than
    # hardcoding `docker-` (which only worked when run from a directory
    # literally named `docker`).
    for svc in postgres seaweedfs query write lifecycle lifecycle-scheduler penca-sql-server; do
        log_file="$log_dir/${svc}.log"
        if docker logs "${COMPOSE_PROJECT_NAME}-${svc}-1" > "$log_file" 2>&1; then
            bytes=$(wc -c < "$log_file" | tr -d ' ')
            printf '│  %-22s %s (%s bytes)\n' "$svc" "$log_file" "$bytes"
        fi
    done
    printf '└─────────────────────────────────────────────────────────────────────\n\n'
    docker compose -f docker/compose.yml --env-file docker/{{profile}}.env --profile infra --profile penca-backend down -v

# Tail logs from services started by `penca-up` (follows by default).
# With no service arg, follows every service; pass one or more service names
# (e.g. `just penca-logs query write`) to filter. Service names:
# postgres, seaweedfs, query, write, lifecycle,
# penca-sql-server.
[arg("profile", long)]
penca-logs profile="dev" *services:
    docker compose -f docker/compose.yml --env-file docker/{{profile}}.env --profile infra --profile penca-backend logs -f {{services}}

# Sync labels and projects to Linear. Requires LINEAR_API_KEY.
# Usage: just sync-linear, just sync-linear --labels, just sync-linear --projects, just sync-linear --retag
sync-linear *args:
    python scripts/sync_linear.py {{args}}

# Print open Linear issues grouped by priority. Requires LINEAR_API_KEY.
# Usage: just roadmap, just roadmap --project "Query Engine", just roadmap --label lifecycle
#        just roadmap --query "purge data cleanup"
[arg("project", long)]
[arg("priority", long)]
[arg("label", long)]
[arg("query", long)]
[arg("include-closed", long)]
roadmap project="" priority="" label="" query="" include-closed="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [[ -n "{{ project }}" ]]; then args+=(--project "{{ project }}"); fi
    if [[ -n "{{ priority }}" ]]; then args+=(--priority "{{ priority }}"); fi
    if [[ -n "{{ label }}" ]]; then args+=(--label "{{ label }}"); fi
    if [[ -n "{{ query }}" ]]; then args+=(--query "{{ query }}"); fi
    if [[ -n "{{ include-closed }}" ]]; then args+=(--include-closed); fi
    python scripts/roadmap.py "${args[@]}"

# Render every Linear issue carrying one label into a single-file HTML epic
# tracker (Mermaid blocks-DAG + status cards, always current). Requires LINEAR_API_KEY.
# Tag an epic's issues with one label, then: just epic-tracker "epic:cold-oltp"
# Reusable for any epic — pass its label. Writes epics-tracker.html by default.
[arg("out", long)]
[arg("group-by", long)]
[arg("group-by-label", long)]
[arg("title", long)]
epic-tracker label out="epics-tracker.html" group-by="" group-by-label="" title="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=("{{ label }}" -o "{{ out }}")
    if [[ -n "{{ group-by }}" ]]; then args+=(--group-by "{{ group-by }}"); fi
    if [[ -n "{{ group-by-label }}" ]]; then args+=(--group-by-label "{{ group-by-label }}"); fi
    if [[ -n "{{ title }}" ]]; then args+=(--title "{{ title }}"); fi
    python scripts/epic_tracker.py "${args[@]}"

# Download Apache Arrow's flight-sql-jdbc-driver JAR from Maven Central
# into `tests/integration/jdbc/lib/`, verified against a pinned SHA-256.
# Required for `TestFlightSqlJdbcProbe` (which shells out to
# `java -cp ... JdbcProbe.java`). The test skips cleanly if the JAR
# isn't there, so this recipe is optional for local dev work that
# doesn't touch FlightSQL — but CI runs it before `integration-test`
# so the JDBC smoke is always part of regression coverage.
#
# Pinned to 19.0.0 (released 2025-10). Bumping: download the new JAR
# manually, recompute `sha256sum`, update both constants below.
fetch-jdbc-driver:
    #!/usr/bin/env bash
    set -euo pipefail
    version="19.0.0"
    sha256="d3beee43c613c457789825343368f652d570d76c08799dad38a43a10e569b57f"
    dest="tests/integration/jdbc/lib"
    jar="$dest/flight-sql-jdbc-driver.jar"
    if [[ -f "$jar" ]] && echo "$sha256  $jar" | sha256sum -c --status; then
        echo "JDBC driver already at $jar (sha256 verified)."
        exit 0
    fi
    mkdir -p "$dest"
    url="https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/$version/flight-sql-jdbc-driver-$version.jar"
    echo "Downloading $url"
    curl -sfL --retry 3 -o "$jar" "$url"
    echo "$sha256  $jar" | sha256sum -c -

# Run integration tests: brings up infra + servicers, runs tests, tears down.
# Requires Docker daemon. Uses the test profile (random ports) by default,
# safe for parallel worktrees. Pass service names to run specific tests:
#   just integration-test lifecycle query
# Set the parallel phase's worker count. Unset means `auto`, capped by
# PENCA_TEST_JOBS_MAX (default 4); an explicit value is used as-is, uncapped,
# so a box with fewer cores than CI can still reproduce its worker count:
#   PENCA_TEST_JOBS=4 just integration-test
# Note that oversubscribing cores this way slows the servicers, which is what
# the pinned 2s QUERY_TIMEOUT_SECONDS measures against — so timeouts seen this
# way may be the oversubscription rather than a real defect.
# Note a named subset now runs its non-serial tests under xdist too, so it is
# representative of CI — but that means output is captured and breakpoint()/pdb
# won't attach. For an interactive debug loop, call pytest directly.
integration-test *services:
    #!/usr/bin/env bash
    set -euo pipefail

    # --profile=test, explicitly: random ports keep parallel worktrees from
    # colliding, and the lifecycle scheduler is idle there so its tick loop
    # cannot race the manual Persist/Snapshot/Purge calls these suites make.
    # penca-up defaults to the dev profile, which is the opposite of both.
    just penca-up --profile=test
    # `penca-down` dumps per-service logs to /tmp before teardown; trap
    # guarantees teardown whether pytest passes, fails, or the shell is
    # interrupted, while preserving pytest's exit code for CI.
    #
    # ONE trap for the whole recipe. bash does not stack EXIT handlers — a
    # second `trap ... EXIT` silently replaces this one, which would leave the
    # stack up, skip the volume wipe that makes the next run a fresh one, and
    # drop the /tmp/penca-logs-* that CI uploads on failure. So the collection
    # scratch file is created here and cleaned up here rather than by a trap of
    # its own.
    collect_out=$(mktemp)
    collect_err=$(mktemp)
    trap 'just penca-down --profile=test; rm -f "$collect_out" "$collect_err"' EXIT

    # `.client.env` — the 6 PENCA_*_URL values for PencaClient.
    # `.baseline.env` — PENCA_DB_* for white-box tests that open a direct
    # Postgres connection (via integration_helpers.get_pg_driver) to verify
    # internal storage state the gRPC API intentionally doesn't expose.
    # `test.env` — non-port lifecycle overrides (e.g.
    # ``LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES``, CHA-215) so tests reading
    # ``os.environ`` see the same caps the containers run under.
    set -a && source docker/test.env && source docker/.client.env && source docker/.baseline.env && set +a

    # Both the full suite and a named subset run the same two phases; only the
    # file list differs. A subset run is the dev loop, so it should be fast AND
    # representative — running it un-split would let a test that is not
    # xdist-safe pass locally and fail only in the merge queue.
    if [ -z "{{services}}" ]; then
        files=(tests/integration/integration_*.py)
    else
        files=()
        for svc in {{services}}; do
            files+=("tests/integration/integration_${svc}_test.py")
        done
    fi

    # Two disjoint phases against the one stack. The `serial` tests must run
    # alone for one of two reasons: they read process-global state (container
    # stdout log windows, pg_stat_statements counters) that a concurrent worker
    # pollutes, or they deliberately park a servicer PG connection while
    # asserting a bounded time. Either way alone means alone — `--dist
    # loadgroup` only serializes the group internally and would still run it
    # concurrently with the parallel phase.
    #
    # CHA-519 retires the first reason, not the second, so it shrinks this
    # phase rather than deleting it.
    #
    # Count each selection up front, so the phases don't have to infer intent
    # from an exit code later and a bad selector fails in seconds rather than
    # after a 30-minute run.
    #
    # The sum check is narrower than it looks: `-m X` and `-m "not X"` are
    # exact complements for ANY well-formed X, so it is a tautology except in
    # the one case where the two literals here stop being negations of each
    # other — i.e. a one-sided typo like `-m "seriall"`, which drops the serial
    # tests from both phases. That is the realistic mistake, so it earns its
    # place, but it does NOT check that the marks themselves are right; the
    # static check does that.
    #
    # Counts collected node ids rather than parsing pytest's summary line,
    # whose wording differs between the deselected and non-deselected cases.
    # Exit 5 (nothing collected) is a legitimate answer of zero; anything else
    # non-zero is a real collection failure — a bad service name gives exit 4 —
    # and must not be silently counted as zero.
    count_tests() {
        # stdout and stderr to separate files: a warning or traceback line
        # containing "::" would otherwise inflate the node-id count.
        uv run pytest "$@" --collect-only -q >"$collect_out" 2>"$collect_err"
        collect_rc=$?
        if [ "$collect_rc" -ne 0 ] && [ "$collect_rc" -ne 5 ]; then
            cat "$collect_out" "$collect_err" >&2
            echo "collection failed (pytest exit $collect_rc)" >&2
            return 1
        fi

        # Anchored: a collected node id is the whole line and starts with the
        # file path. pytest's reporter writes its warnings-summary and
        # deselection text to stdout as well, so an unanchored "::" would
        # count that noise as tests.
        grep -cE '^[^ ]+::' "$collect_out" || true
    }
    # One definition of each selector, shared by the counts below and by the
    # phase invocations. Previously the literals appeared in five places and
    # the guard compared only its own two, so a typo in a PHASE (-m "not
    # seriall") passed the guard untouched and ran every serial test under
    # -n auto, exiting 0 — the same one-sided-typo class the guard exists for,
    # failing in the direction that hides.
    SERIAL_SELECTOR='serial'
    PARALLEL_SELECTOR='not serial'

    total_n=$(count_tests "${files[@]}") || exit 1
    serial_n=$(count_tests "${files[@]}" -m "$SERIAL_SELECTOR") || exit 1
    parallel_n=$(count_tests "${files[@]}" -m "$PARALLEL_SELECTOR") || exit 1

    # Selecting nothing at all is never right: without this both phases would
    # exit 5, both tolerances would fire, and the recipe would report success
    # having run no tests. Reached when the selected files collect cleanly but
    # hold no tests — a mistyped service name does NOT land here, since that is
    # a pytest usage error caught by count_tests above.
    if [ "$total_n" -eq 0 ]; then
        echo "selection matched no tests" >&2
        exit 1
    fi

    if [ "$((serial_n + parallel_n))" -ne "$total_n" ]; then
        echo "phase selectors do not partition the suite:" >&2
        echo "  serial=$serial_n + parallel=$parallel_n != total=$total_n" >&2
        echo "  a test in neither phase would silently never run" >&2
        exit 1
    fi

    # Serial first, and the order is load-bearing. `container_log` buffers and
    # ANSI-strips the container's whole stdout on EVERY call, and `poll_log_for`
    # repeats that every 100ms for up to 5s per assertion. The services log at
    # debug with no size cap, so scraping after the parallel phase means paying
    # a worst-case buffer each time: measured 19m for 56 tests that way, against
    # ~11m for the same files' work back when the suite was fully serial. The
    # gap is buffer cost, not what these tests inherently take.
    serial_rc=0
    uv run pytest "${files[@]}" -m "$SERIAL_SELECTOR" -s || serial_rc=$?

    # No `-s`: N workers interleave into noise, and nothing here needs it — the
    # scrapers read `docker logs`, not pytest's capture, and all ran in phase 1.
    # Runs even if phase 1 failed, so one invocation reports both.
    #
    # `--dist loadgroup` honours `xdist_group`, which pins mutually-conflicting
    # tests to ONE worker while leaving them parallel with everything else.
    # Ungrouped tests distribute exactly as under the default `load`. Note this
    # is not a substitute for the serial phase: loadgroup still runs the group
    # concurrently with other tests, which is fine for tests that conflict only
    # with each other and useless for ones that need the stack quiet.
    #
    # The cap is 4 because that is what CI has, not because of a measured
    # ceiling. Worker count WAS bounded by the servicers' PG pools — at 4
    # workers against a pool of 4, 28 tests failed, every one with "pool timed
    # out while waiting for an open connection" and nothing else — which is why
    # docker/compose.yml now defaults that pool to 12. Keep workers at or under
    # the pool depth if either number moves.
    #
    # What binds now is CPU against the deliberately small
    # QUERY_TIMEOUT_SECONDS=2: at 4 workers on a 2-core box, two heavy
    # compaction tests timed out. Both are now marked `serial` (reason (b)), so
    # they no longer run in this phase — but the shape is worth remembering,
    # since a server-side deadline that small is sensitive to how much CPU the
    # servicers get, not just to how many workers there are.
    #
    # PENCA_TEST_JOBS sets `-n` directly so it can raise as well as lower —
    # without that, a 2-core box could never reach CI's 4 workers and the queue
    # was the only place contention showed up. The cap applies ONLY to `auto`:
    # xdist clamps `min(numprocesses, maxprocesses)` for an explicit `-n` too,
    # so passing both would silently cap a deliberate request.
    parallel_rc=0
    if [ -n "${PENCA_TEST_JOBS:-}" ]; then
        uv run pytest "${files[@]}" -m "$PARALLEL_SELECTOR" --dist loadgroup \
            -n "$PENCA_TEST_JOBS" || parallel_rc=$?
    else
        uv run pytest "${files[@]}" -m "$PARALLEL_SELECTOR" --dist loadgroup \
            -n auto --maxprocesses "${PENCA_TEST_JOBS_MAX:-4}" || parallel_rc=$?
    fi

    # pytest exits 5 (NO_TESTS_COLLECTED) when a phase selects nothing. The
    # counts taken up front say whether that was expected, so neither phase has
    # to infer it from an exit code — an empty phase is fine exactly when its
    # count was zero. That covers a fully-serial named subset, and the
    # post-CHA-519 world where no serial tests remain, without special-casing
    # either.
    if [ "$serial_rc" -eq 5 ] && [ "$serial_n" -eq 0 ]; then
        serial_rc=0
    fi

    if [ "$parallel_rc" -eq 5 ] && [ "$parallel_n" -eq 0 ]; then
        parallel_rc=0
    fi

    # Surface the first non-zero. Written as a full `if` rather than
    # `[ ... ] && rc=...`, whose non-zero test would trip `set -e`.
    rc="$serial_rc"
    if [ "$rc" -eq 0 ]; then
        rc="$parallel_rc"
    fi

    exit "$rc"

# Run performance tests: brings up infra + servicers, runs tests, tears
# down. Requires Docker daemon. Uses random ports by default (safe for
# parallel worktrees). Sources docker/.baseline.env so perf tests can open
# a direct Postgres connection for the baseline comparison.
#
# Servers run at representative verbosity (RUST_LOG=info,penca=info) by default
# so trace-span overhead doesn't perturb the recorded latency. Pass --trace to
# run at penca=trace + span busy/idle timing for a diagnostic run (per-service
# span logs land in /tmp via penca-down); those numbers reflect
# Penca-under-tracing, not production latency.
#
# Each measurement is emitted as JSONL to .perf/results.jsonl (the always-on
# capture). At the end of every run a static HTML report is written to
# .perf/report-<run_id>.html, comparing the run against the recorded history.
# Pass --record to also persist the run into the gitignored SQLite history at
# .perf/perf.db (graph it with `just perf-trends` / `just perf-dashboard`);
# without --record the run is throwaway (JSONL + report only, no history kept).
#
# Pass --profile (CHA-420) to also capture a samply CPU profile of each
# servicer under load → .perf/profile-<svc>.json (open with `samply load`).
# Requires passwordless sudo (servicers run as root in-container, so samply
# attaches as root) and builds the DWARF + frame-pointer `profiling` image, so
# it's opt-in — a plain perf run is never slowed.
#
# Args are paths — a directory (runs everything under it), explicit files, or a
# name resolved relative to tests/performance/. No paths runs everything under
# tests/performance/. Mix flags freely, e.g.
#   just perf-test --record grpc                       # all of tests/performance/grpc/
#   just perf-test --profile performance_query_test.py
#   just perf-test grpc/oltp_test.py performance_write_test.py
perf-test *paths:
    #!/usr/bin/env bash
    set -euo pipefail

    # Split the variadic args into paths and the optional flags:
    # --trace runs servers at trace-level spans for diagnosis (off by default
    # to keep measurements representative). --record persists this run into the
    # SQLite history (otherwise the run is throwaway: JSONL + report only).
    paths=""
    trace_spans=0
    profile_run=0
    record_run=0
    for arg in {{paths}}; do
        if [ "$arg" = "--trace" ]; then
            trace_spans=1
        elif [ "$arg" = "--profile" ]; then
            profile_run=1
        elif [ "$arg" = "--record" ]; then
            record_run=1
        else
            paths="$paths $arg"
        fi
    done

    # Measure at representative verbosity (penca=info) by default, so
    # trace-level span overhead doesn't inflate the recorded elapsed_seconds.
    # Pass --trace to run servers at penca=trace + span busy/idle timing for a
    # diagnostic run (the recorded numbers then reflect Penca-under-tracing,
    # not production latency). Shell exports win over docker/test.env for
    # compose interpolation, overriding the info,penca=debug default.
    if [ "$trace_spans" = "1" ]; then
        export RUST_LOG=info,penca=trace
        export PENCA_SPAN_TIMING=1
    else
        export RUST_LOG=info,penca=info
    fi

    # --profile (CHA-420): wrap the run in samply CPU profiling. The servicers
    # run as root in-container, so samply attaches as root (CAP_PERFMON) —
    # unprivileged perf_event_open across users is denied at every
    # perf_event_paranoid level, and root bypasses paranoid (no sysctl tuning).
    # Build the servicer image with the DWARF + frame-pointer `profiling` Cargo
    # profile (CARGO_PROFILE flows through compose to the Docker build arg) so
    # samply symbolicates to source.
    if [ "$profile_run" = "1" ]; then
        samply_bin=$(command -v samply) || {
            echo "error: samply not found — run 'just install-tools'." >&2
            exit 1
        }
        if ! sudo -n true 2>/dev/null; then
            echo "error: --profile needs passwordless sudo to run samply as root" >&2
            echo "       (the containerized servicers run as root; cross-user perf attach is denied)." >&2
            exit 1
        fi
        export CARGO_PROFILE=profiling
    fi

    just penca-up --profile=test
    trap 'just penca-down --profile=test' EXIT

    set -a && source docker/.client.env && source docker/.baseline.env && set +a

    # Emit one JSON object per measurement; a fresh file per run (the SQLite DB
    # is the accumulator, the JSONL is transient).
    export PERF_RESULTS_JSON=.perf/results.jsonl
    mkdir -p .perf
    : > "$PERF_RESULTS_JSON"

    samply_pids=()
    if [ "$profile_run" = "1" ]; then
        # Attach samply (as root) to each servicer's host PID for the run. Idle
        # servicers just yield small profiles; the point is to capture whichever
        # servers the workload exercises. docker inspect piped to jq avoids
        # go-template braces colliding with just's interpolation.
        profiled_svcs=()
        for svc in query write lifecycle penca-sql-server; do
            container="${COMPOSE_PROJECT_NAME}-${svc}-1"
            pid=$(docker inspect "$container" | jq -r '.[0].State.Pid')
            sudo "$samply_bin" record -p "$pid" --save-only -o ".perf/profile-${svc}.json" \
                2>".perf/.samply-${svc}.log" &
            samply_pids+=("$!")
            profiled_svcs+=("$svc")
        done

        # Wait until every samply has attached before the load starts — a fixed
        # sleep races a cold sudo / 5 concurrent attaches and would profile the
        # first seconds with attaches not yet live. samply prints "Recording
        # process" to stderr once attached; poll for it, bounded by one shared
        # ~30s deadline (the attaches run concurrently), warn-on-miss not hang.
        attach_deadline=$((SECONDS + 30))
        for svc in "${profiled_svcs[@]}"; do
            echo "waiting for samply attach: ${svc} ..."
            while ! grep -q "Recording process" ".perf/.samply-${svc}.log" 2>/dev/null; do
                if [ "$SECONDS" -ge "$attach_deadline" ]; then
                    echo "warning: samply attach for ${svc} not confirmed within timeout; profile may under-cover" >&2
                    break
                fi
                sleep 0.5
            done
        done
    fi

    pytest_rc=0
    if [ -z "$paths" ]; then
        uv run pytest tests/performance/ -s || pytest_rc=$?
    else
        # Each arg is a path: a directory (runs everything under it), an explicit
        # file, or a name resolved relative to tests/performance/. Multiple paths
        # are passed straight through to pytest.
        resolved=""
        for p in $paths; do
            if [ -e "$p" ]; then
                resolved="$resolved $p"
            elif [ -e "tests/performance/$p" ]; then
                resolved="$resolved tests/performance/$p"
            else
                echo "perf-test: path not found: $p (tried ./$p and tests/performance/$p)" >&2
                exit 1
            fi
        done
        uv run pytest $resolved -s || pytest_rc=$?
    fi

    if [ "$profile_run" = "1" ]; then
        # Stop samply (SIGINT flushes the --save-only profiles; sudo forwards it
        # to the samply child) and wait for the writes, then hand the root-owned
        # profiles back to the caller.
        for samply_pid in "${samply_pids[@]}"; do
            kill -INT "$samply_pid" 2>/dev/null || true
        done
        wait "${samply_pids[@]}" 2>/dev/null || true
        sudo chown "$(id -u):$(id -g)" .perf/profile-*.json 2>/dev/null || true
        rm -f .perf/.samply-*.log
        echo "samply profiles written under .perf/profile-<svc>.json"
    fi

    # Persist this run into the SQLite history only when --record was passed
    # (otherwise the run is throwaway: JSONL + report only). Idempotent +
    # additive even if a test failed; the guard keeps an ingest hiccup from
    # masking pytest's exit code, which stays authoritative. (stdlib-only, so
    # bare python.)
    if [ "$record_run" = "1" ]; then
        python scripts/perf/results_to_sqlite.py --json "$PERF_RESULTS_JSON" --db .perf/perf.db \
            || echo "[perf] ingest failed (rc=$?)" >&2
    fi

    # Always render the per-run static HTML report (no flag), comparing this run
    # against the recorded history — no-baseline when the DB is empty or
    # --record wasn't used. The run_id comes from the JSONL the recorder
    # stamped; `uv run` because the report imports matplotlib.
    # Degrade to "latest" rather than aborting (set -e) on an empty/malformed
    # first line, so a run_id-extraction hiccup can't mask pytest's exit code —
    # same guard philosophy as the ingest/report steps.
    if [ -s "$PERF_RESULTS_JSON" ]; then
        report_run_id=$(head -1 "$PERF_RESULTS_JSON" \
            | python -c 'import json,sys; print(json.load(sys.stdin)["run_id"])' \
            2>/dev/null) || report_run_id=latest
    else
        report_run_id=latest
    fi
    uv run python scripts/perf/render_report.py --json "$PERF_RESULTS_JSON" \
        --db .perf/perf.db --out ".perf/report-${report_run_id}.html" \
        || echo "[perf] report failed (rc=$?)" >&2
    echo "[perf] report: .perf/report-${report_run_id}.html"
    exit "$pytest_rc"

# Graph perf trends + summary stats from the SQLite history (.perf/perf.db):
# prints a per-series markdown summary (run counts, latest vs previous,
# regression flags) and writes trend PNGs to .perf/graphs/. Populate the DB
# first with `just perf-test`. Uses `uv run` because perf_trends imports
# matplotlib (a uv dev dependency the system python doesn't have).
perf-trends:
    uv run python scripts/perf/trends.py --db .perf/perf.db --out-dir .perf/graphs

# Launch the interactive Streamlit perf dashboard over the SQLite history.
# Populate the DB first with `just perf-test`. streamlit/pandas are dev deps.
# Pass a run_id to open the comparison view for that run, e.g.
#   just perf-dashboard <run_id>
perf-dashboard run_id="":
    #!/usr/bin/env bash
    set -euo pipefail

    args=()
    [ -n "{{run_id}}" ] && args=(-- --run_id "{{run_id}}")
    uv run streamlit run scripts/perf/dashboard.py "${args[@]}"

# Run the CHA-415 building-blocks floor benches (criterion) and point at the
# criterion HTML report. Only the hot-MVCC bench needs Postgres (cold/merge benches are
# in-memory), so this brings up *only* the postgres infra container — no
# servicer rebuild. Pass criterion args through, e.g.
#   just perf-floor --bench hot_mvcc_floor
# Scales are env-gated: PERF_FLOOR_MAX=1m (default 100k via this recipe),
# PERF_FLOOR_DENSITY=1,10,100, PERF_FLOOR_PARQUET=1. 1M may OOM small hosts.
perf-floor *args:
    #!/usr/bin/env bash
    set -euo pipefail

    compose="docker compose -f docker/compose.yml --env-file docker/test.env --profile infra"
    $compose up -d postgres
    pg_port=$($compose port postgres 5432 | cut -d: -f2)

    export PENCA_DB_HOST=localhost PENCA_DB_PORT="$pg_port"
    export PENCA_DB_DBNAME=penca PENCA_DB_USER=penca PENCA_DB_PASSWORD=penca
    export PERF_FLOOR_MAX="${PERF_FLOOR_MAX:-100k}"

    echo "Postgres on :$pg_port  (PERF_FLOOR_MAX=$PERF_FLOOR_MAX)"
    cargo bench -p penca-merge -p penca-format {{args}}

    echo
    # Per CHA-423, floor numbers are not committed (host-dependent). Criterion
    # writes its own self-contained HTML report — plots + run-over-run deltas.
    echo "Criterion report: target/criterion/report/index.html"

# Run TDD development tests against live infra. Requires Docker daemon.
# These tests live in tests/tdd/ (gitignored, not committed).
# Pass pytest args: just tdd -k test_snapshot_basic
tdd *args:
    #!/usr/bin/env bash
    set -euo pipefail

    just penca-up --profile=test
    trap 'just penca-down --profile=test' EXIT

    set -a && source docker/.client.env && set +a

    uv run pytest tests/tdd/ -s {{args}}
