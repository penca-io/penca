#!/usr/bin/env bash
# scripts/kata-issue-graph.sh — scoped, read-only kata client for the shared
# issue-graph instance (CHA-447).
#
# Sends ONLY read commands to the shared kata daemon named by
# PENCA_KATA_GRAPH_URL, leaving the local task-queue daemon (default kata
# resolution) authoritative for everything else. The scoping is the whole
# point: kata's KATA_SERVER is process-global and highest precedence, so a
# global export of KATA_SERVER — or a repo-root .kata.local.toml — would
# silently route the cha-NNN task-queue drain to the shared daemon too. This
# wrapper sets the remote env INLINE on the exec line only, never exporting it
# into the caller's shell.
#
# Opt-in: with PENCA_KATA_GRAPH_URL unset the wrapper is a clean no-op (exit 3)
# and the VM keeps today's local-only behavior.
#
# Usage:  scripts/kata-issue-graph.sh <show|list|ready|search|projects|labels> [args...]
# Exit:   2 = refused (non-read subcommand); 3 = not configured (local-only).
set -euo pipefail

if [[ -z "${PENCA_KATA_GRAPH_URL:-}" ]]; then
    echo "kata-issue-graph: shared issue-graph client not configured (local-only); set PENCA_KATA_GRAPH_URL" >&2
    exit 3
fi

# The first non-flag argument is the kata subcommand; the shared instance is the
# issue corpus, so this navigation client must never mutate it — allow reads only.
# Pass the subcommand first: a value-taking global flag before it (e.g.
# `--project X show`) would be mis-read as the subcommand and refused. kata also
# accepts those flags after the subcommand, so put them there. A misdetection
# only ever over-refuses (exit 2) — it can never let a write through.
subcommand=""
for arg in "$@"; do
    if [[ "$arg" != -* ]]; then
        subcommand="$arg"
        break
    fi
done

case "$subcommand" in
    show | list | ready | search | projects | labels) ;;
    *)
        echo "kata-issue-graph: read-only (got '${subcommand:-<none>}'); allowed: show list ready search projects labels" >&2
        exit 2
        ;;
esac

# Assemble the scoped remote env. KATA_AUTH_TOKEN and KATA_ALLOW_INSECURE are
# injected only when set: an unconfigured token stays "no auth header" rather
# than an empty-credential handshake, and KATA_ALLOW_INSECURE is opt-in for a
# dev-over-http instance. KATA_SERVER is always present, so the array is never
# empty (no `set -u` empty-expansion hazard on older bash).
remote_env=(KATA_SERVER="$PENCA_KATA_GRAPH_URL")
if [[ -n "${PENCA_KATA_GRAPH_TOKEN:-}" ]]; then
    remote_env+=(KATA_AUTH_TOKEN="$PENCA_KATA_GRAPH_TOKEN")
fi
if [[ -n "${PENCA_KATA_GRAPH_ALLOW_INSECURE:-}" ]]; then
    remote_env+=(KATA_ALLOW_INSECURE=1)
fi

# Inline on exec ONLY — never exported into the caller's shell (that would
# hijack local task-queue kata calls via the process-global KATA_SERVER).
exec env "${remote_env[@]}" kata "$@"
