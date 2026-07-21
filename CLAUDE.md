# Penca

HTAP data lakehouse for agentic development.

## Workflow

- Implement a Linear ticket → `/do-issue` (per-ticket task queue lives in `kata`; see `kata tui` to inspect / approve / drain)
- Review a PR → `/review-pr` (findings mirror to the PR's `cha-NNN` kata queue alongside inline GitHub comments)
- Save or update a memory entry → `/save-memory`
- Linear projects/labels live in `linear/*.toml` — edit there, run `just sync-linear`. MCP is read-only / ad-hoc.
- Agent tooling (kata + roborev post-commit hook + kata bridge) is bootstrapped via `just init-agent-tools`.

Reference docs under `docs/` and `README.md` are read on-demand within the relevant skill, not preemptively.
