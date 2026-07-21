---
name: Linear workflow preference
description: Use repo TOML files + just commands for Linear projects/labels; MCP only for ad-hoc issue work
type: feedback
originSessionId: 5a47237c-23a3-43ad-bc79-27a1e9f14145
---
Prefer repo-integrated Linear tooling over the Linear MCP server for mutations to projects and labels.

**Why:** The TOML files (`linear/projects.toml`, `linear/labels.toml`) are the source of truth. Creating projects/labels via MCP bypasses the repo and causes drift. The user corrected this when we tried to create a project directly via MCP.

**How to apply:**
- To add/update projects: edit `linear/projects.toml`, run `just sync-linear --projects`, commit the TOML
- To add/update labels: edit `linear/labels.toml`, run `just sync-linear --labels`, commit the TOML
- To classify issues: `just sync-linear --retag` (uses Claude Haiku)
- Use Linear MCP for: creating/updating individual issues, reading issue state, ad-hoc queries
