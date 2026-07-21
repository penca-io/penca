---
name: feedback-linear-cross-refs-markdown
description: "In Linear MCP save_issue, use markdown links for cross-ticket references, not bare CHA-NNN identifiers — the server's auto-resolver mis-binds UUIDs"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7c4498ca-3d15-4eef-8e7d-66c771fcc8d4
---

When writing a Linear issue body via `mcp__linear-server__save_issue`, cross-reference other tickets using markdown link syntax — `[CHA-217](https://linear.app/chapala/issue/CHA-217)` — rather than bare identifiers like `CHA-217`.

**Why:** The Linear MCP's server-side renderer auto-resolves bare identifiers (and converts them to `<issue id="<uuid>">CHA-NNN</issue>` tags), and this resolution can be wrong — observed case: bare "CHA-217" in a description got rewritten to a tag pointing at CHA-134's UUID, so the rendered link displayed "CHA-134" instead of CHA-217. The mis-binding is invisible in the input but breaks the cross-references after the round trip.

**How to apply:** Always use explicit markdown link syntax for cross-ticket references when filing or editing Linear issues through the MCP. After save, eyeball the returned `description` field to confirm links resolved to the intended targets. Linking to ADRs by their doc path (e.g. "ADR 0013") is fine — only the `CHA-NNN` identifiers trigger auto-resolution. Related: [[feedback-linear-workflow]].
