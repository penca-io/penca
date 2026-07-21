---
name: linear-mcp-parallel-save-can-drop-writes
description: "Linear MCP save_issue can silently drop a write under parallel calls; verify load-bearing edits with an independent get_issue, don't trust the save echo"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: b4b2df00-f1dc-4839-9489-4dab24193d68
---

Observed 2026-06-15: two `mcp__linear-server__save_issue` calls sent in the **same parallel tool-batch** — one persisted, the other was **silently lost**. Both returned a 200 with the updated body, but a later independent `get_issue` showed the dropped one still carrying its old content (a third ticket from the same batch persisted fine, so it's a per-write race, not a whole-batch failure).

**Why:** the `save_issue` response **echoes the request payload, not the committed state**, so a successful-looking return is not proof of persistence.

**How to apply:** For multi-ticket Linear edits, prefer **sequential** `save_issue` calls (or verify after). Always **confirm load-bearing edits (status changes, dependency relations, description rewrites) with an independent `get_issue`** rather than trusting the returned object. Same caution likely applies to parallel `save_comment` / `save_document`.

Related: [[feedback_gh_pr_edit_broken]] (another Linear/GitHub write path that silently no-ops).
