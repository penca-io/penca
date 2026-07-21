---
name: reference-kata-ready-json-no-labels
description: kata ready --json issues carry NO labels field — jq label post-filters silently drop everything; intersect with kata list instead
metadata: 
  node_type: memory
  type: reference
  originSessionId: 289e6206-dca0-4e6a-944c-d854822798f6
---

`kata ready --unowned --label X --json` returns issue objects WITHOUT a `labels` field (unlike `kata list --json`, whose issues carry `labels` as a plain string array, and `kata show --json`, which has top-level `labels` as objects with a `.label` key). A defensive post-filter like `jq '.issues[] | select(.labels | index("approved"))'` on `kata ready` output evaluates `null | index(...)` → null and silently drops every row — the drain looks empty when work is ready.

**How to apply:** for the /do-issue drain's approved-gate check, intersect two queries: `kata ready --unowned --label cha-NNN --json` (blocker/ownership filter) ∩ `kata list --label cha-NNN --json | jq '.issues[] | select(.labels | index("approved"))'` (label filter). The human-readable `kata ready --label cha-NNN` (no `--json`) reliably *lists* what's ready when you just need to eyeball it — confirmed 2026-06-15 that it showed ready tasks while a careless `--json | jq '.issues[]'` choked (the `--json` shape is the unreliable part, not the readiness computation). Related: [[feedback_kata_list_label_intersect_broken]] (the dual `--label` AND-intersect bug that motivates post-filtering in the first place), [[reference_kata_json_id_fields]].
