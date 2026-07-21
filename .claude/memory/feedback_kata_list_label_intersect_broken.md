---
name: feedback-kata-list-label-intersect-broken
description: "kata list / kata ready ignore the second `--label` flag — repeated `--label` does NOT AND-intersect despite help text claiming it does"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 1171c8ce-3bbc-4ea9-9633-3ae7753cff09
---

`kata list --label X --label Y --json` returns the set matching `X` only — the second `--label` flag is silently dropped, even though `kata list --help` says "(repeatable, AND logic)". Confirmed 2026-05-28 on a fresh project: `--label cha-82` returned 8 issues, `--label plan-draft` returned 0, but `--label cha-82 --label plan-draft` returned 8 (the cha-82 set), not 0 (the intersection).

`kata ready --unowned --label cha-NNN --label approved --json` is the drain consumer's primary query in `/do-issue`. If `--label` is broken the same way, the drain will surface tasks that match `cha-NNN` but aren't approved — including `plan-draft` tasks. That breaks the Step-3 gate at the drain layer.

**Why:** kata bug in flag parsing; surfaced while approving cha-82's task graph.

**How to apply:**
- Don't trust `kata list --label A --label B --json | jq '.issues | length'` for intersection counts. Instead: `kata list --label A --json | jq '.issues[] | select(.labels | index("B"))'`.
- Note: `kata list` returns labels as `["str", ...]`, but `kata show` returns labels as `[{label: "str", ...}]`. The jq path differs.
- Before the `/do-issue` drain runs against an approved set, verify no `plan-draft` tasks slip through: `kata list --label cha-NNN --json | jq -r '.issues[] | select(.labels | index("plan-draft")) | .qualified_id'` should be empty.
- File against the kata repo if it's not already known. Until fixed, the `/do-issue` drain may need a client-side filter to avoid acting on unapproved tasks.
