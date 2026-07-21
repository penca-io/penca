---
name: reference_kata_show_json_links
description: "kata show --json puts blocked-by edges in .links[], not .relationships/.blocked_by"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 023ecd53-07c0-4306-8b1d-b7a53bc9f7a2
---

`kata show <ref> --json` returns top-level keys `{kata_api_version, issue, labels, links, comments}` — NOT a `.relationships.blocked_by` field (that path is null). Issue scalars (title, body, short_id, status, priority) live under `.issue`; `.labels` is an array of objects (`.label` is the string). The blocked-by graph is in **`.links[]`**, each `{from:{short_id}, to:{short_id}, type:"blocks"}` — `from` blocks `to`, so a task's blockers are the `from`s of links where `to.short_id == <task>`.

To build a blocked_by map for a graph audit (e.g. `/plan-reviewer`), iterate every task's `.links` and collect edges:
`kata show <ref> --json | jq -r '.links[]? | select(.type=="blocks") | "\(.from.short_id) -> \(.to.short_id)"'` then dedup across all tasks.

Related: [[reference_kata_json_id_fields]] (create/ready give `.issue.short_id`), [[feedback_kata_list_label_intersect_broken]].
