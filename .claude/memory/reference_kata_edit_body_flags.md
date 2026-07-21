---
name: reference-kata-edit-body-flags
description: "kata edit has NO --body-file/--body-stdin (only kata create does) — use --body \"$(cat file)\""
metadata: 
  node_type: memory
  type: reference
  originSessionId: d09d51b6-5035-40a1-ab0d-9f9e5cc610eb
---

`kata create` accepts `--body-file` / `--body-stdin`, but `kata edit` accepts only `--body <string>` (verified 2026-06-10 via `kata edit --help`). To update a task body from a file: `kata edit <ref> --body "$(cat /tmp/body.md)"`. The wrong "kata edit <ref> --body-stdin" instruction that previously appeared in three skill texts (do-issue, clean-code-refactor, tracing-instrument) was fixed 2026-06-11 (PR #228). Related: [[feedback_kata_list_label_intersect_broken]], [[reference_kata_json_id_fields]].
