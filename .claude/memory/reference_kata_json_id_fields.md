---
name: kata-json-id-fields
description: kata create/ready --json expose .issue.short_id (no qualified_id); build refs as fabric#<short_id>; --blocked-by rejects bare numeric ids
type: reference
---

`kata`'s `--json` output uses different id fields per subcommand:

- `kata create --json` → `{changed, event, issue:{id, short_id, uid, ...}, kata_api_version}`. The newly-created issue's ref is **`.issue.short_id`** (plus numeric `.issue.id` and ULID `.issue.uid`). There is **no `qualified_id`** field.
- `kata ready --json` → `.issues[]` objects also expose `.short_id` (and `.id`, `.uid`), **not** `.qualified_id`.
- `kata list --json` → `.issues[].qualified_id` **is** present (e.g. `fabric#mtfc`), alongside `.labels` and `.blocked_by` (the latter an array of `{uid, short_id}` objects).

To capture a freshly-created task's ref, read `.issue.short_id` and build `fabric#<short_id>` — reading `.qualified_id` off `create`/`ready` output yields empty and silently breaks any downstream `--blocked-by`/`--related` wiring.

Refs passed to `--blocked-by` (and friends) must be the qualified `fabric#<short_id>` form (or the ULID). A bare numeric id is rejected: `"…looks like a legacy issue number; use a short_id (e.g. abc4) or kata#abc4"`.

This bit hard during CHA-349's roborev→kata bridge work: the hook's `orch:*` blocked-by extension silently no-op'd because it parsed `.qualified_id` from `kata create` output (empty), so findings never gated PR open. See also [[feedback_kata_list_label_intersect_broken]] (the sibling `kata list --label A --label B` AND-intersect bug — that lookup also mis-resolved in the same script). The `kata close --done` message/evidence requirements now live in the `/do-issue` skill cheat sheet.
