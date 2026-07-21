---
name: reference_linear_attachment_visibility
description: Linear API file-upload attachments don't render in the issue UI; surface an uploaded asset via a markdown link in a comment, not the attachment entity.
metadata:
  node_type: memory
  type: reference
---

Linear file-upload **attachments created via the API** (`prepare_attachment_upload` → `create_attachment_from_upload`, with `uploads.linear.app` asset URLs) do **not** render in the Linear issue UI. The attachment record exists (visible in `get_issue` `.attachments`), but nothing shows on screen — only integration / external-URL attachments (e.g. GitHub PRs) render as cards. Creating one just leaves a dangling, invisible record.

**How to surface an uploaded asset on a ticket:** post a markdown link to the `assetUrl` in a **comment** (`save_comment`). Linear re-signs the asset URL on view, so a bare `uploads.linear.app/...` link in comment markdown resolves for the reader.

**Working upload flow (file → visible link):**
1. `prepare_attachment_upload` (issue, filename, contentType, exact size) → returns `assetUrl` + a signed `uploadRequest` (60s expiry).
2. `curl -X PUT --data-binary @file` to `uploadRequest.url`, sending the signed headers **verbatim** (content-type, x-goog-content-length-range, cache-control, Content-Disposition) — any omitted/re-cased signed header → HTTP 403.
3. Use the returned `assetUrl` as the markdown link in `save_comment`. **Skip `create_attachment_from_upload`** — it only creates the invisible attachment record.

Note: Linear always sets `Content-Disposition: attachment`, so the link downloads/opens the file rather than rendering inline. For true in-Linear rendering, embed an image (`![](assetUrl)`) instead of a link. Discovered 2026-06-01 attaching an HTML plan visualization to CHA-92; see [[reference_kata_json_id_fields]] for the related kata JSON-shape gotchas.
