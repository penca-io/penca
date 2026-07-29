---
name: reference_proto_comment_edits_regen_py_stubs
description: "Editing a .proto COMMENT requires `just compile-protos-py` — the generated *_pb2_grpc.py stubs are tracked in git, embed proto comments as docstrings, and no CI job checks their freshness"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 4f154a0c-2513-4b05-9185-52b79fed2129
  modified: 2026-07-29T02:16:39.117Z
---

Any edit to a `.proto` file — **including comment-only edits** — must be followed by
`just compile-protos-py`, and the regenerated stubs committed alongside it.

`packages/penca-proto/src/penca_proto/external/v1/*_pb2_grpc.py` are **tracked in
git**, and protoc emits each RPC's leading comment as that method's Python
docstring. So proto comment text is what Python consumers see via `help()` /
IDE hover. Editing the `.proto` alone leaves the stub asserting the old text.

**No CI job regenerates-and-diffs the stubs** (`ci.yml` has no proto-freshness
check), so the drift is silent and surfaces later as unrelated churn in whoever
next touches those protos.

The Rust side is unaffected — `crates/penca-proto/build.rs` compiles at build
time into `OUT_DIR`, so it always tracks the `.proto`.

Found the hard way in the CHA-wide code-comments audit (PR #11, 2026-07-29):
archaeology deleted from `lifecycle.proto` still read
`discovery (CHA-445; formerly on StorageMetadataService).` in the checked-in
stub. Caught by roborev after the branch had already gone green on CI.

Adding a regenerate-and-diff step to `ci.yml` would close this permanently —
not yet filed. Related: [[feedback_poll_roborev_after_any_commits]].
