---
name: project_cold_row_uuid_index_format_agnostic
description: "Cold row_uuid identity index (CHA-412) BUILD is format-agnostic (Parquet AND Lance), not Parquet-only; CHA-339 is predicate-pushdown, not the identity seek"
metadata: 
  node_type: memory
  type: project
  originSessionId: a8c5a134-4b93-4902-a0a3-9e9a6f630aa0
---

The cold-tier internal `row_uuid` identity index (CHA-412 build / CHA-454 seek)
is built for **every** snapshot regardless of storage format. The sidecar is a
sorted `(key, row_offset)` cold file (derived data, format-independent) and
follows the table's storage format — Parquet table → `.parquet` sidecar, Lance
table → `.lance` sidecar — written via the same `FormatWriter` as the base
segments. Do **not** gate the index build on `storage_format == Format::Parquet`.

**Why:** ADR 0026 §6 originally said "the hand-rolled mechanism is the Parquet
path; Lance uses native scalar indexes (CHA-339)." That conflated two different
things and the user rejected it hard ("why tf is it parquet only??"). The
identity index is a single uniform mechanism; gating the build on Parquet would
leave Lance tables with no `row_uuid` index. **CHA-339 (Lance native scalar
indexes / filter-aware decoders) is a predicate-pushdown optimization for
user-column FILTERS — a different concern from the internal row_uuid identity
SEEK, and not a replacement for the identity index.** The *apply* (CHA-454)
diverges per-format only at the final read (Parquet `RowSelection` vs Lance
take-by-offset); the build and the binary-search are uniform.

**How to apply:** ADR 0026 §6 was rewritten (commit on the CHA-412 branch) to
"uniform build, format-specific apply." The integration suite is Lance-only
(`docker/compose.yml` → `OBJECT_STORAGE_FORMAT: lance`), so a Parquet-only build
was also untestable there — format-agnostic build makes RT1/RT2 pass on the
Lance suite with no test-infra changes. See [[feedback_tickets_are_spirit_not_spec]].
