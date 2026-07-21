---
name: feedback-validation-at-grpc-api-layer
description: "Validation on gRPC wire shapes (CreateTableRequest, MutateDataRequest, …) belongs in the gRPC servicer's validation module — `penca-server-grpc/src/validation/<service>.rs` — not as a per-wire-path filter at penca-sql-server, and not buried in a penca-api servicer. The test — would the same failure mode surface different wording depending on whether the caller used Flight SQL vs direct gRPC? If yes, push it to the one validation layer."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5b3fd3df-34f8-42f6-b182-b39ab2b73f71
---

When a validation rule applies to a typed gRPC request shape (`CreateTableRequest`, `CreateSchemaRequest`, `MutateDataRequest`, …), implement it in **`penca-server-grpc/src/validation/<service>.rs`** — the dedicated request-shape validation module the servicer runs *before* dispatching into penca-api (e.g. `validation::write::validate_create_table` checks UUIDs, name format, arrow_schema parseability, column types). NOT as a per-wire-path filter inside penca-sql-server, and NOT inside the penca-api servicer impl. The wire-shape failure mode is path-agnostic — both Flight SQL (which issues a gRPC call) and direct gRPC callers send the same typed struct and pass through this one validation layer — so the validation belongs at that convergence point.

**Why:** the principle was caught during CHA-172 review (PK dedup/membership checks were wrongly in penca-sql-server's `ddl`). The precise *location* was sharpened during CHA-386 review: I put the new column-type gate in `penca-api::write::validate_create_table_column_types`; the user flagged "this should be moved to `penca-server-grpc/src/validation/write.rs`" — and indeed that module already had `validate_create_table` doing the sibling checks. Folded the per-column `CanonicalType::from_arrow` check into it and dropped the penca-api copy. (Note: the older CHA-172 PK check still sits in `penca-api::write` — that's the less-ideal placement this review corrects the pattern away from; new wire-shape validation goes in the penca-server-grpc validation module.)

**How to apply:** when writing validation inside penca-sql-server, ask "does the same failure mode exist for a direct gRPC caller sending the equivalent typed request?" Inventory the candidates before writing the fix; don't reflexively put validation where you happen to be editing.

- **Move down to penca-api** when the rule is on the typed wire shape: PK list dedup/membership/non-empty, retention-config bounds, version-uuid format, arrow_schema field constraints, request-field-cross-references.
- **Stays SQL-side** when the rule is sqlparser-syntactic and has no gRPC equivalent: AST-flag rejections (`reject_unsupported_table_modifiers` style — `IF NOT EXISTS`, `OR REPLACE`, `INHERITS`, `STRICT`, …), `Expr::Identifier` bare-identifier checks in `PRIMARY KEY(...)` (sqlparser produces `Expr`; gRPC takes `Vec<String>`), 3-part name rejection (gRPC takes flat `(catalog_uuid, schema_name, table_name)`), DEFAULT-clause rejection in CREATE TABLE (gRPC takes `arrow_schema` bytes), SQL-type → Arrow-type translator rejections (gRPC takes pre-built `arrow_schema`).

Related: the "Fail fast at the boundary where the failure is detected" section of `docs/style-guide.md` (validate at the boundary the failure was detected), and [[feedback_flight_sql_driver_parity]] (the same single-classifier discipline applied to the SQL routing side).
