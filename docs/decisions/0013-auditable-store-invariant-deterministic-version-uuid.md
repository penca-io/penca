# ADR 0013 — Auditable-store invariant: deterministic `version_uuid`

## Status

Accepted (CHA-164 standardization).

Supersedes the random-`version_uuid` + `UNIQUE(row_uuid, tx_uuid)`
shape briefly introduced at commit 77b1da3.

## Context

Every auditable store in Penca — user data tables
(per-branch `{prefix}_data_upsert_log` / `_data_delete_log` derived
via [`naming::upsert_log_table`] / [`naming::delete_log_table`])
and the metadata Penca Tables `__penca_system__.{schemas,tables}`
(also stored as `{prefix}_data_*` per CHA-177) — holds at most one
**logical** version of a given entity per transaction. The invariant exists because:

- The resolve CTE deduplicates on the entity column (`row_uuid` for
  data, `schema_uuid` for schemas, `table_uuid` for tables) ranked by
  `commit_micros`. Two rows with the same `(entity, tx_uuid)`
  share the same `commit_micros` (it's the tx commit time), so
  the dedup tiebreaker would be undefined.
- Multiple writes of the same entity within one tx are semantically
  one logical update — `INSERT … ON CONFLICT DO UPDATE` from SQL is
  one logical operation regardless of how many physical inserts the
  caller issues.

So one row per `(entity, tx)` per auditable store is the desired
storage invariant. The question is how to enforce it cheaply, without
either an extra index or an awkward INSERT path.

## Decision

Make `version_uuid` **deterministic** from `(row_uuid, tx_uuid)`:

```python
def version_uuid(row_uuid: str, tx_uuid: str) -> str:
    return deterministic_uuid_from(row_uuid, tx_uuid)  # xxh3_128
```

(Mirrored in Rust as `naming::version_uuid`.)

The PRIMARY KEY on `version_uuid` then enforces the invariant on its
own — a duplicate write of `(row_uuid, tx_uuid)` computes the same
`version_uuid` and trips the PK. No separate `UNIQUE(row_uuid,
tx_uuid)` index needed.

Insert paths handle the legitimate "write same entity twice in one
tx" case via `ON CONFLICT (version_uuid) DO UPDATE`:

```sql
INSERT INTO {upsert_table} (version_uuid, row_uuid, tx_uuid, …)
VALUES (…)
ON CONFLICT (version_uuid) DO UPDATE
  SET <user_col> = EXCLUDED.<user_col>, …
```

`version_uuid` is deliberately **not** in the SET clause — it can't
change because it's the value we just conflicted on. The user
columns are updated in place; the row's logical contents change but
its identity (`version_uuid`) is stable for the lifetime of
`(row_uuid, tx_uuid)`.

For `delete_log`, the symmetric shape uses `ON CONFLICT
(version_uuid) DO NOTHING` — a duplicate tombstone is a no-op.

## Consequences

**Positive:**

- One row per `(entity, tx)` is structurally guaranteed by the PK
  alone. No second index to maintain.
- The auditable-store invariant is visible in the schema (PK on a
  derived column) and the naming function — the PK *is* the
  invariant, not a downstream consequence of one.
- `version_uuid` is **pre-knowable**: an ETag-aware caller can
  compute the version_uuid client-side from `(row_uuid, tx_uuid)`
  without round-tripping. Useful for `If-Match`-style conditional
  reads / cache invalidation.
- Storage uniformity: every UUID in Penca is deterministic from its
  inputs (`catalog_uuid`, `schema_uuid`, `table_uuid`, the recursive
  persist + snapshot chain (ADR 0016), and now `version_uuid`). Same
  `xxh3_128` derivation throughout.
- One fewer B-tree index per auditable-store table (~5–10 µs and
  ~50 B/row saved per insert vs. PK + separate `UNIQUE`).

**Negative:**

- `version_uuid` is no longer a fresh value per insert attempt — it
  matches across retries of the same logical write. ETag readers
  watching for "did the row change physically?" can't tell from
  `version_uuid` alone; they observe data changes via the user
  columns. In practice this is what callers want — "did the logical
  version change?" is the meaningful question, and that's
  `(row_uuid, tx_uuid)` itself.
- Every INSERT path needs the `ON CONFLICT (version_uuid)` clause.
  One extra line per insert site, but it's load-bearing — forgetting
  it surfaces as a runtime PK violation rather than the intended
  UPSERT semantic.

## Alternatives considered

### Random `version_uuid` + `UNIQUE(row_uuid, tx_uuid)`

The shape briefly introduced in 77b1da3. `version_uuid` is fresh per
insert (`uuid4()` or `gen_random_uuid()`), and a separate `UNIQUE`
constraint enforces the invariant on `(row_uuid, tx_uuid)`. ON
CONFLICT keys off `(row_uuid, tx_uuid)` and rotates the
`version_uuid` on update so an ETag reader sees the row's identity
change.

**Trade-offs vs. deterministic + PK:**

|                          | Deterministic + PK (chosen) | Random + UNIQUE                      |
|--------------------------|-----------------------------|--------------------------------------|
| Indexes per table        | 1 (PK)                      | 2 (PK + UNIQUE)                      |
| INSERT cost              | PK check                    | PK check + UNIQUE check (~5–10 µs)   |
| ON CONFLICT key          | `(version_uuid)`            | `(row_uuid, tx_uuid)`                |
| `version_uuid` rotates?  | no — stable per `(row, tx)` | yes — fresh per insert attempt       |
| Pre-knowable ETag?       | yes — caller can hash       | no — must round-trip                 |
| Storage symmetry         | every UUID derived          | `version_uuid` is the odd one out    |

**Why we picked deterministic + PK:**

The two enforcement strategies are functionally equivalent —
deterministic + PK catches "(row_uuid, tx_uuid) seen before" via the
identical hash; random + UNIQUE catches it via a separate index
lookup. The deciding factors are storage cost (one fewer index per
table), pre-knowable ETags (clients can derive `version_uuid` from
the same inputs the server uses), and uniformity with the rest of
Penca's UUID derivation chain.

The "ETag reader needs `version_uuid` to rotate to detect physical
re-writes" argument doesn't survive scrutiny — `(row_uuid, tx_uuid)`
already pins the logical version, and ON CONFLICT DO UPDATE rewrites
the user columns, so a reader sees the data change directly.

### Read-time tiebreaker via `BIGSERIAL` or `written_at_micros`

Drop both the PK-via-deterministic and the UNIQUE. Allow N rows per
`(entity, tx)` to coexist. Add a monotonic per-row column
(`seq BIGSERIAL` or `written_at_micros BIGINT NOT NULL DEFAULT
now_micros()`) and break dedup ties in the resolve CTE:

```sql
ROW_NUMBER() OVER (
  PARTITION BY entity_col
  ORDER BY commit_micros DESC, seq DESC  -- tiebreak
)
```

Rejected because: (a) it weakens the storage-level invariant — "at
most one version per (entity, tx)" becomes "the resolve CTE picks
one of N versions", which is a weaker contract for anyone reading
the table directly. (b) The `ORDER BY` extra-sort cost is paid on
every read; the deterministic-PK INSERT cost is paid once per write.
For OLTP workloads (write once, read many), the read-cheap option
wins.

If Penca ever targets bulk-load workloads where the
deterministic-PK INSERT cost is the bottleneck, this trade-off is
worth revisiting — drop the PK enforcement and add the tiebreaker
column.

### Skip enforcement entirely; rely on caller discipline

Just trust callers not to write the same `(entity, tx)` twice. No
constraint, no tiebreaker, undefined behavior on duplicates.

Rejected because: (a) the SQL DML gateway's `INSERT … ON CONFLICT DO
UPDATE` is a legitimate user pattern that translates to "write same
row twice in one tx" at the storage layer; we can't push discipline
up to a SQL caller. (b) Retry-safety isn't a goal here (ADR 0011)
but undefined-behavior-on-retry is hostile to anyone debugging a
transient failure.

## Related decisions

- [ADR 0011](0011-transactional-metadata-stores.md) — the
  transactional-metadata model that makes every metadata store an
  auditable store; this ADR specifies how each one enforces its
  one-per-`(entity, tx)` invariant.
- [ADR 0012](0012-metadata-as-first-class-tables.md) — the
  metadata-as-first-class-tables shape that makes the same enforcement
  mechanism work for both data and metadata logs uniformly.
- [ADR 0016](0016-canonical-uuid-construction-for-derived-rows.md) —
  the recursive `row_uuid_for_pk` chain that names every derived row
  in the persist + snapshot family. ADR 0013's auditable-store
  invariant is **not** extended to those rows: the persist + snapshot
  metadata tables are not auditable stores (no `commit_tx_log` join, no
  per-row tx commit boundary), so the deterministic-UUID mechanism
  is reused as a hash-formula convention only — `ON CONFLICT DO
  UPDATE` collapses phase-1 retries via the same machinery without
  the auditable-store contract attaching.
