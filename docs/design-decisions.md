# Design Decisions

This document records critical design tradeoffs and their rationale. The
README describes *what* Penca does; this document explains *why* it is built
the way it is.

Each entry states the decision, the alternatives considered, and the reason
the chosen approach won.

---

## Branch partitioning over independent tables

Per-catalog log tables (`commit_tx_log`, `begin_tx_log`, `abort_tx_log`,
`table_metadata_upsert_log`, `table_metadata_delete_log`) use Postgres LIST
partitioning by `branch_uuid` rather than fully independent per-branch tables
(the pattern used for data tables). The two `table_metadata_*` tables are
subpartitioned `schema_uuid → branch_uuid` per
[decisions/0008-table-metadata-subpartitioning.md](decisions/0008-table-metadata-subpartitioning.md).

**Why not independent tables?** Some per-catalog operations are not
branch-scoped. The internal `get_tx_status` lookup (used by `CommitTx` /
`AbortTx` to classify a `tx_uuid` as Open / Aborted / Expired / Committed)
joins `begin_tx_log`, `abort_tx_log`, and `commit_tx_log` for a single
`(catalog, branch, tx_uuid)` triple — and the merge-fast-forward and
sweep helpers walk every branch's `commit_tx_log` partition under a catalog.
With partitioning, these queries hit the parent table and Postgres scans
all partitions transparently. With independent tables, the caller would
need to enumerate every branch's table or restructure each internal
helper to require a branch — without any read path actually benefiting
from the schema change.

**Why not keep them unpartitioned?** Without partitioning, concurrent workloads
on different branches share the same heap and indexes, causing B-tree page
splits, shared vacuum pressure, and lock contention. Partitioning gives each
branch its own physical storage while preserving cross-branch queryability
through the parent table.

Per-table data tables (`{data_log_prefix_uuid}_data_upsert_log`,
`{data_log_prefix_uuid}_data_delete_log`) are already per-branch by design —
`data_log_prefix_uuid = xxh3(table_uuid, branch_uuid)` (CHA-177) — and do not
need partitioning. (The unified `upsert_log` layout — which also applies to
the per-catalog `table_metadata_upsert_log` — is recorded in
[decisions/0001-unified-upsert-log.md](decisions/0001-unified-upsert-log.md).)

---

## Partition-direct reads and writes

Hot-path operations (reads and writes where the branch is known) target
partition tables directly instead of going through the parent table.

**Why?** `CREATE TABLE ... PARTITION OF` and `DROP TABLE partition` both take
an `ACCESS EXCLUSIVE` lock on the parent table. This lock conflicts with every
other lock mode, including `ACCESS SHARE` acquired by `SELECT`. Even though the
lock is sub-millisecond, it means creating or deleting a branch momentarily
blocks all reads on the parent — including reads for unrelated branches that
would otherwise be partition-pruned.

By targeting partition tables directly when the branch is known, hot-path
operations never touch the parent table and are unaffected by DDL on other
branches. Only the internal cross-branch reads (e.g., the `tx_uuid`
disambiguation legs of `get_tx_status`) go through the parent table, and
these are infrequent.

---

## Eager branch materialization

`create_branch` eagerly creates the branch's per-catalog log partitions
(`commit_tx_log`, `begin_tx_log`, `abort_tx_log`) plus a `table_metadata_*` branch
sub-partition under every existing schema, copies table metadata records, and
creates empty physical data tables for all tables visible on the source branch.

**Why not lazy?** Lazy creation (materializing on first write) requires branch
hierarchy traversal to find the parent branch's metadata, as-of-timestamp
scoping to respect the branch's base transaction, and handling of race
conditions when concurrent writes trigger lazy creation simultaneously. This
complexity is hard to get right and produces subtle bugs — for example, a table
that exists on the source branch appearing to "not exist" on the new branch
until someone explicitly re-creates it.

Eager materialization avoids all of this. Branch creation is not a hot-path
operation and is expected to be moderately expensive (it creates real Postgres
tables). The tradeoff is acceptable: branch creation takes slightly longer, but
every subsequent read and write on the branch works correctly without fallback
logic.

---

## "Default to main" lives only in the client facade

## First-class domain objects in proto, not response wrapping everywhere

External-facing domain objects (`Catalog`, `Schema`, `Table`, `Branch`)
live in `common.proto` as standalone messages and are composed into response
wrappers (`GetCatalogResponse`, `ListTablesResponse`, etc.) in the service
protos. This makes them reusable across multiple RPCs and gives the client a
single type to depend on. (No `Tx` here: transactions are an internal
mechanism — see [decisions/0018-tx-is-internal-no-introspection-rpc.md](decisions/0018-tx-is-internal-no-introspection-rpc.md).)

**Why not apply this to internal protos?** The messages in
`storage_metadata.proto` (`PlanResponse`, `HotStoragePlan`,
`LogSegment`, `SnapshotSegment`, `GetTableMetadataResponse`) are internal operational
structures, not domain objects. Nothing outside the storage layer references
them, and no message shape is reused across multiple responses. Extracting
them into standalone types would be premature abstraction — the pattern is
only valuable when the same object appears in multiple contexts.

---

## Transactional branch creation and deletion

`create_branch` and `delete_branch` wrap all their work — branch_store
row, partition DDL, table materialization / data table drops — in a single
Postgres transaction. If any step fails, everything rolls back.

**Why is this safe?** Postgres supports transactional DDL (`CREATE TABLE`,
`DROP TABLE` roll back on abort). The DDL targets per-branch partition tables
and per-branch data tables, not shared resources, so the lock footprint is
limited to tables that don't exist yet (create) or are about to be removed
(delete).

**Why not leave them non-transactional?** Without a transaction, a failure
partway through `create_branch` leaves a partially-materialized branch
(branch_store row exists, some partitions created, some tables missing).
Similarly, a failed `delete_branch` could drop data tables but leave the
branch_store row, or vice versa. Both states require manual cleanup.

---

## "Default to main" lives only in the client facade

Proto branch fields (`branch_uuid`, `branch_name`) are optional as part of the
UUID-or-name identifier pattern. The client facade defaults `branch_name` to
`"main"` when neither field is provided. Internal code — `resolve_branch`,
managers, auditable stores — never defaults and requires a concrete branch.

**Why?** This eliminates ambiguity at every internal layer. In particular,
`branch_uuid=None` in the auditable store unambiguously means "I am targeting a
partition table directly, do not add a branch filter" rather than "I forgot to
pass a branch, please default to main." Keeping the default in one place (the
client facade) makes it easy to find and reason about.

---

## Python Flight SQL uses a context factory, not a shared `SessionContext`

The Python reference Flight SQL server (`DataFusionFlightSqlServer`) obtains a
`datafusion.SessionContext` via a caller-supplied factory invoked on every
Flight SQL RPC, rather than holding a single shared context. Semantically:
each concurrent request gets its own isolated planning context; mutations on
one request's context can't leak into another. Also enables dynamic catalog
and schema updates — the factory closes over penca's metadata client, so
freshly-registered tables become visible to new requests without restarting
the server. This matches the Rust server's intended behavior for
dynamically-evolving catalogs.

**Why not share one `SessionContext` across requests?** DataFusion's
`SessionContext` is not guaranteed safe for concurrent planning across
threads, and `FlightServerBase` dispatches RPCs on a native thread pool. A
shared context is a real thread-safety hazard.

**Why not clone the context per request (what Rust does)?** The Rust server
clones a `SessionState` (cheap, Arc-backed) for each call. The Python
`datafusion` package doesn't expose `SessionState` or any real clone
operation — `copy.copy` returns a Python alias pointing at the same
underlying Rust object (mutations leak), and `copy.deepcopy` can't pickle.
No clone primitive exists in the binding.

**Why a factory instead of rebuilding inline?** The factory is configured
once at server construction time (by penca's wiring code, not end users),
keeping the server-side interface clean. Each RPC calls it to get a fresh
context. Guidance for authors: factories should only register pre-built
catalog providers (which resolve tables lazily via `LifecycleManager`) — sub-
millisecond overhead. Avoid eager I/O inside the factory (`register_parquet`
reads footers, `register_listing_table` hits object storage,
`CREATE VIEW` DDL re-plans), because it runs on every RPC.

Tracked upstream: a real `clone_ctx()` in `datafusion-python` would let
Python match Rust's mechanics exactly; until then the factory is the correct
shape.

---

## Cold segments are cached by content hash, not by row uuid

Every cold-artifact metadata row — persist segments, snapshot segments, and
segment index sidecars — carries a `content_hash`: an `xxh3_128` digest of the
**typed in-memory Arrow batch**, computed once at write time
(`penca_core::digest::segment_content_hash`). `SegmentCache` is keyed by
`(content_hash, format)`.

**The problem.** Reference copies mint a new row uuid over an unchanged
`(object_uri, offset, length)`: `insert_carried_snapshot_segments` and
`insert_carried_segment_indexes` carry a snapshot forward (CHA-531), and
`fork_copy` materializes a fork's cold references at fork time (CHA-539).
Nobody rewrites the bytes. A uuid-keyed cache therefore stored N identical
decodes of one byte range — N× the memory under a fixed byte budget, and N cold
reads to fill it. A fork's cold footprint is *mostly* its parent's files, so N
grows with the branch count, which is the thing Penca expects to be cheap.

**Why hash the decoded batch, not the encoded file bytes.** The key has to name
one decoded batch — equal hash must mean equal decode, or two unrelated segments
would share an entry. Hashing the typed batch gives that directly, and it is what
is available where the digest is taken: write time, *before* the format writer
encodes. Hashing the stored object would mean reading back what was just written.
It also makes dedup insensitive to encoding choices, so two independently written
segments holding the same rows share an entry even if the writer picked different
row-group boundaries or dictionary encodings.

**Why `format` is the other half of the key.** Digesting the batch pre-encode is
exactly what lets one hash name files in two formats, once
`OBJECT_STORAGE_FORMAT` has been flipped between two writes of the same content.
The cached *value*, meanwhile, is the file-native decode — a per-format artifact,
since a round-trip may widen a type or re-dictionary-encode. So the format joins
the cache key rather than the digest: the pair names one native decode, and
`content_hash` stays a pure function of the batch, which is what lets a reference
copy inherit it verbatim.

**What the key deliberately does *not* do is separate a reference copy from its
source.** Carry-forward and fork copy select `old.content_hash` verbatim
(`fork_copy.rs`, `snapshot.rs`, `segment_index.rs`) and it is never recomputed —
that inheritance *is* the dedup. So when a fork `ALTER`s a column, its rows still
read the parent's bytes under the parent's hash: parent and fork share one cache
entry while disagreeing about that column's type, and no hashing scheme could
separate them, because neither row's digest was ever taken under its own read
schema.

That is what forces the cached *value* to be the file-native decode, with
caller-shaping (projection + null-fill of columns added by a later
`ALTER TABLE ADD COLUMN`) moved *after* the lookup —
`FormatReader::read_segment_native` plus `reader::shape_to_schema`. A
caller-shaped entry carries the schema of whichever branch decoded first, so the
second branch's read fails on a type mismatch its own metadata never justified.

Concretely, with a file holding `{row_uuid, name, value}` that both branches
reference, parent having run `ADD COLUMN extra Int64` and fork `ADD COLUMN extra
Utf8`:

    caching the shaped batch — the bug
      fork   miss → decode + fill → {row_uuid, name, value, extra: Utf8}
      parent HIT  → gets extra: Utf8, its own metadata says Int64   ✗

    caching the native decode — what ships
      fork   miss → decode → {row_uuid, name, value}
                  → shape to fork   → extra: Utf8    ✓
      parent HIT  →          {row_uuid, name, value}
                  → shape to parent → extra: Int64   ✓

The read-time schema still governs the output — it just governs it at the
shaping step, per caller, rather than at the decode. Only the decode has to be
segment-scoped, because only the decode is shared.

### Rejected alternative: fold the read schema into the key

The symmetric design adds a fingerprint of the read-time schema to the key and
caches a value already adapted to it. Both halves are known before the read, so
it is implementable, and it is correct: the fingerprint separates the two
branches above. It comes in two strengths, and the weaker one is not worth
arguing against.

Caching the fully **shaped** table — projection included — fragments the key
space by query: `SELECT name` and `SELECT name, value` over one segment become
different entries, combinatorial in the column subsets queries touch. That is
decisive but it is also avoidable, so it is not the real comparison.

The strong form caches the **type-cast** table: present columns cast to the
read-time schema's declared types, with projection and null-fill still applied
per caller after the lookup. Its cost is that the key still moves when the
schema does, and only the *key* moves — the data does not:

- **`ALTER TABLE ADD COLUMN` evicts a footprint that did not change.** The added
  column is in no file and affects no cast, yet it re-fingerprints every segment
  of the table, so the whole cold footprint is re-fetched and re-decoded without
  a byte having been rewritten. Narrowing the fingerprint to only the columns a
  file actually holds would fix this, but which columns those are is not known
  until after the read.
- **It collapses the dedup this entry exists for.** A fork and its parent share
  entries only while their schemas agree; the first `ALTER` on either side
  duplicates the entire shared footprint — the sharing degrades exactly when a
  branch is used for what branches are for. That is the motivating case, not an
  edge case.

Its advantage is real but currently unrealizable: a cached cast would let a read
succeed where the file's decoded type differs from the declared one, which the
shipped path cannot do — `null_fill_to_schema` takes present columns verbatim and
`RecordBatch::try_new` then rejects a mismatch. Penca's schema evolution is
`ADD COLUMN` only, so no supported operation produces that divergence. If type
evolution is added later, the answer is to cast in the shaping tail, where it
applies per caller and is visible as a data-semantics decision — not to hide it
behind a cache key.

### What directs the decode today, and what should

Neither reader consults the stored schema. Parquet decodes under the file's
embedded Arrow schema (`builder.schema()`) and Lance under
`reader.metadata().file_schema`; the caller's `SchemaRef` only selects column
*names* to request (`requested_columns`) and drives the shaping tail. No cast or
type coercion happens anywhere in `penca-format`'s readers or writers, so the
decoded types are whatever the encoder chose to write and the decoder chose to
return.

That is why `format` is in the key: the file's embedded schema is a per-format
artifact, and `shape_to_schema` cannot absorb a divergence — `null_fill_to_schema`
takes present columns by name verbatim, and `RecordBatch::try_new` then validates
their types against the output schema, so a mismatch is a hard read error.

**Why the scope is uniform across artifact classes.** Base segments and index
sidecars both come out of object storage through the same `SegmentCache` and are
both reference-copied by carry-forward and fork copy, so they duplicate for the
same reason and dedup the same way. Sidecars were briefly scoped out on the
argument that `segment_index_schema(key_types)` makes the cached value
caller-dependent — but that argument is wrong for the same reason it is wrong
for base segments, and the native-decode split answers it.

`content_hash` is `NOT NULL` with no default on all three tables
(`penca-db/src/dialect/pg.rs`). Every writer computes it and a catalog predating
the column is recreated rather than migrated, so there is no legacy row to
default and no uuid-fallback key space to keep alive.

**Non-goal.** `tx_log_persist_segment_metadata` has no `content_hash`: it is
never cache-read and never reference-copied.

**Open question.** CHA-545 names checksum reuse as a possible second use of this
digest. It is not available yet: the digest is taken on the in-memory batch
before the format writer encodes it, and Parquet may widen a type or
re-dictionary-encode on round-trip, so using it as a stored-file checksum needs
round-trip stability established first.

**Open question.** The decode should be directed by the segment's *stored*
write-time schema rather than by the file's embedded one. `__penca_system__.tables`
already holds it — `arrow_schema` is Arrow IPC and every row is a complete table
definition at a point in time — and a segment row carries `branch_uuid`,
`table_uuid`, and `max_commit_seq_num`, so the definition in force when it was
written is an as-of read away. Doing that would make the decoded schema a function
of metadata instead of of the encoder's round-trip behavior, let a file whose
physical schema contradicts its declared one be rejected rather than silently
trusted, and make `format` droppable from the cache key — both formats would
decode to the same declared types, so `content_hash` would name one decode on its
own. It must be the segment's write-time schema and not the reader's: the cache
key is a property of the segment alone, so anything that varies per reader cannot
direct a shared decode. The cost is resolving that schema per segment, which is
plumbing through the read path rather than a change local to `penca-format`.
