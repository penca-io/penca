# Style Guide

## Formatting

- Use descriptive variable names; avoid single-letter abbreviations.
- When a function call doesn't fit on a single line, put each parameter on
  its own line. If it fits on one line, keep it on one line.
- Add a blank line after compound statement blocks (if/elif/else, for, while,
  with, try/except/finally) before the next statement.
- No cross-package re-exports. Re-exporting from a same-package
  `__init__.py` (e.g., the ABC or a sibling concrete class) is fine.
  The goal is clean dependency boundaries for a future build system
  (e.g., Bazel).
- Import individual functions, classes, and constants — not modules. Write
  `from penca_proto.external.v1.write_pb2 import CreateCatalogRequest`
  instead of `from penca_proto.external.v1 import write_pb2`. Write
  `from penca_client.naming import upsert_log_table` instead of
  `from penca_client import naming`. This applies to all imports, not
  just protos.
- Never alias imports. Write `from penca_client.naming import get_branch_uuid`
  instead of `from penca_client import naming as fc_naming`.
- No lazy (function-level) imports. All imports go at the top of the
  file. Use `if TYPE_CHECKING:` blocks for imports only needed by type
  annotations. Same rule for Rust: `use` statements live at module
  scope, not inside fn bodies — including `use Trait as _;` lines that
  bring extension methods into scope.
- No method or settings-subsection aliasing. Inline the full call
  path at every site instead of binding it to a shorthand variable.
  Write `self._dialect.quote_identifier(name)`, not
  `qi = self._dialect.quote_identifier; qi(name)`. Write
  `settings.object_storage.base_uri`, not
  `obj = settings.object_storage; obj.base_uri`. The full path is
  self-documenting and survives ctrl+click in the IDE; partial
  unpacking (some fields aliased, others inline) reads worse than
  either pole.

## Naming

Rust modules, types, and fields follow these conventions:

- **Identity over transport.** Name a value for what it *is*, not
  how it's distributed. Use `session_uuid` (the identity), not
  `cookie` (the transport), even when the value travels over an HTTP
  `cookie` header. Add an inline comment if the transport is
  non-obvious. Identity-focused names survive transport changes (if
  cookies become bearer tokens, `session_uuid` is still the right
  name).
- **Modules named for the cohesive subsystem.** When a feature has
  both data structures and the plumbing that wires them in (e.g.
  tower layer, interceptor), put both in the subsystem-named file
  (`session.rs` containing `Session`, `SessionCache`, `SessionLayer`)
  rather than splitting into a "machinery" file (`middleware.rs`,
  `session_layer.rs`). Don't name modules for hypothetical future
  neighbours — extract a shared `middleware.rs` only when a second
  layer actually arrives and feels generic enough to share a home.
- **Concise domain types.** When a struct represents one logical
  thing in the domain, name it after the thing (`Session`), not
  after the role it plays in a container (`SessionEntry`). Reserve
  the `Entry` suffix for genuine wrappers (e.g. `CacheEntry` around
  a `Session`).
- **Distinguish server-level fallbacks from session-level pins.**
  When a value is *copied* from a server-wide knob into a per-session
  invariant, the two keep distinct names. The server's fallback is a
  "default" (used when the client doesn't supply one); the session's
  copy is a "pin" (immutable for the connection's lifetime). E.g.
  `SessionCache.default_catalog_name` (server-wide fallback) +
  `Session.catalog_name` / `Session.catalog_uuid` (per-session pin,
  no `default` prefix because once pinned it's THE catalog). Don't
  unify these into one name; they are different things at different
  scopes.
- **Typed value vs. its string form.** When a typed value (`Uuid`, a
  custom struct) is shadowed by its string serialization, the bare
  name is the typed value and the `_str` suffix is the string form;
  `_strs` is a `Vec<String>` of them. Write
  `let foo_uuid: Uuid = …; let foo_uuid_str = foo_uuid.to_string();`,
  never a `_typed` suffix on the typed variant — that inverts the
  reader's expectation (`foo_uuid` should *be* a `Uuid`) and forces
  every call site to re-derive which type it holds. A plain `String`
  with no typed counterpart stays unsuffixed (e.g. `all_written_uris`).
  When introducing the pair, name them `foo` / `foo_str`; if you find
  yourself reaching for `_typed`, rename the existing string shadow to
  `_str` instead (use a typed rename so comments aren't touched).

## Check for parallel implementations before adding helpers

Before writing a new helper, type, or abstraction, grep for similar
SQL fragments / function shapes / inline blocks elsewhere in the
codebase. If you find a parallel implementation, the right move is
usually to **collapse the parallel paths into a single canonical
helper** — not to rename one, not to introduce a third "shared"
layer that wraps both.

- Name the duplication in the plan and propose collapsing — don't
  quietly add a third path.
- Prefer deletion of misleadingly-named functions over renaming. If
  a name only made sense for the original use site, the rename will
  rot the moment a new caller appears; deletion + a single honest
  name is more durable (e.g. `create_merge_tx` was deleted, not
  renamed to `auto_commit_tx`, because the merge-specific framing
  was already a misnomer once `write_data` used it).
- Pre-emptively flag the duplication in your own plan rather than
  waiting for review to catch it. The pattern is "I noticed X and Y
  do the same thing — here's the unified version," not "I added Z
  which mostly replaces X."
- Applies to plumbing too. Don't synthesize values like a hardcoded
  `author="system"` server-side when the caller could plumb the real
  value through — match the canonical pattern (e.g. `WriteData`
  carrying `author` / `comment` from the request).

## Compose small single-responsibility functions

Prefer many small functions that each do exactly one thing and chain
together over a single large function with many knobs. Sketch the
decomposition during design — before writing the code — not after.

Three smells that mean "split into sibling functions, don't add a
knob":

1. A signature heading toward >4–5 parameters.
2. A boolean argument that toggles between two distinct code paths.
3. Two callers that look similar but differ in *shape* (e.g. `SELECT`
   vs `INSERT ... FROM SELECT`, or CTE-body vs full statement), not
   just in data — those are siblings sharing a smaller kernel, not one
   parameterized function.

Mega-functions make future divergence likely (a new caller pressures
you to add another knob instead of writing a sibling), force readers
to hold every toggle at once, and make the next refactor harder than a
rewrite. When in doubt, sketch the call sites first and the shared
helper second — the smallest shared kernel usually emerges only after
two or three callers are written out by hand.

## Fail fast at the boundary where the failure is detected

If a mint / setup / lookup can't complete cleanly, return the error
immediately — don't store an empty/sentinel value and rely on a later
codepath to surface it.

- **No graceful-degradation fallbacks at internal boundaries.** Public
  API contracts and external integrations sometimes need them; internal
  helpers don't. If a lookup fails, return `Err(...)` — don't
  log-and-continue with an empty value.
- **No two-step error paths.** If you're about to "stash an empty
  UUID / null / sentinel, the next call will catch it" — surface the
  error at the first call site instead.
- **No test-only branches in production code.** If a unit test needs a
  fake dependency, use a constructor that produces a real but
  non-connecting instance (e.g. `PgPool::connect_lazy` wrapped in
  `PgDriver::from_pool`). If that's not feasible, the test belongs in
  the integration suite. A code path that only ever fires in tests or
  degraded modes is a smell — restructure to remove it.
- For middleware/services that can't return `Result` directly, use
  protocol-native error propagation — for tower-on-tonic, a
  `Status::into_http()` trailer-only response.

A failure surfaced on the request that triggered it is better UX than
a half-success that fails opaquely two requests later — trust the
client to retry.

### Wire-shape validation lives in the gRPC validation module

Validation of a typed gRPC request shape (`CreateTableRequest`,
`CreateSchemaRequest`, `MutateDataRequest`, …) belongs in
`crates/penca-server-grpc/src/validation/<service>.rs` — the module the
servicer runs *before* dispatching into penca-api. Not as a per-wire-path
filter inside penca-sql-server, and not inside a penca-api servicer impl.

The test: **would the same failure mode surface different wording depending
on whether the caller used Flight SQL or direct gRPC?** Flight SQL issues a
gRPC call, so both callers send the same typed struct through this one
layer — that convergence point is where the check belongs. Splitting it
per-entry-point is how one driver's users get an actionable message and
another's get an internal DataFusion error for the same mistake.

- **Belongs in the validation module** — anything on the typed wire shape:
  retention-config bounds, version-uuid format, `arrow_schema` field
  constraints, supported column types, cross-field references.
  `validation::write::validate_create_table` is the canonical example; fold
  new per-field checks into the existing sibling rather than adding a
  parallel one.
- **Stays SQL-side** — anything sqlparser-syntactic with no gRPC equivalent:
  AST-flag rejections (`IF NOT EXISTS`, `OR REPLACE`, `INHERITS`, `STRICT`),
  `Expr::Identifier` checks in `PRIMARY KEY(...)` (sqlparser yields `Expr`,
  gRPC takes `Vec<String>`), 3-part name rejection, `DEFAULT`-clause
  rejection, and SQL-type → Arrow-type translator rejections.
- **Moves down to the penca-api boundary** when the check must also protect
  **in-process** callers, which never traverse the servicer. The primary-key
  dedup / membership / non-empty checks sit in
  `penca-api::write::create_table` for exactly this reason, and that
  placement is deliberate — see the comment there. Both wire paths still get
  identical wording, because both reach `create_table`.

The two live side by side in `create_table` and the asymmetry is intentional:
the column-type gate is enforced upstream in the validation module, so any
future in-process caller must replicate the `CanonicalType::from_arrow` check
itself, while the PK checks come along for free. Read that comment before
"fixing" either one. Inventory the candidates before writing a fix; don't
reflexively put validation where you happen to be editing.

## Proto messages as canonical types

- Use proto message types directly as function parameters and return types.
  Do not create parallel dataclass or TypedDict hierarchies — the proto
  messages are the canonical representation.
- ABCs accept proto request messages and return proto response messages
  (e.g., `def create_catalog(self, request: write_pb2.CreateCatalogRequest) -> write_pb2.CreateCatalogResponse`).
- ABC method signatures must mirror their corresponding proto service RPCs
  1:1 — same methods, same request/response types, pagination where the
  proto has pagination, streaming (`Iterator[pa.RecordBatch]`) where the
  proto uses `stream`. If the proto changes, update the ABC to match.
- The `PencaClient` facade accepts native Python arguments (strings, ints,
  etc.) and constructs proto request messages internally before delegating to
  the manager ABCs.
- **Proto comments describe current wire semantics — not history or
  internal derivations.** A field comment says what the field *is*
  (what it identifies, when it's required, partition-pruning
  implications), not "the server derives X internally" or "CHA-NNN
  replaces the pre-existing `data_log_prefix_uuid`". Internal mechanics
  leak implementation detail to API consumers and rot when the
  implementation changes; references to removed concepts accumulate
  into archaeology. Put that in an ADR or the commit body. Same for the
  file-level preamble: describe the message shapes, not the internal
  call graph.
- **Response and request shapes carry only fields with a current
  consumer.** Don't add a proto field "for parity" with a sibling
  message or because "the scheduler will likely want it for telemetry"
  — if a value is derivable from metadata and nothing reads it today,
  leave it out. Before adding a field, name the *current* caller that
  reads it; a hypothetical future consumer is not a caller. Same for
  new request fields: only the inputs the server actually needs to do
  its job.

## Future improvements and TODOs

- When identifying a future improvement, create a Linear issue in the
  Chapala workspace and reference it in the README under
  "Open questions and future improvements".
- In code, use `TODO(CHA-123)` comments referencing the Linear issue ID
  so that implementing the improvement later only requires grepping the
  codebase for the issue ID (e.g., `grep -r "CHA-123"`).

## Avoid unnecessary serialization roundtrips

- Do not convert between types just to satisfy a function signature if
  the value will be converted back immediately. For example, parsing a
  UUID string to `Uuid` only to call `.to_string()` for SQL formatting
  is a wasted allocation — keep it as a string throughout.
- When designing helper functions, choose parameter types that match how
  callers actually hold the data. If every caller has a `&str`, don't
  require `Uuid` (forcing a parse) unless the type safety justifies the
  cost (e.g., the function is part of a public API boundary where
  invalid input would be dangerous).
- **Slice parameters in Rust — `&[String]` vs `&[&str]`:** if the
  dominant caller already holds a `Vec<String>` *and* the callee needs
  `Vec<String>` to do its job (e.g., to feed a sqlx bind parameter that
  takes ownership), the function should take `&[String]`. `&[&str]`
  only wins when the dominant caller has string literals
  (`&["a", "b"]`) or already has a `Vec<&str>`. Picking the wrong
  shape forces every call site to write
  `vec.iter().map(String::as_str).collect::<Vec<&str>>()` followed by
  the callee converting it back to `Vec<String>` internally — two
  allocations for zero benefit. SQL-formatting helpers
  (`format_sql_text_array`, `format_sql_uuid_array`) intentionally
  keep `&[&str]` because they're called from both literal-array and
  `Vec`-derived sites; the rest of the metadata-client surface should
  default to `&[String]` since the callers are proto-deserialised
  fields and DB-row decodes that already own their strings.
- Prefer enforcing invariants via visibility (`pub(crate)`) and
  documentation over forcing type conversions that add runtime overhead
  without catching real bugs.

## Intermediate structs and proto construction

Intermediate structs between a data source (DB row, storage-client
result) and a proto response are fine — they keep layering clean and
let helpers return only fields the caller didn't already know. What
they should *not* introduce is gratuitous data copies on the way to
the proto.

**Rust:** Build protos from owned, moved structs, not from borrows.
`Tx { comment: committed.comment, ... }` moves the `String`
(O(1) regardless of payload size); `comment: committed.comment.clone()`
deep-copies. Helpers that produce intermediate structs return them
by value and are consumed once — if you find yourself returning
`&Foo` then cloning fields out, pass ownership through instead.
`RecordBatch` is `Arc`-backed, so passing batches through
`Stream::map` doesn't copy user data; keep the streaming pattern,
don't materialize into a `Vec<RecordBatch>`.

**Python:** Skip the intermediate list when going row → proto. If the
row already carries everything the proto needs, build the proto
directly (see `tx_from_commit_tx_log_row`); don't accumulate dataclasses and
then iterate to build a parallel list of protos. Streaming RPCs
yield protos one at a time via generators — never collect into a
list and `return`. Where intermediate dataclasses *do* exist (small
per-call results like `TxStatus`), read fields off them; don't
`dataclasses.replace` / `copy.deepcopy` to "convert" types.

**Hot-path rule:** intermediate structs feeding streaming or
list-many RPCs must be owned-and-moved (Rust) or read-once (Python).
Stack-copying a struct header is free; deep-copying a payload is
not.

## SQL

- Always use explicit column projections. Never use `SELECT *` or
  `SELECT table.*`.
- Prefer JOINs over two-phase queries. Instead of selecting UUIDs from
  one table then using `WHERE uuid IN (...)` on another, combine both
  lookups into a single JOIN query to reduce database roundtrips.
- **Never split a single network call into two.** This applies across
  abstraction (introducing a storage-client method), porting
  (cross-language), and refactoring. Each client method maps 1:1 to
  one `driver.execute()` call. If the abstraction would force
  splitting, widen the client method (accept a list, return a keyed
  map) so the single-query shape is preserved — or leave the direct
  SQL in the manager with a comment. When composing new methods, write
  ONE SQL statement even if it requires `UNION ALL`, scalar
  subqueries, `CROSS JOIN`, or CTE tricks; `tokio::join!` for parallel
  queries is not a substitute. Before introducing a helper, check
  whether the call site makes one query or many — a cleaner-looking
  refactor that doubles roundtrips is not cleaner.

## Configuration defaults live in the deployment env, not in code

When a config value is set in the deployment (`compose.yml`, a k8s
manifest, a systemd unit, …), don't also bake a default for the same
value into code. Pick one source of truth — almost always the
deployment env — and let the other side fail loudly when the env is
missing.

A directive duplicated in two places (e.g. `RUST_LOG: info,penca=debug`
in `docker/compose.yml` *and* a `const DEFAULT_FILTER` in `lib.rs`)
inevitably drifts, and an in-code fallback masks a misconfigured
deployment with plausible-looking output that surfaces as a confusing
"why don't my logs match production?" much later. Acceptable shapes:

- deployment env owns the value; code reads it via a
  `from_default_env`-equivalent and produces a loud minimal-output mode
  on miss (e.g. `EnvFilter::from_default_env()` → ERROR-only when
  `RUST_LOG` is unset);
- code owns the value and the deployment env is the override;
- both sides only when mechanically generated from a single shared
  source.

Never two hand-maintained defaults. This is about where configuration
*policy* lives; "Fail fast" above is about error paths in code — related
instincts, different concerns.

## Memory and concurrency knobs come from service config

Any knob that caps memory or parallelism (in-flight segment reads,
worker-pool sizes, stream batch sizes, tx TTLs) is a **per-service
operational concern**, not a library constant. Wire it through the
service config struct — env var → `*ServiceConfig` (Rust) /
`*Settings` (Python) → manager constructor → the function that uses
it. Both language stacks follow this pattern; keep the env var name
and type consistent across them.

- Do **not** define module-level constants like
  `const SEGMENT_READ_CONCURRENCY: usize = 4;` or
  `SEGMENT_READ_CONCURRENCY = 4` inside library code. They freeze an
  operator-tunable value at compile time and hide it from deployment
  config.
- Segment-read concurrency specifically is a **memory-safety cap**
  that applies to every bounded-concurrency read over cold segments —
  snapshot segments *and* log segments share the same budget
  (`segment_read_concurrency ≤ floor(reader_memory_budget /
  max_segment_bytes)`). Name the knob generically (e.g.
  `segment_read_concurrency`, not `snapshot_read_concurrency`) so a
  single env var governs both read paths.
- When adding a new knob, add it to:
  1. The per-service config struct (`crates/penca-server-grpc/src/config.rs`
     or `crates/penca-sql-server/src/config.rs`).
  2. The manager struct that consumes it.
  3. The per-service binary that constructs the manager.
  4. `docker/compose.yml` and the relevant `docs/services/*.md`
     env-var table.
