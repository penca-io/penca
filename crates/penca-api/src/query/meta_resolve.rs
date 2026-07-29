//! [`QueryManager`]'s metadata reads (ADR 0028) — the system-table resolves
//! (`resolve_{table,schema,index}_metadata`), the metadata getters, the
//! `(open_tx, as_of)` snapshot resolver, and the by-branch lifecycle reads.
//!
//! The three system-table resolves pass the W_snap-keyed snapshot-list cache
//! unconditionally: the key is content-addressed by snapshot version, so every
//! resolve (current-time or time-travel) hits its own immutable entry with no
//! staleness to gate on. The by-branch lifecycle reads stay `cache = None` —
//! they must always read fresh.

// The SQL readers here carry many positional args and the `&self` receiver
// tips several over the threshold.
#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use futures_util::TryStreamExt;
use penca_core::naming;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use penca_merge::{IndexSeek, ReadSnapshot};
use penca_proto::external::v1::{Index, Schema, Table};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use penca_storage_hot::HotStorageClient;
use penca_storage_meta::convert::{
    self, index_from_record_batch, schema_from_record_batch, table_from_record_batch,
};
use penca_storage_meta::helpers::{parse_uuid, qi, resolve_branch};
use penca_storage_meta::{LifecycleManager, MetadataError, Result};

use crate::error::ApiError;

use super::QueryManager;
use super::cold_read::stream_cold_read;

/// Resolve a `tx_uuid` to its open state, returning the tx's `began_at_seq_num`.
///
/// Reads the single-shot `begin_tx_log ⟕ abort_tx_log ⟕ commit_tx_log` join via
/// `HotStorageClient::get_tx_status` against the request branch's leaf
/// partitions, and rejects:
/// - a tx with no `begin_tx_log` row (never begun, or begun on another
///   branch) → `NotFound`;
/// - a tx that is aborted / expired / already committed →
///   `FailedPrecondition`.
///
/// The returned `began_at_seq_num` is the read path's snapshot anchor
/// ([`ReadSnapshot::OpenTx`]); the append path ignores it and uses this purely as
/// a liveness gate.
///
/// Snapshot read (`for_update=false`): a best-effort fast-fail, not a lock.
/// Under READ COMMITTED a concurrent `abort_tx` / expiry sweep can land an
/// `abort_tx_log` row after this SELECT but before the append commits, so a
/// racing append can still reference a tx that just went non-open. That's
/// acceptable because final consistency is enforced at `CommitTx`
/// (`commit_open_tx` takes `FOR UPDATE OF begin_tx_log` and re-checks abort),
/// so no committed data ever references a non-open tx — the orphaned
/// upsert/delete rows are filtered by the `commit_tx_log` JOIN on read. A
/// `for_update=true` lock here would only serialize concurrent appends to the
/// same tx without buying any correctness — `CommitTx` is the authoritative gate.
pub(crate) async fn resolve_tx(
    driver: &impl DbDriver<Row = PgRow>,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    tx_uuid: &str,
) -> std::result::Result<i64, ApiError> {
    let parsed = Uuid::parse_str(tx_uuid)
        .map_err(|e| ApiError::InvalidRequest(format!("invalid tx_uuid '{tx_uuid}': {e}")))?;
    let begin_partition = naming::begin_tx_log_partition(catalog_uuid, branch_uuid);
    let abort_partition = naming::abort_tx_log_partition(catalog_uuid, branch_uuid);
    let tx_partition = naming::commit_tx_log_partition(catalog_uuid, branch_uuid);
    let hot = HotStorageClient;
    let status = hot
        .get_tx_status(
            driver,
            &begin_partition,
            &abort_partition,
            &tx_partition,
            &parsed,
            false,
        )
        .await?
        // TODO(CHA-541): these four are stringly-typed `ApiError` variants, so a
        // caller wanting to branch on which state the tx is in has to string-match.
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "transaction not found on branch {branch_uuid} \
                 (never begun, or begun on a different branch): {tx_uuid}"
            ))
        })?;
    match status {
        penca_storage_hot::TxStatus::Open {
            began_at_seq_num, ..
        } => Ok(began_at_seq_num),
        penca_storage_hot::TxStatus::Aborted {
            aborted_at_micros, ..
        } => Err(ApiError::FailedPrecondition(format!(
            "transaction {tx_uuid} was aborted at {aborted_at_micros}"
        ))),
        penca_storage_hot::TxStatus::Expired { expired_at_micros } => {
            Err(ApiError::FailedPrecondition(format!(
                "transaction {tx_uuid} expired at {expired_at_micros} \
                 (the lifecycle sweep will move it to abort_tx_log)"
            )))
        }
        penca_storage_hot::TxStatus::Committed { commit_micros } => {
            Err(ApiError::FailedPrecondition(format!(
                "transaction {tx_uuid} was already committed at {commit_micros}"
            )))
        }
    }
}

/// Pure mapping from the `commit_tx_log_seq_num` counter value (the NEXT `commit_seq_num`
/// to allocate) to the per-branch commit frontier (the last committed serial):
/// `counter - 1`. An absent counter row (`None`) or a fresh branch
/// (`seq_num = 0`) both yield `SNAPSHOT_SEQ_GENESIS` (`-1`) — `AsOfSeq(-1)`
/// sees nothing committed. Split out from [`QueryManager::branch_seq_frontier`]
/// so the offset + genesis edge are unit-testable without a row-returning
/// driver (sqlx `PgRow` is not constructible in a unit test).
fn seq_frontier_from_counter(counter_seq_num: Option<i64>) -> i64 {
    counter_seq_num.unwrap_or(0) - 1
}

/// Parse a UUID string into a [`Uuid`], mapping a malformed value to a typed
/// `MetadataError::Db` protocol error attributed to `field` (e.g.
/// `"table_uuid"`) rather than injecting the raw string into SQL.
pub(crate) fn parse_meta_uuid(s: &str, field: &str) -> Result<Uuid> {
    Uuid::parse_str(s).map_err(|e| {
        MetadataError::Db(sqlx::Error::Protocol(format!("invalid {field} '{s}': {e}")))
    })
}

/// How a system-table read selects rows.
pub(crate) enum SystemSelection {
    /// Full scan or plain residual filter (list reads, FK lists,
    /// schema-scoped shapes) — merge path only.
    Scan { filter: Option<String> },
    /// Identity restriction: seek + restricted exclusion via the merge;
    /// bypass-eligible iff `filter` is `None`.
    IdentitySeek {
        row_uuids: Vec<Uuid>,
        filter: Option<String>,
    },
    /// Unique composite name-key selection. The seek IS the complete answer
    /// (exact selection, ADR 0023/0029) → bypass when eligible; else the read
    /// derives the equivalent SQL residual from the same key/values and rides
    /// `stream_merged` (the seek entry itself never reaches the merge —
    /// penca-merge's name-entry fail-fast stays as the guard).
    NameSeek { tuples: Vec<Vec<String>> },
    /// Leading-prefix selection: `table_uuid`-only (arity-1) probes on the
    /// composite `(table_uuid, index_name)` name sidecar — the seek returns
    /// every index row of each listed table. Exact (the seek IS the complete
    /// answer for the `table_uuid IN (…)` predicate) → bypass when eligible;
    /// else the derived `l.table_uuid IN (…)` residual rides `stream_merged`,
    /// so the seek is built on top of the scan.
    PrefixSeek { table_uuids: Vec<String> },
}

/// A resolved system-table seek: the internal seek entry plus its SQL residual
/// fallback, derived from one source (the spec's key columns + the probe
/// values) so the two can't drift. Shared by [`resolve_name_seek`] and
/// [`resolve_prefix_seek`].
struct ResolvedSystemSeek {
    seek: IndexSeek,
    residual: String,
}

/// SQL residual equivalent of a composite name-key seek: per tuple a
/// parenthesized `l.`-qualified conjunction over the spec's key columns,
/// OR-joined across tuples. Single-quote escaping is centralized here.
fn name_seek_residual(key_columns: &[&str], tuples: &[Vec<String>]) -> String {
    tuples
        .iter()
        .map(|tuple| {
            let conjunction = key_columns
                .iter()
                .zip(tuple)
                .map(|(column, value)| format!("l.{column} = '{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({conjunction})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Resolve a name seek against [`naming::system_name_index_spec`]. Fails fast
/// (never a silent scan) on a table without a built-in name index or on a
/// tuple whose arity doesn't match the spec's key columns.
fn resolve_name_seek(
    catalog: &Uuid,
    system_table_uuid: &Uuid,
    tuples: Vec<Vec<String>>,
) -> Result<ResolvedSystemSeek> {
    let Some(spec) = naming::system_name_index_spec(catalog, system_table_uuid) else {
        return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
            "NameSeek on table {system_table_uuid}: no built-in system name index"
        ))));
    };
    // Fail fast (never a silent scan): an empty tuple set would validate
    // vacuously and produce a `residual = ""` — an empty WHERE fragment that
    // surfaces as a confusing DataFusion parse error deep in the merge fallback
    // instead of here.
    if tuples.is_empty() {
        return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
            "NameSeek on table {system_table_uuid}: empty name-key tuple set"
        ))));
    }
    for tuple in &tuples {
        if tuple.len() != spec.key_columns.len() {
            return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
                "NameSeek on table {system_table_uuid}: tuple arity {} != key arity {}",
                tuple.len(),
                spec.key_columns.len()
            ))));
        }
    }

    let residual = name_seek_residual(spec.key_columns, &tuples);
    Ok(ResolvedSystemSeek {
        seek: IndexSeek {
            index_uuid: Some(spec.index_uuid),
            // Carried so the snapshot seek decodes the composite name sidecar
            // against its typed key schema.
            key_columns: spec.key_columns.iter().map(|c| c.to_string()).collect(),
            tuples,
        },
        residual,
    })
}

/// Resolve a `table_uuid` leading-PREFIX seek against the same name-index spec
/// `resolve_name_seek` uses. The composite name sidecar is `(table_uuid,
/// index_name)`, so an arity-1 `table_uuid` probe seeks the leading key column
/// and returns every index row of each listed table.
///
/// Fail fast (never a silent scan): errors if the target has no built-in name
/// index, if the spec's LEADING key column is not `table_uuid` (a re-key that
/// moved it off the lead would make an arity-1 probe silently over-select — pin
/// the coupling here, at the resolve boundary), or on an empty probe set. The
/// seek carries the FULL key columns (so the sidecar decodes at full arity)
/// with arity-1 `table_uuid` tuples; the residual is the
/// `l.table_uuid IN (…)` scan, derived from the same values so the two can't
/// drift.
fn resolve_prefix_seek(
    catalog: &Uuid,
    system_table_uuid: &Uuid,
    table_uuids: Vec<String>,
) -> Result<ResolvedSystemSeek> {
    let Some(spec) = naming::system_name_index_spec(catalog, system_table_uuid) else {
        return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
            "PrefixSeek on table {system_table_uuid}: no built-in system name index"
        ))));
    };
    if spec.key_columns.first() != Some(&"table_uuid") {
        return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
            "PrefixSeek on table {system_table_uuid}: leading key column is {:?}, \
             not \"table_uuid\" — a table_uuid prefix probe would over-select",
            spec.key_columns.first()
        ))));
    }
    if table_uuids.is_empty() {
        return Err(MetadataError::Db(sqlx::Error::Protocol(format!(
            "PrefixSeek on table {system_table_uuid}: empty table_uuid set"
        ))));
    }

    let residual = table_uuid_in_filter(&table_uuids);
    Ok(ResolvedSystemSeek {
        seek: IndexSeek {
            index_uuid: Some(spec.index_uuid),
            // FULL composite key columns so the sidecar decodes at full arity;
            // the arity-1 tuples below make it a leading-prefix probe.
            key_columns: spec.key_columns.iter().map(|c| c.to_string()).collect(),
            tuples: table_uuids
                .into_iter()
                .map(|table_uuid| vec![table_uuid])
                .collect(),
        },
        residual,
    })
}

/// Compose the optional `__penca_system__.tables` schema scope
/// (`l.schema_uuid = '<uuid>'` — a TEXT column, so no cast) with an
/// optional residual filter into the outer-SELECT WHERE fragment. `l.` is the
/// latest-CTE alias `stream_merged` projects on. `None` schema scope returns
/// every schema's tables on the branch (the catalog-wide read shape).
fn schema_scoped_filter(schema_uuid: Option<&str>, filter: Option<&str>) -> Option<String> {
    match (schema_uuid, filter) {
        (Some(scope), Some(f)) => Some(format!("l.schema_uuid = '{scope}' AND ({f})")),
        (Some(scope), None) => Some(format!("l.schema_uuid = '{scope}'")),
        (None, Some(f)) => Some(f.to_string()),
        (None, None) => None,
    }
}

/// Human-readable label for the per-table read error context.
fn system_table_label(catalog: &Uuid, table_uuid: &Uuid) -> &'static str {
    if *table_uuid == naming::system_tables_table_uuid(catalog) {
        "__penca_system__.tables"
    } else if *table_uuid == naming::system_schemas_table_uuid(catalog) {
        "__penca_system__.schemas"
    } else if *table_uuid == naming::system_indexes_table_uuid(catalog) {
        "__penca_system__.indexes"
    } else {
        "__penca_system__ table"
    }
}

/// Demux the rows of a filterless `__penca_system__.indexes` scan into
/// per-table index groups keyed by `table_uuid` — the batched alternative to a
/// per-table `resolve_table_indexes` N+1 in `meta_list_tables`.
fn demux_indexes_by_table(index_batches: &[RecordBatch]) -> HashMap<String, Vec<Index>> {
    let mut indexes_by_table: HashMap<String, Vec<Index>> = HashMap::new();
    for batch in index_batches {
        for row in 0..batch.num_rows() {
            let index = index_from_record_batch(batch, row);
            indexes_by_table
                .entry(index.table_uuid.clone())
                .or_default()
                .push(index);
        }
    }

    indexes_by_table
}

/// Build the `l.table_uuid IN ('u1','u2',…)` residual scoping a batched
/// `__penca_system__.indexes` read to a specific set of tables. `l.` is the
/// latest-CTE alias `stream_merged` projects; `table_uuid` is a text column,
/// so no `::uuid` cast.
///
/// The values are inlined without a `parse_meta_uuid` guard, unlike the
/// single-table `resolve_table_indexes` path: these are stored DB rows, not
/// caller input, so a parse would be a wasted round-trip. A corrupt stored
/// `table_uuid` splices a broken query rather than surfacing a typed error.
///
/// Plain `IN (...)` — NOT Postgres-only `= ANY(ARRAY[…]::text[])` — so it is
/// valid in both the hot (Postgres) and cold (DataFusion) legs `stream_merged`
/// unions. Callers must guard the empty-slice case (an empty `IN ()` is a SQL
/// error); `meta_list_tables` skips the read entirely when no tables are listed.
fn table_uuid_in_filter(table_uuids: &[String]) -> String {
    let list = table_uuids
        .iter()
        .map(|table_uuid| format!("'{table_uuid}'"))
        .collect::<Vec<_>>()
        .join(",");
    format!("l.table_uuid IN ({list})")
}

impl QueryManager {
    /// One plan+dispatch entry for every system-table read: build the plan
    /// through the shared cache gate, then hand off to the shared
    /// [`stream_cold_read`] kernel (DataFusion-free snapshot seek when
    /// eligible, else the `stream_all_cold` / `stream_merged` pipeline).
    ///
    /// Nothing metadata-specific survives past the `SystemSelection -> (seek,
    /// residual, exact)` resolution below — the kernel is byte-identical to
    /// `read_data`'s. The `Vec<RecordBatch>` collect is a boundary wrapper for
    /// metadata callers that want a bounded result, NOT a `collect: bool` knob.
    pub(crate) async fn read_system_table<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: &str,
        system_table_uuid: &Uuid,
        sys_arrow_schema: &SchemaRef,
        selection: SystemSelection,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<RecordBatch>>
    where
        L: DlDriver + ?Sized,
    {
        let catalog = parse_uuid(catalog_uuid);
        let label = system_table_label(&catalog, system_table_uuid);
        // (seek entry, merge residual, exactness). A name/prefix entry rides
        // the merge fallback as a selection accelerator like a user covering
        // index — penca-merge accepts any non-identity seek that carries its
        // residual `filter`.
        let (seek, filter, exact) = match selection {
            SystemSelection::Scan { filter } => (None, filter, false),
            SystemSelection::IdentitySeek { row_uuids, filter } => {
                let exact = filter.is_none();
                (Some(IndexSeek::identity(&row_uuids)), filter, exact)
            }
            SystemSelection::NameSeek { tuples } => {
                let resolved = resolve_name_seek(&catalog, system_table_uuid, tuples)?;
                (Some(resolved.seek), Some(resolved.residual), true)
            }
            SystemSelection::PrefixSeek { table_uuids } => {
                let resolved = resolve_prefix_seek(&catalog, system_table_uuid, table_uuids)?;
                (Some(resolved.seek), Some(resolved.residual), true)
            }
        };

        let (plan, _) = self
            .plan(
                driver,
                catalog_uuid,
                &system_table_uuid.to_string(),
                branch_uuid,
                snapshot.plan_as_of_micros(),
                snapshot.plan_commit_seq_upper(),
                // Metadata reads are never retention-governed, so no floor is
                // read or enforced.
                None,
                // W_snap-keyed, so safe for any resolve; a disabled cache is
                // the per-service opt-out.
                Some(self.snapshot_list_cache.as_ref()),
            )
            .await?;

        let seeks = seek.map(|entry| vec![entry]);
        let stream = stream_cold_read(
            driver,
            dl,
            &plan,
            sys_arrow_schema,
            sys_arrow_schema,
            snapshot,
            filter.as_deref(),
            seeks,
            exact,
            // Metadata reads pull at most 1-2 cold segments per branch, so
            // pruning never fires regardless of these values.
            2,
            1,
        );
        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(|e| MetadataError::Db(sqlx::Error::Protocol(format!("{label}: {e}"))))?;
        Ok(batches)
    }

    /// Resolve table metadata using the transactional auditable store CTE.
    /// Returns raw rows without retention coalesce.
    ///
    /// Visibility resolves via JOIN against `commit_tx_log_partition(catalog,
    /// branch)`; rows are deduplicated by `table_uuid`. When
    /// `open_tx_uuid` is set, uncommitted rows from that tx are visible
    /// (read-your-own-writes).
    ///
    /// Routes through `stream_merged`, so it tolerates the post-persist state
    /// where both the `__penca_system__.tables` rows and their gating
    /// commit_tx_log entries live in cold (hot `commit_tx_log_partition` is
    /// purged unconditionally up to the persist watermark). Callers must
    /// supply a `DlDriver` for cold segment access; a
    /// `ReadSnapshot::AsOfMicros` pinned to `pg_now` is the standard choice
    /// for metadata reads.
    ///
    /// `filter`, when supplied, is appended (`AND`-joined) to the optional
    /// builtin schema_uuid filter. Literals MUST be inlined
    /// (`u.row_uuid = '<uuid>'::uuid`) — `stream_merged`'s SQL builder does
    /// not support `$N` placeholders.
    ///
    /// `schema_uuid = None` returns rows for every schema on the branch — the
    /// catalog-wide read shape that branch RPCs need to fan out across
    /// `s1.t1`, `s2.t2`, ... .
    ///
    /// `table_names`, when supplied, selects by the built-in composite
    /// `(schema_uuid, table_name)` name key — `schema_uuid` must be `Some`
    /// (the key needs the scope) and `filter` must be `None`; the read seeks
    /// the name index (DataFusion-free when snapshot-covered) or degrades to
    /// the equivalent residual filter.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            schema = ?schema_uuid,
            as_of_micros = ?snapshot.plan_as_of_micros(),
            // The field NAME must stay `row_uuids`:
            // tests/integration/integration_metadata_point_read_test.py pins
            // `row_uuids=1` on the resolve spans.
            row_uuids = table_uuids.map_or(0usize, <[Uuid]>::len),
        ),
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_table_metadata<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        schema_uuid: Option<&str>,
        branch_uuid: &str,
        filter: Option<&str>,
        table_uuids: Option<&[Uuid]>,
        table_names: Option<&[String]>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<RecordBatch>>
    where
        L: DlDriver + ?Sized,
    {
        let catalog = parse_uuid(catalog_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(&catalog);
        let sys_arrow_schema: SchemaRef = Arc::new(PgDialect::system_tables_arrow_schema());

        // Filter fragments below must qualify with `l.` (latest): stream_merged
        // appends the filter to the outer SELECT after the
        // `latest l LEFT JOIN deletes d` join, so an unqualified `row_uuid` is
        // ambiguous — both `l` and `d` carry it via USING(row_uuid). `l.` is
        // the projection-side alias.
        let selection = if let Some(names) = table_names {
            let Some(schema) = schema_uuid else {
                return Err(MetadataError::Db(sqlx::Error::Protocol(
                    "by-name table resolve requires schema_uuid (the name key is \
                     (schema_uuid, table_name))"
                        .to_string(),
                )));
            };
            if filter.is_some() {
                return Err(MetadataError::Db(sqlx::Error::Protocol(
                    "by-name table resolve does not compose with a residual filter".to_string(),
                )));
            }

            SystemSelection::NameSeek {
                tuples: names
                    .iter()
                    .map(|name| vec![schema.to_string(), name.clone()])
                    .collect(),
            }
        } else if let Some(uuids) = table_uuids {
            // The schema scope (when supplied) stays a residual so a
            // wrong-schema lookup still resolves to nothing. `row_uuid` does
            // NOT equal the raw table_uuid, so each is derived canonically via
            // `row_uuid_for_pk`; the identity seek is on the `row_uuid` PK.
            SystemSelection::IdentitySeek {
                row_uuids: uuids
                    .iter()
                    .map(|table_uuid| {
                        let table_uuid_str = table_uuid.to_string();
                        naming::row_uuid_for_pk(&sys_tables_table_uuid, &[table_uuid_str.as_str()])
                    })
                    .collect(),
                filter: schema_scoped_filter(schema_uuid, filter),
            }
        } else {
            SystemSelection::Scan {
                filter: schema_scoped_filter(schema_uuid, filter),
            }
        };

        self.read_system_table(
            driver,
            dl,
            catalog_uuid,
            branch_uuid,
            &sys_tables_table_uuid,
            &sys_arrow_schema,
            selection,
            snapshot,
        )
        .await
    }

    /// The per-branch commit-order frontier: the highest committed
    /// `commit_seq_num` = the `commit_tx_log_seq_num` counter's `seq_num - 1`.
    /// The counter row holds the NEXT `commit_seq_num` to allocate, and aborts
    /// roll it back, so `seq_num - 1` is always the last committed serial (and
    /// stays gapless). The counter leaf holds exactly one row; an in-flight
    /// commit's increment is invisible under MVCC until it commits, so this
    /// reads the last-committed frontier (O(1), never a `MAX(commit_seq_num)`
    /// scan — the same source `begin_tx_log` anchors on).
    ///
    /// The seq-axis sibling of [`LifecycleManager::now_micros`] — the default
    /// "read latest" pin for `resolve_read_snapshot` and the data read. A
    /// fresh branch with no commit (`seq_num = 0`, or no counter row) yields
    /// `SNAPSHOT_SEQ_GENESIS` (`-1`): `AsOfSeq(-1)` sees nothing committed,
    /// which is correct for an empty branch.
    pub async fn branch_seq_frontier(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<i64> {
        let catalog = parse_uuid(catalog_uuid);
        let branch = parse_uuid(branch_uuid);
        let table = naming::commit_tx_log_seq_num_partition(&catalog, &branch);
        // The leaf partition holds exactly one row; mirror `begin_tx_log`'s
        // `(SELECT seq_num FROM {lsn})` capture (no WHERE needed).
        let sql = format!("SELECT seq_num FROM {table}", table = qi(&table));
        let rows = driver.execute_params(&sql, &[]).await?;
        let counter_seq_num = rows
            .first()
            .and_then(|r| r.try_get::<i64, _>("seq_num").ok());
        Ok(seq_frontier_from_counter(counter_seq_num))
    }

    /// Resolve a [`ReadSnapshot`] from `(open_tx_uuid, as_of_micros,
    /// as_of_seq)`.
    ///
    /// Mutex precedence (callers must enforce the mutex at the proto
    /// boundary; this method picks deterministically if several are set):
    ///
    /// 1. `open_tx_uuid = Some` → resolves the tx through [`resolve_tx`] and
    ///    returns [`ReadSnapshot::OpenTx`] for snapshot-isolation + RYOW. A tx
    ///    that is not open is an ERROR (`NotFound` / `FailedPrecondition`), never
    ///    a fall-through: resolving a dead tx at committed-latest would serve a
    ///    caller that believes it is inside a transaction. Only `None` — "no tx
    ///    supplied" — reaches the arms below.
    /// 2. `as_of_micros = Some` → [`ReadSnapshot::AsOfMicros`] (identifiers
    ///    resolve on the micros axis, matching a micros data read).
    /// 3. `as_of_seq = Some` → [`ReadSnapshot::AsOfSeq`] — a seq time-travel
    ///    read resolves identifiers on the SAME seq axis as its data, so a
    ///    renamed table is found at its historical name.
    /// 4. None → [`ReadSnapshot::LatestSeq`] pinned to the branch's commit
    ///    frontier. Never an unbounded read: the default "read latest" pins
    ///    the seq axis so identifiers + data compose with the seq tier-fence.
    ///    `LatestSeq` (not `AsOfSeq`) flags this as the default current-time
    ///    resolution — a distinction the DataFusion-free seek bypass gates on.
    ///    Callers that must share one pin across several resolutions in the
    ///    same RPC (e.g. `read_data` pins identifier resolution + the data
    ///    read together) capture the frontier once and thread it as
    ///    `default_frontier`; callers with a single resolution per request
    ///    pass `None` and let this method self-capture
    ///    [`Self::branch_seq_frontier`].
    pub async fn resolve_read_snapshot(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        open_tx_uuid: Option<&str>,
        as_of_micros: Option<i64>,
        as_of_seq: Option<i64>,
        default_frontier: Option<i64>,
    ) -> std::result::Result<ReadSnapshot, ApiError> {
        // The axes are mutually exclusive: RYOW only makes sense at the tx's own
        // begin frontier, so an as_of alongside an open tx is a contradiction
        // rather than a precedence question.
        if open_tx_uuid.is_some() && (as_of_micros.is_some() || as_of_seq.is_some()) {
            return Err(ApiError::InvalidRequest(
                "exactly one of as_of / open_tx_uuid may be set".to_string(),
            ));
        }
        if let Some(tx_str) = open_tx_uuid {
            let tx_uuid = parse_uuid(tx_str);
            let catalog = parse_uuid(catalog_uuid);
            let branch = parse_uuid(branch_uuid);
            let began_at_seq_num = resolve_tx(driver, &catalog, &branch, tx_str).await?;

            return Ok(ReadSnapshot::OpenTx {
                began_at_seq_num,
                tx_uuid,
            });
        }
        if let Some(ts) = as_of_micros {
            return Ok(ReadSnapshot::AsOfMicros(ts));
        }
        if let Some(seq) = as_of_seq {
            return Ok(ReadSnapshot::AsOfSeq(seq));
        }
        let frontier = match default_frontier {
            Some(seq) => seq,
            None => Self::branch_seq_frontier(driver, catalog_uuid, branch_uuid).await?,
        };
        Ok(ReadSnapshot::LatestSeq(frontier))
    }

    /// Get a single table by UUID or name, with retention coalesced from
    /// schema and catalog.
    ///
    /// `table_uuid` is stable across branches and is the dedup column on the
    /// auditable-store resolve, so the filter matches `u.table_uuid` directly
    /// — no physical UUID derivation needed.
    pub async fn meta_get_table<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        schema_uuid: &str,
        table_uuid: Option<&str>,
        table_name: Option<&str>,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<Table>>
    where
        L: DlDriver + ?Sized,
    {
        let resolved_branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;

        // Parsed up front so a malformed UUID returns a typed error rather
        // than injecting into the query.
        let (table_uuids, table_names) = if let Some(uuid_str) = table_uuid {
            let uuid = parse_meta_uuid(uuid_str, "table_uuid")?;
            (Some(vec![uuid]), None)
        } else if let Some(name) = table_name {
            (None, Some(vec![name.to_string()]))
        } else {
            return Ok(None);
        };

        let batches = self
            .resolve_table_metadata(
                driver,
                dl,
                catalog_uuid,
                Some(schema_uuid),
                &resolved_branch,
                None,
                table_uuids.as_deref(),
                table_names.as_deref(),
                snapshot,
            )
            .await?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                let mut table = table_from_record_batch(catalog_uuid, schema_uuid, batch, 0);
                // Retention coalescing is *not* applied here — callers that
                // need resolved retention compose `get_table` + `get_schema`
                // after the read; most read paths only need the table itself.
                //
                // The DEFINED indexes are attached in this same upfront
                // (cache-gated) metadata phase so a reader learns them without
                // a second round-trip.
                table.indexes = self
                    .resolve_table_indexes(
                        driver,
                        dl,
                        catalog_uuid,
                        &resolved_branch,
                        &table.table_uuid,
                        snapshot,
                    )
                    .await?;
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    /// Get a single table by `table_uuid`, **catalog-wide** (schema-agnostic).
    ///
    /// Unlike [`get_table`], this does not scope the read to a `schema_uuid`:
    /// it seeks `__penca_system__.tables` by the `table_uuid`-derived
    /// `row_uuid` alone, so it resolves the table regardless of which schema
    /// it actually lives in (`table_uuid` is globally unique by 128-bit
    /// entropy). That is what lets by-uuid callers pass a *convenient* schema
    /// — or none at all — and still resolve the `__penca_system__`
    /// bootstrap-table rows (filed under `system_schema_uuid`). The returned
    /// `Table` carries the row's own `schema_uuid`, read off the
    /// `__penca_system__.tables` row rather than a caller-supplied value.
    /// Retention is *not* coalesced here — same contract as [`get_table`].
    pub async fn get_table_by_uuid<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        table_uuid: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<Table>>
    where
        L: DlDriver + ?Sized,
    {
        let resolved_branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;

        // Parsed before stitching into SQL so a malformed UUID returns a typed
        // error rather than injecting into the query. `schema_uuid = None`
        // below drops the `l.schema_uuid` clause, so the read spans every
        // schema on the branch.
        let uuid = parse_meta_uuid(table_uuid, "table_uuid")?;
        let batches = self
            .resolve_table_metadata(
                driver,
                dl,
                catalog_uuid,
                None,
                &resolved_branch,
                None,
                Some(std::slice::from_ref(&uuid)),
                None,
                snapshot,
            )
            .await?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                // The row self-describes its schema; read it off the row
                // rather than scoping by a caller-supplied value.
                let row_schema_uuid =
                    convert::rb_uuid_str(batch, "schema_uuid", 0).unwrap_or_default();
                let mut table = table_from_record_batch(catalog_uuid, &row_schema_uuid, batch, 0);
                table.indexes = self
                    .resolve_table_indexes(
                        driver,
                        dl,
                        catalog_uuid,
                        &resolved_branch,
                        &table.table_uuid,
                        snapshot,
                    )
                    .await?;
                return Ok(Some(table));
            }
        }
        Ok(None)
    }

    /// List all tables in a (schema, branch). Retention is *not*
    /// coalesced here (see `get_table` for the rationale; same
    /// applies — tests that use `list_tables` for retention checks
    /// will need to compose the calls).
    pub async fn meta_list_tables<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        schema_uuid: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<Table>>
    where
        L: DlDriver + ?Sized,
    {
        let resolved_branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        let batches = self
            .resolve_table_metadata(
                driver,
                dl,
                catalog_uuid,
                Some(schema_uuid),
                &resolved_branch,
                None,
                // List read (schema-wide) — no single row_uuid.
                None,
                None,
                snapshot,
            )
            .await?;
        // Read the DEFINED indexes for exactly the listed tables in ONE scan,
        // then demux by `table_uuid`. A per-table `resolve_table_indexes` here
        // would be an N+1 on this list-many RPC: N identical fused watermark
        // queries + N filtered reads of the one `__penca_system__.indexes`
        // table. Scoped to the N listed tables — NOT the whole branch's index
        // catalog, which spans every schema.
        //
        // When `__penca_system__.indexes` is snapshot-materialized this
        // bounded scan becomes an O(log n) leading-`table_uuid` prefix seek
        // over the composite `(table_uuid, index_name)` name sidecar. The
        // `PrefixSeek` selection carries that seek AND the derived
        // `l.table_uuid IN (…)` residual; the not-materialized case rides the
        // residual, so the seek sits on top of the scan, not instead of it.
        let table_uuids: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .filter_map(move |i| convert::rb_uuid_str(batch, "table_uuid", i))
            })
            .collect();
        let mut indexes_by_table = if table_uuids.is_empty() {
            HashMap::new()
        } else {
            let index_batches = self
                .resolve_index_metadata(
                    driver,
                    dl,
                    catalog_uuid,
                    &resolved_branch,
                    None,
                    None,
                    None,
                    Some(&table_uuids),
                    snapshot,
                )
                .await?;
            demux_indexes_by_table(&index_batches)
        };

        let mut out = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                let mut table = table_from_record_batch(catalog_uuid, schema_uuid, batch, i);
                // `.remove` moves the `Vec<Index>` (no clone) onto the proto.
                table.indexes = indexes_by_table
                    .remove(&table.table_uuid)
                    .unwrap_or_default();
                out.push(table);
            }
        }
        Ok(out)
    }

    /// Get the Arrow schema bytes, partition keys, clustering keys, and
    /// primary keys for a table on a branch.
    ///
    /// primary_keys are returned so snapshot/persist paths can construct the
    /// widened delete_log schema. clustering_keys drive the in-partition sort
    /// at snapshot time (see `sort_record_batch_by_keys`).
    ///
    /// `table_uuid` is globally unique by probabilistic 128-bit entropy, so
    /// the by-uuid restriction narrows to exactly one row — an additional
    /// `schema_uuid` filter is neither needed nor useful, and
    /// `__penca_system__.tables` is partitioned by branch_uuid rather than by
    /// schema, so it would not prune storage either. Hence `schema_uuid=None`.
    pub async fn get_table_schema_and_layout_keys<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: &str,
        table_uuid: &str,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<(Vec<u8>, Vec<String>, Vec<String>, Vec<String>)>>
    where
        L: DlDriver + ?Sized,
    {
        // Parsed so a malformed uuid returns a typed error rather than
        // injecting into the query.
        let uuid = parse_meta_uuid(table_uuid, "table_uuid")?;
        let batches = self
            .resolve_table_metadata(
                driver,
                dl,
                catalog_uuid,
                None,
                branch_uuid,
                None,
                Some(std::slice::from_ref(&uuid)),
                None,
                snapshot,
            )
            .await?;
        let Some(arrow_schema) = convert::extract_first_binary(&batches, "arrow_schema") else {
            return Ok(None);
        };
        let partition_keys = convert::extract_first_string_list(&batches, "partition_keys");
        let clustering_keys = convert::extract_first_string_list(&batches, "clustering_keys");
        let primary_keys = convert::extract_first_string_list(&batches, "primary_keys");
        Ok(Some((
            arrow_schema,
            partition_keys,
            clustering_keys,
            primary_keys,
        )))
    }

    /// List all `table_uuid`s present on a branch, optionally filtered
    /// to a single schema.
    ///
    /// Per-branch data tables are deterministic in `(table_uuid,
    /// branch_uuid)` — callers pass that pair to the hot-tier helpers
    /// ([`naming::upsert_log_table`] / [`naming::delete_log_table`]) directly,
    /// with no separate prefix column.
    ///
    /// `schema_uuid = None` returns every schema's tables on the branch — the
    /// catalog-wide read shape DeleteBranch needs to fan out cold-storage
    /// cleanup.
    pub async fn list_table_uuids_for_branch<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        schema_uuid: Option<&str>,
        branch_uuid: &str,
    ) -> Result<Vec<String>>
    where
        L: DlDriver + ?Sized,
    {
        // Pin to pg_now rather than an unbounded read.
        let snapshot = LifecycleManager::now_snapshot(driver).await?;
        let batches = self
            .resolve_table_metadata(
                driver,
                dl,
                catalog_uuid,
                schema_uuid,
                branch_uuid,
                None,
                // List read (catalog/schema-wide) — no single row_uuid.
                None,
                None,
                &snapshot,
            )
            .await?;
        let mut out: Vec<String> = Vec::new();
        for batch in &batches {
            let Some(col) = batch.column_by_name("table_uuid") else {
                continue;
            };
            let arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("table_uuid is utf8");
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    out.push(arr.value(i).to_string());
                }
            }
        }
        Ok(out)
    }
    /// Get a schema on `branch_uuid` (defaults to main when None).
    ///
    /// Schemas live in `__penca_system__.schemas` partitioned per-branch. Each
    /// branch sees the schema set materialized at CreateBranch time plus its
    /// own subsequent DDL.
    ///
    /// `schema_uuid` is random-minted server-side, so the name-resolution path
    /// selects by the built-in `schema_name` name key rather than recomputing
    /// a hash. `snapshot` is caller-supplied so the name→uuid lookup uses the
    /// same snapshot as any subsequent data read on the resolved table.
    pub async fn meta_get_schema<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        schema_uuid: Option<&str>,
        schema_name: Option<&str>,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<Schema>>
    where
        L: DlDriver + ?Sized,
    {
        // Parsed up front so a malformed value returns a typed error rather
        // than injecting into the query.
        let (schema_uuids, schema_names) = if let Some(uuid_str) = schema_uuid {
            let uuid = parse_meta_uuid(uuid_str, "schema_uuid")?;
            (Some(vec![uuid]), None)
        } else if let Some(name) = schema_name {
            (None, Some(vec![name.to_string()]))
        } else {
            return Ok(None);
        };

        let branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        let batches = self
            .resolve_schema_metadata(
                driver,
                dl,
                catalog_uuid,
                &branch,
                None,
                schema_uuids.as_deref(),
                schema_names.as_deref(),
                snapshot,
            )
            .await?;
        for batch in &batches {
            if batch.num_rows() > 0 {
                return Ok(Some(schema_from_record_batch(catalog_uuid, batch, 0)));
            }
        }
        Ok(None)
    }

    /// List schemas on `branch_uuid` (defaults to main) ordered by name.
    ///
    /// 1 SQL query.
    pub async fn meta_list_schemas<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: Option<&str>,
    ) -> Result<Vec<Schema>>
    where
        L: DlDriver + ?Sized,
    {
        let branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        // Pin to pg_now rather than an unbounded read.
        let snapshot = LifecycleManager::now_snapshot(driver).await?;
        let batches = self
            .resolve_schema_metadata(
                driver,
                dl,
                catalog_uuid,
                &branch,
                None,
                None,
                None,
                &snapshot,
            )
            .await?;
        let mut out = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                out.push(schema_from_record_batch(catalog_uuid, batch, i));
            }
        }
        out.sort_by(|a, b| a.schema_name.cmp(&b.schema_name));
        Ok(out)
    }

    /// List schemas on `branch_uuid` (defaults to main) with pagination.
    pub async fn list_schemas_paginated<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        limit: i64,
        offset: i64,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<Schema>>
    where
        L: DlDriver + ?Sized,
    {
        let branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        let batches = self
            .resolve_schema_metadata(
                driver,
                dl,
                catalog_uuid,
                &branch,
                None,
                None,
                None,
                snapshot,
            )
            .await?;
        let mut all = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                all.push(schema_from_record_batch(catalog_uuid, batch, i));
            }
        }
        all.sort_by(|a, b| a.schema_name.cmp(&b.schema_name));
        let start = offset.max(0) as usize;
        let end = start.saturating_add(limit.max(0) as usize).min(all.len());
        Ok(all.into_iter().skip(start).take(end - start).collect())
    }

    /// List all schema UUIDs visible on `branch_uuid` (defaults to main).
    pub async fn list_schema_uuids_for_catalog<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: Option<&str>,
    ) -> Result<Vec<String>>
    where
        L: DlDriver + ?Sized,
    {
        let branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        // Pin to pg_now rather than an unbounded read.
        let snapshot = LifecycleManager::now_snapshot(driver).await?;
        let batches = self
            .resolve_schema_metadata(
                driver,
                dl,
                catalog_uuid,
                &branch,
                None,
                None,
                None,
                &snapshot,
            )
            .await?;
        let mut out = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                if let Some(s) = convert::rb_uuid_str(batch, "schema_uuid", i) {
                    out.push(s);
                }
            }
        }
        Ok(out)
    }

    /// Resolve `__penca_system__.schemas` rows for `branch_uuid`. Same
    /// shape as [`Self::resolve_table_metadata`] but for schemas.
    ///
    /// Routes through [`Self::read_system_table`] so the lookup tolerates a
    /// post-persist state where the schema row + its create_schema
    /// commit_tx_log entry both live in cold. `filter` is an optional WHERE
    /// fragment applied to the outer SELECT — qualify columns with `l.` (the
    /// latest CTE alias) to avoid ambiguity with the deletes side of the
    /// merge. `schema_names` selects by the built-in single-column
    /// `schema_name` key and must not compose with `filter`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            as_of_micros = ?snapshot.plan_as_of_micros(),
            // The field NAME must stay `row_uuids` — integration tests scrape it.
            row_uuids = schema_uuids.map_or(0usize, <[Uuid]>::len),
        ),
    )]
    pub async fn resolve_schema_metadata<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: &str,
        filter: Option<&str>,
        schema_uuids: Option<&[Uuid]>,
        schema_names: Option<&[String]>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<RecordBatch>>
    where
        L: DlDriver + ?Sized,
    {
        let catalog = parse_uuid(catalog_uuid);
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(&catalog);
        let sys_arrow_schema: SchemaRef = Arc::new(PgDialect::system_schemas_arrow_schema());

        let selection = if let Some(names) = schema_names {
            if filter.is_some() {
                return Err(MetadataError::Db(sqlx::Error::Protocol(
                    "by-name schema resolve does not compose with a residual filter".to_string(),
                )));
            }

            SystemSelection::NameSeek {
                tuples: names.iter().map(|name| vec![name.clone()]).collect(),
            }
        } else if let Some(uuids) = schema_uuids {
            // row_uuid does not equal schema_uuid; derive it canonically.
            SystemSelection::IdentitySeek {
                row_uuids: uuids
                    .iter()
                    .map(|schema_uuid| {
                        let schema_uuid_str = schema_uuid.to_string();
                        naming::row_uuid_for_pk(
                            &sys_schemas_table_uuid,
                            &[schema_uuid_str.as_str()],
                        )
                    })
                    .collect(),
                filter: filter.map(str::to_string),
            }
        } else {
            SystemSelection::Scan {
                filter: filter.map(str::to_string),
            }
        };

        self.read_system_table(
            driver,
            dl,
            catalog_uuid,
            branch_uuid,
            &sys_schemas_table_uuid,
            &sys_arrow_schema,
            selection,
            snapshot,
        )
        .await
    }
    /// Resolve `__penca_system__.indexes` rows on a branch (mirror of
    /// [`Self::resolve_table_metadata`]). `filter`, when supplied, is
    /// `AND`-appended to the outer SELECT; qualify columns with `l.` (the
    /// latest alias). `index_names` selects by the built-in composite
    /// `(table_uuid, index_name)` key — the owning table scopes the name
    /// (`index_name` is unique only within a table); must not compose with
    /// `filter`.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            as_of_micros = ?snapshot.plan_as_of_micros(),
            // The field NAME must stay `row_uuids` — integration tests scrape it.
            row_uuids = index_uuids.map_or(0usize, <[Uuid]>::len),
        ),
    )]
    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_index_metadata<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        branch_uuid: &str,
        filter: Option<&str>,
        index_uuids: Option<&[Uuid]>,
        index_names: Option<(&Uuid, &[String])>,
        table_uuid_prefixes: Option<&[String]>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<RecordBatch>>
    where
        L: DlDriver + ?Sized,
    {
        let catalog = parse_uuid(catalog_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(&catalog);
        let sys_arrow_schema: SchemaRef = Arc::new(PgDialect::system_indexes_arrow_schema());

        let selection = if let Some((table_uuid, names)) = index_names {
            if filter.is_some() {
                return Err(MetadataError::Db(sqlx::Error::Protocol(
                    "by-name index resolve does not compose with a residual filter".to_string(),
                )));
            }

            SystemSelection::NameSeek {
                tuples: names
                    .iter()
                    .map(|name| vec![table_uuid.to_string(), name.clone()])
                    .collect(),
            }
        } else if let Some(table_uuids) = table_uuid_prefixes {
            // The batched ListTables selector — a leading `table_uuid` prefix
            // seek over the composite name sidecar. Its derived residual IS
            // the fallback, so it can't compose with a caller filter.
            if filter.is_some() {
                return Err(MetadataError::Db(sqlx::Error::Protocol(
                    "table_uuid prefix index resolve does not compose with a residual filter"
                        .to_string(),
                )));
            }

            SystemSelection::PrefixSeek {
                table_uuids: table_uuids.to_vec(),
            }
        } else if let Some(uuids) = index_uuids {
            // row_uuid does not equal index_uuid; derive it canonically.
            SystemSelection::IdentitySeek {
                row_uuids: uuids
                    .iter()
                    .map(|index_uuid| {
                        let index_uuid_str = index_uuid.to_string();
                        naming::row_uuid_for_pk(&sys_indexes_table_uuid, &[index_uuid_str.as_str()])
                    })
                    .collect(),
                filter: filter.map(str::to_string),
            }
        } else {
            SystemSelection::Scan {
                filter: filter.map(str::to_string),
            }
        };

        self.read_system_table(
            driver,
            dl,
            catalog_uuid,
            branch_uuid,
            &sys_indexes_table_uuid,
            &sys_arrow_schema,
            selection,
            snapshot,
        )
        .await
    }

    /// Get a single index by `index_uuid` or by `(table_uuid, index_name)`
    /// (`index_name` is unique only within a table). Returns `None` when
    /// neither selector is supplied or nothing resolves.
    pub async fn meta_get_index<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        table_uuid: &str,
        index_uuid: Option<&str>,
        index_name: Option<&str>,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<Index>>
    where
        L: DlDriver + ?Sized,
    {
        let resolved_branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;

        // UUIDs are parsed up front so a malformed value returns a typed error.
        let (index_uuids, owning_table, index_names) = if let Some(uuid_str) = index_uuid {
            let uuid = parse_meta_uuid(uuid_str, "index_uuid")?;
            (Some(vec![uuid]), None, None)
        } else if let Some(name) = index_name {
            let table = parse_meta_uuid(table_uuid, "table_uuid")?;
            (None, Some(table), Some(vec![name.to_string()]))
        } else {
            return Ok(None);
        };

        let batches = self
            .resolve_index_metadata(
                driver,
                dl,
                catalog_uuid,
                &resolved_branch,
                None,
                index_uuids.as_deref(),
                owning_table.as_ref().zip(index_names.as_deref()),
                None,
                snapshot,
            )
            .await?;

        for batch in &batches {
            if batch.num_rows() > 0 {
                return Ok(Some(index_from_record_batch(batch, 0)));
            }
        }
        Ok(None)
    }

    /// List every index on a table (branch-scoped). Filters
    /// `l.table_uuid = '<uuid>'`.
    pub async fn meta_list_indexes<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        table_uuid: &str,
        branch_uuid: Option<&str>,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<Index>>
    where
        L: DlDriver + ?Sized,
    {
        let resolved_branch = resolve_branch(driver, catalog_uuid, branch_uuid).await?;
        self.resolve_table_indexes(
            driver,
            dl,
            catalog_uuid,
            &resolved_branch,
            table_uuid,
            snapshot,
        )
        .await
    }

    /// Read a table's DEFINED indexes (`__penca_system__.indexes`) for an
    /// ALREADY-resolved branch — the shared body behind
    /// [`Self::meta_list_indexes`] and the `Table.indexes` population on
    /// GetTable/ListTables. MUST route through
    /// [`Self::resolve_index_metadata`] so the read rides the shared
    /// W_snap-keyed snapshot-list cache in the same upfront metadata phase,
    /// never a per-seek miss path.
    async fn resolve_table_indexes<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &str,
        resolved_branch: &str,
        table_uuid: &str,
        snapshot: &ReadSnapshot,
    ) -> Result<Vec<Index>>
    where
        L: DlDriver + ?Sized,
    {
        let table = parse_meta_uuid(table_uuid, "table_uuid")?;
        let row_filter = format!("l.table_uuid = '{table}'");
        let batches = self
            .resolve_index_metadata(
                driver,
                dl,
                catalog_uuid,
                resolved_branch,
                Some(&row_filter),
                // List by table_uuid FK — no single row_uuid.
                None,
                None,
                None,
                snapshot,
            )
            .await?;
        let mut out = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                out.push(index_from_record_batch(batch, i));
            }
        }
        Ok(out)
    }

    /// Get the Arrow schema bytes for a table on a branch by table_uuid
    /// (no schema_uuid required). Used by branch-coordinated persist which
    /// only knows `(catalog, branch, table_uuid)`. Routes through
    /// `stream_merged` so the lookup tolerates the post-persist state where
    /// the row lives in cold + hot commit_tx_log has been purged.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            table = %table_uuid,
            as_of_micros = ?snapshot.plan_as_of_micros(),
        ),
    )]
    pub async fn get_table_arrow_schema_by_branch<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<Vec<u8>>>
    where
        L: DlDriver + ?Sized,
    {
        // The identity restriction below is an exact selection, so a
        // snapshot-covered default read serves it from the direct seek.
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        let sys_arrow_schema: SchemaRef = Arc::new(PgDialect::system_tables_arrow_schema());
        let batches = self
            .read_system_table(
                driver,
                dl,
                &catalog_uuid.to_string(),
                &branch_uuid.to_string(),
                &sys_tables_table_uuid,
                &sys_arrow_schema,
                SystemSelection::IdentitySeek {
                    // row_uuid does not equal the raw table_uuid.
                    row_uuids: vec![naming::row_uuid_for_pk(
                        &sys_tables_table_uuid,
                        &[table_uuid.to_string().as_str()],
                    )],
                    filter: None,
                },
                snapshot,
            )
            .await?;
        Ok(convert::extract_first_binary(&batches, "arrow_schema"))
    }

    /// Like [`Self::get_table_arrow_schema_by_branch`] but returns both the
    /// arrow_schema bytes and the table's primary_keys in one round-trip.
    /// Persist/compact paths need both to construct the widened delete_log
    /// schema.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            branch = %branch_uuid,
            table = %table_uuid,
            as_of_micros = ?snapshot.plan_as_of_micros(),
        ),
    )]
    pub async fn get_table_metadata_by_branch<L>(
        &self,
        driver: &impl DbDriver<Row = PgRow>,
        dl: &L,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
        table_uuid: &Uuid,
        snapshot: &ReadSnapshot,
    ) -> Result<Option<(Vec<u8>, Vec<String>)>>
    where
        L: DlDriver + ?Sized,
    {
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        let sys_arrow_schema: SchemaRef = Arc::new(PgDialect::system_tables_arrow_schema());
        let batches = self
            .read_system_table(
                driver,
                dl,
                &catalog_uuid.to_string(),
                &branch_uuid.to_string(),
                &sys_tables_table_uuid,
                &sys_arrow_schema,
                SystemSelection::IdentitySeek {
                    // row_uuid does not equal the raw table_uuid.
                    row_uuids: vec![naming::row_uuid_for_pk(
                        &sys_tables_table_uuid,
                        &[table_uuid.to_string().as_str()],
                    )],
                    filter: None,
                },
                snapshot,
            )
            .await?;
        let Some(arrow_schema_bytes) = convert::extract_first_binary(&batches, "arrow_schema")
        else {
            return Ok(None);
        };
        let primary_keys = convert::extract_first_string_list(&batches, "primary_keys");
        Ok(Some((arrow_schema_bytes, primary_keys)))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use futures_util::Stream;
    use penca_db::driver::{DbDriver, SqlValue};
    use penca_merge::ReadSnapshot;
    use sqlx::postgres::PgRow;

    use super::{QueryManager, table_uuid_in_filter};

    // Pins the exact residual shape: quoting, comma-join, `l.` alias.
    #[test]
    fn table_uuid_in_filter_shape() {
        assert_eq!(
            table_uuid_in_filter(&["a".to_string(), "b".to_string()]),
            "l.table_uuid IN ('a','b')"
        );
        // Single element: no trailing comma.
        assert_eq!(
            table_uuid_in_filter(&["a".to_string()]),
            "l.table_uuid IN ('a')"
        );
    }

    // No-op driver. The no-tx / no-as_of and explicit-as_of resolution paths
    // never query the database; the open-tx path does, and an empty result set
    // is exactly the "tx absent from begin_tx_log" case.
    struct NoopDriver;

    impl DbDriver for NoopDriver {
        type Row = PgRow;

        async fn execute(&self, _query: &str) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }
        async fn execute_no_result(&self, _query: &str) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn execute_many(&self, _queries: &[String]) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn execute_params(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<Vec<PgRow>, sqlx::Error> {
            Ok(vec![])
        }
        async fn execute_no_result_params(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<(), sqlx::Error> {
            Ok(())
        }
        async fn fetch_optional(
            &self,
            _query: &str,
            _params: &[SqlValue],
        ) -> Result<Option<PgRow>, sqlx::Error> {
            Ok(None)
        }
        async fn close(&self) {}
        fn fetch_stream<'a>(
            &'a self,
            _query: &'a str,
            _params: &'a [SqlValue],
        ) -> Pin<Box<dyn Stream<Item = Result<PgRow, sqlx::Error>> + Send + 'a>> {
            Box::pin(futures_util::stream::empty())
        }
    }

    // The no-tx / no-as_of default must pin the per-branch seq frontier, so
    // identifier resolution and the data read share one seq snapshot.
    #[tokio::test]
    async fn resolve_read_snapshot_no_tx_no_as_of_pins_seq_frontier() {
        let catalog = uuid::Uuid::nil().to_string();
        let branch = uuid::Uuid::nil().to_string();
        let snapshot = QueryManager::resolve_read_snapshot(
            &NoopDriver,
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*as_of_micros=*/ None,
            /*as_of_seq=*/ None,
            /*default_frontier=*/ Some(7_654_321),
        )
        .await
        .expect("resolution must not error on the no-tx / no-as_of path");

        // A threaded default_frontier pins exactly that seq (so a hardcoded
        // constant could not satisfy this), and the default "read latest" pin
        // is `LatestSeq` — the one cache-eligible shape — rather than an
        // explicit `AsOfSeq` time-travel.
        assert_eq!(snapshot, ReadSnapshot::LatestSeq(7_654_321));
    }

    // A supplied `open_tx_uuid` with no `begin_tx_log` row must ERROR, not fall
    // through to `LatestSeq`. `None` is the only legitimate fall-through — that
    // means "no tx supplied", covered by the tests either side of this one.
    //
    // This pins the row-absent inputs only: never begun, wrong branch, or ledger
    // already GC'd. An aborted or expired tx does NOT reach here — its
    // `begin_tx_log` row survives until Purge's grace window drops it
    // (`purge_tx_log.rs`), so the bare `SELECT began_at_seq_num` still returns
    // `Some` and resolution yields a live `OpenTx`. That arm is the more damaging
    // one and it is pinned at the integration level instead
    // (`test_read_with_aborted_tx_raises_failed_precondition`), because a stub
    // driver cannot return a row here: sqlx `PgRow` is not constructible in a
    // unit test — see `seq_frontier_from_counter` above.
    #[tokio::test]
    async fn resolve_read_snapshot_dead_open_tx_is_an_error() {
        let catalog = uuid::Uuid::nil().to_string();
        let branch = uuid::Uuid::nil().to_string();
        let result = QueryManager::resolve_read_snapshot(
            &NoopDriver,
            &catalog,
            &branch,
            /*open_tx_uuid=*/ Some("11111111-1111-1111-1111-111111111111"),
            /*as_of_micros=*/ None,
            /*as_of_seq=*/ None,
            /*default_frontier=*/ None,
        )
        .await;

        assert!(
            result.is_err(),
            "a supplied-but-not-open open_tx_uuid must error, got {:?}",
            result.ok()
        );
    }

    // Regression guard: explicit `as_of_micros` resolves verbatim.
    #[tokio::test]
    async fn resolve_read_snapshot_explicit_as_of_unchanged() {
        let catalog = uuid::Uuid::nil().to_string();
        let branch = uuid::Uuid::nil().to_string();
        let snapshot = QueryManager::resolve_read_snapshot(
            &NoopDriver,
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*as_of_micros=*/ Some(9_876_543),
            /*as_of_seq=*/ None,
            /*default_frontier=*/ None,
        )
        .await
        .expect("explicit as_of must resolve");

        assert_eq!(snapshot, ReadSnapshot::AsOfMicros(9_876_543));
    }

    // The frontier is the counter (next-to-allocate) minus one, so the first
    // commit (counter 0 → 1) has frontier 0 — consistent with the OpenTx
    // `commit_seq_num < began_at_seq_num` rule.
    #[test]
    fn seq_frontier_from_counter_maps_counter_minus_one() {
        assert_eq!(super::seq_frontier_from_counter(Some(5)), 4);
        // First commit: counter is 1, last committed serial is 0.
        assert_eq!(super::seq_frontier_from_counter(Some(1)), 0);
    }

    // The genesis edge: a fresh branch (counter `seq_num = 0`) or an absent
    // counter row both map to SNAPSHOT_SEQ_GENESIS (-1) — AsOfSeq(-1) sees
    // nothing committed.
    #[test]
    fn seq_frontier_from_counter_genesis_is_minus_one() {
        assert_eq!(super::seq_frontier_from_counter(Some(0)), -1);
        assert_eq!(super::seq_frontier_from_counter(None), -1);
    }

    // An explicit `as_of_seq` resolves identifiers on the seq axis verbatim —
    // the metadata side of seq-uniform time travel.
    #[tokio::test]
    async fn resolve_read_snapshot_explicit_as_of_seq_maps_to_as_of_seq() {
        let catalog = uuid::Uuid::nil().to_string();
        let branch = uuid::Uuid::nil().to_string();
        let snapshot = QueryManager::resolve_read_snapshot(
            &NoopDriver,
            &catalog,
            &branch,
            /*open_tx_uuid=*/ None,
            /*as_of_micros=*/ None,
            /*as_of_seq=*/ Some(55),
            /*default_frontier=*/ None,
        )
        .await
        .expect("explicit as_of_seq must resolve");

        assert_eq!(snapshot, ReadSnapshot::AsOfSeq(55));
    }

    #[test]
    fn resolve_name_seek_schemas_single_column_escapes_quotes() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_schemas_table_uuid(&catalog);
        let resolved = super::resolve_name_seek(&catalog, &table, vec![vec!["s'1".to_string()]])
            .expect("schemas carry a built-in name index");
        assert_eq!(
            resolved.seek.index_uuid,
            Some(penca_core::naming::system_name_index_uuid(&table)),
            "read side must recompute the classifier's deterministic index_uuid"
        );
        assert_eq!(resolved.seek.tuples, vec![vec!["s'1".to_string()]]);
        assert_eq!(resolved.residual, "(l.schema_name = 's''1')");
    }

    #[test]
    fn resolve_name_seek_tables_composite_multi_tuple_or_shape() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_tables_table_uuid(&catalog);
        let resolved = super::resolve_name_seek(
            &catalog,
            &table,
            vec![
                vec!["su".to_string(), "t1".to_string()],
                vec!["su".to_string(), "t2".to_string()],
            ],
        )
        .expect("tables carry a built-in name index");
        assert_eq!(
            resolved.residual,
            "(l.schema_uuid = 'su' AND l.table_name = 't1') OR \
             (l.schema_uuid = 'su' AND l.table_name = 't2')"
        );
    }

    #[test]
    fn resolve_name_seek_indexes_composite() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_indexes_table_uuid(&catalog);
        let resolved = super::resolve_name_seek(
            &catalog,
            &table,
            vec![vec!["tu".to_string(), "idx".to_string()]],
        )
        .expect("indexes carry a built-in name index");
        assert_eq!(
            resolved.residual,
            "(l.table_uuid = 'tu' AND l.index_name = 'idx')"
        );
    }

    // A NameSeek on anything without a built-in name index (every user table)
    // must fail fast — never degrade to a silent scan.
    #[test]
    fn resolve_name_seek_rejects_non_system_table() {
        let catalog = uuid::Uuid::new_v4();
        let user_table = uuid::Uuid::new_v4();
        let err = super::resolve_name_seek(&catalog, &user_table, vec![vec!["x".to_string()]]);
        assert!(err.is_err(), "user tables carry no built-in name index");
    }

    #[test]
    fn resolve_name_seek_rejects_arity_mismatch() {
        let catalog = uuid::Uuid::new_v4();
        // tables key is (schema_uuid, table_name) — arity 2.
        let table = penca_core::naming::system_tables_table_uuid(&catalog);
        let err = super::resolve_name_seek(&catalog, &table, vec![vec!["only-one".to_string()]]);
        assert!(err.is_err(), "tuple arity must match the spec's key arity");
    }

    // An empty tuple set must fail fast, not yield an empty `residual = ""`
    // that would surface as a confusing WHERE-fragment parse error downstream.
    #[test]
    fn resolve_name_seek_rejects_empty_tuples() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_schemas_table_uuid(&catalog);
        let err = super::resolve_name_seek(&catalog, &table, vec![]);
        assert!(err.is_err(), "empty name-key tuple set must fail fast");
    }

    #[test]
    fn resolve_prefix_seek_indexes_full_key_arity_one_tuples() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_indexes_table_uuid(&catalog);
        let resolved = super::resolve_prefix_seek(
            &catalog,
            &table,
            vec!["tu1".to_string(), "tu2".to_string()],
        )
        .expect("indexes carry a built-in (table_uuid, index_name) name index");
        // Same index_uuid the name seek recomputes — it's the SAME sidecar.
        assert_eq!(
            resolved.seek.index_uuid,
            Some(penca_core::naming::system_name_index_uuid(&table)),
        );
        // FULL composite key columns (so the sidecar decodes at arity 2) ...
        assert_eq!(resolved.seek.key_columns, vec!["table_uuid", "index_name"]);
        // ... but arity-1 probes (leading-prefix on table_uuid).
        assert_eq!(
            resolved.seek.tuples,
            vec![vec!["tu1".to_string()], vec!["tu2".to_string()]],
        );
        // Residual is the filtered scan, reused verbatim.
        assert_eq!(resolved.residual, "l.table_uuid IN ('tu1','tu2')");
    }

    // A prefix probe is only sound when the sidecar leads with table_uuid. The
    // schemas name index leads with schema_name, so a table_uuid prefix would
    // over-select — must fail fast, not silently.
    #[test]
    fn resolve_prefix_seek_rejects_non_table_uuid_leading_column() {
        let catalog = uuid::Uuid::new_v4();
        let schemas = penca_core::naming::system_schemas_table_uuid(&catalog);
        let err = super::resolve_prefix_seek(&catalog, &schemas, vec!["x".to_string()]);
        assert!(
            err.is_err(),
            "schemas name index leads with schema_name, not table_uuid"
        );
    }

    #[test]
    fn resolve_prefix_seek_rejects_non_system_table() {
        let catalog = uuid::Uuid::new_v4();
        let user_table = uuid::Uuid::new_v4();
        let err = super::resolve_prefix_seek(&catalog, &user_table, vec!["tu".to_string()]);
        assert!(err.is_err(), "user tables carry no built-in name index");
    }

    #[test]
    fn resolve_prefix_seek_rejects_empty_table_uuids() {
        let catalog = uuid::Uuid::new_v4();
        let table = penca_core::naming::system_indexes_table_uuid(&catalog);
        let err = super::resolve_prefix_seek(&catalog, &table, vec![]);
        assert!(err.is_err(), "empty table_uuid prefix set must fail fast");
    }
}
