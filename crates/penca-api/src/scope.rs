//! Resolved-identifier scope shared by read and write data RPCs.
//!
//! [`ResolvedScope`] bundles the `(catalog_uuid, branch_uuid,
//! schema_uuid, table_uuid, snapshot)` clump that opens every
//! `read_data` / `write_data` handler — one demand-driven
//! resolve+validate pass shared verbatim by both paths (CHA-475).
//! Reads route through `QueryManager`'s cached resolver (CHA-472).
//! Two constructors:
//!
//! - [`ResolvedScope::resolve_schema`] for handlers that need at
//!   most a schema (`get_schema`, `list_schemas`, `list_tables`).
//!   `list_schemas` carries no schema identifier; its `schema_uuid`
//!   stays `None`.
//! - [`ResolvedScope::resolve_table`] for handlers that need both
//!   schema and table (`get_table`, `read_data`, `plan_audit`).
//!
//! The snapshot derivation reads from `open_tx_uuid` + `as_of_micros` +
//! `as_of_seq` (CHA-443); `WriteDataRequest` maps its write-side `tx_uuid`
//! onto the `open_tx_uuid` arm (RYOW), and the trait gives `AuditDataRequest`
//! a place to substitute `committed_at.max_micros` for `as_of_micros`
//! (CHA-236). Catalog + branch resolve snapshot-blind
//! (never time-traveled); identifier resolution pins on whichever axis the
//! request time-traveled (micros / seq), defaulting to the per-branch seq
//! frontier — the same axis as the data read.

use crate::query::QueryManager;
use penca_db::driver::DbDriver;
use penca_dl::driver::DlDriver;
use penca_merge::ReadSnapshot;
use penca_proto::external::v1::{
    AuditDataRequest, CreateIndexRequest, CreateSchemaRequest, CreateTableRequest,
    DeleteIndexRequest, DeleteSchemaRequest, DeleteTableRequest, GetIndexRequest, GetSchemaRequest,
    GetTableRequest, IntegerRange, ListIndexesRequest, ListSchemasRequest, ListTablesRequest,
    ReadDataRequest, Schema, Table, UpdateIndexRequest, UpdateSchemaRequest, UpdateTableRequest,
    WriteDataRequest, audit_data_request, read_data_request,
};
use sqlx::postgres::PgRow;
use uuid::Uuid;

use crate::error::ApiError;
use crate::resolve::{parse_resolved_uuid, resolve_branch, resolve_catalog};

/// Identifier + snapshot bundle shared by every Query handler that
/// reads through `__penca_system__`.
///
/// `schema_uuid` is `None` only for `list_schemas` (the request carries
/// no schema identifier); the two get/list-tables handlers populate it.
/// `table_uuid` is `Some` only for the `resolve_table` flow.
pub(crate) struct ResolvedScope {
    pub(crate) catalog_uuid: Uuid,
    pub(crate) branch_uuid: Uuid,
    pub(crate) schema_uuid: Option<Uuid>,
    /// The `__penca_system__.schemas` row captured during schema
    /// resolution. `resolve_schema` populates it on both identifier paths,
    /// so the `get_schema` handler reuses it directly (CHA-365 + CHA-381).
    /// `None` only when no schema row is resolved — the `list_schemas` flow
    /// and the by-uuid `resolve_table` flow (which derives the schema from
    /// the resolved table row instead).
    pub(crate) schema_row: Option<Schema>,
    /// The `__penca_system__.tables` row captured during table resolution.
    /// `resolve_table` always populates it now (CHA-381): the by-uuid path
    /// reads it catalog-wide, the by-name path schema-scoped. `read_data`
    /// reuses its Arrow schema instead of a second identical merge (CHA-352)
    /// and the `get_table` handler reuses the whole row (CHA-365). `None`
    /// only on the `resolve_schema` flow (`get_schema` / `list_schemas` /
    /// `list_tables`), which resolves no table.
    pub(crate) table_row: Option<Table>,
    pub(crate) snapshot: ReadSnapshot,
}

/// Field-accessor trait letting one constructor accept multiple
/// proto request types that share the standard identifier block.
///
/// `open_tx_uuid` / `as_of_micros` are the read-side snapshot inputs
/// — `AuditDataRequest` overrides `as_of_micros` to read from
/// `committed_at.max_micros` (CHA-236) and leaves `open_tx_uuid` at
/// the default `None`.
pub(crate) trait RequestIdents {
    fn catalog_uuid(&self) -> Option<&str>;
    fn catalog_name(&self) -> Option<&str>;
    fn branch_uuid(&self) -> Option<&str>;
    fn branch_name(&self) -> Option<&str>;
    fn open_tx_uuid(&self) -> Option<&str> {
        None
    }
    fn as_of_micros(&self) -> Option<i64> {
        None
    }
    /// CHA-443 (IMPL-6) / CHA-460: the seq-axis time-travel arm. `Some(N)` makes
    /// identifier resolution pin `AsOfSeq(N)` — the same axis as a seq data
    /// read — so a renamed table resolves at its historical name.
    /// `ReadDataRequest` carries it via its `as_of` oneof; the metadata read
    /// requests (`GetSchema`/`ListSchemas`/`GetTable`/`ListTables`) via their
    /// plain `as_of_seq` field (CHA-460, the seq sibling of `as_of_micros`). The
    /// index reads and `AuditData` have no seq consumer and take the `None`
    /// default.
    fn as_of_seq(&self) -> Option<i64> {
        None
    }
    fn schema_uuid(&self) -> Option<&str> {
        None
    }
    fn schema_name(&self) -> Option<&str> {
        None
    }
    fn table_uuid(&self) -> Option<&str> {
        None
    }
    fn table_name(&self) -> Option<&str> {
        None
    }
}

macro_rules! impl_catalog_branch_idents {
    () => {
        fn catalog_uuid(&self) -> Option<&str> {
            self.catalog_uuid.as_deref()
        }
        fn catalog_name(&self) -> Option<&str> {
            self.catalog_name.as_deref()
        }
        fn branch_uuid(&self) -> Option<&str> {
            self.branch_uuid.as_deref()
        }
        fn branch_name(&self) -> Option<&str> {
            self.branch_name.as_deref()
        }
    };
}

macro_rules! impl_snapshot_idents {
    () => {
        fn open_tx_uuid(&self) -> Option<&str> {
            self.open_tx_uuid.as_deref()
        }
        fn as_of_micros(&self) -> Option<i64> {
            self.as_of_micros
        }
    };
}

// CHA-460: the snapshot-ident accessors plus the `as_of_seq` arm, for the
// metadata read requests that carry a seq axis (GetSchema / ListSchemas /
// GetTable / ListTables). Index reads stay on `impl_snapshot_idents!` (micros
// only) — no consumer pins them on seq.
macro_rules! impl_snapshot_idents_with_seq {
    () => {
        impl_snapshot_idents!();
        fn as_of_seq(&self) -> Option<i64> {
            self.as_of_seq
        }
    };
}

macro_rules! impl_schema_idents {
    () => {
        fn schema_uuid(&self) -> Option<&str> {
            self.schema_uuid.as_deref()
        }
        fn schema_name(&self) -> Option<&str> {
            self.schema_name.as_deref()
        }
    };
}

macro_rules! impl_table_idents {
    () => {
        fn table_uuid(&self) -> Option<&str> {
            self.table_uuid.as_deref()
        }
        fn table_name(&self) -> Option<&str> {
            self.table_name.as_deref()
        }
    };
}

// CHA-479: the data + DDL write requests map their write-side `tx_uuid` onto the
// `open_tx_uuid` (RYOW) arm so name resolution sees the open tx's own uncommitted
// writes. Writes never wall-clock/seq time-travel, so the `as_of_*` arms stay at
// the trait default `None`.
macro_rules! impl_write_tx_idents {
    () => {
        fn open_tx_uuid(&self) -> Option<&str> {
            self.tx_uuid.as_deref()
        }
    };
}

impl RequestIdents for GetSchemaRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents_with_seq!();
    impl_schema_idents!();
}

impl RequestIdents for ListSchemasRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents_with_seq!();
}

impl RequestIdents for GetTableRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents_with_seq!();
    impl_schema_idents!();
    impl_table_idents!();
}

impl RequestIdents for ListTablesRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents_with_seq!();
    impl_schema_idents!();
}

// CHA-455: index reads resolve the owning table (schema + table idents)
// and time-travel via the same open_tx_uuid / as_of_micros pin pair.
impl RequestIdents for GetIndexRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents!();
    impl_schema_idents!();
    impl_table_idents!();
}

impl RequestIdents for ListIndexesRequest {
    impl_catalog_branch_idents!();
    impl_snapshot_idents!();
    impl_schema_idents!();
    impl_table_idents!();
}

impl RequestIdents for ReadDataRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    fn open_tx_uuid(&self) -> Option<&str> {
        self.open_tx_uuid.as_deref()
    }
    // CHA-429 / CHA-443: the `as_of` oneof carries either commit axis.
    // Identifier resolution pins on the SAME axis as the data read — the
    // micros arm here, the seq arm via `as_of_seq` below — so a renamed
    // table resolves at its historical name on whichever axis the caller
    // time-traveled. The two arms are mutually exclusive (oneof), so at
    // most one is `Some`.
    fn as_of_micros(&self) -> Option<i64> {
        read_data_as_of_axes(&self.as_of).0
    }
    // CHA-443 (IMPL-6): the seq arm of the `as_of` oneof drives seq-axis
    // identifier resolution (`AsOfSeq(N)`), matching the seq data pin.
    fn as_of_seq(&self) -> Option<i64> {
        read_data_as_of_axes(&self.as_of).1
    }
}

impl RequestIdents for AuditDataRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    // CHA-236: AuditData reuses the `committed` window's micros upper
    // bound as the `as_of_micros` snapshot for name resolution so a
    // renamed table resolves at its historical name across the audit
    // window. Honors only the `commit_micros` arm; a seq-axis window
    // resolves names at the default snapshot (no seq→micros resolution).
    // Falls back to a `pg_now`-pinned `AsOfMicros` (via the resolver's
    // self-capture) when no upper bound is set. AuditDataRequest carries
    // no `open_tx_uuid`; the default `None` applies.
    fn as_of_micros(&self) -> Option<i64> {
        audit_committed_axes(&self.committed).0.and_then(|r| r.max)
    }
}

// CHA-475: the write data path resolves the same `(base + target table)` scope
// as `read_data`, through the same cached `QueryManager` resolver (CHA-472). A
// write never wall-clock / seq time-travels, so the as_of arms stay at their
// `None` default; its `tx_uuid` maps onto the `open_tx_uuid` arm so name
// resolution sees the open tx's own (uncommitted) writes — read-your-own-write,
// exactly as the read path's `open_tx_uuid` does.
impl RequestIdents for WriteDataRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

// CHA-479: the DDL write handlers resolve their identifiers through the same
// shared `ResolvedScope` as `read_data` / `write_data` (collapsing the former
// `WriteRequestScope`). Each request exposes the standard identifier surface it
// carries; `tx_uuid` maps onto `open_tx_uuid` (RYOW) via `impl_write_tx_idents!`.
// CreateSchema mints its own uuid, so it carries no schema ident (base-only).
impl RequestIdents for CreateSchemaRequest {
    impl_catalog_branch_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for UpdateSchemaRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for DeleteSchemaRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for CreateTableRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for UpdateTableRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for DeleteTableRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

// CHA-455: index DDL targets a user table (schema + table idents) — same
// resolve_table flow as update_table / delete_table.
impl RequestIdents for CreateIndexRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for UpdateIndexRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

impl RequestIdents for DeleteIndexRequest {
    impl_catalog_branch_idents!();
    impl_schema_idents!();
    impl_table_idents!();
    impl_write_tx_idents!();
}

/// CHA-429: the single canonical decode of the ReadData `as_of` oneof into
/// its two mutually exclusive commit-axis bounds `(commit_micros,
/// commit_seq_num)`. `read_data` reads both arms for the read snapshot;
/// [`RequestIdents::as_of_micros`] for `ReadDataRequest` reads only
/// `.0` for identifier resolution. Routing both through here is what keeps
/// the two arms from drifting (roborev finding on the I3 commit).
pub(crate) fn read_data_as_of_axes(
    as_of: &Option<read_data_request::AsOf>,
) -> (Option<i64>, Option<i64>) {
    match as_of {
        Some(read_data_request::AsOf::CommitMicros(t)) => (Some(*t), None),
        Some(read_data_request::AsOf::CommitSeqNum(n)) => (None, Some(*n)),
        None => (None, None),
    }
}

/// CHA-429: the single canonical decode of the AuditData `committed` oneof
/// into its two mutually exclusive commit-axis windows `(micros_window,
/// seq_window)`. `plan_audit` reads both windows (the micros window feeds
/// `timestamp_bounds`, the seq window the `(seq_from, seq_to)` pair);
/// [`RequestIdents::as_of_micros`] for `AuditDataRequest` reads only
/// the micros window's `.max`.
pub(crate) fn audit_committed_axes(
    committed: &Option<audit_data_request::Committed>,
) -> (Option<&IntegerRange>, Option<&IntegerRange>) {
    match committed {
        Some(audit_data_request::Committed::CommitMicros(r)) => (Some(r), None),
        Some(audit_data_request::Committed::CommitSeqNum(r)) => (None, Some(r)),
        None => (None, None),
    }
}

impl ResolvedScope {
    /// Constructor for `get_schema`, `list_schemas`, `list_tables`.
    ///
    /// `list_schemas` carries no schema identifier — when the request
    /// has neither `schema_uuid` nor `schema_name`, `schema_uuid`
    /// stays `None` instead of erroring on a missing identifier. The
    /// other two callers always pass at least one, so the resolution
    /// runs.
    pub(crate) async fn resolve_schema<D, L, R>(
        query_manager: &QueryManager,
        driver: &D,
        dl_driver: &L,
        request: &R,
        default_frontier: Option<i64>,
    ) -> Result<Self, ApiError>
    where
        D: DbDriver<Row = PgRow>,
        L: DlDriver + ?Sized,
        R: RequestIdents,
    {
        let (catalog_uuid, branch_uuid, snapshot) =
            resolve_base(driver, request, default_frontier).await?;
        let (schema_uuid, schema_row) =
            if request.schema_uuid().is_some() || request.schema_name().is_some() {
                let schema = query_manager
                    .resolve_schema(
                        driver,
                        dl_driver,
                        &catalog_uuid,
                        request.schema_uuid(),
                        request.schema_name(),
                        Some(&branch_uuid.to_string()),
                        &snapshot,
                    )
                    .await?;
                let uuid = parse_resolved_uuid(&schema.schema_uuid, "schema_uuid")?;
                (Some(uuid), Some(schema))
            } else {
                (None, None)
            };
        Ok(Self {
            catalog_uuid,
            branch_uuid,
            schema_uuid,
            schema_row,
            table_row: None,
            snapshot,
        })
    }

    /// Constructor for `get_table`, `read_data`, `plan_audit`.
    /// Resolves schema (parent) then table (target).
    pub(crate) async fn resolve_table<D, L, R>(
        query_manager: &QueryManager,
        driver: &D,
        dl_driver: &L,
        request: &R,
        default_frontier: Option<i64>,
    ) -> Result<Self, ApiError>
    where
        D: DbDriver<Row = PgRow>,
        L: DlDriver + ?Sized,
        R: RequestIdents,
    {
        let (catalog_uuid, branch_uuid, snapshot) =
            resolve_base(driver, request, default_frontier).await?;
        let branch_str = branch_uuid.to_string();

        // CHA-381 (Design X): dispatch on the table identifier.
        // - table_uuid present → catalog-wide resolve; no schema needed, and
        //   the schema is derived from the resolved row (true residency).
        // - else → schema parent is required to scope the name lookup.
        let (schema_uuid, schema_row, table) = if let Some(table_uuid_str) = request.table_uuid() {
            let table = query_manager
                .resolve_table_by_uuid(
                    driver,
                    dl_driver,
                    &catalog_uuid,
                    table_uuid_str,
                    Some(&branch_str),
                    &snapshot,
                )
                .await?;
            let schema_uuid = parse_resolved_uuid(&table.schema_uuid, "schema_uuid")?;
            (schema_uuid, None, table)
        } else {
            let schema = query_manager
                .resolve_schema(
                    driver,
                    dl_driver,
                    &catalog_uuid,
                    request.schema_uuid(),
                    request.schema_name(),
                    Some(&branch_str),
                    &snapshot,
                )
                .await?;
            let schema_uuid = parse_resolved_uuid(&schema.schema_uuid, "schema_uuid")?;
            let table_name = request.table_name().ok_or_else(|| {
                ApiError::InvalidRequest("must provide table_uuid or table_name".into())
            })?;
            let table = query_manager
                .resolve_table_by_name(
                    driver,
                    dl_driver,
                    &catalog_uuid,
                    &schema_uuid,
                    table_name,
                    Some(&branch_str),
                    &snapshot,
                )
                .await?;
            (schema_uuid, Some(schema), table)
        };
        Ok(Self {
            catalog_uuid,
            branch_uuid,
            schema_uuid: Some(schema_uuid),
            schema_row,
            table_row: Some(table),
            snapshot,
        })
    }
}

/// Common `resolve_catalog → resolve_branch → resolve_read_snapshot`
/// prefix shared by both constructors.
async fn resolve_base<D, R>(
    driver: &D,
    request: &R,
    default_frontier: Option<i64>,
) -> Result<(Uuid, Uuid, ReadSnapshot), ApiError>
where
    D: DbDriver<Row = PgRow>,
    R: RequestIdents,
{
    let catalog = resolve_catalog(driver, request.catalog_uuid(), request.catalog_name()).await?;
    let catalog_uuid = parse_resolved_uuid(&catalog.catalog_uuid, "catalog_uuid")?;
    let branch = resolve_branch(
        driver,
        &catalog_uuid,
        request.branch_uuid(),
        request.branch_name(),
    )
    .await?;
    let branch_uuid = parse_resolved_uuid(&branch.branch_uuid, "branch_uuid")?;
    // CHA-443 (IMPL-6): catalog + branch resolve snapshot-blind (they are
    // never time-traveled — see the module header); then the read snapshot
    // pins on the request's axis (micros / seq / open-tx) or, by default, the
    // per-branch seq frontier captured here (or threaded as `default_frontier`).
    let snapshot = QueryManager::resolve_read_snapshot(
        driver,
        &catalog_uuid.to_string(),
        &branch_uuid.to_string(),
        request.open_tx_uuid(),
        request.as_of_micros(),
        request.as_of_seq(),
        default_frontier,
    )
    .await?;
    Ok((catalog_uuid, branch_uuid, snapshot))
}
