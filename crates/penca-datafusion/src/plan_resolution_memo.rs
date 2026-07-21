//! Build-scoped memo for per-plan metadata resolution (CHA-367).
//!
//! While planning **one** SQL statement, DataFusion calls
//! [`CatalogProvider::schema`](datafusion::catalog::CatalogProvider::schema)
//! and [`SchemaProvider::table`](datafusion::catalog::SchemaProvider::table)
//! (plus `table_exist`) repeatedly for the same identifier. Each call would
//! otherwise be a fresh live gRPC (`get_schema` / `get_table`) because the
//! provider tree resolves live per call (CHA-255 deleted the old TTL cache so
//! mid-session / mid-transaction DDL stays visible). This memo collapses those
//! repeated identical resolutions **within one plan build** to one gRPC each.
//!
//! ## Why a build-scoped memo is safe where a connection cache is not
//!
//! The hazard a connection- or snapshot-scoped cache hits is DDL visibility:
//! a non-tx read pins a fresh `as_of` each request (CHA-86), and inside a tx
//! RYOW means `CREATE TABLE t; SELECT * FROM t` must see `t` even though both
//! statements share one snapshot key (CHA-345). A memo that outlived a single
//! plan build could serve a stale "not found" to a later statement.
//!
//! This memo never outlives one plan build: a [`PlanResolutionMemoGuard`]
//! installs a fresh empty memo into the [`ConnScope`](crate::ConnScope) cell at
//! the top of a build and clears it on drop. Within one build the snapshot is
//! fixed and no DDL is issued mid-flight, so every resolution of a given
//! identifier is provably identical — collapsing them carries zero
//! DDL-visibility risk. The memo deliberately does **not** key on the open
//! `tx_uuid`: a single build is at one logical point, and the guard's lifetime
//! (not the key) is what bounds staleness.
//!
//! ## Concurrency
//!
//! The cell is `Arc`-shared across the connection's HTTP/2 streams (like
//! [`ConnScope::open_tx_cell`](crate::ConnScope)). Flight SQL clients serialise
//! statement execution per connection in practice (ADBC/JDBC issue one
//! statement's RPCs before the next), so the guard installs and clears exactly
//! one memo per build with no real concurrent-build overlap. Were two builds on
//! one connection ever to overlap, they would share a memo at the same
//! snapshot — still correct within a build's non-DDL window; the guard scoping
//! is the structural bound, the same assumption `open_tx_cell` already relies
//! on.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use arrow::datatypes::SchemaRef;

/// A cell holding the memo for the in-flight plan build, or `None` when no
/// build is active. `Arc`-shared between the owning `ConnSession`
/// (penca-sql-server, which installs the guard) and the per-conn provider
/// tree's [`ConnScope`](crate::ConnScope) (which reads/populates it).
pub type PlanResolutionMemoCell = Arc<RwLock<Option<PlanResolutionMemo>>>;

/// One table's defined secondary index (CHA-492), threaded from
/// `Table.indexes` so scan can pack a structured `ReadDataRequest.indexes`
/// seek for a fully-covering equality predicate.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedIndex {
    pub(crate) index_uuid: String,
    pub(crate) index_name: String,
    /// Key columns in declared (apply) order.
    pub(crate) key_columns: Arc<[String]>,
}

/// One table's resolved metadata — exactly what
/// `PencaTableProvider::new` needs, so a memo hit rebuilds the provider
/// with no gRPC and no IPC re-parse.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedTable {
    /// Decoded `Table.arrow_schema` IPC bytes.
    pub(crate) arrow_schema: SchemaRef,
    /// Declared primary-key column names, in declared order
    /// (`Table.primary_keys`) — drives the scan-time `ids` PK batch
    /// (CHA-426).
    pub(crate) primary_keys: Arc<[String]>,
    /// Defined secondary indexes (`Table.indexes`, CHA-492) — drives the
    /// scan-time structured `indexes` seek packing (IMPL-S2).
    pub(crate) indexes: Arc<[ResolvedIndex]>,
}

/// Per-plan-build memo of metadata resolutions. Keyed by identifier; an entry's
/// value is `Some(_)` when the identifier resolved and `None` when the lookup
/// confirmed a miss (so a repeat lookup in the same build doesn't re-issue the
/// gRPC to re-learn "not found").
#[derive(Debug, Default)]
pub struct PlanResolutionMemo {
    /// `schema_name` → canonical schema name (`None` = confirmed not found).
    schemas: HashMap<String, Option<String>>,
    /// `(schema_name, table_name)` → the table's resolved metadata
    /// ([`ResolvedTable`]: Arrow schema + declared primary keys; `None` =
    /// confirmed not found).
    tables: HashMap<(String, String), Option<ResolvedTable>>,
}

impl PlanResolutionMemo {
    /// Memoized schema resolution, or `None` if this build hasn't resolved
    /// `name` yet. The inner `Option<String>` is the canonical name (`Some`) or
    /// a confirmed miss (`None`).
    fn get_schema(&self, name: &str) -> Option<Option<String>> {
        self.schemas.get(name).cloned()
    }

    fn put_schema(&mut self, name: String, resolved: Option<String>) {
        self.schemas.insert(name, resolved);
    }

    /// Memoized table resolution, or `None` if this build hasn't resolved
    /// `(schema, table)` yet. The inner `Option<ResolvedTable>` is the
    /// table's resolved metadata (`Some`) or a confirmed miss (`None`).
    fn get_table(&self, schema: &str, table: &str) -> Option<Option<ResolvedTable>> {
        self.tables
            .get(&(schema.to_string(), table.to_string()))
            .cloned()
    }

    fn put_table(&mut self, schema: String, table: String, resolved: Option<ResolvedTable>) {
        self.tables.insert((schema, table), resolved);
    }
}

/// Read/populate helpers over a [`PlanResolutionMemoCell`]. Each call takes the
/// lock only for the map access — never across the await of a fallback gRPC, so
/// the provider issues its live resolution outside the lock and stores the
/// result afterward.
pub(crate) fn memo_get_schema(cell: &PlanResolutionMemoCell, name: &str) -> Option<Option<String>> {
    cell.read()
        .unwrap()
        .as_ref()
        .and_then(|memo| memo.get_schema(name))
}

pub(crate) fn memo_put_schema(
    cell: &PlanResolutionMemoCell,
    name: String,
    resolved: Option<String>,
) {
    if let Some(memo) = cell.write().unwrap().as_mut() {
        memo.put_schema(name, resolved);
    }
}

pub(crate) fn memo_get_table(
    cell: &PlanResolutionMemoCell,
    schema: &str,
    table: &str,
) -> Option<Option<ResolvedTable>> {
    cell.read()
        .unwrap()
        .as_ref()
        .and_then(|memo| memo.get_table(schema, table))
}

pub(crate) fn memo_put_table(
    cell: &PlanResolutionMemoCell,
    schema: String,
    table: String,
    resolved: Option<ResolvedTable>,
) {
    if let Some(memo) = cell.write().unwrap().as_mut() {
        memo.put_table(schema, table, resolved);
    }
}

/// RAII guard scoping a [`PlanResolutionMemo`] to one plan build. Installs a
/// fresh empty memo into the cell on construction and clears it (back to
/// `None`) on drop, so resolutions never leak across statements. See the module
/// docs for why build-scoping is the correctness bound.
#[must_use = "the memo is cleared as soon as the guard is dropped; bind it for the plan build"]
pub struct PlanResolutionMemoGuard {
    cell: PlanResolutionMemoCell,
}

impl PlanResolutionMemoGuard {
    /// Install a fresh empty memo into `cell` for the duration of one plan
    /// build. Overwrites any memo already present (a prior build that did not
    /// clear — should not happen under per-connection serialisation, but
    /// overwriting keeps the new build from inheriting stale entries).
    pub fn install(cell: PlanResolutionMemoCell) -> Self {
        *cell.write().unwrap() = Some(PlanResolutionMemo::default());
        Self { cell }
    }
}

impl Drop for PlanResolutionMemoGuard {
    fn drop(&mut self) {
        *self.cell.write().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    fn cell() -> PlanResolutionMemoCell {
        Arc::new(RwLock::new(None))
    }

    fn a_resolved_table() -> ResolvedTable {
        ResolvedTable {
            arrow_schema: Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)])),
            primary_keys: vec!["c".to_string()].into(),
            indexes: Vec::new().into(),
        }
    }

    #[test]
    fn without_a_guard_lookups_miss_and_stores_are_noops() {
        // No memo installed: every get returns `None` (resolve live) and puts
        // are dropped, so resolution stays live outside a plan build.
        let cell = cell();
        memo_put_schema(&cell, "public".into(), Some("public".into()));
        assert_eq!(memo_get_schema(&cell, "public"), None);
    }

    #[test]
    fn guard_installs_a_memo_that_caches_within_the_build() {
        let cell = cell();
        let _guard = PlanResolutionMemoGuard::install(cell.clone());

        // Miss until resolved.
        assert_eq!(memo_get_schema(&cell, "public"), None);
        memo_put_schema(&cell, "public".into(), Some("public".into()));
        assert_eq!(
            memo_get_schema(&cell, "public"),
            Some(Some("public".into()))
        );

        // A confirmed miss is cached distinctly from "not yet resolved".
        memo_put_schema(&cell, "ghost".into(), None);
        assert_eq!(memo_get_schema(&cell, "ghost"), Some(None));

        memo_put_table(&cell, "public".into(), "t".into(), Some(a_resolved_table()));
        assert!(matches!(
            memo_get_table(&cell, "public", "t"),
            Some(Some(_))
        ));
        // A confirmed miss is cached distinctly from "not yet resolved".
        memo_put_table(&cell, "public".into(), "gone".into(), None);
        assert_eq!(memo_get_table(&cell, "public", "gone"), Some(None));
        // Not resolved this build → no entry.
        assert!(memo_get_table(&cell, "public", "missing").is_none());
    }

    #[test]
    fn dropping_the_guard_clears_the_memo() {
        let cell = cell();
        {
            let _guard = PlanResolutionMemoGuard::install(cell.clone());
            memo_put_schema(&cell, "public".into(), Some("public".into()));
            assert_eq!(
                memo_get_schema(&cell, "public"),
                Some(Some("public".into()))
            );
        }
        // Guard dropped → memo cleared → next build starts empty, so a stale
        // entry can never serve a later statement.
        assert_eq!(memo_get_schema(&cell, "public"), None);
    }

    #[test]
    fn reinstalling_starts_from_empty() {
        let cell = cell();
        {
            let _g = PlanResolutionMemoGuard::install(cell.clone());
            memo_put_schema(&cell, "public".into(), Some("public".into()));
        }
        let _g = PlanResolutionMemoGuard::install(cell.clone());
        assert_eq!(memo_get_schema(&cell, "public"), None);
    }
}
