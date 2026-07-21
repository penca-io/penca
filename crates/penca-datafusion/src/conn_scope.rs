//! Connection-scoped identity bundle.
//!
//! The four identity fields `(query_channel, catalog_uuid, catalog_name,
//! branch_uuid)` are set once at session-mint time and threaded down
//! every level of the provider tree (`PencaCatalogProviderList` →
//! `PencaCatalogProvider` → `PencaSchemaProvider` →
//! `PencaTableProvider`). Each provider also holds its own
//! execution-level state (`schema_name`, `table_name`,
//! `arrow_schema`); the bundle here is the config half of that split.
//!
//! `branch_uuid` is rename-stable per CHA-255: every wire payload
//! routes by uuid so an out-of-band `UpdateBranch` doesn't break
//! in-flight queries.
//!
//! `open_tx_uuid` (CHA-345) is the **single source** of the connection's
//! open Penca transaction for the provider tree's read paths. It is the
//! only mutable field: an `Arc`-shared cell flipped on
//! `BEGIN`/`COMMIT`/`ROLLBACK` by `ConnSession` (which holds a clone of
//! the same `Arc`). Both `PencaTableProvider::scan` (RYOW data reads)
//! and `PencaSchemaProvider::{table,table_names,table_exist}` (tx-aware
//! metadata reads, so a table created mid-tx resolves) read it. The
//! `SchemaProvider::table` trait signature has no `&Session`, so the
//! open tx must be reachable from `&self`, which the cell on `ConnScope`
//! provides. See ADR 0010's CHA-345 addendum.

use std::sync::{Arc, RwLock};

use tonic::transport::Channel;

use crate::plan_resolution_memo::{self, PlanResolutionMemoCell, ResolvedTable};

#[derive(Debug, Clone)]
pub struct ConnScope {
    pub query_channel: Channel,
    pub catalog_uuid: String,
    pub catalog_name: String,
    pub branch_uuid: String,
    /// Open Penca transaction (if any) for this connection's read
    /// paths. `Arc`-shared with `ConnSession`, which flips it on
    /// `BEGIN`/`COMMIT`/`ROLLBACK`; cloned (Arc-shared) into every
    /// provider in the tree. Read via [`Self::open_tx_uuid`].
    pub open_tx_cell: Arc<RwLock<Option<String>>>,
    /// CHA-374 / CHA-460: the auto-commit statement's pinned read snapshot as a
    /// `commit_seq_num` frontier (the inclusive max committed seq captured at
    /// GetFlightInfo), or `None` (in-tx, or no statement currently executing).
    /// `Arc`-shared with the owning `ConnSession`, which installs a
    /// `PinnedAsOfSeqGuard` around one statement's GetFlightInfo plan build /
    /// DoGet execute and clears it on drop. Read via [`Self::pinned_as_of_seq`];
    /// mutually exclusive with `open_tx_cell` at the read sites (open tx wins).
    pub as_of_seq_cell: Arc<RwLock<Option<i64>>>,
    /// Per-plan-build metadata-resolution memo (CHA-367). `Arc`-shared with
    /// `ConnSession`, which installs a [`PlanResolutionMemoGuard`] around each
    /// plan build; the provider tree reads/populates it so repeated
    /// `schema()`/`table()` calls within one build collapse to one gRPC each.
    /// `None` (no memo installed) outside a build → live resolution every
    /// call. See [`crate::plan_resolution_memo`].
    ///
    /// [`PlanResolutionMemoGuard`]: crate::PlanResolutionMemoGuard
    pub resolution_memo_cell: PlanResolutionMemoCell,
}

impl ConnScope {
    /// Resolve the connection's open transaction (the `_uuid` value, not
    /// the cell). Returns `None` outside a `BEGIN`/`COMMIT` block. The
    /// read lock is held only for the clone — never across an await.
    pub fn open_tx_uuid(&self) -> Option<String> {
        self.open_tx_cell.read().unwrap().clone()
    }

    /// Resolve the connection's pinned auto-commit read snapshot — the
    /// `commit_seq_num` frontier (CHA-374 / CHA-460) — or `None` when no statement
    /// is pinning (in-tx, or between statements). The read lock is held only
    /// for the copy — never across an await.
    pub fn pinned_as_of_seq(&self) -> Option<i64> {
        *self.as_of_seq_cell.read().unwrap()
    }

    /// CHA-374 / CHA-460: the read-snapshot wire fields for a provider RPC — the
    /// open tx (RYOW) and the pinned auto-commit `as_of_seq` frontier, mutually
    /// exclusive (an open tx carries its own snapshot and wins; the pin is
    /// auto-commit-only). Sent by **both** the metadata-resolution reads
    /// (GetSchema/GetTable/ListSchemas/ListTables during GetFlightInfo planning)
    /// and the data scans, so a statement's planning and execution resolve at
    /// one seq snapshot.
    pub fn read_snapshot_fields(&self) -> (Option<String>, Option<i64>) {
        let open_tx_uuid = self.open_tx_uuid();
        let as_of_seq = if open_tx_uuid.is_some() {
            None
        } else {
            self.pinned_as_of_seq()
        };
        (open_tx_uuid, as_of_seq)
    }

    /// Memoized schema resolution for the in-flight plan build (CHA-367).
    /// `None` = no build active OR `name` not yet resolved this build (the
    /// caller must resolve live and then call [`Self::memo_put_schema`]).
    /// `Some(inner)` = memoized: `inner` is the canonical name (`Some`) or a
    /// confirmed miss (`None`).
    pub(crate) fn memo_get_schema(&self, name: &str) -> Option<Option<String>> {
        plan_resolution_memo::memo_get_schema(&self.resolution_memo_cell, name)
    }

    /// Record a schema resolution in the in-flight build's memo (no-op when no
    /// build is active). `resolved` is the canonical name or a confirmed miss.
    pub(crate) fn memo_put_schema(&self, name: String, resolved: Option<String>) {
        plan_resolution_memo::memo_put_schema(&self.resolution_memo_cell, name, resolved);
    }

    /// Memoized table resolution for the in-flight plan build (CHA-367), keyed
    /// by `(schema, table)`. `None` = no build active OR not yet resolved this
    /// build (resolve live); `Some(inner)` = memoized: `inner` is the table's
    /// resolved metadata (`Some`) or a confirmed miss (`None`).
    pub(crate) fn memo_get_table(
        &self,
        schema: &str,
        table: &str,
    ) -> Option<Option<ResolvedTable>> {
        plan_resolution_memo::memo_get_table(&self.resolution_memo_cell, schema, table)
    }

    /// Record a table resolution in the in-flight build's memo (no-op when no
    /// build is active).
    pub(crate) fn memo_put_table(
        &self,
        schema: String,
        table: String,
        resolved: Option<ResolvedTable>,
    ) {
        plan_resolution_memo::memo_put_table(&self.resolution_memo_cell, schema, table, resolved);
    }
}

/// RAII guard that pins an auto-commit read snapshot (a `commit_seq_num` frontier)
/// on a [`ConnScope`]'s `as_of_seq_cell` for the duration of one statement's
/// GetFlightInfo plan build or DoGet execute, clearing it on drop (CHA-374 /
/// CHA-460). Clearing on drop is what keeps a prior auto-commit statement's pin
/// from leaking into a later one on the same connection. Symmetric to
/// [`PlanResolutionMemoGuard`](crate::PlanResolutionMemoGuard).
#[must_use = "the pin is cleared as soon as the guard drops; bind it for the statement"]
pub struct PinnedAsOfSeqGuard {
    cell: Arc<RwLock<Option<i64>>>,
}

impl PinnedAsOfSeqGuard {
    /// Pin `as_of_seq` (a `commit_seq_num` frontier) on `cell` until the guard
    /// drops. Overwrites any pin already present (should not happen under
    /// per-connection serialisation, but overwriting keeps a new statement from
    /// inheriting a stale pin).
    pub fn install(cell: Arc<RwLock<Option<i64>>>, as_of_seq: i64) -> Self {
        *cell.write().unwrap() = Some(as_of_seq);
        Self { cell }
    }
}

impl Drop for PinnedAsOfSeqGuard {
    fn drop(&mut self) {
        *self.cell.write().unwrap() = None;
    }
}
