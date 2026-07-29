//! Postgres-specific SQL expressions, DDL, and type mapping.
//!
//! Three tiers of DDL:
//! - **Global** (bootstrap): catalog_store, segment metadata
//! - **Per-catalog** (UUID-prefixed): branch_store, tx-log family
//!   (commit_tx_log / begin_tx_log / abort_tx_log), each LIST-partitioned by
//!   branch_uuid
//! - **Per-table** (data_log_prefix-prefixed): upsert_log, delete_log — covers
//!   user data and the system tables under `__penca_system__.{schemas,tables}`
//!   via the same shape
//!
//! Per-catalog tx-log tables use LIST partitioning on branch_uuid so each
//! branch gets its own partition for write isolation and cheap DROP on
//! branch delete.

use arrow::datatypes::{DataType, Schema};
use penca_core::naming::{self, CATALOG_STORE};
use penca_core::types::CanonicalType;
use uuid::Uuid;

use super::{ArrowTypeError, DbDialect, Dialect};
use crate::driver::DbDriver;

/// Postgres-specific SQL expressions, DDL, and type mapping.
pub struct PgDialect;

// Postgres uses SQL-standard double-quoting — `Dialect`'s default impls
// for `quote_identifier` / `quote_column` are correct. Only
// `latest_per_partition` (DISTINCT ON) and `uuid_literal` (the explicit
// `::uuid` cast required for UNION/JOIN type-checking against uuid
// columns) are overridden.
impl Dialect for PgDialect {
    fn latest_per_partition(
        select_cols: &[&str],
        inner_from: &str,
        partition_col: &str,
        order_cols: &[&str],
    ) -> String {
        let cols = select_cols
            .iter()
            .map(|c| Self::quote_identifier(c))
            .collect::<Vec<_>>()
            .join(", ");
        let partition = Self::quote_identifier(partition_col);
        let order = order_cols
            .iter()
            .map(|c| format!("{} DESC", Self::quote_identifier(c)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT DISTINCT ON ({partition}) {cols} \
             FROM {inner_from} \
             ORDER BY {partition}, {order}"
        )
    }

    fn uuid_literal(uuid: &uuid::Uuid) -> String {
        format!("'{uuid}'::uuid")
    }
}

impl DbDialect for PgDialect {
    fn arrow_type_to_sql(arrow_type: &DataType) -> Result<String, ArrowTypeError> {
        // `penca_core::types` owns the supported set: `from_arrow` is the gate,
        // and `pg_column_type` is total over the enum.
        let ct = CanonicalType::from_arrow(arrow_type).map_err(|e| ArrowTypeError(e.0))?;
        Ok(Self::pg_column_type(&ct))
    }

    fn microsecond_epoch() -> &'static str {
        "(EXTRACT(EPOCH FROM now()) * 1000000)::bigint"
    }
}

impl PgDialect {
    /// The Postgres column type for a canonical Arrow type. Deliberately total
    /// over [`CanonicalType`] with no `_` arm, so a new canonical variant is a
    /// compile error here until its PG mapping is named.
    ///
    /// PG has no unsigned integers, so they widen to the next signed type
    /// that holds the full range (`UInt8`→SMALLINT, `UInt16`→INTEGER,
    /// `UInt32`→BIGINT). `UInt64`→NUMERIC: a signed `i64` BIGINT cannot
    /// hold values above `i64::MAX`, so the lossless mapping is
    /// arbitrary-precision NUMERIC.
    fn pg_column_type(ct: &CanonicalType) -> String {
        match ct {
            CanonicalType::Int8 | CanonicalType::Int16 | CanonicalType::UInt8 => "SMALLINT".into(),
            CanonicalType::Int32 | CanonicalType::UInt16 => "INTEGER".into(),
            CanonicalType::Int64 | CanonicalType::UInt32 => "BIGINT".into(),
            CanonicalType::UInt64 => "NUMERIC".into(),
            CanonicalType::Float16 | CanonicalType::Float32 => "REAL".into(),
            CanonicalType::Float64 => "DOUBLE PRECISION".into(),
            CanonicalType::Boolean => "BOOLEAN".into(),
            CanonicalType::Utf8 | CanonicalType::LargeUtf8 | CanonicalType::Utf8View => {
                "TEXT".into()
            }
            CanonicalType::Binary | CanonicalType::LargeBinary | CanonicalType::BinaryView => {
                "BYTEA".into()
            }
            CanonicalType::Decimal128 { precision, scale }
            | CanonicalType::Decimal256 { precision, scale } => {
                format!("NUMERIC({precision}, {scale})")
            }
            CanonicalType::Date32 | CanonicalType::Date64 => "DATE".into(),
            CanonicalType::Time32(_) | CanonicalType::Time64(_) => "TIME".into(),
            CanonicalType::Timestamp { tz: None, .. } => "TIMESTAMP".into(),
            CanonicalType::Timestamp { tz: Some(_), .. } => "TIMESTAMPTZ".into(),
            CanonicalType::List(child)
            | CanonicalType::LargeList(child)
            | CanonicalType::FixedSizeList(child, _) => {
                format!("{}[]", Self::pg_column_type(child))
            }
        }
    }
}

impl PgDialect {
    /// Create global resource tables if they don't already exist.
    ///
    /// Idempotent — safe to call on every startup. `catalog_store` is the only
    /// global table; the persist + snapshot metadata parents are per-catalog
    /// and created in [`Self::create_catalog_tables`].
    #[tracing::instrument(level = "info", skip_all)]
    pub async fn bootstrap(driver: &impl DbDriver) -> Result<(), sqlx::Error> {
        // UNIQUE(catalog_name) is what enforces global catalog-name uniqueness:
        // `catalog_uuid` is randomly minted, so it no longer derives from the
        // name. CHA-239 migrates this to a partial index
        // `WHERE deleted_at_micros IS NULL` once soft-delete lands.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {CATALOG_STORE} (
                    catalog_uuid    UUID PRIMARY KEY,
                    catalog_name    TEXT NOT NULL UNIQUE,
                    catalog_owner   TEXT NOT NULL,
                    description     TEXT DEFAULT ''
                )"#,
            ))
            .await?;
        Ok(())
    }
}

impl PgDialect {
    /// Create per-catalog metadata tables and bootstrap the main branch.
    ///
    /// Creates `branch_store` + the tx-log family (partitioned by branch_uuid),
    /// then bootstraps `__penca_system__.{schemas,tables}` as standard Penca
    /// Tables on the main branch and seeds the four well-known rows with the
    /// catalog's genesis tx.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(catalog = %catalog_uuid, branch = %main_branch_uuid),
    )]
    pub async fn create_catalog_tables(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        catalog_uuid: &Uuid,
        main_branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        Self::create_branch_store_table(driver, catalog_uuid).await?;
        Self::create_tx_log_family_parents(driver, catalog_uuid).await?;
        Self::create_commit_tx_log_seq_num_parent(driver, catalog_uuid).await?;
        Self::create_abort_seq_num_parent(driver, catalog_uuid).await?;
        Self::create_persist_snapshot_metadata_parents(driver, catalog_uuid).await?;
        Self::create_persist_snapshot_metadata_indexes(driver, catalog_uuid).await?;
        Self::ensure_tx_log_branch_partitions(driver, catalog_uuid, main_branch_uuid).await?;
        Self::ensure_metadata_branch_partitions(driver, catalog_uuid, main_branch_uuid).await?;
        Self::seed_genesis_and_system_tables(driver, catalog_uuid, main_branch_uuid).await?;
        Ok(())
    }

    /// Create `branch_store`, the per-catalog branch directory.
    ///
    /// UNIQUE(branch_name) is what enforces per-catalog branch-name uniqueness:
    /// `branch_uuid` is randomly minted, so it no longer derives from the name.
    /// CHA-239 migrates this to a partial index
    /// `WHERE deleted_at_micros IS NULL` once soft-delete lands.
    async fn create_branch_store_table(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let branch_store = naming::branch_store_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                // `parent_branch_uuid` records the fork lineage so the read
                // planner can enumerate the parent's cold tier as a second
                // source, capped at `fork_commit_seq_num`. NULL for `main` and
                // any non-forked branch.
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    branch_uuid          UUID PRIMARY KEY,
                    branch_name          TEXT NOT NULL UNIQUE,
                    fork_commit_seq_num  BIGINT NOT NULL,
                    parent_branch_uuid   UUID
                )"#,
                qi = Self::quote_identifier(&branch_store),
            ))
            .await?;
        Ok(())
    }

    /// Create the four tx-log-family parent tables (begin/tx/abort/
    /// tx_table) plus the unique commit-timestamp index on `commit_tx_log`.
    /// Each parent is LIST-partitioned by `branch_uuid`; the per-branch
    /// leaves are added by [`Self::ensure_tx_log_branch_partitions`].
    async fn create_tx_log_family_parents(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let epoch = Self::microsecond_epoch();
        let begin_tx_log = naming::begin_tx_log_table(catalog_uuid);
        let abort_tx_log = naming::abort_tx_log_table(catalog_uuid);
        let commit_tx_log = naming::commit_tx_log_table(catalog_uuid);
        let tx_table_log = naming::tx_table_log_table(catalog_uuid);

        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    tx_uuid             UUID NOT NULL,
                    branch_uuid         UUID NOT NULL,
                    began_at_micros     BIGINT DEFAULT {epoch},
                    began_at_seq_num    BIGINT NOT NULL,
                    expires_at_micros   BIGINT NOT NULL,
                    comment             TEXT NOT NULL,
                    author              TEXT NOT NULL,
                    PRIMARY KEY (tx_uuid, branch_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&begin_tx_log),
            ))
            .await?;

        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    tx_uuid                 UUID NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    began_at_micros         BIGINT NOT NULL,
                    commit_micros           BIGINT DEFAULT {epoch},
                    comment                 TEXT NOT NULL,
                    author                  TEXT NOT NULL,
                    commit_seq_num          BIGINT NOT NULL,
                    PRIMARY KEY (tx_uuid, branch_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&commit_tx_log),
            ))
            .await?;

        // Append-only ledger of aborted transactions; CommitTx checks this
        // table as a precondition and AbortTx writes to it.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    tx_uuid                 UUID NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    aborted_at_micros       BIGINT NOT NULL DEFAULT {epoch},
                    aborted_at_seq_num      BIGINT NOT NULL,
                    PRIMARY KEY (tx_uuid, branch_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&abort_tx_log),
            ))
            .await?;

        // Enforces one commit per timestamp per branch.
        let commit_tx_log_idx = format!("idx_{}_committed", commit_tx_log.replace('-', "_"));
        driver
            .execute_no_result(&format!(
                r#"CREATE UNIQUE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tx} (branch_uuid, commit_micros)"#,
                qi_idx = Self::quote_identifier(&commit_tx_log_idx),
                qi_tx = Self::quote_identifier(&commit_tx_log),
            ))
            .await?;

        // The commit-order serial is unique + gapless per branch, allocated by
        // the `commit_tx_log_seq_num` counter row inside the commit statement.
        let commit_tx_log_seq_idx = format!("idx_{}_seq", commit_tx_log.replace('-', "_"));
        driver
            .execute_no_result(&format!(
                r#"CREATE UNIQUE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tx} (branch_uuid, commit_seq_num)"#,
                qi_idx = Self::quote_identifier(&commit_tx_log_seq_idx),
                qi_tx = Self::quote_identifier(&commit_tx_log),
            ))
            .await?;

        // Per-(tx, table) summary index: bulk inserts pay one summary row, not
        // per-row overhead, and the PK alone enforces idempotent emission
        // across multiple WriteData calls within the same penca tx.
        //
        // The PK leads on `tx_uuid` (matching the tx-log family) so downstream
        // lookups — conflict detection and persist both probe
        // `WHERE tx_uuid IN (...)` after a commit_tx_log scan — hit the PK
        // directly, needing no secondary index on tx_uuid.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    tx_uuid         UUID NOT NULL,
                    branch_uuid     UUID NOT NULL,
                    table_uuid      UUID NOT NULL,
                    PRIMARY KEY (tx_uuid, branch_uuid, table_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&tx_table_log),
            ))
            .await?;
        Ok(())
    }

    /// Create the `commit_tx_log_seq_num` parent — the per-branch gapless
    /// commit-order counter for `commit_tx_log`. Exactly one row per branch
    /// (`branch_uuid` PK) holding `seq_num` = the next `commit_seq_num` to
    /// assign (seeded at 0). Per-branch leaves + their single counter row are
    /// added by [`Self::ensure_tx_log_branch_partitions`].
    ///
    /// Co-located with the tx-log family in the per-branch stack (NOT on
    /// `branch_store` / the global control plane): the counter UPDATE is in
    /// the same tx as the `commit_tx_log` INSERT, so it must share that pg
    /// instance. A dedicated table also keeps the hot commit counter off
    /// `branch_store`, so commits never lock-contend with branch metadata
    /// writes.
    async fn create_commit_tx_log_seq_num_parent(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let commit_tx_log_seq_num = naming::commit_tx_log_seq_num_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    branch_uuid     UUID NOT NULL PRIMARY KEY,
                    seq_num         BIGINT NOT NULL
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&commit_tx_log_seq_num),
            ))
            .await?;
        Ok(())
    }

    /// Seed a forked child branch's `commit_tx_log_seq_num` counter from the
    /// source branch's fork commit, so the child's commit seqs
    /// (`> commit_seq_num(T)`) are disjoint from the parent's
    /// (`<= commit_seq_num(T)`). Latest-wins-on-`commit_seq_num` resolution
    /// then lets the child shadow the parent across the fork boundary with no
    /// lineage tiebreak.
    ///
    /// `fork_seq` is `commit_seq_num(T)`, the fork commit resolved ONCE inside
    /// PersistBranch — the source branch's head commit, or the named `base_tx`.
    /// It is a committed seq by construction (PersistBranch reads it from the
    /// source's `commit_tx_log`, never the write-side counter, which can
    /// transiently lead the log by an allocated-but-uncommitted seq), so a fork
    /// never pins to an uncommitted seq. Resolving `fork_seq` under
    /// PersistBranch rather than re-reading `MAX(commit_seq_num)` here also
    /// closes the window where a source commit between the flush and the
    /// seed-read would bump `MAX` past `T`. A source with zero commits resolves
    /// to `fork_seq = 0`, seeding the child to 1.
    ///
    /// The counter row holds the *next* `commit_seq_num` *to assign* and
    /// allocation returns the pre-increment value (see
    /// [`commit_tx_log_insert_sql`]), so the counter is seeded to `fork_seq + 1`:
    /// the child's first commit (the fork tx) then allocates
    /// `commit_seq_num(T) + 1`, one past the parent's fork-point seq.
    ///
    /// Must run after [`Self::ensure_branch_partitions`] (which seeds the child
    /// counter row to 0) and before the child's first commit.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            catalog = %catalog_uuid,
            child_branch_uuid = %child_branch_uuid,
            fork_seq,
        ),
    )]
    pub async fn seed_commit_seq_num_from_fork(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
        child_branch_uuid: &Uuid,
        fork_seq: i64,
    ) -> Result<(), sqlx::Error> {
        let child_seq_num =
            naming::commit_tx_log_seq_num_partition(catalog_uuid, child_branch_uuid);
        // The child's counter partition holds exactly its one row, so the
        // branch predicate targets it directly (partition-direct, trusted UUID).
        driver
            .execute_no_result(&format!(
                "UPDATE {child} SET seq_num = {seed} WHERE branch_uuid = '{child_branch_uuid}'",
                child = Self::quote_identifier(&child_seq_num),
                seed = fork_seq + 1,
            ))
            .await?;
        Ok(())
    }

    /// Create the `abort_seq_num` parent (ADR 0027) — the per-branch gapless
    /// **abort**-order counter, the abort-axis sibling of
    /// [`Self::create_commit_tx_log_seq_num_parent`]. Exactly one row per branch
    /// (`branch_uuid` PK) holding `seq_num` = the next `aborted_at_seq_num` to
    /// assign (seeded at 0). Incremented under a row lock in the same
    /// statement as the `abort_tx_log` INSERT (see penca-storage-hot
    /// `abort_tx`), so aborts get strictly-monotone, in-allocation-order
    /// values — what the purge abort watermark `Pa` needs. Co-located with the
    /// tx-log family in the per-branch stack for the same reason the commit
    /// counter is: the counter UPDATE shares the abort INSERT's transaction.
    async fn create_abort_seq_num_parent(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let abort_seq_num = naming::abort_seq_num_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    branch_uuid     UUID NOT NULL PRIMARY KEY,
                    seq_num         BIGINT NOT NULL
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&abort_seq_num),
            ))
            .await?;
        Ok(())
    }

    /// Create the persist + snapshot metadata parent tables.
    ///
    /// LIST-partitioned by `branch_uuid` (one leaf per branch, matches
    /// the tx-log family). Composite PKs `(branch_uuid, <existing>)`;
    /// per-segment `commit_micros` gates plan visibility.
    ///
    /// `log_kind` lives on `table_persist_metadata` only, as part of the
    /// deterministic identity chain; segments JOIN up to read the kind rather
    /// than denormalizing a column.
    async fn create_persist_snapshot_metadata_parents(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let epoch = Self::microsecond_epoch();
        let table_persist_metadata = naming::table_persist_metadata_table(catalog_uuid);
        let table_persist_segment_metadata =
            naming::table_persist_segment_metadata_table(catalog_uuid);
        let table_purge_metadata = naming::table_purge_metadata_table(catalog_uuid);
        let table_snapshot_metadata = naming::table_snapshot_metadata_table(catalog_uuid);
        let table_snapshot_segment_metadata =
            naming::table_snapshot_segment_metadata_table(catalog_uuid);

        // One row per (branch, table, persisted_at, log_kind). `log_kind` is
        // part of the deterministic identity chain (`table_persist_uuid =
        // row_uuid_for_pk(catalog_uuid, [branch_uuid, table_uuid,
        // persisted_at, log_kind])`), hence the CHECK pinning it to the closed
        // set the writer emits.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_persist_uuid        UUID NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    table_uuid              UUID NOT NULL,
                    persisted_at_micros       BIGINT NOT NULL,
                    -- CHA-443 (IMPL-1): the persist seq watermark = MAX(commit_seq_num)
                    -- over the committed rows persisted; the seq analog of
                    -- persisted_at_micros. NULL on the aborts-only branch (no
                    -- committed rows) so IMPL-4's MAX(commit_seq_num) ignores it.
                    commit_seq_num          BIGINT,
                    log_kind                TEXT NOT NULL
                        CHECK (log_kind IN ('upsert_log','delete_log')),
                    written_at_micros       BIGINT DEFAULT {epoch},
                    commit_micros           BIGINT,
                    PRIMARY KEY (branch_uuid, table_persist_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_persist_metadata),
            ))
            .await?;

        // One row per cold file. Plan visibility gates on the segment's own
        // `commit_micros`; segments JOIN to `table_persist_metadata` on
        // `table_persist_uuid` for the log_kind classification.
        //
        // `is_sealed` drives the per-scope active+sealed compact model:
        // `false` for both uncompacted rows and rows pointing at the current
        // active merged file, `true` for rows of a previously-sealed merged
        // file. Sealed rows never participate in another compact wave, and the
        // false → true transition is one-way.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_persist_segment_uuid    UUID NOT NULL,
                    table_persist_uuid            UUID NOT NULL,
                    branch_uuid                 UUID NOT NULL,
                    table_uuid                  UUID NOT NULL,
                    chunk_idx                   INTEGER NOT NULL DEFAULT 0,
                    min_tx_commit_micros        BIGINT NOT NULL,
                    max_tx_commit_micros        BIGINT NOT NULL,
                    min_commit_seq_num          BIGINT NOT NULL,
                    max_commit_seq_num          BIGINT NOT NULL,
                    object_uri                  TEXT NOT NULL,
                    "offset"                    BIGINT,
                    length                      BIGINT,
                    row_count                   BIGINT NOT NULL,
                    format                      TEXT NOT NULL,
                    size_bytes                  BIGINT DEFAULT 0,
                    metadata                    JSONB DEFAULT '{{}}'::jsonb,
                    statistics                  BYTEA,
                    written_at_micros           BIGINT DEFAULT {epoch},
                    commit_micros               BIGINT,
                    is_sealed                   BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (branch_uuid, table_persist_segment_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_persist_segment_metadata),
            ))
            .await?;

        // One row per cold tx_log file. persist_tx_log flushes a slim
        // per-branch commit map (commit_seq_num -> commit_micros/author/
        // comment) so fork positions and audit tx-metadata survive hot
        // commit_tx_log GC. Slim + low-volume, so unpartitioned with
        // branch_uuid as a plain column, unlike the high-volume,
        // per-branch-partitioned persist segment index.
        //
        // `committed_at_micros` NULL = uncommitted: the two-phase durable write
        // inserts NULL, writes the file, then stamps it committed, so a crashed
        // flush leaves the row invisible to reads and the watermark.
        let tx_log_persist_segment_metadata =
            naming::tx_log_persist_segment_metadata_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    tx_log_segment_uuid    UUID NOT NULL,
                    branch_uuid            UUID NOT NULL,
                    min_commit_seq_num     BIGINT NOT NULL,
                    max_commit_seq_num     BIGINT NOT NULL,
                    min_commit_micros      BIGINT NOT NULL,
                    max_commit_micros      BIGINT NOT NULL,
                    object_uri             TEXT NOT NULL,
                    row_count              BIGINT NOT NULL,
                    format                 TEXT NOT NULL,
                    committed_at_micros    BIGINT,
                    PRIMARY KEY (branch_uuid, tx_log_segment_uuid)
                )"#,
                qi = Self::quote_identifier(&tx_log_persist_segment_metadata),
            ))
            .await?;

        // One row per purge wave that advances a watermark. Seq-only per
        // ADR 0027: `last_purged_commit_seq_num` (`Pu`) is the committed
        // hot↔cold read fence `plan()` reads; `last_purged_aborted_seq_num`
        // (`Pa`) is the abort cleanup frontier. Both nullable — a wave records
        // only the axis(es) it advanced, and the branch-min/MAX consumers
        // ignore NULL. `commit_micros` is NOT a watermark: it is the two-phase
        // commit timestamp, used by commit_tx_log GC's as-of isolation.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_purge_uuid           UUID NOT NULL,
                    branch_uuid                UUID NOT NULL,
                    table_uuid                 UUID NOT NULL,
                    last_purged_commit_seq_num BIGINT,
                    last_purged_aborted_seq_num BIGINT,
                    written_at_micros          BIGINT DEFAULT {epoch},
                    commit_micros              BIGINT,
                    PRIMARY KEY (branch_uuid, table_purge_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_purge_metadata),
            ))
            .await?;

        // `partition_keys` / `clustering_keys` are the write-time layout keys
        // that governed this snapshot's partition split and intra-partition
        // sort (clustering defaults to primary keys when unset). Parent-level,
        // not per-segment: a key change between snapshots forces a full rewrite
        // (ADR 0024), so every segment in one snapshot shares one key set by
        // construction. Carry-forward reads them for key-change detection.
        //
        // This DDL only runs at CreateCatalog; pre-release there is no in-place
        // migration path — recreate catalogs that predate a schema change.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_snapshot_uuid     UUID NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    table_uuid              UUID NOT NULL,
                    snapshotted_at_micros   BIGINT NOT NULL,
                    -- CHA-443 (IMPL-2): snapshot seq watermark W_snap. NOT NULL;
                    -- -1 (SNAPSHOT_SEQ_GENESIS) for an empty/genesis baseline so
                    -- the seq-aware picker's `commit_seq_num <= N` keeps it selectable
                    -- (a NULL would wrongly exclude it).
                    commit_seq_num          BIGINT NOT NULL,
                    -- CHA-432: durable retention rung. Set once at snapshot
                    -- creation and sticky, so the retention floor (newest
                    -- durable at/before the window start) stays monotonic and
                    -- every downstream op reads one flag instead of re-deriving
                    -- the rung set. Default false; the assignment path stamps
                    -- true per the density cadence.
                    durable                 BOOLEAN NOT NULL DEFAULT false,
                    partition_keys          TEXT[],
                    clustering_keys         TEXT[],
                    written_at_micros       BIGINT DEFAULT {epoch},
                    commit_micros           BIGINT,
                    PRIMARY KEY (branch_uuid, table_snapshot_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_snapshot_metadata),
            ))
            .await?;

        // Snapshot segments are immutable — never compacted (ADR 0024) — so
        // there is no `is_sealed` and no active+sealed model on this side.
        // `"offset"`/`length` are packed row-range addressing, written
        // explicitly on every row (a single-segment file is the whole-file
        // range, never NULL), hence NOT NULL.
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_snapshot_segment_uuid   UUID NOT NULL,
                    table_snapshot_uuid           UUID,
                    branch_uuid                   UUID NOT NULL,
                    table_uuid                    UUID NOT NULL,
                    chunk_idx               INTEGER NOT NULL DEFAULT 0,
                    object_uri              TEXT NOT NULL,
                    "offset"                BIGINT NOT NULL,
                    length                  BIGINT NOT NULL,
                    size_bytes              BIGINT DEFAULT 0,
                    format                  TEXT NOT NULL,
                    metadata                JSONB DEFAULT '{{}}'::jsonb,
                    statistics              BYTEA,
                    row_count               BIGINT NOT NULL,
                    written_at_micros       BIGINT DEFAULT {epoch},
                    commit_micros           BIGINT,
                    PRIMARY KEY (branch_uuid, table_snapshot_segment_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_snapshot_segment_metadata),
            ))
            .await?;

        // In-flight compact merged-file tracking, distinct from
        // `table_persist_segment_metadata`. The compactor INSERTs a row(NULL)
        // before writing a merged file, then UPDATEs `commit_micros` inside the
        // same tx that re-points the input `table_persist_segment_metadata`
        // rows. On crash, the NULL-row + file remain on disk for a future
        // orphan-cleanup routine — concurrent compaction safety itself comes
        // from `SELECT FOR UPDATE` on the input segment rows, not from any
        // sweep over this table. `branch_uuid` / `table_uuid` are NOT NULL so
        // any helper that forgets to pin scope fails loudly at INSERT.
        //
        // Partitioned BY LIST (branch_uuid) so `DeleteBranch` reclaims rows via
        // DROP PARTITION CASCADE in `drop_branch_partitions`. `branch_uuid`
        // participates in the PK because PG requires the partition key in any
        // unique constraint.
        //
        // No FK to `table_persist_segment_metadata` (ADR 0015). No indices
        // beyond the PK — the in-flight set is small and orphan cleanup
        // is a separate routine, not on the hot compact path.
        let compact_segment_metadata = naming::compact_segment_metadata_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    object_uri              TEXT NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    table_uuid              UUID NOT NULL,
                    commit_micros           BIGINT,
                    PRIMARY KEY (branch_uuid, object_uri)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&compact_segment_metadata),
            ))
            .await?;

        // ADR 0019 §"Four-part mechanism" item 3: segment_delete_set holds cold
        // segment files queued for physical deletion past the universal grace
        // window. The persist-compact merge tx INSERTs one row per replaced old
        // URI atomically with the URI swap on
        // `table_persist_segment_metadata`; `sweep_segments` reads rows whose
        // `written_at_micros + query_timeout < now`, deletes the cold file,
        // then deletes the row. The sweep discriminates only by `object_uri` —
        // there is deliberately no `kind` column.
        //
        // Partitioned BY LIST (branch_uuid) so `DeleteBranch` reclaims rows via
        // DROP PARTITION CASCADE; `branch_uuid` participates in the PK per PG's
        // partition-key constraint.
        let segment_delete_set = naming::segment_delete_set_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    segment_delete_uuid     UUID NOT NULL,
                    branch_uuid             UUID NOT NULL,
                    table_uuid              UUID NOT NULL,
                    object_uri              TEXT NOT NULL,
                    written_at_micros       BIGINT NOT NULL DEFAULT {epoch},
                    PRIMARY KEY (branch_uuid, segment_delete_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&segment_delete_set),
            ))
            .await?;

        // The PARENT of the cold-index materialization (ADR 0026 §5) — one row
        // per `(snapshot, index)`. Mirrors `table_snapshot_metadata`: a fileless
        // header, two-phase committed, retired with the snapshot, re-declared
        // fresh each snapshot. `index_uuid` is NULLable (NULL ⇒ the
        // strictly-internal `row_uuid` identity index; non-NULL ⇒ a logical,
        // un-enforced reference to `__penca_system__.indexes`, ADR 0015); that
        // NULL-ness IS the role discriminator, so there is no `index_kind`.
        // `key_columns` denormalizes a USER index's declared key columns onto
        // this snapshot-scoped header so planner covering-index selection reads
        // only snapshot-index metadata and never `index_metadata`; NULL for the
        // internal identity index and the built-in system name indexes, neither
        // of which is planner-selectable.
        let table_snapshot_index_metadata =
            naming::table_snapshot_index_metadata_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    table_snapshot_index_uuid   UUID NOT NULL,
                    branch_uuid                 UUID NOT NULL,
                    table_snapshot_uuid         UUID NOT NULL,
                    index_uuid                  UUID,
                    key_columns                 TEXT[],
                    written_at_micros           BIGINT DEFAULT {epoch},
                    commit_micros               BIGINT,
                    PRIMARY KEY (branch_uuid, table_snapshot_index_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_snapshot_index_metadata),
            ))
            .await?;

        // The CHILD (ADR 0026 §5) — one row per `(segment, index)` sidecar,
        // referencing its parent via `table_snapshot_index_uuid`. Shaped like
        // `table_snapshot_segment_metadata` because an index sidecar is itself
        // a cold file: `object_uri`/`offset`/`length` addressing, `statistics`
        // (indexed-key min/max bounds, binary — decoded in-planner by the
        // seek), the two-phase commit pair, and `segment_delete_set` GC
        // participation. The index identity (internal `row_uuid` vs a user
        // index) lives on the parent, so the child carries only the
        // `table_snapshot_index_uuid` FK and carries forward by reference with
        // its base segment.
        let table_snapshot_segment_index_metadata =
            naming::table_snapshot_segment_index_metadata_table(catalog_uuid);
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    segment_index_uuid          UUID NOT NULL,
                    branch_uuid                 UUID NOT NULL,
                    segment_uuid                UUID NOT NULL,
                    table_snapshot_index_uuid   UUID NOT NULL,
                    object_uri                  TEXT NOT NULL,
                    "offset"                    BIGINT NOT NULL,
                    length                      BIGINT NOT NULL,
                    format                      TEXT NOT NULL,
                    size_bytes                  BIGINT DEFAULT 0,
                    statistics                  BYTEA,
                    written_at_micros           BIGINT DEFAULT {epoch},
                    commit_micros               BIGINT,
                    PRIMARY KEY (branch_uuid, segment_index_uuid)
                ) PARTITION BY LIST (branch_uuid)"#,
                qi = Self::quote_identifier(&table_snapshot_segment_index_metadata),
            ))
            .await?;
        Ok(())
    }

    /// Create the secondary indexes on the persist + snapshot metadata parents.
    ///
    /// Created on the parent → PG propagates a matching index to every
    /// current and future leaf partition. Without these, LIST
    /// partitioning only prunes to a single branch leaf; within that
    /// leaf, lookups by `table_uuid`, `table_persist_uuid`, and
    /// `table_snapshot_uuid` would seq-scan.
    ///
    /// Identifier-length: PG has a 63-byte name cap. Index names use
    /// short per-table codes (tfm/tfsm/tpm/tsm/tssm) + a short column
    /// suffix so each `idx_{catalog_uuid_underscored}_{code}_{col}`
    /// fits in 63 chars regardless of which table it's on.
    async fn create_persist_snapshot_metadata_indexes(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let table_persist_metadata = naming::table_persist_metadata_table(catalog_uuid);
        let table_persist_segment_metadata =
            naming::table_persist_segment_metadata_table(catalog_uuid);
        let table_purge_metadata = naming::table_purge_metadata_table(catalog_uuid);
        let table_snapshot_metadata = naming::table_snapshot_metadata_table(catalog_uuid);
        let table_snapshot_segment_metadata =
            naming::table_snapshot_segment_metadata_table(catalog_uuid);
        let segment_delete_set = naming::segment_delete_set_table(catalog_uuid);
        let cat_u = catalog_uuid.to_string().replace('-', "_");

        // table_persist_metadata: per-table persist-history reads classify
        // by `(table_uuid, log_kind)`.
        let idx_tfm_t = format!("idx_{cat_u}_tfm_t");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_uuid, log_kind)"#,
                qi_idx = Self::quote_identifier(&idx_tfm_t),
                qi_tbl = Self::quote_identifier(&table_persist_metadata),
            ))
            .await?;

        // table_persist_segment_metadata: parent-JOIN reads filter the
        // segment side on `table_persist_uuid`.
        let idx_tfsm_tf = format!("idx_{cat_u}_tfsm_tf");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_persist_uuid)"#,
                qi_idx = Self::quote_identifier(&idx_tfsm_tf),
                qi_tbl = Self::quote_identifier(&table_persist_segment_metadata),
            ))
            .await?;

        // Load-bearing for the seq watermark MAX queries (ADR 0027). `plan()`'s
        // read fence reads `MAX(last_purged_commit_seq_num)` (`Pu`) and
        // commit_tx_log GC reads both that and
        // `MAX(last_purged_aborted_seq_num)` (`Pa`), each
        // `WHERE table_uuid = $ AND commit_micros IS NOT NULL`. One partial
        // index per seq column lets MAX walk the leaf once and skips phase-1
        // (uncommitted) rows in-index.
        let idx_tpm_pu = format!("idx_{cat_u}_tpm_pu");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_uuid, last_purged_commit_seq_num DESC)
                WHERE commit_micros IS NOT NULL"#,
                qi_idx = Self::quote_identifier(&idx_tpm_pu),
                qi_tbl = Self::quote_identifier(&table_purge_metadata),
            ))
            .await?;
        let idx_tpm_pa = format!("idx_{cat_u}_tpm_pa");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_uuid, last_purged_aborted_seq_num DESC)
                WHERE commit_micros IS NOT NULL"#,
                qi_idx = Self::quote_identifier(&idx_tpm_pa),
                qi_tbl = Self::quote_identifier(&table_purge_metadata),
            ))
            .await?;

        // Load-bearing for read_snapshot_segments_for_table's "latest committed
        // snapshot" sub-select (`ORDER BY snapshotted_at_micros DESC LIMIT 1`)
        // and `MAX(snapshotted_at_micros)` in `get_table_metadata`. The
        // `(table_uuid, commit_micros DESC)` shape acts as a point lookup by
        // `table_uuid` only — neither consumer sorts on `commit_micros`, so the
        // DESC ordering does nothing useful, and `branch_uuid` (in every
        // consumer's WHERE) is absent from the key entirely.
        // TODO(CHA-535): reshape to what the consumers actually filter and
        // order by, once measurement confirms it matters at realistic scale.
        let idx_tsm_t = format!("idx_{cat_u}_tsm_t");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_uuid, commit_micros DESC)"#,
                qi_idx = Self::quote_identifier(&idx_tsm_t),
                qi_tbl = Self::quote_identifier(&table_snapshot_metadata),
            ))
            .await?;

        // Load-bearing for the JOIN in get_snapshot_segments_for_table +
        // get_table_metadata.
        let idx_tssm_snap = format!("idx_{cat_u}_tssm_snap");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_snapshot_uuid)"#,
                qi_idx = Self::quote_identifier(&idx_tssm_snap),
                qi_tbl = Self::quote_identifier(&table_snapshot_segment_metadata),
            ))
            .await?;

        // Persist side only — snapshot segments are immutable and never compact
        // (ADR 0024). Compact enumerates the unsealed subset per scope on every
        // wave; on a long-running branch the sealed set accumulates faster than
        // the unsealed, so a PARTIAL index containing only unsealed rows keeps
        // enumeration cheap as the total row count grows.
        let idx_tfsm_seal = format!("idx_{cat_u}_tfsm_seal");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (branch_uuid) WHERE is_sealed = false"#,
                qi_idx = Self::quote_identifier(&idx_tfsm_seal),
                qi_tbl = Self::quote_identifier(&table_persist_segment_metadata),
            ))
            .await?;

        // `sweep_segments` scans by `written_at_micros < now - query_timeout`
        // after partition pruning on `branch_uuid`; this index bounds the scan
        // to grace-expired rows rather than the whole deferred set. Note
        // grace-expired is NOT the same as deletable under the refcount gate: a
        // URI queued by an early retirement while later snapshots still
        // reference it (carry-forward) sits in the expired range until the last
        // reference drops, so each sweep re-scans that standing subset and pays
        // one index-served `object_uri` probe per row. Enqueue-at-last-drop is
        // the structural alternative if the blocked set ever proves large.
        let idx_sds_age = format!("idx_{cat_u}_sds_age");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (branch_uuid, written_at_micros)"#,
                qi_idx = Self::quote_identifier(&idx_sds_age),
                qi_tbl = Self::quote_identifier(&segment_delete_set),
            ))
            .await?;

        // The sweep's refcount gate anti-joins each eligible
        // `segment_delete_set` row against `table_snapshot_segment_metadata` on
        // `object_uri` (delete only at snapshot reference count zero,
        // ADR 0024 §4). This index serves that per-candidate probe; partition
        // pruning on `branch_uuid` happens first, so the URI alone suffices.
        //
        // Stale-catalog note (pre-release, in-place DDL): this CREATE only runs
        // at CreateCatalog, so older catalogs lack the index — the gate stays
        // correct there, the probe just seq-scans the branch leaf.
        let idx_tssm_uri = format!("idx_{cat_u}_tssm_uri");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (object_uri)"#,
                qi_idx = Self::quote_identifier(&idx_tssm_uri),
                qi_tbl = Self::quote_identifier(&table_snapshot_segment_metadata),
            ))
            .await?;

        // The child table is read by query planning grouped by `segment_uuid`
        // (probe the sidecars for the base segments in the plan), and the
        // carry-forward / GC-enqueue plumbing looks rows up by the same key.
        // Without this the lookup is a leaf seq-scan after the `branch_uuid`
        // partition prune.
        let table_snapshot_segment_index_metadata =
            naming::table_snapshot_segment_index_metadata_table(catalog_uuid);
        let idx_tssim_seg = format!("idx_{cat_u}_tssim_seg");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (segment_uuid)"#,
                qi_idx = Self::quote_identifier(&idx_tssim_seg),
                qi_tbl = Self::quote_identifier(&table_snapshot_segment_index_metadata),
            ))
            .await?;

        // The GC sweep's refcount gate (`eligible_segment_delete_set_rows`)
        // anti-joins the child sidecar table on `object_uri` to pin shared
        // carried-sidecar files, mirroring the base-segment `idx_..._tssm_uri`
        // probe.
        let idx_tssim_uri = format!("idx_{cat_u}_tssim_uri");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (object_uri)"#,
                qi_idx = Self::quote_identifier(&idx_tssim_uri),
                qi_tbl = Self::quote_identifier(&table_snapshot_segment_index_metadata),
            ))
            .await?;

        // Query planning asks "does snapshot S have index X?" against the
        // parent keyed by `table_snapshot_uuid`; without this the lookup is a
        // leaf seq-scan after the `branch_uuid` partition prune.
        let table_snapshot_index_metadata =
            naming::table_snapshot_index_metadata_table(catalog_uuid);
        let idx_tsim_snap = format!("idx_{cat_u}_tsim_snap");
        driver
            .execute_no_result(&format!(
                r#"CREATE INDEX IF NOT EXISTS {qi_idx}
                ON {qi_tbl} (table_snapshot_uuid)"#,
                qi_idx = Self::quote_identifier(&idx_tsim_snap),
                qi_tbl = Self::quote_identifier(&table_snapshot_index_metadata),
            ))
            .await?;
        Ok(())
    }

    /// Seed the genesis tx row + main `branch_store` row, then bootstrap
    /// `__penca_system__.{schemas,tables}` as real Penca Tables on the
    /// main branch. Assumes the tx-log family + metadata branch partitions for
    /// `main_branch_uuid` already exist.
    ///
    /// Bootstrap ROW insertion for the system tables is handled by
    /// `penca_storage_meta::LifecycleManager`; this only creates the data
    /// tables.
    async fn seed_genesis_and_system_tables(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
        main_branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let branch_store = naming::branch_store_table(catalog_uuid);
        let genesis_tx_uuid = naming::genesis_tx_uuid(catalog_uuid);
        let commit_tx_log_part = naming::commit_tx_log_partition(catalog_uuid, main_branch_uuid);
        let commit_tx_log_seq_num_part =
            naming::commit_tx_log_seq_num_partition(catalog_uuid, main_branch_uuid);
        // `began_at_micros` uses the Postgres server clock to match how every
        // other tx row is written — `microsecond_epoch()` is the canonical NOW
        // expression.
        let now_micros = Self::microsecond_epoch();
        // Genesis is the first commit, so it allocates commit_seq_num = 0 from
        // the same counter every other commit uses; the counter row was seeded
        // by `ensure_tx_log_branch_partitions`, which `create_catalog_tables`
        // runs before this. The INSERT duplicates the `commit_tx_log_insert_sql`
        // CTE because genesis skips begin_tx_log and so cannot route through
        // that shared path.
        driver
            .execute_no_result(&format!(
                r#"WITH c AS (
                    UPDATE {lsn} SET seq_num = seq_num + 1
                    RETURNING seq_num - 1 AS commit_seq_num
                )
                INSERT INTO {qi} (tx_uuid, branch_uuid, began_at_micros, comment, author, commit_seq_num)
                SELECT '{genesis_tx_uuid}', '{main_branch_uuid}', {now_micros}, 'catalog genesis', 'system', c.commit_seq_num
                FROM c"#,
                lsn = Self::quote_identifier(&commit_tx_log_seq_num_part),
                qi = Self::quote_identifier(&commit_tx_log_part),
            ))
            .await?;

        // main forks from the genesis commit, which is the first commit on the
        // branch and so allocates commit_seq_num 0 (the commit_tx_log row seeded
        // just above via `seq_num - 1`). Record 0 as main's base commit-order
        // position.
        driver
            .execute_no_result(&format!(
                r#"INSERT INTO {qi} (branch_uuid, branch_name, fork_commit_seq_num)
                VALUES ('{main_branch_uuid}', '{main_branch_name}', 0)"#,
                qi = Self::quote_identifier(&branch_store),
                main_branch_name = naming::MAIN_BRANCH_NAME,
            ))
            .await?;

        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(catalog_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        // The system table schemas are static + valid, so the only reachable
        // `DataTableError` variant here is `Sql`; anything else is a bug.
        Self::create_data_tables(
            driver,
            &sys_schemas_table_uuid,
            main_branch_uuid,
            &Self::system_schemas_arrow_schema(),
            &Self::system_schemas_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in bootstrap: {other:?}"),
        })?;
        Self::create_data_tables(
            driver,
            &sys_tables_table_uuid,
            main_branch_uuid,
            &Self::system_tables_arrow_schema(),
            &Self::system_tables_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in bootstrap: {other:?}"),
        })?;
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(catalog_uuid);
        Self::create_data_tables(
            driver,
            &sys_indexes_table_uuid,
            main_branch_uuid,
            &Self::system_indexes_arrow_schema(),
            &Self::system_indexes_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in bootstrap: {other:?}"),
        })?;
        Ok(())
    }

    /// Drop all per-catalog tables (CASCADE removes any sub-partitions).
    ///
    /// `main_branch_uuid` is required to address main's system-table physicals:
    /// it is randomly minted, so it cannot be re-derived here.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(catalog = %catalog_uuid, branch = %main_branch_uuid),
    )]
    pub async fn drop_catalog_tables(
        driver: &impl DbDriver,
        catalog_uuid: &Uuid,
        main_branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        // Drop main branch's system-table physicals first; per-branch
        // physicals for other branches are cleaned up via
        // drop_branch_partitions.
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(catalog_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(catalog_uuid);
        Self::drop_data_tables(driver, &sys_schemas_table_uuid, main_branch_uuid).await?;
        Self::drop_data_tables(driver, &sys_tables_table_uuid, main_branch_uuid).await?;
        Self::drop_data_tables(driver, &sys_indexes_table_uuid, main_branch_uuid).await?;
        let tables = [
            naming::table_snapshot_index_metadata_table(catalog_uuid),
            naming::table_snapshot_segment_index_metadata_table(catalog_uuid),
            naming::segment_delete_set_table(catalog_uuid),
            naming::compact_segment_metadata_table(catalog_uuid),
            naming::table_snapshot_segment_metadata_table(catalog_uuid),
            naming::table_snapshot_metadata_table(catalog_uuid),
            naming::table_purge_metadata_table(catalog_uuid),
            naming::tx_log_persist_segment_metadata_table(catalog_uuid),
            naming::table_persist_segment_metadata_table(catalog_uuid),
            naming::table_persist_metadata_table(catalog_uuid),
            naming::tx_table_log_table(catalog_uuid),
            naming::abort_tx_log_table(catalog_uuid),
            naming::commit_tx_log_table(catalog_uuid),
            naming::begin_tx_log_table(catalog_uuid),
            naming::commit_tx_log_seq_num_table(catalog_uuid),
            naming::abort_seq_num_table(catalog_uuid),
            naming::branch_store_table(catalog_uuid),
        ];
        Self::drop_tables_if_exist(driver, &tables).await
    }

    /// Arrow schema for `__penca_system__.schemas` user columns.
    /// `schema_uuid` is the row's own identity as a first-class PK column, so
    /// `row_uuid = row_uuid_for_pk(system_schemas_table_uuid, [schema_uuid])`
    /// like every other Penca table. Branch is implicit in partition placement.
    pub fn system_schemas_arrow_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};
        arrow::datatypes::Schema::new(vec![
            Field::new("schema_uuid", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("retention_duration_seconds", DataType::Int64, true),
            Field::new("snapshot_density_seconds", DataType::Int64, true),
        ])
    }

    /// Arrow schema for `__penca_system__.tables` user columns.
    /// `table_uuid` is the row's own identity as a first-class PK column, so
    /// `row_uuid = row_uuid_for_pk(system_tables_table_uuid, [table_uuid])`
    /// like every other Penca table. Branch is implicit in partition placement.
    /// `schema_uuid` is a distinct foreign key (each row's schema parent), NOT
    /// the row's own identity.
    pub fn system_tables_arrow_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};
        arrow::datatypes::Schema::new(vec![
            Field::new("table_uuid", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("schema_uuid", DataType::Utf8, false),
            Field::new("arrow_schema", DataType::Binary, false),
            Field::new(
                "partition_keys",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                false,
            ),
            Field::new(
                "clustering_keys",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                false,
            ),
            Field::new(
                "primary_keys",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                false,
            ),
            Field::new("description", DataType::Utf8, false),
            Field::new("retention_duration_seconds", DataType::Int64, true),
            Field::new("snapshot_density_seconds", DataType::Int64, true),
        ])
    }

    /// Arrow schema for `__penca_system__.indexes` user columns.
    /// `index_uuid` is the row's own identity as a first-class PK column, so
    /// `row_uuid = row_uuid_for_pk(system_indexes_table_uuid, [index_uuid])`.
    /// Branch is implicit in partition placement. `table_uuid` is a distinct
    /// foreign key (the owning table, the list-by-table filter key), NOT the
    /// row's own identity; `index_name` is unique only within that table.
    pub fn system_indexes_arrow_schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};
        arrow::datatypes::Schema::new(vec![
            Field::new("index_uuid", DataType::Utf8, false),
            Field::new("table_uuid", DataType::Utf8, false),
            Field::new("index_name", DataType::Utf8, false),
            Field::new(
                "columns",
                DataType::List(std::sync::Arc::new(Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                false,
            ),
            Field::new("index_type", DataType::Int32, false),
        ])
    }

    /// The declared primary-key column of each `__penca_system__` table — the
    /// row's own entity uuid. The single source of truth for the PK passed to
    /// `create_data_tables` (widening the delete log) and seeded onto the
    /// self-describing rows so `row_uuid = row_uuid_for_pk(
    /// system_<x>_table_uuid, [<entity>_uuid])` holds like every other table.
    pub fn system_schemas_primary_keys() -> Vec<String> {
        vec!["schema_uuid".to_string()]
    }

    /// See [`Self::system_schemas_primary_keys`].
    pub fn system_tables_primary_keys() -> Vec<String> {
        vec!["table_uuid".to_string()]
    }

    /// See [`Self::system_schemas_primary_keys`].
    pub fn system_indexes_primary_keys() -> Vec<String> {
        vec!["index_uuid".to_string()]
    }
}

impl PgDialect {
    /// Create all per-catalog log partitions for a branch.
    ///
    /// Creates the tx-log-family branch partitions and the persist + snapshot
    /// metadata branch partitions. Schema/table metadata are first-class Penca
    /// Tables under `__penca_system__.{schemas,tables}`, so their per-branch
    /// physicals are created by the standard CreateBranch materialize path
    /// (`write_data` against the system tables), NOT here. Idempotent.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(catalog = %catalog_uuid, branch = %branch_uuid),
    )]
    pub async fn ensure_branch_partitions(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        Self::ensure_tx_log_branch_partitions(driver, catalog_uuid, branch_uuid).await?;
        Self::ensure_metadata_branch_partitions(driver, catalog_uuid, branch_uuid).await?;
        // Each branch gets its own physical for the system tables (per-branch
        // deterministic prefix); bootstrap them now so materialize / CRUD
        // writes have somewhere to land.
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(catalog_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(catalog_uuid);
        Self::create_data_tables(
            driver,
            &sys_schemas_table_uuid,
            branch_uuid,
            &Self::system_schemas_arrow_schema(),
            &Self::system_schemas_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in branch bootstrap: {other:?}"),
        })?;
        Self::create_data_tables(
            driver,
            &sys_tables_table_uuid,
            branch_uuid,
            &Self::system_tables_arrow_schema(),
            &Self::system_tables_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in branch bootstrap: {other:?}"),
        })?;
        Self::create_data_tables(
            driver,
            &sys_indexes_table_uuid,
            branch_uuid,
            &Self::system_indexes_arrow_schema(),
            &Self::system_indexes_primary_keys(),
        )
        .await
        .map_err(|e| match e {
            DataTableError::Sql(s) => s,
            other => panic!("unexpected DDL error in branch bootstrap: {other:?}"),
        })?;
        Ok(())
    }

    /// Drop per-catalog log partitions + system-table data tables for a branch.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(catalog = %catalog_uuid, branch = %branch_uuid),
    )]
    pub async fn drop_branch_partitions(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let tx_partitions = [
            naming::tx_table_log_partition(catalog_uuid, branch_uuid),
            naming::abort_tx_log_partition(catalog_uuid, branch_uuid),
            naming::commit_tx_log_partition(catalog_uuid, branch_uuid),
            naming::begin_tx_log_partition(catalog_uuid, branch_uuid),
            naming::commit_tx_log_seq_num_partition(catalog_uuid, branch_uuid),
            naming::abort_seq_num_partition(catalog_uuid, branch_uuid),
            // The persist + snapshot metadata partitions hang under the same
            // per-catalog parents, so they drop in this same CASCADE loop and
            // DeleteBranch leaves no residue.
            naming::table_snapshot_segment_metadata_partition(catalog_uuid, branch_uuid),
            naming::table_snapshot_metadata_partition(catalog_uuid, branch_uuid),
            naming::table_purge_metadata_partition(catalog_uuid, branch_uuid),
            naming::table_persist_segment_metadata_partition(catalog_uuid, branch_uuid),
            naming::table_persist_metadata_partition(catalog_uuid, branch_uuid),
            naming::compact_segment_metadata_partition(catalog_uuid, branch_uuid),
            // DROP CASCADE discards any pending deferred-delete rows along with
            // the branch; the underlying cold files are reclaimed by the
            // cold-side branch teardown.
            naming::segment_delete_set_partition(catalog_uuid, branch_uuid),
            naming::table_snapshot_index_metadata_partition(catalog_uuid, branch_uuid),
            naming::table_snapshot_segment_index_metadata_partition(catalog_uuid, branch_uuid),
        ];
        Self::drop_tables_if_exist(driver, &tx_partitions).await?;
        let sys_schemas_table_uuid = naming::system_schemas_table_uuid(catalog_uuid);
        let sys_tables_table_uuid = naming::system_tables_table_uuid(catalog_uuid);
        let sys_indexes_table_uuid = naming::system_indexes_table_uuid(catalog_uuid);
        Self::drop_data_tables(driver, &sys_schemas_table_uuid, branch_uuid).await?;
        Self::drop_data_tables(driver, &sys_tables_table_uuid, branch_uuid).await?;
        Self::drop_data_tables(driver, &sys_indexes_table_uuid, branch_uuid).await?;
        Ok(())
    }

    /// Create the tx-log-family branch partitions for a branch.
    ///
    /// Covers begin_tx_log / abort_tx_log / commit_tx_log / tx_table_log —
    /// single-axis LIST partitions on `branch_uuid`. Idempotent.
    async fn ensure_tx_log_branch_partitions(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let partitions: [(String, String); 6] = [
            (
                naming::begin_tx_log_partition(catalog_uuid, branch_uuid),
                naming::begin_tx_log_table(catalog_uuid),
            ),
            (
                naming::abort_tx_log_partition(catalog_uuid, branch_uuid),
                naming::abort_tx_log_table(catalog_uuid),
            ),
            (
                naming::commit_tx_log_partition(catalog_uuid, branch_uuid),
                naming::commit_tx_log_table(catalog_uuid),
            ),
            (
                naming::tx_table_log_partition(catalog_uuid, branch_uuid),
                naming::tx_table_log_table(catalog_uuid),
            ),
            (
                naming::commit_tx_log_seq_num_partition(catalog_uuid, branch_uuid),
                naming::commit_tx_log_seq_num_table(catalog_uuid),
            ),
            (
                naming::abort_seq_num_partition(catalog_uuid, branch_uuid),
                naming::abort_seq_num_table(catalog_uuid),
            ),
        ];

        Self::ensure_list_partitions(driver, branch_uuid, &partitions).await?;

        // Seed this branch's commit-order and abort-order counter rows
        // (next-to-assign = 0). This is the inner fn that BOTH
        // `create_catalog_tables` (before genesis) and
        // `ensure_branch_partitions` (CreateBranch) call, so genesis and every
        // new branch can allocate from them. Idempotent so re-runs are no-ops.
        for part in [
            naming::commit_tx_log_seq_num_partition(catalog_uuid, branch_uuid),
            naming::abort_seq_num_partition(catalog_uuid, branch_uuid),
        ] {
            driver
                .execute_no_result(&format!(
                    "INSERT INTO {qi} (branch_uuid, seq_num) \
                     VALUES ('{branch_uuid}', 0) ON CONFLICT DO NOTHING",
                    qi = Self::quote_identifier(&part),
                ))
                .await?;
        }
        Ok(())
    }

    /// Create the persist + snapshot metadata branch partitions for a branch.
    ///
    /// Covers table_persist_metadata / table_persist_segment_metadata /
    /// table_purge_metadata / table_snapshot_metadata /
    /// table_snapshot_segment_metadata / compact_segment_metadata — all
    /// single-axis LIST partitions on `branch_uuid`, same shape as
    /// [`Self::ensure_tx_log_branch_partitions`]. Idempotent.
    async fn ensure_metadata_branch_partitions(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        catalog_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let partitions: [(String, String); 9] = [
            (
                naming::table_persist_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_persist_metadata_table(catalog_uuid),
            ),
            (
                naming::table_persist_segment_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_persist_segment_metadata_table(catalog_uuid),
            ),
            (
                naming::table_purge_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_purge_metadata_table(catalog_uuid),
            ),
            (
                naming::table_snapshot_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_snapshot_metadata_table(catalog_uuid),
            ),
            (
                naming::table_snapshot_segment_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_snapshot_segment_metadata_table(catalog_uuid),
            ),
            (
                naming::compact_segment_metadata_partition(catalog_uuid, branch_uuid),
                naming::compact_segment_metadata_table(catalog_uuid),
            ),
            (
                naming::segment_delete_set_partition(catalog_uuid, branch_uuid),
                naming::segment_delete_set_table(catalog_uuid),
            ),
            (
                naming::table_snapshot_index_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_snapshot_index_metadata_table(catalog_uuid),
            ),
            (
                naming::table_snapshot_segment_index_metadata_partition(catalog_uuid, branch_uuid),
                naming::table_snapshot_segment_index_metadata_table(catalog_uuid),
            ),
        ];

        Self::ensure_list_partitions(driver, branch_uuid, &partitions).await
    }

    /// Run `CREATE TABLE IF NOT EXISTS … PARTITION OF … FOR VALUES IN
    /// (branch_uuid)` for each `(partition_name, parent_table)` pair.
    /// Shared body of [`Self::ensure_tx_log_branch_partitions`] and
    /// [`Self::ensure_metadata_branch_partitions`]. Idempotent.
    async fn ensure_list_partitions(
        driver: &impl DbDriver<Row = sqlx::postgres::PgRow>,
        branch_uuid: &Uuid,
        partitions: &[(String, String)],
    ) -> Result<(), sqlx::Error> {
        for (partition_name, parent_table) in partitions {
            driver
                .execute_no_result(&format!(
                    r#"CREATE TABLE IF NOT EXISTS {qi_part} PARTITION OF {qi_parent}
                    FOR VALUES IN ('{branch_uuid}')"#,
                    qi_part = Self::quote_identifier(partition_name),
                    qi_parent = Self::quote_identifier(parent_table),
                ))
                .await?;
        }
        Ok(())
    }

    /// Run `DROP TABLE IF EXISTS … CASCADE` for each table in `tables`.
    /// Shared body of [`Self::drop_catalog_tables`] and
    /// [`Self::drop_branch_partitions`].
    async fn drop_tables_if_exist(
        driver: &impl DbDriver,
        tables: &[String],
    ) -> Result<(), sqlx::Error> {
        for table in tables {
            driver
                .execute_no_result(&format!(
                    "DROP TABLE IF EXISTS {} CASCADE",
                    Self::quote_identifier(table),
                ))
                .await?;
        }
        Ok(())
    }
}

/// Errors from DDL operations on data tables.
#[derive(Debug, thiserror::Error)]
pub enum DataTableError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),

    #[error(transparent)]
    ArrowType(#[from] ArrowTypeError),

    #[error("incompatible type change on column {column}: {old_type} → {new_type}")]
    IncompatibleTypeChange {
        column: String,
        old_type: String,
        new_type: String,
    },

    #[error("primary key column not in arrow_schema: {0}")]
    PrimaryKeyNotInSchema(String),
}

impl PgDialect {
    /// Create per-branch upsert and delete log tables for a data table.
    ///
    /// The delete_log carries `(row_uuid, <pk_cols...>, tx_uuid)` — PK columns
    /// interleave between `row_uuid` and `tx_uuid`, in table-declared PK order,
    /// so `audit_data` renders deletes natively without joining back to
    /// upsert_log.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            table = %table_uuid,
            branch = %branch_uuid,
            field_count = arrow_schema.fields().len(),
            pk_count = primary_keys.len(),
        ),
    )]
    pub async fn create_data_tables(
        driver: &impl DbDriver,
        table_uuid: &Uuid,
        branch_uuid: &Uuid,
        arrow_schema: &Schema,
        primary_keys: &[String],
    ) -> Result<(), DataTableError> {
        let upsert_log = naming::upsert_log_table(table_uuid, branch_uuid);
        let delete_log = naming::delete_log_table(table_uuid, branch_uuid);

        // Create the per-(table, branch) `write_sequence` BEFORE the data logs
        // so each log's `write_seq_num` column can default to its `nextval` —
        // every writer then stamps the ordinal automatically, with no
        // per-writer plumbing.
        //
        // `CACHE 1` (no per-backend caching) is load-bearing: mutations across
        // separate WriteData calls in one tx must order by call sequence, so
        // `write_seq_num` has to be globally monotonic in allocation order.
        // With a cached block per backend, a later call on a pooled connection
        // holding a lower block would draw a SMALLER ordinal than an earlier
        // call — inverting update-then-delete. `nextval` is still lock-free (a
        // brief buffer latch, not the tx-duration row lock a counter-row
        // allocator would hold).
        let write_seq_qi = Self::quote_identifier(&naming::write_sequence(table_uuid, branch_uuid));
        driver
            .execute_no_result(&format!(
                "CREATE SEQUENCE IF NOT EXISTS {write_seq_qi} AS bigint START 0 MINVALUE 0 CACHE 1"
            ))
            .await?;

        let mut user_columns = String::new();
        for field in arrow_schema.fields() {
            let sql_type = Self::arrow_type_to_sql(field.data_type())?;
            let col = Self::quote_identifier(field.name());
            user_columns.push_str(&format!(",\n                    {col} {sql_type}"));
        }

        // PK columns for delete_log, in declared order. Resolved against
        // arrow_schema so the SQL type matches the user table.
        let mut delete_pk_columns = String::new();
        for pk in primary_keys {
            let field = arrow_schema
                .field_with_name(pk)
                .map_err(|_| DataTableError::PrimaryKeyNotInSchema(pk.clone()))?;
            let sql_type = Self::arrow_type_to_sql(field.data_type())?;
            let col = Self::quote_identifier(pk);
            delete_pk_columns.push_str(&format!(",\n                    {col} {sql_type}"));
        }

        // `write_seq_num` is the within-tx mutation ordinal — the secondary key
        // in the merge-on-read version order `(commit_seq_num, write_seq_num)`.
        //
        // `version_uuid` is deterministic = xxh3(row_uuid, tx_uuid) (see
        // naming::version_uuid), so the PRIMARY KEY alone enforces the
        // auditable-store invariant of at most one version per (entity, tx);
        // no separate UNIQUE(row_uuid, tx_uuid) index is needed (ADR 0013).
        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    version_uuid       UUID PRIMARY KEY,
                    row_uuid           UUID NOT NULL,
                    tx_uuid            UUID NOT NULL,
                    write_seq_num        BIGINT NOT NULL DEFAULT nextval('{write_seq_qi}'){user_columns}
                )"#,
                qi = Self::quote_identifier(&upsert_log),
            ))
            .await?;

        driver
            .execute_no_result(&format!(
                r#"CREATE TABLE IF NOT EXISTS {qi} (
                    version_uuid       UUID PRIMARY KEY,
                    row_uuid           UUID NOT NULL{delete_pk_columns},
                    tx_uuid            UUID NOT NULL,
                    write_seq_num        BIGINT NOT NULL DEFAULT nextval('{write_seq_qi}')
                )"#,
                qi = Self::quote_identifier(&delete_log),
            ))
            .await?;

        // Secondary indexes on the hot logs. The `version_uuid` PK only
        // serves idempotent upsert (`ON CONFLICT`); two btrees cover the
        // two read probes:
        //
        // `(tx_uuid, row_uuid)`: the merge-on-read exclusion-set (Query B)
        // and resolve (Query A) joins both probe these logs by `tx_uuid`,
        // and without an index PG seq-scans the whole log — which, for a
        // snapshotted-but-not-yet-purged table, still holds every
        // pre-snapshot row (the ~268ms exclusion-set scan this collapses
        // to ~ms). The trailing `row_uuid` makes Query B's
        // `SELECT row_uuid ... USING (tx_uuid)` an index-only scan.
        //
        // `(row_uuid, tx_uuid)`: serves the ids point-lookup pushdown — the
        // merge `_u`/`_d` sources and the per-arm exclusion scans probe by
        // `WHERE row_uuid IN (...)` below the latest-wins dedup, where
        // `tx_uuid` has the wrong leading column. One btree covers every PK
        // shape, composite included (`row_uuid` derivation is deterministic).
        // It costs measurable write amplification (~7.6ms/op of insert-side
        // maintenance) and pays for itself only on read-heavy OLTP, at ~60ms
        // saved per hot point read.
        //
        // Created at table-creation time only, so log tables predating this
        // keep seq-scanning until recreated. Penca is pre-release, so no
        // backfill is needed today — a one-time index backfill over existing
        // logs is a production-hardening follow-up.
        //
        // The index names are derived from the per-branch log table name,
        // NOT from `table_uuid`: `upsert_log_table`/`delete_log_table` are
        // unique per (table, branch) but every branch shares one PG schema,
        // and a PG index name must be unique per schema. Keying on
        // `table_uuid` alone would make the second branch's
        // `CREATE INDEX IF NOT EXISTS` a silent no-op, leaving
        // all-but-the-first branch unindexed. `idx_tx_<log>` /
        // `idx_row_<log>` stay under the 63-byte NAMEDATALEN cap (log is a
        // 52-char `<row_uuid>_data_{upsert,delete}_log`).
        for log in [&upsert_log, &delete_log] {
            driver
                .execute_no_result(&format!(
                    r#"CREATE INDEX IF NOT EXISTS {qi_idx} ON {qi_tbl} (tx_uuid, row_uuid)"#,
                    qi_idx = Self::quote_identifier(&format!("idx_tx_{log}")),
                    qi_tbl = Self::quote_identifier(log),
                ))
                .await?;
            driver
                .execute_no_result(&format!(
                    r#"CREATE INDEX IF NOT EXISTS {qi_idx} ON {qi_tbl} (row_uuid, tx_uuid)"#,
                    qi_idx = Self::quote_identifier(&format!("idx_row_{log}")),
                    qi_tbl = Self::quote_identifier(log),
                ))
                .await?;
        }

        Ok(())
    }

    /// Drop per-branch data tables (upsert + delete logs).
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(table = %table_uuid, branch = %branch_uuid),
    )]
    pub async fn drop_data_tables(
        driver: &impl DbDriver,
        table_uuid: &Uuid,
        branch_uuid: &Uuid,
    ) -> Result<(), sqlx::Error> {
        let tables = [
            naming::upsert_log_table(table_uuid, branch_uuid),
            naming::delete_log_table(table_uuid, branch_uuid),
        ];
        Self::drop_tables_if_exist(driver, &tables).await?;
        driver
            .execute_no_result(&format!(
                "DROP SEQUENCE IF EXISTS {qi}",
                qi = Self::quote_identifier(&naming::write_sequence(table_uuid, branch_uuid)),
            ))
            .await
    }

    /// Apply additive schema changes to a per-branch data log table.
    ///
    /// For each field in `new_schema` not in `old_schema` (by name),
    /// executes `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`.
    ///
    /// Type changes raise an error — they are not backwards-compatible.
    /// Dropped columns are left as dead columns; the data log accumulates
    /// a superset of all schema versions for time-travel queries.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            table_name = %table_name,
            old_field_count = old_schema.fields().len(),
            new_field_count = new_schema.fields().len(),
        ),
    )]
    pub async fn evolve_data_log_schema(
        driver: &impl DbDriver,
        table_name: &str,
        old_schema: &Schema,
        new_schema: &Schema,
    ) -> Result<(), DataTableError> {
        let old_fields: std::collections::HashSet<&str> = old_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        for field in new_schema.fields() {
            if old_fields.contains(field.name().as_str()) {
                if let Ok(old_field) = old_schema.field_with_name(field.name())
                    && old_field.data_type() != field.data_type()
                {
                    return Err(DataTableError::IncompatibleTypeChange {
                        column: field.name().clone(),
                        old_type: format!("{:?}", old_field.data_type()),
                        new_type: format!("{:?}", field.data_type()),
                    });
                }
                continue;
            }

            let sql_type = Self::arrow_type_to_sql(field.data_type())?;
            let col = Self::quote_identifier(field.name());
            driver
                .execute_no_result(&format!(
                    "ALTER TABLE {qi} ADD COLUMN IF NOT EXISTS {col} {sql_type}",
                    qi = Self::quote_identifier(table_name),
                ))
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};

    #[test]
    fn test_quote_simple_identifier() {
        assert_eq!(PgDialect::quote_identifier("my_table"), "\"my_table\"");
    }

    #[test]
    fn test_quote_identifier_with_embedded_quotes() {
        assert_eq!(
            PgDialect::quote_identifier("table\"name"),
            "\"table\"\"name\""
        );
    }

    #[test]
    fn test_quote_column() {
        assert_eq!(PgDialect::quote_column("t", "my_col"), "t.\"my_col\"");
    }

    #[test]
    fn test_integer_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Int8).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Int16).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Int32).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Int64).unwrap(),
            "BIGINT"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::UInt8).unwrap(),
            "SMALLINT"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::UInt16).unwrap(),
            "INTEGER"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::UInt32).unwrap(),
            "BIGINT"
        );
        // NUMERIC, not BIGINT: u64::MAX exceeds i64::MAX.
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::UInt64).unwrap(),
            "NUMERIC"
        );
    }

    #[test]
    fn test_float_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Float16).unwrap(),
            "REAL"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Float32).unwrap(),
            "REAL"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Float64).unwrap(),
            "DOUBLE PRECISION"
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Utf8).unwrap(),
            "TEXT"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::LargeUtf8).unwrap(),
            "TEXT"
        );
    }

    #[test]
    fn test_binary_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Binary).unwrap(),
            "BYTEA"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::LargeBinary).unwrap(),
            "BYTEA"
        );
    }

    #[test]
    fn test_boolean() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Boolean).unwrap(),
            "BOOLEAN"
        );
    }

    #[test]
    fn test_date_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Date32).unwrap(),
            "DATE"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Date64).unwrap(),
            "DATE"
        );
    }

    #[test]
    fn test_timestamp_without_tz() {
        let dt = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert_eq!(PgDialect::arrow_type_to_sql(&dt).unwrap(), "TIMESTAMP");
    }

    #[test]
    fn test_timestamp_with_tz() {
        let dt = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(PgDialect::arrow_type_to_sql(&dt).unwrap(), "TIMESTAMPTZ");
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Decimal128(18, 6)).unwrap(),
            "NUMERIC(18, 6)"
        );
    }

    #[test]
    fn test_list_type() {
        let inner = Field::new("item", DataType::Int32, true);
        let dt = DataType::List(inner.into());
        assert_eq!(PgDialect::arrow_type_to_sql(&dt).unwrap(), "INTEGER[]");
    }

    #[test]
    fn test_time_types() {
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Time32(TimeUnit::Millisecond)).unwrap(),
            "TIME"
        );
        assert_eq!(
            PgDialect::arrow_type_to_sql(&DataType::Time64(TimeUnit::Microsecond)).unwrap(),
            "TIME"
        );
    }

    #[test]
    fn test_fixed_size_list_type() {
        let inner = Field::new("item", DataType::Int32, true);
        let dt = DataType::FixedSizeList(inner.into(), 3);
        assert_eq!(PgDialect::arrow_type_to_sql(&dt).unwrap(), "INTEGER[]");
    }

    #[test]
    fn test_unsupported_type() {
        assert!(PgDialect::arrow_type_to_sql(&DataType::Null).is_err());
    }

    #[test]
    fn test_microsecond_epoch() {
        let expr = PgDialect::microsecond_epoch();
        assert!(expr.contains("EPOCH"));
        assert!(expr.contains("1000000"));
    }
}
