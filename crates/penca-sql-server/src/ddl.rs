//! SQL DDL → `WriteService::Create{Schema,Table}` translator for the
//! `CREATE SCHEMA` / `CREATE TABLE` slice of the Flight SQL DDL surface
//! (CHA-172 auto-commit; CHA-345 transactional).
//!
//! Counterpart to [`crate::dml`]: `dml` translates INSERT/UPDATE/DELETE
//! into `WriteData{,AndCommitTx}` against the data path; `ddl`
//! translates `CREATE SCHEMA` / `CREATE TABLE` into the corresponding
//! `WriteService` DDL RPCs. The DDL surface is narrower by design (only
//! the two CREATE variants that unblock first-time SQL-client UX);
//! everything else stays rejected by [`crate::gateway::classify`].
//!
//! CHA-345: the same two CREATE variants now work inside a Flight SQL
//! `BEGIN`/`COMMIT` block. [`ddl_tx_identity`] threads the snapshot's
//! `open_tx_uuid` into the request — `None` auto-commits, `Some` writes
//! the metadata row under the open tx (the server honors it via
//! `resolve_or_auto_commit_tx`, CHA-164). The architectural blocker
//! ADR 0010 cited was retired once the open tx became reachable from
//! `PencaSchemaProvider::table` via the `ConnScope` cell.
//!
//! Author/comment are carried (empty-string placeholder) only on the
//! auto-commit path; in-tx they are omitted (the open tx carries its
//! identity, recorded at `BeginTx`). The audit-identity follow-up is
//! tracked in CHA-159 (gRPC interceptors) and CHA-160 (Flight SQL
//! session properties). See [`ddl_tx_identity`].

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::sql::sqlparser::ast::{
    ColumnOption, CreateTable, CreateTableOptions, Expr, HiveDistributionStyle, ObjectName,
    ObjectNamePart, SchemaName, SqlOption, TableConstraint,
};
use penca_proto::external::v1::write_service_client::WriteServiceClient;
use penca_proto::external::v1::{CreateSchemaRequest, CreateTableRequest};
use tonic::Status;
use tonic::transport::Channel;

use crate::session::SessionSnapshot;
use crate::sql_type::sql_type_to_arrow;

/// Inline-fields of [`SqlStatement::CreateSchema`] bundled as a struct
/// so the DDL translator path doesn't thread 6 separate args across
/// every layer (and the gateway dispatcher doesn't carry a 6-binding
/// destructure inline). Built once at the classifier boundary in
/// [`crate::gateway::classify`] when it routes a `CREATE SCHEMA`
/// statement to [`DdlKind::CreateSchema`].
#[derive(Debug)]
pub(crate) struct CreateSchemaArgs {
    pub(crate) schema_name: SchemaName,
    pub(crate) if_not_exists: bool,
    pub(crate) with: Option<Vec<SqlOption>>,
    pub(crate) options: Option<Vec<SqlOption>>,
    pub(crate) default_collate_spec: Option<Expr>,
    pub(crate) clone: Option<ObjectName>,
}

/// The typed two-variant subspace of `SqlStatement` that
/// [`crate::gateway::classify`] routes to the DDL translator. Replaces
/// the previous `Classified::Ddl(SqlStatement)` which was untyped over
/// the whole AST and forced an `unreachable!()` branch at the
/// dispatcher.
/// sqlparser's `CreateTable` is ~2.3KB (50+ fields covering every
/// vendor-specific clause) and `CreateSchemaArgs` is ~0.5KB. Box both
/// variants to keep the enum value-passing cheap (one pointer per
/// variant) — DDL is auto-commit-only and goes through a gRPC
/// round-trip, so a single heap allocation per dispatch is
/// negligible vs the inline cost of moving kilobytes by value
/// through the stack at every match arm.
#[derive(Debug)]
pub(crate) enum DdlKind {
    CreateTable(Box<CreateTable>),
    CreateSchema(Box<CreateSchemaArgs>),
}

/// Aggregator for the DDL translator path — mirrors [`crate::dml::execute`]
/// for the data path. Matches on the [`DdlKind`] variant and delegates to
/// the per-verb helper. Returns `0` rows-affected on success (DDL has no
/// row count).
///
/// Field set mirrors `dml::execute`'s for cross-span filterability:
/// `catalog_uuid` + `branch_uuid` always populated from the snapshot;
/// `kind` populated dynamically with the verb (`"create_table"` /
/// `"create_schema"` — analogous to `dml`'s `"insert"`/`"update"`/`"delete"`).
/// `schema` / `table` record the target identifier when known; both
/// stay `Empty` until the per-variant arm runs. `tx_uuid` records the
/// open transaction (`<none>` for auto-commit) — CHA-345 made in-tx DDL
/// reachable here, so it's a meaningful discriminator now.
#[tracing::instrument(
    skip_all,
    fields(
        catalog_uuid = %snapshot.catalog_uuid,
        branch_uuid = %snapshot.branch_uuid,
        tx_uuid = snapshot.open_tx_uuid.as_deref().unwrap_or("<none>"),
        kind = tracing::field::Empty,
        schema = tracing::field::Empty,
        table = tracing::field::Empty,
    ),
    err,
)]
pub(crate) async fn execute(
    write_channel: &Channel,
    snapshot: &SessionSnapshot,
    default_schema: &str,
    kind: DdlKind,
) -> Result<i64, Status> {
    match kind {
        DdlKind::CreateTable(ct) => {
            let span = tracing::Span::current();
            span.record("kind", "create_table");
            // `ct.name` is a (possibly multi-part) ObjectName; use Display.
            span.record("table", tracing::field::display(&ct.name));
            execute_create_table(write_channel, snapshot, default_schema, *ct).await
        }
        DdlKind::CreateSchema(args) => {
            let span = tracing::Span::current();
            span.record("kind", "create_schema");
            // SchemaName impls Display — captures Simple / AUTHORIZATION
            // variants the same way the user typed them.
            span.record("schema", tracing::field::display(&args.schema_name));
            execute_create_schema(write_channel, snapshot, *args).await
        }
    }
}

/// Translate a parsed `CREATE SCHEMA <name>` into a
/// [`CreateSchemaRequest`] and dispatch it to the WriteService. Returns
/// `0` rows-affected on success (DDL has no row count).
///
/// Rejects schema-extensions Penca doesn't model today: `IF NOT
/// EXISTS`, `WITH (...)` properties, `OPTIONS(...)`, `DEFAULT COLLATE`,
/// `CLONE`, `AUTHORIZATION` (any variant), and dotted schema names
/// (schemas have a single-component name; cross-catalog creates go via
/// the gRPC `CreateCatalog` RPC, intentionally not exposed via Flight
/// SQL).
async fn execute_create_schema(
    write_channel: &Channel,
    snapshot: &SessionSnapshot,
    args: CreateSchemaArgs,
) -> Result<i64, Status> {
    let request = build_create_schema_request(snapshot, args)?;
    WriteServiceClient::new(write_channel.clone())
        .create_schema(request)
        .await?;
    Ok(0)
}

/// Pure request-construction helper extracted from
/// [`execute_create_schema`] so the validation/translation logic is
/// unit-testable without standing up a WriteService mock.
fn build_create_schema_request(
    snapshot: &SessionSnapshot,
    args: CreateSchemaArgs,
) -> Result<CreateSchemaRequest, Status> {
    let CreateSchemaArgs {
        schema_name,
        if_not_exists,
        with,
        options,
        default_collate_spec,
        clone,
    } = args;

    if if_not_exists {
        return Err(Status::unimplemented(
            "CREATE SCHEMA IF NOT EXISTS is not supported (see CHA-172)",
        ));
    }
    if with.is_some() {
        return Err(Status::unimplemented(
            "CREATE SCHEMA WITH (...) properties are not supported (see CHA-172)",
        ));
    }
    if options.is_some() {
        return Err(Status::unimplemented(
            "CREATE SCHEMA OPTIONS(...) is not supported (see CHA-172)",
        ));
    }
    if default_collate_spec.is_some() {
        return Err(Status::unimplemented(
            "CREATE SCHEMA DEFAULT COLLATE is not supported (see CHA-172)",
        ));
    }
    if clone.is_some() {
        return Err(Status::unimplemented(
            "CREATE SCHEMA CLONE is not supported (see CHA-172)",
        ));
    }

    let name = match schema_name {
        SchemaName::Simple(object_name) => extract_simple_schema_name(&object_name)?,
        SchemaName::UnnamedAuthorization(_) | SchemaName::NamedAuthorization(_, _) => {
            return Err(Status::unimplemented(
                "CREATE SCHEMA AUTHORIZATION is not supported (see CHA-172)",
            ));
        }
    };

    let (tx_uuid, author, comment) = ddl_tx_identity(snapshot);
    Ok(CreateSchemaRequest {
        catalog_uuid: Some(snapshot.catalog_uuid.clone()),
        catalog_name: None,
        schema_name: name,
        description: String::new(),
        default_retention_config: None,
        tx_uuid,
        branch_uuid: Some(snapshot.branch_uuid.clone()),
        branch_name: None,
        author,
        comment,
    })
}

/// CHA-345: resolve the `(tx_uuid, author, comment)` triple a
/// WriteService DDL request carries, from the session snapshot.
///
/// The server's `resolve_or_auto_commit_tx` couples the three: it
/// *requires* author + comment for an auto-commit DDL (tx_uuid unset)
/// and *forbids* them in-tx (the open tx already carries its identity,
/// recorded at `BeginTx`). So they move together:
/// * in-tx → `(Some(tx), None, None)`
/// * auto-commit → `(None, Some(""), Some(""))`
///
/// Empty author/comment are the CHA-159 / CHA-160 placeholder until
/// audit identity is wired through; they satisfy the auto-commit
/// NOT-NULL requirement without claiming an identity.
fn ddl_tx_identity(snapshot: &SessionSnapshot) -> (Option<String>, Option<String>, Option<String>) {
    match snapshot.open_tx_uuid.clone() {
        Some(tx) => (Some(tx), None, None),
        None => (None, Some(String::new()), Some(String::new())),
    }
}

/// Validate that a `SchemaName::Simple(_)` ObjectName is exactly one
/// plain identifier and return it. Splits the two failure modes —
/// multi-part name vs function-style identifier — into per-variant
/// error wording (the original `single_part_name` collapsed them into
/// a misleading "cross-catalog" message even for `CREATE SCHEMA
/// my_func()`). Wording mirrors `split_create_table_name`'s
/// function-style rejection on the CREATE TABLE side.
fn extract_simple_schema_name(name: &ObjectName) -> Result<String, Status> {
    if name.0.len() != 1 {
        return Err(Status::invalid_argument(
            "CREATE SCHEMA name must be a single identifier (cross-catalog \
             schema creation goes via the gRPC WriteService API)",
        ));
    }
    match &name.0[0] {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        ObjectNamePart::Function(_) => Err(Status::invalid_argument(
            "function-style identifiers are not supported in CREATE SCHEMA target names",
        )),
    }
}

/// Translate a parsed `CREATE TABLE …` into a [`CreateTableRequest`]
/// and dispatch it to the WriteService. Returns `0` rows-affected on
/// success.
///
/// Rejects every CREATE-TABLE modifier Penca doesn't model on the
/// Flight SQL surface today: `IF NOT EXISTS`, `OR REPLACE`, `TEMPORARY`,
/// `EXTERNAL`, `ICEBERG`, `TRANSIENT`, `VOLATILE`, `DYNAMIC`,
/// `GLOBAL/LOCAL`, `CTAS` (`AS SELECT`), `LIKE`, `CLONE`,
/// `LOCATION`, `WITHOUT ROWID`. Per-column `DEFAULT` clauses also
/// reject (Penca's table metadata has no defaults column). PK is
/// required — Penca derives `row_uuid_for_pk` from the primary key,
/// so PK-less tables are structurally unsupported. 3-part table names
/// are rejected because cross-catalog CREATE TABLE goes via the gRPC
/// WriteService API directly.
///
/// `default_schema` is consulted when the parsed name is a bare table
/// identifier with no schema qualifier — matches DataFusion's
/// `catalog.default_schema` resolution that the DML path also uses
/// (CHA-119 search_path).
async fn execute_create_table(
    write_channel: &Channel,
    snapshot: &SessionSnapshot,
    default_schema: &str,
    stmt: CreateTable,
) -> Result<i64, Status> {
    let request = build_create_table_request(snapshot, default_schema, stmt)?;
    WriteServiceClient::new(write_channel.clone())
        .create_table(request)
        .await?;
    Ok(0)
}

/// Pure request-construction helper extracted from
/// [`execute_create_table`] for unit-testability — same shape as
/// [`build_create_schema_request`].
fn build_create_table_request(
    snapshot: &SessionSnapshot,
    default_schema: &str,
    stmt: CreateTable,
) -> Result<CreateTableRequest, Status> {
    reject_unsupported_table_modifiers(&stmt)?;

    let (schema, table_name) = split_create_table_name(&stmt.name, default_schema)?;
    let primary_keys = extract_primary_keys(&stmt)?;
    let arrow_schema = build_arrow_schema(&stmt)?;
    let arrow_schema_bytes = encode_arrow_schema_ipc(&arrow_schema)?;

    let (tx_uuid, author, comment) = ddl_tx_identity(snapshot);
    Ok(CreateTableRequest {
        catalog_uuid: Some(snapshot.catalog_uuid.clone()),
        catalog_name: None,
        schema_uuid: None,
        schema_name: Some(schema),
        branch_uuid: Some(snapshot.branch_uuid.clone()),
        branch_name: None,
        table_name,
        arrow_schema: arrow_schema_bytes,
        primary_keys,
        partition_keys: Vec::new(),
        clustering_keys: Vec::new(),
        description: String::new(),
        retention_config: None,
        tx_uuid,
        author,
        comment,
        // CHA-455: SQL-level CREATE INDEX is out of scope; the SQL DDL
        // path defines no inline indexes.
        indexes: Vec::new(),
    })
}

/// Reject every CREATE TABLE modifier outside the auto-commit slice
/// this ticket scoped. Each rejection is `Status::unimplemented` —
/// sqlparser parses ~50 vendor-specific clauses (Postgres `INHERITS`,
/// SQLite `STRICT`, ClickHouse `ORDER BY`/`PRIMARY KEY`/`ON CLUSTER`,
/// BigQuery `PARTITION BY`/`CLUSTER BY`, Hive `clustered_by`/`STORED
/// AS`/`SERDE`, Snowflake `COPY GRANTS`/`DATA_RETENTION_TIME_IN_DAYS`/
/// `TARGET_LAG`/etc., Iceberg-table-specific clauses, ...) that
/// Penca's table metadata model does not carry. Each is named
/// individually so silent acceptance is impossible — sqlparser's
/// `CreateTable` is a struct (not an enum), so adding a new field
/// upstream won't break this match; exhaustiveness here is human-
/// audit-only.
///
/// Fields *not* in this list because they're meaningful inputs to the
/// translator: `name` (consumed by `split_create_table_name`),
/// `columns` (consumed by `build_arrow_schema`), `constraints`
/// (consumed by `extract_primary_keys`). Adding a new field to that
/// "consumed" set requires extending this rejection list to remove
/// it from the silent-drop surface.
fn reject_unsupported_table_modifiers(stmt: &CreateTable) -> Result<(), Status> {
    // Base modifier flags (every dialect).
    reject_modifier(stmt.if_not_exists, "CREATE TABLE IF NOT EXISTS")?;
    reject_modifier(stmt.or_replace, "CREATE OR REPLACE TABLE")?;
    reject_modifier(stmt.temporary, "CREATE TEMPORARY TABLE")?;
    reject_modifier(stmt.external, "CREATE EXTERNAL TABLE")?;
    reject_modifier(stmt.iceberg, "CREATE ICEBERG TABLE")?;
    reject_modifier(stmt.transient, "CREATE TRANSIENT TABLE")?;
    reject_modifier(stmt.volatile, "CREATE VOLATILE TABLE")?;
    reject_modifier(stmt.dynamic, "CREATE DYNAMIC TABLE")?;
    reject_modifier(stmt.global.is_some(), "CREATE GLOBAL/LOCAL TABLE")?;
    reject_modifier(stmt.query.is_some(), "CREATE TABLE AS SELECT (CTAS)")?;
    reject_modifier(stmt.like.is_some(), "CREATE TABLE LIKE")?;
    reject_modifier(stmt.clone.is_some(), "CREATE TABLE CLONE")?;
    reject_modifier(stmt.location.is_some(), "CREATE TABLE LOCATION")?;
    reject_modifier(stmt.without_rowid, "CREATE TABLE … WITHOUT ROWID")?;
    reject_modifier(
        stmt.version.is_some(),
        "CREATE TABLE … FOR SYSTEM_TIME AS OF",
    )?;
    reject_modifier(stmt.comment.is_some(), "Hive-style trailing COMMENT clause")?;
    reject_modifier(stmt.on_commit.is_some(), "CREATE TABLE … ON COMMIT")?;

    // Hive: distribution + per-format clauses + table-options.
    reject_modifier(
        !matches!(stmt.hive_distribution, HiveDistributionStyle::NONE),
        "Hive DISTRIBUTION clause (PARTITIONED/SKEWED BY)",
    )?;
    // `hive_formats` is `Some(HiveFormat::default())` even on a vanilla
    // `CREATE TABLE` (sqlparser allocates the empty sentinel
    // unconditionally), so reject only when an inner Hive sub-clause
    // is actually populated.
    let hive_formats_present = stmt.hive_formats.as_ref().is_some_and(|hf| {
        hf.row_format.is_some()
            || hf.serde_properties.is_some()
            || hf.storage.is_some()
            || hf.location.is_some()
    });
    reject_modifier(
        hive_formats_present,
        "Hive table format clauses (STORED AS / ROW FORMAT / SERDE)",
    )?;
    reject_modifier(stmt.file_format.is_some(), "CREATE TABLE … FILE FORMAT")?;
    reject_modifier(
        !matches!(stmt.table_options, CreateTableOptions::None),
        "CREATE TABLE WITH/OPTIONS/Plain table-options clauses",
    )?;
    reject_modifier(stmt.clustered_by.is_some(), "Hive CLUSTERED BY")?;

    // ClickHouse.
    reject_modifier(stmt.on_cluster.is_some(), "CREATE TABLE … ON CLUSTER")?;
    reject_modifier(
        stmt.primary_key.is_some(),
        "ClickHouse trailing PRIMARY KEY clause (use the column-level / table-constraint form instead)",
    )?;
    reject_modifier(
        stmt.order_by.is_some(),
        "CREATE TABLE … ORDER BY (ClickHouse)",
    )?;

    // BigQuery / Postgres / SQLite.
    reject_modifier(stmt.partition_by.is_some(), "CREATE TABLE … PARTITION BY")?;
    reject_modifier(stmt.cluster_by.is_some(), "CREATE TABLE … CLUSTER BY")?;
    reject_modifier(
        stmt.inherits.is_some(),
        "CREATE TABLE … INHERITS (Postgres)",
    )?;
    reject_modifier(stmt.strict, "CREATE TABLE … STRICT (SQLite)")?;

    // Snowflake feature flags.
    reject_modifier(stmt.copy_grants, "Snowflake COPY GRANTS")?;
    reject_modifier(
        stmt.enable_schema_evolution.is_some(),
        "Snowflake ENABLE_SCHEMA_EVOLUTION",
    )?;
    reject_modifier(stmt.change_tracking.is_some(), "Snowflake CHANGE_TRACKING")?;
    reject_modifier(
        stmt.data_retention_time_in_days.is_some(),
        "Snowflake DATA_RETENTION_TIME_IN_DAYS",
    )?;
    reject_modifier(
        stmt.max_data_extension_time_in_days.is_some(),
        "Snowflake MAX_DATA_EXTENSION_TIME_IN_DAYS",
    )?;
    reject_modifier(
        stmt.default_ddl_collation.is_some(),
        "Snowflake DEFAULT_DDL_COLLATION",
    )?;
    reject_modifier(
        stmt.with_aggregation_policy.is_some(),
        "Snowflake WITH AGGREGATION POLICY",
    )?;
    reject_modifier(
        stmt.with_row_access_policy.is_some(),
        "Snowflake WITH ROW ACCESS POLICY",
    )?;
    reject_modifier(stmt.with_tags.is_some(), "Snowflake WITH TAG")?;

    // Snowflake Iceberg table.
    reject_modifier(
        stmt.external_volume.is_some(),
        "Snowflake Iceberg EXTERNAL_VOLUME",
    )?;
    reject_modifier(
        stmt.base_location.is_some(),
        "Snowflake Iceberg BASE_LOCATION",
    )?;
    reject_modifier(
        stmt.catalog.is_some(),
        "Snowflake Iceberg CATALOG clause (distinct from Penca's catalog notion)",
    )?;
    reject_modifier(
        stmt.catalog_sync.is_some(),
        "Snowflake Iceberg CATALOG_SYNC",
    )?;
    reject_modifier(
        stmt.storage_serialization_policy.is_some(),
        "Snowflake Iceberg STORAGE_SERIALIZATION_POLICY",
    )?;

    // Snowflake dynamic table.
    reject_modifier(
        stmt.target_lag.is_some(),
        "Snowflake dynamic table TARGET_LAG",
    )?;
    reject_modifier(
        stmt.warehouse.is_some(),
        "Snowflake dynamic table WAREHOUSE",
    )?;
    reject_modifier(
        stmt.refresh_mode.is_some(),
        "Snowflake dynamic table REFRESH_MODE",
    )?;
    reject_modifier(
        stmt.initialize.is_some(),
        "Snowflake dynamic table INITIALIZE",
    )?;
    reject_modifier(stmt.require_user, "Snowflake dynamic table REQUIRE USER")?;

    Ok(())
}

/// Tiny helper for [`reject_unsupported_table_modifiers`]: collapses
/// the "if predicate then return unimplemented status" pattern that
/// each per-clause check uses, so the function reads as a one-line
/// table of `field → keyword` instead of N copies of the same 3-line
/// `if/return Err` boilerplate.
fn reject_modifier(present: bool, keyword: &str) -> Result<(), Status> {
    if present {
        return Err(Status::unimplemented(format!(
            "{keyword} is not supported (see CHA-172)"
        )));
    }
    Ok(())
}

/// Split a parsed table name into `(schema, table)`. Bare table →
/// `default_schema`; `schema.table` → explicit; 3-part name rejected
/// (cross-catalog CREATE TABLE goes via the gRPC WriteService API).
fn split_create_table_name(
    name: &ObjectName,
    default_schema: &str,
) -> Result<(String, String), Status> {
    let parts: Vec<&str> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(ident) => Ok(ident.value.as_str()),
            ObjectNamePart::Function(_) => Err(Status::invalid_argument(
                "function-style identifiers are not supported in CREATE TABLE target names",
            )),
        })
        .collect::<Result<_, _>>()?;
    match parts.as_slice() {
        [t] => Ok((default_schema.to_string(), (*t).to_string())),
        [s, t] => Ok(((*s).to_string(), (*t).to_string())),
        [_, _, _] => Err(Status::invalid_argument(
            "CREATE TABLE with a 3-part name (catalog.schema.table) is not supported on \
             Flight SQL — cross-catalog table creation goes via the gRPC WriteService API",
        )),
        other => Err(Status::invalid_argument(format!(
            "expected table | schema.table; got {}-part name",
            other.len()
        ))),
    }
}

/// Extract the primary-key column names from both per-column
/// `PRIMARY KEY` options and standalone `PRIMARY KEY(...)` constraints.
///
/// This is the extraction half only — empty / duplicate / undeclared
/// PK validation lives one layer down at the API boundary in
/// `penca-api::write::validate_create_table_primary_keys`, so both
/// Flight SQL and direct gRPC callers see identical rejection wording
/// (see CHA-172). Returns `Ok(vec![])` when no PK is declared and
/// `Ok(["id", "id"])` for duplicates — both surface clean errors at
/// the API-layer validator, not here.
///
/// The only error this function itself produces is
/// `Status::invalid_argument` for a non-identifier `PRIMARY KEY`
/// expression — that's sqlparser-AST-syntactic (no gRPC analog) so it
/// stays SQL-side.
fn extract_primary_keys(stmt: &CreateTable) -> Result<Vec<String>, Status> {
    let mut pks: Vec<String> = Vec::new();

    for col in &stmt.columns {
        for opt in &col.options {
            if let ColumnOption::Unique {
                is_primary: true, ..
            } = opt.option
            {
                pks.push(col.name.value.clone());
            }
        }
    }

    for constraint in &stmt.constraints {
        if let TableConstraint::PrimaryKey { columns, .. } = constraint {
            for index_col in columns {
                let ident = match &index_col.column.expr {
                    Expr::Identifier(id) => id.value.clone(),
                    other => {
                        return Err(Status::invalid_argument(format!(
                            "PRIMARY KEY column must be a bare identifier; got `{other}` \
                             (see CHA-172)"
                        )));
                    }
                };
                pks.push(ident);
            }
        }
    }

    // Empty / duplicate / undeclared PK checks live one layer down in
    // `penca-api::write::create_table` — `CreateTableRequest` is the
    // boundary the gRPC and Flight SQL paths converge on, and both
    // wire paths share the same failure modes ("primary_keys is
    // empty" / "primary_keys contains duplicates" / "primary_keys
    // references column missing from arrow_schema"). Duplicating
    // those checks here as a defensive pre-check would silently
    // diverge the rejection wording between the two wire paths if
    // the two copies drift over time — keep it as one check at the
    // convergence point.
    Ok(pks)
}

/// Build the Arrow schema from the parsed `ColumnDef` list. Per-column
/// `DEFAULT` clauses reject; `NOT NULL` flips nullability from the
/// SQL-convention default (nullable=true).
fn build_arrow_schema(stmt: &CreateTable) -> Result<Schema, Status> {
    let mut fields: Vec<Field> = Vec::with_capacity(stmt.columns.len());
    for col in &stmt.columns {
        let mut nullable = true;
        for opt in &col.options {
            match &opt.option {
                ColumnOption::Default(_) => {
                    return Err(Status::invalid_argument(format!(
                        "column `{}`: DEFAULT clauses are not supported (see CHA-172)",
                        col.name.value
                    )));
                }
                ColumnOption::NotNull => nullable = false,
                // Inline PK (`Unique { is_primary: true }`) doesn't affect
                // nullability per SQL convention — the PK constraint is
                // surfaced via `primary_keys` on the wire request.
                // Other options (`Null`, `Unique { is_primary: false }`,
                // collations, comments, etc.) are accepted as no-ops on
                // Penca's metadata model.
                _ => {}
            }
        }
        let arrow_type = sql_type_to_arrow(&col.data_type)?;
        fields.push(Field::new(col.name.value.clone(), arrow_type, nullable));
    }
    Ok(Schema::new(fields))
}

/// IPC-encode just the schema header (no batches). The WriteService's
/// `CreateTableRequest.arrow_schema` field carries this byte string;
/// it round-trips through `try_schema_from_ipc_buffer` on the read side
/// (see `dml::decode_arrow_schema`).
fn encode_arrow_schema_ipc(schema: &Schema) -> Result<Vec<u8>, Status> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    let schema_arc = Arc::new(schema.clone());
    let mut writer = StreamWriter::try_new_with_options(&mut buf, &schema_arc, options)
        .map_err(|e| Status::internal(format!("failed to open Arrow IPC writer: {e}")))?;
    writer
        .finish()
        .map_err(|e| Status::internal(format!("failed to finish Arrow IPC stream: {e}")))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ipc::convert::try_schema_from_ipc_buffer;
    use datafusion::sql::parser::{DFParser, Statement as DFStatement};
    use datafusion::sql::sqlparser::ast::{
        Ident, ObjectNamePartFunction, Statement as SqlStatement,
    };

    fn snapshot() -> SessionSnapshot {
        SessionSnapshot::for_test(
            "public",
            "00000000-0000-0000-0000-0000000000ca",
            "00000000-0000-0000-0000-0000000000b1",
            "main",
            None,
        )
    }

    /// In-transaction variant of [`snapshot`] — same identifiers, but
    /// `open_tx_uuid = Some(tx)`. CHA-345: transactional DDL via Flight
    /// SQL threads the open tx into the WriteService request so the
    /// metadata row is written under the tx (invisible until COMMIT).
    fn snapshot_in_tx(tx_uuid: &str) -> SessionSnapshot {
        SessionSnapshot::for_test(
            "public",
            "00000000-0000-0000-0000-0000000000ca",
            "00000000-0000-0000-0000-0000000000b1",
            "main",
            Some(tx_uuid.to_string()),
        )
    }

    /// Parse `CREATE SCHEMA …` and return the bundled
    /// [`CreateSchemaArgs`] the translator path now takes.
    fn parse_create_schema(sql: &str) -> CreateSchemaArgs {
        let mut stmts = DFParser::parse_sql(sql).expect("parse");
        match stmts.pop_front().expect("non-empty") {
            DFStatement::Statement(boxed) => match *boxed {
                SqlStatement::CreateSchema {
                    schema_name,
                    if_not_exists,
                    with,
                    options,
                    default_collate_spec,
                    clone,
                } => CreateSchemaArgs {
                    schema_name,
                    if_not_exists,
                    with,
                    options,
                    default_collate_spec,
                    clone,
                },
                other => panic!("expected CreateSchema, got {other:?}"),
            },
            other => panic!("expected SQL statement, got {other:?}"),
        }
    }

    fn build_from(sql: &str) -> Result<CreateSchemaRequest, Status> {
        build_create_schema_request(&snapshot(), parse_create_schema(sql))
    }

    #[test]
    fn simple_create_schema_carries_snapshot_identifiers() {
        let req = build_from("CREATE SCHEMA myapp").unwrap();
        assert_eq!(
            req.catalog_uuid.as_deref(),
            Some("00000000-0000-0000-0000-0000000000ca")
        );
        assert_eq!(
            req.branch_uuid.as_deref(),
            Some("00000000-0000-0000-0000-0000000000b1")
        );
        assert_eq!(req.schema_name, "myapp");
        // tx_uuid stays None — auto-commit (the in-tx case is pinned by
        // create_schema_in_tx_threads_tx_uuid).
        assert!(req.tx_uuid.is_none());
        // Empty author/comment satisfy the NOT NULL constraint without
        // claiming a specific identity; the audit identity TODO is
        // tracked separately (CHA-159 / CHA-160).
        assert_eq!(req.author.as_deref(), Some(""));
        assert_eq!(req.comment.as_deref(), Some(""));
    }

    /// CHA-345 — transactional `CREATE SCHEMA` threads the open
    /// `tx_uuid` into the WriteService request. Auto-commit (no open tx)
    /// still sends `tx_uuid: None` (pinned by
    /// `simple_create_schema_carries_snapshot_identifiers`).
    #[test]
    fn create_schema_in_tx_threads_tx_uuid() {
        let req = build_create_schema_request(
            &snapshot_in_tx("tx-1"),
            parse_create_schema("CREATE SCHEMA myapp"),
        )
        .unwrap();
        assert_eq!(
            req.tx_uuid.as_deref(),
            Some("tx-1"),
            "in-tx CREATE SCHEMA must thread the open tx_uuid"
        );
        // Identifiers unchanged from the auto-commit path.
        assert_eq!(req.schema_name, "myapp");
        assert_eq!(
            req.catalog_uuid.as_deref(),
            Some("00000000-0000-0000-0000-0000000000ca")
        );
    }

    #[test]
    fn create_schema_if_not_exists_rejects_unimplemented() {
        let err = build_from("CREATE SCHEMA IF NOT EXISTS myapp").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("IF NOT EXISTS"), "{}", err.message());
    }

    #[test]
    fn create_schema_authorization_rejects_unimplemented() {
        let err = build_from("CREATE SCHEMA AUTHORIZATION alice").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("AUTHORIZATION"), "{}", err.message());
    }

    #[test]
    fn create_schema_named_authorization_rejects_unimplemented() {
        let err = build_from("CREATE SCHEMA myapp AUTHORIZATION alice").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("AUTHORIZATION"), "{}", err.message());
    }

    #[test]
    fn create_schema_dotted_name_rejects_invalid_argument() {
        let err = build_from("CREATE SCHEMA other_catalog.myapp").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("single identifier"),
            "{}",
            err.message()
        );
        // Negative pin: this is the multi-part-name branch; the
        // function-style branch below must not collapse into the
        // cross-catalog wording.
        assert!(
            !err.message().contains("function-style"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn create_schema_function_style_name_rejects_with_function_specific_wording() {
        // CHA-172 review fix: `extract_simple_schema_name`'s
        // function-style branch fires when an `ObjectName` carries an
        // `ObjectNamePart::Function(_)`. DFParser does not currently
        // route any `CREATE SCHEMA` syntax through this variant —
        // `my_func()` errors at parse time — so the branch is exercised
        // only by hand-constructing the AST value below. The test pins
        // the per-case wording so a future sqlparser version that
        // starts routing function-style names through this path can't
        // silently regress into the old misleading "cross-catalog"
        // collapse.
        let function_name = ObjectName(vec![ObjectNamePart::Function(ObjectNamePartFunction {
            name: Ident::new("my_func"),
            args: vec![],
        })]);
        let err = extract_simple_schema_name(&function_name).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("function-style"),
            "{}",
            err.message()
        );
        // Negative pin: cross-catalog wording must not surface for
        // the function-style case (the discriminator the review finding
        // called out).
        assert!(
            !err.message().contains("cross-catalog"),
            "{}",
            err.message()
        );
    }

    fn parse_create_table(sql: &str) -> CreateTable {
        let mut stmts = DFParser::parse_sql(sql).expect("parse");
        match stmts.pop_front().expect("non-empty") {
            DFStatement::Statement(boxed) => match *boxed {
                SqlStatement::CreateTable(ct) => ct,
                other => panic!("expected CreateTable, got {other:?}"),
            },
            other => panic!("expected SQL statement, got {other:?}"),
        }
    }

    fn build_table_from(sql: &str) -> Result<CreateTableRequest, Status> {
        build_create_table_request(&snapshot(), "public", parse_create_table(sql))
    }

    #[test]
    fn create_table_happy_path_carries_snapshot_identifiers_and_pk() {
        let req = build_table_from(
            "CREATE TABLE myapp.users (id BIGINT NOT NULL, name VARCHAR(64), PRIMARY KEY(id))",
        )
        .unwrap();
        assert_eq!(
            req.catalog_uuid.as_deref(),
            Some("00000000-0000-0000-0000-0000000000ca")
        );
        assert_eq!(
            req.branch_uuid.as_deref(),
            Some("00000000-0000-0000-0000-0000000000b1")
        );
        assert_eq!(req.schema_name.as_deref(), Some("myapp"));
        assert_eq!(req.table_name, "users");
        assert_eq!(req.primary_keys, vec!["id"]);
        assert!(
            req.tx_uuid.is_none(),
            "tx_uuid must stay None (auto-commit)"
        );
        assert_eq!(req.author.as_deref(), Some(""));
        assert_eq!(req.comment.as_deref(), Some(""));

        // Round-trip the arrow_schema bytes to assert per-field shape.
        let decoded = try_schema_from_ipc_buffer(&req.arrow_schema).unwrap();
        let fields: Vec<_> = decoded
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        assert_eq!(
            fields,
            vec![
                (
                    "id".to_string(),
                    arrow::datatypes::DataType::Int64,
                    false, // NOT NULL → non-nullable
                ),
                (
                    "name".to_string(),
                    arrow::datatypes::DataType::Utf8,
                    true, // default nullable per SQL convention
                ),
            ]
        );
    }

    /// CHA-345 — transactional `CREATE TABLE` threads the open
    /// `tx_uuid` into the WriteService request. Auto-commit (no open tx)
    /// still sends `tx_uuid: None` (pinned by
    /// `create_table_happy_path_carries_snapshot_identifiers_and_pk`).
    #[test]
    fn create_table_in_tx_threads_tx_uuid() {
        let req = build_create_table_request(
            &snapshot_in_tx("tx-1"),
            "public",
            parse_create_table("CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id))"),
        )
        .unwrap();
        assert_eq!(
            req.tx_uuid.as_deref(),
            Some("tx-1"),
            "in-tx CREATE TABLE must thread the open tx_uuid"
        );
        // Identifiers / schema unchanged from the auto-commit path.
        assert_eq!(req.schema_name.as_deref(), Some("myapp"));
        assert_eq!(req.table_name, "t");
        assert_eq!(req.primary_keys, vec!["id"]);
    }

    #[test]
    fn create_table_bare_name_falls_back_to_default_schema() {
        let req = build_table_from("CREATE TABLE t (id BIGINT, PRIMARY KEY(id))").unwrap();
        assert_eq!(req.schema_name.as_deref(), Some("public"));
        assert_eq!(req.table_name, "t");
    }

    #[test]
    fn create_table_inline_primary_key_column_option_is_picked_up() {
        let req = build_table_from("CREATE TABLE myapp.t (id BIGINT PRIMARY KEY)").unwrap();
        assert_eq!(req.primary_keys, vec!["id"]);
    }

    // PK validation (empty / duplicate / undeclared) lives one layer
    // down in `penca-api::write::create_table` so both Flight SQL
    // and direct gRPC callers see identical rejection wording. The
    // SQL-side `extract_primary_keys` now only does extraction (per-
    // column option scan + table-constraint scan); validation tests
    // live in penca-api. The two checks below pin that
    // `extract_primary_keys` itself is validation-free at this layer.

    #[test]
    fn extract_primary_keys_returns_empty_vec_when_no_pk_declared() {
        let req =
            build_table_from("CREATE TABLE myapp.t (id BIGINT)").expect("build does not reject");
        assert_eq!(req.primary_keys, Vec::<String>::new());
    }

    #[test]
    fn extract_primary_keys_does_not_dedupe() {
        // Inline `PRIMARY KEY` option + standalone `PRIMARY KEY(id)`
        // constraint produces ["id", "id"]. The SQL layer no longer
        // dedupes; `penca-api::write::create_table` does, so both
        // wire paths surface the same error.
        let req = build_table_from("CREATE TABLE myapp.t (id BIGINT PRIMARY KEY, PRIMARY KEY(id))")
            .expect("extraction succeeds");
        assert_eq!(req.primary_keys, vec!["id", "id"]);
    }

    #[test]
    fn create_table_with_default_clause_rejects_invalid_argument() {
        let err = build_table_from("CREATE TABLE myapp.t (id BIGINT DEFAULT 0, PRIMARY KEY(id))")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("DEFAULT"), "{}", err.message());
    }

    #[test]
    fn create_table_if_not_exists_rejects_unimplemented() {
        let err =
            build_table_from("CREATE TABLE IF NOT EXISTS myapp.t (id BIGINT, PRIMARY KEY(id))")
                .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("IF NOT EXISTS"), "{}", err.message());
    }

    #[test]
    fn create_table_or_replace_rejects_unimplemented() {
        let err = build_table_from("CREATE OR REPLACE TABLE myapp.t (id BIGINT, PRIMARY KEY(id))")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("OR REPLACE"), "{}", err.message());
    }

    #[test]
    fn create_table_temporary_rejects_unimplemented() {
        let err = build_table_from("CREATE TEMPORARY TABLE myapp.t (id BIGINT, PRIMARY KEY(id))")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("TEMPORARY"), "{}", err.message());
    }

    #[test]
    fn create_table_as_select_rejects_unimplemented() {
        let err = build_table_from("CREATE TABLE myapp.t AS SELECT 1 AS id").unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        let msg = err.message();
        assert!(msg.contains("AS SELECT") || msg.contains("CTAS"), "{msg}");
    }

    #[test]
    fn create_table_three_part_name_rejects_invalid_argument() {
        let err =
            build_table_from("CREATE TABLE other_catalog.myapp.t (id BIGINT, PRIMARY KEY(id))")
                .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("3-part"), "{}", err.message());
    }

    /// CHA-172 review-finding fix: when DataFusion's parser accepts a
    /// vendor-specific CREATE TABLE clause, Penca's translator must
    /// reject it explicitly rather than silently drop it on the floor
    /// (the original `reject_unsupported_table_modifiers` covered ~14
    /// fields, missing ~35 more). This test enumerates SQL strings
    /// per dialect and asserts the outcome:
    ///
    /// * If `DFParser` rejects the syntax outright (the parser is the
    ///   guard), the case is skipped — Penca doesn't need its own
    ///   guard.
    /// * If `DFParser` parses the syntax successfully, Penca's
    ///   translator MUST surface `Status::unimplemented` naming the
    ///   clause. Silent acceptance = test failure.
    ///
    /// The test deliberately tolerates parse-side rejection (different
    /// dialects evolve sqlparser support over time). A future
    /// sqlparser bump that starts parsing a previously-rejected
    /// clause silently flips the case from "skipped" to "must-reject"
    /// without changing this test — Penca's translator catches it
    /// because every CreateTable-struct field is wired into
    /// `reject_unsupported_table_modifiers`.
    #[test]
    fn create_table_unsupported_modifiers_reject_per_clause() {
        let cases: &[(&str, &str)] = &[
            // Postgres
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) INHERITS (parent)",
                "INHERITS",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) WITH (autovacuum_enabled = true)",
                "WITH/OPTIONS",
            ),
            // SQLite
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) STRICT",
                "STRICT",
            ),
            // ClickHouse
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) ON CLUSTER 'cluster_a'",
                "ON CLUSTER",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) ORDER BY id",
                "ORDER BY",
            ),
            // BigQuery
            (
                "CREATE TABLE myapp.t (id BIGINT, day DATE, PRIMARY KEY(id)) PARTITION BY day",
                "PARTITION BY",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) CLUSTER BY (id)",
                "CLUSTER BY",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) OPTIONS(description = 'd')",
                "WITH/OPTIONS",
            ),
            // Hive / MySQL trailing-COMMENT
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) COMMENT 'desc'",
                "COMMENT",
            ),
            // Snowflake
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) COPY GRANTS",
                "COPY GRANTS",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) DATA_RETENTION_TIME_IN_DAYS = 7",
                "DATA_RETENTION_TIME_IN_DAYS",
            ),
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) WITH TAG (cost_center = 'core')",
                "WITH TAG",
            ),
            // ANSI / Oracle temporary-table commit semantics
            (
                "CREATE TABLE myapp.t (id BIGINT, PRIMARY KEY(id)) ON COMMIT DELETE ROWS",
                "ON COMMIT",
            ),
        ];
        let mut parsed = 0;
        for (sql, _expected_kw) in cases {
            // Mirror the parse step that runs upstream of build_table_from
            // — if DFParser rejects the SQL itself, the parser is the
            // guard and Penca doesn't have to be.
            let parse_result =
                DFParser::parse_sql(sql).map(|mut s| s.pop_front().expect("non-empty"));
            let Ok(_) = parse_result else { continue };
            parsed += 1;

            // Silent-modifier-acceptance is the bug class this test
            // guards: DFParser accepted, Penca must not silently
            // accept. We don't pin which specific reject_modifier arm
            // fires — sqlparser's GenericDialect routes some clauses
            // through shared fields (e.g. trailing `COMMENT 'x'` lands
            // in `table_options::Plain`, not the dedicated `comment`
            // field), and pinning the specific keyword would couple
            // this test to sqlparser-internal routing decisions. The
            // load-bearing assertion is "Unimplemented + CHA-172
            // cite" — i.e. Penca named a per-clause guard.
            let err = match build_table_from(sql) {
                Ok(_) => panic!(
                    "DFParser accepted `{sql}` but Penca translator silently accepted it — \
                     reject_unsupported_table_modifiers is missing a guard"
                ),
                Err(e) => e,
            };
            assert_eq!(
                err.code(),
                tonic::Code::Unimplemented,
                "{sql}: expected Unimplemented, got {} ({})",
                err.code(),
                err.message()
            );
            assert!(
                err.message().contains("CHA-172"),
                "{sql}: rejection must cite CHA-172; got: {}",
                err.message()
            );
        }
        // Sanity: at least a few cases must have actually exercised the
        // Penca-side rejection path. If sqlparser ever loses
        // dialect coverage and zero cases parse, this assertion fires
        // and the test maintainer revisits the case list.
        assert!(
            parsed >= 3,
            "fewer than 3 of {} CHA-172 modifier-rejection cases parsed via DFParser — \
             sqlparser dialect coverage may have regressed; refresh the case list",
            cases.len()
        );
    }

    #[test]
    fn create_table_with_nested_array_type_rejects_via_sql_type() {
        // The translator surface for unsupported types lives in
        // crate::sql_type::sql_type_to_arrow; ddl just propagates. Pins
        // that the propagation actually fires when the column-type
        // mapping step hits a non-supported variant.
        let err = build_table_from("CREATE TABLE myapp.t (id BIGINT, tags INT[], PRIMARY KEY(id))")
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("core::types"), "{}", err.message());
    }
}
