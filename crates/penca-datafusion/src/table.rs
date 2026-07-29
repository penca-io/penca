//! `TableProvider` backed by Penca's query gRPC service (merge-on-read).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::StreamExt;
use penca_proto::external::v1::query_service_client::QueryServiceClient;
use penca_proto::external::v1::{Projection, ReadDataRequest};
use tonic::transport::Channel;
use tracing_futures::Instrument as _;

use crate::conn_scope::ConnScope;
use crate::expr_to_sql::{exprs_to_where_fragment, is_translatable};
use crate::pk_ids::{all_conjuncts_seeked, build_seek_batch};
use crate::plan_resolution_memo::ResolvedIndex;

/// DataFusion `TableProvider` that reads data via `QueryServiceClient::read_data`.
///
/// Each `scan()` creates a single-partition streaming exec that calls the
/// query microservice's streaming `ReadData` RPC. Each `ReadDataResponse`
/// contains Arrow IPC bytes (one RecordBatch per message). Arrow IPC wire
/// format is the same as in-memory layout — decoding is essentially a
/// pointer cast + validation, not deserialization.
///
/// Carries its [`ConnScope`]. The connection-scoped catalog
/// (`catalog_uuid`) and the open `tx_uuid` (CHA-345, via the scope's
/// `open_tx_cell`) both flow through the scope — the same single source
/// `PencaSchemaProvider` reads — so RYOW data reads and tx-aware
/// metadata reads agree. There is no scan-time cross-catalog check: the
/// catalog-list short-circuit ([`crate::catalog_list`]) gates the SELECT
/// path and `penca_sql_server::tx::validate_session_catalog_name` gates
/// the DML/BEGIN path (CHA-346).
#[derive(Debug)]
pub(crate) struct PencaTableProvider {
    scope: ConnScope,
    schema_name: String,
    table_name: String,
    arrow_schema: SchemaRef,
    /// Declared primary-key column names, in declared order (from
    /// `Table.primary_keys` via the resolution path). Drives the
    /// scan-time `ids` PK-batch point-lookup restriction (CHA-426).
    primary_keys: Arc<[String]>,
    /// Defined secondary indexes (from `Table.indexes` via the resolution
    /// path, CHA-492). Drives the scan-time structured `indexes` seek: an
    /// equality set that fully binds one index's key columns is packed as a
    /// covering seek (IMPL-S2).
    indexes: Arc<[ResolvedIndex]>,
}

impl PencaTableProvider {
    pub(crate) fn new(
        scope: ConnScope,
        schema_name: String,
        table_name: String,
        arrow_schema: SchemaRef,
        primary_keys: Arc<[String]>,
        indexes: Arc<[ResolvedIndex]>,
    ) -> Self {
        Self {
            scope,
            schema_name,
            table_name,
            arrow_schema,
            primary_keys,
            indexes,
        }
    }

    fn build_projected_schema(&self, projection: Option<&[usize]>) -> Result<SchemaRef> {
        match projection {
            Some(indices) => Ok(Arc::new(self.arrow_schema.project(indices)?)),
            None => Ok(self.arrow_schema.clone()),
        }
    }

    /// Translate DataFusion's projection arg to the wire-level
    /// `Projection` message (CHA-180). Three states map straight
    /// through:
    ///   - `None` (no projection): leave the wrapper unset so the
    ///     servicer falls back to "return all user columns."
    ///   - `Some(&[])` (DataFusion's encoding of "no user columns
    ///     needed," used for `COUNT(*)`): send an empty
    ///     `Projection{columns=[]}` so the servicer yields 0-col
    ///     batches with the correct `num_rows`.
    ///   - `Some(&[..])` (named subset): send the projected names
    ///     in order; the servicer projects inside `stream_merged`.
    fn build_projection_message(&self, projection: Option<&[usize]>) -> Option<Projection> {
        projection.map(|indices| Projection {
            columns: indices
                .iter()
                .map(|&i| self.arrow_schema.field(i).name().clone())
                .collect(),
        })
    }

    fn build_read_request(
        &self,
        projection_msg: Option<Projection>,
        filter: Option<String>,
        ids: Vec<u8>,
        indexes: Vec<u8>,
    ) -> ReadDataRequest {
        // CHA-374 / CHA-460: the open tx (RYOW) and the pinned auto-commit
        // as_of_seq frontier are mutually exclusive — the same
        // `read_snapshot_fields` policy the metadata-resolution reads use, so
        // planning and execution resolve at one seq snapshot.
        let (open_tx_uuid, as_of_seq) = self.scope.read_snapshot_fields();
        ReadDataRequest {
            catalog_name: Some(self.scope.catalog_name.clone()),
            schema_name: Some(self.schema_name.clone()),
            table_name: Some(self.table_name.clone()),
            // CHA-255: route by branch_uuid (rename-stable).
            branch_uuid: Some(self.scope.branch_uuid.clone()),
            branch_name: None,
            projection: projection_msg,
            catalog_uuid: None,
            schema_uuid: None,
            table_uuid: None,
            // CHA-460: the SQL pin is a commit_seq_num frontier → the seq arm of the
            // `as_of` oneof (was CommitMicros under the CHA-374 pg_now pin).
            as_of: as_of_seq.map(penca_proto::external::v1::read_data_request::AsOf::CommitSeqNum),
            open_tx_uuid,
            filter,
            // CHA-426: PK-equality point lookups carry the ids PK batch so
            // the server restricts the merge-on-read to the named rows;
            // empty = unrestricted. The full predicate still rides `filter`,
            // so any over-return is trimmed downstream.
            ids,
            // CHA-492: the structured secondary-index seek batch (an equality
            // set fully binding one defined index's key columns); empty = no
            // index seek.
            indexes,
        }
    }
}

#[async_trait]
impl TableProvider for PencaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[tracing::instrument(
        skip_all,
        fields(
            catalog = %self.scope.catalog_uuid,
            schema = %self.schema_name,
            table = %self.table_name,
            branch = %self.scope.branch_uuid,
            open_tx = tracing::field::Empty,
            has_filter = tracing::field::Empty,
            ids_rows = tracing::field::Empty,
            indexes_rows = tracing::field::Empty,
        ),
    )]
    async fn scan(
        &self,
        // `_state` is unused: the open tx is read from the `ConnScope`
        // cell (CHA-345) and there is no scan-time cross-catalog check
        // (CHA-346 — gated upstream by the catalog-list short-circuit /
        // `validate_session_catalog_name`). The param stays because
        // `TableProvider::scan`'s signature requires it.
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let open_tx_uuid = self.scope.open_tx_uuid();
        tracing::Span::current().record("open_tx", tracing::field::debug(&open_tx_uuid));
        let projection = projection.map(Vec::as_slice);
        let output_schema = self.build_projected_schema(projection)?;
        let projection_msg = self.build_projection_message(projection);
        // Push translatable filters into the query microservice as a SQL
        // WHERE fragment. `supports_filters_pushdown` reports `Inexact` so
        // DataFusion still applies a post-filter and any predicate we drop
        // here remains correctness-preserving.
        // CHA-426: a conjunction pinning every declared PK column to a
        // losslessly-typed literal becomes the ids PK batch — the point-lookup
        // restriction the server resolves below the merge-on-read dedup. `None`
        // (any doubt) = unrestricted read.
        let pk_ids = build_seek_batch(filters, &self.arrow_schema, &self.primary_keys);
        // Count only — PK values are PII-gated out of spans (same gate as
        // `filter`); 0 = unrestricted.
        tracing::Span::current().record(
            "ids_rows",
            pk_ids.as_ref().map_or(0, |ids| ids.row_count) as u64,
        );

        // CHA-492 / CHA-485: pack the UNION of the key columns of EVERY defined
        // index fully equality-bound by the predicate. The server's
        // select_from_bindings then selects every covering index — one covering
        // index takes the DataFusion-free exact-cover bypass, several intersect
        // in the merge. `build_seek_batch` over the union returns `None` when a
        // column can't be captured (e.g. an IN on a composite union), leaving
        // `indexes` empty so the server falls back to the filter re-parse path.
        let mut index_columns: Vec<String> = Vec::new();
        for ix in self.indexes.iter() {
            if build_seek_batch(filters, &self.arrow_schema, &ix.key_columns).is_some() {
                for column in ix.key_columns.iter() {
                    if !index_columns.contains(column) {
                        index_columns.push(column.clone());
                    }
                }
            }
        }
        let index_seek = build_seek_batch(filters, &self.arrow_schema, &index_columns);
        tracing::Span::current().record(
            "indexes_rows",
            index_seek.as_ref().map_or(0, |seek| seek.row_count) as u64,
        );

        // Push a WHERE fragment only for the conjuncts the structured seeks did
        // NOT consume. When every conjunct is a seeked-column equality the pushed
        // filter is empty — the server's exact-cover signal for the
        // DataFusion-free bypass (CHA-492). Only strip index columns when the
        // union batch actually built (`index_seek` is `Some`), so an
        // un-capturable predicate keeps its residual. `supports_filters_pushdown`
        // stays Inexact, so DataFusion's FilterExec re-applies the full predicate;
        // dropping the seeked equality here never loses correctness.
        let mut seeked_columns: Vec<&str> = Vec::new();
        if pk_ids.is_some() {
            seeked_columns.extend(self.primary_keys.iter().map(String::as_str));
        }
        if index_seek.is_some() {
            seeked_columns.extend(index_columns.iter().map(String::as_str));
        }
        let filter = if all_conjuncts_seeked(filters, &seeked_columns) {
            None
        } else {
            exprs_to_where_fragment(filters)
        };
        tracing::Span::current().record("has_filter", filter.is_some());

        let ids = pk_ids.map_or_else(Vec::new, |ids| ids.ipc_bytes);
        let indexes = index_seek.map_or_else(Vec::new, |seek| seek.ipc_bytes);
        let request = self.build_read_request(projection_msg, filter, ids, indexes);

        let partition = PencaPartitionStream {
            query_channel: self.scope.query_channel.clone(),
            schema: output_schema.clone(),
            request,
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            output_schema,
            vec![Arc::new(partition)],
            None,   // no projection — already applied above
            vec![], // no ordering
            false,  // not infinite
            None,   // no limit
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|e| {
                if is_translatable(e) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

/// Single-partition stream that calls `QueryServiceClient::read_data` and
/// decodes Arrow IPC bytes from each `ReadDataResponse`.
struct PencaPartitionStream {
    query_channel: Channel,
    schema: SchemaRef,
    request: ReadDataRequest,
}

impl std::fmt::Debug for PencaPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PencaPartitionStream")
            .field("schema", &self.schema)
            .finish()
    }
}

impl PartitionStream for PencaPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::execution::SendableRecordBatchStream {
        let channel = self.query_channel.clone();
        let schema = self.schema.clone();
        let request = self.request.clone();

        let span = tracing::debug_span!(
            "read_data",
            catalog_name = ?request.catalog_name,
            schema = ?request.schema_name,
            table = ?request.table_name,
            branch = ?request.branch_uuid,
            has_filter = request.filter.is_some(),
            // Presence only — the row count lives on the scan span
            // (`ids_rows`) and the server-side read_data/merge_read spans.
            has_ids = !request.ids.is_empty(),
            has_projection = request.projection.is_some(),
            open_tx = ?request.open_tx_uuid,
            as_of = ?request.as_of,
        );

        let stream = async_stream::try_stream! {
            let mut client = QueryServiceClient::new(channel);
            let mut response_stream = client
                .read_data(request)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                .into_inner();

            while let Some(resp) = response_stream.next().await {
                let resp = resp.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                if resp.data.is_empty() {
                    continue;
                }

                let cursor = std::io::Cursor::new(resp.data);
                let reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

                for batch_result in reader {
                    let batch = batch_result
                        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
                    yield batch;
                }
            }
        }
        .instrument(span);

        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array as _, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use datafusion::catalog::SchemaProvider as _;
    use datafusion::prelude::SessionContext;
    use penca_proto::external::v1::{GetTableResponse, Table};
    use std::sync::{Arc, Mutex, RwLock};

    use crate::schema::PencaSchemaProvider;
    use crate::test_support::{StubQuery, spawn_stub};

    // CHA-374 / CHA-460: the pinned auto-commit snapshot (a commit_seq_num frontier)
    // reaches the scan via the build-scoped `ConnScope.as_of_seq_cell`, read by
    // `pinned_as_of_seq()`. A scan stamps it onto the `ReadDataRequest` — but
    // only when there is no open tx (open_tx and as_of are mutually exclusive on
    // the wire).
    fn test_scope(open_tx: Option<String>, as_of: Option<i64>) -> ConnScope {
        ConnScope {
            query_channel: Channel::from_static("http://localhost:0").connect_lazy(),
            catalog_uuid: "cat-uuid".into(),
            catalog_name: "public".into(),
            branch_uuid: "branch-uuid".into(),
            open_tx_cell: Arc::new(RwLock::new(open_tx)),
            as_of_seq_cell: Arc::new(RwLock::new(as_of)),
            resolution_memo_cell: Arc::new(RwLock::new(None)),
        }
    }

    fn test_provider(scope: ConnScope) -> PencaTableProvider {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        PencaTableProvider::new(
            scope,
            "public".into(),
            "tbl".into(),
            schema,
            vec!["a".to_string()].into(),
            Vec::new().into(),
        )
    }

    #[tokio::test]
    async fn pinned_as_of_seq_reader_is_independent_of_open_tx() {
        // The reader returns the raw cell value and does NOT apply the
        // open_tx-vs-as_of mutual exclusion — that decision lives in
        // `build_read_request` (asserted by the scan_request_* tests). Here we
        // pin an as_of_seq *with* an open tx present and confirm the reader
        // still reports it unchanged; clearing the cell reports None.
        let scope = test_scope(Some("tx-1".into()), Some(777));
        assert_eq!(scope.pinned_as_of_seq(), Some(777));
        *scope.as_of_seq_cell.write().unwrap() = None;
        assert_eq!(scope.pinned_as_of_seq(), None);
    }

    #[tokio::test]
    async fn scan_request_carries_pinned_as_of_seq_when_no_open_tx() {
        let provider = test_provider(test_scope(None, Some(777)));
        let req = provider.build_read_request(None, None, Vec::new(), Vec::new());
        assert_eq!(
            req.as_of,
            Some(penca_proto::external::v1::read_data_request::AsOf::CommitSeqNum(777))
        );
        assert_eq!(req.open_tx_uuid, None);
    }

    #[tokio::test]
    async fn scan_request_leaves_as_of_none_when_in_tx() {
        // open_tx wins; as_of must stay None (mutual exclusion).
        let provider = test_provider(test_scope(Some("tx-1".into()), Some(777)));
        let req = provider.build_read_request(None, None, Vec::new(), Vec::new());
        assert_eq!(req.open_tx_uuid.as_deref(), Some("tx-1"));
        assert_eq!(req.as_of, None);
    }

    #[tokio::test]
    async fn scan_request_as_of_none_when_unset() {
        let provider = test_provider(test_scope(None, None));
        let req = provider.build_read_request(None, None, Vec::new(), Vec::new());
        assert_eq!(req.as_of, None);
    }

    // CHA-426 — `scan` must populate `ReadDataRequest.ids` (an Arrow IPC
    // PK batch, the CHA-398 point-lookup restriction) whenever the pushed
    // filters contain a conjunction of `Column = Literal` equalities
    // covering every declared primary-key column. The tests drive real SQL
    // through a provider built via `PencaSchemaProvider::table()` (so the
    // declared `primary_keys` arrive the same way production resolution
    // delivers them) and assert on the wire request captured by the stub.

    /// Serialize `schema` the way `GetTableResponse.table.arrow_schema`
    /// carries it: an Arrow IPC stream with no batches.
    fn schema_ipc_bytes(schema: &Schema) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, schema).unwrap();
        writer.finish().unwrap();
        buf
    }

    /// Stand up a stub query service advertising a table named `t` with
    /// the given schema + declared PKs, resolve the provider through
    /// `PencaSchemaProvider::table()`, and register it as `t` in a fresh
    /// `SessionContext`. Returns the context plus the captured-request
    /// handle.
    async fn ctx_with_table(
        schema: &Schema,
        primary_keys: &[&str],
    ) -> (SessionContext, Arc<Mutex<Vec<ReadDataRequest>>>) {
        let captured_read_data = Arc::new(Mutex::new(Vec::new()));
        let channel = spawn_stub(StubQuery {
            get_table: Some(Ok(GetTableResponse {
                table: Some(Table {
                    table_uuid: "11111111-2222-3333-4444-555555555555".into(),
                    table_name: "t".into(),
                    arrow_schema: schema_ipc_bytes(schema),
                    primary_keys: primary_keys.iter().map(|s| s.to_string()).collect(),
                    ..Default::default()
                }),
            })),
            read_data: Some(Ok(Vec::new())),
            captured_read_data: captured_read_data.clone(),
            ..Default::default()
        })
        .await;

        let scope = ConnScope {
            query_channel: channel,
            catalog_uuid: "cat-uuid".into(),
            catalog_name: "public".into(),
            branch_uuid: "branch-uuid".into(),
            open_tx_cell: Arc::new(RwLock::new(None)),
            as_of_seq_cell: Arc::new(RwLock::new(None)),
            resolution_memo_cell: Arc::new(RwLock::new(None)),
        };
        let provider = PencaSchemaProvider::new(scope, "public".into())
            .table("t")
            .await
            .expect("stubbed get_table must resolve")
            .expect("stubbed table must exist");

        let ctx = SessionContext::new();
        ctx.register_table("t", provider).unwrap();
        (ctx, captured_read_data)
    }

    /// Run `sql` to completion and return the single captured
    /// `ReadDataRequest` the scan sent.
    async fn captured_request(
        ctx: &SessionContext,
        captured: &Arc<Mutex<Vec<ReadDataRequest>>>,
        sql: &str,
    ) -> ReadDataRequest {
        ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let requests = captured.lock().unwrap();
        assert_eq!(requests.len(), 1, "expected exactly one read_data call");
        requests[0].clone()
    }

    /// Decode `req.ids`, assert it is non-empty (`context` names the
    /// red expectation) with the expected flattened row total, and
    /// return the batches. Multi-batch streams are legitimate on this
    /// wire (the server-side kernel flattens in order), so callers read
    /// values across ALL batches, never `batches[0]` alone.
    fn decoded_ids_batches(
        req: &ReadDataRequest,
        expected_rows: usize,
        context: &str,
    ) -> Vec<RecordBatch> {
        assert!(
            !req.ids.is_empty(),
            "ids expected non-empty {context}, got empty"
        );
        let reader = StreamReader::try_new(std::io::Cursor::new(req.ids.to_vec()), None)
            .expect("ids must be a valid Arrow IPC stream");
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, expected_rows);
        batches
    }

    fn pk_schema() -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])
    }

    /// Composite-PK fixture; declared PK order is (region, name).
    fn composite_pk_schema() -> Schema {
        Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])
    }

    /// Concatenated string values of column `idx` across all batches —
    /// the multi-batch-tolerant read shape.
    fn string_values(batches: &[RecordBatch], idx: usize) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .expect("expected Utf8 ids column");
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn ids_single_pk_equality() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(&ctx, &captured, "SELECT * FROM t WHERE name = 'alice'").await;

        let batches = decoded_ids_batches(&req, 1, "for a full-PK equality point lookup");
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(string_values(&batches, 0), vec!["alice"]);

        // CHA-492: a full-PK equality is an EXACT cover — the ids seek fully
        // covers the predicate, so scan strips the pushed WHERE fragment
        // (empty = the server's exact-cover bypass signal). Correctness holds
        // because `supports_filters_pushdown` stays Inexact, so DataFusion's
        // FilterExec re-applies the predicate regardless.
        assert!(
            req.filter.is_none(),
            "a fully-covering PK equality must strip the pushed filter, got {:?}",
            req.filter
        );
    }

    #[tokio::test]
    async fn ids_composite_pk_declared_order() {
        let (ctx, captured) = ctx_with_table(&composite_pk_schema(), &["region", "name"]).await;
        // The SQL spells the conjunction in REVERSED declared order; the
        // ids batch must come out in DECLARED order (the server-side
        // PK-batch validation is strict on column order).
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name = 'alice' AND region = 'eu'",
        )
        .await;

        let batches = decoded_ids_batches(&req, 1, "for a composite full-PK equality");
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["region", "name"]);
        assert_eq!(string_values(&batches, 0), vec!["eu"]);
        assert_eq!(string_values(&batches, 1), vec!["alice"]);
    }

    #[tokio::test]
    async fn ids_in_list_single_pk() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name IN ('alice', 'bob', 'carol')",
        )
        .await;

        let batches = decoded_ids_batches(&req, 3, "for a single-PK IN list");
        let mut values = string_values(&batches, 0);
        values.sort();
        assert_eq!(values, ["alice", "bob", "carol"]);
    }

    // The two multi-row kernel arms each get a wire-level pin that does
    // not depend on the simplifier's IN-inlining threshold: <= 3 items
    // inline into OR chains (the test above actually exercises the OR
    // arm), so the explicit-OR spelling and a 10-item IN list pin each arm
    // regardless of where DataFusion draws that line.

    #[tokio::test]
    async fn ids_explicit_or_same_pk() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name = 'alice' OR name = 'bob'",
        )
        .await;

        let batches = decoded_ids_batches(&req, 2, "for a same-PK OR disjunction");
        let mut values = string_values(&batches, 0);
        values.sort();
        assert_eq!(values, ["alice", "bob"]);
    }

    #[tokio::test]
    async fn ids_in_list_above_inline_threshold() {
        // Ten items: comfortably above DataFusion's IN-inlining
        // threshold (currently 3), so a plausible constant bump cannot
        // silently flip this pin onto the OR arm.
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name IN \
             ('p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9')",
        )
        .await;

        let batches = decoded_ids_batches(&req, 10, "for a 10-item single-PK IN list");
        let mut values = string_values(&batches, 0);
        values.sort();
        let expected: Vec<String> = (0..10).map(|i| format!("p{i}")).collect();
        assert_eq!(values, expected);
    }

    #[tokio::test]
    async fn ids_pk_equality_plus_residual_predicate() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name = 'alice' AND value > 1",
        )
        .await;

        decoded_ids_batches(
            &req,
            1,
            "when PK equality is conjoined with a residual predicate",
        );

        // Both predicates remain in the WHERE fragment — ids narrows, the
        // residual still trims.
        let filter = req.filter.expect("WHERE fragment must still be present");
        assert!(filter.contains("name"), "{filter}");
        assert!(filter.contains("value"), "{filter}");
    }

    // Negative pins — when extraction must NOT fire. Over-triggering is
    // not benign: the server-side PK-batch validation (penca-api
    // pk_batch) hard-rejects any batch whose columns deviate from the
    // declared PK set, so a partial-PK or non-PK ids batch would turn
    // ordinary working queries into InvalidRequest errors. Green today
    // (ids is always empty) and a constraint on the wiring commit.

    #[tokio::test]
    async fn ids_empty_for_partial_composite_pk() {
        let (ctx, captured) = ctx_with_table(&composite_pk_schema(), &["region", "name"]).await;
        let req = captured_request(&ctx, &captured, "SELECT * FROM t WHERE region = 'eu'").await;

        assert!(
            req.ids.is_empty(),
            "a conjunction covering only part of the composite PK must not emit ids"
        );
        assert!(req.filter.is_some(), "WHERE fragment must still push down");
    }

    #[tokio::test]
    async fn ids_empty_for_non_pk_equality() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(&ctx, &captured, "SELECT * FROM t WHERE value = 10").await;

        assert!(req.ids.is_empty(), "a non-PK equality must not emit ids");
        assert!(req.filter.is_some(), "WHERE fragment must still push down");
    }

    #[tokio::test]
    async fn ids_empty_for_no_declared_pks() {
        let (ctx, captured) = ctx_with_table(&pk_schema(), &[]).await;
        let req = captured_request(&ctx, &captured, "SELECT * FROM t WHERE name = 'alice'").await;

        assert!(
            req.ids.is_empty(),
            "a table without declared primary keys must never emit ids"
        );
    }

    #[tokio::test]
    async fn ids_empty_for_cross_column_or() {
        // A disjunction across DIFFERENT columns can never collapse to a
        // single-column IN list, so it must never emit ids. (A same-column
        // OR may legitimately canonicalize to IN upstream — that shape is
        // covered by ids_in_list_single_pk.)
        let (ctx, captured) = ctx_with_table(&pk_schema(), &["name"]).await;
        let req = captured_request(
            &ctx,
            &captured,
            "SELECT * FROM t WHERE name = 'alice' OR value > 1",
        )
        .await;

        assert!(
            req.ids.is_empty(),
            "a cross-column disjunction must not emit ids"
        );
    }
}
