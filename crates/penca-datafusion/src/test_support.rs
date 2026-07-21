//! Shared test-only Flight SQL `QueryService` stub.
//!
//! `catalog.rs` and `schema.rs` both unit-test their providers against an
//! in-process `QueryService` where one RPC returns a canned `Result` and the
//! rest are `unimplemented`. The generated trait has ~10 methods, so an
//! all-`unimplemented`-except-one impl is ~70 lines; keeping it here (rather
//! than duplicated per test module) means a future `QueryService` method
//! addition is a single edit, and the loopback bind-before-serve rationale
//! lives in one place.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::Stream;
use penca_proto::external::v1::query_service_server::{QueryService, QueryServiceServer};
use penca_proto::external::v1::{
    AuditDataRequest, AuditDataResponse, GetBranchRequest, GetBranchResponse, GetCatalogRequest,
    GetCatalogResponse, GetIndexRequest, GetIndexResponse, GetMaxCommitSeqNumRequest,
    GetMaxCommitSeqNumResponse, GetSchemaRequest, GetSchemaResponse, GetTableRequest,
    GetTableResponse, ListBranchesRequest, ListBranchesResponse, ListCatalogsRequest,
    ListCatalogsResponse, ListIndexesRequest, ListIndexesResponse, ListSchemasRequest,
    ListSchemasResponse, ListTablesRequest, ListTablesResponse, ReadDataRequest, ReadDataResponse,
};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

/// In-process `QueryService` stub. Each canned-response field, when `Some`, is
/// what the matching RPC returns; `None` (the default) makes that RPC return
/// `Status::unimplemented`. Build with `StubQuery::default()` and set only the
/// RPCs the unit under test exercises:
///
/// ```ignore
/// let channel = spawn_stub(StubQuery {
///     get_schema: Some(Err(Status::not_found("…"))),
///     ..Default::default()
/// })
/// .await;
/// ```
#[derive(Default)]
pub(crate) struct StubQuery {
    pub get_schema: Option<Result<GetSchemaResponse, Status>>,
    pub get_table: Option<Result<GetTableResponse, Status>>,
    pub list_tables: Option<Result<ListTablesResponse, Status>>,
    /// Canned `read_data` stream items; `None` keeps the RPC
    /// `unimplemented` like the rest.
    pub read_data: Option<Result<Vec<ReadDataResponse>, Status>>,
    /// Every `ReadDataRequest` received, in arrival order — lets scan-level
    /// tests assert on the outgoing wire request (filter / ids / projection).
    pub captured_read_data: Arc<Mutex<Vec<ReadDataRequest>>>,
}

impl StubQuery {
    /// Return the canned response if set, else `unimplemented` naming the RPC.
    fn answer<T: Clone>(
        canned: &Option<Result<T, Status>>,
        rpc: &'static str,
    ) -> Result<Response<T>, Status> {
        match canned {
            Some(result) => result.clone().map(Response::new),
            None => Err(Status::unimplemented(rpc)),
        }
    }
}

#[tonic::async_trait]
impl QueryService for StubQuery {
    async fn get_schema(
        &self,
        _r: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        Self::answer(&self.get_schema, "get_schema")
    }

    async fn get_table(
        &self,
        _r: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        Self::answer(&self.get_table, "get_table")
    }

    async fn list_tables(
        &self,
        _r: Request<ListTablesRequest>,
    ) -> Result<Response<ListTablesResponse>, Status> {
        Self::answer(&self.list_tables, "list_tables")
    }

    async fn get_catalog(
        &self,
        _r: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        Err(Status::unimplemented("get_catalog"))
    }
    async fn list_catalogs(
        &self,
        _r: Request<ListCatalogsRequest>,
    ) -> Result<Response<ListCatalogsResponse>, Status> {
        Err(Status::unimplemented("list_catalogs"))
    }
    async fn list_schemas(
        &self,
        _r: Request<ListSchemasRequest>,
    ) -> Result<Response<ListSchemasResponse>, Status> {
        Err(Status::unimplemented("list_schemas"))
    }
    async fn get_branch(
        &self,
        _r: Request<GetBranchRequest>,
    ) -> Result<Response<GetBranchResponse>, Status> {
        Err(Status::unimplemented("get_branch"))
    }
    async fn list_branches(
        &self,
        _r: Request<ListBranchesRequest>,
    ) -> Result<Response<ListBranchesResponse>, Status> {
        Err(Status::unimplemented("list_branches"))
    }
    async fn get_index(
        &self,
        _r: Request<GetIndexRequest>,
    ) -> Result<Response<GetIndexResponse>, Status> {
        Err(Status::unimplemented("get_index"))
    }
    async fn list_indexes(
        &self,
        _r: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        Err(Status::unimplemented("list_indexes"))
    }

    type ReadDataStream =
        Pin<Box<dyn Stream<Item = Result<ReadDataResponse, Status>> + Send + 'static>>;
    async fn read_data(
        &self,
        r: Request<ReadDataRequest>,
    ) -> Result<Response<Self::ReadDataStream>, Status> {
        self.captured_read_data.lock().unwrap().push(r.into_inner());
        match &self.read_data {
            Some(Ok(items)) => {
                let stream = futures::stream::iter(items.clone().into_iter().map(Ok));
                Ok(Response::new(Box::pin(stream)))
            }
            Some(Err(status)) => Err(status.clone()),
            None => Err(Status::unimplemented("read_data")),
        }
    }

    type AuditDataStream =
        Pin<Box<dyn Stream<Item = Result<AuditDataResponse, Status>> + Send + 'static>>;
    async fn audit_data(
        &self,
        _r: Request<AuditDataRequest>,
    ) -> Result<Response<Self::AuditDataStream>, Status> {
        Err(Status::unimplemented("audit_data"))
    }

    async fn get_max_commit_seq_num(
        &self,
        _r: Request<GetMaxCommitSeqNumRequest>,
    ) -> Result<Response<GetMaxCommitSeqNumResponse>, Status> {
        Err(Status::unimplemented("get_max_commit_seq_num"))
    }
}

/// Serve `stub` on an ephemeral loopback port and return a connected `Channel`.
pub(crate) async fn spawn_stub(stub: StubQuery) -> Channel {
    // Bind first → kernel-side backlog accepts client connections even before
    // the spawned task drains them, so `.connect()` below isn't racing the
    // `serve_with_incoming` call.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpListenerStream::new(listener);

    tokio::spawn(async move {
        Server::builder()
            .add_service(QueryServiceServer::new(stub))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}
