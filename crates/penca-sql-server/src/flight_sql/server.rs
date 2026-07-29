//! Per-TCP-connection driver for the Flight SQL servicer.
//!
//! tonic 0.14's `Server::serve` wraps the layered service in a
//! `BoxCloneService` that hyper clones per HTTP/2 stream, so a
//! `tower::Layer` cannot be the per-conn boundary. Driving
//! `hyper-util::serve_connection` directly with a custom
//! [`tower::Service<SocketAddr>`] make-service is the canonical way to
//! get a true "fresh service per accepted TCP connection" hook in
//! tonic 0.14. [`drive_with_hyper_util`] is that loop; it also owns the
//! graceful-shutdown drain.
//!
//! [`super::service::FlightSqlService::serve_with_shutdown`] is the
//! sole caller — it constructs [`PerConnMakeService`] via
//! [`PerConnMakeService::new`] and hands it to [`drive_with_hyper_util`].

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use hyper::body::Incoming;
use hyper::server::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use tonic::service::Routes;
use tower::Service;

use crate::session::{
    BRANCH_HEADER_NAME, BranchHeader, CATALOG_HEADER_NAME, CatalogHeader, ConnSessionFactory,
    ConnSessionInit,
};

/// `tower::Service<SocketAddr>` that produces a fresh per-conn shell
/// ([`PerConnService`]) on every accepted TCP connection. tonic's
/// `Server::serve` wraps the layered service in `BoxCloneService` that
/// hyper clones per HTTP/2 stream, so `Layer::Service::Clone` fires
/// per-stream, not per-conn. Driving `hyper-util::serve_connection`
/// directly with this `MakeService` is the canonical way to get a true
/// "fresh service per accepted TCP connection" hook in tonic 0.14.
pub(crate) struct PerConnMakeService {
    routes: Routes,
    factory: Arc<ConnSessionFactory>,
}

impl PerConnMakeService {
    pub(crate) fn new(routes: Routes, factory: Arc<ConnSessionFactory>) -> Self {
        Self { routes, factory }
    }
}

impl Service<SocketAddr> for PerConnMakeService {
    type Response = PerConnService;
    type Error = Infallible;
    type Future = std::future::Ready<std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _peer: SocketAddr) -> Self::Future {
        std::future::ready(Ok(PerConnService {
            routes: self.routes.clone(),
            init: ConnSessionInit::new(self.factory.clone()),
        }))
    }
}

/// Per-TCP-connection Service shell. Cloned per HTTP/2 stream by
/// hyper's `BoxCloneService`; all clones share `init` via Arc so the
/// first stream's mint wins and every subsequent stream observes the
/// same `Arc<ConnSession>`. See [`ConnSessionInit`].
pub(crate) struct PerConnService {
    routes: Routes,
    init: ConnSessionInit,
}

impl Clone for PerConnService {
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            init: self.init.clone(),
        }
    }
}

impl Service<http::Request<Incoming>> for PerConnService {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send + 'static>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        <Routes as Service<http::Request<Incoming>>>::poll_ready(&mut self.routes, cx)
    }

    fn call(&mut self, mut req: http::Request<Incoming>) -> Self::Future {
        // Headers needed at conn-mint (first-request only) and per-request
        // re-validation downstream by `validate_*_header`.
        let branch_override = req
            .headers()
            .get(BRANCH_HEADER_NAME)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let catalog_override = req
            .headers()
            .get(CATALOG_HEADER_NAME)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let init = self.init.clone();
        let mut routes = self.routes.clone();
        Box::pin(async move {
            // `init_or_get` takes refs so the post-first-request hot
            // path doesn't clone the override strings just to drop
            // them inside `OnceCell::get_or_try_init`'s already-set
            // branch.
            let conn = match init
                .init_or_get(catalog_override.as_deref(), branch_override.as_deref())
                .await
            {
                Ok(conn) => conn,
                Err(status) => {
                    // Mint failed (catalog/branch name doesn't resolve, or
                    // transient `catalog_store` / `branch_store` error).
                    // Surface as a trailer-only gRPC response — no
                    // `ConnSession` exists, no further handler runs.
                    return Ok(status.into_http());
                }
            };
            let snapshot = conn.snapshot().await;
            req.extensions_mut().insert(conn);
            req.extensions_mut().insert(snapshot);
            req.extensions_mut().insert(BranchHeader(branch_override));
            req.extensions_mut().insert(CatalogHeader(catalog_override));
            routes.call(req).await
        })
    }
}

/// Accept loop driven by `hyper::server::conn::http2::Builder::serve_connection`.
/// Each accepted TCP connection gets a fresh [`PerConnService`] via
/// [`PerConnMakeService::call`]; hyper clones that service per HTTP/2
/// stream to dispatch concurrent requests, but every clone shares the
/// same `ConnSessionInit::cell`, so the conn's `Arc<ConnSession>` is
/// minted once and observed by all streams on the conn.
///
/// Concurrent with the accept loop, the `shutdown` future is polled.
/// When it resolves the loop exits and the driver drains in-flight
/// conn-handler tasks before returning. The drain is bounded by
/// [`DRAIN_TIMEOUT`]; on timeout the remaining tasks are aborted —
/// `ConnSession::Drop` still fires its best-effort `AbortTx` for any
/// open tx in that case (provided the runtime is still alive), and
/// the WriteService TTL is the absolute backstop. The whole point of
/// the graceful shutdown path is to keep the runtime alive long
/// enough for the AbortTx to land instead of relying on the TTL.
pub(crate) async fn drive_with_hyper_util<F>(
    listener: tokio::net::TcpListener,
    mut make: PerConnMakeService,
    shutdown: F,
) -> std::result::Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = ()> + Send + 'static,
{
    /// Upper bound on how long we wait for in-flight conns to close on
    /// their own after the accept loop stops. Sized to comfortably
    /// exceed typical analytics query duration but stay under
    /// orchestrator-level kill timers (k8s `terminationGracePeriodSeconds`
    /// defaults to 30s).
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

    let mut tasks = tokio::task::JoinSet::new();
    let mut shutdown = std::pin::pin!(shutdown);
    loop {
        tokio::select! {
            // `biased` so the shutdown branch is checked first on every
            // poll — without it, a busy accept loop could starve the
            // shutdown signal indefinitely.
            biased;
            () = &mut shutdown => {
                tracing::info!(
                    active_conns = tasks.len(),
                    "shutdown signal received; stopping accept loop and draining in-flight connections"
                );
                break;
            }
            accepted = listener.accept() => {
                let (tcp_stream, peer_addr) = accepted?;
                // CHA-377: disable Nagle — small Flight frames (1-row batches, the
                // stream terminator, GetFlightInfo/DoGet exchanges) otherwise stall
                // ~40ms each on the peer's TCP delayed-ACK.
                if let Err(e) = tcp_stream.set_nodelay(true) {
                    tracing::debug!(error = %e, "failed to set TCP_NODELAY on accepted Flight SQL connection");
                }
                let service = make.call(peer_addr).await?;
                let io = TokioIo::new(tcp_stream);
                tasks.spawn(async move {
                    // `hyper::serve_connection` expects a `hyper::service::Service`;
                    // wrap the tower-side `PerConnService` via `TowerToHyperService`.
                    let hyper_service = TowerToHyperService::new(service);
                    let builder = http2::Builder::new(TokioExecutor::new());
                    if let Err(e) = builder.serve_connection(io, hyper_service).await {
                        // Per-connection transport teardown — expected, not actionable.
                        // The 2s TCP liveness probe (compose healthcheck) opens and closes
                        // a socket with no HTTP/2 preface, and ordinary client disconnects
                        // drop the conn without a clean close; both surface here as
                        // "connection error". Real RPC failures surface as Flight statuses
                        // in the service layer, not here — so this stays at TRACE to keep
                        // the default `penca=debug` log free of per-probe noise.
                        tracing::trace!(error = %e, "serve_connection terminated");
                    }
                });
            }
        }
    }

    // Drain phase. Each task finishes when its client closes the TCP
    // conn (which triggers `ConnSession::Drop` → fire-and-forget
    // `AbortTx` for any in-flight tx). Long-lived idle conns can drag
    // out indefinitely; `DRAIN_TIMEOUT` caps the wait.
    let drain_deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    while !tasks.is_empty() {
        tokio::select! {
            biased;
            () = tokio::time::sleep_until(drain_deadline) => {
                tracing::warn!(
                    remaining = tasks.len(),
                    drain_timeout_secs = DRAIN_TIMEOUT.as_secs(),
                    "drain timeout exceeded; aborting in-flight connections \
                     (ConnSession::Drop will fire AbortTx best-effort; \
                     WriteService TTL is the absolute backstop)"
                );
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                break;
            }
            _ = tasks.join_next() => { /* one conn drained */ }
        }
    }
    tracing::info!("Flight SQL server shut down");
    Ok(())
}

/// Default shutdown signal — SIGINT (Ctrl-C) or SIGTERM (orchestrator
/// kill). Penca deployments are Linux-only; we use `tokio::signal::unix`
/// directly rather than gating with `cfg(unix)`.
///
/// SIGTERM install failure is logged and falls through to a
/// forever-pending future on its `select!` arm, so SIGINT stays armed
/// and the function still responds to Ctrl-C. (Install failure is
/// rare in practice — the kernel signal API is reliable on Linux —
/// but the SIGINT arm must remain reachable regardless.)
pub(crate) async fn default_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let sigterm = async {
        match signal(SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "failed to install SIGTERM handler; only SIGINT will shut down gracefully"
                );
                // Park the SIGTERM arm forever — SIGINT stays armed
                // via the sibling `select!` branch.
                std::future::pending::<()>().await;
            }
        }
    };
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            if let Err(e) = result {
                tracing::error!(error = %e, "ctrl_c handler failed");
            } else {
                tracing::info!("received SIGINT; initiating graceful shutdown");
            }
        }
        _ = sigterm => {
            tracing::info!("received SIGTERM; initiating graceful shutdown");
        }
    }
}
