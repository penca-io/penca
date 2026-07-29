use datafusion::execution::session_state::SessionStateBuilder;
use penca_db::driver::pg::PgDriver;
use penca_sql_server::config::ServerConfig;
use penca_sql_server::flight_sql::FlightSqlService;
use penca_sql_server::session::ConnSessionFactory;
use penca_storage_meta::LifecycleManager;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    penca_observability::init_tracing();
    let config = ServerConfig::from_env();

    // Arrow IPC is the wire format — same as in-memory layout, no serialization
    // overhead. See penca-datafusion crate-level docs for why this is gRPC
    // rather than in-process composition over penca-api's managers.
    let query_channel = Channel::from_shared(config.query_addr)?.connect().await?;
    let write_channel = Channel::from_shared(config.write_addr)?.connect().await?;

    // Orchestrator-only Pg pool: holds a per-(branch, table) advisory lock
    // around strict-INSERT to serialise the SELECT collision check against
    // the WriteData append (ADR 0006). No data plane traffic flows here.
    let pool =
        PgDriver::connect(&config.database_url, config.pg_pool_min, config.pg_pool_max).await?;

    // Template `SessionState` — all default UDFs / analyzer / optimizer
    // rules registered once. `ConnSessionFactory::mint` clones this
    // template once per accepted TCP connection and composes a fresh
    // per-conn `PencaCatalogProviderList` (frozen `(name, uuid)` snapshot)
    // over it.
    let template = SessionStateBuilder::new().with_default_features().build();

    // Fail-fast bootstrap check: the default catalog + default branch
    // must exist post-bootstrap. Both resolved `uuid`s are **discarded**
    // — they're verified-to-exist booleans, nothing more.
    //
    // `catalog_uuid` is server-minted and can change across
    // re-bootstrap. `branch_uuid` is catalog-scoped — the
    // `main` branch in the default catalog has a different uuid than
    // the `main` branch in a header-supplied non-default catalog.
    // Both reasons make a startup-cached uuid actively wrong as a
    // runtime fallback; `ConnSessionFactory` re-resolves both per-conn
    // against the live `catalog_store` / `branch_store`.
    let bootstrap_catalog =
        LifecycleManager::get_catalog(&pool, None, Some(&config.default_catalog))
            .await?
            .ok_or_else(|| {
                format!(
                    "default catalog {:?} not found in catalog_store; run penca-bootstrap first",
                    config.default_catalog
                )
            })?;
    LifecycleManager::get_branch_by_name(
        &pool,
        &bootstrap_catalog.catalog_uuid,
        &config.default_branch,
    )
    .await?
    .ok_or_else(|| {
        format!(
            "default branch {:?} not found in catalog {:?}; run penca-bootstrap first",
            config.default_branch, config.default_catalog
        )
    })?;

    // Per-TCP-connection session factory. Each accepted TCP
    // conn gets a fresh `ConnSession` minted via
    // `ConnSessionFactory::mint`, scoped to that conn for its lifetime
    // (closes when the TCP conn closes). The `x-penca-catalog` /
    // `x-penca-branch` headers on the first request pin catalog +
    // branch at mint; subsequent requests on the same conn re-validate
    // the headers against the conn's pinned values. See ADR 0007.
    let factory = ConnSessionFactory::new(
        config.default_catalog,
        config.default_branch,
        config.default_schema,
        query_channel.clone(),
        write_channel.clone(),
        pool.clone(),
        template,
        config.flight_statement_cache_capacity,
    );

    let service = FlightSqlService::new(query_channel, write_channel, pool, factory);

    tracing::info!(bind_addr = %config.bind_addr, "penca sql server starting");
    service.serve(config.bind_addr).await?;

    Ok(())
}
