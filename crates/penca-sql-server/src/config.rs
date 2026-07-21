//! Flight SQL server configuration.
//!
//! All values are required from environment variables — defaults live in
//! deployment config (.env, docker-compose, k8s), not in Rust code.

use penca_core::config::{required_env, required_env_parsed};

/// Configuration for the Flight SQL server.
///
/// The server's data plane (catalog discovery, query execution, write
/// dispatch) talks exclusively to the CHA-114 microservices over gRPC.
/// A direct Postgres pool is required only for orchestration concerns
/// — currently a per-(branch, table) advisory lock that serialises
/// concurrent strict-INSERTs across SQL clients (CHA-121, ADR 0006).
pub struct ServerConfig {
    /// gRPC address of the query microservice (e.g. "http://[::1]:50052").
    /// Used for read-only metadata discovery (catalog/schema/table lookups,
    /// branch validation) in addition to data reads.
    pub query_addr: String,
    /// gRPC address of the write microservice (e.g. "http://[::1]:50053").
    pub write_addr: String,
    /// Address to bind the Flight SQL server (e.g. "0.0.0.0:50060").
    pub bind_addr: String,
    /// Postgres connection string used solely for orchestrator-level
    /// advisory locks (no data reads/writes — those go through gRPC).
    pub database_url: String,
    /// Lower bound on the orchestrator Pg pool. Sized for the maximum
    /// concurrent strict-INSERT lock holders the server may serve.
    pub pg_pool_min: u32,
    /// Upper bound on the orchestrator Pg pool.
    pub pg_pool_max: u32,
    /// Catalog newly-minted sessions are pinned to **when no
    /// `x-penca-catalog` header is supplied** (CHA-253). The header
    /// wins when present; this env value is the deployment-level
    /// fallback. Immutable for the connection's lifetime once pinned —
    /// `SetSessionOptions(catalog: …)` / `SET catalog = ...`
    /// mid-session no-op on match and are rejected with
    /// `FAILED_PRECONDITION` ("fixed at handshake") on mismatch.
    /// `BEGIN` targets the connection's pinned catalog; cross-catalog
    /// DML / SELECT (auto-commit or open-tx) is rejected.
    pub default_catalog: String,
    /// Initial `default_schema` for newly-minted sessions. Mutable
    /// mid-session via `SET search_path` (Postgres) or the standard
    /// Flight SQL `SetSessionOptions(db_schema: …)` action.
    /// Both DataFusion's SELECT planner and the unqualified-DML path
    /// read from `SessionConfig.options.catalog.default_schema`. Per
    /// CHA-163 every catalog auto-creates a `public` schema at
    /// `CreateCatalog` time, so the typical value here is `"public"`
    /// and out-of-the-box SQL works without operator setup.
    pub default_schema: String,
    /// Branch newly-minted sessions target **when no `x-penca-branch`
    /// header is supplied** (CHA-119). The header wins when present;
    /// this env value is the deployment-level fallback. Immutable for
    /// the connection's lifetime once pinned — `SET branch = ...` /
    /// `SET penca.branch = ...` mid-session is rejected with
    /// `INVALID_ARGUMENT`. The typical value is `"main"` — every
    /// catalog `LifecycleManager::create_catalog_tables` bootstraps a
    /// `main` branch (CHA-163), so out-of-the-box writes work without
    /// operator setup.
    pub default_branch: String,
    /// Per-connection logical-plan cache capacity (CHA-355). `GetFlightInfo`
    /// stashes the statement-query plan under a server-minted handle; `DoGet`
    /// reuses it instead of re-planning. `0` disables the cache (every `DoGet`
    /// re-plans) — the deterministic miss configuration. Owned by deployment
    /// env (`SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY`); no in-code default.
    pub flight_statement_cache_capacity: usize,
}

impl ServerConfig {
    pub fn from_env() -> Self {
        Self {
            query_addr: required_env("QUERY_SERVICE_ADDR"),
            write_addr: required_env("WRITE_SERVICE_ADDR"),
            bind_addr: required_env("BIND_ADDR"),
            database_url: required_env("DATABASE_URL"),
            pg_pool_min: required_env_parsed("PG_POOL_MIN"),
            pg_pool_max: required_env_parsed("PG_POOL_MAX"),
            default_catalog: required_env("SQL_SERVER_DEFAULT_CATALOG"),
            default_schema: required_env("SQL_SERVER_DEFAULT_SCHEMA"),
            default_branch: required_env("SQL_SERVER_DEFAULT_BRANCH"),
            flight_statement_cache_capacity: required_env_parsed(
                "SQL_SERVER_FLIGHT_STATEMENT_CACHE_CAPACITY",
            ),
        }
    }
}
