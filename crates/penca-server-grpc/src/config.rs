//! Structured configuration for gRPC microservice binaries.
//!
//! Each binary has its own config struct — no shared base type. This lets
//! each service evolve its config independently without coupling.
//!
//! All values are required from environment variables — no defaults are baked in.
//! Defaults live in deployment config (.env, docker-compose, k8s manifests).

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use penca_core::Format;
use penca_core::config::{required_env, required_env_parsed};
use penca_format::reader::AnyFormatReader;
use penca_format::reader::lance::LanceFormatReader;
use penca_format::reader::parquet::ParquetFormatReader;
use penca_format::writer::AnyFormatWriter;
use penca_format::writer::lance::LanceFormatWriter;
use penca_format::writer::parquet::ParquetFormatWriter;

/// Config for the query microservice (database + object storage for reads).
pub struct QueryServiceConfig {
    pub database_url: String,
    pub bind_addr: String,
    pub pg_pool_min: u32,
    pub pg_pool_max: u32,
    pub default_page_size: i64,
    pub default_stream_batch_size: u32,
    /// `NonZeroU32` so the config layer rejects 0 at startup —
    /// `buffer_unordered(0)` deadlocks (the spawn loop never polls the
    /// upstream stream), so validating here keeps the request path
    /// from hanging on misconfiguration.
    pub segment_read_concurrency: NonZeroU32,
    /// CHA-353: skip snapshot segment pruning unless the planned segment count
    /// exceeds this. Pruning builds a full DataFusion plan of the filter; below
    /// the threshold the residual filter alone is cheaper. `0` always prunes;
    /// the deployment default is `1` (skip the single-segment case). Not
    /// `NonZero` — `0` is a valid setting.
    pub snapshot_prune_min_segments: u32,
    /// CHA-485: cap on the cartesian product of per-column IN-list bindings
    /// when the planner selects a covering user index for a filtered cold
    /// read. Over the cap the index is skipped (full scan + residual filter —
    /// never a truncated probe set). Not `NonZero` — `0` is a valid setting
    /// that disables user-index selection entirely (operator kill switch).
    pub index_seek_max_probe_tuples: u32,
    /// System-wide hard cap on `read_data` / `audit_data` execute-time
    /// duration. The query service enforces cancellation; the lifecycle
    /// service uses the same value as its destructive-side grace window
    /// (Purge of hot rows, compaction GC). Both services MUST read the
    /// same `QUERY_TIMEOUT_SECONDS` env var — the system invariant in
    /// ADR 0019 requires the cap and the grace window to agree.
    pub query_timeout_seconds: i64,
    /// Byte budget for the in-process snapshot-segment cache (CHA-252).
    /// Total resident decoded-segment weight is bounded to this; deployment
    /// env owns the value (no in-code default).
    pub snapshot_segment_cache_budget_bytes: i64,
    /// TTL (seconds) for the snapshot-list cache (CHA-441) — the immutable
    /// `(segments, W_snap)` baseline list. CORRECTNESS bound: a stale entry
    /// must never outlive the retired snapshot files it names, so the
    /// deployment MUST set this `<= min(snapshot interval,
    /// QUERY_TIMEOUT_SECONDS)` (the snapshot-retire GC grace, ADR 0019). No
    /// in-code default.
    pub snapshot_list_cache_ttl_seconds: i64,
    /// Max number of `(catalog, branch, table)` snapshot lists held in the
    /// CHA-441 cache. The entries are small structs, so this is an entry-count
    /// cap (not a byte budget like the decoded-segment cache). `0` disables.
    pub snapshot_list_cache_max_entries: i64,
}

impl QueryServiceConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: required_env("DATABASE_URL"),
            bind_addr: required_env("BIND_ADDR"),
            pg_pool_min: required_env_parsed("PG_POOL_MIN"),
            pg_pool_max: required_env_parsed("PG_POOL_MAX"),
            default_page_size: required_env_parsed("QUERY_DEFAULT_PAGE_SIZE"),
            default_stream_batch_size: required_env_parsed("QUERY_DEFAULT_STREAM_BATCH_SIZE"),
            segment_read_concurrency: required_env_parsed("QUERY_SEGMENT_READ_CONCURRENCY"),
            snapshot_prune_min_segments: required_env_parsed("QUERY_SNAPSHOT_PRUNE_MIN_SEGMENTS"),
            index_seek_max_probe_tuples: required_env_parsed("QUERY_INDEX_SEEK_MAX_PROBE_TUPLES"),
            query_timeout_seconds: required_env_parsed("QUERY_TIMEOUT_SECONDS"),
            snapshot_segment_cache_budget_bytes: required_env_parsed(
                "QUERY_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES",
            ),
            snapshot_list_cache_ttl_seconds: required_env_parsed(
                "QUERY_SNAPSHOT_LIST_CACHE_TTL_SECONDS",
            ),
            snapshot_list_cache_max_entries: required_env_parsed(
                "QUERY_SNAPSHOT_LIST_CACHE_MAX_ENTRIES",
            ),
        }
    }
}

/// Config for the write microservice — Postgres + object storage (cold writes
/// via the unified `upsert_log`/`delete_log` append). Read-path knobs
/// (`stream_batch_size`, `segment_read_concurrency`) are intentionally
/// absent: under ADR 0006 the WriteService no longer runs the merge-on-read
/// pipeline. Strict-INSERT collision checks and UPDATE/DELETE WHERE
/// resolution live in penca-sql-server.
pub struct WriteServiceConfig {
    pub database_url: String,
    pub bind_addr: String,
    pub pg_pool_min: u32,
    pub pg_pool_max: u32,
    pub default_tx_timeout_seconds: i64,
    pub max_tx_timeout_seconds: i64,
    /// CHA-472: byte budget for the write path's in-process snapshot-segment
    /// cache. See `QueryServiceConfig::snapshot_segment_cache_budget_bytes`.
    pub snapshot_segment_cache_budget_bytes: i64,
    /// CHA-472: TTL (seconds) for the write path's snapshot-list cache. Same
    /// CORRECTNESS bound as the query service — deployment MUST set this
    /// `<= min(snapshot interval, QUERY_TIMEOUT_SECONDS)` so a stale list never
    /// outlives the retired snapshot files it names (ADR 0019 / 0028).
    pub snapshot_list_cache_ttl_seconds: i64,
    /// CHA-472: max `(catalog, branch, table)` snapshot lists held in the write
    /// path's cache. `0` disables. See
    /// `QueryServiceConfig::snapshot_list_cache_max_entries`.
    pub snapshot_list_cache_max_entries: i64,
    /// CHA-273 rework: address of the lifecycle service the write path calls to
    /// flush the source branch hot→cold at `CreateBranch` (PersistBranch). The
    /// persist loop runs in the lifecycle pod, not the write pod, so the write
    /// service no longer carries a `LifecycleManager` or a segment-size knob.
    pub lifecycle_addr: String,
}

impl WriteServiceConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: required_env("DATABASE_URL"),
            bind_addr: required_env("BIND_ADDR"),
            pg_pool_min: required_env_parsed("PG_POOL_MIN"),
            pg_pool_max: required_env_parsed("PG_POOL_MAX"),
            default_tx_timeout_seconds: required_env_parsed("WRITE_DEFAULT_TX_TIMEOUT_SECONDS"),
            max_tx_timeout_seconds: required_env_parsed("WRITE_MAX_TX_TIMEOUT_SECONDS"),
            snapshot_segment_cache_budget_bytes: required_env_parsed(
                "WRITE_SNAPSHOT_SEGMENT_CACHE_BUDGET_BYTES",
            ),
            snapshot_list_cache_ttl_seconds: required_env_parsed(
                "WRITE_SNAPSHOT_LIST_CACHE_TTL_SECONDS",
            ),
            snapshot_list_cache_max_entries: required_env_parsed(
                "WRITE_SNAPSHOT_LIST_CACHE_MAX_ENTRIES",
            ),
            lifecycle_addr: required_env("LIFECYCLE_SERVICE_ADDR"),
        }
    }
}

/// Config for the lifecycle microservice.
pub struct LifecycleServiceConfig {
    pub database_url: String,
    pub bind_addr: String,
    pub pg_pool_min: u32,
    pub pg_pool_max: u32,
    pub default_max_segment_bytes: i64,
    /// See `QueryServiceConfig::segment_read_concurrency`.
    pub segment_read_concurrency: NonZeroU32,
    /// See `QueryServiceConfig::query_timeout_seconds`.
    pub query_timeout_seconds: i64,
    /// CHA-444 (ADR 0027) hot-purge grace window, in seconds: the bound below
    /// the persist watermark `P` within which a purged-from-hot row may still
    /// be needed (kept MVCC-safe, ADR 0027 §3). The expired-begin ledger GC
    /// uses it as its grace; CHA-466's memory-shedding `Pu <= P - hot_grace`
    /// ceiling reuses the same knob.
    pub hot_purge_grace_seconds: i64,
    /// Persist-loop cadence, in seconds — MUST equal the scheduler's
    /// `SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS` (shared env, like
    /// `QUERY_TIMEOUT_SECONDS`). Floors the expired-begin ledger-GC grace
    /// together with the snapshot cadence — see
    /// [`Self::purge_sweep_interval_seconds`]. Non-positive ⇒ that loop is
    /// disabled and contributes no floor.
    pub persist_tick_interval_seconds: i64,
    /// Snapshot-loop cadence, in seconds — MUST equal the scheduler's
    /// `SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS`. Non-positive ⇒ that loop is
    /// disabled and contributes no floor.
    pub snapshot_tick_interval_seconds: i64,
}

impl LifecycleServiceConfig {
    pub fn from_env() -> Self {
        Self {
            database_url: required_env("DATABASE_URL"),
            bind_addr: required_env("BIND_ADDR"),
            pg_pool_min: required_env_parsed("PG_POOL_MIN"),
            pg_pool_max: required_env_parsed("PG_POOL_MAX"),
            default_max_segment_bytes: required_env_parsed("LIFECYCLE_DEFAULT_MAX_SEGMENT_BYTES"),
            segment_read_concurrency: required_env_parsed("LIFECYCLE_SEGMENT_READ_CONCURRENCY"),
            query_timeout_seconds: required_env_parsed("QUERY_TIMEOUT_SECONDS"),
            hot_purge_grace_seconds: required_env_parsed("HOT_PURGE_GRACE_SECONDS"),
            persist_tick_interval_seconds: required_env_parsed(
                "SCHEDULER_PERSIST_TICK_INTERVAL_SECONDS",
            ),
            snapshot_tick_interval_seconds: required_env_parsed(
                "SCHEDULER_SNAPSHOT_TICK_INTERVAL_SECONDS",
            ),
        }
    }

    /// Worst-case gap between consecutive Purge sweeps, in seconds — the
    /// scheduler-cadence half of the expired-begin ledger-GC grace floor
    /// (ADR 0027 §5); `purge_tx_log` supplies the `hot_purge_grace_seconds`
    /// half, so the effective floor is the max of all three.
    ///
    /// Takes the max of BOTH loop cadences as a **conservative bound**, not
    /// because either alone would be wrong today. Purge currently rides the
    /// snapshot loop, so the snapshot cadence alone is the exact worst-case gap;
    /// the max is chosen because the loop-to-op assignment is not an invariant —
    /// CHA-502 moves Purge server-side behind `PurgeBranch`/`PurgeCatalog`, and a
    /// floor that encoded "snapshot owns Purge" would have to be re-pointed by
    /// hand, silently under-waiting if anyone forgot. Over-waiting is the safe
    /// direction: under-waiting strands a timed-out tx's hot rows forever.
    ///
    /// Retention consequence, deliberate: once the split env vars land the
    /// ledger-GC grace floors at whichever cadence is slower — in practice the
    /// snapshot loop, which wants to be long — rather than at one shared knob,
    /// so timed-out txs' `commit_tx_log` bookkeeping is held correspondingly
    /// longer. That is the cost of decoupling the cadences.
    ///
    /// Clamped at 0: a disabled loop (non-positive) contributes no floor.
    pub fn purge_sweep_interval_seconds(&self) -> i64 {
        self.persist_tick_interval_seconds
            .max(self.snapshot_tick_interval_seconds)
            .max(0)
    }

    /// [`Self::purge_sweep_interval_seconds`] in micros — the exact value
    /// `LifecycleManager::purge_sweep_interval_micros` takes, so the binary's
    /// wiring is one field assignment with no arithmetic of its own.
    pub fn purge_sweep_interval_micros(&self) -> i64 {
        self.purge_sweep_interval_seconds() * 1_000_000
    }
}

/// S3-compatible backend config (also used for SeaweedFS, MinIO).
pub struct S3BackendConfig {
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub endpoint: String,
    pub scheme: String,
}

/// Tagged object-storage backend: each variant owns only its own knobs.
pub enum StorageBackend {
    S3(S3BackendConfig),
    Local { path: String },
}

/// Object storage configuration (mirrors Python ObjectStorageSettings).
///
/// Supports S3-compatible (including SeaweedFS, MinIO) and local filesystem.
/// Builds both `object_store::ObjectStore` (for Parquet) and
/// `lance_io::object_store::ObjectStore` (for Lance) from the same config.
pub struct ObjectStorageConfig {
    pub backend: StorageBackend,
    pub format: Format,
}

impl ObjectStorageConfig {
    pub fn from_env() -> Self {
        let provider = required_env("OBJECT_STORAGE_PROVIDER");
        // `bucket` is read unconditionally so misconfigured deployments
        // fail at boot, even though only the S3 arm consumes it.
        let bucket = required_env("OBJECT_STORAGE_BUCKET");
        let format: Format = required_env("OBJECT_STORAGE_FORMAT")
            .parse()
            .unwrap_or_else(|e| panic!("unsupported storage format: {e}"));
        let backend = match provider.as_str() {
            "s3" => StorageBackend::S3(S3BackendConfig {
                bucket,
                access_key: std::env::var("OBJECT_STORAGE_ACCESS_KEY").unwrap_or_default(),
                secret_key: std::env::var("OBJECT_STORAGE_SECRET_KEY").unwrap_or_default(),
                region: std::env::var("OBJECT_STORAGE_REGION").unwrap_or_default(),
                endpoint: std::env::var("OBJECT_STORAGE_ENDPOINT").unwrap_or_default(),
                scheme: std::env::var("OBJECT_STORAGE_SCHEME").unwrap_or_default(),
            }),
            "local" => StorageBackend::Local {
                path: std::env::var("OBJECT_STORAGE_PATH").unwrap_or_default(),
            },
            other => panic!("unsupported object storage provider: {other}"),
        };
        Self { backend, format }
    }

    /// Build an `object_store::ObjectStore` for Parquet reader/writer.
    pub fn build_object_store(&self) -> Arc<dyn object_store::ObjectStore> {
        match &self.backend {
            StorageBackend::S3(s3) => {
                let mut builder = object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&s3.bucket)
                    .with_region(&s3.region);
                if !s3.access_key.is_empty() {
                    builder = builder.with_access_key_id(&s3.access_key);
                }
                if !s3.secret_key.is_empty() {
                    builder = builder.with_secret_access_key(&s3.secret_key);
                }
                if !s3.endpoint.is_empty() {
                    builder = builder.with_endpoint(&s3.endpoint);
                }
                if s3.scheme == "http" {
                    builder = builder.with_allow_http(true);
                }
                Arc::new(builder.build().expect("failed to build S3 object store"))
            }
            StorageBackend::Local { path } => Arc::new(
                LocalFileSystem::new_with_prefix(path).expect("failed to build local object store"),
            ),
        }
    }

    /// Build a `lance_io::object_store::ObjectStore` for Lance reader/writer.
    pub async fn build_lance_object_store(&self) -> Arc<lance_io::object_store::ObjectStore> {
        let base_uri = self.base_uri();
        let mut storage_options = HashMap::new();
        match &self.backend {
            StorageBackend::S3(s3) => {
                if !s3.access_key.is_empty() {
                    storage_options.insert("aws_access_key_id".to_string(), s3.access_key.clone());
                }
                if !s3.secret_key.is_empty() {
                    storage_options
                        .insert("aws_secret_access_key".to_string(), s3.secret_key.clone());
                }
                if !s3.region.is_empty() {
                    storage_options.insert("aws_region".to_string(), s3.region.clone());
                }
                if !s3.endpoint.is_empty() {
                    storage_options.insert("aws_endpoint".to_string(), s3.endpoint.clone());
                }
                if s3.scheme == "http" {
                    storage_options.insert("allow_http".to_string(), "true".to_string());
                }
            }
            StorageBackend::Local { .. } => {}
        }
        let params = lance_io::object_store::ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(
                lance_io::object_store::StorageOptionsAccessor::with_static_options(
                    storage_options,
                ),
            )),
            ..Default::default()
        };
        let (store, _) = lance_io::object_store::ObjectStore::from_uri_and_params(
            Arc::new(lance_io::object_store::ObjectStoreRegistry::default()),
            &base_uri,
            &params,
        )
        .await
        .expect("failed to build Lance object store");
        store
    }

    /// Build format readers for all supported formats.
    ///
    /// Always builds both Parquet and Lance readers — segments on disk
    /// can be in either format regardless of the configured write format.
    pub async fn build_readers(&self) -> HashMap<i32, AnyFormatReader> {
        let obj_store = self.build_object_store();
        let lance_store = self.build_lance_object_store().await;
        let base_uri = self.base_uri();
        let mut readers = HashMap::new();
        readers.insert(
            Format::Parquet.as_wire_code(),
            AnyFormatReader::Parquet(ParquetFormatReader::new(obj_store, base_uri.clone())),
        );
        readers.insert(
            Format::Lance.as_wire_code(),
            AnyFormatReader::Lance(LanceFormatReader::new(lance_store, base_uri)),
        );
        readers
    }

    /// Build a format writer for the configured format.
    pub async fn build_writer(&self) -> AnyFormatWriter {
        let base_uri = self.base_uri();
        match self.format {
            Format::Lance => {
                let obj_store = self.build_object_store();
                let lance_store = self.build_lance_object_store().await;
                AnyFormatWriter::Lance(LanceFormatWriter::new(lance_store, obj_store, base_uri))
            }
            Format::Parquet => {
                let obj_store = self.build_object_store();
                AnyFormatWriter::Parquet(ParquetFormatWriter::new(obj_store, base_uri))
            }
        }
    }

    /// Derive the base URI from provider + bucket/path.
    pub fn base_uri(&self) -> String {
        match &self.backend {
            StorageBackend::S3(s3) => format!("s3://{}", s3.bucket),
            StorageBackend::Local { path } => format!("file://{}", path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lifecycle_config(persist: i64, snapshot: i64) -> LifecycleServiceConfig {
        LifecycleServiceConfig {
            database_url: String::new(),
            bind_addr: String::new(),
            pg_pool_min: 1,
            pg_pool_max: 1,
            default_max_segment_bytes: 1,
            segment_read_concurrency: NonZeroU32::new(1).unwrap(),
            query_timeout_seconds: 900,
            hot_purge_grace_seconds: 60,
            persist_tick_interval_seconds: persist,
            snapshot_tick_interval_seconds: snapshot,
        }
    }

    /// The ledger-GC floor must not assume `snapshot > persist`. Both cadences
    /// are independently env-controlled, so the floor takes their max —
    /// under-waiting strands a timed-out tx's hot rows forever (ADR 0027 §5).
    #[test]
    fn purge_sweep_interval_is_order_independent() {
        // The load-bearing case: persist configured LONGER than snapshot.
        assert_eq!(
            lifecycle_config(10, 5).purge_sweep_interval_seconds(),
            10,
            "floor must follow the slower loop even when persist dominates"
        );
        assert_eq!(lifecycle_config(5, 30).purge_sweep_interval_seconds(), 30);
        assert_eq!(lifecycle_config(5, 5).purge_sweep_interval_seconds(), 5);
    }

    /// A disabled loop (non-positive cadence) contributes no floor; the hot-grace
    /// window then stands alone. Both disabled is the integration-test profile.
    ///
    /// The `(5, -1)` case floors on the persist cadence even though Purge rides
    /// the snapshot loop today — that is the conservative bound doing its job,
    /// not a claim that the persist loop issues Purge.
    #[test]
    fn disabled_loops_contribute_no_floor() {
        assert_eq!(lifecycle_config(-1, -1).purge_sweep_interval_seconds(), 0);
        assert_eq!(lifecycle_config(-1, 30).purge_sweep_interval_seconds(), 30);
        assert_eq!(lifecycle_config(5, -1).purge_sweep_interval_seconds(), 5);
        // Zero is "disabled" here too — the two crates must agree on that, or
        // the floor would credit a loop the scheduler never runs.
        assert_eq!(lifecycle_config(0, -1).purge_sweep_interval_seconds(), 0);
        assert_eq!(lifecycle_config(0, 30).purge_sweep_interval_seconds(), 30);
    }

    /// The binary assigns this straight to
    /// `LifecycleManager::purge_sweep_interval_micros`, so the seconds→micros
    /// conversion is pinned here rather than living unwatched in `main`.
    #[test]
    fn purge_sweep_micros_tracks_both_cadences() {
        assert_eq!(
            lifecycle_config(10, 5).purge_sweep_interval_micros(),
            10_000_000
        );
        assert_eq!(
            lifecycle_config(5, 30).purge_sweep_interval_micros(),
            30_000_000
        );
        assert_eq!(lifecycle_config(-1, -1).purge_sweep_interval_micros(), 0);
    }
}
