//! CHA-415 primitive #1 — Postgres-bound hot-MVCC dedup floor (read + write).
//!
//! Measures the *irreducible* cost of the hot tier's merge-on-read resolution:
//! Postgres executing the real `build_merge_resolved::<PgDialect>` dedup over an
//! upsert/tx/delete log at increasing depth, plus the write throughput of
//! appending versions to that log. Only Postgres is pinned — no Flight SQL, no
//! gRPC, no Arrow IPC. The synthetic workload is the one the `hot_mvcc_floor`
//! workload guard test (`tests/hot_mvcc_floor_workload.rs`) locks for correctness.
//!
//! Curve axes:
//!   - depth   = total versions in the log (the dedup scan size).
//!   - density = versions per `row_uuid` (how much latest-wins discarding the
//!     dedup does); `n_entities = depth / density` survive resolution.
//!
//! Scales are env-gated so a bare run is fast; the full floor is opt-in:
//!   PERF_FLOOR_MAX=1k|10k|100k|1m   (default 10k) — largest depth.
//!   PERF_FLOOR_DENSITY=1,10,100     (default "1,10").
//!
//! Requires live Postgres via `PENCA_DB_*` (e.g. `just perf-floor`); with no
//! such env the bench registers nothing and exits cleanly.
//!
//! NOTE: setup helpers are intentionally duplicated from the workload guard
//! test (benches and integration tests cannot share a module); the
//! `orch:run-cleanup` pass extracts the shared kernel.

use std::hint::black_box;
use std::sync::atomic::{AtomicI64, Ordering};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use penca_core::naming::{commit_tx_log_table, delete_log_table, upsert_log_table};
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::DbDriver;
use penca_db::driver::pg::PgDriver;
use penca_merge::ReadSnapshot;
use penca_merge::sql::build_merge_resolved;
use tokio::runtime::Runtime;
use uuid::Uuid;

/// Per-run quoted log-table names for one freshly-created data table.
struct Names {
    upsert_q: String,
    tx_q: String,
    upsert: String,
    delete: String,
    tx: String,
    branch_uuid: Uuid,
}

fn conninfo() -> Option<String> {
    let host = std::env::var("PENCA_DB_HOST").ok()?;
    let port = std::env::var("PENCA_DB_PORT").ok()?;
    let dbname = std::env::var("PENCA_DB_DBNAME").unwrap_or_else(|_| "penca".into());
    let user = std::env::var("PENCA_DB_USER").unwrap_or_else(|_| "penca".into());
    let password = std::env::var("PENCA_DB_PASSWORD").unwrap_or_else(|_| "penca".into());
    Some(format!(
        "postgres://{user}:{password}@{host}:{port}/{dbname}"
    ))
}

fn depths() -> Vec<u64> {
    let max = match std::env::var("PERF_FLOOR_MAX")
        .unwrap_or_else(|_| "10k".into())
        .as_str()
    {
        "1k" => 1_000,
        "100k" => 100_000,
        "1m" | "1M" => 1_000_000,
        _ => 10_000,
    };
    [1_000u64, 10_000, 100_000, 1_000_000]
        .into_iter()
        .filter(|d| *d <= max)
        .collect()
}

fn densities() -> Vec<u64> {
    std::env::var("PERF_FLOOR_DENSITY")
        .unwrap_or_else(|_| "1,10".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|d| *d >= 1)
        .collect()
}

/// Create a fresh catalog + one user data table (`name TEXT` PK), returning the
/// log-table names. Uses the real `create_catalog_tables` / `create_data_tables`
/// so the bench measures the production schema + `(tx_uuid,row_uuid)` indexes.
async fn fresh_table(driver: &PgDriver) -> Names {
    use arrow::datatypes::{DataType, Field, Schema};
    let catalog_uuid = Uuid::new_v4();
    let branch_uuid = Uuid::new_v4();
    let table_uuid = Uuid::new_v4();
    PgDialect::create_catalog_tables(driver, &catalog_uuid, &branch_uuid)
        .await
        .expect("create catalog tables");
    let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
    PgDialect::create_data_tables(
        driver,
        &table_uuid,
        &branch_uuid,
        &schema,
        &["name".to_string()],
    )
    .await
    .expect("create data tables");
    let upsert = upsert_log_table(&table_uuid, &branch_uuid);
    let delete = delete_log_table(&table_uuid, &branch_uuid);
    let tx = commit_tx_log_table(&catalog_uuid);
    Names {
        upsert_q: PgDialect::quote_identifier(&upsert),
        tx_q: PgDialect::quote_identifier(&tx),
        upsert,
        delete,
        tx,
        branch_uuid,
    }
}

/// Flush a chunk of multi-row INSERTs (system-generated UUID literals, safe).
async fn exec(driver: &PgDriver, sql: &str) {
    driver.execute_no_result(sql).await.expect("bulk insert");
}

/// Append `depth` versions across `depth/density` entities, each version in its
/// own committed tx with a strictly increasing `committed_at` (latest wins).
/// `clock_base` keeps `committed_at` monotonic across calls into the same log.
async fn populate(driver: &PgDriver, n: &Names, depth: u64, density: u64, clock: &AtomicI64) {
    let n_entities = (depth / density).max(1);
    let br = n.branch_uuid;
    let mut tx_rows: Vec<String> = Vec::new();
    let mut up_rows: Vec<String> = Vec::new();
    let chunk = 500usize;
    for _entity in 0..n_entities {
        // Distinct, random entity identity per call so appends never clash on PK.
        let row_uuid = Uuid::new_v4();
        for v in 0..density {
            let tx = Uuid::new_v4();
            // Globally-unique, monotonic committed_at so the commit_tx_log
            // (branch_uuid, commit_micros) unique index never collides —
            // including across write-bench iterations that share one table.
            let committed = clock.fetch_add(1, Ordering::Relaxed);
            // CHA-428: commit_seq_num (NOT NULL) reuses the monotonic-unique
            // `committed` so the (branch_uuid, commit_seq_num) unique index holds.
            tx_rows.push(format!(
                "('{tx}','{br}',{began},{committed},'floor','perf',{committed})",
                began = committed - 1,
            ));
            up_rows.push(format!("(gen_random_uuid(),'{row_uuid}','{tx}','v{v}')"));
            if tx_rows.len() >= chunk {
                flush(driver, n, &mut tx_rows, &mut up_rows).await;
            }
        }
    }
    flush(driver, n, &mut tx_rows, &mut up_rows).await;
}

async fn flush(driver: &PgDriver, n: &Names, tx_rows: &mut Vec<String>, up_rows: &mut Vec<String>) {
    if tx_rows.is_empty() {
        return;
    }
    exec(
        driver,
        &format!(
            "INSERT INTO {tx} (tx_uuid, branch_uuid, began_at_micros, commit_micros, comment, author, commit_seq_num) VALUES {}",
            tx_rows.join(","),
            tx = n.tx_q,
        ),
    )
    .await;
    exec(
        driver,
        &format!(
            "INSERT INTO {up} (version_uuid, row_uuid, tx_uuid, \"name\") VALUES {}",
            up_rows.join(","),
            up = n.upsert_q,
        ),
    )
    .await;
    tx_rows.clear();
    up_rows.clear();
}

fn hot_mvcc_floor(c: &mut Criterion) {
    let Some(conninfo) = conninfo() else {
        eprintln!("hot_mvcc_floor: PENCA_DB_* unset — skipping (run via `just perf-floor`)");
        return;
    };
    let rt = Runtime::new().expect("tokio runtime");
    let driver = rt
        .block_on(PgDriver::connect(&conninfo, 1, 8))
        .expect("connect pg");

    let depths = depths();
    let densities = densities();
    // Shared monotonic source for committed_at across all populate calls.
    let clock = AtomicI64::new(1_700_000_000_000_000);

    // READ floor: PG resolving the dedup over a pre-populated log.
    let mut rg = c.benchmark_group("hot_mvcc_read");
    rg.sample_size(10);
    for &depth in &depths {
        for &density in &densities {
            let n_entities = (depth / density).max(1);
            let names = rt.block_on(fresh_table(&driver));
            rt.block_on(populate(&driver, &names, depth, density, &clock));
            let sql = build_merge_resolved::<PgDialect>(
                &names.upsert,
                &names.delete,
                &names.tx,
                &["name"],
                None,
                &ReadSnapshot::AsOfMicros(i64::MAX),
                None,
            );
            rg.throughput(Throughput::Elements(n_entities));
            rg.bench_with_input(
                BenchmarkId::from_parameter(format!("depth{depth}_density{density}")),
                &sql,
                |b, sql| {
                    b.iter(|| {
                        rt.block_on(driver.execute(black_box(sql)))
                            .expect("resolve");
                    });
                },
            );
        }
    }
    rg.finish();

    // WRITE floor: appending versions to the hot log. The log is TRUNCATEd in
    // each iteration's (untimed) setup so every measured append runs into an
    // empty table — otherwise append cost into a growing B-tree index makes the
    // samples non-stationary and the reported floor drifts with table size.
    //
    // This measures raw bulk-INSERT throughput (the lower bound), NOT the
    // production idempotent append (deterministic version_uuid = xxh3(row_uuid,
    // tx_uuid) + `INSERT … ON CONFLICT`, pg.rs); the ON CONFLICT probe is
    // overhead above this floor.
    //
    // The timed routine also includes the client-side VALUES string-building, a
    // small fraction of per-row cost (dominated by the PG round trip + index
    // maintenance); hoisting it into untimed setup is a possible refinement.
    let mut wg = c.benchmark_group("hot_mvcc_write");
    wg.sample_size(10);
    let names = rt.block_on(fresh_table(&driver));
    for &depth in &depths {
        wg.throughput(Throughput::Elements(depth));
        wg.bench_with_input(
            BenchmarkId::from_parameter(format!("append{depth}")),
            &depth,
            |b, &depth| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            driver
                                .execute_no_result(&format!("TRUNCATE {}", names.tx_q))
                                .await
                                .expect("truncate commit_tx_log");
                            driver
                                .execute_no_result(&format!("TRUNCATE {}", names.upsert_q))
                                .await
                                .expect("truncate upsert_log");
                        });
                    },
                    |()| {
                        rt.block_on(populate(&driver, &names, depth, 1, &clock));
                    },
                    criterion::BatchSize::PerIteration,
                );
            },
        );
    }
    wg.finish();
}

criterion_group!(benches, hot_mvcc_floor);
criterion_main!(benches);
