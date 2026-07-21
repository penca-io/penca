//! CHA-432: `LifecycleManager::retention_floor` — the newest `durable` snapshot
//! at/before the retention window start (`now − retention_duration_seconds`).
//!
//! This is the substrate read the downstream retention ops consume: the persist
//! prune (CHA-434) and snapshot retirement (CHA-55) call it directly; CHA-433
//! folds the same predicate onto the plan-time `hot_min` round trip. It must
//! return BOTH coordinates `(commit_seq_num, snapshotted_at_micros)` so each
//! consumer compares on the axis its `as_of`/`from` arrives on, with no
//! micros↔seq mapping.
//!
//! Fail-first TDD: red until `retention_floor` lands (the test target won't
//! build without the symbol). Behavioral cases run against live Postgres
//! (`PENCA_DB_*` env, e.g. via `just penca-up`) and skip cleanly when that env
//! is absent so a bare `cargo test` doesn't hard-fail.

use penca_core::naming;
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_db::driver::{DbDriver, SqlType, SqlValue};
use penca_storage_meta::LifecycleManager;
use uuid::Uuid;

/// Build a `postgres://` URL from the `PENCA_DB_*` env the perf/white-box
/// tooling uses. `None` (⇒ skip) when unset.
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

/// Seed one snapshot metadata row with an explicit `durable` flag and
/// committed-ness. Raw INSERT (not `insert_snapshot_metadata`) so the reads are
/// exercised independently of the assignment path. `committed = false` leaves
/// `commit_micros` NULL — an uncommitted (phase-1) parent that the reads must
/// exclude.
#[allow(clippy::too_many_arguments)]
async fn seed_snapshot(
    driver: &PgDriver,
    catalog_uuid: &Uuid,
    branch_uuid: &Uuid,
    table_uuid: &Uuid,
    snapshotted_at_micros: i64,
    commit_seq_num: i64,
    durable: bool,
    committed: bool,
) {
    let table = naming::table_snapshot_metadata_table(catalog_uuid);
    let sql = format!(
        "INSERT INTO {tbl} \
         (table_snapshot_uuid, branch_uuid, table_uuid, snapshotted_at_micros, \
          commit_seq_num, durable, commit_micros) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        tbl = PgDialect::quote_identifier(&table),
    );
    let commit_micros = if committed {
        SqlValue::Int64(snapshotted_at_micros)
    } else {
        SqlValue::Null(SqlType::Int64)
    };
    driver
        .execute_no_result_params(
            &sql,
            &[
                SqlValue::Uuid(Uuid::new_v4()),
                SqlValue::Uuid(*branch_uuid),
                SqlValue::Uuid(*table_uuid),
                SqlValue::Int64(snapshotted_at_micros),
                SqlValue::Int64(commit_seq_num),
                SqlValue::Bool(durable),
                commit_micros,
            ],
        )
        .await
        .expect("seed snapshot row");
}

#[tokio::test]
async fn retention_floor_picks_newest_durable_at_or_before_window() {
    let Some(conninfo) = conninfo() else {
        eprintln!("skip: PENCA_DB_* env unset (run under `just penca-up`)");
        return;
    };
    let driver = PgDriver::connect(&conninfo, 1, 4)
        .await
        .expect("connect pg");

    let catalog_uuid = Uuid::new_v4();
    let branch_uuid = Uuid::new_v4();
    let table_uuid = Uuid::new_v4();
    let catalog_str = catalog_uuid.to_string();
    let branch_str = branch_uuid.to_string();
    let table_str = table_uuid.to_string();

    PgDialect::create_catalog_tables(&driver, &catalog_uuid, &branch_uuid)
        .await
        .expect("create catalog tables");

    let base = 1_700_000_000_000_000i64;
    let sec = 1_000_000i64;

    // Ladder: durables at base+100s (seq 10) and base+200s (seq 20); a
    // non-durable at base+150s (seq 15, must be ignored); a newer durable at
    // base+300s (seq 30, after the window, must be excluded).
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 100 * sec,
        10,
        true,
        true,
    )
    .await;
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 150 * sec,
        15,
        false,
        true,
    )
    .await;
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 200 * sec,
        20,
        true,
        true,
    )
    .await;
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 300 * sec,
        30,
        true,
        true,
    )
    .await;

    // Case A — retention disabled (duration None) → None (no query issued).
    let none_duration = LifecycleManager::retention_floor(
        &driver,
        &catalog_str,
        &branch_str,
        &table_str,
        None,
        base + 300 * sec,
    )
    .await
    .expect("retention_floor none-duration");
    assert_eq!(
        none_duration, None,
        "duration None ⇒ retention disabled ⇒ null floor"
    );

    // Case B + boundary — window_start = now − 100s = base+200s. Newest durable
    // at/before it is the seq-20 rung (base+200s exactly, `<=` inclusive); the
    // seq-30 durable is after the window; the seq-15 non-durable is ignored.
    let floor = LifecycleManager::retention_floor(
        &driver,
        &catalog_str,
        &branch_str,
        &table_str,
        Some(100),
        base + 300 * sec,
    )
    .await
    .expect("retention_floor windowed");
    assert_eq!(
        floor,
        Some((20, base + 200 * sec)),
        "floor = newest durable at/before window start, both coordinates",
    );

    // Case C — no durable precedes the window (window_start = base) → None.
    let too_young = LifecycleManager::retention_floor(
        &driver,
        &catalog_str,
        &branch_str,
        &table_str,
        Some(100),
        base + 100 * sec,
    )
    .await
    .expect("retention_floor young-table");
    assert_eq!(
        too_young, None,
        "no durable at/before window start ⇒ null floor"
    );
}

#[tokio::test]
async fn last_durable_snapshot_at_excludes_uncommitted_and_non_durable() {
    let Some(conninfo) = conninfo() else {
        eprintln!("skip: PENCA_DB_* env unset (run under `just penca-up`)");
        return;
    };
    let driver = PgDriver::connect(&conninfo, 1, 4)
        .await
        .expect("connect pg");

    let catalog_uuid = Uuid::new_v4();
    let branch_uuid = Uuid::new_v4();
    let table_uuid = Uuid::new_v4();
    let catalog_str = catalog_uuid.to_string();
    let branch_str = branch_uuid.to_string();
    let table_str = table_uuid.to_string();

    PgDialect::create_catalog_tables(&driver, &catalog_uuid, &branch_uuid)
        .await
        .expect("create catalog tables");

    let base = 1_700_000_000_000_000i64;
    let sec = 1_000_000i64;

    // No durable yet → None.
    let empty =
        LifecycleManager::last_durable_snapshot_at(&driver, &catalog_str, &branch_str, &table_str)
            .await
            .expect("last_durable empty");
    assert_eq!(empty, None, "no durable snapshot ⇒ None");

    // Committed durable at base+100s (the answer); an *uncommitted* durable at
    // base+200s (commit_micros NULL — the phase-1 parent that must be excluded);
    // a committed non-durable at base+300s (must be excluded).
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 100 * sec,
        10,
        true,
        true,
    )
    .await;
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 200 * sec,
        20,
        true,
        false,
    )
    .await;
    seed_snapshot(
        &driver,
        &catalog_uuid,
        &branch_uuid,
        &table_uuid,
        base + 300 * sec,
        30,
        false,
        true,
    )
    .await;

    let last =
        LifecycleManager::last_durable_snapshot_at(&driver, &catalog_str, &branch_str, &table_str)
            .await
            .expect("last_durable");
    assert_eq!(
        last,
        Some(base + 100 * sec),
        "only the committed durable counts; the uncommitted durable and the \
         committed non-durable are excluded",
    );
}
