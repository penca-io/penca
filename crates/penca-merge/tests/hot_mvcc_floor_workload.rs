//! Workload-correctness guard for the hot-MVCC floor bench.
//!
//! The `hot_mvcc_floor` criterion bench times Postgres executing
//! the real `build_merge_resolved::<PgDialect>` dedup over a synthetic upsert/
//! tx/delete log at increasing depth. A throughput number is only meaningful if
//! the synthetic workload actually exercises latest-wins MVCC resolution — a
//! degenerate workload (one version per row, no tombstones) would measure a
//! trivial query and report a misleadingly high floor.
//!
//! This test locks the workload: it builds the same synthetic logs the bench
//! populates (R distinct entities × d versions each, plus K tombstones), runs
//! the production resolver, and asserts it resolves the latest version per
//! entity and excludes tombstoned rows.
//!
//! Its job is to prove the bench measures a non-degenerate dedup, and to catch
//! a future regression in either the resolver or the bench's populator.
//!
//! Requires live Postgres (`PENCA_DB_*` env, e.g. via `just penca-up`); skips
//! cleanly when that env is absent so a bare `cargo test` doesn't hard-fail.

use arrow::datatypes::{DataType, Field, Schema};
use penca_core::naming::{commit_tx_log_table, delete_log_table, upsert_log_table};
use penca_db::dialect::Dialect;
use penca_db::dialect::pg::PgDialect;
use penca_db::driver::pg::PgDriver;
use penca_db::driver::{DbDriver, SqlValue};
use penca_merge::ReadSnapshot;
use penca_merge::sql::build_merge_resolved;
use sqlx::Row;
use uuid::Uuid;

/// Build a `postgres://` connection URL (sqlx accepts the URL form) from the
/// `PENCA_DB_*` env the rest of the perf tooling uses. Returns `None` when
/// unset so the test can skip.
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

/// Run a `SELECT count(*) AS n …` and return the scalar.
async fn scalar_count(driver: &PgDriver, sql: &str) -> i64 {
    let rows = driver.execute(sql).await.expect("count query");
    rows[0].try_get::<i64, _>("n").expect("n column")
}

#[tokio::test]
async fn hot_mvcc_dedup_resolves_latest_wins_and_excludes_tombstones() {
    let Some(conninfo) = conninfo() else {
        eprintln!("skip: PENCA_DB_* env unset (run under `just penca-up`)");
        return;
    };
    let driver = PgDriver::connect(&conninfo, 1, 4)
        .await
        .expect("connect pg");

    // Fresh identities per run so the test is isolated in the shared PG.
    let catalog_uuid = Uuid::new_v4();
    let branch_uuid = Uuid::new_v4();
    let table_uuid = Uuid::new_v4();

    PgDialect::create_catalog_tables(&driver, &catalog_uuid, &branch_uuid)
        .await
        .expect("create catalog tables");

    // One user column ("name"), PK on it — the dedup carries user_cols = ["name"].
    let user_schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
    let primary_keys = vec!["name".to_string()];
    PgDialect::create_data_tables(
        &driver,
        &table_uuid,
        &branch_uuid,
        &user_schema,
        &primary_keys,
    )
    .await
    .expect("create data tables");

    let upsert_log = upsert_log_table(&table_uuid, &branch_uuid);
    let delete_log = delete_log_table(&table_uuid, &branch_uuid);
    let commit_tx_log = commit_tx_log_table(&catalog_uuid);

    let upsert_log_q = PgDialect::quote_identifier(&upsert_log);
    let delete_log_q = PgDialect::quote_identifier(&delete_log);
    let commit_tx_log_q = PgDialect::quote_identifier(&commit_tx_log);

    // Workload shape: R entities, d versions each, K tombstoned.
    const R_ENTITIES: u64 = 50;
    const DENSITY: u64 = 3;
    const K_TOMBSTONED: u64 = 5;

    let entities: Vec<Uuid> = (0..R_ENTITIES).map(|_| Uuid::new_v4()).collect();
    let mut clock: i64 = 1_700_000_000_000_000;

    // Append d versions per entity, each in its own committed tx with a
    // strictly increasing committed_at so the last write wins.
    for row_uuid in &entities {
        for v in 0..DENSITY {
            let tx_uuid = Uuid::new_v4();
            clock += 1;
            insert_tx(&driver, &commit_tx_log_q, &tx_uuid, &branch_uuid, clock).await;
            driver
                .execute_no_result_params(
                    &format!(
                        "INSERT INTO {upsert_log_q} \
                         (version_uuid, row_uuid, tx_uuid, \"name\") \
                         VALUES (gen_random_uuid(), $1, $2, $3)"
                    ),
                    &[
                        SqlValue::Uuid(*row_uuid),
                        SqlValue::Uuid(tx_uuid),
                        SqlValue::Text(format!("v{v}")),
                    ],
                )
                .await
                .expect("insert upsert version");
        }
    }

    // Tombstone the first K entities with a delete committed after all upserts.
    for row_uuid in entities.iter().take(K_TOMBSTONED as usize) {
        let tx_uuid = Uuid::new_v4();
        clock += 1;
        insert_tx(&driver, &commit_tx_log_q, &tx_uuid, &branch_uuid, clock).await;
        // delete_log columns: (version_uuid, row_uuid, <pk: name>, tx_uuid).
        // write_seq_num auto-stamps via its DEFAULT nextval.
        driver
            .execute_no_result_params(
                &format!(
                    "INSERT INTO {delete_log_q} \
                     (version_uuid, row_uuid, \"name\", tx_uuid) \
                     VALUES (gen_random_uuid(), $1, $2, $3)"
                ),
                &[
                    SqlValue::Uuid(*row_uuid),
                    SqlValue::Text("tombstone".into()),
                    SqlValue::Uuid(tx_uuid),
                ],
            )
            .await
            .expect("insert tombstone");
    }

    // The production resolver, exactly as the bench will invoke it.
    let resolved = build_merge_resolved::<PgDialect>(
        &upsert_log,
        &delete_log,
        &commit_tx_log,
        &["name"],
        None,
        &ReadSnapshot::AsOfMicros(i64::MAX),
        None,
    );

    // The resolve emits a two-arm UNION — visible upserts
    // (is_delete = false) plus winning tombstones (is_delete = true). The live
    // delta is the is_delete = false subset (the consumer applies `WHERE NOT
    // is_delete`); the full row_uuid set is the exclusion set. These
    // live-count / live-latest / tombstone-absent checks therefore filter the
    // tombstone arm out, exactly as the read path does.

    // 1. Live count = R - K (tombstoned entities excluded).
    let total = scalar_count(
        &driver,
        &format!("SELECT count(*) AS n FROM ({resolved}) t WHERE NOT t.is_delete"),
    )
    .await;
    assert_eq!(
        total,
        (R_ENTITIES - K_TOMBSTONED) as i64,
        "resolved live-entity count",
    );

    // 2. A live entity resolves to its LATEST version (name = v{DENSITY-1}).
    let live = entities[K_TOMBSTONED as usize];
    let latest_name = format!("v{}", DENSITY - 1);
    let live_hits = scalar_count(
        &driver,
        &format!(
            "SELECT count(*) AS n FROM ({resolved}) t \
             WHERE row_uuid = '{live}'::uuid AND \"name\" = '{latest_name}'"
        ),
    )
    .await;
    assert_eq!(
        live_hits, 1,
        "live entity must resolve to its latest version"
    );

    // 3. A tombstoned entity is absent from the live delta (its winning
    // tombstone is in the resolved set, is_delete = true, so it feeds the
    // exclusion set but not the emitted rows).
    let dead = entities[0];
    let dead_hits = scalar_count(
        &driver,
        &format!(
            "SELECT count(*) AS n FROM ({resolved}) t \
             WHERE row_uuid = '{dead}'::uuid AND NOT t.is_delete"
        ),
    )
    .await;
    assert_eq!(
        dead_hits, 0,
        "tombstoned entity must be excluded from the live delta"
    );
}

/// Insert one committed commit_tx_log row (branch partition created by
/// `create_catalog_tables`). `began_at_micros` is `committed - 1`.
async fn insert_tx(
    driver: &PgDriver,
    commit_tx_log_q: &str,
    tx_uuid: &Uuid,
    branch_uuid: &Uuid,
    commit_micros: i64,
) {
    driver
        .execute_no_result_params(
            &format!(
                // commit_seq_num is the PRIMARY merge order key.
                // Reuse the monotonic-unique commit_micros ($4) as the
                // seq so each successive version's commit_seq_num strictly
                // increases (latest-wins) and the (branch_uuid, commit_seq_num)
                // unique index is satisfied.
                "INSERT INTO {commit_tx_log_q} \
                 (tx_uuid, branch_uuid, began_at_micros, commit_micros, comment, author, commit_seq_num) \
                 VALUES ($1, $2, $3, $4, $5, $6, $4)"
            ),
            &[
                SqlValue::Uuid(*tx_uuid),
                SqlValue::Uuid(*branch_uuid),
                SqlValue::Int64(commit_micros - 1),
                SqlValue::Int64(commit_micros),
                SqlValue::Text("cha-415 floor".into()),
                SqlValue::Text("perf-floor".into()),
            ],
        )
        .await
        .expect("insert tx");
}
