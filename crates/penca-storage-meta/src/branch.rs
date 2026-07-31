//! Branch CRUD on the per-catalog `branch_store` table.

use penca_core::naming;
use penca_db::driver::{DbDriver, SqlType, SqlValue};
use penca_proto::external::v1::Branch;
use sqlx::postgres::PgRow;

use crate::convert::branch_from_row;
use crate::helpers::{parse_uuid, qi};
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Create a branch record.
    ///
    /// 1 SQL query.
    ///
    /// `parent_branch_uuid` records the fork lineage (the source
    /// branch) so the read planner can enumerate the parent's cold tier as a
    /// second cold source, capped at the fork (`fork_commit_seq_num`, seeded
    /// by CHA-505). `None` for `main` / any non-forked branch — `main` is
    /// bootstrapped separately and carries a NULL parent.
    pub async fn create_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        branch_name: &str,
        fork_commit_seq_num: i64,
        fork_commit_micros: i64,
        parent_branch_uuid: Option<&str>,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "INSERT INTO {table} \
             (branch_uuid, branch_name, fork_commit_seq_num, fork_commit_micros, \
              parent_branch_uuid) \
             VALUES ($1, $2, $3, $4, $5)",
            table = qi(&table),
        );
        let parent = match parent_branch_uuid {
            Some(parent_uuid) => SqlValue::uuid_str(parent_uuid)?,
            None => SqlValue::Null(SqlType::Uuid),
        };
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(branch_uuid)?,
                    SqlValue::Text(branch_name.to_string()),
                    SqlValue::Int64(fork_commit_seq_num),
                    SqlValue::Int64(fork_commit_micros),
                    parent,
                ],
            )
            .await?;
        Ok(())
    }

    /// Delete a branch record. Returns `true` if the row existed.
    ///
    /// 1 SQL query.
    pub async fn delete_branch(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<bool> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "DELETE FROM {table} WHERE branch_uuid = $1 \
             RETURNING branch_uuid",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(!rows.is_empty())
    }

    /// Get a branch row by UUID.
    ///
    /// 1 SQL query.
    pub async fn get_branch_row(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
    ) -> Result<Option<Branch>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "SELECT branch_uuid, branch_name, fork_commit_seq_num \
             FROM {table} WHERE branch_uuid = $1",
            table = qi(&table),
        );
        let row = driver
            .fetch_optional(&sql, &[SqlValue::uuid_str(branch_uuid)?])
            .await?;
        Ok(row.as_ref().map(|r| branch_from_row(catalog_uuid, r)))
    }

    /// Get a branch row by name within a catalog (CHA-236).
    ///
    /// Replaces the deleted `naming::get_branch_uuid` hash-derivation
    /// for name → uuid resolution. `branch_store` carries
    /// `UNIQUE(branch_name)` so the lookup is at most one row.
    ///
    /// 1 SQL query.
    pub async fn get_branch_by_name(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_name: &str,
    ) -> Result<Option<Branch>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "SELECT branch_uuid, branch_name, fork_commit_seq_num \
             FROM {table} WHERE branch_name = $1",
            table = qi(&table),
        );
        let row = driver
            .fetch_optional(&sql, &[SqlValue::Text(branch_name.to_string())])
            .await?;
        Ok(row.as_ref().map(|r| branch_from_row(catalog_uuid, r)))
    }

    /// Rename a branch (CHA-236). `UNIQUE(branch_name)` on
    /// `branch_store` enforces per-catalog uniqueness; a collision
    /// surfaces as sqlx `unique_violation` and is mapped to
    /// `AlreadyExists` upstream.
    ///
    /// 1 SQL query.
    pub async fn update_branch_name(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        branch_uuid: &str,
        new_name: &str,
    ) -> Result<()> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "UPDATE {table} SET branch_name = $1 WHERE branch_uuid = $2",
            table = qi(&table),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::Text(new_name.to_string()),
                    SqlValue::uuid_str(branch_uuid)?,
                ],
            )
            .await?;
        Ok(())
    }

    /// List branches with pagination.
    ///
    /// 1 SQL query.
    pub async fn list_branches_paginated(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Branch>> {
        let catalog = parse_uuid(catalog_uuid);
        let table = naming::branch_store_table(&catalog);

        let sql = format!(
            "SELECT branch_uuid, branch_name, fork_commit_seq_num \
             FROM {table} ORDER BY branch_name \
             LIMIT $1 OFFSET $2",
            table = qi(&table),
        );
        let rows = driver
            .execute_params(&sql, &[SqlValue::Int64(limit), SqlValue::Int64(offset)])
            .await?;
        Ok(rows
            .iter()
            .map(|r| branch_from_row(catalog_uuid, r))
            .collect())
    }
}
