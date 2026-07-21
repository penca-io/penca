//! Catalog CRUD on the `catalog_store` table.

use penca_core::naming::CATALOG_STORE;
use penca_db::driver::{DbDriver, SqlValue};
use penca_proto::external::v1::Catalog;
use sqlx::postgres::PgRow;

use crate::convert::catalog_from_row;
use crate::helpers::qi;
use crate::{LifecycleManager, Result};

impl LifecycleManager {
    /// Get a catalog by UUID or name. Returns `None` if not found.
    ///
    /// 1 SQL query. CHA-236: `catalog_uuid` is random-minted server-side,
    /// so name resolution is a SELECT against `catalog_store` rather
    /// than a hash derivation.
    pub async fn get_catalog(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: Option<&str>,
        catalog_name: Option<&str>,
    ) -> Result<Option<Catalog>> {
        let (where_clause, value) = if let Some(uuid) = catalog_uuid {
            ("catalog_uuid = $1", SqlValue::uuid_str(uuid)?)
        } else if let Some(name) = catalog_name {
            ("catalog_name = $1", SqlValue::Text(name.to_string()))
        } else {
            return Ok(None);
        };

        let sql = format!(
            "SELECT catalog_uuid, catalog_name, catalog_owner, description \
             FROM {table} WHERE {where_clause}",
            table = qi(CATALOG_STORE),
        );
        let row = driver.fetch_optional(&sql, &[value]).await?;
        Ok(row.as_ref().map(catalog_from_row))
    }

    /// List all catalogs ordered by name.
    ///
    /// 1 SQL query.
    pub async fn list_catalogs(driver: &impl DbDriver<Row = PgRow>) -> Result<Vec<Catalog>> {
        let sql = format!(
            "SELECT catalog_uuid, catalog_name, catalog_owner, description \
             FROM {table} ORDER BY catalog_name",
            table = qi(CATALOG_STORE),
        );
        let rows = driver.execute(&sql).await?;
        Ok(rows.iter().map(catalog_from_row).collect())
    }

    /// Insert a new catalog.
    ///
    /// 1 SQL query.
    pub async fn create_catalog(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        catalog_name: &str,
        owner: &str,
        description: &str,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO {table} \
             (catalog_uuid, catalog_name, catalog_owner, description) \
             VALUES ($1, $2, $3, $4)",
            table = qi(CATALOG_STORE),
        );
        driver
            .execute_no_result_params(
                &sql,
                &[
                    SqlValue::uuid_str(catalog_uuid)?,
                    SqlValue::Text(catalog_name.to_string()),
                    SqlValue::Text(owner.to_string()),
                    SqlValue::Text(description.to_string()),
                ],
            )
            .await?;
        Ok(())
    }

    /// Update an existing catalog row.
    ///
    /// `new_name`, when set, renames the catalog (CHA-236). The
    /// `UNIQUE(catalog_name)` constraint on `catalog_store` enforces
    /// global uniqueness; a collision surfaces as a sqlx
    /// `unique_violation` and is mapped to `AlreadyExists` upstream.
    ///
    /// 1 SQL query.
    pub async fn update_catalog(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
        owner: &str,
        description: &str,
        new_name: Option<&str>,
    ) -> Result<()> {
        let mut set_fragments = vec![
            "catalog_owner = $1".to_string(),
            "description = $2".to_string(),
        ];
        let mut params: Vec<SqlValue> = vec![
            SqlValue::Text(owner.to_string()),
            SqlValue::Text(description.to_string()),
        ];
        if let Some(name) = new_name {
            set_fragments.push(format!("catalog_name = ${}", params.len() + 1));
            params.push(SqlValue::Text(name.to_string()));
        }
        let uuid_placeholder = format!("${}", params.len() + 1);
        params.push(SqlValue::uuid_str(catalog_uuid)?);
        let sql = format!(
            "UPDATE {table} SET {set} WHERE catalog_uuid = {uuid_placeholder}",
            table = qi(CATALOG_STORE),
            set = set_fragments.join(", "),
        );
        driver.execute_no_result_params(&sql, &params).await?;
        Ok(())
    }

    /// Delete a catalog by UUID.
    ///
    /// 1 SQL query.
    pub async fn delete_catalog(
        driver: &impl DbDriver<Row = PgRow>,
        catalog_uuid: &str,
    ) -> Result<()> {
        let sql = format!(
            "DELETE FROM {table} WHERE catalog_uuid = $1",
            table = qi(CATALOG_STORE),
        );
        driver
            .execute_no_result_params(&sql, &[SqlValue::uuid_str(catalog_uuid)?])
            .await?;
        Ok(())
    }

    /// List catalogs with optional owner filter and pagination.
    ///
    /// 1 SQL query.
    pub async fn list_catalogs_paginated(
        driver: &impl DbDriver<Row = PgRow>,
        owner: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Catalog>> {
        let (sql, params) = if let Some(owner) = owner {
            (
                format!(
                    "SELECT catalog_uuid, catalog_name, catalog_owner, description \
                     FROM {table} WHERE catalog_owner = $1 \
                     ORDER BY catalog_name LIMIT $2 OFFSET $3",
                    table = qi(CATALOG_STORE),
                ),
                vec![
                    SqlValue::Text(owner.to_string()),
                    SqlValue::Int64(limit),
                    SqlValue::Int64(offset),
                ],
            )
        } else {
            (
                format!(
                    "SELECT catalog_uuid, catalog_name, catalog_owner, description \
                     FROM {table} \
                     ORDER BY catalog_name LIMIT $1 OFFSET $2",
                    table = qi(CATALOG_STORE),
                ),
                vec![SqlValue::Int64(limit), SqlValue::Int64(offset)],
            )
        };
        let rows = driver.execute_params(&sql, &params).await?;
        Ok(rows.iter().map(catalog_from_row).collect())
    }
}
