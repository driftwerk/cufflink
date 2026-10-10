use crate::auth::DeployAuth;
use crate::errors::PlatformResult;
use crate::tenant_sessions::{Purpose, Statements};
use crate::AppState;
use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::{json, Value};
use std::sync::Arc;

fn topological_sort(
    table_names: &[String],
    manifest: &cufflink_types::ServiceManifest,
) -> Vec<String> {
    use std::collections::HashSet;
    let seeded: HashSet<&str> = table_names.iter().map(String::as_str).collect();
    let needs: Vec<(&str, HashSet<&str>)> = table_names
        .iter()
        .map(|name| {
            let referenced = manifest
                .get_table(name)
                .into_iter()
                .flat_map(|table| &table.columns)
                .filter_map(|column| column.references.as_deref()?.split_once('.'))
                .map(|(table, _)| table)
                .filter(|table| table != name && seeded.contains(table))
                .collect();
            (name.as_str(), referenced)
        })
        .collect();
    let mut placed: Vec<&str> = Vec::with_capacity(needs.len());
    loop {
        let ready: Vec<&str> = needs
            .iter()
            .filter(|(name, needed)| {
                !placed.contains(name) && needed.iter().all(|table| placed.contains(table))
            })
            .map(|(name, _)| *name)
            .collect();
        if ready.is_empty() {
            break;
        }
        placed.extend(ready);
    }
    let cyclic: Vec<&str> = needs
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !placed.contains(name))
        .collect();
    placed.into_iter().chain(cyclic).map(String::from).collect()
}

pub async fn seed_handler(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(service_id): Path<uuid::Uuid>,
    Json(body): Json<Value>,
) -> PlatformResult<Json<Value>> {
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let tenant_pool = state
        .tenant_sessions
        .service(&service.identity(&user.tenant_slug), Purpose::Platform)
        .await?;
    let schema = tenant_pool.schema_ident();

    let manifest_json = crate::repo::deployments::current_manifest(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| {
            crate::errors::PlatformError::BadRequest("Service has no deployments".into())
        })?;
    let manifest: cufflink_types::ServiceManifest =
        serde_json::from_value(manifest_json).map_err(|e| {
            crate::errors::PlatformError::Internal(format!("Failed to parse manifest: {}", e))
        })?;

    let clear_existing = body
        .get("clear_existing")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let tables = body
        .get("tables")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            crate::errors::PlatformError::BadRequest("Body must contain 'tables' object".into())
        })?;

    let mut client = tenant_pool.get().await?;
    let txn = client.transaction().await?;

    let mut total_inserted = 0i64;
    let mut table_results = serde_json::Map::new();

    let table_names: Vec<String> = tables.keys().cloned().collect();
    let sorted_names = topological_sort(&table_names, &manifest);

    for table_name in &sorted_names {
        let rows = match tables.get(table_name) {
            Some(r) => r,
            None => continue,
        };

        let table = manifest.get_table(table_name).ok_or_else(|| {
            crate::errors::PlatformError::BadRequest(format!("Unknown table: {}", table_name))
        })?;

        let rows = rows.as_array().ok_or_else(|| {
            crate::errors::PlatformError::BadRequest(format!("'{}' must be an array", table_name))
        })?;

        if clear_existing {
            txn.execute(&format!("DELETE FROM {}.{}", schema, table_name), &[])
                .await?;
        }

        let mut inserted = 0i64;
        for row_data in rows {
            let obj = row_data.as_object().ok_or_else(|| {
                crate::errors::PlatformError::BadRequest("Each row must be a JSON object".into())
            })?;

            let mut col_names = Vec::new();
            let mut placeholders = Vec::new();
            let mut values: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

            for col in &table.columns {
                if let Some(val) = obj.get(&col.name) {
                    if !val.is_null() {
                        col_names.push(col.name.clone());
                        values.push(crate::router::row_convert::json_param(col, val)?);
                        placeholders.push(format!("${}", values.len()));
                    }
                } else if col.auto_generate && col.primary_key {
                    col_names.push(col.name.clone());
                    values.push(Box::new(uuid::Uuid::new_v4()));
                    placeholders.push(format!("${}", values.len()));
                }
            }

            if col_names.is_empty() {
                continue;
            }

            let pk_col = table
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.as_str())
                .unwrap_or("id");

            let update_set: Vec<String> = col_names
                .iter()
                .filter(|c| c.as_str() != pk_col)
                .map(|c| format!("{} = EXCLUDED.{}", c, c))
                .collect();

            let sql = if update_set.is_empty() {
                format!(
                    "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                    schema,
                    table_name,
                    col_names.join(", "),
                    placeholders.join(", "),
                    pk_col,
                )
            } else {
                format!(
                    "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
                    schema,
                    table_name,
                    col_names.join(", "),
                    placeholders.join(", "),
                    pk_col,
                    update_set.join(", "),
                )
            };

            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = values
                .iter()
                .map(|v| v.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            txn.execute(&sql, &param_refs).await.map_err(|e| {
                let detail = e
                    .as_db_error()
                    .map(|db| {
                        format!(
                            "{}: {} (detail: {}, hint: {})",
                            db.severity(),
                            db.message(),
                            db.detail().unwrap_or("-"),
                            db.hint().unwrap_or("-"),
                        )
                    })
                    .unwrap_or_else(|| format!("{:?}", e));
                crate::errors::PlatformError::BadRequest(format!(
                    "Seed insert failed on {}.{}: {} — SQL: {}",
                    schema, table_name, detail, sql
                ))
            })?;
            inserted += 1;
        }

        total_inserted += inserted;
        table_results.insert(table_name.clone(), json!(inserted));
    }

    txn.commit().await.map_err(|e| {
        crate::errors::PlatformError::BadRequest(format!("Seed commit failed: {}", e))
    })?;

    Ok(Json(json!({
        "seeded": true,
        "total_rows": total_inserted,
        "tables": table_results,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn referenced_tables_are_seeded_first_and_a_cycle_keeps_its_order() {
        let manifest: cufflink_types::ServiceManifest = serde_json::from_value(json!({
            "name": "shop", "version": "0.1.0", "mode": "crud", "publishes": [],
            "tables": [
                {"name": "lines", "columns": [
                    {"name": "order_id", "column_type": "uuid", "primary_key": false,
                     "nullable": false, "references": "orders.id"},
                    {"name": "item_id", "column_type": "uuid", "primary_key": false,
                     "nullable": false, "references": "items.id"}]},
                {"name": "orders", "columns": [
                    {"name": "customer_id", "column_type": "uuid", "primary_key": false,
                     "nullable": false, "references": "customers.id"}]},
                {"name": "items", "columns": [
                    {"name": "parent_id", "column_type": "uuid", "primary_key": false,
                     "nullable": true, "references": "items.id"}]},
                {"name": "customers", "columns": []},
                {"name": "a", "columns": [
                    {"name": "b_id", "column_type": "uuid", "primary_key": false,
                     "nullable": true, "references": "b.id"}]},
                {"name": "b", "columns": [
                    {"name": "a_id", "column_type": "uuid", "primary_key": false,
                     "nullable": true, "references": "a.id"}]},
            ]
        }))
        .expect("manifest");
        let names = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert_eq!(
            topological_sort(
                &names(&["lines", "orders", "items", "customers", "a", "b"]),
                &manifest
            ),
            names(&["items", "customers", "orders", "lines", "a", "b"])
        );
        assert_eq!(
            topological_sort(&names(&["lines", "orders"]), &manifest),
            names(&["orders", "lines"]),
            "a table that is not seeded is not waited for"
        );
    }
}
