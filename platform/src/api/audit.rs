use crate::auth::DeployAuth;
use crate::errors::PlatformResult;
use crate::AppState;
use axum::{
    extract::{Query, State},
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Deserialize, Default)]
pub struct AuditQuery {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub action: Option<String>,
    pub resource_type: Option<String>,
    pub actor: Option<String>,
}

pub async fn list_audit_log(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Query(params): Query<AuditQuery>,
) -> PlatformResult<Json<Value>> {
    let client = state.platform_db.get().await?;

    let per_page = params.per_page.unwrap_or(50).min(200);
    let page = params.page.unwrap_or(1).max(1);
    let offset = (page - 1) * per_page;

    let mut conditions = Vec::new();
    let mut query_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

    query_params.push(Box::new(user.tenant_id));
    conditions.push(format!("tenant_id = ${}", query_params.len()));

    if let Some(ref action) = params.action {
        query_params.push(Box::new(action.clone()));
        conditions.push(format!("action = ${}", query_params.len()));
    }
    if let Some(ref resource_type) = params.resource_type {
        query_params.push(Box::new(resource_type.clone()));
        conditions.push(format!("resource_type = ${}", query_params.len()));
    }
    if let Some(ref actor) = params.actor {
        query_params.push(Box::new(actor.clone()));
        conditions.push(format!("actor = ${}", query_params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_sql = format!("SELECT COUNT(*) FROM audit_log {}", where_clause);
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = query_params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let count_row = client.query_one(&count_sql, &param_refs).await?;
    let total: i64 = count_row.get(0);

    let data_sql = format!(
        "SELECT id, tenant_id, actor, action, resource_type, resource_id, details, ip_address, created_at
         FROM audit_log {} ORDER BY created_at DESC LIMIT {} OFFSET {}",
        where_clause, per_page, offset
    );

    let rows = client.query(&data_sql, &param_refs).await?;

    let entries: Vec<Value> = rows
        .iter()
        .map(|row| {
            let id: uuid::Uuid = row.get("id");
            let tenant_id: Option<uuid::Uuid> = row.get("tenant_id");
            let actor: String = row.get("actor");
            let action: String = row.get("action");
            let resource_type: String = row.get("resource_type");
            let resource_id: Option<String> = row.get("resource_id");
            let details: Option<Value> = row.get("details");
            let ip_address: Option<String> = row.get("ip_address");
            let created_at: DateTime<Utc> = row.get("created_at");

            json!({
                "id": id.to_string(),
                "tenant_id": tenant_id.map(|t| t.to_string()),
                "actor": actor,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "details": details,
                "ip_address": ip_address,
                "created_at": created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(json!({
        "entries": entries,
        "pagination": {
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total as f64 / per_page as f64).ceil() as i64,
        }
    })))
}
