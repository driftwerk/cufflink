use crate::auth::extractor::require_permission_or_deploy;
use crate::auth::AuthUser;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::webhooks::{self, NewWebhook};
use crate::repo::{audit, webhook_deliveries};
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

const DELIVERIES_LISTED: i64 = 50;
const DELIVERIES_LISTED_MOST: i64 = 500;

async fn changed(state: &AppState, user: &AuthUser, action: &str, id: Uuid, details: Value) {
    state
        .webhooks
        .index
        .changed(&state.platform_db, &state.nats)
        .await;
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(user.tenant_id),
            actor: crate::audit::subject(user),
            credential: user.credential(),
            action,
            resource_type: "webhook",
            resource_id: Some(&id.to_string()),
            details: Some(details),
        },
    )
    .await;
}

fn host(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
}

#[derive(Deserialize)]
pub struct RegisterWebhookRequest {
    pub table_name: String,
    pub event: String,
    pub url: String,
    pub secret: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateWebhookRequest {
    pub active: bool,
}

pub async fn register_webhook(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(service_id): Path<Uuid>,
    Json(req): Json<RegisterWebhookRequest>,
) -> PlatformResult<Json<Value>> {
    require_permission_or_deploy(&state, &user, "webhooks", "manage").await?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    if !["create", "update", "delete"].contains(&req.event.as_str()) {
        return Err(PlatformError::BadRequest(
            "event must be 'create', 'update', or 'delete'".into(),
        ));
    }
    state
        .egress
        .resolve(&req.url)
        .await
        .map_err(|e| PlatformError::BadRequest(format!("webhook url refused: {e}")))?;

    let client = state.platform_db.get().await?;
    let webhook = webhooks::create(
        &state.secrets,
        &client,
        &NewWebhook {
            tenant_id: user.tenant_id,
            service_id,
            service_name: &service.name,
            table_name: &req.table_name,
            event: &req.event,
            url: &req.url,
            secret: &req.secret,
        },
    )
    .await?;
    drop(client);
    changed(
        &state,
        &user,
        "webhook.create",
        webhook.id,
        json!({
            "service": service.name, "table": webhook.table_name,
            "event": webhook.event, "host": host(&webhook.url),
        }),
    )
    .await;

    Ok(Json(json!({
        "id": webhook.id,
        "service_id": webhook.service_id,
        "table_name": webhook.table_name,
        "event": webhook.event,
        "url": webhook.url,
        "active": webhook.active,
        "created_at": webhook.created_at.to_rfc3339(),
    })))
}

pub async fn list_webhooks(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(service_id): Path<Uuid>,
) -> PlatformResult<Json<Value>> {
    require_permission_or_deploy(&state, &user, "webhooks", "manage").await?;
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let hooks = webhooks::list_by_service(&state.secrets, &client, service_id).await?;

    let items: Vec<Value> = hooks
        .iter()
        .map(|h| {
            json!({
                "id": h.id,
                "table_name": h.table_name,
                "event": h.event,
                "url": h.url,
                "active": h.active,
                "created_at": h.created_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(json!({ "webhooks": items })))
}

pub async fn delete_webhook(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((service_id, webhook_id)): Path<(Uuid, Uuid)>,
) -> PlatformResult<Json<Value>> {
    require_permission_or_deploy(&state, &user, "webhooks", "manage").await?;
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let deleted = webhooks::delete(&client, webhook_id, service_id)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to delete webhook: {}", e)))?;

    if !deleted {
        return Err(PlatformError::NotFound("Webhook not found".into()));
    }
    drop(client);
    changed(&state, &user, "webhook.delete", webhook_id, json!({})).await;

    Ok(Json(json!({ "deleted": true })))
}

pub async fn update_webhook(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((service_id, webhook_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateWebhookRequest>,
) -> PlatformResult<Json<Value>> {
    require_permission_or_deploy(&state, &user, "webhooks", "manage").await?;
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let updated = webhooks::set_active(&client, webhook_id, service_id, req.active)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to update webhook: {}", e)))?;

    if !updated {
        return Err(PlatformError::NotFound("Webhook not found".into()));
    }
    drop(client);
    let action = if req.active {
        "webhook.activate"
    } else {
        "webhook.deactivate"
    };
    changed(&state, &user, action, webhook_id, json!({})).await;

    Ok(Json(json!({ "id": webhook_id, "active": req.active })))
}

#[derive(Debug, Default, Deserialize)]
pub struct DeliveriesQuery {
    pub limit: Option<i64>,
}

impl DeliveriesQuery {
    fn limit(&self) -> PlatformResult<i64> {
        match self.limit.unwrap_or(DELIVERIES_LISTED) {
            limit @ 1..=DELIVERIES_LISTED_MOST => Ok(limit),
            limit => Err(PlatformError::Validation(format!(
                "limit must be between 1 and {DELIVERIES_LISTED_MOST}, not {limit}"
            ))),
        }
    }
}

pub async fn list_deliveries(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((service_id, webhook_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<DeliveriesQuery>,
) -> PlatformResult<Json<Value>> {
    require_permission_or_deploy(&state, &user, "webhooks", "manage").await?;
    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    let limit = query.limit()?;

    let client = state.platform_db.get().await?;
    if !webhooks::exists(&client, webhook_id, service_id).await? {
        return Err(PlatformError::NotFound("Webhook not found".into()));
    }
    let deliveries = webhook_deliveries::list(&client, webhook_id, limit).await?;

    Ok(Json(json!({ "deliveries": deliveries })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listing_of_deliveries_is_bounded() {
        let limit = |limit| DeliveriesQuery { limit }.limit();
        assert_eq!(limit(None).expect("the default"), 50);
        assert_eq!(limit(Some(1)).expect("one"), 1);
        assert_eq!(limit(Some(500)).expect("the most"), 500);
        for refused in [0, -1, 501] {
            assert!(
                matches!(limit(Some(refused)), Err(PlatformError::Validation(m))
                    if m == format!("limit must be between 1 and 500, not {refused}")),
                "{refused}"
            );
        }
    }
}
