use crate::auth::{runner_keys, secret_keys, PlatformAdmin};
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::runner_services::Runs;
use crate::repo::{audit, tenants};
use crate::AppState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunnerKeyRequest {
    pub name: String,
    pub runs: Runs,
}

async fn record(
    state: &AppState,
    admin: &PlatformAdmin,
    tenant: &tenants::Tenant,
    action: &str,
    id: Uuid,
    details: Value,
) {
    audit::record(
        &state.platform_db,
        audit::Change {
            tenant_id: Some(tenant.id),
            actor: &admin.subject,
            credential: admin.credential(),
            action,
            resource_type: "runner_key",
            resource_id: Some(&id.to_string()),
            details: Some(details),
        },
    )
    .await;
}

pub async fn create_runner_key(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path(slug): Path<String>,
    Json(req): Json<CreateRunnerKeyRequest>,
) -> PlatformResult<(StatusCode, Json<Value>)> {
    let name = secret_keys::name(&req.name, "Runner key")?;
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let (stored, key) = runner_keys::mint(
        &state.platform_db,
        tenant.id,
        &name,
        req.runs,
        &admin.subject,
    )
    .await?;
    info!(tenant = %slug, key_id = %stored.id, runs = stored.runs.as_str(), actor = %admin.subject, "Created runner key '{}'", stored.name);
    record(
        &state,
        &admin,
        &tenant,
        "runner_key.create",
        stored.id,
        json!({"name": stored.name, "runs": stored.runs, "prefix": stored.prefix}),
    )
    .await;
    Ok((
        StatusCode::CREATED,
        Json(json!({"tenant": slug, "key": key, "runner_key": stored})),
    ))
}

pub async fn list_runner_keys(
    State(state): State<Arc<AppState>>,
    _admin: PlatformAdmin,
    Path(slug): Path<String>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    let keys = crate::repo::runner_keys::list(&state.platform_db, tenant.id).await?;
    Ok(Json(json!({"tenant": slug, "keys": keys})))
}

pub async fn revoke_runner_key(
    State(state): State<Arc<AppState>>,
    admin: PlatformAdmin,
    Path((slug, id)): Path<(String, Uuid)>,
) -> PlatformResult<Json<Value>> {
    let tenant = tenants::require_by_slug(&state.platform_db, &slug).await?;
    if !crate::repo::runner_keys::revoke(&state.platform_db, tenant.id, id).await? {
        return Err(PlatformError::NotFound("Runner key not found".into()));
    }
    info!(tenant = %slug, key_id = %id, actor = %admin.subject, "Revoked runner key");
    record(&state, &admin, &tenant, "runner_key.revoke", id, json!({})).await;
    Ok(Json(json!({"tenant": slug, "id": id, "revoked": true})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_names_the_key_and_its_kind_of_runner_and_nothing_else() {
        let request: CreateRunnerKeyRequest =
            serde_json::from_value(json!({"name": "acme-runner", "runs": "web"}))
                .expect("a request");
        assert_eq!(
            (request.name.as_str(), request.runs),
            ("acme-runner", Runs::Web)
        );
        for refused in [
            json!({}),
            json!({"name": "a"}),
            json!({"name": "a", "runs": "wasm"}),
            json!({"name": "a", "runs": "native", "tenant": "globex"}),
        ] {
            assert!(
                serde_json::from_value::<CreateRunnerKeyRequest>(refused.clone()).is_err(),
                "{refused}"
            );
        }
    }
}
