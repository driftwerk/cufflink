use crate::api::extensions::Overrides;
use crate::auth::{AuthUser, DeployAuth};
use crate::deployer::native;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::deployments::Deployment;
use crate::repo::services::Status;
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use cufflink_types::{ServiceManifest, ServiceMode};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub async fn list_services(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
) -> PlatformResult<Json<Value>> {
    let client = state.platform_db.get().await?;
    let rows = client
        .query(
            "SELECT s.id, s.name, s.current_version, s.status, s.created_at, s.updated_at,
                    t.slug as tenant_slug, t.name as tenant_name,
                    d.manifest->>'mode' as mode, d.commit_hash,
                    COALESCE(d.manifest->'required_platform_features', '[]'::jsonb)
                        as required_platform_features
             FROM services s
             JOIN tenants t ON t.id = s.tenant_id
             LEFT JOIN deployments d ON d.service_id = s.id AND d.version = s.current_version
             WHERE s.tenant_id = $1
             ORDER BY s.name",
            &[&user.tenant_id],
        )
        .await?;

    let services: Vec<Value> = rows
        .iter()
        .map(|row| {
            json!({
                "id": row.get::<_, Uuid>("id").to_string(),
                "name": row.get::<_, String>("name"),
                "current_version": row.get::<_, i32>("current_version"),
                "status": row.get::<_, String>("status"),
                "mode": row.get::<_, Option<String>>("mode").unwrap_or_default(),
                "commit_hash": row.get::<_, Option<String>>("commit_hash"),
                "required_platform_features": row.get::<_, Value>("required_platform_features"),
                "tenant_slug": row.get::<_, String>("tenant_slug"),
                "tenant_name": row.get::<_, String>("tenant_name"),
                "created_at": row.get::<_, chrono::DateTime<chrono::Utc>>("created_at").to_rfc3339(),
                "updated_at": row.get::<_, chrono::DateTime<chrono::Utc>>("updated_at").to_rfc3339(),
            })
        })
        .collect();

    Ok(Json(json!({ "services": services })))
}

pub async fn get_service(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let deployments =
        crate::repo::deployments::list_by_service(&state.platform_db, service_id).await?;
    let current = crate::repo::deployments::running(&deployments, service.current_version);

    Ok(Json(json!({
        "id": service.id.to_string(),
        "name": service.name,
        "current_version": service.current_version,
        "status": service.status,
        "tenant_slug": user.tenant_slug,
        "created_at": service.created_at.to_string(),
        "updated_at": service.updated_at.to_string(),
        "current_manifest": current.map(|d| &d.manifest),
        "deployments_count": deployments.len(),
    })))
}

pub async fn permission_areas(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let _service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let services = state.registry.all_services().await;
    let registered = services
        .iter()
        .find(|s| s.service_id == service_id)
        .ok_or_else(|| PlatformError::NotFound("Service not in registry".into()))?;

    let areas: Vec<Value> = registered
        .manifest
        .authorization
        .as_ref()
        .map(|auth| {
            auth.areas
                .iter()
                .map(|area| {
                    json!({
                        "name": area.name,
                        "operations": area.operations,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Json(json!({ "areas": areas })))
}

pub async fn tenant_permission_areas(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path(tenant_id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let tenant_uuid: Uuid = tenant_id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid tenant ID".into()))?;

    if tenant_uuid != user.tenant_id {
        return Err(PlatformError::Forbidden(
            "Not authorized for this tenant".into(),
        ));
    }

    let services = state.registry.all_services().await;
    let mut areas: Vec<Value> = Vec::new();

    for svc in &services {
        if svc.tenant_id != tenant_uuid {
            continue;
        }
        if let Some(ref auth) = svc.manifest.authorization {
            for area in &auth.areas {
                areas.push(json!({
                    "service": svc.manifest.name,
                    "name": area.name,
                    "operations": area.operations,
                }));
            }
        }
    }

    Ok(Json(json!({ "areas": areas })))
}

pub async fn set_status(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let new_status = requested_status(&body)?;
    let current = crate::repo::deployments::current(&state.platform_db, service_id).await?;
    let mode = current.as_ref().map(Deployment::mode).transpose()?;

    crate::repo::services::update_status(&state.platform_db, service_id, new_status).await?;

    let registered = match new_status {
        Status::Inactive => {
            let _setup = state
                .subscription_tasks
                .lock_service_setup(&user.tenant_slug, &service.name)
                .await;
            state
                .registry
                .remove(service.tenant_id, &service.name)
                .await;
            state
                .subscription_tasks
                .stop_service(&user.tenant_slug, &service.name);
            Ok(())
        }
        Status::Active => {
            let refreshed = state.registry.refresh(service_id).await;
            state.reconciler.trigger();
            refreshed.map(drop)
        }
    };

    state.registry.notify_change().await;

    if let (Some(ServiceMode::Native), Some(deployment)) = (&mode, &current) {
        if new_status == Status::Active {
            let manifest: ServiceManifest = serde_json::from_value(deployment.manifest.clone())
                .map_err(|e| PlatformError::Internal(format!("Failed to parse manifest: {e}")))?;
            native::announce(
                &state,
                &service.identity(&user.tenant_slug),
                deployment,
                &manifest,
            )
            .await;
        } else {
            native::announce_removal(&state, &user.tenant_slug, service_id, &service.name).await;
        }
    }

    registered.map_err(|e| PlatformError::Internal(format!("Failed to reload registry: {}", e)))?;
    Ok(Json(json!({ "status": new_status, "name": service.name })))
}

fn requested_status(body: &Value) -> PlatformResult<Status> {
    body["status"]
        .as_str()
        .ok_or_else(|| PlatformError::BadRequest("Missing 'status' field".into()))?
        .parse()
        .map_err(|unknown: crate::repo::services::UnknownStatus| {
            PlatformError::BadRequest(unknown.to_string())
        })
}

pub async fn delete_service(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    Query(overrides): Query<Overrides>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    let mode = crate::repo::deployments::current_mode(&state.platform_db, service_id).await?;
    let is_native = matches!(mode, Some(ServiceMode::Native));

    let web_artifact_keys = crate::repo::deployments::artifact_keys(
        &state.platform_db.get().await?,
        service_id,
        &ServiceMode::Web,
    )
    .await?;

    let leaving =
        crate::deployer::eventing::leave(&state, service.tenant_id, &user.tenant_slug, service_id)
            .await?;
    let retiring = {
        let _setup = state
            .subscription_tasks
            .lock_service_setup(&user.tenant_slug, &service.name)
            .await;

        let retiring = crate::db_roles::provision::retiring(&state, &service).await;
        remove(&state, &service, is_native && !overrides.force).await?;
        state
            .registry
            .remove(service.tenant_id, &service.name)
            .await;
        state
            .subscription_tasks
            .stop_service(&user.tenant_slug, &service.name);
        retiring
    };
    if let Some(retiring) = retiring {
        retiring.retire(&state).await;
    }
    leaving.complete(&state).await;
    state.registry.notify_change().await;

    if !web_artifact_keys.is_empty() {
        let event_payload = json!({
            "service_id": service_id.to_string(),
            "service_name": service.name,
            "tenant_slug": user.tenant_slug,
        });
        let subject = format!("undeploy.web.{}.{}", user.tenant_slug, service.name);
        if let Err(e) = crate::nats_conn::publish_bounded(
            &state.nats,
            subject.clone(),
            None,
            event_payload.to_string().into(),
        )
        .await
        {
            tracing::warn!("Failed to publish undeploy event on {}: {}", subject, e);
        }

        if let Some(ref storage) = state.storage {
            for s3_key in &web_artifact_keys {
                if let Err(e) = storage.delete(s3_key).await {
                    tracing::warn!("Failed to delete S3 artifact {}: {}", s3_key, e);
                }
            }
        }
    }

    if is_native {
        crate::repo::audit::record(
            &state.platform_db,
            crate::repo::audit::Change {
                tenant_id: Some(service.tenant_id),
                actor: &user.subject,
                credential: user.credential(),
                action: "service.delete",
                resource_type: "service",
                resource_id: Some(&service_id.to_string()),
                details: Some(json!({
                    "name": service.name,
                    "mode": ServiceMode::Native.as_str(),
                    "force": overrides.force,
                })),
            },
        )
        .await;
        native::announce_removal(&state, &user.tenant_slug, service_id, &service.name).await;
    }
    native::delete_objects(&state, service_id).await;

    Ok(Json(json!({ "deleted": true })))
}

async fn remove(
    state: &AppState,
    service: &crate::repo::services::Service,
    unstranded: bool,
) -> PlatformResult<()> {
    let mut client = state.platform_db.get().await?;
    let tx = client.transaction().await?;
    if unstranded {
        if let Some(stranded) =
            crate::repo::extensions::lock_native(&tx, service.tenant_id, service.id, &[]).await?
        {
            return Err(PlatformError::Conflict(format!(
                "service '{}' is deployed calling extension '{}', which '{}' provides; redeploy \
                 it without the extension first, or pass force=true",
                stranded.service, stranded.extension, service.name
            )));
        }
    }
    crate::repo::services::delete_service(&tx, service.id).await?;
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_status_reads_and_answers_as_it_did() {
        for (body, status) in [
            (json!({"status": "active"}), Status::Active),
            (json!({"status": "inactive", "more": 1}), Status::Inactive),
        ] {
            assert_eq!(requested_status(&body).expect("status"), status, "{body}");
        }
        for (body, text) in [
            (json!({}), "Missing 'status' field"),
            (json!({"status": 1}), "Missing 'status' field"),
            (json!({"status": null}), "Missing 'status' field"),
            (json!("inactive"), "Missing 'status' field"),
            (
                json!({"status": "disabled"}),
                "Status must be 'active' or 'inactive'",
            ),
            (
                json!({"status": "Active"}),
                "Status must be 'active' or 'inactive'",
            ),
        ] {
            let refused = requested_status(&body).expect_err("refused");
            assert!(
                matches!(&refused, PlatformError::BadRequest(m) if m == text),
                "{body}: {refused}"
            );
        }
        for (status, wire) in [(Status::Active, "active"), (Status::Inactive, "inactive")] {
            assert_eq!(
                json!({ "status": status, "name": "notes" }),
                json!({ "status": wire, "name": "notes" })
            );
        }
    }
}
