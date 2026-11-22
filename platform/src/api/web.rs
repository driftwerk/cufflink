use crate::auth::{AuthUser, DeployAuth};
use crate::deployer::web;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::deployments::Artifact;
use crate::repo::services::Status;
use crate::wasm_runtime::sha256_hex;
use crate::AppState;
use axum::{
    body::Body,
    extract::{Path, RawQuery, State},
    response::{IntoResponse, Response},
    Json,
};
use cufflink_types::ServiceMode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveRequest {
    pub name: String,
}

fn service_id(id: &str) -> PlatformResult<Uuid> {
    id.parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))
}

pub async fn reserve_web_app(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Json(req): Json<ReserveRequest>,
) -> PlatformResult<Json<Value>> {
    let manifest = crate::deployer::ServiceManifest {
        name: req.name,
        mode: ServiceMode::Web,
        ..Default::default()
    };
    crate::deployer::validator::validate_manifest(&manifest)?;
    let pool = &state.platform_db;
    let existing = crate::repo::services::find(pool, user.tenant_id, &manifest.name).await?;
    if let Some(service) = &existing {
        let running = crate::repo::deployments::current_mode(pool, service.id).await?;
        web::require_web(&manifest.name, running.as_ref())?;
    }
    let service = crate::repo::services::find_or_create(
        pool,
        user.tenant_id,
        &manifest.name,
        &manifest.schema(),
        Status::Active,
    )
    .await?;
    Ok(Json(json!({
        "service_id": service.id.to_string(),
        "name": service.name,
        "current_version": service.current_version,
        "created": existing.is_none(),
    })))
}

async fn require_web_object(
    state: &AppState,
    service: &crate::repo::services::Service,
    object: web::Object,
    sha256: &str,
) -> PlatformResult<()> {
    object.require_sha256(sha256)?;
    let running = crate::repo::deployments::current_mode(&state.platform_db, service.id).await?;
    web::require_web(&service.name, running.as_ref())
}

async fn uploaded(
    state: &AppState,
    user: &AuthUser,
    service: &crate::repo::services::Service,
    stored: PlatformResult<(Artifact, web::Stored)>,
) -> PlatformResult<Json<Value>> {
    metrics::counter!("cufflink_web_uploads_total", "result" => web::upload_result(&stored))
        .increment(1);
    let (upload, stored) = stored?;

    crate::repo::audit::record(
        &state.platform_db,
        crate::repo::audit::Change {
            tenant_id: Some(user.tenant_id),
            actor: &user.subject,
            credential: user.credential(),
            action: "web.upload",
            resource_type: "service",
            resource_id: Some(&service.id.to_string()),
            details: Some(json!({ "sha256": upload.hash, "size": upload.size })),
        },
    )
    .await;
    info!(
        service = %service.name,
        sha256 = %upload.hash,
        size = upload.size,
        stored = stored.as_str(),
        "Web bundle uploaded"
    );
    Ok(Json(json!({
        "sha256": upload.hash,
        "size": upload.size,
        "s3_key": upload.s3_key,
        "stored": stored.as_str(),
    })))
}

pub async fn upload_web_bundle(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((id, sha256)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    let stored = async {
        require_web_object(&state, &service, web::Object::Bundle, &sha256).await?;
        web::Object::Bundle.require_content(&sha256, &sha256_hex(&body))?;
        web::store(&state, service_id, &sha256, body, &user.subject).await
    }
    .await;
    uploaded(&state, &user, &service, stored).await
}

#[derive(Debug, Default)]
struct PartHint {
    bundle: Option<String>,
    index: Option<u32>,
}

impl PartHint {
    fn of(query: Option<&str>) -> Self {
        form_urlencoded::parse(query.unwrap_or_default().as_bytes()).fold(
            Self::default(),
            |hint, (key, value)| match key.as_ref() {
                "bundle" if hint.bundle.is_none() => Self {
                    bundle: web::is_sha256(&value).then(|| web::short(&value).to_string()),
                    ..hint
                },
                "index" if hint.index.is_none() => Self {
                    index: value.parse().ok(),
                    ..hint
                },
                _ => hint,
            },
        )
    }
}

struct Unanswered<F: FnOnce(Duration)> {
    started: Instant,
    dropped: Option<F>,
}

impl<F: FnOnce(Duration)> Unanswered<F> {
    fn new(started: Instant, dropped: F) -> Self {
        Self {
            started,
            dropped: Some(dropped),
        }
    }

    fn answered(mut self) {
        self.dropped = None;
    }
}

impl<F: FnOnce(Duration)> Drop for Unanswered<F> {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            dropped(self.started.elapsed());
        }
    }
}

pub async fn upload_web_part(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((id, sha256)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    body: Body,
) -> Result<Json<Value>, Response> {
    let started = Instant::now();
    let hint = PartHint::of(query.as_deref());
    let (part, bundle, index) = (
        web::is_sha256(&sha256).then(|| web::short(&sha256)),
        hint.bundle.as_deref(),
        hint.index,
    );
    let body = crate::router::custom::body::received(body, web::PART_MAX_BYTES)
        .await
        .map_err(|unread| {
            warn!(
                service_id = %id,
                part,
                bundle,
                index,
                received = unread.received,
                receive_ms = web::millis(started.elapsed()),
                status = unread.refusal.status().as_u16(),
                "Web part not received"
            );
            unread.refusal
        })?;
    let (size, receive_ms) = (body.len(), web::millis(started.elapsed()));
    let unanswered = Unanswered::new(started, |elapsed| {
        warn!(
            service_id = %id,
            part,
            bundle,
            index,
            size,
            receive_ms,
            elapsed_ms = web::millis(elapsed),
            "Web part request dropped before its answer"
        );
    });
    let checked = async {
        let service_id = service_id(&id)?;
        let service =
            super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
        require_web_object(&state, &service, web::Object::Part, &sha256).await?;
        web::Object::Part.require_content(&sha256, &sha256_hex(&body))?;
        Ok::<_, PlatformError>(service_id)
    }
    .await;
    let (stored, store_ms) = match checked {
        Ok(service_id) => {
            let storing = Instant::now();
            let stored = web::store_part(&state, service_id, &sha256, body).await;
            (stored, Some(web::millis(storing.elapsed())))
        }
        Err(e) => (Err(e), None),
    };
    let elapsed_ms = web::millis(started.elapsed());
    unanswered.answered();
    match stored {
        Ok((part_stored, stored)) => {
            info!(
                service_id = %id,
                part,
                bundle,
                index,
                size,
                stored = stored.as_str(),
                receive_ms,
                store_ms,
                elapsed_ms,
                "Web part stored"
            );
            Ok(Json(json!({
                "sha256": part_stored.hash,
                "size": part_stored.size,
                "stored": stored.as_str(),
            })))
        }
        Err(e) => {
            warn!(
                service_id = %id,
                part,
                bundle,
                index,
                size,
                receive_ms,
                store_ms,
                elapsed_ms,
                error = %e,
                "Web part not stored"
            );
            Err(e.into_response())
        }
    }
}

pub async fn assemble_web_bundle(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((id, sha256)): Path<(String, String)>,
    Json(assembly): Json<web::Assembly>,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let bundle = sha256.clone();
    let unanswered = Unanswered::new(Instant::now(), |elapsed| {
        warn!(
            %service_id,
            bundle = web::short(&bundle),
            elapsed_ms = web::millis(elapsed),
            "Web bundle assembly request dropped before its answer; the assembly runs on"
        );
    });
    let assembled = tokio::spawn(async move {
        let service =
            super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
        let assembled = async {
            require_web_object(&state, &service, web::Object::Bundle, &sha256).await?;
            web::assemble(&state, service_id, &sha256, &assembly, &user.subject).await
        }
        .await;
        match assembled {
            Ok(web::Assembled::Incomplete(missing)) => Err(missing.into()),
            Ok(web::Assembled::Stored(stored)) => {
                uploaded(&state, &user, &service, Ok(stored)).await
            }
            Err(e) => uploaded(&state, &user, &service, Err(e)).await,
        }
    })
    .await;
    unanswered.answered();
    assembled.map_err(|e| {
        PlatformError::Internal(format!("The assembly of bundle {bundle} stopped: {e}"))
    })?
}

pub async fn upload_web_artifact(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;
    require_web_version(&state.platform_db, &service).await?;

    let sha256 = sha256_hex(&body);
    let (bundle, _) = web::store(&state, service_id, &sha256, body, &user.subject).await?;

    let lock =
        super::deployments::acquire_deploy_lock(&state, service_id, super::deployments::OPERATION)
            .await?;
    let released = async {
        let current = require_web_version(&state.platform_db, &service).await?;
        let released = web::release_legacy(&state, &current, &bundle).await?;
        web::sweep(&state, service_id).await;
        Ok::<_, PlatformError>(released)
    }
    .await;
    lock.release().await;
    let (deployment, released) = released?;

    if released {
        crate::deployer::module::announce(&state, service_id, None).await;
    }
    let runners_told =
        web::announce(&state, &service.identity(&user.tenant_slug), &deployment).await;

    info!(
        service = %service.name,
        version = deployment.version,
        sha256 = %bundle.hash,
        size = bundle.size,
        released,
        "Web bundle uploaded through the legacy route"
    );

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": deployment.version,
        "artifact_hash": bundle.hash,
        "artifact_s3_key": bundle.s3_key,
        "size_bytes": bundle.size,
        "released": released,
        "runners_told": runners_told,
    })))
}

async fn require_web_version(
    pool: &deadpool_postgres::Pool,
    service: &crate::repo::services::Service,
) -> PlatformResult<crate::repo::deployments::Deployment> {
    let current = crate::repo::deployments::current(pool, service.id).await?;
    match current {
        Some(current) if matches!(current.mode()?, ServiceMode::Web) => Ok(current),
        Some(_) | None => Err(PlatformError::Conflict(format!(
            "service '{}' is not a web app; only a web app's version takes a web artifact",
            service.name
        ))),
    }
}

pub async fn web_status(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let service_id = service_id(&id)?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let client = state.platform_db.get().await?;
    let row = client
        .query_opt(
            "SELECT artifact_s3_key, artifact_hash, artifact_size
             FROM deployments
             WHERE service_id = $1 AND version = $2 AND manifest->>'mode' = 'web'",
            &[&service_id, &service.current_version],
        )
        .await?;

    let (s3_key, hash, size): (Option<String>, Option<String>, Option<i64>) = match row {
        Some(r) => (r.get(0), r.get(1), r.get(2)),
        None => (None, None, None),
    };

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "version": service.current_version,
        "artifact_uploaded": s3_key.is_some(),
        "artifact_s3_key": s3_key,
        "artifact_hash": hash,
        "artifact_size": size,
    })))
}
