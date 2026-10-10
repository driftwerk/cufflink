use crate::auth::DeployAuth;
use crate::deployer::eventing::{self, EventingPlan};
use crate::deployer::job::{self, detached, Job};
use crate::deployer::manifest::ServiceManifest;
use crate::deployer::module::{cleanup_orphans, UploadTracker};
use crate::deployer::native::{self, NativeBinary};
use crate::deployer::pipeline::{self, deploy_hash, DeployContext, DeployOutcome};
use crate::deployer::schema::{Options, Progress, SchemaReport};
use crate::deployer::validator::{
    module_features, validate_manifest, validate_platform_requirements,
};
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::ServiceIdentity;
use crate::repo::deploy_jobs::JobStatus;
use crate::repo::deploy_lock::DeployLock;
use crate::repo::deployments::Snapshot;
use crate::repo::services::Status;
use crate::wasm_runtime::sha256_hex;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use cufflink_types::manifest::validate::{active_refusal, inactive_refusal};
use cufflink_types::ServiceMode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployRequest {
    pub manifest: ServiceManifest,
    #[serde(default)]
    pub allow_destructive: bool,
    #[serde(default)]
    pub accept_drift: bool,
    #[serde(default)]
    pub purge: bool,
    #[serde(default)]
    pub commit_hash: Option<String>,
    #[serde(default)]
    pub wasm_base64: Option<String>,
    #[serde(default)]
    pub native_base64: Option<String>,
    #[serde(default)]
    pub cli_version: Option<String>,
    #[serde(default)]
    pub api_version: Option<u32>,
}

pub const DEPLOY_API_VERSION: u32 = 1;

pub(crate) const FEATURES: &[&str] = &[cufflink_types::features::INACTIVE_DEPLOY];

#[derive(Debug, Default, Deserialize)]
pub struct DeployQuery {
    #[serde(default)]
    pub plan: bool,
    #[serde(default, rename = "async")]
    pub detach: bool,
    #[serde(default)]
    pub inactive: bool,
}

fn admit_status(
    name: &str,
    existing: Option<Status>,
    mode: &ServiceMode,
    inactive: bool,
) -> PlatformResult<Status> {
    if let Some(refusal) = inactive_refusal(mode).filter(|_| inactive) {
        return Err(PlatformError::Validation(refusal.into()));
    }
    match (existing, inactive) {
        (Some(Status::Active), true) => Err(PlatformError::Conflict(active_refusal(name))),
        (Some(status), _) => Ok(status),
        (None, true) => Ok(Status::Inactive),
        (None, false) => Ok(Status::Active),
    }
}

pub async fn deploy_service(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    axum::extract::Query(query): axum::extract::Query<DeployQuery>,
    Json(req): Json<DeployRequest>,
) -> PlatformResult<(StatusCode, Json<Value>)> {
    let admitted = admit(&state, &user, req, query.inactive).await?;
    if query.plan {
        let plan = plan(state.clone(), user, admitted);
        let plan = detached(&state.shutdown, "deploy_plan", plan).await?;
        return Ok((StatusCode::OK, Json(plan)));
    }
    if query.detach {
        return begin(&state, user, admitted).await;
    }
    let done = detached(
        &state.shutdown,
        "deploy",
        deploy(state.clone(), user, admitted),
    );
    Ok((StatusCode::OK, Json(done.await?)))
}

async fn deploy(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    admitted: Admitted,
) -> PlatformResult<Value> {
    let claim = claim(&state, &user, &admitted).await?;
    perform(state, user, admitted, claim, Progress::default()).await
}

struct Claim {
    service: crate::repo::services::Service,
    lock: DeployLock,
}

async fn claim(
    state: &AppState,
    user: &crate::auth::AuthUser,
    admitted: &Admitted,
) -> PlatformResult<Claim> {
    let manifest = &admitted.manifest;
    let service = crate::repo::services::find_or_create(
        &state.platform_db,
        user.tenant_id,
        &manifest.name,
        &manifest.schema(),
        admitted.status(None)?,
    )
    .await?;
    admitted.status(Some(service.status))?;
    let lock = acquire_deploy_lock(state, service.id, "deployment").await?;
    Ok(Claim { service, lock })
}

struct Admitted {
    manifest: ServiceManifest,
    manifest_hash: String,
    wasm_bytes: Option<Vec<u8>>,
    wasm_hash: Option<String>,
    native: Option<NativeBinary>,
    commit_hash: Option<String>,
    allow_destructive: bool,
    accept_drift: bool,
    purge: bool,
    inactive: bool,
}

impl Admitted {
    fn status(&self, existing: Option<Status>) -> PlatformResult<Status> {
        admit_status(
            &self.manifest.name,
            existing,
            &self.manifest.mode,
            self.inactive,
        )
    }

    fn mode_label(&self) -> &'static str {
        self.manifest.mode.as_str()
    }

    fn native_hash(&self) -> Option<&str> {
        self.native.as_ref().map(|binary| binary.hash.as_str())
    }

    fn options(&self, plan_only: bool, progress: Progress) -> Options {
        Options {
            allow_destructive: self.allow_destructive,
            accept_drift: self.accept_drift,
            purge: self.purge,
            plan_only,
            progress,
        }
    }

    fn request(&self, status: Status) -> Value {
        json!({
            "service": self.manifest.name,
            "manifest_hash": self.manifest_hash,
            "wasm_hash": self.wasm_hash,
            "native_hash": self.native_hash(),
            "commit_hash": self.commit_hash,
            "allow_destructive": self.allow_destructive,
            "accept_drift": self.accept_drift,
            "purge": self.purge,
            "status": status,
        })
    }
}

async fn admit(
    state: &AppState,
    user: &crate::auth::AuthUser,
    req: DeployRequest,
    inactive: bool,
) -> PlatformResult<Admitted> {
    let manifest = req.manifest;
    let mode_label = manifest.mode.as_str();

    if let Some(v) = req.api_version {
        if v > DEPLOY_API_VERSION {
            metrics::counter!(
                "cufflink_deployments_total",
                "result" => "rejected",
                "mode" => mode_label,
            )
            .increment(1);
            return Err(PlatformError::BadRequest(format!(
                "CLI speaks deploy api_version={} but this platform only supports up to {}. \
                 Upgrade the platform before deploying.",
                v, DEPLOY_API_VERSION
            )));
        }
    }

    metrics::counter!(
        "cufflink_deployments_total",
        "result" => "attempted",
        "mode" => mode_label,
    )
    .increment(1);

    validate_manifest(&manifest)?;
    let extensions = &state.config.extensions;
    native::allowed(extensions, &manifest.mode)?;
    let native = native::binary(
        &manifest.mode,
        req.native_base64.as_deref(),
        req.wasm_base64.is_some(),
        extensions.native_max_bytes,
    )?;
    if native.is_some() {
        native::storage(state)?;
    }
    let wasm_bytes = decode_wasm_payload(req.wasm_base64.as_deref())?;
    require_platform_features(state, &manifest, wasm_bytes.as_deref())?;
    pipeline::admit(state, &manifest, user.tenant_id, &user.tenant_slug).await?;

    info!(
        service = %manifest.name,
        mode = mode_label,
        cli_version = req.cli_version.as_deref().unwrap_or("unknown"),
        "Deploying service"
    );
    if manifest.requires_wasm() && wasm_bytes.is_none() {
        metrics::counter!(
            "cufflink_deployments_total",
            "result" => "rejected",
            "mode" => mode_label,
        )
        .increment(1);
        metrics::counter!("cufflink_deployment_wasm_missing_total").increment(1);
        return Err(PlatformError::BadRequest(format!(
            "Service '{}' declares user code ({}) but the deploy request did not include \
             a WASM binary (wasm_base64). The CLI must be configured to bundle a WASM \
             artifact for this service. Ensure your Cargo.toml has `[lib] crate-type = \
             [\"cdylib\"]`, your `src/lib.rs` defines the declared handlers via the \
             `handler!` macro, and the CLI version is up to date \
             (compare GET /api/version to `cufflink --version`).",
            manifest.name,
            wasm_required_reason(&manifest)
        )));
    }
    Ok(Admitted {
        manifest_hash: deploy_hash(&manifest)?,
        wasm_hash: wasm_bytes.as_deref().map(sha256_hex),
        manifest,
        wasm_bytes,
        native,
        commit_hash: req.commit_hash,
        allow_destructive: req.allow_destructive,
        accept_drift: req.accept_drift,
        purge: req.purge,
        inactive,
    })
}

async fn plan(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    admitted: Admitted,
) -> PlatformResult<Value> {
    let manifest = &admitted.manifest;
    let existing =
        crate::repo::services::find(&state.platform_db, user.tenant_id, &manifest.name).await?;
    let status = admitted.status(existing.as_ref().map(|service| service.status))?;
    let (manifest_hash, wasm_hash) = (&admitted.manifest_hash, admitted.wasm_hash.as_deref());
    let (identity, version) = match &existing {
        Some(service) => (service.identity(&user.tenant_slug), service.current_version),
        None => (
            ServiceIdentity {
                tenant_id: user.tenant_id,
                tenant_slug: user.tenant_slug.clone(),
                service_id: Uuid::nil(),
                service_name: manifest.name.clone(),
                schema: manifest.schema(),
            },
            0,
        ),
    };
    let ctx = DeployContext {
        state: &state,
        identity,
        manifest,
        manifest_hash,
        wasm_bytes: admitted.wasm_bytes.as_deref(),
        wasm_hash,
        native: admitted.native.as_ref(),
        commit_hash: None,
        actor: &user.subject,
        schema: admitted.options(true, Progress::default()),
    };
    let (schema, eventing, skipped) = match &existing {
        Some(service) => {
            let lock = acquire_deploy_lock(&state, service.id, OPERATION).await?;
            let outcome = pipeline::run_locked(&ctx, &UploadTracker::default()).await;
            lock.release().await;
            match outcome? {
                DeployOutcome::Planned { schema, eventing } => (*schema, eventing, false),
                DeployOutcome::Skipped { .. } => {
                    (SchemaReport::default(), EventingPlan::default(), true)
                }
                DeployOutcome::Deployed { .. } => {
                    return Err(PlatformError::Internal(
                        "a plan deployed; nothing should have".into(),
                    ))
                }
            }
        }
        None => {
            let eventing = eventing::prepare_version(&state, manifest, &ctx.identity, 1).await?;
            (
                crate::deployer::schema::plan(&ctx, 0, &Snapshot::default()).await?,
                eventing,
                false,
            )
        }
    };
    Ok(json!({
        "name": manifest.name,
        "service_id": existing.as_ref().map(|service| service.id.to_string()),
        "version": version,
        "new": existing.is_none(),
        "status": status,
        "skipped": skipped,
        "manifest_hash": manifest_hash,
        "wasm_hash": wasm_hash,
        "native_hash": admitted.native_hash(),
        "plan": pipeline::deploy_plan(&schema, &eventing)?,
        "api_version": DEPLOY_API_VERSION,
    }))
}

async fn begin(
    state: &Arc<AppState>,
    user: crate::auth::AuthUser,
    admitted: Admitted,
) -> PlatformResult<(StatusCode, Json<Value>)> {
    let Claim { service, lock } = claim(state, &user, &admitted).await?;
    let request = admitted.request(service.status);
    let created = crate::repo::deploy_jobs::create(
        &state.platform_db,
        service.id,
        &job::replica_name(),
        &request,
        &user.subject,
    )
    .await;
    let id = match created {
        Ok(job) => job.id,
        Err(e) => {
            lock.release().await;
            return Err(e);
        }
    };
    let job = Job {
        pool: state.platform_db.clone(),
        shutdown: state.shutdown.clone(),
        id,
    };
    let progress = Progress::recorded(state.platform_db.clone(), id);
    let deploying = Arc::clone(state);
    state.shutdown.spawn(
        "deploy",
        job.run(lock, move |lock| {
            perform(deploying, user, admitted, Claim { service, lock }, progress)
        }),
    );
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "deploy_id": id, "status": JobStatus::Queued })),
    ))
}

pub async fn deploy_status(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
) -> PlatformResult<Json<Value>> {
    let id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid deploy ID".into()))?;
    let job = crate::repo::deploy_jobs::get_for_tenant(&state.platform_db, id, user.tenant_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound(format!("Deploy {id} not found")))?;
    Ok(Json(json!({
        "deploy_id": job.id,
        "service_id": job.service_id,
        "status": job.status,
        "replica": job.replica,
        "request": job.request,
        "progress": job.progress,
        "result": job.result,
        "error": job.error,
        "created_at": job.created_at,
        "heartbeat_at": job.heartbeat_at,
        "finished_at": job.finished_at,
    })))
}

async fn perform(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    admitted: Admitted,
    Claim { service, lock }: Claim,
    progress: Progress,
) -> PlatformResult<Value> {
    let uploads = UploadTracker::default();
    let mode_label = admitted.mode_label();
    let (manifest, wasm_bytes, commit_hash) = (
        &admitted.manifest,
        admitted.wasm_bytes.as_deref(),
        admitted.commit_hash.as_deref(),
    );
    let tenant_id = user.tenant_id;
    let tenant_slug = user.tenant_slug.clone();
    let (manifest_hash, wasm_hash) = (&admitted.manifest_hash, admitted.wasm_hash.as_deref());
    let native_hash = admitted.native_hash();
    let ctx = DeployContext {
        state: &state,
        identity: service.identity(&tenant_slug),
        manifest,
        manifest_hash,
        wasm_bytes,
        wasm_hash,
        native: admitted.native.as_ref(),
        commit_hash,
        actor: &user.subject,
        schema: admitted.options(false, progress.clone()),
    };
    let outcome = pipeline::run_locked(&ctx, &uploads).await;
    if outcome.is_err() {
        cleanup_orphans(&state, uploads.take(), "deploy").await;
    }
    lock.release().await;

    if let Ok(outcome) = &outcome {
        if matches!(outcome, DeployOutcome::Deployed { .. }) {
            progress.phase("activated", json!({})).await;
        }
        pipeline::after_release(&ctx, outcome).await?;
    }

    let (service, new_version, schema, deployment, eventing) = match outcome {
        Ok(DeployOutcome::Skipped { service, latest }) => {
            if let Some(ref auth_config) = manifest.authorization {
                seed_default_roles(&state, tenant_id, service.id, auth_config, &user.subject).await;
            }

            metrics::counter!(
                "cufflink_deployments_total",
                "result" => "skipped",
                "mode" => mode_label,
            )
            .increment(1);

            return Ok(json!({
                "service_id": service.id.to_string(),
                "name": manifest.name,
                "version": service.current_version,
                "manifest_hash": manifest_hash,
                "wasm_hash": latest.wasm_hash,
                "native_hash": native_hash,
                "schema_changes": 0,
                "tenant_slug": tenant_slug,
                "deployment_id": latest.id.to_string(),
                "status": service.status,
                "skipped": true,
                "message": "No changes detected, deploy skipped",
                "api_version": DEPLOY_API_VERSION,
            }));
        }
        Ok(DeployOutcome::Deployed {
            service,
            new_version,
            schema,
            deployment,
            eventing,
        }) => (service, new_version, schema, deployment, eventing),
        Ok(DeployOutcome::Planned { .. }) => {
            return Err(PlatformError::Internal(
                "a deploy only planned; nothing should have".into(),
            ))
        }
        Err(e) => {
            metrics::counter!(
                "cufflink_deployments_total",
                "result" => "failed",
                "mode" => mode_label,
            )
            .increment(1);
            return Err(e);
        }
    };

    if let Some(ref auth_config) = manifest.authorization {
        seed_default_roles(&state, tenant_id, service.id, auth_config, &user.subject).await;
    }

    info!(
        "Service '{}' deployed (v{}) — {} SQL migrations applied",
        manifest.name,
        new_version,
        schema.schema_changes.len()
    );

    let audit_client = state.platform_db.get().await?;
    let _ = crate::repo::audit::log_action(crate::repo::audit::AuditEntry {
        client: &audit_client,
        tenant_id: Some(tenant_id),
        actor: &user.subject,
        action: "deploy",
        resource_type: "service",
        resource_id: Some(&service.id.to_string()),
        details: Some(
            json!({ "version": new_version, "schema_changes": schema.schema_changes.len() }),
        ),
        ip_address: None,
    })
    .await;

    metrics::counter!(
        "cufflink_deployments_total",
        "result" => "success",
        "mode" => mode_label,
    )
    .increment(1);

    Ok(json!({
        "service_id": service.id.to_string(),
        "name": manifest.name,
        "version": new_version,
        "manifest_hash": manifest_hash,
        "wasm_hash": wasm_hash,
        "native_hash": native_hash,
        "schema_changes": schema.schema_changes.len(),
        "tenant_slug": tenant_slug,
        "deployment_id": deployment.id.to_string(),
        "commit_hash": commit_hash,
        "status": service.status,
        "api_version": DEPLOY_API_VERSION,
        "plan": pipeline::deploy_plan(&schema, &eventing)?,
    }))
}

pub(crate) fn require_platform_features(
    state: &AppState,
    manifest: &ServiceManifest,
    wasm: Option<&[u8]>,
) -> PlatformResult<()> {
    let declared = wasm.map(module_features).transpose()?.unwrap_or_default();
    validate_platform_requirements(
        manifest
            .required_platform_features
            .iter()
            .chain(&declared)
            .map(String::as_str)
            .chain(manifest.derived_features()),
        &crate::api::version::supports(state),
    )
}

pub(crate) fn decode_wasm_payload(b64: Option<&str>) -> PlatformResult<Option<Vec<u8>>> {
    use base64::Engine as _;
    b64.map(|s| {
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(|e| PlatformError::BadRequest(format!("Invalid wasm_base64: {}", e)))
    })
    .transpose()
}

async fn delete_version(state: &AppState, service_id: Uuid, version: i32) -> PlatformResult<bool> {
    let mut client = state.platform_db.get().await?;
    let tx = client.transaction().await?;
    let Some(deleted) =
        crate::repo::deployments::delete_by_version(&tx, service_id, version).await?
    else {
        return Ok(false);
    };
    match state.wasm_engine.as_ref() {
        Some(engine) => {
            engine.delete_precompiled(&tx, service_id, version).await?;
            tx.commit().await?;
            engine.delete_persisted(service_id, version).await;
        }
        None => {
            crate::repo::module_artifacts::delete_version(&tx, service_id, version).await?;
            tx.commit().await?;
        }
    }
    drop(client);
    if let Some(key) = deleted.native_key {
        native::remove_orphan(state, &key, "deployment delete").await;
    }
    Ok(true)
}

fn wasm_required_reason(manifest: &ServiceManifest) -> String {
    use cufflink_types::HandlerType;
    match manifest.mode {
        ServiceMode::Wasm => return "mode: wasm".into(),
        ServiceMode::Crud | ServiceMode::Container | ServiceMode::Web | ServiceMode::Native => {}
    }
    if manifest.on_migrate.is_some() {
        return "on_migrate hook declared".into();
    }
    if let Some(t) = manifest.tables.iter().find(|t| t.hooks.is_some()) {
        return format!("table '{}' declares CRUD lifecycle hooks", t.name);
    }
    if !manifest.custom_routes.is_empty() {
        return "custom_routes declared".into();
    }
    if !manifest.schedules.is_empty() {
        return "schedules declared".into();
    }
    if manifest
        .subscriptions
        .iter()
        .any(|s| matches!(s.handler_type, HandlerType::Wasm))
    {
        return "wasm subscription declared".into();
    }
    "user code declared".into()
}

#[derive(Deserialize)]
pub struct ListDeploymentsQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn list_deployments(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    axum::extract::Query(query): axum::extract::Query<ListDeploymentsQuery>,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let mut deployments =
        crate::repo::deployments::list_by_service(&state.platform_db, service_id).await?;
    let total = deployments.len();
    if let Some(limit) = query.limit {
        deployments.truncate(limit);
    }

    let results: Vec<Value> = deployments
        .iter()
        .map(|d| {
            json!({
                "id": d.id.to_string(),
                "version": d.version,
                "manifest_hash": d.manifest_hash,
                "manifest": d.manifest,
                "wasm_hash": d.wasm_hash,
                "schema_hash": d.schema_hash,
                "status": d.status,
                "deployed_at": d.deployed_at.to_string(),
                "commit_hash": d.commit_hash,
                "is_current": d.version == service.current_version,
            })
        })
        .collect();

    Ok(Json(json!({
        "deployments": results,
        "total": total,
        "current_version": service.current_version,
    })))
}

pub async fn delete_deployment(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path((id, version)): Path<(String, i32)>,
) -> PlatformResult<Json<Value>> {
    let removal = remove_deployment(state.clone(), user, id, version);
    detached(&state.shutdown, "deployment_delete", removal).await
}

async fn remove_deployment(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    id: String,
    version: i32,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    if version == service.current_version {
        return Err(PlatformError::Conflict(format!(
            "Cannot delete deployment v{}: it is the currently active version. \
             Rollback to a different version first, then delete.",
            version
        )));
    }

    let lock = acquire_deploy_lock(&state, service.id, OPERATION).await?;

    let result = async {
        if !delete_version(&state, service.id, version).await? {
            return Err(PlatformError::NotFound(format!(
                "Deployment v{} of '{}' not found",
                version, service.name
            )));
        }
        Ok::<_, PlatformError>(json!({
            "service_id": service.id.to_string(),
            "name": service.name,
            "deleted_version": version,
        }))
    }
    .await;
    lock.release().await;

    result.map(Json)
}

#[derive(Deserialize)]
pub struct PruneRequest {
    pub keep: usize,
}

pub async fn prune_deployments(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    Json(req): Json<PruneRequest>,
) -> PlatformResult<Json<Value>> {
    detached(
        &state.shutdown,
        "deployment_prune",
        prune(state.clone(), user, id, req),
    )
    .await
}

async fn prune(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    id: String,
    req: PruneRequest,
) -> PlatformResult<Json<Value>> {
    if req.keep == 0 {
        return Err(PlatformError::BadRequest(
            "`keep` must be >= 1 (cannot prune the currently-active version)".into(),
        ));
    }
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;
    let service =
        super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let lock = acquire_deploy_lock(&state, service.id, OPERATION).await?;

    let result = async {
        let deployments =
            crate::repo::deployments::list_by_service(&state.platform_db, service.id).await?;
        let versions: Vec<i32> = deployments.iter().map(|d| d.version).collect();
        let to_delete = prune_candidates(&versions, service.current_version, req.keep);

        let mut deleted_versions = Vec::with_capacity(to_delete.len());
        for version in &to_delete {
            if delete_version(&state, service.id, *version).await? {
                deleted_versions.push(*version);
            }
        }

        Ok::<_, PlatformError>(json!({
            "service_id": service.id.to_string(),
            "name": service.name,
            "kept": req.keep,
            "deleted_versions": deleted_versions,
            "deleted_count": deleted_versions.len(),
        }))
    }
    .await;
    lock.release().await;

    result.map(Json)
}

pub(crate) const OPERATION: &str = "deployment operation";

pub(crate) async fn acquire_deploy_lock(
    state: &AppState,
    service_id: Uuid,
    busy: &str,
) -> PlatformResult<DeployLock> {
    DeployLock::try_acquire(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| {
            PlatformError::Conflict(format!(
                "Another {busy} is in progress for this service. Try again later."
            ))
        })
}

fn prune_candidates(versions: &[i32], current_version: i32, keep: usize) -> Vec<i32> {
    let retain: std::collections::HashSet<i32> = versions
        .iter()
        .take(keep)
        .copied()
        .chain(std::iter::once(current_version))
        .collect();
    versions
        .iter()
        .copied()
        .filter(|v| !retain.contains(v))
        .collect()
}

#[derive(Deserialize)]
pub struct RollbackRequest {
    pub target_version: Option<i32>,
    #[serde(default)]
    pub allow_destructive: Option<bool>,
}

pub async fn rollback_service(
    State(state): State<Arc<AppState>>,
    DeployAuth(user): DeployAuth,
    Path(id): Path<String>,
    Json(req): Json<RollbackRequest>,
) -> PlatformResult<Json<Value>> {
    detached(
        &state.shutdown,
        "rollback",
        rollback(state.clone(), user, id, req),
    )
    .await
}

async fn rollback(
    state: Arc<AppState>,
    user: crate::auth::AuthUser,
    id: String,
    req: RollbackRequest,
) -> PlatformResult<Json<Value>> {
    let service_id: Uuid = id
        .parse()
        .map_err(|_| PlatformError::BadRequest("Invalid service ID".into()))?;

    super::get_service_for_tenant(&state.platform_db, service_id, user.tenant_id).await?;

    let lock = acquire_deploy_lock(&state, service_id, "deployment or rollback").await?;
    let uploads = UploadTracker::default();
    let rollback_result = pipeline::rollback_locked(
        &state,
        service_id,
        &user,
        req.target_version,
        req.allow_destructive.unwrap_or(false),
        &uploads,
    )
    .await;
    if rollback_result.is_err() {
        cleanup_orphans(&state, uploads.take(), "rollback").await;
    }
    lock.release().await;

    let outcome = rollback_result?;
    pipeline::after_rollback(&state, &outcome).await?;

    info!(
        "Service '{}' rolled back to v{} (now v{})",
        outcome.manifest.name, outcome.target_version, outcome.new_version
    );

    Ok(Json(json!({
        "service_id": service_id.to_string(),
        "name": outcome.manifest.name,
        "rolled_back_to": outcome.target_version,
        "new_version": outcome.new_version,
        "schema_changes": outcome.schema.schema_changes.len(),
        "deployment_id": outcome.deployment.id.to_string(),
        "plan": pipeline::deploy_plan(&outcome.schema, &outcome.eventing)?,
        "tenant_slug": user.tenant_slug,
        "wasm_restored": outcome.target_wasm_hash.is_some(),
        "native_hash": outcome.deployment.artifact.as_ref().map(|artifact| &artifact.hash),
        "rolled_back_from_commit": outcome.target_commit_hash,
    })))
}

async fn seed_default_roles(
    state: &Arc<AppState>,
    tenant_id: Uuid,
    service_id: Uuid,
    auth_config: &crate::deployer::manifest::AuthorizationConfig,
    deployer_subject: &str,
) {
    let client = match state.platform_db.get().await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("Failed to get DB client for role seeding: {}", e);
            return;
        }
    };

    let mut admin_role_id: Option<Uuid> = None;
    let mut roles_changed = false;

    for role_def in &auth_config.default_roles {
        let permissions_json = serde_json::to_value(&role_def.permissions).unwrap_or(json!([]));

        let seeded = client
            .query_opt(
                "INSERT INTO roles (tenant_id, service_id, name, description, permissions, is_default)
                 VALUES ($1, $2, $3, $4, $5, true)
                 ON CONFLICT (tenant_id, name) DO UPDATE
                 SET permissions = (
                     SELECT COALESCE(jsonb_agg(DISTINCT elem), '[]'::jsonb)
                     FROM jsonb_array_elements(roles.permissions || EXCLUDED.permissions) AS elem
                 ),
                     is_default = true
                 WHERE NOT roles.is_default OR NOT roles.permissions @> EXCLUDED.permissions
                 RETURNING id",
                &[
                    &tenant_id,
                    &service_id,
                    &role_def.name,
                    &role_def.description,
                    &permissions_json,
                ],
            )
            .await;
        let role_id = match seeded {
            Ok(Some(row)) => {
                roles_changed = true;
                Ok(row.get::<_, Uuid>(0))
            }
            Ok(None) => client
                .query_one(
                    "SELECT id FROM roles WHERE tenant_id = $1 AND name = $2",
                    &[&tenant_id, &role_def.name],
                )
                .await
                .map(|row| row.get::<_, Uuid>(0)),
            Err(e) => Err(e),
        };

        match role_id {
            Ok(role_id) => {
                info!(
                    "Seeded default role '{}' with {} permissions",
                    role_def.name,
                    role_def.permissions.len()
                );
                if role_def.name == "admin" {
                    admin_role_id = Some(role_id);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to seed role '{}': {}", role_def.name, e);
            }
        }
    }
    if roles_changed {
        crate::auth::permissions::invalidate_tenant(state, tenant_id).await;
    }

    if let Some(admin_id) = admin_role_id {
        if deployer_subject.starts_with("bootstrap-") {
            info!(
                "Skipping admin auto-assign: deployer '{}' is a bootstrap identity. \
                 Assign roles to real users via the RBAC API.",
                deployer_subject
            );
        } else {
            let result = client
                .execute(
                    "INSERT INTO user_roles (tenant_id, user_id, role_id)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (tenant_id, user_id, role_id) DO NOTHING",
                    &[&tenant_id, &deployer_subject, &admin_id],
                )
                .await;
            match result {
                Ok(rows) if rows > 0 => {
                    crate::auth::permissions::invalidate_user(state, tenant_id, deployer_subject)
                        .await;
                    info!(
                        "Auto-assigned deploying user '{}' to admin role",
                        deployer_subject
                    );
                }
                Ok(_) => {
                    info!(
                        "Deploying user '{}' already has admin role",
                        deployer_subject
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to assign admin role to deployer: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_native_service_is_what_this_platform_deploys() {
        use crate::shutdown::Shutdown;
        use crate::wasm_runtime::{WasmEngine, WasmLimits};
        let engine = WasmEngine::new(
            WasmLimits::default(),
            Shutdown::new(std::time::Duration::from_secs(1)),
        )
        .expect("engine");
        let state = AppState::minimal(engine).await;
        let manifest = ServiceManifest {
            name: "ext-google".into(),
            mode: ServiceMode::Native,
            provides: vec![cufflink_types::ProvidedExtension {
                name: "google.token".into(),
                callers: vec!["*".into()],
            }],
            ..Default::default()
        };
        validate_manifest(&manifest).expect("a valid native manifest");
        require_platform_features(&state, &manifest, None).expect("native services here");
    }

    #[test]
    fn a_deploy_leaves_a_service_the_status_it_has_and_creates_one_as_asked() {
        use ServiceMode::{Container, Crud, Native, Wasm, Web};
        let (active, inactive) = (Some(Status::Active), Some(Status::Inactive));
        for mode in [Crud, Wasm, Container, Native] {
            for (existing, asked, left) in [
                (None, true, Ok(Status::Inactive)),
                (None, false, Ok(Status::Active)),
                (inactive, true, Ok(Status::Inactive)),
                (inactive, false, Ok(Status::Inactive)),
                (active, false, Ok(Status::Active)),
                (
                    active,
                    true,
                    Err((
                        StatusCode::CONFLICT,
                        "Service 'notes' is active; a deploy never stops a running service. \
                         Disable it first, or deploy without --inactive",
                    )),
                ),
            ] {
                assert_eq!(
                    admitted_status(existing, &mode, asked),
                    left.map_err(|(status, text)| (status, text.to_string())),
                    "{mode:?} {existing:?} inactive={asked}"
                );
            }
        }
        for existing in [None, inactive, active] {
            assert_eq!(
                admitted_status(existing, &Web, true),
                Err((
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "a web app is started by its runner whatever its service's status; \
                     deploy it without --inactive"
                        .to_string()
                )),
                "{existing:?}"
            );
            assert_eq!(
                admitted_status(existing, &Web, false),
                Ok(existing.unwrap_or(Status::Active)),
                "a web deploy that does not ask is as it was: {existing:?}"
            );
        }
    }

    fn admitted_status(
        existing: Option<Status>,
        mode: &ServiceMode,
        inactive: bool,
    ) -> Result<Status, (StatusCode, String)> {
        admit_status("notes", existing, mode, inactive).map_err(|refused| match refused {
            PlatformError::Conflict(text) => (StatusCode::CONFLICT, text),
            PlatformError::Validation(text) => (StatusCode::UNPROCESSABLE_ENTITY, text),
            other => panic!("neither a 409 nor a 422: {other}"),
        })
    }

    #[test]
    fn the_inactive_flag_is_read_from_the_query_and_absent_by_default() {
        let query = |text: &str| {
            let uri = format!("/api/services/deploy?{text}").parse().expect("uri");
            axum::extract::Query::<DeployQuery>::try_from_uri(&uri)
                .map(|query| (query.plan, query.detach, query.inactive))
                .expect("a deploy query")
        };
        assert_eq!(query(""), (false, false, false));
        assert_eq!(query("inactive=true"), (false, false, true));
        assert_eq!(query("plan=true&inactive=true"), (true, false, true));
        assert_eq!(query("async=true&inactive=true"), (false, true, true));
        assert_eq!(query("async=true&unknown=1"), (false, true, false));
    }

    #[test]
    fn a_deploy_request_carries_a_native_binary_by_name() {
        let request: DeployRequest = serde_json::from_value(json!({
            "manifest": {"name": "ext-google", "mode": "native", "tables": []},
            "native_base64": "AAEC",
        }))
        .expect("a native deploy");
        assert_eq!(request.native_base64.as_deref(), Some("AAEC"));
        assert!(serde_json::from_value::<DeployRequest>(json!({
            "manifest": {"name": "ext-google", "mode": "native", "tables": []},
            "native_binary": "AAEC",
        }))
        .is_err());
    }

    #[test]
    fn sha256_hex_is_stable_and_64_hex_chars() {
        let h = sha256_hex(b"hello");
        assert_eq!(h.len(), 64);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(
            h,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn prune_candidates_empty_versions_returns_empty() {
        assert_eq!(prune_candidates(&[], 0, 5), Vec::<i32>::new());
    }

    #[test]
    fn prune_candidates_keep_equal_or_more_than_count_is_noop() {
        let versions = vec![5, 4, 3];
        assert_eq!(prune_candidates(&versions, 5, 3), Vec::<i32>::new());
        assert_eq!(prune_candidates(&versions, 5, 99), Vec::<i32>::new());
    }

    #[test]
    fn prune_candidates_returns_oldest_when_current_is_in_top_keep() {
        let versions = vec![10, 9, 8, 7, 6, 5];
        assert_eq!(prune_candidates(&versions, 10, 3), vec![7, 6, 5]);
    }

    #[test]
    fn prune_candidates_always_retains_current_version() {
        let versions = vec![10, 9, 8, 7, 6, 5];
        assert_eq!(prune_candidates(&versions, 5, 3), vec![7, 6]);
    }

    #[test]
    fn prune_candidates_order_preserved_and_deduped() {
        let versions = vec![10, 9, 8];
        assert_eq!(prune_candidates(&versions, 10, 1), vec![9, 8]);
    }

    #[test]
    fn decode_wasm_payload_none() {
        let r = decode_wasm_payload(None).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn decode_wasm_payload_valid() {
        use base64::Engine as _;
        let input = b"abc123";
        let b64 = base64::engine::general_purpose::STANDARD.encode(input);
        let r = decode_wasm_payload(Some(&b64)).unwrap();
        assert_eq!(r.as_deref(), Some(&input[..]));
    }

    #[test]
    fn decode_wasm_payload_invalid_returns_bad_request() {
        let r = decode_wasm_payload(Some("not!valid!base64!@@"));
        match r {
            Err(PlatformError::BadRequest(msg)) => {
                assert!(msg.contains("wasm_base64"));
            }
            other => panic!("expected BadRequest, got {:?}", other),
        }
    }

    #[test]
    fn sha256_hex_matches_cli_computation() {
        use base64::Engine as _;
        let raw = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 42, 255];
        let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);
        let decoded = decode_wasm_payload(Some(&b64)).unwrap().unwrap();
        assert_eq!(decoded, raw);
        assert_eq!(sha256_hex(&decoded), sha256_hex(&raw));
    }

    #[test]
    fn deploy_request_rejects_unknown_fields() {
        let bad = r#"{
            "manifest": {
                "name": "test",
                "version": "1",
                "tables": [],
                "events": [],
                "subscriptions": [],
                "custom_routes": [],
                "mode": "crud"
            },
            "future_field_the_platform_does_not_know_about": "oops"
        }"#;
        let r: Result<DeployRequest, _> = serde_json::from_str(bad);
        assert!(
            r.is_err(),
            "expected deny_unknown_fields to reject the payload; got {:?}",
            r.map(|_| "Ok").map_err(|e| e.to_string())
        );
    }
}
