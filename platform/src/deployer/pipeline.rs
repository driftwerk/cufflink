use super::eventing::{self, EventingPlan};
use super::module::{
    activate_module, announce, fail_after_schema, restore_wasm_binary, ModuleActivation,
    UploadTracker,
};
use super::native::NativeBinary;
use super::schema;
use crate::auth::AuthUser;
use crate::deployer::manifest::ServiceManifest;
use crate::errors::{PlatformError, PlatformResult};
use crate::extensions::Registration;
use crate::nats_manager::deploy_fanout::DeployReady;
use crate::registry::ServiceIdentity;
use crate::repo::deployments::{
    activate_version, snapshot, ActivateVersion, Deployment, NativeVersion,
};
use crate::AppState;
use cufflink_types::ServiceMode;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

pub(crate) fn deploy_hash(manifest: &ServiceManifest) -> PlatformResult<String> {
    manifest
        .deploy_hash()
        .map_err(|e| PlatformError::Internal(format!("Failed to hash manifest: {e}")))
}

pub(crate) fn deploy_plan(
    schema: &schema::SchemaReport,
    eventing: &EventingPlan,
) -> PlatformResult<Value> {
    let fields = |value: Value| match value {
        Value::Object(fields) => fields,
        _ => Map::new(),
    };
    let mut plan = fields(serde_json::to_value(schema)?);
    let mut events = fields(eventing.summary());
    let warnings: Vec<Value> = [plan.remove("warnings"), events.remove("warnings")]
        .into_iter()
        .flatten()
        .flat_map(|warnings| match warnings {
            Value::Array(warnings) => warnings,
            _ => Vec::new(),
        })
        .collect();
    plan.extend(events);
    plan.insert("warnings".into(), Value::Array(warnings));
    Ok(Value::Object(plan))
}

pub(crate) struct DeployContext<'a> {
    pub state: &'a Arc<AppState>,
    pub identity: ServiceIdentity,
    pub manifest: &'a ServiceManifest,
    pub manifest_hash: &'a str,
    pub wasm_bytes: Option<&'a [u8]>,
    pub wasm_hash: Option<&'a str>,
    pub native: Option<&'a NativeBinary>,
    pub commit_hash: Option<&'a str>,
    pub actor: &'a str,
    pub schema: schema::Options,
}

impl<'a> DeployContext<'a> {
    pub(crate) fn module(&self) -> Option<(&'a [u8], &'a str)> {
        self.wasm_bytes.zip(self.wasm_hash)
    }

    fn code_hash(&self) -> Option<&str> {
        code_hash(
            &self.manifest.mode,
            self.wasm_hash,
            self.native.map(|binary| binary.hash.as_str()),
        )
    }
}

pub(crate) enum DeployOutcome {
    Skipped {
        service: crate::repo::services::Service,
        latest: crate::repo::deployments::Deployment,
    },
    Planned {
        schema: Box<schema::SchemaReport>,
        eventing: EventingPlan,
    },
    Deployed {
        service: crate::repo::services::Service,
        new_version: i32,
        schema: Box<schema::SchemaReport>,
        deployment: crate::repo::deployments::Deployment,
        eventing: EventingPlan,
    },
}

pub(crate) async fn admit(
    state: &AppState,
    manifest: &ServiceManifest,
    tenant_id: Uuid,
    tenant_slug: &str,
) -> PlatformResult<()> {
    let registrations = admit_calls(state, manifest, tenant_id).await?;
    super::native::admit(state, manifest, tenant_id, &registrations).await?;
    eventing::admit(state, manifest, tenant_id, tenant_slug).await
}

pub(crate) async fn admit_calls(
    state: &AppState,
    manifest: &ServiceManifest,
    tenant_id: Uuid,
) -> PlatformResult<BTreeMap<String, Registration>> {
    let registrations =
        crate::repo::extensions::registrations(&state.platform_db.get().await?, tenant_id).await?;
    crate::extensions::require_admitted(manifest, &registrations)?;
    Ok(registrations)
}

fn code_hash<'a>(
    mode: &ServiceMode,
    wasm_hash: Option<&'a str>,
    native_hash: Option<&'a str>,
) -> Option<&'a str> {
    match mode {
        ServiceMode::Native => native_hash,
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web => {
            wasm_hash
        }
    }
}

fn should_skip_deploy(
    latest_manifest_hash: &str,
    new_manifest_hash: &str,
    latest_code_hash: Option<&str>,
    new_code_hash: Option<&str>,
) -> bool {
    latest_manifest_hash == new_manifest_hash && latest_code_hash == new_code_hash
}

pub(crate) async fn run_locked(
    ctx: &DeployContext<'_>,
    uploads: &UploadTracker,
) -> PlatformResult<DeployOutcome> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    let service = crate::repo::services::get_by_id(&state.platform_db, ctx.identity.service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service disappeared".into()))?;
    let new_version = service.current_version + 1;

    let deployments =
        crate::repo::deployments::list_by_service(&state.platform_db, service.id).await?;
    let running = crate::repo::deployments::running(&deployments, service.current_version);
    super::native::require_same_kind(
        &manifest.name,
        running.map(Deployment::mode).transpose()?.as_ref(),
        &manifest.mode,
    )?;
    if let Some(latest) = running {
        let latest_code_hash = code_hash(
            &manifest.mode,
            latest.wasm_hash.as_deref(),
            latest
                .artifact
                .as_ref()
                .map(|artifact| artifact.hash.as_str()),
        );
        if should_skip_deploy(
            &latest.manifest_hash,
            ctx.manifest_hash,
            latest_code_hash,
            ctx.code_hash(),
        ) {
            info!(
                "Service '{}' manifest and code unchanged (hash: {}), skipping deploy",
                manifest.name,
                &ctx.manifest_hash[..16]
            );
            if let Some(new_hash) = ctx.commit_hash {
                if latest.commit_hash.as_deref() != Some(new_hash) {
                    state
                        .platform_db
                        .get()
                        .await?
                        .execute(
                            "UPDATE deployments SET commit_hash = $1 WHERE id = $2",
                            &[&new_hash, &latest.id],
                        )
                        .await
                        .map_err(|e| {
                            PlatformError::Internal(format!(
                                "Failed to update commit_hash on skipped deploy: {}",
                                e
                            ))
                        })?;
                    announce(state, service.id, None).await;
                }
            }
            return Ok(DeployOutcome::Skipped {
                service,
                latest: latest.clone(),
            });
        }
    }

    match manifest.mode {
        ServiceMode::Web | ServiceMode::Native if ctx.schema.plan_only => {
            Ok(DeployOutcome::Planned {
                schema: Box::default(),
                eventing: EventingPlan::default(),
            })
        }
        ServiceMode::Web => activate_in_runner(ctx, service, new_version, None).await,
        ServiceMode::Native => {
            let binary = ctx.native.ok_or_else(super::native::missing)?;
            let artifact = super::native::store(state, service.id, binary, uploads).await?;
            fail_after_schema()?;
            let native = NativeVersion {
                artifact: &artifact,
                actor: ctx.actor,
            };
            activate_in_runner(ctx, service, new_version, Some(native)).await
        }
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container => {
            run_on_platform(ctx, service, new_version, uploads).await
        }
    }
}

async fn activate_in_runner(
    ctx: &DeployContext<'_>,
    service: crate::repo::services::Service,
    new_version: i32,
    native: Option<NativeVersion<'_>>,
) -> PlatformResult<DeployOutcome> {
    let manifest = ctx.manifest;
    let deployment = activate_version(
        &ctx.state.platform_db,
        ActivateVersion {
            service_id: service.id,
            version: new_version,
            manifest_hash: ctx.manifest_hash,
            manifest,
            wasm_hash: None,
            commit_hash: ctx.commit_hash,
            schema_json: None,
            schema_hash: &manifest.schema_hash()?,
            native,
        },
    )
    .await?;
    Ok(DeployOutcome::Deployed {
        service,
        new_version,
        schema: Box::default(),
        deployment,
        eventing: EventingPlan::default(),
    })
}

async fn run_on_platform(
    ctx: &DeployContext<'_>,
    service: crate::repo::services::Service,
    new_version: i32,
    uploads: &UploadTracker,
) -> PlatformResult<DeployOutcome> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    if !ctx.schema.plan_only {
        crate::db_roles::provision::before_deploy(
            state,
            ctx.identity.tenant_id,
            ctx.identity.service_id,
        )
        .await;
    }
    let eventing = eventing::prepare(state, manifest, &ctx.identity).await?;
    let left = snapshot(
        &state.platform_db,
        &manifest.name,
        service.id,
        service.current_version,
    )
    .await?;
    let schema = schema::apply(ctx, &service, &left, new_version, uploads).await?;
    if ctx.schema.plan_only {
        return Ok(DeployOutcome::Planned {
            schema: Box::new(schema),
            eventing,
        });
    }

    fail_after_schema()?;

    eventing::before_activation(state, &ctx.identity, &eventing).await?;
    let deployment = activate_module(
        state,
        ModuleActivation {
            service_id: service.id,
            version: new_version,
            manifest,
            manifest_hash: ctx.manifest_hash,
            wasm_hash: ctx.wasm_hash,
            commit_hash: ctx.commit_hash,
            adoption: schema.adoption(),
        },
    )
    .await?;
    Ok(DeployOutcome::Deployed {
        service,
        new_version,
        schema: Box::new(schema),
        deployment,
        eventing,
    })
}

pub(crate) async fn after_release(
    ctx: &DeployContext<'_>,
    outcome: &DeployOutcome,
) -> PlatformResult<()> {
    let (state, manifest) = (ctx.state, ctx.manifest);
    if manifest.mode.runs_on_platform() {
        crate::db_roles::provision::after_deploy(
            state,
            ctx.identity.tenant_id,
            ctx.identity.service_id,
        )
        .await;
    }
    match outcome {
        DeployOutcome::Planned { .. } => Ok(()),
        DeployOutcome::Skipped { .. } if manifest.subscriptions.is_empty() => Ok(()),
        DeployOutcome::Skipped { .. } => {
            eventing::after_activation(state, manifest, &ctx.identity, &EventingPlan::skipped())
                .await
        }
        DeployOutcome::Deployed {
            deployment,
            eventing,
            ..
        } => {
            announce(
                state,
                ctx.identity.service_id,
                DeployReady::of(deployment, &manifest.name),
            )
            .await;
            match manifest.mode {
                ServiceMode::Web => Ok(()),
                ServiceMode::Native => {
                    super::native::announce(state, &ctx.identity, deployment, manifest).await;
                    Ok(())
                }
                ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container => {
                    eventing::after_activation(state, manifest, &ctx.identity, eventing).await
                }
            }
        }
    }
}

pub(crate) struct RollbackOutcome {
    pub schema: schema::SchemaReport,
    pub deployment: Deployment,
    pub manifest: ServiceManifest,
    pub identity: ServiceIdentity,
    pub target_version: i32,
    pub new_version: i32,
    pub target_wasm_hash: Option<String>,
    pub target_commit_hash: Option<String>,
    pub eventing: EventingPlan,
}

fn rollback_manifest(state: &AppState, stored: &Value) -> PlatformResult<ServiceManifest> {
    let manifest: ServiceManifest = serde_json::from_value(stored.clone())
        .map_err(|e| PlatformError::Internal(format!("Failed to parse manifest: {}", e)))?;
    super::native::allowed(&state.config.extensions, &manifest.mode)?;
    crate::api::deployments::require_platform_features(state, &manifest, None)?;
    Ok(manifest)
}

pub(crate) async fn rollback_locked(
    state: &Arc<AppState>,
    service_id: Uuid,
    user: &AuthUser,
    target: Option<i32>,
    allow_destructive: bool,
    uploads: &UploadTracker,
) -> PlatformResult<RollbackOutcome> {
    let service = crate::repo::services::get_by_id(&state.platform_db, service_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;

    let deployments =
        crate::repo::deployments::list_by_service(&state.platform_db, service_id).await?;
    if deployments.len() < 2 && target.is_none() {
        return Err(PlatformError::BadRequest(
            "Cannot rollback: only one deployment exists".into(),
        ));
    }
    let target_version = target.unwrap_or(service.current_version - 1);
    let target_deploy = deployments
        .iter()
        .find(|d| d.version == target_version)
        .ok_or_else(|| {
            PlatformError::NotFound(format!("Deployment v{} not found", target_version))
        })?;
    let target_wasm_hash = target_deploy.wasm_hash.clone();
    let target_commit_hash = target_deploy.commit_hash.clone();
    let manifest = rollback_manifest(state, &target_deploy.manifest)?;
    admit(state, &manifest, service.tenant_id, &user.tenant_slug).await?;
    let identity = service.identity(&user.tenant_slug);
    let new_version = service.current_version + 1;

    let (schema, eventing, deployment) = match manifest.mode {
        ServiceMode::Native => {
            let artifact = super::native::restore(state, target_deploy).await?;
            let deployment = activate_version(
                &state.platform_db,
                ActivateVersion {
                    service_id,
                    version: new_version,
                    manifest_hash: &deploy_hash(&manifest)?,
                    manifest: &manifest,
                    wasm_hash: None,
                    commit_hash: target_commit_hash.as_deref(),
                    schema_json: None,
                    schema_hash: &manifest.schema_hash()?,
                    native: Some(NativeVersion {
                        artifact: &artifact,
                        actor: &user.subject,
                    }),
                },
            )
            .await?;
            (
                schema::SchemaReport::default(),
                EventingPlan::default(),
                deployment,
            )
        }
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web => {
            let eventing = eventing::prepare(state, &manifest, &identity).await?;
            let left = snapshot(
                &state.platform_db,
                &manifest.name,
                service_id,
                service.current_version,
            )
            .await?;
            let schema = schema::apply_rollback(
                state,
                &service,
                &user.tenant_slug,
                &manifest,
                &left,
                allow_destructive,
            )
            .await?;

            if let Some(expected) = target_wasm_hash.as_deref() {
                uploads.mark(service_id, new_version);
                restore_wasm_binary(state, service_id, target_version, new_version, expected)
                    .await?;
            }

            eventing::before_activation(state, &identity, &eventing).await?;
            let deployment = activate_version(
                &state.platform_db,
                ActivateVersion {
                    service_id,
                    version: new_version,
                    manifest_hash: &deploy_hash(&manifest)?,
                    manifest: &manifest,
                    wasm_hash: target_wasm_hash.as_deref(),
                    commit_hash: target_commit_hash.as_deref(),
                    schema_json: Some(
                        &schema
                            .adoption()
                            .snapshot(
                                &state.platform_db,
                                &manifest.name,
                                service_id,
                                &schema::declared(&manifest),
                            )
                            .await?,
                    ),
                    schema_hash: &manifest.schema_hash()?,
                    native: None,
                },
            )
            .await?;
            (schema, eventing, deployment)
        }
    };

    Ok(RollbackOutcome {
        schema,
        deployment,
        manifest,
        identity,
        target_version,
        new_version,
        target_wasm_hash,
        target_commit_hash,
        eventing,
    })
}

pub(crate) async fn after_rollback(
    state: &Arc<AppState>,
    outcome: &RollbackOutcome,
) -> PlatformResult<()> {
    announce(state, outcome.identity.service_id, None).await;
    match outcome.manifest.mode {
        ServiceMode::Native => {
            super::native::announce(
                state,
                &outcome.identity,
                &outcome.deployment,
                &outcome.manifest,
            )
            .await;
            Ok(())
        }
        ServiceMode::Crud | ServiceMode::Wasm | ServiceMode::Container | ServiceMode::Web => {
            eventing::after_rollback(
                state,
                &outcome.manifest,
                &outcome.identity,
                &outcome.eventing,
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_rollback_carries_the_commit_of_its_target() {
        use crate::registry::replica::{module, replica};
        let Some(replica) = replica().await else {
            return;
        };
        let manifest: ServiceManifest = serde_json::from_value(cufflink_testkit::wasm_manifest(
            "notes",
            &[("echo", "/echo")],
        ))
        .expect("manifest");
        let commits = [Some("1111111aaaa"), Some("2222222bbbb"), None];
        let mut deployed = Vec::new();
        for (variant, commit) in (1u8..).zip(commits) {
            let answer = replica
                .deploy_at(&manifest, &module(&manifest, variant), commit)
                .await
                .expect("deployed");
            deployed.push(answer);
        }
        let service_id = deployed[0]["service_id"].as_str().expect("id");
        let version = |deploy: usize| deployed[deploy]["version"].as_i64().expect("version");
        let health = || async {
            let path = format!("/svc/{}/notes/_meta/health", replica.tenant.slug);
            let (status, health) = replica.get(&path).await;
            assert_eq!(status, 200, "{health}");
            (health["version"].clone(), health["commit_hash"].clone())
        };
        assert_eq!(health().await, (version(2).into(), Value::Null));

        let rolled_back = replica
            .rollback(service_id, version(0))
            .await
            .expect("rolled back");
        assert_eq!(rolled_back["new_version"], version(2) + 1, "{rolled_back}");
        assert_eq!(rolled_back["rolled_back_from_commit"], "1111111aaaa");
        assert_eq!(
            health().await,
            ((version(2) + 1).into(), "1111111aaaa".into()),
            "the version a rollback activates reports the commit whose code it runs"
        );
        assert_eq!(
            replica
                .routed("notes")
                .await
                .expect("registered")
                .commit_hash
                .as_deref(),
            Some("1111111aaaa")
        );

        let rolled_back = replica
            .rollback(service_id, version(2))
            .await
            .expect("rolled back");
        assert_eq!(rolled_back["new_version"], version(2) + 2, "{rolled_back}");
        assert_eq!(
            health().await,
            ((version(2) + 2).into(), Value::Null),
            "a target that was deployed at no commit gives none"
        );
    }

    #[tokio::test]
    async fn a_rollback_to_a_manifest_this_platform_cannot_run_is_refused() {
        use crate::shutdown::Shutdown;
        use crate::wasm_runtime::{WasmEngine, WasmLimits};
        let engine = WasmEngine::new(
            WasmLimits::default(),
            Shutdown::new(std::time::Duration::from_secs(1)),
        )
        .expect("engine");
        let state = AppState::minimal(engine).await;
        let stored = |manifest: &ServiceManifest| serde_json::to_value(manifest).expect("stored");

        for accepted in crate::deployer::validator::fixtures::accepted_by_v0_27_0() {
            let manifest = rollback_manifest(&state, &stored(&accepted))
                .unwrap_or_else(|e| panic!("{}: {e}", accepted.name));
            assert_eq!(manifest.name, accepted.name);
        }
        let private: ServiceManifest = serde_json::from_value(serde_json::json!({
            "name": "loan-service",
            "schema": "adopted",
            "tables": [],
            "private_tables": [{"name": "notes", "columns": [{"name": "body", "column_type": "text"}]}],
            "required_platform_features": ["private_tables", "schema_adopted"]
        }))
        .expect("manifest");
        assert_eq!(
            rollback_manifest(&state, &stored(&private))
                .expect("what a deploy here accepts")
                .private_tables
                .len(),
            1
        );

        let later = ServiceManifest {
            required_platform_features: vec!["time_travel".into()],
            ..private
        };
        let refused = rollback_manifest(&state, &stored(&later)).expect_err("a later feature");
        assert!(matches!(refused, PlatformError::Validation(_)), "{refused}");
        assert!(
            refused
                .to_string()
                .contains("requires platform feature 'time_travel'"),
            "{refused}"
        );
    }

    #[test]
    fn should_skip_deploy_when_manifest_and_code_both_unchanged() {
        assert!(should_skip_deploy("m1", "m1", Some("w1"), Some("w1")));
        assert!(should_skip_deploy("m1", "m1", None, None));
    }

    #[test]
    fn should_skip_deploy_lets_through_code_only_change() {
        assert!(!should_skip_deploy("m1", "m1", Some("w1"), Some("w2")));
    }

    #[test]
    fn should_skip_deploy_lets_through_manifest_only_change() {
        assert!(!should_skip_deploy("m1", "m2", Some("w1"), Some("w1")));
    }

    #[test]
    fn should_skip_deploy_lets_through_both_changed() {
        assert!(!should_skip_deploy("m1", "m2", Some("w1"), Some("w2")));
    }

    #[test]
    fn should_skip_deploy_fails_safe_on_asymmetric_wasm_presence() {
        assert!(!should_skip_deploy("m1", "m1", None, Some("w1")));
        assert!(!should_skip_deploy("m1", "m1", Some("w1"), None));
    }

    fn skipped(
        mode: &ServiceMode,
        running: (&str, Option<&str>, Option<&str>),
        new: (&str, Option<&str>, Option<&str>),
    ) -> bool {
        should_skip_deploy(
            running.0,
            new.0,
            code_hash(mode, running.1, running.2),
            code_hash(mode, new.1, new.2),
        )
    }

    #[test]
    fn a_deploy_is_skipped_only_when_its_manifest_and_its_own_code_are_unchanged() {
        let (wasm, native, web) = (ServiceMode::Wasm, ServiceMode::Native, ServiceMode::Web);
        for (mode, running, new, skip) in [
            (&wasm, ("m", Some("w"), None), ("m", Some("w"), None), true),
            (
                &wasm,
                ("m", Some("w"), None),
                ("m2", Some("w"), None),
                false,
            ),
            (
                &wasm,
                ("m", Some("w"), None),
                ("m", Some("w2"), None),
                false,
            ),
            (
                &native,
                ("m", None, Some("n")),
                ("m", None, Some("n")),
                true,
            ),
            (
                &native,
                ("m", None, Some("n")),
                ("m2", None, Some("n")),
                false,
            ),
            (
                &native,
                ("m", None, Some("n")),
                ("m", None, Some("n2")),
                false,
            ),
            (&native, ("m", None, None), ("m", None, Some("n")), false),
            (&web, ("m", None, Some("bundle")), ("m", None, None), true),
            (&web, ("m", None, Some("bundle")), ("m2", None, None), false),
        ] {
            assert_eq!(
                skipped(mode, running, new),
                skip,
                "{mode:?} {running:?} -> {new:?}"
            );
        }
    }
}
