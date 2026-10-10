use crate::api::deployments::{deploy_service, rollback_service, DeployQuery, RollbackRequest};
use crate::api::services::set_status;
use crate::api::wasm::upload_wasm;
use crate::auth::extractor::{AuthMethod, AuthUser, DeployAuth};
use crate::config::PlatformConfig;
use crate::errors::{PlatformError, PlatformResult};
use crate::registry::{RegisteredService, ServiceRegistry};
use crate::repo::services::Status;
use crate::shutdown::Shutdown;
use crate::storage::S3Storage;
use crate::tenant_db::TenantPoolManager;
use crate::wasm_runtime::{sha256_hex, WasmEngine};
use crate::{AppState, Assembly};
use axum::extract::{Path, Query, State};
use axum::Json;
use base64::Engine;
use cufflink_db::PoolSettings;
use cufflink_types::ServiceManifest;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub(crate) struct Replica {
    pub(crate) state: Arc<AppState>,
    pub(crate) registry_db: deadpool_postgres::Pool,
    pub(crate) tenant: cufflink_testkit::Tenant,
    _db: cufflink_testkit::TestDb,
}

pub(crate) async fn replica() -> Option<Replica> {
    let infra = cufflink_testkit::infra().await?;
    let nats_url = infra.require_nats()?;
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let config = PlatformConfig::for_tests(&db.url, nats_url);
    let settings = PoolSettings::from_vars(4, |_| None).expect("pool settings");
    let pool = |settings: PoolSettings| {
        cufflink_db::build_pool_with(cufflink_db::parse_config(&db.url).expect("url"), settings)
            .expect("pool")
    };
    let platform_db = pool(settings.clone());
    let registry_db = pool(PoolSettings {
        max_size: 1,
        wait_timeout: Duration::from_millis(200),
        ..settings
    });
    let shutdown = Shutdown::new(Duration::from_secs(1));
    let wasm_engine = WasmEngine::new(config.wasm.clone(), shutdown.clone()).expect("engine");
    let storage = S3Storage::from_inner(cufflink_storage::in_memory("cufflink"));
    wasm_engine.set_storage(storage.clone()).await;
    wasm_engine.set_artifact_index(platform_db.clone());
    let nats = crate::nats_conn::connect(nats_url, "cufflink-test", &config.nats_auth)
        .await
        .expect("nats");
    let state = AppState {
        registry: ServiceRegistry::new(registry_db.clone(), nats.clone()),
        ..AppState::new(Assembly {
            tenant_pools: TenantPoolManager::new(
                config.tenant_database_url(),
                config.max_tenant_pools,
                config.max_conns_per_tenant,
            )
            .expect("tenant pools")
            .with_direct_url(&config.database_url)
            .expect("direct url"),
            platform_db,
            nats,
            config,
            wasm_engine: Some(wasm_engine),
            metrics_handle: cufflink_telemetry::metrics_builder()
                .expect("metrics builder")
                .build_recorder()
                .handle(),
            storage: Some(storage),
            keycloak_auth: None,
            redis: crate::redis_conn::RedisHandle::disabled(),
            shutdown,
            service_identity: crate::auth::service_identity::ServiceIdentityKeys::ephemeral(),
        })
    };
    state.tenants.reload().await.expect("tenant catalog");
    Some(Replica {
        state: Arc::new(state),
        registry_db,
        tenant,
        _db: db,
    })
}

pub(crate) fn module(manifest: &ServiceManifest, variant: u8) -> Vec<u8> {
    let handlers: String = manifest
        .declared_wasm_handlers()
        .iter()
        .map(|handler| {
            format!(
                r#"(func (export "{handler}") (param i32 i32) (result i32) i32.const {variant})"#
            )
        })
        .collect();
    wat::parse_str(format!(
        r#"(module
             (memory (export "memory") 1)
             (func (export "alloc") (param i32) (result i32) i32.const 0)
             {handlers})"#
    ))
    .expect("module")
}

impl RegisteredService {
    pub(crate) fn projected(&self) -> Value {
        json!({
            "service_id": self.service_id,
            "tenant_id": self.tenant_id,
            "tenant_slug": self.tenant_slug,
            "name": self.name,
            "version": self.version,
            "wasm_hash": self.wasm_hash,
            "manifest": self.manifest,
        })
    }
}

impl Replica {
    pub(crate) fn operator(&self) -> DeployAuth {
        DeployAuth(AuthUser {
            subject: "operator".into(),
            name: None,
            email: None,
            tenant_id: self.tenant.id,
            tenant_slug: self.tenant.slug.clone(),
            auth_method: AuthMethod::ApiKey {
                id: Uuid::new_v4(),
                user_sub: None,
            },
            token_info: None,
            is_service_account: false,
            is_platform_admin: false,
            has_deploy_authority: true,
            realm: None,
        })
    }

    pub(crate) async fn deploy(
        &self,
        manifest: &ServiceManifest,
        wasm: &[u8],
    ) -> PlatformResult<Value> {
        self.deploy_at(manifest, wasm, None).await
    }

    pub(crate) async fn deploy_at(
        &self,
        manifest: &ServiceManifest,
        wasm: &[u8],
        commit: Option<&str>,
    ) -> PlatformResult<Value> {
        let request = serde_json::from_value(json!({
            "manifest": manifest,
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(wasm),
            "commit_hash": commit,
            "api_version": 1,
        }))
        .expect("request");
        deploy_service(
            State(self.state.clone()),
            self.operator(),
            Query(DeployQuery::default()),
            Json(request),
        )
        .await
        .map(|(status, Json(body))| {
            assert_eq!(status, axum::http::StatusCode::OK);
            body
        })
    }

    pub(crate) async fn rollback(
        &self,
        service_id: &str,
        target_version: i64,
    ) -> PlatformResult<Value> {
        let request: RollbackRequest =
            serde_json::from_value(json!({ "target_version": target_version })).expect("request");
        rollback_service(
            State(self.state.clone()),
            self.operator(),
            Path(service_id.to_string()),
            Json(request),
        )
        .await
        .map(|Json(body)| body)
    }

    pub(crate) async fn upload(&self, service_id: &str, wasm: &[u8]) -> PlatformResult<Value> {
        let request = serde_json::from_value(json!({
            "wasm_base64": base64::engine::general_purpose::STANDARD.encode(wasm),
        }))
        .expect("request");
        upload_wasm(
            State(self.state.clone()),
            self.operator(),
            Path(service_id.to_string()),
            Json(request),
        )
        .await
        .map(|Json(body)| body)
    }

    pub(crate) async fn switch(&self, service_id: &str, status: Status) -> PlatformResult<Value> {
        set_status(
            State(self.state.clone()),
            self.operator(),
            Path(service_id.to_string()),
            Json(json!({ "status": status })),
        )
        .await
        .map(|Json(body)| body)
    }

    pub(crate) async fn routed(&self, name: &str) -> Option<Arc<RegisteredService>> {
        self.state
            .registry
            .get_by_slug(&self.tenant.slug, name)
            .await
    }

    pub(crate) async fn get(&self, path: &str) -> (u16, Value) {
        use tower::ServiceExt;
        let request = axum::http::Request::get(path)
            .body(axum::body::Body::empty())
            .expect("request");
        let response = crate::app::router(self.state.clone())
            .oneshot(request)
            .await
            .expect("answered");
        let status = response.status().as_u16();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    pub(crate) async fn published(&self) -> u64 {
        self.state.nats.flush().await.expect("flushed");
        self.state
            .nats
            .statistics()
            .out_messages
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) async fn stored(&self, service_id: &str) -> (i32, Status) {
        let service = crate::repo::services::get_by_id(
            &self.state.platform_db,
            service_id.parse().expect("service id"),
        )
        .await
        .expect("lookup")
        .expect("service");
        (service.current_version, service.status)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn g9_a_failed_refresh_leaves_a_committed_deploy() {
    let recorder = cufflink_telemetry::metrics_builder()
        .expect("metrics builder")
        .build_recorder();
    let _recording = metrics::set_default_local_recorder(&recorder);
    let failed_refreshes = || {
        cufflink_testkit::infra::metric_value(
            &recorder.handle().render(),
            "cufflink_registry_reload_total",
            &[("result", "error")],
        )
        .unwrap_or_default()
    };
    let Some(replica) = replica().await else {
        return;
    };
    let manifest: ServiceManifest = serde_json::from_value(cufflink_testkit::wasm_manifest(
        "notes",
        &[("echo", "/echo")],
    ))
    .expect("manifest");
    let name = manifest.name.as_str();
    let modules = [1, 2].map(|variant| module(&manifest, variant));
    let deployed = replica
        .deploy(&manifest, &modules[0])
        .await
        .expect("deployed");
    let service_id = deployed["service_id"].as_str().expect("service id");
    let version = deployed["version"].as_i64().expect("version");
    assert_eq!(failed_refreshes(), 0.0);

    let held = replica
        .registry_db
        .get()
        .await
        .expect("the registry's connection");
    let redeployed = replica
        .deploy(&manifest, &modules[1])
        .await
        .expect("the deploy has committed and is answered so");
    assert_eq!(redeployed["version"], version + 1, "{redeployed}");
    assert_eq!(failed_refreshes(), 1.0);
    assert_eq!(
        replica.stored(service_id).await,
        ((version + 1) as i32, Status::Active)
    );
    assert_eq!(
        replica
            .routed(name)
            .await
            .map(|service| i64::from(service.version)),
        Some(version),
        "this replica runs the version it had, as the others do until they reload"
    );

    assert_eq!(
        replica
            .switch(service_id, Status::Inactive)
            .await
            .expect("a disable depends on no read"),
        json!({"status": "inactive", "name": name})
    );
    assert!(replica.routed(name).await.is_none());
    let (asked, published) = (
        replica.state.reconciler.trigger(),
        replica.published().await,
    );
    let refused = replica
        .switch(service_id, Status::Active)
        .await
        .expect_err("an enable that could not read the service back");
    assert!(
        matches!(&refused, PlatformError::Internal(m) if m.starts_with("Failed to reload registry: ")),
        "{refused}"
    );
    assert_eq!(failed_refreshes(), 2.0);
    assert_eq!(
        replica.published().await,
        published + 1,
        "the other replicas are told of the status that was written"
    );
    assert_eq!(
        replica.state.reconciler.trigger(),
        asked + 2,
        "and this one's reconciler is asked for a pass"
    );
    assert_eq!(
        replica.stored(service_id).await.1,
        Status::Active,
        "the status is written; the next reload routes the service"
    );
    assert!(replica.routed(name).await.is_none());

    drop(held);
    replica.state.registry.load_all().await.expect("reloaded");
    let routed = replica.routed(name).await.expect("routed");
    assert_eq!(
        (i64::from(routed.version), routed.wasm_hash.clone()),
        (version + 1, Some(sha256_hex(&modules[1])))
    );
}
