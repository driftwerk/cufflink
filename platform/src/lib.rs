pub mod api;
pub mod app;
pub mod audit;
pub mod auth;
pub mod config;
#[cfg(test)]
mod contract_tests;
pub mod db_error;
pub mod db_roles;
pub mod deployer;
pub mod egress;
pub mod errors;
pub mod extensions;
pub mod middleware;
pub mod nats_conn;
pub mod nats_manager;
pub mod outbox;
pub mod policy;
pub mod readiness;
pub mod redis_conn;
pub mod registry;
pub mod repo;
pub mod router;
pub mod schedules;
pub mod schema_manager;
pub mod secrets;
pub mod storage;
pub mod telemetry;
pub mod tenant_catalog;
pub mod tenant_db;
pub mod tenant_sessions;
pub mod wasm_runtime;
pub mod webhook_dispatcher;
pub mod webhooks;
pub mod worker_dispatch;

pub use cufflink_shutdown as shutdown;

use config::PlatformConfig;

pub struct AppState {
    pub config: PlatformConfig,
    pub platform_db: deadpool_postgres::Pool,
    pub tenant_sessions: tenant_sessions::TenantSessions,
    pub nats: async_nats::Client,
    pub jetstream: async_nats::jetstream::Context,
    pub registry: registry::ServiceRegistry,
    pub tenants: tenant_catalog::TenantCatalog,
    pub wasm_engine: Option<wasm_runtime::WasmEngine>,
    pub metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    pub storage: Option<storage::S3Storage>,
    pub keycloak_auth: Option<auth::KeycloakAuth>,
    pub tenant_keycloak_auths: auth::KeycloakAuthRegistry,
    pub redis: redis_conn::RedisHandle,
    pub subscription_tasks: nats_manager::subscriptions::SubscriptionTasks,
    pub reconciler: nats_manager::reconciler::ConsumerReconciler,
    pub stream_index: nats_manager::stream_index::StreamIndex,
    pub shutdown: shutdown::Shutdown,
    pub readiness: std::sync::Arc<readiness::Readiness>,
    pub cache_signer: auth::permissions::CacheSigner,
    pub sse_limiter: router::sse::SseLimiter,
    pub egress: egress::EgressGuard,
    pub service_identity: std::sync::Arc<auth::service_identity::ServiceIdentityKeys>,
    pub svc_service: wasm_runtime::host::LocalDispatch,
    pub partner_keys: auth::partner_keys::PartnerKeyCache,
    pub adopted: schema_manager::adopt::AdoptedSchemas,
    pub secrets: secrets::SecretBox,
    pub webhooks: webhooks::Webhooks,
    pub outbox_seal: Option<outbox::OutboxSeal>,
    pub audit_reads: audit::ReadAudit,
}

pub struct Assembly {
    pub config: PlatformConfig,
    pub platform_db: deadpool_postgres::Pool,
    pub tenant_pools: tenant_db::TenantPoolManager,
    pub nats: async_nats::Client,
    pub wasm_engine: Option<wasm_runtime::WasmEngine>,
    pub metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    pub storage: Option<storage::S3Storage>,
    pub keycloak_auth: Option<auth::KeycloakAuth>,
    pub redis: redis_conn::RedisHandle,
    pub shutdown: shutdown::Shutdown,
    pub service_identity: auth::service_identity::ServiceIdentityKeys,
}

impl AppState {
    pub fn new(parts: Assembly) -> Self {
        let Assembly {
            config,
            platform_db,
            tenant_pools,
            nats,
            wasm_engine,
            metrics_handle,
            storage,
            keycloak_auth,
            redis,
            shutdown,
            service_identity,
        } = parts;
        let tenants = tenant_catalog::TenantCatalog::new(
            platform_db.clone(),
            nats.clone(),
            config.policy_defaults.clone(),
        );
        let secrets = secrets::SecretBox::new(
            platform_db.clone(),
            config.secret_keys.clone(),
            config.secret_storage,
        );
        Self {
            tenant_sessions: tenant_sessions::TenantSessions::new(tenant_pools, tenants.clone())
                .with_roles(
                    platform_db.clone(),
                    secrets.clone(),
                    config.max_tenant_pools,
                ),
            jetstream: async_nats::jetstream::new(nats.clone()),
            registry: registry::ServiceRegistry::new(platform_db.clone(), nats.clone()),
            tenants,
            tenant_keycloak_auths: auth::KeycloakAuthRegistry::new(config.jwks_max_stale),
            cache_signer: auth::permissions::CacheSigner::from_config(
                config.cache_signing_key.as_deref(),
            ),
            outbox_seal: config.outbox_seal(),
            audit_reads: audit::ReadAudit::new(&config.audit),
            webhooks: webhooks::Webhooks::new(&config.webhook_delivery),
            sse_limiter: router::sse::SseLimiter::new(config.sse_max_streams_per_principal),
            secrets,
            egress: egress::EgressGuard::new(
                config.egress_max_response_bytes,
                wasm_runtime::host::outbound_connect_timeout(),
            ),
            config,
            platform_db,
            nats,
            wasm_engine,
            metrics_handle,
            storage,
            keycloak_auth,
            redis,
            shutdown,
            service_identity: std::sync::Arc::new(service_identity),
            subscription_tasks: Default::default(),
            reconciler: Default::default(),
            stream_index: Default::default(),
            readiness: Default::default(),
            svc_service: Default::default(),
            partner_keys: Default::default(),
            adopted: Default::default(),
        }
    }
}

#[cfg(test)]
impl AppState {
    pub(crate) async fn minimal(wasm_engine: wasm_runtime::WasmEngine) -> std::sync::Arc<Self> {
        Self::minimal_with_policies(wasm_engine, &[]).await
    }

    pub(crate) async fn minimal_with_policies(
        wasm_engine: wasm_runtime::WasmEngine,
        rows: &[repo::tenant_policies::PolicyRow],
    ) -> std::sync::Arc<Self> {
        use std::time::Duration;

        let database_url = "postgres://cufflink:cufflink@127.0.0.1:1/cufflink";
        let nats_url = "nats://127.0.0.1:1";
        let config = PlatformConfig {
            wasm: wasm_engine.limits().clone(),
            ..PlatformConfig::for_tests(database_url, nats_url)
        };
        let settings = cufflink_db::PoolSettings::from_vars(1, |_| None).expect("pool settings");
        let state = Self {
            egress: egress::EgressGuard::new(1024 * 1024, Duration::from_secs(5)),
            ..Self::new(Assembly {
                platform_db: cufflink_db::build_pool_with(
                    cufflink_db::parse_config(database_url).expect("test url"),
                    settings,
                )
                .expect("test pool"),
                tenant_pools: tenant_db::TenantPoolManager::new(database_url, 1, 1)
                    .expect("tenant pools"),
                nats: nats_conn::connect(nats_url, "cufflink-test", &config.nats_auth)
                    .await
                    .expect("nats client"),
                config,
                wasm_engine: Some(wasm_engine),
                metrics_handle: cufflink_telemetry::metrics_builder()
                    .expect("metrics builder")
                    .build_recorder()
                    .handle(),
                storage: Some(storage::S3Storage::from_inner(cufflink_storage::in_memory(
                    "cufflink",
                ))),
                keycloak_auth: None,
                redis: redis_conn::RedisHandle::disabled(),
                shutdown: shutdown::Shutdown::new(Duration::from_secs(1)),
                service_identity: auth::service_identity::ServiceIdentityKeys::ephemeral(),
            })
        };
        state.tenants.seed(
            repo::tenants::Tenant {
                id: uuid::Uuid::nil(),
                ..tenant_catalog::tests::tenant("acme", None)
            },
            rows,
        );
        std::sync::Arc::new(state)
    }
}

pub use cufflink_pgcodec::{numeric as pg_numeric, row_json};
