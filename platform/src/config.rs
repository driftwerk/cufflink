use crate::policy::PolicySet;
use crate::wasm_runtime::WasmLimits;
use cufflink_db::{env_secs, env_var, parse_var};
use eyre::{eyre, Result, WrapErr};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

const DEFAULT_DATABASE_URL: &str = "postgres://cufflink:cufflink@localhost:5432/cufflink";
const DEFAULT_DATABASE_PASSWORD: &[u8] = b"cufflink";

#[derive(Debug, Clone)]
pub struct PlatformConfig {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub nats_url: String,
    pub keycloak_url: String,
    pub keycloak_realm: String,
    pub keycloak_issuer: Option<String>,
    pub redis_url: Option<String>,
    pub loki_url: Option<String>,
    pub loki_label_model: crate::api::logs::LokiLabelModel,
    pub default_tenant_slug: String,
    pub max_tenant_pools: usize,
    pub max_conns_per_tenant: usize,
    pub worker_routes: HashMap<String, Vec<String>>,
    pub sql_max_rows: u64,
    pub sql_timeout_secs: u64,
    pub default_keycloak_ops_client_id: Option<String>,
    pub default_keycloak_ops_client_secret: Option<String>,
    pub service_account_clients: HashSet<String>,
    pub shutdown_grace: Duration,
    pub shutdown_unready_delay: Duration,
    pub startup_deadline: Option<Duration>,
    pub deploy_statement_timeout: Duration,
    pub sql_migrations: crate::schema_manager::migrate_exec::Timeouts,
    pub deploy_job_retention: Duration,
    pub wasm: WasmLimits,
    pub max_open_tx: usize,
    pub cache: crate::wasm_runtime::host::CacheLimits,
    pub platform_internal_url: Option<String>,
    pub tenant_database_url: Option<String>,
    pub tenant_db_pooler: cufflink_db::Pooler,
    pub run_env: RunEnv,
    pub registry_reload_interval: Duration,
    pub dlq_max_age: Duration,
    pub jwks_max_stale: Duration,
    pub policy_defaults: PolicySet,
    pub platform_admin_role: String,
    pub api_key_ttl: chrono::Duration,
    pub cache_signing_key: Option<Vec<u8>>,
    pub sse_max_streams_per_principal: usize,
    pub egress_max_response_bytes: u64,
    pub fetch: crate::wasm_runtime::host::FetchLimits,
    pub storage_max_upload_bytes: usize,
    pub sleep_max: Duration,
    pub nats_auth: crate::nats_conn::NatsAuth,
    pub client_ip_source: crate::middleware::rate_limit::ClientIpSource,
    pub rate_limit: crate::middleware::rate_limit::RateLimitSettings,
    pub http: crate::app::layers::HttpLimits,
    pub admin_api_key_ttl: Option<chrono::Duration>,
    pub eventing: crate::nats_manager::settings::EventingConfig,
    pub outbox: crate::outbox::relay::RelayConfig,
    pub audit: crate::audit::AuditConfig,
    pub webhook_delivery: crate::webhooks::DeliveryConfig,
    pub secret_keys: crate::secrets::KeyRing,
    pub secret_storage: crate::secrets::StorageMode,
    pub dev_bootstrap: Option<crate::app::dev_bootstrap::DevBootstrap>,
    pub service_identity_keys: Option<String>,
    pub extensions: crate::extensions::ExtensionsConfig,
}

const MIN_CACHE_SIGNING_KEY_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunEnv {
    Development,
    Staging,
    Production,
}

impl RunEnv {
    pub fn name(self) -> &'static str {
        match self {
            RunEnv::Development => "development",
            RunEnv::Staging => "staging",
            RunEnv::Production => "production",
        }
    }
}

impl FromStr for RunEnv {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "development" => Ok(RunEnv::Development),
            "staging" => Ok(RunEnv::Staging),
            "production" => Ok(RunEnv::Production),
            other => Err(format!(
                "'{other}' is not one of development, staging, production"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TenantPoolDefaults {
    pub max_pools: usize,
    pub max_conns_per_tenant: usize,
}

impl TenantPoolDefaults {
    pub const PLATFORM: Self = Self {
        max_pools: 64,
        max_conns_per_tenant: 4,
    };
}

impl PlatformConfig {
    pub fn load(vars: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let config = Self::load_with(TenantPoolDefaults::PLATFORM, vars)?;
        crate::outbox::seal::require_key(
            config
                .policy_defaults
                .mode(crate::policy::PolicyKey::Outbox),
            config.outbox_seal().as_ref(),
        )
        .map_err(|e| eyre!("CUFFLINK_POLICY_OUTBOX: {e}"))?;
        Ok(config)
    }

    pub fn outbox_seal(&self) -> Option<crate::outbox::OutboxSeal> {
        crate::outbox::OutboxSeal::of(&self.secret_keys, self.cache_signing_key.as_deref())
    }

    pub fn load_with(
        pool_defaults: TenantPoolDefaults,
        vars: impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let declared_env = std::env::var("RUN_ENV")
            .ok()
            .map(|raw| parse_var("RUN_ENV", Some(raw), RunEnv::Development))
            .transpose()?;
        let run_env = declared_env.unwrap_or(RunEnv::Development);
        let dev_bootstrap = crate::app::dev_bootstrap::DevBootstrap::from_setting(
            declared_env,
            std::env::var(crate::app::dev_bootstrap::VARIABLE)
                .ok()
                .as_deref(),
        )?;
        let database_url = vars("DATABASE_URL");
        reject_default_credentials(run_env, database_url.as_deref())?;
        let tenant_database_url = vars("TENANT_DATABASE_URL").filter(|url| !url.trim().is_empty());
        reject_default_tenant_credentials(run_env, tenant_database_url.as_deref())?;
        reject_ephemeral_storage(run_env, cufflink_storage::Backend::from_vars(&vars)?)?;

        let default_keycloak_ops_client_id =
            std::env::var("CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID").ok();

        let mut service_account_clients: HashSet<String> =
            std::env::var("CUFFLINK_SERVICE_ACCOUNT_CLIENTS")
                .ok()
                .map(|s| {
                    s.split(',')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect()
                })
                .unwrap_or_default();
        if let Some(id) = &default_keycloak_ops_client_id {
            service_account_clients.insert(id.clone());
        }
        let secret_keys = crate::secrets::KeyRing::from_vars(&vars)?;
        let rate_limit = crate::middleware::rate_limit::RateLimitSettings::from_env()?;

        Ok(Self {
            host: std::env::var("PLATFORM_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env_var("PLATFORM_PORT", 8080)?,
            database_url: database_url.unwrap_or_else(|| DEFAULT_DATABASE_URL.into()),
            nats_url: std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into()),
            keycloak_url: std::env::var("KEYCLOAK_URL")
                .unwrap_or_else(|_| "http://localhost:8180".into()),
            keycloak_realm: std::env::var("KEYCLOAK_REALM").unwrap_or_else(|_| "cufflink".into()),
            keycloak_issuer: std::env::var("KEYCLOAK_ISSUER").ok(),
            redis_url: vars("REDIS_URL"),
            loki_url: std::env::var("LOKI_URL").ok(),
            loki_label_model: env_var("LOKI_LABEL_MODEL", Default::default())?,
            default_tenant_slug: std::env::var("DEFAULT_TENANT_SLUG")
                .unwrap_or_else(|_| "default".into()),
            max_tenant_pools: positive("MAX_TENANT_POOLS", pool_defaults.max_pools)?,
            max_conns_per_tenant: positive(
                "MAX_CONNS_PER_TENANT",
                pool_defaults.max_conns_per_tenant,
            )?,
            worker_routes: parse_worker_routes(std::env::var("WORKER_ROUTES").ok().as_deref())?,
            sql_max_rows: env_var("PLATFORM_SQL_MAX_ROWS", 100_000)?,
            sql_timeout_secs: env_var("PLATFORM_SQL_TIMEOUT_SECS", 30)?,
            default_keycloak_ops_client_id,
            default_keycloak_ops_client_secret: vars("CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET"),
            service_account_clients,
            shutdown_grace: env_secs("SHUTDOWN_GRACE_SECS", 45)?,
            shutdown_unready_delay: env_secs("SHUTDOWN_UNREADY_DELAY_SECS", 0)?,
            startup_deadline: Some(env_secs("STARTUP_DEADLINE_SECS", 0)?)
                .filter(|deadline| !deadline.is_zero()),
            deploy_statement_timeout: env_var("DB_DEPLOY_STATEMENT_TIMEOUT_MS", 600_000)
                .map(Duration::from_millis)?,
            sql_migrations: crate::schema_manager::migrate_exec::timeouts_from_env()?,
            deploy_job_retention: days_var(
                "DEPLOY_JOB_RETENTION_DAYS",
                std::env::var("DEPLOY_JOB_RETENTION_DAYS").ok(),
                30,
            )?,
            wasm: WasmLimits::from_env()?,
            max_open_tx: positive("WASM_MAX_OPEN_TX", 2)?,
            cache: crate::wasm_runtime::host::CacheLimits::from_env()?,
            platform_internal_url: parse_internal_url(
                std::env::var("PLATFORM_INTERNAL_URL").ok().as_deref(),
            )?,
            tenant_database_url: tenant_database_url
                .map(|url| {
                    cufflink_db::parse_config(&url)
                        .map(|_| url)
                        .wrap_err("TENANT_DATABASE_URL is not a Postgres connection URL")
                })
                .transpose()?,
            tenant_db_pooler: env_var("TENANT_DB_POOLER", cufflink_db::Pooler::Direct)?,
            run_env,
            registry_reload_interval: env_secs("REGISTRY_RELOAD_SECS", 60)?,
            dlq_max_age: days_var(
                "CUFFLINK_DLQ_MAX_AGE_DAYS",
                std::env::var("CUFFLINK_DLQ_MAX_AGE_DAYS").ok(),
                14,
            )?,
            jwks_max_stale: env_secs(
                "KEYCLOAK_JWKS_MAX_STALE_SECS",
                crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT.as_secs(),
            )?,
            policy_defaults: PolicySet::platform_defaults_from_env().map_err(|e| eyre!(e))?,
            platform_admin_role: std::env::var("CUFFLINK_PLATFORM_ADMIN_ROLE")
                .unwrap_or_else(|_| "cufflink-platform-admin".into()),
            api_key_ttl: chrono::Duration::days(positive("CUFFLINK_API_KEY_TTL_DAYS", 90)? as i64),
            cache_signing_key: vars("CUFFLINK_CACHE_SIGNING_KEY")
                .map(|raw| parse_signing_key(&raw))
                .transpose()?,
            sse_max_streams_per_principal: positive("SSE_MAX_STREAMS_PER_PRINCIPAL", 20)?,
            egress_max_response_bytes: positive("HOST_HTTP_MAX_RESPONSE_BYTES", 10 * 1024 * 1024)?
                as u64,
            fetch: crate::wasm_runtime::host::FetchLimits::from_env()?,
            storage_max_upload_bytes: crate::wasm_runtime::host::max_upload_bytes_from_env()?,
            sleep_max: crate::wasm_runtime::host::sleep_max_from_env()?,
            nats_auth: crate::nats_conn::NatsAuth::from_vars(&vars)?,
            client_ip_source: rate_limit.ip_source,
            rate_limit,
            http: crate::app::layers::HttpLimits::from_env()?,
            admin_api_key_ttl: admin_api_key_ttl(
                std::env::var("CUFFLINK_ADMIN_API_KEY_TTL_DAYS").ok(),
            )?,
            eventing: crate::nats_manager::settings::EventingConfig::from_env()?,
            outbox: crate::outbox::relay::RelayConfig::from_env()?,
            audit: crate::audit::AuditConfig::from_env()?,
            webhook_delivery: crate::webhooks::DeliveryConfig::from_env()?,
            secret_storage: secret_keys.storage_mode_from_vars(&vars)?,
            secret_keys,
            dev_bootstrap,
            service_identity_keys: vars(crate::auth::service_identity::KEYS_ENV),
            extensions: crate::extensions::ExtensionsConfig::from_env()?,
        })
    }

    #[cfg(test)]
    pub(crate) fn for_tests(database_url: &str, nats_url: &str) -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 0,
            database_url: database_url.into(),
            nats_url: nats_url.into(),
            keycloak_url: String::new(),
            keycloak_realm: "cufflink".into(),
            keycloak_issuer: None,
            redis_url: None,
            loki_url: None,
            loki_label_model: Default::default(),
            default_tenant_slug: "default".into(),
            max_tenant_pools: 1,
            max_conns_per_tenant: 1,
            worker_routes: HashMap::new(),
            sql_max_rows: 100,
            sql_timeout_secs: 1,
            default_keycloak_ops_client_id: None,
            default_keycloak_ops_client_secret: None,
            service_account_clients: HashSet::new(),
            shutdown_grace: Duration::from_secs(1),
            shutdown_unready_delay: Duration::ZERO,
            startup_deadline: None,
            deploy_statement_timeout: Duration::from_secs(1),
            sql_migrations: crate::schema_manager::migrate_exec::Timeouts {
                lock_wait: Duration::from_secs(1),
                statement: Duration::from_secs(1),
                lock: Duration::from_secs(1),
            },
            deploy_job_retention: DAY,
            wasm: WasmLimits::default(),
            max_open_tx: 2,
            cache: crate::wasm_runtime::host::CacheLimits::default(),
            platform_internal_url: None,
            tenant_database_url: None,
            tenant_db_pooler: cufflink_db::Pooler::Direct,
            run_env: RunEnv::Development,
            registry_reload_interval: Duration::from_secs(60),
            dlq_max_age: DAY * 14,
            jwks_max_stale: crate::auth::keycloak::JWKS_MAX_STALE_DEFAULT,
            policy_defaults: PolicySet::builtin_defaults(),
            platform_admin_role: "cufflink-platform-admin".into(),
            api_key_ttl: chrono::Duration::days(90),
            cache_signing_key: None,
            sse_max_streams_per_principal: 20,
            egress_max_response_bytes: 10 * 1024 * 1024,
            fetch: crate::wasm_runtime::host::FetchLimits::default(),
            storage_max_upload_bytes: crate::wasm_runtime::host::DEFAULT_MAX_UPLOAD_BYTES,
            sleep_max: crate::wasm_runtime::host::DEFAULT_SLEEP_MAX,
            nats_auth: crate::nats_conn::NatsAuth::Anonymous,
            client_ip_source: crate::middleware::rate_limit::ClientIpSource::Peer,
            rate_limit: Default::default(),
            http: crate::app::layers::HttpLimits::legacy(),
            admin_api_key_ttl: Some(chrono::Duration::days(90)),
            eventing: Default::default(),
            outbox: Default::default(),
            audit: Default::default(),
            webhook_delivery: Default::default(),
            secret_keys: Default::default(),
            secret_storage: crate::secrets::StorageMode::Plaintext,
            dev_bootstrap: None,
            service_identity_keys: None,
            extensions: Default::default(),
        }
    }

    pub fn tenant_database_url(&self) -> &str {
        self.tenant_database_url
            .as_deref()
            .unwrap_or(&self.database_url)
    }

    pub fn deprecated_service_account_env(
        &self,
        rows: &[crate::repo::tenant_policies::PolicyRow],
    ) -> Vec<&'static str> {
        use crate::policy::{PolicyKey, PolicyMode};
        let key = PolicyKey::ServiceAccounts;
        let enforced = self.policy_defaults.mode(key) == PolicyMode::Enforce
            || rows
                .iter()
                .any(|row| row.key == key.as_str() && row.mode == PolicyMode::Enforce);
        let ops_client = self.default_keycloak_ops_client_id.as_deref();
        let listed = self
            .service_account_clients
            .iter()
            .any(|client| Some(client.as_str()) != ops_client);
        [
            ("CUFFLINK_SERVICE_ACCOUNT_CLIENTS", listed),
            ("CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID", ops_client.is_some()),
        ]
        .into_iter()
        .filter(|(_, set)| enforced && *set)
        .map(|(name, _)| name)
        .collect()
    }

    pub fn is_worker_routed(&self, service_name: &str, handler: &str) -> bool {
        self.worker_routes
            .get(service_name)
            .map(|handlers| handlers.iter().any(|h| h == handler))
            .unwrap_or(false)
    }
}

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn days_var(name: &str, raw: Option<String>, default: u32) -> Result<Duration> {
    cufflink_db::positive_var(name, raw, default).map(|days| DAY * days)
}

fn positive(name: &str, default: usize) -> Result<usize> {
    cufflink_db::positive_var(name, std::env::var(name).ok(), default)
}

fn admin_api_key_ttl(raw: Option<String>) -> Result<Option<chrono::Duration>> {
    let days: u32 = cufflink_db::parse_var("CUFFLINK_ADMIN_API_KEY_TTL_DAYS", raw, 90)?;
    Ok((days > 0).then(|| chrono::Duration::days(i64::from(days))))
}

fn parse_worker_routes(raw: Option<&str>) -> Result<HashMap<String, Vec<String>>> {
    raw.map_or(Ok(HashMap::new()), |json| {
        serde_json::from_str(json).wrap_err("WORKER_ROUTES is not valid JSON")
    })
}

fn parse_internal_url(raw: Option<&str>) -> Result<Option<String>> {
    raw.map(str::trim)
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            url::Url::parse(raw)
                .ok()
                .filter(|url| matches!(url.scheme(), "http" | "https") && url.host().is_some())
                .map(|_| raw.trim_end_matches('/').to_string())
                .ok_or_else(|| eyre!("PLATFORM_INTERNAL_URL {raw:?} is not an http(s) URL"))
        })
        .transpose()
}

fn parse_signing_key(raw: &str) -> Result<Vec<u8>> {
    let raw = raw.trim();
    let key = hex::decode(raw).unwrap_or_else(|_| raw.as_bytes().to_vec());
    if key.len() < MIN_CACHE_SIGNING_KEY_BYTES {
        return Err(eyre!(
            "CUFFLINK_CACHE_SIGNING_KEY must be at least {MIN_CACHE_SIGNING_KEY_BYTES} bytes"
        ));
    }
    Ok(key)
}

fn reject_default_credentials(run_env: RunEnv, database_url: Option<&str>) -> Result<()> {
    if run_env != RunEnv::Production {
        return Ok(());
    }
    let url =
        database_url.ok_or_else(|| eyre!("DATABASE_URL must be set when RUN_ENV=production"))?;
    reject_default_password("DATABASE_URL", url)
}

fn reject_default_tenant_credentials(run_env: RunEnv, tenant_url: Option<&str>) -> Result<()> {
    match (run_env, tenant_url) {
        (RunEnv::Production, Some(url)) => reject_default_password("TENANT_DATABASE_URL", url),
        _ => Ok(()),
    }
}

fn reject_default_password(name: &str, url: &str) -> Result<()> {
    let uses_default_password = url
        .parse::<tokio_postgres::Config>()
        .ok()
        .and_then(|config| {
            config
                .get_password()
                .map(|p| p == DEFAULT_DATABASE_PASSWORD)
        })
        .unwrap_or(false);
    if uses_default_password {
        return Err(eyre!(
            "{name} uses the default development password; set real credentials when RUN_ENV=production"
        ));
    }
    Ok(())
}

fn reject_ephemeral_storage(run_env: RunEnv, backend: cufflink_storage::Backend) -> Result<()> {
    if run_env == RunEnv::Production && backend.is_ephemeral() {
        return Err(eyre!(
            "STORAGE_BACKEND={} is for tests and development; use s3 or gcs when RUN_ENV=production",
            backend.name()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn a_retention_is_a_positive_number_of_days() {
        let days =
            |raw: Option<&str>| days_var("CUFFLINK_DLQ_MAX_AGE_DAYS", raw.map(String::from), 14);
        assert_eq!(days(None).ok(), Some(DAY * 14));
        assert_eq!(days(Some("30")).ok(), Some(DAY * 30));
        for refused in ["0", "-1", "soon"] {
            let error = days(Some(refused)).expect_err(refused).to_string();
            assert!(error.contains("CUFFLINK_DLQ_MAX_AGE_DAYS"), "{error}");
        }
    }

    #[test]
    fn service_account_env_is_deprecated_once_a_tenant_enforces() {
        use crate::policy::{PolicyKey, PolicyMode};
        let row = |mode| crate::repo::tenant_policies::PolicyRow {
            tenant_id: uuid::Uuid::nil(),
            key: PolicyKey::ServiceAccounts.as_str().to_string(),
            mode,
            config: serde_json::json!({}),
            updated_at: chrono::Utc::now(),
            updated_by: None,
        };
        let mut config = PlatformConfig::for_tests("postgres://x", "nats://x");
        assert!(config
            .deprecated_service_account_env(&[row(PolicyMode::Enforce)])
            .is_empty());
        config.default_keycloak_ops_client_id = Some("orchard-ops".into());
        config.service_account_clients = HashSet::from(["orchard-ops".to_string()]);
        assert!(config
            .deprecated_service_account_env(&[row(PolicyMode::Warn)])
            .is_empty());
        assert_eq!(
            config.deprecated_service_account_env(&[row(PolicyMode::Enforce)]),
            ["CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID"]
        );
        config.service_account_clients.insert("ci-bot".into());
        assert_eq!(
            config
                .deprecated_service_account_env(&[row(PolicyMode::Warn), row(PolicyMode::Enforce)]),
            [
                "CUFFLINK_SERVICE_ACCOUNT_CLIENTS",
                "CUFFLINK_DEFAULT_KC_OPS_CLIENT_ID"
            ]
        );
    }

    #[test]
    fn admin_keys_expire_after_ninety_days_unless_configured() {
        assert_eq!(
            admin_api_key_ttl(None).expect("default"),
            Some(chrono::Duration::days(90))
        );
        assert_eq!(
            admin_api_key_ttl(Some("30".into())).expect("set"),
            Some(chrono::Duration::days(30))
        );
        assert_eq!(admin_api_key_ttl(Some("0".into())).expect("disabled"), None);
        assert!(admin_api_key_ttl(Some("-1".into())).is_err());
        assert!(admin_api_key_ttl(Some("soon".into())).is_err());
    }

    use cufflink_settings::process_var as env;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env(vars: &[(&str, Option<&str>)], f: impl FnOnce()) {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let saved: Vec<(&str, Option<String>)> = vars
            .iter()
            .map(|(k, _)| (*k, std::env::var(k).ok()))
            .collect();
        for (k, v) in vars {
            match v {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
        f();
        for (k, prev) in saved {
            match prev {
                Some(v) => unsafe { std::env::set_var(k, v) },
                None => unsafe { std::env::remove_var(k) },
            }
        }
    }

    #[test]
    fn env_parse_keeps_the_default_when_unset_and_names_bad_values() {
        with_env(
            &[
                ("CUFFLINK_TEST_UNSET", None),
                ("CUFFLINK_TEST_GOOD", Some(" 12 ")),
                ("CUFFLINK_TEST_BAD", Some("dozen")),
            ],
            || {
                assert_eq!(env_var::<usize>("CUFFLINK_TEST_UNSET", 7).unwrap(), 7);
                assert_eq!(env_var::<usize>("CUFFLINK_TEST_GOOD", 7).unwrap(), 12);
                let err = env_var::<usize>("CUFFLINK_TEST_BAD", 7).unwrap_err();
                let msg = err.to_string();
                assert!(msg.contains("CUFFLINK_TEST_BAD"), "{msg}");
                assert!(msg.contains("dozen"), "{msg}");
                assert_eq!(
                    env_secs("CUFFLINK_TEST_GOOD", 1).unwrap(),
                    Duration::from_secs(12)
                );
            },
        );
    }

    #[test]
    fn run_env_parses_case_insensitively_and_rejects_unknowns() {
        assert_eq!(RunEnv::from_str("Production").unwrap(), RunEnv::Production);
        assert_eq!(RunEnv::from_str(" staging ").unwrap(), RunEnv::Staging);
        assert_eq!(
            RunEnv::from_str("development").unwrap(),
            RunEnv::Development
        );
        assert!(RunEnv::from_str("prod").is_err());
    }

    #[test]
    fn worker_routes_must_be_valid_json_when_present() {
        assert!(parse_worker_routes(None).unwrap().is_empty());
        let routes = parse_worker_routes(Some(r#"{"svc":["a","b"]}"#)).unwrap();
        assert_eq!(routes["svc"], vec!["a".to_string(), "b".to_string()]);
        let err = parse_worker_routes(Some("{not json")).unwrap_err();
        assert!(err.to_string().contains("WORKER_ROUTES"), "{err}");
    }

    #[test]
    fn production_refuses_missing_or_default_database_credentials() {
        assert!(reject_default_credentials(RunEnv::Production, None).is_err());
        assert!(
            reject_default_credentials(RunEnv::Production, Some(DEFAULT_DATABASE_URL)).is_err()
        );
        assert!(reject_default_credentials(
            RunEnv::Production,
            Some("postgres://app:s3cret@db.internal:5432/cufflink")
        )
        .is_ok());
        assert!(reject_default_credentials(RunEnv::Development, None).is_ok());
        assert!(reject_default_credentials(RunEnv::Staging, Some(DEFAULT_DATABASE_URL)).is_ok());

        let tenant = "postgres://cufflink:cufflink@pgbouncer:6432/cufflink";
        let err = reject_default_tenant_credentials(RunEnv::Production, Some(tenant))
            .expect_err("default tenant password in production");
        assert!(err.to_string().contains("TENANT_DATABASE_URL"), "{err}");
        assert!(reject_default_tenant_credentials(RunEnv::Production, None).is_ok());
        assert!(reject_default_tenant_credentials(RunEnv::Staging, Some(tenant)).is_ok());
        assert!(reject_default_tenant_credentials(
            RunEnv::Production,
            Some("postgres://app:s3cret@pgbouncer:6432/cufflink")
        )
        .is_ok());
    }

    #[test]
    fn production_refuses_ephemeral_storage() {
        use cufflink_storage::Backend;
        for backend in [Backend::Memory, Backend::Local] {
            let err = reject_ephemeral_storage(RunEnv::Production, backend)
                .expect_err("ephemeral storage in production");
            assert!(err.to_string().contains(backend.name()), "{err}");
            assert!(reject_ephemeral_storage(RunEnv::Staging, backend).is_ok());
        }
        for backend in [Backend::S3, Backend::Gcs] {
            assert!(reject_ephemeral_storage(RunEnv::Production, backend).is_ok());
        }
    }

    #[test]
    fn load_fails_on_invalid_worker_routes_and_production_defaults() {
        with_env(
            &[
                ("RUN_ENV", None),
                ("DATABASE_URL", None),
                ("WORKER_ROUTES", Some("{bad")),
            ],
            || {
                let err = PlatformConfig::load(env).unwrap_err();
                assert!(err.to_string().contains("WORKER_ROUTES"), "{err}");
            },
        );
        with_env(
            &[
                ("RUN_ENV", Some("production")),
                ("DATABASE_URL", None),
                ("WORKER_ROUTES", None),
            ],
            || {
                let err = PlatformConfig::load(env).unwrap_err();
                assert!(err.to_string().contains("DATABASE_URL"), "{err}");
            },
        );
    }

    #[test]
    fn a_development_bootstrap_is_read_only_under_an_explicit_development_run_env() {
        let raw = "tenant=dev,realm=cufflink-dev,key_file=/run/cufflink/dev-key";
        with_env(
            &[
                ("RUN_ENV", Some("development")),
                ("CUFFLINK_DEV_BOOTSTRAP", Some(raw)),
            ],
            || {
                let config = PlatformConfig::load(env).expect("loads");
                assert_eq!(
                    config.dev_bootstrap.map(|dev| (dev.tenant, dev.realm)),
                    Some(("dev".to_string(), "cufflink-dev".to_string()))
                );
            },
        );
        for run_env in [None, Some("staging"), Some("production")] {
            with_env(
                &[
                    ("RUN_ENV", run_env),
                    ("DATABASE_URL", None),
                    ("CUFFLINK_DEV_BOOTSTRAP", Some(raw)),
                ],
                || {
                    let err = PlatformConfig::load(env).unwrap_err();
                    assert!(
                        err.to_string().starts_with(
                            "CUFFLINK_DEV_BOOTSTRAP is refused unless RUN_ENV=development"
                        ),
                        "{run_env:?}: {err}"
                    );
                },
            );
        }
        with_env(
            &[("RUN_ENV", None), ("CUFFLINK_DEV_BOOTSTRAP", None)],
            || {
                assert_eq!(
                    PlatformConfig::load(env).expect("loads").dev_bootstrap,
                    None
                )
            },
        );
    }

    #[test]
    fn a_startup_deadline_is_opt_in() {
        for (raw, expected) in [
            (Some("0"), None),
            (Some("120"), Some(Duration::from_secs(120))),
        ] {
            with_env(&[("RUN_ENV", None), ("STARTUP_DEADLINE_SECS", raw)], || {
                let config = PlatformConfig::load(env).expect("load");
                assert_eq!(config.startup_deadline, expected);
            });
        }
    }

    #[test]
    fn load_refuses_a_malformed_policy_default() {
        with_env(
            &[
                ("RUN_ENV", None),
                ("CUFFLINK_POLICY_GRAPHQL", Some("sometimes")),
            ],
            || {
                let err = PlatformConfig::load(env).unwrap_err();
                assert!(err.to_string().contains("CUFFLINK_POLICY_GRAPHQL"), "{err}");
            },
        );
    }

    #[test]
    fn tenant_databases_can_be_reached_through_a_pooler() {
        with_env(
            &[
                ("RUN_ENV", None),
                ("DATABASE_URL", Some("postgres://u:p@db:5432/cufflink")),
                (
                    "TENANT_DATABASE_URL",
                    Some("postgres://u:p@pgbouncer:6432/cufflink"),
                ),
                ("TENANT_DB_POOLER", Some("pgbouncer_transaction")),
            ],
            || {
                let config = PlatformConfig::load(env).expect("loads");
                assert_eq!(
                    config.tenant_database_url(),
                    "postgres://u:p@pgbouncer:6432/cufflink"
                );
                assert_eq!(
                    config.tenant_db_pooler,
                    cufflink_db::Pooler::PgBouncerTransaction
                );
            },
        );
        for (name, value) in [
            ("TENANT_DB_POOLER", "session"),
            ("TENANT_DATABASE_URL", "not a url"),
        ] {
            with_env(&[("RUN_ENV", None), (name, Some(value))], || {
                let err = PlatformConfig::load(env).unwrap_err();
                assert!(format!("{err:#}").contains(name), "{err:#}");
            });
        }
    }

    #[test]
    fn the_internal_url_is_an_http_base() {
        assert_eq!(parse_internal_url(None).unwrap(), None);
        assert_eq!(parse_internal_url(Some(" ")).unwrap(), None);
        assert_eq!(
            parse_internal_url(Some("http://cufflink-platform:8080/")).unwrap(),
            Some("http://cufflink-platform:8080".to_string())
        );
        for bad in ["cufflink-platform:8080", "ftp://x", "http://"] {
            let err = parse_internal_url(Some(bad)).unwrap_err();
            assert!(err.to_string().contains("PLATFORM_INTERNAL_URL"), "{err}");
        }
    }

    #[test]
    fn signing_keys_are_hex_or_raw_and_long_enough() {
        assert_eq!(
            parse_signing_key("00112233445566778899aabbccddeeff").unwrap(),
            (0u8..16).map(|i| i * 0x11).collect::<Vec<_>>()
        );
        assert_eq!(
            parse_signing_key("a passphrase of words").unwrap(),
            b"a passphrase of words".to_vec()
        );
        let err = parse_signing_key("short").unwrap_err();
        assert!(
            err.to_string().contains("CUFFLINK_CACHE_SIGNING_KEY"),
            "{err}"
        );
    }

    #[test]
    fn zero_pool_settings_are_refused_by_name() {
        for name in [
            "MAX_TENANT_POOLS",
            "MAX_CONNS_PER_TENANT",
            "WASM_MAX_OPEN_TX",
        ] {
            with_env(
                &[
                    ("RUN_ENV", None),
                    ("MAX_TENANT_POOLS", None),
                    ("MAX_CONNS_PER_TENANT", None),
                    ("WASM_MAX_OPEN_TX", None),
                    (name, Some("0")),
                ],
                || {
                    let err = PlatformConfig::load(env).unwrap_err();
                    assert!(err.to_string().contains(name), "{err}");
                },
            );
        }
    }

    #[test]
    fn an_outbox_enforced_by_default_needs_a_key_to_seal_its_rows() {
        let keys = |cache: Option<&'static str>| {
            [
                ("RUN_ENV", None),
                ("CUFFLINK_POLICY_OUTBOX", Some("enforce")),
                ("CUFFLINK_SECRETS_KEK", None),
                ("CUFFLINK_SECRETS_KEK_ID", None),
                ("CUFFLINK_SECRETS_KEK_PREVIOUS", None),
                ("CUFFLINK_SECRET_STORAGE", None),
                ("CUFFLINK_CACHE_SIGNING_KEY", cache),
            ]
        };
        with_env(&keys(None), || {
            let err = PlatformConfig::load(env).unwrap_err().to_string();
            assert!(
                err.starts_with("CUFFLINK_POLICY_OUTBOX: outbox: enforce needs a key"),
                "{err}"
            );
        });
        with_env(&keys(Some("a key of sixteen bytes")), || {
            let config = PlatformConfig::load(env).expect("keyed");
            assert!(config.outbox_seal().is_some());
        });
    }

    #[test]
    fn secrets_given_as_files_are_read() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let kek = |byte: u8| STANDARD.encode([byte; 32]);
        let secrets = |previous_id: &str| {
            [
                (
                    "DATABASE_URL",
                    "postgres://u:from-file@db:5432/cufflink".to_string(),
                ),
                (
                    "TENANT_DATABASE_URL",
                    "postgres://u:from-file@pgbouncer:6432/cufflink".to_string(),
                ),
                ("REDIS_URL", "redis://:from-file@redis".to_string()),
                ("NATS_PASSWORD", "nats-from-file".to_string()),
                ("CUFFLINK_SECRETS_KEK", kek(7)),
                (
                    "CUFFLINK_SECRETS_KEK_PREVIOUS",
                    format!("{previous_id}:{}", kek(8)),
                ),
                (
                    "CUFFLINK_CACHE_SIGNING_KEY",
                    "a key of sixteen bytes".to_string(),
                ),
                (
                    "CUFFLINK_SERVICE_IDENTITY_KEYS",
                    format!("k1:{}", "01".repeat(32)),
                ),
                (
                    "CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET",
                    "kc-from-file".to_string(),
                ),
            ]
        };
        let load = |secrets: &[(&str, String)]| {
            cufflink_settings::with_secret_files(
                |name| {
                    name.strip_suffix("_FILE")
                        .filter(|secret| secrets.iter().any(|(given, _)| given == secret))
                        .map(|secret| format!("/run/secrets/{secret}"))
                        .or_else(|| env(name))
                },
                |path| {
                    secrets
                        .iter()
                        .find(|(name, _)| path.ends_with(name))
                        .map(|(_, secret)| format!("{secret}\n").into_bytes())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
                },
            )
            .and_then(PlatformConfig::load)
        };
        let environment: Vec<(&str, Option<&str>)> = secrets("old")
            .iter()
            .map(|(name, _)| (*name, None))
            .chain([
                ("RUN_ENV", Some("production")),
                ("STORAGE_BACKEND", Some("s3")),
                ("NATS_USER", Some("platform")),
                ("NATS_NKEY_SEED", None),
                ("NATS_CREDS_FILE", None),
                ("CUFFLINK_SECRETS_KEK_ID", Some("new")),
                ("CUFFLINK_SECRET_STORAGE", None),
                ("CUFFLINK_DEV_BOOTSTRAP", None),
            ])
            .collect();
        with_env(&environment, || {
            let given = secrets("old");
            let secret = |name: &str| {
                given
                    .iter()
                    .find(|(given, _)| *given == name)
                    .map(|(_, secret)| secret.clone())
            };
            let config = load(&given).expect("loads");
            assert_eq!(Some(config.database_url.clone()), secret("DATABASE_URL"));
            assert_eq!(config.tenant_database_url, secret("TENANT_DATABASE_URL"));
            assert_eq!(config.redis_url, secret("REDIS_URL"));
            assert_eq!(
                config.nats_auth,
                crate::nats_conn::NatsAuth::UserPassword {
                    user: "platform".into(),
                    password: "nats-from-file".into()
                }
            );
            assert_eq!(config.secret_keys.current_id(), Some("new"));
            assert_eq!(
                config.cache_signing_key.as_deref(),
                Some(&b"a key of sixteen bytes"[..])
            );
            assert_eq!(
                config.service_identity_keys,
                secret("CUFFLINK_SERVICE_IDENTITY_KEYS")
            );
            assert_eq!(
                config.default_keycloak_ops_client_secret,
                secret("CUFFLINK_DEFAULT_KC_OPS_CLIENT_SECRET")
            );
            let previous = load(&secrets("new")).expect_err("the previous keys are read");
            assert!(
                previous.to_string().contains("key id 'new' names two keys"),
                "{previous}"
            );
        });
    }

    #[test]
    fn the_loki_label_model_is_legacy_unless_set_and_refused_when_unknown() {
        use crate::api::logs::LokiLabelModel;
        for (raw, model) in [
            (None, LokiLabelModel::Legacy),
            (Some("legacy"), LokiLabelModel::Legacy),
            (Some("scoped"), LokiLabelModel::Scoped),
        ] {
            with_env(&[("RUN_ENV", None), ("LOKI_LABEL_MODEL", raw)], || {
                assert_eq!(
                    PlatformConfig::load(env).expect("loads").loki_label_model,
                    model,
                    "{raw:?}"
                );
            });
        }
        with_env(
            &[("RUN_ENV", None), ("LOKI_LABEL_MODEL", Some("otlp"))],
            || {
                assert_eq!(
                    PlatformConfig::load(env).unwrap_err().to_string(),
                    "LOKI_LABEL_MODEL has an invalid value \"otlp\": 'otlp' is not one of legacy, scoped"
                );
            },
        );
    }

    #[test]
    fn load_defaults_when_nothing_is_set() {
        with_env(
            &[
                ("RUN_ENV", None),
                ("DATABASE_URL", None),
                ("WORKER_ROUTES", None),
                ("SHUTDOWN_GRACE_SECS", None),
                ("SHUTDOWN_UNREADY_DELAY_SECS", None),
                ("STARTUP_DEADLINE_SECS", None),
                ("DB_DEPLOY_STATEMENT_TIMEOUT_MS", None),
                ("DEPLOY_JOB_RETENTION_DAYS", None),
                ("CUFFLINK_POLICY_WASM_LIMITS", None),
                ("CUFFLINK_PLATFORM_ADMIN_ROLE", None),
                ("CUFFLINK_API_KEY_TTL_DAYS", None),
                ("CUFFLINK_CACHE_SIGNING_KEY", None),
                ("SSE_MAX_STREAMS_PER_PRINCIPAL", None),
                ("HOST_HTTP_MAX_RESPONSE_BYTES", None),
                ("MAX_TENANT_POOLS", None),
                ("MAX_CONNS_PER_TENANT", None),
                ("STORAGE_BACKEND", None),
                ("REGISTRY_RELOAD_SECS", None),
                ("KEYCLOAK_JWKS_MAX_STALE_SECS", None),
                ("WASM_MAX_OPEN_TX", None),
                ("CACHE_MGET_MAX_KEYS", None),
                ("CACHE_MGET_MAX_BYTES", None),
                ("TENANT_DATABASE_URL", None),
                ("TENANT_DB_POOLER", None),
            ],
            || {
                let config = PlatformConfig::load(env).expect("defaults load");
                assert_eq!(config.max_open_tx, 2);
                assert_eq!(
                    config.cache,
                    crate::wasm_runtime::host::CacheLimits::default()
                );
                assert_eq!(config.tenant_db_pooler, cufflink_db::Pooler::Direct);
                assert_eq!(config.tenant_database_url(), config.database_url);
                assert_eq!(config.registry_reload_interval, Duration::from_secs(60));
                assert_eq!(config.dlq_max_age, Duration::from_secs(14 * 24 * 60 * 60));
                assert_eq!(config.jwks_max_stale, Duration::from_secs(86_400));
                assert_eq!(config.run_env, RunEnv::Development);
                assert_eq!(config.shutdown_grace, Duration::from_secs(45));
                assert_eq!(config.shutdown_unready_delay, Duration::ZERO);
                assert_eq!(config.startup_deadline, None);
                assert_eq!(config.deploy_statement_timeout, Duration::from_secs(600));
                assert_eq!(
                    config.sql_migrations,
                    crate::schema_manager::migrate_exec::Timeouts {
                        lock_wait: Duration::from_secs(600),
                        statement: Duration::from_secs(600),
                        lock: Duration::from_secs(10),
                    }
                );
                assert_eq!(config.deploy_job_retention, DAY * 30);
                assert_eq!(
                    config.wasm,
                    WasmLimits {
                        budget: config.wasm.budget.clone(),
                        ..WasmLimits::default()
                    },
                    "everything but what the host's cgroup decides"
                );
                assert_eq!(config.wasm.budget.mode, crate::policy::PolicyMode::Warn);
                assert_eq!(config.max_tenant_pools, 64);
                assert_eq!(config.max_conns_per_tenant, 4);
                assert!(config.worker_routes.is_empty());
                assert_eq!(config.policy_defaults, PolicySet::builtin_defaults());
                assert_eq!(config.platform_admin_role, "cufflink-platform-admin");
                assert_eq!(config.api_key_ttl, chrono::Duration::days(90));
                assert_eq!(config.cache_signing_key, None);
                assert_eq!(config.sse_max_streams_per_principal, 20);
                assert_eq!(config.egress_max_response_bytes, 10 * 1024 * 1024);
                let worker = PlatformConfig::load_with(
                    TenantPoolDefaults {
                        max_pools: 32,
                        max_conns_per_tenant: 2,
                    },
                    env,
                )
                .expect("worker defaults load");
                assert_eq!(
                    (worker.max_tenant_pools, worker.max_conns_per_tenant),
                    (32, 2)
                );
            },
        );
    }
}
