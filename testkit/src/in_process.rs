use crate::error::TestkitError;
use crate::infra::{self, TestDb};
use crate::issuer::{TestIssuer, TokenBuilder};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use cufflink_platform::api::deployments::DEPLOY_API_VERSION;
use cufflink_platform::config::{PlatformConfig, RunEnv};
use cufflink_platform::shutdown::Shutdown;
use cufflink_platform::{app, storage};
use cufflink_types::policy::{PolicyKey, PolicyMode};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;
use uuid::Uuid;

const PLATFORM_REALM: &str = "cufflink";
const PLATFORM_ADMIN_ROLE: &str = "cufflink-platform-admin";
const DEPLOY_ROLE: &str = "cufflink-deploy";
const BUCKET: &str = "cufflink";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const WORKER_THREADS: usize = 4;
const READY_POLL: Duration = Duration::from_millis(50);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

static METRICS: OnceLock<Result<PrometheusHandle, String>> = OnceLock::new();

fn metrics() -> Result<PrometheusHandle, TestkitError> {
    METRICS
        .get_or_init(|| cufflink_telemetry::install_metrics().map_err(|e| format!("{e:#}")))
        .clone()
        .map_err(TestkitError::Platform)
}

type Configure = Box<dyn FnOnce(PlatformConfig) -> PlatformConfig + Send>;

pub struct PlatformOptions {
    nats: bool,
    redis: bool,
    configure: Option<Configure>,
}

impl Default for PlatformOptions {
    fn default() -> Self {
        Self {
            nats: true,
            redis: false,
            configure: None,
        }
    }
}

impl PlatformOptions {
    pub fn without_nats(self) -> Self {
        Self {
            nats: false,
            ..self
        }
    }

    pub fn with_redis(self) -> Self {
        Self {
            redis: true,
            ..self
        }
    }

    pub fn configure(
        self,
        configure: impl FnOnce(PlatformConfig) -> PlatformConfig + Send + 'static,
    ) -> Self {
        Self {
            configure: Some(Box::new(configure)),
            ..self
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TenantOptions {
    database_name: Option<String>,
    policies: Vec<(PolicyKey, PolicyMode, Option<Value>)>,
    subjects: Vec<String>,
    streams: Vec<String>,
}

impl TenantOptions {
    pub fn database_name(self, database: &str) -> Self {
        Self {
            database_name: Some(database.to_string()),
            ..self
        }
    }

    pub fn policy(mut self, key: PolicyKey, mode: PolicyMode, config: Option<Value>) -> Self {
        self.policies.push((key, mode, config));
        self
    }

    pub fn nats_subject(mut self, pattern: &str) -> Self {
        self.subjects.push(pattern.to_string());
        self
    }

    pub fn nats_stream(mut self, stream: &str) -> Self {
        self.streams.push(stream.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantHandle {
    pub id: Uuid,
    pub slug: String,
    pub realm: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub status: u16,
    pub body: Value,
}

impl Answer {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn success(self, what: &str) -> Result<Value, TestkitError> {
        if self.is_success() {
            Ok(self.body)
        } else {
            Err(TestkitError::Status {
                what: what.to_string(),
                status: self.status,
                body: self.body,
            })
        }
    }
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

impl Client {
    pub fn with_token(self, token: &str) -> Self {
        Self {
            token: Some(token.to_string()),
            ..self
        }
    }

    pub fn anonymous(self) -> Self {
        Self {
            token: None,
            ..self
        }
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Answer, TestkitError> {
        let request = self
            .http
            .request(method, format!("{}{path}", self.base_url));
        let request = match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };
        let response = request.send().await?;
        let status = response.status().as_u16();
        let bytes = response.bytes().await?;
        let body =
            serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                match String::from_utf8_lossy(&bytes) {
                    text if text.is_empty() => Value::Null,
                    text => Value::String(text.into_owned()),
                }
            });
        Ok(Answer { status, body })
    }

    pub async fn get(&self, path: &str) -> Result<Answer, TestkitError> {
        self.request(reqwest::Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: &Value) -> Result<Answer, TestkitError> {
        self.request(reqwest::Method::POST, path, Some(body)).await
    }

    pub async fn put(&self, path: &str, body: &Value) -> Result<Answer, TestkitError> {
        self.request(reqwest::Method::PUT, path, Some(body)).await
    }

    pub async fn patch(&self, path: &str, body: &Value) -> Result<Answer, TestkitError> {
        self.request(reqwest::Method::PATCH, path, Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> Result<Answer, TestkitError> {
        self.request(reqwest::Method::DELETE, path, None).await
    }
}

#[derive(Clone)]
pub struct TenantClient {
    client: Client,
    tenant: TenantHandle,
}

impl std::ops::Deref for TenantClient {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.client
    }
}

impl TenantClient {
    pub fn with_token(self, token: &str) -> Self {
        Self {
            client: self.client.with_token(token),
            ..self
        }
    }

    pub fn anonymous(self) -> Self {
        Self {
            client: self.client.anonymous(),
            ..self
        }
    }

    pub fn svc(&self, service: &str, path: &str) -> String {
        format!("/svc/{}/{service}{path}", self.tenant.slug)
    }
}

pub struct TenantToken<'a> {
    platform: &'a Platform,
    tenant: TenantHandle,
    token: TokenBuilder,
    permissions: Vec<String>,
}

impl TenantToken<'_> {
    fn with(self, change: impl FnOnce(TokenBuilder) -> TokenBuilder) -> Self {
        Self {
            token: change(self.token),
            ..self
        }
    }

    pub fn subject(self, subject: &str) -> Self {
        self.with(|token| token.subject(subject))
    }

    pub fn roles(self, roles: &[&str]) -> Self {
        self.with(|token| token.roles(roles))
    }

    pub fn client_id(self, client: &str) -> Self {
        self.with(|token| token.client(client))
    }

    pub fn claim(self, name: &str, value: impl Into<Value>) -> Self {
        self.with(|token| token.claim(name, value))
    }

    pub fn permissions(self, permissions: &[&str]) -> Self {
        Self {
            permissions: permissions.iter().map(|p| p.to_string()).collect(),
            ..self
        }
    }

    pub async fn sign(self) -> Result<String, TestkitError> {
        if !self.permissions.is_empty() {
            let user = self.token.names().unwrap_or_default().to_string();
            self.platform
                .grant(&self.tenant, &user, &self.permissions)
                .await?;
        }
        self.token.sign()
    }
}

struct OwnRuntime {
    handle: Handle,
    runtime: Option<Runtime>,
}

impl OwnRuntime {
    fn start() -> Result<Self, TestkitError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .thread_name("cufflink-testkit-platform")
            .enable_all()
            .build()?;
        Ok(Self {
            handle: runtime.handle().clone(),
            runtime: Some(runtime),
        })
    }
}

impl Drop for OwnRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

struct Running {
    runtime: OwnRuntime,
    served: JoinHandle<Result<(), String>>,
}

pub struct Platform {
    client: Client,
    issuer: TestIssuer,
    nats_url: Option<String>,
    shutdown: Shutdown,
    running: Option<Running>,
    db: Mutex<TestDb>,
}

fn configuration(
    database_url: &str,
    nats_url: &str,
    redis_url: Option<&str>,
    issuer: &TestIssuer,
) -> Result<PlatformConfig, TestkitError> {
    let ambient = cufflink_settings::from_env()
        .and_then(PlatformConfig::load)
        .map_err(|e| TestkitError::Platform(format!("{e:#}")))?;
    Ok(PlatformConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
        database_url: database_url.to_string(),
        nats_url: nats_url.to_string(),
        keycloak_url: issuer.url().to_string(),
        keycloak_realm: PLATFORM_REALM.to_string(),
        keycloak_issuer: None,
        redis_url: redis_url.map(str::to_string),
        loki_url: None,
        worker_routes: HashMap::new(),
        default_keycloak_ops_client_id: None,
        default_keycloak_ops_client_secret: None,
        service_account_clients: HashSet::new(),
        shutdown_grace: SHUTDOWN_GRACE,
        shutdown_unready_delay: Duration::ZERO,
        startup_deadline: Some(infra::health_deadline()),
        platform_internal_url: None,
        tenant_database_url: None,
        tenant_db_pooler: cufflink_db::Pooler::Direct,
        platform_admin_role: PLATFORM_ADMIN_ROLE.to_string(),
        run_env: RunEnv::Development,
        dev_bootstrap: None,
        ..ambient
    })
}

fn required(wanted: bool, url: Option<&str>, absent: &str) -> Result<Option<String>, TestkitError> {
    match (wanted, url) {
        (false, _) => Ok(None),
        (true, Some(url)) => Ok(Some(url.to_string())),
        (true, None) => Err(TestkitError::Unavailable(absent.to_string())),
    }
}

impl Platform {
    pub async fn start(options: PlatformOptions) -> Result<Platform, TestkitError> {
        let infra = infra::try_infra().await?;
        let nats_url = required(
            options.nats,
            infra.nats_url(),
            "no NATS (CUFFLINK_TEST_NATS_URL unset and no nats-server binary)",
        )?;
        let redis_url = required(
            options.redis,
            infra.redis_url(),
            "no Redis (CUFFLINK_TEST_REDIS_URL unset and no redis-server binary)",
        )?;
        let db = infra.try_create_database().await?;
        let runtime = OwnRuntime::start()?;
        let issuer = runtime
            .handle
            .spawn(TestIssuer::start())
            .await
            .map_err(|e| TestkitError::Platform(format!("the issuer did not start: {e}")))??;
        let unreachable = format!("nats://127.0.0.1:{}", infra::free_port());
        let config = configuration(
            &db.url,
            nats_url.as_deref().unwrap_or(&unreachable),
            redis_url.as_deref(),
            &issuer,
        )?;
        let config = match options.configure {
            Some(configure) => configure(config),
            None => config,
        };
        let shutdown = Shutdown::new(config.shutdown_grace);
        let metrics = metrics()?;
        let assembling = shutdown.clone();
        let (address, served) = runtime
            .handle
            .spawn(async move {
                let objects = storage::S3Storage::from_inner(cufflink_storage::in_memory(BUCKET));
                let platform = app::assemble(config, assembling, metrics, Some(objects))
                    .await
                    .map_err(|e| format!("{e:#}"))?;
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .map_err(|e| e.to_string())?;
                let address = listener.local_addr().map_err(|e| e.to_string())?;
                let served = tokio::spawn(async move {
                    app::serve(platform, listener)
                        .await
                        .map_err(|e| format!("{e:#}"))
                });
                Ok::<_, String>((address, served))
            })
            .await
            .map_err(|e| TestkitError::Platform(format!("the platform was not assembled: {e}")))?
            .map_err(TestkitError::Platform)?;
        let platform = Platform {
            client: Client {
                http: reqwest::Client::builder()
                    .timeout(REQUEST_TIMEOUT)
                    .build()?,
                base_url: format!("http://{address}"),
                token: None,
            },
            issuer,
            nats_url,
            shutdown,
            running: Some(Running { runtime, served }),
            db: Mutex::new(db),
        };
        platform.ready().await?;
        Ok(platform)
    }

    async fn ready(&self) -> Result<(), TestkitError> {
        let deadline = Instant::now() + infra::health_deadline();
        let mut last = Value::Null;
        while Instant::now() < deadline {
            if self
                .running
                .as_ref()
                .is_some_and(|running| running.served.is_finished())
            {
                return Err(TestkitError::Platform(
                    "the platform stopped before it was ready".to_string(),
                ));
            }
            match self.client.get("/ready").await {
                Ok(answer) if answer.body["state"] == "ready" => return Ok(()),
                Ok(answer) => last = answer.body,
                Err(_) => {}
            }
            tokio::time::sleep(READY_POLL).await;
        }
        Err(TestkitError::Platform(format!(
            "/ready never reported ready; it last answered {last}"
        )))
    }

    pub fn base_url(&self) -> &str {
        &self.client.base_url
    }

    pub fn issuer(&self) -> &TestIssuer {
        &self.issuer
    }

    pub fn anonymous(&self) -> Client {
        self.client.clone()
    }

    pub fn admin(&self) -> Result<Client, TestkitError> {
        Ok(self.client.clone().with_token(&self.admin_token()?))
    }

    pub fn admin_token(&self) -> Result<String, TestkitError> {
        self.issuer
            .token(PLATFORM_REALM)
            .subject("testkit-admin")
            .roles(&[PLATFORM_ADMIN_ROLE])
            .sign()
    }

    pub fn operator_token(&self, tenant: &TenantHandle) -> Result<String, TestkitError> {
        self.issuer
            .token(&tenant.realm)
            .subject("testkit-operator")
            .roles(&[DEPLOY_ROLE])
            .sign()
    }

    pub async fn tenant(
        &self,
        slug: &str,
        options: TenantOptions,
    ) -> Result<TenantHandle, TestkitError> {
        const CREATING: &str = "creating the tenant";
        let admin = self.admin()?;
        let created = admin
            .post(
                "/api/tenants",
                &json!({
                    "name": slug,
                    "slug": slug,
                    "keycloak_url": self.issuer.url(),
                    "keycloak_realm": slug,
                    "database_name": options.database_name,
                }),
            )
            .await?
            .success(CREATING)?;
        if options.database_name.is_none() {
            self.db
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .adopt_tenant_db(slug);
        }
        let id = created["id"]
            .as_str()
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| {
                TestkitError::Platform(format!("{CREATING} answered no id: {created}"))
            })?;
        if !options.policies.is_empty() {
            let policies: Map<String, Value> = options
                .policies
                .iter()
                .map(|(key, mode, config)| {
                    (
                        key.as_str().to_string(),
                        json!({ "mode": mode.as_str(), "config": config }),
                    )
                })
                .collect();
            admin
                .put(
                    &format!("/api/tenants/{slug}/policies"),
                    &json!({ "policies": policies }),
                )
                .await?
                .success("setting the tenant's policies")?;
        }
        if !(options.subjects.is_empty() && options.streams.is_empty()) {
            admin
                .put(
                    &format!("/api/tenants/{slug}/nats-claims"),
                    &json!({ "subjects": options.subjects, "streams": options.streams }),
                )
                .await?
                .success("setting the tenant's NATS claims")?;
        }
        Ok(TenantHandle {
            id,
            slug: slug.to_string(),
            realm: slug.to_string(),
        })
    }

    pub async fn external_database(&self, name: &str) -> Result<String, TestkitError> {
        let admin_url = self
            .db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admin_url()
            .to_string();
        let url = infra::create_database_beside(&admin_url, name).await?;
        self.db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .adopt_database(name);
        Ok(url)
    }

    pub fn database_url(&self) -> String {
        self.db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .url
            .clone()
    }

    pub fn tenant_database_url(&self, tenant: &TenantHandle) -> String {
        self.db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .tenant_url(&tenant.slug)
    }

    pub fn unique_slug(prefix: &str) -> String {
        format!("{prefix}-{}", &Uuid::new_v4().simple().to_string()[..8])
    }

    pub async fn deploy(
        &self,
        tenant: &TenantHandle,
        manifest: &impl Serialize,
        wasm: Option<&[u8]>,
    ) -> Result<Answer, TestkitError> {
        self.deploy_as(tenant, "", manifest, wasm).await
    }

    pub async fn deploy_as(
        &self,
        tenant: &TenantHandle,
        query: &str,
        manifest: &impl Serialize,
        wasm: Option<&[u8]>,
    ) -> Result<Answer, TestkitError> {
        let manifest = serde_json::to_value(manifest)
            .map_err(|e| TestkitError::Platform(format!("the manifest is not JSON: {e}")))?;
        let body = json!({
            "manifest": manifest,
            "wasm_base64": wasm.map(|wasm| STANDARD.encode(wasm)),
            "api_version": DEPLOY_API_VERSION,
        });
        self.client(tenant)?
            .post(&format!("/api/services/deploy{query}"), &body)
            .await
    }

    pub fn client(&self, tenant: &TenantHandle) -> Result<TenantClient, TestkitError> {
        Ok(TenantClient {
            client: self
                .client
                .clone()
                .with_token(&self.operator_token(tenant)?),
            tenant: tenant.clone(),
        })
    }

    pub fn token(&self, tenant: &TenantHandle) -> TenantToken<'_> {
        TenantToken {
            platform: self,
            tenant: tenant.clone(),
            token: self.issuer.token(&tenant.realm),
            permissions: Vec::new(),
        }
    }

    async fn grant(
        &self,
        tenant: &TenantHandle,
        user: &str,
        permissions: &[String],
    ) -> Result<(), TestkitError> {
        let operator = self.client(tenant)?;
        let role = operator
            .post(
                &format!("/api/tenants/{}/roles", tenant.id),
                &json!({
                    "name": Self::unique_slug("testkit"),
                    "description": format!("what a test gave {user}"),
                    "permissions": permissions,
                }),
            )
            .await?
            .success("creating a role")?;
        operator
            .post(
                &format!("/api/tenants/{}/roles/assign", tenant.id),
                &json!({ "user_id": user, "role_id": role["id"] }),
            )
            .await?
            .success("assigning a role")
            .map(drop)
    }

    pub async fn nats(&self) -> Result<async_nats::Client, TestkitError> {
        let url = self.nats_url.as_deref().ok_or_else(|| {
            TestkitError::Unavailable("the platform was started without NATS".to_string())
        })?;
        async_nats::connect(url)
            .await
            .map_err(|e| TestkitError::Nats(e.to_string()))
    }

    pub async fn shutdown(mut self) -> Result<(), TestkitError> {
        let Some(Running { runtime, served }) = self.running.take() else {
            return Ok(());
        };
        self.shutdown.trigger();
        let ended = tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(5), served).await;
        drop(runtime);
        match ended {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(reason))) => Err(TestkitError::Platform(reason)),
            Ok(Err(e)) => Err(TestkitError::Platform(format!(
                "the platform's task ended badly: {e}"
            ))),
            Err(_) => Err(TestkitError::Platform(
                "the platform did not stop within its grace".to_string(),
            )),
        }
    }
}

impl Drop for Platform {
    fn drop(&mut self) {
        if let Some(running) = self.running.take() {
            self.shutdown.trigger();
            drop(running);
        }
    }
}
