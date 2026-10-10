use crate::event;
use cufflink_secrets::SecretBox;
use cufflink_storage::ObjectStorage;
use cufflink_supervisor::runner_api::{key_refused, RunnerApi};
use cufflink_supervisor::{locked, ArtifactCap};
use cufflink_types::runner::WebApp;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebDeployment {
    pub tenant: String,
    pub service: String,
    pub service_id: Uuid,
    pub version: i32,
    pub sha256: Option<String>,
    pub bundle: Bundle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bundle {
    Object(String),
    Runner { size: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    Unread,
    Accepted,
    Refused,
}

pub struct Shared {
    pub pool: deadpool_postgres::Pool,
    pub secrets: SecretBox,
    pub storage: Arc<dyn ObjectStorage>,
    pub cap: ArtifactCap,
}

pub struct TenantScoped {
    pub api: RunnerApi,
    pub poll_every: Duration,
    standing: Mutex<Standing>,
}

impl TenantScoped {
    pub fn new(api: RunnerApi, poll_every: Duration) -> Self {
        Self {
            api,
            poll_every,
            standing: Mutex::new(Standing::Unread),
        }
    }

    pub fn tenant(&self) -> &str {
        &self.api.tenant().slug
    }

    pub fn standing(&self) -> Standing {
        *locked(&self.standing)
    }

    pub fn stand(&self, standing: Standing) -> bool {
        std::mem::replace(&mut *locked(&self.standing), standing) != standing
    }

    fn deployment(&self, app: WebApp) -> eyre::Result<WebDeployment> {
        let (sha256, size) = app.sha256.zip(app.size).ok_or_else(|| {
            eyre::eyre!(
                "{}/{} v{} has a bundle with no recorded hash, which the runner API cannot serve; \
                 upload it again",
                self.tenant(),
                app.service_name,
                app.version
            )
        })?;
        Ok(WebDeployment {
            tenant: self.tenant().to_string(),
            service: app.service_name,
            service_id: app.service_id,
            version: app.version,
            sha256: Some(sha256),
            bundle: Bundle::Runner { size },
        })
    }
}

pub enum Source {
    Shared(Shared),
    TenantScoped(TenantScoped),
}

#[derive(Debug)]
pub enum Unread {
    KeyRefused,
    Failed(eyre::Report),
}

impl From<eyre::Report> for Unread {
    fn from(error: eyre::Report) -> Self {
        match key_refused(&error) {
            true => Unread::KeyRefused,
            false => Unread::Failed(error),
        }
    }
}

impl Source {
    pub fn serves(&self, tenant: &str) -> bool {
        match self {
            Source::Shared(_) => true,
            Source::TenantScoped(scoped) => scoped.tenant() == tenant,
        }
    }

    pub fn subjects(&self) -> (String, String) {
        let tenant = match self {
            Source::Shared(_) => "",
            Source::TenantScoped(scoped) => scoped.tenant(),
        };
        let within = |kind: &str| match tenant {
            "" => format!("{kind}.web.>"),
            tenant => format!("{kind}.web.{tenant}.>"),
        };
        (within("deploy"), within("undeploy"))
    }

    pub fn ready(&self) -> bool {
        match self {
            Source::Shared(_) => true,
            Source::TenantScoped(scoped) => scoped.standing() == Standing::Accepted,
        }
    }

    pub fn key_refused(&self) -> bool {
        match self {
            Source::Shared(_) => false,
            Source::TenantScoped(scoped) => scoped.standing() == Standing::Refused,
        }
    }

    pub async fn current(
        &self,
        tenant: &str,
        service: &str,
    ) -> Result<Option<WebDeployment>, Unread> {
        match self {
            Source::Shared(shared) => shared.current(tenant, service).await.map_err(Unread::from),
            Source::TenantScoped(scoped) if scoped.tenant() == tenant => scoped
                .api
                .web()
                .await?
                .apps
                .into_iter()
                .find(|app| app.service_name == service)
                .map(|app| scoped.deployment(app))
                .transpose()
                .map_err(Unread::from),
            Source::TenantScoped(_) => Ok(None),
        }
    }

    pub async fn announced(&self, payload: &Value) -> Result<WebDeployment, String> {
        let (tenant, service) = event::target(payload).map_err(str::to_string)?;
        match self {
            Source::TenantScoped(scoped) if scoped.tenant() != tenant => Err(format!(
                "the event is of tenant '{tenant}', not WEB_TENANT '{}'",
                scoped.tenant()
            )),
            Source::Shared(_) | Source::TenantScoped(_) => {
                match self.current(&tenant, &service).await {
                    Ok(Some(deployment)) => Ok(deployment),
                    Ok(None) => Err(format!("{tenant}/{service} has no web deployment to run")),
                    Err(Unread::KeyRefused) => Err("the platform refuses the runner key".into()),
                    Err(Unread::Failed(e)) => Err(format!("{e:#}")),
                }
            }
        }
    }

    pub async fn bundle(&self, deployment: &WebDeployment) -> eyre::Result<Vec<u8>> {
        match (self, &deployment.bundle) {
            (Source::Shared(shared), Bundle::Object(key)) => shared.download(key).await,
            (Source::TenantScoped(scoped), Bundle::Runner { size }) => {
                let sha256 = deployment
                    .sha256
                    .as_deref()
                    .ok_or_else(|| eyre::eyre!("the runner API addresses a bundle by its hash"))?;
                scoped
                    .api
                    .web_bundle(deployment.service_id, sha256, *size)
                    .await
            }
            (Source::Shared(_), Bundle::Runner { .. })
            | (Source::TenantScoped(_), Bundle::Object(_)) => Err(eyre::eyre!(
                "{}/{} v{} was not read from this runner's source",
                deployment.tenant,
                deployment.service,
                deployment.version
            )),
        }
    }

    pub async fn environment(
        &self,
        deployment: &WebDeployment,
    ) -> eyre::Result<HashMap<String, String>> {
        match self {
            Source::Shared(shared) => Ok(shared.configs(deployment.service_id).await),
            Source::TenantScoped(scoped) => scoped.api.web_environment(deployment.service_id).await,
        }
    }
}

impl Shared {
    async fn current(&self, tenant: &str, service: &str) -> eyre::Result<Option<WebDeployment>> {
        let row = self
            .pool
            .get()
            .await?
            .query_opt(
                "SELECT d.artifact_s3_key, d.version, d.service_id, d.artifact_hash
                 FROM deployments d
                 JOIN services s ON s.id = d.service_id
                 JOIN tenants t ON t.id = s.tenant_id
                 WHERE d.artifact_s3_key IS NOT NULL
                   AND d.manifest->>'mode' = 'web'
                   AND d.status = 'active'
                   AND d.version = s.current_version
                   AND t.slug = $1
                   AND s.name = $2
                 LIMIT 1",
                &[&tenant, &service],
            )
            .await?;
        Ok(row.map(|row| WebDeployment {
            tenant: tenant.to_string(),
            service: service.to_string(),
            bundle: Bundle::Object(row.get(0)),
            version: row.get(1),
            service_id: row.get(2),
            sha256: row.get(3),
        }))
    }

    async fn download(&self, key: &str) -> eyre::Result<Vec<u8>> {
        info!(backend = self.storage.backend_kind(), key = %key, "Downloading artifact");
        self.cap.admit(key, self.storage.head(key).await?)?;
        let data = self.storage.download(key).await?;
        info!(key = %key, bytes = data.len(), "Downloaded artifact");
        Ok(data)
    }

    async fn configs(&self, service_id: Uuid) -> HashMap<String, String> {
        let client = match self.pool.get().await {
            Ok(client) => client,
            Err(e) => {
                warn!(error = %e, "Failed to get DB connection for service_configs");
                return HashMap::new();
            }
        };
        match cufflink_secrets::configs::environment(&self.secrets, &client, service_id).await {
            Ok(configs) => {
                if !configs.is_empty() {
                    info!(
                        "Loaded {} service_configs for service {}",
                        configs.len(),
                        service_id
                    );
                }
                configs
            }
            Err(e) => {
                warn!("Failed to fetch service_configs for {}: {}", service_id, e);
                HashMap::new()
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cufflink_supervisor::runner_api::{RunnerKey, RunnerTenant};
    use serde_json::json;

    fn scoped(tenant: &str) -> Source {
        Source::TenantScoped(TenantScoped::new(
            RunnerApi::new(
                "http://127.0.0.1:1",
                RunnerKey::Value("cfr_AAAAAAAAAAAA_s3cret".into()),
                RunnerTenant::parse("WEB_TENANT", tenant.into()).expect("a slug"),
                ArtifactCap {
                    max_bytes: 1 << 20,
                    setting: "WEB_ARTIFACT_MAX_BYTES",
                },
            )
            .expect("a client"),
            Duration::from_secs(30),
        ))
    }

    fn shared_at(database_url: &str, storage: Arc<dyn ObjectStorage>, max_bytes: u64) -> Source {
        let pool = cufflink_db::build_pool(database_url, 1).expect("a pool");
        Source::Shared(Shared {
            secrets: SecretBox::new(
                pool.clone(),
                cufflink_secrets::KeyRing::from_vars(|_| None).expect("no keys"),
                cufflink_secrets::StorageMode::Plaintext,
            ),
            pool,
            storage,
            cap: ArtifactCap {
                max_bytes,
                setting: "WEB_ARTIFACT_MAX_BYTES",
            },
        })
    }

    pub(crate) fn shared(storage: Arc<dyn ObjectStorage>, max_bytes: u64) -> Source {
        shared_at("postgres://nobody@127.0.0.1:1/nowhere", storage, max_bytes)
    }

    #[test]
    fn a_tenant_scoped_runner_takes_its_tenants_events_and_hosts_alone() {
        let scoped = scoped("acme");
        assert_eq!(
            scoped.subjects(),
            ("deploy.web.acme.>".into(), "undeploy.web.acme.>".into())
        );
        assert!(scoped.serves("acme"));
        assert!(!scoped.serves("globex"));
        let shared = shared(cufflink_storage::in_memory("platform"), 1);
        assert_eq!(
            shared.subjects(),
            ("deploy.web.>".into(), "undeploy.web.>".into())
        );
        assert!(shared.serves("globex"));
    }

    #[tokio::test]
    async fn an_event_of_another_tenant_is_refused_before_anything_is_read() {
        let refused = scoped("acme")
            .announced(&json!({
                "tenant_slug": "globex",
                "service_name": "site",
                "version": 1,
                "s3_key": "web/x/v1.tar.gz",
            }))
            .await
            .expect_err("another tenant's");
        assert_eq!(
            refused,
            "the event is of tenant 'globex', not WEB_TENANT 'acme'"
        );
        assert_eq!(
            scoped("acme")
                .current("globex", "site")
                .await
                .map_err(|e| format!("{e:?}"))
                .expect("not asked"),
            None,
            "another tenant's app is none of this runner's"
        );
    }

    #[tokio::test]
    async fn the_shared_runner_runs_what_the_database_records_whatever_an_event_says() {
        let Some(infra) = cufflink_testkit::infra().await else {
            return;
        };
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let client = db.pool().get().await.expect("checkout");
        let service: Uuid = client
            .query_one(
                "INSERT INTO services (tenant_id, name, current_version)
                 VALUES ($1, 'site', 2) RETURNING id",
                &[&tenant.id],
            )
            .await
            .expect("a service")
            .get(0);
        let (key, hash) = (format!("web/{service}/v2.tar.gz"), "c".repeat(64));
        for (version, key, hash) in [(1, None, None), (2, Some(&key), Some(&hash))] {
            client
                .execute(
                    "INSERT INTO deployments
                       (service_id, version, manifest_hash, manifest, artifact_s3_key, artifact_hash)
                     VALUES ($1, $2, 'h', '{\"mode\": \"web\"}', $3, $4)",
                    &[&service, &version, &key, &hash],
                )
                .await
                .expect("a deployment");
        }
        let source = shared_at(&db.url, cufflink_storage::in_memory("platform"), 1);
        let recorded = WebDeployment {
            tenant: tenant.slug.clone(),
            service: "site".into(),
            service_id: service,
            version: 2,
            sha256: Some(hash.clone()),
            bundle: Bundle::Object(key.clone()),
        };
        for forged in [
            json!({"tenant_slug": tenant.slug, "service_name": "site"}),
            json!({"tenant_slug": tenant.slug, "service_name": "site", "version": 1,
                   "service_id": Uuid::from_u128(7), "artifact_hash": "a".repeat(64),
                   "s3_key": "web/another-service/v1.tar.gz"}),
        ] {
            assert_eq!(
                source.announced(&forged).await,
                Ok(recorded.clone()),
                "{forged}"
            );
        }
        assert_eq!(
            source
                .announced(&json!({"tenant_slug": tenant.slug, "service_name": "other"}))
                .await,
            Err(format!(
                "{}/other has no web deployment to run",
                tenant.slug
            ))
        );
    }

    #[test]
    fn only_a_hashed_bundle_is_served_from_the_runner_api() {
        let Source::TenantScoped(scoped) = scoped("acme") else {
            unreachable!("a tenant-scoped source");
        };
        let app = WebApp {
            service_id: Uuid::nil(),
            service_name: "site".into(),
            version: 2,
            sha256: Some("ab".into()),
            size: Some(9),
        };
        assert_eq!(
            scoped.deployment(app.clone()).expect("a deployment").bundle,
            Bundle::Runner { size: 9 }
        );
        assert!(scoped
            .deployment(WebApp {
                sha256: None,
                ..app
            })
            .expect_err("no hash")
            .to_string()
            .contains("no recorded hash"));
    }

    #[test]
    fn the_key_standing_is_ready_once_accepted_and_refused_when_refused() {
        let source = scoped("acme");
        let Source::TenantScoped(scoped) = &source else {
            unreachable!("a tenant-scoped source");
        };
        assert!(!source.ready() && !source.key_refused());
        assert!(scoped.stand(Standing::Accepted));
        assert!(!scoped.stand(Standing::Accepted), "unchanged");
        assert!(source.ready());
        assert!(scoped.stand(Standing::Refused));
        assert!(!source.ready() && source.key_refused());
    }

    #[tokio::test]
    async fn a_bundle_past_the_byte_cap_is_not_downloaded() {
        let storage = cufflink_storage::in_memory("platform");
        storage
            .upload("web/svc/v1.tar.gz", vec![0; 10], "application/gzip")
            .await
            .expect("upload");
        let deployment = WebDeployment {
            tenant: "acme".into(),
            service: "svc".into(),
            service_id: Uuid::nil(),
            version: 1,
            sha256: None,
            bundle: Bundle::Object("web/svc/v1.tar.gz".into()),
        };
        assert_eq!(
            shared(storage.clone(), 9)
                .bundle(&deployment)
                .await
                .expect_err("too large")
                .to_string(),
            "artifact web/svc/v1.tar.gz is 10 bytes, more than 9 (WEB_ARTIFACT_MAX_BYTES)"
        );
        assert_eq!(
            shared(storage, 10)
                .bundle(&deployment)
                .await
                .expect("at the cap")
                .len(),
            10
        );
    }
}
