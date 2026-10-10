use crate::infra::{infra, Tenant, TestDb};
use crate::legacy;
use crate::platform::PlatformProcess;
use reqwest::Method;
use serde_json::{json, Value};
use tokio_postgres::Client;

pub struct Deployment {
    pub db: TestDb,
    pub platform: PlatformProcess,
    pub tenant: Tenant,
    pub database: Client,
}

impl Deployment {
    pub async fn start(settings: &[(&str, &str)]) -> Option<Self> {
        let infra = infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), settings).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let database = legacy::tenant_database(&db, &tenant).await;
        Some(Self {
            db,
            platform,
            tenant,
            database,
        })
    }

    pub async fn under(key: &str, policy: Value) -> Option<Self> {
        let deployment = Self::start(&[]).await?;
        deployment.policy(key, policy).await;
        Some(deployment)
    }

    pub async fn policy(&self, key: &str, policy: Value) {
        let admin = format!("ApiKey {}", self.db.mint_admin_key().await);
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &admin,
                Some(&json!({"policies": {key: policy}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    pub async fn send(&self, method: Method, path: &str, body: Option<Value>) -> (u16, Value) {
        self.platform
            .send(
                method,
                path,
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
    }

    pub async fn deploy_as(&self, query: &str, request: Value) -> (u16, Value) {
        self.send(
            Method::POST,
            &format!("/api/services/deploy{query}"),
            Some(request),
        )
        .await
    }

    pub async fn deploy(&self, request: Value) -> (u16, Value) {
        self.deploy_as("", request).await
    }

    pub async fn deployed(&self, request: Value) -> Value {
        let (status, body) = self.deploy(request).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    pub async fn texts(&self, sql: &str) -> Vec<String> {
        self.database
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|row| row.get(0))
            .collect()
    }
}
