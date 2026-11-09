use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::wasm_runtime::sha256_hex;
use cufflink_storage::ObjectStorage;
use cufflink_types::runner::WebSet;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const SITE: &str = "site";
const BUCKET: &str = "cufflink";
const QUIET: Duration = Duration::from_millis(500);

struct World {
    platform: PlatformProcess,
    tenant: Tenant,
    admin_key: String,
    db: TestDb,
    _storage_dir: tempfile::TempDir,
    storage: Arc<dyn ObjectStorage>,
    deploys: async_nats::Subscriber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    version: i32,
    key: Option<String>,
    hash: Option<String>,
    size: Option<i64>,
}

impl World {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let storage_dir = tempfile::tempdir().expect("storage directory");
        let dir = storage_dir.path().to_string_lossy().to_string();
        let platform = PlatformProcess::start(
            &db.url,
            Some(nats_url),
            &[
                ("STORAGE_BACKEND", "local"),
                ("STORAGE_LOCAL_DIR", dir.as_str()),
                ("S3_BUCKET", BUCKET),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin_key = db.mint_admin_key().await;
        let nats = async_nats::connect(nats_url).await.expect("nats");
        let deploys = nats
            .subscribe(format!("deploy.web.{}.>", tenant.slug))
            .await
            .expect("subscribe");
        nats.flush()
            .await
            .expect("the subscription reaches the server");
        let storage = cufflink_storage::local_dir(storage_dir.path(), BUCKET).expect("storage");
        Some(Self {
            platform,
            tenant,
            admin_key,
            db,
            _storage_dir: storage_dir,
            storage,
            deploys,
        })
    }

    async fn send(&self, method: Method, path: &str, auth: String, body: &Value) -> (u16, Value) {
        let response = self
            .platform
            .http
            .request(method, format!("{}{path}", self.platform.base_url))
            .header("authorization", auth)
            .json(body)
            .send()
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    fn deployer(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    async fn register(&self, package_version: &str) -> (String, i32) {
        let manifest =
            json!({"name": SITE, "version": package_version, "mode": "web", "tables": []});
        let (status, deployed) = self
            .send(
                Method::POST,
                "/api/services/deploy",
                self.deployer(),
                &json!({"manifest": manifest, "api_version": 1}),
            )
            .await;
        assert_eq!(status, 200, "{deployed}");
        (
            deployed["service_id"]
                .as_str()
                .expect("a service id")
                .to_string(),
            deployed["version"]
                .as_i64()
                .and_then(|version| i32::try_from(version).ok())
                .expect("a version"),
        )
    }

    async fn upload(&mut self, service_id: &str, bundle: &[u8]) {
        let uploaded = self
            .platform
            .http
            .post(format!(
                "{}/api/services/{service_id}/web",
                self.platform.base_url
            ))
            .header("authorization", self.deployer())
            .body(bundle.to_vec())
            .send()
            .await
            .expect("an upload");
        assert_eq!(uploaded.status().as_u16(), 200);
        self.heard().await;
    }

    async fn release(&mut self, package_version: &str, bundle: Option<&[u8]>) -> (String, i32) {
        let (service_id, version) = self.register(package_version).await;
        if let Some(bundle) = bundle {
            self.upload(&service_id, bundle).await;
        }
        (service_id, version)
    }

    async fn rollback(&self, service_id: &str, target: i32) -> (u16, Value) {
        self.send(
            Method::POST,
            &format!("/api/services/{service_id}/rollback"),
            self.deployer(),
            &json!({"target_version": target}),
        )
        .await
    }

    async fn rows(&self, service_id: &str) -> (i32, Vec<Row>) {
        let client = self.db.pool().get().await.expect("checkout");
        let current = client
            .query_one(
                "SELECT current_version FROM services WHERE id = $1::text::uuid",
                &[&service_id],
            )
            .await
            .expect("the service")
            .get(0);
        let rows = client
            .query(
                "SELECT version, artifact_s3_key, artifact_hash, artifact_size FROM deployments
                 WHERE service_id = $1::text::uuid ORDER BY version",
                &[&service_id],
            )
            .await
            .expect("deployments")
            .iter()
            .map(|row| Row {
                version: row.get(0),
                key: row.get(1),
                hash: row.get(2),
                size: row.get(3),
            })
            .collect();
        (current, rows)
    }

    async fn heard(&mut self) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(10), self.deploys.next())
            .await
            .expect("a deploy.web event")
            .expect("the subscription is open");
        assert_eq!(
            message.subject.as_str(),
            format!("deploy.web.{}.{SITE}", self.tenant.slug)
        );
        serde_json::from_slice(&message.payload).expect("a JSON payload")
    }

    async fn heard_nothing(&mut self) {
        assert!(
            tokio::time::timeout(QUIET, self.deploys.next())
                .await
                .is_err(),
            "no deploy.web event"
        );
    }

    async fn runners_see(&self) -> WebSet {
        let (status, minted) = self
            .send(
                Method::POST,
                &format!("/api/tenants/{}/runner-keys", self.tenant.slug),
                format!("ApiKey {}", self.admin_key),
                &json!({"name": "web-runner", "runs": "web"}),
            )
            .await;
        assert_eq!(status, 201, "{minted}");
        let key = minted["key"].as_str().expect("the key");
        let (status, apps) = self
            .send(
                Method::GET,
                "/api/runner/web",
                format!("RunnerKey {key}"),
                &Value::Null,
            )
            .await;
        assert_eq!(status, 200, "{apps}");
        serde_json::from_value(apps).expect("a web set")
    }
}

#[tokio::test]
async fn rolling_back_a_web_app_carries_the_targets_bundle_and_announces_it() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (first, second) = (
        b"bundle of the first".as_slice(),
        b"bundle of the second, larger".as_slice(),
    );
    let (service_id, target) = w.release("0.1.0", Some(first)).await;
    let (_, running) = w.release("0.2.0", Some(second)).await;
    let (_, before) = w.rows(&service_id).await;

    let (status, rolled_back) = w.rollback(&service_id, target).await;
    assert_eq!(status, 200, "{rolled_back}");
    let new_version = running + 1;
    assert_eq!(rolled_back["new_version"], new_version);
    assert_eq!(rolled_back["artifact_hash"], json!(sha256_hex(first)));
    assert_eq!(rolled_back["native_hash"], Value::Null);
    assert_eq!(rolled_back["runners_told"], true);

    let (current, rows) = w.rows(&service_id).await;
    assert_eq!(current, new_version);
    let (earlier, added) = rows.split_at(before.len());
    assert_eq!(earlier, before, "earlier versions are untouched");
    let recorded = before
        .iter()
        .find(|row| row.version == target)
        .expect("the target");
    assert_eq!(
        added,
        [Row {
            version: new_version,
            ..recorded.clone()
        }],
        "the new version records the target's bundle, under its recorded key"
    );

    let event = w.heard().await;
    assert_eq!(event["version"], new_version);
    assert_eq!(event["artifact_hash"], json!(sha256_hex(first)));
    assert_eq!(event["s3_key"], json!(recorded.key));
    assert_eq!(event["service_id"], json!(service_id));

    let apps = w.runners_see().await.apps;
    assert_eq!(apps.len(), 1, "{apps:?}");
    assert_eq!(apps[0].version, new_version);
    assert_eq!(apps[0].sha256, Some(sha256_hex(first)));
    assert_eq!(apps[0].size, Some(first.len() as u64));
}

#[tokio::test]
async fn rolling_back_to_a_version_without_a_bundle_is_refused_before_anything_changes() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, bundled) = w.release("0.1.0", Some(b"bundle of the first")).await;
    let (_, unbundled) = w.release("0.2.0", None).await;
    w.release("0.3.0", Some(b"bundle of the third")).await;
    let before = w.rows(&service_id).await;

    let (status, refused) = w.rollback(&service_id, unbundled).await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "v{unbundled} of site has no bundle (its build or upload never completed); \
             the newest earlier version with one is v{bundled}: \
             cufflink rollback --version {bundled}"
        )),
        "{refused}"
    );
    assert_eq!(w.rows(&service_id).await, before);
    w.heard_nothing().await;
}

#[tokio::test]
async fn rolling_back_to_a_bundle_missing_from_storage_is_refused() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, target) = w.release("0.1.0", Some(b"bundle of the first")).await;
    w.release("0.2.0", Some(b"bundle of the second")).await;
    let before = w.rows(&service_id).await;
    let key = before
        .1
        .iter()
        .find(|row| row.version == target)
        .and_then(|row| row.key.clone())
        .expect("the target records its bundle");
    w.storage.delete(&key).await.expect("the stored bundle");

    let (status, refused) = w.rollback(&service_id, target).await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "the bundle of v{target} ({key}) is missing from object storage"
        )),
        "{refused}"
    );
    assert_eq!(w.rows(&service_id).await, before);
    w.heard_nothing().await;
}
