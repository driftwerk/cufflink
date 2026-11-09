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

    async fn legacy_upload(&self, service_id: &str, bundle: &[u8]) -> (u16, Value) {
        let response = self
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
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn upload(&mut self, service_id: &str, bundle: &[u8]) {
        let (status, uploaded) = self.legacy_upload(service_id, bundle).await;
        assert_eq!(status, 200, "{uploaded}");
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

    async fn reserve(&self, name: &str) -> (u16, Value) {
        self.send(
            Method::POST,
            "/api/services/web",
            self.deployer(),
            &json!({"name": name}),
        )
        .await
    }

    async fn reserved(&self) -> String {
        let (status, reserved) = self.reserve(SITE).await;
        assert_eq!(status, 200, "{reserved}");
        reserved["service_id"]
            .as_str()
            .expect("a service id")
            .to_string()
    }

    async fn put(&self, service_id: &str, sha256: &str, bundle: &[u8]) -> (u16, Value) {
        let response = self
            .platform
            .http
            .put(format!(
                "{}/api/services/{service_id}/web/artifacts/{sha256}",
                self.platform.base_url
            ))
            .header("authorization", self.deployer())
            .body(bundle.to_vec())
            .send()
            .await
            .expect("an upload");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn uploaded(&self, service_id: &str, bundle: &[u8]) -> Value {
        let (status, uploaded) = self.put(service_id, &sha256_hex(bundle), bundle).await;
        assert_eq!(status, 200, "{uploaded}");
        uploaded
    }

    async fn release_named(
        &self,
        package_version: &str,
        sha256: &str,
        size: usize,
    ) -> (u16, Value) {
        let manifest =
            json!({"name": SITE, "version": package_version, "mode": "web", "tables": []});
        self.send(
            Method::POST,
            "/api/services/deploy",
            self.deployer(),
            &json!({
                "manifest": manifest,
                "api_version": 1,
                "commit_hash": format!("commit-{package_version}"),
                "web_artifact": {"sha256": sha256, "size": size},
            }),
        )
        .await
    }

    async fn ship(&mut self, package_version: &str, bundle: &[u8]) -> (String, Value) {
        let service_id = self.reserved().await;
        self.uploaded(&service_id, bundle).await;
        let (status, released) = self
            .release_named(package_version, &sha256_hex(bundle), bundle.len())
            .await;
        assert_eq!(status, 200, "{released}");
        assert_eq!(
            self.heard().await["artifact_hash"],
            json!(sha256_hex(bundle))
        );
        (service_id, released)
    }

    async fn uploads(&self, service_id: &str) -> Vec<(String, String)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT sha256, s3_key FROM web_uploads WHERE service_id = $1::text::uuid
                 ORDER BY sha256",
                &[&service_id],
            )
            .await
            .expect("uploads")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    async fn age_uploads(&self, service_id: &str, sha256: &str) {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE web_uploads SET uploaded_at = now() - interval '25 hours'
                 WHERE service_id = $1::text::uuid AND sha256 = $2",
                &[&service_id, &sha256],
            )
            .await
            .expect("aged");
    }

    async fn stored(&self, key: &str) -> Option<Vec<u8>> {
        self.storage.download(key).await.ok()
    }

    fn key(service_id: &str, bundle: &[u8]) -> String {
        format!("web/{service_id}/{}.tar.gz", sha256_hex(bundle))
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

fn version_of(answer: &Value) -> i32 {
    answer["version"]
        .as_i64()
        .and_then(|version| i32::try_from(version).ok())
        .expect("a version")
}

#[tokio::test]
async fn reserving_a_web_app_creates_no_version() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (status, first) = w.reserve(SITE).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["name"], SITE);
    assert_eq!(first["created"], true);
    let service_id = first["service_id"].as_str().expect("a service id");

    let (status, again) = w.reserve(SITE).await;
    assert_eq!(status, 200, "{again}");
    assert_eq!(again["service_id"], json!(service_id));
    assert_eq!(again["current_version"], first["current_version"]);
    assert_eq!(again["created"], false);
    assert!(w.rows(service_id).await.1.is_empty(), "no version");
    w.heard_nothing().await;

    let (status, refused) = w.reserve("").await;
    assert_eq!(status, 422, "{refused}");
}

#[tokio::test]
async fn reserving_a_service_of_another_mode_is_refused() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let manifest = json!({
        "name": "catalog-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{"name": "items", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
        ]}],
    });
    let (status, deployed) = w
        .send(
            Method::POST,
            "/api/services/deploy",
            w.deployer(),
            &json!({"manifest": manifest}),
        )
        .await;
    assert_eq!(status, 200, "{deployed}");
    let service_id = deployed["service_id"].as_str().expect("a service id");
    let before = w.rows(service_id).await;

    let (status, refused) = w.reserve("catalog-service").await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused
            .to_string()
            .contains("service 'catalog-service' is not a web app"),
        "{refused}"
    );
    let (status, refused) = w
        .put(service_id, &sha256_hex(b"a bundle"), b"a bundle")
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(w.rows(service_id).await, before);
    assert!(w.uploads(service_id).await.is_empty());
    w.heard_nothing().await;
}

#[tokio::test]
async fn an_upload_is_stored_by_content_and_releases_nothing() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, _) = w.ship("0.1.0", b"bundle of the first").await;
    let before = w.rows(&service_id).await;
    let bundle = b"bundle of the second".as_slice();

    let uploaded = w.uploaded(&service_id, bundle).await;
    assert_eq!(uploaded["sha256"], json!(sha256_hex(bundle)));
    assert_eq!(uploaded["size"], bundle.len());
    assert_eq!(uploaded["s3_key"], json!(World::key(&service_id, bundle)));
    assert_eq!(uploaded["stored"], "uploaded");
    assert_eq!(
        w.uploaded(&service_id, bundle).await["stored"],
        "present",
        "the same content is not stored twice"
    );

    assert_eq!(w.rows(&service_id).await, before, "no version changed");
    w.heard_nothing().await;
    assert_eq!(
        w.stored(&World::key(&service_id, bundle)).await.as_deref(),
        Some(bundle)
    );
    assert!(w
        .uploads(&service_id)
        .await
        .contains(&(sha256_hex(bundle), World::key(&service_id, bundle))));

    let (status, refused) = w.put(&service_id, "3F9A", bundle).await;
    assert_eq!(status, 400, "{refused}");
}

#[tokio::test]
async fn concurrent_uploads_of_the_same_bundle_store_it_once() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = b"bundle uploaded twice at once".as_slice();
    let sha256 = sha256_hex(bundle);

    let ((first, a), (second, b)) = tokio::join!(
        w.put(&service_id, &sha256, bundle),
        w.put(&service_id, &sha256, bundle)
    );
    assert_eq!((first, second), (200, 200), "{a} {b}");
    for answer in [&a, &b] {
        assert_eq!(answer["sha256"], json!(sha256));
        assert_eq!(answer["size"], bundle.len());
    }
    assert_eq!(
        w.uploads(&service_id).await,
        [(sha256.clone(), World::key(&service_id, bundle))]
    );
    assert_eq!(
        w.stored(&World::key(&service_id, bundle)).await.as_deref(),
        Some(bundle)
    );
}

#[tokio::test]
async fn an_upload_whose_bytes_are_not_its_hash_is_refused() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let claimed = sha256_hex(b"what was built");
    let (status, refused) = w.put(&service_id, &claimed, b"something else").await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "the bundle's SHA-256 is {}, not {claimed}",
            sha256_hex(b"something else")
        )),
        "{refused}"
    );
    assert!(w.uploads(&service_id).await.is_empty());
    assert_eq!(
        w.stored(&format!("web/{service_id}/{claimed}.tar.gz"))
            .await,
        None
    );
    w.heard_nothing().await;
}

#[tokio::test]
async fn a_release_records_its_bundle_and_announces_it() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = b"bundle of the first".as_slice();
    w.uploaded(&service_id, bundle).await;
    let (status, released) = w
        .release_named("0.1.0", &sha256_hex(bundle), bundle.len())
        .await;
    assert_eq!(status, 200, "{released}");
    let version = version_of(&released);
    assert_eq!(
        released["web_artifact"],
        json!({"sha256": sha256_hex(bundle), "size": bundle.len()})
    );
    assert_eq!(released["runners_told"], true);
    assert_eq!(released.get("warnings"), None);

    let recorded = Row {
        version,
        key: Some(World::key(&service_id, bundle)),
        hash: Some(sha256_hex(bundle)),
        size: Some(bundle.len() as i64),
    };
    assert_eq!(w.rows(&service_id).await, (version, vec![recorded.clone()]));
    let event = w.heard().await;
    assert_eq!(event["version"], version);
    assert_eq!(event["s3_key"], json!(recorded.key));
    assert_eq!(event["artifact_hash"], json!(recorded.hash));

    let apps = w.runners_see().await.apps;
    assert_eq!(apps.len(), 1, "{apps:?}");
    assert_eq!(apps[0].sha256, Some(sha256_hex(bundle)));
}

#[tokio::test]
async fn a_release_naming_a_bundle_never_uploaded_changes_nothing() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, _) = w.ship("0.1.0", b"bundle of the first").await;
    let before = w.rows(&service_id).await;
    let never = b"bundle never uploaded".as_slice();

    let (status, refused) = w
        .release_named("0.2.0", &sha256_hex(never), never.len())
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "bundle {} of site was not uploaded, or is no longer stored; upload it again",
            sha256_hex(never)
        )),
        "{refused}"
    );

    let other = b"bundle uploaded at another size".as_slice();
    w.uploaded(&service_id, other).await;
    let (status, refused) = w
        .release_named("0.2.0", &sha256_hex(other), other.len() + 1)
        .await;
    assert_eq!(status, 409, "{refused}");

    w.storage
        .delete(&World::key(&service_id, other))
        .await
        .expect("the stored bundle");
    let (status, refused) = w
        .release_named("0.2.0", &sha256_hex(other), other.len())
        .await;
    assert_eq!(status, 409, "{refused}");

    assert_eq!(w.rows(&service_id).await, before);
    w.heard_nothing().await;
}

#[tokio::test]
async fn a_second_bundle_under_the_same_package_version_never_replaces_the_first() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (first, second) = (
        b"bundle of the first build".as_slice(),
        b"bundle of the second build".as_slice(),
    );
    let (service_id, shipped) = w.ship("0.1.0", first).await;
    let (_, v1) = w.rows(&service_id).await;

    let (_, released) = w.ship("0.1.0", second).await;
    assert_eq!(
        version_of(&released),
        version_of(&shipped) + 1,
        "{released}"
    );
    let (current, rows) = w.rows(&service_id).await;
    assert_eq!(current, version_of(&released));
    assert_eq!(rows[0], v1[0], "v1's row is unchanged");
    assert_eq!(rows[1].hash, Some(sha256_hex(second)));
    assert_eq!(
        w.stored(&World::key(&service_id, first)).await.as_deref(),
        Some(first),
        "v1's bundle is unchanged"
    );
}

#[tokio::test]
async fn the_same_bundle_again_is_skipped_and_announced() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let bundle = b"bundle of the first".as_slice();
    let (service_id, shipped) = w.ship("0.1.0", bundle).await;
    let before = w.rows(&service_id).await;

    let (status, skipped) = w
        .release_named("0.1.0", &sha256_hex(bundle), bundle.len())
        .await;
    assert_eq!(status, 200, "{skipped}");
    assert_eq!(skipped["skipped"], true);
    assert_eq!(skipped["version"], shipped["version"]);
    assert_eq!(
        skipped["web_artifact"],
        json!({"sha256": sha256_hex(bundle), "size": bundle.len()})
    );
    assert_eq!(skipped["runners_told"], true);
    assert_eq!(w.rows(&service_id).await, before);
    let event = w.heard().await;
    assert_eq!(event["version"], shipped["version"]);
    assert_eq!(event["artifact_hash"], json!(sha256_hex(bundle)));
}

#[tokio::test]
async fn a_web_release_records_its_bundle_in_the_deploy_audit() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let bundle = b"bundle of the first".as_slice();
    let (service_id, shipped) = w.ship("0.1.0", bundle).await;
    let actions: Vec<(String, Value)> =
        w.db.pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT action, details FROM audit_log WHERE resource_id = $1 ORDER BY created_at",
                &[&service_id],
            )
            .await
            .expect("audit")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
    let upload = actions
        .iter()
        .find(|(action, _)| action == "web.upload")
        .expect("the upload is audited");
    assert_eq!(upload.1["sha256"], json!(sha256_hex(bundle)));
    assert_eq!(upload.1["size"], bundle.len());
    let deploy = actions
        .iter()
        .find(|(action, _)| action == "deploy")
        .expect("the release is audited");
    assert_eq!(deploy.1["artifact_hash"], json!(sha256_hex(bundle)));
    assert_eq!(deploy.1["version"], shipped["version"]);
}

#[tokio::test]
async fn a_web_deploy_without_its_bundle_warns_and_is_skipped_by_its_manifest() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let bundle = b"bundle of the first".as_slice();
    let (service_id, _) = w.ship("0.1.0", bundle).await;
    let before = w.rows(&service_id).await;
    let (status, skipped) = w
        .send(
            Method::POST,
            "/api/services/deploy",
            w.deployer(),
            &json!({"manifest": {"name": SITE, "version": "0.1.0", "mode": "web", "tables": []}}),
        )
        .await;
    assert_eq!(status, 200, "{skipped}");
    assert_eq!(skipped["skipped"], true, "{skipped}");
    assert!(
        skipped["warnings"][0]
            .as_str()
            .is_some_and(|warning| warning.contains("Upgrade the CLI")),
        "{skipped}"
    );
    assert_eq!(w.rows(&service_id).await, before);
}

#[tokio::test]
async fn the_legacy_upload_fills_a_version_without_a_bundle() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, version) = w.register("0.1.0").await;
    let bundle = b"bundle of the first".as_slice();
    let (status, uploaded) = w.legacy_upload(&service_id, bundle).await;
    assert_eq!(status, 200, "{uploaded}");
    assert_eq!(uploaded["version"], version);
    assert_eq!(uploaded["released"], true);
    assert_eq!(
        uploaded["artifact_s3_key"],
        json!(World::key(&service_id, bundle))
    );
    assert_eq!(
        w.rows(&service_id).await,
        (
            version,
            vec![Row {
                version,
                key: Some(World::key(&service_id, bundle)),
                hash: Some(sha256_hex(bundle)),
                size: Some(bundle.len() as i64),
            }]
        )
    );
    assert_eq!(w.heard().await["version"], version);
}

#[tokio::test]
async fn the_legacy_upload_of_the_running_bundle_changes_nothing() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let bundle = b"bundle of the first".as_slice();
    let (service_id, shipped) = w.ship("0.1.0", bundle).await;
    let before = w.rows(&service_id).await;
    let (status, uploaded) = w.legacy_upload(&service_id, bundle).await;
    assert_eq!(status, 200, "{uploaded}");
    assert_eq!(uploaded["version"], shipped["version"]);
    assert_eq!(uploaded["released"], false);
    assert_eq!(w.rows(&service_id).await, before);
    assert_eq!(
        w.heard().await["version"],
        shipped["version"],
        "the runners are told again"
    );
}

#[tokio::test]
async fn the_legacy_upload_of_new_content_releases_a_new_version() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (first, second) = (
        b"bundle of the first".as_slice(),
        b"bundle of the second".as_slice(),
    );
    let (service_id, shipped) = w.ship("0.1.0", first).await;
    let (_, before) = w.rows(&service_id).await;

    let (status, uploaded) = w.legacy_upload(&service_id, second).await;
    assert_eq!(status, 200, "{uploaded}");
    let released = version_of(&shipped) + 1;
    assert_eq!(uploaded["version"], released);
    assert_eq!(uploaded["released"], true);
    let (current, rows) = w.rows(&service_id).await;
    assert_eq!(current, released);
    assert_eq!(
        rows[..1],
        before[..],
        "the running version's row is untouched"
    );
    assert_eq!(rows[1].hash, Some(sha256_hex(second)));
    assert_eq!(
        w.stored(&World::key(&service_id, first)).await.as_deref(),
        Some(first)
    );
    let commits: Vec<Option<String>> = w
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT commit_hash FROM deployments WHERE service_id = $1::text::uuid ORDER BY version",
            &[&service_id],
        )
        .await
        .expect("commits")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        commits[0], commits[1],
        "the new version keeps the current commit"
    );
    assert_eq!(w.heard().await["version"], released);
}

#[tokio::test]
async fn pruning_removes_a_bundle_no_version_records_and_keeps_a_shared_one() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (first, second, third) = (
        b"bundle of the first".as_slice(),
        b"bundle of the second".as_slice(),
        b"bundle of the third".as_slice(),
    );
    let (service_id, shipped) = w.ship("0.1.0", first).await;
    w.ship("0.2.0", second).await;
    w.ship("0.3.0", third).await;
    let oldest = version_of(&shipped);
    let (status, rolled_back) = w.rollback(&service_id, oldest).await;
    assert_eq!(status, 200, "{rolled_back}");
    w.heard().await;
    for bundle in [first, second, third] {
        w.age_uploads(&service_id, &sha256_hex(bundle)).await;
    }

    let (status, pruned) = w
        .send(
            Method::POST,
            &format!("/api/services/{service_id}/deployments/prune"),
            w.deployer(),
            &json!({"keep": 1}),
        )
        .await;
    assert_eq!(status, 200, "{pruned}");
    assert_eq!(
        pruned["deleted_versions"],
        json!([oldest + 2, oldest + 1, oldest])
    );
    assert_eq!(
        w.stored(&World::key(&service_id, first)).await.as_deref(),
        Some(first),
        "the rolled-back version records the bundle the oldest recorded"
    );
    assert_eq!(w.stored(&World::key(&service_id, second)).await, None);
    assert_eq!(w.stored(&World::key(&service_id, third)).await, None);
    assert_eq!(
        w.uploads(&service_id).await,
        [(sha256_hex(first), World::key(&service_id, first))]
    );
}

#[tokio::test]
async fn pruning_keeps_a_bundle_uploaded_again_since() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (first, second) = (
        b"bundle of the first".as_slice(),
        b"bundle of the second".as_slice(),
    );
    let (service_id, shipped) = w.ship("0.1.0", first).await;
    w.ship("0.2.0", second).await;
    for bundle in [first, second] {
        w.age_uploads(&service_id, &sha256_hex(bundle)).await;
    }
    assert_eq!(w.uploaded(&service_id, first).await["stored"], "present");

    let (status, pruned) = w
        .send(
            Method::POST,
            &format!("/api/services/{service_id}/deployments/prune"),
            w.deployer(),
            &json!({"keep": 1}),
        )
        .await;
    assert_eq!(status, 200, "{pruned}");
    assert_eq!(pruned["deleted_versions"], json!([version_of(&shipped)]));
    assert_eq!(
        w.stored(&World::key(&service_id, first)).await.as_deref(),
        Some(first),
        "an upload within the retention holds the bundle its pruned version recorded"
    );
    let (status, released) = w
        .release_named("0.3.0", &sha256_hex(first), first.len())
        .await;
    assert_eq!(status, 200, "{released}");
    assert_eq!(
        released["web_artifact"],
        json!({"sha256": sha256_hex(first), "size": first.len()})
    );
}

#[tokio::test]
async fn an_unreleased_upload_is_removed_after_its_retention_and_a_fresh_one_kept() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, _) = w.ship("0.1.0", b"bundle of the first").await;
    let (stale, fresh) = (
        b"bundle uploaded yesterday".as_slice(),
        b"bundle uploaded now".as_slice(),
    );
    w.uploaded(&service_id, stale).await;
    w.uploaded(&service_id, fresh).await;
    w.age_uploads(&service_id, &sha256_hex(stale)).await;

    w.ship("0.2.0", b"bundle of the second").await;

    assert_eq!(w.stored(&World::key(&service_id, stale)).await, None);
    assert_eq!(
        w.stored(&World::key(&service_id, fresh)).await.as_deref(),
        Some(fresh)
    );
    let uploads = w.uploads(&service_id).await;
    assert!(
        !uploads.iter().any(|(sha, _)| *sha == sha256_hex(stale)),
        "{uploads:?}"
    );
    assert!(
        uploads.iter().any(|(sha, _)| *sha == sha256_hex(fresh)),
        "{uploads:?}"
    );
}

#[tokio::test]
async fn deleting_a_web_app_removes_every_bundle_and_upload() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (released, unreleased) = (
        b"bundle of the first".as_slice(),
        b"bundle never released".as_slice(),
    );
    let (service_id, _) = w.ship("0.1.0", released).await;
    w.uploaded(&service_id, unreleased).await;
    let legacy = format!("web/{service_id}/v1.tar.gz");
    w.storage
        .upload(
            &legacy,
            b"a bundle of an earlier release".to_vec(),
            "application/gzip",
        )
        .await
        .expect("a legacy key");

    let (status, deleted) = w
        .send(
            Method::DELETE,
            &format!("/api/services/{service_id}"),
            w.deployer(),
            &Value::Null,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    for key in [
        World::key(&service_id, released),
        World::key(&service_id, unreleased),
        legacy,
    ] {
        assert_eq!(w.stored(&key).await, None, "{key}");
    }
    assert!(w.uploads(&service_id).await.is_empty());
}
