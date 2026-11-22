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

    async fn put_part(&self, service_id: &str, part: &[u8]) -> (u16, Value) {
        self.put_part_hinted(service_id, part, "").await
    }

    async fn put_part_hinted(&self, service_id: &str, part: &[u8], hint: &str) -> (u16, Value) {
        let response = self
            .platform
            .http
            .put(format!(
                "{}/api/services/{service_id}/web/parts/{}{hint}",
                self.platform.base_url,
                sha256_hex(part)
            ))
            .header("authorization", self.deployer())
            .body(part.to_vec())
            .send()
            .await
            .expect("a part");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn assemble(
        &self,
        service_id: &str,
        sha256: &str,
        size: usize,
        parts: &[String],
    ) -> (u16, Value) {
        self.send(
            Method::POST,
            &format!("/api/services/{service_id}/web/artifacts/{sha256}/parts"),
            self.deployer(),
            &json!({"size": size, "parts": parts}),
        )
        .await
    }

    async fn sent_in_parts(&self, service_id: &str, parts: &[&[u8]]) -> Vec<String> {
        for part in parts {
            let (status, stored) = self.put_part(service_id, part).await;
            assert_eq!(status, 200, "{stored}");
        }
        parts.iter().map(|part| sha256_hex(part)).collect()
    }

    async fn parts(&self, service_id: &str) -> Vec<(String, String)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT sha256, s3_key FROM web_upload_parts WHERE service_id = $1::text::uuid
                 ORDER BY sha256",
                &[&service_id],
            )
            .await
            .expect("parts")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    async fn age_part(&self, service_id: &str, sha256: &str) {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .execute(
                "UPDATE web_upload_parts SET uploaded_at = now() - interval '25 hours'
                 WHERE service_id = $1::text::uuid AND sha256 = $2",
                &[&service_id, &sha256],
            )
            .await
            .expect("aged");
    }

    async fn audited(&self, service_id: &str) -> Vec<(String, Value)> {
        self.db
            .pool()
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
            .collect()
    }

    fn part_key(service_id: &str, part: &[u8]) -> String {
        format!("web/{service_id}/parts/{}", sha256_hex(part))
    }

    fn logged(&self, message: &str) -> Vec<Value> {
        self.platform
            .output_tail()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .map(|line| line.get("fields").cloned().unwrap_or(line))
            .filter(|fields| fields["message"] == message)
            .collect()
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
    let (status, refused) = w.put_part(service_id, b"a part").await;
    assert_eq!(status, 409, "{refused}");
    let (status, refused) = w
        .assemble(
            service_id,
            &sha256_hex(b"a part"),
            6,
            &[sha256_hex(b"a part")],
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(w.rows(service_id).await, before);
    assert!(w.uploads(service_id).await.is_empty());
    assert!(w.parts(service_id).await.is_empty());
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
    let actions = w.audited(&service_id).await;
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

const PARTED: [&[u8]; 3] = [b"a bundle ", b"uploaded in ", b"three parts"];

fn whole(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

#[tokio::test]
async fn a_bundle_assembled_from_its_parts_is_stored_released_and_its_parts_removed() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);

    let (status, first) = w.put_part(&service_id, PARTED[0]).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(
        first,
        json!({"sha256": sha256_hex(PARTED[0]), "size": PARTED[0].len(), "stored": "uploaded"})
    );
    assert_eq!(
        w.put_part(&service_id, PARTED[0]).await.1["stored"],
        "present",
        "the same part is not stored twice"
    );
    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    assert_eq!(
        w.stored(&World::part_key(&service_id, PARTED[1]))
            .await
            .as_deref(),
        Some(PARTED[1])
    );

    let (status, assembled) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &parts)
        .await;
    assert_eq!(status, 200, "{assembled}");
    assert_eq!(
        assembled,
        json!({"sha256": sha256_hex(&bundle), "size": bundle.len(),
               "s3_key": World::key(&service_id, &bundle), "stored": "uploaded"})
    );
    assert_eq!(
        w.stored(&World::key(&service_id, &bundle)).await,
        Some(bundle.clone()),
        "the bundle is its parts in order"
    );
    assert!(w.parts(&service_id).await.is_empty());
    for part in PARTED {
        assert_eq!(w.stored(&World::part_key(&service_id, part)).await, None);
    }
    let (status, again) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &parts)
        .await;
    assert_eq!(status, 200, "{again}");
    assert_eq!(
        again["stored"], "present",
        "an assembly asked again once it is stored needs none of its parts"
    );
    w.heard_nothing().await;

    let (status, released) = w
        .release_named("0.1.0", &sha256_hex(&bundle), bundle.len())
        .await;
    assert_eq!(status, 200, "{released}");
    assert_eq!(w.heard().await["artifact_hash"], json!(sha256_hex(&bundle)));
}

#[tokio::test]
async fn a_bundle_already_stored_is_assembled_without_reading_a_part() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);
    w.uploaded(&service_id, &bundle).await;
    let mut named = w.sent_in_parts(&service_id, &PARTED[..1]).await;
    named.push(sha256_hex(b"never uploaded"));

    let (status, assembled) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &named)
        .await;
    assert_eq!(status, 200, "{assembled}");
    assert_eq!(assembled["stored"], "present");
    assert!(
        w.parts(&service_id).await.is_empty(),
        "the parts it names are removed"
    );
    assert_eq!(
        w.stored(&World::part_key(&service_id, PARTED[0])).await,
        None
    );
}

#[tokio::test]
async fn a_bundle_above_the_multipart_threshold_streams_from_its_parts_only_when_it_is_its_hash() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let parts = [b'a', b'b', b'c'].map(|byte| vec![byte; 12 * 1024 * 1024]);
    let slices = parts.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let bundle = whole(&slices);
    let hashes = w.sent_in_parts(&service_id, &slices).await;

    let claimed = sha256_hex(b"a bundle of other bytes");
    let (status, refused) = w
        .assemble(&service_id, &claimed, bundle.len(), &hashes)
        .await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        w.stored(&format!("web/{service_id}/{claimed}.tar.gz"))
            .await,
        None,
        "a stream that is not its hash is never completed"
    );
    assert_eq!(w.parts(&service_id).await.len(), slices.len());

    let (status, assembled) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &hashes)
        .await;
    assert_eq!(status, 200, "{assembled}");
    assert_eq!(assembled["stored"], "uploaded");
    assert!(
        w.stored(&World::key(&service_id, &bundle)).await == Some(bundle),
        "the bundle is its parts in order"
    );
    assert!(w.parts(&service_id).await.is_empty());
}

#[tokio::test]
async fn concurrent_assemblies_of_the_same_bundle_both_answer_and_store_it_once() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);
    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    let sha256 = sha256_hex(&bundle);

    let ((first, a), (second, b)) = tokio::join!(
        w.assemble(&service_id, &sha256, bundle.len(), &parts),
        w.assemble(&service_id, &sha256, bundle.len(), &parts)
    );
    assert_eq!((first, second), (200, 200), "{a} {b}");
    assert_eq!(
        w.stored(&World::key(&service_id, &bundle)).await,
        Some(bundle.clone())
    );
    assert_eq!(
        w.uploads(&service_id).await,
        [(sha256_hex(&bundle), World::key(&service_id, &bundle))]
    );
    assert!(w.parts(&service_id).await.is_empty());
}

#[tokio::test]
async fn a_part_stored_at_another_size_than_recorded_is_named_in_a_conflict() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);
    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    w.storage
        .upload(
            &World::part_key(&service_id, PARTED[1]),
            b"other bytes of another length".to_vec(),
            "application/octet-stream",
        )
        .await
        .expect("replaced");

    let (status, refused) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &parts)
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "part {} of bundle {} was not uploaded, or is no longer stored",
            parts[1],
            sha256_hex(&bundle)
        )),
        "{refused}"
    );
    assert_eq!(w.stored(&World::key(&service_id, &bundle)).await, None);
}

#[tokio::test]
async fn a_part_recorded_but_no_longer_stored_is_named_in_a_conflict() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);
    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    w.storage
        .delete(&World::part_key(&service_id, PARTED[2]))
        .await
        .expect("deleted");

    let (status, refused) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &parts)
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "part {} of bundle {} was not uploaded, or is no longer stored",
            parts[2],
            sha256_hex(&bundle)
        )),
        "{refused}"
    );
    assert_eq!(w.stored(&World::key(&service_id, &bundle)).await, None);
}

#[tokio::test]
async fn an_assembly_of_parts_that_are_not_the_bundle_stores_nothing() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    let bundle = whole(&PARTED);
    let claimed = sha256_hex(b"another bundle");
    let unstored = |sha256: &str| format!("web/{service_id}/{sha256}.tar.gz");

    let never = sha256_hex(b"never uploaded");
    let (status, refused) = w
        .assemble(
            &service_id,
            &claimed,
            bundle.len(),
            &[parts[0].clone(), never.clone()],
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "part {never} of bundle {claimed} was not uploaded, or is no longer stored; \
             upload it again"
        )),
        "{refused}"
    );

    let unsent = sha256_hex(b"also never uploaded");
    let (status, refused) = w
        .assemble(
            &service_id,
            &claimed,
            bundle.len(),
            &[
                never.clone(),
                parts[0].clone(),
                unsent.clone(),
                never.clone(),
            ],
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "parts {never}, {unsent} of bundle {claimed} were not uploaded, or are no longer \
             stored; upload them again"
        )),
        "every missing part is named once: {refused}"
    );

    let (status, refused) = w
        .assemble(&service_id, &claimed, bundle.len(), &parts)
        .await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "the bundle's SHA-256 is {}, not {claimed}",
            sha256_hex(&bundle)
        )),
        "{refused}"
    );

    let (status, refused) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len() + 1, &parts)
        .await;
    assert_eq!(status, 422, "{refused}");
    assert!(
        refused.to_string().contains(&format!(
            "the parts of bundle {} hold {} bytes, not {}",
            sha256_hex(&bundle),
            bundle.len(),
            bundle.len() + 1
        )),
        "{refused}"
    );

    let (status, refused) = w
        .assemble(&service_id, &sha256_hex(&bundle), bundle.len(), &[])
        .await;
    assert_eq!(status, 400, "{refused}");
    let (status, refused) = w
        .assemble(
            &service_id,
            &sha256_hex(&bundle),
            bundle.len(),
            &["3F9A".to_string()],
        )
        .await;
    assert_eq!(status, 400, "{refused}");
    assert!(
        refused
            .to_string()
            .contains("'3F9A' is not a part's SHA-256"),
        "{refused}"
    );
    let (status, refused) = w
        .assemble(
            &service_id,
            &sha256_hex(&bundle),
            bundle.len(),
            &vec![parts[0].clone(); 1025],
        )
        .await;
    assert_eq!(status, 400, "{refused}");
    assert!(
        refused
            .to_string()
            .contains("a bundle is assembled from 1 to 1024 parts, not 1025"),
        "{refused}"
    );
    let (status, refused) = w
        .assemble(
            &service_id,
            &sha256_hex(&bundle),
            250 * 1024 * 1024 + 1,
            &parts,
        )
        .await;
    assert_eq!(status, 413, "{refused}");

    for sha256 in [&claimed, &sha256_hex(&bundle)] {
        assert_eq!(w.stored(&unstored(sha256)).await, None);
    }
    assert!(w.uploads(&service_id).await.is_empty());
    assert_eq!(
        w.parts(&service_id).await.len(),
        PARTED.len(),
        "the parts are kept"
    );
    w.heard_nothing().await;
}

#[tokio::test]
async fn a_part_must_be_its_hash_not_empty_and_at_most_its_limit() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let put = |sha256: String, part: Vec<u8>| {
        let request = w
            .platform
            .http
            .put(format!(
                "{}/api/services/{service_id}/web/parts/{sha256}",
                w.platform.base_url
            ))
            .header("authorization", w.deployer())
            .body(part);
        async move {
            request
                .send()
                .await
                .map(|response| response.status().as_u16())
        }
    };

    assert_eq!(
        put(sha256_hex(b"built"), b"something else".to_vec())
            .await
            .ok(),
        Some(422)
    );
    assert_eq!(put(sha256_hex(b""), Vec::new()).await.ok(), Some(400));
    let oversized = vec![b'x'; 16 * 1024 * 1024 + 1];
    let refused = put(sha256_hex(&oversized), oversized).await;
    assert!(
        matches!(refused, Ok(413)) || refused.as_ref().is_err_and(|e| !e.is_connect()),
        "a part above the limit is refused with a 413, or its connection closed before it is read: {refused:?}"
    );
    let largest = vec![b'x'; 16 * 1024 * 1024];
    assert_eq!(put(sha256_hex(&largest), largest).await.ok(), Some(200));
    assert_eq!(w.parts(&service_id).await.len(), 1);
}

#[tokio::test]
async fn parts_never_assembled_are_removed_after_their_retention_and_fresh_ones_kept() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let (service_id, _) = w.ship("0.1.0", b"bundle of the first").await;
    let (stale, fresh) = (PARTED[0], PARTED[1]);
    w.sent_in_parts(&service_id, &[stale, fresh]).await;
    w.age_part(&service_id, &sha256_hex(stale)).await;

    w.ship("0.2.0", b"bundle of the second").await;

    assert_eq!(w.stored(&World::part_key(&service_id, stale)).await, None);
    assert_eq!(
        w.stored(&World::part_key(&service_id, fresh))
            .await
            .as_deref(),
        Some(fresh)
    );
    assert_eq!(
        w.parts(&service_id).await,
        [(sha256_hex(fresh), World::part_key(&service_id, fresh))]
    );
}

fn short(sha256: &str) -> &str {
    &sha256[..12]
}

fn timed(fields: &Value, timings: &[&str]) -> bool {
    timings.iter().all(|timing| fields[*timing].is_u64())
}

#[tokio::test]
async fn each_part_and_assembly_logs_what_it_waited_for() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let bundle = whole(&PARTED);
    let sha256 = sha256_hex(&bundle);
    let parts = PARTED.map(sha256_hex).to_vec();

    let (status, lacking) = w.assemble(&service_id, &sha256, bundle.len(), &parts).await;
    assert_eq!(status, 409, "{lacking}");
    let lacked = w.logged("Web bundle assembly answered");
    assert_eq!(lacked.len(), 1, "{lacked:?}");
    assert_eq!(
        (
            &lacked[0]["service_id"],
            &lacked[0]["bundle"],
            &lacked[0]["parts"],
            &lacked[0]["size"],
            &lacked[0]["outcome"],
            &lacked[0]["missing"],
        ),
        (
            &json!(service_id),
            &json!(short(&sha256)),
            &json!(3),
            &json!(bundle.len()),
            &json!("incomplete"),
            &json!(3),
        )
    );
    assert!(timed(&lacked[0], &["turn_ms", "elapsed_ms"]), "{lacked:?}");

    for (part, hint) in PARTED.iter().zip([
        format!("?bundle={sha256}&index=1&bundle=not-a-hash&index=9"),
        "?bundle=not-a-hash&index=two".to_string(),
        String::new(),
    ]) {
        let (status, stored) = w.put_part_hinted(&service_id, part, &hint).await;
        assert_eq!(status, 200, "{stored}");
    }
    let stored = w.logged("Web part stored");
    assert_eq!(stored.len(), 3, "{stored:?}");
    for (fields, part) in stored.iter().zip(PARTED) {
        assert_eq!(
            (
                &fields["service_id"],
                &fields["part"],
                &fields["size"],
                &fields["stored"]
            ),
            (
                &json!(service_id),
                &json!(short(&sha256_hex(part))),
                &json!(part.len()),
                &json!("uploaded")
            )
        );
        assert!(
            timed(fields, &["receive_ms", "store_ms", "elapsed_ms"]),
            "{fields}"
        );
    }
    assert_eq!(
        (&stored[0]["bundle"], &stored[0]["index"]),
        (&json!(short(&sha256)), &json!(1)),
        "a part names the bundle and place its uploader gave first"
    );
    for unhinted in &stored[1..] {
        assert!(
            unhinted.get("bundle").is_none() && unhinted.get("index").is_none(),
            "a hint that is not a hash and an index is left out: {unhinted}"
        );
    }

    let (status, assembled) = w.assemble(&service_id, &sha256, bundle.len(), &parts).await;
    assert_eq!(status, 200, "{assembled}");
    let answered = w.logged("Web bundle assembly answered");
    assert_eq!(answered.len(), 2, "{answered:?}");
    assert_eq!(
        (&answered[1]["outcome"], answered[1].get("missing")),
        (&json!("uploaded"), None)
    );
    assert!(
        timed(
            &answered[1],
            &[
                "turn_ms",
                "slot_ms",
                "read_wait_ms",
                "stream_ms",
                "elapsed_ms"
            ]
        ),
        "{answered:?}"
    );
    assert!(
        answered[1]["read_wait_ms"].as_u64() <= answered[1]["stream_ms"].as_u64()
            && answered[1]["stream_ms"].as_u64() <= answered[1]["elapsed_ms"].as_u64(),
        "the reads are waited on within the stream, and the stream within the assembly: {answered:?}"
    );
}

#[tokio::test]
async fn a_part_or_assembly_that_fails_logs_how_long_it_took() {
    let Some(w) = World::start().await else {
        return;
    };
    let service_id = w.reserved().await;
    let claimed = sha256_hex(b"built");
    let status = w
        .platform
        .http
        .put(format!(
            "{}/api/services/{service_id}/web/parts/{claimed}?bundle={claimed}&index=4",
            w.platform.base_url
        ))
        .header("authorization", w.deployer())
        .body(b"something else".to_vec())
        .send()
        .await
        .expect("an answer")
        .status();
    assert_eq!(status, 422);
    let failed = w.logged("Web part not stored");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(
        (
            &failed[0]["part"],
            &failed[0]["bundle"],
            &failed[0]["index"],
            &failed[0]["size"]
        ),
        (
            &json!(short(&claimed)),
            &json!(short(&claimed)),
            &json!(4),
            &json!(b"something else".len())
        )
    );
    assert!(
        failed[0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("SHA-256")),
        "{failed:?}"
    );
    assert!(
        timed(&failed[0], &["receive_ms", "elapsed_ms"]),
        "{failed:?}"
    );
    assert!(
        failed[0].get("store_ms").is_none(),
        "a part refused before its write has no write to time: {failed:?}"
    );

    let parts = w.sent_in_parts(&service_id, &PARTED).await;
    let other = sha256_hex(b"a bundle of other bytes");
    let (status, refused) = w
        .assemble(&service_id, &other, whole(&PARTED).len(), &parts)
        .await;
    assert_eq!(status, 422, "{refused}");
    let failed = w.logged("Web bundle not assembled");
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert_eq!(
        (&failed[0]["bundle"], &failed[0]["parts"]),
        (&json!(short(&other)), &json!(3))
    );
    assert!(
        timed(
            &failed[0],
            &[
                "turn_ms",
                "slot_ms",
                "read_wait_ms",
                "stream_ms",
                "elapsed_ms"
            ]
        ),
        "{failed:?}"
    );
}
