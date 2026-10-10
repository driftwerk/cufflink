use cufflink_testkit as common;

use base64::Engine as _;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::wasm_runtime::sha256_hex;
use cufflink_storage::ObjectStorage;
use cufflink_test_guests::Profile;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const NATIVE: &str = "ext-native";
const CALLER: &str = "caller-svc";
const BUCKET: &str = "cufflink";
const MAX_BYTES: usize = 4096;

fn provides(extensions: &[(&str, &[&str])]) -> Value {
    json!(extensions
        .iter()
        .map(|(name, callers)| json!({"name": name, "callers": callers}))
        .collect::<Vec<_>>())
}

fn native_manifest(extensions: &[(&str, &[&str])]) -> Value {
    json!({
        "name": NATIVE,
        "version": "0.1.0",
        "mode": "native",
        "tables": [],
        "provides": provides(extensions),
    })
}

fn caller_manifest(extensions: &[&str]) -> Value {
    let mut manifest = common::wasm_manifest(CALLER, &[("call", "/call")]);
    manifest["extensions"] = json!(extensions);
    manifest
}

fn encoded(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn object_key(service_id: &str, binary: &[u8]) -> String {
    format!("native/{service_id}/{}", sha256_hex(binary))
}

struct World {
    platform: PlatformProcess,
    tenant: Tenant,
    admin_key: String,
    db: TestDb,
    nats_url: &'static str,
    storage_dir: tempfile::TempDir,
    storage: Arc<dyn ObjectStorage>,
}

impl World {
    async fn start(extra: &[(&str, &str)]) -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let storage_dir = tempfile::tempdir().expect("storage directory");
        let dir = storage_dir.path().to_string_lossy().to_string();
        let max_bytes = MAX_BYTES.to_string();
        let env: Vec<(&str, &str)> = [
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("S3_BUCKET", BUCKET),
            ("NATIVE_ARTIFACT_MAX_BYTES", max_bytes.as_str()),
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
        let platform = PlatformProcess::start(&db.url, Some(nats_url), &env).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin_key = db.mint_admin_key().await;
        let storage = cufflink_storage::local_dir(storage_dir.path(), BUCKET).expect("storage");
        Some(Self {
            platform,
            tenant,
            admin_key,
            db,
            nats_url,
            storage_dir,
            storage,
        })
    }

    async fn beside(&self, extra: &[(&str, &str)]) -> PlatformProcess {
        let dir = self.storage_dir.path().to_string_lossy().to_string();
        let max_bytes = MAX_BYTES.to_string();
        let env: Vec<(&str, &str)> = [
            ("STORAGE_BACKEND", "local"),
            ("STORAGE_LOCAL_DIR", dir.as_str()),
            ("S3_BUCKET", BUCKET),
            ("NATIVE_ARTIFACT_MAX_BYTES", max_bytes.as_str()),
            ("CUFFLINK_EXTENSIONS", "true"),
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
        let platform = PlatformProcess::start(&self.db.url, Some(self.nats_url), &env).await;
        platform.wait_ready().await;
        platform
    }

    fn admin(&self) -> String {
        format!("ApiKey {}", self.admin_key)
    }

    fn deployer(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    async fn as_deployer(
        &self,
        platform: &PlatformProcess,
        method: Method,
        path: &str,
        body: &Value,
    ) -> (u16, Value) {
        let response = platform
            .http
            .request(method, format!("{}{path}", platform.base_url))
            .header("authorization", self.deployer())
            .json(body)
            .send()
            .await
            .expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn deploy_native_on(
        &self,
        platform: &PlatformProcess,
        query: &str,
        manifest: &Value,
        binary: &[u8],
    ) -> (u16, Value) {
        self.as_deployer(
            platform,
            Method::POST,
            &format!("/api/services/deploy{query}"),
            &json!({"manifest": manifest, "native_base64": encoded(binary), "api_version": 1}),
        )
        .await
    }

    async fn deploy_native(&self, manifest: &Value, binary: &[u8]) -> (u16, Value) {
        self.deploy_native_on(&self.platform, "", manifest, binary)
            .await
    }

    async fn ended(&self, platform: &PlatformProcess, begun: &Value) -> Value {
        let id = begun["deploy_id"].as_str().expect("a deploy id");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let (status, job) = self
                .as_deployer(
                    platform,
                    Method::GET,
                    &format!("/api/deploys/{id}"),
                    &Value::Null,
                )
                .await;
            assert_eq!(status, 200, "{job}");
            if matches!(job["status"].as_str(), Some("succeeded" | "failed")) {
                return job;
            }
            assert!(tokio::time::Instant::now() < deadline, "{job}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn services(&self) -> Vec<String> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT name FROM services WHERE tenant_id = $1 ORDER BY name",
                &[&self.tenant.id],
            )
            .await
            .expect("services")
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    async fn service_id(&self, name: &str) -> Option<String> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_opt(
                "SELECT id::text FROM services WHERE tenant_id = $1 AND name = $2",
                &[&self.tenant.id, &name],
            )
            .await
            .expect("service")
            .map(|row| row.get(0))
    }

    async fn rollback(&self, service_id: &str, target: i64) -> (u16, Value) {
        self.as_deployer(
            &self.platform,
            Method::POST,
            &format!("/api/services/{service_id}/rollback"),
            &json!({"target_version": target}),
        )
        .await
    }

    async fn registrations(&self) -> Vec<(String, String, Value, Value)> {
        let (status, listed) = self
            .platform
            .send(
                Method::GET,
                &format!("/api/tenants/{}/extensions", self.tenant.slug),
                &self.admin(),
                None,
            )
            .await;
        assert_eq!(status, 200, "{listed}");
        listed["extensions"]
            .as_array()
            .expect("extensions")
            .iter()
            .map(|e| {
                (
                    e["name"].as_str().unwrap_or_default().to_string(),
                    e["backend"].as_str().unwrap_or_default().to_string(),
                    e["callers"].clone(),
                    e["service_id"].clone(),
                )
            })
            .collect()
    }

    async fn artifacts(
        &self,
        service_id: &str,
    ) -> Vec<(i32, Option<String>, Option<String>, Option<i64>)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT version, artifact_s3_key, artifact_hash, artifact_size FROM deployments
                 WHERE service_id = $1::text::uuid ORDER BY version",
                &[&service_id],
            )
            .await
            .expect("deployments")
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect()
    }

    async fn rows_of(&self, service_id: &str) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM tenant_extensions WHERE service_id = $1::text::uuid",
                &[&service_id],
            )
            .await
            .expect("count")
            .get(0)
    }

    async fn stored(&self, key: &str) -> bool {
        self.storage
            .head_in_bucket(BUCKET, key)
            .await
            .expect("head")
            .is_some()
    }

    async fn call(&self, extension: &str) -> Value {
        let response = self
            .platform
            .http
            .post(self.platform.fn_url(&self.tenant, CALLER, "call"))
            .json(&json!({"extension": extension, "payload": null}))
            .send()
            .await
            .expect("call");
        assert_eq!(response.status().as_u16(), 200);
        response.json().await.expect("an answer")
    }
}

struct Heard {
    ext: futures_util::stream::BoxStream<'static, async_nats::Message>,
    web: async_nats::Subscriber,
}

impl Heard {
    async fn listen(nats_url: &str, tenant: &Tenant) -> Self {
        let client = async_nats::connect(nats_url).await.expect("nats");
        let subscribe = |subject: String| {
            let client = client.clone();
            async move { client.subscribe(subject).await.expect("subscribe") }
        };
        let deployed = subscribe(format!("deploy.ext.{}.>", tenant.slug)).await;
        let undeployed = subscribe(format!("undeploy.ext.{}.>", tenant.slug)).await;
        let web = subscribe(format!("undeploy.web.{}.>", tenant.slug)).await;
        client
            .flush()
            .await
            .expect("subscriptions reach the server");
        Self {
            ext: futures_util::stream::select(deployed, undeployed).boxed(),
            web,
        }
    }

    async fn next(&mut self) -> (String, Value) {
        let message = tokio::time::timeout(Duration::from_secs(10), self.ext.next())
            .await
            .expect("an event in time")
            .expect("an event");
        (
            message.subject.to_string(),
            serde_json::from_slice(&message.payload).expect("a JSON event"),
        )
    }

    async fn nothing_more(&mut self) {
        let quiet = Duration::from_millis(500);
        assert!(
            tokio::time::timeout(quiet, self.ext.next()).await.is_err(),
            "no further extension event"
        );
        assert!(
            tokio::time::timeout(quiet, self.web.next()).await.is_err(),
            "no web app event"
        );
    }
}

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm("ext-call", Profile::Debug).expect("built guest")),
    }
}

#[tokio::test]
async fn a_native_service_is_deployed_rolled_back_and_removed_by_the_tenants_deployer() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let mut heard = Heard::listen(w.nats_url, &w.tenant).await;
    let everyone: &[&str] = &["*"];
    let (binary_a, binary_b) = (
        b"\x7fELF native a".as_slice(),
        b"\x7fELF native b".as_slice(),
    );

    let v1 = native_manifest(&[("test.native", everyone)]);
    let (status, deployed) = w.deploy_native(&v1, binary_a).await;
    assert_eq!(status, 200, "{deployed}");
    assert_eq!(deployed["version"], 2);
    assert_eq!(deployed["native_hash"], sha256_hex(binary_a));
    assert_eq!(deployed["wasm_hash"], Value::Null);
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    let key_a = object_key(&service_id, binary_a);
    assert!(
        w.stored(&key_a).await,
        "the binary is stored under its content"
    );
    assert_eq!(
        w.storage.download(&key_a).await.expect("the binary"),
        binary_a
    );
    assert_eq!(
        w.artifacts(&service_id).await,
        [(
            2,
            Some(key_a.clone()),
            Some(sha256_hex(binary_a)),
            Some(binary_a.len() as i64)
        )]
    );
    assert_eq!(
        w.registrations().await,
        [(
            "test.native".to_string(),
            "native".to_string(),
            json!(["*"]),
            json!(service_id)
        )]
    );
    assert_eq!(
        heard.next().await,
        (
            format!("deploy.ext.{}.{NATIVE}", w.tenant.slug),
            json!({
                "service_id": service_id,
                "service_name": NATIVE,
                "tenant_slug": w.tenant.slug,
                "version": 2,
                "s3_key": key_a,
                "artifact_hash": sha256_hex(binary_a),
                "extensions": ["test.native"],
            })
        )
    );

    let (status, skipped) = w.deploy_native(&v1, binary_a).await;
    assert_eq!(status, 200, "{skipped}");
    assert_eq!(skipped["skipped"], true);
    assert_eq!(skipped["version"], 2);
    assert_eq!(skipped["native_hash"], sha256_hex(binary_a));

    let (status, code_only) = w.deploy_native(&v1, binary_b).await;
    assert_eq!(status, 200, "{code_only}");
    assert_eq!(
        code_only["version"], 3,
        "a code-only change is a new version"
    );
    let key_b = object_key(&service_id, binary_b);
    assert!(w.stored(&key_b).await);
    assert!(
        w.stored(&key_a).await,
        "the old binary is kept for rollback"
    );
    assert_eq!(heard.next().await.1["s3_key"], json!(key_b));

    let v3 = native_manifest(&[("test.native", everyone), ("test.second", everyone)]);
    let (status, widened) = w.deploy_native(&v3, binary_b).await;
    assert_eq!(status, 200, "{widened}");
    assert_eq!(widened["version"], 4);
    assert_eq!(
        heard.next().await.1["extensions"],
        json!(["test.native", "test.second"])
    );
    assert_eq!(w.rows_of(&service_id).await, 2);

    let (status, caller) = w
        .platform
        .deploy_module(&w.tenant, &caller_manifest(&["test.native"]), &wasm)
        .await;
    assert_eq!(status, 200, "{caller}");
    assert_eq!(
        w.call("test.native").await["error"],
        "unavailable",
        "the registration is found and admits the caller; no runner serves it yet"
    );

    for narrowed in [
        native_manifest(&[("test.native", &["other-svc"]), ("test.second", everyone)]),
        native_manifest(&[("test.second", everyone)]),
    ] {
        let (status, refused) = w.deploy_native(&narrowed, binary_b).await;
        assert_eq!(status, 409, "{refused}");
        assert_eq!(
            refused["error"],
            "provides: service 'caller-svc' is deployed calling extension 'test.native'; \
             redeploy it without the extension first"
        );
    }

    let commit_of = |version: i32, labelled: Option<&'static str>| {
        let (pool, service_id) = (w.db.pool(), service_id.clone());
        async move {
            pool.get()
                .await
                .expect("checkout")
                .query_one(
                    "UPDATE deployments SET commit_hash = COALESCE($3, commit_hash)
                     WHERE service_id = $1::text::uuid AND version = $2 RETURNING commit_hash",
                    &[&service_id, &version, &labelled],
                )
                .await
                .expect("deployment")
                .get::<_, Option<String>>(0)
        }
    };
    commit_of(2, Some("4f2a9c1")).await;
    let (status, rolled_back) = w.rollback(&service_id, 2).await;
    assert_eq!(status, 200, "{rolled_back}");
    assert_eq!(rolled_back["new_version"], 5);
    assert_eq!(rolled_back["native_hash"], sha256_hex(binary_a));
    assert_eq!(
        commit_of(5, None).await.as_deref(),
        Some("4f2a9c1"),
        "a native rollback carries its target's commit, as any other does"
    );
    assert_eq!(
        w.artifacts(&service_id).await.last(),
        Some(&(
            5,
            Some(key_a.clone()),
            Some(sha256_hex(binary_a)),
            Some(binary_a.len() as i64)
        ))
    );
    assert_eq!(
        w.registrations().await,
        [(
            "test.native".to_string(),
            "native".to_string(),
            json!(["*"]),
            json!(service_id)
        )],
        "the rollback restored what version 2 provides"
    );
    let (subject, event) = heard.next().await;
    assert_eq!(subject, format!("deploy.ext.{}.{NATIVE}", w.tenant.slug));
    assert_eq!(event["version"], 5);
    assert_eq!(event["s3_key"], json!(key_a));
    assert_eq!(event["extensions"], json!(["test.native"]));

    let (status, web) = w
        .platform
        .api_get(&w.tenant, &format!("/api/services/{service_id}/web/status"))
        .await;
    assert_eq!(status, 200, "{web}");
    assert_eq!(web["artifact_uploaded"], false, "{web}");
    assert_eq!(web["artifact_s3_key"], Value::Null);
    let replaced = w
        .platform
        .http
        .post(format!(
            "{}/api/services/{service_id}/web",
            w.platform.base_url
        ))
        .header("authorization", w.deployer())
        .body(b"a web bundle".to_vec())
        .send()
        .await
        .expect("web upload");
    assert_eq!(
        replaced.status().as_u16(),
        409,
        "a web bundle never replaces a binary"
    );
    let (status, refused) = w
        .platform
        .send(
            Method::POST,
            &format!("/api/services/{service_id}/wasm"),
            &w.deployer(),
            Some(&json!({"wasm_base64": encoded(&wasm)})),
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(w.artifacts(&service_id).await.len(), 4);

    let delete_version = |version: i32| {
        let (platform, deployer, id) = (&w.platform, w.deployer(), service_id.clone());
        async move {
            platform
                .send(
                    Method::DELETE,
                    &format!("/api/services/{id}/deployments/{version}"),
                    &deployer,
                    None,
                )
                .await
        }
    };
    let (status, deleted) = delete_version(3).await;
    assert_eq!(status, 200, "{deleted}");
    assert!(w.stored(&key_b).await, "version 4 still records the binary");
    let (status, deleted) = delete_version(4).await;
    assert_eq!(status, 200, "{deleted}");
    assert!(
        !w.stored(&key_b).await,
        "no version records the binary any more"
    );
    assert!(w.stored(&key_a).await, "versions 2 and 5 record this one");
    assert_eq!(w.artifacts(&service_id).await.len(), 2);

    let undeployed = (
        format!("undeploy.ext.{}.{NATIVE}", w.tenant.slug),
        json!({"service_id": service_id, "service_name": NATIVE, "tenant_slug": w.tenant.slug}),
    );
    let status_path = format!("/api/services/{service_id}/status");
    let (status, switched) = w
        .as_deployer(
            &w.platform,
            Method::PATCH,
            &status_path,
            &json!({"status": "inactive"}),
        )
        .await;
    assert_eq!(status, 200, "{switched}");
    assert_eq!(heard.next().await, undeployed);
    assert_eq!(
        w.rows_of(&service_id).await,
        1,
        "an inactive service keeps what it provides"
    );
    assert_eq!(w.call("test.native").await["error"], "unavailable");
    let (status, switched) = w
        .as_deployer(
            &w.platform,
            Method::PATCH,
            &status_path,
            &json!({"status": "active"}),
        )
        .await;
    assert_eq!(status, 200, "{switched}");
    let (subject, event) = heard.next().await;
    assert_eq!(subject, format!("deploy.ext.{}.{NATIVE}", w.tenant.slug));
    assert_eq!(event["version"], 5);
    assert_eq!(event["s3_key"], json!(key_a));

    let stray = format!("native/{service_id}/left-by-a-lost-deploy");
    w.storage
        .upload(&stray, b"stray".to_vec(), "application/octet-stream")
        .await
        .expect("a stray object");
    let service_path = format!("/api/services/{service_id}");
    let (status, refused) = w
        .as_deployer(&w.platform, Method::DELETE, &service_path, &Value::Null)
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        refused["error"],
        format!(
            "service 'caller-svc' is deployed calling extension 'test.native', which '{NATIVE}' \
             provides; redeploy it without the extension first, or pass force=true"
        )
    );
    assert_eq!(w.rows_of(&service_id).await, 1, "a refused delete keeps it");
    let (status, deleted) = w
        .as_deployer(
            &w.platform,
            Method::DELETE,
            &format!("{service_path}?force=true"),
            &Value::Null,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert_eq!(heard.next().await, undeployed);
    heard.nothing_more().await;
    for key in [&key_a, &key_b, &stray] {
        assert!(!w.stored(key).await, "{key} is removed with the service");
    }
    assert_eq!(w.rows_of(&service_id).await, 0);
    assert_eq!(w.registrations().await, []);
    assert_eq!(w.call("test.native").await["error"], "not_found");
    let audited: Vec<Value> =
        w.db.pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT details FROM audit_log
                 WHERE tenant_id = $1 AND action = 'service.delete' AND resource_id = $2",
                &[&w.tenant.id, &service_id],
            )
            .await
            .expect("audit log")
            .iter()
            .map(|row| row.get(0))
            .collect();
    assert_eq!(audited.len(), 1, "{audited:?}");
    assert_eq!(audited[0]["force"], true);
    assert_eq!(audited[0]["mode"], "native");
}

#[tokio::test]
async fn a_native_deploy_is_refused_what_it_may_not_do() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let manifest = native_manifest(&[("test.native", &["*"])]);

    let wasm = common::wasm_manifest("plain", &[]);
    for (body, says) in [
        (
            json!({"manifest": manifest}),
            "mode: native requires the service's binary in native_base64",
        ),
        (
            json!({"manifest": wasm, "native_base64": encoded(b"bin")}),
            "native_base64 is accepted only with mode: native",
        ),
        (
            json!({"manifest": manifest, "native_base64": "not base64!"}),
            "Invalid native_base64",
        ),
        (
            json!({"manifest": manifest, "native_base64": encoded(&[0; MAX_BYTES + 1])}),
            "NATIVE_ARTIFACT_MAX_BYTES",
        ),
        (
            json!({"manifest": manifest, "native_base64": encoded(b"bin"), "wasm_base64": encoded(b"wasm")}),
            "carries no WASM module",
        ),
    ] {
        let (status, refused) = w
            .as_deployer(&w.platform, Method::POST, "/api/services/deploy", &body)
            .await;
        assert_eq!(status, 400, "{says}: {refused}");
        assert!(
            refused["error"].as_str().unwrap_or_default().contains(says),
            "{says}: {refused}"
        );
    }

    let (status, registered) = w
        .platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/extensions/test.nats", w.tenant.slug),
            &w.admin(),
            Some(&json!({"callers": ["*"]})),
        )
        .await;
    assert_eq!(status, 200, "{registered}");
    let (status, refused) = w
        .deploy_native(
            &native_manifest(&[("test.native", &["*"]), ("test.nats", &["*"])]),
            b"bin",
        )
        .await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        refused["error"],
        "provides: 'test.nats' is registered for this tenant as a NATS provider"
    );

    let (status, deployed) = w.deploy_native(&manifest, b"bin").await;
    assert_eq!(status, 200, "{deployed}");
    let mut other = manifest.clone();
    other["name"] = json!("ext-other");
    let (status, refused) = w.deploy_native(&other, b"other").await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        refused["error"],
        "provides: 'test.native' is provided by another native service of this tenant"
    );
    let (status, refused) = w
        .platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/extensions/test.native", w.tenant.slug),
            &w.admin(),
            Some(&json!({"callers": ["*"]})),
        )
        .await;
    assert_eq!(
        status, 409,
        "a NATS provider never takes a native name: {refused}"
    );

    let switched = common::wasm_manifest(NATIVE, &[]);
    let (status, refused) = w.platform.deploy(&w.tenant, &switched).await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        w.services().await,
        [NATIVE],
        "a refused deploy leaves no service behind"
    );
}

#[tokio::test]
async fn a_native_deploy_runs_beyond_its_request_and_cleans_up_when_it_fails() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let mut heard = Heard::listen(w.nats_url, &w.tenant).await;
    let manifest = native_manifest(&[("test.native", &["*"])]);
    let (status, begun) = w
        .deploy_native_on(&w.platform, "?async=true", &manifest, b"kept")
        .await;
    assert_eq!(status, 202, "{begun}");
    let job = w.ended(&w.platform, &begun).await;
    assert_eq!(job["status"], "succeeded", "{job}");
    assert_eq!(job["result"]["native_hash"], sha256_hex(b"kept"), "{job}");
    let service_id = job["result"]["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    let kept = object_key(&service_id, b"kept");
    assert!(w.stored(&kept).await);
    assert_eq!(
        w.registrations().await,
        [(
            "test.native".to_string(),
            "native".to_string(),
            json!(["*"]),
            json!(service_id)
        )]
    );
    let (subject, event) = heard.next().await;
    assert_eq!(subject, format!("deploy.ext.{}.{NATIVE}", w.tenant.slug));
    assert_eq!(event["artifact_hash"], sha256_hex(b"kept"));

    let failing = w.beside(&[("CUFFLINK_TEST_FAIL_AFTER_SCHEMA", "1")]).await;
    let (status, begun) = w
        .deploy_native_on(&failing, "?async=true", &manifest, b"fresh")
        .await;
    assert_eq!(status, 202, "{begun}");
    let job = w.ended(&failing, &begun).await;
    assert_eq!(job["status"], "failed", "{job}");
    assert!(
        !w.stored(&object_key(&service_id, b"fresh")).await,
        "the binary the failed deploy stored is removed"
    );
    assert!(w.stored(&kept).await);
    assert_eq!(w.artifacts(&service_id).await.len(), 1);
    assert_eq!(w.rows_of(&service_id).await, 1);
    heard.nothing_more().await;
}

#[tokio::test]
async fn a_native_plan_says_what_a_deploy_would_do_and_does_nothing() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let mut heard = Heard::listen(w.nats_url, &w.tenant).await;
    let manifest = native_manifest(&[("test.native", &["*"])]);
    let (status, planned) = w
        .deploy_native_on(&w.platform, "?plan=true", &manifest, b"one")
        .await;
    assert_eq!(status, 200, "{planned}");
    assert_eq!(planned["new"], true, "{planned}");
    assert_eq!(planned["skipped"], false);
    assert_eq!(planned["native_hash"], sha256_hex(b"one"));
    assert_eq!(planned["service_id"], Value::Null);
    assert!(w.services().await.is_empty(), "a plan makes no service");
    assert_eq!(w.registrations().await, []);

    let (status, deployed) = w.deploy_native(&manifest, b"one").await;
    assert_eq!(status, 200, "{deployed}");
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    heard.next().await;
    for (binary, skipped) in [(b"one".as_slice(), true), (b"two".as_slice(), false)] {
        let (status, planned) = w
            .deploy_native_on(&w.platform, "?plan=true", &manifest, binary)
            .await;
        assert_eq!(status, 200, "{planned}");
        assert_eq!(planned["new"], false, "{planned}");
        assert_eq!(planned["skipped"], skipped, "{planned}");
        assert_eq!(planned["service_id"], json!(service_id));
        assert_eq!(planned["native_hash"], sha256_hex(binary));
    }
    assert!(
        !w.stored(&object_key(&service_id, b"two")).await,
        "a plan stores no binary"
    );
    assert_eq!(w.artifacts(&service_id).await.len(), 1);
    heard.nothing_more().await;
}

#[tokio::test]
async fn native_services_are_refused_where_extensions_are_not_allowed() {
    let Some(w) = World::start(&[]).await else {
        return;
    };
    let (status, refused) = w
        .deploy_native(&native_manifest(&[("test.native", &["*"])]), b"bin")
        .await;
    assert_eq!(status, 403, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .unwrap_or_default()
            .contains("CUFFLINK_EXTENSIONS=false"),
        "{refused}"
    );
    let (status, version) = w.platform.get_json("/api/version").await.expect("version");
    assert_eq!(status, 200);
    assert!(
        version["supports"]
            .as_array()
            .expect("supports")
            .contains(&json!("native_services")),
        "{version}"
    );
}

#[tokio::test]
async fn a_failed_native_deploy_removes_only_the_binary_it_stored() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let manifest = native_manifest(&[("test.native", &["*"])]);
    let (status, deployed) = w.deploy_native(&manifest, b"kept").await;
    assert_eq!(status, 200, "{deployed}");
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    let (kept, fresh) = (
        object_key(&service_id, b"kept"),
        object_key(&service_id, b"fresh"),
    );

    let failing = w.beside(&[("CUFFLINK_TEST_FAIL_AFTER_SCHEMA", "1")]).await;
    let (status, failed) = w.deploy_native_on(&failing, "", &manifest, b"fresh").await;
    assert_eq!(status, 500, "{failed}");
    assert!(
        !w.stored(&fresh).await,
        "the binary the failed deploy stored is removed"
    );
    let wider = native_manifest(&[("test.native", &["*"]), ("test.more", &["*"])]);
    let (status, failed) = w.deploy_native_on(&failing, "", &wider, b"kept").await;
    assert_eq!(status, 500, "{failed}");
    assert!(
        w.stored(&kept).await,
        "a binary a version records is never removed"
    );
    assert_eq!(w.artifacts(&service_id).await.len(), 1);
    assert_eq!(w.rows_of(&service_id).await, 1);
}

#[tokio::test]
async fn deleting_a_tenant_undeploys_its_native_services() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let mut heard = Heard::listen(w.nats_url, &w.tenant).await;
    let (status, deployed) = w
        .deploy_native(&native_manifest(&[("test.native", &["*"])]), b"one")
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (status, _) = w
        .deploy_native(&native_manifest(&[("test.native", &["*"])]), b"two")
        .await;
    assert_eq!(status, 200);
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    let keys = [
        object_key(&service_id, b"one"),
        object_key(&service_id, b"two"),
    ];
    for _ in 0..2 {
        heard.next().await;
    }

    let (status, deleted) = w
        .platform
        .send(
            Method::DELETE,
            &format!("/api/tenants/{}", w.tenant.slug),
            &w.admin(),
            None,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert_eq!(deleted["native_services_undeployed"], 1);
    assert_eq!(deleted["web_services_undeployed"], 0);
    assert_eq!(deleted["web_artifacts_removed"], 0);
    assert_eq!(
        heard.next().await,
        (
            format!("undeploy.ext.{}.{NATIVE}", w.tenant.slug),
            json!({"service_id": service_id, "service_name": NATIVE, "tenant_slug": w.tenant.slug})
        )
    );
    heard.nothing_more().await;
    for key in &keys {
        assert!(!w.stored(key).await, "{key} is removed with the tenant");
    }
    assert_eq!(w.rows_of(&service_id).await, 0);
}

#[tokio::test]
async fn a_deployer_removes_or_switches_a_native_service_where_extensions_are_off() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let manifest = native_manifest(&[("test.native", &["*"])]);
    let (status, deployed) = w.deploy_native(&manifest, b"one").await;
    assert_eq!(status, 200, "{deployed}");
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    let (status, second) = w.deploy_native(&manifest, b"one and a half").await;
    assert_eq!(status, 200, "{second}");
    let off = w.beside(&[("CUFFLINK_EXTENSIONS", "false")]).await;
    let status_path = format!("/api/services/{service_id}/status");

    for switched in ["inactive", "active"] {
        let (status, answer) = w
            .as_deployer(
                &off,
                Method::PATCH,
                &status_path,
                &json!({"status": switched}),
            )
            .await;
        assert_eq!(status, 200, "{answer}");
    }
    let (status, refused) = w.deploy_native_on(&off, "", &manifest, b"two").await;
    assert_eq!(status, 403, "a deploy still needs extensions on: {refused}");
    let (status, refused) = w
        .as_deployer(
            &off,
            Method::POST,
            &format!("/api/services/{service_id}/rollback"),
            &json!({"target_version": deployed["version"]}),
        )
        .await;
    assert_eq!(
        status, 403,
        "a rollback still needs extensions on: {refused}"
    );
    assert!(
        refused["error"]
            .as_str()
            .unwrap_or_default()
            .contains("CUFFLINK_EXTENSIONS=false"),
        "{refused}"
    );

    let (status, deleted) = w
        .as_deployer(
            &off,
            Method::DELETE,
            &format!("/api/services/{service_id}"),
            &Value::Null,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert!(w.services().await.is_empty());
    for binary in [b"one".as_slice(), b"one and a half"] {
        assert!(!w.stored(&object_key(&service_id, binary)).await);
    }
    assert_eq!(w.rows_of(&service_id).await, 0);
}

#[tokio::test]
async fn deleting_a_service_with_no_current_version_removes_the_binaries_stored_under_it() {
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let failing = w.beside(&[("CUFFLINK_TEST_FAIL_AFTER_SCHEMA", "1")]).await;
    let (status, failed) = w
        .deploy_native_on(
            &failing,
            "",
            &native_manifest(&[("test.native", &["*"])]),
            b"first",
        )
        .await;
    assert_eq!(status, 500, "{failed}");
    let service_id = w
        .service_id(NATIVE)
        .await
        .expect("a failed first deploy leaves its service");
    assert_eq!(w.artifacts(&service_id).await, []);
    let stray = format!("native/{service_id}/{}", sha256_hex(b"first"));
    w.storage
        .upload(&stray, b"first".to_vec(), "application/octet-stream")
        .await
        .expect("a binary its cleanup missed");

    let (status, deleted) = w
        .platform
        .send(
            Method::DELETE,
            &format!("/api/services/{service_id}"),
            &w.deployer(),
            None,
        )
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert!(
        !w.stored(&stray).await,
        "nothing is left under native/{service_id}/"
    );
}
