use cufflink_testkit as common;

use common::{ChildProcess, PlatformProcess, Tenant, TestDb};
use reqwest::Method;
use serde_json::{json, Value};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const BASE_DOMAIN: &str = "apps.test";
const SERVICE: &str = "site";
const TOKEN: &str = "ops-token";

async fn eventually<T, F: Future<Output = Option<T>>>(
    what: &str,
    timeout: Duration,
    mut probe: impl FnMut() -> F,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(found) = probe().await {
            return found;
        }
        assert!(Instant::now() < deadline, "{what} within {timeout:?}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn node_available() -> bool {
    let available = std::process::Command::new("node")
        .arg("--version")
        .output()
        .is_ok();
    assert!(
        available || std::env::var_os("CUFFLINK_CI").is_none(),
        "node is required under CUFFLINK_CI"
    );
    if !available {
        common::skip("node is not installed");
    }
    available
}

fn bundle(marker: &str) -> Vec<u8> {
    let next = format!(
        r#"const [, , , , port, , host] = process.argv;
require("http").createServer((req, res) => {{
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify({{ marker: "{marker}", env: process.env }}));
}}).listen(Number(port), host);
"#
    );
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    for (path, contents) in [
        (".next/BUILD_ID", marker),
        ("node_modules/next/package.json", "{}"),
        ("node_modules/next/dist/bin/next", next.as_str()),
    ] {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, path, contents.as_bytes())
            .expect("append");
    }
    builder
        .into_inner()
        .and_then(|gz| gz.finish())
        .expect("finish tarball")
}

struct World {
    platform: PlatformProcess,
    nats_url: &'static str,
    admin: String,
    a: Tenant,
    b: Tenant,
    key_file: PathBuf,
    apps: TempDir,
    _keys: TempDir,
    _storage: TempDir,
    _db: TestDb,
}

impl World {
    async fn start() -> Option<Self> {
        if !node_available() {
            return None;
        }
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let storage = tempfile::tempdir().expect("storage");
        let storage_dir = storage.path().to_string_lossy().to_string();
        let platform = PlatformProcess::start(
            &db.url,
            Some(nats_url),
            &[
                ("STORAGE_BACKEND", "local"),
                ("STORAGE_LOCAL_DIR", storage_dir.as_str()),
                ("S3_BUCKET", "cufflink"),
            ],
        )
        .await;
        platform.wait_ready().await;
        let (a, b) = (db.mint_tenant().await, db.mint_tenant().await);
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let keys = tempfile::tempdir().expect("keys");
        Some(Self {
            key_file: keys.path().join("runner-key"),
            platform,
            nats_url,
            admin,
            a,
            b,
            apps: tempfile::tempdir().expect("apps"),
            _keys: keys,
            _storage: storage,
            _db: db,
        })
    }

    async fn deploy(&self, tenant: &Tenant, bundle: Vec<u8>) -> String {
        let (status, deployed) = self
            .platform
            .send(
                Method::POST,
                "/api/services/deploy",
                &format!("ApiKey {}", tenant.api_key),
                Some(&json!({
                    "manifest": {"name": SERVICE, "version": "0.1.0", "mode": "web", "tables": []},
                    "api_version": 1,
                })),
            )
            .await;
        assert_eq!(status, 200, "{deployed}");
        let service = deployed["service_id"]
            .as_str()
            .expect("a service id")
            .to_string();
        let uploaded = self
            .platform
            .http
            .post(format!(
                "{}/api/services/{service}/web",
                self.platform.base_url
            ))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .body(bundle)
            .send()
            .await
            .expect("an upload");
        assert_eq!(uploaded.status().as_u16(), 200);
        service
    }

    async fn rotate_key(&self) -> String {
        let (status, minted) = self
            .platform
            .send(
                Method::POST,
                &format!("/api/tenants/{}/runner-keys", self.a.slug),
                &self.admin,
                Some(&json!({"name": "web-runner", "runs": "web"})),
            )
            .await;
        assert_eq!(status, 201, "{minted}");
        let written = self.key_file.with_extension("new");
        std::fs::write(&written, minted["key"].as_str().expect("a key")).expect("the new key");
        std::fs::rename(&written, &self.key_file).expect("the key file");
        minted["runner_key"]["id"]
            .as_str()
            .expect("an id")
            .to_string()
    }

    async fn revoke(&self, id: &str) {
        let (status, revoked) = self
            .platform
            .send(
                Method::DELETE,
                &format!("/api/tenants/{}/runner-keys/{id}", self.a.slug),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{revoked}");
    }

    async fn runner(&self) -> ChildProcess {
        let (apps, key_file) = (
            self.apps.path().to_string_lossy().to_string(),
            self.key_file.to_string_lossy().to_string(),
        );
        ChildProcess::spawn(
            Path::new(env!("CARGO_BIN_EXE_cufflink-web-runner")),
            "RUNNER_PORT",
            &[
                ("WEB_TENANT", self.a.slug.as_str()),
                ("PLATFORM_URL", self.platform.base_url.as_str()),
                ("CUFFLINK_RUNNER_KEY_FILE", key_file.as_str()),
                ("NATS_URL", self.nats_url),
                ("CUFFLINK_BASE_DOMAIN", BASE_DOMAIN),
                ("CUFFLINK_PUBLIC_SCHEME", "http"),
                ("APPS_DIR", apps.as_str()),
                ("WEB_PORT_RANGE", "21020-21030"),
                ("WEB_POLL_SECS", "1"),
                ("WEB_STATUS_TOKEN", TOKEN),
                ("RUST_LOG", "info,cufflink_web_runner=debug"),
            ],
        )
        .await
    }
}

async fn visit(runner: &ChildProcess, tenant: &Tenant) -> (u16, Value) {
    let response = runner
        .http
        .get(format!("{}/", runner.base_url))
        .header("host", format!("{SERVICE}--{}.{BASE_DOMAIN}", tenant.slug))
        .send()
        .await
        .expect("a request");
    let status = response.status().as_u16();
    (status, response.json().await.unwrap_or(Value::Null))
}

async fn running(runner: &ChildProcess) -> Vec<(String, String)> {
    let status: Value = runner
        .http
        .get(format!("{}/_cufflink/status", runner.base_url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("a status request")
        .json()
        .await
        .expect("status");
    status["apps"]
        .as_array()
        .expect("apps")
        .iter()
        .map(|app| {
            (
                app["tenant"].as_str().unwrap_or_default().to_string(),
                app["service"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

async fn served_marker(runner: &ChildProcess, tenant: &Tenant, marker: &str) -> Value {
    eventually(
        &format!("{} serving {marker}", tenant.slug),
        Duration::from_secs(60),
        || async move {
            let (status, body) = visit(runner, tenant).await;
            (status == 200 && body["marker"] == marker).then_some(body)
        },
    )
    .await
}

async fn until_ready(runner: &ChildProcess, expected: u16) {
    eventually(
        &format!("/_cufflink/ready answering {expected}"),
        Duration::from_secs(30),
        || async move { (runner.status("/_cufflink/ready").await == Some(expected)).then_some(()) },
    )
    .await;
}

#[tokio::test]
async fn a_tenant_scoped_runner_serves_its_tenant_from_the_runner_api_alone() {
    let Some(w) = World::start().await else {
        return;
    };
    let site_a = w.deploy(&w.a, bundle("a-1")).await;
    let site_b = w.deploy(&w.b, bundle("b-1")).await;
    let (status, body) = w
        .platform
        .send(
            Method::PUT,
            &format!("/api/services/{site_a}/config"),
            &format!("ApiKey {}", w.a.api_key),
            Some(&json!({"key": "SITE_SECRET", "value": "only-a", "is_secret": true})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let first_key = w.rotate_key().await;
    let mut runner = w.runner().await;
    until_ready(&runner, 200).await;

    let served = served_marker(&runner, &w.a, "a-1").await;
    let env = &served["env"];
    assert_eq!(
        env["SITE_SECRET"], "only-a",
        "its configs, from the runner API"
    );
    assert_eq!(env["CUFFLINK_TENANT"], json!(w.a.slug));
    assert!(
        env.get("CUFFLINK_RUNNER_KEY_FILE").is_none(),
        "the key never reaches an app: {env}"
    );
    let (status, _) = visit(&runner, &w.b).await;
    assert_eq!(
        status, 404,
        "another tenant's host is none of this runner's"
    );

    w.deploy(&w.a, bundle("a-2")).await;
    served_marker(&runner, &w.a, "a-2").await;

    let nats = async_nats::connect(w.nats_url).await.expect("nats");
    for (subject, forged) in [
        (
            format!("deploy.web.{}.{SERVICE}", w.a.slug),
            json!({"tenant_slug": w.b.slug, "service_name": SERVICE, "version": 1,
                   "service_id": site_b, "s3_key": format!("web/{site_b}/v1.tar.gz")}),
        ),
        (
            format!("deploy.web.{}.{SERVICE}", w.a.slug),
            json!({"tenant_slug": w.a.slug, "service_name": SERVICE, "version": 1,
                   "service_id": site_b, "s3_key": format!("web/{site_b}/v1.tar.gz")}),
        ),
        (
            format!("deploy.web.{}.{SERVICE}", w.b.slug),
            json!({"tenant_slug": w.b.slug, "service_name": SERVICE, "version": 1,
                   "service_id": site_b, "s3_key": format!("web/{site_b}/v1.tar.gz")}),
        ),
        (
            format!("undeploy.web.{}.{SERVICE}", w.a.slug),
            json!({"tenant_slug": w.b.slug, "service_name": SERVICE}),
        ),
    ] {
        nats.publish(subject, forged.to_string().into())
            .await
            .expect("publish");
    }
    nats.flush().await.expect("flush");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (status, body) = visit(&runner, &w.a).await;
    assert_eq!(
        (status, &body["marker"]),
        (200, &json!("a-2")),
        "an event names what changed, never what runs: {body}"
    );
    assert_eq!(
        running(&runner).await,
        [(w.a.slug.clone(), SERVICE.to_string())]
    );

    w.revoke(&first_key).await;
    until_ready(&runner, 503).await;
    let tenant_dir = w.apps.path().join(&w.a.slug);
    eventually(
        "every app of the tenant stopped, and nothing of it kept to serve from",
        Duration::from_secs(30),
        || {
            let (runner, tenant_dir) = (&runner, &tenant_dir);
            async move { (running(runner).await.is_empty() && !tenant_dir.exists()).then_some(()) }
        },
    )
    .await;
    let (status, body) = visit(&runner, &w.a).await;
    assert_eq!(status, 503, "nothing is served on a refused key: {body}");

    w.rotate_key().await;
    until_ready(&runner, 200).await;
    served_marker(&runner, &w.a, "a-2").await;

    runner.sigterm();
    assert!(
        runner
            .wait_exit(Duration::from_secs(30))
            .await
            .is_some_and(|status| status.success()),
        "the runner stops cleanly; its output:\n{}",
        runner.output_tail()
    );
}
