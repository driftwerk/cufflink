use cufflink_testkit as common;

use common::{ChildProcess, Tenant, TestDb};
use cufflink_storage::ObjectStorage;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

const BASE_DOMAIN: &str = "apps.test";
const SERVICE: &str = "site";
const BUCKET: &str = "cufflink";

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
  const wait = Number(new URL(req.url, "http://app").searchParams.get("wait") ?? 0);
  setTimeout(() => res.end("{marker}"), wait);
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
    db: TestDb,
    tenant: Tenant,
    service_id: Uuid,
    storage: Arc<dyn ObjectStorage>,
    storage_dir: TempDir,
    apps: TempDir,
    nats_url: &'static str,
}

impl World {
    async fn start() -> Option<Self> {
        if !node_available() {
            return None;
        }
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_migrated_database().await;
        let tenant = db.mint_tenant().await;
        let service_id: Uuid = db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "INSERT INTO services (tenant_id, name) VALUES ($1, $2) RETURNING id",
                &[&tenant.id, &SERVICE],
            )
            .await
            .expect("service")
            .get(0);
        let storage_dir = tempfile::tempdir().expect("storage");
        let storage = cufflink_storage::local_dir(storage_dir.path(), BUCKET).expect("storage");
        Some(Self {
            db,
            tenant,
            service_id,
            storage,
            storage_dir,
            apps: tempfile::tempdir().expect("apps"),
            nats_url,
        })
    }

    async fn release(&self, version: i32, bundle: Vec<u8>) {
        let key = format!("web/{}/v{version}.tar.gz", self.service_id);
        let (sha256, size) = (hex::encode(Sha256::digest(&bundle)), bundle.len() as i64);
        self.storage
            .upload(&key, bundle, "application/gzip")
            .await
            .expect("upload");
        let client = self.db.pool().get().await.expect("checkout");
        client
            .execute(
                "INSERT INTO deployments (service_id, version, manifest_hash, manifest,
                                          artifact_s3_key, artifact_hash, artifact_size)
                 VALUES ($1, $2, 'm', $3, $4, $5, $6)",
                &[
                    &self.service_id,
                    &version,
                    &json!({"name": SERVICE, "mode": "web", "tables": []}),
                    &key,
                    &sha256,
                    &size,
                ],
            )
            .await
            .expect("deployment");
        client
            .execute(
                "UPDATE services SET current_version = $2 WHERE id = $1",
                &[&self.service_id, &version],
            )
            .await
            .expect("current version");
    }

    async fn announce(&self, version: i32) {
        let nats = async_nats::connect(self.nats_url).await.expect("nats");
        nats.publish(
            format!("deploy.web.{}.{SERVICE}", self.tenant.slug),
            json!({"tenant_slug": self.tenant.slug, "service_name": SERVICE,
                   "service_id": self.service_id, "version": version})
            .to_string()
            .into(),
        )
        .await
        .expect("publish");
        nats.flush().await.expect("flush");
    }

    async fn runner(&self) -> ChildProcess {
        let (storage_dir, apps_dir) = (
            self.storage_dir.path().to_string_lossy().to_string(),
            self.apps.path().to_string_lossy().to_string(),
        );
        ChildProcess::spawn(
            Path::new(env!("CARGO_BIN_EXE_cufflink-web-runner")),
            "RUNNER_PORT",
            &[
                ("DATABASE_URL", self.db.url.as_str()),
                ("NATS_URL", self.nats_url),
                ("CUFFLINK_BASE_DOMAIN", BASE_DOMAIN),
                ("CUFFLINK_PUBLIC_SCHEME", "http"),
                ("STORAGE_BACKEND", "local"),
                ("STORAGE_LOCAL_DIR", storage_dir.as_str()),
                ("S3_BUCKET", BUCKET),
                ("APPS_DIR", apps_dir.as_str()),
                ("WEB_PORT_RANGE", "21040-21050"),
                ("WEB_DRAIN_GRACE_SECS", "30"),
                ("RUST_LOG", "info,cufflink_web_runner=debug"),
            ],
        )
        .await
    }
}

async fn visit(runner: &ChildProcess, host: &str) -> (u16, String) {
    let response = runner
        .http
        .get(format!("{}/", runner.base_url))
        .header("host", host)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("a request");
    let status = response.status().as_u16();
    (status, response.text().await.expect("a body"))
}

async fn serving(runner: &ChildProcess, host: &str, marker: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while visit(runner, host).await != (200, marker.to_string()) {
        assert!(
            Instant::now() < deadline,
            "{host} serving {marker}; the runner's output:\n{}",
            runner.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_request_spanning_a_deploy_completes_and_the_next_reaches_the_new_version() {
    let Some(w) = World::start().await else {
        return;
    };
    w.release(1, bundle("one")).await;
    let mut runner = w.runner().await;
    let host = format!("{SERVICE}--{}.{BASE_DOMAIN}", w.tenant.slug);
    serving(&runner, &host, "one").await;

    let slow = {
        let (http, base_url, host) = (runner.http.clone(), runner.base_url.clone(), host.clone());
        tokio::spawn(async move {
            let response = http
                .get(format!("{base_url}/?wait=3000"))
                .header("host", host)
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .expect("the slow request");
            let status = response.status().as_u16();
            (status, response.text().await.expect("its body"))
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    w.release(2, bundle("two")).await;
    w.announce(2).await;
    serving(&runner, &host, "two").await;
    assert_eq!(
        slow.await.expect("joined"),
        (200, "one".to_string()),
        "the request in flight ends on the process it reached"
    );

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
