use cufflink_testkit as common;

use cufflink_types::ServiceManifest;
use serde_json::{json, Value};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn manifest() -> Value {
    let mut manifest = common::wasm_manifest("catalog-service", &[]);
    manifest["routing"] = json!("strict");
    manifest["openapi"] = json!({
        "title": "Catalog",
        "docs": [{
            "name": "public-api",
            "mount": "/docs/public-api",
            "include_tags": ["Catalog"],
            "info": {"title": "Catalog Public API", "version": "1.0.0"},
        }],
    });
    manifest["custom_routes"] = json!([{
        "method": "GET",
        "path": "/orders",
        "handler": "echo",
        "posture": {"kind": "public", "reason": "integration test"},
        "doc": {"tags": ["Catalog"]},
    }]);
    manifest
}

struct Sources {
    root: tempfile::TempDir,
}

impl Sources {
    fn new(project: Option<String>) -> Self {
        let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("directory");
        let dir = root.path();
        std::fs::write(dir.join("manifest.json"), manifest().to_string()).expect("manifest");
        let tools = dir.join("tools");
        std::fs::create_dir_all(&tools).expect("directory");
        let cargo = tools.join("cargo");
        std::fs::write(
            &cargo,
            format!(
                "#!/bin/sh\n[ \"$*\" = \"run -- --emit-manifest\" ] && exec cat \"{}\"\nexit 1\n",
                dir.join("manifest.json").display()
            ),
        )
        .expect("cargo");
        std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).expect("mode");
        if let Some(project) = project {
            std::fs::write(dir.join("Cufflink.toml"), project).expect("Cufflink.toml");
        }
        Self { root }
    }

    fn dir(&self) -> &Path {
        self.root.path()
    }

    fn openapi(&self, arguments: &[&str]) -> Output {
        let path = format!(
            "{}:{}",
            self.dir().join("tools").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        Command::new(env!("CARGO_BIN_EXE_cufflink"))
            .arg("openapi")
            .args(arguments)
            .current_dir(self.dir())
            .env("PATH", path)
            .env("HOME", self.dir())
            .env_remove("CUFFLINK_API_KEY")
            .output()
            .expect("the cufflink binary runs")
    }
}

fn printed(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("json")
}

#[test]
fn a_local_public_document_is_the_one_the_platform_builds() {
    let sources = Sources::new(None);
    let manifest: ServiceManifest = serde_json::from_value(manifest()).expect("manifest");
    assert_eq!(
        printed(&sources.openapi(&["--local", "--doc", "public-api"])),
        cufflink_types::openapi::public_doc_spec(&manifest, "local", "public-api")
            .expect("document")
    );
    assert_eq!(
        printed(&sources.openapi(&["--local", "--doc", "public-api", "--tenant-slug", "acme"])),
        cufflink_types::openapi::public_doc_spec(&manifest, "acme", "public-api")
            .expect("document")
    );

    let unknown = sources.openapi(&["--local", "--doc", "status-api"]);
    assert!(!unknown.status.success());
    assert!(
        String::from_utf8_lossy(&unknown.stderr)
            .contains("no public document 'status-api': the service declares public-api"),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );
}

#[tokio::test]
async fn a_deployed_public_document_is_fetched_without_a_token() {
    if common::require_binary("cufflink-platform").is_none() {
        return;
    }
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = common::PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let (status, body) = platform.deploy(&tenant, &manifest()).await;
    assert_eq!(status, 200, "deploy failed: {body}");

    let sources = Sources::new(Some(format!(
        "[service]\ndefault_env = \"test\"\n\n[environments.test]\napi_url = \"{}\"\n\
         tenant = \"{}\"\n",
        platform.base_url, tenant.slug
    )));
    let served = platform
        .http
        .get(format!(
            "{}/svc/{}/catalog-service/docs/public-api/openapi.json",
            platform.base_url, tenant.slug
        ))
        .send()
        .await
        .expect("request")
        .json::<Value>()
        .await
        .expect("json");
    assert_eq!(printed(&sources.openapi(&["--doc", "public-api"])), served);
    let output = sources.dir().join("public-api.json");
    let saved = sources.openapi(&[
        "--doc",
        "public-api",
        "--output",
        output.to_str().expect("path"),
    ]);
    assert!(
        saved.status.success(),
        "{}",
        String::from_utf8_lossy(&saved.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&std::fs::read(&output).expect("saved")).expect("json"),
        served
    );
}
