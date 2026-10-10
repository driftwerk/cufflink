use cufflink_testkit as common;

use common::{PlatformProcess, Tenant, TestDb};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const SERVICE: &str = "notes";
const COMMITS: [&str; 3] = [
    "1111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "2222222bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "3333333ccccccccccccccccccccccccccccccccc",
];

const MODULE_FEATURE: &str = "db_v2";

fn module(variant: u8) -> Vec<u8> {
    wat::parse_str(format!(
        r#"(module
             (@custom "cufflink.features" "{MODULE_FEATURE}\n")
             (memory (export "memory") 1)
             (func (export "alloc") (param i32) (result i32) i32.const 0)
             (func (export "echo") (param i32 i32) (result i32) i32.const {variant}))"#
    ))
    .expect("module")
}

fn executable(path: &Path, script: &str) {
    std::fs::write(path, script).expect("script");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("mode");
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn ok(self, what: &str) -> String {
        assert_eq!(self.code, Some(0), "{what}: {}{}", self.stdout, self.stderr);
        self.stdout
    }

    fn failed(self, what: &str) -> String {
        assert_eq!(self.code, Some(1), "{what}: {}{}", self.stdout, self.stderr);
        self.stderr
    }
}

fn contents(bundle: &Path) -> Value {
    let mut json = String::new();
    flate2::read::GzDecoder::new(std::fs::File::open(bundle).expect("bundle"))
        .read_to_string(&mut json)
        .expect("gzip");
    serde_json::from_str(&json).expect("json")
}

struct World {
    _db: TestDb,
    platform: PlatformProcess,
    tenant: Tenant,
    root: tempfile::TempDir,
}

impl World {
    async fn start() -> Option<Self> {
        common::require_binary("cufflink-platform")?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let root = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).expect("directory");
        let world = Self {
            _db: db,
            platform,
            tenant,
            root,
        };
        world.lay_out();
        Some(world)
    }

    fn sources(&self) -> PathBuf {
        self.root.path().join("sources")
    }

    fn elsewhere(&self) -> PathBuf {
        self.root.path().join("elsewhere")
    }

    fn web_app(&self) -> PathBuf {
        self.root.path().join("web-app")
    }

    fn built(&self) -> PathBuf {
        self.root
            .path()
            .join("built/wasm32-unknown-unknown/release/notes.wasm")
    }

    fn ran_a_build(&self) -> PathBuf {
        self.root.path().join("a-build-ran")
    }

    fn bundle(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    fn lay_out(&self) {
        let root = self.root.path();
        let project = format!(
            "[service]\ndefault_env = \"test\"\n\n[environments.test]\napi_url = \"{}\"\n\
             tenant = \"{}\"\n",
            self.platform.base_url, self.tenant.slug
        );
        for dir in [self.sources(), self.elsewhere()] {
            std::fs::create_dir_all(&dir).expect("directory");
            std::fs::write(dir.join("Cufflink.toml"), &project).expect("Cufflink.toml");
        }
        std::fs::create_dir_all(self.web_app()).expect("directory");
        std::fs::write(
            self.web_app().join("Cufflink.toml"),
            project.replace("[service]", "[service]\nname = \"site\"\nmode = \"web\""),
        )
        .expect("Cufflink.toml");
        std::fs::create_dir_all(self.built().parent().expect("parent")).expect("directory");
        let manifest = common::wasm_manifest(SERVICE, &[("echo", "/echo")]);
        std::fs::write(root.join("manifest.json"), manifest.to_string()).expect("manifest");

        let building = root.join("building");
        let failing = root.join("failing");
        for dir in [&building, &failing] {
            std::fs::create_dir_all(dir).expect("directory");
        }
        executable(
            &building.join("cargo"),
            &format!(
                r#"#!/bin/sh
case "$*" in
  "run -- --emit-manifest") cat "{root}/manifest.json" ;;
  "metadata --format-version 1 --no-deps")
    printf '{{"target_directory":"%s","packages":[{{"name":"notes","manifest_path":"%s/Cargo.toml","targets":[{{"name":"notes","crate_types":["cdylib"]}}]}}]}}' "{root}/built" "$(pwd -P)" ;;
  "metadata --format-version 1") echo '{{"packages":[]}}' ;;
  build*) ;;
  *) exit 1 ;;
esac
"#,
                root = root.display()
            ),
        );
        executable(
            &building.join("rustup"),
            "#!/bin/sh\necho wasm32-unknown-unknown\n",
        );
        let says_it_ran = format!(
            "#!/bin/sh\necho \"$0 $*\" >> \"{}\"\nexit 1\n",
            self.ran_a_build().display()
        );
        for tool in ["cargo", "rustup"] {
            executable(&failing.join(tool), &says_it_ran);
        }
    }

    fn run(&self, dir: &Path, tools: &str, arguments: &[&str]) -> Ran {
        let path = format!(
            "{}:{}",
            self.root.path().join(tools).display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let output = Command::new(env!("CARGO_BIN_EXE_cufflink"))
            .args(arguments)
            .current_dir(dir)
            .env("PATH", path)
            .env("HOME", self.root.path())
            .env("CUFFLINK_API_KEY", &self.tenant.api_key)
            .output()
            .expect("the cufflink binary runs");
        Ran {
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    fn in_sources(&self, variant: u8, arguments: &[&str]) -> Ran {
        std::fs::write(self.built(), module(variant)).expect("module");
        self.run(&self.sources(), "building", arguments)
    }

    fn without_sources(&self, arguments: &[&str]) -> Ran {
        self.run(&self.elsewhere(), "failing", arguments)
    }

    fn bundled(&self, name: &str, variant: u8, commit: &str) -> PathBuf {
        let bundle = self.bundle(name);
        let path = bundle.to_str().expect("path");
        self.in_sources(
            variant,
            &["build", "--bundle", path, "--commit-hash", commit],
        )
        .ok("build --bundle");
        bundle
    }

    fn deploy_bundle(&self, bundle: &Path, more: &[&str]) -> Ran {
        let arguments: Vec<&str> = ["deploy", "--bundle", bundle.to_str().expect("path")]
            .into_iter()
            .chain(more.iter().copied())
            .collect();
        self.without_sources(&arguments)
    }

    async fn service_id(&self) -> Option<String> {
        let (status, body) = self.platform.api_get(&self.tenant, "/api/services").await;
        assert_eq!(status, 200, "{body}");
        body["services"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|service| service["name"] == SERVICE)
            .and_then(|service| service["id"].as_str().map(str::to_string))
    }

    async fn versions(&self) -> Vec<(i64, Value, Value, Value)> {
        let id = self.service_id().await.expect("the service is deployed");
        let (status, body) = self
            .platform
            .api_get(&self.tenant, &format!("/api/services/{id}/deployments"))
            .await;
        assert_eq!(status, 200, "{body}");
        body["deployments"]
            .as_array()
            .expect("deployments")
            .iter()
            .map(|deployment| {
                (
                    deployment["version"].as_i64().expect("version"),
                    deployment["manifest_hash"].clone(),
                    deployment["wasm_hash"].clone(),
                    deployment["commit_hash"].clone(),
                )
            })
            .collect()
    }

    async fn running(&self) -> (Value, Value) {
        let url = format!(
            "{}/svc/{}/{SERVICE}/_meta/health",
            self.platform.base_url, self.tenant.slug
        );
        let response = self.platform.http.get(url).send().await.expect("health");
        assert_eq!(response.status().as_u16(), 200);
        let health: Value = response.json().await.expect("json");
        (health["version"].clone(), health["commit_hash"].clone())
    }

    fn no_build_ran(&self) {
        assert!(
            !self.ran_a_build().exists(),
            "a deploy of a bundle and a rollback by name build nothing: {}",
            std::fs::read_to_string(self.ran_a_build()).unwrap_or_default()
        );
    }
}

#[tokio::test]
async fn cf8_a_bundle_deploys_as_its_source_does() {
    let Some(world) = World::start().await else {
        return;
    };
    world
        .in_sources(1, &["deploy", "--commit-hash", COMMITS[0]])
        .ok("deploy from source");
    let from_source = world.versions().await;
    assert_eq!(from_source.len(), 1);
    let version = from_source[0].0;

    let bundle = world.bundle("notes.bundle");
    let built = world
        .in_sources(
            1,
            &[
                "build",
                "--bundle",
                bundle.to_str().expect("path"),
                "--commit-hash",
                COMMITS[0],
            ],
        )
        .ok("build --bundle");
    let bytes = std::fs::read(&bundle).expect("bundle");
    let named = format!("  SHA-256: {}", hex::encode(Sha256::digest(&bytes)));
    assert!(
        built.lines().any(|line| line == named),
        "a build prints the digest of the file it wrote, on stdout: {built}"
    );
    let again = world.bundled("again.bundle", 1, COMMITS[0]);
    assert_eq!(
        std::fs::read(&again).expect("bundle"),
        bytes,
        "one manifest, module and commit give one file"
    );
    let other_commit = world.bundled("other.bundle", 1, COMMITS[1]);
    assert_ne!(std::fs::read(&other_commit).expect("bundle"), bytes);
    let held = contents(&bundle);
    assert_eq!(
        (
            &held["manifest"]["required_platform_features"],
            &held["features"]
        ),
        (&json!([MODULE_FEATURE]), &json!([MODULE_FEATURE])),
        "the feature the module declares is folded into the manifest the bundle carries"
    );

    let deployed = world.deploy_bundle(&bundle, &[]).ok("deploy --bundle");

    assert!(
        deployed.contains(&format!(
            "No schema changes for notes v{version} — manifest unchanged"
        )),
        "the platform derives the hash it derived from the source deploy: {deployed}"
    );
    assert!(
        deployed.lines().any(|line| line == named),
        "a deploy says which file it deploys: {deployed}"
    );
    assert_eq!(
        world.versions().await,
        from_source,
        "the same manifest hash, the same module hash, and no new version"
    );
    world.no_build_ran();
}

#[tokio::test]
async fn cf8_a_bundle_deploys_without_sources() {
    let Some(world) = World::start().await else {
        return;
    };
    let bundle = world.bundled("notes.bundle", 1, COMMITS[0]);

    let planned = world
        .deploy_bundle(&bundle, &["--plan"])
        .ok("deploy --bundle --plan");
    assert!(planned.contains("Plan for notes"), "{planned}");
    assert_eq!(world.service_id().await, None, "a plan deploys nothing");

    let deployed = world.deploy_bundle(&bundle, &[]).ok("deploy --bundle");
    assert!(deployed.contains("Deployed notes v"), "{deployed}");
    assert!(
        deployed.contains("← bundle from cufflink v"),
        "what asks the platform for its features is the bundle: {deployed}"
    );
    let (version, commit) = world.running().await;
    assert_eq!(
        commit,
        json!(COMMITS[0]),
        "the commit the bundle was built at"
    );

    world
        .deploy_bundle(&bundle, &["--commit-hash", COMMITS[1]])
        .ok("deploy --bundle --commit-hash");
    assert_eq!(
        world.running().await,
        (version, json!(COMMITS[1])),
        "a commit on the command line is the one deployed, and here relabels the version"
    );

    let altered = world.bundle("altered.bundle");
    let bytes = std::fs::read(&bundle).expect("bundle");
    std::fs::write(&altered, &bytes[..bytes.len() / 2]).expect("file");
    let refused = world
        .deploy_bundle(&altered, &[])
        .failed("a truncated bundle");
    assert!(
        refused.contains(&format!(
            "Cannot deploy the bundle {}: it is not a whole gzip file",
            altered.display()
        )),
        "{refused}"
    );
    assert_eq!(world.versions().await.len(), 1);
    world.no_build_ran();
}

#[tokio::test]
async fn cf8_a_bundle_is_not_built_or_deployed_from_a_web_project() {
    let Some(world) = World::start().await else {
        return;
    };
    let bundle = world.bundled("notes.bundle", 1, COMMITS[0]);
    let never = world.bundle("never.bundle");

    let unbuilt = world
        .run(
            &world.web_app(),
            "failing",
            &["build", "--bundle", never.to_str().expect("path")],
        )
        .failed("build --bundle of a web app");
    assert!(
        unbuilt.contains("bundles carry WASM services; deploy web services from source"),
        "{unbuilt}"
    );
    assert!(!never.exists(), "a build that is refused leaves no file");

    let foreign = world
        .run(
            &world.web_app(),
            "failing",
            &["deploy", "--bundle", bundle.to_str().expect("path")],
        )
        .failed("deploy --bundle from another service's project");
    assert!(
        foreign.contains(&format!(
            "Cannot deploy the bundle {}: it is of notes, and the Cufflink.toml here is of site",
            bundle.display()
        )),
        "{foreign}"
    );
    assert_eq!(world.service_id().await, None, "nothing was sent");
    world.no_build_ran();
}

#[tokio::test]
async fn cf8_a_bundle_deploys_inactive() {
    let Some(world) = World::start().await else {
        return;
    };
    let bundle = world.bundled("notes.bundle", 1, COMMITS[0]);
    let status = || async {
        let (status, body) = world.platform.api_get(&world.tenant, "/api/services").await;
        assert_eq!(status, 200, "{body}");
        body["services"][0]["status"].clone()
    };

    let created = world
        .deploy_bundle(&bundle, &["--inactive"])
        .ok("deploy --bundle --inactive");
    assert!(
        created.contains("(inactive; start it with: cufflink services enable notes)"),
        "{created}"
    );
    assert_eq!(status().await, "inactive");

    let other = world.bundled("other.bundle", 2, COMMITS[1]);
    world
        .deploy_bundle(&other, &["--inactive"])
        .ok("deploy --bundle --inactive of an inactive service");
    assert_eq!(status().await, "inactive");
    assert_eq!(world.versions().await.len(), 2);
    world.no_build_ran();
}

#[tokio::test]
async fn cf8_rollback_by_commit() {
    let Some(world) = World::start().await else {
        return;
    };
    for (variant, commit) in (1u8..).zip(COMMITS) {
        let bundle = world.bundled("notes.bundle", variant, commit);
        world.deploy_bundle(&bundle, &[]).ok("deploy --bundle");
    }
    let versions: Vec<i64> = world
        .versions()
        .await
        .into_iter()
        .map(|(version, ..)| version)
        .collect();
    let (newest, oldest) = (versions[0], versions[2]);
    assert_eq!(world.running().await, (json!(newest), json!(COMMITS[2])));

    let rolled_back = world
        .without_sources(&[
            "rollback",
            "--commit",
            &COMMITS[0][..9].to_ascii_uppercase(),
            "--service",
            SERVICE,
        ])
        .ok("rollback --commit");

    assert!(
        rolled_back.contains(&format!(
            "Rolling back notes from v{newest} to v{oldest} (commit {})...",
            COMMITS[0]
        )),
        "{rolled_back}"
    );
    assert_eq!(
        world.running().await,
        (json!(newest + 1), json!(COMMITS[0])),
        "the version a rollback activates reports the commit it was rolled back to"
    );

    let again = world
        .without_sources(&["rollback", "--commit", COMMITS[0], "--service", SERVICE])
        .failed("a rollback to the running commit");
    assert!(
        again.contains(&format!("{} is the running version", COMMITS[0])),
        "{again}"
    );
    assert_eq!(world.running().await.0, json!(newest + 1));
    world.no_build_ran();
}

#[tokio::test]
async fn cf8_rollback_to_a_relabelled_commit_is_not_found() {
    let Some(world) = World::start().await else {
        return;
    };
    let [first, relabelling, later] = COMMITS;
    for (variant, commit) in [(1, first), (1, relabelling), (2, later)] {
        let bundle = world.bundled("notes.bundle", variant, commit);
        world.deploy_bundle(&bundle, &[]).ok("deploy --bundle");
    }
    let retained = world.versions().await;
    let newest = retained[0].0;
    assert_eq!(
        retained
            .into_iter()
            .map(|(_, _, _, commit)| commit)
            .collect::<Vec<_>>(),
        [json!(later), json!(relabelling)],
        "the second commit deployed nothing new and took the first version's label"
    );

    let not_found = world
        .without_sources(&["rollback", "--commit", first, "--service", SERVICE])
        .failed("a rollback to a commit nothing retained carries");

    assert!(
        not_found.contains(&format!("no retained version was deployed at {first}")),
        "{not_found}"
    );
    assert_eq!(world.running().await, (json!(newest), json!(later)));

    world
        .without_sources(&["rollback", "--commit", relabelling, "--service", SERVICE])
        .ok("rollback --commit");
    assert_eq!(
        world.running().await,
        (json!(newest + 1), json!(relabelling))
    );
    world.no_build_ran();
}
