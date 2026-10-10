use cufflink_testkit as common;

use base64::Engine as _;
use common::otlp::Capture;
use common::{ChildProcess, PlatformProcess, Tenant, TestDb};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const CALLER: &str = "caller-svc";
const BUCKET: &str = "cufflink";
const TOKEN: &str = "ops-token";
const ECHO: [&str; 3] = ["echo.fail", "echo.reply", "echo.sleep"];

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

struct Binaries {
    runner: PathBuf,
    echo: PathBuf,
    guest: Vec<u8>,
}

impl Binaries {
    fn built() -> Option<Self> {
        if let Some(reason) = cufflink_test_guests::UNAVAILABLE {
            common::skip(reason);
            return None;
        }
        Some(Self {
            runner: PathBuf::from(env!("CARGO_BIN_EXE_cufflink-ext-runner")),
            echo: common::require_binary("ext-echo")?,
            guest: cufflink_test_guests::wasm("ext-call", Profile::Debug).expect("built guest"),
        })
    }

    fn echo(&self) -> Vec<u8> {
        std::fs::read(&self.echo).expect("the ext-echo binary")
    }

    fn wrapped_echo(&self, variant: &str) -> Vec<u8> {
        self.echo_serving(variant, &ECHO)
    }

    fn echo_serving(&self, variant: &str, extensions: &[&str]) -> Vec<u8> {
        format!(
            "#!/bin/sh\n# {variant}\nEXT_ECHO_EXTENSIONS='{}' exec '{}' \"$@\"\n",
            extensions.join(","),
            self.echo.display()
        )
        .into_bytes()
    }

    fn echo_manifest(&self) -> Value {
        let output = std::process::Command::new(&self.echo)
            .arg("--emit-manifest")
            .output()
            .expect("run ext-echo");
        assert!(output.status.success(), "{output:?}");
        serde_json::from_slice(&output.stdout).expect("a manifest")
    }
}

fn native_manifest(name: &str, extensions: &[&str]) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "mode": "native",
        "tables": [],
        "provides": extensions
            .iter()
            .map(|name| json!({"name": name, "callers": ["*"]}))
            .collect::<Vec<_>>(),
    })
}

struct World {
    platform: PlatformProcess,
    runner: Option<ChildProcess>,
    tenant: Tenant,
    admin: String,
    runner_key_id: String,
    runner_key: String,
    db: TestDb,
    nats_url: &'static str,
    binaries: Binaries,
    _storage: TempDir,
    apps: TempDir,
    sockets: TempDir,
}

async fn runner_key(platform: &PlatformProcess, admin: &str, tenant: &Tenant) -> (String, String) {
    let (status, minted) = platform
        .send(
            Method::POST,
            &format!("/api/tenants/{}/runner-keys", tenant.slug),
            admin,
            Some(&json!({"name": "ext-runner", "runs": "native"})),
        )
        .await;
    assert_eq!(status, 201, "{minted}");
    (
        minted["runner_key"]["id"]
            .as_str()
            .expect("an id")
            .to_string(),
        minted["key"].as_str().expect("a key").to_string(),
    )
}

impl World {
    async fn start() -> Option<Self> {
        let binaries = Binaries::built()?;
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
                ("S3_BUCKET", BUCKET),
                ("CUFFLINK_EXTENSIONS", "true"),
            ],
        )
        .await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let (runner_key_id, runner_key) = runner_key(&platform, &admin, &tenant).await;
        Some(Self {
            platform,
            runner: None,
            tenant,
            admin,
            runner_key_id,
            runner_key,
            db,
            nats_url,
            binaries,
            _storage: storage,
            apps: tempfile::tempdir().expect("apps"),
            sockets: tempfile::Builder::new()
                .prefix("cxr")
                .tempdir_in("/tmp")
                .expect("a short socket directory"),
        })
    }

    async fn start_runner(&mut self) {
        self.start_runner_with(&[]).await;
    }

    async fn start_runner_with(&mut self, settings: &[(&str, &str)]) {
        let runner = self.launch_runner(settings).await;
        eventually("the runner converged once", Duration::from_secs(30), || {
            let runner = &runner;
            async move {
                runner
                    .status("/_cufflink/ready")
                    .await
                    .filter(|status| *status == 200)
            }
        })
        .await;
        self.runner = Some(runner);
    }

    async fn launch_runner(&self, settings: &[(&str, &str)]) -> ChildProcess {
        let paths = [&self.apps, &self.sockets].map(|dir| dir.path().to_string_lossy().to_string());
        let defaults = [
            ("EXT_TENANT", self.tenant.slug.as_str()),
            ("PLATFORM_URL", self.platform.base_url.as_str()),
            ("CUFFLINK_RUNNER_KEY", self.runner_key.as_str()),
            ("NATS_URL", self.nats_url),
            ("APPS_DIR", paths[0].as_str()),
            ("EXT_SOCKET_DIR", paths[1].as_str()),
            ("EXT_RECONCILE_SECS", "1"),
            ("EXT_HEALTH_INTERVAL_SECS", "1"),
            ("EXT_READY_TIMEOUT_SECS", "20"),
            ("EXT_CHILD_STOP_TIMEOUT_SECS", "5"),
            ("EXT_STATUS_TOKEN", TOKEN),
            ("SHUTDOWN_GRACE_SECS", "20"),
            ("RUST_LOG", "info,cufflink_ext_runner=debug,ext_child=debug"),
        ];
        let env: Vec<(&str, &str)> = defaults
            .into_iter()
            .filter(|(name, _)| settings.iter().all(|(set, _)| set != name))
            .chain(settings.iter().copied())
            .collect();
        ChildProcess::spawn(&self.binaries.runner, "RUNNER_PORT", &env).await
    }

    fn runner(&self) -> &ChildProcess {
        self.runner.as_ref().expect("the runner runs")
    }

    async fn ops(&self, path: &str) -> reqwest::Response {
        ops(self.runner(), path).await
    }

    async fn children(&self, name: &str) -> Vec<Value> {
        children(self.runner(), name).await
    }

    async fn child_in(&self, name: &str, version: &Value, state: &str) -> Value {
        eventually(
            &format!("{name} v{version} {state}"),
            Duration::from_secs(30),
            || async {
                match self.children(name).await.as_slice() {
                    [child] if child["version"] == *version && child["state"] == state => {
                        Some(child.clone())
                    }
                    _ => None,
                }
            },
        )
        .await
    }

    async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let text = self
            .ops("/_cufflink/metrics")
            .await
            .text()
            .await
            .expect("metrics");
        common::metric_value(&text, name, labels)
    }

    async fn deploy_native(&self, manifest: &Value, binary: &[u8]) -> Value {
        self.deploy_native_for(&self.tenant, manifest, binary).await
    }

    async fn deploy_native_for(&self, tenant: &Tenant, manifest: &Value, binary: &[u8]) -> Value {
        let response = self
            .platform
            .http
            .post(format!("{}/api/services/deploy", self.platform.base_url))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .json(&json!({
                "manifest": manifest,
                "native_base64": base64::engine::general_purpose::STANDARD.encode(binary),
                "api_version": 1,
            }))
            .send()
            .await
            .expect("deploy");
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["skipped"], Value::Null, "{body}");
        body
    }

    async fn deploy_caller(&self) {
        self.deploy_caller_of(&ECHO).await;
    }

    async fn deploy_caller_of(&self, extensions: &[&str]) {
        self.deploy_caller_for(&self.tenant, extensions).await;
    }

    async fn deploy_caller_for(&self, tenant: &Tenant, extensions: &[&str]) {
        let mut manifest = common::wasm_manifest(CALLER, &[("call", "/call")]);
        manifest["extensions"] = json!(extensions);
        let (status, body) = self
            .platform
            .deploy_module(tenant, &manifest, &self.binaries.guest)
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn call(&self, extension: &str, payload: Value) -> Value {
        call(&self.platform, &self.tenant, extension, payload).await
    }

    async fn unserved(&self, extension: &str) -> Value {
        unserved(&self.platform, &self.tenant, extension).await
    }

    async fn manage(&self, method: Method, path: &str, body: Option<&Value>) -> Value {
        let request = self
            .platform
            .http
            .request(method, format!("{}{path}", self.platform.base_url))
            .header("authorization", format!("ApiKey {}", self.tenant.api_key));
        let response = match body {
            Some(body) => request.json(body),
            None => request,
        }
        .send()
        .await
        .expect("request");
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{body}");
        body
    }

    fn sockets(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.sockets.path())
            .expect("the socket directory")
            .map(|entry| entry.expect("an entry").path())
            .collect()
    }

    async fn stop_runner(&mut self) {
        if let Some(mut runner) = self.runner.take() {
            runner.sigterm();
            let exited = runner.wait_exit(Duration::from_secs(30)).await;
            assert!(
                exited.is_some_and(|status| status.success()),
                "the runner stops cleanly ({exited:?}); its output:\n{}",
                runner.output_tail()
            );
        }
    }
}

async fn ops(runner: &ChildProcess, path: &str) -> reqwest::Response {
    let response = runner
        .http
        .get(format!("{}{path}", runner.base_url))
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("an ops request");
    assert_eq!(response.status().as_u16(), 200, "{path}");
    response
}

async fn children(runner: &ChildProcess, name: &str) -> Vec<Value> {
    let status: Value = ops(runner, "/_cufflink/status")
        .await
        .json()
        .await
        .expect("status");
    status["children"]
        .as_array()
        .expect("children")
        .iter()
        .filter(|child| child["service"] == name)
        .cloned()
        .collect()
}

async fn call(
    platform: &PlatformProcess,
    tenant: &Tenant,
    extension: &str,
    payload: Value,
) -> Value {
    call_with(
        platform,
        tenant,
        json!({"extension": extension, "payload": payload}),
    )
    .await
}

async fn unserved(platform: &PlatformProcess, tenant: &Tenant, extension: &str) -> Value {
    call_with(
        platform,
        tenant,
        json!({"extension": extension, "payload": null, "timeout_ms": 2_000}),
    )
    .await
}

async fn call_with(platform: &PlatformProcess, tenant: &Tenant, body: Value) -> Value {
    let response = platform
        .http
        .post(platform.fn_url(tenant, CALLER, "call"))
        .json(&body)
        .send()
        .await
        .expect("call");
    assert_eq!(response.status().as_u16(), 200);
    response.json().await.expect("an answer")
}

fn write_key(file: &Path, key: &str) {
    let written = file.with_extension("new");
    std::fs::write(&written, key).expect("the new key");
    std::fs::rename(&written, file).expect("the key file");
}

fn is_alive(pid: libc::pid_t) -> bool {
    // SAFETY: a signal-less probe of a pid.
    unsafe { libc::kill(pid, 0) == 0 }
}

fn pid(child: &Value) -> libc::pid_t {
    libc::pid_t::try_from(child["pid"].as_u64().expect("a pid")).expect("pid")
}

#[tokio::test]
async fn a_native_service_answers_through_the_runner_across_redeploys_crashes_and_its_removal() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let manifest = w.binaries.echo_manifest();
    let v1 = w.deploy_native(&manifest, &w.binaries.echo()).await;
    let service_id = v1["service_id"].as_str().expect("service id").to_string();
    w.deploy_caller().await;
    w.start_runner().await;

    let first = w.child_in("ext-echo", &v1["version"], "ready").await;
    assert_eq!(first["tenant"], json!(w.tenant.slug));
    assert_eq!(first["extensions"], json!(ECHO));
    assert_eq!(
        first["sha"],
        json!(&v1["native_hash"].as_str().expect("hash")[..12])
    );
    assert_eq!(w.sockets().len(), 1, "one socket per child");

    let echoed = w.call("echo.reply", json!({"n": 1})).await;
    assert_eq!(
        echoed,
        json!({"ok": {
            "payload": {"n": 1},
            "tenant": w.tenant.slug,
            "service": {"name": CALLER, "version": echoed["ok"]["service"]["version"]},
        }}),
        "answered by the child with the caller's context"
    );
    assert_eq!(
        w.call(
            "echo.fail",
            json!({"code": "quota", "message": "out of it"})
        )
        .await,
        json!({"error": "failed", "code": "quota", "message": "out of it"})
    );

    let calling = Arc::new(AtomicBool::new(true));
    let callers: Vec<_> = (0..4)
        .map(|n| {
            let (calling, platform_url) = (
                calling.clone(),
                w.platform.fn_url(&w.tenant, CALLER, "call"),
            );
            let http = w.platform.http.clone();
            tokio::spawn(async move {
                let mut answers = Vec::new();
                let call = match n % 2 {
                    0 => json!({"extension": "echo.reply", "payload": {"caller": n}}),
                    _ => json!({"extension": "echo.sleep", "payload": {"ms": 250}}),
                };
                while calling.load(Ordering::SeqCst) {
                    let answer: Value = http
                        .post(&platform_url)
                        .json(&call)
                        .send()
                        .await
                        .expect("call")
                        .json()
                        .await
                        .expect("an answer");
                    answers.push(answer);
                }
                answers
            })
        })
        .collect();
    let v2 = w
        .deploy_native(&manifest, &w.binaries.wrapped_echo("v2"))
        .await;
    let second = w.child_in("ext-echo", &v2["version"], "ready").await;
    eventually("the old child stopped", Duration::from_secs(10), || async {
        (!is_alive(pid(&first)) && w.sockets().len() == 1).then_some(())
    })
    .await;
    calling.store(false, Ordering::SeqCst);
    let answers: Vec<Value> = futures_util::future::join_all(callers)
        .await
        .into_iter()
        .flat_map(|answers| answers.expect("a caller"))
        .collect();
    let slept = answers
        .iter()
        .filter(|a| a["ok"]["slept_ms"] == 250)
        .count();
    assert!(
        answers.len() > 8 && slept >= 2,
        "calls, slow ones among them, ran while the children switched"
    );
    let failed: Vec<&Value> = answers.iter().filter(|a| a.get("ok").is_none()).collect();
    assert!(
        failed.is_empty(),
        "no call fails while the children switch: {failed:?}"
    );

    // SAFETY: a signal to the child the runner started.
    assert_eq!(unsafe { libc::kill(pid(&second), libc::SIGKILL) }, 0);
    let restarted = eventually("the child restarted", Duration::from_secs(30), || async {
        w.children("ext-echo")
            .await
            .into_iter()
            .find(|child| child["state"] == "ready" && child["pid"] != second["pid"])
    })
    .await;
    assert_eq!(restarted["version"], v2["version"]);
    assert_eq!(w.call("echo.reply", json!(2)).await["ok"]["payload"], 2);
    assert!(w
        .metric(
            "cufflink_ext_runner_restarts_total",
            &[("reason", "signal")]
        )
        .await
        .is_some_and(|restarts| restarts >= 1.0));

    let mut widened = manifest.clone();
    widened["provides"]
        .as_array_mut()
        .expect("provides")
        .push(json!({"name": "echo.extra", "callers": ["*"]}));
    let refused = w
        .deploy_native(&widened, &w.binaries.wrapped_echo("v2"))
        .await;
    eventually(
        "the version is refused",
        Duration::from_secs(30),
        || async {
            let children = w.children("ext-echo").await;
            let states: Vec<(Value, Value)> = children
                .iter()
                .map(|child| (child["version"].clone(), child["state"].clone()))
                .collect();
            (states
                == [
                    (v2["version"].clone(), json!("ready")),
                    (refused["version"].clone(), json!("refused")),
                ])
            .then_some(())
        },
    )
    .await;
    assert_eq!(
        w.call("echo.reply", json!(3)).await["ok"]["payload"],
        3,
        "the old version keeps serving"
    );
    assert_eq!(
        w.children("ext-echo")
            .await
            .into_iter()
            .filter(|child| child["state"] == "ready")
            .map(|child| child["pid"].clone())
            .collect::<Vec<_>>(),
        [restarted["pid"].clone()],
        "the child that served is the one that ran before the refused deploy"
    );
    let restored = w
        .deploy_native(&manifest, &w.binaries.wrapped_echo("v3"))
        .await;
    w.child_in("ext-echo", &restored["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(4)).await["ok"]["payload"], 4);

    for (name, labels) in [
        ("cufflink_ext_runner_calls_total", &[("result", "ok")][..]),
        ("cufflink_ext_runner_calls_total", &[("result", "failed")]),
        ("cufflink_ext_runner_call_duration_seconds_count", &[]),
        ("cufflink_ext_runner_children", &[("state", "ready")]),
    ] {
        assert!(
            w.metric(name, labels)
                .await
                .is_some_and(|value| value >= 1.0),
            "{name}{labels:?}"
        );
    }
    assert_eq!(
        w.metric(
            "cufflink_ext_runner_children",
            &[("state", "crash_looping")]
        )
        .await,
        Some(0.0)
    );

    w.manage(
        Method::DELETE,
        &format!("/api/services/{service_id}?force=true"),
        None,
    )
    .await;
    eventually("the child stopped", Duration::from_secs(30), || async {
        (w.children("ext-echo").await.is_empty() && w.sockets().is_empty()).then_some(())
    })
    .await;
    assert_eq!(w.call("echo.reply", json!(5)).await["error"], "not_found");
    w.stop_runner().await;
}

#[tokio::test]
async fn a_binary_that_exits_at_once_crash_loops_until_its_next_deploy() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let manifest = native_manifest("ext-echo", &ECHO);
    w.start_runner().await;
    let crashing = w.deploy_native(&manifest, b"#!/bin/sh\nexit 3\n").await;
    w.deploy_caller().await;
    w.child_in("ext-echo", &crashing["version"], "crash_looping")
        .await;
    assert_eq!(
        w.metric(
            "cufflink_ext_runner_children",
            &[("state", "crash_looping")]
        )
        .await,
        Some(1.0)
    );
    assert_eq!(
        w.metric(
            "cufflink_ext_runner_restarts_total",
            &[("reason", "crash_loop")]
        )
        .await,
        Some(1.0)
    );
    assert_eq!(
        w.metric(
            "cufflink_ext_runner_restarts_total",
            &[("reason", "unready")]
        )
        .await,
        Some(2.0),
        "two restarts, then the crash loop"
    );
    assert_eq!(w.unserved("echo.reply").await["error"], "unavailable");

    let fixed = w.deploy_native(&manifest, &w.binaries.echo()).await;
    w.child_in("ext-echo", &fixed["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);
    w.stop_runner().await;
}

#[tokio::test]
async fn what_no_event_announces_is_converged_by_the_timer() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let deployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.echo())
        .await;
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();
    w.deploy_caller().await;
    w.start_runner().await;
    w.child_in("ext-echo", &deployed["version"], "ready").await;

    let status = format!("/api/services/{service_id}/status");
    w.manage(Method::PATCH, &status, Some(&json!({"status": "inactive"})))
        .await;
    eventually(
        "the inactive service's child stopped",
        Duration::from_secs(15),
        || async { w.children("ext-echo").await.is_empty().then_some(()) },
    )
    .await;
    assert_eq!(w.unserved("echo.reply").await["error"], "unavailable");

    w.manage(Method::PATCH, &status, Some(&json!({"status": "active"})))
        .await;
    let hung = w.child_in("ext-echo", &deployed["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);

    // SAFETY: a signal to the child the runner started.
    assert_eq!(unsafe { libc::kill(pid(&hung), libc::SIGSTOP) }, 0);
    let replaced = eventually(
        "the hung child was killed and started again",
        Duration::from_secs(30),
        || async {
            w.children("ext-echo")
                .await
                .into_iter()
                .find(|child| child["state"] == "ready" && child["pid"] != hung["pid"])
        },
    )
    .await;
    assert!(!is_alive(pid(&hung)), "the hung child is gone");
    assert_eq!(replaced["version"], deployed["version"]);
    assert_eq!(
        w.metric(
            "cufflink_ext_runner_restarts_total",
            &[("reason", "killed")]
        )
        .await,
        Some(1.0)
    );
    assert_eq!(w.call("echo.reply", json!(2)).await["ok"]["payload"], 2);
    w.stop_runner().await;
    assert!(
        !is_alive(pid(&replaced)),
        "a stopped runner stops its children"
    );
    assert!(
        w.sockets().is_empty(),
        "a stopped runner leaves no socket behind: {:?}",
        w.sockets()
    );
}

#[tokio::test]
async fn g9_an_inactive_native_service_is_not_started() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let manifest = w.binaries.echo_manifest();
    let v1 = w.deploy_native(&manifest, &w.binaries.echo()).await;
    let service = format!(
        "/api/services/{}",
        v1["service_id"].as_str().expect("service id")
    );
    w.deploy_caller().await;
    w.start_runner().await;
    w.child_in("ext-echo", &v1["version"], "ready").await;

    w.manage(
        Method::PATCH,
        &format!("{service}/status"),
        Some(&json!({"status": "inactive"})),
    )
    .await;
    eventually(
        "the inactive service's child stopped",
        Duration::from_secs(15),
        || async { w.children("ext-echo").await.is_empty().then_some(()) },
    )
    .await;
    let never_started = |after: &'static str| {
        let w = &w;
        async move {
            let watched = Instant::now() + Duration::from_secs(6);
            while Instant::now() < watched {
                assert_eq!(
                    w.children("ext-echo").await,
                    Vec::<Value>::new(),
                    "a child of the inactive service runs after {after}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert_eq!(w.unserved("echo.reply").await["error"], "unavailable");
        }
    };

    let v2 = w
        .deploy_native(&manifest, &w.binaries.wrapped_echo("v2"))
        .await;
    never_started("its deploy").await;
    let rolled_back = w
        .manage(
            Method::POST,
            &format!("{service}/rollback"),
            Some(&json!({"target_version": v1["version"]})),
        )
        .await;
    assert_eq!(
        rolled_back["new_version"].as_i64(),
        v2["version"].as_i64().map(|version| version + 1),
        "{rolled_back}"
    );
    never_started("its rollback").await;

    w.manage(
        Method::PATCH,
        &format!("{service}/status"),
        Some(&json!({"status": "active"})),
    )
    .await;
    w.child_in("ext-echo", &rolled_back["new_version"], "ready")
        .await;
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);
    w.stop_runner().await;
}

#[tokio::test]
async fn one_call_at_a_time_reaches_every_extension() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let deployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.echo())
        .await;
    w.deploy_caller().await;
    w.start_runner_with(&[("EXT_MAX_CONCURRENT_CALLS", "1")])
        .await;
    w.child_in("ext-echo", &deployed["version"], "ready").await;
    let started = Instant::now();
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);
    assert_eq!(
        w.call("echo.fail", json!({"code": "quota"})).await["code"],
        "quota"
    );
    assert_eq!(
        w.call("echo.sleep", json!({"ms": 10})).await["ok"]["slept_ms"],
        10
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "no call waits on a permit an idle extension holds"
    );
    w.stop_runner().await;
}

#[tokio::test]
async fn a_redeploy_that_serves_more_or_fewer_extensions_moves_its_subscriptions() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let manifest = w.binaries.echo_manifest();
    let first = w.deploy_native(&manifest, &w.binaries.echo()).await;
    w.deploy_caller().await;
    w.start_runner().await;
    w.child_in("ext-echo", &first["version"], "ready").await;

    let widened_set = [&["echo.extra"], &ECHO[..]].concat();
    let widened = w
        .deploy_native(
            &native_manifest("ext-echo", &widened_set),
            &w.binaries.echo_serving("widened", &widened_set),
        )
        .await;
    let child = w.child_in("ext-echo", &widened["version"], "ready").await;
    assert_eq!(child["extensions"], json!(widened_set));
    w.deploy_caller_of(&widened_set).await;
    assert_eq!(
        w.call("echo.extra", json!("more")).await["ok"]["payload"],
        "more",
        "the added extension is served"
    );
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);

    w.deploy_caller().await;
    let narrowed = w
        .deploy_native(&manifest, &w.binaries.wrapped_echo("narrowed"))
        .await;
    w.child_in("ext-echo", &narrowed["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(2)).await["ok"]["payload"], 2);
    let nats = async_nats::connect(w.nats_url).await.expect("NATS");
    let subject = cufflink_types::ext::call_subject(&w.tenant.slug, "echo.extra");
    eventually(
        "nothing serves the extension no longer provided",
        Duration::from_secs(15),
        || async {
            match nats.request(subject.clone(), "{}".into()).await {
                Err(e) if e.kind() == async_nats::RequestErrorKind::NoResponders => Some(()),
                Ok(_) | Err(_) => None,
            }
        },
    )
    .await;
    w.stop_runner().await;
}

#[tokio::test]
async fn a_slow_call_holds_up_no_other_service_and_shutdown_stops_every_child() {
    let Some(mut w) = World::start().await else {
        return;
    };
    const OTHER: [&str; 1] = ["other.reply"];
    let echo = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.echo())
        .await;
    let other = w
        .deploy_native(
            &native_manifest("ext-other", &OTHER),
            &w.binaries.echo_serving("other", &OTHER),
        )
        .await;
    w.deploy_caller_of(&[&ECHO[..], &OTHER].concat()).await;
    w.start_runner().await;
    let first = w.child_in("ext-echo", &echo["version"], "ready").await;
    let crashing = w.child_in("ext-other", &other["version"], "ready").await;

    let slow = tokio::spawn({
        let (http, url) = (
            w.platform.http.clone(),
            w.platform.fn_url(&w.tenant, CALLER, "call"),
        );
        async move {
            let answer = http
                .post(url)
                .json(&json!({"extension": "echo.sleep", "payload": {"ms": 10_000}}))
                .send()
                .await
                .expect("the slow call");
            answer.json::<Value>().await.expect("an answer")
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let redeployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.wrapped_echo("v2"))
        .await;
    let second = w
        .child_in("ext-echo", &redeployed["version"], "ready")
        .await;
    assert!(is_alive(pid(&first)), "the old child answers its slow call");

    // SAFETY: a signal to the child the runner started.
    assert_eq!(unsafe { libc::kill(pid(&crashing), libc::SIGKILL) }, 0);
    let restarted = eventually(
        "the crashed service restarted",
        Duration::from_secs(20),
        || async {
            w.children("ext-other")
                .await
                .into_iter()
                .find(|child| child["state"] == "ready" && child["pid"] != crashing["pid"])
        },
    )
    .await;
    assert!(
        !slow.is_finished(),
        "the crashed service restarted while the slow call still ran"
    );
    assert_eq!(w.call("other.reply", json!(1)).await["ok"]["payload"], 1);

    w.stop_runner().await;
    assert_eq!(
        slow.await.expect("the slow call"),
        json!({"ok": {"slept_ms": 10_000}}),
        "a draining child answers its call before the runner stops"
    );
    for child in [&first, &second, &restarted] {
        assert!(!is_alive(pid(child)), "{child}");
    }
    assert!(
        w.sockets().is_empty(),
        "a stopped runner leaves no socket behind: {:?}",
        w.sockets()
    );
}

#[tokio::test]
async fn a_runner_serves_its_own_tenant_alone() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let other = w.db.mint_tenant().await;
    let manifest = w.binaries.echo_manifest();
    let own = w.deploy_native(&manifest, &w.binaries.echo()).await;
    let theirs = w
        .deploy_native_for(&other, &manifest, &w.binaries.wrapped_echo("theirs"))
        .await;
    w.deploy_caller().await;
    w.deploy_caller_for(&other, &ECHO).await;
    w.start_runner().await;

    let child = w.child_in("ext-echo", &own["version"], "ready").await;
    assert_eq!(child["tenant"], json!(w.tenant.slug));
    assert_eq!(
        child["sha"],
        json!(&own["native_hash"].as_str().expect("hash")[..12])
    );
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);
    assert_eq!(
        unserved(&w.platform, &other, "echo.reply").await["error"],
        "unavailable",
        "no runner of the other tenant, and this one never serves it"
    );
    let theirs_id = theirs["service_id"].as_str().expect("service id");
    for path in [
        format!(
            "/api/runner/native/{theirs_id}/{}",
            theirs["native_hash"].as_str().expect("hash")
        ),
        format!("/api/runner/native/{theirs_id}/environment"),
    ] {
        let (status, refused) = w
            .platform
            .send(
                Method::GET,
                &path,
                &format!("RunnerKey {}", w.runner_key),
                None,
            )
            .await;
        assert_eq!(status, 404, "{path}: {refused}");
    }

    let (_, other_key) = runner_key(&w.platform, &w.admin, &other).await;
    let (other_apps, other_sockets) = (
        tempfile::tempdir().expect("apps"),
        tempfile::Builder::new()
            .prefix("cxo")
            .tempdir_in("/tmp")
            .expect("a short socket directory"),
    );
    let dirs = [&other_apps, &other_sockets].map(|dir| dir.path().to_string_lossy().to_string());
    let beside = w
        .launch_runner(&[
            ("EXT_TENANT", other.slug.as_str()),
            ("CUFFLINK_RUNNER_KEY", other_key.as_str()),
            ("APPS_DIR", dirs[0].as_str()),
            ("EXT_SOCKET_DIR", dirs[1].as_str()),
        ])
        .await;
    let theirs_child = eventually(
        "the other tenant's runner serves its own",
        Duration::from_secs(30),
        || {
            let (beside, version) = (&beside, &theirs["version"]);
            async move {
                match children(beside, "ext-echo").await.as_slice() {
                    [child] if child["version"] == *version && child["state"] == "ready" => {
                        Some(child.clone())
                    }
                    _ => None,
                }
            }
        },
    )
    .await;
    assert_eq!(theirs_child["tenant"], json!(other.slug));
    assert_eq!(
        call(&w.platform, &other, "echo.reply", json!(2)).await["ok"]["tenant"],
        json!(other.slug)
    );
    assert_eq!(
        w.children("ext-echo").await.len(),
        1,
        "each runner runs its own tenant's child alone"
    );
    assert_eq!(
        w.call("echo.reply", json!(3)).await["ok"]["tenant"],
        json!(w.tenant.slug)
    );
    assert_eq!(
        w.child_in("ext-echo", &own["version"], "ready").await["pid"],
        child["pid"],
        "the other tenant's runner left this runner's child running"
    );
    let mut beside = beside;
    beside.sigterm();
    assert!(beside
        .wait_exit(Duration::from_secs(30))
        .await
        .is_some_and(|status| status.success()));
    w.stop_runner().await;
}

#[tokio::test]
async fn a_runner_with_another_tenants_key_runs_nothing() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let other = w.db.mint_tenant().await;
    let manifest = w.binaries.echo_manifest();
    w.deploy_native(&manifest, &w.binaries.echo()).await;
    w.deploy_native_for(&other, &manifest, &w.binaries.wrapped_echo("theirs"))
        .await;
    let (_, other_key) = runner_key(&w.platform, &w.admin, &other).await;
    let mut runner = w
        .launch_runner(&[("CUFFLINK_RUNNER_KEY", other_key.as_str())])
        .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        runner.status("/_cufflink/ready").await,
        Some(503),
        "a runner whose key is another tenant's never converges"
    );
    assert!(children(&runner, "ext-echo").await.is_empty());
    assert!(
        std::fs::read_dir(w.apps.path())
            .expect("the apps directory")
            .next()
            .is_none(),
        "no binary of either tenant is fetched"
    );
    runner.sigterm();
    assert!(runner
        .wait_exit(Duration::from_secs(30))
        .await
        .is_some_and(|status| status.success()));
}

#[tokio::test]
async fn a_runner_whose_key_is_refused_stops_its_children_until_a_key_is_accepted() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let deployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.echo())
        .await;
    w.deploy_caller().await;
    let keys = tempfile::tempdir().expect("a key directory");
    let key_file = keys.path().join("runner-key");
    write_key(&key_file, &w.runner_key);
    let key_path = key_file.to_string_lossy().to_string();
    w.start_runner_with(&[
        ("CUFFLINK_RUNNER_KEY", ""),
        ("CUFFLINK_RUNNER_KEY_FILE", key_path.as_str()),
    ])
    .await;
    let child = w.child_in("ext-echo", &deployed["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(1)).await["ok"]["payload"], 1);

    let (status, revoked) = w
        .platform
        .send(
            Method::DELETE,
            &format!(
                "/api/tenants/{}/runner-keys/{}",
                w.tenant.slug, w.runner_key_id
            ),
            &w.admin,
            None,
        )
        .await;
    assert_eq!(status, 200, "{revoked}");
    eventually("every child stopped", Duration::from_secs(30), || async {
        (w.children("ext-echo").await.is_empty() && !is_alive(pid(&child))).then_some(())
    })
    .await;
    eventually("the runner unready", Duration::from_secs(30), || async {
        (w.runner().status("/_cufflink/ready").await == Some(503)).then_some(())
    })
    .await;
    assert_eq!(
        w.unserved("echo.reply").await["error"],
        "unavailable",
        "the runner no longer subscribes to the tenant's calls"
    );
    eventually("no socket left", Duration::from_secs(30), || async {
        w.sockets().is_empty().then_some(())
    })
    .await;

    let (_, key) = runner_key(&w.platform, &w.admin, &w.tenant).await;
    write_key(&key_file, &key);
    w.child_in("ext-echo", &deployed["version"], "ready").await;
    assert_eq!(w.call("echo.reply", json!(3)).await["ok"]["payload"], 3);
    eventually("the runner ready", Duration::from_secs(30), || async {
        (w.runner().status("/_cufflink/ready").await == Some(200)).then_some(())
    })
    .await;
    w.stop_runner().await;
}

#[test]
fn the_runner_refuses_a_secret_given_as_a_variable_and_a_file() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cufflink-ext-runner"))
        .env_clear()
        .env("EXT_TENANT", "acme")
        .env("PLATFORM_URL", "http://127.0.0.1:1")
        .env("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret")
        .env("EXT_STATUS_TOKEN", "0ps-t0ken")
        .env("EXT_STATUS_TOKEN_FILE", "/run/secrets/absent")
        .output()
        .expect("the runner runs");
    assert!(!output.status.success());
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(
        said.contains("EXT_STATUS_TOKEN and EXT_STATUS_TOKEN_FILE are both set; set one"),
        "{said}"
    );
    assert!(!said.contains("0ps-t0ken"), "no secret is echoed: {said}");
}

#[test]
fn the_runner_refuses_a_socket_directory_too_long_for_its_sockets() {
    let long = Path::new("/tmp").join("d".repeat(80));
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_cufflink-ext-runner"))
        .env_clear()
        .env("EXT_TENANT", "acme")
        .env("PLATFORM_URL", "http://127.0.0.1:1")
        .env("CUFFLINK_RUNNER_KEY", "cfr_AAAAAAAAAAAA_s3cret")
        .env("EXT_SOCKET_DIR", &long)
        .output()
        .expect("the runner runs");
    assert!(!output.status.success());
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(said.contains("EXT_SOCKET_DIR"), "{said}");
    assert!(said.contains("is too long"), "{said}");
    assert!(!long.exists(), "nothing is created");
}

const CHILD_SAYS: &str = "the echo service starts";

impl Binaries {
    fn talkative_echo(&self) -> Vec<u8> {
        format!(
            "#!/bin/sh\necho \"{CHILD_SAYS} as $OTEL_SERVICE_NAME of $OTEL_RESOURCE_ATTRIBUTES\"\nexec '{}' \"$@\"\n",
            self.echo.display()
        )
        .into_bytes()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf4_child_output_is_exported_under_the_childs_service() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let capture = Capture::start().await.expect("capture");
    let deployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.talkative_echo())
        .await;
    let settings: Vec<(&str, &str)> = capture
        .env()
        .into_iter()
        .chain([
            ("CUFFLINK_CHILD_TELEMETRY", "scoped"),
            ("CUFFLINK_CHILD_OUTPUT_LEVEL", "info"),
            ("RUST_LOG", "info"),
        ])
        .collect();
    w.start_runner_with(&settings).await;
    w.child_in("ext-echo", &deployed["version"], "ready").await;

    let slug = w.tenant.slug.as_str();
    let version = format!("v{}", deployed["version"]);
    let said = capture
        .log("the child's line", |record| {
            record.text.starts_with(CHILD_SAYS)
        })
        .await
        .expect("exported");
    assert_eq!(
        said.text,
        format!(
            "{CHILD_SAYS} as ext-echo of service.namespace={slug},service.version={version},cufflink.component=ext-runner"
        )
    );
    assert_eq!(
        said.scope(),
        [
            ("cufflink.kind", "child"),
            ("cufflink.service", "ext-echo"),
            ("cufflink.tenant", slug),
            ("cufflink.version", version.as_str()),
        ]
    );
    assert_eq!(
        (said.attribute("stream"), said.attribute("service")),
        (Some("stdout"), Some("ext-echo")),
        "the event keeps its fields"
    );
    assert_eq!(
        (
            said.resource.get("service.name").map(String::as_str),
            said.resource.get("cufflink.component").map(String::as_str),
            said.resource.get("cufflink.tenant").map(String::as_str),
        ),
        (Some("cufflink-ext-runner"), Some("ext-runner"), Some(slug)),
        "exported by the runner, once"
    );

    w.stop_runner().await;
    let output = capture
        .span("the span the line was said under", |span| {
            span.span_id == said.span_id
        })
        .await
        .expect("exported once the child has stopped");
    assert_eq!(
        (
            output.text.as_str(),
            output.scope(),
            output.attribute("stream")
        ),
        ("child_output", said.scope(), Some("stdout"))
    );
    assert_eq!(
        output.parent_span_id, "",
        "a child's output is its own trace"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf4_child_output_is_silent_by_default() {
    let Some(mut w) = World::start().await else {
        return;
    };
    let capture = Capture::start().await.expect("capture");
    let deployed = w
        .deploy_native(&w.binaries.echo_manifest(), &w.binaries.talkative_echo())
        .await;
    let settings: Vec<(&str, &str)> = capture
        .env()
        .into_iter()
        .chain([("RUST_LOG", "info")])
        .collect();
    w.start_runner_with(&settings).await;
    w.child_in("ext-echo", &deployed["version"], "ready").await;

    let started = capture
        .log("the runner's own line", |record| {
            record.text == "Starting a native service"
        })
        .await
        .expect("exported");
    assert_eq!(started.scope(), []);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let from_children: Vec<_> = capture
        .logs()
        .into_iter()
        .filter(|record| !record.scope().is_empty() || record.text.contains(CHILD_SAYS))
        .collect();
    assert_eq!(from_children, []);
    w.stop_runner().await;
}
