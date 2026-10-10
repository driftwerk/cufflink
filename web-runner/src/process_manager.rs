use crate::child_env::ChildIdentity;
use crate::host::{compose_public_url, HostnameFormat};
use cufflink_shutdown::Shutdown;
use cufflink_supervisor::{
    child_env, output_event, runner_env, ChildSettings, ChildTelemetry, EnvMode, Exit, Launch,
    OutputLevel, Runner, StartError, Stop, Stream, Supervisor, Unready,
};
use cufflink_telemetry::{Component, Scope};
use std::collections::{BTreeSet, HashMap};
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

pub enum DeployOutcome {
    Success { port: u16 },
    HealthCheckFailed { port: u16, error: String },
}

type AppKey = (String, String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    pub version: i32,
    pub dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningApp {
    pub tenant: String,
    pub service: String,
    pub version: i32,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortAllocator {
    range: RangeInclusive<u16>,
    in_use: BTreeSet<u16>,
}

impl PortAllocator {
    pub fn new(range: RangeInclusive<u16>) -> Self {
        Self {
            range,
            in_use: BTreeSet::new(),
        }
    }

    fn allocate(&mut self) -> Option<u16> {
        let port = self
            .range
            .clone()
            .find(|port| !self.in_use.contains(port))?;
        self.in_use.insert(port);
        Some(port)
    }

    fn release(&mut self, port: u16) {
        self.in_use.remove(&port);
    }
}

struct WebApps {
    output_level: OutputLevel,
}

impl Runner for WebApps {
    type Key = AppKey;
    type Endpoint = u16;
    type Endpoints = PortAllocator;
    type Info = Release;

    const OUTPUT_TASK: &'static str = "web_child_output";
    const SUPERVISOR_TASK: &'static str = "web_child_supervisor";
    const TRACKS_CRASH_LOOPS: bool = true;

    fn release(ports: &mut PortAllocator, port: &u16) {
        ports.release(*port);
    }

    fn scope(&self, (tenant, service): &AppKey, release: &Release) -> Scope {
        Scope::service(tenant.as_str(), service.as_str(), release.version)
    }

    fn output_line(
        &self,
        (tenant, service): &AppKey,
        release: &Release,
        stream: Stream,
        line: &[u8],
    ) {
        output_event!(self.output_level, target: "web_child", tenant = %tenant, service = %service, version = release.version, stream = stream.as_str(), "{}", String::from_utf8_lossy(line));
    }

    fn output_dropped(
        &self,
        (tenant, service): &AppKey,
        release: &Release,
        stream: Stream,
        lines: u64,
    ) {
        output_event!(self.output_level, target: "web_child", tenant = %tenant, service = %service, version = release.version, stream = stream.as_str(), lines, "Lines of the app's output over the log budget were not logged");
    }

    fn exited(&self, exit: Exit<Self>) {
        let ((tenant, service), version, port, status) =
            (&exit.key, exit.info.version, exit.endpoint, &exit.status);
        metrics::counter!("cufflink_web_runner_child_exits_total", "reason" => exit.reason().as_str())
            .increment(1);
        if exit.crash_loop {
            error!(tenant = %tenant, service = %service, version, port, status = ?status, "Web app keeps exiting right after it starts; not restarting it until its next deploy");
        } else {
            warn!(tenant = %tenant, service = %service, version, port, status = ?status, "Web app exited; the next request starts it again");
        }
    }
}

pub struct ProcessManager {
    apps: Supervisor<WebApps>,
    base_domain: String,
    public_scheme: String,
    hostname_format: HostnameFormat,
    env_mode: EnvMode,
    child_telemetry: ChildTelemetry,
}

impl ProcessManager {
    pub fn new(
        ports: RangeInclusive<u16>,
        base_domain: String,
        public_scheme: String,
        hostname_format: HostnameFormat,
        env_mode: EnvMode,
        children: ChildSettings,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            apps: Supervisor::new(
                Arc::new(WebApps {
                    output_level: children.output_level,
                }),
                PortAllocator::new(ports),
                shutdown,
            ),
            base_domain,
            public_scheme,
            hostname_format,
            env_mode,
            child_telemetry: children.telemetry,
        }
    }

    pub async fn deploy(
        &self,
        tenant_slug: &str,
        service_name: &str,
        release: Release,
        env_vars: HashMap<String, String>,
    ) -> eyre::Result<DeployOutcome> {
        let (version, app_dir) = (release.version, release.dir.clone());
        if !app_dir.join(".next").exists() {
            eyre::bail!(
                ".next directory not found in {}. Is this a Next.js build?",
                app_dir.display()
            );
        }

        let port = self
            .apps
            .endpoints(PortAllocator::allocate)
            .ok_or_else(|| {
                eyre::eyre!(
                    "no free port for {}/{}: every port of WEB_PORT_RANGE is in use",
                    tenant_slug,
                    service_name
                )
            })?;

        info!(
            tenant = %tenant_slug,
            service = %service_name,
            version = version,
            port = port,
            dir = %app_dir.display(),
            "Starting Next.js process"
        );

        let public_url = compose_public_url(
            &self.public_scheme,
            tenant_slug,
            service_name,
            &self.base_domain,
            self.hostname_format,
        );
        let env = child_env(
            runner_env(),
            &self.env_mode,
            &env_vars,
            self.child_telemetry.naming(
                &Scope::service(tenant_slug, service_name, version),
                Component::WebRunner,
            ),
            ChildIdentity {
                port,
                public_url: public_url.clone(),
                tenant: tenant_slug.to_string(),
                service: service_name.to_string(),
            }
            .vars(),
        );
        info!(
            tenant = %tenant_slug,
            service = %service_name,
            public_url = %public_url,
            variables = env.len(),
            "Prepared the app's environment"
        );

        let launch = Launch {
            key: (tenant_slug.to_string(), service_name.to_string()),
            endpoint: port,
            info: release,
            program: "node".into(),
            args: [
                "node_modules/next/dist/bin/next",
                "start",
                "-p",
                &port.to_string(),
                "-H",
                "127.0.0.1",
            ]
            .map(str::to_string)
            .to_vec(),
            cwd: app_dir,
            env,
        };
        match self.apps.start(launch, wait_for_ready(port, 30)).await {
            Ok((replaced, ())) => {
                if let Some(old) = replaced {
                    info!(
                        tenant = %tenant_slug,
                        service = %service_name,
                        old_version = old.info.version,
                        old_port = old.endpoint,
                        new_version = version,
                        new_port = port,
                        "Replacing old process with new (zero-downtime)"
                    );
                    let _ = old.stop(Stop::Kill).await;
                }
                info!(
                    tenant = %tenant_slug,
                    service = %service_name,
                    version = version,
                    port = port,
                    "Deploy successful"
                );
                Ok(DeployOutcome::Success { port })
            }
            Err(StartError::Spawn(e)) => eyre::bail!(
                "Failed to spawn Node.js process for {}/{}: {}",
                tenant_slug,
                service_name,
                e
            ),
            Err(StartError::Unready(unready)) => {
                let error = match unready {
                    Unready::Exited(status) => {
                        format!("the app exited before it answered ({status})")
                    }
                    Unready::Unwaitable(e) => format!("the app could not be waited on: {e}"),
                    Unready::Probe(e) => e.to_string(),
                };
                warn!(
                    tenant = %tenant_slug,
                    service = %service_name,
                    version = version,
                    port = port,
                    error = %error,
                    "New process failed health check, NOT replacing old process"
                );
                Ok(DeployOutcome::HealthCheckFailed { port, error })
            }
            Err(StartError::Closed) => eyre::bail!(
                "the web runner is shutting down; {}/{} v{} was not started",
                tenant_slug,
                service_name,
                version
            ),
        }
    }

    pub fn get_port(&self, tenant_slug: &str, service_name: &str) -> Option<u16> {
        self.apps
            .endpoint(&(tenant_slug.to_string(), service_name.to_string()))
    }

    pub fn release(&self, tenant_slug: &str, service_name: &str) -> Option<Release> {
        self.apps
            .info(&(tenant_slug.to_string(), service_name.to_string()))
    }

    pub fn is_crash_looping(&self, tenant_slug: &str, service_name: &str) -> bool {
        self.apps
            .is_crash_looping(&(tenant_slug.to_string(), service_name.to_string()))
    }

    pub fn forget_exits(&self, tenant_slug: &str, service_name: &str) {
        self.apps
            .forget_exits(&(tenant_slug.to_string(), service_name.to_string()));
    }

    pub fn list(&self) -> Vec<RunningApp> {
        self.apps.list(|(tenant, service), app| RunningApp {
            tenant: tenant.clone(),
            service: service.clone(),
            version: app.info.version,
            port: app.endpoint,
        })
    }

    pub async fn stop(&self, tenant_slug: &str, service_name: &str) -> bool {
        let key = (tenant_slug.to_string(), service_name.to_string());
        if let Some(app) = self.apps.remove(&key) {
            info!(
                "Stopping web app {}/{} v{} (port {})",
                tenant_slug, service_name, app.info.version, app.endpoint
            );
            let _ = app.stop(Stop::Kill).await;
            true
        } else {
            info!(
                "No running process found for {}/{}",
                tenant_slug, service_name
            );
            false
        }
    }

    pub async fn shutdown_all(&self, stop_timeout: Duration) {
        let apps = self.apps.close();
        let stops = apps.into_iter().map(|((tenant, service), app)| async move {
            let (version, port) = (app.info.version, app.endpoint);
            let outcome = app.stop(Stop::Terminate(stop_timeout)).await;
            info!(
                tenant = %tenant,
                service = %service,
                version,
                port,
                outcome = ?outcome,
                "Web app stopped"
            );
        });
        futures_util::future::join_all(stops).await;
    }
}

async fn wait_for_ready(port: u16, max_retries: u32) -> eyre::Result<()> {
    let client: reqwest::Client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let mut last_status: Option<reqwest::StatusCode> = None;

    for attempt in 1..=max_retries {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        match client
            .get(format!("http://127.0.0.1:{}/", port))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() || resp.status().is_redirection() => {
                info!(
                    port = port,
                    attempt = attempt,
                    status = %resp.status(),
                    "Health check passed"
                );
                return Ok(());
            }
            Ok(resp) => {
                let status = resp.status();
                warn!(
                    port = port,
                    attempt = attempt,
                    status = %status,
                    max_retries = max_retries,
                    "Health check got unhealthy status, retrying"
                );
                last_status = Some(status);
                if attempt == max_retries {
                    return Err(eyre::eyre!(
                        "Process on port {} returned unhealthy status {} after {} attempts",
                        port,
                        status,
                        max_retries
                    ));
                }
            }
            Err(_) if attempt < max_retries => {
                continue;
            }
            Err(e) => {
                error!(
                    port = port,
                    attempts = max_retries,
                    error = %e,
                    "Health check failed: process never became reachable"
                );
                return Err(eyre::eyre!(
                    "Process on port {} not reachable after {} attempts: {}",
                    port,
                    max_retries,
                    e
                ));
            }
        }
    }

    if let Some(status) = last_status {
        Err(eyre::eyre!(
            "Process returned unhealthy status {} after {} attempts",
            status,
            max_retries
        ))
    } else {
        Err(eyre::eyre!("Process not ready"))
    }
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use std::time::Instant;
    use tokio::process::Command;

    #[test]
    fn ports_are_the_lowest_free_and_run_out_without_wrapping() {
        let mut ports = PortAllocator::new(3200..=3202);
        assert_eq!(ports.allocate(), Some(3200));
        assert_eq!(ports.allocate(), Some(3201));
        ports.release(3200);
        assert_eq!(ports.allocate(), Some(3200), "a freed port is reused");
        assert_eq!(ports.allocate(), Some(3202));
        assert_eq!(ports.allocate(), None, "exhausted, not wrapped");
        ports.release(3201);
        assert_eq!(ports.allocate(), Some(3201));
    }

    pub(crate) async fn node_available() -> bool {
        let available = Command::new("node").arg("--version").output().await.is_ok();
        assert!(
            available || std::env::var_os("CUFFLINK_CI").is_none(),
            "node is required under CUFFLINK_CI"
        );
        if !available {
            eprintln!("node is not installed; skipping");
        }
        available
    }

    #[tokio::test]
    async fn an_app_that_exits_before_answering_fails_its_health_check_at_once() {
        if !node_available().await {
            return;
        }
        let root = tempfile::TempDir::new().expect("temp dir");
        let app_dir = fake_next_app(root.path());
        std::fs::write(
            app_dir.join("node_modules/next/dist/bin/next"),
            "process.exit(3);\n",
        )
        .expect("exiting stand-in");
        let manager = ProcessManager::new(
            3290..=3290,
            "example.com".into(),
            "https".into(),
            HostnameFormat::Flat,
            crate::child_env::env_mode(None).expect("the built-in allowlist"),
            ChildSettings::default(),
            Shutdown::new(Duration::from_secs(5)),
        );
        let started = Instant::now();
        let outcome = manager
            .deploy(
                "acme",
                "web",
                Release {
                    version: 1,
                    dir: app_dir,
                },
                HashMap::new(),
            )
            .await
            .expect("deploy");
        assert!(
            matches!(&outcome, DeployOutcome::HealthCheckFailed { error, .. } if error.contains("exited")),
            "no polling past the exit"
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(
            manager.apps.endpoints(PortAllocator::allocate),
            Some(3290),
            "its port is free again"
        );
    }

    fn fake_next_app(root: &std::path::Path) -> PathBuf {
        let app = root.join("app");
        std::fs::create_dir_all(app.join(".next")).expect(".next");
        let bin = app.join("node_modules/next/dist/bin");
        std::fs::create_dir_all(&bin).expect("bin dir");
        std::fs::write(
            bin.join("next"),
            r#"const [, , , , port, , host] = process.argv;
require("http").createServer((req, res) => {
  res.setHeader("content-type", "application/json");
  res.end(JSON.stringify({ host, pid: process.pid, env: process.env }));
}).listen(Number(port), host);
"#,
        )
        .expect("next stand-in");
        app
    }

    #[tokio::test]
    async fn cf4_an_app_started_scoped_is_named_as_its_tenants_service() {
        if !node_available().await {
            return;
        }
        let root = tempfile::TempDir::new().expect("temp dir");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let manager = ProcessManager::new(
            port..=port,
            "example.com".into(),
            "https".into(),
            HostnameFormat::Flat,
            crate::child_env::env_mode(None).expect("the built-in allowlist"),
            ChildSettings {
                telemetry: ChildTelemetry::Scoped,
                ..ChildSettings::default()
            },
            Shutdown::new(Duration::from_secs(5)),
        );
        let release = Release {
            version: 3,
            dir: fake_next_app(root.path()),
        };
        let outcome = manager
            .deploy("acme", "portal", release, HashMap::new())
            .await
            .expect("deploy");
        assert!(matches!(outcome, DeployOutcome::Success { .. }));
        let served: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/"))
            .await
            .expect("app answers")
            .json()
            .await
            .expect("json");
        assert_eq!(served["env"]["OTEL_SERVICE_NAME"], "portal");
        let attributes: std::collections::BTreeSet<&str> = served["env"]
            ["OTEL_RESOURCE_ATTRIBUTES"]
            .as_str()
            .expect("resource attributes")
            .split(',')
            .collect();
        assert_eq!(
            attributes,
            [
                "service.namespace=acme",
                "service.version=v3",
                "cufflink.component=web-runner"
            ]
            .into()
        );

        let pid = served["pid"].as_i64().expect("pid") as libc::pid_t;
        // SAFETY: a signal to the app process this test started.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    }

    #[tokio::test]
    async fn an_app_starts_on_loopback_with_only_its_allowed_environment() {
        if !node_available().await {
            return;
        }
        let root = tempfile::TempDir::new().expect("temp dir");
        let app_dir = fake_next_app(root.path());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port")
            .port();
        let manager = ProcessManager::new(
            port..=port,
            "staging.example.com".into(),
            "https".into(),
            HostnameFormat::Flat,
            crate::child_env::env_mode(None).expect("the built-in allowlist"),
            ChildSettings::default(),
            Shutdown::new(Duration::from_secs(5)),
        );
        let configs = HashMap::from([("NEXTAUTH_SECRET".to_string(), "s".to_string())]);
        let outcome = manager
            .deploy(
                "preview-pr-7",
                "portal",
                Release {
                    version: 3,
                    dir: app_dir.clone(),
                },
                configs,
            )
            .await
            .expect("deploy");
        assert_eq!(
            manager.release("preview-pr-7", "portal"),
            Some(Release {
                version: 3,
                dir: app_dir
            })
        );
        assert!(matches!(outcome, DeployOutcome::Success { .. }));
        let served: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/"))
            .await
            .expect("app answers")
            .json()
            .await
            .expect("json");
        assert_eq!(served["host"], "127.0.0.1");
        let env = served["env"].as_object().expect("env");
        assert_eq!(env["CUFFLINK_TENANT"], "preview-pr-7");
        assert_eq!(
            env["CUFFLINK_PUBLIC_URL"],
            "https://portal--preview-pr-7.staging.example.com"
        );
        assert_eq!(env["NEXTAUTH_SECRET"], "s");
        assert_eq!(env["PORT"], port.to_string());
        let set_by_runner = [
            "PORT",
            "NODE_ENV",
            "HOSTNAME",
            "CUFFLINK_PUBLIC_URL",
            "CUFFLINK_TENANT",
            "CUFFLINK_SERVICE",
            "NEXTAUTH_SECRET",
        ];
        let unexpected: Vec<&String> = env
            .keys()
            .filter(|name| {
                !crate::child_env::WEB_ENV
                    .allowlist()
                    .any(|allowed| allowed == name.as_str())
            })
            .filter(|name| !set_by_runner.contains(&name.as_str()))
            .filter(|name| !name.starts_with("__CF_"))
            .collect();
        assert!(
            unexpected.is_empty(),
            "leaked from the runner: {unexpected:?}"
        );

        let pid = served["pid"].as_i64().expect("pid") as libc::pid_t;
        // SAFETY: a signal to the app process this test started.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        for _ in 0..100 {
            if manager.get_port("preview-pr-7", "portal").is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            manager.get_port("preview-pr-7", "portal"),
            None,
            "a crashed app leaves the table, so the next request cold-starts it"
        );
        manager.shutdown_all(Duration::from_secs(2)).await;
    }
}
