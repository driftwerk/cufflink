use crate::bundle::Bundle;
use crate::commands::services::{self, Status};
use crate::commands::{native, version};
use crate::config::CliConfig;
use crate::project_config::ProjectConfig;
use cufflink_types::manifest::validate::{active_refusal, inactive_refusal};
use cufflink_types::ServiceMode;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const CLI_DEPLOY_API_VERSION: u32 = 1;

const REQUIRED_PACKAGE: &str = "next";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeployFlags {
    pub allow_destructive: bool,
    pub accept_drift: bool,
    pub purge: bool,
    pub plan: bool,
    pub inactive: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Target<'a> {
    env: Option<&'a str>,
    tenant: Option<&'a str>,
}

impl<'a> Target<'a> {
    fn of(
        config: &CliConfig,
        env: Option<&'a str>,
        tenant: Option<&'a str>,
        platform_key: bool,
    ) -> Self {
        Target {
            env,
            tenant: config.acting(tenant, platform_key).named(),
        }
    }

    fn services(&self, action: &str, name: &str) -> String {
        [("--env", self.env), ("--tenant", self.tenant)]
            .into_iter()
            .filter_map(|(flag, value)| value.map(|value| format!(" {flag} {value}")))
            .fold(
                format!("cufflink services {action} {name}"),
                |command, flag| command + &flag,
            )
    }
}

#[derive(Debug, Clone, Copy)]
struct NotRunning(());

#[derive(Debug)]
struct Unsettled(String);

impl std::fmt::Display for Unsettled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unsettled {}

struct Deploy<'a> {
    url: String,
    name: &'a str,
    plan: bool,
    quiet: Option<NotRunning>,
    target: Target<'a>,
    following: Following,
}

#[derive(Debug)]
pub struct Refused {
    pub body: String,
}

impl Refused {
    pub const EXIT_CODE: u8 = 3;
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Plan refused: {}", self.body)
    }
}

impl std::error::Error for Refused {}

#[derive(Debug, Clone, Copy)]
struct Following {
    interval: Duration,
    attempts: u32,
    lost_grace: Duration,
}

const FOLLOWING: Following = Following {
    interval: Duration::from_secs(2),
    attempts: 5,
    lost_grace: Duration::from_secs(120),
};

const REPLICA_LOST: &str = "replica lost";

enum Unfollowed {
    Passing(String),
    Lasting(String),
}

fn unanswered(plan: bool, status: reqwest::StatusCode, body: String) -> eyre::Report {
    match (plan, status) {
        (true, reqwest::StatusCode::UNPROCESSABLE_ENTITY) => Refused { body }.into(),
        (true, _) => eyre::eyre!("Plan failed ({status}): {body}"),
        (false, _) => eyre::eyre!("Deploy failed ({status}): {body}"),
    }
}

async fn poll(
    config: &CliConfig,
    client: &reqwest::Client,
    url: &str,
) -> Result<serde_json::Value, Unfollowed> {
    let passing = |e: reqwest::Error| Unfollowed::Passing(e.to_string());
    let resp = config
        .auth_request(client, reqwest::Method::GET, url)
        .send()
        .await
        .map_err(passing)?;
    let status = resp.status();
    if status.is_success() {
        return resp.json().await.map_err(passing);
    }
    let failure = format!("{status}: {}", resp.text().await.unwrap_or_default());
    Err(
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Unfollowed::Passing(failure)
        } else {
            Unfollowed::Lasting(failure)
        },
    )
}

fn deploy_url(api_url: &str, flags: &DeployFlags, supports: &[String]) -> String {
    let run = if flags.plan {
        Some("plan=true")
    } else {
        supports
            .iter()
            .any(|s| s == cufflink_types::features::ASYNC_DEPLOY)
            .then_some("async=true")
    };
    let query: Vec<&str> = run
        .into_iter()
        .chain(flags.inactive.then_some("inactive=true"))
        .collect();
    let query = match query.as_slice() {
        [] => String::new(),
        asked => format!("?{}", asked.join("&")),
    };
    format!("{api_url}/api/services/deploy{query}")
}

fn progress_line(progress: &serde_json::Value) -> Option<String> {
    let phase = progress["phase"].as_str()?;
    let details: Vec<String> = progress
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(key, _)| *key != "phase")
        .map(|(key, value)| format!("{key} {value}"))
        .collect();
    Some(if details.is_empty() {
        phase.to_string()
    } else {
        format!("{phase} ({})", details.join(", "))
    })
}

async fn await_deploy(
    config: &CliConfig,
    client: &reqwest::Client,
    deploy_id: &str,
    following: Following,
) -> eyre::Result<serde_json::Value> {
    let url = format!("{}/api/deploys/{deploy_id}", config.api_url);
    let mut last_progress = String::new();
    let mut failed = 0;
    let mut lost: Option<Instant> = None;
    loop {
        tokio::time::sleep(following.interval).await;
        let job = match poll(config, client, &url).await {
            Ok(job) => job,
            Err(Unfollowed::Passing(reason)) if failed + 1 < following.attempts => {
                failed += 1;
                eprintln!("  Deploy {deploy_id} not heard from ({reason}); asking again");
                continue;
            }
            Err(Unfollowed::Passing(reason) | Unfollowed::Lasting(reason)) => {
                return Err(Unsettled(format!(
                    "Deploy {deploy_id} could not be followed ({reason}); it may still be \
                     running: GET /api/deploys/{deploy_id} says how it ends"
                ))
                .into())
            }
        };
        failed = 0;
        if let Some(line) = progress_line(&job["progress"]).filter(|line| *line != last_progress) {
            println!("  {line}");
            last_progress = line;
        }
        match (job["status"].as_str(), job["error"].as_str()) {
            (Some("succeeded"), _) => return Ok(job["result"].clone()),
            (Some("failed"), Some(REPLICA_LOST)) => {
                let since = *lost.get_or_insert_with(|| {
                    eprintln!(
                        "  The platform lost the heartbeat of the replica running deploy \
                         {deploy_id}; following it a while longer in case it goes on"
                    );
                    Instant::now()
                });
                if since.elapsed() >= following.lost_grace {
                    return Err(Unsettled(format!(
                        "Deploy failed: {REPLICA_LOST}. It may still end: \
                         GET /api/deploys/{deploy_id} says how"
                    ))
                    .into());
                }
            }
            (Some("failed"), error) => {
                eyre::bail!("Deploy failed: {}", error.unwrap_or("no reason recorded"))
            }
            _ => lost = None,
        }
    }
}

const WITHOUT_THE_FLAG: &str = "Upgrade the platform or deploy without the flag.";

impl DeployFlags {
    fn requested(&self) -> Vec<(&'static str, &'static str)> {
        [
            (
                self.accept_drift,
                "accept_drift",
                cufflink_types::features::SCHEMA_GUARD,
            ),
            (self.purge, "purge", cufflink_types::features::SCHEMA_GUARD),
        ]
        .into_iter()
        .filter_map(|(set, flag, feature)| set.then_some((flag, feature)))
        .collect()
    }

    fn verify(&self, platform: &version::PlatformVersion) -> eyre::Result<()> {
        let lacks = |feature: &str| !platform.offers(feature);
        if self.inactive && lacks(cufflink_types::features::INACTIVE_DEPLOY) {
            eyre::bail!(
                "platform {} cannot deploy a service inactive (needs {}). {WITHOUT_THE_FLAG}",
                platform.label(),
                cufflink_types::features::INACTIVE_DEPLOY
            );
        }
        let plan = self
            .plan
            .then_some(("plan", cufflink_types::features::ASYNC_DEPLOY));
        match self
            .requested()
            .into_iter()
            .chain(plan)
            .find(|(_, feature)| lacks(feature))
        {
            Some((flag, feature)) => eyre::bail!(
                "--{} needs a platform that supports '{}'; this one does not. \
                 {WITHOUT_THE_FLAG}",
                flag.replace('_', "-"),
                feature
            ),
            None => Ok(()),
        }
    }

    fn admits(&self, mode: &ServiceMode) -> eyre::Result<()> {
        match inactive_refusal(mode).filter(|_| self.inactive) {
            Some(refusal) => eyre::bail!(refusal),
            None => Ok(()),
        }
    }

    fn write(&self, request: &mut serde_json::Value) {
        self.requested().into_iter().for_each(|(flag, _)| {
            request[flag] = serde_json::Value::Bool(true);
        });
    }
}

pub async fn run(
    allow_destructive: bool,
    commit_hash_override: Option<String>,
    env: Option<&str>,
    tenant_override: Option<&str>,
) -> eyre::Result<()> {
    let flags = DeployFlags {
        allow_destructive,
        ..DeployFlags::default()
    };
    run_with(flags, commit_hash_override, env, tenant_override).await
}

fn loaded(env: Option<&str>, tenant_override: Option<&str>) -> eyre::Result<CliConfig> {
    let mut config = CliConfig::load_with_env(env)?;
    if let Some(tenant) = tenant_override {
        config.tenant_override = Some(tenant.to_string());
    }
    if let Some(ref name) = config.env_name {
        println!("Environment: {}", name);
    }
    Ok(config)
}

pub async fn run_with(
    flags: DeployFlags,
    commit_hash_override: Option<String>,
    env: Option<&str>,
    tenant_override: Option<&str>,
) -> eyre::Result<()> {
    let config = loaded(env, tenant_override)?;

    let project = ProjectConfig::find_and_load()?;
    let mode = project
        .as_ref()
        .and_then(|p| p.service.mode.as_deref())
        .unwrap_or("rust");

    let commit_hash = commit_hash_override.or_else(detect_git_commit_hash);

    if mode == "web" {
        flags.admits(&ServiceMode::Web)?;
        let service_name = project
            .as_ref()
            .and_then(|p| p.service.name.as_deref())
            .ok_or_else(|| eyre::eyre!("Web mode requires [service].name in Cufflink.toml"))?;
        deploy_web(
            &config,
            service_name,
            project.as_ref(),
            commit_hash.as_deref(),
            env,
        )
        .await
    } else {
        let target = Target::of(&config, env, tenant_override, CliConfig::has_platform_key());
        deploy_rust(
            &config,
            flags,
            project.as_ref(),
            commit_hash.as_deref(),
            target,
        )
        .await
    }
}

fn bundle_commit<'a>(given: Option<&'a str>, built_at: Option<&'a str>) -> Option<&'a str> {
    given.or(built_at)
}

fn bundle_lines(
    path: &Path,
    package: &Package,
    cli_version: &str,
    file: &crate::bundle::File,
    given: Option<&str>,
    built_at: Option<&str>,
) -> Vec<String> {
    let commit = match (given, built_at) {
        (Some(given), Some(built_at)) if given != built_at => {
            format!("{given} (--commit-hash; the bundle was built at {built_at})")
        }
        (Some(given), None) => format!("{given} (--commit-hash; the bundle names no commit)"),
        _ => bundle_commit(given, built_at).unwrap_or("none").to_string(),
    };
    vec![
        format!(
            "Deploying {} (mode: {}) from the bundle {} (built by cufflink v{cli_version})...",
            package.manifest.name,
            package.manifest.mode.as_str(),
            path.display()
        ),
        format!("  Commit:  {commit}"),
        format!("  SHA-256: {}", file.sha256),
    ]
}

fn foreign_project(project: Option<&ProjectConfig>, service: &str) -> Option<String> {
    project
        .and_then(|project| project.foreign_to(service))
        .map(|foreign| {
            format!(
                "it is of {service}, and {}",
                foreign.refusal("the deploy would write to", "Deploy")
            )
        })
}

pub async fn run_bundle(
    flags: DeployFlags,
    path: &Path,
    commit_hash_override: Option<String>,
    env: Option<&str>,
    tenant_override: Option<&str>,
) -> eyre::Result<()> {
    let config = loaded(env, tenant_override)?;
    let project = ProjectConfig::find_and_load()?;

    let (bundle, file) = Bundle::read(path)?;
    let (cli_version, built_at) = (bundle.cli_version.clone(), bundle.commit_hash.clone());
    let package = Package::of(bundle).map_err(|why| crate::bundle::refused(path, why))?;
    if let Some(foreign) = foreign_project(project.as_ref(), &package.manifest.name) {
        return Err(crate::bundle::refused(path, foreign));
    }
    let (given, built_at) = (commit_hash_override.as_deref(), built_at.as_deref());
    bundle_lines(path, &package, &cli_version, &file, given, built_at)
        .iter()
        .for_each(|line| println!("{line}"));

    let target = Target::of(&config, env, tenant_override, CliConfig::has_platform_key());
    let to = Destination::reached(&config, flags, project.as_ref(), target).await?;
    let quiet = quiet(&config, &to.client, &flags, &package.manifest.name).await?;
    let asking = version::Asking::Bundle { cli: &cli_version };
    sent(
        &to,
        &package,
        &asking,
        bundle_commit(given, built_at),
        quiet,
    )
    .await
}

struct Emitted {
    manifest_json: serde_json::Value,
    manifest: cufflink_types::ServiceManifest,
}

fn emitted() -> eyre::Result<Emitted> {
    println!("Building service...");
    let output = Command::new("cargo")
        .args(["run", "--", "--emit-manifest"])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        eyre::bail!("Build failed:\n{}", stderr);
    }

    let stdout = String::from_utf8(output.stdout)?;
    let manifest_json: serde_json::Value = serde_json::from_str(stdout.trim())
        .map_err(|e| eyre::eyre!("Failed to parse manifest JSON: {}", e))?;
    let manifest = parsed(&manifest_json)?;
    Ok(Emitted {
        manifest_json,
        manifest,
    })
}

fn parsed(manifest_json: &serde_json::Value) -> eyre::Result<cufflink_types::ServiceManifest> {
    let manifest: cufflink_types::ServiceManifest =
        serde_json::from_value(manifest_json.clone())
            .map_err(|e| eyre::eyre!("Manifest is not a valid ServiceManifest: {}", e))?;
    verify_private_tables(&manifest)?;
    Ok(manifest)
}

fn required_features(manifest: &cufflink_types::ServiceManifest) -> Vec<String> {
    let native = matches!(manifest.mode, ServiceMode::Native);
    manifest
        .required_platform_features
        .iter()
        .cloned()
        .chain(native.then(|| cufflink_types::features::NATIVE_SERVICES.to_string()))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

struct Module {
    base64: String,
    hash: String,
}

pub(crate) struct Package {
    manifest_json: serde_json::Value,
    manifest: cufflink_types::ServiceManifest,
    wasm: Option<Module>,
    native: Option<native::NativeArtifact>,
    required: Vec<String>,
}

fn verify_sources(emitted: &Emitted) -> eyre::Result<()> {
    verify_migrations(&std::env::current_dir()?, &emitted.manifest.migrations)
}

fn built(emitted: Emitted) -> eyre::Result<Package> {
    let declared = required_features(&emitted.manifest);
    let Emitted {
        mut manifest_json,
        manifest,
    } = emitted;

    let wasm_artifact = if manifest.requires_wasm() {
        println!(
            "  WASM required ({}); building...",
            wasm_required_reason(&manifest)
        );
        let artifact = build_wasm_artifact()?;
        println!(
            "  Bundled WASM: {} bytes, hash: {}",
            artifact.size,
            &artifact.hash[..16]
        );
        Some(artifact)
    } else {
        None
    };
    let native = matches!(manifest.mode, ServiceMode::Native)
        .then(|| {
            native::build().inspect(|artifact| {
                println!(
                    "  Native binary: {} bytes, hash: {}",
                    artifact.size,
                    &artifact.hash[..16]
                )
            })
        })
        .transpose()?;
    let required = wasm_artifact.as_ref().map_or_else(
        || declared.clone(),
        |artifact| require_module_features(&mut manifest_json, &declared, &artifact.features),
    );
    Ok(Package {
        manifest_json,
        manifest,
        wasm: wasm_artifact.map(|artifact| Module {
            base64: artifact.base64,
            hash: artifact.hash,
        }),
        native,
        required,
    })
}

fn unbundled(mode: &ServiceMode) -> String {
    format!(
        "bundles carry WASM services; deploy {} services from source",
        mode.as_str()
    )
}

fn bundled(mode: &ServiceMode) -> eyre::Result<()> {
    match mode {
        ServiceMode::Crud | ServiceMode::Wasm => Ok(()),
        ServiceMode::Native | ServiceMode::Web | ServiceMode::Container => {
            eyre::bail!(unbundled(mode))
        }
    }
}

pub(crate) fn bundle_of_sources(commit_hash: Option<String>) -> eyre::Result<Bundle> {
    let project = ProjectConfig::find_and_load()?;
    if project.and_then(|project| project.service.mode).as_deref() == Some("web") {
        eyre::bail!(unbundled(&ServiceMode::Web));
    }
    let emitted = emitted()?;
    bundled(&emitted.manifest.mode)?;
    verify_sources(&emitted)?;
    println!(
        "Bundling {} (mode: {})...",
        emitted.manifest.name,
        emitted.manifest.mode.as_str()
    );
    Ok(built(emitted)?.bundle(commit_hash.or_else(detect_git_commit_hash)))
}

impl Package {
    fn bundle(self, commit_hash: Option<String>) -> Bundle {
        let (wasm_base64, wasm_hash) = self.wasm.map(|module| (module.base64, module.hash)).unzip();
        Bundle {
            service: self.manifest.name,
            mode: self.manifest.mode,
            manifest: self.manifest_json,
            wasm_base64,
            wasm_hash,
            features: self.required,
            commit_hash,
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    fn of(bundle: Bundle) -> eyre::Result<Self> {
        let manifest = parsed(&bundle.manifest)
            .map_err(|refused| eyre::eyre!("its manifest is refused: {refused}"))?;
        bundled(&manifest.mode)?;
        if (&bundle.service, &bundle.mode) != (&manifest.name, &manifest.mode) {
            eyre::bail!(
                "it says it is {} (mode: {}), and its manifest is of {} (mode: {})",
                bundle.service,
                bundle.mode.as_str(),
                manifest.name,
                manifest.mode.as_str()
            );
        }
        let required = required_features(&manifest);
        if bundle.features.iter().collect::<BTreeSet<_>>() != required.iter().collect() {
            eyre::bail!(
                "it says it needs the features {}, and its manifest requires {}",
                version::listed(&bundle.features),
                version::listed(&required)
            );
        }
        let wasm = bundle
            .wasm_base64
            .zip(bundle.wasm_hash)
            .map(|(base64, hash)| Module { base64, hash });
        match (manifest.requires_wasm(), &wasm) {
            (true, None) => eyre::bail!(
                "it carries no module, and its manifest needs one ({})",
                wasm_required_reason(&manifest)
            ),
            (false, Some(_)) => {
                eyre::bail!("it carries a module, and its manifest declares nothing that runs one")
            }
            (true, Some(_)) | (false, None) => {}
        }
        Ok(Self {
            manifest_json: bundle.manifest,
            manifest,
            wasm,
            native: None,
            required,
        })
    }
}

struct Destination<'a> {
    config: &'a CliConfig,
    client: reqwest::Client,
    platform: version::PlatformVersion,
    flags: DeployFlags,
    project: Option<&'a ProjectConfig>,
    target: Target<'a>,
}

impl<'a> Destination<'a> {
    async fn reached(
        config: &'a CliConfig,
        flags: DeployFlags,
        project: Option<&'a ProjectConfig>,
        target: Target<'a>,
    ) -> eyre::Result<Self> {
        let client = config.http_client();
        let platform = verify_platform_version(config, &client).await?;
        flags.verify(&platform)?;
        Ok(Self {
            config,
            client,
            platform,
            flags,
            project,
            target,
        })
    }
}

async fn quiet(
    config: &CliConfig,
    client: &reqwest::Client,
    flags: &DeployFlags,
    name: &str,
) -> eyre::Result<Option<NotRunning>> {
    if flags.inactive {
        not_running(config, client, name).await.map(Some)
    } else {
        Ok(None)
    }
}

async fn deploy_rust(
    config: &CliConfig,
    flags: DeployFlags,
    project: Option<&ProjectConfig>,
    commit_hash: Option<&str>,
    target: Target<'_>,
) -> eyre::Result<()> {
    let to = Destination::reached(config, flags, project, target).await?;
    let sdk = version::sdk_version();
    let asking = version::Asking::Sdk(sdk.as_deref());

    let emitted = emitted()?;
    let quiet = quiet(config, &to.client, &flags, &emitted.manifest.name).await?;
    let (negotiation, verdict) = negotiate(
        &to.platform,
        &asking,
        &emitted.manifest_json,
        &required_features(&emitted.manifest),
    );
    if verdict.is_err() {
        println!("  {negotiation}");
    }
    verdict?;
    verify_sources(&emitted)?;

    println!(
        "Deploying {} (mode: {})...",
        emitted.manifest.name,
        emitted.manifest.mode.as_str()
    );
    let package = built(emitted)?;
    sent(&to, &package, &asking, commit_hash, quiet).await
}

async fn sent(
    to: &Destination<'_>,
    package: &Package,
    asking: &version::Asking<'_>,
    commit_hash: Option<&str>,
    quiet: Option<NotRunning>,
) -> eyre::Result<()> {
    let Destination {
        config,
        client,
        platform,
        flags,
        project,
        target,
    } = to;
    let (flags, project, target) = (*flags, *project, *target);
    let Package {
        manifest_json,
        manifest,
        wasm,
        native,
        required,
    } = package;
    let env = target.env;
    let (negotiation, verdict) = negotiate(platform, asking, manifest_json, required);
    println!("  {negotiation}");
    verdict?;

    let expected_wasm_hash = wasm.as_ref().map(|module| module.hash.as_str());
    let expected_native_hash = native.as_ref().map(|artifact| artifact.hash.as_str());

    let deploy_req = deploy_request(
        manifest_json,
        &flags,
        commit_hash,
        wasm.as_ref().map(|module| module.base64.as_str()),
        native.as_ref().map(|artifact| artifact.base64.as_str()),
    );

    let deploy = Deploy {
        url: deploy_url(&config.api_url, &flags, &platform.supports),
        name: &manifest.name,
        plan: flags.plan,
        quiet,
        target,
        following: FOLLOWING,
    };
    let body = settled(config, client, &deploy, &deploy_req).await?;
    if flags.plan {
        plan_report(&body)
            .iter()
            .for_each(|line| println!("{line}"));
        return Ok(());
    }
    let service_id = service_id(&body)?.to_string();

    if body["skipped"].as_bool() == Some(true) {
        println!("{}", summary(&body, &target));
        verify_echoed(
            &body,
            "native_hash",
            "Native binary",
            expected_native_hash,
            &config.api_url,
        )?;
        maybe_sync_configs(config, &service_id, project, env, Some(platform)).await?;
        if let Some(module) = wasm {
            upload_wasm(config, client, &service_id, commit_hash, module).await?;
            kept_inactive(config, client, &deploy, &service_id).await?;
        }
        verify_deploy_ready(config, client, &service_id).await?;
        return Ok(());
    }

    verify_echoed(
        &body,
        "wasm_hash",
        "WASM",
        expected_wasm_hash,
        &config.api_url,
    )?;
    verify_echoed(
        &body,
        "native_hash",
        "Native binary",
        expected_native_hash,
        &config.api_url,
    )?;

    maybe_sync_configs(config, &service_id, project, env, Some(platform)).await?;

    println!("{}", summary(&body, &target));
    println!("  Schema changes: {}", body["schema_changes"]);
    println!("  Deployment ID:  {}", body["deployment_id"]);
    println!("  Tenant:         {}", body["tenant_slug"]);
    if let Some(hash) = body["commit_hash"].as_str() {
        println!("  Commit:         {}", hash);
    }
    if let Some(hash) = body["wasm_hash"].as_str() {
        println!("  WASM hash:      {}", &hash[..16.min(hash.len())]);
    }
    if let Some(hash) = body["native_hash"].as_str() {
        println!("  Native hash:    {}", &hash[..16.min(hash.len())]);
        native::provided_lines(manifest)
            .iter()
            .for_each(|line| println!("  Provides:       {line}"));
    }
    if let Some(ref handler) = manifest.on_migrate {
        println!("  on_migrate:     {} (ran between phases)", handler);
    }
    plan_lines(&body["plan"])
        .iter()
        .for_each(|line| println!("  {line}"));

    verify_deploy_ready(config, client, &service_id).await?;

    Ok(())
}

async fn answered(
    config: &CliConfig,
    client: &reqwest::Client,
    deploy: &Deploy<'_>,
    request: &serde_json::Value,
) -> eyre::Result<serde_json::Value> {
    let resp = config
        .auth_request(client, reqwest::Method::POST, &deploy.url)
        .json(request)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(unanswered(deploy.plan, status, body));
    }
    let body: serde_json::Value = resp.json().await?;
    match body["deploy_id"].as_str().filter(|_| !deploy.plan) {
        Some(deploy_id) => {
            println!("  Deploy {deploy_id} runs on the platform; following it");
            await_deploy(config, client, deploy_id, deploy.following).await
        }
        None => Ok(body),
    }
}

fn left_inactive(body: &serde_json::Value) -> bool {
    Status::named(&body["status"]) == Some(Status::Inactive)
}

fn service_id(body: &serde_json::Value) -> eyre::Result<&str> {
    body["service_id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No service_id in deploy response"))
}

async fn not_running(
    config: &CliConfig,
    client: &reqwest::Client,
    name: &str,
) -> eyre::Result<NotRunning> {
    match services::status_of(config, client, name).await? {
        Some(Status::Active) => eyre::bail!(active_refusal(name)),
        Some(Status::Inactive) | None => Ok(NotRunning(())),
    }
}

async fn settled(
    config: &CliConfig,
    client: &reqwest::Client,
    deploy: &Deploy<'_>,
    request: &serde_json::Value,
) -> eyre::Result<serde_json::Value> {
    let name = deploy.name;
    let disable = || deploy.target.services("disable", name);
    let body = match (
        answered(config, client, deploy, request).await,
        deploy.quiet,
    ) {
        (Ok(body), Some(NotRunning(()))) if !left_inactive(&body) => body,
        (Err(unsettled), Some(_)) if unsettled.is::<Unsettled>() => eyre::bail!(
            "{unsettled}; it was asked not to start {name}: when it ends, check the service's \
             status and, if it is active, disable it: {}",
            disable()
        ),
        (answer, _) => return answer,
    };
    if deploy.plan {
        eyre::bail!(
            "the platform planned {name} as a deploy that starts it; nothing was deployed. \
             Is a platform upgrade in progress?"
        );
    }
    if Status::named(&body["status"]) == Some(Status::Active) {
        eyre::bail!(
            "the platform deployed {name} active: it was enabled while the deploy ran, and \
             was not stopped. If it should not run: {}",
            disable()
        );
    }
    let disabled = match service_id(&body) {
        Ok(id) => services::write_status(config, client, id, Status::Inactive).await,
        Err(unnamed) => Err(unnamed),
    };
    match disabled {
        Ok(()) => eyre::bail!(
            "the platform deployed {name} active; it was disabled. \
             Is a platform upgrade in progress?"
        ),
        Err(failure) => eyre::bail!(
            "the platform deployed {name} active, and it could not be disabled ({failure}). \
             Disable it now: {}",
            disable()
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reassertion {
    Disable,
    Enabled,
    Gone,
}

impl Reassertion {
    fn of(NotRunning(()): NotRunning, listed: Option<Status>) -> Self {
        match listed {
            Some(Status::Inactive) => Self::Disable,
            Some(Status::Active) => Self::Enabled,
            None => Self::Gone,
        }
    }
}

async fn kept_inactive(
    config: &CliConfig,
    client: &reqwest::Client,
    deploy: &Deploy<'_>,
    id: &str,
) -> eyre::Result<()> {
    let Some(quiet) = deploy.quiet else {
        return Ok(());
    };
    let name = deploy.name;
    let disable = || deploy.target.services("disable", name);
    let written = match services::status_of(config, client, name)
        .await
        .map(|listed| Reassertion::of(quiet, listed))
    {
        Ok(Reassertion::Disable) => {
            services::write_status(config, client, id, Status::Inactive).await
        }
        Ok(Reassertion::Enabled) => eyre::bail!(
            "{name} was enabled while its module was uploaded, and was not stopped. \
             If it should not run: {}",
            disable()
        ),
        Ok(Reassertion::Gone) => Ok(()),
        Err(unseen) => Err(unseen),
    };
    written.map_err(|failure| {
        eyre::eyre!(
            "the module of {name} was uploaded, and the service could not be kept inactive \
             ({failure}). Disable it now: {}",
            disable()
        )
    })
}

fn summary(body: &serde_json::Value, target: &Target<'_>) -> String {
    let (name, version) = (body["name"].as_str().unwrap_or("?"), &body["version"]);
    let ended = if body["skipped"].as_bool() == Some(true) {
        format!("No schema changes for {name} v{version} — manifest unchanged")
    } else {
        format!("Deployed {name} v{version}")
    };
    if left_inactive(body) {
        format!(
            "{ended} (inactive; start it with: {})",
            target.services("enable", name)
        )
    } else {
        ended
    }
}

fn plan_report(body: &serde_json::Value) -> Vec<String> {
    let new = body["new"].as_bool() == Some(true);
    let heading = format!(
        "Plan for {} v{}{}",
        body["name"].as_str().unwrap_or("?"),
        body["version"],
        if new { " (a new service)" } else { "" }
    );
    let skipped = body["skipped"].as_bool() == Some(true);
    let would = left_inactive(body)
        .then(|| format!("status: {}", Status::Inactive.as_str()))
        .into_iter()
        .chain(skipped.then(|| "Nothing to do: manifest and code unchanged".to_string()))
        .chain(texts(&body["plan"]["schema_changes"]))
        .chain(plan_lines(&body["plan"]))
        .map(|line| format!("  {line}"));
    std::iter::once(heading).chain(would).collect()
}

fn deploy_request(
    manifest_json: &serde_json::Value,
    flags: &DeployFlags,
    commit_hash: Option<&str>,
    wasm_base64: Option<&str>,
    native_base64: Option<&str>,
) -> serde_json::Value {
    let mut request = serde_json::json!({
        "manifest": manifest_json,
        "allow_destructive": flags.allow_destructive,
        "commit_hash": commit_hash,
        "wasm_base64": wasm_base64,
        "cli_version": env!("CARGO_PKG_VERSION"),
        "api_version": CLI_DEPLOY_API_VERSION,
    });
    if let Some(binary) = native_base64 {
        request["native_base64"] = serde_json::Value::String(binary.to_string());
    }
    flags.write(&mut request);
    request
}

fn verify_echoed(
    body: &serde_json::Value,
    field: &str,
    what: &str,
    expected: Option<&str>,
    api_url: &str,
) -> eyre::Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    match body[field].as_str() {
        Some(stored) if stored == expected => Ok(()),
        Some(stored) => eyre::bail!(
            "{what} hash mismatch: CLI built {} but platform stored {}. \
             This indicates corruption somewhere in the upload path.",
            &expected[..16.min(expected.len())],
            &stored[..16.min(stored.len())]
        ),
        None => eyre::bail!(
            "Platform did not echo {field} in the deploy response. \
             This usually means the platform is older than expected; \
             check `curl {api_url}/api/version`."
        ),
    }
}

const NOT_SOURCES: [&str; 3] = ["target", "node_modules", ".git"];
const MIGRATIONS_DEPTH: usize = 3;

fn directories(root: &Path, depth: usize) -> Vec<PathBuf> {
    let mut below: Vec<PathBuf> = std::fs::read_dir(root)
        .into_iter()
        .filter(|_| depth > 0)
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && !path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| NOT_SOURCES.contains(&name))
        })
        .collect();
    below.sort();
    std::iter::once(root.to_path_buf())
        .chain(below.iter().flat_map(|dir| directories(dir, depth - 1)))
        .collect()
}

fn holds(dir: &Path, migration: &cufflink_types::SqlMigration) -> bool {
    let named = format!("__{}.sql", migration.name);
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .any(|file| {
            file.strip_prefix('V')
                .and_then(|rest| rest.strip_suffix(named.as_str()))
                .is_some_and(|digits| {
                    digits.bytes().all(|b| b.is_ascii_digit())
                        && digits.parse::<u32>() == Ok(migration.version)
                })
        })
}

fn verify_migrations(root: &Path, built: &[cufflink_types::SqlMigration]) -> eyre::Result<()> {
    let Some(first) = built.first() else {
        return Ok(());
    };
    let mut candidates: Vec<PathBuf> = directories(root, MIGRATIONS_DEPTH)
        .into_iter()
        .filter(|dir| holds(dir, first))
        .collect();
    candidates.sort_by_key(|dir| dir.components().count());
    let scanned: Vec<(&PathBuf, Result<Vec<_>, _>)> = candidates
        .iter()
        .map(|dir| (dir, cufflink_migrate::scan_dir(dir)))
        .collect();
    if scanned
        .iter()
        .any(|(_, on_disk)| on_disk.as_deref() == Ok(built))
    {
        return Ok(());
    }
    match scanned.into_iter().next() {
        None => eyre::bail!(
            "the manifest has SQL migrations, and {}.sql is in no directory under {}; \
             deploy from the service's directory",
            first.file_stem(),
            root.display()
        ),
        Some((dir, Err(e))) => eyre::bail!("migrations in {}: {e}", dir.display()),
        Some((dir, Ok(on_disk))) => eyre::bail!(
            "migrations directory changed since the last build ({} holds {}, the manifest {}); \
             run `cargo clean -p <crate>` or touch src/main.rs",
            dir.display(),
            stems(&on_disk),
            stems(built)
        ),
    }
}

fn stems(migrations: &[cufflink_types::SqlMigration]) -> String {
    migrations
        .iter()
        .map(cufflink_types::SqlMigration::file_stem)
        .collect::<Vec<_>>()
        .join(", ")
}

fn migration_names(list: &serde_json::Value) -> Vec<String> {
    list.as_array()
        .into_iter()
        .flatten()
        .map(|m| {
            format!(
                "V{}__{} ({})",
                m["version"],
                m["name"].as_str().unwrap_or("?"),
                m["checksum"]
            )
        })
        .collect()
}

pub(crate) const PRIVATE_TABLES: &str = "Private tables (checked, not served)";

fn verify_private_tables(manifest: &cufflink_types::ServiceManifest) -> eyre::Result<()> {
    manifest
        .private_tables
        .iter()
        .try_for_each(cufflink_types::manifest::validate::validate_private_table)
        .map_err(|refusal| eyre::eyre!(refusal))
}

fn texts(list: &serde_json::Value) -> Vec<String> {
    list.as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

fn plan_lines(plan: &serde_json::Value) -> Vec<String> {
    let guard = plan["schema_guard"].as_str();
    let drift = plan["drift"].as_array().into_iter().flatten().map(|d| {
        let at = [&d["table"], &d["column"], &d["index"]]
            .into_iter()
            .filter_map(|part| part.as_str())
            .collect::<Vec<_>>()
            .join(".");
        format!("drift: {} {at}", d["kind"].as_str().unwrap_or("differs"))
    });
    let dependents = plan["dependents"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|d| {
            format!(
                "unmanaged: {} {} depends on {}",
                d["kind"].as_str().unwrap_or("object").replace('_', " "),
                d["name"].as_str().unwrap_or("?"),
                d["on"].as_str().unwrap_or("?")
            )
        });
    let migrations = &plan["sql_migrations"];
    let history = migrations["history"].as_str().map(|history| {
        format!(
            "SQL migrations: {history} ({}, {} verified)",
            migrations["mode"].as_str().unwrap_or("apply"),
            migrations["verified"]
        )
    });
    let private = texts(&plan["private_tables"]);
    let heading = (!private.is_empty()).then(|| format!("{PRIVATE_TABLES}:"));
    heading
        .into_iter()
        .chain(private.into_iter().map(|table| format!("  {table}")))
        .chain(
            texts(&plan["no_longer_served"])
                .into_iter()
                .map(|table| format!("table {table}: no longer served")),
        )
        .chain(history)
        .chain(
            migration_names(&migrations["applied"])
                .into_iter()
                .map(|m| format!("applied: {m}")),
        )
        .chain(
            migration_names(&migrations["pending"])
                .into_iter()
                .map(|m| format!("pending: {m}")),
        )
        .chain(guard.map(|mode| format!("Schema guard:   {mode}")))
        .chain(
            texts(&plan["satisfied"])
                .into_iter()
                .map(|change| format!("already there: {change}")),
        )
        .chain(drift)
        .chain(dependents)
        .chain(
            texts(&plan["extra_columns"])
                .into_iter()
                .map(|column| format!("not declared: {column}")),
        )
        .chain(
            texts(&plan["warnings"])
                .into_iter()
                .map(|warning| format!("warning: {warning}")),
        )
        .collect()
}

async fn verify_platform_version(
    config: &CliConfig,
    client: &reqwest::Client,
) -> eyre::Result<version::PlatformVersion> {
    let info = match version::asked(config, client).await {
        Err(version::Unanswered::Refused { status, .. })
            if status == reqwest::StatusCode::NOT_FOUND =>
        {
            eyre::bail!(
                "Platform at {} does not expose /api/version. \
                 This CLI (v{}) requires a platform that supports deploy api_version >= {}. \
                 Upgrade the platform image before deploying.",
                config.api_url,
                env!("CARGO_PKG_VERSION"),
                CLI_DEPLOY_API_VERSION
            )
        }
        answer => answer?,
    };
    let platform_api = info.api_version;
    if platform_api < CLI_DEPLOY_API_VERSION {
        eyre::bail!(
            "Platform deploy api_version is {} but this CLI (v{}) requires >= {}. \
             Upgrade the platform image before deploying.",
            platform_api,
            env!("CARGO_PKG_VERSION"),
            CLI_DEPLOY_API_VERSION
        );
    }
    Ok(info)
}

fn negotiate(
    platform: &version::PlatformVersion,
    asking: &version::Asking<'_>,
    manifest_json: &serde_json::Value,
    required: &[String],
) -> (String, eyre::Result<()>) {
    (
        version::negotiated(platform, asking, manifest_json),
        verify_required_features(required, &platform.supports),
    )
}

fn verify_required_features(required: &[String], supports: &[String]) -> eyre::Result<()> {
    let missing: Vec<&String> = required
        .iter()
        .filter(|f| !supports.iter().any(|s| s == *f))
        .collect();
    if !missing.is_empty() {
        eyre::bail!(
            "This service requires platform capabilities the target does not advertise: {}. \
             Upgrade the platform image before deploying (it advertises: {}).",
            missing
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            if supports.is_empty() {
                "none".to_string()
            } else {
                supports.join(", ")
            }
        );
    }
    Ok(())
}

fn require_module_features(
    manifest_json: &mut serde_json::Value,
    required: &[String],
    declared: &BTreeSet<String>,
) -> Vec<String> {
    if declared.is_empty() {
        return required.to_vec();
    }
    let union: Vec<String> = required
        .iter()
        .chain(declared)
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if let Some(manifest) = manifest_json.as_object_mut() {
        manifest.insert(
            "required_platform_features".into(),
            serde_json::json!(union),
        );
    }
    union
}

async fn verify_deploy_ready(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
) -> eyre::Result<()> {
    let url = format!("{}/api/services/{}/wasm/status", config.api_url, service_id);

    let delays = [
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::from_secs(4),
        Duration::from_secs(8),
        Duration::from_secs(14),
    ];

    let mut last_body = String::new();
    for delay in delays {
        let resp = config
            .auth_request(client, reqwest::Method::GET, &url)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eyre::bail!(
                "Deploy verification failed ({} on wasm/status): {}",
                status,
                body
            );
        }
        let body: serde_json::Value = resp.json().await?;
        last_body = serde_json::to_string(&body).unwrap_or_default();

        let is_wasm_service = body["wasm_hash"]
            .as_str()
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !is_wasm_service {
            return Ok(());
        }

        if body["db_state"].as_bool() == Some(true) {
            println!(
                "  Deploy verified: db_state=true, local_cache={}",
                body["local_cache"].as_bool().unwrap_or(false)
            );
            return Ok(());
        }

        tokio::time::sleep(delay).await;
    }

    eyre::bail!(
        "Deploy verification timed out: wasm/status never reported db_state=true \
         within 30s. Last status body: {}. The platform accepted the deploy but \
         the WASM binary is not durably persisted — this is a bug. Check platform \
         logs (`cufflink_deployment_wasm_missing_total` metric) and retry.",
        last_body
    );
}

async fn deploy_web(
    config: &CliConfig,
    service_name: &str,
    project: Option<&ProjectConfig>,
    commit_hash: Option<&str>,
    env: Option<&str>,
) -> eyre::Result<()> {
    println!("Deploying web app: {}", service_name);

    let pkg_json = read_package_json()?;
    let scripts = pkg_json["scripts"].as_object();

    if let Some(s) = scripts {
        if s.contains_key("typecheck") {
            run_npm_script("typecheck", "Type checking")?;
        }
        if s.contains_key("lint") {
            run_npm_script("lint", "Linting")?;
        }
        if s.contains_key("test") {
            run_npm_script("test", "Running tests")?;
        }
    }

    let version = pkg_json["version"].as_str().unwrap_or("0.1.0");
    let manifest = serde_json::json!({
        "name": service_name,
        "mode": "web",
        "version": version,
        "tables": [],
    });

    let deploy_req = serde_json::json!({
        "manifest": manifest,
        "allow_destructive": false,
        "commit_hash": commit_hash,
    });

    let client = config.http_client();
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/deploy", config.api_url),
        )
        .json(&deploy_req)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Deploy failed ({}): {}", status, body);
    }

    let body: serde_json::Value = resp.json().await?;
    let service_id = body["service_id"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("No service_id in deploy response"))?;

    println!(
        "Registered {} v{}",
        body["name"].as_str().unwrap_or("?"),
        body["version"]
    );
    println!("  Deployment ID: {}", body["deployment_id"]);
    println!("  Tenant:        {}", body["tenant_slug"]);
    if let Some(hash) = body["commit_hash"].as_str() {
        println!("  Commit:        {}", hash);
    }

    maybe_sync_configs(config, service_id, project, env, None).await?;

    println!("Resolving service configs...");
    let local_envs = resolve_local_build_envs(project, env)?;
    let mut build_envs: HashMap<String, String> = fetch_service_configs(config, service_name)
        .await
        .into_iter()
        .collect();
    build_envs.extend(local_envs);

    if !build_envs.is_empty() {
        println!(
            "  Injecting {} service configs into build",
            build_envs.len()
        );
    }

    check_pnpm_workspace()?;

    println!("Building Next.js app...");
    let pm = detect_package_manager();
    let mut build_cmd = Command::new(&pm);
    build_cmd.args(["run", "build"]);
    for (key, value) in &build_envs {
        build_cmd.env(key, value);
    }
    let build_output = build_cmd.output()?;

    if !build_output.status.success() {
        eyre::bail!("Build failed:\n{}", format_command_output(&build_output));
    }
    println!("  Build complete");

    if !Path::new(".next").exists() {
        eyre::bail!("Next.js build output not found at .next/");
    }

    println!("Creating deployment artifact...");
    let tarball = create_web_tarball()?;
    let tarball_size = tarball.len();
    println!(
        "  Artifact size: {} bytes ({:.1} MB)",
        tarball_size,
        tarball_size as f64 / 1_048_576.0
    );

    println!("Uploading web artifact...");
    let resp = config
        .auth_request(
            &client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/web", config.api_url, service_id),
        )
        .header("Content-Type", "application/gzip")
        .body(tarball)
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await?;
        println!(
            "  Artifact hash: {}",
            body["artifact_hash"].as_str().unwrap_or("?")
        );
        println!("  Size:          {} bytes", body["size_bytes"]);
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("Web artifact upload failed ({}): {}", status, body);
    }

    println!("\nWeb app deployed successfully!");
    Ok(())
}

fn check_pnpm_workspace() -> eyre::Result<()> {
    use crate::pnpm::{check_workspace_membership, WorkspaceCheck};

    let cwd = std::env::current_dir()?;
    if let WorkspaceCheck::NotIncluded {
        workspace_file,
        suggested_glob,
        ..
    } = check_workspace_membership(&cwd)
    {
        eyre::bail!(
            "This package is inside a pnpm workspace but is not listed in {}\n\n\
             pnpm install will not have installed dependencies for this package,\n\
             so the build will fail.\n\n\
             Add \"{}\" to the packages list in {} and run pnpm install",
            workspace_file.display(),
            suggested_glob,
            workspace_file.display(),
        );
    }
    Ok(())
}

fn read_package_json() -> eyre::Result<serde_json::Value> {
    let content = std::fs::read_to_string("package.json")
        .map_err(|_| eyre::eyre!("package.json not found in current directory"))?;
    serde_json::from_str(&content).map_err(|e| eyre::eyre!("Failed to parse package.json: {}", e))
}

fn run_npm_script(script: &str, label: &str) -> eyre::Result<()> {
    println!("{}...", label);
    let pm = detect_package_manager();
    let output = Command::new(&pm).args(["run", script]).output()?;

    if !output.status.success() {
        eyre::bail!("{} failed:\n{}", label, format_command_output(&output));
    }
    println!("  {} passed", label);
    Ok(())
}

fn detect_package_manager() -> String {
    let mut dir = std::env::current_dir().unwrap_or_default();
    loop {
        if dir.join("pnpm-lock.yaml").exists() {
            return "pnpm".to_string();
        }
        if dir.join("yarn.lock").exists() {
            return "yarn".to_string();
        }
        if dir.join("bun.lockb").exists() || dir.join("bun.lock").exists() {
            return "bun".to_string();
        }
        if dir.join("package-lock.json").exists() {
            return "npm".to_string();
        }
        if !dir.pop() {
            break;
        }
    }
    "npm".to_string()
}

fn format_command_output(output: &std::process::Output) -> String {
    [&output.stdout, &output.stderr]
        .into_iter()
        .map(|bytes| String::from_utf8_lossy(bytes).trim().to_owned())
        .filter(|stream| !stream.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn create_web_tarball() -> eyre::Result<Vec<u8>> {
    let cwd = std::env::current_dir()?;
    let nm_dir = resolve_node_modules()?;

    let staging = tempfile::tempdir()?;
    let stage = staging.path();

    let next_dir = cwd.join(".next");
    if next_dir.exists() {
        std::os::unix::fs::symlink(&next_dir, stage.join(".next"))?;
    }

    std::os::unix::fs::symlink(&nm_dir, stage.join("node_modules"))?;

    std::os::unix::fs::symlink(cwd.join("package.json"), stage.join("package.json"))?;

    let public_dir = cwd.join("public");
    let has_public = public_dir.exists();
    if has_public {
        std::os::unix::fs::symlink(&public_dir, stage.join("public"))?;
    }

    let output = Command::new("tar")
        .args(web_tarball_args(has_public))
        .current_dir(stage)
        .output()?;

    if !output.status.success() {
        eyre::bail!("tar command failed:\n{}", format_command_output(&output));
    }

    if let Some(parent) = nm_dir.parent() {
        if parent.starts_with(std::env::temp_dir()) {
            let _ = std::fs::remove_dir_all(parent);
        }
    }

    Ok(output.stdout)
}

fn web_tarball_args(has_public: bool) -> impl Iterator<Item = &'static str> {
    [
        "czfh",
        "-",
        "--exclude=.next/cache",
        ".next",
        "node_modules",
        "package.json",
    ]
    .into_iter()
    .chain(has_public.then_some("public"))
}

fn resolve_node_modules() -> eyre::Result<std::path::PathBuf> {
    if let Some(root) = find_pnpm_workspace_root() {
        println!("  Detected pnpm monorepo, creating pruned node_modules...");
        match pnpm_deploy_node_modules(&root) {
            Ok(node_modules) => return Ok(node_modules),
            Err(err) => eprintln!("  {err}\n  Falling back to nearest node_modules..."),
        }
    }

    find_node_modules()
}

fn pnpm_deploy_node_modules(root: &Path) -> eyre::Result<std::path::PathBuf> {
    let pkg_name = read_package_name()?;
    let deploy_dir = tempfile::tempdir()?.keep();

    let output = Command::new("pnpm")
        .args([
            "--filter",
            &pkg_name,
            "deploy",
            "--prod",
            &deploy_dir.display().to_string(),
        ])
        .current_dir(root)
        .output()?;

    let node_modules = deploy_dir.join("node_modules");
    if output.status.success() && node_modules.join(REQUIRED_PACKAGE).exists() {
        println!("  Pruned node_modules ready");
        return Ok(node_modules);
    }

    let _ = std::fs::remove_dir_all(&deploy_dir);
    if output.status.success() {
        eyre::bail!("pnpm deploy succeeded but produced no `{REQUIRED_PACKAGE}` package");
    }
    eyre::bail!("pnpm deploy failed:\n{}", format_command_output(&output));
}

fn read_package_name() -> eyre::Result<String> {
    read_package_json()?
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| eyre::eyre!("package.json missing 'name' field"))
}

fn find_pnpm_workspace_root() -> Option<std::path::PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join("pnpm-lock.yaml").exists() {
            return Some(dir);
        }
        if !dir.pop() {
            break;
        }
    }
    None
}

fn find_node_modules() -> eyre::Result<std::path::PathBuf> {
    find_node_modules_from(&std::env::current_dir()?)
}

fn find_node_modules_from(start: &Path) -> eyre::Result<std::path::PathBuf> {
    let mut dir = start.to_path_buf();
    loop {
        let candidate = dir.join("node_modules");
        if candidate.join(REQUIRED_PACKAGE).exists() {
            return Ok(candidate);
        }
        if !dir.pop() {
            break;
        }
    }
    eyre::bail!(
        "No node_modules containing `{REQUIRED_PACKAGE}` found in any ancestor of {}.\n\
         Run a dependency install (e.g. `pnpm install`) before deploying.",
        start.display()
    )
}

fn resolve_local_build_envs(
    project: Option<&ProjectConfig>,
    env: Option<&str>,
) -> eyre::Result<HashMap<String, String>> {
    let project = match project {
        Some(p) => p,
        None => return Ok(HashMap::new()),
    };
    let Some(env_name) = project.env_name(env) else {
        return Ok(HashMap::new());
    };
    super::config_cmd::resolve_local_envs(project, &env_name)
}

async fn maybe_sync_configs(
    config: &CliConfig,
    service_id: &str,
    project: Option<&ProjectConfig>,
    env: Option<&str>,
    platform: Option<&version::PlatformVersion>,
) -> eyre::Result<()> {
    let project = match project {
        Some(p) => p,
        None => return Ok(()),
    };

    let Some(env_name) = project.env_name(env) else {
        return Ok(());
    };

    let env_config = match project.environments.get(&env_name) {
        Some(c) => c,
        None => return Ok(()),
    };

    if env_config.config.is_empty() && env_config.secrets.is_empty() {
        return Ok(());
    }

    println!("Syncing configs...");
    let asked;
    let platform = match platform {
        Some(platform) => platform,
        None => {
            asked = version::asked(config, &config.http_client()).await?;
            &asked
        }
    };
    super::config_cmd::sync_to_platform(config, service_id, project, &env_name, platform).await
}

async fn fetch_service_configs(config: &CliConfig, service_name: &str) -> Vec<(String, String)> {
    let client = config.http_client();

    let resp = match config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services", config.api_url),
        )
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };

    let services: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let service_id = match services["services"].as_array().and_then(|arr| {
        arr.iter()
            .find(|s| s["name"].as_str() == Some(service_name))
            .and_then(|s| s["id"].as_str())
    }) {
        Some(id) => id.to_string(),
        None => return Vec::new(),
    };

    let resp = match config
        .auth_request(
            &client,
            reqwest::Method::GET,
            &format!("{}/api/services/{}/config", config.api_url, service_id),
        )
        .send()
        .await
    {
        Ok(r) if r.status().is_success() => r,
        _ => return Vec::new(),
    };

    let body: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    body["configs"]
        .as_array()
        .map(|configs| {
            configs
                .iter()
                .filter_map(|c| {
                    let key = c["key"].as_str()?;
                    let value = c["value"].as_str()?;
                    Some((key.to_string(), value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn detect_git_commit_hash() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

struct WasmArtifact {
    base64: String,
    size: usize,
    hash: String,
    features: BTreeSet<String>,
}

fn wasm_required_reason(manifest: &cufflink_types::ServiceManifest) -> String {
    use cufflink_types::HandlerType;
    match manifest.mode {
        ServiceMode::Wasm => return "mode: wasm".into(),
        ServiceMode::Crud | ServiceMode::Container | ServiceMode::Web | ServiceMode::Native => {}
    }
    if manifest.on_migrate.is_some() {
        return "on_migrate hook declared".into();
    }
    if let Some(t) = manifest.tables.iter().find(|t| t.hooks.is_some()) {
        return format!("table '{}' declares CRUD hooks", t.name);
    }
    if !manifest.custom_routes.is_empty() {
        return "custom_routes declared".into();
    }
    if manifest
        .subscriptions
        .iter()
        .any(|s| matches!(s.handler_type, HandlerType::Wasm))
    {
        return "wasm subscription declared".into();
    }
    "user code declared".into()
}

fn build_wasm_artifact() -> eyre::Result<WasmArtifact> {
    let rustup_check = Command::new("rustup")
        .args(["target", "list", "--installed"])
        .output()?;
    let installed_targets = String::from_utf8_lossy(&rustup_check.stdout);
    if !installed_targets.contains("wasm32-unknown-unknown") {
        eyre::bail!("WASM target not installed. Run:\n  rustup target add wasm32-unknown-unknown");
    }

    println!("Building WASM module...");
    let build_output = Command::new("cargo")
        .args([
            "build",
            "--lib",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
        ])
        .output()?;

    if !build_output.status.success() {
        let stderr = String::from_utf8_lossy(&build_output.stderr);
        eyre::bail!("WASM build failed:\n{}", stderr);
    }

    let wasm_path = find_wasm_artifact()?;
    let wasm_bytes = std::fs::read(&wasm_path)?;
    let size = wasm_bytes.len();
    println!("  WASM artifact: {} ({} bytes)", wasm_path.display(), size);

    let hash = crate::digest::sha256_hex(&wasm_bytes);

    let features = cufflink_types::wasm_features::declared(&wasm_bytes)
        .map_err(|e| eyre::eyre!("{}: {e}", wasm_path.display()))?;

    use base64::Engine as _;
    let base64 = base64::engine::general_purpose::STANDARD.encode(&wasm_bytes);

    Ok(WasmArtifact {
        base64,
        size,
        hash,
        features,
    })
}

async fn upload_wasm(
    config: &CliConfig,
    client: &reqwest::Client,
    service_id: &str,
    commit_hash: Option<&str>,
    module: &Module,
) -> eyre::Result<()> {
    let expected_hash = module.hash.as_str();
    let upload_req = serde_json::json!({
        "wasm_base64": module.base64,
        "commit_hash": commit_hash,
    });

    println!("Uploading WASM module...");
    let resp = config
        .auth_request(
            client,
            reqwest::Method::POST,
            &format!("{}/api/services/{}/wasm", config.api_url, service_id),
        )
        .json(&upload_req)
        .send()
        .await?;

    if resp.status().is_success() {
        let body: serde_json::Value = resp.json().await?;
        let echoed = body["wasm_hash"].as_str().unwrap_or("");
        println!("  WASM hash:  {}", echoed);
        println!("  Size:       {} bytes", body["size_bytes"]);
        println!("{}", upload_outcome(&body));
        if echoed != expected_hash {
            eyre::bail!(
                "WASM hash mismatch on upload: CLI built {} but platform stored {}",
                &expected_hash[..16.min(expected_hash.len())],
                &echoed[..16.min(echoed.len())]
            );
        }
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        eyre::bail!("WASM upload failed ({}): {}", status, body);
    }

    Ok(())
}

fn upload_outcome(body: &serde_json::Value) -> String {
    match (
        body["unchanged"].as_bool(),
        body["previous_version"].as_i64(),
    ) {
        (Some(true), _) if body["restored"].as_bool() == Some(true) => format!(
            "  Version:    v{} (module unchanged, restored to storage)",
            body["version"]
        ),
        (Some(true), _) => format!("  Version:    v{} (module unchanged)", body["version"]),
        (_, Some(previous)) => format!("  Version:    v{} (was v{previous})", body["version"]),
        _ => format!("  Version:    v{}", body["version"]),
    }
}

fn find_wasm_artifact() -> eyre::Result<std::path::PathBuf> {
    let metadata_output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()?;

    if !metadata_output.status.success() {
        eyre::bail!("Failed to run cargo metadata");
    }

    let metadata: serde_json::Value = serde_json::from_slice(&metadata_output.stdout)?;
    let target_dir = metadata["target_directory"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("Cannot determine target directory"))?;

    let cwd = std::env::current_dir()?;
    let cwd_str = cwd.to_string_lossy();

    let crate_name = metadata["packages"]
        .as_array()
        .and_then(|pkgs| {
            pkgs.iter().find(|p| {
                p["manifest_path"]
                    .as_str()
                    .map(|mp| mp.starts_with(cwd_str.as_ref()))
                    .unwrap_or(false)
            })
        })
        .and_then(|pkg| {
            pkg["targets"]
                .as_array()
                .and_then(|targets| {
                    targets.iter().find(|t| {
                        t["crate_types"]
                            .as_array()
                            .map(|types| types.iter().any(|ct| ct.as_str() == Some("cdylib")))
                            .unwrap_or(false)
                    })
                })
                .and_then(|t| t["name"].as_str())
                .or_else(|| pkg["name"].as_str())
        })
        .unwrap_or("unknown");

    let wasm_name = crate_name.replace('-', "_");
    let wasm_path = std::path::PathBuf::from(target_dir)
        .join("wasm32-unknown-unknown")
        .join("release")
        .join(format!("{}.wasm", wasm_name));

    if wasm_path.exists() {
        return Ok(wasm_path);
    }

    eyre::bail!(
        "WASM artifact not found at {:?}.\n\
         Ensure your Cargo.toml has:\n  \
         [lib]\n  \
         crate-type = [\"cdylib\"]",
        wasm_path
    )
}

#[cfg(test)]
mod tests {
    use super::super::stub::answering;
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};

    const MODULE: &[u8] = b"\0asm\x01\0\0\0";

    fn bundle_of(manifest: serde_json::Value) -> Bundle {
        use base64::Engine as _;
        let features = |manifest: &serde_json::Value| {
            manifest["required_platform_features"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|feature| feature.as_str().map(str::to_string))
                .collect()
        };
        Bundle {
            service: manifest["name"].as_str().unwrap_or_default().to_string(),
            mode: serde_json::from_value(manifest["mode"].clone()).unwrap_or_default(),
            features: features(&manifest),
            manifest,
            wasm_base64: Some(base64::engine::general_purpose::STANDARD.encode(MODULE)),
            wasm_hash: Some(crate::digest::sha256_hex(MODULE)),
            commit_hash: Some("4f2a9c1".into()),
            cli_version: "0.21.0".into(),
        }
    }

    fn loans() -> serde_json::Value {
        serde_json::json!({
            "name": "loans", "mode": "wasm", "tables": [],
            "required_platform_features": ["db_v2", "route_posture"],
        })
    }

    fn without_module(bundle: Bundle) -> Bundle {
        Bundle {
            wasm_base64: None,
            wasm_hash: None,
            ..bundle
        }
    }

    #[test]
    fn a_bundle_carries_a_service_whose_code_is_a_module_or_that_has_none() {
        assert!(bundled(&ServiceMode::Wasm).is_ok());
        assert!(bundled(&ServiceMode::Crud).is_ok());
        for (mode, named) in [
            (ServiceMode::Native, "native"),
            (ServiceMode::Web, "web"),
            (ServiceMode::Container, "container"),
        ] {
            assert_eq!(
                bundled(&mode).expect_err("no bundle").to_string(),
                format!("bundles carry WASM services; deploy {named} services from source")
            );
        }
    }

    #[test]
    fn a_package_and_its_bundle_are_one_service() {
        let bundle = bundle_of(loans());
        let package = Package::of(bundle.clone()).expect("a package");
        assert_eq!(package.manifest_json, loans());
        assert_eq!(package.manifest.name, "loans");
        assert_eq!(package.required, ["db_v2", "route_posture"]);
        assert!(package.native.is_none());
        assert_eq!(
            package.wasm.as_ref().map(|module| module.hash.clone()),
            Some(crate::digest::sha256_hex(MODULE))
        );
        assert_eq!(
            package.bundle(Some("4f2a9c1".into())),
            Bundle {
                cli_version: env!("CARGO_PKG_VERSION").into(),
                ..bundle
            },
            "a bundle read and written again is the bundle it was"
        );

        let crud = without_module(bundle_of(
            serde_json::json!({"name": "todos", "mode": "crud", "tables": []}),
        ));
        assert!(Package::of(crud).expect("a package").wasm.is_none());
    }

    #[test]
    fn a_bundle_is_checked_as_what_was_just_built_and_against_what_it_says_of_itself() {
        let refused = |bundle: Bundle| match Package::of(bundle) {
            Ok(_) => panic!("a bundle that is not deployed"),
            Err(refused) => refused.to_string(),
        };
        assert_eq!(
            refused(Bundle {
                service: "other".into(),
                ..bundle_of(loans())
            }),
            "it says it is other (mode: wasm), and its manifest is of loans (mode: wasm)"
        );
        assert_eq!(
            refused(Bundle {
                features: vec!["db_v2".into()],
                ..bundle_of(loans())
            }),
            "it says it needs the features db_v2, and its manifest requires db_v2, route_posture",
            "the feature check of a deploy reads the manifest, so the two must agree"
        );
        assert_eq!(
            refused(without_module(bundle_of(loans()))),
            "it carries no module, and its manifest needs one (mode: wasm)"
        );
        assert_eq!(
            refused(bundle_of(
                serde_json::json!({"name": "todos", "mode": "crud", "tables": []})
            )),
            "it carries a module, and its manifest declares nothing that runs one"
        );
        assert_eq!(
            refused(bundle_of(
                serde_json::json!({"name": "ext", "mode": "native", "tables": []})
            )),
            "bundles carry WASM services; deploy native services from source"
        );
        assert_eq!(
            refused(bundle_of(serde_json::json!({
                "name": "loans", "mode": "wasm", "schema": "adopted", "tables": [],
                "private_tables": [{"name": "labels", "permission_area": "labels",
                                    "columns": [{"name": "id", "column_type": "uuid"}]}],
            }))),
            "its manifest is refused: Private table 'labels' declares permission_area, which \
             only data routes read",
            "a private table is checked before a bundle is sent, as before one is built"
        );
        assert!(refused(bundle_of(serde_json::json!({"tables": 7})))
            .starts_with("its manifest is refused: Manifest is not a valid ServiceManifest"));
    }

    #[test]
    fn a_bundle_is_not_deployed_from_the_project_of_another_service() {
        let project = |name: Option<&str>| ProjectConfig {
            service: crate::project_config::ServiceConfig {
                name: name.map(str::to_string),
                ..Default::default()
            },
            environments: HashMap::new(),
            project_dir: PathBuf::new(),
        };
        assert_eq!(foreign_project(None, "loans"), None);
        assert_eq!(foreign_project(Some(&project(None)), "loans"), None);
        assert_eq!(
            foreign_project(Some(&project(Some("loans"))), "loans"),
            None
        );
        assert_eq!(
            foreign_project(Some(&project(Some("site"))), "loans").as_deref(),
            Some(
                "it is of loans, and the Cufflink.toml here is of site, whose config the \
                 deploy would write to loans. Deploy it where Cufflink.toml is of loans, or \
                 names no service"
            )
        );
    }

    #[test]
    fn a_deploy_of_a_bundle_says_which_file_and_which_commit() {
        let package = Package::of(bundle_of(loans())).expect("a package");
        let file = crate::bundle::File {
            size: 512,
            sha256: "9e8d7c".into(),
        };
        let lines = |given, built_at| {
            bundle_lines(
                Path::new("out/loans.bundle"),
                &package,
                "0.21.0",
                &file,
                given,
                built_at,
            )
        };
        assert_eq!(
            lines(None, Some("4f2a9c1")),
            [
                "Deploying loans (mode: wasm) from the bundle out/loans.bundle (built by \
                 cufflink v0.21.0)...",
                "  Commit:  4f2a9c1",
                "  SHA-256: 9e8d7c",
            ]
        );
        assert_eq!(
            lines(Some("4f2a9c1"), Some("4f2a9c1"))[1],
            "  Commit:  4f2a9c1"
        );
        assert_eq!(
            lines(Some("aaaaaaa"), Some("4f2a9c1"))[1],
            "  Commit:  aaaaaaa (--commit-hash; the bundle was built at 4f2a9c1)"
        );
        assert_eq!(
            lines(Some("aaaaaaa"), None)[1],
            "  Commit:  aaaaaaa (--commit-hash; the bundle names no commit)"
        );
        assert_eq!(lines(None, None)[1], "  Commit:  none");
        assert_eq!(
            bundle_commit(Some("aaaaaaa"), Some("4f2a9c1")),
            Some("aaaaaaa")
        );
        assert_eq!(bundle_commit(None, Some("4f2a9c1")), Some("4f2a9c1"));
        assert_eq!(bundle_commit(None, None), None);
    }

    fn destination<'a>(config: &'a CliConfig, supports: &[&str]) -> Destination<'a> {
        Destination {
            config,
            client: config.http_client(),
            platform: platform(supports),
            flags: DeployFlags::default(),
            project: None,
            target: Target::default(),
        }
    }

    #[tokio::test]
    async fn a_package_is_sent_as_it_is_and_a_skipped_deploy_uploads_its_module() {
        let package = Package::of(bundle_of(loans())).expect("a package");
        let asking = version::Asking::Bundle { cli: "0.21.0" };
        let hash = crate::digest::sha256_hex(MODULE);
        let (config, asked) = answering(vec![
            Some((
                200,
                serde_json::json!({"skipped": true, "service_id": "7", "name": "loans",
                                   "version": 3}),
            )),
            Some((
                200,
                serde_json::json!({"wasm_hash": hash, "size_bytes": 8, "version": 3,
                                   "unchanged": true}),
            )),
            Some((
                200,
                serde_json::json!({"wasm_hash": hash, "db_state": true}),
            )),
        ])
        .await;

        sent(
            &destination(&config, &["db_v2", "route_posture"]),
            &package,
            &asking,
            Some("4f2a9c1"),
            None,
        )
        .await
        .expect("deployed");

        assert_eq!(
            asked.requests(),
            [
                "POST /api/services/deploy",
                "POST /api/services/7/wasm",
                "GET /api/services/7/wasm/status",
            ]
        );
        let bodies = asked.bodies();
        assert_eq!(bodies[0]["manifest"], loans());
        assert_eq!(bodies[0]["commit_hash"], "4f2a9c1");
        assert_eq!(
            bodies[1],
            serde_json::json!({"wasm_base64": bodies[0]["wasm_base64"], "commit_hash": "4f2a9c1"}),
            "the module the package carries, not one built for the upload"
        );

        let (config, asked) = answering(Vec::new()).await;
        let refused = sent(
            &destination(&config, &["db_v2"]),
            &package,
            &asking,
            None,
            None,
        )
        .await
        .expect_err("a platform that lacks what the bundle needs");
        assert!(
            refused.to_string().starts_with(
                "This service requires platform capabilities the target does not advertise: \
                 route_posture."
            ),
            "{refused}"
        );
        assert_eq!(asked.count(), 0, "refused before anything is sent");
    }

    async fn skipped_quietly(
        target: Target<'static>,
        after: Vec<(u16, serde_json::Value)>,
    ) -> (eyre::Result<()>, Vec<String>, Vec<serde_json::Value>) {
        let package = Package::of(bundle_of(loans())).expect("a package");
        let hash = crate::digest::sha256_hex(MODULE);
        let answers = [
            (
                200,
                serde_json::json!({"skipped": true, "service_id": "7", "name": "loans",
                                   "version": 3, "status": "inactive"}),
            ),
            (
                200,
                serde_json::json!({"wasm_hash": hash, "size_bytes": 8, "version": 4}),
            ),
        ];
        let (config, asked) = answering(answers.into_iter().chain(after).map(Some).collect()).await;
        let supports = ["db_v2", "route_posture", "inactive_deploy"];
        let to = Destination {
            flags: QUIET,
            target,
            ..destination(&config, &supports)
        };
        let ended = sent(
            &to,
            &package,
            &version::Asking::Bundle { cli: "0.21.0" },
            None,
            Some(NotRunning(())),
        )
        .await;
        (ended, asked.requests(), asked.bodies())
    }

    fn listed_as(status: Option<&str>) -> (u16, serde_json::Value) {
        let loans =
            status.map(|status| serde_json::json!({"id": "7", "name": "loans", "status": status}));
        (
            200,
            serde_json::json!({"services": loans.into_iter().collect::<Vec<_>>()}),
        )
    }

    const SKIPPED_UPLOAD: [&str; 2] = [
        "POST /api/services/deploy?inactive=true",
        "POST /api/services/7/wasm",
    ];

    #[tokio::test]
    async fn a_module_uploaded_for_a_service_not_to_be_started_leaves_it_inactive() {
        let hash = crate::digest::sha256_hex(MODULE);
        let (ended, asked, bodies) = skipped_quietly(
            Target::default(),
            vec![
                listed_as(Some("inactive")),
                (
                    200,
                    serde_json::json!({"status": "inactive", "name": "loans"}),
                ),
                (
                    200,
                    serde_json::json!({"wasm_hash": hash, "db_state": true}),
                ),
            ],
        )
        .await;
        ended.expect("deployed, and left inactive");
        assert_eq!(
            asked,
            SKIPPED_UPLOAD
                .into_iter()
                .chain([
                    "GET /api/services",
                    "PATCH /api/services/7/status",
                    "GET /api/services/7/wasm/status",
                ])
                .collect::<Vec<_>>()
        );
        assert_eq!(bodies[3], serde_json::json!({"status": "inactive"}));
    }

    #[tokio::test]
    async fn a_service_enabled_while_its_module_was_uploaded_is_not_stopped() {
        let (ended, asked, _) =
            skipped_quietly(STAGING_ACME, vec![listed_as(Some("active"))]).await;
        assert_eq!(
            ended.expect_err("enabled meanwhile").to_string(),
            "loans was enabled while its module was uploaded, and was not stopped. \
             If it should not run: cufflink services disable loans --env staging --tenant acme"
        );
        assert_eq!(
            asked,
            SKIPPED_UPLOAD
                .into_iter()
                .chain(["GET /api/services"])
                .collect::<Vec<_>>(),
            "a service someone enabled is not stopped"
        );
    }

    #[tokio::test]
    async fn a_service_that_could_not_be_kept_inactive_says_how_to_disable_it() {
        let restarting = || (503, serde_json::json!({"error": "restarting"}));
        for (after, sent) in [
            (vec![restarting()], 3),
            (vec![listed_as(Some("inactive")), restarting()], 4),
        ] {
            let (ended, asked, _) = skipped_quietly(STAGING_ACME, after).await;
            let left = ended.expect_err("not known to be inactive").to_string();
            assert!(
                left.starts_with(
                    "the module of loans was uploaded, and the service could not be kept \
                     inactive ("
                ) && left.contains("503")
                    && left.ends_with(
                        "Disable it now: cufflink services disable loans --env staging \
                         --tenant acme"
                    ),
                "{left}"
            );
            assert_eq!(asked.len(), sent, "{asked:?}");
        }
    }

    #[test]
    fn only_a_service_still_not_enabled_is_written_inactive_again() {
        let reassertion = |listed| Reassertion::of(NotRunning(()), listed);
        assert_eq!(reassertion(Some(Status::Inactive)), Reassertion::Disable);
        assert_eq!(reassertion(Some(Status::Active)), Reassertion::Enabled);
        assert_eq!(reassertion(None), Reassertion::Gone);
    }

    #[test]
    fn a_native_binary_is_sent_only_when_there_is_one() {
        let manifest = serde_json::json!({"name": "svc"});
        let plain = deploy_request(
            &manifest,
            &DeployFlags::default(),
            None,
            Some("d2FzbQ=="),
            None,
        );
        assert_eq!(plain["wasm_base64"], "d2FzbQ==");
        assert!(plain.get("native_base64").is_none(), "{plain}");
        let native = deploy_request(
            &manifest,
            &DeployFlags::default(),
            Some("abc"),
            None,
            Some("Ymlu"),
        );
        assert_eq!(native["native_base64"], "Ymlu");
        assert_eq!(native["wasm_base64"], serde_json::Value::Null);
        assert_eq!(native["commit_hash"], "abc");
    }

    #[test]
    fn the_stored_code_is_what_was_built() {
        let body = serde_json::json!({"wasm_hash": "a".repeat(64), "native_hash": "b".repeat(64)});
        let api = "https://platform.test";
        assert!(verify_echoed(&body, "native_hash", "Native binary", None, api).is_ok());
        assert!(verify_echoed(
            &body,
            "native_hash",
            "Native binary",
            Some(&"b".repeat(64)),
            api
        )
        .is_ok());
        let mismatch = verify_echoed(
            &body,
            "native_hash",
            "Native binary",
            Some(&"c".repeat(64)),
            api,
        )
        .expect_err("another binary")
        .to_string();
        assert!(
            mismatch.starts_with("Native binary hash mismatch"),
            "{mismatch}"
        );
        let silent = verify_echoed(
            &serde_json::json!({}),
            "native_hash",
            "Native binary",
            Some("b"),
            api,
        )
        .expect_err("no echo")
        .to_string();
        assert!(silent.contains("did not echo native_hash"), "{silent}");
    }

    fn platform_at(version: &str, supports: &[&str]) -> version::PlatformVersion {
        version::PlatformVersion {
            api_version: CLI_DEPLOY_API_VERSION,
            platform_version: Some(version.into()),
            supports: supports.iter().map(|feature| feature.to_string()).collect(),
        }
    }

    fn platform(supports: &[&str]) -> version::PlatformVersion {
        platform_at("0.30.0", supports)
    }

    #[test]
    fn a_flag_is_sent_only_when_set_and_only_to_a_platform_that_knows_it() {
        let guarded = platform(&["bundled_wasm", "schema_guard"]);
        let older = platform(&["bundled_wasm"]);
        let plain = DeployFlags {
            allow_destructive: true,
            ..DeployFlags::default()
        };
        let mut request = serde_json::json!({"allow_destructive": true});
        assert!(plain.verify(&older).is_ok());
        plain.write(&mut request);
        assert_eq!(request, serde_json::json!({"allow_destructive": true}));

        let flags = DeployFlags {
            accept_drift: true,
            purge: true,
            ..plain
        };
        assert!(flags.verify(&guarded).is_ok());
        flags.write(&mut request);
        assert_eq!(
            request,
            serde_json::json!({"allow_destructive": true, "accept_drift": true, "purge": true})
        );
        let refused = flags.verify(&older).unwrap_err().to_string();
        assert!(refused.contains("--accept-drift"), "{refused}");
        assert!(refused.contains("'schema_guard'"), "{refused}");
    }

    fn sql(version: u32, name: &str, sql: &str) -> cufflink_types::SqlMigration {
        cufflink_types::SqlMigration {
            version,
            name: name.into(),
            sql: sql.into(),
            no_transaction: false,
        }
    }

    #[test]
    fn the_manifests_migrations_are_the_files_on_disk() {
        let root = tempfile::tempdir().expect("tempdir");
        let dir = root.path().join("db").join("migrations");
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::create_dir_all(root.path().join("target").join("old")).expect("target");
        let built = vec![
            sql(1, "initial_tables", "CREATE TABLE loans ();\r\n"),
            sql(
                6,
                "add_loan_kind",
                "ALTER TABLE loans ADD COLUMN kind text;",
            ),
        ];
        built.iter().for_each(|m| {
            std::fs::write(dir.join(format!("{}.sql", m.file_stem())), &m.sql).expect("write");
            std::fs::write(
                root.path()
                    .join("target")
                    .join("old")
                    .join(format!("{}.sql", m.file_stem())),
                "stale",
            )
            .expect("write");
        });
        assert!(verify_migrations(root.path(), &[]).is_ok());
        assert!(verify_migrations(root.path(), &built).is_ok());

        std::fs::write(
            dir.join("V7__renewal_index.sql"),
            "CREATE INDEX i ON loans (kind);",
        )
        .expect("a file added after the build");
        let added = verify_migrations(root.path(), &built)
            .expect_err("added")
            .to_string();
        assert!(added.contains("changed since the last build"), "{added}");
        assert!(added.contains("V7__renewal_index"), "{added}");
        assert!(added.contains("cargo clean"), "{added}");
        std::fs::remove_file(dir.join("V7__renewal_index.sql")).expect("remove");

        std::fs::write(
            dir.join("V6__add_loan_kind.sql"),
            "ALTER TABLE loans ADD COLUMN kind text;\n",
        )
        .expect("edit");
        assert!(verify_migrations(root.path(), &built).is_err());

        let elsewhere = tempfile::tempdir().expect("tempdir");
        let lost = verify_migrations(elsewhere.path(), &built)
            .expect_err("no directory")
            .to_string();
        assert!(lost.contains("V1__initial_tables.sql"), "{lost}");
    }

    #[test]
    fn a_copy_of_the_migrations_elsewhere_and_a_padded_version_are_no_refusal() {
        let root = tempfile::tempdir().expect("tempdir");
        let write = |dir: &str, file: &str, sql: &str| {
            let dir = root.path().join(dir);
            std::fs::create_dir_all(&dir).expect("dir");
            std::fs::write(dir.join(file), sql).expect("write");
        };
        let built = vec![
            sql(7, "initial_tables", "CREATE TABLE loans ();\n"),
            sql(8, "add_kind", "ALTER TABLE loans ADD COLUMN kind text;\n"),
        ];
        write(
            "a/fixtures",
            "V7__initial_tables.sql",
            "CREATE TABLE loans ();\n",
        );
        write(
            "migrations",
            "V007__initial_tables.sql",
            "CREATE TABLE loans ();\n",
        );
        write(
            "migrations",
            "V008__add_kind.sql",
            "ALTER TABLE loans ADD COLUMN kind text;\n",
        );
        assert!(
            verify_migrations(root.path(), &built).is_ok(),
            "a decoy found first, and versions padded with zeros"
        );

        write("migrations", "V9__later.sql", "SELECT 1;\n");
        let changed = verify_migrations(root.path(), &built)
            .expect_err("a file added after the build")
            .to_string();
        assert!(
            changed.contains("migrations holds V7__initial_tables, V8__add_kind, V9__later"),
            "the nearest directory is named: {changed}"
        );
        write("migrations", "loans.sql", "SELECT 1;\n");
        let unreadable = verify_migrations(root.path(), &built)
            .expect_err("a file that is no migration")
            .to_string();
        assert!(unreadable.contains("loans.sql"), "{unreadable}");
    }

    #[test]
    fn only_a_422_refuses_a_plan() {
        use reqwest::StatusCode;
        let refused = |plan: bool, status: StatusCode| {
            let report = unanswered(plan, status, "{}".into());
            (
                report.downcast_ref::<Refused>().is_some(),
                report.to_string(),
            )
        };
        assert_eq!(
            refused(true, StatusCode::UNPROCESSABLE_ENTITY),
            (true, "Plan refused: {}".to_string())
        );
        for status in [
            StatusCode::CONFLICT,
            StatusCode::UNAUTHORIZED,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            assert_eq!(
                refused(true, status),
                (false, format!("Plan failed ({status}): {{}}"))
            );
        }
        assert_eq!(
            refused(false, StatusCode::UNPROCESSABLE_ENTITY),
            (
                false,
                "Deploy failed (422 Unprocessable Entity): {}".to_string()
            )
        );
    }

    const QUICKLY: Following = Following {
        interval: Duration::from_millis(10),
        attempts: 3,
        lost_grace: Duration::from_millis(200),
    };

    #[tokio::test]
    async fn a_deploy_taken_for_lost_is_followed_until_it_ends_or_the_grace_does() {
        let lost = || {
            Some((
                200,
                serde_json::json!({"status": "failed", "error": "replica lost"}),
            ))
        };
        let (config, _) = answering(vec![
            lost(),
            lost(),
            Some((
                200,
                serde_json::json!({"status": "succeeded", "result": {"version": 5}}),
            )),
        ])
        .await;
        let result = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect("the replica went on and ended it");
        assert_eq!(result, serde_json::json!({"version": 5}));

        let (config, asked) = answering(std::iter::repeat_with(lost).take(200).collect()).await;
        let error = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect_err("lost for longer than the grace")
            .to_string();
        assert!(error.starts_with("Deploy failed: replica lost"), "{error}");
        assert!(error.contains("GET /api/deploys/d1"), "{error}");
        assert!(asked.count() < 200);
    }

    #[tokio::test]
    async fn a_poll_that_fails_for_a_while_is_asked_again() {
        let running = serde_json::json!({"status": "running", "progress": {"phase": "additive"}});
        let (config, asked) = answering(vec![
            Some((503, serde_json::json!({"error": "restarting"}))),
            None,
            Some((200, running.clone())),
            Some((502, serde_json::json!({}))),
            Some((
                200,
                serde_json::json!({"status": "succeeded", "result": {"version": 4}}),
            )),
        ])
        .await;
        let result = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect("followed to its end");
        assert_eq!(result, serde_json::json!({"version": 4}));
        assert_eq!(asked.count(), 5);
    }

    #[tokio::test]
    async fn a_poll_that_keeps_failing_or_is_refused_gives_up() {
        let (config, asked) = answering(vec![
            Some((503, serde_json::json!({}))),
            None,
            Some((503, serde_json::json!({"error": "down"}))),
        ])
        .await;
        let error = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect_err("three in a row")
            .to_string();
        assert!(error.contains("503"), "{error}");
        assert!(error.contains("may still be running"), "{error}");
        assert_eq!(asked.count(), 3);

        let (config, asked) =
            answering(vec![Some((404, serde_json::json!({"error": "not found"})))]).await;
        let error = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect_err("not there")
            .to_string();
        assert!(error.contains("404"), "{error}");
        assert_eq!(asked.count(), 1);

        let (config, _) = answering(vec![Some((
            200,
            serde_json::json!({"status": "failed", "error": "V2__broken failed"}),
        ))])
        .await;
        let error = await_deploy(&config, &reqwest::Client::new(), "d1", QUICKLY)
            .await
            .expect_err("failed")
            .to_string();
        assert_eq!(error, "Deploy failed: V2__broken failed");
    }

    #[test]
    fn the_migrations_of_a_deploy_are_printed_with_their_checksums() {
        let plan = serde_json::json!({
            "schema_changes": [],
            "sql_migrations": {
                "history": "loan_service.schema_history", "mode": "verify_only", "verified": 2,
                "baselined": [], "applied": [{"version": 6, "name": "add_loan_kind", "checksum": 11}],
                "pending": [{"version": 7, "name": "renewal_index", "checksum": 42}],
                "recovered_indexes": []
            },
            "warnings": ["migration_history.mode is verify_only: V7__renewal_index left to the legacy authority"]
        });
        assert_eq!(
            plan_lines(&plan),
            [
                "SQL migrations: loan_service.schema_history (verify_only, 2 verified)",
                "applied: V6__add_loan_kind (11)",
                "pending: V7__renewal_index (42)",
                "warning: migration_history.mode is verify_only: V7__renewal_index left to the legacy authority",
            ]
        );
    }

    #[test]
    fn a_deploy_is_sent_where_the_platform_can_run_it() {
        let flags = DeployFlags::default();
        let older = platform(&["bundled_wasm"]);
        let newer = platform(&["bundled_wasm", "async_deploy"]);
        assert_eq!(
            deploy_url("https://p", &flags, &older.supports),
            "https://p/api/services/deploy"
        );
        assert_eq!(
            deploy_url("https://p", &flags, &newer.supports),
            "https://p/api/services/deploy?async=true"
        );
        let plan = DeployFlags {
            plan: true,
            ..flags
        };
        assert_eq!(
            deploy_url("https://p", &plan, &newer.supports),
            "https://p/api/services/deploy?plan=true"
        );
        assert!(plan.verify(&newer).is_ok());
        let refused = plan.verify(&older).unwrap_err().to_string();
        assert!(refused.contains("--plan"), "{refused}");
        assert!(refused.contains("'async_deploy'"), "{refused}");
        let mut request = serde_json::json!({});
        plan.write(&mut request);
        assert_eq!(
            request,
            serde_json::json!({}),
            "a plan is asked in the query"
        );
    }

    #[test]
    fn a_deploy_that_starts_nothing_is_asked_in_the_query_of_a_platform_that_can() {
        let quiet = DeployFlags {
            inactive: true,
            ..DeployFlags::default()
        };
        let newer = platform(&["async_deploy", "inactive_deploy"]);
        assert!(quiet.verify(&newer).is_ok());
        assert_eq!(
            deploy_url("https://p", &quiet, &newer.supports),
            "https://p/api/services/deploy?async=true&inactive=true"
        );
        assert_eq!(
            deploy_url("https://p", &quiet, &["inactive_deploy".to_string()]),
            "https://p/api/services/deploy?inactive=true"
        );
        let planned = DeployFlags {
            plan: true,
            ..quiet
        };
        assert_eq!(
            deploy_url("https://p", &planned, &newer.supports),
            "https://p/api/services/deploy?plan=true&inactive=true"
        );
        let mut request = serde_json::json!({});
        quiet.write(&mut request);
        assert_eq!(request, serde_json::json!({}), "asked in the query alone");

        let older = platform_at("0.29.2", &["async_deploy", "schema_guard"]);
        for flags in [quiet, planned] {
            assert_eq!(
                flags.verify(&older).expect_err("refused").to_string(),
                "platform v0.29.2 cannot deploy a service inactive (needs inactive_deploy). \
                 Upgrade the platform or deploy without the flag."
            );
        }
        let unknown = version::PlatformVersion::default();
        assert_eq!(
            quiet.verify(&unknown).expect_err("refused").to_string(),
            "platform of an unknown version cannot deploy a service inactive \
             (needs inactive_deploy). Upgrade the platform or deploy without the flag."
        );
        assert!(DeployFlags::default().verify(&older).is_ok());
    }

    #[test]
    fn a_web_app_is_refused_inactive_in_the_platforms_words() {
        use ServiceMode::{Container, Crud, Native, Wasm, Web};
        let quiet = DeployFlags {
            inactive: true,
            ..DeployFlags::default()
        };
        assert_eq!(
            quiet.admits(&Web).expect_err("refused").to_string(),
            "a web app is started by its runner whatever its service's status; \
             deploy it without --inactive"
        );
        assert!(DeployFlags::default().admits(&Web).is_ok());
        for mode in [Crud, Wasm, Container, Native] {
            assert!(quiet.admits(&mode).is_ok(), "{mode:?}");
        }
    }

    const QUIET: DeployFlags = DeployFlags {
        allow_destructive: false,
        accept_drift: false,
        purge: false,
        plan: false,
        inactive: true,
    };

    const STAGING_ACME: Target<'static> = Target {
        env: Some("staging"),
        tenant: Some("acme"),
    };

    async fn settling_at(
        target: Target<'static>,
        flags: DeployFlags,
        supports: &[&str],
        answers: Vec<(u16, serde_json::Value)>,
    ) -> (eyre::Result<serde_json::Value>, Vec<String>) {
        let (config, asked) = answering(answers.into_iter().map(Some).collect()).await;
        let deploy = Deploy {
            url: deploy_url(&config.api_url, &flags, &platform(supports).supports),
            name: "loans",
            plan: flags.plan,
            quiet: flags.inactive.then_some(NotRunning(())),
            target,
            following: QUICKLY,
        };
        let settled = settled(
            &config,
            &reqwest::Client::new(),
            &deploy,
            &serde_json::json!({"manifest": {"name": "loans"}}),
        )
        .await;
        (settled, asked.requests())
    }

    async fn settling(
        flags: DeployFlags,
        supports: &[&str],
        answers: Vec<(u16, serde_json::Value)>,
    ) -> (eyre::Result<serde_json::Value>, Vec<String>) {
        settling_at(Target::default(), flags, supports, answers).await
    }

    fn disabled() -> (u16, serde_json::Value) {
        (
            200,
            serde_json::json!({"status": "inactive", "name": "loans"}),
        )
    }

    #[tokio::test]
    async fn an_answer_that_is_not_inactive_disables_the_service_and_fails() {
        let started = serde_json::json!({"service_id": "11", "name": "loans", "version": 1});
        let refusal = "the platform deployed loans active; it was disabled. \
                       Is a platform upgrade in progress?";

        let (settled, asked) = settling(
            QUIET,
            &["inactive_deploy"],
            vec![(200, started.clone()), disabled()],
        )
        .await;
        assert_eq!(settled.expect_err("started").to_string(), refusal);
        assert_eq!(
            asked,
            [
                "POST /api/services/deploy?inactive=true",
                "PATCH /api/services/11/status"
            ]
        );

        let (settled, asked) = settling(
            QUIET,
            &["async_deploy", "inactive_deploy"],
            vec![
                (
                    202,
                    serde_json::json!({"deploy_id": "d1", "status": "queued"}),
                ),
                (
                    200,
                    serde_json::json!({"status": "succeeded", "result": started}),
                ),
                disabled(),
            ],
        )
        .await;
        assert_eq!(settled.expect_err("started").to_string(), refusal);
        assert_eq!(
            asked,
            [
                "POST /api/services/deploy?async=true&inactive=true",
                "GET /api/deploys/d1",
                "PATCH /api/services/11/status"
            ]
        );

        let (settled, asked) = settling(
            QUIET,
            &["inactive_deploy"],
            vec![
                (200, started.clone()),
                (503, serde_json::json!({"error": "restarting"})),
            ],
        )
        .await;
        let left_running = settled.expect_err("still started").to_string();
        assert!(
            left_running
                .starts_with("the platform deployed loans active, and it could not be disabled (")
                && left_running.contains("503")
                && left_running.ends_with("Disable it now: cufflink services disable loans"),
            "{left_running}"
        );
        assert_eq!(asked.len(), 2);

        let (settled, asked) = settling_at(
            STAGING_ACME,
            QUIET,
            &["inactive_deploy"],
            vec![(200, serde_json::json!({"name": "loans", "version": 1}))],
        )
        .await;
        assert_eq!(
            settled.expect_err("still started").to_string(),
            "the platform deployed loans active, and it could not be disabled \
             (No service_id in deploy response). Disable it now: \
             cufflink services disable loans --env staging --tenant acme"
        );
        assert_eq!(asked, ["POST /api/services/deploy?inactive=true"]);
    }

    fn listing(status: Option<&str>) -> (u16, serde_json::Value) {
        let loans =
            status.map(|status| serde_json::json!({"id": "11", "name": "loans", "status": status}));
        let reports = serde_json::json!({"id": "22", "name": "reports", "status": "active"});
        (
            200,
            serde_json::json!({"services": loans.into_iter().chain([reports]).collect::<Vec<_>>()}),
        )
    }

    #[tokio::test]
    async fn a_service_that_runs_is_refused_before_anything_is_built_or_sent() {
        let preflight = |answer| async {
            let (config, asked) = answering(vec![Some(answer)]).await;
            let seen = not_running(&config, &reqwest::Client::new(), "loans").await;
            (seen.map(drop), asked.requests())
        };

        let (seen, asked) = preflight(listing(Some("active"))).await;
        assert_eq!(
            seen.expect_err("it runs").to_string(),
            "Service 'loans' is active; a deploy never stops a running service. \
             Disable it first, or deploy without --inactive"
        );
        assert_eq!(asked, ["GET /api/services"]);

        for stopped in [Some("inactive"), None] {
            let (seen, asked) = preflight(listing(stopped)).await;
            assert!(seen.is_ok(), "{stopped:?}: {seen:?}");
            assert_eq!(asked, ["GET /api/services"]);
        }

        let (seen, _) = preflight((503, serde_json::json!({"error": "restarting"}))).await;
        let unseen = seen.expect_err("what it is was not seen").to_string();
        assert!(
            unseen.starts_with("Failed to list services: 503"),
            "{unseen}"
        );
    }

    #[tokio::test]
    async fn only_a_service_seen_not_running_is_disabled() {
        let started = serde_json::json!({"service_id": "11", "name": "loans", "version": 1});
        for before in [Some("inactive"), None] {
            let (config, asked) = answering(vec![
                Some(listing(before)),
                Some((200, started.clone())),
                Some(disabled()),
            ])
            .await;
            let client = reqwest::Client::new();
            let quiet = not_running(&config, &client, "loans").await.expect("seen");
            let deploy = Deploy {
                url: deploy_url(&config.api_url, &QUIET, &[]),
                name: "loans",
                plan: false,
                quiet: Some(quiet),
                target: Target::default(),
                following: QUICKLY,
            };
            let failed = settled(&config, &client, &deploy, &serde_json::json!({}))
                .await
                .expect_err("started by a replica that read no flag");
            assert_eq!(
                failed.to_string(),
                "the platform deployed loans active; it was disabled. \
                 Is a platform upgrade in progress?"
            );
            assert_eq!(
                asked.requests(),
                [
                    "GET /api/services",
                    "POST /api/services/deploy?inactive=true",
                    "PATCH /api/services/11/status"
                ],
                "{before:?}"
            );
        }

        let enabled = serde_json::json!(
            {"service_id": "11", "name": "loans", "version": 1, "status": "active"}
        );
        let (settled, asked) = settling_at(
            STAGING_ACME,
            QUIET,
            &["inactive_deploy"],
            vec![(200, enabled)],
        )
        .await;
        assert_eq!(
            settled.expect_err("enabled meanwhile").to_string(),
            "the platform deployed loans active: it was enabled while the deploy ran, and \
             was not stopped. If it should not run: \
             cufflink services disable loans --env staging --tenant acme"
        );
        assert_eq!(
            asked,
            ["POST /api/services/deploy?inactive=true"],
            "a service someone enabled is not stopped"
        );
    }

    #[tokio::test]
    async fn a_deploy_that_could_not_be_followed_says_to_check_a_service_it_was_not_to_start() {
        let unfollowed = || {
            vec![
                (
                    202,
                    serde_json::json!({"deploy_id": "d1", "status": "queued"}),
                ),
                (404, serde_json::json!({"error": "not found"})),
            ]
        };
        let supports = ["async_deploy", "inactive_deploy"];
        let (settled, _) = settling_at(STAGING_ACME, QUIET, &supports, unfollowed()).await;
        let unknown = settled.expect_err("not followed to its end").to_string();
        assert!(
            unknown.starts_with("Deploy d1 could not be followed (404")
                && unknown.ends_with(
                    "GET /api/deploys/d1 says how it ends; it was asked not to start loans: \
                     when it ends, check the service's status and, if it is active, disable \
                     it: cufflink services disable loans --env staging --tenant acme"
                ),
            "{unknown}"
        );

        let (settled, _) = settling(DeployFlags::default(), &supports, unfollowed()).await;
        let unknown = settled.expect_err("not followed to its end").to_string();
        assert!(
            unknown.ends_with("GET /api/deploys/d1 says how it ends"),
            "{unknown}"
        );

        let failed = vec![
            (
                202,
                serde_json::json!({"deploy_id": "d1", "status": "queued"}),
            ),
            (
                200,
                serde_json::json!({"status": "failed", "error": "V2__broken failed"}),
            ),
        ];
        let (settled, _) = settling(QUIET, &supports, failed).await;
        assert_eq!(
            settled.expect_err("failed").to_string(),
            "Deploy failed: V2__broken failed",
            "a deploy that ended is not one to check"
        );
    }

    #[tokio::test]
    async fn a_plan_that_is_not_inactive_fails_and_changes_nothing() {
        let planned = DeployFlags {
            plan: true,
            ..QUIET
        };
        let plan = serde_json::json!({"name": "loans", "version": 0, "new": true, "plan": {}});
        let (settled, asked) = settling(
            planned,
            &["async_deploy", "inactive_deploy"],
            vec![(200, plan)],
        )
        .await;
        let failed = settled.expect_err("a plan of a deploy that starts the service");
        assert!(failed.downcast_ref::<Refused>().is_none(), "exit 1, not 3");
        assert_eq!(
            failed.to_string(),
            "the platform planned loans as a deploy that starts it; nothing was deployed. \
             Is a platform upgrade in progress?"
        );
        assert_eq!(asked, ["POST /api/services/deploy?plan=true&inactive=true"]);
    }

    #[tokio::test]
    async fn an_answer_is_taken_as_it_is_when_it_is_what_was_asked() {
        let quiet = serde_json::json!(
            {"service_id": "11", "name": "loans", "version": 1, "status": "inactive"}
        );
        let (settled, asked) =
            settling(QUIET, &["inactive_deploy"], vec![(200, quiet.clone())]).await;
        assert_eq!(settled.expect("left inactive"), quiet);
        assert_eq!(asked, ["POST /api/services/deploy?inactive=true"]);

        let older = serde_json::json!({"service_id": "11", "name": "loans", "version": 1});
        for answer in [older, quiet] {
            let (settled, asked) =
                settling(DeployFlags::default(), &[], vec![(200, answer.clone())]).await;
            assert_eq!(settled.expect("nothing was asked of its status"), answer);
            assert_eq!(asked, ["POST /api/services/deploy"]);
        }

        let (settled, asked) = settling(
            QUIET,
            &["inactive_deploy"],
            vec![(
                409,
                serde_json::json!({"error": "Service 'loans' is active"}),
            )],
        )
        .await;
        let refused = settled.expect_err("an active service").to_string();
        assert!(refused.starts_with("Deploy failed (409"), "{refused}");
        assert_eq!(
            asked.len(),
            1,
            "a refused deploy started nothing to disable"
        );
    }

    #[test]
    fn a_deploy_that_left_its_service_inactive_says_how_to_start_it() {
        let deployed = |extra: serde_json::Value| {
            let mut body = serde_json::json!({"name": "loans", "version": 3});
            for (key, value) in extra.as_object().into_iter().flatten() {
                body[key] = value.clone();
            }
            summary(&body, &Target::default())
        };
        assert_eq!(deployed(serde_json::json!({})), "Deployed loans v3");
        assert_eq!(
            deployed(serde_json::json!({"status": "active"})),
            "Deployed loans v3"
        );
        assert_eq!(
            deployed(serde_json::json!({"status": "inactive"})),
            "Deployed loans v3 (inactive; start it with: cufflink services enable loans)"
        );
        assert_eq!(
            deployed(serde_json::json!({"skipped": true})),
            "No schema changes for loans v3 — manifest unchanged"
        );
        assert_eq!(
            deployed(serde_json::json!({"skipped": true, "status": "inactive"})),
            "No schema changes for loans v3 — manifest unchanged \
             (inactive; start it with: cufflink services enable loans)"
        );
    }

    #[tokio::test]
    async fn a_command_the_cli_names_acts_where_the_deploy_did() {
        let stopped = serde_json::json!({"name": "loans", "version": 3, "status": "inactive"});
        let staging = Target {
            env: Some("staging"),
            tenant: None,
        };
        assert_eq!(
            summary(&stopped, &staging),
            "Deployed loans v3 (inactive; start it with: \
             cufflink services enable loans --env staging)"
        );
        assert_eq!(
            summary(&stopped, &STAGING_ACME),
            "Deployed loans v3 (inactive; start it with: \
             cufflink services enable loans --env staging --tenant acme)"
        );
        let acme = Target {
            env: None,
            tenant: Some("acme"),
        };
        assert_eq!(
            acme.services("disable", "loans"),
            "cufflink services disable loans --tenant acme"
        );
        assert_eq!(
            summary(
                &serde_json::json!({"name": "loans", "version": 3}),
                &STAGING_ACME
            ),
            "Deployed loans v3"
        );

        let (config, _) = answering(Vec::new()).await;
        let target =
            |tenant, platform_key| Target::of(&config, Some("staging"), tenant, platform_key);
        assert_eq!(target(Some("acme"), true), STAGING_ACME);
        assert_eq!(target(None, true), staging);
        assert_eq!(
            target(Some("other"), false),
            staging,
            "a deploy that no platform key lets act for a tenant acted on the configuration's own"
        );
    }

    #[test]
    fn a_plan_is_printed_under_what_it_is_of() {
        assert_eq!(
            plan_report(&serde_json::json!({
                "name": "loans", "version": 0, "new": true, "status": "inactive",
                "plan": {"schema_changes": ["CREATE TABLE loans ()"], "schema_guard": "warn"},
            })),
            [
                "Plan for loans v0 (a new service)",
                "  status: inactive",
                "  CREATE TABLE loans ()",
                "  Schema guard:   warn",
            ]
        );
        assert_eq!(
            plan_report(&serde_json::json!({
                "name": "loans", "version": 4, "new": false, "status": "active",
                "skipped": true, "plan": {"schema_changes": []},
            })),
            [
                "Plan for loans v4",
                "  Nothing to do: manifest and code unchanged"
            ]
        );
    }

    #[test]
    fn a_deploys_progress_is_one_line() {
        assert_eq!(
            progress_line(
                &serde_json::json!({"phase": "sql_migrations", "applied": 3, "pending": 2})
            ),
            Some("sql_migrations (applied 3, pending 2)".to_string())
        );
        assert_eq!(
            progress_line(&serde_json::json!({"phase": "on_migrate"})),
            Some("on_migrate".to_string())
        );
        assert_eq!(progress_line(&serde_json::json!({})), None);
    }

    #[test]
    fn the_plan_of_a_deploy_is_printed_line_by_line() {
        assert!(plan_lines(&serde_json::Value::Null).is_empty());
        assert!(plan_lines(&serde_json::json!({"schema_changes": ["CREATE TABLE x"]})).is_empty());
        let plan = serde_json::json!({
            "schema_changes": [],
            "schema_guard": "warn",
            "satisfied": ["DROP COLUMN sailings.midpoint"],
            "drift": [
                {"kind": "column_missing", "table": "sailings", "column": "notes"},
                {"kind": "table_missing", "table": "batches"}
            ],
            "dependents": [{
                "on": "catalog_service.book.status",
                "kind": "materialized_view",
                "name": "reporting.book_totals",
                "description": "rule _RETURN on materialized view reporting.book_totals"
            }],
            "extra_columns": ["sailings.legacy_ref"],
            "warnings": ["schema_guard could not read the schema: timeout"]
        });
        assert_eq!(
            plan_lines(&plan),
            [
                "Schema guard:   warn",
                "already there: DROP COLUMN sailings.midpoint",
                "drift: column_missing sailings.notes",
                "drift: table_missing batches",
                "unmanaged: materialized view reporting.book_totals depends on catalog_service.book.status",
                "not declared: sailings.legacy_ref",
                "warning: schema_guard could not read the schema: timeout",
            ]
        );
    }

    #[test]
    fn a_plan_prints_private_tables_under_their_own_heading() {
        let plan = serde_json::json!({
            "schema_changes": [],
            "private_tables": ["loans", "loan_tags"],
            "no_longer_served": ["loans"],
            "warnings": ["schema_guard could not read the schema: timeout"]
        });
        assert_eq!(
            plan_lines(&plan),
            [
                "Private tables (checked, not served):",
                "  loans",
                "  loan_tags",
                "table loans: no longer served",
                "warning: schema_guard could not read the schema: timeout",
            ]
        );
    }

    #[test]
    fn a_private_table_is_refused_before_upload_in_the_platforms_words() {
        let manifest = |table: serde_json::Value| -> cufflink_types::ServiceManifest {
            serde_json::from_value(serde_json::json!({
                "name": "loan-service",
                "schema": "adopted",
                "tables": [],
                "private_tables": [
                    {"name": "labels", "columns": [{"name": "id", "column_type": "uuid"}]},
                    table
                ]
            }))
            .expect("manifest")
        };
        let columns = serde_json::json!([{"name": "id", "column_type": "uuid"}]);
        assert!(verify_private_tables(&manifest(
            serde_json::json!({"name": "loans", "columns": columns, "soft_delete": true})
        ))
        .is_ok());
        let refused = verify_private_tables(&manifest(serde_json::json!({
            "name": "loans", "columns": columns, "permission_area": "loans"
        })))
        .expect_err("permission_area")
        .to_string();
        assert_eq!(
            refused,
            "Private table 'loans' declares permission_area, which only data routes read"
        );
        let served: cufflink_types::ServiceManifest = serde_json::from_value(serde_json::json!({
            "name": "svc",
            "tables": [{"name": "loans", "columns": columns, "permission_area": "loans"}]
        }))
        .expect("manifest");
        assert!(verify_private_tables(&served).is_ok());
    }

    #[test]
    fn upload_outcomes_name_the_version() {
        use serde_json::json;
        assert_eq!(
            upload_outcome(&json!({"version": 4, "unchanged": true})),
            "  Version:    v4 (module unchanged)"
        );
        assert_eq!(
            upload_outcome(&json!({"version": 4, "unchanged": true, "restored": true})),
            "  Version:    v4 (module unchanged, restored to storage)"
        );
        assert_eq!(
            upload_outcome(&json!({"version": 5, "previous_version": 4})),
            "  Version:    v5 (was v4)"
        );
        assert_eq!(upload_outcome(&json!({"version": 4})), "  Version:    v4");
    }

    #[test]
    fn required_features_accept_when_all_advertised() {
        let supports = vec![
            "route_posture".to_string(),
            "nats_publish_authz".to_string(),
        ];
        assert!(verify_required_features(&["route_posture".to_string()], &supports).is_ok());
        assert!(verify_required_features(&[], &supports).is_ok());
    }

    #[test]
    fn module_features_join_the_manifest_requirements_sorted() {
        let legacy =
            serde_json::json!({"name": "svc", "required_platform_features": ["route_posture"]});
        let mut untouched = legacy.clone();
        let required = require_module_features(
            &mut untouched,
            &["route_posture".to_string()],
            &BTreeSet::new(),
        );
        assert_eq!(untouched, legacy);
        assert_eq!(required, vec!["route_posture"]);

        let mut manifest = legacy.clone();
        let declared = BTreeSet::from(["response_v2".to_string(), "db_v2".to_string()]);
        let required =
            require_module_features(&mut manifest, &["route_posture".to_string()], &declared);
        assert_eq!(required, vec!["db_v2", "response_v2", "route_posture"]);
        assert_eq!(
            manifest["required_platform_features"],
            serde_json::json!(["db_v2", "response_v2", "route_posture"])
        );

        let mut crud_only = serde_json::json!({"name": "svc"});
        require_module_features(&mut crud_only, &[], &declared);
        assert_eq!(
            crud_only["required_platform_features"],
            serde_json::json!(["db_v2", "response_v2"])
        );
    }

    #[test]
    fn required_features_reject_when_missing() {
        let supports = vec!["bundled_wasm".to_string()];
        let err = verify_required_features(&["route_posture".to_string()], &supports)
            .unwrap_err()
            .to_string();
        assert!(err.contains("route_posture"), "got: {err}");
    }

    #[test]
    fn private_tables_are_refused_before_upload_by_a_platform_that_predates_them() {
        let required = [cufflink_types::features::PRIVATE_TABLES.to_string()];
        let older = platform_at("0.29.2", &["bundled_wasm", "async_deploy", "schema_guard"]);
        assert_eq!(
            verify_required_features(&required, &older.supports)
                .expect_err("v0.29 would drop the key")
                .to_string(),
            "This service requires platform capabilities the target does not advertise: \
             private_tables. Upgrade the platform image before deploying (it advertises: \
             bundled_wasm, async_deploy, schema_guard)."
        );
        let newer = platform(&["bundled_wasm", "private_tables"]);
        assert!(verify_required_features(&required, &newer.supports).is_ok());
    }

    #[test]
    fn a_refused_deploy_is_told_what_was_negotiated() {
        let platform = version::PlatformVersion {
            api_version: 1,
            platform_version: Some("sha-4f2a".to_string()),
            supports: vec!["bundled_wasm".to_string()],
        };
        let manifest = serde_json::json!({"required_platform_features": ["streams"]});
        let (line, verdict) = negotiate(
            &platform,
            &version::Asking::Sdk(Some("v0.27.0")),
            &manifest,
            &["streams".to_string()],
        );
        assert_eq!(
            line,
            "platform sha-4f2a (features bundled_wasm) ← sdk v0.27.0 (needs streams)"
        );
        assert!(
            verdict
                .expect_err("refused")
                .to_string()
                .contains("streams"),
            "the line says what the refusal is about"
        );
        let (_, admitted) = negotiate(
            &platform,
            &version::Asking::Sdk(None),
            &serde_json::json!({}),
            &[],
        );
        assert!(admitted.is_ok());
    }

    fn install_package(node_modules: &Path, name: &str) {
        std::fs::create_dir_all(node_modules.join(name)).unwrap();
    }

    fn output(code: i32, stdout: &str, stderr: &str) -> Output {
        Output {
            status: ExitStatus::from_raw(code),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        }
    }

    #[test]
    fn find_node_modules_walks_past_hoisted_workspace_dir_to_root() {
        let root = tempfile::tempdir().unwrap();
        let root_nm = root.path().join("node_modules");
        install_package(&root_nm, REQUIRED_PACKAGE);

        let app = root.path().join("apps").join("portal");
        install_package(&app.join("node_modules"), "@scope-only");

        assert_eq!(find_node_modules_from(&app).unwrap(), root_nm);
    }

    #[test]
    fn find_node_modules_uses_local_dir_when_it_has_the_runtime() {
        let root = tempfile::tempdir().unwrap();
        let app = root.path().join("app");
        let app_nm = app.join("node_modules");
        install_package(&app_nm, REQUIRED_PACKAGE);

        assert_eq!(find_node_modules_from(&app).unwrap(), app_nm);
    }

    #[test]
    fn find_node_modules_errors_when_runtime_absent() {
        let root = tempfile::tempdir().unwrap();
        let app = root.path().join("app");
        install_package(&app.join("node_modules"), "other");

        let err = find_node_modules_from(&app).unwrap_err().to_string();
        assert!(err.contains(REQUIRED_PACKAGE));
    }

    #[test]
    fn format_command_output_combines_and_trims_streams() {
        assert_eq!(
            format_command_output(&output(0, "  out line  \n", "err line")),
            "out line\nerr line"
        );
    }

    #[test]
    fn format_command_output_skips_empty_streams() {
        assert_eq!(
            format_command_output(&output(0, "", "only stderr\n")),
            "only stderr"
        );
    }

    #[test]
    fn web_tarball_args_exclude_the_build_cache() {
        assert_eq!(
            web_tarball_args(false).collect::<Vec<_>>(),
            [
                "czfh",
                "-",
                "--exclude=.next/cache",
                ".next",
                "node_modules",
                "package.json"
            ]
        );
    }

    #[test]
    fn web_tarball_args_add_public_only_when_present() {
        assert_eq!(web_tarball_args(true).last(), Some("public"));
        assert_eq!(web_tarball_args(false).last(), Some("package.json"));
    }
}
