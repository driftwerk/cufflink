use axum::routing::get;
use cufflink_shutdown::{self as shutdown, Shutdown, KILL_MARGIN};
use cufflink_supervisor::runner_api::{forbid_inspection, RunnerApi};
use cufflink_telemetry::{scope, Component};
use futures_util::StreamExt;
use serde_json::json;
use std::future::IntoFuture;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};

mod apps;
mod artifact;
mod child_env;
mod coalescer;
mod config;
mod event;
mod host;
mod process_manager;
mod proxy;
mod source;
mod warmer;

use apps::Apps;
use config::{Scope, WebRunnerConfig};
use process_manager::ProcessManager;
use source::{Shared, Source, TenantScoped};
use warmer::LazyWarmer;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    forbid_inspection()?;
    let vars = cufflink_settings::from_env()?;
    let config = WebRunnerConfig::load(&vars)?;
    cufflink_telemetry::init(
        Component::WebRunner,
        config
            .scope
            .tenant()
            .map(|tenant| (scope::TENANT, tenant.slug.clone()))
            .as_slice(),
    )?;
    let metrics = cufflink_telemetry::install_metrics()?;

    info!(
        port = config.runner_port,
        base_domain = %config.base_domain,
        hostname_format = config.hostname_format.as_str(),
        "Starting Cufflink Web Runner"
    );
    let shutdown = Shutdown::new(config.shutdown_grace);

    let source = Arc::new(source(&config, &vars).await?);

    let nats =
        cufflink_nats::connect(&config.nats_url, "cufflink-web-runner", &config.nats_auth).await?;
    info!(
        "NATS client started for {} (connects in the background)",
        cufflink_nats::redact_urls(&config.nats_url)
    );

    let process_manager = Arc::new(ProcessManager::new(
        config.port_range.clone(),
        config.base_domain.clone(),
        config.public_scheme.clone(),
        config.hostname_format,
        config.env_mode.clone(),
        config.children,
        shutdown.clone(),
    ));
    let apps_dir = PathBuf::from(&config.apps_dir);
    cufflink_supervisor::ensure_writable_dir(&apps_dir, "APPS_DIR", "web-runner")?;

    let apps = Arc::new(Apps::new(
        source.clone(),
        artifact::ArtifactStore::new(config.artifact_caps),
        process_manager.clone(),
        apps_dir,
    ));

    let warmer = Arc::new(LazyWarmer::new(apps.clone()));

    let (deploys, undeploys) = source.subjects();
    let nats_sub = nats.subscribe(deploys.clone()).await?;
    let listening = Arc::new(AtomicBool::new(true));
    shutdown.spawn(
        "deploy_events",
        handle_deploy_events(
            nats_sub,
            deploys,
            apps.clone(),
            shutdown.token(),
            listening.clone(),
        ),
    );
    let nats_unsub = nats.subscribe(undeploys.clone()).await?;
    shutdown.spawn(
        "undeploy_events",
        handle_undeploy_events(nats_unsub, undeploys, apps.clone(), shutdown.token()),
    );
    if let Source::TenantScoped(_) = source.as_ref() {
        shutdown.spawn(
            "runner_key",
            apps::watch_key(apps.clone(), shutdown.token()),
        );
    }

    let proxy_state = Arc::new(proxy::ProxyState::new(
        process_manager.clone(),
        warmer,
        config.base_domain.clone(),
        config.hostname_format,
    ));

    let status = {
        let process_manager = process_manager.clone();
        move || {
            let answer = json!({ "apps": running(&process_manager) });
            async move { answer }
        }
    };
    let app = cufflink_supervisor::ops::routes(
        move || listening.load(Ordering::SeqCst) && source.ready(),
        shutdown.clone(),
        config.status_token.clone(),
        status,
        metrics,
    )
    .fallback(
        get(proxy::host_proxy_handler)
            .post(proxy::host_proxy_handler)
            .put(proxy::host_proxy_handler)
            .delete(proxy::host_proxy_handler)
            .patch(proxy::host_proxy_handler),
    )
    .layer(CorsLayer::permissive())
    .with_state(proxy_state);

    let addr = format!("0.0.0.0:{}", config.runner_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Web Runner listening on {}", addr);

    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled())
        .into_future();
    tokio::pin!(server);
    let ended = tokio::select! {
        biased;
        _ = shutdown::wait_for_signal() => {
            info!("Shutdown signal received");
            shutdown.trigger();
            None
        }
        _ = shutdown.cancelled() => None,
        result = &mut server => Some(result),
    };

    let http_budget = shutdown
        .remaining()
        .saturating_sub(config.child_stop_timeout + KILL_MARGIN);
    let http = match ended {
        Some(result) if !shutdown.is_triggered() => return result.map_err(Into::into),
        Some(result) => shutdown::phase(&shutdown, "http", std::future::ready(result)).await,
        None => shutdown::phase_within(&shutdown, "http", http_budget, &mut server).await,
    };
    if let Some(Err(e)) = &http {
        warn!(error = %e, "HTTP server ended with an error");
    }
    shutdown::phase(
        &shutdown,
        "children",
        process_manager.shutdown_all(config.child_stop_timeout),
    )
    .await;
    let tasks = shutdown::phase(&shutdown, "tasks", shutdown.drain_tasks()).await;
    shutdown::finish(
        &shutdown,
        &[
            ("http", http.is_some()),
            ("tasks", shutdown::drained(tasks)),
        ],
        nats.drain(),
    )
    .await
}

fn running(process_manager: &ProcessManager) -> Vec<serde_json::Value> {
    process_manager
        .list()
        .iter()
        .map(|app| {
            json!({"tenant": app.tenant, "service": app.service, "version": app.version, "port": app.port})
        })
        .collect()
}

async fn source(
    config: &WebRunnerConfig,
    vars: impl Fn(&str) -> Option<String>,
) -> eyre::Result<Source> {
    match &config.scope {
        Scope::Shared { database_url } => {
            let pool = cufflink_db::build_pool(database_url, 4)?;
            let secrets = cufflink_secrets::SecretBox::from_vars(pool.clone(), &vars)?;
            drop(pool.get().await?);
            info!("Connected to PostgreSQL");
            info!(
                mode = secrets.mode().as_str(),
                "Service configs are read as secret storage stores them"
            );
            let storage = cufflink_storage::from_vars(&vars)?;
            info!(
                "Object storage configured (backend: {}, bucket: {})",
                storage.backend_kind(),
                storage.primary_bucket()
            );
            Ok(Source::Shared(Shared {
                pool,
                secrets,
                storage,
                cap: config.artifact_caps.download(),
            }))
        }
        Scope::TenantScoped {
            tenant,
            platform_url,
            runner_key,
            poll_every,
        } => {
            info!(tenant = %tenant.slug, platform = %platform_url, "Reading the tenant's apps from the runner API");
            Ok(Source::TenantScoped(TenantScoped::new(
                RunnerApi::new(
                    platform_url,
                    runner_key.clone(),
                    tenant.clone(),
                    config.artifact_caps.download(),
                )?,
                *poll_every,
            )))
        }
    }
}

struct Listening(Arc<AtomicBool>);

impl Drop for Listening {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

async fn handle_deploy_events(
    mut sub: async_nats::Subscriber,
    subject: String,
    apps: Arc<Apps>,
    token: CancellationToken,
    listening: Arc<AtomicBool>,
) {
    info!("Listening for web deploy events on {subject}");
    let _listening = Listening(listening);

    while let Some(msg) = tokio::select! {
        _ = token.cancelled() => None,
        msg = sub.next() => msg,
    } {
        let payload: serde_json::Value = match serde_json::from_slice(&msg.payload) {
            Ok(v) => v,
            Err(e) => {
                warn!(error = %e, "Invalid deploy event payload");
                continue;
            }
        };
        match apps.source.announced(&payload).await {
            Ok(deployment) => apps.deploy(&deployment).await,
            Err(reason) => warn!(subject = %msg.subject, reason, "Refusing a deploy event"),
        }
    }
}

async fn handle_undeploy_events(
    mut sub: async_nats::Subscriber,
    subject: String,
    apps: Arc<Apps>,
    token: CancellationToken,
) {
    info!("Listening for web undeploy events on {subject}");

    while let Some(msg) = tokio::select! {
        _ = token.cancelled() => None,
        msg = sub.next() => msg,
    } {
        let payload: serde_json::Value = match serde_json::from_slice(&msg.payload) {
            Ok(v) => v,
            Err(e) => {
                warn!("Invalid undeploy event payload: {}", e);
                continue;
            }
        };
        match event::target(&payload) {
            Ok((tenant, service)) if apps.source.serves(&tenant) => {
                info!("Received undeploy event: {}/{}", tenant, service);
                apps.retire(&tenant, &service).await;
            }
            Ok((tenant, _)) => {
                warn!(subject = %msg.subject, tenant, "Refusing an undeploy event of another tenant")
            }
            Err(reason) => warn!(reason, "Refusing an undeploy event with an invalid target"),
        }
    }
}
