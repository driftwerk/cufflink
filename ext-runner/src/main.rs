use cufflink_shutdown::{self as shutdown, Shutdown, KILL_MARGIN};
use cufflink_supervisor::runner_api::{forbid_inspection, RunnerApi};
use cufflink_supervisor::{ensure_writable_dir, ArtifactCap};
use cufflink_telemetry::{scope, Component};
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};

mod apps;
mod child;
mod config;
mod deployment;
mod fleet;
mod plan;
mod ready;
mod relay;
mod restart;
mod socket;
mod status;

use config::Config;
use cufflink_types::ext::{deploy_subject, undeploy_subject};
use fleet::{Fleet, Sources};
use relay::Relay;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    forbid_inspection()?;
    let config = Arc::new(Config::load(cufflink_settings::from_env()?)?);
    cufflink_telemetry::init(
        Component::ExtRunner,
        &[(scope::TENANT, config.tenant.slug.clone())],
    )?;
    let metrics = cufflink_telemetry::install_metrics()?;

    info!(
        port = config.runner_port,
        apps_dir = %config.apps_dir.display(),
        socket_dir = %config.socket_dir.display(),
        tenant = %config.tenant.slug,
        platform = %config.platform_url,
        "Starting Cufflink Extension Runner"
    );
    let shutdown = Shutdown::new(config.shutdown_grace);
    ensure_writable_dir(&config.apps_dir, "APPS_DIR", "ext-runner")?;
    ensure_writable_dir(&config.socket_dir, "EXT_SOCKET_DIR", "ext-runner")?;

    let nats =
        cufflink_nats::connect(&config.nats_url, "cufflink-ext-runner", &config.nats_auth).await?;
    info!(
        "NATS client started for {} (connects in the background)",
        cufflink_nats::redact_urls(&config.nats_url)
    );

    let platform = RunnerApi::new(
        &config.platform_url,
        config.runner_key.clone(),
        config.tenant.clone(),
        ArtifactCap {
            max_bytes: config.artifact_max_bytes,
            setting: "EXT_ARTIFACT_MAX_BYTES",
        },
    )?;
    let fleet = Arc::new(Fleet::new(
        config.clone(),
        Sources {
            platform,
            relay: Relay::new(nats.clone()),
        },
        shutdown.clone(),
    ));

    let events = futures_util::stream::select_all(
        futures_util::future::try_join_all(
            [
                deploy_subject(&config.tenant.slug, ">"),
                undeploy_subject(&config.tenant.slug, ">"),
            ]
            .map(|subject| nats.subscribe(subject)),
        )
        .await?,
    );
    let reconciled = Arc::new(AtomicBool::new(false));
    let reconciler = shutdown.spawn("ext_reconcile", {
        let (fleet, reconciled, stop) = (fleet.clone(), reconciled.clone(), shutdown.token());
        async move { fleet.run(events, &reconciled, stop).await }
    });

    let ready = {
        let nats = nats.clone();
        move || {
            reconciled.load(Ordering::SeqCst)
                && nats.connection_state() == async_nats::connection::State::Connected
        }
    };
    let status = {
        let fleet = fleet.clone();
        move || {
            let answer = fleet.status();
            async move { answer }
        }
    };
    let app: axum::Router = cufflink_supervisor::ops::routes(
        ready,
        shutdown.clone(),
        config.status_token.clone(),
        status,
        metrics,
    );

    let addr = format!("0.0.0.0:{}", config.runner_port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Extension Runner listening on {}", addr);

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
    shutdown::phase(&shutdown, "reconcile", reconciler).await;
    let calls = shutdown::phase(&shutdown, "calls", fleet.unroute_all()).await;
    let children = shutdown::phase(&shutdown, "children", fleet.stop_all()).await;
    let tasks = shutdown::phase(&shutdown, "tasks", shutdown.drain_tasks()).await;
    shutdown::finish(
        &shutdown,
        &[
            ("http", http.is_some()),
            ("calls", calls.is_some()),
            ("children", children.is_some()),
            ("tasks", shutdown::drained(tasks)),
        ],
        nats.drain(),
    )
    .await
}
