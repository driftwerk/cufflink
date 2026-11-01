use cufflink_platform::{app, config, shutdown::Shutdown};
use tokio::net::TcpListener;
use tracing::info;

fn main() -> eyre::Result<()> {
    cufflink_platform::wasm_runtime::budget::runtime()?.block_on(run())
}

async fn run() -> eyre::Result<()> {
    cufflink_telemetry::init(cufflink_telemetry::Component::Platform, &[])?;

    let vars = cufflink_settings::from_env()?;
    let config = config::PlatformConfig::load(&vars)?;
    info!(
        version = cufflink_platform::api::version::platform_version(),
        "Starting Cufflink Platform on {}:{}", config.host, config.port
    );
    let metrics_handle = cufflink_telemetry::install_metrics()?;
    info!("Prometheus metrics recorder installed");

    let addr = format!("{}:{}", config.host, config.port);
    let shutdown = Shutdown::new(config.shutdown_grace);
    let platform = app::build(config, shutdown, metrics_handle, &vars).await?;

    let listener = TcpListener::bind(&addr).await?;
    info!("Cufflink Platform listening on {}", addr);
    app::serve(platform, listener).await
}
