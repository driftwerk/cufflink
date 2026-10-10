pub mod scope;

pub use scope::{Kind, Scope};
#[doc(hidden)]
pub use tracing as __tracing;

use eyre::WrapErr;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{BatchLogProcessor, LogExporter, SdkLoggerProvider};
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanExporter};
use opentelemetry_sdk::Resource;
use scope::{ScopeLayer, ScopeLogProcessor};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{info, warn, Subscriber};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0,
];

pub const BYTE_BUCKETS: &[f64] = &[
    1024.0,
    16.0 * 1024.0,
    256.0 * 1024.0,
    1024.0 * 1024.0,
    4.0 * 1024.0 * 1024.0,
    16.0 * 1024.0 * 1024.0,
    64.0 * 1024.0 * 1024.0,
    256.0 * 1024.0 * 1024.0,
    1024.0 * 1024.0 * 1024.0,
];

pub const COMPONENT: &str = "cufflink.component";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
    Platform,
    Worker,
    WebRunner,
    ExtRunner,
}

impl Component {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Worker => "worker",
            Self::WebRunner => "web-runner",
            Self::ExtRunner => "ext-runner",
        }
    }

    pub fn service_name(self) -> String {
        format!("cufflink-{}", self.as_str())
    }
}

pub struct Exporters<W, S, L> {
    pub stdout: W,
    pub spans: Option<S>,
    pub logs: Option<L>,
}

#[derive(Default)]
pub struct Providers {
    tracer: Option<SdkTracerProvider>,
    logger: Option<SdkLoggerProvider>,
}

impl Providers {
    fn is_empty(&self) -> bool {
        self.tracer.is_none() && self.logger.is_none()
    }

    pub fn shutdown(&self) -> [(&'static str, Option<OTelSdkResult>); 2] {
        [
            (
                "trace",
                self.tracer.as_ref().map(SdkTracerProvider::shutdown),
            ),
            ("log", self.logger.as_ref().map(SdkLoggerProvider::shutdown)),
        ]
    }
}

static GUARD: OnceLock<Providers> = OnceLock::new();

pub fn init(component: Component, resource: &[(&'static str, String)]) -> eyre::Result<()> {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| default_filter(&component.service_name()).into());
    let (spans, logs) = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .map(|_| otlp_exporters())
        .unwrap_or_default();
    let (subscriber, providers) = layers(
        component,
        filter,
        self::resource(component, resource),
        Exporters {
            stdout: std::io::stdout,
            spans,
            logs,
        },
    );
    subscriber
        .try_init()
        .wrap_err("install tracing subscriber")?;

    if let Some(provider) = &providers.tracer {
        opentelemetry::global::set_tracer_provider(provider.clone());
    }
    match (&providers.tracer, &providers.logger) {
        (Some(_), Some(_)) => info!("OpenTelemetry tracing and log export enabled"),
        (Some(_), None) => info!("OpenTelemetry tracing enabled (logs export failed)"),
        _ => {}
    }
    GUARD.set(providers).ok();
    Ok(())
}

fn default_filter(service_name: &str) -> String {
    format!("info,{}=debug", service_name.replace('-', "_"))
}

pub fn resource(component: Component, extra: &[(&'static str, String)]) -> Resource {
    Resource::builder()
        .with_service_name(component.service_name())
        .with_attribute(KeyValue::new(COMPONENT, component.as_str()))
        .with_attributes(
            extra
                .iter()
                .map(|(name, value)| KeyValue::new(*name, value.clone())),
        )
        .build()
}

pub fn layers<W, S, L>(
    component: Component,
    filter: EnvFilter,
    resource: Resource,
    exporters: Exporters<W, S, L>,
) -> (impl Subscriber + Send + Sync + 'static, Providers)
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
    S: SpanExporter + 'static,
    L: LogExporter + 'static,
{
    let tracer = exporters.spans.map(|exporter| {
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.clone())
            .build()
    });
    let logger = exporters.logs.map(|exporter| {
        SdkLoggerProvider::builder()
            .with_log_processor(ScopeLogProcessor::new(
                BatchLogProcessor::builder(exporter).build(),
            ))
            .with_resource(resource)
            .build()
    });
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(exporters.stdout),
        )
        .with(tracer.as_ref().map(|provider| {
            tracing_opentelemetry::layer().with_tracer(provider.tracer(component.service_name()))
        }))
        .with(ScopeLayer::new(
            logger.as_ref().map(OpenTelemetryTracingBridge::new),
        ));
    (subscriber, Providers { tracer, logger })
}

fn otlp_exporters() -> (
    Option<opentelemetry_otlp::SpanExporter>,
    Option<opentelemetry_otlp::LogExporter>,
) {
    let spans = built(
        "trace",
        opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build(),
    );
    let logs = spans.as_ref().and_then(|_| {
        built(
            "log",
            opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .build(),
        )
    });
    (spans, logs)
}

fn built<T, E: std::fmt::Display>(what: &str, exporter: Result<T, E>) -> Option<T> {
    exporter
        .inspect_err(|e| eprintln!("Failed to initialize OTLP {what} exporter: {e}"))
        .ok()
}

pub async fn shutdown(timeout: Duration) {
    let Some(guard) = GUARD.get().filter(|guard| !guard.is_empty()) else {
        return;
    };
    let flush = tokio::task::spawn_blocking(|| guard.shutdown());
    match tokio::time::timeout(timeout, flush).await {
        Ok(Ok(outcomes)) => {
            for (what, outcome) in outcomes {
                if let Some(Err(e)) = outcome {
                    warn!(exporter = what, error = %e, "OpenTelemetry exporter shutdown failed");
                }
            }
            info!("OpenTelemetry exporters flushed");
        }
        Ok(Err(e)) => warn!(error = %e, "OpenTelemetry shutdown task failed"),
        Err(_) => warn!(?timeout, "OpenTelemetry exporters did not flush in time"),
    }
}

pub struct GaugeGuard(metrics::Gauge);

impl GaugeGuard {
    pub fn enter(gauge: metrics::Gauge) -> Self {
        gauge.increment(1.0);
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

pub fn metrics_builder() -> eyre::Result<PrometheusBuilder> {
    PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), DURATION_BUCKETS)
        .and_then(|b| b.set_buckets_for_metric(Matcher::Suffix("_bytes".into()), BYTE_BUCKETS))
        .wrap_err("configure metrics histogram buckets")
}

pub fn install_metrics() -> eyre::Result<PrometheusHandle> {
    metrics_builder()?
        .install_recorder()
        .wrap_err("install Prometheus metrics recorder")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_metrics_twice_is_an_error_not_a_panic() {
        let first = install_metrics();
        assert!(first.is_ok(), "first install: {:?}", first.err());
        let second = install_metrics();
        assert!(second.is_err(), "a second global recorder must be refused");
    }

    #[test]
    fn default_filter_names_the_binary_crate() {
        assert_eq!(
            default_filter("cufflink-platform"),
            "info,cufflink_platform=debug"
        );
        assert_eq!(
            default_filter("cufflink-worker"),
            "info,cufflink_worker=debug"
        );
    }

    #[test]
    fn gauge_guard_holds_the_gauge_up_while_alive() {
        let recorder = metrics_builder().expect("builder").build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            let guard = GaugeGuard::enter(metrics::gauge!("cufflink_test_in_flight"));
            assert!(handle.render().contains("cufflink_test_in_flight 1"));
            drop(guard);
            assert!(handle.render().contains("cufflink_test_in_flight 0"));
        });
    }

    #[test]
    fn buckets_are_ascending() {
        assert!(DURATION_BUCKETS.windows(2).all(|w| w[0] < w[1]));
        assert!(BYTE_BUCKETS.windows(2).all(|w| w[0] < w[1]));
    }
}
