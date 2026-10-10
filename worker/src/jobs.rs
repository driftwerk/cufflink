use cufflink_platform::nats_manager::pump::PumpExit;
use cufflink_telemetry::GaugeGuard;
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub const KINDS: [&str; 3] = ["sync", "async", "long"];

pub fn span(kind: &'static str, headers: Option<&async_nats::HeaderMap>) -> tracing::Span {
    let span = cufflink_telemetry::scopable_span!(
        parent: None,
        "worker_job",
        otel.name = %format!("worker/{kind}"),
        otel.kind = "consumer",
        job.kind = kind,
    );
    if let Some(parent) = headers.and_then(cufflink_platform::telemetry::context_from_nats_headers)
    {
        span.set_parent(parent);
    }
    span
}

pub fn record(kind: &'static str, result: &'static str) {
    metrics::counter!("cufflink_worker_jobs_total", "kind" => kind, "result" => result)
        .increment(1);
}

pub fn in_flight(kind: &'static str) -> GaugeGuard {
    GaugeGuard::enter(metrics::gauge!("cufflink_worker_inflight", "kind" => kind))
}

pub fn ended(consumer: &str, exit: PumpExit) -> eyre::Result<()> {
    match exit {
        PumpExit::Stopped | PumpExit::ShuttingDown => Ok(()),
        PumpExit::ConsumerGone => Err(eyre::eyre!("the {consumer} consumer or its stream is gone")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::registry::LookupSpan;

    #[test]
    fn a_job_starts_its_own_trace_under_its_pump() {
        let roots = tracing::subscriber::with_default(tracing_subscriber::registry(), || {
            let _pump = tracing::info_span!("task", task = "sync_jobs").entered();
            KINDS.map(|kind| {
                span(kind, None)
                    .with_subscriber(|(id, dispatch)| {
                        dispatch
                            .downcast_ref::<tracing_subscriber::Registry>()
                            .and_then(|registry| registry.span(id))
                            .is_some_and(|span| span.parent().is_none())
                    })
                    .unwrap_or(false)
            })
        });
        assert_eq!(roots, [true; 3]);
    }

    #[test]
    fn a_consumer_that_is_gone_is_rebuilt_and_a_stopped_one_is_not() {
        assert!(ended("backup", PumpExit::Stopped).is_ok());
        assert!(ended("backup", PumpExit::ShuttingDown).is_ok());
        assert_eq!(
            ended("backup", PumpExit::ConsumerGone)
                .expect_err("gone")
                .to_string(),
            "the backup consumer or its stream is gone"
        );
    }
}
