use crate::nats_conn::{effective_replicas, needs_replica_upgrade, stream_replicas};
use async_nats::jetstream::{
    self,
    context::{CreateStreamError, CreateStreamErrorKind, GetStreamErrorKind},
    stream,
};
use tracing::{error, info};

pub async fn get_or_create_stream(
    js: &jetstream::Context,
    mut config: stream::Config,
) -> eyre::Result<stream::Stream> {
    let target = stream_replicas();
    config.num_replicas = target;
    let stream = match js.get_or_create_stream(config.clone()).await {
        Ok(stream) => stream,
        Err(e) if target > 1 && is_server_rejection(&e) => {
            error!(
                stream = %config.name,
                replicas = target,
                error = %e,
                "JetStream rejected the stream at the configured replicas; creating it with 1. \
                 Check NATS_STREAM_REPLICAS against the cluster size"
            );
            config.num_replicas = 1;
            js.get_or_create_stream(config).await?
        }
        Err(e) => return Err(e.into()),
    };

    let current = stream.cached_info().config.num_replicas;
    if !needs_replica_upgrade(current, target) {
        return Ok(stream);
    }
    let mut scaled = stream.cached_info().config.clone();
    let name = scaled.name.clone();
    scaled.num_replicas = target;
    match js.update_stream(scaled).await {
        Ok(_) => {
            info!(stream = %name, from = current, to = target, "Scaled JetStream stream replicas");
            Ok(js.get_stream(&name).await?)
        }
        Err(e) => {
            error!(
                stream = %name,
                replicas = current,
                target,
                error = %e,
                "Could not scale JetStream stream replicas; keeping the existing stream"
            );
            Ok(stream)
        }
    }
}

pub async fn create_or_update_stream(
    js: &jetstream::Context,
    mut config: stream::Config,
) -> eyre::Result<stream::Info> {
    let target = stream_replicas();
    let existing = existing_replicas(js, &config.name).await?;
    let fallback = existing.unwrap_or(1).max(1);
    config.num_replicas = effective_replicas(target, existing);
    if config.num_replicas > fallback {
        info!(
            stream = %config.name,
            from = fallback,
            to = config.num_replicas,
            "Setting JetStream stream replicas"
        );
    }
    match js.create_or_update_stream(config.clone()).await {
        Ok(info) => Ok(info),
        Err(e) if config.num_replicas > fallback && is_server_rejection(&e) => {
            error!(
                stream = %config.name,
                replicas = config.num_replicas,
                fallback,
                error = %e,
                "JetStream rejected the stream replicas; keeping it at the replicas it has. \
                 Check NATS_STREAM_REPLICAS against the cluster size"
            );
            config.num_replicas = fallback;
            Ok(js.create_or_update_stream(config).await?)
        }
        Err(e) => Err(e.into()),
    }
}

fn is_server_rejection(e: &CreateStreamError) -> bool {
    matches!(e.kind(), CreateStreamErrorKind::JetStream(_))
}

pub fn is_refused(report: &eyre::Report) -> bool {
    report
        .chain()
        .filter_map(|cause| cause.downcast_ref::<CreateStreamError>())
        .any(is_server_rejection)
}

async fn existing_replicas(js: &jetstream::Context, name: &str) -> eyre::Result<Option<usize>> {
    match js.get_stream(name).await {
        Ok(stream) => Ok(Some(stream.cached_info().config.num_replicas)),
        Err(e) if is_stream_not_found(e.kind()) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn is_stream_not_found(kind: GetStreamErrorKind) -> bool {
    matches!(kind, GetStreamErrorKind::JetStream(err) if err.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_the_server_refused_is_told_from_one_nats_was_not_asked_for() {
        let refusal = || {
            let answer: jetstream::Error = serde_json::from_value(serde_json::json!({
                "code": 400, "err_code": 10074, "description": "insufficient resources",
            }))
            .expect("an answer of the server");
            eyre::Report::new(CreateStreamError::new(CreateStreamErrorKind::JetStream(
                answer,
            )))
        };
        assert!(is_refused(&refusal()));
        assert!(is_refused(&refusal().wrap_err("stream 'BOOKS'")));
        for unasked in [
            eyre::Report::new(CreateStreamError::new(CreateStreamErrorKind::TimedOut)),
            eyre::eyre!("connection closed"),
        ] {
            assert!(!is_refused(&unasked), "{unasked}");
        }
    }
}
