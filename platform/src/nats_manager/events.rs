use async_nats::jetstream;
use tracing::info;

pub async fn setup_tenant_streams(js: &jetstream::Context, tenant_slug: &str) -> eyre::Result<()> {
    let config = crate::nats_manager::subjects::tenant_stream_config(tenant_slug);
    let stream_name = config.name.clone();
    super::replicas::get_or_create_stream(js, config).await?;
    info!(
        "NATS stream '{}' ready for tenant '{}'",
        stream_name, tenant_slug
    );
    Ok(())
}

pub const BACKUP_STREAM: &str = "CUFFLINK_BACKUP";
const BACKUP_SUBJECTS: &str = "cufflink.backup.>";

pub async fn backup_stream(js: &jetstream::Context) -> eyre::Result<jetstream::stream::Stream> {
    super::replicas::get_or_create_stream(
        js,
        jetstream::stream::Config {
            name: BACKUP_STREAM.to_string(),
            subjects: vec![BACKUP_SUBJECTS.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(24 * 60 * 60),
            ..Default::default()
        },
    )
    .await
}

pub async fn setup_backup_stream(js: &jetstream::Context) -> eyre::Result<()> {
    backup_stream(js).await?;
    info!("NATS stream '{}' ready", BACKUP_STREAM);
    Ok(())
}

pub async fn dlq_stream(
    js: &jetstream::Context,
    max_age: std::time::Duration,
) -> eyre::Result<jetstream::stream::Stream> {
    super::replicas::get_or_create_stream(
        js,
        jetstream::stream::Config {
            name: super::dlq::DLQ_STREAM.to_string(),
            subjects: vec![format!("{}>", super::dlq::DLQ_SUBJECT_PREFIX)],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_age,
            duplicate_window: std::time::Duration::from_secs(120),
            ..Default::default()
        },
    )
    .await
}

pub async fn setup_dlq_stream(
    js: &jetstream::Context,
    max_age: std::time::Duration,
) -> eyre::Result<()> {
    dlq_stream(js, max_age).await?;
    info!("NATS stream '{}' ready", super::dlq::DLQ_STREAM);
    Ok(())
}

pub async fn setup_audit_stream(
    js: &jetstream::Context,
    audit: &crate::audit::AuditConfig,
) -> eyre::Result<()> {
    use crate::outbox::route::{AUDIT_STREAM, AUDIT_SUBJECT_PREFIX};
    super::replicas::get_or_create_stream(
        js,
        jetstream::stream::Config {
            name: AUDIT_STREAM.to_string(),
            subjects: vec![format!("{AUDIT_SUBJECT_PREFIX}>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            discard: jetstream::stream::DiscardPolicy::Old,
            max_age: audit.max_age,
            max_bytes: audit.max_bytes.unwrap_or(-1),
            duplicate_window: OUTBOX_DUPLICATE_WINDOW,
            ..Default::default()
        },
    )
    .await?;
    info!("NATS stream '{}' ready", AUDIT_STREAM);
    Ok(())
}

pub async fn webhooks_stream(js: &jetstream::Context) -> eyre::Result<jetstream::stream::Stream> {
    use crate::outbox::route::{WEBHOOKS_STREAM, WEBHOOK_SUBJECT_PREFIX};
    super::replicas::get_or_create_stream(
        js,
        jetstream::stream::Config {
            name: WEBHOOKS_STREAM.to_string(),
            subjects: vec![format!("{WEBHOOK_SUBJECT_PREFIX}>")],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(7 * 24 * 60 * 60),
            duplicate_window: OUTBOX_DUPLICATE_WINDOW,
            ..Default::default()
        },
    )
    .await
}

const OUTBOX_DUPLICATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(10 * 60);

pub const ASYNC_JOBS_STREAM: &str = "CUFFLINK_WASM_JOBS";
const ASYNC_JOBS_SUBJECTS: &str = "cufflink.wasm.async.>";

pub async fn async_jobs_stream(js: &jetstream::Context) -> eyre::Result<jetstream::stream::Stream> {
    super::replicas::get_or_create_stream(
        js,
        jetstream::stream::Config {
            name: ASYNC_JOBS_STREAM.to_string(),
            subjects: vec![ASYNC_JOBS_SUBJECTS.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(3600),
            ..Default::default()
        },
    )
    .await
}

pub const LONG_JOBS_STREAM: &str = "CUFFLINK_LONG_JOBS";
pub const LONG_JOBS_SUBJECT_PREFIX: &str = "cufflink.jobs.";

pub const WORK_DUPLICATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

pub async fn setup_long_jobs_stream(js: &jetstream::Context) -> eyre::Result<()> {
    super::replicas::create_or_update_stream(
        js,
        jetstream::stream::Config {
            name: LONG_JOBS_STREAM.to_string(),
            subjects: vec![format!("{}>", LONG_JOBS_SUBJECT_PREFIX)],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: std::time::Duration::from_secs(24 * 60 * 60),
            duplicate_window: WORK_DUPLICATE_WINDOW,
            ..Default::default()
        },
    )
    .await?;

    info!("NATS stream '{}' ready", LONG_JOBS_STREAM);
    Ok(())
}

pub const SCHEDULES_STREAM: &str = "CUFFLINK_SCHEDULES";

pub async fn setup_schedules_stream(js: &jetstream::Context) -> eyre::Result<()> {
    super::replicas::create_or_update_stream(
        js,
        jetstream::stream::Config {
            name: SCHEDULES_STREAM.to_string(),
            subjects: vec![format!("{}>", crate::schedules::SUBJECT_PREFIX)],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            max_age: crate::schedules::fire::HORIZON,
            duplicate_window: WORK_DUPLICATE_WINDOW,
            ..Default::default()
        },
    )
    .await?;

    info!("NATS stream '{}' ready", SCHEDULES_STREAM);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_manager::subjects::{patterns_overlap, tenant_stream_config};
    use cufflink_types::ext::SUBJECT_PREFIX;

    #[test]
    fn no_stream_the_platform_creates_captures_an_extension_call() {
        let calls = format!("{SUBJECT_PREFIX}.>");
        let captured: Vec<String> = [
            BACKUP_SUBJECTS.to_string(),
            ASYNC_JOBS_SUBJECTS.to_string(),
            format!("{}>", super::super::dlq::DLQ_SUBJECT_PREFIX),
            format!("{}>", crate::outbox::route::AUDIT_SUBJECT_PREFIX),
            format!("{}>", crate::outbox::route::WEBHOOK_SUBJECT_PREFIX),
            format!("{LONG_JOBS_SUBJECT_PREFIX}>"),
            format!("{}>", crate::schedules::SUBJECT_PREFIX),
            super::super::tick::SUBJECT_PATTERN.to_string(),
        ]
        .into_iter()
        .chain(tenant_stream_config("acme").subjects)
        .collect();
        for subjects in &captured {
            assert!(!patterns_overlap(subjects, &calls), "{subjects}");
        }
        assert!(patterns_overlap("cufflink.>", &calls));
    }
}
