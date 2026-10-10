use super::consumers::desired;
use super::consumers::plan::DesiredConsumer;
use super::events::SCHEDULES_STREAM;
use super::stream_index::{StreamEntry, STREAMS_CHANGED_SUBJECT};
use super::subjects::{resolve_subject, tenant_stream_config, tenant_stream_name};
use crate::nats_conn::PublishError;
use crate::registry::RegisteredService;
use crate::AppState;
use async_nats::jetstream;
use async_nats::jetstream::stream::{self, DiscardPolicy, RetentionPolicy, StorageType};
use cufflink_types::{DiscardSpec, StreamDefinition, StreamRetention, StreamStorage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamOp {
    Create(Box<stream::Config>),
    Update {
        config: Box<stream::Config>,
        changes: Vec<String>,
    },
    Keep(String),
}

impl StreamOp {
    pub fn label(&self) -> &'static str {
        match self {
            StreamOp::Create(_) => "create",
            StreamOp::Update { .. } => "update",
            StreamOp::Keep(_) => "keep",
        }
    }

    pub fn stream(&self) -> &str {
        match self {
            StreamOp::Create(config) | StreamOp::Update { config, .. } => &config.name,
            StreamOp::Keep(name) => name,
        }
    }

    pub fn planned(&self) -> Option<StreamEntry> {
        match self {
            StreamOp::Create(config) | StreamOp::Update { config, .. } => Some(entry_of(config)),
            StreamOp::Keep(_) => None,
        }
    }

    pub fn summary(&self) -> Value {
        match self {
            StreamOp::Update { changes, .. } => {
                json!({"op": self.label(), "stream": self.stream(), "changes": changes})
            }
            _ => json!({"op": self.label(), "stream": self.stream()}),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPlan {
    pub op: StreamOp,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamPlanError {
    #[error(
        "stream '{stream}' is a {existing} stream and is declared {declared}; a stream's \
         retention cannot be changed"
    )]
    RetentionMismatch {
        stream: String,
        existing: &'static str,
        declared: &'static str,
    },
    #[error(
        "stream '{stream}' is stored in {existing} and is declared in {declared}; a stream's \
         storage cannot be changed"
    )]
    StorageMismatch {
        stream: String,
        existing: &'static str,
        declared: &'static str,
    },
    #[error(
        "stream '{stream}': {field} is {existing} and is declared {declared}; lowering it \
         deletes messages, which a deploy never does. Lower it on the server, then deploy"
    )]
    WouldShrink {
        stream: String,
        field: &'static str,
        existing: String,
        declared: i64,
    },
    #[error(
        "stream '{stream}' is declared by service '{other_service}' too, and differently; \
         every service that declares a stream declares it the same"
    )]
    DeclaredDifferently {
        stream: String,
        other_service: String,
    },
}

fn retention(declared: StreamRetention) -> RetentionPolicy {
    match declared {
        StreamRetention::Limits => RetentionPolicy::Limits,
        StreamRetention::WorkQueue => RetentionPolicy::WorkQueue,
        StreamRetention::Interest => RetentionPolicy::Interest,
    }
}

fn retention_name(policy: RetentionPolicy) -> &'static str {
    match policy {
        RetentionPolicy::Limits => "limits",
        RetentionPolicy::WorkQueue => "work_queue",
        RetentionPolicy::Interest => "interest",
    }
}

fn storage(declared: StreamStorage) -> StorageType {
    match declared {
        StreamStorage::File => StorageType::File,
        StreamStorage::Memory => StorageType::Memory,
    }
}

fn storage_name(storage: StorageType) -> &'static str {
    match storage {
        StorageType::File => "file",
        StorageType::Memory => "memory",
    }
}

fn discard(declared: DiscardSpec) -> DiscardPolicy {
    match declared {
        DiscardSpec::Old => DiscardPolicy::Old,
        DiscardSpec::New => DiscardPolicy::New,
    }
}

fn entry_of(config: &stream::Config) -> StreamEntry {
    StreamEntry {
        name: config.name.clone(),
        subjects: config.subjects.clone(),
        retention: config.retention,
    }
}

pub fn declared_entry(declared: &StreamDefinition, tenant_slug: &str) -> StreamEntry {
    entry_of(&declared_config(declared, tenant_slug))
}

fn declared_config(desired: &StreamDefinition, tenant_slug: &str) -> stream::Config {
    stream::Config {
        name: desired.name.clone(),
        subjects: desired
            .subjects
            .iter()
            .map(|subject| resolve_subject(subject, tenant_slug))
            .collect(),
        retention: retention(desired.retention),
        storage: storage(desired.storage),
        max_age: desired
            .max_age_secs
            .map_or(Duration::ZERO, Duration::from_secs),
        max_messages: desired.max_msgs.unwrap_or_default(),
        max_bytes: desired.max_bytes.unwrap_or_default(),
        duplicate_window: desired
            .duplicate_window_secs
            .map_or(Duration::ZERO, Duration::from_secs),
        discard: discard(desired.discard),
        description: desired.description.clone(),
        ..Default::default()
    }
}

fn limit(value: i64) -> Option<i64> {
    (value > 0).then_some(value)
}

fn seconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

fn raised(
    stream: &str,
    field: &'static str,
    existing: Option<i64>,
    declared: Option<i64>,
) -> Result<Option<i64>, StreamPlanError> {
    match (existing, declared) {
        (_, None) => Ok(existing),
        (Some(existing), Some(declared)) if declared >= existing => Ok(Some(declared)),
        (existing, Some(declared)) => Err(StreamPlanError::WouldShrink {
            stream: stream.to_string(),
            field,
            existing: existing.map_or_else(|| "unlimited".to_string(), |limit| limit.to_string()),
            declared,
        }),
    }
}

pub fn plan_stream(
    desired: &StreamDefinition,
    existing: Option<&stream::Config>,
    tenant_slug: &str,
) -> Result<StreamPlan, StreamPlanError> {
    let declared = declared_config(desired, tenant_slug);
    let Some(existing) = existing else {
        return Ok(StreamPlan {
            op: StreamOp::Create(Box::new(declared)),
            warnings: Vec::new(),
        });
    };
    let name = &desired.name;
    if existing.retention != declared.retention {
        return Err(StreamPlanError::RetentionMismatch {
            stream: name.clone(),
            existing: retention_name(existing.retention),
            declared: retention_name(declared.retention),
        });
    }
    if existing.storage != declared.storage {
        return Err(StreamPlanError::StorageMismatch {
            stream: name.clone(),
            existing: storage_name(existing.storage),
            declared: storage_name(declared.storage),
        });
    }
    let added = declared
        .subjects
        .iter()
        .filter(|subject| !existing.subjects.contains(subject));
    let max_age = raised(
        name,
        "max_age_secs",
        limit(seconds(existing.max_age)),
        desired
            .max_age_secs
            .map(|secs| i64::try_from(secs).unwrap_or(i64::MAX)),
    )?;
    let config = stream::Config {
        subjects: existing.subjects.iter().chain(added).cloned().collect(),
        max_age: max_age.map_or(Duration::ZERO, |secs| {
            Duration::from_secs(secs.unsigned_abs())
        }),
        max_messages: raised(
            name,
            "max_msgs",
            limit(existing.max_messages),
            desired.max_msgs,
        )?
        .unwrap_or(existing.max_messages),
        max_bytes: raised(
            name,
            "max_bytes",
            limit(existing.max_bytes),
            desired.max_bytes,
        )?
        .unwrap_or(existing.max_bytes),
        duplicate_window: desired
            .duplicate_window_secs
            .map_or(existing.duplicate_window, Duration::from_secs),
        discard: declared.discard,
        description: declared
            .description
            .or_else(|| existing.description.clone()),
        ..existing.clone()
    };
    let changes: Vec<String> = [
        ("subjects", config.subjects != existing.subjects),
        ("max_age_secs", config.max_age != existing.max_age),
        ("max_msgs", config.max_messages != existing.max_messages),
        ("max_bytes", config.max_bytes != existing.max_bytes),
        (
            "duplicate_window_secs",
            config.duplicate_window != existing.duplicate_window,
        ),
        ("discard", config.discard != existing.discard),
        ("description", config.description != existing.description),
    ]
    .into_iter()
    .filter(|(_, changed)| *changed)
    .map(|(field, _)| field.to_string())
    .collect();
    let warnings = existing
        .subjects
        .iter()
        .filter(|subject| !declared.subjects.contains(subject))
        .map(|subject| {
            format!("stream '{name}' captures '{subject}', which the manifest does not declare; it is kept")
        })
        .collect();
    Ok(StreamPlan {
        op: if changes.is_empty() {
            StreamOp::Keep(name.clone())
        } else {
            StreamOp::Update {
                config: Box::new(config),
                changes,
            }
        },
        warnings,
    })
}

pub fn config_hash(declared: &StreamDefinition) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_vec(declared).unwrap_or_default(),
    ))
}

pub fn declared_differently<'a>(
    declared: &StreamDefinition,
    others: impl IntoIterator<Item = &'a RegisteredService>,
) -> Option<StreamPlanError> {
    others.into_iter().find_map(|other| {
        other
            .manifest
            .streams
            .iter()
            .any(|theirs| theirs.name == declared.name && theirs != declared)
            .then(|| StreamPlanError::DeclaredDifferently {
                stream: declared.name.clone(),
                other_service: other.name.clone(),
            })
    })
}

pub async fn existing(js: &jetstream::Context, name: &str) -> eyre::Result<Option<stream::Config>> {
    match js.get_stream(name).await {
        Ok(stream) => Ok(Some(stream.cached_info().config.clone())),
        Err(e) if super::replicas::is_stream_not_found(e.kind()) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn publish_acked(
    js: &jetstream::Context,
    subject: String,
    headers: async_nats::HeaderMap,
    payload: bytes::Bytes,
    within: Duration,
) -> Result<jetstream::publish::PublishAck, PublishError> {
    tokio::time::timeout(within, async {
        js.publish_with_headers(subject, headers, payload)
            .await?
            .await
    })
    .await
    .map_err(|_| PublishError::TimedOut(within))?
    .map_err(|e| PublishError::Failed(e.to_string()))
}

pub async fn apply_stream_ops(state: &AppState, ops: &[StreamOp]) -> eyre::Result<()> {
    let mut changed = false;
    for op in ops {
        metrics::counter!("cufflink_stream_ops_total", "op" => op.label()).increment(1);
        if let StreamOp::Create(config) | StreamOp::Update { config, .. } = op {
            super::replicas::create_or_update_stream(&state.jetstream, (**config).clone()).await?;
            info!(stream = %config.name, op = op.label(), "Declared stream ready");
            changed = true;
        }
    }
    if changed {
        state.stream_index.refresh(&state.jetstream).await?;
        crate::nats_conn::publish_change(&state.nats, STREAMS_CHANGED_SUBJECT).await;
    }
    Ok(())
}

pub async fn ensure_tenant_stream(state: &Arc<AppState>, tenant_slug: &str) -> eyre::Result<()> {
    let _tenant_stream = state
        .subscription_tasks
        .lock_tenant_stream(tenant_slug)
        .await;
    super::events::setup_tenant_streams(&state.jetstream, tenant_slug).await
}

pub async fn expand_tenant_stream(
    state: &Arc<AppState>,
    tenant_slug: &str,
    subjects: &[String],
) -> eyre::Result<()> {
    let js = &state.jetstream;
    let _tenant_stream = state
        .subscription_tasks
        .lock_tenant_stream(tenant_slug)
        .await;
    super::events::setup_tenant_streams(js, tenant_slug).await?;
    if subjects.is_empty() {
        return Ok(());
    }
    let name = tenant_stream_name(tenant_slug);
    let captured = js
        .get_stream(&name)
        .await?
        .cached_info()
        .config
        .subjects
        .clone();
    let added: Vec<String> = subjects
        .iter()
        .filter(|subject| !captured.contains(subject))
        .cloned()
        .collect();
    if added.is_empty() {
        return Ok(());
    }
    info!("Expanding stream '{name}' subjects to include: {added:?}");
    super::replicas::create_or_update_stream(
        js,
        jetstream::stream::Config {
            subjects: captured.into_iter().chain(added).collect(),
            ..tenant_stream_config(tenant_slug)
        },
    )
    .await?;
    Ok(())
}

pub async fn ensure_for_service(
    state: &Arc<AppState>,
    service: &RegisteredService,
    consumers: &[DesiredConsumer],
) -> eyre::Result<()> {
    let tenant_stream = tenant_stream_name(&service.tenant_slug);
    let mut lost: Vec<&str> = consumers
        .iter()
        .map(|consumer| consumer.stream.as_str())
        .filter(|stream| *stream != tenant_stream)
        .collect();
    lost.sort_unstable();
    lost.dedup();
    if lost.contains(&SCHEDULES_STREAM) {
        super::events::setup_schedules_stream(&state.jetstream).await?;
        state.stream_index.refresh(&state.jetstream).await?;
    }
    let ops = lost
        .into_iter()
        .filter(|name| *name != SCHEDULES_STREAM)
        .map(|name| {
            let declared = service
                .manifest
                .streams
                .iter()
                .find(|declared| declared.name == name)
                .ok_or_else(|| eyre::eyre!("stream '{name}' does not exist"))?;
            Ok(plan_stream(declared, None, &service.tenant_slug)?.op)
        })
        .collect::<eyre::Result<Vec<StreamOp>>>()?;
    apply_stream_ops(state, &ops).await?;
    if consumers
        .iter()
        .any(|consumer| consumer.stream == tenant_stream)
    {
        expand_tenant_stream(
            state,
            &service.tenant_slug,
            &desired::uncaptured_tenant_subjects(
                consumers,
                &Default::default(),
                &service.tenant_slug,
            ),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::ServiceManifest;
    use uuid::Uuid;

    fn books() -> StreamDefinition {
        StreamDefinition {
            name: "BOOKS".into(),
            subjects: vec!["books.created".into(), "books.updated".into()],
            retention: StreamRetention::Limits,
            max_age_secs: Some(30 * 24 * 60 * 60),
            max_msgs: Some(500_000),
            max_bytes: Some(512 * 1024 * 1024),
            ..Default::default()
        }
    }

    fn as_declared() -> stream::Config {
        stream::Config {
            name: "BOOKS".to_string(),
            subjects: vec!["books.created".to_string(), "books.updated".to_string()],
            retention: RetentionPolicy::Limits,
            max_age: Duration::from_secs(30 * 24 * 60 * 60),
            max_messages: 500_000,
            max_bytes: 512 * 1024 * 1024,
            storage: StorageType::File,
            ..Default::default()
        }
    }

    fn on_the_server(config: stream::Config) -> stream::Config {
        let unlimited = |value: i64| if value == 0 { -1 } else { value };
        stream::Config {
            max_messages: unlimited(config.max_messages),
            max_bytes: unlimited(config.max_bytes),
            max_messages_per_subject: -1,
            max_consumers: -1,
            max_message_size: -1,
            num_replicas: 1,
            duplicate_window: Duration::from_secs(120),
            ..config
        }
    }

    fn planned(
        desired: &StreamDefinition,
        existing: Option<&stream::Config>,
    ) -> Result<StreamPlan, StreamPlanError> {
        plan_stream(desired, existing, "harbor")
    }

    fn op(desired: &StreamDefinition, existing: &stream::Config) -> StreamOp {
        planned(desired, Some(existing)).expect("plan").op
    }

    #[test]
    fn a_stream_the_server_lacks_is_created_exactly_as_declared() {
        assert_eq!(
            planned(&books(), None),
            Ok(StreamPlan {
                op: StreamOp::Create(Box::new(as_declared())),
                warnings: Vec::new(),
            })
        );
        let tenants = StreamDefinition {
            name: "ORDERS".into(),
            subjects: vec!["dw.{tenant}.orders.>".into()],
            retention: StreamRetention::WorkQueue,
            storage: StreamStorage::Memory,
            duplicate_window_secs: Some(600),
            discard: DiscardSpec::New,
            description: Some("order jobs".into()),
            ..Default::default()
        };
        assert_eq!(
            planned(&tenants, None).expect("plan").op,
            StreamOp::Create(Box::new(stream::Config {
                name: "ORDERS".into(),
                subjects: vec!["dw.harbor.orders.>".into()],
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::Memory,
                duplicate_window: Duration::from_secs(600),
                discard: DiscardPolicy::New,
                description: Some("order jobs".into()),
                ..Default::default()
            })),
            "what is not declared is unlimited"
        );
    }

    #[test]
    fn a_stream_that_is_as_declared_is_kept() {
        let server = on_the_server(as_declared());
        assert_eq!(
            planned(&books(), Some(&server)),
            Ok(StreamPlan {
                op: StreamOp::Keep("BOOKS".into()),
                warnings: Vec::new(),
            })
        );
        let undeclared = StreamDefinition {
            max_age_secs: None,
            max_msgs: None,
            max_bytes: None,
            ..books()
        };
        assert_eq!(
            op(&undeclared, &server),
            StreamOp::Keep("BOOKS".into()),
            "a limit that is not declared stays the server's"
        );
    }

    #[test]
    fn subjects_are_added_and_never_removed() {
        let server = on_the_server(stream::Config {
            subjects: vec!["books.updated".into(), "books.ingest.request".into()],
            ..as_declared()
        });
        let plan = planned(&books(), Some(&server)).expect("plan");
        assert_eq!(
            plan.op,
            StreamOp::Update {
                config: Box::new(stream::Config {
                    subjects: vec![
                        "books.updated".into(),
                        "books.ingest.request".into(),
                        "books.created".into()
                    ],
                    ..server
                }),
                changes: vec!["subjects".into()],
            }
        );
        assert_eq!(
            plan.warnings,
            [
                "stream 'BOOKS' captures 'books.ingest.request', which the manifest does not \
              declare; it is kept"
            ]
        );
    }

    #[test]
    fn a_limit_is_raised_and_never_lowered() {
        let server = on_the_server(as_declared());
        let raised = StreamDefinition {
            max_age_secs: Some(90 * 24 * 60 * 60),
            max_msgs: Some(1_000_000),
            max_bytes: Some(1024 * 1024 * 1024),
            ..books()
        };
        assert_eq!(
            op(&raised, &server),
            StreamOp::Update {
                config: Box::new(stream::Config {
                    max_age: Duration::from_secs(90 * 24 * 60 * 60),
                    max_messages: 1_000_000,
                    max_bytes: 1024 * 1024 * 1024,
                    ..server.clone()
                }),
                changes: vec!["max_age_secs".into(), "max_msgs".into(), "max_bytes".into()],
            }
        );
        for (lowered, field, existing, declared) in [
            (
                StreamDefinition {
                    max_age_secs: Some(86_400),
                    ..books()
                },
                "max_age_secs",
                "2592000",
                86_400,
            ),
            (
                StreamDefinition {
                    max_msgs: Some(499_999),
                    ..books()
                },
                "max_msgs",
                "500000",
                499_999,
            ),
            (
                StreamDefinition {
                    max_bytes: Some(1),
                    ..books()
                },
                "max_bytes",
                "536870912",
                1,
            ),
        ] {
            assert_eq!(
                planned(&lowered, Some(&server)),
                Err(StreamPlanError::WouldShrink {
                    stream: "BOOKS".into(),
                    field,
                    existing: existing.into(),
                    declared,
                })
            );
        }
    }

    #[test]
    fn a_limit_where_the_server_has_none_lowers_it() {
        let unlimited = on_the_server(stream::Config {
            max_age: Duration::ZERO,
            max_messages: 0,
            max_bytes: 0,
            ..as_declared()
        });
        let error = planned(&books(), Some(&unlimited)).expect_err("refused");
        assert_eq!(
            error,
            StreamPlanError::WouldShrink {
                stream: "BOOKS".into(),
                field: "max_age_secs",
                existing: "unlimited".into(),
                declared: 2_592_000,
            }
        );
        assert!(error.to_string().contains("deletes messages"), "{error}");
    }

    #[test]
    fn what_a_stream_is_cannot_be_changed() {
        let server = on_the_server(as_declared());
        assert_eq!(
            planned(
                &StreamDefinition {
                    retention: StreamRetention::WorkQueue,
                    ..books()
                },
                Some(&server)
            ),
            Err(StreamPlanError::RetentionMismatch {
                stream: "BOOKS".into(),
                existing: "limits",
                declared: "work_queue",
            })
        );
        assert_eq!(
            planned(
                &StreamDefinition {
                    storage: StreamStorage::Memory,
                    ..books()
                },
                Some(&server)
            ),
            Err(StreamPlanError::StorageMismatch {
                stream: "BOOKS".into(),
                existing: "file",
                declared: "memory",
            })
        );
    }

    #[test]
    fn what_is_declared_of_the_rest_is_set() {
        let server = on_the_server(stream::Config {
            description: Some("as it was".into()),
            num_replicas: 3,
            ..as_declared()
        });
        let declares = StreamDefinition {
            duplicate_window_secs: Some(600),
            discard: DiscardSpec::New,
            description: Some("book events".into()),
            ..books()
        };
        assert_eq!(
            op(&declares, &server),
            StreamOp::Update {
                config: Box::new(stream::Config {
                    duplicate_window: Duration::from_secs(600),
                    discard: DiscardPolicy::New,
                    description: Some("book events".into()),
                    ..server.clone()
                }),
                changes: vec![
                    "duplicate_window_secs".into(),
                    "discard".into(),
                    "description".into()
                ],
            }
        );
        assert_eq!(
            op(&books(), &server),
            StreamOp::Keep("BOOKS".into()),
            "a description that is not declared stays"
        );
    }

    fn declaring(name: &str, streams: Vec<StreamDefinition>) -> RegisteredService {
        RegisteredService {
            service_id: Uuid::new_v4(),
            tenant_id: Uuid::nil(),
            tenant_slug: "harbor".into(),
            name: name.into(),
            manifest: ServiceManifest {
                name: name.into(),
                streams,
                ..Default::default()
            },
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        }
    }

    #[test]
    fn every_service_that_declares_a_stream_declares_it_the_same() {
        let same = declaring("catalog-service", vec![books()]);
        let other = declaring(
            "receipt-service",
            vec![StreamDefinition {
                name: "RECEIPTS".into(),
                ..books()
            }],
        );
        let differs = declaring(
            "loans-service",
            vec![StreamDefinition {
                max_msgs: Some(1),
                ..books()
            }],
        );
        assert_eq!(declared_differently(&books(), [&same, &other]), None);
        assert_eq!(
            declared_differently(&books(), [&same, &differs]),
            Some(StreamPlanError::DeclaredDifferently {
                stream: "BOOKS".into(),
                other_service: "loans-service".into(),
            })
        );
        assert_eq!(config_hash(&books()), config_hash(&books()));
        assert_ne!(
            config_hash(&books()),
            config_hash(&differs.manifest.streams[0])
        );
    }

    #[test]
    fn a_change_is_reported_with_what_it_changes() {
        assert_eq!(
            StreamOp::Create(Box::new(as_declared())).summary(),
            json!({"op": "create", "stream": "BOOKS"})
        );
        assert_eq!(
            StreamOp::Update {
                config: Box::new(as_declared()),
                changes: vec!["subjects".into()],
            }
            .summary(),
            json!({"op": "update", "stream": "BOOKS", "changes": ["subjects"]})
        );
        assert_eq!(StreamOp::Keep("BOOKS".into()).planned(), None);
        assert_eq!(
            StreamOp::Create(Box::new(as_declared()))
                .planned()
                .map(|stream| (stream.name, stream.retention)),
            Some(("BOOKS".to_string(), RetentionPolicy::Limits))
        );
    }
}
