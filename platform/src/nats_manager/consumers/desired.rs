use super::naming::{durable_name, scoped, DurableName};
use super::plan::{
    ConsumerKind, CreatedWith, DesiredConsumer, METADATA_HANDLER, METADATA_KIND, METADATA_MANAGED,
    METADATA_SERVICE, METADATA_TENANT,
};
use super::LoopKey;
use crate::deployer::manifest::{ServiceManifest, SubscriptionDefinition};
use crate::deployer::validator::{subscription_scope_violation, SHARED_STREAMS};
use crate::errors::PlatformError;
use crate::nats_manager::events::SCHEDULES_STREAM;
use crate::nats_manager::stream_index::{StreamEntry, StreamSnapshot};
use crate::nats_manager::subjects::{resolve_subject, tenant_stream_config, Namespace};
use crate::registry::ServiceIdentity;
use crate::AppState;
use async_nats::jetstream::stream::RetentionPolicy;
use cufflink_types::{AckMode, ConsumerSpec, DeliverPolicySpec, ScheduleDefinition};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use uuid::Uuid;

pub const ON_SUCCESS_MAX_DELIVER: i64 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Defaults {
    pub ack_wait: Duration,
}

#[derive(Debug, Clone, Copy)]
pub enum Declared<'a> {
    Subscription(&'a SubscriptionDefinition),
    Schedule(&'a ScheduleDefinition),
}

#[derive(Debug, Clone)]
pub struct Wanted<'a> {
    pub declared: Declared<'a>,
    pub consumer: DesiredConsumer,
}

#[derive(Debug, Clone, PartialEq)]
struct Placement {
    resolved_subject: String,
    stream_name: String,
    retention: RetentionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    #[error("Subscription handler '{handler}': declared stream '{declared}' does not exist")]
    StreamNotFound { handler: String, declared: String },
    #[error(
        "Subscription handler '{handler}': stream '{declared}' does not capture subject '{resolved}'"
    )]
    StreamDoesNotCaptureSubject {
        handler: String,
        resolved: String,
        declared: String,
    },
    #[error(
        "Subscription handler '{handler}' subject '{resolved}' is captured by stream \
         '{owning_stream}'. Declare it explicitly in the manifest:\n    \
         (\"{original}\", \"{handler}\", {handler_type}, \"{owning_stream}\")"
    )]
    ImplicitOverlap {
        handler: String,
        original: String,
        resolved: String,
        handler_type: String,
        owning_stream: String,
    },
}

pub fn tenant_stream(tenant_slug: &str) -> StreamEntry {
    let config = tenant_stream_config(tenant_slug);
    StreamEntry {
        name: config.name,
        subjects: config.subjects,
        retention: config.retention,
    }
}

pub fn deployed_streams(manifest: &ServiceManifest, tenant_slug: &str) -> Vec<StreamEntry> {
    std::iter::once(tenant_stream(tenant_slug))
        .chain(
            manifest.streams.iter().map(|declared| {
                crate::nats_manager::streams::declared_entry(declared, tenant_slug)
            }),
        )
        .collect()
}

pub fn with_planned(
    streams: &StreamSnapshot,
    planned: impl IntoIterator<Item = StreamEntry>,
) -> StreamSnapshot {
    let planned: Vec<StreamEntry> = planned
        .into_iter()
        .filter(|stream| streams.get(&stream.name).is_none())
        .collect();
    StreamSnapshot::new(streams.iter().cloned().chain(planned).collect())
}

pub fn with_changed(streams: &StreamSnapshot, changed: &[StreamEntry]) -> StreamSnapshot {
    let is_changed = |stream: &&StreamEntry| changed.iter().any(|other| other.name == stream.name);
    StreamSnapshot::new(
        streams
            .iter()
            .filter(|stream| !is_changed(stream))
            .chain(changed)
            .cloned()
            .collect(),
    )
}

fn pick_stream(
    sub: &SubscriptionDefinition,
    resolved_subject: &str,
    streams: &StreamSnapshot,
    tenant_stream: &StreamEntry,
) -> Result<Placement, PlacementError> {
    let placed_on = |stream: &StreamEntry| Placement {
        resolved_subject: resolved_subject.to_string(),
        stream_name: stream.name.clone(),
        retention: stream.retention,
    };
    if let Some(declared) = sub.stream.as_deref() {
        return match streams.get(declared) {
            None => Err(PlacementError::StreamNotFound {
                handler: sub.handler.clone(),
                declared: declared.to_string(),
            }),
            Some(stream) if !stream.captures(resolved_subject) => {
                Err(PlacementError::StreamDoesNotCaptureSubject {
                    handler: sub.handler.clone(),
                    resolved: resolved_subject.to_string(),
                    declared: declared.to_string(),
                })
            }
            Some(stream) => Ok(placed_on(stream)),
        };
    }
    match streams
        .iter()
        .find(|stream| stream.name != tenant_stream.name && stream.captures(resolved_subject))
    {
        Some(owning) => Err(PlacementError::ImplicitOverlap {
            handler: sub.handler.clone(),
            original: sub.subject.clone(),
            resolved: resolved_subject.to_string(),
            handler_type: sub.handler_type.as_str().to_string(),
            owning_stream: owning.name.clone(),
        }),
        None => Ok(placed_on(
            streams.get(&tenant_stream.name).unwrap_or(tenant_stream),
        )),
    }
}

fn max_deliver_under(ack: AckMode) -> i64 {
    match ack {
        AckMode::Always => ConsumerSpec::UNLIMITED_DELIVERIES,
        AckMode::OnSuccess => ON_SUCCESS_MAX_DELIVER,
    }
}

fn metadata(
    identity: &ServiceIdentity,
    handler: &str,
    kind: ConsumerKind,
) -> BTreeMap<String, String> {
    [
        (METADATA_TENANT, identity.tenant_slug.as_str()),
        (METADATA_SERVICE, identity.service_name.as_str()),
        (METADATA_HANDLER, handler),
        (METADATA_MANAGED, "1"),
        (METADATA_KIND, kind.as_str()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_string(), value.to_string()))
    .collect()
}

fn consumer(
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
    sub: &SubscriptionDefinition,
    placement: Placement,
    defaults: Defaults,
) -> DesiredConsumer {
    let name = durable_name(
        identity,
        sub,
        manifest.consumers,
        &placement.stream_name,
        &placement.resolved_subject,
    );
    let declared = sub.consumer.clone().unwrap_or_default();
    let created_with = CreatedWith {
        ack_wait: declared
            .ack_wait_secs
            .map_or(defaults.ack_wait, |secs| Duration::from_secs(secs.into())),
        max_deliver: declared
            .max_deliver
            .unwrap_or_else(|| max_deliver_under(sub.ack)),
    };
    let kind = if SHARED_STREAMS.contains(&placement.stream_name.as_str()) {
        ConsumerKind::Tick
    } else {
        ConsumerKind::Subscription
    };
    let bound = name.is_bound();
    DesiredConsumer {
        key: LoopKey {
            tenant_slug: identity.tenant_slug.clone(),
            service_name: identity.service_name.clone(),
            handler: sub.handler.clone(),
            subject: placement.resolved_subject.clone(),
        },
        filters: std::iter::once(placement.resolved_subject)
            .chain(
                declared
                    .filter_subjects
                    .iter()
                    .map(|filter| resolve_subject(filter, &identity.tenant_slug)),
            )
            .collect(),
        stream: placement.stream_name,
        retention: placement.retention,
        ack_wait: if bound {
            declared
                .ack_wait_secs
                .map(|secs| Duration::from_secs(secs.into()))
        } else {
            Some(created_with.ack_wait)
        },
        max_deliver: if bound {
            declared.max_deliver
        } else {
            Some(created_with.max_deliver)
        },
        max_ack_pending: declared.max_ack_pending,
        created_with,
        deliver_policy: declared.deliver_policy.unwrap_or(DeliverPolicySpec::All),
        kind,
        metadata: if bound {
            BTreeMap::new()
        } else {
            metadata(identity, &sub.handler, kind)
        },
        name,
    }
}

fn schedule_consumer(
    identity: &ServiceIdentity,
    schedule: &ScheduleDefinition,
    defaults: Defaults,
) -> DesiredConsumer {
    let subject = crate::schedules::subject(identity, &schedule.name);
    let created_with = CreatedWith {
        ack_wait: defaults.ack_wait,
        max_deliver: crate::schedules::work::MAX_DELIVER,
    };
    DesiredConsumer {
        key: LoopKey {
            tenant_slug: identity.tenant_slug.clone(),
            service_name: identity.service_name.clone(),
            handler: schedule.handler.clone(),
            subject: subject.clone(),
        },
        stream: SCHEDULES_STREAM.to_string(),
        retention: RetentionPolicy::WorkQueue,
        name: DurableName::Scoped(scoped(
            &[
                "sched",
                &identity.tenant_slug,
                &identity.service_name,
                &schedule.name,
            ],
            &[SCHEDULES_STREAM, &subject],
        )),
        filters: [subject].into(),
        ack_wait: Some(created_with.ack_wait),
        max_deliver: Some(created_with.max_deliver),
        max_ack_pending: Some(crate::schedules::work::in_flight(schedule.overlap)),
        created_with,
        deliver_policy: DeliverPolicySpec::All,
        kind: ConsumerKind::Schedule,
        metadata: metadata(identity, &schedule.handler, ConsumerKind::Schedule),
    }
}

pub fn defaults(state: &AppState) -> Defaults {
    Defaults {
        ack_wait: state.config.eventing.consumer_default_ack_wait,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WantsError {
    #[error(transparent)]
    Unplaced(#[from] PlacementError),
    #[error("the NATS claims of tenant '{tenant}' cannot be read: {reason}")]
    ClaimsUnread { tenant: String, reason: String },
}

impl From<WantsError> for PlatformError {
    fn from(e: WantsError) -> Self {
        match e {
            WantsError::Unplaced(_) => PlatformError::Validation(e.to_string()),
            WantsError::ClaimsUnread { .. } => PlatformError::Internal(e.to_string()),
        }
    }
}

pub async fn namespace_of(
    state: &AppState,
    identity: &ServiceIdentity,
) -> Result<Namespace, WantsError> {
    state
        .tenants
        .get_or_load(identity.tenant_id)
        .await
        .map(|entry| {
            entry.map_or_else(
                || Namespace::own(&identity.tenant_slug),
                |entry| entry.namespace.clone(),
            )
        })
        .map_err(|e| WantsError::ClaimsUnread {
            tenant: identity.tenant_slug.clone(),
            reason: e.to_string(),
        })
}

async fn placements_of_service(
    state: &AppState,
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
    streams: &StreamSnapshot,
) -> Result<Vec<Result<DesiredConsumer, PlacementError>>, WantsError> {
    let namespace = namespace_of(state, identity).await?;
    Ok(placements(
        identity,
        manifest,
        in_scope(manifest, &namespace),
        &with_planned(streams, deployed_streams(manifest, &identity.tenant_slug)),
        defaults(state),
    )
    .into_iter()
    .map(|placed| placed.map(|wanted| wanted.consumer))
    .collect())
}

pub async fn of_service(
    state: &AppState,
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
    streams: &StreamSnapshot,
) -> Result<Vec<DesiredConsumer>, WantsError> {
    Ok(placements_of_service(state, identity, manifest, streams)
        .await?
        .into_iter()
        .collect::<Result<_, _>>()?)
}

pub async fn placeable_of_service(
    state: &AppState,
    identity: &ServiceIdentity,
    manifest: &ServiceManifest,
    streams: &StreamSnapshot,
) -> Result<Vec<DesiredConsumer>, WantsError> {
    Ok(placements_of_service(state, identity, manifest, streams)
        .await?
        .into_iter()
        .filter_map(Result::ok)
        .collect())
}

pub async fn wanted_by_active(
    state: &AppState,
    identity: &ServiceIdentity,
    streams: &StreamSnapshot,
) -> eyre::Result<HashSet<(String, String)>> {
    let active =
        crate::repo::deployments::current_manifest(&state.platform_db, identity.service_id)
            .await?
            .map(serde_json::from_value::<ServiceManifest>)
            .transpose()?;
    let Some(active) = active else {
        return Ok(HashSet::new());
    };
    Ok(placeable_of_service(state, identity, &active, streams)
        .await?
        .into_iter()
        .map(|consumer| (consumer.stream, consumer.name.to_string()))
        .collect())
}

pub fn consumes(manifest: &ServiceManifest) -> bool {
    !manifest.subscriptions.is_empty() || !manifest.schedules.is_empty()
}

pub async fn wanted_elsewhere(
    state: &AppState,
    streams: &StreamSnapshot,
    except: Uuid,
) -> Result<HashSet<(String, String)>, WantsError> {
    let mut wanted = HashSet::new();
    for service in state.registry.all_services().await {
        if service.service_id == except || !consumes(&service.manifest) {
            continue;
        }
        wanted.extend(
            placeable_of_service(state, &service.identity(), &service.manifest, streams)
                .await?
                .into_iter()
                .map(|consumer| (consumer.stream, consumer.name.to_string())),
        );
    }
    Ok(wanted)
}

pub fn in_scope<'a>(
    manifest: &'a ServiceManifest,
    namespace: &'a Namespace,
) -> impl Iterator<Item = &'a SubscriptionDefinition> {
    manifest
        .subscriptions
        .iter()
        .filter(|sub| subscription_scope_violation(sub, namespace).is_none())
}

fn placements<'a>(
    identity: &ServiceIdentity,
    manifest: &'a ServiceManifest,
    subscriptions: impl IntoIterator<Item = &'a SubscriptionDefinition>,
    streams: &StreamSnapshot,
    defaults: Defaults,
) -> Vec<Result<Wanted<'a>, PlacementError>> {
    let tenant_stream = tenant_stream(&identity.tenant_slug);
    subscriptions
        .into_iter()
        .map(|subscription| {
            let resolved = resolve_subject(&subscription.subject, &identity.tenant_slug);
            pick_stream(subscription, &resolved, streams, &tenant_stream).map(|placement| Wanted {
                declared: Declared::Subscription(subscription),
                consumer: consumer(identity, manifest, subscription, placement, defaults),
            })
        })
        .chain(manifest.schedules.iter().map(|schedule| {
            Ok(Wanted {
                declared: Declared::Schedule(schedule),
                consumer: schedule_consumer(identity, schedule, defaults),
            })
        }))
        .collect()
}

pub fn desired_consumers<'a>(
    identity: &ServiceIdentity,
    manifest: &'a ServiceManifest,
    subscriptions: impl IntoIterator<Item = &'a SubscriptionDefinition>,
    streams: &StreamSnapshot,
    defaults: Defaults,
) -> Result<Vec<Wanted<'a>>, PlacementError> {
    placements(identity, manifest, subscriptions, streams, defaults)
        .into_iter()
        .collect()
}

pub fn uncaptured_tenant_subjects(
    consumers: &[DesiredConsumer],
    streams: &StreamSnapshot,
    tenant_slug: &str,
) -> Vec<String> {
    let tenant_stream = tenant_stream(tenant_slug);
    let captured = |subject: &String| {
        streams
            .get(&tenant_stream.name)
            .unwrap_or(&tenant_stream)
            .captures(subject)
    };
    consumers
        .iter()
        .filter(|consumer| consumer.stream == tenant_stream.name)
        .flat_map(|consumer| &consumer.filters)
        .filter(|subject| !captured(subject))
        .fold(Vec::new(), |mut subjects, subject| {
            if !subjects.contains(subject) {
                subjects.push(subject.clone());
            }
            subjects
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_manager::consumers::naming::DurableName;
    use cufflink_types::{ConsumerNaming, HandlerType};

    const DEFAULTS: Defaults = Defaults {
        ack_wait: Duration::from_secs(30),
    };

    fn identity() -> ServiceIdentity {
        ServiceIdentity {
            tenant_id: Uuid::nil(),
            tenant_slug: "default".into(),
            service_id: Uuid::nil(),
            service_name: "stock-service".into(),
            schema: "stock_service".into(),
        }
    }

    fn sub(subject: &str, handler: &str, stream: Option<&str>) -> SubscriptionDefinition {
        SubscriptionDefinition {
            stream: stream.map(str::to_string),
            ..SubscriptionDefinition::new(subject, handler, HandlerType::Wasm)
        }
    }

    fn stream(name: &str, subjects: &[&str], retention: RetentionPolicy) -> StreamEntry {
        StreamEntry {
            name: name.to_string(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            retention,
        }
    }

    fn streams() -> StreamSnapshot {
        StreamSnapshot::new(vec![
            stream("DW_DEFAULT", &["dw.default.>"], RetentionPolicy::Interest),
            stream(
                "CUFFLINK_TICKS",
                &["cufflink.tick.>"],
                RetentionPolicy::Interest,
            ),
            stream(
                "BOOKS",
                &["books.due_date.set", "books.deleted.loans"],
                RetentionPolicy::Limits,
            ),
            stream("RECEIPTS", &["receipts.>"], RetentionPolicy::WorkQueue),
        ])
    }

    fn place(sub: &SubscriptionDefinition, subject: &str) -> Result<Placement, PlacementError> {
        pick_stream(sub, subject, &streams(), &tenant_stream("default"))
    }

    fn wanted(manifest: &ServiceManifest) -> Vec<DesiredConsumer> {
        desired_consumers(
            &identity(),
            manifest,
            &manifest.subscriptions,
            &streams(),
            DEFAULTS,
        )
        .expect("placed")
        .into_iter()
        .map(|wanted| wanted.consumer)
        .collect()
    }

    fn manifest(subscriptions: Vec<SubscriptionDefinition>) -> ServiceManifest {
        ServiceManifest {
            name: "stock-service".into(),
            subscriptions,
            ..Default::default()
        }
    }

    #[test]
    fn what_can_be_placed_is_wanted_though_a_sibling_cannot_be() {
        let manifest = manifest(vec![
            sub("cufflink.tick.5min", "on_tick", Some("CUFFLINK_TICKS")),
            sub("stocks.created", "on_created", Some("ASSETS")),
            sub("dw.{tenant}.stocks.deleted", "on_deleted", None),
        ]);
        let placed = placements(
            &identity(),
            &manifest,
            &manifest.subscriptions,
            &streams(),
            DEFAULTS,
        );
        let names: Vec<Result<String, PlacementError>> = placed
            .into_iter()
            .map(|placed| placed.map(|wanted| wanted.consumer.name.to_string()))
            .collect();
        let unplaced = PlacementError::StreamNotFound {
            handler: "on_created".into(),
            declared: "ASSETS".into(),
        };
        assert_eq!(
            names,
            [
                Ok("stock_service_on_tick".to_string()),
                Err(unplaced.clone()),
                Ok("stock_service_on_deleted".to_string()),
            ]
        );
        assert_eq!(
            desired_consumers(
                &identity(),
                &manifest,
                &manifest.subscriptions,
                &streams(),
                DEFAULTS
            )
            .map(|wanted| wanted.len()),
            Err(unplaced)
        );
    }

    #[test]
    fn what_cannot_be_told_is_answered_as_whose_it_is() {
        let unplaced = PlatformError::from(WantsError::Unplaced(PlacementError::StreamNotFound {
            handler: "on_created".into(),
            declared: "ASSETS".into(),
        }));
        assert!(
            matches!(unplaced, PlatformError::Validation(message) if message.contains("ASSETS"))
        );
        let unread = PlatformError::from(WantsError::ClaimsUnread {
            tenant: "harbor".into(),
            reason: "pool timed out".into(),
        });
        assert!(matches!(unread, PlatformError::Internal(message) if message.contains("harbor")));
    }

    #[test]
    fn pick_stream_honors_explicit_binding() {
        let s = sub("cufflink.tick.5min", "h", Some("CUFFLINK_TICKS"));
        let placement = place(&s, "cufflink.tick.5min").expect("placement");
        assert_eq!(placement.stream_name, "CUFFLINK_TICKS");
        let queued = place(
            &sub("receipts.created", "h", Some("RECEIPTS")),
            "receipts.created",
        )
        .expect("placement");
        assert_eq!(queued.retention, RetentionPolicy::WorkQueue);
    }

    #[test]
    fn pick_stream_errors_when_declared_stream_missing() {
        let s = sub("cufflink.tick.5min", "handle_x", Some("TYPO_STREAM"));
        assert_eq!(
            place(&s, "cufflink.tick.5min"),
            Err(PlacementError::StreamNotFound {
                handler: "handle_x".into(),
                declared: "TYPO_STREAM".into(),
            })
        );
    }

    #[test]
    fn pick_stream_errors_when_declared_stream_does_not_capture() {
        let s = sub("cufflink.tick.5min", "handle_x", Some("DW_DEFAULT"));
        assert_eq!(
            place(&s, "cufflink.tick.5min"),
            Err(PlacementError::StreamDoesNotCaptureSubject {
                handler: "handle_x".into(),
                resolved: "cufflink.tick.5min".into(),
                declared: "DW_DEFAULT".into(),
            })
        );
    }

    #[test]
    fn pick_stream_errors_loudly_on_implicit_overlap() {
        let s = sub("cufflink.tick.5min", "handle_tick_resync", None);
        let err = place(&s, "cufflink.tick.5min").expect_err("should error");
        assert_eq!(
            err,
            PlacementError::ImplicitOverlap {
                handler: "handle_tick_resync".into(),
                original: "cufflink.tick.5min".into(),
                resolved: "cufflink.tick.5min".into(),
                handler_type: "wasm".into(),
                owning_stream: "CUFFLINK_TICKS".into(),
            }
        );
        assert!(err
            .to_string()
            .contains(r#"("cufflink.tick.5min", "handle_tick_resync", wasm, "CUFFLINK_TICKS")"#));
    }

    #[test]
    fn a_subject_no_stream_captures_goes_to_the_tenants_stream() {
        let placement =
            place(&sub("external.subject", "h", None), "external.subject").expect("placement");
        assert_eq!(placement.stream_name, "DW_DEFAULT");
        assert_eq!(placement.retention, RetentionPolicy::Interest);
        let consumers = wanted(&manifest(vec![
            sub("external.subject", "h", None),
            sub("dw.{tenant}.stocks.created", "on_created", None),
            sub("external.subject", "again", None),
        ]));
        assert_eq!(
            uncaptured_tenant_subjects(&consumers, &streams(), "default"),
            ["external.subject"]
        );
    }

    #[test]
    fn a_tenant_without_its_stream_yet_is_planned_as_if_it_had_it() {
        let none = StreamSnapshot::default();
        let placed = pick_stream(
            &sub("dw.{tenant}.stocks.created", "h", None),
            "dw.default.stocks.created",
            &none,
            &tenant_stream("default"),
        )
        .expect("placement");
        assert_eq!(placed.stream_name, "DW_DEFAULT");
        let consumers = desired_consumers(
            &identity(),
            &manifest(vec![]),
            &[sub("dw.{tenant}.stocks.created", "h", None)],
            &none,
            DEFAULTS,
        )
        .expect("placed")
        .into_iter()
        .map(|wanted| wanted.consumer)
        .collect::<Vec<_>>();
        assert!(uncaptured_tenant_subjects(&consumers, &none, "default").is_empty());
    }

    #[test]
    fn planned_streams_join_those_the_server_has() {
        let planned = with_planned(
            &streams(),
            [
                stream("LOANS", &["loans.>"], RetentionPolicy::Limits),
                stream("BOOKS", &["books.other"], RetentionPolicy::WorkQueue),
            ],
        );
        assert!(planned.get("LOANS").is_some());
        assert_eq!(
            planned.get("BOOKS").map(|stream| stream.retention),
            Some(RetentionPolicy::Limits),
            "what the server has is what it is"
        );
        assert_eq!(planned.iter().count(), 5);

        let changed = with_changed(
            &streams(),
            &[
                stream("LOANS", &["loans.>"], RetentionPolicy::Limits),
                stream(
                    "BOOKS",
                    &["books.due_date.set", "books.created"],
                    RetentionPolicy::Limits,
                ),
            ],
        );
        assert_eq!(changed.iter().count(), 5);
        assert!(changed
            .get("BOOKS")
            .is_some_and(|books| books.captures("books.created")));
    }

    #[test]
    fn a_subscription_that_declares_nothing_wants_the_durable_it_always_had() {
        let consumers = wanted(&manifest(vec![sub(
            "dw.{tenant}.stocks.created",
            "on_created",
            None,
        )]));
        assert_eq!(
            consumers,
            [DesiredConsumer {
                key: LoopKey {
                    tenant_slug: "default".into(),
                    service_name: "stock-service".into(),
                    handler: "on_created".into(),
                    subject: "dw.default.stocks.created".into(),
                },
                stream: "DW_DEFAULT".into(),
                retention: RetentionPolicy::Interest,
                name: DurableName::Legacy("stock_service_on_created".into()),
                filters: ["dw.default.stocks.created".to_string()].into(),
                ack_wait: Some(Duration::from_secs(30)),
                max_deliver: Some(-1),
                max_ack_pending: None,
                created_with: CreatedWith {
                    ack_wait: Duration::from_secs(30),
                    max_deliver: -1,
                },
                deliver_policy: DeliverPolicySpec::All,
                kind: ConsumerKind::Subscription,
                metadata: metadata(&identity(), "on_created", ConsumerKind::Subscription),
            }]
        );
    }

    #[test]
    fn on_success_gives_a_managed_durable_a_limit() {
        let retried = SubscriptionDefinition {
            ack: AckMode::OnSuccess,
            ..sub("dw.{tenant}.stocks.created", "on_created", None)
        };
        let consumers = wanted(&ServiceManifest {
            consumers: ConsumerNaming::Scoped,
            ..manifest(vec![retried])
        });
        assert_eq!(consumers[0].max_deliver, Some(ON_SUCCESS_MAX_DELIVER));
        assert!(matches!(consumers[0].name, DurableName::Scoped(_)));
    }

    #[test]
    fn a_tick_subscription_is_a_timetable_consumer() {
        let consumers = wanted(&manifest(vec![sub(
            "cufflink.tick.5min",
            "handle_tick",
            Some("CUFFLINK_TICKS"),
        )]));
        assert_eq!(consumers[0].kind, ConsumerKind::Tick);
        assert_eq!(consumers[0].metadata[METADATA_KIND], "subscription");
    }

    #[test]
    fn a_bound_durable_is_held_only_to_what_the_manifest_declares() {
        let bound = |spec: ConsumerSpec| SubscriptionDefinition {
            ack: AckMode::OnSuccess,
            consumer: Some(ConsumerSpec {
                durable_name: Some("loans-service-book-consumer".into()),
                filter_subjects: vec!["books.deleted.loans".into()],
                ..spec
            }),
            ..sub("books.due_date.set", "on_book_event", Some("BOOKS"))
        };
        let undeclared = wanted(&manifest(vec![bound(ConsumerSpec::default())]));
        assert_eq!(
            (
                undeclared[0].ack_wait,
                undeclared[0].max_deliver,
                undeclared[0].max_ack_pending
            ),
            (None, None, None)
        );
        assert_eq!(
            undeclared[0].created_with,
            CreatedWith {
                ack_wait: Duration::from_secs(30),
                max_deliver: ON_SUCCESS_MAX_DELIVER,
            },
            "what a fresh environment creates it with"
        );
        assert!(undeclared[0].metadata.is_empty());
        assert_eq!(
            undeclared[0].filters,
            ["books.deleted.loans", "books.due_date.set"]
                .map(String::from)
                .into()
        );
        assert_eq!(undeclared[0].retention, RetentionPolicy::Limits);

        let declared = wanted(&manifest(vec![bound(ConsumerSpec {
            ack_wait_secs: Some(30),
            max_deliver: Some(3),
            max_ack_pending: Some(1000),
            deliver_policy: Some(DeliverPolicySpec::New),
            ..Default::default()
        })]));
        assert_eq!(
            (
                declared[0].ack_wait,
                declared[0].max_deliver,
                declared[0].max_ack_pending,
                declared[0].deliver_policy,
            ),
            (
                Some(Duration::from_secs(30)),
                Some(3),
                Some(1000),
                DeliverPolicySpec::New
            )
        );
    }

    fn scheduled(overlap: cufflink_types::Overlap) -> ScheduleDefinition {
        ScheduleDefinition {
            overlap,
            ..ScheduleDefinition::new(
                "order_retry",
                "retry_orders",
                cufflink_types::ScheduleSpec::Every { secs: 30 },
            )
        }
    }

    #[test]
    fn a_schedule_wants_a_durable_of_its_own_on_the_stream_of_the_fires() {
        let manifest = ServiceManifest {
            schedules: vec![scheduled(cufflink_types::Overlap::Skip)],
            ..manifest(vec![sub("dw.{tenant}.stocks.created", "on_created", None)])
        };
        let wanted = wanted(&manifest);
        assert_eq!(
            wanted
                .iter()
                .map(|consumer| consumer.kind)
                .collect::<Vec<_>>(),
            [ConsumerKind::Subscription, ConsumerKind::Schedule],
            "after the subscriptions, which keep their order"
        );
        let durable = &wanted[1];
        let subject = "cufflink.schedule.default.stock-service.order_retry";
        assert_eq!(
            (durable.stream.as_str(), durable.retention),
            ("CUFFLINK_SCHEDULES", RetentionPolicy::WorkQueue)
        );
        assert_eq!(durable.filters, [subject.to_string()].into());
        assert_eq!(
            (durable.key.handler.as_str(), durable.key.subject.as_str()),
            ("retry_orders", subject)
        );
        assert!(
            durable
                .name
                .as_str()
                .starts_with("sched_default_stock-service_order_retry_"),
            "{}",
            durable.name
        );
        assert!(!durable.name.is_bound() && !durable.name.is_legacy());
        assert_eq!(
            (
                durable.ack_wait,
                durable.max_deliver,
                durable.max_ack_pending,
                durable.deliver_policy,
            ),
            (
                Some(Duration::from_secs(30)),
                Some(3),
                Some(1),
                DeliverPolicySpec::All
            ),
            "one fire in flight on every replica together; a work queue is read from its start"
        );
        assert_eq!(
            durable.metadata.get(METADATA_KIND).map(String::as_str),
            Some("schedule")
        );
        assert_eq!(
            durable.metadata.get(METADATA_HANDLER).map(String::as_str),
            Some("retry_orders")
        );
    }

    #[test]
    fn a_schedule_is_named_whatever_the_manifest_names_its_consumers() {
        let named = |consumers| {
            wanted(&ServiceManifest {
                consumers,
                schedules: vec![scheduled(cufflink_types::Overlap::Skip)],
                ..manifest(vec![])
            })[0]
                .name
                .clone()
        };
        assert_eq!(
            named(cufflink_types::ConsumerNaming::Legacy),
            named(cufflink_types::ConsumerNaming::Scoped)
        );
    }

    #[test]
    fn a_schedule_that_lets_fires_overlap_has_four_in_flight() {
        let overlapping = wanted(&ServiceManifest {
            schedules: vec![scheduled(cufflink_types::Overlap::Allow)],
            ..manifest(vec![])
        });
        assert_eq!(overlapping[0].max_ack_pending, Some(4));
    }

    #[test]
    fn a_manifest_consumes_with_a_subscription_or_a_schedule() {
        assert!(!consumes(&manifest(vec![])));
        assert!(consumes(&manifest(vec![sub(
            "dw.{tenant}.stocks.created",
            "on_created",
            None
        )])));
        assert!(consumes(&ServiceManifest {
            schedules: vec![scheduled(cufflink_types::Overlap::Skip)],
            ..manifest(vec![])
        }));
    }

    #[test]
    fn a_subscription_outside_the_tenant_is_not_in_scope() {
        let manifest = manifest(vec![
            sub("dw.{tenant}.stocks.created", "mine", None),
            sub("dw.other.stocks.created", "theirs", None),
            sub("books.created", "unclaimed", None),
        ]);
        let namespace = Namespace::own("default");
        let handlers: Vec<&str> = in_scope(&manifest, &namespace)
            .map(|sub| sub.handler.as_str())
            .collect();
        assert_eq!(handlers, ["mine"]);
    }
}
