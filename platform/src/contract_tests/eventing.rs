use crate::deployer::manifest::ServiceManifest;
use crate::nats_manager::consumers::apply::created;
use crate::nats_manager::consumers::desired::{desired_consumers, Defaults};
use crate::nats_manager::consumers::plan::{plan_consumers, ConsumerOp, ObservedConsumer};
use crate::nats_manager::delivery::{decide, Decision, Outcome};
use crate::nats_manager::pump::PumpConfig;
use crate::nats_manager::stream_index::{StreamEntry, StreamSnapshot};
use crate::nats_manager::subscriptions::SubscriptionSpec;
use crate::registry::ServiceIdentity;
use async_nats::jetstream::consumer::{self, AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::RetentionPolicy;
use cufflink_types::backoff::Backoff;
use cufflink_types::AckMode;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;
use uuid::Uuid;

const SAMPLE: &str = include_str!("../../../types/fixtures/sample_manifest.json");

fn manifest() -> ServiceManifest {
    serde_json::from_str(SAMPLE).expect("fixture manifest")
}

fn identity() -> ServiceIdentity {
    ServiceIdentity {
        tenant_id: Uuid::nil(),
        tenant_slug: "default".into(),
        service_id: Uuid::nil(),
        service_name: "orders-service".into(),
        schema: "orders_service".into(),
    }
}

fn streams() -> StreamSnapshot {
    let interest = |name: &str, subject: &str| StreamEntry {
        name: name.into(),
        subjects: vec![subject.into()],
        retention: RetentionPolicy::Interest,
    };
    StreamSnapshot::new(vec![
        interest("DW_DEFAULT", "dw.default.>"),
        interest("CUFFLINK_TICKS", "cufflink.tick.>"),
    ])
}

#[test]
fn durables_keep_the_names_and_the_configuration_they_had() {
    let manifest = manifest();
    let wanted = desired_consumers(
        &identity(),
        &manifest,
        &manifest.subscriptions,
        &streams(),
        Defaults {
            ack_wait: Duration::from_secs(30),
        },
    )
    .expect("placed");
    let durables: Vec<(&str, &str, consumer::Config)> = wanted
        .iter()
        .map(|wanted| {
            (
                wanted.consumer.stream.as_str(),
                wanted.consumer.name.as_str(),
                consumer::Config {
                    metadata: Default::default(),
                    ..created(&wanted.consumer, wanted.consumer.deliver_policy.into())
                },
            )
        })
        .collect();
    let as_v0_27_0 = |name: &str, subject: &str| consumer::Config {
        durable_name: Some(name.into()),
        filter_subject: subject.into(),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        max_deliver: -1,
        ..Default::default()
    };
    assert_eq!(
        durables,
        [
            (
                "DW_DEFAULT",
                "orders_service_on_payment",
                as_v0_27_0("orders_service_on_payment", "dw.default.payments.succeeded")
            ),
            (
                "CUFFLINK_TICKS",
                "orders_service_sweep_stale",
                as_v0_27_0("orders_service_sweep_stale", "cufflink.tick.hourly")
            ),
        ],
        "what the server made of the defaults v0.27.0 created them with"
    );

    let on_the_server: Vec<ObservedConsumer> = wanted
        .iter()
        .map(|wanted| ObservedConsumer {
            stream: wanted.consumer.stream.clone(),
            name: wanted.consumer.name.to_string(),
            filters: wanted.consumer.filters.clone(),
            ack_wait: Duration::from_secs(30),
            max_deliver: -1,
            max_ack_pending: 1000,
            ack_policy: AckPolicy::Explicit,
            metadata: BTreeMap::new(),
            ack_floor_stream_seq: 12,
            delivered: 12,
        })
        .collect();
    let consumers: Vec<_> = wanted.into_iter().map(|wanted| wanted.consumer).collect();
    let plan =
        plan_consumers(&consumers, &on_the_server, &consumers, &HashSet::new()).expect("plan");
    assert!(
        plan.ops
            .iter()
            .all(|op| matches!(op, ConsumerOp::Keep { .. })),
        "a deploy changes no durable of the release before: {plan:?}"
    );
    assert!(plan.warnings.is_empty());
}

#[test]
fn every_message_is_acknowledged_whatever_its_handler_did() {
    let manifest = manifest();
    for sub in &manifest.subscriptions {
        assert_eq!(sub.ack, AckMode::Always);
        let spec = SubscriptionSpec::of(sub);
        assert_eq!(
            spec.pump(Duration::from_secs(30)),
            PumpConfig::sequential(Duration::from_secs(30)),
            "one message at a time"
        );
    }
    for outcome in [
        Outcome::Ok,
        Outcome::Interrupted,
        Outcome::Retry {
            after: None,
            reason: "trap".into(),
        },
        Outcome::Terminal {
            reason: "rejected".into(),
        },
        Outcome::Invalid {
            reason: "not JSON".into(),
        },
    ] {
        assert_eq!(
            decide(
                AckMode::Always,
                &outcome,
                1,
                -1,
                &Backoff::explicit(Vec::new()),
                false,
                1
            ),
            Decision::Ack
        );
    }
}

#[test]
fn a_manifest_of_the_release_before_demands_no_delivery_feature() {
    let demanded = manifest().derived_features();
    for feature in crate::nats_manager::subscriptions::FEATURES {
        assert!(!demanded.contains(feature), "{feature}");
    }
}
