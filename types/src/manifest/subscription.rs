use crate::features;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubscriptionDefinition {
    pub subject: String,
    pub handler: String,
    pub handler_type: HandlerType,
    #[serde(default, serialize_with = "crate::serde_util::sorted")]
    pub config: HashMap<String, String>,
    /// Explicit JetStream stream to host this subscription's consumer. When
    /// `None`, the platform uses the tenant stream (`DW_{tenant}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// When a delivery is acknowledged.
    #[serde(default, skip_serializing_if = "AckMode::is_always")]
    pub ack: AckMode,
    /// The durable consumer's configuration; the platform's own when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer: Option<ConsumerSpec>,
    /// The handler's execution budget; 60 s when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u32>,
}

impl SubscriptionDefinition {
    pub const DEFAULT_TIMEOUT_SECS: u32 = 60;
    /// The range `timeout_secs` must fall in.
    pub const TIMEOUT_SECS: std::ops::RangeInclusive<u32> = 1..=300;

    /// A subscription with the platform's delivery defaults.
    pub fn new(
        subject: impl Into<String>,
        handler: impl Into<String>,
        handler_type: HandlerType,
    ) -> Self {
        Self {
            subject: subject.into(),
            handler: handler.into(),
            handler_type,
            ..Self::default()
        }
    }

    /// Platform features this subscription's declaration requires.
    pub fn derived_features(&self) -> impl Iterator<Item = &'static str> {
        (self.consumer.is_some() || !self.ack.is_always() || self.timeout_secs.is_some())
            .then_some(features::NATS_CONSUMER_CONFIG)
            .into_iter()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandlerType {
    DeleteCascade,
    UpdateField,
    Webhook,
    #[default]
    Wasm,
}

impl HandlerType {
    /// The name a manifest declares it by.
    pub fn as_str(&self) -> &'static str {
        match self {
            HandlerType::DeleteCascade => "delete_cascade",
            HandlerType::UpdateField => "update_field",
            HandlerType::Webhook => "webhook",
            HandlerType::Wasm => "wasm",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Acknowledged whatever the handler returns.
    #[default]
    Always,
    /// Acknowledged when the handler succeeds; redelivered, then
    /// dead-lettered, when it does not.
    OnSuccess,
}

impl AckMode {
    pub fn is_always(&self) -> bool {
        *self == AckMode::Always
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerSpec {
    /// Bind, or create, exactly this durable; the platform never deletes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_wait_secs: Option<u32>,
    /// `-1` redelivers without limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_deliver: Option<i64>,
    /// The delay before each redelivery, the last repeating.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backoff_secs: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ack_pending: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_policy: Option<DeliverPolicySpec>,
    /// Filters beside the subscription's subject, which is always one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter_subjects: Vec<String>,
    /// Messages handled at once per replica; 1 when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u32>,
    /// Whether a message given up on is published to the dead-letter
    /// stream; on when unset and `ack` is `on_success`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter: Option<bool>,
}

impl ConsumerSpec {
    pub const ACK_WAIT_SECS: std::ops::RangeInclusive<u32> = 5..=3600;
    pub const MAX_DELIVER: std::ops::RangeInclusive<i64> = 1..=100;
    pub const UNLIMITED_DELIVERIES: i64 = -1;
    pub const MAX_BACKOFF_STEPS: usize = 10;
    pub const BACKOFF_SECS: std::ops::RangeInclusive<u32> = 1..=86_400;
    pub const MAX_ACK_PENDING: std::ops::RangeInclusive<i64> = 1..=100_000;
    pub const BATCH: std::ops::RangeInclusive<u32> = 1..=256;
    pub const CONCURRENCY: std::ops::RangeInclusive<u32> = 1..=64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverPolicySpec {
    All,
    New,
    Last,
    LastPerSubject,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handler_type_is_named_as_a_manifest_declares_it() {
        for handler_type in [
            HandlerType::DeleteCascade,
            HandlerType::UpdateField,
            HandlerType::Webhook,
            HandlerType::Wasm,
        ] {
            assert_eq!(
                serde_json::to_value(&handler_type).expect("serialize"),
                serde_json::json!(handler_type.as_str())
            );
        }
    }

    #[test]
    fn test_subscription_stream_defaults_to_none_in_legacy_json() {
        let json = r#"{"subject":"x","handler":"h","handler_type":"wasm"}"#;
        let sub: SubscriptionDefinition = serde_json::from_str(json).unwrap();
        assert!(sub.stream.is_none());
    }

    #[test]
    fn test_subscription_stream_roundtrip_when_set() {
        let sub = SubscriptionDefinition {
            stream: Some("CUFFLINK_TICKS".into()),
            ..SubscriptionDefinition::new(
                "cufflink.tick.5min",
                "handle_tick_resync",
                HandlerType::Wasm,
            )
        };
        let json = serde_json::to_string(&sub).unwrap();
        assert!(json.contains("\"stream\":\"CUFFLINK_TICKS\""));
        let parsed: SubscriptionDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.stream.as_deref(), Some("CUFFLINK_TICKS"));
    }

    #[test]
    fn test_subscription_stream_skipped_when_none() {
        let sub = SubscriptionDefinition::new("x", "h", HandlerType::Wasm);
        let json = serde_json::to_string(&sub).unwrap();
        assert!(!json.contains("stream"));
    }

    #[test]
    fn delivery_fields_are_omitted_at_their_defaults() {
        let sub = SubscriptionDefinition::new("dw.{tenant}.items.created", "h", HandlerType::Wasm);
        assert_eq!(
            serde_json::to_value(&sub).unwrap(),
            serde_json::json!({
                "subject": "dw.{tenant}.items.created",
                "handler": "h",
                "handler_type": "wasm",
                "config": {}
            })
        );
        assert_eq!(sub.derived_features().count(), 0);
    }

    #[test]
    fn delivery_fields_round_trip_and_demand_consumer_config() {
        let sub = SubscriptionDefinition {
            stream: Some("BOOKS".into()),
            ack: AckMode::OnSuccess,
            consumer: Some(ConsumerSpec {
                durable_name: Some("loans-service-book-consumer".into()),
                ack_wait_secs: Some(30),
                max_deliver: Some(3),
                backoff_secs: vec![10, 60],
                max_ack_pending: Some(1000),
                batch: Some(32),
                deliver_policy: Some(DeliverPolicySpec::LastPerSubject),
                filter_subjects: vec!["books.deleted.loans".into()],
                concurrency: Some(4),
                dead_letter: Some(false),
            }),
            timeout_secs: Some(120),
            ..SubscriptionDefinition::new("books.due_date.set", "on_book", HandlerType::Wasm)
        };
        let json = serde_json::to_value(&sub).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "subject": "books.due_date.set",
                "handler": "on_book",
                "handler_type": "wasm",
                "config": {},
                "stream": "BOOKS",
                "ack": "on_success",
                "consumer": {
                    "durable_name": "loans-service-book-consumer",
                    "ack_wait_secs": 30,
                    "max_deliver": 3,
                    "backoff_secs": [10, 60],
                    "max_ack_pending": 1000,
                    "batch": 32,
                    "deliver_policy": "last_per_subject",
                    "filter_subjects": ["books.deleted.loans"],
                    "concurrency": 4,
                    "dead_letter": false
                },
                "timeout_secs": 120
            })
        );
        let parsed: SubscriptionDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.ack, AckMode::OnSuccess);
        assert_eq!(parsed.consumer, sub.consumer);
        assert_eq!(parsed.timeout_secs, Some(120));
    }

    #[test]
    fn a_config_is_written_in_the_order_of_its_keys() {
        let sub = SubscriptionDefinition {
            config: [
                ("where_column", "customer_id"),
                ("table", "order_stats"),
                ("set_value", "lines + 1"),
                ("set_column", "lines"),
            ]
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
            ..SubscriptionDefinition::new("x", "count_lines", HandlerType::UpdateField)
        };
        assert_eq!(
            serde_json::to_string(&sub).unwrap(),
            r#"{"subject":"x","handler":"count_lines","handler_type":"update_field","config":{"set_column":"lines","set_value":"lines + 1","table":"order_stats","where_column":"customer_id"}}"#
        );
    }

    #[test]
    fn a_default_subscription_is_a_wasm_handler_with_the_platforms_delivery() {
        let sub = SubscriptionDefinition::default();
        assert!(matches!(sub.handler_type, HandlerType::Wasm));
        assert_eq!(sub.ack, AckMode::Always);
        assert_eq!((&sub.consumer, sub.timeout_secs), (&None, None));
        assert_eq!(sub.derived_features().count(), 0);
    }

    #[test]
    fn each_delivery_field_alone_demands_consumer_config() {
        let plain = || SubscriptionDefinition::new("x", "h", HandlerType::Wasm);
        for sub in [
            SubscriptionDefinition {
                ack: AckMode::OnSuccess,
                ..plain()
            },
            SubscriptionDefinition {
                consumer: Some(ConsumerSpec::default()),
                ..plain()
            },
            SubscriptionDefinition {
                timeout_secs: Some(60),
                ..plain()
            },
        ] {
            assert_eq!(
                sub.derived_features().collect::<Vec<_>>(),
                [features::NATS_CONSUMER_CONFIG]
            );
        }
    }
}
