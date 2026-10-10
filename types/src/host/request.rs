//! The request JSON a guest handler receives, whatever delivered it.

use crate::RetryOn;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const METHOD_SUBSCRIPTION: &str = "SUBSCRIPTION";
pub const METHOD_SCHEDULE: &str = "SCHEDULE";
/// The `method` of a CRUD lifecycle hook and of `on_migrate`.
pub const METHOD_HOOK: &str = "HOOK";

/// Keys a delivery does not carry are absent from the JSON, so each kind of
/// request keeps exactly the keys it has always had.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GuestRequest {
    pub method: String,
    pub handler: String,
    /// Lowercase names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<Map<String, Value>>,
    #[serde(default)]
    pub body: Value,
    /// The exact bytes the caller sent, as base64; empty without a body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_raw_b64: Option<String>,
    pub tenant: String,
    pub service: String,
    /// `Some(Value::Null)` is an anonymous caller.
    #[serde(
        default,
        deserialize_with = "crate::serde_util::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub auth: Option<Value>,
    /// `Some(Value::Null)` is a route that declares no posture.
    #[serde(
        default,
        deserialize_with = "crate::serde_util::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub posture: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// In path order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_params: Option<Vec<(String, String)>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_string: Option<String>,
    /// Every value of every key, in order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart: Option<Vec<MultipartField>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription: Option<SubscriptionMeta>,
    #[serde(default, rename = "_job", skip_serializing_if = "Option::is_none")]
    pub job: Option<JobMeta>,
}

impl GuestRequest {
    /// The JSON the guest parses.
    pub fn to_value(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartField {
    pub name: Option<String>,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    #[serde(rename = "data_b64", with = "crate::serde_util::base64_bytes")]
    pub data: Vec<u8>,
}

/// The JetStream delivery behind a `SUBSCRIPTION` or `SCHEDULE` request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionMeta {
    pub subject: String,
    pub stream: String,
    pub consumer: String,
    pub stream_sequence: u64,
    pub consumer_sequence: u64,
    /// 1 on the first delivery.
    pub num_delivered: u64,
    pub num_pending: u64,
    /// RFC 3339.
    pub published_at: String,
    /// The message's headers without the platform's own (`Cufflink-*`).
    #[serde(default)]
    pub headers: BTreeMap<String, Vec<String>>,
}

/// The body of a `SCHEDULE` request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleFire {
    pub schedule: String,
    /// The instant the schedule was due, RFC 3339.
    pub fire_at: String,
    /// The fire is delivered late, after an outage.
    pub catch_up: bool,
}

/// The `_job` key of a request a worker runs as a job.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobMeta {
    pub id: String,
    pub attempt: u32,
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retry_on: Vec<RetryOn>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::round_trip;
    use serde_json::json;

    fn subscription() -> GuestRequest {
        GuestRequest {
            method: METHOD_SUBSCRIPTION.into(),
            handler: "on_created".into(),
            headers: Some(Map::new()),
            body: json!({"id": 7}),
            tenant: "default".into(),
            service: "stock-service".into(),
            auth: Some(Value::Null),
            ..Default::default()
        }
    }

    #[test]
    fn a_subscription_request_keeps_its_legacy_keys() {
        let wire = json!({
            "method": "SUBSCRIPTION",
            "handler": "on_created",
            "headers": {},
            "body": {"id": 7},
            "tenant": "default",
            "service": "stock-service",
            "auth": null
        });
        round_trip(&subscription(), wire.clone());
        assert_eq!(
            serde_json::to_string(&subscription().to_value().expect("value")).expect("text"),
            serde_json::to_string(&wire).expect("text")
        );
    }

    #[test]
    fn delivery_metadata_is_an_added_key() {
        let meta = SubscriptionMeta {
            subject: "books.due_date.set".into(),
            stream: "BOOKS".into(),
            consumer: "loans-service-book-consumer".into(),
            stream_sequence: 81234,
            consumer_sequence: 17,
            num_delivered: 2,
            num_pending: 3,
            published_at: "2026-09-28T10:00:00Z".into(),
            headers: BTreeMap::from([("Nats-Msg-Id".to_string(), vec!["book-7".to_string()])]),
        };
        let request = GuestRequest {
            subscription: Some(meta),
            ..subscription()
        };
        let wire = request.to_value().expect("value");
        assert_eq!(
            wire["subscription"],
            json!({
                "subject": "books.due_date.set",
                "stream": "BOOKS",
                "consumer": "loans-service-book-consumer",
                "stream_sequence": 81234,
                "consumer_sequence": 17,
                "num_delivered": 2,
                "num_pending": 3,
                "published_at": "2026-09-28T10:00:00Z",
                "headers": {"Nats-Msg-Id": ["book-7"]}
            })
        );
        round_trip(&request, wire);
    }

    #[test]
    fn a_route_request_carries_its_posture_even_when_null() {
        let request = GuestRequest {
            method: "POST".into(),
            handler: "checkout".into(),
            headers: Some(Map::from_iter([(
                "stripe-signature".to_string(),
                json!("t=1"),
            )])),
            body: json!({"params": {"a": "1"}}),
            body_raw_b64: Some(String::new()),
            tenant: "acme".into(),
            service: "payments".into(),
            auth: Some(json!({"sub": "u-1"})),
            posture: Some(Value::Null),
            path: Some("/books/7".into()),
            path_params: Some(vec![("id".into(), "7".into())]),
            query_string: Some("a=1&a=2".into()),
            query: Some(BTreeMap::from([(
                "a".to_string(),
                vec!["1".to_string(), "2".to_string()],
            )])),
            client_ip: Some("203.0.113.9".into()),
            multipart: Some(vec![MultipartField {
                name: Some("file".into()),
                filename: None,
                content_type: Some("text/plain".into()),
                data: b"hi".to_vec(),
            }]),
            ..Default::default()
        };
        round_trip(
            &request,
            json!({
                "method": "POST",
                "handler": "checkout",
                "headers": {"stripe-signature": "t=1"},
                "body": {"params": {"a": "1"}},
                "body_raw_b64": "",
                "tenant": "acme",
                "service": "payments",
                "auth": {"sub": "u-1"},
                "posture": null,
                "path": "/books/7",
                "path_params": [["id", "7"]],
                "query_string": "a=1&a=2",
                "query": {"a": ["1", "2"]},
                "client_ip": "203.0.113.9",
                "multipart": [
                    {"name": "file", "filename": null, "content_type": "text/plain", "data_b64": "aGk="}
                ]
            }),
        );
    }

    #[test]
    fn a_job_request_names_its_attempt() {
        let first = JobMeta {
            id: "j-1".into(),
            attempt: 1,
            max_attempts: 3,
            ..Default::default()
        };
        round_trip(
            &first,
            json!({"id": "j-1", "attempt": 1, "max_attempts": 3}),
        );
        round_trip(
            &JobMeta {
                attempt: 2,
                last_error: Some("upstream 503".into()),
                retry_on: vec![RetryOn::Error, RetryOn::Timeout],
                ..first.clone()
            },
            json!({
                "id": "j-1",
                "attempt": 2,
                "max_attempts": 3,
                "last_error": "upstream 503",
                "retry_on": ["error", "timeout"]
            }),
        );
        let request = GuestRequest {
            method: "POST".into(),
            handler: "run".into(),
            tenant: "t".into(),
            service: "s".into(),
            auth: Some(Value::Null),
            job: Some(first),
            ..Default::default()
        };
        assert_eq!(
            request.to_value().expect("value"),
            json!({
                "method": "POST",
                "handler": "run",
                "body": null,
                "tenant": "t",
                "service": "s",
                "auth": null,
                "_job": {"id": "j-1", "attempt": 1, "max_attempts": 3}
            })
        );
    }

    #[test]
    fn a_schedule_fire_is_the_request_body() {
        round_trip(
            &ScheduleFire {
                schedule: "order_retry".into(),
                fire_at: "2026-09-28T10:00:30Z".into(),
                catch_up: false,
            },
            json!({"schedule": "order_retry", "fire_at": "2026-09-28T10:00:30Z", "catch_up": false}),
        );
    }
}
