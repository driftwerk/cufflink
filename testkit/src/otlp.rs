use crate::TestkitError;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const EXPORT_EVERY_MS: &str = "50";
const SCOPE_PREFIX: &str = "cufflink.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signal {
    pub text: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub attributes: BTreeMap<String, String>,
    pub resource: BTreeMap<String, String>,
}

impl Signal {
    pub fn scope(&self) -> Vec<(&str, &str)> {
        self.attributes
            .iter()
            .filter(|(name, _)| name.starts_with(SCOPE_PREFIX))
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect()
    }

    pub fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes.get(name).map(String::as_str)
    }
}

#[derive(Default)]
struct Received {
    spans: Vec<Signal>,
    logs: Vec<Signal>,
}

type Kept = Arc<Mutex<Received>>;

pub struct Capture {
    endpoint: String,
    received: Kept,
    server: JoinHandle<()>,
}

impl Capture {
    pub async fn start() -> Result<Self, TestkitError> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let received = Kept::default();
        let routes = Router::new()
            .route("/v1/traces", post(traces))
            .route("/v1/logs", post(logs))
            .with_state(received.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, routes).await;
        });
        Ok(Self {
            endpoint,
            received,
            server,
        })
    }

    pub fn env(&self) -> [(&'static str, &str); 3] {
        [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", self.endpoint.as_str()),
            ("OTEL_BSP_SCHEDULE_DELAY", EXPORT_EVERY_MS),
            ("OTEL_BLRP_SCHEDULE_DELAY", EXPORT_EVERY_MS),
        ]
    }

    pub fn spans(&self) -> Vec<Signal> {
        self.kept(|received| received.spans.clone())
    }

    pub fn logs(&self) -> Vec<Signal> {
        self.kept(|received| received.logs.clone())
    }

    pub async fn span(
        &self,
        what: &str,
        wanted: impl Fn(&Signal) -> bool,
    ) -> Result<Signal, TestkitError> {
        self.awaited(what, |received| &received.spans, wanted).await
    }

    pub async fn log(
        &self,
        what: &str,
        wanted: impl Fn(&Signal) -> bool,
    ) -> Result<Signal, TestkitError> {
        self.awaited(what, |received| &received.logs, wanted).await
    }

    fn kept<T>(&self, read: impl FnOnce(&Received) -> T) -> T {
        read(&self.received.lock().unwrap_or_else(PoisonError::into_inner))
    }

    async fn awaited(
        &self,
        what: &str,
        of: impl Fn(&Received) -> &Vec<Signal>,
        wanted: impl Fn(&Signal) -> bool,
    ) -> Result<Signal, TestkitError> {
        const WITHIN: Duration = Duration::from_secs(30);
        let find =
            || self.kept(|received| of(received).iter().find(|signal| wanted(signal)).cloned());
        crate::until(what, WITHIN, async || find().is_some()).await?;
        find().ok_or_else(|| TestkitError::Never(what.to_string()))
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn traces(State(received): State<Kept>, body: Bytes) -> StatusCode {
    keep(
        &received,
        ExportTraceServiceRequest::decode(body),
        |kept, request| {
            kept.spans
                .extend(request.resource_spans.into_iter().flat_map(|of_resource| {
                    let resource = resource_attributes(of_resource.resource);
                    of_resource
                        .scope_spans
                        .into_iter()
                        .flat_map(|of_scope| of_scope.spans)
                        .map(move |span| Signal {
                            text: span.name,
                            trace_id: hex::encode(span.trace_id),
                            span_id: hex::encode(span.span_id),
                            parent_span_id: hex::encode(span.parent_span_id),
                            attributes: attributes(span.attributes),
                            resource: resource.clone(),
                        })
                }))
        },
    )
}

async fn logs(State(received): State<Kept>, body: Bytes) -> StatusCode {
    keep(
        &received,
        ExportLogsServiceRequest::decode(body),
        |kept, request| {
            kept.logs
                .extend(request.resource_logs.into_iter().flat_map(|of_resource| {
                    let resource = resource_attributes(of_resource.resource);
                    of_resource
                        .scope_logs
                        .into_iter()
                        .flat_map(|of_scope| of_scope.log_records)
                        .map(move |record| Signal {
                            text: record.body.map(text).unwrap_or_default(),
                            trace_id: hex::encode(record.trace_id),
                            span_id: hex::encode(record.span_id),
                            parent_span_id: String::new(),
                            attributes: attributes(record.attributes),
                            resource: resource.clone(),
                        })
                }))
        },
    )
}

fn keep<R>(
    received: &Kept,
    decoded: Result<R, prost::DecodeError>,
    add: impl FnOnce(&mut Received, R),
) -> StatusCode {
    match decoded {
        Ok(request) => {
            add(
                &mut received.lock().unwrap_or_else(PoisonError::into_inner),
                request,
            );
            StatusCode::OK
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

fn resource_attributes(resource: Option<Resource>) -> BTreeMap<String, String> {
    attributes(
        resource
            .map(|resource| resource.attributes)
            .unwrap_or_default(),
    )
}

fn attributes(pairs: Vec<KeyValue>) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .map(|pair| (pair.key, pair.value.map(text).unwrap_or_default()))
        .collect()
}

fn text(value: AnyValue) -> String {
    match value.value {
        Some(any_value::Value::StringValue(text)) => text,
        Some(any_value::Value::BoolValue(flag)) => flag.to_string(),
        Some(any_value::Value::IntValue(number)) => number.to_string(),
        Some(any_value::Value::DoubleValue(number)) => number.to_string(),
        Some(other) => format!("{other:?}"),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    fn string(name: &str, value: &str) -> KeyValue {
        KeyValue {
            key: name.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
        }
    }

    fn resource() -> Option<Resource> {
        Some(Resource {
            attributes: vec![string("service.name", "cufflink-platform")],
            ..Default::default()
        })
    }

    async fn sent(capture: &Capture, path: &str, body: Vec<u8>) -> u16 {
        reqwest::Client::new()
            .post(format!("{}{path}", capture.endpoint))
            .body(body)
            .send()
            .await
            .expect("sent")
            .status()
            .as_u16()
    }

    #[tokio::test]
    async fn what_is_exported_is_kept_with_its_ids_attributes_and_resource() {
        let capture = Capture::start().await.expect("capture");
        let spans = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        name: "acme/notes/list_notes".into(),
                        trace_id: vec![0xab; 16],
                        span_id: vec![0x01; 8],
                        attributes: vec![
                            string("cufflink.tenant", "acme"),
                            string("cufflink.service", "notes"),
                            string("http.method", "GET"),
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let logs = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: resource(),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("listed".into())),
                        }),
                        trace_id: vec![0xab; 16],
                        span_id: vec![0x01; 8],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        assert_eq!(
            sent(&capture, "/v1/traces", spans.encode_to_vec()).await,
            200
        );
        assert_eq!(sent(&capture, "/v1/logs", logs.encode_to_vec()).await, 200);
        assert_eq!(sent(&capture, "/v1/logs", vec![0xff, 0xff]).await, 400);

        let span = capture
            .span("the span", |span| span.text == "acme/notes/list_notes")
            .await
            .expect("kept");
        assert_eq!(
            span.scope(),
            [("cufflink.service", "notes"), ("cufflink.tenant", "acme")]
        );
        assert_eq!(span.attribute("http.method"), Some("GET"));
        assert_eq!(span.trace_id, "ab".repeat(16));
        assert_eq!(span.span_id, "01".repeat(8));
        assert_eq!(span.parent_span_id, "");
        assert_eq!(
            span.resource.get("service.name").map(String::as_str),
            Some("cufflink-platform")
        );
        let log = capture
            .log("the record", |record| record.text == "listed")
            .await
            .expect("kept");
        assert_eq!(
            (&log.trace_id, &log.span_id),
            (&span.trace_id, &span.span_id)
        );
        assert_eq!(log.scope(), []);
        assert_eq!(capture.spans().len(), 1);
        assert_eq!(capture.logs().len(), 1);
    }
}
