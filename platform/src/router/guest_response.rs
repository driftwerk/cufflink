use super::HOP_BY_HOP_HEADERS;
use crate::errors::{GuestOriginated, PlatformResult};
use crate::policy::record::LogThrottle;
use crate::router::sse_bridge::SseSpec;
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose, Engine};
use cufflink_storage::ObjectReader;
use cufflink_types::delivery::{DeliveryControl, DELIVERY_KEY};
use futures_util::StreamExt;
use serde_json::{Map, Value};
use std::future::Future;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

const MAX_HEADERS: usize = 64;
const MAX_HEADER_VALUE: usize = 8 * 1024;

const DENIED_HEADERS: [&str; 4] = [
    "content-length",
    "set-cookie",
    "strict-transport-security",
    "x-request-id",
];
const DENIED_PREFIXES: [&str; 2] = ["proxy-", "access-control-"];

fn denied(name: &HeaderName) -> bool {
    let name = name.as_str();
    HOP_BY_HOP_HEADERS.contains(&name)
        || DENIED_HEADERS.contains(&name)
        || DENIED_PREFIXES.iter().any(|p| name.starts_with(p))
}

#[derive(Debug)]
pub(crate) enum GuestResponse {
    Legacy { status: StatusCode, body: Value },
    V2(V2Response),
}

#[derive(Debug)]
pub(crate) struct V2Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: V2Body,
    pub dropped: Vec<HeaderName>,
    pub delivery: Option<Box<DeliveryControl>>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum V2Body {
    Empty,
    Json(Value),
    Bytes {
        content_type: HeaderValue,
        bytes: Bytes,
    },
    S3(S3Spec),
    Sse(Box<SseSpec>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct S3Spec {
    pub bucket: String,
    pub key: String,
    pub content_type: Option<HeaderValue>,
}

impl V2Body {
    fn label(&self) -> &'static str {
        match self {
            V2Body::Empty => "empty",
            V2Body::Json(_) => "json",
            V2Body::Bytes { .. } => "bytes",
            V2Body::S3(_) => "s3",
            V2Body::Sse(_) => "sse",
        }
    }
}

pub(crate) trait Bodies {
    fn open(&self, spec: &S3Spec) -> impl Future<Output = PlatformResult<ObjectReader>> + Send;
    fn sse(&mut self, spec: SseSpec) -> PlatformResult<Response>;
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GuestResponseError(String);

impl std::fmt::Display for GuestResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid response {}", self.0)
    }
}

fn invalid(what: impl Into<String>) -> GuestResponseError {
    GuestResponseError(what.into())
}

pub(crate) fn decode(raw: String) -> Result<GuestResponse, GuestResponseError> {
    let value: Value = serde_json::from_str(&raw).unwrap_or(Value::String(raw));
    match value {
        Value::Object(envelope) if envelope.get("__v") == Some(&Value::from(2)) => {
            decode_v2(envelope).map(GuestResponse::V2)
        }
        legacy => Ok(decode_legacy(legacy)),
    }
}

fn decode_legacy(value: Value) -> GuestResponse {
    let (status, body) = match value.get("__status") {
        Some(status) => (
            status.as_u64().unwrap_or(200) as u16,
            value
                .get("__body")
                .cloned()
                .unwrap_or_else(|| value.clone()),
        ),
        None => (200, value),
    };
    GuestResponse::Legacy {
        status: StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        body,
    }
}

fn decode_v2(mut envelope: Map<String, Value>) -> Result<V2Response, GuestResponseError> {
    let status = envelope
        .get("__status")
        .and_then(Value::as_u64)
        .and_then(|status| u16::try_from(status).ok())
        .filter(|status| (200..=599).contains(status))
        .and_then(|status| StatusCode::from_u16(status).ok())
        .ok_or_else(|| invalid("status"))?;
    let (headers, dropped) = headers(envelope.get("__headers"))?;
    let delivery = envelope
        .remove(DELIVERY_KEY)
        .filter(|delivery| !delivery.is_null())
        .map(|delivery| {
            serde_json::from_value(delivery)
                .map(Box::new)
                .map_err(|_| invalid(DELIVERY_KEY))
        })
        .transpose()?;
    let body = body(&mut envelope)?;
    let bodiless = status == StatusCode::NO_CONTENT || status == StatusCode::NOT_MODIFIED;
    Ok(V2Response {
        status,
        headers,
        body: if bodiless { V2Body::Empty } else { body },
        dropped,
        delivery,
    })
}

fn headers(value: Option<&Value>) -> Result<(HeaderMap, Vec<HeaderName>), GuestResponseError> {
    let pairs = match value {
        None | Some(Value::Null) => return Ok((HeaderMap::new(), Vec::new())),
        Some(Value::Object(pairs)) if pairs.len() <= MAX_HEADERS => pairs,
        Some(_) => return Err(invalid("headers")),
    };
    pairs.iter().try_fold(
        (HeaderMap::new(), Vec::new()),
        |(mut allowed, mut dropped), (name, value)| {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| invalid(format!("header name '{name}'")))?;
            let value = value
                .as_str()
                .filter(|value| value.len() <= MAX_HEADER_VALUE)
                .and_then(|value| HeaderValue::from_str(value).ok())
                .ok_or_else(|| invalid(format!("header value of '{name}'")))?;
            if denied(&name) {
                dropped.push(name);
            } else {
                allowed.insert(name, value);
            }
            Ok((allowed, dropped))
        },
    )
}

fn body(envelope: &mut Map<String, Value>) -> Result<V2Body, GuestResponseError> {
    match (
        envelope.remove("__body"),
        envelope.remove("__body_b64"),
        envelope.remove("__stream"),
        envelope.remove("__sse"),
    ) {
        (Some(json), None, None, None) => Ok(V2Body::Json(json)),
        (None, None, Some(stream), None) => s3_spec(&stream).map(V2Body::S3),
        (None, None, None, Some(sse)) => SseSpec::parse(&sse)
            .map(|spec| V2Body::Sse(Box::new(spec)))
            .map_err(invalid),
        (None, None, None, None) => Ok(V2Body::Empty),
        (None, Some(encoded), None, None) => {
            let bytes = encoded
                .as_str()
                .and_then(|encoded| general_purpose::STANDARD.decode(encoded).ok())
                .ok_or_else(|| invalid("__body_b64"))?;
            let content_type = match envelope.get("__content_type") {
                None => HeaderValue::from_static("application/octet-stream"),
                Some(content_type) => content_type
                    .as_str()
                    .and_then(|ct| HeaderValue::from_str(ct).ok())
                    .ok_or_else(|| invalid("__content_type"))?,
            };
            Ok(V2Body::Bytes {
                content_type,
                bytes: Bytes::from(bytes),
            })
        }
        _ => Err(invalid(
            "body: more than one of __body, __body_b64, __stream, __sse",
        )),
    }
}

fn s3_spec(stream: &Value) -> Result<S3Spec, GuestResponseError> {
    let text = |key: &str| {
        stream[key]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| invalid(format!("__stream.{key}")))
    };
    if stream["kind"] != "s3" {
        return Err(invalid("__stream.kind"));
    }
    let content_type = match &stream["content_type"] {
        Value::Null => None,
        value => Some(
            value
                .as_str()
                .and_then(|ct| HeaderValue::from_str(ct).ok())
                .ok_or_else(|| invalid("__stream.content_type"))?,
        ),
    };
    Ok(S3Spec {
        bucket: text("bucket")?,
        key: text("key")?,
        content_type,
    })
}

impl GuestResponse {
    pub(crate) async fn render(self, service: &str, bodies: &mut impl Bodies) -> Response {
        let rendered = match self {
            GuestResponse::Legacy { status, body } => Ok((status, Json(body)).into_response()),
            GuestResponse::V2(v2) => render_v2(v2, service, bodies).await,
        };
        match rendered {
            Ok(mut response) => {
                response.extensions_mut().insert(GuestOriginated);
                response
            }
            Err(error) => error.into_response(),
        }
    }
}

async fn render_v2(
    v2: V2Response,
    service: &str,
    bodies: &mut impl Bodies,
) -> PlatformResult<Response> {
    metrics::counter!("cufflink_guest_response_v2_total", "body" => v2.body.label()).increment(1);
    v2.dropped
        .iter()
        .for_each(|name| dropped_header(service, name));
    let mut response = match v2.body {
        V2Body::Empty => Body::empty().into_response(),
        V2Body::Json(value) => Json(value).into_response(),
        V2Body::Bytes {
            content_type,
            bytes,
        } => ([(header::CONTENT_TYPE, content_type)], bytes).into_response(),
        V2Body::S3(spec) => stream_object(&spec, bodies.open(&spec).await?),
        V2Body::Sse(spec) => bodies.sse(*spec)?,
    };
    *response.status_mut() = v2.status;
    response.headers_mut().extend(v2.headers);
    Ok(response)
}

fn stream_object(spec: &S3Spec, object: ObjectReader) -> Response {
    let content_type = spec
        .content_type
        .clone()
        .or_else(|| {
            object
                .content_type
                .as_deref()
                .and_then(|ct| HeaderValue::from_str(ct).ok())
        })
        .unwrap_or(HeaderValue::from_static("application/octet-stream"));
    let stream = object.stream.inspect(|chunk| {
        if let Ok(chunk) = chunk {
            metrics::counter!("cufflink_s3_stream_bytes_total").increment(chunk.len() as u64);
        }
    });
    let mut response = Body::from_stream(stream).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    if let Some(size) = object.size {
        response
            .headers_mut()
            .insert(header::CONTENT_LENGTH, HeaderValue::from(size));
    }
    response
}

static DROPPED_HEADERS: LazyLock<LogThrottle<(String, HeaderName)>> =
    LazyLock::new(|| LogThrottle::new(Duration::from_secs(60)));

fn dropped_header(service: &str, name: &HeaderName) {
    metrics::counter!("cufflink_guest_header_dropped_total", "name" => name.as_str().to_string())
        .increment(1);
    if DROPPED_HEADERS.should_log((service.to_string(), name.clone()), Instant::now()) {
        tracing::warn!(service, header = %name, "Dropped a response header the platform owns");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn legacy(raw: &str) -> (u16, Value) {
        match decode(raw.to_string()) {
            Ok(GuestResponse::Legacy { status, body }) => (status.as_u16(), body),
            other => panic!("{raw}: {other:?}"),
        }
    }

    fn v2(envelope: &Value) -> Result<V2Response, GuestResponseError> {
        match decode(envelope.to_string())? {
            GuestResponse::V2(v2) => Ok(v2),
            other => panic!("{envelope}: {other:?}"),
        }
    }

    #[test]
    fn legacy_decoding_is_unchanged() {
        for (raw, status, body) in [
            (r#"{"ok":true}"#, 200, json!({"ok": true})),
            (
                r#"{"__status":201,"__body":{"id":1}}"#,
                201,
                json!({"id": 1}),
            ),
            (
                r#"{"__status":404,"__body":{"error":"x"}}"#,
                404,
                json!({"error": "x"}),
            ),
            (r#"{"__status":204}"#, 204, json!({"__status": 204})),
            (r#"{"__status":"201","__body":1}"#, 200, json!(1)),
            (r#"{"__status":65737,"__body":1}"#, 201, json!(1)),
            (r#"{"__status":0,"__body":1}"#, 200, json!(1)),
            (r#"{"__status":1000,"__body":1}"#, 200, json!(1)),
            (r#"{"__status":-1,"__body":1}"#, 200, json!(1)),
            (r#"[{"__status":500}]"#, 200, json!([{"__status": 500}])),
            ("not json", 200, json!("not json")),
            ("", 200, json!("")),
            (r#"{"__v":1,"__status":201,"__body":1}"#, 201, json!(1)),
            (r#"{"__v":"2","__status":201,"__body":1}"#, 201, json!(1)),
        ] {
            assert_eq!(legacy(raw), (status, body), "{raw}");
        }
    }

    #[test]
    fn v2_envelopes_carry_status_headers_and_bodies() {
        let created = v2(
            &json!({"__v": 2, "__status": 201, "__headers": {"x-total": "1"}, "__body": {"id": 1}}),
        )
        .expect("v2");
        assert_eq!(created.status, StatusCode::CREATED);
        assert_eq!(created.headers["x-total"], "1");
        assert_eq!(created.body, V2Body::Json(json!({"id": 1})));

        let csv = v2(
            &json!({"__v": 2, "__status": 200, "__content_type": "text/csv", "__body_b64": "YSxi"}),
        )
        .expect("v2");
        assert_eq!(
            csv.body,
            V2Body::Bytes {
                content_type: HeaderValue::from_static("text/csv"),
                bytes: Bytes::from_static(b"a,b"),
            }
        );
        let untyped = v2(&json!({"__v": 2, "__status": 200, "__body_b64": ""})).expect("v2");
        assert!(
            matches!(&untyped.body, V2Body::Bytes { content_type, .. } if content_type == "application/octet-stream")
        );
        let empty = v2(&json!({"__v": 2, "__status": 200})).expect("v2");
        assert_eq!(empty.body, V2Body::Empty);
    }

    #[test]
    fn a_delivery_block_is_read_and_a_malformed_one_refused() {
        let delivery = |block: Value| {
            v2(&json!({"__v": 2, "__status": 503, "__body": {}, "__delivery": block}))
                .map(|response| response.delivery)
        };
        assert_eq!(
            delivery(json!({"retry_after_ms": 1500})),
            Ok(Some(Box::new(DeliveryControl::RetryAfter {
                retry_after_ms: 1500
            })))
        );
        assert_eq!(
            delivery(json!({"terminal": "unknown book"})),
            Ok(Some(Box::new(DeliveryControl::Terminal {
                terminal: "unknown book".into()
            })))
        );
        assert_eq!(delivery(Value::Null), Ok(None));
        assert_eq!(
            v2(&json!({"__v": 2, "__status": 200}))
                .expect("v2")
                .delivery,
            None
        );
        for malformed in [
            json!({}),
            json!({"retry_after_ms": -1}),
            json!({"retry_after_ms": 1, "terminal": "both"}),
            json!("terminal"),
        ] {
            assert_eq!(
                delivery(malformed.clone()),
                Err(invalid("__delivery")),
                "{malformed}"
            );
        }
    }

    #[test]
    fn s3_streams_name_a_bucket_and_key() {
        let spec = |stream: Value| match v2(&json!({"__v": 2, "__status": 200, "__stream": stream}))
        {
            Ok(V2Response {
                body: V2Body::S3(spec),
                ..
            }) => Ok(spec),
            Ok(other) => panic!("{other:?}"),
            Err(e) => Err(e),
        };
        assert_eq!(
            spec(
                json!({"kind": "s3", "bucket": "media", "key": "a.pdf", "content_type": "application/pdf"})
            ),
            Ok(S3Spec {
                bucket: "media".into(),
                key: "a.pdf".into(),
                content_type: Some(HeaderValue::from_static("application/pdf")),
            })
        );
        assert!(
            spec(json!({"kind": "s3", "bucket": "media", "key": "a.pdf"}))
                .is_ok_and(|s| s.content_type.is_none())
        );
        for stream in [
            json!({"kind": "gcs", "bucket": "m", "key": "k"}),
            json!({"kind": "s3", "bucket": "", "key": "k"}),
            json!({"kind": "s3", "bucket": "m"}),
            json!({"kind": "s3", "bucket": "m", "key": "k", "content_type": 7}),
            json!("s3://m/k"),
        ] {
            assert!(spec(stream.clone()).is_err(), "{stream}");
        }
        assert!(v2(&json!({"__v": 2, "__status": 200, "__body": 1,
            "__stream": {"kind": "s3", "bucket": "m", "key": "k"}}))
        .is_err());
    }

    #[tokio::test]
    async fn s3_bodies_stream_with_length_and_type_or_answer_the_open_error() {
        let response = rendered(
            &json!({"__v": 2, "__status": 200, "__headers": {"content-disposition": "inline"},
                "__stream": {"kind": "s3", "bucket": "m", "key": "report.csv"}})
            .to_string(),
        )
        .await;
        assert!(response.extensions().get::<GuestOriginated>().is_some());
        assert_eq!(response.headers()[header::CONTENT_TYPE], "text/csv");
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "3");
        assert_eq!(response.headers()[header::CONTENT_DISPOSITION], "inline");
        let body = axum::body::to_bytes(response.into_body(), 16)
            .await
            .expect("body");
        assert_eq!(&body[..], b"a,b");

        let typed = rendered(
            &json!({"__v": 2, "__status": 200,
                "__stream": {"kind": "s3", "bucket": "m", "key": "report.csv", "content_type": "application/x-csv"}})
            .to_string(),
        )
        .await;
        assert_eq!(typed.headers()[header::CONTENT_TYPE], "application/x-csv");

        let missing = rendered(
            &json!({"__v": 2, "__status": 200, "__stream": {"kind": "s3", "bucket": "m", "key": "gone"}})
                .to_string(),
        )
        .await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
        assert!(missing.extensions().get::<GuestOriginated>().is_none());
        assert!(missing
            .extensions()
            .get::<crate::errors::PlatformErrorBody>()
            .is_some());
    }

    #[test]
    fn statuses_without_a_body_force_it_empty() {
        for status in [204, 304] {
            let response = v2(&json!({"__v": 2, "__status": status, "__body": {"ignored": true}}))
                .expect("v2");
            assert_eq!(response.body, V2Body::Empty, "{status}");
        }
    }

    #[test]
    fn platform_owned_headers_are_dropped_and_reported() {
        let response = v2(&json!({"__v": 2, "__status": 200, "__headers": {
            "set-cookie": "s=1", "Content-Length": "9", "transfer-encoding": "chunked",
            "access-control-allow-origin": "*", "strict-transport-security": "max-age=1",
            "x-request-id": "r", "proxy-authenticate": "x", "connection": "close",
            "cache-control": "no-store",
        }}))
        .expect("v2");
        assert_eq!(response.headers.len(), 1);
        assert_eq!(response.headers["cache-control"], "no-store");
        assert_eq!(response.dropped.len(), 8);
    }

    #[test]
    fn malformed_envelopes_are_errors() {
        let too_many: Map<String, Value> = (0..=MAX_HEADERS)
            .map(|i| (format!("x-h{i}"), json!("v")))
            .collect();
        for envelope in [
            json!({"__v": 2, "__status": 99}),
            json!({"__v": 2, "__status": 100}),
            json!({"__v": 2, "__status": 101}),
            json!({"__v": 2, "__status": 199}),
            json!({"__v": 2, "__status": 600}),
            json!({"__v": 2, "__status": "200"}),
            json!({"__v": 2}),
            json!({"__v": 2, "__status": 200, "__headers": {"bad name": "v"}}),
            json!({"__v": 2, "__status": 200, "__headers": {"x-a": "line\nbreak"}}),
            json!({"__v": 2, "__status": 200, "__headers": {"x-a": 1}}),
            json!({"__v": 2, "__status": 200, "__headers": {"x-a": "v".repeat(MAX_HEADER_VALUE + 1)}}),
            json!({"__v": 2, "__status": 200, "__headers": too_many}),
            json!({"__v": 2, "__status": 200, "__headers": ["x-a", "v"]}),
            json!({"__v": 2, "__status": 200, "__body_b64": "%%%"}),
            json!({"__v": 2, "__status": 200, "__body_b64": "", "__content_type": "a\nb"}),
            json!({"__v": 2, "__status": 200, "__body": 1, "__body_b64": ""}),
        ] {
            assert!(v2(&envelope).is_err(), "{envelope}");
        }
    }

    struct Objects;

    impl Bodies for Objects {
        fn sse(&mut self, _spec: SseSpec) -> PlatformResult<Response> {
            Err(crate::errors::PlatformError::Internal(
                "no event stream".into(),
            ))
        }

        async fn open(&self, spec: &S3Spec) -> PlatformResult<ObjectReader> {
            match spec.key.as_str() {
                "report.csv" => Ok(ObjectReader {
                    size: Some(3),
                    content_type: Some("text/csv".into()),
                    stream: futures_util::stream::iter([Ok(Bytes::from_static(b"a,b"))]).boxed(),
                }),
                _ => Err(crate::errors::PlatformError::NotFound("Not found".into())),
            }
        }
    }

    async fn rendered(raw: &str) -> Response {
        decode(raw.to_string())
            .expect("decodes")
            .render("svc", &mut Objects)
            .await
    }

    #[tokio::test]
    async fn guest_responses_are_marked_as_the_guests() {
        let response = rendered(r#"{"__status":404,"__body":{"error":"no such order"}}"#).await;
        assert!(response.extensions().get::<GuestOriginated>().is_some());
        assert!(response
            .extensions()
            .get::<crate::errors::PlatformErrorBody>()
            .is_none());
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        assert_eq!(&body[..], br#"{"error":"no such order"}"#);
    }

    #[tokio::test]
    async fn v2_renders_bytes_with_their_headers_and_no_content_empty() {
        let response = rendered(
            &json!({"__v": 2, "__status": 200,
                "__headers": {"content-disposition": "attachment; filename=\"a.xlsx\""},
                "__content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "__body_b64": "AQID"})
            .to_string(),
        )
        .await;
        assert!(response.extensions().get::<GuestOriginated>().is_some());
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"a.xlsx\""
        );
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        assert_eq!(&body[..], &[1, 2, 3]);

        let response = rendered(r#"{"__v":2,"__status":204,"__headers":{}}"#).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(response.headers().get(header::CONTENT_TYPE).is_none());
        assert!(axum::body::to_bytes(response.into_body(), 16)
            .await
            .expect("body")
            .is_empty());

        let response = rendered(
            r#"{"__v":2,"__status":200,"__headers":{"content-type":"application/problem+json"},"__body":{"a":1}}"#,
        )
        .await;
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/problem+json"
        );
    }
}
