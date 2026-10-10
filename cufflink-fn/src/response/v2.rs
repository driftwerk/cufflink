//! Response v2: any status, headers, and non-JSON bodies, sent to the
//! platform in the `__v: 2` envelope. A module using it declares
//! `response_v2`, so it only deploys to a platform that renders it.

use super::{Repr, Response};
use crate::sse::SseSpec;
use base64::{engine::general_purpose, Engine};
use cufflink_types::delivery::{DeliveryControl, DELIVERY_KEY};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// A redirect's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Redirect {
    /// 301
    MovedPermanently,
    /// 302
    Found,
    /// 303: the client follows with a GET.
    SeeOther,
    /// 307: the client repeats the method and body.
    Temporary,
    /// 308: the client repeats the method and body.
    Permanent,
}

impl Redirect {
    pub const fn status(self) -> u16 {
        match self {
            Redirect::MovedPermanently => 301,
            Redirect::Found => 302,
            Redirect::SeeOther => 303,
            Redirect::Temporary => 307,
            Redirect::Permanent => 308,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct V2 {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Body,
    delivery: Option<DeliveryControl>,
}

#[derive(Debug, Clone)]
enum Body {
    Empty,
    Json(Value),
    Bytes { content_type: String, data: Vec<u8> },
    S3(S3Object),
    Sse(SseSpec),
}

/// An object the platform streams from storage as the response body, so a
/// large file never passes through the guest. The guest's own storage
/// permissions apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Object {
    pub bucket: String,
    pub key: String,
    /// Defaults to the object's stored content type.
    pub content_type: Option<String>,
    pub disposition: Option<Disposition>,
}

impl V2 {
    fn new(status: u16, body: Body) -> Self {
        Self {
            status,
            headers: BTreeMap::new(),
            body,
            delivery: None,
        }
    }

    pub(super) fn status(&self) -> u16 {
        self.status
    }

    pub(super) fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    pub(super) fn delivery(&self) -> Option<&DeliveryControl> {
        self.delivery.as_ref()
    }

    pub(super) fn body_bytes(&self) -> Vec<u8> {
        match &self.body {
            Body::Json(value) => value.to_string().into_bytes(),
            Body::Bytes { data, .. } => data.clone(),
            Body::Empty | Body::S3(_) | Body::Sse(_) => Vec::new(),
        }
    }

    pub(super) fn with_status(self, status: u16) -> Self {
        Self { status, ..self }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .insert(name.to_ascii_lowercase(), value.to_string());
        self
    }

    pub(super) fn envelope(self) -> Value {
        let mut envelope = Map::from_iter([
            ("__v".to_string(), json!(2)),
            ("__status".to_string(), json!(self.status)),
            ("__headers".to_string(), json!(self.headers)),
        ]);
        match self.body {
            Body::Empty => {}
            Body::Json(value) => {
                envelope.insert("__body".into(), value);
            }
            Body::Bytes { content_type, data } => {
                envelope.insert("__content_type".into(), Value::String(content_type));
                envelope.insert(
                    "__body_b64".into(),
                    Value::String(general_purpose::STANDARD.encode(data)),
                );
            }
            Body::Sse(spec) => {
                envelope.insert("__sse".into(), spec.envelope());
            }
            Body::S3(object) => {
                envelope.insert(
                    "__stream".into(),
                    json!({
                        "kind": "s3",
                        "bucket": object.bucket,
                        "key": object.key,
                        "content_type": object.content_type,
                    }),
                );
            }
        }
        envelope.extend(
            self.delivery
                .map(|delivery| (DELIVERY_KEY.to_string(), json!(delivery))),
        );
        Value::Object(envelope)
    }
}

/// How a browser presents a downloaded body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// Saved as a file of this name.
    Attachment(String),
    /// Shown in the browser, saved under this name.
    Inline(String),
}

impl Disposition {
    /// The `Content-Disposition` value: `attachment; filename="report.xlsx"`,
    /// plus an RFC 5987 `filename*` carrying the exact name when it is not
    /// ASCII.
    pub fn header_value(&self) -> String {
        let (kind, filename) = match self {
            Disposition::Attachment(filename) => ("attachment", filename),
            Disposition::Inline(filename) => ("inline", filename),
        };
        let fallback: String = filename
            .chars()
            .map(|c| match c {
                '"' | '\\' => format!("\\{c}"),
                c if c.is_ascii() && !c.is_ascii_control() => c.to_string(),
                _ => "_".to_string(),
            })
            .collect();
        if filename.is_ascii() && !filename.chars().any(|c| c.is_ascii_control()) {
            format!("{kind}; filename=\"{fallback}\"")
        } else {
            format!(
                "{kind}; filename=\"{fallback}\"; filename*=UTF-8''{}",
                rfc5987(filename)
            )
        }
    }
}

/// `value` percent-encoded outside RFC 5987's `attr-char`.
fn rfc5987(value: &str) -> String {
    value
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'!'
            | b'#'
            | b'$'
            | b'&'
            | b'+'
            | b'-'
            | b'.'
            | b'^'
            | b'_'
            | b'`'
            | b'|'
            | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

impl Response {
    fn v2(v2: V2) -> Self {
        Self {
            repr: Repr::V2(Box::new(v2)),
        }
    }

    /// 204 No Content, with an empty body.
    pub fn no_content() -> Self {
        Self::v2(V2::new(204, Body::Empty))
    }

    /// A 200 with `body` as-is under `content_type`.
    pub fn bytes(content_type: &str, body: Vec<u8>) -> Self {
        Self::v2(V2::new(
            200,
            Body::Bytes {
                content_type: content_type.to_string(),
                data: body,
            },
        ))
    }

    /// A file download: `Content-Disposition: attachment`.
    pub fn attachment(filename: &str, content_type: &str, body: Vec<u8>) -> Self {
        Self::bytes(content_type, body).header(
            "content-disposition",
            &Disposition::Attachment(filename.to_string()).header_value(),
        )
    }

    /// A file the browser shows: `Content-Disposition: inline`.
    pub fn inline(filename: &str, content_type: &str, body: Vec<u8>) -> Self {
        Self::bytes(content_type, body).header(
            "content-disposition",
            &Disposition::Inline(filename.to_string()).header_value(),
        )
    }

    /// `text/plain; charset=utf-8`.
    pub fn plain(text: &str) -> Self {
        Self::bytes("text/plain; charset=utf-8", text.as_bytes().to_vec())
    }

    /// The object at `bucket`/`key`, streamed by the platform.
    pub fn s3_object(object: S3Object) -> Self {
        let disposition = object.disposition.as_ref().map(Disposition::header_value);
        let response = Self::v2(V2::new(200, Body::S3(object)));
        match disposition {
            Some(disposition) => response.header("content-disposition", &disposition),
            None => response,
        }
    }

    /// Serve the route's event stream (a route declared with `sse(...)`):
    /// `spec.initial` first, then the bridged subject's messages.
    pub fn sse(spec: SseSpec) -> Self {
        let headers = spec.headers.clone();
        headers.iter().fold(
            Self::v2(V2::new(200, Body::Sse(spec))),
            |response, (name, value)| response.header(name, value),
        )
    }

    /// A redirect to `location`.
    pub fn redirect(redirect: Redirect, location: &str) -> Self {
        Self::v2(V2::new(redirect.status(), Body::Empty)).header("location", location)
    }

    /// Ask for the message or job this handler ran for to be delivered
    /// again after `delay`, whatever its consumer's backoff. To an HTTP
    /// caller it is a 503.
    #[cfg(feature = "delivery-control")]
    pub fn retry_after(delay: std::time::Duration) -> Self {
        Self::settling(
            503,
            "retry",
            DeliveryControl::RetryAfter {
                retry_after_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            },
        )
    }

    /// Say that the message or job this handler ran for can never
    /// succeed: it is not delivered again, and is dead-lettered with
    /// `reason`. To an HTTP caller it is a 422.
    #[cfg(feature = "delivery-control")]
    pub fn terminal(reason: &str) -> Self {
        Self::settling(
            422,
            reason,
            DeliveryControl::Terminal {
                terminal: reason.to_string(),
            },
        )
    }

    #[cfg(feature = "delivery-control")]
    fn settling(status: u16, error: &str, delivery: DeliveryControl) -> Self {
        Self::v2(V2 {
            delivery: Some(delivery),
            ..V2::new(status, Body::Json(json!({ "error": error })))
        })
    }

    /// Add a response header; a later value for the same name replaces an
    /// earlier one. The platform drops headers it owns (hop-by-hop,
    /// `content-length`, `set-cookie`, CORS, `strict-transport-security`,
    /// `x-request-id`).
    pub fn header(self, name: &str, value: &str) -> Self {
        let v2 = match self.repr {
            Repr::Legacy { data, status } => V2::new(
                status,
                Body::Json(serde_json::from_str(&data).unwrap_or(Value::String(data))),
            ),
            Repr::V2(v2) => *v2,
        };
        Self::v2(v2.with_header(name, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(response: Response) -> Value {
        serde_json::from_str(&response.into_data()).expect("envelope")
    }

    #[test]
    fn every_constructor_emits_its_v2_envelope() {
        for (response, expected) in [
            (
                Response::no_content(),
                json!({"__v": 2, "__status": 204, "__headers": {}}),
            ),
            (
                Response::bytes("text/csv", b"a,b".to_vec()),
                json!({"__v": 2, "__status": 200, "__headers": {}, "__content_type": "text/csv", "__body_b64": "YSxi"}),
            ),
            (
                Response::plain("hi"),
                json!({"__v": 2, "__status": 200, "__headers": {}, "__content_type": "text/plain; charset=utf-8", "__body_b64": "aGk="}),
            ),
            (
                Response::redirect(Redirect::SeeOther, "/books/7"),
                json!({"__v": 2, "__status": 303, "__headers": {"location": "/books/7"}}),
            ),
            (
                Response::created(&json!({"id": 1}))
                    .header("X-Total", "1")
                    .header("x-total", "2"),
                json!({"__v": 2, "__status": 201, "__headers": {"x-total": "2"}, "__body": {"id": 1}}),
            ),
            (
                Response::no_content()
                    .header("etag", "\"v1\"")
                    .with_status(202),
                json!({"__v": 2, "__status": 202, "__headers": {"etag": "\"v1\""}}),
            ),
        ] {
            assert_eq!(envelope(response), expected);
        }
    }

    #[test]
    fn a_v2_response_can_be_read_back() {
        let csv = Response::attachment("books.csv", "text/csv", b"a,b".to_vec());
        assert_eq!(csv.status(), 200);
        assert_eq!(csv.body_bytes(), b"a,b");
        assert_eq!(
            csv.headers().get("content-disposition").map(String::as_str),
            Some("attachment; filename=\"books.csv\"")
        );
        assert!(csv.json_as::<Value>().is_err());
        let created = Response::created(&json!({"id": 1})).header("X-Total", "1");
        assert_eq!(created.status(), 201);
        assert_eq!(created.json_as::<Value>().expect("json"), json!({"id": 1}));
        assert!(Response::no_content().body_bytes().is_empty());
        assert!(created.delivery().is_none());
    }

    #[cfg(feature = "delivery-control")]
    #[test]
    fn a_handler_can_say_how_its_message_is_settled() {
        let retry = Response::retry_after(std::time::Duration::from_millis(1_500));
        assert_eq!(
            retry.delivery(),
            Some(&DeliveryControl::RetryAfter {
                retry_after_ms: 1_500
            })
        );
        assert_eq!(retry.status(), 503);
        assert_eq!(
            envelope(retry),
            json!({
                "__v": 2,
                "__status": 503,
                "__headers": {},
                "__body": {"error": "retry"},
                "__delivery": {"retry_after_ms": 1500}
            })
        );
        let terminal = Response::terminal("unknown book");
        assert_eq!(
            terminal.delivery(),
            Some(&DeliveryControl::Terminal {
                terminal: "unknown book".into()
            })
        );
        assert_eq!(
            envelope(terminal.header("x-reason", "gone")),
            json!({
                "__v": 2,
                "__status": 422,
                "__headers": {"x-reason": "gone"},
                "__body": {"error": "unknown book"},
                "__delivery": {"terminal": "unknown book"}
            })
        );
        assert_eq!(
            Response::retry_after(std::time::Duration::MAX).delivery(),
            Some(&DeliveryControl::RetryAfter {
                retry_after_ms: u64::MAX
            })
        );
    }

    #[test]
    fn redirects_use_only_the_redirect_statuses() {
        assert_eq!(
            [
                Redirect::MovedPermanently,
                Redirect::Found,
                Redirect::SeeOther,
                Redirect::Temporary,
                Redirect::Permanent,
            ]
            .map(|redirect| envelope(Response::redirect(redirect, "/x"))["__status"].clone()),
            [301, 302, 303, 307, 308].map(Value::from)
        );
    }

    #[test]
    fn s3_objects_are_streamed_by_the_platform() {
        assert_eq!(
            envelope(Response::s3_object(S3Object {
                bucket: "media".into(),
                key: "tenants/acme/docs/q.pdf".into(),
                content_type: Some("application/pdf".into()),
                disposition: Some(Disposition::Attachment("q.pdf".into())),
            })),
            json!({"__v": 2, "__status": 200,
                "__headers": {"content-disposition": "attachment; filename=\"q.pdf\""},
                "__stream": {"kind": "s3", "bucket": "media", "key": "tenants/acme/docs/q.pdf", "content_type": "application/pdf"}})
        );
        assert_eq!(
            envelope(Response::s3_object(S3Object {
                bucket: "media".into(),
                key: "k".into(),
                content_type: None,
                disposition: None,
            }))["__stream"],
            json!({"kind": "s3", "bucket": "media", "key": "k", "content_type": null})
        );
    }

    #[test]
    fn event_streams_carry_their_spec_and_headers() {
        use crate::sse::{SseEvent, SseSpec};
        let response = Response::sse(SseSpec {
            initial: vec![SseEvent::data(json!({"type": "snapshot"}))],
            state: json!({"done": []}),
            headers: vec![("Cache-Control".into(), "no-cache".into())],
            ..SseSpec::default()
        });
        assert_eq!(
            envelope(response),
            json!({"__v": 2, "__status": 200, "__headers": {"cache-control": "no-cache"},
                "__sse": {"initial": [{"data": {"type": "snapshot"}}], "state": {"done": []},
                    "keep_alive_secs": 15, "ping": null, "timeout_secs": 300}})
        );
    }

    #[test]
    fn downloads_carry_their_content_disposition() {
        let xlsx = envelope(Response::attachment(
            "test.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            vec![1, 2, 3],
        ));
        assert_eq!(
            xlsx["__headers"]["content-disposition"],
            "attachment; filename=\"test.xlsx\""
        );
        assert_eq!(
            xlsx["__content_type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        assert_eq!(xlsx["__body_b64"], "AQID");
        let pdf = envelope(Response::inline("receipt.pdf", "application/pdf", vec![]));
        assert_eq!(
            pdf["__headers"]["content-disposition"],
            "inline; filename=\"receipt.pdf\""
        );
    }

    #[test]
    fn content_disposition_escapes_quotes_and_encodes_non_ascii_names() {
        assert_eq!(
            Disposition::Attachment("a \"b\"\\.csv".into()).header_value(),
            r#"attachment; filename="a \"b\"\\.csv""#
        );
        assert_eq!(
            Disposition::Attachment("résumé 2026.pdf".into()).header_value(),
            "attachment; filename=\"r_sum_ 2026.pdf\"; filename*=UTF-8''r%C3%A9sum%C3%A9%202026.pdf"
        );
        assert_eq!(
            Disposition::Inline("a\nb.txt".into()).header_value(),
            "inline; filename=\"a_b.txt\"; filename*=UTF-8''a%0Ab.txt"
        );
    }
}
