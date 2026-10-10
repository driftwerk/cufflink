//! `http_fetch_v2`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchRequestV2 {
    pub method: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(
        default,
        rename = "body_b64",
        with = "crate::serde_util::base64_bytes::option",
        skip_serializing_if = "Option::is_none"
    )]
    pub body: Option<Vec<u8>>,
    /// Assembled into a `multipart/form-data` body by the host; exclusive
    /// with `body`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart: Option<Vec<MultipartPart>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub follow_redirects: bool,
}

impl FetchRequestV2 {
    pub const METHODS: [&'static str; 7] =
        ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MultipartPart {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(rename = "body_b64", with = "crate::serde_util::base64_bytes")]
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum FetchResponseV2 {
    /// Any response the server gave, whatever its status.
    Ok {
        status: u16,
        /// In the order received, repeated names kept.
        headers: Vec<(String, String)>,
        #[serde(rename = "body_b64", with = "crate::serde_util::base64_bytes")]
        body: Vec<u8>,
    },
    Error(HttpErrorEnvelope),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpErrorEnvelope {
    pub kind: HttpErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpErrorKind {
    Denied,
    InvalidRequest,
    Dns,
    Connect,
    Tls,
    Timeout,
    TooLarge,
    Redirect,
    Io,
}

impl HttpErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            HttpErrorKind::Denied => "denied",
            HttpErrorKind::InvalidRequest => "invalid_request",
            HttpErrorKind::Dns => "dns",
            HttpErrorKind::Connect => "connect",
            HttpErrorKind::Tls => "tls",
            HttpErrorKind::Timeout => "timeout",
            HttpErrorKind::TooLarge => "too_large",
            HttpErrorKind::Redirect => "redirect",
            HttpErrorKind::Io => "io",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    #[test]
    fn a_minimal_request_carries_only_what_it_sets() {
        round_trip(
            &FetchRequestV2 {
                method: "GET".into(),
                url: "https://api.example.com/v1?expand[]=a&expand[]=b".into(),
                ..Default::default()
            },
            json!({"method": "GET", "url": "https://api.example.com/v1?expand[]=a&expand[]=b"}),
        );
    }

    #[test]
    fn bodies_and_parts_travel_as_base64() {
        round_trip(
            &FetchRequestV2 {
                method: "POST".into(),
                url: "https://a.example/".into(),
                headers: vec![("accept".into(), "application/pdf".into())],
                body: Some(vec![0, 255]),
                timeout_ms: Some(1000),
                max_response_bytes: Some(1024),
                follow_redirects: true,
                ..Default::default()
            },
            json!({
                "method": "POST",
                "url": "https://a.example/",
                "headers": [["accept", "application/pdf"]],
                "body_b64": "AP8=",
                "timeout_ms": 1000,
                "max_response_bytes": 1024,
                "follow_redirects": true
            }),
        );
        round_trip(
            &FetchRequestV2 {
                method: "POST".into(),
                url: "https://pdf.example.com/forms/chromium/convert/html".into(),
                multipart: Some(vec![
                    MultipartPart {
                        name: "files".into(),
                        filename: Some("index.html".into()),
                        content_type: Some("text/html".into()),
                        body: b"<p>".to_vec(),
                    },
                    MultipartPart {
                        name: "landscape".into(),
                        body: b"true".to_vec(),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            },
            json!({
                "method": "POST",
                "url": "https://pdf.example.com/forms/chromium/convert/html",
                "multipart": [
                    {"name": "files", "filename": "index.html", "content_type": "text/html", "body_b64": "PHA+"},
                    {"name": "landscape", "body_b64": "dHJ1ZQ=="}
                ]
            }),
        );
    }

    #[test]
    fn responses_are_tagged_by_result() {
        round_trip(
            &FetchResponseV2::Ok {
                status: 404,
                headers: vec![
                    ("set-cookie".into(), "a=1".into()),
                    ("set-cookie".into(), "b=2".into()),
                ],
                body: b"no".to_vec(),
            },
            json!({
                "result": "ok",
                "status": 404,
                "headers": [["set-cookie", "a=1"], ["set-cookie", "b=2"]],
                "body_b64": "bm8="
            }),
        );
        for kind in [
            HttpErrorKind::Denied,
            HttpErrorKind::InvalidRequest,
            HttpErrorKind::Dns,
            HttpErrorKind::Connect,
            HttpErrorKind::Tls,
            HttpErrorKind::Timeout,
            HttpErrorKind::TooLarge,
            HttpErrorKind::Redirect,
            HttpErrorKind::Io,
        ] {
            round_trip(
                &FetchResponseV2::Error(HttpErrorEnvelope {
                    kind,
                    message: "why".into(),
                }),
                json!({"result": "error", "kind": kind.as_str(), "message": "why"}),
            );
        }
    }

    #[test]
    fn unknown_tags_kinds_and_bodies_are_refused() {
        refused::<FetchResponseV2>(json!({"result": "redirected"}));
        refused::<FetchResponseV2>(json!({"result": "error", "kind": "proxy", "message": ""}));
        refused::<FetchResponseV2>(json!({"status": 200, "headers": [], "body_b64": ""}));
        refused::<FetchRequestV2>(json!({"method": "GET", "url": "u", "body_b64": "%%"}));
        refused::<FetchRequestV2>(json!({"method": "GET", "url": "u", "retries": 2}));
        refused::<FetchRequestV2>(json!({"method": "GET"}));
    }
}
