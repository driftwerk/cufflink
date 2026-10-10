//! Binary bodies, multipart forms and failures that say what failed. Needs
//! a platform that supports `http_v2`; the `http-v2` feature declares that
//! in the module.

pub use cufflink_types::host::http::HttpErrorKind;
use cufflink_types::host::http::{
    FetchRequestV2, FetchResponseV2, HttpErrorEnvelope, MultipartPart,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;

const CONTENT_TYPE: &str = "content-type";

/// A request under construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest(FetchRequestV2);

impl HttpRequest {
    /// `method` is one of GET, POST, PUT, PATCH, DELETE, HEAD and OPTIONS.
    /// The URL is sent as written: a query's repeated keys stay.
    pub fn new(method: &str, url: &str) -> Self {
        Self(FetchRequestV2 {
            method: method.to_string(),
            url: url.to_string(),
            ..Default::default()
        })
    }

    pub fn get(url: &str) -> Self {
        Self::new("GET", url)
    }

    pub fn post(url: &str) -> Self {
        Self::new("POST", url)
    }

    /// Adds a header; one added twice is sent twice.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.0.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn body(mut self, bytes: Vec<u8>) -> Self {
        self.0.body = Some(bytes);
        self
    }

    /// `value` as the body, with `content-type: application/json` in place
    /// of any content type set before.
    pub fn json<T: Serialize>(mut self, value: &T) -> Result<Self, HttpError> {
        let body = serde_json::to_vec(value).map_err(|e| {
            HttpError::new(
                HttpErrorKind::InvalidRequest,
                format!("the body could not be encoded as JSON: {e}"),
            )
        })?;
        self.0
            .headers
            .retain(|(name, _)| !name.eq_ignore_ascii_case(CONTENT_TYPE));
        Ok(self.header(CONTENT_TYPE, "application/json").body(body))
    }

    /// The platform's default when unset or `0`; never past what is left
    /// of the handler's own budget.
    pub fn timeout_ms(mut self, ms: u32) -> Self {
        self.0.timeout_ms = Some(ms);
        self
    }

    /// How much of a response to accept, up to the platform's ceiling; a
    /// longer one fails with [`HttpErrorKind::TooLarge`].
    pub fn max_response_bytes(mut self, bytes: u64) -> Self {
        self.0.max_response_bytes = Some(bytes);
        self
    }

    /// Off unless set: a redirect is returned as the response.
    pub fn follow_redirects(mut self, on: bool) -> Self {
        self.0.follow_redirects = on;
        self
    }
}

/// One part of a `multipart/form-data` body. The platform frames the body
/// and chooses its boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part(MultipartPart);

impl Part {
    pub fn text(name: &str, value: &str) -> Self {
        Self(MultipartPart {
            name: name.to_string(),
            body: value.as_bytes().to_vec(),
            ..Default::default()
        })
    }

    pub fn file(name: &str, filename: &str, content_type: &str, bytes: Vec<u8>) -> Self {
        Self(MultipartPart {
            name: name.to_string(),
            filename: Some(filename.to_string()),
            content_type: Some(content_type.to_string()),
            body: bytes,
        })
    }
}

/// The server's answer, whatever its status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    /// The first header of that name, whatever its case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.header_all(name).into_iter().next()
    }

    /// Every header of that name, in the order received.
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.body
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.body
    }

    pub fn text(&self) -> Result<&str, HttpError> {
        std::str::from_utf8(&self.body)
            .map_err(|e| HttpError::new(HttpErrorKind::Io, format!("the body is not UTF-8: {e}")))
    }

    pub fn json<T: DeserializeOwned>(&self) -> Result<T, HttpError> {
        serde_json::from_slice(&self.body).map_err(|e| {
            HttpError::new(
                HttpErrorKind::Io,
                format!("the body is not the expected JSON: {e}"),
            )
        })
    }

    /// A 2xx status.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Why no response came back. A response with a failing status is not
/// one: it is an [`HttpResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    pub kind: HttpErrorKind,
    pub message: String,
}

impl HttpError {
    fn new(kind: HttpErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// What every call returns where there is no platform to send it.
    #[cfg(not(target_arch = "wasm32"))]
    fn unavailable() -> Self {
        Self::new(
            HttpErrorKind::Connect,
            "outbound HTTP is only available inside the Cufflink platform",
        )
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for HttpError {}

impl From<HttpErrorEnvelope> for HttpError {
    fn from(envelope: HttpErrorEnvelope) -> Self {
        Self::new(envelope.kind, envelope.message)
    }
}

/// Send `request` and read the whole response as bytes.
///
/// ```rust,ignore
/// let email = json!({"from": from, "to": to, "html": html});
/// let request = HttpRequest::post("https://mail.example.com/messages")
///     .header("X-Api-Token", &token)
///     .json(&email)?;
/// let sent = http::fetch_bytes(&request)?;
/// ```
pub fn fetch_bytes(request: &HttpRequest) -> Result<HttpResponse, HttpError> {
    send(&request.0)
}

/// Send `parts` as the `multipart/form-data` body of `request`, which
/// gives the method, the URL and the headers and has no body of its own.
///
/// ```rust,ignore
/// let pdf = http::multipart(
///     &HttpRequest::post(&format!("{renderer}/convert/html")),
///     &[Part::file("files", "index.html", "text/html", html.into_bytes())],
/// )?
/// .into_bytes();
/// ```
pub fn multipart(request: &HttpRequest, parts: &[Part]) -> Result<HttpResponse, HttpError> {
    if request.0.body.is_some() {
        return Err(HttpError::new(
            HttpErrorKind::InvalidRequest,
            "a multipart request has no body of its own",
        ));
    }
    send(&FetchRequestV2 {
        multipart: Some(parts.iter().map(|part| part.0.clone()).collect()),
        ..request.0.clone()
    })
}

fn send(request: &FetchRequestV2) -> Result<HttpResponse, HttpError> {
    match exchange(request)? {
        FetchResponseV2::Ok {
            status,
            headers,
            body,
        } => Ok(HttpResponse {
            status,
            headers,
            body,
        }),
        FetchResponseV2::Error(envelope) => Err(envelope.into()),
    }
}

#[cfg(target_arch = "wasm32")]
fn exchange(request: &FetchRequestV2) -> Result<FetchResponseV2, HttpError> {
    use crate::envelope::{call, Failure};

    #[link(wasm_import_module = "env")]
    extern "C" {
        fn http_fetch_v2(ptr: i32, len: i32) -> i32;
    }
    call(http_fetch_v2, request).map_err(|failure| {
        let kind = match failure {
            Failure::Encode(_) => HttpErrorKind::InvalidRequest,
            Failure::Refused => HttpErrorKind::TooLarge,
            Failure::Malformed(_) => HttpErrorKind::Io,
        };
        HttpError::new(kind, failure.to_string())
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange(request: &FetchRequestV2) -> Result<FetchResponseV2, HttpError> {
    crate::host::http()
        .map(|host| host.fetch(request))
        .ok_or_else(HttpError::unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn a_request_is_the_envelope_the_platform_reads() {
        let request = HttpRequest::new("PUT", "https://a.example/v1?expand[]=a&expand[]=b")
            .header("accept", "application/pdf")
            .header("x-tag", "a")
            .header("x-tag", "b")
            .body(vec![0, 255])
            .timeout_ms(1500)
            .max_response_bytes(1024)
            .follow_redirects(true);
        assert_eq!(
            serde_json::to_value(&request.0).expect("json"),
            json!({
                "method": "PUT",
                "url": "https://a.example/v1?expand[]=a&expand[]=b",
                "headers": [["accept", "application/pdf"], ["x-tag", "a"], ["x-tag", "b"]],
                "body_b64": "AP8=",
                "timeout_ms": 1500,
                "max_response_bytes": 1024,
                "follow_redirects": true
            })
        );
        assert_eq!(HttpRequest::get("u").0.method, "GET");
        assert_eq!(
            serde_json::to_value(&HttpRequest::post("u").0).expect("json"),
            json!({"method": "POST", "url": "u"})
        );
    }

    #[test]
    fn a_json_body_brings_its_content_type() {
        let request = HttpRequest::post("https://mail.example.com/messages")
            .header("Content-Type", "text/plain")
            .header("X-Api-Token", "t")
            .json(&json!({"to": "a@b.c"}))
            .expect("json");
        assert_eq!(
            request.0.headers,
            [
                ("X-Api-Token".to_string(), "t".to_string()),
                ("content-type".to_string(), "application/json".to_string()),
            ]
        );
        assert_eq!(request.0.body.as_deref(), Some(&br#"{"to":"a@b.c"}"#[..]));
        let unencodable = std::collections::HashMap::from([((1, 2), 3)]);
        assert_eq!(
            HttpRequest::post("u")
                .json(&unencodable)
                .expect_err("keys")
                .kind,
            HttpErrorKind::InvalidRequest
        );
    }

    #[test]
    fn parts_are_text_or_files() {
        assert_eq!(
            serde_json::to_value(&Part::text("waitDelay", "1s").0).expect("json"),
            json!({"name": "waitDelay", "body_b64": "MXM="})
        );
        assert_eq!(
            serde_json::to_value(
                &Part::file("files", "index.html", "text/html", b"<p>".to_vec()).0
            )
            .expect("json"),
            json!({
                "name": "files",
                "filename": "index.html",
                "content_type": "text/html",
                "body_b64": "PHA+"
            })
        );
    }

    #[test]
    fn a_response_is_read_as_bytes_text_or_json() {
        let response = HttpResponse {
            status: 201,
            headers: vec![
                ("Set-Cookie".into(), "a=1".into()),
                ("content-type".into(), "application/json".into()),
                ("set-cookie".into(), "b=2".into()),
            ],
            body: br#"{"MessageID":"m-1"}"#.to_vec(),
        };
        assert!(response.is_success());
        assert_eq!(response.header("SET-COOKIE"), Some("a=1"));
        assert_eq!(response.header_all("set-cookie"), ["a=1", "b=2"]);
        assert_eq!(response.header("location"), None);
        assert_eq!(response.text().expect("text"), r#"{"MessageID":"m-1"}"#);
        assert_eq!(
            response.json::<Value>().expect("json"),
            json!({"MessageID": "m-1"})
        );
        assert_eq!(response.bytes(), response.clone().into_bytes());

        let binary = HttpResponse {
            status: 404,
            headers: Vec::new(),
            body: vec![0x25, 0x50, 0xff],
        };
        assert!(!binary.is_success());
        assert_eq!(binary.text().expect_err("bytes").kind, HttpErrorKind::Io);
        assert_eq!(
            binary.json::<Value>().expect_err("bytes").kind,
            HttpErrorKind::Io
        );
        assert_eq!(binary.into_bytes(), [0x25, 0x50, 0xff]);
    }

    #[test]
    fn a_multipart_request_has_no_body_of_its_own() {
        let error = multipart(
            &HttpRequest::post("https://a.example/").body(b"raw".to_vec()),
            &[Part::text("a", "b")],
        )
        .expect_err("refused");
        assert_eq!(error.kind, HttpErrorKind::InvalidRequest);
        assert_eq!(
            error.to_string(),
            "invalid_request: a multipart request has no body of its own"
        );
    }

    #[cfg(not(feature = "test-host"))]
    #[test]
    fn outside_the_platform_every_call_is_unavailable() {
        let request = HttpRequest::get("https://a.example/");
        for error in [
            fetch_bytes(&request).expect_err("no platform"),
            multipart(&request, &[]).expect_err("no platform"),
        ] {
            assert_eq!(error.kind, HttpErrorKind::Connect);
            assert!(error.message.contains("inside the Cufflink platform"));
        }
    }

    #[cfg(feature = "test-host")]
    mod hosted {
        use super::*;
        use crate::host::{self, HostSet};
        use crate::testing::{MockHttp, UrlMatch};
        use std::rc::Rc;

        fn hosted<T>(mock: &MockHttp, body: impl FnOnce() -> T) -> T {
            let hosts = HostSet {
                http: Some(Rc::new(mock.clone())),
                ..Default::default()
            };
            host::with(hosts, body)
        }

        #[test]
        fn the_renderer_call_sends_its_parts_and_reads_the_pdf() {
            let pdf = vec![0x25, 0x50, 0x44, 0x46, 0x00, 0xff];
            let url = "https://pdf.example.com/convert/html";
            let mock = MockHttp::new();
            mock.expect("POST", UrlMatch::exact(url))
                .respond(200, &[("content-type", "application/pdf")], pdf.clone())
                .times(1);
            let parts = [
                Part::text("printBackground", "true"),
                Part::file(
                    "files",
                    "index.html",
                    "text/html",
                    b"<h1>Receipt</h1>".to_vec(),
                ),
            ];
            let response =
                hosted(&mock, || multipart(&HttpRequest::post(url), &parts)).expect("response");
            assert_eq!(response.header("Content-Type"), Some("application/pdf"));
            assert_eq!(response.into_bytes(), pdf);
            assert_eq!(mock.verify(), Ok(()));
            let sent = mock.requests();
            assert_eq!(sent.len(), 1);
            assert_eq!(sent[0].body, None);
            assert_eq!(
                sent[0].multipart,
                Some(parts.iter().map(|part| part.0.clone()).collect())
            );
        }

        #[test]
        fn a_failure_keeps_its_kind_and_a_failing_status_is_a_response() {
            let mock = MockHttp::new();
            mock.expect("GET", UrlMatch::prefix("https://slow.example/"))
                .fail(HttpErrorKind::Timeout, "request timed out after 2s");
            mock.expect("GET", UrlMatch::exact("https://a.example/missing"))
                .respond(404, &[], "no");
            let (slow, missing, unscripted) = hosted(&mock, || {
                (
                    fetch_bytes(&HttpRequest::get("https://slow.example/x")),
                    fetch_bytes(&HttpRequest::get("https://a.example/missing")),
                    fetch_bytes(&HttpRequest::get("https://other.example/")),
                )
            });
            assert_eq!(
                slow,
                Err(HttpError::new(
                    HttpErrorKind::Timeout,
                    "request timed out after 2s"
                ))
            );
            assert_eq!(missing.expect("a response").status, 404);
            assert_eq!(unscripted.expect_err("denied").kind, HttpErrorKind::Denied);
            assert!(fetch_bytes(&HttpRequest::get("https://a.example/missing")).is_err());
        }
    }
}
