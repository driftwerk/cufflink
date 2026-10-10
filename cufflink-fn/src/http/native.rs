//! The legacy functions in a native build: through the installed
//! [`HttpHost`](crate::host::http::HttpHost) as the platform answers them,
//! and nothing without one.

use super::FetchResponse;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use cufflink_types::host::http::{FetchRequestV2, FetchResponseV2};
use std::collections::BTreeMap;

const METHODS: [&str; 5] = ["GET", "POST", "PUT", "DELETE", "PATCH"];

/// `None` without a host. Redirects are followed and a header given twice
/// is sent once, as the legacy imports do.
pub(super) fn fetch(
    method: &str,
    url: &str,
    headers: &[(&str, &str)],
    body: Option<&str>,
    timeout_ms: Option<u64>,
) -> Option<Result<FetchResponse, String>> {
    let host = crate::host::http()?;
    if !METHODS.contains(&method.to_uppercase().as_str()) {
        return Some(Err(format!("Unsupported HTTP method: {method}")));
    }
    let headers: BTreeMap<&str, &str> = headers.iter().copied().collect();
    let request = FetchRequestV2 {
        method: method.to_string(),
        url: url.to_string(),
        headers: headers
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        body: body.map(|body| body.as_bytes().to_vec()),
        timeout_ms: timeout_ms.map(|ms| u32::try_from(ms).unwrap_or(u32::MAX)),
        follow_redirects: true,
        ..Default::default()
    };
    Some(match host.fetch(&request) {
        FetchResponseV2::Ok {
            status,
            headers,
            body,
        } => Ok(legacy(status, headers, &body)),
        FetchResponseV2::Error(e) => Err(e.message),
    })
}

/// The response as the legacy imports render it: a body of a binary
/// content type as base64, any other as text.
fn legacy(status: u16, headers: Vec<(String, String)>, body: &[u8]) -> FetchResponse {
    let headers: std::collections::HashMap<String, String> = headers
        .into_iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();
    let content_type = headers.get("content-type").map_or("", String::as_str);
    let binary = ["image/", "audio/", "video/"]
        .iter()
        .any(|family| content_type.starts_with(family))
        || ["application/octet-stream", "application/pdf"].contains(&content_type);
    let (body, body_encoding) = if binary {
        (STANDARD.encode(body), "base64")
    } else {
        (String::from_utf8_lossy(body).into_owned(), "utf8")
    };
    FetchResponse {
        status: i32::from(status),
        body,
        body_encoding: body_encoding.to_string(),
        headers,
    }
}

#[cfg(all(test, feature = "test-host"))]
mod tests {
    use crate::host::{self, HostSet};
    use crate::http;
    use crate::testing::{MockHttp, UrlMatch};
    use cufflink_types::host::http::HttpErrorKind;
    use std::rc::Rc;

    fn hosted<T>(mock: &MockHttp, body: impl FnOnce() -> T) -> T {
        let hosts = HostSet {
            http: Some(Rc::new(mock.clone())),
            ..Default::default()
        };
        host::with(hosts, body)
    }

    #[test]
    fn the_legacy_functions_reach_the_host_as_the_platform_answers_them() {
        let url = "https://api.stripe.com/v1/promotion_codes?expand[]=a&expand[]=b";
        let mock = MockHttp::new();
        mock.expect("GET", UrlMatch::exact(url)).respond(
            200,
            &[("Content-Type", "application/json")],
            r#"{"data":[]}"#,
        );
        mock.expect("POST", UrlMatch::exact("https://a.example/pdf"))
            .respond(
                200,
                &[("content-type", "application/pdf")],
                vec![0x25, 0xff],
            );
        mock.expect("GET", UrlMatch::exact("https://a.example/down"))
            .fail(HttpErrorKind::Connect, "refused");
        let (listed, pdf, down, traced) = hosted(&mock, || {
            (
                http::get(url, &[("authorization", "a"), ("authorization", "b")]),
                http::post_with_timeout("https://a.example/pdf", &[], "{}", 120_000),
                http::get("https://a.example/down", &[]),
                http::fetch("TRACE", "https://a.example/", &[], None),
            )
        });
        let listed = listed.expect("listed");
        assert_eq!((listed.status, listed.is_base64()), (200, false));
        assert_eq!(listed.json(), Some(serde_json::json!({"data": []})));
        assert_eq!(listed.headers["content-type"], "application/json");
        let pdf = pdf.expect("pdf");
        assert_eq!((pdf.body.as_str(), pdf.is_base64()), ("Jf8=", true));
        assert!(down.is_none() && traced.is_none());

        let sent = mock.requests();
        assert_eq!(sent.len(), 3, "an unsupported method is not sent");
        assert_eq!(sent[0].url, url);
        assert_eq!(sent[0].headers, [("authorization".into(), "b".into())]);
        assert!(sent.iter().all(|request| request.follow_redirects));
        assert_eq!(
            (sent[0].timeout_ms, sent[1].timeout_ms),
            (None, Some(120_000))
        );
        assert_eq!(sent[1].body.as_deref(), Some(&b"{}"[..]));
    }

    #[test]
    fn a_batch_answers_slot_by_slot() {
        let mock = MockHttp::new();
        mock.expect("GET", UrlMatch::exact("https://a.example/ok"))
            .respond(204, &[], "");
        let request = |url| http::FetchRequest {
            method: "GET",
            url,
            headers: &[],
            body: None,
        };
        let batch = [
            request("https://a.example/ok"),
            request("https://a.example/unscripted"),
        ];
        let results = hosted(&mock, || http::fetch_many(&batch));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().map(|r| r.status), Ok(204));
        assert_eq!(
            results[1].as_ref().map(|r| r.status),
            Err(&"no mock matched GET https://a.example/unscripted".to_string())
        );
        assert!(http::fetch_many(&batch).is_empty());
    }
}
