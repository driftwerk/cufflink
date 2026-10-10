//! First-class Stripe support for cufflink services.
//!
//! Enable with `cufflink-fn = { version = "...", features = ["stripe"] }`.
//!
//! Stripe integration is one of the most common things a cufflink service
//! needs, and every consumer otherwise re-derives the same three pieces of
//! plumbing: webhook signature verification, `x-www-form-urlencoded` request
//! encoding, and a REST client with auth + idempotency headers. This module
//! ships those once, built entirely on top of [`http`](crate::http) and
//! [`Request::raw_body`](crate::Request::raw_body) — no new host primitive.
//!
//! Store the secret key out-of-band via
//! `cufflink config set STRIPE_SECRET_KEY sk_live_... --secret`; whether you
//! talk to Stripe's live or test environment is decided by the key prefix
//! (`sk_live_` vs `sk_test_`), not by this module.
//!
//! # Receiving a webhook
//!
//! ```rust,ignore
//! handler!(stripe_webhook, |req: Request| {
//!     let secret = config::get("STRIPE_WEBHOOK_SECRET").unwrap_or_default();
//!     let event = match stripe::verify_webhook(&req, &secret) {
//!         Ok(ev) => ev,
//!         Err(reason) => return Response::error(reason).with_status(400),
//!     };
//!     match event["type"].as_str().unwrap_or("") {
//!         "transfer.reversed" => { /* ... */ }
//!         _ => {}
//!     }
//!     Response::json(&json!({ "received": true }))
//! });
//! ```
//!
//! # Calling the API
//!
//! ```rust,ignore
//! let client = stripe::Client::from_config()?;
//! let transfer = client.post_form(
//!     "/transfers",
//!     &[("amount", "4250"), ("currency", "usd"), ("destination", "acct_123")],
//!     Some("transfer-listing-789"), // idempotency key — safe to retry
//! )?;
//! ```

#[allow(unused_imports)]
use super::*;
use serde_json::Value;

use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Base URL for the Stripe REST API. Live vs test is selected by the secret
/// key prefix, not the URL, so this is constant.
const STRIPE_API_BASE: &str = "https://api.stripe.com/v1";

/// The Stripe API version every request is pinned to, sent as `Stripe-Version`.
///
/// Without this header Stripe serves each request at the *account's* default
/// version, so request/response shapes would be set in a dashboard rather than
/// in code — and would change under a running deployment the moment somebody
/// clicked "upgrade version", with no deploy and no signal.
///
/// This module and its callers are written against basil: `/invoices/create_preview`
/// (which replaced `/invoices/upcoming`), `discounts[]` (which replaced the
/// top-level `promotion_code` param), `confirmation_secret`, and
/// `current_period_end` on the subscription item. Changing this constant is a
/// breaking change to every caller — treat it as a migration, not a bump.
pub const STRIPE_API_VERSION: &str = "2025-03-31.basil";

/// Stripe's documented replay tolerance: the maximum allowed gap between the
/// signature timestamp and receive time. See
/// <https://stripe.com/docs/webhooks/signatures>.
pub const DEFAULT_REPLAY_TOLERANCE_SECONDS: i64 = 300;

/// A failed Stripe API call. `kind` is a coarse bucket (`"transport"`,
/// `"config"`, `"parse"`, or `"api"`); for `"api"` errors the `code` and
/// `status` carry Stripe's own error envelope so callers can branch on e.g.
/// `balance_insufficient` without string-matching the message.
#[derive(Debug, Clone)]
pub struct StripeError {
    pub kind: String,
    pub code: Option<String>,
    pub message: String,
    pub status: Option<i32>,
}

impl StripeError {
    fn new(kind: &str, message: impl Into<String>) -> Self {
        StripeError {
            kind: kind.to_string(),
            code: None,
            message: message.into(),
            status: None,
        }
    }
}

impl std::fmt::Display for StripeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.code, self.status) {
            (Some(code), Some(status)) => {
                write!(
                    f,
                    "stripe {} error [{} / {}]: {}",
                    self.kind, status, code, self.message
                )
            }
            (None, Some(status)) => {
                write!(
                    f,
                    "stripe {} error [{}]: {}",
                    self.kind, status, self.message
                )
            }
            _ => write!(f, "stripe {} error: {}", self.kind, self.message),
        }
    }
}

impl std::error::Error for StripeError {}

/// `application/x-www-form-urlencoded` encoding, the wire format every Stripe
/// REST endpoint expects for request bodies.
pub mod form {
    /// Percent-encode a single value per the form-encoding subset of RFC 3986:
    /// spaces become `+`, unreserved characters pass through, everything else
    /// is `%`-escaped.
    pub fn percent_encode(value: &str) -> String {
        let mut out = String::with_capacity(value.len());
        for byte in value.bytes() {
            match byte {
                b' ' => out.push('+'),
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char);
                }
                _ => {
                    out.push('%');
                    out.push_str(&format!("{:02X}", byte));
                }
            }
        }
        out
    }

    /// Build a urlencoded body from `(key, value)` pairs. Nested Stripe params
    /// are expressed by the caller in the key, e.g.
    /// `("metadata[listing_id]", "abc")`.
    pub fn build_body(pairs: &[(&str, &str)]) -> String {
        pairs
            .iter()
            .map(|(k, v)| format!("{}={}", percent_encode(k), percent_encode(v)))
            .collect::<Vec<_>>()
            .join("&")
    }
}

/// A parsed `Stripe-Signature` header. Only the fields cufflink verifies are
/// retained (`t` and the `v1` HMACs); legacy `v0` schemes are ignored.
pub struct WebhookSignature {
    pub timestamp: i64,
    /// Every `v1` signature on the header, in the order Stripe sent them.
    /// Stripe emits more than one while an endpoint secret is being rolled, so
    /// this is a list rather than a single value.
    pub v1: Vec<String>,
}

/// Parse a `Stripe-Signature` header of the form `t=<unix>,v1=<hex>[,v1=<hex>…]`.
/// Returns `None` if the timestamp is absent/malformed or no `v1` is present.
///
/// During a signing-secret roll Stripe signs the payload with both the old and
/// the new secret and sends **both** `v1` entries, so every one is retained and
/// [`verify_signature`] accepts a match against any. Keeping only one
/// would reject every webhook for the rotation window.
pub fn parse_signature_header(header: &str) -> Option<WebhookSignature> {
    let mut timestamp: Option<i64> = None;
    let mut v1: Vec<String> = Vec::new();
    for part in header.split(',') {
        let (key, value) = match part.split_once('=') {
            Some(kv) => kv,
            None => continue,
        };
        match key.trim() {
            "t" => timestamp = value.trim().parse::<i64>().ok(),
            "v1" => v1.push(value.trim().to_string()),
            _ => {}
        }
    }
    if v1.is_empty() {
        return None;
    }
    Some(WebhookSignature {
        timestamp: timestamp?,
        v1,
    })
}

/// Constant-time byte comparison — avoids leaking the correct signature
/// through early-exit timing.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn compute_hmac(secret: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

/// Verify a Stripe webhook signature over the exact raw request bytes.
///
/// The HMAC is computed over `"{timestamp}.{raw_body}"`, so `raw_body` MUST be
/// the bytes as Stripe sent them — use [`Request::raw_body`](crate::Request::raw_body),
/// never the re-serialized parsed body. `now_unix` is supplied by the caller so
/// this stays pure and unit-testable; [`verify_webhook`] fills it in for you.
pub fn verify_signature(
    raw_body: &[u8],
    signature_header: &str,
    secret: &str,
    now_unix: i64,
    tolerance_seconds: i64,
) -> Result<(), &'static str> {
    let parsed = parse_signature_header(signature_header).ok_or("malformed Stripe-Signature")?;
    if (now_unix - parsed.timestamp).abs() > tolerance_seconds {
        return Err("timestamp outside replay tolerance");
    }
    let mut signed_payload = parsed.timestamp.to_string().into_bytes();
    signed_payload.push(b'.');
    signed_payload.extend_from_slice(raw_body);
    let computed = compute_hmac(secret.as_bytes(), &signed_payload);
    let expected_hex = hex::encode(&computed);
    let matched = parsed
        .v1
        .iter()
        .any(|candidate| constant_time_eq(candidate.as_bytes(), expected_hex.as_bytes()));
    if !matched {
        return Err("signature mismatch");
    }
    Ok(())
}

/// Header names are looked up case-sensitively by [`Request::header`], and
/// gateways disagree on casing, so check both forms Stripe and proxies emit.
fn signature_header(req: &Request) -> Option<&str> {
    req.header("stripe-signature")
        .or_else(|| req.header("Stripe-Signature"))
}

/// Current host time as a unix timestamp, derived from the RFC3339 string the
/// platform clock returns. Returns 0 outside WASM (the native fallback clock
/// is not RFC3339) — native tests should exercise [`verify_signature`] with an
/// explicit `now_unix` instead.
fn now_unix() -> i64 {
    let raw = util::current_time();
    match raw.parse::<chrono::DateTime<chrono::Utc>>() {
        Ok(dt) => dt.timestamp(),
        Err(_) => {
            log::error(&format!(
                "[stripe] host clock returned an unparseable time ({:?}); every webhook will be \
                 rejected as outside the replay window until this is fixed",
                raw
            ));
            0
        }
    }
}

/// Verify an incoming webhook on a [`Request`](crate::Request) and return the
/// parsed event JSON. Pulls the signature header and raw body off the request,
/// checks the HMAC against `secret` within the
/// [`DEFAULT_REPLAY_TOLERANCE_SECONDS`] window, and parses the body.
///
/// On the happy path the returned `Value` is the Stripe event; branch on
/// `event["type"]`. Returns a static reason string suitable for a `400`
/// response on any verification failure.
pub fn verify_webhook(req: &Request, secret: &str) -> Result<Value, &'static str> {
    if secret.is_empty() {
        return Err("webhook secret not configured");
    }
    let header = signature_header(req).ok_or("missing Stripe-Signature header")?;
    verify_signature(
        req.raw_body(),
        header,
        secret,
        now_unix(),
        DEFAULT_REPLAY_TOLERANCE_SECONDS,
    )?;
    serde_json::from_slice(req.raw_body()).map_err(|_| "webhook body is not valid JSON")
}

/// A thin Stripe REST client: it owns the secret key and attaches the
/// `Authorization`, `Content-Type`, and (optionally) `Idempotency-Key` headers
/// so callers express only the endpoint and form params. All money-moving
/// requests should pass an idempotency key — NATS and webhooks are
/// at-least-once, and a retried `Transfer` without one double-pays.
#[derive(Clone)]
pub struct Client {
    secret_key: String,
}

impl Client {
    /// Construct a client from an explicit secret key.
    pub fn new(secret_key: impl Into<String>) -> Self {
        Client {
            secret_key: secret_key.into(),
        }
    }

    /// Construct a client from the `STRIPE_SECRET_KEY` config value. Returns a
    /// `"config"` error if it is unset or empty.
    pub fn from_config() -> Result<Self, StripeError> {
        Self::from_config_key("STRIPE_SECRET_KEY")
    }

    /// Like [`from_config`](Self::from_config) but reads an arbitrary config
    /// key — for services juggling more than one Stripe account.
    pub fn from_config_key(config_key: &str) -> Result<Self, StripeError> {
        match config::get(config_key).filter(|s| !s.is_empty()) {
            Some(secret) => Ok(Client::new(secret)),
            None => Err(StripeError::new(
                "config",
                format!("{} not configured", config_key),
            )),
        }
    }

    fn auth_header(&self) -> String {
        format!("Bearer {}", self.secret_key)
    }

    /// POST form-encoded params to a Stripe endpoint (e.g. `"/transfers"`).
    /// Pass `idempotency_key` for any non-idempotent mutation so retries are
    /// safe. Returns the parsed response object, or a [`StripeError`] carrying
    /// Stripe's error envelope on a non-2xx response.
    pub fn post_form(
        &self,
        path: &str,
        params: &[(&str, &str)],
        idempotency_key: Option<&str>,
    ) -> Result<Value, StripeError> {
        let url = format!("{}{}", STRIPE_API_BASE, path);
        let auth = self.auth_header();
        let mut headers: Vec<(&str, &str)> = vec![
            ("Authorization", auth.as_str()),
            ("Content-Type", "application/x-www-form-urlencoded"),
            ("Stripe-Version", STRIPE_API_VERSION),
        ];
        if let Some(key) = idempotency_key {
            headers.push(("Idempotency-Key", key));
        }
        let body = form::build_body(params);
        let resp = http::post(&url, &headers, &body)
            .ok_or_else(|| StripeError::new("transport", "failed to reach Stripe API"))?;
        Self::interpret(resp)
    }

    /// GET a Stripe resource (e.g. `"/transfers/tr_123"`).
    pub fn get(&self, path: &str) -> Result<Value, StripeError> {
        let url = format!("{}{}", STRIPE_API_BASE, path);
        let auth = self.auth_header();
        let headers: Vec<(&str, &str)> = vec![
            ("Authorization", auth.as_str()),
            ("Stripe-Version", STRIPE_API_VERSION),
        ];
        let resp = http::get(&url, &headers)
            .ok_or_else(|| StripeError::new("transport", "failed to reach Stripe API"))?;
        Self::interpret(resp)
    }

    /// Map a raw [`http::FetchResponse`] into the parsed object or a typed
    /// error. A 2xx body that fails to parse is a `"parse"` error; a non-2xx
    /// body is unpacked into Stripe's `{ "error": { type, code, message } }`
    /// envelope where present.
    fn interpret(resp: http::FetchResponse) -> Result<Value, StripeError> {
        let parsed: Option<Value> = serde_json::from_str(&resp.body).ok();
        if resp.is_success() {
            return parsed.ok_or_else(|| StripeError::new("parse", "Stripe response was not JSON"));
        }
        let err = parsed.as_ref().map(|v| &v["error"]);
        let message = err
            .and_then(|e| e["message"].as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("Stripe returned status {}", resp.status));
        let code = err
            .and_then(|e| e["code"].as_str().or_else(|| e["type"].as_str()))
            .map(|s| s.to_string());
        Err(StripeError {
            kind: "api".to_string(),
            code,
            message,
            status: Some(resp.status),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encode_keeps_unreserved() {
        assert_eq!(form::percent_encode("Abc-_.~123"), "Abc-_.~123");
    }

    #[test]
    fn percent_encode_replaces_spaces_with_plus() {
        assert_eq!(form::percent_encode("hello world"), "hello+world");
    }

    #[test]
    fn percent_encode_escapes_reserved() {
        assert_eq!(form::percent_encode("a/b"), "a%2Fb");
        assert_eq!(form::percent_encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(form::percent_encode("a@b.com"), "a%40b.com");
    }

    #[test]
    fn percent_encode_handles_utf8() {
        assert_eq!(form::percent_encode("£"), "%C2%A3");
    }

    #[test]
    fn build_body_empty() {
        assert_eq!(form::build_body(&[]), "");
    }

    #[test]
    fn build_body_joins_and_encodes_each_pair() {
        let body = form::build_body(&[("email", "alice@example.com"), ("note", "hi & bye")]);
        assert_eq!(body, "email=alice%40example.com&note=hi+%26+bye");
    }

    #[test]
    fn build_body_supports_nested_keys() {
        let body = form::build_body(&[("metadata[listing_id]", "abc-1")]);
        assert_eq!(body, "metadata%5Blisting_id%5D=abc-1");
    }

    #[test]
    fn parse_signature_handles_canonical_header() {
        let parsed = parse_signature_header("t=1234567890,v1=abcdef").unwrap();
        assert_eq!(parsed.timestamp, 1234567890);
        assert_eq!(parsed.v1, vec!["abcdef".to_string()]);
    }

    #[test]
    fn parse_signature_ignores_unknown_schemes() {
        let parsed = parse_signature_header("t=1,v0=legacy,v1=current,scheme=foo").unwrap();
        assert_eq!(parsed.timestamp, 1);
        assert_eq!(parsed.v1, vec!["current".to_string()]);
    }

    #[test]
    fn parse_signature_keeps_every_v1_during_a_secret_roll() {
        let parsed = parse_signature_header("t=1,v1=old_sig,v1=new_sig").unwrap();
        assert_eq!(
            parsed.v1,
            vec!["old_sig".to_string(), "new_sig".to_string()]
        );
    }

    #[test]
    fn parse_signature_rejects_missing_components() {
        assert!(parse_signature_header("v1=abc").is_none());
        assert!(parse_signature_header("t=1").is_none());
        assert!(parse_signature_header("not-a-signature").is_none());
    }

    #[test]
    fn constant_time_eq_behaves() {
        assert!(constant_time_eq(b"hello", b"hello"));
        assert!(!constant_time_eq(b"hello", b"hell"));
        assert!(!constant_time_eq(b"hello", b"world"));
    }

    fn known_good_signature(timestamp: i64, body: &[u8], secret: &[u8]) -> String {
        let mut signed = timestamp.to_string().into_bytes();
        signed.push(b'.');
        signed.extend_from_slice(body);
        let mac = compute_hmac(secret, &signed);
        format!("t={},v1={}", timestamp, hex::encode(&mac))
    }

    #[test]
    fn verify_signature_accepts_known_good() {
        let body = br#"{"id":"evt_123","type":"transfer.reversed"}"#;
        let header = known_good_signature(1_700_000_000, body, b"whsec_test");
        assert!(verify_signature(body, &header, "whsec_test", 1_700_000_010, 300).is_ok());
    }

    #[test]
    fn verify_signature_rejects_tampered_body() {
        let body = br#"{"id":"evt_123"}"#;
        let header = known_good_signature(1_700_000_000, body, b"whsec_test");
        let tampered = br#"{"id":"evt_999"}"#;
        assert_eq!(
            verify_signature(tampered, &header, "whsec_test", 1_700_000_010, 300),
            Err("signature mismatch")
        );
    }

    #[test]
    fn verify_signature_rejects_wrong_secret() {
        let body = br#"{"id":"evt_123"}"#;
        let header = known_good_signature(1_700_000_000, body, b"whsec_a");
        assert_eq!(
            verify_signature(body, &header, "whsec_b", 1_700_000_010, 300),
            Err("signature mismatch")
        );
    }

    #[test]
    fn verify_signature_enforces_replay_window_both_directions() {
        let body = br#"{"id":"evt_123"}"#;
        let stale = known_good_signature(1_700_000_000, body, b"whsec_test");
        assert_eq!(
            verify_signature(body, &stale, "whsec_test", 1_700_000_000 + 600, 300),
            Err("timestamp outside replay tolerance")
        );
        let future = known_good_signature(1_700_000_600, body, b"whsec_test");
        assert_eq!(
            verify_signature(body, &future, "whsec_test", 1_700_000_000, 300),
            Err("timestamp outside replay tolerance")
        );
    }

    #[test]
    fn verify_signature_rejects_malformed_header() {
        assert_eq!(
            verify_signature(b"{}", "garbage", "whsec", 0, 300),
            Err("malformed Stripe-Signature")
        );
    }

    /// A header as Stripe sends it mid-roll: `t` once, then one `v1` per secret
    /// still active on the endpoint.
    fn rolling_signature(timestamp: i64, body: &[u8], secrets: &[&[u8]]) -> String {
        let sigs = secrets
            .iter()
            .map(|secret| {
                let mut signed = timestamp.to_string().into_bytes();
                signed.push(b'.');
                signed.extend_from_slice(body);
                format!("v1={}", hex::encode(compute_hmac(secret, &signed)))
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("t={},{}", timestamp, sigs)
    }

    #[test]
    fn verify_signature_accepts_either_secret_mid_rotation() {
        let body = br#"{"id":"evt_123"}"#;
        let header = rolling_signature(1_700_000_000, body, &[b"whsec_old", b"whsec_new"]);
        for secret in ["whsec_old", "whsec_new"] {
            assert!(
                verify_signature(body, &header, secret, 1_700_000_010, 300).is_ok(),
                "{} should verify against a dual-signed header",
                secret
            );
        }
    }

    #[test]
    fn verify_signature_still_rejects_a_secret_on_neither_side_of_a_roll() {
        let body = br#"{"id":"evt_123"}"#;
        let header = rolling_signature(1_700_000_000, body, &[b"whsec_old", b"whsec_new"]);
        assert_eq!(
            verify_signature(body, &header, "whsec_attacker", 1_700_000_010, 300),
            Err("signature mismatch")
        );
    }

    #[test]
    fn interpret_unpacks_stripe_error_envelope() {
        let resp = http::FetchResponse {
            status: 402,
            body: r#"{"error":{"type":"card_error","code":"balance_insufficient","message":"Insufficient funds"}}"#.to_string(),
            body_encoding: "utf8".to_string(),
            headers: std::collections::HashMap::new(),
        };
        let err = Client::interpret(resp).unwrap_err();
        assert_eq!(err.kind, "api");
        assert_eq!(err.status, Some(402));
        assert_eq!(err.code.as_deref(), Some("balance_insufficient"));
        assert_eq!(err.message, "Insufficient funds");
    }

    #[test]
    fn interpret_returns_object_on_success() {
        let resp = http::FetchResponse {
            status: 200,
            body: r#"{"id":"tr_123","object":"transfer"}"#.to_string(),
            body_encoding: "utf8".to_string(),
            headers: std::collections::HashMap::new(),
        };
        let ok = Client::interpret(resp).unwrap();
        assert_eq!(ok["id"], "tr_123");
    }

    #[test]
    fn interpret_flags_unparseable_success_body() {
        let resp = http::FetchResponse {
            status: 200,
            body: "not json".to_string(),
            body_encoding: "utf8".to_string(),
            headers: std::collections::HashMap::new(),
        };
        let err = Client::interpret(resp).unwrap_err();
        assert_eq!(err.kind, "parse");
    }
}
