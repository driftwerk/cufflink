use super::{block_on_within, clamp_to_budget, read_guest_bytes, wrap, HostIoError, HostState};
use crate::app::routes::SvcService;
use crate::auth::service_identity::{self, Claims, MAX_CALL_DEPTH};
use crate::egress::{EgressError, EgressGuard, EgressTarget, OutboundRequest};
use crate::policy::{Denied, PolicyKey};
use crate::router::HOP_BY_HOP_HEADERS;
use crate::telemetry::{record_response, request_span, resolving};
use axum::body::Body;
use axum::extract::{ConnectInfo, DefaultBodyLimit};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, Request};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::cell::OnceCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tower::{Layer, ServiceExt};
use tracing::Instrument;
use url::Url;
use uuid::Uuid;
use wasmtime::Linker;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

const DECODE_ROUNDS: usize = 3;

const RESERVED_HEADERS: [&str; 13] = [
    "authorization",
    "cf-connecting-ip",
    "content-length",
    "cookie",
    "forwarded",
    "host",
    "proxy-authorization",
    "traceparent",
    "tracestate",
    "x-api-key",
    "x-forwarded-for",
    "x-real-ip",
    service_identity::HEADER,
];

fn reserved(name: &str) -> bool {
    HOP_BY_HOP_HEADERS.contains(&name) || RESERVED_HEADERS.contains(&name)
}

#[derive(Default)]
pub struct LocalDispatch(OnceLock<SvcService>);

impl LocalDispatch {
    pub(crate) fn install(&self, service: SvcService) -> eyre::Result<()> {
        self.0
            .set(SvcService::new(DefaultBodyLimit::disable().layer(service)))
            .map_err(|_| eyre::eyre!("the in-process /svc service is already installed"))
    }

    fn get(&self) -> Option<SvcService> {
        self.0.get().cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallRequest {
    service: String,
    method: String,
    path: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body_b64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CallFailure {
    Timeout,
    DepthExceeded,
    UnknownService(String),
    Transport(String),
    Denied(String),
}

impl CallFailure {
    fn kind(&self) -> &'static str {
        match self {
            CallFailure::Timeout => "timeout",
            CallFailure::DepthExceeded => "depth_exceeded",
            CallFailure::UnknownService(_) => "unknown_service",
            CallFailure::Transport(_) => "transport",
            CallFailure::Denied(_) => "denied",
        }
    }

    fn message(&self) -> String {
        match self {
            CallFailure::Timeout => "the service call timed out".into(),
            CallFailure::DepthExceeded => {
                format!("service calls nest at most {MAX_CALL_DEPTH} deep")
            }
            CallFailure::UnknownService(service) => {
                format!("no service {service:?} in this tenant")
            }
            CallFailure::Transport(message) | CallFailure::Denied(message) => message.clone(),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            CallFailure::DepthExceeded => "depth",
            other => other.kind(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CallResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    service: String,
    method: Method,
    path_and_query: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

fn confined(path_and_query: &str) -> bool {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _)| path);
    !path.contains("//") && path.split('/').skip(1).all(plain_segment)
}

fn plain_segment(segment: &str) -> bool {
    std::iter::successors(Some(Cow::Borrowed(segment)), |current| {
        let decoded = percent_decode_str(current).decode_utf8_lossy();
        (decoded != *current).then(|| Cow::Owned(decoded.into_owned()))
    })
    .take(DECODE_ROUNDS + 1)
    .all(|decoded| {
        !decoded.contains('\\')
            && decoded
                .split('/')
                .all(|piece| piece != "." && piece != "..")
    })
}

fn target(request: CallRequest) -> Result<Target, CallFailure> {
    if !cufflink_types::slug::is_valid_service_name(&request.service) {
        return Err(CallFailure::UnknownService(request.service));
    }
    let method = match request.method.to_ascii_uppercase().as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "PATCH" => Method::PATCH,
        "DELETE" => Method::DELETE,
        other => {
            return Err(CallFailure::Denied(format!(
                "method {other:?} is not allowed"
            )))
        }
    };
    let valid_path = request.path.starts_with('/')
        && request
            .path
            .bytes()
            .all(|b| b.is_ascii_graphic() && b != b'#');
    if !valid_path {
        return Err(CallFailure::Denied(format!(
            "path {:?} must be absolute, printable and without a fragment",
            request.path
        )));
    }
    if !confined(&request.path) {
        return Err(CallFailure::Denied(format!(
            "path {:?} must not contain empty or dot segments or backslashes",
            request.path
        )));
    }
    let headers = request
        .headers
        .iter()
        .filter(|(name, _)| !reserved(&name.to_ascii_lowercase()))
        .map(|(name, value)| {
            HeaderName::from_bytes(name.to_ascii_lowercase().as_bytes())
                .ok()
                .zip(HeaderValue::from_str(value).ok())
                .ok_or_else(|| CallFailure::Denied(format!("header {name:?} is not valid")))
        })
        .collect::<Result<HeaderMap, _>>()?;
    let body = request
        .body_b64
        .map(|b64| STANDARD.decode(b64))
        .transpose()
        .map_err(|e| CallFailure::Denied(format!("body_b64 is not base64: {e}")))?
        .unwrap_or_default();
    Ok(Target {
        service: request.service,
        method,
        path_and_query: request.path,
        headers,
        body,
    })
}

fn next_depth(current: u8) -> Result<u8, CallFailure> {
    current
        .checked_add(1)
        .filter(|depth| *depth <= MAX_CALL_DEPTH)
        .ok_or(CallFailure::DepthExceeded)
}

pub(crate) fn inbound_depth(request: &Value) -> u8 {
    request["caller"]["depth"]
        .as_u64()
        .map_or(0, |depth| u8::try_from(depth).unwrap_or(u8::MAX))
}

struct CallerIdentity<'a> {
    tenant_id: Uuid,
    tenant_slug: &'a str,
    service: &'a str,
    service_id: Uuid,
    version: i32,
    depth: u8,
    on_behalf_of: Option<String>,
}

fn claims(
    caller: &CallerIdentity<'_>,
    target: &Target,
    path: &str,
    remaining: Duration,
    now: i64,
) -> Result<Claims, CallFailure> {
    let lifetime = i64::try_from(remaining.as_secs())
        .unwrap_or(i64::MAX)
        .min(service_identity::MAX_LIFETIME_SECS);
    if lifetime < 1 {
        return Err(CallFailure::Timeout);
    }
    Ok(Claims {
        tenant_id: caller.tenant_id,
        caller_service: caller.service.to_string(),
        caller_service_id: caller.service_id,
        caller_version: caller.version,
        target_service: target.service.clone(),
        method: target.method.to_string(),
        path: path.to_string(),
        issued_at: now,
        expires_at: now + lifetime,
        jti: Uuid::new_v4(),
        depth: next_depth(caller.depth)?,
        on_behalf_of: caller.on_behalf_of.clone(),
    })
}

fn svc_prefix(tenant_slug: &str, service: &str) -> String {
    format!("/svc/{tenant_slug}/{service}/")
}

fn svc_uri(tenant_slug: &str, target: &Target) -> String {
    format!(
        "/svc/{tenant_slug}/{}{}",
        target.service, target.path_and_query
    )
}

fn http_scope(base: &str, tenant_slug: &str, service: &str) -> Result<Url, CallFailure> {
    Url::parse(&format!("{base}{}", svc_prefix(tenant_slug, service)))
        .map_err(|e| CallFailure::Transport(format!("PLATFORM_INTERNAL_URL: {e}")))
}

fn within(scope: &Url, url: &Url) -> bool {
    url.origin() == scope.origin() && url.path().starts_with(scope.path())
}

fn service_path<'a>(sent: &'a str, prefix: &str) -> Result<&'a str, CallFailure> {
    prefix
        .strip_suffix('/')
        .and_then(|service| sent.strip_prefix(service))
        .filter(|path| path.starts_with('/'))
        .ok_or_else(|| CallFailure::Denied(format!("path {sent:?} leaves {prefix}")))
}

fn refuse_event_stream(headers: &HeaderMap) -> Result<(), CallFailure> {
    let streaming = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim_start().starts_with("text/event-stream"));
    if streaming {
        Err(CallFailure::Denied(
            "event streams cannot be read through a service call".into(),
        ))
    } else {
        Ok(())
    }
}

fn with_identity(
    mut headers: HeaderMap,
    token: &str,
    has_body: bool,
) -> Result<HeaderMap, CallFailure> {
    let token = HeaderValue::from_str(token)
        .map_err(|e| CallFailure::Transport(format!("identity token: {e}")))?;
    headers.insert(service_identity::HEADER, token);
    let mut trace = HashMap::new();
    crate::telemetry::inject_current_context(&mut trace);
    headers.extend(trace.iter().filter_map(|(name, value)| {
        Some((
            HeaderName::from_bytes(name.as_bytes()).ok()?,
            HeaderValue::from_str(value).ok()?,
        ))
    }));
    if has_body && !headers.contains_key(axum::http::header::CONTENT_TYPE) {
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    Ok(headers)
}

fn response_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

async fn send_local(
    service: SvcService,
    uri: axum::http::Uri,
    scope: &str,
    target: Target,
    headers: HeaderMap,
    max_bytes: usize,
) -> Result<CallResponse, CallFailure> {
    let mut request = Request::builder()
        .method(target.method)
        .uri(uri)
        .body(Body::from(target.body))
        .map_err(|e| CallFailure::Denied(format!("request: {e}")))?;
    if !request.uri().path().starts_with(scope) {
        return Err(CallFailure::Denied(format!(
            "path {:?} leaves {scope}",
            request.uri().path()
        )));
    }
    *request.headers_mut() = headers;
    request
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
    let span = request_span(request.method(), request.uri().path(), request.headers());
    let (response, _) = resolving(service.oneshot(request))
        .instrument(span.clone())
        .await;
    let response = match response {
        Ok(response) => response,
        Err(infallible) => match infallible {},
    };
    record_response(&span, response.status());
    refuse_event_stream(response.headers())?;
    let status = response.status().as_u16();
    let headers = response_headers(response.headers());
    let body = axum::body::to_bytes(response.into_body(), max_bytes)
        .await
        .map_err(|e| CallFailure::Transport(format!("response body: {e}")))?;
    Ok(CallResponse {
        status,
        headers,
        body: body.to_vec(),
    })
}

async fn send_http(
    egress: &EgressGuard,
    url: String,
    scope: &Url,
    target: Target,
    headers: HeaderMap,
    timeout: Duration,
    max_bytes: u64,
) -> Result<CallResponse, CallFailure> {
    let has_body = !target.body.is_empty();
    let in_scope = |to: &EgressTarget| {
        within(scope, &to.url).then_some(()).ok_or(Denied {
            key: PolicyKey::Egress,
            code: "service_call_scope",
        })
    };
    let response = egress
        .send(
            OutboundRequest {
                method: target.method,
                url,
                headers,
                body: has_body.then_some(target.body),
                timeout,
            },
            &in_scope,
            false,
        )
        .await
        .map_err(egress_failure)?;
    refuse_event_stream(response.headers())?;
    let status = response.status().as_u16();
    let headers = response_headers(response.headers());
    let body = EgressGuard::read_body(response, max_bytes)
        .await
        .map_err(egress_failure)?;
    Ok(CallResponse {
        status,
        headers,
        body,
    })
}

fn egress_failure(error: EgressError) -> CallFailure {
    match error {
        EgressError::TimedOut(_) => CallFailure::Timeout,
        EgressError::Policy(denied) => CallFailure::Denied(format!(
            "the call leaves its target service ({})",
            denied.code
        )),
        other => CallFailure::Transport(other.to_string()),
    }
}

async fn call(
    state: &HostState,
    request: CallRequest,
    timeout: Duration,
    resolved: &OnceCell<String>,
) -> Result<CallResponse, CallFailure> {
    let target = target(request)?;
    let app = &state.app_state;
    let local = app.svc_service.get();
    let known = match &local {
        Some(_) => app
            .registry
            .get_by_slug(&state.identity.tenant_slug, &target.service)
            .await
            .is_some(),
        None => runs_in_tenant(&app.platform_db, state.identity.tenant_id, &target.service)
            .await
            .map_err(|e| CallFailure::Transport(format!("service lookup failed: {e}")))?,
    };
    if !known {
        return Err(CallFailure::UnknownService(target.service));
    }
    let _ = resolved.set(target.service.clone());
    let caller = CallerIdentity {
        tenant_id: state.identity.tenant_id,
        tenant_slug: &state.identity.tenant_slug,
        service: &state.identity.service_name,
        service_id: state.identity.service_id,
        version: state.service_version,
        depth: state.call_depth,
        on_behalf_of: state.auth.as_ref().map(|auth| auth.subject.clone()),
    };
    let remaining = state.remaining_budget();
    let identify = |path: &str| {
        let now = chrono::Utc::now().timestamp();
        let token = service_identity::mint(
            &app.service_identity,
            &claims(&caller, &target, path, remaining, now)?,
        )
        .map_err(|e| CallFailure::Transport(format!("identity: {e}")))?;
        with_identity(target.headers.clone(), &token, !target.body.is_empty())
    };
    let uri = svc_uri(caller.tenant_slug, &target);
    let prefix = svc_prefix(caller.tenant_slug, &target.service);
    let max_bytes = app.egress.effective_max_bytes(None);
    match (local, app.config.platform_internal_url.as_deref()) {
        (Some(service), _) => {
            let sent = uri
                .parse::<axum::http::Uri>()
                .map_err(|e| CallFailure::Denied(format!("request: {e}")))?;
            let headers = identify(service_path(sent.path(), &prefix)?)?;
            send_local(
                service,
                sent,
                &prefix,
                target,
                headers,
                usize::try_from(max_bytes).unwrap_or(usize::MAX),
            )
            .await
        }
        (None, Some(base)) => {
            let base = base.trim_end_matches('/');
            let scope = http_scope(base, caller.tenant_slug, &target.service)?;
            let url = Url::parse(&format!("{base}{uri}"))
                .map_err(|e| CallFailure::Denied(format!("request: {e}")))?;
            let headers = identify(service_path(url.path(), scope.path())?)?;
            send_http(
                &app.egress,
                url.into(),
                &scope,
                target,
                headers,
                timeout,
                max_bytes,
            )
            .await
        }
        (None, None) => Err(CallFailure::Transport(
            "service calls from the worker need PLATFORM_INTERNAL_URL".into(),
        )),
    }
}

async fn runs_in_tenant(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    service: &str,
) -> crate::errors::PlatformResult<bool> {
    match crate::repo::services::find(pool, tenant_id, service).await? {
        Some(service) => Ok(crate::repo::deployments::current(pool, service.id)
            .await?
            .is_some()),
        None => Ok(false),
    }
}

fn envelope(outcome: &Result<CallResponse, CallFailure>) -> Value {
    match outcome {
        Ok(response) => json!({
            "ok": true,
            "status": response.status,
            "headers": response.headers,
            "body_b64": STANDARD.encode(&response.body),
        }),
        Err(failure) => json!({
            "ok": false,
            "error": { "kind": failure.kind(), "message": failure.message() },
        }),
    }
}

fn result_label(outcome: &Result<CallResponse, CallFailure>) -> &'static str {
    match outcome {
        Ok(response) if response.status >= 500 => "http_5xx",
        Ok(response) if response.status >= 400 => "http_4xx",
        Ok(_) => "ok",
        Err(failure) => failure.label(),
    }
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "service_call",
        |mut caller: wasmtime::Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let bytes = host_try!(read_guest_bytes(&mut caller, "service_call", ptr, len));
            let started = Instant::now();
            let state = caller.data();
            let resolved = OnceCell::new();
            let outcome = match serde_json::from_slice::<CallRequest>(&bytes) {
                Ok(request) => {
                    let timeout = clamp_to_budget(None, DEFAULT_TIMEOUT, state.remaining_budget());
                    let span = tracing::info_span!(
                        "service_call",
                        otel.name = "service_call",
                        otel.kind = "client",
                        target = %request.service,
                    );
                    block_on_within(
                        timeout,
                        &state.cancel,
                        call(state, request, timeout, &resolved).instrument(span),
                    )
                    .map_err(|e| match e {
                        HostIoError::TimedOut(_) | HostIoError::Cancelled => CallFailure::Timeout,
                        HostIoError::Failed(failure) => failure,
                    })
                }
                Err(e) => Err(CallFailure::Denied(format!(
                    "malformed service_call request: {e}"
                ))),
            };
            let target_label = resolved.into_inner().unwrap_or_else(|| "-".to_string());
            metrics::counter!(
                "cufflink_service_calls_total",
                "caller" => state.identity.service_name.clone(),
                "target" => target_label,
                "result" => result_label(&outcome),
            )
            .increment(1);
            metrics::histogram!("cufflink_service_call_duration_seconds")
                .record(started.elapsed().as_secs_f64());
            caller.data_mut().host_response = Some(envelope(&outcome).to_string());
            0
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn request(service: &str, method: &str, path: &str) -> CallRequest {
        CallRequest {
            service: service.into(),
            method: method.into(),
            path: path.into(),
            headers: BTreeMap::new(),
            body_b64: None,
        }
    }

    #[test]
    fn targets_are_validated_and_split_at_the_query() {
        let t = target(request("assets", "post", "/_fn/reprice?dry=1")).expect("target");
        assert_eq!(t.method, Method::POST);
        assert_eq!(t.path_and_query, "/_fn/reprice?dry=1");
        assert_eq!(svc_uri("acme", &t), "/svc/acme/assets/_fn/reprice?dry=1");
        let longest = "s".repeat(64);
        for good in ["Assets", "pricing_v2", longest.as_str()] {
            assert!(target(request(good, "GET", "/x")).is_ok(), "{good:?}");
        }
        let too_long = "s".repeat(65);
        for bad in ["", "a/b", "../x", "a b", "a:b", "a.b", too_long.as_str()] {
            assert_eq!(
                target(request(bad, "GET", "/x")),
                Err(CallFailure::UnknownService(bad.into())),
                "{bad:?}"
            );
        }
        for bad in ["x", "//evil.example/x", "/a b", "/a#frag", "/é"] {
            assert!(
                matches!(
                    target(request("s", "GET", bad)),
                    Err(CallFailure::Denied(_))
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn paths_cannot_leave_the_target_service() {
        let escapes = [
            "/..",
            "/../other/x",
            "/a/../../other",
            "/a/./b",
            "/.",
            "/%2e%2e/other",
            "/%2E%2E/other",
            "/.%2e/other",
            "/%2e./other",
            "/%2e",
            "/%252e%252e/other",
            "/%25252e%25252e/other",
            "/%252E./other",
            "/..%2fother",
            "/..%2Fother",
            "/x%2f..%2f..%2fapi",
            "/..%5cother",
            "/..%5Cother",
            "/a\\..\\b",
            "/a\\b",
            "/%255c",
            "/a//b",
            "/a/b//",
            "//",
            "/a/..?x=1",
        ];
        for path in escapes {
            assert!(
                matches!(
                    target(request("s", "GET", path)),
                    Err(CallFailure::Denied(_))
                ),
                "{path:?}"
            );
        }
        let kept = [
            "/",
            "/a.b",
            "/..hidden",
            "/a..",
            "/.well-known/x",
            "/files/a%2Fb",
            "/x?next=../y",
            "/x?a=//b",
            "/_fn/reprice",
        ];
        for path in kept {
            assert!(target(request("s", "GET", path)).is_ok(), "{path:?}");
        }
    }

    fn egress_target(url: &str) -> EgressTarget {
        EgressTarget {
            url: Url::parse(url).expect("url"),
            host: "platform".into(),
            addrs: vec![],
        }
    }

    #[test]
    fn a_worker_call_is_held_to_the_platform_and_the_target_service() {
        let scope = http_scope("http://platform:8080", "acme", "assets").expect("scope");
        assert_eq!(scope.path(), "/svc/acme/assets/");
        let prefixed = http_scope("http://platform:8080/internal", "acme", "assets")
            .expect("scope under a base path");
        assert_eq!(prefixed.path(), "/internal/svc/acme/assets/");
        for inside in [
            "http://platform:8080/svc/acme/assets/",
            "http://platform:8080/svc/acme/assets/books/7?x=1",
        ] {
            assert!(within(&scope, &egress_target(inside).url), "{inside}");
        }
        for outside in [
            "http://platform:8080/svc/acme/assets/../billing/x",
            "http://platform:8080/svc/acme/assets/%2e%2e/%2e%2e/api/tenants",
            "http://platform:8080/svc/other/assets/x",
            "http://platform:8080/svc/acme/assetsx/y",
            "http://platform:8080/svc/acme/assets",
            "http://platform:8080/api/tenants",
            "http://platform:9090/svc/acme/assets/x",
            "https://platform:8080/svc/acme/assets/x",
            "http://evil:8080/svc/acme/assets/x",
        ] {
            assert!(!within(&scope, &egress_target(outside).url), "{outside}");
        }
        assert!(!within(
            &prefixed,
            &egress_target("http://platform:8080/svc/acme/assets/x").url
        ));
    }

    #[test]
    fn event_streams_are_refused() {
        let with = |content_type: &'static str| {
            HeaderMap::from_iter([(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static(content_type),
            )])
        };
        assert!(matches!(
            refuse_event_stream(&with("text/event-stream; charset=utf-8")),
            Err(CallFailure::Denied(_))
        ));
        assert_eq!(refuse_event_stream(&with("application/json")), Ok(()));
        assert_eq!(refuse_event_stream(&HeaderMap::new()), Ok(()));
        assert!(matches!(
            target(request("s", "TRACE", "/x")),
            Err(CallFailure::Denied(_))
        ));
    }

    #[test]
    fn platform_headers_cannot_be_set_by_the_guest() {
        let mut call = request("s", "POST", "/x");
        call.headers = BTreeMap::from([
            ("X-Tenant-Hint".to_string(), "a".to_string()),
            ("Host".to_string(), "evil".to_string()),
            ("Authorization".to_string(), "Bearer user".to_string()),
            ("x-api-key".to_string(), "ck_deploy".to_string()),
            ("Cookie".to_string(), "session=1".to_string()),
            ("Transfer-Encoding".to_string(), "chunked".to_string()),
            (service_identity::HEADER.to_string(), "forged".to_string()),
            (
                "Traceparent".to_string(),
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            ("tracestate".to_string(), "vendor=1".to_string()),
        ]);
        call.body_b64 = Some(STANDARD.encode(b"{}"));
        let t = target(call).expect("target");
        assert_eq!(t.headers.len(), 1);
        assert_eq!(t.headers["x-tenant-hint"], "a");
        assert_eq!(t.body, b"{}");
        let headers = with_identity(t.headers, "cfsi1.k.p.s", true).expect("headers");
        assert_eq!(headers[service_identity::HEADER], "cfsi1.k.p.s");
        assert_eq!(headers["content-type"], "application/json");
        let mut bad = request("s", "GET", "/x");
        bad.headers = BTreeMap::from([("x-bad".to_string(), "line\nbreak".to_string())]);
        assert!(matches!(target(bad), Err(CallFailure::Denied(_))));
        let mut bad_body = request("s", "POST", "/x");
        bad_body.body_b64 = Some("%%".into());
        assert!(matches!(target(bad_body), Err(CallFailure::Denied(_))));
    }

    #[test]
    fn depth_grows_by_one_and_stops_at_the_maximum() {
        assert_eq!(next_depth(0), Ok(1));
        assert_eq!(next_depth(MAX_CALL_DEPTH - 1), Ok(MAX_CALL_DEPTH));
        assert_eq!(next_depth(MAX_CALL_DEPTH), Err(CallFailure::DepthExceeded));
        assert_eq!(next_depth(u8::MAX), Err(CallFailure::DepthExceeded));
        assert_eq!(inbound_depth(&json!({"caller": {"depth": 3}})), 3);
        assert_eq!(inbound_depth(&json!({"caller": {"depth": 900}})), u8::MAX);
        assert_eq!(inbound_depth(&json!({"auth": null})), 0);
    }

    #[test]
    fn guests_cannot_set_what_the_platform_owns_on_a_call() {
        let mut call = request("assets", "GET", "/x");
        call.headers = [
            "Authorization",
            "cookie",
            "X-Forwarded-For",
            "x-real-ip",
            "forwarded",
            "cf-connecting-ip",
            "proxy-connection",
            "keep-alive",
            service_identity::HEADER,
            "x-trace",
        ]
        .into_iter()
        .map(|name| (name.to_string(), "v".to_string()))
        .collect();
        let t = target(call).expect("target");
        assert_eq!(
            t.headers.keys().map(HeaderName::as_str).collect::<Vec<_>>(),
            ["x-trace"]
        );
    }

    #[test]
    fn claims_pin_the_call_and_live_within_the_budget() {
        let t = target(request("assets", "PUT", "/books/7?x=1")).expect("target");
        let caller = CallerIdentity {
            tenant_id: Uuid::from_u128(1),
            tenant_slug: "acme",
            service: "pricing",
            service_id: Uuid::from_u128(2),
            version: 5,
            depth: 1,
            on_behalf_of: Some("user-1".into()),
        };
        let c = claims(&caller, &t, "/books/7", Duration::from_secs(900), 100).expect("claims");
        assert_eq!(
            (c.method.as_str(), c.path.as_str(), c.depth, c.expires_at),
            ("PUT", "/books/7", 2, 160)
        );
        assert_eq!(c.target_service, "assets");
        assert_eq!(c.on_behalf_of.as_deref(), Some("user-1"));
        assert_eq!(
            claims(&caller, &t, "/books/7", Duration::from_millis(1500), 100)
                .expect("claims")
                .expires_at,
            101
        );
        assert_eq!(
            claims(&caller, &t, "/books/7", Duration::from_millis(999), 100),
            Err(CallFailure::Timeout),
            "a caller with less than a second left cannot outlive its budget"
        );
        let deep = CallerIdentity {
            depth: MAX_CALL_DEPTH,
            ..caller
        };
        assert_eq!(
            claims(&deep, &t, "/books/7", Duration::from_secs(1), 100),
            Err(CallFailure::DepthExceeded)
        );
    }

    #[test]
    fn the_pinned_path_is_the_path_as_sent() {
        let t = target(request("assets", "GET", "/files/{id}/\"a\"?q={x}")).expect("target");
        let uri = svc_uri("acme", &t);
        let prefix = svc_prefix("acme", "assets");
        let local = uri.parse::<axum::http::Uri>().expect("uri");
        assert_eq!(service_path(local.path(), &prefix), Ok("/files/{id}/\"a\""));
        let base = "http://platform:8080/internal";
        let scope = http_scope(base, "acme", "assets").expect("scope");
        let url = Url::parse(&format!("{base}{uri}")).expect("url");
        assert_eq!(
            service_path(url.path(), scope.path()),
            Ok("/files/%7Bid%7D/%22a%22")
        );
        assert!(matches!(
            service_path("/svc/acme/other/x", &prefix),
            Err(CallFailure::Denied(_))
        ));
        assert!(matches!(
            service_path("/svc/acme/assetsx/y", &prefix),
            Err(CallFailure::Denied(_))
        ));
    }

    async fn platform_stub() -> (String, Arc<AtomicUsize>) {
        use axum::routing::post;
        let hits = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&hits);
        let app = axum::Router::new()
            .route(
                "/svc/acme/assets/{*rest}",
                post(|headers: HeaderMap, body: String| async move {
                    let seen = |name: &str| {
                        headers
                            .get(name)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string()
                    };
                    (
                        [
                            ("x-seen-identity", seen(service_identity::HEADER)),
                            ("x-seen-authorization", seen("authorization")),
                        ],
                        format!("{body}{}", "!".repeat(if body == "big" { 2048 } else { 0 })),
                    )
                }),
            )
            .fallback(|| async { axum::http::StatusCode::IM_A_TEAPOT })
            .layer(axum::middleware::from_fn(
                move |request: Request<Body>, next: axum::middleware::Next| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    next.run(request)
                },
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        (base, hits)
    }

    #[tokio::test]
    async fn the_worker_sends_calls_to_the_platform_over_http() {
        let (base, hits) = platform_stub().await;
        let egress = EgressGuard::new(1024, Duration::from_secs(5));
        let scope = http_scope(&base, "acme", "assets").expect("scope");
        let send = |body: &'static [u8], url: Option<String>| {
            let mut call = request("assets", "POST", "/_fn/reprice?dry=1");
            call.body_b64 = Some(STANDARD.encode(body));
            call.headers = BTreeMap::from([("authorization".into(), "Bearer user".into())]);
            let t = target(call).expect("target");
            let headers = with_identity(t.headers.clone(), "cfsi1.k.p.s", true).expect("headers");
            let url = url.unwrap_or_else(|| format!("{base}{}", svc_uri("acme", &t)));
            let (egress, scope) = (egress.clone(), scope.clone());
            async move {
                send_http(
                    &egress,
                    url,
                    &scope,
                    t,
                    headers,
                    Duration::from_secs(5),
                    1024,
                )
                .await
            }
        };
        let response = send(b"hello", None).await.expect("response");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"hello");
        assert_eq!(response.headers["x-seen-identity"], "cfsi1.k.p.s");
        assert_eq!(response.headers["x-seen-authorization"], "");
        assert!(matches!(
            send(b"big", None).await,
            Err(CallFailure::Transport(_))
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        for escaping in [
            format!("{base}/svc/acme/assets/../billing/_fn/x"),
            format!("{base}/svc/acme/assets/%2e%2e/%2E%2E/%2e%2e/api/tenants"),
            format!("{base}/svc/acme/assets/.%2e/.%2e/other/x"),
            format!("{base}/api/tenants"),
            format!("{base}/svc/other/assets/x"),
        ] {
            assert!(
                matches!(
                    send(b"hello", Some(escaping.clone())).await,
                    Err(CallFailure::Denied(_))
                ),
                "{escaping}"
            );
        }
        assert_eq!(hits.load(Ordering::SeqCst), 2, "nothing escaping was sent");
    }

    #[test]
    fn envelopes_and_labels_follow_the_outcome() {
        let ok = Ok(CallResponse {
            status: 201,
            headers: BTreeMap::from([("x-a".to_string(), "1".to_string())]),
            body: b"hi".to_vec(),
        });
        assert_eq!(
            envelope(&ok),
            json!({"ok": true, "status": 201, "headers": {"x-a": "1"}, "body_b64": "aGk="})
        );
        assert_eq!(result_label(&ok), "ok");
        let failed = Err(CallFailure::DepthExceeded);
        assert_eq!(envelope(&failed)["error"]["kind"], "depth_exceeded");
        assert_eq!(result_label(&failed), "depth");
        for (status, label) in [(404, "http_4xx"), (503, "http_5xx")] {
            assert_eq!(
                result_label(&Ok(CallResponse {
                    status,
                    headers: BTreeMap::new(),
                    body: vec![]
                })),
                label
            );
        }
    }
}
