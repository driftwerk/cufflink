use crate::errors::PlatformError;
use crate::{middleware, AppState};
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Router;
use eyre::{eyre, Result, WrapErr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

const ARTIFACT_BODY_LIMIT: usize = 250 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpProfile {
    Legacy,
    Hardened,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsPolicy {
    Permissive,
    Origins(Vec<HeaderValue>),
    Unset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpLimits {
    pub profile: HttpProfile,
    pub data_body_limit: usize,
    pub data_timeout: Option<Duration>,
    pub max_concurrent_requests: Option<usize>,
    pub cors: CorsPolicy,
    pub crud_max_in_items: Option<usize>,
    pub rate_limit_by_default: bool,
}

impl HttpLimits {
    pub fn legacy() -> Self {
        Self {
            profile: HttpProfile::Legacy,
            data_body_limit: ARTIFACT_BODY_LIMIT,
            data_timeout: None,
            max_concurrent_requests: None,
            cors: CorsPolicy::Permissive,
            crud_max_in_items: None,
            rate_limit_by_default: false,
        }
    }

    pub fn from_env() -> Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let profile = match lookup("CUFFLINK_HTTP_PROFILE").as_deref().map(str::trim) {
            None | Some("") | Some("legacy") => HttpProfile::Legacy,
            Some("hardened") => HttpProfile::Hardened,
            Some(other) => {
                return Err(eyre!(
                    "CUFFLINK_HTTP_PROFILE must be 'legacy' or 'hardened', not '{other}'"
                ))
            }
        };
        let hardened = profile == HttpProfile::Hardened;
        let number = |name: &str, legacy: usize, hardened_default: usize| -> Result<usize> {
            let default = if hardened { hardened_default } else { legacy };
            lookup(name).map_or(Ok(default), |raw| {
                raw.trim()
                    .parse()
                    .wrap_err_with(|| format!("{name} must be a whole number, not '{raw}'"))
            })
        };
        let optional = |value: usize| (value > 0).then_some(value);
        let data_body_limit = number("HTTP_DATA_BODY_LIMIT_BYTES", ARTIFACT_BODY_LIMIT, 4 << 20)?;
        if data_body_limit == 0 {
            return Err(eyre!("HTTP_DATA_BODY_LIMIT_BYTES must be greater than 0"));
        }
        Ok(Self {
            profile,
            data_body_limit,
            data_timeout: optional(number("HTTP_DATA_TIMEOUT_SECS", 0, 60)?)
                .map(|secs| Duration::from_secs(secs as u64)),
            max_concurrent_requests: optional(number("HTTP_MAX_CONCURRENT_REQUESTS", 0, 1024)?),
            cors: cors_policy(lookup("CORS_ALLOWED_ORIGINS"), hardened)?,
            crud_max_in_items: optional(number("CRUD_MAX_IN_ITEMS", 0, 100)?),
            rate_limit_by_default: hardened && lookup("RATE_LIMIT_ENABLED").is_none(),
        })
    }
}

fn cors_policy(raw: Option<String>, hardened: bool) -> Result<CorsPolicy> {
    let Some(raw) = raw.filter(|raw| !raw.trim().is_empty()) else {
        return Ok(if hardened {
            CorsPolicy::Unset
        } else {
            CorsPolicy::Permissive
        });
    };
    raw.split(',')
        .map(str::trim)
        .filter(|origin| !origin.is_empty())
        .map(allowed_origin)
        .collect::<Result<Vec<_>>>()
        .map(CorsPolicy::Origins)
}

fn allowed_origin(origin: &str) -> Result<HeaderValue> {
    let invalid = |reason: &str| {
        eyre!(
            "CORS_ALLOWED_ORIGINS: '{origin}' is not a valid origin ({reason}; expected \
             scheme://host[:port], e.g. https://app.example.com)"
        )
    };
    if origin == "*" {
        return Err(eyre!(
            "CORS_ALLOWED_ORIGINS lists origins; '*' is not accepted"
        ));
    }
    let url = url::Url::parse(origin).map_err(|e| invalid(&e.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(invalid("the scheme must be http or https"));
    }
    if url.host_str().is_none_or(|host| host.contains('*')) {
        return Err(invalid("wildcard hosts are not supported"));
    }
    let canonical = url.origin().ascii_serialization();
    if canonical != origin {
        return Err(invalid(&format!("a browser sends '{canonical}'")));
    }
    HeaderValue::from_str(origin).map_err(|e| invalid(&e.to_string()))
}

impl HttpLimits {
    pub fn check_serving(&self) -> Result<()> {
        match self.cors {
            CorsPolicy::Unset => Err(eyre!(
                "CORS_ALLOWED_ORIGINS is required with CUFFLINK_HTTP_PROFILE=hardened"
            )),
            _ => Ok(()),
        }
    }
}

fn cors_layer(policy: &CorsPolicy) -> CorsLayer {
    match policy {
        CorsPolicy::Permissive => CorsLayer::permissive(),
        CorsPolicy::Unset => CorsLayer::new(),
        CorsPolicy::Origins(origins) => CorsLayer::new()
            .allow_origin(AllowOrigin::list(origins.iter().cloned()))
            .allow_methods(Any)
            .allow_headers(Any)
            .expose_headers(Any),
    }
}

pub(crate) fn data_group(
    routes: Router<Arc<AppState>>,
    limits: &HttpLimits,
) -> Router<Arc<AppState>> {
    let routes = routes
        .layer(DefaultBodyLimit::max(limits.data_body_limit))
        .layer(axum::middleware::map_response(close_when_too_large));
    match limits.data_timeout {
        Some(timeout) => routes.layer(axum::middleware::from_fn_with_state(timeout, deadline)),
        None => routes,
    }
}

async fn close_when_too_large(mut response: Response) -> Response {
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        response
            .headers_mut()
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
    }
    response
}

async fn deadline(State(timeout): State<Duration>, request: Request, next: Next) -> Response {
    tokio::time::timeout(timeout, next.run(request))
        .await
        .unwrap_or_else(|_| PlatformError::Unavailable("request timed out".into()).into_response())
}

async fn shed_load(
    State(permits): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    match permits.try_acquire_owned() {
        Ok(_permit) => next.run(request).await,
        Err(_) => {
            metrics::counter!("cufflink_http_load_shed_total").increment(1);
            PlatformError::Unavailable("server busy".into()).into_response()
        }
    }
}

pub(super) fn outer(routes: Router<Arc<AppState>>, state: &AppState) -> Router<Arc<AppState>> {
    let limits = &state.config.http;
    let mut rate_limit_settings = state.config.rate_limit.clone();
    rate_limit_settings.enabled |= limits.rate_limit_by_default;
    let rate_limiter = if rate_limit_settings.enabled {
        info!(
            per_second = rate_limit_settings.per_second,
            burst = rate_limit_settings.burst,
            ip_source = ?rate_limit_settings.ip_source,
            exempt_private_peers = rate_limit_settings.exempt_private_peers,
            exempt_ips = rate_limit_settings.exempt_ips.len(),
            "Rate limiting enabled"
        );
        Some(middleware::rate_limit::GlobalRateLimiter::new(
            state.redis.clone(),
            rate_limit_settings,
        ))
    } else {
        info!("Rate limiting disabled (RATE_LIMIT_ENABLED is not true)");
        None
    };

    let routes = routes.layer(DefaultBodyLimit::max(ARTIFACT_BODY_LIMIT));
    let routes = match limits.max_concurrent_requests {
        Some(max) => routes.layer(axum::middleware::from_fn_with_state(
            Arc::new(Semaphore::new(max)),
            shed_load,
        )),
        None => routes,
    };
    routes
        .layer(axum::middleware::from_fn_with_state(
            rate_limiter,
            middleware::rate_limit::rate_limit_middleware,
        ))
        .layer(cors_layer(&limits.cors))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &axum::http::Request<_>| {
                    crate::telemetry::request_span(req.method(), req.uri().path(), req.headers())
                })
                .on_response(
                    |resp: &axum::http::Response<_>, _latency: Duration, span: &tracing::Span| {
                        crate::telemetry::record_response(span, resp.status())
                    },
                ),
        )
        .layer(axum::middleware::from_fn(middleware::http_metrics::record))
        .layer(middleware::request_id::propagate_layer())
        .layer(middleware::request_id::set_layer())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn limits(vars: &[(&str, &str)]) -> Result<HttpLimits> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        HttpLimits::from_vars(|name| vars.get(name).cloned())
    }

    #[tokio::test]
    async fn a_body_refused_as_too_large_closes_its_connection() {
        use tower::ServiceExt;
        let routes = Router::new().route(
            "/notes",
            axum::routing::post(|axum::Json(_): axum::Json<serde_json::Value>| async { "ok" }),
        );
        let limits = HttpLimits {
            data_body_limit: 8,
            ..HttpLimits::legacy()
        };
        let engine = crate::wasm_runtime::WasmEngine::new(
            Default::default(),
            crate::shutdown::Shutdown::new(Duration::from_secs(1)),
        )
        .expect("engine");
        let app = data_group(routes, &limits).with_state(AppState::minimal(engine).await);
        let request = |body: &'static str| {
            axum::http::Request::post("/notes")
                .header(header::CONTENT_TYPE, "application/json")
                .body(axum::body::Body::from(body))
                .expect("request")
        };
        let refused = app
            .clone()
            .oneshot(request(r#"{"title": "too long"}"#))
            .await
            .expect("response");
        assert_eq!(refused.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(refused.headers()[header::CONNECTION], "close");
        let accepted = app.oneshot(request("{}")).await.expect("response");
        assert_eq!(accepted.status(), StatusCode::OK);
        assert!(accepted.headers().get(header::CONNECTION).is_none());
    }

    #[test]
    fn an_install_that_sets_nothing_keeps_todays_limits() {
        assert_eq!(limits(&[]).expect("legacy"), HttpLimits::legacy());
        assert_eq!(
            limits(&[("CUFFLINK_HTTP_PROFILE", "legacy")]).expect("legacy"),
            HttpLimits::legacy()
        );
    }

    #[test]
    fn the_hardened_profile_tightens_every_default() {
        let hardened = limits(&[
            ("CUFFLINK_HTTP_PROFILE", "hardened"),
            (
                "CORS_ALLOWED_ORIGINS",
                "https://app.example.com, https://admin.example.com",
            ),
        ])
        .expect("hardened");
        assert_eq!(
            hardened,
            HttpLimits {
                profile: HttpProfile::Hardened,
                data_body_limit: 4 << 20,
                data_timeout: Some(Duration::from_secs(60)),
                max_concurrent_requests: Some(1024),
                cors: CorsPolicy::Origins(vec![
                    HeaderValue::from_static("https://app.example.com"),
                    HeaderValue::from_static("https://admin.example.com"),
                ]),
                crud_max_in_items: Some(100),
                rate_limit_by_default: true,
            }
        );
    }

    #[test]
    fn each_limit_is_overridable() {
        let tuned = limits(&[
            ("CUFFLINK_HTTP_PROFILE", "hardened"),
            ("CORS_ALLOWED_ORIGINS", "https://app.example.com"),
            ("HTTP_DATA_BODY_LIMIT_BYTES", "1024"),
            ("HTTP_DATA_TIMEOUT_SECS", "0"),
            ("HTTP_MAX_CONCURRENT_REQUESTS", "0"),
            ("CRUD_MAX_IN_ITEMS", "7"),
            ("RATE_LIMIT_ENABLED", "false"),
        ])
        .expect("tuned");
        assert_eq!(tuned.data_body_limit, 1024);
        assert_eq!(tuned.data_timeout, None);
        assert_eq!(tuned.max_concurrent_requests, None);
        assert_eq!(tuned.crud_max_in_items, Some(7));
        assert!(!tuned.rate_limit_by_default);
        let legacy = limits(&[
            ("HTTP_DATA_TIMEOUT_SECS", "5"),
            (
                "CORS_ALLOWED_ORIGINS",
                "https://a.example, http://localhost:3000",
            ),
        ])
        .expect("legacy overrides");
        assert_eq!(legacy.data_timeout, Some(Duration::from_secs(5)));
        assert_eq!(
            legacy.cors,
            CorsPolicy::Origins(vec![
                HeaderValue::from_static("https://a.example"),
                HeaderValue::from_static("http://localhost:3000"),
            ])
        );
    }

    #[test]
    fn only_a_serving_platform_needs_the_origin_allowlist() {
        let worker = limits(&[("CUFFLINK_HTTP_PROFILE", "hardened")])
            .expect("a worker loads without CORS_ALLOWED_ORIGINS");
        assert_eq!(worker.cors, CorsPolicy::Unset);
        let err = worker
            .check_serving()
            .expect_err("a platform does not serve");
        assert!(format!("{err}").contains("CORS_ALLOWED_ORIGINS is required"));
        assert!(HttpLimits::legacy().check_serving().is_ok());
    }

    #[test]
    fn invalid_values_are_startup_errors() {
        for (vars, reason) in [
            (
                vec![("CUFFLINK_HTTP_PROFILE", "strict")],
                "must be 'legacy' or 'hardened'",
            ),
            (vec![("CORS_ALLOWED_ORIGINS", "*")], "'*' is not accepted"),
            (
                vec![("CORS_ALLOWED_ORIGINS", "https://a\u{1}.example")],
                "not a valid origin",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "app.example.com")],
                "relative URL without a base",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "https://a.example/")],
                "a browser sends 'https://a.example'",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "https://A.example")],
                "a browser sends 'https://a.example'",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "https://a.example:443")],
                "a browser sends 'https://a.example'",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "https://*.example")],
                "wildcard hosts are not supported",
            ),
            (
                vec![("CORS_ALLOWED_ORIGINS", "ftp://a.example")],
                "the scheme must be http or https",
            ),
            (
                vec![("HTTP_DATA_TIMEOUT_SECS", "soon")],
                "HTTP_DATA_TIMEOUT_SECS must be a whole number",
            ),
            (
                vec![("HTTP_DATA_BODY_LIMIT_BYTES", "0")],
                "must be greater than 0",
            ),
        ] {
            let err = limits(&vars).expect_err(reason);
            assert!(format!("{err:#}").contains(reason), "{vars:?}: {err:#}");
        }
    }
}
