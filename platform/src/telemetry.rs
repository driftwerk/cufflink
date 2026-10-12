pub use cufflink_telemetry::{init, install_metrics, shutdown};

use cufflink_telemetry::Scope;
use std::cell::Cell;
use std::future::Future;

pub fn record_pool_gauges(state: &crate::AppState) {
    cufflink_db::pool_status_gauges("platform", &state.platform_db);
    for (slug, status) in state.tenant_sessions.pool_statuses() {
        cufflink_db::record_status_gauges(&format!("tenant:{slug}"), &status);
    }
    for (label, status) in state.tenant_sessions.login_pool_statuses() {
        cufflink_db::record_status_gauges(&label, &status);
    }
}

fn is_id_like(segment: &str) -> bool {
    let b = segment.as_bytes();
    let uuid_shaped = b.len() == 36
        && b.iter().enumerate().all(|(i, &c)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        });
    uuid_shaped
        || b.len() >= 25
        || (b.len() >= 16 && b.iter().all(|c| c.is_ascii_hexdigit()))
        || (!segment.is_empty() && b.iter().all(|c| c.is_ascii_digit()))
}

fn is_name_safe(segment: &str) -> bool {
    (1..=64).contains(&segment.len())
        && segment
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
}

pub fn span_name_for_path(method: &str, path: &str) -> String {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segments.as_slice() {
        ["health"] | ["metrics"] => format!("{} /{}", method, segments[0]),
        ["api", rest @ ..] if rest.iter().all(|s| is_id_like(s) || is_name_safe(s)) => {
            let sanitized: Vec<&str> = segments
                .iter()
                .map(|s| if is_id_like(s) { ":id" } else { *s })
                .collect();
            format!("{} /{}", method, sanitized.join("/"))
        }
        _ => format!("{} unmatched", method),
    }
}

tokio::task_local! {
    static RESOLVED: Cell<Option<String>>;
}

pub async fn resolving<T>(request: impl Future<Output = T>) -> (T, Option<String>) {
    RESOLVED
        .scope(Cell::new(None), async {
            let answer = request.await;
            (answer, RESOLVED.with(Cell::take))
        })
        .await
}

fn route_name(scope: &Scope, leaf: &str) -> String {
    match &scope.service {
        Some(service) => format!("{}/{service}/{leaf}", scope.tenant),
        None => format!("{}/{leaf}", scope.tenant),
    }
}

pub fn resolve(scope: &Scope, leaf: &str) {
    let route = route_name(scope, leaf);
    let span = tracing::Span::current();
    scope.record_on(&span);
    span.record("otel.name", route.as_str());
    let _ = RESOLVED.try_with(|resolved| resolved.set(Some(route)));
}

struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

pub fn request_span(
    method: &axum::http::Method,
    path: &str,
    headers: &axum::http::HeaderMap,
) -> tracing::Span {
    let name = span_name_for_path(method.as_str(), path);
    let span = cufflink_telemetry::scopable_span!(
        parent: None,
        "request",
        otel.name = %name,
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
        http.method = %method,
        http.response.status_code = tracing::field::Empty,
        url.path = %path,
        request.id = tracing::field::Empty,
    );
    if let Some(id) = crate::middleware::request_id::request_id(headers) {
        span.record("request.id", id);
    }
    let parent =
        opentelemetry::global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(headers)));
    {
        use opentelemetry::trace::TraceContextExt;
        let remote = parent.span().span_context().clone();
        if remote.is_valid() && remote.is_sampled() {
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let _ = span.set_parent(parent);
        }
    }
    span
}

pub fn record_response(span: &tracing::Span, status: axum::http::StatusCode) {
    span.record("http.response.status_code", status.as_u16());
    if status.is_server_error() {
        span.record("otel.status_code", "ERROR");
    }
}

pub struct HashMapInjector<'a>(pub &'a mut std::collections::HashMap<String, String>);

impl opentelemetry::propagation::Injector for HashMapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

pub fn inject_current_context(carrier: &mut std::collections::HashMap<String, String>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let ctx = tracing::Span::current().context();
    opentelemetry::global::get_text_map_propagator(|p| {
        p.inject_context(&ctx, &mut HashMapInjector(carrier))
    });
}

pub fn context_from_map(
    headers: &std::collections::HashMap<String, String>,
) -> Option<opentelemetry::Context> {
    struct MapExtractor<'a>(&'a std::collections::HashMap<String, String>);
    impl opentelemetry::propagation::Extractor for MapExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(|s| s.as_str())
        }
        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }
    let ctx = opentelemetry::global::get_text_map_propagator(|p| p.extract(&MapExtractor(headers)));
    use opentelemetry::trace::TraceContextExt;
    let remote = ctx.span().span_context().clone();
    (remote.is_valid() && remote.is_sampled()).then_some(ctx)
}

pub fn context_from_nats_headers(
    headers: &async_nats::HeaderMap,
) -> Option<opentelemetry::Context> {
    let carrier: std::collections::HashMap<String, String> = headers
        .iter()
        .filter_map(|(k, vs)| {
            vs.first()
                .map(|v| (k.to_string().to_lowercase(), v.to_string()))
        })
        .collect();
    context_from_map(&carrier)
}

pub fn traced_nats_headers() -> async_nats::HeaderMap {
    let mut carrier = std::collections::HashMap::new();
    inject_current_context(&mut carrier);
    let mut headers = async_nats::HeaderMap::new();
    for (k, v) in &carrier {
        headers.insert(k.as_str(), v.as_str());
    }
    headers
}

#[cfg(test)]
pub(crate) fn is_a_root_under_a_pump(message_span: impl FnOnce() -> tracing::Span) -> bool {
    use tracing_subscriber::registry::LookupSpan;
    tracing::subscriber::with_default(tracing_subscriber::registry(), || {
        let _pump = tracing::info_span!("task", task = "pump").entered();
        message_span()
            .with_subscriber(|(id, dispatch)| {
                dispatch
                    .downcast_ref::<tracing_subscriber::Registry>()
                    .and_then(|registry| registry.span(id))
                    .is_some_and(|span| span.parent().is_none())
            })
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_paths_keep_method_and_collapse_ids() {
        assert_eq!(
            span_name_for_path("GET", "/api/services/12345/logs"),
            "GET /api/services/:id/logs"
        );
        assert_eq!(span_name_for_path("GET", "/health"), "GET /health");
    }

    #[test]
    fn nothing_under_svc_is_named_from_its_path() {
        for path in [
            "/svc/default/orders-service/_fn/handle_update_status",
            "/svc/default/stock-service/items",
            "/svc/default/stock-service/items/97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad",
            "/svc/default/stock-service/_meta/health",
            "/svc/default/_jobs/97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad",
            "/svc/default",
            "/svc",
        ] {
            assert_eq!(span_name_for_path("POST", path), "POST unmatched", "{path}");
        }
    }

    #[test]
    fn a_resolved_request_is_named_by_its_scope_and_leaf() {
        let service = Scope::service("acme", "notes", 3);
        assert_eq!(route_name(&service, "list_notes"), "acme/notes/list_notes");
        assert_eq!(
            route_name(&service, "_meta/health"),
            "acme/notes/_meta/health"
        );
        assert_eq!(route_name(&Scope::tenant("acme"), "_jobs"), "acme/_jobs");
    }

    #[tokio::test]
    async fn the_route_a_handler_resolves_is_answered_beside_its_response() {
        let (answer, route) = resolving(async {
            resolve(&Scope::service("acme", "notes", 3), "notes");
            404
        })
        .await;
        assert_eq!((answer, route.as_deref()), (404, Some("acme/notes/notes")));

        let (answer, route) = resolving(async { 429 }).await;
        assert_eq!((answer, route), (429, None));

        resolve(&Scope::tenant("acme"), "_jobs");
    }

    #[test]
    fn junk_paths_collapse_to_unmatched() {
        assert_eq!(span_name_for_path("GET", ""), "GET unmatched");
        assert_eq!(
            span_name_for_path("GET", "/wp-admin/setup.php%00"),
            "GET unmatched"
        );
        assert_eq!(
            span_name_for_path("GET", &format!("/svc/{}/svc/_fn/x", "a".repeat(80))),
            "GET unmatched"
        );
        assert_eq!(
            span_name_for_path("GET", "/api/services/../../etc%2fpasswd"),
            "GET unmatched"
        );
    }

    #[test]
    fn id_like_covers_common_identifier_shapes() {
        assert!(is_id_like("97c9e5c1-3f4a-4d0f-a53a-2273c4be3aad"));
        assert!(is_id_like("12345"));
        assert!(is_id_like("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(is_id_like("deadbeefcafebabe0123"));
        assert!(!is_id_like("items"));
        assert!(!is_id_like("handle_update_status"));
    }

    fn traceparent_map(header: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([("traceparent".to_string(), header.to_string())])
    }

    #[test]
    fn context_from_map_accepts_only_valid_sampled_parents() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let valid = "00-97c9e5c13f4a4d0fa53a2273c4be3aad-00f067aa0ba902b7-01";
        assert!(context_from_map(&traceparent_map(valid)).is_some());
        let unsampled = "00-97c9e5c13f4a4d0fa53a2273c4be3aad-00f067aa0ba902b7-00";
        assert!(context_from_map(&traceparent_map(unsampled)).is_none());
        let all_zero = "00-00000000000000000000000000000000-0000000000000000-01";
        assert!(context_from_map(&traceparent_map(all_zero)).is_none());
        assert!(context_from_map(&traceparent_map("garbage")).is_none());
        assert!(context_from_map(&std::collections::HashMap::new()).is_none());
    }

    #[test]
    fn inject_extract_round_trip_preserves_trace_identity() {
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
        };
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let remote = SpanContext::new(
            TraceId::from_hex("97c9e5c13f4a4d0fa53a2273c4be3aad").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let ctx = opentelemetry::Context::new().with_remote_span_context(remote.clone());
        let mut carrier = std::collections::HashMap::new();
        opentelemetry::global::get_text_map_propagator(|p| {
            p.inject_context(&ctx, &mut HashMapInjector(&mut carrier))
        });
        let round_tripped = context_from_map(&carrier).expect("valid context survives");
        assert_eq!(
            round_tripped.span().span_context().trace_id(),
            remote.trace_id()
        );
    }
}
