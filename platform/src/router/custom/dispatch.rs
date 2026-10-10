use super::body;
use super::request::{guest_headers, parse_body, RequestV2, RouteRequest, StrictTarget};
use super::stream::GuestBodies;
use crate::audit::{Actor, AuditRecord};
use crate::errors::{PlatformError, PlatformResult};
use crate::outbox::emit::Sink;
use crate::registry::RegisteredService;
use crate::router::authz::guest_auth_json;
use crate::router::guest_response::decode;
use crate::router::postures::{authorize_route, Authorized, NamedPrincipal, ServiceCaller, Target};
use crate::router::sse::stream_principal;
use crate::router::sse_bridge::{self, Reducer, ReducerContext};
use crate::router::strict::split_svc_path;
use crate::router::{jobs, map_execution_error};
use crate::wasm_runtime::{CompiledModule, Invocation, InvocationKind, WasmEngine};
use crate::AppState;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Uri};
use axum::response::{IntoResponse, Response};
use cufflink_telemetry::Kind;
use cufflink_types::{CustomRouteDefinition, RoutingMode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const HANDLER_BUDGET: Duration = Duration::from_secs(30);

pub(crate) struct CustomCall {
    pub service: Arc<RegisteredService>,
    pub route: usize,
    pub strict: Option<StrictTarget>,
    pub method: Method,
    pub uri: Uri,
    pub query: HashMap<String, String>,
    pub headers: HeaderMap,
    pub body: Body,
    pub peer: Option<SocketAddr>,
}

pub(crate) enum Refusal {
    Platform(PlatformError),
    Body(Response),
}

impl From<PlatformError> for Refusal {
    fn from(error: PlatformError) -> Self {
        Refusal::Platform(error)
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        match self {
            Refusal::Platform(error) => error.into_response(),
            Refusal::Body(response) => response,
        }
    }
}

pub(crate) async fn run(state: &Arc<AppState>, call: CustomCall) -> Response {
    let started = std::time::Instant::now();
    let labels = route_label(&call).map(|route| (call.service.name.clone(), route));
    if let Some(route) = call.service.manifest.custom_routes.get(call.route) {
        crate::telemetry::resolve(
            &call
                .service
                .scope()
                .running(Kind::Route, route.handler.as_str()),
            route_leaf(route, call.strict.is_some()),
        );
    }
    let response = dispatch(state, call)
        .await
        .unwrap_or_else(IntoResponse::into_response);
    if let Some((service, route)) = labels {
        metrics::histogram!(
            "cufflink_custom_route_duration_seconds",
            "service" => service,
            "route" => route,
            "status_class" => status_class(response.status()),
        )
        .record(started.elapsed().as_secs_f64());
    }
    response
}

fn route_label(call: &CustomCall) -> Option<String> {
    call.service
        .manifest
        .custom_routes
        .get(call.route)
        .map(|route| match call.strict {
            Some(_) => route.path.clone(),
            None => route.handler.clone(),
        })
}

fn route_leaf(route: &CustomRouteDefinition, strict: bool) -> &str {
    match strict {
        false => &route.handler,
        true => route
            .path
            .split('/')
            .find(|segment| !segment.is_empty())
            .filter(|segment| !segment.starts_with('{'))
            .unwrap_or(":id"),
    }
}

fn status_class(status: axum::http::StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        _ => "5xx",
    }
}

fn guest_method(method: &Method, strict: bool) -> &str {
    if strict && method == Method::HEAD {
        Method::GET.as_str()
    } else {
        method.as_str()
    }
}

fn budget(route: &CustomRouteDefinition) -> Duration {
    route.timeout_secs.map_or_else(
        || InvocationKind::Route.budget(HANDLER_BUDGET),
        |secs| Duration::from_secs(u64::from(secs)),
    )
}

fn body_limit(route: &CustomRouteDefinition) -> u64 {
    route
        .body_limit_bytes
        .unwrap_or(CustomRouteDefinition::DEFAULT_BODY_LIMIT_BYTES)
}

async fn dispatch(state: &Arc<AppState>, call: CustomCall) -> Result<Response, Refusal> {
    let CustomCall {
        service,
        route,
        strict,
        method,
        uri,
        query,
        headers,
        body,
        peer,
    } = call;
    let route = service
        .manifest
        .custom_routes
        .get(route)
        .ok_or_else(|| PlatformError::NotFound("Not found".into()))?;
    let tenant = state
        .tenants
        .get_or_load(service.tenant_id)
        .await?
        .ok_or_else(|| PlatformError::NotFound("Service not found".into()))?;
    let target = Target {
        method: &method,
        path: split_svc_path(uri.path()).map_or(uri.path(), |(_, _, rest)| rest),
    };
    let Authorized { principal, call } = authorize_route(
        state,
        &service,
        &tenant,
        &headers,
        route.posture.as_ref(),
        &target,
    )
    .await?;
    let (auth_user, principal) = principal.into_parts();
    let actor = actor(call.as_ref(), &auth_user, principal.as_ref());

    let limit = body_limit(route);
    let body = body::read(body, usize::try_from(limit).unwrap_or(usize::MAX))
        .await
        .map_err(Refusal::Body)?;
    let parts = if route.multipart {
        Some(body::multipart(&headers, body.clone(), limit).await?)
    } else {
        None
    };

    let wasm_engine = state
        .wasm_engine
        .as_ref()
        .ok_or_else(|| PlatformError::Internal("WASM runtime not available".into()))?;
    let compiled = module(wasm_engine, &service).await?;

    let body_value = parse_body(&body);
    let audit_body = if auth_user.is_some() {
        body_value.clone()
    } else {
        json!({ "redacted": "unauthenticated request body not recorded" })
    };

    let client_ip = crate::middleware::rate_limit::client_ip(
        &headers,
        peer.map(|peer| peer.ip()),
        state.config.client_ip_source,
    );

    let request_json = RouteRequest {
        method: guest_method(&method, strict.is_some()),
        handler: &route.handler,
        headers: guest_headers(&tenant, &service.name, route, &headers),
        body: body_value,
        raw_body: &body,
        query: &query,
        tenant: &service.tenant_slug,
        service: &service.name,
        auth: guest_auth_json(state, &auth_user, service.tenant_id).await,
        posture: route.posture.as_ref(),
        caller: call.as_ref(),
        v2: (service.manifest.routing == RoutingMode::Strict || route.uses_request_v2()).then(
            || RequestV2 {
                strict: strict.as_ref(),
                query_string: uri.query().unwrap_or_default(),
                client_ip,
                multipart: parts.as_deref(),
                principal,
            },
        ),
    }
    .into_json()?;

    if let Some(cfg) = &route.job {
        let auth_snapshot = auth_user.as_ref().map(|u| jobs::AuthSnapshot {
            subject: u.subject.clone(),
            claims: request_json["auth"].clone(),
        });
        return jobs::submit_job(
            state,
            &service,
            &route.handler,
            &request_json,
            auth_snapshot,
            cfg,
        )
        .await
        .map(IntoResponse::into_response)
        .map_err(Refusal::from);
    }

    let sse = match &route.sse {
        Some(sse) => {
            let params = strict.as_ref().map_or(&[][..], |strict| &strict.params[..]);
            let principal = stream_principal(auth_user.as_ref(), client_ip)?;
            let bridge = sse_bridge::open(state, sse, &service, params, principal).await?;
            let reducer = sse.reducer.as_ref().map(|handler| Reducer {
                state: state.clone(),
                compiled: compiled.clone(),
                handler: handler.clone(),
                service: service.clone(),
                context: ReducerContext::of(&request_json),
            });
            Some((bridge, reducer))
        }
        None => None,
    };

    info!(
        tenant = %service.tenant_slug,
        service = %service.name,
        "Executing WASM handler '{}'", route.handler
    );
    let raw = execute(
        state,
        Execution {
            service: &service,
            handler: &route.handler,
            compiled,
            request: &request_json,
            kind: InvocationKind::Route,
            budget: ServiceCaller::bound(call.as_ref(), budget(route)),
        },
    )
    .await?;
    Sink::of(state, &tenant, &service).record(
        &audit_record(
            &service,
            &route.handler,
            &auth_user,
            actor,
            crate::middleware::request_id::request_id(&headers),
            audit_body,
        )
        .redact(&route.audit_redact),
    );
    let response =
        decode(raw).map_err(|e| PlatformError::Internal(format!("WASM execution error: {e}")))?;
    let mut bodies = GuestBodies {
        state,
        service: &service,
        tenant: &tenant,
        sse,
    };
    Ok(response.render(&service.name, &mut bodies).await)
}

async fn module(
    wasm_engine: &WasmEngine,
    service: &RegisteredService,
) -> PlatformResult<Arc<CompiledModule>> {
    match wasm_engine
        .get_module(
            service.service_id,
            service.version,
            service.wasm_hash.as_deref(),
        )
        .await
    {
        Some(compiled) => Ok(compiled),
        None => {
            metrics::counter!(
                "cufflink_router_wasm_binary_missing_total",
                "service" => service.name.clone(),
            )
            .increment(1);
            tracing::error!(
                tenant = %service.tenant_slug,
                service = %service.name,
                version = service.version,
                service_id = %service.service_id,
                "WASM binary missing for active service version — this is corruption, \
                 the deploy should have been rejected. Check S3 and consider rolling back."
            );
            Err(PlatformError::Internal(format!(
                "WASM binary for {} v{} is missing from storage — service is broken until rolled back or redeployed",
                service.name, service.version
            )))
        }
    }
}

pub(crate) struct Execution<'a> {
    pub service: &'a RegisteredService,
    pub handler: &'a str,
    pub compiled: Arc<CompiledModule>,
    pub request: &'a Value,
    pub kind: InvocationKind,
    pub budget: Duration,
}

pub(crate) async fn execute(
    state: &Arc<AppState>,
    execution: Execution<'_>,
) -> PlatformResult<String> {
    let Execution {
        service,
        handler,
        compiled,
        request,
        kind,
        budget,
    } = execution;
    let request_str = serde_json::to_string(request)
        .map_err(|e| PlatformError::Internal(format!("Failed to serialize request: {}", e)))?;
    if state.config.is_worker_routed(&service.name, handler) {
        let job = crate::worker_dispatch::WasmJob::of(service, handler, request_str);
        crate::worker_dispatch::execute_on_worker(&state.nats, &service.name, job, budget).await
    } else {
        state
            .wasm_engine
            .as_ref()
            .ok_or_else(|| PlatformError::Internal("WASM runtime not available".into()))?
            .run(
                state.clone(),
                Invocation {
                    compiled,
                    handler: handler.to_string(),
                    request_json: request_str,
                    identity: service.identity(),
                    publishes: service.manifest.publishes.clone(),
                    extensions: service.manifest.extensions.clone(),
                    kind,
                    scope_kind: None,
                    budget,
                    cancel: None,
                },
            )
            .await
            .map_err(|e| map_execution_error("WASM", e))
    }
}

fn actor(
    call: Option<&ServiceCaller>,
    user: &Option<crate::auth::AuthUser>,
    principal: Option<&NamedPrincipal>,
) -> Actor {
    let partner = |key: &Value| {
        Some(Actor::partner(
            key.get("id")?.as_str()?.parse().ok()?,
            key.get("name")?.as_str()?,
        ))
    };
    match (call, principal) {
        (Some(call), _) => Actor::service(&call.service),
        (None, Some(NamedPrincipal::ApiKey(key))) => {
            partner(key).unwrap_or_else(|| Actor::of(user))
        }
        (None, _) => Actor::of(user),
    }
}

fn audit_record(
    service: &RegisteredService,
    handler: &str,
    auth_user: &Option<crate::auth::AuthUser>,
    actor: Actor,
    request_id: Option<&str>,
    data: Value,
) -> AuditRecord {
    let (user_id, user_name) = crate::audit::named(auth_user);
    AuditRecord {
        subject: crate::audit::subject_of(auth_user),
        timestamp: chrono::Utc::now(),
        tenant: service.tenant_slug.clone(),
        service: service.name.clone(),
        table: None,
        action: format!("custom:{handler}"),
        user_id,
        user_name,
        record_id: None,
        data: Some(data),
        previous: None,
        before: None,
        scope: None,
        actor,
        request_id: request_id.map(str::to_string),
        redacted: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::ActorKind;
    use axum::http::StatusCode;

    #[test]
    fn a_call_is_recorded_as_made_by_whoever_the_route_admitted() {
        let nobody = None;
        let caller = ServiceCaller {
            service: "notes".into(),
            on_behalf_of: Some("user-1".into()),
            depth: 1,
            remaining: Duration::from_secs(5),
        };
        let key = uuid::Uuid::from_u128(3);
        let partner = NamedPrincipal::ApiKey(json!({"id": key, "name": "acme", "scopes": []}));
        let scoped = NamedPrincipal::Scope(json!({"kind": "all"}));
        assert_eq!(
            actor(Some(&caller), &nobody, Some(&partner)),
            Actor::service("notes")
        );
        assert_eq!(
            actor(None, &nobody, Some(&partner)),
            Actor::partner(key, "acme")
        );
        for unnamed in [
            None,
            Some(&scoped),
            Some(&NamedPrincipal::ApiKey(json!({"name": "no id"}))),
        ] {
            assert_eq!(actor(None, &nobody, unnamed).kind, ActorKind::Anonymous);
        }
    }

    #[test]
    fn the_record_of_a_call_is_what_it_always_was_and_who_called() {
        let service = RegisteredService {
            service_id: uuid::Uuid::nil(),
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            name: "books".into(),
            manifest: serde_json::from_value(json!({"name": "books", "tables": []}))
                .expect("manifest"),
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        };
        let record = audit_record(
            &service,
            "export",
            &None,
            Actor::partner(uuid::Uuid::from_u128(3), "acme"),
            Some("req-1"),
            json!({"redacted": "unauthenticated request body not recorded"}),
        );
        let legacy = record.legacy_payload();
        let keys: Vec<&str> = legacy
            .as_object()
            .expect("an object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            [
                "action",
                "data",
                "previous",
                "record_id",
                "service",
                "table",
                "tenant",
                "timestamp",
                "user_id",
                "user_name"
            ]
        );
        assert_eq!(
            (
                &legacy["action"],
                &legacy["table"],
                &legacy["record_id"],
                &legacy["user_id"]
            ),
            (
                &json!("custom:export"),
                &Value::Null,
                &Value::Null,
                &json!("anonymous")
            )
        );
        let durable = record.durable_payload();
        assert_eq!(durable["actor"]["kind"], "partner_key");
        assert_eq!(durable["request_id"], "req-1");
        assert_eq!(durable["data"], legacy["data"]);
    }

    fn call(strict: Option<StrictTarget>, route: usize) -> CustomCall {
        let manifest: cufflink_types::ServiceManifest = serde_json::from_value(serde_json::json!({
            "name": "books",
            "tables": [],
            "custom_routes": [{"method": "GET", "path": "/books/{book_id}", "handler": "get_book"}],
        }))
        .expect("manifest");
        CustomCall {
            service: Arc::new(RegisteredService {
                service_id: uuid::Uuid::nil(),
                tenant_id: uuid::Uuid::nil(),
                tenant_slug: "acme".into(),
                name: "books".into(),
                manifest,
                version: 1,
                wasm_hash: None,
                commit_hash: None,
            }),
            route,
            strict,
            method: Method::GET,
            uri: Uri::from_static("/svc/acme/books/books/7"),
            query: HashMap::new(),
            headers: HeaderMap::new(),
            body: Body::empty(),
            peer: None,
        }
    }

    #[test]
    fn a_strict_head_runs_the_get_it_is() {
        assert_eq!(guest_method(&Method::HEAD, true), "GET");
        assert_eq!(guest_method(&Method::HEAD, false), "HEAD");
        assert_eq!(guest_method(&Method::POST, true), "POST");
    }

    #[test]
    fn routes_are_measured_by_their_declaration_never_the_request_path() {
        let strict = StrictTarget {
            path: "/books/7".into(),
            params: vec![("book_id".into(), "7".into())],
        };
        assert_eq!(
            route_label(&call(Some(strict), 0)).as_deref(),
            Some("/books/{book_id}")
        );
        assert_eq!(route_label(&call(None, 0)).as_deref(), Some("get_book"));
        assert_eq!(route_label(&call(None, 3)), None);
        let route = |path: &str| CustomRouteDefinition {
            path: path.into(),
            handler: "get_book".into(),
            ..Default::default()
        };
        assert_eq!(route_leaf(&route("/books/{book_id}"), false), "get_book");
        for (path, leaf) in [
            ("/books/{book_id}", "books"),
            ("/books", "books"),
            ("/{book_id}/close", ":id"),
            ("/", ":id"),
        ] {
            assert_eq!(route_leaf(&route(path), true), leaf, "{path}");
        }
        assert_eq!(
            [
                StatusCode::SWITCHING_PROTOCOLS,
                StatusCode::NO_CONTENT,
                StatusCode::SEE_OTHER,
                StatusCode::NOT_FOUND,
                StatusCode::BAD_GATEWAY
            ]
            .map(status_class),
            ["1xx", "2xx", "3xx", "4xx", "5xx"]
        );
    }
}
