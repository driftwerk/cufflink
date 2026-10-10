mod table;

pub(crate) use table::{RouteMatch, RouteTable, RouteTableError};

use crate::registry::RegisteredService;
use crate::router::custom::{self, StrictTarget};
use crate::AppState;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use cufflink_types::route_path::RESERVED_FIRST_SEGMENTS;
use cufflink_types::ServiceManifest;
use std::sync::Arc;
use table::{allow_header, MatchOutcome};

enum Resolution {
    Pass(Vec<Method>),
    Dispatch(Arc<RegisteredService>, usize, StrictTarget),
    Reject(Response),
}

pub(crate) async fn route(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    match resolve(&state, request.method(), request.uri().path()).await {
        Resolution::Pass(allow) => also_allowing(next.run(request).await, &allow),
        Resolution::Dispatch(service, route, target) => {
            let head = request.method() == Method::HEAD;
            let response =
                custom::strict_route_handler(state, service, route, target, request).await;
            if head {
                response.map(|_| Body::empty())
            } else {
                response
            }
        }
        Resolution::Reject(response) => response,
    }
}

async fn resolve(state: &AppState, method: &Method, path: &str) -> Resolution {
    let Some((tenant, service, rest)) = split_svc_path(path) else {
        return Resolution::Pass(Vec::new());
    };
    let Some((service, routes)) = state.registry.strict_routes(tenant, service).await else {
        return Resolution::Pass(Vec::new());
    };
    if first_segment(rest).is_some_and(|first| RESERVED_FIRST_SEGMENTS.contains(&first)) {
        return counted("passthrough", Resolution::Pass(Vec::new()));
    }
    match routes.match_route(method, rest) {
        MatchOutcome::Matched(RouteMatch { route, params }) => counted(
            "matched",
            Resolution::Dispatch(
                service,
                route,
                StrictTarget {
                    path: rest.to_string(),
                    params,
                },
            ),
        ),
        MatchOutcome::MethodNotAllowed { allow } if is_crud_shape(&service.manifest, rest) => {
            counted("passthrough", Resolution::Pass(allow))
        }
        _ if is_crud_shape(&service.manifest, rest) => {
            counted("passthrough", Resolution::Pass(Vec::new()))
        }
        MatchOutcome::Undecodable { param } => counted(
            "no_route",
            Resolution::Reject(
                crate::errors::PlatformError::BadRequest(format!(
                    "path parameter '{param}' is not valid UTF-8"
                ))
                .into_response(),
            ),
        ),
        MatchOutcome::MethodNotAllowed { allow } => counted(
            "method_not_allowed",
            Resolution::Reject(method_not_allowed(&allow)),
        ),
        MatchOutcome::NoRoute => counted(
            "no_route",
            Resolution::Reject(
                crate::errors::PlatformError::NotFound("Not found".into()).into_response(),
            ),
        ),
    }
}

fn counted(result: &'static str, resolution: Resolution) -> Resolution {
    metrics::counter!("cufflink_strict_route_matches_total", "result" => result).increment(1);
    resolution
}

fn method_not_allowed(allow: &[Method]) -> Response {
    let mut response = StatusCode::METHOD_NOT_ALLOWED.into_response();
    if let Ok(value) = HeaderValue::from_str(&allow_header(allow)) {
        response.headers_mut().insert(header::ALLOW, value);
    }
    response
}

fn also_allowing(mut response: Response, strict: &[Method]) -> Response {
    if response.status() != StatusCode::METHOD_NOT_ALLOWED || strict.is_empty() {
        return response;
    }
    let served: Vec<Method> = response
        .headers()
        .get(header::ALLOW)
        .and_then(|value| value.to_str().ok())
        .into_iter()
        .flat_map(|value| value.split(','))
        .filter_map(|method| Method::from_bytes(method.trim().as_bytes()).ok())
        .collect();
    let merged: Vec<Method> = strict.iter().fold(served, |mut merged, method| {
        if !merged.contains(method) {
            merged.push(method.clone());
        }
        merged
    });
    if let Ok(value) = HeaderValue::from_str(&allow_header(&merged)) {
        response.headers_mut().insert(header::ALLOW, value);
    }
    response
}

pub(crate) fn split_svc_path(path: &str) -> Option<(&str, &str, &str)> {
    let below = path.strip_prefix("/svc/")?;
    let (tenant, below) = below.split_once('/')?;
    let (service, rest) = below.find('/').map_or((below, ""), |at| below.split_at(at));
    (!tenant.is_empty() && !service.is_empty() && !rest.is_empty())
        .then_some((tenant, service, rest))
}

fn first_segment(rest: &str) -> Option<&str> {
    rest.strip_prefix('/')
        .and_then(|below| below.split('/').next())
}

fn is_crud_shape(manifest: &ServiceManifest, rest: &str) -> bool {
    let segments: Vec<&str> = rest.trim_start_matches('/').split('/').collect();
    (1..=2).contains(&segments.len())
        && segments
            .first()
            .is_some_and(|first| manifest.get_table(first).is_some())
}

const ANY_ID: &str = "%7Bid%7D";

pub(crate) fn crud_shadow<'a>(
    routes: &RouteTable,
    manifest: &'a ServiceManifest,
) -> Option<(usize, &'a str)> {
    manifest.tables.iter().find_map(|table| {
        let collection = format!("/{}", table.name);
        let item = format!("/{}/{ANY_ID}", table.name);
        [
            (Method::GET, collection.clone()),
            (Method::POST, collection),
            (Method::GET, item.clone()),
            (Method::PUT, item.clone()),
            (Method::DELETE, item),
            (Method::GET, format!("/{}/stream", table.name)),
            (Method::POST, format!("/{}/batch", table.name)),
        ]
        .iter()
        .find_map(|(method, path)| match routes.match_route(method, path) {
            MatchOutcome::Matched(m) => Some((m.route, table.name.as_str())),
            _ => None,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::{ColumnDefinition, ColumnType, CustomRouteDefinition, TableDefinition};

    #[test]
    fn splits_svc_paths_and_passes_everything_else() {
        assert_eq!(
            split_svc_path("/svc/acme/books/books/7/close"),
            Some(("acme", "books", "/books/7/close"))
        );
        assert_eq!(
            split_svc_path("/svc/acme/books/"),
            Some(("acme", "books", "/"))
        );
        for path in [
            "/svc/acme/books",
            "/svc/acme",
            "/svc//books/x",
            "/api/version",
            "/svc",
        ] {
            assert_eq!(split_svc_path(path), None, "{path}");
        }
        assert_eq!(first_segment("/_fn/x"), Some("_fn"));
        assert_eq!(first_segment("/"), Some(""));
    }

    #[test]
    fn a_crud_405_also_allows_the_strict_methods_on_its_path() {
        let crud = || {
            let mut response = StatusCode::METHOD_NOT_ALLOWED.into_response();
            response.headers_mut().insert(
                header::ALLOW,
                HeaderValue::from_static("GET,HEAD,PUT,DELETE"),
            );
            response
        };
        let allow = |response: &Response| response.headers()[header::ALLOW].clone();
        assert_eq!(
            allow(&also_allowing(crud(), &[Method::PATCH, Method::GET])),
            "GET,HEAD,PUT,DELETE,PATCH"
        );
        assert_eq!(allow(&also_allowing(crud(), &[])), "GET,HEAD,PUT,DELETE");
        let ok = also_allowing(StatusCode::OK.into_response(), &[Method::PATCH]);
        assert!(ok.headers().get(header::ALLOW).is_none());
    }

    fn manifest(routes: &[(&str, &str)]) -> ServiceManifest {
        ServiceManifest {
            name: "books".into(),
            tables: vec![TableDefinition {
                name: "books".into(),
                columns: vec![ColumnDefinition {
                    name: "id".into(),
                    column_type: ColumnType::Uuid,
                    primary_key: true,
                    nullable: false,
                    auto_generate: true,
                    default_value: None,
                    references: None,
                    on_delete: None,
                    unique: false,
                    validations: vec![],
                    ..Default::default()
                }],
                indexes: vec![],
                soft_delete: false,
                owner_field: None,
                auth_required: false,
                permission_area: None,
                owner_self_service: false,
                hooks: None,
                scoped_by: None,
                ..Default::default()
            }],
            custom_routes: routes
                .iter()
                .map(|(method, path)| CustomRouteDefinition {
                    method: method.to_string(),
                    path: path.to_string(),
                    handler: "h".into(),
                    job: None,
                    posture: None,
                    forward_auth: false,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn crud_shapes_are_one_or_two_segments_under_a_table() {
        let m = manifest(&[]);
        for (rest, crud) in [
            ("/books", true),
            ("/books/7", true),
            ("/books/stream", true),
            ("/books/7/close", false),
            ("/notes", false),
            ("/", false),
        ] {
            assert_eq!(is_crud_shape(&m, rest), crud, "{rest}");
        }
    }

    #[test]
    fn routes_that_take_crud_requests_shadow_them() {
        for (routes, shadow) in [
            (vec![("PUT", "/books/{book_id}/due-date")], None),
            (vec![("GET", "/books/reading-lists")], None),
            (vec![("PATCH", "/books/{id}")], None),
            (vec![("GET", "/books/{id}")], Some(0)),
            (vec![("POST", "/books")], Some(0)),
            (
                vec![("GET", "/other"), ("DELETE", "/{table}/{id}")],
                Some(1),
            ),
            (vec![("GET", "/books/stream")], Some(0)),
            (vec![("GET", "/{*rest}")], Some(0)),
        ] {
            let m = manifest(&routes);
            let table = RouteTable::build(&m.custom_routes).expect("builds");
            assert_eq!(
                crud_shadow(&table, &m).map(|(route, table)| (route, table.to_string())),
                shadow.map(|route| (route, "books".to_string())),
                "{routes:?}"
            );
        }
    }

    #[test]
    fn a_strict_route_may_take_a_private_tables_paths() {
        let routes = [
            ("GET", "/books"),
            ("GET", "/books/{id}"),
            ("POST", "/books"),
        ];
        let served = manifest(&routes);
        let table = RouteTable::build(&served.custom_routes).expect("builds");
        assert_eq!(crud_shadow(&table, &served), Some((0, "books")));

        let private = ServiceManifest {
            private_tables: served.tables.clone(),
            tables: vec![],
            ..served
        };
        assert_eq!(crud_shadow(&table, &private), None);
        for rest in ["/books", "/books/7", "/books/stream", "/books/batch"] {
            assert!(!is_crud_shape(&private, rest), "{rest}");
        }
    }
}
