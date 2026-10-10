use axum::http::Method;
use cufflink_types::route_path::{RoutePathError, RoutePattern};
use cufflink_types::CustomRouteDefinition;
use percent_encoding::percent_decode_str;
use std::collections::{HashMap, HashSet};
use std::fmt;

const ROUTABLE_METHODS: [Method; 5] = [
    Method::GET,
    Method::POST,
    Method::PUT,
    Method::PATCH,
    Method::DELETE,
];

pub(crate) struct RouteTable {
    by_method: HashMap<Method, matchit::Router<usize>>,
    streams: HashSet<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RouteMatch {
    pub route: usize,
    pub params: Vec<(String, String)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MatchOutcome {
    Matched(RouteMatch),
    Undecodable { param: String },
    MethodNotAllowed { allow: Vec<Method> },
    NoRoute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RouteTableError {
    Method {
        handler: String,
        method: String,
    },
    Path {
        handler: String,
        error: RoutePathError,
    },
    Conflict {
        handler: String,
        route: String,
        message: String,
    },
}

impl fmt::Display for RouteTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Method { handler, method } => write!(
                f,
                "route '{handler}': method '{method}' is not one of GET, POST, PUT, PATCH, DELETE"
            ),
            Self::Path { handler, error } => write!(f, "route '{handler}': {error}"),
            Self::Conflict {
                handler,
                route,
                message,
            } => write!(f, "route '{handler}' ({route}) conflicts: {message}"),
        }
    }
}

impl std::error::Error for RouteTableError {}

impl RouteTable {
    pub(crate) fn build(routes: &[CustomRouteDefinition]) -> Result<Self, RouteTableError> {
        routes
            .iter()
            .enumerate()
            .try_fold(HashMap::new(), |mut by_method, (index, route)| {
                let method = routable_method(route)?;
                let pattern =
                    RoutePattern::parse(&route.path).map_err(|error| RouteTableError::Path {
                        handler: route.handler.clone(),
                        error,
                    })?;
                by_method
                    .entry(method)
                    .or_insert_with(matchit::Router::new)
                    .insert(pattern.to_matchit(), index)
                    .map_err(|e| RouteTableError::Conflict {
                        handler: route.handler.clone(),
                        route: format!("{} {}", route.method, route.path),
                        message: e.to_string(),
                    })?;
                Ok(by_method)
            })
            .map(|by_method| Self {
                by_method,
                streams: routes
                    .iter()
                    .enumerate()
                    .filter(|(_, route)| route.sse.is_some())
                    .map(|(index, _)| index)
                    .collect(),
            })
    }

    pub(crate) fn match_route(&self, method: &Method, path: &str) -> MatchOutcome {
        let head = method == Method::HEAD;
        let method = if head { &Method::GET } else { method };
        let found = self
            .by_method
            .get(method)
            .and_then(|r| r.at(path).ok())
            .filter(|found| !(head && self.streams.contains(found.value)));
        match found {
            Some(found) => decode_params(found.params.iter())
                .map(|params| {
                    MatchOutcome::Matched(RouteMatch {
                        route: *found.value,
                        params,
                    })
                })
                .unwrap_or_else(|param| MatchOutcome::Undecodable { param }),
            None => {
                let allow = self.allowed(path);
                if allow.is_empty() {
                    MatchOutcome::NoRoute
                } else {
                    MatchOutcome::MethodNotAllowed { allow }
                }
            }
        }
    }

    fn allowed(&self, path: &str) -> Vec<Method> {
        ROUTABLE_METHODS
            .iter()
            .filter_map(|method| {
                self.by_method
                    .get(method)
                    .and_then(|router| router.at(path).ok())
                    .map(|found| (method, *found.value))
            })
            .flat_map(|(method, route)| {
                let head = (method == Method::GET && !self.streams.contains(&route))
                    .then_some(Method::HEAD);
                std::iter::once(method.clone()).chain(head)
            })
            .collect()
    }
}

fn routable_method(route: &CustomRouteDefinition) -> Result<Method, RouteTableError> {
    ROUTABLE_METHODS
        .iter()
        .find(|method| method.as_str() == route.method)
        .cloned()
        .ok_or_else(|| RouteTableError::Method {
            handler: route.handler.clone(),
            method: route.method.clone(),
        })
}

fn decode_params<'a>(
    params: impl Iterator<Item = (&'a str, &'a str)>,
) -> Result<Vec<(String, String)>, String> {
    params
        .map(|(name, raw)| {
            percent_decode_str(raw)
                .decode_utf8()
                .map(|value| (name.to_string(), value.into_owned()))
                .map_err(|_| name.to_string())
        })
        .collect()
}

pub(crate) fn allow_header(allow: &[Method]) -> String {
    allow
        .iter()
        .map(Method::as_str)
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(method: &str, path: &str, handler: &str) -> CustomRouteDefinition {
        CustomRouteDefinition {
            method: method.into(),
            path: path.into(),
            handler: handler.into(),
            job: None,
            posture: None,
            forward_auth: false,
            ..Default::default()
        }
    }

    fn table(routes: &[(&str, &str)]) -> RouteTable {
        let routes: Vec<_> = routes
            .iter()
            .enumerate()
            .map(|(i, (method, path))| route(method, path, &format!("h{i}")))
            .collect();
        RouteTable::build(&routes).expect("builds")
    }

    fn matched(route: usize, params: &[(&str, &str)]) -> MatchOutcome {
        MatchOutcome::Matched(RouteMatch {
            route,
            params: params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        })
    }

    #[test]
    fn matches_literals_params_and_catch_alls_per_method() {
        let t = table(&[
            ("GET", "/books/reading-lists"),
            ("GET", "/books/reading-lists/{list_id}"),
            ("PUT", "/books/reading-lists/{list_id}"),
            ("GET", "/books/reading-lists/{list_id}/members/{patron_id}"),
            ("PUT", "/books/:book_id/due-date"),
            ("GET", "/files/{*rest}"),
            ("GET", "/"),
        ]);
        for (method, path, outcome) in [
            ("GET", "/books/reading-lists", matched(0, &[])),
            (
                "GET",
                "/books/reading-lists/p1",
                matched(1, &[("list_id", "p1")]),
            ),
            (
                "PUT",
                "/books/reading-lists/p1",
                matched(2, &[("list_id", "p1")]),
            ),
            (
                "GET",
                "/books/reading-lists/p1/members/u%20x",
                matched(3, &[("list_id", "p1"), ("patron_id", "u x")]),
            ),
            ("PUT", "/books/7/due-date", matched(4, &[("book_id", "7")])),
            (
                "GET",
                "/files/a/b/c.pdf",
                matched(5, &[("rest", "a/b/c.pdf")]),
            ),
            ("HEAD", "/books/reading-lists", matched(0, &[])),
            ("GET", "/", matched(6, &[])),
            ("GET", "/books/reading-lists/", MatchOutcome::NoRoute),
            ("GET", "/books", MatchOutcome::NoRoute),
            (
                "GET",
                "/books/reading-lists/%FF",
                MatchOutcome::Undecodable {
                    param: "list_id".into(),
                },
            ),
            (
                "DELETE",
                "/books/reading-lists/p1",
                MatchOutcome::MethodNotAllowed {
                    allow: vec![Method::GET, Method::HEAD, Method::PUT],
                },
            ),
            (
                "POST",
                "/books/7/due-date",
                MatchOutcome::MethodNotAllowed {
                    allow: vec![Method::PUT],
                },
            ),
        ] {
            assert_eq!(
                t.match_route(
                    &Method::from_bytes(method.as_bytes()).expect("method"),
                    path
                ),
                outcome,
                "{method} {path}"
            );
        }
    }

    #[test]
    fn a_head_never_opens_an_event_stream() {
        let mut routes = vec![
            route("GET", "/progress/{id}", "progress"),
            route("POST", "/progress/{id}", "reset"),
        ];
        routes[0].sse = Some(cufflink_types::SseRouteDef {
            subject: "dw.{tenant}.progress.{id}".into(),
            reducer: None,
            anonymous: false,
        });
        let t = RouteTable::build(&routes).expect("builds");
        assert_eq!(
            t.match_route(&Method::GET, "/progress/7"),
            matched(0, &[("id", "7")])
        );
        let refused = MatchOutcome::MethodNotAllowed {
            allow: vec![Method::GET, Method::POST],
        };
        assert_eq!(t.match_route(&Method::HEAD, "/progress/7"), refused);
        assert_eq!(t.match_route(&Method::DELETE, "/progress/7"), refused);
    }

    #[test]
    fn build_rejects_conflicts_bad_methods_and_bad_paths() {
        let conflict =
            RouteTable::build(&[route("GET", "/a/{x}", "one"), route("GET", "/a/{y}", "two")])
                .err()
                .expect("conflict");
        assert!(
            matches!(&conflict, RouteTableError::Conflict { handler, .. } if handler == "two"),
            "{conflict}"
        );
        let duplicate = RouteTable::build(&[route("GET", "/a", "one"), route("GET", "/a", "two")]);
        assert!(matches!(duplicate, Err(RouteTableError::Conflict { .. })));
        assert!(RouteTable::build(&[
            route("GET", "/a/{x}", "one"),
            route("POST", "/a/{y}", "two")
        ])
        .is_ok());
        assert_eq!(
            RouteTable::build(&[route("get", "/a", "lower")]).err(),
            Some(RouteTableError::Method {
                handler: "lower".into(),
                method: "get".into(),
            })
        );
        assert!(RouteTable::build(&[route("OPTIONS", "/a", "o")]).is_err());
        assert_eq!(
            RouteTable::build(&[route("GET", "/_meta/x", "m")]).err(),
            Some(RouteTableError::Path {
                handler: "m".into(),
                error: RoutePathError::ReservedPrefix("_meta".into()),
            })
        );
    }

    const LIBRARY_ROUTES: &[(&str, &str)] = &[
        ("GET", "/books"),
        ("POST", "/books"),
        ("GET", "/books/search"),
        ("GET", "/books/overdue"),
        ("POST", "/books/import"),
        ("GET", "/books/import/{import_id}"),
        ("GET", "/books/{book_id}"),
        ("PUT", "/books/{book_id}"),
        ("PATCH", "/books/{book_id}"),
        ("DELETE", "/books/{book_id}"),
        ("GET", "/books/{book_id}/copies"),
        ("POST", "/books/{book_id}/copies"),
        ("GET", "/books/{book_id}/copies/available"),
        ("GET", "/books/{book_id}/copies/{copy_id}"),
        ("DELETE", "/books/{book_id}/copies/{copy_id}"),
        ("PUT", "/books/{book_id}/copies/{copy_id}/shelf"),
        ("POST", "/books/{book_id}/copies/{copy_id}/withdraw"),
        ("GET", "/books/{book_id}/holds"),
        ("POST", "/books/{book_id}/holds"),
        ("DELETE", "/books/{book_id}/holds/{hold_id}"),
        ("PUT", "/books/{book_id}/cover"),
        ("GET", "/books/{book_id}/history"),
        ("GET", "/loans"),
        ("POST", "/loans"),
        ("GET", "/loans/due-today"),
        ("GET", "/loans/overdue"),
        ("POST", "/loans/overdue/remind"),
        ("GET", "/loans/{loan_id}"),
        ("POST", "/loans/{loan_id}/renew"),
        ("POST", "/loans/{loan_id}/return"),
        ("GET", "/loans/{loan_id}/receipt"),
        ("PUT", "/loans/{loan_id}/memo"),
        ("GET", "/patrons"),
        ("POST", "/patrons"),
        ("GET", "/patrons/me"),
        ("GET", "/patrons/me/loans"),
        ("GET", "/patrons/{patron_id}"),
        ("PATCH", "/patrons/{patron_id}"),
        ("GET", "/patrons/{patron_id}/loans"),
        ("GET", "/patrons/{patron_id}/loans/{loan_id}"),
        ("GET", "/patrons/{patron_id}/fines"),
        ("POST", "/patrons/{patron_id}/fines/{fine_id}/waive"),
        ("GET", "/branches"),
        ("GET", "/branches/{branch_id}"),
        ("GET", "/branches/{branch_id}/shelves"),
        ("GET", "/branches/{branch_id}/shelves/{shelf_id}"),
        ("PUT", "/branches/{branch_id}/shelves/{shelf_id}"),
        ("GET", "/branches/{branch_id}/shelves/{shelf_id}/books"),
        ("POST", "/branches/{branch_id}/transfers"),
        ("GET", "/branches/{branch_id}/transfers/{transfer_id}"),
        (
            "POST",
            "/branches/{branch_id}/transfers/{transfer_id}/receive",
        ),
        ("GET", "/reports/circulation"),
        ("GET", "/reports/circulation/{year}"),
        ("GET", "/reports/circulation/{year}/{month}"),
    ];

    #[test]
    fn a_services_routes_build_one_table_and_each_matches_its_own_url() {
        let routes = LIBRARY_ROUTES;
        let definitions: Vec<CustomRouteDefinition> = routes
            .iter()
            .enumerate()
            .map(|(i, (method, path))| route(method, path, &format!("h{i}")))
            .collect();
        let table = RouteTable::build(&definitions).expect("the routes do not conflict");
        for (index, (method, path)) in routes.iter().enumerate() {
            let url = path
                .split('/')
                .map(|segment| {
                    if segment.starts_with('{') {
                        "x1"
                    } else {
                        segment
                    }
                })
                .collect::<Vec<_>>()
                .join("/");
            let method = Method::from_bytes(method.as_bytes()).expect("method");
            assert!(
                matches!(table.match_route(&method, &url), MatchOutcome::Matched(m) if m.route == index),
                "{method} {url}"
            );
        }
    }

    #[test]
    fn allow_header_lists_methods_in_order() {
        assert_eq!(
            allow_header(&[Method::GET, Method::HEAD, Method::DELETE]),
            "GET,HEAD,DELETE"
        );
    }
}
