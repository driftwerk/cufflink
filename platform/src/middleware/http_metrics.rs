use crate::telemetry::{resolving, span_name_for_path};
use axum::{
    extract::Request,
    http::{Method, StatusCode},
    middleware::Next,
    response::Response,
};
use cufflink_telemetry::GaugeGuard;
use std::time::Instant;

pub async fn record(req: Request, next: Next) -> Response {
    let method = method_label(req.method());
    let unresolved = span_name_for_path(req.method().as_str(), req.uri().path());
    let started = Instant::now();
    let _in_flight = GaugeGuard::enter(metrics::gauge!("cufflink_http_requests_in_flight"));
    let (response, resolved) = resolving(next.run(req)).await;
    let route = resolved.unwrap_or(unresolved);
    metrics::histogram!(
        "cufflink_http_request_duration_seconds",
        "method" => method,
        "route" => route,
        "status_class" => status_class(response.status()),
    )
    .record(started.elapsed().as_secs_f64());
    response
}

fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::PATCH => "PATCH",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::CONNECT => "CONNECT",
        Method::TRACE => "TRACE",
        _ => "other",
    }
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn measured(app: axum::Router, path: &str) -> (String, String) {
        use tower::ServiceExt;
        let recorder = cufflink_telemetry::metrics_builder()
            .expect("builder")
            .build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime")
                .block_on(
                    app.layer(axum::middleware::from_fn(record)).oneshot(
                        Request::get(path)
                            .body(axum::body::Body::empty())
                            .expect("request"),
                    ),
                )
                .expect("answered");
        });
        let rendered = handle.render();
        let label = |name: &str| {
            rendered
                .lines()
                .find(|line| line.starts_with("cufflink_http_request_duration_seconds_count"))
                .and_then(|line| line.split_once(&format!("{name}=\"")))
                .and_then(|(_, rest)| rest.split_once('"'))
                .map(|(value, _)| value.to_string())
                .unwrap_or_else(|| panic!("no {name} label in {rendered}"))
        };
        (label("route"), label("status_class"))
    }

    fn resolved(status: StatusCode) -> axum::Router {
        axum::Router::new().route(
            "/svc/{tenant}/{service}/{table}",
            axum::routing::get(move || async move {
                crate::telemetry::resolve(
                    &cufflink_telemetry::Scope::service("acme", "notes", 3),
                    "notes",
                );
                status
            }),
        )
    }

    #[test]
    fn a_request_is_labelled_by_the_route_its_handler_resolved() {
        for (status, class) in [
            (StatusCode::OK, "2xx"),
            (StatusCode::NOT_FOUND, "4xx"),
            (StatusCode::INTERNAL_SERVER_ERROR, "5xx"),
        ] {
            assert_eq!(
                measured(resolved(status), "/svc/acme/notes/notes"),
                ("acme/notes/notes".to_string(), class.to_string())
            );
        }
    }

    #[test]
    fn an_answer_given_before_a_handler_resolved_is_unmatched_with_its_status() {
        let limited = resolved(StatusCode::OK).layer(axum::middleware::from_fn(
            |_: Request, _: Next| async { StatusCode::TOO_MANY_REQUESTS },
        ));
        assert_eq!(
            measured(limited, "/svc/acme/notes/notes"),
            ("GET unmatched".to_string(), "4xx".to_string())
        );
        assert_eq!(
            measured(resolved(StatusCode::OK), "/svc/acme/zzz-9f3c"),
            ("GET unmatched".to_string(), "4xx".to_string())
        );
        assert_eq!(
            measured(resolved(StatusCode::OK), "/api/services"),
            ("GET /api/services".to_string(), "4xx".to_string())
        );
    }

    #[test]
    fn status_classes_cover_every_range() {
        assert_eq!(status_class(StatusCode::CONTINUE), "1xx");
        assert_eq!(status_class(StatusCode::OK), "2xx");
        assert_eq!(status_class(StatusCode::NOT_MODIFIED), "3xx");
        assert_eq!(status_class(StatusCode::NOT_FOUND), "4xx");
        assert_eq!(status_class(StatusCode::BAD_GATEWAY), "5xx");
    }

    #[test]
    fn method_labels_are_bounded_to_the_standard_set() {
        assert_eq!(method_label(&Method::GET), "GET");
        assert_eq!(method_label(&Method::PATCH), "PATCH");
        assert_eq!(method_label(&Method::TRACE), "TRACE");
        let extension = Method::from_bytes(b"PROPFIND").expect("extension method");
        assert_eq!(method_label(&extension), "other");
        let junk = Method::from_bytes(b"X-ANYTHING-GOES").expect("extension method");
        assert_eq!(method_label(&junk), "other");
    }
}
