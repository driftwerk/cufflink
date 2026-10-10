use crate::auth::runner_keys::RunnerAuth;
use crate::errors::{PlatformError, PlatformResult};
use crate::repo::runner_services::{self, Current, Runs};
use crate::AppState;
use axum::body::Body;
use axum::extract::{MatchedPath, Path, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use cufflink_types::runner::{
    is_sha256, Environment, NativeService, NativeSet, WebApp, WebSet, RUNNER_NATIVE_PATH,
    RUNNER_WEB_PATH,
};
use cufflink_types::ProvidedExtension;
use std::sync::Arc;
use uuid::Uuid;

const ARTIFACT: &str = "/{service_id}/{sha256}";
const ENVIRONMENT: &str = "/{service_id}/environment";

impl Runs {
    fn path(self) -> &'static str {
        match self {
            Runs::Native => RUNNER_NATIVE_PATH,
            Runs::Web => RUNNER_WEB_PATH,
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Runs::Native => "application/octet-stream",
            Runs::Web => "application/gzip",
        }
    }
}

fn endpoint(route: &str) -> &'static str {
    match (
        route.strip_prefix(RUNNER_NATIVE_PATH),
        route.strip_prefix(RUNNER_WEB_PATH),
    ) {
        (Some(""), _) => "native",
        (Some(ARTIFACT), _) => "binary",
        (Some(ENVIRONMENT), _) => "environment",
        (_, Some("")) => "web",
        (_, Some(ARTIFACT)) => "bundle",
        (_, Some(ENVIRONMENT)) => "web_environment",
        _ => "unmatched",
    }
}

fn result(status: StatusCode) -> &'static str {
    match status {
        status if status.is_success() => "ok",
        StatusCode::UNAUTHORIZED => "unauthorized",
        StatusCode::NOT_FOUND => "not_found",
        status if status.is_client_error() => "invalid",
        _ => "error",
    }
}

async fn counted(route: MatchedPath, request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    metrics::counter!(
        "cufflink_runner_api_requests_total",
        "endpoint" => endpoint(route.as_str()),
        "result" => result(response.status()),
    )
    .increment(1);
    response
}

pub(crate) fn routes() -> Router<Arc<AppState>> {
    [Runs::Native, Runs::Web]
        .into_iter()
        .fold(Router::new(), |router, runs| {
            let path = runs.path();
            router
                .route(
                    path,
                    match runs {
                        Runs::Native => get(native),
                        Runs::Web => get(web),
                    },
                )
                .route(
                    &format!("{path}{ARTIFACT}"),
                    get(move |state, runner, target| artifact(state, runner, target, runs)),
                )
                .route(
                    &format!("{path}{ENVIRONMENT}"),
                    get(move |state, runner, service| environment(state, runner, service, runs)),
                )
        })
        .route_layer(axum::middleware::from_fn(counted))
}

fn not_found() -> PlatformError {
    PlatformError::NotFound("Not found".into())
}

fn running(runner: RunnerAuth, runs: Runs) -> PlatformResult<RunnerAuth> {
    match runner.runs == runs {
        true => Ok(runner),
        false => Err(not_found()),
    }
}

fn size(service: &Current) -> Option<u64> {
    service.size.and_then(|size| u64::try_from(size).ok())
}

fn provided(service: &Current) -> Vec<String> {
    service
        .provides
        .clone()
        .map(serde_json::from_value::<Vec<ProvidedExtension>>)
        .and_then(Result::ok)
        .map(|provides| provides.into_iter().map(|provided| provided.name).collect())
        .unwrap_or_default()
}

fn native_service(service: Current) -> NativeService {
    NativeService {
        extensions: provided(&service),
        size: size(&service),
        service_id: service.service_id,
        service_name: service.service_name,
        version: service.version,
        sha256: service.sha256,
    }
}

fn web_app(service: Current) -> WebApp {
    WebApp {
        size: size(&service),
        service_id: service.service_id,
        service_name: service.service_name,
        version: service.version,
        sha256: service.sha256,
    }
}

pub async fn native(
    State(state): State<Arc<AppState>>,
    runner: RunnerAuth,
) -> PlatformResult<Json<NativeSet>> {
    let runner = running(runner, Runs::Native)?;
    let services =
        runner_services::current(&state.platform_db, runner.tenant_id, Runs::Native).await?;
    Ok(Json(NativeSet {
        tenant: runner.tenant_slug,
        services: services.into_iter().map(native_service).collect(),
    }))
}

pub async fn web(
    State(state): State<Arc<AppState>>,
    runner: RunnerAuth,
) -> PlatformResult<Json<WebSet>> {
    let runner = running(runner, Runs::Web)?;
    let apps = runner_services::current(&state.platform_db, runner.tenant_id, Runs::Web).await?;
    Ok(Json(WebSet {
        tenant: runner.tenant_slug,
        apps: apps.into_iter().map(web_app).collect(),
    }))
}

async fn artifact(
    State(state): State<Arc<AppState>>,
    runner: RunnerAuth,
    Path((service_id, sha256)): Path<(Uuid, String)>,
    runs: Runs,
) -> PlatformResult<Response> {
    let runner = running(runner, runs)?;
    if !is_sha256(&sha256) {
        return Err(not_found());
    }
    let (key, size) = runner_services::artifact(
        &state.platform_db,
        runner.tenant_id,
        service_id,
        &sha256,
        runs,
    )
    .await?
    .ok_or_else(not_found)?;
    let storage = state.storage.as_ref().ok_or_else(|| {
        PlatformError::Internal(
            "the runner API serves artifacts from object storage, which this platform does not \
             have"
                .into(),
        )
    })?;
    let object = storage
        .open_read(storage.primary_bucket(), &key)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to read {key}: {e}")))?
        .ok_or_else(|| {
            PlatformError::Internal(format!("the artifact {key} is missing from storage"))
        })?;
    let length = object
        .size
        .or_else(|| u64::try_from(size).ok())
        .ok_or_else(|| PlatformError::Internal(format!("{key} has no size")))?;
    Ok((
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(runs.content_type()),
            ),
            (header::CONTENT_LENGTH, HeaderValue::from(length)),
        ],
        Body::from_stream(object.stream),
    )
        .into_response())
}

async fn environment(
    State(state): State<Arc<AppState>>,
    runner: RunnerAuth,
    Path(service_id): Path<Uuid>,
    runs: Runs,
) -> PlatformResult<Response> {
    let runner = running(runner, runs)?;
    if !runner_services::runs(&state.platform_db, runner.tenant_id, service_id, runs).await? {
        return Err(not_found());
    }
    let client = state.platform_db.get().await?;
    let environment =
        crate::secrets::configs::environment(&state.secrets, &client, service_id).await?;
    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(Environment {
            environment: environment.into_iter().collect(),
        }),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_route_is_counted_under_its_endpoint() {
        for (route, counted) in [
            ("/api/runner/native", "native"),
            ("/api/runner/native/{service_id}/{sha256}", "binary"),
            ("/api/runner/native/{service_id}/environment", "environment"),
            ("/api/runner/web", "web"),
            ("/api/runner/web/{service_id}/{sha256}", "bundle"),
            (
                "/api/runner/web/{service_id}/environment",
                "web_environment",
            ),
            ("/api/runner/natives", "unmatched"),
            ("/api/services", "unmatched"),
        ] {
            assert_eq!(endpoint(route), counted, "{route}");
        }
        for (status, counted) in [
            (StatusCode::OK, "ok"),
            (StatusCode::UNAUTHORIZED, "unauthorized"),
            (StatusCode::NOT_FOUND, "not_found"),
            (StatusCode::BAD_REQUEST, "invalid"),
            (StatusCode::INTERNAL_SERVER_ERROR, "error"),
            (StatusCode::SERVICE_UNAVAILABLE, "error"),
        ] {
            assert_eq!(result(status), counted, "{status}");
        }
    }

    fn current() -> Current {
        Current {
            service_id: Uuid::nil(),
            service_name: "ext-pdf".into(),
            version: 4,
            sha256: Some("ab".into()),
            size: Some(9),
            provides: Some(json!([
                {"name": "pdf.render", "callers": ["*"]},
                {"name": "pdf.merge", "callers": ["reports"]}
            ])),
        }
    }

    #[test]
    fn a_current_native_deployment_is_answered_with_what_it_provides() {
        assert_eq!(
            native_service(current()),
            NativeService {
                service_id: Uuid::nil(),
                service_name: "ext-pdf".into(),
                version: 4,
                sha256: Some("ab".into()),
                size: Some(9),
                extensions: vec!["pdf.render".into(), "pdf.merge".into()],
            }
        );
        for provides in [None, Some(json!({"name": "pdf.render"}))] {
            assert_eq!(
                native_service(Current {
                    provides,
                    ..current()
                })
                .extensions,
                Vec::<String>::new()
            );
        }
        assert_eq!(
            native_service(Current {
                sha256: None,
                size: None,
                ..current()
            })
            .size,
            None
        );
    }

    #[test]
    fn a_key_reaches_the_routes_of_its_own_kind_of_runner_alone() {
        let runner = |runs| RunnerAuth {
            key_id: Uuid::nil(),
            tenant_id: Uuid::nil(),
            tenant_slug: "acme".into(),
            runs,
        };
        for runs in Runs::ALL {
            assert_eq!(running(runner(runs), runs).ok(), Some(runner(runs)));
        }
        for (key, route) in [(Runs::Native, Runs::Web), (Runs::Web, Runs::Native)] {
            assert!(matches!(
                running(runner(key), route),
                Err(PlatformError::NotFound(_))
            ));
        }
    }

    #[test]
    fn a_current_web_deployment_is_answered_with_its_bundle() {
        assert_eq!(
            web_app(Current {
                service_name: "portal".into(),
                provides: None,
                ..current()
            }),
            WebApp {
                service_id: Uuid::nil(),
                service_name: "portal".into(),
                version: 4,
                sha256: Some("ab".into()),
                size: Some(9),
            }
        );
    }
}
