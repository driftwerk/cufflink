use axum::{
    http::{header::RETRY_AFTER, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use cufflink_types::db::{DbError, DbErrorKind};
use serde_json::json;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrudOp {
    Read,
    Create,
    Update,
    Delete,
    Commit,
}

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Too many requests: {0}")]
    TooManyRequests(String),

    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),

    #[error("Service unavailable: {0}")]
    Unavailable(String),

    #[error("Service unavailable: {message}")]
    RetryLater { message: String, after: Duration },

    #[error("Gateway timeout: {0}")]
    GatewayTimeout(String),

    #[error("Schema migration error: {0}")]
    SchemaMigration(String),

    #[error("Destructive changes: {0}")]
    DestructiveChanges(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),

    #[error("{source}")]
    DatabaseOp {
        op: CrudOp,
        source: tokio_postgres::Error,
    },

    #[error(transparent)]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    Eyre(#[from] eyre::Report),
}

impl From<cufflink_types::openapi::OpenApiError> for PlatformError {
    fn from(error: cufflink_types::openapi::OpenApiError) -> Self {
        PlatformError::Validation(format!("OpenAPI docs: {error}"))
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            PlatformError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            PlatformError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            PlatformError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg.clone()),
            PlatformError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()),
            PlatformError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            PlatformError::Validation(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg.clone()),
            PlatformError::TooManyRequests(msg) => (StatusCode::TOO_MANY_REQUESTS, msg.clone()),
            PlatformError::PayloadTooLarge(msg) => (StatusCode::PAYLOAD_TOO_LARGE, msg.clone()),
            PlatformError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg.clone()),
            PlatformError::RetryLater { message, .. } => {
                (StatusCode::SERVICE_UNAVAILABLE, message.clone())
            }
            PlatformError::GatewayTimeout(msg) => (StatusCode::GATEWAY_TIMEOUT, msg.clone()),
            PlatformError::SchemaMigration(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            PlatformError::DestructiveChanges(msg) => {
                (StatusCode::UNPROCESSABLE_ENTITY, msg.clone())
            }
            PlatformError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
            PlatformError::Database(e) => {
                tracing::error!("Database error: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal database error".into(),
                )
            }
            PlatformError::DatabaseOp { op, source } => {
                let error = crate::db_error::from_pg(source);
                let (status, message) = db_error_response(&error, *op);
                metrics::counter!("cufflink_crud_db_errors_total", "kind" => error.kind.as_str(), "status" => status.as_str().to_string()).increment(1);
                if status.is_server_error() {
                    tracing::error!("Database error: {:?}", source);
                }
                (status, message)
            }
            PlatformError::Pool(e) => {
                tracing::error!("Pool error: {:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Database pool error".into(),
                )
            }
            PlatformError::SerdeJson(e) => (StatusCode::BAD_REQUEST, format!("JSON error: {}", e)),
            PlatformError::Eyre(e) => {
                tracing::error!("Internal error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".into())
            }
        };

        let body = json!({ "error": message });
        let mut response = (status, Json(body)).into_response();
        if let PlatformError::RetryLater { after, .. } = &self {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from(retry_after_secs(*after)));
        }
        response
            .extensions_mut()
            .insert(PlatformErrorBody { status, message });
        response
    }
}

fn retry_after_secs(after: Duration) -> u64 {
    u64::try_from(after.as_millis().div_ceil(1000))
        .unwrap_or(u64::MAX)
        .max(1)
}

pub fn db_error_response(error: &DbError, op: CrudOp) -> (StatusCode, String) {
    let constraint = || error.constraint.as_deref().unwrap_or("unnamed constraint");
    match error.kind {
        DbErrorKind::UniqueViolation | DbErrorKind::ExclusionViolation => {
            (StatusCode::CONFLICT, format!("conflict: {}", constraint()))
        }
        DbErrorKind::ForeignKeyViolation if matches!(op, CrudOp::Create | CrudOp::Update) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("invalid reference ({})", constraint()),
        ),
        DbErrorKind::ForeignKeyViolation => (
            StatusCode::CONFLICT,
            format!("conflict: row is referenced ({})", constraint()),
        ),
        DbErrorKind::CheckViolation | DbErrorKind::NotNullViolation => (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "constraint violated: {}",
                error
                    .constraint
                    .as_deref()
                    .or(error.column.as_deref())
                    .unwrap_or("unnamed constraint")
            ),
        ),
        DbErrorKind::RaiseException => (StatusCode::UNPROCESSABLE_ENTITY, error.message.clone()),
        DbErrorKind::InvalidTextRepresentation
        | DbErrorKind::NumericOutOfRange
        | DbErrorKind::DatetimeFieldOverflow
        | DbErrorKind::DataException => (
            StatusCode::BAD_REQUEST,
            format!("invalid value: {}", error.message),
        ),
        DbErrorKind::QueryCanceled => (
            StatusCode::SERVICE_UNAVAILABLE,
            "query timed out".to_string(),
        ),
        DbErrorKind::SerializationFailure | DbErrorKind::DeadlockDetected => {
            (StatusCode::CONFLICT, "conflict: retry".to_string())
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal database error".to_string(),
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformErrorBody {
    pub status: StatusCode,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestOriginated;

pub type PlatformResult<T> = Result<T, PlatformError>;

#[cfg(test)]
mod tests {
    use super::*;

    async fn rendered(
        error: PlatformError,
    ) -> (StatusCode, Option<String>, String, PlatformErrorBody) {
        let response = error.into_response();
        let marker = response
            .extensions()
            .get::<PlatformErrorBody>()
            .cloned()
            .expect("marker");
        let status = response.status();
        let content_type = response
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        (
            status,
            content_type,
            String::from_utf8_lossy(&body).into_owned(),
            marker,
        )
    }

    #[tokio::test]
    async fn a_request_to_be_made_again_says_when() {
        let later = |after| PlatformError::RetryLater {
            message: "not ready".into(),
            after,
        };
        for (after, seconds) in [
            (Duration::from_secs(30), "30"),
            (Duration::from_millis(200), "1"),
            (Duration::from_millis(1500), "2"),
            (Duration::ZERO, "1"),
        ] {
            let response = later(after).into_response();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some(seconds)
            );
        }
        let (status, _, body, _) = rendered(later(Duration::from_secs(1))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, r#"{"error":"not ready"}"#);
        let unavailable = PlatformError::Unavailable("down".into()).into_response();
        assert!(unavailable.headers().get(RETRY_AFTER).is_none());
    }

    #[tokio::test]
    async fn platform_errors_keep_their_response_and_carry_a_marker() {
        let (status, content_type, body, marker) =
            rendered(PlatformError::NotFound("Service not found".into())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(content_type.as_deref(), Some("application/json"));
        assert_eq!(body, r#"{"error":"Service not found"}"#);
        assert_eq!(
            marker,
            PlatformErrorBody {
                status: StatusCode::NOT_FOUND,
                message: "Service not found".into(),
            }
        );

        let (status, _, body, marker) =
            rendered(PlatformError::Eyre(eyre::eyre!("secret detail"))).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, r#"{"error":"Internal error"}"#);
        assert_eq!(marker.message, "Internal error");
    }

    #[tokio::test]
    async fn spec_errors_are_the_services_and_budget_exhaustion_a_gateway_timeout() {
        let (status, _, body, _) = rendered(PlatformError::from(
            cufflink_types::openapi::OpenApiError("operation id 'x' is used more than once".into()),
        ))
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body,
            r#"{"error":"OpenAPI docs: operation id 'x' is used more than once"}"#
        );
        let (status, ..) = rendered(PlatformError::GatewayTimeout("spent".into())).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    }

    fn db_error(code: &str, constraint: Option<&str>, column: Option<&str>) -> DbError {
        cufflink_types::db::DbErrorFields {
            code: Some(code.into()),
            constraint: constraint.map(str::to_string),
            column: column.map(str::to_string),
            ..cufflink_types::db::DbErrorFields::new(
                DbErrorKind::from_sqlstate(code),
                "value \"x\" is out of range",
            )
        }
        .into()
    }

    #[test]
    fn sqlstates_map_to_statuses_by_operation() {
        use CrudOp::*;
        let cases = [
            (
                "23505",
                Some("books_email_key"),
                None,
                Create,
                409,
                "conflict: books_email_key",
            ),
            (
                "23P01",
                Some("no_overlap"),
                None,
                Update,
                409,
                "conflict: no_overlap",
            ),
            (
                "23503",
                Some("child_parent_fkey"),
                None,
                Delete,
                409,
                "conflict: row is referenced (child_parent_fkey)",
            ),
            (
                "23503",
                Some("child_parent_fkey"),
                None,
                Commit,
                409,
                "conflict: row is referenced (child_parent_fkey)",
            ),
            (
                "23503",
                Some("child_parent_fkey"),
                None,
                Create,
                422,
                "invalid reference (child_parent_fkey)",
            ),
            (
                "23503",
                None,
                None,
                Update,
                422,
                "invalid reference (unnamed constraint)",
            ),
            (
                "23514",
                Some("price_positive"),
                None,
                Create,
                422,
                "constraint violated: price_positive",
            ),
            (
                "23502",
                None,
                Some("title"),
                Create,
                422,
                "constraint violated: title",
            ),
            (
                "P0001",
                None,
                None,
                Update,
                422,
                "value \"x\" is out of range",
            ),
            (
                "22P02",
                None,
                None,
                Read,
                400,
                "invalid value: value \"x\" is out of range",
            ),
            (
                "22003",
                None,
                None,
                Create,
                400,
                "invalid value: value \"x\" is out of range",
            ),
            (
                "22008",
                None,
                None,
                Create,
                400,
                "invalid value: value \"x\" is out of range",
            ),
            (
                "22012",
                None,
                None,
                Read,
                400,
                "invalid value: value \"x\" is out of range",
            ),
            ("57014", None, None, Read, 503, "query timed out"),
            ("40001", None, None, Commit, 409, "conflict: retry"),
            ("40P01", None, None, Update, 409, "conflict: retry"),
            ("42P01", None, None, Read, 500, "Internal database error"),
            ("08006", None, None, Read, 500, "Internal database error"),
        ];
        for (code, constraint, column, op, status, message) in cases {
            let (actual_status, actual_message) =
                db_error_response(&db_error(code, constraint, column), op);
            assert_eq!(
                (actual_status.as_u16(), actual_message.as_str()),
                (status, message),
                "{code} {op:?}"
            );
        }
    }
}
