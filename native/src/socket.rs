//! The Unix-socket transport the extension runner relays calls over:
//! `GET /ready` and `POST /call`, HTTP/1.1, each with the runner's token
//! once one is required.

use crate::deadline::CallDeadline;
use crate::dispatch::{decode, Dispatcher};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use cufflink_types::ext::{ExtReply, Ready};
use eyre::WrapErr;
use std::fs::{self, Permissions};
use std::future::Future;
use std::io::ErrorKind;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::net::UnixListener;

/// A service bound to its socket, ready to [`serve`](Self::serve).
pub struct SocketServer {
    listener: UnixListener,
    socket: SocketFile,
    dispatcher: Arc<Dispatcher>,
    token: Option<Arc<str>>,
}

/// The socket file, removed once nothing listens on it.
struct SocketFile(PathBuf);

impl Drop for SocketFile {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.0) {
            if e.kind() != ErrorKind::NotFound {
                tracing::warn!(socket = %self.0.display(), error = %e, "cannot remove the socket");
            }
        }
    }
}

fn remove_stale(path: &Path) -> eyre::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => fs::remove_file(path)
            .wrap_err_with(|| format!("cannot remove the stale socket {}", path.display())),
        Ok(_) => eyre::bail!("{} exists and is not a socket", path.display()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).wrap_err_with(|| format!("cannot inspect {}", path.display())),
    }
}

impl SocketServer {
    pub(crate) fn bind(path: &Path, dispatcher: Dispatcher) -> eyre::Result<Self> {
        remove_stale(path)?;
        let listener = UnixListener::bind(path)
            .wrap_err_with(|| format!("cannot listen on {}", path.display()))?;
        let socket = SocketFile(path.to_path_buf());
        fs::set_permissions(path, Permissions::from_mode(0o600))
            .wrap_err_with(|| format!("cannot restrict {} to its owner", path.display()))?;
        Ok(Self {
            listener,
            socket,
            dispatcher: Arc::new(dispatcher),
            token: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.socket.0
    }

    /// Answer only requests bearing `token` (`Authorization: Bearer
    /// <token>`), anything else with 401: another process allowed to
    /// connect to the socket cannot call as the runner.
    pub fn require_token(self, token: impl Into<String>) -> Self {
        Self {
            token: Some(Arc::from(token.into())),
            ..self
        }
    }

    /// Serve until `shutdown` completes, then stop accepting, finish the
    /// calls in flight and remove the socket.
    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> eyre::Result<()> {
        let Self {
            listener,
            socket,
            dispatcher,
            token,
        } = self;
        tracing::info!(
            socket = %socket.0.display(),
            extensions = ?dispatcher.ready().extensions,
            "serving"
        );
        axum::serve(listener, router(dispatcher, token))
            .with_graceful_shutdown(shutdown)
            .await
            .wrap_err("the socket server failed")?;
        drop(socket);
        tracing::info!("stopped");
        Ok(())
    }
}

fn router(dispatcher: Arc<Dispatcher>, token: Option<Arc<str>>) -> Router {
    let routes = Router::new()
        .route("/ready", get(ready))
        .route("/call", post(call))
        .fallback(not_found)
        .method_not_allowed_fallback(not_found)
        .layer(DefaultBodyLimit::disable())
        .with_state(dispatcher);
    match token {
        Some(token) => routes.layer(middleware::from_fn_with_state(token, authorized)),
        None => routes,
    }
}

/// Whether `headers` carry `Authorization: Bearer <token>` (the scheme in
/// any case), the token compared in constant time.
fn bears(headers: &HeaderMap, token: &str) -> bool {
    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map_or("", |(_, credentials)| credentials.trim());
    bool::from(presented.as_bytes().ct_eq(token.as_bytes()))
}

async fn authorized(State(token): State<Arc<str>>, request: Request, next: Next) -> Response {
    match bears(request.headers(), &token) {
        true => next.run(request).await,
        false => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn ready(State(dispatcher): State<Arc<Dispatcher>>) -> Json<Ready> {
    Json(dispatcher.ready())
}

async fn call(
    State(dispatcher): State<Arc<Dispatcher>>,
    body: Bytes,
) -> (StatusCode, Json<ExtReply>) {
    match decode(&body) {
        Ok(request) => {
            let deadline = request.context.deadline();
            (
                StatusCode::OK,
                Json(dispatcher.dispatch(request, deadline).await),
            )
        }
        Err(refused) => (StatusCode::BAD_REQUEST, Json(refused)),
    }
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_whole_token_as_a_bearer_is_borne() {
        let headers = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, value.parse().expect("header"));
            headers
        };
        assert!(bears(&headers("Bearer abc"), "abc"));
        assert!(bears(&headers("bearer abc"), "abc"));
        for refused in ["Bearer", "Bearer ab", "Bearer abcd", "Basic abc", "abc"] {
            assert!(!bears(&headers(refused), "abc"), "{refused}");
        }
        assert!(!bears(&HeaderMap::new(), "abc"));
    }
}
