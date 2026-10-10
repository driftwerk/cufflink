use bytes::Bytes;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixStream;
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint {
    socket: Arc<Path>,
    token: Arc<str>,
}

impl Endpoint {
    pub fn new(socket: &Path, token: &str) -> Self {
        Self {
            socket: Arc::from(socket),
            token: Arc::from(token),
        }
    }

    pub fn generate(socket: &Path) -> eyre::Result<Self> {
        let mut token = [0u8; 32];
        getrandom::fill(&mut token)
            .map_err(|e| eyre::eyre!("no OS randomness for a child's token: {e}"))?;
        Ok(Self::new(socket, &hex::encode(token)))
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn token(&self) -> &str {
        &self.token
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("socket", &self.socket)
            .finish_non_exhaustive()
    }
}

fn path_limit() -> usize {
    std::mem::size_of::<libc::sockaddr_un>() - std::mem::offset_of!(libc::sockaddr_un, sun_path) - 1
}

pub fn name(service_id: Uuid, version: i32, start: u64) -> String {
    let id = service_id.simple().to_string();
    format!("{}-{version}-{start}.sock", &id[..12])
}

pub fn fitting(dir: &Path) -> eyre::Result<PathBuf> {
    fits(dir, path_limit()).map(|()| dir.to_path_buf())
}

fn fits(dir: &Path, limit: usize) -> eyre::Result<()> {
    let longest = name(Uuid::max(), i32::MIN, u64::MAX);
    let needed = dir.join(&longest).as_os_str().len();
    match needed <= limit {
        true => Ok(()),
        false => Err(eyre::eyre!(
            "EXT_SOCKET_DIR {} is too long: a socket in it takes up to {needed} bytes and a \
             socket path is at most {limit} here; use a directory of at most {} bytes",
            dir.display(),
            limit.saturating_sub(longest.len() + 1)
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: StatusCode,
    pub body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("the answer is larger than {0} bytes")]
    TooLarge(usize),
    #[error("{0}")]
    Failed(String),
}

fn failed(e: impl std::fmt::Display) -> ExchangeError {
    ExchangeError::Failed(e.to_string())
}

pub async fn exchange(
    endpoint: &Endpoint,
    method: Method,
    path: &str,
    body: Bytes,
    limit: usize,
) -> Result<Answer, ExchangeError> {
    let stream = UnixStream::connect(endpoint.socket())
        .await
        .map_err(failed)?;
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(failed)?;
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header(HOST, "ext")
        .header(AUTHORIZATION, format!("Bearer {}", endpoint.token()))
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(body))
        .map_err(failed)?;
    let answered = async move {
        let response = sender.send_request(request).await.map_err(failed)?;
        let status = response.status();
        let body = Limited::new(response.into_body(), limit)
            .collect()
            .await
            .map_err(|e| match e.is::<LengthLimitError>() {
                true => ExchangeError::TooLarge(limit),
                false => failed(e),
            })?
            .to_bytes();
        Ok(Answer { status, body })
    };
    tokio::join!(answered, connection).0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use axum::routing::{get, post};
    use axum::Router;
    use tempfile::TempDir;

    #[test]
    fn every_start_has_its_own_socket() {
        let id = Uuid::parse_str("0123456789ab4def8123456789abcdef").expect("uuid");
        assert_eq!(name(id, 3, 17), "0123456789ab-3-17.sock");
        assert_ne!(name(id, 3, 17), name(id, 3, 18));
        assert_eq!(
            name(Uuid::max(), i32::MIN, u64::MAX),
            "ffffffffffff--2147483648-18446744073709551615.sock"
        );
    }

    #[test]
    fn a_socket_directory_must_leave_room_for_the_longest_socket() {
        let longest = name(Uuid::max(), i32::MIN, u64::MAX).len();
        let at_most = |limit: usize| "/".to_string() + &"d".repeat(limit - longest - 2);
        for limit in [103, 107] {
            assert!(fits(Path::new(&at_most(limit)), limit).is_ok(), "{limit}");
            let over = at_most(limit) + "d";
            let refused = fits(Path::new(&over), limit)
                .expect_err("one byte too long")
                .to_string();
            assert_eq!(
                refused,
                format!(
                    "EXT_SOCKET_DIR {over} is too long: a socket in it takes up to {} bytes and a \
                     socket path is at most {limit} here; use a directory of at most {} bytes",
                    limit + 1,
                    limit - longest - 1
                )
            );
        }
        assert!(fitting(Path::new("/tmp/cufflink-ext")).is_ok());
        assert!(path_limit() == 103 || path_limit() == 107);
    }

    async fn serving(app: Router) -> (TempDir, Endpoint) {
        let dir = tempfile::Builder::new()
            .prefix("cxs")
            .tempdir_in("/tmp")
            .expect("socket dir");
        let socket = dir.path().join("s.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        tokio::spawn(async move { axum::serve(listener, app).await });
        (dir, Endpoint::new(&socket, "t0ken"))
    }

    #[test]
    fn every_endpoint_has_a_token_of_its_own_never_printed() {
        let socket = Path::new("/tmp/cufflink-ext/a.sock");
        let (first, second) = (
            Endpoint::generate(socket).expect("token"),
            Endpoint::generate(socket).expect("token"),
        );
        assert_eq!(first.token().len(), 64);
        assert!(first.token().bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(first.token(), second.token());
        assert_eq!(first.socket(), socket);
        assert!(!format!("{first:?}").contains(first.token()));
    }

    #[tokio::test]
    async fn an_exchange_bears_the_childs_token() {
        let app = Router::new().route(
            "/ready",
            get(|headers: HeaderMap| async move {
                headers[AUTHORIZATION].to_str().expect("ascii").to_string()
            }),
        );
        let (_dir, endpoint) = serving(app).await;
        let answer = exchange(&endpoint, Method::GET, "/ready", Bytes::new(), 64)
            .await
            .expect("answered");
        assert_eq!(answer.body, "Bearer t0ken");
    }

    #[tokio::test]
    async fn an_exchange_reads_the_answer_up_to_its_limit() {
        let app = Router::new()
            .route("/ready", get(|| async { "{\"ok\":1}" }))
            .route("/call", post(|body: Bytes| async move { body }));
        let (_dir, endpoint) = serving(app).await;
        let answer = exchange(&endpoint, Method::GET, "/ready", Bytes::new(), 64)
            .await
            .expect("answered");
        assert_eq!(answer.status, StatusCode::OK);
        assert_eq!(answer.body, "{\"ok\":1}");
        let echoed = exchange(
            &endpoint,
            Method::POST,
            "/call",
            Bytes::from("x".repeat(64)),
            64,
        )
        .await
        .expect("at the limit");
        assert_eq!(echoed.body.len(), 64);
        assert!(matches!(
            exchange(
                &endpoint,
                Method::POST,
                "/call",
                Bytes::from("x".repeat(65)),
                64
            )
            .await,
            Err(ExchangeError::TooLarge(64))
        ));
        let missing = exchange(&endpoint, Method::GET, "/nowhere", Bytes::new(), 64)
            .await
            .expect("answered");
        assert_eq!(missing.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn nothing_listening_is_a_failed_exchange() {
        let dir = TempDir::new().expect("dir");
        assert!(matches!(
            exchange(
                &Endpoint::new(&dir.path().join("gone.sock"), "t0ken"),
                Method::GET,
                "/ready",
                Bytes::new(),
                64
            )
            .await,
            Err(ExchangeError::Failed(_))
        ));
    }
}
