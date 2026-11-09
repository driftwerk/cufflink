use crate::host::{parse_host, HostnameFormat};
use crate::process_manager::{Entered, Lease, ProcessManager};
use crate::warmer::{LazyWarmer, WarmError};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use http_body::{Frame, SizeHint};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

const TENANT_HEADER: &str = "x-tenant-slug";

pub struct ProxyState {
    pub process_manager: Arc<ProcessManager>,
    pub warmer: Arc<LazyWarmer>,
    pub http_client: Client<hyper_util::client::legacy::connect::HttpConnector, Body>,
    pub base_domain: String,
    pub hostname_format: HostnameFormat,
}

impl ProxyState {
    pub fn new(
        process_manager: Arc<ProcessManager>,
        warmer: Arc<LazyWarmer>,
        base_domain: String,
        hostname_format: HostnameFormat,
    ) -> Self {
        let http_client = Client::builder(TokioExecutor::new()).build_http();
        Self {
            process_manager,
            warmer,
            http_client,
            base_domain,
            hostname_format,
        }
    }
}

pub async fn host_proxy_handler(
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> Response {
    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let host_no_port = host.split(':').next().unwrap_or(host);

    let (tenant, service) =
        match parse_host(host_no_port, &state.base_domain, state.hostname_format) {
            Some(result) => result,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("Cannot determine app from Host: {}", host),
                )
                    .into_response();
            }
        };

    if !state.warmer.serves(&tenant) {
        return (
            StatusCode::NOT_FOUND,
            format!("No app of this runner at Host: {host}"),
        )
            .into_response();
    }

    let Lease { port, entered } = match state.process_manager.lease(&tenant, &service) {
        Some(lease) => lease,
        None => match state.warmer.ensure_warm(&tenant, &service).await {
            Ok(()) => match state.process_manager.lease(&tenant, &service) {
                Some(lease) => lease,
                None => {
                    return (
                        StatusCode::BAD_GATEWAY,
                        format!(
                            "Warmup of {tenant}/{service} reported success but no process is registered"
                        ),
                    )
                        .into_response();
                }
            },
            Err(WarmError::NotFound { tenant, service }) => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("No active deployment for {tenant}/{service}"),
                )
                    .into_response();
            }
            Err(e @ WarmError::WarmupFailed(_)) => {
                return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
            }
            Err(e @ WarmError::KeyRefused) => {
                return (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response();
            }
        },
    };

    let path = req.uri().path();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{}", q))
        .unwrap_or_default();
    let downstream_uri = format!("http://127.0.0.1:{}{}{}", port, path, query);

    let (parts, body) = req.into_parts();
    let mut downstream_req = Request::builder().method(parts.method).uri(&downstream_uri);

    for (key, value) in &parts.headers {
        if key.as_str().eq_ignore_ascii_case(TENANT_HEADER) {
            continue;
        }
        downstream_req = downstream_req.header(key, value);
    }
    downstream_req = downstream_req.header(TENANT_HEADER, &tenant);

    let downstream_req = match downstream_req.body(body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to build proxy request: {}", e),
            )
                .into_response();
        }
    };

    match state.http_client.request(downstream_req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            let body =
                http_body_util::BodyExt::map_err(body, |e| std::io::Error::other(e.to_string()));
            Response::from_parts(
                parts,
                Body::new(Counted {
                    body,
                    entered: Some(entered),
                }),
            )
        }
        Err(e) => (StatusCode::BAD_GATEWAY, format!("Proxy error: {}", e)).into_response(),
    }
}

struct Counted<B> {
    body: B,
    entered: Option<Entered>,
}

impl<B> http_body::Body for Counted<B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let polled = Pin::new(&mut self.body).poll_frame(cx);
        if let Poll::Ready(None) = polled {
            self.entered = None;
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_manager::InFlight;
    use http_body_util::{BodyExt, Full, StreamBody};
    use std::convert::Infallible;
    use std::time::Duration;

    async fn idle_within(in_flight: &InFlight, within: Duration) -> bool {
        tokio::time::timeout(within, in_flight.idle()).await.is_ok()
    }

    #[tokio::test]
    async fn a_request_counts_until_its_body_ends() {
        let in_flight = InFlight::new();
        let mut body = Counted {
            body: Full::new(Bytes::from_static(b"done")),
            entered: in_flight.enter(),
        };
        assert_eq!(in_flight.close(), 1);
        assert!(!idle_within(&in_flight, Duration::from_millis(100)).await);
        while body.frame().await.is_some() {}
        assert!(
            idle_within(&in_flight, Duration::from_secs(1)).await,
            "a body read to its end no longer counts, though not yet dropped"
        );
    }

    #[tokio::test]
    async fn a_request_whose_client_goes_away_no_longer_counts() {
        let in_flight = InFlight::new();
        let streaming = Counted {
            body: StreamBody::new(futures_util::stream::pending::<
                Result<Frame<Bytes>, Infallible>,
            >()),
            entered: in_flight.enter(),
        };
        assert_eq!(in_flight.close(), 1);
        assert!(
            !idle_within(&in_flight, Duration::from_millis(100)).await,
            "a stream still being sent counts"
        );
        drop(streaming);
        assert!(idle_within(&in_flight, Duration::from_secs(1)).await);
    }
}
