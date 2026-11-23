//! An object assembled from objects the store already holds, copied inside
//! the store so that none of their bytes are sent to it.

use bytes::Bytes;
use eyre::Result;
use futures_util::future::{self, Either};
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use object_store::client::{
    HttpClient, HttpConnector, HttpErrorKind, HttpRequest, ReqwestConnector,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};

/// How many copies run at once: a copy sends no bytes, so it is bounded by
/// the store's work, not by the network.
pub(crate) const COPIES_AT_ONCE: usize = 8;

/// The characters a key or query value keeps unencoded in a store's URL.
pub(crate) const STRICT: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

pub(crate) fn generic(
    backend: &'static str,
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> object_store::Error {
    object_store::Error::Generic {
        store: backend,
        source: source.into(),
    }
}

/// Runs `copies` while `data` is read to its end. An error `data` yields is
/// returned as it was yielded, before a copy's missing source, so a part
/// the read finds missing is answered as the read answers it; any other
/// failed copy is answered at once.
pub(crate) async fn alongside<T>(
    copies: impl std::future::Future<Output = Result<T>>,
    data: BoxStream<'static, Result<Bytes>>,
) -> Result<T> {
    let drained = data.try_for_each(|_| future::ready(Ok(())));
    match future::select(std::pin::pin!(copies), std::pin::pin!(drained)).await {
        Either::Left((Err(e), _)) if !missing(&e) => Err(e),
        Either::Left((copied, drained)) => drained.await.and(copied),
        Either::Right((drained, copies)) => {
            drained?;
            copies.await
        }
    }
}

fn missing(e: &eyre::Report) -> bool {
    matches!(
        e.downcast_ref::<object_store::Error>(),
        Some(object_store::Error::NotFound { .. })
    )
}

pub(crate) fn client(options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
    ReqwestConnector::default().connect(options)
}

/// Sends the request `request` builds, again after a pause while the store
/// answers with a server error or the connection fails, as object_store
/// retries its own requests, and answers what `read` makes of its body or
/// the store's refusal as the storage error it stands for.
pub(crate) async fn sent<T, Fut>(
    client: &HttpClient,
    backend: &'static str,
    location: &str,
    request: impl Fn() -> Fut,
    read: impl Fn(&[u8]) -> std::result::Result<T, Refused>,
) -> object_store::Result<T>
where
    Fut: std::future::Future<Output = object_store::Result<HttpRequest>>,
{
    let retry = crate::common::retry_config();
    let started = tokio::time::Instant::now();
    let mut pause = retry.backoff.init_backoff;
    let mut retried = 0;
    loop {
        match answered(client, backend, location, request().await?)
            .await
            .and_then(|body| read(&body))
        {
            Err(Refused::Transient(_))
                if retried < retry.max_retries
                    && started.elapsed() + pause <= retry.retry_timeout =>
            {
                tokio::time::sleep(pause).await;
                pause = pause
                    .mul_f64(retry.backoff.base)
                    .min(retry.backoff.max_backoff);
                retried += 1;
            }
            answer => return answer.map_err(Refused::into_error),
        }
    }
}

/// Why the store did not do what it was asked: for a while, so asking
/// again may succeed, or for good.
#[derive(Debug)]
pub(crate) enum Refused {
    Transient(object_store::Error),
    Final(object_store::Error),
}

impl Refused {
    fn into_error(self) -> object_store::Error {
        match self {
            Refused::Transient(error) | Refused::Final(error) => error,
        }
    }
}

async fn answered(
    client: &HttpClient,
    backend: &'static str,
    location: &str,
    request: HttpRequest,
) -> std::result::Result<Bytes, Refused> {
    let failed = |source: object_store::client::HttpError| match source.kind() {
        HttpErrorKind::Connect
        | HttpErrorKind::Request
        | HttpErrorKind::Timeout
        | HttpErrorKind::Interrupted => Refused::Transient(generic(backend, source)),
        _ => Refused::Final(generic(backend, source)),
    };
    let answer = client.execute(request).await.map_err(failed)?;
    let status = answer.status();
    let body = answer.into_body().bytes().await.map_err(failed)?;
    if status.is_success() {
        return Ok(body);
    }
    let source = format!(
        "the store answered {status}: {}",
        String::from_utf8_lossy(&body)
    );
    let path = location.to_string();
    Err(match status {
        http::StatusCode::NOT_FOUND => Refused::Final(object_store::Error::NotFound {
            path,
            source: source.into(),
        }),
        http::StatusCode::PRECONDITION_FAILED => {
            Refused::Final(object_store::Error::Precondition {
                path,
                source: source.into(),
            })
        }
        http::StatusCode::UNAUTHORIZED => Refused::Final(object_store::Error::Unauthenticated {
            path,
            source: source.into(),
        }),
        http::StatusCode::FORBIDDEN => Refused::Final(object_store::Error::PermissionDenied {
            path,
            source: source.into(),
        }),
        status if status.is_server_error() || status == http::StatusCode::TOO_MANY_REQUESTS => {
            Refused::Transient(generic(backend, source))
        }
        _ => Refused::Final(generic(backend, source)),
    })
}
