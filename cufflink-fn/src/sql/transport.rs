//! One `db_call` round trip: the request envelope out, the response envelope
//! back, a failure as the `Err`.

use cufflink_types::db::{DbError, DbErrorKind, DbRequest, DbResponse};

/// A successful response.
pub(crate) enum Reply {
    Rows(Vec<super::Row>),
    Affected(u64),
    Begun(u32),
    Done,
}

pub(crate) fn call(request: &DbRequest) -> Result<Reply, DbError> {
    match exchange(request)? {
        DbResponse::Rows { rows, columns } => Ok(Reply::Rows(super::row::rows(rows, columns))),
        DbResponse::Affected(n) => Ok(Reply::Affected(n)),
        DbResponse::Begun(tx) => Ok(Reply::Begun(tx)),
        DbResponse::Done => Ok(Reply::Done),
        DbResponse::Failed(error) => Err(error),
    }
}

pub(crate) fn protocol(message: impl Into<String>) -> DbError {
    DbError::new(DbErrorKind::Protocol, message)
}

impl Reply {
    fn kind(&self) -> &'static str {
        match self {
            Reply::Rows(_) => "rows",
            Reply::Affected(_) => "an affected-row count",
            Reply::Begun(_) => "a transaction id",
            Reply::Done => "no result",
        }
    }

    pub(crate) fn rows(self) -> Result<Vec<super::Row>, DbError> {
        match self {
            Reply::Rows(rows) => Ok(rows),
            other => Err(unexpected("rows", &other)),
        }
    }

    pub(crate) fn affected(self) -> Result<u64, DbError> {
        match self {
            Reply::Affected(n) => Ok(n),
            other => Err(unexpected("an affected-row count", &other)),
        }
    }

    pub(crate) fn begun(self) -> Result<u32, DbError> {
        match self {
            Reply::Begun(tx) => Ok(tx),
            other => Err(unexpected("a transaction id", &other)),
        }
    }

    pub(crate) fn done(self) -> Result<(), DbError> {
        match self {
            Reply::Done => Ok(()),
            other => Err(unexpected("no result", &other)),
        }
    }
}

fn unexpected(expected: &str, got: &Reply) -> DbError {
    protocol(format!(
        "expected {expected} from the platform, got {}",
        got.kind()
    ))
}

#[cfg(all(target_arch = "wasm32", not(test)))]
fn exchange(request: &DbRequest) -> Result<DbResponse, DbError> {
    #[link(wasm_import_module = "env")]
    extern "C" {
        fn db_call(ptr: i32, len: i32) -> i32;
    }
    let payload = serde_json::to_vec(request).map_err(|e| protocol(e.to_string()))?;
    let rc = unsafe { db_call(payload.as_ptr() as i32, payload.len() as i32) };
    if rc < 0 {
        return Err(protocol(
            "the platform could not read the request (it may exceed the size limit)",
        ));
    }
    let bytes = crate::host_buffer::read_host_bytes()
        .ok_or_else(|| protocol("the platform returned no response"))?;
    serde_json::from_slice(&bytes).map_err(|e| protocol(format!("malformed response: {e}")))
}

#[cfg(all(not(target_arch = "wasm32"), not(test)))]
fn exchange(request: &DbRequest) -> Result<DbResponse, DbError> {
    crate::host::db()
        .map(|host| host.call(request))
        .ok_or_else(DbError::unavailable)
}

#[cfg(test)]
fn exchange(request: &DbRequest) -> Result<DbResponse, DbError> {
    mock::exchange(request)
}

/// A scripted platform for unit tests: records every request and answers
/// with the handler's response.
#[cfg(test)]
pub(crate) mod mock {
    use super::*;
    use std::cell::RefCell;

    type Handler = Box<dyn FnMut(&DbRequest) -> DbResponse>;

    thread_local! {
        static HANDLER: RefCell<Option<Handler>> = RefCell::new(None);
        static SEEN: RefCell<Vec<DbRequest>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn exchange(request: &DbRequest) -> Result<DbResponse, DbError> {
        SEEN.with(|seen| seen.borrow_mut().push(request.clone()));
        HANDLER.with(|handler| match handler.borrow_mut().as_mut() {
            Some(handler) => Ok(handler(request)),
            None => Err(DbError::unavailable()),
        })
    }

    /// Run `body` against `handler`, returning its result and the requests
    /// it made.
    pub(crate) fn with<T>(
        handler: impl FnMut(&DbRequest) -> DbResponse + 'static,
        body: impl FnOnce() -> T,
    ) -> (T, Vec<DbRequest>) {
        HANDLER.with(|h| *h.borrow_mut() = Some(Box::new(handler)));
        SEEN.with(|seen| seen.borrow_mut().clear());
        let result = body();
        HANDLER.with(|h| *h.borrow_mut() = None);
        (result, SEEN.with(|seen| seen.take()))
    }
}
