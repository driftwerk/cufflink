//! One envelope host call: the request out as JSON, the response read back
//! from the host buffer.

use crate::host_buffer;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt;

/// Why a call brought no response back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Failure {
    Encode(String),
    /// The platform could not read the request.
    Refused,
    Malformed(String),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Encode(e) => write!(f, "the request could not be encoded: {e}"),
            Failure::Refused => f.write_str(
                "the platform could not read the request (it may exceed the size limit)",
            ),
            Failure::Malformed(e) => write!(f, "malformed response from the platform: {e}"),
        }
    }
}

pub(crate) fn call<Request: Serialize, Response: DeserializeOwned>(
    import: unsafe extern "C" fn(i32, i32) -> i32,
    request: &Request,
) -> Result<Response, Failure> {
    let payload = serde_json::to_vec(request).map_err(|e| Failure::Encode(e.to_string()))?;
    if unsafe { import(payload.as_ptr() as i32, payload.len() as i32) } < 0 {
        return Err(Failure::Refused);
    }
    let bytes =
        host_buffer::read_host_bytes().ok_or_else(|| Failure::Malformed("it is empty".into()))?;
    serde_json::from_slice(&bytes).map_err(|e| Failure::Malformed(e.to_string()))
}
