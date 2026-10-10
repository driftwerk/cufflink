//! The host response buffer: where host functions leave the data they return,
//! read with `get_host_response_len` and `get_host_response`.

use crate::ffi;
use serde::de::DeserializeOwned;

/// The bytes the last host call left in the buffer; `None` when it is empty.
pub(crate) fn read_host_bytes() -> Option<Vec<u8>> {
    let len = unsafe { ffi::get_host_response_len() };
    if len <= 0 {
        return None;
    }
    let mut buf = vec![0u8; len as usize];
    let read = unsafe { ffi::get_host_response(buf.as_mut_ptr() as i32, len) };
    if read <= 0 {
        return None;
    }
    buf.truncate(read as usize);
    Some(buf)
}

/// The buffer as UTF-8 text; `None` when empty or not UTF-8.
pub(crate) fn read_host_string() -> Option<String> {
    read_host_bytes().and_then(|bytes| String::from_utf8(bytes).ok())
}

/// The buffer as JSON; `None` when empty or not a `T`.
pub(crate) fn read_host_json<T: DeserializeOwned>() -> Option<T> {
    read_host_bytes().and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

/// The buffer as JSON after replacing invalid UTF-8, for the envelopes whose
/// string values may carry arbitrary bytes from a row or a response body.
pub(crate) fn read_host_json_lossy<T: DeserializeOwned>() -> Option<T> {
    read_host_bytes().and_then(|bytes| serde_json::from_str(&String::from_utf8_lossy(&bytes)).ok())
}
