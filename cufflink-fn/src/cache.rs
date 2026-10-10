//! A key-value cache private to this service: keys are namespaced by the
//! platform per tenant and service, so the same key in two services (or two
//! tenants) never collides. Every entry expires, between 1 second and 30
//! days after it is written. Needs a platform that supports `cache`; the
//! `cache` feature declares that in the module.
//!
//! ```rust,ignore
//! use std::time::Duration;
//!
//! if cache::set_nx("lock:invoice:42", "1", Duration::from_secs(30))? {
//!     // this invocation holds the lock
//! }
//! let hits = cache::incr("hits:today", 1, Duration::from_secs(86_400))?;
//! ```
//!
//! Outside the platform every call is `Err(CacheError::Unavailable)`, unless
//! a test host answers it.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// No cache is configured, or it cannot be reached.
    Unavailable,
    Timeout,
    /// Keys are 1 to 512 bytes of printable ASCII without spaces.
    InvalidKey(String),
    /// A TTL outside 1 second to 30 days.
    InvalidTtl(String),
    /// Values are at most 1 MiB.
    TooLarge,
    Protocol(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::Unavailable => f.write_str("the cache is not available"),
            CacheError::Timeout => f.write_str("the cache did not answer in time"),
            CacheError::InvalidKey(message)
            | CacheError::InvalidTtl(message)
            | CacheError::Protocol(message) => f.write_str(message),
            CacheError::TooLarge => f.write_str("cache values are at most 1 MiB"),
        }
    }
}

impl std::error::Error for CacheError {}

pub fn get(key: &str) -> Result<Option<String>, CacheError> {
    call(json!({"op": "get", "key": key})).map(|reply| reply.value)
}

/// The values of several keys at once, `None` for each absent one.
pub fn mget(keys: &[&str]) -> Result<Vec<Option<String>>, CacheError> {
    call(json!({"op": "mget", "keys": keys}))?.field(|r| r.values)
}

pub fn set(key: &str, value: &str, ttl: Duration) -> Result<(), CacheError> {
    call(json!({"op": "set", "key": key, "value": value, "ttl_ms": millis(ttl)})).map(drop)
}

/// Set the key only if it does not exist; `true` when this call set it.
/// With a short TTL this is a lock that frees itself.
pub fn set_nx(key: &str, value: &str, ttl: Duration) -> Result<bool, CacheError> {
    call(json!({"op": "set_nx", "key": key, "value": value, "ttl_ms": millis(ttl)}))?
        .field(|r| r.set)
}

/// Delete the key; `true` when it existed.
pub fn del(key: &str) -> Result<bool, CacheError> {
    call(json!({"op": "del", "key": key}))?.field(|r| r.deleted)
}

/// Add `by` to the integer at `key` (a missing key counts from 0) and
/// return the result. A key this call creates expires after `ttl_if_new`;
/// an existing key keeps its expiry.
pub fn incr(key: &str, by: i64, ttl_if_new: Duration) -> Result<i64, CacheError> {
    call(json!({"op": "incr", "key": key, "by": by, "ttl_ms": millis(ttl_if_new)}))?
        .field(|r| r.count)
}

/// Give the key a new TTL; `true` when it existed.
pub fn expire(key: &str, ttl: Duration) -> Result<bool, CacheError> {
    call(json!({"op": "expire", "key": key, "ttl_ms": millis(ttl)}))?.field(|r| r.set)
}

pub fn get_json<T: DeserializeOwned>(key: &str) -> Result<Option<T>, CacheError> {
    get(key)?
        .map(|text| {
            serde_json::from_str(&text).map_err(|e| {
                CacheError::Protocol(format!(
                    "the value at {key:?} is not the expected JSON: {e}"
                ))
            })
        })
        .transpose()
}

pub fn set_json<T: Serialize>(key: &str, value: &T, ttl: Duration) -> Result<(), CacheError> {
    let text = serde_json::to_string(value)
        .map_err(|e| CacheError::Protocol(format!("the value is not serializable: {e}")))?;
    set(key, &text, ttl)
}

fn millis(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}

/// The response envelope: `{"ok": true, <field>}` or
/// `{"ok": false, "error": {"kind", "message"}}`.
#[derive(Debug, Default, Deserialize)]
struct Reply {
    ok: bool,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    values: Option<Vec<Option<String>>>,
    #[serde(default)]
    set: Option<bool>,
    #[serde(default)]
    deleted: Option<bool>,
    #[serde(default)]
    count: Option<i64>,
    #[serde(default)]
    error: Option<Failure>,
}

#[derive(Debug, Deserialize)]
struct Failure {
    kind: String,
    message: String,
}

impl Reply {
    fn field<T>(self, pick: impl FnOnce(Self) -> Option<T>) -> Result<T, CacheError> {
        pick(self).ok_or_else(|| CacheError::Protocol("the response lacks its result".into()))
    }
}

fn call(request: Value) -> Result<Reply, CacheError> {
    decode(&exchange(&request)?)
}

fn decode(bytes: &[u8]) -> Result<Reply, CacheError> {
    let reply: Reply = serde_json::from_slice(bytes)
        .map_err(|e| CacheError::Protocol(format!("malformed response: {e}")))?;
    match (reply.ok, reply.error) {
        (true, _) => Ok(Reply {
            error: None,
            ..reply
        }),
        (false, Some(Failure { kind, message })) => Err(match kind.as_str() {
            "unavailable" => CacheError::Unavailable,
            "timeout" => CacheError::Timeout,
            "invalid_key" => CacheError::InvalidKey(message),
            "invalid_ttl" => CacheError::InvalidTtl(message),
            "too_large" => CacheError::TooLarge,
            _ => CacheError::Protocol(message),
        }),
        (false, None) => Err(CacheError::Protocol(
            "a failed response carries no error".into(),
        )),
    }
}

#[cfg(target_arch = "wasm32")]
fn exchange(request: &Value) -> Result<Vec<u8>, CacheError> {
    #[link(wasm_import_module = "env")]
    extern "C" {
        fn cache_call(ptr: i32, len: i32) -> i32;
    }
    let payload = request.to_string();
    let rc = unsafe { cache_call(payload.as_ptr() as i32, payload.len() as i32) };
    if rc < 0 {
        return Err(CacheError::Protocol(
            "the platform could not read the request (it may exceed the size limit)".into(),
        ));
    }
    crate::host_buffer::read_host_bytes()
        .ok_or_else(|| CacheError::Protocol("the platform returned no response".into()))
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange(request: &Value) -> Result<Vec<u8>, CacheError> {
    let host = crate::host::cache().ok_or(CacheError::Unavailable)?;
    serde_json::to_vec(&host.call(request))
        .map_err(|e| CacheError::Protocol(format!("malformed response: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(wire: Value) -> Result<Reply, CacheError> {
        decode(wire.to_string().as_bytes())
    }

    #[test]
    fn outside_the_platform_the_cache_is_unavailable() {
        assert_eq!(get("k"), Err(CacheError::Unavailable));
        assert_eq!(
            set("k", "v", Duration::from_secs(1)),
            Err(CacheError::Unavailable)
        );
        assert_eq!(get_json::<Value>("k"), Err(CacheError::Unavailable));
    }

    #[test]
    fn replies_decode_into_their_fields() {
        assert_eq!(
            reply(json!({"ok": true, "value": "v"})).map(|r| r.value),
            Ok(Some("v".into()))
        );
        assert_eq!(
            reply(json!({"ok": true, "value": null})).map(|r| r.value),
            Ok(None)
        );
        assert_eq!(
            reply(json!({"ok": true, "values": ["a", null]})).and_then(|r| r.field(|r| r.values)),
            Ok(vec![Some("a".into()), None])
        );
        assert_eq!(
            reply(json!({"ok": true, "count": 7})).and_then(|r| r.field(|r| r.count)),
            Ok(7)
        );
        assert!(matches!(
            reply(json!({"ok": true})).and_then(|r| r.field(|r| r.set)),
            Err(CacheError::Protocol(_))
        ));
    }

    #[test]
    fn failures_decode_into_their_kinds() {
        let failure = |kind: &str| {
            reply(json!({"ok": false, "error": {"kind": kind, "message": "m"}})).map(drop)
        };
        assert_eq!(failure("unavailable"), Err(CacheError::Unavailable));
        assert_eq!(failure("timeout"), Err(CacheError::Timeout));
        assert_eq!(
            failure("invalid_key"),
            Err(CacheError::InvalidKey("m".into()))
        );
        assert_eq!(
            failure("invalid_ttl"),
            Err(CacheError::InvalidTtl("m".into()))
        );
        assert_eq!(failure("too_large"), Err(CacheError::TooLarge));
        assert_eq!(
            failure("from_the_future"),
            Err(CacheError::Protocol("m".into()))
        );
        assert!(matches!(
            reply(json!({"ok": false})),
            Err(CacheError::Protocol(_))
        ));
        assert!(matches!(decode(b"not json"), Err(CacheError::Protocol(_))));
    }

    #[test]
    fn ttls_travel_in_milliseconds() {
        assert_eq!(millis(Duration::from_millis(1_500)), 1_500);
        assert_eq!(millis(Duration::MAX), u64::MAX);
    }
}
