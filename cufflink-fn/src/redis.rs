//! Read and write values in Redis (backed by the platform's Redis connection).
//!
//! Use this for caching, session storage, or any key-value data that needs
//! to be shared across services or requests with low latency.

#[cfg(target_arch = "wasm32")]
use crate::{ffi, host_buffer};

/// Get a value from Redis by key. Returns `None` if the key doesn't exist
/// or Redis is not configured.
///
/// ```rust,ignore
/// if let Some(cached) = redis::get("auth:perms:user-123") {
///     log::info(&format!("Cache hit: {}", cached));
/// }
/// ```
pub fn get(key: &str) -> Option<String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = key.as_bytes();
        let result = unsafe { ffi::redis_get(bytes.as_ptr() as i32, bytes.len() as i32) };
        if result < 0 {
            return None;
        }
        host_buffer::read_host_string()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::cache().and_then(|host| host.get(key).ok().flatten())
    }
}

/// Set a value in Redis. Use `ttl_secs = 0` for no expiry.
///
/// Returns `true` on success, `false` on failure.
///
/// ```rust,ignore
/// redis::set("auth:perms:user-123", &perms_json, 3600); // 1 hour TTL
/// ```
pub fn set(key: &str, value: &str, ttl_secs: i32) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        let key_bytes = key.as_bytes();
        let val_bytes = value.as_bytes();
        let result = unsafe {
            ffi::redis_set(
                key_bytes.as_ptr() as i32,
                key_bytes.len() as i32,
                val_bytes.as_ptr() as i32,
                val_bytes.len() as i32,
                ttl_secs,
            )
        };
        result == 0
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::cache().is_none_or(|host| host.set(key, value, ttl_secs))
    }
}

/// Delete a key from Redis.
///
/// Returns `true` on success, `false` on failure.
///
/// ```rust,ignore
/// redis::del("auth:perms:user-123");
/// ```
pub fn del(key: &str) -> bool {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = key.as_bytes();
        let result = unsafe { ffi::redis_del(bytes.as_ptr() as i32, bytes.len() as i32) };
        result == 0
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::cache().is_none_or(|host| host.del(key))
    }
}

/// Like [`get`] but distinguishes "key absent" from "Redis error" so the
/// caller can choose its failure mode at the callsite.
///
/// * `Ok(Some(value))` — cache hit (value may be the empty string).
/// * `Ok(None)` — key is not in the cache.
/// * `Err(message)` — Redis is unavailable or returned an error.
pub fn get_with_status(key: &str) -> Result<Option<String>, String> {
    #[cfg(target_arch = "wasm32")]
    {
        let bytes = key.as_bytes();
        let code = unsafe { ffi::redis_get_status(bytes.as_ptr() as i32, bytes.len() as i32) };
        if code == 1 {
            return Ok(None);
        }
        let body = host_buffer::read_host_string().unwrap_or_default();
        if code < 0 {
            Err(err_from_response(body))
        } else {
            Ok(Some(body))
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::cache().map_or(Ok(None), |host| host.get(key))
    }
}

/// Fetch multiple keys in a single round-trip. Returns a vector parallel to
/// `keys`: `Some(value)` for hits, `None` for misses, in input order.
///
/// ```rust,ignore
/// let flags = redis::mget(&["feature_flags:default:a", "feature_flags:default:b"])?;
/// ```
pub fn mget(keys: &[&str]) -> Result<Vec<Option<String>>, String> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    #[cfg(target_arch = "wasm32")]
    {
        let payload = serde_json::to_string(keys).map_err(|e| e.to_string())?;
        let bytes = payload.as_bytes();
        let code = unsafe { ffi::redis_mget(bytes.as_ptr() as i32, bytes.len() as i32) };
        let response = host_buffer::read_host_string().unwrap_or_default();
        if code < 0 {
            return Err(err_from_response(response));
        }
        serde_json::from_str(&response).map_err(|e| e.to_string())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        crate::host::cache().map_or_else(|| Ok(vec![None; keys.len()]), |host| host.mget(keys))
    }
}

#[cfg(target_arch = "wasm32")]
fn err_from_response(body: String) -> String {
    if body.is_empty() {
        "redis error".to_string()
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use crate::redis;

    #[test]
    fn test_redis_get_noop_on_native() {
        let val = redis::get("some-key");
        assert!(val.is_none());
    }

    #[test]
    fn test_redis_set_noop_on_native() {
        let ok = redis::set("key", "value", 3600);
        assert!(ok);
    }

    #[test]
    fn test_redis_del_noop_on_native() {
        let ok = redis::del("key");
        assert!(ok);
    }

    #[test]
    fn redis_get_with_status_returns_none_outside_wasm() {
        assert_eq!(redis::get_with_status("any"), Ok(None));
    }

    #[test]
    fn redis_mget_returns_empty_for_empty_input() {
        assert_eq!(redis::mget(&[]), Ok(Vec::new()));
    }

    #[test]
    fn redis_mget_returns_misses_outside_wasm() {
        assert_eq!(redis::mget(&["a", "b", "c"]), Ok(vec![None, None, None]));
    }
}
