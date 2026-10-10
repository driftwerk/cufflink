//! Redis and the namespaced cache.

use serde_json::Value;

/// What answers `redis::*` and `cache::*`: two keyspaces, as on the
/// platform.
pub trait CacheHost {
    /// `Ok(None)` for a key that is absent.
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    /// `ttl_secs` of `0` never expires.
    fn set(&self, key: &str, value: &str, ttl_secs: i32) -> bool;
    fn del(&self, key: &str) -> bool;
    /// A value or `None` for each key, in order.
    fn mget(&self, keys: &[&str]) -> Result<Vec<Option<String>>, String>;
    /// One `cache_call`: the request envelope in, the response envelope out.
    #[cfg_attr(not(feature = "cache"), allow(dead_code))]
    fn call(&self, request: &Value) -> Value;
}
