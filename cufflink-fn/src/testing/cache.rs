//! Redis and the namespaced cache kept in memory.

use super::clock::FakeClock;
use crate::host::cache::CacheHost;
use crate::host::clock::ClockHost;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

/// Key prefixes of `redis::*` the platform keeps to itself in every mode
/// of the `redis_scope` policy.
const RESERVED_PREFIXES: [&str; 3] = ["auth:", "cufflink:", "t:"];
const RESERVED_KEY: &str = "reserved key";
const UNAVAILABLE: &str = "redis is not available";

const MAX_KEY_BYTES: usize = 512;
const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MIN_TTL_MS: u64 = 1_000;
const MAX_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
/// The platform's defaults of `CACHE_MGET_MAX_KEYS` and
/// `CACHE_MGET_MAX_BYTES`.
const MGET_MAX_KEYS: usize = 256;
const MGET_MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    value: String,
    /// Milliseconds since the Unix epoch; `None` never expires.
    expires_at: Option<i64>,
}

#[derive(Default)]
struct Store {
    redis: BTreeMap<String, Entry>,
    cache: BTreeMap<String, Entry>,
    unavailable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Get {
        key: String,
    },
    Mget {
        keys: Vec<String>,
    },
    Set {
        key: String,
        value: String,
        ttl_ms: u64,
    },
    SetNx {
        key: String,
        value: String,
        ttl_ms: u64,
    },
    Del {
        key: String,
    },
    Incr {
        key: String,
        by: i64,
        ttl_ms: u64,
    },
    Expire {
        key: String,
        ttl_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Failure {
    Unavailable,
    InvalidKey(String),
    InvalidTtl(u64),
    TooLarge(String),
    Protocol(String),
}

impl Failure {
    fn kind(&self) -> &'static str {
        match self {
            Failure::Unavailable => "unavailable",
            Failure::InvalidKey(_) => "invalid_key",
            Failure::InvalidTtl(_) => "invalid_ttl",
            Failure::TooLarge(_) => "too_large",
            Failure::Protocol(_) => "protocol",
        }
    }

    fn message(&self) -> String {
        match self {
            Failure::Unavailable => "the cache is not available".into(),
            Failure::InvalidKey(key) => format!(
                "invalid cache key {key:?}: 1 to {MAX_KEY_BYTES} bytes of printable ASCII without spaces"
            ),
            Failure::InvalidTtl(ms) => {
                format!("invalid ttl of {ms} ms: every entry expires within 1 s to 30 days")
            }
            Failure::TooLarge(message) | Failure::Protocol(message) => message.clone(),
        }
    }
}

fn valid_key(key: String) -> Result<String, Failure> {
    if (1..=MAX_KEY_BYTES).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_graphic()) {
        Ok(key)
    } else {
        Err(Failure::InvalidKey(key))
    }
}

fn valid_ttl(ttl_ms: u64) -> Result<i64, Failure> {
    (MIN_TTL_MS..=MAX_TTL_MS)
        .contains(&ttl_ms)
        .then(|| i64::try_from(ttl_ms).ok())
        .flatten()
        .ok_or(Failure::InvalidTtl(ttl_ms))
}

fn bounded_value(value: String) -> Result<String, Failure> {
    (value.len() <= MAX_VALUE_BYTES)
        .then_some(value)
        .ok_or_else(|| {
            Failure::TooLarge(format!("cache values are at most {MAX_VALUE_BYTES} bytes"))
        })
}

fn live(entries: &BTreeMap<String, Entry>, key: &str, now: i64) -> Option<Entry> {
    entries
        .get(key)
        .filter(|entry| entry.expires_at.is_none_or(|at| at > now))
        .cloned()
}

/// Both keyspaces of the platform's Redis: the raw one of `redis::*`, with
/// the prefixes the platform reserves refused, and the one of `cache::*`
/// with its limits on keys, values and lifetimes. Entries expire by the
/// clock given, so a test moves time to see a lock free itself. Clones
/// share one store.
#[derive(Clone)]
pub struct InMemoryCache {
    store: Rc<RefCell<Store>>,
    now: Rc<dyn Fn() -> i64>,
}

impl Default for InMemoryCache {
    fn default() -> Self {
        Self {
            store: Rc::default(),
            now: Rc::new(crate::util::system_millis),
        }
    }
}

impl InMemoryCache {
    /// A cache whose entries expire by the system's time.
    pub fn new() -> Self {
        Self::default()
    }

    /// A cache whose entries expire by `clock`.
    pub fn with_clock(clock: &FakeClock) -> Self {
        let clock = clock.clone();
        Self {
            store: Rc::default(),
            now: Rc::new(move || clock.now_millis()),
        }
    }

    /// Fail every call from now on as a Redis that cannot be reached, or
    /// answer again.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.store.borrow_mut().unavailable = unavailable;
    }

    /// What `cache::get` would answer.
    pub fn value(&self, key: &str) -> Option<String> {
        live(&self.store.borrow().cache, key, (self.now)()).map(|entry| entry.value)
    }

    /// What `redis::get` would answer.
    pub fn redis_value(&self, key: &str) -> Option<String> {
        live(&self.store.borrow().redis, key, (self.now)()).map(|entry| entry.value)
    }

    fn redis_key<'a>(&self, key: &'a str) -> Result<&'a str, String> {
        if self.store.borrow().unavailable {
            Err(UNAVAILABLE.to_string())
        } else if RESERVED_PREFIXES.iter().any(|p| key.starts_with(p)) {
            Err(RESERVED_KEY.to_string())
        } else {
            Ok(key)
        }
    }

    fn run(&self, request: Request) -> Result<Value, Failure> {
        let now = (self.now)();
        let mut store = self.store.borrow_mut();
        if store.unavailable {
            return Err(Failure::Unavailable);
        }
        let entries = &mut store.cache;
        match request {
            Request::Get { key } => {
                let value = live(entries, &valid_key(key)?, now).map(|entry| entry.value);
                Ok(json!({ "ok": true, "value": value }))
            }
            Request::Mget { keys } => {
                if keys.len() > MGET_MAX_KEYS {
                    return Err(Failure::TooLarge(format!(
                        "mget reads at most {MGET_MAX_KEYS} keys"
                    )));
                }
                let values = keys
                    .into_iter()
                    .map(|key| valid_key(key).map(|key| live(entries, &key, now)))
                    .map(|found| found.map(|entry| entry.map(|entry| entry.value)))
                    .collect::<Result<Vec<Option<String>>, _>>()?;
                let bytes: usize = values.iter().flatten().map(String::len).sum();
                if bytes > MGET_MAX_BYTES {
                    return Err(Failure::TooLarge(format!(
                        "mget reads at most {MGET_MAX_BYTES} bytes of values"
                    )));
                }
                Ok(json!({ "ok": true, "values": values }))
            }
            Request::Set { key, value, ttl_ms } => {
                let (key, value, ttl) =
                    (valid_key(key)?, bounded_value(value)?, valid_ttl(ttl_ms)?);
                entries.insert(
                    key,
                    Entry {
                        value,
                        expires_at: Some(now.saturating_add(ttl)),
                    },
                );
                Ok(json!({ "ok": true }))
            }
            Request::SetNx { key, value, ttl_ms } => {
                let (key, value, ttl) =
                    (valid_key(key)?, bounded_value(value)?, valid_ttl(ttl_ms)?);
                let free = live(entries, &key, now).is_none();
                if free {
                    entries.insert(
                        key,
                        Entry {
                            value,
                            expires_at: Some(now.saturating_add(ttl)),
                        },
                    );
                }
                Ok(json!({ "ok": true, "set": free }))
            }
            Request::Del { key } => {
                let key = valid_key(key)?;
                let existed = live(entries, &key, now).is_some();
                entries.remove(&key);
                Ok(json!({ "ok": true, "deleted": existed }))
            }
            Request::Incr { key, by, ttl_ms } => {
                let (key, ttl) = (valid_key(key)?, valid_ttl(ttl_ms)?);
                let current = live(entries, &key, now);
                let count = current
                    .as_ref()
                    .map_or(Ok(0), |entry| entry.value.parse::<i64>())
                    .ok()
                    .and_then(|count| count.checked_add(by))
                    .ok_or_else(|| {
                        Failure::Protocol("value is not an integer or out of range".into())
                    })?;
                entries.insert(
                    key,
                    Entry {
                        value: count.to_string(),
                        expires_at: current
                            .map_or(Some(now.saturating_add(ttl)), |entry| entry.expires_at),
                    },
                );
                Ok(json!({ "ok": true, "count": count }))
            }
            Request::Expire { key, ttl_ms } => {
                let (key, ttl) = (valid_key(key)?, valid_ttl(ttl_ms)?);
                let existing = live(entries, &key, now);
                let existed = existing.is_some();
                if let Some(entry) = existing {
                    entries.insert(
                        key,
                        Entry {
                            expires_at: Some(now.saturating_add(ttl)),
                            ..entry
                        },
                    );
                }
                Ok(json!({ "ok": true, "set": existed }))
            }
        }
    }
}

impl CacheHost for InMemoryCache {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        self.redis_key(key).map(|key| self.redis_value(key))
    }

    fn set(&self, key: &str, value: &str, ttl_secs: i32) -> bool {
        self.redis_key(key).is_ok_and(|key| {
            let expires_at = (ttl_secs > 0)
                .then(|| (self.now)().saturating_add(i64::from(ttl_secs).saturating_mul(1_000)));
            self.store.borrow_mut().redis.insert(
                key.to_string(),
                Entry {
                    value: value.to_string(),
                    expires_at,
                },
            );
            true
        })
    }

    fn del(&self, key: &str) -> bool {
        self.redis_key(key).is_ok_and(|key| {
            self.store.borrow_mut().redis.remove(key);
            true
        })
    }

    fn mget(&self, keys: &[&str]) -> Result<Vec<Option<String>>, String> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    fn call(&self, request: &Value) -> Value {
        serde_json::from_value::<Request>(request.clone())
            .map_err(|e| Failure::Protocol(format!("malformed cache_call request: {e}")))
            .and_then(|request| self.run(request))
            .unwrap_or_else(|failure| {
                json!({
                    "ok": false,
                    "error": { "kind": failure.kind(), "message": failure.message() },
                })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::{self, HostSet};
    use crate::redis;

    fn hosted<T>(cache: &InMemoryCache, body: impl FnOnce() -> T) -> T {
        let hosts = HostSet {
            cache: Some(Rc::new(cache.clone())),
            ..Default::default()
        };
        host::with(hosts, body)
    }

    #[test]
    fn redis_holds_what_was_set_until_it_expires() {
        let clock = FakeClock::at(0);
        let cache = InMemoryCache::with_clock(&clock);
        hosted(&cache, || {
            assert!(redis::set("flags:a", "on", 60));
            assert!(redis::set("flags:b", "", 0));
            assert_eq!(redis::get("flags:a").as_deref(), Some("on"));
            assert_eq!(redis::get_with_status("flags:b"), Ok(Some(String::new())));
            assert_eq!(redis::get_with_status("flags:c"), Ok(None));
            assert_eq!(
                redis::mget(&["flags:a", "flags:c", "flags:b"]),
                Ok(vec![Some("on".into()), None, Some(String::new())])
            );
            clock.advance(60_000);
            assert_eq!(redis::get("flags:a"), None, "its minute is over");
            assert_eq!(redis::get("flags:b").as_deref(), Some(""), "never expires");
            assert!(redis::del("flags:b") && redis::del("flags:b"));
            assert_eq!(redis::get("flags:b"), None);
        });
        assert_eq!(cache.redis_value("flags:b"), None);
        assert_eq!(cache.value("flags:a"), None, "another keyspace");
    }

    #[test]
    fn the_platforms_own_keys_are_refused() {
        let cache = InMemoryCache::new();
        hosted(&cache, || {
            for key in ["cufflink:cache:x", "auth:perms:u-1", "t:acme:k"] {
                assert!(!redis::set(key, "v", 0), "{key}");
                assert!(!redis::del(key), "{key}");
                assert_eq!(redis::get(key), None, "{key}");
                assert_eq!(
                    redis::get_with_status(key),
                    Err("reserved key".to_string()),
                    "{key}"
                );
                assert_eq!(
                    redis::mget(&["ok", key]),
                    Err("reserved key".to_string()),
                    "{key}"
                );
            }
        });
    }

    #[test]
    fn a_redis_that_is_down_fails_every_call() {
        let cache = InMemoryCache::new();
        hosted(&cache, || {
            assert!(redis::set("k", "v", 0));
            cache.set_unavailable(true);
            assert!(!redis::set("k", "w", 0) && !redis::del("k"));
            assert_eq!(redis::get("k"), None);
            assert_eq!(
                redis::get_with_status("k"),
                Err("redis is not available".to_string())
            );
            cache.set_unavailable(false);
            assert_eq!(redis::get("k").as_deref(), Some("v"));
        });
    }

    #[cfg(feature = "cache")]
    mod namespaced {
        use super::*;
        use crate::cache::{self, CacheError};
        use std::time::Duration;

        const MINUTE: Duration = Duration::from_secs(60);

        #[test]
        fn a_lock_frees_itself_when_its_time_is_up() {
            let clock = FakeClock::at(1_000);
            let store = InMemoryCache::with_clock(&clock);
            hosted(&store, || {
                assert_eq!(cache::set_nx("lock:invoice:42", "1", MINUTE), Ok(true));
                assert_eq!(cache::set_nx("lock:invoice:42", "2", MINUTE), Ok(false));
                assert_eq!(cache::get("lock:invoice:42"), Ok(Some("1".into())));
                clock.advance(59_999);
                assert_eq!(cache::set_nx("lock:invoice:42", "2", MINUTE), Ok(false));
                clock.advance(1);
                assert_eq!(cache::get("lock:invoice:42"), Ok(None));
                assert_eq!(cache::set_nx("lock:invoice:42", "2", MINUTE), Ok(true));
                assert_eq!(cache::del("lock:invoice:42"), Ok(true));
                assert_eq!(cache::del("lock:invoice:42"), Ok(false));
            });
        }

        #[test]
        fn a_counter_keeps_the_expiry_it_was_created_with() {
            let clock = FakeClock::at(0);
            let store = InMemoryCache::with_clock(&clock);
            hosted(&store, || {
                assert_eq!(cache::incr("hits", 2, MINUTE), Ok(2));
                clock.advance(30_000);
                assert_eq!(cache::incr("hits", -5, Duration::from_secs(3_600)), Ok(-3));
                assert_eq!(
                    cache::mget(&["hits", "misses"]),
                    Ok(vec![Some("-3".into()), None])
                );
                clock.advance(30_000);
                assert_eq!(cache::get("hits"), Ok(None), "the first expiry held");
                assert_eq!(cache::set("hits", "many", MINUTE), Ok(()));
                assert!(matches!(
                    cache::incr("hits", 1, MINUTE),
                    Err(CacheError::Protocol(_))
                ));
                assert_eq!(cache::set("hits", &i64::MAX.to_string(), MINUTE), Ok(()));
                assert!(matches!(
                    cache::incr("hits", 1, MINUTE),
                    Err(CacheError::Protocol(_))
                ));
                assert_eq!(cache::expire("hits", Duration::from_secs(1)), Ok(true));
                assert_eq!(cache::expire("absent", MINUTE), Ok(false));
                clock.advance(1_000);
                assert_eq!(cache::get("hits"), Ok(None));
            });
        }

        #[test]
        fn json_values_round_trip() {
            let store = InMemoryCache::new();
            hosted(&store, || {
                let value = serde_json::json!({"book": 7, "tags": ["a"]});
                assert_eq!(cache::set_json("book:7", &value, MINUTE), Ok(()));
                assert_eq!(cache::get_json::<Value>("book:7"), Ok(Some(value)));
            });
            assert_eq!(
                store.value("book:7").as_deref(),
                Some(r#"{"book":7,"tags":["a"]}"#)
            );
        }

        #[test]
        fn the_platforms_limits_are_the_fakes() {
            let store = InMemoryCache::new();
            hosted(&store, || {
                for key in ["", "has space", "tab\tbed", "é", &"k".repeat(513)] {
                    assert!(
                        matches!(cache::get(key), Err(CacheError::InvalidKey(_))),
                        "{key:?}"
                    );
                }
                assert_eq!(cache::get(&"k".repeat(512)), Ok(None));
                for ttl in [
                    Duration::ZERO,
                    Duration::from_millis(999),
                    Duration::from_secs(2_592_001),
                ] {
                    assert!(
                        matches!(cache::set("k", "v", ttl), Err(CacheError::InvalidTtl(_))),
                        "{ttl:?}"
                    );
                }
                assert_eq!(cache::set("k", "v", Duration::from_secs(2_592_000)), Ok(()));
                assert_eq!(
                    cache::set("k", &"v".repeat(1024 * 1024 + 1), MINUTE),
                    Err(CacheError::TooLarge)
                );
                assert_eq!(cache::set("k", &"v".repeat(1024 * 1024), MINUTE), Ok(()));
                let many: Vec<String> = (0..257).map(|i| format!("k{i}")).collect();
                let many: Vec<&str> = many.iter().map(String::as_str).collect();
                assert_eq!(cache::mget(&many), Err(CacheError::TooLarge));
                assert_eq!(cache::mget(&many[..256]).map(|v| v.len()), Ok(256));
                assert_eq!(cache::mget(&[]), Ok(vec![]));
                store.set_unavailable(true);
                assert_eq!(cache::get("k"), Err(CacheError::Unavailable));
            });
            assert_eq!(cache::get("k"), Err(CacheError::Unavailable), "no host");
        }

        #[test]
        fn a_request_the_platform_could_not_read_is_a_protocol_failure() {
            let store = InMemoryCache::new();
            let reply = store.call(&json!({"op": "flush"}));
            assert_eq!(reply["ok"], false);
            assert_eq!(reply["error"]["kind"], "protocol");
            let reply = store.call(&json!({"op": "get", "key": "k", "extra": 1}));
            assert_eq!(reply["error"]["kind"], "protocol");
        }
    }
}
