use super::{block_on_bounded, read_guest_bytes, wrap, HostIoError, HostState};
use crate::redis_conn::{RedisCallError, RedisHandle};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Instant;
use uuid::Uuid;
use wasmtime::{Caller, Linker};

const MAX_KEY_BYTES: usize = 512;
const MAX_VALUE_BYTES: usize = 1024 * 1024;
const MIN_TTL_MS: u64 = 1_000;
const MAX_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const MGET_KEYS_CEILING: usize = 4_096;

static INCR: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r"
local count = redis.call('INCRBY', KEYS[1], ARGV[1])
if redis.call('PTTL', KEYS[1]) == -1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return count
",
    )
});

static BOUNDED_MGET: LazyLock<redis::Script> = LazyLock::new(|| {
    redis::Script::new(
        r"
local total = 0
for _, key in ipairs(KEYS) do
  total = total + redis.call('STRLEN', key)
  if total > tonumber(ARGV[1]) then
    return false
  end
end
return redis.call('MGET', unpack(KEYS))
",
    )
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheLimits {
    pub mget_max_keys: usize,
    pub mget_max_bytes: usize,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            mget_max_keys: 256,
            mget_max_bytes: 8 * 1024 * 1024,
        }
    }
}

impl CacheLimits {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        let limits = Self {
            mget_max_keys: vars.positive("CACHE_MGET_MAX_KEYS", defaults.mget_max_keys)?,
            mget_max_bytes: vars.positive("CACHE_MGET_MAX_BYTES", defaults.mget_max_bytes)?,
        };
        if limits.mget_max_keys > MGET_KEYS_CEILING {
            eyre::bail!("CACHE_MGET_MAX_KEYS must be at most {MGET_KEYS_CEILING}");
        }
        Ok(limits)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum CacheRequest {
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

impl CacheRequest {
    fn op(&self) -> &'static str {
        match self {
            CacheRequest::Get { .. } => "get",
            CacheRequest::Mget { .. } => "mget",
            CacheRequest::Set { .. } => "set",
            CacheRequest::SetNx { .. } => "set_nx",
            CacheRequest::Del { .. } => "del",
            CacheRequest::Incr { .. } => "incr",
            CacheRequest::Expire { .. } => "expire",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CacheFailure {
    Unavailable,
    Timeout,
    InvalidKey(String),
    InvalidTtl(u64),
    TooLarge(String),
    Protocol(String),
}

impl CacheFailure {
    fn kind(&self) -> &'static str {
        match self {
            CacheFailure::Unavailable => "unavailable",
            CacheFailure::Timeout => "timeout",
            CacheFailure::InvalidKey(_) => "invalid_key",
            CacheFailure::InvalidTtl(_) => "invalid_ttl",
            CacheFailure::TooLarge(_) => "too_large",
            CacheFailure::Protocol(_) => "protocol",
        }
    }

    fn message(&self) -> String {
        match self {
            CacheFailure::Unavailable => "the cache is not available".into(),
            CacheFailure::Timeout => "the cache did not answer in time".into(),
            CacheFailure::InvalidKey(key) => format!(
                "invalid cache key {key:?}: 1 to {MAX_KEY_BYTES} bytes of printable ASCII without spaces"
            ),
            CacheFailure::InvalidTtl(ms) => {
                format!("invalid ttl of {ms} ms: every entry expires within 1 s to 30 days")
            }
            CacheFailure::TooLarge(message) | CacheFailure::Protocol(message) => message.clone(),
        }
    }
}

impl From<RedisCallError> for CacheFailure {
    fn from(error: RedisCallError) -> Self {
        match error {
            RedisCallError::TimedOut(_) => CacheFailure::Timeout,
            RedisCallError::Redis(e) if e.is_timeout() => CacheFailure::Timeout,
            e if e.is_unavailable() => CacheFailure::Unavailable,
            e => CacheFailure::Protocol(e.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CacheReply {
    Value(Option<String>),
    Values(Vec<Option<String>>),
    Stored,
    Set(bool),
    Deleted(bool),
    Count(i64),
}

struct Namespace {
    prefix: String,
}

impl Namespace {
    fn new(tenant_id: Uuid, service: &str) -> Self {
        Self {
            prefix: format!("cufflink:cache:{tenant_id}:{service}:"),
        }
    }

    fn key(&self, key: &str) -> Result<String, CacheFailure> {
        let valid =
            (1..=MAX_KEY_BYTES).contains(&key.len()) && key.bytes().all(|b| b.is_ascii_graphic());
        valid
            .then(|| format!("{}{key}", self.prefix))
            .ok_or_else(|| CacheFailure::InvalidKey(key.to_string()))
    }
}

fn ttl(ttl_ms: u64) -> Result<u64, CacheFailure> {
    (MIN_TTL_MS..=MAX_TTL_MS)
        .contains(&ttl_ms)
        .then_some(ttl_ms)
        .ok_or(CacheFailure::InvalidTtl(ttl_ms))
}

fn bounded_value(value: String) -> Result<String, CacheFailure> {
    (value.len() <= MAX_VALUE_BYTES)
        .then_some(value)
        .ok_or_else(|| {
            CacheFailure::TooLarge(format!("cache values are at most {MAX_VALUE_BYTES} bytes"))
        })
}

fn bounded_keys(keys: Vec<String>, limits: CacheLimits) -> Result<Vec<String>, CacheFailure> {
    (keys.len() <= limits.mget_max_keys)
        .then_some(keys)
        .ok_or_else(|| {
            CacheFailure::TooLarge(format!("mget reads at most {} keys", limits.mget_max_keys))
        })
}

async fn run(
    redis: &RedisHandle,
    namespace: &Namespace,
    limits: CacheLimits,
    request: CacheRequest,
) -> Result<CacheReply, CacheFailure> {
    use redis::AsyncCommands;
    match request {
        CacheRequest::Get { key } => {
            let key = namespace.key(&key)?;
            redis
                .call(|mut c| async move { c.get(&key).await })
                .await
                .map(CacheReply::Value)
                .map_err(Into::into)
        }
        CacheRequest::Mget { keys } => {
            let keys = bounded_keys(keys, limits)?
                .iter()
                .map(|key| namespace.key(key))
                .collect::<Result<Vec<_>, _>>()?;
            if keys.is_empty() {
                return Ok(CacheReply::Values(Vec::new()));
            }
            let values: Option<Vec<Option<String>>> = redis
                .call(|mut c| async move {
                    BOUNDED_MGET
                        .key(&keys)
                        .arg(limits.mget_max_bytes)
                        .invoke_async(&mut c)
                        .await
                })
                .await?;
            values.map(CacheReply::Values).ok_or_else(|| {
                CacheFailure::TooLarge(format!(
                    "mget reads at most {} bytes of values",
                    limits.mget_max_bytes
                ))
            })
        }
        CacheRequest::Set { key, value, ttl_ms } => {
            let (key, value, ttl_ms) = (namespace.key(&key)?, bounded_value(value)?, ttl(ttl_ms)?);
            redis
                .call(|mut c| async move { c.pset_ex::<_, _, ()>(&key, &value, ttl_ms).await })
                .await?;
            Ok(CacheReply::Stored)
        }
        CacheRequest::SetNx { key, value, ttl_ms } => {
            let (key, value, ttl_ms) = (namespace.key(&key)?, bounded_value(value)?, ttl(ttl_ms)?);
            let set: Option<String> = redis
                .call(|mut c| async move {
                    redis::cmd("SET")
                        .arg(&key)
                        .arg(&value)
                        .arg("NX")
                        .arg("PX")
                        .arg(ttl_ms)
                        .query_async(&mut c)
                        .await
                })
                .await?;
            Ok(CacheReply::Set(set.is_some()))
        }
        CacheRequest::Del { key } => {
            let key = namespace.key(&key)?;
            let deleted: i64 = redis.call(|mut c| async move { c.del(&key).await }).await?;
            Ok(CacheReply::Deleted(deleted > 0))
        }
        CacheRequest::Incr { key, by, ttl_ms } => {
            let (key, ttl_ms) = (namespace.key(&key)?, ttl(ttl_ms)?);
            let count: i64 = redis
                .call(|mut c| async move {
                    INCR.key(&key)
                        .arg(by)
                        .arg(ttl_ms)
                        .invoke_async(&mut c)
                        .await
                })
                .await?;
            Ok(CacheReply::Count(count))
        }
        CacheRequest::Expire { key, ttl_ms } => {
            let (key, ttl_ms) = (namespace.key(&key)?, ttl(ttl_ms)?);
            let millis = i64::try_from(ttl_ms).map_err(|_| CacheFailure::InvalidTtl(ttl_ms))?;
            redis
                .call(|mut c| async move { c.pexpire(&key, millis).await })
                .await
                .map(CacheReply::Set)
                .map_err(Into::into)
        }
    }
}

fn envelope(outcome: Result<CacheReply, CacheFailure>) -> Value {
    match outcome {
        Ok(CacheReply::Value(value)) => json!({ "ok": true, "value": value }),
        Ok(CacheReply::Values(values)) => json!({ "ok": true, "values": values }),
        Ok(CacheReply::Stored) => json!({ "ok": true }),
        Ok(CacheReply::Set(set)) => json!({ "ok": true, "set": set }),
        Ok(CacheReply::Deleted(deleted)) => json!({ "ok": true, "deleted": deleted }),
        Ok(CacheReply::Count(count)) => json!({ "ok": true, "count": count }),
        Err(failure) => json!({
            "ok": false,
            "error": { "kind": failure.kind(), "message": failure.message() },
        }),
    }
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "cache_call",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let bytes = host_try!(read_guest_bytes(&mut caller, "cache_call", ptr, len));
            let started = Instant::now();
            let (op, outcome) = match serde_json::from_slice::<CacheRequest>(&bytes) {
                Ok(request) => {
                    let state = caller.data();
                    let namespace =
                        Namespace::new(state.identity.tenant_id, &state.identity.service_name);
                    let op = request.op();
                    let outcome = block_on_bounded(
                        &state.bounds(),
                        run(
                            &state.app_state.redis,
                            &namespace,
                            state.app_state.config.cache,
                            request,
                        ),
                    )
                    .map_err(|e| match e {
                        HostIoError::TimedOut(_) | HostIoError::Cancelled => CacheFailure::Timeout,
                        HostIoError::Failed(failure) => failure,
                    });
                    (op, outcome)
                }
                Err(e) => (
                    "invalid",
                    Err(CacheFailure::Protocol(format!(
                        "malformed cache_call request: {e}"
                    ))),
                ),
            };
            let result = outcome.as_ref().err().map_or("ok", CacheFailure::kind);
            metrics::counter!("cufflink_cache_calls_total", "op" => op, "result" => result)
                .increment(1);
            metrics::histogram!("cufflink_cache_call_duration_seconds", "op" => op)
                .record(started.elapsed().as_secs_f64());
            caller.data_mut().host_response = Some(envelope(outcome).to_string());
            0
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_namespaced_by_tenant_and_service() {
        let tenant = Uuid::nil();
        assert_eq!(
            Namespace::new(tenant, "orders").key("user:1").ok(),
            Some(format!("cufflink:cache:{tenant}:orders:user:1"))
        );
        assert_ne!(
            Namespace::new(tenant, "orders").key("k").ok(),
            Namespace::new(tenant, "billing").key("k").ok()
        );
        assert_ne!(
            Namespace::new(tenant, "orders").key("k").ok(),
            Namespace::new(Uuid::from_u128(1), "orders").key("k").ok()
        );
    }

    #[test]
    fn keys_must_be_short_printable_ascii() {
        let ns = Namespace::new(Uuid::nil(), "svc");
        for bad in [
            "",
            "has space",
            "tab\there",
            "new\nline",
            "émoji",
            &"k".repeat(513),
        ] {
            assert_eq!(
                ns.key(bad),
                Err(CacheFailure::InvalidKey(bad.to_string())),
                "{bad:?}"
            );
        }
        assert!(ns.key(&"k".repeat(512)).is_ok());
        assert!(ns.key("a:b/c-d_e.f~!").is_ok());
    }

    #[test]
    fn ttls_are_bounded_and_values_capped() {
        assert_eq!(ttl(999), Err(CacheFailure::InvalidTtl(999)));
        assert_eq!(ttl(0), Err(CacheFailure::InvalidTtl(0)));
        assert_eq!(ttl(1_000), Ok(1_000));
        assert_eq!(ttl(MAX_TTL_MS), Ok(MAX_TTL_MS));
        assert_eq!(
            ttl(MAX_TTL_MS + 1),
            Err(CacheFailure::InvalidTtl(MAX_TTL_MS + 1))
        );
        assert!(bounded_value("x".repeat(MAX_VALUE_BYTES)).is_ok());
        assert!(matches!(
            bounded_value("x".repeat(MAX_VALUE_BYTES + 1)),
            Err(CacheFailure::TooLarge(_))
        ));
    }

    #[test]
    fn mget_key_counts_are_capped_and_configurable() {
        let limits = CacheLimits {
            mget_max_keys: 2,
            ..CacheLimits::default()
        };
        let keys = |n: usize| vec!["k".to_string(); n];
        assert_eq!(bounded_keys(keys(2), limits), Ok(keys(2)));
        assert_eq!(
            bounded_keys(keys(3), limits).map_err(|e| (e.kind(), e.message())),
            Err(("too_large", "mget reads at most 2 keys".to_string()))
        );
        let from = |pairs: &[(&str, &str)]| {
            let pairs: Vec<(String, String)> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            CacheLimits::from_vars(move |name| {
                pairs
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
            })
        };
        assert_eq!(from(&[]).expect("defaults"), CacheLimits::default());
        assert_eq!(
            from(&[
                ("CACHE_MGET_MAX_KEYS", "10"),
                ("CACHE_MGET_MAX_BYTES", "1024")
            ])
            .expect("configured"),
            CacheLimits {
                mget_max_keys: 10,
                mget_max_bytes: 1024
            }
        );
        for (name, value) in [
            ("CACHE_MGET_MAX_KEYS", "0"),
            ("CACHE_MGET_MAX_KEYS", "4097"),
            ("CACHE_MGET_MAX_BYTES", "0"),
            ("CACHE_MGET_MAX_BYTES", "lots"),
        ] {
            let err = from(&[(name, value)]).expect_err(value);
            assert!(err.to_string().contains(name), "{err}");
        }
    }

    #[test]
    fn namespaces_cannot_overlap() {
        let tenant = Uuid::from_u128(7);
        assert!(!tenant.to_string().contains(':'));
        assert!(!cufflink_types::slug::is_valid_service_name("orders:x"));
        let orders = Namespace::new(tenant, "orders").prefix;
        let orders_x = Namespace::new(tenant, "orders_x").prefix;
        assert!(!orders.starts_with(&orders_x) && !orders_x.starts_with(&orders));
        assert_eq!(orders.matches(':').count(), 4);
    }

    #[test]
    fn requests_parse_from_the_wire_shapes() {
        let parse = |v: Value| serde_json::from_value::<CacheRequest>(v);
        assert_eq!(
            parse(json!({"op": "set_nx", "key": "k", "value": "v", "ttl_ms": 5000})).ok(),
            Some(CacheRequest::SetNx {
                key: "k".into(),
                value: "v".into(),
                ttl_ms: 5_000
            })
        );
        assert_eq!(
            parse(json!({"op": "mget", "keys": ["a", "b"]})).ok(),
            Some(CacheRequest::Mget {
                keys: vec!["a".into(), "b".into()]
            })
        );
        assert_eq!(
            parse(json!({"op": "incr", "key": "k", "by": -2, "ttl_ms": 1000})).ok(),
            Some(CacheRequest::Incr {
                key: "k".into(),
                by: -2,
                ttl_ms: 1_000
            })
        );
        assert!(
            parse(json!({"op": "set", "key": "k", "value": "v"})).is_err(),
            "ttl required"
        );
        assert!(parse(json!({"op": "flushall"})).is_err());
    }

    #[test]
    fn envelopes_carry_ok_and_the_fields_or_the_error() {
        for (reply, wire) in [
            (CacheReply::Value(None), json!({"ok": true, "value": null})),
            (
                CacheReply::Values(vec![Some("a".into()), None]),
                json!({"ok": true, "values": ["a", null]}),
            ),
            (CacheReply::Stored, json!({"ok": true})),
            (CacheReply::Set(true), json!({"ok": true, "set": true})),
            (
                CacheReply::Deleted(false),
                json!({"ok": true, "deleted": false}),
            ),
            (CacheReply::Count(-3), json!({"ok": true, "count": -3})),
        ] {
            assert_eq!(envelope(Ok(reply)), wire);
        }
        assert_eq!(
            envelope(Err(CacheFailure::Unavailable)),
            json!({"ok": false, "error": {"kind": "unavailable", "message": "the cache is not available"}})
        );
        assert_eq!(
            envelope(Err(CacheFailure::InvalidTtl(5)))["error"]["kind"],
            "invalid_ttl"
        );
    }

    #[tokio::test]
    async fn without_redis_every_call_is_unavailable() {
        let namespace = Namespace::new(Uuid::nil(), "svc");
        for request in [
            CacheRequest::Get { key: "k".into() },
            CacheRequest::Set {
                key: "k".into(),
                value: "v".into(),
                ttl_ms: 1_000,
            },
        ] {
            assert_eq!(
                run(
                    &RedisHandle::disabled(),
                    &namespace,
                    CacheLimits::default(),
                    request
                )
                .await,
                Err(CacheFailure::Unavailable)
            );
        }
    }

    #[test]
    fn redis_failures_map_to_cache_failures() {
        assert_eq!(
            CacheFailure::from(RedisCallError::NotConfigured),
            CacheFailure::Unavailable
        );
        assert_eq!(
            CacheFailure::from(RedisCallError::Unavailable),
            CacheFailure::Unavailable
        );
        assert_eq!(
            CacheFailure::from(RedisCallError::TimedOut(std::time::Duration::from_millis(
                5
            ))),
            CacheFailure::Timeout
        );
        let wrong_type = redis::RedisError::from((redis::ErrorKind::TypeError, "WRONGTYPE"));
        assert!(matches!(
            CacheFailure::from(RedisCallError::Redis(wrong_type)),
            CacheFailure::Protocol(_)
        ));
    }
}
