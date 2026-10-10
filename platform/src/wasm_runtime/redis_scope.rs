use crate::policy::PolicyMode;
use crate::redis_conn::{RedisCallError, RedisHandle};
use redis::{FromRedisValue, RedisResult};
use uuid::Uuid;

pub const RESERVED_PREFIXES: [&str; 3] = ["auth:", "cufflink:", "t:"];

const TOMBSTONE: &[u8] = b"\xffcufflink:deleted";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Stored {
    Value(String),
    Deleted,
}

impl Stored {
    fn value(self) -> Option<String> {
        match self {
            Stored::Value(value) => Some(value),
            Stored::Deleted => None,
        }
    }
}

impl FromRedisValue for Stored {
    fn from_redis_value(value: &redis::Value) -> RedisResult<Self> {
        match value {
            redis::Value::BulkString(bytes) if bytes == TOMBSTONE => Ok(Stored::Deleted),
            other => String::from_redis_value(other).map(Stored::Value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyPlan {
    Raw(String),
    Prefixed {
        primary: String,
        fallback: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReservedKey(pub String);

pub fn prefix(tenant_id: Uuid) -> String {
    format!("t:{tenant_id}:")
}

pub fn plan(mode: PolicyMode, tenant_id: Uuid, key: &str) -> Result<KeyPlan, ReservedKey> {
    if RESERVED_PREFIXES.iter().any(|p| key.starts_with(p)) {
        return Err(ReservedKey(key.to_string()));
    }
    let primary = || format!("{}{key}", prefix(tenant_id));
    Ok(match mode {
        PolicyMode::Off => KeyPlan::Raw(key.to_string()),
        PolicyMode::Warn => KeyPlan::Prefixed {
            primary: primary(),
            fallback: Some(key.to_string()),
        },
        PolicyMode::Enforce => KeyPlan::Prefixed {
            primary: primary(),
            fallback: None,
        },
    })
}

impl KeyPlan {
    pub fn primary(&self) -> &str {
        match self {
            KeyPlan::Raw(key) => key,
            KeyPlan::Prefixed { primary, .. } => primary,
        }
    }

    pub fn fallback(&self) -> Option<&str> {
        match self {
            KeyPlan::Raw(_) => None,
            KeyPlan::Prefixed { fallback, .. } => fallback.as_deref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Read<T> {
    pub value: T,
    pub from_fallback: bool,
}

pub async fn get(
    redis: &RedisHandle,
    plan: &KeyPlan,
) -> Result<Read<Option<String>>, RedisCallError> {
    use redis::AsyncCommands;
    let primary = plan.primary().to_string();
    let stored: Option<Stored> = redis
        .call(|mut conn| async move { conn.get(&primary).await })
        .await?;
    match (stored, plan.fallback()) {
        (None, Some(fallback)) => {
            let fallback = fallback.to_string();
            let value: Option<String> = redis
                .call(|mut conn| async move { conn.get(&fallback).await })
                .await?;
            Ok(Read {
                from_fallback: value.is_some(),
                value,
            })
        }
        (stored, _) => Ok(Read {
            value: stored.and_then(Stored::value),
            from_fallback: false,
        }),
    }
}

pub async fn set(
    redis: &RedisHandle,
    plan: &KeyPlan,
    value: &str,
    ttl_secs: Option<u64>,
) -> Result<(), RedisCallError> {
    use redis::AsyncCommands;
    let key = plan.primary().to_string();
    let value = value.to_string();
    redis
        .call(|mut conn| async move {
            match ttl_secs {
                Some(ttl) => conn.set_ex::<_, _, ()>(&key, &value, ttl).await,
                None => conn.set::<_, _, ()>(&key, &value).await,
            }
        })
        .await
}

pub async fn del(redis: &RedisHandle, plan: &KeyPlan) -> Result<(), RedisCallError> {
    use redis::AsyncCommands;
    let (key, fallback) = (
        plan.primary().to_string(),
        plan.fallback().map(str::to_string),
    );
    redis
        .call(|mut conn| async move {
            let lifetime = match &fallback {
                Some(fallback) => Some(conn.pttl::<_, i64>(fallback).await?),
                None => None,
            };
            match lifetime {
                Some(-1) => conn.set::<_, _, ()>(&key, TOMBSTONE).await,
                Some(ms) if ms > 0 => {
                    conn.pset_ex::<_, _, ()>(&key, TOMBSTONE, ms.unsigned_abs())
                        .await
                }
                Some(_) | None => conn.del::<_, ()>(&key).await,
            }
        })
        .await
}

pub async fn mget(
    redis: &RedisHandle,
    plans: &[KeyPlan],
) -> Result<Read<Vec<Option<String>>>, RedisCallError> {
    use redis::AsyncCommands;
    let primaries: Vec<String> = plans.iter().map(|p| p.primary().to_string()).collect();
    let stored: Vec<Option<Stored>> = redis
        .call(|mut conn| async move { conn.mget(&primaries).await })
        .await?;
    let missing: Vec<(usize, String)> = stored
        .iter()
        .zip(plans)
        .enumerate()
        .filter_map(|(i, (stored, plan))| {
            stored
                .is_none()
                .then(|| plan.fallback().map(|f| (i, f.to_string())))
                .flatten()
        })
        .collect();
    let values: Vec<Option<String>> = stored
        .into_iter()
        .map(|stored| stored.and_then(Stored::value))
        .collect();
    if missing.is_empty() {
        return Ok(Read {
            value: values,
            from_fallback: false,
        });
    }
    let fallback_keys: Vec<String> = missing.iter().map(|(_, key)| key.clone()).collect();
    let fallbacks: Vec<Option<String>> = redis
        .call(|mut conn| async move { conn.mget(&fallback_keys).await })
        .await?;
    let from_fallback = fallbacks.iter().any(Option::is_some);
    let mut merged = values;
    for ((i, _), value) in missing.into_iter().zip(fallbacks) {
        merged[i] = value;
    }
    Ok(Read {
        value: merged,
        from_fallback,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_prefixes_rejected_in_every_mode() {
        for mode in PolicyMode::ALL {
            for key in [
                "auth:perms:t:u",
                "cufflink:x",
                "t:00000000-0000-0000-0000-000000000000:k",
            ] {
                assert_eq!(
                    plan(mode, Uuid::nil(), key),
                    Err(ReservedKey(key.to_string())),
                    "{mode} {key}"
                );
            }
        }
    }

    #[test]
    fn off_is_raw() {
        assert_eq!(
            plan(PolicyMode::Off, Uuid::nil(), "feature_flags:default:x"),
            Ok(KeyPlan::Raw("feature_flags:default:x".into()))
        );
    }

    #[test]
    fn warn_prefixed_with_fallback() {
        let planned = plan(PolicyMode::Warn, Uuid::nil(), "feature_flags:default:x").expect("plan");
        assert_eq!(
            planned.primary(),
            "t:00000000-0000-0000-0000-000000000000:feature_flags:default:x"
        );
        assert_eq!(planned.fallback(), Some("feature_flags:default:x"));
    }

    #[test]
    fn enforce_prefixed_without_fallback() {
        let planned = plan(PolicyMode::Enforce, Uuid::nil(), "k").expect("plan");
        assert_eq!(planned.fallback(), None);
    }

    #[test]
    fn a_tombstone_reads_as_deleted_and_every_other_value_as_itself() {
        let read =
            |bytes: &[u8]| Stored::from_redis_value(&redis::Value::BulkString(bytes.to_vec()));
        assert_eq!(read(TOMBSTONE).expect("a tombstone"), Stored::Deleted);
        assert_eq!(read(TOMBSTONE).expect("a tombstone").value(), None);
        assert_eq!(
            read(b"cufflink:deleted").expect("a value"),
            Stored::Value("cufflink:deleted".into()),
            "what a guest can write is a value"
        );
        assert!(
            String::from_utf8(TOMBSTONE.to_vec()).is_err(),
            "no guest value is a tombstone"
        );
        assert_eq!(
            Option::<Stored>::from_redis_value(&redis::Value::Nil).expect("absent"),
            None
        );
    }

    #[test]
    fn prefix_uses_tenant_id() {
        let id = Uuid::new_v4();
        assert_eq!(prefix(id), format!("t:{id}:"));
    }
}
