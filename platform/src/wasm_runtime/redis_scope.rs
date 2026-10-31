use crate::policy::PolicyMode;
use crate::redis_conn::{RedisCallError, RedisHandle};
use crate::wasm_runtime::budget::{Hold, HostCaps, Refused};
use redis::{FromRedisValue, RedisResult};
use uuid::Uuid;

pub const RESERVED_PREFIXES: [&str; 3] = ["auth:", "cufflink:", "t:"];

const TOMBSTONE: &[u8] = b"\xffcufflink:deleted";
const READ_ATTEMPTS: usize = 3;

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

    fn from_owned_redis_value(value: redis::Value) -> RedisResult<Self> {
        match value {
            redis::Value::BulkString(bytes) if bytes == TOMBSTONE => Ok(Stored::Deleted),
            other => String::from_owned_redis_value(other).map(Stored::Value),
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
    reading: Reading<'_>,
    hold: &mut Hold,
) -> Result<Read<Option<String>>, ReadError> {
    let mut reader = Reader::new(reading, hold);
    let primary = vec![plan.primary().to_string()];
    let stored: Option<Stored> = decoded(one(reader.values(redis, primary, true).await?))?;
    match (stored, plan.fallback()) {
        (None, Some(fallback)) => {
            let fallback = vec![fallback.to_string()];
            let value: Option<String> = decoded(one(reader.values(redis, fallback, true).await?))?;
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
    reading: Reading<'_>,
    hold: &mut Hold,
) -> Result<Read<Vec<Option<String>>>, ReadError> {
    let mut reader = Reader::new(reading, hold);
    let primaries: Vec<String> = plans.iter().map(|p| p.primary().to_string()).collect();
    let stored: Vec<Option<Stored>> = reader
        .values(redis, primaries, false)
        .await?
        .into_iter()
        .map(decoded)
        .collect::<Result<_, _>>()?;
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
    let fallbacks: Vec<Option<String>> = reader
        .values(redis, fallback_keys, false)
        .await?
        .into_iter()
        .map(decoded)
        .collect::<Result<_, _>>()?;
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

#[derive(Debug, Clone, Copy)]
pub enum Reading<'a> {
    Plain,
    Bounded {
        caps: &'a HostCaps,
        path: &'static str,
    },
}

impl<'a> Reading<'a> {
    pub fn of(caps: &'a HostCaps, path: &'static str) -> Self {
        match caps.enforces() {
            true => Self::Bounded { caps, path },
            false => Self::Plain,
        }
    }
}

struct Reader<'a> {
    reading: Reading<'a>,
    hold: &'a mut Hold,
    read: usize,
}

impl<'a> Reader<'a> {
    fn new(reading: Reading<'a>, hold: &'a mut Hold) -> Self {
        Self {
            reading,
            hold,
            read: 0,
        }
    }

    async fn values(
        &mut self,
        redis: &RedisHandle,
        keys: Vec<String>,
        single: bool,
    ) -> Result<Vec<redis::Value>, ReadError> {
        use redis::AsyncCommands;
        let Reading::Bounded { caps, path } = self.reading else {
            return Ok(redis
                .call(|mut conn| async move {
                    match (single, keys.as_slice()) {
                        (true, [key]) => conn.get(key).await.map(|value| vec![value]),
                        _ => conn.mget(&keys).await,
                    }
                })
                .await?);
        };
        rereading(async || self.bounded(redis, &keys, single, caps, path).await).await
    }

    async fn bounded(
        &mut self,
        redis: &RedisHandle,
        keys: &[String],
        single: bool,
        caps: &HostCaps,
        path: &'static str,
    ) -> Result<Vec<redis::Value>, ReadError> {
        let sizes = measured(redis, keys, single).await?;
        let reserved = self.reserve(caps, path, &sizes)?;
        let values = ranges(redis, keys, &sizes, single).await?;
        let grew = values
            .iter()
            .zip(&sizes)
            .any(|(value, size)| value_len(value) > *size);
        let read = match grew {
            true => 0,
            false => values.iter().map(value_len).sum(),
        };
        self.hold.shrink_to(
            self.hold
                .bytes()
                .saturating_sub(reserved.saturating_sub(read)),
        );
        if grew {
            self.read = self.read.saturating_sub(sizes.iter().sum());
            return Err(ReadError::Changed);
        }
        Ok(values)
    }

    fn reserve(
        &mut self,
        caps: &HostCaps,
        path: &'static str,
        sizes: &[usize],
    ) -> Result<usize, ReadError> {
        let bytes: usize = sizes.iter().sum();
        self.read = self.read.saturating_add(bytes);
        caps.response(path, self.read)
            .map_err(|cap| ReadError::Refused(Refused::Cap(cap)))?;
        let reserved = bytes.saturating_mul(2).saturating_add(sizes.len());
        self.hold
            .grow(reserved)
            .map_err(|capacity| ReadError::Refused(Refused::Budget(capacity)))?;
        Ok(reserved)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error(transparent)]
    Redis(#[from] RedisCallError),
    #[error("{0}")]
    Refused(Refused),
    #[error("a Redis value changed while it was read; read it again")]
    Changed,
}

impl From<redis::RedisError> for ReadError {
    fn from(error: redis::RedisError) -> Self {
        Self::Redis(RedisCallError::Redis(error))
    }
}

async fn rereading<T>(mut read: impl AsyncFnMut() -> Result<T, ReadError>) -> Result<T, ReadError> {
    for _ in 1..READ_ATTEMPTS {
        match read().await {
            Err(ReadError::Changed) => {}
            settled => return settled,
        }
    }
    read().await
}

fn one(values: Vec<redis::Value>) -> redis::Value {
    values.into_iter().next().unwrap_or(redis::Value::Nil)
}

fn value_len(value: &redis::Value) -> usize {
    match value {
        redis::Value::BulkString(bytes) => bytes.len(),
        _ => 0,
    }
}

fn decoded<T: FromRedisValue>(value: redis::Value) -> Result<Option<T>, ReadError> {
    value
        .extract_error()
        .and_then(Option::<T>::from_owned_redis_value)
        .map_err(ReadError::from)
}

async fn transaction(
    redis: &RedisHandle,
    block: redis::Pipeline,
    commands: usize,
) -> Result<Vec<redis::Value>, ReadError> {
    use redis::aio::ConnectionLike;
    let exec = redis
        .call(|mut conn| async move {
            conn.req_packed_commands(&block, commands + 1, 1)
                .await
                .map(one)
        })
        .await?;
    match exec {
        redis::Value::Array(replies) if replies.len() == commands => Ok(replies),
        redis::Value::Array(_) => Err(protocol(
            "the bounded read's transaction answered an unexpected number of replies",
        )),
        redis::Value::ServerError(error) => Err(redis::RedisError::from(error).into()),
        _ => Err(protocol("the bounded read's transaction was aborted")),
    }
}

fn protocol(what: &'static str) -> ReadError {
    redis::RedisError::from((redis::ErrorKind::ResponseError, what)).into()
}

fn block<T>(
    items: impl IntoIterator<Item = T>,
    queue: impl Fn(&mut redis::Pipeline, T),
) -> redis::Pipeline {
    let mut block = redis::pipe();
    block.cmd("MULTI");
    for item in items {
        queue(&mut block, item);
    }
    block.cmd("EXEC");
    block
}

async fn measured(
    redis: &RedisHandle,
    keys: &[String],
    single: bool,
) -> Result<Vec<usize>, ReadError> {
    let block = block(keys, |block, key| {
        block.cmd("STRLEN").arg(key);
    });
    transaction(redis, block, keys.len())
        .await?
        .into_iter()
        .map(|reply| match reply {
            redis::Value::Int(len) => Ok(usize::try_from(len).unwrap_or_default()),
            redis::Value::ServerError(wrong_type) if single => {
                Err(redis::RedisError::from(wrong_type).into())
            }
            redis::Value::ServerError(_) => Ok(0),
            _ => Err(protocol("STRLEN answered something other than a length")),
        })
        .collect()
}

async fn ranges(
    redis: &RedisHandle,
    keys: &[String],
    sizes: &[usize],
    single: bool,
) -> Result<Vec<redis::Value>, ReadError> {
    let block = block(keys.iter().zip(sizes), |block, (key, size)| {
        block.cmd("EXISTS").arg(key);
        block.cmd("GETRANGE").arg(key).arg(0).arg(*size);
    });
    let mut replies = transaction(redis, block, 2 * keys.len()).await?.into_iter();
    std::iter::from_fn(|| replies.next().zip(replies.next()))
        .map(|reply| match reply {
            (redis::Value::Int(0), _) => Ok(redis::Value::Nil),
            (_, value) => match value.extract_error() {
                Ok(value) => Ok(value),
                Err(wrong_type) if single => Err(wrong_type),
                Err(_) => Ok(redis::Value::Nil),
            },
        })
        .collect::<RedisResult<_>>()
        .map_err(ReadError::from)
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

    #[tokio::test]
    async fn a_value_that_changed_is_read_again_and_only_the_last_change_is_answered() {
        let mut attempts = 0;
        let settled = rereading(async || {
            attempts += 1;
            match attempts {
                1 | 2 => Err(ReadError::Changed),
                _ => Ok(attempts),
            }
        })
        .await;
        assert_eq!(settled.expect("the third read"), READ_ATTEMPTS);

        let mut attempts = 0;
        let changing: Result<(), ReadError> = rereading(async || {
            attempts += 1;
            Err(ReadError::Changed)
        })
        .await;
        assert!(matches!(changing, Err(ReadError::Changed)));
        assert_eq!(attempts, READ_ATTEMPTS);

        let mut attempts = 0;
        let refused: Result<(), ReadError> = rereading(async || {
            attempts += 1;
            Err(protocol("not a race"))
        })
        .await;
        assert!(matches!(refused, Err(ReadError::Redis(_))));
        assert_eq!(attempts, 1, "only a change is read again");
    }

    #[test]
    fn prefix_uses_tenant_id() {
        let id = Uuid::new_v4();
        assert_eq!(prefix(id), format!("t:{id}:"));
    }
}
