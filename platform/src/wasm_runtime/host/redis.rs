use super::{block_on, read_guest_string, wrap, HostState};
use crate::policy::{self, PolicyKey, Violation};
use crate::wasm_runtime::redis_scope::{self, Reading};
use std::collections::HashSet;
use wasmtime::{Caller, Linker};

pub(crate) const RESERVED_REDIS_KEY: &str = "reserved key";

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "redis_get",
        |mut caller: Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
            let key = host_try!(read_guest_string(
                &mut caller,
                "redis_get",
                key_ptr,
                key_len
            ));
            let Some(plan) = caller.data().redis_plan(&key) else {
                return -1;
            };
            let app_state = caller.data().app_state.clone();
            match block_on(redis_scope::get(
                &app_state.redis,
                &plan,
                Reading::of(caller.data().caps()),
            )) {
                Ok(read) => {
                    caller.data().record_redis_fallback(read.from_fallback);
                    caller.data_mut().respond_with("redis_get", read.value)
                }
                Err(e) => {
                    tracing::error!("[wasm] redis_get error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "redis_set",
        |mut caller: Caller<'_, HostState>,
         key_ptr: i32,
         key_len: i32,
         val_ptr: i32,
         val_len: i32,
         ttl_secs: i32|
         -> i32 {
            let key = host_try!(read_guest_string(
                &mut caller,
                "redis_set",
                key_ptr,
                key_len
            ));
            let val = host_try!(read_guest_string(
                &mut caller,
                "redis_set",
                val_ptr,
                val_len
            ));
            let Some(plan) = caller.data().redis_plan(&key) else {
                return -1;
            };
            let app_state = caller.data().app_state.clone();
            let ttl = u64::try_from(ttl_secs).ok().filter(|ttl| *ttl > 0);
            match block_on(redis_scope::set(&app_state.redis, &plan, &val, ttl)) {
                Ok(()) => 0,
                Err(e) => {
                    tracing::error!("[wasm] redis_set error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "redis_del",
        |mut caller: Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
            let key = host_try!(read_guest_string(
                &mut caller,
                "redis_del",
                key_ptr,
                key_len
            ));
            let Some(plan) = caller.data().redis_plan(&key) else {
                return -1;
            };
            let app_state = caller.data().app_state.clone();
            match block_on(redis_scope::del(&app_state.redis, &plan)) {
                Ok(()) => 0,
                Err(e) => {
                    tracing::error!("[wasm] redis_del error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "redis_get_status",
        |mut caller: Caller<'_, HostState>, key_ptr: i32, key_len: i32| -> i32 {
            let key = match read_guest_string(&mut caller, "redis_get_status", key_ptr, key_len) {
                Ok(s) => s,
                Err(e) => {
                    caller
                        .data_mut()
                        .respond("redis_get_status", "invalid key buffer".into());
                    return e.code();
                }
            };
            let Some(plan) = caller.data().redis_plan(&key) else {
                caller
                    .data_mut()
                    .respond("redis_get_status", RESERVED_REDIS_KEY.into());
                return -1;
            };
            let app_state = caller.data().app_state.clone();
            match block_on(redis_scope::get(
                &app_state.redis,
                &plan,
                Reading::of(caller.data().caps()),
            )) {
                Ok(read) => {
                    caller.data().record_redis_fallback(read.from_fallback);
                    let found = read.value.is_some();
                    match caller
                        .data_mut()
                        .respond_with("redis_get_status", read.value)
                    {
                        0 if found => 0,
                        0 => 1,
                        refused => refused,
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    tracing::error!("[wasm] redis_get_status error: {}", msg);
                    caller.data_mut().respond("redis_get_status", msg);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "redis_mget",
        |mut caller: Caller<'_, HostState>, keys_ptr: i32, keys_len: i32| -> i32 {
            let keys_json = match read_guest_string(&mut caller, "redis_mget", keys_ptr, keys_len) {
                Ok(s) => s,
                Err(e) => {
                    caller
                        .data_mut()
                        .respond("redis_mget", "invalid keys buffer".into());
                    return e.code();
                }
            };
            let keys: Vec<String> = match serde_json::from_str(&keys_json) {
                Ok(k) => k,
                Err(e) => {
                    let msg = format!("invalid keys JSON: {}", e);
                    tracing::error!("[wasm] redis_mget: {}", msg);
                    caller.data_mut().respond("redis_mget", msg);
                    return -1;
                }
            };
            if keys.is_empty() {
                return caller.data_mut().respond("redis_mget", "[]".to_string());
            }
            let Some(plans) = keys
                .iter()
                .map(|key| caller.data().redis_plan(key))
                .collect::<Option<Vec<_>>>()
            else {
                caller
                    .data_mut()
                    .respond("redis_mget", RESERVED_REDIS_KEY.into());
                return -1;
            };
            let app_state = caller.data().app_state.clone();
            let result = block_on(redis_scope::mget(
                &app_state.redis,
                &plans,
                Reading::of(caller.data().caps()),
            ))
            .map_err(eyre::Report::from)
            .and_then(|read| {
                caller.data().record_redis_fallback(read.from_fallback);
                serde_json::to_string(&read.value).map_err(Into::into)
            });
            match result {
                Ok(json) => caller.data_mut().respond("redis_mget", json),
                Err(e) => {
                    let msg = e.to_string();
                    tracing::error!("[wasm] redis_mget error: {}", msg);
                    caller.data_mut().respond("redis_mget", msg);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

impl HostState {
    fn redis_plan(&self, key: &str) -> Option<redis_scope::KeyPlan> {
        redis_scope::plan(
            self.policies.mode(PolicyKey::RedisScope),
            self.identity.tenant_id,
            key,
        )
        .map_err(|redis_scope::ReservedKey(key)| {
            tracing::error!(
                tenant = %self.identity.tenant_slug,
                service = %self.identity.service_name,
                key = %key,
                "Guest Redis key uses a reserved platform prefix"
            );
            policy::deny(
                PolicyKey::RedisScope,
                &self.identity.tenant_slug,
                Violation::new("reserved_prefix", format!("key {key}")),
            )
        })
        .ok()
    }

    fn record_redis_fallback(&self, from_fallback: bool) {
        if from_fallback {
            let _ = self.policies.apply(
                PolicyKey::RedisScope,
                &self.identity.tenant_slug,
                Some(Violation::new(
                    "unprefixed_fallback",
                    "read served from an unprefixed key",
                )),
            );
        }
    }
}
