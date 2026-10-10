use cufflink_fn::cache::CacheError;
use cufflink_fn::prelude::*;
use std::time::Duration;

cufflink_fn::init!();

fn respond<T>(result: Result<T, CacheError>) -> Response
where
    Value: From<T>,
{
    match result {
        Ok(value) => Response::json(&json!({ "result": Value::from(value) })),
        Err(e) => Response::json(&json!({ "error": e.to_string(), "kind": kind(&e) })),
    }
}

fn kind(error: &CacheError) -> &'static str {
    match error {
        CacheError::Unavailable => "unavailable",
        CacheError::Timeout => "timeout",
        CacheError::InvalidKey(_) => "invalid_key",
        CacheError::InvalidTtl(_) => "invalid_ttl",
        CacheError::TooLarge => "too_large",
        CacheError::Protocol(_) => "protocol",
    }
}

fn text<'a>(req: &'a Request, field: &str) -> &'a str {
    req.body()[field].as_str().unwrap_or_default()
}

fn ttl(req: &Request) -> Duration {
    Duration::from_millis(req.body()["ttl_ms"].as_u64().unwrap_or(60_000))
}

cufflink_fn::handler!(put, |req: Request| {
    respond(cache::set(text(&req, "key"), text(&req, "value"), ttl(&req)).map(|()| Value::Null))
});

cufflink_fn::handler!(fetch, |req: Request| respond(cache::get(text(&req, "key"))));

cufflink_fn::handler!(fetch_many, |req: Request| {
    let keys: Vec<&str> = req.body()["keys"]
        .as_array()
        .map(|keys| keys.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    respond(cache::mget(&keys))
});

cufflink_fn::handler!(lock, |req: Request| {
    respond(cache::set_nx(text(&req, "key"), "held", ttl(&req)))
});

cufflink_fn::handler!(count, |req: Request| {
    let by = req.body()["by"].as_i64().unwrap_or(1);
    respond(cache::incr(text(&req, "key"), by, ttl(&req)))
});

cufflink_fn::handler!(forget, |req: Request| respond(cache::del(text(&req, "key"))));

cufflink_fn::handler!(extend, |req: Request| {
    respond(cache::expire(text(&req, "key"), ttl(&req)))
});

cufflink_fn::handler!(json_round_trip, |req: Request| {
    let key = text(&req, "key");
    respond(
        cache::set_json(key, &req.body()["value"], ttl(&req))
            .and_then(|()| cache::get_json::<Value>(key)),
    )
});

cufflink_fn::handler!(raw, |req: Request| {
    Response::json(&match redis::get_with_status(text(&req, "key")) {
        Ok(value) => json!({ "result": value }),
        Err(e) => json!({ "error": e }),
    })
});
