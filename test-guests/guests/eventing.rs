
use cufflink_fn::prelude::*;
use std::time::Duration;

cufflink_fn::init!();

fn text(value: Option<&str>) -> String {
    value.map_or_else(
        || "NULL".to_string(),
        |value| format!("'{}'", db::sql_escape(value)),
    )
}

fn record(req: &Request) {
    let delivery = req.subscription();
    let number = |value: Option<u64>| value.map_or_else(|| "NULL".to_string(), |n| n.to_string());
    db::execute(&format!(
        "INSERT INTO deliveries (handler, body, attempt, subject, stream, consumer, sequence, msg_id) \
         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
        text(Some(req.handler())),
        text(Some(&req.body().to_string())),
        number(delivery.map(|d| d.attempt())),
        text(delivery.map(|d| d.subject())),
        text(delivery.map(|d| d.stream())),
        text(delivery.map(|d| d.consumer())),
        number(delivery.map(|d| d.sequence())),
        text(delivery.and_then(|d| d.msg_id())),
    ));
}

fn deliveries_of(req: &Request) -> i64 {
    db::query_one(&format!(
        "SELECT count(*) AS n FROM deliveries WHERE handler = {} AND body = {}",
        text(Some(req.handler())),
        text(Some(&req.body().to_string())),
    ))
    .and_then(|row| row["n"].as_i64())
    .unwrap_or_default()
}

fn tries(req: &Request) -> usize {
    let tried = format!("{}:try", req.handler());
    let body = text(Some(&req.body().to_string()));
    db::execute(&format!(
        "INSERT INTO deliveries (handler, body) VALUES ({}, {body})",
        text(Some(&tried))
    ));
    db::query_one(&format!(
        "SELECT count(*) AS n FROM deliveries WHERE handler = {} AND body = {body}",
        text(Some(&tried))
    ))
    .and_then(|row| row["n"].as_u64())
    .and_then(|n| usize::try_from(n).ok())
    .unwrap_or(1)
}

fn holding(req: &Request) -> Response {
    let body = req.body();
    let mib = match body["mib"].as_array() {
        Some(by_try) => by_try
            .get(tries(req).saturating_sub(1))
            .or_else(|| by_try.last())
            .and_then(Value::as_u64),
        None => body["mib"].as_u64(),
    };
    let mib = mib.and_then(|n| usize::try_from(n).ok()).unwrap_or(0);
    let held = vec![1u8; mib * 1024 * 1024];
    if let Some(key) = body["gate"].as_i64() {
        db::execute(&format!("SELECT pg_advisory_xact_lock({key})"));
    }
    record(req);
    Response::json(&json!({ "held": held.len() }))
}

cufflink_fn::handler!(hold, |req: Request| { holding(&req) });

cufflink_fn::handler!(hold_more, |req: Request| { holding(&req) });

cufflink_fn::handler!(grow, |req: Request| { holding(&req) });

cufflink_fn::handler!(succeed, |req: Request| {
    record(&req);
    Response::json(&json!({ "ok": true }))
});

cufflink_fn::handler!(fail, |req: Request| {
    record(&req);
    Response::error_message(503, "the mail server is down")
});

cufflink_fn::handler!(reject, |req: Request| {
    record(&req);
    Response::error_message(422, "no such book")
});

cufflink_fn::handler!(say_error, |req: Request| {
    record(&req);
    Response::json(&json!({ "error": "said, not answered" }))
});

cufflink_fn::handler!(fail_twice, |req: Request| {
    record(&req);
    if deliveries_of(&req) < 3 {
        Response::error_message(500, "not yet")
    } else {
        Response::json(&json!({ "ok": true }))
    }
});

cufflink_fn::handler!(retry_later, |req: Request| {
    record(&req);
    if deliveries_of(&req) < 2 {
        Response::retry_after(Duration::from_millis(1500))
    } else {
        Response::json(&json!({ "ok": true }))
    }
});

cufflink_fn::handler!(give_up, |req: Request| {
    record(&req);
    Response::terminal("unknown book")
});
