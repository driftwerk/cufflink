
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
