
use cufflink_fn::prelude::*;

cufflink_fn::init!();

fn text(value: &str) -> String {
    format!("'{}'", db::sql_escape(value))
}

fn run(req: &Request, ms: u32) -> Response {
    let Some(fire) = req.schedule() else {
        return Response::error_message(400, "not the fire of a schedule");
    };
    let attempt = req
        .subscription()
        .map_or_else(|| "NULL".to_string(), |delivery| delivery.attempt().to_string());
    db::execute(&format!(
        "INSERT INTO runs (schedule, handler, fire_at, catch_up, attempt, started_at) \
         VALUES ({}, {}, {}, {}, {attempt}, clock_timestamp())",
        text(fire.name()),
        text(req.handler()),
        text(fire.fire_at()),
        fire.is_catch_up(),
    ));
    if ms > 0 {
        util::sleep_ms(ms);
    }
    db::execute(&format!(
        "UPDATE runs SET ended_at = clock_timestamp() \
         WHERE schedule = {} AND fire_at = {} AND ended_at IS NULL",
        text(fire.name()),
        text(fire.fire_at()),
    ));
    Response::json(&json!({ "ok": true }))
}

cufflink_fn::handler!(quick, |req: Request| run(&req, 0));

cufflink_fn::handler!(slow, |req: Request| run(&req, 15_000));

cufflink_fn::handler!(failing, |req: Request| {
    run(&req, 0);
    Response::error_message(503, "the ledger is closed")
});
