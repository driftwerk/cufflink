
use cufflink_fn::prelude::*;
use std::time::Duration;

cufflink_fn::init!();

fn told(req: &Request) -> Response {
    let job = req.job();
    Response::json(&json!({
        "attempt": job.map(|job| job.attempt),
        "max_attempts": job.map(|job| job.max_attempts),
        "last_error": job.and_then(|job| job.last_error()),
        "authorization": req.header("authorization"),
        "subject": req.auth().map(|auth| auth.sub.as_str()),
    }))
}

fn first_attempt(req: &Request) -> bool {
    req.job().is_none_or(|job| job.attempt == 1)
}

cufflink_fn::handler!(job_fail_first, |req: Request| {
    if first_attempt(&req) {
        Response::error_message(503, "the model is overloaded")
    } else {
        told(&req)
    }
});

cufflink_fn::handler!(job_retry_later, |req: Request| {
    if first_attempt(&req) {
        Response::retry_after(Duration::from_millis(1200))
    } else {
        told(&req)
    }
});

cufflink_fn::handler!(job_give_up, |_req: Request| {
    Response::terminal("unknown item")
});
