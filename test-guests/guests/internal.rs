use cufflink_fn::http::{call_service, CallError};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(hop, |req: Request| {
    let call = req.body();
    let text = |field: &str| call[field].as_str().unwrap_or_default().to_string();
    let body = call["body"].to_string();
    Response::json(
        &match call_service(&text("service"), "POST", &text("path"), &[], Some(body.as_bytes())) {
            Ok(response) => json!({
                "status": response.status,
                "body": response.json::<Value>().unwrap_or(Value::Null),
            }),
            Err(CallError::DepthExceeded) => json!({ "error": "depth_exceeded" }),
            Err(error) => json!({ "error": error.to_string() }),
        },
    )
});

cufflink_fn::handler!(whoami, |req: Request| {
    cufflink_fn::log::info("asked who calls");
    Response::json(&json!({
        "caller": req.caller().map(|caller| json!({
            "service": caller.service,
            "on_behalf_of": caller.on_behalf_of,
            "depth": caller.depth,
        })),
        "authenticated": req.auth().is_some(),
        "identity_header": req.header("x-cufflink-service-identity"),
    }))
});

cufflink_fn::handler!(spin, |_req: Request| loop {
    std::hint::spin_loop();
});
