use cufflink_fn::http::{call_service, CallError};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

fn error_kind(error: &CallError) -> &'static str {
    match error {
        CallError::Timeout => "timeout",
        CallError::DepthExceeded => "depth_exceeded",
        CallError::UnknownService(_) => "unknown_service",
        CallError::Transport(_) => "transport",
        CallError::Denied(_) => "denied",
    }
}

cufflink_fn::handler!(relay, |req: Request| {
    let call = req.body();
    let text = |field: &str| call[field].as_str().unwrap_or_default().to_string();
    let headers: Vec<(String, String)> = call["headers"]
        .as_object()
        .map(|headers| {
            headers
                .iter()
                .map(|(name, value)| (name.clone(), value.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default();
    let headers: Vec<(&str, &str)> = headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    let body = match call["pad_bytes"].as_u64() {
        Some(pad) => Some(json!({ "pad": "x".repeat(usize::try_from(pad).unwrap_or(0)) }).to_string()),
        None => (!call["body"].is_null()).then(|| call["body"].to_string()),
    };
    Response::json(
        &match call_service(
            &text("service"),
            &text("method"),
            &text("path"),
            &headers,
            body.as_deref().map(str::as_bytes),
        ) {
            Ok(response) => json!({
                "status": response.status,
                "content_type": response.headers.get("content-type"),
                "body": response.json::<Value>().unwrap_or(Value::Null),
            }),
            Err(error) => json!({"error": error_kind(&error), "message": error.to_string()}),
        },
    )
});

cufflink_fn::handler!(echo, |req: Request| {
    Response::json(&json!({"method": req.method(), "body": req.body(), "service": req.service()}))
});

cufflink_fn::handler!(recurse, |req: Request| {
    let mut data = req.body()["data"].clone();
    let nested = json!({"title": "nested"}).to_string();
    if let Err(error) = call_service(req.service(), "POST", "/items", &[], Some(nested.as_bytes())) {
        data["title"] = json!(error_kind(&error));
    }
    Response::json(&json!({ "data": data }))
});
