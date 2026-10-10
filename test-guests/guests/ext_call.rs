use cufflink_fn::ext::{self, ExtError};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

fn answer(error: &ExtError) -> Value {
    let (kind, code) = match error {
        ExtError::NotDeclared => ("not_declared", None),
        ExtError::NotFound => ("not_found", None),
        ExtError::Denied => ("denied", None),
        ExtError::Unavailable(_) => ("unavailable", None),
        ExtError::Timeout => ("timeout", None),
        ExtError::TooLarge => ("too_large", None),
        ExtError::Failed { code, .. } => ("failed", Some(code)),
        ExtError::Invalid(_) => ("invalid", None),
    };
    let message = match error {
        ExtError::Failed { message, .. } => message.clone(),
        other => other.to_string(),
    };
    match code {
        Some(code) => json!({"error": kind, "code": code, "message": message}),
        None => json!({"error": kind, "message": message}),
    }
}

fn forward(req: Request) -> Response {
    let call = req.body();
    let extension = call["extension"].as_str().unwrap_or_default();
    let payload = &call["payload"];
    let outcome = match call["timeout_ms"].as_u64() {
        Some(timeout_ms) => ext::call_with_timeout(extension, payload, timeout_ms),
        None => ext::call(extension, payload),
    };
    Response::json(&match outcome {
        Ok(value) => json!({ "ok": value }),
        Err(error) => answer(&error),
    })
}

cufflink_fn::handler!(call, forward);

cufflink_fn::handler!(call_as_user, forward);
