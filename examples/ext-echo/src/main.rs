use cufflink_native::prelude::*;
use cufflink_native::Service;
use std::time::Duration;

const EXTENSIONS: &str = "echo.reply,echo.fail,echo.sleep";

fn reply(ctx: &CallContext, payload: Value) -> Result<Value> {
    Ok(json!({ "payload": payload, "tenant": ctx.tenant, "service": ctx.service }))
}

fn fail(_: &CallContext, payload: Value) -> Result<Value> {
    let code = payload["code"].as_str().ok_or("code is required")?;
    Err(ExtError::new(
        code,
        payload["message"].as_str().unwrap_or("failed as asked"),
    ))
}

fn sleep(_: &CallContext, payload: Value) -> Result<Value> {
    let ms = payload["ms"].as_u64().ok_or("ms is required")?;
    std::thread::sleep(Duration::from_millis(ms));
    Ok(json!({ "slept_ms": ms }))
}

fn provide(service: Service, name: &str) -> Service {
    match name.rsplit('.').next() {
        Some("fail") => service.extension(name, fail, ["*"]),
        Some("sleep") => service.extension(name, sleep, ["*"]),
        _ => service.extension(name, reply, ["*"]),
    }
}

fn main() -> std::process::ExitCode {
    std::env::var("EXT_ECHO_EXTENSIONS")
        .unwrap_or_else(|_| EXTENSIONS.into())
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .fold(cufflink_native::service("ext-echo"), provide)
        .run()
}
