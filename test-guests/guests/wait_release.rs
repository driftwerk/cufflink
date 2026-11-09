use cufflink_fn::prelude::*;

cufflink_fn::init!();

#[link(wasm_import_module = "env")]
extern "C" {
    fn http_fetch_v2(ptr: i32, len: i32) -> i32;
    fn service_call(ptr: i32, len: i32) -> i32;
    fn get_host_response_len() -> i32;
    fn get_host_response(ptr: i32, len: i32) -> i32;
}

fn answer() -> Value {
    let len = unsafe { get_host_response_len() };
    let mut answer = vec![0u8; usize::try_from(len).unwrap_or(0)];
    unsafe { get_host_response(answer.as_mut_ptr() as i32, len) };
    String::from_utf8(answer)
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(Value::Null)
}

fn relayed(request: &Value) -> Value {
    let sent = request["request"].to_string();
    let (ptr, len) = (sent.as_ptr() as i32, sent.len() as i32);
    let rc = match request["host"].as_str() {
        Some("service_call") => unsafe { service_call(ptr, len) },
        _ => unsafe { http_fetch_v2(ptr, len) },
    };
    match rc {
        0 => answer(),
        rc => json!({ "rc": rc }),
    }
}

cufflink_fn::handler!(relay, |req: Request| {
    let answered = relayed(req.body());
    if req.body()["then"] == "trap" {
        core::arch::wasm32::unreachable();
    }
    Response::json(&answered)
});

cufflink_fn::handler!(compute, |_req: Request| {
    Response::json(&json!({ "computed": true }))
});
