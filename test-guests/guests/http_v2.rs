use cufflink_fn::http::{HttpError, HttpRequest, HttpResponse, Part};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

fn bytes(value: &Value) -> Option<Vec<u8>> {
    value.as_array().map(|bytes| {
        bytes
            .iter()
            .filter_map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
            .collect()
    })
}

fn respond(result: Result<HttpResponse, HttpError>) -> Response {
    Response::json(&match result {
        Ok(response) => json!({
            "status": response.status,
            "content_type": response.header("content-type"),
            "cookies": response.header_all("set-cookie"),
            "body": response.bytes(),
        }),
        Err(error) => json!({"error": error.kind.as_str(), "message": error.message}),
    })
}

cufflink_fn::handler!(convert, |req: Request| {
    let text = |field: &str| req.body()[field].as_str().unwrap_or_default();
    let html = format!(
        "<style>@page {{ margin: 0; }} html, body {{ margin: 0; padding: 0; }}</style>{}",
        text("html")
    );
    respond(http::multipart(
        &HttpRequest::post(&format!("{}/convert/html", text("renderer"))),
        &[
            Part::text("emulatedMediaType", "print"),
            Part::text("printBackground", "true"),
            Part::text("preferCssPageSize", "true"),
            Part::text("waitDelay", "1s"),
            Part::text("marginTop", "0"),
            Part::text("marginBottom", "0"),
            Part::text("marginLeft", "0"),
            Part::text("marginRight", "0"),
            Part::file("files", "index.html", "text/html", html.into_bytes()),
        ],
    ))
});

fn set<T>(
    request: HttpRequest,
    value: Option<T>,
    set: impl FnOnce(HttpRequest, T) -> HttpRequest,
) -> HttpRequest {
    match value {
        Some(value) => set(request, value),
        None => request,
    }
}

cufflink_fn::handler!(fetch, |req: Request| {
    let call = req.body();
    let text = |field: &str| call[field].as_str().unwrap_or_default();
    let request = call["headers"]
        .as_array()
        .into_iter()
        .flatten()
        .fold(HttpRequest::new(text("method"), text("url")), |request, header| {
            request.header(
                header[0].as_str().unwrap_or_default(),
                header[1].as_str().unwrap_or_default(),
            )
        })
        .follow_redirects(call["follow_redirects"].as_bool().unwrap_or(false));
    let request = set(request, bytes(&call["body"]), HttpRequest::body);
    let request = set(request, call["timeout_ms"].as_u64(), |request, ms| {
        request.timeout_ms(u32::try_from(ms).unwrap_or(u32::MAX))
    });
    let request = set(
        request,
        call["max_response_bytes"].as_u64(),
        HttpRequest::max_response_bytes,
    );
    respond(http::fetch_bytes(&request))
});

cufflink_fn::handler!(legacy, |req: Request| {
    let url = req.body()["url"].as_str().unwrap_or_default();
    Response::json(&match http::get(url, &[]) {
        Some(response) => json!({"status": response.status, "body": response.body}),
        None => json!({"none": true}),
    })
});
