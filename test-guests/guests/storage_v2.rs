use cufflink_fn::http::{HttpError, HttpRequest, Part};
use cufflink_fn::prelude::*;
use cufflink_fn::storage::{StorageError, UploadOptions};

cufflink_fn::init!();

fn failed(kind: &str, message: &str) -> Response {
    Response::json(&json!({"error": kind, "message": message}))
}

fn storage_failed(error: StorageError) -> Response {
    failed(error.kind.as_str(), &error.message)
}

fn http_failed(error: HttpError) -> Response {
    failed(error.kind.as_str(), &error.message)
}

fn text<'a>(req: &'a Request, field: &str) -> &'a str {
    req.body()[field].as_str().unwrap_or_default()
}

fn body(req: &Request) -> Vec<u8> {
    req.body()["body_len"]
        .as_u64()
        .and_then(|len| usize::try_from(len).ok())
        .map_or_else(
            || {
                req.body()["body"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
                    .collect()
            },
            |len| vec![7; len],
        )
}

cufflink_fn::handler!(upload, |req: Request| {
    let body = body(&req);
    let options = [
        ("content_disposition", UploadOptions::content_disposition as fn(_, &str) -> _),
        ("cache_control", UploadOptions::cache_control),
    ]
    .into_iter()
    .fold(UploadOptions::new(), |options, (field, set)| {
        match req.body()[field].as_str() {
            Some(value) => set(options, value),
            None => options,
        }
    });
    storage::upload_with(
        text(&req, "bucket"),
        text(&req, "key"),
        text(&req, "content_type"),
        &body,
        &options,
    )
    .map_or_else(storage_failed, |stored| {
        Response::json(&json!({"size": stored.size, "etag": stored.etag}))
    })
});

cufflink_fn::handler!(head, |req: Request| {
    storage::head(text(&req, "bucket"), text(&req, "key")).map_or_else(storage_failed, |found| {
        Response::json(&json!({"found": found.map(|meta| json!({
            "size": meta.size,
            "content_type": meta.content_type,
            "etag": meta.etag,
            "last_modified": meta.last_modified,
        }))}))
    })
});

cufflink_fn::handler!(download, |req: Request| {
    Response::json(&json!({
        "base64": storage::download(text(&req, "bucket"), text(&req, "key")),
    }))
});

cufflink_fn::handler!(receipt_pdf, |req: Request| {
    let rendered = http::multipart(
        &HttpRequest::post(&format!("{}/convert/html", text(&req, "renderer"))),
        &[
            Part::text("printBackground", "true"),
            Part::file(
                "files",
                "index.html",
                "text/html",
                text(&req, "html").as_bytes().to_vec(),
            ),
        ],
    );
    let pdf = match rendered {
        Ok(response) if response.is_success() => response.into_bytes(),
        Ok(response) => return failed("renderer", &response.status.to_string()),
        Err(error) => return http_failed(error),
    };
    let key = format!(
        "{}/{}_{}.pdf",
        text(&req, "book_id"),
        text(&req, "receipt_id"),
        text(&req, "branch")
    );
    storage::upload(text(&req, "bucket"), &key, "application/pdf", &pdf).map_or_else(
        storage_failed,
        |stored| Response::json(&json!({"key": key, "size": stored.size})),
    )
});

cufflink_fn::handler!(send_receipts, |req: Request| {
    let attachments: Option<Vec<Value>> = req.body()["attachments"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|attachment| {
            storage::download(
                text(&req, "bucket"),
                attachment["key"].as_str().unwrap_or_default(),
            )
            .map(|content| {
                json!({
                    "name": attachment["filename"],
                    "content": content,
                    "content_type": "application/pdf",
                })
            })
        })
        .collect();
    let Some(attachments) = attachments else {
        return failed("attachment", "a receipt PDF could not be read");
    };
    let email = json!({
        "from": text(&req, "from"),
        "to": text(&req, "to"),
        "html": text(&req, "html"),
        "subject": text(&req, "subject"),
        "attachments": attachments,
    });
    HttpRequest::post(&format!("{}/messages", text(&req, "mail_api")))
        .header("Accept", "application/json")
        .header("X-Api-Token", text(&req, "token"))
        .json(&email)
        .and_then(|request| http::fetch_bytes(&request))
        .and_then(|sent| {
            sent.json::<Value>()
                .map(|body| json!({"status": sent.status, "sent": body}))
        })
        .map_or_else(http_failed, |sent| Response::json(&sent))
});
