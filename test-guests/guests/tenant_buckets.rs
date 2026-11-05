use cufflink_fn::prelude::*;
use cufflink_fn::storage::{CannedAcl, StorageError, UploadOptions};

cufflink_fn::init!();

fn text<'a>(req: &'a Request, field: &str) -> &'a str {
    req.body()[field].as_str().unwrap_or_default()
}

fn storage_failed(error: StorageError) -> Response {
    Response::json(&json!({"error": error.kind.as_str(), "message": error.message}))
}

cufflink_fn::handler!(upload, |req: Request| {
    let options = match req.body()["acl"].as_str() {
        Some(acl) => match CannedAcl::parse(acl) {
            Some(acl) => UploadOptions::new().acl(acl),
            None => return Response::json(&json!({"error": "acl", "message": acl})),
        },
        None => UploadOptions::new(),
    };
    storage::upload_with(
        text(&req, "bucket"),
        text(&req, "key"),
        "application/pdf",
        text(&req, "body").as_bytes(),
        &options,
    )
    .map_or_else(storage_failed, |stored| {
        Response::json(&json!({"size": stored.size}))
    })
});

cufflink_fn::handler!(head, |req: Request| {
    storage::head(text(&req, "bucket"), text(&req, "key")).map_or_else(storage_failed, |found| {
        Response::json(&json!({"size": found.map(|meta| meta.size)}))
    })
});

cufflink_fn::handler!(download, |req: Request| {
    Response::json(&json!({
        "base64": storage::download(text(&req, "bucket"), text(&req, "key")),
    }))
});

cufflink_fn::handler!(download_many, |req: Request| {
    let items: Vec<(&str, &str)> = req.body()["items"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            (
                item["bucket"].as_str().unwrap_or_default(),
                item["key"].as_str().unwrap_or_default(),
            )
        })
        .collect();
    let slots: Vec<Value> = storage::download_many(&items)
        .into_iter()
        .map(|slot| match slot {
            Ok(bytes) => json!({"size": bytes.len()}),
            Err(error) => json!({"error": error}),
        })
        .collect();
    Response::json(&json!({ "slots": slots }))
});

cufflink_fn::handler!(presign, |req: Request| {
    Response::json(&json!({
        "url": storage::presign_download(text(&req, "bucket"), text(&req, "key"), 60),
    }))
});

cufflink_fn::handler!(delete, |req: Request| {
    Response::json(&json!({
        "deleted": storage::delete(text(&req, "bucket"), text(&req, "key")),
    }))
});

cufflink_fn::handler!(object, |req: Request| {
    Response::s3_object(S3Object {
        bucket: text(&req, "bucket").to_string(),
        key: text(&req, "key").to_string(),
        content_type: None,
        disposition: None,
    })
});
