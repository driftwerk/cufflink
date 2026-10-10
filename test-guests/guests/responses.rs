use cufflink_fn::prelude::*;
use std::collections::BTreeMap;

cufflink_fn::init!();

const XLSX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

cufflink_fn::handler!(created, |_req: Request| Response::created(&json!({ "id": 1 })));

cufflink_fn::handler!(no_content, |_req: Request| Response::no_content());

cufflink_fn::handler!(export_xlsx, |_req: Request| Response::attachment(
    "test.xlsx",
    XLSX,
    vec![1, 2, 3, 4, 5]
));

cufflink_fn::handler!(download_pdf, |_req: Request| Response::attachment(
    "receipt.pdf",
    "application/pdf",
    b"%PDF-1.4".to_vec()
)
.header("cache-control", "no-cache, no-store, must-revalidate")
.header("pragma", "no-cache"));

cufflink_fn::handler!(view_pdf, |_req: Request| Response::inline(
    "receipt.pdf",
    "application/pdf",
    b"%PDF-1.4".to_vec()
));

cufflink_fn::handler!(redirect, |_req: Request| Response::redirect(Redirect::SeeOther, "/books/7"));

fn pong(_req: Request) -> Response {
    Response::plain("pong")
}

cufflink_fn::handler!(plain, pong);

cufflink_fn::handler!(owned_headers, |_req: Request| Response::json(
    &json!({ "ok": true })
)
.header("set-cookie", "session=stolen")
.header("access-control-allow-origin", "*")
.header("x-request-id", "forged")
.header("x-total-count", "3"));

cufflink_fn::handler!(invalid_status, |_req: Request| Response::no_content()
    .with_status(700));

cufflink_fn::handler!(validation, |_req: Request| {
    let errors = BTreeMap::from([(
        "content".to_string(),
        vec!["Content cannot be empty".to_string()],
    )]);
    Response::validation_errors(&errors)
});

cufflink_fn::handler!(legacy_error, |_req: Request| Response::not_found("gone"));

cufflink_fn::handler!(object, |req: Request| {
    let text = |key: &str| req.body()[key].as_str().map(str::to_string);
    Response::s3_object(S3Object {
        bucket: text("bucket").unwrap_or_default(),
        key: text("key").unwrap_or_default(),
        content_type: text("content_type"),
        disposition: Some(Disposition::Attachment("big.bin".into())),
    })
});
