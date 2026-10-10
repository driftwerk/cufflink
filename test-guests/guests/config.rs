use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(read, |req: Request| {
    let key = req.body()["key"].as_str().unwrap_or_default();
    Response::json(&json!({ "value": config::get(key) }))
});
