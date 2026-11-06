use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(parse, |req: Request| {
    let sql = req.body()["sql"].as_str().unwrap_or_default();
    Response::json(&match sql::parse(sql) {
        Ok(parsed) => json!({ "ok": parsed }),
        Err(error) => json!({ "error": error }),
    })
});
