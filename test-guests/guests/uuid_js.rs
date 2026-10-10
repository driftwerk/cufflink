
use cufflink_fn::prelude::*;
use cufflink_fn::{ext, stripe, util};

cufflink_fn::init!();

cufflink_fn::handler!(mint, |_req: Request| Response::json(
    &json!({ "id": util::generate_uuid() })
));

cufflink_fn::handler!(webhook, |req: Request| Response::json(
    &match stripe::verify_webhook(&req, "whsec_test") {
        Ok(event) => json!({ "event": event }),
        Err(error) => json!({ "error": error }),
    }
));

cufflink_fn::handler!(call, |req: Request| Response::json(
    &match ext::call("echo.ping", req.body()) {
        Ok(value) => json!({ "ok": value }),
        Err(error) => json!({ "error": error.to_string() }),
    }
));
