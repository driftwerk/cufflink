use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(ping, |_req: Request| Response::json(&json!({ "ok": true })));
