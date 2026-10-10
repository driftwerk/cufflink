use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(sleep, |req: Request| {
    let ms = req.body()["ms"]
        .as_u64()
        .and_then(|ms| u32::try_from(ms).ok())
        .unwrap_or(0);
    let before = util::now_millis();
    let slept = util::sleep_ms(ms);
    Response::json(&json!({"slept": slept, "elapsed": util::now_millis() - before}))
});

cufflink_fn::handler!(now, |_req: Request| {
    Response::json(&json!({"millis": util::now_millis(), "time": util::current_time()}))
});
