use cufflink_fn::prelude::*;

cufflink_fn::init!();

cufflink_fn::handler!(receipt_progress, |req: Request| {
    let number = |name: &str| req.query_first(name).and_then(|v| v.parse::<u32>().ok());
    if let Some(millis) = number("snapshot_ms") {
        db::execute(&format!("SELECT pg_sleep({millis} / 1000.0)"));
    }
    let total = number("total").unwrap_or(2);
    Response::sse(SseSpec {
        initial: vec![SseEvent::data(json!({
            "type": "snapshot",
            "book_id": req.path_param("book_id"),
            "completed": 0,
            "total": total,
        }))],
        state: json!({ "done": [], "total": total }),
        ping: number("ping").map(|interval_secs| SsePing {
            interval_secs,
            event: SseEvent::data(json!({ "type": "ping" })),
        }),
        timeout_secs: number("timeout").unwrap_or(300),
        headers: vec![
            ("cache-control".into(), "no-cache, no-store, must-revalidate".into()),
            ("x-accel-buffering".into(), "no".into()),
        ],
        ..SseSpec::default()
    })
});

cufflink_fn::handler!(receipt_progress_reduce, |req: Request| sse::reduce(
    req,
    |state, message| {
        let receipt = message.data["receipt_id"].as_str().unwrap_or_default().to_string();
        let mut done: Vec<String> = state["done"]
            .as_array()
            .map(|done| {
                done.iter()
                    .filter_map(|id| id.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        if !done.contains(&receipt) {
            done.push(receipt.clone());
        }
        let total = state["total"].as_u64().unwrap_or(0);
        let complete = done.len() as u64 >= total;
        let finished = SseEvent::data(json!({
            "type": "receipt_completed",
            "receipt_id": receipt,
            "completed": done.len(),
        }));
        Reduction {
            events: std::iter::once(finished)
                .chain(complete.then(|| SseEvent::data(json!({ "type": "all_completed" }))))
                .collect(),
            state: json!({ "done": done, "total": total }),
            close: complete,
        }
    }
));

cufflink_fn::handler!(not_a_stream, |_req: Request| Response::json(
    &json!({ "stream": false })
));
