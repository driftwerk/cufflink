
use cufflink_fn::nats::{PubAck, PublishError, PublishOptions};
use cufflink_fn::prelude::*;

cufflink_fn::init!();

fn answer(published: Result<PubAck, PublishError>) -> Response {
    match published {
        Ok(ack) => Response::json(&json!({
            "ack": {"stream": ack.stream, "sequence": ack.sequence, "duplicate": ack.duplicate}
        })),
        Err(error) => Response::json(&json!({
            "error": {"kind": error.kind.as_str(), "message": error.message}
        })),
    }
}

cufflink_fn::handler!(publish, |req: Request| {
    let body = req.body();
    let subject = body["subject"].as_str().unwrap_or_default();
    let opts = headers_of(body)
        .into_iter()
        .fold(PublishOptions::new(), |opts, (name, value)| {
            opts.header(name, value)
        });
    let opts = match body["msg_id"].as_str() {
        Some(id) => opts.msg_id(id),
        None => opts,
    };
    let published = match body["size"].as_u64() {
        Some(size) => nats::publish_with(subject, &vec![b'x'; size as usize], &opts),
        None => nats::publish_json(subject, &body["payload"], &opts),
    };
    answer(published)
});

fn headers_of(body: &Value) -> Vec<(&str, &str)> {
    body["headers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|pair| Some((pair[0].as_str()?, pair[1].as_str()?)))
        .collect()
}

cufflink_fn::handler!(publish_core, |req: Request| {
    let body = req.body();
    let subject = body["subject"].as_str().unwrap_or_default();
    let headers = match headers_of(body) {
        given if given.is_empty() => vec![("x-source", "guest")],
        given => given,
    };
    match nats::publish_core(subject, body["payload"].to_string().as_bytes(), &headers) {
        Ok(()) => Response::json(&json!({"sent": true})),
        Err(error) => Response::json(&json!({
            "error": {"kind": error.kind.as_str(), "message": error.message}
        })),
    }
});
