cufflink::service! {
    name: "svc",
    tables: [],
    subscriptions: [("dw.{tenant}.a.b", "h", wasm, ack = on_success, ack = always)],
}

fn main() {}
