cufflink::service! {
    name: "svc",
    tables: [],
    subscriptions: [("dw.{tenant}.a.b", "h", wasm, ack = sometimes)],
}

fn main() {}
