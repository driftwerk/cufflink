cufflink::service! {
    name: "svc",
    tables: [],
    subscriptions: [("dw.{tenant}.a.b", "h", wasm, max_deliver = 0)],
}

fn main() {}
