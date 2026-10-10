cufflink::service! {
    name: "svc",
    tables: [],
    subscriptions: [("dw.{tenant}.a.b", "h", wasm, durable = "a.b")],
}

fn main() {}
