cufflink::service! {
    name: "svc",
    tables: [],
    subscriptions: [("dw.{tenant}.a.b", "h", wasm, retries = 3)],
}

fn main() {}
