cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/progress", "progress", auth, sse(subject = "dw.{tenant}.progress"))],
}

fn main() {}
