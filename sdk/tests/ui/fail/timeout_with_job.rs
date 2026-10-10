cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/export", "export", auth, job, timeout = 60)],
}

fn main() {}
