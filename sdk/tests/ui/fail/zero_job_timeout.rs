cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/run", "run", auth, job(timeout = 0))],
}

fn main() {}
