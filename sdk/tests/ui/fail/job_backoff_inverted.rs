cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/run", "run", auth, job(backoff(base = 600, max = 60)))],
}

fn main() {}
