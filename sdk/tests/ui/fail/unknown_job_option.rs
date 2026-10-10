cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/run", "run", auth, job(retries = 2))],
}

fn main() {}
