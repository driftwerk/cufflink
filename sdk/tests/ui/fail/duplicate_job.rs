cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/run", "run", auth, job, job(timeout = 5))],
}

fn main() {}
