cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/run", "run", auth, job(retry_on = [panic]))],
}

fn main() {}
