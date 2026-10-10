cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/p", "p", auth, forward_auth, forward_auth)],
}

fn main() {}
