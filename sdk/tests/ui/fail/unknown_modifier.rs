cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("GET", "/me", "me", authenticated)],
}

fn main() {}
