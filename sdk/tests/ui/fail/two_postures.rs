cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("GET", "/me", "me", auth, owner("customer_id"))],
}

fn main() {}
