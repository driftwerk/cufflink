cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/import", "import", body_limit = 1024, perm("items", "edit"))],
}

fn main() {}
