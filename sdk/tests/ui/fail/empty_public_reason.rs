cufflink::service! {
    name: "svc",
    tables: [],
    custom_routes: [("POST", "/hook", "hook", public("  "))],
}

fn main() {}
