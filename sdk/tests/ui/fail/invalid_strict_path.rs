cufflink::service! {
    name: "svc",
    tables: [],
    routing: strict,
    custom_routes: [("GET", "/books/{Book}", "get_book", auth)],
}

fn main() {}
