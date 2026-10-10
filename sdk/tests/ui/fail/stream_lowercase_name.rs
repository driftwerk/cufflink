cufflink::service! {
    name: "svc",
    tables: [],
    streams: [("loans", ["loans.created"], limits)],
}

fn main() {}
