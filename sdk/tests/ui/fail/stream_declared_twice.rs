cufflink::service! {
    name: "svc",
    tables: [],
    streams: [("LOANS", ["loans.a"], limits), ("LOANS", ["loans.b"], limits)],
}

fn main() {}
