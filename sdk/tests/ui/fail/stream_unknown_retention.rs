cufflink::service! {
    name: "svc",
    tables: [],
    streams: [("LOANS", ["loans.created"], forever)],
}

fn main() {}
