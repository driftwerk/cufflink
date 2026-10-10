cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("sweep", every(30), "a"), ("sweep", every(60), "b")],
}

fn main() {}
