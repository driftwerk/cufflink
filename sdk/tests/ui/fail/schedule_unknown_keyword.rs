cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("sweep", every(30), "a", jitter = 5)],
}

fn main() {}
