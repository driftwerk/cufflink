cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("busy", every(5), "run")],
}

fn main() {}
