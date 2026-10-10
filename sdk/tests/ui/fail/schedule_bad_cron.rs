cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("nightly", cron("61 * * * *"), "run")],
}

fn main() {}
