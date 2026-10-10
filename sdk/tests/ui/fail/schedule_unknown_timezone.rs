cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("nightly", cron("0 2 * * *"), "run", timezone = "Mars/Olympus")],
}

fn main() {}
