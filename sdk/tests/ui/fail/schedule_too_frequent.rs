cufflink::service! {
    name: "svc",
    tables: [],
    schedules: [("busy", cron("* * * * * *"), "run")],
}

fn main() {}
