use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(
    name = "loans",
    auth_required,
    permission_area = "loans",
    managed = false,
    search = ["title", "body"],
    search_config = "english",
    pagination = keyset,
    audit_redact = ["body", "bank_*"],
    sensitive,
    check(name = "title_present", expr = "length(title) > 0"),
    index(name = "uq_loans_book_title", columns = ["book_id", "title"], unique, where = "deleted_at IS NULL"),
    index(name = "loans_tags_gin", columns = ["tags"], using = "gin"),
)]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[index]
    pub book_id: Uuid,
    #[column(varchar(120))]
    pub title: String,
    pub body: String,
    #[column(numeric(12, 2))]
    pub fee: String,
    #[validate(one_of("call", "email", "meeting"))]
    pub kind: String,
    pub tags: Vec<String>,
    pub attachment: Option<Vec<u8>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

cufflink::service! {
    name: "loan-service",
    tables: [Loan],
    mode: wasm,
    schema: adopted,
    schema_name: "loan_service",
    consumers: scoped,
    streams: [
        ("LOANS", ["loans.created", "loans.updated", "loans.deleted", "loans.renewal.deleted"], limits,
         max_age = 2592000, max_msgs = 100000, max_bytes = 268435456),
        ("LOAN_JOBS", ["loans.jobs.>"], work_queue, storage = memory, duplicate_window = 120,
         discard = new, description = "loan jobs"),
    ],
    subscriptions: [
        ("books.renewal.set", "on_book_event", wasm, "BOOKS",
         durable = "loan-service-book-consumer", ack = on_success, max_deliver = 3, ack_wait = 30,
         max_ack_pending = 1000, filters = ["books.deleted.loans"]),
        ("dw.{tenant}.loans.archived", "on_archived", wasm, ack = on_success, max_deliver = -1,
         backoff = [5, 30, 300], batch = 16, deliver = new, concurrency = 4, dead_letter = false,
         timeout = 120),
        ("cufflink.tick.hourly", "sweep", wasm, "CUFFLINK_TICKS"),
        ("dw.{tenant}.books.deleted", "cascade", delete_cascade),
    ],
    schedules: [
        ("order_retry", every(30), "retry_orders"),
        ("payment_reminders", cron("0 * * * *"), "run_payment_reminders", timezone = "Europe/London",
         job(timeout = 900, retry_on = [error, timeout], backoff(base = 10, max = 600))),
        ("nightly", cron("0 30 2 * * *"), "nightly", overlap = allow, catch_up = all, timeout = 120),
    ],
    custom_routes: [
        ("POST", "/loans/export", "export_loans", perm("loans", "view"),
         job(timeout = 900, max_attempts = 5, retry_on = [error])),
        ("POST", "/loans/pay", "pay", auth, audit_redact = ["card.number", "lines[].sku"]),
    ],
}
