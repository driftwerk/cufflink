#![cfg(all(
    feature = "test-host",
    feature = "db-v2",
    feature = "nats-publish-v2",
    feature = "http-v2",
    feature = "storage-v2",
    feature = "util-v2",
    feature = "delivery-control"
))]

mod library_example;
mod support;

use cufflink_fn::testing::{
    AuthBuilder, FakeClock, MigrationHistory, MigrationLock, SubscriptionBuilder, TestDb, TestHost,
    UrlMatch,
};
use cufflink_fn::{DeliveryControl, Response};
use cufflink_types::host::http::HttpErrorKind;
use cufflink_types::host::request::JobMeta;
use serde_json::{json, Value};
use support::{database, loan_migrations};

const CATALOG: &str = "https://catalog.example.com";
const PDF_RENDERER: &str = "https://pdf.example.com";
const MAIL_API: &str = "https://mail.example.com";
const BOOK: &str = "11111111-1111-4111-8111-111111111111";
const ADA: &str = "22222222-2222-4222-8222-222222222222";
const GRACE: &str = "33333333-3333-4333-8333-333333333333";
const NOW: i64 = 1_790_589_600_000;

fn loan_service() -> Option<(TestDb, TestHost)> {
    let db = database()?;
    let history = MigrationHistory {
        table: "schema_history".into(),
        lock: MigrationLock::LegacyTableHash,
        ..MigrationHistory::default()
    };
    db.migrate("loan_service", &loan_migrations(), &history)
        .expect("migrations");
    db.execute(
        "CREATE TABLE loan_service.reminders (id uuid PRIMARY KEY, book_id uuid NOT NULL, \
         fire_at text NOT NULL, sent_at timestamptz NOT NULL)",
    )
    .expect("the table the service's manifest declares");
    let host = TestHost::builder()
        .tenant("harbor")
        .service("loan-service")
        .clock(FakeClock::at(NOW))
        .db(&db)
        .build()
        .expect("host");
    host.config.set("CATALOG_URL", CATALOG);
    host.config.set("PDF_RENDERER_URL", PDF_RENDERER);
    host.config.set("MAIL_API_URL", MAIL_API);
    host.config.set("MAIL_API_TOKEN", "mail-token");
    host.config.set("REMINDERS_TO", "desk@example.com");
    Some((db, host))
}

fn user(user_id: &str, name: &str) -> AuthBuilder {
    AuthBuilder::user(&format!("kc-{name}"))
        .name(name)
        .claim("user_id", user_id)
        .claim("branch_id", "44444444-4444-4444-8444-444444444444")
        .permissions(&["loans:create", "loans:view", "loans:delete"])
}

fn available_book(host: &TestHost) {
    host.http
        .expect("GET", UrlMatch::exact(format!("{CATALOG}/books/{BOOK}")))
        .respond(
            200,
            &[("content-type", "application/json")],
            json!({"id": BOOK, "status": "Available"}).to_string(),
        );
}

fn body(response: &Response) -> Value {
    response.json_as().expect("a JSON body")
}

fn create(host: &TestHost, auth: AuthBuilder, memo: &str) -> Response {
    let request = host
        .request()
        .handler("create_loan")
        .header("x-request-id", "rer-7")
        .auth(auth)
        .json(&json!({"book_id": BOOK, "memo": memo}))
        .build()
        .expect("request");
    host.call(library_example::create_loan, request)
}

fn loans(db: &TestDb) -> Vec<Value> {
    db.query("SELECT memo, loan_kind, created_by FROM loan_service.loans ORDER BY created_at, id")
        .expect("loans")
}

#[test]
fn a_loan_is_created_stored_and_audited() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    available_book(&host);
    let created = create(&host, user(ADA, "Ada"), "  Two weeks, front desk  ");
    assert_eq!(created.status(), 200);
    assert_eq!(
        body(&created),
        json!({
            "id": "00000000-0000-4000-8000-000000000001",
            "book_id": BOOK,
            "memo": "Two weeks, front desk",
            "loan_kind": "Standard",
            "created_by": ADA,
            "created_by_name": "Ada",
            "created_at": "2026-09-28T10:00:00",
            "updated_at": "2026-09-28T10:00:00",
        })
    );
    assert_eq!(
        loans(&db),
        [json!({"memo": "Two weeks, front desk", "loan_kind": "Standard", "created_by": ADA})]
    );

    let asked = host.http.requests();
    assert_eq!(asked.len(), 1);
    assert_eq!(
        asked[0].headers,
        [("x-request-id".to_string(), "rer-7".to_string())]
    );
    assert_eq!(host.verify(), Ok(()));

    let published = host.nats.published_to("audit.logs");
    assert_eq!(published.len(), 1);
    assert_eq!(
        published[0].msg_id.as_deref(),
        Some("loan-Create:00000000-0000-4000-8000-000000000001")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&published[0].payload).expect("an event"),
        json!({
            "area": "Loans",
            "operation": "Create",
            "user_id": ADA,
            "user_name": "Ada",
            "entity_id": "00000000-0000-4000-8000-000000000001",
            "data": {"book_id": BOOK, "memo": "Two weeks, front desk"},
            "parent": {"id": BOOK, "area": "Books"},
        })
    );
}

#[test]
fn a_loan_that_may_not_be_created_is_refused() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    let refused = |response: Response| (response.status(), body(&response));
    assert_eq!(
        refused(create(&host, user(ADA, "Ada"), "   ")),
        (
            422,
            json!({"validation_errors": {"memo": ["Memo cannot be empty"]}})
        )
    );
    assert_eq!(
        refused(create(
            &host,
            user(ADA, "Ada").permissions(&["loans:view"]),
            "x"
        )),
        (
            403,
            json!({"error_message": "Unauthorised error: User does not have permission to create loans"})
        )
    );
    let anonymous = host.call(
        library_example::create_loan,
        host.request().json(&json!({})).build().expect("request"),
    );
    assert_eq!(anonymous.status(), 401);
    assert!(
        host.http.requests().is_empty(),
        "nothing was asked of the catalog"
    );

    let book = UrlMatch::exact(format!("{CATALOG}/books/{BOOK}"));
    host.http
        .expect("GET", book.clone())
        .respond(404, &[], "")
        .times(1);
    host.http
        .expect("GET", book.clone())
        .fail(HttpErrorKind::Timeout, "the catalog did not answer")
        .times(1);
    host.http
        .expect("GET", book)
        .respond(
            200,
            &[],
            json!({"id": BOOK, "status": "Withdrawn"}).to_string(),
        )
        .times(1);
    assert_eq!(
        refused(create(&host, user(ADA, "Ada"), "x")),
        (404, json!({"error_message": "Book not found"}))
    );
    assert_eq!(
        refused(create(&host, user(ADA, "Ada"), "x")),
        (502, json!({"error_message": "Catalog service unavailable"}))
    );
    assert_eq!(
        refused(create(&host, user(ADA, "Ada"), "x")),
        (
            422,
            json!({"error_message": "Cannot lend a withdrawn book"})
        )
    );
    assert_eq!(host.verify(), Ok(()));
    assert!(loans(&db).is_empty() && host.nats.published().is_empty());

    available_book(&host);
    let ops = AuthBuilder::service_account("service-account-harbor-ops")
        .name("Harbor Ops")
        .claim("user_id", GRACE);
    assert_eq!(
        create(&host, ops, "Imported").status(),
        200,
        "a service account needs no permission"
    );
}

#[test]
fn the_loans_of_a_book_are_listed_by_the_page() {
    let Some((_db, host)) = loan_service() else {
        return;
    };
    available_book(&host);
    (1..=5).for_each(|n| {
        host.clock.advance(1_000);
        assert_eq!(
            create(&host, user(ADA, "Ada"), &format!("loan {n}")).status(),
            200
        );
    });
    let page = |query: &[(&str, &str)]| {
        let request = query
            .iter()
            .fold(
                host.request()
                    .method("GET")
                    .path(&format!("/loans/book/{BOOK}"))
                    .path_param("book_id", BOOK)
                    .auth(user(GRACE, "Grace")),
                |request, (name, value)| request.query(name, value),
            )
            .build()
            .expect("request");
        body(&host.call(library_example::loans_by_book, request))
    };
    let memos = |listed: &Value| -> Vec<String> {
        listed["results"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|loan| loan["memo"].as_str().map(str::to_string))
            .collect()
    };
    let second = page(&[("page", "2"), ("per_page", "2")]);
    assert_eq!(memos(&second), ["loan 3", "loan 2"]);
    assert_eq!(
        second["pagination"],
        json!({"current_page": 2, "total_pages": 3, "per_page": 2, "total_records": 5})
    );
    assert!(second["results"][0].get("total_records").is_none());
    let all = page(&[]);
    assert_eq!(memos(&all).len(), 5);
    assert_eq!(all["pagination"]["per_page"], 20);
    assert_eq!(
        page(&[("per_page", "1000"), ("page", "0")])["pagination"],
        json!({"current_page": 1, "total_pages": 1, "per_page": 100, "total_records": 5})
    );
}

fn book_event(host: &TestHost, subject: &str, attempt: u64, event: &Value) -> Response {
    let request = host
        .request()
        .handler("on_book_event")
        .json(event)
        .subscription(
            SubscriptionBuilder::on(subject)
                .stream("BOOKS")
                .consumer("loan-service-book-consumer")
                .sequence(81_234)
                .attempt(attempt),
        )
        .build()
        .expect("request");
    host.call(library_example::on_book_event, request)
}

fn renewal(until: &str, borrower: &str) -> Value {
    json!({
        "book_id": BOOK,
        "renewed_until": until,
        "borrower_name": borrower,
        "set_by_user_id": GRACE,
        "set_by_user_name": "Grace",
        "set_at": "2026-09-28T09:59:58Z",
    })
}

#[test]
fn a_book_event_keeps_one_renewal_loan_however_often_it_is_delivered() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    const SET: &str = "books.renewal.set";
    let first = book_event(
        &host,
        SET,
        1,
        &renewal("2026-10-05T09:30:00", "Ada Lovelace"),
    );
    assert_eq!((first.status(), body(&first)), (200, json!({"changed": 1})));
    let again = book_event(
        &host,
        SET,
        2,
        &renewal("2026-10-05T09:30:00", "Ada Lovelace"),
    );
    assert_eq!(body(&again), json!({"changed": 0}), "a redelivery");
    assert_eq!(
        loans(&db),
        [json!({
            "memo": "Renewed until October 5, 2026 for Ada Lovelace",
            "loan_kind": "Renewal",
            "created_by": GRACE,
        })]
    );
    book_event(
        &host,
        SET,
        1,
        &renewal("2026-11-12T16:00:00", "Ada Lovelace"),
    );
    assert_eq!(
        loans(&db),
        [json!({
            "memo": "Renewed until November 12, 2026 for Ada Lovelace",
            "loan_kind": "Renewal",
            "created_by": GRACE,
        })],
        "the renewal it replaces is gone"
    );

    let deleted = book_event(&host, "books.deleted.loans", 1, &json!({"book_id": BOOK}));
    assert_eq!(body(&deleted), json!({"changed": 1}));
    assert!(loans(&db).is_empty());
}

#[test]
fn a_book_event_that_cannot_be_handled_is_given_up_or_asked_for_again() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    let settled = |response: Response| response.delivery().cloned();
    assert_eq!(
        settled(book_event(&host, "books.status.changed", 1, &json!({}))),
        Some(DeliveryControl::Terminal {
            terminal: "no handler for 'books.status.changed'".into()
        })
    );
    let malformed = settled(book_event(
        &host,
        "books.deleted.loans",
        1,
        &json!({"book_id": "not-a-book"}),
    ));
    assert!(
        matches!(malformed, Some(DeliveryControl::Terminal { .. })),
        "{malformed:?}"
    );
    db.execute("ALTER TABLE loan_service.loans RENAME TO loans_moved")
        .expect("the table goes away");
    assert_eq!(
        settled(book_event(
            &host,
            "books.deleted.loans",
            3,
            &json!({"book_id": BOOK})
        )),
        Some(DeliveryControl::RetryAfter {
            retry_after_ms: 5_000
        })
    );
    let without_delivery = host
        .request()
        .json(&json!({"book_id": BOOK}))
        .subscription_without_delivery()
        .build()
        .expect("request");
    assert!(matches!(
        settled(host.call(library_example::on_book_event, without_delivery)),
        Some(DeliveryControl::Terminal { .. })
    ));
}

#[test]
fn deleting_a_renewal_loan_is_announced() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    book_event(
        &host,
        "books.renewal.set",
        1,
        &renewal("2026-10-05T09:30:00", "Ada Lovelace"),
    );
    let renewed = "00000000-0000-4000-8000-000000000001";
    let delete = |auth: AuthBuilder, loan_id: &str| {
        let request = host
            .request()
            .method("DELETE")
            .path(&format!("/loans/{loan_id}"))
            .path_param("loan_id", loan_id)
            .auth(auth)
            .build()
            .expect("request");
        let response = host.call(library_example::delete_loan, request);
        (response.status(), body(&response))
    };
    assert_eq!(
        delete(user(ADA, "Ada"), renewed),
        (
            403,
            json!({"error_message": "Unauthorised error: User can only delete their own loans"})
        )
    );
    assert_eq!(
        delete(user(GRACE, "Grace"), "99999999-9999-4999-8999-999999999999"),
        (
            404,
            json!({"error_message": "Loan not found with id: 99999999-9999-4999-8999-999999999999"})
        )
    );
    assert_eq!(
        delete(user(GRACE, "Grace").permissions(&["loans:view"]), renewed),
        (
            403,
            json!({"error_message": "Unauthorised error: User does not have permission to delete loans"})
        ),
        "the handler checks what the route's posture checks before it on the platform"
    );
    assert_eq!(
        delete(user(GRACE, "Grace"), "not-a-loan"),
        (400, json!({"error_message": "Invalid identifier"}))
    );
    assert_eq!(loans(&db).len(), 1);
    assert!(host.nats.published_to("loans.renewal.deleted").is_empty());

    available_book(&host);
    host.clock.advance(90_000);
    assert_eq!(
        delete(user(GRACE, "Grace"), renewed),
        (200, json!({"message": "Loan deleted successfully"}))
    );
    assert!(loans(&db).is_empty());
    let announced = host.nats.published_to("loans.renewal.deleted");
    assert_eq!(announced.len(), 1);
    assert_eq!(
        announced[0].msg_id.as_deref(),
        Some("renewal-deleted:00000000-0000-4000-8000-000000000001")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&announced[0].payload).expect("an event"),
        json!({
            "loan_id": renewed,
            "book_id": BOOK,
            "deleted_by_user_id": GRACE,
            "deleted_by_user_name": "Grace",
            "deleted_at": "2026-09-28T10:01:30.000+00:00",
        })
    );
    assert_eq!(
        host.nats
            .published_to("audit.logs")
            .iter()
            .map(
                |audit| serde_json::from_slice::<Value>(&audit.payload).expect("an audit")
                    ["operation"]
                    .clone()
            )
            .collect::<Vec<_>>(),
        [json!("Create"), json!("Delete")],
        "the renewal loan the event made, and its deletion"
    );
    assert_eq!(host.verify(), Ok(()));
}

#[test]
fn the_loan_of_a_withdrawn_book_is_kept() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    let loan = "00000000-0000-4000-8000-000000000009";
    db.execute(&format!(
        "INSERT INTO loan_service.loans \
         (id, book_id, memo, loan_kind, created_by, created_at, updated_at) \
         VALUES ('{loan}', '{BOOK}', 'Lent to Ada', 'Standard', '{GRACE}', now(), now())"
    ))
    .expect("a loan");
    host.http
        .expect("GET", UrlMatch::exact(format!("{CATALOG}/books/{BOOK}")))
        .respond(
            200,
            &[("content-type", "application/json")],
            json!({"id": BOOK, "status": "Withdrawn"}).to_string(),
        );
    let request = host
        .request()
        .method("DELETE")
        .path(&format!("/loans/{loan}"))
        .path_param("loan_id", loan)
        .auth(user(GRACE, "Grace"))
        .build()
        .expect("request");
    let refused = host.call(library_example::delete_loan, request);
    assert_eq!(
        (refused.status(), body(&refused)),
        (
            400,
            json!({"error_message": format!("Book is withdrawn: {BOOK}")})
        )
    );
    assert_eq!(loans(&db).len(), 1);
    assert!(host.nats.published().is_empty());
}

#[test]
fn an_announcement_no_stream_takes_leaves_the_deletion_done() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    host.config.set("SUBJECT_PREFIX", "m1");
    host.nats.captures("AUDIT", "m1.audit.logs");
    book_event(
        &host,
        "m1.books.renewal.set",
        1,
        &renewal("2026-10-05T09:30:00", "Ada Lovelace"),
    );
    available_book(&host);
    let request = host
        .request()
        .method("DELETE")
        .path("/loans/00000000-0000-4000-8000-000000000001")
        .path_param("loan_id", "00000000-0000-4000-8000-000000000001")
        .auth(user(GRACE, "Grace"))
        .build()
        .expect("request");
    let deleted = host.call(library_example::delete_loan, request);
    assert_eq!(
        (deleted.status(), body(&deleted)),
        (200, json!({"message": "Loan deleted successfully"})),
        "the service logs an announcement that failed and goes on"
    );
    assert!(loans(&db).is_empty());
    assert_eq!(
        host.nats.published_to("m1.loans.renewal.deleted").len(),
        1,
        "announced once, and refused"
    );
    assert_eq!(host.nats.published_to("m1.audit.logs").len(), 2);
}

#[test]
fn a_schedule_enqueues_a_reminder_for_every_renewal_that_is_due() {
    let Some((db, host)) = loan_service() else {
        return;
    };
    let other = "55555555-5555-4555-8555-555555555555";
    for book in [BOOK, other] {
        db.execute(&format!(
            "INSERT INTO loan_service.loans \
             (id, book_id, memo, loan_kind, created_by, created_at, updated_at) \
             VALUES (gen_random_uuid(), '{book}', 'Renewed', 'Renewal', '{ADA}', now(), now()), \
                    (gen_random_uuid(), '{book}', 'Lent', 'Standard', '{ADA}', now(), now())"
        ))
        .expect("seed");
    }
    let fire = host
        .request()
        .handler("overdue_reminders")
        .schedule_catch_up("overdue_reminders", "2026-09-28T10:00:00Z")
        .build()
        .expect("request");
    let ran = host.call(library_example::overdue_reminders, fire);
    assert_eq!(
        body(&ran),
        json!({"schedule": "overdue_reminders", "catch_up": true, "enqueued": 2})
    );
    let enqueued = host.jobs.enqueued();
    assert_eq!(
        enqueued
            .iter()
            .map(|job| (job.handler.as_str(), job.payload.clone()))
            .collect::<Vec<_>>(),
        [BOOK, other].map(|book| (
            "send_overdue_reminder",
            json!({"book_id": book, "fire_at": "2026-09-28T10:00:00Z"})
        ))
    );
    let posted = host
        .request()
        .handler("overdue_reminders")
        .build()
        .expect("request");
    assert_eq!(
        host.call(library_example::overdue_reminders, posted)
            .status(),
        400,
        "only the platform fires a schedule"
    );

    host.http
        .expect("POST", UrlMatch::exact(format!("{MAIL_API}/messages")))
        .respond(200, &[], r#"{"id":"m-1"}"#)
        .times(1);
    host.http
        .expect("POST", UrlMatch::exact(format!("{MAIL_API}/messages")))
        .respond(422, &[], r#"{"error":"inactive recipient"}"#)
        .times(1);
    let reminder = |payload: &Value, attempt: u32| {
        let request = host
            .request()
            .enqueued()
            .handler("send_overdue_reminder")
            .json(payload)
            .job(attempt)
            .build()
            .expect("request");
        host.call(library_example::send_overdue_reminder, request)
    };
    assert_eq!(
        body(&reminder(&enqueued[0].payload, 1)),
        json!({"sent": true})
    );
    assert_eq!(
        body(&reminder(&enqueued[0].payload, 2)),
        json!({"sent": false}),
        "a job that runs again sends nothing again"
    );
    assert_eq!(
        reminder(&enqueued[1].payload, 1).delivery(),
        Some(&DeliveryControl::Terminal {
            terminal: "The mail API refused the email".into()
        })
    );
    assert_eq!(
        db.query("SELECT book_id, fire_at, sent_at FROM loan_service.reminders")
            .expect("reminders"),
        [json!({
            "book_id": BOOK,
            "fire_at": "2026-09-28T10:00:00Z",
            "sent_at": "2026-09-28T10:00:00+00:00",
        })]
    );
    assert_eq!(host.verify(), Ok(()));
    let sent = &host.http.requests()[0];
    assert_eq!(
        serde_json::from_slice::<Value>(sent.body.as_deref().unwrap_or_default())
            .expect("an email")["metadata"],
        json!({"book_id": BOOK, "fire_at": "2026-09-28T10:00:00Z"})
    );
    assert!(sent
        .headers
        .contains(&("authorization".into(), "Bearer mail-token".into())));
}

#[test]
fn a_receipt_is_rendered_once_the_renderer_answers_and_stored() {
    let Some((_db, host)) = loan_service() else {
        return;
    };
    let convert = UrlMatch::exact(format!("{PDF_RENDERER}/convert/html"));
    host.http
        .expect("POST", convert.clone())
        .respond(503, &[], "busy")
        .times(1);
    host.http
        .expect("POST", convert.clone())
        .fail(HttpErrorKind::Connect, "refused")
        .times(1);
    host.http
        .expect("POST", convert.clone())
        .respond(
            200,
            &[("content-type", "application/pdf")],
            b"%PDF-1.7".to_vec(),
        )
        .times(1);
    let render = |attempt: u32| {
        let request = host
            .request()
            .handler("render_receipt_pdf")
            .json(&json!({"book_id": BOOK, "receipt_id": "r-7", "html": "<h1>Receipt</h1>"}))
            .job_with(JobMeta {
                id: "j-1".into(),
                attempt,
                max_attempts: 3,
                last_error: (attempt > 1).then(|| "The PDF renderer answered 503".to_string()),
                ..JobMeta::default()
            })
            .build()
            .expect("request");
        host.call(library_example::render_receipt_pdf, request)
    };
    let started = std::time::Instant::now();
    let rendered = render(1);
    assert_eq!(
        body(&rendered),
        json!({"key": format!("{BOOK}/r-7.pdf"), "size": 8, "rendered": true, "attempt": 1})
    );
    assert_eq!(host.clock.slept_ms(), 1_500, "500 ms, then 1 s");
    assert!(started.elapsed() < std::time::Duration::from_secs(1));
    let stored = host
        .storage
        .object("receipts", &format!("{BOOK}/r-7.pdf"))
        .expect("stored");
    assert_eq!(
        (stored.content_type.as_str(), stored.body.as_slice()),
        ("application/pdf", &b"%PDF-1.7"[..])
    );
    let asked = host.http.requests();
    assert_eq!(asked.len(), 3);
    let page = asked[2].multipart.as_deref().expect("a form");
    assert_eq!(page.len(), 1);
    assert_eq!(
        (page[0].name.as_str(), page[0].filename.as_deref()),
        ("files", Some("index.html"))
    );
    assert_eq!(page[0].body, b"<h1>Receipt</h1>");

    assert_eq!(
        body(&render(2)),
        json!({"key": format!("{BOOK}/r-7.pdf"), "size": 8, "rendered": false}),
        "what is stored is not rendered again"
    );
    assert_eq!(host.http.requests().len(), 3);
    assert_eq!(host.verify(), Ok(()));
}

#[test]
fn a_receipt_the_renderer_never_renders_fails_its_job() {
    let Some((_db, host)) = loan_service() else {
        return;
    };
    host.http
        .expect("POST", UrlMatch::prefix(format!("{PDF_RENDERER}/convert/")))
        .respond(503, &[], "busy")
        .times(3);
    let request = host
        .request()
        .json(&json!({"book_id": BOOK, "receipt_id": "r-8", "html": "<p>"}))
        .job(1)
        .build()
        .expect("request");
    let failed = host.call(library_example::render_receipt_pdf, request);
    assert_eq!(
        (failed.status(), body(&failed)),
        (
            502,
            json!({"error_message": "The PDF renderer answered 503"})
        )
    );
    assert!(failed.delivery().is_none(), "the route's retry_on decides");
    assert!(host.storage.keys("receipts").is_empty());

    let denied = TestHost::builder().build().expect("host");
    denied.config.set("PDF_RENDERER_URL", PDF_RENDERER);
    let unreachable = denied.call(
        library_example::render_receipt_pdf,
        denied
            .request()
            .json(&json!({"book_id": BOOK, "receipt_id": "r-9", "html": "<p>"}))
            .build()
            .expect("request"),
    );
    assert_eq!(unreachable.status(), 502);
}
