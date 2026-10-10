use cufflink_fn::nats::PublishOptions;
use cufflink_fn::prelude::*;
use cufflink_fn::sql::{DbError, Param, Row, Tx};
use cufflink_fn::{http::HttpRequest, jobs};
use std::collections::BTreeMap;
use std::time::Duration;

const RECEIPTS: &str = "receipts";
const RENDER_ATTEMPTS: u32 = 3;
const DEFAULT_PER_PAGE: i64 = 20;
const MAX_PER_PAGE: i64 = 100;
const LOAN_COLUMNS: &str = "id, book_id, memo, loan_kind, created_by, created_at, updated_at";
const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

fn refused(status: u16, message: &str) -> Response {
    Response::error_message(status, message)
}

fn failed(error: &DbError) -> Response {
    refused(500, &error.to_string())
}

fn setting(key: &str) -> String {
    config::get(key).unwrap_or_default()
}

fn subject(name: &str) -> String {
    match setting("SUBJECT_PREFIX") {
        prefix if prefix.is_empty() => name.to_string(),
        prefix => format!("{prefix}.{name}"),
    }
}

fn is_uuid(text: &str) -> bool {
    text.len() == 36
        && text.bytes().enumerate().all(|(at, byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        })
}

struct Librarian {
    id: String,
    name: String,
}

fn permitted(req: &Request, operation: &str) -> Result<(Librarian, bool), Response> {
    let auth = req.require_auth()?;
    if !(auth.is_service_account || auth.can("loans", operation)) {
        return Err(refused(
            403,
            &format!("Unauthorised error: User does not have permission to {operation} loans"),
        ));
    }
    let id = auth
        .claim("user_id")
        .and_then(Value::as_str)
        .filter(|id| is_uuid(id))
        .ok_or_else(|| refused(403, "Unauthorised error: User has no user_id claim"))?;
    Ok((
        Librarian {
            id: id.to_string(),
            name: auth.name.clone().unwrap_or_default(),
        },
        auth.is_service_account,
    ))
}

fn book(req: &Request, book_id: &str) -> Result<Value, Response> {
    let url = format!("{}/books/{book_id}", setting("CATALOG_URL"));
    let request = req
        .header("x-request-id")
        .into_iter()
        .fold(HttpRequest::get(&url), |request, id| {
            request.header("x-request-id", id)
        });
    let unavailable = || refused(502, "Catalog service unavailable");
    match http::fetch_bytes(&request) {
        Ok(found) if found.status == 404 => Err(refused(404, "Book not found")),
        Ok(found) if found.is_success() => found.json::<Value>().map_err(|_| unavailable()),
        Ok(_) | Err(_) => Err(unavailable()),
    }
}

fn withdrawn(book: &Value) -> bool {
    book["status"] == "Withdrawn"
}

fn publish(name: &str, msg_id: &str, event: &Value) {
    let subject = subject(name);
    if let Err(error) = nats::publish_json(&subject, event, &PublishOptions::new().msg_id(msg_id)) {
        log::warn(&format!("{subject} was not published: {error}"));
    }
}

fn audit(operation: &str, by: &Librarian, loan: &Value) {
    let id = loan["id"].as_str().unwrap_or_default();
    publish(
        "audit.logs",
        &format!("loan-{operation}:{id}"),
        &json!({
            "area": "Loans",
            "operation": operation,
            "user_id": by.id,
            "user_name": by.name,
            "entity_id": id,
            "data": {"book_id": loan["book_id"], "memo": loan["memo"]},
            "parent": {"id": loan["book_id"], "area": "Books"},
        }),
    );
}

fn insert_loan(
    tx: &mut Tx,
    book_id: &str,
    memo: &str,
    loan_kind: &str,
    by: &Librarian,
) -> Result<Value, DbError> {
    tx.query_one(
        &format!(
            "INSERT INTO loans ({LOAN_COLUMNS}) VALUES ($1, $2, $3, $4, $5, \
             $6::timestamptz AT TIME ZONE 'UTC', $6::timestamptz AT TIME ZONE 'UTC') \
             RETURNING {LOAN_COLUMNS}"
        ),
        &[
            Param::Uuid(util::generate_uuid()),
            Param::Uuid(book_id.to_string()),
            Param::Text(memo.to_string()),
            Param::Text(loan_kind.to_string()),
            Param::Uuid(by.id.clone()),
            Param::Timestamp(util::current_time()),
        ],
    )
    .map(|row| row.map_or(Value::Null, Row::into_value))
}

pub fn create_loan(req: Request) -> Response {
    let (by, _) = match permitted(&req, "create") {
        Ok(caller) => caller,
        Err(refusal) => return refusal,
    };
    let book_id = req.body()["book_id"].as_str().unwrap_or_default();
    let memo = req.body()["memo"].as_str().unwrap_or_default().trim();
    let errors: BTreeMap<String, Vec<String>> = [
        (is_uuid(book_id), "book_id", "Book id must be a UUID"),
        (!memo.is_empty(), "memo", "Memo cannot be empty"),
    ]
    .into_iter()
    .filter(|(valid, ..)| !valid)
    .map(|(_, field, message)| (field.to_string(), vec![message.to_string()]))
    .collect();
    if !errors.is_empty() {
        return Response::validation_errors(&errors);
    }
    match book(&req, book_id) {
        Ok(book) if withdrawn(&book) => return refused(422, "Cannot lend a withdrawn book"),
        Ok(_) => {}
        Err(refusal) => return refusal,
    }
    match sql::transaction(|tx| insert_loan(tx, book_id, memo, "Standard", &by)) {
        Ok(loan) => {
            audit("Create", &by, &loan);
            let named = json!({"created_by_name": by.name});
            Response::json(&merged(loan, named))
        }
        Err(error) => failed(&error),
    }
}

fn merged(into: Value, from: Value) -> Value {
    match (into, from) {
        (Value::Object(mut into), Value::Object(from)) => {
            into.extend(from);
            Value::Object(into)
        }
        (into, _) => into,
    }
}

pub fn loans_by_book(req: Request) -> Response {
    if let Err(refusal) = permitted(&req, "view") {
        return refusal;
    }
    let book_id = req.path_param("book_id").unwrap_or_default();
    if !is_uuid(book_id) {
        return refused(400, "Invalid identifier");
    }
    let number = |name: &str| {
        req.query_first(name)
            .and_then(|text| text.parse::<i64>().ok())
    };
    let per_page = number("per_page")
        .unwrap_or(DEFAULT_PER_PAGE)
        .clamp(1, MAX_PER_PAGE);
    let page = number("page").unwrap_or(1).max(1);
    let of_book = [Param::Uuid(book_id.to_string())];
    let listed = sql::query_one(
        "SELECT count(*) AS total FROM loans WHERE book_id = $1",
        &of_book,
    )
    .and_then(|counted| counted.map_or(Ok(0), |row| row.get::<i64>("total")))
    .and_then(|total| {
        sql::query(
            &format!(
                "SELECT {LOAN_COLUMNS} FROM loans WHERE book_id = $1 \
                 ORDER BY created_at DESC, id DESC LIMIT $2 OFFSET $3"
            ),
            &[
                Param::Uuid(book_id.to_string()),
                Param::I64(per_page),
                Param::I64((page - 1) * per_page),
            ],
        )
        .map(|rows| (total, rows))
    });
    match listed {
        Ok((total, rows)) => Response::json(&json!({
            "results": rows.into_iter().map(Row::into_value).collect::<Vec<_>>(),
            "pagination": {
                "current_page": page,
                "total_pages": ((total + per_page - 1) / per_page).max(1),
                "per_page": per_page,
                "total_records": total,
            },
        })),
        Err(error) => failed(&error),
    }
}

enum Unhandled {
    Malformed(String),
    Database(DbError),
}

impl From<DbError> for Unhandled {
    fn from(error: DbError) -> Self {
        Unhandled::Database(error)
    }
}

fn field<'a>(event: &'a Value, name: &str) -> Result<&'a str, Unhandled> {
    event[name]
        .as_str()
        .filter(|text| !text.is_empty())
        .ok_or_else(|| Unhandled::Malformed(format!("the event has no {name}")))
}

fn identifier<'a>(event: &'a Value, name: &str) -> Result<&'a str, Unhandled> {
    field(event, name).and_then(|id| {
        is_uuid(id)
            .then_some(id)
            .ok_or_else(|| Unhandled::Malformed(format!("{name} is not a UUID")))
    })
}

fn long_date(timestamp: &str) -> Option<String> {
    let mut parts = timestamp.get(..10)?.split('-');
    let year: u32 = parts.next()?.parse().ok()?;
    let month: usize = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    let month = MONTHS.get(month.checked_sub(1)?)?;
    (1..=31)
        .contains(&day)
        .then(|| format!("{month} {day}, {year}"))
}

fn renewal_set(event: &Value) -> Result<u64, Unhandled> {
    let book_id = identifier(event, "book_id")?;
    let by = Librarian {
        id: identifier(event, "set_by_user_id")?.to_string(),
        name: field(event, "set_by_user_name")?.to_string(),
    };
    let until = long_date(field(event, "renewed_until")?)
        .ok_or_else(|| Unhandled::Malformed("renewed_until is not a date".to_string()))?;
    let memo = format!(
        "Renewed until {until} for {}",
        field(event, "borrower_name")?
    );
    let of_book = [Param::Uuid(book_id.to_string())];
    let renewed = sql::transaction(|tx| {
        let kept = tx.query(
            "SELECT memo FROM loans WHERE book_id = $1 AND loan_kind = 'Renewal' FOR UPDATE",
            &of_book,
        )?;
        let unchanged = kept.len() == 1
            && kept
                .iter()
                .all(|row| row.get::<String>("memo").is_ok_and(|kept| kept == memo));
        if unchanged {
            return Ok(None);
        }
        tx.execute(
            "DELETE FROM loans WHERE book_id = $1 AND loan_kind = 'Renewal'",
            &of_book,
        )?;
        insert_loan(tx, book_id, &memo, "Renewal", &by).map(Some)
    })?;
    Ok(renewed.map_or(0, |loan| {
        audit("Create", &by, &loan);
        1
    }))
}

fn loans_deleted(event: &Value) -> Result<u64, Unhandled> {
    let book_id = identifier(event, "book_id")?;
    Ok(sql::execute(
        "DELETE FROM loans WHERE book_id = $1",
        &[Param::Uuid(book_id.to_string())],
    )?)
}

pub fn on_book_event(req: Request) -> Response {
    let Some(delivery) = req.subscription() else {
        return Response::terminal("a book event came without its delivery");
    };
    let prefix = subject("");
    let name = delivery
        .subject()
        .strip_prefix(prefix.as_str())
        .unwrap_or(delivery.subject());
    let changed = match name {
        "books.renewal.set" => renewal_set(req.body()),
        "books.deleted.loans" => loans_deleted(req.body()),
        _ => {
            return Response::terminal(&format!("no handler for '{}'", delivery.subject()));
        }
    };
    match changed {
        Ok(changed) => Response::json(&json!({"changed": changed})),
        Err(Unhandled::Malformed(reason)) => Response::terminal(&reason),
        Err(Unhandled::Database(error)) => {
            log::error(&format!(
                "{} #{} failed on attempt {}: {error}",
                delivery.stream(),
                delivery.sequence(),
                delivery.attempt()
            ));
            Response::retry_after(Duration::from_secs(5))
        }
    }
}

pub fn delete_loan(req: Request) -> Response {
    let (by, is_service_account) = match permitted(&req, "delete") {
        Ok(caller) => caller,
        Err(refusal) => return refusal,
    };
    let loan_id = req.path_param("loan_id").unwrap_or_default();
    if !is_uuid(loan_id) {
        return refused(400, "Invalid identifier");
    }
    let of_loan = [Param::Uuid(loan_id.to_string())];
    let found = sql::query_one(
        &format!("SELECT {LOAN_COLUMNS} FROM loans WHERE id = $1"),
        &of_loan,
    );
    let loan = match found {
        Ok(Some(row)) => row.into_value(),
        Ok(None) => return refused(404, &format!("Loan not found with id: {loan_id}")),
        Err(error) => return failed(&error),
    };
    if !is_service_account && loan["created_by"] != by.id.as_str() {
        return refused(
            403,
            "Unauthorised error: User can only delete their own loans",
        );
    }
    let book_id = loan["book_id"].as_str().unwrap_or_default();
    match book(&req, book_id) {
        Ok(book) if withdrawn(&book) => {
            return refused(400, &format!("Book is withdrawn: {book_id}"));
        }
        Ok(_) => {}
        Err(refusal) => return refusal,
    }
    if let Err(error) = sql::execute("DELETE FROM loans WHERE id = $1", &of_loan) {
        return failed(&error);
    }
    if loan["loan_kind"] == "Renewal" {
        publish(
            "loans.renewal.deleted",
            &format!("renewal-deleted:{loan_id}"),
            &json!({
                "loan_id": loan_id,
                "book_id": book_id,
                "deleted_by_user_id": by.id,
                "deleted_by_user_name": by.name,
                "deleted_at": util::current_time(),
            }),
        );
    }
    audit("Delete", &by, &loan);
    Response::json(&json!({"message": "Loan deleted successfully"}))
}

pub fn overdue_reminders(req: Request) -> Response {
    let Some(fire) = req.schedule() else {
        return Response::error("only the platform fires a schedule");
    };
    let renewed = sql::query(
        "SELECT DISTINCT book_id FROM loans WHERE loan_kind = 'Renewal' ORDER BY book_id",
        &[],
    );
    match renewed {
        Ok(rows) => {
            let enqueued = rows
                .iter()
                .filter_map(|row| row.get::<String>("book_id").ok())
                .filter_map(|book_id| {
                    jobs::enqueue(
                        "send_overdue_reminder",
                        &json!({"book_id": book_id, "fire_at": fire.fire_at()}),
                    )
                })
                .count();
            Response::json(&json!({
                "schedule": fire.name(),
                "catch_up": fire.is_catch_up(),
                "enqueued": enqueued,
            }))
        }
        Err(error) => {
            log::error(&format!(
                "{} could not read the loans: {error}",
                fire.name()
            ));
            Response::retry_after(Duration::from_secs(30))
        }
    }
}

pub fn send_overdue_reminder(req: Request) -> Response {
    let (Some(book_id), Some(fire_at)) = (
        req.body()["book_id"].as_str().filter(|id| is_uuid(id)),
        req.body()["fire_at"].as_str(),
    ) else {
        return Response::terminal("a reminder needs a book_id and a fire_at");
    };
    let of_reminder = [
        Param::Uuid(book_id.to_string()),
        Param::Text(fire_at.to_string()),
    ];
    match sql::query_one(
        "SELECT id FROM reminders WHERE book_id = $1 AND fire_at = $2",
        &of_reminder,
    ) {
        Ok(Some(_)) => return Response::json(&json!({"sent": false})),
        Ok(None) => {}
        Err(error) => return failed(&error),
    }
    let email = json!({
        "to": setting("REMINDERS_TO"),
        "subject": "A renewal is overdue",
        "text": format!("The renewal of book {book_id} was due at {fire_at}."),
        "metadata": {"book_id": book_id, "fire_at": fire_at},
    });
    let sent = HttpRequest::post(&format!("{}/messages", setting("MAIL_API_URL")))
        .header(
            "authorization",
            &format!("Bearer {}", setting("MAIL_API_TOKEN")),
        )
        .json(&email)
        .and_then(|request| http::fetch_bytes(&request));
    match sent {
        Ok(answer) if answer.is_success() => {
            let recorded = sql::execute(
                "INSERT INTO reminders (id, book_id, fire_at, sent_at) VALUES ($1, $2, $3, $4)",
                &[
                    Param::Uuid(util::generate_uuid()),
                    Param::Uuid(book_id.to_string()),
                    Param::Text(fire_at.to_string()),
                    Param::Timestamp(util::current_time()),
                ],
            );
            match recorded {
                Ok(_) => Response::json(&json!({"sent": true})),
                Err(error) => failed(&error),
            }
        }
        Ok(answer) if (400..500).contains(&answer.status) => {
            Response::terminal("The mail API refused the email")
        }
        Ok(_) | Err(_) => Response::retry_after(Duration::from_secs(60)),
    }
}

fn rendered(html: &str) -> Result<Vec<u8>, String> {
    let convert = HttpRequest::post(&format!("{}/convert/html", setting("PDF_RENDERER_URL")));
    let mut failure = String::new();
    for attempt in 0..RENDER_ATTEMPTS {
        if attempt > 0 {
            util::sleep_ms(250 << attempt);
        }
        let page = http::Part::file("files", "index.html", "text/html", html.as_bytes().to_vec());
        failure = match http::multipart(&convert, &[page]) {
            Ok(answer) if answer.is_success() => return Ok(answer.into_bytes()),
            Ok(answer) => format!("The PDF renderer answered {}", answer.status),
            Err(error) => format!("The PDF renderer did not answer: {error}"),
        };
    }
    Err(failure)
}

pub fn render_receipt_pdf(req: Request) -> Response {
    let text = |name: &str| req.body()[name].as_str().unwrap_or_default();
    let key = format!("{}/{}.pdf", text("book_id"), text("receipt_id"));
    match storage::head(RECEIPTS, &key) {
        Ok(Some(stored)) => {
            return Response::json(&json!({"key": key, "size": stored.size, "rendered": false}));
        }
        Ok(None) => {}
        Err(error) => return refused(502, &error.to_string()),
    }
    if let Some(job) = req.job().filter(|job| job.is_retry()) {
        log::info(&format!(
            "rendering {key} again, attempt {} of {}, after: {}",
            job.attempt,
            job.max_attempts,
            job.last_error().unwrap_or("an unknown failure")
        ));
    }
    let pdf = match rendered(text("html")) {
        Ok(pdf) => pdf,
        Err(reason) => return refused(502, &reason),
    };
    match storage::upload(RECEIPTS, &key, "application/pdf", &pdf) {
        Ok(stored) => Response::json(&json!({
            "key": key,
            "size": stored.size,
            "rendered": true,
            "attempt": req.job().map_or(1, |job| job.attempt),
        })),
        Err(error) => refused(502, &error.to_string()),
    }
}
