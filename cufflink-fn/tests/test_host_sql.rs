#![cfg(all(feature = "test-host", feature = "db-v2"))]

mod support;

use cufflink_fn::sql::{self, DbError, DbErrorKind, Isolation, Param, TxOptions};
use cufflink_fn::testing::{AuthBuilder, MigrationHistory, TestDb, TestHost};
use cufflink_fn::{params, Request, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use support::{database, loan_migrations};

const BOOK: &str = "11111111-1111-4111-8111-111111111111";

#[derive(Debug, Deserialize, PartialEq)]
struct Loan {
    id: String,
    memo: String,
    loan_kind: String,
    created_by: String,
}

fn loans() -> Option<(TestDb, TestHost)> {
    let db = database()?;
    let history = MigrationHistory {
        table: "schema_history".into(),
        ..MigrationHistory::default()
    };
    db.migrate("loan_service", &loan_migrations(), &history)
        .expect("migrations");
    let host = TestHost::builder()
        .tenant("harbor")
        .service("loan-service")
        .db(&db)
        .build()
        .expect("host");
    Some((db, host))
}

fn failed(error: DbError) -> Response {
    match error.kind {
        DbErrorKind::UniqueViolation => Response::error_message(409, "Loan already exists"),
        _ => Response::error_message(500, &error.to_string()),
    }
}

fn create_loan(request: Request) -> Response {
    let Ok(auth) = request.require_auth() else {
        return Response::error_message(401, "Unauthorised");
    };
    let text = |key: &str| request.body()[key].as_str().unwrap_or_default().to_string();
    let inserted = sql::query_as::<Loan>(
        "INSERT INTO loans (id, book_id, memo, created_by, created_at, updated_at) \
         VALUES ($1, $2, $3, $4, now(), now()) \
         RETURNING id, memo, loan_kind, created_by",
        &[
            Param::Uuid(text("id")),
            Param::Uuid(text("book_id")),
            Param::Text(text("memo")),
            Param::Uuid(auth.sub.clone()),
        ],
    );
    match inserted {
        Ok(loans) => Response::json(&json!({ "created": loans.len(), "id": loans[0].id })),
        Err(error) => failed(error),
    }
}

fn move_loans(request: Request) -> Response {
    let text = |key: &str| request.body()[key].as_str().unwrap_or_default().to_string();
    let (from, to) = (text("from"), text("to"));
    let moved = sql::transaction_with(
        TxOptions {
            isolation: Isolation::Serializable,
            read_only: false,
        },
        |tx| {
            tx.advisory_xact_lock(7)?;
            let held: i64 = tx
                .query_one(
                    "SELECT count(*) AS held FROM loans WHERE book_id = $1",
                    &[Param::Uuid(from.clone())],
                )?
                .map(|row| row.get("held"))
                .transpose()?
                .unwrap_or(0);
            let moved = tx.execute(
                "UPDATE loans SET book_id = $2 WHERE book_id = $1",
                &[Param::Uuid(from.clone()), Param::Uuid(to.clone())],
            )?;
            let kept = tx.savepoint(|inner| {
                inner.execute(
                    "UPDATE loans SET loan_kind = $1 WHERE book_id = $2",
                    &params!["Renewal", Param::Uuid(to.clone())],
                )?;
                Err::<(), _>(DbError::new(DbErrorKind::CheckViolation, "not after all"))
            });
            if held > 2 {
                return Err(DbError::new(
                    DbErrorKind::CheckViolation,
                    "too many to move",
                ));
            }
            Ok(json!({ "moved": moved, "kept_kind": kept.is_err() }))
        },
    );
    match moved {
        Ok(moved) => Response::json(&moved),
        Err(error) => Response::error_message(422, &error.message),
    }
}

fn create(host: &TestHost, id: u32, book: &str) -> Response {
    let request = host
        .request()
        .handler("create_loan_handler")
        .auth(
            AuthBuilder::user("22222222-2222-4222-8222-222222222222")
                .permissions(&["loans:create"]),
        )
        .json(&json!({
            "id": format!("00000000-0000-4000-8000-{id:012}"),
            "book_id": book,
            "memo": "Due back from Ada on Monday",
        }))
        .build()
        .expect("request");
    host.call(create_loan, request)
}

fn body(response: &Response) -> Value {
    response.json_as().expect("a JSON body")
}

#[test]
fn a_handler_writes_with_bound_parameters_and_reads_typed_rows() {
    let Some((db, host)) = loans() else {
        return;
    };
    let created = create(&host, 1, BOOK);
    assert_eq!(created.status(), 200);
    assert_eq!(
        body(&created),
        json!({"created": 1, "id": "00000000-0000-4000-8000-000000000001"})
    );
    let again = create(&host, 1, BOOK);
    assert_eq!(
        (again.status(), body(&again)),
        (409, json!({"error_message": "Loan already exists"}))
    );
    let anonymous = host.call(
        create_loan,
        host.request().json(&json!({})).build().expect("request"),
    );
    assert_eq!(anonymous.status(), 401);
    assert_eq!(
        db.query("SELECT memo, loan_kind FROM loan_service.loans")
            .expect("rows"),
        [json!({"memo": "Due back from Ada on Monday", "loan_kind": "Standard"})]
    );
    let read = host.run(|| {
        sql::query_as::<Loan>(
            "SELECT id::text, memo, loan_kind, created_by::text FROM loans WHERE book_id = $1",
            &[Param::Uuid(BOOK.into())],
        )
    });
    assert_eq!(
        read.expect("rows"),
        [Loan {
            id: "00000000-0000-4000-8000-000000000001".into(),
            memo: "Due back from Ada on Monday".into(),
            loan_kind: "Standard".into(),
            created_by: "22222222-2222-4222-8222-222222222222".into(),
        }]
    );
}

#[test]
fn a_transaction_commits_everything_or_nothing() {
    let Some((db, host)) = loans() else {
        return;
    };
    let other = "33333333-3333-4333-8333-333333333333";
    (1..=2).for_each(|id| assert_eq!(create(&host, id, BOOK).status(), 200));
    let moving = |from: &str, to: &str| {
        host.call(
            move_loans,
            host.request()
                .json(&json!({"from": from, "to": to}))
                .build()
                .expect("request"),
        )
    };
    let books = || {
        db.query("SELECT book_id, loan_kind FROM loan_service.loans ORDER BY id")
            .expect("rows")
    };
    let moved = moving(BOOK, other);
    assert_eq!(
        (moved.status(), body(&moved)),
        (200, json!({"moved": 2, "kept_kind": true}))
    );
    assert_eq!(
        books(),
        [
            json!({"book_id": other, "loan_kind": "Standard"}),
            json!({"book_id": other, "loan_kind": "Standard"}),
        ],
        "the savepoint's work was undone and the rest committed"
    );
    assert_eq!(create(&host, 3, other).status(), 200);
    let refused = moving(other, BOOK);
    assert_eq!(
        (refused.status(), body(&refused)),
        (422, json!({"error_message": "too many to move"}))
    );
    assert!(
        books().iter().all(|loan| loan["book_id"] == other),
        "nothing moved"
    );
    assert_eq!(host.db.as_ref().map(|db| db.open_transactions()), Some(0));
}

#[test]
fn a_conflict_between_transactions_is_retried() {
    let Some((db, host)) = loans() else {
        return;
    };
    assert_eq!(create(&host, 1, BOOK).status(), 200);
    let attempts = std::cell::Cell::new(0);
    let renamed = host.run(|| {
        sql::retry_transaction(
            3,
            TxOptions {
                isolation: Isolation::RepeatableRead,
                read_only: false,
            },
            |tx| {
                attempts.set(attempts.get() + 1);
                tx.query("SELECT memo FROM loans", &[])?;
                if attempts.get() == 1 {
                    db.execute("UPDATE loan_service.loans SET memo = 'theirs'")
                        .map_err(|e| DbError::new(DbErrorKind::Other, e.to_string()))?;
                }
                tx.execute("UPDATE loans SET memo = $1", &params!["ours"])
            },
        )
    });
    assert_eq!(renamed.ok(), Some(1));
    assert_eq!(attempts.get(), 2, "the first attempt could not serialise");
    assert_eq!(
        db.query("SELECT memo FROM loan_service.loans")
            .expect("rows"),
        [json!({"memo": "ours"})]
    );
}

#[test]
fn without_a_database_sql_is_unavailable() {
    let host = TestHost::builder().build().expect("host");
    let unavailable = host.run(|| sql::query("SELECT 1", &[]));
    assert_eq!(
        unavailable.err().map(|e| e.kind),
        Some(DbErrorKind::Unavailable)
    );
    assert_eq!(
        sql::execute("SELECT 1", &[]).err().map(|e| e.kind),
        Some(DbErrorKind::Unavailable),
        "no host at all"
    );
}
