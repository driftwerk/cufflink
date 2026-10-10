#![cfg(feature = "test-host")]

mod support;

use cufflink_fn::host::db::DbHost;
use cufflink_fn::testing::{MigrationHistory, MigrationLock, TestDb, TestHost};
use cufflink_fn::{db, Request, Response};
use cufflink_types::db::{DbRequest, DbResponse, Isolation, Param};
use serde_json::{json, Value};
use std::future::Future;
use support::{database, loan_migrations};

const BOOK: &str = "11111111-1111-4111-8111-111111111111";
const AUTHOR: &str = "22222222-2222-4222-8222-222222222222";

async fn run_with_loans<F, Fut>(test: F)
where
    F: FnOnce(TestDb, TestHost) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(db) = database() else {
        return;
    };
    db.migrate(
        "loan_service",
        &loan_migrations(),
        &MigrationHistory {
            table: "schema_history".into(),
            lock: MigrationLock::LegacyTableHash,
            ..MigrationHistory::default()
        },
    )
    .expect("migrations");
    let host = TestHost::builder()
        .tenant("harbor")
        .service("loan-service")
        .db(&db)
        .build()
        .expect("host");
    test(db, host).await;
}

fn create_loan(request: Request) -> Response {
    let id = request.path_param("id").unwrap_or_default().to_string();
    let memo = request.body()["memo"].as_str().unwrap_or_default();
    db::execute(&format!(
        "INSERT INTO loans (id, book_id, memo, created_by, created_at, updated_at) \
         VALUES ('{id}', '{BOOK}', '{memo}', '{AUTHOR}', now(), now())"
    ));
    Response::json(&json!({
        "loans": db::query(&format!("SELECT memo FROM loans WHERE id = '{id}'")),
    }))
}

fn begin(host: &impl DbHost) -> u32 {
    match host.call(&DbRequest::Begin {
        isolation: Isolation::ReadCommitted,
        read_only: false,
    }) {
        DbResponse::Begun(tx) => tx,
        other => panic!("{other:?}"),
    }
}

async fn the_host_serves_a_handler(db: TestDb, host: TestHost) {
    let request = host
        .request()
        .handler("create_loan")
        .path("/loans/{id}")
        .path_param("id", "00000000-0000-4000-8000-000000000001")
        .json(&json!({"memo": "Due back on Friday"}))
        .build()
        .expect("request");
    tokio::task::yield_now().await;
    let response = host.call(create_loan, request);
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json_as::<Value>().expect("json"),
        json!({"loans": [{"memo": "Due back on Friday"}]})
    );

    let handle = host.db.clone().expect("a database");
    host.run(|| {
        let tx = begin(&handle);
        assert_eq!(
            handle.call(&DbRequest::Execute {
                sql: "DELETE FROM loans".into(),
                params: Vec::<Param>::new(),
                tx: Some(tx),
            }),
            DbResponse::Affected(1)
        );
    });
    assert_eq!(handle.open_transactions(), 0);
    tokio::task::yield_now().await;
    assert_eq!(
        db.query("SELECT count(*) AS loans FROM loan_service.loans")
            .expect("count"),
        [json!({"loans": 1})],
        "what the handler left open was rolled back"
    );

    drop((handle, host));
    let name = db.name().to_string();
    let other = database();
    drop(db);
    if let Some(other) = other {
        assert_eq!(
            other
                .query(&format!(
                    "SELECT count(*) AS there FROM pg_database WHERE datname = '{name}'"
                ))
                .expect("pg_database"),
            [json!({"there": 0})],
            "the database was dropped inside the runtime"
        );
    }
}

#[tokio::test]
async fn a_host_works_inside_a_current_thread_runtime() {
    run_with_loans(the_host_serves_a_handler).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_host_works_inside_a_multi_thread_runtime() {
    run_with_loans(the_host_serves_a_handler).await;
}
