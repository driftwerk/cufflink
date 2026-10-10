use cufflink_testkit as common;

use common::Deployment;
use reqwest::Method;
use serde_json::{json, Value};

fn column(name: &str, column_type: Value, nullable: bool) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": nullable})
}

fn book(price: Value, indexes: Value, checks: Value) -> Value {
    json!({
        "name": "book-service", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{
            "name": "book",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                column("status", json!({"varchar": {"length": 50}}), false),
                column("price", price, true),
                column("seen_at", json!("timestamp_naive"), true),
                column("call_at", json!("time"), true),
                column("photo", json!("bytea"), true),
                column("services", json!("text_array"), true),
                column("watchers", json!("uuid_array"), true),
                column("scores", json!("integer_array"), true),
                column("totals", json!("biginteger_array"), true),
                column("events", json!("jsonb_array"), true),
                {"name": "created_at", "column_type": "timestamp", "primary_key": false,
                 "nullable": false, "auto_generate": true},
            ],
            "indexes": indexes,
            "checks": checks,
        }]
    })
}

fn numeric() -> Value {
    json!({"numeric": {"precision": 12, "scale": 2}})
}

fn indexes() -> Value {
    json!([
        {"name": "book_open_status", "columns": ["status", "price"], "unique": true,
         "predicate": "status <> 'Closed'"},
        {"name": "book_services", "columns": ["services"], "unique": false, "method": "gin"},
    ])
}

fn checks() -> Value {
    json!([{"name": "book_price_nonneg", "expr": "price IS NULL OR price >= 0"}])
}

async fn rows(d: &Deployment, method: Method, rest: &str, body: Option<Value>) -> (u16, Value) {
    let path = format!("/svc/{}/book-service/book{rest}", d.tenant.slug);
    d.send(method, &path, body).await
}

const INDEXES: &str =
    "SELECT indexdef FROM pg_indexes WHERE schemaname = 'book_service' ORDER BY indexname";
const CHECKS: &str = "SELECT conname || ' ' || pg_get_constraintdef(oid) FROM pg_constraint \
     WHERE conrelid = 'book_service.book'::regclass AND contype = 'c' ORDER BY conname";

#[tokio::test]
async fn a_table_is_made_with_the_types_indexes_and_checks_it_declares() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, body) = d
        .deploy(json!({"manifest": book(numeric(), indexes(), checks())}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["schema_changes"], 6, "{body}");
    assert_eq!(
        d.texts(
            "SELECT attname || ' ' || format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = 'book_service.book'::regclass AND attnum > 0 ORDER BY attnum"
        )
        .await,
        [
            "id uuid",
            "status character varying(50)",
            "price numeric(12,2)",
            "seen_at timestamp without time zone",
            "call_at time without time zone",
            "photo bytea",
            "services text[]",
            "watchers uuid[]",
            "scores integer[]",
            "totals bigint[]",
            "events jsonb[]",
            "created_at timestamp with time zone",
        ]
    );
    assert_eq!(
        d.texts(INDEXES).await,
        [
            "CREATE UNIQUE INDEX book_open_status ON book_service.book USING btree (status, price) \
             WHERE ((status)::text <> 'Closed'::text)",
            "CREATE UNIQUE INDEX book_pkey ON book_service.book USING btree (id)",
            "CREATE INDEX book_services ON book_service.book USING gin (services)",
        ]
    );
    assert_eq!(
        d.texts(CHECKS).await,
        ["book_price_nonneg CHECK (((price IS NULL) OR (price >= (0)::numeric)))"]
    );
    assert_eq!(
        d.texts("SELECT convalidated::text FROM pg_constraint WHERE conname = 'book_price_nonneg'")
            .await,
        ["true"]
    );
}

#[tokio::test]
async fn rows_are_written_and_read_as_the_types_of_their_columns() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, body) = d
        .deploy(json!({"manifest": book(numeric(), indexes(), checks())}))
        .await;
    assert_eq!(status, 200, "{body}");

    let watcher = "0f8fad5b-d9cb-469f-a165-70867728950e";
    let written = json!({
        "status": "New",
        "price": "1250000.50",
        "seen_at": "2026-09-28T10:30:00",
        "call_at": "16:45:00",
        "photo": "aGVsbG8=",
        "services": ["Fiction", "Local"],
        "watchers": [watcher],
        "scores": [1, 2, 3],
        "totals": [9007199254740993_i64],
        "events": [{"kind": "loan"}, [1, 2]],
    });
    let (status, created) = rows(&d, Method::POST, "", Some(written.clone())).await;
    assert!((200..300).contains(&status), "{created}");
    let id = created["id"].as_str().expect("id").to_string();
    let (status, read) = rows(&d, Method::GET, &format!("/{id}"), None).await;
    assert_eq!(status, 200, "{read}");
    for (name, value) in written.as_object().expect("object") {
        assert_eq!(&read[name], value, "{name}: {read}");
    }

    for (name, bad) in [
        ("price", json!("a lot")),
        ("status", json!("s".repeat(51))),
        ("seen_at", json!("2026-09-28T10:30:00+01:00")),
        ("call_at", json!("teatime")),
        ("photo", json!("not base64!")),
        ("services", json!("Fiction")),
        ("watchers", json!(["nobody"])),
        ("scores", json!([1.5])),
    ] {
        let mut body = json!({"status": "New"});
        body[name] = bad;
        let (status, refused) = rows(&d, Method::POST, "", Some(body)).await;
        assert_eq!(status, 400, "{name}: {refused}");
        assert!(
            refused["error"]
                .as_str()
                .is_some_and(|e| e.contains(&format!("column '{name}'"))),
            "{name}: {refused}"
        );
    }
    let (status, refused) = rows(
        &d,
        Method::POST,
        "",
        Some(json!({"status": "Bid", "price": "-1"})),
    )
    .await;
    assert!(
        (400..500).contains(&status),
        "the CHECK refuses the row: {status} {refused}"
    );

    let (status, updated) = rows(
        &d,
        Method::PUT,
        &format!("/{id}"),
        Some(json!({"price": 99.5, "services": [], "photo": null})),
    )
    .await;
    assert_eq!(status, 200, "{updated}");
    assert_eq!(updated["price"], "99.50", "{updated}");
    assert_eq!(updated["services"], json!([]), "{updated}");
    assert_eq!(updated["photo"], Value::Null, "{updated}");

    let (status, listed) = rows(&d, Method::GET, "?status=New&price=99.5", None).await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["results"].as_array().map(Vec::len), Some(1));
    let filter = r#"[{"n":"price","f":">=","v":"99.50"},{"n":"status","f":"IN","v":"New, Bid"}]"#;
    let (status, listed) = rows(
        &d,
        Method::GET,
        &format!("?filter={}", urlencode(filter)),
        None,
    )
    .await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["results"][0]["id"], json!(id), "{listed}");
    let (status, refused) = rows(&d, Method::GET, "?price=cheap", None).await;
    assert_eq!(status, 400, "{refused}");
    assert!(
        refused["error"]
            .as_str()
            .is_some_and(|e| e.contains("column 'price'")),
        "{refused}"
    );
}

fn urlencode(text: &str) -> String {
    text.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                char::from(b).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

#[tokio::test]
async fn an_index_a_check_and_a_type_follow_the_manifest() {
    for mode in ["off", "warn", "enforce"] {
        let Some(d) = Deployment::under("schema_guard", json!({"mode": mode})).await else {
            return;
        };
        let enforced = mode == "enforce";
        let (status, body) = d
            .deploy(json!({"manifest": book(json!("integer"), indexes(), checks())}))
            .await;
        assert_eq!(status, 200, "{mode}: {body}");
        let (status, created) = rows(
            &d,
            Method::POST,
            "",
            Some(json!({"status": "New", "price": 1250})),
        )
        .await;
        assert!((200..300).contains(&status), "{mode}: {created}");

        let reshaped = json!([
            {"name": "book_open_status", "columns": ["status"], "unique": false,
             "predicate": "status <> 'Closed'"},
            {"name": "book_watchers", "columns": ["watchers"], "unique": false, "method": "gin"},
        ]);
        let rechecked = json!([
            {"name": "book_price_nonneg", "expr": "price IS NULL OR price > 0"},
            {"name": "book_status_known", "expr": "status <> ''"},
        ]);
        let changed = book(numeric(), reshaped, rechecked);
        let (status, refused) = d.deploy(json!({"manifest": changed.clone()})).await;
        assert_eq!(status, 422, "{mode}: {refused}");
        let error = refused["error"].as_str().expect("error");
        for destructive in [
            "ALTER COLUMN book.price from INTEGER to NUMERIC(12,2)",
            "DROP INDEX book_services",
        ] {
            assert!(error.contains(destructive), "{mode}: {error}");
        }
        assert_eq!(
            error.contains("REPLACE INDEX book_open_status ON book"),
            enforced,
            "{mode}: {error}"
        );

        let (status, body) = d
            .deploy(json!({"manifest": changed, "allow_destructive": true}))
            .await;
        assert_eq!(status, 200, "{mode}: {body}");
        let warnings: Vec<&str> = body["plan"]["warnings"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let (status_index, warning) = if enforced {
            (
                "CREATE INDEX book_open_status ON book_service.book USING btree (status) \
                 WHERE ((status)::text <> 'Closed'::text)",
                "REPLACE INDEX book_open_status ON book: the index is dropped and built again",
            )
        } else {
            (
                "CREATE UNIQUE INDEX book_open_status ON book_service.book USING btree \
                 (status, price) WHERE ((status)::text <> 'Closed'::text)",
                "REPLACE INDEX book_open_status ON book is not run",
            )
        };
        assert!(
            matches!(warnings[..], [only] if only.starts_with(warning)),
            "{mode}: {warnings:?}"
        );
        assert_eq!(
            d.texts(INDEXES).await,
            [
                status_index,
                "CREATE UNIQUE INDEX book_pkey ON book_service.book USING btree (id)",
                "CREATE INDEX book_watchers ON book_service.book USING gin (watchers)",
            ],
            "{mode}"
        );
        assert_eq!(
            d.texts(CHECKS).await,
            [
                "book_price_nonneg CHECK (((price IS NULL) OR (price > (0)::numeric)))",
                "book_status_known CHECK (((status)::text <> ''::text))",
            ],
            "{mode}"
        );
        let id = created["id"].as_str().expect("id");
        let (status, read) = rows(&d, Method::GET, &format!("/{id}"), None).await;
        assert_eq!(status, 200, "{mode}: {read}");
        assert_eq!(read["price"], "1250.00", "{mode}: {read}");

        let (status, body) = d
            .deploy(
                json!({"manifest": book(numeric(), json!([]), json!([])), "allow_destructive": true}),
            )
            .await;
        assert_eq!(status, 200, "{mode}: {body}");
        assert_eq!(d.texts(CHECKS).await, Vec::<String>::new(), "{mode}");
        assert_eq!(d.texts(INDEXES).await.len(), 1, "{mode}");
    }
}

#[tokio::test]
async fn a_check_the_rows_break_fails_the_deploy_and_is_added_once_they_do_not() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, body) = d
        .deploy(json!({"manifest": book(numeric(), json!([]), json!([]))}))
        .await;
    assert_eq!(status, 200, "{body}");
    d.database
        .batch_execute(
            "INSERT INTO book_service.book (id, status, price) \
                 VALUES (gen_random_uuid(), 'New', -5); \
             ALTER TABLE book_service.book \
                 ADD CONSTRAINT book_price_nonneg CHECK (price > -100) NOT VALID",
        )
        .await
        .expect("a row the check refuses, and a check of its name a failed deploy left");
    let checked = book(numeric(), json!([]), checks());
    let (status, failed) = d.deploy(json!({"manifest": checked.clone()})).await;
    assert_eq!(status, 500, "{failed}");
    assert!(
        failed["error"]
            .as_str()
            .is_some_and(|e| e.contains("book_price_nonneg")),
        "{failed}"
    );
    d.database
        .batch_execute("UPDATE book_service.book SET price = 5")
        .await
        .expect("repair");
    let (status, body) = d.deploy(json!({"manifest": checked})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        d.texts(CHECKS).await,
        ["book_price_nonneg CHECK (((price IS NULL) OR (price >= (0)::numeric)))"]
    );
}

#[tokio::test]
async fn a_check_that_would_close_its_own_parenthesis_is_refused() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    d.deployed(json!({"manifest": book(numeric(), indexes(), checks())}))
        .await;
    for expr in [
        "'(' = '(' ) , DROP COLUMN photo CASCADE , ADD CONSTRAINT z CHECK ( ')' = ')'",
        "E'\\' ) , DROP COLUMN photo CASCADE , ADD CHECK ( '' = ''",
        "$$ ) , DROP COLUMN photo CASCADE , ADD CHECK ( $$ = ''",
    ] {
        let exploit = json!([{"name": "book_status_known", "expr": expr}]);
        let (status, body) = d
            .deploy(json!({
                "manifest": book(numeric(), indexes(), exploit),
                "allow_destructive": true,
            }))
            .await;
        assert_eq!(status, 422, "{expr}: {body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.contains("checks[0].expr")),
            "{expr}: {body}"
        );
    }
    assert_eq!(
        d.texts(
            "SELECT column_name::text FROM information_schema.columns \
             WHERE table_schema = 'book_service' AND table_name = 'book' AND column_name = 'photo'"
        )
        .await,
        ["photo"]
    );
}
