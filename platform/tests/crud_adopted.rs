use cufflink_testkit as common;

use common::{legacy, PlatformProcess, Tenant, TestDb};
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

const DESK: &str = "
    CREATE SCHEMA desk;
    CREATE TYPE desk.book_format AS ENUM ('hardback', 'paperback');
    CREATE DOMAIN desk.status AS TEXT CHECK (VALUE IN ('new', 'lent', 'lost'));
    CREATE TABLE desk.book (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        format desk.book_format NOT NULL,
        status desk.status NOT NULL DEFAULT 'new',
        title TEXT,
        rank INTEGER NOT NULL DEFAULT 3,
        position POINT NOT NULL DEFAULT '(1,2)',
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE TABLE desk.settings (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        loan_days INTEGER NOT NULL DEFAULT 7
    );
    CREATE TABLE desk.shelf_section (key VARCHAR(50) PRIMARY KEY, label TEXT NOT NULL);
    CREATE DOMAIN desk.pos AS INTEGER CHECK (VALUE > 0);
    CREATE DOMAIN desk.ref AS UUID;
    CREATE DOMAIN desk.moment AS TIMESTAMPTZ;
    CREATE DOMAIN desk.meta AS JSONB;
    CREATE TABLE desk.ticket (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        rank desk.pos NOT NULL,
        book_ref desk.ref,
        due desk.moment,
        meta desk.meta NOT NULL DEFAULT '{}'
    );
";

fn column(name: &str, column_type: &str, nullable: bool) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false,
           "nullable": nullable, "auto_generate": false})
}

fn key(name: &str, column_type: &str, auto_generate: bool) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": true,
           "nullable": false, "auto_generate": auto_generate})
}

fn desk() -> Value {
    json!({
        "name": "desk",
        "version": "0.1.0",
        "mode": "crud",
        "schema": "adopted",
        "tables": [
            {"name": "book", "columns": [
                key("id", "uuid", true),
                column("format", "text", false),
                column("status", "text", false),
                column("title", "text", true),
                column("rank", "integer", false),
                {"name": "created_at", "column_type": "timestamp", "primary_key": false,
                 "nullable": false, "auto_generate": true},
            ]},
            {"name": "settings", "columns": [
                key("id", "integer", false),
                column("loan_days", "integer", false),
            ]},
            {"name": "shelf_section", "columns": [
                key("key", "text", false),
                column("label", "text", false),
            ]},
            {"name": "ticket", "columns": [
                key("id", "uuid", true),
                column("rank", "integer", false),
                column("book_ref", "uuid", true),
                column("due", "timestamp", true),
                column("meta", "jsonb", false),
            ]},
        ],
        "publishes": [],
    })
}

struct Desk {
    platform: PlatformProcess,
    tenant: Tenant,
    heard: async_nats::Subscriber,
    audited: async_nats::Subscriber,
    _db: TestDb,
}

impl Desk {
    async fn start() -> Option<Self> {
        Self::start_with(&[], desk()).await
    }

    async fn start_with(extra: &[(&str, &str)], manifest: Value) -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, Some(nats_url), extra).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let database = legacy::tenant_database(&db, &tenant).await;
        database.batch_execute(DESK).await.expect("the schema");
        legacy::record_ddl(&database).await;
        let nats = async_nats::connect(nats_url).await.expect("nats");
        let heard = nats
            .subscribe(format!("dw.{}.desk.>", tenant.slug))
            .await
            .expect("subscribe");
        let audited = nats
            .subscribe(format!("dw.{}.logs", tenant.slug))
            .await
            .expect("subscribe");
        let (status, deployed) = platform.deploy(&tenant, &manifest).await;
        assert_eq!(status, 200, "{deployed}");
        assert_eq!(
            legacy::recorded_ddl(&database, "desk").await,
            Vec::<String>::new()
        );
        Some(Self {
            platform,
            tenant,
            heard,
            audited,
            _db: db,
        })
    }

    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> (u16, Value) {
        self.platform
            .send(
                method,
                &format!("/svc/{}/desk{path}", self.tenant.slug),
                &format!("ApiKey {}", self.tenant.api_key),
                body.as_ref(),
            )
            .await
    }

    async fn ok(&self, method: Method, path: &str, body: Option<Value>) -> Value {
        let (status, answer) = self.call(method.clone(), path, body).await;
        assert_eq!(status, 200, "{method} {path}: {answer}");
        answer
    }

    async fn event(&mut self) -> (String, Value) {
        let message = tokio::time::timeout(Duration::from_secs(10), self.heard.next())
            .await
            .expect("an event within 10 s")
            .expect("a message");
        let event: Value = serde_json::from_slice(&message.payload).expect("json");
        (
            message
                .subject
                .rsplit('.')
                .next()
                .unwrap_or_default()
                .to_string(),
            event["data"].clone(),
        )
    }
}

impl Desk {
    async fn audit(&mut self) -> (String, String, String) {
        let message = tokio::time::timeout(Duration::from_secs(10), self.audited.next())
            .await
            .expect("an audit record within 10 s")
            .expect("a message");
        let record: Value = serde_json::from_slice(&message.payload).expect("json");
        let text = |key: &str| record[key].as_str().unwrap_or_default().to_string();
        (text("action"), text("table"), text("record_id"))
    }
}

fn without_times(mut row: Value) -> Value {
    if let Some(fields) = row.as_object_mut() {
        fields.remove("created_at");
    }
    row
}

#[tokio::test]
async fn rows_of_an_adopted_table_are_written_and_read_as_the_database_holds_them() {
    let Some(mut desk) = Desk::start().await else {
        return;
    };
    let created = desk
        .ok(Method::POST, "/book", Some(json!({"format": "hardback"})))
        .await;
    let id = created["id"].as_str().expect("the database gave a key");
    let book = |status: &str, title: Value| json!({"id": id, "format": "hardback", "status": status, "title": title, "rank": 3});
    assert_eq!(without_times(created.clone()), book("new", Value::Null));
    assert!(created["created_at"].is_string(), "{created}");
    assert_eq!(desk.event().await, ("created".to_string(), created.clone()));

    let path = format!("/book/{id}");
    assert_eq!(desk.ok(Method::GET, &path, None).await, created);
    let lent = desk
        .ok(
            Method::PUT,
            &path,
            Some(json!({"status": "lent", "title": "first"})),
        )
        .await;
    assert_eq!(without_times(lent.clone()), book("lent", json!("first")));
    assert_eq!(desk.event().await, ("updated".to_string(), lent.clone()));

    for (body, what) in [
        (json!({"format": "vinyl"}), "a value the enum has not"),
        (
            json!({"format": "hardback", "status": "open"}),
            "a value the domain refuses",
        ),
        (
            json!({"title": "no format"}),
            "no value where the database has no default",
        ),
    ] {
        let (status, refused) = desk.call(Method::POST, "/book", Some(body)).await;
        assert!((400..500).contains(&status), "{what}: {status} {refused}");
    }

    let listed = desk.ok(Method::GET, "/book", None).await;
    assert_eq!(listed["results"], json!([lent]), "{listed}");

    let batch = desk
        .ok(
            Method::POST,
            "/book/batch",
            Some(json!({"operations": [
                {"action": "create", "data": {"format": "paperback", "rank": 1}},
                {"action": "update", "id": id, "data": {"status": "lost"}},
                {"action": "delete", "id": id},
            ]})),
        )
        .await;
    assert_eq!(
        batch["summary"],
        json!({"created": 1, "updated": 1, "deleted": 1, "total": 3})
    );
    let second = batch["results"][0].clone();
    assert_eq!(second["format"], "paperback");
    assert_eq!(second["status"], "new");
    let lost = book("lost", json!("first"));
    assert_eq!(without_times(batch["results"][1].clone()), lost);
    let mut events = Vec::new();
    for _ in 0..3 {
        let (action, data) = desk.event().await;
        events.push((action, without_times(data)));
    }
    assert_eq!(
        events,
        [
            ("created".to_string(), without_times(second.clone())),
            ("updated".to_string(), lost.clone()),
            ("deleted".to_string(), lost),
        ]
    );

    let second_path = format!("/book/{}", second["id"].as_str().expect("an id"));
    assert_eq!(
        desk.ok(Method::DELETE, &second_path, None).await,
        json!({"deleted": true})
    );
    assert_eq!(desk.event().await, ("deleted".to_string(), second));
    let (status, gone) = desk.call(Method::GET, &second_path, None).await;
    assert_eq!(status, 404, "{gone}");
}

#[tokio::test]
async fn a_row_is_addressed_by_the_key_its_table_has() {
    let Some(mut desk) = Desk::start().await else {
        return;
    };
    assert_eq!(
        desk.ok(Method::POST, "/settings", Some(json!({}))).await,
        json!({"id": 1, "loan_days": 7}),
        "the database gives the key and the days"
    );
    let (status, second) = desk.call(Method::POST, "/settings", Some(json!({}))).await;
    assert_eq!(status, 409, "there is one row of settings: {second}");
    assert_eq!(
        desk.ok(Method::PUT, "/settings/1", Some(json!({"loan_days": 14})))
            .await,
        json!({"id": 1, "loan_days": 14})
    );
    assert_eq!(
        desk.ok(Method::GET, "/settings/1", None).await,
        json!({"id": 1, "loan_days": 14})
    );
    let (status, refused) = desk.call(Method::GET, "/settings/one", None).await;
    assert_eq!(
        (status, &refused["error"]),
        (400, &json!("Invalid integer key"))
    );
    let (status, missing) = desk.call(Method::GET, "/settings/2", None).await;
    assert_eq!(status, 404, "{missing}");

    let reference = json!({"key": "reference", "label": "Reference"});
    assert_eq!(
        desk.ok(Method::POST, "/shelf_section", Some(reference.clone()))
            .await,
        reference
    );
    let (status, refused) = desk
        .call(
            Method::POST,
            "/shelf_section",
            Some(json!({"label": "No key"})),
        )
        .await;
    assert_eq!(status, 422, "a key the database cannot give: {refused}");
    assert_eq!(
        desk.ok(Method::GET, "/shelf_section/reference", None).await,
        reference
    );
    assert_eq!(
        desk.ok(
            Method::PUT,
            "/shelf_section/reference",
            Some(json!({"label": "Reference works"}))
        )
        .await,
        json!({"key": "reference", "label": "Reference works"})
    );
    assert_eq!(
        desk.ok(Method::DELETE, "/shelf_section/reference", None)
            .await,
        json!({"deleted": true})
    );
    let (status, gone) = desk
        .call(Method::GET, "/shelf_section/reference", None)
        .await;
    assert_eq!(status, 404, "{gone}");

    let mut audited = Vec::new();
    for _ in 0..5 {
        audited.push(desk.audit().await);
    }
    let record = |action: &str, table: &str, key: &str| {
        (action.to_string(), table.to_string(), key.to_string())
    };
    assert_eq!(
        audited,
        [
            record("create", "settings", "1"),
            record("update", "settings", "1"),
            record("create", "shelf_section", "reference"),
            record("update", "shelf_section", "reference"),
            record("delete", "shelf_section", "reference"),
        ],
        "a row is recorded under the value of its key"
    );

    let (_, version) = desk.platform.api_get(&desk.tenant, "/api/version").await;
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("schema_adopted"))),
        "{version}"
    );
}

#[tokio::test]
async fn columns_over_domains_take_what_their_base_types_take() {
    let Some(mut desk) = Desk::start().await else {
        return;
    };
    let book_ref = uuid::Uuid::new_v4().to_string();
    let ticket = json!({
        "rank": 2,
        "book_ref": book_ref,
        "due": "2026-10-01T09:00:00Z",
        "meta": {"lines": [1, "x"], "gift": true},
    });
    let created = desk.ok(Method::POST, "/ticket", Some(ticket.clone())).await;
    let id = created["id"].as_str().expect("the database gave a key");
    let stored = |rank: i64, meta: Value| json!({"id": id, "rank": rank, "book_ref": book_ref, "due": "2026-10-01T09:00:00+00:00", "meta": meta});
    assert_eq!(created, stored(2, ticket["meta"].clone()));
    assert_eq!(desk.event().await, ("created".to_string(), created.clone()));

    let path = format!("/ticket/{id}");
    assert_eq!(desk.ok(Method::GET, &path, None).await, created);
    let listed = desk.ok(Method::GET, "/ticket?sort=rank&rank=2", None).await;
    assert_eq!(listed["results"], json!([created]), "{listed}");
    let listed = desk
        .ok(
            Method::GET,
            &format!("/ticket?sort=rank&book_ref={book_ref}"),
            None,
        )
        .await;
    assert_eq!(listed["results"], json!([created]), "{listed}");

    let updated = desk
        .ok(
            Method::PUT,
            &path,
            Some(json!({"rank": 3, "meta": {"gift": false}})),
        )
        .await;
    assert_eq!(updated, stored(3, json!({"gift": false})));
    assert_eq!(desk.event().await, ("updated".to_string(), updated.clone()));
    let (status, refused) = desk
        .call(Method::PUT, &path, Some(json!({"rank": 0})))
        .await;
    assert_eq!(status, 422, "the domain refuses a rank of 0: {refused}");

    assert_eq!(
        desk.ok(Method::DELETE, &path, None).await,
        json!({"deleted": true})
    );
    assert_eq!(desk.event().await, ("deleted".to_string(), updated));
    let (status, gone) = desk.call(Method::GET, &path, None).await;
    assert_eq!(status, 404, "{gone}");
}

#[tokio::test]
async fn a_value_is_bound_as_the_base_of_its_domain() {
    use cufflink_platform::router::bind::AsBase;
    use tokio_postgres::types::ToSql;

    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    client
        .batch_execute(
            "CREATE DOMAIN tags AS TEXT[] CHECK (array_length(VALUE, 1) > 0);
             CREATE DOMAIN pos AS INTEGER CHECK (VALUE > 0);
             CREATE DOMAIN ref AS UUID;
             CREATE DOMAIN moment AS TIMESTAMPTZ;
             CREATE DOMAIN meta AS JSONB;
             CREATE TABLE t (rank pos, book_ref ref, due moment, meta meta, tags tags)",
        )
        .await
        .expect("the schema");
    let (book_ref, due, meta) = (
        uuid::Uuid::new_v4(),
        chrono::DateTime::parse_from_rfc3339("2026-10-01T09:00:00Z")
            .expect("a time")
            .with_timezone(&chrono::Utc),
        json!({"gift": true}),
    );
    let tags = vec![Some("a".to_string()), None, Some("c".to_string())];
    let as_base = |value: Box<dyn ToSql + Sync + Send>| AsBase(value);
    let insert = "INSERT INTO t VALUES ($1, $2, $3, $4, $5)";
    let refused = client
        .execute(insert, &[&2_i32, &book_ref, &due, &meta, &tags])
        .await
        .expect_err("a value bound as itself is not a value of the domain");
    assert!(
        refused
            .to_string()
            .contains("error serializing parameter 0"),
        "{refused}"
    );
    client
        .execute(
            insert,
            &[
                &as_base(Box::new(2_i32)),
                &as_base(Box::new(book_ref)),
                &as_base(Box::new(due)),
                &as_base(Box::new(meta.clone())),
                &as_base(Box::new(tags.clone())),
            ],
        )
        .await
        .expect("bound as the base of each domain");
    let row = client
        .query_one(
            "SELECT rank::integer, book_ref::uuid, due::timestamptz, meta::jsonb, tags::text[] FROM t",
            &[],
        )
        .await
        .expect("the row");
    assert_eq!(row.get::<_, i32>(0), 2);
    assert_eq!(row.get::<_, uuid::Uuid>(1), book_ref);
    assert_eq!(row.get::<_, chrono::DateTime<chrono::Utc>>(2), due);
    assert_eq!(row.get::<_, Value>(3), meta);
    assert_eq!(row.get::<_, Vec<Option<String>>>(4), tags);
    let mismatched = client
        .execute(
            insert,
            &[
                &as_base(Box::new("two".to_string())),
                &as_base(Box::new(book_ref)),
                &as_base(Box::new(due)),
                &as_base(Box::new(meta)),
                &as_base(Box::new(tags)),
            ],
        )
        .await
        .expect_err("a text is no integer, base or not");
    assert!(
        mismatched
            .to_string()
            .contains("error serializing parameter 0"),
        "{mismatched}"
    );
}

#[tokio::test]
async fn the_row_an_update_changed_is_recorded_as_the_row_is_read() {
    let mut manifest = desk();
    manifest["tables"][0]["sensitive"] = json!(true);
    let Some(desk) = Desk::start_with(&[("CUFFLINK_AUDIT_DURABLE_ALL", "true")], manifest).await
    else {
        return;
    };
    let nats = async_nats::connect(
        common::infra()
            .await
            .and_then(|infra| infra.nats_url())
            .expect("nats"),
    )
    .await
    .expect("nats");
    let mut kept = nats
        .subscribe(format!("cufflink.audit.{}.desk", desk.tenant.slug))
        .await
        .expect("subscribe");
    let created = desk
        .ok(
            Method::POST,
            "/book",
            Some(json!({"format": "hardback", "title": "a"})),
        )
        .await;
    let id = created["id"].as_str().expect("id");
    let updated = desk
        .ok(
            Method::PUT,
            &format!("/book/{id}"),
            Some(json!({"status": "lent"})),
        )
        .await;
    let record = loop {
        let message = tokio::time::timeout(Duration::from_secs(10), kept.next())
            .await
            .expect("a durable record within 10 s")
            .expect("a message");
        let record: Value = serde_json::from_slice(&message.payload).expect("json");
        if record["action"] == "update" {
            break record;
        }
    };
    assert_eq!(record["data"], updated, "{record}");
    assert_eq!(
        record["previous"], created,
        "the declared columns alone, as a read answers them: {record}"
    );
    assert!(record["previous"].get("position").is_none(), "{record}");
}

#[tokio::test]
async fn a_query_reads_an_adopted_table_as_its_declared_columns() {
    let mut manifest = desk();
    manifest["graphql"] = json!(true);
    let Some(desk) = Desk::start_with(&[], manifest).await else {
        return;
    };
    let created = desk
        .ok(
            Method::POST,
            "/book",
            Some(json!({"format": "paperback", "title": "q"})),
        )
        .await;
    for (query, expected) in [
        (
            "{ book { id format status } }",
            json!([{"id": created["id"], "format": "paperback", "status": "new"}]),
        ),
        ("{ book }", json!([without_times(created.clone())])),
    ] {
        let queried = desk
            .ok(Method::POST, "/graphql", Some(json!({ "query": query })))
            .await;
        let rows: Vec<Value> = queried["data"]["book"]
            .as_array()
            .unwrap_or_else(|| panic!("{queried}"))
            .iter()
            .cloned()
            .map(without_times)
            .collect();
        assert_eq!(json!(rows), expected, "{query}");
    }
}
