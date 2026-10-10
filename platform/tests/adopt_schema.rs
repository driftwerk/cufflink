use cufflink_testkit as common;

use common::{legacy, Deployment};
use cufflink_platform::policy::PolicyMode;
use cufflink_platform::repo::deployments::{self, ActivateVersion, Adoption};
use cufflink_platform::schema_manager::adopt::AdoptedSchemas;
use cufflink_platform::schema_manager::executor::{
    apply_schema, ensure_schema, prepare_schema_changes, Direction, Guard, SchemaRequest, Tables,
};
use cufflink_platform::schema_manager::introspect::TypeKind;
use cufflink_platform::tenant_sessions::ServiceSessions;
use cufflink_types::{SchemaOwnership, TableDefinition};
use serde_json::{json, Value};
use std::time::Duration;

fn column(name: &str, column_type: Value, nullable: bool) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": nullable})
}

fn key(name: &str, column_type: Value) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": true, "nullable": false})
}

fn loans() -> Value {
    json!({"name": "loans", "columns": [
        key("id", json!("uuid")),
        column("book_id", json!("uuid"), false),
        column("memo", json!("text"), false),
        column("loan_kind", json!({"varchar": {"length": 50}}), false),
        column("created_at", json!("timestamp_naive"), false),
    ]})
}

fn books() -> Value {
    json!([
        {"name": "book", "columns": [
            key("id", json!("uuid")),
            column("status", json!("text"), false),
            column("format", json!("text"), false),
            column("tags", json!("text_array"), false),
            column("replacement_cost", json!({"numeric": {"precision": 12, "scale": 2}}), true),
            column("created_at", json!("timestamp_naive"), false),
        ]},
        {"name": "settings", "columns": [
            key("id", json!("integer")),
            column("loan_days", json!("integer"), false),
        ]},
        {"name": "shelf_section", "columns": [
            key("key", json!({"varchar": {"length": 50}})),
            column("label", json!("text"), false),
        ]},
    ])
}

fn tables(json: Value) -> Vec<TableDefinition> {
    serde_json::from_value(json).expect("tables")
}

fn adopting<'a>(
    ownership: SchemaOwnership,
    tables: &'a Tables,
    mode: PolicyMode,
) -> SchemaRequest<'a> {
    SchemaRequest {
        service: "catalog-service",
        ownership,
        tables: &tables.managed,
        adopted: &tables.adopted,
        old_tables: &tables.left,
        leaving: 1,
        allow_destructive: true,
        direction: Direction::Forward,
        guard: Guard { mode, ..Guard::OFF },
    }
}

#[tokio::test]
async fn an_adopted_schema_is_read_and_never_changed() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;
    legacy::record_ddl(&client).await;
    let sessions = ServiceSessions::over(db.pool(), "catalog_service");

    for mode in PolicyMode::ALL {
        let declared = Tables::of(
            SchemaOwnership::Adopted,
            &tables(books()),
            &tables(json!([{"name": "gone", "columns": [key("id", json!("uuid"))]}])),
            &Default::default(),
        );
        let request = adopting(SchemaOwnership::Adopted, &declared, mode);
        ensure_schema(&sessions, request.ownership)
            .await
            .expect("nothing to ensure");
        let prepared = prepare_schema_changes(&sessions, &request)
            .await
            .expect("adopted");
        assert!(prepared.additive_sql.is_empty() && prepared.destructive_sql.is_empty());
        assert!(prepared.diff.is_empty());
        assert_eq!(prepared.guard, None);
        let executed = apply_schema(
            &sessions,
            &prepared,
            "catalog-service",
            Duration::from_secs(30),
        )
        .await
        .expect("applied");
        assert!(executed.is_empty());

        let report = prepared.adoption.expect("adoption report");
        assert_eq!(report.errors, Vec::<String>::new());
        assert_eq!(report.warnings, Vec::<String>::new());
        assert_eq!(
            serde_json::to_value(&report.undeclared_columns).expect("json"),
            json!({"book": 1, "settings": 1})
        );
        let format = report.adopted.column("book", "format").expect("format");
        assert_eq!(format.typtype, TypeKind::Enum);
        assert_eq!(format.bound("$2"), "$2::text::catalog_service.book_format");
        let id = report.adopted.column("settings", "id").expect("id");
        assert!(id.may_be_omitted(), "the singleton's key has a default");
        assert_eq!(
            report.adopted.tables().collect::<Vec<_>>(),
            ["book", "settings", "shelf_section"]
        );
    }
    assert_eq!(
        legacy::recorded_ddl(&client, "catalog_service").await,
        Vec::<String>::new()
    );
    client
        .batch_execute("CREATE TABLE catalog_service.probe (id int)")
        .await
        .expect("probe");
    assert_eq!(
        legacy::recorded_ddl(&client, "catalog_service").await,
        ["CREATE TABLE catalog_service.probe"]
    );
}

#[tokio::test]
async fn what_the_database_cannot_be_is_refused_before_anything_runs() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;
    legacy::record_ddl(&client).await;

    let mut declared = books();
    declared[0]["columns"][1] = column("status", json!("integer"), false);
    declared[0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(column("referrer", json!("text"), true));
    declared[1]["columns"][0] = key("loan_days", json!("integer"));
    declared
        .as_array_mut()
        .expect("tables")
        .push(json!({"name": "milestone", "columns": [key("id", json!("uuid"))]}));
    let declared = Tables::of(
        SchemaOwnership::Adopted,
        &tables(declared),
        &[],
        &Default::default(),
    );
    let error = prepare_schema_changes(
        &ServiceSessions::over(db.pool(), "catalog_service"),
        &adopting(SchemaOwnership::Adopted, &declared, PolicyMode::Off),
    )
    .await
    .expect_err("refused")
    .to_string();
    for finding in [
        "catalog_service.book.status is character varying(50), which a column declared INTEGER cannot be",
        "catalog_service.book.referrer is missing",
        "catalog_service.settings has the primary key 'id', not the declared 'loan_days'",
        "table catalog_service.milestone is missing",
    ] {
        assert!(error.contains(finding), "{error}");
    }

    let nowhere = prepare_schema_changes(
        &ServiceSessions::over(db.pool(), "receipt_service"),
        &adopting(
            SchemaOwnership::Adopted,
            &Tables::default(),
            PolicyMode::Off,
        ),
    )
    .await
    .expect_err("no schema")
    .to_string();
    assert!(
        nowhere.contains("schema receipt_service is missing"),
        "{nowhere}"
    );
    assert_eq!(
        legacy::recorded_ddl(&client, "receipt_service").await,
        Vec::<String>::new(),
        "an adopted schema is never created"
    );
}

#[tokio::test]
async fn an_unmanaged_table_is_left_alone_among_managed_ones() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;
    legacy::record_ddl(&client).await;
    let sessions = ServiceSessions::over(db.pool(), "loan_service");

    let mut unmanaged = loans();
    unmanaged["managed"] = json!(false);
    let tags = json!({"name": "loan_tags", "columns": [
        {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
        column("tag", json!("text"), false),
    ]});
    let was_managed = tables(json!([loans(), tags.clone()]));
    let declared = Tables::of(
        SchemaOwnership::Managed,
        &tables(json!([unmanaged, tags])),
        &was_managed[..1],
        &Default::default(),
    );
    let request = adopting(SchemaOwnership::Managed, &declared, PolicyMode::Enforce);
    ensure_schema(&sessions, request.ownership)
        .await
        .expect("schema");
    let prepared = prepare_schema_changes(&sessions, &request)
        .await
        .expect("prepared");
    let executed = apply_schema(
        &sessions,
        &prepared,
        "loan-service",
        Duration::from_secs(30),
    )
    .await
    .expect("applied");
    assert_eq!(
        executed,
        ["CREATE TABLE IF NOT EXISTS \"loan_service\".\"loan_tags\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"tag\" TEXT NOT NULL)"]
    );
    assert_eq!(
        legacy::recorded_ddl(&client, "loan_service").await,
        [
            "CREATE TABLE loan_service.loan_tags",
            "CREATE INDEX loan_service.loan_tags_pkey"
        ]
    );
    let adoption = prepared.adoption.expect("adoption");
    assert_eq!(adoption.adopted.tables().collect::<Vec<_>>(), ["loans"]);
    assert_eq!(
        serde_json::to_value(&adoption.undeclared_columns).expect("json"),
        json!({"loans": 2})
    );
    let guard = prepared.guard.expect("guard");
    assert!(guard.drift.is_empty() && guard.extra_columns.is_empty());
}

#[tokio::test]
async fn what_a_version_adopted_is_kept_with_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_migrated_database().await;
    let tenant = db.mint_tenant().await;
    let pool = db.pool();
    let service =
        cufflink_testkit::active_service(&pool, tenant.id, "catalog-service", "catalog_service")
            .await
            .expect("service");
    let declared = tables(books());
    let adopted = serde_json::from_value(json!({"book": {"format": {
        "type": "catalog_service.book_format", "typtype": "enum", "nullable": false, "has_default": true
    }}}))
    .expect("adopted");
    let activate = |version: i32, adoption: Adoption<'static>| {
        let (pool, declared) = (pool.clone(), declared.clone());
        async move {
            let snapshot = adoption
                .snapshot(&pool, "catalog-service", service.id, &declared)
                .await
                .expect("snapshot");
            deployments::activate_version(
                &pool,
                ActivateVersion {
                    service_id: service.id,
                    version,
                    manifest_hash: "m",
                    manifest: &Default::default(),
                    wasm_hash: None,
                    commit_hash: None,
                    schema_json: Some(&snapshot),
                    schema_hash: "s",
                    native: None,
                },
            )
            .await
            .expect("activated");
            snapshot
        }
    };
    let adopted: &'static _ = Box::leak(Box::new(adopted));
    let first = activate(2, Adoption::Read(adopted)).await;
    assert_eq!(first["adopted"]["book"]["format"]["typtype"], "enum");
    assert_eq!(first["tables"][0]["name"], "book");
    let kept = activate(3, Adoption::Kept).await;
    assert_eq!(kept, first);

    let cache = AdoptedSchemas::default();
    let read = cache
        .of(&pool, "catalog-service", service.id, 3)
        .await
        .expect("adopted");
    assert_eq!(&*read, adopted);
    pool.get()
        .await
        .expect("checkout")
        .execute("DELETE FROM schema_snapshots WHERE version = 3", &[])
        .await
        .expect("delete");
    let again = cache
        .of(&pool, "catalog-service", service.id, 3)
        .await
        .expect("cached");
    assert!(std::sync::Arc::ptr_eq(&read, &again));

    let plain = activate(4, Adoption::Nothing).await;
    assert_eq!(plain, serde_json::to_value(&declared).expect("tables"));
    assert!(cache
        .of(&pool, "catalog-service", service.id, 4)
        .await
        .expect("nothing adopted")
        .is_empty());
    assert!(
        deployments::snapshot(&pool, "catalog-service", service.id, 9)
            .await
            .expect("no snapshot")
            .tables
            .is_empty()
    );
}

#[tokio::test]
async fn adopted_mode_emits_no_ddl() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    legacy::migrated(&d.database).await;
    legacy::record_ddl(&d.database).await;

    let (status, body) = d
        .deploy(json!({"manifest": {
            "name": "loan-service", "version": "0.1.0", "mode": "crud",
            "schema": "adopted", "tables": [loans()], "publishes": []
        }}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["schema_changes"], 0, "{body}");
    assert_eq!(
        body["plan"]["adoption"],
        json!({"errors": [], "warnings": [], "undeclared_columns": {"loans": 2}}),
        "{body}"
    );
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE,
        "not a statement of DDL begins, whatever it names"
    );
    assert_eq!(
        legacy::recorded_ddl(&d.database, "loan_service").await,
        Vec::<String>::new()
    );
}

const WATCHED_BOOKS: &str = "\
    CREATE SCHEMA watch; \
    CREATE TABLE watch.books ( \
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(), \
        title text NOT NULL, \
        watchers uuid[] NOT NULL DEFAULT '{}'); \
    CREATE INDEX books_watchers_gin ON watch.books USING gin (watchers); \
    INSERT INTO watch.books (title, watchers) \
        SELECT 'book ' || n, ARRAY[gen_random_uuid(), gen_random_uuid()] \
        FROM generate_series(1, 100000) AS n; \
    ANALYZE watch.books";

#[tokio::test]
async fn a_uuid_array_of_an_adopted_table_is_searched_through_its_gin_index() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    d.database
        .batch_execute(WATCHED_BOOKS)
        .await
        .expect("a table made before the platform adopts it");
    let watcher = uuid::Uuid::new_v4();
    d.database
        .execute(
            "UPDATE watch.books SET watchers = watchers || $1::uuid \
             WHERE title IN ('book 7', 'book 300')",
            &[&watcher],
        )
        .await
        .expect("two books watched");

    let explained: Value = d
        .database
        .query_one(
            "EXPLAIN (FORMAT JSON) SELECT id FROM watch.books WHERE \"watchers\" @> $1",
            &[&vec![watcher]],
        )
        .await
        .expect("explain")
        .get(0);
    let plan = serde_json::to_string(&explained).expect("json");
    assert!(plan.contains("books_watchers_gin"), "{plan}");

    d.deployed(json!({"manifest": {
        "name": "watch", "version": "0.1.0", "mode": "crud", "publishes": [],
        "schema": "adopted",
        "tables": [{"name": "books", "columns": [
            key("id", json!("uuid")),
            column("title", json!("text"), false),
            column("watchers", json!("uuid_array"), false),
        ]}],
    }}))
    .await;
    let scans = || async {
        d.database
            .query_one(
                "SELECT idx_scan FROM pg_stat_user_indexes \
                 WHERE schemaname = 'watch' AND indexrelname = 'books_watchers_gin'",
                &[],
            )
            .await
            .expect("index statistics")
            .get::<_, i64>(0)
    };
    let before = scans().await;
    let filter = json!({"f": "watchers", "op": "contains", "v": [watcher]}).to_string();
    let query: String = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("filter_mode", "v2")
        .append_pair("filter", &filter)
        .append_pair("sort", "title")
        .finish();
    let (status, body) = d
        .send(
            reqwest::Method::GET,
            &format!("/svc/{}/watch/books?{query}", d.tenant.slug),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let titles: Vec<&str> = body["results"]
        .as_array()
        .expect("results")
        .iter()
        .filter_map(|row| row["title"].as_str())
        .collect();
    assert_eq!(titles, ["book 300", "book 7"], "{body}");
    let flushed = std::time::Instant::now() + Duration::from_secs(30);
    while scans().await == before {
        assert!(
            std::time::Instant::now() < flushed,
            "the list was not served by books_watchers_gin (a session reports its scans \
             within ten seconds of going idle)"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
