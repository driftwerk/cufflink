use cufflink_testkit as common;

use common::{legacy, Deployment};
use cufflink_platform::policy::PolicyMode;
use cufflink_platform::schema_manager::executor::{
    prepare_schema_changes, Direction, Guard, SchemaRequest, Tables,
};
use cufflink_platform::tenant_sessions::ServiceSessions;
use cufflink_types::{SchemaOwnership, TableDefinition};
use reqwest::Method;
use serde_json::{json, Value};

fn items(search: Value) -> Value {
    json!({
        "name": "shop", "version": "0.1.0", "mode": "crud", "publishes": [],
        "tables": [{
            "name": "items",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": false},
                {"name": "body", "column_type": "text", "primary_key": false, "nullable": true},
                {"name": "sku", "column_type": "text", "primary_key": false, "nullable": false},
                {"name": "created_at", "column_type": "timestamp", "primary_key": false,
                 "nullable": false, "auto_generate": true},
            ],
            "search": search,
        }]
    })
}

async fn deploy(d: &Deployment, manifest: Value) -> (u16, Value) {
    d.deploy(json!({"manifest": manifest, "allow_destructive": true}))
        .await
}

async fn add(d: &Deployment, title: &str, body: &str) {
    let path = format!("/svc/{}/shop/items", d.tenant.slug);
    let (status, created) = d
        .send(
            Method::POST,
            &path,
            Some(json!({"title": title, "body": body, "sku": title.replace(' ', "-")})),
        )
        .await;
    assert!((200..300).contains(&status), "{created}");
    assert!(created.get("_search").is_none(), "{created}");
}

async fn titles(d: &Deployment, rest: &str) -> Vec<String> {
    let path = format!("/svc/{}/shop/items{rest}", d.tenant.slug);
    let (status, body) = d.send(Method::GET, &path, None).await;
    assert_eq!(status, 200, "{body}");
    body["results"]
        .as_array()
        .expect("results")
        .iter()
        .map(|row| {
            assert!(row.get("_search").is_none(), "{row}");
            row["title"].as_str().expect("title").to_string()
        })
        .collect()
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
async fn declared_search_uses_gin() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, body) = deploy(
        &d,
        items(json!({"columns": ["title", "body"], "config": "english"})),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["schema_changes"], 4, "{body}");
    add(&d, "Brass table lamp", "A lamp of brass for a table").await;
    add(&d, "Copper kettle", "Whistles when the water boils").await;
    add(
        &d,
        "Reading lamp",
        "Brass, with a green shade; lamps like it are rare",
    )
    .await;
    add(&d, "Garden table", "Teak").await;

    assert_eq!(
        titles(&d, "?search=brass%20lamp").await,
        ["Brass table lamp", "Reading lamp"],
        "the best match first"
    );
    assert_eq!(titles(&d, "?fts=kettle").await, ["Copper kettle"]);
    assert_eq!(
        titles(&d, "?search=lamp&sort=title").await,
        ["Brass table lamp", "Reading lamp"],
        "a sort of its own is kept"
    );
    assert_eq!(
        titles(&d, &format!("?search={}", urlencode("\"green shade\""))).await,
        ["Reading lamp"]
    );
    assert_eq!(titles(&d, "?search=lamp&filter_mode=v2").await.len(), 2);
    assert!(titles(&d, "?search=teak%20-garden").await.is_empty());
    assert_eq!(titles(&d, "").await.len(), 4);

    let (_, openapi) = d
        .platform
        .get_json(&format!("/svc/{}/shop/_meta/openapi.json", d.tenant.slug))
        .await
        .expect("openapi");
    assert!(
        !serde_json::to_string(&openapi)
            .expect("json")
            .contains("_search"),
        "the vector is the platform's, not the API's"
    );

    d.database
        .batch_execute("SET enable_seqscan = off")
        .await
        .expect("a planner that prefers the index over four rows");
    let explained: Value = d
        .database
        .query_one(
            "EXPLAIN (FORMAT JSON) SELECT id FROM shop.items \
             WHERE \"_search\" @@ websearch_to_tsquery('english', 'lamp')",
            &[],
        )
        .await
        .expect("explain")
        .get(0);
    let text = serde_json::to_string(&explained).expect("json");
    assert!(text.contains("items__search_gin"), "{text}");

    let (status, body) = deploy(
        &d,
        items(json!({"columns": ["title"], "config": "english"})),
    )
    .await;
    assert_eq!(
        status, 200,
        "the vector is kept again over the title alone: {body}"
    );
    assert_eq!(
        body["plan"]["schema_changes"].as_array().map(Vec::len),
        Some(3)
    );
    assert!(titles(&d, "?search=whistles").await.is_empty());
    assert_eq!(titles(&d, "?search=kettle").await, ["Copper kettle"]);
    assert_eq!(titles(&d, "?search=lamp").await.len(), 2);

    let (status, body) = deploy(&d, items(Value::Null)).await;
    assert_eq!(status, 200, "{body}");
    let columns: Vec<String> = d
        .database
        .query(
            "SELECT attname::text FROM pg_attribute WHERE attrelid = 'shop.items'::regclass \
             AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            &[],
        )
        .await
        .expect("columns")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(columns, ["id", "title", "body", "sku", "created_at"]);
    assert_eq!(
        titles(&d, "?search=whistles").await,
        ["Copper kettle"],
        "without a declaration the table is searched as it always was"
    );

    let (status, body) = deploy(
        &d,
        items(json!({"columns": ["title"], "config": "klingon"})),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("no text search configuration 'klingon'")),
        "{body}"
    );
}

const SEARCH_INDEX: &str = "SELECT indisvalid::text || ' ' || pg_get_indexdef(indexrelid) \
     FROM pg_index WHERE indexrelid = to_regclass('shop.items__search_gin')";

fn english() -> Value {
    json!({"columns": ["title", "body"], "config": "english"})
}

fn warnings(body: &Value) -> Vec<&str> {
    body["plan"]["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect()
}

#[tokio::test]
async fn a_search_added_to_a_table_with_rows_is_indexed_online() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let (status, body) = deploy(&d, items(Value::Null)).await;
    assert_eq!(status, 200, "{body}");
    add(&d, "Brass table lamp", "A lamp of brass for a table").await;
    add(&d, "Copper kettle", "Whistles when the water boils").await;
    legacy::record_ddl(&d.database).await;

    let rewrite = "SET SEARCH ON items: the search vector is a stored generated column, and \
                   adding it rewrites items while nothing else reads or writes it";
    let (status, plan) = d
        .deploy_as("?plan=true", json!({"manifest": items(english())}))
        .await;
    assert_eq!(status, 200, "{plan}");
    assert!(
        matches!(warnings(&plan)[..], [only] if only.starts_with(rewrite)),
        "{plan}"
    );
    assert_eq!(
        legacy::ddl_statements(&d.database).await,
        legacy::DdlStatements::NONE,
        "a plan runs nothing"
    );

    let (status, body) = deploy(&d, items(english())).await;
    assert_eq!(status, 200, "{body}");
    assert!(
        matches!(warnings(&body)[..], [only] if only.starts_with(rewrite)),
        "{body}"
    );
    let build = "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"items__search_gin\" ON \
                 \"shop\".\"items\" USING gin (\"_search\")";
    assert_eq!(
        plan["plan"]["schema_changes"],
        body["plan"]["schema_changes"]
    );
    assert_eq!(body["plan"]["schema_changes"][2], build, "{body}");
    let ran = legacy::ddl_statements(&d.database).await;
    assert_eq!(
        ran.committed.first().map(String::as_str),
        Some("CREATE SCHEMA: CREATE SCHEMA IF NOT EXISTS \"shop\""),
        "{ran:?}"
    );
    assert_eq!(ran.begun, 4, "{ran:?}");
    assert_eq!(ran.committed.len(), 4, "nothing was rolled back: {ran:?}");
    assert_eq!(
        ran.committed.last().map(String::as_str),
        Some(format!("CREATE INDEX: {build}").as_str()),
        "the statement is the transaction: {ran:?}"
    );
    assert_eq!(
        d.texts(SEARCH_INDEX).await,
        ["true CREATE INDEX items__search_gin ON shop.items USING gin (_search)"]
    );
    assert_eq!(titles(&d, "?search=kettle").await, ["Copper kettle"]);

    let (status, body) = deploy(&d, items(Value::Null)).await;
    assert_eq!(status, 200, "{body}");
    assert!(warnings(&body).is_empty(), "{body}");
    assert!(d.texts(SEARCH_INDEX).await.is_empty());
}

const ITEMS_MADE_BY_HAND: &str = "\
    CREATE SCHEMA shop; \
    CREATE TABLE shop.items ( \
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(), \
        title text NOT NULL, body text, sku text NOT NULL, \
        created_at timestamptz NOT NULL DEFAULT now()); \
    INSERT INTO shop.items (title, body, sku) \
        VALUES ('Copper kettle', 'Whistles when the water boils', 'kettle')";

#[tokio::test]
async fn a_table_that_is_there_already_is_given_its_search_vector() {
    let Some(d) = Deployment::under("schema_guard", json!({"mode": "enforce"})).await else {
        return;
    };
    d.database
        .batch_execute(ITEMS_MADE_BY_HAND)
        .await
        .expect("a table made before its first deploy");
    let (status, body) = deploy(&d, items(english())).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["satisfied"],
        json!(["CREATE TABLE items"]),
        "{body}"
    );
    assert_eq!(body["schema_changes"], 3, "{body}");
    assert_eq!(
        d.texts(SEARCH_INDEX).await,
        ["true CREATE INDEX items__search_gin ON shop.items USING gin (_search)"]
    );
    assert_eq!(titles(&d, "?search=whistles").await, ["Copper kettle"]);

    let mut again = items(english());
    again["version"] = json!("0.2.0");
    again["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({"name": "colour", "column_type": "text", "primary_key": false, "nullable": true}));
    let (status, body) = deploy(&d, again).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["schema_changes"], 1,
        "a vector that is kept is not remade: {body}"
    );
}

#[tokio::test]
async fn a_search_column_made_by_hand_is_never_dropped() {
    let Some(d) = Deployment::under("schema_guard", json!({"mode": "enforce"})).await else {
        return;
    };
    d.database
        .batch_execute(ITEMS_MADE_BY_HAND)
        .await
        .expect("a table made before its first deploy");
    d.database
        .batch_execute(
            "ALTER TABLE shop.items ADD COLUMN _search text; \
             UPDATE shop.items SET _search = 'kept by hand'",
        )
        .await
        .expect("a column of the vector's name");
    let (status, body) = deploy(&d, items(english())).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains(
            "column items._search (text) is not the generated tsvector the platform keeps the \
             search vector of items in"
        )),
        "{body}"
    );
    assert_eq!(
        d.texts("SELECT _search FROM shop.items").await,
        ["kept by hand"]
    );

    let mut declared = items(english());
    declared["tables"][0]["columns"]
        .as_array_mut()
        .expect("columns")
        .push(json!({"name": "_search", "column_type": "text", "primary_key": false, "nullable": true}));
    let (status, body) = deploy(&d, declared).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains(
            "tables.items.columns._search: '_search' is the column the platform keeps the search \
             vector of the table in"
        )),
        "{body}"
    );
    assert_eq!(
        d.texts("SELECT _search FROM shop.items").await,
        ["kept by hand"]
    );
}

#[tokio::test]
async fn an_adopted_table_is_searched_by_its_own_vector() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;
    client
        .batch_execute(
            "ALTER TABLE loan_service.loans ADD COLUMN document tsvector \
             GENERATED ALWAYS AS (to_tsvector('simple', memo)) STORED",
        )
        .await
        .expect("a vector of its own");
    let loans = |vector: &str| -> Vec<TableDefinition> {
        serde_json::from_value(json!([{
            "name": "loans",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false},
                {"name": "memo", "column_type": "text", "primary_key": false, "nullable": false},
            ],
            "search": {"vector_column": vector, "config": "simple"},
        }]))
        .expect("tables")
    };
    let sessions = ServiceSessions::over(db.pool(), "loan_service");
    let adoption = |declared: Vec<TableDefinition>| {
        let sessions = &sessions;
        async move {
            let tables = Tables::of(
                SchemaOwnership::Adopted,
                &declared,
                &[],
                &Default::default(),
            );
            let request = SchemaRequest {
                service: "loan-service",
                ownership: SchemaOwnership::Adopted,
                tables: &tables.managed,
                adopted: &tables.adopted,
                old_tables: &tables.left,
                leaving: 1,
                allow_destructive: false,
                direction: Direction::Forward,
                guard: Guard {
                    mode: PolicyMode::Off,
                    ..Guard::OFF
                },
            };
            prepare_schema_changes(sessions, &request).await
        }
    };
    let prepared = adoption(loans("document")).await.expect("adopted");
    assert!(prepared.adoption.expect("adoption").errors.is_empty());

    let error = adoption(loans("memo"))
        .await
        .expect_err("not a vector")
        .to_string();
    assert!(
        error.contains("loan_service.loans.memo is text; search.vector_column names a tsvector"),
        "{error}"
    );
}
