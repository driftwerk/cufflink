use cufflink_testkit as common;

use common::{legacy, Deployment};
use cufflink_platform::schema_manager::introspect::{
    self, DependentKind, DropTarget, RelationKind, TypeKind,
};
use cufflink_platform::tenant_sessions::ServiceSessions;
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn column(name: &str, column_type: &str, nullable: bool) -> Value {
    json!({
        "name": name, "column_type": column_type, "primary_key": name == "id",
        "nullable": nullable, "auto_generate": name == "id"
    })
}

fn manifest(service: &str, tables: Value) -> Value {
    json!({"name": service, "version": "0.1.0", "mode": "crud", "tables": tables, "publishes": []})
}

fn sailings(columns: &[Value]) -> Value {
    let id = column("id", "uuid", false);
    json!([{
        "name": "sailing_requests",
        "columns": std::iter::once(&id).chain(columns).collect::<Vec<_>>()
    }])
}

async fn columns(d: &Deployment, schema: &str, table: &str) -> Vec<String> {
    d.texts(&format!(
        "SELECT column_name::text FROM information_schema.columns \
         WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"
    ))
    .await
}

async fn guarded(mode: &str) -> Option<Deployment> {
    Deployment::under("schema_guard", json!({"mode": mode})).await
}

#[tokio::test]
async fn the_schema_is_read_as_the_database_holds_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;

    let catalog = ServiceSessions::over(db.pool(), "catalog_service");
    let live = introspect::live_schema(&catalog)
        .await
        .expect("live schema");
    assert!(live.exists);
    let book = live.table("book").expect("book");
    let described: Vec<(&str, &str, TypeKind, bool, bool)> = book
        .columns
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.type_name.as_str(),
                c.kind,
                c.not_null,
                c.has_default,
            )
        })
        .collect();
    assert_eq!(
        described,
        [
            ("id", "uuid", TypeKind::Base, true, false),
            (
                "status",
                "character varying(50)",
                TypeKind::Base,
                true,
                true
            ),
            (
                "format",
                "catalog_service.book_format",
                TypeKind::Enum,
                true,
                true
            ),
            ("tags", "text[]", TypeKind::Base, true, true),
            (
                "replacement_cost",
                "numeric(12,2)",
                TypeKind::Base,
                false,
                false
            ),
            (
                "created_at",
                "timestamp without time zone",
                TypeKind::Base,
                true,
                true
            ),
            (
                "updated_at",
                "timestamp without time zone",
                TypeKind::Base,
                true,
                true
            ),
        ]
    );
    let gin = book.index("idx_book_tags").expect("gin index");
    assert_eq!(
        (gin.method.as_str(), gin.unique, gin.partial),
        ("gin", false, false)
    );
    assert_eq!(gin.columns, ["tags"]);
    let primary = book.index("book_pkey").expect("primary key index");
    assert!(primary.primary && primary.unique && primary.valid);
    let checks: Vec<&str> = book
        .constraints
        .iter()
        .filter(|c| c.kind == "c")
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(checks, ["book_cost_check", "book_tags_check"]);
    assert_eq!(book.triggers, ["touch_book_updated_at"]);
    let key = live.table("shelf_section").expect("shelf_section");
    assert_eq!(key.columns[0].type_name, "character varying(50)");
    assert!(live.table("settings").is_some());

    let loans = ServiceSessions::over(db.pool(), "loan_service");
    let live_loans = introspect::live_schema(&loans).await.expect("loans");
    let partial = live_loans.index("idx_loans_loan_kind").expect("index");
    assert_eq!(partial.0, "loans");

    let reporting = ServiceSessions::over(db.pool(), "reporting");
    let views = introspect::live_schema(&reporting).await.expect("views");
    assert_eq!(
        views.relation("book_overview").map(|r| r.kind),
        Some(RelationKind::View)
    );
    assert!(views.table("book_overview").is_none());

    let absent = ServiceSessions::over(db.pool(), "receipt_service");
    let nothing = introspect::live_schema(&absent).await.expect("absent");
    assert!(!nothing.exists && nothing.relations.is_empty());
}

#[tokio::test]
async fn what_depends_on_a_table_or_column_is_found_whoever_made_it() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let db = infra.create_database().await;
    let client = legacy::connect(&db.url).await;
    legacy::migrated(&client).await;
    client
        .batch_execute(
            "CREATE MATERIALIZED VIEW reporting.book_totals AS \
                 SELECT status, count(*) AS books FROM catalog_service.book GROUP BY status; \
             CREATE TABLE catalog_service.milestone ( \
                 id uuid PRIMARY KEY, \
                 book_id uuid NOT NULL REFERENCES catalog_service.book(id)); \
             CREATE TRIGGER loan_status AFTER UPDATE OF status ON catalog_service.book \
                 FOR EACH ROW EXECUTE FUNCTION catalog_service.touch_updated_at(); \
             ALTER TABLE catalog_service.book ENABLE ROW LEVEL SECURITY; \
             CREATE POLICY open_books ON catalog_service.book USING (status <> 'Withdrawn');",
        )
        .await
        .expect("dependents");
    let catalog = ServiceSessions::over(db.pool(), "catalog_service");
    let found = |targets: Vec<DropTarget>| {
        let catalog = &catalog;
        async move {
            introspect::dependents(catalog, &targets.into_iter().collect::<BTreeSet<_>>())
                .await
                .expect("dependents")
                .into_iter()
                .map(|d| (d.kind, d.name, d.on_target))
                .collect::<Vec<_>>()
        }
    };
    let status = DropTarget::Column {
        table: "book".into(),
        column: "status".into(),
    };
    assert_eq!(
        found(vec![status]).await,
        [
            (
                DependentKind::View,
                "reporting.book_overview".to_string(),
                false
            ),
            (
                DependentKind::MaterializedView,
                "reporting.book_totals".to_string(),
                false
            ),
            (DependentKind::Trigger, "loan_status".to_string(), true),
            (DependentKind::Policy, "open_books".to_string(), true),
            (DependentKind::Index, "idx_book_status".to_string(), true),
        ]
    );
    let id = DropTarget::Column {
        table: "book".into(),
        column: "id".into(),
    };
    let on_id = found(vec![id]).await;
    assert!(
        on_id.contains(&(
            DependentKind::Constraint,
            "milestone_book_id_fkey".to_string(),
            false
        )),
        "{on_id:?}"
    );
    assert!(
        on_id.contains(&(DependentKind::Constraint, "book_pkey".to_string(), true)),
        "{on_id:?}"
    );
    let on_table = found(vec![DropTarget::Table("book".into())]).await;
    for expected in [
        (DependentKind::View, "reporting.book_overview", false),
        (DependentKind::Trigger, "touch_book_updated_at", true),
        (DependentKind::Trigger, "loan_status", true),
        (DependentKind::Index, "idx_book_tags", true),
        (DependentKind::Constraint, "book_cost_check", true),
        (DependentKind::Constraint, "milestone_book_id_fkey", false),
        (DependentKind::Policy, "open_books", true),
    ] {
        let expected = (expected.0, expected.1.to_string(), expected.2);
        assert!(on_table.contains(&expected), "{expected:?} in {on_table:?}");
    }
    assert!(found(vec![DropTarget::Table("no_such_table".into())])
        .await
        .is_empty());
    assert!(found(vec![DropTarget::Column {
        table: "book".into(),
        column: "no_such_column".into()
    }])
    .await
    .is_empty());
    assert!(found(vec![]).await.is_empty());
}

fn book(columns: &[&str]) -> Value {
    let id = column("id", "uuid", false);
    let declared: Vec<Value> = columns.iter().map(|c| column(c, "text", false)).collect();
    json!([{"name": "book", "columns": std::iter::once(id).chain(declared).collect::<Vec<_>>()}])
}

#[tokio::test]
async fn schema_guard_refuses_dropping_viewed_column() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    legacy::migrated(&d.database).await;
    let body = d
        .deployed(json!({"manifest": manifest("catalog-service", book(&["status", "format"]))}))
        .await;
    assert_eq!(body["plan"]["schema_guard"], "enforce", "{body}");
    assert_eq!(
        body["plan"]["satisfied"],
        json!(["CREATE TABLE book"]),
        "{body}"
    );
    assert_eq!(body["schema_changes"], 0, "{body}");

    for purge in [false, true] {
        let (status, body) = d
            .deploy(json!({
                "manifest": manifest("catalog-service", book(&["format"])),
                "allow_destructive": true, "accept_drift": true, "purge": purge
            }))
            .await;
        assert_eq!(status, 422, "{body}");
        let error = body["error"].as_str().expect("error");
        assert!(
            error.contains("view reporting.book_overview depends on catalog_service.book.status"),
            "{error}"
        );
        assert!(
            error.contains("index catalog_service.idx_book_status"),
            "{error}"
        );
    }
    assert!(columns(&d, "catalog_service", "book")
        .await
        .contains(&"status".to_string()));
    assert_eq!(
        d.texts("SELECT count(*)::text FROM reporting.book_overview")
            .await,
        ["0"]
    );
    assert!(
        d.platform
            .metric(
                "cufflink_schema_guard_findings_total",
                &[("kind", "dependent")]
            )
            .await
            .is_some_and(|found| found >= 2.0),
        "dependents are counted"
    );
}

#[tokio::test]
async fn a_guard_that_warns_reports_and_runs_what_a_deploy_always_ran() {
    let Some(d) = guarded("warn").await else {
        return;
    };
    legacy::migrated(&d.database).await;
    d.database
        .batch_execute(
            "ALTER TABLE catalog_service.book ADD COLUMN source text NOT NULL DEFAULT 'web'; \
             CREATE INDEX book_source_partial ON catalog_service.book (source) WHERE source <> 'web'",
        )
        .await
        .expect("a column with an index made by hand");
    d.deployed(
        json!({"manifest": manifest("catalog-service", book(&["status", "format", "source"]))}),
    )
    .await;

    let (status, body) = d
        .deploy(json!({
            "manifest": manifest("catalog-service", book(&["format", "source"])),
            "allow_destructive": true
        }))
        .await;
    assert_eq!(status, 500, "Postgres refuses the drop: {body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("DROP COLUMN IF EXISTS \"status\"")),
        "{body}"
    );
    assert!(columns(&d, "catalog_service", "book")
        .await
        .contains(&"status".to_string()));

    let body = d
        .deployed(json!({
            "manifest": manifest("catalog-service", book(&["status", "format"])),
            "allow_destructive": true
        }))
        .await;
    assert_eq!(body["plan"]["schema_guard"], "warn", "{body}");
    assert_eq!(
        body["plan"]["schema_changes"],
        json!(["ALTER TABLE \"catalog_service\".\"book\" DROP COLUMN IF EXISTS \"source\""]),
        "{body}"
    );
    let dependents = body["plan"]["dependents"].as_array().expect("dependents");
    assert_eq!(dependents.len(), 1, "{body}");
    assert_eq!(dependents[0]["kind"], "index");
    assert_eq!(dependents[0]["name"], "book_source_partial");
    assert_eq!(dependents[0]["on"], "catalog_service.book.source");
    assert!(!columns(&d, "catalog_service", "book")
        .await
        .contains(&"source".to_string()));
}

#[tokio::test]
async fn schema_guard_counts_unmanaged_index_and_trigger() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let mut reference = column("reference", "text", false);
    reference["unique"] = json!(true);
    let mut tables = sailings(&[
        reference,
        column("status", "text", false),
        column("notes", "text", true),
    ]);
    tables[0]["indexes"] =
        json!([{"name": "sailings_by_status", "columns": ["status"], "unique": false}]);
    tables
        .as_array_mut()
        .expect("tables")
        .push(json!({"name": "batches", "columns": [column("id", "uuid", false)]}));
    d.deployed(json!({"manifest": manifest("ferry-service", tables.clone())}))
        .await;
    d.database
        .batch_execute(
            "CREATE UNIQUE INDEX scheduled_sailings_dedup \
                 ON ferry_service.sailing_requests (status, notes) WHERE status = 'pending'; \
             CREATE FUNCTION ferry_service.touch() RETURNS trigger AS $$ \
                 BEGIN RETURN NEW; END $$ LANGUAGE plpgsql; \
             CREATE TRIGGER touch_notes BEFORE UPDATE OF notes ON ferry_service.sailing_requests \
                 FOR EACH ROW EXECUTE FUNCTION ferry_service.touch(); \
             CREATE TRIGGER touch_batches BEFORE UPDATE ON ferry_service.batches \
                 FOR EACH ROW EXECUTE FUNCTION ferry_service.touch();",
        )
        .await
        .expect("objects made by hand");

    let without = |dropped: &str| {
        let mut tables = tables.clone();
        tables[0]["columns"]
            .as_array_mut()
            .expect("columns")
            .retain(|c| c["name"] != dropped);
        tables
    };
    let (status, body) = d.deploy(json!({"manifest": manifest("ferry-service", without("notes")), "allow_destructive": true}),
    )
    .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    for dependent in [
        "index ferry_service.scheduled_sailings_dedup depends on ferry_service.sailing_requests.notes",
        "trigger touch_notes on table ferry_service.sailing_requests depends on ferry_service.sailing_requests.notes",
    ] {
        assert!(error.contains(dependent), "{error}");
    }

    let mut one_table = tables.clone();
    one_table.as_array_mut().expect("tables").pop();
    let (status, body) = d
        .deploy(
            json!({"manifest": manifest("ferry-service", one_table), "allow_destructive": true}),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.contains(
            "trigger touch_batches on table ferry_service.batches depends on ferry_service.batches"
        ),
        "{error}"
    );
    assert!(!error.contains("batches_pkey"), "{error}");

    let mut unindexed = without("reference");
    unindexed[0]["indexes"] = json!([]);
    unindexed[0]["columns"]
        .as_array_mut()
        .expect("columns")
        .retain(|c| c["name"] != "status");
    let (status, body) = d
        .deploy(
            json!({"manifest": manifest("ferry-service", unindexed), "allow_destructive": true}),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(error.contains("scheduled_sailings_dedup"), "{error}");
    assert!(!error.contains("sailing_requests_reference_key"), "{error}");
    assert!(!error.contains("sailings_by_status"), "{error}");
}

#[tokio::test]
async fn hand_dropped_column_is_satisfied() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let with_photos = sailings(&[
        column("address", "text", false),
        column("boarding_photo_urls", "text", true),
        column("missing_item_photo_url", "text", true),
    ]);
    d.deployed(json!({"manifest": manifest("ferry-service", with_photos)}))
        .await;
    d.database
        .batch_execute(
            "ALTER TABLE ferry_service.sailing_requests \
                 DROP COLUMN IF EXISTS \"boarding_photo_urls\", \
                 DROP COLUMN IF EXISTS \"missing_item_photo_url\"",
        )
        .await
        .expect("dropped by hand");

    let body = d
        .deployed(json!({"manifest": manifest(
            "ferry-service",
            sailings(&[column("address", "text", false)])
        )}))
        .await;
    assert_eq!(body["schema_changes"], 0, "{body}");
    assert_eq!(body["plan"]["schema_changes"], json!([]), "{body}");
    assert_eq!(
        body["plan"]["satisfied"],
        json!([
            "DROP COLUMN sailing_requests.boarding_photo_urls",
            "DROP COLUMN sailing_requests.missing_item_photo_url"
        ]),
        "{body}"
    );
    assert_eq!(body["plan"]["drift"], json!([]), "{body}");
    assert_eq!(
        columns(&d, "ferry_service", "sailing_requests").await,
        ["id", "address"]
    );
}

#[tokio::test]
async fn forward_drop_archives_and_relaxes_not_null() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let long = format!("{}_received_at", "n".repeat(50));
    let full = sailings(&[
        column("address", "text", false),
        column("phone", "text", false),
        column(&long, "text", false),
    ]);
    let slim = sailings(&[column("address", "text", false)]);
    let first = d
        .deployed(json!({"manifest": manifest("ferry-service", full.clone())}))
        .await;
    let leaving = first["version"].as_i64().expect("version");
    let rows = format!("/svc/{}/ferry-service/sailing_requests", d.tenant.slug);
    let key = format!("ApiKey {}", d.tenant.api_key);
    let (status, kept) = d
        .platform
        .send(
            Method::POST,
            &rows,
            &key,
            Some(&json!({"address": "1 High St", "phone": "0123", long.clone(): "noon"})),
        )
        .await;
    assert!((200..300).contains(&status), "{kept}");

    let (status, body) = d
        .deploy(json!({"manifest": manifest("ferry-service", slim.clone())}))
        .await;
    assert_eq!(status, 422, "a drop still needs permission: {body}");

    let body = d
        .deployed(json!({"manifest": manifest("ferry-service", slim), "allow_destructive": true}))
        .await;
    let archived_phone = format!("_archived_v{leaving}_phone");
    let live = columns(&d, "ferry_service", "sailing_requests").await;
    assert_eq!(live.len(), 4, "{live:?}");
    assert!(live.contains(&archived_phone), "{live:?}");
    let archived_long = live
        .iter()
        .find(|name| name.starts_with(&format!("_archived_v{leaving}_nnn")))
        .expect("the long column's archive");
    assert_eq!(archived_long.len(), 63);
    assert_eq!(
        d.texts(&format!(
            "SELECT col_description('ferry_service.sailing_requests'::regclass, attnum) \
                 FROM pg_attribute WHERE attrelid = 'ferry_service.sailing_requests'::regclass \
                 AND attname = '{archived_long}'"
        ))
        .await,
        [format!("cufflink:archived:{long}")]
    );
    assert_eq!(body["schema_changes"], 5, "{body}");

    let (status, body) = d
        .platform
        .send(
            Method::POST,
            &rows,
            &key,
            Some(&json!({"address": "2 Low Rd"})),
        )
        .await;
    assert!(
        (200..300).contains(&status),
        "a row is written without the archived columns: {body}"
    );

    let (status, body) = d
        .deploy(json!({"manifest": manifest("ferry-service", full.clone())}))
        .await;
    assert_eq!(
        status, 500,
        "the restored column cannot be NOT NULL while a row lacks it: {body}"
    );
    assert!(
        columns(&d, "ferry_service", "sailing_requests")
            .await
            .contains(&archived_phone),
        "a deploy that fails restores nothing"
    );
    d.database
        .batch_execute(&format!(
            "UPDATE ferry_service.sailing_requests \
             SET {archived_phone} = 'unknown', {archived_long} = 'unknown' \
             WHERE {archived_phone} IS NULL"
        ))
        .await
        .expect("backfill");
    let body = d
        .deployed(json!({"manifest": manifest("ferry-service", full)}))
        .await;
    assert_eq!(body["plan"]["drift"], json!([]), "{body}");
    assert_eq!(
        columns(&d, "ferry_service", "sailing_requests").await,
        ["id", "address", "phone", long.as_str()]
    );
    assert_eq!(
        d.texts("SELECT phone FROM ferry_service.sailing_requests ORDER BY phone")
            .await,
        ["0123", "unknown"]
    );
    assert_eq!(
        d.texts(
            "SELECT attname::text FROM pg_attribute \
             WHERE attrelid = 'ferry_service.sailing_requests'::regclass \
             AND attnum > 0 AND NOT attisdropped AND NOT attnotnull"
        )
        .await,
        Vec::<String>::new()
    );
    assert_eq!(
        d.texts(&format!(
                "SELECT coalesce(col_description('ferry_service.sailing_requests'::regclass, attnum), '') \
                 FROM pg_attribute WHERE attrelid = 'ferry_service.sailing_requests'::regclass \
                 AND attname = '{long}'"
            )
        )
        .await,
        [""]
    );
}

#[tokio::test]
async fn a_rollback_archives_and_relaxes_not_null() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let address = [column("address", "text", false)];
    let first = d
        .deployed(json!({"manifest": manifest("ferry-service", sailings(&address))}))
        .await;
    let second = d
        .deployed(json!({"manifest": manifest(
            "ferry-service",
            sailings(&[address[0].clone(), column("phone", "text", false)])
        )}))
        .await;
    let key = format!("ApiKey {}", d.tenant.api_key);
    let (status, body) = d
        .platform
        .send(
            Method::POST,
            &format!(
                "/api/services/{}/rollback",
                first["service_id"].as_str().expect("id")
            ),
            &key,
            Some(&json!({"target_version": first["version"]})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let archived = format!("_archived_v{}_phone", second["version"]);
    assert_eq!(
        body["plan"]["schema_changes"],
        json!([
            format!("ALTER TABLE \"ferry_service\".\"sailing_requests\" RENAME COLUMN \"phone\" TO \"{archived}\""),
            format!("ALTER TABLE \"ferry_service\".\"sailing_requests\" ALTER COLUMN \"{archived}\" DROP NOT NULL"),
        ]),
        "{body}"
    );
    assert_eq!(body["schema_changes"], 2, "{body}");
    let (status, body) = d
        .platform
        .send(
            Method::POST,
            &format!("/svc/{}/ferry-service/sailing_requests", d.tenant.slug),
            &key,
            Some(&json!({"address": "2 Low Rd"})),
        )
        .await;
    assert!((200..300).contains(&status), "{body}");
    assert_eq!(
        columns(&d, "ferry_service", "sailing_requests").await,
        ["id", "address", archived.as_str()]
    );
}

#[tokio::test]
async fn a_purge_drops_what_would_be_archived() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let mut tables = sailings(&[
        column("address", "text", false),
        column("phone", "text", false),
    ]);
    tables
        .as_array_mut()
        .expect("tables")
        .push(json!({"name": "batches", "columns": [column("id", "uuid", false)]}));
    d.deployed(json!({"manifest": manifest("ferry-service", tables)}))
        .await;
    let body = d
        .deployed(json!({
            "manifest": manifest("ferry-service", sailings(&[column("address", "text", false)])),
            "allow_destructive": true, "purge": true
        }))
        .await;
    assert_eq!(
        body["plan"]["schema_changes"],
        json!([
            "DROP TABLE IF EXISTS \"ferry_service\".\"batches\" RESTRICT",
            "ALTER TABLE \"ferry_service\".\"sailing_requests\" DROP COLUMN IF EXISTS \"phone\" RESTRICT"
        ]),
        "{body}"
    );
    assert_eq!(
        d.texts(
            "SELECT table_name::text FROM information_schema.tables \
             WHERE table_schema = 'ferry_service'"
        )
        .await,
        ["sailing_requests"]
    );
    assert_eq!(
        columns(&d, "ferry_service", "sailing_requests").await,
        ["id", "address"]
    );
}

fn contacts(extra: &[Value]) -> Value {
    let mut depot_updates = column("depot_updates", "boolean", false);
    depot_updates["default_value"] = json!("false");
    let columns: Vec<Value> = [
        column("id", "uuid", false),
        column("ref_type", "text", false),
        column("status", "text", false),
        depot_updates,
        column("capacity", "integer", true),
    ]
    .into_iter()
    .chain(extra.iter().cloned())
    .collect();
    json!([{"name": "passenger_contacts", "columns": columns}])
}

const MADE_BY_HAND: &str = "\
    CREATE UNIQUE INDEX queued_emails_dedup \
        ON notification_service.passenger_contacts (ref_type, status) WHERE status = 'pending'; \
    CREATE INDEX queued_emails_due \
        ON notification_service.passenger_contacts (status) WHERE status = 'pending'; \
    CREATE INDEX contacts_primary_first \
        ON notification_service.passenger_contacts (ref_type, depot_updates DESC); \
    ALTER TABLE notification_service.passenger_contacts \
        ADD CONSTRAINT items_condition_check CHECK (capacity IS NULL OR capacity >= 0); \
    ALTER TABLE notification_service.passenger_contacts \
        ALTER COLUMN \"depot_updates\" SET DEFAULT true; \
    ALTER TABLE notification_service.passenger_contacts \
        ALTER COLUMN \"capacity\" DROP DEFAULT; \
    ALTER TABLE notification_service.passenger_contacts \
        ADD COLUMN IF NOT EXISTS condition text; \
    DO $$ BEGIN \
        CREATE ROLE reports_ro NOLOGIN NOINHERIT; \
    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$; \
    DO $$ BEGIN \
        CREATE ROLE dashboard_ro NOLOGIN; \
    EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$; \
    CREATE SCHEMA reporting; \
    CREATE VIEW reporting.customer_overview AS \
        SELECT id, ref_type, status, capacity FROM notification_service.passenger_contacts; \
    GRANT USAGE ON SCHEMA reporting TO reports_ro; \
    GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO reports_ro; \
    ALTER DEFAULT PRIVILEGES IN SCHEMA reporting GRANT SELECT ON TABLES TO reports_ro; \
    GRANT USAGE ON SCHEMA notification_service TO dashboard_ro; \
    GRANT SELECT ON ALL TABLES IN SCHEMA notification_service TO dashboard_ro;";

const WHAT_WAS_MADE_BY_HAND: &str = "\
    SELECT indexdef FROM pg_indexes WHERE schemaname = 'notification_service' \
    UNION ALL SELECT conname::text || ' ' || pg_get_constraintdef(oid) FROM pg_constraint \
        WHERE conrelid = 'notification_service.passenger_contacts'::regclass AND contype = 'c' \
    UNION ALL SELECT pg_get_expr(adbin, adrelid) FROM pg_attrdef \
        WHERE adrelid = 'notification_service.passenger_contacts'::regclass \
    UNION ALL SELECT viewname::text FROM pg_views WHERE schemaname = 'reporting' \
    UNION ALL SELECT grantee::text || ':' || privilege_type FROM information_schema.role_table_grants \
        WHERE grantee IN ('reports_ro', 'dashboard_ro') \
    UNION ALL SELECT 'default:' || defaclacl::text FROM pg_default_acl \
    ORDER BY 1";

#[tokio::test]
async fn what_was_made_by_hand_is_left_alone() {
    for mode in ["warn", "enforce"] {
        let Some(d) = guarded(mode).await else {
            return;
        };
        d.deployed(json!({"manifest": manifest("notification-service", contacts(&[]))}))
            .await;
        d.database
            .batch_execute(MADE_BY_HAND)
            .await
            .expect("what was made by hand");
        let before = d.texts(WHAT_WAS_MADE_BY_HAND).await;
        for made in [
            "queued_emails_dedup",
            "depot_updates DESC",
            "items_condition_check",
            "customer_overview",
            "default:",
        ] {
            assert!(
                before.iter().any(|found| found.contains(made)),
                "{made} in {before:?}"
            );
        }

        let unchanged = d.deployed(json!({"manifest": manifest("notification-service", contacts(&[])), "commit_hash": "abc"}),
        )
        .await;
        assert_eq!(unchanged["skipped"], true, "{mode}: {unchanged}");

        let body = d
            .deployed(json!({"manifest": manifest(
                "notification-service",
                contacts(&[column("digest", "text", true), column("condition", "text", false)])
            )}))
            .await;
        assert_eq!(
            body["plan"],
            json!({
                "schema_changes": [
                    "ALTER TABLE \"notification_service\".\"passenger_contacts\" ADD COLUMN IF NOT EXISTS \"digest\" TEXT",
                    "ALTER TABLE \"notification_service\".\"passenger_contacts\" ADD COLUMN IF NOT EXISTS \"condition\" TEXT DEFAULT NULL",
                    "ALTER TABLE \"notification_service\".\"passenger_contacts\" ALTER COLUMN \"condition\" SET NOT NULL"
                ],
                "schema_guard": mode,
                "satisfied": [],
                "drift": [],
                "dependents": [],
                "streams": [],
                "consumers": [],
                "warnings": []
            }),
            "{mode}: {body}"
        );
        assert_eq!(d.texts(WHAT_WAS_MADE_BY_HAND).await, before, "{mode}");
        assert_eq!(
            d.texts(
                "SELECT attname::text FROM pg_attribute \
                 WHERE attrelid = 'notification_service.passenger_contacts'::regclass \
                 AND attname IN ('condition', 'digest') AND attnotnull"
            )
            .await,
            ["condition"],
            "{mode}"
        );
        assert_eq!(
            d.texts("SELECT count(*)::text FROM reporting.customer_overview")
                .await,
            ["0"]
        );
    }
}

#[tokio::test]
async fn an_enforced_guard_refuses_a_drop_for_each_thing_made_by_hand_on_it() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    d.deployed(json!({"manifest": manifest("notification-service", contacts(&[]))}))
        .await;
    d.database
        .batch_execute(MADE_BY_HAND)
        .await
        .expect("what was made by hand");
    let before = d.texts(WHAT_WAS_MADE_BY_HAND).await;
    let without = |dropped: &str| {
        let mut tables = contacts(&[]);
        tables[0]["columns"]
            .as_array_mut()
            .expect("columns")
            .retain(|c| c["name"] != dropped);
        tables
    };
    let on = |column: &str, dependents: &[&str]| {
        dependents
            .iter()
            .map(|dependent| {
                format!("{dependent} depends on notification_service.passenger_contacts.{column}")
            })
            .collect::<Vec<_>>()
    };
    for (dropped, dependents) in [
        (
            "status",
            on(
                "status",
                &[
                    "index notification_service.queued_emails_dedup",
                    "index notification_service.queued_emails_due",
                    "view reporting.customer_overview",
                ],
            ),
        ),
        (
            "capacity",
            on(
                "capacity",
                &[
                    "constraint items_condition_check on table \
                     notification_service.passenger_contacts",
                    "view reporting.customer_overview",
                ],
            ),
        ),
        (
            "depot_updates",
            on(
                "depot_updates",
                &["index notification_service.contacts_primary_first"],
            ),
        ),
    ] {
        for purge in [false, true] {
            let (status, body) = d
                .deploy(json!({
                    "manifest": manifest("notification-service", without(dropped)),
                    "allow_destructive": true, "accept_drift": true, "purge": purge
                }))
                .await;
            assert_eq!(status, 422, "{dropped}: {body}");
            let error = body["error"].as_str().expect("error");
            for dependent in &dependents {
                assert!(error.contains(dependent.as_str()), "{dependent} in {error}");
            }
        }
    }
    assert_eq!(d.texts(WHAT_WAS_MADE_BY_HAND).await, before);
    assert_eq!(
        columns(&d, "notification_service", "passenger_contacts").await,
        [
            "id",
            "ref_type",
            "status",
            "depot_updates",
            "capacity",
            "condition"
        ]
    );
}

fn migration(version: u32, name: &str, sql: &str) -> Value {
    json!({"version": version, "name": name, "sql": sql})
}

async fn scalar(d: &Deployment, sql: &str) -> String {
    d.texts(sql).await.join(",")
}

#[tokio::test]
async fn an_enforced_guard_finishes_the_deploy_that_failed_after_its_additive_phase() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let address = column("address", "text", false);
    d.deployed(
        json!({"manifest": manifest("ferry-service", sailings(std::slice::from_ref(&address)))}),
    )
    .await;
    d.database
        .batch_execute("INSERT INTO ferry_service.sailing_requests (address) VALUES ('1 High St')")
        .await
        .expect("a row");

    let with_phone = sailings(&[address, column("phone", "text", false)]);
    let mut failing = manifest("ferry-service", with_phone.clone());
    failing["migrations"] = json!([migration(1, "fails", "SELECT 1 / 0;\n")]);
    failing["migration_history"] = json!({"table": "schema_history", "baseline": 0});
    let (status, body) = d.deploy(json!({"manifest": failing})).await;
    assert_eq!(status, 500, "{body}");
    let nullable = "SELECT (NOT attnotnull)::text FROM pg_attribute \
                    WHERE attrelid = 'ferry_service.sailing_requests'::regclass \
                    AND attname = 'phone'";
    assert_eq!(
        scalar(&d, nullable).await,
        "true",
        "the additive phase added the column and the deploy failed before it was NOT NULL"
    );
    d.database
        .batch_execute("UPDATE ferry_service.sailing_requests SET phone = '0123'")
        .await
        .expect("what on_migrate would have filled");

    let body = d
        .deployed(json!({"manifest": manifest("ferry-service", with_phone)}))
        .await;
    assert_eq!(
        body["plan"]["schema_changes"],
        json!([
            "ALTER TABLE \"ferry_service\".\"sailing_requests\" ADD COLUMN IF NOT EXISTS \"phone\" TEXT DEFAULT NULL",
            "ALTER TABLE \"ferry_service\".\"sailing_requests\" ALTER COLUMN \"phone\" SET NOT NULL"
        ]),
        "{body}"
    );
    assert_eq!(body["plan"]["satisfied"], json!([]), "{body}");
    assert_eq!(scalar(&d, nullable).await, "false");
}

#[tokio::test]
async fn an_enforced_guard_applies_a_check_whose_condition_changed() {
    let Some(d) = guarded("enforce").await else {
        return;
    };
    let checked = |expr: &str| {
        let mut tables = sailings(&[column("weight", "integer", false)]);
        tables[0]["checks"] = json!([{"name": "weight_allowed", "expr": expr}]);
        manifest("ferry-service", tables)
    };
    d.deployed(json!({"manifest": checked("weight > 0")})).await;
    let condition = "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
                     WHERE conrelid = 'ferry_service.sailing_requests'::regclass \
                     AND conname = 'weight_allowed'";
    assert_eq!(scalar(&d, condition).await, "CHECK ((weight > 0))");

    let body = d
        .deployed(json!({"manifest": checked("weight > 10")}))
        .await;
    assert_eq!(body["plan"]["satisfied"], json!([]), "{body}");
    assert_eq!(body["schema_changes"], 3, "{body}");
    assert_eq!(scalar(&d, condition).await, "CHECK ((weight > 10))");
    let refused = d
        .database
        .batch_execute("INSERT INTO ferry_service.sailing_requests (weight) VALUES (5)")
        .await
        .expect_err("the changed condition holds");
    assert_eq!(
        refused.as_db_error().and_then(|e| e.constraint()),
        Some("weight_allowed")
    );

    d.database
        .batch_execute(
            "ALTER TABLE ferry_service.sailing_requests DROP CONSTRAINT weight_allowed; \
             ALTER TABLE ferry_service.sailing_requests \
                 ADD CONSTRAINT weight_allowed CHECK (weight > 20)",
        )
        .await
        .expect("what a deploy that failed after its additive phase left");
    let body = d
        .deployed(json!({"manifest": checked("weight > 20")}))
        .await;
    assert_eq!(
        body["plan"]["satisfied"],
        json!(["ADD CHECK weight_allowed ON sailing_requests"]),
        "{body}"
    );
    assert_eq!(body["schema_changes"], 0, "{body}");
    assert_eq!(scalar(&d, condition).await, "CHECK ((weight > 20))");
}

#[tokio::test]
async fn without_a_guard_a_deploy_reports_only_what_it_ran() {
    let Some(d) = Deployment::start(&[]).await else {
        return;
    };
    let body = d
        .deployed(json!({"manifest": manifest(
            "ferry-service",
            sailings(&[column("address", "text", false)])
        )}))
        .await;
    assert_eq!(body["schema_changes"], 1, "{body}");
    assert_eq!(
        body["plan"],
        json!({
            "schema_changes": [
                "CREATE TABLE IF NOT EXISTS \"ferry_service\".\"sailing_requests\" (\"id\" UUID PRIMARY KEY DEFAULT gen_random_uuid(), \"address\" TEXT NOT NULL)"
            ],
            "streams": [],
            "consumers": [],
            "warnings": []
        }),
        "{body}"
    );
    let (status, body) = d
        .deploy(json!({
            "manifest": manifest("ferry-service", sailings(&[])),
            "allow_destructive": true, "accept_drift": true, "purge": true
        }))
        .await;
    assert_eq!(
        status, 200,
        "the flags mean nothing without a guard: {body}"
    );
    assert_eq!(
        body["plan"]["schema_changes"],
        json!([
            "ALTER TABLE \"ferry_service\".\"sailing_requests\" DROP COLUMN IF EXISTS \"address\""
        ]),
        "{body}"
    );
    let (_, version) = d.platform.get_json("/api/version").await.expect("version");
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|s| s.contains(&json!("schema_guard"))),
        "{version}"
    );
}
