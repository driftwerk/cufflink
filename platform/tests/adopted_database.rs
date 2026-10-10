use cufflink_testkit as common;

use common::{PlatformProcess, TestDb};
use reqwest::Method;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

struct Adoption {
    platform: PlatformProcess,
    db: TestDb,
    admin: String,
    suffix: String,
}

impl Adoption {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &[]).await;
        platform.wait_ready().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        Some(Self {
            platform,
            db,
            admin,
            suffix: Uuid::new_v4().simple().to_string()[..8].to_string(),
        })
    }

    async fn create_tenant(&self, slug: &str, database_name: Option<&str>) -> (u16, Value) {
        let mut body = json!({"name": slug, "slug": slug});
        if let Some(database) = database_name {
            body["database_name"] = json!(database);
        }
        self.platform
            .send(Method::POST, "/api/tenants", &self.admin, Some(&body))
            .await
    }

    async fn mint_key(&self, id: &str) -> String {
        let api_key = format!("cfk_{}", Uuid::new_v4().simple());
        let tenant_id: Uuid = id.parse().expect("tenant id");
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .execute(
                "INSERT INTO api_keys (tenant_id, key_hash, name) VALUES ($1, $2, 'integration')",
                &[&tenant_id, &hex::encode(Sha256::digest(api_key.as_bytes()))],
            )
            .await
            .expect("api key");
        format!("ApiKey {api_key}")
    }
}

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

fn books_manifest() -> Value {
    json!({
        "name": "books",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "books", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "name", "column_type": "text", "primary_key": false, "nullable": false,
             "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false,
             "nullable": false, "auto_generate": true},
        ]}],
        "publishes": [],
    })
}

#[tokio::test]
async fn adopted_tenant_db_never_dropped() {
    let Some(mut test) = Adoption::start().await else {
        return;
    };
    let database = format!("harbor_{}", test.suffix);
    let slug = format!("it-adopt-{}", test.suffix);
    let url = test.db.create_external_database(&database).await;
    let existing = connect(&url).await;
    existing
        .batch_execute(
            "CREATE SCHEMA legacy; CREATE TABLE legacy.kept (id INT PRIMARY KEY);
             INSERT INTO legacy.kept VALUES (1), (2), (3)",
        )
        .await
        .expect("what the database held before");

    let (status, tenant) = test.create_tenant(&slug, Some(&database)).await;
    assert_eq!(status, 200, "{tenant}");
    assert_eq!(tenant["database_name"], json!(database));
    let managed = cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&slug);
    assert!(
        !test.db.database_exists(&managed).await,
        "no database is created for a tenant that adopts one"
    );
    let extensions: i64 = existing
        .query_one(
            "SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto'",
            &[],
        )
        .await
        .expect("extensions")
        .get(0);
    assert_eq!(extensions, 0, "nothing is installed in an adopted database");

    let key = test.mint_key(tenant["id"].as_str().expect("id")).await;
    let (status, deployed) = test
        .platform
        .send(
            Method::POST,
            "/api/services/deploy",
            &key,
            Some(&json!({"manifest": books_manifest()})),
        )
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (status, created) = test
        .platform
        .send(
            Method::POST,
            &format!("/svc/{slug}/books/books"),
            &key,
            Some(&json!({"name": "first"})),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    let written: i64 = existing
        .query_one("SELECT count(*) FROM books.books", &[])
        .await
        .expect("the service's table is in the adopted database")
        .get(0);
    assert_eq!(written, 1);

    let tenant_path = format!("/api/tenants/{slug}");
    let (status, refused) = test
        .platform
        .send(
            Method::PUT,
            &tenant_path,
            &test.admin,
            Some(&json!({"database_name": "elsewhere"})),
        )
        .await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        json!(format!(
            "database_name is immutable: the tenant has '{database}'"
        ))
    );
    let (status, renamed) = test
        .platform
        .send(
            Method::PUT,
            &tenant_path,
            &test.admin,
            Some(&json!({"name": "Harbor", "database_name": database})),
        )
        .await;
    assert_eq!(status, 200, "{renamed}");
    assert_eq!(renamed["database_name"], json!(database));

    drop(existing);
    let (status, deleted) = test
        .platform
        .send(Method::DELETE, &tenant_path, &test.admin, None)
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert_eq!(deleted["database"], "kept (adopted)");
    assert_eq!(deleted["database_dropped"], false);
    assert!(test.db.database_exists(&database).await);
    let after = connect(&url).await;
    let kept: i64 = after
        .query_one("SELECT count(*) FROM legacy.kept", &[])
        .await
        .expect("what the database held before is still there")
        .get(0);
    let books: i64 = after
        .query_one("SELECT count(*) FROM books.books", &[])
        .await
        .expect("and so is what the service wrote")
        .get(0);
    assert_eq!((kept, books), (3, 1));
}

#[tokio::test]
async fn a_database_that_cannot_be_adopted_fails_the_creation() {
    let Some(mut test) = Adoption::start().await else {
        return;
    };
    let tenants = |test: &Adoption| {
        let pool = test.db.pool();
        async move {
            pool.get()
                .await
                .expect("checkout")
                .query_one("SELECT count(*) FROM tenants", &[])
                .await
                .expect("tenants")
                .get::<_, i64>(0)
        }
    };

    let missing = format!("absent_{}", test.suffix);
    let (status, body) = test.create_tenant("it-missing", Some(&missing)).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().expect("error");
    assert!(
        error.starts_with(&format!(
            "database_name: cannot connect to database '{missing}'"
        )) && error.ends_with(&format!(
            "GRANT CONNECT, CREATE ON DATABASE \"{missing}\" TO <platform role>"
        )),
        "{error}"
    );
    assert!(!test.db.database_exists(&missing).await);

    for (name, reason) in [
        (
            "cufflink_acme",
            "is named like a database the platform creates and drops",
        ),
        ("postgres", "is a database of the server itself"),
        ("my-db", "is not an identifier"),
    ] {
        let (status, body) = test.create_tenant("it-refused", Some(name)).await;
        assert_eq!(status, 422, "{name}: {body}");
        assert!(
            body["error"].as_str().is_some_and(|e| e.contains(reason)),
            "{name}: {body}"
        );
    }
    assert_eq!(
        tenants(&test).await,
        0,
        "a refused creation leaves no tenant"
    );

    let database = format!("shared_{}", test.suffix);
    test.db.create_external_database(&database).await;
    let (status, first) = test
        .create_tenant(&format!("it-first-{}", test.suffix), Some(&database))
        .await;
    assert_eq!(status, 200, "{first}");
    let (status, second) = test
        .create_tenant(&format!("it-second-{}", test.suffix), Some(&database))
        .await;
    assert_eq!(status, 409, "{second}");
    assert_eq!(
        second["error"],
        json!(format!("Database '{database}' is already a tenant's"))
    );
    assert_eq!(tenants(&test).await, 1);
}

#[tokio::test]
async fn a_tenant_without_a_database_name_gets_and_loses_its_own() {
    let Some(mut test) = Adoption::start().await else {
        return;
    };
    let slug = format!("it-managed-{}", test.suffix);
    let managed = cufflink_platform::tenant_db::TenantPoolManager::managed_db_name(&slug);
    test.db.adopt_tenant_db(&slug);

    let (status, tenant) = test.create_tenant(&slug, None).await;
    assert_eq!(status, 200, "{tenant}");
    assert!(
        tenant.get("database_name").is_none(),
        "the response of a tenant that adopts nothing is the one it always was: {tenant}"
    );
    assert!(test.db.database_exists(&managed).await);

    let tenant_path = format!("/api/tenants/{slug}");
    let (status, refused) = test
        .platform
        .send(
            Method::PUT,
            &tenant_path,
            &test.admin,
            Some(&json!({"database_name": "harbor"})),
        )
        .await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        "database_name is immutable: the tenant has the database the platform \
         created for it"
    );

    let (status, deleted) = test
        .platform
        .send(Method::DELETE, &tenant_path, &test.admin, None)
        .await;
    assert_eq!(status, 200, "{deleted}");
    assert_eq!(deleted["database"], "dropped");
    assert_eq!(deleted["database_dropped"], true);
    assert!(!test.db.database_exists(&managed).await);
}

#[tokio::test]
async fn the_platforms_own_database_is_never_a_tenants() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let mut db = infra.create_database().await;
    let own = format!("control_{}", &Uuid::new_v4().simple().to_string()[..8]);
    let url = db.create_external_database(&own).await;
    let platform = PlatformProcess::start(&url, infra.nats_url(), &[]).await;
    platform.wait_ready().await;
    let admin = {
        let api_key = format!("ck_platform_{}", Uuid::new_v4().simple());
        connect(&url)
            .await
            .execute(
                "INSERT INTO api_keys (key_hash, name, is_platform_admin) VALUES ($1, 'admin', true)",
                &[&hex::encode(Sha256::digest(api_key.as_bytes()))],
            )
            .await
            .expect("admin key");
        format!("ApiKey {api_key}")
    };
    for name in [own.clone(), own.to_uppercase()] {
        let (status, body) = platform
            .send(
                Method::POST,
                "/api/tenants",
                &admin,
                Some(&json!({"name": "it-own", "slug": "it-own", "database_name": name})),
            )
            .await;
        assert_eq!(status, 422, "{body}");
        assert_eq!(
            body["error"],
            json!(format!(
                "database_name: '{name}' is the platform's own database"
            ))
        );
    }
    let tenants: i64 = connect(&url)
        .await
        .query_one("SELECT count(*) FROM tenants", &[])
        .await
        .expect("tenants")
        .get(0);
    assert_eq!(tenants, 0);
}

#[tokio::test]
async fn one_database_adopted_by_two_tenants_at_once_is_one_tenants() {
    let Some(mut test) = Adoption::start().await else {
        return;
    };
    let database = format!("raced_{}", test.suffix);
    test.db.create_external_database(&database).await;
    let (first, second) = (
        format!("it-race-a-{}", test.suffix),
        format!("it-race-b-{}", test.suffix),
    );
    let (one, other) = tokio::join!(
        test.create_tenant(&first, Some(&database)),
        test.create_tenant(&second, Some(&database)),
    );
    let mut statuses = [one.0, other.0];
    statuses.sort_unstable();
    assert_eq!(statuses, [200, 409], "{} / {}", one.1, other.1);
    let refused = if one.0 == 409 { one.1 } else { other.1 };
    assert_eq!(
        refused["error"],
        json!(format!("Database '{database}' is already a tenant's"))
    );
    let adopters: i64 = test
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM tenants WHERE database_name = $1",
            &[&database],
        )
        .await
        .expect("tenants")
        .get(0);
    assert_eq!(adopters, 1);
}
