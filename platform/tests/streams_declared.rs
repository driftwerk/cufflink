use cufflink_testkit as common;

use async_nats::jetstream::{self, stream};
use common::{PlatformProcess, Tenant};
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use uuid::Uuid;

async fn jetstream(nats_url: &str) -> jetstream::Context {
    jetstream::new(async_nats::connect(nats_url).await.expect("nats"))
}

struct Declared {
    platform: PlatformProcess,
    tenant: Tenant,
    js: jetstream::Context,
    stream: String,
    prefix: String,
    _db: common::TestDb,
}

async fn claimed() -> Option<Declared> {
    let infra = common::infra().await?;
    let nats_url = infra.require_nats()?;
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let id = Uuid::new_v4().simple().to_string()[..10].to_string();
    let (stream, prefix) = (format!("BOOKS_{}", id.to_uppercase()), format!("books{id}"));
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/nats-claims", tenant.slug),
            &format!("ApiKey {}", db.mint_admin_key().await),
            Some(&json!({"subjects": [format!("{prefix}.>")], "streams": [stream]})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    Some(Declared {
        platform,
        js: jetstream(nats_url).await,
        tenant,
        stream,
        prefix,
        _db: db,
    })
}

impl Declared {
    fn stream(&self, subjects: &[&str], limits: Value) -> Value {
        let mut declared = json!({
            "name": self.stream,
            "subjects": subjects
                .iter()
                .map(|subject| format!("{}.{subject}", self.prefix))
                .collect::<Vec<_>>(),
            "retention": "limits",
        });
        declared
            .as_object_mut()
            .expect("object")
            .extend(limits.as_object().expect("object").clone());
        declared
    }

    fn manifest(&self, service: &str, stream: Value, subscribes_to: Option<&str>) -> Value {
        let subscriptions: Vec<Value> = subscribes_to
            .iter()
            .map(|subject| {
                json!({
                    "subject": format!("{}.{subject}", self.prefix),
                    "handler": "drop_loans",
                    "handler_type": "delete_cascade",
                    "config": {"table": "loans", "foreign_key": "book_id"},
                    "stream": self.stream,
                })
            })
            .collect();
        json!({
            "name": service,
            "version": "0.1.0",
            "mode": "crud",
            "tables": [{"name": "loans", "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
                 "auto_generate": true},
                {"name": "book_id", "column_type": "uuid", "primary_key": false,
                 "nullable": false, "auto_generate": false},
            ]}],
            "streams": [stream],
            "subscriptions": subscriptions,
            "publishes": [],
        })
    }

    async fn config(&self) -> Option<stream::Config> {
        self.js
            .get_stream(&self.stream)
            .await
            .ok()
            .map(|stream| stream.cached_info().config.clone())
    }

    async fn holders(&self) -> Vec<(String, Option<String>)> {
        self._db
            .pool()
            .get()
            .await
            .expect("platform database")
            .query(
                "SELECT ts.name, s.name FROM tenant_streams ts \
                 LEFT JOIN services s ON s.id = ts.declared_by \
                 WHERE ts.tenant_id = $1 ORDER BY ts.name",
                &[&self.tenant.id],
            )
            .await
            .expect("tenant_streams")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }
}

fn subjects(declared: &Declared, names: &[&str]) -> Vec<String> {
    names
        .iter()
        .map(|name| format!("{}.{name}", declared.prefix))
        .collect()
}

const THIRTY_DAYS: u64 = 30 * 24 * 60 * 60;

#[tokio::test]
async fn stream_declaration_extends_never_shrinks() {
    let Some(declared) = claimed().await else {
        return;
    };
    declared
        .js
        .create_stream(stream::Config {
            name: declared.stream.clone(),
            subjects: subjects(&declared, &["updated", "ingest.request"]),
            retention: stream::RetentionPolicy::Limits,
            max_age: Duration::from_secs(90 * 24 * 60 * 60),
            max_messages: 500_000,
            ..Default::default()
        })
        .await
        .expect("the stream the tenant's own services created");
    declared
        .js
        .publish(format!("{}.updated", declared.prefix), "{}".into())
        .await
        .expect("publish")
        .await
        .expect("stored");

    let extends = declared.manifest(
        "catalog-service",
        declared.stream(&["created", "updated"], json!({"max_msgs": 600_000})),
        Some("created"),
    );
    let (status, body) = declared.platform.deploy(&declared.tenant, &extends).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["streams"],
        json!([{"op": "update", "stream": declared.stream, "changes": ["subjects", "max_msgs"]}])
    );
    assert_eq!(
        body["plan"]["warnings"],
        json!([format!(
            "stream '{}' captures '{}.ingest.request', which the manifest does not declare; it \
             is kept",
            declared.stream, declared.prefix
        )])
    );
    assert_eq!(
        body["plan"]["consumers"][0]["op"], "create",
        "on the subject the same deploy added: {body}"
    );
    let config = declared.config().await.expect("stream");
    assert_eq!(
        config.subjects,
        subjects(&declared, &["updated", "ingest.request", "created"])
    );
    assert_eq!(
        (config.max_age, config.max_messages, config.max_bytes),
        (Duration::from_secs(90 * 24 * 60 * 60), 600_000, -1),
        "what is not declared stays the server's"
    );
    let stream = declared
        .js
        .get_stream(&declared.stream)
        .await
        .expect("stream");
    assert_eq!(stream.cached_info().state.messages, 1, "and what it held");

    let shrinks = declared.manifest(
        "catalog-service",
        declared.stream(
            &["created", "updated"],
            json!({"max_age_secs": THIRTY_DAYS}),
        ),
        Some("created"),
    );
    let (status, body) = declared.platform.deploy(&declared.tenant, &shrinks).await;
    assert_eq!(status, 422, "{body}");
    let error = body["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("max_age_secs is 7776000 and is declared 2592000"),
        "{body}"
    );
    let changes = declared.manifest(
        "catalog-service",
        json!({"name": declared.stream, "subjects": [format!("{}.created", declared.prefix)],
               "retention": "work_queue"}),
        None,
    );
    let (status, body) = declared.platform.deploy(&declared.tenant, &changes).await;
    assert_eq!(status, 422, "{body}");
    assert_eq!(
        declared.config().await.expect("stream"),
        config,
        "a deploy that is refused changes nothing"
    );
}

#[tokio::test]
async fn a_declared_stream_is_created_and_outlives_its_declaration() {
    let Some(declared) = claimed().await else {
        return;
    };
    let limits =
        json!({"max_age_secs": THIRTY_DAYS, "max_msgs": 100_000, "max_bytes": 268_435_456});
    let declares = |service: &str, limits: &Value, subscribes_to| {
        declared.manifest(
            service,
            declared.stream(&["created", "deleted"], limits.clone()),
            subscribes_to,
        )
    };

    let (status, body) = declared
        .platform
        .deploy(
            &declared.tenant,
            &declares("catalog-service", &limits, None),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["streams"],
        json!([{"op": "create", "stream": declared.stream}])
    );
    let created = declared.config().await.expect("stream");
    assert_eq!(
        (
            created.subjects.clone(),
            created.retention,
            created.storage,
            created.max_age,
            created.max_messages,
            created.max_bytes,
        ),
        (
            subjects(&declared, &["created", "deleted"]),
            stream::RetentionPolicy::Limits,
            stream::StorageType::File,
            Duration::from_secs(THIRTY_DAYS),
            100_000,
            268_435_456
        )
    );
    assert_eq!(
        declared.holders().await,
        [(declared.stream.clone(), Some("catalog-service".to_string()))]
    );

    let (status, body) = declared
        .platform
        .deploy(
            &declared.tenant,
            &declares("notes-service", &limits, Some("deleted")),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["streams"],
        json!([{"op": "keep", "stream": declared.stream}])
    );
    let (status, body) = declared
        .platform
        .deploy(
            &declared.tenant,
            &declares(
                "audit-service",
                &json!({"max_age_secs": THIRTY_DAYS, "max_msgs": 200_000}),
                None,
            ),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().is_some_and(
            |error| error.contains("declared by service") && error.contains("and differently")
        ),
        "{body}"
    );

    let mut undeclared = declares("catalog-service", &limits, None);
    undeclared["streams"] = json!([]);
    let (status, body) = declared
        .platform
        .deploy(&declared.tenant, &undeclared)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(declared.config().await, Some(created), "the stream stays");
    let deadline = Instant::now() + Duration::from_secs(10);
    while declared.holders().await != [(declared.stream.clone(), Some("notes-service".to_string()))]
    {
        assert!(
            Instant::now() < deadline,
            "the stream passes to the service that still declares it: {:?}",
            declared.holders().await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_stream_the_tenant_does_not_claim_is_refused() {
    let Some(declared) = claimed().await else {
        return;
    };
    let foreign = json!({"name": "SOMEONE_ELSES", "subjects": [format!("{}.created", declared.prefix)],
                         "retention": "limits"});
    let outside = json!({"name": declared.stream, "subjects": ["elsewhere.created"],
                         "retention": "limits"});
    for stream in [foreign, outside] {
        let (status, body) = declared
            .platform
            .deploy(
                &declared.tenant,
                &declared.manifest("catalog-service", stream, None),
            )
            .await;
        assert_eq!(status, 422, "{body}");
    }
    assert_eq!(declared.config().await, None);
    assert_eq!(declared.holders().await, []);
}
