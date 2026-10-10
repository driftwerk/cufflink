use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer::DeliverPolicy};
use common::{PlatformProcess, Tenant, TestDb};
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;

const REDACTED: &str = "[redacted]";
const QUIET: Duration = Duration::from_secs(2);
const PATIENCE: Duration = Duration::from_secs(30);

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true,
           "auto_generate": false})
}

fn id() -> Value {
    json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
           "auto_generate": true})
}

fn clinic() -> Value {
    json!({
        "name": "clinic",
        "version": "0.1.0",
        "mode": "crud",
        "graphql": true,
        "custom_routes": [{
            "method": "POST", "path": "/echo", "handler": "echo",
            "posture": {"kind": "public", "reason": "integration test"},
            "audit_redact": ["card.number"],
        }],
        "tables": [
            {
                "name": "patients",
                "columns": [id(), column("name", "text"), column("dob", "text"),
                            column("bank_iban", "text"), column("bank_bic", "text")],
                "audit_redact": ["dob", "bank_*"],
                "sensitive": true
            },
            {"name": "visits", "columns": [id(), column("reason", "text")]}
        ],
        "publishes": [],
    })
}

struct Clinic {
    platform: PlatformProcess,
    tenant: Tenant,
    admin: String,
    legacy: async_nats::Subscriber,
    events: async_nats::Subscriber,
    js: jetstream::Context,
    _db: TestDb,
}

impl Clinic {
    async fn start(settings: &[(&str, &str)]) -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, Some(nats_url), settings).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let (status, deployed) = platform.deploy(&tenant, &clinic()).await;
        assert_eq!(status, 200, "{deployed}");
        let nats = async_nats::connect(nats_url).await.expect("nats");
        Some(Self {
            legacy: nats
                .subscribe(format!("dw.{}.logs", tenant.slug))
                .await
                .expect("subscribe"),
            events: nats
                .subscribe(format!("dw.{}.clinic.>", tenant.slug))
                .await
                .expect("subscribe"),
            js: jetstream::new(nats),
            platform,
            tenant,
            admin,
            _db: db,
        })
    }

    async fn enforce(&self) {
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"outbox": {"mode": "enforce"}}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn asked(
        &self,
        method: Method,
        path: &str,
        request_id: &str,
        body: Option<Value>,
    ) -> reqwest::Response {
        let url = format!(
            "{}/svc/{}/clinic{path}",
            self.platform.base_url, self.tenant.slug
        );
        let request = self
            .platform
            .http
            .request(method.clone(), url)
            .header("authorization", format!("ApiKey {}", self.tenant.api_key))
            .header("x-request-id", request_id);
        let response = match body {
            Some(body) => request.json(&body),
            None => request,
        }
        .send()
        .await
        .expect("request");
        assert_eq!(response.status().as_u16(), 200, "{method} {path}");
        response
    }

    async fn ok(&self, method: Method, path: &str, request_id: &str, body: Option<Value>) -> Value {
        self.asked(method, path, request_id, body)
            .await
            .json()
            .await
            .unwrap_or(Value::Null)
    }

    async fn kept(&self, count: usize, through_an_outbox: bool) -> Vec<Value> {
        let records = self
            .js
            .get_stream("CUFFLINK_AUDIT")
            .await
            .expect("the stream of durable records")
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: format!("cufflink.audit.{}.clinic", self.tenant.slug),
                deliver_policy: DeliverPolicy::All,
                ..Default::default()
            })
            .await
            .expect("consumer");
        let mut messages = records.messages().await.expect("messages");
        let mut kept = Vec::with_capacity(count);
        while kept.len() < count {
            let record = tokio::time::timeout(Duration::from_secs(15), messages.next())
                .await
                .unwrap_or_else(|_| panic!("{} of {count} durable records: {kept:?}", kept.len()))
                .expect("the stream of records")
                .expect("a record");
            assert_eq!(
                record
                    .headers
                    .as_ref()
                    .is_some_and(|headers| headers.get("Nats-Msg-Id").is_some()),
                through_an_outbox,
                "a record of an outbox is published under the id of its row"
            );
            kept.push(serde_json::from_slice(&record.payload).expect("json"));
            record.ack().await.expect("ack");
        }
        let more = tokio::time::timeout(QUIET, messages.next()).await;
        assert!(more.is_err(), "no more than {count}: {more:?}");
        kept
    }
}

async fn heard(subscriber: &mut async_nats::Subscriber, expected: usize) -> Vec<Value> {
    let mut messages = Vec::new();
    loop {
        let patience = if messages.len() < expected {
            PATIENCE
        } else {
            QUIET
        };
        match tokio::time::timeout(patience, subscriber.next()).await {
            Ok(Some(message)) => {
                messages.push(serde_json::from_slice(&message.payload).expect("json"))
            }
            _ => return messages,
        }
    }
}

fn keys(record: &Value) -> Vec<&str> {
    record
        .as_object()
        .expect("an object")
        .keys()
        .map(String::as_str)
        .collect()
}

const LEGACY_KEYS: [&str; 10] = [
    "action",
    "data",
    "previous",
    "record_id",
    "service",
    "table",
    "tenant",
    "timestamp",
    "user_id",
    "user_name",
];

#[tokio::test]
async fn durable_audit_and_redaction() {
    let Some(mut clinic) = Clinic::start(&[]).await else {
        return;
    };
    let slug = clinic.tenant.slug.clone();

    let visit = clinic
        .ok(
            Method::POST,
            "/visits",
            "req-off",
            Some(json!({"reason": "check-up"})),
        )
        .await;
    let before = heard(&mut clinic.legacy, 1).await;
    assert_eq!(before.len(), 1, "{before:?}");
    assert_eq!(keys(&before[0]), LEGACY_KEYS);
    assert_eq!(heard(&mut clinic.events, 1).await.len(), 1);

    clinic.enforce().await;
    let ada = json!({"name": "Ada", "dob": "1990-01-01", "bank_iban": "GB00", "bank_bic": "X"});
    let created = clinic
        .ok(Method::POST, "/patients", "req-create", Some(ada.clone()))
        .await;
    let patient = created["id"].as_str().expect("an id").to_string();
    assert_eq!(
        created["dob"], "1990-01-01",
        "the caller is answered its row"
    );
    let path = format!("/patients/{patient}");
    let updated = clinic
        .ok(
            Method::PUT,
            &path,
            "req-update",
            Some(json!({"name": "Ada L.", "dob": "1990-01-02"})),
        )
        .await;
    assert_eq!(updated["name"], "Ada L.");
    let again = clinic
        .ok(
            Method::POST,
            "/visits",
            "req-visit",
            Some(json!({"reason": "follow-up"})),
        )
        .await;
    let listed = clinic
        .ok(
            Method::GET,
            "/patients?name=Ada%20L.&sort=name",
            "req-list",
            None,
        )
        .await;
    assert_eq!(listed["results"], json!([updated]), "{listed}");
    assert_eq!(
        clinic.ok(Method::GET, &path, "req-get", None).await,
        updated
    );
    clinic
        .ok(Method::GET, "/visits?sort=reason", "req-unrecorded", None)
        .await;
    assert_eq!(
        clinic.ok(Method::DELETE, &path, "req-delete", None).await,
        json!({"deleted": true})
    );

    let events = heard(&mut clinic.events, 4).await;
    let shown: Vec<(&Value, &Value)> = events
        .iter()
        .map(|event| (&event["action"], &event["data"]["dob"]))
        .collect();
    assert_eq!(
        shown,
        [
            (&json!("created"), &json!("1990-01-01")),
            (&json!("updated"), &json!("1990-01-02")),
            (&json!("created"), &Value::Null),
            (&json!("deleted"), &json!("1990-01-02")),
        ],
        "an event holds its row as it is: {events:?}"
    );

    let redacted = |name: &str| json!({"id": patient, "name": name, "dob": REDACTED, "bank_iban": REDACTED, "bank_bic": REDACTED});
    let legacy = heard(&mut clinic.legacy, 4).await;
    let told: Vec<(&Value, &Value, &Value, &Value)> = legacy
        .iter()
        .map(|record| {
            assert_eq!(keys(record), LEGACY_KEYS, "{record}");
            (
                &record["action"],
                &record["table"],
                &record["data"],
                &record["previous"],
            )
        })
        .collect();
    let (ada, ada_l, follow_up) = (redacted("Ada"), redacted("Ada L."), json!(again));
    assert_eq!(
        told,
        [
            (&json!("create"), &json!("patients"), &ada, &Value::Null),
            (&json!("update"), &json!("patients"), &ada_l, &Value::Null),
            (&json!("create"), &json!("visits"), &follow_up, &Value::Null),
            (&json!("delete"), &json!("patients"), &Value::Null, &ada_l),
        ],
        "no read, and no value that is redacted"
    );
    let unredacted = |record: &Value, request: &str| {
        json!({
            "timestamp": record["timestamp"], "tenant": slug, "service": "clinic",
            "table": "visits", "action": "create", "user_id": record["user_id"],
            "user_name": "integration", "record_id": record["data"]["id"],
            "data": {"id": record["data"]["id"], "reason": request}, "previous": null,
        })
        .to_string()
    };
    assert_eq!(before[0].to_string(), unredacted(&before[0], "check-up"));
    assert_eq!(before[0]["data"], visit);
    assert_eq!(
        legacy[2].to_string(),
        unredacted(&legacy[2], "follow-up"),
        "as it is with the outbox off, byte for byte"
    );

    let durable = clinic.kept(6, true).await;
    let expected = [
        (
            "create",
            "patients",
            "req-create",
            json!(["bank_bic", "bank_iban", "dob"]),
        ),
        (
            "update",
            "patients",
            "req-update",
            json!(["bank_bic", "bank_iban", "dob"]),
        ),
        ("create", "visits", "req-visit", json!([])),
        ("read", "patients", "req-list", json!([])),
        ("read", "patients", "req-get", json!([])),
        (
            "delete",
            "patients",
            "req-delete",
            json!(["bank_bic", "bank_iban", "dob"]),
        ),
    ];
    for (record, (action, table, request, hidden)) in durable.iter().zip(&expected) {
        assert_eq!(
            (
                &record["v"],
                record["action"].as_str(),
                record["table"].as_str(),
                record["request_id"].as_str(),
                &record["redacted"],
                record["tenant"].as_str(),
                record["service"].as_str(),
            ),
            (
                &json!(2),
                Some(*action),
                Some(*table),
                Some(*request),
                hidden,
                Some(slug.as_str()),
                Some("clinic")
            ),
            "{record}"
        );
        assert_eq!(record["actor"]["kind"], "api_key", "{record}");
        assert_eq!(record["actor"]["sub"], record["user_id"], "{record}");
        assert!(
            record["actor"]["key_id"]
                .as_str()
                .is_some_and(|key| key.parse::<uuid::Uuid>().is_ok()),
            "{record}"
        );
    }
    assert_eq!(
        (&durable[0]["data"], &durable[0]["previous"]),
        (&ada, &Value::Null)
    );
    assert_eq!(durable[1]["data"], ada_l);
    assert_eq!(
        durable[1]["previous"]["name"], "Ada",
        "an update of a table that records it holds the row as it was: {}",
        durable[1]
    );
    for hidden in ["dob", "bank_iban", "bank_bic"] {
        assert_eq!(durable[1]["previous"][hidden], REDACTED, "{}", durable[1]);
    }
    assert_eq!(durable[2]["data"], follow_up);
    let read = |ids: Value, filtered: Value, sort: Value| {
        json!({
            "record_ids": ids, "returned": 1, "truncated": false,
            "query": {"filter_columns": filtered, "search": false, "sort": sort},
        })
    };
    assert_eq!(
        durable[3]["data"],
        read(json!([patient]), json!(["name"]), json!("name")),
        "what was asked for is recorded, and not what it was filtered to"
    );
    assert_eq!(
        durable[4]["data"],
        read(json!([patient]), json!([]), Value::Null)
    );
    assert_eq!(
        (&durable[5]["data"], &durable[5]["previous"]),
        (&Value::Null, &ada_l)
    );

    for (class, count) in [("write", 5.0), ("read", 2.0)] {
        assert_eq!(
            clinic
                .platform
                .metric(
                    "cufflink_audit_records_total",
                    &[("tenant", &slug), ("action_class", class)]
                )
                .await,
            Some(count),
            "{class}"
        );
    }
    let (_, version) = clinic
        .platform
        .api_get(&clinic.tenant, "/api/version")
        .await;
    assert!(
        version["supports"]
            .as_array()
            .is_some_and(|supports| supports.contains(&json!("audit_redaction"))),
        "{version}"
    );
}

#[tokio::test]
async fn the_reads_of_streams_and_queries_and_the_calls_of_routes_are_recorded() {
    let Some(mut clinic) = Clinic::start(&[]).await else {
        return;
    };
    clinic.enforce().await;
    let created = clinic
        .ok(
            Method::POST,
            "/patients",
            "req-create",
            Some(json!({"name": "Ada", "dob": "1990-01-01"})),
        )
        .await;
    let patient = created["id"].as_str().expect("an id").to_string();

    let queried = clinic
        .ok(
            Method::POST,
            "/graphql",
            "req-query",
            Some(json!({"query": "{ patients(name: \"Ada\") { id name } visits { id } }"})),
        )
        .await;
    assert_eq!(
        queried["data"]["patients"],
        json!([{"id": patient, "name": "Ada"}]),
        "{queried}"
    );
    let stream = clinic
        .asked(Method::GET, "/patients/stream", "req-stream", None)
        .await;
    drop(stream);
    let echoed = clinic
        .ok(
            Method::POST,
            "/_fn/echo",
            "req-call",
            Some(json!({"said": "hello", "card": {"number": "4242", "brand": "visa"}})),
        )
        .await;
    assert_eq!(
        echoed["body"],
        json!({"said": "hello", "card": {"number": "4242", "brand": "visa"}}),
        "the guest is given the body whole: {echoed}"
    );

    let legacy = heard(&mut clinic.legacy, 2).await;
    let told: Vec<&Value> = legacy.iter().map(|record| &record["action"]).collect();
    assert_eq!(told, [&json!("create"), &json!("custom:echo")]);
    assert_eq!(keys(&legacy[1]), LEGACY_KEYS);
    assert_eq!(
        (
            &legacy[1]["table"],
            &legacy[1]["record_id"],
            &legacy[1]["data"]
        ),
        (
            &Value::Null,
            &Value::Null,
            &json!({"said": "hello", "card": {"number": "[redacted]", "brand": "visa"}})
        )
    );

    let durable = clinic.kept(4, true).await;
    let recorded: Vec<(&Value, &Value, &Value)> = durable
        .iter()
        .map(|record| (&record["action"], &record["table"], &record["request_id"]))
        .collect();
    assert_eq!(
        recorded,
        [
            (&json!("create"), &json!("patients"), &json!("req-create")),
            (&json!("read"), &json!("patients"), &json!("req-query")),
            (&json!("read"), &json!("patients"), &json!("req-stream")),
            (&json!("custom:echo"), &Value::Null, &json!("req-call")),
        ]
    );
    assert_eq!(
        durable[1]["data"],
        json!({
            "record_ids": [patient], "returned": 1, "truncated": false,
            "query": {"filter_columns": ["name"], "search": false, "sort": null},
        })
    );
    assert_eq!(
        durable[2]["data"],
        json!({
            "record_ids": [], "returned": 0, "truncated": false,
            "query": {"filter_columns": [], "search": false, "sort": null},
        }),
        "a stream is recorded when it is opened"
    );
    assert_eq!(durable[3]["data"], legacy[1]["data"]);
    assert_eq!(durable[3]["redacted"], json!(["card.number"]));
    assert_eq!(durable[3]["actor"]["kind"], "api_key");
}

#[tokio::test]
async fn where_every_tenants_records_are_durable_a_tenant_without_an_outbox_has_its_own_kept() {
    let Some(mut clinic) = Clinic::start(&[("CUFFLINK_AUDIT_DURABLE_ALL", "true")]).await else {
        return;
    };
    let created = clinic
        .ok(
            Method::POST,
            "/patients",
            "req-create",
            Some(json!({"name": "Ada", "dob": "1990-01-01"})),
        )
        .await;
    let patient = created["id"].as_str().expect("an id").to_string();
    let path = format!("/patients/{patient}");
    assert_eq!(
        clinic.ok(Method::GET, &path, "req-get", None).await,
        created
    );
    let echoed = clinic
        .ok(Method::POST, "/_fn/echo", "req-call", Some(json!({"n": 1})))
        .await;
    assert_eq!(echoed["body"], json!({"n": 1}), "{echoed}");

    let legacy = heard(&mut clinic.legacy, 2).await;
    let told: Vec<&Value> = legacy.iter().map(|record| &record["action"]).collect();
    assert_eq!(told, [&json!("create"), &json!("custom:echo")]);
    assert_eq!(legacy[0]["data"]["dob"], REDACTED);

    let mut durable = clinic.kept(3, false).await;
    durable.sort_by_key(|record| record["request_id"].as_str().map(str::to_string));
    let recorded: Vec<(&Value, &Value)> = durable
        .iter()
        .map(|record| (&record["request_id"], &record["action"]))
        .collect();
    assert_eq!(
        recorded,
        [
            (&json!("req-call"), &json!("custom:echo")),
            (&json!("req-create"), &json!("create")),
            (&json!("req-get"), &json!("read")),
        ]
    );
    assert_eq!(durable[1]["data"]["dob"], REDACTED);
    assert_eq!(
        durable[1]["redacted"],
        json!(["bank_bic", "bank_iban", "dob"]),
        "a column is redacted whatever it holds"
    );

    let database = common::legacy::tenant_database(&clinic._db, &clinic.tenant).await;
    let has_schema: bool = database
        .query_one("SELECT to_regnamespace('_cufflink') IS NOT NULL", &[])
        .await
        .expect("regnamespace")
        .get(0);
    assert!(!has_schema, "a tenant that enforces no outbox has none");
}
