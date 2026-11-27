use cufflink_testkit as common;

use async_nats::jetstream::{self, consumer, stream};
use common::{until, until_loops, PlatformProcess, Tenant, TestDb};
use futures_util::TryStreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

const FAST: &[(&str, &str)] = &[("CONSUMER_RECONCILE_INTERVAL_SECS", "1")];
const WITHIN: Duration = Duration::from_secs(30);

struct Claimed {
    platform: PlatformProcess,
    db: TestDb,
    admin: String,
    harbor: Tenant,
    js: jetstream::Context,
    stem: String,
    subjects: String,
}

impl Claimed {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, Some(nats_url), FAST).await;
        platform.wait_ready().await;
        let harbor = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let id = Uuid::new_v4().simple().to_string()[..10].to_uppercase();
        Some(Self {
            platform,
            db,
            admin,
            harbor,
            js: jetstream::new(async_nats::connect(nats_url).await.expect("nats")),
            subjects: format!("catalog{}", id.to_lowercase()),
            stem: format!("CATALOG{id}_"),
        })
    }

    fn stream(&self, suffix: &str) -> String {
        format!("{}{suffix}", self.stem)
    }

    fn prefix(&self) -> String {
        format!("{}*", self.stem)
    }

    fn subject(&self, rest: &str) -> String {
        format!("{}.{rest}", self.subjects)
    }

    /// Harbor claims the suite's subjects; another tenant claims none, so
    /// what it is refused is its streams alone.
    async fn put(&self, tenant: &Tenant, streams: &[String]) -> (u16, Value) {
        let subjects: Vec<String> = (tenant.slug == self.harbor.slug)
            .then(|| self.subject(">"))
            .into_iter()
            .collect();
        self.platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/nats-claims", tenant.slug),
                &self.admin,
                Some(&json!({"subjects": subjects, "streams": streams})),
            )
            .await
    }

    async fn claimed(&self) -> Value {
        let (status, body) = self
            .platform
            .send(
                Method::GET,
                &format!("/api/tenants/{}/nats-claims", self.harbor.slug),
                &self.admin,
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        body["streams"].clone()
    }

    fn declaring(&self, service: &str, stream: &str, topic: &str) -> Value {
        json!({
            "name": service,
            "version": "0.1.0",
            "mode": "crud",
            "tables": [{"name": "items", "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
                 "auto_generate": true},
            ]}],
            "streams": [{
                "name": stream,
                "subjects": [self.subject(&format!("{topic}.>"))],
                "retention": "limits",
            }],
            "publishes": [],
        })
    }

    fn subscriber(&self, service: &str, stream: &str, topic: &str) -> Value {
        json!({
            "name": service,
            "version": "0.1.0",
            "mode": "crud",
            "tables": [{"name": "loans", "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
                 "auto_generate": true},
                {"name": "item_id", "column_type": "uuid", "primary_key": false,
                 "nullable": false, "auto_generate": false},
            ]}],
            "subscriptions": [{
                "subject": self.subject(&format!("{topic}.deleted")),
                "handler": "drop_loans",
                "handler_type": "delete_cascade",
                "config": {"table": "loans", "foreign_key": "item_id"},
                "stream": stream,
            }],
            "publishes": [],
        })
    }

    async fn deploy(&self, manifest: &Value, inactive: bool) -> (u16, Value) {
        self.platform
            .send(
                Method::POST,
                &format!("/api/services/deploy?inactive={inactive}"),
                &format!("ApiKey {}", self.harbor.api_key),
                Some(&json!({"manifest": manifest})),
            )
            .await
    }

    async fn live(&self, name: &str, topic: &str) {
        self.js
            .create_stream(stream::Config {
                name: name.to_string(),
                subjects: vec![self.subject(&format!("{topic}.>"))],
                ..Default::default()
            })
            .await
            .expect("a stream made outside Cufflink");
    }

    async fn exists(&self, name: &str) -> bool {
        self.js.get_stream(name).await.is_ok()
    }

    async fn durables(&self, name: &str) -> Vec<consumer::Info> {
        match self.js.get_stream(name).await {
            Ok(stream) => stream
                .consumers()
                .try_collect()
                .await
                .expect("the stream's consumers"),
            Err(_) => Vec::new(),
        }
    }

    /// Publishes a deletion on the stream and waits for a durable on it to
    /// acknowledge it.
    async fn delivered(&self, name: &str, topic: &str) {
        let sequence = self
            .js
            .publish(
                self.subject(&format!("{topic}.deleted")),
                r#"{"data":{}}"#.into(),
            )
            .await
            .expect("publish")
            .await
            .expect("stored")
            .sequence;
        let mut durables = Vec::new();
        until(
            &format!("a durable on '{name}' acknowledges what was published"),
            WITHIN,
            async || {
                durables = self.durables(name).await;
                durables
                    .iter()
                    .any(|durable| durable.ack_floor.stream_sequence >= sequence)
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{e}: {durables:?}"));
    }

    async fn reconciled(&self) {
        let runs = async || {
            self.platform
                .metric(
                    "cufflink_consumer_reconcile_runs_total",
                    &[("result", "ok")],
                )
                .await
                .unwrap_or_default()
        };
        let before = runs().await;
        until("two reconcile passes", WITHIN, async || {
            runs().await >= before + 2.0
        })
        .await
        .expect("the reconciler runs");
    }
}

fn versioned(manifest: &Value, version: &str) -> Value {
    let mut manifest = manifest.clone();
    manifest["version"] = json!(version);
    manifest
}

impl Drop for Claimed {
    fn drop(&mut self) {
        let (js, stem) = (self.js.clone(), self.stem.clone());
        let _ = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map(|runtime| {
                    runtime.block_on(async {
                        for suffix in ["EVENTS", "OLD", "LEGACY"] {
                            let _ = js.delete_stream(format!("{stem}{suffix}")).await;
                        }
                    })
                })
        })
        .join();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_prefix_admits_the_streams_the_tenants_services_declare() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(test.claimed().await, json!([test.prefix()]));

    let events = test.stream("EVENTS");
    let subscriber = test.subscriber("loan-service", &events, "items");
    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.to_string().contains(&format!(
            "stream '{events}' is claimed only through '{}' and no service of this tenant declares it yet",
            test.prefix()
        )),
        "a subscriber deployed before its stream's declarer: {body}"
    );

    let (status, body) = test
        .deploy(&test.declaring("catalog-service", &events, "items"), true)
        .await;
    assert_eq!(status, 200, "an inactive declarer: {body}");
    assert!(test.exists(&events).await, "the declared stream is created");
    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(status, 200, "an inactive declarer counts: {body}");

    let outside = format!("RECEIPTS{}", &test.stem["CATALOG".len()..]);
    let (status, body) = test
        .deploy(
            &test.declaring("receipt-service", &outside, "receipts"),
            false,
        )
        .await;
    assert_eq!(status, 422, "a stream outside the prefix: {body}");
    assert!(!test.exists(&outside).await);

    let (status, body) = test
        .put(&test.harbor, &[test.prefix(), format!("{}X*", test.stem)])
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body.to_string().contains(&format!(
            "'{}X*' is covered by '{}'",
            test.stem,
            test.prefix()
        )),
        "{body}"
    );
    assert_eq!(test.claimed().await, json!([test.prefix()]));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_prefix_is_refused_over_streams_that_are_not_the_tenants() {
    let Some(mut test) = Claimed::start().await else {
        return;
    };
    let orchard = test.db.mint_tenant().await;
    let events = test.stream("EVENTS");
    let (status, body) = test.put(&orchard, std::slice::from_ref(&events)).await;
    assert_eq!(status, 200, "{body}");
    test.live(&events, "items").await;
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(
        status, 409,
        "another tenant's live stream under the prefix: {body}"
    );
    assert!(
        body.to_string().contains(&format!(
            "'{}' overlaps '{events}', which tenant '{}' claims",
            test.prefix(),
            orchard.slug
        )),
        "an administrator is told whose stream: {body}"
    );
    let (status, body) = test.put(&orchard, &[]).await;
    assert_eq!(status, 200, "{body}");
    let _ = test.js.delete_stream(&events).await;

    let old = test.stream("OLD");
    test.live(&old, "old").await;
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 409, "a live stream no manifest declares: {body}");
    assert!(
        body.to_string().contains(&format!("'{old}'")),
        "the stream is named: {body}"
    );
    assert_eq!(
        test.claimed().await,
        json!([]),
        "a refused put changes nothing"
    );

    let (status, body) = test.put(&test.harbor, &[test.prefix(), old.clone()]).await;
    assert_eq!(status, 200, "adopted by name beside the prefix: {body}");
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 409, "a later put re-checks what is there: {body}");
    let _ = test.js.delete_stream(&old).await;

    let without_nats = PlatformProcess::start(&test.db.url, None, &[]).await;
    without_nats.wait_ready().await;
    test.platform = without_nats;
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 503, "JetStream unreachable: {body}");
    assert_eq!(
        test.claimed().await,
        json!([test.prefix(), old]),
        "nothing changes while the check cannot run"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_put_that_swaps_names_for_a_prefix_strands_what_no_manifest_declares() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (events, legacy) = (test.stream("EVENTS"), test.stream("LEGACY"));
    test.live(&legacy, "legacy").await;
    let (status, body) = test
        .put(&test.harbor, &[test.prefix(), legacy.clone()])
        .await;
    assert_eq!(
        status, 200,
        "a live stream adopted beside the prefix: {body}"
    );
    let (status, body) = test
        .deploy(&test.subscriber("loan-service", &legacy, "legacy"), false)
        .await;
    assert_eq!(
        status, 200,
        "a subscription binds the adopted stream: {body}"
    );

    let (status, body) = test
        .put(&test.harbor, &[events.clone(), legacy.clone()])
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .deploy(&test.declaring("catalog-service", &events, "items"), false)
        .await;
    assert_eq!(status, 200, "{body}");

    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body.to_string().contains("service 'loan-service'") && body.to_string().contains(&legacy),
        "the subscriber of the stream no manifest declares is stranded: {body}"
    );
    let (status, body) = test
        .put(&test.harbor, &[test.prefix(), legacy.clone()])
        .await;
    assert_eq!(status, 200, "keeping the name strands nothing: {body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_held_through_a_prefix_gets_its_durable_and_its_messages() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "{body}");
    let events = test.stream("EVENTS");
    let (status, body) = test
        .deploy(&test.declaring("catalog-service", &events, "items"), false)
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .deploy(&test.subscriber("loan-service", &events, "items"), false)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["consumers"][0]["op"], "create",
        "the deploy plans the durable: {body}"
    );
    assert_eq!(test.durables(&events).await.len(), 1);
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the subscriber's loop starts");
    test.delivered(&events, "items").await;
    test.reconciled().await;
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("a reconcile keeps the loop");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_keeps_its_durable_and_loop_when_its_stream_moves_from_a_name_to_a_prefix() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let events = test.stream("EVENTS");
    let (status, body) = test.put(&test.harbor, std::slice::from_ref(&events)).await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = test
        .deploy(&test.declaring("catalog-service", &events, "items"), false)
        .await;
    assert_eq!(status, 200, "{body}");
    let subscriber = test.subscriber("loan-service", &events, "items");
    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(status, 200, "{body}");
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the subscriber's loop starts");
    let created = test.durables(&events).await[0].created;

    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "the declared stream strands nothing: {body}");
    test.reconciled().await;
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("a reconcile keeps the loop");
    test.delivered(&events, "items").await;

    let (status, body) = test.deploy(&versioned(&subscriber, "0.1.1"), false).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["plan"]["consumers"][0]["op"], "keep",
        "a redeploy keeps the durable: {body}"
    );
    let durables = test.durables(&events).await;
    assert_eq!(
        durables
            .iter()
            .map(|durable| durable.created)
            .collect::<Vec<_>>(),
        [created]
    );
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the redeployed loop runs");
    test.delivered(&events, "items").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_made_under_a_prefix_is_bound_only_once_a_declarer_takes_it_over() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "{body}");
    let old = test.stream("OLD");
    test.live(&old, "old").await;
    let made = test
        .js
        .get_stream(&old)
        .await
        .expect("stream")
        .cached_info()
        .created;

    let subscriber = test.subscriber("loan-service", &old, "old");
    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(
        status, 422,
        "an out-of-band stream no service declares: {body}"
    );
    assert!(test.durables(&old).await.is_empty());

    let (status, body) = test
        .deploy(&test.declaring("catalog-service", &old, "old"), false)
        .await;
    assert_eq!(status, 200, "{body}");
    let op = &body["plan"]["streams"][0];
    assert_eq!(op["stream"], json!(old), "{body}");
    assert_ne!(
        op["op"], "create",
        "the live stream is planned over: {body}"
    );
    assert_eq!(
        test.js
            .get_stream(&old)
            .await
            .expect("stream")
            .cached_info()
            .created,
        made,
        "the declarer takes the stream over as it is"
    );

    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(status, 200, "{body}");
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the subscriber's loop starts");
    test.delivered(&old, "old").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_version_that_stops_declaring_leaves_its_prefix_subscriber_running_until_it_redeploys() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "{body}");
    let events = test.stream("EVENTS");
    let declarer = test.declaring("catalog-service", &events, "items");
    let (status, body) = test.deploy(&declarer, false).await;
    assert_eq!(status, 200, "{body}");
    let subscriber = test.subscriber("loan-service", &events, "items");
    let (status, body) = test.deploy(&subscriber, false).await;
    assert_eq!(status, 200, "{body}");
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the subscriber's loop starts");

    let mut undeclaring = versioned(&declarer, "0.1.1");
    undeclaring["streams"] = json!([]);
    let (status, body) = test.deploy(&undeclaring, false).await;
    assert_eq!(status, 200, "{body}");
    assert!(test.exists(&events).await, "the stream stays on the server");
    test.reconciled().await;
    until_loops(&[&test.platform], 1.0)
        .await
        .expect("the running subscriber keeps its loop");
    assert_eq!(test.durables(&events).await.len(), 1);
    test.delivered(&events, "items").await;

    let (status, body) = test.deploy(&versioned(&subscriber, "0.1.1"), false).await;
    assert_eq!(status, 422, "its next deploy: {body}");
    assert!(
        body.to_string().contains(&format!(
            "stream '{events}' is claimed only through '{}' and no service of this tenant declares it yet",
            test.prefix()
        )),
        "{body}"
    );
    assert_eq!(
        test.durables(&events).await.len(),
        1,
        "a refused deploy changes nothing"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_the_declarer_leaves_its_stream_to_be_adopted() {
    let Some(test) = Claimed::start().await else {
        return;
    };
    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 200, "{body}");
    let events = test.stream("EVENTS");
    let (status, deployed) = test
        .deploy(&test.declaring("catalog-service", &events, "items"), false)
        .await;
    assert_eq!(status, 200, "{deployed}");
    let (status, body) = test
        .platform
        .send(
            Method::DELETE,
            &format!(
                "/api/services/{}",
                deployed["service_id"].as_str().expect("service id")
            ),
            &format!("ApiKey {}", test.harbor.api_key),
            None,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert!(test.exists(&events).await);

    let (status, body) = test.put(&test.harbor, &[test.prefix()]).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body.to_string()
            .contains(&format!("'{events}' already exists")),
        "{body}"
    );
    let (status, body) = test
        .put(&test.harbor, &[test.prefix(), events.clone()])
        .await;
    assert_eq!(status, 200, "adopted by name: {body}");
}
