use cufflink_testkit as common;

use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use common::infra::metric_value;
use common::PlatformProcess;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const SECRET: &str = "whsec_deliveries";
static ONE_DELIVERER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const LOOKS: usize = 10;

fn orders() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "tables": [{"name": "orders", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false,
             "auto_generate": true},
            {"name": "n", "column_type": "integer", "primary_key": false, "nullable": false,
             "auto_generate": false},
        ]}],
        "publishes": [],
    })
}

struct Received {
    headers: HeaderMap,
    body: Bytes,
    connections_out: Option<f64>,
}

async fn connections_out(http: &reqwest::Client, platform: &str) -> Option<f64> {
    let mut fewest: Option<f64> = None;
    for _ in 0..LOOKS {
        let metrics = http
            .get(format!("{platform}/metrics"))
            .send()
            .await
            .ok()?
            .text()
            .await
            .ok()?;
        let of = |name: &str| metric_value(&metrics, name, &[("pool", "platform")]);
        let out = of("cufflink_db_pool_size")? - of("cufflink_db_pool_available")?;
        fewest = Some(fewest.map_or(out, |fewest| fewest.min(out)));
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    fewest
}

async fn receiver(platform: String, takes: usize) -> (String, mpsc::UnboundedReceiver<Received>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (attempts, http) = (Arc::new(AtomicUsize::new(0)), reqwest::Client::new());
    let app = Router::new().route(
        "/hook",
        post(move |headers: HeaderMap, body: Bytes| {
            let (tx, attempts, http, platform) =
                (tx.clone(), attempts.clone(), http.clone(), platform.clone());
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let connections_out = connections_out(&http, &platform).await;
                let _ = tx.send(Received {
                    headers,
                    body,
                    connections_out,
                });
                if attempt < takes {
                    (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "1")])
                } else {
                    (StatusCode::OK, [("retry-after", "0")])
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}/hook", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (url, rx)
}

fn hmac_hex(parts: &[&[u8]]) -> String {
    use ring::hmac;
    hex::encode(hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, SECRET.as_bytes()),
        &parts.concat(),
    ))
}

async fn eventually<F, Fut>(what: &str, within: Duration, platform: &PlatformProcess, mut done: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + within;
    while !done().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: not within {within:?}\n{}",
            platform.output_tail()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn webhook_delivery_retries_and_signs() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let sealed_under = {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode([7; 32])
    };
    let platform = PlatformProcess::start(
        &db.url,
        Some(nats_url),
        &[
            ("CUFFLINK_SECRETS_KEK", sealed_under.as_str()),
            ("CUFFLINK_SECRETS_KEK_ID", "a"),
            ("CUFFLINK_SECRET_STORAGE", "encrypted"),
        ],
    )
    .await;
    platform.wait_ready().await;
    let tenant = db.mint_tenant().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let key = format!("ApiKey {}", tenant.api_key);
    let (status, deployed) = platform.deploy(&tenant, &orders()).await;
    assert_eq!(status, 200, "{deployed}");
    let service = deployed["service_id"].as_str().expect("service id");
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", tenant.slug),
            &admin,
            Some(&json!({"policies": {"outbox": {"mode": "enforce"}}})),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let (url, mut received) = receiver(platform.base_url.clone(), 3).await;
    let webhooks = format!("/api/services/{service}/webhooks");
    let (status, webhook) = platform
        .send(
            Method::POST,
            &webhooks,
            &key,
            Some(&json!({"table_name": "orders", "event": "create", "url": url, "secret": SECRET})),
        )
        .await;
    assert_eq!(status, 200, "{webhook}");
    let webhook_id = webhook["id"].as_str().expect("webhook id");
    let stored: (Option<String>, bool) = {
        let row = db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT secret, secret_enc IS NOT NULL FROM webhooks WHERE id = $1::text::uuid",
                &[&webhook_id],
            )
            .await
            .expect("the webhook");
        (row.get(0), row.get(1))
    };
    assert_eq!(
        stored,
        (None, true),
        "the secret deliveries are signed with is stored sealed, and nowhere else"
    );
    let (status, other) = platform
        .send(
            Method::POST,
            &webhooks,
            &key,
            Some(&json!({"table_name": "orders", "event": "delete", "url": url, "secret": SECRET})),
        )
        .await;
    assert_eq!(status, 200, "{other}");

    let (status, order) = platform
        .send(
            Method::POST,
            &format!("/svc/{}/shop/orders", tenant.slug),
            &key,
            Some(&json!({"n": 7})),
        )
        .await;
    assert_eq!(status, 200, "{order}");
    let event = json!({
        "action": "created", "service": "shop", "table": "orders", "data": order,
        "tenant_id": tenant.id,
    })
    .to_string();

    let began = tokio::time::Instant::now();
    let mut attempts = Vec::new();
    while attempts.len() < 3 {
        let attempt = tokio::time::timeout(Duration::from_secs(30), received.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} of 3 attempts within 30 s\n{}",
                    attempts.len(),
                    platform.output_tail()
                )
            })
            .expect("the receiver");
        attempts.push(attempt);
    }
    assert!(
        began.elapsed() >= Duration::from_secs(2),
        "a receiver that asks to be left alone for a second is: {:?}",
        began.elapsed()
    );
    let header = |attempt: &Received, name: &str| {
        attempt
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    let delivery = header(&attempts[0], "x-cufflink-delivery");
    assert!(delivery.parse::<uuid::Uuid>().is_ok(), "{delivery}");
    for attempt in &attempts {
        assert_eq!(
            String::from_utf8_lossy(&attempt.body),
            event,
            "the event, byte for byte what is published"
        );
        assert_eq!(header(attempt, "x-cufflink-delivery"), delivery);
        assert_eq!(header(attempt, "x-cufflink-event"), "create");
        assert_eq!(header(attempt, "content-type"), "application/json");
        assert_eq!(
            header(attempt, "x-cufflink-signature"),
            format!("sha256={}", hmac_hex(&[&attempt.body]))
        );
        let timed = header(attempt, "x-cufflink-signature-256");
        let (at, signature) = timed
            .strip_prefix("t=")
            .and_then(|timed| timed.split_once(",v1="))
            .unwrap_or_else(|| panic!("{timed}"));
        assert_eq!(
            signature,
            hmac_hex(&[format!("{at}.").as_bytes(), &attempt.body])
        );
        let age = chrono::Utc::now().timestamp() - at.parse::<i64>().expect("seconds");
        assert!((0..60).contains(&age), "signed when it was sent: {age} s");
        assert_eq!(
            attempt.connections_out,
            Some(0.0),
            "no connection is held while the receiver is waited for"
        );
    }

    let deliveries = format!("{webhooks}/{webhook_id}/deliveries");
    eventually(
        "the delivery is recorded as delivered",
        Duration::from_secs(10),
        &platform,
        || async {
            let (_, listed) = platform.send(Method::GET, &deliveries, &key, None).await;
            listed["deliveries"][0]["status"] == "delivered"
        },
    )
    .await;
    let (status, listed) = platform.send(Method::GET, &deliveries, &key, None).await;
    assert_eq!(status, 200, "{listed}");
    let recorded = listed["deliveries"].as_array().expect("deliveries");
    assert_eq!(recorded.len(), 1, "{listed}");
    assert_eq!(
        (
            &recorded[0]["id"],
            &recorded[0]["event"],
            &recorded[0]["status"],
            &recorded[0]["attempts"],
            &recorded[0]["last_status"],
            &recorded[0]["last_error"],
        ),
        (
            &json!(delivery),
            &json!("create"),
            &json!("delivered"),
            &json!(3),
            &json!(200),
            &Value::Null
        ),
        "{listed}"
    );
    assert!(recorded[0]["delivered_at"].is_string(), "{listed}");

    let more = tokio::time::timeout(Duration::from_secs(2), received.recv()).await;
    assert!(
        more.is_err(),
        "delivered once, and not by the dispatcher as well"
    );
    for (result, count) in [("retry", 2.0), ("delivered", 1.0)] {
        assert_eq!(
            platform
                .metric("cufflink_webhook_deliveries_total", &[("result", result)])
                .await,
            Some(count),
            "{result}"
        );
    }

    let (status, refused) = platform
        .send(
            Method::GET,
            &format!("{webhooks}/{}/deliveries", uuid::Uuid::new_v4()),
            &key,
            None,
        )
        .await;
    assert_eq!(status, 404, "{refused}");
    let (status, refused) = platform
        .send(Method::GET, &format!("{deliveries}?limit=0"), &key, None)
        .await;
    assert_eq!(status, 422, "{refused}");

    let pool = db.pool();
    pool.get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE webhook_deliveries SET updated_at = now() - interval '31 days' WHERE id = $1::text::uuid",
            &[&delivery],
        )
        .await
        .expect("an old delivery");
    let month = Duration::from_secs(30 * 24 * 60 * 60);
    let swept = cufflink_platform::webhooks::retention::sweep(&pool, month).await;
    assert_eq!(
        swept.expect("swept"),
        1,
        "a delivery written to a month ago"
    );
    let (_, listed) = platform.send(Method::GET, &deliveries, &key, None).await;
    assert_eq!(listed["deliveries"], json!([]), "{listed}");

    let audited: Vec<(String, String)> = db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT action, actor FROM audit_log
             WHERE resource_type = 'webhook' AND tenant_id = $1 ORDER BY created_at, action",
            &[&tenant.id],
        )
        .await
        .expect("the audit log")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(audited.len(), 2, "{audited:?}");
    assert!(
        audited
            .iter()
            .all(|(action, actor)| action == "webhook.create" && !actor.is_empty()),
        "{audited:?}"
    );
}

async fn stalling_receiver(answers_its_head: bool) -> String {
    let app = Router::new().route(
        "/hook",
        post(move || async move {
            if !answers_its_head {
                std::future::pending::<()>().await;
            }
            let body = futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from("{"))])
                .chain(futures_util::stream::pending());
            axum::body::Body::from_stream(body)
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}/hook", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    url
}

struct Shop {
    tenant: common::Tenant,
    key: String,
    service: String,
    webhooks: Vec<String>,
}

impl Shop {
    async fn open(
        platform: &PlatformProcess,
        db: &mut common::TestDb,
        admin: &str,
        urls: &[&str],
    ) -> Self {
        let tenant = db.mint_tenant().await;
        let key = format!("ApiKey {}", tenant.api_key);
        let (status, deployed) = platform.deploy(&tenant, &orders()).await;
        assert_eq!(status, 200, "{deployed}");
        let service = deployed["service_id"]
            .as_str()
            .expect("service id")
            .to_string();
        let (status, body) = platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", tenant.slug),
                admin,
                Some(&json!({"policies": {"outbox": {"mode": "enforce"}}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let mut webhooks = Vec::new();
        for url in urls {
            let (status, webhook) = platform
                .send(
                    Method::POST,
                    &format!("/api/services/{service}/webhooks"),
                    &key,
                    Some(&json!({"table_name": "orders", "event": "create", "url": url, "secret": SECRET})),
                )
                .await;
            assert_eq!(status, 200, "{webhook}");
            webhooks.push(webhook["id"].as_str().expect("webhook id").to_string());
        }
        Self {
            tenant,
            key,
            service,
            webhooks,
        }
    }

    fn orders(&self) -> String {
        format!("/svc/{}/shop/orders", self.tenant.slug)
    }

    fn deliveries(&self, webhook: &str) -> String {
        format!(
            "/api/services/{}/webhooks/{webhook}/deliveries",
            self.service
        )
    }
}

#[tokio::test]
async fn a_receiver_that_stalls_holds_up_its_own_deliveries_alone() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (silent, headless) = (
        stalling_receiver(false).await,
        stalling_receiver(true).await,
    );
    let stalled = Shop::open(&platform, &mut db, &admin, &[&silent, &headless]).await;
    let (url, mut received) = receiver(platform.base_url.clone(), 1).await;
    let healthy = Shop::open(&platform, &mut db, &admin, &[&url]).await;

    for n in 0..12 {
        let (status, order) = platform
            .send(
                Method::POST,
                &stalled.orders(),
                &stalled.key,
                Some(&json!({ "n": n })),
            )
            .await;
        assert_eq!(status, 200, "{order}");
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (status, order) = platform
        .send(
            Method::POST,
            &healthy.orders(),
            &healthy.key,
            Some(&json!({"n": 1})),
        )
        .await;
    assert_eq!(status, 200, "{order}");
    let written = tokio::time::Instant::now();
    let delivered = tokio::time::timeout(Duration::from_secs(5), received.recv())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "another tenant's delivery waited for the stalled receivers\n{}",
                platform.output_tail()
            )
        })
        .expect("the receiver");
    assert!(
        String::from_utf8_lossy(&delivered.body).contains("\"n\":1"),
        "{:?}",
        delivered.body
    );
    assert!(
        written.elapsed() < Duration::from_secs(5),
        "{:?}",
        written.elapsed()
    );
    assert!(
        platform
            .metric(
                "cufflink_webhook_deliveries_total",
                &[("result", "deferred")]
            )
            .await
            .is_some_and(|deferred| deferred > 0.0),
        "the stalled tenant's deliveries waited for room"
    );

    let recorded = |webhook: &str| {
        let path = stalled.deliveries(webhook);
        let (platform, key) = (&platform, stalled.key.clone());
        async move {
            let (_, listed) = platform.send(Method::GET, &path, &key, None).await;
            listed["deliveries"].as_array().cloned().unwrap_or_default()
        }
    };
    eventually(
        "an attempt whose receiver stalls ends as the attempt's time does",
        Duration::from_secs(30),
        &platform,
        || async {
            let (silent, headless) = (
                recorded(&stalled.webhooks[0]).await,
                recorded(&stalled.webhooks[1]).await,
            );
            silent.iter().any(|delivery| {
                delivery["status"] == "pending"
                    && delivery["attempts"] == 1
                    && delivery["last_error"]
                        .as_str()
                        .is_some_and(|error| error.contains("timed out after 10s"))
            }) && headless
                .iter()
                .any(|delivery| delivery["status"] == "delivered")
        },
    )
    .await;
}

#[tokio::test]
async fn a_guest_forges_no_webhook_delivery() {
    use common::outbox::{dead_letters, forge, seal};
    use cufflink_platform::outbox::{OutboxKind, OutboxRow};

    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (url, mut received) = receiver(platform.base_url.clone(), 1).await;
    let shop = Shop::open(&platform, &mut db, &admin, &[&url]).await;
    let tenant = &shop.tenant;
    let subject = format!("cufflink.webhook.{}.{}", tenant.slug, shop.webhooks[0]);
    let forged = |n: u8| {
        OutboxRow::new(
            OutboxKind::Webhook,
            "shop",
            subject.clone(),
            json!({ "forged": n }).to_string().into_bytes(),
            Vec::new(),
        )
    };
    let nats = async_nats::connect(nats_url).await.expect("nats");
    forge(
        &common::outbox::sessions(&db, &nats),
        tenant,
        "shop",
        &[
            forged(1),
            OutboxRow {
                payload: json!({ "forged": 2 }).to_string().into_bytes(),
                ..seal().seal(tenant.id, forged(0))
            },
            seal().seal(uuid::Uuid::new_v4(), forged(3)),
        ],
    )
    .await;
    let (status, order) = platform
        .send(
            Method::POST,
            &shop.orders(),
            &shop.key,
            Some(&json!({"n": 7})),
        )
        .await;
    assert_eq!(status, 200, "{order}");

    let delivered = tokio::time::timeout(Duration::from_secs(30), received.recv())
        .await
        .unwrap_or_else(|_| panic!("the write's delivery\n{}", platform.output_tail()))
        .expect("the receiver");
    let body: Value = serde_json::from_slice(&delivered.body).expect("json");
    assert_eq!(body["data"], order, "{body}");
    let more = tokio::time::timeout(Duration::from_secs(2), received.recv()).await;
    assert!(more.is_err(), "nothing forged is signed and sent");
    let filed: Vec<(String, Value)> = dead_letters(&async_nats::jetstream::new(nats), tenant, 3)
        .await
        .into_iter()
        .map(|letter| (letter.subject, letter.payload["forged"].clone()))
        .collect();
    let unauthenticated = format!("cufflink.dlq.{}.shop.outbox.unauthenticated", tenant.slug);
    assert_eq!(
        filed,
        (1..=3)
            .map(|n| (unauthenticated.clone(), json!(n)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_outbox_rows_unauthenticated_total",
                &[("tenant", tenant.slug.as_str()), ("kind", "webhook")]
            )
            .await,
        Some(3.0)
    );
}

async fn answering(status: u16, location: Option<&str>) -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let (counted, location) = (hits.clone(), location.map(str::to_string));
    let app = Router::new().route(
        "/hook",
        post(move || {
            let (counted, location) = (counted.clone(), location.clone());
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                let mut headers = HeaderMap::new();
                if let Some(location) = location.and_then(|to| to.parse().ok()) {
                    headers.insert("location", location);
                }
                (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
                    headers,
                )
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let url = format!("http://{}/hook", listener.local_addr().expect("addr"));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (url, hits)
}

async fn recorded(platform: &PlatformProcess, shop: &Shop, webhook: &str) -> Vec<Value> {
    let (_, listed) = platform
        .send(Method::GET, &shop.deliveries(webhook), &shop.key, None)
        .await;
    listed["deliveries"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn a_delivery_is_made_to_its_tenants_active_webhook_and_given_up_when_it_is_refused() {
    use common::outbox::{dead_letters, forge, seal};
    use cufflink_platform::outbox::{OutboxKind, OutboxRow};

    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (gone, gone_hits) = answering(410, None).await;
    let (elsewhere, elsewhere_hits) = answering(200, None).await;
    let (moved, moved_hits) = answering(302, Some(&elsewhere)).await;
    let (off, off_hits) = answering(200, None).await;
    let (deleted, deleted_hits) = answering(200, None).await;
    let shop = Shop::open(&platform, &mut db, &admin, &[&gone, &moved, &off, &deleted]).await;
    let (other, other_hits) = answering(200, None).await;
    let other_shop = Shop::open(&platform, &mut db, &admin, &[&other]).await;
    let webhook = |index: usize| {
        format!(
            "/api/services/{}/webhooks/{}",
            shop.service, shop.webhooks[index]
        )
    };
    let (status, body) = platform
        .send(
            Method::PUT,
            &webhook(2),
            &shop.key,
            Some(&json!({"active": false})),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = platform
        .send(Method::DELETE, &webhook(3), &shop.key, None)
        .await;
    assert_eq!(status, 200, "{body}");

    let (status, order) = platform
        .send(
            Method::POST,
            &shop.orders(),
            &shop.key,
            Some(&json!({"n": 1})),
        )
        .await;
    assert_eq!(status, 200, "{order}");
    let row = |webhook: &str| {
        seal().seal(
            shop.tenant.id,
            OutboxRow::new(
                OutboxKind::Webhook,
                "shop",
                format!("cufflink.webhook.{}.{webhook}", shop.tenant.slug),
                json!({"n": 2}).to_string().into_bytes(),
                Vec::new(),
            ),
        )
    };
    let nats = async_nats::connect(nats_url).await.expect("nats");
    forge(
        &common::outbox::sessions(&db, &nats),
        &shop.tenant,
        "shop",
        &[
            row(&shop.webhooks[2]),
            row(&shop.webhooks[3]),
            row(&other_shop.webhooks[0]),
        ],
    )
    .await;

    eventually(
        "the refused deliveries are given up",
        Duration::from_secs(30),
        &platform,
        || async {
            let refused = |deliveries: Vec<Value>| {
                deliveries.len() == 1
                    && deliveries[0]["status"] == "failed"
                    && deliveries[0]["attempts"] == 1
            };
            refused(recorded(&platform, &shop, &shop.webhooks[0]).await)
                && refused(recorded(&platform, &shop, &shop.webhooks[1]).await)
                && recorded(&platform, &shop, &shop.webhooks[2]).await.len() == 1
        },
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        gone_hits.load(Ordering::SeqCst),
        1,
        "a 4xx is attempted once"
    );
    assert_eq!(
        moved_hits.load(Ordering::SeqCst),
        1,
        "a 3xx is attempted once"
    );
    assert_eq!(elsewhere_hits.load(Ordering::SeqCst), 0, "and not followed");
    for (hits, what) in [
        (&off_hits, "a webhook switched off"),
        (&deleted_hits, "a deleted webhook"),
        (&other_hits, "another tenant's webhook"),
    ] {
        assert_eq!(hits.load(Ordering::SeqCst), 0, "{what} is sent nothing");
    }
    let switched_off = recorded(&platform, &shop, &shop.webhooks[2]).await;
    assert!(
        switched_off
            .iter()
            .all(|delivery| delivery["status"] == "dropped" && delivery["attempts"] == 1),
        "{switched_off:?}"
    );
    assert!(recorded(&platform, &other_shop, &other_shop.webhooks[0])
        .await
        .is_empty());
    let js = async_nats::jetstream::new(nats);
    let mut filed: Vec<(String, String)> = dead_letters(&js, &shop.tenant, 3)
        .await
        .into_iter()
        .map(|letter| (letter.subject, letter.reason))
        .collect();
    filed.sort();
    let slug = &shop.tenant.slug;
    let letter = |webhook: &str| format!("cufflink.dlq.{slug}.shop.webhook.{webhook}");
    let mut expected = vec![
        letter(&shop.webhooks[0]),
        letter(&shop.webhooks[1]),
        format!(
            "cufflink.dlq.{slug}.shop.webhook.{}",
            other_shop.webhooks[0]
        ),
    ];
    expected.sort();
    assert_eq!(
        filed
            .iter()
            .map(|(subject, _)| subject.clone())
            .collect::<Vec<_>>(),
        expected
    );
    assert!(
        filed
            .iter()
            .any(|(_, reason)| reason.contains("is not of the tenant")),
        "{filed:?}"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_webhook_deliveries_total",
                &[("result", "dropped")]
            )
            .await,
        Some(2.0),
        "the switched off webhook's delivery, recorded, and the deleted one's, not"
    );
}

#[tokio::test]
async fn a_webhook_registered_on_one_replica_is_delivered_for_a_write_on_another() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let first = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    first.wait_ready().await;
    let second = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    second.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let (url, mut received) = receiver(first.base_url.clone(), 1).await;
    let shop = Shop::open(&first, &mut db, &admin, &[]).await;
    let (status, order) = second
        .send(
            Method::POST,
            &shop.orders(),
            &shop.key,
            Some(&json!({"n": 1})),
        )
        .await;
    assert_eq!(status, 200, "a write before any webhook: {order}");
    let (status, webhook) = first
        .send(
            Method::POST,
            &format!("/api/services/{}/webhooks", shop.service),
            &shop.key,
            Some(&json!({"table_name": "orders", "event": "create", "url": url, "secret": SECRET})),
        )
        .await;
    assert_eq!(status, 200, "{webhook}");
    let mut written = Vec::new();
    let mut delivered = None;
    for n in 2..20 {
        let (status, order) = second
            .send(
                Method::POST,
                &shop.orders(),
                &shop.key,
                Some(&json!({ "n": n })),
            )
            .await;
        assert_eq!(status, 200, "{order}");
        written.push(order);
        if let Ok(Some(delivery)) =
            tokio::time::timeout(Duration::from_secs(2), received.recv()).await
        {
            delivered = Some(delivery);
            break;
        }
    }
    let delivery = delivered.unwrap_or_else(|| {
        panic!(
            "the other replica learnt of the webhook\n{}",
            second.output_tail()
        )
    });
    let body: Value = serde_json::from_slice(&delivery.body).expect("json");
    assert!(
        written.contains(&body["data"]),
        "a write of the other replica: {body}"
    );
}

#[tokio::test]
async fn what_was_written_under_enforce_is_delivered_after_the_outbox_is_turned_off() {
    let Some(infra) = common::infra().await else {
        return;
    };
    let _alone = ONE_DELIVERER.lock().await;
    let Some(nats_url) = infra.require_nats() else {
        return;
    };
    let mut db = infra.create_database().await;
    let platform = PlatformProcess::start(&db.url, Some(nats_url), &[]).await;
    platform.wait_ready().await;
    let admin = format!("ApiKey {}", db.mint_admin_key().await);
    let address = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr")
    };
    let url = format!("http://{address}/hook");
    let shop = Shop::open(&platform, &mut db, &admin, &[&url]).await;
    let (status, order) = platform
        .send(
            Method::POST,
            &shop.orders(),
            &shop.key,
            Some(&json!({"n": 1})),
        )
        .await;
    assert_eq!(status, 200, "{order}");
    eventually(
        "the receiver that is not there is tried",
        Duration::from_secs(20),
        &platform,
        || async {
            recorded(&platform, &shop, &shop.webhooks[0])
                .await
                .first()
                .is_some_and(|delivery| delivery["status"] == "pending")
        },
    )
    .await;
    let (status, body) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{}/policies", shop.tenant.slug),
            &admin,
            Some(&json!({"policies": {"outbox": {"mode": "off"}}})),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let (tx, mut received) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/hook",
        post(move |body: Bytes| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(body);
                "ok"
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("the receiver's address again");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let body = tokio::time::timeout(Duration::from_secs(40), received.recv())
        .await
        .unwrap_or_else(|_| panic!("the delivery is made\n{}", platform.output_tail()))
        .expect("the receiver");
    let event: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(event["data"], order);
    eventually(
        "the delivery is recorded as delivered",
        Duration::from_secs(10),
        &platform,
        || async {
            recorded(&platform, &shop, &shop.webhooks[0])
                .await
                .first()
                .is_some_and(|delivery| {
                    delivery["status"] == "delivered" && delivery["attempts"] == 2
                })
        },
    )
    .await;
    let more = tokio::time::timeout(Duration::from_secs(2), received.recv()).await;
    assert!(more.is_err(), "delivered once");
}
