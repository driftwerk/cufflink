use cufflink_testkit as common;

use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::Router;
use common::keycloak::MockKeycloak;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::repo::tenants::NewTenant;
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;
use tokio::sync::mpsc;

const GOLDEN: &str = include_str!("fixtures/crud_events/v0_27_0.json");
const WEBHOOK_SECRET: &str = "whsec_crud_events";
const QUIET: Duration = Duration::from_secs(3);
const SETTLED: Duration = Duration::from_millis(500);
const PATIENCE: Duration = Duration::from_secs(30);

static ONE_PLATFORM: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct Shapes {
    uuids: Vec<String>,
    names: Vec<(String, &'static str)>,
}

impl Shapes {
    fn named(&mut self, value: &str, name: &'static str) {
        self.names.push((value.to_string(), name));
    }

    fn of(&mut self, text: &str) -> String {
        let uuid =
            regex::Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                .expect("pattern");
        let time =
            regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?").expect("pattern");
        let hex = regex::Regex::new("[0-9a-f]{64}").expect("pattern");
        let named = self
            .names
            .iter()
            .fold(text.to_string(), |text, (value, name)| {
                text.replace(value, name)
            });
        let numbered = uuid.replace_all(&named, |found: &regex::Captures| {
            let id = found[0].to_string();
            let index = self.uuids.iter().position(|seen| *seen == id);
            let index = index.unwrap_or_else(|| {
                self.uuids.push(id);
                self.uuids.len() - 1
            });
            format!("{{uuid:{}}}", index + 1)
        });
        let timed = time.replace_all(&numbered, "{time}");
        hex.replace_all(&timed, "{hex}").into_owned()
    }
}

struct Delivery {
    headers: HeaderMap,
    body: Bytes,
}

async fn receiver() -> (String, mpsc::UnboundedReceiver<Delivery>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let app = Router::new().route(
        "/hook",
        post(move |headers: HeaderMap, body: Bytes| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(Delivery { headers, body });
                "ok"
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

fn column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "column_type": column_type, "primary_key": false, "nullable": true, "auto_generate": false})
}

fn id_column() -> Value {
    json!({"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true})
}

fn created_at() -> Value {
    json!({"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true})
}

fn shop() -> Value {
    json!({
        "name": "shop",
        "version": "0.1.0",
        "mode": "crud",
        "authorization": {
            "areas": [{"name": "accounts", "operations": ["view", "create", "edit", "delete"]}]
        },
        "tables": [
            {
                "name": "orders",
                "columns": [
                    id_column(),
                    column("title", "text"),
                    column("qty", "integer"),
                    column("units", "biginteger"),
                    column("total", "double"),
                    column("paid", "boolean"),
                    column("due", "date"),
                    column("tags", "jsonb"),
                    created_at()
                ]
            },
            {
                "name": "notes",
                "columns": [id_column(), column("body", "text"), created_at()],
                "soft_delete": true
            },
            {
                "name": "accounts",
                "columns": [
                    id_column(),
                    column("user_id", "text"),
                    column("org_no", "integer"),
                    column("title", "text"),
                    created_at()
                ],
                "auth_required": true,
                "owner_field": "user_id",
                "scoped_by": "accounts"
            },
            {
                "name": "tickets",
                "columns": [id_column(), column("title", "text"), column("body", "text"), created_at()],
                "hooks": {
                    "before_create": "stamp_create", "after_create": "seen",
                    "before_update": "stamp_update", "after_update": "seen",
                    "before_delete": "allow", "after_delete": "seen"
                }
            },
            {
                "name": "vaults",
                "columns": [id_column(), column("title", "text"), created_at()],
                "hooks": {"before_delete": "refuse"}
            },
            {
                "name": "seen",
                "columns": [id_column(), column("envelope", "text"), created_at()]
            }
        ],
        "publishes": [],
    })
}

const ANSWERS: [(&str, &str); 4] = [
    ("allow", r#"{"ok":true}"#),
    (
        "stamp_create",
        r#"{"data":{"title":"created by the hook"}}"#,
    ),
    (
        "stamp_update",
        r#"{"data":{"title":"updated by the hook"}}"#,
    ),
    ("refuse", r#"{"error":"the hook keeps the row"}"#),
];
const KEEP: [&str; 2] = ["INSERT INTO seen (envelope) VALUES ('", "')"];
const APART: usize = 128;

fn hooks() -> Vec<u8> {
    let texts: Vec<&str> = ANSWERS
        .iter()
        .map(|(_, answer)| *answer)
        .chain(KEEP)
        .collect();
    let data: String = texts
        .iter()
        .enumerate()
        .map(|(index, text)| {
            format!(
                "(data (i32.const {}) \"{}\")\n",
                index * APART,
                text.replace('"', "\\\"")
            )
        })
        .collect();
    let placed = |index: usize| {
        format!(
            "(i32.const {}) (i32.const {})",
            index * APART,
            texts[index].len()
        )
    };
    let answering: String = ANSWERS
        .iter()
        .enumerate()
        .map(|(index, (name, _))| {
            format!(
                "(func (export \"{name}\") (param i32 i32) (result i32) (call $respond {}))\n",
                placed(index)
            )
        })
        .collect();
    let (allow, head, tail) = (placed(0), placed(ANSWERS.len()), placed(ANSWERS.len() + 1));
    let module = format!(
        r#"(module
  (import "env" "db_execute" (func $db_execute (param i32 i32) (result i32)))
  (memory (export "memory") 2)
  (global $heap (mut i32) (i32.const 4096))
  {data}
  (func $alloc (export "alloc") (param $n i32) (result i32)
    (local $p i32) (local $end i32) (local $pages i32)
    (local.set $p (global.get $heap))
    (local.set $end (i32.add (local.get $p) (local.get $n)))
    (local.set $pages (i32.shr_u (i32.add (local.get $end) (i32.const 65535)) (i32.const 16)))
    (if (i32.gt_u (local.get $pages) (memory.size))
      (then (drop (memory.grow (i32.sub (local.get $pages) (memory.size))))))
    (global.set $heap (local.get $end))
    (local.get $p))
  (func $respond (param $src i32) (param $len i32) (result i32)
    (local $out i32)
    (local.set $out (call $alloc (i32.add (local.get $len) (i32.const 4))))
    (i32.store (local.get $out) (local.get $len))
    (memory.copy (i32.add (local.get $out) (i32.const 4)) (local.get $src) (local.get $len))
    (local.get $out))
  (func $append (param $to i32) (param $src i32) (param $len i32) (result i32)
    (memory.copy (local.get $to) (local.get $src) (local.get $len))
    (i32.add (local.get $to) (local.get $len)))
  {answering}
  (func (export "seen") (param $ptr i32) (param $len i32) (result i32)
    (local $sql i32) (local $end i32)
    (local.set $sql (call $alloc (i32.add (local.get $len) (i32.const {APART}))))
    (local.set $end (call $append (local.get $sql) {head}))
    (local.set $end (call $append (local.get $end) (local.get $ptr) (local.get $len)))
    (local.set $end (call $append (local.get $end) {tail}))
    (drop (call $db_execute (local.get $sql) (i32.sub (local.get $end) (local.get $sql))))
    (call $respond {allow}))
)"#
    );
    wat::parse_str(module).expect("the hooks compile")
}

struct Scenario {
    keycloak: MockKeycloak,
    platform: PlatformProcess,
    tenant: Tenant,
    realm: String,
    admin: String,
    heard: async_nats::Subscriber,
    deliveries: mpsc::UnboundedReceiver<Delivery>,
    shapes: Shapes,
    expected: VecDeque<(usize, usize)>,
    _db: TestDb,
    _alone: tokio::sync::MutexGuard<'static, ()>,
}

impl Scenario {
    async fn start() -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let alone = ONE_PLATFORM.lock().await;
        let keycloak = MockKeycloak::start().await;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(
            &db.url,
            Some(nats_url),
            &[
                ("KEYCLOAK_URL", keycloak.url.as_str()),
                ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
            ],
        )
        .await;
        platform.wait_ready().await;
        let realm = format!("realm-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let tenant = db
            .mint_tenant_with(NewTenant {
                keycloak_url: Some(&keycloak.url),
                keycloak_realm: Some(&realm),
                ..Default::default()
            })
            .await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let heard = async_nats::connect(nats_url)
            .await
            .expect("nats")
            .subscribe(format!("dw.{}.>", tenant.slug))
            .await
            .expect("subscribe");
        let (status, deployed) = platform.deploy_module(&tenant, &shop(), &hooks()).await;
        assert_eq!(status, 200, "deploy failed: {deployed}");
        let (url, deliveries) = receiver().await;
        let mut shapes = Shapes::default();
        shapes.named(&tenant.slug, "{tenant}");
        let scenario = Self {
            keycloak,
            platform,
            tenant,
            realm,
            admin,
            heard,
            deliveries,
            shapes,
            expected: VecDeque::new(),
            _db: db,
            _alone: alone,
        };
        for event in ["create", "update", "delete"] {
            let (status, webhook) = scenario
                .platform
                .send(
                    Method::POST,
                    &format!(
                        "/api/services/{}/webhooks",
                        deployed["service_id"].as_str().expect("service id")
                    ),
                    &scenario.key(),
                    Some(&json!({"table_name": "orders", "event": event, "url": url, "secret": WEBHOOK_SECRET})),
                )
                .await;
            assert_eq!(status, 200, "{webhook}");
        }
        for (subject, permissions) in [
            ("gina", "accounts:*@all"),
            ("ina", "accounts:*@field(org_no=org.no)"),
            ("owen", "accounts:*@own"),
        ] {
            scenario.grant(subject, permissions).await;
        }
        Some(scenario)
    }

    fn key(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    fn bearer(&self, claims: Value) -> String {
        format!("Bearer {}", self.keycloak.token(&self.realm, claims))
    }

    async fn minted_for(&self, sub: &str) -> String {
        let token = self.keycloak.token(
            &self.realm,
            json!({"sub": sub, "preferred_username": sub,
                   "realm_access": {"roles": ["cufflink-deploy"]}}),
        );
        let response = self
            .platform
            .http
            .post(format!("{}/api/auth/device/token", self.platform.base_url))
            .json(&json!({"access_token": token, "tenant_slug": self.tenant.slug}))
            .send()
            .await
            .expect("the device login");
        let status = response.status().as_u16();
        let minted: Value = response.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{minted}");
        format!(
            "ApiKey {}",
            minted["api_key"].as_str().expect("a minted key")
        )
    }

    async fn grant(&self, subject: &str, permission: &str) {
        let roles = format!("/api/tenants/{}/roles", self.tenant.id);
        let (status, role) = self
            .platform
            .send(
                Method::POST,
                &roles,
                &self.key(),
                Some(&json!({"name": format!("role-{subject}"), "permissions": [permission]})),
            )
            .await;
        assert_eq!(status, 200, "{role}");
        let (status, body) = self
            .platform
            .send(
                Method::POST,
                &format!("{roles}/assign"),
                &self.key(),
                Some(&json!({"user_id": subject, "role_id": role["id"]})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn set_outbox(&self, mode: &str) {
        let (status, body) = self
            .platform
            .send(
                Method::PUT,
                &format!("/api/tenants/{}/policies", self.tenant.slug),
                &self.admin,
                Some(&json!({"policies": {"outbox": {"mode": mode}}})),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        authorization: Option<&str>,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let url = format!(
            "{}/svc/{}/shop{path}",
            self.platform.base_url, self.tenant.slug
        );
        let request = self.platform.http.request(method, url);
        let request = match authorization {
            Some(authorization) => request.header("authorization", authorization),
            None => request,
        };
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };
        let response = request.send().await.expect("request");
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }

    async fn step(
        &mut self,
        name: &str,
        method: Method,
        path: &str,
        authorization: Option<&str>,
        body: Option<Value>,
    ) -> (Value, Value) {
        let (status, answer) = self.call(method, path, authorization, body.as_ref()).await;
        let expected = self.expected.pop_front();
        let started = tokio::time::Instant::now();
        let mut messages = Vec::new();
        let mut deliveries = Vec::new();
        loop {
            let quiet = match expected {
                None => QUIET,
                Some(counts) if counts <= (messages.len(), deliveries.len()) => SETTLED,
                Some(_) => PATIENCE.saturating_sub(started.elapsed()),
            };
            tokio::select! {
                message = self.heard.next() => messages.push(message.expect("a message")),
                delivery = self.deliveries.recv() => deliveries.push(delivery.expect("a delivery")),
                _ = tokio::time::sleep(quiet) => break,
            }
        }
        messages.sort_by(|a, b| {
            (a.subject.as_str(), a.payload.as_ref()).cmp(&(b.subject.as_str(), b.payload.as_ref()))
        });
        deliveries.sort_by(|a, b| a.body.cmp(&b.body));
        let shaped = self.shapes.of(&answer.to_string());
        let messages: Vec<Value> = messages
            .iter()
            .map(|message| {
                let mut headers: Vec<String> = message
                    .headers
                    .iter()
                    .flat_map(|headers| headers.iter())
                    .map(|(name, _)| name.to_string())
                    .collect();
                headers.sort();
                json!({
                    "subject": self.shapes.of(message.subject.as_str()),
                    "headers": headers,
                    "payload": self.shapes.of(&String::from_utf8_lossy(&message.payload)),
                })
            })
            .collect();
        let deliveries: Vec<Value> = deliveries
            .iter()
            .map(|delivery| {
                assert_eq!(
                    delivery
                        .headers
                        .get("x-cufflink-signature")
                        .and_then(|value| value.to_str().ok()),
                    Some(signature(&delivery.body).as_str()),
                    "{name}: the body is signed with the webhook's secret"
                );
                let headers: BTreeMap<String, String> = delivery
                    .headers
                    .iter()
                    .filter(|(name, _)| name.as_str() != "host")
                    .map(|(name, value)| {
                        let value = value.to_str().unwrap_or_default();
                        let kept = match name.as_str() {
                            "content-length" => "{n}".to_string(),
                            "x-cufflink-signature-256" => regex::Regex::new(r"t=\d+")
                                .expect("pattern")
                                .replace(&self.shapes.of(value), "t={n}")
                                .into_owned(),
                            _ => self.shapes.of(value),
                        };
                        (name.to_string(), kept)
                    })
                    .collect();
                json!({
                    "headers": headers,
                    "body": self.shapes.of(&String::from_utf8_lossy(&delivery.body)),
                })
            })
            .collect();
        (
            json!({
                "step": name,
                "status": status,
                "answer": shaped,
                "messages": messages,
                "deliveries": deliveries,
            }),
            answer,
        )
    }
}

fn signature(body: &[u8]) -> String {
    use ring::hmac;
    let key = hmac::Key::new(hmac::HMAC_SHA256, WEBHOOK_SECRET.as_bytes());
    format!("sha256={}", hex::encode(hmac::sign(&key, body)))
}

fn id_of(row: &Value) -> String {
    row["id"].as_str().expect("an id").to_string()
}

async fn single_writes(s: &mut Scenario) -> Vec<Value> {
    let clerk = s.bearer(json!({"sub": "clerk-1", "name": "Clara Clerk"}));
    let (gina, ina, owen) = (
        s.bearer(json!({"sub": "gina", "name": "Gina Global"})),
        s.bearer(json!({"sub": "ina", "org": {"no": 5}})),
        s.bearer(json!({"sub": "owen"})),
    );
    let key = s.key();
    let mut steps = Vec::new();
    let mut run = |step: (Value, Value)| {
        steps.push(step.0);
        step.1
    };

    let order = json!({
        "title": "Tea, \"loose\" — 2 kg", "qty": 3, "units": 9007199254740993_i64,
        "total": 12.5, "paid": false, "due": "2026-10-01",
        "tags": {"gift": true, "lines": [1, 2.5, null, "x"]}
    });
    run(s
        .step(
            "create by nobody",
            Method::POST,
            "/orders",
            None,
            Some(json!({"title": "anonymous"})),
        )
        .await);
    let keyed = run(s
        .step(
            "create by an api key",
            Method::POST,
            "/orders",
            Some(&key),
            Some(json!({"title": "keyed", "qty": 1})),
        )
        .await);
    let capitals = id_of(&keyed).to_uppercase();
    s.shapes.named(&capitals, "{the id in capitals}");
    run(s
        .step(
            "update by an id in capitals",
            Method::PUT,
            &format!("/orders/{capitals}"),
            Some(&key),
            Some(json!({"qty": 2})),
        )
        .await);
    let created = run(s
        .step(
            "create by a user",
            Method::POST,
            "/orders",
            Some(&clerk),
            Some(order),
        )
        .await);
    let order_path = format!("/orders/{}", id_of(&created));
    run(s
        .step(
            "update by a user",
            Method::PUT,
            &order_path,
            Some(&clerk),
            Some(json!({"paid": true, "total": 13.0})),
        )
        .await);
    run(s
        .step(
            "delete by a user",
            Method::DELETE,
            &order_path,
            Some(&clerk),
            None,
        )
        .await);
    run(s
        .step(
            "update of nothing",
            Method::PUT,
            &order_path,
            Some(&clerk),
            Some(json!({"paid": false})),
        )
        .await);
    run(s
        .step(
            "delete of nothing",
            Method::DELETE,
            &order_path,
            Some(&clerk),
            None,
        )
        .await);
    run(s
        .step(
            "create refused",
            Method::POST,
            "/orders",
            Some(&clerk),
            Some(json!(["not", "an", "object"])),
        )
        .await);

    let note = run(s
        .step(
            "create of a row that is soft-deleted",
            Method::POST,
            "/notes",
            Some(&key),
            Some(json!({"body": "kept"})),
        )
        .await);
    run(s
        .step(
            "soft delete",
            Method::DELETE,
            &format!("/notes/{}", id_of(&note)),
            Some(&key),
            None,
        )
        .await);

    let everywhere = run(s
        .step(
            "create in the scope all",
            Method::POST,
            "/accounts",
            Some(&gina),
            Some(json!({"title": "global", "org_no": 7})),
        )
        .await);
    let fielded = run(s
        .step(
            "create in a field scope",
            Method::POST,
            "/accounts",
            Some(&ina),
            Some(json!({"title": "of org 5"})),
        )
        .await);
    run(s
        .step(
            "update in a field scope",
            Method::PUT,
            &format!("/accounts/{}", id_of(&fielded)),
            Some(&ina),
            Some(json!({"title": "still of org 5"})),
        )
        .await);
    let owned = run(s
        .step(
            "create in the scope own",
            Method::POST,
            "/accounts",
            Some(&owen),
            Some(json!({"title": "mine"})),
        )
        .await);
    run(s
        .step(
            "delete in the scope own",
            Method::DELETE,
            &format!("/accounts/{}", id_of(&owned)),
            Some(&owen),
            None,
        )
        .await);
    run(s
        .step(
            "delete in the scope all",
            Method::DELETE,
            &format!("/accounts/{}", id_of(&everywhere)),
            Some(&gina),
            None,
        )
        .await);

    let ticket = run(s
        .step(
            "create through a hook that changes it",
            Method::POST,
            "/tickets",
            Some(&key),
            Some(json!({"title": "as it was sent", "body": "sent"})),
        )
        .await);
    let ticket_path = format!("/tickets/{}", id_of(&ticket));
    run(s
        .step(
            "update through a hook that changes it",
            Method::PUT,
            &ticket_path,
            None,
            Some(json!({"body": "sent again"})),
        )
        .await);
    run(s
        .step(
            "delete through a hook that lets it",
            Method::DELETE,
            &ticket_path,
            Some(&key),
            None,
        )
        .await);
    let vault = run(s
        .step(
            "create of a row a hook keeps",
            Method::POST,
            "/vaults",
            Some(&key),
            Some(json!({"title": "kept"})),
        )
        .await);
    run(s
        .step(
            "delete refused by a hook",
            Method::DELETE,
            &format!("/vaults/{}", id_of(&vault)),
            Some(&key),
            None,
        )
        .await);
    run(s
        .step(
            "what the hooks were given once their writes were made",
            Method::GET,
            "/seen?sort=created_at",
            Some(&key),
            None,
        )
        .await);
    let minted = s.minted_for("devi").await;
    run(s
        .step(
            "create by an api key minted for a user",
            Method::POST,
            "/orders",
            Some(&minted),
            Some(json!({"title": "minted", "qty": 4})),
        )
        .await);
    steps
}

async fn batch(s: &mut Scenario) -> Value {
    s.shapes = Shapes::default();
    s.shapes.named(&s.tenant.slug.clone(), "{tenant}");
    let clerk = s.bearer(json!({"sub": "clerk-1", "name": "Clara Clerk"}));
    let (_, kept) = s
        .step(
            "create before the batch",
            Method::POST,
            "/orders",
            Some(&clerk),
            Some(json!({"title": "to update", "qty": 1})),
        )
        .await;
    let (_, doomed) = s
        .step(
            "create before the batch",
            Method::POST,
            "/orders",
            Some(&clerk),
            Some(json!({"title": "to delete", "qty": 2})),
        )
        .await;
    s.step(
        "batch",
        Method::POST,
        "/orders/batch",
        Some(&clerk),
        Some(json!({"operations": [
            {"action": "create", "data": {"title": "first", "qty": 10}},
            {"action": "create", "data": {"title": "second", "qty": 20}},
            {"action": "update", "id": id_of(&kept), "data": {"qty": 5}},
            {"action": "create", "data": {"title": "third", "qty": 30}},
            {"action": "delete", "id": id_of(&doomed)},
        ]})),
    )
    .await
    .0
}

fn golden() -> Value {
    serde_json::from_str(GOLDEN).expect("the golden file")
}

fn steps_of(recorded: &Value) -> Vec<Value> {
    recorded["single_writes"]
        .as_array()
        .cloned()
        .expect("steps")
}

fn counts_of(steps: &[Value]) -> VecDeque<(usize, usize)> {
    let count = |step: &Value, of: &str| step[of].as_array().map_or(0, Vec::len);
    steps
        .iter()
        .map(|step| (count(step, "messages"), count(step, "deliveries")))
        .collect()
}

fn through_the_outbox(step: &Value) -> Value {
    let mut step = step.clone();
    for message in step["messages"].as_array_mut().into_iter().flatten() {
        let headers = message["headers"].as_array_mut().expect("headers");
        headers.push(json!("Nats-Msg-Id"));
        headers.sort_by_key(|name| name.as_str().map(str::to_string));
    }
    step
}

fn assert_steps(recorded: &[Value], expected: &[Value], mode: &str) {
    assert_eq!(recorded.len(), expected.len(), "{mode}");
    for (recorded, expected) in recorded.iter().zip(expected) {
        assert_eq!(
            recorded,
            expected,
            "{mode}, {}:\n{}\nwhere v0.27.0 had\n{}",
            expected["step"],
            serde_json::to_string_pretty(recorded).unwrap_or_default(),
            serde_json::to_string_pretty(expected).unwrap_or_default(),
        );
    }
}

#[tokio::test]
async fn with_the_outbox_off_the_data_routes_publish_what_v0_27_0_published() {
    let Some(mut s) = Scenario::start().await else {
        return;
    };
    let recording = std::env::var("CUFFLINK_GOLDEN_WRITE").ok();
    if recording.is_none() {
        s.expected = counts_of(&steps_of(&golden()));
    }
    let single_writes = single_writes(&mut s).await;
    if let Some(file) = recording {
        let batch = batch(&mut s).await;
        let recorded: Map<String, Value> = [
            ("single_writes".to_string(), Value::from(single_writes)),
            ("batch".to_string(), batch),
        ]
        .into_iter()
        .collect();
        let text = serde_json::to_string_pretty(&recorded).expect("json");
        std::fs::write(&file, format!("{text}\n")).expect("the golden file is written");
        return;
    }
    assert_steps(&single_writes, &steps_of(&golden()), "outbox: off");
}

#[tokio::test]
async fn with_the_outbox_enforced_the_data_routes_publish_what_v0_27_0_published() {
    if std::env::var_os("CUFFLINK_GOLDEN_WRITE").is_some() {
        return;
    }
    let Some(mut s) = Scenario::start().await else {
        return;
    };
    s.set_outbox("enforce").await;
    let expected: Vec<Value> = steps_of(&golden()).iter().map(through_the_outbox).collect();
    s.expected = counts_of(&expected);
    assert_steps(&single_writes(&mut s).await, &expected, "outbox: enforce");
}

#[tokio::test]
async fn a_batch_deletes_no_row_that_is_deleted() {
    if std::env::var_os("CUFFLINK_GOLDEN_WRITE").is_some() {
        return;
    }
    for mode in ["off", "enforce"] {
        let Some(mut s) = Scenario::start().await else {
            return;
        };
        s.set_outbox(mode).await;
        s.expected = [(2, 0), (2, 0), (0, 0)].into();
        let key = s.key();
        let (_, note) = s
            .step(
                "create",
                Method::POST,
                "/notes",
                Some(&key),
                Some(json!({"body": "kept"})),
            )
            .await;
        let (deleted, _) = s
            .step(
                "soft delete",
                Method::DELETE,
                &format!("/notes/{}", id_of(&note)),
                Some(&key),
                None,
            )
            .await;
        assert_eq!(
            deleted["messages"].as_array().map(Vec::len),
            Some(2),
            "{mode}"
        );
        let (again, answer) = s
            .step(
                "batch",
                Method::POST,
                "/notes/batch",
                Some(&key),
                Some(json!({"operations": [{"action": "delete", "id": id_of(&note)}]})),
            )
            .await;
        assert_eq!(again["status"], 200, "{mode}: {again}");
        assert_eq!(
            answer["summary"],
            json!({"created": 0, "updated": 0, "deleted": 1, "total": 1}),
            "{mode}: answered as a delete that finds no row always was"
        );
        assert_eq!(
            again["messages"],
            json!([]),
            "{mode}: a row is deleted, and published as deleted, once"
        );
    }
}

fn messages_of(batch: &Value) -> Vec<(String, Value)> {
    batch["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|message| {
            (
                message["subject"].as_str().unwrap_or_default().to_string(),
                serde_json::from_str(message["payload"].as_str().unwrap_or_default())
                    .expect("a payload is JSON"),
            )
        })
        .collect()
}

#[tokio::test]
async fn batch_emits_events_and_audit() {
    if std::env::var_os("CUFFLINK_GOLDEN_WRITE").is_some() {
        return;
    }
    let before = golden()["batch"].clone();
    assert_eq!(before["status"], 200);
    assert_eq!(
        before["messages"],
        json!([]),
        "v0.27.0 published nothing for a batch"
    );
    for mode in ["off", "enforce"] {
        let Some(mut s) = Scenario::start().await else {
            return;
        };
        s.set_outbox(mode).await;
        s.expected = [(2, 1), (2, 1), (10, 5)].into();
        let batch = batch(&mut s).await;
        assert_eq!(batch["status"], 200, "{mode}: {batch}");
        assert_eq!(batch["answer"], before["answer"], "{mode}");
        let published = messages_of(&batch);
        let subjects: Vec<&str> = published
            .iter()
            .map(|(subject, _)| subject.as_str())
            .collect();
        assert_eq!(
            subjects,
            [
                "dw.{tenant}.logs",
                "dw.{tenant}.logs",
                "dw.{tenant}.logs",
                "dw.{tenant}.logs",
                "dw.{tenant}.logs",
                "dw.{tenant}.shop.orders.created",
                "dw.{tenant}.shop.orders.created",
                "dw.{tenant}.shop.orders.created",
                "dw.{tenant}.shop.orders.deleted",
                "dw.{tenant}.shop.orders.updated",
            ],
            "{mode}"
        );
        let mut audited: Vec<(String, String, bool, bool)> = published
            .iter()
            .filter(|(subject, _)| subject.ends_with(".logs"))
            .map(|(_, audit)| {
                assert_eq!(audit["user_id"], "clerk-1", "{mode}");
                assert_eq!(audit["user_name"], "Clara Clerk", "{mode}");
                assert_eq!(audit["service"], "shop", "{mode}");
                assert_eq!(audit["table"], "orders", "{mode}");
                (
                    audit["action"].as_str().unwrap_or_default().to_string(),
                    audit["data"]["title"]
                        .as_str()
                        .or(audit["previous"]["title"].as_str())
                        .unwrap_or_default()
                        .to_string(),
                    audit["record_id"] == audit["data"]["id"]
                        || audit["record_id"] == audit["previous"]["id"],
                    audit["previous"].is_null(),
                )
            })
            .collect();
        audited.sort();
        let row = |action: &str, title: &str, no_previous: bool| {
            (action.to_string(), title.to_string(), true, no_previous)
        };
        assert_eq!(
            audited,
            [
                row("create", "first", true),
                row("create", "second", true),
                row("create", "third", true),
                row("delete", "to delete", false),
                row("update", "to update", true),
            ],
            "{mode}"
        );
        let mut events: Vec<(String, String, i64)> = published
            .iter()
            .filter(|(subject, _)| !subject.ends_with(".logs"))
            .map(|(_, event)| {
                assert_eq!(event["service"], "shop", "{mode}");
                assert_eq!(event["table"], "orders", "{mode}");
                assert_eq!(event["tenant_id"], s.shapes.of(&s.tenant.id.to_string()));
                (
                    event["action"].as_str().unwrap_or_default().to_string(),
                    event["data"]["title"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                    event["data"]["qty"].as_i64().unwrap_or_default(),
                )
            })
            .collect();
        events.sort();
        let event = |action: &str, title: &str, qty| (action.to_string(), title.to_string(), qty);
        assert_eq!(
            events,
            [
                event("created", "first", 10),
                event("created", "second", 20),
                event("created", "third", 30),
                event("deleted", "to delete", 2),
                event("updated", "to update", 5),
            ],
            "{mode}"
        );
        let delivered: Vec<&str> = batch["deliveries"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|delivery| delivery["headers"]["x-cufflink-event"].as_str())
            .collect();
        assert_eq!(delivered.len(), 5, "{mode}: {batch}");
    }
}
