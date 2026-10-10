use cufflink_testkit as common;

use common::keycloak::MockKeycloak;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_native::{CallContext, ExtError, NatsAuth, NatsProvider, Result as ExtResult};
use cufflink_platform::repo::tenants::NewTenant;
use cufflink_test_guests::Profile;
use cufflink_types::ext::{call_subject, PROTOCOL_VERSION, QUEUE_GROUP};
use futures_util::StreamExt;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;

const CALLER: &str = "caller-svc";
const MAX_BYTES: usize = 64 * 1024;

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm("ext-call", Profile::Debug).expect("built guest")),
    }
}

fn manifest(service: &str, extensions: &[&str]) -> Value {
    let mut manifest = common::wasm_manifest(service, &[("call", "/call")]);
    manifest["custom_routes"]
        .as_array_mut()
        .expect("routes")
        .push(json!({
            "method": "POST",
            "path": "/call_as_user",
            "handler": "call_as_user",
            "posture": {"kind": "authenticated"},
        }));
    manifest["extensions"] = json!(extensions);
    manifest
}

fn echo(ctx: &CallContext, asked: Value) -> ExtResult<Value> {
    if let Some(ms) = asked["sleep_ms"].as_u64() {
        std::thread::sleep(Duration::from_millis(ms));
    }
    match (&asked["fail"], asked["reply_bytes"].as_u64()) {
        (Value::Object(failure), _) => Err(ExtError::new(
            failure["code"].as_str().unwrap_or_default(),
            failure["message"].as_str().unwrap_or_default(),
        )),
        (_, Some(bytes)) => Ok(json!("x".repeat(usize::try_from(bytes).unwrap_or(0)))),
        _ => Ok(json!({
            "v": PROTOCOL_VERSION,
            "extension": "test.echo",
            "context": ctx,
            "payload": asked,
        })),
    }
}

struct Provider {
    stop: oneshot::Sender<()>,
    served: tokio::task::JoinHandle<eyre::Result<()>>,
}

impl Provider {
    async fn start(nats_url: &str, tenant: &str) -> Self {
        let provider =
            NatsProvider::new(tenant, nats_url, NatsAuth::Anonymous).expect("a provider");
        let server = cufflink_native::service("ext-test")
            .extension("test.echo", echo, ["*"])
            .bind_nats(provider)
            .await
            .expect("subscribed");
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(server.serve(async {
            let _ = stopped.await;
        }));
        Self { stop, served }
    }

    async fn stop(self) {
        self.stop.send(()).expect("still serving");
        self.served.await.expect("provider task").expect("served");
    }
}

async fn answer_nonsense_once(nats_url: &str, tenant: &str) -> tokio::task::JoinHandle<()> {
    let client = async_nats::connect(nats_url)
        .await
        .expect("responder connects");
    let mut calls = client
        .queue_subscribe(call_subject(tenant, "test.echo"), QUEUE_GROUP.into())
        .await
        .expect("subscribe");
    client
        .flush()
        .await
        .expect("the subscription reaches the server");
    tokio::spawn(async move {
        let message = calls.next().await.expect("a call");
        client
            .publish(
                message.reply.expect("a reply subject"),
                "not a reply".into(),
            )
            .await
            .expect("reply");
        calls.unsubscribe().await.expect("unsubscribe");
        client.flush().await.expect("the server takes it all");
    })
}

struct World {
    platform: PlatformProcess,
    keycloak: MockKeycloak,
    realm: String,
    tenant: Tenant,
    admin: String,
    db: TestDb,
    nats_url: &'static str,
}

impl World {
    async fn start(extra: &[(&str, &str)]) -> Option<Self> {
        let infra = common::infra().await?;
        let nats_url = infra.require_nats()?;
        let keycloak = MockKeycloak::start().await;
        let mut db = infra.create_database().await;
        let env: Vec<(&str, &str)> = [
            ("KEYCLOAK_URL", keycloak.url.as_str()),
            ("KEYCLOAK_ISSUER", keycloak.issuer_base.as_str()),
        ]
        .into_iter()
        .chain(extra.iter().copied())
        .collect();
        let platform = PlatformProcess::start(&db.url, Some(nats_url), &env).await;
        platform.wait_ready().await;
        let realm = format!("ext-{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let tenant = db
            .mint_tenant_with(NewTenant {
                keycloak_url: Some(&keycloak.url),
                keycloak_realm: Some(&realm),
                ..Default::default()
            })
            .await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        Some(Self {
            platform,
            keycloak,
            realm,
            tenant,
            admin,
            db,
            nats_url,
        })
    }

    fn extensions(&self) -> String {
        format!("/api/tenants/{}/extensions", self.tenant.slug)
    }

    async fn list(&self) -> Value {
        let (status, body) = self
            .platform
            .send(Method::GET, &self.extensions(), &self.admin, None)
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn register(&self, name: &str, callers: &[&str], force: bool) -> (u16, Value) {
        self.platform
            .send(
                Method::PUT,
                &format!("{}/{name}?force={force}", self.extensions()),
                &self.admin,
                Some(&json!({ "callers": callers })),
            )
            .await
    }

    async fn unregister(&self, name: &str, force: bool) -> (u16, Value) {
        self.platform
            .send(
                Method::DELETE,
                &format!("{}/{name}?force={force}", self.extensions()),
                &self.admin,
                None,
            )
            .await
    }

    async fn deploy(&self, manifest: &Value, wasm: &[u8]) -> (u16, Value) {
        self.platform
            .deploy_module(&self.tenant, manifest, wasm)
            .await
    }

    fn user(&self) -> String {
        format!(
            "Bearer {}",
            self.keycloak.token(&self.realm, json!({"sub": "staff-1"}))
        )
    }

    async fn call_with(&self, handler: &str, authorization: Option<&str>, call: Value) -> Value {
        self.call_on(&self.platform, handler, authorization, call)
            .await
    }

    async fn call_on(
        &self,
        platform: &PlatformProcess,
        handler: &str,
        authorization: Option<&str>,
        call: Value,
    ) -> Value {
        let request = platform
            .http
            .post(platform.fn_url(&self.tenant, CALLER, handler))
            .json(&call);
        let request = match authorization {
            Some(authorization) => request.header("authorization", authorization),
            None => request,
        };
        let response = request.send().await.expect("call");
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn call(&self, extension: &str, payload: Value) -> Value {
        self.call_with(
            "call",
            None,
            json!({"extension": extension, "payload": payload}),
        )
        .await
    }

    async fn calls(&self, extension: &str, result: &str) -> Option<f64> {
        self.platform
            .metric(
                "cufflink_ext_calls_total",
                &[
                    ("caller", CALLER),
                    ("extension", extension),
                    ("result", result),
                ],
            )
            .await
    }

    async fn audited(&self) -> Vec<(String, String, Value)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT action, resource_id, details FROM audit_log
                 WHERE tenant_id = $1 AND action LIKE 'extension.%' ORDER BY created_at",
                &[&self.tenant.id],
            )
            .await
            .expect("audit log")
            .iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect()
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("millis")
}

#[tokio::test]
async fn a_guest_calls_what_its_tenant_registers_for_it() {
    let Some(wasm) = guest() else {
        return;
    };
    let max_bytes = MAX_BYTES.to_string();
    let Some(w) = World::start(&[
        ("CUFFLINK_EXTENSIONS", "true"),
        ("EXT_CALL_MAX_BYTES", max_bytes.as_str()),
    ])
    .await
    else {
        return;
    };
    let declared = ["test.echo", "test.idle", "test.gone"];
    assert_eq!(
        w.list().await,
        json!({"tenant": w.tenant.slug, "extensions": []})
    );

    let (status, refused) = w.deploy(&manifest(CALLER, &declared), &wasm).await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        "extensions: 'test.echo' is not registered for this tenant"
    );
    let (status, set) = w.register("test.echo", &["other-svc"], false).await;
    assert_eq!(status, 200, "{set}");
    let (status, refused) = w.deploy(&manifest(CALLER, &declared), &wasm).await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        "extensions: the registration of 'test.echo' does not list 'caller-svc' among its callers"
    );

    for (name, body, status) in [
        ("Test.Echo", json!({"callers": ["caller-svc"]}), 422),
        ("test.echo", json!({"callers": []}), 422),
        ("test.echo", json!({"callers": ["a b"]}), 422),
        ("test.echo", json!({"caller": ["caller-svc"]}), 422),
    ] {
        let (answered, body) = w
            .platform
            .send(
                Method::PUT,
                &format!("{}/{name}", w.extensions()),
                &w.admin,
                Some(&body),
            )
            .await;
        assert_eq!(answered, status, "{name}: {body}");
    }
    let (status, _) = w
        .platform
        .send(
            Method::PUT,
            "/api/tenants/no-such-tenant/extensions/test.echo",
            &w.admin,
            Some(&json!({"callers": ["*"]})),
        )
        .await;
    assert_eq!(status, 404);
    let (status, _) = w
        .platform
        .send(
            Method::PUT,
            &format!("{}/test.echo", w.extensions()),
            &format!("ApiKey {}", w.tenant.api_key),
            Some(&json!({"callers": ["*"]})),
        )
        .await;
    assert_eq!(status, 401, "a tenant key administers no extensions");

    let (status, set) = w
        .register("test.echo", &[CALLER, "other-svc", CALLER], false)
        .await;
    assert_eq!(status, 200, "{set}");
    assert_eq!(
        set["extension"]["callers"],
        json!(["caller-svc", "other-svc"])
    );
    assert_eq!(set["extension"]["backend"], "nats");
    for (name, callers) in [("test.idle", ["*"]), ("test.gone", [CALLER])] {
        let (status, set) = w.register(name, &callers, false).await;
        assert_eq!(status, 200, "{name}: {set}");
    }
    let (status, deployed) = w.deploy(&manifest(CALLER, &declared), &wasm).await;
    assert_eq!(status, 200, "{deployed}");
    let service_id = deployed["service_id"]
        .as_str()
        .expect("service id")
        .to_string();

    let provider = Provider::start(w.nats_url, &w.tenant.slug).await;

    let before = now_ms();
    let echoed = w.call("test.echo", json!({"n": 1})).await;
    assert_eq!(echoed["ok"]["payload"], json!({"n": 1}), "{echoed}");
    assert_eq!(echoed["ok"]["v"], 1);
    assert_eq!(echoed["ok"]["extension"], "test.echo");
    let context = &echoed["ok"]["context"];
    assert_eq!(context["tenant"], json!(w.tenant.slug));
    assert_eq!(
        context["service"],
        json!({"name": CALLER, "version": deployed["version"]})
    );
    assert!(context.get("auth").is_none(), "{context}");
    let deadline = context["deadline_ms"].as_u64().expect("deadline");
    assert!(
        deadline > before && deadline <= now_ms() + 30_000,
        "{deadline}"
    );

    let user = w.user();
    let as_user = w
        .call_with(
            "call_as_user",
            Some(&user),
            json!({"extension": "test.echo", "payload": {"n": 2}}),
        )
        .await;
    let context = &as_user["ok"]["context"];
    assert_eq!(context["auth"]["sub"], "staff-1", "{as_user}");
    let token = user.trim_start_matches("Bearer ");
    assert!(
        !as_user.to_string().contains(token),
        "the caller's token never reaches the extension"
    );

    assert_eq!(
        w.call(
            "test.echo",
            json!({"fail": {"code": "quota", "message": "daily quota exhausted"}})
        )
        .await,
        json!({"error": "failed", "code": "quota", "message": "daily quota exhausted"})
    );
    provider.stop().await;
    let nonsense = answer_nonsense_once(w.nats_url, &w.tenant.slug).await;
    assert_eq!(
        w.call("test.echo", json!({"nonsense": true})).await["error"],
        "unavailable"
    );
    nonsense.await.expect("the responder answered and left");
    let _provider = Provider::start(w.nats_url, &w.tenant.slug).await;
    for (extension, error) in [
        ("test.other", "not_declared"),
        ("Not An Extension", "not_declared"),
        ("test.idle", "unavailable"),
    ] {
        assert_eq!(
            w.call(extension, json!(null)).await["error"],
            error,
            "{extension}"
        );
    }
    let timed_out = w
        .call_with(
            "call",
            None,
            json!({"extension": "test.echo", "payload": {"sleep_ms": 3000}, "timeout_ms": 300}),
        )
        .await;
    assert_eq!(timed_out["error"], "timeout", "{timed_out}");
    assert_eq!(
        w.call("test.echo", json!("x".repeat(MAX_BYTES))).await["error"],
        "too_large"
    );
    assert_eq!(
        w.call("test.echo", json!({"reply_bytes": MAX_BYTES + 1}))
            .await["error"],
        "too_large"
    );
    let nats_max = async_nats::connect(w.nats_url)
        .await
        .expect("nats")
        .server_info()
        .max_payload;
    let beyond_nats = w
        .call("test.echo", json!({"reply_bytes": nats_max + 1}))
        .await;
    assert_eq!(
        (&beyond_nats["error"], beyond_nats.get("code")),
        (&json!("too_large"), None),
        "a reply NATS cannot carry is too large, as its provider answers: {beyond_nats}"
    );

    let (status, refused) = w.register("test.echo", &["other-svc"], false).await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!(
        refused["error"],
        "service 'caller-svc' is deployed calling extension 'test.echo'; redeploy it without the \
         extension first, or pass force=true"
    );
    let (status, set) = w.register("test.echo", &["other-svc"], true).await;
    assert_eq!(status, 200, "{set}");
    assert_eq!(w.call("test.echo", json!(null)).await["error"], "denied");
    let (status, set) = w.register("test.echo", &["*"], false).await;
    assert_eq!(status, 200, "{set}");
    assert_eq!(
        w.call("test.echo", json!({"n": 3})).await["ok"]["payload"],
        json!({"n": 3})
    );

    let (status, refused) = w.unregister("test.gone", false).await;
    assert_eq!(status, 409, "{refused}");
    let (status, removed) = w.unregister("test.gone", true).await;
    assert_eq!(status, 200, "{removed}");
    assert_eq!(
        removed,
        json!({"tenant": w.tenant.slug, "name": "test.gone", "removed": true})
    );
    assert_eq!(w.call("test.gone", json!(null)).await["error"], "not_found");
    let (status, _) = w.unregister("test.gone", true).await;
    assert_eq!(status, 404);

    w.db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO tenant_extensions (tenant_id, name, backend, service_id, callers, created_by)
             VALUES ($1, 'test.native', 'native', $2::text::uuid, '{*}', 'deploy')",
            &[&w.tenant.id, &service_id],
        )
        .await
        .expect("a native service's extension");
    for (status, refused) in [
        w.register("test.native", &["*"], true).await,
        w.unregister("test.native", true).await,
    ] {
        assert_eq!(status, 409, "{refused}");
    }
    let listed = w.list().await;
    let listed: Vec<(&str, &str, &Value, &Value)> = listed["extensions"]
        .as_array()
        .expect("extensions")
        .iter()
        .map(|e| {
            (
                e["name"].as_str().unwrap_or_default(),
                e["backend"].as_str().unwrap_or_default(),
                &e["callers"],
                &e["service_id"],
            )
        })
        .collect();
    assert_eq!(
        listed,
        [
            ("test.echo", "nats", &json!(["*"]), &Value::Null),
            ("test.idle", "nats", &json!(["*"]), &Value::Null),
            ("test.native", "native", &json!(["*"]), &json!(service_id)),
        ]
    );

    for (extension, result, count) in [
        ("test.echo", "ok", 3.0),
        ("test.echo", "failed", 1.0),
        ("test.echo", "unavailable", 1.0),
        ("test.echo", "timeout", 1.0),
        ("test.echo", "too_large", 3.0),
        ("test.echo", "denied", 1.0),
        ("test.gone", "not_found", 1.0),
        ("test.idle", "unavailable", 1.0),
        ("-", "not_declared", 2.0),
    ] {
        assert_eq!(
            w.calls(extension, result).await,
            Some(count),
            "{extension} {result}"
        );
    }

    let audited = w.audited().await;
    let actions: Vec<(&str, &str, bool)> = audited
        .iter()
        .map(|(action, name, details)| {
            (
                action.as_str(),
                name.as_str(),
                details["force"].as_bool().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        actions,
        [
            ("extension.register", "test.echo", false),
            ("extension.register", "test.echo", false),
            ("extension.register", "test.idle", false),
            ("extension.register", "test.gone", false),
            ("extension.register", "test.echo", true),
            ("extension.register", "test.echo", false),
            ("extension.unregister", "test.gone", true),
        ]
    );
}

#[tokio::test]
async fn a_rollback_calls_only_what_is_registered_for_it() {
    let Some(wasm) = guest() else {
        return;
    };
    let Some(w) = World::start(&[("CUFFLINK_EXTENSIONS", "true")]).await else {
        return;
    };
    let (status, set) = w.register("test.echo", &[CALLER], false).await;
    assert_eq!(status, 200, "{set}");
    let (status, first) = w.deploy(&manifest(CALLER, &["test.echo"]), &wasm).await;
    assert_eq!(status, 200, "{first}");
    let (status, second) = w.deploy(&manifest(CALLER, &[]), &wasm).await;
    assert_eq!(status, 200, "{second}");
    let (status, removed) = w.unregister("test.echo", false).await;
    assert_eq!(
        status, 200,
        "no deployed version calls it any more: {removed}"
    );

    let rollback = format!(
        "/api/services/{}/rollback",
        second["service_id"].as_str().expect("service id")
    );
    let deployer = format!("ApiKey {}", w.tenant.api_key);
    let to_first = json!({"target_version": first["version"]});
    let (status, refused) = w
        .platform
        .send(Method::POST, &rollback, &deployer, Some(&to_first))
        .await;
    assert_eq!(status, 422, "{refused}");
    assert_eq!(
        refused["error"],
        "extensions: 'test.echo' is not registered for this tenant"
    );
    let (status, set) = w.register("test.echo", &[CALLER], false).await;
    assert_eq!(status, 200, "{set}");
    let (status, rolled_back) = w
        .platform
        .send(Method::POST, &rollback, &deployer, Some(&to_first))
        .await;
    assert_eq!(status, 200, "{rolled_back}");
}

#[tokio::test]
async fn extensions_are_registered_only_where_the_platform_allows_it() {
    let Some(w) = World::start(&[]).await else {
        return;
    };
    let (status, refused) = w.register("test.echo", &["*"], false).await;
    assert_eq!(status, 403, "{refused}");
    assert_eq!(
        refused["error"],
        "extensions cannot be registered on this platform (CUFFLINK_EXTENSIONS=false)"
    );
    assert_eq!(
        w.list().await,
        json!({"tenant": w.tenant.slug, "extensions": []})
    );
    w.db.pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "INSERT INTO tenant_extensions (tenant_id, name, backend, callers, created_by)
             VALUES ($1, 'test.echo', 'nats', '{*}', 'admin')",
            &[&w.tenant.id],
        )
        .await
        .expect("a registration made while extensions were allowed");
    let (status, removed) = w.unregister("test.echo", false).await;
    assert_eq!(
        status, 200,
        "a registration is removed either way: {removed}"
    );
    assert_eq!(
        w.list().await,
        json!({"tenant": w.tenant.slug, "extensions": []})
    );
}

#[tokio::test]
async fn calls_are_answered_where_extensions_are_not_registered() {
    let Some(wasm) = guest() else {
        return;
    };
    let storage_dir = tempfile::tempdir().expect("storage directory");
    let dir = storage_dir.path().to_string_lossy().to_string();
    let storage = [
        ("STORAGE_BACKEND", "local"),
        ("STORAGE_LOCAL_DIR", dir.as_str()),
    ];
    let Some(w) =
        World::start(&[storage.as_slice(), &[("CUFFLINK_EXTENSIONS", "true")]].concat()).await
    else {
        return;
    };
    let (status, set) = w.register("test.echo", &[CALLER], false).await;
    assert_eq!(status, 200, "{set}");
    let (status, deployed) = w.deploy(&manifest(CALLER, &["test.echo"]), &wasm).await;
    assert_eq!(status, 200, "{deployed}");
    let _provider = Provider::start(w.nats_url, &w.tenant.slug).await;
    let disabled = PlatformProcess::start(
        &w.db.url,
        Some(w.nats_url),
        &[storage.as_slice(), &[("CUFFLINK_EXTENSIONS", "false")]].concat(),
    )
    .await;
    disabled.wait_ready().await;
    let echoed = w
        .call_on(
            &disabled,
            "call",
            None,
            json!({"extension": "test.echo", "payload": {"n": 1}}),
        )
        .await;
    assert_eq!(echoed["ok"]["payload"], json!({"n": 1}), "{echoed}");
    let (status, listed) = disabled
        .send(Method::GET, &w.extensions(), &w.admin, None)
        .await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(listed["extensions"][0]["name"], "test.echo");
}
