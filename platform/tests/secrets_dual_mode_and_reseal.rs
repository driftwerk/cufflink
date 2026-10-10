use cufflink_testkit as common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::repo::tenants::NewTenant;
use cufflink_platform::secrets::{
    KeyRing, SecretBox, StorageMode, Stored, SERVICE_CONFIG_VALUE, TENANT_OPS_SECRET,
    WEBHOOK_SECRET,
};
use cufflink_test_guests::Profile;
use reqwest::Method;
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

fn kek(byte: u8) -> String {
    STANDARD.encode([byte; 32])
}

fn in_test_box(db: &TestDb, byte: u8, id: &str) -> SecretBox {
    SecretBox::new(
        db.pool(),
        KeyRing::parse(Some(&kek(byte)), Some(id), None).expect("ring"),
        StorageMode::Dual,
    )
}

fn guest() -> Option<Vec<u8>> {
    match cufflink_test_guests::UNAVAILABLE {
        Some(reason) => {
            common::skip(reason);
            None
        }
        None => Some(cufflink_test_guests::wasm("config", Profile::Debug).expect("built guest")),
    }
}

struct Harness {
    db: TestDb,
    tenant: Tenant,
    admin: String,
    service_id: Uuid,
    wasm: Vec<u8>,
    deploys: std::cell::Cell<u32>,
}

async fn deploy(platform: &PlatformProcess, tenant: &Tenant, wasm: &[u8], n: u32) -> Uuid {
    let mut manifest = common::platform::wasm_manifest("cfg", &[("read", "/read")]);
    manifest["version"] = json!(format!("0.1.{n}"));
    let (status, body) = platform.deploy_module(tenant, &manifest, wasm).await;
    assert_eq!(status, 200, "deploy: {body}");
    body["service_id"]
        .as_str()
        .expect("id")
        .parse()
        .expect("uuid")
}

impl Harness {
    async fn start(env: &[(&str, &str)]) -> Option<(Self, PlatformProcess)> {
        let wasm = guest()?;
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), env).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let admin = format!("ApiKey {}", db.mint_admin_key().await);
        let service_id = deploy(&platform, &tenant, &wasm, 0).await;
        Some((
            Self {
                db,
                tenant,
                admin,
                service_id,
                wasm,
                deploys: std::cell::Cell::new(0),
            },
            platform,
        ))
    }

    async fn restart(&self, env: &[(&str, &str)]) -> PlatformProcess {
        let infra = common::infra().await.expect("infra");
        let platform = PlatformProcess::start(&self.db.url, infra.nats_url(), env).await;
        platform.wait_ready().await;
        self.deploys.set(self.deploys.get() + 1);
        assert_eq!(
            deploy(&platform, &self.tenant, &self.wasm, self.deploys.get()).await,
            self.service_id
        );
        platform
    }

    fn key(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    fn config_path(&self) -> String {
        format!("/api/services/{}/config", self.service_id)
    }

    async fn set(&self, platform: &PlatformProcess, key: &str, value: &str, is_secret: bool) {
        let (status, body) = platform
            .send(
                Method::PUT,
                &self.config_path(),
                &self.key(),
                Some(&json!({"key": key, "value": value, "is_secret": is_secret})),
            )
            .await;
        assert_eq!(status, 200, "set {key}: {body}");
    }

    async fn list(&self, platform: &PlatformProcess) -> (u16, Value) {
        platform
            .send(Method::GET, &self.config_path(), &self.key(), None)
            .await
    }

    async fn guest_reads(&self, platform: &PlatformProcess, key: &str) -> Value {
        let response = platform
            .http
            .post(platform.fn_url(&self.tenant, "cfg", "read"))
            .json(&json!({ "key": key }))
            .send()
            .await
            .expect("fn request");
        assert_eq!(response.status().as_u16(), 200);
        response.json::<Value>().await.expect("json")["value"].clone()
    }

    async fn reseal(&self, platform: &PlatformProcess, operation: &str) -> Value {
        let (status, body) = platform
            .send(
                Method::POST,
                "/api/admin/secrets/reseal",
                &self.admin,
                Some(&json!({ "operation": operation })),
            )
            .await;
        assert_eq!(status, 200, "reseal {operation}: {body}");
        body
    }

    async fn stored(&self, key: &str) -> Stored {
        let row = self
            .db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT value, value_enc FROM service_configs WHERE service_id = $1 AND key = $2",
                &[&self.service_id, &key],
            )
            .await
            .expect("row");
        Stored::new(row.get(0), row.get(1))
    }

    async fn sql(&self, sql: &str) {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .execute(sql, &[&self.service_id])
            .await
            .expect(sql);
    }

    async fn count(&self, sql: &str) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(sql, &[&self.tenant.id])
            .await
            .expect(sql)
            .get(0)
    }

    async fn opened(&self, secrets: &SecretBox, key: &str) -> Option<String> {
        let aad = SERVICE_CONFIG_VALUE.aad([self.service_id.to_string(), key.to_string()]);
        secrets
            .open(
                &self.db.pool().get().await.expect("checkout"),
                self.tenant.id,
                &aad,
                self.stored(key).await.sealed().expect("sealed"),
            )
            .await
            .ok()
            .map(|value| value.expose().to_string())
    }
}

fn kek_env<'a>(kek: &'a str, id: &'a str) -> [(&'a str, &'a str); 2] {
    [
        ("CUFFLINK_SECRETS_KEK", kek),
        ("CUFFLINK_SECRETS_KEK_ID", id),
    ]
}

#[tokio::test]
async fn secrets_dual_mode_and_reseal() {
    let (kek_a, kek_b) = (kek(1), kek(2));
    let Some((harness, platform)) = Harness::start(&kek_env(&kek_a, "a")).await else {
        return;
    };
    let reader = in_test_box(&harness.db, 1, "a");

    harness.set(&platform, "API_TOKEN", "tok-1", true).await;
    harness.set(&platform, "GREETING", "hello", false).await;
    let stored = harness.stored("API_TOKEN").await;
    assert_eq!(
        stored.plain(),
        Some("tok-1"),
        "dual storage keeps the plaintext"
    );
    assert_eq!(
        harness.opened(&reader, "API_TOKEN").await.as_deref(),
        Some("tok-1"),
        "and seals it under the tenant's data key"
    );
    let as_the_web_runner_reads = cufflink_platform::secrets::configs::all(
        &in_test_box(&harness.db, 1, "a"),
        &harness.db.pool().get().await.expect("checkout"),
        harness.service_id,
    )
    .await
    .expect("configs");
    assert_eq!(
        as_the_web_runner_reads
            .iter()
            .map(|(key, value)| (key.as_str(), value.expose()))
            .collect::<Vec<_>>(),
        [("API_TOKEN", "tok-1"), ("GREETING", "hello")],
        "the web runner opens what the platform sealed"
    );
    let (status, listing) = harness.list(&platform).await;
    assert_eq!(status, 200, "{listing}");
    let configs = listing["configs"].as_array().expect("configs");
    assert_eq!(configs[0]["key"], "API_TOKEN");
    assert_eq!(
        configs[0]["value"], "***",
        "a secret is masked as it always was"
    );
    assert_eq!(configs[1]["value"], "hello", "a value that is not is shown");
    assert_eq!(harness.guest_reads(&platform, "API_TOKEN").await, "tok-1");
    assert_eq!(
        harness.guest_reads(&platform, "MISSING").await,
        Value::Null,
        "a key the service has no value for"
    );

    harness
        .sql("INSERT INTO service_configs (service_id, key, value) VALUES ($1, 'LEGACY', 'old')")
        .await;
    assert_eq!(
        harness.guest_reads(&platform, "LEGACY").await,
        "old",
        "a row written before secrets were sealed reads as before"
    );
    harness
        .sql("UPDATE service_configs SET value = 'tok-2' WHERE service_id = $1 AND key = 'API_TOKEN'")
        .await;
    assert_eq!(
        harness.guest_reads(&platform, "API_TOKEN").await,
        "tok-2",
        "a plaintext a release that does not seal wrote after the sealing is the later write"
    );
    assert_eq!(
        platform
            .metric(
                "cufflink_secret_operations_total",
                &[("op", "open"), ("result", "stale")]
            )
            .await,
        Some(1.0)
    );

    let report = harness.reseal(&platform, "encrypt").await;
    assert_eq!(
        report["rows_rewritten"], 2,
        "the legacy row and the stale one: {report}"
    );
    assert_eq!(report["rows_checked"], 3, "{report}");
    assert_eq!(
        harness.opened(&reader, "API_TOKEN").await.as_deref(),
        Some("tok-2")
    );
    assert_eq!(
        harness.opened(&reader, "LEGACY").await.as_deref(),
        Some("old")
    );
    let again = harness.reseal(&platform, "encrypt").await;
    assert_eq!(
        again["rows_rewritten"], 0,
        "sealing again changes nothing: {again}"
    );

    harness
        .sql(
            "UPDATE service_configs SET value_enc = set_byte(value_enc, 24, get_byte(value_enc, 24) # 1)
             WHERE service_id = $1 AND key = 'GREETING'",
        )
        .await;
    let (status, body) = harness.list(&platform).await;
    assert_eq!(
        status, 500,
        "a sealed value that was altered is an error, not the plaintext: {body}"
    );
    assert!(!body.to_string().contains("hello"), "{body}");
    assert_eq!(
        harness.guest_reads(&platform, "GREETING").await,
        Value::Null,
        "the guest reads nothing rather than the plaintext"
    );
    harness.set(&platform, "GREETING", "hello", false).await;
    assert_eq!(harness.guest_reads(&platform, "GREETING").await, "hello");

    let report = harness.reseal(&platform, "rotate_dek").await;
    assert_eq!(report["tenants_rotated"], 1, "{report}");
    assert_eq!(
        report["rows_rewritten"], 3,
        "every value is sealed under the new key: {report}"
    );
    assert_eq!(
        harness
            .count("SELECT count(*) FROM tenant_data_keys WHERE tenant_id = $1")
            .await,
        2,
        "the retired key is kept"
    );
    assert_eq!(
        harness
            .count(
                "SELECT count(*) FROM service_configs c JOIN services s ON s.id = c.service_id
                 WHERE s.tenant_id = $1 AND substring(c.value_enc FROM 2 FOR 16)
                   <> (SELECT decode(replace(key_id::text, '-', ''), 'hex') FROM tenant_data_keys WHERE tenant_id = $1 AND active)"
            )
            .await,
        0
    );
    assert_eq!(harness.guest_reads(&platform, "API_TOKEN").await, "tok-2");
    drop(platform);

    let only_b = harness.restart(&kek_env(&kek_b, "b")).await;
    assert_eq!(
        harness.guest_reads(&only_b, "API_TOKEN").await,
        Value::Null,
        "a key ring without the key the data keys are wrapped under opens nothing"
    );
    drop(only_b);
    let previous = format!("a:{kek_a}");
    let b_then_a = harness
        .restart(&[
            ("CUFFLINK_SECRETS_KEK", &kek_b),
            ("CUFFLINK_SECRETS_KEK_ID", "b"),
            ("CUFFLINK_SECRETS_KEK_PREVIOUS", &previous),
        ])
        .await;
    assert_eq!(harness.guest_reads(&b_then_a, "API_TOKEN").await, "tok-2");
    let report = harness.reseal(&b_then_a, "rotate_kek").await;
    assert_eq!(report["keys_rewrapped"], 2, "{report}");
    assert_eq!(
        harness
            .count("SELECT count(*) FROM tenant_data_keys WHERE tenant_id = $1 AND kek_id <> 'b'")
            .await,
        0
    );
    drop(b_then_a);
    let only_b = harness.restart(&kek_env(&kek_b, "b")).await;
    assert_eq!(harness.guest_reads(&only_b, "API_TOKEN").await, "tok-2");
    drop(only_b);

    let encrypted = harness
        .restart(&[
            ("CUFFLINK_SECRETS_KEK", &kek_b),
            ("CUFFLINK_SECRETS_KEK_ID", "b"),
            ("CUFFLINK_SECRET_STORAGE", "encrypted"),
        ])
        .await;
    assert_eq!(
        encrypted
            .metric(
                "cufflink_secret_plaintext_rows",
                &[("table", "service_configs")]
            )
            .await,
        Some(3.0),
        "what dual storage wrote keeps its plaintext until a reseal"
    );
    assert!(
        encrypted.output_tail().contains("secret_plaintext_kept"),
        "and the start says so"
    );
    harness.set(&encrypted, "API_TOKEN", "tok-3", true).await;
    let stored = harness.stored("API_TOKEN").await;
    assert_eq!(
        stored.plain(),
        None,
        "encrypted storage writes no plaintext"
    );
    assert!(stored.sealed().is_some());
    assert_eq!(harness.guest_reads(&encrypted, "API_TOKEN").await, "tok-3");
    let report = harness.reseal(&encrypted, "encrypt").await;
    assert_eq!(
        report["rows_rewritten"], 2,
        "the plaintext of the other rows is removed: {report}"
    );
    assert_eq!(
        harness
            .count(
                "SELECT count(*) FROM service_configs c JOIN services s ON s.id = c.service_id
                 WHERE s.tenant_id = $1 AND c.value IS NOT NULL"
            )
            .await,
        0
    );
    assert_eq!(harness.guest_reads(&encrypted, "GREETING").await, "hello");
    drop(encrypted);

    harness
        .sql(
            "INSERT INTO service_configs (service_id, key, value) VALUES ($1, 'UNSEALED', 'plain')",
        )
        .await;
    let mut refused = PlatformProcess::launch(
        &harness.db.url,
        None,
        &[
            ("CUFFLINK_SECRETS_KEK", &kek_b),
            ("CUFFLINK_SECRETS_KEK_ID", "b"),
            ("CUFFLINK_SECRET_STORAGE", "encrypted"),
        ],
    );
    let status = refused.wait_exit(Duration::from_secs(30)).await;
    assert!(
        status.is_some_and(|status| !status.success()),
        "encrypted storage with a secret in plaintext alone does not start: {status:?}\n{}",
        refused.output_tail()
    );
    assert!(
        refused
            .output_tail()
            .contains("secrets are stored in plaintext alone (service_configs: 1)"),
        "{}",
        refused.output_tail()
    );
}

#[tokio::test]
async fn webhook_and_tenant_secrets_are_sealed_the_same_way() {
    let kek_a = kek(3);
    let Some((mut harness, platform)) = Harness::start(&kek_env(&kek_a, "a")).await else {
        return;
    };
    let reader = in_test_box(&harness.db, 3, "a");
    let pool = harness.db.pool();

    let (status, webhook) = platform
        .send(
            Method::POST,
            &format!("/api/services/{}/webhooks", harness.service_id),
            &harness.key(),
            Some(&json!({
                "table_name": "items", "event": "create",
                "url": "http://127.0.0.1:9/hook", "secret": "whsec",
            })),
        )
        .await;
    assert_eq!(status, 200, "{webhook}");
    let webhook_id: Uuid = webhook["id"].as_str().expect("id").parse().expect("uuid");
    let row = pool
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT secret, secret_enc FROM webhooks WHERE id = $1",
            &[&webhook_id],
        )
        .await
        .expect("webhook row");
    let stored = Stored::new(row.get(0), row.get(1));
    assert_eq!(stored.plain(), Some("whsec"));
    let opened = reader
        .open(
            &pool.get().await.expect("checkout"),
            harness.tenant.id,
            &WEBHOOK_SECRET.aad([webhook_id.to_string()]),
            stored.sealed().expect("sealed"),
        )
        .await
        .expect("open");
    assert_eq!(opened.expose(), "whsec");

    let slug = format!("ops-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let (status, created) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &harness.admin,
            Some(&json!({
                "name": slug, "slug": slug,
                "keycloak_ops_client_id": "ops", "keycloak_ops_client_secret": "ops-secret-1",
            })),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    assert!(
        created.get("keycloak_ops_client_secret").is_none(),
        "the response carries no secret: {created}"
    );
    let tenant_id: Uuid = created["id"].as_str().expect("id").parse().expect("uuid");
    let ops_secret = |pool: deadpool_postgres::Pool| async move {
        let row = pool
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT keycloak_ops_client_secret, keycloak_ops_client_secret_enc FROM tenants WHERE id = $1",
                &[&tenant_id],
            )
            .await
            .expect("tenant row");
        Stored::new(row.get(0), row.get(1))
    };
    let stored = ops_secret(pool.clone()).await;
    assert_eq!(stored.plain(), Some("ops-secret-1"));
    let aad = TENANT_OPS_SECRET.aad([tenant_id.to_string()]);
    let opened = reader
        .open(
            &pool.get().await.expect("checkout"),
            tenant_id,
            &aad,
            stored.sealed().expect("sealed"),
        )
        .await
        .expect("open");
    assert_eq!(opened.expose(), "ops-secret-1");

    let (status, updated) = platform
        .send(
            Method::PUT,
            &format!("/api/tenants/{slug}"),
            &harness.admin,
            Some(&json!({ "keycloak_ops_client_secret": "ops-secret-2" })),
        )
        .await;
    assert_eq!(status, 200, "{updated}");
    let stored = ops_secret(pool.clone()).await;
    assert_eq!(stored.plain(), Some("ops-secret-2"));
    assert_eq!(
        reader
            .open(
                &pool.get().await.expect("checkout"),
                tenant_id,
                &aad,
                stored.sealed().expect("sealed"),
            )
            .await
            .expect("open")
            .expose(),
        "ops-secret-2"
    );

    let report = harness.reseal(&platform, "encrypt").await;
    assert_eq!(
        report["rows_checked"], 2,
        "the webhook and the tenant: {report}"
    );
    assert_eq!(report["rows_rewritten"], 0, "{report}");

    harness
        .db
        .mint_tenant_with(NewTenant {
            keycloak_ops_client_secret: Some("plain-only"),
            ..Default::default()
        })
        .await;
    let report = harness.reseal(&platform, "encrypt").await;
    assert_eq!(
        report["rows_rewritten"], 1,
        "a tenant row written in plaintext alone: {report}"
    );
}

const SEALED_ALONE: &str = "SELECT
    (SELECT count(*) FROM service_configs WHERE value IS NULL AND value_enc IS NOT NULL)
  + (SELECT count(*) FROM webhooks WHERE secret IS NULL AND secret_enc IS NOT NULL)
  + (SELECT count(*) FROM tenants
      WHERE keycloak_ops_client_secret IS NULL AND keycloak_ops_client_secret_enc IS NOT NULL)";

#[tokio::test]
async fn leaving_encrypted_storage_for_dual_writes_every_plaintext_back() {
    let kek_a = kek(6);
    let dual = kek_env(&kek_a, "a");
    let Some((harness, platform)) = Harness::start(&dual).await else {
        return;
    };
    harness.set(&platform, "API_TOKEN", "tok-1", true).await;
    let (status, webhook) = platform
        .send(
            Method::POST,
            &format!("/api/services/{}/webhooks", harness.service_id),
            &harness.key(),
            Some(&json!({
                "table_name": "items", "event": "create",
                "url": "http://127.0.0.1:9/hook", "secret": "whsec",
            })),
        )
        .await;
    assert_eq!(status, 200, "{webhook}");
    let slug = format!("ops-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let (status, created) = platform
        .send(
            Method::POST,
            "/api/tenants",
            &harness.admin,
            Some(&json!({
                "name": slug, "slug": slug,
                "keycloak_ops_client_id": "ops", "keycloak_ops_client_secret": "ops-secret",
            })),
        )
        .await;
    assert_eq!(status, 200, "{created}");
    drop(platform);

    let encrypted = harness
        .restart(&[
            ("CUFFLINK_SECRETS_KEK", &kek_a),
            ("CUFFLINK_SECRETS_KEK_ID", "a"),
            ("CUFFLINK_SECRET_STORAGE", "encrypted"),
        ])
        .await;
    let report = harness.reseal(&encrypted, "encrypt").await;
    assert_eq!(
        report["rows_rewritten"], 3,
        "the plaintext of each table is removed: {report}"
    );
    let pool = harness.db.pool();
    let sealed_alone = || async {
        pool.get()
            .await
            .expect("checkout")
            .query_one(SEALED_ALONE, &[])
            .await
            .expect("count")
            .get::<_, i64>(0)
    };
    assert_eq!(
        sealed_alone().await,
        3,
        "no earlier release could read them"
    );
    drop(encrypted);

    let platform = harness.restart(&dual).await;
    let report = harness.reseal(&platform, "encrypt").await;
    assert_eq!(
        report["rows_rewritten"], 3,
        "under dual a reseal writes each plaintext back: {report}"
    );
    assert_eq!(
        sealed_alone().await,
        0,
        "an earlier release reads every one"
    );
    let plain: Vec<Option<String>> = pool
        .get()
        .await
        .expect("checkout")
        .query(
            "SELECT value FROM service_configs WHERE service_id = $1
             UNION ALL SELECT secret FROM webhooks WHERE service_id = $1
             UNION ALL SELECT keycloak_ops_client_secret FROM tenants WHERE slug = $2",
            &[&harness.service_id, &slug],
        )
        .await
        .expect("plaintext")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        plain,
        [
            Some("tok-1".to_string()),
            Some("whsec".to_string()),
            Some("ops-secret".to_string())
        ],
        "as a release that does not seal reads them"
    );
    assert_eq!(harness.guest_reads(&platform, "API_TOKEN").await, "tok-1");
    let again = harness.reseal(&platform, "encrypt").await;
    assert_eq!(again["rows_rewritten"], 0, "{again}");
}

#[tokio::test]
async fn without_a_key_secrets_are_stored_as_before() {
    let Some((harness, platform)) = Harness::start(&[]).await else {
        return;
    };
    harness.set(&platform, "API_TOKEN", "tok-1", true).await;
    let stored = harness.stored("API_TOKEN").await;
    assert_eq!(stored.plain(), Some("tok-1"));
    assert_eq!(stored.sealed(), None, "plaintext storage seals nothing");
    assert_eq!(harness.guest_reads(&platform, "API_TOKEN").await, "tok-1");
    let (status, body) = platform
        .send(
            Method::POST,
            "/api/admin/secrets/reseal",
            &harness.admin,
            Some(&json!({ "operation": "encrypt" })),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("CUFFLINK_SECRETS_KEK")),
        "{body}"
    );
    assert_eq!(
        harness
            .count("SELECT count(*) FROM tenant_data_keys WHERE tenant_id = $1")
            .await,
        0
    );
}

const CONCURRENT_READS: usize = 24;

#[tokio::test]
async fn guests_read_sealed_secrets_on_the_connection_they_hold() {
    let kek_a = kek(4);
    let env = [
        ("CUFFLINK_SECRETS_KEK", kek_a.as_str()),
        ("CUFFLINK_SECRETS_KEK_ID", "a"),
        ("DB_POOL_TIMEOUT_MS", "1500"),
    ];
    let Some((harness, platform)) = Harness::start(&env).await else {
        return;
    };
    harness.set(&platform, "API_TOKEN", "tok-1", true).await;
    drop(platform);
    let platform = harness.restart(&env).await;
    let reads = futures_util::future::join_all(
        (0..CONCURRENT_READS).map(|_| harness.guest_reads(&platform, "API_TOKEN")),
    )
    .await;
    assert!(
        reads.iter().all(|value| value == "tok-1"),
        "a process that knows no data key yet reads it where the guest's read is: {reads:?}"
    );
}

#[tokio::test]
async fn a_reseal_names_the_row_it_cannot_open() {
    let kek_a = kek(5);
    let Some((harness, platform)) = Harness::start(&kek_env(&kek_a, "a")).await else {
        return;
    };
    harness.set(&platform, "API_TOKEN", "tok-1", true).await;
    harness
        .sql(
            "UPDATE service_configs SET value = NULL,
                    value_enc = set_byte(value_enc, length(value_enc) - 1,
                                         get_byte(value_enc, length(value_enc) - 1) # 1)
             WHERE service_id = $1 AND key = 'API_TOKEN'",
        )
        .await;
    let id: Uuid = harness
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT id FROM service_configs WHERE service_id = $1 AND key = 'API_TOKEN'",
            &[&harness.service_id],
        )
        .await
        .expect("row")
        .get(0);
    let (status, body) = platform
        .send(
            Method::POST,
            "/api/admin/secrets/reseal",
            &harness.admin,
            Some(&json!({ "operation": "encrypt" })),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(
        body["error"],
        format!(
            "service_configs row {id}: a sealed value failed authentication: it was altered, or belongs to another row"
        ),
        "the operator is told which row, never what it holds"
    );
}
