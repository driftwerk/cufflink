use cufflink_testkit as common;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use common::{PlatformProcess, Tenant, TestDb};
use cufflink_platform::repo::deploy_lock::DeployLock;
use cufflink_platform::repo::service_configs;
use cufflink_platform::secrets::{KeyRing, SecretBox, StorageMode};
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;
use uuid::Uuid;

const SERVICE: &str = "cfg";
const SECRET: &str = "sk_live_never_answered";
const KEK_ID: &str = "a";

fn kek() -> String {
    STANDARD.encode([7u8; 32])
}

fn entry(key: &str, value: &str, is_secret: bool) -> Value {
    json!({"key": key, "value": value, "is_secret": is_secret})
}

fn plain(key: &str, value: &str) -> Value {
    entry(key, value, false)
}

fn diff(added: &[&str], changed: &[&str], removed: &[&str], unchanged: &[&str]) -> Value {
    json!({"added": added, "changed": changed, "removed": removed, "unchanged": unchanged})
}

fn answer(diff: Value, applied: bool) -> Value {
    let mut answer = diff;
    answer["applied"] = json!(applied);
    answer
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Row {
    key: String,
    value: String,
    is_secret: bool,
    updated_at: chrono::DateTime<chrono::Utc>,
    xmin: String,
}

struct Configured {
    db: TestDb,
    platform: PlatformProcess,
    tenant: Tenant,
    service: Uuid,
}

impl Configured {
    async fn start() -> Option<Self> {
        Self::start_with(&[]).await
    }

    async fn start_with(settings: &[(&str, &str)]) -> Option<Self> {
        let infra = common::infra().await?;
        let mut db = infra.create_database().await;
        let kek = kek();
        let env: Vec<(&str, &str)> = [
            ("CUFFLINK_SECRETS_KEK", kek.as_str()),
            ("CUFFLINK_SECRETS_KEK_ID", KEK_ID),
        ]
        .into_iter()
        .chain(settings.iter().copied())
        .collect();
        let platform = PlatformProcess::start(&db.url, infra.nats_url(), &env).await;
        platform.wait_ready().await;
        let tenant = db.mint_tenant().await;
        let service = deployed(&platform, &tenant).await;
        Some(Self {
            db,
            platform,
            tenant,
            service,
        })
    }

    fn key(&self) -> String {
        format!("ApiKey {}", self.tenant.api_key)
    }

    fn config_path(&self) -> String {
        format!("/api/services/{}/config", self.service)
    }

    async fn apply_as(&self, body: &Value) -> (u16, Value) {
        let path = format!("{}/apply", self.config_path());
        self.platform
            .send(Method::POST, &path, &self.key(), Some(body))
            .await
    }

    async fn apply(&self, entries: &[Value], prune: bool, dry_run: bool) -> Value {
        let (status, body) = self
            .apply_as(&json!({"entries": entries, "prune": prune, "dry_run": dry_run}))
            .await;
        assert_eq!(status, 200, "apply: {body}");
        body
    }

    async fn rows(&self) -> Vec<Row> {
        let pool = self.db.pool();
        let client = pool.get().await.expect("checkout");
        let secrets = SecretBox::new(
            pool.clone(),
            KeyRing::parse(Some(&kek()), Some(KEK_ID), None).expect("ring"),
            StorageMode::Dual,
        );
        let written = client
            .query(
                "SELECT xmin::text FROM service_configs WHERE service_id = $1 ORDER BY key",
                &[&self.service],
            )
            .await
            .expect("rows");
        service_configs::opened(&secrets, &client, self.tenant.id, self.service)
            .await
            .expect("configs")
            .into_iter()
            .zip(&written)
            .map(|(entry, row)| Row {
                key: entry.key,
                value: entry
                    .value
                    .map(|value| value.expose().to_string())
                    .unwrap_or_default(),
                is_secret: entry.is_secret,
                updated_at: entry.updated_at,
                xmin: row.get(0),
            })
            .collect()
    }

    async fn held(&self) -> Vec<(String, String, bool)> {
        self.rows()
            .await
            .into_iter()
            .map(|row| (row.key, row.value, row.is_secret))
            .collect()
    }

    async fn audited(&self) -> Vec<(String, Value)> {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query(
                "SELECT action, details FROM audit_log
                 WHERE resource_type = 'service_config' AND resource_id = $1
                 ORDER BY created_at, id",
                &[&self.service.to_string()],
            )
            .await
            .expect("audit rows")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    async fn data_keys(&self) -> i64 {
        self.db
            .pool()
            .get()
            .await
            .expect("checkout")
            .query_one(
                "SELECT count(*) FROM tenant_data_keys WHERE tenant_id = $1",
                &[&self.tenant.id],
            )
            .await
            .expect("data keys")
            .get(0)
    }
}

async fn deployed(platform: &PlatformProcess, tenant: &Tenant) -> Uuid {
    let manifest = common::wasm_manifest(SERVICE, &[("echo", "/echo")]);
    let (status, body) = platform.deploy(tenant, &manifest).await;
    assert_eq!(status, 200, "deploy: {body}");
    body["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("service id")
}

fn held(rows: &[(&str, &str, bool)]) -> Vec<(String, String, bool)> {
    rows.iter()
        .map(|(key, value, is_secret)| (key.to_string(), value.to_string(), *is_secret))
        .collect()
}

#[tokio::test]
async fn cf7_apply_is_one_transaction() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    configured
        .apply(&[plain("A", "old"), plain("GONE", "x")], false, false)
        .await;
    let before = configured.rows().await;

    let (status, body) = configured
        .apply_as(&json!({
            "entries": [plain("A", "new"), plain("B", "added"), plain("C\u{0}", "unwritable")],
            "prune": true,
        }))
        .await;

    assert_eq!(
        status, 500,
        "a key the database refuses fails the apply: {body}"
    );
    assert_eq!(
        configured.rows().await,
        before,
        "the change and the addition made before the refused key are undone with it, and the \
         removal is not made"
    );
    assert_eq!(
        configured
            .audited()
            .await
            .iter()
            .filter(|(action, _)| action == "config.apply")
            .count(),
        1,
        "an apply that failed is not recorded as one that was made"
    );
}

#[tokio::test]
async fn cf7_prune_removes_only_with_the_flag() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    configured
        .apply(&[plain("A", "1"), plain("B", "2")], false, false)
        .await;
    let (status, body) = configured
        .platform
        .send(
            Method::PUT,
            &configured.config_path(),
            &configured.key(),
            Some(&plain("BY_HAND", "kept")),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    assert_eq!(
        configured.apply(&[plain("A", "1")], false, false).await,
        answer(diff(&[], &[], &[], &["A"]), true),
        "a key the set does not name is not mentioned"
    );
    assert_eq!(
        configured.held().await,
        held(&[
            ("A", "1", false),
            ("B", "2", false),
            ("BY_HAND", "kept", false)
        ])
    );

    assert_eq!(
        configured.apply(&[plain("A", "1")], true, false).await,
        answer(diff(&[], &[], &["B", "BY_HAND"], &["A"]), true)
    );
    assert_eq!(configured.held().await, held(&[("A", "1", false)]));

    assert_eq!(
        configured.apply(&[], true, false).await,
        answer(diff(&[], &[], &["A"], &[]), true),
        "an empty set, pruned, is no configuration"
    );
    assert!(configured.rows().await.is_empty());
}

#[tokio::test]
async fn cf7_dry_run_writes_nothing_and_names_no_value() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    let declared = [
        plain("A", "1"),
        entry("TOKEN", SECRET, true),
        plain("C", "3"),
    ];
    assert_eq!(
        configured.apply(&declared, false, true).await,
        answer(diff(&["A", "C", "TOKEN"], &[], &[], &[]), false)
    );
    assert_eq!(
        configured.data_keys().await,
        0,
        "a dry run seals nothing, so it makes the tenant no data key"
    );
    assert!(configured.rows().await.is_empty());
    configured.apply(&declared, false, false).await;
    let before = configured.rows().await;

    let (status, body) = configured
        .apply_as(&json!({
            "entries": [plain("A", "changed"), entry("TOKEN", SECRET, true), plain("D", "4")],
            "prune": true,
            "dry_run": true,
        }))
        .await;

    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body,
        answer(diff(&["D"], &["A"], &["C"], &["TOKEN"]), false),
        "keys only, and a secret that is what was sent is unchanged"
    );
    assert_eq!(configured.rows().await, before);
    assert_eq!(
        configured
            .apply(&[plain("TOKEN", SECRET)], false, true)
            .await,
        answer(diff(&[], &["TOKEN"], &[], &[]), false),
        "the same value no longer a secret is a change"
    );
    assert_eq!(configured.rows().await, before);
}

#[tokio::test]
async fn cf7_two_concurrent_applies_serialise() {
    let Some(configured) = Configured::start().await else {
        return;
    };

    let mut holder = configured.db.pool().get().await.expect("checkout");
    let held_by_the_test = holder.transaction().await.expect("begin");
    service_configs::lock_apply(&held_by_the_test, configured.service)
        .await
        .expect("lock");
    let first = [plain("A", "1")];
    let waiting = configured.apply(&first, true, false);
    tokio::pin!(waiting);
    assert!(
        tokio::time::timeout(Duration::from_millis(700), &mut waiting)
            .await
            .is_err(),
        "an apply waits for the one that holds the service's apply lock"
    );
    let (status, body) = configured
        .platform
        .send(
            Method::PUT,
            &configured.config_path(),
            &configured.key(),
            Some(&plain("LATE", "written while the apply waited")),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    held_by_the_test.commit().await.expect("commit");
    assert_eq!(
        waiting.await,
        answer(diff(&["A"], &[], &["LATE"], &[]), true),
        "it reads once it holds the lock: what was written while it waited is in its plan"
    );

    let sets: Vec<Vec<Value>> = (0..8)
        .map(|set| {
            (0..4)
                .map(|key| plain(&format!("S{set}_K{key}"), "v"))
                .collect()
        })
        .collect();
    futures_util::future::join_all(sets.iter().map(|set| configured.apply(set, true, false))).await;

    let left: BTreeSet<String> = configured
        .rows()
        .await
        .into_iter()
        .map(|row| row.key)
        .collect();
    let whole = |set: &Vec<Value>| -> BTreeSet<String> {
        set.iter()
            .filter_map(|entry| entry["key"].as_str().map(str::to_string))
            .collect()
    };
    assert!(
        sets.iter().any(|set| whole(set) == left),
        "pruning applies made at once leave exactly the set of the last, never a mix: {left:?}"
    );
}

#[tokio::test]
async fn cf7_an_apply_runs_beside_a_deploy() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    let deploying = DeployLock::try_acquire(&configured.db.pool(), configured.service)
        .await
        .expect("lock")
        .expect("no deploy runs");
    let manifest = common::wasm_manifest(SERVICE, &[("echo", "/echo")]);
    let (status, body) = configured
        .platform
        .deploy(&configured.tenant, &manifest)
        .await;
    assert_eq!(status, 409, "the deploy lock is held: {body}");

    assert_eq!(
        configured.apply(&[plain("A", "1")], false, false).await,
        answer(diff(&["A"], &[], &[], &[]), true),
        "an apply does not wait for a deploy, and is not refused for one"
    );
    deploying.release().await;
}

#[tokio::test]
async fn cf7_a_dry_run_is_audited_as_a_check() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    configured.apply(&[plain("A", "1")], false, false).await;
    configured
        .apply(&[plain("A", "2"), plain("B", "3")], false, true)
        .await;

    let audited = configured.audited().await;
    let recorded: Vec<(&str, Value)> = audited
        .iter()
        .map(|(action, details)| {
            let mut details = details.clone();
            let credential = details
                .as_object_mut()
                .and_then(|details| details.remove("credential"));
            assert_eq!(
                credential.map(|credential| credential["method"].clone()),
                Some(json!("api_key")),
                "{action} names what it was made with"
            );
            (action.as_str(), details)
        })
        .collect();
    assert_eq!(
        recorded,
        [
            ("config.apply", diff(&["A"], &[], &[], &[])),
            ("config.check", diff(&["B"], &["A"], &[], &[])),
        ]
    );
}

#[tokio::test]
async fn cf7_an_unchanged_key_keeps_its_updated_at() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    let set = [plain("A", "1"), entry("TOKEN", SECRET, true)];
    configured.apply(&set, false, false).await;
    let first = configured.rows().await;

    assert_eq!(
        configured.apply(&set, false, false).await,
        answer(diff(&[], &[], &[], &["A", "TOKEN"]), true)
    );
    assert_eq!(
        configured.rows().await,
        first,
        "the same set applied again rewrites no row"
    );

    configured
        .apply(
            &[plain("A", "2"), entry("TOKEN", SECRET, true)],
            false,
            false,
        )
        .await;
    let second = configured.rows().await;
    assert_eq!(second[1], first[1], "the key that did not change");
    assert_eq!(second[0].value, "2");
    assert!(second[0].updated_at > first[0].updated_at);
}

#[tokio::test]
async fn cf7_the_answer_never_holds_a_value() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    let path = format!("{}/apply", configured.config_path());
    let asked = |entries: Value, dry_run: bool| {
        configured
            .platform
            .http
            .post(format!("{}{path}", configured.platform.base_url))
            .header("authorization", configured.key())
            .json(&json!({"entries": entries, "dry_run": dry_run}))
            .send()
    };
    let mut answers = Vec::new();
    for (value, dry_run) in [(SECRET, true), (SECRET, false), ("another", true)] {
        let response = asked(json!([entry("TOKEN", value, true)]), dry_run)
            .await
            .expect("apply");
        assert_eq!(response.status().as_u16(), 200);
        answers.push(response.text().await.expect("body"));
    }

    assert!(
        answers.iter().all(|answer| !answer.contains(SECRET)),
        "{answers:?}"
    );
    let audited = configured.audited().await;
    assert_eq!(audited.len(), 3);
    assert!(
        audited
            .iter()
            .all(|(_, details)| !details.to_string().contains(SECRET)),
        "{audited:?}"
    );
    assert_eq!(
        configured.held().await,
        held(&[("TOKEN", SECRET, true)]),
        "the value was stored"
    );
}

#[tokio::test]
async fn cf7_per_key_routes_answer_as_v0_29_2() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    let asked = |method: Method, path: String, body: Option<Value>| {
        let request = configured
            .platform
            .http
            .request(method, format!("{}{path}", configured.platform.base_url))
            .header("authorization", configured.key());
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        async move {
            let response = request.send().await.expect("request");
            (
                response.status().as_u16(),
                response.text().await.expect("body"),
            )
        }
    };
    let config = configured.config_path();

    assert_eq!(
        asked(Method::PUT, config.clone(), Some(plain("apply", "1"))).await,
        (
            200,
            r#"{"is_secret":false,"key":"apply","status":"saved"}"#.to_string()
        )
    );
    assert_eq!(
        asked(
            Method::PUT,
            config.clone(),
            Some(json!({"key": "TOKEN", "value": SECRET, "is_secret": true, "note": "ignored"}))
        )
        .await,
        (
            200,
            r#"{"is_secret":true,"key":"TOKEN","status":"saved"}"#.to_string()
        ),
        "a field the route does not know is passed over, as it was"
    );

    let listed: Vec<String> = configured
        .rows()
        .await
        .iter()
        .map(|row| {
            format!(
                r#"{{"is_secret":{},"key":"{}","updated_at":"{}","value":"{}"}}"#,
                row.is_secret,
                row.key,
                row.updated_at.to_rfc3339(),
                if row.is_secret { "***" } else { &row.value }
            )
        })
        .collect();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        asked(Method::GET, config.clone(), None).await,
        (200, format!(r#"{{"configs":[{}]}}"#, listed.join(",")))
    );

    assert_eq!(
        asked(Method::DELETE, format!("{config}/apply"), None).await,
        (200, r#"{"deleted":true,"key":"apply"}"#.to_string()),
        "a key named as the apply route is still deleted by its name"
    );
    assert_eq!(
        asked(Method::DELETE, format!("{config}/apply"), None).await,
        (
            404,
            r#"{"error":"Config key 'apply' not found"}"#.to_string()
        )
    );
    assert_eq!(
        asked(Method::DELETE, format!("{config}/TOKEN"), None).await,
        (200, r#"{"deleted":true,"key":"TOKEN"}"#.to_string())
    );
    assert_eq!(
        configured
            .audited()
            .await
            .iter()
            .map(|(action, _)| action.as_str())
            .collect::<Vec<_>>(),
        ["config.set", "config.set", "config.delete", "config.delete"],
        "one audit row for each key written, as before"
    );
}

#[tokio::test]
async fn cf7_a_first_seal_rolled_back_caches_no_key() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    assert_eq!(
        configured.data_keys().await,
        0,
        "nothing of the tenant was sealed yet"
    );

    let (status, body) = configured
        .apply_as(&json!({"entries": [plain("A", "1"), plain("B\u{0}", "unwritable")]}))
        .await;
    assert_eq!(status, 500, "{body}");
    assert_eq!(
        configured.data_keys().await,
        0,
        "the data key the apply made went with it"
    );

    configured.apply(&[plain("A", "1")], false, false).await;
    assert_eq!(configured.data_keys().await, 1);
    assert_eq!(
        configured.held().await,
        held(&[("A", "1", false)]),
        "the value is sealed under the key the database holds, not one remembered from the apply that failed"
    );
}

#[tokio::test]
async fn cf7_an_apply_that_cannot_be_planned_is_refused() {
    let Some(mut configured) = Configured::start().await else {
        return;
    };

    let (status, body) = configured
        .apply_as(&json!({"entries": [plain("A", "1"), entry("A", "2", true)]}))
        .await;
    assert_eq!(
        (status, body),
        (
            400,
            json!({"error": "Config key 'A' is given more than once"})
        )
    );

    for unknown in [
        json!({"entries": [], "purge": true}),
        json!({"entries": [{"key": "A", "value": "1", "secret": true}]}),
        json!({"prune": true}),
    ] {
        let (status, _) = configured.apply_as(&unknown).await;
        assert_eq!(status, 422, "{unknown}");
    }

    let path = format!("/api/services/{}/config/apply", Uuid::new_v4());
    let (status, body) = configured
        .platform
        .send(
            Method::POST,
            &path,
            &configured.key(),
            Some(&json!({"entries": []})),
        )
        .await;
    assert_eq!((status, body), (404, json!({"error": "Service not found"})));

    let another = configured.db.mint_tenant().await;
    let (status, body) = configured
        .platform
        .send(
            Method::POST,
            &format!("{}/apply", configured.config_path()),
            &format!("ApiKey {}", another.api_key),
            Some(&json!({"entries": [plain("A", "1")]})),
        )
        .await;
    assert_eq!(
        (status, body),
        (404, json!({"error": "Service not found"})),
        "another tenant's service is none of this one's"
    );
    assert!(configured.rows().await.is_empty());
    assert!(configured.audited().await.is_empty());
}

#[tokio::test]
async fn cf7_a_value_that_does_not_open_is_written_again_by_an_apply_that_names_it() {
    let Some(configured) = Configured::start().await else {
        return;
    };
    configured
        .apply(
            &[entry("TOKEN", SECRET, true), plain("OTHER", "1")],
            false,
            false,
        )
        .await;
    configured
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .execute(
            "UPDATE service_configs
             SET value_enc = set_byte(value_enc, 24, get_byte(value_enc, 24) # 1)
             WHERE service_id = $1",
            &[&configured.service],
        )
        .await
        .expect("both values altered");

    assert_eq!(
        configured.apply(&[plain("NEW", "2")], false, false).await,
        answer(diff(&["NEW"], &[], &[], &[]), true),
        "a value no apply names does not stand in the way of one"
    );
    assert_eq!(
        configured
            .apply(&[entry("TOKEN", SECRET, true)], false, true)
            .await,
        answer(diff(&[], &["TOKEN"], &[], &[]), false),
        "a value that does not open is never the one wanted"
    );
    assert_eq!(
        configured
            .apply(
                &[entry("TOKEN", SECRET, true), plain("NEW", "2")],
                true,
                false
            )
            .await,
        answer(diff(&[], &["TOKEN"], &["OTHER"], &["NEW"]), true)
    );
    assert_eq!(
        configured.held().await,
        held(&[("NEW", "2", false), ("TOKEN", SECRET, true)]),
        "written again, the value opens"
    );
}

#[tokio::test]
async fn cf7_an_apply_compares_what_is_sealed_where_nothing_is_kept_in_plaintext() {
    let Some(configured) =
        Configured::start_with(&[("CUFFLINK_SECRET_STORAGE", "encrypted")]).await
    else {
        return;
    };
    let set = [plain("A", "1"), entry("TOKEN", SECRET, true)];
    configured.apply(&set, false, false).await;
    let plaintext: i64 = configured
        .db
        .pool()
        .get()
        .await
        .expect("checkout")
        .query_one(
            "SELECT count(*) FROM service_configs WHERE service_id = $1 AND value IS NOT NULL",
            &[&configured.service],
        )
        .await
        .expect("count")
        .get(0);
    assert_eq!(plaintext, 0, "the storage under test keeps no plaintext");
    let first = configured.rows().await;

    assert_eq!(
        configured.apply(&set, false, false).await,
        answer(diff(&[], &[], &[], &["A", "TOKEN"]), true)
    );
    assert_eq!(configured.rows().await, first, "no row is rewritten");
    assert_eq!(
        configured
            .apply(
                &[plain("A", "1"), entry("TOKEN", "rotated", true)],
                false,
                false
            )
            .await,
        answer(diff(&[], &["TOKEN"], &[], &["A"]), true)
    );
    assert_eq!(
        configured.held().await,
        held(&[("A", "1", false), ("TOKEN", "rotated", true)])
    );
}

#[tokio::test]
async fn cf7_an_apply_that_waits_too_long_for_another_is_told_to_try_again() {
    let Some(configured) = Configured::start_with(&[("DB_STATEMENT_TIMEOUT_MS", "3000")]).await
    else {
        return;
    };
    let mut holder = configured.db.pool().get().await.expect("checkout");
    let held_by_the_test = holder.transaction().await.expect("begin");
    service_configs::lock_apply(&held_by_the_test, configured.service)
        .await
        .expect("lock");

    let (status, body) = configured
        .apply_as(&json!({"entries": [plain("A", "1")]}))
        .await;

    assert_eq!(
        (status, body),
        (
            503,
            json!({"error": "Another apply of this service's config is still running; try again"})
        )
    );
    held_by_the_test.rollback().await.expect("rollback");
    assert!(configured.rows().await.is_empty());
}
