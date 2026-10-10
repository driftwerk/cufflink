use cufflink_platform::config::PlatformConfig;
use cufflink_platform::extensions::ExtensionsConfig;
use cufflink_platform::wasm_runtime::sha256_hex;
use cufflink_testkit::{
    guest, metric_value, unless_absent, Platform, PlatformOptions, TenantHandle, TenantOptions,
};
use cufflink_types::runner::{Environment, NativeService, NativeSet, WebApp, WebSet};
use reqwest::header::{HeaderMap, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::Method;
use serde_json::{json, Value};
use uuid::Uuid;

const WASM: &str = "kit-service";

struct World {
    platform: Platform,
    http: reqwest::Client,
    a: TenantHandle,
    b: TenantHandle,
}

struct Answered {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Answered {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }
}

impl World {
    async fn start() -> Option<Self> {
        let options = PlatformOptions::default().configure(|config| PlatformConfig {
            extensions: ExtensionsConfig {
                enabled: true,
                ..config.extensions
            },
            ..config
        });
        let platform = unless_absent(Platform::start(options).await).expect("a platform")?;
        let tenant = |prefix| {
            let platform = &platform;
            async move {
                platform
                    .tenant(&Platform::unique_slug(prefix), TenantOptions::default())
                    .await
                    .expect("a tenant")
            }
        };
        let (a, b) = (tenant("runa").await, tenant("runb").await);
        Some(Self {
            http: reqwest::Client::new(),
            platform,
            a,
            b,
        })
    }

    async fn send(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, String)],
        body: Option<&Value>,
    ) -> Answered {
        let request = headers.iter().fold(
            self.http
                .request(method, format!("{}{path}", self.platform.base_url())),
            |request, (name, value)| request.header(*name, value),
        );
        let response = match body {
            Some(body) => request.json(body),
            None => request,
        }
        .send()
        .await
        .expect("a request");
        Answered {
            status: response.status().as_u16(),
            headers: response.headers().clone(),
            body: response.bytes().await.expect("a body").to_vec(),
        }
    }

    fn admin(&self) -> String {
        format!("Bearer {}", self.platform.admin_token().expect("a token"))
    }

    fn operator(&self, tenant: &TenantHandle) -> String {
        format!(
            "Bearer {}",
            self.platform.operator_token(tenant).expect("a token")
        )
    }

    async fn deploy_native(
        &self,
        tenant: &TenantHandle,
        name: &str,
        extension: &str,
        binary: &[u8],
    ) -> Uuid {
        use base64::Engine as _;
        let manifest = json!({
            "name": name,
            "version": "0.1.0",
            "mode": "native",
            "tables": [],
            "provides": [{"name": extension, "callers": ["*"]}],
        });
        let deployed = self
            .send(
                Method::POST,
                "/api/services/deploy",
                &[("authorization", self.operator(tenant))],
                Some(&json!({
                    "manifest": manifest,
                    "native_base64": base64::engine::general_purpose::STANDARD.encode(binary),
                    "api_version": 1,
                })),
            )
            .await;
        assert_eq!(deployed.status, 200, "{}", deployed.json());
        deployed.json()["service_id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .expect("a service id")
    }

    async fn deploy_web(&self, tenant: &TenantHandle, name: &str, bundle: &[u8]) -> Uuid {
        let manifest = json!({"name": name, "version": "0.1.0", "mode": "web", "tables": []});
        let deployed = self
            .send(
                Method::POST,
                "/api/services/deploy",
                &[("authorization", self.operator(tenant))],
                Some(&json!({"manifest": manifest, "api_version": 1})),
            )
            .await;
        assert_eq!(deployed.status, 200, "{}", deployed.json());
        let service: Uuid = deployed.json()["service_id"]
            .as_str()
            .and_then(|id| id.parse().ok())
            .expect("a service id");
        let uploaded = self
            .http
            .post(format!(
                "{}/api/services/{service}/web",
                self.platform.base_url()
            ))
            .header("authorization", self.operator(tenant))
            .body(bundle.to_vec())
            .send()
            .await
            .expect("an upload");
        assert_eq!(uploaded.status().as_u16(), 200);
        service
    }

    async fn set_config(&self, tenant: &TenantHandle, service: Uuid, key: &str, secret: bool) {
        let set = self
            .send(
                Method::PUT,
                &format!("/api/services/{service}/config"),
                &[("authorization", self.operator(tenant))],
                Some(&json!({"key": key, "value": format!("{key}-of-{}", tenant.slug), "is_secret": secret})),
            )
            .await;
        assert_eq!(set.status, 200, "{}", set.json());
    }

    async fn mint(&self, tenant: &TenantHandle, name: &str, runs: &str) -> (Uuid, String) {
        let minted = self
            .send(
                Method::POST,
                &format!("/api/tenants/{}/runner-keys", tenant.slug),
                &[("authorization", self.admin())],
                Some(&json!({"name": name, "runs": runs})),
            )
            .await;
        assert_eq!(minted.status, 201, "{}", minted.json());
        let body = minted.json();
        (
            body["runner_key"]["id"]
                .as_str()
                .and_then(|id| id.parse().ok())
                .expect("an id"),
            body["key"].as_str().expect("the key").to_string(),
        )
    }

    async fn api_key(&self, tenant: &TenantHandle) -> String {
        let minted = self
            .send(
                Method::POST,
                "/api/auth/api-keys",
                &[("authorization", self.operator(tenant))],
                Some(&json!({"name": "ci"})),
            )
            .await;
        assert_eq!(minted.status, 200, "{}", minted.json());
        minted.json()["key"].as_str().expect("a key").to_string()
    }

    async fn runner(&self, key: &str, path: &str) -> Answered {
        self.send(
            Method::GET,
            path,
            &[("authorization", format!("RunnerKey {key}"))],
            None,
        )
        .await
    }

    async fn audited(&self, action: &str, id: Uuid) -> Vec<(Option<Uuid>, Value)> {
        let (client, connection) =
            tokio_postgres::connect(&self.platform.database_url(), tokio_postgres::NoTls)
                .await
                .expect("the platform's database");
        tokio::spawn(connection);
        client
            .query(
                "SELECT tenant_id, details FROM audit_log
                 WHERE action = $1 AND resource_type = 'runner_key' AND resource_id = $2",
                &[&action, &id.to_string()],
            )
            .await
            .expect("the audit log")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    async fn metric(&self, labels: &[(&str, &str)]) -> f64 {
        let text = self.send(Method::GET, "/metrics", &[], None).await.body;
        metric_value(
            &String::from_utf8_lossy(&text),
            "cufflink_runner_api_requests_total",
            labels,
        )
        .unwrap_or(0.0)
    }
}

#[tokio::test]
async fn runner_keys_are_a_platform_admins_to_mint_list_and_revoke() {
    let Some(w) = World::start().await else {
        return;
    };
    let keys = format!("/api/tenants/{}/runner-keys", w.a.slug);
    let tenant_key = format!("ApiKey {}", w.api_key(&w.a).await);
    for (authorization, refused) in [
        (w.operator(&w.a), "the tenant's operator"),
        (tenant_key, "the tenant's API key"),
    ] {
        let answered = w
            .send(
                Method::POST,
                &keys,
                &[("authorization", authorization)],
                Some(&json!({"name": "runner", "runs": "native"})),
            )
            .await;
        assert_eq!(answered.status, 401, "{refused}: {}", answered.json());
    }
    let requested = |body: Value| {
        let (w, keys) = (&w, keys.clone());
        async move {
            w.send(
                Method::POST,
                &keys,
                &[("authorization", w.admin())],
                Some(&body),
            )
            .await
            .status
        }
    };
    for refused in [
        json!({"name": " ", "runs": "native"}),
        json!({"name": "runner"}),
        json!({"name": "runner", "runs": "wasm"}),
    ] {
        assert_eq!(requested(refused.clone()).await, 422, "{refused}");
    }
    let nowhere = w
        .send(
            Method::POST,
            "/api/tenants/no-such-tenant/runner-keys",
            &[("authorization", w.admin())],
            Some(&json!({"name": "runner", "runs": "native"})),
        )
        .await;
    assert_eq!(nowhere.status, 404);

    let (id, key) = w.mint(&w.a, "runner-1", "native").await;
    assert!(key.starts_with("cfr_"), "{key}");
    let listed = w
        .send(Method::GET, &keys, &[("authorization", w.admin())], None)
        .await
        .json();
    assert_eq!(listed["tenant"], json!(w.a.slug));
    let listed = listed["keys"].as_array().expect("keys").clone();
    assert_eq!(listed.len(), 1, "{listed:?}");
    assert_eq!(listed[0]["id"], json!(id));
    assert_eq!(listed[0]["name"], "runner-1");
    assert_eq!(listed[0]["runs"], "native");
    assert_eq!(listed[0]["prefix"], json!(&key[4..16]));
    assert_eq!(listed[0]["revoked_at"], Value::Null);
    assert!(
        !listed[0].to_string().contains(&key[17..]),
        "a listing never carries the secret"
    );
    assert_eq!(
        w.audited("runner_key.create", id)
            .await
            .into_iter()
            .map(|(tenant, details)| (tenant, details["name"].clone(), details["runs"].clone()))
            .collect::<Vec<_>>(),
        [(Some(w.a.id), json!("runner-1"), json!("native"))]
    );
    assert_eq!(w.runner(&key, "/api/runner/native").await.status, 200);
    assert_eq!(
        w.runner(&key, "/api/runner/web").await.status,
        404,
        "a key of the extension runner finds no web runner's route"
    );

    let revoke = |slug: &str| {
        let (w, path) = (&w, format!("/api/tenants/{slug}/runner-keys/{id}"));
        async move {
            w.send(Method::DELETE, &path, &[("authorization", w.admin())], None)
                .await
                .status
        }
    };
    assert_eq!(revoke(&w.b.slug).await, 404, "another tenant's key");
    assert_eq!(w.runner(&key, "/api/runner/native").await.status, 200);
    assert_eq!(revoke(&w.a.slug).await, 200);
    assert_eq!(revoke(&w.a.slug).await, 404, "revoked once");
    for path in ["/api/runner/native", "/api/runner/web"] {
        let refused = w.runner(&key, path).await;
        assert_eq!(refused.status, 401, "{path}: {}", refused.json());
        assert_eq!(refused.json()["error"], "Invalid runner key");
    }
    let listed = w
        .send(Method::GET, &keys, &[("authorization", w.admin())], None)
        .await
        .json();
    assert!(listed["keys"][0]["revoked_at"].is_string(), "{listed}");
    assert_eq!(w.audited("runner_key.revoke", id).await.len(), 1);
    w.platform.shutdown().await.expect("a clean shutdown");
}

#[tokio::test]
async fn the_runner_api_answers_for_the_keys_tenant_alone() {
    let Some(w) = World::start().await else {
        return;
    };
    let (binary_a, binary_b) = (b"\x7fELF of a".as_slice(), b"\x7fELF of b".as_slice());
    let ext_a = w.deploy_native(&w.a, "ext-a", "a.echo", binary_a).await;
    let ext_b = w.deploy_native(&w.b, "ext-b", "b.echo", binary_b).await;
    let plain = json!({
        "name": "plain",
        "version": "0.1.0",
        "tables": [{"name": "notes", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true}
        ]}],
    });
    let deployed = w
        .platform
        .deploy(&w.a, &plain, None)
        .await
        .expect("a deploy");
    assert!(deployed.is_success(), "{deployed:?}");
    let plain: Uuid = deployed.body["service_id"]
        .as_str()
        .and_then(|id| id.parse().ok())
        .expect("a service id");
    let (bundle_a, bundle_b) = (
        b"\x1f\x8b bundle of a".as_slice(),
        b"\x1f\x8b bundle of b".as_slice(),
    );
    let web_a = w.deploy_web(&w.a, "site-a", bundle_a).await;
    let web_b = w.deploy_web(&w.b, "site-b", bundle_b).await;
    w.set_config(&w.a, ext_a, "PLAIN", false).await;
    w.set_config(&w.a, ext_a, "TOKEN", true).await;
    w.set_config(&w.a, plain, "TOKEN", true).await;
    w.set_config(&w.a, web_a, "SITE_TOKEN", true).await;
    w.set_config(&w.b, ext_b, "TOKEN", true).await;
    w.set_config(&w.b, web_b, "SITE_TOKEN", true).await;
    let (_, key_a) = w.mint(&w.a, "runner-a", "native").await;
    let (_, key_b) = w.mint(&w.b, "runner-b", "native").await;
    let (_, web_key_a) = w.mint(&w.a, "web-runner-a", "web").await;
    let not_found_before = w
        .metric(&[("endpoint", "binary"), ("result", "not_found")])
        .await;

    let listed = w.runner(&key_a, "/api/runner/native").await;
    assert_eq!(listed.status, 200);
    assert_eq!(
        serde_json::from_slice::<NativeSet>(&listed.body).expect("a native set"),
        NativeSet {
            tenant: w.a.slug.clone(),
            services: vec![NativeService {
                service_id: ext_a,
                service_name: "ext-a".into(),
                version: 2,
                sha256: Some(sha256_hex(binary_a)),
                size: Some(binary_a.len() as u64),
                extensions: vec!["a.echo".into()],
            }],
        }
    );
    assert!(
        !String::from_utf8_lossy(&listed.body).contains("TOKEN-of"),
        "secrets are answered by the environment endpoint alone"
    );
    let listed_b = w.runner(&key_b, "/api/runner/native").await;
    let listed_b: NativeSet = serde_json::from_slice(&listed_b.body).expect("a native set");
    assert_eq!(listed_b.tenant, w.b.slug);
    assert_eq!(
        listed_b
            .services
            .iter()
            .map(|service| service.service_id)
            .collect::<Vec<_>>(),
        [ext_b]
    );

    let fetched = w
        .runner(
            &key_a,
            &format!("/api/runner/native/{ext_a}/{}", sha256_hex(binary_a)),
        )
        .await;
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.body, binary_a);
    assert_eq!(fetched.headers[CONTENT_TYPE], "application/octet-stream");
    assert_eq!(
        fetched.headers[CONTENT_LENGTH],
        binary_a.len().to_string().as_str()
    );
    for (service, sha) in [
        (ext_b, sha256_hex(binary_b)),
        (ext_a, sha256_hex(binary_b)),
        (ext_a, sha256_hex(b"no deployment records this")),
        (ext_a, sha256_hex(binary_a).to_uppercase()),
        (plain, sha256_hex(binary_a)),
        (web_a, sha256_hex(bundle_a)),
        (Uuid::new_v4(), sha256_hex(binary_a)),
    ] {
        let refused = w
            .runner(&key_a, &format!("/api/runner/native/{service}/{sha}"))
            .await;
        assert_eq!(refused.status, 404, "{service} {sha}: {}", refused.json());
    }

    let environment = w
        .runner(&key_a, &format!("/api/runner/native/{ext_a}/environment"))
        .await;
    assert_eq!(environment.status, 200);
    assert_eq!(environment.headers[CACHE_CONTROL], "no-store");
    assert_eq!(
        serde_json::from_slice::<Environment>(&environment.body).expect("an environment"),
        Environment {
            environment: [
                ("PLAIN".to_string(), format!("PLAIN-of-{}", w.a.slug)),
                ("TOKEN".to_string(), format!("TOKEN-of-{}", w.a.slug)),
            ]
            .into(),
        }
    );
    for service in [ext_b, plain, web_a, Uuid::new_v4()] {
        let refused = w
            .runner(&key_a, &format!("/api/runner/native/{service}/environment"))
            .await;
        assert_eq!(refused.status, 404, "{service}: {}", refused.json());
        assert!(!String::from_utf8_lossy(&refused.body).contains("TOKEN-of"));
    }
    let apps = w.runner(&web_key_a, "/api/runner/web").await;
    assert_eq!(apps.status, 200);
    assert_eq!(
        serde_json::from_slice::<WebSet>(&apps.body).expect("a web set"),
        WebSet {
            tenant: w.a.slug.clone(),
            apps: vec![WebApp {
                service_id: web_a,
                service_name: "site-a".into(),
                version: 2,
                sha256: Some(sha256_hex(bundle_a)),
                size: Some(bundle_a.len() as u64),
            }],
        },
        "web apps alone, and the key's tenant's alone"
    );
    let bundle = w
        .runner(
            &web_key_a,
            &format!("/api/runner/web/{web_a}/{}", sha256_hex(bundle_a)),
        )
        .await;
    assert_eq!(bundle.status, 200);
    assert_eq!(bundle.body, bundle_a);
    assert_eq!(bundle.headers[CONTENT_TYPE], "application/gzip");
    assert_eq!(
        bundle.headers[CONTENT_LENGTH],
        bundle_a.len().to_string().as_str()
    );
    for path in [
        format!("/api/runner/web/{web_b}/{}", sha256_hex(bundle_b)),
        format!("/api/runner/web/{web_a}/{}", sha256_hex(bundle_b)),
        format!("/api/runner/web/{ext_a}/{}", sha256_hex(binary_a)),
        format!("/api/runner/web/{plain}/{}", sha256_hex(bundle_a)),
        format!(
            "/api/runner/web/{}/{}",
            Uuid::new_v4(),
            sha256_hex(bundle_a)
        ),
    ] {
        let refused = w.runner(&web_key_a, &path).await;
        assert_eq!(refused.status, 404, "{path}: {}", refused.json());
    }
    let opened = w
        .runner(&web_key_a, &format!("/api/runner/web/{web_a}/environment"))
        .await;
    assert_eq!(opened.status, 200);
    assert_eq!(opened.headers[CACHE_CONTROL], "no-store");
    assert_eq!(
        serde_json::from_slice::<Environment>(&opened.body).expect("an environment"),
        Environment {
            environment: [(
                "SITE_TOKEN".to_string(),
                format!("SITE_TOKEN-of-{}", w.a.slug)
            )]
            .into(),
        }
    );
    for path in [
        format!("/api/runner/web/{web_b}/environment"),
        format!("/api/runner/web/{ext_a}/environment"),
        format!("/api/runner/web/{plain}/environment"),
    ] {
        let refused = w.runner(&web_key_a, &path).await;
        assert_eq!(refused.status, 404, "{path}: {}", refused.json());
        assert!(!String::from_utf8_lossy(&refused.body).contains("TOKEN-of"));
    }
    for (key, path) in [
        (&web_key_a, "/api/runner/native".to_string()),
        (
            &web_key_a,
            format!("/api/runner/native/{ext_a}/{}", sha256_hex(binary_a)),
        ),
        (
            &web_key_a,
            format!("/api/runner/native/{ext_a}/environment"),
        ),
        (&key_a, "/api/runner/web".to_string()),
        (
            &key_a,
            format!("/api/runner/web/{web_a}/{}", sha256_hex(bundle_a)),
        ),
        (&key_a, format!("/api/runner/web/{web_a}/environment")),
    ] {
        let refused = w.runner(key, &path).await;
        assert_eq!(
            refused.status, 404,
            "a key reaches its own kind of runner's routes alone: {path}"
        );
        assert!(!String::from_utf8_lossy(&refused.body).contains("TOKEN-of"));
    }
    assert!(
        w.metric(&[("endpoint", "binary"), ("result", "not_found")])
            .await
            >= not_found_before + 7.0
    );
    for (endpoint, result) in [
        ("native", "ok"),
        ("binary", "ok"),
        ("environment", "ok"),
        ("environment", "not_found"),
        ("web", "ok"),
        ("bundle", "ok"),
        ("bundle", "not_found"),
        ("web_environment", "ok"),
        ("web_environment", "not_found"),
    ] {
        assert!(
            w.metric(&[("endpoint", endpoint), ("result", result)])
                .await
                >= 1.0,
            "{endpoint} {result}"
        );
    }

    let admin_key = w
        .send(
            Method::POST,
            "/api/platform/api-keys",
            &[("authorization", w.admin())],
            Some(&json!({"name": "ops"})),
        )
        .await
        .json()["key"]
        .as_str()
        .expect("an admin key")
        .to_string();
    let partner_key = w
        .send(
            Method::POST,
            &format!("/api/tenants/{}/partner-keys", w.a.slug),
            &[("authorization", w.operator(&w.a))],
            Some(&json!({"name": "partner", "scopes": ["books:ingest"]})),
        )
        .await
        .json()["key"]
        .as_str()
        .expect("a partner key")
        .to_string();
    let tenant_key = w.api_key(&w.a).await;
    let unauthorized_before = w
        .metric(&[("endpoint", "native"), ("result", "unauthorized")])
        .await;
    let refused: Vec<Vec<(&str, String)>> = vec![
        vec![],
        vec![("authorization", format!("ApiKey {tenant_key}"))],
        vec![("authorization", w.operator(&w.a))],
        vec![("authorization", format!("ApiKey {admin_key}"))],
        vec![
            ("authorization", format!("ApiKey {admin_key}")),
            ("x-tenant-slug", w.a.slug.clone()),
        ],
        vec![("authorization", w.admin())],
        vec![("x-api-key", partner_key.clone())],
        vec![("authorization", format!("RunnerKey {partner_key}"))],
        vec![("authorization", format!("ApiKey {key_a}"))],
        vec![("authorization", format!("Bearer {key_a}"))],
        vec![("authorization", format!("RunnerKey {}x", &key_a))],
    ];
    let web_unauthorized_before = w
        .metric(&[("endpoint", "web"), ("result", "unauthorized")])
        .await;
    for path in ["/api/runner/native", "/api/runner/web"] {
        for headers in &refused {
            let answered = w.send(Method::GET, path, headers, None).await;
            assert_eq!(
                answered.status,
                401,
                "{path} {headers:?}: {}",
                answered.json()
            );
        }
    }
    assert!(
        w.metric(&[("endpoint", "native"), ("result", "unauthorized")])
            .await
            >= unauthorized_before + refused.len() as f64
    );
    assert!(
        w.metric(&[("endpoint", "web"), ("result", "unauthorized")])
            .await
            >= web_unauthorized_before + refused.len() as f64
    );
    w.platform.shutdown().await.expect("a clean shutdown");
}

struct Family<'a> {
    name: &'a str,
    method: Method,
    path: String,
    body: Option<&'a Value>,
    admitted: (&'a str, String),
}

#[tokio::test]
async fn nothing_but_the_runner_api_accepts_a_runner_key() {
    let Some(w) = World::start().await else {
        return;
    };
    let route = |path: &str, posture: Value| json!({"method": "POST", "path": path, "handler": "echo", "posture": posture});
    let manifest = json!({
        "name": WASM,
        "version": "0.1.0",
        "mode": "wasm",
        "routing": "strict",
        "tables": [{"name": "items", "columns": [
            {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
            {"name": "title", "column_type": "text", "primary_key": false, "nullable": true, "auto_generate": false},
            {"name": "created_at", "column_type": "timestamp", "primary_key": false, "nullable": false, "auto_generate": true}
        ], "auth_required": true}],
        "custom_routes": [
            route("/me", json!({"kind": "authenticated"})),
            route("/ingest", json!({"kind": "api_key", "scope": "books:ingest"})),
        ],
        "publishes": [],
    });
    let deployed = w
        .platform
        .deploy(&w.a, &manifest, Some(&guest::module_bytes()))
        .await
        .expect("a deploy");
    assert!(deployed.is_success(), "{deployed:?}");
    let service = deployed.body["service_id"]
        .as_str()
        .expect("a service id")
        .to_string();
    let (_, runner_key) = w.mint(&w.a, "runner", "native").await;
    let (_, web_key) = w.mint(&w.a, "web-runner", "web").await;
    let partner_key = w
        .send(
            Method::POST,
            &format!("/api/tenants/{}/partner-keys", w.a.slug),
            &[("authorization", w.operator(&w.a))],
            Some(&json!({"name": "partner", "scopes": ["books:ingest"]})),
        )
        .await
        .json()["key"]
        .as_str()
        .expect("a partner key")
        .to_string();
    let svc = |path: &str| format!("/svc/{}/{WASM}{path}", w.a.slug);
    let body = json!({"title": "t"});

    let families = [
        Family {
            name: "a tenant route",
            method: Method::GET,
            path: "/api/services".into(),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "the caller's identity",
            method: Method::GET,
            path: "/api/auth/me".into(),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a deploy-authority route",
            method: Method::GET,
            path: format!("/api/services/{service}/config"),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a web app route",
            method: Method::GET,
            path: format!("/api/services/{service}/web/status"),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a role-administration route",
            method: Method::GET,
            path: format!("/api/tenants/{}/roles", w.a.id),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a platform-admin route",
            method: Method::GET,
            path: "/api/tenants".into(),
            body: None,
            admitted: ("authorization", w.admin()),
        },
        Family {
            name: "a runner-key administration route",
            method: Method::GET,
            path: format!("/api/tenants/{}/runner-keys", w.a.slug),
            body: None,
            admitted: ("authorization", w.admin()),
        },
        Family {
            name: "an authenticated custom route",
            method: Method::POST,
            path: svc("/me"),
            body: Some(&body),
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a CRUD route",
            method: Method::GET,
            path: svc("/items"),
            body: None,
            admitted: ("authorization", w.operator(&w.a)),
        },
        Family {
            name: "a partner-key route",
            method: Method::POST,
            path: svc("/ingest"),
            body: Some(&body),
            admitted: ("x-api-key", partner_key),
        },
    ];
    for Family {
        name: family,
        method,
        path,
        body,
        admitted,
    } in &families
    {
        let answered = w
            .send(method.clone(), path, std::slice::from_ref(admitted), *body)
            .await;
        assert!(
            (200..300).contains(&answered.status),
            "{family} admits its own credential: {} {}",
            answered.status,
            answered.json()
        );
        let presented: Vec<(&str, String)> = [&runner_key, &web_key]
            .into_iter()
            .flat_map(|key| {
                ["RunnerKey", "ApiKey", "Bearer"]
                    .map(|scheme| (AUTHORIZATION.as_str(), format!("{scheme} {key}")))
                    .into_iter()
                    .chain([("x-api-key", key.clone())])
            })
            .collect();
        for header in presented {
            let headers = vec![header, ("x-tenant-slug", w.a.slug.clone())];
            let answered = w.send(method.clone(), path, &headers, *body).await;
            assert_eq!(
                answered.status,
                401,
                "{family} refuses a runner key as {:?}: {}",
                headers[0],
                answered.json()
            );
        }
    }
    for (key, path) in [
        (&runner_key, "/api/runner/native"),
        (&web_key, "/api/runner/web"),
    ] {
        assert_eq!(
            w.runner(key, path).await.status,
            200,
            "the runner API of its kind accepts it: {path}"
        );
    }
    w.platform.shutdown().await.expect("a clean shutdown");
}
