use crate::error::TestkitError;
use crate::guest;
use crate::infra::{free_port, workspace_binary, ChildProcess, Tenant};
use serde_json::{json, Value};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct PlatformProcess(ChildProcess);

pub const SIGNING_KEY: &str = "cufflink-test-signing-key";

fn platform_binary() -> PathBuf {
    workspace_binary("cufflink-platform")
}

fn nats_or_unreachable(nats_url: Option<&str>) -> String {
    nats_url.map_or_else(
        || format!("nats://127.0.0.1:{}", free_port()),
        str::to_string,
    )
}

fn environment<'a>(
    database_url: &'a str,
    nats_url: &'a str,
    extra: &[(&'a str, &'a str)],
) -> Vec<(&'a str, &'a str)> {
    [
        ("DATABASE_URL", database_url),
        ("NATS_URL", nats_url),
        ("PLATFORM_HOST", "127.0.0.1"),
        ("KEYCLOAK_URL", ""),
        ("STORAGE_BACKEND", "memory"),
        ("RUST_LOG", "info"),
        ("CUFFLINK_CACHE_SIGNING_KEY", SIGNING_KEY),
    ]
    .into_iter()
    .chain(extra.iter().copied())
    .collect()
}

impl Deref for PlatformProcess {
    type Target = ChildProcess;

    fn deref(&self) -> &ChildProcess {
        &self.0
    }
}

impl DerefMut for PlatformProcess {
    fn deref_mut(&mut self) -> &mut ChildProcess {
        &mut self.0
    }
}

impl PlatformProcess {
    pub async fn start(database_url: &str, nats_url: Option<&str>, extra: &[(&str, &str)]) -> Self {
        let nats = nats_or_unreachable(nats_url);
        let env = environment(database_url, &nats, extra);
        Self(ChildProcess::spawn(&platform_binary(), "PLATFORM_PORT", &env).await)
    }

    pub async fn start_with(env: &[(&str, &str)]) -> Self {
        Self(ChildProcess::spawn(&platform_binary(), "PLATFORM_PORT", env).await)
    }

    pub fn launch(database_url: &str, nats_url: Option<&str>, extra: &[(&str, &str)]) -> Self {
        let nats = nats_or_unreachable(nats_url);
        let env = environment(database_url, &nats, extra);
        Self(ChildProcess::launch_unchecked(
            &platform_binary(),
            "PLATFORM_PORT",
            &env,
        ))
    }

    pub async fn deploy(&self, tenant: &Tenant, manifest: &Value) -> (u16, Value) {
        self.deploy_module(tenant, manifest, &guest::module_bytes())
            .await
    }

    pub async fn deploy_module(
        &self,
        tenant: &Tenant,
        manifest: &Value,
        wasm: &[u8],
    ) -> (u16, Value) {
        use base64::Engine;
        let wasm_base64 = base64::engine::general_purpose::STANDARD.encode(wasm);
        let response = self
            .http
            .post(format!("{}/api/services/deploy", self.base_url))
            .header("authorization", format!("ApiKey {}", tenant.api_key))
            .json(&json!({
                "manifest": manifest,
                "wasm_base64": wasm_base64,
                "api_version": 1,
            }))
            .send()
            .await
            .expect("deploy request");
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    pub async fn api_get(&self, tenant: &Tenant, path: &str) -> (u16, Value) {
        self.send(
            reqwest::Method::GET,
            path,
            &format!("ApiKey {}", tenant.api_key),
            None,
        )
        .await
    }

    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        authorization: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let request = self
            .http
            .request(method, format!("{}{path}", self.base_url))
            .header("authorization", authorization);
        let request = match body {
            Some(body) => request.json(body),
            None => request,
        };
        let response = request.send().await.expect("request");
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    pub fn fn_url(&self, tenant: &Tenant, service_name: &str, handler: &str) -> String {
        format!(
            "{}/svc/{}/{service_name}/_fn/{handler}",
            self.base_url, tenant.slug
        )
    }

    pub async fn call_fn(
        &self,
        tenant: &Tenant,
        service_name: &str,
        handler: &str,
    ) -> (u16, Value) {
        let response = self
            .http
            .post(self.fn_url(tenant, service_name, handler))
            .json(&json!({}))
            .send()
            .await
            .expect("fn request");
        let status = response.status().as_u16();
        let body = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }
}

pub fn wasm_manifest(service_name: &str, routes: &[(&str, &str)]) -> Value {
    let custom_routes: Vec<Value> = routes
        .iter()
        .map(|(handler, path)| {
            json!({
                "method": "POST",
                "path": path,
                "handler": handler,
                "posture": { "kind": "public", "reason": "integration test" },
            })
        })
        .collect();
    json!({
        "name": service_name,
        "version": "0.1.0",
        "mode": "wasm",
        "tables": [{
            "name": "items",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true, "nullable": false, "auto_generate": true},
                {"name": "title", "column_type": "text", "primary_key": false, "nullable": false, "auto_generate": false}
            ]
        }],
        "custom_routes": custom_routes,
        "publishes": [],
    })
}

pub async fn until(
    what: &str,
    within: Duration,
    mut reached: impl AsyncFnMut() -> bool,
) -> Result<(), TestkitError> {
    let deadline = Instant::now() + within;
    while !reached().await {
        if Instant::now() >= deadline {
            return Err(TestkitError::Never(what.to_string()));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

pub async fn until_loops(replicas: &[&PlatformProcess], wanted: f64) -> Result<(), TestkitError> {
    const WITHIN: Duration = Duration::from_secs(60);
    for replica in replicas {
        let mut running = None;
        until("its consumer loops", WITHIN, async || {
            running = replica.metric("cufflink_consumer_loops", &[]).await;
            running == Some(wanted)
        })
        .await
        .map_err(|_| {
            TestkitError::Never(format!(
                "{running:?} consumer loops run on a replica, not {wanted}"
            ))
        })?;
    }
    Ok(())
}
