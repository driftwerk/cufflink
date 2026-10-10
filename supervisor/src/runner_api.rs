use crate::ArtifactCap;
use cufflink_settings::Vars;
use cufflink_types::runner::{Environment, NativeSet, WebSet, RUNNER_NATIVE_PATH, RUNNER_WEB_PATH};
use cufflink_types::slug::{is_valid_tenant_slug, TENANT_SLUG_MAX};
use reqwest::header::AUTHORIZATION;
use reqwest::{Response, StatusCode};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

pub const KEY_VAR: &str = "CUFFLINK_RUNNER_KEY";
pub const KEY_FILE_VAR: &str = "CUFFLINK_RUNNER_KEY_FILE";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, PartialEq, Eq)]
pub enum RunnerKey {
    Value(String),
    File(PathBuf),
}

impl RunnerKey {
    pub fn configured(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<Option<Self>> {
        let set = |name: &str| vars.get(name).filter(|value| !value.trim().is_empty());
        match (set(KEY_VAR), set(KEY_FILE_VAR)) {
            (Some(key), None) => Ok(Some(Self::Value(key.trim().to_string()))),
            (None, Some(file)) => Ok(Some(Self::File(PathBuf::from(file)))),
            (Some(_), Some(_)) => Err(eyre::eyre!(
                "{KEY_VAR} and {KEY_FILE_VAR} are both set; set one"
            )),
            (None, None) => Ok(None),
        }
    }

    pub fn from_vars(vars: &Vars<impl Fn(&str) -> Option<String>>) -> eyre::Result<Self> {
        Self::configured(vars)?.ok_or_else(|| {
            eyre::eyre!(
                "Missing required environment variable: {KEY_VAR} (or {KEY_FILE_VAR}), the \
                 tenant's runner key (`cufflink tenants runner-keys create <tenant> --for \
                 <native|web>`)"
            )
        })
    }

    pub async fn read(&self) -> eyre::Result<String> {
        match self {
            Self::Value(key) => Ok(key.clone()),
            Self::File(file) => tokio::fs::read_to_string(file)
                .await
                .map(|key| key.trim().to_string())
                .map_err(|e| eyre::eyre!("cannot read {KEY_FILE_VAR} {}: {e}", file.display()))
                .and_then(|key| match key.is_empty() {
                    true => Err(eyre::eyre!("{KEY_FILE_VAR} {} is empty", file.display())),
                    false => Ok(key),
                }),
        }
    }
}

impl fmt::Debug for RunnerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(_) => f.write_str("RunnerKey::Value(..)"),
            Self::File(file) => f.debug_tuple("RunnerKey::File").field(file).finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerTenant {
    pub slug: String,
    pub setting: &'static str,
}

impl RunnerTenant {
    pub fn parse(setting: &'static str, raw: String) -> eyre::Result<Self> {
        match is_valid_tenant_slug(&raw) {
            true => Ok(Self { slug: raw, setting }),
            false => Err(eyre::eyre!(
                "{setting}: '{raw}' is not a tenant slug (1 to {TENANT_SLUG_MAX} of a-z, 0-9 and \
                 '-')"
            )),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "the runner key is tenant '{answered}''s, not {setting} '{tenant}''s; nothing runs until the \
     key and {setting} name one tenant"
)]
pub struct OtherTenant {
    setting: &'static str,
    tenant: String,
    answered: String,
}

trait Answered {
    fn tenant(&self) -> &str;
}

impl Answered for NativeSet {
    fn tenant(&self) -> &str {
        &self.tenant
    }
}

impl Answered for WebSet {
    fn tenant(&self) -> &str {
        &self.tenant
    }
}

pub fn platform_url(raw: String) -> eyre::Result<String> {
    let url = reqwest::Url::parse(&raw)
        .map_err(|e| eyre::eyre!("PLATFORM_URL: '{raw}' is not a URL: {e}"))?;
    match (url.scheme(), url.query(), url.fragment()) {
        ("http" | "https", None, None) => Ok(raw.trim_end_matches('/').to_string()),
        _ => Err(eyre::eyre!(
            "PLATFORM_URL: '{raw}' is not the http or https URL of the platform"
        )),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("the platform answered {status} to GET {path}{}", reason.as_ref().map(|reason| format!(": {reason}")).unwrap_or_default())]
pub struct Refused {
    pub status: StatusCode,
    path: String,
    reason: Option<String>,
}

pub fn key_refused(error: &eyre::Report) -> bool {
    error.downcast_ref::<OtherTenant>().is_some()
        || error
            .downcast_ref::<Refused>()
            .is_some_and(|refused| refused.status == StatusCode::UNAUTHORIZED)
}

#[derive(Clone)]
pub struct RunnerApi {
    http: reqwest::Client,
    url: String,
    key: RunnerKey,
    tenant: RunnerTenant,
    cap: ArtifactCap,
}

impl RunnerApi {
    pub fn new(
        url: &str,
        key: RunnerKey,
        tenant: RunnerTenant,
        cap: ArtifactCap,
    ) -> eyre::Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(READ_TIMEOUT)
                .build()?,
            url: url.to_string(),
            key,
            tenant,
            cap,
        })
    }

    pub fn tenant(&self) -> &RunnerTenant {
        &self.tenant
    }

    fn owned<S: Answered>(&self, set: S) -> eyre::Result<S> {
        match set.tenant() == self.tenant.slug {
            true => Ok(set),
            false => Err(OtherTenant {
                setting: self.tenant.setting,
                tenant: self.tenant.slug.clone(),
                answered: set.tenant().to_string(),
            }
            .into()),
        }
    }

    pub fn cap(&self) -> &ArtifactCap {
        &self.cap
    }

    async fn get(&self, path: &str) -> eyre::Result<Response> {
        let key = self.key.read().await?;
        let response = self
            .http
            .get(format!("{}{path}", self.url))
            .header(AUTHORIZATION, format!("RunnerKey {key}"))
            .send()
            .await?;
        match response.status() {
            status if status.is_success() => Ok(response),
            status => {
                let reason = response
                    .json::<serde_json::Value>()
                    .await
                    .ok()
                    .and_then(|body| body["error"].as_str().map(str::to_string));
                Err(Refused {
                    status,
                    path: path.to_string(),
                    reason,
                }
                .into())
            }
        }
    }

    async fn artifact(&self, path: &str, size: u64) -> eyre::Result<Vec<u8>> {
        self.cap.admit(path, size)?;
        let mut response = self.get(path).await?;
        if let Some(length) = response.content_length() {
            self.cap.admit(path, length)?;
        }
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            data.extend_from_slice(&chunk);
            self.cap.admit(path, data.len() as u64)?;
        }
        Ok(data)
    }

    async fn environment(&self, path: &str) -> eyre::Result<HashMap<String, String>> {
        let answer: Environment = self.get(path).await?.json().await?;
        Ok(answer.environment.into_iter().collect())
    }

    pub async fn native(&self) -> eyre::Result<NativeSet> {
        self.owned(
            self.get(RUNNER_NATIVE_PATH)
                .await?
                .json::<NativeSet>()
                .await?,
        )
    }

    pub async fn native_binary(
        &self,
        service_id: Uuid,
        sha256: &str,
        size: u64,
    ) -> eyre::Result<Vec<u8>> {
        self.artifact(&format!("{RUNNER_NATIVE_PATH}/{service_id}/{sha256}"), size)
            .await
    }

    pub async fn native_environment(
        &self,
        service_id: Uuid,
    ) -> eyre::Result<HashMap<String, String>> {
        self.environment(&format!("{RUNNER_NATIVE_PATH}/{service_id}/environment"))
            .await
    }

    pub async fn web(&self) -> eyre::Result<WebSet> {
        self.owned(self.get(RUNNER_WEB_PATH).await?.json::<WebSet>().await?)
    }

    pub async fn web_bundle(
        &self,
        service_id: Uuid,
        sha256: &str,
        size: u64,
    ) -> eyre::Result<Vec<u8>> {
        self.artifact(&format!("{RUNNER_WEB_PATH}/{service_id}/{sha256}"), size)
            .await
    }

    pub async fn web_environment(&self, service_id: Uuid) -> eyre::Result<HashMap<String, String>> {
        self.environment(&format!("{RUNNER_WEB_PATH}/{service_id}/environment"))
            .await
    }
}

#[cfg(target_os = "linux")]
pub fn forbid_inspection() -> eyre::Result<()> {
    // SAFETY: PR_SET_DUMPABLE takes plain integers and touches no memory.
    match unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } {
        0 => Ok(()),
        _ => Err(eyre::Report::new(std::io::Error::last_os_error())
            .wrap_err("cannot make the runner undumpable")),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn forbid_inspection() -> eyre::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::response::{IntoResponse, Response};
    use axum::routing::get;
    use axum::{Json, Router};
    use cufflink_types::runner::{NativeService, WebApp};
    use serde_json::json;
    use std::sync::Arc;

    const KEY: &str = "cfr_AAAAAAAAAAAA_s3cret";
    const SECRET: &str = "the-service-secret";

    #[derive(Clone)]
    struct Served {
        native: NativeSet,
        web: WebSet,
        artifact: Vec<u8>,
        length: Option<usize>,
    }

    fn authorized(headers: &HeaderMap) -> bool {
        headers.get("authorization").and_then(|v| v.to_str().ok())
            == Some(&format!("RunnerKey {KEY}"))
    }

    fn refused() -> Response {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid runner key"})),
        )
            .into_response()
    }

    async fn native(State(served): State<Arc<Served>>, headers: HeaderMap) -> Response {
        match authorized(&headers) {
            true => Json(served.native.clone()).into_response(),
            false => refused(),
        }
    }

    async fn web(State(served): State<Arc<Served>>, headers: HeaderMap) -> Response {
        match authorized(&headers) {
            true => Json(served.web.clone()).into_response(),
            false => refused(),
        }
    }

    async fn download(
        State(served): State<Arc<Served>>,
        headers: HeaderMap,
        Path((_, sha)): Path<(Uuid, String)>,
    ) -> Response {
        match (authorized(&headers), sha.as_str()) {
            (false, _) => refused(),
            (true, "missing") => {
                (StatusCode::NOT_FOUND, Json(json!({"error": "Not found"}))).into_response()
            }
            (true, _) => {
                let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
                    served.artifact.chunks(3).map(|c| Ok(c.to_vec())).collect();
                let body = Body::from_stream(futures_util::stream::iter(chunks));
                match served.length {
                    Some(length) => {
                        ([("content-length", length.to_string())], body).into_response()
                    }
                    None => body.into_response(),
                }
            }
        }
    }

    async fn opened(headers: HeaderMap) -> Response {
        match authorized(&headers) {
            true => Json(json!({"environment": {"TOKEN": SECRET}})).into_response(),
            false => (
                StatusCode::FORBIDDEN,
                Json(json!({"environment": {"TOKEN": SECRET}})),
            )
                .into_response(),
        }
    }

    async fn serve(artifact: &[u8], length: Option<usize>) -> String {
        let served = Arc::new(Served {
            native: NativeSet {
                tenant: "acme".into(),
                services: vec![NativeService {
                    service_id: Uuid::nil(),
                    service_name: "ext-pdf".into(),
                    version: 1,
                    sha256: Some("ab".into()),
                    size: Some(2),
                    extensions: vec!["pdf.render".into()],
                }],
            },
            web: WebSet {
                tenant: "acme".into(),
                apps: vec![WebApp {
                    service_id: Uuid::nil(),
                    service_name: "portal".into(),
                    version: 1,
                    sha256: Some("ab".into()),
                    size: Some(2),
                }],
            },
            artifact: artifact.to_vec(),
            length,
        });
        let app = [RUNNER_NATIVE_PATH, RUNNER_WEB_PATH]
            .into_iter()
            .fold(Router::new(), |router, path| {
                router
                    .route(&format!("{path}/{{service_id}}/environment"), get(opened))
                    .route(&format!("{path}/{{service_id}}/{{sha256}}"), get(download))
            })
            .route(RUNNER_NATIVE_PATH, get(native))
            .route(RUNNER_WEB_PATH, get(web))
            .with_state(served);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let url = format!("http://{}", listener.local_addr().expect("an address"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    fn acme() -> RunnerTenant {
        RunnerTenant::parse("EXT_TENANT", "acme".into()).expect("a slug")
    }

    fn api(url: &str, key: &str, max_bytes: u64) -> RunnerApi {
        RunnerApi::new(
            url,
            RunnerKey::Value(key.into()),
            acme(),
            ArtifactCap {
                max_bytes,
                setting: "EXT_ARTIFACT_MAX_BYTES",
            },
        )
        .expect("a client")
    }

    #[tokio::test]
    async fn every_set_artifact_and_environment_is_read_with_the_key() {
        let url = serve(b"\x7fELF binary", Some(11)).await;
        let api = api(&url, KEY, 1 << 20);
        assert_eq!(api.native().await.expect("the native set").tenant, "acme");
        assert_eq!(
            api.web().await.expect("the web set").apps[0].service_name,
            "portal"
        );
        let environment = HashMap::from([("TOKEN".to_string(), SECRET.to_string())]);
        assert_eq!(
            api.native_binary(Uuid::nil(), "ab", 11)
                .await
                .expect("the binary"),
            b"\x7fELF binary"
        );
        assert_eq!(
            api.web_bundle(Uuid::nil(), "ab", 11)
                .await
                .expect("the bundle"),
            b"\x7fELF binary"
        );
        assert_eq!(
            api.native_environment(Uuid::nil())
                .await
                .expect("the environment"),
            environment
        );
        assert_eq!(
            api.web_environment(Uuid::nil())
                .await
                .expect("the environment"),
            environment
        );
    }

    #[tokio::test]
    async fn a_refusal_says_why_and_never_what_it_carried() {
        let url = serve(b"bin", Some(3)).await;
        let wrong_key = api(&url, "cfr_AAAAAAAAAAAA_wrong", 1 << 20);
        let refused = wrong_key.native().await.expect_err("refused");
        assert_eq!(
            refused.to_string(),
            "the platform answered 401 Unauthorized to GET /api/runner/native: Invalid runner key"
        );
        assert!(key_refused(&refused));
        assert!(key_refused(&wrong_key.web().await.expect_err("refused")));
        let forbidden = wrong_key
            .native_environment(Uuid::nil())
            .await
            .expect_err("refused");
        assert!(!key_refused(&forbidden), "only a 401 refuses the key");
        let forbidden = forbidden.to_string();
        assert!(
            forbidden.starts_with("the platform answered 403"),
            "{forbidden}"
        );
        assert!(!forbidden.contains(SECRET), "{forbidden}");
        let missing = api(&url, KEY, 1 << 20)
            .native_binary(Uuid::nil(), "missing", 3)
            .await
            .expect_err("not found");
        assert!(!key_refused(&missing));
        assert!(missing.to_string().ends_with(": Not found"), "{missing}");
    }

    #[tokio::test]
    async fn a_set_of_another_tenant_refuses_the_key() {
        let url = serve(b"bin", Some(3)).await;
        let globex = RunnerApi::new(
            &url,
            RunnerKey::Value(KEY.into()),
            RunnerTenant::parse("WEB_TENANT", "globex".into()).expect("a slug"),
            ArtifactCap {
                max_bytes: 1 << 20,
                setting: "WEB_ARTIFACT_MAX_BYTES",
            },
        )
        .expect("a client");
        for refused in [
            globex.native().await.expect_err("acme's"),
            globex.web().await.expect_err("acme's"),
        ] {
            assert!(key_refused(&refused), "{refused}");
            assert_eq!(
                refused.to_string(),
                "the runner key is tenant 'acme''s, not WEB_TENANT 'globex''s; nothing runs \
                 until the key and WEB_TENANT name one tenant"
            );
        }
        assert_eq!(globex.tenant().slug, "globex");
    }

    #[test]
    fn a_runner_serves_a_tenant_its_setting_names_by_slug() {
        assert_eq!(
            acme(),
            RunnerTenant {
                slug: "acme".into(),
                setting: "EXT_TENANT"
            }
        );
        for raw in ["Acme", "acme,globex", "*", ""] {
            assert_eq!(
                RunnerTenant::parse("WEB_TENANT", raw.into())
                    .expect_err(raw)
                    .to_string(),
                format!(
                    "WEB_TENANT: '{raw}' is not a tenant slug (1 to {TENANT_SLUG_MAX} of a-z, \
                     0-9 and '-')"
                )
            );
        }
    }

    #[tokio::test]
    async fn an_artifact_past_the_cap_is_refused_before_and_while_it_arrives() {
        let artifact = b"0123456789";
        for (length, recorded) in [(Some(10), 4), (Some(10), 10), (None, 4), (None, 10)] {
            let url = serve(artifact, length).await;
            let refused = api(&url, KEY, 9)
                .native_binary(Uuid::nil(), "ab", recorded)
                .await
                .expect_err("too large")
                .to_string();
            assert!(
                refused.contains("bytes, more than 9 (EXT_ARTIFACT_MAX_BYTES)"),
                "{length:?} {recorded}: {refused}"
            );
        }
        let url = serve(artifact, None).await;
        assert_eq!(
            api(&url, KEY, 10)
                .web_bundle(Uuid::nil(), "ab", 10)
                .await
                .expect("at the cap"),
            artifact
        );
    }

    #[tokio::test]
    async fn neither_an_unreachable_platform_nor_an_unreadable_key_refuses_the_key() {
        let unreachable = api("http://127.0.0.1:1", KEY, 1 << 20)
            .native()
            .await
            .expect_err("unreachable");
        assert!(!key_refused(&unreachable));
        let unreadable = RunnerApi::new(
            "http://127.0.0.1:1",
            RunnerKey::File("/nonexistent/runner-key".into()),
            acme(),
            ArtifactCap {
                max_bytes: 1,
                setting: "EXT_ARTIFACT_MAX_BYTES",
            },
        )
        .expect("a client")
        .web()
        .await
        .expect_err("no key");
        assert!(!key_refused(&unreadable));
    }

    fn vars(pairs: &[(&str, &str)]) -> Vars<impl Fn(&str) -> Option<String>> {
        let pairs: HashMap<String, String> = pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Vars(move |name: &str| pairs.get(name).cloned())
    }

    #[test]
    fn a_key_is_a_value_or_a_file_never_both() {
        assert_eq!(
            RunnerKey::configured(&vars(&[(KEY_VAR, " cfr_AAAAAAAAAAAA_s3cret ")])).expect("a key"),
            Some(RunnerKey::Value("cfr_AAAAAAAAAAAA_s3cret".into()))
        );
        assert_eq!(
            RunnerKey::configured(&vars(&[(KEY_FILE_VAR, "/run/key"), (KEY_VAR, " ")]))
                .expect("a file"),
            Some(RunnerKey::File("/run/key".into()))
        );
        assert_eq!(RunnerKey::configured(&vars(&[])).expect("none"), None);
        assert_eq!(
            RunnerKey::configured(&vars(&[(KEY_VAR, "k"), (KEY_FILE_VAR, "/run/key")]))
                .expect_err("both")
                .to_string(),
            "CUFFLINK_RUNNER_KEY and CUFFLINK_RUNNER_KEY_FILE are both set; set one"
        );
        assert!(RunnerKey::from_vars(&vars(&[]))
            .expect_err("required")
            .to_string()
            .starts_with(
                "Missing required environment variable: CUFFLINK_RUNNER_KEY (or \
                 CUFFLINK_RUNNER_KEY_FILE)"
            ));
        assert!(
            !format!("{:?}", RunnerKey::Value("s3cret".into())).contains("s3cret"),
            "the key is never printed"
        );
    }

    #[tokio::test]
    async fn a_key_in_a_file_is_read_at_each_use() {
        let dir = tempfile::tempdir().expect("a directory");
        let file = dir.path().join("runner-key");
        let key = RunnerKey::File(file.clone());
        assert!(key
            .read()
            .await
            .expect_err("not there yet")
            .to_string()
            .starts_with("cannot read CUFFLINK_RUNNER_KEY_FILE"));
        std::fs::write(&file, " \n").expect("blank");
        assert!(key
            .read()
            .await
            .expect_err("blank")
            .to_string()
            .ends_with("is empty"));
        std::fs::write(&file, "cfr_AAAAAAAAAAAA_first\n").expect("a key");
        assert_eq!(key.read().await.expect("read"), "cfr_AAAAAAAAAAAA_first");
        std::fs::write(&file, "cfr_AAAAAAAAAAAA_rotated").expect("rotated");
        assert_eq!(key.read().await.expect("read"), "cfr_AAAAAAAAAAAA_rotated");
        assert_eq!(
            RunnerKey::Value("k".into()).read().await.expect("value"),
            "k"
        );
    }

    #[test]
    fn the_platform_url_is_an_http_url_without_query() {
        assert_eq!(
            platform_url("http://platform:8080/".into()).expect("a URL"),
            "http://platform:8080"
        );
        assert_eq!(
            platform_url("https://platform.internal".into()).expect("a URL"),
            "https://platform.internal"
        );
        for url in ["platform:8080", "ftp://platform", "http://platform/?x=1"] {
            assert!(
                platform_url(url.into())
                    .expect_err(url)
                    .to_string()
                    .starts_with(&format!("PLATFORM_URL: '{url}'")),
                "{url}"
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_runner_can_make_itself_undumpable() {
        forbid_inspection().expect("undumpable");
        // SAFETY: PR_GET_DUMPABLE reads a flag of this process.
        assert_eq!(unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) }, 0);
    }
}
