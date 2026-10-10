use super::util::{ended, exchange};
use super::{
    block_on_within, budget_cap, clamp_to_budget, duration_from_env, fanout_concurrency,
    read_guest_bytes, read_guest_string, wrap, BatchResult, HostState,
};
use crate::egress::{
    EgressDenied, EgressError, EgressGuard, EgressPolicy, EgressTarget, OutboundRequest,
};
use crate::policy::PolicySet;
use cufflink_types::host::http::{
    FetchRequestV2, FetchResponseV2, HttpErrorEnvelope, HttpErrorKind, MultipartPart,
};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING,
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use wasmtime::{Caller, Linker};

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "http_fetch",
        |mut caller: Caller<'_, HostState>,
         method_ptr: i32,
         method_len: i32,
         url_ptr: i32,
         url_len: i32,
         headers_ptr: i32,
         headers_len: i32,
         body_ptr: i32,
         body_len: i32|
         -> i32 {
            http_fetch_host(
                &mut caller,
                method_ptr,
                method_len,
                url_ptr,
                url_len,
                headers_ptr,
                headers_len,
                body_ptr,
                body_len,
                None,
            )
        },
    )?;

    wrap(
        linker,
        names,
        "http_fetch_timeout",
        |mut caller: Caller<'_, HostState>,
         method_ptr: i32,
         method_len: i32,
         url_ptr: i32,
         url_len: i32,
         headers_ptr: i32,
         headers_len: i32,
         body_ptr: i32,
         body_len: i32,
         timeout_ms: i64|
         -> i32 {
            let requested = u64::try_from(timeout_ms)
                .ok()
                .filter(|ms| *ms > 0)
                .map(std::time::Duration::from_millis);
            http_fetch_host(
                &mut caller,
                method_ptr,
                method_len,
                url_ptr,
                url_len,
                headers_ptr,
                headers_len,
                body_ptr,
                body_len,
                requested,
            )
        },
    )?;

    wrap(
        linker,
        names,
        "http_fetch_many",
        |mut caller: Caller<'_, HostState>, items_ptr: i32, items_len: i32| -> i32 {
            let items_json = host_try!(read_guest_string(
                &mut caller,
                "http_fetch_many",
                items_ptr,
                items_len
            ));
            let mut items: Vec<HttpItem> = match serde_json::from_str(&items_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("[wasm] http_fetch_many: bad input: {}", e);
                    return -1;
                }
            };
            let _span = tracing::info_span!(
                "http.fetch_many",
                otel.kind = "client",
                http.request_count = items.len()
            )
            .entered();
            for item in &mut items {
                item.headers.retain(|k, _| {
                    !k.eq_ignore_ascii_case("traceparent") && !k.eq_ignore_ascii_case("tracestate")
                });
                crate::telemetry::inject_current_context(&mut item.headers);
            }

            let default_timeout = outbound_http_timeout();
            let remaining = caller.data().remaining_budget();
            let egress = caller.data().guest_egress();
            let cancel = caller.data().cancel.clone();

            let result = block_on_within(budget_cap(remaining), &cancel, async {
                let permits = fanout_concurrency();
                let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(permits));
                let egress = &egress;
                let futs = items.into_iter().map(|item| {
                    let sem = sem.clone();
                    let timeout = clamp_to_budget(
                        item.timeout_ms.map(std::time::Duration::from_millis),
                        default_timeout,
                        remaining,
                    );
                    async move {
                        let _p = sem.acquire_owned().await.ok();
                        let body = item.body.map(String::into_bytes);
                        match egress
                            .fetch(&item.method, item.url, &item.headers, body, timeout)
                            .await
                        {
                            Ok(response) => BatchResult::ok(response),
                            Err(e) => BatchResult::err(&e),
                        }
                    }
                });
                let results: Vec<BatchResult> = futures_util::future::join_all(futs)
                    .await
                    .into_iter()
                    .collect();
                Ok::<String, eyre::Report>(serde_json::to_string(&results)?)
            });

            match result {
                Ok(json) => {
                    caller.data_mut().host_response = Some(json);
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] http_fetch_many error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

#[derive(serde::Deserialize)]
struct HttpItem {
    method: String,
    url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

const OUTBOUND_HTTP_TIMEOUT_SECS: u64 = 28;

const OUTBOUND_CONNECT_TIMEOUT_SECS: u64 = 20;

const _: () = assert!(OUTBOUND_CONNECT_TIMEOUT_SECS > 5);

pub(super) fn outbound_http_timeout() -> std::time::Duration {
    duration_from_env(
        "HOST_OUTBOUND_HTTP_TIMEOUT_SECS",
        OUTBOUND_HTTP_TIMEOUT_SECS,
    )
}

pub fn outbound_connect_timeout() -> std::time::Duration {
    duration_from_env(
        "HOST_OUTBOUND_CONNECT_TIMEOUT_SECS",
        OUTBOUND_CONNECT_TIMEOUT_SECS,
    )
}

#[allow(clippy::too_many_arguments)]
fn http_fetch_host(
    caller: &mut Caller<'_, HostState>,
    method_ptr: i32,
    method_len: i32,
    url_ptr: i32,
    url_len: i32,
    headers_ptr: i32,
    headers_len: i32,
    body_ptr: i32,
    body_len: i32,
    requested_timeout: Option<std::time::Duration>,
) -> i32 {
    let method = host_try!(read_guest_string(
        caller,
        "http_fetch",
        method_ptr,
        method_len
    ));
    let url = host_try!(read_guest_string(caller, "http_fetch", url_ptr, url_len));
    let headers_json = host_try!(read_guest_string(
        caller,
        "http_fetch",
        headers_ptr,
        headers_len
    ));
    let mut headers: HashMap<String, String> =
        serde_json::from_str(&headers_json).unwrap_or_default();
    let _span = tracing::info_span!(
        "http.fetch",
        otel.kind = "client",
        http.method = %method,
        server = %server(&url)
    )
    .entered();
    headers.retain(|k, _| {
        !k.eq_ignore_ascii_case("traceparent") && !k.eq_ignore_ascii_case("tracestate")
    });
    crate::telemetry::inject_current_context(&mut headers);

    let body = if body_len > 0 {
        Some(host_try!(read_guest_bytes(
            caller,
            "http_fetch",
            body_ptr,
            body_len
        )))
    } else {
        None
    };

    let timeout = clamp_to_budget(
        requested_timeout,
        outbound_http_timeout(),
        caller.data().remaining_budget(),
    );

    let egress = caller.data().guest_egress();
    let result = block_on_within(
        timeout,
        &caller.data().cancel,
        egress.fetch(&method, url, &headers, body, timeout),
    )
    .map(|response| response.to_string());

    match result {
        Ok(json_str) => {
            caller.data_mut().host_response = Some(json_str);
            0
        }
        Err(e) => {
            tracing::error!("[wasm] http_fetch error: {}", e);
            -1
        }
    }
}

fn server(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_default()
}

pub(crate) struct GuestEgress {
    guard: EgressGuard,
    policies: Arc<PolicySet>,
    tenant_slug: String,
}

struct Exchanged {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl GuestEgress {
    fn policy(&self) -> EgressPolicy<'_> {
        EgressPolicy::new(&self.policies, &self.tenant_slug)
    }

    async fn exchange(
        &self,
        request: OutboundRequest,
        follow_redirects: bool,
        max_bytes: u64,
    ) -> Result<Exchanged, EgressError> {
        let policy = self.policy();
        let check = |target: &EgressTarget| policy.check(target);
        let response = self.guard.send(request, &check, follow_redirects).await?;
        Ok(Exchanged {
            status: response.status().as_u16(),
            headers: response.headers().clone(),
            body: EgressGuard::read_body(response, max_bytes).await?,
        })
    }

    async fn fetch(
        &self,
        method: &str,
        url: String,
        headers: &HashMap<String, String>,
        body: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Value, String> {
        let method = match method.to_uppercase().as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            "PATCH" => reqwest::Method::PATCH,
            _ => return Err(format!("Unsupported HTTP method: {}", method)),
        };
        let headers = headers
            .iter()
            .map(|(name, value)| header(name, value))
            .collect::<Result<HeaderMap, String>>()?;
        let request = OutboundRequest {
            method,
            url,
            headers,
            body,
            timeout,
        };
        let max_bytes = self
            .guard
            .effective_max_bytes(self.policy().max_response_bytes());
        let Exchanged {
            status,
            headers,
            body,
        } = self
            .exchange(request, true, max_bytes)
            .await
            .map_err(|e| e.to_string())?;
        let response_headers: HashMap<String, String> = headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let content_type = response_headers
            .get("content-type")
            .map(String::as_str)
            .unwrap_or("");
        let is_binary = content_type.starts_with("image/")
            || content_type.starts_with("audio/")
            || content_type.starts_with("video/")
            || content_type == "application/octet-stream"
            || content_type == "application/pdf";
        let (body, encoding) = if is_binary {
            use base64::Engine;
            (
                base64::engine::general_purpose::STANDARD.encode(&body),
                "base64",
            )
        } else {
            (String::from_utf8_lossy(&body).into_owned(), "utf8")
        };
        Ok(json!({
            "status": status,
            "body": body,
            "body_encoding": encoding,
            "headers": response_headers,
        }))
    }

    async fn fetch_v2(
        &self,
        plan: Plan,
        limits: &FetchLimits,
    ) -> Result<FetchResponseV2, HttpErrorEnvelope> {
        let max_bytes = response_cap(
            plan.max_response_bytes,
            self.guard.effective_max_bytes(None),
            limits.max_response_bytes,
            self.policy().max_response_bytes(),
        );
        self.exchange(plan.request, plan.follow_redirects, max_bytes)
            .await
            .map(|exchanged| FetchResponseV2::Ok {
                status: exchanged.status,
                headers: exchanged
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect(),
                body: exchanged.body,
            })
            .map_err(|e| failure(&e))
    }
}

fn header(name: &str, value: &str) -> Result<(HeaderName, HeaderValue), String> {
    Ok((
        HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("invalid header name {name}: {e}"))?,
        HeaderValue::from_str(value)
            .map_err(|e| format!("invalid header value for {name}: {e}"))?,
    ))
}

impl HostState {
    pub(crate) fn guest_egress(&self) -> GuestEgress {
        GuestEgress {
            guard: self.app_state.egress.clone(),
            policies: Arc::clone(&self.policies),
            tenant_slug: self.identity.tenant_slug.clone(),
        }
    }
}

pub(super) fn register_v2(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "http_fetch_v2",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            exchange(
                &mut caller,
                "http_fetch_v2",
                (ptr, len),
                |reason| recorded(Err(invalid(reason))),
                |state, request| recorded(fetch_v2(state, request)),
            )
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchLimits {
    pub max_request_bytes: usize,
    pub max_response_bytes: u64,
}

impl Default for FetchLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 32 * 1024 * 1024,
            max_response_bytes: 50 * 1024 * 1024,
        }
    }
}

impl FetchLimits {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        Ok(Self {
            max_request_bytes: vars
                .positive("HOST_HTTP_MAX_REQUEST_BYTES", defaults.max_request_bytes)?,
            max_response_bytes: vars.positive(
                "HOST_HTTP_V2_MAX_RESPONSE_BYTES",
                defaults.max_response_bytes,
            )?,
        })
    }
}

#[derive(Debug)]
struct Plan {
    request: OutboundRequest,
    follow_redirects: bool,
    max_response_bytes: Option<u64>,
}

fn error(kind: HttpErrorKind, message: impl Into<String>) -> HttpErrorEnvelope {
    HttpErrorEnvelope {
        kind,
        message: message.into(),
    }
}

fn invalid(message: impl Into<String>) -> HttpErrorEnvelope {
    error(HttpErrorKind::InvalidRequest, message)
}

fn fetch_v2(
    state: &HostState,
    request: FetchRequestV2,
) -> Result<FetchResponseV2, HttpErrorEnvelope> {
    let _span = tracing::info_span!(
        "http.fetch",
        otel.kind = "client",
        http.method = %request.method,
        server = %server(&request.url)
    )
    .entered();
    let limits = state.app_state.config.fetch;
    let timeout = clamp_to_budget(
        request
            .timeout_ms
            .filter(|ms| *ms > 0)
            .map(|ms| Duration::from_millis(u64::from(ms))),
        outbound_http_timeout(),
        state.remaining_budget(),
    );
    let plan = plan(request, &limits, &boundary(), timeout).map(traced)?;
    block_on_within(
        timeout,
        &state.cancel,
        state.guest_egress().fetch_v2(plan, &limits),
    )
    .map_err(|e| ended(e, |waited| error(HttpErrorKind::Timeout, waited)))
}

fn recorded(outcome: Result<FetchResponseV2, HttpErrorEnvelope>) -> FetchResponseV2 {
    let result = match outcome.as_ref().map_err(|e| e.kind) {
        Ok(_) => "ok",
        Err(HttpErrorKind::Denied) => "denied",
        Err(HttpErrorKind::Timeout) => "timeout",
        Err(HttpErrorKind::TooLarge) => "too_large",
        Err(_) => "error",
    };
    metrics::counter!("cufflink_host_http_v2_total", "result" => result).increment(1);
    outcome.unwrap_or_else(FetchResponseV2::Error)
}

fn traced(mut plan: Plan) -> Plan {
    let mut carrier = HashMap::new();
    crate::telemetry::inject_current_context(&mut carrier);
    let headers = &mut plan.request.headers;
    ["traceparent", "tracestate"].iter().for_each(|name| {
        headers.remove(*name);
    });
    headers.extend(
        carrier
            .iter()
            .filter_map(|(name, value)| header(name, value).ok()),
    );
    plan
}

fn boundary() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

fn plan(
    request: FetchRequestV2,
    limits: &FetchLimits,
    boundary: &str,
    timeout: Duration,
) -> Result<Plan, HttpErrorEnvelope> {
    let method = request.method.to_ascii_uppercase();
    let method = FetchRequestV2::METHODS
        .contains(&method.as_str())
        .then(|| reqwest::Method::from_bytes(method.as_bytes()).ok())
        .flatten()
        .ok_or_else(|| invalid(format!("unsupported HTTP method: {}", request.method)))?;
    let mut headers = request
        .headers
        .iter()
        .map(|(name, value)| header(name, value).map_err(invalid))
        .try_fold(HeaderMap::new(), |mut headers, entry| {
            entry.map(|(name, value)| {
                headers.append(name, value);
                headers
            })
        })?;
    [CONTENT_LENGTH, TRANSFER_ENCODING].iter().for_each(|name| {
        headers.remove(name);
    });
    let too_large = |size: usize| {
        error(
            HttpErrorKind::TooLarge,
            format!(
                "request body of {size} bytes exceeds {} bytes",
                limits.max_request_bytes
            ),
        )
    };
    let within = |size: usize| (size <= limits.max_request_bytes).then_some(size);
    let body = match (request.body, request.multipart) {
        (Some(_), Some(_)) => return Err(invalid("body and multipart are mutually exclusive")),
        (body, None) => body,
        (None, Some(parts)) => {
            let content: usize = parts.iter().map(|part| part.body.len()).sum();
            within(content).ok_or_else(|| too_large(content))?;
            let content_type =
                HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}"))
                    .map_err(|e| invalid(format!("invalid boundary: {e}")))?;
            headers.insert(CONTENT_TYPE, content_type);
            Some(form(&parts, boundary)?)
        }
    };
    let size = body.as_ref().map_or(0, Vec::len);
    within(size).ok_or_else(|| too_large(size))?;
    Ok(Plan {
        request: OutboundRequest {
            method,
            url: request.url,
            headers,
            body,
            timeout,
        },
        follow_redirects: request.follow_redirects,
        max_response_bytes: request.max_response_bytes,
    })
}

fn form(parts: &[MultipartPart], boundary: &str) -> Result<Vec<u8>, HttpErrorEnvelope> {
    parts
        .iter()
        .try_fold(Vec::new(), |mut body, part| {
            body.extend_from_slice(part_head(part, boundary)?.as_bytes());
            body.extend_from_slice(&part.body);
            body.extend_from_slice(b"\r\n");
            Ok(body)
        })
        .map(|mut body| {
            body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
            body
        })
}

fn part_head(part: &MultipartPart, boundary: &str) -> Result<String, HttpErrorEnvelope> {
    let name = quoted("name", &part.name)?;
    let filename = part
        .filename
        .as_deref()
        .map(|filename| quoted("filename", filename))
        .transpose()?
        .map(|filename| format!("; filename=\"{filename}\""))
        .unwrap_or_default();
    let content_type = part
        .content_type
        .as_deref()
        .map(|content_type| {
            HeaderValue::from_str(content_type)
                .map(|_| format!("Content-Type: {content_type}\r\n"))
                .map_err(|_| invalid(format!("invalid content type of part {}", part.name)))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"{filename}\r\n{content_type}\r\n"
    ))
}

fn quoted(parameter: &str, value: &str) -> Result<String, HttpErrorEnvelope> {
    value
        .chars()
        .map(|c| match c {
            '"' => Ok("%22".to_string()),
            '\r' => Ok("%0D".to_string()),
            '\n' => Ok("%0A".to_string()),
            c if c.is_control() => Err(invalid(format!(
                "multipart {parameter} holds a control character"
            ))),
            c => Ok(c.to_string()),
        })
        .collect()
}

fn response_cap(requested: Option<u64>, legacy: u64, ceiling: u64, tenant: Option<u64>) -> u64 {
    let cap = requested.unwrap_or(legacy).min(ceiling);
    tenant.map_or(cap, |tenant| cap.min(tenant))
}

fn failure(e: &EgressError) -> HttpErrorEnvelope {
    let kind = match e {
        EgressError::Denied(EgressDenied::InvalidUrl(_) | EgressDenied::UnsupportedScheme(_)) => {
            HttpErrorKind::InvalidRequest
        }
        EgressError::Denied(EgressDenied::MetadataEndpoint(_)) | EgressError::Policy(_) => {
            HttpErrorKind::Denied
        }
        EgressError::Denied(EgressDenied::Unresolvable(_)) => HttpErrorKind::Dns,
        EgressError::Denied(EgressDenied::BodyTooLarge(_)) => HttpErrorKind::TooLarge,
        EgressError::TooManyRedirects => HttpErrorKind::Redirect,
        EgressError::TimedOut(_) => HttpErrorKind::Timeout,
        EgressError::Http(e) if e.is_timeout() => HttpErrorKind::Timeout,
        EgressError::Http(e) if e.is_connect() && is_tls(e) => HttpErrorKind::Tls,
        EgressError::Http(e) if e.is_connect() => HttpErrorKind::Connect,
        EgressError::Http(e) if e.is_redirect() => HttpErrorKind::Redirect,
        EgressError::Http(e) if e.is_builder() => HttpErrorKind::InvalidRequest,
        EgressError::Http(_) => HttpErrorKind::Io,
    };
    error(kind, chain(e))
}

fn sources<'a>(
    e: &'a (dyn std::error::Error + 'static),
) -> impl Iterator<Item = &'a (dyn std::error::Error + 'static)> {
    std::iter::successors(Some(e), |e| e.source())
}

fn is_tls(e: &(dyn std::error::Error + 'static)) -> bool {
    e.is::<rustls::Error>()
        || e.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .is_some_and(|wrapped| is_tls(wrapped))
        || e.source().is_some_and(is_tls)
}

fn chain(e: &(dyn std::error::Error + 'static)) -> String {
    sources(e)
        .map(ToString::to_string)
        .fold(Vec::<String>::new(), |mut seen, message| {
            if seen.last().is_none_or(|last| !last.contains(&message)) {
                seen.push(message);
            }
            seen
        })
        .join(": ")
}

#[cfg(test)]
mod v2_tests {
    use super::*;
    use axum::extract::RawQuery;
    use axum::http::StatusCode;
    use axum::routing::{any, get};
    use axum::Router;
    use serde_json::json;

    const BOUNDARY: &str = "cufflink-test-boundary";
    const TIMEOUT: Duration = Duration::from_secs(5);

    fn request(method: &str, url: &str) -> FetchRequestV2 {
        FetchRequestV2 {
            method: method.into(),
            url: url.into(),
            ..Default::default()
        }
    }

    fn planned(request: FetchRequestV2) -> Result<Plan, HttpErrorEnvelope> {
        plan(request, &FetchLimits::default(), BOUNDARY, TIMEOUT)
    }

    fn refused(request: FetchRequestV2) -> HttpErrorKind {
        planned(request).expect_err("refused").kind
    }

    fn part(name: &str, body: &[u8]) -> MultipartPart {
        MultipartPart {
            name: name.into(),
            body: body.to_vec(),
            ..Default::default()
        }
    }

    fn file(name: &str, filename: &str, content_type: &str, body: &[u8]) -> MultipartPart {
        MultipartPart {
            filename: Some(filename.into()),
            content_type: Some(content_type.into()),
            ..part(name, body)
        }
    }

    fn egress(policy: &[(&str, &str)]) -> GuestEgress {
        let policies = PolicySet::platform_defaults_from(|name| {
            policy
                .iter()
                .find(|(var, _)| *var == name)
                .map(|(_, value)| value.to_string())
        })
        .expect("policies");
        GuestEgress {
            guard: EgressGuard::new(1024, Duration::from_secs(2)),
            policies: Arc::new(policies),
            tenant_slug: "harbor".into(),
        }
    }

    async fn serve(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let base = format!("http://{}", listener.local_addr().expect("addr"));
        tokio::spawn(async move { axum::serve(listener, app).await });
        base
    }

    async fn fetched(
        egress: &GuestEgress,
        request: FetchRequestV2,
    ) -> Result<FetchResponseV2, HttpErrorEnvelope> {
        let limits = FetchLimits {
            max_response_bytes: 4096,
            ..FetchLimits::default()
        };
        let plan = plan(request, &limits, BOUNDARY, Duration::from_secs(2))?;
        egress.fetch_v2(plan, &limits).await
    }

    async fn failed(egress: &GuestEgress, request: FetchRequestV2) -> HttpErrorKind {
        fetched(egress, request).await.expect_err("failed").kind
    }

    #[test]
    fn the_method_is_one_of_the_seven() {
        for method in FetchRequestV2::METHODS {
            let plan = planned(request(method, "https://a.example/")).expect(method);
            assert_eq!(plan.request.method.as_str(), method);
            let lower = planned(request(&method.to_lowercase(), "https://a.example/"));
            assert_eq!(lower.expect(method).request.method.as_str(), method);
        }
        for method in ["TRACE", "CONNECT", "", "GET /"] {
            assert_eq!(
                refused(request(method, "https://a.example/")),
                HttpErrorKind::InvalidRequest,
                "{method}"
            );
        }
    }

    #[test]
    fn a_request_has_a_body_or_parts_never_both() {
        let both = FetchRequestV2 {
            body: Some(b"raw".to_vec()),
            multipart: Some(vec![part("a", b"b")]),
            ..request("POST", "https://a.example/")
        };
        assert_eq!(refused(both), HttpErrorKind::InvalidRequest);
        let raw = planned(FetchRequestV2 {
            body: Some(vec![0, 159, 255]),
            ..request("POST", "https://a.example/")
        })
        .expect("raw");
        assert_eq!(raw.request.body.as_deref(), Some(&[0u8, 159, 255][..]));
        assert!(raw.request.headers.is_empty());
        let none = planned(request("GET", "https://a.example/")).expect("none");
        assert_eq!(none.request.body, None);
        assert!(!none.follow_redirects);
        assert_eq!(none.request.timeout, TIMEOUT);
    }

    #[test]
    fn the_platform_frames_every_body() {
        let framed = |body: Option<Vec<u8>>| {
            planned(FetchRequestV2 {
                headers: vec![
                    ("Content-Length".into(), "3".into()),
                    ("transfer-encoding".into(), "chunked".into()),
                    ("content-type".into(), "application/pdf".into()),
                    ("x-kept".into(), "1".into()),
                ],
                body,
                ..request("POST", "https://a.example/")
            })
            .expect("plan")
            .request
            .headers
        };
        for headers in [framed(Some(b"raw".to_vec())), framed(None)] {
            assert!(!headers.contains_key(CONTENT_LENGTH));
            assert!(!headers.contains_key(TRANSFER_ENCODING));
            assert_eq!(headers[CONTENT_TYPE], "application/pdf");
            assert_eq!(headers["x-kept"], "1");
        }
    }

    #[test]
    fn a_span_names_the_host_alone() {
        assert_eq!(
            server("https://user:secret@api.example:8443/v1?k=v"),
            "api.example"
        );
        assert_eq!(server("http://127.0.0.1:3000/forms"), "127.0.0.1");
        assert_eq!(server("http://[::1]:8080/"), "[::1]");
        assert_eq!(server("not a url"), "");
        assert_eq!(server(""), "");
    }

    #[test]
    fn headers_keep_their_repeats_and_are_validated() {
        let plan = planned(FetchRequestV2 {
            headers: vec![
                ("Accept".into(), "application/pdf".into()),
                ("X-Tag".into(), "a".into()),
                ("x-tag".into(), "b".into()),
            ],
            ..request("GET", "https://a.example/")
        })
        .expect("plan");
        let tags: Vec<_> = plan.request.headers.get_all("x-tag").iter().collect();
        assert_eq!(tags, ["a", "b"]);
        assert_eq!(plan.request.headers["accept"], "application/pdf");
        for (name, value) in [("bad name", "v"), ("x-a", "line\r\nbreak"), ("", "v")] {
            assert_eq!(
                refused(FetchRequestV2 {
                    headers: vec![(name.into(), value.into())],
                    ..request("GET", "https://a.example/")
                }),
                HttpErrorKind::InvalidRequest,
                "{name:?}"
            );
        }
    }

    #[test]
    fn the_pdf_renderer_form_is_framed_as_the_http_client_frames_it() {
        let html = "<style>@page { margin: 0; }</style><h1>Receipt</h1>";
        let plan = planned(FetchRequestV2 {
            headers: vec![
                (
                    "content-type".into(),
                    "multipart/form-data; boundary=x".into(),
                ),
                ("content-length".into(), "3".into()),
            ],
            multipart: Some(vec![
                part("emulatedMediaType", b"print"),
                part("printBackground", b"true"),
                part("waitDelay", b"1s"),
                file("files", "index.html", "text/html", html.as_bytes()),
            ]),
            ..request(
                "POST",
                "https://pdf.example.com/forms/chromium/convert/html",
            )
        })
        .expect("plan");
        assert_eq!(
            plan.request.headers[CONTENT_TYPE],
            format!("multipart/form-data; boundary={BOUNDARY}")
        );
        assert_eq!(plan.request.headers.get_all(CONTENT_TYPE).iter().count(), 1);
        assert!(!plan.request.headers.contains_key(CONTENT_LENGTH));
        let expected = format!(
            "--{BOUNDARY}\r\n\
             Content-Disposition: form-data; name=\"emulatedMediaType\"\r\n\r\n\
             print\r\n\
             --{BOUNDARY}\r\n\
             Content-Disposition: form-data; name=\"printBackground\"\r\n\r\n\
             true\r\n\
             --{BOUNDARY}\r\n\
             Content-Disposition: form-data; name=\"waitDelay\"\r\n\r\n\
             1s\r\n\
             --{BOUNDARY}\r\n\
             Content-Disposition: form-data; name=\"files\"; filename=\"index.html\"\r\n\
             Content-Type: text/html\r\n\r\n\
             {html}\r\n\
             --{BOUNDARY}--\r\n"
        );
        assert_eq!(
            String::from_utf8(plan.request.body.expect("body")).expect("utf8"),
            expected
        );
    }

    #[test]
    fn a_form_of_no_parts_is_its_closing_boundary() {
        assert_eq!(
            form(&[], BOUNDARY).expect("form"),
            format!("--{BOUNDARY}--\r\n").as_bytes()
        );
    }

    #[test]
    fn no_part_can_end_its_header_or_its_quotes() {
        let head = part_head(
            &MultipartPart {
                filename: Some("a\"; name=\"evil\r\nX-Injected: 1\n.pdf".into()),
                ..part("fi\"les", b"")
            },
            BOUNDARY,
        )
        .expect("head");
        assert_eq!(
            head,
            format!(
                "--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"fi%22les\"; \
                 filename=\"a%22; name=%22evil%0D%0AX-Injected: 1%0A.pdf\"\r\n\r\n"
            )
        );
        assert_eq!(quoted("name", "quöte").expect("unicode"), "quöte");
        for (name, filename, content_type) in [
            ("a\0b", None, None),
            ("a", Some("b\u{7f}"), None),
            ("a", None, Some("text/html\r\nX-Injected: 1")),
            ("a", None, Some("text/\0")),
        ] {
            let part = MultipartPart {
                filename: filename.map(str::to_string),
                content_type: content_type.map(str::to_string),
                ..part(name, b"")
            };
            assert_eq!(
                part_head(&part, BOUNDARY).expect_err("refused").kind,
                HttpErrorKind::InvalidRequest,
                "{part:?}"
            );
        }
    }

    #[test]
    fn a_part_that_holds_a_boundary_of_its_own_stays_inside_its_part() {
        let smuggled =
            b"--other--\r\n--other\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\n";
        let body = form(&[part("a", smuggled)], BOUNDARY).expect("form");
        let text = String::from_utf8(body).expect("utf8");
        assert_eq!(text.matches(BOUNDARY).count(), 2);
        assert!(text.ends_with(&format!("\r\n--{BOUNDARY}--\r\n")));
        let (first, second) = (boundary(), boundary());
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn request_bodies_are_capped() {
        let limits = FetchLimits {
            max_request_bytes: 8,
            ..FetchLimits::default()
        };
        let sized = |body: Option<Vec<u8>>, multipart: Option<Vec<MultipartPart>>| {
            plan(
                FetchRequestV2 {
                    body,
                    multipart,
                    ..request("POST", "https://a.example/")
                },
                &limits,
                "b",
                TIMEOUT,
            )
            .map(|plan| plan.request.body.map_or(0, |body| body.len()))
            .map_err(|e| e.kind)
        };
        assert_eq!(sized(Some(vec![1; 8]), None), Ok(8));
        assert_eq!(sized(Some(vec![1; 9]), None), Err(HttpErrorKind::TooLarge));
        assert_eq!(
            sized(None, Some(vec![part("a", &[1; 5]), part("b", &[1; 4])])),
            Err(HttpErrorKind::TooLarge)
        );
        assert_eq!(
            sized(None, Some(vec![part("a", &[1; 8])])),
            Err(HttpErrorKind::TooLarge),
            "the framing counts"
        );
        assert_eq!(sized(None, Some(vec![])), Ok(7));
    }

    #[test]
    fn the_response_cap_is_the_lowest_that_applies() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(response_cap(None, 10 * MIB, 50 * MIB, None), 10 * MIB);
        assert_eq!(
            response_cap(Some(40 * MIB), 10 * MIB, 50 * MIB, None),
            40 * MIB
        );
        assert_eq!(
            response_cap(Some(80 * MIB), 10 * MIB, 50 * MIB, None),
            50 * MIB
        );
        assert_eq!(
            response_cap(Some(40 * MIB), 10 * MIB, 50 * MIB, Some(MIB)),
            MIB
        );
        assert_eq!(
            response_cap(None, 10 * MIB, 5 * MIB, Some(20 * MIB)),
            5 * MIB
        );
        assert_eq!(response_cap(Some(0), 10 * MIB, 50 * MIB, None), 0);
    }

    #[test]
    fn limits_default_and_follow_the_environment() {
        let none = FetchLimits::from_vars(|_| None).expect("defaults");
        assert_eq!(
            (none.max_request_bytes, none.max_response_bytes),
            (33_554_432, 52_428_800)
        );
        let set = FetchLimits::from_vars(|name| match name {
            "HOST_HTTP_MAX_REQUEST_BYTES" => Some("1024".into()),
            "HOST_HTTP_V2_MAX_RESPONSE_BYTES" => Some("2048".into()),
            _ => None,
        })
        .expect("set");
        assert_eq!(
            (set.max_request_bytes, set.max_response_bytes),
            (1024, 2048)
        );
        for bad in ["0", "lots"] {
            assert!(FetchLimits::from_vars(|name| {
                (name == "HOST_HTTP_MAX_REQUEST_BYTES").then(|| bad.to_string())
            })
            .is_err());
        }
    }

    #[test]
    fn the_guests_trace_headers_are_replaced() {
        let plan = planned(FetchRequestV2 {
            headers: vec![
                ("TraceParent".into(), "00-guest".into()),
                ("tracestate".into(), "guest=1".into()),
                ("x-kept".into(), "1".into()),
            ],
            ..request("GET", "https://a.example/")
        })
        .map(traced)
        .expect("plan");
        let values = |name: &str| -> Vec<String> {
            plan.request
                .headers
                .get_all(name)
                .iter()
                .filter_map(|value| value.to_str().ok().map(str::to_string))
                .collect()
        };
        assert!(!values("traceparent").contains(&"00-guest".to_string()));
        assert!(values("tracestate").is_empty());
        assert_eq!(values("x-kept"), ["1"]);
    }

    #[tokio::test]
    async fn bytes_headers_and_statuses_come_back_as_they_were_sent() {
        let pdf: &[u8] = &[0x25, 0x50, 0x44, 0x46, 0x00, 0xff, 0xfe];
        let base = serve(
            Router::new()
                .route(
                    "/pdf",
                    any(move |body: axum::body::Bytes| async move {
                        (
                            axum::response::AppendHeaders([
                                ("set-cookie", "a=1"),
                                ("set-cookie", "b=2"),
                                ("x-echo", if body == pdf { "same" } else { "differs" }),
                            ]),
                            pdf,
                        )
                    }),
                )
                .route("/missing", get(|| async { (StatusCode::NOT_FOUND, "no") }))
                .route(
                    "/moved",
                    get(|| async { (StatusCode::FOUND, [("location", "/pdf")], "") }),
                ),
        )
        .await;
        let egress = egress(&[]);
        let sent = FetchRequestV2 {
            body: Some(pdf.to_vec()),
            ..request("PUT", &format!("{base}/pdf"))
        };
        let FetchResponseV2::Ok {
            status,
            headers,
            body,
        } = fetched(&egress, sent).await.expect("response")
        else {
            panic!("an error");
        };
        assert_eq!((status, body.as_slice()), (200, pdf));
        let cookies: Vec<&str> = headers
            .iter()
            .filter(|(name, _)| name == "set-cookie")
            .map(|(_, value)| value.as_str())
            .collect();
        assert_eq!(cookies, ["a=1", "b=2"]);
        assert!(headers.contains(&("x-echo".into(), "same".into())));

        let status_of = |path: &'static str, follow_redirects: bool| {
            let sent = FetchRequestV2 {
                follow_redirects,
                ..request("GET", &format!("{base}{path}"))
            };
            let egress = &egress;
            async move {
                match fetched(egress, sent).await {
                    Ok(FetchResponseV2::Ok { status, .. }) => status,
                    other => panic!("{path}: {other:?}"),
                }
            }
        };
        assert_eq!(status_of("/missing", false).await, 404);
        assert_eq!(status_of("/moved", false).await, 302);
        assert_eq!(status_of("/moved", true).await, 200);
    }

    #[tokio::test]
    async fn a_head_is_answered_whatever_length_it_announces() {
        let base = serve(Router::new().route("/big", get(|| async { "x".repeat(8192) }))).await;
        let egress = egress(&[]);
        let FetchResponseV2::Ok {
            status,
            headers,
            body,
        } = fetched(&egress, request("HEAD", &format!("{base}/big")))
            .await
            .expect("a response")
        else {
            panic!("an error");
        };
        assert_eq!((status, body.len()), (200, 0));
        assert!(headers.contains(&("content-length".into(), "8192".into())));
        assert_eq!(
            failed(&egress, request("GET", &format!("{base}/big"))).await,
            HttpErrorKind::TooLarge
        );
    }

    #[tokio::test]
    async fn repeated_query_keys_reach_the_server_through_both_imports() {
        let base = serve(Router::new().route(
            "/v1/promotion_codes",
            get(|RawQuery(query): RawQuery| async move { query.unwrap_or_default() }),
        ))
        .await;
        let query = "code=SPRING&expand[]=data.coupon&expand[]=data.customer&expand%5B%5D=c";
        let url = format!("{base}/v1/promotion_codes?{query}");
        let egress = egress(&[]);
        let legacy = egress
            .fetch("GET", url.clone(), &HashMap::new(), None, TIMEOUT)
            .await
            .expect("legacy");
        assert_eq!(legacy["body"], json!(query));
        let FetchResponseV2::Ok { body, .. } =
            fetched(&egress, request("GET", &url)).await.expect("v2")
        else {
            panic!("an error");
        };
        assert_eq!(String::from_utf8(body).expect("utf8"), query);
    }

    #[tokio::test]
    async fn failures_name_their_kind() {
        let base = serve(
            Router::new()
                .route("/big", get(|| async { "x".repeat(8192) }))
                .route(
                    "/slow",
                    get(|| async {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        "late"
                    }),
                )
                .route(
                    "/loop",
                    get(|| async { (StatusCode::FOUND, [("location", "/loop")], "") }),
                ),
        )
        .await;
        let open = egress(&[]);
        let get = |url: String| request("GET", &url);
        assert_eq!(
            failed(&open, get(format!("{base}/big"))).await,
            HttpErrorKind::TooLarge
        );
        let small = FetchRequestV2 {
            max_response_bytes: Some(16),
            ..get(format!("{base}/big"))
        };
        assert_eq!(failed(&open, small).await, HttpErrorKind::TooLarge);
        assert_eq!(
            failed(&open, get(format!("{base}/slow"))).await,
            HttpErrorKind::Timeout
        );
        let looping = FetchRequestV2 {
            follow_redirects: true,
            ..get(format!("{base}/loop"))
        };
        assert_eq!(failed(&open, looping).await, HttpErrorKind::Redirect);
        for (url, kind) in [
            (
                "http://169.254.169.254/latest/meta-data/",
                HttpErrorKind::Denied,
            ),
            ("http://metadata.google.internal/", HttpErrorKind::Denied),
            ("http://no-such-host.invalid/", HttpErrorKind::Dns),
            ("ftp://a.example/file", HttpErrorKind::InvalidRequest),
            ("not a url", HttpErrorKind::InvalidRequest),
        ] {
            assert_eq!(failed(&open, get(url.into())).await, kind, "{url}");
        }
        let unused = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            listener.local_addr().expect("addr")
        };
        assert_eq!(
            failed(&open, get(format!("http://{unused}/"))).await,
            HttpErrorKind::Connect
        );
        assert_eq!(
            failed(&open, get(base.replacen("http", "https", 1))).await,
            HttpErrorKind::Tls
        );
    }

    #[tokio::test]
    async fn the_tenants_egress_policy_decides_every_hop() {
        let inner = serve(Router::new().route("/", get(|| async { "inner" }))).await;
        let target = inner.clone();
        let outer = serve(Router::new().route(
            "/",
            get(move || {
                let location = format!("{target}/");
                async move { (StatusCode::TEMPORARY_REDIRECT, [("location", location)], "") }
            }),
        ))
        .await;
        let enforce = egress(&[("CUFFLINK_POLICY_EGRESS", "enforce")]);
        assert_eq!(
            failed(&enforce, request("GET", &inner)).await,
            HttpErrorKind::Denied
        );
        let allowed = egress(&[
            ("CUFFLINK_POLICY_EGRESS", "enforce"),
            (
                "CUFFLINK_POLICY_EGRESS_CONFIG",
                r#"{"allow_hosts": ["127.0.0.1"], "max_response_bytes": 3}"#,
            ),
        ]);
        assert_eq!(
            failed(&allowed, request("GET", &inner)).await,
            HttpErrorKind::TooLarge,
            "the tenant's cap is below what the guest may ask for"
        );
        let roomy = egress(&[
            ("CUFFLINK_POLICY_EGRESS", "enforce"),
            (
                "CUFFLINK_POLICY_EGRESS_CONFIG",
                r#"{"allow_hosts": ["127.0.0.1"]}"#,
            ),
        ]);
        let followed = FetchRequestV2 {
            follow_redirects: true,
            ..request("GET", &outer)
        };
        assert!(matches!(
            fetched(&roomy, followed.clone()).await,
            Ok(FetchResponseV2::Ok { status: 200, .. })
        ));
        let metadata = serve(Router::new().route(
            "/",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [("location", "http://169.254.169.254/latest/")],
                    "",
                )
            }),
        ))
        .await;
        let to_metadata = FetchRequestV2 {
            follow_redirects: true,
            ..request("GET", &metadata)
        };
        assert_eq!(failed(&roomy, to_metadata).await, HttpErrorKind::Denied);
    }
}
