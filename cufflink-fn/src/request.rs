use crate::{Auth, Response, Scope};
use base64::{engine::general_purpose, Engine};
use cufflink_types::host::request::{ScheduleFire, SubscriptionMeta, METHOD_SCHEDULE};
use cufflink_types::RetryOn;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// Context the platform attaches to a request when it was delivered via the
/// long-running jobs runtime. Absent on direct HTTP invocations.
///
/// Use this when a handler needs to distinguish a fresh invocation from a
/// worker redelivery — e.g. to skip an idempotency-sensitive side effect that
/// the prior attempt already paid for.
#[derive(Debug, Clone, Default)]
pub struct JobContext {
    pub id: String,
    pub attempt: u32,
    pub max_attempts: u32,
    /// What the attempt before this one failed with.
    pub last_error: Option<String>,
    /// The failures the job is attempted again after.
    pub retry_on: Vec<RetryOn>,
}

impl JobContext {
    /// `true` once the worker has already delivered this job at least once.
    pub fn is_retry(&self) -> bool {
        self.attempt > 1
    }

    /// What the attempt before this one failed with; `None` on the first
    /// attempt and on a platform that does not say.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// The failures the job is attempted again after, as its route
    /// declares them; none on a platform that does not say. A crash of
    /// its worker and a retry its handler asks for are attempted again
    /// whatever it declares.
    pub fn retry_on(&self) -> &[RetryOn] {
        &self.retry_on
    }
}

/// The JetStream delivery behind a subscription or schedule handler's
/// request. Absent on a platform that does not send it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionContext(SubscriptionMeta);

impl SubscriptionContext {
    pub fn subject(&self) -> &str {
        &self.0.subject
    }

    pub fn stream(&self) -> &str {
        &self.0.stream
    }

    /// The durable consumer that delivered it.
    pub fn consumer(&self) -> &str {
        &self.0.consumer
    }

    /// The message's sequence in its stream: with the stream's name, what
    /// identifies it across deliveries.
    pub fn sequence(&self) -> u64 {
        self.0.stream_sequence
    }

    /// 1 on the first delivery.
    pub fn attempt(&self) -> u64 {
        self.0.num_delivered
    }

    pub fn is_redelivery(&self) -> bool {
        self.0.num_delivered > 1
    }

    /// Messages of the consumer not yet delivered.
    pub fn pending(&self) -> u64 {
        self.0.num_pending
    }

    /// When the message was published, RFC 3339.
    pub fn published_at(&self) -> &str {
        &self.0.published_at
    }

    /// The first value of a header of the message, its name in any case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.0
            .headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .and_then(|(_, values)| values.first())
            .map(String::as_str)
    }

    /// The id the publisher gave the message (`Nats-Msg-Id`).
    pub fn msg_id(&self) -> Option<&str> {
        self.header("Nats-Msg-Id")
    }
}

/// The fire a schedule handler runs for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleContext(ScheduleFire);

impl ScheduleContext {
    pub fn name(&self) -> &str {
        &self.0.schedule
    }

    /// The instant the schedule was due, RFC 3339.
    pub fn fire_at(&self) -> &str {
        &self.0.fire_at
    }

    /// Whether the fire is delivered late, after an outage.
    pub fn is_catch_up(&self) -> bool {
        self.0.catch_up
    }
}

/// One part of a `multipart/form-data` body, on a route declared
/// `multipart`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    pub name: Option<String>,
    pub filename: Option<String>,
    pub content_type: Option<String>,
    pub data: Vec<u8>,
}

/// The service that called an `internal` route, through the platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    pub service: String,
    /// The end user the calling service was serving; attribution only.
    pub on_behalf_of: Option<String>,
    /// How many service calls deep this invocation runs.
    pub depth: u32,
}

/// The partner key that called an `api_key` route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyPrincipal {
    pub id: String,
    pub name: String,
    pub scopes: Vec<String>,
}

/// An incoming HTTP request from the Cufflink platform.
///
/// The platform serializes the full request context (method, headers, body,
/// tenant, service name, auth) into JSON and passes it to your handler.
#[derive(Debug, Clone)]
pub struct Request {
    method: String,
    handler: String,
    headers: HashMap<String, String>,
    body: Value,
    raw_body: Vec<u8>,
    tenant: String,
    service: String,
    auth: Option<Auth>,
    job: Option<JobContext>,
    subscription: Option<SubscriptionContext>,
    posture: Option<Value>,
    path: Option<String>,
    path_params: Vec<(String, String)>,
    query_string: String,
    query: HashMap<String, Vec<String>>,
    client_ip: Option<String>,
    multipart: Vec<Part>,
    caller: Option<Caller>,
    api_key: Option<ApiKeyPrincipal>,
    scope: Option<Scope>,
}

impl Request {
    /// Parse a `Request` from the JSON the platform provides.
    pub fn from_json(json: &str) -> Option<Self> {
        serde_json::from_str(json)
            .ok()
            .map(|v| Self::from_value(&v))
    }

    /// The request a handler receives when the platform's JSON did not parse:
    /// every field at its default.
    pub(crate) fn unparsed() -> Self {
        Self::from_value(&Value::Null)
    }

    fn from_value(v: &Value) -> Self {
        let headers = v["headers"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let auth = v["auth"].as_object().map(Auth::from_json);

        let raw_body = v["body_raw_b64"]
            .as_str()
            .filter(|s| !s.is_empty())
            .and_then(|s| general_purpose::STANDARD.decode(s).ok())
            .unwrap_or_default();

        let query = v["query"]
            .as_object()
            .map(|query| {
                query
                    .iter()
                    .map(|(key, values)| (key.clone(), strings(values)))
                    .collect()
            })
            .unwrap_or_default();

        let multipart = v["multipart"]
            .as_array()
            .map(|parts| parts.iter().filter_map(part).collect())
            .unwrap_or_default();

        let job = v["_job"].as_object().and_then(|j| {
            let id = j.get("id")?.as_str()?.to_string();
            let attempt = u32::try_from(j.get("attempt")?.as_u64()?).ok()?;
            let max_attempts = u32::try_from(j.get("max_attempts")?.as_u64()?).ok()?;
            Some(JobContext {
                id,
                attempt,
                max_attempts,
                last_error: j
                    .get("last_error")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                retry_on: j
                    .get("retry_on")
                    .and_then(Value::as_array)
                    .map(|failures| {
                        failures
                            .iter()
                            .filter_map(|failure| RetryOn::deserialize(failure).ok())
                            .collect()
                    })
                    .unwrap_or_default(),
            })
        });

        let subscription = serde_json::from_value::<SubscriptionMeta>(v["subscription"].clone())
            .ok()
            .map(SubscriptionContext);

        let path_params = v["path_params"]
            .as_array()
            .map(|params| {
                params
                    .iter()
                    .filter_map(|pair| match pair.as_array()?.as_slice() {
                        [Value::String(name), Value::String(value)] => {
                            Some((name.clone(), value.clone()))
                        }
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();

        let posture = match &v["posture"] {
            Value::Object(_) => Some(v["posture"].clone()),
            _ => None,
        };

        Self {
            method: v["method"].as_str().unwrap_or("GET").to_string(),
            handler: v["handler"].as_str().unwrap_or("").to_string(),
            headers,
            body: v["body"].clone(),
            raw_body,
            tenant: v["tenant"].as_str().unwrap_or("").to_string(),
            service: v["service"].as_str().unwrap_or("").to_string(),
            auth,
            job,
            subscription,
            posture,
            path: v["path"].as_str().map(str::to_string),
            path_params,
            query_string: v["query_string"].as_str().unwrap_or_default().to_string(),
            query,
            client_ip: v["client_ip"].as_str().map(str::to_string),
            multipart,
            caller: caller(&v["caller"]),
            api_key: api_key(&v["api_key"]),
            scope: Scope::from_json(&v["scope"]),
        }
    }

    /// The path below the service a `routing: strict` route matched, such as
    /// `/books/7/renew`. `None` when the handler was called through `_fn/`.
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// A percent-decoded path parameter of a strict route.
    pub fn path_param(&self, name: &str) -> Option<&str> {
        self.path_params
            .iter()
            .find(|(param, _)| param == name)
            .map(|(_, value)| value.as_str())
    }

    /// Every path parameter of a strict route, in path order; empty through
    /// `_fn/`.
    pub fn path_params(&self) -> &[(String, String)] {
        &self.path_params
    }

    /// The HTTP method (GET, POST, PUT, DELETE).
    pub fn method(&self) -> &str {
        &self.method
    }

    /// The handler name from the URL path.
    pub fn handler(&self) -> &str {
        &self.handler
    }

    /// All HTTP headers as a map.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// A header's value, by case-insensitive name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(|s| s.as_str())
    }

    /// The raw query string, without `?`. Empty unless the route is strict
    /// or declares an R2 keyword (`body_limit`, `timeout`, `multipart`, …).
    pub fn query_string(&self) -> &str {
        &self.query_string
    }

    /// Every value of a query parameter, in order.
    pub fn query_all(&self, name: &str) -> &[String] {
        self.query.get(name).map_or(&[], Vec::as_slice)
    }

    /// The first value of a query parameter.
    pub fn query_first(&self, name: &str) -> Option<&str> {
        self.query_all(name).first().map(String::as_str)
    }

    /// The query string deserialized into `T`.
    pub fn query<T: DeserializeOwned>(&self) -> Result<T, String> {
        serde_urlencoded::from_str(&self.query_string).map_err(|e| e.to_string())
    }

    /// The caller's IP address as the platform determined it (behind its
    /// trusted proxies), when known.
    pub fn client_ip(&self) -> Option<&str> {
        self.client_ip.as_deref()
    }

    /// The service calling an `internal` route; `None` on every other route.
    pub fn caller(&self) -> Option<&Caller> {
        self.caller.as_ref()
    }

    /// The partner key calling an `api_key` route; `None` on every other
    /// route.
    pub fn api_key(&self) -> Option<&ApiKeyPrincipal> {
        self.api_key.as_ref()
    }

    /// The rows a `scoped` route's caller may reach; `None` on every other
    /// route.
    pub fn scope(&self) -> Option<&Scope> {
        self.scope.as_ref()
    }

    /// The parts of a `multipart` route's form body.
    pub fn multipart(&self) -> &[Part] {
        &self.multipart
    }

    /// The parsed JSON body. Returns `Value::Null` if no body was sent.
    pub fn body(&self) -> &Value {
        &self.body
    }

    /// The raw, unparsed request body bytes as the caller sent them.
    ///
    /// Use this when verifying webhook signatures that HMAC over the
    /// original payload (Stripe, OpenPhone/Quo, etc.) — the parsed
    /// `body()` is canonicalized by `serde_json` and will not byte-match
    /// what the sender signed. Returns an empty slice when no body was
    /// sent.
    pub fn raw_body(&self) -> &[u8] {
        &self.raw_body
    }

    /// The tenant slug from the URL.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// The service name from the URL.
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Get the authenticated user context, if present.
    ///
    /// Returns `None` if no valid JWT or API key was provided with the request.
    pub fn auth(&self) -> Option<&Auth> {
        self.auth.as_ref()
    }

    /// Get the job context attached by the long-running jobs runtime.
    ///
    /// Returns `None` when the handler is invoked directly via HTTP. Present
    /// for handlers annotated with `job(...)` and dispatched by a worker.
    pub fn job(&self) -> Option<&JobContext> {
        self.job.as_ref()
    }

    /// The JetStream delivery a subscription or schedule handler runs for:
    /// which message it is and how often it has been delivered. `None` on
    /// any other request, and on a platform that does not send it.
    pub fn subscription(&self) -> Option<&SubscriptionContext> {
        self.subscription.as_ref()
    }

    /// The fire a schedule handler runs for; `None` on any other request.
    pub fn schedule(&self) -> Option<ScheduleContext> {
        (self.method == METHOD_SCHEDULE)
            .then(|| serde_json::from_value::<ScheduleFire>(self.body.clone()).ok())
            .flatten()
            .map(ScheduleContext)
    }

    /// The route's declared posture as raw JSON (`{"kind": "...", ...}`), if the
    /// platform enforces postures for this route. `None` on a legacy platform.
    pub fn posture(&self) -> Option<&Value> {
        self.posture.as_ref()
    }

    /// For an `owner("field")` route, the owner column the handler must scope by.
    ///
    /// The platform guarantees the caller is authenticated and passes the field
    /// name here; the handler owns the query, so it must constrain rows to the
    /// caller. `field` may hold the caller's subject directly, or an application
    /// id resolved from it (e.g. a customer row looked up by `auth.sub`) — scope
    /// by whichever your schema stores. Returns `None` for any other posture.
    ///
    /// ```rust,ignore
    /// let auth = req.require_auth()?;
    /// if let Some(field) = req.owner_field() {
    ///     // Direct: the column stores the caller's subject.
    ///     let sql = format!(
    ///         "SELECT * FROM items WHERE {} = {}",
    ///         field, db::sql_escape(&auth.sub)
    ///     );
    ///     // Indirect: resolve the owning id from `auth.sub` first, then
    ///     // constrain `field` by that id instead.
    /// }
    /// ```
    pub fn owner_field(&self) -> Option<&str> {
        let posture = self.posture.as_ref()?;
        if posture.get("kind")?.as_str()? == "owner" {
            posture.get("field")?.as_str()
        } else {
            None
        }
    }

    /// Require authentication. Returns the auth context or an error response.
    ///
    /// ```rust,ignore
    /// handler!(protected, |req: Request| {
    ///     let auth = match req.require_auth() {
    ///         Ok(auth) => auth,
    ///         Err(resp) => return resp,
    ///     };
    ///     Response::json(&json!({"user": auth.sub}))
    /// });
    /// ```
    pub fn require_auth(&self) -> Result<&Auth, Response> {
        self.auth
            .as_ref()
            .ok_or_else(|| Response::unauthorized("Authentication required"))
    }
}

fn strings(values: &Value) -> Vec<String> {
    values
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn caller(value: &Value) -> Option<Caller> {
    Some(Caller {
        service: value["service"].as_str()?.to_string(),
        on_behalf_of: value["on_behalf_of"].as_str().map(str::to_string),
        depth: value["depth"]
            .as_u64()
            .and_then(|depth| u32::try_from(depth).ok())
            .unwrap_or(0),
    })
}

fn api_key(value: &Value) -> Option<ApiKeyPrincipal> {
    Some(ApiKeyPrincipal {
        id: value["id"].as_str()?.to_string(),
        name: value["name"].as_str().unwrap_or_default().to_string(),
        scopes: strings(&value["scopes"]),
    })
}

fn part(value: &Value) -> Option<Part> {
    let text = |key: &str| value[key].as_str().map(str::to_string);
    Some(Part {
        name: text("name"),
        filename: text("filename"),
        content_type: text("content_type"),
        data: general_purpose::STANDARD
            .decode(value["data_b64"].as_str()?)
            .ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_request_parsing() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "checkout",
            "headers": {"content-type": "application/json"},
            "body": {"item": "widget", "qty": 3},
            "tenant": "acme",
            "service": "shop"
        }))
        .unwrap();

        let req = Request::from_json(&json).unwrap();
        assert_eq!(req.method(), "POST");
        assert_eq!(req.handler(), "checkout");
        assert_eq!(req.tenant(), "acme");
        assert_eq!(req.service(), "shop");
        assert_eq!(req.body()["item"], "widget");
        assert_eq!(req.body()["qty"], 3);
        assert_eq!(req.header("content-type"), Some("application/json"));
    }

    #[test]
    fn owner_field_exposed_only_for_owner_posture() {
        let owner = Request::from_json(
            &json!({"method": "GET", "posture": {"kind": "owner", "field": "customer_id"}})
                .to_string(),
        )
        .unwrap();
        assert_eq!(owner.owner_field(), Some("customer_id"));
        assert!(owner.posture().is_some());

        let perm = Request::from_json(
            &json!({"method": "GET", "posture": {"kind": "permission", "area": "a", "operation": "edit"}})
                .to_string(),
        )
        .unwrap();
        assert_eq!(perm.owner_field(), None);

        let legacy = Request::from_json(r#"{"method": "GET"}"#).unwrap();
        assert_eq!(legacy.owner_field(), None);
        assert!(legacy.posture().is_none());
    }

    #[test]
    fn strict_routes_expose_their_path_and_params() {
        let req = Request::from_json(
            &json!({
                "method": "PUT",
                "path": "/books/7/due-date",
                "path_params": [["note", "a b"], ["book_id", "7"]],
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(req.path(), Some("/books/7/due-date"));
        assert_eq!(req.path_param("book_id"), Some("7"));
        assert_eq!(req.path_param("note"), Some("a b"));
        assert_eq!(req.path_param("missing"), None);
        assert_eq!(
            req.path_params(),
            [
                ("note".to_string(), "a b".to_string()),
                ("book_id".to_string(), "7".to_string())
            ]
        );

        let fn_call = Request::from_json(r#"{"method": "PUT"}"#).unwrap();
        assert_eq!(fn_call.path(), None);
        assert_eq!(fn_call.path_param("book_id"), None);
        assert!(fn_call.path_params().is_empty());
    }

    #[test]
    fn r2_requests_expose_query_client_ip_and_parts() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Page {
            page: u32,
            sort: Option<String>,
        }
        let req = Request::from_json(
            &json!({
                "method": "POST",
                "headers": {"x-signature": "s"},
                "query_string": "page=2&tag=a&tag=b",
                "query": {"page": ["2"], "tag": ["a", "b"]},
                "client_ip": "203.0.113.7",
                "multipart": [
                    {"name": "file", "filename": "r.csv", "content_type": "text/csv", "data_b64": "YSxi"},
                    {"name": "broken", "data_b64": "%%%"}
                ],
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(req.header("X-Signature"), Some("s"));
        assert_eq!(req.query_all("tag"), ["a", "b"]);
        assert_eq!(req.query_first("tag"), Some("a"));
        assert_eq!(req.query_first("none"), None);
        assert!(req.query_all("none").is_empty());
        assert_eq!(req.query_string(), "page=2&tag=a&tag=b");
        assert_eq!(
            req.query::<Page>(),
            Ok(Page {
                page: 2,
                sort: None
            })
        );
        assert!(req.query::<HashMap<String, u32>>().is_err());
        assert_eq!(req.client_ip(), Some("203.0.113.7"));
        assert_eq!(
            req.multipart(),
            [Part {
                name: Some("file".into()),
                filename: Some("r.csv".into()),
                content_type: Some("text/csv".into()),
                data: b"a,b".to_vec(),
            }]
        );

        let internal = Request::from_json(
            &json!({"caller": {"service": "pricing", "on_behalf_of": "kc-1", "depth": 2}})
                .to_string(),
        )
        .unwrap();
        assert_eq!(
            internal.caller(),
            Some(&Caller {
                service: "pricing".into(),
                on_behalf_of: Some("kc-1".into()),
                depth: 2,
            })
        );

        let partner = Request::from_json(
            &json!({"api_key": {"id": "k-1", "name": "acme-ingest", "scopes": ["books:*"]}})
                .to_string(),
        )
        .unwrap();
        assert_eq!(
            partner.api_key(),
            Some(&ApiKeyPrincipal {
                id: "k-1".into(),
                name: "acme-ingest".into(),
                scopes: vec!["books:*".into()],
            })
        );

        let scoped = Request::from_json(&json!({"scope": {"kind": "all"}}).to_string()).unwrap();
        assert_eq!(scoped.scope(), Some(&Scope::All));

        let legacy = Request::from_json(r#"{"method": "GET"}"#).unwrap();
        assert!(
            legacy.caller().is_none() && legacy.api_key().is_none() && legacy.scope().is_none()
        );
        assert_eq!(legacy.query_string(), "");
        assert!(legacy.query_all("page").is_empty());
        assert_eq!(legacy.client_ip(), None);
        assert!(legacy.multipart().is_empty());
    }

    #[test]
    fn test_request_missing_fields() {
        let json = r#"{"method": "GET"}"#;
        let req = Request::from_json(json).unwrap();
        assert_eq!(req.method(), "GET");
        assert_eq!(req.handler(), "");
        assert_eq!(req.tenant(), "");
        assert_eq!(req.body(), &Value::Null);
        assert!(req.raw_body().is_empty());
    }

    #[test]
    fn test_request_raw_body_round_trip() {
        use base64::{engine::general_purpose, Engine};
        let raw = br#"{"type":"message.delivered","data":{"object":{"id":"AC1"}}}"#;
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "webhook",
            "headers": {"content-type": "application/json"},
            "body": serde_json::from_slice::<Value>(raw).unwrap(),
            "body_raw_b64": general_purpose::STANDARD.encode(raw),
            "tenant": "acme",
            "service": "shop",
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert_eq!(req.raw_body(), raw);
    }

    #[test]
    fn test_request_raw_body_invalid_base64_yields_empty() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "webhook",
            "body": Value::Null,
            "body_raw_b64": "not%base64!",
            "tenant": "acme",
            "service": "shop",
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert!(req.raw_body().is_empty());
    }

    #[test]
    fn test_request_job_context_parsed_when_present() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "handle_run_report",
            "body": {"id": "item-1"},
            "tenant": "acme",
            "service": "asset",
            "_job": {
                "id": "11111111-1111-1111-1111-111111111111",
                "attempt": 2,
                "max_attempts": 3,
            },
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        let job = req.job().expect("job context should be parsed");
        assert_eq!(job.id, "11111111-1111-1111-1111-111111111111");
        assert_eq!(job.attempt, 2);
        assert_eq!(job.max_attempts, 3);
        assert!(job.is_retry());
    }

    #[test]
    fn a_job_says_what_its_last_attempt_failed_with() {
        let job = |envelope: Value| {
            Request::from_json(&json!({"method": "POST", "_job": envelope}).to_string())
                .and_then(|request| request.job().cloned())
                .expect("job")
        };
        let first = job(json!({"id": "j-1", "attempt": 1, "max_attempts": 3}));
        assert_eq!(first.last_error(), None);
        assert_eq!(first.retry_on(), []);
        let retried = job(json!({
            "id": "j-1", "attempt": 2, "max_attempts": 3,
            "last_error": "upstream 503", "retry_on": ["error"],
        }));
        assert_eq!(retried.last_error(), Some("upstream 503"));
        assert_eq!(retried.retry_on(), [RetryOn::Error]);
    }

    #[test]
    fn a_job_reads_the_failures_it_knows_of_those_it_is_retried_after() {
        let retry_on = |declared: Value| {
            Request::from_json(
                &json!({
                    "method": "POST",
                    "_job": {"id": "j-1", "attempt": 1, "max_attempts": 3, "retry_on": declared},
                })
                .to_string(),
            )
            .and_then(|request| request.job().cloned())
            .expect("job")
            .retry_on
        };
        assert_eq!(
            retry_on(json!(["timeout", "error"])),
            [RetryOn::Timeout, RetryOn::Error]
        );
        assert_eq!(
            retry_on(json!(["timeout", "out_of_memory", 7])),
            [RetryOn::Timeout],
            "a failure a later platform names is not this SDK's to read"
        );
        assert_eq!(retry_on(json!("error")), []);
    }

    fn delivery(method: &str, body: Value) -> Value {
        json!({
            "method": method,
            "handler": "on_book",
            "headers": {},
            "body": body,
            "tenant": "acme",
            "service": "loan-service",
            "auth": null,
            "subscription": {
                "subject": "books.created",
                "stream": "BOOKS",
                "consumer": "loan-service_on_book",
                "stream_sequence": 81234,
                "consumer_sequence": 17,
                "num_delivered": 2,
                "num_pending": 3,
                "published_at": "2026-09-28T10:00:00Z",
                "headers": {"Nats-Msg-Id": ["book-7"], "X-Source": ["boss", "retry"]},
            },
        })
    }

    #[test]
    fn a_delivery_says_which_message_it_is() {
        let request = Request::from_json(&delivery("SUBSCRIPTION", json!({"id": 7})).to_string())
            .expect("request");
        let message = request.subscription().expect("subscription");
        assert_eq!(message.subject(), "books.created");
        assert_eq!(message.stream(), "BOOKS");
        assert_eq!(message.consumer(), "loan-service_on_book");
        assert_eq!(message.sequence(), 81234);
        assert_eq!(message.attempt(), 2);
        assert!(message.is_redelivery());
        assert_eq!(message.pending(), 3);
        assert_eq!(message.published_at(), "2026-09-28T10:00:00Z");
        assert_eq!(message.msg_id(), Some("book-7"));
        assert_eq!(message.header("x-source"), Some("boss"));
        assert_eq!(message.header("absent"), None);
        assert_eq!(request.schedule(), None);
    }

    #[test]
    fn a_platform_that_sends_no_delivery_metadata_gives_none() {
        let legacy = json!({"method": "SUBSCRIPTION", "handler": "on_book", "body": {"id": 7}});
        let request = Request::from_json(&legacy.to_string()).expect("request");
        assert_eq!(request.subscription(), None);
        let malformed = json!({"method": "SUBSCRIPTION", "subscription": {"subject": 1}});
        let request = Request::from_json(&malformed.to_string()).expect("request");
        assert_eq!(request.subscription(), None);
    }

    #[test]
    fn a_schedule_handler_reads_its_fire() {
        let fire = json!({
            "schedule": "nightly", "fire_at": "2026-09-28T02:00:00Z", "catch_up": true,
        });
        let request =
            Request::from_json(&delivery("SCHEDULE", fire.clone()).to_string()).expect("request");
        let schedule = request.schedule().expect("schedule");
        assert_eq!(schedule.name(), "nightly");
        assert_eq!(schedule.fire_at(), "2026-09-28T02:00:00Z");
        assert!(schedule.is_catch_up());
        assert!(request.subscription().is_some());

        let posted = Request::from_json(&json!({"method": "POST", "body": fire}).to_string())
            .expect("request");
        assert_eq!(
            posted.schedule(),
            None,
            "only the platform delivers a schedule's fire"
        );
    }

    #[test]
    fn test_request_job_context_absent_on_http_direct() {
        let json = serde_json::to_string(&json!({
            "method": "GET",
            "handler": "list",
            "body": Value::Null,
            "tenant": "acme",
            "service": "asset",
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert!(req.job().is_none());
    }

    #[test]
    fn test_request_job_context_first_attempt_is_not_retry() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "handle_run_report",
            "body": Value::Null,
            "tenant": "acme",
            "service": "asset",
            "_job": {
                "id": "22222222-2222-2222-2222-222222222222",
                "attempt": 1,
                "max_attempts": 2,
            },
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        let job = req.job().expect("job context should be parsed");
        assert_eq!(job.attempt, 1);
        assert!(!job.is_retry());
    }

    #[test]
    fn test_request_job_context_rejects_malformed_envelope() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "handle_run_report",
            "body": Value::Null,
            "tenant": "acme",
            "service": "asset",
            "_job": {
                "id": "33333333-3333-3333-3333-333333333333",
                "max_attempts": 2,
            },
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert!(
            req.job().is_none(),
            "missing attempt should yield None, not a partial JobContext"
        );
    }

    #[test]
    fn test_request_with_auth() {
        let json = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "checkout",
            "headers": {},
            "body": {},
            "tenant": "acme",
            "service": "shop",
            "auth": {
                "sub": "user-123",
                "preferred_username": "john",
                "name": "John Doe",
                "email": "john@example.com",
                "realm_roles": ["admin", "manager"],
                "claims": {"department": "engineering"}
            }
        }))
        .unwrap();

        let req = Request::from_json(&json).unwrap();
        let auth = req.auth().unwrap();
        assert_eq!(auth.sub, "user-123");
        assert_eq!(auth.preferred_username.as_deref(), Some("john"));
        assert_eq!(auth.name.as_deref(), Some("John Doe"));
        assert_eq!(auth.email.as_deref(), Some("john@example.com"));
        assert!(auth.has_role("admin"));
        assert!(auth.has_role("manager"));
        assert!(!auth.has_role("viewer"));
        assert_eq!(
            auth.claim("department").and_then(|v| v.as_str()),
            Some("engineering")
        );
    }

    #[test]
    fn test_request_without_auth() {
        let json = r#"{"method": "GET"}"#;
        let req = Request::from_json(json).unwrap();
        assert!(req.auth().is_none());
    }

    #[test]
    fn test_request_null_auth() {
        let json = serde_json::to_string(&json!({
            "method": "GET",
            "auth": null
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert!(req.auth().is_none());
    }

    #[test]
    fn test_require_auth_success() {
        let json = serde_json::to_string(&json!({
            "method": "GET",
            "auth": {"sub": "user-1", "realm_roles": [], "claims": {}}
        }))
        .unwrap();
        let req = Request::from_json(&json).unwrap();
        assert!(req.require_auth().is_ok());
        assert_eq!(req.require_auth().unwrap().sub, "user-1");
    }

    #[test]
    fn test_require_auth_fails_when_unauthenticated() {
        let json = r#"{"method": "GET"}"#;
        let req = Request::from_json(json).unwrap();
        let err = req.require_auth().unwrap_err();
        let wrapped: Value = serde_json::from_str(&err.into_data()).unwrap();
        assert_eq!(wrapped["__status"], 401);
        assert_eq!(wrapped["__body"]["error"], "Authentication required");
    }
}
