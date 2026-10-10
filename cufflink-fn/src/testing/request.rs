//! Requests as the platform builds them: the struct it serialises,
//! serialised, and parsed as a handler's request always is.

use super::error::TestHostError;
use crate::Request;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use cufflink_types::host::request::{
    GuestRequest, JobMeta, MultipartField, ScheduleFire, SubscriptionMeta, METHOD_HOOK,
    METHOD_SCHEDULE, METHOD_SUBSCRIPTION,
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The stream the platform delivers schedules on.
const SCHEDULES_STREAM: &str = "CUFFLINK_SCHEDULES";
const JOB_ID: &str = "00000000-0000-4000-8000-000000000001";
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// A request of a custom route: `POST`, anonymous, without a body, until
/// told otherwise.
pub fn request() -> RequestBuilder {
    RequestBuilder::default()
}

/// Which keys a request carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A custom route: headers, the raw body and the posture.
    Route,
    /// What the platform itself delivers: a hook, a message, a fire.
    Delivery,
    /// The job a guest enqueued with `jobs::enqueue`.
    Enqueued,
}

/// The caller of a request, as the platform describes one it authenticated.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthBuilder {
    sub: String,
    preferred_username: Option<String>,
    name: Option<String>,
    email: Option<String>,
    realm_roles: Vec<String>,
    claims: Map<String, Value>,
    permissions: Vec<String>,
    role_names: Vec<String>,
    scoped_permissions: Vec<String>,
    is_service_account: bool,
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

impl AuthBuilder {
    /// A user with the subject `sub`, which is what the platform compares
    /// for an `owner(...)` route and an `@own` grant: the test of a service
    /// that compares a claim instead sets that claim too.
    pub fn user(sub: &str) -> Self {
        Self {
            sub: sub.to_string(),
            preferred_username: None,
            name: None,
            email: None,
            realm_roles: Vec::new(),
            claims: Map::new(),
            permissions: Vec::new(),
            role_names: Vec::new(),
            scoped_permissions: Vec::new(),
            is_service_account: false,
        }
    }

    /// A service account, which the platform lets past permission checks.
    pub fn service_account(sub: &str) -> Self {
        Self {
            is_service_account: true,
            ..Self::user(sub)
        }
    }

    pub fn username(self, username: &str) -> Self {
        Self {
            preferred_username: Some(username.to_string()),
            ..self
        }
    }

    pub fn name(self, name: &str) -> Self {
        Self {
            name: Some(name.to_string()),
            ..self
        }
    }

    pub fn email(self, email: &str) -> Self {
        Self {
            email: Some(email.to_string()),
            ..self
        }
    }

    /// The realm roles of the token.
    pub fn realm_roles(self, roles: &[&str]) -> Self {
        Self {
            realm_roles: strings(roles),
            ..self
        }
    }

    /// A claim of the token beside the ones the platform reads itself.
    pub fn claim(mut self, name: &str, value: impl Into<Value>) -> Self {
        self.claims.insert(name.to_string(), value.into());
        self
    }

    /// The permissions the caller's roles resolve to, such as `loans:create`.
    pub fn permissions(self, permissions: &[&str]) -> Self {
        Self {
            permissions: strings(permissions),
            ..self
        }
    }

    /// The Cufflink roles the caller holds.
    pub fn role_names(self, roles: &[&str]) -> Self {
        Self {
            role_names: strings(roles),
            ..self
        }
    }

    /// The caller's scoped grants, such as `books:view@all`; each is also a
    /// permission by its base.
    pub fn scoped_permissions(self, grants: &[&str]) -> Self {
        let bases = grants
            .iter()
            .filter_map(|grant| grant.split_once('@'))
            .map(|(base, _)| base.to_string())
            .filter(|base| !self.permissions.contains(base))
            .collect::<Vec<_>>();
        Self {
            scoped_permissions: strings(grants),
            permissions: self.permissions.into_iter().chain(bases).collect(),
            ..self
        }
    }

    fn into_value(self) -> Value {
        let auth = json!({
            "sub": self.sub,
            "preferred_username": self.preferred_username,
            "name": self.name,
            "email": self.email,
            "realm_roles": self.realm_roles,
            "claims": self.claims,
            "is_service_account": self.is_service_account,
            "permissions": self.permissions,
            "role_names": self.role_names,
        });
        match (auth, self.scoped_permissions.is_empty()) {
            (Value::Object(mut auth), false) => {
                auth.insert("scoped_permissions".into(), json!(self.scoped_permissions));
                Value::Object(auth)
            }
            (auth, _) => auth,
        }
    }
}

/// The delivery behind a subscription's request: the first delivery of the
/// stream's first message, until told otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionBuilder(SubscriptionMeta);

impl SubscriptionBuilder {
    /// A message published on `subject`.
    pub fn on(subject: &str) -> Self {
        Self(SubscriptionMeta {
            subject: subject.to_string(),
            stream_sequence: 1,
            consumer_sequence: 1,
            num_delivered: 1,
            published_at: "1970-01-01T00:00:00Z".to_string(),
            ..SubscriptionMeta::default()
        })
    }

    pub fn stream(self, stream: &str) -> Self {
        Self(SubscriptionMeta {
            stream: stream.to_string(),
            ..self.0
        })
    }

    pub fn consumer(self, consumer: &str) -> Self {
        Self(SubscriptionMeta {
            consumer: consumer.to_string(),
            ..self.0
        })
    }

    /// The message's sequence in its stream.
    pub fn sequence(self, sequence: u64) -> Self {
        Self(SubscriptionMeta {
            stream_sequence: sequence,
            ..self.0
        })
    }

    /// The delivery's sequence in its consumer.
    pub fn consumer_sequence(self, sequence: u64) -> Self {
        Self(SubscriptionMeta {
            consumer_sequence: sequence,
            ..self.0
        })
    }

    /// Which delivery this is; 1 is the first.
    pub fn attempt(self, attempt: u64) -> Self {
        Self(SubscriptionMeta {
            num_delivered: attempt,
            consumer_sequence: self.0.consumer_sequence.max(attempt),
            ..self.0
        })
    }

    /// Messages of the consumer not yet delivered.
    pub fn pending(self, pending: u64) -> Self {
        Self(SubscriptionMeta {
            num_pending: pending,
            ..self.0
        })
    }

    /// RFC 3339.
    pub fn published_at(self, at: &str) -> Self {
        Self(SubscriptionMeta {
            published_at: at.to_string(),
            ..self.0
        })
    }

    /// A header of the message; one given again has every value.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.0
            .headers
            .entry(name.to_string())
            .or_default()
            .push(value.to_string());
        self
    }

    /// The id the publisher gave the message (`Nats-Msg-Id`).
    pub fn msg_id(self, id: &str) -> Self {
        self.header("Nats-Msg-Id", id)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RequestBuilder {
    kind: Kind,
    method: String,
    handler: String,
    tenant: String,
    service: String,
    headers: Map<String, Value>,
    body: Value,
    raw_body: Vec<u8>,
    query: Vec<(String, String)>,
    query_string: String,
    auth: Value,
    posture: Value,
    path: Option<String>,
    path_params: Vec<(String, String)>,
    client_ip: Option<String>,
    multipart: Option<Vec<MultipartField>>,
    caller: Option<Value>,
    api_key: Option<Value>,
    scope: Option<Value>,
    v2: bool,
    /// What was given that only a route's request carries.
    route_only: BTreeSet<&'static str>,
    subscription: Option<SubscriptionMeta>,
    job: Option<JobMeta>,
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self {
            kind: Kind::Route,
            method: "POST".to_string(),
            handler: String::new(),
            tenant: String::new(),
            service: String::new(),
            headers: Map::new(),
            body: Value::Null,
            raw_body: Vec::new(),
            query: Vec::new(),
            query_string: String::new(),
            auth: Value::Null,
            posture: Value::Null,
            path: None,
            path_params: Vec::new(),
            client_ip: None,
            multipart: None,
            caller: None,
            api_key: None,
            scope: None,
            v2: false,
            route_only: BTreeSet::new(),
            subscription: None,
            job: None,
        }
    }
}

/// The body as the platform reads one: `null` when empty, the text itself
/// when it is not JSON.
fn parsed(body: &[u8]) -> Value {
    if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
    }
}

/// Query parameters merged into the body under `params`, the last value of
/// a name given twice: added to an object body, alone when there is no
/// body, and beside a non-object body under `data`.
fn with_params(body: Value, query: &[(String, String)]) -> Value {
    if query.is_empty() {
        return body;
    }
    let params: BTreeMap<&str, &str> = query
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    match body {
        Value::Object(mut map) => {
            map.insert("params".to_string(), json!(params));
            Value::Object(map)
        }
        Value::Null => json!({ "params": params }),
        other => json!({ "data": other, "params": params }),
    }
}

fn multi_valued(query: &[(String, String)]) -> BTreeMap<String, Vec<String>> {
    query
        .iter()
        .fold(BTreeMap::new(), |mut values, (name, value)| {
            values
                .entry(name.clone())
                .or_insert_with(Vec::new)
                .push(value.clone());
            values
        })
}

impl RequestBuilder {
    /// `what` was given, which only a route's request carries.
    fn of_a_route(mut self, what: &'static str) -> Self {
        self.route_only.insert(what);
        self
    }

    pub fn method(self, method: &str) -> Self {
        Self {
            method: method.to_string(),
            ..self.of_a_route("a method")
        }
    }

    pub fn handler(self, handler: &str) -> Self {
        Self {
            handler: handler.to_string(),
            ..self
        }
    }

    pub fn tenant(self, tenant: &str) -> Self {
        Self {
            tenant: tenant.to_string(),
            ..self
        }
    }

    pub fn service(self, service: &str) -> Self {
        Self {
            service: service.to_string(),
            ..self
        }
    }

    /// A header; its name reaches the handler in lowercase. The platform
    /// keeps credentials (`authorization`, `cookie`) from a handler whose
    /// route authenticates, and so should a test.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .insert(name.to_ascii_lowercase(), Value::String(value.to_string()));
        self.of_a_route("headers")
    }

    /// A JSON body, sent as its compact text.
    pub fn json(self, body: &Value) -> Self {
        Self {
            raw_body: body.to_string().into_bytes(),
            body: body.clone(),
            ..self
        }
    }

    /// The bytes the caller sent; the body a handler parses is their JSON,
    /// or their text when they are not JSON.
    pub fn raw_body(self, body: impl Into<Vec<u8>>) -> Self {
        let raw_body = body.into();
        Self {
            body: parsed(&raw_body),
            raw_body,
            ..self.of_a_route("a raw body")
        }
    }

    /// A query parameter, added to the ones given before.
    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.query.push((name.to_string(), value.to_string()));
        let query_string = serde_urlencoded::to_string(&self.query).unwrap_or_default();
        Self {
            query_string,
            ..self.of_a_route("a query")
        }
    }

    /// The query string as the caller wrote it, without `?`, in place of
    /// any parameter given before. It is read as the platform reads one: a
    /// malformed escape is kept as written, never dropped.
    pub fn query_string(self, query_string: &str) -> Self {
        Self {
            query: serde_urlencoded::from_str(query_string).unwrap_or_default(),
            query_string: query_string.to_string(),
            ..self.of_a_route("a query")
        }
    }

    /// The caller the platform authenticated.
    pub fn auth(self, auth: AuthBuilder) -> Self {
        self.auth_json(auth.into_value())
    }

    /// The `auth` object itself.
    pub fn auth_json(self, auth: Value) -> Self {
        Self { auth, ..self }
    }

    /// The posture the route declares, as its manifest writes it:
    /// `{"kind": "owner", "field": "customer_id"}`.
    pub fn posture(self, posture: Value) -> Self {
        Self {
            posture,
            ..self.of_a_route("a posture")
        }
    }

    /// The path a `routing: strict` route matched, below the service.
    pub fn path(self, path: &str) -> Self {
        Self {
            path: Some(path.to_string()),
            v2: true,
            ..self.of_a_route("a path")
        }
    }

    /// A percent-decoded parameter of the path, in path order.
    pub fn path_param(mut self, name: &str, value: &str) -> Self {
        self.path_params.push((name.to_string(), value.to_string()));
        Self {
            v2: true,
            ..self.of_a_route("path parameters")
        }
    }

    /// A route that is not strict and declares `body_limit`, `timeout`,
    /// `multipart` or another keyword of R2: its handler reads the query,
    /// the caller's address and the parts, as a strict route's does.
    pub fn declares_r2(self) -> Self {
        Self {
            v2: true,
            ..self.of_a_route("the keys of R2")
        }
    }

    pub fn client_ip(self, address: &str) -> Self {
        Self {
            client_ip: Some(address.to_string()),
            v2: true,
            ..self.of_a_route("a client address")
        }
    }

    /// A part of a `multipart` route's form.
    pub fn part(
        mut self,
        name: &str,
        filename: Option<&str>,
        content_type: Option<&str>,
        data: impl Into<Vec<u8>>,
    ) -> Self {
        self.multipart
            .get_or_insert_with(Vec::new)
            .push(MultipartField {
                name: Some(name.to_string()),
                filename: filename.map(str::to_string),
                content_type: content_type.map(str::to_string),
                data: data.into(),
            });
        Self {
            v2: true,
            ..self.of_a_route("parts")
        }
    }

    /// The service calling an `internal` route, `depth` calls deep.
    pub fn caller(self, service: &str, on_behalf_of: Option<&str>, depth: u32) -> Self {
        Self {
            caller: Some(json!({
                "service": service,
                "on_behalf_of": on_behalf_of,
                "depth": depth,
            })),
            ..self.of_a_route("a calling service")
        }
    }

    /// The partner key calling an `api_key` route.
    pub fn api_key(self, id: &str, name: &str, scopes: &[&str]) -> Self {
        Self {
            api_key: Some(json!({ "id": id, "name": name, "scopes": scopes })),
            v2: true,
            ..self.of_a_route("a partner key")
        }
    }

    /// The rows the caller of a `scoped` route may reach, as the platform
    /// writes them: `{"kind": "all"}`.
    pub fn scope(self, scope: Value) -> Self {
        Self {
            scope: Some(scope),
            v2: true,
            ..self.of_a_route("a scope")
        }
    }

    /// A CRUD hook or `on_migrate`; its body is what the hook is told.
    pub fn hook(self) -> Self {
        Self {
            kind: Kind::Delivery,
            method: METHOD_HOOK.to_string(),
            ..self
        }
    }

    /// A message of a subscription, with the delivery behind it.
    pub fn subscription(self, delivery: SubscriptionBuilder) -> Self {
        Self {
            subscription: Some(delivery.0),
            ..self.subscription_without_delivery()
        }
    }

    /// A message of a subscription as a platform delivers it that says
    /// nothing of the delivery.
    pub fn subscription_without_delivery(self) -> Self {
        Self {
            kind: Kind::Delivery,
            method: METHOD_SUBSCRIPTION.to_string(),
            subscription: None,
            ..self
        }
    }

    /// The fire of the schedule `name` that was due at `fire_at` (RFC
    /// 3339), on time.
    pub fn schedule(self, name: &str, fire_at: &str) -> Self {
        self.fire(name, fire_at, false)
    }

    /// A fire delivered late, after an outage.
    pub fn schedule_catch_up(self, name: &str, fire_at: &str) -> Self {
        self.fire(name, fire_at, true)
    }

    fn fire(self, name: &str, fire_at: &str, catch_up: bool) -> Self {
        let delivery = SubscriptionBuilder::on(&format!(
            "cufflink.schedule.{}.{}.{name}",
            self.tenant, self.service
        ))
        .stream(SCHEDULES_STREAM)
        .published_at(fire_at);
        Self {
            kind: Kind::Delivery,
            method: METHOD_SCHEDULE.to_string(),
            body: json!(ScheduleFire {
                schedule: name.to_string(),
                fire_at: fire_at.to_string(),
                catch_up,
            }),
            subscription: Some(delivery.0),
            ..self
        }
    }

    /// The request of a job a guest enqueued with `jobs::enqueue`, which
    /// has neither headers nor a posture.
    pub fn enqueued(self) -> Self {
        Self {
            kind: Kind::Enqueued,
            ..self
        }
    }

    /// A worker's `attempt` at running the request as a job, of three or
    /// as many as it takes.
    pub fn job(self, attempt: u32) -> Self {
        self.job_with(JobMeta {
            id: JOB_ID.to_string(),
            attempt,
            max_attempts: attempt.max(DEFAULT_MAX_ATTEMPTS),
            ..JobMeta::default()
        })
    }

    pub fn job_with(self, job: JobMeta) -> Self {
        Self {
            job: Some(job),
            ..self
        }
    }

    fn guest(&self) -> GuestRequest {
        let common = GuestRequest {
            method: self.method.clone(),
            handler: self.handler.clone(),
            tenant: self.tenant.clone(),
            service: self.service.clone(),
            auth: Some(self.auth.clone()),
            job: self.job.clone(),
            ..GuestRequest::default()
        };
        match self.kind {
            Kind::Enqueued => GuestRequest {
                body: self.body.clone(),
                ..common
            },
            Kind::Delivery => GuestRequest {
                headers: Some(Map::new()),
                body: self.body.clone(),
                subscription: self.subscription.clone(),
                ..common
            },
            Kind::Route => {
                let route = GuestRequest {
                    headers: Some(self.headers.clone()),
                    body: with_params(self.body.clone(), &self.query),
                    body_raw_b64: Some(STANDARD.encode(&self.raw_body)),
                    posture: Some(self.posture.clone()),
                    caller: self.caller.clone(),
                    ..common
                };
                if self.v2 {
                    GuestRequest {
                        path: self.path.clone(),
                        path_params: self.path.as_ref().map(|_| self.path_params.clone()),
                        query_string: Some(self.query_string.clone()),
                        query: Some(multi_valued(&self.query)),
                        client_ip: self.client_ip.clone(),
                        multipart: self.multipart.clone(),
                        api_key: self.api_key.clone(),
                        scope: self.scope.clone(),
                        ..route
                    }
                } else {
                    route
                }
            }
        }
    }

    /// What makes the request one the platform would never send.
    fn refused(&self) -> Option<String> {
        if self.path.is_none() && !self.path_params.is_empty() {
            Some("a path parameter needs the path it is a parameter of".to_string())
        } else if self.kind != Kind::Route && !self.route_only.is_empty() {
            let given: Vec<&str> = self.route_only.iter().copied().collect();
            Some(format!("only a route has {}", given.join(", ")))
        } else {
            None
        }
    }

    /// The JSON the platform would hand the handler.
    pub fn to_value(&self) -> Result<Value, TestHostError> {
        if let Some(reason) = self.refused() {
            return Err(TestHostError::Request(reason));
        }
        self.guest()
            .to_value()
            .map_err(|e| TestHostError::Request(format!("the request is not JSON: {e}")))
    }

    pub fn build(&self) -> Result<Request, TestHostError> {
        let text = self.to_value()?.to_string();
        Request::from_json(&text)
            .ok_or_else(|| TestHostError::Request("the request does not parse".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pinned(text: &str) -> Value {
        serde_json::from_str(text).expect("pinned text")
    }

    fn webhook() -> RequestBuilder {
        request()
            .handler("stripe_webhook")
            .tenant("acme")
            .service("payments")
            .header("Stripe-Signature", "t=1,v1=x")
            .header("x-shopify-hmac-sha256", "h")
            .header("x-sweep-secret", "s")
            .query("source", "stripe")
    }

    /// The texts are the ones the platform's request builders are pinned
    /// to (`…_reads_the_text_it_always_read`).
    #[test]
    fn a_route_request_is_the_one_the_platform_builds() {
        assert_eq!(
            webhook()
                .raw_body(&b"{\"id\":\"evt_1\"}"[..])
                .auth_json(json!({"sub": "kc-user-1"}))
                .posture(json!({"kind": "permission", "area": "payments", "operation": "edit"}))
                .to_value()
                .expect("value"),
            pinned(
                r#"{"auth":{"sub":"kc-user-1"},"body":{"id":"evt_1","params":{"source":"stripe"}},"body_raw_b64":"eyJpZCI6ImV2dF8xIn0=","handler":"stripe_webhook","headers":{"stripe-signature":"t=1,v1=x","x-shopify-hmac-sha256":"h","x-sweep-secret":"s"},"method":"POST","posture":{"area":"payments","kind":"permission","operation":"edit"},"service":"payments","tenant":"acme"}"#
            )
        );
        assert_eq!(
            webhook().to_value().expect("value"),
            pinned(
                r#"{"auth":null,"body":{"params":{"source":"stripe"}},"body_raw_b64":"","handler":"stripe_webhook","headers":{"stripe-signature":"t=1,v1=x","x-shopify-hmac-sha256":"h","x-sweep-secret":"s"},"method":"POST","posture":null,"service":"payments","tenant":"acme"}"#
            )
        );
    }

    /// The platform's fixture hands its builder the legacy parameters
    /// apart from the query string; a request has them from it.
    #[test]
    fn a_strict_route_request_adds_what_the_platform_adds() {
        let built = request()
            .handler("import")
            .tenant("acme")
            .service("books")
            .path("/books/7/import")
            .path_param("book_id", "7")
            .query_string("tag=a&tag=b&q=x%20y&empty")
            .client_ip("203.0.113.7")
            .part("file", Some("r.csv"), Some("text/csv"), &b"a,b"[..])
            .scope(json!({"kind": "all"}))
            .to_value()
            .expect("value");
        let mut expected = pinned(
            r#"{"auth":null,"body":{"params":{"tag":"b"}},"body_raw_b64":"","client_ip":"203.0.113.7","handler":"import","headers":{},"method":"POST","multipart":[{"content_type":"text/csv","data_b64":"YSxi","filename":"r.csv","name":"file"}],"path":"/books/7/import","path_params":[["book_id","7"]],"posture":null,"query":{"empty":[""],"q":["x y"],"tag":["a","b"]},"query_string":"tag=a&tag=b&q=x%20y&empty","scope":{"kind":"all"},"service":"books","tenant":"acme"}"#,
        );
        expected["body"]["params"] = json!({"tag": "b", "q": "x y", "empty": ""});
        assert_eq!(built, expected);
    }

    #[test]
    fn a_legacy_route_keeps_its_query_in_the_body_alone() {
        let built = webhook().to_value().expect("value");
        assert!(built.get("query").is_none() && built.get("query_string").is_none());
        let declared = webhook().declares_r2().to_value().expect("value");
        assert_eq!(declared["query_string"], "source=stripe");
        assert_eq!(declared["query"], json!({"source": ["stripe"]}));
        assert!(declared.get("path").is_none() && declared.get("path_params").is_none());
        let request = webhook().declares_r2().build().expect("request");
        assert_eq!(request.query_first("source"), Some("stripe"));
        assert_eq!(request.body()["params"]["source"], "stripe");
        assert_eq!(request.header("STRIPE-SIGNATURE"), Some("t=1,v1=x"));
    }

    #[test]
    fn bodies_are_read_as_the_platform_reads_them() {
        let body = |builder: RequestBuilder| builder.to_value().expect("value")["body"].clone();
        assert_eq!(body(request().raw_body("not json")), json!("not json"));
        assert_eq!(
            body(request().raw_body("[1]").query("a", "1")),
            json!({"data": [1], "params": {"a": "1"}})
        );
        assert_eq!(body(request().raw_body("")), Value::Null);
        let signed = request()
            .raw_body(&b"{ \"id\" : 1 }"[..])
            .build()
            .expect("request");
        assert_eq!(signed.raw_body(), b"{ \"id\" : 1 }");
        assert_eq!(signed.body(), &json!({"id": 1}));
        let sent = request()
            .json(&json!({"b": 1, "a": 2}))
            .build()
            .expect("request");
        assert_eq!(sent.raw_body(), br#"{"a":2,"b":1}"#);
    }

    #[test]
    fn a_hook_and_a_message_are_what_the_platform_delivers() {
        let item =
            json!({"hook": "before_create", "table": "items", "data": {"title": "x", "amount": 3}});
        assert_eq!(
            request()
                .hook()
                .handler("validate_item")
                .tenant("acme")
                .service("ferry-service")
                .json(&item)
                .to_value()
                .expect("value"),
            pinned(
                r#"{"auth":null,"body":{"data":{"amount":3,"title":"x"},"hook":"before_create","table":"items"},"handler":"validate_item","headers":{},"method":"HOOK","service":"ferry-service","tenant":"acme"}"#
            )
        );
        let message = request()
            .tenant("acme")
            .service("loan-service")
            .handler("on_book")
            .json(&json!({"id": 7, "data": {"title": "x", "amount": 3}}));
        assert_eq!(
            message
                .clone()
                .subscription_without_delivery()
                .to_value()
                .expect("value"),
            pinned(
                r#"{"auth":null,"body":{"data":{"amount":3,"title":"x"},"id":7},"handler":"on_book","headers":{},"method":"SUBSCRIPTION","service":"loan-service","tenant":"acme"}"#
            )
        );
        assert_eq!(
            message
                .clone()
                .subscription(
                    SubscriptionBuilder::on("dw.acme.books.created")
                        .stream("DW_ACME")
                        .consumer("loan_service_on_book")
                        .sequence(81_234)
                        .consumer_sequence(17)
                        .attempt(2)
                        .pending(3)
                        .published_at("2026-09-28T10:00:00.250Z"),
                )
                .to_value()
                .expect("value"),
            pinned(concat!(
                r#"{"auth":null,"body":{"data":{"amount":3,"title":"x"},"id":7},"handler":"on_book","headers":{},"method":"SUBSCRIPTION","service":"loan-service","#,
                r#""subscription":{"consumer":"loan_service_on_book","consumer_sequence":17,"headers":{},"num_delivered":2,"num_pending":3,"published_at":"2026-09-28T10:00:00.250Z","stream":"DW_ACME","stream_sequence":81234,"subject":"dw.acme.books.created"},"#,
                r#""tenant":"acme"}"#
            ))
        );
        let delivered = message
            .subscription(
                SubscriptionBuilder::on("books.renewal.set")
                    .stream("BOOKS")
                    .consumer("loan-service-book-consumer")
                    .sequence(81_234)
                    .attempt(2)
                    .pending(3)
                    .published_at("2026-09-28T10:00:00Z")
                    .msg_id("book-7"),
            )
            .build()
            .expect("request");
        let delivery = delivered.subscription().expect("delivery");
        assert_eq!(delivery.subject(), "books.renewal.set");
        assert_eq!(delivery.stream(), "BOOKS");
        assert_eq!(delivery.consumer(), "loan-service-book-consumer");
        assert_eq!((delivery.sequence(), delivery.attempt()), (81_234, 2));
        assert!(delivery.is_redelivery());
        assert_eq!(delivery.pending(), 3);
        assert_eq!(delivery.msg_id(), Some("book-7"));
        assert!(delivered.auth().is_none() && delivered.headers().is_empty());
    }

    #[test]
    fn a_fire_names_its_schedule() {
        let fire = request()
            .tenant("harbor")
            .service("order-service")
            .handler("retry_orders")
            .schedule_catch_up("order_retry", "2026-09-28T10:00:30Z")
            .build()
            .expect("request");
        let schedule = fire.schedule().expect("schedule");
        assert_eq!(schedule.name(), "order_retry");
        assert_eq!(schedule.fire_at(), "2026-09-28T10:00:30Z");
        assert!(schedule.is_catch_up());
        let delivery = fire.subscription().expect("delivery");
        assert_eq!(
            (delivery.subject(), delivery.stream()),
            (
                "cufflink.schedule.harbor.order-service.order_retry",
                "CUFFLINK_SCHEDULES"
            )
        );
        let on_time = request()
            .schedule("nightly", "2026-09-28T02:00:00Z")
            .build()
            .expect("request");
        assert_eq!(
            on_time.schedule().map(|fire| fire.is_catch_up()),
            Some(false)
        );
    }

    #[test]
    fn a_job_is_the_request_it_was_enqueued_with_and_its_attempt() {
        let enqueued = request()
            .enqueued()
            .handler("handle_run_report")
            .tenant("acme")
            .service("stock-service")
            .auth_json(json!({"sub": "user-1"}))
            .json(&json!({"id": "item-1"}));
        assert_eq!(
            enqueued.to_value().expect("value"),
            pinned(
                r#"{"auth":{"sub":"user-1"},"body":{"id":"item-1"},"handler":"handle_run_report","method":"POST","service":"stock-service","tenant":"acme"}"#
            )
        );
        let second = enqueued
            .job_with(JobMeta {
                id: "00000000-0000-0000-0000-000000000000".into(),
                attempt: 2,
                max_attempts: 3,
                ..JobMeta::default()
            })
            .to_value()
            .expect("value");
        assert_eq!(
            second,
            pinned(
                r#"{"_job":{"attempt":2,"id":"00000000-0000-0000-0000-000000000000","max_attempts":3},"auth":{"sub":"user-1"},"body":{"id":"item-1"},"handler":"handle_run_report","method":"POST","service":"stock-service","tenant":"acme"}"#
            )
        );
        let route = request().job(5).build().expect("request");
        let job = route.job().expect("job");
        assert_eq!((job.attempt, job.max_attempts), (5, 5));
        assert!(job.is_retry() && job.last_error().is_none());
        assert_eq!(
            request()
                .job(1)
                .build()
                .expect("request")
                .job()
                .map(|j| j.max_attempts),
            Some(3)
        );
    }

    #[test]
    fn a_caller_is_described_as_the_platform_describes_one() {
        let ada = AuthBuilder::user("kc-user-1")
            .username("ada")
            .name("Ada")
            .email("ada@example.com")
            .realm_roles(&["staff"])
            .claim("customer_id", "c-1")
            .permissions(&["orders:view", "orders:edit"])
            .role_names(&["staff"]);
        assert_eq!(
            request().auth(ada.clone()).to_value().expect("value")["auth"],
            json!({
                "sub": "kc-user-1",
                "preferred_username": "ada",
                "name": "Ada",
                "email": "ada@example.com",
                "realm_roles": ["staff"],
                "claims": {"customer_id": "c-1"},
                "is_service_account": false,
                "permissions": ["orders:view", "orders:edit"],
                "role_names": ["staff"],
            })
        );
        let scoped = request()
            .auth(ada.scoped_permissions(&["orders:view@all", "books:view@own"]))
            .build()
            .expect("request");
        let auth = scoped.auth().expect("auth");
        assert!(auth.has_role("staff") && auth.can("orders", "edit") && auth.can("books", "view"));
        assert_eq!(
            auth.permissions,
            ["orders:view", "orders:edit", "books:view"]
        );
        assert_eq!(
            auth.scoped_permissions,
            ["orders:view@all", "books:view@own"]
        );
        assert_eq!(auth.claim("customer_id"), Some(&json!("c-1")));

        let ops = request()
            .auth(AuthBuilder::service_account("service-account-harbor-ops"))
            .build()
            .expect("request");
        assert!(ops.auth().is_some_and(|auth| auth.is_service_account));
        let anonymous = request().build().expect("request");
        assert!(anonymous.auth().is_none() && anonymous.require_auth().is_err());
    }

    #[test]
    fn a_request_the_platform_would_never_send_is_not_built() {
        let reason = |builder: RequestBuilder| match builder.build() {
            Err(TestHostError::Request(reason)) => reason,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            reason(request().path_param("loan_id", "7")),
            "a path parameter needs the path it is a parameter of"
        );
        for (builder, given) in [
            (request().path("/loans/7").hook(), "a path"),
            (
                request()
                    .subscription_without_delivery()
                    .client_ip("203.0.113.7"),
                "a client address",
            ),
            (
                request()
                    .schedule("nightly", "2026-09-28T02:00:00Z")
                    .scope(json!({"kind": "all"})),
                "a scope",
            ),
            (request().enqueued().declares_r2(), "the keys of R2"),
            (
                request()
                    .subscription(SubscriptionBuilder::on("books.created"))
                    .query("page", "2"),
                "a query",
            ),
            (request().hook().header("x-request-id", "r-1"), "headers"),
            (request().method("PUT").hook(), "a method"),
            (request().enqueued().raw_body("raw"), "a raw body"),
            (
                request()
                    .enqueued()
                    .posture(json!({"kind": "authenticated"})),
                "a posture",
            ),
            (
                request().hook().caller("pricing", None, 1),
                "a calling service",
            ),
        ] {
            assert_eq!(reason(builder), format!("only a route has {given}"));
        }
        assert_eq!(
            reason(request().hook().header("x-a", "1").query_string("page=2")),
            "only a route has a query, headers"
        );
        assert!(request().hook().json(&json!({"id": 7})).build().is_ok());
        assert_eq!(
            request()
                .declares_r2()
                .query_string("page=%ZZ&x")
                .build()
                .expect("request")
                .query_first("page"),
            Some("%ZZ"),
            "a malformed escape is kept"
        );
        assert!(request()
            .path("/loans/7")
            .path_param("loan_id", "7")
            .build()
            .is_ok());
    }

    #[test]
    fn postures_callers_and_keys_reach_the_handler() {
        let owned = request()
            .posture(json!({"kind": "owner", "field": "customer_id"}))
            .build()
            .expect("request");
        assert_eq!(owned.owner_field(), Some("customer_id"));
        let internal = request()
            .caller("pricing", Some("kc-1"), 2)
            .build()
            .expect("request");
        assert_eq!(
            internal
                .caller()
                .map(|c| (c.service.as_str(), c.on_behalf_of.as_deref(), c.depth)),
            Some(("pricing", Some("kc-1"), 2))
        );
        let partner = request()
            .api_key("k-1", "acme-ingest", &["books:*"])
            .build()
            .expect("request");
        assert_eq!(
            partner.api_key().map(|key| key.scopes.clone()),
            Some(vec!["books:*".to_string()])
        );
    }
}
