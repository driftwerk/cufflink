use crate::features;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CustomRouteDefinition {
    pub method: String,
    pub path: String,
    pub handler: String,
    /// When present, the HTTP route accepts the request, persists a
    /// `wasm_jobs` row, publishes to JetStream, and returns `202 Accepted`
    /// with `{job_id}`. The handler executes asynchronously on a worker
    /// with no inline 30s wall.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<JobConfig>,
    /// Who may call this route. `None` marks a manifest built before route
    /// postures existed: the platform runs it in legacy mode (best-effort auth,
    /// no enforcement) and warns. `Some` — always emitted by the current SDK —
    /// makes the platform enforce the declared posture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub posture: Option<RoutePosture>,
    /// Pass the caller's `authorization`, `cookie` and `proxy-authorization`
    /// headers into the guest. Without it they are stripped once the tenant's
    /// `tenant_binding` policy is enforced.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forward_auth: bool,
    /// The largest request body the route accepts; 4 MiB when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_limit_bytes: Option<u64>,
    /// The route's execution budget; 30 s when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u32>,
    /// Parse a `multipart/form-data` body into parts for the guest.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub multipart: bool,
    /// Serve the route as a server-sent event stream fed by a NATS subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse: Option<SseRouteDef>,
    /// The route's OpenAPI description; documentation only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<RouteDoc>,
    /// What of the request body no audit record of a call holds: paths as
    /// a table's `audit_redact` names columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_redact: Vec<String>,
}

/// How a custom route appears in the service's OpenAPI spec. Schemas are
/// JSON Schema (draft 2020-12) referring to `#/components/schemas/…`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RouteDoc {
    /// Defaults to the handler name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Defaults to the service name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deprecated: bool,
    /// An object schema whose properties are the path parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_params: Option<Value>,
    /// An object schema whose properties are the query parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<BodyDoc>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub responses: BTreeMap<u16, ResponseDoc>,
    /// Security scheme names; `None` derives them from the posture.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub security: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BodyDoc {
    pub content_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseDoc {
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content: Vec<MediaDoc>,
}

impl ResponseDoc {
    /// The description a response gets when its declaration gives none: the
    /// status's reason phrase.
    pub fn default_description(status: u16) -> &'static str {
        match status {
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            204 => "No Content",
            301 => "Moved Permanently",
            302 => "Found",
            303 => "See Other",
            304 => "Not Modified",
            307 => "Temporary Redirect",
            308 => "Permanent Redirect",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            409 => "Conflict",
            413 => "Payload Too Large",
            415 => "Unsupported Media Type",
            422 => "Unprocessable Entity",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            _ => "Response",
        }
    }
}

/// A response body of one media type; without a schema it documents a body
/// of that type (a file) with no structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaDoc {
    pub content_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
}

/// The NATS subject an event-stream route bridges, and the handler that
/// turns each message into events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseRouteDef {
    /// Dot-separated tokens starting `dw.{tenant}.`; a `{name}` token is the
    /// route's path parameter of that name. `*` and `>` wildcards are allowed.
    pub subject: String,
    /// Exported handler reducing each message; without one every message is
    /// forwarded as a data-only event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reducer: Option<String>,
    /// Streams to callers the route does not authenticate: required on a
    /// public or undeclared posture.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub anonymous: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseSubjectError {
    /// A `{name}` placeholder with no value.
    Missing(String),
    /// A value that is not a single subject token.
    InvalidToken(String),
}

impl std::fmt::Display for SseSubjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(name) => write!(f, "the event subject needs path parameter '{name}'"),
            Self::InvalidToken(name) => write!(
                f,
                "path parameter '{name}' must be one subject token (no '.', '*', '>' or whitespace)"
            ),
        }
    }
}

impl std::error::Error for SseSubjectError {}

impl SseRouteDef {
    /// Every subject starts with this: an event stream reads only its own
    /// tenant's subjects.
    pub const SUBJECT_PREFIX: &'static str = "dw.{tenant}.";

    /// The method of the request a reducer receives.
    pub const REDUCER_METHOD: &'static str = "SSE_EVENT";

    /// Tokens after the prefix the platform itself publishes under (the
    /// tenant's audit log), never bridged.
    pub const RESERVED_TOKENS: &'static [&'static str] = &["logs"];

    /// Why the subject cannot be bridged for a route whose path parameters
    /// are `params` (`None` on a legacy route, which has none): it must be
    /// dot-separated tokens under the prefix, name its events with a literal
    /// token that is not reserved, and bind every `{name}` but `{tenant}`
    /// to a path parameter.
    pub fn subject_violation(&self, params: Option<&[String]>) -> Option<String> {
        let tokens: Vec<&str> = self.subject.split('.').collect();
        let malformed = tokens.iter().any(|token| {
            token.is_empty()
                || (token.contains(['{', '}']) && placeholder(token).is_none_or(str::is_empty))
        });
        if !self.subject.starts_with(Self::SUBJECT_PREFIX) || malformed {
            return Some(format!(
                "sse subject '{}' must be dot-separated tokens starting {}",
                self.subject,
                Self::SUBJECT_PREFIX
            ));
        }
        let named = tokens.get(2).copied().unwrap_or_default();
        if matches!(named, "*" | ">") || placeholder(named).is_some() {
            return Some(format!(
                "sse subject '{}' must name its events with a literal token after {}",
                self.subject,
                Self::SUBJECT_PREFIX
            ));
        }
        if Self::RESERVED_TOKENS.contains(&named) {
            return Some(format!(
                "sse subject '{}' reads dw.{{tenant}}.{named}, which the platform reserves",
                self.subject
            ));
        }
        let unbound = self.placeholders().find(|name| {
            *name != "tenant" && !params.is_some_and(|params| params.iter().any(|p| p == name))
        })?;
        Some(match params {
            Some(_) => format!(
                "sse subject placeholder '{{{unbound}}}' is not a path parameter of this strict route"
            ),
            None => format!(
                "sse subject placeholder '{{{unbound}}}' needs routing: strict, where it names a path parameter"
            ),
        })
    }

    /// Why the stream cannot be served on a posture that admits anonymous
    /// callers (public or undeclared) unless it opts in.
    pub fn anonymous_violation(&self, anonymous_posture: bool) -> Option<&'static str> {
        (anonymous_posture && !self.anonymous).then_some(
            "sse on a public or undeclared posture streams to anonymous callers; \
             declare anonymous = true to allow it",
        )
    }

    /// The names of the template's `{name}` tokens, `tenant` included.
    pub fn placeholders(&self) -> impl Iterator<Item = &str> {
        self.subject.split('.').filter_map(placeholder)
    }

    /// The subject for `tenant` and the route's path parameters.
    pub fn resolve(
        &self,
        tenant: &str,
        params: &[(String, String)],
    ) -> Result<String, SseSubjectError> {
        self.subject
            .split('.')
            .map(|token| match placeholder(token) {
                None => Ok(token),
                Some("tenant") => Ok(tenant),
                Some(name) => params
                    .iter()
                    .find(|(param, _)| param == name)
                    .map(|(_, value)| value.as_str())
                    .ok_or_else(|| SseSubjectError::Missing(name.to_string()))
                    .and_then(|value| {
                        if is_subject_token(value) {
                            Ok(value)
                        } else {
                            Err(SseSubjectError::InvalidToken(name.to_string()))
                        }
                    }),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|tokens| tokens.join("."))
    }
}

fn placeholder(token: &str) -> Option<&str> {
    token.strip_prefix('{').and_then(|t| t.strip_suffix('}'))
}

fn is_subject_token(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|c| matches!(c, '.' | '*' | '>') || c.is_whitespace())
}

/// The authorization posture of a custom route, enforced by the platform so a
/// guest handler cannot forget it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutePosture {
    /// Deliberately anonymous. `reason` is mandatory so every public route is
    /// greppable and reviewable (third-party webhooks, signup, public profiles).
    Public { reason: String },
    /// Any authenticated tenant principal.
    Authenticated,
    /// Authenticated and holding the `{area}:{operation}` permission.
    Permission { area: String, operation: String },
    /// Authenticated; the guest scopes the operation to rows whose `field`
    /// equals the caller's subject. The platform enforces authentication.
    Owner { field: String },
    /// Only another service of the tenant, calling through the platform
    /// with a platform-minted identity: `callers` names the services
    /// allowed, or is `["*"]` for any. User credentials are not accepted.
    Internal { callers: Vec<String> },
    /// Only a partner key (`X-Api-Key`) of the tenant holding a scope that
    /// covers `scope` (`resource[:action]`, `resource:*` covering every
    /// action). User credentials are not accepted.
    ApiKey { scope: String },
    /// Authenticated and holding a grant on `{area}:{operation}`; the
    /// platform resolves the grants' scopes (`@own`, `@field(…)`, `@all`)
    /// into the rows the guest may reach, `@own` meaning `owner_field`
    /// holds the caller's subject.
    Scoped {
        area: String,
        operation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        owner_field: Option<String>,
    },
}

impl CustomRouteDefinition {
    /// The body cap of a route that declares none.
    pub const DEFAULT_BODY_LIMIT_BYTES: u64 = 4 * 1024 * 1024;
    /// The largest `body_limit_bytes` a route may declare.
    pub const MAX_BODY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
    /// The range `timeout_secs` must fall in.
    pub const TIMEOUT_SECS: std::ops::RangeInclusive<u32> = 1..=120;
    /// The methods a multipart route may use.
    pub const MULTIPART_METHODS: [&'static str; 3] = ["POST", "PUT", "PATCH"];

    /// Platform features this route's declaration requires.
    pub fn derived_features(&self) -> impl Iterator<Item = &'static str> {
        let jobs_v2 = self.job.as_ref().is_some_and(JobConfig::uses_v2);
        self.request_features()
            .chain(jobs_v2.then_some(features::JOBS_V2))
    }

    /// The features that shape how the route is reached and what its guest
    /// is sent.
    fn request_features(&self) -> impl Iterator<Item = &'static str> {
        let limits =
            self.body_limit_bytes.is_some() || self.timeout_secs.is_some() || self.multipart;
        [
            self.posture.is_some().then_some(features::ROUTE_POSTURE),
            limits.then_some(features::ROUTE_LIMITS),
            self.sse.is_some().then_some(features::SSE_BRIDGE),
            self.posture.as_ref().and_then(RoutePosture::feature),
            (!self.audit_redact.is_empty()).then_some(features::AUDIT_REDACTION),
        ]
        .into_iter()
        .flatten()
    }

    /// Whether the guest receives the R2 request keys (`query`, `client_ip`,
    /// …): only a route declaring an R2 capability does, so every other
    /// guest's request JSON stays byte-identical.
    pub fn uses_request_v2(&self) -> bool {
        self.request_features()
            .any(|feature| feature != features::ROUTE_POSTURE)
    }
}

impl RoutePosture {
    /// Whether a caller with no credentials can reach this route.
    pub fn is_anonymous(&self) -> bool {
        matches!(self, RoutePosture::Public { .. })
    }

    /// Short human label for audit output.
    pub fn label(&self) -> &'static str {
        match self {
            RoutePosture::Public { .. } => "public",
            RoutePosture::Authenticated => "authenticated",
            RoutePosture::Permission { .. } => "permission",
            RoutePosture::Owner { .. } => "owner",
            RoutePosture::Internal { .. } => "internal",
            RoutePosture::ApiKey { .. } => "api_key",
            RoutePosture::Scoped { .. } => "scoped",
        }
    }

    /// The platform feature this posture needs beyond `route_posture`.
    pub fn feature(&self) -> Option<&'static str> {
        match self {
            RoutePosture::Internal { .. } => Some(features::INTERNAL_POSTURE),
            RoutePosture::ApiKey { .. } => Some(features::API_KEY_POSTURE),
            RoutePosture::Scoped { .. } => Some(features::SCOPED_POSTURE),
            RoutePosture::Public { .. }
            | RoutePosture::Authenticated
            | RoutePosture::Permission { .. }
            | RoutePosture::Owner { .. } => None,
        }
    }

    /// Whether `service` may call a route of this posture: listed, or any
    /// service when `callers` is `["*"]`.
    pub fn admits_caller(callers: &[String], service: &str) -> bool {
        callers
            .iter()
            .any(|caller| caller == "*" || caller == service)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct JobConfig {
    /// Per-handler wall-clock ceiling enforced by the worker.
    pub timeout_secs: u32,
    /// Maximum delivery attempts before the job is permanently failed.
    /// `1` disables retry-on-crash.
    pub max_attempts: u32,
    /// The failures retried beside a crash, which always is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retry_on: Vec<RetryOn>,
    /// The delay between attempts; 30 s doubling to an hour when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff: Option<BackoffSpec>,
}

impl JobConfig {
    pub const DEFAULT_TIMEOUT_SECS: u32 = 600;
    pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

    /// Whether the job declares retry behaviour only a jobs-v2 platform
    /// provides.
    pub fn uses_v2(&self) -> bool {
        !self.retry_on.is_empty() || self.backoff.is_some()
    }
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            timeout_secs: Self::DEFAULT_TIMEOUT_SECS,
            max_attempts: Self::DEFAULT_MAX_ATTEMPTS,
            retry_on: Vec::new(),
            backoff: None,
        }
    }
}

/// A failure a job opts into retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryOn {
    /// The handler returned an error or a 5xx status.
    Error,
    /// The handler ran out of its `timeout_secs`.
    Timeout,
}

impl RetryOn {
    pub fn as_str(self) -> &'static str {
        match self {
            RetryOn::Error => "error",
            RetryOn::Timeout => "timeout",
        }
    }
}

/// Exponential backoff between a job's attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackoffSpec {
    #[serde(default = "BackoffSpec::default_base_secs")]
    pub base_secs: u32,
    #[serde(default = "BackoffSpec::default_max_secs")]
    pub max_secs: u32,
}

impl BackoffSpec {
    pub const DEFAULT_BASE_SECS: u32 = 30;
    pub const DEFAULT_MAX_SECS: u32 = 3600;
    /// The range both delays must fall in.
    pub const SECS: std::ops::RangeInclusive<u32> = 1..=86_400;

    fn default_base_secs() -> u32 {
        Self::DEFAULT_BASE_SECS
    }

    fn default_max_secs() -> u32 {
        Self::DEFAULT_MAX_SECS
    }
}

impl Default for BackoffSpec {
    fn default() -> Self {
        Self {
            base_secs: Self::DEFAULT_BASE_SECS,
            max_secs: Self::DEFAULT_MAX_SECS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_config_defaults() {
        let cfg = JobConfig::default();
        assert_eq!(cfg.timeout_secs, JobConfig::DEFAULT_TIMEOUT_SECS);
        assert_eq!(cfg.max_attempts, JobConfig::DEFAULT_MAX_ATTEMPTS);
        assert!(!cfg.uses_v2());
        assert_eq!(
            serde_json::to_value(&cfg).unwrap(),
            serde_json::json!({"timeout_secs": 600, "max_attempts": 3})
        );
    }

    #[test]
    fn job_retry_fields_round_trip_and_demand_jobs_v2() {
        let cfg = JobConfig {
            retry_on: vec![RetryOn::Error, RetryOn::Timeout],
            backoff: Some(BackoffSpec {
                base_secs: 5,
                max_secs: 600,
            }),
            ..Default::default()
        };
        let wire = serde_json::json!({
            "timeout_secs": 600,
            "max_attempts": 3,
            "retry_on": ["error", "timeout"],
            "backoff": {"base_secs": 5, "max_secs": 600}
        });
        assert_eq!(serde_json::to_value(&cfg).unwrap(), wire);
        assert_eq!(serde_json::from_value::<JobConfig>(wire).unwrap(), cfg);
        assert_eq!(
            serde_json::from_value::<BackoffSpec>(serde_json::json!({})).unwrap(),
            BackoffSpec::default()
        );
        let route = |job| CustomRouteDefinition {
            job: Some(job),
            ..Default::default()
        };
        assert_eq!(route(JobConfig::default()).derived_features().count(), 0);
        assert!(!route(cfg.clone()).uses_request_v2());
        assert_eq!(
            route(cfg).derived_features().collect::<Vec<_>>(),
            [features::JOBS_V2]
        );
    }

    #[test]
    fn custom_route_serde_round_trip_with_job() {
        let route = CustomRouteDefinition {
            method: "POST".into(),
            path: "/run".into(),
            handler: "do_thing".into(),
            job: Some(JobConfig {
                timeout_secs: 900,
                max_attempts: 2,
                ..Default::default()
            }),
            posture: None,
            forward_auth: false,
            ..Default::default()
        };
        let json = serde_json::to_string(&route).unwrap();
        let round: CustomRouteDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(round.method, "POST");
        let job = round.job.unwrap();
        assert_eq!(job.timeout_secs, 900);
        assert_eq!(job.max_attempts, 2);
    }

    #[test]
    fn forward_auth_defaults_off_and_is_only_serialized_when_set() {
        let stored: CustomRouteDefinition =
            serde_json::from_str(r#"{"method":"GET","path":"/x","handler":"x"}"#).unwrap();
        assert!(!stored.forward_auth);
        let json = serde_json::to_string(&stored).unwrap();
        assert!(!json.contains("forward_auth"), "{json}");
        let forwarding = CustomRouteDefinition {
            forward_auth: true,
            ..stored
        };
        let json = serde_json::to_value(&forwarding).unwrap();
        assert_eq!(json["forward_auth"], true);
    }

    #[test]
    fn custom_route_without_job_serializes_without_field() {
        let route = CustomRouteDefinition {
            method: "GET".into(),
            path: "/x".into(),
            handler: "x".into(),
            job: None,
            posture: None,
            forward_auth: false,
            ..Default::default()
        };
        let json = serde_json::to_value(&route).unwrap();
        assert!(json.get("job").is_none(), "absent job should be omitted");
        assert!(
            json.get("posture").is_none(),
            "absent posture should be omitted"
        );
    }

    #[test]
    fn custom_route_legacy_json_without_job_deserializes() {
        let json = r#"{"method":"GET","path":"/x","handler":"x"}"#;
        let route: CustomRouteDefinition = serde_json::from_str(json).unwrap();
        assert!(route.job.is_none());
        assert!(route.posture.is_none());
    }

    #[test]
    fn sse_subjects_substitute_tenant_and_path_parameters() {
        let sse = SseRouteDef {
            subject: "dw.{tenant}.receipts.pdf.progress.{book_id}".into(),
            reducer: Some("reduce".into()),
            anonymous: false,
        };
        assert_eq!(
            sse.placeholders().collect::<Vec<_>>(),
            ["tenant", "book_id"]
        );
        let params = |value: &str| vec![("book_id".to_string(), value.to_string())];
        assert_eq!(
            sse.resolve("acme", &params("L-7")),
            Ok("dw.acme.receipts.pdf.progress.L-7".into())
        );
        for bad in ["a.b", "*", ">", "a b", ""] {
            assert_eq!(
                sse.resolve("acme", &params(bad)),
                Err(SseSubjectError::InvalidToken("book_id".into())),
                "{bad}"
            );
        }
        assert_eq!(
            sse.resolve("acme", &[]),
            Err(SseSubjectError::Missing("book_id".into()))
        );
        let wildcard = SseRouteDef {
            subject: "dw.{tenant}.jobs.>".into(),
            reducer: None,
            anonymous: false,
        };
        assert_eq!(wildcard.resolve("acme", &[]), Ok("dw.acme.jobs.>".into()));
    }

    #[test]
    fn sse_subjects_name_unreserved_events_and_bind_their_placeholders() {
        let sse = |subject: &str| SseRouteDef {
            subject: subject.into(),
            reducer: None,
            anonymous: false,
        };
        let params = ["id".to_string()];
        for ok in [
            "dw.{tenant}.jobs.>",
            "dw.{tenant}.q.{id}",
            "dw.{tenant}.q.*",
        ] {
            assert_eq!(sse(ok).subject_violation(Some(&params)), None, "{ok}");
        }
        for (subject, params, needle) in [
            (
                "dw.other.q",
                Some(&params[..]),
                "must be dot-separated tokens",
            ),
            (
                "dw.{tenant}.",
                Some(&params[..]),
                "must be dot-separated tokens",
            ),
            (
                "dw.{tenant}.q.{}",
                Some(&params[..]),
                "must be dot-separated tokens",
            ),
            ("dw.{tenant}.>", Some(&params[..]), "with a literal token"),
            (
                "dw.{tenant}.{id}.x",
                Some(&params[..]),
                "with a literal token",
            ),
            (
                "dw.{tenant}.logs",
                Some(&params[..]),
                "the platform reserves",
            ),
            (
                "dw.{tenant}.q.{other}",
                Some(&params[..]),
                "not a path parameter",
            ),
            ("dw.{tenant}.q.{id}", None, "needs routing: strict"),
        ] {
            let violation = sse(subject).subject_violation(params).unwrap_or_default();
            assert!(violation.contains(needle), "{subject}: {violation}");
        }
        assert!(sse("dw.{tenant}.q").anonymous_violation(true).is_some());
        assert!(sse("dw.{tenant}.q").anonymous_violation(false).is_none());
        let opted_in = SseRouteDef {
            anonymous: true,
            ..sse("dw.{tenant}.q")
        };
        assert!(opted_in.anonymous_violation(true).is_none());
        assert_eq!(
            serde_json::to_value(&opted_in).unwrap(),
            serde_json::json!({"subject": "dw.{tenant}.q", "anonymous": true})
        );
    }

    #[test]
    fn internal_routes_demand_their_feature_and_admit_listed_callers() {
        let route = CustomRouteDefinition {
            posture: Some(RoutePosture::Internal {
                callers: vec!["pricing".into()],
            }),
            ..Default::default()
        };
        assert_eq!(
            route.derived_features().collect::<Vec<_>>(),
            [features::ROUTE_POSTURE, features::INTERNAL_POSTURE]
        );
        assert!(route.uses_request_v2());
        assert_eq!(
            serde_json::to_value(route.posture).unwrap(),
            serde_json::json!({"kind": "internal", "callers": ["pricing"]})
        );
        let listed = ["pricing".to_string()];
        assert!(RoutePosture::admits_caller(&listed, "pricing"));
        assert!(!RoutePosture::admits_caller(&listed, "assets"));
        assert!(RoutePosture::admits_caller(&["*".to_string()], "assets"));
        assert!(!RoutePosture::admits_caller(&[], "assets"));
    }

    #[test]
    fn api_key_routes_demand_their_feature() {
        let route = CustomRouteDefinition {
            posture: Some(RoutePosture::ApiKey {
                scope: "books:ingest".into(),
            }),
            ..Default::default()
        };
        assert_eq!(
            route.derived_features().collect::<Vec<_>>(),
            [features::ROUTE_POSTURE, features::API_KEY_POSTURE]
        );
        assert_eq!(
            serde_json::to_value(route.posture).unwrap(),
            serde_json::json!({"kind": "api_key", "scope": "books:ingest"})
        );
    }

    #[test]
    fn scoped_routes_demand_their_feature() {
        let route = CustomRouteDefinition {
            posture: Some(RoutePosture::Scoped {
                area: "assets".into(),
                operation: "view".into(),
                owner_field: None,
            }),
            ..Default::default()
        };
        assert_eq!(
            route.derived_features().collect::<Vec<_>>(),
            [features::ROUTE_POSTURE, features::SCOPED_POSTURE]
        );
        assert_eq!(
            serde_json::to_value(route.posture).unwrap(),
            serde_json::json!({"kind": "scoped", "area": "assets", "operation": "view"})
        );
    }

    #[test]
    fn route_posture_serde_tagged_round_trip() {
        let cases = [
            RoutePosture::Public {
                reason: "stripe webhook".into(),
            },
            RoutePosture::Authenticated,
            RoutePosture::Permission {
                area: "items".into(),
                operation: "edit".into(),
            },
            RoutePosture::Owner {
                field: "customer_id".into(),
            },
            RoutePosture::Internal {
                callers: vec!["pricing-service".into()],
            },
            RoutePosture::ApiKey {
                scope: "books:ingest".into(),
            },
            RoutePosture::Scoped {
                area: "assets".into(),
                operation: "view".into(),
                owner_field: Some("customer_id".into()),
            },
            RoutePosture::Scoped {
                area: "assets".into(),
                operation: "view".into(),
                owner_field: None,
            },
        ];
        for p in cases {
            let json = serde_json::to_string(&p).unwrap();
            let round: RoutePosture = serde_json::from_str(&json).unwrap();
            assert_eq!(round, p);
        }
        let public = serde_json::to_value(RoutePosture::Authenticated).unwrap();
        assert_eq!(public["kind"], "authenticated");
    }
}
