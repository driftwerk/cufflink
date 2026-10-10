//! Extensions: the names, the NATS subjects and the messages of an
//! `ext_call`, shared by the platform, the worker, the extension runner,
//! the native services and the guest SDK.

use crate::host::ext::{CallErrorEnvelope, CallErrorKind};
use crate::slug::is_valid_service_name;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
#[cfg(feature = "protocol")]
use uuid::Uuid;

/// The longest extension name, in bytes.
pub const NAME_MAX: usize = 128;

/// The subject every call subject starts with.
pub const SUBJECT_PREFIX: &str = "cufflink.ext";

/// The queue group every provider of an extension subscribes in, so each
/// call reaches one of them.
pub const QUEUE_GROUP: &str = "cufflink-ext";

/// The version of `ExtRequest` and [`Ready`] this crate speaks.
pub const PROTOCOL_VERSION: u32 = 1;

/// A caller that stands for every service of the tenant.
pub const ANY_CALLER: &str = "*";

/// The variable naming the Unix socket a native service serves on.
pub const SOCKET_VAR: &str = "CUFFLINK_EXT_SOCKET";

/// The variable holding the token the extension runner sends a native
/// service as `Authorization: Bearer <token>` with every request; each
/// start of a child has its own.
pub const TOKEN_VAR: &str = "CUFFLINK_EXT_TOKEN";

/// Whether `name` is an extension name: 1 to [`NAME_MAX`] bytes of
/// dot-separated segments of `[a-z0-9-]+` (`google.token`, `pdf.render-v2`).
pub fn is_valid_extension_name(name: &str) -> bool {
    name.len() <= NAME_MAX
        && name.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

/// Why `name` is not an extension name; `None` when it is one.
pub fn name_violation(name: &str) -> Option<String> {
    (!is_valid_extension_name(name)).then(|| {
        format!(
            "'{name}' is not an extension name (segments of a-z, 0-9 and '-' joined by '.', at \
             most {NAME_MAX} bytes)"
        )
    })
}

/// The callers of an extension as they are stored: each a service name or
/// [`ANY_CALLER`], at least one, each once, sorted.
pub fn callers(requested: Vec<String>) -> Result<Vec<String>, String> {
    match requested
        .iter()
        .find(|caller| caller.as_str() != ANY_CALLER && !is_valid_service_name(caller))
    {
        Some(invalid) => Err(format!(
            "callers: '{invalid}' is neither a service name nor '{ANY_CALLER}'"
        )),
        None if requested.is_empty() => Err("callers: at least one caller is required".into()),
        None => Ok(requested
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()),
    }
}

/// The subject the calls of the tenant `tenant` to the extension `name`
/// travel on.
pub fn call_subject(tenant: &str, name: &str) -> String {
    format!("{SUBJECT_PREFIX}.{tenant}.{name}")
}

/// The subject a deploy or rollback of the tenant's native service
/// `service` is announced on, for the extension runner.
pub fn deploy_subject(tenant: &str, service: &str) -> String {
    format!("deploy.ext.{tenant}.{service}")
}

/// The subject the removal of the tenant's native service `service` is
/// announced on.
pub fn undeploy_subject(tenant: &str, service: &str) -> String {
    format!("undeploy.ext.{tenant}.{service}")
}

/// A call as its provider receives it.
#[cfg(feature = "protocol")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtRequest {
    /// [`PROTOCOL_VERSION`] of the sender.
    pub v: u32,
    pub call_id: Uuid,
    pub extension: String,
    pub context: CallContext,
    pub payload: Value,
}

#[cfg(feature = "protocol")]
impl ExtRequest {
    /// Whether the call is `tenant`'s and came on `subject`, its
    /// extension's: a call relayed on another subject is refused.
    pub fn matches(&self, tenant: &str, subject: &str) -> bool {
        self.context.tenant == tenant && subject == call_subject(tenant, &self.extension)
    }
}

/// Who calls, and until when the caller waits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallContext {
    pub tenant: String,
    pub service: CallerService,
    /// The verified claims of the caller's principal, as the calling
    /// handler sees them; never a token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<Value>,
    /// Milliseconds since the Unix epoch after which nobody waits for the
    /// reply.
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallerService {
    pub name: String,
    pub version: i32,
}

/// A provider's answer: `{"ok": <value>}` or
/// `{"error": {"code": …, "message": …}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtReply {
    Ok(Value),
    Error(ExtFailure),
}

impl ExtReply {
    /// The reply that fails a call with `code`.
    pub fn failure(code: &str, message: impl Into<String>) -> Self {
        Self::Error(ExtFailure {
            code: code.into(),
            message: message.into(),
        })
    }
}

/// Why an extension refused or failed a call, in its own words.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtFailure {
    pub code: String,
    pub message: String,
}

/// The codes a runner or a provider answers for the transport rather than
/// for the extension: each is the label of the kind the caller sees.
pub const RESERVED_CODES: [CallErrorKind; 3] = [
    CallErrorKind::Unavailable,
    CallErrorKind::Timeout,
    CallErrorKind::TooLarge,
];

impl ExtFailure {
    /// What the caller sees of the failure: the kind a reserved code
    /// names, and [`CallErrorKind::Failed`] for every other code.
    pub fn kind(&self) -> CallErrorKind {
        RESERVED_CODES
            .into_iter()
            .find(|kind| kind.as_str() == self.code)
            .unwrap_or(CallErrorKind::Failed)
    }
}

impl From<ExtFailure> for CallErrorEnvelope {
    fn from(failure: ExtFailure) -> Self {
        let kind = failure.kind();
        Self {
            kind,
            code: (kind == CallErrorKind::Failed).then_some(failure.code),
            message: failure.message,
        }
    }
}

/// What a native service answers once it serves its extensions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ready {
    pub protocol: u32,
    pub extensions: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::tests::{refused, round_trip};
    use serde_json::json;

    #[test]
    fn extension_names() {
        let longest = ["a".repeat(63), "b".repeat(64)].join(".");
        assert_eq!(longest.len(), NAME_MAX);
        for valid in [
            "google.token",
            "pdf.render-v2",
            "a",
            "0",
            "-",
            "a.b.c.d",
            "x-1.y-2",
            &"a".repeat(NAME_MAX),
            &longest,
        ] {
            assert!(is_valid_extension_name(valid), "{valid}");
        }
        for invalid in [
            "",
            ".",
            ".google",
            "google.",
            "google..token",
            "Google.token",
            "google.Token",
            "google.*",
            "google.>",
            "*",
            ">",
            "google token",
            " google",
            "google_token",
            "google/token",
            "göögle",
            "google.token\n",
            &"a".repeat(NAME_MAX + 1),
            &format!("{longest}a"),
        ] {
            assert!(!is_valid_extension_name(invalid), "{invalid:?}");
        }
    }

    #[test]
    fn callers_are_services_or_any_each_once() {
        let names = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert_eq!(
            callers(names(&["reports", "billing", "reports"])),
            Ok(names(&["billing", "reports"]))
        );
        assert_eq!(callers(names(&["*", "*"])), Ok(names(&["*"])));
        assert_eq!(
            callers(vec![]),
            Err("callers: at least one caller is required".into())
        );
        for invalid in ["", "a b", "a.b", "**", "reports/*"] {
            assert_eq!(
                callers(names(&["reports", invalid])),
                Err(format!(
                    "callers: '{invalid}' is neither a service name nor '*'"
                )),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn a_call_travels_on_the_subject_of_its_tenant_and_extension() {
        assert_eq!(
            call_subject("harbor", "google.token"),
            "cufflink.ext.harbor.google.token"
        );
        assert_eq!(
            deploy_subject("harbor", "ext-google"),
            "deploy.ext.harbor.ext-google"
        );
        assert_eq!(
            undeploy_subject("harbor", "ext-google"),
            "undeploy.ext.harbor.ext-google"
        );
    }

    fn context() -> CallContext {
        CallContext {
            tenant: "harbor".into(),
            service: CallerService {
                name: "reports-service".into(),
                version: 7,
            },
            auth: Some(json!({"sub": "kc-1", "realm_access": {"roles": ["staff"]}})),
            deadline_ms: 1_790_589_600_000,
        }
    }

    #[cfg(feature = "protocol")]
    #[test]
    fn a_call_matches_only_its_tenant_and_the_subject_of_its_extension() {
        let request = ExtRequest {
            v: PROTOCOL_VERSION,
            call_id: Uuid::nil(),
            extension: "google.token".into(),
            context: context(),
            payload: Value::Null,
        };
        assert!(request.matches("harbor", "cufflink.ext.harbor.google.token"));
        for (tenant, subject) in [
            ("harbor", "cufflink.ext.harbor.google.other"),
            ("harbor", "cufflink.ext.globex.google.token"),
            ("globex", "cufflink.ext.globex.google.token"),
            ("globex", "cufflink.ext.harbor.google.token"),
        ] {
            assert!(!request.matches(tenant, subject), "{tenant} {subject}");
        }
    }

    #[cfg(feature = "protocol")]
    #[test]
    fn a_request_round_trips() {
        round_trip(
            &ExtRequest {
                v: PROTOCOL_VERSION,
                call_id: Uuid::from_u128(0x0123_4567_89ab_4def_8123_4567_89ab_cdef),
                extension: "google.token".into(),
                context: context(),
                payload: json!({"scope": "analytics.readonly"}),
            },
            json!({
                "v": 1,
                "call_id": "01234567-89ab-4def-8123-456789abcdef",
                "extension": "google.token",
                "context": {
                    "tenant": "harbor",
                    "service": {"name": "reports-service", "version": 7},
                    "auth": {"sub": "kc-1", "realm_access": {"roles": ["staff"]}},
                    "deadline_ms": 1_790_589_600_000u64
                },
                "payload": {"scope": "analytics.readonly"}
            }),
        );
        refused::<ExtRequest>(json!({
            "v": 1,
            "call_id": "not a uuid",
            "extension": "a",
            "context": {"tenant": "t", "service": {"name": "s", "version": 1}, "deadline_ms": 0},
            "payload": null
        }));
    }

    #[test]
    fn a_context_without_auth_omits_it() {
        round_trip(
            &CallContext {
                auth: None,
                ..context()
            },
            json!({
                "tenant": "harbor",
                "service": {"name": "reports-service", "version": 7},
                "deadline_ms": 1_790_589_600_000u64
            }),
        );
    }

    #[test]
    fn a_reply_is_keyed_by_its_outcome() {
        round_trip(
            &ExtReply::Ok(json!({"token": "ya29"})),
            json!({"ok": {"token": "ya29"}}),
        );
        round_trip(&ExtReply::Ok(Value::Null), json!({"ok": null}));
        round_trip(
            &ExtReply::Error(ExtFailure {
                code: "quota".into(),
                message: "daily quota exhausted".into(),
            }),
            json!({"error": {"code": "quota", "message": "daily quota exhausted"}}),
        );
    }

    #[test]
    fn a_reply_with_both_outcomes_or_neither_is_refused() {
        for wire in [
            json!({"ok": 1, "error": {"code": "c", "message": "m"}}),
            json!({"error": {"code": "c", "message": "m"}, "ok": 1}),
            json!({}),
            json!("ok"),
            json!({"error": {"code": "c"}}),
            json!({"done": 1}),
            json!(null),
        ] {
            refused::<ExtReply>(wire.clone());
            let parsed = serde_json::from_str::<ExtReply>(&wire.to_string());
            assert!(parsed.is_err(), "{wire} parsed as {parsed:?}");
        }
    }

    #[test]
    fn only_the_reserved_codes_name_a_kind_of_their_own() {
        let failure = |code: &str| ExtFailure {
            code: code.into(),
            message: "why".into(),
        };
        for (code, kind) in [
            ("unavailable", CallErrorKind::Unavailable),
            ("timeout", CallErrorKind::Timeout),
            ("too_large", CallErrorKind::TooLarge),
            ("failed", CallErrorKind::Failed),
            ("quota", CallErrorKind::Failed),
            ("not_found", CallErrorKind::Failed),
            ("invalid", CallErrorKind::Failed),
            ("denied", CallErrorKind::Failed),
            ("Timeout", CallErrorKind::Failed),
            ("", CallErrorKind::Failed),
        ] {
            assert_eq!(failure(code).kind(), kind, "{code:?}");
        }
        assert_eq!(
            CallErrorEnvelope::from(failure("too_large")),
            CallErrorEnvelope {
                kind: CallErrorKind::TooLarge,
                code: None,
                message: "why".into(),
            }
        );
        assert_eq!(
            CallErrorEnvelope::from(failure("quota")),
            CallErrorEnvelope {
                kind: CallErrorKind::Failed,
                code: Some("quota".into()),
                message: "why".into(),
            }
        );
        assert_eq!(
            ExtReply::failure("quota", "why"),
            ExtReply::Error(failure("quota"))
        );
    }

    #[test]
    fn a_name_that_is_not_an_extension_name_says_what_one_is() {
        assert_eq!(name_violation("google.token"), None);
        assert_eq!(
            name_violation("Google"),
            Some(
                "'Google' is not an extension name (segments of a-z, 0-9 and '-' joined by '.', \
                 at most 128 bytes)"
                    .into()
            )
        );
    }

    #[test]
    fn readiness_round_trips() {
        round_trip(
            &Ready {
                protocol: PROTOCOL_VERSION,
                extensions: vec!["google.token".into(), "pdf.render".into()],
            },
            json!({"protocol": 1, "extensions": ["google.token", "pdf.render"]}),
        );
    }
}
