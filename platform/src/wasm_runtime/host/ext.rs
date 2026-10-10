use super::util::exchange;
use super::{block_on_within, clamp_to_budget, wrap, HostIoError, HostState, FETCH_TIMEOUT_FLOOR};
use crate::extensions::Registration;
use crate::nats_manager::headers::wire_len;
use async_nats::client::{Request, RequestError, RequestErrorKind};
use async_nats::HeaderMap;
use cufflink_types::ext::{
    call_subject, is_valid_extension_name, CallContext, CallerService, ExtReply, ExtRequest,
    PROTOCOL_VERSION,
};
use cufflink_types::host::ext::{CallErrorEnvelope, CallErrorKind, CallRequest, CallResponse};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::time::{Duration, Instant};
use uuid::Uuid;
use wasmtime::{Caller, Linker};

const UNNAMED: &str = "-";
const NO_RESPONDERS_FIRST_WAIT: Duration = Duration::from_millis(25);
const NO_RESPONDERS_MAX_WAIT: Duration = Duration::from_millis(250);
const NO_RESPONDERS_WINDOW: Duration = Duration::from_secs(1);

fn refused(kind: CallErrorKind, message: impl Into<String>) -> CallErrorEnvelope {
    CallErrorEnvelope {
        kind,
        code: None,
        message: message.into(),
    }
}

struct Calling<'a> {
    tenant: &'a str,
    service: &'a str,
    version: i32,
    declared: &'a [String],
    registrations: Option<&'a BTreeMap<String, Registration>>,
    auth: Option<&'a Value>,
    remaining: Duration,
    now_ms: u64,
}

struct Limits {
    max_bytes: usize,
    default_timeout: Duration,
}

#[derive(Debug, PartialEq)]
struct Call {
    subject: String,
    body: Vec<u8>,
    timeout: Duration,
}

fn decide(
    calling: &Calling<'_>,
    limits: &Limits,
    call_id: Uuid,
    request: CallRequest,
) -> Result<Call, CallErrorEnvelope> {
    let name = request.extension;
    if !is_valid_extension_name(&name) || !calling.declared.contains(&name) {
        return Err(refused(
            CallErrorKind::NotDeclared,
            format!("the service does not declare the extension '{name}'"),
        ));
    }
    let registration = calling
        .registrations
        .and_then(|registrations| registrations.get(&name))
        .ok_or_else(|| {
            refused(
                CallErrorKind::NotFound,
                format!("no extension '{name}' is registered for the tenant"),
            )
        })?;
    if !registration.admits(calling.service) {
        return Err(refused(
            CallErrorKind::Denied,
            format!(
                "the registration of '{name}' does not list '{}' among its callers",
                calling.service
            ),
        ));
    }
    let timeout = clamp_to_budget(
        request
            .timeout_ms
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis),
        limits.default_timeout,
        calling.remaining,
    );
    let subject = call_subject(calling.tenant, &name);
    let body = serde_json::to_vec(&ExtRequest {
        v: PROTOCOL_VERSION,
        call_id,
        extension: name,
        context: CallContext {
            tenant: calling.tenant.to_string(),
            service: CallerService {
                name: calling.service.to_string(),
                version: calling.version,
            },
            auth: calling.auth.cloned(),
            deadline_ms: calling
                .now_ms
                .saturating_add(u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)),
        },
        payload: request.payload,
    })
    .map_err(|e| refused(CallErrorKind::Invalid, format!("encode the call: {e}")))?;
    if body.len() > limits.max_bytes {
        return Err(refused(
            CallErrorKind::TooLarge,
            format!(
                "the call is {} bytes; at most {} are carried",
                body.len(),
                limits.max_bytes
            ),
        ));
    }
    if calling.remaining < FETCH_TIMEOUT_FLOOR {
        return Err(refused(
            CallErrorKind::Timeout,
            "the handler's budget is spent",
        ));
    }
    Ok(Call {
        subject,
        body,
        timeout,
    })
}

fn body_limit(configured: usize, server_max: Option<usize>, headers: usize) -> usize {
    server_max.map_or(configured, |max| {
        configured.min(max.saturating_sub(headers))
    })
}

fn replied(payload: &[u8], max_bytes: usize) -> Result<Value, CallErrorEnvelope> {
    if payload.len() > max_bytes {
        return Err(refused(
            CallErrorKind::TooLarge,
            format!(
                "the reply is {} bytes; at most {max_bytes} are carried",
                payload.len()
            ),
        ));
    }
    match serde_json::from_slice::<ExtReply>(payload) {
        Ok(ExtReply::Ok(value)) => Ok(value),
        Ok(ExtReply::Error(failure)) => Err(failure.into()),
        Err(e) => Err(refused(
            CallErrorKind::Unavailable,
            format!("the extension answered what is not a reply: {e}"),
        )),
    }
}

fn unanswered(ended: HostIoError<RequestError>) -> CallErrorEnvelope {
    match ended {
        HostIoError::TimedOut(_) | HostIoError::Cancelled => refused(
            CallErrorKind::Timeout,
            "the extension did not answer in time",
        ),
        HostIoError::Failed(e) => match e.kind() {
            RequestErrorKind::TimedOut => refused(
                CallErrorKind::Timeout,
                "the extension did not answer in time",
            ),
            RequestErrorKind::NoResponders => {
                refused(CallErrorKind::Unavailable, "nothing serves the extension")
            }
            RequestErrorKind::MaxPayloadExceeded => refused(
                CallErrorKind::TooLarge,
                "the call is more than the server carries",
            ),
            RequestErrorKind::InvalidSubject | RequestErrorKind::Other => {
                refused(CallErrorKind::Unavailable, e.to_string())
            }
        },
    }
}

async fn requested<T, Answer>(
    timeout: Duration,
    attempt: impl Fn(Duration) -> Answer,
) -> Result<T, RequestError>
where
    Answer: Future<Output = Result<T, RequestError>>,
{
    let started = tokio::time::Instant::now();
    let deadline = started + timeout;
    let retry_until = started + NO_RESPONDERS_WINDOW;
    let mut wait = NO_RESPONDERS_FIRST_WAIT;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match attempt(left).await {
            Err(e)
                if e.kind() == RequestErrorKind::NoResponders
                    && tokio::time::Instant::now() + wait <= retry_until
                    && deadline.saturating_duration_since(tokio::time::Instant::now())
                        > wait * 2 =>
            {
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(NO_RESPONDERS_MAX_WAIT);
            }
            answered => return answered,
        }
    }
}

fn send(state: &HostState, call: Call, headers: HeaderMap) -> Result<Value, CallErrorEnvelope> {
    let nats = &state.app_state.nats;
    let (subject, body) = (call.subject, bytes::Bytes::from(call.body));
    block_on_within(
        call.timeout,
        &state.cancel,
        requested(call.timeout, |left| {
            nats.send_request(
                subject.clone(),
                Request::new()
                    .headers(headers.clone())
                    .payload(body.clone())
                    .timeout(Some(left)),
            )
        }),
    )
    .map_err(unanswered)
    .and_then(|reply| {
        replied(
            &reply.payload,
            state.app_state.config.extensions.call_max_bytes,
        )
    })
}

fn call(
    state: &HostState,
    extension: &str,
    request: CallRequest,
) -> Result<Value, CallErrorEnvelope> {
    let _span = tracing::info_span!("ext.call", otel.kind = "client", extension).entered();
    let headers = crate::telemetry::traced_nats_headers();
    let app = &state.app_state;
    let entry = app.tenants.get(state.identity.tenant_id);
    let limits = Limits {
        max_bytes: body_limit(
            app.config.extensions.call_max_bytes,
            crate::outbox::publish::max_payload(&app.nats),
            wire_len(&headers),
        ),
        default_timeout: app.config.extensions.call_timeout,
    };
    let calling = Calling {
        tenant: &state.identity.tenant_slug,
        service: &state.identity.service_name,
        version: state.service_version,
        declared: &state.extensions,
        registrations: entry.as_deref().map(|entry| &entry.extensions),
        auth: state.auth.as_ref().map(|auth| &auth.claims),
        remaining: state.remaining_budget(),
        now_ms: u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or_default(),
    };
    let call = decide(&calling, &limits, Uuid::new_v4(), request)?;
    send(state, call, headers)
}

fn label<'a>(extension: &'a str, outcome: &Result<Value, CallErrorEnvelope>) -> &'a str {
    match outcome.as_ref().map_err(|refusal| refusal.kind) {
        Err(CallErrorKind::Invalid | CallErrorKind::NotDeclared) => UNNAMED,
        Err(
            CallErrorKind::NotFound
            | CallErrorKind::Denied
            | CallErrorKind::Unavailable
            | CallErrorKind::Timeout
            | CallErrorKind::TooLarge
            | CallErrorKind::Failed,
        )
        | Ok(_) => extension,
    }
}

fn recorded(
    caller: &str,
    extension: &str,
    started: Instant,
    outcome: Result<Value, CallErrorEnvelope>,
) -> CallResponse {
    let extension = label(extension, &outcome).to_string();
    let result = outcome
        .as_ref()
        .map_or_else(|refusal| refusal.kind.as_str(), |_| "ok");
    metrics::counter!(
        "cufflink_ext_calls_total",
        "caller" => caller.to_string(),
        "extension" => extension.clone(),
        "result" => result,
    )
    .increment(1);
    metrics::histogram!(
        "cufflink_ext_call_duration_seconds",
        "extension" => extension,
    )
    .record(started.elapsed().as_secs_f64());
    outcome.map_or_else(CallResponse::Error, CallResponse::Ok)
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "ext_call",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            let started = Instant::now();
            let service = caller.data().identity.service_name.clone();
            exchange(
                &mut caller,
                "ext_call",
                (ptr, len),
                |reason| {
                    recorded(
                        &service,
                        UNNAMED,
                        started,
                        Err(refused(CallErrorKind::Invalid, reason)),
                    )
                },
                |state, request: CallRequest| {
                    let extension = request.extension.clone();
                    let outcome = call(state, &extension, request);
                    recorded(&service, &extension, started, outcome)
                },
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extensions::Backend;
    use cufflink_types::ext::ExtFailure;
    use serde_json::json;

    const MAX: usize = 4096;

    fn registered(callers: &[&str]) -> Registration {
        Registration {
            backend: Backend::Nats,
            callers: callers.iter().map(|c| c.to_string()).collect(),
        }
    }

    fn registrations() -> BTreeMap<String, Registration> {
        BTreeMap::from([
            ("google.token".to_string(), registered(&["reports"])),
            ("pdf.render".to_string(), registered(&["*"])),
            ("maps.geocode".to_string(), registered(&["billing"])),
        ])
    }

    fn declared() -> Vec<String> {
        [
            "google.token",
            "pdf.render",
            "maps.geocode",
            "unregistered.x",
        ]
        .map(String::from)
        .to_vec()
    }

    fn request(extension: &str, payload: Value, timeout_ms: Option<u64>) -> CallRequest {
        CallRequest {
            extension: extension.into(),
            payload,
            timeout_ms,
        }
    }

    const CALL_ID: Uuid = Uuid::from_u128(0x0123_4567_89ab_4def_8123_4567_89ab_cdef);

    fn decided(
        registrations: Option<&BTreeMap<String, Registration>>,
        remaining: Duration,
        request: CallRequest,
    ) -> Result<Call, CallErrorEnvelope> {
        let declared = declared();
        let auth = json!({"sub": "user-1", "realm_access": {"roles": ["staff"]}});
        decide(
            &Calling {
                tenant: "acme",
                service: "reports",
                version: 4,
                declared: &declared,
                registrations,
                auth: Some(&auth),
                remaining,
                now_ms: 1_000_000,
            },
            &Limits {
                max_bytes: MAX,
                default_timeout: Duration::from_secs(30),
            },
            CALL_ID,
            request,
        )
    }

    fn kind(outcome: &Result<Call, CallErrorEnvelope>) -> Option<CallErrorKind> {
        outcome.as_ref().err().map(|refusal| refusal.kind)
    }

    #[test]
    fn a_call_is_sent_as_its_extension_expects_it() {
        let registrations = registrations();
        let call = decided(
            Some(&registrations),
            Duration::from_secs(60),
            request("google.token", json!({"scope": "analytics"}), None),
        )
        .expect("admitted");
        assert_eq!(call.subject, "cufflink.ext.acme.google.token");
        assert_eq!(call.timeout, Duration::from_secs(30));
        let sent: ExtRequest = serde_json::from_slice(&call.body).expect("an ExtRequest");
        assert_eq!(
            sent,
            ExtRequest {
                v: PROTOCOL_VERSION,
                call_id: CALL_ID,
                extension: "google.token".into(),
                context: CallContext {
                    tenant: "acme".into(),
                    service: CallerService {
                        name: "reports".into(),
                        version: 4,
                    },
                    auth: Some(json!({"sub": "user-1", "realm_access": {"roles": ["staff"]}})),
                    deadline_ms: 1_030_000,
                },
                payload: json!({"scope": "analytics"}),
            }
        );
    }

    #[test]
    fn every_refusal_is_decided_before_anything_is_sent() {
        let registrations = registrations();
        let ample = Duration::from_secs(60);
        let big = json!("x".repeat(MAX));
        for (registrations, remaining, request, expected) in [
            (
                Some(&registrations),
                ample,
                request("Google.Token", json!(null), None),
                CallErrorKind::NotDeclared,
            ),
            (
                Some(&registrations),
                ample,
                request("", json!(null), None),
                CallErrorKind::NotDeclared,
            ),
            (
                Some(&registrations),
                ample,
                request("cufflink.ext.>", json!(null), None),
                CallErrorKind::NotDeclared,
            ),
            (
                Some(&registrations),
                ample,
                request("billing.charge", json!(null), None),
                CallErrorKind::NotDeclared,
            ),
            (
                Some(&registrations),
                ample,
                request("unregistered.x", json!(null), None),
                CallErrorKind::NotFound,
            ),
            (
                None,
                ample,
                request("google.token", json!(null), None),
                CallErrorKind::NotFound,
            ),
            (
                Some(&registrations),
                ample,
                request("maps.geocode", json!(null), None),
                CallErrorKind::Denied,
            ),
            (
                Some(&registrations),
                ample,
                request("pdf.render", big.clone(), None),
                CallErrorKind::TooLarge,
            ),
            (
                Some(&registrations),
                Duration::from_millis(100),
                request("pdf.render", big, None),
                CallErrorKind::TooLarge,
            ),
            (
                Some(&registrations),
                Duration::from_millis(499),
                request("pdf.render", json!(null), None),
                CallErrorKind::Timeout,
            ),
        ] {
            assert_eq!(
                kind(&decided(registrations, remaining, request.clone())),
                Some(expected),
                "{request:?}"
            );
        }
    }

    #[test]
    fn any_caller_is_every_service_of_the_tenant() {
        let registrations = registrations();
        let call = decided(
            Some(&registrations),
            Duration::from_secs(60),
            request("pdf.render", json!({"doc": 1}), None),
        )
        .expect("admitted");
        assert_eq!(call.subject, "cufflink.ext.acme.pdf.render");
    }

    #[test]
    fn a_call_waits_what_the_guest_asks_within_the_budget() {
        let registrations = registrations();
        let waits = |remaining: Duration, timeout_ms: Option<u64>| {
            decided(
                Some(&registrations),
                remaining,
                request("pdf.render", json!(null), timeout_ms),
            )
            .expect("admitted")
            .timeout
        };
        let secs = Duration::from_secs;
        assert_eq!(waits(secs(900), Some(120_000)), secs(120));
        assert_eq!(waits(secs(900), Some(0)), secs(30), "0 is the default");
        assert_eq!(waits(secs(10), None), secs(9));
        assert_eq!(waits(secs(10), Some(2_000)), secs(2));
        assert_eq!(
            waits(Duration::from_millis(600), None),
            FETCH_TIMEOUT_FLOOR,
            "what is left of the floor is waited"
        );
    }

    #[test]
    fn the_request_fits_the_platforms_limit_and_the_servers_with_its_headers() {
        assert_eq!(body_limit(1024, None, 100), 1024);
        assert_eq!(body_limit(1024, Some(4096), 100), 1024);
        assert_eq!(body_limit(4096, Some(1024), 100), 924);
        assert_eq!(body_limit(4096, Some(64), 100), 0);
    }

    #[test]
    fn a_reply_is_the_value_or_the_extensions_own_failure() {
        let wire = |reply: &ExtReply| serde_json::to_vec(reply).expect("encode");
        assert_eq!(
            replied(&wire(&ExtReply::Ok(json!({"token": "ya29"}))), MAX),
            Ok(json!({"token": "ya29"}))
        );
        assert_eq!(
            replied(
                &wire(&ExtReply::Error(ExtFailure {
                    code: "quota".into(),
                    message: "daily quota exhausted".into(),
                })),
                MAX
            ),
            Err(CallErrorEnvelope {
                kind: CallErrorKind::Failed,
                code: Some("quota".into()),
                message: "daily quota exhausted".into(),
            })
        );
        for (code, kind) in [
            ("unavailable", CallErrorKind::Unavailable),
            ("timeout", CallErrorKind::Timeout),
            ("too_large", CallErrorKind::TooLarge),
        ] {
            assert_eq!(
                replied(&wire(&ExtReply::failure(code, "the runner says why")), MAX),
                Err(refused(kind, "the runner says why")),
                "{code}"
            );
        }
        for code in ["not_found", "invalid", "panic", "failed"] {
            assert_eq!(
                replied(&wire(&ExtReply::failure(code, "m")), MAX)
                    .map_err(|refusal| (refusal.kind, refusal.code)),
                Err((CallErrorKind::Failed, Some(code.to_string()))),
                "{code}"
            );
        }
        for nonsense in [&b"{\"done\":1}"[..], b"not json", b""] {
            assert_eq!(
                replied(nonsense, MAX).map_err(|refusal| refusal.kind),
                Err(CallErrorKind::Unavailable)
            );
        }
        let large = wire(&ExtReply::Ok(json!("x".repeat(MAX))));
        assert_eq!(
            replied(&large, MAX).map_err(|refusal| refusal.kind),
            Err(CallErrorKind::TooLarge)
        );
    }

    async fn attempted(
        timeout: Duration,
        answers: Vec<Result<u32, RequestErrorKind>>,
    ) -> (Result<u32, RequestErrorKind>, Vec<Duration>, Duration) {
        let started = tokio::time::Instant::now();
        let (answers, given) = (
            std::sync::Mutex::new(answers.into_iter()),
            std::sync::Mutex::new(Vec::new()),
        );
        let outcome = requested(timeout, |left| {
            given.lock().expect("given").push(left);
            let answer = answers.lock().expect("answers").next().expect("an answer");
            async move { answer.map_err(RequestError::new) }
        })
        .await
        .map_err(|e| e.kind());
        (
            outcome,
            given.into_inner().expect("given"),
            started.elapsed(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_no_one_received_is_sent_again_within_its_time() {
        let ms = Duration::from_millis;
        let none = || Err(RequestErrorKind::NoResponders);
        assert_eq!(
            attempted(ms(1_000), vec![none(), none(), none(), Ok(7)]).await,
            (Ok(7), vec![ms(1_000), ms(975), ms(925), ms(825)], ms(175)),
            "25, 50 and 100 ms apart, each with what is left"
        );
        let (outcome, given, took) = attempted(ms(300), vec![none(); 8]).await;
        assert_eq!(
            (outcome, given.len(), took),
            (Err(RequestErrorKind::NoResponders), 4, ms(175)),
            "no attempt past the time the call has"
        );
        let answers = [vec![none(); 6], vec![Ok(1)]].concat();
        let (outcome, given, _) = attempted(ms(5_000), answers).await;
        assert_eq!(outcome, Ok(1));
        assert_eq!(
            given
                .windows(2)
                .map(|pair| pair[0] - pair[1])
                .collect::<Vec<_>>(),
            [ms(25), ms(50), ms(100), ms(200), ms(250), ms(250)],
            "the wait doubles up to 250 ms"
        );
        let (outcome, given, took) = attempted(ms(30_000), vec![none(); 12]).await;
        assert_eq!(
            (outcome, given.len(), took),
            (Err(RequestErrorKind::NoResponders), 7, ms(875)),
            "a call nothing serves is unavailable within a second, whatever its time"
        );
        for other in [
            RequestErrorKind::TimedOut,
            RequestErrorKind::Other,
            RequestErrorKind::MaxPayloadExceeded,
        ] {
            assert_eq!(
                attempted(ms(1_000), vec![Err(other), Ok(1)]).await,
                (Err(other), vec![ms(1_000)], Duration::ZERO),
                "{other:?} is not sent again"
            );
        }
    }

    #[test]
    fn a_request_that_brought_no_reply_is_unavailable_or_timed_out() {
        let failed = |kind| unanswered(HostIoError::Failed(RequestError::new(kind))).kind;
        assert_eq!(
            failed(RequestErrorKind::NoResponders),
            CallErrorKind::Unavailable
        );
        assert_eq!(failed(RequestErrorKind::TimedOut), CallErrorKind::Timeout);
        assert_eq!(
            failed(RequestErrorKind::MaxPayloadExceeded),
            CallErrorKind::TooLarge
        );
        assert_eq!(failed(RequestErrorKind::Other), CallErrorKind::Unavailable);
        for stopped in [
            HostIoError::TimedOut(Duration::from_secs(1)),
            HostIoError::Cancelled,
        ] {
            assert_eq!(unanswered(stopped).kind, CallErrorKind::Timeout);
        }
    }

    #[test]
    fn a_call_is_counted_under_its_name_only_once_it_is_declared() {
        let ended = |kind| Err(refused(kind, "refused"));
        assert_eq!(label("google.token", &Ok(json!(1))), "google.token");
        for (kind, counted) in [
            (CallErrorKind::Invalid, UNNAMED),
            (CallErrorKind::NotDeclared, UNNAMED),
            (CallErrorKind::NotFound, "google.token"),
            (CallErrorKind::Denied, "google.token"),
            (CallErrorKind::Unavailable, "google.token"),
            (CallErrorKind::Timeout, "google.token"),
            (CallErrorKind::TooLarge, "google.token"),
            (CallErrorKind::Failed, "google.token"),
        ] {
            assert_eq!(label("google.token", &ended(kind)), counted, "{kind:?}");
        }
        assert_eq!(
            label("cufflink.ext.>", &ended(CallErrorKind::NotDeclared)),
            UNNAMED
        );
    }
}
