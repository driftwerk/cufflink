use super::http::outbound_http_timeout;
use super::util::{ended, exchange};
use super::{
    block_on, block_on_within, clamp_to_budget, read_guest_bytes, read_guest_string, wrap,
    HostState,
};
use crate::nats_conn::PublishError;
use crate::nats_manager::headers;
use crate::nats_manager::stream_index::StreamRef;
use crate::nats_manager::streams::publish_acked;
use crate::nats_manager::subjects::{is_reserved_for_guest, subject_matches, PublishScope};
use async_nats::jetstream::publish::PublishAck;
use async_nats::HeaderMap;
use cufflink_types::host::nats::{
    PublishErrorKind, PublishMode, PublishRequest, PublishResponse, MSG_ID_HEADER,
};
use cufflink_types::nats_names::subject_violation;
use futures_util::StreamExt;
use std::collections::HashSet;
use std::future::Future;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use wasmtime::{Caller, Linker};

const TRACE_HEADERS: [&str; 2] = ["traceparent", "tracestate"];

#[derive(Clone)]
pub enum NatsPublishPolicy {
    Legacy,
    Enforce { allow: Vec<String> },
}

#[derive(Debug, PartialEq, Eq)]
enum PublishDecision {
    Allow,
    LegacyAllow,
    Deny,
    Invalid(String),
    Reserved,
    Claimed,
}

impl NatsPublishPolicy {
    pub fn from_manifest_publishes(publishes: Option<Vec<String>>) -> Self {
        match publishes {
            None => NatsPublishPolicy::Legacy,
            Some(allow) => NatsPublishPolicy::Enforce { allow },
        }
    }

    fn decide(&self, scope: &PublishScope, subject: &str) -> PublishDecision {
        if let Some(reason) = subject_violation(subject) {
            return PublishDecision::Invalid(reason);
        }
        if is_reserved_for_guest(subject, scope.namespace.tenant_slug()) {
            return PublishDecision::Reserved;
        }
        if scope.claimed_by_another(subject).is_some() {
            return PublishDecision::Claimed;
        }
        match self {
            NatsPublishPolicy::Legacy => PublishDecision::LegacyAllow,
            NatsPublishPolicy::Enforce { allow } => {
                if scope.namespace.contains(subject)
                    || allow.iter().any(|p| subject_matches(p, subject))
                {
                    PublishDecision::Allow
                } else {
                    PublishDecision::Deny
                }
            }
        }
    }
}

fn authorize_nats_send(state: &HostState, subject: &str) -> bool {
    match state
        .nats_publish_policy
        .decide(&state.nats_publish_scope, subject)
    {
        PublishDecision::Allow => true,
        PublishDecision::LegacyAllow => {
            metrics::counter!("cufflink_nats_publish_unscoped_total").increment(1);
            tracing::debug!(
                tenant = %state.identity.tenant_slug,
                subject = %subject,
                "Guest published without a declared publish policy — allowed in legacy mode."
            );
            true
        }
        PublishDecision::Deny => {
            metrics::counter!("cufflink_nats_publish_denied_total", "reason" => "policy")
                .increment(1);
            tracing::warn!(
                tenant = %state.identity.tenant_slug,
                subject = %subject,
                "Guest denied NATS publish/request to a subject outside its own tenant \
                 namespace and declared publish allowlist."
            );
            false
        }
        PublishDecision::Claimed => {
            metrics::counter!("cufflink_nats_publish_denied_total", "reason" => "claimed")
                .increment(1);
            tracing::warn!(
                tenant = %state.identity.tenant_slug,
                subject = %subject,
                "Guest denied NATS publish/request to a subject another tenant claims."
            );
            false
        }
        PublishDecision::Invalid(reason) => {
            metrics::counter!("cufflink_nats_publish_denied_total", "reason" => "invalid")
                .increment(1);
            tracing::warn!(
                tenant = %state.identity.tenant_slug,
                %reason,
                "Guest denied NATS publish/request to what is not a subject."
            );
            false
        }
        PublishDecision::Reserved => {
            metrics::counter!("cufflink_nats_publish_denied_total", "reason" => "reserved")
                .increment(1);
            tracing::warn!(
                tenant = %state.identity.tenant_slug,
                subject = %subject,
                "Guest denied NATS publish/request to a reserved subject."
            );
            false
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
        "nats_publish_v2",
        |mut caller: Caller<'_, HostState>, ptr: i32, len: i32| -> i32 {
            exchange(
                &mut caller,
                "nats_publish_v2",
                (ptr, len),
                |reason| {
                    recorded(
                        PublishMode::JetStream,
                        Err(refused(PublishErrorKind::Invalid, reason)),
                    )
                },
                |state, request: PublishRequest| recorded(request.mode, publish_v2(state, request)),
            )
        },
    )
}

fn refused(kind: PublishErrorKind, message: impl Into<String>) -> PublishResponse {
    PublishResponse::Error {
        kind,
        message: message.into(),
    }
}

fn recorded(
    mode: PublishMode,
    outcome: Result<PublishResponse, PublishResponse>,
) -> PublishResponse {
    let response = outcome.unwrap_or_else(|refused| refused);
    let result = match &response {
        PublishResponse::Ack {
            duplicate: true, ..
        } => "duplicate",
        PublishResponse::Ack { .. } => "ack",
        PublishResponse::Sent => "sent",
        PublishResponse::Error { kind, .. } => kind.as_str(),
    };
    metrics::counter!(
        "cufflink_nats_guest_publish_total",
        "mode" => match mode {
            PublishMode::JetStream => "jetstream",
            PublishMode::Core => "core",
        },
        "result" => result,
    )
    .increment(1);
    response
}

fn publish_headers(
    request: &PublishRequest,
    traced: HeaderMap,
) -> Result<HeaderMap, PublishResponse> {
    let invalid = |reason: String| refused(PublishErrorKind::Invalid, reason);
    if let Some(reason) = request.violation() {
        return Err(invalid(reason));
    }
    let carried = request
        .headers
        .iter()
        .filter(|(name, _)| {
            !TRACE_HEADERS
                .iter()
                .any(|trace| name.eq_ignore_ascii_case(trace))
        })
        .try_fold(traced, |carried, (name, value)| {
            headers::appended(carried, name, value)
        });
    match request.msg_id.as_deref().filter(|id| !id.is_empty()) {
        Some(msg_id) => {
            carried.and_then(|carried| headers::replaced(carried, MSG_ID_HEADER, msg_id))
        }
        None => carried,
    }
    .map_err(|e| invalid(e.to_string()))
}

fn within_max_payload(
    payload: usize,
    headers: &HeaderMap,
    max_payload: usize,
) -> Result<(), PublishResponse> {
    let carried = headers::wire_len(headers);
    match payload.checked_add(carried) {
        Some(size) if size <= max_payload => Ok(()),
        _ => Err(refused(
            PublishErrorKind::TooLarge,
            format!(
                "the payload is {payload} bytes and its headers {carried}; the server takes \
                 {max_payload} at most"
            ),
        )),
    }
}

fn publish_v2(
    state: &HostState,
    request: PublishRequest,
) -> Result<PublishResponse, PublishResponse> {
    if let Some(reason) = subject_violation(&request.subject) {
        return Err(refused(PublishErrorKind::Invalid, reason));
    }
    if !authorize_nats_send(state, &request.subject) {
        return Err(refused(
            PublishErrorKind::Denied,
            format!("publishing on '{}' is not allowed", request.subject),
        ));
    }
    let _span = tracing::info_span!(
        "nats.publish",
        otel.kind = "producer",
        subject = %request.subject
    )
    .entered();
    let headers = publish_headers(&request, crate::telemetry::traced_nats_headers())?;
    within_max_payload(
        request.payload.len(),
        &headers,
        state.app_state.nats.server_info().max_payload,
    )?;
    let payload = bytes::Bytes::from(request.payload);
    let timeout = clamp_to_budget(
        request
            .timeout_ms
            .filter(|ms| *ms > 0)
            .map(|ms| Duration::from_millis(u64::from(ms))),
        crate::nats_conn::publish_timeout(),
        state.remaining_budget(),
    );
    let app = &state.app_state;
    match request.mode {
        PublishMode::Core => within(timeout, &state.cancel, async {
            crate::nats_conn::publish_bounded(&app.nats, request.subject, Some(headers), payload)
                .await
                .map(|()| PublishResponse::Sent)
                .map_err(|e| unavailable(e.to_string()))
        }),
        PublishMode::JetStream => within(
            timeout,
            &state.cancel,
            acked_by_stream(
                &request.subject,
                app.stream_index
                    .capturing_or_refresh(&app.jetstream, &request.subject, timeout),
                publish_acked(
                    &app.jetstream,
                    request.subject.clone(),
                    headers,
                    payload,
                    timeout,
                ),
            ),
        ),
    }
}

fn within(
    timeout: Duration,
    cancel: &CancellationToken,
    publish: impl Future<Output = Result<PublishResponse, PublishResponse>>,
) -> Result<PublishResponse, PublishResponse> {
    block_on_within(timeout, cancel, publish)
        .map_err(|stopped| ended(stopped, |why| refused(PublishErrorKind::Timeout, why)))
}

async fn acked_by_stream(
    subject: &str,
    capture: impl Future<Output = Option<StreamRef>>,
    publish: impl Future<Output = Result<PublishAck, PublishError>>,
) -> Result<PublishResponse, PublishResponse> {
    let Some(stream) = capture.await else {
        return Err(refused(
            PublishErrorKind::NoStream,
            format!("no stream captures '{subject}'"),
        ));
    };
    match publish.await {
        Ok(ack) => Ok(PublishResponse::Ack {
            stream: ack.stream,
            sequence: ack.sequence,
            duplicate: ack.duplicate,
        }),
        Err(PublishError::Failed(e)) => Err(unavailable(format!(
            "stream '{}' did not take the message: {e}",
            stream.name
        ))),
        Err(PublishError::TimedOut(timeout)) => Err(refused(
            PublishErrorKind::Timeout,
            format!(
                "stream '{}' did not acknowledge within {timeout:?}",
                stream.name
            ),
        )),
    }
}

fn unavailable(message: String) -> PublishResponse {
    metrics::counter!("cufflink_nats_publish_failed_total", "kind" => "guest_v2", "reason" => "error")
        .increment(1);
    refused(PublishErrorKind::Unavailable, message)
}

pub(super) fn register(
    linker: &mut Linker<HostState>,
    names: &mut HashSet<&'static str>,
) -> eyre::Result<()> {
    wrap(
        linker,
        names,
        "nats_publish",
        |mut caller: Caller<'_, HostState>,
         subject_ptr: i32,
         subject_len: i32,
         payload_ptr: i32,
         payload_len: i32|
         -> i32 {
            let subject = host_try!(read_guest_string(
                &mut caller,
                "nats_publish",
                subject_ptr,
                subject_len
            ));
            let payload_buf = host_try!(read_guest_bytes(
                &mut caller,
                "nats_publish",
                payload_ptr,
                payload_len
            ));
            if !authorize_nats_send(caller.data(), &subject) {
                return -1;
            }
            let payload = payload_buf.into();

            let nats = caller.data().app_state.nats.clone();

            let _span = tracing::info_span!(
                "nats.publish",
                otel.kind = "producer",
                subject = %subject
            )
            .entered();
            let nats_headers = crate::telemetry::traced_nats_headers();

            let result = block_on(crate::nats_conn::publish_bounded(
                &nats,
                subject,
                Some(nats_headers),
                payload,
            ));

            match result {
                Ok(_) => 0,
                Err(e) => {
                    tracing::error!("[wasm] nats_publish error: {}", e);
                    -1
                }
            }
        },
    )?;

    wrap(
        linker,
        names,
        "nats_request",
        |mut caller: Caller<'_, HostState>,
         subject_ptr: i32,
         subject_len: i32,
         payload_ptr: i32,
         payload_len: i32,
         timeout_ms: i32|
         -> i32 {
            let subject = host_try!(read_guest_string(
                &mut caller,
                "nats_request",
                subject_ptr,
                subject_len
            ));
            let payload = host_try!(read_guest_bytes(
                &mut caller,
                "nats_request",
                payload_ptr,
                payload_len
            ));
            if !authorize_nats_send(caller.data(), &subject) {
                return -1;
            }

            let nats = caller.data().app_state.nats.clone();

            let _span = tracing::info_span!(
                "nats.request",
                otel.kind = "client",
                subject = %subject
            )
            .entered();
            let trace_headers = crate::telemetry::traced_nats_headers();
            let timeout = clamp_to_budget(
                Some(Duration::from_millis(u64::from(
                    timeout_ms.max(1000).unsigned_abs(),
                ))),
                outbound_http_timeout(),
                caller.data().remaining_budget(),
            );
            let result = block_on(async {
                let inbox = nats.new_inbox();
                let mut inbox_sub = tokio::time::timeout(
                    crate::nats_conn::publish_timeout(),
                    nats.subscribe(inbox.clone()),
                )
                .await
                .map_err(|_| eyre::eyre!("NATS subscribe timed out"))?
                .map_err(|e| eyre::eyre!("NATS subscribe failed: {}", e))?;
                let mut headers = trace_headers;
                headers.insert(
                    crate::nats_manager::subscriptions::CUFFLINK_REPLY_HEADER,
                    inbox.as_str(),
                );
                crate::nats_conn::publish_bounded(&nats, subject, Some(headers), payload.into())
                    .await
                    .map_err(|e| eyre::eyre!("NATS publish failed: {}", e))?;
                let reply = tokio::time::timeout(timeout, inbox_sub.next())
                    .await
                    .map_err(|_| eyre::eyre!("NATS request timed out"))?
                    .ok_or_else(|| eyre::eyre!("NATS inbox closed"))?;
                Ok::<String, eyre::Report>(String::from_utf8_lossy(&reply.payload).to_string())
            });

            match result {
                Ok(response_str) => {
                    caller.data_mut().host_response = Some(response_str);
                    0
                }
                Err(e) => {
                    tracing::error!("[wasm] nats_request error: {}", e);
                    -1
                }
            }
        },
    )?;

    Ok(())
}

#[cfg(test)]
mod nats_authz_tests {
    use super::{NatsPublishPolicy, PublishDecision};
    use crate::nats_manager::subjects::{Claim, Namespace, PublishScope};
    use cufflink_types::nats_names::SUBJECT_MAX;

    fn acme() -> PublishScope {
        PublishScope::own("acme")
    }

    fn acme_among_claims() -> PublishScope {
        PublishScope {
            namespace: Namespace::new("acme", [Claim::subject("orders.>")]),
            foreign: vec!["books.>".into(), "audit.logs".into()],
        }
    }

    #[test]
    fn legacy_allows_anything_but_flags_it() {
        let p = NatsPublishPolicy::Legacy;
        assert_eq!(
            p.decide(&acme(), "anything.at.all"),
            PublishDecision::LegacyAllow
        );
    }

    #[test]
    fn enforce_allows_own_namespace_and_declared_only() {
        let p = NatsPublishPolicy::Enforce {
            allow: vec!["acme.items.*".into(), "acme.batches.closed".into()],
        };
        assert_eq!(
            p.decide(&acme(), "dw.acme.orders.created"),
            PublishDecision::Allow
        );
        assert_eq!(
            p.decide(&acme(), "acme.items.updated"),
            PublishDecision::Allow
        );
        assert_eq!(
            p.decide(&acme(), "acme.batches.closed"),
            PublishDecision::Allow
        );
        assert_eq!(
            p.decide(&acme(), "dw.other.orders.created"),
            PublishDecision::Reserved
        );
        assert_eq!(
            p.decide(&acme(), "acme.secrets.leaked"),
            PublishDecision::Deny
        );
    }

    #[test]
    fn enforce_with_empty_allowlist_still_permits_own_namespace() {
        let p = NatsPublishPolicy::Enforce { allow: vec![] };
        assert_eq!(p.decide(&acme(), "dw.acme.x.y"), PublishDecision::Allow);
        assert_eq!(p.decide(&acme(), "dw.other.x.y"), PublishDecision::Reserved);
        assert_eq!(
            p.decide(&acme(), "dw.acme"),
            PublishDecision::Deny,
            "the namespace is what is under dw.acme, as it always was"
        );
    }

    #[test]
    fn legacy_denies_reserved() {
        let p = NatsPublishPolicy::Legacy;
        for subject in [
            "cufflink.wasm.svc.handler",
            "$JS.API.STREAM.DELETE.DW_ACME",
            "undeploy.web.acme.svc",
            "_INBOX.stolen",
            "dw.victim.logs",
        ] {
            assert_eq!(
                p.decide(&acme(), subject),
                PublishDecision::Reserved,
                "{subject}"
            );
        }
        assert_eq!(p.decide(&acme(), "dw.acme.x"), PublishDecision::LegacyAllow);
    }

    #[test]
    fn no_policy_publishes_on_what_is_not_a_subject() {
        let too_long = format!("dw.acme.{}", "x".repeat(SUBJECT_MAX));
        for policy in [
            NatsPublishPolicy::Legacy,
            NatsPublishPolicy::Enforce {
                allow: vec![">".into()],
            },
        ] {
            for (subject, reason) in [
                ("dw.acme.*", "'dw.acme.*' is not a subject"),
                ("dw.acme.>", "'dw.acme.>' is not a subject"),
                ("dw..acme", "'dw..acme' is not a subject"),
                (
                    too_long.as_str(),
                    "a subject of 1032 bytes is longer than the 1024 bytes a message is published on",
                ),
            ] {
                assert_eq!(
                    policy.decide(&acme(), subject),
                    PublishDecision::Invalid(reason.into()),
                    "{subject}"
                );
            }
        }
    }

    #[test]
    fn enforce_allowlist_cannot_grant_reserved() {
        let p = NatsPublishPolicy::Enforce {
            allow: vec![">".into(), "cufflink.>".into(), "dw.*.>".into()],
        };
        assert_eq!(
            p.decide(&acme(), "cufflink.tick.5min"),
            PublishDecision::Reserved
        );
        assert_eq!(
            p.decide(&acme(), "dw.victim.logs"),
            PublishDecision::Reserved
        );
        assert_eq!(p.decide(&acme(), "acme.custom"), PublishDecision::Allow);
    }

    #[test]
    fn no_policy_reaches_a_subject_another_tenant_claims() {
        for policy in [
            NatsPublishPolicy::Legacy,
            NatsPublishPolicy::Enforce { allow: vec![] },
            NatsPublishPolicy::Enforce {
                allow: vec!["books.>".into(), ">".into()],
            },
        ] {
            for subject in ["books.created", "books.a.b.c", "audit.logs"] {
                assert_eq!(
                    policy.decide(&acme_among_claims(), subject),
                    PublishDecision::Claimed,
                    "{subject}"
                );
            }
        }
    }

    #[test]
    fn what_no_tenant_claims_is_decided_as_it_was_before_claims() {
        let scope = acme_among_claims();
        for subject in ["misc.thing", "audit.other", "books"] {
            assert_eq!(
                NatsPublishPolicy::Legacy.decide(&scope, subject),
                NatsPublishPolicy::Legacy.decide(&acme(), subject),
                "{subject}"
            );
            assert_eq!(
                NatsPublishPolicy::Legacy.decide(&scope, subject),
                PublishDecision::LegacyAllow
            );
            let declared = NatsPublishPolicy::Enforce {
                allow: vec!["misc.>".into()],
            };
            assert_eq!(
                declared.decide(&scope, subject),
                declared.decide(&acme(), subject),
                "{subject}"
            );
        }
    }

    #[test]
    fn a_tenants_own_claim_is_its_namespace() {
        let scope = acme_among_claims();
        assert_eq!(
            NatsPublishPolicy::Enforce { allow: vec![] }.decide(&scope, "orders.created"),
            PublishDecision::Allow
        );
        assert_eq!(
            NatsPublishPolicy::Legacy.decide(&scope, "orders.created"),
            PublishDecision::LegacyAllow
        );
    }
}

#[cfg(test)]
mod publish_v2_tests {
    use super::*;

    fn request(headers: &[(&str, &str)], msg_id: Option<&str>) -> PublishRequest {
        PublishRequest {
            subject: "dw.acme.books.created".into(),
            payload: b"{}".to_vec(),
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            msg_id: msg_id.map(str::to_string),
            ..Default::default()
        }
    }

    fn traced() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "00-host-span-01");
        headers
    }

    fn values(headers: &HeaderMap, name: &str) -> Vec<String> {
        headers
            .get_all(name)
            .map(|value| value.to_string())
            .collect()
    }

    #[test]
    fn the_hosts_trace_and_the_message_id_ride_with_the_guests_headers() {
        let headers = publish_headers(
            &request(
                &[
                    ("x-source", "a"),
                    ("x-source", "b"),
                    ("traceparent", "00-guest-forged-01"),
                    ("TraceState", "vendor=x"),
                ],
                Some("book-7"),
            ),
            traced(),
        )
        .expect("headers");
        assert_eq!(values(&headers, "x-source"), ["a", "b"]);
        assert_eq!(values(&headers, "traceparent"), ["00-host-span-01"]);
        assert!(values(&headers, "tracestate").is_empty());
        assert_eq!(values(&headers, MSG_ID_HEADER), ["book-7"]);
        assert!(crate::nats_manager::headers::msg_id_unhidden(&headers));
        let none = publish_headers(&request(&[], Some("")), HeaderMap::new()).expect("headers");
        assert!(
            values(&none, MSG_ID_HEADER).is_empty(),
            "an empty id is none"
        );
    }

    fn is_invalid(refused: &Result<HeaderMap, PublishResponse>) -> bool {
        matches!(
            refused,
            Err(PublishResponse::Error {
                kind: PublishErrorKind::Invalid,
                ..
            })
        )
    }

    #[test]
    fn a_header_the_server_or_the_platform_acts_on_is_refused() {
        for name in [
            "Nats-Msg-Id",
            "nats-expected-stream",
            "Cufflink-Reply",
            "CUFFLINK-DLQ-REASON",
        ] {
            assert!(
                is_invalid(&publish_headers(&request(&[(name, "x")], None), traced())),
                "{name}"
            );
        }
        assert!(publish_headers(&request(&[("x-nats-like", "1")], None), traced()).is_ok());
        assert!(is_invalid(&publish_headers(
            &request(&[("x-note", "copied from Nats-Msg-Id")], Some("book-7")),
            traced()
        )));
    }

    #[test]
    fn what_the_client_would_panic_on_is_refused() {
        for name in [
            "", "x bad", "x:bad", "x\rbad", "x\nbad", "x\0bad", "naïve", " ", "x-ok ",
        ] {
            assert!(
                is_invalid(&publish_headers(&request(&[(name, "v")], None), traced())),
                "{name:?}"
            );
        }
        for value in ["a\r\nNats-Expected-Stream: OTHER", "a\nb", "a\rb", "a\0b"] {
            assert!(
                is_invalid(&publish_headers(
                    &request(&[("x-source", value)], None),
                    traced()
                )),
                "{value:?}"
            );
            assert!(
                is_invalid(&publish_headers(&request(&[], Some(value)), traced())),
                "message id {value:?}"
            );
            assert!(
                is_invalid(&publish_headers(
                    &request(&[("traceparent", value)], None),
                    traced()
                )),
                "a header the host replaces is read all the same: {value:?}"
            );
        }
    }

    #[test]
    fn the_headers_count_towards_what_the_server_takes() {
        let headers = publish_headers(&request(&[("x-source", "abc")], Some("book-7")), traced())
            .expect("headers");
        let carried = headers::wire_len(&headers);
        assert!(carried > "x-source: abc".len());
        assert!(within_max_payload(100, &headers, 100 + carried).is_ok());
        for (payload, max_payload) in [(100, 99 + carried), (usize::MAX, usize::MAX)] {
            assert!(
                matches!(
                    within_max_payload(payload, &headers, max_payload),
                    Err(PublishResponse::Error {
                        kind: PublishErrorKind::TooLarge,
                        ..
                    })
                ),
                "{payload}"
            );
        }
        assert!(within_max_payload(100, &HeaderMap::new(), 100).is_ok());
    }

    fn stream() -> StreamRef {
        StreamRef {
            name: "BOOKS".into(),
            retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        }
    }

    async fn two_steps(
        step: Duration,
        timeout: Duration,
        cancel: CancellationToken,
    ) -> (Result<PublishResponse, PublishResponse>, Duration) {
        tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let ended = within(
                timeout,
                &cancel,
                acked_by_stream(
                    "dw.acme.books.created",
                    async move {
                        tokio::time::sleep(step).await;
                        Some(stream())
                    },
                    async move {
                        tokio::time::sleep(step).await;
                        Err(PublishError::Failed("refused".into()))
                    },
                ),
            );
            (ended, started.elapsed())
        })
        .await
        .expect("join")
    }

    fn kind(ended: &Result<PublishResponse, PublishResponse>) -> Option<PublishErrorKind> {
        match ended {
            Err(PublishResponse::Error { kind, .. }) => Some(*kind),
            _ => None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn finding_the_stream_and_publishing_share_one_bound() {
        let timeout = Duration::from_millis(1500);
        let (ended, took) =
            two_steps(Duration::from_secs(1), timeout, CancellationToken::new()).await;
        assert_eq!(kind(&ended), Some(PublishErrorKind::Timeout), "{ended:?}");
        assert!(
            took < Duration::from_secs(2),
            "each step within the bound, both beyond it: {took:?}"
        );
        let (ended, _) =
            two_steps(Duration::from_millis(10), timeout, CancellationToken::new()).await;
        assert_eq!(
            kind(&ended),
            Some(PublishErrorKind::Unavailable),
            "within the bound the publish ends as it did: {ended:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_invocation_stops_waiting_on_its_publish() {
        let cancel = CancellationToken::new();
        let canceller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            canceller.cancel();
        });
        let (ended, took) =
            two_steps(Duration::from_secs(30), Duration::from_secs(60), cancel).await;
        assert_eq!(kind(&ended), Some(PublishErrorKind::Timeout), "{ended:?}");
        assert!(took < Duration::from_secs(5), "{took:?}");
    }

    #[tokio::test]
    async fn nothing_is_published_where_no_stream_captures_the_subject() {
        let published = std::sync::atomic::AtomicBool::new(false);
        let ended = acked_by_stream("dw.acme.x", async { None }, async {
            published.store(true, std::sync::atomic::Ordering::SeqCst);
            Err(PublishError::Failed("unreached".into()))
        })
        .await;
        assert_eq!(kind(&ended), Some(PublishErrorKind::NoStream), "{ended:?}");
        assert!(!published.load(std::sync::atomic::Ordering::SeqCst));
    }
}
