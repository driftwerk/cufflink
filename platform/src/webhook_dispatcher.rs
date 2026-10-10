use crate::egress::{EgressError, EgressGuard, EgressPolicy, EgressTarget, OutboundRequest};
use crate::policy::{PolicyKey, PolicyMode, PolicySet};
use crate::registry::RegisteredService;
use crate::repo::webhooks::WebhookRecord;
use crate::AppState;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Method;
use ring::hmac;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

pub const FEATURES: &[&str] = &[
    cufflink_types::features::OUTBOX,
    cufflink_types::features::SCHEMA_ADOPTED,
    cufflink_types::features::AUDIT_REDACTION,
];

const DELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ATTEMPTS: u64 = 3;
pub(crate) const RESPONSE_READ_CAP: u64 = 64 * 1024;

pub fn start(state: Arc<AppState>) {
    let shutdown = state.shutdown.clone();
    shutdown.spawn("webhook_dispatcher", dispatch_loop(state));
}

#[derive(Debug, PartialEq, Eq)]
pub struct EventSubject<'a> {
    pub tenant_slug: &'a str,
    pub service_name: &'a str,
    pub table_name: &'a str,
    pub event: &'static str,
}

pub fn parse_event_subject(subject: &str) -> Option<EventSubject<'_>> {
    let mut parts = subject.split('.');
    let (Some("dw"), Some(tenant_slug), Some(service_name), Some(table_name), Some(action), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    let event = match action {
        "created" => "create",
        "updated" => "update",
        "deleted" => "delete",
        _ => return None,
    };
    Some(EventSubject {
        tenant_slug,
        service_name,
        table_name,
        event,
    })
}

async fn dispatch_loop(state: Arc<AppState>) {
    use futures_util::StreamExt;

    let mut subscriber = match state
        .nats
        .queue_subscribe("dw.>", "webhook-dispatch".into())
        .await
    {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to subscribe for webhook dispatch: {}", e);
            return;
        }
    };

    info!("Webhook dispatcher started, listening for CRUD events");

    let token = state.shutdown.token();
    loop {
        let msg = tokio::select! {
            _ = token.cancelled() => return,
            msg = subscriber.next() => msg,
        };
        let Some(msg) = msg else {
            return;
        };
        let Some(subject) = parse_event_subject(&msg.subject) else {
            continue;
        };
        let Some(service) = state
            .registry
            .get_by_slug(subject.tenant_slug, subject.service_name)
            .await
        else {
            continue;
        };
        if !payload_tenant_matches(&msg.payload, service.tenant_id)
            || delivered_from_the_outbox(&state, service.tenant_id)
        {
            continue;
        }
        let table = subject.table_name.to_string();
        let event = subject.event;
        let payload = msg.payload.to_vec();
        let task_state = Arc::clone(&state);
        state.shutdown.spawn("webhook_dispatch", async move {
            if let Err(e) = dispatch(&task_state, &service, &table, event, &payload).await {
                warn!(
                    tenant = %service.tenant_slug,
                    service = %service.name,
                    table = %table,
                    event,
                    error = %e,
                    "Webhook dispatch error"
                );
            }
        });
    }
}

fn delivered_from_the_outbox(state: &AppState, tenant_id: Uuid) -> bool {
    state
        .tenants
        .get(tenant_id)
        .is_some_and(|tenant| tenant.mode(PolicyKey::Outbox) == PolicyMode::Enforce)
}

fn payload_tenant_matches(payload: &[u8], tenant_id: Uuid) -> bool {
    let claimed = serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .and_then(|event| event.get("tenant_id")?.as_str()?.parse::<Uuid>().ok());
    match claimed {
        Some(claimed) if claimed == tenant_id => true,
        Some(_) => {
            metrics::counter!(
                "cufflink_webhook_dispatch_denied_total",
                "reason" => "payload_tenant_mismatch",
            )
            .increment(1);
            false
        }
        None => false,
    }
}

async fn dispatch(
    state: &Arc<AppState>,
    service: &RegisteredService,
    table_name: &str,
    event: &'static str,
    payload: &[u8],
) -> eyre::Result<()> {
    let webhooks = {
        let client = state.platform_db.get().await?;
        crate::repo::webhooks::list_active_for_event(
            &state.secrets,
            &client,
            service.service_id,
            table_name,
            event,
        )
        .await?
    };
    if webhooks.is_empty() {
        return Ok(());
    }
    let policies = state.tenants.policies_or_load(service.tenant_id).await?;
    for webhook in &webhooks {
        deliver(&state.egress, &policies, service, webhook, event, payload).await;
    }
    Ok(())
}

async fn deliver(
    egress: &EgressGuard,
    policies: &PolicySet,
    service: &RegisteredService,
    webhook: &WebhookRecord,
    event: &'static str,
    payload: &[u8],
) {
    let policy = EgressPolicy::new(policies, &service.tenant_slug);
    let check = |target: &EgressTarget| policy.check(target);
    let delivery_id = Uuid::new_v4();
    for attempt in 1..=MAX_ATTEMPTS {
        let outcome = match signed_request(webhook, event, payload, delivery_id) {
            Ok(request) => egress.send(request, &check, false).await,
            Err(e) => {
                warn!(webhook = %webhook.id, error = %e, "Webhook cannot be signed");
                return;
            }
        };
        match outcome {
            Ok(response) if response.status().is_success() => {
                let status = response.status();
                if let Err(e) = EgressGuard::read_body(response, RESPONSE_READ_CAP).await {
                    warn!(webhook = %webhook.id, error = %e, "Webhook response not read");
                }
                info!(webhook = %webhook.id, event, table = %webhook.table_name, %status, "Webhook delivered");
                metrics::counter!("cufflink_webhooks_delivered_total", "event" => event)
                    .increment(1);
                return;
            }
            Ok(response) => {
                warn!(webhook = %webhook.id, status = %response.status(), attempt, "Webhook failed");
            }
            Err(e) => match refusal(&e) {
                Some(reason) => {
                    refused(webhook, reason, &e.to_string());
                    return;
                }
                None => warn!(webhook = %webhook.id, error = %e, attempt, "Webhook error"),
            },
        }
        if attempt == MAX_ATTEMPTS {
            metrics::counter!("cufflink_webhooks_failed_total", "event" => event).increment(1);
            warn!(webhook = %webhook.id, "Webhook delivery failed after {MAX_ATTEMPTS} attempts");
            return;
        }
        tokio::time::sleep(Duration::from_secs(attempt)).await;
    }
}

pub(crate) fn refusal(error: &EgressError) -> Option<&'static str> {
    match error {
        EgressError::Denied(denied) if denied.is_permanent() => Some(denied.reason()),
        EgressError::Policy(denied) => Some(denied.code),
        _ => None,
    }
}

fn refused(webhook: &WebhookRecord, reason: &'static str, detail: &str) {
    metrics::counter!("cufflink_webhook_dispatch_denied_total", "reason" => reason).increment(1);
    warn!(webhook = %webhook.id, reason, detail, "Webhook delivery refused");
}

pub(crate) fn signed_request(
    webhook: &WebhookRecord,
    event: &str,
    payload: &[u8],
    delivery_id: Uuid,
) -> eyre::Result<OutboundRequest> {
    let timestamp = chrono::Utc::now().timestamp();
    let header = |value: String| HeaderValue::from_str(&value).map_err(eyre::Report::from);
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        "x-cufflink-signature",
        header(format!(
            "sha256={}",
            signature_v1(webhook.secret.expose(), payload)
        ))?,
    );
    headers.insert(
        "x-cufflink-signature-256",
        header(signature_v2(webhook.secret.expose(), timestamp, payload))?,
    );
    headers.insert("x-cufflink-event", header(event.to_string())?);
    headers.insert("x-cufflink-delivery", header(delivery_id.to_string())?);
    Ok(OutboundRequest {
        method: Method::POST,
        url: webhook.url.clone(),
        headers,
        body: Some(payload.to_vec()),
        timeout: DELIVERY_TIMEOUT,
    })
}

fn hmac_hex(secret: &str, parts: &[&[u8]]) -> String {
    let mut context =
        hmac::Context::with_key(&hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()));
    parts.iter().for_each(|part| context.update(part));
    hex::encode(context.sign())
}

pub fn signature_v1(secret: &str, payload: &[u8]) -> String {
    hmac_hex(secret, &[payload])
}

pub fn signature_v2(secret: &str, timestamp: i64, payload: &[u8]) -> String {
    let prefix = format!("{timestamp}.");
    format!(
        "t={timestamp},v1={}",
        hmac_hex(secret, &[prefix.as_bytes(), payload])
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_event_subject_shapes() {
        assert_eq!(
            parse_event_subject("dw.default.stock-service.items.created"),
            Some(EventSubject {
                tenant_slug: "default",
                service_name: "stock-service",
                table_name: "items",
                event: "create",
            })
        );
        assert_eq!(
            parse_event_subject("dw.default.stock-service.items.deleted").map(|s| s.event),
            Some("delete")
        );
        for subject in [
            "dw.default.logs",
            "dw.default.svc.items.archived",
            "dw.default.svc.items.created.extra",
            "cufflink.default.svc.items.created",
        ] {
            assert_eq!(parse_event_subject(subject), None, "{subject}");
        }
    }

    #[test]
    fn signature_v1_unchanged() {
        assert_eq!(
            signature_v1("whsec", br#"{"id":1}"#),
            "cc2f1f736275a85762c723f8a3c29fbecbb7c1427ad1ddbe14589ad49036a629"
        );
    }

    #[test]
    fn signature_v2_format_and_hmac_vector() {
        assert_eq!(
            signature_v2("whsec", 1_700_000_000, br#"{"id":1}"#),
            "t=1700000000,v1=e79220cb981f992adbc8b93ac6d46028b0217ea19327d27dc9d18bf334403bde"
        );
    }

    #[test]
    fn only_permanent_failures_are_refused() {
        use crate::egress::EgressDenied;
        use crate::policy::{Denied, PolicyKey};
        let denied = |d: EgressDenied| EgressError::Denied(d);
        assert_eq!(
            refusal(&denied(EgressDenied::MetadataEndpoint("m".into()))),
            Some("metadata")
        );
        assert_eq!(
            refusal(&denied(EgressDenied::InvalidUrl("u".into()))),
            Some("invalid_url")
        );
        assert_eq!(
            refusal(&EgressError::Policy(Denied {
                key: PolicyKey::Egress,
                code: "private_address",
            })),
            Some("private_address")
        );
        assert_eq!(
            refusal(&denied(EgressDenied::Unresolvable("dns".into()))),
            None
        );
        assert_eq!(
            refusal(&EgressError::TimedOut(Duration::from_secs(1))),
            None
        );
        assert_eq!(refusal(&EgressError::TooManyRedirects), None);
    }

    #[tokio::test]
    async fn deliveries_do_not_follow_redirects() {
        use axum::routing::post;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let landed = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/hook",
                post(|| async {
                    (
                        reqwest::StatusCode::FOUND,
                        [(reqwest::header::LOCATION, "/landing")],
                    )
                }),
            )
            .route(
                "/landing",
                post({
                    let landed = Arc::clone(&landed);
                    move || async move {
                        landed.fetch_add(1, Ordering::SeqCst);
                        "ok"
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let service = RegisteredService {
            service_id: Uuid::new_v4(),
            tenant_id: Uuid::new_v4(),
            tenant_slug: "acme".into(),
            name: "shop".into(),
            manifest: serde_json::from_value(serde_json::json!({
                "name": "shop",
                "version": "1",
                "tables": [],
                "mode": "crud"
            }))
            .expect("manifest"),
            version: 1,
            wasm_hash: None,
            commit_hash: None,
        };
        let webhook = WebhookRecord {
            id: Uuid::new_v4(),
            tenant_id: service.tenant_id,
            service_id: service.service_id,
            service_name: service.name.clone(),
            table_name: "orders".into(),
            event: "create".into(),
            url: format!("http://{addr}/hook"),
            secret: "whsec".into(),
            active: true,
            created_at: chrono::Utc::now(),
        };
        deliver(
            &EgressGuard::new(1024, Duration::from_secs(2)),
            &PolicySet::builtin_defaults(),
            &service,
            &webhook,
            "create",
            br#"{"id":1}"#,
        )
        .await;
        assert_eq!(landed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn payload_tenant_must_match_the_subject_tenant() {
        let tenant = Uuid::new_v4();
        let event =
            |id: String| format!(r#"{{"action":"created","tenant_id":"{id}"}}"#).into_bytes();
        assert!(payload_tenant_matches(&event(tenant.to_string()), tenant));
        assert!(!payload_tenant_matches(
            &event(Uuid::new_v4().to_string()),
            tenant
        ));
        assert!(!payload_tenant_matches(br#"{"action":"created"}"#, tenant));
        assert!(!payload_tenant_matches(b"not json", tenant));
    }
}
