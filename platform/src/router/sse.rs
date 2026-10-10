use crate::audit::Read;
use crate::auth::columns::ColumnView;
use crate::auth::scope::EffectiveScope;
use crate::auth::{AuthUser, OptionalAuth};
use crate::deployer::manifest::ColumnDefinition;
use crate::errors::{PlatformError, PlatformResult};
use crate::middleware::rate_limit::client_ip;
use crate::router::authz::{
    authorize_access, requested_scope, resolve_data_route, Access, DataRoute, DataTarget,
};
use crate::router::crud::{outlet_of, Asked, DataQuery};
use crate::tenant_sessions::Purpose;
use crate::AppState;
use axum::{
    extract::{ConnectInfo, Path, RawQuery, State},
    http::HeaderMap,
    response::sse::{Event, Sse},
};
use futures_util::stream::Stream;
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

type ActiveStreams = Arc<Mutex<HashMap<String, usize>>>;

#[derive(Clone)]
pub struct SseLimiter {
    max: usize,
    active: ActiveStreams,
}

pub struct SseSlot {
    key: String,
    active: ActiveStreams,
}

impl SseLimiter {
    pub fn new(max: usize) -> Self {
        Self {
            max,
            active: Arc::default(),
        }
    }

    pub fn acquire(&self, key: String) -> Option<SseSlot> {
        let mut active = self.active.lock().unwrap_or_else(PoisonError::into_inner);
        let count = active.entry(key.clone()).or_default();
        if *count >= self.max {
            return None;
        }
        *count += 1;
        metrics::gauge!("cufflink_sse_streams_active").increment(1.0);
        Some(SseSlot {
            key,
            active: Arc::clone(&self.active),
        })
    }

    pub fn active_for(&self, key: &str) -> usize {
        self.active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

impl Drop for SseSlot {
    fn drop(&mut self) {
        let mut active = self.active.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = active.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.key);
            }
        }
        metrics::gauge!("cufflink_sse_streams_active").decrement(1.0);
    }
}

pub(crate) fn principal_key(
    principal: Option<&AuthUser>,
    client: Option<IpAddr>,
) -> Option<String> {
    match (principal, client) {
        (Some(user), _) => Some(format!("{}:{}", user.tenant_id, user.subject)),
        (None, Some(client)) => Some(format!("anon:{client}")),
        (None, None) => None,
    }
}

pub(crate) fn stream_principal(
    principal: Option<&AuthUser>,
    client: Option<IpAddr>,
) -> PlatformResult<String> {
    principal_key(principal, client).ok_or_else(|| {
        PlatformError::BadRequest("An anonymous event stream needs the caller's address".into())
    })
}

pub(crate) struct Delivered {
    pub scope: EffectiveScope,
    pub columns: Vec<ColumnDefinition>,
    pub view: ColumnView,
}

const EVENT_KEYS: [&str; 4] = ["action", "service", "table", "tenant_id"];

pub(crate) fn event_visible(payload: &[u8], delivered: &Delivered) -> Option<String> {
    let Delivered {
        scope,
        columns,
        view,
    } = delivered;
    if !view.withholds_reads() && *scope == EffectiveScope::All {
        return Some(String::from_utf8_lossy(payload).into_owned());
    }
    let mut event: Value = serde_json::from_slice(payload).ok()?;
    let row = event.get_mut("data")?;
    if !scope.matches(row, columns) {
        return None;
    }
    if !view.withholds_reads() {
        return Some(event.to_string());
    }
    let read: serde_json::Map<String, Value> = std::mem::take(row.as_object_mut()?)
        .into_iter()
        .filter(|(name, _)| view.reads(name) && columns.iter().any(|c| c.name == *name))
        .collect();
    let event = event.as_object_mut()?;
    let said: serde_json::Map<String, Value> = EVENT_KEYS
        .into_iter()
        .filter_map(|key| event.remove(key).map(|value| (key.to_string(), value)))
        .chain([("data".to_string(), Value::Object(read))])
        .collect();
    Some(Value::Object(said).to_string())
}

fn event_type(subject: &str) -> String {
    subject.rsplit('.').next().unwrap_or("message").to_string()
}

async fn forward<S>(
    mut events: S,
    tx: mpsc::Sender<Result<Event, Infallible>>,
    delivered: Delivered,
    shutdown: tokio_util::sync::CancellationToken,
) where
    S: Stream<Item = async_nats::Message> + Unpin,
{
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                let _ = tx.send(Ok(Event::default().event("shutdown"))).await;
                return;
            }
            _ = tx.closed() => return,
            next = events.next() => {
                let Some(msg) = next else {
                    return;
                };
                let Some(data) = event_visible(&msg.payload, &delivered) else {
                    continue;
                };
                let event = Event::default().event(event_type(&msg.subject)).data(data);
                if tx.send(Ok(event)).await.is_err() {
                    return;
                }
            }
        }
    }
}

pub(crate) async fn stream_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    target: DataTarget,
    OptionalAuth(auth_user): OptionalAuth,
    Path((tenant_slug, service_name, table_name)): Path<(String, String, String)>,
    RawQuery(raw_query): RawQuery,
) -> PlatformResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let DataRoute {
        service,
        tenant,
        principal,
        call,
    } = resolve_data_route(
        &state,
        &headers,
        &target,
        &tenant_slug,
        &service_name,
        auth_user,
    )
    .await?;

    let table = service
        .manifest
        .get_table(&table_name)
        .ok_or_else(|| PlatformError::NotFound(format!("Table '{}' not found", table_name)))?;

    let Access { rows, columns } = authorize_access(
        &state,
        service.tenant_id,
        table,
        "view",
        &principal,
        requested_scope(
            table,
            DataQuery::from_raw(raw_query.as_deref()).scope.as_deref(),
        )?,
    )
    .await?;

    let outlet = outlet_of(
        &state,
        &tenant,
        &service,
        table,
        Asked {
            path: (&tenant_slug, &service_name, &table_name),
            caller: &principal,
            call: call.as_ref(),
            headers: &headers,
        },
    );
    if table.sensitive && outlet.durable() {
        let client = state
            .tenant_sessions
            .service(&service.identity(), Purpose::Crud)
            .await?
            .get()
            .await?;
        outlet.read(&client, Read::default(), rows.scoped()).await?;
    }

    let slot = state
        .sse_limiter
        .acquire(stream_principal(
            principal.as_ref(),
            client_ip(&headers, Some(peer.ip()), state.config.client_ip_source),
        )?)
        .ok_or_else(|| {
            metrics::counter!("cufflink_sse_rejected_total", "reason" => "cap").increment(1);
            PlatformError::TooManyRequests("Too many open streams for this principal".into())
        })?;

    let bus_subject = format!("dw.{}.{}.{}.>", tenant_slug, service_name, table_name);
    let subscriber = state
        .nats
        .subscribe(bus_subject)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to subscribe to NATS: {}", e)))?;

    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(32);

    let _ = tx
        .send(Ok(Event::default().event("connected").data(
            serde_json::json!({
                "service": service_name,
                "table": table_name,
                "tenant": tenant_slug,
            })
            .to_string(),
        )))
        .await;

    let delivered = Delivered {
        scope: rows.into_filter(),
        columns: table.columns.clone(),
        view: columns,
    };
    let token = state.shutdown.token();
    state.shutdown.spawn("sse_forwarder", async move {
        let _slot = slot;
        forward(subscriber, tx, delivered, token).await;
    });

    let stream = ReceiverStream::new(rx);
    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::binding::tests::jwt_user;
    use crate::auth::scope::RowPredicate;
    use crate::tenant_catalog::tests::tenant;
    use serde_json::json;
    use std::time::Duration;

    fn crud_event(owner: Value) -> Vec<u8> {
        json!({"action": "created", "table": "customer_notifications", "data": {"id": "n1", "customer_id": owner}})
            .to_string()
            .into_bytes()
    }

    fn message(subject: &str, payload: Vec<u8>) -> async_nats::Message {
        async_nats::Message {
            subject: subject.into(),
            reply: None,
            payload: payload.into(),
            headers: None,
            status: None,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn principal_key_shapes() {
        let t = tenant("default", Some("orchard"));
        let user = jwt_user(&t, "orchard", "portal", &[]);
        let peer: SocketAddr = "10.1.2.3:4567".parse().expect("addr");
        assert_eq!(
            principal_key(Some(&user), None),
            Some(format!("{}:user-1", t.id))
        );
        assert_eq!(
            principal_key(None, Some(peer.ip())).as_deref(),
            Some("anon:10.1.2.3")
        );
        assert_eq!(principal_key(None, None), None);
    }

    fn columns() -> Vec<ColumnDefinition> {
        serde_json::from_value(json!([
            {"name": "id", "column_type": "text"},
            {"name": "customer_id", "column_type": "text"},
            {"name": "org", "column_type": "uuid"},
        ]))
        .expect("columns")
    }

    fn owned_by(subject: &str) -> EffectiveScope {
        EffectiveScope::Rows(vec![RowPredicate::Owner {
            column: "customer_id".into(),
            subject: subject.into(),
        }])
    }

    fn every_column(scope: EffectiveScope) -> Delivered {
        Delivered {
            scope,
            columns: columns(),
            view: ColumnView::ALL,
        }
    }

    fn without_org(scope: EffectiveScope) -> Delivered {
        let table = crate::deployer::manifest::TableDefinition {
            name: "customer_notifications".into(),
            columns: serde_json::from_value(json!([
                {"name": "id", "column_type": "text", "primary_key": true},
                {"name": "customer_id", "column_type": "text"},
                {"name": "org", "column_type": "uuid", "access": {"read": "internal"}},
            ]))
            .expect("columns"),
            permission_area: Some("notifications".into()),
            ..Default::default()
        };
        Delivered {
            scope,
            view: crate::auth::columns::view(&table, &[], false),
            columns: table.columns,
        }
    }

    #[test]
    fn event_visible_filters_by_owner() {
        let visible = |owner: Value, scope: EffectiveScope| {
            event_visible(&crud_event(owner), &every_column(scope)).is_some()
        };
        assert!(visible(json!("c-1"), owned_by("c-1")));
        assert!(!visible(json!("c-2"), owned_by("c-1")));
        assert!(visible(json!(42), owned_by("42")));
        assert!(!visible(json!("c-1"), EffectiveScope::Denied));
        assert!(!visible(Value::Null, owned_by("c-1")));
    }

    #[test]
    fn event_visible_filters_by_claim_values() {
        let org = "0f8fad5b-d9cb-469f-a165-70867728950e";
        let scope = every_column(EffectiveScope::Rows(vec![RowPredicate::Claim {
            column: "org".into(),
            values: vec![json!(org)],
        }]));
        let event = |org: &str| {
            json!({"action": "updated", "data": {"id": "n1", "org": org}})
                .to_string()
                .into_bytes()
        };
        assert!(event_visible(&event(&org.to_uppercase()), &scope).is_some());
        assert!(event_visible(&event("5c3b5f1e-0000-4000-8000-000000000000"), &scope).is_none());
    }

    #[test]
    fn event_visible_passthrough_for_every_row() {
        let raw = b"{\"flat\":true}";
        assert_eq!(
            event_visible(raw, &every_column(EffectiveScope::All)).as_deref(),
            Some("{\"flat\":true}")
        );
    }

    #[test]
    fn event_visible_leaves_out_the_columns_the_subscriber_does_not_read() {
        let org = "0f8fad5b-d9cb-469f-a165-70867728950e";
        let event =
            json!({"action": "updated", "data": {"id": "n1", "customer_id": "c-1", "org": org}})
                .to_string()
                .into_bytes();
        let data = |delivered: &Delivered| -> Option<Value> {
            let event: Value = serde_json::from_str(&event_visible(&event, delivered)?).ok()?;
            event.get("data").cloned()
        };
        let shown = json!({"id": "n1", "customer_id": "c-1"});
        assert_eq!(data(&without_org(EffectiveScope::All)), Some(shown.clone()));
        assert_eq!(data(&without_org(owned_by("c-1"))), Some(shown));
        assert_eq!(data(&without_org(owned_by("c-2"))), None);
        let by_hidden_column = EffectiveScope::Rows(vec![RowPredicate::Claim {
            column: "org".into(),
            values: vec![json!(org)],
        }]);
        assert_eq!(
            data(&without_org(by_hidden_column)),
            Some(json!({"id": "n1", "customer_id": "c-1"}))
        );
        assert!(event_visible(b"{\"flat\":true}", &without_org(EffectiveScope::All)).is_none());
    }

    #[test]
    fn a_subscriber_without_every_column_is_delivered_nothing_but_a_crud_event() {
        let org = "0f8fad5b-d9cb-469f-a165-70867728950e";
        let delivered = |event: Value| -> Option<Value> {
            let payload = event.to_string().into_bytes();
            let shown = event_visible(&payload, &without_org(EffectiveScope::All))?;
            serde_json::from_str(&shown).ok()
        };
        assert_eq!(
            delivered(json!({
                "action": "updated", "service": "svc", "table": "customer_notifications",
                "tenant_id": "t", "previous": {"org": org},
                "data": {"id": "n1", "org": org, "nested": {"org": org}}
            })),
            Some(json!({
                "action": "updated", "service": "svc", "table": "customer_notifications",
                "tenant_id": "t", "data": {"id": "n1"}
            }))
        );
        for not_a_row in [json!([{"org": org}]), json!(org), Value::Null] {
            assert_eq!(
                delivered(json!({"action": "updated", "data": not_a_row})),
                None
            );
        }
        let every = every_column(EffectiveScope::All);
        let forged = json!({"data": [{"org": org}]}).to_string();
        assert_eq!(
            event_visible(forged.as_bytes(), &every).as_deref(),
            Some(forged.as_str()),
            "a subscriber that reads every column is passed the payload as it is"
        );
    }

    #[test]
    fn event_visible_drops_malformed_payload() {
        let owned = every_column(owned_by("c-1"));
        assert!(event_visible(b"not json", &owned).is_none());
        assert!(event_visible(b"{\"customer_id\":\"c-1\"}", &owned).is_none());
    }

    #[test]
    fn limiter_caps_and_releases_on_drop() {
        let limiter = SseLimiter::new(2);
        let first = limiter.acquire("t:u".into()).expect("first");
        let second = limiter.acquire("t:u".into()).expect("second");
        assert!(limiter.acquire("t:u".into()).is_none(), "cap reached");
        assert!(limiter.acquire("t:other".into()).is_some(), "per principal");
        drop(first);
        assert_eq!(limiter.active_for("t:u"), 1);
        assert!(limiter.acquire("t:u".into()).is_some());
        drop(second);
    }

    #[tokio::test]
    async fn forwarder_filters_and_ends_when_receiver_dropped() {
        let (events_tx, events_rx) = mpsc::channel(8);
        let (tx, mut rx) = mpsc::channel(8);
        let token = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(forward(
            ReceiverStream::new(events_rx),
            tx,
            every_column(owned_by("c-1")),
            token,
        ));
        let subject = "dw.default.notification-service.customer_notifications.created";
        for owner in ["c-2", "c-1"] {
            events_tx
                .send(message(subject, crud_event(json!(owner))))
                .await
                .expect("publish");
        }
        let delivered = rx.recv().await.expect("an event").expect("infallible");
        let rendered = format!("{delivered:?}");
        assert!(
            rendered.contains("c-1") && !rendered.contains("c-2"),
            "{rendered}"
        );

        drop(rx);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the forwarder ends once the client is gone")
            .expect("join");
    }

    #[tokio::test]
    async fn forwarder_sends_shutdown_and_ends() {
        let (_events_tx, events_rx) = mpsc::channel::<async_nats::Message>(1);
        let (tx, mut rx) = mpsc::channel(8);
        let token = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(forward(
            ReceiverStream::new(events_rx),
            tx,
            Delivered {
                scope: EffectiveScope::All,
                columns: vec![],
                view: ColumnView::ALL,
            },
            token.clone(),
        ));
        token.cancel();
        let last = rx
            .recv()
            .await
            .expect("shutdown event")
            .expect("infallible");
        assert!(format!("{last:?}").contains("shutdown"));
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("ends")
            .expect("join");
    }
}
