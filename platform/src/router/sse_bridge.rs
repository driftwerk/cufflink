use crate::errors::{PlatformError, PlatformResult};
use crate::nats_manager::subjects::{resolve_subject, subject_covers};
use crate::registry::{RegisteredService, ServiceIdentity};
use crate::router::charged_guest_text;
use crate::router::custom::{execute, Execution};
use crate::router::sse::SseSlot;
use crate::wasm_runtime::budget::{ChargedJson, Hold};
use crate::wasm_runtime::{Class, CompiledModule, GuestOutput, InvocationKind};
use crate::AppState;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use cufflink_types::host::request::GuestRequest;
use cufflink_types::SseRouteDef;
use futures_util::{FutureExt, StreamExt};
use serde_json::{json, Map, Value};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

const MAX_BUFFERED: usize = 1000;
const MAX_STATE_BYTES: usize = 64 * 1024;
const REDUCER_BUDGET: Duration = Duration::from_secs(5);
const TIMEOUT_SECS: std::ops::RangeInclusive<u64> = 1..=3600;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseEvent {
    event: Option<String>,
    data: String,
    id: Option<String>,
}

impl SseEvent {
    fn parse(value: &Value) -> Result<Self, String> {
        let field = |key: &str| match &value[key] {
            Value::Null => Ok(None),
            Value::String(text) if !text.contains(['\r', '\n', '\0']) => Ok(Some(text.clone())),
            _ => Err(format!("event {key}")),
        };
        Ok(Self {
            event: field("event")?,
            data: match &value["data"] {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            },
            id: field("id")?,
        })
    }

    fn data_only(data: String) -> Self {
        Self {
            event: None,
            data,
            id: None,
        }
    }

    fn render(&self) -> Event {
        let event = Event::default().data(&self.data);
        let event = match &self.event {
            Some(name) => event.event(name),
            None => event,
        };
        match &self.id {
            Some(id) => event.id(id),
            None => event,
        }
    }
}

fn events(value: &Value) -> Result<Vec<SseEvent>, String> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(events) => events.iter().map(SseEvent::parse).collect(),
        _ => Err("events".into()),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SseSpec {
    initial: Vec<SseEvent>,
    state: Value,
    keep_alive: Duration,
    ping: Option<(Duration, SseEvent)>,
    timeout: Duration,
}

impl SseSpec {
    pub(crate) fn parse(value: &Value) -> Result<Self, String> {
        let secs = |value: &Value, key: &str| {
            value[key]
                .as_u64()
                .filter(|secs| TIMEOUT_SECS.contains(secs))
                .map(Duration::from_secs)
                .ok_or_else(|| format!("__sse.{key}"))
        };
        let ping = match &value["ping"] {
            Value::Null => None,
            ping => Some((
                secs(ping, "interval_secs")?,
                SseEvent::parse(&ping["event"])?,
            )),
        };
        Ok(Self {
            initial: events(&value["initial"])?,
            state: value["state"].clone(),
            keep_alive: secs(value, "keep_alive_secs")?,
            ping,
            timeout: secs(value, "timeout_secs")?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Schedule {
    deadline: Instant,
    ping: Option<(Duration, Instant)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tick {
    Ping,
    Timeout,
}

impl Schedule {
    fn new(start: Instant, timeout: Duration, ping: Option<Duration>) -> Self {
        Self {
            deadline: start + timeout,
            ping: ping.map(|every| (every, start + every)),
        }
    }

    fn next(&self) -> (Instant, Tick) {
        match self.ping {
            Some((_, at)) if at < self.deadline => (at, Tick::Ping),
            _ => (self.deadline, Tick::Timeout),
        }
    }

    fn pinged(self) -> Self {
        Self {
            ping: self.ping.map(|(every, at)| (every, at + every)),
            ..self
        }
    }
}

pub(crate) struct Bridge {
    subscriber: async_nats::Subscriber,
    slot: SseSlot,
}

pub(crate) async fn open(
    state: &AppState,
    sse: &SseRouteDef,
    service: &RegisteredService,
    params: &[(String, String)],
    principal: String,
) -> PlatformResult<Bridge> {
    let subject = sse
        .resolve(&service.tenant_slug, params)
        .map_err(|e| PlatformError::BadRequest(e.to_string()))?;
    if !readable(state, service, &subject).await {
        metrics::counter!("cufflink_sse_rejected_total", "reason" => "subject").increment(1);
        return Err(PlatformError::Forbidden(
            "This stream reads another service's events, which its manifest does not subscribe to"
                .into(),
        ));
    }
    let slot = state.sse_limiter.acquire(principal).ok_or_else(|| {
        metrics::counter!("cufflink_sse_rejected_total", "reason" => "cap").increment(1);
        PlatformError::TooManyRequests("Too many open streams for this principal".into())
    })?;
    let subscriber = state
        .nats
        .subscribe(subject)
        .await
        .map_err(|e| PlatformError::Internal(format!("Failed to subscribe to NATS: {e}")))?;
    Ok(Bridge { subscriber, slot })
}

async fn readable(state: &AppState, service: &RegisteredService, subject: &str) -> bool {
    let named = subject.split('.').nth(2).unwrap_or_default();
    named == service.name
        || state
            .registry
            .get_by_slug(&service.tenant_slug, named)
            .await
            .is_none()
        || service.manifest.subscriptions.iter().any(|subscription| {
            subject_covers(
                &resolve_subject(&subscription.subject, &service.tenant_slug),
                subject,
            )
        })
}

pub(crate) struct Reducer {
    pub state: Arc<AppState>,
    pub compiled: Arc<CompiledModule>,
    pub handler: String,
    pub service: Arc<RegisteredService>,
    pub context: ReducerContext,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ReducerContext {
    auth: Option<Value>,
    posture: Option<Value>,
    caller: Option<Value>,
}

impl ReducerContext {
    pub(crate) fn of(request: &Value) -> Self {
        let key = |key: &str| request.get(key).cloned();
        Self {
            auth: key("auth"),
            posture: key("posture"),
            caller: key("caller"),
        }
    }

    fn request(
        &self,
        service: &ServiceIdentity,
        handler: &str,
        state: &Value,
        message: Value,
    ) -> GuestRequest {
        GuestRequest {
            method: SseRouteDef::REDUCER_METHOD.to_string(),
            handler: handler.to_string(),
            headers: Some(Map::new()),
            body: json!({"state": state, "message": message}),
            body_raw_b64: Some(String::new()),
            tenant: service.tenant_slug.clone(),
            service: service.service_name.clone(),
            auth: self.auth.clone(),
            posture: self.posture.clone(),
            caller: self.caller.clone(),
            ..GuestRequest::default()
        }
    }
}

#[derive(Debug, PartialEq)]
struct Reduction {
    events: Vec<SseEvent>,
    state: Value,
    close: bool,
}

impl Reduction {
    fn read(output: GuestOutput) -> Result<Self, String> {
        let GuestOutput { text, mut hold } = output;
        let value = ChargedJson::parse(text.as_bytes(), &mut hold)
            .map_err(|e| e.to_string())?
            .value;
        drop(text);
        Self::of(value)
    }

    fn of(value: Value) -> Result<Self, String> {
        if value.get("__status").is_some() {
            return Err(format!("the reducer answered {}", value["__status"]));
        }
        Ok(Self {
            events: events(&value["events"])?,
            state: value["state"].clone(),
            close: value["close"].as_bool().unwrap_or(false),
        })
    }
}

impl Reducer {
    async fn reduce(
        &self,
        state: &Value,
        message: &async_nats::Message,
    ) -> Result<Reduction, String> {
        let identity = self.service.identity();
        let mut held = self
            .state
            .wasm_engine
            .as_ref()
            .map_or_else(Hold::none, |engine| {
                engine.hold(
                    &identity,
                    Class::Interactive,
                    &self.handler,
                    InvocationKind::SseReducer,
                )
            });
        let request = charged_guest_text(
            &self
                .context
                .request(&identity, &self.handler, state, message_json(message)),
            &mut held,
        )
        .map_err(|e| e.to_string())?;
        let output = execute(
            &self.state,
            Execution {
                service: &self.service,
                handler: &self.handler,
                compiled: self.compiled.clone(),
                request,
                kind: InvocationKind::SseReducer,
                budget: REDUCER_BUDGET,
                class: Class::Interactive,
                admission: None,
            },
        )
        .await
        .map_err(|e| e.to_string())?;
        drop(held);
        Reduction::read(output)
    }
}

fn message_json(message: &async_nats::Message) -> Value {
    let headers: Map<String, Value> = message
        .headers
        .iter()
        .flat_map(|headers| headers.iter())
        .filter_map(|(name, values)| {
            values
                .first()
                .map(|value| (name.to_string(), Value::String(value.to_string())))
        })
        .collect();
    json!({
        "subject": message.subject.as_str(),
        "data": payload(message),
        "headers": headers,
    })
}

fn payload(message: &async_nats::Message) -> Value {
    serde_json::from_slice(&message.payload)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&message.payload).into_owned()))
}

fn overflowed(buffered: usize) -> bool {
    buffered > MAX_BUFFERED
}

fn state_fits(state: &Value) -> bool {
    state.to_string().len() <= MAX_STATE_BYTES
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Closed {
    Client,
    Timeout,
    Reducer,
    Shutdown,
    Overflow,
    SubscriptionEnded,
}

impl Closed {
    fn label(self) -> &'static str {
        match self {
            Closed::Client => "client",
            Closed::Timeout => "timeout",
            Closed::Reducer => "reducer",
            Closed::Shutdown => "shutdown",
            Closed::Overflow => "overflow",
            Closed::SubscriptionEnded => "subscription_ended",
        }
    }
}

fn counted(result: &'static str) {
    metrics::counter!("cufflink_sse_bridge_events_total", "result" => result).increment(1);
}

struct Active;

impl Active {
    fn enter() -> Self {
        metrics::gauge!("cufflink_sse_bridge_streams_active").increment(1.0);
        Active
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        metrics::gauge!("cufflink_sse_bridge_streams_active").decrement(1.0);
    }
}

type Sender = mpsc::Sender<Result<Event, Infallible>>;

impl Bridge {
    pub(crate) fn stream(
        self,
        spec: SseSpec,
        reducer: Option<Reducer>,
        state: &AppState,
    ) -> Response {
        let Bridge {
            mut subscriber,
            slot,
        } = self;
        let buffered: Vec<async_nats::Message> =
            std::iter::from_fn(|| subscriber.next().now_or_never().flatten())
                .take(MAX_BUFFERED + 1)
                .collect();
        let keep_alive = spec.keep_alive;
        let (tx, rx) = mpsc::channel(32);
        let shutdown = state.shutdown.token();
        state.shutdown.spawn("sse_bridge", async move {
            let _slot = slot;
            let _active = Active::enter();
            let closed = run(subscriber, buffered, spec, reducer, &tx, shutdown).await;
            metrics::counter!("cufflink_sse_bridge_closed_total", "reason" => closed.label())
                .increment(1);
        });
        Sse::new(ReceiverStream::new(rx))
            .keep_alive(KeepAlive::new().interval(keep_alive))
            .into_response()
    }
}

async fn run(
    mut subscriber: async_nats::Subscriber,
    buffered: Vec<async_nats::Message>,
    spec: SseSpec,
    reducer: Option<Reducer>,
    tx: &Sender,
    shutdown: tokio_util::sync::CancellationToken,
) -> Closed {
    if !send_all(tx, &spec.initial).await {
        return Closed::Client;
    }
    if overflowed(buffered.len()) {
        tracing::warn!(
            buffered = buffered.len(),
            "Event stream closed: more messages arrived while its handler ran than can be buffered"
        );
        buffered.iter().for_each(|_| counted("dropped"));
        return Closed::Overflow;
    }
    let mut delivery = Delivery {
        state: spec.state,
        reducer,
    };
    for message in &buffered {
        if let Some(closed) = delivery.deliver(message, tx).await {
            return closed;
        }
    }
    let mut schedule = Schedule::new(
        Instant::now(),
        spec.timeout,
        spec.ping.as_ref().map(|(every, _)| *every),
    );
    loop {
        let (at, tick) = schedule.next();
        tokio::select! {
            _ = tx.closed() => return Closed::Client,
            _ = shutdown.cancelled() => return Closed::Shutdown,
            _ = tokio::time::sleep_until(at) => match (tick, &spec.ping) {
                (Tick::Ping, Some((_, event))) => {
                    if !send_all(tx, std::slice::from_ref(event)).await {
                        return Closed::Client;
                    }
                    schedule = schedule.pinged();
                }
                _ => return Closed::Timeout,
            },
            next = subscriber.next() => match next {
                Some(message) => {
                    if let Some(closed) = delivery.deliver(&message, tx).await {
                        return closed;
                    }
                }
                None => return Closed::SubscriptionEnded,
            },
        }
    }
}

async fn send_all(tx: &Sender, events: &[SseEvent]) -> bool {
    for event in events {
        if tx.send(Ok(event.render())).await.is_err() {
            return false;
        }
    }
    true
}

struct Delivery {
    state: Value,
    reducer: Option<Reducer>,
}

impl Delivery {
    async fn deliver(&mut self, message: &async_nats::Message, tx: &Sender) -> Option<Closed> {
        let Some(reducer) = &self.reducer else {
            counted("forwarded");
            let event = SseEvent::data_only(String::from_utf8_lossy(&message.payload).into_owned());
            return (!send_all(tx, &[event]).await).then_some(Closed::Client);
        };
        match reducer.reduce(&self.state, message).await {
            Ok(reduction) => {
                counted("reduced");
                if !send_all(tx, &reduction.events).await {
                    return Some(Closed::Client);
                }
                if !state_fits(&reduction.state) {
                    tracing::warn!(
                        handler = %reducer.handler,
                        "Event stream closed: the reducer's state is over {MAX_STATE_BYTES} bytes"
                    );
                    return Some(Closed::Reducer);
                }
                self.state = reduction.state;
                reduction.close.then_some(Closed::Reducer)
            }
            Err(error) => {
                counted("reducer_error");
                tracing::warn!(handler = %reducer.handler, %error, "Event-stream reducer failed; message dropped");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(raw: &str) -> Result<Reduction, String> {
        Reduction::read(GuestOutput {
            text: raw.to_string(),
            hold: Hold::none(),
        })
    }

    #[test]
    fn a_reduction_carries_the_principal_of_its_route() {
        let service = ServiceIdentity {
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            service_id: uuid::Uuid::nil(),
            service_name: "books".into(),
            schema: "books".into(),
        };
        let message = json!({"subject": "dw.acme.books.created", "data": {"id": 7}, "headers": {}});
        let route = json!({
            "method": "GET",
            "handler": "stream",
            "auth": {"sub": "u-1"},
            "posture": null,
            "caller": {"service": "pricing", "on_behalf_of": null, "depth": 1},
            "client_ip": "203.0.113.7",
        });
        assert_eq!(
            ReducerContext::of(&route)
                .request(&service, "reduce", &json!({"seen": 1}), message.clone())
                .to_value()
                .expect("request"),
            json!({
                "method": SseRouteDef::REDUCER_METHOD,
                "handler": "reduce",
                "headers": {},
                "body": {"state": {"seen": 1}, "message": message},
                "body_raw_b64": "",
                "tenant": "acme",
                "service": "books",
                "auth": {"sub": "u-1"},
                "posture": null,
                "caller": {"service": "pricing", "on_behalf_of": null, "depth": 1},
            })
        );
        assert_eq!(
            crate::router::guest_text(&ReducerContext::of(&route).request(
                &service,
                "reduce",
                &json!({"seen": 1}),
                message.clone()
            ))
            .expect("text"),
            r#"{"auth":{"sub":"u-1"},"body":{"message":{"data":{"id":7},"headers":{},"subject":"dw.acme.books.created"},"state":{"seen":1}},"body_raw_b64":"","caller":{"depth":1,"on_behalf_of":null,"service":"pricing"},"handler":"reduce","headers":{},"method":"SSE_EVENT","posture":null,"service":"books","tenant":"acme"}"#,
            "the bytes a reducer always read"
        );
        assert_eq!(
            ReducerContext::of(&json!({"method": "GET"}))
                .request(&service, "reduce", &Value::Null, Value::Null)
                .to_value()
                .expect("request"),
            json!({
                "method": SseRouteDef::REDUCER_METHOD,
                "handler": "reduce",
                "headers": {},
                "body": {"state": null, "message": null},
                "body_raw_b64": "",
                "tenant": "acme",
                "service": "books",
            })
        );
    }

    #[test]
    fn specs_parse_with_bounded_timings_and_safe_events() {
        let spec = SseSpec::parse(&json!({
            "initial": [{"data": {"type": "snapshot"}}, {"event": "note", "data": "text", "id": "1"}],
            "state": {"done": []},
            "keep_alive_secs": 15,
            "ping": {"interval_secs": 30, "event": {"data": {"type": "ping"}}},
            "timeout_secs": 300,
        }))
        .expect("spec");
        assert_eq!(
            spec.initial,
            vec![
                SseEvent::data_only(r#"{"type":"snapshot"}"#.into()),
                SseEvent {
                    event: Some("note".into()),
                    data: "text".into(),
                    id: Some("1".into()),
                },
            ]
        );
        assert_eq!(spec.timeout, Duration::from_secs(300));
        assert_eq!(
            spec.ping.map(|(every, _)| every),
            Some(Duration::from_secs(30))
        );
        for bad in [
            json!({"keep_alive_secs": 15, "timeout_secs": 0}),
            json!({"keep_alive_secs": 15, "timeout_secs": 3601}),
            json!({"keep_alive_secs": 0, "timeout_secs": 30}),
            json!({"keep_alive_secs": 15, "timeout_secs": 30, "ping": {"interval_secs": 0, "event": {"data": 1}}}),
            json!({"keep_alive_secs": 15, "timeout_secs": 30, "initial": [{"event": "a\nb", "data": 1}]}),
            json!({"keep_alive_secs": 15, "timeout_secs": 30, "initial": [{"id": "a\0", "data": 1}]}),
            json!({"keep_alive_secs": 15, "timeout_secs": 30, "initial": {"data": 1}}),
        ] {
            assert!(SseSpec::parse(&bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn reductions_come_from_the_reducers_json() {
        assert_eq!(
            parsed(
                r#"{"events":[{"data":{"type":"all_completed"}}],"state":{"n":2},"close":true}"#
            ),
            Ok(Reduction {
                events: vec![SseEvent::data_only(r#"{"type":"all_completed"}"#.into())],
                state: json!({"n": 2}),
                close: true,
            })
        );
        assert_eq!(
            parsed(r#"{"events":[],"state":null}"#).map(|r| r.close),
            Ok(false)
        );
        assert!(parsed(r#"{"__status":400,"__body":{"error":"x"}}"#).is_err());
        assert!(parsed("not json").is_err());
        assert!(parsed(r#"{"events":[{"event":"a\rb","data":1}]}"#).is_err());
    }

    #[test]
    fn the_buffer_and_the_reducer_state_are_bounded() {
        assert!(!overflowed(MAX_BUFFERED));
        assert!(overflowed(MAX_BUFFERED + 1));
        let sized = |len: usize| Value::String("x".repeat(len - 2));
        assert!(state_fits(&sized(MAX_STATE_BYTES)));
        assert!(!state_fits(&sized(MAX_STATE_BYTES + 1)));
    }

    #[test]
    fn the_schedule_pings_until_the_deadline_which_wins_a_tie() {
        let start = Instant::now();
        let secs = Duration::from_secs;
        let schedule = Schedule::new(start, secs(3), Some(secs(1)));
        assert_eq!(schedule.next(), (start + secs(1), Tick::Ping));
        let schedule = schedule.pinged();
        assert_eq!(schedule.next(), (start + secs(2), Tick::Ping));
        let schedule = schedule.pinged();
        assert_eq!(schedule.next(), (start + secs(3), Tick::Timeout));
        let quiet = Schedule::new(start, secs(300), None);
        assert_eq!(quiet.next(), (start + secs(300), Tick::Timeout));
    }

    fn message(payload: &str) -> async_nats::Message {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("receipt-id", "q1");
        async_nats::Message {
            subject: "dw.acme.receipts.pdf.progress.L7".into(),
            reply: None,
            payload: payload.to_string().into(),
            headers: Some(headers),
            status: None,
            description: None,
            length: payload.len(),
        }
    }

    #[test]
    fn reducers_see_json_payloads_parsed_and_others_as_text() {
        assert_eq!(
            message_json(&message(r#"{"receipt_id":"q1","status":"completed"}"#)),
            json!({
                "subject": "dw.acme.receipts.pdf.progress.L7",
                "data": {"receipt_id": "q1", "status": "completed"},
                "headers": {"receipt-id": "q1"},
            })
        );
        assert_eq!(
            message_json(&message("plain")),
            json!({
                "subject": "dw.acme.receipts.pdf.progress.L7",
                "data": "plain",
                "headers": {"receipt-id": "q1"},
            })
        );
    }
}
