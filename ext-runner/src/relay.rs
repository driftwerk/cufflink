use crate::socket::{exchange, Answer, Endpoint, ExchangeError};
use async_nats::{Client, Message, Subscriber};
use bytes::Bytes;
use cufflink_native::CallDeadline;
use cufflink_types::ext::{call_subject, ExtReply, ExtRequest, QUEUE_GROUP};
use cufflink_types::host::ext::CallErrorKind;
use eyre::WrapErr;
use futures_util::{FutureExt, StreamExt};
use hyper::Method;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const LEAVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Failed,
    Unavailable,
    Timeout,
    TooLarge,
    Invalid,
    Late,
    NoReply,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Unavailable => "unavailable",
            Self::Timeout => "timeout",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
            Self::Late => "late",
            Self::NoReply => "no_reply",
        }
    }

    fn of(reply: &ExtReply) -> Self {
        match reply {
            ExtReply::Ok(_) => Self::Ok,
            ExtReply::Error(failure) => match failure.kind() {
                CallErrorKind::Unavailable => Self::Unavailable,
                CallErrorKind::Timeout => Self::Timeout,
                CallErrorKind::TooLarge => Self::TooLarge,
                CallErrorKind::NotDeclared
                | CallErrorKind::NotFound
                | CallErrorKind::Denied
                | CallErrorKind::Failed
                | CallErrorKind::Invalid => Self::Failed,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Relayed {
    Answered { body: Bytes, reply: ExtReply },
    TooLarge(String),
    Unreachable(String),
}

fn encoded(reply: &ExtReply) -> Bytes {
    serde_json::to_vec(reply)
        .expect("a reply always serializes")
        .into()
}

pub fn admit(tenant: &str, subject: &str, payload: &[u8]) -> Result<Instant, ExtReply> {
    let request: ExtRequest = serde_json::from_slice(payload).map_err(|e| {
        ExtReply::failure(CallErrorKind::Invalid.as_str(), format!("not a call: {e}"))
    })?;
    match request.matches(tenant, subject) {
        true => Ok(request.context.deadline().into()),
        false => Err(ExtReply::failure(
            CallErrorKind::Invalid.as_str(),
            "the call does not match the subject it came on",
        )),
    }
}

pub fn relayed(answer: Result<Answer, ExchangeError>) -> Relayed {
    match answer {
        Ok(Answer { status, body }) => match serde_json::from_slice(&body) {
            Ok(reply) => Relayed::Answered { body, reply },
            Err(e) => Relayed::Unreachable(format!(
                "the native service answered {status} with no reply: {e}"
            )),
        },
        Err(ExchangeError::TooLarge(limit)) => Relayed::TooLarge(format!(
            "the native service's reply is larger than {limit} bytes"
        )),
        Err(ExchangeError::Failed(e)) => {
            Relayed::Unreachable(format!("the native service did not answer: {e}"))
        }
    }
}

pub fn settle(
    relayed: Option<Relayed>,
    deadline: Instant,
    now: Instant,
) -> (Outcome, Option<Bytes>) {
    let Some(relayed) = relayed.filter(|_| now < deadline) else {
        return (Outcome::Late, None);
    };
    match relayed {
        Relayed::Answered { body, reply } => (Outcome::of(&reply), Some(body)),
        Relayed::TooLarge(message) => (
            Outcome::TooLarge,
            Some(encoded(&ExtReply::failure(
                CallErrorKind::TooLarge.as_str(),
                message,
            ))),
        ),
        Relayed::Unreachable(message) => (
            Outcome::Unavailable,
            Some(encoded(&ExtReply::failure(
                CallErrorKind::Unavailable.as_str(),
                message,
            ))),
        ),
    }
}

pub struct Target {
    endpoint: Endpoint,
    calls: TaskTracker,
}

impl Target {
    pub fn new(endpoint: Endpoint) -> Arc<Self> {
        Arc::new(Self {
            endpoint,
            calls: TaskTracker::new(),
        })
    }

    pub fn socket(&self) -> &Path {
        self.endpoint.socket()
    }

    pub async fn settled(&self) {
        self.calls.close();
        self.calls.wait().await;
    }
}

#[derive(Clone)]
pub struct Destination(Arc<Mutex<Arc<Target>>>);

impl Destination {
    pub fn new(target: Arc<Target>) -> Self {
        Self(Arc::new(Mutex::new(target)))
    }

    fn target(&self) -> MutexGuard<'_, Arc<Target>> {
        cufflink_supervisor::locked(&self.0)
    }

    pub fn current(&self) -> Arc<Target> {
        self.target().clone()
    }

    pub fn retarget(&self, target: Arc<Target>) -> Arc<Target> {
        std::mem::replace(&mut *self.target(), target)
    }

    fn relay<F>(&self, call: impl FnOnce(Endpoint) -> F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let target = self.target();
        target.calls.spawn(call(target.endpoint.clone()));
    }
}

#[derive(Clone)]
pub struct Relay {
    client: Client,
}

impl Relay {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn subscribe(
        &self,
        tenant: &str,
        extensions: impl IntoIterator<Item = String>,
        deadline: Instant,
    ) -> eyre::Result<Vec<(String, Subscriber)>> {
        let subscribed = async {
            let subscribed = extensions.into_iter().map(|name| async move {
                self.client
                    .queue_subscribe(call_subject(tenant, &name), QUEUE_GROUP.into())
                    .await
                    .map(|calls| (name, calls))
            });
            let calls = futures_util::future::try_join_all(subscribed)
                .await
                .wrap_err("cannot subscribe to the calls")?;
            self.client
                .flush()
                .await
                .wrap_err("cannot send the subscriptions to NATS")?;
            Ok(calls)
        };
        tokio::time::timeout_at(deadline, subscribed)
            .await
            .wrap_err("NATS did not take the subscriptions in time")?
    }

    pub async fn run(
        self,
        mut calls: Subscriber,
        tenant: Arc<str>,
        destination: Destination,
        permits: Arc<Semaphore>,
        stop: CancellationToken,
    ) {
        let relay = |message: Message, permit| {
            let (client, tenant) = (self.client.clone(), tenant.clone());
            destination.relay(|endpoint| async move {
                let _permit = permit;
                counted(answer(&client, &tenant, message, |payload| {
                    forward(&client, &endpoint, payload)
                }))
                .await;
            });
        };
        let held = loop {
            let message = tokio::select! {
                () = stop.cancelled() => break None,
                message = calls.next() => match message {
                    Some(message) => message,
                    None => break None,
                },
            };
            let permit = tokio::select! {
                () = stop.cancelled() => break Some(message),
                permit = permits.clone().acquire_owned() => permit,
            };
            match permit {
                Ok(permit) => relay(message, permit),
                Err(_) => break Some(message),
            }
        };
        if let Err(e) = calls.drain().await {
            tracing::warn!(%tenant, error = %e, "Cannot unsubscribe from the calls");
        }
        let mut taken = futures_util::stream::iter(held).chain(calls);
        let until = Instant::now() + LEAVE_TIMEOUT;
        let stranded = loop {
            let message = match tokio::time::timeout_at(until, taken.next()).await {
                Ok(Some(message)) => message,
                Ok(None) => break None,
                Err(_) => break Some(None),
            };
            match tokio::time::timeout_at(until, permits.clone().acquire_owned()).await {
                Ok(Ok(permit)) => relay(message, permit),
                Ok(Err(_)) | Err(_) => break Some(Some(message)),
            }
        };
        let Some(held) = stranded else {
            return;
        };
        let left: Vec<Message> = held
            .into_iter()
            .chain(std::iter::from_fn(|| taken.next().now_or_never().flatten()))
            .collect();
        tracing::warn!(%tenant, calls = left.len(), "The calls still waiting to be relayed are answered unavailable");
        for message in left {
            counted(answer(&self.client, &tenant, message, |_| async {
                Relayed::Unreachable("the runner stopped relaying the service's calls".into())
            }))
            .await;
        }
    }
}

async fn forward(client: &Client, endpoint: &Endpoint, payload: Bytes) -> Relayed {
    relayed(
        exchange(
            endpoint,
            Method::POST,
            "/call",
            payload,
            client.max_payload(),
        )
        .await,
    )
}

async fn counted(outcome: impl Future<Output = Outcome>) {
    let started = std::time::Instant::now();
    let outcome = outcome.await;
    metrics::counter!("cufflink_ext_runner_calls_total", "result" => outcome.as_str()).increment(1);
    metrics::histogram!("cufflink_ext_runner_call_duration_seconds")
        .record(started.elapsed().as_secs_f64());
}

async fn answer<F>(
    client: &Client,
    tenant: &str,
    message: Message,
    relay: impl FnOnce(Bytes) -> F,
) -> Outcome
where
    F: Future<Output = Relayed>,
{
    let Some(reply_to) = message.reply else {
        tracing::warn!(subject = %message.subject, "A call without a reply subject is dropped");
        return Outcome::NoReply;
    };
    let (outcome, reply) = match admit(tenant, &message.subject, &message.payload) {
        Err(refused) => {
            tracing::warn!(subject = %message.subject, "A malformed call or one for another subject is refused");
            (Outcome::Invalid, Some(encoded(&refused)))
        }
        Ok(deadline) => {
            let relayed = tokio::time::timeout_at(deadline, relay(message.payload))
                .await
                .ok();
            settle(relayed, deadline, Instant::now())
        }
    };
    if let Some(reply) = reply {
        if let Err(e) = client.publish(reply_to, reply).await {
            tracing::warn!(error = %e, "Cannot send a reply");
        }
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use cufflink_types::ext::{CallContext, CallerService, PROTOCOL_VERSION};
    use hyper::StatusCode;
    use serde_json::{json, Value};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn in_ms(ms: u64) -> u64 {
        u64::try_from(
            (SystemTime::now() + Duration::from_millis(ms))
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis(),
        )
        .expect("millis")
    }

    fn request(tenant: &str, extension: &str) -> Vec<u8> {
        request_until(tenant, extension, in_ms(5_000))
    }

    fn request_until(tenant: &str, extension: &str, deadline_ms: u64) -> Vec<u8> {
        serde_json::to_vec(&ExtRequest {
            v: PROTOCOL_VERSION,
            call_id: uuid::Uuid::new_v4(),
            extension: extension.into(),
            context: CallContext {
                tenant: tenant.into(),
                service: CallerService {
                    name: "reports".into(),
                    version: 2,
                },
                auth: None,
                deadline_ms,
            },
            payload: json!({"n": 1}),
        })
        .expect("request")
    }

    fn code(reply: &[u8]) -> Value {
        serde_json::from_slice::<Value>(reply).expect("a reply")["error"]["code"].clone()
    }

    fn refused(reply: Result<Instant, ExtReply>) -> String {
        match reply {
            Err(ExtReply::Error(failure)) => failure.code,
            other => panic!("admitted: {other:?}"),
        }
    }

    #[test]
    fn a_call_is_admitted_until_its_deadline() {
        let deadline = admit(
            "acme",
            "cufflink.ext.acme.echo.reply",
            &request("acme", "echo.reply"),
        )
        .expect("admitted");
        let left = deadline - Instant::now();
        assert!(
            left > Duration::from_secs(4) && left <= Duration::from_secs(5),
            "{left:?}"
        );
    }

    #[test]
    fn a_malformed_call_or_one_on_another_subject_is_invalid() {
        assert_eq!(
            refused(admit("acme", "cufflink.ext.acme.echo.reply", b"{")),
            "invalid"
        );
        for (tenant, subject, body) in [
            (
                "acme",
                "cufflink.ext.acme.echo.fail",
                request("acme", "echo.reply"),
            ),
            (
                "acme",
                "cufflink.ext.acme.echo.reply",
                request("globex", "echo.reply"),
            ),
            (
                "globex",
                "cufflink.ext.acme.echo.reply",
                request("acme", "echo.reply"),
            ),
        ] {
            assert_eq!(
                refused(admit(tenant, subject, &body)),
                "invalid",
                "{subject}"
            );
        }
    }

    #[test]
    fn a_childs_answer_is_its_reply_or_why_there_is_none() {
        let answer = |status: StatusCode, body: &str| {
            relayed(Ok(Answer {
                status,
                body: Bytes::from(body.to_string()),
            }))
        };
        assert_eq!(
            answer(StatusCode::OK, r#"{"ok":{"n":1}}"#),
            Relayed::Answered {
                body: Bytes::from(r#"{"ok":{"n":1}}"#),
                reply: ExtReply::Ok(json!({"n": 1}))
            }
        );
        assert!(matches!(
            answer(
                StatusCode::BAD_REQUEST,
                r#"{"error":{"code":"invalid","message":"m"}}"#
            ),
            Relayed::Answered {
                reply: ExtReply::Error(_),
                ..
            }
        ));
        assert!(matches!(
            answer(StatusCode::NOT_FOUND, ""),
            Relayed::Unreachable(message) if message.starts_with("the native service answered 404 Not Found with no reply")
        ));
        assert!(matches!(
            relayed(Err(ExchangeError::TooLarge(8))),
            Relayed::TooLarge(message) if message == "the native service's reply is larger than 8 bytes"
        ));
        assert!(matches!(
            relayed(Err(ExchangeError::Failed("connection refused".into()))),
            Relayed::Unreachable(message) if message == "the native service did not answer: connection refused"
        ));
    }

    #[test]
    fn a_settled_call_answers_before_its_deadline_and_never_after() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(1);
        let ok = Relayed::Answered {
            body: Bytes::from(r#"{"ok":1}"#),
            reply: ExtReply::Ok(json!(1)),
        };
        let failed = Relayed::Answered {
            body: Bytes::from(r#"{"error":{"code":"quota","message":"m"}}"#),
            reply: ExtReply::failure("quota", "m"),
        };
        assert_eq!(
            settle(Some(ok.clone()), deadline, now),
            (Outcome::Ok, Some(Bytes::from(r#"{"ok":1}"#)))
        );
        assert_eq!(settle(Some(failed), deadline, now).0, Outcome::Failed);
        for (code, outcome) in [
            ("unavailable", Outcome::Unavailable),
            ("timeout", Outcome::Timeout),
            ("too_large", Outcome::TooLarge),
            ("invalid", Outcome::Failed),
            ("not_found", Outcome::Failed),
        ] {
            let reply = ExtReply::failure(code, "m");
            let answered = Relayed::Answered {
                body: encoded(&reply),
                reply,
            };
            assert_eq!(settle(Some(answered), deadline, now).0, outcome, "{code}");
        }
        let (outcome, reply) = settle(Some(Relayed::Unreachable("gone".into())), deadline, now);
        assert_eq!(
            (outcome, code(&reply.expect("a reply"))),
            (Outcome::Unavailable, json!("unavailable"))
        );
        let (outcome, reply) = settle(Some(Relayed::TooLarge("big".into())), deadline, now);
        assert_eq!(
            (outcome, code(&reply.expect("a reply"))),
            (Outcome::TooLarge, json!("too_large"))
        );
        assert_eq!(settle(None, deadline, now), (Outcome::Late, None));
        assert_eq!(
            settle(Some(ok), deadline, deadline),
            (Outcome::Late, None),
            "an answer at the deadline is not sent"
        );
        assert_eq!(
            settle(
                Some(Relayed::Unreachable("gone".into())),
                deadline,
                deadline
            ),
            (Outcome::Late, None)
        );
    }

    #[tokio::test]
    async fn a_retired_target_settles_once_its_calls_are_answered() {
        let (first, second) = (
            Target::new(Endpoint::new(Path::new("/a.sock"), "a")),
            Target::new(Endpoint::new(Path::new("/b.sock"), "b")),
        );
        let destination = Destination::new(first.clone());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = |endpoint: Endpoint| {
            let seen = seen.clone();
            move || {
                seen.lock()
                    .expect("seen")
                    .push(endpoint.socket().to_path_buf())
            }
        };
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        destination.relay(|socket| {
            let answered = record(socket);
            async move {
                let _ = released.await;
                answered();
            }
        });
        let retired = destination.retarget(second.clone());
        assert!(Arc::ptr_eq(&retired, &first));
        assert!(Arc::ptr_eq(&destination.current(), &second));
        destination.relay(|socket| {
            let answered = record(socket);
            async move { answered() }
        });
        second.settled().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), retired.settled())
                .await
                .is_err(),
            "a call in flight to the retired child holds it"
        );
        release.send(()).expect("still waiting");
        retired.settled().await;
        assert_eq!(
            *seen.lock().expect("seen"),
            [Path::new("/b.sock"), Path::new("/a.sock")]
        );
    }

    #[test]
    fn outcomes_are_labelled() {
        assert_eq!(
            [
                Outcome::Ok,
                Outcome::Failed,
                Outcome::Unavailable,
                Outcome::Timeout,
                Outcome::TooLarge,
                Outcome::Invalid,
                Outcome::Late,
                Outcome::NoReply
            ]
            .map(Outcome::as_str),
            [
                "ok",
                "failed",
                "unavailable",
                "timeout",
                "too_large",
                "invalid",
                "late",
                "no_reply"
            ]
        );
    }

    async fn nats() -> Option<Client> {
        let url = cufflink_testkit::infra().await?.require_nats()?;
        Some(async_nats::connect(url).await.expect("the test NATS"))
    }

    fn tenant() -> String {
        format!("r-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
    }

    fn child() -> (tempfile::TempDir, Endpoint) {
        let dir = tempfile::Builder::new()
            .prefix("cxr")
            .tempdir_in("/tmp")
            .expect("socket dir");
        let socket = dir.path().join("c.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let app = axum::Router::new().route(
            "/call",
            axum::routing::post(|| async { r#"{"ok":"answered"}"# }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await });
        (dir, Endpoint::new(&socket, "t0ken"))
    }

    async fn relaying(
        client: &Client,
        tenant: &str,
        names: &[&str],
        endpoint: Endpoint,
        permits: Arc<Semaphore>,
    ) -> (CancellationToken, Vec<tokio::task::JoinHandle<()>>) {
        let relay = Relay::new(client.clone());
        let subscriptions = relay
            .subscribe(
                tenant,
                names.iter().map(|name| name.to_string()),
                Instant::now() + Duration::from_secs(10),
            )
            .await
            .expect("subscribed");
        let (stop, destination) = (
            CancellationToken::new(),
            Destination::new(Target::new(endpoint)),
        );
        let relays = subscriptions
            .into_iter()
            .map(|(_, calls)| {
                tokio::spawn(relay.clone().run(
                    calls,
                    Arc::from(tenant),
                    destination.clone(),
                    permits.clone(),
                    stop.clone(),
                ))
            })
            .collect();
        (stop, relays)
    }

    async fn ask(client: Client, tenant: String, extension: &'static str) -> Value {
        let reply = client
            .send_request(
                call_subject(&tenant, extension),
                async_nats::Request::new()
                    .payload(request_until(&tenant, extension, in_ms(30_000)).into())
                    .timeout(Some(Duration::from_secs(20))),
            )
            .await
            .expect("a reply");
        serde_json::from_slice(&reply.payload).expect("a reply")
    }

    #[tokio::test]
    async fn a_subscription_waiting_for_its_next_call_holds_no_permit() {
        let Some(client) = nats().await else {
            return;
        };
        let (tenant, (_dir, endpoint)) = (tenant(), child());
        let names = ["ext.one", "ext.two", "ext.three"];
        let (stop, relays) = relaying(
            &client,
            &tenant,
            &names,
            endpoint,
            Arc::new(Semaphore::new(1)),
        )
        .await;
        for name in names {
            assert_eq!(
                ask(client.clone(), tenant.clone(), name).await,
                json!({"ok": "answered"}),
                "{name}"
            );
        }
        stop.cancel();
        for relay in relays {
            relay.await.expect("the relay ends");
        }
    }

    #[tokio::test]
    async fn a_call_still_waiting_for_a_permit_when_the_relay_stops_is_answered_unavailable() {
        let Some(client) = nats().await else {
            return;
        };
        let (tenant, (_dir, endpoint)) = (tenant(), child());
        let (stop, relays) = relaying(
            &client,
            &tenant,
            &["ext.one"],
            endpoint,
            Arc::new(Semaphore::new(0)),
        )
        .await;
        let asked = tokio::spawn(ask(client.clone(), tenant.clone(), "ext.one"));
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.cancel();
        let answer = asked.await.expect("asked");
        assert_eq!(answer["error"]["code"], "unavailable", "{answer}");
        for relay in relays {
            relay.await.expect("the relay ends");
        }
    }
}
