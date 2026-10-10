//! Answering one call, the same way over every transport.

use crate::Extension;
use cufflink_types::ext::{ExtReply, ExtRequest, Ready, PROTOCOL_VERSION};
use cufflink_types::host::ext::CallErrorKind;
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;

pub(crate) const PANIC: &str = "panic";
pub(crate) const UNSUPPORTED_PROTOCOL: &str = "unsupported_protocol";

/// The extensions of a service, and the calls they may run at once.
pub(crate) struct Dispatcher {
    extensions: BTreeMap<String, Arc<dyn Extension>>,
    permits: Arc<Semaphore>,
}

/// The call `bytes` carry, or the reply refusing them.
pub(crate) fn decode(bytes: &[u8]) -> Result<ExtRequest, ExtReply> {
    serde_json::from_slice(bytes).map_err(|e| {
        tracing::warn!("a malformed call is refused");
        ExtReply::failure(CallErrorKind::Invalid.as_str(), format!("not a call: {e}"))
    })
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    payload
        .downcast::<String>()
        .map(|message| *message)
        .or_else(|payload| {
            payload
                .downcast::<&str>()
                .map(|message| message.to_string())
        })
        .unwrap_or_else(|_| "the extension panicked".into())
}

impl Dispatcher {
    pub(crate) fn new(
        extensions: BTreeMap<String, Arc<dyn Extension>>,
        concurrency: usize,
    ) -> Self {
        Self {
            extensions,
            permits: Arc::new(Semaphore::new(concurrency)),
        }
    }

    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.extensions.keys().map(String::as_str)
    }

    pub(crate) fn ready(&self) -> Ready {
        Ready {
            protocol: PROTOCOL_VERSION,
            extensions: self.names().map(str::to_string).collect(),
        }
    }

    /// Answer `request` by `deadline`, which its transport reads from the
    /// call as it arrives: the extension runs on the blocking pool once one
    /// of the service's call slots is free, is never started after the
    /// deadline, and is no longer waited for at it.
    pub(crate) async fn dispatch(&self, request: ExtRequest, deadline: Instant) -> ExtReply {
        let (extension, call_id, started) =
            (request.extension.clone(), request.call_id, Instant::now());
        let reply = self.answer(request, deadline).await;
        match &reply {
            ExtReply::Ok(_) => {
                tracing::debug!(%extension, %call_id, elapsed = ?started.elapsed(), "call answered")
            }
            ExtReply::Error(failure) if failure.code == PANIC => {
                tracing::warn!(%extension, %call_id, "the extension panicked")
            }
            ExtReply::Error(failure) => tracing::debug!(
                %extension,
                %call_id,
                code = %failure.code,
                elapsed = ?started.elapsed(),
                "call failed"
            ),
        }
        reply
    }

    async fn answer(&self, request: ExtRequest, deadline: Instant) -> ExtReply {
        if request.v != PROTOCOL_VERSION {
            return ExtReply::failure(
                UNSUPPORTED_PROTOCOL,
                format!(
                    "protocol {} is not {PROTOCOL_VERSION}, the one this service speaks",
                    request.v
                ),
            );
        }
        let Some(extension) = self.extensions.get(&request.extension).map(Arc::clone) else {
            return ExtReply::failure(
                CallErrorKind::NotFound.as_str(),
                format!("'{}' is not served here", request.extension),
            );
        };
        let permits = Arc::clone(&self.permits);
        let call = async move {
            let permit = permits.acquire_owned().await;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                (Instant::now() < deadline)
                    .then(|| extension.call(&request.context, request.payload))
            })
            .await
        };
        match tokio::time::timeout_at(deadline.into(), call).await {
            Ok(Ok(Some(answered))) => {
                answered.map_or_else(|e| ExtReply::Error(e.into()), ExtReply::Ok)
            }
            Ok(Ok(None)) | Err(_) => ExtReply::failure(
                CallErrorKind::Timeout.as_str(),
                "the caller's deadline passed",
            ),
            Ok(Err(joined)) => match joined.try_into_panic() {
                Ok(payload) => ExtReply::failure(PANIC, panic_message(payload)),
                Err(_) => ExtReply::failure(
                    CallErrorKind::Unavailable.as_str(),
                    "the service is shutting down",
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deadline::tests::{context_until, in_ms};
    use crate::deadline::CallDeadline;
    use crate::{CallContext, ExtError, Result};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use uuid::Uuid;

    fn request(extension: &str, payload: Value, deadline_ms: u64) -> ExtRequest {
        ExtRequest {
            v: PROTOCOL_VERSION,
            call_id: Uuid::new_v4(),
            extension: extension.into(),
            context: context_until(deadline_ms),
            payload,
        }
    }

    fn echo(ctx: &CallContext, payload: Value) -> Result<Value> {
        Ok(json!({"tenant": ctx.tenant, "payload": payload}))
    }

    fn fail(_: &CallContext, payload: Value) -> Result<Value> {
        let code = payload["code"].as_str().ok_or("code is required")?;
        Err(ExtError::new(code, "failed as asked"))
    }

    fn sleep(_: &CallContext, payload: Value) -> Result<Value> {
        std::thread::sleep(Duration::from_millis(payload["ms"].as_u64().unwrap_or(0)));
        Ok(payload)
    }

    fn panics(_: &CallContext, payload: Value) -> Result<Value> {
        match payload.as_str() {
            Some(message) => panic!("{message}"),
            None => std::panic::panic_any(7),
        }
    }

    fn dispatcher(concurrency: usize) -> Dispatcher {
        let extensions: [(&str, Arc<dyn Extension>); 4] = [
            ("test.echo", Arc::new(echo)),
            ("test.fail", Arc::new(fail)),
            ("test.sleep", Arc::new(sleep)),
            ("test.panic", Arc::new(panics)),
        ];
        Dispatcher::new(
            extensions
                .into_iter()
                .map(|(name, extension)| (name.to_string(), extension))
                .collect(),
            concurrency,
        )
    }

    async fn dispatch(dispatcher: &Dispatcher, request: ExtRequest) -> ExtReply {
        let deadline = request.context.deadline();
        dispatcher.dispatch(request, deadline).await
    }

    fn code(reply: &ExtReply) -> &str {
        match reply {
            ExtReply::Error(failure) => &failure.code,
            ExtReply::Ok(value) => panic!("answered {value}"),
        }
    }

    #[tokio::test]
    async fn a_call_is_answered_by_its_extension() {
        let reply = dispatch(
            &dispatcher(4),
            request("test.echo", json!({"n": 1}), in_ms(5_000)),
        )
        .await;
        assert_eq!(
            reply,
            ExtReply::Ok(json!({"tenant": "harbor", "payload": {"n": 1}}))
        );
    }

    #[tokio::test]
    async fn an_unknown_extension_is_not_found() {
        let reply = dispatch(
            &dispatcher(4),
            request("test.gone", json!(null), in_ms(5_000)),
        )
        .await;
        assert_eq!(
            reply,
            ExtReply::failure(
                CallErrorKind::NotFound.as_str(),
                "'test.gone' is not served here"
            )
        );
    }

    #[tokio::test]
    async fn an_error_answers_its_code_and_message() {
        let d = dispatcher(4);
        assert_eq!(
            dispatch(
                &d,
                request("test.fail", json!({"code": "quota"}), in_ms(5_000))
            )
            .await,
            ExtReply::failure("quota", "failed as asked")
        );
        assert_eq!(
            dispatch(&d, request("test.fail", json!({}), in_ms(5_000))).await,
            ExtReply::failure("failed", "code is required")
        );
    }

    #[tokio::test]
    async fn a_panic_answers_panic_with_its_message() {
        let d = dispatcher(4);
        assert_eq!(
            dispatch(
                &d,
                request("test.panic", json!("token store gone"), in_ms(5_000))
            )
            .await,
            ExtReply::failure(PANIC, "token store gone")
        );
        assert_eq!(
            dispatch(&d, request("test.panic", json!(null), in_ms(5_000))).await,
            ExtReply::failure(PANIC, "the extension panicked")
        );
        assert_eq!(
            dispatch(&d, request("test.echo", json!(1), in_ms(5_000))).await,
            ExtReply::Ok(json!({"tenant": "harbor", "payload": 1})),
            "a panic leaves the service serving"
        );
    }

    #[tokio::test]
    async fn a_passed_deadline_is_a_timeout_without_a_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        let counting = move |_: &CallContext, _: Value| -> Result<Value> {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(Value::Null)
        };
        let d = Dispatcher::new(
            [(
                "test.count".to_string(),
                Arc::new(counting) as Arc<dyn Extension>,
            )]
            .into(),
            4,
        );
        for deadline_ms in [0, in_ms(0) - 1_000] {
            assert_eq!(
                code(&dispatch(&d, request("test.count", json!(null), deadline_ms)).await),
                CallErrorKind::Timeout.as_str()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_call_is_given_up_at_its_deadline() {
        let started = Instant::now();
        let reply = dispatch(
            &dispatcher(4),
            request("test.sleep", json!({"ms": 1_000}), in_ms(200)),
        )
        .await;
        assert_eq!(code(&reply), CallErrorKind::Timeout.as_str());
        assert!(
            started.elapsed() < Duration::from_millis(800),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn calls_beyond_the_concurrency_wait_for_a_slot_until_their_deadline() {
        let d = Arc::new(dispatcher(1));
        let busy = tokio::spawn({
            let d = Arc::clone(&d);
            async move { dispatch(&d, request("test.sleep", json!({"ms": 600}), in_ms(5_000))).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let waiting = dispatch(&d, request("test.echo", json!(1), in_ms(200))).await;
        assert_eq!(code(&waiting), CallErrorKind::Timeout.as_str());
        let queued = dispatch(&d, request("test.echo", json!(2), in_ms(5_000))).await;
        assert_eq!(
            queued,
            ExtReply::Ok(json!({"tenant": "harbor", "payload": 2}))
        );
        assert_eq!(
            busy.await.expect("busy call"),
            ExtReply::Ok(json!({"ms": 600}))
        );
    }

    #[tokio::test]
    async fn another_protocol_is_refused() {
        let reply = dispatch(
            &dispatcher(4),
            ExtRequest {
                v: PROTOCOL_VERSION + 1,
                ..request("test.echo", json!(null), in_ms(5_000))
            },
        )
        .await;
        assert_eq!(
            reply,
            ExtReply::failure(
                UNSUPPORTED_PROTOCOL,
                "protocol 2 is not 1, the one this service speaks"
            )
        );
    }

    #[test]
    fn a_malformed_call_is_invalid() {
        for bytes in [&b"not json"[..], br#"{"v": 1}"#] {
            assert_eq!(
                code(&decode(bytes).expect_err("malformed")),
                CallErrorKind::Invalid.as_str()
            );
        }
    }

    #[test]
    fn ready_lists_the_extensions_in_order() {
        assert_eq!(
            dispatcher(1).ready(),
            Ready {
                protocol: PROTOCOL_VERSION,
                extensions: vec![
                    "test.echo".into(),
                    "test.fail".into(),
                    "test.panic".into(),
                    "test.sleep".into()
                ],
            }
        );
    }
}
