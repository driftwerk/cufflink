use crate::socket::{exchange, Answer, Endpoint};
use bytes::Bytes;
use cufflink_types::ext::{Ready, PROTOCOL_VERSION};
use hyper::{Method, StatusCode};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

const READY_MAX_BYTES: usize = 64 * 1024;
const POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Mismatch {
    #[error("it speaks protocol {0}, not {PROTOCOL_VERSION}")]
    Protocol(u32),
    #[error("it serves {served:?}, but its manifest provides {provided:?}")]
    Extensions {
        served: BTreeSet<String>,
        provided: BTreeSet<String>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum NotReady {
    #[error("{0}")]
    Unanswered(String),
    #[error(transparent)]
    Mismatch(#[from] Mismatch),
}

pub fn check(ready: &Ready, provided: &BTreeSet<String>) -> Result<(), Mismatch> {
    let served: BTreeSet<String> = ready.extensions.iter().cloned().collect();
    match (ready.protocol == PROTOCOL_VERSION, served == *provided) {
        (false, _) => Err(Mismatch::Protocol(ready.protocol)),
        (true, false) => Err(Mismatch::Extensions {
            served,
            provided: provided.clone(),
        }),
        (true, true) => Ok(()),
    }
}

fn readiness(answer: Answer, provided: &BTreeSet<String>) -> Result<(), NotReady> {
    match answer.status {
        StatusCode::OK => serde_json::from_slice::<Ready>(&answer.body)
            .map_err(|e| NotReady::Unanswered(format!("its readiness does not parse: {e}")))
            .and_then(|ready| Ok(check(&ready, provided)?)),
        status => Err(NotReady::Unanswered(format!(
            "it answered ready with {status}"
        ))),
    }
}

pub async fn probe(endpoint: &Endpoint, provided: &BTreeSet<String>) -> Result<(), NotReady> {
    exchange(
        endpoint,
        Method::GET,
        "/ready",
        Bytes::new(),
        READY_MAX_BYTES,
    )
    .await
    .map_err(|e| NotReady::Unanswered(e.to_string()))
    .and_then(|answer| readiness(answer, provided))
}

pub async fn until_ready(
    endpoint: &Endpoint,
    provided: &BTreeSet<String>,
    timeout: Duration,
) -> eyre::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last = String::from("it never answered");
    while Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, probe(endpoint, provided)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(NotReady::Mismatch(mismatch))) => return Err(mismatch.into()),
            Ok(Err(NotReady::Unanswered(why))) => last = why,
            Err(_) => break,
        }
        tokio::time::sleep_until(deadline.min(Instant::now() + POLL)).await;
    }
    Err(eyre::eyre!(
        "it did not answer ready within {timeout:?}: {last}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn ready(protocol: u32, extensions: &[&str]) -> Ready {
        Ready {
            protocol,
            extensions: extensions.iter().map(|name| name.to_string()).collect(),
        }
    }

    #[test]
    fn a_child_is_ready_serving_what_it_provides_in_any_order() {
        let provided = names(&["echo.fail", "echo.reply"]);
        assert_eq!(
            check(
                &ready(PROTOCOL_VERSION, &["echo.reply", "echo.fail"]),
                &provided
            ),
            Ok(())
        );
    }

    #[test]
    fn another_protocol_or_other_extensions_are_a_mismatch() {
        let provided = names(&["echo.fail", "echo.reply"]);
        assert_eq!(
            check(
                &ready(PROTOCOL_VERSION + 1, &["echo.fail", "echo.reply"]),
                &provided
            ),
            Err(Mismatch::Protocol(PROTOCOL_VERSION + 1))
        );
        for served in [
            &["echo.reply"][..],
            &["echo.fail", "echo.reply", "echo.sleep"],
            &[],
        ] {
            assert_eq!(
                check(&ready(PROTOCOL_VERSION, served), &provided),
                Err(Mismatch::Extensions {
                    served: names(served),
                    provided: provided.clone()
                })
            );
        }
        assert_eq!(
            Mismatch::Extensions {
                served: names(&["echo.reply"]),
                provided: names(&["echo.fail", "echo.reply"]),
            }
            .to_string(),
            r#"it serves {"echo.reply"}, but its manifest provides {"echo.fail", "echo.reply"}"#
        );
        assert_eq!(
            Mismatch::Protocol(2).to_string(),
            "it speaks protocol 2, not 1"
        );
    }

    #[test]
    fn only_a_readiness_document_answers_ready() {
        let provided = names(&["echo.reply"]);
        let answer = |status: StatusCode, body: &str| Answer {
            status,
            body: Bytes::from(body.to_string()),
        };
        assert!(readiness(
            answer(
                StatusCode::OK,
                r#"{"protocol":1,"extensions":["echo.reply"]}"#
            ),
            &provided
        )
        .is_ok());
        assert!(matches!(
            readiness(
                answer(StatusCode::OK, r#"{"protocol":1,"extensions":[]}"#),
                &provided
            ),
            Err(NotReady::Mismatch(_))
        ));
        for (status, body) in [
            (StatusCode::OK, "ready"),
            (
                StatusCode::NOT_FOUND,
                r#"{"protocol":1,"extensions":["echo.reply"]}"#,
            ),
        ] {
            assert!(matches!(
                readiness(answer(status, body), &provided),
                Err(NotReady::Unanswered(_))
            ));
        }
    }

    #[tokio::test]
    async fn a_child_that_never_answers_is_given_up_on_at_the_timeout() {
        let dir = tempfile::TempDir::new().expect("dir");
        let started = Instant::now();
        let err = until_ready(
            &Endpoint::new(&dir.path().join("never.sock"), "t0ken"),
            &names(&["echo.reply"]),
            Duration::from_millis(300),
        )
        .await
        .expect_err("never ready")
        .to_string();
        assert!(started.elapsed() >= Duration::from_millis(300));
        assert!(
            err.starts_with("it did not answer ready within 300ms: "),
            "{err}"
        );
    }
}
