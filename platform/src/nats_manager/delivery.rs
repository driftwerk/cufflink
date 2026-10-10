use cufflink_types::backoff::Backoff;
use cufflink_types::AckMode;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Retry {
        after: Option<Duration>,
        reason: String,
    },
    Terminal {
        reason: String,
    },
    Invalid {
        reason: String,
    },
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    Ack,
    Nak(Duration),
    DeadLetterThenTerm { reason: String },
    Term { reason: String },
}

fn exhausted(delivered: u64, max_deliver: i64) -> bool {
    u64::try_from(max_deliver).is_ok_and(|max| max > 0 && delivered >= max)
}

fn give_up(dead_letter: bool, reason: &str) -> Decision {
    let reason = reason.to_string();
    if dead_letter {
        Decision::DeadLetterThenTerm { reason }
    } else {
        Decision::Term { reason }
    }
}

fn attempt(delivered: u64) -> u32 {
    u32::try_from(delivered.max(1)).unwrap_or(u32::MAX)
}

pub fn decide(
    ack: AckMode,
    outcome: &Outcome,
    delivered: u64,
    max_deliver: i64,
    backoff: &Backoff,
    dead_letter: bool,
    seed: u64,
) -> Decision {
    match (ack, outcome) {
        (AckMode::Always, _) | (AckMode::OnSuccess, Outcome::Ok) => Decision::Ack,
        (AckMode::OnSuccess, Outcome::Interrupted) => Decision::Nak(Duration::ZERO),
        (AckMode::OnSuccess, Outcome::Invalid { reason } | Outcome::Terminal { reason }) => {
            give_up(dead_letter, reason)
        }
        (AckMode::OnSuccess, Outcome::Retry { reason, .. })
            if exhausted(delivered, max_deliver) =>
        {
            give_up(dead_letter, reason)
        }
        (AckMode::OnSuccess, Outcome::Retry { after, .. }) => {
            Decision::Nak(after.unwrap_or_else(|| backoff.delay(attempt(delivered), seed)))
        }
    }
}

pub fn result_label(outcome: &Outcome, decision: &Decision) -> &'static str {
    match (decision, outcome) {
        (Decision::Ack, Outcome::Ok) => "ok",
        (Decision::Ack, Outcome::Invalid { .. }) => "invalid",
        (Decision::Ack, _) => "error",
        (Decision::Nak(_), Outcome::Interrupted) => "nak",
        (Decision::Nak(_), _) => "retry",
        (Decision::DeadLetterThenTerm { .. }, _) => "dead_letter",
        (Decision::Term { .. }, _) => "terminal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(after: Option<Duration>) -> Outcome {
        Outcome::Retry {
            after,
            reason: "handler answered 503".into(),
        }
    }

    fn terminal() -> Outcome {
        Outcome::Terminal {
            reason: "handler rejected the message with 422".into(),
        }
    }

    fn invalid() -> Outcome {
        Outcome::Invalid {
            reason: "payload is not JSON".into(),
        }
    }

    fn outcomes() -> [Outcome; 6] {
        [
            Outcome::Ok,
            Outcome::Interrupted,
            retry(None),
            retry(Some(Duration::from_secs(7))),
            terminal(),
            invalid(),
        ]
    }

    fn fixed() -> Backoff {
        Backoff::explicit(vec![Duration::from_secs(10), Duration::from_secs(30)])
    }

    #[test]
    fn always_acknowledges_whatever_the_handler_did() {
        for outcome in outcomes() {
            for (delivered, max_deliver, dead_letter) in
                [(1, 5, true), (5, 5, true), (9, 3, false), (1, -1, true)]
            {
                assert_eq!(
                    decide(
                        AckMode::Always,
                        &outcome,
                        delivered,
                        max_deliver,
                        &fixed(),
                        dead_letter,
                        42
                    ),
                    Decision::Ack,
                    "{outcome:?} {delivered}/{max_deliver}"
                );
            }
        }
    }

    #[test]
    fn on_success_hands_an_interrupted_message_back_at_once() {
        for (delivered, max_deliver) in [(1, 5), (5, 5), (7, -1)] {
            assert_eq!(
                decide(
                    AckMode::OnSuccess,
                    &Outcome::Interrupted,
                    delivered,
                    max_deliver,
                    &fixed(),
                    true,
                    1
                ),
                Decision::Nak(Duration::ZERO),
                "{delivered}/{max_deliver}"
            );
        }
    }

    #[test]
    fn on_success_retries_until_the_deliveries_run_out() {
        let decided = |outcome: &Outcome, delivered, max_deliver, dead_letter| {
            decide(
                AckMode::OnSuccess,
                outcome,
                delivered,
                max_deliver,
                &fixed(),
                dead_letter,
                42,
            )
        };
        assert_eq!(decided(&Outcome::Ok, 3, 3, true), Decision::Ack);
        assert_eq!(
            decided(&retry(Some(Duration::from_secs(7))), 1, 3, true),
            Decision::Nak(Duration::from_secs(7)),
            "the handler's own delay wins"
        );
        assert_eq!(
            decided(&retry(None), 1, 3, true),
            Decision::Nak(fixed().delay(1, 42))
        );
        assert_eq!(
            decided(&retry(None), 2, 3, true),
            Decision::Nak(fixed().delay(2, 42))
        );
        for outcome in [retry(None), retry(Some(Duration::from_secs(7)))] {
            assert_eq!(
                decided(&outcome, 3, 3, true),
                Decision::DeadLetterThenTerm {
                    reason: "handler answered 503".into()
                }
            );
            assert_eq!(
                decided(&outcome, 4, 3, false),
                Decision::Term {
                    reason: "handler answered 503".into()
                }
            );
        }
    }

    #[test]
    fn on_success_gives_up_at_once_on_what_no_retry_can_fix() {
        for (outcome, reason) in [
            (terminal(), "handler rejected the message with 422"),
            (invalid(), "payload is not JSON"),
        ] {
            for (delivered, max_deliver) in [(1, 5), (1, -1), (5, 5)] {
                assert_eq!(
                    decide(
                        AckMode::OnSuccess,
                        &outcome,
                        delivered,
                        max_deliver,
                        &fixed(),
                        true,
                        0
                    ),
                    Decision::DeadLetterThenTerm {
                        reason: reason.into()
                    }
                );
                assert_eq!(
                    decide(
                        AckMode::OnSuccess,
                        &outcome,
                        delivered,
                        max_deliver,
                        &fixed(),
                        false,
                        0
                    ),
                    Decision::Term {
                        reason: reason.into()
                    }
                );
            }
        }
    }

    #[test]
    fn unlimited_deliveries_never_run_out() {
        for max_deliver in [-1, 0] {
            for delivered in [1, 100, u64::MAX] {
                assert!(
                    matches!(
                        decide(
                            AckMode::OnSuccess,
                            &retry(None),
                            delivered,
                            max_deliver,
                            &fixed(),
                            true,
                            3
                        ),
                        Decision::Nak(delay) if delay >= Duration::from_secs(1)
                    ),
                    "{delivered}/{max_deliver}"
                );
            }
        }
    }

    #[test]
    fn a_decision_is_counted_under_the_result_it_had() {
        let nak = Decision::Nak(Duration::ZERO);
        let dead = Decision::DeadLetterThenTerm { reason: "r".into() };
        let term = Decision::Term { reason: "r".into() };
        for (outcome, decision, label) in [
            (Outcome::Ok, Decision::Ack, "ok"),
            (invalid(), Decision::Ack, "invalid"),
            (retry(None), Decision::Ack, "error"),
            (terminal(), Decision::Ack, "error"),
            (Outcome::Interrupted, Decision::Ack, "error"),
            (Outcome::Interrupted, nak.clone(), "nak"),
            (retry(None), nak, "retry"),
            (retry(None), dead.clone(), "dead_letter"),
            (invalid(), dead, "dead_letter"),
            (terminal(), term, "terminal"),
        ] {
            assert_eq!(result_label(&outcome, &decision), label, "{outcome:?}");
        }
    }
}
