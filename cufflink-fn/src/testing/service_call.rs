//! Scripted services of the tenant.

use super::http::UrlMatch;
use crate::call::CallError;
use crate::host::service_call::ServiceCallHost;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::Deserialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

const METHODS: [&str; 6] = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"];

/// A call a handler made.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCall {
    pub service: String,
    pub method: String,
    /// The path inside the service, with its query.
    pub path: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default, rename = "body_b64", deserialize_with = "body")]
    pub body: Vec<u8>,
}

fn body<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|encoded| STANDARD.decode(encoded).map_err(serde::de::Error::custom))
        .transpose()
        .map(Option::unwrap_or_default)
}

#[derive(Debug, Clone)]
enum Answer {
    Response {
        status: u16,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
    },
    Failure(CallError),
}

impl Answer {
    fn envelope(&self) -> Value {
        match self {
            Answer::Response {
                status,
                headers,
                body,
            } => json!({
                "ok": true,
                "status": status,
                "headers": headers,
                "body_b64": STANDARD.encode(body),
            }),
            Answer::Failure(error) => failure(error),
        }
    }
}

fn failure(error: &CallError) -> Value {
    let kind = match error {
        CallError::Timeout => "timeout",
        CallError::DepthExceeded => "depth_exceeded",
        CallError::UnknownService(_) => "unknown_service",
        CallError::Transport(_) => "transport",
        CallError::Denied(_) => "denied",
    };
    json!({ "ok": false, "error": { "kind": kind, "message": error.to_string() } })
}

struct Expectation {
    service: String,
    method: String,
    path: UrlMatch,
    answer: Answer,
    times: Option<usize>,
    hits: usize,
}

impl Expectation {
    fn open(&self) -> Option<(String, String, UrlMatch)> {
        self.times
            .is_none_or(|times| self.hits < times)
            .then(|| (self.service.clone(), self.method.clone(), self.path.clone()))
    }

    fn unmet(&self) -> Option<String> {
        let met = match self.times {
            Some(times) => self.hits == times,
            None => self.hits > 0,
        };
        (!met).then(|| {
            format!(
                "{} {}{} was called {} time(s), expected {}",
                self.method,
                self.service,
                self.path,
                self.hits,
                self.times
                    .map_or_else(|| "at least 1".to_string(), |times| times.to_string())
            )
        })
    }
}

#[derive(Default)]
struct Script {
    expectations: Vec<Expectation>,
    calls: Vec<ServiceCall>,
    unmatched: Vec<String>,
}

/// Answers service calls from its expectations, in the order they were
/// added, and records every call the platform would have sent. A call
/// nothing answers fails as a transport error. Clones share one script.
#[derive(Clone, Default)]
pub struct MockServiceCalls {
    script: Rc<RefCell<Script>>,
}

/// An expectation being described; it answers `200` with an empty body,
/// any number of times and at least once, until told otherwise.
pub struct ExpectingCall<'a> {
    mock: &'a MockServiceCalls,
    index: usize,
}

/// What the platform refuses before it sends anything.
fn refused(call: &ServiceCall) -> Option<CallError> {
    let method = call.method.to_ascii_uppercase();
    let valid_path =
        call.path.starts_with('/') && call.path.bytes().all(|b| b.is_ascii_graphic() && b != b'#');
    if !METHODS.contains(&method.as_str()) {
        Some(CallError::Denied(format!(
            "method {method:?} is not allowed"
        )))
    } else if !valid_path {
        Some(CallError::Denied(format!(
            "path {:?} is not a path of a service",
            call.path
        )))
    } else {
        None
    }
}

impl MockServiceCalls {
    pub fn new() -> Self {
        Self::default()
    }

    /// Expect `method` on a path of `service`; the path is matched with its
    /// query.
    pub fn expect(&self, service: &str, method: &str, path: UrlMatch) -> ExpectingCall<'_> {
        let mut script = self.script.borrow_mut();
        script.expectations.push(Expectation {
            service: service.to_string(),
            method: method.to_string(),
            path,
            answer: Answer::Response {
                status: 200,
                headers: BTreeMap::new(),
                body: Vec::new(),
            },
            times: None,
            hits: 0,
        });
        ExpectingCall {
            mock: self,
            index: script.expectations.len() - 1,
        }
    }

    /// Every call the platform would have sent, answered or not, in order.
    pub fn calls(&self) -> Vec<ServiceCall> {
        self.script.borrow().calls.clone()
    }

    /// The expectations that were not met and the calls nothing answered.
    pub fn verify(&self) -> Result<(), Vec<String>> {
        let script = self.script.borrow();
        let problems: Vec<String> = script
            .expectations
            .iter()
            .filter_map(Expectation::unmet)
            .chain(script.unmatched.iter().cloned())
            .collect();
        problems.is_empty().then_some(()).ok_or(problems)
    }

    fn answered_by(&self, call: &ServiceCall) -> Option<usize> {
        let open: Vec<(usize, String, String, UrlMatch)> = self
            .script
            .borrow()
            .expectations
            .iter()
            .enumerate()
            .filter_map(|(index, expectation)| {
                expectation
                    .open()
                    .map(|(service, method, path)| (index, service, method, path))
            })
            .collect();
        open.into_iter()
            .find(|(_, service, method, path)| {
                *service == call.service
                    && method.eq_ignore_ascii_case(&call.method)
                    && path.matches(&call.path)
            })
            .map(|(index, ..)| index)
    }

    fn answer(&self, call: ServiceCall) -> Value {
        let answered = self.answered_by(&call);
        let mut script = self.script.borrow_mut();
        let unmatched = format!(
            "no mock matched {} {}{}",
            call.method, call.service, call.path
        );
        script.calls.push(call);
        match answered.and_then(|index| script.expectations.get_mut(index)) {
            Some(expectation) => {
                expectation.hits += 1;
                expectation.answer.envelope()
            }
            None => {
                script.unmatched.push(unmatched.clone());
                failure(&CallError::Transport(unmatched))
            }
        }
    }
}

impl ExpectingCall<'_> {
    fn set(self, change: impl FnOnce(&mut Expectation)) -> Self {
        if let Some(expectation) = self
            .mock
            .script
            .borrow_mut()
            .expectations
            .get_mut(self.index)
        {
            change(expectation);
        }
        self
    }

    /// Header names are answered in lowercase, as the platform answers them.
    pub fn respond(self, status: u16, headers: &[(&str, &str)], body: impl Into<Vec<u8>>) -> Self {
        let answer = Answer::Response {
            status,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), value.to_string()))
                .collect(),
            body: body.into(),
        };
        self.set(|expectation| expectation.answer = answer)
    }

    /// Answer `value` as `application/json`.
    pub fn respond_json(self, status: u16, value: &Value) -> Self {
        self.respond(
            status,
            &[("content-type", "application/json")],
            value.to_string(),
        )
    }

    pub fn fail(self, error: CallError) -> Self {
        self.set(|expectation| expectation.answer = Answer::Failure(error))
    }

    /// Answer exactly `times` calls.
    pub fn times(self, times: usize) -> Self {
        self.set(|expectation| expectation.times = Some(times))
    }
}

impl ServiceCallHost for MockServiceCalls {
    fn call(&self, request: &Value) -> Value {
        match serde_json::from_value::<ServiceCall>(request.clone()) {
            Ok(call) => match refused(&call) {
                Some(refusal) => failure(&refusal),
                None => self.answer(call),
            },
            Err(e) => failure(&CallError::Transport(format!(
                "malformed service_call request: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::{self, HostSet};
    use crate::http::call_service;

    fn hosted<T>(services: &MockServiceCalls, body: impl FnOnce() -> T) -> T {
        let hosts = HostSet {
            service_call: Some(Rc::new(services.clone())),
            ..Default::default()
        };
        host::with(hosts, body)
    }

    #[test]
    fn a_call_is_answered_as_the_target_would_answer_it() {
        let services = MockServiceCalls::new();
        services
            .expect("catalog-service", "GET", UrlMatch::prefix("/books/"))
            .respond_json(200, &json!({"id": "7", "status": "open"}));
        services
            .expect("pricing", "post", UrlMatch::exact("/_fn/price?draft=1"))
            .respond(422, &[("X-Reason", "sku")], "unknown sku")
            .times(1);
        let (book, price) = hosted(&services, || {
            (
                call_service("catalog-service", "GET", "/books/7", &[], None),
                call_service(
                    "pricing",
                    "POST",
                    "/_fn/price?draft=1",
                    &[("x-request-source", "loans")],
                    Some(br#"{"sku":"a"}"#),
                ),
            )
        });
        let book = book.expect("book");
        assert!(book.is_success());
        assert_eq!(book.headers["content-type"], "application/json");
        assert_eq!(
            book.json::<Value>().ok(),
            Some(json!({"id": "7", "status": "open"}))
        );
        let price = price.expect("price");
        assert_eq!(
            (price.status, price.text().ok()),
            (422, Some("unknown sku"))
        );
        assert_eq!(price.headers["x-reason"], "sku");
        assert_eq!(services.verify(), Ok(()));
        assert_eq!(
            services.calls(),
            [
                ServiceCall {
                    service: "catalog-service".into(),
                    method: "GET".into(),
                    path: "/books/7".into(),
                    headers: BTreeMap::new(),
                    body: Vec::new(),
                },
                ServiceCall {
                    service: "pricing".into(),
                    method: "POST".into(),
                    path: "/_fn/price?draft=1".into(),
                    headers: BTreeMap::from([("x-request-source".into(), "loans".into())]),
                    body: br#"{"sku":"a"}"#.to_vec(),
                },
            ]
        );
    }

    #[test]
    fn failures_reach_the_handler_as_their_kind() {
        let services = MockServiceCalls::new();
        services
            .expect("slow", "GET", UrlMatch::exact("/x"))
            .fail(CallError::Timeout);
        services
            .expect("deep", "GET", UrlMatch::exact("/x"))
            .fail(CallError::DepthExceeded);
        services
            .expect("gone", "GET", UrlMatch::exact("/x"))
            .fail(CallError::UnknownService(
                "no service \"gone\" in this tenant".into(),
            ));
        hosted(&services, || {
            assert_eq!(
                call_service("slow", "GET", "/x", &[], None),
                Err(CallError::Timeout)
            );
            assert_eq!(
                call_service("deep", "GET", "/x", &[], None),
                Err(CallError::DepthExceeded)
            );
            assert_eq!(
                call_service("gone", "GET", "/x", &[], None),
                Err(CallError::UnknownService(
                    "no service \"gone\" in this tenant".into()
                ))
            );
        });
        assert_eq!(services.verify(), Ok(()));
    }

    #[test]
    fn what_was_not_scripted_fails_and_is_reported() {
        let services = MockServiceCalls::new();
        services
            .expect("pricing", "GET", UrlMatch::exact("/once"))
            .times(1);
        services.expect("pricing", "GET", UrlMatch::exact("/never"));
        hosted(&services, || {
            assert!(call_service("pricing", "GET", "/once", &[], None).is_ok());
            assert_eq!(
                call_service("pricing", "GET", "/once", &[], None),
                Err(CallError::Transport(
                    "no mock matched GET pricing/once".into()
                ))
            );
            assert_eq!(
                call_service("billing", "GET", "/once", &[], None),
                Err(CallError::Transport(
                    "no mock matched GET billing/once".into()
                ))
            );
        });
        assert_eq!(
            services.verify(),
            Err(vec![
                "GET pricing/never was called 0 time(s), expected at least 1".into(),
                "no mock matched GET pricing/once".into(),
                "no mock matched GET billing/once".into(),
            ])
        );
        assert_eq!(services.calls().len(), 3);
    }

    #[test]
    fn what_the_platform_refuses_is_never_sent() {
        let services = MockServiceCalls::new();
        services.expect("pricing", "TRACE", UrlMatch::prefix("/"));
        hosted(&services, || {
            assert_eq!(
                call_service("pricing", "TRACE", "/x", &[], None),
                Err(CallError::Denied("method \"TRACE\" is not allowed".into()))
            );
            for path in ["x", "", "/a b", "/a#b"] {
                assert!(
                    matches!(
                        call_service("pricing", "GET", path, &[], None),
                        Err(CallError::Denied(_))
                    ),
                    "{path:?}"
                );
            }
        });
        assert!(services.calls().is_empty());
        assert!(matches!(
            call_service("pricing", "GET", "/x", &[], None),
            Err(CallError::Transport(_))
        ));
    }
}
