//! Scripted extensions of the tenant.

use crate::ext::CallErrorKind;
use crate::host::ext::ExtHost;
use cufflink_types::ext::{is_valid_extension_name, ExtFailure};
use cufflink_types::host::ext::{CallErrorEnvelope, CallRequest, CallResponse};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

struct Expectation {
    extension: String,
    answer: CallResponse,
    times: Option<usize>,
    hits: usize,
}

impl Expectation {
    fn answers(&self, extension: &str) -> bool {
        self.extension == extension && self.times.is_none_or(|times| self.hits < times)
    }

    fn unmet(&self) -> Option<String> {
        let met = match self.times {
            Some(times) => self.hits == times,
            None => self.hits > 0,
        };
        (!met).then(|| {
            format!(
                "extension '{}' was called {} time(s), expected {}",
                self.extension,
                self.hits,
                self.times
                    .map_or_else(|| "at least 1".to_string(), |times| times.to_string())
            )
        })
    }
}

#[derive(Default)]
struct Script {
    declared: Option<BTreeSet<String>>,
    expectations: Vec<Expectation>,
    calls: Vec<CallRequest>,
    unmatched: Vec<String>,
}

/// Answers extension calls from its expectations, in the order they were
/// added, and records every call the platform would have sent. A call to
/// an extension the service does not declare is refused `not_declared` and
/// never sent; a call nothing answers is `not_found`. Clones share one
/// script.
#[derive(Clone, Default)]
pub struct MockExtensions {
    script: Rc<RefCell<Script>>,
}

/// An expectation being described; it answers `null`, any number of times
/// and at least once, until told otherwise.
pub struct ExpectingExt<'a> {
    mock: &'a MockExtensions,
    index: usize,
}

fn refused(kind: CallErrorKind, extension: &str) -> CallResponse {
    let message = match kind {
        CallErrorKind::NotDeclared => {
            format!("the service does not declare the extension '{extension}'")
        }
        CallErrorKind::NotFound => {
            format!("no extension '{extension}' is registered for the tenant")
        }
        CallErrorKind::Denied => {
            format!("the registration of '{extension}' does not list the service")
        }
        CallErrorKind::Unavailable => format!("nothing serves the extension '{extension}'"),
        CallErrorKind::Timeout => format!("the extension '{extension}' did not answer in time"),
        CallErrorKind::TooLarge => {
            format!("the call to '{extension}' or its reply is too large")
        }
        CallErrorKind::Failed => format!("the extension '{extension}' failed"),
        CallErrorKind::Invalid => format!("the call to '{extension}' is malformed"),
    };
    CallResponse::Error(CallErrorEnvelope {
        kind,
        code: None,
        message,
    })
}

impl MockExtensions {
    /// Every extension counts as declared by the service.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only `extensions` count as declared by the service, as its
    /// manifest's `extensions` list them.
    pub fn declaring<I, S>(extensions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mock = Self::default();
        mock.script.borrow_mut().declared = Some(extensions.into_iter().map(Into::into).collect());
        mock
    }

    pub fn expect(&self, extension: &str) -> ExpectingExt<'_> {
        let mut script = self.script.borrow_mut();
        script.expectations.push(Expectation {
            extension: extension.to_string(),
            answer: CallResponse::Ok(Value::Null),
            times: None,
            hits: 0,
        });
        ExpectingExt {
            mock: self,
            index: script.expectations.len() - 1,
        }
    }

    /// Every call the platform would have sent, answered or not, in order.
    pub fn calls(&self) -> Vec<CallRequest> {
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

    fn declares(&self, extension: &str) -> bool {
        is_valid_extension_name(extension)
            && self
                .script
                .borrow()
                .declared
                .as_ref()
                .is_none_or(|declared| declared.contains(extension))
    }

    fn answer(&self, request: &CallRequest) -> CallResponse {
        let mut script = self.script.borrow_mut();
        script.calls.push(request.clone());
        match script
            .expectations
            .iter_mut()
            .find(|expectation| expectation.answers(&request.extension))
        {
            Some(expectation) => {
                expectation.hits += 1;
                expectation.answer.clone()
            }
            None => {
                script
                    .unmatched
                    .push(format!("no mock matched extension '{}'", request.extension));
                refused(CallErrorKind::NotFound, &request.extension)
            }
        }
    }
}

impl ExpectingExt<'_> {
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

    pub fn respond(self, value: Value) -> Self {
        self.set(|expectation| expectation.answer = CallResponse::Ok(value))
    }

    /// Answer as the platform does when the extension fails with `code`:
    /// a reserved code (`unavailable`, `timeout`, `too_large`) as its
    /// kind, any other as the extension's own failure.
    pub fn fail(self, code: &str, message: &str) -> Self {
        let answer = CallResponse::Error(
            ExtFailure {
                code: code.to_string(),
                message: message.to_string(),
            }
            .into(),
        );
        self.set(|expectation| expectation.answer = answer)
    }

    /// Answer as the platform does when it refuses the call as `kind`.
    pub fn refuse(self, kind: CallErrorKind) -> Self {
        self.set(|expectation| expectation.answer = refused(kind, &expectation.extension))
    }

    /// Answer exactly `times` calls.
    pub fn times(self, times: usize) -> Self {
        self.set(|expectation| expectation.times = Some(times))
    }
}

impl ExtHost for MockExtensions {
    fn call(&self, request: &CallRequest) -> CallResponse {
        if self.declares(&request.extension) {
            self.answer(request)
        } else {
            refused(CallErrorKind::NotDeclared, &request.extension)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ext::{call, call_with_timeout, ExtError};
    use crate::host::{self, HostSet};
    use serde_json::json;

    fn hosted<T>(extensions: &MockExtensions, body: impl FnOnce() -> T) -> T {
        let hosts = HostSet {
            ext: Some(Rc::new(extensions.clone())),
            ..Default::default()
        };
        host::with(hosts, body)
    }

    #[test]
    fn a_call_is_answered_as_the_extension_would_answer_it() {
        let extensions = MockExtensions::new();
        extensions
            .expect("google.token")
            .respond(json!({"token": "ya29"}))
            .times(1);
        extensions
            .expect("google.token")
            .fail("quota", "daily quota exhausted")
            .times(1);
        extensions
            .expect("google.token")
            .fail("timeout", "the runner gave up");
        extensions.expect("audit.ping");
        let answers = hosted(&extensions, || {
            [
                call("google.token", &json!({"scope": "analytics.readonly"})),
                call_with_timeout("google.token", &json!({"scope": "x"}), 2_500),
                call("google.token", &Value::Null),
                call("audit.ping", &Value::Null),
            ]
        });
        assert_eq!(
            answers,
            [
                Ok(json!({"token": "ya29"})),
                Err(ExtError::Failed {
                    code: "quota".into(),
                    message: "daily quota exhausted".into(),
                }),
                Err(ExtError::Timeout),
                Ok(Value::Null),
            ]
        );
        assert_eq!(extensions.verify(), Ok(()));
        assert_eq!(
            extensions.calls(),
            [
                CallRequest {
                    extension: "google.token".into(),
                    payload: json!({"scope": "analytics.readonly"}),
                    timeout_ms: None,
                },
                CallRequest {
                    extension: "google.token".into(),
                    payload: json!({"scope": "x"}),
                    timeout_ms: Some(2_500),
                },
                CallRequest {
                    extension: "google.token".into(),
                    payload: Value::Null,
                    timeout_ms: None,
                },
                CallRequest {
                    extension: "audit.ping".into(),
                    payload: Value::Null,
                    timeout_ms: None,
                },
            ]
        );
    }

    #[test]
    fn refusals_reach_the_handler_as_their_kind() {
        let extensions = MockExtensions::new();
        for (name, kind) in [
            ("a.not-declared", CallErrorKind::NotDeclared),
            ("a.not-found", CallErrorKind::NotFound),
            ("a.denied", CallErrorKind::Denied),
            ("a.unavailable", CallErrorKind::Unavailable),
            ("a.timeout", CallErrorKind::Timeout),
            ("a.too-large", CallErrorKind::TooLarge),
            ("a.failed", CallErrorKind::Failed),
            ("a.invalid", CallErrorKind::Invalid),
        ] {
            extensions.expect(name).refuse(kind);
        }
        let answers: Vec<_> = hosted(&extensions, || {
            [
                "a.not-declared",
                "a.not-found",
                "a.denied",
                "a.unavailable",
                "a.timeout",
                "a.too-large",
                "a.failed",
                "a.invalid",
            ]
            .map(|name| call(name, &json!({})))
            .into()
        });
        assert_eq!(
            answers,
            [
                Err(ExtError::NotDeclared),
                Err(ExtError::NotFound),
                Err(ExtError::Denied),
                Err(ExtError::Unavailable(
                    "nothing serves the extension 'a.unavailable'".into()
                )),
                Err(ExtError::Timeout),
                Err(ExtError::TooLarge),
                Err(ExtError::Failed {
                    code: "failed".into(),
                    message: "the extension 'a.failed' failed".into(),
                }),
                Err(ExtError::Invalid(
                    "the call to 'a.invalid' is malformed".into()
                )),
            ]
        );
        assert_eq!(extensions.verify(), Ok(()));
    }

    #[test]
    fn what_was_not_scripted_is_not_found_and_reported() {
        let extensions = MockExtensions::new();
        extensions.expect("google.token").times(1);
        extensions.expect("pdf.render");
        hosted(&extensions, || {
            assert_eq!(call("google.token", &json!({})), Ok(Value::Null));
            assert_eq!(call("google.token", &json!({})), Err(ExtError::NotFound));
            assert_eq!(call("maps.geocode", &json!({})), Err(ExtError::NotFound));
        });
        assert_eq!(
            extensions.verify(),
            Err(vec![
                "extension 'pdf.render' was called 0 time(s), expected at least 1".into(),
                "no mock matched extension 'google.token'".into(),
                "no mock matched extension 'maps.geocode'".into(),
            ])
        );
        assert_eq!(extensions.calls().len(), 3, "unanswered calls are recorded");
    }

    #[test]
    fn an_undeclared_extension_is_refused_and_never_sent() {
        let declared = MockExtensions::declaring(["google.token"]);
        declared.expect("google.token").respond(json!("ya29"));
        declared.expect("pdf.render").respond(json!("%PDF"));
        hosted(&declared, || {
            assert_eq!(call("google.token", &json!({})), Ok(json!("ya29")));
            assert_eq!(call("pdf.render", &json!({})), Err(ExtError::NotDeclared));
        });
        assert_eq!(declared.calls().len(), 1);
        assert_eq!(
            declared.verify(),
            Err(vec![
                "extension 'pdf.render' was called 0 time(s), expected at least 1".into()
            ])
        );
        let open = MockExtensions::new();
        open.expect("Google.Token");
        hosted(&open, || {
            for invalid in ["Google.Token", "", "google.*", "a..b"] {
                assert_eq!(
                    call(invalid, &json!({})),
                    Err(ExtError::NotDeclared),
                    "{invalid:?}"
                );
            }
        });
        assert!(open.calls().is_empty());
    }
}
