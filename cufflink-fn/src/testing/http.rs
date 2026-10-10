//! A scripted HTTP server.

use crate::host::http::HttpHost;
use cufflink_types::host::http::{
    FetchRequestV2, FetchResponseV2, HttpErrorEnvelope, HttpErrorKind,
};
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

/// Which URLs an expectation answers.
#[derive(Clone)]
pub enum UrlMatch {
    Exact(String),
    Prefix(String),
    Matching(Rc<dyn Fn(&str) -> bool>),
}

impl UrlMatch {
    pub fn exact(url: impl Into<String>) -> Self {
        UrlMatch::Exact(url.into())
    }

    pub fn prefix(prefix: impl Into<String>) -> Self {
        UrlMatch::Prefix(prefix.into())
    }

    /// A predicate over the URL; it may read the mock it is installed in.
    pub fn matching(predicate: impl Fn(&str) -> bool + 'static) -> Self {
        UrlMatch::Matching(Rc::new(predicate))
    }

    pub(super) fn matches(&self, url: &str) -> bool {
        match self {
            UrlMatch::Exact(exact) => url == exact,
            UrlMatch::Prefix(prefix) => url.starts_with(prefix.as_str()),
            UrlMatch::Matching(predicate) => predicate(url),
        }
    }
}

impl fmt::Display for UrlMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UrlMatch::Exact(exact) => f.write_str(exact),
            UrlMatch::Prefix(prefix) => write!(f, "{prefix}*"),
            UrlMatch::Matching(_) => f.write_str("(a predicate)"),
        }
    }
}

struct Expectation {
    method: String,
    url: UrlMatch,
    response: FetchResponseV2,
    times: Option<usize>,
    hits: usize,
}

impl Expectation {
    /// What this expectation still answers, once no borrow of the script
    /// is held: a predicate is free to read the mock.
    fn open(&self) -> Option<(String, UrlMatch)> {
        self.times
            .is_none_or(|times| self.hits < times)
            .then(|| (self.method.clone(), self.url.clone()))
    }

    fn unmet(&self) -> Option<String> {
        let met = match self.times {
            Some(times) => self.hits == times,
            None => self.hits > 0,
        };
        (!met).then(|| {
            format!(
                "{} {} was requested {} time(s), expected {}",
                self.method,
                self.url,
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
    requests: Vec<FetchRequestV2>,
    unmatched: Vec<String>,
}

/// Answers requests from its expectations, in the order they were added,
/// and records every request. A request nothing answers fails with
/// `Denied`. Clones share one script.
#[derive(Clone, Default)]
pub struct MockHttp {
    script: Rc<RefCell<Script>>,
}

/// An expectation being described; it answers `200` with an empty body,
/// any number of times and at least once, until told otherwise.
pub struct Expecting<'a> {
    mock: &'a MockHttp,
    index: usize,
}

impl MockHttp {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn expect(&self, method: &str, url: UrlMatch) -> Expecting<'_> {
        let mut script = self.script.borrow_mut();
        script.expectations.push(Expectation {
            method: method.to_string(),
            url,
            response: FetchResponseV2::Ok {
                status: 200,
                headers: Vec::new(),
                body: Vec::new(),
            },
            times: None,
            hits: 0,
        });
        Expecting {
            mock: self,
            index: script.expectations.len() - 1,
        }
    }

    /// Every request made, answered or not, in order.
    pub fn requests(&self) -> Vec<FetchRequestV2> {
        self.script.borrow().requests.clone()
    }

    /// The expectations that were not met and the requests nothing answered.
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

    /// The index of the first open expectation that answers `request`.
    fn answered_by(&self, request: &FetchRequestV2) -> Option<usize> {
        let open: Vec<(usize, String, UrlMatch)> = self
            .script
            .borrow()
            .expectations
            .iter()
            .enumerate()
            .filter_map(|(index, expectation)| {
                expectation.open().map(|(method, url)| (index, method, url))
            })
            .collect();
        open.into_iter()
            .find(|(_, method, url)| {
                method.eq_ignore_ascii_case(&request.method) && url.matches(&request.url)
            })
            .map(|(index, ..)| index)
    }
}

impl Expecting<'_> {
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

    pub fn respond(self, status: u16, headers: &[(&str, &str)], body: impl Into<Vec<u8>>) -> Self {
        let response = FetchResponseV2::Ok {
            status,
            headers: headers
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            body: body.into(),
        };
        self.set(|expectation| expectation.response = response)
    }

    pub fn fail(self, kind: HttpErrorKind, message: &str) -> Self {
        let response = FetchResponseV2::Error(HttpErrorEnvelope {
            kind,
            message: message.to_string(),
        });
        self.set(|expectation| expectation.response = response)
    }

    /// Answer exactly `times` requests.
    pub fn times(self, times: usize) -> Self {
        self.set(|expectation| expectation.times = Some(times))
    }
}

impl HttpHost for MockHttp {
    fn fetch(&self, request: &FetchRequestV2) -> FetchResponseV2 {
        let answered = self.answered_by(request);
        let mut script = self.script.borrow_mut();
        script.requests.push(request.clone());
        match answered.and_then(|index| script.expectations.get_mut(index)) {
            Some(expectation) => {
                expectation.hits += 1;
                expectation.response.clone()
            }
            None => {
                let message = format!("no mock matched {} {}", request.method, request.url);
                script.unmatched.push(message.clone());
                FetchResponseV2::Error(HttpErrorEnvelope {
                    kind: HttpErrorKind::Denied,
                    message,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(url: &str) -> FetchRequestV2 {
        FetchRequestV2 {
            method: "GET".into(),
            url: url.into(),
            ..Default::default()
        }
    }

    fn status(response: FetchResponseV2) -> Result<u16, HttpErrorKind> {
        match response {
            FetchResponseV2::Ok { status, .. } => Ok(status),
            FetchResponseV2::Error(e) => Err(e.kind),
        }
    }

    #[test]
    fn the_first_expectation_with_answers_left_answers() {
        let mock = MockHttp::new();
        mock.expect("GET", UrlMatch::exact("https://a.example/x"))
            .respond(503, &[], "busy")
            .times(1);
        mock.expect("get", UrlMatch::prefix("https://a.example/"))
            .respond(201, &[("x-a", "1")], vec![0, 255]);
        mock.expect(
            "POST",
            UrlMatch::matching(|url| url.ends_with("/convert/html")),
        )
        .fail(HttpErrorKind::Timeout, "slow");
        let host: &dyn HttpHost = &mock;
        assert_eq!(status(host.fetch(&get("https://a.example/x"))), Ok(503));
        assert_eq!(
            host.fetch(&get("https://a.example/x")),
            FetchResponseV2::Ok {
                status: 201,
                headers: vec![("x-a".into(), "1".into())],
                body: vec![0, 255],
            }
        );
        let post = FetchRequestV2 {
            method: "POST".into(),
            ..get("https://pdf.example.com/convert/html")
        };
        assert_eq!(status(host.fetch(&post)), Err(HttpErrorKind::Timeout));
        assert_eq!(mock.verify(), Ok(()));
        assert_eq!(mock.requests().len(), 3);
        assert_eq!(mock.requests()[2], post);
    }

    #[test]
    fn what_was_not_scripted_is_denied_and_reported() {
        let mock = MockHttp::new();
        mock.expect("GET", UrlMatch::exact("https://a.example/once"))
            .times(1);
        mock.expect("GET", UrlMatch::exact("https://a.example/never"));
        mock.expect("GET", UrlMatch::exact("https://a.example/none"))
            .times(0);
        let once = get("https://a.example/once");
        assert_eq!(status(mock.fetch(&once)), Ok(200));
        assert_eq!(status(mock.fetch(&once)), Err(HttpErrorKind::Denied));
        assert_eq!(
            status(mock.fetch(&get("https://a.example/other"))),
            Err(HttpErrorKind::Denied)
        );
        assert_eq!(
            mock.verify(),
            Err(vec![
                "GET https://a.example/never was requested 0 time(s), expected at least 1".into(),
                "no mock matched GET https://a.example/once".into(),
                "no mock matched GET https://a.example/other".into(),
            ])
        );
        assert_eq!(mock.requests().len(), 3, "unanswered requests are recorded");
    }

    #[test]
    fn a_predicate_may_read_the_mock_it_answers_for() {
        let mock = MockHttp::new();
        let seen = mock.clone();
        mock.expect(
            "GET",
            UrlMatch::matching(move |url| {
                url.ends_with("/retry") && seen.requests().iter().any(|r| r.url.ends_with("/first"))
            }),
        )
        .respond(200, &[], "after the first");
        assert_eq!(
            status(mock.fetch(&get("https://a.example/retry"))),
            Err(HttpErrorKind::Denied),
            "nothing was seen yet"
        );
        assert_eq!(
            status(mock.fetch(&get("https://a.example/first"))),
            Err(HttpErrorKind::Denied)
        );
        assert_eq!(status(mock.fetch(&get("https://a.example/retry"))), Ok(200));
        assert_eq!(mock.requests().len(), 3);
        assert!(mock.verify().is_err(), "two requests went unanswered");
    }
}
