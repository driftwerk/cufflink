use async_nats::{HeaderMap, HeaderName, HeaderValue};
use cufflink_types::host::nats::header_violation;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Unsendable(String);

pub fn parsed(name: &str, value: &str) -> Result<(HeaderName, HeaderValue), Unsendable> {
    if let Some(reason) = header_violation(name, value) {
        return Err(Unsendable(reason));
    }
    let unsendable = |reason: &dyn std::fmt::Display| {
        Unsendable(format!("header {name:?} cannot be sent: {reason}"))
    };
    Ok((
        HeaderName::from_str(name).map_err(|e| unsendable(&e))?,
        HeaderValue::from_str(value).map_err(|e| unsendable(&e))?,
    ))
}

pub fn appended(mut headers: HeaderMap, name: &str, value: &str) -> Result<HeaderMap, Unsendable> {
    let (name, value) = parsed(name, value)?;
    headers.append(name, value);
    Ok(headers)
}

pub fn replaced(mut headers: HeaderMap, name: &str, value: &str) -> Result<HeaderMap, Unsendable> {
    let (name, value) = parsed(name, value)?;
    headers.insert(name, value);
    Ok(headers)
}

pub fn wire_len(headers: &HeaderMap) -> usize {
    const VERSION_LINE: usize = b"NATS/1.0\r\n".len();
    const LINE_END: usize = b"\r\n".len();
    const SEPARATOR: usize = b": ".len();
    if headers.is_empty() {
        return 0;
    }
    headers
        .iter()
        .flat_map(|(name, values)| {
            let name = AsRef::<str>::as_ref(name).len();
            values
                .iter()
                .map(move |value| name + SEPARATOR + value.as_str().len() + LINE_END)
        })
        .sum::<usize>()
        + VERSION_LINE
        + LINE_END
}

#[cfg(test)]
pub(crate) fn msg_id_unhidden(headers: &HeaderMap) -> bool {
    use cufflink_types::host::nats::{names_msg_id, MSG_ID_HEADER};
    headers.iter().all(|(name, values)| {
        let name: &str = name.as_ref();
        name == MSG_ID_HEADER
            || !names_msg_id(name) && values.iter().all(|value| !names_msg_id(value.as_str()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(headers: &HeaderMap, name: &str) -> Vec<String> {
        headers
            .get_all(name)
            .map(|value| value.to_string())
            .collect()
    }

    #[test]
    fn what_parses_is_carried() {
        let headers = appended(HeaderMap::new(), "x-source", "a")
            .and_then(|headers| appended(headers, "x-source", "b"))
            .and_then(|headers| appended(headers, "Nats-Msg-Id", "book-7"))
            .and_then(|headers| replaced(headers, "Nats-Msg-Id", "book-8"))
            .expect("headers");
        assert_eq!(values(&headers, "x-source"), ["a", "b"]);
        assert_eq!(values(&headers, "Nats-Msg-Id"), ["book-8"]);
    }

    #[test]
    fn what_the_client_would_assert_is_an_error() {
        for (name, value) in [
            ("", "v"),
            ("x bad", "v"),
            ("x:bad", "v"),
            ("x\rbad", "v"),
            ("x\nbad", "v"),
            ("x\0bad", "v"),
            ("naïve", "v"),
            ("x-ok", "a\r\nNats-Expected-Stream: OTHER"),
            ("x-ok", "a\nb"),
            ("x-ok", "a\0b"),
        ] {
            for built in [appended, replaced] {
                let refused = built(HeaderMap::new(), name, value);
                assert!(refused.is_err(), "{name:?}: {value:?}");
            }
        }
        let big = "x".repeat(4 * 1024 * 1024);
        assert!(
            appended(HeaderMap::new(), "x-big", &big).is_ok(),
            "its size is the server's to refuse"
        );
    }

    #[test]
    fn headers_weigh_what_they_take_on_the_wire() {
        assert_eq!(wire_len(&HeaderMap::new()), 0);
        let headers = appended(HeaderMap::new(), "a", "1")
            .and_then(|headers| appended(headers, "a", "22"))
            .and_then(|headers| appended(headers, "bc", ""))
            .expect("headers");
        assert_eq!(
            wire_len(&headers),
            "NATS/1.0\r\na: 1\r\na: 22\r\nbc: \r\n\r\n".len()
        );
    }
}
