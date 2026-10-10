//! NATS naming rules shared by the macros, the CLI and the platform.

pub const STREAM_NAME_MAX: usize = 64;
pub const CONSUMER_NAME_MAX: usize = 200;
/// The longest subject a message is published on. The server reads a
/// subject in its control line, which it holds to 4096 bytes and closes the
/// connection over.
pub const SUBJECT_MAX: usize = 1024;

/// `^[A-Z0-9_]{1,64}$`
pub fn is_valid_stream_name(name: &str) -> bool {
    (1..=STREAM_NAME_MAX).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// `^[A-Za-z0-9_-]{1,200}$`
pub fn is_valid_consumer_name(name: &str) -> bool {
    (1..=CONSUMER_NAME_MAX).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn is_literal_token(token: &str) -> bool {
    !token.is_empty()
        && token
            .chars()
            .all(|c| !c.is_whitespace() && !c.is_control() && !matches!(c, '*' | '>'))
}

/// Why `subject`, which `what` names, is longer than a subject can be.
fn over_length(what: &str, subject: &str) -> Option<String> {
    let len = subject.len();
    (len > SUBJECT_MAX).then(|| {
        format!("{what} of {len} bytes is longer than the {SUBJECT_MAX} bytes a message is published on")
    })
}

/// Why `subject` is not one a message is published on, if it is not: one
/// is dot-separated literal tokens, [`SUBJECT_MAX`] bytes at most.
pub fn subject_violation(subject: &str) -> Option<String> {
    over_length("a subject", subject).or_else(|| {
        (!subject.split('.').all(is_literal_token)).then(|| format!("'{subject}' is not a subject"))
    })
}

/// A subject a message is published on ([`subject_violation`]).
pub fn is_valid_subject(subject: &str) -> bool {
    subject_violation(subject).is_none()
}

/// Why `pattern` is not one a consumer filters on, if it is not: one is
/// literal tokens, `*` for one token and a final `>` for one or more,
/// [`SUBJECT_MAX`] bytes at most, as the subjects it matches are.
pub fn pattern_violation(pattern: &str) -> Option<String> {
    over_length("a subject pattern", pattern).or_else(|| {
        let tokens: Vec<&str> = pattern.split('.').collect();
        let well_formed = tokens
            .iter()
            .enumerate()
            .all(|(index, token)| match *token {
                "*" => true,
                ">" => index + 1 == tokens.len(),
                literal => is_literal_token(literal),
            });
        (!well_formed).then(|| format!("'{pattern}' is not a subject pattern"))
    })
}

/// A subject a consumer filters on ([`pattern_violation`]).
pub fn is_valid_subject_pattern(pattern: &str) -> bool {
    pattern_violation(pattern).is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_subject_is_literal_tokens_no_longer_than_the_server_reads() {
        let longest = format!("dw.acme.{}", "x".repeat(SUBJECT_MAX - "dw.acme.".len()));
        assert!(is_valid_subject(&longest));
        assert_eq!(
            subject_violation(&format!("{longest}x")).as_deref(),
            Some("a subject of 1025 bytes is longer than the 1024 bytes a message is published on")
        );
        for invalid in ["", "dw..x", "dw.*", "dw.>", "dw.a b", "dw.a\n"] {
            assert_eq!(
                subject_violation(invalid),
                Some(format!("'{invalid}' is not a subject")),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn stream_names() {
        for valid in [
            "BOOKS",
            "DW_DEFAULT",
            "A",
            "PROPERTY_JOBS_2",
            &"S".repeat(64),
        ] {
            assert!(is_valid_stream_name(valid), "{valid}");
        }
        for invalid in [
            "",
            "books",
            "BOOKS.X",
            "BOOKS-X",
            "LE ADS",
            "A*",
            &"S".repeat(65),
        ] {
            assert!(!is_valid_stream_name(invalid), "{invalid}");
        }
    }

    #[test]
    fn consumer_names() {
        for valid in [
            "loans-service-book-consumer",
            "stock_service_on_created",
            "A1",
            &"c".repeat(200),
        ] {
            assert!(is_valid_consumer_name(valid), "{valid}");
        }
        for invalid in ["", "a.b", "a*", "a>", "a b", "é", &"c".repeat(201)] {
            assert!(!is_valid_consumer_name(invalid), "{invalid}");
        }
    }

    #[test]
    fn subjects_and_patterns() {
        for valid in ["books.created", "dw.default.items.created", "a"] {
            assert!(is_valid_subject(valid), "{valid}");
            assert!(is_valid_subject_pattern(valid), "{valid}");
        }
        for invalid in ["", "a..b", ".a", "a.", "a b", "a.\tb"] {
            assert!(!is_valid_subject(invalid), "{invalid:?}");
            assert!(!is_valid_subject_pattern(invalid), "{invalid:?}");
        }
        for pattern in ["books.*", "books.>", "*.created", ">", "*"] {
            assert!(!is_valid_subject(pattern), "{pattern}");
            assert!(is_valid_subject_pattern(pattern), "{pattern}");
        }
        for invalid in ["books.>.x", "books.a*", "books.>x", "lea*ds"] {
            assert_eq!(
                pattern_violation(invalid),
                Some(format!("'{invalid}' is not a subject pattern")),
                "{invalid}"
            );
        }
    }

    #[test]
    fn a_pattern_is_no_longer_than_the_subjects_it_matches() {
        let longest = format!("dw.*.{}.>", "x".repeat(SUBJECT_MAX - "dw.*..>".len()));
        assert_eq!(longest.len(), SUBJECT_MAX);
        assert_eq!(pattern_violation(&longest), None);
        assert_eq!(
            pattern_violation(&format!("x{longest}")).as_deref(),
            Some(
                "a subject pattern of 1025 bytes is longer than the 1024 bytes a message is \
                 published on"
            )
        );
    }
}
