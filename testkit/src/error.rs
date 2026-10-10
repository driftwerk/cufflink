use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum TestkitError {
    #[error("{0}")]
    Unavailable(String),
    #[error("database: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool: {0}")]
    Pool(String),
    #[error("request: {0}")]
    Http(#[from] reqwest::Error),
    #[error("NATS: {0}")]
    Nats(String),
    #[error("token: {0}")]
    Token(#[from] jsonwebtoken::errors::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("platform: {0}")]
    Platform(String),
    #[error("{what} answered {status}: {body}")]
    Status {
        what: String,
        status: u16,
        body: Value,
    },
    #[error("never: {0}")]
    Never(String),
}

pub fn unless_absent<T>(result: Result<T, TestkitError>) -> Result<Option<T>, TestkitError> {
    match result {
        Err(TestkitError::Unavailable(reason)) if !crate::infra::skips_are_failures() => {
            crate::infra::report_skip(&reason);
            Ok(None)
        }
        other => other.map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_absent_is_told_from_what_failed() {
        assert_eq!(unless_absent(Ok(7)).ok(), Some(Some(7)));
        let failed = unless_absent::<u8>(Err(TestkitError::Platform("it stopped".into())));
        assert!(
            matches!(failed, Err(TestkitError::Platform(_))),
            "{failed:?}"
        );
        let absent = unless_absent::<u8>(Err(TestkitError::Unavailable("no NATS".into())));
        match crate::infra::skips_are_failures() {
            true => assert!(matches!(absent, Err(TestkitError::Unavailable(_)))),
            false => assert!(matches!(absent, Ok(None))),
        }
    }

    #[test]
    fn an_answer_that_was_not_asked_for_says_what_was() {
        let refused = TestkitError::Status {
            what: "creating the tenant".into(),
            status: 409,
            body: serde_json::json!({"error": "Tenant 'acme' already exists"}),
        };
        assert_eq!(
            refused.to_string(),
            r#"creating the tenant answered 409: {"error":"Tenant 'acme' already exists"}"#
        );
    }
}
