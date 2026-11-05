//! The `sql_parse` host function's answer: PostgreSQL's parse tree of a
//! statement, and the identity of the parser that made it.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A statement's parse tree, as `pg_query`'s protobuf serialised by serde.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parsed {
    pub tree: Value,
    /// The `pg_query` release that parsed it.
    pub parser: String,
    /// The SHA-256 of the release's probe corpus as this parser reads it.
    pub fingerprint: String,
}

/// Why a statement has no tree. No message quotes the statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum ParseError {
    Syntax(String),
    TooLarge(String),
    Unavailable(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Syntax(message) => write!(f, "syntax: {message}"),
            Self::TooLarge(message) => write!(f, "too large: {message}"),
            Self::Unavailable(message) => write!(f, "unavailable: {message}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl ParseError {
    pub fn syntax() -> Self {
        Self::Syntax("the statement is not valid SQL".into())
    }

    pub fn unavailable() -> Self {
        Self::Unavailable("the SQL parser is unavailable".into())
    }
}

/// What the host function answers: `{"ok": {…}}` or `{"error": {…}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseResponse {
    Ok(Parsed),
    Error(ParseError),
}

impl From<Result<Parsed, ParseError>> for ParseResponse {
    fn from(result: Result<Parsed, ParseError>) -> Self {
        result.map_or_else(Self::Error, Self::Ok)
    }
}

impl From<ParseResponse> for Result<Parsed, ParseError> {
    fn from(response: ParseResponse) -> Self {
        match response {
            ParseResponse::Ok(parsed) => Ok(parsed),
            ParseResponse::Error(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn answers_match_the_wire_shapes() {
        let parsed = Parsed {
            tree: json!({"version": 170007, "stmts": []}),
            parser: "6.2.1".into(),
            fingerprint: "ab".into(),
        };
        for (result, wire) in [
            (
                Ok(parsed.clone()),
                json!({"ok": {"tree": {"version": 170007, "stmts": []}, "parser": "6.2.1", "fingerprint": "ab"}}),
            ),
            (
                Err(ParseError::syntax()),
                json!({"error": {"kind": "syntax", "message": "the statement is not valid SQL"}}),
            ),
            (
                Err(ParseError::TooLarge("over".into())),
                json!({"error": {"kind": "too_large", "message": "over"}}),
            ),
            (
                Err(ParseError::unavailable()),
                json!({"error": {"kind": "unavailable", "message": "the SQL parser is unavailable"}}),
            ),
        ] {
            let response = ParseResponse::from(result.clone());
            assert_eq!(serde_json::to_value(&response).expect("encode"), wire);
            let back: ParseResponse = serde_json::from_value(wire).expect("decode");
            assert_eq!(Result::<Parsed, ParseError>::from(back), result);
        }
        assert!(serde_json::from_value::<ParseResponse>(
            json!({"error": {"kind": "fault", "message": "x"}})
        )
        .is_err());
    }
}
