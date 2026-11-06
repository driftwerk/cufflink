//! The golden corpus: statements accepted and refused, and the answer this
//! release gives each, committed so that every implementation of the parse
//! (the parser process, the platform, the test host) is checked against
//! fixed bytes rather than against itself.
//!
//! An answer is a tree's canonical text, or `!` and the refusal's kind
//! (`!syntax`, `!too_large`, `!unavailable`).

use cufflink_types::sql_parse::ParseError;

/// The statements past the probe corpus, one a line.
pub const STATEMENTS: &str = include_str!("../golden/statements.sql");

/// The answer to each statement of [`cases`], one a line, in order.
pub const ANSWERS: &str = include_str!("../golden/answers.txt");

/// The probe corpus's statements, then [`STATEMENTS`].
pub fn statements() -> impl Iterator<Item = &'static str> {
    crate::PROBE
        .lines()
        .chain(STATEMENTS.lines())
        .filter(|line| !line.trim().is_empty())
}

/// Each statement with its committed answer.
pub fn cases() -> impl Iterator<Item = (&'static str, &'static str)> {
    statements().zip(ANSWERS.lines())
}

/// A parse's answer as the corpus writes it.
pub fn answer(parsed: &Result<String, ParseError>) -> String {
    match parsed {
        Ok(text) => text.clone(),
        Err(error) => format!("!{}", kind(error)),
    }
}

/// A refusal's kind, as the wire names it.
pub fn kind(error: &ParseError) -> &'static str {
    match error {
        ParseError::Syntax(_) => "syntax",
        ParseError::TooLarge(_) => "too_large",
        ParseError::Unavailable(_) => "unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_statement_has_one_answer() {
        assert_eq!(statements().count(), ANSWERS.lines().count());
        assert!(cases()
            .all(|(_, answer)| answer.starts_with('{')
                || ["!syntax", "!too_large"].contains(&answer)));
        assert!(cases().any(|(_, answer)| answer == "!syntax"));
        assert!(cases().any(|(_, answer)| answer == "!too_large"));
    }
}
