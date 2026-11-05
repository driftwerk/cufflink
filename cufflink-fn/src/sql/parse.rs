use cufflink_types::sql_parse::{ParseError, ParseResponse, Parsed};

/// PostgreSQL's parse tree of `sql`, one or more statements, for a guest
/// that has to know what SQL it did not write does before running it.
/// Needs a platform that supports `sql_parse`; the `sql-parse` feature
/// declares that in the module.
///
/// The tree is `pg_query`'s protobuf as serde writes it, from the
/// `pg_query` release [`Parsed::parser`] names; [`Parsed::fingerprint`]
/// changes whenever a release reads its probe corpus differently, so a
/// guest that matches trees can pin what it was written against.
///
/// No error quotes `sql`. A statement past the platform's bounds (1 MiB,
/// nested deeper than the parser is allowed to go, or a tree nested
/// deeper than 120 levels, which a guest's JSON reader cannot take) is
/// [`ParseError::TooLarge`]; outside the platform, without a test host,
/// every call is [`ParseError::Unavailable`].
///
/// ```rust,ignore
/// let parsed = sql::parse("SELECT id FROM orders WHERE total > 10")?;
/// let only_selects = parsed.tree["stmts"]
///     .as_array()
///     .is_some_and(|stmts| stmts.iter().all(|s| s["stmt"]["node"].get("SelectStmt").is_some()));
/// ```
pub fn parse(sql: &str) -> Result<Parsed, ParseError> {
    exchange(sql).into()
}

#[cfg(target_arch = "wasm32")]
fn exchange(sql: &str) -> ParseResponse {
    #[link(wasm_import_module = "env")]
    extern "C" {
        fn sql_parse(ptr: i32, len: i32) -> i32;
    }
    let Ok(len) = i32::try_from(sql.len()) else {
        return ParseResponse::Error(too_large());
    };
    if unsafe { sql_parse(sql.as_ptr() as i32, len) } < 0 {
        return ParseResponse::Error(too_large());
    }
    crate::host_buffer::read_host_json().unwrap_or_else(|| {
        ParseResponse::Error(ParseError::Unavailable(
            "malformed response from the platform".into(),
        ))
    })
}

/// The platform refuses to read a statement, or to hold its tree, past
/// the handler's limits.
#[cfg(target_arch = "wasm32")]
fn too_large() -> ParseError {
    ParseError::TooLarge(
        "the statement or its tree is more than the platform holds for the handler".into(),
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn exchange(sql: &str) -> ParseResponse {
    crate::host::sql_parse().map_or_else(
        || {
            ParseResponse::Error(ParseError::Unavailable(
                "the SQL parser is only available inside the Cufflink platform".into(),
            ))
        },
        |host| host.parse(sql),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outside_the_platform_the_parser_is_unavailable() {
        assert_eq!(
            parse("SELECT 1"),
            Err(ParseError::Unavailable(
                "the SQL parser is only available inside the Cufflink platform".into()
            ))
        );
    }
}
