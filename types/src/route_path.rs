//! Route path patterns of `routing: strict` services: parsed once here and
//! shared by the `service!` macro, the deploy validator, the platform's route
//! table and the OpenAPI generator.

use std::collections::BTreeSet;
use std::fmt;

/// First path segments the platform serves itself under every service, which a
/// strict route may not claim.
pub const RESERVED_FIRST_SEGMENTS: [&str; 4] = ["_fn", "_meta", "_jobs", "graphql"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    Literal(String),
    Param(String),
    CatchAll(String),
}

/// A parsed route path such as `/books/{book_id}/notes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePattern {
    segments: Vec<Segment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutePathError {
    NotAbsolute,
    EmptySegment,
    InvalidParamName(String),
    DuplicateParam(String),
    CatchAllNotLast,
    ReservedPrefix(String),
    InvalidLiteral(String),
}

impl fmt::Display for RoutePathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotAbsolute => write!(f, "a route path must start with '/'"),
            Self::EmptySegment => write!(f, "a route path may not contain an empty segment"),
            Self::InvalidParamName(name) => {
                write!(f, "path parameter '{name}' must match [a-z_][a-z0-9_]*")
            }
            Self::DuplicateParam(name) => {
                write!(f, "path parameter '{name}' appears more than once")
            }
            Self::CatchAllNotLast => {
                write!(f, "a catch-all parameter must be the last segment")
            }
            Self::ReservedPrefix(prefix) => write!(
                f,
                "the first segment '{prefix}' is reserved by the platform"
            ),
            Self::InvalidLiteral(literal) => write!(
                f,
                "path segment '{literal}' may contain only letters, digits and . _ ~ -"
            ),
        }
    }
}

impl std::error::Error for RoutePathError {}

impl RoutePattern {
    /// Accepts `{name}` and the legacy `:name`; `{*rest}` only as the last
    /// segment. `/` alone is the service root.
    pub fn parse(path: &str) -> Result<Self, RoutePathError> {
        let rest = path.strip_prefix('/').ok_or(RoutePathError::NotAbsolute)?;
        let raw: Vec<&str> = if rest.is_empty() {
            Vec::new()
        } else {
            rest.split('/').collect()
        };
        let last = raw.len().saturating_sub(1);
        let segments = raw
            .iter()
            .enumerate()
            .map(|(index, raw)| parse_segment(raw, index == last))
            .collect::<Result<Vec<_>, _>>()?;
        let pattern = Self { segments };
        pattern.check_unique_params()?;
        match pattern.first_literal() {
            Some(first) if RESERVED_FIRST_SEGMENTS.contains(&first) => {
                Err(RoutePathError::ReservedPrefix(first.to_string()))
            }
            _ => Ok(pattern),
        }
    }

    fn check_unique_params(&self) -> Result<(), RoutePathError> {
        let mut seen = BTreeSet::new();
        self.params()
            .find(|name| !seen.insert(*name))
            .map_or(Ok(()), |name| {
                Err(RoutePathError::DuplicateParam(name.to_string()))
            })
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Parameter names in path order, the catch-all included.
    pub fn params(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().filter_map(|segment| match segment {
            Segment::Param(name) | Segment::CatchAll(name) => Some(name.as_str()),
            Segment::Literal(_) => None,
        })
    }

    /// The pattern in `matchit` syntax: `/books/{book_id}/close`.
    pub fn to_matchit(&self) -> String {
        self.render(|name| format!("{{*{name}}}"))
    }

    /// The pattern as an OpenAPI path template, the catch-all as `{rest}`.
    pub fn to_openapi(&self) -> String {
        self.render(|name| format!("{{{name}}}"))
    }

    fn render(&self, catch_all: impl Fn(&str) -> String) -> String {
        if self.segments.is_empty() {
            return "/".to_string();
        }
        self.segments
            .iter()
            .map(|segment| match segment {
                Segment::Literal(literal) => format!("/{literal}"),
                Segment::Param(name) => format!("/{{{name}}}"),
                Segment::CatchAll(name) => format!("/{}", catch_all(name)),
            })
            .collect()
    }

    /// The first segment when it is a literal.
    pub fn first_literal(&self) -> Option<&str> {
        match self.segments.first() {
            Some(Segment::Literal(literal)) => Some(literal),
            _ => None,
        }
    }
}

fn parse_segment(raw: &str, last: bool) -> Result<Segment, RoutePathError> {
    if raw.is_empty() {
        return Err(RoutePathError::EmptySegment);
    }
    let param = raw
        .strip_prefix('{')
        .and_then(|inner| inner.strip_suffix('}'))
        .or_else(|| raw.strip_prefix(':'));
    match param {
        Some(inner) => match inner.strip_prefix('*') {
            Some(_) if !last => Err(RoutePathError::CatchAllNotLast),
            Some(name) if raw.starts_with('{') => param_name(name).map(Segment::CatchAll),
            Some(_) => Err(RoutePathError::InvalidParamName(inner.to_string())),
            None => param_name(inner).map(Segment::Param),
        },
        None if raw.bytes().all(is_literal_byte) => Ok(Segment::Literal(raw.to_string())),
        None => Err(RoutePathError::InvalidLiteral(raw.to_string())),
    }
}

fn param_name(name: &str) -> Result<String, RoutePathError> {
    let mut bytes = name.bytes();
    let valid = bytes
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
        && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if valid {
        Ok(name.to_string())
    } else {
        Err(RoutePathError::InvalidParamName(name.to_string()))
    }
}

fn is_literal_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'~' | b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(s: &str) -> Segment {
        Segment::Literal(s.into())
    }

    fn param(s: &str) -> Segment {
        Segment::Param(s.into())
    }

    #[test]
    fn parses_literals_params_and_a_trailing_catch_all() {
        for (path, segments) in [
            ("/", vec![]),
            ("/books", vec![lit("books")]),
            (
                "/books/{book_id}/due-date",
                vec![lit("books"), param("book_id"), lit("due-date")],
            ),
            (
                "/books/:book_id/close",
                vec![lit("books"), param("book_id"), lit("close")],
            ),
            (
                "/files/{*rest}",
                vec![lit("files"), Segment::CatchAll("rest".into())],
            ),
            ("/{_id}/v1.2~x", vec![param("_id"), lit("v1.2~x")]),
        ] {
            assert_eq!(
                RoutePattern::parse(path).map(|p| p.segments),
                Ok(segments),
                "{path}"
            );
        }
    }

    #[test]
    fn rejects_malformed_paths_with_the_reason() {
        for (path, error) in [
            ("books", RoutePathError::NotAbsolute),
            ("", RoutePathError::NotAbsolute),
            ("/books/", RoutePathError::EmptySegment),
            ("//books", RoutePathError::EmptySegment),
            (
                "/books/{Book}",
                RoutePathError::InvalidParamName("Book".into()),
            ),
            ("/books/{1d}", RoutePathError::InvalidParamName("1d".into())),
            ("/books/{}", RoutePathError::InvalidParamName("".into())),
            (
                "/books/:*rest",
                RoutePathError::InvalidParamName("*rest".into()),
            ),
            (
                "/a/{id}/b/{id}",
                RoutePathError::DuplicateParam("id".into()),
            ),
            ("/a/{id}/{*id}", RoutePathError::DuplicateParam("id".into())),
            ("/files/{*rest}/x", RoutePathError::CatchAllNotLast),
            ("/_fn/x", RoutePathError::ReservedPrefix("_fn".into())),
            ("/_meta", RoutePathError::ReservedPrefix("_meta".into())),
            ("/_jobs/x", RoutePathError::ReservedPrefix("_jobs".into())),
            ("/graphql", RoutePathError::ReservedPrefix("graphql".into())),
            ("/books/a b", RoutePathError::InvalidLiteral("a b".into())),
            (
                "/books/{id}x",
                RoutePathError::InvalidLiteral("{id}x".into()),
            ),
            ("/books/%20", RoutePathError::InvalidLiteral("%20".into())),
        ] {
            assert_eq!(RoutePattern::parse(path), Err(error), "{path}");
        }
    }

    #[test]
    fn renders_matchit_and_openapi_templates() {
        let pattern = RoutePattern::parse("/files/:bucket/{*rest}").expect("parses");
        assert_eq!(pattern.to_matchit(), "/files/{bucket}/{*rest}");
        assert_eq!(pattern.to_openapi(), "/files/{bucket}/{rest}");
        assert_eq!(pattern.params().collect::<Vec<_>>(), ["bucket", "rest"]);
        assert_eq!(pattern.first_literal(), Some("files"));
        let root = RoutePattern::parse("/").expect("root");
        assert_eq!(
            (root.to_matchit(), root.to_openapi()),
            ("/".into(), "/".into())
        );
        assert_eq!(
            RoutePattern::parse("/{id}").expect("param").first_literal(),
            None
        );
    }

    #[test]
    fn errors_read_as_sentences() {
        assert_eq!(
            RoutePathError::ReservedPrefix("_fn".into()).to_string(),
            "the first segment '_fn' is reserved by the platform"
        );
    }
}
