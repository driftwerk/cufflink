//! Permission strings: the single owner of their grammar, shared by the
//! platform and the guest SDK. Only the scope suffix has a grammar; the part
//! before `@` is compared exactly as before scopes existed, so no role
//! string without `@` changes meaning.
//!
//! ```text
//! grant := base [ "@" scope ]
//! scope := "own" | "all" | "field(" column "=" claim ")"
//! ```

use serde_json::Value;
use std::collections::HashMap;
use std::fmt;

/// Which rows a grant reaches on a table that opts into scoped permissions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    /// The caller's own rows.
    Own,
    /// Rows whose `column` holds the value of the caller's claim `claim`.
    Field { column: String, claim: String },
    /// Every row.
    All,
}

/// A parsed permission string. A string without `@` is an `Own` grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant<'a> {
    pub base: &'a str,
    pub scope: Scope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantError {
    EmptyBase,
    InvalidScope(String),
}

impl fmt::Display for GrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantError::EmptyBase => {
                f.write_str("a scoped permission needs a permission before '@'")
            }
            GrantError::InvalidScope(scope) => write!(
                f,
                "invalid scope '{scope}' (expected own, all or field(column=claim))"
            ),
        }
    }
}

impl std::error::Error for GrantError {}

pub fn parse(s: &str) -> Result<Grant<'_>, GrantError> {
    let Some((base, suffix)) = s.split_once('@') else {
        return Ok(Grant {
            base: s,
            scope: Scope::Own,
        });
    };
    if base.is_empty() {
        return Err(GrantError::EmptyBase);
    }
    parse_scope(suffix)
        .map(|scope| Grant { base, scope })
        .ok_or_else(|| GrantError::InvalidScope(suffix.to_string()))
}

/// A string carrying a well-formed scope suffix, parsed; `None` for a
/// string without `@` or with a malformed suffix.
pub fn parse_scoped(s: &str) -> Option<Grant<'_>> {
    s.contains('@').then(|| parse(s).ok()).flatten()
}

fn parse_scope(suffix: &str) -> Option<Scope> {
    match suffix {
        "own" => Some(Scope::Own),
        "all" => Some(Scope::All),
        _ => {
            let (column, claim) = suffix
                .strip_prefix("field(")?
                .strip_suffix(')')?
                .split_once('=')?;
            (is_column(column) && is_claim(claim)).then(|| Scope::Field {
                column: column.to_string(),
                claim: claim.to_string(),
            })
        }
    }
}

fn is_column(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_claim(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | ':' | '-'))
}

/// Whether a permission (without its scope) grants `{area}:{operation}`:
/// exactly, as `{area}:*`, or as `*`.
pub fn covers(base: &str, area: &str, operation: &str) -> bool {
    base == "*"
        || base
            .strip_prefix(area)
            .and_then(|rest| rest.strip_prefix(':'))
            .is_some_and(|granted| granted == operation || granted == "*")
}

impl Grant<'_> {
    pub fn covers(&self, area: &str, operation: &str) -> bool {
        covers(self.base, area, operation)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::Own => f.write_str("own"),
            Scope::All => f.write_str("all"),
            Scope::Field { column, claim } => write!(f, "field({column}={claim})"),
        }
    }
}

/// A token claim by name: the claim of exactly that name, else the dotted
/// path through nested objects (Keycloak nests a mapper named `org.id` as
/// `{"org": {"id": …}}`).
pub fn claim<'a>(claims: &'a HashMap<String, Value>, name: &str) -> Option<&'a Value> {
    claims.get(name).or_else(|| {
        let mut path = name.split('.');
        let root = claims.get(path.next()?)?;
        path.try_fold(root, |value, segment| value.get(segment))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The scope-suffix grammar: each string and its `(base, scope)`.
    const GRAMMAR: &[(&str, Option<(&str, &str)>)] = &[
        ("books:view", Some(("books:view", "own"))),
        ("books:view@own", Some(("books:view", "own"))),
        ("books:view@all", Some(("books:view", "all"))),
        (
            "books:view@field(branch_id=branch_id)",
            Some(("books:view", "field(branch_id=branch_id)")),
        ),
        (
            "books:*@field(_org2=realm_access.org:id-1)",
            Some(("books:*", "field(_org2=realm_access.org:id-1)")),
        ),
        ("*@all", Some(("*", "all"))),
        ("books:view@", None),
        ("books:view@All", None),
        ("books:view@own ", None),
        ("books:view@all@all", None),
        ("books:view@field()", None),
        ("books:view@field(org=)", None),
        ("books:view@field(=org)", None),
        ("books:view@field(Org=org)", None),
        ("books:view@field(1org=org)", None),
        ("books:view@field(org=a b)", None),
        ("books:view@field(org=org", None),
        ("books:view@field(org=org))", None),
        ("books:view@field(org=org=x)", None),
        ("@all", None),
    ];

    #[test]
    fn scope_suffix_grammar() {
        for (input, expected) in GRAMMAR {
            let parsed = parse(input)
                .ok()
                .map(|g| (g.base.to_string(), g.scope.to_string()));
            let expected = expected.map(|(b, s)| (b.to_string(), s.to_string()));
            assert_eq!(parsed, expected, "{input}");
            assert_eq!(
                parse_scoped(input).is_some(),
                input.contains('@') && expected.is_some(),
                "{input}"
            );
        }
        assert_eq!(parse("@all"), Err(GrantError::EmptyBase));
        assert_eq!(
            parse("x@field(org=org").map(|g| g.scope),
            Err(GrantError::InvalidScope("field(org=org".into()))
        );
        assert_eq!(
            parse("x@field(a=b)").map(|g| g.scope),
            Ok(Scope::Field {
                column: "a".into(),
                claim: "b".into()
            })
        );
    }

    #[test]
    fn a_base_covers_its_operation_its_area_wildcard_or_everything() {
        for (base, area, operation, expected) in [
            ("books:view", "books", "view", true),
            ("books:*", "books", "edit", true),
            ("*", "anything", "at_all", true),
            ("books:view", "books", "edit", false),
            ("books", "books", "view", false),
            ("books:view:x", "books", "view", false),
            ("a:b:c", "a:b", "c", true),
            ("a:b:c", "a", "b:c", true),
            ("books:*", "books", "*", true),
            ("booksx:view", "books", "view", false),
            ("", "", "", false),
            (":", "", "", true),
        ] {
            assert_eq!(
                covers(base, area, operation),
                expected,
                "{base} {area}:{operation}"
            );
        }
    }

    #[test]
    fn claims_resolve_by_name_then_by_dotted_path() {
        let claims: HashMap<String, Value> = [
            ("branch_id".to_string(), json!("org-1")),
            ("org".to_string(), json!({"id": "org-2", "team": {"id": 7}})),
            ("org.id".to_string(), json!("flat")),
            ("tags".to_string(), json!(["a", "b"])),
        ]
        .into_iter()
        .collect();
        assert_eq!(claim(&claims, "branch_id"), Some(&json!("org-1")));
        assert_eq!(claim(&claims, "org.id"), Some(&json!("flat")));
        assert_eq!(claim(&claims, "org.team.id"), Some(&json!(7)));
        assert_eq!(claim(&claims, "tags"), Some(&json!(["a", "b"])));
        for missing in [
            "org.missing",
            "org.team.id.more",
            "nope",
            "nope.id",
            "tags.0",
        ] {
            assert_eq!(claim(&claims, missing), None, "{missing}");
        }
    }
}
