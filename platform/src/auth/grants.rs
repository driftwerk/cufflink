pub use cufflink_types::grants::{claim, parse, parse_scoped, Grant, GrantError, Scope};
use std::collections::HashSet;

pub fn has_permission(permissions: &[String], area: &str, operation: &str) -> bool {
    permissions
        .iter()
        .filter_map(|p| parse(p).ok())
        .any(|g| g.covers(area, operation))
}

pub fn validate_scoped(permissions: &[String]) -> Result<(), Vec<(String, GrantError)>> {
    let invalid: Vec<(String, GrantError)> = permissions
        .iter()
        .filter(|p| p.contains('@'))
        .filter_map(|p| parse(p).err().map(|e| (p.clone(), e)))
        .collect();
    if invalid.is_empty() {
        Ok(())
    } else {
        Err(invalid)
    }
}

pub fn describe(invalid: &[(String, GrantError)]) -> String {
    invalid
        .iter()
        .map(|(permission, error)| format!("permission '{permission}': {error}"))
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GuestPermissions {
    pub permissions: Vec<String>,
    pub scoped: Vec<String>,
}

pub fn guest_view(permissions: &[String]) -> GuestPermissions {
    let scoped: Vec<String> = permissions
        .iter()
        .filter(|p| parse_scoped(p).is_some())
        .cloned()
        .collect();
    let mut seen = HashSet::new();
    let permissions = permissions
        .iter()
        .map(|p| parse_scoped(p).map_or(p.as_str(), |grant| grant.base))
        .filter(|p| seen.insert(*p))
        .map(str::to_string)
        .collect();
    GuestPermissions {
        permissions,
        scoped,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }

    fn pre_scope_has_permission(permissions: &[String], area: &str, operation: &str) -> bool {
        let required = format!("{}:{}", area, operation);
        let wildcard = format!("{}:*", area);
        permissions
            .iter()
            .any(|p| p == &required || p == &wildcard || p == "*")
    }

    fn corpus() -> Vec<String> {
        const PARTS: &[&str] = &[
            "",
            "*",
            ":",
            "items",
            "view",
            "Items",
            "VIEW",
            " ",
            ".",
            "a:b:c",
            "é",
            "日本",
            "\t",
            "::",
            "*:*",
            "items:",
            ":view",
            "-",
            "items:view ",
        ];
        PARTS
            .iter()
            .flat_map(|a| {
                PARTS
                    .iter()
                    .flat_map(move |b| PARTS.iter().map(move |c| format!("{a}{b}{c}")))
            })
            .collect()
    }

    #[test]
    fn strings_without_a_scope_behave_exactly_as_before() {
        let corpus = corpus();
        let queries = [
            ("items", "view"),
            ("items", "*"),
            ("Items", "VIEW"),
            ("", ""),
            ("a", "b:c"),
            ("a:b", "c"),
            ("é", "日本"),
            ("*", "*"),
            ("items:view", ""),
        ];
        for permission in &corpus {
            let set = vec![permission.clone()];
            for (area, operation) in queries {
                assert_eq!(
                    has_permission(&set, area, operation),
                    pre_scope_has_permission(&set, area, operation),
                    "{permission:?} vs {area}:{operation}"
                );
            }
        }
        let view = guest_view(&corpus);
        let mut unique = corpus.clone();
        let mut seen = HashSet::new();
        unique.retain(|p| seen.insert(p.clone()));
        assert_eq!(view.permissions, unique);
        assert!(view.scoped.is_empty());
        assert!(validate_scoped(&corpus).is_ok());
    }

    #[test]
    fn a_valid_scope_also_grants_the_unscoped_permission() {
        for scoped in ["items:view@all", "items:view@own", "items:*@field(org=org)"] {
            assert!(
                has_permission(&strings(&[scoped]), "items", "view"),
                "{scoped}"
            );
        }
        assert!(!has_permission(
            &strings(&["items:view@everything"]),
            "items",
            "view"
        ));
        assert!(!has_permission(
            &strings(&["items:view@all"]),
            "items",
            "edit"
        ));
    }

    #[test]
    fn guests_see_bases_and_the_scoped_originals() {
        let view = guest_view(&strings(&[
            "a:x",
            "items:view@all",
            "items:view",
            "items:edit@field(org=org)",
            "items:edit@own",
            "broken@nope",
        ]));
        assert_eq!(
            view.permissions,
            strings(&["a:x", "items:view", "items:edit", "broken@nope"])
        );
        assert_eq!(
            view.scoped,
            strings(&[
                "items:view@all",
                "items:edit@field(org=org)",
                "items:edit@own"
            ])
        );
    }

    #[test]
    fn validation_names_every_malformed_scope_only() {
        let err = validate_scoped(&strings(&[
            "items:view",
            "items:view@all",
            "items:view@nope",
            "@own",
            "odd string with spaces",
        ]))
        .expect_err("malformed");
        assert_eq!(
            err,
            vec![
                (
                    "items:view@nope".to_string(),
                    GrantError::InvalidScope("nope".into())
                ),
                ("@own".to_string(), GrantError::EmptyBase),
            ]
        );
        assert_eq!(
            describe(&err),
            "permission 'items:view@nope': invalid scope 'nope' (expected own, all or field(column=claim)); \
             permission '@own': a scoped permission needs a permission before '@'"
        );
    }
}
