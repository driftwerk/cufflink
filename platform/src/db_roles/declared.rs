use crate::db_roles::names::{self, MANAGED_PREFIX};
use cufflink_types::manifest::validate::is_lowercase_identifier;

const STEM: char = '*';

fn stem(entry: &str) -> Option<&str> {
    entry.strip_suffix(STEM)
}

/// Whether an entry of `declared_roles` admits `role`: an exact name, or a
/// stem followed by `*` that begins it.
pub fn admits(entries: &[String], role: &str) -> bool {
    entries.iter().any(|entry| match stem(entry) {
        Some(stem) => role.starts_with(stem),
        None => entry == role,
    })
}

pub fn refusal_of_entry(entry: &str) -> Option<String> {
    let stemmed = stem(entry);
    let name = stemmed.unwrap_or(entry);
    let platforms =
        names::is_managed_name(name) || (stemmed.is_some() && MANAGED_PREFIX.starts_with(name));
    match (stemmed, is_lowercase_identifier(name)) {
        (Some(""), _) => Some(format!(
            "declared_roles entry '{entry}' would admit every role; name the roles or a stem of them"
        )),
        (_, false) => Some(format!(
            "declared_roles entry '{entry}' must be a role name of lowercase letters, digits and '_', \
             not starting with a digit, or such a name followed by '*'"
        )),
        (_, true) if platforms => Some(format!(
            "declared_roles entry '{entry}' would admit the platform's own roles, whose names begin with '{MANAGED_PREFIX}'"
        )),
        (_, true) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| entry.to_string()).collect()
    }

    #[test]
    fn an_entry_admits_its_name_or_what_its_stem_begins() {
        let allowed = entries(&["catalog_service_user", "loan_*"]);
        assert!(admits(&allowed, "catalog_service_user"));
        assert!(admits(&allowed, "loan_"));
        assert!(admits(&allowed, "loan_service_user"));
        assert!(!admits(&allowed, "catalog_service_user2"));
        assert!(!admits(&allowed, "catalog_service"));
        assert!(!admits(&allowed, "loans"));
        assert!(!admits(&[], "catalog_service_user"));
    }

    #[test]
    fn an_entry_is_a_role_name_or_a_stem_that_spares_the_platforms_roles() {
        for good in [
            "catalog_service_user",
            "loan_*",
            "l*",
            "_x*",
            "s",
            "svc",
            "sales_*",
        ] {
            assert_eq!(refusal_of_entry(good), None, "{good}");
        }
        for (bad, why) in [
            ("*", "every role"),
            ("s*", "platform's own"),
            ("svc*", "platform's own"),
            ("svc_*", "platform's own"),
            ("svc_loans*", "platform's own"),
            ("svc_loans", "platform's own"),
            ("Loan_*", "lowercase"),
            ("loan-*", "lowercase"),
            ("loan_**", "lowercase"),
            ("*loan", "lowercase"),
            ("", "lowercase"),
            ("9loan", "lowercase"),
        ] {
            let refusal = refusal_of_entry(bad).unwrap_or_default();
            assert!(refusal.contains(why), "{bad}: {refusal}");
        }
    }
}
