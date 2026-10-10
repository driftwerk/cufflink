//! Identifier rules shared by the platform and the processes that turn
//! identifiers into database names, NATS subjects and filesystem paths.

pub const TENANT_SLUG_MAX: usize = 40;
pub const SERVICE_NAME_MAX: usize = 64;

/// `^[a-z0-9-]{1,40}$`
pub fn is_valid_tenant_slug(s: &str) -> bool {
    (1..=TENANT_SLUG_MAX).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// `^[A-Za-z0-9_-]{1,64}$`
pub fn is_valid_service_name(s: &str) -> bool {
    (1..=SERVICE_NAME_MAX).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_slugs() {
        for valid in ["default", "preview-pr-7", "a", "0", &"x".repeat(40)] {
            assert!(is_valid_tenant_slug(valid), "{valid}");
        }
        for invalid in [
            "",
            "Default",
            "preview_pr",
            "a.b",
            "../etc",
            "tenant slug",
            "ténant",
            &"x".repeat(41),
        ] {
            assert!(!is_valid_tenant_slug(invalid), "{invalid}");
        }
    }

    #[test]
    fn service_names() {
        for valid in ["stock-service", "Blog_Service", "a", &"s".repeat(64)] {
            assert!(is_valid_service_name(valid), "{valid}");
        }
        for invalid in ["", "a.b", "../x", "a/b", "sérvice", "a b", &"s".repeat(65)] {
            assert!(!is_valid_service_name(invalid), "{invalid}");
        }
    }
}
