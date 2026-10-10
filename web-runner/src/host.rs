use std::str::FromStr;

pub const DEFAULT_TENANT: &str = "default";
pub const TENANT_SEPARATOR: &str = "--";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HostnameFormat {
    #[default]
    Flat,
    Hierarchical,
}

impl HostnameFormat {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Flat => "flat",
            Self::Hierarchical => "hierarchical",
        }
    }
}

impl FromStr for HostnameFormat {
    type Err = eyre::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "flat" => Ok(Self::Flat),
            "hierarchical" => Ok(Self::Hierarchical),
            other => Err(eyre::eyre!(
                "Invalid hostname format '{}': expected 'flat' or 'hierarchical'",
                other
            )),
        }
    }
}

pub fn parse_host(
    host: &str,
    base_domain: &str,
    format: HostnameFormat,
) -> Option<(String, String)> {
    match format {
        HostnameFormat::Flat => parse_flat(host),
        HostnameFormat::Hierarchical => parse_hierarchical(host, base_domain),
    }
}

pub fn compose_public_url(
    scheme: &str,
    tenant: &str,
    service: &str,
    base_domain: &str,
    format: HostnameFormat,
) -> String {
    if tenant == DEFAULT_TENANT {
        return format!("{scheme}://{service}.{base_domain}");
    }
    match format {
        HostnameFormat::Flat => {
            format!("{scheme}://{service}{TENANT_SEPARATOR}{tenant}.{base_domain}")
        }
        HostnameFormat::Hierarchical => {
            format!("{scheme}://{service}.{tenant}.{base_domain}")
        }
    }
}

fn parse_flat(host: &str) -> Option<(String, String)> {
    let (first, rest) = host.split_once('.')?;
    if first.is_empty() || rest.is_empty() {
        return None;
    }
    let (service, tenant) = match first.split_once(TENANT_SEPARATOR) {
        Some((s, t)) if !s.is_empty() && !t.is_empty() => (s, t),
        _ => (first, DEFAULT_TENANT),
    };
    Some((tenant.to_string(), service.to_string()))
}

fn parse_hierarchical(host: &str, base_domain: &str) -> Option<(String, String)> {
    let host_lower = host.to_ascii_lowercase();
    let base_lower = base_domain.to_ascii_lowercase();
    let prefix = host_lower
        .strip_suffix(&base_lower)
        .and_then(|s| s.strip_suffix('.'))?;
    let labels: Vec<&str> = prefix.split('.').collect();
    match labels.as_slice() {
        [service] if !service.is_empty() => {
            Some((DEFAULT_TENANT.to_string(), (*service).to_string()))
        }
        [service, tenant] if !service.is_empty() && !tenant.is_empty() => {
            Some(((*tenant).to_string(), (*service).to_string()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FLAT_BASE: &str = "staging.example.com";
    const HIER_BASE: &str = "example.org";

    fn flat(host: &str) -> Option<(String, String)> {
        parse_host(host, FLAT_BASE, HostnameFormat::Flat)
    }

    fn hier(host: &str) -> Option<(String, String)> {
        parse_host(host, HIER_BASE, HostnameFormat::Hierarchical)
    }

    #[test]
    fn flat_bare_service_host_resolves_to_default_tenant() {
        assert_eq!(
            flat("backoffice.staging.example.com"),
            Some(("default".into(), "backoffice".into()))
        );
        assert_eq!(
            flat("portal.prod.example.com"),
            Some(("default".into(), "portal".into()))
        );
        assert_eq!(
            flat("backoffice.localhost"),
            Some(("default".into(), "backoffice".into()))
        );
    }

    #[test]
    fn flat_service_tenant_separator_extracts_preview_tenant() {
        assert_eq!(
            flat("backoffice--preview-pr-51.staging.example.com"),
            Some(("preview-pr-51".into(), "backoffice".into()))
        );
        assert_eq!(
            flat("portal--preview-pr-99.staging.example.com"),
            Some(("preview-pr-99".into(), "portal".into()))
        );
    }

    #[test]
    fn flat_hyphenated_service_without_separator_is_a_single_service_name() {
        assert_eq!(
            flat("my-web-app.staging.example.com"),
            Some(("default".into(), "my-web-app".into()))
        );
    }

    #[test]
    fn flat_hyphenated_service_with_separator_splits_correctly() {
        assert_eq!(
            flat("my-web-app--preview-pr-7.staging.example.com"),
            Some(("preview-pr-7".into(), "my-web-app".into()))
        );
    }

    #[test]
    fn flat_single_label_host_is_rejected() {
        assert_eq!(flat("localhost"), None);
        assert_eq!(flat(""), None);
    }

    #[test]
    fn flat_empty_service_segment_is_rejected() {
        assert_eq!(flat(".staging.example.com"), None);
    }

    #[test]
    fn flat_malformed_separator_falls_back_to_default_tenant() {
        assert_eq!(
            flat("backoffice--.staging.example.com"),
            Some(("default".into(), "backoffice--".into()))
        );
        assert_eq!(
            flat("--preview-pr-1.staging.example.com"),
            Some(("default".into(), "--preview-pr-1".into()))
        );
    }

    #[test]
    fn flat_compose_default_tenant_omits_separator() {
        assert_eq!(
            compose_public_url(
                "https",
                "default",
                "backoffice",
                "staging.example.com",
                HostnameFormat::Flat
            ),
            "https://backoffice.staging.example.com"
        );
    }

    #[test]
    fn flat_compose_named_tenant_uses_separator() {
        assert_eq!(
            compose_public_url(
                "https",
                "preview-pr-60",
                "backoffice",
                "staging.example.com",
                HostnameFormat::Flat,
            ),
            "https://backoffice--preview-pr-60.staging.example.com"
        );
    }

    #[test]
    fn flat_compose_respects_scheme() {
        assert_eq!(
            compose_public_url(
                "http",
                "default",
                "backoffice",
                "localhost.example.com",
                HostnameFormat::Flat
            ),
            "http://backoffice.localhost.example.com"
        );
    }

    #[test]
    fn flat_compose_handles_hyphenated_service_and_tenant() {
        assert_eq!(
            compose_public_url(
                "https",
                "preview-pr-60",
                "my-web-app",
                "staging.example.com",
                HostnameFormat::Flat,
            ),
            "https://my-web-app--preview-pr-60.staging.example.com"
        );
    }

    #[test]
    fn flat_parse_compose_round_trip() {
        let cases = [
            ("default", "backoffice"),
            ("default", "portal"),
            ("default", "my-web-app"),
            ("preview-pr-60", "backoffice"),
            ("preview-pr-99", "portal"),
            ("preview-pr-7", "my-web-app"),
        ];
        for (tenant, service) in cases {
            let url = compose_public_url("https", tenant, service, FLAT_BASE, HostnameFormat::Flat);
            let host = url.strip_prefix("https://").expect("composed scheme");
            assert_eq!(
                flat(host),
                Some((tenant.into(), service.into())),
                "round-trip failed for ({tenant}, {service})"
            );
        }
    }

    #[test]
    fn hier_bare_service_host_resolves_to_default_tenant() {
        assert_eq!(
            hier("backoffice.example.org"),
            Some(("default".into(), "backoffice".into()))
        );
        assert_eq!(
            hier("portal.example.org"),
            Some(("default".into(), "portal".into()))
        );
    }

    #[test]
    fn hier_service_dot_tenant_extracts_tenant() {
        assert_eq!(
            hier("backoffice.globex.example.org"),
            Some(("globex".into(), "backoffice".into()))
        );
        assert_eq!(
            hier("portal.preview-pr-51.example.org"),
            Some(("preview-pr-51".into(), "portal".into()))
        );
    }

    #[test]
    fn hier_hyphenated_service_in_two_label_prefix() {
        assert_eq!(
            hier("my-web-app.globex.example.org"),
            Some(("globex".into(), "my-web-app".into()))
        );
    }

    #[test]
    fn hier_hyphenated_tenant_in_two_label_prefix() {
        assert_eq!(
            hier("backoffice.preview-pr-12.example.org"),
            Some(("preview-pr-12".into(), "backoffice".into()))
        );
    }

    #[test]
    fn hier_host_not_under_base_domain_returns_none() {
        assert_eq!(hier("backoffice.example.com"), None);
        assert_eq!(hier("backoffice.staging.example.com"), None);
    }

    #[test]
    fn hier_too_many_labels_returns_none() {
        assert_eq!(hier("backoffice.globex.extra.example.org"), None);
    }

    #[test]
    fn hier_bare_base_domain_returns_none() {
        assert_eq!(hier("example.org"), None);
    }

    #[test]
    fn hier_empty_labels_are_rejected() {
        assert_eq!(hier(".example.org"), None);
        assert_eq!(hier("backoffice..example.org"), None);
        assert_eq!(hier(".globex.example.org"), None);
    }

    #[test]
    fn hier_host_match_is_case_insensitive() {
        assert_eq!(
            hier("Backoffice.Globex.Example.ORG"),
            Some(("globex".into(), "backoffice".into()))
        );
    }

    #[test]
    fn hier_compose_default_tenant_omits_tenant_segment() {
        assert_eq!(
            compose_public_url(
                "https",
                "default",
                "backoffice",
                HIER_BASE,
                HostnameFormat::Hierarchical
            ),
            "https://backoffice.example.org"
        );
    }

    #[test]
    fn hier_compose_named_tenant_uses_dot() {
        assert_eq!(
            compose_public_url(
                "https",
                "globex",
                "backoffice",
                HIER_BASE,
                HostnameFormat::Hierarchical
            ),
            "https://backoffice.globex.example.org"
        );
    }

    #[test]
    fn hier_parse_compose_round_trip() {
        let cases = [
            ("default", "backoffice"),
            ("default", "portal"),
            ("globex", "backoffice"),
            ("preview-pr-60", "backoffice"),
            ("preview-pr-7", "my-web-app"),
        ];
        for (tenant, service) in cases {
            let url = compose_public_url(
                "https",
                tenant,
                service,
                HIER_BASE,
                HostnameFormat::Hierarchical,
            );
            let host = url.strip_prefix("https://").expect("composed scheme");
            assert_eq!(
                hier(host),
                Some((tenant.into(), service.into())),
                "round-trip failed for ({tenant}, {service})"
            );
        }
    }

    #[test]
    fn from_str_accepts_known_values() {
        assert_eq!(
            "flat".parse::<HostnameFormat>().unwrap(),
            HostnameFormat::Flat
        );
        assert_eq!(
            "hierarchical".parse::<HostnameFormat>().unwrap(),
            HostnameFormat::Hierarchical
        );
    }

    #[test]
    fn from_str_rejects_unknown_values() {
        assert!("Flat".parse::<HostnameFormat>().is_err());
        assert!("".parse::<HostnameFormat>().is_err());
        assert!("hier".parse::<HostnameFormat>().is_err());
    }

    #[test]
    fn as_str_round_trips_through_from_str() {
        for fmt in [HostnameFormat::Flat, HostnameFormat::Hierarchical] {
            assert_eq!(fmt.as_str().parse::<HostnameFormat>().unwrap(), fmt);
        }
    }
}
