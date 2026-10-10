use crate::policy::config::EgressConfig;
use crate::policy::{Denied, PolicyKey, PolicySet, Violation};
use reqwest::header::{HeaderMap, HeaderValue, LOCATION, REFERER};
use reqwest::{Method, Response, StatusCode, Url};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use url::Host;

const MAX_REDIRECTS: usize = 10;
const CLIENT_CACHE_MAX: usize = 256;
const CLIENT_CACHE_TTL: Duration = Duration::from_secs(300);

const METADATA_HOSTS: [&str; 5] = [
    "metadata.google.internal",
    "metadata",
    "metadata.internal",
    "instance-data",
    "instance-data.ec2.internal",
];

const METADATA_V4: [Ipv4Addr; 3] = [
    Ipv4Addr::new(168, 63, 129, 16),
    Ipv4Addr::new(192, 0, 0, 192),
    Ipv4Addr::new(100, 100, 100, 200),
];
const METADATA_V6: [Ipv6Addr; 3] = [
    Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254),
    Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x23),
    Ipv6Addr::new(0xfd20, 0xce, 0, 0, 0, 0, 0, 0x254),
];

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EgressDenied {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("metadata endpoint denied: {0}")]
    MetadataEndpoint(String),
    #[error("unresolvable host: {0}")]
    Unresolvable(String),
    #[error("response body exceeds {0} bytes")]
    BodyTooLarge(u64),
}

impl EgressDenied {
    pub fn reason(&self) -> &'static str {
        match self {
            EgressDenied::InvalidUrl(_) => "invalid_url",
            EgressDenied::UnsupportedScheme(_) => "scheme",
            EgressDenied::MetadataEndpoint(_) => "metadata",
            EgressDenied::Unresolvable(_) => "unresolvable",
            EgressDenied::BodyTooLarge(_) => "body_too_large",
        }
    }

    pub fn is_permanent(&self) -> bool {
        !matches!(self, EgressDenied::Unresolvable(_))
    }

    fn counted(self) -> Self {
        metrics::counter!("cufflink_egress_denied_total", "reason" => self.reason()).increment(1);
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EgressError {
    #[error(transparent)]
    Denied(#[from] EgressDenied),
    #[error("egress denied by policy: {}", .0.code)]
    Policy(Denied),
    #[error("too many redirects")]
    TooManyRedirects,
    #[error("request timed out after {0:?}")]
    TimedOut(Duration),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, Clone)]
pub struct EgressTarget {
    pub url: Url,
    pub host: String,
    pub addrs: Vec<SocketAddr>,
}

#[derive(Debug, Clone)]
pub struct OutboundRequest {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
    pub timeout: Duration,
}

pub type TargetCheck<'a> = &'a (dyn Fn(&EgressTarget) -> Result<(), Denied> + Sync);

pub struct EgressPolicy<'a> {
    policies: &'a PolicySet,
    tenant_slug: &'a str,
    config: &'a EgressConfig,
}

impl<'a> EgressPolicy<'a> {
    pub fn new(policies: &'a PolicySet, tenant_slug: &'a str) -> Self {
        Self {
            policies,
            tenant_slug,
            config: policies.config::<EgressConfig>(),
        }
    }

    pub fn check(&self, target: &EgressTarget) -> Result<(), Denied> {
        self.policies.apply(
            PolicyKey::Egress,
            self.tenant_slug,
            EgressGuard::private_violation(target, &self.config.allow_hosts),
        )
    }

    pub fn max_response_bytes(&self) -> Option<u64> {
        self.config.max_response_bytes
    }
}

type ClientKey = (String, Vec<SocketAddr>);

#[derive(Clone)]
pub struct EgressGuard {
    max_response_bytes: u64,
    connect_timeout: Duration,
    clients: Arc<Mutex<HashMap<ClientKey, (reqwest::Client, Instant)>>>,
}

impl EgressGuard {
    pub fn new(max_response_bytes: u64, connect_timeout: Duration) -> Self {
        Self {
            max_response_bytes,
            connect_timeout,
            clients: Arc::default(),
        }
    }

    pub async fn resolve(&self, raw: &str) -> Result<EgressTarget, EgressDenied> {
        self.target(raw).await.map_err(EgressDenied::counted)
    }

    async fn target(&self, raw: &str) -> Result<EgressTarget, EgressDenied> {
        let url = Url::parse(raw).map_err(|e| EgressDenied::InvalidUrl(e.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(EgressDenied::UnsupportedScheme(url.scheme().to_string()));
        }
        let port = url
            .port_or_known_default()
            .ok_or_else(|| EgressDenied::InvalidUrl(format!("{raw}: no port")))?;
        let (host, addrs) = match url.host() {
            Some(Host::Domain(domain)) => {
                let host = domain.trim_end_matches('.').to_ascii_lowercase();
                if is_metadata_host(&host) {
                    return Err(EgressDenied::MetadataEndpoint(host));
                }
                let addrs = self.lookup(&host, port).await?;
                (host, addrs)
            }
            Some(Host::Ipv4(ip)) => (ip.to_string(), vec![SocketAddr::new(ip.into(), port)]),
            Some(Host::Ipv6(ip)) => (ip.to_string(), vec![SocketAddr::new(ip.into(), port)]),
            None => return Err(EgressDenied::InvalidUrl(format!("{raw}: no host"))),
        };
        if let Some(metadata) = addrs.iter().find(|a| is_metadata_ip(a.ip())) {
            return Err(EgressDenied::MetadataEndpoint(metadata.ip().to_string()));
        }
        Ok(EgressTarget { url, host, addrs })
    }

    async fn lookup(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, EgressDenied> {
        let unresolvable = |detail: String| EgressDenied::Unresolvable(format!("{host}: {detail}"));
        let mut addrs: Vec<SocketAddr> =
            tokio::time::timeout(self.connect_timeout, tokio::net::lookup_host((host, port)))
                .await
                .map_err(|_| unresolvable("lookup timed out".into()))?
                .map_err(|e| unresolvable(e.to_string()))?
                .collect();
        addrs.sort();
        addrs.dedup();
        if addrs.is_empty() {
            return Err(unresolvable("no addresses".into()));
        }
        Ok(addrs)
    }

    pub fn private_violation(target: &EgressTarget, allow_hosts: &[String]) -> Option<Violation> {
        let internal = target.addrs.iter().any(|a| is_private_ip(a.ip()));
        let allowed = allow_hosts.iter().any(|p| host_matches(p, &target.host));
        (internal && !allowed).then(|| {
            Violation::new(
                "private_address",
                format!("{} resolves to an internal address", target.host),
            )
        })
    }

    pub fn client(&self, target: &EgressTarget) -> Result<reqwest::Client, EgressError> {
        let domain = target.url.host_str().unwrap_or(&target.host);
        let key = (domain.to_string(), target.addrs.clone());
        let fresh = |built: &Instant| built.elapsed() < CLIENT_CACHE_TTL;
        if let Some((client, _)) = self
            .clients
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .filter(|(_, built)| fresh(built))
        {
            return Ok(client.clone());
        }
        let builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(self.connect_timeout);
        let builder = match (target.url.host(), target.url.host_str()) {
            (Some(Host::Domain(_)), Some(domain)) => {
                builder.resolve_to_addrs(domain, &target.addrs)
            }
            _ => builder,
        };
        let client = builder.build()?;
        let mut clients = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        clients.retain(|_, (_, built)| fresh(built));
        if clients.len() >= CLIENT_CACHE_MAX {
            if let Some(oldest) = clients
                .iter()
                .min_by_key(|(_, (_, built))| *built)
                .map(|(key, _)| key.clone())
            {
                clients.remove(&oldest);
            }
        }
        clients.insert(key, (client.clone(), Instant::now()));
        Ok(client)
    }

    pub fn effective_max_bytes(&self, tenant_override: Option<u64>) -> u64 {
        tenant_override.map_or(self.max_response_bytes, |bytes| {
            bytes.min(self.max_response_bytes)
        })
    }

    pub async fn read_body(mut response: Response, max_bytes: u64) -> Result<Vec<u8>, EgressError> {
        let too_large = || EgressDenied::BodyTooLarge(max_bytes).counted().into();
        if response.content_length().is_some_and(|len| len > max_bytes) {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if (body.len() + chunk.len()) as u64 > max_bytes {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    pub async fn send(
        &self,
        request: OutboundRequest,
        check: TargetCheck<'_>,
        follow_redirects: bool,
    ) -> Result<Response, EgressError> {
        let timeout = request.timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        let expired = || tokio::time::Instant::now() >= deadline;
        match tokio::time::timeout_at(
            deadline,
            self.follow(request, check, follow_redirects, deadline),
        )
        .await
        {
            Ok(Err(EgressError::Http(e))) if e.is_timeout() && expired() => {
                Err(EgressError::TimedOut(timeout))
            }
            Ok(outcome) => outcome,
            Err(_) => Err(EgressError::TimedOut(timeout)),
        }
    }

    async fn follow(
        &self,
        request: OutboundRequest,
        check: TargetCheck<'_>,
        follow_redirects: bool,
        deadline: tokio::time::Instant,
    ) -> Result<Response, EgressError> {
        let OutboundRequest {
            mut method,
            url,
            mut headers,
            mut body,
            timeout: _,
        } = request;
        let mut target = self.checked(&url, check).await?;
        let mut hops = 0;
        loop {
            let mut builder = self
                .client(&target)?
                .request(method.clone(), target.url.clone())
                .headers(headers.clone())
                .timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
            if let Some(bytes) = &body {
                builder = builder.body(bytes.clone());
            }
            let response = builder.send().await?;
            let Some(hop) = follow_redirects
                .then(|| redirect(response.status(), &method, response.headers(), &target.url))
                .flatten()
            else {
                return Ok(response);
            };
            hops += 1;
            if hops > MAX_REDIRECTS {
                return Err(EgressError::TooManyRedirects);
            }
            let previous = target.url.clone();
            target = self.checked(hop.url.as_str(), check).await?;
            method = hop.method;
            if hop.drop_body {
                body = None;
                strip_payload_headers(&mut headers);
            }
            if cross_origin(&previous, &target.url) {
                strip_credentials(&mut headers);
            }
            match referer(&target.url, &previous) {
                Some(value) => headers.insert(REFERER, value),
                None => headers.remove(REFERER),
            };
        }
    }

    async fn checked(
        &self,
        url: &str,
        check: TargetCheck<'_>,
    ) -> Result<EgressTarget, EgressError> {
        let target = self.resolve(url).await?;
        check(&target).map_err(EgressError::Policy)?;
        Ok(target)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Hop {
    url: Url,
    method: Method,
    drop_body: bool,
}

fn redirect(
    status: StatusCode,
    method: &Method,
    headers: &HeaderMap,
    current: &Url,
) -> Option<Hop> {
    let (method, drop_body) = match status {
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND if *method == Method::POST => {
            (Method::GET, true)
        }
        StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND => (method.clone(), false),
        StatusCode::SEE_OTHER if *method == Method::HEAD => (Method::HEAD, true),
        StatusCode::SEE_OTHER => (Method::GET, true),
        StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT => (method.clone(), false),
        _ => return None,
    };
    let location = headers.get(LOCATION)?.to_str().ok()?;
    let url = current.join(location).ok()?;
    Some(Hop {
        url,
        method,
        drop_body,
    })
}

fn cross_origin(previous: &Url, next: &Url) -> bool {
    previous.host_str() != next.host_str()
        || previous.port_or_known_default() != next.port_or_known_default()
}

fn strip_credentials(headers: &mut HeaderMap) {
    for name in [
        "authorization",
        "cookie",
        "cookie2",
        "proxy-authorization",
        "www-authenticate",
    ] {
        headers.remove(name);
    }
}

fn strip_payload_headers(headers: &mut HeaderMap) {
    for name in [
        "content-type",
        "content-length",
        "content-encoding",
        "transfer-encoding",
    ] {
        headers.remove(name);
    }
}

fn referer(next: &Url, previous: &Url) -> Option<HeaderValue> {
    if next.scheme() == "http" && previous.scheme() == "https" {
        return None;
    }
    let mut referer = previous.clone();
    referer.set_username("").ok()?;
    referer.set_password(None).ok()?;
    referer.set_fragment(None);
    HeaderValue::from_str(referer.as_str()).ok()
}

pub fn is_metadata_host(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    METADATA_HOSTS.contains(&host.as_str())
}

fn embedded_v4(v6: Ipv6Addr) -> Option<Ipv4Addr> {
    const NAT64: [u16; 6] = [0x64, 0xff9b, 0, 0, 0, 0];
    let segments = v6.segments();
    let [.., hi, lo] = segments;
    let tail = || Ipv4Addr::from((u32::from(hi) << 16) | u32::from(lo));
    match segments[..6] {
        _ if v6.is_loopback() || v6.is_unspecified() => None,
        [0, 0, 0, 0, 0, 0 | 0xffff] => Some(tail()),
        ref prefix if prefix == NAT64 => Some(tail()),
        _ => None,
    }
}

fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => embedded_v4(v6).map_or(ip, IpAddr::V4),
        IpAddr::V4(_) => ip,
    }
}

pub fn is_metadata_ip(ip: IpAddr) -> bool {
    match canonical(ip) {
        IpAddr::V4(v4) => v4.is_link_local() || METADATA_V4.contains(&v4),
        IpAddr::V6(v6) => METADATA_V6.contains(&v6),
    }
}

pub fn is_private_ip(ip: IpAddr) -> bool {
    match canonical(ip) {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || a == 0
                || (a == 100 && (64..128).contains(&b))
                || (a == 192 && b == 0 && c == 0)
                || (a == 198 && (b == 18 || b == 19))
        }
        IpAddr::V6(v6) => {
            let first = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
        }
    }
}

pub fn host_matches(pattern: &str, host: &str) -> bool {
    let host = host.trim_end_matches('.');
    match pattern.strip_prefix("*.") {
        Some(suffix) => {
            host.len() > suffix.len() + 1
                && host
                    .to_ascii_lowercase()
                    .ends_with(&format!(".{}", suffix.to_ascii_lowercase()))
        }
        None => pattern.eq_ignore_ascii_case(host),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::Router;

    fn guard() -> EgressGuard {
        EgressGuard::new(1024, Duration::from_secs(2))
    }

    fn allow_all(_: &EgressTarget) -> Result<(), Denied> {
        Ok(())
    }

    async fn serve(app: Router) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    fn request(method: Method, url: String) -> OutboundRequest {
        OutboundRequest {
            method,
            url,
            headers: HeaderMap::new(),
            body: Some(b"payload".to_vec()),
            timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn metadata_literals_denied() {
        for url in [
            "http://metadata.google.internal/computeMetadata/v1/",
            "http://metadata.google.internal./",
            "http://METADATA/",
            "http://instance-data/latest/",
        ] {
            assert!(
                matches!(
                    guard().resolve(url).await,
                    Err(EgressDenied::MetadataEndpoint(_))
                ),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn metadata_ips_denied() {
        for url in [
            "http://169.254.169.254/latest/meta-data/",
            "http://2852039166/",
            "http://0251.0376.0251.0376/",
            "http://0xa9fea9fe/",
            "http://169.254.43518/",
            "http://[::ffff:169.254.169.254]/",
            "http://[::169.254.169.254]/",
            "http://[64:ff9b::a9fe:a9fe]/",
            "http://169.254.169.252/",
            "http://169.254.170.2/v2/credentials",
            "http://169.254.170.23/v1/credentials",
            "http://[fd00:ec2::23]/",
            "http://100.100.100.200/",
            "http://168.63.129.16/",
            "http://192.0.0.192/",
            "http://[fd00:ec2::254]/",
            "http://[fd20:ce::254]/computeMetadata/v1/",
        ] {
            assert!(
                matches!(
                    guard().resolve(url).await,
                    Err(EgressDenied::MetadataEndpoint(_))
                ),
                "{url}"
            );
        }
    }

    #[tokio::test]
    async fn resolve_rejects_schemes_and_garbage() {
        for url in ["ftp://example.com/x", "file:///etc/passwd", "gopher://x/"] {
            assert!(
                matches!(
                    guard().resolve(url).await,
                    Err(EgressDenied::UnsupportedScheme(_))
                ),
                "{url}"
            );
        }
        assert!(matches!(
            guard().resolve("not a url").await,
            Err(EgressDenied::InvalidUrl(_))
        ));
        assert!(matches!(
            guard().resolve("http://no-such-host.invalid/").await,
            Err(EgressDenied::Unresolvable(_))
        ));
    }

    #[test]
    fn private_ranges_classified() {
        let private = [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "255.255.255.255",
            "192.0.0.8",
            "198.18.0.1",
            "::1",
            "::",
            "fd12::1",
            "fe80::1",
            "::ffff:10.0.0.1",
            "::10.0.0.1",
            "64:ff9b::7f00:1",
        ];
        let public = [
            "8.8.8.8",
            "100.128.0.1",
            "172.32.0.1",
            "2606:4700::1111",
            "::ffff:1.1.1.1",
            "64:ff9b::101:101",
        ];
        for ip in private {
            assert!(is_private_ip(ip.parse().expect("ip")), "{ip}");
        }
        for ip in public {
            assert!(!is_private_ip(ip.parse().expect("ip")), "{ip}");
        }
    }

    #[tokio::test]
    async fn loopback_and_unspecified_hosts_are_private() {
        for url in ["http://localhost/", "http://0.0.0.0/", "http://[::]/"] {
            let target = guard().resolve(url).await.expect(url);
            assert_eq!(
                EgressGuard::private_violation(&target, &[]).map(|v| v.code),
                Some("private_address"),
                "{url}"
            );
        }
    }

    #[test]
    fn host_matches_exact_and_wildcard() {
        assert!(host_matches("keycloak", "keycloak"));
        assert!(host_matches("keycloak", "KEYCLOAK."));
        assert!(host_matches(
            "*.storefront.example",
            "shop.storefront.example"
        ));
        assert!(host_matches(
            "*.storefront.example",
            "a.b.storefront.example"
        ));
        assert!(!host_matches("*.storefront.example", "storefront.example"));
        assert!(!host_matches(
            "*.storefront.example",
            "evilstorefront.example"
        ));
        assert!(!host_matches("keycloak", "keycloak.evil.com"));
    }

    #[tokio::test]
    async fn private_violation_none_when_host_allowlisted() {
        let target = guard()
            .resolve("http://127.0.0.1:8080/")
            .await
            .expect("target");
        let violation = EgressGuard::private_violation(&target, &[]).expect("private");
        assert_eq!(violation.code, "private_address");
        assert!(EgressGuard::private_violation(&target, &["127.0.0.1".into()]).is_none());
        let public = EgressTarget {
            url: Url::parse("https://api.stripe.com/").expect("url"),
            host: "api.stripe.com".into(),
            addrs: vec!["3.18.12.63:443".parse().expect("addr")],
        };
        assert!(EgressGuard::private_violation(&public, &[]).is_none());
    }

    #[test]
    fn effective_max_bytes_only_lowers() {
        let g = EgressGuard::new(10, Duration::from_secs(1));
        assert_eq!(g.effective_max_bytes(None), 10);
        assert_eq!(g.effective_max_bytes(Some(4)), 4);
        assert_eq!(g.effective_max_bytes(Some(40)), 10);
    }

    #[test]
    fn redirect_rules_match_the_http_client() {
        let at = Url::parse("https://a.test/x/y").expect("url");
        let location = |loc: &str| {
            let mut h = HeaderMap::new();
            h.insert(LOCATION, loc.parse().expect("location"));
            h
        };
        let hop = |status, method: Method| redirect(status, &method, &location("../z"), &at);
        assert_eq!(
            hop(StatusCode::FOUND, Method::POST),
            Some(Hop {
                url: Url::parse("https://a.test/z").expect("url"),
                method: Method::GET,
                drop_body: true
            })
        );
        assert_eq!(
            hop(StatusCode::MOVED_PERMANENTLY, Method::PUT).map(|h| (h.method, h.drop_body)),
            Some((Method::PUT, false))
        );
        assert_eq!(
            hop(StatusCode::SEE_OTHER, Method::PUT).map(|h| (h.method, h.drop_body)),
            Some((Method::GET, true))
        );
        assert_eq!(
            hop(StatusCode::SEE_OTHER, Method::HEAD).map(|h| h.method),
            Some(Method::HEAD)
        );
        assert_eq!(
            hop(StatusCode::TEMPORARY_REDIRECT, Method::POST).map(|h| (h.method, h.drop_body)),
            Some((Method::POST, false))
        );
        assert_eq!(hop(StatusCode::OK, Method::GET), None);
        assert_eq!(
            redirect(StatusCode::FOUND, &Method::GET, &HeaderMap::new(), &at),
            None
        );
    }

    #[test]
    fn cross_origin_hops_drop_credentials_and_referer_follows_the_client() {
        let a = Url::parse("https://a.test/").expect("url");
        assert!(!cross_origin(
            &a,
            &Url::parse("https://a.test:443/p").expect("url")
        ));
        assert!(cross_origin(
            &a,
            &Url::parse("https://b.test/").expect("url")
        ));
        assert!(cross_origin(
            &a,
            &Url::parse("https://a.test:8443/").expect("url")
        ));
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer x".parse().expect("h"));
        headers.insert("x-custom", "kept".parse().expect("h"));
        strip_credentials(&mut headers);
        assert_eq!(headers.len(), 1);
        let from = Url::parse("https://u:p@a.test/page#frag").expect("url");
        assert_eq!(
            referer(&Url::parse("https://b.test/").expect("url"), &from).expect("referer"),
            "https://a.test/page"
        );
        assert!(referer(&Url::parse("http://b.test/").expect("url"), &from).is_none());
    }

    #[tokio::test]
    async fn read_body_caps() {
        let addr = serve(Router::new().route("/big", get(|| async { "x".repeat(4096) }))).await;
        let g = guard();
        let response = g
            .send(
                request(Method::GET, format!("http://{addr}/big")),
                &allow_all,
                true,
            )
            .await
            .expect("response");
        assert!(matches!(
            EgressGuard::read_body(response, 1024).await,
            Err(EgressError::Denied(EgressDenied::BodyTooLarge(1024)))
        ));
        let response = g
            .send(
                request(Method::GET, format!("http://{addr}/big")),
                &allow_all,
                true,
            )
            .await
            .expect("response");
        assert_eq!(
            EgressGuard::read_body(response, 8192)
                .await
                .expect("body")
                .len(),
            4096
        );
    }

    #[tokio::test]
    async fn client_pins_resolved_addrs() {
        let addr = serve(Router::new().route("/", get(|| async { "pinned" }))).await;
        let g = guard();
        let target = EgressTarget {
            url: Url::parse(&format!("http://pinned.invalid:{}/", addr.port())).expect("url"),
            host: "pinned.invalid".into(),
            addrs: vec![addr],
        };
        let body = g
            .client(&target)
            .expect("client")
            .get(target.url.clone())
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");
        assert_eq!(
            body, "pinned",
            "the unresolvable name connects to the checked address"
        );
    }

    #[tokio::test]
    async fn a_warm_client_never_serves_a_differently_spelled_host() {
        let addr = serve(Router::new().route("/", get(|| async { "pinned" }))).await;
        let g = guard();
        for spelling in ["pinned.invalid", "pinned.invalid."] {
            let target = EgressTarget {
                url: Url::parse(&format!("http://{spelling}:{}/", addr.port())).expect("url"),
                host: "pinned.invalid".into(),
                addrs: vec![addr],
            };
            let response = g
                .client(&target)
                .expect("client")
                .get(target.url.clone())
                .send()
                .await
                .unwrap_or_else(|e| panic!("{spelling} left the pinned addresses: {e}"));
            assert_eq!(response.text().await.expect("text"), "pinned");
        }
    }

    #[tokio::test]
    async fn every_redirect_hop_is_checked() {
        let upstream = serve(Router::new().route("/landing", get(|| async { "landed" }))).await;
        let to_metadata = serve(Router::new().route(
            "/",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [(LOCATION, "http://169.254.169.254/latest/")],
                    "",
                )
            }),
        ))
        .await;
        let to_upstream = serve(Router::new().route(
            "/",
            get(move || async move {
                let location = format!("http://{upstream}/landing");
                (StatusCode::TEMPORARY_REDIRECT, [(LOCATION, location)], "")
            }),
        ))
        .await;
        let g = guard();
        let err = g
            .send(
                request(Method::GET, format!("http://{to_metadata}/")),
                &allow_all,
                true,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, EgressError::Denied(EgressDenied::MetadataEndpoint(_))),
            "{err}"
        );

        let landed = g
            .send(
                request(Method::GET, format!("http://{to_upstream}/")),
                &allow_all,
                true,
            )
            .await
            .expect("followed");
        assert_eq!(landed.text().await.expect("text"), "landed");

        let refuse_upstream = move |target: &EgressTarget| {
            if target.addrs.contains(&upstream) {
                Err(Denied {
                    key: crate::policy::PolicyKey::Egress,
                    code: "private_address",
                })
            } else {
                Ok(())
            }
        };
        let err = g
            .send(
                request(Method::GET, format!("http://{to_upstream}/")),
                &refuse_upstream,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EgressError::Policy(_)), "{err}");

        let unfollowed = g
            .send(
                request(Method::GET, format!("http://{to_upstream}/")),
                &allow_all,
                false,
            )
            .await
            .expect("response");
        assert_eq!(unfollowed.status(), StatusCode::TEMPORARY_REDIRECT);
    }

    async fn redirect_chain(delay: Duration) -> SocketAddr {
        serve(Router::new().route(
            "/{n}",
            get(
                move |axum::extract::Path(n): axum::extract::Path<u32>| async move {
                    tokio::time::sleep(delay).await;
                    match n.checked_sub(1) {
                        Some(next) => (StatusCode::FOUND, [(LOCATION, format!("/{next}"))], "")
                            .into_response(),
                        None => "end".into_response(),
                    }
                },
            ),
        ))
        .await
    }

    #[tokio::test]
    async fn redirect_limit_matches_the_http_client() {
        let chain = redirect_chain(Duration::ZERO).await;
        let followed = guard()
            .send(
                request(Method::GET, format!("http://{chain}/{MAX_REDIRECTS}")),
                &allow_all,
                true,
            )
            .await
            .expect("ten redirects are followed");
        assert_eq!(followed.text().await.expect("text"), "end");
        let err = guard()
            .send(
                request(Method::GET, format!("http://{chain}/{}", MAX_REDIRECTS + 1)),
                &allow_all,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EgressError::TooManyRedirects), "{err}");
    }

    #[tokio::test]
    async fn one_deadline_bounds_the_whole_redirect_chain() {
        let chain = redirect_chain(Duration::from_millis(300)).await;
        let started = Instant::now();
        let err = guard()
            .send(
                OutboundRequest {
                    timeout: Duration::from_secs(1),
                    ..request(Method::GET, format!("http://{chain}/8"))
                },
                &allow_all,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EgressError::TimedOut(_)), "{err}");
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "each hop does not get a fresh timeout: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn redirect_loops_stop() {
        let looping = serve(Router::new().route(
            "/",
            get(|| async { (StatusCode::FOUND, [(LOCATION, "/")], "") }),
        ))
        .await;
        let err = guard()
            .send(
                request(Method::GET, format!("http://{looping}/")),
                &allow_all,
                true,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EgressError::TooManyRedirects), "{err}");
    }
}
