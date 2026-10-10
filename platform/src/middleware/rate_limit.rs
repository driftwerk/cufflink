use crate::redis_conn::{RedisCallError, RedisHandle};
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{header::RETRY_AFTER, HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use eyre::{eyre, Result};
use governor::{clock::DefaultClock, state::keyed::DashMapStateStore, Quota, RateLimiter};
use ipnet::IpNet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

type KeyedLimiter = Arc<RateLimiter<String, DashMapStateStore<String>, DefaultClock>>;

const DEFAULT_PER_SECOND: u32 = 20;
const DEFAULT_BURST: u32 = 60;

const EXEMPT_PATHS: &[&str] = &["/health", "/ready", "/metrics"];

const CLOUDFLARE_V4: &[(Ipv4Addr, u8)] = &[
    (Ipv4Addr::new(173, 245, 48, 0), 20),
    (Ipv4Addr::new(103, 21, 244, 0), 22),
    (Ipv4Addr::new(103, 22, 200, 0), 22),
    (Ipv4Addr::new(103, 31, 4, 0), 22),
    (Ipv4Addr::new(141, 101, 64, 0), 18),
    (Ipv4Addr::new(108, 162, 192, 0), 18),
    (Ipv4Addr::new(190, 93, 240, 0), 20),
    (Ipv4Addr::new(188, 114, 96, 0), 20),
    (Ipv4Addr::new(197, 234, 240, 0), 22),
    (Ipv4Addr::new(198, 41, 128, 0), 17),
    (Ipv4Addr::new(162, 158, 0, 0), 15),
    (Ipv4Addr::new(104, 16, 0, 0), 13),
    (Ipv4Addr::new(104, 24, 0, 0), 14),
    (Ipv4Addr::new(172, 64, 0, 0), 13),
    (Ipv4Addr::new(131, 0, 72, 0), 22),
];
const CLOUDFLARE_V6: &[(Ipv6Addr, u8)] = &[
    (Ipv6Addr::new(0x2400, 0xcb00, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2606, 0x4700, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2803, 0xf800, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2405, 0xb500, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2405, 0x8100, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x2a06, 0x98c0, 0, 0, 0, 0, 0, 0), 29),
    (Ipv6Addr::new(0x2c0f, 0xf248, 0, 0, 0, 0, 0, 0), 32),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientIpSource {
    Peer,
    Gclb,
    XffFirst,
}

impl ClientIpSource {
    pub fn from_setting(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("gclb") => ClientIpSource::Gclb,
            Some("xff-first") => ClientIpSource::XffFirst,
            _ => ClientIpSource::Peer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitSettings {
    pub enabled: bool,
    pub per_second: u32,
    pub burst: u32,
    pub ip_source: ClientIpSource,
    pub exempt_private_peers: bool,
    pub exempt_ips: Vec<IpNet>,
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            per_second: DEFAULT_PER_SECOND,
            burst: DEFAULT_BURST,
            ip_source: ClientIpSource::Peer,
            exempt_private_peers: true,
            exempt_ips: Vec::new(),
        }
    }
}

impl RateLimitSettings {
    pub fn from_env() -> Result<Self> {
        Self::from_values(|name| std::env::var(name).ok())
    }

    fn from_values(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let flag = |name: &str, default: bool| {
            get(name)
                .map(|v| {
                    matches!(
                        v.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes" | "on"
                    )
                })
                .unwrap_or(default)
        };
        let number = |name: &str, default: u32| {
            get(name)
                .and_then(|v| v.trim().parse::<u32>().ok())
                .filter(|n| *n > 0)
                .unwrap_or(default)
        };
        let per_second = number("RATE_LIMIT_PER_SECOND", DEFAULT_PER_SECOND);
        let burst = number("RATE_LIMIT_BURST", DEFAULT_BURST).max(per_second);
        let ip_source = ClientIpSource::from_setting(get("RATE_LIMIT_CLIENT_IP_SOURCE").as_deref());
        Ok(Self {
            enabled: flag("RATE_LIMIT_ENABLED", false),
            per_second,
            burst,
            ip_source,
            exempt_private_peers: flag("RATE_LIMIT_EXEMPT_PRIVATE_PEERS", true),
            exempt_ips: parse_exempt_ips(get("RATE_LIMIT_EXEMPT_IPS").as_deref())?,
        })
    }

    fn window_secs(&self) -> u64 {
        u64::from(self.burst.div_ceil(self.per_second)).max(1)
    }

    fn window_limit(&self) -> u64 {
        u64::from(self.per_second) * self.window_secs()
    }
}

fn parse_exempt_ips(raw: Option<&str>) -> Result<Vec<IpNet>> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse::<IpAddr>()
                .map(IpNet::from)
                .or_else(|_| entry.parse::<IpNet>())
                .map_err(|_| {
                    eyre!(
                        "RATE_LIMIT_EXEMPT_IPS entry '{entry}' is not an IP address or CIDR range"
                    )
                })
        })
        .collect()
}

#[derive(Clone)]
pub struct GlobalRateLimiter {
    redis: RedisHandle,
    fallback: KeyedLimiter,
    settings: Arc<RateLimitSettings>,
}

impl GlobalRateLimiter {
    pub fn new(redis: RedisHandle, settings: RateLimitSettings) -> Self {
        let per_second = NonZeroU32::new(settings.per_second).unwrap_or(NonZeroU32::MIN);
        let burst = NonZeroU32::new(settings.burst).unwrap_or(per_second);
        let fallback: KeyedLimiter = Arc::new(RateLimiter::keyed(
            Quota::per_second(per_second).allow_burst(burst),
        ));

        let cleanup = fallback.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                cleanup.retain_recent();
            }
        });

        Self {
            redis,
            fallback,
            settings: Arc::new(settings),
        }
    }

    pub fn settings(&self) -> &RateLimitSettings {
        &self.settings
    }

    pub async fn check(&self, caller: &str) -> bool {
        if self.redis.is_connected() {
            match self.check_redis(caller).await {
                Ok(allowed) => return allowed,
                Err(e) => {
                    warn!(error = %e, "Redis rate limit check failed; using the in-memory limiter")
                }
            }
        }
        self.fallback.check_key(&caller.to_string()).is_ok()
    }

    async fn check_redis(&self, caller: &str) -> Result<bool, RedisCallError> {
        use redis::AsyncCommands;

        let window = self.settings.window_secs();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        let key = format!("cufflink:rate:{}:{}", caller, now / window);
        let ttl = i64::try_from(window + 1).unwrap_or(2);

        let count: u64 = self
            .redis
            .call(|mut conn| async move {
                let count: u64 = conn.incr(&key, 1u64).await?;
                if count == 1 {
                    let _: () = conn.expire(&key, ttl).await?;
                }
                Ok(count)
            })
            .await?;

        Ok(count <= self.settings.window_limit())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Decision {
    Exempt(&'static str),
    Limit(String),
}

pub fn classify(
    path: &str,
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    settings: &RateLimitSettings,
) -> Decision {
    if EXEMPT_PATHS.contains(&path) {
        return Decision::Exempt("probe");
    }
    if settings.exempt_private_peers && peer.is_some_and(is_internal) {
        return Decision::Exempt("in-cluster");
    }
    match client_ip(headers, peer, settings.ip_source) {
        Some(ip) if is_listed(ip, &settings.exempt_ips) => Decision::Exempt("listed"),
        Some(ip) => Decision::Limit(format!("ip:{ip}")),
        None => Decision::Limit("unknown".to_string()),
    }
}

pub fn client_ip(
    headers: &HeaderMap,
    peer: Option<IpAddr>,
    source: ClientIpSource,
) -> Option<IpAddr> {
    let forwarded: Vec<IpAddr> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|entry| entry.trim().parse::<IpAddr>().ok())
        .collect();

    match source {
        ClientIpSource::Peer => peer,
        ClientIpSource::XffFirst => forwarded
            .first()
            .copied()
            .or_else(|| header_ip(headers, "x-real-ip"))
            .or(peer),
        ClientIpSource::Gclb => {
            let seen_by_lb = match forwarded.len() {
                0 => return peer,
                1 => forwarded[0],
                n => forwarded[n - 2],
            };
            if is_cloudflare(seen_by_lb) {
                if let Some(visitor) = header_ip(headers, "cf-connecting-ip") {
                    return Some(visitor);
                }
            }
            Some(seen_by_lb)
        }
    }
}

fn header_ip(headers: &HeaderMap, name: &str) -> Option<IpAddr> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<IpAddr>().ok())
}

fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_loopback() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_internal(IpAddr::V4(v4));
            }
            let first = v6.segments()[0];
            v6.is_loopback() || (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
    }
}

fn is_listed(ip: IpAddr, nets: &[IpNet]) -> bool {
    if nets.iter().any(|net| net.contains(&ip)) {
        return true;
    }
    match ip {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .is_some_and(|v4| nets.iter().any(|net| net.contains(&IpAddr::V4(v4)))),
        IpAddr::V4(_) => false,
    }
}

fn is_cloudflare(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let addr = u32::from(v4);
            CLOUDFLARE_V4.iter().any(|(net, len)| {
                let mask = u32::MAX.checked_shl(32 - u32::from(*len)).unwrap_or(0);
                addr & mask == u32::from(*net) & mask
            })
        }
        IpAddr::V6(v6) => {
            let addr = u128::from(v6);
            CLOUDFLARE_V6.iter().any(|(net, len)| {
                let mask = u128::MAX.checked_shl(128 - u32::from(*len)).unwrap_or(0);
                addr & mask == u128::from(*net) & mask
            })
        }
    }
}

pub async fn rate_limit_middleware(
    State(limiter): State<Option<GlobalRateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(limiter) = limiter else {
        return next.run(request).await;
    };
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip());
    let path = request.uri().path().to_string();

    match classify(&path, request.headers(), peer, limiter.settings()) {
        Decision::Exempt(reason) => {
            debug!(reason, path = %path, "Rate limit bypassed");
            next.run(request).await
        }
        Decision::Limit(caller) => {
            if limiter.check(&caller).await {
                return next.run(request).await;
            }
            metrics::counter!("cufflink_rate_limited_total").increment(1);
            warn!(
                caller = %caller,
                method = %request.method(),
                path = %path,
                per_second = limiter.settings().per_second,
                burst = limiter.settings().burst,
                "Rate limited"
            );
            let mut response =
                (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response();
            if let Ok(value) = HeaderValue::from_str(&limiter.settings().window_secs().to_string())
            {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, routing::get, Router};
    use std::collections::HashMap;
    use tower::ServiceExt;

    fn try_settings(pairs: &[(&str, &str)]) -> Result<RateLimitSettings> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        RateLimitSettings::from_values(|name| map.get(name).cloned())
    }

    fn settings(pairs: &[(&str, &str)]) -> RateLimitSettings {
        try_settings(pairs).expect("valid settings")
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn disabled_by_default_with_generous_limits() {
        let s = settings(&[]);
        assert!(!s.enabled);
        assert_eq!((s.per_second, s.burst), (20, 60));
        assert_eq!(s.ip_source, ClientIpSource::Peer);
        assert!(s.exempt_private_peers);
        assert!(s.exempt_ips.is_empty());
        assert_eq!(s, RateLimitSettings::default());
        assert_eq!((s.window_secs(), s.window_limit()), (3, 60));
    }

    #[test]
    fn exempt_ips_parse_addresses_and_ranges() {
        let s = settings(&[(
            "RATE_LIMIT_EXEMPT_IPS",
            " 203.0.113.7, 2001:db8::/32 ,198.51.100.0/24,2001:db8::1, ",
        )]);
        let expected: Vec<IpNet> = [
            "203.0.113.7/32",
            "2001:db8::/32",
            "198.51.100.0/24",
            "2001:db8::1/128",
        ]
        .iter()
        .map(|net| net.parse().unwrap())
        .collect();
        assert_eq!(s.exempt_ips, expected);
        assert!(settings(&[("RATE_LIMIT_EXEMPT_IPS", "")])
            .exempt_ips
            .is_empty());
    }

    #[test]
    fn exempt_ips_reject_the_whole_setting_naming_the_bad_entry() {
        for raw in [
            "203.0.113.7,not-an-ip",
            "203.0.113.0/33",
            "example.com",
            "2001:db8::/129",
        ] {
            let err = try_settings(&[("RATE_LIMIT_EXEMPT_IPS", raw)]).unwrap_err();
            let bad = raw.rsplit(',').next().unwrap();
            let message = err.to_string();
            assert!(
                message.contains("RATE_LIMIT_EXEMPT_IPS") && message.contains(bad),
                "{raw}: {message}"
            );
        }
    }

    #[test]
    fn listed_client_ips_are_exempt_and_unlisted_are_not() {
        let s = settings(&[
            ("RATE_LIMIT_CLIENT_IP_SOURCE", "gclb"),
            ("RATE_LIMIT_EXEMPT_IPS", "203.0.113.7,2001:db8::/32"),
        ]);
        let gfe = Some(ip("35.191.10.20"));
        for forwarded in [
            "203.0.113.7,34.36.111.151",
            "2001:db8:1::9,34.36.111.151",
            "::ffff:203.0.113.7,34.36.111.151",
        ] {
            assert_eq!(
                classify(
                    "/svc/default/x/y",
                    &headers(&[("x-forwarded-for", forwarded)]),
                    gfe,
                    &s
                ),
                Decision::Exempt("listed"),
                "forwarded {forwarded}"
            );
        }
        assert_eq!(
            classify(
                "/svc/default/x/y",
                &headers(&[("x-forwarded-for", "203.0.113.8,34.36.111.151")]),
                gfe,
                &s
            ),
            Decision::Limit("ip:203.0.113.8".into())
        );
        assert_eq!(
            classify(
                "/svc/default/x/y",
                &headers(&[("x-forwarded-for", "203.0.113.7,203.0.113.8,34.36.111.151")]),
                gfe,
                &s
            ),
            Decision::Limit("ip:203.0.113.8".into())
        );
    }

    #[test]
    fn settings_parse_and_burst_never_below_rate() {
        let s = settings(&[
            ("RATE_LIMIT_ENABLED", "true"),
            ("RATE_LIMIT_PER_SECOND", "50"),
            ("RATE_LIMIT_BURST", "10"),
            ("RATE_LIMIT_CLIENT_IP_SOURCE", "GCLB"),
            ("RATE_LIMIT_EXEMPT_PRIVATE_PEERS", "false"),
        ]);
        assert!(s.enabled);
        assert_eq!((s.per_second, s.burst), (50, 50));
        assert_eq!(s.ip_source, ClientIpSource::Gclb);
        assert!(!s.exempt_private_peers);
        assert_eq!((s.window_secs(), s.window_limit()), (1, 50));
    }

    #[test]
    fn probes_and_in_cluster_callers_are_exempt() {
        let s = settings(&[("RATE_LIMIT_CLIENT_IP_SOURCE", "gclb")]);
        let public_gfe = Some(ip("35.191.10.20"));
        for path in ["/health", "/ready", "/metrics"] {
            assert_eq!(
                classify(path, &HeaderMap::new(), public_gfe, &s),
                Decision::Exempt("probe")
            );
        }
        for peer in [
            "10.60.1.66",
            "172.16.0.9",
            "192.168.1.2",
            "127.0.0.1",
            "fd00::1",
            "::ffff:10.0.0.5",
        ] {
            assert_eq!(
                classify(
                    "/svc/default/x/y",
                    &headers(&[("x-forwarded-for", "203.0.113.9")]),
                    Some(ip(peer)),
                    &s
                ),
                Decision::Exempt("in-cluster"),
                "peer {peer}"
            );
        }
        assert_eq!(
            classify(
                "/svc/default/x/y",
                &headers(&[("x-forwarded-for", "203.0.113.9,34.36.111.151")]),
                public_gfe,
                &s
            ),
            Decision::Limit("ip:203.0.113.9".into())
        );
    }

    #[test]
    fn exemption_can_be_turned_off() {
        let s = settings(&[("RATE_LIMIT_EXEMPT_PRIVATE_PEERS", "false")]);
        assert_eq!(
            classify("/api/x", &HeaderMap::new(), Some(ip("10.0.0.7")), &s),
            Decision::Limit("ip:10.0.0.7".into())
        );
    }

    #[test]
    fn gclb_ignores_client_supplied_forwarded_entries() {
        let h = headers(&[(
            "x-forwarded-for",
            "1.2.3.4, 9.9.9.9, 203.0.113.50, 34.36.111.151",
        )]);
        assert_eq!(
            client_ip(&h, Some(ip("35.191.0.1")), ClientIpSource::Gclb),
            Some(ip("203.0.113.50"))
        );
    }

    #[test]
    fn gclb_uses_cf_connecting_ip_only_behind_cloudflare() {
        let via_cf = headers(&[
            (
                "x-forwarded-for",
                "198.51.100.23, 172.70.1.2, 34.36.111.151",
            ),
            ("cf-connecting-ip", "198.51.100.23"),
        ]);
        assert_eq!(
            client_ip(&via_cf, None, ClientIpSource::Gclb),
            Some(ip("198.51.100.23"))
        );

        let forged = headers(&[
            ("x-forwarded-for", "203.0.113.77, 34.36.111.151"),
            ("cf-connecting-ip", "1.1.1.1"),
        ]);
        assert_eq!(
            client_ip(&forged, None, ClientIpSource::Gclb),
            Some(ip("203.0.113.77"))
        );
    }

    #[test]
    fn gclb_falls_back_to_single_entry_then_peer() {
        let one = headers(&[("x-forwarded-for", "203.0.113.1")]);
        assert_eq!(
            client_ip(&one, None, ClientIpSource::Gclb),
            Some(ip("203.0.113.1"))
        );
        assert_eq!(
            client_ip(
                &HeaderMap::new(),
                Some(ip("35.191.0.9")),
                ClientIpSource::Gclb
            ),
            Some(ip("35.191.0.9"))
        );
        let garbage = headers(&[("x-forwarded-for", "not-an-ip")]);
        assert_eq!(
            client_ip(&garbage, Some(ip("35.191.0.9")), ClientIpSource::Gclb),
            Some(ip("35.191.0.9"))
        );
    }

    #[test]
    fn peer_and_xff_first_sources() {
        let h = headers(&[
            ("x-forwarded-for", "203.0.113.7, 10.0.0.1"),
            ("authorization", "Bearer rotating"),
        ]);
        assert_eq!(
            client_ip(&h, Some(ip("35.191.0.1")), ClientIpSource::Peer),
            Some(ip("35.191.0.1"))
        );
        assert_eq!(
            client_ip(&h, Some(ip("35.191.0.1")), ClientIpSource::XffFirst),
            Some(ip("203.0.113.7"))
        );
        let real = headers(&[("x-real-ip", "198.51.100.4")]);
        assert_eq!(
            client_ip(&real, None, ClientIpSource::XffFirst),
            Some(ip("198.51.100.4"))
        );
        assert_eq!(
            client_ip(&HeaderMap::new(), None, ClientIpSource::XffFirst),
            None
        );
    }

    #[test]
    fn cloudflare_ranges() {
        assert!(is_cloudflare(ip("172.70.1.2")));
        assert!(is_cloudflare(ip("104.16.0.1")));
        assert!(is_cloudflare(ip("2606:4700::1")));
        assert!(is_cloudflare(ip("2a06:98c7::1")));
        assert!(!is_cloudflare(ip("203.0.113.1")));
        assert!(!is_cloudflare(ip("2001:db8::1")));
    }

    #[tokio::test]
    async fn without_redis_limits_in_memory_and_never_blocks() {
        let limiter = GlobalRateLimiter::new(
            RedisHandle::disabled(),
            settings(&[("RATE_LIMIT_PER_SECOND", "1"), ("RATE_LIMIT_BURST", "3")]),
        );
        for _ in 0..3 {
            assert!(limiter.check("ip:203.0.113.1").await);
        }
        assert!(!limiter.check("ip:203.0.113.1").await);
        assert!(limiter.check("ip:203.0.113.2").await);
    }

    #[tokio::test]
    async fn unreachable_redis_fails_open_to_in_memory_quickly() {
        let redis = RedisHandle::connect_in_background("redis://127.0.0.1:1").unwrap();
        let limiter = GlobalRateLimiter::new(redis, settings(&[]));
        let started = std::time::Instant::now();
        assert!(limiter.check("ip:203.0.113.3").await);
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    fn app(limiter: Option<GlobalRateLimiter>) -> Router {
        Router::new()
            .route("/api/thing", get(|| async { "ok" }))
            .route("/metrics", get(|| async { "metrics" }))
            .layer(axum::middleware::from_fn_with_state(
                limiter,
                rate_limit_middleware,
            ))
    }

    fn request(path: &str, peer: &str, xff: Option<&str>) -> Request {
        let mut builder = Request::builder().uri(path);
        if let Some(xff) = xff {
            builder = builder.header("x-forwarded-for", xff);
        }
        let mut req = builder.body(Body::empty()).unwrap();
        req.extensions_mut()
            .insert(ConnectInfo(SocketAddr::new(ip(peer), 443)));
        req
    }

    #[tokio::test]
    async fn disabled_switch_passes_everything() {
        let app = app(None);
        for _ in 0..200 {
            let res = app
                .clone()
                .oneshot(request(
                    "/api/thing",
                    "35.191.0.1",
                    Some("203.0.113.9,34.36.0.1"),
                ))
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn middleware_limits_public_clients_with_retry_after_but_not_probes_or_cluster() {
        let limiter = GlobalRateLimiter::new(
            RedisHandle::disabled(),
            settings(&[
                ("RATE_LIMIT_ENABLED", "true"),
                ("RATE_LIMIT_PER_SECOND", "1"),
                ("RATE_LIMIT_BURST", "2"),
                ("RATE_LIMIT_CLIENT_IP_SOURCE", "gclb"),
            ]),
        );
        let app = app(Some(limiter));
        let public = || request("/api/thing", "35.191.0.1", Some("203.0.113.9,34.36.0.1"));

        assert_eq!(
            app.clone().oneshot(public()).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            app.clone().oneshot(public()).await.unwrap().status(),
            StatusCode::OK
        );
        let limited = app.clone().oneshot(public()).await.unwrap();
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limited.headers().get(RETRY_AFTER).unwrap(), "2");

        for _ in 0..10 {
            let probe = app
                .clone()
                .oneshot(request(
                    "/metrics",
                    "35.191.0.1",
                    Some("203.0.113.9,34.36.0.1"),
                ))
                .await
                .unwrap();
            assert_eq!(probe.status(), StatusCode::OK);
            let internal = app
                .clone()
                .oneshot(request("/api/thing", "10.60.1.66", None))
                .await
                .unwrap();
            assert_eq!(internal.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn middleware_never_limits_a_listed_client_ip() {
        let limiter = GlobalRateLimiter::new(
            RedisHandle::disabled(),
            settings(&[
                ("RATE_LIMIT_ENABLED", "true"),
                ("RATE_LIMIT_PER_SECOND", "1"),
                ("RATE_LIMIT_BURST", "2"),
                ("RATE_LIMIT_CLIENT_IP_SOURCE", "gclb"),
                ("RATE_LIMIT_EXEMPT_IPS", "203.0.113.7,2001:db8::/32"),
            ]),
        );
        let app = app(Some(limiter));

        for forwarded in ["203.0.113.7,34.36.0.1", "2001:db8:1::9,34.36.0.1"] {
            for _ in 0..10 {
                let res = app
                    .clone()
                    .oneshot(request("/api/thing", "35.191.0.1", Some(forwarded)))
                    .await
                    .unwrap();
                assert_eq!(res.status(), StatusCode::OK, "forwarded {forwarded}");
            }
        }

        let other = || request("/api/thing", "35.191.0.1", Some("203.0.113.8,34.36.0.1"));
        for _ in 0..2 {
            assert_eq!(
                app.clone().oneshot(other()).await.unwrap().status(),
                StatusCode::OK
            );
        }
        assert_eq!(
            app.clone().oneshot(other()).await.unwrap().status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
