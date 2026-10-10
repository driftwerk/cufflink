use super::body::Part;
use crate::errors::PlatformResult;
use crate::policy::{Correction, PolicyKey, Violation};
use crate::router::postures::{NamedPrincipal, ServiceCaller};
use crate::tenant_catalog::TenantEntry;
use axum::body::Bytes;
use axum::http::HeaderMap;
use base64::{engine::general_purpose, Engine};
use cufflink_types::host::request::{GuestRequest, MultipartField};
use cufflink_types::{CustomRouteDefinition, RoutePosture};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::net::IpAddr;

const SENSITIVE_HEADERS: [&str; 3] = ["authorization", "cookie", "proxy-authorization"];

const PLATFORM_HEADERS: [&str; 1] = [crate::auth::service_identity::HEADER];

pub(crate) fn strip_credential_headers(headers: &mut Map<String, Value>) {
    headers.retain(|name, _| {
        !SENSITIVE_HEADERS
            .iter()
            .chain(&[crate::router::postures::API_KEY_HEADER])
            .any(|credential| name.eq_ignore_ascii_case(credential))
    });
}

fn carries_sensitive_headers(headers: &HeaderMap) -> bool {
    SENSITIVE_HEADERS.iter().any(|h| headers.contains_key(*h))
}

pub(crate) fn guest_headers(
    tenant: &TenantEntry,
    service: &str,
    route: &CustomRouteDefinition,
    headers: &HeaderMap,
) -> Map<String, Value> {
    let unforwardable_credentials = (carries_sensitive_headers(headers) && !route.forward_auth)
        .then(|| {
            Violation::new(
                "auth_header_forwarded",
                format!(
                    "{service}/{} receives the caller's credentials without forward_auth",
                    route.handler
                ),
            )
        });
    let strip_sensitive =
        tenant.correct(PolicyKey::TenantBinding, unforwardable_credentials) == Correction::Correct;
    if strip_sensitive {
        metrics::counter!(
            "cufflink_route_credentials_stripped_total",
            "tenant" => tenant.tenant.slug.clone(),
            "service" => service.to_string(),
        )
        .increment(1);
    }
    guest_header_map(
        headers,
        strip_sensitive,
        posture_headers(route.posture.as_ref()),
    )
}

fn posture_headers(posture: Option<&RoutePosture>) -> &'static [&'static str] {
    match posture {
        Some(RoutePosture::ApiKey { .. }) => &[crate::router::postures::API_KEY_HEADER],
        _ => &[],
    }
}

fn guest_header_map(
    headers: &HeaderMap,
    strip_sensitive: bool,
    hidden: &[&str],
) -> Map<String, Value> {
    headers
        .iter()
        .filter(|(name, _)| {
            !PLATFORM_HEADERS.contains(&name.as_str()) && !hidden.contains(&name.as_str())
        })
        .filter(|(name, _)| !(strip_sensitive && SENSITIVE_HEADERS.contains(&name.as_str())))
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|v| (name.as_str().to_string(), Value::String(v.to_string())))
        })
        .collect()
}

pub(super) fn parse_body(body: &Bytes) -> Value {
    if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body)
            .unwrap_or(Value::String(String::from_utf8_lossy(body).into_owned()))
    }
}

fn with_params(body: Value, query: &HashMap<String, String>) -> Value {
    if query.is_empty() {
        return body;
    }
    let params = json!(query);
    match body {
        Value::Object(mut map) => {
            map.insert("params".to_string(), params);
            Value::Object(map)
        }
        Value::Null => json!({ "params": params }),
        other => json!({ "data": other, "params": params }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StrictTarget {
    pub path: String,
    pub params: Vec<(String, String)>,
}

pub(super) struct RequestV2<'a> {
    pub strict: Option<&'a StrictTarget>,
    pub query_string: &'a str,
    pub client_ip: Option<IpAddr>,
    pub multipart: Option<&'a [Part]>,
    pub principal: Option<NamedPrincipal>,
}

impl RequestV2<'_> {
    fn onto(self, legacy: GuestRequest) -> GuestRequest {
        let (api_key, scope) = match self.principal {
            Some(NamedPrincipal::ApiKey(partner)) => (Some(partner), None),
            Some(NamedPrincipal::Scope(scope)) => (None, Some(scope)),
            None => (None, None),
        };
        GuestRequest {
            path: self.strict.map(|strict| strict.path.clone()),
            path_params: self.strict.map(|strict| strict.params.clone()),
            query_string: Some(self.query_string.to_string()),
            query: Some(multi_value_query(self.query_string)),
            client_ip: self.client_ip.map(|ip| ip.to_string()),
            multipart: self
                .multipart
                .map(|parts| parts.iter().map(multipart_field).collect()),
            api_key,
            scope,
            ..legacy
        }
    }
}

fn multi_value_query(query_string: &str) -> BTreeMap<String, Vec<String>> {
    form_urlencoded::parse(query_string.as_bytes()).fold(
        BTreeMap::new(),
        |mut query, (key, value)| {
            query
                .entry(key.into_owned())
                .or_insert_with(Vec::new)
                .push(value.into_owned());
            query
        },
    )
}

fn multipart_field(part: &Part) -> MultipartField {
    MultipartField {
        name: part.name.clone(),
        filename: part.filename.clone(),
        content_type: part.content_type.clone(),
        data: part.data.to_vec(),
    }
}

pub(super) struct RouteRequest<'a> {
    pub method: &'a str,
    pub handler: &'a str,
    pub headers: Map<String, Value>,
    pub body: Value,
    pub raw_body: &'a [u8],
    pub query: &'a HashMap<String, String>,
    pub tenant: &'a str,
    pub service: &'a str,
    pub auth: Value,
    pub posture: Option<&'a RoutePosture>,
    pub caller: Option<&'a ServiceCaller>,
    pub v2: Option<RequestV2<'a>>,
}

impl RouteRequest<'_> {
    pub(super) fn into_json(self) -> PlatformResult<Value> {
        let legacy = GuestRequest {
            method: self.method.to_string(),
            handler: self.handler.to_string(),
            headers: Some(self.headers),
            body: with_params(self.body, self.query),
            body_raw_b64: Some(general_purpose::STANDARD.encode(self.raw_body)),
            tenant: self.tenant.to_string(),
            service: self.service.to_string(),
            auth: Some(self.auth),
            posture: Some(json!(self.posture)),
            caller: self.caller.map(ServiceCaller::to_json),
            ..GuestRequest::default()
        };
        crate::router::guest_json(&match self.v2 {
            Some(v2) => v2.onto(legacy),
            None => legacy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_request_that_is_kept_carries_no_credentials() {
        let mut headers: Map<String, Value> = [
            "authorization",
            "Cookie",
            "proxy-authorization",
            "X-Api-Key",
            "x-request-id",
            "stripe-signature",
        ]
        .into_iter()
        .map(|name| (name.to_string(), json!("value")))
        .collect();
        strip_credential_headers(&mut headers);
        assert_eq!(
            headers.keys().collect::<Vec<_>>(),
            ["stripe-signature", "x-request-id"]
        );
    }

    #[test]
    fn body_parses_as_json_or_falls_back_to_text() {
        assert_eq!(parse_body(&Bytes::new()), Value::Null);
        assert_eq!(
            parse_body(&Bytes::from_static(b"{\"a\":1}")),
            json!({"a": 1})
        );
        assert_eq!(
            parse_body(&Bytes::from_static(b"a=1&b=2")),
            json!("a=1&b=2")
        );
    }

    #[test]
    fn query_params_merge_into_every_body_shape() {
        let q = query(&[("page", "2")]);
        assert_eq!(
            with_params(json!({"a": 1}), &q),
            json!({"a": 1, "params": {"page": "2"}})
        );
        assert_eq!(
            with_params(Value::Null, &q),
            json!({"params": {"page": "2"}})
        );
        assert_eq!(
            with_params(json!([1, 2]), &q),
            json!({"data": [1, 2], "params": {"page": "2"}})
        );
        assert_eq!(with_params(json!("text"), &query(&[])), json!("text"));
    }

    fn caller() -> crate::auth::AuthUser {
        crate::auth::AuthUser {
            subject: "kc-user-1".into(),
            name: Some("Ada".into()),
            email: Some("ada@example.com".into()),
            tenant_id: uuid::Uuid::nil(),
            tenant_slug: "acme".into(),
            auth_method: crate::auth::extractor::AuthMethod::Jwt,
            token_info: Some(crate::auth::keycloak::TokenInfo {
                sub: "kc-user-1".into(),
                preferred_username: Some("ada".into()),
                name: Some("Ada".into()),
                email: Some("ada@example.com".into()),
                realm_roles: vec!["staff".into()],
                claims: HashMap::from([("customer_id".to_string(), json!("c-1"))]),
            }),
            is_service_account: false,
            is_platform_admin: false,
            has_deploy_authority: false,
            realm: Some("acme".into()),
        }
    }

    fn signed_headers() -> Map<String, Value> {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("Stripe-Signature", "t=1,v1=x"),
            ("X-Shopify-Hmac-Sha256", "h"),
            ("X-Sweep-Secret", "s"),
        ] {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                value.parse().expect("value"),
            );
        }
        guest_header_map(&headers, false, &[])
    }

    fn request_json(body: &[u8], auth: Value, posture: Option<&RoutePosture>) -> Value {
        RouteRequest {
            method: "POST",
            handler: "stripe_webhook",
            headers: signed_headers(),
            body: parse_body(&Bytes::copy_from_slice(body)),
            raw_body: body,
            query: &query(&[("source", "stripe")]),
            tenant: "acme",
            service: "payments",
            auth,
            posture,
            caller: None,
            v2: None,
        }
        .into_json()
        .expect("request")
    }

    #[test]
    fn legacy_request_json_is_unchanged_for_every_posture_and_caller() {
        let postures = [
            (None, Value::Null),
            (
                Some(RoutePosture::Public {
                    reason: "Stripe webhook".into(),
                }),
                json!({"kind": "public", "reason": "Stripe webhook"}),
            ),
            (
                Some(RoutePosture::Authenticated),
                json!({"kind": "authenticated"}),
            ),
            (
                Some(RoutePosture::Permission {
                    area: "orders".into(),
                    operation: "view".into(),
                }),
                json!({"kind": "permission", "area": "orders", "operation": "view"}),
            ),
            (
                Some(RoutePosture::Owner {
                    field: "customer_id".into(),
                }),
                json!({"kind": "owner", "field": "customer_id"}),
            ),
        ];
        let resolved = crate::auth::permissions::ResolvedPermissions {
            permissions: vec!["orders:view".into(), "orders:edit".into()],
            role_names: vec!["staff".into()],
        };
        let callers = [
            (Value::Null, Value::Null),
            (
                crate::router::authz::authenticated_guest_auth(&caller(), &resolved),
                json!({
                    "sub": "kc-user-1",
                    "preferred_username": "ada",
                    "name": "Ada",
                    "email": "ada@example.com",
                    "realm_roles": ["staff"],
                    "claims": {"customer_id": "c-1"},
                    "is_service_account": false,
                    "permissions": ["orders:view", "orders:edit"],
                    "role_names": ["staff"],
                }),
            ),
        ];
        for (posture, posture_json) in &postures {
            for (auth, auth_json) in &callers {
                assert_eq!(
                    request_json(b"{\"id\":\"evt_1\"}", auth.clone(), posture.as_ref()),
                    json!({
                        "method": "POST",
                        "handler": "stripe_webhook",
                        "headers": {
                            "stripe-signature": "t=1,v1=x",
                            "x-shopify-hmac-sha256": "h",
                            "x-sweep-secret": "s",
                        },
                        "body": {"id": "evt_1", "params": {"source": "stripe"}},
                        "body_raw_b64": "eyJpZCI6ImV2dF8xIn0=",
                        "tenant": "acme",
                        "service": "payments",
                        "auth": auth_json,
                        "posture": posture_json,
                    }),
                    "{posture:?} / {auth}"
                );
            }
        }
    }

    #[test]
    fn legacy_request_json_wraps_a_non_object_body_beside_its_params() {
        let request = request_json(b"[1,2]", Value::Null, None);
        assert_eq!(
            request["body"],
            json!({"data": [1, 2], "params": {"source": "stripe"}})
        );
        assert_eq!(request["body_raw_b64"], "WzEsMl0=");
        let request = request_json(b"a=1&b=2", Value::Null, None);
        assert_eq!(
            request["body"],
            json!({"data": "a=1&b=2", "params": {"source": "stripe"}})
        );
    }

    fn v2_request(v2: RequestV2<'_>) -> Value {
        RouteRequest {
            method: "POST",
            handler: "import",
            headers: Map::new(),
            body: Value::Null,
            raw_body: b"",
            query: &query(&[("tag", "b")]),
            tenant: "acme",
            service: "books",
            auth: Value::Null,
            posture: None,
            caller: None,
            v2: Some(v2),
        }
        .into_json()
        .expect("request")
    }

    #[test]
    fn a_route_reads_the_text_it_always_read() {
        assert_eq!(
            request_json(
                b"{\"id\":\"evt_1\"}",
                json!({"sub": "kc-user-1"}),
                Some(&RoutePosture::Permission {
                    area: "payments".into(),
                    operation: "edit".into()
                }),
            )
            .to_string(),
            r#"{"auth":{"sub":"kc-user-1"},"body":{"id":"evt_1","params":{"source":"stripe"}},"body_raw_b64":"eyJpZCI6ImV2dF8xIn0=","handler":"stripe_webhook","headers":{"stripe-signature":"t=1,v1=x","x-shopify-hmac-sha256":"h","x-sweep-secret":"s"},"method":"POST","posture":{"area":"payments","kind":"permission","operation":"edit"},"service":"payments","tenant":"acme"}"#
        );
        assert_eq!(
            request_json(b"", Value::Null, None).to_string(),
            r#"{"auth":null,"body":{"params":{"source":"stripe"}},"body_raw_b64":"","handler":"stripe_webhook","headers":{"stripe-signature":"t=1,v1=x","x-shopify-hmac-sha256":"h","x-sweep-secret":"s"},"method":"POST","posture":null,"service":"payments","tenant":"acme"}"#
        );
        let target = StrictTarget {
            path: "/books/7/import".into(),
            params: vec![("book_id".into(), "7".into())],
        };
        let parts = [Part {
            name: Some("file".into()),
            filename: Some("r.csv".into()),
            content_type: Some("text/csv".into()),
            data: Bytes::from_static(b"a,b"),
        }];
        assert_eq!(
            v2_request(RequestV2 {
                strict: Some(&target),
                query_string: "tag=a&tag=b&q=x%20y&empty",
                client_ip: Some("203.0.113.7".parse().expect("ip")),
                multipart: Some(&parts),
                principal: Some(NamedPrincipal::Scope(json!({"kind": "all"}))),
            })
            .to_string(),
            r#"{"auth":null,"body":{"params":{"tag":"b"}},"body_raw_b64":"","client_ip":"203.0.113.7","handler":"import","headers":{},"method":"POST","multipart":[{"content_type":"text/csv","data_b64":"YSxi","filename":"r.csv","name":"file"}],"path":"/books/7/import","path_params":[["book_id","7"]],"posture":null,"query":{"empty":[""],"q":["x y"],"tag":["a","b"]},"query_string":"tag=a&tag=b&q=x%20y&empty","scope":{"kind":"all"},"service":"books","tenant":"acme"}"#
        );
    }

    #[test]
    fn v2_requests_add_path_query_client_and_parts_to_the_legacy_keys() {
        let target = StrictTarget {
            path: "/books/7/import".into(),
            params: vec![("book_id".into(), "7".into())],
        };
        let parts = [Part {
            name: Some("file".into()),
            filename: Some("r.csv".into()),
            content_type: Some("text/csv".into()),
            data: Bytes::from_static(b"a,b"),
        }];
        let request = v2_request(RequestV2 {
            strict: Some(&target),
            query_string: "tag=a&tag=b&q=x%20y&empty",
            client_ip: Some("203.0.113.7".parse().expect("ip")),
            multipart: Some(&parts),
            principal: Some(NamedPrincipal::Scope(json!({"kind": "all"}))),
        });
        assert_eq!(request["body"], json!({"params": {"tag": "b"}}));
        assert_eq!(request["path"], "/books/7/import");
        assert_eq!(request["path_params"], json!([["book_id", "7"]]));
        assert_eq!(request["query_string"], "tag=a&tag=b&q=x%20y&empty");
        assert_eq!(
            request["query"],
            json!({"tag": ["a", "b"], "q": ["x y"], "empty": [""]})
        );
        assert_eq!(request["client_ip"], "203.0.113.7");
        assert_eq!(request["scope"], json!({"kind": "all"}));
        assert_eq!(
            request["multipart"],
            json!([{"name": "file", "filename": "r.csv", "content_type": "text/csv", "data_b64": "YSxi"}])
        );

        let fn_alias = v2_request(RequestV2 {
            strict: None,
            query_string: "",
            client_ip: None,
            multipart: None,
            principal: None,
        });
        for absent in [
            "path",
            "path_params",
            "client_ip",
            "multipart",
            "caller",
            "scope",
        ] {
            assert!(fn_alias.get(absent).is_none(), "{absent}: {fn_alias}");
        }
        assert_eq!(fn_alias["query"], json!({}));
        let legacy = request_json(b"{}", Value::Null, None);
        for absent in ["path", "query", "query_string", "client_ip", "multipart"] {
            assert!(legacy.get(absent).is_none(), "{absent}: {legacy}");
        }
    }

    #[test]
    fn carries_sensitive_headers_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "r".parse().expect("header"));
        assert!(!carries_sensitive_headers(&headers));
        for name in ["Authorization", "COOKIE", "Proxy-Authorization"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("name"),
                "x".parse().expect("value"),
            );
            assert!(carries_sensitive_headers(&headers), "{name}");
        }
    }

    #[test]
    fn a_verified_service_call_carries_its_caller_on_any_route() {
        let caller = ServiceCaller {
            service: "pricing".into(),
            on_behalf_of: None,
            depth: 3,
            remaining: std::time::Duration::from_secs(5),
        };
        let request = RouteRequest {
            method: "POST",
            handler: "reprice",
            headers: Map::new(),
            body: Value::Null,
            raw_body: b"",
            query: &HashMap::new(),
            tenant: "acme",
            service: "assets",
            auth: Value::Null,
            posture: None,
            caller: Some(&caller),
            v2: None,
        }
        .into_json()
        .expect("request");
        assert_eq!(
            request["caller"],
            json!({"service": "pricing", "on_behalf_of": null, "depth": 3})
        );
    }

    #[test]
    fn a_service_identity_never_reaches_a_guest() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::auth::service_identity::HEADER,
            "cfsi1.k.p.s".parse().expect("header"),
        );
        headers.insert("x-trace", "t".parse().expect("header"));
        for strip_sensitive in [false, true] {
            let seen = guest_header_map(&headers, strip_sensitive, &[]);
            assert_eq!(seen.keys().collect::<Vec<_>>(), vec!["x-trace"]);
        }
    }

    #[test]
    fn an_api_key_route_hides_the_partner_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "cfp_x_y".parse().expect("header"));
        let api_key = RoutePosture::ApiKey {
            scope: "books:ingest".into(),
        };
        assert!(guest_header_map(&headers, false, posture_headers(Some(&api_key))).is_empty());
        assert_eq!(
            guest_header_map(&headers, false, posture_headers(None))["x-api-key"],
            "cfp_x_y"
        );
    }

    #[test]
    fn guest_header_map_strips_only_when_asked() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer t".parse().expect("header"));
        headers.insert("cookie", "s=1".parse().expect("header"));
        headers.insert("stripe-signature", "t=1,v1=x".parse().expect("header"));
        let kept = guest_header_map(&headers, false, &[]);
        assert_eq!(kept.len(), 3);
        assert_eq!(kept["authorization"], "Bearer t");
        let stripped = guest_header_map(&headers, true, &[]);
        assert_eq!(
            stripped.keys().collect::<Vec<_>>(),
            vec!["stripe-signature"]
        );
    }
}
