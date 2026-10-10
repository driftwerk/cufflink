use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use chrono::{DateTime, Utc};
use eyre::{eyre, Result, WrapErr};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use reqwest::Client;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::time::Duration;

const HOST: &str = "storage.googleapis.com";
const SCOPE_TAIL: &str = "auto/storage/goog4_request";
const ALGORITHM: &str = "GOOG4-RSA-SHA256";
const SIGNED_HEADERS: &str = "host";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const MAX_EXPIRES_SECS: u64 = 7 * 24 * 60 * 60;

const METADATA_BASE_DEFAULT: &str = "http://metadata.google.internal";
const IAM_BASE_DEFAULT: &str = "https://iamcredentials.googleapis.com";

const OBJECT_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

const QUERY_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

#[async_trait]
pub(crate) trait BlobSigner: Send + Sync {
    async fn signer_email(&self) -> Result<String>;
    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>>;
}

pub(crate) async fn sign_url<S: BlobSigner + ?Sized>(
    signer: &S,
    now: DateTime<Utc>,
    method: &http::Method,
    bucket: &str,
    key: &str,
    expires_secs: u64,
) -> Result<String> {
    if expires_secs == 0 || expires_secs > MAX_EXPIRES_SECS {
        return Err(eyre!(
            "GCS V4 expires_secs must be 1..={MAX_EXPIRES_SECS}, got {expires_secs}"
        ));
    }
    if bucket.is_empty() {
        return Err(eyre!("GCS V4 sign: bucket is empty"));
    }
    if key.is_empty() {
        return Err(eyre!("GCS V4 sign: key is empty"));
    }

    let email = signer.signer_email().await?;
    let query = build_query(&email, now, expires_secs);
    let canonical = canonical_request(method.as_str(), bucket, key, &query);
    let sts = string_to_sign(now, &credential_scope(now), &canonical);
    let sig = signer.sign(sts.as_bytes()).await?;
    Ok(assemble_url(bucket, key, &query, &hex::encode(sig)))
}

fn build_query(email: &str, now: DateTime<Utc>, expires_secs: u64) -> BTreeMap<String, String> {
    let mut q = BTreeMap::new();
    q.insert("X-Goog-Algorithm".into(), ALGORITHM.into());
    q.insert(
        "X-Goog-Credential".into(),
        format!("{email}/{}", credential_scope(now)),
    );
    q.insert("X-Goog-Date".into(), iso8601_basic(now));
    q.insert("X-Goog-Expires".into(), expires_secs.to_string());
    q.insert("X-Goog-SignedHeaders".into(), SIGNED_HEADERS.into());
    q
}

fn canonical_request(
    method: &str,
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
) -> String {
    let canonical_uri = format!("/{bucket}/{}", encode_object(key));
    let canonical_query = encode_query(query);
    let canonical_headers = format!("host:{HOST}\n");
    format!(
        "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{SIGNED_HEADERS}\n{UNSIGNED_PAYLOAD}"
    )
}

fn string_to_sign(now: DateTime<Utc>, scope: &str, canonical: &str) -> String {
    let mut h = Sha256::new();
    h.update(canonical.as_bytes());
    format!(
        "{ALGORITHM}\n{}\n{scope}\n{}",
        iso8601_basic(now),
        hex::encode(h.finalize())
    )
}

fn assemble_url(
    bucket: &str,
    key: &str,
    query: &BTreeMap<String, String>,
    signature_hex: &str,
) -> String {
    format!(
        "https://{HOST}/{bucket}/{}?{}&X-Goog-Signature={signature_hex}",
        encode_object(key),
        encode_query(query),
    )
}

fn encode_object(key: &str) -> String {
    utf8_percent_encode(key, OBJECT_ENCODE_SET).to_string()
}

fn encode_query(query: &BTreeMap<String, String>) -> String {
    query
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                utf8_percent_encode(k, QUERY_ENCODE_SET),
                utf8_percent_encode(v, QUERY_ENCODE_SET),
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn credential_scope(now: DateTime<Utc>) -> String {
    format!("{}/{SCOPE_TAIL}", now.format("%Y%m%d"))
}

fn iso8601_basic(now: DateTime<Utc>) -> String {
    now.format("%Y%m%dT%H%M%SZ").to_string()
}

pub(crate) struct IamSignBlobSigner {
    http: Client,
    metadata_base: String,
    iam_base: String,
}

impl IamSignBlobSigner {
    pub(crate) fn new() -> Result<Self> {
        Self::with_endpoints(METADATA_BASE_DEFAULT, IAM_BASE_DEFAULT)
    }

    fn with_endpoints(metadata_base: &str, iam_base: &str) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .wrap_err("building reqwest client for IAM signer")?;
        Ok(Self {
            http,
            metadata_base: metadata_base.trim_end_matches('/').to_string(),
            iam_base: iam_base.trim_end_matches('/').to_string(),
        })
    }

    async fn access_token(&self) -> Result<String> {
        #[derive(Deserialize)]
        struct TokenResp {
            access_token: String,
        }
        let url = format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/token",
            self.metadata_base
        );
        let resp = self
            .http
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .wrap_err("metadata server token request failed")?
            .error_for_status()
            .wrap_err("metadata server returned non-2xx for token")?
            .json::<TokenResp>()
            .await
            .wrap_err("metadata server token response was not valid JSON")?;
        Ok(resp.access_token)
    }
}

#[async_trait]
impl BlobSigner for IamSignBlobSigner {
    async fn signer_email(&self) -> Result<String> {
        let url = format!(
            "{}/computeMetadata/v1/instance/service-accounts/default/email",
            self.metadata_base
        );
        let email = self
            .http
            .get(url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .wrap_err("metadata server email request failed")?
            .error_for_status()
            .wrap_err("metadata server returned non-2xx for email")?
            .text()
            .await
            .wrap_err("metadata server email response was not text")?;
        Ok(email.trim().to_string())
    }

    async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        #[derive(Deserialize)]
        struct SignResp {
            #[serde(rename = "signedBlob")]
            signed_blob: String,
        }
        let token = self.access_token().await?;
        let email = self.signer_email().await?;
        let url = format!(
            "{}/v1/projects/-/serviceAccounts/{email}:signBlob",
            self.iam_base
        );
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .json(&serde_json::json!({ "payload": B64.encode(payload) }))
            .send()
            .await
            .wrap_err("IAM signBlob request failed")?;

        let status = resp.status();
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(eyre!(
                "IAM signBlob returned 403 — grant `roles/iam.serviceAccountTokenCreator` on the workload GSA to itself (self-impersonation) so it can sign blobs for its own identity"
            ));
        }
        let parsed = resp
            .error_for_status()
            .wrap_err("IAM signBlob returned non-2xx")?
            .json::<SignResp>()
            .await
            .wrap_err("IAM signBlob response was not valid JSON")?;
        B64.decode(parsed.signed_blob.as_bytes())
            .wrap_err("IAM signBlob `signedBlob` was not valid base64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::Mutex;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn fixed_now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 30, 12, 0, 0).unwrap()
    }

    struct StubSigner {
        email: &'static str,
        sig: Vec<u8>,
        captured: Mutex<Option<Vec<u8>>>,
    }

    #[async_trait]
    impl BlobSigner for StubSigner {
        async fn signer_email(&self) -> Result<String> {
            Ok(self.email.to_string())
        }
        async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
            *self.captured.lock().unwrap() = Some(payload.to_vec());
            Ok(self.sig.clone())
        }
    }

    #[test]
    fn encode_object_preserves_slashes_and_encodes_specials() {
        assert_eq!(
            encode_object("sailing_media/abc/bag_collection/x y.jpg"),
            "sailing_media/abc/bag_collection/x%20y.jpg"
        );
        assert_eq!(encode_object("a/b+c?d"), "a/b%2Bc%3Fd");
        assert_eq!(encode_object("héllo/wörld"), "h%C3%A9llo/w%C3%B6rld");
    }

    #[test]
    fn query_is_sorted_and_percent_encoded() {
        let q = build_query("sa@p.iam.gserviceaccount.com", fixed_now(), 300);
        let encoded = encode_query(&q);
        let parts: Vec<&str> = encoded.split('&').collect();
        let sorted = {
            let mut s = parts.clone();
            s.sort();
            s
        };
        assert_eq!(
            parts, sorted,
            "query string must be lexicographically sorted"
        );
        assert!(encoded.contains("X-Goog-Algorithm=GOOG4-RSA-SHA256"));
        assert!(encoded.contains("X-Goog-Date=20260530T120000Z"));
        assert!(encoded.contains("X-Goog-Expires=300"));
        assert!(encoded.contains("X-Goog-SignedHeaders=host"));
        assert!(encoded.contains(
            "X-Goog-Credential=sa%40p.iam.gserviceaccount.com%2F20260530%2Fauto%2Fstorage%2Fgoog4_request"
        ));
    }

    #[test]
    fn canonical_request_shape_is_byte_exact() {
        let q = build_query("sa@p.iam.gserviceaccount.com", fixed_now(), 300);
        let cr = canonical_request("PUT", "my-bucket", "uploads/photo.jpg", &q);
        let expected = format!(
            "PUT\n/my-bucket/uploads/photo.jpg\n{}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD",
            encode_query(&q),
        );
        assert_eq!(cr, expected);
    }

    #[test]
    fn string_to_sign_format() {
        let canonical = "PUT\n/b/k\n\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD";
        let sts = string_to_sign(fixed_now(), &credential_scope(fixed_now()), canonical);
        let mut h = Sha256::new();
        h.update(canonical.as_bytes());
        let expected = format!(
            "GOOG4-RSA-SHA256\n20260530T120000Z\n20260530/auto/storage/goog4_request\n{}",
            hex::encode(h.finalize())
        );
        assert_eq!(sts, expected);
    }

    #[tokio::test]
    async fn sign_url_assembles_expected_put_url() {
        let stub = StubSigner {
            email: "sa@p.iam.gserviceaccount.com",
            sig: vec![0xde, 0xad, 0xbe, 0xef],
            captured: Mutex::new(None),
        };
        let url = sign_url(
            &stub,
            fixed_now(),
            &http::Method::PUT,
            "my-bucket",
            "sailing_media/abc/x.jpg",
            300,
        )
        .await
        .unwrap();

        assert!(
            url.starts_with("https://storage.googleapis.com/my-bucket/sailing_media/abc/x.jpg?"),
            "got {url}"
        );
        assert!(url.ends_with("&X-Goog-Signature=deadbeef"));
        assert!(url.contains("X-Goog-Algorithm=GOOG4-RSA-SHA256"));
        assert!(url.contains("X-Goog-Date=20260530T120000Z"));
        assert!(url.contains("X-Goog-Expires=300"));
        assert!(url.contains("X-Goog-SignedHeaders=host"));

        let captured = stub.captured.lock().unwrap().clone().unwrap();
        let s = String::from_utf8(captured).unwrap();
        assert!(s.starts_with(
            "GOOG4-RSA-SHA256\n20260530T120000Z\n20260530/auto/storage/goog4_request\n"
        ));
    }

    #[tokio::test]
    async fn sign_url_signs_the_method_into_the_canonical_request() {
        let stub = StubSigner {
            email: "sa@p.iam.gserviceaccount.com",
            sig: vec![0x01],
            captured: Mutex::new(None),
        };
        let url = sign_url(
            &stub,
            fixed_now(),
            &http::Method::GET,
            "my-bucket",
            "media/professional/photo.jpg",
            300,
        )
        .await
        .unwrap();
        assert!(url
            .starts_with("https://storage.googleapis.com/my-bucket/media/professional/photo.jpg?"));

        let q = build_query("sa@p.iam.gserviceaccount.com", fixed_now(), 300);
        let canonical = canonical_request("GET", "my-bucket", "media/professional/photo.jpg", &q);
        let expected_sts = string_to_sign(fixed_now(), &credential_scope(fixed_now()), &canonical);
        let captured = stub.captured.lock().unwrap().clone().unwrap();
        assert_eq!(String::from_utf8(captured).unwrap(), expected_sts);
    }

    #[tokio::test]
    async fn sign_url_rejects_zero_or_excessive_expiry() {
        let stub = StubSigner {
            email: "sa@p",
            sig: vec![1],
            captured: Mutex::new(None),
        };
        let m = http::Method::PUT;
        assert!(sign_url(&stub, fixed_now(), &m, "b", "k", 0).await.is_err());
        assert!(
            sign_url(&stub, fixed_now(), &m, "b", "k", MAX_EXPIRES_SECS + 1)
                .await
                .is_err()
        );
        assert!(sign_url(&stub, fixed_now(), &m, "", "k", 300)
            .await
            .is_err());
        assert!(sign_url(&stub, fixed_now(), &m, "b", "", 300)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn iam_signer_happy_path_against_mock() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(
                "/computeMetadata/v1/instance/service-accounts/default/email",
            ))
            .and(header("Metadata-Flavor", "Google"))
            .respond_with(
                ResponseTemplate::new(200).set_body_string("svc@p.iam.gserviceaccount.com"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/computeMetadata/v1/instance/service-accounts/default/token",
            ))
            .and(header("Metadata-Flavor", "Google"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "tok-xyz",
                "expires_in": 3600,
                "token_type": "Bearer"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/v1/projects/-/serviceAccounts/svc@p.iam.gserviceaccount.com:signBlob",
            ))
            .and(header("authorization", "Bearer tok-xyz"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "signedBlob": B64.encode([0xab, 0xcd])
            })))
            .mount(&server)
            .await;

        let signer = IamSignBlobSigner::with_endpoints(&server.uri(), &server.uri()).unwrap();
        assert_eq!(
            signer.signer_email().await.unwrap(),
            "svc@p.iam.gserviceaccount.com"
        );
        assert_eq!(signer.sign(b"payload").await.unwrap(), vec![0xab, 0xcd]);
    }

    #[tokio::test]
    async fn iam_signer_403_names_the_role() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/computeMetadata/v1/instance/service-accounts/default/email",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_string("svc@p"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/computeMetadata/v1/instance/service-accounts/default/token",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "tok", "expires_in": 3600, "token_type": "Bearer"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/projects/-/serviceAccounts/svc@p:signBlob"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;

        let signer = IamSignBlobSigner::with_endpoints(&server.uri(), &server.uri()).unwrap();
        let err = signer.sign(b"x").await.unwrap_err().to_string();
        assert!(
            err.contains("roles/iam.serviceAccountTokenCreator"),
            "error should name the missing role, got: {err}"
        );
    }
}
