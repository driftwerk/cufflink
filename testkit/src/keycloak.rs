use crate::issuer::TestIssuer;
use serde_json::Value;

const ISSUER_BASE: &str = "https://auth.cufflink.test";

pub struct MockKeycloak {
    pub url: String,
    pub issuer_base: String,
    issuer: TestIssuer,
}

impl MockKeycloak {
    pub async fn start() -> Self {
        let issuer = TestIssuer::start_behind(ISSUER_BASE)
            .await
            .expect("bind mock keycloak");
        Self {
            url: issuer.url().to_string(),
            issuer_base: issuer.issuer_base().to_string(),
            issuer,
        }
    }

    pub fn set_discovery(&self, up: bool) {
        self.issuer.set_discovery(up);
    }

    pub fn token(&self, realm: &str, claims: Value) -> String {
        let Value::Object(claims) = claims else {
            panic!("the claims of a token are an object, not {claims}");
        };
        self.issuer
            .token(realm)
            .claims(claims)
            .sign()
            .expect("sign token")
    }
}
