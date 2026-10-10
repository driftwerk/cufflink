use super::secret_keys::{self, KeyKind};
use crate::errors::{PlatformError, PlatformResult};
use crate::policy::record::LogThrottle;
use crate::repo::runner_keys::{self, NewRunnerKey, RunnerKey, StoredRunnerKey};
use crate::repo::runner_services::Runs;
use crate::AppState;
use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const KIND: KeyKind = KeyKind::new("cfr_");
pub const SCHEME: &str = "RunnerKey";
const TOUCH_EVERY: Duration = Duration::from_secs(60);

static TOUCHED: LazyLock<LogThrottle<Uuid>> = LazyLock::new(|| LogThrottle::new(TOUCH_EVERY));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerAuth {
    pub key_id: Uuid,
    pub tenant_id: Uuid,
    pub tenant_slug: String,
    pub runs: Runs,
}

fn presented(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix(SCHEME)?
        .strip_prefix(' ')
}

fn check(stored: &StoredRunnerKey, secret: &str) -> Option<RunnerAuth> {
    (secret_keys::proves(secret, &stored.secret_hash) && stored.key.revoked_at.is_none()).then(
        || RunnerAuth {
            key_id: stored.key.id,
            tenant_id: stored.key.tenant_id,
            tenant_slug: stored.tenant_slug.clone(),
            runs: stored.key.runs,
        },
    )
}

pub(crate) async fn authenticate(
    pool: &deadpool_postgres::Pool,
    key: &str,
) -> PlatformResult<Option<RunnerAuth>> {
    let Some((prefix, secret)) = KIND.parse(key) else {
        return Ok(None);
    };
    Ok(runner_keys::find_by_prefix(pool, prefix)
        .await?
        .and_then(|stored| check(&stored, secret)))
}

pub(crate) async fn mint(
    pool: &deadpool_postgres::Pool,
    tenant_id: Uuid,
    name: &str,
    runs: Runs,
    created_by: &str,
) -> PlatformResult<(RunnerKey, String)> {
    secret_keys::mint(KIND, |prefix, secret| async move {
        runner_keys::create(
            pool,
            &NewRunnerKey {
                tenant_id,
                name,
                runs,
                prefix: &prefix,
                secret: &secret,
                created_by,
            },
        )
        .await
    })
    .await
}

impl FromRequestParts<Arc<AppState>> for RunnerAuth {
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let refused = || PlatformError::Unauthorized("Invalid runner key".into());
        let key = presented(&parts.headers).ok_or_else(refused)?;
        let runner = authenticate(&state.platform_db, key)
            .await?
            .ok_or_else(refused)?;
        if TOUCHED.should_log(runner.key_id, Instant::now()) {
            if let Err(e) = runner_keys::touch(&state.platform_db, runner.key_id).await {
                tracing::warn!(key_id = %runner.key_id, error = %e, "Could not record a runner key's last use");
            }
        }
        Ok(runner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::runner_keys::RunnerKey;

    fn stored(secret: &str) -> StoredRunnerKey {
        StoredRunnerKey {
            key: RunnerKey {
                id: Uuid::from_u128(1),
                tenant_id: Uuid::from_u128(2),
                name: "acme-runner".into(),
                runs: Runs::Web,
                prefix: "AAAAAAAAAAAA".into(),
                created_by: "admin".into(),
                created_at: chrono::Utc::now(),
                revoked_at: None,
                last_used_at: None,
            },
            secret_hash: secret_keys::secret_hash(secret),
            tenant_slug: "acme".into(),
        }
    }

    fn headers(authorization: &str) -> HeaderMap {
        [(AUTHORIZATION, authorization.parse().expect("a header"))]
            .into_iter()
            .collect()
    }

    #[test]
    fn a_runner_key_is_presented_under_its_own_scheme() {
        let key = KIND.format("AAAAAAAAAAAA", "s3cret");
        assert_eq!(
            presented(&headers(&format!("RunnerKey {key}"))),
            Some(key.as_str())
        );
        for refused in [
            format!("ApiKey {key}"),
            format!("Bearer {key}"),
            format!("RunnerKey{key}"),
        ] {
            assert_eq!(presented(&headers(&refused)), None, "{refused}");
        }
        assert_eq!(presented(&HeaderMap::new()), None);
        for other in ["cfp_AAAAAAAAAAAA_s3cret", "ck_abc"] {
            assert_eq!(KIND.parse(other), None, "{other}");
        }
    }

    #[test]
    fn a_key_authenticates_its_tenants_runner_until_it_is_revoked() {
        let key = stored("s3cret");
        assert_eq!(
            check(&key, "s3cret"),
            Some(RunnerAuth {
                key_id: Uuid::from_u128(1),
                tenant_id: Uuid::from_u128(2),
                tenant_slug: "acme".into(),
                runs: Runs::Web,
            })
        );
        assert_eq!(check(&key, "guess"), None);
        let revoked = StoredRunnerKey {
            key: RunnerKey {
                revoked_at: Some(chrono::Utc::now()),
                ..key.key.clone()
            },
            ..key
        };
        assert_eq!(check(&revoked, "s3cret"), None);
    }
}
