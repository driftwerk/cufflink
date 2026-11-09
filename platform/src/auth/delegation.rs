use super::extractor::{AuthMethod, AuthUser};
use super::keycloak::TokenInfo;
use super::service_identity::{
    Claims, DelegatedMethod, DelegatedToken, Delegation, CLOCK_SKEW_SECS,
};
use crate::redis_conn::RedisHandle;
use crate::repo::tenants::Tenant;
use axum::http::Extensions;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use uuid::Uuid;

pub const REPLAY_MAX_ENV: &str = "SERVICE_IDENTITY_REPLAY_MAX";
pub const DEFAULT_REPLAY_MAX: usize = 262_144;
/// The verifier's skew, plus two replicas each up to that skew from true time.
const ANY_REPLICA_ACCEPTS_PAST_EXPIRY_SECS: i64 = 3 * CLOCK_SKEW_SECS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DelegationError {
    #[error("the delegated user's token has expired")]
    Expired,
    #[error("the delegated identity was already used")]
    Replayed,
    #[error("too many delegated identities are in flight to check this one")]
    ReplayCapacity,
    #[error("the delegated identity could not be checked for replay")]
    ReplayUnavailable,
}

impl DelegationError {
    pub fn reason(self) -> &'static str {
        match self {
            DelegationError::Expired => "delegation_expired",
            DelegationError::Replayed => "replayed",
            DelegationError::ReplayCapacity => "replay_capacity",
            DelegationError::ReplayUnavailable => "replay_unavailable",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MintedHere;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arrival {
    InProcess,
    Network,
}

impl Arrival {
    pub(crate) fn of(extensions: &Extensions) -> Self {
        match extensions.get::<MintedHere>() {
            Some(MintedHere) => Arrival::InProcess,
            None => Arrival::Network,
        }
    }
}

impl Delegation {
    pub fn of(user: &AuthUser) -> Option<Self> {
        let (method, expires_at) = match &user.auth_method {
            AuthMethod::Jwt => (
                DelegatedMethod::Jwt {},
                Some(
                    user.token_info
                        .as_ref()?
                        .claims
                        .get("exp")
                        .and_then(Value::as_i64)?,
                ),
            ),
            AuthMethod::ApiKey { id, user_sub } => (
                DelegatedMethod::ApiKey {
                    id: *id,
                    user_sub: user_sub.clone(),
                },
                None,
            ),
        };
        Some(Self {
            subject: user.subject.clone(),
            method,
            name: user.name.clone(),
            email: user.email.clone(),
            service_account: user.is_service_account,
            realm: user.realm.clone(),
            expires_at,
            token: user.token_info.as_ref().map(|token| DelegatedToken {
                preferred_username: token.preferred_username.clone(),
                realm_roles: token.realm_roles.clone(),
                claims: token
                    .claims
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            }),
        })
    }

    pub(crate) fn into_user(self, tenant: &Tenant) -> AuthUser {
        let token_info = self.token.map(|token| TokenInfo {
            sub: self.subject.clone(),
            preferred_username: token.preferred_username,
            name: self.name.clone(),
            email: self.email.clone(),
            realm_roles: token.realm_roles,
            claims: token.claims.into_iter().collect(),
        });
        AuthUser {
            subject: self.subject,
            name: self.name,
            email: self.email,
            tenant_id: tenant.id,
            tenant_slug: tenant.slug.clone(),
            auth_method: match self.method {
                DelegatedMethod::Jwt {} => AuthMethod::Jwt,
                DelegatedMethod::ApiKey { id, user_sub } => AuthMethod::ApiKey { id, user_sub },
            },
            token_info,
            is_service_account: self.service_account,
            is_platform_admin: false,
            has_deploy_authority: false,
            realm: self.realm,
        }
    }

    fn expired(&self, now: i64) -> bool {
        self.expires_at
            .is_some_and(|expires_at| expires_at < now.saturating_sub(CLOCK_SKEW_SECS))
    }
}

pub struct SeenIdentities {
    seen: Mutex<Seen>,
    capacity: usize,
}

#[derive(Default)]
struct Seen {
    expiring: HashMap<Uuid, i64>,
    earliest: Option<i64>,
}

impl Seen {
    fn forget(&mut self, live: impl Fn(&i64) -> bool) {
        self.expiring.retain(|_, expires_at| live(expires_at));
        self.earliest = self.expiring.values().min().copied();
    }

    fn remember(&mut self, jti: Uuid, expires_at: i64) {
        self.expiring.insert(jti, expires_at);
        self.earliest = Some(
            self.earliest
                .map_or(expires_at, |earliest| earliest.min(expires_at)),
        );
    }
}

impl SeenIdentities {
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: Mutex::new(Seen::default()),
            capacity,
        }
    }

    pub(crate) fn first_use(
        &self,
        jti: Uuid,
        expires_at: i64,
        now: i64,
    ) -> Result<(), DelegationError> {
        let live = |expires_at: &i64| expires_at.saturating_add(CLOCK_SKEW_SECS) >= now;
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.expiring.get(&jti).is_some_and(live) {
            return Err(DelegationError::Replayed);
        }
        if seen.expiring.len() >= self.capacity && seen.earliest.is_some_and(|e| !live(&e)) {
            seen.forget(live);
        }
        if seen.expiring.len() >= self.capacity {
            Err(DelegationError::ReplayCapacity)
        } else {
            seen.remember(jti, expires_at);
            Ok(())
        }
    }
}

pub(crate) async fn check(
    seen: &SeenIdentities,
    redis: &RedisHandle,
    claims: &Claims,
    arrival: Arrival,
    now: i64,
) -> Result<(), DelegationError> {
    let Some(delegation) = &claims.acting_for else {
        return Ok(());
    };
    if delegation.expired(now) {
        return Err(DelegationError::Expired);
    }
    seen.first_use(claims.jti, claims.expires_at, now)?;
    match arrival {
        Arrival::Network if redis.is_configured() => {
            claim(redis, claims.jti, claim_ttl(claims.expires_at, now)).await
        }
        Arrival::Network | Arrival::InProcess => Ok(()),
    }
}

fn claim_ttl(expires_at: i64, now: i64) -> i64 {
    expires_at
        .saturating_add(ANY_REPLICA_ACCEPTS_PAST_EXPIRY_SECS)
        .saturating_sub(now)
        .max(1)
}

async fn claim(redis: &RedisHandle, jti: Uuid, ttl_secs: i64) -> Result<(), DelegationError> {
    let key = format!("cufflink:sid:{jti}");
    redis
        .call(|mut conn| async move {
            redis::cmd("SET")
                .arg(&key)
                .arg(1)
                .arg("NX")
                .arg("EX")
                .arg(ttl_secs)
                .query_async::<Option<String>>(&mut conn)
                .await
        })
        .await
        .map_err(|_| DelegationError::ReplayUnavailable)?
        .map(drop)
        .ok_or(DelegationError::Replayed)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::auth::binding::tests::{api_key_user, jwt_user};
    use crate::tenant_catalog::tests::tenant;
    use serde_json::json;

    const NOW: i64 = 1_800_000_000;

    pub(crate) fn user(on: &Tenant) -> AuthUser {
        let mut user = jwt_user(on, "acme", "portal", &["loans-officer"]);
        user.subject = "clara".into();
        user.name = Some("Clara Clerk".into());
        user.email = Some("clara@example.com".into());
        if let Some(token) = user.token_info.as_mut() {
            token.sub = "clara".into();
            token.name = user.name.clone();
            token.email = user.email.clone();
            token.preferred_username = Some("clara".into());
            token.claims.insert("exp".into(), json!(NOW + 300));
        }
        user
    }

    pub(crate) fn claims(acting_for: Option<Delegation>) -> Claims {
        Claims {
            tenant_id: Uuid::nil(),
            caller_service: "catalog-service".into(),
            caller_service_id: Uuid::nil(),
            caller_version: 1,
            target_service: "loan-service".into(),
            method: "POST".into(),
            path: "/_fn/assess".into(),
            issued_at: NOW,
            expires_at: NOW + 30,
            jti: Uuid::new_v4(),
            depth: 1,
            on_behalf_of: acting_for.as_ref().map(|act| act.subject.clone()),
            acting_for,
        }
    }

    fn same_user(left: &AuthUser, right: &AuthUser) {
        assert_eq!(
            (
                &left.subject,
                &left.name,
                &left.email,
                left.tenant_id,
                &left.tenant_slug,
                &left.auth_method,
                left.is_service_account,
                &left.realm,
            ),
            (
                &right.subject,
                &right.name,
                &right.email,
                right.tenant_id,
                &right.tenant_slug,
                &right.auth_method,
                right.is_service_account,
                &right.realm,
            )
        );
        assert_eq!(left.to_auth_json(), right.to_auth_json());
        assert_eq!(
            crate::audit::subject(left),
            crate::audit::subject(right),
            "the audit subject"
        );
    }

    #[test]
    fn a_delegated_user_is_rebuilt_as_the_platform_verified_them() {
        let acme = tenant("acme", Some("acme"));
        let service_account = AuthUser {
            is_service_account: true,
            ..user(&acme)
        };
        let key_for_clara = AuthUser {
            auth_method: AuthMethod::ApiKey {
                id: Uuid::from_u128(9),
                user_sub: Some("clara".into()),
            },
            ..api_key_user(&acme, false)
        };
        for direct in [
            user(&acme),
            service_account,
            api_key_user(&acme, false),
            key_for_clara,
        ] {
            let rebuilt = Delegation::of(&direct).expect("delegable").into_user(&acme);
            same_user(&rebuilt, &direct);
        }
    }

    #[test]
    fn platform_admin_and_deploy_authority_are_never_delegated() {
        let acme = tenant("acme", Some("acme"));
        for elevated in [
            AuthUser {
                is_platform_admin: true,
                has_deploy_authority: true,
                ..user(&acme)
            },
            api_key_user(&acme, true),
            api_key_user(&acme, false),
        ] {
            let rebuilt = Delegation::of(&elevated)
                .expect("delegable")
                .into_user(&acme);
            assert!(!rebuilt.is_platform_admin && !rebuilt.has_deploy_authority);
        }
    }

    #[test]
    fn partner_keys_and_anonymous_callers_have_no_delegation() {
        use crate::auth::partner_keys::PartnerPrincipal;
        use crate::router::postures::Principal;
        let partner = Principal::Partner(PartnerPrincipal {
            id: Uuid::from_u128(3),
            name: "globex-feed".into(),
            scopes: vec!["loans:ingest".into()],
        });
        for principal in [partner, Principal::User(None), Principal::Service] {
            let (user, _) = principal.into_parts();
            assert_eq!(user.as_ref().and_then(Delegation::of), None);
        }
        let acme = tenant("acme", Some("acme"));
        let mut unbounded = user(&acme);
        if let Some(token) = unbounded.token_info.as_mut() {
            token.claims.remove("exp");
        }
        assert_eq!(
            Delegation::of(&unbounded),
            None,
            "a token without exp is never delegated"
        );
    }

    #[test]
    fn a_delegation_carries_the_users_expiry_and_api_keys_have_none() {
        let acme = tenant("acme", Some("acme"));
        assert_eq!(
            Delegation::of(&user(&acme)).and_then(|act| act.expires_at),
            Some(NOW + 300)
        );
        assert_eq!(
            Delegation::of(&api_key_user(&acme, false)).map(|act| act.expires_at),
            Some(None)
        );
    }

    async fn checked(claims: &Claims, now: i64) -> Result<(), DelegationError> {
        check(
            &SeenIdentities::new(8),
            &RedisHandle::disabled(),
            claims,
            Arrival::Network,
            now,
        )
        .await
    }

    #[tokio::test]
    async fn a_delegation_past_its_users_expiry_is_refused_after_verify() {
        let acme = tenant("acme", Some("acme"));
        let claims = claims(Delegation::of(&user(&acme)));
        assert_eq!(checked(&claims, NOW + 305).await, Ok(()), "within the skew");
        assert_eq!(
            checked(&claims, NOW + 306).await,
            Err(DelegationError::Expired)
        );
        let key = self::claims(Delegation::of(&api_key_user(&acme, false)));
        assert_eq!(
            checked(&key, NOW + 10_000).await,
            Ok(()),
            "no user expiry: the identity's lifetime alone bounds it"
        );
        assert_eq!(
            checked(&self::claims(None), NOW + 10_000).await,
            Ok(()),
            "service-only identities are not checked here"
        );
    }

    #[test]
    fn a_replayed_jti_is_refused_and_an_expired_one_is_forgotten() {
        let seen = SeenIdentities::new(2);
        let jti = Uuid::from_u128(1);
        assert_eq!(seen.first_use(jti, NOW + 30, NOW), Ok(()));
        assert_eq!(
            seen.first_use(jti, NOW + 30, NOW + 35),
            Err(DelegationError::Replayed)
        );
        assert_eq!(seen.first_use(Uuid::from_u128(2), NOW + 30, NOW), Ok(()));
        assert_eq!(
            seen.first_use(Uuid::from_u128(3), NOW + 90, NOW + 36),
            Ok(()),
            "a full set forgets what has expired"
        );
        assert_eq!(seen.first_use(jti, NOW + 30, NOW + 36), Ok(()));
    }

    #[test]
    fn a_full_seen_set_refuses_rather_than_forgets() {
        let seen = SeenIdentities::new(2);
        assert_eq!(seen.first_use(Uuid::from_u128(1), NOW + 30, NOW), Ok(()));
        assert_eq!(seen.first_use(Uuid::from_u128(2), NOW + 30, NOW), Ok(()));
        assert_eq!(
            seen.first_use(Uuid::from_u128(3), NOW + 30, NOW + 10),
            Err(DelegationError::ReplayCapacity)
        );
        assert_eq!(
            seen.first_use(Uuid::from_u128(1), NOW + 30, NOW + 10),
            Err(DelegationError::Replayed),
            "a full set still names a replay as one"
        );
        for later in 11..=35 {
            assert_eq!(
                seen.first_use(Uuid::from_u128(100 + later as u128), NOW + 90, NOW + later),
                Err(DelegationError::ReplayCapacity),
                "refused while every identity it holds is live"
            );
        }
        assert_eq!(
            seen.first_use(Uuid::from_u128(4), NOW + 90, NOW + 36),
            Ok(()),
            "admitted once the earliest has expired"
        );
    }

    #[tokio::test]
    async fn a_delegated_identity_is_single_use_on_each_path() {
        let acme = tenant("acme", Some("acme"));
        let claims = claims(Delegation::of(&user(&acme)));
        for arrival in [Arrival::InProcess, Arrival::Network] {
            let seen = SeenIdentities::new(8);
            let redis = RedisHandle::disabled();
            assert_eq!(check(&seen, &redis, &claims, arrival, NOW).await, Ok(()));
            assert_eq!(
                check(&seen, &redis, &claims, arrival, NOW + 1).await,
                Err(DelegationError::Replayed)
            );
        }
    }

    #[tokio::test]
    async fn an_unreachable_redis_refuses_a_networked_delegation_and_spares_the_in_process_one() {
        let acme = tenant("acme", Some("acme"));
        let redis = RedisHandle::connect_in_background("redis://127.0.0.1:1").expect("a handle");
        let seen = SeenIdentities::new(8);
        let delegated = claims(Delegation::of(&user(&acme)));
        assert_eq!(
            check(&seen, &redis, &delegated, Arrival::Network, NOW).await,
            Err(DelegationError::ReplayUnavailable)
        );
        let in_process = claims(Delegation::of(&user(&acme)));
        assert_eq!(
            check(&seen, &redis, &in_process, Arrival::InProcess, NOW).await,
            Ok(())
        );
        assert_eq!(
            check(&seen, &redis, &claims(None), Arrival::Network, NOW).await,
            Ok(()),
            "a service-only identity never asks Redis"
        );
    }

    #[tokio::test]
    async fn a_replay_to_another_replica_is_refused_through_redis() {
        let Some(url) = cufflink_testkit::infra()
            .await
            .and_then(|infra| infra.require_redis())
        else {
            return;
        };
        let redis = RedisHandle::connect_in_background(url).expect("a handle");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !redis.is_connected() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Redis connects");
        let acme = tenant("acme", Some("acme"));
        let delegated = claims(Delegation::of(&user(&acme)));
        let (first, second) = (SeenIdentities::new(8), SeenIdentities::new(8));
        assert_eq!(
            check(&first, &redis, &delegated, Arrival::Network, NOW).await,
            Ok(())
        );
        assert_eq!(
            check(&second, &redis, &delegated, Arrival::Network, NOW + 1).await,
            Err(DelegationError::Replayed),
            "another replica's seen-set has not seen it, Redis has"
        );
        let key = format!("cufflink:sid:{}", delegated.jti);
        let ttl = redis
            .call(|mut conn| async move {
                redis::cmd("TTL")
                    .arg(&key)
                    .query_async::<i64>(&mut conn)
                    .await
            })
            .await
            .expect("a TTL");
        assert!(
            (41..=45).contains(&ttl),
            "kept until no replica accepts it: {ttl}"
        );
        let fresh = claims(Delegation::of(&user(&acme)));
        assert_eq!(
            check(&second, &redis, &fresh, Arrival::Network, NOW).await,
            Ok(())
        );
    }

    #[test]
    fn a_claim_outlives_the_identity_on_every_replica_that_could_accept_it() {
        let expires_at = NOW + 30;
        let accepted_until = |clock_offset: i64| expires_at + CLOCK_SKEW_SECS - clock_offset;
        for claimed_by in [-CLOCK_SKEW_SECS, 0, CLOCK_SKEW_SECS] {
            let released_at = NOW + claim_ttl(expires_at, NOW + claimed_by);
            for verified_by in [-CLOCK_SKEW_SECS, 0, CLOCK_SKEW_SECS] {
                assert!(
                    released_at >= accepted_until(verified_by),
                    "claimed {claimed_by} s off true time, verified {verified_by} s off"
                );
            }
        }
        assert_eq!(claim_ttl(NOW, NOW + 3_600), 1, "never zero or negative");
    }

    #[test]
    fn only_a_request_marked_by_local_dispatch_arrived_in_process() {
        let mut extensions = Extensions::new();
        assert_eq!(Arrival::of(&extensions), Arrival::Network);
        extensions.insert(MintedHere);
        assert_eq!(Arrival::of(&extensions), Arrival::InProcess);
    }

    #[test]
    fn refusal_reasons_are_stable_labels() {
        assert_eq!(
            [
                DelegationError::Expired,
                DelegationError::Replayed,
                DelegationError::ReplayCapacity,
                DelegationError::ReplayUnavailable,
            ]
            .map(DelegationError::reason),
            [
                "delegation_expired",
                "replayed",
                "replay_capacity",
                "replay_unavailable"
            ]
        );
    }
}
