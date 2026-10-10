use crate::auth::extractor::AuthMethod;
use crate::auth::AuthUser;
use crate::nats_conn::jetstream_publish_bounded;
use crate::AppState;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const ANONYMOUS: &str = "anonymous";
pub const REDACTED: &str = "[redacted]";
pub const DURABLE_VERSION: u8 = 2;
pub const READ_IDS_KEPT: usize = 100;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_MAX_BYTES: i64 = 1024 * 1024 * 1024;
const COALESCED_MOST: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditConfig {
    pub max_age: Duration,
    pub max_bytes: Option<i64>,
    pub durable_all: bool,
    pub read_sample_percent: u8,
    pub read_coalesce: Option<Duration>,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            max_age: DAY * 90,
            max_bytes: Some(DEFAULT_MAX_BYTES),
            durable_all: false,
            read_sample_percent: 100,
            read_coalesce: None,
        }
    }
}

impl AuditConfig {
    pub fn from_env() -> eyre::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let (defaults, vars) = (Self::default(), cufflink_db::Vars(var));
        Ok(Self {
            max_age: DAY
                * vars.positive(
                    "CUFFLINK_AUDIT_MAX_AGE_DAYS",
                    u32::try_from(defaults.max_age.as_secs() / DAY.as_secs())?,
                )?,
            max_bytes: match vars.parsed(
                "CUFFLINK_AUDIT_MAX_BYTES",
                defaults.max_bytes.unwrap_or_default().unsigned_abs(),
            )? {
                0 => None,
                bytes => Some(i64::try_from(bytes)?),
            },
            durable_all: vars.parsed("CUFFLINK_AUDIT_DURABLE_ALL", defaults.durable_all)?,
            read_sample_percent: match vars.parsed(
                "CUFFLINK_AUDIT_READ_SAMPLE_PERCENT",
                defaults.read_sample_percent,
            )? {
                percent @ 1..=100 => percent,
                _ => eyre::bail!("CUFFLINK_AUDIT_READ_SAMPLE_PERCENT must be from 1 to 100"),
            },
            read_coalesce: Some(Duration::from_secs(
                vars.parsed("CUFFLINK_AUDIT_READ_COALESCE_SECS", 0)?,
            ))
            .filter(|window| !window.is_zero()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ActorKind {
    User,
    ServiceAccount,
    ApiKey,
    PartnerKey,
    Service,
    Anonymous,
}

impl ActorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ActorKind::User => "user",
            ActorKind::ServiceAccount => "service_account",
            ActorKind::ApiKey => "api_key",
            ActorKind::PartnerKey => "partner_key",
            ActorKind::Service => "service",
            ActorKind::Anonymous => ANONYMOUS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Actor {
    pub sub: String,
    pub kind: ActorKind,
    pub key_id: Option<Uuid>,
    pub azp: Option<String>,
}

impl Actor {
    pub fn anonymous() -> Self {
        Self {
            sub: ANONYMOUS.to_string(),
            kind: ActorKind::Anonymous,
            key_id: None,
            azp: None,
        }
    }

    pub fn service(name: &str) -> Self {
        Self {
            sub: name.to_string(),
            kind: ActorKind::Service,
            key_id: None,
            azp: None,
        }
    }

    pub fn partner(key_id: Uuid, name: &str) -> Self {
        Self {
            sub: name.to_string(),
            kind: ActorKind::PartnerKey,
            key_id: Some(key_id),
            azp: None,
        }
    }

    pub fn of(caller: &Option<AuthUser>) -> Self {
        let Some(user) = caller else {
            return Self::anonymous();
        };
        match &user.auth_method {
            AuthMethod::ApiKey { id, .. } => Self {
                sub: subject(user).to_string(),
                kind: ActorKind::ApiKey,
                key_id: Some(*id),
                azp: None,
            },
            AuthMethod::Jwt => Self {
                sub: user.subject.clone(),
                kind: if user.is_service_account {
                    ActorKind::ServiceAccount
                } else {
                    ActorKind::User
                },
                key_id: None,
                azp: user
                    .token_info
                    .as_ref()
                    .and_then(|token| token.claims.get("azp")?.as_str())
                    .map(str::to_string),
            },
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "sub": self.sub,
            "kind": self.kind.as_str(),
            "key_id": self.key_id,
            "azp": self.azp,
        })
    }
}

pub fn subject(user: &AuthUser) -> &str {
    match &user.auth_method {
        AuthMethod::ApiKey {
            user_sub: Some(minted_for),
            ..
        } => minted_for,
        _ => &user.subject,
    }
}

pub fn named(caller: &Option<AuthUser>) -> (String, String) {
    let user_id = caller
        .as_ref()
        .map_or(ANONYMOUS, |user| user.subject.as_str());
    let user_name = caller
        .as_ref()
        .and_then(|user| user.name.as_deref())
        .unwrap_or_default();
    (user_id.to_string(), user_name.to_string())
}

pub fn subject_of(caller: &Option<AuthUser>) -> String {
    caller.as_ref().map_or(ANONYMOUS, subject).to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditRecord {
    pub timestamp: DateTime<Utc>,
    pub tenant: String,
    pub service: String,
    pub table: Option<String>,
    pub action: String,
    pub user_id: String,
    pub user_name: String,
    pub subject: String,
    pub record_id: Option<String>,
    pub data: Option<Value>,
    pub previous: Option<Value>,
    pub before: Option<Value>,
    pub scope: Option<&'static str>,
    pub actor: Actor,
    pub request_id: Option<String>,
    pub redacted: Vec<String>,
}

pub fn action_class(action: &str) -> &'static str {
    match action {
        "read" => "read",
        custom if custom.starts_with("custom:") => "custom",
        _ => "write",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key<'a> {
    name: &'a str,
    prefix: bool,
    each: bool,
}

impl<'a> Key<'a> {
    fn of(key: &'a str) -> Self {
        let (key, each) = key
            .strip_suffix("[]")
            .map_or((key, false), |key| (key, true));
        let (name, prefix) = key
            .strip_suffix('*')
            .map_or((key, false), |name| (name, true));
        Self { name, prefix, each }
    }

    fn names(&self, key: &str) -> bool {
        match self.prefix {
            true => key.starts_with(self.name),
            false => key == self.name,
        }
    }
}

fn hide(value: Value, rule: &[Key<'_>], at: &str) -> (Value, Vec<String>) {
    let Some((key, rest)) = rule.split_first() else {
        return (Value::from(REDACTED), vec![at.to_string()]);
    };
    let Value::Object(fields) = value else {
        return (value, Vec::new());
    };
    let (kept, hidden) = fields.into_iter().fold(
        (Map::new(), Vec::new()),
        |(mut kept, mut hidden), (name, value)| {
            let path = match at {
                "" => name.clone(),
                at => format!("{at}.{name}"),
            };
            let (value, found) = match (key.names(&name), key.each, value) {
                (false, _, value) => (value, Vec::new()),
                (true, false, value) => hide(value, rest, &path),
                (true, true, Value::Array(items)) => {
                    let each = format!("{path}[]");
                    let (items, found): (Vec<Value>, Vec<Vec<String>>) = items
                        .into_iter()
                        .map(|item| hide(item, rest, &each))
                        .unzip();
                    (Value::Array(items), found.concat())
                }
                (true, true, value) => (value, Vec::new()),
            };
            kept.insert(name, value);
            hidden.extend(found);
            (kept, hidden)
        },
    );
    (Value::Object(kept), hidden)
}

fn redacted(value: Option<Value>, rules: &[String]) -> (Option<Value>, BTreeSet<String>) {
    match value {
        Some(value) => {
            let (value, hidden) =
                rules
                    .iter()
                    .fold((value, BTreeSet::new()), |(value, mut hidden), rule| {
                        let keys: Vec<Key<'_>> = rule.split('.').map(Key::of).collect();
                        let (value, found) = hide(value, &keys, "");
                        hidden.extend(found);
                        (value, hidden)
                    });
            (Some(value), hidden)
        }
        None => (None, BTreeSet::new()),
    }
}

impl AuditRecord {
    pub fn redact(self, rules: &[String]) -> Self {
        if rules.is_empty() {
            return self;
        }
        let (data, in_data) = redacted(self.data, rules);
        let (previous, in_previous) = redacted(self.previous, rules);
        let (before, in_before) = redacted(self.before, rules);
        Self {
            data,
            previous,
            before,
            redacted: self
                .redacted
                .into_iter()
                .chain(in_data)
                .chain(in_previous)
                .chain(in_before)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            ..self
        }
    }

    pub fn legacy_payload(&self) -> Value {
        let mut payload = json!({
            "timestamp": self.timestamp.to_rfc3339(),
            "tenant": self.tenant,
            "service": self.service,
            "table": self.table,
            "action": self.action,
            "user_id": self.user_id,
            "user_name": self.user_name,
            "record_id": self.record_id,
            "data": self.data,
            "previous": self.previous,
        });
        if let (Some(scope), Some(fields)) = (self.scope, payload.as_object_mut()) {
            fields.insert("scope".to_string(), json!(scope));
        }
        payload
    }

    pub fn durable_payload(&self) -> Value {
        json!({
            "v": DURABLE_VERSION,
            "timestamp": self.timestamp.to_rfc3339(),
            "tenant": self.tenant,
            "service": self.service,
            "table": self.table,
            "action": self.action,
            "user_id": self.subject,
            "user_name": self.user_name,
            "record_id": self.record_id,
            "data": self.data,
            "previous": self.before.as_ref().or(self.previous.as_ref()),
            "actor": self.actor.to_json(),
            "request_id": self.request_id,
            "scope": self.scope,
            "redacted": self.redacted,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Read {
    pub record_ids: Vec<String>,
    pub returned: usize,
    pub filter_columns: Vec<String>,
    pub search: bool,
    pub sort: Option<String>,
    pub coalesced: Option<u64>,
    pub sample_percent: Option<u8>,
}

impl Read {
    pub fn of_row(record_id: &str) -> Self {
        Self {
            record_ids: vec![record_id.to_string()],
            returned: 1,
            ..Self::default()
        }
    }

    pub fn of_rows(rows: &[Value], key: &str) -> Self {
        Self {
            record_ids: rows
                .iter()
                .filter_map(|row| match row.get(key)? {
                    Value::String(id) => Some(id.clone()),
                    Value::Number(id) => Some(id.to_string()),
                    _ => None,
                })
                .collect(),
            returned: rows.len(),
            ..Self::default()
        }
    }

    pub fn data(&self) -> Value {
        let kept = self.record_ids.len().min(READ_IDS_KEPT);
        let mut data = json!({
            "record_ids": self.record_ids[..kept],
            "returned": self.returned,
            "truncated": kept < self.record_ids.len(),
            "query": {
                "filter_columns": self.filter_columns,
                "search": self.search,
                "sort": self.sort,
            },
        });
        if let Some(fields) = data.as_object_mut() {
            let counted = [
                ("coalesced", self.coalesced.map(Value::from)),
                ("sample_percent", self.sample_percent.map(Value::from)),
            ];
            fields.extend(
                counted
                    .into_iter()
                    .filter_map(|(key, value)| Some((key.to_string(), value?))),
            );
        }
        data
    }
}

#[derive(Clone)]
pub struct ReadAudit {
    sample_percent: u8,
    window: Option<Duration>,
    recent: Arc<Mutex<HashMap<u64, (Instant, u64)>>>,
}

impl ReadAudit {
    pub fn new(config: &AuditConfig) -> Self {
        Self {
            sample_percent: config.read_sample_percent,
            window: config.read_coalesce,
            recent: Arc::default(),
        }
    }

    fn sampled(&self) -> bool {
        self.sample_percent >= 100
            || Uuid::new_v4().as_u128() % 100 < u128::from(self.sample_percent)
    }

    fn coalesce(&self, kind: u64, now: Instant) -> Option<u64> {
        let Some(window) = self.window else {
            return Some(0);
        };
        let mut recent = self.recent.lock().unwrap_or_else(PoisonError::into_inner);
        match recent.get_mut(&kind) {
            Some((at, counted)) if now.duration_since(*at) < window => {
                *counted += 1;
                None
            }
            Some((at, counted)) => {
                let before = std::mem::take(counted);
                *at = now;
                Some(before)
            }
            None => {
                if recent.len() >= COALESCED_MOST {
                    recent.retain(|_, (at, _)| now.duration_since(*at) < window);
                }
                if recent.len() < COALESCED_MOST {
                    recent.insert(kind, (now, 0));
                }
                Some(0)
            }
        }
    }

    pub fn admit(&self, kind: impl Hash, read: Read) -> Result<Read, Unrecorded> {
        if !self.sampled() {
            return Err(Unrecorded::Sampled);
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        kind.hash(&mut hasher);
        let coalesced = self
            .coalesce(hasher.finish(), Instant::now())
            .ok_or(Unrecorded::Coalesced)?;
        Ok(Read {
            coalesced: self.window.map(|_| coalesced),
            sample_percent: (self.sample_percent < 100).then_some(self.sample_percent),
            ..read
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unrecorded {
    Sampled,
    Coalesced,
}

impl Unrecorded {
    pub fn as_str(self) -> &'static str {
        match self {
            Unrecorded::Sampled => "sampled",
            Unrecorded::Coalesced => "coalesced",
        }
    }
}

pub fn count(tenant_slug: &str, action: &str) {
    metrics::counter!(
        "cufflink_audit_records_total",
        "tenant" => tenant_slug.to_string(),
        "action_class" => action_class(action),
    )
    .increment(1);
}

pub fn count_failed(sink: &'static str) {
    metrics::counter!("cufflink_audit_publish_failed_total", "sink" => sink).increment(1);
}

pub fn publish_durable(state: &AppState, subject: String, payload: Vec<u8>) {
    let (jetstream, headers) = (
        state.jetstream.clone(),
        crate::telemetry::traced_nats_headers(),
    );
    state.shutdown.spawn("audit_durable", async move {
        let published =
            jetstream_publish_bounded(&jetstream, subject, Some(headers), payload.into()).await;
        if let Err(e) = published {
            count_failed("durable");
            tracing::warn!(error = %e, "Dropped durable audit record");
        }
    });
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::auth::binding::tests::jwt_user;
    use crate::tenant_catalog::tests::tenant;

    pub(crate) fn record(
        action: &str,
        data: Option<Value>,
        previous: Option<Value>,
    ) -> AuditRecord {
        AuditRecord {
            timestamp: "2026-09-28T12:00:00Z".parse().expect("a time"),
            tenant: "harbor".into(),
            service: "books".into(),
            table: Some("clients".into()),
            action: action.into(),
            user_id: "user-1".into(),
            user_name: "Clara Clerk".into(),
            subject: "user-1".into(),
            record_id: Some("7".into()),
            data,
            previous,
            before: None,
            scope: None,
            actor: Actor {
                sub: "user-1".into(),
                kind: ActorKind::User,
                key_id: None,
                azp: Some("portal".into()),
            },
            request_id: Some("req-1".into()),
            redacted: Vec::new(),
        }
    }

    fn rules(rules: &[&str]) -> Vec<String> {
        rules.iter().map(|rule| rule.to_string()).collect()
    }

    #[test]
    fn a_rule_names_a_key_or_what_keys_begin_with() {
        let client = json!({
            "id": "7", "name": "Ada", "dob": "1990-01-01", "dob_verified": true,
            "bank_iban": "GB00", "bank_bic": "X", "banker": "kept",
            "address": {"dob": "nested is not a column"},
        });
        let redacted =
            record("create", Some(client.clone()), None).redact(&rules(&["dob", "bank_*"]));
        assert_eq!(
            redacted.data,
            Some(json!({
                "id": "7", "name": "Ada", "dob": REDACTED, "dob_verified": true,
                "bank_iban": REDACTED, "bank_bic": REDACTED, "banker": "kept",
                "address": {"dob": "nested is not a column"},
            }))
        );
        assert_eq!(redacted.redacted, ["bank_bic", "bank_iban", "dob"]);
        assert_eq!(
            record("create", Some(client.clone()), None).redact(&[]),
            record("create", Some(client), None)
        );
    }

    #[test]
    fn a_rule_is_a_path_into_what_a_column_holds() {
        let client = json!({
            "id": "7",
            "profile": {"dob": "1990-01-01", "Dob": "kept", "name": "Ada", "card_no": "4242",
                        "card_cvc": "123"},
            "contacts": [{"email": "a@example.com", "kind": "work"}, {"kind": "none"}, "loose"],
            "tags": ["vip", "debtor"],
            "loans": "a text is no object",
            "lines": [[{"sku": "x"}]],
            "orders": {"sku": "not an array"},
        });
        let redacted = record("create", Some(client), None).redact(&rules(&[
            "profile.dob",
            "profile.card_*",
            "contacts[].email",
            "tags[]",
            "loans.inside",
            "lines[].sku",
            "orders[].sku",
            "absent.key",
        ]));
        assert_eq!(
            redacted.data,
            Some(json!({
                "id": "7",
                "profile": {"dob": REDACTED, "Dob": "kept", "name": "Ada", "card_no": REDACTED,
                            "card_cvc": REDACTED},
                "contacts": [{"email": REDACTED, "kind": "work"}, {"kind": "none"}, "loose"],
                "tags": [REDACTED, REDACTED],
                "loans": "a text is no object",
                "lines": [[{"sku": "x"}]],
                "orders": {"sku": "not an array"},
            }))
        );
        assert_eq!(
            redacted.redacted,
            [
                "contacts[].email",
                "profile.card_cvc",
                "profile.card_no",
                "profile.dob",
                "tags[]"
            ]
        );
    }

    #[test]
    fn what_is_redacted_is_redacted_of_every_row_a_record_holds() {
        let was = json!({"id": "7", "dob": "1990-01-01", "name": "Ada"});
        let is = json!({"id": "7", "name": "Ada L.", "bank_iban": "GB00"});
        let updated = AuditRecord {
            before: Some(was.clone()),
            ..record("update", Some(is), None)
        }
        .redact(&rules(&["dob", "bank_*", "absent"]));
        assert_eq!(updated.redacted, ["bank_iban", "dob"]);
        assert_eq!(
            updated.before,
            Some(json!({"id": "7", "dob": REDACTED, "name": "Ada"}))
        );
        let deleted = record("delete", None, Some(was)).redact(&rules(&["dob"]));
        for payload in [deleted.legacy_payload(), deleted.durable_payload()] {
            assert_eq!(payload["previous"]["dob"], REDACTED, "{payload}");
            assert_eq!(payload["previous"]["name"], "Ada");
            assert_eq!(payload["data"], Value::Null);
        }
        let of_a_read = record("read", Some(json!(["not", "a", "row"])), None);
        assert_eq!(of_a_read.clone().redact(&rules(&["dob"])), of_a_read);
    }

    #[test]
    fn the_durable_record_holds_what_the_legacy_record_does_and_who_asked() {
        let row = json!({"id": "7", "name": "Ada"});
        let updated = AuditRecord {
            before: Some(json!({"id": "7", "name": "Ad"})),
            scope: Some("own"),
            redacted: vec!["dob".into()],
            ..record("update", Some(row.clone()), None)
        };
        let legacy = updated.legacy_payload();
        assert_eq!(
            legacy.to_string(),
            "{\"action\":\"update\",\"data\":{\"id\":\"7\",\"name\":\"Ada\"},\"previous\":null,\
             \"record_id\":\"7\",\"scope\":\"own\",\"service\":\"books\",\"table\":\"clients\",\
             \"tenant\":\"harbor\",\"timestamp\":\"2026-09-28T12:00:00+00:00\",\
             \"user_id\":\"user-1\",\"user_name\":\"Clara Clerk\"}"
        );
        let durable = updated.durable_payload();
        assert_eq!(
            durable,
            json!({
                "v": 2,
                "timestamp": "2026-09-28T12:00:00+00:00",
                "tenant": "harbor", "service": "books", "table": "clients",
                "action": "update", "user_id": "user-1", "user_name": "Clara Clerk",
                "record_id": "7", "data": row, "previous": {"id": "7", "name": "Ad"},
                "actor": {"sub": "user-1", "kind": "user", "key_id": null, "azp": "portal"},
                "request_id": "req-1", "scope": "own", "redacted": ["dob"],
            })
        );
        for (key, value) in legacy.as_object().expect("an object") {
            if key != "previous" {
                assert_eq!(&durable[key], value, "{key}");
            }
        }
    }

    #[test]
    fn the_legacy_record_names_the_credential_and_the_durable_one_its_user() {
        let by_key = AuditRecord {
            user_id: "owner-1".into(),
            subject: "clara".into(),
            ..record("create", Some(json!({"id": "7"})), None)
        };
        assert_eq!(by_key.legacy_payload()["user_id"], "owner-1");
        assert_eq!(by_key.durable_payload()["user_id"], "clara");
    }

    #[test]
    fn a_caller_is_who_its_credential_says() {
        let t = tenant("default", Some("orchard"));
        let user = jwt_user(&t, "orchard", "portal", &[]);
        let service_account = AuthUser {
            is_service_account: true,
            ..user.clone()
        };
        let key = |user_sub: Option<&str>| AuthUser {
            subject: "owner-1".into(),
            name: Some("ci".into()),
            auth_method: AuthMethod::ApiKey {
                id: Uuid::from_u128(9),
                user_sub: user_sub.map(str::to_string),
            },
            token_info: None,
            ..user.clone()
        };
        let actor = |sub: &str, kind, key_id: Option<u128>, azp: Option<&str>| Actor {
            sub: sub.into(),
            kind,
            key_id: key_id.map(Uuid::from_u128),
            azp: azp.map(str::to_string),
        };
        let azp = user
            .token_info
            .as_ref()
            .and_then(|token| token.claims.get("azp")?.as_str());
        let name = user.name.as_deref().unwrap_or_default();
        for (caller, is, named_as, durably) in [
            (None, Actor::anonymous(), (ANONYMOUS, ""), ANONYMOUS),
            (
                Some(user.clone()),
                actor(&user.subject, ActorKind::User, None, azp),
                (user.subject.as_str(), name),
                user.subject.as_str(),
            ),
            (
                Some(service_account),
                actor(&user.subject, ActorKind::ServiceAccount, None, azp),
                (user.subject.as_str(), name),
                user.subject.as_str(),
            ),
            (
                Some(key(None)),
                actor("owner-1", ActorKind::ApiKey, Some(9), None),
                ("owner-1", "ci"),
                "owner-1",
            ),
            (
                Some(key(Some("clara"))),
                actor("clara", ActorKind::ApiKey, Some(9), None),
                ("owner-1", "ci"),
                "clara",
            ),
        ] {
            assert_eq!(Actor::of(&caller), is);
            assert_eq!(
                named(&caller),
                (named_as.0.to_string(), named_as.1.to_string()),
                "as v0.27.0 named it"
            );
            assert_eq!(subject_of(&caller), durably);
        }
        assert_eq!(
            Actor::service("loans").to_json(),
            json!({"sub": "loans", "kind": "service", "key_id": null, "azp": null})
        );
        assert_eq!(
            Actor::partner(Uuid::from_u128(3), "acme").to_json(),
            json!({"sub": "acme", "kind": "partner_key",
                   "key_id": "00000000-0000-0000-0000-000000000003", "azp": null})
        );
    }

    #[test]
    fn a_read_names_the_rows_it_answered_and_not_what_it_was_filtered_to() {
        let ids = |count: usize| (0..count).map(|n| n.to_string()).collect::<Vec<_>>();
        let few = Read {
            record_ids: ids(2),
            returned: 2,
            filter_columns: vec!["status".into()],
            search: true,
            sort: Some("-created_at".into()),
            ..Read::default()
        };
        assert_eq!(
            few.data(),
            json!({
                "record_ids": ["0", "1"], "returned": 2, "truncated": false,
                "query": {"filter_columns": ["status"], "search": true, "sort": "-created_at"},
            })
        );
        let many = Read {
            record_ids: ids(READ_IDS_KEPT + 1),
            returned: READ_IDS_KEPT + 1,
            ..Read::default()
        }
        .data();
        let keyless = Read::of_rows(
            &[
                json!({"id": "a"}),
                json!({"title": "no key"}),
                json!({"id": 7}),
            ],
            "id",
        );
        assert_eq!(
            (keyless.record_ids.as_slice(), keyless.returned),
            (["a".to_string(), "7".to_string()].as_slice(), 3)
        );
        assert_eq!(
            many["record_ids"].as_array().map(Vec::len),
            Some(READ_IDS_KEPT)
        );
        assert_eq!(
            (&many["returned"], &many["truncated"]),
            (&json!(101), &json!(true))
        );
        assert_eq!(
            Read::of_row("7").data(),
            json!({
                "record_ids": ["7"], "returned": 1, "truncated": false,
                "query": {"filter_columns": [], "search": false, "sort": null},
            })
        );
    }

    #[test]
    fn actions_are_counted_by_their_class() {
        for (action, class) in [
            ("create", "write"),
            ("update", "write"),
            ("delete", "write"),
            ("read", "read"),
            ("custom:export", "custom"),
        ] {
            assert_eq!(action_class(action), class);
        }
    }

    #[test]
    fn durable_records_are_kept_as_the_platform_is_told() {
        let with = |vars: &[(&str, &str)]| {
            AuditConfig::from_vars(|name| {
                vars.iter()
                    .find(|(var, _)| *var == name)
                    .map(|(_, value)| value.to_string())
            })
        };
        assert_eq!(with(&[]).expect("defaults"), AuditConfig::default());
        assert_eq!(
            AuditConfig::default(),
            AuditConfig {
                max_age: DAY * 90,
                max_bytes: Some(1024 * 1024 * 1024),
                durable_all: false,
                read_sample_percent: 100,
                read_coalesce: None,
            }
        );
        assert_eq!(
            with(&[
                ("CUFFLINK_AUDIT_MAX_AGE_DAYS", "7"),
                ("CUFFLINK_AUDIT_MAX_BYTES", "1048576"),
                ("CUFFLINK_AUDIT_DURABLE_ALL", "true"),
                ("CUFFLINK_AUDIT_READ_SAMPLE_PERCENT", "25"),
                ("CUFFLINK_AUDIT_READ_COALESCE_SECS", "60"),
            ])
            .expect("set"),
            AuditConfig {
                max_age: DAY * 7,
                max_bytes: Some(1_048_576),
                durable_all: true,
                read_sample_percent: 25,
                read_coalesce: Some(Duration::from_secs(60)),
            }
        );
        let unbounded = with(&[("CUFFLINK_AUDIT_MAX_BYTES", "0")]).expect("set");
        assert_eq!(unbounded.max_bytes, None, "0 bounds it by age alone");
        for (name, value) in [
            ("CUFFLINK_AUDIT_MAX_AGE_DAYS", "0"),
            ("CUFFLINK_AUDIT_MAX_AGE_DAYS", "ninety"),
            ("CUFFLINK_AUDIT_MAX_BYTES", "-1"),
            ("CUFFLINK_AUDIT_DURABLE_ALL", "yes"),
            ("CUFFLINK_AUDIT_READ_SAMPLE_PERCENT", "0"),
            ("CUFFLINK_AUDIT_READ_SAMPLE_PERCENT", "101"),
            ("CUFFLINK_AUDIT_READ_COALESCE_SECS", "-5"),
        ] {
            let refused = with(&[(name, value)]).expect_err("refused").to_string();
            assert!(refused.contains(name), "{refused}");
        }
    }

    fn reads(sample_percent: u8, window: Option<Duration>) -> ReadAudit {
        ReadAudit::new(&AuditConfig {
            read_sample_percent: sample_percent,
            read_coalesce: window,
            ..AuditConfig::default()
        })
    }

    #[test]
    fn by_default_every_read_is_recorded_as_it_was() {
        let all = reads(100, None);
        for _ in 0..3 {
            assert_eq!(all.admit("same", Read::of_row("7")), Ok(Read::of_row("7")));
        }
        assert_eq!(
            Read::of_row("7").data(),
            json!({
                "record_ids": ["7"], "returned": 1, "truncated": false,
                "query": {"filter_columns": [], "search": false, "sort": null},
            })
        );
    }

    #[test]
    fn a_read_like_one_recorded_a_moment_ago_is_counted_into_the_next() {
        let window = Duration::from_secs(60);
        let coalescing = reads(100, Some(window));
        let first = coalescing.admit("a", Read::of_row("7")).expect("recorded");
        assert_eq!(first.coalesced, Some(0));
        assert_eq!(first.data()["coalesced"], 0);
        for _ in 0..3 {
            assert_eq!(
                coalescing.admit("a", Read::of_row("7")),
                Err(Unrecorded::Coalesced)
            );
        }
        assert!(
            coalescing.admit("b", Read::of_row("8")).is_ok(),
            "another kind"
        );
        let now = Instant::now();
        assert_eq!(coalescing.coalesce(1, now), Some(0));
        assert_eq!(coalescing.coalesce(1, now), None);
        assert_eq!(coalescing.coalesce(1, now), None);
        assert_eq!(
            coalescing.coalesce(1, now + window),
            Some(2),
            "the next record counts the reads it stands for"
        );
        assert_eq!(coalescing.coalesce(1, now + window), None);
    }

    #[test]
    fn a_replica_coalesces_no_more_reads_than_it_can_hold() {
        let window = Duration::from_secs(60);
        let coalescing = reads(100, Some(window));
        let now = Instant::now();
        for kind in 0..u64::try_from(COALESCED_MOST).expect("fits") {
            assert_eq!(coalescing.coalesce(kind, now), Some(0));
        }
        let beyond = u64::try_from(COALESCED_MOST).expect("fits");
        assert_eq!(coalescing.coalesce(beyond, now), Some(0));
        assert_eq!(
            coalescing.coalesce(beyond, now),
            Some(0),
            "recorded each time while there is no room"
        );
        assert_eq!(coalescing.coalesce(beyond, now + window), Some(0));
        assert_eq!(
            coalescing.coalesce(beyond, now + window),
            None,
            "room made of the reads whose window passed"
        );
    }

    #[test]
    fn of_sampled_reads_some_are_recorded_and_say_so() {
        let sampled = reads(50, None);
        let admitted: Vec<Result<Read, Unrecorded>> = (0..400)
            .map(|_| sampled.admit("a", Read::of_row("7")))
            .collect();
        let recorded = admitted.iter().filter(|admitted| admitted.is_ok()).count();
        assert!((100..300).contains(&recorded), "{recorded} of 400");
        assert!(admitted.iter().all(
            |admitted| matches!(admitted, Ok(read) if read.data()["sample_percent"] == 50)
                || *admitted == Err(Unrecorded::Sampled)
        ));
    }
}
