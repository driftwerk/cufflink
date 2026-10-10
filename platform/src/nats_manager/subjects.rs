use async_nats::jetstream::stream::{self, RetentionPolicy};
use std::time::Duration;

pub const FEATURES: &[&str] = &[cufflink_types::features::NAMESPACE_CLAIMS];

pub const RESERVED_PUBLISH_PREFIXES: [&str; 5] =
    ["$", "cufflink.", "deploy.", "undeploy.", "_INBOX."];

pub const PLATFORM_INBOX_PREFIX: &str = "_INBOX.";

pub fn subject_matches(pattern: &str, subject: &str) -> bool {
    let pat: Vec<&str> = pattern.split('.').collect();
    let sub: Vec<&str> = subject.split('.').collect();
    for (i, token) in pat.iter().enumerate() {
        match *token {
            ">" => return i + 1 == pat.len() && i < sub.len(),
            "*" => {
                if i >= sub.len() {
                    return false;
                }
            }
            literal => {
                if sub.get(i) != Some(&literal) {
                    return false;
                }
            }
        }
    }
    pat.len() == sub.len()
}

pub fn subject_covers(outer: &str, inner: &str) -> bool {
    let outer: Vec<&str> = outer.split('.').collect();
    let inner: Vec<&str> = inner.split('.').collect();
    outer.iter().enumerate().all(|(i, token)| match *token {
        ">" => i + 1 == outer.len() && i < inner.len(),
        "*" => inner.get(i).is_some_and(|t| *t != ">"),
        literal => inner.get(i) == Some(&literal),
    }) && (outer.len() == inner.len() || outer.last() == Some(&">"))
}

pub use cufflink_types::nats_names::is_valid_subject as is_valid_publish_subject;

pub fn is_reserved_for_guest(subject: &str, own_tenant_slug: &str) -> bool {
    let own = format!("dw.{own_tenant_slug}");
    RESERVED_PUBLISH_PREFIXES
        .iter()
        .any(|prefix| subject.starts_with(prefix))
        || (subject.starts_with("dw.")
            && subject != own
            && !subject.starts_with(&format!("{own}.")))
}

pub fn resolve_subject(pattern: &str, tenant_slug: &str) -> String {
    pattern.replace("{tenant}", tenant_slug)
}

pub fn tenant_stream_name(tenant_slug: &str) -> String {
    format!("DW_{}", tenant_slug.to_uppercase().replace('-', "_"))
}

pub fn tenant_subjects(tenant_slug: &str) -> String {
    format!("dw.{tenant_slug}.>")
}

const TENANT_STREAM_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

pub fn tenant_stream_config(tenant_slug: &str) -> stream::Config {
    stream::Config {
        name: tenant_stream_name(tenant_slug),
        subjects: vec![tenant_subjects(tenant_slug)],
        retention: RetentionPolicy::Interest,
        max_age: TENANT_STREAM_MAX_AGE,
        ..Default::default()
    }
}

pub fn patterns_overlap(a: &str, b: &str) -> bool {
    fn overlap(a: &[&str], b: &[&str]) -> bool {
        match (a, b) {
            ([], []) => true,
            ([], _) | (_, []) => false,
            ([">", ..], _) | (_, [">", ..]) => true,
            ([x, rest_a @ ..], [y, rest_b @ ..]) => {
                (*x == "*" || *y == "*" || x == y) && overlap(rest_a, rest_b)
            }
        }
    }
    let (a, b): (Vec<&str>, Vec<&str>) = (a.split('.').collect(), b.split('.').collect());
    overlap(&a, &b)
}

pub const UNCLAIMABLE_NAMESPACES: [&str; 5] = ["dw", "cufflink", "deploy", "undeploy", "_INBOX"];

fn unclaimable_namespace(pattern: &str) -> Option<String> {
    let first = pattern.split('.').next().unwrap_or_default();
    match first {
        "*" | ">" => UNCLAIMABLE_NAMESPACES.first().map(|name| name.to_string()),
        server if server.starts_with('$') => Some(server.to_string()),
        name => UNCLAIMABLE_NAMESPACES
            .contains(&name)
            .then(|| name.to_string()),
    }
}

pub const UNCLAIMABLE_STREAM_PREFIXES: [&str; 2] = ["DW_", "CUFFLINK_"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ClaimKind {
    Subject,
    Stream,
}

impl ClaimKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ClaimKind::Subject => "subject",
            ClaimKind::Stream => "stream",
        }
    }
}

impl std::str::FromStr for ClaimKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "subject" => Ok(ClaimKind::Subject),
            "stream" => Ok(ClaimKind::Stream),
            other => Err(format!("'{other}' is not one of subject, stream")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Claim {
    pub kind: ClaimKind,
    pub pattern: String,
}

impl Claim {
    pub fn subject(pattern: impl Into<String>) -> Self {
        Self {
            kind: ClaimKind::Subject,
            pattern: pattern.into(),
        }
    }

    pub fn stream(name: impl Into<String>) -> Self {
        Self {
            kind: ClaimKind::Stream,
            pattern: name.into(),
        }
    }

    fn collides_with(&self, other: &Claim) -> bool {
        match (self.kind, other.kind) {
            (ClaimKind::Subject, ClaimKind::Subject) => {
                patterns_overlap(&self.pattern, &other.pattern)
            }
            (ClaimKind::Stream, ClaimKind::Stream) => self.pattern == other.pattern,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClaimError {
    #[error("subjects: '{0}' is not a subject pattern")]
    InvalidSubject(String),
    #[error("subjects: '{pattern}' reaches into '{namespace}.>', which no tenant claims")]
    ReservedSubject { pattern: String, namespace: String },
    #[error("streams: '{0}' is not a stream name (A-Z, 0-9 and '_', at most 64 characters)")]
    InvalidStream(String),
    #[error("streams: '{name}' is named like the streams the platform creates ({prefix}*)")]
    ReservedStream { name: String, prefix: &'static str },
    #[error("{}s: '{pattern}' is claimed twice", kind.as_str())]
    Repeated { kind: ClaimKind, pattern: String },
    #[error(
        "{}s: '{pattern}' overlaps '{theirs}', which tenant '{tenant}' claims",
        kind.as_str()
    )]
    Taken {
        kind: ClaimKind,
        pattern: String,
        tenant: String,
        theirs: String,
    },
}

impl ClaimError {
    pub fn is_conflict(&self) -> bool {
        matches!(self, ClaimError::Taken { .. })
    }
}

fn validate_claim(claim: &Claim) -> Result<(), ClaimError> {
    let pattern = claim.pattern.clone();
    match claim.kind {
        ClaimKind::Subject if !cufflink_types::nats_names::is_valid_subject_pattern(&pattern) => {
            Err(ClaimError::InvalidSubject(pattern))
        }
        ClaimKind::Subject => unclaimable_namespace(&pattern).map_or(Ok(()), |namespace| {
            Err(ClaimError::ReservedSubject { pattern, namespace })
        }),
        ClaimKind::Stream if !cufflink_types::nats_names::is_valid_stream_name(&pattern) => {
            Err(ClaimError::InvalidStream(pattern))
        }
        ClaimKind::Stream => UNCLAIMABLE_STREAM_PREFIXES
            .into_iter()
            .find(|prefix| pattern.starts_with(prefix))
            .map_or(Ok(()), |prefix| {
                Err(ClaimError::ReservedStream {
                    name: pattern,
                    prefix,
                })
            }),
    }
}

pub fn validate_claims(new: &[Claim], others: &[(String, Claim)]) -> Result<(), ClaimError> {
    new.iter().enumerate().try_for_each(|(index, claim)| {
        validate_claim(claim)?;
        if new[..index].contains(claim) {
            return Err(ClaimError::Repeated {
                kind: claim.kind,
                pattern: claim.pattern.clone(),
            });
        }
        others
            .iter()
            .find(|(_, theirs)| claim.collides_with(theirs))
            .map_or(Ok(()), |(tenant, theirs)| {
                Err(ClaimError::Taken {
                    kind: claim.kind,
                    pattern: claim.pattern.clone(),
                    tenant: tenant.clone(),
                    theirs: theirs.pattern.clone(),
                })
            })
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishScope {
    pub namespace: Namespace,
    pub foreign: Vec<String>,
}

impl PublishScope {
    pub fn own(tenant_slug: &str) -> Self {
        Self {
            namespace: Namespace::own(tenant_slug),
            foreign: Vec::new(),
        }
    }

    pub fn claimed_by_another(&self, subject: &str) -> Option<&str> {
        self.foreign
            .iter()
            .map(String::as_str)
            .find(|claim| subject_matches(claim, subject))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    tenant_slug: String,
    own_stream: String,
    subjects: Vec<String>,
    stream_claims: Vec<String>,
}

impl Namespace {
    pub fn new(tenant_slug: &str, claims: impl IntoIterator<Item = Claim>) -> Self {
        let (subjects, streams): (Vec<Claim>, Vec<Claim>) = claims
            .into_iter()
            .partition(|claim| claim.kind == ClaimKind::Subject);
        let patterns = |claims: Vec<Claim>| claims.into_iter().map(|claim| claim.pattern);
        Self {
            tenant_slug: tenant_slug.to_string(),
            own_stream: tenant_stream_name(tenant_slug),
            subjects: std::iter::once(tenant_subjects(tenant_slug))
                .chain(patterns(subjects))
                .collect(),
            stream_claims: patterns(streams).collect(),
        }
    }

    pub fn own(tenant_slug: &str) -> Self {
        Self::new(tenant_slug, [])
    }

    pub fn tenant_slug(&self) -> &str {
        &self.tenant_slug
    }

    pub fn claims(&self) -> impl Iterator<Item = Claim> + '_ {
        let subjects = self.subjects.iter().skip(1).cloned().map(Claim::subject);
        let streams = self.stream_claims.iter().cloned().map(Claim::stream);
        subjects.chain(streams)
    }

    pub fn contains(&self, subject: &str) -> bool {
        self.subjects
            .iter()
            .any(|claim| subject_matches(claim, subject))
    }

    pub fn covers(&self, pattern: &str) -> bool {
        self.subjects
            .iter()
            .any(|claim| subject_covers(claim, pattern))
    }

    pub fn has_stream(&self, stream: &str) -> bool {
        self.own_stream == stream || self.stream_claims.iter().any(|name| name == stream)
    }

    pub fn own_stream(&self) -> &str {
        &self.own_stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_tenant_claims_or_publishes_on_an_extensions_subject() {
        for pattern in [
            "cufflink.ext.>",
            "cufflink.ext.acme.>",
            "cufflink.*.acme.google.token",
        ] {
            assert_eq!(
                validate_claims(&[Claim::subject(pattern)], &[]),
                Err(ClaimError::ReservedSubject {
                    pattern: pattern.into(),
                    namespace: "cufflink".into(),
                }),
                "{pattern}"
            );
        }
        assert!(is_reserved_for_guest(
            &cufflink_types::ext::call_subject("acme", "google.token"),
            "acme"
        ));
    }

    #[test]
    fn a_tenants_stream_is_its_namespace_kept_a_week_while_it_is_wanted() {
        let config = tenant_stream_config("acme-corp");
        assert_eq!(config.name, "DW_ACME_CORP");
        assert_eq!(config.subjects, ["dw.acme-corp.>"]);
        assert_eq!(config.retention, RetentionPolicy::Interest);
        assert_eq!(config.max_age, Duration::from_secs(604_800));
        assert!(Namespace::own("acme-corp").covers(&config.subjects[0]));
    }

    #[test]
    fn overlap_and_cover_are_what_matching_makes_them() {
        fn of_length(tokens: &[&str], length: usize) -> Vec<String> {
            (0..length).fold(vec![String::new()], |prefixes, _| {
                prefixes
                    .iter()
                    .flat_map(|prefix| {
                        tokens.iter().map(move |token| match prefix.as_str() {
                            "" => token.to_string(),
                            prefix => format!("{prefix}.{token}"),
                        })
                    })
                    .collect()
            })
        }
        let up_to = |tokens: &[&str], longest: usize| -> Vec<String> {
            (1..=longest)
                .flat_map(|length| of_length(tokens, length))
                .collect()
        };
        let patterns: Vec<String> = up_to(&["a", "b", "*", ">"], 3)
            .into_iter()
            .filter(|pattern| cufflink_types::nats_names::is_valid_subject_pattern(pattern))
            .collect();
        let subjects = up_to(&["a", "b", "c"], 4);
        assert_eq!((patterns.len(), subjects.len()), (52, 120));
        for a in &patterns {
            for b in &patterns {
                let both = subjects
                    .iter()
                    .any(|subject| subject_matches(a, subject) && subject_matches(b, subject));
                assert_eq!(patterns_overlap(a, b), both, "overlap of '{a}' and '{b}'");
                let all = subjects
                    .iter()
                    .filter(|subject| subject_matches(b, subject))
                    .all(|subject| subject_matches(a, subject));
                assert_eq!(subject_covers(a, b), all, "'{a}' covering '{b}'");
            }
        }
    }

    #[test]
    fn patterns_overlap_when_a_subject_matches_both() {
        for (a, b, expected) in [
            ("a.*", "a.b", true),
            ("a.>", "a", false),
            ("a.>", "a.b.c", true),
            ("*.b", "a.*", true),
            ("a.b", "a.c", false),
            ("a.b", "a.b", true),
            ("a.b", "a.b.c", false),
            ("a.*", "a.b.c", false),
            (">", "anything.at.all", true),
            ("books.>", "books.*.loans", true),
            ("books.>", "receipts.>", false),
            ("*.created", "books.>", true),
            ("a.*.c", "a.b.>", true),
            ("a.*.c", "a.b.d", false),
        ] {
            assert_eq!(patterns_overlap(a, b), expected, "{a} / {b}");
            assert_eq!(patterns_overlap(b, a), expected, "{b} / {a}");
        }
    }

    #[test]
    fn a_namespace_is_the_tenants_own_and_what_it_claims() {
        let own = Namespace::own("acme");
        assert!(own.contains("dw.acme.loans.created"));
        assert!(own.covers("dw.acme.>"));
        assert!(own.covers("dw.acme.*.created"));
        assert!(!own.covers("dw.acme"));
        assert!(!own.covers("dw.*.loans.created"));
        assert!(!own.contains("dw.other.loans.created"));
        assert!(!own.contains("books.created"));
        assert!(own.has_stream("DW_ACME"));
        assert!(!own.has_stream("BOOKS"));
        assert_eq!(own.own_stream(), "DW_ACME");
        assert_eq!(own.claims().count(), 0);

        let claims = [
            Claim::subject("books.>"),
            Claim::subject("audit.logs"),
            Claim::stream("BOOKS"),
        ];
        let harbor = Namespace::new("harbor", claims.clone());
        assert_eq!(harbor.tenant_slug(), "harbor");
        assert_eq!(harbor.claims().collect::<Vec<_>>(), claims);
        for subject in ["books.created", "books.deleted.loans", "audit.logs"] {
            assert!(harbor.contains(subject), "{subject}");
        }
        for subject in ["books", "audit.logs.extra", "receipts.created", "dw.acme.x"] {
            assert!(!harbor.contains(subject), "{subject}");
        }
        assert!(harbor.covers("books.*.loans"));
        assert!(harbor.covers("books.>"));
        assert!(harbor.covers("dw.harbor.loans.created"));
        assert!(!harbor.covers("*.created"));
        assert!(!harbor.covers(">"));
        assert!(!harbor.covers("audit.*"));
        assert!(harbor.has_stream("BOOKS") && harbor.has_stream("DW_HARBOR"));
        assert!(!harbor.has_stream("RECEIPTS"));
    }

    #[test]
    fn claims_are_well_formed_unreserved_and_clear_of_other_tenants() {
        let others = [
            ("harbor".to_string(), Claim::subject("books.>")),
            ("harbor".to_string(), Claim::stream("BOOKS")),
        ];
        assert_eq!(
            validate_claims(
                &[
                    Claim::subject("receipts.>"),
                    Claim::subject("audit.logs"),
                    Claim::stream("RECEIPTS"),
                    Claim::stream("BOOKS_2")
                ],
                &others
            ),
            Ok(())
        );
        assert_eq!(validate_claims(&[], &others), Ok(()));
        let taken = |pattern: &str, theirs: &str, kind| ClaimError::Taken {
            kind,
            pattern: pattern.into(),
            tenant: "harbor".into(),
            theirs: theirs.into(),
        };
        let reserved = |pattern: &str, namespace: &str| ClaimError::ReservedSubject {
            pattern: pattern.into(),
            namespace: namespace.into(),
        };
        for (claim, expected) in [
            (
                Claim::subject("books.created"),
                taken("books.created", "books.>", ClaimKind::Subject),
            ),
            (
                Claim::subject("books.*"),
                taken("books.*", "books.>", ClaimKind::Subject),
            ),
            (
                Claim::stream("BOOKS"),
                taken("BOOKS", "BOOKS", ClaimKind::Stream),
            ),
            (Claim::subject("dw.acme.>"), reserved("dw.acme.>", "dw")),
            (
                Claim::subject("cufflink.tick.hourly"),
                reserved("cufflink.tick.hourly", "cufflink"),
            ),
            (Claim::subject("$JS.API.>"), reserved("$JS.API.>", "$JS")),
            (Claim::subject("$SYS.>"), reserved("$SYS.>", "$SYS")),
            (
                Claim::subject("_INBOX.abc"),
                reserved("_INBOX.abc", "_INBOX"),
            ),
            (
                Claim::subject("deploy.web.x"),
                reserved("deploy.web.x", "deploy"),
            ),
            (Claim::subject("*.created"), reserved("*.created", "dw")),
            (Claim::subject(">"), reserved(">", "dw")),
            (Claim::subject("$X.anything"), reserved("$X.anything", "$X")),
            (Claim::subject("dw"), reserved("dw", "dw")),
            (
                Claim::subject("a..b"),
                ClaimError::InvalidSubject("a..b".into()),
            ),
            (
                Claim::subject("a.>.b"),
                ClaimError::InvalidSubject("a.>.b".into()),
            ),
            (
                Claim::subject(""),
                ClaimError::InvalidSubject(String::new()),
            ),
            (
                Claim::stream("books"),
                ClaimError::InvalidStream("books".into()),
            ),
            (
                Claim::stream("DW_ACME"),
                ClaimError::ReservedStream {
                    name: "DW_ACME".into(),
                    prefix: "DW_",
                },
            ),
            (
                Claim::stream("CUFFLINK_TICKS"),
                ClaimError::ReservedStream {
                    name: "CUFFLINK_TICKS".into(),
                    prefix: "CUFFLINK_",
                },
            ),
        ] {
            assert_eq!(
                validate_claims(std::slice::from_ref(&claim), &others),
                Err(expected),
                "{claim:?}"
            );
        }
        assert_eq!(
            validate_claims(&[Claim::subject("a.b"), Claim::subject("a.b")], &[]),
            Err(ClaimError::Repeated {
                kind: ClaimKind::Subject,
                pattern: "a.b".into()
            })
        );
        assert_eq!(
            validate_claims(
                &[Claim::subject("BOOKS"), Claim::stream("BOOKS_X")],
                &others
            ),
            Ok(()),
            "a subject and a stream of one name are different things"
        );
        assert!(taken("a", "b", ClaimKind::Subject).is_conflict());
        assert!(!ClaimError::InvalidSubject("a".into()).is_conflict());
        assert_eq!(
            taken("books.*", "books.>", ClaimKind::Subject).to_string(),
            "subjects: 'books.*' overlaps 'books.>', which tenant 'harbor' claims"
        );
        assert_eq!(
            reserved("*.created", "dw").to_string(),
            "subjects: '*.created' reaches into 'dw.>', which no tenant claims"
        );
    }

    #[test]
    fn subject_matches_literal() {
        assert!(subject_matches("foo.bar", "foo.bar"));
        assert!(!subject_matches("foo.bar", "foo.baz"));
        assert!(!subject_matches("foo.bar", "foo.bar.baz"));
    }

    #[test]
    fn subject_matches_single_token_wildcard() {
        assert!(subject_matches("foo.*", "foo.bar"));
        assert!(subject_matches("foo.*.baz", "foo.x.baz"));
        assert!(!subject_matches("foo.*", "foo.bar.baz"));
        assert!(!subject_matches("foo.*", "foo"));
    }

    #[test]
    fn subject_matches_multi_token_tail() {
        assert!(subject_matches("foo.>", "foo.bar"));
        assert!(subject_matches("foo.>", "foo.bar.baz"));
        assert!(subject_matches("cufflink.tick.>", "cufflink.tick.5min"));
        assert!(!subject_matches("foo.>", "foo"));
        assert!(!subject_matches("foo.>", "bar.baz"));
        assert!(!subject_matches("foo.>.bar", "foo.x.bar"));
    }

    #[test]
    fn patterns_cover_the_patterns_whose_subjects_they_match() {
        for (outer, inner) in [
            ("dw.acme.assets.>", "dw.acme.assets.items.*"),
            ("dw.acme.assets.>", "dw.acme.assets.>"),
            ("dw.acme.*.items.created", "dw.acme.assets.items.created"),
            ("dw.acme.*.items.*", "dw.acme.assets.items.*"),
            (
                "dw.acme.assets.items.created",
                "dw.acme.assets.items.created",
            ),
        ] {
            assert!(subject_covers(outer, inner), "{outer} covers {inner}");
        }
        for (outer, inner) in [
            ("dw.acme.assets.items.created", "dw.acme.assets.items.*"),
            ("dw.acme.assets.*", "dw.acme.assets.>"),
            ("dw.acme.assets.*", "dw.acme.assets.items.created"),
            ("dw.acme.assets.>", "dw.acme.assets"),
            ("dw.acme.assets.items", "dw.acme.assets"),
            ("dw.acme.other.>", "dw.acme.assets.items.created"),
        ] {
            assert!(
                !subject_covers(outer, inner),
                "{outer} does not cover {inner}"
            );
        }
    }

    #[test]
    fn reserved_platform_prefixes() {
        for subject in [
            "cufflink.wasm.svc.h",
            "cufflink.tick.5min",
            "$JS.API.STREAM.DELETE.DW_ACME",
            "$SYS.REQ.SERVER.PING",
            "$KV.flags.x",
            "$O.bucket.x",
            "$JSC.R.abc",
            "$SRV.PING",
            "$NRG.AE.x",
            "$MQTT.sess.x",
            "deploy.web.acme.svc",
            "undeploy.web.acme.svc",
            "_INBOX.abc",
        ] {
            assert!(is_reserved_for_guest(subject, "acme"), "{subject}");
        }
    }

    #[test]
    fn reserved_other_tenant_dw() {
        assert!(is_reserved_for_guest("dw.victim.logs", "acme"));
        assert!(is_reserved_for_guest("dw.acmex.items.created", "acme"));
    }

    #[test]
    fn own_dw_not_reserved() {
        assert!(!is_reserved_for_guest("dw.acme.items.created", "acme"));
        assert!(!is_reserved_for_guest("dw.acme", "acme"));
        assert!(!is_reserved_for_guest("acme.items.updated", "acme"));
    }

    #[test]
    fn wildcards_invalid_for_publish() {
        for subject in [
            "",
            "dw.acme.*",
            "dw.acme.>",
            "dw..x",
            "dw.acme. x",
            "dw.acme.x\n",
        ] {
            assert!(!is_valid_publish_subject(subject), "{subject:?}");
        }
        assert!(is_valid_publish_subject("dw.acme.items.created"));
    }

    #[test]
    fn resolve_subject_substitutes_tenant() {
        assert_eq!(
            resolve_subject("dw.{tenant}.blog.posts.deleted", "preview-pr-7"),
            "dw.preview-pr-7.blog.posts.deleted"
        );
        assert_eq!(tenant_stream_name("preview-pr-7"), "DW_PREVIEW_PR_7");
    }
}
