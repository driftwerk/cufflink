use async_nats::jetstream::stream::{self, RetentionPolicy};
use std::collections::BTreeSet;
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
use cufflink_types::nats_names::{is_valid_stream_name, STREAM_NAME_MAX};

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

/// What a stream prefix may neither begin nor be begun by: the platform's
/// streams, and JetStream's key-value and object stores.
pub const UNCLAIMABLE_STREAM_STEMS: [&str; 4] = ["DW_", "CUFFLINK_", "KV_", "OBJ_"];

const STEM_MARK: char = '*';

fn maker_of(prefix: &str) -> &'static str {
    match prefix {
        "KV_" | "OBJ_" => "JetStream",
        _ => "the platform",
    }
}

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

    /// The stem of a stream claim made by prefix (`CATALOG_` of
    /// `CATALOG_*`).
    pub fn stem(&self) -> Option<&str> {
        match self.kind {
            ClaimKind::Stream => self.pattern.strip_suffix(STEM_MARK),
            ClaimKind::Subject => None,
        }
    }

    pub fn matches_stream(&self, name: &str) -> bool {
        self.kind == ClaimKind::Stream
            && self
                .stem()
                .map_or(self.pattern == name, |stem| name.starts_with(stem))
    }

    fn covered_by(&self, other: &Claim) -> bool {
        match (self.stem(), other.stem()) {
            (Some(inner), Some(outer)) => inner != outer && inner.starts_with(outer),
            _ => false,
        }
    }

    fn collides_with(&self, other: &Claim) -> bool {
        match (self.kind, other.kind) {
            (ClaimKind::Subject, ClaimKind::Subject) => {
                patterns_overlap(&self.pattern, &other.pattern)
            }
            (ClaimKind::Stream, ClaimKind::Stream) => match (self.stem(), other.stem()) {
                (Some(ours), Some(theirs)) => ours.starts_with(theirs) || theirs.starts_with(ours),
                (Some(_), None) => self.matches_stream(&other.pattern),
                (None, _) => other.matches_stream(&self.pattern),
            },
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
    #[error(
        "streams: '{0}' is neither a stream name (A-Z, 0-9 and '_', at most 64 characters) \
         nor a prefix (such a name of at most 63 characters, then '*')"
    )]
    InvalidStream(String),
    #[error(
        "streams: '{name}' is named like the streams {} creates ({prefix}*)",
        maker_of(prefix)
    )]
    ReservedStream { name: String, prefix: &'static str },
    #[error("{}s: '{pattern}' is claimed twice", kind.as_str())]
    Repeated { kind: ClaimKind, pattern: String },
    #[error("streams: '{pattern}' is covered by '{by}'")]
    Covered { pattern: String, by: String },
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
        ClaimKind::Stream => {
            let reserved = match claim.stem() {
                Some(stem) if stem.len() < STREAM_NAME_MAX && is_valid_stream_name(stem) => {
                    UNCLAIMABLE_STREAM_STEMS
                        .into_iter()
                        .find(|prefix| stem.starts_with(prefix) || prefix.starts_with(stem))
                }
                None if is_valid_stream_name(&pattern) => UNCLAIMABLE_STREAM_PREFIXES
                    .into_iter()
                    .find(|prefix| pattern.starts_with(prefix)),
                _ => return Err(ClaimError::InvalidStream(pattern)),
            };
            reserved.map_or(Ok(()), |prefix| {
                Err(ClaimError::ReservedStream {
                    name: pattern,
                    prefix,
                })
            })
        }
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
        if let Some(outer) = new.iter().find(|other| claim.covered_by(other)) {
            return Err(ClaimError::Covered {
                pattern: claim.pattern.clone(),
                by: outer.pattern.clone(),
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
    stream_claims: Vec<Claim>,
    declared: Declared,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Declared {
    Streams(BTreeSet<String>),
    Admitted,
}

impl Namespace {
    pub fn new(tenant_slug: &str, claims: impl IntoIterator<Item = Claim>) -> Self {
        let (subjects, streams): (Vec<Claim>, Vec<Claim>) = claims
            .into_iter()
            .partition(|claim| claim.kind == ClaimKind::Subject);
        Self {
            tenant_slug: tenant_slug.to_string(),
            own_stream: tenant_stream_name(tenant_slug),
            subjects: std::iter::once(tenant_subjects(tenant_slug))
                .chain(subjects.into_iter().map(|claim| claim.pattern))
                .collect(),
            stream_claims: streams,
            declared: Declared::Streams(BTreeSet::new()),
        }
    }

    /// The namespace with the streams the tenant's services declare, which
    /// a prefix claim admits.
    pub fn declaring(self, declared: BTreeSet<String>) -> Self {
        Self {
            declared: Declared::Streams(declared),
            ..self
        }
    }

    /// The namespace a deployed version runs in: its deploy checked the
    /// declarations, so a prefix holds every stream under it until the
    /// version is replaced.
    pub fn admitted(self) -> Self {
        Self {
            declared: Declared::Admitted,
            ..self
        }
    }

    pub fn claims_by_prefix(&self) -> bool {
        self.stream_claims
            .iter()
            .any(|claim| claim.stem().is_some())
    }

    pub fn own(tenant_slug: &str) -> Self {
        Self::new(tenant_slug, [])
    }

    pub fn tenant_slug(&self) -> &str {
        &self.tenant_slug
    }

    pub fn claims(&self) -> impl Iterator<Item = Claim> + '_ {
        let subjects = self.subjects.iter().skip(1).cloned().map(Claim::subject);
        subjects.chain(self.stream_claims.iter().cloned())
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

    /// Whether the tenant's claims admit the stream: a claim of its name, or
    /// a prefix under which one of the tenant's services declares it.
    pub fn admits_stream(&self, name: &str) -> bool {
        self.stream_claims.iter().any(|claim| {
            claim.matches_stream(name)
                && (claim.stem().is_none()
                    || match &self.declared {
                        Declared::Streams(declared) => declared.contains(name),
                        Declared::Admitted => true,
                    })
        })
    }

    /// Whether a subscription may bind the stream: the tenant's own, or one
    /// its claims admit.
    pub fn binds_stream(&self, name: &str) -> bool {
        name == self.own_stream || self.admits_stream(name)
    }

    /// The prefix that claims a stream the namespace does not admit because
    /// no service of the tenant declares it.
    pub fn undeclared_under(&self, name: &str) -> Option<&str> {
        self.stream_claims
            .iter()
            .find(|claim| claim.matches_stream(name))
            .filter(|_| !self.admits_stream(name))
            .map(|claim| claim.pattern.as_str())
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
        assert!(!own.admits_stream("BOOKS"));
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
        assert!(harbor.admits_stream("BOOKS"));
        assert!(!harbor.admits_stream("RECEIPTS") && !harbor.admits_stream("DW_HARBOR"));
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
    fn a_stream_claim_is_a_name_or_a_stem_and_a_stem_matches_what_begins_with_it() {
        let prefix = Claim::stream("CATALOG_*");
        assert_eq!(prefix.stem(), Some("CATALOG_"));
        assert_eq!(Claim::stream("CATALOG").stem(), None);
        assert_eq!(Claim::subject("catalog.*").stem(), None);
        for name in ["CATALOG_", "CATALOG_EVENTS", "CATALOG_X_Y"] {
            assert!(prefix.matches_stream(name), "{name}");
        }
        for name in ["CATALOG", "XCATALOG_EVENTS", "catalog_events"] {
            assert!(!prefix.matches_stream(name), "{name}");
        }
        assert!(Claim::stream("BOOKS").matches_stream("BOOKS"));
        assert!(!Claim::stream("BOOKS").matches_stream("BOOKS_2"));
        assert!(!Claim::subject("BOOKS").matches_stream("BOOKS"));
    }

    #[test]
    fn stream_prefixes_collide_across_tenants_and_are_never_covered_within_one() {
        let held = |claims: &[&str]| -> Vec<(String, Claim)> {
            claims
                .iter()
                .map(|claim| ("orchard".to_string(), Claim::stream(*claim)))
                .collect()
        };
        let taken = |pattern: &str, theirs: &str| {
            Err(ClaimError::Taken {
                kind: ClaimKind::Stream,
                pattern: pattern.into(),
                tenant: "orchard".into(),
                theirs: theirs.into(),
            })
        };
        for (ours, theirs) in [
            ("CATALOG_*", "CATALOG_EVENTS"),
            ("CATALOG_EVENTS", "CATALOG_*"),
            ("CATALOG_*", "CATALOG_X*"),
            ("CATALOG_X*", "CATALOG_*"),
            ("CATALOG_*", "CATALOG_*"),
        ] {
            assert_eq!(
                validate_claims(&[Claim::stream(ours)], &held(&[theirs])),
                taken(ours, theirs),
                "{ours} / {theirs}"
            );
        }
        for (ours, theirs) in [("CATALOG_*", "CATALO"), ("CATALOG_*", "LOANS_*")] {
            assert_eq!(
                validate_claims(&[Claim::stream(ours)], &held(&[theirs])),
                Ok(()),
                "{ours} / {theirs}"
            );
        }
        assert_eq!(
            validate_claims(
                &[Claim::stream("CATALOG_X*"), Claim::stream("CATALOG_*")],
                &[]
            ),
            Err(ClaimError::Covered {
                pattern: "CATALOG_X*".into(),
                by: "CATALOG_*".into()
            })
        );
        assert_eq!(
            ClaimError::Covered {
                pattern: "CATALOG_X*".into(),
                by: "CATALOG_*".into()
            }
            .to_string(),
            "streams: 'CATALOG_X*' is covered by 'CATALOG_*'"
        );
        assert_eq!(
            validate_claims(
                &[Claim::stream("CATALOG_*"), Claim::stream("CATALOG_*")],
                &[]
            ),
            Err(ClaimError::Repeated {
                kind: ClaimKind::Stream,
                pattern: "CATALOG_*".into()
            })
        );
        assert_eq!(
            validate_claims(
                &[Claim::stream("CATALOG_*"), Claim::stream("CATALOG_LEGACY")],
                &[]
            ),
            Ok(()),
            "a name beside the prefix that covers it adopts a stream no service declares"
        );
    }

    #[test]
    fn a_stem_is_well_formed_and_clear_of_the_platforms_and_jetstreams_streams() {
        let longest = format!("{}*", "A".repeat(63));
        assert_eq!(
            validate_claims(&[Claim::stream(longest.as_str())], &[]),
            Ok(())
        );
        for invalid in [
            "*".to_string(),
            "catalog_*".to_string(),
            "CATALOG**".to_string(),
            "CATALOG*_".to_string(),
            format!("{}*", "A".repeat(64)),
        ] {
            assert_eq!(
                validate_claims(&[Claim::stream(invalid.as_str())], &[]),
                Err(ClaimError::InvalidStream(invalid.clone())),
                "{invalid}"
            );
        }
        for (stem, prefix) in [
            ("DW_ACME*", "DW_"),
            ("D*", "DW_"),
            ("CUFFLINK_*", "CUFFLINK_"),
            ("CUFF*", "CUFFLINK_"),
            ("KV_*", "KV_"),
            ("K*", "KV_"),
            ("KV_FLAGS*", "KV_"),
            ("OBJ_*", "OBJ_"),
            ("O*", "OBJ_"),
        ] {
            assert_eq!(
                validate_claims(&[Claim::stream(stem)], &[]),
                Err(ClaimError::ReservedStream {
                    name: stem.into(),
                    prefix
                }),
                "{stem}"
            );
        }
        assert_eq!(
            validate_claims(&[Claim::stream("KV_FLAGS")], &[]),
            Ok(()),
            "a name keeps the rule it had"
        );
        assert_eq!(
            ClaimError::ReservedStream {
                name: "K*".into(),
                prefix: "KV_"
            }
            .to_string(),
            "streams: 'K*' is named like the streams JetStream creates (KV_*)"
        );
    }

    #[test]
    fn a_namespace_admits_a_stream_by_name_or_under_a_prefix_its_services_declare() {
        let catalog = Namespace::new(
            "harbor",
            [Claim::stream("CATALOG_*"), Claim::stream("CATALOG_LEGACY")],
        );
        assert!(catalog.claims_by_prefix());
        assert!(!Namespace::new("harbor", [Claim::stream("BOOKS")]).claims_by_prefix());
        assert!(catalog.admits_stream("CATALOG_LEGACY"));
        assert!(!catalog.admits_stream("CATALOG_EVENTS"));
        assert_eq!(
            catalog.undeclared_under("CATALOG_EVENTS"),
            Some("CATALOG_*")
        );
        assert_eq!(catalog.undeclared_under("CATALOG_LEGACY"), None);
        assert_eq!(catalog.undeclared_under("RECEIPTS"), None);
        let declaring =
            catalog.declaring(["CATALOG_EVENTS".to_string(), "RECEIPTS".to_string()].into());
        assert!(declaring.admits_stream("CATALOG_EVENTS"));
        assert_eq!(declaring.undeclared_under("CATALOG_EVENTS"), None);
        assert!(
            !declaring.admits_stream("RECEIPTS"),
            "what a service declares is admitted only under a claim"
        );
        assert_eq!(declaring.claims().count(), 2);
    }

    #[test]
    fn an_admitted_namespace_holds_every_stream_under_its_prefixes_and_binds_its_own() {
        let admitted = Namespace::new("harbor", [Claim::stream("CATALOG_*")]).admitted();
        assert!(admitted.admits_stream("CATALOG_EVENTS"));
        assert_eq!(admitted.undeclared_under("CATALOG_EVENTS"), None);
        assert!(!admitted.admits_stream("RECEIPTS"));
        assert!(admitted.binds_stream("DW_HARBOR") && admitted.binds_stream("CATALOG_EVENTS"));
        assert!(!admitted.binds_stream("DW_ACME"));
        assert!(
            !Namespace::new("harbor", [Claim::stream("CATALOG_*")]).binds_stream("CATALOG_EVENTS")
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
