mod layer;
mod logs;
mod spans;

pub use layer::ScopeLayer;
pub use logs::ScopeLogProcessor;
pub use spans::ScopeSpanProcessor;

use opentelemetry::StringValue;
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::Span;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Registry;

pub const TENANT: &str = "cufflink.tenant";
pub const SERVICE: &str = "cufflink.service";
pub const VERSION: &str = "cufflink.version";
pub const HANDLER: &str = "cufflink.handler";
pub const KIND: &str = "cufflink.kind";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub tenant: Arc<str>,
    pub service: Option<Arc<str>>,
    pub version: Option<i32>,
    pub handler: Option<Arc<str>>,
    pub kind: Option<Kind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Route,
    Crud,
    Hook,
    Subscription,
    Schedule,
    Job,
    WorkerRoute,
    SseReducer,
    OnMigrate,
    Child,
}

impl Kind {
    pub const ALL: [Self; 10] = [
        Self::Route,
        Self::Crud,
        Self::Hook,
        Self::Subscription,
        Self::Schedule,
        Self::Job,
        Self::WorkerRoute,
        Self::SseReducer,
        Self::OnMigrate,
        Self::Child,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Route => "route",
            Self::Crud => "crud",
            Self::Hook => "hook",
            Self::Subscription => "subscription",
            Self::Schedule => "schedule",
            Self::Job => "job",
            Self::WorkerRoute => "worker_route",
            Self::SseReducer => "sse_reducer",
            Self::OnMigrate => "on_migrate",
            Self::Child => "child",
        }
    }

    fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == name)
    }
}

fn version_label(number: i32) -> String {
    format!("v{number}")
}

fn version_number(label: &str) -> Option<i32> {
    label.strip_prefix('v')?.parse().ok()
}

impl Scope {
    pub fn tenant(tenant: impl Into<Arc<str>>) -> Self {
        Self {
            tenant: tenant.into(),
            service: None,
            version: None,
            handler: None,
            kind: None,
        }
    }

    pub fn service(
        tenant: impl Into<Arc<str>>,
        service: impl Into<Arc<str>>,
        version: impl Into<Option<i32>>,
    ) -> Self {
        Self {
            service: Some(service.into()),
            version: version.into(),
            ..Self::tenant(tenant)
        }
    }

    pub fn of_kind(self, kind: Kind) -> Self {
        Self {
            kind: Some(kind),
            ..self
        }
    }

    pub fn running(self, kind: Kind, handler: impl Into<Arc<str>>) -> Self {
        Self {
            handler: Some(handler.into()),
            ..self.of_kind(kind)
        }
    }

    pub fn version_label(&self) -> Option<String> {
        self.version.map(version_label)
    }

    pub fn record_on(&self, span: &Span) {
        span.record(TENANT, &*self.tenant);
        span.record(SERVICE, self.service.as_deref());
        span.record(VERSION, self.version_label().as_deref());
        span.record(HANDLER, self.handler.as_deref());
        span.record(KIND, self.kind.map(Kind::as_str));
    }

    pub fn current() -> Option<Self> {
        Span::current()
            .with_subscriber(|(id, dispatch)| {
                dispatch
                    .downcast_ref::<Registry>()?
                    .span(id)?
                    .extensions()
                    .get::<Fields>()?
                    .scope()
            })
            .flatten()
    }
}

#[macro_export]
macro_rules! scoped_span {
    (@open [$($head:tt)*], $scope:expr, $($field:tt)*) => {{
        let scope: &$crate::Scope = &$scope;
        $crate::__tracing::info_span!(
            $($head)*,
            cufflink.tenant = &*scope.tenant,
            cufflink.service = scope.service.as_deref(),
            cufflink.version = scope.version_label().as_deref(),
            cufflink.handler = scope.handler.as_deref(),
            cufflink.kind = scope.kind.map($crate::Kind::as_str),
            $($field)*
        )
    }};
    (parent: $parent:expr, $name:expr, $scope:expr $(, $($field:tt)*)?) => {
        $crate::scoped_span!(@open [parent: $parent, $name], $scope, $($($field)*)?)
    };
    ($name:expr, $scope:expr $(, $($field:tt)*)?) => {
        $crate::scoped_span!(@open [$name], $scope, $($($field)*)?)
    };
}

#[macro_export]
macro_rules! scopable_span {
    (@open [$($head:tt)*], $($field:tt)*) => {
        $crate::__tracing::info_span!(
            $($head)*,
            cufflink.tenant = $crate::__tracing::field::Empty,
            cufflink.service = $crate::__tracing::field::Empty,
            cufflink.version = $crate::__tracing::field::Empty,
            cufflink.handler = $crate::__tracing::field::Empty,
            cufflink.kind = $crate::__tracing::field::Empty,
            $($field)*
        )
    };
    (parent: $parent:expr, $name:expr $(, $($field:tt)*)?) => {
        $crate::scopable_span!(@open [parent: $parent, $name], $($($field)*)?)
    };
    ($name:expr $(, $($field:tt)*)?) => {
        $crate::scopable_span!(@open [$name], $($($field)*)?)
    };
}

const NAMES: [&str; 5] = [TENANT, SERVICE, VERSION, HANDLER, KIND];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Fields([Option<Arc<str>>; 5]);

impl Fields {
    fn declared(record: impl FnOnce(&mut dyn Visit)) -> Self {
        let mut fields = Self::default();
        record(&mut fields);
        fields
    }

    fn is_empty(&self) -> bool {
        self.0.iter().all(Option::is_none)
    }

    fn or(&self, inherited: &Self) -> Self {
        Self(std::array::from_fn(|at| {
            self.0[at].clone().or_else(|| inherited.0[at].clone())
        }))
    }

    fn set(&mut self, name: &str, value: &str) {
        if let Some(at) = NAMES.iter().position(|field| *field == name) {
            self.0[at] = Some(value.into());
        }
    }

    fn attributes(&self) -> impl Iterator<Item = (&'static str, StringValue)> + '_ {
        NAMES
            .into_iter()
            .zip(&self.0)
            .filter_map(|(name, value)| Some((name, value.clone()?.into())))
    }

    fn scope(&self) -> Option<Scope> {
        let [tenant, service, version, handler, kind] = &self.0;
        Some(Scope {
            tenant: tenant.clone()?,
            service: service.clone(),
            version: version.as_deref().and_then(version_number),
            handler: handler.clone(),
            kind: kind.as_deref().and_then(Kind::named),
        })
    }
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.set(field.name(), value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if NAMES.contains(&field.name()) {
            self.set(field.name(), &format!("{value:?}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kind_is_named_as_the_signal_contract_names_it() {
        assert_eq!(
            Kind::ALL.map(Kind::as_str),
            [
                "route",
                "crud",
                "hook",
                "subscription",
                "schedule",
                "job",
                "worker_route",
                "sse_reducer",
                "on_migrate",
                "child",
            ]
        );
        assert!(Kind::ALL
            .into_iter()
            .all(|kind| Kind::named(kind.as_str()) == Some(kind)));
        assert_eq!(Kind::named("Route"), None);
    }

    #[test]
    fn a_version_is_rendered_and_read_back_in_one_place() {
        let scope = Scope {
            version: Some(7),
            ..Scope::tenant("acme")
        };
        assert_eq!(scope.version_label().as_deref(), Some("v7"));
        assert_eq!(version_number("v7"), Some(7));
        assert_eq!(Scope::tenant("acme").version_label(), None);
        assert!(["7", "v", "vx", "v7.1", ""]
            .into_iter()
            .all(|label| version_number(label).is_none()));
    }

    #[test]
    fn a_scope_is_narrowed_from_the_tenant_down() {
        let service = Scope::service("acme", "notes", 3);
        assert_eq!(
            service,
            Scope {
                service: Some("notes".into()),
                version: Some(3),
                ..Scope::tenant("acme")
            }
        );
        assert_eq!(Scope::service("acme", "notes", None).version, None);
        assert_eq!(
            service.clone().of_kind(Kind::Crud),
            Scope {
                kind: Some(Kind::Crud),
                ..service.clone()
            }
        );
        assert_eq!(
            service.clone().running(Kind::Hook, "before_create"),
            Scope {
                handler: Some("before_create".into()),
                kind: Some(Kind::Hook),
                ..service
            }
        );
    }

    fn fields(values: &[(&str, &str)]) -> Fields {
        let mut fields = Fields::default();
        values
            .iter()
            .for_each(|(name, value)| fields.set(name, value));
        fields
    }

    fn exported(fields: &Fields) -> Vec<(&'static str, String)> {
        fields
            .attributes()
            .map(|(name, value)| (name, value.as_str().to_owned()))
            .collect()
    }

    #[test]
    fn declared_fields_win_over_inherited_ones_field_by_field() {
        let inherited = fields(&[(TENANT, "acme"), (SERVICE, "notes"), (KIND, "route")]);
        let declared = fields(&[(HANDLER, "on_book"), (KIND, "hook"), (VERSION, "v3")]);

        let effective = declared.or(&inherited);
        assert_eq!(
            effective.scope(),
            Some(Scope {
                tenant: "acme".into(),
                service: Some("notes".into()),
                version: Some(3),
                handler: Some("on_book".into()),
                kind: Some(Kind::Hook),
            })
        );
        assert_eq!(
            exported(&effective),
            [
                (TENANT, "acme".to_owned()),
                (SERVICE, "notes".to_owned()),
                (VERSION, "v3".to_owned()),
                (HANDLER, "on_book".to_owned()),
                (KIND, "hook".to_owned()),
            ]
        );
    }

    #[test]
    fn fields_without_a_tenant_are_no_scope() {
        let handler_only = fields(&[(HANDLER, "on_book")]);
        assert!(!handler_only.is_empty());
        assert_eq!(handler_only.scope(), None);
        assert!(Fields::default().is_empty());
        assert!(fields(&[("tenant", "acme")]).is_empty());
    }
}
