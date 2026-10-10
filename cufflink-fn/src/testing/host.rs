//! Every capability of the platform, faked or real, for one service under
//! test.

use super::cache::InMemoryCache;
use super::clock::FakeClock;
use super::config::ConfigMap;
use super::db::{PgHost, PgOptions, PgRole, TestDb};
use super::error::TestHostError;
#[cfg(feature = "ext-call")]
use super::ext::MockExtensions;
use super::http::MockHttp;
use super::image::ImageRecorder;
use super::jobs::JobRecorder;
use super::nats::NatsRecorder;
use super::posture;
use super::request::{request, RequestBuilder};
#[cfg(feature = "service-call")]
use super::service_call::MockServiceCalls;
use super::storage::InMemoryStorage;
use crate::host::{self, HostSet};
use crate::{Request, Response};
use cufflink_pgcodec::pooler::Pooler;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

/// What a handler of one service reaches while a test runs it. Every field
/// is the fake a test scripts and reads back.
pub struct TestHost {
    /// `None` without a database: `db::*` and `sql::*` are then what they
    /// are outside the platform.
    pub db: Option<PgHost>,
    pub http: MockHttp,
    pub nats: NatsRecorder,
    pub storage: InMemoryStorage,
    pub cache: InMemoryCache,
    pub config: ConfigMap,
    pub clock: FakeClock,
    pub jobs: JobRecorder,
    pub images: ImageRecorder,
    #[cfg(feature = "service-call")]
    pub services: MockServiceCalls,
    #[cfg(feature = "ext-call")]
    pub ext: MockExtensions,
    tenant: String,
    service: String,
    error_key: String,
    running: Rc<Cell<usize>>,
}

/// A host of the service `service` of the tenant `default`, without a
/// database, until told otherwise. With one, its statements run as the
/// platform runs them at its strictest: logged in as the service's own
/// role, and behind a transaction pooler.
pub struct TestHostBuilder<'a> {
    tenant: String,
    service: String,
    schema: Option<String>,
    db: Option<&'a TestDb>,
    role: Option<PgRole>,
    options: PgOptions,
    clock: Option<FakeClock>,
    error_key: String,
    #[cfg(feature = "ext-call")]
    extensions: Option<Vec<String>>,
}

impl<'a> TestHostBuilder<'a> {
    pub fn tenant(self, tenant: &str) -> Self {
        Self {
            tenant: tenant.to_string(),
            ..self
        }
    }

    pub fn service(self, service: &str) -> Self {
        Self {
            service: service.to_string(),
            ..self
        }
    }

    /// The schema the service's unqualified names resolve in; the one its
    /// name gives (`loan-service` has `loan_service`) unless set.
    pub fn schema(self, schema: &str) -> Self {
        Self {
            schema: Some(schema.to_string()),
            ..self
        }
    }

    pub fn db(self, db: &'a TestDb) -> Self {
        Self {
            db: Some(db),
            ..self
        }
    }

    /// Who the service's statements run as; unless set, a login as the
    /// role [`TestDb::service_role`] makes for the service, as under
    /// `db_roles: enforce`. [`PgRole::Connection`] runs them as the test
    /// server's user, as under `db_roles: off`.
    pub fn role(self, role: PgRole) -> Self {
        Self {
            role: Some(role),
            ..self
        }
    }

    /// What sits between the platform and the database; a transaction
    /// pooler unless set, which refuses what a direct connection runs too.
    pub fn pooler(self, pooler: Pooler) -> Self {
        Self {
            options: PgOptions {
                pooler,
                ..self.options
            },
            ..self
        }
    }

    /// How long one statement may run; 30 seconds unless set.
    pub fn statement_timeout(self, timeout: Duration) -> Self {
        Self {
            options: PgOptions {
                statement_timeout: timeout,
                ..self.options
            },
            ..self
        }
    }

    /// The role `db::query_read_only` runs as; it has to exist. A service
    /// that logs in as its role is granted it, as the platform grants the
    /// read-only role its binding names.
    pub fn read_only_role(self, role: &str) -> Self {
        Self {
            options: PgOptions {
                read_only_role: Some(role.to_string()),
                ..self.options
            },
            ..self
        }
    }

    /// The rows `db::query_read_only` answers at most.
    pub fn read_only_max_rows(self, rows: i64) -> Self {
        Self {
            options: PgOptions {
                read_only_max_rows: rows,
                ..self.options
            },
            ..self
        }
    }

    /// The key the platform's own error responses carry their message
    /// under, as the service's error profile (`errors: { key: … }`) names
    /// it; `error` unless set.
    pub fn error_key(self, key: &str) -> Self {
        Self {
            error_key: key.to_string(),
            ..self
        }
    }

    /// The clock handlers read and cache entries expire by; one at the
    /// system's time unless set.
    pub fn clock(self, clock: FakeClock) -> Self {
        Self {
            clock: Some(clock),
            ..self
        }
    }

    /// The extensions the service declares, as its manifest's `extensions`
    /// list them: a call to any other is refused `not_declared`. Unless
    /// set, every extension counts as declared.
    #[cfg(feature = "ext-call")]
    pub fn extensions<I, S>(self, extensions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            extensions: Some(extensions.into_iter().map(Into::into).collect()),
            ..self
        }
    }

    pub fn build(self) -> Result<TestHost, TestHostError> {
        let schema = self
            .schema
            .unwrap_or_else(|| self.service.replace('-', "_"));
        let clock = self
            .clock
            .unwrap_or_else(|| FakeClock::at(crate::util::system_millis()));
        let storage = InMemoryStorage::new();
        Ok(TestHost {
            db: self
                .db
                .map(|db| database(db, &schema, self.role, self.options))
                .transpose()?,
            http: MockHttp::new(),
            nats: NatsRecorder::new(),
            cache: InMemoryCache::with_clock(&clock),
            config: ConfigMap::new(),
            jobs: JobRecorder::new(),
            images: ImageRecorder::over(storage.clone()),
            #[cfg(feature = "service-call")]
            services: MockServiceCalls::new(),
            #[cfg(feature = "ext-call")]
            ext: self
                .extensions
                .map_or_else(MockExtensions::new, MockExtensions::declaring),
            storage,
            clock,
            tenant: self.tenant,
            service: self.service,
            error_key: self.error_key,
            running: Rc::default(),
        })
    }
}

/// The host of the database `db`, whose statements run as `role`.
fn database(
    db: &TestDb,
    schema: &str,
    role: Option<PgRole>,
    options: PgOptions,
) -> Result<PgHost, TestHostError> {
    let role = match role {
        Some(role) => role,
        None => PgRole::Login(db.service_role(schema)?),
    };
    if let (PgRole::Login(role), Some(read_only)) = (&role, &options.read_only_role) {
        db.grant_read_only(role, read_only)?;
    }
    PgHost::connect(db, schema, PgOptions { role, ..options })
}

/// Ends one [`TestHost::run`]: when it is the outermost, what the handler
/// left open is discarded, as when an invocation ends.
struct Running<'a>(&'a TestHost);

impl Drop for Running<'_> {
    fn drop(&mut self) {
        let running = self.0.running.get().saturating_sub(1);
        self.0.running.set(running);
        if let (0, Some(db)) = (running, &self.0.db) {
            db.abandon_open_transactions();
        }
    }
}

impl TestHost {
    pub fn builder<'a>() -> TestHostBuilder<'a> {
        TestHostBuilder {
            tenant: "default".to_string(),
            service: "service".to_string(),
            schema: None,
            db: None,
            role: None,
            options: PgOptions {
                pooler: Pooler::PgBouncerTransaction,
                ..PgOptions::default()
            },
            clock: None,
            error_key: "error".to_string(),
            #[cfg(feature = "ext-call")]
            extensions: None,
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    /// A request of the host's tenant and service.
    pub fn request(&self) -> RequestBuilder {
        request().tenant(&self.tenant).service(&self.service)
    }

    fn hosts(&self) -> HostSet {
        HostSet {
            db: self.db.clone().map(|db| Rc::new(db) as Rc<_>),
            http: Some(Rc::new(self.http.clone())),
            nats: Some(Rc::new(self.nats.clone())),
            storage: Some(Rc::new(self.storage.clone())),
            cache: Some(Rc::new(self.cache.clone())),
            config: Some(Rc::new(self.config.clone())),
            clock: Some(Rc::new(self.clock.clone())),
            jobs: Some(Rc::new(self.jobs.clone())),
            image: Some(Rc::new(self.images.clone())),
            #[cfg(feature = "service-call")]
            service_call: Some(Rc::new(self.services.clone())),
            #[cfg(not(feature = "service-call"))]
            service_call: None,
            #[cfg(feature = "ext-call")]
            ext: Some(Rc::new(self.ext.clone())),
            #[cfg(not(feature = "ext-call"))]
            ext: None,
            tenant: self.tenant.clone(),
        }
    }

    /// Run `body` with the host installed on this thread; what was
    /// installed before is back when it returns or unwinds. A transaction
    /// left open is discarded when the outermost run ends.
    pub fn run<T>(&self, body: impl FnOnce() -> T) -> T {
        self.running.set(self.running.get() + 1);
        let _running = Running(self);
        host::with(self.hosts(), body)
    }

    /// Run a handler on `request`, as one invocation, once the posture
    /// the request's route declares admits its principal; otherwise the
    /// platform's refusal, as it answers one without running the handler.
    pub fn call(&self, handler: impl FnOnce(Request) -> Response, request: Request) -> Response {
        posture::refused(&request, &self.error_key).unwrap_or_else(|| self.run(|| handler(request)))
    }

    /// What was scripted and never asked for, and what was asked for and
    /// never scripted, of every fake that scripts.
    pub fn verify(&self) -> Result<(), Vec<String>> {
        let unmet = self.http.verify().err().unwrap_or_default();
        #[cfg(feature = "service-call")]
        let unmet: Vec<String> = unmet
            .into_iter()
            .chain(self.services.verify().err().unwrap_or_default())
            .collect();
        #[cfg(feature = "ext-call")]
        let unmet: Vec<String> = unmet
            .into_iter()
            .chain(self.ext.verify().err().unwrap_or_default())
            .collect();
        unmet.is_empty().then_some(()).ok_or(unmet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::UrlMatch;
    use crate::{config, context, http, redis, storage, util};
    use serde_json::json;

    fn host() -> TestHost {
        TestHost::builder()
            .tenant("harbor")
            .service("loan-service")
            .clock(FakeClock::at(1_790_589_600_000))
            .build()
            .expect("host")
    }

    fn describe(request: Request) -> Response {
        let book =
            http::get("https://catalog.example.com/books/7", &[]).and_then(|found| found.json());
        redis::set("seen:7", "1", 60);
        Response::json(&json!({
            "tenant": context::tenant(),
            "service": request.service(),
            "at": util::current_time(),
            "id": util::generate_uuid(),
            "book": book,
            "renderer": config::get("PDF_RENDERER_URL"),
            "template": storage::download("templates", "receipt.html"),
        }))
    }

    #[test]
    fn a_handler_reaches_every_capability_of_its_host() {
        let host = host();
        host.http
            .expect(
                "GET",
                UrlMatch::exact("https://catalog.example.com/books/7"),
            )
            .respond(200, &[("content-type", "application/json")], r#"{"id":7}"#);
        host.config
            .set("PDF_RENDERER_URL", "https://pdf.example.com");
        host.storage
            .put("templates", "receipt.html", "text/html", b"<p>");
        let request = host.request().handler("describe").build().expect("request");
        let response = host.call(describe, request);
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json_as::<serde_json::Value>().expect("json"),
            json!({
                "tenant": "harbor",
                "service": "loan-service",
                "at": "2026-09-28T10:00:00.000+00:00",
                "id": "00000000-0000-4000-8000-000000000001",
                "book": {"id": 7},
                "renderer": "https://pdf.example.com",
                "template": "PHA+",
            })
        );
        assert_eq!(host.cache.redis_value("seen:7").as_deref(), Some("1"));
        host.clock.advance(60_000);
        assert_eq!(host.cache.redis_value("seen:7"), None);
        assert_eq!(host.verify(), Ok(()));
        assert_eq!(context::tenant(), "", "nothing stays installed");
    }

    #[test]
    fn runs_nest_and_a_panic_leaves_nothing_installed() {
        let (outer, inner) = (
            host(),
            TestHost::builder().tenant("acme").build().expect("host"),
        );
        outer.run(|| {
            assert_eq!(context::tenant(), "harbor");
            inner.run(|| assert_eq!(context::tenant(), "acme"));
            outer.run(|| assert_eq!(context::tenant(), "harbor"));
            assert_eq!(context::tenant(), "harbor");
        });
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            outer.run(|| panic!("handler"))
        }));
        assert!(unwound.is_err());
        assert_eq!(context::tenant(), "");
        assert_eq!(outer.running.get(), 0);
    }

    #[test]
    fn what_was_not_scripted_is_reported() {
        let host = host();
        host.http
            .expect("GET", UrlMatch::exact("https://a.example/never"));
        host.run(|| assert!(http::get("https://a.example/other", &[]).is_none()));
        assert_eq!(
            host.verify(),
            Err(vec![
                "GET https://a.example/never was requested 0 time(s), expected at least 1".into(),
                "no mock matched GET https://a.example/other".into(),
            ])
        );
    }

    #[test]
    fn a_host_without_a_database_leaves_sql_inert() {
        let host = host();
        assert!(host.db.is_none());
        host.run(|| {
            assert!(crate::db::query("SELECT 1").is_empty());
            assert_eq!(crate::db::execute("SELECT 1"), 0);
        });
    }

    #[test]
    fn a_service_has_the_schema_its_name_gives() {
        assert_eq!(host().service(), "loan-service");
        let named = TestHost::builder().build().expect("host");
        assert_eq!((named.tenant(), named.service()), ("default", "service"));
    }

    #[cfg(feature = "ext-call")]
    mod extensions {
        use super::*;
        use crate::ext::{self, CallErrorKind, ExtError};

        fn traffic(_: Request) -> Response {
            match ext::call("google.token", &json!({"scope": "analytics.readonly"})) {
                Ok(token) => Response::json(&json!({"token": token})),
                Err(error) => Response::error(&error.to_string()),
            }
        }

        #[test]
        fn a_handler_calls_the_extensions_its_host_scripts() {
            let host = host();
            host.ext
                .expect("google.token")
                .respond(json!("ya29"))
                .times(1);
            let request = host.request().handler("traffic").build().expect("request");
            let response = host.call(traffic, request);
            assert_eq!(
                response.json_as::<serde_json::Value>().expect("json"),
                json!({"token": "ya29"})
            );
            assert_eq!(host.ext.calls().len(), 1);
            assert_eq!(host.verify(), Ok(()));
        }

        #[test]
        fn a_host_built_with_extensions_refuses_every_other() {
            let host = TestHost::builder()
                .extensions(["google.token"])
                .build()
                .expect("host");
            host.ext.expect("google.token").respond(json!("ya29"));
            host.ext.expect("pdf.render").refuse(CallErrorKind::Timeout);
            host.run(|| {
                assert_eq!(ext::call("google.token", &json!({})), Ok(json!("ya29")));
                assert_eq!(
                    ext::call("pdf.render", &json!({})),
                    Err(ExtError::NotDeclared)
                );
            });
            assert_eq!(
                host.verify(),
                Err(vec![
                    "extension 'pdf.render' was called 0 time(s), expected at least 1".into()
                ])
            );
            let open = TestHost::builder().build().expect("host");
            open.run(|| {
                assert_eq!(ext::call("pdf.render", &json!({})), Err(ExtError::NotFound));
            });
            assert_eq!(
                open.verify(),
                Err(vec!["no mock matched extension 'pdf.render'".into()])
            );
        }
    }
}
