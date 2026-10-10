//! The native test host: a handler run in `cargo test` against a real
//! Postgres and fakes of everything else the platform gives it.
//!
//! ```rust,ignore
//! use cufflink_fn::testing::{AuthBuilder, TestDb, TestHost};
//!
//! let db = TestDb::start()?;
//! db.migrate("loan_service", Path::new("migrations"), &MigrationHistory::default())?;
//! let host = TestHost::builder().service("loan-service").db(&db).build()?;
//! let request = host
//!     .request()
//!     .auth(AuthBuilder::user("kc-1").permissions(&["loans:create"]))
//!     .json(&json!({"book_id": book, "memo": "Two weeks"}))
//!     .build()?;
//! let response = host.call(create_loan, request);
//! assert_eq!(response.status(), 200);
//! ```
//!
//! None of the fakes panics: what a test did not script is an error the
//! handler under test receives, and `verify` reports.

pub mod cache;
pub mod clock;
pub mod config;
pub mod db;
pub mod error;
#[cfg(feature = "ext-call")]
pub mod ext;
pub mod host;
pub mod http;
pub mod image;
pub mod jobs;
pub mod nats;
mod posture;
pub mod request;
#[cfg(feature = "service-call")]
pub mod service_call;
pub mod storage;

pub use cache::InMemoryCache;
pub use clock::FakeClock;
pub use config::ConfigMap;
pub use cufflink_migrate::Plan;
pub use cufflink_pgcodec::pooler::Pooler;
pub use cufflink_types::{HistoryMode, MigrationHistory, MigrationLock};
pub use db::{PgHost, PgOptions, PgRole, ServiceRole, TestDb};
pub use error::TestHostError;
#[cfg(feature = "ext-call")]
pub use ext::{ExpectingExt, MockExtensions};
pub use host::{TestHost, TestHostBuilder};
pub use http::{MockHttp, UrlMatch};
pub use image::{ImageRecorder, Transformed};
pub use jobs::{EnqueuedJob, JobRecorder};
pub use nats::NatsRecorder;
pub use request::{request, AuthBuilder, RequestBuilder, SubscriptionBuilder};
#[cfg(feature = "service-call")]
pub use service_call::{MockServiceCalls, ServiceCall};
pub use storage::{InMemoryStorage, StoredObject};
