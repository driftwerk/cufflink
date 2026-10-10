//! # cufflink-fn
//!
//! Write custom Cufflink handlers in Rust that compile to WASM.
//!
//! This crate wraps the raw WASM host ABI so you write normal Rust code
//! instead of pointer manipulation. Use it with `cufflink` services running
//! in WASM mode.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use cufflink_fn::prelude::*;
//!
//! cufflink_fn::init!();
//!
//! handler!(hello, |req: Request| {
//!     let name = req.body()["name"].as_str().unwrap_or("world");
//!     Response::json(&json!({"message": format!("Hello, {}!", name)}))
//! });
//! ```
//!
//! ## Architecture
//!
//! Organize your code in layers:
//!
//! - **Handlers** (thin) — parse request, call operation, return response
//! - **Operations** (fat) — validation, business rules, orchestration
//! - **Repos** (data) — pure SQL via [`db::query`] / [`db::execute`]

mod auth;
#[cfg(feature = "service-call")]
mod call;
#[cfg(all(
    target_arch = "wasm32",
    any(
        feature = "http-v2",
        feature = "storage-v2",
        feature = "nats-publish-v2",
        feature = "ext-call"
    )
))]
mod envelope;
mod features;
#[cfg(target_arch = "wasm32")]
mod ffi;
#[cfg(all(not(target_arch = "wasm32"), not(feature = "test-host")))]
mod host;
#[cfg(target_arch = "wasm32")]
mod host_buffer;
mod request;
mod response;
mod runtime;
mod scope;

#[cfg(feature = "cache")]
pub mod cache;
pub mod config;
pub mod context;
pub mod db;
#[cfg(feature = "ext-call")]
pub mod ext;
#[cfg(all(not(target_arch = "wasm32"), feature = "test-host"))]
pub mod host;
pub mod http;
pub mod image;
pub mod jobs;
pub mod log;
pub mod migrate;
pub mod nats;
pub mod redis;
#[cfg(feature = "db-v2")]
pub mod sql;
pub mod sse;
pub mod storage;
#[cfg(all(not(target_arch = "wasm32"), feature = "test-host"))]
pub mod testing;
pub mod util;

pub use auth::{Auth, GrantScope};
pub use cufflink_types::delivery::DeliveryControl;
pub use cufflink_types::RetryOn;
pub use request::{
    ApiKeyPrincipal, Caller, JobContext, Part, Request, ScheduleContext, SubscriptionContext,
};
pub use response::Response;
#[cfg(feature = "response-v2")]
pub use response::{Disposition, Redirect, S3Object};
#[doc(hidden)]
pub use runtime::{__alloc, __run_handler};
pub use scope::{Predicate, Scope};

/// First-class Stripe support (webhook verification, form encoding, REST
/// client). Behind the `stripe` feature so its crypto deps don't burden
/// services that don't need it.
#[cfg(feature = "stripe")]
pub mod stripe;

/// Import everything you need to write handlers.
///
/// ```rust,ignore
/// use cufflink_fn::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "cache")]
    pub use crate::cache;
    pub use crate::config;
    pub use crate::context;
    pub use crate::db;
    #[cfg(feature = "ext-call")]
    pub use crate::ext;
    pub use crate::http;
    pub use crate::image;
    pub use crate::log;
    pub use crate::migrate;
    pub use crate::nats;
    pub use crate::redis;
    #[cfg(feature = "db-v2")]
    pub use crate::sql;
    pub use crate::sse::{self, Reduction, SseEvent, SseMessage, SsePing, SseSpec};
    pub use crate::storage;
    #[cfg(feature = "stripe")]
    pub use crate::stripe;
    pub use crate::util;
    pub use crate::Auth;
    pub use crate::JobContext;
    pub use crate::Part;
    pub use crate::Request;
    pub use crate::Response;
    pub use crate::ScheduleContext;
    pub use crate::Scope;
    pub use crate::SubscriptionContext;
    #[cfg(feature = "response-v2")]
    pub use crate::{Disposition, Redirect, S3Object};
    pub use serde_json::{json, Value};
}
