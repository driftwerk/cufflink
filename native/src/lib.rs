//! Native Cufflink services: extensions written in Rust that WASM services
//! call through `ext_call`.
//!
//! A native service is a binary whose `main` names the service and the
//! extensions it provides, each with the services allowed to call it:
//!
//! ```no_run
//! use cufflink_native::prelude::*;
//!
//! struct GoogleToken;
//!
//! impl Extension for GoogleToken {
//!     fn call(&self, ctx: &CallContext, payload: Value) -> Result<Value> {
//!         let scope = payload["scope"].as_str().ok_or("scope is required")?;
//!         Ok(json!({ "token": scope, "tenant": ctx.tenant }))
//!     }
//! }
//!
//! fn main() -> std::process::ExitCode {
//!     cufflink_native::service("ext-google")
//!         .extension("google.token", GoogleToken, ["reports-service"])
//!         .run()
//! }
//! ```
//!
//! [`Service::run`] prints the service's manifest under `--emit-manifest`
//! (what `cufflink deploy` reads) and otherwise serves the Unix socket the
//! extension runner starts it with. [`Service::run_nats`] serves the same
//! extensions straight from NATS, as a provider the project runs itself.
//!
//! Each call runs on the blocking pool with the caller's [`CallContext`]
//! and is given up at its [deadline](CallDeadline::deadline). Besides the
//! codes of an extension's own [`ExtError`]s, a call fails with
//! `not_found` (no such extension), `timeout` (the deadline passed),
//! `panic` (the extension panicked), `invalid` (a malformed call, or on
//! NATS one whose body does not match its subject), `unsupported_protocol`
//! (a caller speaking another protocol), `unavailable` (the service is
//! shutting down) or `too_large` (a reply over the NATS server's maximum
//! payload). On NATS, a call whose deadline has passed gets no reply.

mod deadline;
mod dispatch;
mod error;
mod nats;
mod service;
mod socket;

pub use cufflink_nats::NatsAuth;
pub use cufflink_types::ext::CallContext;
pub use deadline::CallDeadline;
pub use error::{ExtError, Result};
pub use nats::{NatsProvider, NatsServer};
pub use service::{service, Service};
pub use socket::SocketServer;

use serde_json::Value;

/// An extension: what answers the calls made to one name.
///
/// `call` runs on the blocking pool, so it may block; closures and
/// functions of the same signature are extensions too.
pub trait Extension: Send + Sync + 'static {
    fn call(&self, ctx: &CallContext, payload: Value) -> Result<Value>;
}

impl<F> Extension for F
where
    F: Fn(&CallContext, Value) -> Result<Value> + Send + Sync + 'static,
{
    fn call(&self, ctx: &CallContext, payload: Value) -> Result<Value> {
        self(ctx, payload)
    }
}

/// What an extension needs in scope.
pub mod prelude {
    pub use crate::{CallContext, CallDeadline, ExtError, Extension, Result};
    pub use serde_json::{json, Value};
}
