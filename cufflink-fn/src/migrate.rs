//! Helpers for the optional `on_migrate` schema migration hook.
//!
//! Declared in your service:
//!
//! ```rust,ignore
//! cufflink::service! {
//!     name: "ferry-service",
//!     mode: wasm,
//!     tables: [SailingRequest],
//!     on_migrate: "handle_on_migrate",
//!     // ...
//! }
//! ```
//!
//! And implemented as a handler that delegates to [`migrate::run`]:
//!
//! ```rust,ignore
//! use cufflink_fn::prelude::*;
//!
//! handler!(handle_on_migrate, |req: Request| {
//!     migrate::run(req, |diff| {
//!         if diff.added_column("sailing_requests", "approximate_item_count_min")
//!             && diff.dropped_column("sailing_requests", "approximate_item_count")
//!         {
//!             db::execute("UPDATE sailing_requests \
//!                          SET approximate_item_count_min = approximate_item_count \
//!                          WHERE approximate_item_count_min IS NULL");
//!         }
//!         Ok(())
//!     })
//! });
//! ```
//!
//! The closure must be **idempotent** — cufflink may invoke it on retried
//! deploys, on deploys that change only the code, and on first-time
//! deploys against a fresh database (a deploy whose manifest and code are
//! both unchanged is skipped and does not invoke it). Use `WHERE … IS NULL`
//! guards on every UPDATE.

use crate::{Request, Response};
pub use cufflink_types::SchemaDiff;

/// Parse the [`SchemaDiff`] from the request body and invoke `handler`.
///
/// Returns a `200 {"ok": true}` response on success or a `400` with the
/// error message if `handler` returns `Err`. If the request body fails
/// to deserialise as a `SchemaDiff`, returns a `400` describing the
/// parse error.
pub fn run<F>(req: Request, handler: F) -> Response
where
    F: FnOnce(SchemaDiff) -> Result<(), String>,
{
    match serde_json::from_value::<SchemaDiff>(req.body().clone()) {
        Ok(diff) => match handler(diff) {
            Ok(()) => Response::json(&serde_json::json!({"ok": true})),
            Err(e) => Response::error(&e),
        },
        Err(e) => Response::error(&format!(
            "on_migrate: failed to parse SchemaDiff payload: {}",
            e
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::{migrate, Request};
    use serde_json::{json, Value};

    fn migrate_request(diff: serde_json::Value) -> Request {
        let payload = serde_json::to_string(&json!({
            "method": "POST",
            "handler": "handle_on_migrate",
            "headers": {},
            "body": diff,
            "tenant": "default",
            "service": "ferry-service",
        }))
        .unwrap();
        Request::from_json(&payload).unwrap()
    }

    #[test]
    fn test_migrate_run_success() {
        let req = migrate_request(json!({
            "added_columns": [["sailings", "min"]],
            "dropped_columns": [["sailings", "midpoint"]],
        }));
        let resp = migrate::run(req, |diff| {
            assert!(diff.added_column("sailings", "min"));
            assert!(diff.dropped_column("sailings", "midpoint"));
            Ok(())
        });
        let parsed: Value = serde_json::from_str(&resp.into_data()).unwrap();
        assert_eq!(parsed["ok"], true);
    }

    #[test]
    fn test_migrate_run_handler_error() {
        let req = migrate_request(json!({}));
        let resp = migrate::run(req, |_| Err("backfill failed".into()));
        let parsed: Value = serde_json::from_str(&resp.into_data()).unwrap();
        assert_eq!(parsed["__status"], 400);
        assert_eq!(parsed["__body"]["error"], "backfill failed");
    }

    #[test]
    fn test_migrate_run_invalid_payload() {
        let req = migrate_request(json!("not a diff"));
        let resp = migrate::run(req, |_| {
            panic!("closure should not be called for invalid payload")
        });
        let parsed: Value = serde_json::from_str(&resp.into_data()).unwrap();
        assert_eq!(parsed["__status"], 400);
        assert!(parsed["__body"]["error"]
            .as_str()
            .unwrap()
            .contains("on_migrate: failed to parse SchemaDiff payload"));
    }

    #[test]
    fn test_migrate_run_empty_diff() {
        let req = migrate_request(json!({}));
        let mut called = false;
        let resp = migrate::run(req, |diff| {
            assert!(diff.is_empty());
            called = true;
            Ok(())
        });
        assert!(called);
        let parsed: Value = serde_json::from_str(&resp.into_data()).unwrap();
        assert_eq!(parsed["ok"], true);
    }
}
