//! Cufflink SDK — Build CRUD microservices in a few lines of code.
//!
//! # Example
//! ```rust,ignore
//! use cufflink::prelude::*;
//!
//! #[derive(Table, Serialize, Deserialize, Clone)]
//! #[table(name = "todos", auth_required, permission_area = "todos")]
//! pub struct Todo {
//!     #[key] pub id: Uuid,
//!     pub title: String,
//!     pub completed: bool,
//!     #[timestamp] pub created_at: DateTime<Utc>,
//!     #[timestamp] pub updated_at: DateTime<Utc>,
//! }
//!
//! cufflink::service! {
//!     name: "todo-service",
//!     tables: [Todo],
//!     authorization: [
//!         areas: [("todos", ["create", "view", "edit", "delete"])],
//!         default_roles: [
//!             ("admin", "Full access", ["todos:*"]),
//!             ("viewer", "Read-only", ["todos:view"]),
//!         ],
//!     ],
//! }
//! ```

/// Re-export manifest types so proc macro generated code can reference them
pub mod manifest {
    pub use cufflink_types::*;
}

/// Re-export the service! macro at crate root
pub use cufflink_sdk_macros::service;

/// JSON Schemas for typed routes (`request = T`, `responses = [...]`,
/// `query = T`, `path = T`), behind the `typed-routes` feature.
#[cfg(feature = "typed-routes")]
pub mod openapi;

/// Prelude — import this in your service for all common types and derive macros
pub mod prelude {
    pub use chrono::{DateTime, Utc};
    pub use serde::{Deserialize, Serialize};
    pub use uuid::Uuid;

    pub use cufflink_sdk_macros::Cell;
    pub use cufflink_sdk_macros::Table;
}
