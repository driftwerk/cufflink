//! How a Postgres value becomes JSON and a JSON parameter becomes a
//! Postgres value: one codec for the platform's host functions, its data
//! routes and the native test host, so the three cannot disagree.

pub mod bind;
pub mod error;
pub mod numeric;
pub mod pooler;
pub mod render;
pub mod roles;
pub mod row_json;
pub mod session;
pub mod statement;
pub mod tx;

pub use bind::{bind, coerce, Bound};
pub use error::{from_pg, server_message};
pub use numeric::{NumericError, PgNumeric};
pub use row_json::{base_type, check_columns_v2, column_to_json, column_to_json_v2, PgInterval};
