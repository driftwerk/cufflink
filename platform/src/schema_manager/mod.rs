pub const FEATURES: &[&str] = &[
    cufflink_types::features::SCHEMA_GUARD,
    cufflink_types::features::SQL_MIGRATIONS,
    cufflink_types::features::COLUMN_TYPES_V2,
    cufflink_types::features::KEYSET_LISTS,
    cufflink_types::features::ASYNC_DEPLOY,
    cufflink_types::features::PRIVATE_TABLES,
];

pub mod adopt;
pub mod diff;
pub mod executor;
pub mod guard;
pub mod introspect;
pub mod migrate_exec;
pub mod migration;
pub mod sql_migrations;
