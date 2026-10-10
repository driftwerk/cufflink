use crate::deployer::manifest::ServiceManifest;
use crate::errors::PlatformError;
use cufflink_types::manifest::validate::{
    validate_schedules, validate_schema_name, validate_sql_migrations, validate_streams,
};
use cufflink_types::schedule::compile;

pub(super) fn validate(manifest: &ServiceManifest) -> Result<(), PlatformError> {
    manifest
        .schema_name
        .as_deref()
        .map_or(Ok(()), |name| validate_schema_name("schema_name", name))
        .and_then(|()| validate_streams("streams", &manifest.streams))
        .and_then(|()| validate_schedules("schedules", &manifest.schedules))
        .and_then(|()| {
            manifest
                .schedules
                .iter()
                .enumerate()
                .try_for_each(|(i, schedule)| {
                    compile(&format!("schedules[{i}]"), schedule).map(drop)
                })
        })
        .and_then(|()| {
            validate_sql_migrations(
                "migrations",
                &manifest.migrations,
                manifest.migration_history.as_ref(),
            )
        })
        .map_err(PlatformError::Validation)
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::minimal_manifest;
    use super::super::validate_manifest;
    use cufflink_types::{MigrationHistory, SqlMigration};
    use serde_json::json;

    fn refused(edit: impl FnOnce(&mut cufflink_types::ServiceManifest), path: &str) {
        let mut manifest = minimal_manifest();
        edit(&mut manifest);
        let err = validate_manifest(&manifest).unwrap_err().to_string();
        assert!(err.contains(path), "'{err}' does not name {path}");
    }

    #[test]
    fn valid_declarations_pass() {
        let mut m = minimal_manifest();
        m.schema_name = Some("loan_service".into());
        m.streams = serde_json::from_value(json!([
            {"name": "NOTES", "subjects": ["notes.created"], "retention": "limits"}
        ]))
        .expect("streams");
        m.schedules = serde_json::from_value(json!([
            {"name": "order_retry", "handler": "retry_orders", "spec": {"kind": "every", "secs": 30}}
        ]))
        .expect("schedules");
        m.migrations = vec![SqlMigration {
            version: 1,
            name: "initial_tables".into(),
            sql: "CREATE TABLE notes ();".into(),
            no_transaction: false,
        }];
        m.migration_history = Some(MigrationHistory {
            table: "schema_history".into(),
            ..Default::default()
        });
        assert!(validate_manifest(&m).is_ok());
    }

    #[test]
    fn each_declaration_is_validated_by_path() {
        refused(|m| m.schema_name = Some("pg_catalog".into()), "schema_name");
        refused(
            |m| {
                m.streams = serde_json::from_value(json!([
                    {"name": "notes", "subjects": ["notes.created"], "retention": "limits"}
                ]))
                .expect("streams")
            },
            "streams[0].name",
        );
        refused(
            |m| {
                m.schedules = serde_json::from_value(json!([
                    {"name": "s", "handler": "h", "spec": {"kind": "every", "secs": 5}}
                ]))
                .expect("schedules")
            },
            "schedules[0].spec.secs",
        );
        refused(
            |m| {
                m.schedules = serde_json::from_value(json!([
                    {"name": "s", "handler": "h", "spec": {"kind": "cron", "expr": "* * * * * *"}}
                ]))
                .expect("schedules")
            },
            "schedules[0].spec.expr",
        );
        refused(
            |m| {
                m.migrations = vec![SqlMigration {
                    version: 1,
                    name: "bad-name".into(),
                    sql: "SELECT 1;".into(),
                    no_transaction: false,
                }]
            },
            "migrations[0].name",
        );
        refused(
            |m| {
                m.migration_history = Some(MigrationHistory {
                    table: "a.b.c".into(),
                    ..Default::default()
                })
            },
            "migration_history.table",
        );
    }
}
