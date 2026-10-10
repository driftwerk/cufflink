//! The history table: its name, its DDL and its rows.

use crate::plan::PlanError;
use chrono::{DateTime, SecondsFormat, Utc};
use cufflink_types::manifest::validate::{
    folded, folded_ident, is_identifier, is_lowercase_identifier,
};

/// refinery's DDL; `%T` stands for the table.
pub const HISTORY_DDL: &str = "CREATE TABLE IF NOT EXISTS %T(
             version INT4 PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));";

/// The statement that creates `table` unless it exists.
pub fn history_ddl(table: &QualifiedName) -> String {
    HISTORY_DDL.replace("%T", &table.quoted())
}

/// A table and the schema it is in, both as Postgres stores them: what a
/// statement quotes and what a session resolves unquoted are one table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    schema: String,
    table: String,
}

impl QualifiedName {
    /// `table`, which is in `default_schema`, or `schema.table`. What is
    /// declared is lowercase; the default schema, a service's own, is
    /// folded as Postgres folded it when the schema was created.
    pub fn parse(raw: &str, default_schema: &str) -> Result<Self, String> {
        let declared = |schema: Option<&str>, table: &str| {
            schema.is_none_or(is_lowercase_identifier) && is_lowercase_identifier(table)
        };
        let (schema, table) = match raw.split_once('.') {
            Some((schema, table)) => (Some(schema), table),
            None => (None, raw),
        };
        if declared(schema, table) && is_identifier(default_schema) {
            Ok(Self {
                schema: folded(schema.unwrap_or(default_schema)),
                table: table.to_string(),
            })
        } else {
            Err(format!(
                "'{raw}' is not 'table' or 'schema.table' in lowercase"
            ))
        }
    }

    pub fn schema(&self) -> &str {
        &self.schema
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// `"schema"."table"`.
    pub fn quoted(&self) -> String {
        format!(
            "{}.{}",
            folded_ident(&self.schema),
            folded_ident(&self.table)
        )
    }

    /// `schema.table`, the name lock keys are derived from.
    pub fn dotted(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// A migration's version as the history's `INT4` holds it. A version the
/// column cannot hold is refused before a run is planned.
pub fn history_version(version: u32) -> i32 {
    i32::try_from(version).unwrap_or(i32::MAX)
}

/// A row of the history table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Applied {
    pub version: i32,
    pub name: String,
    pub checksum: u64,
    pub applied_on: String,
}

/// A history row read with every column but the version as nullable text.
/// A row refinery could not have written is refused, where refinery's own
/// driver panics.
pub fn parse_applied(
    version: i32,
    name: Option<String>,
    applied_on: Option<String>,
    checksum: Option<String>,
) -> Result<Applied, PlanError> {
    let bad = |reason: String| PlanError::BadHistoryRow { version, reason };
    let name = name.ok_or_else(|| bad("name is NULL".to_string()))?;
    let applied_on = applied_on.ok_or_else(|| bad("applied_on is NULL".to_string()))?;
    DateTime::parse_from_rfc3339(&applied_on)
        .map_err(|_| bad(format!("applied_on '{applied_on}' is not an RFC 3339 time")))?;
    let checksum = checksum
        .ok_or_else(|| bad("checksum is NULL".to_string()))
        .and_then(|text| {
            text.parse::<u64>()
                .map_err(|_| bad(format!("checksum '{text}' is not a number")))
        })?;
    Ok(Applied {
        version,
        name,
        checksum,
        applied_on,
    })
}

/// The `applied_on` of a migration applied now: RFC 3339 in UTC, as
/// refinery writes it.
pub fn applied_on_now() -> String {
    applied_on(Utc::now())
}

fn applied_on(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        name: Option<&str>,
        applied_on: Option<&str>,
        checksum: Option<&str>,
    ) -> Result<Applied, PlanError> {
        parse_applied(
            6,
            name.map(str::to_string),
            applied_on.map(str::to_string),
            checksum.map(str::to_string),
        )
    }

    #[test]
    fn a_row_refinery_wrote_is_read() {
        assert_eq!(
            row(
                Some("add_loan_kind"),
                Some("2024-05-01T09:30:00.123456789Z"),
                Some("13745322902634218637")
            ),
            Ok(Applied {
                version: 6,
                name: "add_loan_kind".into(),
                checksum: 13_745_322_902_634_218_637,
                applied_on: "2024-05-01T09:30:00.123456789Z".into(),
            })
        );
        assert!(row(Some("n"), Some("2024-05-01T09:30:00+01:00"), Some("0")).is_ok());
    }

    #[test]
    fn a_row_refinery_could_not_have_written_is_refused_with_its_reason() {
        for (result, reason) in [
            (
                row(None, Some("2024-05-01T09:30:00Z"), Some("1")),
                "name is NULL",
            ),
            (row(Some("n"), None, Some("1")), "applied_on is NULL"),
            (
                row(Some("n"), Some("yesterday"), Some("1")),
                "applied_on 'yesterday' is not an RFC 3339 time",
            ),
            (
                row(Some("n"), Some("2024-05-01T09:30:00Z"), None),
                "checksum is NULL",
            ),
            (
                row(Some("n"), Some("2024-05-01T09:30:00Z"), Some("-4")),
                "checksum '-4' is not a number",
            ),
            (
                row(Some("n"), Some("2024-05-01T09:30:00Z"), Some("abc")),
                "checksum 'abc' is not a number",
            ),
        ] {
            assert_eq!(
                result,
                Err(PlanError::BadHistoryRow {
                    version: 6,
                    reason: reason.to_string()
                })
            );
        }
    }

    #[test]
    fn applied_on_is_rfc_3339_in_utc_with_nanoseconds() {
        let at = DateTime::parse_from_rfc3339("2026-09-28T10:00:00.5Z")
            .expect("time")
            .with_timezone(&Utc);
        assert_eq!(applied_on(at), "2026-09-28T10:00:00.500000000Z");
        assert!(row(Some("n"), Some(&applied_on_now()), Some("1")).is_ok());
    }

    #[test]
    fn a_table_is_in_the_default_schema_unless_it_names_one() {
        let own = QualifiedName::parse("schema_history", "loan_service").expect("name");
        assert_eq!(
            (own.schema(), own.table()),
            ("loan_service", "schema_history")
        );
        let deferred =
            QualifiedName::parse("public.schema_history_deferred", "report_service").expect("name");
        assert_eq!(deferred.quoted(), "\"public\".\"schema_history_deferred\"");
        assert_eq!(deferred.dotted(), "public.schema_history_deferred");
        for raw in [
            "a.b.c",
            "",
            "a.",
            ".b",
            "bad-name",
            "a b",
            "x\"y",
            "Schema_History",
            "Public.schema_history",
        ] {
            assert_eq!(
                QualifiedName::parse(raw, "s"),
                Err(format!(
                    "'{raw}' is not 'table' or 'schema.table' in lowercase"
                )),
                "{raw}"
            );
        }
        assert!(QualifiedName::parse("t", "bad-schema").is_err());
    }

    #[test]
    fn a_default_schema_is_the_one_postgres_folded_it_to() {
        let legacy = QualifiedName::parse("schema_history", "Order_Desk_2").expect("name");
        assert_eq!(legacy.schema(), "order_desk_2");
        assert_eq!(legacy.quoted(), "\"order_desk_2\".\"schema_history\"");
        assert_eq!(legacy.dotted(), "order_desk_2.schema_history");
        let keyword = QualifiedName::parse("schema_history", "order").expect("name");
        assert_eq!(keyword.quoted(), "\"order\".\"schema_history\"");
    }

    #[test]
    fn a_version_is_what_the_history_column_holds() {
        assert_eq!(history_version(0), 0);
        assert_eq!(history_version(2_147_483_647), i32::MAX);
        assert_eq!(history_version(u32::MAX), i32::MAX);
    }

    #[test]
    fn the_ddl_is_refinerys_over_the_quoted_table() {
        let table = QualifiedName::parse("schema_history", "loan_service").expect("name");
        assert_eq!(
            history_ddl(&table),
            "CREATE TABLE IF NOT EXISTS \"loan_service\".\"schema_history\"(
             version INT4 PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));"
        );
    }
}
