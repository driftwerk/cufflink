use crate::deployer::manifest::{ColumnType, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

pub fn is_key_type(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Uuid
            | ColumnType::Text
            | ColumnType::Varchar { .. }
            | ColumnType::Integer
            | ColumnType::BigInteger
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PkValue {
    Uuid(Uuid),
    Text(String),
    Integer(i32),
    BigInteger(i64),
}

impl PkValue {
    pub fn as_param(&self) -> &(dyn ToSql + Sync) {
        match self {
            PkValue::Uuid(value) => value,
            PkValue::Text(value) => value,
            PkValue::Integer(value) => value,
            PkValue::BigInteger(value) => value,
        }
    }

    pub fn into_param(self) -> Box<dyn ToSql + Sync + Send> {
        match self {
            PkValue::Uuid(value) => Box::new(value),
            PkValue::Text(value) => Box::new(value),
            PkValue::Integer(value) => Box::new(value),
            PkValue::BigInteger(value) => Box::new(value),
        }
    }
}

impl std::fmt::Display for PkValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PkValue::Uuid(value) => value.fmt(f),
            PkValue::Text(value) => value.fmt(f),
            PkValue::Integer(value) => value.fmt(f),
            PkValue::BigInteger(value) => value.fmt(f),
        }
    }
}

fn invalid(what: &str) -> PlatformError {
    PlatformError::BadRequest(format!("Invalid {what}"))
}

pub fn parse_pk(table: &TableDefinition, raw: &str) -> PlatformResult<PkValue> {
    let key = table.primary_key().ok_or_else(|| {
        PlatformError::Internal(format!("Table '{}' has no primary key", table.name))
    })?;
    match &key.column_type {
        ColumnType::Uuid => raw.parse().map(PkValue::Uuid).map_err(|_| invalid("UUID")),
        ColumnType::Integer => raw
            .parse()
            .map(PkValue::Integer)
            .map_err(|_| invalid("integer key")),
        ColumnType::BigInteger => raw
            .parse()
            .map(PkValue::BigInteger)
            .map_err(|_| invalid("integer key")),
        ColumnType::Text if !raw.is_empty() => Ok(PkValue::Text(raw.to_string())),
        ColumnType::Varchar { length }
            if !raw.is_empty() && raw.chars().count() <= *length as usize =>
        {
            Ok(PkValue::Text(raw.to_string()))
        }
        ColumnType::Text | ColumnType::Varchar { .. } => Err(invalid("key")),
        other => Err(PlatformError::Internal(format!(
            "Table '{}' has a primary key of type {}, which no route can address",
            table.name,
            other.to_sql()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployer::manifest::ColumnDefinition;

    fn keyed(column_type: ColumnType) -> TableDefinition {
        TableDefinition {
            name: "shelf_section".into(),
            columns: vec![ColumnDefinition {
                name: "key".into(),
                column_type,
                primary_key: true,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn refused(column_type: ColumnType, raw: &str) -> String {
        match parse_pk(&keyed(column_type), raw) {
            Err(PlatformError::BadRequest(message)) => message,
            other => panic!("expected a bad request, got {other:?}"),
        }
    }

    #[test]
    fn a_key_is_read_as_the_type_of_the_tables_key() {
        let id = "0f8fad5b-d9cb-469f-a165-70867728950e";
        let parsed = |column_type, raw| parse_pk(&keyed(column_type), raw).expect("key");
        assert_eq!(
            parsed(ColumnType::Uuid, id),
            PkValue::Uuid(id.parse().expect("uuid"))
        );
        assert_eq!(
            parsed(ColumnType::Text, "purchase"),
            PkValue::Text("purchase".into())
        );
        assert_eq!(
            parsed(ColumnType::Varchar { length: 8 }, "purchase"),
            PkValue::Text("purchase".into())
        );
        assert_eq!(parsed(ColumnType::Integer, "1"), PkValue::Integer(1));
        assert_eq!(
            parsed(ColumnType::BigInteger, "9007199254740993"),
            PkValue::BigInteger(9_007_199_254_740_993)
        );
        assert_eq!(parsed(ColumnType::Integer, "-7").to_string(), "-7");
        assert_eq!(parsed(ColumnType::Uuid, id).to_string(), id);
    }

    #[test]
    fn a_key_that_is_not_one_is_a_bad_request() {
        assert_eq!(refused(ColumnType::Uuid, "purchase"), "Invalid UUID");
        assert_eq!(refused(ColumnType::Uuid, ""), "Invalid UUID");
        assert_eq!(refused(ColumnType::Integer, "one"), "Invalid integer key");
        assert_eq!(
            refused(ColumnType::Integer, "2147483648"),
            "Invalid integer key"
        );
        assert_eq!(
            refused(ColumnType::BigInteger, "1.5"),
            "Invalid integer key"
        );
        assert_eq!(refused(ColumnType::Text, ""), "Invalid key");
        assert_eq!(
            refused(ColumnType::Varchar { length: 7 }, "purchase"),
            "Invalid key"
        );
    }

    #[test]
    fn a_table_no_route_can_address_is_the_platforms_fault() {
        assert!(matches!(
            parse_pk(&keyed(ColumnType::Jsonb), "{}"),
            Err(PlatformError::Internal(_))
        ));
        let unkeyed = TableDefinition {
            columns: vec![],
            ..keyed(ColumnType::Uuid)
        };
        assert!(matches!(
            parse_pk(&unkeyed, "1"),
            Err(PlatformError::Internal(_))
        ));
        assert!(is_key_type(&ColumnType::Varchar { length: 50 }));
        assert!(!is_key_type(&ColumnType::Boolean));
    }
}
