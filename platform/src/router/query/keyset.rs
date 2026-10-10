use super::SqlParams;
use crate::deployer::manifest::{ColumnDefinition, ColumnType, TableDefinition};
use crate::errors::{PlatformError, PlatformResult};
use crate::router::bind::{self, Typed};
use crate::router::row_convert::manifest_ident;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Asc,
    Desc,
}

impl Direction {
    fn sql(self) -> &'static str {
        match self {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
        }
    }

    fn after(self) -> &'static str {
        match self {
            Direction::Asc => ">",
            Direction::Desc => "<",
        }
    }

    fn short(self) -> &'static str {
        match self {
            Direction::Asc => "+",
            Direction::Desc => "-",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortKey {
    name: String,
    column_type: ColumnType,
    nullable: bool,
    direction: Direction,
}

impl SortKey {
    fn of(column: &ColumnDefinition, direction: Direction) -> Self {
        Self {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            nullable: column.nullable,
            direction,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Keys(Vec<SortKey>);

impl Keys {
    pub(crate) fn of(table: &TableDefinition, sort: Option<&str>) -> PlatformResult<Self> {
        let pk = table
            .primary_key()
            .ok_or_else(|| PlatformError::Internal("Table has no primary key".into()))?;
        let requested: Vec<SortKey> = sort
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter_map(|part| {
                let (name, direction) = match part.strip_prefix('-') {
                    Some(name) => (name, Direction::Desc),
                    None => (part, Direction::Asc),
                };
                table
                    .columns
                    .iter()
                    .find(|c| c.name == name)
                    .map(|column| SortKey::of(column, direction))
            })
            .collect();
        let keys = if requested.is_empty() {
            default_keys(table)
        } else {
            requested
        };
        let keys: Vec<SortKey> = keys.into_iter().filter(|key| key.name != pk.name).collect();
        let tie_breaker = keys.last().map_or(Direction::Asc, |key| key.direction);
        let keys: Vec<SortKey> = keys
            .into_iter()
            .chain([SortKey::of(pk, tie_breaker)])
            .collect();
        if let Some(nullable) = keys.iter().find(|key| key.nullable) {
            return Err(PlatformError::BadRequest(format!(
                "sort: '{}' may be null, and a keyset listing is ordered by columns that cannot be",
                nullable.name
            )));
        }
        Ok(Self(keys))
    }

    pub(crate) fn default_nullable(table: &TableDefinition) -> Option<&ColumnDefinition> {
        default_keys(table)
            .into_iter()
            .find(|key| key.nullable)
            .and_then(|key| table.columns.iter().find(|c| c.name == key.name))
    }

    pub(crate) fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|key| key.name.as_str())
    }

    pub(crate) fn order_by(&self, table: &str) -> String {
        let table = manifest_ident(table);
        let keys: Vec<String> = self
            .0
            .iter()
            .map(|key| {
                format!(
                    "{table}.{} {}",
                    manifest_ident(&key.name),
                    key.direction.sql()
                )
            })
            .collect();
        format!("ORDER BY {}", keys.join(", "))
    }

    fn signature(&self) -> String {
        self.0
            .iter()
            .map(|key| format!("{}{}", key.direction.short(), key.name))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub(crate) fn cursor_after(&self, last: &Value) -> Option<String> {
        let values: Option<Vec<Value>> = self
            .0
            .iter()
            .map(|key| last.get(&key.name).cloned())
            .collect();
        let cursor = Cursor {
            v: values?,
            s: self.signature(),
        };
        serde_json::to_vec(&cursor)
            .ok()
            .map(|json| URL_SAFE_NO_PAD.encode(json))
    }

    pub(crate) fn after(
        &self,
        cursor: &str,
        params: &mut SqlParams,
        bound: impl Fn(&str, String) -> String,
    ) -> PlatformResult<String> {
        let decoded = Cursor::decode(cursor)?;
        if decoded.s != self.signature() {
            return Err(PlatformError::BadRequest(
                "cursor: the cursor is of another sort; start again without one".into(),
            ));
        }
        if decoded.v.len() != self.0.len() {
            return Err(PlatformError::BadRequest("Invalid cursor value".into()));
        }
        let placeholders = self
            .0
            .iter()
            .zip(&decoded.v)
            .map(|(key, value)| {
                bind::typed(&key.column_type, value)
                    .map(Typed::into_sql)
                    .map(|value| bound(&key.name, params.push(value)))
                    .ok_or_else(|| {
                        PlatformError::BadRequest(format!(
                            "cursor: '{}' is not a value of column '{}'",
                            value, key.name
                        ))
                    })
            })
            .collect::<PlatformResult<Vec<String>>>()?;
        let columns: Vec<String> = self.0.iter().map(|key| manifest_ident(&key.name)).collect();
        let direction = self.0.first().map(|key| key.direction);
        if let Some(direction) = direction.filter(|d| self.0.iter().all(|key| key.direction == *d))
        {
            return Ok(format!(
                "({}) {} ({})",
                columns.join(", "),
                direction.after(),
                placeholders.join(", ")
            ));
        }
        let branches: Vec<String> = (0..self.0.len())
            .map(|at| {
                let equal = (0..at).map(|i| format!("{} = {}", columns[i], placeholders[i]));
                let beyond = format!(
                    "{} {} {}",
                    columns[at],
                    self.0[at].direction.after(),
                    placeholders[at]
                );
                let terms: Vec<String> = equal.chain([beyond]).collect();
                format!("({})", terms.join(" AND "))
            })
            .collect();
        Ok(format!("({})", branches.join(" OR ")))
    }
}

fn default_keys(table: &TableDefinition) -> Vec<SortKey> {
    table
        .columns
        .iter()
        .find(|c| c.name == "created_at")
        .map(|column| SortKey::of(column, Direction::Desc))
        .into_iter()
        .collect()
}

#[derive(Debug, Serialize, Deserialize)]
struct Cursor {
    v: Vec<Value>,
    s: String,
}

impl Cursor {
    fn decode(cursor: &str) -> PlatformResult<Self> {
        URL_SAFE_NO_PAD
            .decode(cursor)
            .ok()
            .and_then(|json| serde_json::from_slice(&json).ok())
            .ok_or_else(|| PlatformError::BadRequest("Invalid cursor".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployer::manifest::ColumnType;
    use serde_json::json;

    fn column(name: &str, column_type: ColumnType, nullable: bool) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            primary_key: name == "id",
            nullable,
            ..Default::default()
        }
    }

    fn books(created_at: bool) -> TableDefinition {
        TableDefinition {
            name: "book".into(),
            columns: [
                Some(column("id", ColumnType::Uuid, false)),
                Some(column("status", ColumnType::Text, false)),
                Some(column("price", ColumnType::Integer, false)),
                Some(column("notes", ColumnType::Text, true)),
                created_at.then(|| column("created_at", ColumnType::Timestamp, false)),
            ]
            .into_iter()
            .flatten()
            .collect(),
            ..Default::default()
        }
    }

    fn keys(sort: Option<&str>) -> Keys {
        Keys::of(&books(true), sort).expect("keys")
    }

    fn as_it_is(_: &str, placeholder: String) -> String {
        placeholder
    }

    #[test]
    fn the_keys_are_the_sort_with_the_primary_key_last() {
        assert_eq!(
            keys(None).order_by("book"),
            "ORDER BY \"book\".\"created_at\" DESC, \"book\".\"id\" DESC"
        );
        assert_eq!(
            keys(Some("status,-price,ghost")).order_by("book"),
            "ORDER BY \"book\".\"status\" ASC, \"book\".\"price\" DESC, \"book\".\"id\" DESC"
        );
        assert_eq!(
            keys(Some("-id")).order_by("book"),
            "ORDER BY \"book\".\"id\" ASC"
        );
        assert_eq!(
            keys(Some("status")).order_by("book"),
            "ORDER BY \"book\".\"status\" ASC, \"book\".\"id\" ASC"
        );
        assert_eq!(
            keys(Some("ghost")).order_by("book"),
            "ORDER BY \"book\".\"created_at\" DESC, \"book\".\"id\" DESC"
        );
        assert_eq!(
            Keys::of(&books(false), None)
                .expect("keys")
                .order_by("book"),
            "ORDER BY \"book\".\"id\" ASC"
        );
        assert_eq!(
            keys(Some("-created_at,status"))
                .columns()
                .collect::<Vec<_>>(),
            ["created_at", "status", "id"]
        );
    }

    #[test]
    fn a_key_that_may_be_null_is_refused() {
        let error = Keys::of(&books(true), Some("notes"))
            .expect_err("nullable")
            .to_string();
        assert!(error.contains("'notes' may be null"), "{error}");
        let nullable_default = TableDefinition {
            columns: vec![
                column("id", ColumnType::Uuid, false),
                column("created_at", ColumnType::Timestamp, true),
            ],
            ..books(false)
        };
        assert_eq!(
            Keys::default_nullable(&nullable_default).map(|c| c.name.as_str()),
            Some("created_at")
        );
        assert!(Keys::default_nullable(&books(true)).is_none());
        assert!(Keys::of(&nullable_default, None).is_err());
    }

    #[test]
    fn a_cursor_carries_the_last_rows_keys_and_the_order_they_are_in() {
        let keys = keys(Some("status,-price"));
        let last = json!({"id": "0f8fad5b-d9cb-469f-a165-70867728950e", "status": "New", "price": 7, "notes": null});
        let cursor = keys.cursor_after(&last).expect("cursor");
        let decoded = Cursor::decode(&cursor).expect("decodes");
        assert_eq!(decoded.s, "+status,-price,-id");
        assert_eq!(
            decoded.v,
            json!(["New", 7, "0f8fad5b-d9cb-469f-a165-70867728950e"])
                .as_array()
                .cloned()
                .expect("values")
        );
        assert_eq!(keys.cursor_after(&json!({"status": "New"})), None);
    }

    #[test]
    fn the_rows_after_a_cursor_are_compared_as_a_row_or_branch_by_branch() {
        let last =
            json!({"id": "0f8fad5b-d9cb-469f-a165-70867728950e", "status": "New", "price": 7});
        let mut params = SqlParams::default();
        let uniform = keys(Some("status,price"));
        let cursor = uniform.cursor_after(&last).expect("cursor");
        assert_eq!(
            uniform
                .after(&cursor, &mut params, as_it_is)
                .expect("predicate"),
            "(\"status\", \"price\", \"id\") > ($1, $2, $3)"
        );
        assert_eq!(
            format!("{params:?}"),
            "[\"New\", 7, 0f8fad5b-d9cb-469f-a165-70867728950e]"
        );

        let mut params = SqlParams::default();
        params.push(Box::new(1_i32));
        let mixed = keys(Some("status,-price"));
        let cursor = mixed.cursor_after(&last).expect("cursor");
        assert_eq!(
            mixed
                .after(&cursor, &mut params, as_it_is)
                .expect("predicate"),
            "((\"status\" > $2) OR (\"status\" = $2 AND \"price\" < $3) OR \
             (\"status\" = $2 AND \"price\" = $3 AND \"id\" < $4))"
        );
        let descending = keys(Some("-created_at"));
        let cursor = descending
            .cursor_after(&json!({"id": "0f8fad5b-d9cb-469f-a165-70867728950e", "created_at": "2026-09-28T10:30:00+00:00"}))
            .expect("cursor");
        assert_eq!(
            descending
                .after(&cursor, &mut SqlParams::default(), as_it_is)
                .expect("predicate"),
            "(\"created_at\", \"id\") < ($1, $2)"
        );
    }

    #[test]
    fn a_key_of_an_adopted_table_is_compared_as_the_type_the_database_gives_it() {
        let adopted: crate::schema_manager::adopt::AdoptedSchema =
            serde_json::from_value(json!({"book": {"status": {
                "type": "catalog_service.book_status", "typtype": "enum",
                "nullable": false, "has_default": false
            }}}))
            .expect("an adopted schema");
        let keys = keys(Some("status"));
        let cursor = keys
            .cursor_after(&json!({"id": "0f8fad5b-d9cb-469f-a165-70867728950e", "status": "New"}))
            .expect("cursor");
        let mut params = SqlParams::default();
        assert_eq!(
            keys.after(&cursor, &mut params, |column, placeholder| adopted.bound(
                "book",
                column,
                placeholder
            ))
            .expect("predicate"),
            "(\"status\", \"id\") > ($1::text::catalog_service.book_status, $2)"
        );
        assert_eq!(
            format!("{params:?}"),
            "[\"New\", 0f8fad5b-d9cb-469f-a165-70867728950e]"
        );
    }

    #[test]
    fn a_cursor_of_another_order_or_of_other_values_is_refused() {
        let last =
            json!({"id": "0f8fad5b-d9cb-469f-a165-70867728950e", "status": "New", "price": 7});
        let cursor = keys(Some("status")).cursor_after(&last).expect("cursor");
        let refused = |keys: Keys, cursor: &str| {
            keys.after(cursor, &mut SqlParams::default(), as_it_is)
                .expect_err("refused")
                .to_string()
        };
        assert!(refused(keys(Some("price")), &cursor).contains("of another sort"));
        assert!(refused(keys(None), "!!!").contains("Invalid cursor"));
        let forged = URL_SAFE_NO_PAD.encode(r#"{"v":["New","seven","0f8fad5b-d9cb-469f-a165-70867728950e"],"s":"+status,+price,+id"}"#);
        assert!(refused(keys(Some("status,price")), &forged)
            .contains("'\"seven\"' is not a value of column 'price'"));
        let short = URL_SAFE_NO_PAD.encode(r#"{"v":["New"],"s":"+status,+id"}"#);
        assert!(refused(keys(Some("status")), &short).contains("Invalid cursor value"));
    }
}
