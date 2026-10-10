//! The v2 `filter` parameter: a JSON boolean tree over typed conditions.
//! Every error is the caller's (400); column names come from the manifest,
//! never from the input.
//!
//! ```json
//! {"and": [{"f": "created_at", "op": "gte", "v": "2026-01-01T00:00:00Z"},
//!          {"or": [{"f": "status", "op": "in", "v": ["new", "open"]},
//!                  {"f": "owner_id", "op": "is_null"}]},
//!          {"not": {"f": "archived", "op": "eq", "v": true}}]}
//! ```
//!
//! A top-level array is the `and` of its elements; a v1 object (`{n, f, v,
//! c}`) inside it is accepted with its connector honoured: an element whose
//! `c` is `OR` joins the previous element's OR-group.

use super::SqlParams;
use crate::deployer::manifest::{ColumnDefinition, ColumnType, TableDefinition};
use crate::router::bind;
use crate::router::row_convert::manifest_ident;
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Cond { field: String, op: Op, value: Value },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Like,
    Ilike,
    In,
    NotIn,
    IsNull,
    IsNotNull,
    Contains,
}

impl Op {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "eq" => Op::Eq,
            "ne" => Op::Ne,
            "gt" => Op::Gt,
            "gte" => Op::Gte,
            "lt" => Op::Lt,
            "lte" => Op::Lte,
            "like" => Op::Like,
            "ilike" => Op::Ilike,
            "in" => Op::In,
            "not_in" => Op::NotIn,
            "is_null" => Op::IsNull,
            "is_not_null" => Op::IsNotNull,
            "contains" => Op::Contains,
            _ => return None,
        })
    }

    fn parse_v1(name: &str) -> Option<Self> {
        Some(match name.to_uppercase().as_str() {
            "=" | "EQ" => Op::Eq,
            "!=" | "NEQ" => Op::Ne,
            ">" | "GT" => Op::Gt,
            ">=" | "GTE" => Op::Gte,
            "<" | "LT" => Op::Lt,
            "<=" | "LTE" => Op::Lte,
            "LIKE" => Op::Like,
            "ILIKE" => Op::Ilike,
            "IN" => Op::In,
            "IS NULL" => Op::IsNull,
            "IS NOT NULL" => Op::IsNotNull,
            _ => return None,
        })
    }

    fn sql(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "<>",
            Op::Gt => ">",
            Op::Gte => ">=",
            Op::Lt => "<",
            Op::Lte => "<=",
            Op::Like => "LIKE",
            Op::Ilike => "ILIKE",
            Op::In => "= ANY",
            Op::NotIn => "<> ALL",
            Op::IsNull => "IS NULL",
            Op::IsNotNull => "IS NOT NULL",
            Op::Contains => "@>",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilterLimits {
    pub max_depth: usize,
    pub max_conditions: usize,
    pub max_in: usize,
}

impl Default for FilterLimits {
    fn default() -> Self {
        Self {
            max_depth: 8,
            max_conditions: 64,
            max_in: 100,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("filter: {0}")]
pub struct FilterError(pub String);

fn invalid(reason: impl Into<String>) -> FilterError {
    FilterError(reason.into())
}

pub fn parse(json: &str, limits: &FilterLimits) -> Result<Filter, FilterError> {
    let value: Value =
        serde_json::from_str(json).map_err(|e| invalid(format!("not valid JSON ({e})")))?;
    let filter = node(&value)?;
    let (depth, conditions) = measure(&filter);
    if depth > limits.max_depth {
        return Err(invalid(format!(
            "nested {depth} deep, more than the {} allowed",
            limits.max_depth
        )));
    }
    if conditions > limits.max_conditions {
        return Err(invalid(format!(
            "{conditions} conditions, more than the {} allowed",
            limits.max_conditions
        )));
    }
    Ok(filter)
}

fn measure(filter: &Filter) -> (usize, usize) {
    match filter {
        Filter::Cond { .. } => (1, 1),
        Filter::Not(inner) => {
            let (depth, conditions) = measure(inner);
            (depth + 1, conditions)
        }
        Filter::And(items) | Filter::Or(items) => {
            let (depth, conditions) = items
                .iter()
                .map(measure)
                .fold((0, 0), |(depth, conditions), (d, c)| {
                    (depth.max(d), conditions + c)
                });
            (depth + 1, conditions)
        }
    }
}

fn node(value: &Value) -> Result<Filter, FilterError> {
    match value {
        Value::Array(items) => grouped(items),
        Value::Object(fields) => object(fields),
        other => Err(invalid(format!("expected an object or array, got {other}"))),
    }
}

fn group(items: &Value, name: &str) -> Result<Vec<Filter>, FilterError> {
    let items = items
        .as_array()
        .ok_or_else(|| invalid(format!("'{name}' takes an array")))?;
    if items.is_empty() {
        return Err(invalid(format!("'{name}' is empty")));
    }
    items.iter().map(node).collect()
}

fn object(fields: &Map<String, Value>) -> Result<Filter, FilterError> {
    let only = |key: &str| fields.len() == 1 && fields.contains_key(key);
    if only("and") {
        return group(&fields["and"], "and").map(Filter::And);
    }
    if only("or") {
        return group(&fields["or"], "or").map(Filter::Or);
    }
    if only("not") {
        return node(&fields["not"]).map(|f| Filter::Not(Box::new(f)));
    }
    if fields.contains_key("n") {
        return v1_condition(fields);
    }
    condition(fields)
}

fn unknown_keys(fields: &Map<String, Value>, known: &[&str]) -> Result<(), FilterError> {
    fields
        .keys()
        .find(|k| !known.contains(&k.as_str()))
        .map_or(Ok(()), |k| Err(invalid(format!("unknown key '{k}'"))))
}

fn condition(fields: &Map<String, Value>) -> Result<Filter, FilterError> {
    unknown_keys(fields, &["f", "op", "v"])?;
    let field = fields
        .get("f")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("a condition needs a field name in 'f'"))?;
    let op_name = fields
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("condition on '{field}' needs an 'op'")))?;
    let op = Op::parse(op_name).ok_or_else(|| invalid(format!("unknown operator '{op_name}'")))?;
    Ok(Filter::Cond {
        field: field.to_string(),
        op,
        value: fields.get("v").cloned().unwrap_or(Value::Null),
    })
}

fn v1_condition(fields: &Map<String, Value>) -> Result<Filter, FilterError> {
    unknown_keys(fields, &["n", "f", "v", "c"])?;
    let text = |key: &str| fields.get(key).and_then(Value::as_str);
    let field = text("n").ok_or_else(|| invalid("a v1 condition needs 'n'"))?;
    let op_name = text("f").unwrap_or("=");
    let op =
        Op::parse_v1(op_name).ok_or_else(|| invalid(format!("unknown v1 operator '{op_name}'")))?;
    let raw = text("v").unwrap_or_default();
    let value = match op {
        Op::In => Value::Array(
            raw.split(',')
                .map(|s| Value::String(s.trim().to_string()))
                .collect(),
        ),
        Op::IsNull | Op::IsNotNull => Value::Null,
        _ => Value::String(raw.to_string()),
    };
    Ok(Filter::Cond {
        field: field.to_string(),
        op,
        value,
    })
}

fn is_or_connector(item: &Value) -> bool {
    item.get("n").is_some()
        && item
            .get("c")
            .and_then(Value::as_str)
            .is_some_and(|c| c.eq_ignore_ascii_case("OR"))
}

fn grouped(items: &[Value]) -> Result<Filter, FilterError> {
    if items.is_empty() {
        return Err(invalid("the filter array is empty"));
    }
    let mut groups: Vec<Vec<Filter>> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let filter = node(item)?;
        match groups.last_mut() {
            Some(group) if index > 0 && is_or_connector(item) => group.push(filter),
            _ => groups.push(vec![filter]),
        }
    }
    let mut conjuncts: Vec<Filter> = groups
        .into_iter()
        .map(|mut group| match group.len() {
            1 => group.remove(0),
            _ => Filter::Or(group),
        })
        .collect();
    Ok(match conjuncts.len() {
        1 => conjuncts.remove(0),
        _ => Filter::And(conjuncts),
    })
}

fn is_text(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Text | ColumnType::Varchar { .. })
}

fn is_array(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::TextArray
            | ColumnType::UuidArray
            | ColumnType::IntegerArray
            | ColumnType::BigIntegerArray
            | ColumnType::JsonbArray
    )
}

fn is_ordered(column_type: &ColumnType) -> bool {
    !matches!(
        column_type,
        ColumnType::Boolean | ColumnType::Jsonb | ColumnType::Bytea
    ) && !is_array(column_type)
}

fn bound(column: &ColumnDefinition, value: &Value) -> Result<bind::SqlValue, FilterError> {
    if value.is_null() {
        return Err(invalid(format!(
            "'{}' compared with null; use is_null or is_not_null",
            column.name
        )));
    }
    bind::bind_column_value(column, value).map_err(|e| invalid(e.to_string()))
}

fn list<'v>(
    column: &ColumnDefinition,
    value: &'v Value,
    limits: &FilterLimits,
) -> Result<&'v [Value], FilterError> {
    let items = value
        .as_array()
        .ok_or_else(|| invalid(format!("'{}' in/not_in takes an array", column.name)))?;
    if items.len() > limits.max_in {
        return Err(invalid(format!(
            "'{}' lists {} values, more than the {} allowed",
            column.name,
            items.len(),
            limits.max_in
        )));
    }
    items
        .iter()
        .try_for_each(|item| bound(column, item).map(drop))?;
    Ok(items)
}

pub fn to_sql(
    filter: &Filter,
    table: &TableDefinition,
    params: &mut SqlParams,
    limits: &FilterLimits,
) -> Result<String, FilterError> {
    let joined = |items: &[Filter], params: &mut SqlParams, separator: &str| {
        items
            .iter()
            .map(|f| to_sql(f, table, params, limits))
            .collect::<Result<Vec<_>, _>>()
            .map(|parts| format!("({})", parts.join(separator)))
    };
    match filter {
        Filter::And(items) => joined(items, params, " AND "),
        Filter::Or(items) => joined(items, params, " OR "),
        Filter::Not(inner) => Ok(format!("NOT ({})", to_sql(inner, table, params, limits)?)),
        Filter::Cond { field, op, value } => {
            let column = table
                .columns
                .iter()
                .find(|c| c.name == *field)
                .ok_or_else(|| invalid(format!("unknown field '{field}'")))?;
            let name = manifest_ident(&column.name);
            let refuse = |what: &str| {
                Err(invalid(format!(
                    "'{}' is not valid on {} column '{field}'",
                    what,
                    column.column_type.filter_type()
                )))
            };
            match op {
                Op::IsNull | Op::IsNotNull => Ok(format!("{name} {}", op.sql())),
                Op::Like | Op::Ilike if !is_text(&column.column_type) => {
                    refuse(if *op == Op::Like { "like" } else { "ilike" })
                }
                Op::Contains
                    if column.column_type != ColumnType::Jsonb
                        && !is_array(&column.column_type) =>
                {
                    refuse("contains")
                }
                Op::Gt | Op::Gte | Op::Lt | Op::Lte if !is_ordered(&column.column_type) => {
                    refuse("an ordering comparison")
                }
                Op::In | Op::NotIn if is_array(&column.column_type) => {
                    refuse(if *op == Op::In { "in" } else { "not_in" })
                }
                Op::In | Op::NotIn => {
                    let items = list(column, value, limits)?;
                    bind::bind_column_array(column, items)
                        .map(|list| format!("{name} {}({})", op.sql(), params.push(list)))
                        .ok_or_else(|| invalid(format!("'{field}' cannot be listed")))
                }
                _ => Ok(format!(
                    "{name} {} {}",
                    op.sql(),
                    params.push(bound(column, value)?)
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> TableDefinition {
        serde_json::from_value(json!({
            "name": "books",
            "columns": [
                {"name": "id", "column_type": "uuid", "primary_key": true},
                {"name": "status", "column_type": "text"},
                {"name": "rank", "column_type": "integer"},
                {"name": "archived", "column_type": "boolean"},
                {"name": "created_at", "column_type": "timestamp"},
                {"name": "tags", "column_type": "jsonb"},
                {"name": "watchers", "column_type": "uuid_array"},
                {"name": "Mixed", "column_type": "text"}
            ]
        }))
        .expect("table")
    }

    fn render(filter: &str) -> Result<(String, String), String> {
        let limits = FilterLimits::default();
        let parsed = parse(filter, &limits).map_err(|e| e.to_string())?;
        let mut params = SqlParams::default();
        let sql = to_sql(&parsed, &table(), &mut params, &limits).map_err(|e| e.to_string())?;
        Ok((sql, format!("{params:?}")))
    }

    #[test]
    fn trees_render_to_bound_sql() {
        let ok = |sql: &str, params: &str| Ok((sql.to_string(), params.to_string()));
        let cases = [
            (
                r#"{"and": [{"f": "created_at", "op": "gte", "v": "2026-01-01T00:00:00Z"},
                            {"f": "created_at", "op": "lt", "v": "2026-02-01T00:00:00Z"},
                            {"or": [{"f": "status", "op": "in", "v": ["new", "open"]},
                                    {"f": "rank", "op": "is_null"}]},
                            {"not": {"f": "archived", "op": "eq", "v": true}}]}"#,
                ok(
                    r#"("created_at" >= $1 AND "created_at" < $2 AND ("status" = ANY($3) OR "rank" IS NULL) AND NOT ("archived" = $4))"#,
                    r#"[2026-01-01T00:00:00Z, 2026-02-01T00:00:00Z, ["new", "open"], true]"#,
                ),
            ),
            (
                r#"[{"f": "rank", "op": "not_in", "v": [1, "2"]}, {"f": "tags", "op": "contains", "v": {"a": 1}}]"#,
                ok(
                    r#"("rank" <> ALL($1) AND "tags" @> $2)"#,
                    r#"[[1, 2], Object {"a": Number(1)}]"#,
                ),
            ),
            (
                r#"{"f": "Mixed", "op": "ilike", "v": "%x%"}"#,
                ok(r#""mixed" ILIKE $1"#, r#"["%x%"]"#),
            ),
            (
                r#"{"f": "status", "op": "ne", "v": 5}"#,
                ok(r#""status" <> $1"#, r#"["5"]"#),
            ),
            (
                r#"{"f": "watchers", "op": "contains", "v": ["0f8fad5b-d9cb-469f-a165-70867728950e"]}"#,
                ok(
                    r#""watchers" @> $1"#,
                    r#"[[Some(0f8fad5b-d9cb-469f-a165-70867728950e)]]"#,
                ),
            ),
        ];
        for (filter, expected) in cases {
            assert_eq!(render(filter), expected, "{filter}");
        }
    }

    #[test]
    fn v1_objects_honour_their_connector() {
        assert_eq!(
            render(
                r#"[{"n": "status", "f": "=", "v": "new"},
                    {"n": "status", "f": "=", "v": "open", "c": "OR"},
                    {"n": "rank", "f": ">=", "v": "3"},
                    {"n": "rank", "f": "IS NULL", "c": "or"},
                    {"n": "id", "f": "IN", "v": "0f8fad5b-d9cb-469f-a165-70867728950e, 5c3b5f1e-0000-4000-8000-000000000000", "c": "AND"}]"#
            ),
            Ok((
                r#"(("status" = $1 OR "status" = $2) AND ("rank" >= $3 OR "rank" IS NULL) AND "id" = ANY($4))"#
                    .to_string(),
                r#"["new", "open", 3, [0f8fad5b-d9cb-469f-a165-70867728950e, 5c3b5f1e-0000-4000-8000-000000000000]]"#
                    .to_string()
            ))
        );
        assert_eq!(
            render(r#"[{"n": "status", "v": "x", "c": "OR"}]"#),
            Ok((r#""status" = $1"#.to_string(), r#"["x"]"#.to_string())),
            "the first element has nothing to join"
        );
    }

    #[test]
    fn every_mistake_is_a_filter_error() {
        let cases = [
            ("{not json", "filter: not valid JSON"),
            ("7", "filter: expected an object or array"),
            ("[]", "filter: the filter array is empty"),
            (r#"{"and": []}"#, "filter: 'and' is empty"),
            (r#"{"or": {}}"#, "filter: 'or' takes an array"),
            (
                r#"{"f": "ghost", "op": "eq", "v": 1}"#,
                "filter: unknown field 'ghost'",
            ),
            (
                r#"{"f": "rank", "op": "between", "v": 1}"#,
                "filter: unknown operator 'between'",
            ),
            (
                r#"{"f": "rank", "op": "eq", "v": 1, "x": 2}"#,
                "filter: unknown key 'x'",
            ),
            (
                r#"{"f": "rank", "v": 1}"#,
                "filter: condition on 'rank' needs an 'op'",
            ),
            (
                r#"{"op": "eq"}"#,
                "filter: a condition needs a field name in 'f'",
            ),
            (
                r#"{"f": "rank", "op": "like", "v": "1%"}"#,
                "filter: 'like' is not valid on integer column 'rank'",
            ),
            (
                r#"{"f": "status", "op": "contains", "v": "x"}"#,
                "filter: 'contains' is not valid on text column 'status'",
            ),
            (
                r#"{"f": "watchers", "op": "in", "v": ["0f8fad5b-d9cb-469f-a165-70867728950e"]}"#,
                "filter: 'in' is not valid on array column 'watchers'",
            ),
            (
                r#"{"f": "archived", "op": "gt", "v": true}"#,
                "filter: 'an ordering comparison' is not valid on boolean column 'archived'",
            ),
            (
                r#"{"f": "id", "op": "eq", "v": "abc"}"#,
                r#"filter: column 'id': expected a uuid, got "abc""#,
            ),
            (
                r#"{"f": "rank", "op": "eq", "v": "x"}"#,
                r#"filter: column 'rank': expected a 32-bit integer, got "x""#,
            ),
            (
                r#"{"f": "created_at", "op": "gt", "v": "yesterday"}"#,
                "filter: column 'created_at': expected an RFC 3339 timestamp",
            ),
            (
                r#"{"f": "rank", "op": "eq"}"#,
                "filter: 'rank' compared with null; use is_null or is_not_null",
            ),
            (
                r#"{"f": "rank", "op": "in", "v": 3}"#,
                "filter: 'rank' in/not_in takes an array",
            ),
            (
                r#"{"f": "rank", "op": "in", "v": [1, "x"]}"#,
                "filter: column 'rank'",
            ),
            (
                r#"[{"n": "rank", "f": "BETWEEN", "v": "1"}]"#,
                "filter: unknown v1 operator 'BETWEEN'",
            ),
        ];
        for (filter, expected) in cases {
            let err = render(filter).expect_err(filter);
            assert!(err.starts_with(expected), "{filter}: {err}");
        }
    }

    #[test]
    fn limits_bound_depth_conditions_and_lists() {
        let limits = FilterLimits {
            max_depth: 3,
            max_conditions: 2,
            max_in: 2,
        };
        let cond = r#"{"f": "rank", "op": "eq", "v": 1}"#;
        assert!(parse(&format!(r#"{{"not": {{"not": {cond}}}}}"#), &limits).is_ok());
        assert_eq!(
            parse(
                &format!(r#"{{"not": {{"not": {{"not": {cond}}}}}}}"#),
                &limits
            ),
            Err(invalid("nested 4 deep, more than the 3 allowed"))
        );
        assert_eq!(
            parse(&format!("[{cond}, {cond}, {cond}]"), &limits),
            Err(invalid("3 conditions, more than the 2 allowed"))
        );
        let long = parse(r#"{"f": "rank", "op": "in", "v": [1, 2, 3]}"#, &limits).expect("parses");
        assert_eq!(
            to_sql(&long, &table(), &mut SqlParams::default(), &limits),
            Err(invalid("'rank' lists 3 values, more than the 2 allowed"))
        );
    }
}
