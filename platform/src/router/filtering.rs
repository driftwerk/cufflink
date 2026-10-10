use crate::deployer::manifest::{ColumnDefinition, ColumnType};
use crate::errors::PlatformResult;
use crate::router::row_convert;
use serde::Deserialize;
use std::collections::HashMap;

pub type Conditions = (
    Vec<String>,
    Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
);

#[derive(Debug, Deserialize, Default)]
pub struct ListParams {
    pub page: Option<i64>,
    pub per_page: Option<i64>,
    pub sort: Option<String>,
    pub filter: Option<String>,
    pub search: Option<String>,
    pub select: Option<String>,
    pub include_deleted: Option<bool>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub fts: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Deserialize, serde::Serialize)]
pub struct JsonFilter {
    pub n: String,
    pub f: String,
    #[serde(default = "default_connector")]
    #[allow(dead_code)]
    pub c: String,
    #[serde(default)]
    pub v: String,
}

fn default_connector() -> String {
    "AND".into()
}

pub fn merge_extra_params(
    filter_json: Option<&str>,
    extra: &HashMap<String, String>,
    columns: &[ColumnDefinition],
) -> Option<String> {
    let mut filters: Vec<JsonFilter> = match filter_json {
        Some(json) => serde_json::from_str(json).unwrap_or_default(),
        None => vec![],
    };

    for (key, value) in extra {
        if columns.iter().any(|c| c.name == *key) {
            filters.push(JsonFilter {
                n: key.clone(),
                f: "=".into(),
                c: "AND".into(),
                v: value.clone(),
            });
        }
    }

    if filters.is_empty() {
        None
    } else {
        serde_json::to_string(&filters).ok()
    }
}

pub fn filter_conditions(
    filter_json: Option<&str>,
    columns: &[ColumnDefinition],
) -> PlatformResult<Conditions> {
    filter_json
        .and_then(|json| serde_json::from_str::<Vec<JsonFilter>>(json).ok())
        .map_or_else(
            || Ok((vec![], vec![])),
            |filters| build_filters_sql(&filters, columns),
        )
}

pub fn longest_in_list(filter_json: Option<&str>) -> usize {
    filter_json
        .and_then(|json| serde_json::from_str::<Vec<JsonFilter>>(json).ok())
        .into_iter()
        .flatten()
        .filter(|f| f.f.eq_ignore_ascii_case("IN"))
        .map(|f| f.v.split(',').count())
        .max()
        .unwrap_or(0)
}

fn build_filters_sql(
    filters: &[JsonFilter],
    columns: &[ColumnDefinition],
) -> PlatformResult<Conditions> {
    let mut field_order: Vec<String> = Vec::new();
    let mut field_groups: HashMap<String, Vec<&JsonFilter>> = HashMap::new();

    for filter in filters {
        if !field_groups.contains_key(&filter.n) {
            field_order.push(filter.n.clone());
        }
        field_groups
            .entry(filter.n.clone())
            .or_default()
            .push(filter);
    }

    let mut conditions = Vec::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

    for field_name in &field_order {
        let group = &field_groups[field_name];

        let col = match columns.iter().find(|c| c.name == *field_name) {
            Some(c) => c,
            None => continue,
        };

        let mut group_conditions = Vec::new();

        for filter in group {
            let condition = match filter.f.to_uppercase().as_str() {
                "IS NULL" => format!("{} IS NULL", field_name),
                "IS NOT NULL" => format!("{} IS NOT NULL", field_name),
                "IN" => {
                    let values: Vec<&str> = filter.v.split(',').map(|s| s.trim()).collect();
                    let placeholders = values
                        .iter()
                        .map(|v| {
                            params.push(row_convert::string_param(col, v)?);
                            Ok(format!("${}", params.len()))
                        })
                        .collect::<PlatformResult<Vec<String>>>()?;
                    format!("{} IN ({})", field_name, placeholders.join(", "))
                }
                op => {
                    let sql_op = match op {
                        "=" | "EQ" => "=",
                        "!=" | "NEQ" => "!=",
                        ">" | "GT" => ">",
                        "<" | "LT" => "<",
                        ">=" | "GTE" => ">=",
                        "<=" | "LTE" => "<=",
                        "LIKE" => "LIKE",
                        "ILIKE" => "ILIKE",
                        _ => "=",
                    };
                    params.push(row_convert::string_param(col, &filter.v)?);
                    format!("{} {} ${}", field_name, sql_op, params.len())
                }
            };

            group_conditions.push(condition);
        }

        let group_sql = match group_conditions.as_slice() {
            [single] => single.clone(),
            _ => format!("({})", group_conditions.join(" OR ")),
        };

        conditions.push(group_sql);
    }

    Ok((conditions, params))
}

pub fn build_search_clause(
    search: Option<&str>,
    columns: &[ColumnDefinition],
    param_offset: usize,
) -> (
    String,
    Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
) {
    let search_term = match search {
        Some(s) if !s.is_empty() => s,
        _ => return (String::new(), vec![]),
    };

    let text_columns: Vec<&str> = columns
        .iter()
        .filter(|c| matches!(c.column_type, ColumnType::Text))
        .map(|c| c.name.as_str())
        .collect();

    if text_columns.is_empty() {
        return (String::new(), vec![]);
    }

    let param_idx = param_offset + 1;
    let pattern = format!("%{}%", search_term);
    let conditions: Vec<String> = text_columns
        .iter()
        .map(|col| format!("{} ILIKE ${}", col, param_idx))
        .collect();

    let clause = format!("({})", conditions.join(" OR "));
    let params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![Box::new(pattern)];

    (clause, params)
}

pub fn build_fts_clause(
    fts: Option<&str>,
    columns: &[ColumnDefinition],
    param_offset: usize,
) -> (
    String,
    Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>,
) {
    let query = match fts {
        Some(q) if !q.is_empty() => q,
        _ => return (String::new(), vec![]),
    };

    let text_columns: Vec<&str> = columns
        .iter()
        .filter(|c| matches!(c.column_type, ColumnType::Text))
        .map(|c| c.name.as_str())
        .collect();

    if text_columns.is_empty() {
        return (String::new(), vec![]);
    }

    let param_idx = param_offset + 1;

    let tsvector_parts: Vec<String> = text_columns
        .iter()
        .map(|col| format!("coalesce({}, '')", col))
        .collect();
    let tsvector_expr = format!(
        "to_tsvector('english', {})",
        tsvector_parts.join(" || ' ' || ")
    );

    let clause = format!(
        "{} @@ plainto_tsquery('english', ${})",
        tsvector_expr, param_idx
    );

    let params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        vec![Box::new(query.to_string())];

    (clause, params)
}

pub fn list_conditions(
    filter_json: Option<&str>,
    search: Option<&str>,
    fts: Option<&str>,
    columns: &[ColumnDefinition],
) -> PlatformResult<Conditions> {
    let (mut conditions, mut params) = filter_conditions(filter_json, columns)?;
    let (search_clause, search_params) = build_search_clause(search, columns, params.len());
    params.extend(search_params);
    let (fts_clause, fts_params) = build_fts_clause(fts, columns, params.len());
    params.extend(fts_params);
    conditions.extend(
        [search_clause, fts_clause]
            .into_iter()
            .filter(|clause| !clause.is_empty()),
    );
    Ok((conditions, params))
}

pub fn requested_fields(select_cols: &[String]) -> Option<&[String]> {
    if select_cols.len() == 1 && select_cols[0] == "*" {
        None
    } else {
        Some(select_cols)
    }
}

pub fn parse_select(select: Option<&str>, columns: &[ColumnDefinition]) -> Vec<String> {
    match select {
        Some(s) if !s.is_empty() => {
            let requested: Vec<&str> = s.split(',').map(|c| c.trim()).collect();
            requested
                .into_iter()
                .filter(|r| columns.iter().any(|c| c.name == *r))
                .map(|s| s.to_string())
                .collect()
        }
        _ => vec!["*".to_string()],
    }
}

pub fn build_order_by(
    sort: Option<&str>,
    columns: &[ColumnDefinition],
    default: &str,
    table: Option<&str>,
) -> String {
    let named = |column: &str| match table {
        Some(table) => format!(
            "{}.{}",
            row_convert::manifest_ident(table),
            row_convert::manifest_ident(column)
        ),
        None => column.to_string(),
    };
    let sort_str = sort.unwrap_or(default);

    let parts: Vec<&str> = sort_str.split(',').map(|s| s.trim()).collect();
    let mut order_parts = Vec::new();

    for part in parts {
        let (column, direction) = if let Some(col) = part.strip_prefix('-') {
            (col, "DESC")
        } else {
            (part, "ASC")
        };

        if columns.iter().any(|c| c.name == column) {
            order_parts.push(format!("{} {}", named(column), direction));
        }
    }

    if order_parts.is_empty() {
        let (default_col, default_dir) = if let Some(col) = default.strip_prefix('-') {
            (col, "DESC")
        } else {
            (default, "ASC")
        };
        format!("ORDER BY {} {}", named(default_col), default_dir)
    } else {
        format!("ORDER BY {}", order_parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_col(name: &str, ct: ColumnType, pk: bool, auto: bool) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type: ct,
            primary_key: pk,
            nullable: false,
            auto_generate: auto,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        }
    }

    fn test_columns() -> Vec<ColumnDefinition> {
        vec![
            test_col("id", ColumnType::Uuid, true, true),
            test_col("title", ColumnType::Text, false, false),
            test_col("completed", ColumnType::Boolean, false, false),
            test_col("created_at", ColumnType::Timestamp, false, true),
        ]
    }

    #[test]
    fn test_order_by_asc() {
        let cols = test_columns();
        let order = build_order_by(Some("title"), &cols, "-created_at", None);
        assert_eq!(order, "ORDER BY title ASC");
    }

    #[test]
    fn test_order_by_desc() {
        let cols = test_columns();
        let order = build_order_by(Some("-created_at"), &cols, "-created_at", None);
        assert_eq!(order, "ORDER BY created_at DESC");
    }

    #[test]
    fn test_order_by_multi() {
        let cols = test_columns();
        let order = build_order_by(Some("-completed,title"), &cols, "-created_at", None);
        assert_eq!(order, "ORDER BY completed DESC, title ASC");
    }

    #[test]
    fn test_order_by_named_in_its_table() {
        let cols = test_columns();
        assert_eq!(
            build_order_by(
                Some("-completed,title"),
                &cols,
                "-created_at",
                Some("todos")
            ),
            "ORDER BY \"todos\".\"completed\" DESC, \"todos\".\"title\" ASC"
        );
        assert_eq!(
            build_order_by(None, &cols, "-created_at", Some("todos")),
            "ORDER BY \"todos\".\"created_at\" DESC"
        );
    }

    #[test]
    fn test_order_by_invalid_column() {
        let cols = test_columns();
        let order = build_order_by(Some("nonexistent"), &cols, "-created_at", None);
        assert_eq!(order, "ORDER BY created_at DESC");
    }

    #[test]
    fn test_select_specific_columns() {
        let cols = test_columns();
        let selected = parse_select(Some("id,title"), &cols);
        assert_eq!(selected, vec!["id", "title"]);
    }

    #[test]
    fn test_select_all() {
        let cols = test_columns();
        let selected = parse_select(None, &cols);
        assert_eq!(selected, vec!["*"]);
    }

    #[test]
    fn test_select_rejects_invalid() {
        let cols = test_columns();
        let selected = parse_select(Some("id,nonexistent,title"), &cols);
        assert_eq!(selected, vec!["id", "title"]);
    }

    #[test]
    fn test_where_clause_empty() {
        let cols = test_columns();
        let (conditions, params) = filter_conditions(None, &cols).expect("conditions");
        assert!(conditions.is_empty());
        assert!(params.is_empty());
        let (conditions, params) = filter_conditions(Some("not json"), &cols).expect("conditions");
        assert!(conditions.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_where_clause_single_filter() {
        let cols = test_columns();
        let filter = r#"[{"n":"completed","f":"=","v":"true"}]"#;
        let (conditions, params) = filter_conditions(Some(filter), &cols).expect("conditions");
        assert_eq!(conditions, ["completed = $1"]);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_where_clause_multiple_filters() {
        let cols = test_columns();
        let filter =
            r#"[{"n":"completed","f":"=","v":"true"},{"n":"title","f":"LIKE","v":"%test%"}]"#;
        let (conditions, params) = filter_conditions(Some(filter), &cols).expect("conditions");
        assert_eq!(conditions, ["completed = $1", "title LIKE $2"]);
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_search_clause() {
        let cols = test_columns();
        let (clause, params) = build_search_clause(Some("hello"), &cols, 0);
        assert!(clause.contains("title ILIKE $1"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_search_clause_empty() {
        let cols = test_columns();
        let (clause, params) = build_search_clause(None, &cols, 0);
        assert!(clause.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_combined_where() {
        let cols = test_columns();
        let filter = r#"[{"n":"completed","f":"=","v":"true"}]"#;
        let (conditions, params) =
            list_conditions(Some(filter), Some("hello"), None, &cols).expect("conditions");
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], "completed = $1");
        assert!(conditions[1].contains("title ILIKE $2"));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn test_fts_clause() {
        let cols = test_columns();
        let (clause, params) = build_fts_clause(Some("hello world"), &cols, 0);
        assert!(clause.contains("to_tsvector('english'"));
        assert!(clause.contains("plainto_tsquery('english', $1)"));
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_fts_clause_empty() {
        let cols = test_columns();
        let (clause, params) = build_fts_clause(None, &cols, 0);
        assert!(clause.is_empty());
        assert!(params.is_empty());
    }

    #[test]
    fn test_combined_with_fts() {
        let cols = test_columns();
        let filter = r#"[{"n":"completed","f":"=","v":"true"}]"#;
        let (conditions, params) =
            list_conditions(Some(filter), None, Some("hello"), &cols).expect("conditions");
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0], "completed = $1");
        assert!(conditions[1].contains("plainto_tsquery('english', $2)"));
        assert_eq!(params.len(), 2);
    }

    #[test]
    fn longest_in_list_counts_the_widest_in() {
        let filter = r#"[{"n":"id","f":"IN","v":"a,b,c"},{"n":"id","f":"in","v":"a"},{"n":"id","f":"=","v":"a,b,c,d"}]"#;
        assert_eq!(longest_in_list(Some(filter)), 3);
        assert_eq!(longest_in_list(Some("not json")), 0);
        assert_eq!(longest_in_list(None), 0);
    }

    #[test]
    fn test_merge_extra_params_column_filter() {
        let cols = test_columns();
        let mut extra = HashMap::new();
        extra.insert("completed".into(), "true".into());

        let merged = merge_extra_params(None, &extra, &cols);
        assert!(merged.is_some());

        let (conditions, params) = filter_conditions(merged.as_deref(), &cols).expect("conditions");
        assert_eq!(conditions, ["completed = $1"]);
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_merge_extra_params_ignores_unknown() {
        let cols = test_columns();
        let mut extra = HashMap::new();
        extra.insert("nonexistent".into(), "value".into());

        let merged = merge_extra_params(None, &extra, &cols);
        assert!(merged.is_none());
    }

    #[test]
    fn test_merge_extra_params_combines_with_filter() {
        let cols = test_columns();
        let mut extra = HashMap::new();
        extra.insert("title".into(), "hello".into());
        let filter = r#"[{"n":"completed","f":"=","v":"true"}]"#;

        let merged = merge_extra_params(Some(filter), &extra, &cols);
        assert!(merged.is_some());

        let (conditions, params) = filter_conditions(merged.as_deref(), &cols).expect("conditions");
        assert_eq!(conditions, ["completed = $1", "title = $2"]);
        assert_eq!(params.len(), 2);
    }
}
