use super::filter_v2::{self, FilterLimits};
use super::keyset::Keys;
use super::search::Search;
use super::SqlParams;
use crate::deployer::manifest::TableDefinition;
use crate::errors::{PlatformError, PlatformResult};
use crate::router::authz::RowScope;
use crate::router::bind;
use crate::router::filtering::{self, ListParams};
use crate::router::row_convert::manifest_ident;
use crate::schema_manager::adopt::AdoptedSchema;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use cufflink_types::Pagination;
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilterMode {
    V1,
    V2,
}

impl FilterMode {
    pub(crate) fn parse(raw: Option<&str>) -> PlatformResult<Option<Self>> {
        match raw {
            None => Ok(None),
            Some("v1") => Ok(Some(Self::V1)),
            Some("v2") => Ok(Some(Self::V2)),
            Some(other) => Err(PlatformError::BadRequest(format!(
                "filter_mode must be 'v1' or 'v2', not '{other}'"
            ))),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CountMode {
    Unasked,
    Exact,
    Estimate,
    None,
}

impl CountMode {
    pub(crate) fn parse(raw: Option<&str>) -> PlatformResult<Self> {
        match raw {
            None => Ok(Self::Unasked),
            Some("exact") => Ok(Self::Exact),
            Some("estimate") => Ok(Self::Estimate),
            Some("none") => Ok(Self::None),
            Some(other) => Err(PlatformError::BadRequest(format!(
                "count must be 'exact', 'estimate' or 'none', not '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListOptions<'a> {
    pub mode: FilterMode,
    pub count: CountMode,
    pub v1_max_in: Option<usize>,
    pub adopted: Option<&'a AdoptedSchema>,
}

const V2_PER_PAGE: std::ops::RangeInclusive<i64> = 1..=500;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Total {
    Exact(i64),
    Estimated(i64),
    Uncounted,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Page {
    pub limit: i64,
    pub offset: i64,
    per_page: i64,
    pagination: Value,
    uncounted: bool,
}

impl Page {
    pub(crate) fn finish(&self, mut rows: Vec<Value>) -> (Vec<Value>, Value) {
        let mut pagination = self.pagination.clone();
        if self.uncounted {
            let has_more = rows.len() as i64 > self.per_page;
            rows.truncate(self.per_page.max(0) as usize);
            if let Some(fields) = pagination.as_object_mut() {
                fields.insert("has_more".to_string(), json!(has_more));
            }
        }
        (rows, pagination)
    }
}

pub(crate) fn page(
    mode: FilterMode,
    page: Option<i64>,
    per_page: Option<i64>,
    total: Total,
) -> PlatformResult<Page> {
    let per_page = match mode {
        FilterMode::V1 => per_page.unwrap_or(50).clamp(1, 200),
        FilterMode::V2 => per_page.unwrap_or(50),
    };
    let requested = page.unwrap_or(1);
    let (current_page, pagination) = match total {
        Total::Exact(records) | Total::Estimated(records) => {
            let total_pages = (records as f64 / per_page as f64).ceil() as i64;
            let last = total_pages.max(1);
            let current_page = match (mode, total) {
                (FilterMode::V1, _) => requested.clamp(1, last),
                (FilterMode::V2, Total::Exact(_)) if requested > last => {
                    return Err(PlatformError::NotFound(format!(
                        "page {requested} is past the last page ({last})"
                    )))
                }
                (FilterMode::V2, _) => requested,
            };
            let mut pagination = json!({
                "current_page": current_page,
                "per_page": per_page,
                "total_records": records,
                "total_pages": total_pages,
            });
            if let (Total::Estimated(_), Some(fields)) = (total, pagination.as_object_mut()) {
                fields.insert("total_records_estimated".to_string(), json!(true));
            }
            (current_page, pagination)
        }
        Total::Uncounted => {
            let current_page = requested.max(1);
            (
                current_page,
                json!({"current_page": current_page, "per_page": per_page}),
            )
        }
    };
    let uncounted = total == Total::Uncounted;
    let offset = (current_page - 1)
        .checked_mul(per_page)
        .ok_or_else(|| PlatformError::BadRequest(format!("page {requested} is out of range")))?;
    Ok(Page {
        limit: per_page + i64::from(uncounted),
        offset,
        per_page,
        pagination,
        uncounted,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Paging {
    Offset {
        page: Option<i64>,
        per_page: Option<i64>,
    },
    Cursor {
        limit: i64,
        key: String,
    },
    Keyset {
        per_page: i64,
        keys: Keys,
    },
}

pub(crate) struct ListPlan {
    from: String,
    where_sql: String,
    pub params: SqlParams,
    pub select: Vec<String>,
    pub requested: Vec<String>,
    expressions: Option<Vec<(String, String)>>,
    order_by: String,
    pub paging: Paging,
}

impl ListPlan {
    pub(crate) fn build(
        table: &TableDefinition,
        named: &TableDefinition,
        params: &ListParams,
        schema: &str,
        rows: &RowScope,
        options: &ListOptions<'_>,
    ) -> PlatformResult<Self> {
        let reached = table;
        let table = named;
        let search = Search::of(table, options.adopted.is_some());
        let (searched, fts) = match search {
            Some(_) => (None, None),
            None => (params.search.as_deref(), params.fts.as_deref()),
        };
        let (filters, mut sql_params) = match options.mode {
            FilterMode::V1 => {
                let merged_filter = filtering::merge_extra_params(
                    params.filter.as_deref(),
                    &params.extra,
                    &table.columns,
                );
                let longest = filtering::longest_in_list(merged_filter.as_deref());
                if let Some(max) = options.v1_max_in.filter(|max| longest > *max) {
                    return Err(PlatformError::BadRequest(format!(
                        "filter: an IN list gives {longest} values, more than the {max} allowed"
                    )));
                }
                let (filters, filter_params) = filtering::list_conditions(
                    merged_filter.as_deref(),
                    searched,
                    fts,
                    &table.columns,
                )?;
                (filters, SqlParams::from(filter_params))
            }
            FilterMode::V2 => v2_conditions(table, params, searched, fts)?,
        };
        let term = [&params.search, &params.fts]
            .into_iter()
            .flatten()
            .find(|term| !term.is_empty());
        let (matching, ranked) = match (&search, term) {
            (Some(search), Some(term)) => {
                let (condition, order) = search.matching(term, &mut sql_params);
                (Some(condition), order)
            }
            _ => (None, None),
        };
        let soft_delete = (table.soft_delete && !params.include_deleted.unwrap_or(false))
            .then(|| "deleted_at IS NULL".to_string());
        let owner = rows.condition(reached, &mut sql_params)?;
        let (paging, after, order_by) = match params.cursor.as_deref() {
            _ if table.pagination == Pagination::Keyset => {
                if params.page.is_some() {
                    return Err(PlatformError::BadRequest(
                        "page: the table is listed by cursor; use cursor and per_page".into(),
                    ));
                }
                let keys = Keys::of(table, params.sort.as_deref())?;
                let after = params
                    .cursor
                    .as_deref()
                    .filter(|cursor| !cursor.is_empty())
                    .map(|cursor| {
                        keys.after(cursor, &mut sql_params, |column, placeholder| match options
                            .adopted
                        {
                            Some(adopted) => adopted.bound(&table.name, column, placeholder),
                            None => placeholder,
                        })
                    })
                    .transpose()?;
                let per_page = per_page(options.mode, params.per_page.or(params.limit))?;
                let order_by = keys.order_by(&table.name);
                (Paging::Keyset { per_page, keys }, after, order_by)
            }
            Some(cursor) => {
                let pk = table
                    .primary_key()
                    .ok_or_else(|| PlatformError::Internal("Table has no primary key".into()))?;
                let after = cursor_key(cursor)?
                    .map(|key| format!("{} > {}", pk.name, sql_params.push(Box::new(key))));
                let limit = params.limit.unwrap_or(50).clamp(1, 200);
                (
                    Paging::Cursor {
                        limit,
                        key: pk.name.clone(),
                    },
                    after,
                    format!("ORDER BY {} ASC", pk.name),
                )
            }
            None => {
                let order_by = match (params.sort.as_deref(), ranked, table.primary_key()) {
                    (None, Some(rank), Some(pk)) => {
                        format!("ORDER BY {rank}, {} ASC", manifest_ident(&pk.name))
                    }
                    _ => filtering::build_order_by(
                        params.sort.as_deref(),
                        &table.columns,
                        &default_sort(table, options.mode)?,
                        options.adopted.map(|_| table.name.as_str()),
                    ),
                };
                (
                    Paging::Offset {
                        page: params.page,
                        per_page: params.per_page,
                    },
                    None,
                    order_by,
                )
            }
        };
        let conditions: Vec<String> = soft_delete
            .into_iter()
            .chain(filters)
            .chain(matching)
            .chain(owner)
            .chain(after)
            .collect();
        let where_sql = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        if options.mode == FilterMode::V2 {
            check_v2_paging(params)?;
        }
        let requested = filtering::parse_select(params.select.as_deref(), &table.columns);
        let select = match &paging {
            Paging::Keyset { keys, .. } if requested != ["*"] => requested
                .iter()
                .map(String::as_str)
                .chain(keys.columns())
                .fold(Vec::new(), |mut select, column| {
                    if !select.iter().any(|known| known == column) {
                        select.push(column.to_string());
                    }
                    select
                }),
            _ => requested.clone(),
        };
        Ok(Self {
            from: format!("{}.{}", schema, table.name),
            where_sql,
            params: sql_params,
            select,
            requested,
            expressions: None,
            order_by,
            paging,
        })
    }

    pub(crate) fn keyset_sql(&self, per_page: i64) -> String {
        format!("{} LIMIT {}", self.select_sql(), per_page + 1)
    }

    pub(crate) fn count_sql(&self) -> String {
        format!("SELECT COUNT(*) FROM {} {}", self.from, self.where_sql)
    }

    pub(crate) fn reltuples_sql(&self) -> Option<(&'static str, String)> {
        self.where_sql.is_empty().then(|| {
            (
                "SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass($1)",
                self.from.clone(),
            )
        })
    }

    pub(crate) fn explain_sql(&self) -> String {
        format!(
            "EXPLAIN (FORMAT JSON) SELECT * FROM {} {}",
            self.from, self.where_sql
        )
    }

    pub(crate) fn page_sql(&self, page: &Page) -> String {
        format!(
            "{} LIMIT {} OFFSET {}",
            self.select_sql(),
            page.limit,
            page.offset
        )
    }

    pub(crate) fn cursor_sql(&self, limit: i64) -> String {
        format!("{} LIMIT {}", self.select_sql(), limit)
    }

    pub(crate) fn reading(self, expressions: Vec<(String, String)>) -> Self {
        Self {
            expressions: Some(expressions),
            ..self
        }
    }

    fn selected(&self) -> String {
        let requested = filtering::requested_fields(&self.select);
        match &self.expressions {
            None => self.select.join(", "),
            Some(expressions) => expressions
                .iter()
                .filter(|(name, _)| requested.is_none_or(|fields| fields.contains(name)))
                .map(|(_, expression)| expression.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        }
    }

    fn select_sql(&self) -> String {
        format!(
            "SELECT {} FROM {} {} {}",
            self.selected(),
            self.from,
            self.where_sql,
            self.order_by
        )
    }
}

fn per_page(mode: FilterMode, requested: Option<i64>) -> PlatformResult<i64> {
    match mode {
        FilterMode::V1 => Ok(requested.unwrap_or(50).clamp(1, 200)),
        FilterMode::V2 => match requested {
            Some(per_page) if !V2_PER_PAGE.contains(&per_page) => {
                Err(PlatformError::BadRequest(format!(
                    "per_page must be between {} and {}, not {per_page}",
                    V2_PER_PAGE.start(),
                    V2_PER_PAGE.end()
                )))
            }
            _ => Ok(requested.unwrap_or(50)),
        },
    }
}

pub(crate) fn explained_rows(plan: &Value) -> Option<i64> {
    plan.get(0)?
        .get("Plan")?
        .get("Plan Rows")?
        .as_f64()
        .map(|rows| rows.round() as i64)
}

fn default_sort(table: &TableDefinition, mode: FilterMode) -> PlatformResult<String> {
    if mode == FilterMode::V1 || table.columns.iter().any(|c| c.name == "created_at") {
        return Ok("-created_at".to_string());
    }
    table
        .primary_key()
        .map(|pk| pk.name.clone())
        .ok_or_else(|| PlatformError::Internal("Table has no primary key".into()))
}

fn check_v2_paging(params: &ListParams) -> PlatformResult<()> {
    if let Some(per_page) = params.per_page.filter(|p| !V2_PER_PAGE.contains(p)) {
        return Err(PlatformError::BadRequest(format!(
            "per_page must be between {} and {}, not {per_page}",
            V2_PER_PAGE.start(),
            V2_PER_PAGE.end()
        )));
    }
    match params.page {
        Some(page) if page < 1 => Err(PlatformError::BadRequest(format!(
            "page must be at least 1, not {page}"
        ))),
        _ => Ok(()),
    }
}

fn v2_conditions(
    table: &TableDefinition,
    params: &ListParams,
    search: Option<&str>,
    fts: Option<&str>,
) -> PlatformResult<(Vec<String>, SqlParams)> {
    let limits = FilterLimits::default();
    let bad = |e: filter_v2::FilterError| PlatformError::BadRequest(e.to_string());
    let mut sql_params = SqlParams::default();
    let tree = params
        .filter
        .as_deref()
        .map(|json| {
            filter_v2::parse(json, &limits)
                .and_then(|filter| filter_v2::to_sql(&filter, table, &mut sql_params, &limits))
        })
        .transpose()
        .map_err(bad)?;
    let mut extra: Vec<(&String, &String)> = params.extra.iter().collect();
    extra.sort();
    let equalities = extra
        .into_iter()
        .filter_map(|(key, value)| {
            table.columns.iter().find(|c| c.name == *key).map(|column| {
                bind::bind_column_value(column, &Value::String(value.clone()))
                    .map(|bound| {
                        format!(
                            "{} = {}",
                            manifest_ident(&column.name),
                            sql_params.push(bound)
                        )
                    })
                    .map_err(|e| PlatformError::BadRequest(format!("filter: {e}")))
            })
        })
        .collect::<PlatformResult<Vec<_>>>()?;
    let (search, search_params) =
        filtering::build_search_clause(search, &table.columns, sql_params.len());
    sql_params.extend(search_params);
    let (fts, fts_params) = filtering::build_fts_clause(fts, &table.columns, sql_params.len());
    sql_params.extend(fts_params);
    let conditions = tree
        .into_iter()
        .chain(equalities)
        .chain([search, fts].into_iter().filter(|c| !c.is_empty()))
        .collect();
    Ok((conditions, sql_params))
}

fn cursor_key(cursor: &str) -> PlatformResult<Option<Uuid>> {
    if cursor.is_empty() {
        return Ok(None);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| PlatformError::BadRequest("Invalid cursor".into()))?;
    let text = String::from_utf8(bytes)
        .map_err(|_| PlatformError::BadRequest("Invalid cursor encoding".into()))?;
    Uuid::parse_str(&text)
        .map(Some)
        .map_err(|_| PlatformError::BadRequest("Invalid cursor value".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::columns::ColumnView;
    use crate::deployer::manifest::{ColumnDefinition, ColumnType};

    fn sql(
        table: &TableDefinition,
        params: &ListParams,
        subject: Option<&str>,
        total: i64,
    ) -> Result<Vec<String>, String> {
        let rows = RowScope::Legacy {
            owner: table
                .owner_field
                .clone()
                .map(|column| (column, subject.map(str::to_string))),
        };
        let options = ListOptions {
            mode: FilterMode::V1,
            count: CountMode::Exact,
            v1_max_in: None,
            adopted: None,
        };
        let plan = ListPlan::build(table, table, params, "svc_shop", &rows, &options)
            .map_err(|e| e.to_string())?;
        Ok(match plan.paging {
            Paging::Cursor { limit, .. } => {
                vec![plan.cursor_sql(limit), format!("{:?}", plan.params)]
            }
            Paging::Keyset { per_page, .. } => {
                vec![plan.keyset_sql(per_page), format!("{:?}", plan.params)]
            }
            Paging::Offset { page, per_page } => vec![
                plan.count_sql(),
                plan.page_sql(
                    &super::page(FilterMode::V1, page, per_page, Total::Exact(total))
                        .map_err(|e| e.to_string())?,
                ),
                format!("{:?}", plan.params),
            ],
        })
    }

    fn column(name: &str, column_type: ColumnType, primary_key: bool) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            column_type,
            primary_key,
            nullable: false,
            auto_generate: primary_key,
            default_value: None,
            references: None,
            on_delete: None,
            unique: false,
            validations: vec![],
            ..Default::default()
        }
    }

    fn table(soft_delete: bool, owner: Option<&str>, pk: bool) -> TableDefinition {
        TableDefinition {
            name: "items".into(),
            columns: vec![
                column("id", ColumnType::Uuid, pk),
                column("title", ColumnType::Text, false),
                column("body", ColumnType::Text, false),
                column("status", ColumnType::Text, false),
                column("rank", ColumnType::Integer, false),
                column("owner_id", ColumnType::Uuid, false),
                column("created_at", ColumnType::Timestamp, false),
            ],
            indexes: vec![],
            soft_delete,
            owner_field: owner.map(str::to_string),
            auth_required: false,
            permission_area: None,
            owner_self_service: false,
            hooks: None,
            scoped_by: None,
            ..Default::default()
        }
    }

    fn params(pairs: &[(&str, &str)]) -> ListParams {
        let encode = |s: &str| -> String {
            s.bytes()
                .map(|b| {
                    if b.is_ascii_alphanumeric() {
                        char::from(b).to_string()
                    } else {
                        format!("%{b:02X}")
                    }
                })
                .collect()
        };
        let query: Vec<String> = pairs
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect();
        let uri: axum::http::Uri = format!("/items?{}", query.join("&")).parse().expect("uri");
        axum::extract::Query::<ListParams>::try_from_uri(&uri)
            .expect("list params")
            .0
    }

    #[test]
    fn list_sql_is_unchanged() {
        let subject = "7d5bd1a8-4ab6-4b25-9a2e-7a3a3b7a9d11";
        let cursor = URL_SAFE_NO_PAD.encode("0f8fad5b-d9cb-469f-a165-70867728950e");
        let filter = r#"[{"n":"status","f":"=","v":"open"},{"n":"status","f":"=","v":"new"},{"n":"rank","f":">=","v":"3"},{"n":"title","f":"ILIKE","v":"%a%"},{"n":"owner_id","f":"IS NULL"},{"n":"id","f":"IN","v":"0f8fad5b-d9cb-469f-a165-70867728950e, bad"},{"n":"ghost","f":"=","v":"x"}]"#;
        type Case<'a> = (
            &'a str,
            TableDefinition,
            Vec<(&'a str, &'a str)>,
            Option<&'a str>,
            i64,
            Result<Vec<&'a str>, &'a str>,
        );
        let cases: Vec<Case> = vec![
            (
                "plain",
                table(false, None, true),
                vec![],
                None,
                0,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items "#,
                    r#"SELECT * FROM svc_shop.items  ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"[]"#,
                ]),
            ),
            (
                "paged",
                table(false, None, true),
                vec![
                    ("page", "3"),
                    ("per_page", "10"),
                    ("sort", "title,-rank,ghost"),
                    ("select", "id, title, ghost"),
                ],
                None,
                45,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items "#,
                    r#"SELECT id, title FROM svc_shop.items  ORDER BY title ASC, rank DESC LIMIT 10 OFFSET 20"#,
                    r#"[]"#,
                ]),
            ),
            (
                "page_past_end",
                table(false, None, true),
                vec![("page", "9"), ("per_page", "500")],
                None,
                45,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items "#,
                    r#"SELECT * FROM svc_shop.items  ORDER BY created_at DESC LIMIT 200 OFFSET 0"#,
                    r#"[]"#,
                ]),
            ),
            (
                "filter",
                table(false, None, true),
                vec![("filter", filter)],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6)"#,
                    r#"SELECT * FROM svc_shop.items WHERE (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["open", "new", 3, "%a%", 0f8fad5b-d9cb-469f-a165-70867728950e, 00000000-0000-0000-0000-000000000000]"#,
                ]),
            ),
            (
                "bad_filter",
                table(false, None, true),
                vec![("filter", "{not json")],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items "#,
                    r#"SELECT * FROM svc_shop.items  ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"[]"#,
                ]),
            ),
            (
                "extra",
                table(false, None, true),
                vec![("status", "closed")],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE status = $1"#,
                    r#"SELECT * FROM svc_shop.items WHERE status = $1 ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["closed"]"#,
                ]),
            ),
            (
                "search_fts",
                table(false, None, true),
                vec![("search", "lamp"), ("fts", "brass lamp")],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE (title ILIKE $1 OR body ILIKE $1 OR status ILIKE $1) AND to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(status, '')) @@ plainto_tsquery('english', $2)"#,
                    r#"SELECT * FROM svc_shop.items WHERE (title ILIKE $1 OR body ILIKE $1 OR status ILIKE $1) AND to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(status, '')) @@ plainto_tsquery('english', $2) ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["%lamp%", "brass lamp"]"#,
                ]),
            ),
            (
                "soft",
                table(true, None, true),
                vec![],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE deleted_at IS NULL"#,
                    r#"SELECT * FROM svc_shop.items WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"[]"#,
                ]),
            ),
            (
                "soft_filter",
                table(true, None, true),
                vec![("filter", filter), ("search", "x")],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE deleted_at IS NULL AND (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) AND (title ILIKE $7 OR body ILIKE $7 OR status ILIKE $7)"#,
                    r#"SELECT * FROM svc_shop.items WHERE deleted_at IS NULL AND (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) AND (title ILIKE $7 OR body ILIKE $7 OR status ILIKE $7) ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["open", "new", 3, "%a%", 0f8fad5b-d9cb-469f-a165-70867728950e, 00000000-0000-0000-0000-000000000000, "%x%"]"#,
                ]),
            ),
            (
                "soft_included",
                table(true, None, true),
                vec![("include_deleted", "true")],
                None,
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items "#,
                    r#"SELECT * FROM svc_shop.items  ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"[]"#,
                ]),
            ),
            (
                "owner",
                table(false, Some("owner_id"), true),
                vec![],
                Some(subject),
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE owner_id = $1"#,
                    r#"SELECT * FROM svc_shop.items WHERE owner_id = $1 ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"[7d5bd1a8-4ab6-4b25-9a2e-7a3a3b7a9d11]"#,
                ]),
            ),
            (
                "owner_filter",
                table(true, Some("owner_id"), true),
                vec![("filter", filter), ("fts", "q")],
                Some(subject),
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE deleted_at IS NULL AND (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) AND to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(status, '')) @@ plainto_tsquery('english', $7) AND owner_id = $8"#,
                    r#"SELECT * FROM svc_shop.items WHERE deleted_at IS NULL AND (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) AND to_tsvector('english', coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(status, '')) @@ plainto_tsquery('english', $7) AND owner_id = $8 ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["open", "new", 3, "%a%", 0f8fad5b-d9cb-469f-a165-70867728950e, 00000000-0000-0000-0000-000000000000, "q", 7d5bd1a8-4ab6-4b25-9a2e-7a3a3b7a9d11]"#,
                ]),
            ),
            (
                "owner_anonymous",
                table(false, Some("owner_id"), true),
                vec![],
                None,
                1,
                Err(r#"Unauthorized: Authentication required for this resource"#),
            ),
            (
                "owner_bad_subject",
                table(false, Some("owner_id"), true),
                vec![],
                Some("not-a-uuid"),
                1,
                Err(
                    r#"Unauthorized: authenticated subject is not a uuid as owner_field 'owner_id' requires"#,
                ),
            ),
            (
                "owner_text",
                table(false, Some("title"), true),
                vec![],
                Some("someone"),
                1,
                Ok(vec![
                    r#"SELECT COUNT(*) FROM svc_shop.items WHERE title = $1"#,
                    r#"SELECT * FROM svc_shop.items WHERE title = $1 ORDER BY created_at DESC LIMIT 50 OFFSET 0"#,
                    r#"["someone"]"#,
                ]),
            ),
            (
                "cursor_start",
                table(false, None, true),
                vec![("cursor", ""), ("limit", "500")],
                None,
                1,
                Ok(vec![
                    r#"SELECT * FROM svc_shop.items  ORDER BY id ASC LIMIT 200"#,
                    r#"[]"#,
                ]),
            ),
            (
                "cursor_after",
                table(true, Some("owner_id"), true),
                vec![
                    ("cursor", cursor.as_str()),
                    ("filter", filter),
                    ("select", "title"),
                ],
                Some(subject),
                1,
                Ok(vec![
                    r#"SELECT title FROM svc_shop.items WHERE deleted_at IS NULL AND (status = $1 OR status = $2) AND rank >= $3 AND title ILIKE $4 AND owner_id IS NULL AND id IN ($5, $6) AND owner_id = $7 AND id > $8 ORDER BY id ASC LIMIT 50"#,
                    r#"["open", "new", 3, "%a%", 0f8fad5b-d9cb-469f-a165-70867728950e, 00000000-0000-0000-0000-000000000000, 7d5bd1a8-4ab6-4b25-9a2e-7a3a3b7a9d11, 0f8fad5b-d9cb-469f-a165-70867728950e]"#,
                ]),
            ),
            (
                "cursor_bad_b64",
                table(false, None, true),
                vec![("cursor", "!!!")],
                None,
                1,
                Err(r#"Bad request: Invalid cursor"#),
            ),
            (
                "cursor_bad_utf8",
                table(false, None, true),
                vec![("cursor", "_w")],
                None,
                1,
                Err(r#"Bad request: Invalid cursor encoding"#),
            ),
            (
                "cursor_bad_uuid",
                table(false, None, true),
                vec![("cursor", "YWJj")],
                None,
                1,
                Err(r#"Bad request: Invalid cursor value"#),
            ),
            (
                "cursor_no_pk",
                table(false, None, false),
                vec![("cursor", "")],
                None,
                1,
                Err(r#"Internal error: Table has no primary key"#),
            ),
        ];
        for (name, table, query, subject, total, expected) in cases {
            let expected = expected
                .map(|sql| sql.into_iter().map(str::to_string).collect::<Vec<_>>())
                .map_err(str::to_string);
            assert_eq!(
                sql(&table, &params(&query), subject, total),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn v1_pages_clamp_as_they_always_did() {
        let window = |page, per_page, total| {
            let page = super::page(FilterMode::V1, page, per_page, Total::Exact(total))
                .expect("v1 never refuses");
            (page.limit, page.offset, page.finish(vec![]).1)
        };
        assert_eq!(
            window(None, None, 100),
            (
                50,
                0,
                json!({"current_page": 1, "per_page": 50, "total_records": 100, "total_pages": 2})
            )
        );
        assert_eq!(
            window(Some(2), Some(10), 25).2,
            json!({"current_page": 2, "per_page": 10, "total_records": 25, "total_pages": 3})
        );
        assert_eq!(window(Some(100), Some(10), 25).2["current_page"], 3);
        assert_eq!(
            window(None, Some(500), 100).0,
            200,
            "per_page clamps silently"
        );
        assert_eq!(window(None, Some(0), 100).0, 1);
        assert_eq!(
            window(None, None, 0).2,
            json!({"current_page": 1, "per_page": 50, "total_records": 0, "total_pages": 0})
        );
        assert_eq!(window(Some(-3), None, 10).1, 0);
    }

    #[test]
    fn v2_pages_refuse_the_past_and_count_as_asked() {
        let v2 = |page, per_page, total| super::page(FilterMode::V2, page, per_page, total);
        assert_eq!(
            v2(Some(4), Some(10), Total::Exact(25)).map_err(|e| e.to_string()),
            Err("Not found: page 4 is past the last page (3)".to_string())
        );
        assert_eq!(
            v2(Some(2), None, Total::Exact(0)).map_err(|e| e.to_string()),
            Err("Not found: page 2 is past the last page (1)".to_string())
        );
        let first = v2(Some(1), Some(500), Total::Exact(0)).expect("page 1 always exists");
        assert_eq!((first.limit, first.offset), (500, 0));

        let estimated = v2(Some(9), Some(10), Total::Estimated(25)).expect("estimates never 404");
        assert_eq!(
            estimated.finish(vec![]).1,
            json!({
                "current_page": 9, "per_page": 10, "total_records": 25, "total_pages": 3,
                "total_records_estimated": true
            })
        );

        let uncounted = v2(Some(2), Some(2), Total::Uncounted).expect("uncounted");
        assert_eq!((uncounted.limit, uncounted.offset), (3, 2));
        let (rows, pagination) = uncounted.finish(vec![json!(1), json!(2), json!(3)]);
        assert_eq!(rows, vec![json!(1), json!(2)]);
        assert_eq!(
            pagination,
            json!({"current_page": 2, "per_page": 2, "has_more": true})
        );
        assert_eq!(
            uncounted.finish(vec![json!(1)]),
            (
                vec![json!(1)],
                json!({"current_page": 2, "per_page": 2, "has_more": false})
            )
        );
    }

    #[test]
    fn a_page_whose_offset_overflows_is_refused() {
        for (mode, total) in [
            (FilterMode::V2, Total::Uncounted),
            (FilterMode::V2, Total::Estimated(10)),
            (FilterMode::V1, Total::Uncounted),
        ] {
            assert_eq!(
                super::page(mode, Some(i64::MAX), Some(10), total).map_err(|e| e.to_string()),
                Err(format!("Bad request: page {} is out of range", i64::MAX)),
                "{mode:?} {total:?}"
            );
        }
        let last = super::page(FilterMode::V1, Some(i64::MAX), None, Total::Exact(10))
            .expect("v1 clamps a counted page");
        assert_eq!(last.offset, 0);
    }

    fn v2_sql(
        table: &TableDefinition,
        query: &[(&str, &str)],
        count: CountMode,
    ) -> Result<Vec<String>, String> {
        let options = ListOptions {
            mode: FilterMode::V2,
            count,
            v1_max_in: None,
            adopted: None,
        };
        let plan = ListPlan::build(
            table,
            table,
            &params(query),
            "svc_shop",
            &RowScope::Legacy { owner: None },
            &options,
        )
        .map_err(|e| e.to_string())?;
        let page = super::page(FilterMode::V2, Some(1), Some(10), Total::Exact(1))
            .map_err(|e| e.to_string())?;
        Ok(vec![
            plan.page_sql(&page),
            plan.explain_sql(),
            format!("{:?}", plan.reltuples_sql()),
            format!("{:?}", plan.params),
        ])
    }

    fn withheld_body() -> (TableDefinition, ColumnView) {
        let mut items = table(false, None, true);
        items.permission_area = Some("items".into());
        items.columns[2].access = Some(cufflink_types::ColumnAccess {
            read: Some("internal".into()),
            write: None,
        });
        let view = crate::auth::columns::view(&items, &[], false);
        (items, view)
    }

    fn page_sql(
        table: &TableDefinition,
        query: &[(&str, &str)],
        mode: FilterMode,
        rows: &RowScope,
        columns: &ColumnView,
    ) -> Result<Vec<String>, String> {
        let options = ListOptions {
            mode,
            count: CountMode::Exact,
            v1_max_in: None,
            adopted: None,
        };
        let plan = ListPlan::build(
            table,
            &columns.named(table),
            &params(query),
            "svc_shop",
            rows,
            &options,
        )
        .map_err(|e| e.to_string())?;
        let page =
            super::page(mode, Some(1), Some(10), Total::Exact(1)).map_err(|e| e.to_string())?;
        Ok(vec![plan.page_sql(&page), format!("{:?}", plan.params)])
    }

    #[test]
    fn a_withheld_column_is_no_column_of_the_listing() {
        let (items, withheld) = withheld_body();
        let every = RowScope::Legacy { owner: None };
        let v1 = |query: &[(&str, &str)], columns: &ColumnView| {
            page_sql(&items, query, FilterMode::V1, &every, columns)
        };
        let named = [
            ("select", "id,body,title"),
            ("sort", "-body,title"),
            ("body", "secret"),
            (
                "filter",
                r#"[{"n": "body", "f": "LIKE", "v": "s%"}, {"n": "title", "f": "=", "v": "lamp"}]"#,
            ),
            ("search", "lamp"),
        ];
        assert_eq!(
            v1(&named, &withheld),
            Ok(vec![
                "SELECT id, title FROM svc_shop.items WHERE title = $1 AND (title ILIKE $2 OR status ILIKE $2) ORDER BY title ASC LIMIT 10 OFFSET 0".to_string(),
                r#"["lamp", "%lamp%"]"#.to_string(),
            ])
        );
        let granted = v1(&named, &ColumnView::ALL).expect("sql");
        assert!(
            granted[0].starts_with("SELECT id, body, title FROM")
                && granted[0].contains("body ILIKE")
                && granted[0].contains("ORDER BY body DESC, title ASC"),
            "{granted:?}"
        );
        for (query, refused) in [
            (("filter", r#"{"f": "body", "op": "eq", "v": "s"}"#), "body"),
            (
                ("filter", r#"{"f": "ghost", "op": "eq", "v": "s"}"#),
                "ghost",
            ),
        ] {
            let err =
                page_sql(&items, &[query], FilterMode::V2, &every, &withheld).expect_err("refused");
            let unknown =
                page_sql(&items, &[query], FilterMode::V2, &every, &ColumnView::ALL).err();
            assert!(err.contains(refused), "{err}");
            assert_eq!(unknown.is_some(), refused == "ghost", "{unknown:?}");
        }
    }

    #[test]
    fn rows_are_reached_through_a_column_the_caller_does_not_read() {
        let (mut items, _) = withheld_body();
        items.owner_field = Some("owner_id".into());
        items.scoped_by = Some("items".into());
        items.columns[2].name = "team".into();
        let withheld = crate::auth::columns::view(&items, &[], false);
        let by_team = RowScope::Scoped(crate::auth::scope::EffectiveScope::Rows(vec![
            crate::auth::scope::RowPredicate::Claim {
                column: "team".into(),
                values: vec![json!("north")],
            },
        ]));
        assert_eq!(
            page_sql(&items, &[("team", "south")], FilterMode::V1, &by_team, &withheld),
            Ok(vec![
                r#"SELECT * FROM svc_shop.items WHERE ("team" = ANY($1)) ORDER BY created_at DESC LIMIT 10 OFFSET 0"#.to_string(),
                r#"[["north"]]"#.to_string(),
            ])
        );
    }

    #[test]
    fn v2_listings_are_typed_and_strict() {
        let filter = r#"{"or": [{"f": "status", "op": "in", "v": ["open", "new"]}, {"f": "rank", "op": "gte", "v": 3}]}"#;
        assert_eq!(
            v2_sql(&table(true, None, true), &[("filter", filter), ("status", "open"), ("ghost", "x"), ("search", "lamp")], CountMode::Exact),
            Ok(vec![
                r#"SELECT * FROM svc_shop.items WHERE deleted_at IS NULL AND ("status" = ANY($1) OR "rank" >= $2) AND "status" = $3 AND (title ILIKE $4 OR body ILIKE $4 OR status ILIKE $4) ORDER BY created_at DESC LIMIT 10 OFFSET 0"#.to_string(),
                r#"EXPLAIN (FORMAT JSON) SELECT * FROM svc_shop.items WHERE deleted_at IS NULL AND ("status" = ANY($1) OR "rank" >= $2) AND "status" = $3 AND (title ILIKE $4 OR body ILIKE $4 OR status ILIKE $4)"#.to_string(),
                "None".to_string(),
                r#"[["open", "new"], 3, "open", "%lamp%"]"#.to_string(),
            ])
        );
        assert_eq!(
            v2_sql(&table(false, None, true), &[], CountMode::Estimate).map(|sql| sql[2].clone()),
            Ok(r#"Some(("SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass($1)", "svc_shop.items"))"#.to_string())
        );
        for (query, error) in [
            (
                vec![("filter", "{bad")],
                "Bad request: filter: not valid JSON",
            ),
            (
                vec![("rank", "x")],
                r#"Bad request: filter: column 'rank': expected a 32-bit integer, got "x""#,
            ),
            (
                vec![("per_page", "501")],
                "Bad request: per_page must be between 1 and 500, not 501",
            ),
            (
                vec![("per_page", "0")],
                "Bad request: per_page must be between 1 and 500, not 0",
            ),
            (
                vec![("page", "0")],
                "Bad request: page must be at least 1, not 0",
            ),
        ] {
            let err =
                v2_sql(&table(false, None, true), &query, CountMode::Exact).expect_err("refused");
            assert!(err.starts_with(error), "{query:?}: {err}");
        }
    }

    #[test]
    fn v2_sorts_by_the_key_when_there_is_no_created_at() {
        let mut keyed = table(false, None, true);
        keyed.columns.retain(|c| c.name != "created_at");
        assert!(v2_sql(&keyed, &[], CountMode::Exact).expect("sql")[0].contains("ORDER BY id ASC"));
        let v1 = ListPlan::build(
            &keyed,
            &keyed,
            &params(&[]),
            "svc_shop",
            &RowScope::Legacy { owner: None },
            &ListOptions {
                mode: FilterMode::V1,
                count: CountMode::Exact,
                v1_max_in: None,
                adopted: None,
            },
        )
        .expect("plan");
        let page = super::page(FilterMode::V1, None, None, Total::Exact(0)).expect("page");
        assert!(
            v1.page_sql(&page).contains("ORDER BY created_at DESC"),
            "v1 keeps its SQL"
        );
    }

    #[test]
    fn explain_rows_are_read_from_the_top_plan() {
        assert_eq!(
            explained_rows(&json!([{"Plan": {"Node Type": "Seq Scan", "Plan Rows": 1234.6}}])),
            Some(1235)
        );
        assert_eq!(explained_rows(&json!([])), None);
        assert_eq!(
            CountMode::parse(Some("sometimes")).map_err(|e| e.to_string()),
            Err("Bad request: count must be 'exact', 'estimate' or 'none', not 'sometimes'".into())
        );
        assert_eq!(
            FilterMode::parse(Some("v2")).expect("v2"),
            Some(FilterMode::V2)
        );
        assert!(FilterMode::parse(Some("v3")).is_err());
    }

    #[test]
    fn a_v1_in_list_is_bounded_only_when_configured() {
        let filter = r#"[{"n":"rank","f":"IN","v":"1,2,3"}]"#;
        let build = |v1_max_in| {
            ListPlan::build(
                &table(false, None, true),
                &table(false, None, true),
                &params(&[("filter", filter)]),
                "svc_shop",
                &RowScope::Legacy { owner: None },
                &ListOptions {
                    mode: FilterMode::V1,
                    count: CountMode::Exact,
                    v1_max_in,
                    adopted: None,
                },
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
        };
        assert_eq!(build(None), Ok(()));
        assert_eq!(build(Some(3)), Ok(()));
        assert_eq!(
            build(Some(2)),
            Err("Bad request: filter: an IN list gives 3 values, more than the 2 allowed".into())
        );
    }

    #[test]
    fn the_columns_of_an_adopted_table_are_selected_as_they_are_to_be_read() {
        let table = table(false, None, true);
        let expressions = || {
            vec![
                ("id".to_string(), "\"id\"".to_string()),
                (
                    "status".to_string(),
                    "\"status\"::TEXT AS \"status\"".to_string(),
                ),
                ("title".to_string(), "\"title\"".to_string()),
            ]
        };
        let options = ListOptions {
            mode: FilterMode::V1,
            count: CountMode::Exact,
            v1_max_in: None,
            adopted: None,
        };
        let page = super::page(FilterMode::V1, Some(1), Some(50), Total::Exact(1)).expect("a page");
        let listed = |query: &[(&str, &str)]| {
            ListPlan::build(
                &table,
                &table,
                &params(query),
                "svc_shop",
                &RowScope::Legacy { owner: None },
                &options,
            )
            .expect("a plan")
            .reading(expressions())
            .page_sql(&page)
        };
        assert_eq!(
            listed(&[]),
            "SELECT \"id\", \"status\"::TEXT AS \"status\", \"title\" FROM svc_shop.items  \
             ORDER BY created_at DESC LIMIT 50 OFFSET 0"
        );
        assert_eq!(
            listed(&[("select", "status,id")]),
            "SELECT \"id\", \"status\"::TEXT AS \"status\" FROM svc_shop.items  \
             ORDER BY created_at DESC LIMIT 50 OFFSET 0"
        );
    }

    mod declared_search_and_keyset {
        use super::*;
        use cufflink_types::{Pagination, SearchSpec};

        fn searched(pagination: Pagination, adopted_vector: Option<&str>) -> TableDefinition {
            TableDefinition {
                search: Some(SearchSpec {
                    columns: vec!["title".into(), "body".into()],
                    config: "english".into(),
                    vector_column: adopted_vector.map(str::to_string),
                }),
                pagination,
                ..table(false, None, true)
            }
        }

        fn build(
            table: &TableDefinition,
            query: &[(&str, &str)],
            mode: FilterMode,
            adopted: Option<&AdoptedSchema>,
        ) -> Result<(ListPlan, Vec<String>), String> {
            let options = ListOptions {
                mode,
                count: CountMode::Unasked,
                v1_max_in: None,
                adopted,
            };
            let plan = ListPlan::build(
                table,
                table,
                &params(query),
                "svc_shop",
                &RowScope::Legacy { owner: None },
                &options,
            )
            .map_err(|e| e.to_string())?;
            let statements = match &plan.paging {
                Paging::Keyset { per_page, .. } => vec![plan.keyset_sql(*per_page)],
                Paging::Cursor { limit, .. } => vec![plan.cursor_sql(*limit)],
                Paging::Offset { page, per_page } => {
                    let page = super::page(plan_mode(&plan), *page, *per_page, Total::Exact(9))
                        .map_err(|e| e.to_string())?;
                    vec![plan.count_sql(), plan.page_sql(&page)]
                }
            };
            Ok((plan, statements))
        }

        fn plan_mode(_: &ListPlan) -> FilterMode {
            FilterMode::V1
        }

        #[test]
        fn a_declared_search_matches_the_vector_and_ranks_when_nothing_else_orders() {
            let kept = searched(Pagination::Offset, None);
            for mode in [FilterMode::V1, FilterMode::V2] {
                let (plan, statements) =
                    build(&kept, &[("search", "brass lamp")], mode, None).expect("plan");
                assert_eq!(
                    statements[1],
                    "SELECT * FROM svc_shop.items WHERE \"_search\" @@ websearch_to_tsquery('english', $1) \
                     ORDER BY ts_rank(\"_search\", websearch_to_tsquery('english', $1)) DESC, \"id\" ASC \
                     LIMIT 50 OFFSET 0",
                    "{mode:?}"
                );
                assert_eq!(format!("{:?}", plan.params), "[\"brass lamp\"]");
            }
            let (_, statements) = build(
                &kept,
                &[("fts", "lamp"), ("sort", "-rank"), ("status", "open")],
                FilterMode::V2,
                None,
            )
            .expect("plan");
            assert_eq!(
                statements[1],
                "SELECT * FROM svc_shop.items WHERE \"status\" = $1 AND \"_search\" @@ \
                 websearch_to_tsquery('english', $2) ORDER BY rank DESC LIMIT 50 OFFSET 0"
            );
            let (_, statements) = build(&kept, &[], FilterMode::V1, None).expect("plan");
            assert_eq!(
                statements[1],
                "SELECT * FROM svc_shop.items  ORDER BY created_at DESC LIMIT 50 OFFSET 0",
                "without a term the table is listed as it always was"
            );
        }

        #[test]
        fn an_adopted_table_is_searched_by_its_vector_or_by_pattern() {
            let own = searched(Pagination::Offset, Some("document"));
            let (_, statements) = build(
                &own,
                &[("search", "lamp")],
                FilterMode::V1,
                Some(&AdoptedSchema::default()),
            )
            .expect("plan");
            assert!(
                statements[1].contains("\"document\" @@ websearch_to_tsquery('english', $1)"),
                "{}",
                statements[1]
            );
            let columns = searched(Pagination::Offset, None);
            let (plan, statements) = build(
                &columns,
                &[("search", "50%")],
                FilterMode::V1,
                Some(&AdoptedSchema::default()),
            )
            .expect("plan");
            assert_eq!(
                statements[1],
                "SELECT * FROM svc_shop.items WHERE (\"title\"::text ILIKE $1 ESCAPE '\\' OR \
                 \"body\"::text ILIKE $1 ESCAPE '\\') ORDER BY \"items\".\"created_at\" DESC \
                 LIMIT 50 OFFSET 0"
            );
            assert_eq!(format!("{:?}", plan.params), "[\"%50\\\\%%\"]");
        }

        #[test]
        fn a_keyset_listing_reads_one_row_past_its_page_in_the_order_of_its_keys() {
            let keyed = TableDefinition {
                pagination: Pagination::Keyset,
                ..table(false, None, true)
            };
            let (plan, statements) = build(&keyed, &[], FilterMode::V1, None).expect("plan");
            assert_eq!(
                statements,
                ["SELECT * FROM svc_shop.items  ORDER BY \"items\".\"created_at\" DESC, \"items\".\"id\" DESC \
                  LIMIT 51"]
            );
            assert_eq!(plan.requested, ["*"]);

            let (plan, statements) = build(
                &keyed,
                &[
                    ("sort", "status,-rank"),
                    ("per_page", "10"),
                    ("select", "title"),
                ],
                FilterMode::V2,
                None,
            )
            .expect("plan");
            assert_eq!(
                statements,
                [
                    "SELECT title, status, rank, id FROM svc_shop.items  ORDER BY \"items\".\"status\" ASC, \
                  \"items\".\"rank\" DESC, \"items\".\"id\" DESC LIMIT 11"
                ]
            );
            assert_eq!(plan.requested, ["title"]);
            let Paging::Keyset { keys, .. } = &plan.paging else {
                panic!("keyset");
            };
            let cursor = keys
                .cursor_after(&serde_json::json!({
                    "status": "open", "rank": 3, "id": "0f8fad5b-d9cb-469f-a165-70867728950e"
                }))
                .expect("cursor");
            let (plan, statements) = build(
                &keyed,
                &[
                    ("sort", "status,-rank"),
                    ("cursor", &cursor),
                    ("limit", "500"),
                ],
                FilterMode::V1,
                None,
            )
            .expect("plan");
            assert_eq!(
                statements,
                ["SELECT * FROM svc_shop.items WHERE ((\"status\" > $1) OR (\"status\" = $1 AND \
                  \"rank\" < $2) OR (\"status\" = $1 AND \"rank\" = $2 AND \"id\" < $3)) \
                  ORDER BY \"items\".\"status\" ASC, \"items\".\"rank\" DESC, \"items\".\"id\" DESC \
                  LIMIT 201"]
            );
            assert_eq!(
                format!("{:?}", plan.params),
                "[\"open\", 3, 0f8fad5b-d9cb-469f-a165-70867728950e]"
            );
        }

        #[test]
        fn a_keyset_listing_refuses_a_page_a_nullable_sort_and_a_cursor_of_another_sort() {
            let mut keyed = TableDefinition {
                pagination: Pagination::Keyset,
                ..table(false, None, true)
            };
            keyed.columns.push(ColumnDefinition {
                nullable: true,
                ..column("notes", ColumnType::Text, false)
            });
            let refused = |query: &[(&str, &str)], mode| match build(&keyed, query, mode, None) {
                Err(error) => error,
                Ok(_) => panic!("{query:?} was not refused"),
            };
            assert!(refused(&[("page", "2")], FilterMode::V1).contains("listed by cursor"));
            assert!(refused(&[("sort", "notes")], FilterMode::V1).contains("may be null"));
            assert!(refused(&[("per_page", "0")], FilterMode::V2).contains("per_page"));
            assert!(refused(&[("cursor", "!!!")], FilterMode::V1).contains("Invalid cursor"));
            let (plan, _) = build(&keyed, &[], FilterMode::V1, None).expect("plan");
            let Paging::Keyset { keys, .. } = &plan.paging else {
                panic!("keyset");
            };
            let cursor = keys
                .cursor_after(&serde_json::json!({
                    "created_at": "2026-09-28T10:30:00+00:00",
                    "id": "0f8fad5b-d9cb-469f-a165-70867728950e"
                }))
                .expect("cursor");
            assert!(
                refused(&[("cursor", &cursor), ("sort", "status")], FilterMode::V1)
                    .contains("of another sort")
            );
            let (_, statements) =
                build(&keyed, &[("cursor", &cursor)], FilterMode::V1, None).expect("plan");
            assert!(statements[0].contains("(\"created_at\", \"id\") < ($1, $2)"));
        }
    }
}
