use super::{explained_rows, page, CountMode, FilterMode, ListPlan, Page, Paging, Total};
use crate::deployer::manifest::TableDefinition;
use crate::errors::{CrudOp, PlatformError, PlatformResult};
use crate::router::crud::db;
use crate::router::filtering;
use crate::router::row_convert::row_to_json;
use crate::tenant_sessions::Statements;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Listing<'a> {
    pub table: &'a TableDefinition,
    pub mode: FilterMode,
    pub count: CountMode,
}

pub(crate) async fn list<C: Statements>(
    client: &C,
    plan: &ListPlan,
    listing: Listing<'_>,
) -> PlatformResult<Value> {
    match &plan.paging {
        Paging::Keyset { per_page, keys } => {
            let sql = plan.keyset_sql(*per_page);
            let read: Vec<tokio_postgres::Row> = client
                .query(&sql, &plan.params.as_refs())
                .await
                .map_err(db(CrudOp::Read))?;
            let has_more = read.len() as i64 > *per_page;
            let page = &read[..read.len().min(*per_page as usize)];
            let next_cursor = has_more.then(|| page.last()).flatten().and_then(|last| {
                keys.cursor_after(&row_to_json(last, &listing.table.columns, None))
            });
            let requested = filtering::requested_fields(&plan.requested);
            let results: Vec<Value> = page
                .iter()
                .map(|row| row_to_json(row, &listing.table.columns, requested))
                .collect();
            let mut pagination = json!({
                "per_page": per_page,
                "next_cursor": next_cursor,
                "has_more": has_more,
            });
            if let (Total::Exact(records) | Total::Estimated(records), Some(fields)) = (
                total(client, plan, listing.count).await?,
                pagination.as_object_mut(),
            ) {
                fields.insert("total_records".to_string(), json!(records));
            }
            Ok(json!({"results": results, "pagination": pagination}))
        }
        Paging::Cursor { limit, key } => {
            let results = rows(client, plan, &plan.cursor_sql(*limit), listing.table).await?;
            Ok(cursor_page(results, key, *limit))
        }
        Paging::Offset {
            page: requested,
            per_page,
        } => {
            let total = total(client, plan, listing.count).await?;
            let page = page(listing.mode, *requested, *per_page, total)?;
            let results = rows(client, plan, &plan.page_sql(&page), listing.table).await?;
            Ok(offset_page(&page, results))
        }
    }
}

async fn rows<C: Statements>(
    client: &C,
    plan: &ListPlan,
    sql: &str,
    table: &TableDefinition,
) -> PlatformResult<Vec<Value>> {
    let requested = filtering::requested_fields(&plan.requested);
    Ok(client
        .query(sql, &plan.params.as_refs())
        .await
        .map_err(db(CrudOp::Read))?
        .iter()
        .map(|row| row_to_json(row, &table.columns, requested))
        .collect())
}

async fn total<C: Statements>(
    client: &C,
    plan: &ListPlan,
    count: CountMode,
) -> PlatformResult<Total> {
    match count {
        CountMode::Unasked if matches!(plan.paging, Paging::Keyset { .. }) => Ok(Total::Uncounted),
        CountMode::Unasked | CountMode::Exact => client
            .query_one(&plan.count_sql(), &plan.params.as_refs())
            .await
            .map(|counted| Total::Exact(counted.get(0)))
            .map_err(db(CrudOp::Read)),
        CountMode::Estimate => estimate_total(client, plan).await.map(Total::Estimated),
        CountMode::None => Ok(Total::Uncounted),
    }
}

async fn estimate_total<C: Statements>(client: &C, plan: &ListPlan) -> PlatformResult<i64> {
    if let Some((sql, table)) = plan.reltuples_sql() {
        let analysed = client
            .query_opt(sql, &[&table])
            .await
            .map_err(db(CrudOp::Read))?
            .and_then(|row| row.get::<_, Option<i64>>(0))
            .filter(|rows| *rows >= 0);
        if let Some(rows) = analysed {
            return Ok(rows);
        }
    }
    let explained = client
        .query_one(&plan.explain_sql(), &plan.params.as_refs())
        .await
        .map_err(db(CrudOp::Read))?;
    explained_rows(&explained.get::<_, Value>(0))
        .ok_or_else(|| PlatformError::Internal("EXPLAIN returned no row estimate".into()))
}

fn cursor_page(results: Vec<Value>, key: &str, limit: i64) -> Value {
    let next_cursor = results
        .last()
        .and_then(|last| last.get(key))
        .and_then(Value::as_str)
        .map(|id| URL_SAFE_NO_PAD.encode(id));
    let has_more = results.len() as i64 == limit;
    json!({
        "results": results,
        "next_cursor": next_cursor,
        "has_more": has_more,
    })
}

fn offset_page(page: &Page, rows: Vec<Value>) -> Value {
    let (results, pagination) = page.finish(rows);
    json!({
        "results": results,
        "pagination": pagination,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn books(ids: &[&str]) -> Vec<Value> {
        ids.iter()
            .map(|id| json!({"id": id, "name": "book"}))
            .collect()
    }

    fn text(value: &Value) -> String {
        serde_json::to_string(value).expect("text")
    }

    #[test]
    fn a_full_cursor_page_names_the_cursor_of_its_last_row() {
        assert_eq!(
            text(&cursor_page(books(&["a1", "b2"]), "id", 2)),
            r#"{"has_more":true,"next_cursor":"YjI","results":[{"id":"a1","name":"book"},{"id":"b2","name":"book"}]}"#
        );
    }

    #[test]
    fn a_short_cursor_page_is_the_last() {
        assert_eq!(
            text(&cursor_page(books(&["a1"]), "id", 2)),
            r#"{"has_more":false,"next_cursor":"YTE","results":[{"id":"a1","name":"book"}]}"#
        );
        assert_eq!(
            text(&cursor_page(Vec::new(), "id", 2)),
            r#"{"has_more":false,"next_cursor":null,"results":[]}"#
        );
    }

    #[test]
    fn a_key_that_is_not_text_gives_no_cursor() {
        assert_eq!(
            text(&cursor_page(vec![json!({"id": 7})], "id", 1)),
            r#"{"has_more":true,"next_cursor":null,"results":[{"id":7}]}"#
        );
        assert_eq!(
            cursor_page(books(&["a1"]), "uuid", 1)["next_cursor"],
            Value::Null
        );
    }

    #[test]
    fn a_counted_page_describes_its_total() {
        let page = page(FilterMode::V1, Some(2), Some(2), Total::Exact(5)).expect("page");
        assert_eq!((page.limit, page.offset), (2, 2));
        assert_eq!(
            text(&offset_page(&page, books(&["c3", "d4"]))),
            r#"{"pagination":{"current_page":2,"per_page":2,"total_pages":3,"total_records":5},"results":[{"id":"c3","name":"book"},{"id":"d4","name":"book"}]}"#
        );
    }

    #[test]
    fn an_uncounted_page_drops_the_row_it_read_ahead() {
        let page = page(FilterMode::V2, Some(1), Some(2), Total::Uncounted).expect("page");
        let response = offset_page(&page, books(&["a1", "b2", "c3"]));
        assert_eq!(response["results"], json!(books(&["a1", "b2"])));
        assert_eq!(response["pagination"]["has_more"], true);
        let last = offset_page(&page, books(&["a1"]));
        assert_eq!(last["results"], json!(books(&["a1"])));
        assert_eq!(last["pagination"]["has_more"], false);
    }
}
