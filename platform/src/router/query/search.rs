use super::SqlParams;
use crate::deployer::manifest::{SearchSpec, TableDefinition};
use crate::router::row_convert::manifest_ident;

pub const SEARCH_COLUMN: &str = "_search";

pub fn search_index_name(table: &str) -> String {
    format!("{table}__search_gin")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Search<'a> {
    Vector { column: String, config: &'a str },
    Pattern { columns: &'a [String] },
}

impl<'a> Search<'a> {
    pub(crate) fn of(table: &'a TableDefinition, adopted: bool) -> Option<Self> {
        let spec: &'a SearchSpec = table.search.as_ref()?;
        Some(match (&spec.vector_column, adopted) {
            (Some(column), _) => Search::Vector {
                column: column.clone(),
                config: &spec.config,
            },
            (None, false) => Search::Vector {
                column: SEARCH_COLUMN.to_string(),
                config: &spec.config,
            },
            (None, true) => Search::Pattern {
                columns: &spec.columns,
            },
        })
    }

    pub(crate) fn matching(&self, term: &str, params: &mut SqlParams) -> (String, Option<String>) {
        match self {
            Search::Vector { column, config } => {
                let placeholder = params.push(Box::new(term.to_string()));
                let query = format!("websearch_to_tsquery('{config}', {placeholder})");
                let column = manifest_ident(column);
                (
                    format!("{column} @@ {query}"),
                    Some(format!("ts_rank({column}, {query}) DESC")),
                )
            }
            Search::Pattern { columns } => {
                let placeholder = params.push(Box::new(format!("%{}%", escape_like(term))));
                let matches: Vec<String> = columns
                    .iter()
                    .map(|column| {
                        format!(
                            "{}::text ILIKE {placeholder} ESCAPE '\\'",
                            manifest_ident(column)
                        )
                    })
                    .collect();
                (format!("({})", matches.join(" OR ")), None)
            }
        }
    }
}

fn escape_like(term: &str) -> String {
    term.chars()
        .flat_map(|c| match c {
            '\\' | '%' | '_' => vec!['\\', c],
            c => vec![c],
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(spec: Option<SearchSpec>) -> TableDefinition {
        TableDefinition {
            name: "book".into(),
            search: spec,
            ..Default::default()
        }
    }

    fn declared(columns: &[&str], vector_column: Option<&str>) -> SearchSpec {
        SearchSpec {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            config: "english".into(),
            vector_column: vector_column.map(str::to_string),
        }
    }

    #[test]
    fn a_table_is_searched_as_it_declares() {
        assert_eq!(Search::of(&table(None), false), None);
        let kept = table(Some(declared(&["title", "body"], None)));
        assert_eq!(
            Search::of(&kept, false),
            Some(Search::Vector {
                column: "_search".into(),
                config: "english"
            })
        );
        assert_eq!(
            Search::of(&kept, true),
            Some(Search::Pattern {
                columns: &["title".to_string(), "body".to_string()]
            })
        );
        let own = table(Some(declared(&[], Some("document"))));
        assert_eq!(
            Search::of(&own, true),
            Some(Search::Vector {
                column: "document".into(),
                config: "english"
            })
        );
    }

    #[test]
    fn a_vector_search_matches_and_ranks() {
        let mut params = SqlParams::default();
        params.push(Box::new(1_i32));
        let (condition, order) = Search::Vector {
            column: SEARCH_COLUMN.into(),
            config: "simple",
        }
        .matching("brass lamp", &mut params);
        assert_eq!(
            condition,
            "\"_search\" @@ websearch_to_tsquery('simple', $2)"
        );
        assert_eq!(
            order.as_deref(),
            Some("ts_rank(\"_search\", websearch_to_tsquery('simple', $2)) DESC")
        );
        assert_eq!(format!("{params:?}"), "[1, \"brass lamp\"]");
    }

    #[test]
    fn a_pattern_search_matches_the_term_literally() {
        let mut params = SqlParams::default();
        let columns = ["title".to_string(), "body".to_string()];
        let (condition, order) =
            Search::Pattern { columns: &columns }.matching("50%_off\\", &mut params);
        assert_eq!(
            condition,
            "(\"title\"::text ILIKE $1 ESCAPE '\\' OR \"body\"::text ILIKE $1 ESCAPE '\\')"
        );
        assert_eq!(order, None);
        assert_eq!(format!("{params:?}"), "[\"%50\\\\%\\\\_off\\\\\\\\%\"]");
    }

    #[test]
    fn the_index_over_the_vector_is_named_for_its_table() {
        assert_eq!(search_index_name("book"), "book__search_gin");
    }
}
