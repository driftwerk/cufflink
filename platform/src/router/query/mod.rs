pub(crate) mod filter_v2;
pub(crate) mod keyset;
mod plan;
pub(crate) mod response;
pub(crate) mod search;

use plan::{explained_rows, page, Page, Paging, Total};
pub(crate) use plan::{CountMode, FilterMode, ListOptions, ListPlan};

use crate::router::bind::AsBase;
use tokio_postgres::types::ToSql;

#[derive(Default)]
pub(crate) struct SqlParams(Vec<AsBase>);

impl SqlParams {
    pub(crate) fn push(&mut self, value: Box<dyn ToSql + Sync + Send>) -> String {
        self.0.push(AsBase(value));
        format!("${}", self.0.len())
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn extend(&mut self, values: Vec<Box<dyn ToSql + Sync + Send>>) {
        self.0.extend(values.into_iter().map(AsBase));
    }

    pub(crate) fn as_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.0.iter().map(|p| p as &(dyn ToSql + Sync)).collect()
    }
}

impl From<Vec<Box<dyn ToSql + Sync + Send>>> for SqlParams {
    fn from(params: Vec<Box<dyn ToSql + Sync + Send>>) -> Self {
        Self(params.into_iter().map(AsBase).collect())
    }
}

impl std::fmt::Debug for SqlParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_number_parameters_in_push_order() {
        let mut params = SqlParams::from(vec![Box::new(1_i32) as Box<dyn ToSql + Sync + Send>]);
        assert_eq!(params.push(Box::new("a".to_string())), "$2");
        assert_eq!(params.push(Box::new(true)), "$3");
        assert_eq!(params.as_refs().len(), 3);
        assert_eq!(format!("{params:?}"), r#"[1, "a", true]"#);
    }
}
