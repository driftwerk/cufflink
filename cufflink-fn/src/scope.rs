//! The rows a `scoped("area", "op")` route's caller may reach, resolved by
//! the platform from their grants (`@all`, `@own`, `@field(column=claim)`).
//! With `db-v2`, [`Scope::to_sql`] renders it as a condition to add to a
//! query, so a handler never writes the predicate itself.

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum Scope {
    /// Every row.
    All,
    /// Rows satisfying at least one predicate.
    Rows(Vec<Predicate>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `column` holds the caller's subject.
    Owner { column: String, subject: String },
    /// `column` holds one of `values`; none means no row.
    Claim { column: String, values: Vec<Value> },
    /// The first predicate and at least one of the rest.
    And(Box<Predicate>, Vec<Predicate>),
}

impl Scope {
    /// The platform's `scope` JSON; `None` for anything else, including a
    /// denied scope, which never reaches a guest.
    pub(crate) fn from_json(value: &Value) -> Option<Self> {
        match value["kind"].as_str()? {
            "all" => Some(Scope::All),
            "rows" => value["predicates"]
                .as_array()?
                .iter()
                .map(Predicate::from_json)
                .collect::<Option<Vec<_>>>()
                .map(Scope::Rows),
            _ => None,
        }
    }
}

impl Predicate {
    fn from_json(value: &Value) -> Option<Self> {
        let text = |v: &Value, key: &str| v[key].as_str().map(str::to_string);
        if let Some(owner) = value.get("owner") {
            return Some(Predicate::Owner {
                column: text(owner, "column")?,
                subject: text(owner, "subject")?,
            });
        }
        if let Some(claim) = value.get("claim") {
            return Some(Predicate::Claim {
                column: text(claim, "column")?,
                values: claim["values"].as_array()?.clone(),
            });
        }
        let [first, rest] = value.get("and")?.as_array()?.as_slice() else {
            return None;
        };
        Some(Predicate::And(
            Box::new(Predicate::from_json(first)?),
            rest["or"]
                .as_array()?
                .iter()
                .map(Predicate::from_json)
                .collect::<Option<Vec<_>>>()?,
        ))
    }
}

#[cfg(feature = "db-v2")]
mod sql {
    use super::{Predicate, Scope};
    use crate::sql::Param;
    use serde_json::Value;

    fn column(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn bind(params: &mut Vec<Param>, param: Param) -> String {
        params.push(param);
        format!("${}", params.len())
    }

    /// Integers bind as an integer array, anything else as text, which the
    /// platform coerces to the column's type; the platform has already
    /// dropped values that are not of the column's type where it knows it.
    fn values(values: &[Value]) -> Param {
        let integers: Option<Vec<Option<i64>>> =
            values.iter().map(|v| v.as_i64().map(Some)).collect();
        integers.map_or_else(
            || {
                Param::TextArray(
                    values
                        .iter()
                        .map(|v| Some(v.as_str().map_or_else(|| v.to_string(), str::to_string)))
                        .collect(),
                )
            },
            Param::I64Array,
        )
    }

    impl Predicate {
        fn to_sql(&self, params: &mut Vec<Param>) -> String {
            match self {
                Predicate::Owner {
                    column: name,
                    subject,
                } => {
                    format!(
                        "{} = {}",
                        column(name),
                        bind(params, Param::Text(subject.clone()))
                    )
                }
                Predicate::Claim { values: v, .. } if v.is_empty() => "FALSE".to_string(),
                Predicate::Claim {
                    column: name,
                    values: v,
                } => {
                    format!("{} = ANY({})", column(name), bind(params, values(v)))
                }
                Predicate::And(first, rest) => {
                    format!("({} AND {})", first.to_sql(params), any(rest, params))
                }
            }
        }
    }

    /// At least one of `predicates`; none is no row.
    fn any(predicates: &[Predicate], params: &mut Vec<Param>) -> String {
        if predicates.is_empty() {
            return "FALSE".to_string();
        }
        let sql: Vec<String> = predicates.iter().map(|p| p.to_sql(params)).collect();
        format!("({})", sql.join(" OR "))
    }

    impl Scope {
        /// The scope as a SQL condition, its values appended to `params` and
        /// referred to as `$n` after the ones already there:
        ///
        /// ```rust,ignore
        /// let mut params = vec![Param::from(status)];
        /// let condition = scope.to_sql(&mut params);
        /// sql::query(&format!("SELECT * FROM assets WHERE status = $1 AND {condition}"), &params)
        /// ```
        pub fn to_sql(&self, params: &mut Vec<Param>) -> String {
            match self {
                Scope::All => "TRUE".to_string(),
                Scope::Rows(predicates) => any(predicates, params),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use serde_json::json;

        #[test]
        fn scopes_render_the_platforms_sql_after_the_callers_params() {
            let scope = Scope::from_json(&json!({"kind": "rows", "predicates": [
                {"owner": {"column": "customer_id", "subject": "u-1"}},
                {"claim": {"column": "branch_id", "values": ["o-1", "o-2"]}},
                {"claim": {"column": "rank", "values": [1, 2]}},
                {"claim": {"column": "region", "values": []}},
                {"and": [{"owner": {"column": "user_id", "subject": "u-1"}},
                    {"or": [{"claim": {"column": "team\"x", "values": ["t"]}}]}]},
            ]}))
            .expect("scope");
            let mut params = vec![Param::Text("open".into())];
            assert_eq!(
                scope.to_sql(&mut params),
                "(\"customer_id\" = $2 OR \"branch_id\" = ANY($3) OR \"rank\" = ANY($4) \
                 OR FALSE OR (\"user_id\" = $5 AND (\"team\"\"x\" = ANY($6))))"
            );
            assert_eq!(
                params,
                vec![
                    Param::Text("open".into()),
                    Param::Text("u-1".into()),
                    Param::TextArray(vec![Some("o-1".into()), Some("o-2".into())]),
                    Param::I64Array(vec![Some(1), Some(2)]),
                    Param::Text("u-1".into()),
                    Param::TextArray(vec![Some("t".into())]),
                ]
            );
            let mut none = Vec::new();
            assert_eq!(Scope::All.to_sql(&mut none), "TRUE");
            assert_eq!(Scope::Rows(Vec::new()).to_sql(&mut none), "FALSE");
            let owner = || Predicate::Owner {
                column: "c".into(),
                subject: "s".into(),
            };
            assert_eq!(
                Scope::Rows(vec![Predicate::And(Box::new(owner()), Vec::new())]).to_sql(&mut none),
                "((\"c\" = $1 AND FALSE))"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_platforms_scope_json_parses() {
        assert_eq!(Scope::from_json(&json!({"kind": "all"})), Some(Scope::All));
        assert_eq!(
            Scope::from_json(&json!({"kind": "rows", "predicates": [
                {"and": [{"owner": {"column": "c", "subject": "s"}},
                    {"or": [{"claim": {"column": "o", "values": ["x"]}}]}]}
            ]})),
            Some(Scope::Rows(vec![Predicate::And(
                Box::new(Predicate::Owner {
                    column: "c".into(),
                    subject: "s".into()
                }),
                vec![Predicate::Claim {
                    column: "o".into(),
                    values: vec![json!("x")]
                }],
            )]))
        );
        for invalid in [
            json!({"kind": "denied"}),
            json!(null),
            json!({"kind": "rows", "predicates": [{"owner": {"column": "c"}}]}),
            json!({"kind": "rows", "predicates": [{"and": []}]}),
        ] {
            assert_eq!(Scope::from_json(&invalid), None, "{invalid}");
        }
    }
}
