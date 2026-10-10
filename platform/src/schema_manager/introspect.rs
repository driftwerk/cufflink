use crate::deployer::manifest::ColumnType;
use crate::errors::PlatformResult;
use crate::tenant_sessions::{ServiceSessions, Statements};
use cufflink_types::manifest::validate::folded;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

const CATALOG_ONLY: &str = "SET LOCAL search_path TO pg_catalog";

const RELATIONS: &str = "\
SELECT c.relname::text, c.relkind::text, a.attname::text,
       format_type(a.atttypid, a.atttypmod),
       t.typtype::text,
       CASE WHEN t.typtype = 'd' THEN format_type(t.typbasetype, t.typtypmod) END,
       a.attnotnull, a.atthasdef, a.attgenerated <> ''
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  LEFT JOIN pg_type t ON t.oid = a.atttypid
 WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'v', 'm')
 ORDER BY c.relname, a.attnum";

const INDEXES: &str = "\
SELECT t.relname::text, i.relname::text, x.indisunique, x.indisprimary, x.indisvalid,
       am.amname::text, x.indpred IS NOT NULL,
       ARRAY(SELECT pg_get_indexdef(x.indexrelid, k, true)
               FROM generate_series(1, x.indnkeyatts) AS k ORDER BY k)::text[],
       pg_get_indexdef(x.indexrelid)
  FROM pg_index x
  JOIN pg_class i ON i.oid = x.indexrelid
  JOIN pg_class t ON t.oid = x.indrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
  JOIN pg_am am ON am.oid = i.relam
 WHERE n.nspname = $1
 ORDER BY t.relname, i.relname";

const CONSTRAINTS: &str = "\
SELECT t.relname::text, c.conname::text, c.contype::text, pg_get_constraintdef(c.oid),
       ARRAY(SELECT a.attname::text
               FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, position)
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
              ORDER BY k.position)::text[]
  FROM pg_constraint c
  JOIN pg_class t ON t.oid = c.conrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
 WHERE n.nspname = $1
 ORDER BY t.relname, c.conname";

const TRIGGERS: &str = "\
SELECT t.relname::text, g.tgname::text
  FROM pg_trigger g
  JOIN pg_class t ON t.oid = g.tgrelid
  JOIN pg_namespace n ON n.oid = t.relnamespace
 WHERE n.nspname = $1 AND NOT g.tgisinternal
 ORDER BY t.relname, g.tgname";

const ARCHIVED: &str = "\
SELECT c.relname::text, NULL::text, obj_description(c.oid, 'pg_class')
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND starts_with(c.relname, $2)
UNION ALL
SELECT c.relname::text, a.attname::text, col_description(c.oid, a.attnum)
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
   AND starts_with(a.attname, $2)";

const DEPENDENTS: &str = "\
WITH target AS (
    SELECT c.oid AS relid, c.relname::text AS relation, t.column_name,
           COALESCE(a.attnum, 0)::int4 AS attnum
      FROM unnest($2::text[], $3::text[]) AS t(relation, column_name)
      JOIN pg_class c ON c.relname = t.relation AND c.relkind IN ('r', 'p')
      JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = $1
      LEFT JOIN pg_attribute a
        ON a.attrelid = c.oid AND a.attname = t.column_name AND NOT a.attisdropped
     WHERE t.column_name = '' OR a.attnum IS NOT NULL
)
SELECT DISTINCT target.relation, target.column_name,
       d.classid::regclass::text,
       pg_describe_object(d.classid, d.objid, d.objsubid),
       COALESCE(
           (SELECT v.relkind::text FROM pg_rewrite r JOIN pg_class v ON v.oid = r.ev_class
             WHERE d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid),
           (SELECT k.relkind::text FROM pg_class k
             WHERE d.classid = 'pg_class'::regclass AND k.oid = d.objid)),
       COALESCE(
           (SELECT r.ev_class::regclass::text FROM pg_rewrite r
             WHERE d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid
               AND r.ev_class <> target.relid),
           (SELECT r.rulename::text FROM pg_rewrite r
             WHERE d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid),
           (SELECT g.tgname::text FROM pg_trigger g
             WHERE d.classid = 'pg_trigger'::regclass AND g.oid = d.objid),
           (SELECT p.polname::text FROM pg_policy p
             WHERE d.classid = 'pg_policy'::regclass AND p.oid = d.objid),
           (SELECT o.conname::text FROM pg_constraint o
             WHERE d.classid = 'pg_constraint'::regclass AND o.oid = d.objid),
           (SELECT a.attname::text FROM pg_attrdef f
              JOIN pg_attribute a ON a.attrelid = f.adrelid AND a.attnum = f.adnum
             WHERE d.classid = 'pg_attrdef'::regclass AND f.oid = d.objid),
           (SELECT k.relname::text FROM pg_class k
             WHERE d.classid = 'pg_class'::regclass AND k.oid = d.objid),
           pg_describe_object(d.classid, d.objid, d.objsubid)),
       COALESCE(
           (SELECT r.ev_class = target.relid FROM pg_rewrite r
             WHERE d.classid = 'pg_rewrite'::regclass AND r.oid = d.objid),
           (SELECT g.tgrelid = target.relid FROM pg_trigger g
             WHERE d.classid = 'pg_trigger'::regclass AND g.oid = d.objid),
           (SELECT p.polrelid = target.relid FROM pg_policy p
             WHERE d.classid = 'pg_policy'::regclass AND p.oid = d.objid),
           (SELECT o.conrelid = target.relid FROM pg_constraint o
             WHERE d.classid = 'pg_constraint'::regclass AND o.oid = d.objid),
           (SELECT x.indrelid = target.relid FROM pg_index x
             WHERE d.classid = 'pg_class'::regclass AND x.indexrelid = d.objid),
           (SELECT f.adrelid = target.relid FROM pg_attrdef f
             WHERE d.classid = 'pg_attrdef'::regclass AND f.oid = d.objid),
           false)
  FROM target
  JOIN pg_depend d
    ON d.refclassid = 'pg_class'::regclass AND d.refobjid = target.relid
   AND (target.attnum = 0 OR d.refobjsubid = target.attnum)
   AND d.deptype IN ('n', 'a')
 WHERE NOT (d.classid = 'pg_class'::regclass AND d.objid = target.relid)
   AND NOT EXISTS (
       SELECT 1 FROM pg_attrdef f
        WHERE d.classid = 'pg_attrdef'::regclass AND f.oid = d.objid
          AND f.adrelid = target.relid
          AND (target.attnum = 0 OR f.adnum = target.attnum))
   AND NOT EXISTS (
       SELECT 1 FROM pg_constraint o
        WHERE d.classid = 'pg_constraint'::regclass AND o.oid = d.objid AND o.contype = 'n')
 ORDER BY 1, 2, 3, 4";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    Table,
    View,
    MaterializedView,
}

impl RelationKind {
    fn of(relkind: &str) -> Option<Self> {
        match relkind {
            "r" | "p" => Some(Self::Table),
            "v" => Some(Self::View),
            "m" => Some(Self::MaterializedView),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeKind {
    Base,
    Enum,
    Domain,
    Other,
}

impl TypeKind {
    fn of(typtype: &str) -> Self {
        match typtype {
            "b" => Self::Base,
            "e" => Self::Enum,
            "d" => Self::Domain,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveColumn {
    pub name: String,
    pub type_name: String,
    pub kind: TypeKind,
    pub domain_base: Option<String>,
    pub not_null: bool,
    pub has_default: bool,
    pub generated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypeName<'a> {
    before: &'a str,
    modifier: Option<&'a str>,
    after: &'a str,
    array: bool,
}

impl<'a> TypeName<'a> {
    fn parse(name: &'a str) -> Self {
        let (element, array) = match name.strip_suffix("[]") {
            Some(element) => (element, true),
            None => (name, false),
        };
        let parts = element
            .split_once('(')
            .and_then(|(before, rest)| rest.split_once(')').map(|(m, after)| (before, m, after)));
        match parts {
            Some((before, modifier, after)) => Self {
                before: before.trim_end(),
                modifier: Some(modifier),
                after,
                array,
            },
            None => Self {
                before: element,
                modifier: None,
                after: "",
                array,
            },
        }
    }

    fn spelling(&self) -> String {
        let unqualified = self
            .before
            .rsplit_once('.')
            .map_or(self.before, |(_, name)| name);
        format!(
            "{unqualified}{}{}",
            self.after,
            if self.array { "[]" } else { "" }
        )
    }

    fn modifiers(&self) -> Vec<&'a str> {
        self.modifier
            .map(|m| m.split(',').map(str::trim).collect())
            .unwrap_or_default()
    }
}

fn spelled(column_type: &ColumnType, name: &TypeName<'_>) -> bool {
    column_type
        .pg_type_names()
        .contains(&name.spelling().as_str())
}

fn declared_as(column_type: &ColumnType, name: &TypeName<'_>) -> bool {
    let modifiers = name.modifiers();
    spelled(column_type, name)
        && match column_type {
            ColumnType::Varchar { length } => modifiers == [length.to_string()],
            ColumnType::Numeric {
                precision: Some(precision),
                scale,
            } => modifiers == [precision.to_string(), scale.unwrap_or(0).to_string()],
            _ => true,
        }
}

impl LiveColumn {
    pub fn holds(&self, column_type: &ColumnType) -> bool {
        let text = *column_type == ColumnType::Text;
        match self.kind {
            TypeKind::Enum => text,
            TypeKind::Domain => self
                .domain_base
                .as_deref()
                .is_some_and(|base| declared_as(column_type, &TypeName::parse(base))),
            TypeKind::Base | TypeKind::Other => {
                declared_as(column_type, &TypeName::parse(&self.type_name))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIndex {
    pub name: String,
    pub unique: bool,
    pub primary: bool,
    pub valid: bool,
    pub method: String,
    pub partial: bool,
    pub columns: Vec<String>,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveConstraint {
    pub name: String,
    pub kind: String,
    pub columns: Vec<String>,
    pub definition: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveRelation {
    pub kind: RelationKind,
    pub columns: Vec<LiveColumn>,
    pub indexes: Vec<LiveIndex>,
    pub constraints: Vec<LiveConstraint>,
    pub triggers: Vec<String>,
}

impl LiveRelation {
    pub fn of(kind: RelationKind) -> Self {
        Self {
            kind,
            columns: Vec::new(),
            indexes: Vec::new(),
            constraints: Vec::new(),
            triggers: Vec::new(),
        }
    }

    pub fn column(&self, name: &str) -> Option<&LiveColumn> {
        let name = folded(name);
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn index(&self, name: &str) -> Option<&LiveIndex> {
        let name = folded(name);
        self.indexes.iter().find(|i| i.name == name)
    }

    pub fn constraint(&self, name: &str) -> Option<&LiveConstraint> {
        let name = folded(name);
        self.constraints.iter().find(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSchema {
    pub exists: bool,
    pub relations: BTreeMap<String, LiveRelation>,
}

impl LiveSchema {
    pub fn relation(&self, name: &str) -> Option<&LiveRelation> {
        self.relations.get(&folded(name))
    }

    pub fn table(&self, name: &str) -> Option<&LiveRelation> {
        self.relation(name)
            .filter(|relation| relation.kind == RelationKind::Table)
    }

    pub fn index(&self, name: &str) -> Option<(&str, &LiveIndex)> {
        self.relations
            .iter()
            .find_map(|(table, relation)| relation.index(name).map(|index| (table.as_str(), index)))
    }
}

pub async fn live_schema(db: &ServiceSessions) -> PlatformResult<LiveSchema> {
    let schema = folded(db.schema());
    let mut session = db.get().await?;
    let tx = session.read_only_transaction().await?;
    tx.batch_execute(CATALOG_ONLY).await?;
    let exists: bool = tx
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&schema],
        )
        .await?
        .get(0);
    let mut relations: BTreeMap<String, LiveRelation> = BTreeMap::new();
    for row in tx.query(RELATIONS, &[&schema]).await? {
        let Some(kind) = RelationKind::of(row.get(1)) else {
            continue;
        };
        let relation = relations
            .entry(row.get(0))
            .or_insert_with(|| LiveRelation::of(kind));
        if let Some(name) = row.get::<_, Option<String>>(2) {
            relation.columns.push(LiveColumn {
                name,
                type_name: row.get(3),
                kind: TypeKind::of(row.get(4)),
                domain_base: row.get(5),
                not_null: row.get(6),
                has_default: row.get(7),
                generated: row.get(8),
            });
        }
    }
    for row in tx.query(INDEXES, &[&schema]).await? {
        if let Some(relation) = relations.get_mut(row.get::<_, &str>(0)) {
            relation.indexes.push(LiveIndex {
                name: row.get(1),
                unique: row.get(2),
                primary: row.get(3),
                valid: row.get(4),
                method: row.get(5),
                partial: row.get(6),
                columns: row.get(7),
                definition: row.get(8),
            });
        }
    }
    for row in tx.query(CONSTRAINTS, &[&schema]).await? {
        if let Some(relation) = relations.get_mut(row.get::<_, &str>(0)) {
            relation.constraints.push(LiveConstraint {
                name: row.get(1),
                kind: row.get(2),
                definition: row.get(3),
                columns: row.get(4),
            });
        }
    }
    for row in tx.query(TRIGGERS, &[&schema]).await? {
        if let Some(relation) = relations.get_mut(row.get::<_, &str>(0)) {
            relation.triggers.push(row.get(1));
        }
    }
    tx.rollback().await?;
    Ok(LiveSchema { exists, relations })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Archived {
    pub table: Option<String>,
    pub archive_name: String,
    pub version: i32,
    pub original: String,
}

pub const ARCHIVE_PREFIX: &str = "_archived_v";
pub const ARCHIVE_COMMENT: &str = "cufflink:archived:";

impl Archived {
    pub fn parse(relation: &str, column: Option<&str>, comment: Option<&str>) -> Option<Self> {
        let archive_name = column.unwrap_or(relation);
        let (version, named) = archive_name.strip_prefix(ARCHIVE_PREFIX)?.split_once('_')?;
        Some(Self {
            table: column.map(|_| relation.to_string()),
            archive_name: archive_name.to_string(),
            version: version.parse().ok()?,
            original: comment
                .and_then(|comment| comment.strip_prefix(ARCHIVE_COMMENT))
                .unwrap_or(named)
                .to_string(),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Archives(Vec<Archived>);

impl Archives {
    pub fn new(archived: Vec<Archived>) -> Self {
        Self(archived)
    }

    pub fn latest(&self, table: Option<&str>, name: &str) -> Option<&Archived> {
        let (table, name) = (table.map(folded), folded(name));
        self.0
            .iter()
            .filter(|a| a.table == table && a.original == name)
            .max_by_key(|a| a.version)
    }
}

pub async fn archives(db: &ServiceSessions) -> PlatformResult<Archives> {
    Ok(Archives(
        db.get()
            .await?
            .query(ARCHIVED, &[&folded(db.schema()), &ARCHIVE_PREFIX])
            .await?
            .iter()
            .filter_map(|row| {
                Archived::parse(
                    row.get(0),
                    row.get::<_, Option<&str>>(1),
                    row.get::<_, Option<&str>>(2),
                )
            })
            .collect(),
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DropTarget {
    Table(String),
    Column { table: String, column: String },
}

impl DropTarget {
    pub fn table(&self) -> &str {
        match self {
            DropTarget::Table(table) | DropTarget::Column { table, .. } => table,
        }
    }

    fn column(&self) -> &str {
        match self {
            DropTarget::Table(_) => "",
            DropTarget::Column { column, .. } => column,
        }
    }

    pub fn named_in(&self, schema: &str) -> String {
        match self {
            DropTarget::Table(table) => format!("{schema}.{table}"),
            DropTarget::Column { table, column } => format!("{schema}.{table}.{column}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependentKind {
    View,
    MaterializedView,
    Rule,
    Trigger,
    Policy,
    Index,
    Constraint,
    Sequence,
    GeneratedColumn,
    Other,
}

impl DependentKind {
    fn of(catalog: &str, relkind: Option<&str>, on_target: bool) -> Self {
        match (catalog, relkind) {
            ("pg_rewrite", Some("m")) if !on_target => Self::MaterializedView,
            ("pg_rewrite", _) if !on_target => Self::View,
            ("pg_rewrite", _) => Self::Rule,
            ("pg_trigger", _) => Self::Trigger,
            ("pg_policy", _) => Self::Policy,
            ("pg_constraint", _) => Self::Constraint,
            ("pg_attrdef", _) => Self::GeneratedColumn,
            ("pg_class", Some("i" | "I")) => Self::Index,
            ("pg_class", Some("S")) => Self::Sequence,
            _ => Self::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::MaterializedView => "materialized view",
            Self::Rule => "rule",
            Self::Trigger => "trigger",
            Self::Policy => "policy",
            Self::Index => "index",
            Self::Constraint => "constraint",
            Self::Sequence => "sequence",
            Self::GeneratedColumn => "generated column",
            Self::Other => "object",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Found {
    pub target: DropTarget,
    pub kind: DependentKind,
    pub name: String,
    pub on_target: bool,
    pub description: String,
}

pub async fn dependents(
    db: &ServiceSessions,
    targets: &BTreeSet<DropTarget>,
) -> PlatformResult<Vec<Found>> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }
    let (tables, columns): (Vec<String>, Vec<String>) = targets
        .iter()
        .map(|target| (folded(target.table()), folded(target.column())))
        .unzip();
    let mut session = db.get().await?;
    let tx = session.read_only_transaction().await?;
    tx.batch_execute(CATALOG_ONLY).await?;
    let found = tx
        .query(DEPENDENTS, &[&folded(db.schema()), &tables, &columns])
        .await?
        .iter()
        .map(|row| {
            let (table, column): (String, String) = (row.get(0), row.get(1));
            let on_target: bool = row.get(6);
            Found {
                target: if column.is_empty() {
                    DropTarget::Table(table)
                } else {
                    DropTarget::Column { table, column }
                },
                kind: DependentKind::of(row.get(2), row.get::<_, Option<&str>>(4), on_target),
                name: row.get(5),
                on_target,
                description: row.get(3),
            }
        })
        .collect::<BTreeSet<_>>();
    tx.rollback().await?;
    Ok(found.into_iter().collect())
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    pub(crate) fn column(name: &str, type_name: &str) -> LiveColumn {
        LiveColumn {
            name: name.into(),
            type_name: type_name.into(),
            kind: TypeKind::Base,
            domain_base: None,
            not_null: true,
            has_default: false,
            generated: false,
        }
    }

    pub(crate) fn index(name: &str, columns: &[&str]) -> LiveIndex {
        LiveIndex {
            name: name.into(),
            unique: false,
            primary: false,
            valid: true,
            method: "btree".into(),
            partial: false,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            definition: String::new(),
        }
    }

    pub(crate) fn primary_key(table: &str, columns: &[&str]) -> LiveConstraint {
        LiveConstraint {
            name: format!("{table}_pkey"),
            kind: "p".into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            definition: format!("PRIMARY KEY ({})", columns.join(", ")),
        }
    }

    pub(crate) fn table(columns: Vec<LiveColumn>) -> LiveRelation {
        LiveRelation {
            columns,
            ..LiveRelation::of(RelationKind::Table)
        }
    }

    pub(crate) fn schema(tables: Vec<(&str, LiveRelation)>) -> LiveSchema {
        LiveSchema {
            exists: true,
            relations: tables
                .into_iter()
                .map(|(name, relation)| (name.to_string(), relation))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::column;
    use super::*;

    fn holds(column_type: ColumnType, type_name: &str) -> bool {
        column("c", type_name).holds(&column_type)
    }

    #[test]
    fn a_type_name_is_taken_apart_wherever_its_modifier_stands() {
        let spelling = |name| TypeName::parse(name).spelling();
        assert_eq!(spelling("text"), "text");
        assert_eq!(spelling("character varying(50)"), "character varying");
        assert_eq!(spelling("character varying(50)[]"), "character varying[]");
        assert_eq!(
            spelling("timestamp(3) with time zone"),
            "timestamp with time zone"
        );
        assert_eq!(spelling("public.citext"), "citext");
        assert_eq!(spelling("numeric(12,2)"), "numeric");
        assert_eq!(TypeName::parse("numeric(12,2)").modifiers(), ["12", "2"]);
        assert!(TypeName::parse("uuid").modifiers().is_empty());
    }

    #[test]
    fn every_declared_type_is_held_by_the_type_it_makes() {
        for column_type in [
            ColumnType::Uuid,
            ColumnType::Text,
            ColumnType::Integer,
            ColumnType::BigInteger,
            ColumnType::Float,
            ColumnType::Double,
            ColumnType::Boolean,
            ColumnType::Timestamp,
            ColumnType::Date,
            ColumnType::Jsonb,
            ColumnType::TimestampNaive,
            ColumnType::Time,
            ColumnType::Bytea,
            ColumnType::TextArray,
            ColumnType::UuidArray,
            ColumnType::IntegerArray,
            ColumnType::BigIntegerArray,
            ColumnType::JsonbArray,
        ] {
            let live = column_type.pg_type_names()[0];
            assert!(holds(column_type.clone(), live), "{column_type:?} {live}");
            assert!(!holds(column_type.clone(), "point"), "{column_type:?}");
        }
    }

    #[test]
    fn text_is_declared_over_every_text_type() {
        for live in [
            "text",
            "character varying",
            "character varying(50)",
            "character(3)",
            "public.citext",
        ] {
            assert!(holds(ColumnType::Text, live), "{live}");
        }
        assert!(!holds(ColumnType::Text, "uuid"));
        assert!(!holds(ColumnType::Text, "text[]"));
        assert!(holds(ColumnType::TextArray, "character varying(20)[]"));
    }

    #[test]
    fn a_declared_length_or_precision_is_the_columns_exactly() {
        let varchar = ColumnType::Varchar { length: 50 };
        assert!(holds(varchar.clone(), "character varying(50)"));
        assert!(!holds(varchar.clone(), "character varying(51)"));
        assert!(!holds(varchar, "character varying"));
        let money = ColumnType::Numeric {
            precision: Some(12),
            scale: Some(2),
        };
        assert!(holds(money.clone(), "numeric(12,2)"));
        assert!(!holds(money.clone(), "numeric(12,4)"));
        assert!(!holds(money, "numeric"));
        let whole = ColumnType::Numeric {
            precision: Some(12),
            scale: None,
        };
        assert!(holds(whole, "numeric(12,0)"));
        let any = ColumnType::Numeric {
            precision: None,
            scale: None,
        };
        assert!(holds(any.clone(), "numeric"));
        assert!(holds(any, "numeric(12,2)"));
    }

    #[test]
    fn text_is_declared_over_an_enum_and_over_a_domain_of_text() {
        let of = |kind, domain_base: Option<&str>| LiveColumn {
            kind,
            domain_base: domain_base.map(str::to_string),
            ..column("kind", "catalog_service.book_kind")
        };
        assert!(of(TypeKind::Enum, None).holds(&ColumnType::Text));
        assert!(!of(TypeKind::Enum, None).holds(&ColumnType::Uuid));
        assert!(of(TypeKind::Domain, Some("character varying(20)")).holds(&ColumnType::Text));
        assert!(of(TypeKind::Domain, Some("integer")).holds(&ColumnType::Integer));
        assert!(!of(TypeKind::Domain, Some("integer")).holds(&ColumnType::Text));
        assert!(!of(TypeKind::Other, None).holds(&ColumnType::Text));
    }

    #[test]
    fn an_archive_says_what_it_was_by_its_name_or_its_comment() {
        assert_eq!(
            Archived::parse("_archived_v3_orders", None, None),
            Some(Archived {
                table: None,
                archive_name: "_archived_v3_orders".into(),
                version: 3,
                original: "orders".into(),
            })
        );
        assert_eq!(
            Archived::parse(
                "users",
                Some("_archived_v12_a_long_na_1f2e3d"),
                Some("cufflink:archived:a_long_name")
            ),
            Some(Archived {
                table: Some("users".into()),
                archive_name: "_archived_v12_a_long_na_1f2e3d".into(),
                version: 12,
                original: "a_long_name".into(),
            })
        );
        assert_eq!(Archived::parse("_archived_vfoo_title", None, None), None);
        assert_eq!(Archived::parse("_archived_v3", None, None), None);
        assert_eq!(Archived::parse("orders", None, None), None);
        assert_eq!(
            Archived::parse("_archived_v3_orders", None, Some("kept for audit"))
                .map(|a| a.original),
            Some("orders".into())
        );
    }

    #[test]
    fn the_latest_archive_of_a_name_is_the_one_restored() {
        let archives = Archives::new(
            [
                ("users", Some("_archived_v2_title")),
                ("users", Some("_archived_v5_title")),
                ("users", Some("_archived_v3_sub_title")),
                ("posts", Some("_archived_v9_title")),
                ("_archived_v4_title", None),
            ]
            .into_iter()
            .filter_map(|(relation, column)| Archived::parse(relation, column, None))
            .collect(),
        );
        let latest = |table, name| {
            archives
                .latest(table, name)
                .map(|a| a.archive_name.as_str())
        };
        assert_eq!(latest(Some("users"), "title"), Some("_archived_v5_title"));
        assert_eq!(latest(Some("Users"), "Title"), Some("_archived_v5_title"));
        assert_eq!(
            latest(Some("users"), "sub_title"),
            Some("_archived_v3_sub_title")
        );
        assert_eq!(latest(None, "title"), Some("_archived_v4_title"));
        assert_eq!(latest(Some("users"), "body"), None);
    }

    #[test]
    fn a_dependent_is_named_for_what_it_is() {
        let kind = DependentKind::of;
        assert_eq!(kind("pg_rewrite", Some("v"), false), DependentKind::View);
        assert_eq!(
            kind("pg_rewrite", Some("m"), false),
            DependentKind::MaterializedView
        );
        assert_eq!(kind("pg_rewrite", Some("r"), true), DependentKind::Rule);
        assert_eq!(kind("pg_trigger", None, true), DependentKind::Trigger);
        assert_eq!(kind("pg_policy", None, true), DependentKind::Policy);
        assert_eq!(
            kind("pg_constraint", None, false),
            DependentKind::Constraint
        );
        assert_eq!(kind("pg_class", Some("i"), true), DependentKind::Index);
        assert_eq!(kind("pg_class", Some("S"), false), DependentKind::Sequence);
        assert_eq!(
            kind("pg_attrdef", None, true),
            DependentKind::GeneratedColumn
        );
        assert_eq!(kind("pg_proc", None, false), DependentKind::Other);
    }

    #[test]
    fn a_target_is_named_in_its_schema() {
        assert_eq!(
            DropTarget::Table("book".into()).named_in("catalog_service"),
            "catalog_service.book"
        );
        assert_eq!(
            DropTarget::Column {
                table: "book".into(),
                column: "status".into()
            }
            .named_in("catalog_service"),
            "catalog_service.book.status"
        );
    }
}
