//! The `#[table(...)]` keys and `#[column(...)]` attribute added with
//! column types v2: what a table declares beyond its columns. Their values
//! are checked here, with the rules the platform applies at deploy.

use crate::keywords::{assigned, assigned_strings, once};
use cufflink_types::manifest::validate::{
    validate_audit_redact, validate_check_constraint, validate_column_type, validate_index,
    validate_search_spec,
};
use cufflink_types::{
    CheckConstraint, ColumnAccess, ColumnType, IndexDefinition, IndexMethod, Pagination, SearchSpec,
};
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::ext::IdentExt;
use syn::parse::ParseStream;
use syn::spanned::Spanned;
use syn::{Expr, ExprLit, Ident, Lit, LitBool, LitInt, LitStr, Meta, Token};

pub(crate) struct Declarations {
    pub(crate) managed: bool,
    pub(crate) sensitive: bool,
    pub(crate) pagination: Pagination,
    pub(crate) audit_redact: Vec<String>,
    pub(crate) checks: Vec<CheckConstraint>,
    pub(crate) indexes: Vec<(IndexDefinition, Span)>,
    search_columns: Option<(Vec<String>, Span)>,
    search_config: Option<String>,
    search_vector: Option<(String, Span)>,
}

impl Default for Declarations {
    fn default() -> Self {
        Self {
            managed: true,
            sensitive: false,
            pagination: Pagination::Offset,
            audit_redact: Vec::new(),
            checks: Vec::new(),
            indexes: Vec::new(),
            search_columns: None,
            search_config: None,
            search_vector: None,
        }
    }
}

fn string(value: &Expr, key: &str) -> syn::Result<String> {
    match value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) => Ok(text.value()),
        other => Err(syn::Error::new(
            other.span(),
            format!("{key} takes a string literal"),
        )),
    }
}

fn boolean(value: &Expr, key: &str) -> syn::Result<bool> {
    match value {
        Expr::Lit(ExprLit {
            lit: Lit::Bool(flag),
            ..
        }) => Ok(flag.value),
        other => Err(syn::Error::new(
            other.span(),
            format!("{key} takes true or false"),
        )),
    }
}

fn strings(value: &Expr, key: &str) -> syn::Result<Vec<String>> {
    match value {
        Expr::Array(array) => array.elems.iter().map(|item| string(item, key)).collect(),
        other => Err(syn::Error::new(
            other.span(),
            format!("{key} takes a list of string literals"),
        )),
    }
}

fn pagination(value: &Expr) -> syn::Result<Pagination> {
    let named = match value {
        Expr::Path(path) => path.path.get_ident().map(Ident::to_string),
        _ => None,
    };
    match named.as_deref() {
        Some("offset") => Ok(Pagination::Offset),
        Some("keyset") => Ok(Pagination::Keyset),
        _ => Err(syn::Error::new(
            value.span(),
            "pagination takes offset or keyset",
        )),
    }
}

impl Declarations {
    /// Record `meta` when it is one of the keys this module owns; `false`
    /// leaves it to the caller.
    pub(crate) fn apply(&mut self, meta: &Meta) -> syn::Result<bool> {
        let key = match meta.path().get_ident() {
            Some(key) => key.to_string(),
            None => return Ok(false),
        };
        let refuse = |reason: String| Err(syn::Error::new(meta.span(), reason));
        match (key.as_str(), meta) {
            ("managed", Meta::NameValue(nv)) => self.managed = boolean(&nv.value, "managed")?,
            ("sensitive", Meta::Path(_)) => self.sensitive = true,
            ("pagination", Meta::NameValue(nv)) => self.pagination = pagination(&nv.value)?,
            ("audit_redact", Meta::NameValue(nv)) => {
                let rules = strings(&nv.value, "audit_redact")?;
                validate_audit_redact("audit_redact", &rules).or_else(refuse)?;
                self.audit_redact = rules;
            }
            ("search", Meta::NameValue(nv)) => {
                self.search_columns = Some((strings(&nv.value, "search")?, nv.value.span()))
            }
            ("search_config", Meta::NameValue(nv)) => {
                self.search_config = Some(string(&nv.value, "search_config")?)
            }
            ("search_vector", Meta::NameValue(nv)) => {
                self.search_vector = Some((string(&nv.value, "search_vector")?, nv.value.span()))
            }
            ("check", Meta::List(list)) => {
                let check = list.parse_args_with(parse_check)?;
                validate_check_constraint("check", &check).or_else(refuse)?;
                self.checks.push(check);
            }
            ("index", Meta::List(list)) => {
                let index = list.parse_args_with(parse_index)?;
                validate_index("index", &index).or_else(refuse)?;
                self.indexes.push((index, list.span()));
            }
            (
                "managed" | "sensitive" | "pagination" | "audit_redact" | "search"
                | "search_config" | "search_vector" | "check" | "index",
                _,
            ) => refuse(format!("'{key}' is not written that way"))?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    pub(crate) fn search(&self) -> Option<SearchSpec> {
        let declared = self.search_columns.is_some()
            || self.search_config.is_some()
            || self.search_vector.is_some();
        declared.then(|| SearchSpec {
            columns: self
                .search_columns
                .as_ref()
                .map(|(columns, _)| columns.clone())
                .unwrap_or_default(),
            config: self
                .search_config
                .clone()
                .unwrap_or_else(SearchSpec::default_config),
            vector_column: self
                .search_vector
                .as_ref()
                .map(|(column, _)| column.clone()),
        })
    }

    /// What the keys say together, checked against the table's `columns`.
    pub(crate) fn validate(&self, table: Span, columns: &[String]) -> syn::Result<()> {
        let declared = |what: &str, column: &str, span: Span| {
            if columns.iter().any(|c| c == column) {
                Ok(())
            } else {
                Err(syn::Error::new(
                    span,
                    format!("{what} names '{column}', which is not a field of the table"),
                ))
            }
        };
        self.indexes.iter().try_for_each(|(index, span)| {
            index
                .columns
                .iter()
                .try_for_each(|column| declared("index", column, *span))
        })?;
        if let Some((search_columns, span)) = &self.search_columns {
            search_columns
                .iter()
                .try_for_each(|column| declared("search", column, *span))?;
        }
        if let Some((column, span)) = &self.search_vector {
            declared("search_vector", column, *span)?;
        }
        self.search().map_or(Ok(()), |search| {
            validate_search_spec("search", &search).map_err(|reason| syn::Error::new(table, reason))
        })?;
        let names = |what: &str, names: Vec<&str>| {
            names
                .iter()
                .enumerate()
                .find(|(index, name)| names[..*index].contains(name))
                .map_or(Ok(()), |(_, name)| {
                    Err(syn::Error::new(
                        table,
                        format!("{what} '{name}' is declared twice"),
                    ))
                })
        };
        names(
            "check",
            self.checks.iter().map(|c| c.name.as_str()).collect(),
        )?;
        names(
            "index",
            self.indexes.iter().map(|(i, _)| i.name.as_str()).collect(),
        )
    }

    pub(crate) fn index_tokens(&self) -> Vec<TokenStream> {
        self.indexes
            .iter()
            .map(|(index, _)| index_tokens(index))
            .collect()
    }

    /// The `TableDefinition` fields this module owns.
    pub(crate) fn field_tokens(&self) -> TokenStream {
        let managed = self.managed;
        let sensitive = self.sensitive;
        let pagination = match self.pagination {
            Pagination::Offset => quote! { ::cufflink::manifest::Pagination::Offset },
            Pagination::Keyset => quote! { ::cufflink::manifest::Pagination::Keyset },
        };
        let audit_redact = &self.audit_redact;
        let checks = self.checks.iter().map(|check| {
            let CheckConstraint { name, expr } = check;
            quote! {
                ::cufflink::manifest::CheckConstraint {
                    name: #name.to_string(),
                    expr: #expr.to_string(),
                }
            }
        });
        let search = match self.search() {
            Some(SearchSpec {
                columns,
                config,
                vector_column,
            }) => {
                let vector_column = match vector_column {
                    Some(column) => quote! { ::std::option::Option::Some(#column.to_string()) },
                    None => quote! { ::std::option::Option::None },
                };
                quote! {
                    ::std::option::Option::Some(::cufflink::manifest::SearchSpec {
                        columns: vec![#(#columns.to_string()),*],
                        config: #config.to_string(),
                        vector_column: #vector_column,
                    })
                }
            }
            None => quote! { ::std::option::Option::None },
        };
        quote! {
            managed: #managed,
            checks: vec![#(#checks),*],
            search: #search,
            audit_redact: vec![#(#audit_redact.to_string()),*],
            sensitive: #sensitive,
            pagination: #pagination,
        }
    }
}

/// `restricted(read = "internal", write = "internal")`.
fn parse_restricted(input: ParseStream) -> syn::Result<ColumnAccess> {
    let (mut read, mut write) = (None, None);
    items(input, |key, input| match key.to_string().as_str() {
        "read" => once(&mut read, key, assigned::<LitStr>(input)?.value()),
        "write" => once(&mut write, key, assigned::<LitStr>(input)?.value()),
        other => Err(syn::Error::new(
            key.span(),
            format!("unknown restricted option '{other}'; expected read or write"),
        )),
    })?;
    Ok(ColumnAccess { read, write })
}

/// What a field's `#[restricted(...)]` asks of a caller, written once.
pub(crate) fn restricted(attrs: &[syn::Attribute]) -> syn::Result<Option<ColumnAccess>> {
    let mut declared = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("restricted"));
    match (declared.next(), declared.next()) {
        (_, Some(again)) => Err(syn::Error::new(
            again.span(),
            "restricted is written once for a field",
        )),
        (Some(attr), None) => attr.parse_args_with(parse_restricted).map(Some),
        (None, _) => Ok(None),
    }
}

pub(crate) fn access_tokens(access: &Option<ColumnAccess>) -> TokenStream {
    let operation = |operation: &Option<String>| match operation {
        Some(operation) => quote! { ::std::option::Option::Some(#operation.to_string()) },
        None => quote! { ::std::option::Option::None },
    };
    match access {
        Some(ColumnAccess { read, write }) => {
            let (read, write) = (operation(read), operation(write));
            quote! {
                ::std::option::Option::Some(::cufflink::manifest::ColumnAccess {
                    read: #read,
                    write: #write,
                })
            }
        }
        None => quote! { ::std::option::Option::None },
    }
}

pub(crate) fn index_tokens(index: &IndexDefinition) -> TokenStream {
    let IndexDefinition {
        name,
        columns,
        unique,
        predicate,
        method,
    } = index;
    let predicate = match predicate {
        Some(predicate) => quote! { ::std::option::Option::Some(#predicate.to_string()) },
        None => quote! { ::std::option::Option::None },
    };
    let method = match method {
        IndexMethod::Btree => quote! { ::cufflink::manifest::IndexMethod::Btree },
        IndexMethod::Gin => quote! { ::cufflink::manifest::IndexMethod::Gin },
        IndexMethod::Gist => quote! { ::cufflink::manifest::IndexMethod::Gist },
        IndexMethod::Hash => quote! { ::cufflink::manifest::IndexMethod::Hash },
        IndexMethod::Brin => quote! { ::cufflink::manifest::IndexMethod::Brin },
    };
    quote! {
        ::cufflink::manifest::IndexDefinition {
            name: #name.to_string(),
            columns: vec![#(#columns.to_string()),*],
            unique: #unique,
            predicate: #predicate,
            method: #method,
        }
    }
}

/// The `key = value` and bare-flag items between the parentheses of
/// `check(...)` and `index(...)`; `where` is a keyword, so the keys are
/// read as any identifier.
fn items(
    input: ParseStream,
    mut apply: impl FnMut(&Ident, ParseStream) -> syn::Result<()>,
) -> syn::Result<()> {
    while !input.is_empty() {
        let key = input.call(Ident::parse_any)?;
        apply(&key, input)?;
        if input.is_empty() {
            break;
        }
        input.parse::<Token![,]>()?;
    }
    Ok(())
}

/// `check(name = "price_nonneg", expr = "price_cents >= 0")`.
fn parse_check(input: ParseStream) -> syn::Result<CheckConstraint> {
    let (mut name, mut expr) = (None, None);
    items(input, |key, input| match key.to_string().as_str() {
        "name" => once(&mut name, key, assigned::<LitStr>(input)?.value()),
        "expr" => once(&mut expr, key, assigned::<LitStr>(input)?.value()),
        other => Err(syn::Error::new(
            key.span(),
            format!("unknown check option '{other}'; expected name or expr"),
        )),
    })?;
    match (name, expr) {
        (Some(name), Some(expr)) => Ok(CheckConstraint { name, expr }),
        _ => Err(input.error("check(...) takes name = \"...\" and expr = \"...\"")),
    }
}

fn values(literals: Vec<LitStr>) -> Vec<String> {
    literals.iter().map(LitStr::value).collect()
}

fn index_method(input: ParseStream) -> syn::Result<IndexMethod> {
    input.parse::<Token![=]>()?;
    let method: LitStr = input.parse()?;
    match method.value().as_str() {
        "btree" => Ok(IndexMethod::Btree),
        "gin" => Ok(IndexMethod::Gin),
        "gist" => Ok(IndexMethod::Gist),
        "hash" => Ok(IndexMethod::Hash),
        "brin" => Ok(IndexMethod::Brin),
        other => Err(syn::Error::new(
            method.span(),
            format!("unknown index method '{other}'; expected btree, gin, gist, hash or brin"),
        )),
    }
}

/// `index(name = "uq_branch_email", columns = ["branch_id", "email"],
/// unique, where = "deleted_at IS NULL", using = "btree")`.
fn parse_index(input: ParseStream) -> syn::Result<IndexDefinition> {
    let (mut name, mut columns, mut predicate, mut method) = (None, None, None, None);
    let mut unique = None;
    items(input, |key, input| match key.to_string().as_str() {
        "name" => once(&mut name, key, assigned::<LitStr>(input)?.value()),
        "columns" => once(&mut columns, key, values(assigned_strings(input)?)),
        "where" => once(&mut predicate, key, assigned::<LitStr>(input)?.value()),
        "using" => once(&mut method, key, index_method(input)?),
        "unique" if input.peek(Token![=]) => {
            input.parse::<Token![=]>()?;
            once(&mut unique, key, input.parse::<LitBool>()?.value)
        }
        "unique" => once(&mut unique, key, true),
        other => Err(syn::Error::new(
            key.span(),
            format!(
                "unknown index option '{other}'; expected name, columns, unique, where or using"
            ),
        )),
    })?;
    match (name, columns) {
        (Some(name), Some(columns)) => Ok(IndexDefinition {
            name,
            columns,
            unique: unique.unwrap_or(false),
            predicate,
            method: method.unwrap_or_default(),
        }),
        _ => Err(input.error("index(...) takes name = \"...\" and columns = [...]")),
    }
}

/// `#[column(numeric(12, 2))]`, `#[column(numeric(12))]`,
/// `#[column(numeric)]` or `#[column(varchar(50))]`.
pub(crate) fn parse_column(input: ParseStream) -> syn::Result<ColumnType> {
    let kind: Ident = input.parse()?;
    let numbers = |input: ParseStream| -> syn::Result<Vec<LitInt>> {
        if !input.peek(syn::token::Paren) {
            return Ok(Vec::new());
        }
        let inner;
        syn::parenthesized!(inner in input);
        Ok(inner
            .parse_terminated(|s| s.parse::<LitInt>(), Token![,])?
            .into_iter()
            .collect())
    };
    let arguments = numbers(input)?;
    let column_type = match (kind.to_string().as_str(), arguments.as_slice()) {
        ("numeric", []) => ColumnType::Numeric {
            precision: None,
            scale: None,
        },
        ("numeric", [precision]) => ColumnType::Numeric {
            precision: Some(precision.base10_parse()?),
            scale: None,
        },
        ("numeric", [precision, scale]) => ColumnType::Numeric {
            precision: Some(precision.base10_parse()?),
            scale: Some(scale.base10_parse()?),
        },
        ("varchar", [length]) => ColumnType::Varchar {
            length: length.base10_parse()?,
        },
        _ => {
            return Err(syn::Error::new(
                kind.span(),
                "column(...) takes numeric, numeric(precision), numeric(precision, scale) \
                 or varchar(length)",
            ))
        }
    };
    validate_column_type("column", &column_type)
        .map(|()| column_type)
        .map_err(|reason| syn::Error::new(kind.span(), reason))
}

pub(crate) fn column_type_tokens(column_type: &ColumnType) -> TokenStream {
    let option = |value: &Option<u16>| match value {
        Some(n) => quote! { ::std::option::Option::Some(#n) },
        None => quote! { ::std::option::Option::None },
    };
    match column_type {
        ColumnType::Uuid => quote! { ::cufflink::manifest::ColumnType::Uuid },
        ColumnType::Text => quote! { ::cufflink::manifest::ColumnType::Text },
        ColumnType::Integer => quote! { ::cufflink::manifest::ColumnType::Integer },
        ColumnType::BigInteger => quote! { ::cufflink::manifest::ColumnType::BigInteger },
        ColumnType::Float => quote! { ::cufflink::manifest::ColumnType::Float },
        ColumnType::Double => quote! { ::cufflink::manifest::ColumnType::Double },
        ColumnType::Boolean => quote! { ::cufflink::manifest::ColumnType::Boolean },
        ColumnType::Timestamp => quote! { ::cufflink::manifest::ColumnType::Timestamp },
        ColumnType::Date => quote! { ::cufflink::manifest::ColumnType::Date },
        ColumnType::Jsonb => quote! { ::cufflink::manifest::ColumnType::Jsonb },
        ColumnType::Numeric { precision, scale } => {
            let (precision, scale) = (option(precision), option(scale));
            quote! {
                ::cufflink::manifest::ColumnType::Numeric { precision: #precision, scale: #scale }
            }
        }
        ColumnType::Varchar { length } => {
            quote! { ::cufflink::manifest::ColumnType::Varchar { length: #length } }
        }
        ColumnType::TimestampNaive => quote! { ::cufflink::manifest::ColumnType::TimestampNaive },
        ColumnType::Time => quote! { ::cufflink::manifest::ColumnType::Time },
        ColumnType::Bytea => quote! { ::cufflink::manifest::ColumnType::Bytea },
        ColumnType::TextArray => quote! { ::cufflink::manifest::ColumnType::TextArray },
        ColumnType::UuidArray => quote! { ::cufflink::manifest::ColumnType::UuidArray },
        ColumnType::IntegerArray => quote! { ::cufflink::manifest::ColumnType::IntegerArray },
        ColumnType::BigIntegerArray => quote! { ::cufflink::manifest::ColumnType::BigIntegerArray },
        ColumnType::JsonbArray => quote! { ::cufflink::manifest::ColumnType::JsonbArray },
    }
}
