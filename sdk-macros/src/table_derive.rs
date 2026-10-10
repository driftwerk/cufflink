use crate::table_declarations::{
    access_tokens, column_type_tokens, index_tokens, parse_column, restricted, Declarations,
};
use cufflink_types::manifest::validate::validate_column_access;
use cufflink_types::{ColumnDefinition, ColumnType, IndexDefinition, TableDefinition};
use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Fields, Lit, Meta, Type};

pub fn impl_table(input: &DeriveInput) -> syn::Result<TokenStream> {
    let struct_name = &input.ident;

    let TableAttrs {
        name: table_name,
        soft_delete,
        auth_required,
        permission_area,
        owner_field,
        owner_self_service,
        scoped_by,
        declarations,
    } = extract_table_attrs(input)?;

    let hooks = extract_hooks_attr(input)?;

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    input,
                    "Table derive requires named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "Table derive only works on structs",
            ))
        }
    };

    let mut column_tokens = Vec::new();
    let mut field_index_tokens = Vec::new();
    let mut columns = Vec::new();
    let column_names: Vec<String> = fields
        .iter()
        .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
        .collect();
    declarations.validate(input.span(), &column_names)?;

    for field in fields {
        let field_name = field
            .ident
            .as_ref()
            .ok_or_else(|| syn::Error::new_spanned(field, "Expected named field"))?;
        let field_name_str = field_name.to_string();

        let is_key = has_attr(&field.attrs, "key");
        let is_timestamp = has_attr(&field.attrs, "timestamp");
        let default_value = extract_default_value(&field.attrs);
        let references = extract_string_attr(&field.attrs, "references");
        let on_delete = extract_string_attr(&field.attrs, "on_delete");
        let is_unique = has_attr(&field.attrs, "unique");
        let has_index = has_attr(&field.attrs, "index");
        let validations = extract_validations(&field.attrs);

        let (inner_type, nullable) = unwrap_option(&field.ty);
        let declared_type = declared_column_type(field, inner_type)?;
        let column_type = column_type_tokens(&declared_type);
        let access = restricted(&field.attrs)?;
        let access_token = access_tokens(&access);

        let auto_generate = is_key || is_timestamp;
        let primary_key = is_key;

        let default_token = match &default_value {
            Some(val) => quote! { ::std::option::Option::Some(#val.to_string()) },
            None => quote! { ::std::option::Option::None },
        };

        let references_token = match &references {
            Some(val) => quote! { ::std::option::Option::Some(#val.to_string()) },
            None => quote! { ::std::option::Option::None },
        };

        let on_delete_token = match on_delete.as_deref() {
            Some("cascade") => {
                quote! { ::std::option::Option::Some(::cufflink::manifest::ForeignKeyAction::Cascade) }
            }
            Some("set_null") => {
                quote! { ::std::option::Option::Some(::cufflink::manifest::ForeignKeyAction::SetNull) }
            }
            Some("restrict") => {
                quote! { ::std::option::Option::Some(::cufflink::manifest::ForeignKeyAction::Restrict) }
            }
            Some("no_action") => {
                quote! { ::std::option::Option::Some(::cufflink::manifest::ForeignKeyAction::NoAction) }
            }
            _ => quote! { ::std::option::Option::None },
        };

        let validation_tokens: Vec<_> = validations.iter().map(|v| {
            match v {
                Validation::OneOf(values) => quote! {
                    ::cufflink::manifest::ValidationRule::OneOf {
                        values: vec![#(#values.to_string()),*],
                    }
                },
                Validation::Text(s) if s.starts_with("regex=") => {
                    let pattern = &s[6..];
                    quote! { ::cufflink::manifest::ValidationRule::Regex { pattern: #pattern.to_string() } }
                }
                Validation::Text(s) if s.starts_with("min=") => {
                    let val: f64 = s[4..].parse().unwrap_or(0.0);
                    quote! { ::cufflink::manifest::ValidationRule::Min { value: #val } }
                }
                Validation::Text(s) if s.starts_with("max=") => {
                    let val: f64 = s[4..].parse().unwrap_or(0.0);
                    quote! { ::cufflink::manifest::ValidationRule::Max { value: #val } }
                }
                Validation::Text(s) if s.starts_with("min_length=") => {
                    let val: usize = s[11..].parse().unwrap_or(0);
                    quote! { ::cufflink::manifest::ValidationRule::MinLength { value: #val } }
                }
                Validation::Text(s) if s.starts_with("max_length=") => {
                    let val: usize = s[11..].parse().unwrap_or(0);
                    quote! { ::cufflink::manifest::ValidationRule::MaxLength { value: #val } }
                }
                Validation::Text(_) => quote! {}
            }
        }).collect();

        column_tokens.push(quote! {
            ::cufflink::manifest::ColumnDefinition {
                name: #field_name_str.to_string(),
                column_type: #column_type,
                primary_key: #primary_key,
                nullable: #nullable,
                auto_generate: #auto_generate,
                default_value: #default_token,
                references: #references_token,
                on_delete: #on_delete_token,
                unique: #is_unique,
                validations: vec![#(#validation_tokens),*],
                access: #access_token,
            }
        });
        columns.push(ColumnDefinition {
            name: field_name_str.clone(),
            column_type: declared_type,
            primary_key,
            nullable,
            auto_generate,
            default_value,
            unique: is_unique,
            access,
            ..Default::default()
        });

        if has_index {
            field_index_tokens.push(index_tokens(&IndexDefinition {
                name: format!("idx_{}_{}", table_name, field_name_str),
                columns: vec![field_name_str.clone()],
                ..Default::default()
            }));
        }
    }
    validate_column_access(
        &format!("tables.{table_name}"),
        &TableDefinition {
            name: table_name.clone(),
            columns,
            indexes: declarations
                .indexes
                .iter()
                .map(|(index, _)| index.clone())
                .collect(),
            owner_field: owner_field.clone(),
            permission_area: permission_area.clone(),
            scoped_by: scoped_by.clone(),
            search: declarations.search(),
            ..Default::default()
        },
    )
    .map_err(|reason| syn::Error::new(input.span(), reason))?;
    let index_tokens: Vec<TokenStream> = field_index_tokens
        .into_iter()
        .chain(declarations.index_tokens())
        .collect();
    let declared_fields = declarations.field_tokens();

    let permission_area_token = opt_str_token(&permission_area);
    let owner_field_token = opt_str_token(&owner_field);
    let scoped_by_token = opt_str_token(&scoped_by);

    let hooks_token = match &hooks {
        Some(h) => {
            let bc = opt_str_token(&h.before_create);
            let ac = opt_str_token(&h.after_create);
            let bu = opt_str_token(&h.before_update);
            let au = opt_str_token(&h.after_update);
            let bd = opt_str_token(&h.before_delete);
            let ad = opt_str_token(&h.after_delete);
            quote! {
                ::std::option::Option::Some(::cufflink::manifest::TableHooks {
                    before_create: #bc,
                    after_create: #ac,
                    before_update: #bu,
                    after_update: #au,
                    before_delete: #bd,
                    after_delete: #ad,
                })
            }
        }
        None => quote! { ::std::option::Option::None },
    };

    let expanded = quote! {
        impl #struct_name {
            #[doc(hidden)]
            pub fn __cufflink_table_def() -> ::cufflink::manifest::TableDefinition {
                ::cufflink::manifest::TableDefinition {
                    name: #table_name.to_string(),
                    columns: vec![#(#column_tokens),*],
                    indexes: vec![#(#index_tokens),*],
                    soft_delete: #soft_delete,
                    owner_field: #owner_field_token,
                    auth_required: #auth_required,
                    permission_area: #permission_area_token,
                    owner_self_service: #owner_self_service,
                    hooks: #hooks_token,
                    scoped_by: #scoped_by_token,
                    #declared_fields
                }
            }
        }
    };

    Ok(expanded)
}

struct TableAttrs {
    name: String,
    soft_delete: bool,
    auth_required: bool,
    permission_area: Option<String>,
    owner_field: Option<String>,
    owner_self_service: bool,
    scoped_by: Option<String>,
    declarations: Declarations,
}

fn meta_str(nv: &syn::MetaNameValue) -> Option<String> {
    if let syn::Expr::Lit(expr_lit) = &nv.value {
        if let Lit::Str(lit_str) = &expr_lit.lit {
            return Some(lit_str.value());
        }
    }
    None
}

fn extract_table_attrs(input: &DeriveInput) -> syn::Result<TableAttrs> {
    let mut name = None;
    let mut soft_delete = false;
    let mut auth_required = false;
    let mut permission_area = None;
    let mut owner_field = None;
    let mut owner_self_service = false;
    let mut scoped_by = None;
    let mut declarations = Declarations::default();

    for attr in &input.attrs {
        if !attr.path().is_ident("table") {
            continue;
        }
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        )?;
        for meta in nested {
            if declarations.apply(&meta)? {
                continue;
            }
            match &meta {
                Meta::NameValue(nv) if nv.path.is_ident("name") => name = meta_str(nv),
                Meta::NameValue(nv) if nv.path.is_ident("permission_area") => {
                    permission_area = meta_str(nv)
                }
                Meta::NameValue(nv) if nv.path.is_ident("owner_field") => {
                    owner_field = meta_str(nv)
                }
                Meta::NameValue(nv) if nv.path.is_ident("scoped_by") => scoped_by = meta_str(nv),
                Meta::Path(p) if p.is_ident("soft_delete") => soft_delete = true,
                Meta::Path(p) if p.is_ident("auth_required") => auth_required = true,
                Meta::Path(p) if p.is_ident("owner_self_service") => owner_self_service = true,
                _ => {}
            }
        }
    }

    let name = name.ok_or_else(|| {
        syn::Error::new_spanned(input, r#"#[table(name = "...")] attribute is required"#)
    })?;

    Ok(TableAttrs {
        name,
        soft_delete,
        auth_required,
        permission_area,
        owner_field,
        owner_self_service,
        scoped_by,
        declarations,
    })
}

/// Parsed hooks from #[hooks(before_create = "...", after_create = "...", ...)]
struct ParsedHooks {
    before_create: Option<String>,
    after_create: Option<String>,
    before_update: Option<String>,
    after_update: Option<String>,
    before_delete: Option<String>,
    after_delete: Option<String>,
}

fn extract_hooks_attr(input: &DeriveInput) -> syn::Result<Option<ParsedHooks>> {
    for attr in &input.attrs {
        if attr.path().is_ident("hooks") {
            let nested = attr.parse_args_with(
                syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
            )?;

            let mut hooks = ParsedHooks {
                before_create: None,
                after_create: None,
                before_update: None,
                after_update: None,
                before_delete: None,
                after_delete: None,
            };

            for meta in nested {
                if let Meta::NameValue(nv) = &meta {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(lit_str) = &expr_lit.lit {
                            let val = lit_str.value();
                            if nv.path.is_ident("before_create") {
                                hooks.before_create = Some(val);
                            } else if nv.path.is_ident("after_create") {
                                hooks.after_create = Some(val);
                            } else if nv.path.is_ident("before_update") {
                                hooks.before_update = Some(val);
                            } else if nv.path.is_ident("after_update") {
                                hooks.after_update = Some(val);
                            } else if nv.path.is_ident("before_delete") {
                                hooks.before_delete = Some(val);
                            } else if nv.path.is_ident("after_delete") {
                                hooks.after_delete = Some(val);
                            }
                        }
                    }
                }
            }

            return Ok(Some(hooks));
        }
    }
    Ok(None)
}

fn opt_str_token(val: &Option<String>) -> TokenStream {
    match val {
        Some(s) => quote! { ::std::option::Option::Some(#s.to_string()) },
        None => quote! { ::std::option::Option::None },
    }
}

fn has_attr(attrs: &[syn::Attribute], name: &str) -> bool {
    attrs.iter().any(|a| a.path().is_ident(name))
}

fn extract_default_value(attrs: &[syn::Attribute]) -> Option<String> {
    extract_string_attr(attrs, "default")
}

fn extract_string_attr(attrs: &[syn::Attribute], name: &str) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident(name) {
            if let Ok(Lit::Str(s)) = attr.parse_args::<Lit>() {
                return Some(s.value());
            }
        }
    }
    None
}

/// One rule of a `#[validate(...)]` attribute.
enum Validation {
    /// `"regex=..."`, `"min=..."`, …: a rule the macro does not know is
    /// dropped, as it always was.
    Text(String),
    /// `one_of("a", "b")`.
    OneOf(Vec<String>),
}

impl syn::parse::Parse for Validation {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if input.peek(syn::LitStr) {
            return input
                .parse::<syn::LitStr>()
                .map(|text| Validation::Text(text.value()));
        }
        let rule: syn::Ident = input.parse()?;
        if rule != "one_of" {
            return Err(syn::Error::new(
                rule.span(),
                format!("unknown validation '{rule}'; expected one_of(\"a\", \"b\")"),
            ));
        }
        let values;
        syn::parenthesized!(values in input);
        let values: Vec<String> = values
            .parse_terminated(|s| s.parse::<syn::LitStr>(), syn::Token![,])?
            .into_iter()
            .map(|value| value.value())
            .collect();
        if values.is_empty() {
            return Err(syn::Error::new(
                rule.span(),
                "one_of(...) lists the values allowed",
            ));
        }
        Ok(Validation::OneOf(values))
    }
}

/// The rules of `#[validate("regex=...", one_of("a", "b"), ...)]`. An
/// attribute that does not parse declares none, as it always has.
fn extract_validations(attrs: &[syn::Attribute]) -> Vec<Validation> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("validate"))
        .filter_map(|attr| {
            attr.parse_args_with(
                syn::punctuated::Punctuated::<Validation, syn::Token![,]>::parse_terminated,
            )
            .ok()
        })
        .flatten()
        .collect()
}

/// If the type is Option<T>, return (T, true). Otherwise return (ty, false).
fn unwrap_option(ty: &Type) -> (&Type, bool) {
    if let Type::Path(type_path) = ty {
        if let Some(seg) = type_path.path.segments.last() {
            if seg.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return (inner, true);
                    }
                }
            }
        }
    }
    (ty, false)
}

/// The column type a field declares: its `#[column(...)]` attribute over
/// a Rust type that can hold it, or the type its Rust type maps to.
fn declared_column_type(field: &syn::Field, ty: &Type) -> syn::Result<ColumnType> {
    let mapped = rust_type_to_column_type(ty)?;
    let declared = field
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("column"))
        .map(|attr| attr.parse_args_with(parse_column).map(|ty| (ty, attr)))
        .transpose()?;
    match (declared, mapped) {
        (None, mapped) => Ok(mapped),
        (Some((declared @ ColumnType::Varchar { .. }, _)), ColumnType::Text)
        | (
            Some((declared @ ColumnType::Numeric { .. }, _)),
            ColumnType::Text | ColumnType::Numeric { .. },
        ) => Ok(declared),
        (Some((ColumnType::Varchar { .. }, attr)), _) => Err(syn::Error::new(
            attr.span(),
            "column(varchar(..)) is for a String field",
        )),
        (Some((_, attr)), _) => Err(syn::Error::new(
            attr.span(),
            "column(numeric(..)) is for a PgNumeric, Decimal or String field",
        )),
    }
}

/// The element type of a `Vec<T>`.
fn vec_element(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    match (&segment.arguments, segment.ident == "Vec") {
        (syn::PathArguments::AngleBracketed(args), true) => match args.args.first()? {
            syn::GenericArgument::Type(element) => Some(element),
            _ => None,
        },
        _ => None,
    }
}

const SUPPORTED_TYPES: &str = "Uuid, String, bool, i32, i64, f32, f64, DateTime<Utc>, \
    NaiveDateTime, NaiveDate, NaiveTime, Value, PgNumeric, Decimal, Vec<u8>, Vec<String>, \
    Vec<Uuid>, Vec<i32>, Vec<i64>, Vec<Value>";

/// The column type a Rust type maps to.
fn rust_type_to_column_type(ty: &Type) -> syn::Result<ColumnType> {
    let unsupported = |name: String| {
        syn::Error::new_spanned(
            ty,
            format!("Unsupported type '{name}'. Supported: {SUPPORTED_TYPES}"),
        )
    };
    let type_name = extract_type_name(ty);
    if let Some(element) = vec_element(ty) {
        let element_name = extract_type_name(element);
        return match element_name.as_str() {
            "u8" => Ok(ColumnType::Bytea),
            "String" => Ok(ColumnType::TextArray),
            "Uuid" => Ok(ColumnType::UuidArray),
            "i32" => Ok(ColumnType::IntegerArray),
            "i64" => Ok(ColumnType::BigIntegerArray),
            "Value" => Ok(ColumnType::JsonbArray),
            _ => Err(unsupported(format!("Vec<{element_name}>"))),
        };
    }
    match type_name.as_str() {
        "Uuid" => Ok(ColumnType::Uuid),
        "String" => Ok(ColumnType::Text),
        "bool" => Ok(ColumnType::Boolean),
        "i32" => Ok(ColumnType::Integer),
        "i64" => Ok(ColumnType::BigInteger),
        "f32" => Ok(ColumnType::Float),
        "f64" => Ok(ColumnType::Double),
        "DateTime" => Ok(ColumnType::Timestamp),
        "NaiveDateTime" => Ok(ColumnType::TimestampNaive),
        "NaiveDate" => Ok(ColumnType::Date),
        "NaiveTime" => Ok(ColumnType::Time),
        "Value" => Ok(ColumnType::Jsonb),
        "PgNumeric" | "Decimal" => Ok(ColumnType::Numeric {
            precision: None,
            scale: None,
        }),
        _ => Err(unsupported(type_name)),
    }
}

/// Extract the last segment name from a type path (e.g. "DateTime" from "chrono::DateTime<Utc>")
fn extract_type_name(ty: &Type) -> String {
    match ty {
        Type::Path(type_path) => {
            if let Some(seg) = type_path.path.segments.last() {
                seg.ident.to_string()
            } else {
                String::new()
            }
        }
        _ => String::new(),
    }
}
