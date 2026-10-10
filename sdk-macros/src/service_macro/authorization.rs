use syn::parse::ParseStream;
use syn::{Ident, LitStr, Token};

pub(super) struct AreaEntry {
    pub(super) name: LitStr,
    pub(super) operations: Vec<LitStr>,
}

pub(super) struct DefaultRoleEntry {
    pub(super) name: LitStr,
    pub(super) description: LitStr,
    pub(super) permissions: Vec<LitStr>,
}

pub(super) struct AuthorizationBlock {
    pub(super) areas: Vec<AreaEntry>,
    pub(super) default_roles: Vec<DefaultRoleEntry>,
}

/// Parse `[areas: [...], default_roles: [...]]`.
pub(super) fn parse_authorization(input: ParseStream) -> syn::Result<AuthorizationBlock> {
    let content;
    syn::bracketed!(content in input);
    let mut areas: Vec<AreaEntry> = Vec::new();
    let mut default_roles: Vec<DefaultRoleEntry> = Vec::new();

    while !content.is_empty() {
        let inner_key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;

        match inner_key.to_string().as_str() {
            "areas" => areas.extend(parse_areas(&content)?),
            "default_roles" => default_roles.extend(parse_default_roles(&content)?),
            other => {
                return Err(syn::Error::new(
                    inner_key.span(),
                    format!("Unknown authorization field: '{}'", other),
                ));
            }
        }

        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }

    Ok(AuthorizationBlock {
        areas,
        default_roles,
    })
}

/// `[("area", ["op", …]), …]`.
fn parse_areas(input: ParseStream) -> syn::Result<Vec<AreaEntry>> {
    let areas_content;
    syn::bracketed!(areas_content in input);
    let mut areas = Vec::new();
    while !areas_content.is_empty() {
        let tuple;
        syn::parenthesized!(tuple in areas_content);
        let name: LitStr = tuple.parse()?;
        tuple.parse::<Token![,]>()?;
        let operations = parse_strings(&tuple)?;
        areas.push(AreaEntry { name, operations });
        if areas_content.peek(Token![,]) {
            areas_content.parse::<Token![,]>()?;
        }
    }
    Ok(areas)
}

/// `[("role", "description", ["area:op", …]), …]`.
fn parse_default_roles(input: ParseStream) -> syn::Result<Vec<DefaultRoleEntry>> {
    let roles_content;
    syn::bracketed!(roles_content in input);
    let mut roles = Vec::new();
    while !roles_content.is_empty() {
        let tuple;
        syn::parenthesized!(tuple in roles_content);
        let name: LitStr = tuple.parse()?;
        tuple.parse::<Token![,]>()?;
        let description: LitStr = tuple.parse()?;
        tuple.parse::<Token![,]>()?;
        let permissions = parse_strings(&tuple)?;
        roles.push(DefaultRoleEntry {
            name,
            description,
            permissions,
        });
        if roles_content.peek(Token![,]) {
            roles_content.parse::<Token![,]>()?;
        }
    }
    Ok(roles)
}

/// `["a", "b", …]`.
fn parse_strings(input: ParseStream) -> syn::Result<Vec<LitStr>> {
    let content;
    syn::bracketed!(content in input);
    Ok(content
        .parse_terminated(|input| input.parse::<LitStr>(), Token![,])?
        .into_iter()
        .collect())
}
