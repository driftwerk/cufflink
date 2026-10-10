//! Parsing shared by the keyword lists of the macros: `key = value` pairs
//! after the positional items, each allowed once.

use syn::parse::ParseStream;
use syn::{Ident, LitBool, LitInt, LitStr, Token};

/// Refuse a keyword given twice.
pub(crate) fn once<T>(slot: &mut Option<T>, key: &Ident, value: T) -> syn::Result<()> {
    match slot.replace(value) {
        Some(_) => Err(syn::Error::new(
            key.span(),
            format!("duplicate {key} keyword"),
        )),
        None => Ok(()),
    }
}

/// `= <value>` after a keyword.
pub(crate) fn assigned<T: syn::parse::Parse>(input: ParseStream) -> syn::Result<T> {
    input.parse::<Token![=]>()?;
    input.parse()
}

/// `= <integer>`, which may be negative.
pub(crate) fn assigned_signed(input: ParseStream) -> syn::Result<(i64, proc_macro2::Span)> {
    input.parse::<Token![=]>()?;
    let negative = input.parse::<Option<Token![-]>>()?.is_some();
    let literal: LitInt = input.parse()?;
    let magnitude: i64 = literal.base10_parse()?;
    Ok((
        if negative { -magnitude } else { magnitude },
        literal.span(),
    ))
}

/// `= <integer>` that fits `T`.
pub(crate) fn assigned_number<T>(input: ParseStream) -> syn::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    assigned::<LitInt>(input)?.base10_parse()
}

pub(crate) fn assigned_bool(input: ParseStream) -> syn::Result<bool> {
    assigned::<LitBool>(input).map(|flag| flag.value)
}

/// `= ["a", "b"]`.
pub(crate) fn assigned_strings(input: ParseStream) -> syn::Result<Vec<LitStr>> {
    input.parse::<Token![=]>()?;
    strings(input)
}

/// `["a", "b"]`.
pub(crate) fn strings(input: ParseStream) -> syn::Result<Vec<LitStr>> {
    let content;
    syn::bracketed!(content in input);
    Ok(content
        .parse_terminated(|s| s.parse::<LitStr>(), Token![,])?
        .into_iter()
        .collect())
}

/// `= [1, 2]`.
pub(crate) fn assigned_numbers<T>(input: ParseStream) -> syn::Result<Vec<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    input.parse::<Token![=]>()?;
    let content;
    syn::bracketed!(content in input);
    content
        .parse_terminated(|s| s.parse::<LitInt>(), Token![,])?
        .into_iter()
        .map(|literal| literal.base10_parse())
        .collect()
}

/// `= <identifier>` naming one of `choices`.
pub(crate) fn assigned_choice<T: Copy>(
    input: ParseStream,
    what: &str,
    choices: &[(&str, T)],
) -> syn::Result<T> {
    choice(&assigned::<Ident>(input)?, what, choices)
}

/// The choice `ident` names.
pub(crate) fn choice<T: Copy>(ident: &Ident, what: &str, choices: &[(&str, T)]) -> syn::Result<T> {
    choices
        .iter()
        .find(|(name, _)| ident == name)
        .map(|(_, value)| *value)
        .ok_or_else(|| {
            let names: Vec<&str> = choices.iter().map(|(name, _)| *name).collect();
            syn::Error::new(
                ident.span(),
                format!("unknown {what} '{ident}'; expected {}", one_of(&names)),
            )
        })
}

/// `a`, `a or b`, `a, b or c`.
pub(crate) fn one_of(names: &[&str]) -> String {
    match names {
        [] => String::new(),
        [only] => only.to_string(),
        [rest @ .., last] => format!("{} or {last}", rest.join(", ")),
    }
}

/// The trailing `, keyword …` items of a tuple, each handed to `apply`.
pub(crate) fn trailing(
    tuple: ParseStream,
    mut apply: impl FnMut(&Ident, ParseStream) -> syn::Result<()>,
) -> syn::Result<()> {
    while tuple.peek(Token![,]) {
        tuple.parse::<Token![,]>()?;
        if tuple.is_empty() {
            break;
        }
        let key: Ident = tuple.parse()?;
        apply(&key, tuple)?;
    }
    if tuple.is_empty() {
        Ok(())
    } else {
        Err(tuple.error("expected ',' or the end of the tuple"))
    }
}

/// The tuples of `[(…), (…)]`, each parsed by `entry`.
pub(crate) fn tuples<T>(
    input: ParseStream,
    entry: impl Fn(ParseStream) -> syn::Result<T>,
) -> syn::Result<Vec<T>> {
    let content;
    syn::bracketed!(content in input);
    let mut entries = Vec::new();
    while !content.is_empty() {
        let tuple;
        syn::parenthesized!(tuple in content);
        entries.push(entry(&tuple)?);
        if content.peek(Token![,]) {
            content.parse::<Token![,]>()?;
        }
    }
    Ok(entries)
}
