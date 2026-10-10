use proc_macro::TokenStream;

mod keywords;
mod service_macro;
mod table_declarations;
mod table_derive;

/// Derive macro for Table — generates schema metadata from Rust struct definitions.
///
/// # Attributes
/// - `#[table(name = "table_name")]` — required, sets the SQL table name
/// - `#[key]` — marks the primary key (auto-generated UUID)
/// - `#[timestamp]` — marks auto-generated timestamp fields
/// - `#[default("value")]` — sets a default value
/// - `#[column(numeric(12, 2))]`, `#[column(varchar(50))]` — sets the SQL type
/// - `#[restricted(read = "op", write = "op")]` — the operations of the table's permission area a
///   caller is granted to read the column through the data routes, and to write it
/// - `Option<T>` fields are automatically nullable
#[proc_macro_derive(
    Table,
    attributes(
        table, key, timestamp, default, references, on_delete, unique, index, validate, hooks,
        column, restricted
    )
)]
pub fn derive_table(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    match table_derive::impl_table(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Derive macro for Cell — generates single-value store metadata (not yet implemented)
#[proc_macro_derive(Cell, attributes(cell))]
pub fn derive_cell(_input: TokenStream) -> TokenStream {
    quote::quote! {
        compile_error!("Cell derive is not yet implemented. Use Table derive instead.");
    }
    .into()
}

/// Service definition macro — generates a main() function with manifest emission support.
///
/// # Usage
/// ```ignore
/// cufflink::service! {
///     name: "my-service",
///     tables: [MyTable, AnotherTable],
/// }
/// ```
#[proc_macro]
pub fn service(input: TokenStream) -> TokenStream {
    match service_macro::impl_service(input.into()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}
