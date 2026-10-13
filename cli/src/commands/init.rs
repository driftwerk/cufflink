use std::fs;
use std::path::Path;

const SDK_VERSION: &str = "0.17";
const FN_VERSION: &str = "0.33";

pub async fn run(
    name: &str,
    template: Option<&str>,
    mode: &str,
    from: Option<&str>,
) -> eyre::Result<()> {
    let project_dir = Path::new(name);

    if project_dir.exists() {
        eyre::bail!("Directory '{}' already exists", name);
    }

    if let Some(source) = from {
        return init_from_starter_kit(name, source).await;
    }

    fs::create_dir_all(project_dir.join("src"))?;

    match mode {
        "wasm" => generate_wasm_project(name, project_dir, template)?,
        "crud" => generate_crud_project(name, project_dir, template)?,
        other => eyre::bail!("Unknown mode '{}'. Available: crud, wasm", other),
    }

    generate_cufflink_toml(name, project_dir)?;

    let has_wasm_lib = mode == "wasm" || matches!((mode, template), ("crud", Some("hooks")));

    println!("Created service project: {}/", name);
    if let Some(t) = template {
        println!("  Template: {}", t);
    }
    println!("  Mode: {}", mode);
    println!("  {}/Cufflink.toml", name);
    println!("  {}/Cargo.toml", name);
    println!("  {}/src/main.rs", name);
    if has_wasm_lib {
        println!("  {}/src/lib.rs", name);
    }
    println!();
    println!("Next steps:");
    println!("  cd {}", name);
    println!("  # Edit src/main.rs to customize your tables");
    if has_wasm_lib {
        println!("  # Edit src/lib.rs to write your WASM handlers");
    }
    println!("  cufflink deploy");

    Ok(())
}

pub fn list_templates() {
    println!("Available templates:");
    println!();
    println!("  CRUD mode (default):");
    println!("    default     - Basic item table");
    println!("    todo        - Todo list with tasks and categories");
    println!("    blog        - Blog with posts, comments, and tags");
    println!("    ecommerce   - E-commerce with products, orders, and customers");
    println!("    cms         - Content management with pages and media");
    println!("    hooks       - CRUD service with before/after table lifecycle hooks");
    println!();
    println!("  WASM mode (--mode wasm):");
    println!("    default     - Basic WASM service with a custom handler");
    println!("    ecommerce   - Order processing with checkout handler");
    println!();
    println!("Usage: cufflink init <name> [--template <template>] [--mode <mode>]");
}

fn default_table_name(name: &str) -> String {
    let base = name.replace('-', "_");
    if base.ends_with('s') {
        base
    } else {
        format!("{base}s")
    }
}

fn generate_cufflink_toml(name: &str, project_dir: &Path) -> eyre::Result<()> {
    let content = format!(
        r#"[service]
default_env = "local"

[environments.local]
api_url = "http://localhost:8080"
tenant = "default"

# Uncomment and configure for staging/production:
#
# [environments.staging]
# api_url = "https://cufflink.example.com"
# tenant = "{name}-staging"
# api_key_env = "CUFFLINK_STAGING_API_KEY"
#
# [environments.production]
# api_url = "https://cufflink.example.com"
# tenant = "{name}-prod"
# api_key_env = "CUFFLINK_PROD_API_KEY"
"#
    );
    fs::write(project_dir.join("Cufflink.toml"), content)?;
    Ok(())
}

fn generate_crud_project(
    name: &str,
    project_dir: &Path,
    template: Option<&str>,
) -> eyre::Result<()> {
    let cargo_toml = format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[dependencies]
cufflink = {{ version = "{SDK_VERSION}" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
uuid = {{ version = "1", features = ["v4", "serde"] }}
chrono = {{ version = "0.4", features = ["serde"] }}
"#
    );
    fs::write(project_dir.join("Cargo.toml"), cargo_toml)?;

    if matches!(template, Some("hooks")) {
        return generate_crud_hooks_project(name, project_dir);
    }

    let main_rs = match template {
        Some("todo") => template_todo(name),
        Some("blog") => template_blog(name),
        Some("ecommerce") => template_ecommerce(name),
        Some("cms") => template_cms(name),
        Some(t) => {
            eyre::bail!(
                "Unknown template '{}'. Use `cufflink templates` to see available templates.",
                t
            );
        }
        None => template_default(name),
    };

    fs::write(project_dir.join("src/main.rs"), main_rs)?;
    Ok(())
}

fn generate_crud_hooks_project(name: &str, project_dir: &Path) -> eyre::Result<()> {
    let table_name = default_table_name(name);
    let cargo_toml = format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
cufflink = {{ version = "{SDK_VERSION}" }}
cufflink-fn = {{ version = "{FN_VERSION}" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
# `v4` is intentionally omitted: the platform auto-generates row IDs, and
# pulling in `getrandom`/`v4` makes the wasm32 build fail without a JS shim.
uuid = {{ version = "1", features = ["serde"] }}
chrono = {{ version = "0.4", features = ["serde"] }}
"#
    );
    fs::write(project_dir.join("Cargo.toml"), cargo_toml)?;

    let main_rs = format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "{table_name}")]
#[hooks(
    before_create = "validate_item",
    after_create = "audit_item_created",
    before_delete = "check_owner_can_delete",
)]
pub struct Item {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Item],
}}
"#
    );
    fs::write(project_dir.join("src/main.rs"), main_rs)?;

    let lib_rs = r#"//! WASM hook handlers for the CRUD service defined in main.rs.
//!
//! Cargo builds this as a cdylib (`cargo build --lib --target wasm32-unknown-unknown`)
//! and the cufflink CLI bundles the resulting .wasm into the deploy
//! payload. The platform invokes each named handler when the corresponding
//! lifecycle event fires.

use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(validate_item, |req: Request| {
    let data = &req.body()["data"];
    let name = data["name"].as_str().unwrap_or("");
    if name.trim().is_empty() {
        return Response::error("name is required");
    }
    // Returning the body lets you mutate `data` before the INSERT runs.
    Response::json(req.body())
});

handler!(audit_item_created, |req: Request| {
    let result = &req.body()["result"];
    log::info(&format!(
        "item created: id={}",
        result["id"].as_str().unwrap_or("?")
    ));
    Response::empty()
});

handler!(check_owner_can_delete, |req: Request| {
    // before_delete hooks can reject the operation by returning an error.
    if req.auth().is_none() {
        return Response::error("authentication required");
    }
    Response::empty()
});
"#
    .to_string();
    fs::write(project_dir.join("src/lib.rs"), lib_rs)?;

    Ok(())
}

fn generate_wasm_project(
    name: &str,
    project_dir: &Path,
    template: Option<&str>,
) -> eyre::Result<()> {
    let cargo_toml = format!(
        r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
cufflink = {{ version = "{SDK_VERSION}" }}
cufflink-fn = {{ version = "{FN_VERSION}" }}
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
# `v4` is intentionally omitted: the platform auto-generates row IDs, and
# pulling in `getrandom`/`v4` makes the wasm32 build fail without a JS shim.
uuid = {{ version = "1", features = ["serde"] }}
chrono = {{ version = "0.4", features = ["serde"] }}
"#
    );
    fs::write(project_dir.join("Cargo.toml"), cargo_toml)?;

    let (main_rs, lib_rs) = match template {
        Some("ecommerce") => wasm_template_ecommerce(name),
        Some(t) => {
            eyre::bail!(
                "Unknown WASM template '{}'. Use `cufflink templates` to see available templates.",
                t
            );
        }
        None => wasm_template_default(name),
    };

    fs::write(project_dir.join("src/main.rs"), main_rs)?;
    fs::write(project_dir.join("src/lib.rs"), lib_rs)?;
    Ok(())
}

fn template_default(name: &str) -> String {
    let table_name = default_table_name(name);
    format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "{table_name}")]
pub struct Item {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Item],
}}
"#
    )
}

fn template_todo(name: &str) -> String {
    format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "categories")]
pub struct Category {{
    #[key]
    pub id: Uuid,
    pub name: String,
    pub color: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "todos", soft_delete)]
pub struct Todo {{
    #[key]
    pub id: Uuid,
    #[validate("max_length=500")]
    pub title: String,
    pub description: Option<String>,
    pub completed: bool,
    pub priority: Option<String>,
    #[references("categories.id", on_delete = "set_null")]
    pub category_id: Option<Uuid>,
    pub due_date: Option<NaiveDate>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Category, Todo],
}}
"#
    )
}

fn template_blog(name: &str) -> String {
    format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "authors")]
pub struct Author {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[unique]
    pub email: String,
    pub bio: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "posts", soft_delete)]
pub struct Post {{
    #[key]
    pub id: Uuid,
    #[validate("max_length=200")]
    pub title: String,
    pub slug: String,
    pub content: String,
    pub published: bool,
    #[references("authors.id", on_delete = "cascade")]
    pub author_id: Uuid,
    pub tags: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "comments")]
pub struct Comment {{
    #[key]
    pub id: Uuid,
    #[references("posts.id", on_delete = "cascade")]
    pub post_id: Uuid,
    pub author_name: String,
    pub content: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Author, Post, Comment],
}}
"#
    )
}

fn template_ecommerce(name: &str) -> String {
    format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "customers")]
pub struct Customer {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[unique]
    pub email: String,
    pub phone: Option<String>,
    pub address: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "products", soft_delete)]
pub struct Product {{
    #[key]
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    #[validate("min=0")]
    pub price_cents: i64,
    #[validate("min=0")]
    pub stock_quantity: i32,
    pub sku: Option<String>,
    pub category: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders")]
pub struct Order {{
    #[key]
    pub id: Uuid,
    #[references("customers.id", on_delete = "cascade")]
    pub customer_id: Uuid,
    pub status: String,
    pub total_cents: i64,
    pub notes: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "order_items")]
pub struct OrderItem {{
    #[key]
    pub id: Uuid,
    #[references("orders.id", on_delete = "cascade")]
    pub order_id: Uuid,
    #[references("products.id", on_delete = "restrict")]
    pub product_id: Uuid,
    pub quantity: i32,
    pub unit_price_cents: i64,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Customer, Product, Order, OrderItem],
}}
"#
    )
}

fn template_cms(name: &str) -> String {
    format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "pages", soft_delete)]
pub struct Page {{
    #[key]
    pub id: Uuid,
    #[validate("max_length=200")]
    pub title: String,
    #[unique]
    pub slug: String,
    pub content: String,
    pub published: bool,
    pub sort_order: Option<i32>,
    pub parent_id: Option<Uuid>,
    pub meta_title: Option<String>,
    pub meta_description: Option<String>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "media")]
pub struct Media {{
    #[key]
    pub id: Uuid,
    pub filename: String,
    pub content_type: String,
    pub url: String,
    pub alt_text: Option<String>,
    pub size_bytes: i64,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "navigation")]
pub struct Navigation {{
    #[key]
    pub id: Uuid,
    pub label: String,
    pub url: String,
    pub sort_order: i32,
    pub parent_id: Option<Uuid>,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    tables: [Page, Media, Navigation],
}}
"#
    )
}

fn wasm_template_default(name: &str) -> (String, String) {
    let table_name = default_table_name(name);
    let main_rs = format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "{table_name}")]
pub struct Item {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

cufflink::service! {{
    name: "{name}",
    mode: wasm,
    tables: [Item],
    custom_routes: [
        // Every route declares a posture: public("why"), auth, perm("area", "op")
        // or owner("field"). Without one the platform runs it unenforced.
        ("GET", "/hello", "hello", public("demo endpoint")),
    ],
}}
"#
    );

    let lib_rs = r#"use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

// Reachable at GET/POST /svc/{tenant}/{service}/_fn/hello.
// A JSON POST body arrives as req.body(); GET query parameters arrive under
// req.body()["params"].
handler!(hello, |req: Request| {
    let body = req.body();
    let name = body["name"]
        .as_str()
        .or_else(|| body["params"]["name"].as_str())
        .unwrap_or("world");
    Response::json(&json!({"message": format!("Hello, {}!", name)}))
});
"#
    .to_string();

    (main_rs, lib_rs)
}

fn wasm_template_ecommerce(name: &str) -> (String, String) {
    let main_rs = format!(
        r#"use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "customers")]
pub struct Customer {{
    #[key]
    pub id: Uuid,
    pub name: String,
    #[unique]
    pub email: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "products")]
pub struct Product {{
    #[key]
    pub id: Uuid,
    pub name: String,
    pub price_cents: i64,
    pub stock_quantity: i32,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders")]
pub struct Order {{
    #[key]
    pub id: Uuid,
    #[references("customers.id", on_delete = "cascade")]
    pub customer_id: Uuid,
    pub status: String,
    pub total_cents: i64,
    #[timestamp]
    pub created_at: DateTime<Utc>,
    #[timestamp]
    pub updated_at: DateTime<Utc>,
}}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "order_items")]
pub struct OrderItem {{
    #[key]
    pub id: Uuid,
    #[references("orders.id", on_delete = "cascade")]
    pub order_id: Uuid,
    #[references("products.id", on_delete = "restrict")]
    pub product_id: Uuid,
    pub quantity: i32,
    pub unit_price_cents: i64,
}}

cufflink::service! {{
    name: "{name}",
    mode: wasm,
    tables: [Customer, Product, Order, OrderItem],
    custom_routes: [
        ("POST", "/checkout", "handle_checkout"),
        ("GET", "/order-summary/:id", "get_order_summary"),
    ],
}}
"#
    );

    let lib_rs = r#"use cufflink_fn::handler;
use cufflink_fn::prelude::*;

cufflink_fn::init!();

handler!(handle_checkout, |req: Request| {
    let body = req.body();
    let customer_id = body["customer_id"].as_str().unwrap_or("");
    let items = body["items"].as_array();

    let items = match items {
        Some(items) => items,
        None => return Response::error("Missing 'items' array in request body"),
    };

    // Calculate total
    let total: i64 = items
        .iter()
        .map(|i| i["quantity"].as_i64().unwrap_or(0) * i["unit_price_cents"].as_i64().unwrap_or(0))
        .sum();

    // Create order
    db::execute(&format!(
        "INSERT INTO orders (customer_id, status, total_cents) VALUES ('{}', 'pending', {})",
        customer_id, total
    ));

    // Publish event
    nats::publish(
        "orders.created",
        &json!({"customer_id": customer_id, "total_cents": total}).to_string(),
    );

    log::info(&format!(
        "Checkout completed for customer {}: {} cents",
        customer_id, total
    ));

    Response::json(&json!({
        "status": "pending",
        "total_cents": total,
        "item_count": items.len()
    }))
});

handler!(get_order_summary, |req: Request| {
    let body = req.body();
    let order_id = body["id"].as_str().unwrap_or("");

    let order = db::query_one(&format!(
        "SELECT id, customer_id, status, total_cents, created_at FROM orders WHERE id = '{}'",
        order_id
    ));

    match order {
        Some(o) => Response::json(&o),
        None => Response::error("Order not found"),
    }
});
"#
    .to_string();

    (main_rs, lib_rs)
}

async fn init_from_starter_kit(name: &str, source: &str) -> eyre::Result<()> {
    let gh = crate::github::GitHubSource::parse(source)?;
    println!("Scaffolding '{}' from {}...", name, gh.display());

    let temp = tempfile::tempdir()?;
    crate::github::download(&gh, temp.path()).await?;

    let dest = Path::new(name);
    let output = std::process::Command::new("cp")
        .args([
            "-r",
            &temp.path().to_string_lossy(),
            &dest.to_string_lossy(),
        ])
        .output()?;
    if !output.status.success() {
        eyre::bail!("Failed to copy starter kit to {}/", name);
    }

    if let Ok(manifest) = crate::package_manifest::PackageManifest::load(dest) {
        println!();
        manifest.print_summary();
    }

    println!("Project '{}' created from {}", name, gh.display());
    println!();
    println!("Next steps:");
    println!("  cd {}", name);
    println!("  cufflink deploy");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workspace_minor(crate_dir: &str) -> String {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join(crate_dir)
            .join("Cargo.toml");
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(manifest).unwrap()).unwrap();
        let version = parsed["package"]["version"].as_str().unwrap();
        version.rsplit_once('.').unwrap().0.to_string()
    }

    #[test]
    fn scaffold_versions_track_the_workspace_crates() {
        assert_eq!(SDK_VERSION, workspace_minor("sdk"));
        assert_eq!(FN_VERSION, workspace_minor("cufflink-fn"));
    }

    #[test]
    fn wasm_scaffold_pins_the_current_sdk_crates() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("src")).unwrap();
        generate_wasm_project("svc", dir.path(), None).unwrap();
        let cargo = fs::read_to_string(dir.path().join("Cargo.toml")).unwrap();
        assert!(cargo.contains(&format!("cufflink = {{ version = \"{SDK_VERSION}\" }}")));
        assert!(cargo.contains(&format!("cufflink-fn = {{ version = \"{FN_VERSION}\" }}")));
    }
}
