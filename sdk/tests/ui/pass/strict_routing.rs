use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "books", auth_required, permission_area = "books")]
pub struct Book {
    #[key]
    pub id: Uuid,
}

cufflink::service! {
    name: "catalog-service",
    tables: [Book],
    mode: wasm,
    routing: strict,
    errors: { key: "error_message" },
    custom_routes: [
        ("PUT", "/books/{book_id}/due-date", "set_due_date", perm("books", "edit")),
        ("GET", "/books/assignment-pools/:pool_id", "get_pool", perm("books", "view")),
        ("GET", "/files/{*rest}", "get_file", auth, tags = ["files"], operation_id = "download_file",
            summary = "Download a file", description = "Streams the stored file.", deprecated,
            security = ["bearer_auth"], responses = [(200, bytes("application/pdf"), "PDF"), (404)]),
        ("GET", "/receipts/book/{book_id}/progress", "receipt_progress", perm("books", "view"),
            sse(subject = "dw.{tenant}.receipts.pdf.progress.{book_id}", reducer = "receipt_progress_reduce")),
        ("POST", "/books/reprice", "reprice_books", internal("pricing-service")),
        ("POST", "/ingest-book/{branch}", "ingest_book", api_key("books:ingest")),
        ("GET", "/books/mine", "my_books", scoped("books", "view", owner = "user_id")),
        ("POST", "/templates/import", "import_templates", perm("books", "edit"), multipart, body_limit = 10_485_760, timeout = 60),
    ],
}
