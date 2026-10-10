use cufflink::manifest::ColumnAccess;
use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(
    name = "listings",
    auth_required,
    permission_area = "listings",
    owner_field = "customer_id",
    owner_self_service
)]
pub struct Listing {
    #[key]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub title: String,
    #[restricted(read = "internal", write = "internal")]
    pub ops_notes: Option<String>,
    #[restricted(write = "manage")]
    #[default("draft")]
    pub status: String,
    #[restricted(read = "internal")]
    pub storage_location: Option<String>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(
    name = "books",
    auth_required,
    owner_field = "user_id",
    scoped_by = "books"
)]
pub struct Book {
    #[key]
    pub id: Uuid,
    pub user_id: Uuid,
    #[restricted(read = "internal")]
    pub score: Option<i32>,
}

fn access(read: Option<&str>, write: Option<&str>) -> Option<ColumnAccess> {
    Some(ColumnAccess {
        read: read.map(str::to_string),
        write: write.map(str::to_string),
    })
}

#[test]
fn restricted_is_emitted_and_derives_its_feature() {
    let listing = Listing::__cufflink_table_def();
    let declared: Vec<(&str, Option<ColumnAccess>)> = listing
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column.access.clone()))
        .collect();
    assert_eq!(
        declared,
        [
            ("id", None),
            ("customer_id", None),
            ("title", None),
            ("ops_notes", access(Some("internal"), Some("internal"))),
            ("status", access(None, Some("manage"))),
            ("storage_location", access(Some("internal"), None)),
        ]
    );
    assert_eq!(
        listing.derived_features().collect::<Vec<_>>(),
        ["column_permissions"]
    );
}

#[test]
fn a_scoped_table_restricts_columns_in_its_scope_area() {
    let book = Book::__cufflink_table_def();
    assert_eq!(
        book.permissions_of(&book.columns[2]),
        access(Some("books:internal"), None)
    );
    assert_eq!(
        book.derived_features().collect::<Vec<_>>(),
        ["scoped_permissions", "column_permissions"]
    );
}

#[test]
fn a_table_without_restricted_columns_serialises_no_access() {
    let mut listing = Listing::__cufflink_table_def();
    listing
        .columns
        .iter_mut()
        .for_each(|column| column.access = None);
    assert_eq!(listing.derived_features().count(), 0);
    assert!(!serde_json::to_string(&listing)
        .expect("serializes")
        .contains("access"));
}
