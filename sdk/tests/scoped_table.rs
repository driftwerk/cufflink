use cufflink::prelude::*;

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
    pub branch_id: Uuid,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "loans", auth_required, permission_area = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
}

#[test]
fn scoped_by_is_emitted_and_derives_its_feature() {
    let book = Book::__cufflink_table_def();
    assert_eq!(book.scoped_by.as_deref(), Some("books"));
    assert_eq!(
        book.derived_features().collect::<Vec<_>>(),
        ["scoped_permissions"]
    );
    let loan = Loan::__cufflink_table_def();
    assert!(loan.scoped_by.is_none());
    assert_eq!(loan.derived_features().count(), 0);
}
