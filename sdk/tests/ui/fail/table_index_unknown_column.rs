use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", index(name = "loans_by_book", columns = ["book"]))]
pub struct Loan {
    #[key]
    pub id: Uuid,
    pub book_id: Uuid,
}

fn main() {}
