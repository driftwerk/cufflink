use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", permission_area = "loans", search = ["body"])]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[restricted(read = "internal")]
    pub body: Option<String>,
}

fn main() {}
