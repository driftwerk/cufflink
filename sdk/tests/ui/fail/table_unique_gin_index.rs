use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", index(name = "i", columns = ["tags"], unique, using = "gin"))]
pub struct Loan {
    #[key]
    pub id: Uuid,
    pub tags: Vec<String>,
}

fn main() {}
