use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", check(name = "c", expr = "true; DROP TABLE loans"))]
pub struct Loan {
    #[key]
    pub id: Uuid,
}

fn main() {}
