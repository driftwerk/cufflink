use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", pagination = cursor)]
pub struct Loan {
    #[key]
    pub id: Uuid,
}

fn main() {}
