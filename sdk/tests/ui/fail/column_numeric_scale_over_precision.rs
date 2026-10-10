use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[column(numeric(2, 5))]
    pub fee: String,
}

fn main() {}
