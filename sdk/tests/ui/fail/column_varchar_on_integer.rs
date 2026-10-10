use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[column(varchar(10))]
    pub count: i32,
}

fn main() {}
