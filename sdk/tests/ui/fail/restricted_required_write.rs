use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", permission_area = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[restricted(write = "internal")]
    pub body: String,
}

fn main() {}
