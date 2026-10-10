use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", permission_area = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[restricted(read = "internal")]
    #[restricted(write = "internal")]
    pub body: Option<String>,
}

fn main() {}
