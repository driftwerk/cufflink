use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", permission_area = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[restricted(read = "internal", read = "staff")]
    pub body: Option<String>,
}

fn main() {}
