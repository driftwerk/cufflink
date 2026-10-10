use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", permission_area = "loans")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[unique]
    #[restricted(read = "internal")]
    pub reference: Option<String>,
}

fn main() {}
