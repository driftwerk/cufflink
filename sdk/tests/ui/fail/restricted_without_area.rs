use cufflink::prelude::*;

#[derive(Table)]
#[table(name = "loans", auth_required)]
pub struct Loan {
    #[key]
    pub id: Uuid,
    #[restricted(read = "internal")]
    pub body: Option<String>,
}

fn main() {}
