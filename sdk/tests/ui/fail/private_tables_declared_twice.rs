use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "loans", managed = false)]
pub struct Loan {
    #[key]
    pub id: Uuid,
}

cufflink::service! {
    name: "svc",
    private_tables: [Loan],
    private_tables: [Loan],
}

fn main() {}
