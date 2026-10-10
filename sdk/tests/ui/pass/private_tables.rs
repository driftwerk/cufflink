use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "loans", soft_delete)]
pub struct Loan {
    #[key]
    pub id: Uuid,
    pub book_id: Uuid,
    pub body: String,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "loan_tags")]
pub struct LoanTag {
    pub loan_id: Uuid,
    pub tag: String,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "labels")]
pub struct Label {
    #[key]
    pub id: Uuid,
    pub name: String,
}

cufflink::service! {
    name: "loan-service",
    schema: adopted,
    tables: [Label],
    private_tables: [Loan, LoanTag],
    mode: wasm,
    routing: strict,
    custom_routes: [
        ("GET", "/loans", "list_loans", auth),
        ("GET", "/loans/{id}", "get_loan", auth),
    ],
}
