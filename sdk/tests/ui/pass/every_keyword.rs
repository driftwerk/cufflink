use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "loans", auth_required, permission_area = "loans", owner_field = "borrower_id")]
pub struct Loan {
    #[key]
    pub id: Uuid,
    pub borrower_id: Uuid,
    pub status: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "volumes", permission_area = "volumes")]
pub struct Volume {
    #[key]
    pub id: Uuid,
    pub book_id: Uuid,
}

cufflink::service! {
    name: "catalog-service",
    tables: [Loan, Volume],
    mode: wasm,
    custom_routes: [
        ("POST", "/stripe/webhook", "stripe_webhook", public("Stripe-signed webhook")),
        ("GET", "/me/loans", "my_loans", owner("borrower_id")),
        ("GET", "/loans/summary", "loans_summary", auth),
        ("POST", "/loans/export", "export_loans", perm("loans", "view"), job(timeout = 900, max_attempts = 1)),
        ("POST", "/loans/rebuild", "rebuild", job, perm("loans", "edit")),
        ("POST", "/loans/proxy", "proxy_loans", perm("loans", "edit"), forward_auth,),
        ("GET", "/legacy", "legacy"),
    ],
    subscriptions: [
        ("dw.{tenant}.fines.paid", "on_fine_paid", wasm),
        ("cufflink.tick.hourly", "sweep_stale", wasm, "CUFFLINK_TICKS"),
        ("dw.{tenant}.loans.deleted", "cascade", delete_cascade),
    ],
    authorization: [
        areas: [("loans", ["view", "create", "edit", "delete"]), ("volumes", ["view"])],
        default_roles: [
            ("librarian", "Front desk", ["loans:*", "volumes:view"]),
            ("admin", "Everything", ["*"]),
        ],
    ],
    on_migrate: "handle_on_migrate",
    publishes: ["dw.shared.loans.>"],
    graphql: true,
}
