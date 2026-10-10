use cufflink::prelude::*;

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "orders", auth_required, permission_area = "orders", owner_field = "customer_id")]
pub struct Order {
    #[key]
    pub id: Uuid,
    pub customer_id: Uuid,
    pub status: String,
    #[timestamp]
    pub created_at: DateTime<Utc>,
}

#[derive(Table, Serialize, Deserialize, Clone)]
#[table(name = "shipments", permission_area = "shipments")]
pub struct Shipment {
    #[key]
    pub id: Uuid,
    pub order_id: Uuid,
}

cufflink::service! {
    name: "orders-service",
    tables: [Order, Shipment],
    mode: wasm,
    custom_routes: [
        ("POST", "/stripe/webhook", "stripe_webhook", public("Stripe-signed webhook")),
        ("GET", "/me/orders", "my_orders", owner("customer_id")),
        ("GET", "/orders/summary", "orders_summary", auth),
        ("POST", "/orders/export", "export_orders", perm("orders", "view"), job(timeout = 900, max_attempts = 1)),
        ("POST", "/orders/rebuild", "rebuild", job, perm("orders", "edit")),
        ("POST", "/orders/proxy", "proxy_orders", perm("orders", "edit"), forward_auth,),
        ("GET", "/legacy", "legacy"),
    ],
    subscriptions: [
        ("dw.{tenant}.payments.succeeded", "on_payment", wasm),
        ("cufflink.tick.hourly", "sweep_stale", wasm, "CUFFLINK_TICKS"),
        ("dw.{tenant}.orders.deleted", "cascade", delete_cascade),
    ],
    authorization: [
        areas: [("orders", ["view", "create", "edit", "delete"]), ("shipments", ["view"])],
        default_roles: [
            ("staff", "Back office", ["orders:*", "shipments:view"]),
            ("admin", "Everything", ["*"]),
        ],
    ],
    on_migrate: "handle_on_migrate",
    publishes: ["dw.shared.orders.>"],
    graphql: true,
}
