mod catalog_docs {
    pub fn public_api_description() -> String {
        ["Products, prices", "and orders."].join(" ")
    }
}

cufflink::service! {
    name: "catalog-service",
    tables: [],
    mode: wasm,
    routing: strict,
    custom_routes: [
        ("GET", "/products", "list_products", public("catalog"), tags = ["Catalog"]),
        ("POST", "/orders", "place_order", public("checkout"), tags = ["Orders"], security = ["api_key"]),
        ("GET", "/status", "status", public("probe"), tags = ["Status"]),
        ("GET", "/audit", "audit", auth, tags = ["Admin"]),
    ],
    openapi: {
        title: "catalog-service",
        docs: [
            {
                name: "public-api",
                mount: "/docs/public-api",
                include_tags: ["Catalog", "Orders"],
                title: "Catalog Public API",
                version: "1.0.0",
                description: catalog_docs::public_api_description(),
                contact: { name: "API Support", email: "support@example.com" },
                servers: [("https://api.example.com", "Production")],
                tags: [("Catalog", "Products and prices"), ("Orders", "Placing and tracking orders")],
                path_prefix: "/api",
                api_key: { header: "Authorization", description: "The key issued to you, without a prefix" },
                cors: mirror,
            },
            {
                name: "status",
                mount: "/docs/status",
                include_tags: ["Status"],
                title: "Status",
                version: "1",
            },
        ],
    },
}
