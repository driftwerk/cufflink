include!("ui/pass/public_docs.rs");

#[test]
fn public_docs_reach_the_manifest_and_demand_their_feature() {
    let manifest = __cufflink_manifest().expect("manifest");
    assert_eq!(
        manifest.required_platform_features,
        ["public_docs", "route_matching", "route_posture"]
    );
    let wire = serde_json::to_value(&manifest).expect("serializes");
    assert_eq!(
        wire["openapi"]["docs"],
        serde_json::json!([
            {
                "name": "public-api",
                "mount": "/docs/public-api",
                "include_tags": ["Catalog", "Orders"],
                "info": {
                    "title": "Catalog Public API",
                    "version": "1.0.0",
                    "description": "Products, prices and orders.",
                    "contact": {"name": "API Support", "email": "support@example.com"}
                },
                "servers": [{"url": "https://api.example.com", "description": "Production"}],
                "tags": [
                    {"name": "Catalog", "description": "Products and prices"},
                    {"name": "Orders", "description": "Placing and tracking orders"}
                ],
                "path_prefix": "/api",
                "api_key": {
                    "header": "Authorization",
                    "description": "The key issued to you, without a prefix"
                },
                "cors": "mirror"
            },
            {
                "name": "status",
                "mount": "/docs/status",
                "include_tags": ["Status"],
                "info": {"title": "Status", "version": "1"}
            }
        ])
    );
    let document = cufflink::manifest::openapi::public_doc_spec(&manifest, "acme", "public-api")
        .expect("document");
    assert_eq!(
        document["paths"]
            .as_object()
            .map(|paths| paths.keys().cloned().collect::<Vec<_>>()),
        Some(vec!["/api/orders".to_string(), "/api/products".to_string()])
    );
}

#[allow(dead_code)]
mod duplicate_name {
    cufflink::service! {
        name: "catalog-service",
        tables: [],
        mode: wasm,
        routing: strict,
        custom_routes: [("GET", "/products", "list_products", public("catalog"), tags = ["Catalog"])],
        openapi: {
            docs: [
                { name: "public-api", mount: "/docs/a", include_tags: ["Catalog"], title: "A", version: "1" },
                { name: "public-api", mount: "/docs/b", include_tags: ["Catalog"], title: "B", version: "1" },
            ],
        },
    }

    #[test]
    fn the_emitted_main_refuses_two_documents_of_one_name() {
        assert_eq!(
            __cufflink_manifest().map(|_| ()),
            Err("two public documents are named 'public-api'".to_string())
        );
    }
}

#[allow(dead_code)]
mod legacy_routing {
    cufflink::service! {
        name: "catalog-service",
        tables: [],
        mode: wasm,
        custom_routes: [("GET", "/products", "list_products", public("catalog"), tags = ["Catalog"])],
        openapi: {
            docs: [{ name: "public-api", mount: "/docs/public-api", include_tags: ["Catalog"], title: "A", version: "1" }],
        },
    }

    #[test]
    fn the_emitted_main_refuses_documents_of_a_legacy_routing_service() {
        assert_eq!(
            __cufflink_manifest().map(|_| ()),
            Err("public document 'public-api' needs routing: strict".to_string())
        );
    }
}
