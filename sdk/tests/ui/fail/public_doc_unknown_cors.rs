cufflink::service! {
    name: "catalog-service",
    tables: [],
    routing: strict,
    openapi: {
        docs: [
            { name: "public-api", mount: "/docs/public-api", include_tags: ["Catalog"], title: "A", version: "1", cors: open },
        ],
    },
}

fn main() {}
