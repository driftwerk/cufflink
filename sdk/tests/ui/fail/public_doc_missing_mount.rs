cufflink::service! {
    name: "catalog-service",
    tables: [],
    routing: strict,
    openapi: {
        docs: [
            { name: "public-api", include_tags: ["Catalog"], title: "A", version: "1" },
        ],
    },
}

fn main() {}
