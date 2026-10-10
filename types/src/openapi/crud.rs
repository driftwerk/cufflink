use super::schema::{column_to_openapi_type, schema_names};
use crate::TableDefinition;
use serde_json::{json, Value};

/// The operation ids of `table`'s list, create, get, update and delete.
pub(super) fn operation_ids(table: &TableDefinition) -> [String; 5] {
    let one = singular(&table.name);
    [
        format!("list_{}", table.name),
        format!("create_{one}"),
        format!("get_{one}"),
        format!("update_{one}"),
        format!("delete_{one}"),
    ]
}

/// `table`'s CRUD operations at `collection` and `item`.
pub(super) fn table_paths(
    base_path: String,
    item_path: String,
    table: &TableDefinition,
) -> [(String, Value); 2] {
    let [list_id, create_id, get_id, update_id, delete_id] = operation_ids(table);
    let (schema_name, create_schema_name) = schema_names(table);
    [
        (
            base_path,
            json!({
                "get": {
                    "summary": format!("List {}", table.name),
                    "operationId": list_id,
                    "tags": [table.name],
                    "parameters": list_parameters(table),
                    "responses": {
                        "200": {
                            "description": "Paginated list",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "results": {
                                                "type": "array",
                                                "items": { "$ref": format!("#/components/schemas/{}", schema_name) }
                                            },
                                            "pagination": { "$ref": "#/components/schemas/Pagination" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "post": {
                    "summary": format!("Create {}", singular(&table.name)),
                    "operationId": create_id,
                    "tags": [table.name],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": format!("#/components/schemas/{}", create_schema_name) }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Created",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", schema_name) }
                                }
                            }
                        }
                    }
                }
            }),
        ),
        (
            item_path,
            json!({
                "get": {
                    "summary": format!("Get {} by ID", singular(&table.name)),
                    "operationId": get_id,
                    "tags": [table.name],
                    "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}}],
                    "responses": {
                        "200": {
                            "description": "Found",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", schema_name) }
                                }
                            }
                        },
                        "404": { "description": "Not found" }
                    }
                },
                "put": {
                    "summary": format!("Update {}", singular(&table.name)),
                    "operationId": update_id,
                    "tags": [table.name],
                    "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}}],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": { "$ref": format!("#/components/schemas/{}", create_schema_name) }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Updated",
                            "content": {
                                "application/json": {
                                    "schema": { "$ref": format!("#/components/schemas/{}", schema_name) }
                                }
                            }
                        }
                    }
                },
                "delete": {
                    "summary": format!("Delete {}", singular(&table.name)),
                    "operationId": delete_id,
                    "tags": [table.name],
                    "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "string", "format": "uuid"}}],
                    "responses": {
                        "200": { "description": "Deleted" }
                    }
                }
            }),
        ),
    ]
}

fn list_parameters(table: &TableDefinition) -> Vec<Value> {
    let mut params = vec![
        json!({"name": "page", "in": "query", "schema": {"type": "integer"}, "description": "Page number (default: 1)"}),
        json!({"name": "per_page", "in": "query", "schema": {"type": "integer"}, "description": "Items per page (default: 50, max: 200)"}),
        json!({"name": "sort", "in": "query", "schema": {"type": "string"}, "description": "Sort by column (prefix - for DESC)"}),
        json!({"name": "filter", "in": "query", "schema": {"type": "string"}, "description": "JSON filter array"}),
    ];

    for col in &table.columns {
        params.push(json!({
            "name": col.name,
            "in": "query",
            "schema": column_to_openapi_type(&col.column_type),
            "description": format!("Filter by {}", col.name)
        }));
    }

    params
}

fn singular(s: &str) -> String {
    if s.ends_with('s') && s.len() > 1 {
        s[..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}
