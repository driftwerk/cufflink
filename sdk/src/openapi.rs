//! JSON Schemas for typed routes, from `schemars`. The `service!` macro's
//! generated `main` collects every type a route documents into the
//! manifest's `components`.

pub use schemars;

use schemars::generate::SchemaSettings;
use schemars::JsonSchema;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// Collects the component schemas of the types routes document, refusing
/// two different types under one schema name (schemars names a generic
/// instantiation by its bare type name, so `Page<Book>` and `Page<Receipt>`
/// would otherwise collide).
#[derive(Debug, Default)]
pub struct SchemaCollector {
    components: BTreeMap<String, Collected>,
    conflict: Option<String>,
}

#[derive(Debug)]
struct Collected {
    schema: Value,
    origin: &'static str,
}

const COMPONENTS: &str = "#/components/schemas/";

impl SchemaCollector {
    pub fn new() -> Self {
        Self::default()
    }

    /// `T`'s schema, a `$ref` for a named type, with `T` and every type it
    /// uses registered as components.
    pub fn schema_for<T: JsonSchema + ?Sized>(&mut self) -> Value {
        self.collect::<T>(false)
    }

    /// `T`'s own schema, for flattening a `query = T` or `path = T` struct
    /// into parameters; only the types it uses are registered.
    pub fn inline_schema_for<T: JsonSchema + ?Sized>(&mut self) -> Value {
        self.collect::<T>(true)
    }

    fn collect<T: JsonSchema + ?Sized>(&mut self, inline: bool) -> Value {
        let mut generator = SchemaSettings::draft2020_12()
            .with(|settings| {
                settings.definitions_path = "/components/schemas".into();
                settings.meta_schema = None;
            })
            .into_generator();
        let schema = generator.subschema_for::<T>().to_value();
        let mut definitions = generator.take_definitions(true);
        let own = schema["$ref"]
            .as_str()
            .and_then(|reference| reference.strip_prefix(COMPONENTS))
            .filter(|_| inline)
            .and_then(|name| definitions.remove(name).map(|own| (name.to_string(), own)));
        let schema = match own {
            Some((name, own_schema)) => {
                if definitions
                    .values()
                    .any(|definition| refers_to(definition, &name))
                    || refers_to(&own_schema, &name)
                {
                    definitions.insert(name, own_schema.clone());
                }
                own_schema
            }
            None => schema,
        };
        let origin = std::any::type_name::<T>();
        definitions
            .into_iter()
            .for_each(|(name, definition)| self.add(name, normalize(definition), origin));
        normalize(schema)
    }

    fn add(&mut self, name: String, schema: Value, origin: &'static str) {
        match self.components.get(&name) {
            Some(existing) if existing.schema != schema => {
                self.conflict.get_or_insert_with(|| {
                    format!(
                        "schema name \"{name}\" is used by two different types (reached from {} \
                         and {origin}); give one of them #[schemars(rename = \"…\")]",
                        existing.origin
                    )
                });
            }
            Some(_) => {}
            None => {
                self.components.insert(name, Collected { schema, origin });
            }
        }
    }

    /// The collected components, or the first name two types shared.
    pub fn into_components(self) -> Result<BTreeMap<String, Value>, String> {
        match self.conflict {
            Some(conflict) => Err(conflict),
            None => Ok(self
                .components
                .into_iter()
                .map(|(name, collected)| (name, collected.schema))
                .collect()),
        }
    }
}

/// Whether any `$ref` in `value` names the component `name`.
fn refers_to(value: &Value, name: &str) -> bool {
    match value {
        Value::Object(map) => {
            map.get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix(COMPONENTS))
                == Some(name)
                || map.values().any(|nested| refers_to(nested, name))
        }
        Value::Array(items) => items.iter().any(|nested| refers_to(nested, name)),
        _ => false,
    }
}

/// Boolean subschemas as the objects OpenAPI tooling reads: `true` is
/// `{}`, `false` is `{"not": {}}`.
fn normalize(schema: Value) -> Value {
    match schema {
        Value::Bool(true) => json!({}),
        Value::Bool(false) => json!({ "not": {} }),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| {
                    let value = match key.as_str() {
                        "properties" | "patternProperties" | "dependentSchemas" | "$defs" => {
                            each_value(value)
                        }
                        "allOf" | "anyOf" | "oneOf" | "prefixItems" => each_item(value),
                        "items"
                        | "additionalProperties"
                        | "not"
                        | "if"
                        | "then"
                        | "else"
                        | "contains"
                        | "propertyNames"
                        | "unevaluatedProperties"
                        | "unevaluatedItems" => match value {
                            Value::Array(_) => each_item(value),
                            schema => normalize(schema),
                        },
                        _ => value,
                    };
                    (key, value)
                })
                .collect(),
        ),
        other => other,
    }
}

fn each_value(value: Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, schema)| (key, normalize(schema)))
                .collect::<Map<_, _>>(),
        ),
        other => other,
    }
}

fn each_item(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Loan {
        id: uuid::Uuid,
        count: u32,
        kind: Kind,
        parent: Option<Box<Loan>>,
        extra: serde_json::Value,
        maybe: Option<i32>,
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    enum Kind {
        Standard,
        Renewal,
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Page<T> {
        results: Vec<T>,
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Query {
        /// Page number.
        page: Option<i32>,
        kind: Option<Kind>,
    }

    mod other {
        #[derive(schemars::JsonSchema)]
        #[allow(dead_code)]
        pub struct Loan {
            pub title: String,
        }
    }

    #[test]
    fn named_types_become_components_referenced_by_ref() {
        let mut collector = SchemaCollector::new();
        assert_eq!(
            collector.schema_for::<Loan>(),
            json!({"$ref": "#/components/schemas/Loan"})
        );
        let components = collector.into_components().expect("components");
        let loan = &components["Loan"];
        assert_eq!(
            loan["properties"]["id"],
            json!({"type": "string", "format": "uuid"})
        );
        assert_eq!(
            loan["properties"]["kind"],
            json!({"$ref": "#/components/schemas/Kind"})
        );
        assert_eq!(
            loan["properties"]["extra"],
            json!({}),
            "a Value is any JSON"
        );
        assert_eq!(
            loan["properties"]["maybe"],
            json!({"type": ["integer", "null"], "format": "int32"})
        );
        assert_eq!(
            components["Kind"],
            json!({"type": "string", "enum": ["Standard", "Renewal"]})
        );
        assert!(loan.get("title").is_none() && loan.get("$schema").is_none());
    }

    #[test]
    fn references_are_found_by_structure_not_by_text() {
        assert!(refers_to(
            &json!({"items": [{"$ref": "#/components/schemas/Loan"}]}),
            "Loan"
        ));
        assert!(!refers_to(
            &json!({"description": "\"#/components/schemas/Loan\"", "$ref": "#/components/schemas/Loans"}),
            "Loan"
        ));
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct Tree {
        children: Vec<Tree>,
    }

    #[test]
    fn an_inlined_recursive_type_stays_a_component() {
        let mut collector = SchemaCollector::new();
        let tree = collector.inline_schema_for::<Tree>();
        assert_eq!(
            tree["properties"]["children"]["items"],
            json!({"$ref": "#/components/schemas/Tree"})
        );
        assert!(collector
            .into_components()
            .expect("components")
            .contains_key("Tree"));
    }

    #[test]
    fn inline_schemas_are_not_registered_but_their_types_are() {
        let mut collector = SchemaCollector::new();
        let query = collector.inline_schema_for::<Query>();
        assert_eq!(query["type"], "object");
        assert_eq!(query["properties"]["page"]["description"], "Page number.");
        let components = collector.into_components().expect("components");
        assert!(!components.contains_key("Query"));
        assert!(components.contains_key("Kind"));
    }

    #[test]
    fn the_same_type_twice_is_one_component_but_two_types_under_one_name_are_refused() {
        let mut collector = SchemaCollector::new();
        collector.schema_for::<Loan>();
        collector.schema_for::<Loan>();
        collector.schema_for::<Page<Loan>>();
        assert!(collector.into_components().is_ok());

        let mut collector = SchemaCollector::new();
        collector.schema_for::<Page<Loan>>();
        collector.schema_for::<Page<Kind>>();
        let err = collector.into_components().unwrap_err();
        assert!(
            err.starts_with("schema name \"Page\" is used by two different types"),
            "{err}"
        );

        let mut collector = SchemaCollector::new();
        collector.schema_for::<Loan>();
        collector.schema_for::<other::Loan>();
        assert!(collector.into_components().is_err());
    }
}
