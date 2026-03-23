use serde::Deserialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{Display, Formatter, Write};

#[derive(Debug)]
pub enum CodegenError {
    Parse(serde_json::Error),
    Invalid(String),
}

impl Display for CodegenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parse(error) => write!(f, "schema parse failed: {error}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl Error for CodegenError {}

impl From<serde_json::Error> for CodegenError {
    fn from(value: serde_json::Error) -> Self {
        Self::Parse(value)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectSpec {
    #[serde(default)]
    pub module: Option<String>,
    #[serde(default)]
    pub filters: Vec<FilterSpec>,
    #[serde(default)]
    pub resolvers: Vec<ResolverSpec>,
    pub collections: Vec<CollectionSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterSpec {
    pub name: String,
    pub decoded_type: String,
    pub encoded_type: String,
    pub encode: String,
    pub decode: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolverSpec {
    pub name: String,
    pub output_type: String,
    pub resolve: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CollectionSpec {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default = "default_true")]
    pub document_security: bool,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default = "default_id_max_length")]
    pub id_max_length: usize,
    #[serde(default)]
    pub indexes: Vec<IndexSpec>,
    pub attributes: Vec<AttributeSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexSpec {
    pub id: String,
    pub kind: IndexKindSpec,
    pub attributes: Vec<String>,
    #[serde(default)]
    pub orders: Vec<OrderSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributeSpec {
    pub id: String,
    #[serde(default)]
    pub column: Option<String>,
    pub kind: AttributeKindSpec,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub array: bool,
    #[serde(default)]
    pub length: Option<usize>,
    #[serde(default)]
    pub filters: Vec<String>,
    #[serde(default)]
    pub resolver: Option<String>,
    #[serde(default)]
    pub relationship: Option<RelationshipSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipSpec {
    pub related_collection: String,
    #[serde(default)]
    pub kind: RelationshipKindSpec,
    #[serde(default)]
    pub side: RelationshipSideSpec,
    #[serde(default)]
    pub two_way: bool,
    pub two_way_key: Option<String>,
    #[serde(default)]
    pub on_delete: OnDeleteActionSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RelationshipKindSpec {
    #[default]
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RelationshipSideSpec {
    #[default]
    Parent,
    Child,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OnDeleteActionSpec {
    SetNull,
    Cascade,
    #[default]
    Restrict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttributeKindSpec {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Relationship,
    Virtual,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexKindSpec {
    Key,
    Unique,
    FullText,
    Spatial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSpec {
    Asc,
    Desc,
    None,
}

fn default_true() -> bool {
    true
}

fn default_id_max_length() -> usize {
    255
}

pub fn parse_project_spec(input: &str) -> Result<ProjectSpec, CodegenError> {
    serde_json::from_str(input).map_err(CodegenError::from)
}

pub fn validate_project_spec(spec: &ProjectSpec) -> Result<(), CodegenError> {
    if spec.collections.is_empty() {
        return Err(CodegenError::Invalid(
            "project must define at least one collection".into(),
        ));
    }

    let mut filter_names = BTreeSet::new();
    for filter in &spec.filters {
        if filter.name.trim().is_empty() {
            return Err(CodegenError::Invalid("filter name cannot be empty".into()));
        }

        if filter.decoded_type.trim().is_empty() || filter.encoded_type.trim().is_empty() {
            return Err(CodegenError::Invalid(format!(
                "filter '{}' must declare decodedType and encodedType",
                filter.name
            )));
        }

        if filter.encode.trim().is_empty() || filter.decode.trim().is_empty() {
            return Err(CodegenError::Invalid(format!(
                "filter '{}' must declare encode and decode function paths",
                filter.name
            )));
        }

        if !filter_names.insert(filter.name.as_str()) {
            return Err(CodegenError::Invalid(format!(
                "duplicate filter name '{}'",
                filter.name
            )));
        }
    }

    let filters_by_name: BTreeMap<&str, &FilterSpec> = spec
        .filters
        .iter()
        .map(|filter| (filter.name.as_str(), filter))
        .collect();
    let mut resolver_names = BTreeSet::new();
    for resolver in &spec.resolvers {
        if resolver.name.trim().is_empty() {
            return Err(CodegenError::Invalid(
                "resolver name cannot be empty".into(),
            ));
        }
        if resolver.output_type.trim().is_empty() || resolver.resolve.trim().is_empty() {
            return Err(CodegenError::Invalid(format!(
                "resolver '{}' must declare outputType and resolve function path",
                resolver.name
            )));
        }
        if !resolver_names.insert(resolver.name.as_str()) {
            return Err(CodegenError::Invalid(format!(
                "duplicate resolver name '{}'",
                resolver.name
            )));
        }
    }
    let resolvers_by_name: BTreeMap<&str, &ResolverSpec> = spec
        .resolvers
        .iter()
        .map(|resolver| (resolver.name.as_str(), resolver))
        .collect();
    let mut collection_ids = BTreeSet::new();

    for collection in &spec.collections {
        if collection.id.trim().is_empty() {
            return Err(CodegenError::Invalid(
                "collection id cannot be empty".into(),
            ));
        }

        if collection.name.trim().is_empty() {
            return Err(CodegenError::Invalid(format!(
                "collection '{}' name cannot be empty",
                collection.id
            )));
        }

        if !collection_ids.insert(collection.id.as_str()) {
            return Err(CodegenError::Invalid(format!(
                "duplicate collection id '{}'",
                collection.id
            )));
        }

        if collection.id_max_length == 0 {
            return Err(CodegenError::Invalid(format!(
                "collection '{}' idMaxLength must be greater than zero",
                collection.id
            )));
        }

        let mut attribute_ids = BTreeSet::new();
        let mut columns = BTreeSet::new();

        for attribute in &collection.attributes {
            if attribute.id.trim().is_empty() {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}' has an attribute with an empty id",
                    collection.id
                )));
            }

            if attribute.id == "id" {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}': attribute id 'id' is reserved",
                    collection.id
                )));
            }

            if !attribute_ids.insert(attribute.id.as_str()) {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}' has duplicate attribute '{}'",
                    collection.id, attribute.id
                )));
            }

            if attribute.kind == AttributeKindSpec::Virtual {
                if !attribute.filters.is_empty() {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': virtual attribute '{}' cannot declare filters",
                        collection.id, attribute.id
                    )));
                }
                if attribute.required {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': virtual attribute '{}' cannot be required",
                        collection.id, attribute.id
                    )));
                }
                resolve_attribute_resolver(&resolvers_by_name, collection, attribute)?;
            } else {
                if attribute.resolver.is_some() {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': non-virtual attribute '{}' cannot declare a resolver",
                        collection.id, attribute.id
                    )));
                }

                let column = attribute.column.as_deref().unwrap_or(&attribute.id);
                if !columns.insert(column) {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}' has duplicate column '{}'",
                        collection.id, column
                    )));
                }
            }

            if !attribute.filters.is_empty() {
                let chain = resolve_attribute_filters(&filters_by_name, collection, attribute)?;
                let storage_type = storage_field_base_type(attribute.kind, attribute.array);

                if let Some(last) = chain.last() {
                    if last.encoded_type != storage_type {
                        return Err(CodegenError::Invalid(format!(
                            "collection '{}': attribute '{}' filter chain stores '{}', expected '{}'",
                            collection.id, attribute.id, last.encoded_type, storage_type
                        )));
                    }
                }
            }
        }

        let mut index_ids = BTreeSet::new();
        for index in &collection.indexes {
            if index.id.trim().is_empty() {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}' has an index with an empty id",
                    collection.id
                )));
            }

            if !index_ids.insert(index.id.as_str()) {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}' has duplicate index '{}'",
                    collection.id, index.id
                )));
            }

            if index.attributes.is_empty() {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}': index '{}' must reference at least one attribute",
                    collection.id, index.id
                )));
            }

            if !index.orders.is_empty() && index.orders.len() != index.attributes.len() {
                return Err(CodegenError::Invalid(format!(
                    "collection '{}': index '{}' orders length must match attributes length",
                    collection.id, index.id
                )));
            }

            for attribute_id in &index.attributes {
                let Some(attribute) = collection
                    .attributes
                    .iter()
                    .find(|attribute| attribute.id == *attribute_id)
                else {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': index '{}' references unknown attribute '{}'",
                        collection.id, index.id, attribute_id
                    )));
                };

                if attribute.kind == AttributeKindSpec::Virtual {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': index '{}' cannot reference virtual attribute '{}'",
                        collection.id, index.id, attribute_id
                    )));
                }
            }
        }
    }

    Ok(())
}

pub fn generate_from_json(input: &str) -> Result<String, CodegenError> {
    let spec = parse_project_spec(input)?;
    generate(&spec)
}

pub fn generate(spec: &ProjectSpec) -> Result<String, CodegenError> {
    validate_project_spec(spec)?;

    let mut out = String::new();
    let _needs_take_optional = spec
        .collections
        .iter()
        .flat_map(|collection| collection.attributes.iter())
        .any(|attribute| !attribute.required && attribute.kind != AttributeKindSpec::Virtual);
    let needs_encoded_field = spec.collections.iter().any(|collection| {
        collection
            .attributes
            .iter()
            .any(|attribute| !attribute.filters.is_empty())
    });
    let module_name = spec
        .module
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("generated_models");

    writeln!(&mut out, "// @generated by database-cli.").unwrap();
    writeln!(&mut out, "// Do not edit by hand.").unwrap();
    writeln!(&mut out).unwrap();
    writeln!(&mut out, "#[allow(dead_code)]").unwrap();
    writeln!(&mut out, "#[allow(unused_imports)]").unwrap();
    writeln!(&mut out, "pub mod {module_name} {{").unwrap();
    writeln!(&mut out, "    use nx_db::traits::storage::StorageRecord;").unwrap();
    let needs_model_future = spec.collections.iter().any(|collection| {
        collection
            .attributes
            .iter()
            .any(|attribute| attribute.kind == AttributeKindSpec::Virtual)
    });
    let mut optional_imports = String::new();
    if needs_encoded_field {
        optional_imports.push_str("EncodedField, ");
    }
    if needs_model_future {
        optional_imports.push_str("ModelFuture, ");
    }

    writeln!(
        &mut out,
        "    use nx_db::{{insert_value, take_optional, take_required, get_optional, get_required, AttributeKind, AttributePersistence, AttributeSchema, CollectionSchema, Context, DatabaseError, {}Field, Key, Model, Patch, QuerySpec, RelationshipKind, RelationshipSchema, RelationshipSide, StaticRegistry, FIELD_ID, FIELD_SEQUENCE, FIELD_CREATED_AT, FIELD_UPDATED_AT, FIELD_PERMISSIONS}};",
        optional_imports
    )
    .unwrap();
    writeln!(&mut out).unwrap();

    for collection in &spec.collections {
        emit_collection(&mut out, spec, collection)?;
    }

    writeln!(
        &mut out,
        "    pub fn registry() -> Result<StaticRegistry, DatabaseError> {{"
    )
    .unwrap();
    writeln!(&mut out, "        let registry = StaticRegistry::new()").unwrap();
    for collection in &spec.collections {
        let const_name = format!("{}_SCHEMA", screaming_snake(&collection.id));
        writeln!(&mut out, "            .register(&{const_name})?").unwrap();
    }
    writeln!(&mut out, "            ;").unwrap();
    writeln!(&mut out, "        Ok(registry)").unwrap();
    writeln!(&mut out, "    }}").unwrap();
    writeln!(&mut out, "}}").unwrap();

    Ok(out)
}

fn emit_collection(
    out: &mut String,
    spec: &ProjectSpec,
    collection: &CollectionSpec,
) -> Result<(), CodegenError> {
    let model_name = collection
        .model
        .clone()
        .unwrap_or_else(|| default_model_name(collection));
    let entity_name = format!("{model_name}Entity");
    let create_name = format!("Create{model_name}");
    let update_name = format!("Update{model_name}");
    let id_name = format!("{model_name}Id");
    let const_base = screaming_snake(&collection.id);
    let model_const = screaming_snake(&model_name);
    let schema_const = format!("{const_base}_SCHEMA");
    let attrs_const = format!("{const_base}_ATTRIBUTES");
    let indexes_const = format!("{const_base}_INDEXES");
    let resolvers_by_name: BTreeMap<&str, &ResolverSpec> = spec
        .resolvers
        .iter()
        .map(|resolver| (resolver.name.as_str(), resolver))
        .collect();

    writeln!(
        out,
        "    pub type {id_name} = Key<{}>;",
        collection.id_max_length
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(
        out,
        "    #[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]"
    )
    .unwrap();
    writeln!(out, "    pub struct {entity_name} {{").unwrap();
    writeln!(out, "        pub id: {id_name},").unwrap();
    for attribute in &collection.attributes {
        let field_type = entity_field_type(spec, collection, attribute)?;
        writeln!(
            out,
            "        pub {}: {},",
            rust_field_name(&attribute.id),
            field_type
        )
        .unwrap();
    }
    writeln!(out, "        pub _metadata: nx_db::Metadata,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "    #[derive(Debug, Clone)]").unwrap();
    writeln!(out, "    pub struct {create_name} {{").unwrap();
    writeln!(out, "        pub id: {id_name},").unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual {
            continue;
        }
        let field_type = entity_field_type(spec, collection, attribute)?;
        writeln!(
            out,
            "        pub {}: {},",
            rust_field_name(&attribute.id),
            field_type
        )
        .unwrap();
    }
    writeln!(out, "        pub permissions: Vec<String>,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "    #[derive(Debug, Clone, Default)]").unwrap();
    writeln!(out, "    pub struct {update_name} {{").unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual {
            continue;
        }
        let field_type = entity_field_type(spec, collection, attribute)?;
        writeln!(
            out,
            "        pub {}: Patch<{}>,",
            rust_field_name(&attribute.id),
            field_type
        )
        .unwrap();
    }
    writeln!(out, "        pub permissions: Patch<Vec<String>>,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "    #[derive(Debug, Clone, Copy)]").unwrap();
    writeln!(out, "    pub struct {model_name};").unwrap();
    writeln!(
        out,
        "    pub const {model_const}: {model_name} = {model_name};"
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(
        out,
        "    pub const {model_const}_ID: Field<{model_name}, {id_name}> = Field::new(FIELD_ID);"
    )
    .unwrap();
    let filters_by_name: BTreeMap<&str, &FilterSpec> = spec
        .filters
        .iter()
        .map(|filter| (filter.name.as_str(), filter))
        .collect();
    for attribute in &collection.attributes {
        let const_name = format!("{model_const}_{}", screaming_snake(&attribute.id));
        if attribute.filters.is_empty() {
            let query_type = query_field_type(attribute);
            if attribute.kind == AttributeKindSpec::Virtual {
                continue;
            }
            writeln!(
                out,
                "    pub const {const_name}: Field<{model_name}, {}> = Field::new(\"{}\");",
                query_type, attribute.id
            )
            .unwrap();
        } else {
            let public_type = filtered_query_field_type(&filters_by_name, collection, attribute)?;
            writeln!(
                out,
                "    pub const {const_name}: EncodedField<{model_name}, {}> = EncodedField::new(\"{}\", encode_query_{}_{});",
                public_type,
                attribute.id,
                rust_field_name(&model_name),
                rust_field_name(&attribute.id)
            )
            .unwrap();
        }
    }
    writeln!(out).unwrap();

    for attribute in &collection.attributes {
        if attribute.filters.is_empty() {
            continue;
        }

        let helpers = attribute_filter_helpers(&filters_by_name, collection, attribute)?;
        let encode_fn = encode_helper_name(&model_name, &attribute.id);
        let decode_fn = decode_helper_name(&model_name, &attribute.id);
        let query_encode_fn = query_encode_helper_name(&model_name, &attribute.id);

        writeln!(
            out,
            "    fn {encode_fn}(value: {}) -> Result<{}, DatabaseError> {{",
            helpers.public_type, helpers.storage_type
        )
        .unwrap();
        if attribute.required {
            for filter in &helpers.chain {
                writeln!(out, "        let value = {}(value)?;", filter.encode).unwrap();
            }
            writeln!(out, "        Ok(value)").unwrap();
        } else {
            writeln!(out, "        if let Some(value) = value {{").unwrap();
            for filter in &helpers.chain {
                writeln!(out, "            let value = {}(value)?;", filter.encode).unwrap();
            }
            writeln!(out, "            Ok(Some(value))").unwrap();
            writeln!(out, "        }} else {{").unwrap();
            writeln!(out, "            Ok(None)").unwrap();
            writeln!(out, "        }}").unwrap();
        }
        writeln!(out, "    }}").unwrap();
        writeln!(out).unwrap();

        writeln!(
            out,
            "    fn {query_encode_fn}(value: {}) -> Result<nx_db::traits::storage::StorageValue, DatabaseError> {{",
            helpers.public_type
        )
        .unwrap();
        if attribute.required {
            writeln!(
                out,
                "        Ok(nx_db::IntoStorage::into_storage({encode_fn}(value)?))"
            )
            .unwrap();
        } else {
            writeln!(out, "        if let Some(value) = value {{").unwrap();
            writeln!(
                out,
                "            Ok(nx_db::IntoStorage::into_storage({}(value)?))",
                helpers.chain.last().unwrap().encode
            )
            .unwrap();
            writeln!(out, "        }} else {{").unwrap();
            writeln!(
                out,
                "            Ok(nx_db::traits::storage::StorageValue::Null)"
            )
            .unwrap();
            writeln!(out, "        }}").unwrap();
        }
        writeln!(out, "    }}").unwrap();
        writeln!(out).unwrap();

        writeln!(
            out,
            "    fn {decode_fn}(value: {}) -> Result<{}, DatabaseError> {{",
            helpers.storage_type, helpers.public_type
        )
        .unwrap();
        if attribute.required {
            for filter in helpers.chain.iter().rev() {
                writeln!(out, "        let value = {}(value)?;", filter.decode).unwrap();
            }
            writeln!(out, "        Ok(value)").unwrap();
        } else {
            writeln!(out, "        if let Some(value) = value {{").unwrap();
            for filter in helpers.chain.iter().rev() {
                writeln!(out, "            let value = {}(value)?;", filter.decode).unwrap();
            }
            writeln!(out, "            Ok(Some(value))").unwrap();
            writeln!(out, "        }} else {{").unwrap();
            writeln!(out, "            Ok(None)").unwrap();
            writeln!(out, "        }}").unwrap();
        }
        writeln!(out, "    }}").unwrap();
        writeln!(out).unwrap();
    }

    writeln!(out, "    const {attrs_const}: &[AttributeSchema] = &[").unwrap();
    for attribute in &collection.attributes {
        writeln!(out, "        AttributeSchema {{").unwrap();
        writeln!(out, "            id: \"{}\",", attribute.id).unwrap();
        writeln!(
            out,
            "            column: \"{}\",",
            attribute_column(attribute)
        )
        .unwrap();
        writeln!(
            out,
            "            kind: {},",
            attribute_kind_expr(attribute.kind)
        )
        .unwrap();
        writeln!(out, "            required: {},", attribute.required).unwrap();
        writeln!(out, "            array: {},", attribute.array).unwrap();
        writeln!(
            out,
            "            length: {},",
            attribute
                .length
                .map(|l| format!("Some({})", l))
                .unwrap_or_else(|| "None".to_string())
        )
        .unwrap();
        writeln!(out, "            default: None,").unwrap();
        writeln!(
            out,
            "            persistence: AttributePersistence::{},",
            if attribute.kind == AttributeKindSpec::Virtual {
                "Virtual"
            } else {
                "Persisted"
            }
        )
        .unwrap();
        writeln!(
            out,
            "            filters: &[{}],",
            attribute
                .filters
                .iter()
                .map(|value| format!("\"{}\"", escape_string(value)))
                .collect::<Vec<_>>()
                .join(", ")
        )
        .unwrap();
        if let Some(rel) = &attribute.relationship {
            writeln!(
                out,
                "            relationship: Some(nx_db::RelationshipSchema {{"
            )
            .unwrap();
            writeln!(
                out,
                "                related_collection: \"{}\",",
                rel.related_collection
            )
            .unwrap();
            writeln!(
                out,
                "                kind: {},",
                relationship_kind_expr(rel.kind)
            )
            .unwrap();
            writeln!(
                out,
                "                side: {},",
                relationship_side_expr(rel.side)
            )
            .unwrap();
            writeln!(out, "                two_way: {},", rel.two_way).unwrap();
            writeln!(
                out,
                "                two_way_key: {},",
                rel.two_way_key
                    .as_ref()
                    .map(|k| format!("Some(\"{}\")", escape_string(k)))
                    .unwrap_or_else(|| "None".to_string())
            )
            .unwrap();
            writeln!(
                out,
                "                on_delete: {},",
                on_delete_action_expr(rel.on_delete)
            )
            .unwrap();
            writeln!(out, "            }}),").unwrap();
        } else {
            writeln!(out, "            relationship: None,").unwrap();
        }
        writeln!(out, "        }},").unwrap();
    }
    writeln!(out, "    ];").unwrap();
    writeln!(out, "    const {indexes_const}: &[nx_db::IndexSchema] = &[").unwrap();
    for index in &collection.indexes {
        writeln!(out, "        nx_db::IndexSchema {{").unwrap();
        writeln!(out, "            id: \"{}\",", escape_string(&index.id)).unwrap();
        writeln!(out, "            kind: {},", index_kind_expr(index.kind)).unwrap();
        writeln!(
            out,
            "            attributes: &[{}],",
            index
                .attributes
                .iter()
                .map(|value| format!("\"{}\"", escape_string(value)))
                .collect::<Vec<_>>()
                .join(", ")
        )
        .unwrap();
        writeln!(
            out,
            "            orders: &[{}],",
            index
                .orders
                .iter()
                .map(|order| order_expr(*order))
                .collect::<Vec<_>>()
                .join(", ")
        )
        .unwrap();
        writeln!(out, "        }},").unwrap();
    }
    writeln!(out, "    ];").unwrap();
    writeln!(out, "    pub static {schema_const}: CollectionSchema = CollectionSchema {{ id: \"{}\", name: \"{}\", document_security: {}, enabled: true, permissions: &[{}], attributes: {attrs_const}, indexes: {indexes_const} }};",
        collection.id, escape_string(&collection.name), collection.document_security,
        collection.permissions.iter().map(|p| format!("\"{}\"", escape_string(p))).collect::<Vec<_>>().join(", ")
    ).unwrap();

    writeln!(out, "    impl {model_name} {{").unwrap();
    writeln!(
        out,
        "        pub const ID: Field<{model_name}, {id_name}> = Field::new(FIELD_ID);"
    )
    .unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual {
            continue;
        }
        let const_name = screaming_snake(&attribute.id);
        if attribute.filters.is_empty() {
            writeln!(
                out,
                "        pub const {const_name}: Field<{model_name}, {}> = Field::new(\"{}\");",
                query_field_type(attribute),
                attribute.id
            )
            .unwrap();
        } else {
            let public_type = filtered_query_field_type(&filters_by_name, collection, attribute)?;
            writeln!(out, "        pub const {const_name}: EncodedField<{model_name}, {}> = EncodedField::new(\"{}\", encode_query_{}_{});",
                public_type, attribute.id, rust_field_name(&model_name), rust_field_name(&attribute.id)).unwrap();
        }
    }
    writeln!(out, "    }}").unwrap();

    let mut attribute_lines = Vec::new();
    let mut virtual_lines = Vec::new();
    let mut resolver_lines = Vec::new();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual {
            let field = rust_field_name(&attribute.id);
            virtual_lines.push(field.clone());
            let resolver = resolve_attribute_resolver(&resolvers_by_name, collection, attribute)?;
            resolver_lines.push(format!("{} : {}", field, resolver.resolve));
            continue;
        }
        let field_id = &attribute.id;
        let field_name = rust_field_name(&attribute.id);
        let storage_type = query_field_type(attribute);
        let required_flag = if attribute.required { " :required" } else { "" };
        if let Some(decoder) = attribute
            .filters
            .first()
            .map(|_| decode_helper_name(&model_name, &attribute.id))
        {
            let encoder = encode_helper_name(&model_name, &attribute.id);
            attribute_lines.push(format!(
                "                \"{}\" => {} : {} [{}, {}]{}",
                field_id, field_name, storage_type, encoder, decoder, required_flag
            ));
        } else {
            attribute_lines.push(format!(
                "                \"{}\" => {} : {}{}",
                field_id, field_name, storage_type, required_flag
            ));
        }
    }

    if virtual_lines.is_empty() {
        writeln!(out, "    nx_db::impl_model! {{ name: {}, id: {}, entity: {}, create: {}, update: {}, schema: {}, fields: {{ {} }} }}",
            model_name, id_name, entity_name, create_name, update_name, schema_const, attribute_lines.join(", ")
        ).unwrap();
    } else {
        writeln!(out, "    nx_db::impl_model! {{ name: {}, id: {}, entity: {}, create: {}, update: {}, schema: {}, fields: {{ {} }}, virtuals: {{ {} }}, resolvers: {{ {} }} }}",
            model_name, id_name, entity_name, create_name, update_name, schema_const, attribute_lines.join(", "), virtual_lines.join(", "), resolver_lines.join(", ")
        ).unwrap();
    }

    Ok(())
}

fn default_model_name(collection: &CollectionSpec) -> String {
    let base = pascal_case(&collection.id);

    if let Some(prefix) = base.strip_suffix("ies") {
        format!("{prefix}y")
    } else if base.len() > 1 && base.ends_with('s') && !base.ends_with("ss") {
        base[..base.len() - 1].to_string()
    } else {
        base
    }
}

fn attribute_kind_expr(kind: AttributeKindSpec) -> &'static str {
    match kind {
        AttributeKindSpec::String => "AttributeKind::String",
        AttributeKindSpec::Integer => "AttributeKind::Integer",
        AttributeKindSpec::Float => "AttributeKind::Float",
        AttributeKindSpec::Boolean => "AttributeKind::Boolean",
        AttributeKindSpec::Timestamp => "AttributeKind::Timestamp",
        AttributeKindSpec::Relationship => "AttributeKind::Relationship",
        AttributeKindSpec::Virtual => "AttributeKind::Virtual",
        AttributeKindSpec::Json => "AttributeKind::Json",
    }
}

fn index_kind_expr(kind: IndexKindSpec) -> &'static str {
    match kind {
        IndexKindSpec::Key => "nx_db::IndexKind::Key",
        IndexKindSpec::Unique => "nx_db::IndexKind::Unique",
        IndexKindSpec::FullText => "nx_db::IndexKind::FullText",
        IndexKindSpec::Spatial => "nx_db::IndexKind::Spatial",
    }
}

fn order_expr(order: OrderSpec) -> &'static str {
    match order {
        OrderSpec::Asc => "nx_db::Order::Asc",
        OrderSpec::Desc => "nx_db::Order::Desc",
        OrderSpec::None => "nx_db::Order::None",
    }
}

fn relationship_kind_expr(kind: RelationshipKindSpec) -> &'static str {
    match kind {
        RelationshipKindSpec::OneToOne => "nx_db::RelationshipKind::OneToOne",
        RelationshipKindSpec::OneToMany => "nx_db::RelationshipKind::OneToMany",
        RelationshipKindSpec::ManyToOne => "nx_db::RelationshipKind::ManyToOne",
        RelationshipKindSpec::ManyToMany => "nx_db::RelationshipKind::ManyToMany",
    }
}

fn relationship_side_expr(side: RelationshipSideSpec) -> &'static str {
    match side {
        RelationshipSideSpec::Parent => "nx_db::RelationshipSide::Parent",
        RelationshipSideSpec::Child => "nx_db::RelationshipSide::Child",
    }
}

fn on_delete_action_expr(action: OnDeleteActionSpec) -> &'static str {
    match action {
        OnDeleteActionSpec::SetNull => "nx_db::OnDeleteAction::SetNull",
        OnDeleteActionSpec::Cascade => "nx_db::OnDeleteAction::Cascade",
        OnDeleteActionSpec::Restrict => "nx_db::OnDeleteAction::Restrict",
    }
}

fn storage_field_base_type(kind: AttributeKindSpec, array: bool) -> String {
    let base = match kind {
        AttributeKindSpec::String | AttributeKindSpec::Relationship => "String",
        AttributeKindSpec::Integer => "i64",
        AttributeKindSpec::Float => "f64",
        AttributeKindSpec::Boolean => "bool",
        AttributeKindSpec::Timestamp => "nx_db::time::OffsetDateTime",
        AttributeKindSpec::Virtual | AttributeKindSpec::Json => "String",
    };

    if array {
        format!("Vec<{base}>")
    } else {
        base.to_string()
    }
}

fn attribute_column(attribute: &AttributeSpec) -> &str {
    if attribute.kind == AttributeKindSpec::Virtual {
        ""
    } else {
        attribute.column.as_deref().unwrap_or(&attribute.id)
    }
}

fn entity_field_type(
    spec: &ProjectSpec,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<String, CodegenError> {
    if attribute.kind == AttributeKindSpec::Virtual {
        let resolvers_by_name: BTreeMap<&str, &ResolverSpec> = spec
            .resolvers
            .iter()
            .map(|resolver| (resolver.name.as_str(), resolver))
            .collect();
        let resolver = resolve_attribute_resolver(&resolvers_by_name, collection, attribute)?;
        return Ok(format!("Option<{}>", resolver.output_type));
    }

    let base = if attribute.filters.is_empty() {
        storage_field_base_type(attribute.kind, attribute.array)
    } else {
        let filters_by_name: BTreeMap<&str, &FilterSpec> = spec
            .filters
            .iter()
            .map(|filter| (filter.name.as_str(), filter))
            .collect();
        let helpers = attribute_filter_helpers(&filters_by_name, collection, attribute)?;
        helpers.public_type
    };

    if attribute.required {
        Ok(base)
    } else if attribute.filters.is_empty() {
        Ok(format!("Option<{base}>"))
    } else {
        // base already contains Option if needed because it came from attribute_filter_helpers
        Ok(base)
    }
}

fn query_field_type(attribute: &AttributeSpec) -> String {
    let base = storage_field_base_type(attribute.kind, attribute.array);
    if attribute.required {
        base
    } else {
        format!("Option<{base}>")
    }
}

fn filtered_query_field_type<'a>(
    filters_by_name: &BTreeMap<&'a str, &'a FilterSpec>,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<String, CodegenError> {
    let helpers = attribute_filter_helpers(filters_by_name, collection, attribute)?;
    Ok(helpers.public_type)
}

fn resolve_attribute_filters<'a>(
    filters_by_name: &BTreeMap<&'a str, &'a FilterSpec>,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<Vec<&'a FilterSpec>, CodegenError> {
    let mut chain = Vec::with_capacity(attribute.filters.len());

    for name in &attribute.filters {
        let filter = filters_by_name.get(name.as_str()).copied().ok_or_else(|| {
            CodegenError::Invalid(format!(
                "collection '{}': attribute '{}' references unknown filter '{}'",
                collection.id, attribute.id, name
            ))
        })?;
        chain.push(filter);
    }

    for pair in chain.windows(2) {
        if pair[0].encoded_type != pair[1].decoded_type {
            return Err(CodegenError::Invalid(format!(
                "collection '{}': attribute '{}' filter '{}' encodes '{}', which does not match filter '{}' decoded type '{}'",
                collection.id,
                attribute.id,
                pair[0].name,
                pair[0].encoded_type,
                pair[1].name,
                pair[1].decoded_type
            )));
        }
    }

    Ok(chain)
}

fn resolve_attribute_resolver<'a>(
    resolvers_by_name: &BTreeMap<&'a str, &'a ResolverSpec>,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<&'a ResolverSpec, CodegenError> {
    let Some(name) = attribute.resolver.as_deref() else {
        return Err(CodegenError::Invalid(format!(
            "collection '{}': virtual attribute '{}' requires a resolver",
            collection.id, attribute.id
        )));
    };

    resolvers_by_name.get(name).copied().ok_or_else(|| {
        CodegenError::Invalid(format!(
            "collection '{}': attribute '{}' references unknown resolver '{}'",
            collection.id, attribute.id, name
        ))
    })
}

struct AttributeFilterHelpers<'a> {
    public_type: String,
    storage_type: String,
    chain: Vec<&'a FilterSpec>,
}

fn attribute_filter_helpers<'a>(
    filters_by_name: &BTreeMap<&'a str, &'a FilterSpec>,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<AttributeFilterHelpers<'a>, CodegenError> {
    let chain = resolve_attribute_filters(filters_by_name, collection, attribute)?;
    let mut public_type = chain
        .first()
        .map(|filter| filter.decoded_type.clone())
        .unwrap_or_else(|| storage_field_base_type(attribute.kind, attribute.array));
    let mut storage_type = storage_field_base_type(attribute.kind, attribute.array);

    if !attribute.required {
        public_type = format!("Option<{}>", public_type);
        storage_type = format!("Option<{}>", storage_type);
    }

    Ok(AttributeFilterHelpers {
        public_type,
        storage_type,
        chain,
    })
}

fn encode_helper_name(model_name: &str, attribute_id: &str) -> String {
    format!(
        "encode_{}_{}",
        rust_field_name(model_name),
        rust_field_name(attribute_id)
    )
}

fn decode_helper_name(model_name: &str, attribute_id: &str) -> String {
    format!(
        "decode_{}_{}",
        rust_field_name(model_name),
        rust_field_name(attribute_id)
    )
}

fn query_encode_helper_name(model_name: &str, attribute_id: &str) -> String {
    format!(
        "encode_query_{}_{}",
        rust_field_name(model_name),
        rust_field_name(attribute_id)
    )
}

fn rust_field_name(input: &str) -> String {
    let mut out = String::new();
    let mut prev_was_sep = false;

    for (index, ch) in input.chars().enumerate() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if index > 0 && !prev_was_sep {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            } else {
                out.push(ch);
            }
            prev_was_sep = false;
        } else {
            if !out.is_empty() && !prev_was_sep {
                out.push('_');
            }
            prev_was_sep = true;
        }
    }

    if out.is_empty() {
        "field".to_string()
    } else {
        out
    }
}

fn pascal_case(input: &str) -> String {
    let mut out = String::new();
    let mut upper = true;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if upper {
                out.push(ch.to_ascii_uppercase());
            } else {
                out.push(ch.to_ascii_lowercase());
            }
            upper = false;
        } else {
            upper = true;
        }
    }

    if out.is_empty() {
        "GeneratedModel".to_string()
    } else {
        out
    }
}

fn screaming_snake(input: &str) -> String {
    let mut out = String::new();
    let mut prev_was_sep = true;

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            if !prev_was_sep && ch.is_ascii_uppercase() {
                out.push('_');
            }
            out.push(ch.to_ascii_uppercase());
            prev_was_sep = false;
        } else if !prev_was_sep {
            out.push('_');
            prev_was_sep = true;
        }
    }

    out.trim_matches('_').to_string()
}

fn escape_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

impl database_core::traits::migration::MigrationCollection for CollectionSpec {
    fn id(&self) -> &str {
        &self.id
    }
    fn attributes(&self) -> Vec<database_core::traits::migration::MigrationAttribute> {
        self.attributes
            .iter()
            .map(|a| database_core::traits::migration::MigrationAttribute {
                id: a.id.clone(),
                column: a.column.clone().unwrap_or_else(|| a.id.clone()),
                kind: match a.kind {
                    AttributeKindSpec::String => database_core::AttributeKind::String,
                    AttributeKindSpec::Integer => database_core::AttributeKind::Integer,
                    AttributeKindSpec::Float => database_core::AttributeKind::Float,
                    AttributeKindSpec::Boolean => database_core::AttributeKind::Boolean,
                    AttributeKindSpec::Timestamp => database_core::AttributeKind::Timestamp,
                    AttributeKindSpec::Relationship => database_core::AttributeKind::Relationship,
                    AttributeKindSpec::Virtual => database_core::AttributeKind::Virtual,
                    AttributeKindSpec::Json => database_core::AttributeKind::Json,
                },
                required: a.required,
                array: a.array,
                length: a.length,
                default: None,
                persistence: if a.kind == AttributeKindSpec::Virtual {
                    database_core::AttributePersistence::Virtual
                } else {
                    database_core::AttributePersistence::Persisted
                },
            })
            .collect()
    }
    fn indexes(&self) -> Vec<database_core::traits::migration::MigrationIndex> {
        self.indexes
            .iter()
            .map(|i| database_core::traits::migration::MigrationIndex {
                id: i.id.clone(),
                kind: match i.kind {
                    IndexKindSpec::Key => database_core::IndexKind::Key,
                    IndexKindSpec::Unique => database_core::IndexKind::Unique,
                    IndexKindSpec::FullText => database_core::IndexKind::FullText,
                    IndexKindSpec::Spatial => database_core::IndexKind::Spatial,
                },
                attributes: i.attributes.clone(),
                orders: i
                    .orders
                    .iter()
                    .map(|o| match o {
                        OrderSpec::Asc => database_core::Order::Asc,
                        OrderSpec::Desc => database_core::Order::Desc,
                        OrderSpec::None => database_core::Order::None,
                    })
                    .collect(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{generate, parse_project_spec, validate_project_spec};

    const SPEC: &str = r#"
    {
      "module": "app_models",
      "filters": [
        {
          "name": "displayName",
          "decodedType": "crate::codecs::DisplayName",
          "encodedType": "String",
          "encode": "crate::codecs::encode_display_name",
          "decode": "crate::codecs::decode_display_name"
        }
      ],
      "resolvers": [
        {
          "name": "profileLabel",
          "outputType": "String",
          "resolve": "crate::resolvers::resolve_profile_label"
        }
      ],
      "collections": [
        {
          "id": "users",
          "name": "Users",
          "documentSecurity": true,
          "permissions": ["read(\"any\")", "create(\"any\")"],
          "idMaxLength": 32,
          "indexes": [
            { "id": "users_name_idx", "kind": "key", "attributes": ["name"], "orders": ["asc"] },
            { "id": "users_email_unique", "kind": "unique", "attributes": ["email"] }
          ],
          "attributes": [
            { "id": "name", "kind": "string", "required": true, "filters": ["displayName"] },
            { "id": "email", "kind": "string" },
            { "id": "active", "kind": "boolean", "required": true },
            { "id": "profileLabel", "kind": "virtual", "resolver": "profileLabel" }
          ]
        }
      ]
    }
    "#;

    #[test]
    fn validates_project_spec() {
        let spec = parse_project_spec(SPEC).expect("spec should parse");
        validate_project_spec(&spec).expect("spec should be valid");
    }

    #[test]
    fn generates_rust_models() {
        let spec = parse_project_spec(SPEC).expect("spec should parse");
        let output = generate(&spec).expect("code should generate");

        assert!(output.contains("pub mod app_models"));
        assert!(output.contains("pub struct User;"));
        assert!(output.contains("pub struct UserEntity"));
        assert!(output.contains("pub struct CreateUser"));
        assert!(output.contains("pub struct UpdateUser"));
        assert!(output.contains("Patch<Option<String>>"));
        assert!(output.contains("pub name: crate::codecs::DisplayName"));
        assert!(output.contains("pub profile_label: Option<String>"));
        assert!(output.contains("fn encode_user_name(value: crate::codecs::DisplayName)"));
        assert!(output.contains("crate::codecs::encode_display_name(value)?;"));
        assert!(output.contains("fn decode_user_name(value: String)"));
        assert!(
            output.contains("pub const USER_NAME: EncodedField<User, crate::codecs::DisplayName>")
        );
        assert!(output.contains("fn encode_query_user_name(value: crate::codecs::DisplayName) -> Result<nx_db::traits::storage::StorageValue, DatabaseError>"));
        assert!(output.contains("persistence: AttributePersistence::Virtual"));
        assert!(output.contains("const USERS_INDEXES: &[nx_db::IndexSchema] = &["));
        assert!(output.contains("id: \"users_name_idx\""));
        assert!(output.contains("kind: nx_db::IndexKind::Key"));
        assert!(output.contains("orders: &[nx_db::Order::Asc]"));
        assert!(output.contains("virtuals: { profile_label }"));
        assert!(
            output
                .contains("resolvers: { profile_label : crate::resolvers::resolve_profile_label }")
        );
        assert!(output.contains("pub fn registry() -> Result<StaticRegistry, DatabaseError>"));
    }
}
