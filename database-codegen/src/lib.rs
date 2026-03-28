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
    pub elements: Option<Vec<String>>,
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
    pub through_collection: Option<String>,
    pub through_local_field: Option<String>,
    pub through_remote_field: Option<String>,
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
    Enum,
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

        if collection.id_max_length < database_core::GENERATED_ID_MIN_LENGTH {
            return Err(CodegenError::Invalid(format!(
                "collection '{}' idMaxLength must be at least {} to support generated ids",
                collection.id,
                database_core::GENERATED_ID_MIN_LENGTH
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

            if attribute.kind == AttributeKindSpec::Enum {
                if attribute
                    .elements
                    .as_ref()
                    .map(|e| e.is_empty())
                    .unwrap_or(true)
                {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': enum attribute '{}' must declare elements",
                        collection.id, attribute.id
                    )));
                }
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

                if attribute_is_relation_many(attribute) {
                    if !attribute.filters.is_empty() {
                        return Err(CodegenError::Invalid(format!(
                            "collection '{}': relation-many attribute '{}' cannot declare filters",
                            collection.id, attribute.id
                        )));
                    }
                    if attribute.required {
                        return Err(CodegenError::Invalid(format!(
                            "collection '{}': relation-many attribute '{}' cannot be required",
                            collection.id, attribute.id
                        )));
                    }
                    let _ = related_entity_type(spec, collection, attribute)?;
                } else {
                    let column = attribute.column.as_deref().unwrap_or(&attribute.id);
                    if !columns.insert(column) {
                        return Err(CodegenError::Invalid(format!(
                            "collection '{}' has duplicate column '{}'",
                            collection.id, column
                        )));
                    }
                }
            }

            if !attribute.filters.is_empty() {
                let chain = resolve_attribute_filters(&filters_by_name, collection, attribute)?;
                let storage_type = storage_field_base_type(collection, attribute);

                if let Some(last) = chain.last() {
                    if last.encoded_type != storage_type {
                        return Err(CodegenError::Invalid(format!(
                            "collection '{}': attribute '{}' filter chain stores '{}', expected '{}'",
                            collection.id, attribute.id, last.encoded_type, storage_type
                        )));
                    }
                }
            }

            if let Some(rel) = &attribute.relationship {
                match rel.kind {
                    RelationshipKindSpec::ManyToMany => {
                        if rel.through_collection.as_deref().unwrap_or("").is_empty()
                            || rel.through_local_field.as_deref().unwrap_or("").is_empty()
                            || rel.through_remote_field.as_deref().unwrap_or("").is_empty()
                        {
                            return Err(CodegenError::Invalid(format!(
                                "collection '{}': many-to-many attribute '{}' requires throughCollection, throughLocalField, and throughRemoteField",
                                collection.id, attribute.id
                            )));
                        }
                    }
                    _ => {
                        if rel.through_collection.is_some()
                            || rel.through_local_field.is_some()
                            || rel.through_remote_field.is_some()
                        {
                            return Err(CodegenError::Invalid(format!(
                                "collection '{}': non-many-to-many attribute '{}' cannot declare throughCollection/throughLocalField/throughRemoteField",
                                collection.id, attribute.id
                            )));
                        }
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

                if !attribute_is_persisted(attribute) {
                    return Err(CodegenError::Invalid(format!(
                        "collection '{}': index '{}' cannot reference non-persisted relationship attribute '{}'",
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
    let module_name = spec
        .module
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("generated_models");

    writeln!(&mut out, "// @generated by database-cli.").unwrap();
    writeln!(&mut out, "// Do not edit by hand.").unwrap();
    writeln!(&mut out).unwrap();
    writeln!(&mut out, "#[allow(dead_code)]").unwrap();
    writeln!(&mut out, "pub mod {module_name} {{").unwrap();
    let needs_database_error = spec.collections.iter().any(|collection| {
        collection
            .attributes
            .iter()
            .any(|attribute| !attribute.filters.is_empty())
    });
    let mut imports = vec![
        "AttributeKind",
        "AttributeSchema",
        "CollectionSchema",
        "Key",
        "Patch",
        "FIELD_ID",
    ];
    if needs_database_error {
        imports.push("DatabaseError");
    }

    writeln!(&mut out, "    use nx_db::{{{}}};", imports.join(", ")).unwrap();
    writeln!(&mut out).unwrap();

    for collection in &spec.collections {
        for attribute in &collection.attributes {
            if attribute.kind == AttributeKindSpec::Enum {
                let enum_name = enum_type_name(collection, attribute);
                let elements = attribute.elements.as_ref().unwrap();

                writeln!(out, "    nx_db::impl_enum! {{").unwrap();
                writeln!(out, "        name: {enum_name},").unwrap();
                writeln!(out, "        variants: {{").unwrap();
                for element in elements {
                    writeln!(
                        out,
                        "            {} => \"{}\",",
                        pascal_case(element),
                        element
                    )
                    .unwrap();
                }
                writeln!(out, "        }}").unwrap();
                writeln!(out, "    }}").unwrap();
                writeln!(out).unwrap();
            }
        }
    }

    for collection in &spec.collections {
        emit_collection(&mut out, spec, collection)?;
    }

    writeln!(&mut out, "    nx_db::impl_registry_fn! {{").unwrap();
    writeln!(&mut out, "        fn: registry,").unwrap();
    writeln!(&mut out, "        schemas: [").unwrap();
    for collection in &spec.collections {
        let const_name = format!("{}_SCHEMA", screaming_snake(&collection.id));
        writeln!(&mut out, "            {const_name},").unwrap();
    }
    writeln!(&mut out, "        ]").unwrap();
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
        "    #[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize, nx_db::NxEntity)]"
    )
    .unwrap();
    writeln!(out, "    #[serde(rename_all = \"camelCase\")]").unwrap();
    writeln!(out, "    pub struct {entity_name} {{").unwrap();
    writeln!(out, "        #[nx(id)]").unwrap();
    writeln!(out, "        pub id: {id_name},").unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual {
            let resolver = resolve_attribute_resolver(&resolvers_by_name, collection, attribute)?;
            writeln!(
                out,
                "        #[serde(skip_serializing_if = \"Option::is_none\")]"
            )
            .unwrap();
            writeln!(
                out,
                "        #[nx(virtual, resolve = \"{}\")]",
                resolver.resolve
            )
            .unwrap();
            writeln!(
                out,
                "        pub {}: {},",
                rust_field_name(&attribute.id),
                entity_field_type(spec, collection, attribute)?
            )
            .unwrap();
            continue;
        }
        if attribute_is_relation_many(attribute) {
            writeln!(
                out,
                "        #[serde(default, skip_serializing_if = \"nx_db::RelationMany::is_not_loaded\")]"
            )
            .unwrap();
            writeln!(out, "        #[nx(loaded_many)]").unwrap();
            writeln!(
                out,
                "        pub {}: {},",
                rust_field_name(&attribute.id),
                entity_field_type(spec, collection, attribute)?
            )
            .unwrap();
            continue;
        }

        let field_attr = if attribute.filters.is_empty() {
            if attribute.required {
                format!("#[nx(field = \"{}\", required)]", attribute.id)
            } else {
                format!("#[nx(field = \"{}\")]", attribute.id)
            }
        } else {
            let decode = decode_helper_name(&model_name, &attribute.id);
            if attribute.required {
                format!(
                    "#[nx(field = \"{}\", required, decode = \"{}\")]",
                    attribute.id, decode
                )
            } else {
                format!(
                    "#[nx(field = \"{}\", decode = \"{}\")]",
                    attribute.id, decode
                )
            }
        };
        writeln!(out, "        {field_attr}").unwrap();
        let field_type = entity_field_type(spec, collection, attribute)?;
        writeln!(
            out,
            "        pub {}: {},",
            rust_field_name(&attribute.id),
            field_type
        )
        .unwrap();
        if attribute_is_relation_one(attribute) {
            writeln!(
                out,
                "        #[serde(default, skip_serializing_if = \"nx_db::RelationOne::is_not_loaded\")]"
            )
            .unwrap();
            writeln!(out, "        #[nx(loaded_one)]").unwrap();
            writeln!(
                out,
                "        pub {}: nx_db::RelationOne<{}>,",
                loaded_relation_field_name(attribute),
                related_entity_type(spec, collection, attribute)?
            )
            .unwrap();
        }
    }
    writeln!(out, "        #[serde(flatten)]").unwrap();
    writeln!(out, "        #[nx(metadata)]").unwrap();
    writeln!(out, "        pub _metadata: nx_db::Metadata,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    writeln!(
        out,
        "    #[derive(Debug, Clone, ::serde::Serialize, ::serde::Deserialize, nx_db::NxCreate)]"
    )
    .unwrap();
    writeln!(out, "    #[serde(rename_all = \"camelCase\")]").unwrap();
    writeln!(out, "    pub struct {create_name} {{").unwrap();
    writeln!(out, "        #[nx(id)]").unwrap();
    writeln!(out, "        pub id: Option<{id_name}>,").unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual || attribute_is_relation_many(attribute) {
            continue;
        }
        if attribute.filters.is_empty() {
            if attribute.required {
                writeln!(out, "        #[nx(field = \"{}\", required)]", attribute.id).unwrap();
            } else {
                writeln!(out, "        #[nx(field = \"{}\")]", attribute.id).unwrap();
            }
        } else {
            let encode = encode_helper_name(&model_name, &attribute.id);
            if attribute.required {
                writeln!(
                    out,
                    "        #[nx(field = \"{}\", required, encode = \"{}\")]",
                    attribute.id, encode
                )
                .unwrap();
            } else {
                writeln!(
                    out,
                    "        #[nx(field = \"{}\", encode = \"{}\")]",
                    attribute.id, encode
                )
                .unwrap();
            }
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
    writeln!(
        out,
        "        #[serde(default, skip_serializing_if = \"Vec::is_empty\")]"
    )
    .unwrap();
    writeln!(out, "        #[nx(permissions)]").unwrap();
    writeln!(out, "        pub permissions: Vec<String>,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "    #[derive(Debug, Clone, Default, nx_db::NxUpdate)]").unwrap();
    writeln!(out, "    pub struct {update_name} {{").unwrap();
    for attribute in &collection.attributes {
        if attribute.kind == AttributeKindSpec::Virtual || attribute_is_relation_many(attribute) {
            continue;
        }
        if attribute.filters.is_empty() {
            writeln!(out, "        #[nx(field = \"{}\")]", attribute.id).unwrap();
        } else {
            writeln!(
                out,
                "        #[nx(field = \"{}\", encode = \"{}\")]",
                attribute.id,
                encode_helper_name(&model_name, &attribute.id)
            )
            .unwrap();
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
    writeln!(out, "        #[nx(permissions)]").unwrap();
    writeln!(out, "        pub permissions: Patch<Vec<String>>,").unwrap();
    writeln!(out, "    }}").unwrap();
    writeln!(out).unwrap();

    let filters_by_name: BTreeMap<&str, &FilterSpec> = spec
        .filters
        .iter()
        .map(|filter| (filter.name.as_str(), filter))
        .collect();
    writeln!(out, "    nx_db::declare_model! {{").unwrap();
    writeln!(out, "        name: {model_name},").unwrap();
    writeln!(out, "        const: {model_const},").unwrap();
    writeln!(out, "        entity: {entity_name},").unwrap();
    writeln!(out, "        create: {create_name},").unwrap();
    writeln!(out, "        update: {update_name},").unwrap();
    writeln!(out, "        schema: {schema_const},").unwrap();
    writeln!(out, "        plain: {{").unwrap();
    writeln!(
        out,
        "            {model_const}_ID => ID: {id_name} = FIELD_ID,"
    )
    .unwrap();
    for attribute in &collection.attributes {
        if attribute_is_relation_many(attribute)
            || attribute.kind == AttributeKindSpec::Virtual
            || !attribute.filters.is_empty()
        {
            continue;
        }
        let top_const_name = format!("{model_const}_{}", screaming_snake(&attribute.id));
        let assoc_const_name = screaming_snake(&attribute.id);
        let query_type = query_field_type(collection, attribute);
        writeln!(
            out,
            "            {top_const_name} => {assoc_const_name}: {query_type} = \"{}\",",
            attribute.id
        )
        .unwrap();
    }
    writeln!(out, "        }},").unwrap();
    writeln!(out, "        encoded: {{").unwrap();
    for attribute in &collection.attributes {
        if attribute_is_relation_many(attribute) || attribute.filters.is_empty() {
            continue;
        }
        let top_const_name = format!("{model_const}_{}", screaming_snake(&attribute.id));
        let assoc_const_name = screaming_snake(&attribute.id);
        let public_type = filtered_query_field_type(&filters_by_name, collection, attribute)?;
        writeln!(
            out,
            "            {top_const_name} => {assoc_const_name}: {public_type} = \"{}\" => encode_query_{}_{},",
            attribute.id,
            rust_field_name(&model_name),
            rust_field_name(&attribute.id)
        )
        .unwrap();
    }
    writeln!(out, "        }}").unwrap();
    writeln!(out, "    }}").unwrap();
    for attribute in &collection.attributes {
        let Some(rel) = &attribute.relationship else {
            continue;
        };

        let Some(related_collection) = spec
            .collections
            .iter()
            .find(|candidate| candidate.id == rel.related_collection)
        else {
            continue;
        };

        let related_model_name = related_collection
            .model
            .clone()
            .unwrap_or_else(|| default_model_name(related_collection));
        let rel_const_name = format!("{model_const}_{}_REL", screaming_snake(&attribute.id));
        let rel_expr = match rel.kind {
            RelationshipKindSpec::ManyToOne => Some(format!(
                "nx_db::Rel::<{model_name}, {related_model_name}>::many_to_one(\"{}\", \"{}\")",
                attribute.id, attribute.id
            )),
            RelationshipKindSpec::OneToMany => rel.two_way_key.as_ref().map(|remote_fk| {
                format!(
                    "nx_db::Rel::<{model_name}, {related_model_name}>::one_to_many(\"{}\", \"{}\")",
                    attribute.id,
                    escape_string(remote_fk)
                )
            }),
            RelationshipKindSpec::OneToOne => {
                let remote_field_expr = rel
                    .two_way_key
                    .as_ref()
                    .map(|field| format!("\"{}\"", escape_string(field)))
                    .unwrap_or_else(|| "nx_db::FIELD_ID".to_string());
                let local_field_expr = if rel.side == RelationshipSideSpec::Child {
                    "nx_db::FIELD_ID".to_string()
                } else {
                    format!("\"{}\"", escape_string(&attribute.id))
                };
                Some(format!(
                    "nx_db::Rel::<{model_name}, {related_model_name}>::one_to_one(\"{}\", {}, {})",
                    attribute.id, local_field_expr, remote_field_expr
                ))
            }
            RelationshipKindSpec::ManyToMany => Some(format!(
                "nx_db::Rel::<{model_name}, {related_model_name}>::many_to_many(\"{}\", \"{}\", \"{}\", \"{}\")",
                attribute.id,
                escape_string(
                    rel.through_collection
                        .as_deref()
                        .expect("validated throughCollection")
                ),
                escape_string(
                    rel.through_local_field
                        .as_deref()
                        .expect("validated throughLocalField")
                ),
                escape_string(
                    rel.through_remote_field
                        .as_deref()
                        .expect("validated throughRemoteField")
                )
            )),
        };

        if let Some(rel_expr) = rel_expr {
            let populate_const_name =
                format!("{model_const}_{}_POPULATE", screaming_snake(&attribute.id));
            let local_fn = format!(
                "populate_{}_{}_local_key",
                rust_field_name(&model_name),
                rust_field_name(&attribute.id)
            );
            let remote_fn = format!(
                "populate_{}_{}_remote_key",
                rust_field_name(&model_name),
                rust_field_name(&attribute.id)
            );
            let set_fn = format!(
                "populate_{}_{}_set",
                rust_field_name(&model_name),
                rust_field_name(&attribute.id)
            );

            match rel.kind {
                RelationshipKindSpec::ManyToOne | RelationshipKindSpec::OneToOne => {
                    let local_key_expr = if rel.kind == RelationshipKindSpec::OneToOne
                        && rel.side == RelationshipSideSpec::Child
                    {
                        "Some(local_entity.id.to_string())".to_string()
                    } else {
                        entity_optional_string_key_expr(collection, &attribute.id, "local_entity")?
                    };
                    let remote_field = if rel.kind == RelationshipKindSpec::OneToOne {
                        rel.two_way_key.as_deref().unwrap_or("id")
                    } else {
                        "id"
                    };
                    let remote_key_expr = entity_optional_string_key_expr(
                        related_collection,
                        remote_field,
                        "remote_entity",
                    )?;
                    let loaded_field = loaded_relation_field_name(attribute);

                    writeln!(
                        out,
                        "    nx_db::impl_relation_one! {{ rel_const: {rel_const_name}, populate_const: {populate_const_name}, rel_expr: {rel_expr}, local_fn: {local_fn}, remote_fn: {remote_fn}, set_fn: {set_fn}, model: {model_name}, related_model: {related_model_name}, entity: {entity_name}, related_entity: {related_model_name}Entity, field: {loaded_field}, local_key: |local_entity| {local_key_expr}, remote_key: |remote_entity| {remote_key_expr} }}"
                    )
                    .unwrap();
                }
                RelationshipKindSpec::OneToMany | RelationshipKindSpec::ManyToMany => {
                    let local_key_expr = "local_entity.id.to_string()".to_string();
                    let remote_field = if rel.kind == RelationshipKindSpec::OneToMany {
                        rel.two_way_key.as_deref().unwrap_or("id")
                    } else {
                        "id"
                    };
                    let remote_key_expr = entity_optional_string_key_expr(
                        related_collection,
                        remote_field,
                        "remote_entity",
                    )?;
                    let field_name = rust_field_name(&attribute.id);

                    writeln!(
                        out,
                        "    nx_db::impl_relation_many! {{ rel_const: {rel_const_name}, populate_const: {populate_const_name}, rel_expr: {rel_expr}, local_fn: {local_fn}, remote_fn: {remote_fn}, set_fn: {set_fn}, model: {model_name}, related_model: {related_model_name}, entity: {entity_name}, related_entity: {related_model_name}Entity, field: {field_name}, local_key: |local_entity| {local_key_expr}, remote_key: |remote_entity| {remote_key_expr} }}"
                    )
                    .unwrap();
                }
            }
        }
    }
    writeln!(out).unwrap();

    for attribute in &collection.attributes {
        if attribute.filters.is_empty() || attribute_is_relation_many(attribute) {
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
        writeln!(out, "        {},", attribute_schema_expr(attribute)).unwrap();
    }
    writeln!(out, "    ];").unwrap();
    writeln!(out, "    const {indexes_const}: &[nx_db::IndexSchema] = &[").unwrap();
    for index in &collection.indexes {
        writeln!(out, "        {},", index_schema_expr(index)).unwrap();
    }
    writeln!(out, "    ];").unwrap();
    writeln!(
        out,
        "    pub static {schema_const}: CollectionSchema = CollectionSchema::new(\"{}\", \"{}\").document_security({}).permissions({}).attributes({attrs_const}).indexes({indexes_const});",
        collection.id,
        escape_string(&collection.name),
        collection.document_security,
        string_slice_expr(&collection.permissions),
    )
    .unwrap();

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
        AttributeKindSpec::Enum => "AttributeKind::Enum",
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

fn string_slice_expr(values: &[String]) -> String {
    format!(
        "&[{}]",
        values
            .iter()
            .map(|value| format!("\"{}\"", escape_string(value)))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn optional_string_expr(value: Option<&String>) -> String {
    value
        .map(|value| format!("Some(\"{}\")", escape_string(value)))
        .unwrap_or_else(|| "None".to_string())
}

fn relationship_schema_expr(relationship: &RelationshipSpec) -> String {
    let mut expr = format!(
        "nx_db::RelationshipSchema::new(\"{}\", {}, {})",
        escape_string(&relationship.related_collection),
        relationship_kind_expr(relationship.kind),
        relationship_side_expr(relationship.side),
    );

    if relationship.two_way {
        expr.push_str(&format!(
            ".two_way({})",
            optional_string_expr(relationship.two_way_key.as_ref())
        ));
    }

    if let (Some(through_collection), Some(through_local_field), Some(through_remote_field)) = (
        relationship.through_collection.as_ref(),
        relationship.through_local_field.as_ref(),
        relationship.through_remote_field.as_ref(),
    ) {
        expr.push_str(&format!(
            ".through(\"{}\", \"{}\", \"{}\")",
            escape_string(through_collection),
            escape_string(through_local_field),
            escape_string(through_remote_field),
        ));
    }

    if relationship.on_delete != OnDeleteActionSpec::Restrict {
        expr.push_str(&format!(
            ".on_delete({})",
            on_delete_action_expr(relationship.on_delete)
        ));
    }

    expr
}

fn attribute_schema_expr(attribute: &AttributeSpec) -> String {
    let mut expr = if attribute_is_persisted(attribute) {
        format!(
            "AttributeSchema::persisted(\"{}\", \"{}\", {})",
            escape_string(&attribute.id),
            escape_string(&attribute_column(attribute)),
            attribute_kind_expr(attribute.kind),
        )
    } else {
        format!(
            "AttributeSchema::virtual_field(\"{}\", {})",
            escape_string(&attribute.id),
            attribute_kind_expr(attribute.kind),
        )
    };

    if attribute.required {
        expr.push_str(".required()");
    }

    if attribute.array {
        expr.push_str(".array()");
    }

    if let Some(length) = attribute.length {
        expr.push_str(&format!(".length({length})"));
    }

    if !attribute.filters.is_empty() {
        expr.push_str(&format!(
            ".filters({})",
            string_slice_expr(&attribute.filters)
        ));
    }

    if let Some(elements) = attribute.elements.as_ref() {
        expr.push_str(&format!(".enum_elements({})", string_slice_expr(elements)));
    }

    if let Some(relationship) = attribute.relationship.as_ref() {
        expr.push_str(&format!(
            ".relationship({})",
            relationship_schema_expr(relationship)
        ));
    }

    expr
}

fn index_schema_expr(index: &IndexSpec) -> String {
    let mut expr = format!(
        "nx_db::IndexSchema::new(\"{}\", {}, {})",
        escape_string(&index.id),
        index_kind_expr(index.kind),
        string_slice_expr(&index.attributes),
    );

    if !index.orders.is_empty() {
        expr.push_str(&format!(
            ".orders(&[{}])",
            index
                .orders
                .iter()
                .map(|order| order_expr(*order))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    expr
}

fn storage_field_base_type(collection: &CollectionSpec, attribute: &AttributeSpec) -> String {
    let base = match attribute.kind {
        AttributeKindSpec::String | AttributeKindSpec::Relationship => "String".to_string(),
        AttributeKindSpec::Integer => "i64".to_string(),
        AttributeKindSpec::Float => "f64".to_string(),
        AttributeKindSpec::Boolean => "bool".to_string(),
        AttributeKindSpec::Timestamp => "nx_db::time::OffsetDateTime".to_string(),
        AttributeKindSpec::Virtual | AttributeKindSpec::Json => "String".to_string(),
        AttributeKindSpec::Enum => enum_type_name(collection, attribute),
    };

    if attribute.array {
        format!("Vec<{base}>")
    } else {
        base
    }
}

fn enum_type_name(collection: &CollectionSpec, attribute: &AttributeSpec) -> String {
    format!(
        "{}{}",
        pascal_case(&collection.id),
        pascal_case(&attribute.id)
    )
}

fn attribute_is_relation_many(attribute: &AttributeSpec) -> bool {
    attribute.kind == AttributeKindSpec::Relationship
        && matches!(
            attribute.relationship.as_ref().map(|rel| rel.kind),
            Some(RelationshipKindSpec::OneToMany | RelationshipKindSpec::ManyToMany)
        )
}

fn attribute_is_relation_one(attribute: &AttributeSpec) -> bool {
    attribute.kind == AttributeKindSpec::Relationship
        && matches!(
            attribute.relationship.as_ref().map(|rel| rel.kind),
            Some(RelationshipKindSpec::ManyToOne | RelationshipKindSpec::OneToOne)
        )
}

fn loaded_relation_field_name(attribute: &AttributeSpec) -> String {
    format!("{}_rel", rust_field_name(&attribute.id))
}

fn attribute_is_persisted(attribute: &AttributeSpec) -> bool {
    attribute.kind != AttributeKindSpec::Virtual && !attribute_is_relation_many(attribute)
}

fn attribute_column(attribute: &AttributeSpec) -> &str {
    if !attribute_is_persisted(attribute) {
        ""
    } else {
        attribute.column.as_deref().unwrap_or(&attribute.id)
    }
}

fn related_entity_type(
    spec: &ProjectSpec,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<String, CodegenError> {
    let rel = attribute.relationship.as_ref().ok_or_else(|| {
        CodegenError::Invalid(format!(
            "collection '{}': relationship attribute '{}' is missing relationship metadata",
            collection.id, attribute.id
        ))
    })?;
    let related_collection = spec
        .collections
        .iter()
        .find(|candidate| candidate.id == rel.related_collection)
        .ok_or_else(|| {
            CodegenError::Invalid(format!(
                "collection '{}': attribute '{}' references unknown related collection '{}'",
                collection.id, attribute.id, rel.related_collection
            ))
        })?;
    let related_model = related_collection
        .model
        .clone()
        .unwrap_or_else(|| default_model_name(related_collection));
    Ok(format!("{related_model}Entity"))
}

fn entity_optional_string_key_expr(
    collection: &CollectionSpec,
    field_id: &str,
    entity_var: &str,
) -> Result<String, CodegenError> {
    if field_id == "id" {
        return Ok(format!("Some({entity_var}.id.to_string())"));
    }

    let attribute = collection
        .attributes
        .iter()
        .find(|attribute| attribute.id == field_id)
        .ok_or_else(|| {
            CodegenError::Invalid(format!(
                "collection '{}': unknown relationship key field '{}'",
                collection.id, field_id
            ))
        })?;
    let field_name = rust_field_name(&attribute.id);
    if attribute.required {
        Ok(format!("Some({entity_var}.{field_name}.clone())"))
    } else {
        Ok(format!("{entity_var}.{field_name}.clone()"))
    }
}

fn entity_field_type(
    spec: &ProjectSpec,
    collection: &CollectionSpec,
    attribute: &AttributeSpec,
) -> Result<String, CodegenError> {
    if attribute_is_relation_many(attribute) {
        return Ok(format!(
            "nx_db::RelationMany<{}>",
            related_entity_type(spec, collection, attribute)?
        ));
    }

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
        storage_field_base_type(collection, attribute)
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

fn query_field_type(collection: &CollectionSpec, attribute: &AttributeSpec) -> String {
    let base = storage_field_base_type(collection, attribute);
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
        .unwrap_or_else(|| storage_field_base_type(collection, attribute));
    let mut storage_type = storage_field_base_type(collection, attribute);

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
                    AttributeKindSpec::Enum => database_core::AttributeKind::Enum,
                },
                required: a.required,
                array: a.array,
                length: a.length,
                default: None,
                persistence: if !attribute_is_persisted(a) {
                    database_core::AttributePersistence::Virtual
                } else {
                    database_core::AttributePersistence::Persisted
                },
                elements: a.elements.clone(),
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
          "idMaxLength": 48,
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

    const MANY_TO_MANY_SPEC: &str = r#"
    {
      "module": "membership_models",
      "collections": [
        {
          "id": "users",
          "name": "Users",
          "attributes": [
            { "id": "name", "kind": "string", "required": true },
            {
              "id": "roles",
              "kind": "relationship",
              "relationship": {
                "relatedCollection": "roles",
                "kind": "manytomany",
                "throughCollection": "user_roles",
                "throughLocalField": "userId",
                "throughRemoteField": "roleId"
              }
            }
          ]
        },
        {
          "id": "roles",
          "name": "Roles",
          "attributes": [
            { "id": "name", "kind": "string", "required": true }
          ]
        },
        {
          "id": "user_roles",
          "name": "UserRoles",
          "attributes": [
            { "id": "userId", "kind": "relationship", "required": true },
            { "id": "roleId", "kind": "relationship", "required": true }
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
        assert!(output.contains("nx_db::declare_model! {"));
        assert!(output.contains("pub struct UserEntity"));
        assert!(output.contains("pub struct CreateUser"));
        assert!(output.contains("pub struct UpdateUser"));
        assert!(output.contains("pub id: Option<UserId>"));
        assert!(output.contains("const: USER"));
        assert!(output.contains("USER_ID => ID: UserId = FIELD_ID"));
        assert!(output.contains("Patch<Option<String>>"));
        assert!(output.contains("pub name: crate::codecs::DisplayName"));
        assert!(output.contains("pub profile_label: Option<String>"));
        assert!(output.contains("fn encode_user_name(value: crate::codecs::DisplayName)"));
        assert!(output.contains("crate::codecs::encode_display_name(value)?;"));
        assert!(output.contains("fn decode_user_name(value: String)"));
        assert!(output.contains(
            "USER_NAME => NAME: crate::codecs::DisplayName = \"name\" => encode_query_user_name"
        ));
        assert!(output.contains("fn encode_query_user_name(value: crate::codecs::DisplayName) -> Result<nx_db::traits::storage::StorageValue, DatabaseError>"));
        assert!(
            output.contains(
                "AttributeSchema::virtual_field(\"profileLabel\", AttributeKind::Virtual)"
            )
        );
        assert!(output.contains("const USERS_INDEXES: &[nx_db::IndexSchema] = &["));
        assert!(output.contains("nx_db::IndexSchema::new(\"users_name_idx\", nx_db::IndexKind::Key, &[\"name\"]).orders(&[nx_db::Order::Asc])"));
        assert!(
            output
                .contains("#[nx(virtual, resolve = \"crate::resolvers::resolve_profile_label\")]")
        );
        assert!(output.contains("nx_db::impl_registry_fn! {"));
    }

    #[test]
    fn generates_many_to_many_relationship_metadata() {
        let spec = parse_project_spec(MANY_TO_MANY_SPEC).expect("spec should parse");
        validate_project_spec(&spec).expect("many-to-many spec should be valid");
        let output = generate(&spec).expect("code should generate");

        assert!(output.contains(
            "nx_db::impl_relation_many! { rel_const: USER_ROLES_REL, populate_const: USER_ROLES_POPULATE, rel_expr: nx_db::Rel::<User, Role>::many_to_many(\"roles\", \"user_roles\", \"userId\", \"roleId\")"
        ));
        assert!(output.contains("pub roles: nx_db::RelationMany<RoleEntity>"));
        assert!(output.contains(".through(\"user_roles\", \"userId\", \"roleId\")"));
    }
}
