use crate::enums::{
    AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide,
};
use crate::errors::DatabaseError;
use crate::traits::storage::StorageValue;
use std::collections::BTreeSet;

/// A compile-time default value for an attribute that is applied on insert
/// when no value is provided.
///
/// The enum is intentionally `Copy + Eq` so it can live inside `const` items.
/// Floats are stored as IEEE-754 raw bits (`u64`) to satisfy `Eq`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultValue {
    /// Explicit SQL NULL.
    Null,
    /// Boolean default.
    Bool(bool),
    /// 64-bit integer default.
    Int(i64),
    /// 64-bit float stored as raw IEEE-754 bits.  Use [`DefaultValue::float`]
    /// to construct this variant from an `f64`.
    Float(u64),
    /// Static string default.
    Str(&'static str),
    /// Insert the current UTC timestamp at write time.
    Now,
}

impl DefaultValue {
    /// Construct a float default from an `f64`.
    pub const fn float(v: f64) -> Self {
        Self::Float(v.to_bits())
    }

    /// Convert this default into a [`StorageValue`] suitable for insertion.
    pub fn into_storage(self) -> StorageValue {
        match self {
            Self::Null => StorageValue::Null,
            Self::Bool(b) => StorageValue::Bool(b),
            Self::Int(i) => StorageValue::Int(i),
            Self::Float(bits) => StorageValue::Float(f64::from_bits(bits)),
            Self::Str(s) => StorageValue::String(s.to_string()),
            Self::Now => StorageValue::Timestamp(time::OffsetDateTime::now_utc()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionSchema {
    pub id: &'static str,
    pub name: &'static str,
    pub document_security: bool,
    pub enabled: bool,
    pub permissions: &'static [&'static str],
    pub attributes: &'static [AttributeSchema],
    pub indexes: &'static [IndexSchema],
}

impl CollectionSchema {
    /// Find an attribute by its logical id.
    pub fn attribute(&self, id: &str) -> Option<&AttributeSchema> {
        self.attributes.iter().find(|a| a.id == id)
    }

    /// Iterate over attributes that are persisted to storage.
    pub fn persisted_attributes(&self) -> impl Iterator<Item = &AttributeSchema> {
        self.attributes
            .iter()
            .filter(|a| a.persistence == AttributePersistence::Persisted)
    }

    /// Validate the schema for logical consistency.
    pub fn validate(&self) -> Result<(), DatabaseError> {
        if self.id.is_empty() {
            return Err(DatabaseError::Other("collection id cannot be empty".into()));
        }

        let mut attribute_ids = BTreeSet::new();
        let mut column_names = BTreeSet::new();

        for attribute in self.attributes {
            if attribute.id.is_empty() {
                return Err(DatabaseError::Other(format!(
                    "collection '{}' has an attribute with an empty id",
                    self.id
                )));
            }

            if !attribute_ids.insert(attribute.id) {
                return Err(DatabaseError::Other(format!(
                    "collection '{}' has duplicate attribute '{}'",
                    self.id, attribute.id
                )));
            }

            if attribute.persistence == AttributePersistence::Persisted {
                if attribute.column.is_empty() {
                    return Err(DatabaseError::Other(format!(
                        "persisted attribute '{}.{}' requires a column name",
                        self.id, attribute.id
                    )));
                }

                if !column_names.insert(attribute.column) {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}' has duplicate column '{}'",
                        self.id, attribute.column
                    )));
                }
            }

            if attribute.relationship.is_some() && attribute.kind != AttributeKind::Relationship {
                return Err(DatabaseError::Other(format!(
                    "attribute '{}.{}' declares relationship metadata but is not a relationship",
                    self.id, attribute.id
                )));
            }

            if let Some(relationship) = attribute.relationship {
                match relationship.kind {
                    RelationshipKind::ManyToMany => {
                        if relationship.through_collection.is_none()
                            || relationship.through_local_field.is_none()
                            || relationship.through_remote_field.is_none()
                        {
                            return Err(DatabaseError::Other(format!(
                                "attribute '{}.{}' is many-to-many but missing through metadata",
                                self.id, attribute.id
                            )));
                        }
                    }
                    _ => {
                        if relationship.through_collection.is_some()
                            || relationship.through_local_field.is_some()
                            || relationship.through_remote_field.is_some()
                        {
                            return Err(DatabaseError::Other(format!(
                                "attribute '{}.{}' declares through metadata for a non-many-to-many relationship",
                                self.id, attribute.id
                            )));
                        }
                    }
                }
            }
        }

        let mut index_ids = BTreeSet::new();

        for index in self.indexes {
            if index.id.is_empty() {
                return Err(DatabaseError::Other(format!(
                    "collection '{}' has an index with an empty id",
                    self.id
                )));
            }

            if !index_ids.insert(index.id) {
                return Err(DatabaseError::Other(format!(
                    "collection '{}' has duplicate index '{}'",
                    self.id, index.id
                )));
            }

            if index.attributes.is_empty() {
                return Err(DatabaseError::Other(format!(
                    "index '{}.{}' must reference at least one attribute",
                    self.id, index.id
                )));
            }

            if !index.orders.is_empty() && index.orders.len() != index.attributes.len() {
                return Err(DatabaseError::Other(format!(
                    "index '{}.{}' orders length must match attributes length",
                    self.id, index.id
                )));
            }

            for attribute_id in index.attributes {
                let Some(attribute) = self.attribute(attribute_id) else {
                    return Err(DatabaseError::Other(format!(
                        "index '{}.{}' references unknown attribute '{}'",
                        self.id, index.id, attribute_id
                    )));
                };

                if attribute.persistence != AttributePersistence::Persisted {
                    return Err(DatabaseError::Other(format!(
                        "index '{}.{}' cannot reference virtual attribute '{}'",
                        self.id, index.id, attribute_id
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Schema description for a single attribute/column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttributeSchema {
    /// Logical attribute id used in queries and records.
    pub id: &'static str,
    /// Physical column name in the backing store.
    pub column: &'static str,
    /// Value kind for this attribute.
    pub kind: AttributeKind,
    /// Whether the attribute must be provided on insert.
    pub required: bool,
    /// Whether the attribute holds an array of values.
    pub array: bool,
    /// Optional maximum length for string attributes.
    pub length: Option<usize>,
    /// Optional default value applied on insert when no value is given.
    pub default: Option<DefaultValue>,
    /// Whether this attribute is stored or computed.
    pub persistence: AttributePersistence,
    /// Named filter codecs that apply to this attribute.
    pub filters: &'static [&'static str],
    /// Enum elements, set when `kind == AttributeKind::Enum`.
    pub elements: Option<&'static [&'static str]>,
    /// Relationship metadata, set when `kind == AttributeKind::Relationship`.
    pub relationship: Option<RelationshipSchema>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttributePersistence {
    Persisted,
    Virtual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationshipSchema {
    pub related_collection: &'static str,
    pub kind: RelationshipKind,
    pub side: RelationshipSide,
    pub two_way: bool,
    pub two_way_key: Option<&'static str>,
    pub through_collection: Option<&'static str>,
    pub through_local_field: Option<&'static str>,
    pub through_remote_field: Option<&'static str>,
    pub on_delete: OnDeleteAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSchema {
    pub id: &'static str,
    pub kind: IndexKind,
    pub attributes: &'static [&'static str],
    pub orders: &'static [Order],
}

impl crate::traits::migration::MigrationCollection for &'static CollectionSchema {
    fn id(&self) -> &str {
        self.id
    }

    fn attributes(&self) -> Vec<crate::traits::migration::MigrationAttribute> {
        self.attributes
            .iter()
            .map(|a| crate::traits::migration::MigrationAttribute {
                id: a.id.to_string(),
                column: a.column.to_string(),
                kind: a.kind,
                required: a.required,
                array: a.array,
                length: a.length,
                default: a.default,
                persistence: a.persistence,
                elements: a
                    .elements
                    .map(|e| e.iter().map(|s| s.to_string()).collect()),
            })
            .collect()
    }

    fn indexes(&self) -> Vec<crate::traits::migration::MigrationIndex> {
        self.indexes
            .iter()
            .map(|i| crate::traits::migration::MigrationIndex {
                id: i.id.to_string(),
                kind: i.kind,
                attributes: i.attributes.iter().map(|a| a.to_string()).collect(),
                orders: i.orders.to_vec(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AttributePersistence, AttributeSchema, CollectionSchema, DefaultValue, IndexSchema,
        RelationshipSchema,
    };
    use crate::enums::{
        AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide,
    };

    const ATTRIBUTES: &[AttributeSchema] = &[
        AttributeSchema {
            id: "name",
            column: "name",
            kind: AttributeKind::String,
            required: true,
            array: false,
            length: None,
            default: None,
            persistence: AttributePersistence::Persisted,
            filters: &[],
            elements: None,
            relationship: None,
        },
        AttributeSchema {
            id: "score",
            column: "score",
            kind: AttributeKind::Integer,
            required: false,
            array: false,
            length: None,
            default: Some(DefaultValue::Int(0)),
            persistence: AttributePersistence::Persisted,
            filters: &[],
            elements: None,
            relationship: None,
        },
        AttributeSchema {
            id: "author",
            column: "author_id",
            kind: AttributeKind::Relationship,
            required: false,
            array: false,
            length: None,
            default: None,
            persistence: AttributePersistence::Persisted,
            filters: &[],
            elements: None,
            relationship: Some(RelationshipSchema {
                related_collection: "users",
                kind: RelationshipKind::ManyToOne,
                side: RelationshipSide::Parent,
                two_way: false,
                two_way_key: None,
                through_collection: None,
                through_local_field: None,
                through_remote_field: None,
                on_delete: OnDeleteAction::Restrict,
            }),
        },
    ];

    const INDEXES: &[IndexSchema] = &[IndexSchema {
        id: "posts_name_idx",
        kind: IndexKind::Key,
        attributes: &["name"],
        orders: &[Order::Asc],
    }];

    const COLLECTION: CollectionSchema = CollectionSchema {
        id: "posts",
        name: "Posts",
        document_security: true,
        enabled: true,
        permissions: &["read(\"any\")"],
        attributes: ATTRIBUTES,
        indexes: INDEXES,
    };

    #[test]
    fn validates_schema() {
        assert!(COLLECTION.validate().is_ok());
    }

    #[test]
    fn default_value_float_roundtrip() {
        let d = DefaultValue::float(3.14);
        if let super::StorageValue::Float(v) = d.into_storage() {
            assert!((v - 3.14).abs() < 1e-10);
        } else {
            panic!("expected Float storage value");
        }
    }
}
