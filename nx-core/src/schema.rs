use crate::enums::{
    AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide,
};
use crate::errors::DatabaseError;
use std::collections::BTreeSet;

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
    pub fn attribute(&self, id: &str) -> Option<&AttributeSchema> {
        self.attributes.iter().find(|attribute| attribute.id == id)
    }

    pub fn persisted_attributes(&self) -> impl Iterator<Item = &AttributeSchema> {
        self.attributes
            .iter()
            .filter(|attribute| attribute.persistence == AttributePersistence::Persisted)
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttributeSchema {
    pub id: &'static str,
    pub column: &'static str,
    pub kind: AttributeKind,
    pub required: bool,
    pub array: bool,
    pub persistence: AttributePersistence,
    pub filters: &'static [&'static str],
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
    pub on_delete: OnDeleteAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexSchema {
    pub id: &'static str,
    pub kind: IndexKind,
    pub attributes: &'static [&'static str],
    pub orders: &'static [Order],
}

#[cfg(test)]
mod tests {
    use super::{
        AttributePersistence, AttributeSchema, CollectionSchema, IndexSchema, RelationshipSchema,
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
            persistence: AttributePersistence::Persisted,
            filters: &[],
            relationship: None,
        },
        AttributeSchema {
            id: "author",
            column: "author_id",
            kind: AttributeKind::Relationship,
            required: false,
            array: false,
            persistence: AttributePersistence::Persisted,
            filters: &[],
            relationship: Some(RelationshipSchema {
                related_collection: "users",
                kind: RelationshipKind::ManyToOne,
                side: RelationshipSide::Parent,
                two_way: false,
                two_way_key: None,
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
}
