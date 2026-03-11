use crate::errors::DatabaseError;
use crate::schema::CollectionSchema;
use std::collections::BTreeMap;

pub trait CollectionRegistry {
    type Iter<'a>: Iterator<Item = &'static CollectionSchema> + 'a
    where
        Self: 'a;

    fn get(&self, id: &str) -> Option<&'static CollectionSchema>;
    fn iter(&self) -> Self::Iter<'_>;

    fn validate(&self) -> Result<(), DatabaseError> {
        for collection in self.iter() {
            collection.validate()?;
        }

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct StaticRegistry {
    collections: BTreeMap<&'static str, &'static CollectionSchema>,
}

impl StaticRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        mut self,
        collection: &'static CollectionSchema,
    ) -> Result<Self, DatabaseError> {
        if self.collections.insert(collection.id, collection).is_some() {
            return Err(DatabaseError::Other(format!(
                "collection '{}' is already registered",
                collection.id
            )));
        }

        Ok(self)
    }

    pub fn extend<I>(mut self, collections: I) -> Result<Self, DatabaseError>
    where
        I: IntoIterator<Item = &'static CollectionSchema>,
    {
        for collection in collections {
            self = self.register(collection)?;
        }

        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.collections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }
}

impl CollectionRegistry for StaticRegistry {
    type Iter<'a>
        = std::iter::Copied<
        std::collections::btree_map::Values<'a, &'static str, &'static CollectionSchema>,
    >
    where
        Self: 'a;

    fn get(&self, id: &str) -> Option<&'static CollectionSchema> {
        self.collections.get(id).copied()
    }

    fn iter(&self) -> Self::Iter<'_> {
        self.collections.values().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::{CollectionRegistry, StaticRegistry};
    use crate::enums::AttributeKind;
    use crate::schema::{AttributePersistence, AttributeSchema, CollectionSchema};

    const ATTRIBUTES: &[AttributeSchema] = &[AttributeSchema {
        id: "name",
        column: "name",
        kind: AttributeKind::String,
        required: true,
        array: false,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    }];

    static USERS: CollectionSchema = CollectionSchema {
        id: "users",
        name: "Users",
        document_security: true,
        enabled: true,
        permissions: &["read(\"any\")"],
        attributes: ATTRIBUTES,
        indexes: &[],
    };

    #[test]
    fn registers_and_reads_collections() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");

        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.get("users").map(|schema| schema.name),
            Some("Users")
        );
        assert!(registry.validate().is_ok());
    }
}
