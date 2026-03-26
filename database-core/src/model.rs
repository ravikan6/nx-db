use crate::context::Context;
use crate::errors::DatabaseError;
use crate::schema::CollectionSchema;
use crate::traits::storage::{StorageRecord, StorageValue};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use time::OffsetDateTime;

pub type ModelFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub fn extract_metadata(
    record: &mut crate::traits::storage::StorageRecord,
) -> Result<Metadata, crate::errors::DatabaseError> {
    Ok(Metadata {
        sequence: crate::take_required(record, crate::FIELD_SEQUENCE)?,
        uid: crate::take_required(record, crate::FIELD_ID)?,
        created_at: crate::take_required(record, crate::FIELD_CREATED_AT)?,
        updated_at: crate::take_required(record, crate::FIELD_UPDATED_AT)?,
        permissions: crate::take_required(record, crate::FIELD_PERMISSIONS)?,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    pub sequence: i64,
    pub uid: String,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub permissions: Vec<String>,
}

pub trait EntityRecord: Clone + Send + Sync + 'static {
    type Id: AsRef<str> + Clone + Send + Sync + 'static;

    fn entity_to_id(entity: &Self) -> &Self::Id;
    fn entity_metadata(entity: &Self) -> &Metadata;

    fn from_record(record: StorageRecord, context: &Context) -> Result<Self, DatabaseError>;

    fn resolve_entity<'a>(
        entity: Self,
        _context: &'a Context,
    ) -> ModelFuture<'a, Result<Self, DatabaseError>> {
        Box::pin(async move { Ok(entity) })
    }
}

pub trait CreateRecord: Send + Sync + 'static {
    type Id: crate::GenerateId + AsRef<str> + Clone + Send + Sync + 'static;

    fn create_to_record(self, context: &Context) -> Result<StorageRecord, DatabaseError>;
}

pub trait UpdateRecord: Send + Sync + 'static {
    fn update_to_record(self, context: &Context) -> Result<StorageRecord, DatabaseError>;
}

pub trait Model: Copy + Send + Sync + 'static {
    type Id: AsRef<str> + Clone + Send + Sync + 'static;
    type Entity: Clone + Send + Sync + 'static;
    type Create: Send + Sync + 'static;
    type Update: Send + Sync + 'static;

    fn schema() -> &'static CollectionSchema;

    fn id_to_string(id: &Self::Id) -> &str {
        id.as_ref()
    }

    fn entity_to_id(entity: &Self::Entity) -> &Self::Id;
    fn entity_metadata(entity: &Self::Entity) -> &Metadata;

    fn create_to_record(
        input: Self::Create,
        context: &Context,
    ) -> Result<StorageRecord, DatabaseError>;

    fn encode_record(
        record: StorageRecord,
        _context: &Context,
    ) -> Result<StorageRecord, DatabaseError> {
        Ok(record)
    }

    fn decode_record(
        record: StorageRecord,
        _context: &Context,
    ) -> Result<StorageRecord, DatabaseError> {
        Ok(record)
    }

    fn resolve_entity<'a>(
        entity: Self::Entity,
        _context: &'a Context,
    ) -> ModelFuture<'a, Result<Self::Entity, DatabaseError>> {
        Box::pin(async move { Ok(entity) })
    }

    fn populate_entities<'a>(
        entities: &'a mut [Self::Entity],
        context: &'a Context,
        db: &'a dyn PopulateContext,
    ) -> ModelFuture<'a, Result<(), DatabaseError>> {
        let _ = (entities, context, db);
        Box::pin(async move { Ok(()) })
    }

    fn update_to_record(
        input: Self::Update,
        context: &Context,
    ) -> Result<StorageRecord, DatabaseError>;

    fn entity_from_record(
        record: StorageRecord,
        context: &Context,
    ) -> Result<Self::Entity, DatabaseError>;
}

pub trait PopulateContext: Send + Sync {
    fn populate_related<'a, M: Model>(
        &'a self,
        context: &'a Context,
        entities: &'a mut [M::Entity],
        name: &'static str,
    ) -> ModelFuture<'a, Result<(), DatabaseError>>
    where
        M::Entity: serde::Serialize + serde::de::DeserializeOwned;
}
