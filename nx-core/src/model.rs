use crate::context::Context;
use crate::errors::DatabaseError;
use crate::schema::CollectionSchema;
use crate::traits::storage::StorageRecord;

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

    fn create_to_record(
        input: Self::Create,
        context: &Context,
    ) -> Result<StorageRecord, DatabaseError>;

    fn update_to_record(
        input: Self::Update,
        context: &Context,
    ) -> Result<StorageRecord, DatabaseError>;

    fn entity_from_record(
        record: StorageRecord,
        context: &Context,
    ) -> Result<Self::Entity, DatabaseError>;
}
