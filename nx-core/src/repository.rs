use crate::context::Context;
use crate::database::Database;
use crate::errors::DatabaseError;
use crate::events::EventBus;
use crate::model::Model;
use crate::registry::CollectionRegistry;
use crate::schema::CollectionSchema;
use crate::traits::storage::StorageAdapter;
use std::marker::PhantomData;

pub struct ScopedDatabase<'a, A, R, E> {
    database: &'a Database<A, R, E>,
    context: Context,
}

impl<'a, A, R, E> ScopedDatabase<'a, A, R, E> {
    pub(crate) fn new(database: &'a Database<A, R, E>, context: Context) -> Self {
        Self { database, context }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }

    pub fn database(&self) -> &'a Database<A, R, E> {
        self.database
    }

    pub fn repo<M>(&self) -> Repository<'a, A, R, E, M>
    where
        M: Model,
    {
        Repository::new(self.database, self.context.clone())
    }

    pub fn get_repo<M>(&self, _model: M) -> Repository<'a, A, R, E, M>
    where
        M: Model,
    {
        self.repo::<M>()
    }
}

pub struct Repository<'a, A, R, E, M> {
    database: &'a Database<A, R, E>,
    context: Context,
    marker: PhantomData<M>,
}

impl<'a, A, R, E, M> Clone for Repository<'a, A, R, E, M> {
    fn clone(&self) -> Self {
        Self {
            database: self.database,
            context: self.context.clone(),
            marker: PhantomData,
        }
    }
}

impl<'a, A, R, E, M> Repository<'a, A, R, E, M> {
    pub(crate) fn new(database: &'a Database<A, R, E>, context: Context) -> Self {
        Self {
            database,
            context,
            marker: PhantomData,
        }
    }

    pub fn context(&self) -> &Context {
        &self.context
    }
}

impl<'a, A, R, E, M> Repository<'a, A, R, E, M>
where
    A: StorageAdapter,
    R: CollectionRegistry,
    E: EventBus,
    M: Model,
{
    pub fn schema(&self) -> Result<&'static CollectionSchema, DatabaseError> {
        self.database.collection(M::schema().id)
    }

    pub async fn create_collection(&self) -> Result<(), DatabaseError> {
        let collection = self.schema()?;
        self.database
            .create_collection_in_context(&self.context, collection)
            .await
    }

    pub async fn insert(&self, input: M::Create) -> Result<M::Entity, DatabaseError> {
        let collection = self.schema()?;
        let record = M::create_to_record(input, &self.context)?;

        self.database.validate_storage_record(collection, &record)?;

        let stored = self
            .database
            .adapter()
            .insert(&self.context, collection, record)
            .await?;

        M::entity_from_record(stored, &self.context)
    }

    pub async fn get(&self, id: &M::Id) -> Result<Option<M::Entity>, DatabaseError> {
        let collection = self.schema()?;
        let record = self
            .database
            .adapter()
            .get(&self.context, collection, M::id_to_string(id))
            .await?;

        record
            .map(|value| M::entity_from_record(value, &self.context))
            .transpose()
    }

    pub async fn update(
        &self,
        id: &M::Id,
        input: M::Update,
    ) -> Result<Option<M::Entity>, DatabaseError> {
        let collection = self.schema()?;
        let record = M::update_to_record(input, &self.context)?;

        self.database.validate_storage_record(collection, &record)?;

        let stored = self
            .database
            .adapter()
            .update(&self.context, collection, M::id_to_string(id), record)
            .await?;

        stored
            .map(|value| M::entity_from_record(value, &self.context))
            .transpose()
    }

    pub async fn delete(&self, id: &M::Id) -> Result<bool, DatabaseError> {
        let collection = self.schema()?;
        self.database
            .adapter()
            .delete(&self.context, collection, M::id_to_string(id))
            .await
    }
}
