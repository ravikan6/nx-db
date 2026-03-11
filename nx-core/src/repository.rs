use crate::context::Context;
use crate::database::Database;
use crate::errors::DatabaseError;
use crate::events::EventBus;
use crate::model::Model;
use crate::query::QuerySpec;
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
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
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
        self.database.insert_model::<M>(&self.context, input).await
    }

    pub async fn get(&self, id: &M::Id) -> Result<Option<M::Entity>, DatabaseError> {
        self.database.get_model::<M>(&self.context, id).await
    }

    pub async fn update(
        &self,
        id: &M::Id,
        input: M::Update,
    ) -> Result<Option<M::Entity>, DatabaseError> {
        self.database
            .update_model::<M>(&self.context, id, input)
            .await
    }

    pub async fn delete(&self, id: &M::Id) -> Result<bool, DatabaseError> {
        self.database.delete_model::<M>(&self.context, id).await
    }

    pub async fn find(&self, query: QuerySpec) -> Result<Vec<M::Entity>, DatabaseError> {
        self.database.find_models::<M>(&self.context, &query).await
    }

    pub async fn find_one(&self, query: QuerySpec) -> Result<Option<M::Entity>, DatabaseError> {
        let mut records = self.find(query.limit(1)).await?;
        Ok(records.pop())
    }

    pub async fn count(&self, query: QuerySpec) -> Result<u64, DatabaseError> {
        self.database.count_models::<M>(&self.context, &query).await
    }
}
