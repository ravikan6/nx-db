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

    pub async fn insert_many(
        &self,
        inputs: Vec<M::Create>,
    ) -> Result<Vec<M::Entity>, DatabaseError> {
        self.database
            .insert_many_models::<M>(&self.context, inputs)
            .await
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

    pub async fn update_many(
        &self,
        query: QuerySpec,
        input: M::Update,
    ) -> Result<u64, DatabaseError> {
        self.database
            .update_many_models::<M>(&self.context, &query, input)
            .await
    }

    pub async fn delete(&self, id: &M::Id) -> Result<bool, DatabaseError> {
        self.database.delete_model::<M>(&self.context, id).await
    }

    pub async fn delete_many(&self, query: QuerySpec) -> Result<u64, DatabaseError> {
        self.database
            .delete_many_models::<M>(&self.context, &query)
            .await
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

    pub async fn load_many_to_one<RM>(
        &self,
        entities: &[M::Entity],
        extract_foreign_key: impl Fn(&M::Entity) -> Option<String>,
    ) -> Result<std::collections::HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if entities.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let mut keys = std::collections::HashSet::new();
        for entity in entities {
            if let Some(key) = extract_foreign_key(entity) {
                keys.insert(key);
            }
        }

        if keys.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let repo = self.database.scope(self.context.clone()).repo::<RM>();
        let mut query = QuerySpec::new();
        query = query.filter(crate::query::Filter::field(
            crate::system_fields::FIELD_ID,
            crate::query::FilterOp::In(
                keys.into_iter()
                    .map(crate::traits::storage::StorageValue::String)
                    .collect(),
            ),
        ));


        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map = std::collections::HashMap::with_capacity(related.len());
        for rel in related {
            let id = RM::id_to_string(RM::entity_to_id(&rel)).to_string();
            map.insert(id, rel);
        }

        Ok(map)
    }

    pub async fn load_one_to_many<RM>(
        &self,
        entities: &[M::Entity],
        foreign_key_field: &'static str,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_related_foreign_key: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<std::collections::HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if entities.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let mut keys = std::collections::HashSet::new();
        for entity in entities {
            keys.insert(extract_local_key(entity));
        }

        let repo = self.database.scope(self.context.clone()).repo::<RM>();
        let mut query = QuerySpec::new();
        query = query.filter(crate::query::Filter::field(
            foreign_key_field,
            crate::query::FilterOp::In(
                keys.into_iter()
                    .map(crate::traits::storage::StorageValue::String)
                    .collect(),
            ),
        ));

        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map: std::collections::HashMap<String, Vec<RM::Entity>> =
            std::collections::HashMap::new();

        for rel in related {
            if let Some(fk) = extract_related_foreign_key(&rel) {
                map.entry(fk).or_default().push(rel);
            }
        }

        Ok(map)
    }
}
