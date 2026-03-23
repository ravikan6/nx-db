use crate::context::Context;
use crate::database::Database;
use crate::errors::DatabaseError;
use crate::events::EventBus;
use crate::model::Model;
use crate::query::QuerySpec;
use crate::registry::CollectionRegistry;
use crate::schema::CollectionSchema;
use crate::traits::storage::StorageAdapter;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A database handle bound to a specific [`Context`].
///
/// Use [`ScopedDatabase::repo`] to get a typed [`Repository`] for a model.
pub struct ScopedDatabase<'a, A, R, E> {
    database: &'a Database<A, R, E>,
    context: Context,
}

impl<'a, A, R, E> ScopedDatabase<'a, A, R, E> {
    pub(crate) fn new(database: &'a Database<A, R, E>, context: Context) -> Self {
        Self { database, context }
    }

    /// Returns a reference to the current context.
    pub fn context(&self) -> &Context {
        &self.context
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'a Database<A, R, E> {
        self.database
    }

    /// Returns a [`Repository`] for model `M` using the scoped context.
    pub fn repo<M>(&self) -> Repository<'a, A, R, E, M>
    where
        M: Model,
    {
        Repository::new(self.database, self.context.clone())
    }
}

/// A typed repository that provides CRUD and query operations for model `M`.
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

    /// Returns a reference to the current context.
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
    /// Returns the registered [`CollectionSchema`] for this model.
    pub fn schema(&self) -> Result<&'static CollectionSchema, DatabaseError> {
        self.database.collection(M::schema().id)
    }

    /// Creates the collection table/index in the backing store.
    pub async fn create_collection(&self) -> Result<(), DatabaseError> {
        let collection = self.schema()?;
        self.database
            .create_collection_in_context(&self.context, collection)
            .await
    }

    /// Insert a new document and return the materialized entity.
    pub async fn insert(&self, input: M::Create) -> Result<M::Entity, DatabaseError> {
        self.database.insert_model::<M>(&self.context, input).await
    }

    /// Insert multiple documents in a single batch.
    pub async fn insert_many(
        &self,
        inputs: Vec<M::Create>,
    ) -> Result<Vec<M::Entity>, DatabaseError> {
        self.database
            .insert_many_models::<M>(&self.context, inputs)
            .await
    }

    /// Fetch a single document by its id.  Returns `None` when not found or
    /// when document-level authorization denies access.
    pub async fn get(&self, id: &M::Id) -> Result<Option<M::Entity>, DatabaseError> {
        self.database.get_model::<M>(&self.context, id).await
    }

    /// Update a document by id, applying only the supplied patch fields.
    /// Returns `None` when the document does not exist or is not accessible.
    pub async fn update(
        &self,
        id: &M::Id,
        input: M::Update,
    ) -> Result<Option<M::Entity>, DatabaseError> {
        self.database
            .update_model::<M>(&self.context, id, input)
            .await
    }

    /// Apply the same patch to every document matching `query`.
    /// Returns the number of affected rows.
    pub async fn update_many(
        &self,
        query: QuerySpec,
        input: M::Update,
    ) -> Result<u64, DatabaseError> {
        self.database
            .update_many_models::<M>(&self.context, &query, input)
            .await
    }

    /// Delete a document by id.  Returns `true` if a row was removed.
    pub async fn delete(&self, id: &M::Id) -> Result<bool, DatabaseError> {
        self.database.delete_model::<M>(&self.context, id).await
    }

    /// Delete every document matching `query`.
    /// Returns the number of removed rows.
    pub async fn delete_many(&self, query: QuerySpec) -> Result<u64, DatabaseError> {
        self.database
            .delete_many_models::<M>(&self.context, &query)
            .await
    }

    /// Return all documents matching `query`.
    pub async fn find(&self, query: QuerySpec) -> Result<Vec<M::Entity>, DatabaseError> {
        self.database.find_models::<M>(&self.context, &query).await
    }

    /// Return the first document matching `query`, or `None`.
    pub async fn find_one(&self, query: QuerySpec) -> Result<Option<M::Entity>, DatabaseError> {
        let mut records = self.find(query.limit(1)).await?;
        Ok(records.pop())
    }

    /// Count documents matching `query`.
    pub async fn count(&self, query: QuerySpec) -> Result<u64, DatabaseError> {
        self.database.count_models::<M>(&self.context, &query).await
    }

    // -----------------------------------------------------------------------
    // Relationship helpers
    // -----------------------------------------------------------------------

    /// Load the parent entity for a many-to-one relationship.
    ///
    /// For each entity in `entities`, `extract_fk` should return the foreign
    /// key string (or `None` for nullable FK columns).  The returned map is
    /// keyed by that foreign-key value.
    ///
    /// # Example
    /// ```ignore
    /// // posts → user (many-to-one)
    /// let users = post_repo
    ///     .load_parent::<User>(&posts, |post| post.author_id.as_deref().map(String::from))
    ///     .await?;
    /// ```
    pub async fn load_parent<RM>(
        &self,
        entities: &[M::Entity],
        extract_fk: impl Fn(&M::Entity) -> Option<String>,
    ) -> Result<HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if entities.is_empty() {
            return Ok(HashMap::new());
        }

        // Collect unique non-null foreign keys.
        let mut keys: Vec<String> = Vec::with_capacity(entities.len());
        for entity in entities {
            if let Some(key) = extract_fk(entity) {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let repo = self.database.scope(self.context.clone()).repo::<RM>();
        let query = QuerySpec::new().filter(crate::query::Filter::field(
            crate::system_fields::FIELD_ID,
            crate::query::FilterOp::In(
                keys.into_iter()
                    .map(crate::traits::storage::StorageValue::String)
                    .collect(),
            ),
        ));

        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map = HashMap::with_capacity(related.len());
        for entity in related {
            let id = RM::id_to_string(RM::entity_to_id(&entity)).to_string();
            map.insert(id, entity);
        }

        Ok(map)
    }

    /// Load the child entities for a one-to-many relationship.
    ///
    /// * `extract_local_key` — returns the local entity's primary key string.
    /// * `foreign_field` — the attribute id on `RM` that holds the FK back to `M`.
    /// * `extract_fk` — extracts that FK value from a related entity.
    ///
    /// Returns a map from local key → `Vec` of related entities.
    ///
    /// # Example
    /// ```ignore
    /// // user → posts (one-to-many)
    /// let posts_by_user = user_repo
    ///     .load_children::<Post>(
    ///         &users,
    ///         |u| u.id.to_string(),
    ///         "userId",
    ///         |post| Some(post.user_id.clone()),
    ///     )
    ///     .await?;
    /// ```
    pub async fn load_children<RM>(
        &self,
        entities: &[M::Entity],
        extract_local_key: impl Fn(&M::Entity) -> String,
        foreign_field: &'static str,
        extract_fk: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if entities.is_empty() {
            return Ok(HashMap::new());
        }

        let mut local_keys: Vec<String> = Vec::with_capacity(entities.len());
        for entity in entities {
            let key = extract_local_key(entity);
            if !local_keys.contains(&key) {
                local_keys.push(key);
            }
        }

        let repo = self.database.scope(self.context.clone()).repo::<RM>();
        let query = QuerySpec::new().filter(crate::query::Filter::field(
            foreign_field,
            crate::query::FilterOp::In(
                local_keys
                    .into_iter()
                    .map(crate::traits::storage::StorageValue::String)
                    .collect(),
            ),
        ));

        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map: HashMap<String, Vec<RM::Entity>> = HashMap::new();

        for entity in related {
            if let Some(fk) = extract_fk(&entity) {
                map.entry(fk).or_default().push(entity);
            }
        }

        Ok(map)
    }

    /// Deprecated alias for [`Self::load_parent`].
    #[deprecated(since = "0.2.0", note = "use `load_parent` instead")]
    pub async fn load_many_to_one<RM>(
        &self,
        entities: &[M::Entity],
        extract_foreign_key: impl Fn(&M::Entity) -> Option<String>,
    ) -> Result<HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.load_parent::<RM>(entities, extract_foreign_key).await
    }

    /// Deprecated alias for [`Self::load_children`].
    #[deprecated(since = "0.2.0", note = "use `load_children` instead")]
    pub async fn load_one_to_many<RM>(
        &self,
        entities: &[M::Entity],
        foreign_key_field: &'static str,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_related_foreign_key: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.load_children::<RM>(
            entities,
            extract_local_key,
            foreign_key_field,
            extract_related_foreign_key,
        )
        .await
    }
}
