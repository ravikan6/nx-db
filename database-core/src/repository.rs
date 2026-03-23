use crate::context::Context;
use crate::database::Database;
use crate::errors::DatabaseError;
use crate::events::EventBus;
use crate::model::Model;
use crate::query::{Filter, FilterOp, QuerySpec, Rel};
use crate::registry::CollectionRegistry;
use crate::schema::CollectionSchema;
use crate::traits::storage::{StorageAdapter, StorageValue};
use crate::value::Populated;
use std::collections::HashMap;
use std::marker::PhantomData;

/// A database handle bound to a specific [`Context`].
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

/// A typed repository that provides CRUD, query, and relationship-loading
/// operations for model `M`.
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

    /// Creates the collection table/indexes in the backing store.
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

    /// Insert multiple documents in a single batch operation.
    pub async fn insert_many(
        &self,
        inputs: Vec<M::Create>,
    ) -> Result<Vec<M::Entity>, DatabaseError> {
        self.database
            .insert_many_models::<M>(&self.context, inputs)
            .await
    }

    /// Fetch a single document by id.  Returns `None` when not found or when
    /// document-level authorization denies access.
    pub async fn get(&self, id: &M::Id) -> Result<Option<M::Entity>, DatabaseError> {
        self.database.get_model::<M>(&self.context, id).await
    }

    /// Update a document by id, applying only the supplied patch fields.
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
    pub async fn update_many(
        &self,
        query: QuerySpec,
        input: M::Update,
    ) -> Result<u64, DatabaseError> {
        self.database
            .update_many_models::<M>(&self.context, &query, input)
            .await
    }

    /// Delete a document by id.
    pub async fn delete(&self, id: &M::Id) -> Result<bool, DatabaseError> {
        self.database.delete_model::<M>(&self.context, id).await
    }

    /// Delete every document matching `query`.
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

    // ── Batch relationship loading ─────────────────────────────────────────

    /// Batch-load the **parent** entities for a many-to-one relationship and
    /// return them as a `HashMap` keyed by the FK value.
    ///
    /// Executes exactly **one** `IN (...)` query regardless of how many
    /// entities are passed.
    ///
    /// # Example
    /// ```ignore
    /// // Load the author for every post (posts -> user)
    /// let users = post_repo
    ///     .load_parent::<User>(&posts, |p| Some(p.author_id.clone()))
    ///     .await?;
    /// let author = users.get(&posts[0].author_id);
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
        let query = QuerySpec::new().filter(Filter::field(
            crate::system_fields::FIELD_ID,
            FilterOp::In(keys.into_iter().map(StorageValue::String).collect()),
        ));

        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map = HashMap::with_capacity(related.len());
        for entity in related {
            map.insert(
                RM::id_to_string(RM::entity_to_id(&entity)).to_string(),
                entity,
            );
        }
        Ok(map)
    }

    /// Batch-load **child** entities for a one-to-many relationship and return
    /// them as a `HashMap` keyed by the local entity's ID.
    ///
    /// Executes exactly **one** `IN (...)` query.
    ///
    /// # Example
    /// ```ignore
    /// // Load all posts for each user (user -> posts)
    /// let posts_map = user_repo
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
        let query = QuerySpec::new().filter(Filter::field(
            foreign_field,
            FilterOp::In(local_keys.into_iter().map(StorageValue::String).collect()),
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

    // ── In-place population ────────────────────────────────────────────────

    /// Populate a many-to-one relationship **in-place** on a slice of entities.
    ///
    /// Uses a single batch query (same as [`load_parent`]).  After this call,
    /// `set` is invoked once per entity with `Populated::Loaded(Some(related))`
    /// or `Populated::Loaded(None)` when the FK is null or the related entity
    /// was not found.
    ///
    /// # Example
    /// ```ignore
    /// let mut posts = post_repo.find(QuerySpec::new()).await?;
    /// post_repo
    ///     .populate_parent::<User>(
    ///         &mut posts,
    ///         |p| Some(p.author_id.clone()),
    ///         |p, loaded| p.author = loaded,
    ///     )
    ///     .await?;
    /// // posts[0].author == Populated::Loaded(Some(UserEntity { ... }))
    /// ```
    pub async fn populate_parent<RM>(
        &self,
        entities: &mut Vec<M::Entity>,
        extract_fk: impl Fn(&M::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, Populated<RM::Entity>),
    ) -> Result<(), DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let map = self.load_parent::<RM>(entities, &extract_fk).await?;
        for entity in entities.iter_mut() {
            let loaded = match extract_fk(entity) {
                Some(fk) => Populated::Loaded(map.get(&fk).cloned()),
                None => Populated::Loaded(None),
            };
            set(entity, loaded);
        }
        Ok(())
    }

    /// Populate a one-to-many relationship **in-place** on a slice of entities.
    ///
    /// Uses a single batch query.  After this call, `set` is invoked for every
    /// entity with the (possibly empty) `Vec` of child entities.
    ///
    /// # Example
    /// ```ignore
    /// let mut users = user_repo.find(QuerySpec::new()).await?;
    /// user_repo
    ///     .populate_children::<Post>(
    ///         &mut users,
    ///         |u| u.id.to_string(),
    ///         "userId",
    ///         |post| Some(post.user_id.clone()),
    ///         |u, posts| u.posts = Populated::Loaded(Some(posts)),
    ///     )
    ///     .await?;
    /// ```
    pub async fn populate_children<RM>(
        &self,
        entities: &mut Vec<M::Entity>,
        extract_local_key: impl Fn(&M::Entity) -> String,
        foreign_field: &'static str,
        extract_fk: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, Vec<RM::Entity>),
    ) -> Result<(), DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let map = self
            .load_children::<RM>(entities, &extract_local_key, foreign_field, &extract_fk)
            .await?;
        for entity in entities.iter_mut() {
            let local_key = extract_local_key(entity);
            let children = map.get(&local_key).cloned().unwrap_or_default();
            set(entity, children);
        }
        Ok(())
    }

    // ── Combined find + populate ───────────────────────────────────────────

    /// Find entities and populate a many-to-one relationship in **one call**
    /// (two queries total).
    ///
    /// # Example
    /// ```ignore
    /// let posts = post_repo
    ///     .find_with_parent::<User>(
    ///         QuerySpec::new(),
    ///         |p| Some(p.author_id.clone()),
    ///         |p, loaded| p.author = loaded,
    ///     )
    ///     .await?;
    /// ```
    pub async fn find_with_parent<RM>(
        &self,
        query: QuerySpec,
        extract_fk: impl Fn(&M::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, Populated<RM::Entity>),
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut entities = self.find(query).await?;
        self.populate_parent::<RM>(&mut entities, extract_fk, set)
            .await?;
        Ok(entities)
    }

    /// Find entities and populate a one-to-many relationship in **one call**
    /// (two queries total).
    pub async fn find_with_children<RM>(
        &self,
        query: QuerySpec,
        extract_local_key: impl Fn(&M::Entity) -> String,
        foreign_field: &'static str,
        extract_fk: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, Vec<RM::Entity>),
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut entities = self.find(query).await?;
        self.populate_children::<RM>(
            &mut entities,
            extract_local_key,
            foreign_field,
            extract_fk,
            set,
        )
        .await?;
        Ok(entities)
    }

    // ── Typed Rel-based helpers ────────────────────────────────────────────

    /// Like [`load_parent`] but driven by a [`Rel`] descriptor instead of a
    /// raw field name string.
    ///
    /// The `Rel` stores the local FK attribute id; you still supply the closure
    /// that extracts the runtime FK value from each entity.
    pub async fn load_related_parent<RM>(
        &self,
        entities: &[M::Entity],
        _rel: Rel<M, RM>,
        extract_fk: impl Fn(&M::Entity) -> Option<String>,
    ) -> Result<HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.load_parent::<RM>(entities, extract_fk).await
    }

    /// Like [`load_children`] but driven by a [`Rel`] descriptor.
    pub async fn load_related_children<RM>(
        &self,
        entities: &[M::Entity],
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_fk: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.load_children::<RM>(entities, extract_local_key, rel.remote_field, extract_fk)
            .await
    }

    // ── Deprecated aliases ─────────────────────────────────────────────────

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
