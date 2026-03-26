use crate::context::Context;
use crate::database::Database;
use crate::enums::RelationshipKind;
use crate::errors::DatabaseError;
use crate::events::EventBus;
use crate::model::Model;
use crate::query::{Filter, FilterOp, PopulateMany, PopulateOne, QueryInclude, QuerySpec, Rel};
use crate::registry::CollectionRegistry;
use crate::schema::CollectionSchema;
use crate::traits::storage::{
    PopulatedStorageRow, StorageAdapter, StoragePopulate, StorageRelation, StorageThroughRelation,
    StorageValue,
};
use crate::value::{Populated, RelationMany, RelationOne};
use std::collections::{HashMap, HashSet};
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

pub trait PopulateDescriptor<'repo, A, R, E, M>: Send + Sync
where
    A: StorageAdapter,
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    fn include(&self) -> QueryInclude;

    fn storage_populate(
        &self,
        repo: &Repository<'repo, A, R, E, M>,
    ) -> Result<StoragePopulate, DatabaseError>;

    fn try_execute<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        query: &'a QuerySpec,
    ) -> crate::model::ModelFuture<'a, Result<Option<Vec<M::Entity>>, DatabaseError>>;

    fn apply_joined<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
        related_by_base: &'a HashMap<String, Vec<crate::traits::storage::StorageRecord>>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>>;

    fn populate<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>>;
}

pub struct RepositoryQuery<'a, A, R, E, M>
where
    A: StorageAdapter,
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    repo: Repository<'a, A, R, E, M>,
    query: QuerySpec,
    populates: Vec<Box<dyn PopulateDescriptor<'a, A, R, E, M> + 'a>>,
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

impl<'repo, A, R, E, M, RM> PopulateDescriptor<'repo, A, R, E, M> for PopulateOne<M, RM>
where
    A: StorageAdapter,
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    RM: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    fn include(&self) -> QueryInclude {
        PopulateOne::include(self)
    }

    fn storage_populate(
        &self,
        repo: &Repository<'repo, A, R, E, M>,
    ) -> Result<StoragePopulate, DatabaseError> {
        Ok(StoragePopulate {
            name: self.include().name,
            schema: repo.database.collection(RM::schema().id)?,
            relation: repo.storage_relation(self.rel)?,
        })
    }

    fn try_execute<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        query: &'a QuerySpec,
    ) -> crate::model::ModelFuture<'a, Result<Option<Vec<M::Entity>>, DatabaseError>> {
        Box::pin(async move {
            repo.find_including_one_via_adapter(query, self.rel, &self.set)
                .await
        })
    }

    fn apply_joined<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
        related_by_base: &'a HashMap<String, Vec<crate::traits::storage::StorageRecord>>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>> {
        Box::pin(async move {
            let related_schema = repo.database.collection(RM::schema().id)?;
            for entity in entities.iter_mut() {
                let base_id = M::id_to_string(M::entity_to_id(entity)).to_string();
                let related = match related_by_base
                    .get(&base_id)
                    .and_then(|records| records.first())
                {
                    Some(record) => Some(
                        repo.database
                            .materialize_entity_fast::<RM>(
                                &repo.context,
                                related_schema,
                                record.clone(),
                                false,
                            )
                            .await?,
                    ),
                    None => None,
                };
                (self.set)(entity, RelationOne::Loaded(related));
            }
            Ok(())
        })
    }

    fn populate<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>> {
        Box::pin(async move {
            repo.populate_related_one::<RM>(
                entities,
                self.rel,
                self.extract_local_key,
                self.extract_remote_key,
                self.set,
            )
            .await
        })
    }
}

impl<'repo, A, R, E, M, RM> PopulateDescriptor<'repo, A, R, E, M> for PopulateMany<M, RM>
where
    A: StorageAdapter,
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    RM: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    fn include(&self) -> QueryInclude {
        PopulateMany::include(self)
    }

    fn storage_populate(
        &self,
        repo: &Repository<'repo, A, R, E, M>,
    ) -> Result<StoragePopulate, DatabaseError> {
        Ok(StoragePopulate {
            name: self.include().name,
            schema: repo.database.collection(RM::schema().id)?,
            relation: repo.storage_relation(self.rel)?,
        })
    }

    fn try_execute<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        query: &'a QuerySpec,
    ) -> crate::model::ModelFuture<'a, Result<Option<Vec<M::Entity>>, DatabaseError>> {
        Box::pin(async move {
            repo.find_including_many_via_adapter(query, self.rel, &self.set)
                .await
        })
    }

    fn apply_joined<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
        related_by_base: &'a HashMap<String, Vec<crate::traits::storage::StorageRecord>>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>> {
        Box::pin(async move {
            let related_schema = repo.database.collection(RM::schema().id)?;
            for entity in entities.iter_mut() {
                let base_id = M::id_to_string(M::entity_to_id(entity)).to_string();
                let mut related_entities = Vec::new();
                let mut seen_related = HashSet::new();
                if let Some(records) = related_by_base.get(&base_id) {
                    for record in records {
                        let Some(related_id) = record_id(record) else {
                            continue;
                        };
                        if !seen_related.insert(related_id) {
                            continue;
                        }
                        related_entities.push(
                            repo.database
                                .materialize_entity_fast::<RM>(
                                    &repo.context,
                                    related_schema,
                                    record.clone(),
                                    false,
                                )
                                .await?,
                        );
                    }
                }
                (self.set)(entity, RelationMany::Loaded(related_entities));
            }
            Ok(())
        })
    }

    fn populate<'a>(
        &'a self,
        repo: &'a Repository<'repo, A, R, E, M>,
        entities: &'a mut Vec<M::Entity>,
    ) -> crate::model::ModelFuture<'a, Result<(), DatabaseError>> {
        Box::pin(async move {
            repo.populate_related_many::<RM>(
                entities,
                self.rel,
                self.extract_local_key,
                self.extract_remote_key,
                self.set,
            )
            .await
        })
    }
}

impl<'a, A, R, E, M> RepositoryQuery<'a, A, R, E, M>
where
    A: StorageAdapter,
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    fn new(repo: Repository<'a, A, R, E, M>, query: QuerySpec) -> Self {
        Self {
            repo,
            query,
            populates: Vec::new(),
        }
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.query = self.query.filter(filter);
        self
    }

    pub fn try_filter(
        mut self,
        filter: Result<Filter, DatabaseError>,
    ) -> Result<Self, DatabaseError> {
        self.query = self.query.try_filter(filter)?;
        Ok(self)
    }

    pub fn sort(mut self, sort: crate::query::Sort) -> Self {
        self.query = self.query.sort(sort);
        self
    }

    pub fn select(mut self, fields: Vec<&'static str>) -> Self {
        self.query = self.query.select(fields);
        self
    }

    pub fn include<I>(mut self, include: I) -> Self
    where
        I: Into<QueryInclude>,
    {
        self.query = self.query.include(include);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query = self.query.limit(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.query = self.query.offset(offset);
        self
    }

    pub fn populate<P>(mut self, descriptor: P) -> Self
    where
        P: PopulateDescriptor<'a, A, R, E, M> + 'a,
    {
        self.query = self.query.include(descriptor.include());
        self.populates.push(Box::new(descriptor));
        self
    }

    pub fn spec(&self) -> &QuerySpec {
        &self.query
    }

    pub async fn all(self) -> Result<Vec<M::Entity>, DatabaseError> {
        if self.populates.is_empty() {
            return self
                .repo
                .database
                .find_models::<M>(&self.repo.context, &self.query)
                .await;
        }

        if let Some(entities) = self.try_find_populated_via_adapter().await? {
            return Ok(entities);
        }

        let mut executed_join_index = None;
        let mut entities = if let Some(first) = self.populates.first() {
            if let Some(loaded) = first.try_execute(&self.repo, &self.query).await? {
                executed_join_index = Some(0);
                loaded
            } else {
                self.repo
                    .database
                    .find_models::<M>(&self.repo.context, &self.query)
                    .await?
            }
        } else {
            unreachable!("guarded above")
        };

        for (index, populate) in self.populates.iter().enumerate() {
            if executed_join_index == Some(index) {
                continue;
            }
            populate.populate(&self.repo, &mut entities).await?;
        }

        Ok(entities)
    }

    pub async fn one(mut self) -> Result<Option<M::Entity>, DatabaseError> {
        self.query = self.query.limit(1);
        let mut entities = self.all().await?;
        Ok(entities.pop())
    }

    async fn try_find_populated_via_adapter(
        &self,
    ) -> Result<Option<Vec<M::Entity>>, DatabaseError> {
        if self.populates.is_empty()
            || !self
                .repo
                .database
                .adapter()
                .enforces_document_filtering(crate::utils::PermissionEnum::Read)
        {
            return Ok(None);
        }

        let base_schema = self.repo.database.collection(M::schema().id)?;
        self.repo
            .database
            .validate_query(base_schema, &self.query)?;

        let mut plans = Vec::with_capacity(self.populates.len());
        for populate in &self.populates {
            plans.push(populate.storage_populate(&self.repo)?);
        }

        let Some(rows) = self
            .repo
            .database
            .adapter()
            .find_populated(&self.repo.context, base_schema, &self.query, plans)
            .await?
        else {
            return Ok(None);
        };

        self.materialize_populated_rows(base_schema, rows)
            .await
            .map(Some)
    }

    async fn materialize_populated_rows(
        &self,
        base_schema: &'static CollectionSchema,
        rows: Vec<PopulatedStorageRow>,
    ) -> Result<Vec<M::Entity>, DatabaseError> {
        let mut entities = Vec::new();
        let mut index_by_id = HashMap::new();
        let mut related_by_name: HashMap<
            String,
            HashMap<String, Vec<crate::traits::storage::StorageRecord>>,
        > = HashMap::new();

        for row in rows {
            let Some(base_id) = record_id(&row.base) else {
                continue;
            };

            if !index_by_id.contains_key(&base_id) {
                let entity = self
                    .repo
                    .database
                    .materialize_entity_fast::<M>(&self.repo.context, base_schema, row.base, false)
                    .await?;
                index_by_id.insert(base_id.clone(), entities.len());
                entities.push(entity);
            }

            for (name, maybe_record) in row.related {
                let Some(record) = maybe_record else {
                    continue;
                };
                related_by_name
                    .entry(name)
                    .or_default()
                    .entry(base_id.clone())
                    .or_default()
                    .push(record);
            }
        }

        for populate in &self.populates {
            let related = related_by_name
                .remove(populate.include().name)
                .unwrap_or_default();
            populate
                .apply_joined(&self.repo, &mut entities, &related)
                .await?;
        }

        Ok(entities)
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
    R: CollectionRegistry + Sync,
    E: EventBus + Sync,
    M: Model,
    M::Entity: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn query(&self) -> RepositoryQuery<'a, A, R, E, M> {
        RepositoryQuery::new(self.clone(), QuerySpec::new())
    }

    pub fn query_spec(&self, query: QuerySpec) -> RepositoryQuery<'a, A, R, E, M> {
        RepositoryQuery::new(self.clone(), query)
    }

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

    /// Return all documents matching `query`, with relationships populated.
    pub async fn find_including(&self, query: QuerySpec) -> Result<Vec<M::Entity>, DatabaseError>
    where
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut entities = self.find(query).await?;
        self.populate_many(&mut entities).await?;
        Ok(entities)
    }

    /// Return the first document matching `query`, with relationships populated.
    pub async fn find_one_including(
        &self,
        query: QuerySpec,
    ) -> Result<Option<M::Entity>, DatabaseError>
    where
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let mut entities = self.find_including(query.limit(1)).await?;
        Ok(entities.pop())
    }

    /// Count documents matching `query`.
    pub async fn count(&self, query: QuerySpec) -> Result<u64, DatabaseError> {
        self.database.count_models::<M>(&self.context, &query).await
    }

    /// Batch-populate relationships for a slice of entities.
    pub async fn populate_many(&self, entities: &mut [M::Entity]) -> Result<(), DatabaseError>
    where
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        M::populate_entities(entities, &self.context, self.database).await
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
    #[deprecated(since = "0.2.0", note = "use `load_related_one` instead")]
    pub async fn load_related_parent<RM>(
        &self,
        entities: &[M::Entity],
        rel: Rel<M, RM>,
        extract_fk: impl Fn(&M::Entity) -> Option<String>,
    ) -> Result<HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.load_related_one::<RM>(entities, rel, extract_fk, |entity| {
            Some(RM::id_to_string(RM::entity_to_id(entity)).to_string())
        })
        .await
    }

    /// Like [`load_children`] but driven by a [`Rel`] descriptor.
    #[deprecated(since = "0.2.0", note = "use `load_related_many` instead")]
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
        self.load_related_many::<RM>(entities, rel, extract_local_key, extract_fk)
            .await
    }

    /// Load any to-one relationship (`many-to-one` or `one-to-one`) using a
    /// typed [`Rel`] descriptor.
    pub async fn load_related_one<RM>(
        &self,
        entities: &[M::Entity],
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> Option<String>,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<HashMap<String, RM::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if !rel.is_to_one() {
            return Err(DatabaseError::Other(format!(
                "relationship '{}' is {:?}, expected a to-one relation",
                rel.name, rel.kind
            )));
        }

        if entities.is_empty() {
            return Ok(HashMap::new());
        }

        let mut keys: Vec<String> = Vec::with_capacity(entities.len());
        for entity in entities {
            if let Some(key) = extract_local_key(entity) {
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
            rel.remote_field,
            FilterOp::In(keys.into_iter().map(StorageValue::String).collect()),
        ));

        let related: Vec<RM::Entity> = repo.find(query).await?;
        let mut map = HashMap::with_capacity(related.len());
        for entity in related {
            if let Some(key) = extract_remote_key(&entity) {
                map.entry(key).or_insert(entity);
            }
        }
        Ok(map)
    }

    /// Load any to-many relationship (`one-to-many` or `many-to-many`) using
    /// a typed [`Rel`] descriptor.
    pub async fn load_related_many<RM>(
        &self,
        entities: &[M::Entity],
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
    ) -> Result<HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if !rel.is_to_many() {
            return Err(DatabaseError::Other(format!(
                "relationship '{}' is {:?}, expected a to-many relation",
                rel.name, rel.kind
            )));
        }

        match rel.kind {
            RelationshipKind::OneToMany => {
                self.load_children::<RM>(
                    entities,
                    extract_local_key,
                    rel.remote_field,
                    extract_remote_key,
                )
                .await
            }
            RelationshipKind::ManyToMany => {
                self.load_related_many_through::<RM>(entities, rel, extract_local_key)
                    .await
            }
            _ => unreachable!("guarded above"),
        }
    }

    pub async fn populate_related_one<RM>(
        &self,
        entities: &mut Vec<M::Entity>,
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> Option<String>,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, RelationOne<RM::Entity>),
    ) -> Result<(), DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let map = self
            .load_related_one::<RM>(entities, rel, &extract_local_key, extract_remote_key)
            .await?;
        for entity in entities.iter_mut() {
            let loaded = match extract_local_key(entity) {
                Some(key) => RelationOne::Loaded(map.get(&key).cloned()),
                None => RelationOne::Loaded(None),
            };
            set(entity, loaded);
        }
        Ok(())
    }

    pub async fn populate_related_many<RM>(
        &self,
        entities: &mut Vec<M::Entity>,
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, RelationMany<RM::Entity>),
    ) -> Result<(), DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let map = self
            .load_related_many::<RM>(entities, rel, &extract_local_key, extract_remote_key)
            .await?;
        for entity in entities.iter_mut() {
            let local_key = extract_local_key(entity);
            let related = map.get(&local_key).cloned().unwrap_or_default();
            set(entity, RelationMany::Loaded(related));
        }
        Ok(())
    }

    pub async fn find_including_one<RM>(
        &self,
        query: QuerySpec,
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> Option<String>,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, RelationOne<RM::Entity>),
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let query = query.include(rel);
        if let Some(entities) = self
            .find_including_one_via_adapter(&query, rel, &set)
            .await?
        {
            return Ok(entities);
        }

        let mut entities = self.find(query).await?;
        self.populate_related_one::<RM>(
            &mut entities,
            rel,
            extract_local_key,
            extract_remote_key,
            set,
        )
        .await?;
        Ok(entities)
    }

    pub async fn find_including_many<RM>(
        &self,
        query: QuerySpec,
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> String,
        extract_remote_key: impl Fn(&RM::Entity) -> Option<String>,
        set: impl Fn(&mut M::Entity, RelationMany<RM::Entity>),
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let query = query.include(rel);
        if let Some(entities) = self
            .find_including_many_via_adapter(&query, rel, &set)
            .await?
        {
            return Ok(entities);
        }

        let mut entities = self.find(query).await?;
        self.populate_related_many::<RM>(
            &mut entities,
            rel,
            extract_local_key,
            extract_remote_key,
            set,
        )
        .await?;
        Ok(entities)
    }

    async fn find_including_one_via_adapter<RM>(
        &self,
        query: &QuerySpec,
        rel: Rel<M, RM>,
        set: &impl Fn(&mut M::Entity, RelationOne<RM::Entity>),
    ) -> Result<Option<Vec<M::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if !rel.is_to_one()
            || !self
                .database
                .adapter()
                .enforces_document_filtering(crate::utils::PermissionEnum::Read)
        {
            return Ok(None);
        }

        let base_schema = self.database.collection(M::schema().id)?;
        let related_schema = self.database.collection(RM::schema().id)?;
        self.database.validate_query(base_schema, query)?;

        let Some(joined) = self
            .database
            .adapter()
            .find_related(
                &self.context,
                base_schema,
                related_schema,
                query,
                self.storage_relation(rel)?,
            )
            .await?
        else {
            return Ok(None);
        };

        let mut entities = Vec::with_capacity(joined.len());
        let mut seen_ids = HashSet::new();

        for joined_record in joined {
            let Some(base_id) = record_id(&joined_record.base) else {
                continue;
            };
            if !seen_ids.insert(base_id) {
                continue;
            }

            let mut entity = self
                .database
                .materialize_entity_fast::<M>(&self.context, base_schema, joined_record.base, false)
                .await?;
            let related = match joined_record.related {
                Some(record) => Some(
                    self.database
                        .materialize_entity_fast::<RM>(&self.context, related_schema, record, false)
                        .await?,
                ),
                None => None,
            };
            set(&mut entity, RelationOne::Loaded(related));
            entities.push(entity);
        }

        Ok(Some(entities))
    }

    async fn find_including_many_via_adapter<RM>(
        &self,
        query: &QuerySpec,
        rel: Rel<M, RM>,
        set: &impl Fn(&mut M::Entity, RelationMany<RM::Entity>),
    ) -> Result<Option<Vec<M::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if !rel.is_to_many()
            || !self
                .database
                .adapter()
                .enforces_document_filtering(crate::utils::PermissionEnum::Read)
        {
            return Ok(None);
        }

        let base_schema = self.database.collection(M::schema().id)?;
        let related_schema = self.database.collection(RM::schema().id)?;
        self.database.validate_query(base_schema, query)?;

        let Some(joined) = self
            .database
            .adapter()
            .find_related(
                &self.context,
                base_schema,
                related_schema,
                query,
                self.storage_relation(rel)?,
            )
            .await?
        else {
            return Ok(None);
        };

        let mut entities = Vec::new();
        let mut index_by_id = HashMap::new();
        let mut related_by_id: HashMap<String, Vec<RM::Entity>> = HashMap::new();

        for joined_record in joined {
            let Some(base_id) = record_id(&joined_record.base) else {
                continue;
            };

            if !index_by_id.contains_key(&base_id) {
                let entity = self
                    .database
                    .materialize_entity_fast::<M>(
                        &self.context,
                        base_schema,
                        joined_record.base,
                        false,
                    )
                    .await?;
                index_by_id.insert(base_id.clone(), entities.len());
                entities.push(entity);
            }

            if let Some(record) = joined_record.related {
                let related = self
                    .database
                    .materialize_entity_fast::<RM>(&self.context, related_schema, record, false)
                    .await?;
                related_by_id.entry(base_id).or_default().push(related);
            }
        }

        for entity in &mut entities {
            let local_id = M::id_to_string(M::entity_to_id(entity)).to_string();
            let related = related_by_id.remove(&local_id).unwrap_or_default();
            set(entity, RelationMany::Loaded(related));
        }

        Ok(Some(entities))
    }

    async fn load_related_many_through<RM>(
        &self,
        entities: &[M::Entity],
        rel: Rel<M, RM>,
        extract_local_key: impl Fn(&M::Entity) -> String,
    ) -> Result<HashMap<String, Vec<RM::Entity>>, DatabaseError>
    where
        RM: Model,
        RM::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let Some(through) = rel.through else {
            return Err(DatabaseError::Other(format!(
                "relationship '{}' is many-to-many but missing through metadata",
                rel.name
            )));
        };

        if entities.is_empty() {
            return Ok(HashMap::new());
        }

        if !self
            .database
            .adapter()
            .enforces_document_filtering(crate::utils::PermissionEnum::Read)
        {
            return Err(DatabaseError::Other(format!(
                "relationship '{}' many-to-many loading requires adapter-level document filtering",
                rel.name
            )));
        }

        let through_schema = self.database.collection(through.collection)?;
        let mut local_keys = Vec::with_capacity(entities.len());
        for entity in entities {
            let key = extract_local_key(entity);
            if !local_keys.contains(&key) {
                local_keys.push(key);
            }
        }

        let through_query = QuerySpec::new().filter(Filter::field(
            through.local_field,
            FilterOp::In(
                local_keys
                    .iter()
                    .cloned()
                    .map(StorageValue::String)
                    .collect(),
            ),
        ));
        self.database
            .validate_query(through_schema, &through_query)?;
        let through_records = self
            .database
            .adapter()
            .find(&self.context, through_schema, &through_query)
            .await?;

        let mut related_ids = Vec::new();
        let mut related_ids_by_local: HashMap<String, Vec<String>> = HashMap::new();
        for record in through_records {
            let Some(local_key) = record_string(&record, through.local_field) else {
                continue;
            };
            let Some(remote_key) = record_string(&record, through.remote_field) else {
                continue;
            };
            if !related_ids.contains(&remote_key) {
                related_ids.push(remote_key.clone());
            }
            related_ids_by_local
                .entry(local_key)
                .or_default()
                .push(remote_key);
        }

        if related_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let repo = self.database.scope(self.context.clone()).repo::<RM>();
        let related = repo
            .find(QuerySpec::new().filter(Filter::field(
                rel.remote_field,
                FilterOp::In(related_ids.into_iter().map(StorageValue::String).collect()),
            )))
            .await?;

        let mut entities_by_id = HashMap::with_capacity(related.len());
        for entity in related {
            let id = RM::id_to_string(RM::entity_to_id(&entity)).to_string();
            entities_by_id.insert(id, entity);
        }

        let mut map = HashMap::new();
        for (local_key, remote_keys) in related_ids_by_local {
            let mut loaded = Vec::new();
            for remote_key in remote_keys {
                if let Some(entity) = entities_by_id.get(&remote_key) {
                    loaded.push(entity.clone());
                }
            }
            map.insert(local_key, loaded);
        }

        Ok(map)
    }

    fn storage_relation<RM>(&self, rel: Rel<M, RM>) -> Result<StorageRelation, DatabaseError>
    where
        RM: Model,
    {
        Ok(StorageRelation {
            kind: rel.kind,
            local_field: rel.local_field,
            remote_field: rel.remote_field,
            through: match rel.through {
                Some(through) => Some(StorageThroughRelation {
                    schema: self.database.collection(through.collection)?,
                    local_field: through.local_field,
                    remote_field: through.remote_field,
                }),
                None => None,
            },
        })
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

fn record_id(record: &crate::traits::storage::StorageRecord) -> Option<String> {
    match record.get(crate::FIELD_ID) {
        Some(StorageValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn record_string(record: &crate::traits::storage::StorageRecord, field: &str) -> Option<String> {
    match record.get(field) {
        Some(StorageValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}
