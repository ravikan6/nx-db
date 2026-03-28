use crate::Context;
use crate::errors::DatabaseError;
use crate::events::{Event, EventBus, NoopEventBus};
use crate::model::Model;
use crate::query::{Filter, QuerySpec};
use crate::registry::CollectionRegistry;
use crate::repository::{Repository, ScopedDatabase};
use crate::schema::CollectionSchema;
use crate::system_fields::is_system_field;
use crate::traits::storage::{StorageAdapter, StorageRecord, StorageValue};
use crate::utils::{Authorization, AuthorizationContext, Permission, PermissionEnum, Role};
use database_cache::{CacheBackend, CacheKey, CacheWrite};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthorizationScope {
    Collection,
    Document,
}

pub struct Database<A, R, E = NoopEventBus> {
    adapter: A,
    registry: R,
    events: E,
    cache: Option<Arc<dyn CacheBackend>>,
}

impl<A, R> Database<A, R, NoopEventBus> {
    pub fn builder() -> DatabaseBuilder<A, R, NoopEventBus> {
        DatabaseBuilder::default()
    }
}

pub struct DatabaseBuilder<A, R, E = NoopEventBus> {
    adapter: Option<A>,
    registry: Option<R>,
    events: E,
    cache: Option<Arc<dyn CacheBackend>>,
}

impl<A, R> Default for DatabaseBuilder<A, R, NoopEventBus> {
    fn default() -> Self {
        Self {
            adapter: None,
            registry: None,
            events: NoopEventBus,
            cache: None,
        }
    }
}

impl<A, R, E> DatabaseBuilder<A, R, E> {
    pub fn with_adapter(mut self, adapter: A) -> Self {
        self.adapter = Some(adapter);
        self
    }

    pub fn with_registry(mut self, registry: R) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_events<NE: EventBus>(self, events: NE) -> DatabaseBuilder<A, R, NE> {
        DatabaseBuilder {
            adapter: self.adapter,
            registry: self.registry,
            events,
            cache: self.cache,
        }
    }

    pub fn with_cache<CB: CacheBackend + 'static>(mut self, cache: CB) -> Self {
        self.cache = Some(Arc::new(cache));
        self
    }

    pub fn build(self) -> Result<Database<A, R, E>, DatabaseError> {
        Ok(Database {
            adapter: self
                .adapter
                .ok_or_else(|| DatabaseError::Other("adapter is required".into()))?,
            registry: self
                .registry
                .ok_or_else(|| DatabaseError::Other("registry is required".into()))?,
            events: self.events,
            cache: self.cache,
        })
    }
}

impl<A, R> Database<A, R, NoopEventBus> {
    pub fn new(adapter: A, registry: R) -> Self {
        Self {
            adapter,
            registry,
            events: NoopEventBus,
            cache: None,
        }
    }

    pub fn with_cache(mut self, cache: Arc<dyn CacheBackend>) -> Self {
        self.cache = Some(cache);
        self
    }
}

impl<A, R, E> Database<A, R, E> {
    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn registry(&self) -> &R {
        &self.registry
    }

    pub fn events(&self) -> &E {
        &self.events
    }

    pub fn cache(&self) -> Option<&dyn CacheBackend> {
        self.cache.as_deref()
    }

    pub fn scope(&self, context: Context) -> ScopedDatabase<'_, A, R, E> {
        ScopedDatabase::new(self, context)
    }

    pub fn repo<M>(&self) -> Repository<'_, A, R, E, M>
    where
        M: Model,
    {
        self.scope(Context::default()).repo::<M>()
    }

    pub fn get_repo<M>(&self, model: M) -> Repository<'_, A, R, E, M>
    where
        M: Model,
    {
        let _ = model;
        self.scope(Context::default()).repo::<M>()
    }
}

impl<A, R, E> Database<A, R, E>
where
    A: StorageAdapter,
    R: CollectionRegistry,
    E: EventBus,
{
    pub fn collection(&self, id: &str) -> Result<&'static CollectionSchema, DatabaseError> {
        self.registry
            .get(id)
            .ok_or_else(|| DatabaseError::NotFound(format!("collection '{id}' is not registered")))
    }

    pub fn validate_registry(&self) -> Result<(), DatabaseError> {
        self.registry.validate()
    }

    pub fn validate_storage_record(
        &self,
        collection: &'static CollectionSchema,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        self.validate_record_fields(collection, record)?;
        self.validate_required_attributes(collection, record)
    }

    pub fn validate_partial_record(
        &self,
        collection: &'static CollectionSchema,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        self.validate_record_fields(collection, record)
    }

    fn validate_record_fields(
        &self,
        collection: &'static CollectionSchema,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        for key in record.keys() {
            if key.starts_with('_') || is_system_field(key) {
                continue;
            }

            if collection.attribute(key).is_none() {
                return Err(DatabaseError::Other(format!(
                    "collection '{}': unknown attribute '{}'",
                    collection.id, key
                )));
            }
        }

        Ok(())
    }

    fn validate_required_attributes(
        &self,
        collection: &'static CollectionSchema,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        for attribute in collection.persisted_attributes() {
            if !attribute.required {
                continue;
            }

            match record.get(attribute.id) {
                Some(StorageValue::Null) | None => {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}': missing required attribute '{}'",
                        collection.id, attribute.id
                    )));
                }
                Some(_) => {}
            }
        }

        Ok(())
    }

    pub fn validate_query(
        &self,
        collection: &'static CollectionSchema,
        query: &QuerySpec,
    ) -> Result<(), DatabaseError> {
        for filter in query.filters() {
            self.validate_filter(collection, filter)?;
        }

        for sort in query.sorts() {
            if !is_system_field(sort.field) {
                let attribute = collection.attribute(sort.field).ok_or_else(|| {
                    DatabaseError::Other(format!(
                        "collection '{}': unknown sort field '{}'",
                        collection.id, sort.field
                    ))
                })?;
                if attribute.persistence != crate::AttributePersistence::Persisted {
                    return Err(DatabaseError::Other(format!(
                        "collection '{}': virtual field '{}' cannot be used in sorts",
                        collection.id, sort.field
                    )));
                }
            }
        }

        Ok(())
    }

    fn validate_filter(
        &self,
        collection: &'static CollectionSchema,
        filter: &Filter,
    ) -> Result<(), DatabaseError> {
        match filter {
            Filter::Field { field, .. } => {
                if !is_system_field(field) {
                    let attribute = collection.attribute(field).ok_or_else(|| {
                        DatabaseError::Validation(format!(
                            "collection '{}': unknown query field '{}'",
                            collection.id, field
                        ))
                    })?;
                    if attribute.persistence != crate::AttributePersistence::Persisted {
                        return Err(DatabaseError::Other(format!(
                            "collection '{}': virtual field '{}' cannot be used in filters",
                            collection.id, field
                        )));
                    }
                }
            }
            Filter::And(filters) | Filter::Or(filters) => {
                for f in filters {
                    self.validate_filter(collection, f)?;
                }
            }
            Filter::Not(filter) => {
                self.validate_filter(collection, filter)?;
            }
        }
        Ok(())
    }

    pub fn authorize_collection(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        action: PermissionEnum,
    ) -> Result<(), DatabaseError> {
        let authorization_context = self.authorization_context(context);
        let allowed_roles = self.collection_permission_roles(collection, action)?;
        Authorization::new(action, &authorization_context)
            .validate(&allowed_roles)
            .map_err(DatabaseError::from)
    }

    fn collection_permission_roles(
        &self,
        collection: &'static CollectionSchema,
        action: PermissionEnum,
    ) -> Result<Vec<Role>, DatabaseError> {
        self.permission_roles(collection.permissions.iter().copied(), action)
    }

    pub async fn ping(&self) -> Result<(), DatabaseError> {
        self.adapter.ping(&Context::default()).await
    }

    pub async fn create_collection(&self, id: &str) -> Result<(), DatabaseError> {
        let collection = self.collection(id)?;
        self.create_collection_in_context(&Context::default(), collection)
            .await
    }

    pub async fn create_collection_in_context(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
    ) -> Result<(), DatabaseError> {
        collection.validate()?;
        self.adapter.create_collection(context, collection).await?;
        self.events
            .dispatch(Event::collection_created(collection.id));
        Ok(())
    }

    pub async fn insert_model<M>(
        &self,
        context: &Context,
        input: M::Create,
    ) -> Result<M::Entity, DatabaseError>
    where
        M: Model,
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let collection = self.collection(M::schema().id)?;
        self.authorize_collection(context, collection, PermissionEnum::Create)?;

        let record = M::create_to_record(input, context)?;
        let encoded = self.prepare_record_for_storage::<M>(context, collection, record, false)?;

        let stored = self.adapter.insert(context, collection, encoded).await?;
        let entity = self
            .materialize_entity_fast::<M>(context, collection, stored.clone(), false)
            .await?;

        // Populate cache on insert
        if let Some(cache) = &self.cache {
            let id_str = M::id_to_string(M::entity_to_id(&entity));
            let cache_key = self.build_cache_key(collection.id, &id_str);
            self.write_cache::<M>(cache.as_ref(), &cache_key, &stored, &entity)
                .await?;
        }

        self.events.dispatch(Event::document_created(
            collection.id,
            M::id_to_string(M::entity_to_id(&entity)).to_string(),
        ));

        Ok(entity)
    }

    pub async fn insert_many_models<M>(
        &self,
        context: &Context,
        inputs: Vec<M::Create>,
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        M: Model,
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let collection = self.collection(M::schema().id)?;
        self.authorize_collection(context, collection, PermissionEnum::Create)?;

        let mut encoded_records = Vec::with_capacity(inputs.len());
        for input in inputs {
            let record = M::create_to_record(input, context)?;
            let encoded =
                self.prepare_record_for_storage::<M>(context, collection, record, false)?;
            encoded_records.push(encoded);
        }

        let stored_records = self
            .adapter
            .insert_many(context, collection, encoded_records)
            .await?;

        let mut entities = Vec::with_capacity(stored_records.len());
        for stored in stored_records {
            let entity = self
                .materialize_entity_fast::<M>(context, collection, stored.clone(), false)
                .await?;

            // Populate cache
            if let Some(cache) = &self.cache {
                let id_str = M::id_to_string(M::entity_to_id(&entity));
                let cache_key = self.build_cache_key(collection.id, &id_str);
                self.write_cache::<M>(cache.as_ref(), &cache_key, &stored, &entity)
                    .await?;
            }

            self.events.dispatch(Event::document_created(
                collection.id,
                M::id_to_string(M::entity_to_id(&entity)).to_string(),
            ));

            entities.push(entity);
        }

        Ok(entities)
    }

    pub async fn update_model<M>(
        &self,
        context: &Context,
        id: &M::Id,
        input: M::Update,
    ) -> Result<Option<M::Entity>, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        let authorization =
            self.authorization_scope(context, collection, PermissionEnum::Update)?;
        let id_str = M::id_to_string(id);

        let record = M::update_to_record(input, context)?;
        let encoded = self.prepare_record_for_storage::<M>(context, collection, record, true)?;

        if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Update)
        {
            let existing = self.adapter.get(context, collection, &id_str).await?;

            let Some(existing) = existing else {
                return Ok(None);
            };

            self.authorize_document(context, collection, PermissionEnum::Update, &existing)?;
        }

        let stored = self
            .adapter
            .update(context, collection, &id_str, encoded)
            .await?;

        let Some(stored) = stored else {
            return Ok(None);
        };

        let entity = self
            .materialize_entity_fast::<M>(context, collection, stored.clone(), false)
            .await?;

        // Invalidate cache on update, next get will repopulate
        if let Some(cache) = &self.cache {
            self.invalidate_cache(cache.as_ref(), collection.id, &id_str)
                .await?;
        }

        self.events.dispatch(Event::document_updated(
            collection.id,
            M::id_to_string(M::entity_to_id(&entity)),
        ));

        Ok(Some(entity))
    }

    pub async fn delete_model<M>(
        &self,
        context: &Context,
        id: &M::Id,
    ) -> Result<bool, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        let authorization =
            self.authorization_scope(context, collection, PermissionEnum::Delete)?;
        let id_str = M::id_to_string(id);

        if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Delete)
        {
            let existing = self.adapter.get(context, collection, &id_str).await?;

            let Some(existing) = existing else {
                return Ok(false);
            };

            self.authorize_document(context, collection, PermissionEnum::Delete, &existing)?;
        }

        let deleted = self.adapter.delete(context, collection, &id_str).await?;

        // Invalidate cache on delete
        if deleted {
            if let Some(cache) = &self.cache {
                self.invalidate_cache(cache.as_ref(), collection.id, &id_str)
                    .await?;
            }
            self.events
                .dispatch(Event::document_deleted(collection.id, id_str));
        }

        Ok(deleted)
    }

    pub async fn get_model<M>(
        &self,
        context: &Context,
        id: &M::Id,
    ) -> Result<Option<M::Entity>, DatabaseError>
    where
        M: Model,
        M::Entity: serde::Serialize + serde::de::DeserializeOwned,
    {
        let collection = self.collection(M::schema().id)?;
        let authorization = self.authorization_scope(context, collection, PermissionEnum::Read)?;
        let id_str = M::id_to_string(id);

        // Try cache first for get operations
        if let Some(cache) = &self.cache {
            let cache_key = self.build_cache_key(collection.id, &id_str);
            if let Some(cached) = self.read_cache(cache.as_ref(), &cache_key).await? {
                // Check document permissions on cached data
                if authorization == AuthorizationScope::Document {
                    if !self.is_cached_document_authorized(
                        context,
                        &cached,
                        PermissionEnum::Read,
                    )? {
                        let authorization_context = self.authorization_context(context);
                        let roles = self.permission_roles(
                            cached
                                .permissions
                                .iter()
                                .map(|permission| permission.as_str()),
                            PermissionEnum::Read,
                        )?;
                        Authorization::new(PermissionEnum::Read, &authorization_context)
                            .validate(&roles)
                            .map_err(DatabaseError::from)?;
                    }
                }
                // Decode cached entity
                let entity = self.decode_cached_entity::<M>(&cached, context)?;
                return Ok(Some(entity));
            }
        }

        // Cache miss - go to adapter
        let record = self.adapter.get(context, collection, &id_str).await?;

        let Some(record) = record else {
            return Ok(None);
        };

        if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Read)
        {
            self.authorize_read_record(context, collection, authorization, &record)?;
        }

        let entity = self
            .materialize_entity_fast::<M>(context, collection, record.clone(), false)
            .await?;

        // Populate cache after successful read
        if let Some(cache) = &self.cache {
            let cache_key = self.build_cache_key(collection.id, &id_str);
            self.write_cache::<M>(cache.as_ref(), &cache_key, &record, &entity)
                .await?;
        }

        Ok(Some(entity))
    }

    pub async fn find_models<M>(
        &self,
        context: &Context,
        query: &QuerySpec,
    ) -> Result<Vec<M::Entity>, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        self.validate_query(collection, query)?;
        let authorization = self.authorization_scope(context, collection, PermissionEnum::Read)?;
        let records = self.adapter.find(context, collection, query).await?;
        let filtered = if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Read)
        {
            self.filter_authorized_records(context, collection, authorization, records)?
        } else {
            records
        };

        let mut entities = Vec::with_capacity(filtered.len());
        for record in filtered {
            entities.push(
                self.materialize_entity_fast::<M>(context, collection, record, false)
                    .await?,
            );
        }

        Ok(entities)
    }

    pub async fn count_models<M>(
        &self,
        context: &Context,
        query: &QuerySpec,
    ) -> Result<u64, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        self.validate_query(collection, query)?;

        match self.authorization_scope(context, collection, PermissionEnum::Read)? {
            AuthorizationScope::Collection => self.adapter.count(context, collection, query).await,
            AuthorizationScope::Document => {
                if self
                    .adapter
                    .enforces_document_filtering(PermissionEnum::Read)
                {
                    self.adapter.count(context, collection, query).await
                } else {
                    let records = self.adapter.find(context, collection, query).await?;
                    let filtered = self.filter_authorized_records(
                        context,
                        collection,
                        AuthorizationScope::Document,
                        records,
                    )?;
                    Ok(filtered.len() as u64)
                }
            }
        }
    }

    pub async fn update_many_models<M>(
        &self,
        context: &Context,
        query: &QuerySpec,
        input: M::Update,
    ) -> Result<u64, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        self.validate_query(collection, query)?;
        let authorization =
            self.authorization_scope(context, collection, PermissionEnum::Update)?;

        if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Update)
        {
            return Err(DatabaseError::Other(
                "update_many is not supported without adapter-level document filtering".to_string(),
            ));
        }

        let record = M::update_to_record(input, context)?;
        let encoded = self.prepare_record_for_storage::<M>(context, collection, record, true)?;

        let updated = self
            .adapter
            .update_many(context, collection, query, encoded)
            .await?;

        if updated > 0 {
            if let Some(cache) = &self.cache {
                let namespace =
                    database_cache::Namespace::new(collection.id).expect("valid namespace");
                let _ = cache.clear_namespace(&namespace).await;
            }
        }

        Ok(updated)
    }

    pub async fn delete_many_models<M>(
        &self,
        context: &Context,
        query: &QuerySpec,
    ) -> Result<u64, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        self.validate_query(collection, query)?;
        let authorization =
            self.authorization_scope(context, collection, PermissionEnum::Delete)?;

        if authorization == AuthorizationScope::Document
            && !self
                .adapter
                .enforces_document_filtering(PermissionEnum::Delete)
        {
            return Err(DatabaseError::Other(
                "delete_many is not supported without adapter-level document filtering".to_string(),
            ));
        }

        let deleted = self.adapter.delete_many(context, collection, query).await?;

        if deleted > 0 {
            if let Some(cache) = &self.cache {
                let namespace =
                    database_cache::Namespace::new(collection.id).expect("valid namespace");
                let _ = cache.clear_namespace(&namespace).await;
            }
        }

        Ok(deleted)
    }

    pub async fn materialize_entity<M>(
        &self,
        context: &Context,
        record: StorageRecord,
    ) -> Result<M::Entity, DatabaseError>
    where
        M: Model,
    {
        let collection = self.collection(M::schema().id)?;
        self.materialize_entity_fast::<M>(context, collection, record, true)
            .await
    }

    pub(crate) async fn materialize_entity_fast<M>(
        &self,
        context: &Context,
        _collection: &'static CollectionSchema,
        record: StorageRecord,
        validate: bool,
    ) -> Result<M::Entity, DatabaseError>
    where
        M: Model,
    {
        let decoded = M::decode_record(record, context)?;
        if validate {
            self.validate_storage_record(_collection, &decoded)?;
        }
        let entity = M::entity_from_record(decoded, context)?;
        M::resolve_entity(entity, context).await
    }

    fn prepare_record_for_storage<M>(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        mut record: StorageRecord,
        partial: bool,
    ) -> Result<StorageRecord, DatabaseError>
    where
        M: Model,
    {
        let now = time::OffsetDateTime::now_utc();

        if !partial {
            record
                .entry(crate::system_fields::FIELD_CREATED_AT.to_string())
                .or_insert(StorageValue::Timestamp(now));
            record
                .entry(crate::system_fields::FIELD_UPDATED_AT.to_string())
                .or_insert(StorageValue::Timestamp(now));

            // Apply attribute defaults for missing fields on insert.
            for attribute in collection.persisted_attributes() {
                if let Some(default) = attribute.default {
                    record
                        .entry(attribute.id.to_string())
                        .or_insert_with(|| default.into_storage());
                }
            }
        } else {
            record
                .entry(crate::system_fields::FIELD_UPDATED_AT.to_string())
                .or_insert(StorageValue::Timestamp(now));
        }

        if partial {
            self.validate_partial_record(collection, &record)?;
        } else {
            self.validate_storage_record(collection, &record)?;
        }

        let encoded = M::encode_record(record, context)?;

        if partial {
            self.validate_partial_record(collection, &encoded)?;
        } else {
            self.validate_storage_record(collection, &encoded)?;
        }

        Ok(encoded)
    }

    fn authorization_context(&self, context: &Context) -> AuthorizationContext {
        let roles = context.roles().cloned().collect::<Vec<Role>>();
        if context.authorization_enabled() {
            AuthorizationContext::enabled(roles)
        } else {
            AuthorizationContext::disabled(roles)
        }
    }

    fn permission_roles<'a, I>(
        &self,
        permissions: I,
        action: PermissionEnum,
    ) -> Result<Vec<Role>, DatabaseError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut roles = Vec::new();

        for permission in permissions {
            let permission = Permission::parse(permission)
                .map_err(|error| DatabaseError::Other(format!("invalid permission: {error}")))?;

            let matches = match (permission.permission(), action) {
                (PermissionEnum::Write, PermissionEnum::Create)
                | (PermissionEnum::Write, PermissionEnum::Update)
                | (PermissionEnum::Write, PermissionEnum::Delete) => true,
                (current, target) => current == target,
            };

            if matches {
                roles.push(permission.role_instance().clone());
            }
        }

        Ok(roles)
    }

    fn authorization_scope(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        action: PermissionEnum,
    ) -> Result<AuthorizationScope, DatabaseError> {
        match self.authorize_collection(context, collection, action) {
            Ok(()) => Ok(AuthorizationScope::Collection),
            Err(DatabaseError::Authorization(_)) if collection.document_security => {
                Ok(AuthorizationScope::Document)
            }
            Err(error) => Err(error),
        }
    }

    fn authorize_read_record(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        authorization: AuthorizationScope,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        match authorization {
            AuthorizationScope::Collection => Ok(()),
            AuthorizationScope::Document => {
                self.authorize_document(context, collection, PermissionEnum::Read, record)
            }
        }
    }

    fn filter_authorized_records(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        authorization: AuthorizationScope,
        records: Vec<StorageRecord>,
    ) -> Result<Vec<StorageRecord>, DatabaseError> {
        if authorization == AuthorizationScope::Collection {
            return Ok(records);
        }

        let mut filtered = Vec::with_capacity(records.len());
        for record in records {
            if self.is_document_authorized(context, collection, PermissionEnum::Read, &record)? {
                filtered.push(record);
            }
        }

        Ok(filtered)
    }

    fn authorize_document(
        &self,
        context: &Context,
        _collection: &'static CollectionSchema,
        action: PermissionEnum,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        let authorization_context = self.authorization_context(context);
        let permissions = self.document_permission_roles(record, action)?;
        Authorization::new(action, &authorization_context)
            .validate(&permissions)
            .map_err(DatabaseError::from)
    }

    fn is_document_authorized(
        &self,
        context: &Context,
        collection: &'static CollectionSchema,
        action: PermissionEnum,
        record: &StorageRecord,
    ) -> Result<bool, DatabaseError> {
        match self.authorize_document(context, collection, action, record) {
            Ok(()) => Ok(true),
            Err(DatabaseError::Authorization(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn document_permission_roles(
        &self,
        record: &StorageRecord,
        action: PermissionEnum,
    ) -> Result<Vec<Role>, DatabaseError> {
        let permissions = match record.get(crate::FIELD_PERMISSIONS) {
            Some(StorageValue::StringArray(values)) => values,
            Some(StorageValue::Null) | None => return Ok(Vec::new()),
            Some(_) => {
                return Err(DatabaseError::Other(
                    "document permissions field must be a string array".into(),
                ));
            }
        };

        self.permission_roles(permissions.iter().map(String::as_str), action)
    }

    fn build_cache_key(&self, collection: &str, id: &str) -> CacheKey {
        CacheKey::new(format!(
            "{}__{}",
            Self::cache_key_component(collection),
            Self::cache_key_component(id)
        ))
        .expect("cache key components should always encode to a valid cache key")
    }

    fn cache_key_component(value: &str) -> String {
        // Hex-encode the raw bytes with a leading 'h' sentinel so the resulting
        // string is safe for any cache key character set.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let bytes = value.as_bytes();
        let mut encoded = Vec::with_capacity(1 + bytes.len() * 2);
        encoded.push(b'h');
        for &byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize]);
            encoded.push(HEX[(byte & 0xf) as usize]);
        }
        // SAFETY: only ASCII bytes were pushed.
        unsafe { String::from_utf8_unchecked(encoded) }
    }

    async fn read_cache(
        &self,
        cache: &dyn CacheBackend,
        key: &CacheKey,
    ) -> Result<Option<CachedDocument>, DatabaseError> {
        use database_cache::Namespace;
        let namespace = Namespace::new("docs").expect("valid namespace");

        match cache.get(&namespace, key).await {
            Ok(Some(bytes)) => {
                let cached: CachedDocument =
                    bincode::serde::decode_from_slice(bytes.as_ref(), bincode::config::standard())
                        .map_err(|e| DatabaseError::Other(format!("cache decode error: {}", e)))?
                        .0;
                Ok(Some(cached))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(DatabaseError::Other(format!("cache read error: {}", e))),
        }
    }

    async fn write_cache<M>(
        &self,
        cache: &dyn CacheBackend,
        key: &CacheKey,
        record: &StorageRecord,
        entity: &M::Entity,
    ) -> Result<(), DatabaseError>
    where
        M: Model,
        M::Entity: serde::Serialize,
    {
        use database_cache::Namespace;
        let namespace = Namespace::new("docs").expect("valid namespace");

        let permissions = record
            .get(crate::FIELD_PERMISSIONS)
            .and_then(|v| match v {
                StorageValue::StringArray(arr) => Some(arr.clone()),
                _ => None,
            })
            .unwrap_or_default();

        let cached = CachedDocument {
            entity_bytes: bincode::serde::encode_to_vec(entity, bincode::config::standard())
                .map_err(|e| DatabaseError::Other(format!("cache encode error: {}", e)))?,
            permissions,
        };

        let bytes = bincode::serde::encode_to_vec(&cached, bincode::config::standard())
            .map_err(|e| DatabaseError::Other(format!("cache encode error: {}", e)))?;

        let write = CacheWrite {
            key: key.clone(),
            value: bytes.into(),
            ttl: Some(Duration::from_secs(3600)),
        };

        cache
            .set(&namespace, write)
            .await
            .map_err(|e| DatabaseError::Other(format!("cache write error: {}", e)))?;

        Ok(())
    }

    async fn invalidate_cache(
        &self,
        cache: &dyn CacheBackend,
        collection: &str,
        id: &str,
    ) -> Result<(), DatabaseError> {
        use database_cache::Namespace;
        let namespace = Namespace::new("docs").expect("valid namespace");
        let key = self.build_cache_key(collection, id);

        cache
            .delete(&namespace, &key)
            .await
            .map_err(|e| DatabaseError::Other(format!("cache delete error: {}", e)))?;

        Ok(())
    }

    fn decode_cached_entity<M>(
        &self,
        cached: &CachedDocument,
        _context: &Context,
    ) -> Result<M::Entity, DatabaseError>
    where
        M: Model,
        M::Entity: serde::de::DeserializeOwned,
    {
        let entity: M::Entity =
            bincode::serde::decode_from_slice(&cached.entity_bytes, bincode::config::standard())
                .map_err(|e| DatabaseError::Other(format!("cached entity decode error: {}", e)))?
                .0;
        Ok(entity)
    }

    fn is_cached_document_authorized(
        &self,
        context: &Context,
        cached: &CachedDocument,
        action: PermissionEnum,
    ) -> Result<bool, DatabaseError> {
        let authorization_context = self.authorization_context(context);
        let roles = self.permission_roles(cached.permissions.iter().map(|s| s.as_str()), action)?;

        match Authorization::new(action, &authorization_context).validate(&roles) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct CachedDocument {
    entity_bytes: Vec<u8>,
    permissions: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::enums::AttributeKind;
    use crate::errors::DatabaseError;
    use crate::events::{Event, EventBus};
    use crate::key::Key;
    use crate::model::Model;
    use crate::query::{Field, FilterOp, QuerySpec, SortDirection};
    use crate::registry::StaticRegistry;
    use crate::schema::{AttributePersistence, AttributeSchema, CollectionSchema};
    use crate::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
    use crate::utils::Role;
    use database_cache::{CacheBackend, CacheKey};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker};

    const ATTRIBUTES: &[AttributeSchema] = &[AttributeSchema {
        id: "name",
        column: "name",
        kind: AttributeKind::String,
        required: true,
        array: false,
        length: None,
        default: None,
        elements: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    }];

    static USERS: CollectionSchema = CollectionSchema {
        id: "users",
        name: "Users",
        document_security: true,
        enabled: true,
        permissions: &[
            "read(\"any\")",
            "create(\"any\")",
            "update(\"any\")",
            "delete(\"any\")",
        ],
        attributes: ATTRIBUTES,
        indexes: &[],
    };

    static RESTRICTED_USERS: CollectionSchema = CollectionSchema {
        id: "restricted_users",
        name: "RestrictedUsers",
        document_security: true,
        enabled: true,
        permissions: &[
            "read(\"user:admin\")",
            "create(\"user:admin\")",
            "update(\"user:admin\")",
            "delete(\"user:admin\")",
        ],
        attributes: ATTRIBUTES,
        indexes: &[],
    };

    static INVALID_COLLECTION_PERMISSIONS: CollectionSchema = CollectionSchema {
        id: "invalid_collection_permissions",
        name: "InvalidCollectionPermissions",
        document_security: true,
        enabled: true,
        permissions: &["read"],
        attributes: ATTRIBUTES,
        indexes: &[],
    };

    #[derive(Default)]
    struct FakeAdapter {
        rows: Arc<Mutex<BTreeMap<(String, String, String), StorageRecord>>>,
    }

    impl StorageAdapter for FakeAdapter {
        fn ping(&self, _context: &crate::Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
            Box::pin(async { Ok(()) })
        }

        fn create_collection(
            &self,
            _context: &crate::Context,
            _schema: &'static CollectionSchema,
        ) -> AdapterFuture<'_, Result<(), DatabaseError>> {
            Box::pin(async { Ok(()) })
        }

        fn insert(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            values: StorageRecord,
        ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();

            Box::pin(async move {
                let id = match values.get(crate::system_fields::FIELD_ID) {
                    Some(StorageValue::String(value)) => value.clone(),
                    _ => {
                        return Err(DatabaseError::Other(
                            "record is missing string id field".into(),
                        ));
                    }
                };

                rows.lock()
                    .expect("rows lock")
                    .insert((schema_name, collection, id), values.clone());

                Ok(values)
            })
        }

        fn insert_many(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            values: Vec<StorageRecord>,
        ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();

            Box::pin(async move {
                let mut locked = rows.lock().expect("rows lock");
                for record in &values {
                    let id = match record.get(crate::system_fields::FIELD_ID) {
                        Some(StorageValue::String(value)) => value.clone(),
                        _ => {
                            return Err(DatabaseError::Other(
                                "record is missing string id field".into(),
                            ));
                        }
                    };
                    locked.insert(
                        (schema_name.clone(), collection.clone(), id),
                        record.clone(),
                    );
                }
                Ok(values)
            })
        }

        fn get(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            id: &str,
        ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
            let rows = self.rows.clone();
            let key = (
                context.schema().to_string(),
                schema.id.to_string(),
                id.to_string(),
            );

            Box::pin(async move { Ok(rows.lock().expect("rows lock").get(&key).cloned()) })
        }

        fn update(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            id: &str,
            values: StorageRecord,
        ) -> AdapterFuture<'_, Result<Option<StorageRecord>, DatabaseError>> {
            let rows = self.rows.clone();
            let key = (
                context.schema().to_string(),
                schema.id.to_string(),
                id.to_string(),
            );

            Box::pin(async move {
                let mut guard = rows.lock().expect("rows lock");
                let Some(existing) = guard.get_mut(&key) else {
                    return Ok(None);
                };

                for (field, value) in values {
                    existing.insert(field, value);
                }

                Ok(Some(existing.clone()))
            })
        }

        fn delete(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            id: &str,
        ) -> AdapterFuture<'_, Result<bool, DatabaseError>> {
            let rows = self.rows.clone();
            let key = (
                context.schema().to_string(),
                schema.id.to_string(),
                id.to_string(),
            );

            Box::pin(async move { Ok(rows.lock().expect("rows lock").remove(&key).is_some()) })
        }

        fn update_many(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            query: &QuerySpec,
            values: StorageRecord,
        ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();
            let query = query.clone();

            Box::pin(async move {
                let mut locked = rows.lock().expect("rows lock");
                let mut count = 0;
                let mut to_update = Vec::new();

                for (key, record) in locked.iter() {
                    if key.0 == schema_name && key.1 == collection {
                        if query
                            .filters()
                            .iter()
                            .all(|filter| matches_filter(record, filter))
                        {
                            to_update.push(key.clone());
                        }
                    }
                }

                for key in to_update {
                    if let Some(record) = locked.get_mut(&key) {
                        for (field, value) in &values {
                            record.insert(field.clone(), value.clone());
                        }
                        count += 1;
                    }
                }

                Ok(count)
            })
        }

        fn delete_many(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            query: &QuerySpec,
        ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();
            let query = query.clone();

            Box::pin(async move {
                let mut locked = rows.lock().expect("rows lock");
                let mut count = 0;
                let mut to_delete = Vec::new();

                for (key, record) in locked.iter() {
                    if key.0 == schema_name && key.1 == collection {
                        if query
                            .filters()
                            .iter()
                            .all(|filter| matches_filter(record, filter))
                        {
                            to_delete.push(key.clone());
                        }
                    }
                }

                for key in to_delete {
                    locked.remove(&key);
                    count += 1;
                }

                Ok(count)
            })
        }

        fn find(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            query: &QuerySpec,
        ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();
            let query = query.clone();

            Box::pin(async move {
                let mut records: Vec<_> = rows
                    .lock()
                    .expect("rows lock")
                    .iter()
                    .filter(|((row_schema, row_collection, _), _)| {
                        row_schema == &schema_name && row_collection == &collection
                    })
                    .map(|(_, record)| record.clone())
                    .collect();

                records.retain(|record| {
                    query
                        .filters()
                        .iter()
                        .all(|filter| matches_filter(record, filter))
                });

                for sort in query.sorts().iter().rev() {
                    records.sort_by(|left, right| {
                        compare_records(left, right, &sort.field, sort.direction)
                    });
                }

                let offset = query.offset_value().unwrap_or(0);
                let limited = records.into_iter().skip(offset);
                let result = if let Some(limit) = query.limit_value() {
                    limited.take(limit).collect()
                } else {
                    limited.collect()
                };

                Ok(result)
            })
        }

        fn count(
            &self,
            context: &crate::Context,
            schema: &'static CollectionSchema,
            query: &QuerySpec,
        ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
            let rows = self.rows.clone();
            let schema_name = context.schema().to_string();
            let collection = schema.id.to_string();
            let query = query.clone();

            Box::pin(async move {
                let count = rows
                    .lock()
                    .expect("rows lock")
                    .iter()
                    .filter(|((row_schema, row_collection, _), record)| {
                        row_schema == &schema_name
                            && row_collection == &collection
                            && query
                                .filters()
                                .iter()
                                .all(|filter| matches_filter(record, filter))
                    })
                    .count();

                Ok(count as u64)
            })
        }
    }

    #[derive(Clone, Default)]
    struct RecordingEvents {
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl EventBus for RecordingEvents {
        fn dispatch(&self, event: Event) {
            self.events.lock().expect("event lock").push(event);
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                raw_waker()
            }

            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}

            RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
            )
        }

        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut future = Box::pin(future);
        let mut context = TaskContext::from_waker(&waker);

        loop {
            match Future::poll(Pin::as_mut(&mut future), &mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct UserEntity {
        id: Key<32>,
        name: String,
        metadata: crate::model::Metadata,
    }

    fn extract_metadata(record: &mut StorageRecord) -> crate::model::Metadata {
        use crate::system_fields::*;
        crate::model::Metadata {
            sequence: match record.remove(FIELD_SEQUENCE) {
                Some(StorageValue::Int(v)) => v,
                _ => 0,
            },
            created_at: match record.remove(FIELD_CREATED_AT) {
                Some(StorageValue::Timestamp(v)) => v,
                _ => time::OffsetDateTime::now_utc(),
            },
            updated_at: match record.remove(FIELD_UPDATED_AT) {
                Some(StorageValue::Timestamp(v)) => v,
                _ => time::OffsetDateTime::now_utc(),
            },
            permissions: match record.get(FIELD_PERMISSIONS) {
                Some(StorageValue::StringArray(v)) => v.clone(),
                _ => vec![],
            },
        }
    }

    #[derive(Debug, Clone)]
    struct CreateUser {
        id: Key<32>,
        name: String,
    }

    #[derive(Debug, Clone)]
    struct UpdateUser {
        name: String,
    }

    #[derive(Debug, Clone, Copy)]
    struct User;

    const USER_ID_FIELD: Field<User, Key<32>> = Field::new(crate::FIELD_ID);
    const USER_NAME_FIELD: Field<User, String> = Field::new("name");

    impl Model for User {
        type Id = Key<32>;
        type Entity = UserEntity;
        type Create = CreateUser;
        type Update = UpdateUser;

        fn schema() -> &'static CollectionSchema {
            &USERS
        }

        fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
            &entity.id
        }

        fn entity_metadata(entity: &Self::Entity) -> &crate::model::Metadata {
            &entity.metadata
        }

        fn create_to_record(
            input: Self::Create,
            _context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            Ok(BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String(input.id.to_string()),
                ),
                ("name".into(), StorageValue::String(input.name)),
            ]))
        }

        fn update_to_record(
            input: Self::Update,
            _context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            Ok(BTreeMap::from([(
                "name".into(),
                StorageValue::String(input.name),
            )]))
        }

        fn entity_from_record(
            mut record: StorageRecord,
            _context: &crate::Context,
        ) -> Result<Self::Entity, DatabaseError> {
            let metadata = extract_metadata(&mut record);
            let id = match record.remove(crate::system_fields::FIELD_ID) {
                Some(StorageValue::String(value)) => Key::<32>::new(value)?,
                None => match record.remove(crate::FIELD_ID) {
                    Some(StorageValue::String(value)) => Key::<32>::new(value)?,
                    _ => return Err(DatabaseError::Other("missing id".into())),
                },
                _ => return Err(DatabaseError::Other("missing id".into())),
            };

            let name = match record.remove("name") {
                Some(StorageValue::String(value)) => value,
                _ => return Err(DatabaseError::Other("missing name".into())),
            };

            Ok(UserEntity { id, name, metadata })
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct RestrictedUser;

    impl Model for RestrictedUser {
        type Id = Key<32>;
        type Entity = UserEntity;
        type Create = CreateUser;
        type Update = UpdateUser;

        fn schema() -> &'static CollectionSchema {
            &RESTRICTED_USERS
        }

        fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
            &entity.id
        }

        fn entity_metadata(entity: &Self::Entity) -> &crate::model::Metadata {
            &entity.metadata
        }

        fn create_to_record(
            input: Self::Create,
            context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            User::create_to_record(input, context)
        }

        fn update_to_record(
            input: Self::Update,
            context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            User::update_to_record(input, context)
        }

        fn entity_from_record(
            record: StorageRecord,
            context: &crate::Context,
        ) -> Result<Self::Entity, DatabaseError> {
            User::entity_from_record(record, context)
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct InvalidCollectionPermissionsUser;

    impl Model for InvalidCollectionPermissionsUser {
        type Id = Key<32>;
        type Entity = UserEntity;
        type Create = CreateUser;
        type Update = UpdateUser;

        fn schema() -> &'static CollectionSchema {
            &INVALID_COLLECTION_PERMISSIONS
        }

        fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
            &entity.id
        }

        fn entity_metadata(entity: &Self::Entity) -> &crate::model::Metadata {
            &entity.metadata
        }

        fn create_to_record(
            input: Self::Create,
            context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            User::create_to_record(input, context)
        }

        fn update_to_record(
            input: Self::Update,
            context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            User::update_to_record(input, context)
        }

        fn entity_from_record(
            record: StorageRecord,
            context: &crate::Context,
        ) -> Result<Self::Entity, DatabaseError> {
            User::entity_from_record(record, context)
        }
    }

    #[test]
    fn validates_storage_records_with_system_fields() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let database = Database::new(FakeAdapter::default(), registry);
        let collection = database.collection("users").expect("registered collection");
        let record = BTreeMap::from([
            (crate::FIELD_ID.into(), StorageValue::String("usr_1".into())),
            (
                crate::FIELD_PERMISSIONS.into(),
                StorageValue::StringArray(vec![]),
            ),
            ("name".into(), StorageValue::String("Ravi".into())),
        ]);

        assert!(
            database
                .validate_storage_record(collection, &record)
                .is_ok()
        );
    }

    #[test]
    fn creates_registered_collection() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let events = RecordingEvents::default();
        let database = Database::builder()
            .with_adapter(FakeAdapter::default())
            .with_registry(registry)
            .with_events(events.clone())
            .build()
            .expect("database should build");

        block_on(database.create_collection("users")).expect("collection should be created");

        let recorded = events.events.lock().expect("event lock");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], Event::collection_created("users"));
    }

    #[test]
    fn uses_repository_api_with_scoped_context() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let database = Database::new(FakeAdapter::default(), registry);
        let context = crate::Context::default().with_schema("tenant_alpha");
        let repo = database.scope(context.clone()).repo::<User>();

        let created = block_on(repo.insert(CreateUser {
            id: Key::<32>::new("usr_1").expect("valid id"),
            name: "Ravi".into(),
        }))
        .expect("create should succeed");

        assert_eq!(User::entity_to_id(&created).as_str(), "usr_1");

        let fetched = block_on(repo.get(&created.id))
            .expect("get should succeed")
            .expect("entity should exist");
        assert_eq!(fetched.name, "Ravi");

        let default_repo = database.repo::<User>();
        let missing = block_on(default_repo.get(&created.id)).expect("get should succeed");
        assert!(
            missing.is_none(),
            "default context should not see tenant_alpha data"
        );
        assert_eq!(default_repo.context().schema(), "public");
        assert_eq!(repo.context().schema(), context.schema());
    }

    #[test]
    fn insert_pipeline_applies_encode_decode_and_events() {
        #[derive(Debug, Clone, Copy)]
        struct EncodedUser;

        impl Model for EncodedUser {
            type Id = Key<32>;
            type Entity = UserEntity;
            type Create = CreateUser;
            type Update = UpdateUser;

            fn schema() -> &'static CollectionSchema {
                &USERS
            }

            fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
                &entity.id
            }

            fn entity_metadata(entity: &Self::Entity) -> &crate::model::Metadata {
                &entity.metadata
            }

            fn create_to_record(
                input: Self::Create,
                context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                User::create_to_record(input, context)
            }

            fn encode_record(
                mut record: StorageRecord,
                _context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                match record.remove("name") {
                    Some(StorageValue::String(value)) => {
                        record.insert("name".into(), StorageValue::String(format!("enc:{value}")));
                        Ok(record)
                    }
                    _ => Err(DatabaseError::Other("missing name".into())),
                }
            }

            fn decode_record(
                mut record: StorageRecord,
                _context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                match record.remove("name") {
                    Some(StorageValue::String(value)) => {
                        let decoded = value.strip_prefix("enc:").unwrap_or(&value).to_string();
                        record.insert("name".into(), StorageValue::String(decoded));
                        Ok(record)
                    }
                    _ => Err(DatabaseError::Other("missing name".into())),
                }
            }

            fn resolve_entity<'a>(
                mut entity: Self::Entity,
                _context: &'a crate::Context,
            ) -> crate::model::ModelFuture<'a, Result<Self::Entity, DatabaseError>> {
                Box::pin(async move {
                    entity.name.push_str(" (resolved)");
                    Ok(entity)
                })
            }

            fn update_to_record(
                input: Self::Update,
                context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                User::update_to_record(input, context)
            }

            fn entity_from_record(
                record: StorageRecord,
                context: &crate::Context,
            ) -> Result<Self::Entity, DatabaseError> {
                User::entity_from_record(record, context)
            }
        }

        let adapter = FakeAdapter::default();
        let rows = adapter.rows.clone();
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let events = RecordingEvents::default();
        let database = Database::builder()
            .with_adapter(adapter)
            .with_registry(registry)
            .with_events(events.clone())
            .build()
            .expect("database should build");
        let repo = database.repo::<EncodedUser>();

        let created = block_on(repo.insert(CreateUser {
            id: Key::<32>::new("usr_encoded").expect("valid id"),
            name: "Ravi".into(),
        }))
        .expect("insert should succeed");

        assert_eq!(created.id.as_str(), "usr_encoded");
        assert_eq!(created.name, "Ravi (resolved)");

        let stored = rows
            .lock()
            .expect("rows lock")
            .get(&(
                "public".to_string(),
                "users".to_string(),
                "usr_encoded".to_string(),
            ))
            .cloned()
            .expect("stored row should exist");
        assert_eq!(
            stored.get("name"),
            Some(&StorageValue::String("enc:Ravi".into()))
        );

        let fetched = block_on(repo.get(&created.id))
            .expect("get should succeed")
            .expect("entity should exist");
        assert_eq!(fetched.name, "Ravi (resolved)");

        let recorded = events.events.lock().expect("event lock");
        assert!(recorded.contains(&Event::document_created("users", "usr_encoded")));
    }

    #[test]
    fn update_pipeline_applies_encode_decode_and_events() {
        #[derive(Debug, Clone, Copy)]
        struct EncodedUser;

        impl Model for EncodedUser {
            type Id = Key<32>;
            type Entity = UserEntity;
            type Create = CreateUser;
            type Update = UpdateUser;

            fn schema() -> &'static CollectionSchema {
                &USERS
            }

            fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
                &entity.id
            }

            fn entity_metadata(entity: &Self::Entity) -> &crate::model::Metadata {
                &entity.metadata
            }

            fn create_to_record(
                input: Self::Create,
                context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                User::create_to_record(input, context)
            }

            fn encode_record(
                mut record: StorageRecord,
                _context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                match record.remove("name") {
                    Some(StorageValue::String(value)) => {
                        record.insert("name".into(), StorageValue::String(format!("enc:{value}")));
                        Ok(record)
                    }
                    _ => Err(DatabaseError::Other("missing name".into())),
                }
            }

            fn decode_record(
                mut record: StorageRecord,
                _context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                match record.remove("name") {
                    Some(StorageValue::String(value)) => {
                        let decoded = value.strip_prefix("enc:").unwrap_or(&value).to_string();
                        record.insert("name".into(), StorageValue::String(decoded));
                        Ok(record)
                    }
                    _ => Err(DatabaseError::Other("missing name".into())),
                }
            }

            fn resolve_entity<'a>(
                mut entity: Self::Entity,
                _context: &'a crate::Context,
            ) -> crate::model::ModelFuture<'a, Result<Self::Entity, DatabaseError>> {
                Box::pin(async move {
                    entity.name.push_str(" (resolved)");
                    Ok(entity)
                })
            }

            fn update_to_record(
                input: Self::Update,
                context: &crate::Context,
            ) -> Result<StorageRecord, DatabaseError> {
                User::update_to_record(input, context)
            }

            fn entity_from_record(
                record: StorageRecord,
                context: &crate::Context,
            ) -> Result<Self::Entity, DatabaseError> {
                User::entity_from_record(record, context)
            }
        }

        let adapter = FakeAdapter::default();
        let rows = adapter.rows.clone();
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let events = RecordingEvents::default();
        let database = Database::builder()
            .with_adapter(adapter)
            .with_registry(registry)
            .with_events(events.clone())
            .build()
            .expect("database should build");
        let repo = database.repo::<EncodedUser>();

        let created = block_on(repo.insert(CreateUser {
            id: Key::<32>::new("usr_updated").expect("valid id"),
            name: "Ravi".into(),
        }))
        .expect("insert should succeed");

        let updated = block_on(repo.update(
            &created.id,
            UpdateUser {
                name: "Aman".into(),
            },
        ))
        .expect("update should succeed")
        .expect("entity should exist");

        assert_eq!(updated.id.as_str(), "usr_updated");
        assert_eq!(updated.name, "Aman (resolved)");

        let stored = rows
            .lock()
            .expect("rows lock")
            .get(&(
                "public".to_string(),
                "users".to_string(),
                "usr_updated".to_string(),
            ))
            .cloned()
            .expect("stored row should exist");
        assert_eq!(
            stored.get("name"),
            Some(&StorageValue::String("enc:Aman".into()))
        );

        let fetched = block_on(repo.get(&updated.id))
            .expect("get should succeed")
            .expect("entity should exist");
        assert_eq!(fetched.name, "Aman (resolved)");

        let recorded = events.events.lock().expect("event lock");
        assert!(recorded.contains(&Event::document_updated("users", "usr_updated")));
    }

    #[test]
    fn delete_pipeline_dispatches_event_only_when_row_is_deleted() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let events = RecordingEvents::default();
        let database = Database::builder()
            .with_adapter(FakeAdapter::default())
            .with_registry(registry)
            .with_events(events.clone())
            .build()
            .expect("database should build");
        let repo = database.repo::<User>();

        let created = block_on(repo.insert(CreateUser {
            id: Key::<32>::new("usr_deleted").expect("valid id"),
            name: "Ravi".into(),
        }))
        .expect("insert should succeed");

        let deleted = block_on(repo.delete(&created.id)).expect("delete should succeed");
        assert!(deleted);

        let missing = block_on(repo.get(&created.id)).expect("get should succeed");
        assert!(missing.is_none());

        let second_delete = block_on(repo.delete(&created.id)).expect("delete should succeed");
        assert!(!second_delete);

        let recorded = events.events.lock().expect("event lock");
        let deleted_events = recorded
            .iter()
            .filter(|event| **event == Event::document_deleted("users", "usr_deleted"))
            .count();
        assert_eq!(deleted_events, 1);
    }

    #[test]
    fn get_uses_document_permissions_when_collection_read_is_denied() {
        let adapter = FakeAdapter::default();
        adapter.rows.lock().expect("rows lock").insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_reader".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_reader".into()),
                ),
                ("name".into(), StorageValue::String("Reader".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["read(\"user:reader\")".into()]),
                ),
            ]),
        );

        let registry = StaticRegistry::new()
            .register(&RESTRICTED_USERS)
            .expect("registry should accept collection");
        let database = Database::new(adapter, registry);
        let reader_role = Role::user("reader", None).expect("reader role should parse");
        let repo = database
            .scope(crate::Context::default().with_role(reader_role))
            .repo::<RestrictedUser>();

        let fetched = block_on(repo.get(&Key::<32>::new("usr_reader").expect("valid id")))
            .expect("get should succeed")
            .expect("record should be readable");
        assert_eq!(fetched.name, "Reader");

        let denied = block_on(
            database
                .repo::<RestrictedUser>()
                .get(&Key::<32>::new("usr_reader").expect("valid id")),
        );
        assert!(denied.is_err());
    }

    #[test]
    fn find_and_count_filter_by_document_permissions_when_needed() {
        let adapter = FakeAdapter::default();
        let mut rows = adapter.rows.lock().expect("rows lock");
        rows.insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_one".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_one".into()),
                ),
                ("name".into(), StorageValue::String("Visible".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["read(\"user:reader\")".into()]),
                ),
            ]),
        );
        rows.insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_two".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_two".into()),
                ),
                ("name".into(), StorageValue::String("Hidden".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["read(\"user:other\")".into()]),
                ),
            ]),
        );
        drop(rows);

        let registry = StaticRegistry::new()
            .register(&RESTRICTED_USERS)
            .expect("registry should accept collection");
        let database = Database::new(adapter, registry);
        let reader_role = Role::user("reader", None).expect("reader role should parse");
        let repo = database
            .scope(crate::Context::default().with_role(reader_role))
            .repo::<RestrictedUser>();

        let records = block_on(repo.find(QuerySpec::new().sort(USER_ID_FIELD.asc())))
            .expect("find should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id.as_str(), "usr_one");

        let count = block_on(repo.count(QuerySpec::new())).expect("count should succeed");
        assert_eq!(count, 1);
    }

    #[test]
    fn update_uses_document_permissions_when_collection_update_is_denied() {
        let adapter = FakeAdapter::default();
        adapter.rows.lock().expect("rows lock").insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_editor".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_editor".into()),
                ),
                ("name".into(), StorageValue::String("Before".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["update(\"user:editor\")".into()]),
                ),
            ]),
        );

        let registry = StaticRegistry::new()
            .register(&RESTRICTED_USERS)
            .expect("registry should accept collection");
        let database = Database::new(adapter, registry);
        let editor_role = Role::user("editor", None).expect("editor role should parse");
        let repo = database
            .scope(crate::Context::default().with_role(editor_role))
            .repo::<RestrictedUser>();
        let id = Key::<32>::new("usr_editor").expect("valid id");

        let updated = block_on(repo.update(
            &id,
            UpdateUser {
                name: "After".into(),
            },
        ))
        .expect("update should succeed")
        .expect("record should exist");

        assert_eq!(updated.name, "After");

        let denied = block_on(database.repo::<RestrictedUser>().update(
            &id,
            UpdateUser {
                name: "Denied".into(),
            },
        ));
        assert!(denied.is_err());
    }

    #[test]
    fn delete_uses_document_permissions_when_collection_delete_is_denied() {
        let adapter = FakeAdapter::default();
        let mut rows = adapter.rows.lock().expect("rows lock");
        rows.insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_delete".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_delete".into()),
                ),
                ("name".into(), StorageValue::String("Delete Me".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["delete(\"user:editor\")".into()]),
                ),
            ]),
        );
        rows.insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_denied_delete".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_denied_delete".into()),
                ),
                ("name".into(), StorageValue::String("Keep Me".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["delete(\"user:other\")".into()]),
                ),
            ]),
        );
        drop(rows);

        let registry = StaticRegistry::new()
            .register(&RESTRICTED_USERS)
            .expect("registry should accept collection");
        let database = Database::new(adapter, registry);
        let editor_role = Role::user("editor", None).expect("editor role should parse");
        let repo = database
            .scope(crate::Context::default().with_role(editor_role))
            .repo::<RestrictedUser>();
        let id = Key::<32>::new("usr_delete").expect("valid id");

        let deleted = block_on(repo.delete(&id)).expect("delete should succeed");
        assert!(deleted);

        let denied = block_on(repo.delete(&Key::<32>::new("usr_denied_delete").expect("valid id")));
        assert!(denied.is_err());
    }

    #[test]
    fn collection_permission_parse_errors_do_not_fall_back_to_document_scope() {
        let adapter = FakeAdapter::default();
        adapter.rows.lock().expect("rows lock").insert(
            (
                "public".to_string(),
                "invalid_collection_permissions".to_string(),
                "usr_invalid_scope".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_invalid_scope".into()),
                ),
                ("name".into(), StorageValue::String("Reader".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["read(\"user:reader\")".into()]),
                ),
            ]),
        );

        let registry = StaticRegistry::new()
            .register(&INVALID_COLLECTION_PERMISSIONS)
            .expect("registry should accept collection");
        let database = Database::new(adapter, registry);
        let reader_role = Role::user("reader", None).expect("reader role should parse");
        let repo = database
            .scope(crate::Context::default().with_role(reader_role))
            .repo::<InvalidCollectionPermissionsUser>();
        let id = Key::<32>::new("usr_invalid_scope").expect("valid id");

        let error =
            block_on(repo.get(&id)).expect_err("invalid collection permissions should fail");
        assert!(
            matches!(error, DatabaseError::Other(message) if message.contains("invalid permission"))
        );
    }

    #[test]
    fn cached_get_preserves_document_authorization_failures() {
        let adapter = FakeAdapter::default();
        adapter.rows.lock().expect("rows lock").insert(
            (
                "public".to_string(),
                "restricted_users".to_string(),
                "usr_cached_reader".to_string(),
            ),
            BTreeMap::from([
                (
                    crate::FIELD_ID.into(),
                    StorageValue::String("usr_cached_reader".into()),
                ),
                ("name".into(), StorageValue::String("Reader".into())),
                (
                    crate::FIELD_PERMISSIONS.into(),
                    StorageValue::StringArray(vec!["read(\"user:reader\")".into()]),
                ),
            ]),
        );

        let registry = StaticRegistry::new()
            .register(&RESTRICTED_USERS)
            .expect("registry should accept collection");
        let cache = Arc::new(database_cache::MemoryCacheBackend::default());
        let database = Database::new(adapter, registry).with_cache(cache);
        let reader_role = Role::user("reader", None).expect("reader role should parse");
        let reader_repo = database
            .scope(crate::Context::default().with_role(reader_role))
            .repo::<RestrictedUser>();
        let id = Key::<32>::new("usr_cached_reader").expect("valid id");

        let fetched = block_on(reader_repo.get(&id))
            .expect("authorized get should succeed")
            .expect("record should exist");
        assert_eq!(fetched.name, "Reader");

        let denied = block_on(database.repo::<RestrictedUser>().get(&id));
        assert!(
            denied.is_err(),
            "cached reads should still respect auth failures"
        );
    }

    #[test]
    fn cache_is_populated_and_invalidated_across_crud_flow() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let cache = Arc::new(database_cache::MemoryCacheBackend::default());
        let database = Database::new(FakeAdapter::default(), registry).with_cache(cache.clone());
        let repo = database.repo::<User>();
        let id = Key::<32>::new("usr_cached_flow").expect("valid id");
        let cache_key =
            CacheKey::new("h7573657273__h7573725f6361636865645f666c6f77").expect("valid key");
        let namespace = database_cache::Namespace::new("docs").expect("valid namespace");

        let created = block_on(repo.insert(CreateUser {
            id: id.clone(),
            name: "Ravi".into(),
        }))
        .expect("insert should succeed");
        assert_eq!(created.name, "Ravi");
        assert!(block_on(cache.exists(&namespace, &cache_key)).expect("exists should succeed"));

        let updated = block_on(repo.update(
            &id,
            UpdateUser {
                name: "Aman".into(),
            },
        ))
        .expect("update should succeed")
        .expect("row should exist");
        assert_eq!(updated.name, "Aman");
        assert!(
            !block_on(cache.exists(&namespace, &cache_key)).expect("exists should succeed"),
            "update should invalidate the stale cache entry"
        );

        let fetched = block_on(repo.get(&id))
            .expect("get should succeed")
            .expect("row should exist");
        assert_eq!(fetched.name, "Aman");
        assert!(block_on(cache.exists(&namespace, &cache_key)).expect("exists should succeed"));

        let deleted = block_on(repo.delete(&id)).expect("delete should succeed");
        assert!(deleted);
        assert!(
            !block_on(cache.exists(&namespace, &cache_key)).expect("exists should succeed"),
            "delete should invalidate the cache entry"
        );
    }

    #[test]
    fn finds_and_counts_with_typed_query_fields() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let database = Database::new(FakeAdapter::default(), registry);
        let repo = database.repo::<User>();

        for (id, name) in [("usr_1", "Ravi"), ("usr_2", "Aman"), ("usr_3", "Ravi")] {
            block_on(repo.insert(CreateUser {
                id: Key::<32>::new(id).expect("valid id"),
                name: name.to_string(),
            }))
            .expect("insert should succeed");
        }

        let ravis = block_on(
            repo.find(
                QuerySpec::new()
                    .filter(USER_NAME_FIELD.eq("Ravi"))
                    .sort(USER_ID_FIELD.desc()),
            ),
        )
        .expect("find should succeed");

        assert_eq!(ravis.len(), 2);
        assert_eq!(ravis[0].id.as_str(), "usr_3");
        assert_eq!(ravis[1].id.as_str(), "usr_1");

        let first_ravi = block_on(
            repo.find_one(
                QuerySpec::new()
                    .filter(USER_NAME_FIELD.eq("Ravi"))
                    .sort(USER_ID_FIELD.asc()),
            ),
        )
        .expect("find_one should succeed")
        .expect("record should exist");
        assert_eq!(first_ravi.id.as_str(), "usr_1");

        let count = block_on(repo.count(QuerySpec::new().filter(USER_NAME_FIELD.eq("Ravi"))))
            .expect("count should succeed");
        assert_eq!(count, 2);
    }

    #[test]
    fn enforces_collection_permissions_in_repository_api() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .and_then(|registry| registry.register(&RESTRICTED_USERS))
            .expect("registry should accept collections");
        let database = Database::new(FakeAdapter::default(), registry);

        let denied_repo = database.repo::<RestrictedUser>();
        let denied_create = block_on(denied_repo.insert(CreateUser {
            id: Key::<32>::new("usr_denied").expect("valid id"),
            name: "Denied".into(),
        }));
        assert!(denied_create.is_err());

        let admin_role = Role::user("admin", None).expect("admin role should parse");
        let allowed_repo = database
            .scope(crate::Context::default().with_role(admin_role))
            .repo::<RestrictedUser>();

        let created = block_on(allowed_repo.insert(CreateUser {
            id: Key::<32>::new("usr_admin").expect("valid id"),
            name: "Admin".into(),
        }))
        .expect("admin insert should succeed");

        let fetched = block_on(allowed_repo.get(&created.id))
            .expect("admin get should succeed")
            .expect("admin record should exist");
        assert_eq!(fetched.name, "Admin");

        let denied_read = block_on(database.repo::<RestrictedUser>().find(QuerySpec::new()))
            .expect("find should succeed");
        assert!(denied_read.is_empty());
    }

    fn matches_filter(record: &StorageRecord, filter: &crate::query::Filter) -> bool {
        match filter {
            crate::query::Filter::Field { field, op } => {
                let value = record.get(&field.to_string());

                match op {
                    FilterOp::Eq(expected) => value == Some(expected),
                    FilterOp::NotEq(expected) => value != Some(expected),
                    FilterOp::In(values) => value
                        .map(|current| values.contains(current))
                        .unwrap_or(false),
                    FilterOp::Gt(expected) => compare_value(value, Some(expected)).is_gt(),
                    FilterOp::Gte(expected) => {
                        let ordering = compare_value(value, Some(expected));
                        ordering.is_gt() || ordering.is_eq()
                    }
                    FilterOp::Lt(expected) => compare_value(value, Some(expected)).is_lt(),
                    FilterOp::Lte(expected) => {
                        let ordering = compare_value(value, Some(expected));
                        ordering.is_lt() || ordering.is_eq()
                    }
                    FilterOp::Contains(expected) => match (value, expected) {
                        (Some(StorageValue::String(s)), StorageValue::String(sub)) => {
                            s.contains(sub)
                        }
                        _ => false,
                    },
                    FilterOp::StartsWith(expected) => match (value, expected) {
                        (Some(StorageValue::String(s)), StorageValue::String(prefix)) => {
                            s.starts_with(prefix)
                        }
                        _ => false,
                    },
                    FilterOp::EndsWith(expected) => match (value, expected) {
                        (Some(StorageValue::String(s)), StorageValue::String(suffix)) => {
                            s.ends_with(suffix)
                        }
                        _ => false,
                    },
                    FilterOp::TextSearch(expected) => match (value, expected) {
                        (Some(StorageValue::String(s)), StorageValue::String(query)) => {
                            s.to_lowercase().contains(&query.to_lowercase())
                        }
                        _ => false,
                    },
                    FilterOp::IsNull => matches!(value, None | Some(StorageValue::Null)),
                    FilterOp::IsNotNull => !matches!(value, None | Some(StorageValue::Null)),
                }
            }
            crate::query::Filter::And(filters) => filters.iter().all(|f| matches_filter(record, f)),
            crate::query::Filter::Or(filters) => filters.iter().any(|f| matches_filter(record, f)),
            crate::query::Filter::Not(filter) => !matches_filter(record, filter),
        }
    }

    fn compare_records(
        left: &StorageRecord,
        right: &StorageRecord,
        field: &str,
        direction: SortDirection,
    ) -> std::cmp::Ordering {
        let ordering = compare_value(left.get(field), right.get(field));
        match direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        }
    }

    fn compare_value(
        left: Option<&StorageValue>,
        right: Option<&StorageValue>,
    ) -> std::cmp::Ordering {
        match (left, right) {
            (Some(StorageValue::String(left)), Some(StorageValue::String(right))) => {
                left.cmp(right)
            }
            (Some(StorageValue::Int(left)), Some(StorageValue::Int(right))) => left.cmp(right),
            (Some(StorageValue::Float(left)), Some(StorageValue::Float(right))) => {
                left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal)
            }
            (Some(StorageValue::Bool(left)), Some(StorageValue::Bool(right))) => left.cmp(right),
            (Some(StorageValue::Timestamp(left)), Some(StorageValue::Timestamp(right))) => {
                left.cmp(right)
            }
            (Some(StorageValue::Null), Some(StorageValue::Null)) | (None, None) => {
                std::cmp::Ordering::Equal
            }
            (None, Some(_)) | (Some(StorageValue::Null), Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) | (Some(_), Some(StorageValue::Null)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}
