use crate::events::{Event, EventBus, NoopEventBus};
use crate::errors::DatabaseError;
use crate::model::Model;
use crate::registry::CollectionRegistry;
use crate::repository::{Repository, ScopedDatabase};
use crate::schema::CollectionSchema;
use crate::traits::storage::{StorageAdapter, StorageRecord, StorageValue};
use crate::Context;

pub struct Database<A, R, E = NoopEventBus> {
    adapter: A,
    registry: R,
    events: E,
}

impl<A, R> Database<A, R, NoopEventBus> {
    pub fn new(adapter: A, registry: R) -> Self {
        Self {
            adapter,
            registry,
            events: NoopEventBus,
        }
    }
}

impl<A, R, E> Database<A, R, E> {
    pub fn with_events(adapter: A, registry: R, events: E) -> Self {
        Self {
            adapter,
            registry,
            events,
        }
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn registry(&self) -> &R {
        &self.registry
    }

    pub fn events(&self) -> &E {
        &self.events
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
        self.scope(Context::default()).get_repo(model)
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
            .ok_or_else(|| DatabaseError::Other(format!("collection '{id}' is not registered")))
    }

    pub fn validate_registry(&self) -> Result<(), DatabaseError> {
        self.registry.validate()
    }

    pub fn validate_storage_record(
        &self,
        collection: &'static CollectionSchema,
        record: &StorageRecord,
    ) -> Result<(), DatabaseError> {
        for key in record.keys() {
            if key.starts_with('$') || key.starts_with('_') {
                continue;
            }

            if collection.attribute(key).is_none() {
                return Err(DatabaseError::Other(format!(
                    "collection '{}': unknown attribute '{}'",
                    collection.id, key
                )));
            }
        }

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
        self.events.dispatch(Event::collection_created(collection.id));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::enums::AttributeKind;
    use crate::events::{Event, EventBus};
    use crate::key::Key;
    use crate::model::Model;
    use crate::registry::StaticRegistry;
    use crate::schema::{AttributePersistence, AttributeSchema, CollectionSchema};
    use crate::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
    use crate::errors::DatabaseError;
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker};

    const ATTRIBUTES: &[AttributeSchema] = &[
        AttributeSchema {
            id: "id",
            column: "id",
            kind: AttributeKind::String,
            required: true,
            array: false,
            persistence: AttributePersistence::Persisted,
            filters: &[],
            relationship: None,
        },
        AttributeSchema {
            id: "name",
            column: "name",
            kind: AttributeKind::String,
            required: true,
            array: false,
            persistence: AttributePersistence::Persisted,
            filters: &[],
            relationship: None,
        },
    ];

    static USERS: CollectionSchema = CollectionSchema {
        id: "users",
        name: "Users",
        document_security: true,
        enabled: true,
        permissions: &["read(\"any\")"],
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
                let id = match values.get("id") {
                    Some(StorageValue::String(value)) => value.clone(),
                    _ => {
                        return Err(DatabaseError::Other(
                            "record is missing string id field".into(),
                        ))
                    }
                };

                rows.lock()
                    .expect("rows lock")
                    .insert((schema_name, collection, id), values.clone());

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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct UserEntity {
        id: Key<32>,
        name: String,
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

    const USER: User = User;

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

        fn create_to_record(
            input: Self::Create,
            _context: &crate::Context,
        ) -> Result<StorageRecord, DatabaseError> {
            Ok(BTreeMap::from([
                ("id".into(), StorageValue::String(input.id.to_string())),
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
            let id = match record.remove("id") {
                Some(StorageValue::String(value)) => Key::<32>::new(value)?,
                _ => return Err(DatabaseError::Other("missing id".into())),
            };

            let name = match record.remove("name") {
                Some(StorageValue::String(value)) => value,
                _ => return Err(DatabaseError::Other("missing name".into())),
            };

            Ok(UserEntity { id, name })
        }
    }

    #[test]
    fn creates_registered_collection() {
        let registry = StaticRegistry::new()
            .register(&USERS)
            .expect("registry should accept collection");
        let events = RecordingEvents::default();
        let database = Database::with_events(FakeAdapter::default(), registry, events.clone());

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
        let repo = database.scope(context.clone()).get_repo(USER);

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
        assert!(missing.is_none(), "default context should not see tenant_alpha data");
        assert_eq!(default_repo.context().schema(), "public");
        assert_eq!(repo.context().schema(), context.schema());
    }
}
