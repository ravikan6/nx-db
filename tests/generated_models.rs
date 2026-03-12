use database::query::{Filter, FilterOp};
use database::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use database::{Context, Database, DatabaseError, Field, QuerySpec};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, RawWaker, RawWakerVTable, Waker};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/codegen/models.rs"
));
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/codegen/filtered_models.rs"
));
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/codegen/virtual_models.rs"
));

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DisplayName(String);

impl DisplayName {
    fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

mod codecs {
    use super::DisplayName;
    use database::DatabaseError;

    pub fn encode_display_name(value: DisplayName) -> Result<String, DatabaseError> {
        Ok(format!("stored::{}", value.0.to_ascii_uppercase()))
    }

    pub fn decode_display_name(value: String) -> Result<DisplayName, DatabaseError> {
        Ok(DisplayName(
            value.trim_start_matches("stored::").to_ascii_lowercase(),
        ))
    }
}

mod resolvers {
    use crate::virtual_app_models;
    use database::{Context, DatabaseError};

    pub async fn resolve_profile_label(
        entity: &virtual_app_models::UserEntity,
        _context: &Context,
    ) -> Result<String, DatabaseError> {
        Ok(format!("profile:{}", entity.name.to_ascii_lowercase()))
    }
}

#[derive(Default, Clone)]
struct FakeAdapter {
    rows: Arc<Mutex<BTreeMap<(String, String, String), StorageRecord>>>,
}

impl StorageAdapter for FakeAdapter {
    fn ping(&self, _context: &Context) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async { Ok(()) })
    }

    fn create_collection(
        &self,
        _context: &Context,
        _schema: &'static database::CollectionSchema,
    ) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async { Ok(()) })
    }

    fn insert(
        &self,
        context: &Context,
        schema: &'static database::CollectionSchema,
        values: StorageRecord,
    ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let rows = self.rows.clone();
        let key = (
            context.schema().to_string(),
            schema.id.to_string(),
            match values.get(database::FIELD_ID) {
                Some(StorageValue::String(value)) => value.clone(),
                _ => return Box::pin(async { Err(DatabaseError::Other("missing id".into())) }),
            },
        );

        Box::pin(async move {
            rows.lock().expect("rows lock").insert(key, values.clone());
            Ok(values)
        })
    }

    fn insert_many(
        &self,
        context: &Context,
        schema: &'static database::CollectionSchema,
        values: Vec<StorageRecord>,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let rows = self.rows.clone();
        let schema_name = context.schema().to_string();
        let collection = schema.id.to_string();

        Box::pin(async move {
            let mut locked = rows.lock().expect("rows lock");
            for record in &values {
                let id = match record.get(database::FIELD_ID) {
                    Some(StorageValue::String(value)) => value.clone(),
                    _ => return Err(DatabaseError::Other("missing id".into())),
                };
                locked.insert((schema_name.clone(), collection.clone(), id), record.clone());
            }
            Ok(values)
        })
    }

    fn get(
        &self,
        context: &Context,
        schema: &'static database::CollectionSchema,
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
        context: &Context,
        schema: &'static database::CollectionSchema,
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
        context: &Context,
        schema: &'static database::CollectionSchema,
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
        context: &Context,
        schema: &'static database::CollectionSchema,
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
                    if query.filters().iter().all(|filter| matches_filter(record, filter)) {
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
        context: &Context,
        schema: &'static database::CollectionSchema,
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
                    if query.filters().iter().all(|filter| matches_filter(record, filter)) {
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
        context: &Context,
        schema: &'static database::CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let rows = self.rows.clone();
        let schema_name = context.schema().to_string();
        let collection = schema.id.to_string();
        let query = query.clone();

        Box::pin(async move {
            Ok(rows
                .lock()
                .expect("rows lock")
                .iter()
                .filter(|((row_schema, row_collection, _), _)| {
                    row_schema == &schema_name && row_collection == &collection
                })
                .filter(|(_, record)| {
                    query
                        .filters()
                        .iter()
                        .all(|filter| matches_filter(record, filter))
                })
                .map(|(_, record)| record.clone())
                .collect())
        })
    }

    fn count(
        &self,
        context: &Context,
        schema: &'static database::CollectionSchema,
        query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let rows = self.rows.clone();
        let schema_name = context.schema().to_string();
        let collection = schema.id.to_string();
        let query = query.clone();

        Box::pin(async move {
            Ok(rows
                .lock()
                .expect("rows lock")
                .iter()
                .filter(|((row_schema, row_collection, _), _)| {
                    row_schema == &schema_name && row_collection == &collection
                })
                .filter(|(_, record)| {
                    query
                        .filters()
                        .iter()
                        .all(|filter| matches_filter(record, filter))
                })
                .count() as u64)
        })
    }
}

fn matches_filter(record: &StorageRecord, filter: &Filter) -> bool {
    let value = record.get(&filter.field);

    match &filter.op {
        FilterOp::Eq(expected) => value == Some(expected),
        FilterOp::NotEq(expected) => value != Some(expected),
        FilterOp::In(expected) => value
            .map(|current| expected.iter().any(|item| item == current))
            .unwrap_or(false),
        FilterOp::Gt(expected) => compare_value(value, expected, |ordering| ordering.is_gt()),
        FilterOp::Gte(expected) => compare_value(value, expected, |ordering| {
            ordering.is_gt() || ordering.is_eq()
        }),
        FilterOp::Lt(expected) => compare_value(value, expected, |ordering| ordering.is_lt()),
        FilterOp::Lte(expected) => compare_value(value, expected, |ordering| {
            ordering.is_lt() || ordering.is_eq()
        }),
        FilterOp::Contains(expected) => match (value, expected) {
            (Some(StorageValue::String(s)), StorageValue::String(sub)) => s.contains(sub),
            _ => false,
        },
        FilterOp::StartsWith(expected) => match (value, expected) {
            (Some(StorageValue::String(s)), StorageValue::String(prefix)) => s.starts_with(prefix),
            _ => false,
        },
        FilterOp::EndsWith(expected) => match (value, expected) {
            (Some(StorageValue::String(s)), StorageValue::String(suffix)) => s.ends_with(suffix),
            _ => false,
        },
        FilterOp::TextSearch(expected) => match (value, expected) {
            (Some(StorageValue::String(s)), StorageValue::String(query)) => s.to_lowercase().contains(&query.to_lowercase()),
            _ => false,
        },
        FilterOp::IsNull => matches!(value, None | Some(StorageValue::Null)),
        FilterOp::IsNotNull => !matches!(value, None | Some(StorageValue::Null)),
    }
}

fn compare_value(
    current: Option<&StorageValue>,
    expected: &StorageValue,
    cmp: impl Fn(std::cmp::Ordering) -> bool,
) -> bool {
    match (current, expected) {
        (Some(StorageValue::String(left)), StorageValue::String(right)) => cmp(left.cmp(right)),
        (Some(StorageValue::Bool(left)), StorageValue::Bool(right)) => cmp(left.cmp(right)),
        (Some(StorageValue::Int(left)), StorageValue::Int(right)) => cmp(left.cmp(right)),
        (Some(StorageValue::Float(left)), StorageValue::Float(right)) => {
            left.partial_cmp(right).map(cmp).unwrap_or(false)
        }
        (Some(StorageValue::Timestamp(left)), StorageValue::Timestamp(right)) => {
            cmp(left.cmp(right))
        }
        _ => false,
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

#[test]
fn generated_models_compile_and_work_with_repository_api() {
    let registry = app_models::registry().expect("registry should build");
    let database = Database::new(FakeAdapter::default(), registry);
    let repo = database.repo::<app_models::User>();

    let created = block_on(repo.insert(app_models::CreateUser {
        id: app_models::UserId::new("usr_generated").expect("valid id"),
        name: "Ravi".into(),
        email: Some("ravi@example.com".into()),
        active: true,
    }))
    .expect("insert should succeed");

    assert_eq!(created.id.as_str(), "usr_generated");
    assert_eq!(created.email.as_deref(), Some("ravi@example.com"));

    let fetched = block_on(repo.get(&created.id))
        .expect("get should succeed")
        .expect("row should exist");
    assert_eq!(fetched.name, "Ravi");

    let updated = block_on(repo.update(
        &created.id,
        app_models::UpdateUser {
            email: database::Patch::set(Some("updated@example.com".into())),
            ..Default::default()
        },
    ))
    .expect("update should succeed")
    .expect("row should exist");
    assert_eq!(updated.email.as_deref(), Some("updated@example.com"));

    let repo_by_marker = database.get_repo(app_models::USER);
    let count = block_on(repo_by_marker.count(QuerySpec::new())).expect("count should succeed");
    assert_eq!(count, 1);

    let field: Field<app_models::User, bool> = app_models::USER_ACTIVE;
    let active =
        block_on(repo.find(QuerySpec::new().filter(field.eq(true)))).expect("find should succeed");
    assert_eq!(active.len(), 1);
}

#[test]
fn generated_filtered_models_apply_encode_decode_hooks() {
    let registry = filtered_app_models::registry().expect("registry should build");
    let adapter = FakeAdapter::default();
    let database = Database::new(adapter.clone(), registry);
    let repo = database.repo::<filtered_app_models::User>();

    let created = block_on(repo.insert(filtered_app_models::CreateUser {
        id: filtered_app_models::UserId::new("usr_filtered").expect("valid id"),
        name: DisplayName::new("Ravi"),
        active: true,
    }))
    .expect("insert should succeed");

    assert_eq!(created.name.as_str(), "ravi");

    let stored = adapter.rows.lock().expect("rows lock");
    let row = stored
        .get(&(
            "public".to_string(),
            "users".to_string(),
            "usr_filtered".to_string(),
        ))
        .expect("stored row should exist");
    assert_eq!(
        row.get("name"),
        Some(&StorageValue::String("stored::RAVI".into()))
    );
    drop(stored);

    let fetched = block_on(repo.get(&created.id))
        .expect("get should succeed")
        .expect("row should exist");
    assert_eq!(fetched.name.as_str(), "ravi");

    let matching = block_on(
        repo.find(
            QuerySpec::new()
                .try_filter(filtered_app_models::USER_NAME.eq(DisplayName::new("Ravi")))
                .expect("query filter should encode"),
        ),
    )
    .expect("find should succeed");
    assert_eq!(matching.len(), 1);

    let updated = block_on(repo.update(
        &created.id,
        filtered_app_models::UpdateUser {
            name: database::Patch::set(DisplayName::new("Kiran")),
            ..Default::default()
        },
    ))
    .expect("update should succeed")
    .expect("row should exist");
    assert_eq!(updated.name.as_str(), "kiran");

    let stored = adapter.rows.lock().expect("rows lock");
    let row = stored
        .get(&(
            "public".to_string(),
            "users".to_string(),
            "usr_filtered".to_string(),
        ))
        .expect("stored row should exist");
    assert_eq!(
        row.get("name"),
        Some(&StorageValue::String("stored::KIRAN".into()))
    );
    drop(stored);

    let matching = block_on(
        repo.find(
            QuerySpec::new()
                .try_filter(filtered_app_models::USER_NAME.eq(DisplayName::new("Kiran")))
                .expect("query filter should encode"),
        ),
    )
    .expect("find should succeed");
    assert_eq!(matching.len(), 1);
}

#[test]
fn generated_virtual_models_resolve_after_reads_and_reject_virtual_queries() {
    let registry = virtual_app_models::registry().expect("registry should build");
    let adapter = FakeAdapter::default();
    let database = Database::new(adapter, registry);
    let repo = database.repo::<virtual_app_models::User>();

    let created = block_on(repo.insert(virtual_app_models::CreateUser {
        id: virtual_app_models::UserId::new("usr_virtual").expect("valid id"),
        name: "Ravi".into(),
        active: true,
    }))
    .expect("insert should succeed");

    assert_eq!(created.profile_label.as_deref(), Some("profile:ravi"));

    let fetched = block_on(repo.get(&created.id))
        .expect("get should succeed")
        .expect("row should exist");
    assert_eq!(fetched.profile_label.as_deref(), Some("profile:ravi"));

    let error = block_on(repo.find(QuerySpec::new().filter(Filter {
        field: "profileLabel".into(),
        op: FilterOp::Eq(StorageValue::String("profile:ravi".into())),
    })))
    .expect_err("virtual query should be rejected");
    assert!(
        error
            .to_string()
            .contains("virtual field 'profileLabel' cannot be used in filters")
    );
}
