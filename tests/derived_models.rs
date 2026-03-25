use nx_db::traits::storage::{AdapterFuture, StorageAdapter, StorageRecord, StorageValue};
use nx_db::{CollectionSchema, Context, Database, DatabaseError, Field, QuerySpec, StaticRegistry};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DisplayName(String);

fn encode_display_name(value: DisplayName) -> Result<String, DatabaseError> {
    Ok(value.0.to_ascii_uppercase())
}

fn decode_display_name(value: String) -> Result<DisplayName, DatabaseError> {
    Ok(DisplayName(value.to_ascii_lowercase()))
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, nx_db::NxEntity)]
struct DerivedUserEntity {
    #[nx(id)]
    id: nx_db::Key<48>,
    #[nx(field = "name", required, decode = "crate::decode_display_name")]
    name: DisplayName,
    #[nx(field = "email")]
    email: Option<String>,
    #[nx(field = "active", required)]
    active: bool,
    #[nx(metadata)]
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone, nx_db::NxCreate)]
struct CreateDerivedUser {
    #[nx(id)]
    id: Option<nx_db::Key<48>>,
    #[nx(field = "name", required, encode = "crate::encode_display_name")]
    name: DisplayName,
    #[nx(field = "email")]
    email: Option<String>,
    #[nx(field = "active", required)]
    active: bool,
    #[nx(permissions)]
    permissions: Vec<String>,
}

#[derive(Debug, Clone, Default, nx_db::NxUpdate)]
struct UpdateDerivedUser {
    #[nx(field = "name", encode = "crate::encode_display_name")]
    name: nx_db::Patch<DisplayName>,
    #[nx(field = "email")]
    email: nx_db::Patch<Option<String>>,
    #[nx(field = "active")]
    active: nx_db::Patch<bool>,
    #[nx(permissions)]
    permissions: nx_db::Patch<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
struct DerivedUser;

const DERIVED_USER_NAME: Field<DerivedUser, DisplayName> = Field::new("name");

const DERIVED_USERS_ATTRIBUTES: &[nx_db::AttributeSchema] = &[
    nx_db::AttributeSchema {
        id: "name",
        column: "name",
        kind: nx_db::AttributeKind::String,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: nx_db::AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
    nx_db::AttributeSchema {
        id: "email",
        column: "email",
        kind: nx_db::AttributeKind::String,
        required: false,
        array: false,
        length: None,
        default: None,
        persistence: nx_db::AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
    nx_db::AttributeSchema {
        id: "active",
        column: "active",
        kind: nx_db::AttributeKind::Boolean,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: nx_db::AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
];

static DERIVED_USERS_SCHEMA: CollectionSchema = CollectionSchema {
    id: "derived_users",
    name: "DerivedUsers",
    document_security: true,
    enabled: true,
    permissions: &["read(\"any\")", "create(\"any\")", "update(\"any\")"],
    attributes: DERIVED_USERS_ATTRIBUTES,
    indexes: &[],
};

nx_db::impl_model_record_bridge! {
    name: DerivedUser,
    entity: DerivedUserEntity,
    create: CreateDerivedUser,
    update: UpdateDerivedUser,
    schema: DERIVED_USERS_SCHEMA
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
        _schema: &'static CollectionSchema,
    ) -> AdapterFuture<'_, Result<(), DatabaseError>> {
        Box::pin(async { Ok(()) })
    }

    fn insert(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        mut values: StorageRecord,
    ) -> AdapterFuture<'_, Result<StorageRecord, DatabaseError>> {
        let rows = self.rows.clone();
        let key = (
            context.schema().to_string(),
            schema.id.to_string(),
            match values.get(nx_db::FIELD_ID) {
                Some(StorageValue::String(value)) => value.clone(),
                _ => return Box::pin(async { Err(DatabaseError::Other("missing id".into())) }),
            },
        );

        values
            .entry(nx_db::FIELD_SEQUENCE.to_string())
            .or_insert(StorageValue::Int(1));
        values
            .entry(nx_db::FIELD_CREATED_AT.to_string())
            .or_insert_with(|| StorageValue::Timestamp(nx_db::time::OffsetDateTime::now_utc()));
        values
            .entry(nx_db::FIELD_UPDATED_AT.to_string())
            .or_insert_with(|| StorageValue::Timestamp(nx_db::time::OffsetDateTime::now_utc()));
        values
            .entry(nx_db::FIELD_PERMISSIONS.to_string())
            .or_insert(StorageValue::StringArray(vec![]));

        Box::pin(async move {
            rows.lock().expect("rows lock").insert(key, values.clone());
            Ok(values)
        })
    }

    fn insert_many(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        values: Vec<StorageRecord>,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let adapter = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let mut inserted = Vec::new();
            for value in values {
                inserted.push(adapter.insert(&context, schema, value).await?);
            }
            Ok(inserted)
        })
    }

    fn get(
        &self,
        context: &Context,
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
        context: &Context,
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
            let mut rows = rows.lock().expect("rows lock");
            let Some(record) = rows.get_mut(&key) else {
                return Ok(None);
            };
            for (field, value) in values {
                record.insert(field, value);
            }
            record.insert(
                nx_db::FIELD_UPDATED_AT.to_string(),
                StorageValue::Timestamp(nx_db::time::OffsetDateTime::now_utc()),
            );
            Ok(Some(record.clone()))
        })
    }

    fn update_many(
        &self,
        _context: &Context,
        _schema: &'static CollectionSchema,
        _query: &QuerySpec,
        _values: StorageRecord,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async { Ok(0) })
    }

    fn delete(
        &self,
        context: &Context,
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

    fn delete_many(
        &self,
        _context: &Context,
        _schema: &'static CollectionSchema,
        _query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        Box::pin(async { Ok(0) })
    }

    fn find(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        _query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<Vec<StorageRecord>, DatabaseError>> {
        let rows = self.rows.clone();
        let schema_name = context.schema().to_string();
        let collection = schema.id.to_string();
        Box::pin(async move {
            Ok(rows
                .lock()
                .expect("rows lock")
                .iter()
                .filter(|((row_schema, row_collection, _), _)| {
                    row_schema == &schema_name && row_collection == &collection
                })
                .map(|(_, record)| record.clone())
                .collect())
        })
    }

    fn count(
        &self,
        context: &Context,
        schema: &'static CollectionSchema,
        _query: &QuerySpec,
    ) -> AdapterFuture<'_, Result<u64, DatabaseError>> {
        let rows = self.rows.clone();
        let schema_name = context.schema().to_string();
        let collection = schema.id.to_string();
        Box::pin(async move {
            Ok(rows
                .lock()
                .expect("rows lock")
                .iter()
                .filter(|((row_schema, row_collection, _), _)| {
                    row_schema == &schema_name && row_collection == &collection
                })
                .count() as u64)
        })
    }
}

#[tokio::test]
async fn derive_based_model_flow_works_with_bridge_macro() {
    let registry = StaticRegistry::new()
        .register(&DERIVED_USERS_SCHEMA)
        .expect("schema should register");
    let database = Database::new(FakeAdapter::default(), registry);
    let repo = database.repo::<DerivedUser>();

    let created = repo
        .insert(
            CreateDerivedUser::builder(DisplayName("Ravi".into()), true)
                .email(Some("ravi@example.com".into())),
        )
        .await
        .expect("insert should succeed");

    assert_eq!(created.name, DisplayName("ravi".into()));
    assert_eq!(created.email.as_deref(), Some("ravi@example.com"));
    assert_eq!(DERIVED_USER_NAME.name(), "name");

    let fetched = repo
        .get(&created.id)
        .await
        .expect("get should succeed")
        .expect("entity should exist");
    assert_eq!(fetched.name, DisplayName("ravi".into()));

    let updated = repo
        .update(
            &created.id,
            UpdateDerivedUser {
                name: nx_db::Patch::set(DisplayName("RAVIKAN".into())),
                email: nx_db::Patch::set(Some("rk@example.com".into())),
                ..Default::default()
            },
        )
        .await
        .expect("update should succeed")
        .expect("entity should update");

    assert_eq!(updated.name, DisplayName("ravikan".into()));
    assert_eq!(updated.email.as_deref(), Some("rk@example.com"));
}
