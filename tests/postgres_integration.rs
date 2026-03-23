#![cfg(feature = "postgres")]

use driver_postgres::PostgresAdapter;
use nx_db::traits::storage::StorageRecord;
use nx_db::{
    AttributeKind, AttributePersistence, AttributeSchema, CollectionSchema, Context, Database,
    DatabaseError, FIELD_PERMISSIONS, Key, Model, QuerySpec, Role, insert_value, take_required,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Row};
use std::collections::BTreeSet;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/codegen/models.rs"
));

const RESTRICTED_ATTRIBUTES: &[AttributeSchema] = &[AttributeSchema {
    id: "name",
    column: "name",
    kind: AttributeKind::String,
    required: true,
    array: false,
    persistence: AttributePersistence::Persisted,
    filters: &[],
    relationship: None,
    length: None,
    default: None,
}];

static RESTRICTED_USERS_SCHEMA: CollectionSchema = CollectionSchema {
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
    attributes: RESTRICTED_ATTRIBUTES,
    indexes: &[],
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RestrictedUserEntity {
    id: Key<32>,
    name: String,
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone)]
struct CreateRestrictedUser {
    id: Key<32>,
    name: String,
    permissions: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct UpdateRestrictedUser {
    name: nx_db::Patch<String>,
}

#[derive(Debug, Clone, Copy)]
struct RestrictedUser;

impl Model for RestrictedUser {
    type Id = Key<32>;
    type Entity = RestrictedUserEntity;
    type Create = CreateRestrictedUser;
    type Update = UpdateRestrictedUser;

    fn schema() -> &'static CollectionSchema {
        &RESTRICTED_USERS_SCHEMA
    }

    fn entity_to_id(entity: &Self::Entity) -> &Self::Id {
        &entity.id
    }

    fn entity_metadata(entity: &Self::Entity) -> &nx_db::Metadata {
        &entity._metadata
    }

    fn create_to_record(
        input: Self::Create,
        _context: &Context,
    ) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();
        insert_value(&mut record, nx_db::FIELD_ID, input.id);
        insert_value(&mut record, "name", input.name);
        insert_value(&mut record, FIELD_PERMISSIONS, input.permissions);
        Ok(record)
    }

    fn update_to_record(
        input: Self::Update,
        _context: &Context,
    ) -> Result<StorageRecord, DatabaseError> {
        let mut record = StorageRecord::new();
        if let nx_db::Patch::Set(value) = input.name {
            insert_value(&mut record, "name", value);
        }
        Ok(record)
    }

    fn entity_from_record(
        mut record: StorageRecord,
        _context: &Context,
    ) -> Result<Self::Entity, DatabaseError> {
        Ok(RestrictedUserEntity {
            id: nx_db::get_required(&record, nx_db::FIELD_ID)?,
            name: take_required(&mut record, "name")?,
            _metadata: nx_db::core::model::extract_metadata(&mut record)?,
        })
    }
}

fn test_database_url() -> Option<String> {
    env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| env::var("DATABASE_URL").ok())
}

fn unique_schema() -> String {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be valid")
        .as_nanos();
    format!("it_{}_{}", std::process::id(), stamp)
}

#[tokio::test]
async fn actual_postgres_repo_flow_and_document_security() -> Result<(), Box<dyn std::error::Error>>
{
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping postgres integration test: TEST_DATABASE_URL or DATABASE_URL not set");
        return Ok(());
    };

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let schema = unique_schema();
    let context = Context::default().with_schema(schema.clone());
    let admin_context = context
        .clone()
        .with_role(Role::user("admin", None).expect("admin role should parse"));
    let reader_context = context
        .clone()
        .with_role(Role::user("reader", None).expect("reader role should parse"));

    let registry = app_models::registry()?.register(&RESTRICTED_USERS_SCHEMA)?;
    let adapter = PostgresAdapter::new(pool.clone());
    let database = Database::new(adapter, registry);

    let result = async {
        database
            .scope(admin_context.clone())
            .repo::<app_models::User>()
            .create_collection()
            .await?;
        database
            .scope(admin_context.clone())
            .repo::<app_models::Session>()
            .create_collection()
            .await?;
        database
            .scope(admin_context.clone())
            .repo::<RestrictedUser>()
            .create_collection()
            .await?;

        let user_indexes = sqlx::query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
        )
        .bind(&schema)
        .bind("users")
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("indexname"))
        .collect::<BTreeSet<_>>();
        assert!(user_indexes.contains("users_uid"));
        assert!(user_indexes.contains("users_email_unique"));
        assert!(user_indexes.contains("users_name_active_idx"));

        let session_indexes = sqlx::query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename = $2",
        )
        .bind(&schema)
        .bind("sessions")
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("indexname"))
        .collect::<BTreeSet<_>>();
        assert!(session_indexes.contains("sessions_user_token_idx"));

        let user_repo = database.scope(context.clone()).repo::<app_models::User>();
        let created = user_repo
            .insert(
                app_models::CreateUser::builder("Ravi".into(), true)
                    .email(Some("ravi@example.com".into())),
            )
            .await?;

        assert!(created.id.as_str().len() >= nx_db::GENERATED_ID_MIN_LENGTH);

        let fetched = user_repo
            .get(&created.id)
            .await?
            .expect("user should exist");
        assert_eq!(fetched.name, "Ravi");

        let active = user_repo
            .find(QuerySpec::new().filter(app_models::USER_ACTIVE.eq(true)))
            .await?;
        assert_eq!(active.len(), 1);

        let updated = user_repo
            .update(
                &created.id,
                app_models::UpdateUser {
                    email: nx_db::Patch::set(Some("updated@example.com".into())),
                    ..Default::default()
                },
            )
            .await?
            .expect("user should update");
        assert_eq!(updated.email.as_deref(), Some("updated@example.com"));

        let main_permissions: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT _permissions FROM \"{schema}\".\"restricted_users\" WHERE _uid = $1"
        ))
        .bind("doc_reader")
        .fetch_optional(&pool)
        .await?
        .unwrap_or_default();
        assert!(main_permissions.is_empty());

        let admin_repo = database.scope(admin_context).repo::<RestrictedUser>();
        admin_repo
            .insert(CreateRestrictedUser {
                id: Key::<32>::new("doc_reader").expect("valid id"),
                name: "Allowed".into(),
                permissions: vec![
                    "read(\"user:reader\")".into(),
                    "update(\"user:reader\")".into(),
                    "delete(\"user:reader\")".into(),
                ],
            })
            .await?;
        admin_repo
            .insert(CreateRestrictedUser {
                id: Key::<32>::new("doc_other").expect("valid id"),
                name: "Hidden".into(),
                permissions: vec!["read(\"user:other\")".into()],
            })
            .await?;

        let main_permissions: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT _permissions FROM \"{schema}\".\"restricted_users\" WHERE _uid = $1"
        ))
        .bind("doc_reader")
        .fetch_one(&pool)
        .await?;
        assert!(main_permissions.contains(&"read(\"user:reader\")".to_string()));

        let perms_row: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT permissions FROM \"{schema}\".\"restricted_users_perms\" WHERE permission_type = $1 AND document_id = (SELECT _id FROM \"{schema}\".\"restricted_users\" WHERE _uid = $2)"
        ))
        .bind("read")
        .bind("doc_reader")
        .fetch_one(&pool)
        .await?;
        assert_eq!(perms_row, vec!["user:reader".to_string()]);

        let reader_repo = database.scope(reader_context).repo::<RestrictedUser>();
        let visible = reader_repo.find(QuerySpec::new()).await?;
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].id.as_str(), "doc_reader");

        let visible_count = reader_repo.count(QuerySpec::new()).await?;
        assert_eq!(visible_count, 1);

        let allowed = reader_repo
            .get(&Key::<32>::new("doc_reader").expect("valid id"))
            .await?;
        assert!(allowed.is_some());

        let hidden = reader_repo
            .get(&Key::<32>::new("doc_other").expect("valid id"))
            .await?;
        assert!(hidden.is_none());

        let updated = reader_repo
            .update(
                &Key::<32>::new("doc_reader").expect("valid id"),
                UpdateRestrictedUser {
                    name: nx_db::Patch::set("Updated By Reader".to_string()),
                },
            )
            .await?;
        assert!(updated.is_some());

        let denied_update = reader_repo
            .update(
                &Key::<32>::new("doc_other").expect("valid id"),
                UpdateRestrictedUser {
                    name: nx_db::Patch::set("Denied".to_string()),
                },
            )
            .await?;
        assert!(denied_update.is_none());

        let deleted = reader_repo
            .delete(&Key::<32>::new("doc_reader").expect("valid id"))
            .await?;
        assert!(deleted);

        let denied_delete = reader_repo
            .delete(&Key::<32>::new("doc_other").expect("valid id"))
            .await?;
        assert!(!denied_delete);

        let remaining: i64 = sqlx::query(&format!(
            "SELECT COUNT(*) AS count FROM \"{schema}\".\"restricted_users\""
        ))
        .fetch_one(&pool)
        .await?
        .try_get("count")?;
        assert_eq!(remaining, 1);

        user_repo.delete(&created.id).await?;

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;

    let drop_result = pool
        .execute(format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE").as_str())
        .await;

    result?;
    drop_result?;

    Ok(())
}
