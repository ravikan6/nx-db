#![cfg(feature = "sqlite")]

use driver_sqlite::SqliteAdapter;
use nx_db::{
    AttributeKind, AttributePersistence, AttributeSchema, CollectionSchema, Database, Field,
    Filter, FilterOp, Key, Patch, QuerySpec, StaticRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;

include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/codegen/production_models.rs"
));

#[tokio::test]
async fn sqlite_find_including_relations_uses_join_capable_adapter_path()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;

    let database = Database::new(SqliteAdapter::new(pool), prod_models::registry()?);
    let user_repo = database.repo::<prod_models::User>();
    let post_repo = database.repo::<prod_models::Post>();

    user_repo.create_collection().await?;
    post_repo.create_collection().await?;

    let user_indexes = sqlx::query("PRAGMA index_list('users')")
        .fetch_all(database.adapter().get_pool())
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(user_indexes.iter().any(|name| name == "users_email_unique"));

    let post_indexes = sqlx::query("PRAGMA index_list('posts')")
        .fetch_all(database.adapter().get_pool())
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<Vec<_>>();
    assert!(post_indexes.iter().any(|name| name == "full_text_content"));

    let user = user_repo
        .insert(prod_models::CreateUser::builder(
            "Ravi".into(),
            "ravi@example.com".into(),
        ))
        .await?;

    post_repo
        .insert(
            prod_models::CreatePost::builder("First".into(), user.id.to_string())
                .content(Some("one".into())),
        )
        .await?;
    post_repo
        .insert(prod_models::CreatePost::builder(
            "Second".into(),
            user.id.to_string(),
        ))
        .await?;

    let posts = post_repo
        .query()
        .sort(prod_models::POST_TITLE.asc())
        .populate(prod_models::POST_AUTHOR_POPULATE)
        .all()
        .await?;
    assert_eq!(posts.len(), 2);
    assert!(posts[0].author_rel.is_loaded());

    let users = user_repo
        .query()
        .populate(prod_models::USER_POSTS_POPULATE)
        .all()
        .await?;

    assert_eq!(users.len(), 1);
    let loaded_posts = users[0]
        .posts
        .as_slice()
        .expect("posts relation should be loaded");
    assert_eq!(loaded_posts.len(), 2);
    assert_eq!(loaded_posts[0].title, "First");
    assert_eq!(loaded_posts[1].title, "Second");

    Ok(())
}

#[tokio::test]
async fn sqlite_supports_startswith_endswith_and_textsearch_filters()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;

    let database = Database::new(SqliteAdapter::new(pool), prod_models::registry()?);
    let user_repo = database.repo::<prod_models::User>();
    let post_repo = database.repo::<prod_models::Post>();

    user_repo.create_collection().await?;
    post_repo.create_collection().await?;

    let user = user_repo
        .insert(prod_models::CreateUser::builder(
            "Ravi".into(),
            "ravi@example.com".into(),
        ))
        .await?;

    post_repo
        .insert(
            prod_models::CreatePost::builder("First benchmark".into(), user.id.to_string())
                .content(Some("sqlite search body one".into())),
        )
        .await?;
    post_repo
        .insert(
            prod_models::CreatePost::builder("Second article".into(), user.id.to_string())
                .content(Some("different text body two".into())),
        )
        .await?;

    let starts_with = post_repo
        .find(QuerySpec::new().filter(prod_models::POST_TITLE.starts_with("First")))
        .await?;
    assert_eq!(starts_with.len(), 1);
    assert_eq!(starts_with[0].title, "First benchmark");

    let ends_with = post_repo
        .find(QuerySpec::new().filter(Filter::field(
            "content",
            FilterOp::EndsWith(nx_db::traits::storage::StorageValue::String("one".into())),
        )))
        .await?;
    assert_eq!(ends_with.len(), 1);
    assert_eq!(ends_with[0].title, "First benchmark");

    let text_search = post_repo
        .find(QuerySpec::new().filter(Filter::field(
            "content",
            FilterOp::TextSearch(nx_db::traits::storage::StorageValue::String(
                "sqlite one".into(),
            )),
        )))
        .await?;
    assert_eq!(text_search.len(), 1);
    assert_eq!(text_search[0].title, "First benchmark");

    Ok(())
}

#[tokio::test]
async fn sqlite_update_and_delete_operations_use_main_alias_consistently()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;

    let database = Database::new(SqliteAdapter::new(pool), prod_models::registry()?);
    let user_repo = database.repo::<prod_models::User>();
    let post_repo = database.repo::<prod_models::Post>();

    user_repo.create_collection().await?;
    post_repo.create_collection().await?;

    let user = user_repo
        .insert(
            prod_models::CreateUser::builder("Ravi".into(), "ravi@example.com".into()).permissions(
                vec![
                    "read(\"any\")".into(),
                    "update(\"any\")".into(),
                    "delete(\"any\")".into(),
                ],
            ),
        )
        .await?;

    let post = post_repo
        .insert(
            prod_models::CreatePost::builder("Original".into(), user.id.to_string())
                .content(Some("first body".into()))
                .permissions(vec![
                    "read(\"any\")".into(),
                    "update(\"any\")".into(),
                    "delete(\"any\")".into(),
                ]),
        )
        .await?;

    let updated = post_repo
        .update(
            &post.id,
            prod_models::UpdatePost {
                content: nx_db::Patch::set(Some("updated body".into())),
                ..Default::default()
            },
        )
        .await?
        .expect("post should update");
    assert_eq!(updated.content.as_deref(), Some("updated body"));

    let updated_count = post_repo
        .update_many(
            QuerySpec::new().filter(prod_models::POST_TITLE.eq("Original")),
            prod_models::UpdatePost {
                content: nx_db::Patch::set(Some("batch body".into())),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(updated_count, 1);

    let deleted_count = post_repo
        .delete_many(QuerySpec::new().filter(prod_models::POST_TITLE.eq("Original")))
        .await?;
    assert_eq!(deleted_count, 1);

    Ok(())
}

type UserId = Key<48>;
type RoleId = Key<48>;
type UserRoleId = Key<48>;
type ProfileId = Key<48>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserWithRolesEntity {
    id: UserId,
    name: String,
    profile: nx_db::RelationOne<ProfileEntity>,
    roles: nx_db::RelationMany<RoleEntity>,
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone)]
struct CreateUserWithRoles {
    id: Option<UserId>,
    name: String,
    permissions: Vec<String>,
}

nx_db::impl_create_builder! { create: CreateUserWithRoles, id: UserId, required: { name: String }, optional: {  } }

#[derive(Debug, Clone, Default)]
struct UpdateUserWithRoles {
    name: Patch<String>,
    permissions: Patch<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
struct UserWithRoles;

const USER_WITH_ROLES_NAME: Field<UserWithRoles, String> = Field::new("name");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RoleEntity {
    id: RoleId,
    name: String,
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone)]
struct CreateRole {
    id: Option<RoleId>,
    name: String,
    permissions: Vec<String>,
}

nx_db::impl_create_builder! { create: CreateRole, id: RoleId, required: { name: String }, optional: {  } }

#[derive(Debug, Clone, Default)]
struct UpdateRole {
    name: Patch<String>,
    permissions: Patch<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
struct Role;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct UserRoleEntity {
    id: UserRoleId,
    user_id: String,
    role_id: String,
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone)]
struct CreateUserRole {
    id: Option<UserRoleId>,
    user_id: String,
    role_id: String,
    permissions: Vec<String>,
}

nx_db::impl_create_builder! { create: CreateUserRole, id: UserRoleId, required: { user_id: String, role_id: String }, optional: {  } }

#[derive(Debug, Clone, Default)]
struct UpdateUserRole {
    user_id: Patch<String>,
    role_id: Patch<String>,
    permissions: Patch<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
struct UserRole;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ProfileEntity {
    id: ProfileId,
    user_id: String,
    bio: String,
    _metadata: nx_db::Metadata,
}

#[derive(Debug, Clone)]
struct CreateProfile {
    id: Option<ProfileId>,
    user_id: String,
    bio: String,
    permissions: Vec<String>,
}

nx_db::impl_create_builder! { create: CreateProfile, id: ProfileId, required: { user_id: String, bio: String }, optional: {  } }

#[derive(Debug, Clone, Default)]
struct UpdateProfile {
    user_id: Patch<String>,
    bio: Patch<String>,
    permissions: Patch<Vec<String>>,
}

#[derive(Debug, Clone, Copy)]
struct Profile;

const USER_WITH_ROLES_ATTRIBUTES: &[AttributeSchema] = &[
    AttributeSchema {
        id: "name",
        column: "name",
        kind: AttributeKind::String,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
    AttributeSchema {
        id: "profile",
        column: "",
        kind: AttributeKind::Relationship,
        required: false,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Virtual,
        filters: &[],
        relationship: Some(nx_db::RelationshipSchema {
            related_collection: "profiles",
            kind: nx_db::RelationshipKind::OneToOne,
            side: nx_db::RelationshipSide::Parent,
            two_way: false,
            two_way_key: Some("userId"),
            through_collection: None,
            through_local_field: None,
            through_remote_field: None,
            on_delete: nx_db::OnDeleteAction::Restrict,
        }),
    },
];

const ROLE_ATTRIBUTES: &[AttributeSchema] = &[AttributeSchema {
    id: "name",
    column: "name",
    kind: AttributeKind::String,
    required: true,
    array: false,
    length: None,
    default: None,
    persistence: AttributePersistence::Persisted,
    filters: &[],
    relationship: None,
}];

const USER_ROLE_ATTRIBUTES: &[AttributeSchema] = &[
    AttributeSchema {
        id: "userId",
        column: "user_id",
        kind: AttributeKind::Relationship,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
    AttributeSchema {
        id: "roleId",
        column: "role_id",
        kind: AttributeKind::Relationship,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
];

const PROFILE_ATTRIBUTES: &[AttributeSchema] = &[
    AttributeSchema {
        id: "userId",
        column: "user_id",
        kind: AttributeKind::Relationship,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
    AttributeSchema {
        id: "bio",
        column: "bio",
        kind: AttributeKind::String,
        required: true,
        array: false,
        length: None,
        default: None,
        persistence: AttributePersistence::Persisted,
        filters: &[],
        relationship: None,
    },
];

static USER_WITH_ROLES_SCHEMA: CollectionSchema = CollectionSchema {
    id: "mm_users",
    name: "MmUsers",
    document_security: true,
    enabled: true,
    permissions: &["read(\"any\")", "create(\"any\")"],
    attributes: USER_WITH_ROLES_ATTRIBUTES,
    indexes: &[],
};

static ROLE_SCHEMA: CollectionSchema = CollectionSchema {
    id: "mm_roles",
    name: "MmRoles",
    document_security: true,
    enabled: true,
    permissions: &["read(\"any\")", "create(\"any\")"],
    attributes: ROLE_ATTRIBUTES,
    indexes: &[],
};

static USER_ROLE_SCHEMA: CollectionSchema = CollectionSchema {
    id: "user_roles",
    name: "UserRoles",
    document_security: true,
    enabled: true,
    permissions: &["read(\"any\")", "create(\"any\")"],
    attributes: USER_ROLE_ATTRIBUTES,
    indexes: &[],
};

static PROFILE_SCHEMA: CollectionSchema = CollectionSchema {
    id: "profiles",
    name: "Profiles",
    document_security: true,
    enabled: true,
    permissions: &["read(\"any\")", "create(\"any\")"],
    attributes: PROFILE_ATTRIBUTES,
    indexes: &[],
};

const USER_ROLES_REL: nx_db::Rel<UserWithRoles, Role> =
    nx_db::Rel::<UserWithRoles, Role>::many_to_many("roles", "user_roles", "userId", "roleId");
const USER_PROFILE_REL: nx_db::Rel<UserWithRoles, Profile> =
    nx_db::Rel::<UserWithRoles, Profile>::one_to_one("profile", nx_db::FIELD_ID, "userId");
fn populate_user_roles_local_key(entity: &UserWithRolesEntity) -> String {
    entity.id.to_string()
}
fn populate_user_roles_remote_key(entity: &RoleEntity) -> Option<String> {
    Some(entity.id.to_string())
}
fn populate_user_roles_set(
    entity: &mut UserWithRolesEntity,
    value: nx_db::RelationMany<RoleEntity>,
) {
    entity.roles = value;
}
const USER_ROLES_POPULATE: nx_db::core::PopulateMany<UserWithRoles, Role> =
    nx_db::core::PopulateMany::new(
        USER_ROLES_REL,
        populate_user_roles_local_key,
        populate_user_roles_remote_key,
        populate_user_roles_set,
    );
fn populate_user_profile_local_key(entity: &UserWithRolesEntity) -> Option<String> {
    Some(entity.id.to_string())
}
fn populate_user_profile_remote_key(entity: &ProfileEntity) -> Option<String> {
    Some(entity.user_id.clone())
}
fn populate_user_profile_set(
    entity: &mut UserWithRolesEntity,
    value: nx_db::RelationOne<ProfileEntity>,
) {
    entity.profile = value;
}
const USER_PROFILE_POPULATE: nx_db::core::PopulateOne<UserWithRoles, Profile> =
    nx_db::core::PopulateOne::new(
        USER_PROFILE_REL,
        populate_user_profile_local_key,
        populate_user_profile_remote_key,
        populate_user_profile_set,
    );

nx_db::impl_model! {
    name: UserWithRoles,
    id: UserId,
    entity: UserWithRolesEntity,
    create: CreateUserWithRoles,
    update: UpdateUserWithRoles,
    schema: USER_WITH_ROLES_SCHEMA,
    fields: { "name" => name : String :required },
    loaded_one: { profile },
    loaded_many: { roles }
}

nx_db::impl_model! {
    name: Role,
    id: RoleId,
    entity: RoleEntity,
    create: CreateRole,
    update: UpdateRole,
    schema: ROLE_SCHEMA,
    fields: { "name" => name : String :required }
}

nx_db::impl_model! {
    name: UserRole,
    id: UserRoleId,
    entity: UserRoleEntity,
    create: CreateUserRole,
    update: UpdateUserRole,
    schema: USER_ROLE_SCHEMA,
    fields: {
        "userId" => user_id : String :required,
        "roleId" => role_id : String :required
    }
}

nx_db::impl_model! {
    name: Profile,
    id: ProfileId,
    entity: ProfileEntity,
    create: CreateProfile,
    update: UpdateProfile,
    schema: PROFILE_SCHEMA,
    fields: {
        "userId" => user_id : String :required,
        "bio" => bio : String :required
    }
}

fn many_to_many_registry() -> Result<StaticRegistry, nx_db::DatabaseError> {
    StaticRegistry::new()
        .register(&USER_WITH_ROLES_SCHEMA)?
        .register(&ROLE_SCHEMA)?
        .register(&USER_ROLE_SCHEMA)?
        .register(&PROFILE_SCHEMA)
}

#[tokio::test]
async fn sqlite_many_to_many_include_loads_through_join_table_and_preserves_base_pagination()
-> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;

    let database = Database::new(SqliteAdapter::new(pool), many_to_many_registry()?);
    let user_repo = database.repo::<UserWithRoles>();
    let role_repo = database.repo::<Role>();
    let user_role_repo = database.repo::<UserRole>();
    let profile_repo = database.repo::<Profile>();

    user_repo.create_collection().await?;
    role_repo.create_collection().await?;
    user_role_repo.create_collection().await?;
    profile_repo.create_collection().await?;

    let alpha = user_repo
        .insert(CreateUserWithRoles::builder("Alpha".into()))
        .await?;
    let beta = user_repo
        .insert(CreateUserWithRoles::builder("Beta".into()))
        .await?;

    profile_repo
        .insert(CreateProfile::builder(
            alpha.id.to_string(),
            "Alpha bio".into(),
        ))
        .await?;
    profile_repo
        .insert(CreateProfile::builder(
            beta.id.to_string(),
            "Beta bio".into(),
        ))
        .await?;

    let admin = role_repo
        .insert(CreateRole::builder("Admin".into()))
        .await?;
    let editor = role_repo
        .insert(CreateRole::builder("Editor".into()))
        .await?;
    let viewer = role_repo
        .insert(CreateRole::builder("Viewer".into()))
        .await?;

    user_role_repo
        .insert(CreateUserRole::builder(
            alpha.id.to_string(),
            admin.id.to_string(),
        ))
        .await?;
    user_role_repo
        .insert(CreateUserRole::builder(
            alpha.id.to_string(),
            editor.id.to_string(),
        ))
        .await?;
    user_role_repo
        .insert(CreateUserRole::builder(
            beta.id.to_string(),
            viewer.id.to_string(),
        ))
        .await?;

    let users = user_repo
        .query()
        .sort(USER_WITH_ROLES_NAME.asc())
        .limit(1)
        .populate(USER_PROFILE_POPULATE)
        .populate(USER_ROLES_POPULATE)
        .all()
        .await?;

    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Alpha");
    let loaded_profile = users[0]
        .profile
        .get()
        .expect("profile relation should be loaded");
    assert_eq!(loaded_profile.bio, "Alpha bio");
    let loaded_roles = users[0]
        .roles
        .as_slice()
        .expect("roles relation should be loaded");
    assert_eq!(loaded_roles.len(), 2);
    assert_eq!(loaded_roles[0].name, "Admin");
    assert_eq!(loaded_roles[1].name, "Editor");

    Ok(())
}
