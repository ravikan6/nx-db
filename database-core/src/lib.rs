// Internal modules – not all of these are public API.
pub mod context;
pub mod database;
pub mod enums;
pub mod errors;
pub mod events;
pub mod key;
pub mod macros;
pub mod model;
pub mod query;
pub mod registry;
pub mod repository;
pub mod schema;
pub mod system_fields;
pub mod traits;
pub mod utils;
pub mod value;

// ── Context ──────────────────────────────────────────────────────────────────
pub use context::Context;

// ── Database & repository ────────────────────────────────────────────────────
pub use database::{Database, DatabaseBuilder};
pub use repository::{PopulateDescriptor, Repository, RepositoryQuery, ScopedDatabase};

// ── Schema types ─────────────────────────────────────────────────────────────
pub use schema::{
    AttributePersistence, AttributeSchema, CollectionSchema, DefaultValue, IndexSchema,
    RelationshipSchema,
};

// ── Enum types ────────────────────────────────────────────────────────────────
pub use enums::{
    AttributeKind, IndexKind, OnDeleteAction, Order, RelationshipKind, RelationshipSide,
};

// ── Errors ────────────────────────────────────────────────────────────────────
pub use errors::{
    AuthorizationError, AuthorizationErrorKind, DatabaseError, PermissionError,
    PermissionErrorKind, RoleError, RoleErrorKind,
};

// ── Events ────────────────────────────────────────────────────────────────────
pub use events::{Event, EventBus, NoopEventBus};

// ── Key ───────────────────────────────────────────────────────────────────────
pub use key::{GENERATED_ID_MIN_LENGTH, GenerateId, Key, generate_id_string};

// ── Model ─────────────────────────────────────────────────────────────────────
pub use model::{CreateRecord, EntityRecord, Metadata, Model, ModelFuture, UpdateRecord};

// ── Query ─────────────────────────────────────────────────────────────────────
pub use query::{
    EncodedField, Field, Filter, FilterOp, IntoQueryValue, PopulateMany, PopulateOne, QueryInclude,
    QuerySpec, Rel, Sort, SortDirection, ThroughRel,
};

// ── Registry ──────────────────────────────────────────────────────────────────
pub use registry::{CollectionRegistry, StaticRegistry};

// ── System field name constants ───────────────────────────────────────────────
pub use system_fields::{
    COLUMN_CREATED_AT, COLUMN_ID, COLUMN_PERMISSIONS, COLUMN_SEQUENCE, COLUMN_TENANT,
    COLUMN_UPDATED_AT, FIELD_CREATED_AT, FIELD_ID, FIELD_PERMISSIONS, FIELD_SEQUENCE, FIELD_TENANT,
    FIELD_UPDATED_AT, is_system_field,
};

// ── Utilities ─────────────────────────────────────────────────────────────────
pub use utils::{
    Authorization, AuthorizationContext, Permission, PermissionEnum, Role, RoleName, UserDimension,
};

// ── Value helpers ─────────────────────────────────────────────────────────────
pub use value::{
    FromStorage, IntoStorage, Patch, Populated, RelationMany, RelationOne, get_optional,
    get_required, insert_value, take_optional, take_required,
};

// ── Re-export the `time` crate so downstream crates don't need to add it.
pub use time;
