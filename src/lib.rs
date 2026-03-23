pub use database_cache as cache;
pub use database_core as core;

// ── Sub-module aliases so paths like `nx_db::traits::storage::…` keep working.
pub mod traits {
    pub use database_core::traits::migration;
    pub use database_core::traits::storage;
}

// ── Top-level re-exports for the most common types ───────────────────────────
pub use database_core::{
    // Schema
    AttributeKind,
    AttributePersistence,
    AttributeSchema,
    COLUMN_CREATED_AT,
    COLUMN_ID,
    COLUMN_PERMISSIONS,
    COLUMN_SEQUENCE,
    COLUMN_UPDATED_AT,
    // Registry
    CollectionRegistry,
    CollectionSchema,
    // Context
    Context,
    // Database & repository
    Database,
    DatabaseBuilder,
    // Errors
    DatabaseError,
    DefaultValue,
    // Query
    EncodedField,
    // Events
    Event,
    EventBus,
    // System fields
    FIELD_CREATED_AT,
    FIELD_ID,
    FIELD_PERMISSIONS,
    FIELD_SEQUENCE,
    FIELD_TENANT,
    FIELD_UPDATED_AT,
    Field,
    Filter,
    FilterOp,
    // Value helpers
    FromStorage,
    IndexKind,
    IndexSchema,
    IntoStorage,
    // Key
    Key,
    // Model
    Metadata,
    Model,
    ModelFuture,
    NoopEventBus,
    OnDeleteAction,
    Order,
    Patch,
    // Utilities
    Permission,
    PermissionEnum,
    QuerySpec,
    RelationshipKind,
    RelationshipSchema,
    RelationshipSide,
    Repository,
    Role,
    RoleName,
    ScopedDatabase,
    Sort,
    SortDirection,
    StaticRegistry,
    get_optional,
    get_required,
    insert_value,
    take_optional,
    take_required,
};

// ── Macro re-exports ──────────────────────────────────────────────────────────
pub use database_core::{and, db_context, db_query, db_registry, impl_model, not, or};

// ── Driver feature modules ────────────────────────────────────────────────────
#[cfg(feature = "postgres")]
pub mod postgres {
    pub use driver_postgres::PostgresAdapter;
    pub mod migration {
        pub use driver_postgres::migration::*;
    }
}

#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use driver_sqlite::SqliteAdapter;
    pub mod migration {
        pub use driver_sqlite::migration::*;
    }
}

// ── Prelude ───────────────────────────────────────────────────────────────────
/// Convenience re-exports for the most common types.
///
/// ```rust,ignore
/// use nx_db::prelude::*;
/// ```
pub mod prelude {
    #[cfg(feature = "postgres")]
    pub use crate::postgres::PostgresAdapter;
    #[cfg(feature = "sqlite")]
    pub use crate::sqlite::SqliteAdapter;
    pub use crate::{
        Context, Database, DatabaseError, Field, Filter, Key, Model, Patch, QuerySpec, Repository,
        Role, StaticRegistry,
    };
}

/// Connect to a database by URL.
///
/// ```rust,ignore
/// let pool = db_connect!(postgres, "postgres://localhost/mydb").await?;
/// ```
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[macro_export]
macro_rules! db_connect {
    (postgres, $url:expr) => {
        sqlx::PgPool::connect($url)
    };
    (sqlite, $url:expr) => {
        sqlx::SqlitePool::connect($url)
    };
}
